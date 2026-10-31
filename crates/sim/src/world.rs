//! The simulated world: nodes, the bucket, the network between them, and
//! the shell that turns each core's effects into later events.
//!
//! Everything runs on one simulated millisecond clock. The agenda is an
//! ordered map keyed by `(time, sequence)`. Two items due at the same
//! instant run in the order they were scheduled, so the whole run is a
//! function of the seed.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};

use celld_logic::{
    on_event, pressure, Config, Effect, Event, NodeLeaseSpec, OwnershipOnEvict, State, Timer,
};

use crate::store::Store;
use crate::{Outcome, Rng, SimConfig};

type Time = u64;

/// Simulated time zero on the wall clock.
const WALL_EPOCH_MS: u64 = 1_800_000_000_000;
const PEER_PROTOCOL: u16 = 1;

/// Something due at a simulated instant.
enum Item {
    /// A completion the shell reports some time after the effect started.
    Deliver { node: usize, event: Event },
    /// A timer the core armed. `ordinal` tells a live arm from one that a
    /// later `ScheduleTimer` for the same slot replaced.
    Timer {
        node: usize,
        slot: Slot,
        ordinal: u64,
    },
    /// A bucket request arrives at the bucket and is applied.
    StoreApply { call: u64 },
    /// The bucket's response arrives back at the node.
    StoreReply { call: u64 },
}

/// Which armed timer a new arm replaces. This follows `TimerSlot` in
/// `crates/celld/actor.rs`: the lease timers coalesce, and the rest are
/// keyed so that arming one never disarms another.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Slot {
    Renew,
    Fence,
    Alarm(String),
    Deadline(u64),
    Queued(String, u64),
    Recovery(String, u64),
}

impl Slot {
    fn of(timer: &Timer) -> Self {
        match timer {
            Timer::NodeLeaseRenew { .. } => Slot::Renew,
            Timer::NodeLeaseFence { .. } => Slot::Fence,
            Timer::CellAlarm { cell, .. } => Slot::Alarm(cell.clone()),
            Timer::OperationDeadline { op } => Slot::Deadline(*op),
            Timer::QueuedActivation { cell, generation } => Slot::Queued(cell.clone(), *generation),
            Timer::OwnerLogRecoveryRetry { cell, generation } => {
                Slot::Recovery(cell.clone(), *generation)
            }
        }
    }
}

/// One bucket round trip. The request carries the completion event the
/// node will receive. The bucket fills in its result when the request
/// applies, and the clocks are stamped when the response arrives.
struct Call {
    node: usize,
    request: Effect,
    reply: Option<Event>,
}

struct Node {
    name: String,
    state: State,
    /// Where this node's monotonic clock starts. Nodes boot at different
    /// moments, so their monotonic clocks disagree.
    mono_base: u64,
    timers: BTreeMap<Slot, (u64, Timer)>,
    next_ordinal: u64,
    halted: bool,
}

pub struct World {
    seed: u64,
    config: SimConfig,
    t: Time,
    seq: u64,
    agenda: BTreeMap<(Time, u64), Item>,
    store: Store,
    nodes: Vec<Node>,
    calls: BTreeMap<u64, Call>,
    next_call: u64,
    net_rng: Rng,
    hash: Fnv,
    trace: Vec<String>,
    events: u64,
    panic: Option<String>,
}

impl World {
    pub fn new(seed: u64, config: SimConfig) -> Self {
        let mut master = Rng::new(seed);
        let mut net_rng = master.fork();
        let core_config = core_config();
        let nodes = (0..config.nodes)
            .map(|index| Node {
                name: format!("n{index}"),
                state: State::new(format!("n{index}"), core_config),
                mono_base: net_rng.range(1_000, 1_000_000),
                timers: BTreeMap::new(),
                next_ordinal: 0,
                halted: false,
            })
            .collect();
        Self {
            seed,
            config,
            t: 0,
            seq: 0,
            agenda: BTreeMap::new(),
            store: Store::default(),
            nodes,
            calls: BTreeMap::new(),
            next_call: 0,
            net_rng,
            hash: Fnv(0xcbf2_9ce4_8422_2325),
            trace: Vec::new(),
            events: 0,
            panic: None,
        }
    }

    fn after(&mut self, delay: u64, item: Item) {
        self.seq += 1;
        self.agenda.insert((self.t + delay, self.seq), item);
    }

    fn wall(&self) -> u64 {
        WALL_EPOCH_MS + self.t
    }

    fn mono(&self, node: usize) -> u64 {
        self.nodes[node].mono_base + self.t
    }

    pub fn run(mut self) -> Outcome {
        for node in 0..self.nodes.len() {
            let spec = NodeLeaseSpec {
                addr: self.nodes[node].name.clone(),
                peer_protocol: PEER_PROTOCOL,
                generation: format!("g{node}"),
                resume_generation: None,
                ttl_ms: self.config.ttl_ms,
            };
            self.drive(
                node,
                Event::StartNodeLease {
                    now_ms: 0,
                    now_mono_ms: 0,
                    spec,
                },
            );
        }
        while let Some((&(t, _), _)) = self.agenda.first_key_value() {
            if t > self.config.duration_ms || self.panic.is_some() {
                break;
            }
            let ((t, _), item) = self.agenda.pop_first().unwrap();
            self.t = t;
            match item {
                Item::Deliver { node, event } => self.drive(node, event),
                Item::Timer {
                    node,
                    slot,
                    ordinal,
                } => self.timer(node, slot, ordinal),
                Item::StoreApply { call } => self.store_apply(call),
                Item::StoreReply { call } => self.store_reply(call),
            }
        }
        self.finish()
    }

    /// Run one event through a node's core and carry out its effects.
    fn drive(&mut self, node: usize, mut event: Event) {
        if self.nodes[node].halted {
            return;
        }
        stamp(&mut event, self.wall(), self.mono(node));
        self.record(node, "<-", &event);
        self.events += 1;
        let state = &mut self.nodes[node].state;
        // `on_event` runs `State::validate` after every event in builds with
        // debug assertions, so a broken core invariant arrives here as a
        // panic, and is reported with its seed.
        let effects = match catch_unwind(AssertUnwindSafe(|| on_event(state, event))) {
            Ok(effects) => effects,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "non-string panic".into());
                self.panic = Some(format!("t={} n{node}: {message}", self.t));
                return;
            }
        };
        for effect in effects {
            self.record(node, "->", &effect);
            self.execute(node, effect);
        }
    }

    /// The shell: carry out one effect, as `Actor::execute` does.
    fn execute(&mut self, node: usize, effect: Effect) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                let mono = self.mono(node);
                let n = &mut self.nodes[node];
                let slot = Slot::of(&timer);
                let ordinal = n.next_ordinal;
                n.next_ordinal += 1;
                n.timers.insert(slot.clone(), (ordinal, timer));
                self.after(
                    at_mono_ms.saturating_sub(mono),
                    Item::Timer {
                        node,
                        slot,
                        ordinal,
                    },
                );
            }
            Effect::ReadSelfNodeLease { .. } | Effect::CasNodeLease { .. } => {
                self.store_call(node, effect)
            }
            Effect::ReadLocalCells => {
                // No disk yet, so no local cells. Production scans on a
                // blocking task, so the answer arrives a little later.
                let delay = self.net_rng.range(1, 5);
                let event = Event::LocalCellsRead { result: Ok(vec![]) };
                self.after(delay, Item::Deliver { node, event });
            }
            Effect::Halt { reason, .. } => {
                self.note(|| format!("n{node} halts: {reason:?}"));
                self.nodes[node].halted = true;
            }
            other => unimplemented!("the simulated shell does not carry out {other:?} yet"),
        }
    }

    fn timer(&mut self, node: usize, slot: Slot, ordinal: u64) {
        let n = &mut self.nodes[node];
        if n.timers.get(&slot).map(|(armed, _)| *armed) != Some(ordinal) {
            // A later arm replaced this one.
            return;
        }
        let (_, timer) = n.timers.remove(&slot).unwrap();
        self.drive(
            node,
            Event::TimerFired {
                timer,
                now_ms: 0,
                now_mono_ms: 0,
            },
        );
    }

    // -----------------------------------------------------------------------
    // The bucket and the network to it

    /// Send a request to the bucket. It applies after one drawn latency and
    /// its response arrives after another, so other events can happen in
    /// between, on this node and on others.
    fn store_call(&mut self, node: usize, request: Effect) {
        let call = self.next_call;
        self.next_call += 1;
        self.calls.insert(
            call,
            Call {
                node,
                request,
                reply: None,
            },
        );
        let delay = self.latency();
        self.after(delay, Item::StoreApply { call });
    }

    fn latency(&mut self) -> u64 {
        let (lo, hi) = self.config.store_latency_ms;
        self.net_rng.range(lo, hi)
    }

    fn store_apply(&mut self, call: u64) {
        let Call { node, request, .. } = &self.calls[&call];
        let name = &self.nodes[*node].name;
        let reply = match request {
            Effect::ReadSelfNodeLease { op } => Event::SelfNodeLeaseRead {
                op: *op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Ok(self.store.read_lease(name)),
            },
            Effect::CasNodeLease {
                op, guard, record, ..
            } => Event::NodeLeaseCasCompleted {
                op: *op,
                now_mono_ms: 0,
                result: Ok(self.store.cas_lease(guard, record)),
                // The bucket posture: lease bodies carry no node-log state.
                stamped_log_state: None,
            },
            other => unreachable!("not a bucket request: {other:?}"),
        };
        self.calls.get_mut(&call).unwrap().reply = Some(reply);
        let delay = self.latency();
        self.after(delay, Item::StoreReply { call });
    }

    fn store_reply(&mut self, call: u64) {
        let Call { node, reply, .. } = self.calls.remove(&call).unwrap();
        self.drive(node, reply.expect("a reply follows its apply"));
    }

    // -----------------------------------------------------------------------
    // Tracing and the result

    /// Fold an event or effect into the trace hash, and into the readable
    /// trace when it is on.
    fn record(&mut self, node: usize, arrow: &str, item: &dyn std::fmt::Debug) {
        let _ = write!(self.hash, "{} {node} {arrow} {item:?};", self.t);
        if self.config.trace {
            self.trace
                .push(format!("{:>7} n{node} {arrow} {item:?}", self.t));
        }
    }

    fn note(&mut self, line: impl FnOnce() -> String) {
        if self.config.trace {
            let line = line();
            self.trace.push(format!("{:>7} {line}", self.t));
        }
    }

    fn finish(self) -> Outcome {
        Outcome {
            seed: self.seed,
            trace_hash: self.hash.0,
            trace: self.trace,
            events: self.events,
            halts: names(&self.nodes, |n| n.halted),
            authoritative: names(&self.nodes, |n| n.state.node_authoritative()),
            panic: self.panic,
        }
    }
}

fn names(nodes: &[Node], keep: impl Fn(&Node) -> bool) -> Vec<String> {
    nodes
        .iter()
        .filter(|n| keep(n))
        .map(|n| n.name.clone())
        .collect()
}

fn core_config() -> Config {
    Config {
        max_resident: 4,
        max_activations: 4,
        max_evictions: 2,
        max_releases: 2,
        max_outbound_websockets: 32,
        ownership_on_evict: OwnershipOnEvict::Release,
        require_node_lease: true,
        peer_protocol: PEER_PROTOCOL,
        operation_deadline_ms: Some(15_000),
        owner_log_recovery_backoff_ms: 1_000,
        owner_log_recovery_attempts: 240,
        alarm_resident_ms: 3_600_000,
        idle_evict_ms: None,
        pressure: pressure::PressureConfig::from_limits(None, None),
    }
}

/// Fill in the clocks an event carries, sampled on the receiving node at
/// the instant it is delivered. Production samples them at the same point.
fn stamp(event: &mut Event, wall: u64, mono: u64) {
    match event {
        Event::StartNodeLease {
            now_ms,
            now_mono_ms,
            ..
        }
        | Event::SelfNodeLeaseRead {
            now_ms,
            now_mono_ms,
            ..
        }
        | Event::TimerFired {
            now_ms,
            now_mono_ms,
            ..
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        Event::NodeLeaseCasCompleted { now_mono_ms, .. } => *now_mono_ms = mono,
        _ => {}
    }
}

/// FNV-1a over everything written to it.
struct Fnv(u64);

impl std::fmt::Write for Fnv {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        for byte in s.bytes() {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Ok(())
    }
}
