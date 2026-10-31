//! The simulated world: nodes, the bucket, the clients, and the agenda that
//! orders everything they do.
//!
//! Everything runs on one simulated millisecond clock. The agenda is an
//! ordered map keyed by `(time, sequence)`. Two items due at the same
//! instant run in the order they were scheduled, so the whole run is a
//! function of the seed.
//!
//! This file owns the scheduler and the step that feeds one event to one
//! core. The rest of the world lives in its children:
//!
//! - `shell`: carries out each effect, as `Actor::execute` does, including
//!   the round trips to the bucket.
//! - `replicator`: each node's open databases, their uploads and the
//!   durability proofs, as `ltx_repl.rs` does.
//! - `clients`: the workload and the scripted handler that serves it.

mod clients;
mod replicator;
mod shell;

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};

use celld_logic::{
    on_event, pressure, Config, Epoch, Event, NodeLeaseSpec, OpId, OwnershipOnEvict, RequestId,
    State, Timer,
};

use crate::checks::Checker;
use crate::store::Store;
use crate::{Outcome, Rng, SimConfig, Stats, WriteId};

use clients::{Op, Req};
use replicator::Replica;
use shell::{Slot, StoreReq};

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
    StoreApply { node: usize, req: StoreReq },
    /// The bucket confirmed an upload of `len` writes.
    Uploaded {
        node: usize,
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    /// The bucket answered a restore's chain read.
    ChainRead {
        node: usize,
        op: OpId,
        cell: String,
        epoch: Epoch,
        chain: Option<Vec<WriteId>>,
    },
    /// A client sends its next request.
    ClientNew,
    /// A request reaches a node: from its client, or forwarded by the
    /// ingress on route `via`.
    Arrive {
        node: usize,
        op: u64,
        via: Option<Epoch>,
    },
    /// A handler commits its write.
    HandlerCommit { node: usize, request: RequestId },
    /// A handler returns its response.
    HandlerEnd { node: usize, request: RequestId },
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
    replicas: BTreeMap<(String, Epoch), Replica>,
    /// The runtime registry: cells this node serves, at which epoch.
    published: BTreeMap<String, Epoch>,
    requests: BTreeMap<RequestId, Req>,
}

pub struct World {
    seed: u64,
    config: SimConfig,
    t: Time,
    seq: u64,
    agenda: BTreeMap<(Time, u64), Item>,
    store: Store,
    nodes: Vec<Node>,
    ops: BTreeMap<u64, Op>,
    next_op: u64,
    next_request: RequestId,
    next_write: WriteId,
    net_rng: Rng,
    work_rng: Rng,
    checker: Checker,
    stats: Stats,
    hash: Fnv,
    trace: Vec<String>,
    panic: Option<String>,
}

impl World {
    pub fn new(seed: u64, config: SimConfig) -> Self {
        let mut master = Rng::new(seed);
        // One stream per concern, so that drawing more for one does not
        // shift every decision of the other.
        let mut net_rng = master.fork();
        let work_rng = master.fork();
        let core_config = core_config();
        let nodes = (0..config.nodes)
            .map(|index| Node {
                name: format!("n{index}"),
                state: State::new(format!("n{index}"), core_config),
                mono_base: net_rng.range(1_000, 1_000_000),
                timers: BTreeMap::new(),
                next_ordinal: 0,
                halted: false,
                replicas: BTreeMap::new(),
                published: BTreeMap::new(),
                requests: BTreeMap::new(),
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
            ops: BTreeMap::new(),
            next_op: 1,
            next_request: 1,
            next_write: 1,
            net_rng,
            work_rng,
            checker: Checker::default(),
            stats: Stats::default(),
            hash: Fnv(0xcbf2_9ce4_8422_2325),
            trace: Vec::new(),
            panic: None,
        }
    }

    fn after(&mut self, delay: u64, item: Item) {
        self.seq += 1;
        self.agenda.insert((self.t + delay, self.seq), item);
    }

    fn deliver(&mut self, node: usize, delay: u64, event: Event) {
        self.after(delay, Item::Deliver { node, event });
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
            let event = Event::StartNodeLease {
                now_ms: 0,
                now_mono_ms: 0,
                spec,
            };
            self.deliver(node, 0, event);
        }
        self.start_clients();
        while let Some(((t, _), item)) = self.agenda.pop_first() {
            if t > self.config.duration_ms {
                break;
            }
            self.t = t;
            // A panic anywhere in the step, in a core or in the simulated
            // shell, ends the run and is reported with its seed and time.
            // `on_event` runs `State::validate` after every event in builds
            // with debug assertions, so a broken core invariant arrives here
            // as a panic too.
            let step = catch_unwind(AssertUnwindSafe(|| self.handle(item)));
            if let Err(payload) = step {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "non-string panic".into());
                self.panic = Some(format!("seed {} t={}: {message}", self.seed, self.t));
                break;
            }
        }
        self.finish()
    }

    fn handle(&mut self, item: Item) {
        match item {
            Item::Deliver { node, event } => self.drive(node, event),
            Item::Timer {
                node,
                slot,
                ordinal,
            } => self.timer(node, slot, ordinal),
            Item::StoreApply { node, req } => self.store_apply(node, req),
            Item::Uploaded {
                node,
                cell,
                epoch,
                len,
            } => self.uploaded(node, cell, epoch, len),
            Item::ChainRead {
                node,
                op,
                cell,
                epoch,
                chain,
            } => self.chain_read(node, op, cell, epoch, chain),
            Item::ClientNew => self.client_new(),
            Item::Arrive { node, op, via } => self.arrive(node, op, via),
            Item::HandlerCommit { node, request } => self.handler_commit(node, request),
            Item::HandlerEnd { node, request } => self.handler_end(node, request),
        }
    }

    /// Run one event through a node's core and carry out its effects. Some
    /// effects complete inside the same step, as they do in `Actor::drive`;
    /// their events queue behind the current one.
    fn drive(&mut self, node: usize, event: Event) {
        let mut queue = VecDeque::from([event]);
        while let Some(mut event) = queue.pop_front() {
            if self.nodes[node].halted {
                return;
            }
            shell::stamp(&mut event, self.wall(), self.mono(node));
            self.record(node, "<-", &event);
            self.stats.events += 1;
            let effects = on_event(&mut self.nodes[node].state, event);
            for effect in effects {
                self.record(node, "->", &effect);
                self.execute(node, effect, &mut queue);
            }
        }
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
        let names = |keep: &dyn Fn(&Node) -> bool| {
            self.nodes
                .iter()
                .filter(|n| keep(n))
                .map(|n| n.name.clone())
                .collect()
        };
        Outcome {
            seed: self.seed,
            trace_hash: self.hash.0,
            halts: names(&|n| n.halted),
            authoritative: names(&|n| n.state.node_authoritative()),
            trace: self.trace,
            panic: self.panic,
            violations: self.checker.violations,
            stats: self.stats,
        }
    }
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
