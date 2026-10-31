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
//! - `replicator`: each node's disk and open databases, their uploads and
//!   the durability proofs, as `ltx_repl.rs` does.
//! - `clients`: the workload and the scripted handler that serves it.
//! - `faults`: what goes wrong, and the supervisor that restarts a node.
//! - `liveness`: the probe and the audit that end every run.
//!
//! A node is a machine with a disk. The process on it can die and be
//! restarted. Each start is a new *incarnation* with a fresh core, fresh
//! clocks and no memory. Work addressed to a process carries its
//! incarnation and is dropped if that process has died since: a dead
//! process's timers never fire and its bucket answers reach nobody.

mod clients;
mod faults;
mod liveness;
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

use clients::{Bounce, Op, Req};
use faults::{Fault, Partition};
use liveness::Goal;
use replicator::{Disk, Replica};
use shell::{Slot, StoreReq};

type Time = u64;

/// The core's deadline for an operation, and the shell's for its own work.
const OPERATION_DEADLINE_MS: u64 = 15_000;
/// How often a process samples its load (`LOAD_SAMPLE_PERIOD`, main.rs).
const LOAD_SAMPLE_MS: u64 = 1_000;

/// Simulated time zero on the wall clock.
const WALL_EPOCH_MS: u64 = 1_800_000_000_000;
const PEER_PROTOCOL: u16 = 1;

/// Something due at a simulated instant.
enum Item {
    /// Work for one process. Dropped if that process has died since.
    Proc { node: usize, inc: u64, work: Work },
    /// A bucket request arrives at the bucket and is applied. It applies
    /// even if its sender has died since: it was already on the wire. Its
    /// answer returns after `answer_ms`, or never when the response is lost.
    StoreApply {
        node: usize,
        inc: u64,
        req: StoreReq,
        answer_ms: Option<u64>,
    },
    /// A client sends its next request.
    ClientNew,
    /// A request reaches a node: from its client, or forwarded by the
    /// ingress on route `via`. Not tied to a process: it reaches whichever
    /// process runs on the node by then, or none.
    Arrive {
        node: usize,
        op: u64,
        via: Option<Epoch>,
    },
    /// A forward came back to the ingress without running.
    Bounced { op: u64, why: Bounce },
    /// The ingress resolves the request's route again.
    Redispatch { op: u64 },
    /// The next fault of this kind.
    Fault(Fault),
    /// The supervisor starts a new process on the node.
    Restart { node: usize },
    /// A paused process runs again.
    Resume { node: usize, inc: u64 },
    /// A node's network comes back.
    Heal { node: usize, partition: u64 },
    /// Liveness: every cell must now take a write.
    Probe,
    /// Liveness: the fleet dies and a fresh node must restore every cell.
    Audit,
    /// One attempt at a liveness goal.
    GoalSend { goal: usize },
    /// The run is over.
    End,
}

/// What a process does later.
enum Work {
    /// A completion the shell reports some time after the effect started.
    Deliver(Event),
    /// A timer the core armed. `ordinal` tells a live arm from one that a
    /// later `ScheduleTimer` for the same slot replaced.
    Timer { slot: Slot, ordinal: u64 },
    /// The bucket confirmed an upload of `len` writes.
    Uploaded {
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    /// An upload failed, or its answer was lost.
    UploadFailed { cell: String, epoch: Epoch },
    /// Upload whatever the bucket does not have yet.
    Upload { cell: String, epoch: Epoch },
    /// The replicator gives up on a proof, or on a stop's final upload.
    ProofDeadline {
        cell: String,
        epoch: Epoch,
        op: OpId,
    },
    /// The bucket answered a restore's chain read.
    ChainRead {
        op: OpId,
        cell: String,
        epoch: Epoch,
        chain: Option<Vec<WriteId>>,
    },
    /// The process samples its load and tells its core.
    LoadSample,
    /// A handler commits its write.
    HandlerCommit { request: RequestId },
    /// A handler returns its response.
    HandlerEnd { request: RequestId },
}

/// The process running on a node, or the gap between two of them. A crash
/// replaces the whole value, so nothing of the old process survives in it.
struct Node {
    name: String,
    /// Which start of this node's process this is.
    inc: u64,
    up: bool,
    /// The core asked to halt; the process exits after this step.
    halting: bool,
    /// While paused, the process runs nothing: its work waits until here.
    paused_until: Option<Time>,
    /// When a VM suspend began: the monotonic clock stops with it.
    suspended_at: Option<Time>,
    /// How far this process's wall clock was stepped.
    wall_step_ms: i64,
    state: State,
    /// Where this process's monotonic clock starts. Processes start at
    /// different moments, so their monotonic clocks disagree.
    mono_base: u64,
    /// Armed timers: the arm's ordinal, the timer, and its monotonic due time.
    timers: BTreeMap<Slot, (u64, Timer, u64)>,
    next_ordinal: u64,
    replicas: BTreeMap<(String, Epoch), Replica>,
    /// The runtime registry: cells this process serves, at which epoch.
    published: BTreeMap<String, Epoch>,
    requests: BTreeMap<RequestId, Req>,
}

impl Node {
    fn new(index: usize, inc: u64, mono_base: u64, config: Config) -> Self {
        let name = format!("n{index}");
        Self {
            state: State::new(name.clone(), config),
            name,
            inc,
            up: false,
            halting: false,
            paused_until: None,
            suspended_at: None,
            wall_step_ms: 0,
            mono_base,
            timers: BTreeMap::new(),
            next_ordinal: 0,
            replicas: BTreeMap::new(),
            published: BTreeMap::new(),
            requests: BTreeMap::new(),
        }
    }
}

pub struct World {
    seed: u64,
    config: SimConfig,
    t: Time,
    seq: u64,
    /// The core configuration every process starts with.
    core: Config,
    agenda: BTreeMap<(Time, u64), Item>,
    store: Store,
    nodes: Vec<Node>,
    disks: Vec<Disk>,
    /// Each node's fixed wall clock offset.
    skews: Vec<i64>,
    /// Each node's network partition, while it lasts.
    partitions: Vec<Option<(u64, Partition)>>,
    next_partition: u64,
    ops: BTreeMap<u64, Op>,
    next_op: u64,
    next_request: RequestId,
    next_write: WriteId,
    net_rng: Rng,
    work_rng: Rng,
    fault_rng: Rng,
    checker: Checker,
    stats: Stats,
    hash: Fnv,
    trace: Vec<String>,
    halts: Vec<String>,
    goals: Vec<Goal>,
    /// The audit has begun: dead processes stay dead.
    auditing: bool,
    panic: Option<String>,
}

impl World {
    pub fn new(seed: u64, config: SimConfig) -> Self {
        let mut master = Rng::new(seed);
        // One stream per concern, so that drawing more for one does not
        // shift every decision of the other.
        let net_rng = master.fork();
        let work_rng = master.fork();
        let mut fault_rng = master.fork();
        let skew = config.faults.max_skew_ms;
        let skews = (0..config.nodes)
            .map(|_| fault_rng.range(0, 2 * skew) as i64 - skew as i64)
            .collect();
        let core = core_config(&config);
        let nodes = (0..config.nodes)
            .map(|index| Node::new(index, 0, 0, core))
            .collect();
        let disks = (0..config.nodes).map(|_| Disk::default()).collect();
        let partitions = vec![None; config.nodes];
        Self {
            seed,
            config,
            t: 0,
            seq: 0,
            core,
            agenda: BTreeMap::new(),
            store: Store::default(),
            nodes,
            disks,
            skews,
            partitions,
            next_partition: 1,
            ops: BTreeMap::new(),
            next_op: 1,
            next_request: 1,
            next_write: 1,
            net_rng,
            work_rng,
            fault_rng,
            checker: Checker::default(),
            stats: Stats::default(),
            hash: Fnv(0xcbf2_9ce4_8422_2325),
            trace: Vec::new(),
            halts: Vec::new(),
            goals: Vec::new(),
            auditing: false,
            panic: None,
        }
    }

    fn after(&mut self, delay: u64, item: Item) {
        self.seq += 1;
        self.agenda.insert((self.t + delay, self.seq), item);
    }

    /// Schedule work for the process now running on `node`.
    fn work(&mut self, node: usize, delay: u64, work: Work) {
        let inc = self.nodes[node].inc;
        self.after(delay, Item::Proc { node, inc, work });
    }

    fn deliver(&mut self, node: usize, delay: u64, event: Event) {
        self.work(node, delay, Work::Deliver(event));
    }

    /// The process on `node`, by name and incarnation: `n0#2`.
    fn who(&self, node: usize) -> String {
        format!("{}#{}", self.nodes[node].name, self.nodes[node].inc)
    }

    /// The node's wall clock: true time, plus the machine's skew, plus
    /// whatever its process's clock was stepped by.
    fn wall(&self, node: usize) -> u64 {
        let skew = self.skews[node] + self.nodes[node].wall_step_ms;
        (WALL_EPOCH_MS as i64 + self.t as i64 + skew) as u64
    }

    fn mono(&self, node: usize) -> u64 {
        self.nodes[node].mono_base + self.t
    }

    pub fn run(mut self) -> Outcome {
        for node in 0..self.nodes.len() {
            let delay = self.net_rng.range(0, 50);
            self.after(delay, Item::Restart { node });
        }
        self.start_clients();
        self.start_faults();
        self.start_liveness();
        while let Some(((t, _), item)) = self.agenda.pop_first() {
            if matches!(item, Item::End) {
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
            Item::Proc { node, inc, work } => {
                let n = &self.nodes[node];
                if !n.up || n.inc != inc {
                    return;
                }
                match n.paused_until {
                    // A paused process runs this when it resumes, in order.
                    Some(until) => self.after(until - self.t, Item::Proc { node, inc, work }),
                    None => self.proc_work(node, work),
                }
            }
            Item::StoreApply {
                node,
                inc,
                req,
                answer_ms,
            } => self.store_apply(node, inc, req, answer_ms),
            Item::ClientNew => self.client_new(),
            Item::Arrive { node, op, via } => self.arrive(node, op, via),
            Item::Bounced { op, why } => self.bounced(op, why),
            Item::Redispatch { op } => self.redispatch(op),
            Item::Fault(fault) => self.fault(fault),
            Item::Restart { node } => self.restart(node),
            Item::Resume { node, inc } => self.resume(node, inc),
            Item::Heal { node, partition } => self.heal(node, partition),
            Item::Probe => self.probe(),
            Item::Audit => self.audit(),
            Item::GoalSend { goal } => self.goal_send(goal),
            Item::End => {}
        }
    }

    fn proc_work(&mut self, node: usize, work: Work) {
        match work {
            Work::Deliver(event) => self.drive(node, event),
            Work::Timer { slot, ordinal } => self.timer(node, slot, ordinal),
            Work::Uploaded { cell, epoch, len } => self.uploaded(node, cell, epoch, len),
            Work::UploadFailed { cell, epoch } => self.upload_failed(node, cell, epoch),
            Work::Upload { cell, epoch } => self.upload(node, &cell, epoch),
            Work::ProofDeadline { cell, epoch, op } => self.proof_deadline(node, cell, epoch, op),
            Work::ChainRead {
                op,
                cell,
                epoch,
                chain,
            } => self.chain_read(node, op, cell, epoch, chain),
            Work::LoadSample => self.sample_load(node),
            Work::HandlerCommit { request } => self.handler_commit(node, request),
            Work::HandlerEnd { request } => self.handler_end(node, request),
        }
    }

    /// Start a new process on `node`: a fresh core that first acquires its
    /// node lease.
    fn restart(&mut self, node: usize) {
        if self.nodes[node].up || (self.auditing && node < self.config.nodes) {
            return;
        }
        let inc = self.nodes[node].inc + 1;
        // Large enough that a suspend can hold the clock back without it
        // ever reaching zero.
        let mono_base = self.net_rng.range(1_000_000_000, 2_000_000_000);
        self.nodes[node] = Node {
            up: true,
            ..Node::new(node, inc, mono_base, self.core)
        };
        self.note(|| format!("start n{node}#{inc}"));
        let spec = NodeLeaseSpec {
            addr: self.nodes[node].name.clone(),
            peer_protocol: PEER_PROTOCOL,
            generation: format!("g{node}.{inc}"),
            resume_generation: None,
            ttl_ms: self.config.ttl_ms,
        };
        let event = Event::StartNodeLease {
            now_ms: 0,
            now_mono_ms: 0,
            spec,
        };
        self.drive(node, event);
        let delay = self.net_rng.range(1, LOAD_SAMPLE_MS);
        self.work(node, delay, Work::LoadSample);
    }

    /// Production's periodic sampler. The simulation has no memory to
    /// measure, so only the cell count varies, and the node never reads as
    /// pressured; the sample still drives idle eviction.
    fn sample_load(&mut self, node: usize) {
        let load = pressure::Load {
            resident_cells: self.nodes[node].state.occupied(),
            rss_bytes: 0,
            in_use_bytes: 0,
            cgroup_working_set_bytes: None,
            cgroup_current_bytes: None,
            container_reserved_bytes: 0,
        };
        let event = Event::LoadSampled {
            load,
            now_mono_ms: 0,
        };
        self.drive(node, event);
        self.work(node, LOAD_SAMPLE_MS, Work::LoadSample);
    }

    /// The process on `node` dies. Its open databases stay on disk as it
    /// left them, unless the disk is wiped too. Every request it held
    /// fails, and the supervisor starts a new process later.
    fn crash(&mut self, node: usize, wipe: bool) {
        if !self.nodes[node].up {
            return;
        }
        let who = self.who(node);
        self.note(|| format!("crash {who}{}", if wipe { " (disk wiped)" } else { "" }));
        let inc = self.nodes[node].inc;
        let dead = std::mem::replace(&mut self.nodes[node], Node::new(node, inc, 0, self.core));
        self.close_replicas(node, dead.replicas);
        if wipe {
            self.disks[node] = Disk::default();
        }
        self.fail_requests(node, dead.requests);
        if !self.auditing {
            self.schedule_restart(node);
        }
    }
    /// Run one event through a node's core and carry out its effects. Some
    /// effects complete inside the same step, as they do in `Actor::drive`;
    /// their events queue behind the current one.
    fn drive(&mut self, node: usize, event: Event) {
        let mut queue = VecDeque::from([event]);
        while let Some(mut event) = queue.pop_front() {
            if !self.nodes[node].up || self.nodes[node].halting {
                break;
            }
            shell::stamp(&mut event, self.wall(node), self.mono(node));
            self.record(node, "<-", &event);
            self.stats.events += 1;
            let effects = on_event(&mut self.nodes[node].state, event);
            for effect in effects {
                self.record(node, "->", &effect);
                self.execute(node, effect, &mut queue);
            }
        }
        // A halt is the process exiting: as far as the fleet can tell, a
        // crash that leaves its disk alone.
        if self.nodes[node].halting {
            self.halts.push(self.who(node));
            self.crash(node, false);
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

    fn finish(mut self) -> Outcome {
        // A run that panicked stopped early: its goals prove nothing.
        if self.panic.is_none() {
            self.judge_goals();
        }
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
            halts: self.halts,
            authoritative: names(&|n| n.state.node_authoritative()),
            trace: self.trace,
            panic: self.panic,
            violations: self.checker.violations,
            stats: self.stats,
        }
    }
}

fn core_config(config: &SimConfig) -> Config {
    Config {
        max_resident: config.max_resident,
        max_activations: config.max_activations,
        max_evictions: 2,
        max_releases: 2,
        max_outbound_websockets: 32,
        ownership_on_evict: if config.sticky_eviction {
            OwnershipOnEvict::Sticky
        } else {
            OwnershipOnEvict::Release
        },
        require_node_lease: true,
        peer_protocol: PEER_PROTOCOL,
        operation_deadline_ms: Some(OPERATION_DEADLINE_MS),
        owner_log_recovery_backoff_ms: 1_000,
        owner_log_recovery_attempts: 240,
        alarm_resident_ms: 3_600_000,
        idle_evict_ms: config.idle_evict_ms,
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
