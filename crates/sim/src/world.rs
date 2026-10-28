//! The simulated fleet: processes, their disks, the network, the clients,
//! and the shell that turns each core's effects into later events.
//!
//! Everything happens on one simulated millisecond clock. The agenda is an
//! ordered map keyed by `(time, sequence)`, so two items due at the same
//! instant run in the order they were scheduled, and the whole run is a
//! function of the seed.
//!
//! The shell follows `crates/celld/actor.rs` effect by effect. Where the
//! production shell runs a task and reports later, this one schedules the
//! report after a drawn latency; where production completes an effect inside
//! the same step (`Publish`, the in-memory adoption refusal), this one does
//! too. Each part of the shell has its module: [`process`], the effects in
//! [`shell`], the bucket calls in [`store_rpc`], databases and their
//! replication in [`replica`], [`requests`], [`clients`], [`faults`] and
//! the phases, alarms and their wake entries in [`alarms`], the fleet
//! durability tier in [`node_log`], graceful shutdowns and balancing in
//! [`lifecycle`], the handoff of released cells in [`handoff`], and
//! eviction stops and the operator's evictions in [`stops`], and sockets,
//! gates and Worker requests on resident runtimes in [`resident`]. The
//! checkers in [`checks`] only read what the shell reports.

mod alarms;
mod checks;
mod clients;
mod faults;
mod handoff;
mod lifecycle;
mod node_log;
mod process;
mod replica;
mod requests;
mod resident;
mod shell;
mod stops;
mod store_rpc;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};

use celld_logic::{
    format::BUCKET_FORMAT, on_event, pressure, CapacityPeer, CasGuard, CasOutcome, Channel, Config,
    Effect, Epoch, Event, Failure, LeaseCasOutcome, LocalCell, NodeLeaseRecord, NodeLeaseSpec,
    OpId, OwnerRecord, OwnershipOnEvict, Phase, ProofSource, RequestId, RestoreOutcome,
    RestoreSpec, Route, State, StopCause, Timer,
};

use crate::store::{Session, Store};
use crate::{Coverage, Mutation, Outcome, ProcId, Rng, SimConfig, Stats, Violation, WriteId};
use alarms::{AlarmLedger, AlarmOp, ArmGate, ArmWaiter, ScanKind, WakeShell};
use checks::{Checker, View};
use faults::FaultKind;
use handoff::{Accept, Handoff};
use lifecycle::{LifeTask, Shutdown};
use node_log::{Fragment, LogShell, LogWork, RecoverMode};
use replica::{Proof, Waiter};
use resident::Socket;
use stops::{Asked, Stop};
use store_rpc::{Call, Reply, StoreReq, StoreRes};

type Time = u64;

const PEER_PROTOCOL: u16 = 1;
const WALL_EPOCH_MS: u64 = 1_800_000_000_000;
const OPERATION_DEADLINE_MS: u64 = 15_000;
/// The replicator's durability deadline for one proof.
const DURABILITY_TIMEOUT_MS: u64 = 10_000;
const CLIENT_TIMEOUT_MS: u64 = 20_000;
const PROBE_WINDOW_MS: u64 = 30_000;
/// How long past a deploy's maximum age a stale cell may still be swapping.
const GENERATION_SLACK_MS: u64 = 5_000;
const LOAD_SAMPLE_MS: u64 = 1_000;

pub fn run(seed: u64, config: &SimConfig) -> Outcome {
    let mut world = World::new(seed, config.clone());
    world.run();
    world.finish()
}

// ---------------------------------------------------------------------------
// Agenda items

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Slot {
    Renew,
    Fence,
    Alarm(String),
    Deadline(OpId),
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

/// Work that runs inside one process. It waits while the process is paused
/// and vanishes with it when it crashes.
#[derive(Debug)]
enum Work {
    Deliver(Event),
    Timer {
        slot: Slot,
        ordinal: u64,
    },
    StoreReply {
        call: u64,
    },
    HandlerCommit {
        request: RequestId,
    },
    HandlerEnd {
        request: RequestId,
    },
    ActivityFinished {
        request: RequestId,
    },
    UploadKick {
        cell: String,
        epoch: Epoch,
    },
    WaiterDeadline {
        cell: String,
        epoch: Epoch,
        op: OpId,
    },
    /// An eviction's capture ended, or a failed stop's retry pause did.
    StopStep {
        cell: String,
        epoch: Epoch,
        op: OpId,
    },
    LoadTick,
    /// A client sends a message on its socket.
    SocketMessage {
        websocket: u64,
    },
    /// A client closes its socket.
    SocketClose {
        websocket: u64,
    },
    /// A resident Worker request ends.
    WorkerEnd {
        request: RequestId,
    },
    /// An admitted output gate's effect has left.
    GateEnd {
        gate: RequestId,
    },
    ClientArrive {
        op: u64,
        attempt: u32,
        request: RequestId,
        epoch: Option<Epoch>,
    },
    /// A peer answered a forwarded request with `409 stale route`.
    StaleRoute {
        op: u64,
        attempt: u32,
        cell: String,
        node: String,
        epoch: Epoch,
    },
    /// The ingress resolves the attempt again.
    Redispatch {
        op: u64,
        attempt: u32,
    },
    AlarmEnd {
        op: OpId,
    },
    /// The handler's `AlarmFinished` reaches the core.
    AlarmAnswer(Event),
    /// A wake-maintenance durability proof resolved.
    TaskProven {
        task: u64,
        ok: bool,
    },
    TaskDeadline {
        task: u64,
    },
    BootScan {
        waited_ms: u64,
    },
    WakeTick {
        cleanup: bool,
    },
    Log(LogWork),
    ShutdownTick,
    TokenRetry,
    RebalanceTick,
    HandoffRetry {
        op: OpId,
    },
    HandoffDeadline {
        op: OpId,
    },
    HandoffTimeout {
        op: OpId,
        attempt: u64,
    },
    /// A donor's handoff request reaches this process.
    HandoffArrive {
        donor: ProcId,
        op: OpId,
        attempt: u64,
        cell: String,
        released_epoch: Epoch,
    },
    HandoffAnswer {
        op: OpId,
        attempt: u64,
        answer: Option<handoff::Answer>,
    },
}

#[derive(Debug)]
enum What {
    Proc { proc: ProcId, work: Work },
    StoreApply { call: u64 },
    ClientNew,
    ClientRetry { op: u64 },
    ClientTimeout { op: u64, attempt: u32 },
    Restart { node: usize },
    Resume { proc: ProcId },
    Heal { node: usize, partition: u64 },
    Fault(FaultKind),
    Shutdown,
    EvictTick,
    SocketTick,
    WorkerTick,
    ChaosEnd,
    Probe,
    CloseLogs,
    Audit,
    AlarmCheck,
    End,
}

// ---------------------------------------------------------------------------
// Processes and nodes

#[derive(Default)]
struct Disk {
    live: BTreeMap<(String, Epoch), Vec<WriteId>>,
    evicted: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// The node-log fragments this node holds as a follower, by session.
    peerlog: BTreeMap<Session, Fragment>,
    /// `.clean-reload.json`: the generation a clean shutdown certified.
    clean_reload: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PartitionMode {
    /// Requests hang until the client times out: an ambiguous failure.
    Hang,
    /// Connections are refused: a definite failure.
    Refuse,
}

struct Node {
    name: String,
    disk: Disk,
    proc: Option<ProcId>,
    partition: Option<(u64, PartitionMode)>,
    skew_ms: i64,
    restart_pending: bool,
}

#[derive(Debug)]
struct Replica {
    content: Vec<WriteId>,
    /// Content present when the epoch opened.
    start_len: usize,
    /// Position the bucket holds for this epoch's lineage.
    uploaded: usize,
    /// Position a credited bundle covers, and the position every member of
    /// the ensemble confirmed: the bucket and fleet proofs of the node log.
    bundled: usize,
    fleet: usize,
    /// Position the node log has taken from this replica.
    queued: usize,
    uploading: bool,
    running: bool,
    waiters: Vec<Waiter>,
    stop: Option<Stop>,
}

#[derive(Debug)]
struct ReqCtx {
    op: u64,
    attempt: u32,
    cell: String,
    epoch: Option<Epoch>,
    /// The route a forwarding ingress dispatched this request on, or `None`
    /// when a client sent it here directly.
    via: Option<Epoch>,
    write: bool,
    alarm: Option<AlarmIntent>,
    committed: Option<(WriteId, u64)>,
    arm_gate: ArmGate,
    /// The output gate this handler raised and waits for.
    gate: Option<RequestId>,
    answer: Answer,
    aborted: bool,
    activity: Activity,
    /// The client has its answer: the core released the reply, or the
    /// request failed. The context outlives it for late events.
    replied: bool,
}

/// How far a request's reply has got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    /// The handler is running.
    Pending,
    /// The handler ended, and its reply waits behind the wake-entry PUT.
    HeldByArmGate,
    /// The handler ended, and its reply waits for its output gate.
    HeldByGate,
    /// The reply went to the core.
    Sent,
}

/// When the request's `ActivityGuard` drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Activity {
    Running,
    /// Once the core releases the reply.
    OnRelease,
    Finished,
}

/// What a request does to its cell's alarm, besides its write.
#[derive(Clone, Copy, Debug)]
enum AlarmIntent {
    /// `setAlarm` for this instant of simulated time, in the handling
    /// process's wall clock.
    Set {
        due: Time,
    },
    Clear,
}

/// Whether a process runs. Paused, its work waits; suspended, its monotonic
/// clock stands still as well, as a suspended VM's does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pause {
    Running,
    Paused,
    Suspended { since: Time },
}

struct Proc {
    node: usize,
    state: State,
    pause: Pause,
    deferred: Vec<Work>,
    start_t: Time,
    mono_base: u64,
    /// Monotonic time lost to suspends that ended.
    suspended_ms: u64,
    wall_step_ms: i64,
    timers: BTreeMap<Slot, (u64, Timer, u64)>,
    next_ordinal: u64,
    replicas: BTreeMap<(String, Epoch), Replica>,
    published: BTreeMap<String, Epoch>,
    requests: BTreeMap<RequestId, ReqCtx>,
    wake: WakeShell,
    log: LogShell,
    shutdown: Option<Shutdown>,
    /// The generation a clean-reload certificate named at startup.
    resume_generation: Option<String>,
    life_tasks: BTreeMap<u64, LifeTask>,
    /// The first readiness gate opened (`fleet_ready`).
    fleet_ready: bool,
    /// The wall instant the last balancing batch could have reached every
    /// lease.
    rebalance_settled_ms: u64,
    /// `AdoptReleased` tasks in flight, by operation.
    handoffs: BTreeMap<OpId, Handoff>,
    /// The handoff directory: when the peers were read, and what they were.
    directory: Option<(Time, Vec<CapacityPeer>)>,
    /// Handoff requests from donors, by core request.
    accepts: BTreeMap<RequestId, Accept>,
    /// Operators' evictions waiting for their answers.
    evictions: BTreeMap<RequestId, Asked>,
    /// The host refuses to release any cell before this instant.
    wedged_until: Time,
    /// When this process last adopted a generation.
    deployed_at: Time,
    /// Client sockets open on this process.
    sockets: BTreeMap<u64, Socket>,
    /// Worker requests running on residents: the cell and epoch.
    workers: BTreeMap<RequestId, (String, Epoch)>,
    /// Output gates raised, by gate request, with the handler they hold.
    gates: BTreeMap<RequestId, RequestId>,
    halting: bool,
}

impl Proc {
    fn mono(&self, t: Time) -> u64 {
        let suspended = self.suspended_ms
            + match self.pause {
                Pause::Suspended { since } => t.saturating_sub(since),
                Pause::Running | Pause::Paused => 0,
            };
        self.mono_base + t.saturating_sub(self.start_t).saturating_sub(suspended)
    }
}

// ---------------------------------------------------------------------------
// Clients

#[derive(Debug)]
struct ClientOp {
    cell: String,
    write: bool,
    alarm: Option<AlarmIntent>,
    deadline: Time,
    attempt: u32,
    done: bool,
    retry: bool,
    ingress: Option<usize>,
    succeeded: bool,
    dispatch: Dispatch,
    /// The process and socket a socket message arrives on.
    socket: Option<(ProcId, u64)>,
}

/// The ingress node's forwarding loop for one client attempt
/// (`dispatch_fetch` in `crates/celld/main.rs`): a forwarded request is never
/// forwarded again, and the ingress decides whether to re-resolve.
#[derive(Debug, Default)]
struct Dispatch {
    proc: ProcId,
    deadline: Time,
    retried_not_owner: bool,
    retried_never_connected: bool,
    wait_ms: u64,
}

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

struct World {
    seed: u64,
    config: SimConfig,
    t: Time,
    seq: u64,
    agenda: BTreeMap<(Time, u64), What>,
    store: Store,
    nodes: Vec<Node>,
    procs: BTreeMap<ProcId, Proc>,
    next_proc: ProcId,
    calls: BTreeMap<u64, Call>,
    next_call: u64,
    ops: BTreeMap<u64, ClientOp>,
    next_op: u64,
    next_request: RequestId,
    next_websocket: u64,
    next_write: WriteId,
    next_partition: u64,
    cells: Vec<String>,
    chaos: bool,
    audit: bool,
    workload: bool,
    probe_ops: Vec<u64>,
    audit_ops: Vec<u64>,
    work_rng: Rng,
    fault_rng: Rng,
    net_rng: Rng,
    /// What each write does to its cell's alarm.
    alarm_ops: AlarmLedger,
    next_task: u64,
    audit_proc: Option<ProcId>,
    checker: Checker,
    /// Violations already written to the trace.
    noted: usize,
    stats: Stats,
    coverage: Coverage,
    hash: Fnv,
    trace: Vec<String>,
    panic: Option<String>,
}

impl World {
    fn new(seed: u64, config: SimConfig) -> Self {
        let mut master = Rng::new(seed);
        let work_rng = master.fork();
        let mut fault_rng = master.fork();
        let net_rng = master.fork();
        let skew = config.faults.max_skew_ms as i64;
        let nodes = (0..config.nodes)
            .map(|index| Node {
                name: format!("n{index}"),
                disk: Disk::default(),
                proc: None,
                partition: None,
                skew_ms: if skew == 0 {
                    0
                } else {
                    fault_rng.range(0, 2 * skew as u64) as i64 - skew
                },
                restart_pending: false,
            })
            .collect();
        let cells = (0..config.cells).map(|index| format!("c{index}")).collect();
        Self {
            seed,
            store: Store::new(config.mutation),
            config,
            t: 0,
            seq: 0,
            agenda: BTreeMap::new(),
            nodes,
            procs: BTreeMap::new(),
            next_proc: 1,
            calls: BTreeMap::new(),
            next_call: 1,
            ops: BTreeMap::new(),
            next_op: 1,
            next_request: 1,
            next_websocket: 1,
            next_write: 1,
            next_partition: 1,
            cells,
            chaos: true,
            audit: false,
            workload: true,
            probe_ops: Vec::new(),
            audit_ops: Vec::new(),
            work_rng,
            fault_rng,
            net_rng,
            alarm_ops: AlarmLedger::default(),
            next_task: 1,
            audit_proc: None,
            checker: Checker::default(),
            noted: 0,
            stats: Stats::default(),
            coverage: Coverage::default(),
            hash: Fnv(0xcbf2_9ce4_8422_2325),
            trace: Vec::new(),
            panic: None,
        }
    }

    fn at(&mut self, t: Time, what: What) {
        self.seq += 1;
        self.agenda.insert((t, self.seq), what);
    }

    fn after(&mut self, delay: u64, what: What) {
        self.at(self.t + delay, what);
    }

    fn work(&mut self, proc: ProcId, delay: u64, work: Work) {
        self.after(delay, What::Proc { proc, work });
    }

    fn note(&mut self, line: impl FnOnce() -> String) {
        if self.config.trace {
            let line = line();
            self.trace.push(format!("{:>7} {line}", self.t));
        }
    }

    /// The checker and what it may read, borrowed apart from the rest.
    fn checks(&mut self) -> (&mut Checker, View<'_>) {
        let view = View {
            t: self.t,
            store: &self.store,
            nodes: &self.nodes,
            ledger: &self.alarm_ops,
        };
        (&mut self.checker, view)
    }

    fn note_violations(&mut self) {
        if !self.config.trace {
            return;
        }
        let found = &self.checker.violations()[self.noted..];
        self.noted += found.len();
        for violation in found {
            self.trace.push(format!(
                "{:>7} VIOLATION {}: {}",
                self.t, violation.kind, violation.detail
            ));
        }
    }

    // -----------------------------------------------------------------------
    // The run

    fn run(&mut self) {
        for node in 0..self.nodes.len() {
            let delay = self.net_rng.range(0, 50);
            self.at(delay, What::Restart { node });
            self.nodes[node].restart_pending = true;
        }
        self.at(1, What::ClientNew);
        if self.config.evict_gap_ms.is_some() {
            self.at(100, What::EvictTick);
        }
        if self.config.resident.sockets > 0.0 {
            self.at(200, What::SocketTick);
        }
        if self.config.resident.workers > 0.0 {
            self.at(300, What::WorkerTick);
        }
        if self.config.lifecycle.shutdowns > 0.0 {
            let gap = self.fault_gap(self.config.lifecycle.shutdowns);
            self.at(gap, What::Shutdown);
        }
        let faults = self.config.faults.clone();
        for (kind, rate) in [
            (FaultKind::Crash, faults.crashes),
            (FaultKind::Pause, faults.pauses),
            (FaultKind::Partition, faults.partitions),
            (FaultKind::ClockStep, faults.clock_steps),
            (FaultKind::Wedge, self.config.stops.wedges),
            (FaultKind::Deploy, self.config.deploys.rate),
        ] {
            if rate > 0.0 {
                let gap = self.fault_gap(rate);
                self.at(gap, What::Fault(kind));
            }
        }
        let chaos_end = self.config.chaos_ms;
        let probe = chaos_end + self.config.settle_ms;
        self.at(chaos_end, What::ChaosEnd);
        self.at(probe, What::Probe);
        let audit = self.audit_start();
        if self.config.fleet {
            self.at(audit - node_log::CLOSE_LEAD_MS, What::CloseLogs);
        }
        self.at(audit, What::Audit);
        self.at(audit + self.alarm_window(), What::AlarmCheck);
        let audit_end = audit + self.alarm_window() + 4 * self.config.ttl_ms + PROBE_WINDOW_MS;
        self.at(audit_end + 1_000, What::End);

        while let Some(((t, _), what)) = self.agenda.pop_first() {
            self.t = t;
            if matches!(what, What::End) {
                break;
            }
            self.dispatch(what);
            self.note_violations();
            if self.panic.is_some() {
                break;
            }
        }
    }

    fn audit_start(&self) -> Time {
        self.config.chaos_ms + self.config.settle_ms + PROBE_WINDOW_MS + 1_000
    }

    /// How long the audit node runs with no requests, so only wake entries
    /// can bring a cell back for its alarm: the dead fleet's leases expire,
    /// a due scan finds the entry, and the takeover restores the cell.
    fn alarm_window(&self) -> u64 {
        2 * self.config.ttl_ms + 3 * self.config.wake_tick_ms + 10_000 + self.log_grace()
    }

    /// How overdue an alarm must be, at the end of that window, before its
    /// absence counts as lost.
    fn alarm_grace(&self) -> u64 {
        self.config.ttl_ms + 2 * self.config.wake_tick_ms + 5_000 + self.log_grace()
    }

    /// In the fleet posture a takeover may wait for node-log recovery,
    /// which gives up on an unreachable follower only once its lease has
    /// been dead this long.
    fn log_grace(&self) -> u64 {
        if self.config.fleet {
            self.dead_member_grace()
        } else {
            0
        }
    }

    fn fault_gap(&mut self, per_minute: f64) -> u64 {
        let mean = (60_000.0 / per_minute) as u64;
        self.fault_rng.range(1, 2 * mean.max(1))
    }

    fn dispatch(&mut self, what: What) {
        match what {
            What::Proc { proc, work } => self.proc_work(proc, work),
            What::StoreApply { call } => self.store_apply(call),
            What::ClientNew => self.client_new(),
            What::ClientRetry { op } => self.client_send(op),
            What::ClientTimeout { op, attempt } => self.client_timeout(op, attempt),
            What::Restart { node } => self.restart(node),
            What::Resume { proc } => self.resume(proc),
            What::Heal { node, partition } => {
                if self.nodes[node]
                    .partition
                    .is_some_and(|(id, _)| id == partition)
                {
                    self.nodes[node].partition = None;
                    self.note(|| format!("heal partition of n{node}"));
                }
            }
            What::Fault(kind) => self.fault(kind),
            What::Shutdown => self.shutdown_fault(),
            What::EvictTick => self.evict_tick(),
            What::SocketTick => self.socket_tick(),
            What::WorkerTick => self.worker_tick(),
            What::ChaosEnd => self.chaos_end(),
            What::Probe => self.probe(),
            // The fleet shuts down gracefully before the audit kills it.
            What::CloseLogs => self.close_logs(),
            What::Audit => self.start_audit(),
            What::AlarmCheck => {
                if let Some(audit) = self.audit_proc {
                    let grace = self.alarm_grace();
                    self.audit_alarms(audit, grace);
                }
                self.audit_reads();
            }
            What::End => {}
        }
    }

    // -----------------------------------------------------------------------
    // Driving the core

    /// Run one event, then every event the shell completes inside the same
    /// step, the way the Actor's drive loop does.
    fn drive(&mut self, id: ProcId, event: Event) {
        let mut queue = VecDeque::from([event]);
        while let Some(mut event) = queue.pop_front() {
            if !self.procs.contains_key(&id) {
                return;
            }
            let (now_ms, mono) = (self.wall(id), self.mono(id));
            stamp(&mut event, now_ms, mono);
            let _ = write!(self.hash, "{} {id} {event:?}", self.t);
            if self.config.trace {
                self.trace.push(format!("{:>7} p{id} <- {event:?}", self.t));
            }
            self.stats.events += 1;
            self.coverage.event(&event);
            let state = &mut self.procs.get_mut(&id).unwrap().state;
            let effects = match catch_unwind(AssertUnwindSafe(|| on_event(state, event))) {
                Ok(effects) => effects,
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "non-string panic".into());
                    self.panic = Some(format!("t={} p{id}: {message}", self.t));
                    return;
                }
            };
            for effect in effects {
                let _ = write!(self.hash, "{effect:?}");
                self.coverage.effect(&effect);
                if self.config.trace {
                    self.trace
                        .push(format!("{:>7} p{id} -> {effect:?}", self.t));
                }
                self.execute(id, effect, &mut queue);
                if !self.procs.contains_key(&id) {
                    return;
                }
            }
        }
        if self
            .procs
            .get(&id)
            .is_some_and(|proc| !proc.accepts.is_empty())
        {
            self.settle_accepts(id);
        }
        if self.procs.get(&id).is_some_and(|proc| proc.halting) {
            self.stats.halts += 1;
            self.crash(id, false);
        }
    }

    // -----------------------------------------------------------------------
    // Final checks

    fn finish(mut self) -> Outcome {
        if self.panic.is_none() {
            let served = [("probe", &self.probe_ops), ("audit", &self.audit_ops)]
                .into_iter()
                .flat_map(|(phase, ops)| ops.iter().map(move |op| (phase, op)))
                .map(|(phase, op)| {
                    let entry = &self.ops[op];
                    (phase, entry.cell.clone(), entry.succeeded)
                })
                .collect();
            self.checker.finish(self.t, served);
        }
        let violations = self.checker.into_violations();
        let _ = write!(self.hash, "{violations:?}");
        Outcome {
            seed: self.seed,
            violations,
            stats: self.stats,
            coverage: self.coverage,
            trace_hash: self.hash.0,
            trace: self.trace,
            panic: self.panic,
        }
    }
}

/// Write the sampling instants into an event at the moment the process sees
/// it, as the production shell samples its clocks when a task completes.
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
        }
        | Event::WebSocketRequestAt {
            now_ms,
            now_mono_ms,
            ..
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::OutputAt { now_mono_ms, .. }
        | Event::LoadSampled { now_mono_ms, .. }
        | Event::EvictRequested { now_mono_ms, .. }
        | Event::GateRequestAt { now_mono_ms, .. } => *now_mono_ms = mono,
        Event::OwnerRead { now_ms, .. } | Event::NodeLeaseRead { now_ms, .. } => *now_ms = wall,
        Event::AlarmObserved {
            now_ms,
            now_mono_ms,
            ..
        }
        | Event::AlarmFinished {
            now_ms,
            now_mono_ms,
            ..
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        Event::WakeHintAt { now_mono_ms, .. } => *now_mono_ms = mono,
        Event::HandoffRequestAt {
            now_ms,
            now_mono_ms,
            ..
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        Event::NudgeNodeLease {
            now_ms,
            now_mono_ms,
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        _ => {}
    }
}
