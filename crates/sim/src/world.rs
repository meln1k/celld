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
//! too. The replicator and the restore source rule follow
//! `crates/celld/ltx_repl.rs`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};

use celld_logic::{
    format::BUCKET_FORMAT, on_event, pressure, CapacityPeer, CasGuard, CasOutcome, Channel, Config,
    Effect, Epoch, Event, Failure, LeaseCasOutcome, LocalCell, NodeLeaseRecord, NodeLeaseSpec,
    OpId, OwnerRecord, OwnershipOnEvict, ProofSource, RequestId, RestoreOutcome, RestoreSpec,
    Route, State, StopCause, Timer,
};

use crate::store::Store;
use crate::{Mutation, Outcome, ProcId, Rng, SimConfig, Stats, Violation, WriteId};

type Time = u64;

const PEER_PROTOCOL: u16 = 1;
const WALL_EPOCH_MS: u64 = 1_800_000_000_000;
const OPERATION_DEADLINE_MS: u64 = 15_000;
/// The replicator's durability deadline for one proof.
const DURABILITY_TIMEOUT_MS: u64 = 10_000;
const CLIENT_TIMEOUT_MS: u64 = 20_000;
const PROBE_WINDOW_MS: u64 = 30_000;
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
    LoadTick,
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
    EvictTick,
    ChaosEnd,
    Probe,
    Audit,
    End,
}

#[derive(Clone, Copy, Debug)]
enum FaultKind {
    Crash,
    Pause,
    Partition,
    ClockStep,
}

// ---------------------------------------------------------------------------
// Store calls

#[derive(Debug)]
enum StoreReq {
    ReadOwner {
        cell: String,
    },
    CasOwner {
        cell: String,
        guard: CasGuard,
        epoch: Epoch,
        takeover: bool,
    },
    ReleaseOwner {
        cell: String,
        epoch: Epoch,
    },
    ReadLease {
        node: String,
    },
    CasLease {
        guard: CasGuard,
        record: NodeLeaseRecord,
        resident: usize,
        owned: usize,
    },
    ReadPeers,
    Put {
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    ReadChain {
        cell: String,
        epoch: Epoch,
    },
}

#[derive(Debug)]
enum StoreRes {
    Owner(Option<OwnerRecord>),
    Cas(CasOutcome),
    Lease(Option<NodeLeaseRecord>),
    LeaseCas(LeaseCasOutcome),
    Peers {
        view_ms: u64,
        peers: Vec<CapacityPeer>,
    },
    Put,
    Chain(Option<Vec<WriteId>>),
}

/// What the reply becomes in the calling process.
#[derive(Debug)]
enum Reply {
    OwnerRead(OpId),
    OwnerCas(OpId),
    OwnerReleased(OpId),
    SelfLease(OpId),
    NodeLease(OpId),
    LeaseCas(OpId),
    Peers(OpId),
    Verify {
        op: OpId,
        epoch: Epoch,
    },
    Upload {
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    Restore {
        op: OpId,
        cell: String,
        spec: RestoreSpec,
    },
}

#[derive(Debug)]
struct Call {
    proc: ProcId,
    node: usize,
    req: StoreReq,
    reply: Reply,
    /// Filled when the request reaches the store.
    result: Option<StoreRes>,
    /// The caller's view: a definite failure, a lost exchange, or the result.
    failure: Option<Failure>,
}

// ---------------------------------------------------------------------------
// Processes and nodes

#[derive(Default)]
struct Disk {
    live: BTreeMap<(String, Epoch), Vec<WriteId>>,
    evicted: BTreeMap<(String, Epoch), Vec<WriteId>>,
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
enum Waiter {
    Await { op: OpId, position: u64 },
    Ensure { op: OpId, position: u64 },
}

#[derive(Debug)]
struct Stop {
    op: OpId,
    preserve: bool,
}

#[derive(Debug)]
struct Replica {
    content: Vec<WriteId>,
    /// Content present when the epoch opened.
    start_len: usize,
    /// Position the bucket holds for this epoch's lineage.
    uploaded: usize,
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
    committed: Option<(WriteId, u64)>,
    aborted: bool,
    output_sent: bool,
    activity_finished: bool,
    finish_on_release: bool,
}

struct Proc {
    node: usize,
    state: State,
    paused: Option<bool>,
    deferred: Vec<Work>,
    start_t: Time,
    mono_base: u64,
    suspended_ms: u64,
    suspend_started: Option<Time>,
    wall_step_ms: i64,
    timers: BTreeMap<Slot, (u64, Timer, u64)>,
    next_ordinal: u64,
    replicas: BTreeMap<(String, Epoch), Replica>,
    published: BTreeMap<String, Epoch>,
    requests: BTreeMap<RequestId, ReqCtx>,
    halting: bool,
}

impl Proc {
    fn mono(&self, t: Time) -> u64 {
        let suspended = self.suspended_ms
            + self
                .suspend_started
                .map_or(0, |since| t.saturating_sub(since));
        self.mono_base + t.saturating_sub(self.start_t).saturating_sub(suspended)
    }
}

// ---------------------------------------------------------------------------
// Clients and checker records

#[derive(Debug)]
struct ClientOp {
    cell: String,
    write: bool,
    deadline: Time,
    attempt: u32,
    done: bool,
    retry: bool,
    ingress: Option<usize>,
    succeeded: bool,
    dispatch: Dispatch,
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

#[derive(Debug)]
struct Ack {
    t: Time,
    cell: String,
    epoch: Epoch,
    write: WriteId,
}

#[derive(Debug)]
struct RestoreRecord {
    t: Time,
    proc: ProcId,
    cell: String,
    epoch: Epoch,
    source: &'static str,
    content: BTreeSet<WriteId>,
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
    acks: Vec<Ack>,
    restores: Vec<RestoreRecord>,
    runtime_owner: BTreeMap<(String, Epoch), ProcId>,
    flagged: BTreeSet<String>,
    violations: Vec<Violation>,
    stats: Stats,
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
            acks: Vec::new(),
            restores: Vec::new(),
            runtime_owner: BTreeMap::new(),
            flagged: BTreeSet::new(),
            violations: Vec::new(),
            stats: Stats::default(),
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

    fn violation(&mut self, kind: &'static str, key: String, detail: String) {
        if self.flagged.insert(format!("{kind}/{key}")) {
            self.note(|| format!("VIOLATION {kind}: {detail}"));
            self.violations.push(Violation {
                at_ms: self.t,
                kind,
                detail,
            });
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
        let faults = self.config.faults.clone();
        for (kind, rate) in [
            (FaultKind::Crash, faults.crashes),
            (FaultKind::Pause, faults.pauses),
            (FaultKind::Partition, faults.partitions),
            (FaultKind::ClockStep, faults.clock_steps),
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
        self.at(probe + PROBE_WINDOW_MS + 1_000, What::Audit);
        let audit_end = probe + PROBE_WINDOW_MS + 1_000 + 4 * self.config.ttl_ms + PROBE_WINDOW_MS;
        self.at(audit_end + 1_000, What::End);

        while let Some(((t, _), what)) = self.agenda.pop_first() {
            self.t = t;
            if matches!(what, What::End) {
                break;
            }
            self.dispatch(what);
            if self.panic.is_some() {
                break;
            }
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
            What::EvictTick => self.evict_tick(),
            What::ChaosEnd => self.chaos_end(),
            What::Probe => self.probe(),
            What::Audit => self.start_audit(),
            What::End => {}
        }
    }

    // -----------------------------------------------------------------------
    // Processes

    fn restart(&mut self, node: usize) {
        self.nodes[node].restart_pending = false;
        if self.nodes[node].proc.is_some() || (self.audit && node < self.config.nodes) {
            return;
        }
        let id = self.next_proc;
        self.next_proc += 1;
        let name = self.nodes[node].name.clone();
        let config = self.core_config();
        let mono_base = self.net_rng.range(1_000, 1_000_000);
        self.procs.insert(
            id,
            Proc {
                node,
                state: State::new(name.clone(), config),
                paused: None,
                deferred: Vec::new(),
                start_t: self.t,
                mono_base,
                suspended_ms: 0,
                suspend_started: None,
                wall_step_ms: 0,
                timers: BTreeMap::new(),
                next_ordinal: 0,
                replicas: BTreeMap::new(),
                published: BTreeMap::new(),
                requests: BTreeMap::new(),
                halting: false,
            },
        );
        self.nodes[node].proc = Some(id);
        self.note(|| format!("start p{id} on {name}"));
        let spec = NodeLeaseSpec {
            addr: name,
            peer_protocol: PEER_PROTOCOL,
            generation: format!("g{id}"),
            resume_generation: None,
            ttl_ms: self.config.ttl_ms,
        };
        self.drive(
            id,
            Event::StartNodeLease {
                now_ms: 0,
                now_mono_ms: 0,
                spec,
            },
        );
        let tick = self.net_rng.range(1, LOAD_SAMPLE_MS);
        self.work(id, tick, Work::LoadTick);
    }

    fn core_config(&self) -> Config {
        Config {
            max_resident: self.config.max_resident,
            max_activations: self.config.max_activations,
            max_evictions: 2,
            max_releases: 2,
            max_outbound_websockets: 32,
            ownership_on_evict: if self.config.sticky_eviction {
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
            idle_evict_ms: self.config.idle_evict_ms,
            pressure: pressure::PressureConfig::from_limits(None, Some(0)),
        }
    }

    fn crash(&mut self, id: ProcId, wipe: bool) {
        let Some(proc) = self.procs.remove(&id) else {
            return;
        };
        let node = proc.node;
        self.nodes[node].proc = None;
        // The open databases stay on disk exactly as the process left them,
        // with whatever it committed and never uploaded.
        for ((cell, epoch), replica) in proc.replicas {
            self.nodes[node]
                .disk
                .live
                .insert((cell, epoch), replica.content);
        }
        if wipe {
            self.nodes[node].disk = Disk::default();
        }
        self.note(|| format!("crash p{id}{}", if wipe { " (disk wiped)" } else { "" }));
        if !self.audit && !self.nodes[node].restart_pending {
            // The supervisor waits at least one lease lifetime.
            let ttl = self.config.ttl_ms;
            let delay = self.fault_rng.range(ttl, 3 * ttl);
            self.nodes[node].restart_pending = true;
            self.after(delay, What::Restart { node });
        }
    }

    fn resume(&mut self, id: ProcId) {
        let t = self.t;
        let Some(proc) = self.procs.get_mut(&id) else {
            return;
        };
        if proc.paused.is_none() {
            return;
        }
        proc.paused = None;
        if let Some(since) = proc.suspend_started.take() {
            proc.suspended_ms += t - since;
        }
        let deferred = std::mem::take(&mut proc.deferred);
        self.note(|| format!("resume p{id}"));
        for work in deferred {
            self.work(id, 0, work);
        }
    }

    fn wall(&self, id: ProcId) -> u64 {
        let proc = &self.procs[&id];
        let wall = WALL_EPOCH_MS as i64
            + self.t as i64
            + self.nodes[proc.node].skew_ms
            + proc.wall_step_ms;
        wall.max(0) as u64
    }

    fn mono(&self, id: ProcId) -> u64 {
        self.procs[&id].mono(self.t)
    }

    fn proc_work(&mut self, id: ProcId, work: Work) {
        let Some(proc) = self.procs.get_mut(&id) else {
            return;
        };
        if proc.paused.is_some() {
            proc.deferred.push(work);
            return;
        }
        match work {
            Work::Deliver(event) => self.drive(id, event),
            Work::Timer { slot, ordinal } => self.timer(id, slot, ordinal),
            Work::StoreReply { call } => self.store_reply(id, call),
            Work::HandlerCommit { request } => self.handler_commit(id, request),
            Work::HandlerEnd { request } => self.handler_end(id, request),
            Work::ActivityFinished { request } => {
                let proc = self.procs.get_mut(&id).unwrap();
                if let Some(ctx) = proc.requests.get_mut(&request) {
                    if !ctx.activity_finished {
                        ctx.activity_finished = true;
                        self.drive(id, Event::ActivityFinished { request });
                    }
                }
            }
            Work::UploadKick { cell, epoch } => self.kick_upload(id, &cell, epoch),
            Work::WaiterDeadline { cell, epoch, op } => self.waiter_deadline(id, &cell, epoch, op),
            Work::LoadTick => {
                let proc = &self.procs[&id];
                let load = pressure::Load {
                    resident_cells: proc.state.residents().len(),
                    rss_bytes: 0,
                    in_use_bytes: 0,
                    cgroup_working_set_bytes: None,
                    cgroup_current_bytes: None,
                    container_reserved_bytes: 0,
                };
                self.drive(
                    id,
                    Event::LoadSampled {
                        load,
                        now_mono_ms: 0,
                    },
                );
                self.work(id, LOAD_SAMPLE_MS, Work::LoadTick);
            }
            Work::ClientArrive {
                op,
                attempt,
                request,
                epoch,
            } => self.client_arrive(id, op, attempt, request, epoch),
            Work::StaleRoute {
                op,
                attempt,
                cell,
                node,
                epoch,
            } => self.stale_route(id, op, attempt, cell, node, epoch),
            Work::Redispatch { op, attempt } => self.redispatch(id, op, attempt),
        }
    }

    fn timer(&mut self, id: ProcId, slot: Slot, ordinal: u64) {
        let mono = self.mono(id);
        let proc = self.procs.get_mut(&id).unwrap();
        let Some((armed, _, at_mono)) = proc.timers.get(&slot) else {
            return;
        };
        if *armed != ordinal {
            return;
        }
        if mono < *at_mono {
            // The process clock fell behind the world, by a suspend.
            let delay = *at_mono - mono;
            self.work(id, delay, Work::Timer { slot, ordinal });
            return;
        }
        let (_, timer, _) = proc.timers.remove(&slot).unwrap();
        self.drive(
            id,
            Event::TimerFired {
                timer,
                now_ms: 0,
                now_mono_ms: 0,
            },
        );
    }

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
        if self.procs.get(&id).is_some_and(|proc| proc.halting) {
            self.stats.halts += 1;
            self.crash(id, false);
        }
    }

    // -----------------------------------------------------------------------
    // The shell

    fn execute(&mut self, id: ProcId, effect: Effect, immediate: &mut VecDeque<Event>) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                let mono = self.mono(id);
                let proc = self.procs.get_mut(&id).unwrap();
                let slot = Slot::of(&timer);
                let ordinal = proc.next_ordinal;
                proc.next_ordinal += 1;
                proc.timers
                    .insert(slot.clone(), (ordinal, timer, at_mono_ms));
                self.work(
                    id,
                    at_mono_ms.saturating_sub(mono),
                    Work::Timer { slot, ordinal },
                );
            }
            Effect::ReadSelfNodeLease { op } => {
                let node = self.node_name(id);
                self.store_call(id, StoreReq::ReadLease { node }, Reply::SelfLease(op));
            }
            Effect::CasNodeLease {
                op, guard, record, ..
            } => {
                let state = &self.procs[&id].state;
                let (resident, owned) = (state.residents().len(), state.owned_cells());
                self.store_call(
                    id,
                    StoreReq::CasLease {
                        guard,
                        record,
                        resident,
                        owned,
                    },
                    Reply::LeaseCas(op),
                );
            }
            Effect::ReadLocalCells => {
                let node = self.procs[&id].node;
                let disk = &self.nodes[node].disk;
                let mut cells: Vec<LocalCell> = disk
                    .live
                    .keys()
                    .map(|(cell, epoch)| LocalCell {
                        id: cell.clone(),
                        epoch: *epoch,
                        live: true,
                    })
                    .chain(disk.evicted.keys().map(|(cell, epoch)| LocalCell {
                        id: cell.clone(),
                        epoch: *epoch,
                        live: false,
                    }))
                    .collect();
                cells.sort();
                let delay = self.net_rng.range(1, 5);
                self.work(
                    id,
                    delay,
                    Work::Deliver(Event::LocalCellsRead { result: Ok(cells) }),
                );
            }
            Effect::ReadOwner { op, cell } => {
                self.store_call(id, StoreReq::ReadOwner { cell }, Reply::OwnerRead(op))
            }
            Effect::ReadNodeLease { op, owner, .. } => self.store_call(
                id,
                StoreReq::ReadLease { node: owner },
                Reply::NodeLease(op),
            ),
            Effect::RecoverNodeLog { op, .. } => {
                // No fleet log in this model: every lease carries no log
                // state, so nothing is ever left to recover.
                immediate.push_back(Event::NodeLogRecovered { op, result: Ok(()) });
            }
            Effect::ReadCapacityPeers { op, .. } => {
                self.store_call(id, StoreReq::ReadPeers, Reply::Peers(op))
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                takeover,
            } => self.store_call(
                id,
                StoreReq::CasOwner {
                    cell,
                    guard,
                    epoch,
                    takeover,
                },
                Reply::OwnerCas(op),
            ),
            Effect::ReconcileWakeEntry { .. } => {}
            Effect::ReleaseOwner { op, cell, epoch } => self.store_call(
                id,
                StoreReq::ReleaseOwner { cell, epoch },
                Reply::OwnerReleased(op),
            ),
            Effect::AdoptReleased { op, .. } => {
                // As the in-memory adapter does: no successor acknowledges a
                // handoff, so released cells wait for their next request.
                immediate.push_back(Event::SuccessorAdopted {
                    op,
                    result: Err(Failure::Definite),
                });
            }
            Effect::CancelCellActivity { requests, .. } => {
                let proc = self.procs.get_mut(&id).unwrap();
                let mut ended = Vec::new();
                for request in requests {
                    if let Some(ctx) = proc.requests.get_mut(&request) {
                        ctx.aborted = true;
                        if !ctx.output_sent {
                            ended.push(request);
                        }
                    }
                }
                for request in ended {
                    self.work(id, 0, Work::HandlerEnd { request });
                }
            }
            Effect::Restore { op, cell, spec } => self.restore(id, op, cell, spec),
            Effect::StartRuntime { op, cell, epoch } => self.start_runtime(id, op, cell, epoch),
            Effect::Publish { op, cell, epoch } => {
                let others = self
                    .procs
                    .iter()
                    .any(|(other, proc)| *other != id && proc.published.contains_key(&cell));
                if others {
                    self.stats.concurrent_serving += 1;
                }
                self.procs
                    .get_mut(&id)
                    .unwrap()
                    .published
                    .insert(cell, epoch);
                immediate.push_back(Event::Published { op, result: Ok(()) });
            }
            Effect::EvictionAdmission { .. } | Effect::EvictionFinished { .. } => {}
            Effect::EnsureDurable {
                op, cell, epoch, ..
            } => {
                let proc = self.procs.get_mut(&id).unwrap();
                match proc.replicas.get_mut(&(cell.clone(), epoch)) {
                    Some(replica) => {
                        let position = replica.content.len() as u64;
                        replica.waiters.push(Waiter::Ensure { op, position });
                        self.work(
                            id,
                            DURABILITY_TIMEOUT_MS,
                            Work::WaiterDeadline {
                                cell: cell.clone(),
                                epoch,
                                op,
                            },
                        );
                        self.settle_waiters(id, &cell, epoch);
                        self.kick_upload(id, &cell, epoch);
                    }
                    None => self.work(
                        id,
                        1,
                        Work::Deliver(Event::DurabilityChecked {
                            op,
                            result: Err(Failure::Definite),
                        }),
                    ),
                }
            }
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => {
                if self.config.mutation == Mutation::AckBeforeUpload {
                    self.work(
                        id,
                        1,
                        Work::Deliver(Event::DurableReached {
                            op,
                            result: Ok(position),
                            source: ProofSource::Bucket,
                        }),
                    );
                    return;
                }
                let proc = self.procs.get_mut(&id).unwrap();
                match proc.replicas.get_mut(&(cell.clone(), epoch)) {
                    Some(replica) => {
                        replica.waiters.push(Waiter::Await { op, position });
                        self.work(
                            id,
                            DURABILITY_TIMEOUT_MS,
                            Work::WaiterDeadline {
                                cell: cell.clone(),
                                epoch,
                                op,
                            },
                        );
                        self.settle_waiters(id, &cell, epoch);
                        self.kick_upload(id, &cell, epoch);
                    }
                    None => self.work(
                        id,
                        1,
                        Work::Deliver(Event::DurableReached {
                            op,
                            result: Err(Failure::Ambiguous),
                            source: ProofSource::Bucket,
                        }),
                    ),
                }
            }
            Effect::VerifyOwnership { op, cell, epoch } => {
                if self.config.mutation == Mutation::SkipVerifyOwnership {
                    immediate.push_back(Event::OwnershipVerified { op, result: Ok(()) });
                } else {
                    self.store_call(
                        id,
                        StoreReq::ReadOwner { cell },
                        Reply::Verify { op, epoch },
                    );
                }
            }
            Effect::Release {
                request,
                channel,
                result,
            } => self.release(id, request, channel, result.is_ok()),
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
                bounded,
            } => self.stop_runtime(id, op, cell, epoch, cause, bounded, immediate),
            Effect::AbandonEvictionStop { op, cell } => {
                let proc = self.procs.get_mut(&id).unwrap();
                let found = proc
                    .replicas
                    .iter_mut()
                    .find(|((name, _), replica)| {
                        *name == cell && replica.stop.as_ref().is_some_and(|stop| stop.op == op)
                    })
                    .map(|(_, replica)| replica);
                if let Some(replica) = found {
                    // The final sync has not finished, so no handoff snapshot
                    // is visible yet: the stop can still give the cell back.
                    replica.stop = None;
                    self.work(id, 1, Work::Deliver(Event::RuntimeStopCancelled { op }));
                }
            }
            Effect::FireAlarm {
                op, cell, epoch, ..
            } => {
                immediate.push_back(Event::AlarmFinished {
                    op,
                    cell,
                    epoch,
                    now_ms: self.wall(id),
                    now_mono_ms: self.mono(id),
                    result: Ok((
                        celld_logic::wake::AlarmSnapshot::without_wake(None),
                        true,
                        None,
                    )),
                });
            }
            Effect::Complete { request, result } => self.complete(id, request, result),
            Effect::CompleteWorker { .. } | Effect::CloseWebSocket { .. } => {}
            Effect::Halt { .. } => {
                self.procs.get_mut(&id).unwrap().halting = true;
            }
        }
    }

    fn node_name(&self, id: ProcId) -> String {
        self.nodes[self.procs[&id].node].name.clone()
    }

    // -----------------------------------------------------------------------
    // The bucket and the network to it

    fn store_call(&mut self, id: ProcId, req: StoreReq, reply: Reply) {
        let call = self.next_call;
        self.next_call += 1;
        let node = self.procs[&id].node;
        let faults = &self.config.faults;
        let (lo, hi) = faults.store_latency_ms;
        let mut request_ms = self.net_rng.range(lo, hi);
        let mut response_ms = self.net_rng.range(lo, hi);
        let timeout = faults.store_timeout_ms;
        let mut failure = None;
        let mut applies = true;
        if self.chaos {
            if self.net_rng.chance(faults.store_spike_probability) {
                request_ms += self.net_rng.range(0, faults.store_spike_ms);
            }
            if self.net_rng.chance(faults.store_spike_probability) {
                response_ms += self.net_rng.range(0, faults.store_spike_ms);
            }
            if self.net_rng.chance(faults.store_definite) {
                failure = Some(Failure::Definite);
                applies = false;
            } else if self.net_rng.chance(faults.store_lost_request) {
                failure = Some(Failure::Ambiguous);
                applies = false;
            } else if self.net_rng.chance(faults.store_lost_response) {
                failure = Some(Failure::Ambiguous);
            }
        }
        match self.nodes[node].partition {
            Some((_, PartitionMode::Refuse)) => {
                failure = Some(Failure::Definite);
                applies = false;
            }
            Some((_, PartitionMode::Hang)) => {
                failure = Some(Failure::Ambiguous);
                applies = false;
            }
            None => {}
        }
        if request_ms + response_ms > timeout && failure.is_none() {
            // The client gives up first. The request may still land later.
            failure = Some(Failure::Ambiguous);
        }
        let reply_ms = match failure {
            None => request_ms + response_ms,
            Some(Failure::Definite) => request_ms.min(timeout),
            Some(Failure::Ambiguous) => timeout,
        };
        if applies {
            self.after(request_ms, What::StoreApply { call });
        }
        self.calls.insert(
            call,
            Call {
                proc: id,
                node,
                req,
                reply,
                result: None,
                failure,
            },
        );
        self.work(id, reply_ms.max(1), Work::StoreReply { call });
    }

    fn store_apply(&mut self, call: u64) {
        let Some(entry) = self.calls.get(&call) else {
            return;
        };
        // The writer may be gone by now; its request is still on the wire.
        let proc = entry.proc;
        let sender = entry.node;
        let node = self.nodes[sender].name.clone();
        let result = match &entry.req {
            StoreReq::ReadOwner { cell } => StoreRes::Owner(self.store.read_owner(cell)),
            StoreReq::CasOwner {
                cell,
                guard,
                epoch,
                takeover,
            } => {
                let (cell, guard, epoch, takeover) =
                    (cell.clone(), guard.clone(), *epoch, *takeover);
                let outcome = self.store.cas_owner(&cell, &guard, epoch, &node, proc);
                if outcome == CasOutcome::Applied && takeover {
                    self.stats.takeovers += 1;
                }
                StoreRes::Cas(outcome)
            }
            StoreReq::ReleaseOwner { cell, epoch } => {
                let (cell, epoch) = (cell.clone(), *epoch);
                StoreRes::Cas(self.store.release_owner(&cell, epoch, &node))
            }
            StoreReq::ReadLease { node } => StoreRes::Lease(self.store.read_lease(node)),
            StoreReq::CasLease {
                guard,
                record,
                resident,
                owned,
            } => {
                let sampled_ms = record.expires_ms.saturating_sub(self.config.ttl_ms);
                let (guard, record, resident, owned) =
                    (guard.clone(), record.clone(), *resident, *owned);
                StoreRes::LeaseCas(
                    self.store
                        .cas_lease(&guard, &record, resident, owned, sampled_ms),
                )
            }
            StoreReq::ReadPeers => {
                let view_ms = self.call_wall(proc, sender);
                let peers = self
                    .store
                    .leases()
                    .map(|lease| CapacityPeer {
                        node: lease.record.node.clone(),
                        addr: lease.record.addr.clone(),
                        expires_ms: lease.record.expires_ms,
                        peer_protocol: lease.record.peer_protocol,
                        sampled_ms: lease.sampled_ms,
                        owned_cells: Some(lease.owned),
                        placement_weight: Some(1),
                        bucket_format: Some(BUCKET_FORMAT),
                        resident_cells: lease.resident,
                        host_websockets: 0,
                        rss_bytes: 0,
                        in_use_bytes: Some(0),
                        pressured: false,
                        memory_headroom: Some(true),
                        restoring: 0,
                        paced_handoff: false,
                        rebalance_paused: false,
                        draining: false,
                    })
                    .collect();
                StoreRes::Peers { view_ms, peers }
            }
            StoreReq::Put {
                cell,
                epoch,
                content,
            } => {
                let (cell, epoch, content) = (cell.clone(), *epoch, content.clone());
                if self
                    .store
                    .owner_epoch(&cell)
                    .is_some_and(|owner| owner > epoch)
                {
                    self.stats.stale_uploads += 1;
                }
                let writers = self.store.put_ltx(&cell, epoch, &content, proc);
                if writers > 1 {
                    self.violation(
                        "two-writers",
                        format!("prefix/{cell}/{epoch}"),
                        format!(
                            "{writers} processes uploaded into the prefix of {cell} epoch {epoch}"
                        ),
                    );
                }
                StoreRes::Put
            }
            StoreReq::ReadChain { cell, epoch } => {
                StoreRes::Chain(self.store.read_chain(cell, *epoch))
            }
        };
        self.calls.get_mut(&call).unwrap().result = Some(result);
    }

    fn call_wall(&self, proc: ProcId, node: usize) -> u64 {
        if self.procs.contains_key(&proc) {
            self.wall(proc)
        } else {
            (WALL_EPOCH_MS as i64 + self.t as i64 + self.nodes[node].skew_ms).max(0) as u64
        }
    }

    fn store_reply(&mut self, id: ProcId, call: u64) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call {
            req,
            reply,
            result,
            failure,
            ..
        } = entry;
        // A lost response still applied; the caller cannot know it did.
        let result = match failure {
            Some(failure) => Err(failure),
            None => Ok(result.expect("a successful call reached the store")),
        };
        let _ = req;
        let event = match reply {
            Reply::OwnerRead(op) => Event::OwnerRead {
                op,
                now_ms: 0,
                result: result.map(|res| match res {
                    StoreRes::Owner(record) => record,
                    _ => unreachable!(),
                }),
            },
            Reply::OwnerCas(op) => Event::OwnerCasCompleted {
                op,
                result: result.map(|res| match res {
                    StoreRes::Cas(outcome) => outcome,
                    _ => unreachable!(),
                }),
            },
            Reply::OwnerReleased(op) => Event::OwnerReleased {
                op,
                result: result.map(|res| match res {
                    StoreRes::Cas(outcome) => outcome,
                    _ => unreachable!(),
                }),
            },
            Reply::SelfLease(op) => Event::SelfNodeLeaseRead {
                op,
                now_ms: 0,
                now_mono_ms: 0,
                result: result.map(|res| match res {
                    StoreRes::Lease(record) => record,
                    _ => unreachable!(),
                }),
            },
            Reply::NodeLease(op) => Event::NodeLeaseRead {
                op,
                now_ms: 0,
                result: result.map(|res| match res {
                    StoreRes::Lease(record) => record,
                    _ => unreachable!(),
                }),
            },
            Reply::LeaseCas(op) => Event::NodeLeaseCasCompleted {
                op,
                now_mono_ms: 0,
                result: result.map(|res| match res {
                    StoreRes::LeaseCas(outcome) => outcome,
                    _ => unreachable!(),
                }),
                stamped_log_state: None,
            },
            Reply::Peers(op) => {
                let wall = self.wall(id);
                match result {
                    Ok(StoreRes::Peers { view_ms, peers }) => Event::CapacityPeersRead {
                        op,
                        now_ms: view_ms,
                        result: Ok(peers),
                    },
                    Err(failure) => Event::CapacityPeersRead {
                        op,
                        now_ms: wall,
                        result: Err(failure),
                    },
                    Ok(_) => unreachable!(),
                }
            }
            Reply::Verify { op, epoch } => {
                let node = self.node_name(id);
                let result = match result {
                    Ok(StoreRes::Owner(Some(record)))
                        if record.node.as_deref() == Some(node.as_str())
                            && record.epoch == epoch =>
                    {
                        Ok(())
                    }
                    Ok(_) => Err(Failure::Definite),
                    Err(failure) => Err(failure),
                };
                Event::OwnershipVerified { op, result }
            }
            Reply::Upload { cell, epoch, len } => {
                let proc = self.procs.get_mut(&id).unwrap();
                if let Some(replica) = proc.replicas.get_mut(&(cell.clone(), epoch)) {
                    replica.uploading = false;
                    if result.is_ok() {
                        replica.uploaded = replica.uploaded.max(len);
                        self.settle_waiters(id, &cell, epoch);
                        self.kick_upload(id, &cell, epoch);
                    } else {
                        let delay = self.net_rng.range(50, 300);
                        self.work(id, delay, Work::UploadKick { cell, epoch });
                    }
                }
                return;
            }
            Reply::Restore { op, cell, spec } => match result {
                Ok(StoreRes::Chain(chain)) => {
                    self.stats.restores_bucket += 1;
                    let restored = chain.is_some();
                    self.open_replica(id, &cell, spec.epoch, chain.unwrap_or_default(), "bucket");
                    Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored,
                            alarm: None,
                        }),
                    }
                }
                Ok(_) => unreachable!(),
                Err(_) => Event::RestoreCompleted {
                    op,
                    result: Err(Failure::Definite),
                },
            },
        };
        self.drive(id, event);
    }

    // -----------------------------------------------------------------------
    // Restore, runtime, replication

    fn restore(&mut self, id: ProcId, op: OpId, cell: String, spec: RestoreSpec) {
        let node = self.procs[&id].node;
        let epoch = spec.epoch;
        let disk = &mut self.nodes[node].disk;
        let snapshot = if spec.fresh || spec.resume_local {
            None
        } else if self.config.mutation == Mutation::ReuseOlderSnapshot {
            disk.evicted
                .range((cell.clone(), 0)..(cell.clone(), epoch))
                .next_back()
                .map(|(key, _)| key.clone())
        } else if celld_logic::restore::previous_epoch_reusable(epoch, spec.took_over) {
            Some((cell.clone(), epoch - 1)).filter(|key| disk.evicted.contains_key(key))
        } else {
            None
        };
        let delay = self.net_rng.range(1, 5);
        if spec.resume_local {
            let content = disk
                .live
                .get(&(cell.clone(), epoch))
                .cloned()
                .unwrap_or_default();
            self.stats.restores_local += 1;
            self.open_replica(id, &cell, epoch, content, "resume");
            self.restore_done(id, op, delay, true);
        } else if let Some(key) = snapshot {
            let content = disk.evicted.remove(&key).unwrap();
            self.stats.restores_local += 1;
            self.open_replica(id, &cell, epoch, content, "snapshot");
            self.restore_done(id, op, delay, true);
        } else if !spec.fresh {
            self.store_call(
                id,
                StoreReq::ReadChain {
                    cell: cell.clone(),
                    epoch,
                },
                Reply::Restore { op, cell, spec },
            );
        } else {
            self.stats.restores_fresh += 1;
            self.open_replica(id, &cell, epoch, Vec::new(), "fresh");
            self.restore_done(id, op, delay, false);
        }
    }

    fn restore_done(&mut self, id: ProcId, op: OpId, delay: u64, restored: bool) {
        self.work(
            id,
            delay,
            Work::Deliver(Event::RestoreCompleted {
                op,
                result: Ok(RestoreOutcome {
                    restored,
                    alarm: None,
                }),
            }),
        );
    }

    fn open_replica(
        &mut self,
        id: ProcId,
        cell: &str,
        epoch: Epoch,
        content: Vec<WriteId>,
        source: &'static str,
    ) {
        self.restores.push(RestoreRecord {
            t: self.t,
            proc: id,
            cell: cell.to_string(),
            epoch,
            source,
            content: content.iter().copied().collect(),
        });
        let node = self.procs[&id].node;
        self.nodes[node]
            .disk
            .live
            .remove(&(cell.to_string(), epoch));
        let len = content.len();
        self.procs.get_mut(&id).unwrap().replicas.insert(
            (cell.to_string(), epoch),
            Replica {
                content,
                start_len: len,
                // The opening content is the chain this epoch was restored
                // from, already in the bucket below this epoch.
                uploaded: len,
                uploading: false,
                running: false,
                waiters: Vec::new(),
                stop: None,
            },
        );
    }

    fn start_runtime(&mut self, id: ProcId, op: OpId, cell: String, epoch: Epoch) {
        let key = (cell.clone(), epoch);
        match self.runtime_owner.get(&key) {
            Some(other) if *other != id => {
                let other = *other;
                self.violation(
                    "two-writers",
                    format!("runtime/{cell}/{epoch}"),
                    format!("p{id} started {cell} epoch {epoch}, which p{other} already ran"),
                );
            }
            _ => {
                self.runtime_owner.insert(key.clone(), id);
            }
        }
        let claimed = self
            .store
            .claims
            .iter()
            .any(|claim| claim.cell == cell && claim.epoch == epoch && claim.proc == id);
        if !claimed {
            self.violation(
                "unclaimed-runtime",
                format!("{cell}/{epoch}/{id}"),
                format!("p{id} started {cell} epoch {epoch} without an applied claim of its own"),
            );
        }
        let generation = self.procs[&id].state.current_generation();
        let proc = self.procs.get_mut(&id).unwrap();
        let result = match proc.replicas.get_mut(&key) {
            Some(replica) => {
                replica.running = true;
                Ok(())
            }
            None => Err(Failure::Definite),
        };
        let delay = self.net_rng.range(1, 10);
        self.work(
            id,
            delay,
            Work::Deliver(Event::RuntimeStarted {
                op,
                isolate: None,
                generation,
                result,
            }),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn stop_runtime(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: String,
        epoch: Epoch,
        cause: StopCause,
        bounded: bool,
        immediate: &mut VecDeque<Event>,
    ) {
        let key = (cell.clone(), epoch);
        let node = self.procs[&id].node;
        let proc = self.procs.get_mut(&id).unwrap();
        proc.published.remove(&cell);
        let Some(replica) = proc.replicas.get_mut(&key) else {
            immediate.push_back(Event::RuntimeStopped { op });
            return;
        };
        replica.running = false;
        match cause {
            StopCause::Swap => immediate.push_back(Event::RuntimeStopped { op }),
            StopCause::Fence => {
                // Close in place: no durability pass, the file stays live.
                let replica = proc.replicas.remove(&key).unwrap();
                self.nodes[node].disk.live.insert(key, replica.content);
                let delay = self.net_rng.range(1, 5);
                self.work(id, delay, Work::Deliver(Event::RuntimeStopped { op }));
            }
            StopCause::Reset => {
                proc.replicas.remove(&key);
                let delay = self.net_rng.range(1, 5);
                self.work(id, delay, Work::Deliver(Event::RuntimeStopped { op }));
            }
            StopCause::Cleanup | StopCause::Evict { .. } => {
                let preserve = !matches!(cause, StopCause::Evict { rebalance: true });
                replica.stop = Some(Stop { op, preserve });
                if bounded {
                    self.work(
                        id,
                        OPERATION_DEADLINE_MS,
                        Work::WaiterDeadline {
                            cell: cell.clone(),
                            epoch,
                            op,
                        },
                    );
                }
                self.settle_waiters(id, &cell, epoch);
                self.kick_upload(id, &cell, epoch);
            }
        }
    }

    fn kick_upload(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let Some(proc) = self.procs.get_mut(&id) else {
            return;
        };
        let Some(replica) = proc.replicas.get_mut(&(cell.to_string(), epoch)) else {
            return;
        };
        if replica.uploading || replica.content.len() <= replica.uploaded {
            return;
        }
        replica.uploading = true;
        let content = replica.content.clone();
        let len = content.len();
        self.store_call(
            id,
            StoreReq::Put {
                cell: cell.to_string(),
                epoch,
                content,
            },
            Reply::Upload {
                cell: cell.to_string(),
                epoch,
                len,
            },
        );
    }

    fn settle_waiters(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        let node = self.procs[&id].node;
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(replica) = proc.replicas.get_mut(&key) else {
            return;
        };
        let uploaded = replica.uploaded as u64;
        let mut events = Vec::new();
        replica.waiters.retain(|waiter| match waiter {
            Waiter::Await { op, position } if uploaded >= *position => {
                events.push(Event::DurableReached {
                    op: *op,
                    result: Ok(uploaded),
                    source: ProofSource::Bucket,
                });
                false
            }
            Waiter::Ensure { op, position } if uploaded >= *position => {
                events.push(Event::DurabilityChecked {
                    op: *op,
                    result: Ok(()),
                });
                false
            }
            _ => true,
        });
        let stopped = replica.stop.is_some() && uploaded >= replica.content.len() as u64;
        if stopped {
            let replica = proc.replicas.remove(&key).unwrap();
            let stop = replica.stop.unwrap();
            let disk = &mut self.nodes[node].disk;
            disk.live.remove(&key);
            if stop.preserve {
                disk.evicted.insert(key, replica.content);
            }
            events.push(Event::RuntimeStopped { op: stop.op });
            // Waiters on a stopped replica can no longer be proven here.
            for waiter in replica.waiters {
                events.push(match waiter {
                    Waiter::Await { op, .. } => Event::DurableReached {
                        op,
                        result: Err(Failure::Ambiguous),
                        source: ProofSource::Bucket,
                    },
                    Waiter::Ensure { op, .. } => Event::DurabilityChecked {
                        op,
                        result: Err(Failure::Ambiguous),
                    },
                });
            }
        }
        for event in events {
            let delay = self.net_rng.range(0, 2);
            self.work(id, delay, Work::Deliver(event));
        }
    }

    fn waiter_deadline(&mut self, id: ProcId, cell: &str, epoch: Epoch, op: OpId) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(replica) = proc.replicas.get_mut(&(cell.to_string(), epoch)) else {
            return;
        };
        let event = if replica.stop.as_ref().is_some_and(|stop| stop.op == op) {
            // The final sync ran out of time: the runtime is gone, the
            // database stays open, and the core restarts it in place.
            replica.stop = None;
            Some(Event::RuntimeStopFailed { op })
        } else if let Some(index) = replica.waiters.iter().position(|waiter| match waiter {
            Waiter::Await { op: o, .. } | Waiter::Ensure { op: o, .. } => *o == op,
        }) {
            Some(match replica.waiters.remove(index) {
                Waiter::Await { op, .. } => Event::DurableReached {
                    op,
                    result: Err(Failure::Ambiguous),
                    source: ProofSource::Bucket,
                },
                Waiter::Ensure { op, .. } => Event::DurabilityChecked {
                    op,
                    result: Err(Failure::Ambiguous),
                },
            })
        } else {
            None
        };
        if let Some(event) = event {
            self.drive(id, event);
        }
    }

    // -----------------------------------------------------------------------
    // Requests and handlers

    fn complete(
        &mut self,
        id: ProcId,
        request: RequestId,
        result: Result<Route, celld_logic::RequestError>,
    ) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        match result {
            Ok(Route::Local) => {
                self.stats.local_routes += 1;
                let epoch = proc.published.get(&ctx.cell).copied();
                let Some(epoch) = epoch else {
                    let cell = ctx.cell.clone();
                    self.violation(
                        "local-without-runtime",
                        format!("{id}/{request}"),
                        format!("p{id} routed request {request} for {cell} locally with no published runtime"),
                    );
                    // Finish the request so the core is not left waiting.
                    self.work(id, 0, Work::HandlerEnd { request });
                    return;
                };
                ctx.epoch = Some(epoch);
                let write = ctx.write;
                let commit = self.work_rng.range(0, 10);
                let end = commit + self.work_rng.range(1, 30);
                if write {
                    self.work(id, commit, Work::HandlerCommit { request });
                }
                self.work(id, end, Work::HandlerEnd { request });
            }
            Ok(Route::Remote { node, epoch, .. }) => {
                self.stats.remote_routes += 1;
                let ctx = proc.requests.remove(&request).unwrap();
                match ctx.via {
                    None => self.forward(id, ctx, node, epoch),
                    Some(via) => {
                        let own = self.node_name(id);
                        self.reply_stale(ctx, own, via);
                    }
                }
            }
            Err(error) => {
                self.stats.route_errors += 1;
                let ctx = proc.requests.remove(&request).unwrap();
                if let (Some(via), celld_logic::RequestError::CapacityExhausted) = (ctx.via, error)
                {
                    // The peer answers with the same stale-route header.
                    let own = self.node_name(id);
                    self.reply_stale(ctx, own, via);
                } else {
                    self.attempt_failed(ctx.op, ctx.attempt);
                }
            }
        }
    }

    /// The peer's `409 stale route` travels back to the ingress.
    fn reply_stale(&mut self, ctx: ReqCtx, node: String, epoch: Epoch) {
        let Some(entry) = self.ops.get(&ctx.op) else {
            return;
        };
        let ingress = entry.dispatch.proc;
        let delay = self.net_rng.range(1, 5);
        self.work(
            ingress,
            delay,
            Work::StaleRoute {
                op: ctx.op,
                attempt: ctx.attempt,
                cell: ctx.cell,
                node,
                epoch,
            },
        );
    }

    fn stale_route(
        &mut self,
        id: ProcId,
        op: u64,
        attempt: u32,
        cell: String,
        node: String,
        epoch: Epoch,
    ) {
        let t = self.t;
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        if t >= entry.dispatch.deadline {
            self.attempt_failed(op, attempt);
            return;
        }
        let wait = if epoch == 0 {
            // A refused candidate did not run the request and costs no
            // retry budget; invalidation excludes that load sample.
            25
        } else if !entry.dispatch.retried_not_owner {
            entry.dispatch.retried_not_owner = true;
            let wait = entry.dispatch.wait_ms.max(10);
            entry.dispatch.wait_ms = (wait * 2).min(250);
            wait
        } else {
            self.attempt_failed(op, attempt);
            return;
        };
        self.drive(id, Event::InvalidateRemote { cell, node, epoch });
        self.work(id, wait, Work::Redispatch { op, attempt });
    }

    fn redispatch(&mut self, id: ProcId, op: u64, attempt: u32) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        let request = self.next_request;
        self.next_request += 1;
        self.client_arrive(id, op, attempt, request, None);
    }

    fn forward(&mut self, from: ProcId, ctx: ReqCtx, node: String, epoch: Epoch) {
        let target = self.nodes.iter().position(|n| n.name == node);
        let from_node = self.procs[&from].node;
        let reachable = target.and_then(|index| {
            let blocked =
                self.nodes[index].partition.is_some() || self.nodes[from_node].partition.is_some();
            if blocked {
                None
            } else {
                self.nodes[index].proc
            }
        });
        match reachable {
            Some(proc) => {
                let request = self.next_request;
                self.next_request += 1;
                let delay = self.net_rng.range(1, 5);
                self.work(
                    proc,
                    delay,
                    Work::ClientArrive {
                        op: ctx.op,
                        attempt: ctx.attempt,
                        request,
                        epoch: Some(epoch),
                    },
                );
            }
            None => {
                // The connection was never established, so nothing ran:
                // one redispatch after invalidating the route.
                let Some(entry) = self.ops.get_mut(&ctx.op) else {
                    return;
                };
                if entry.done || entry.attempt != ctx.attempt {
                    return;
                }
                if entry.dispatch.retried_never_connected {
                    self.attempt_failed(ctx.op, ctx.attempt);
                    return;
                }
                entry.dispatch.retried_never_connected = true;
                let cell = ctx.cell.clone();
                self.drive(from, Event::InvalidateRemote { cell, node, epoch });
                let delay = self.net_rng.range(1, 20);
                self.work(
                    from,
                    delay,
                    Work::Redispatch {
                        op: ctx.op,
                        attempt: ctx.attempt,
                    },
                );
            }
        }
    }

    fn handler_commit(&mut self, id: ProcId, request: RequestId) {
        let write = self.next_write;
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        let Some(epoch) = ctx.epoch else {
            return;
        };
        if ctx.aborted || proc.published.get(&ctx.cell) != Some(&epoch) {
            return;
        }
        let cell = ctx.cell.clone();
        let Some(replica) = proc.replicas.get_mut(&(cell.clone(), epoch)) else {
            return;
        };
        if !replica.running {
            return;
        }
        self.next_write += 1;
        replica.content.push(write);
        ctx.committed = Some((write, replica.content.len() as u64));
        self.stats.writes += 1;
        self.kick_upload(id, &cell, epoch);
    }

    fn handler_end(&mut self, id: ProcId, request: RequestId) {
        let finish_first = self.work_rng.chance(0.5);
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        if ctx.output_sent {
            return;
        }
        ctx.output_sent = true;
        if ctx.aborted || ctx.epoch.is_none() {
            ctx.activity_finished = true;
            let (op, attempt) = (ctx.op, ctx.attempt);
            self.drive(id, Event::ActivityFinished { request });
            self.attempt_failed(op, attempt);
            return;
        }
        let position = ctx.committed.map(|(_, position)| position);
        let observed = if position.is_some() {
            None
        } else {
            let epoch = ctx.epoch.unwrap();
            proc.replicas
                .get(&(ctx.cell.clone(), epoch))
                .filter(|replica| replica.content.len() > replica.start_len)
                .map(|replica| replica.content.len() as u64)
        };
        ctx.finish_on_release = !finish_first;
        if finish_first {
            ctx.activity_finished = true;
        }
        self.drive(
            id,
            Event::OutputAt {
                request,
                channel: Channel::Response,
                position,
                observed,
                epoch: None,
                now_mono_ms: 0,
            },
        );
        if finish_first {
            self.drive(id, Event::ActivityFinished { request });
        }
    }

    fn release(&mut self, id: ProcId, request: RequestId, channel: Channel, ok: bool) {
        if channel != Channel::Response {
            return;
        }
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        let (op, attempt, cell, epoch, committed) = (
            ctx.op,
            ctx.attempt,
            ctx.cell.clone(),
            ctx.epoch,
            ctx.committed,
        );
        let finish = ctx.finish_on_release && !ctx.activity_finished;
        if finish {
            let delay = self.work_rng.range(0, 5);
            self.work(id, delay, Work::ActivityFinished { request });
        }
        if !ok {
            self.stats.refused_outputs += 1;
            self.attempt_failed(op, attempt);
            return;
        }
        if let (Some((write, position)), Some(epoch)) = (committed, epoch) {
            let in_prefix = self
                .store
                .prefix(&cell, epoch)
                .and_then(|prefix| prefix.entries.get(&position))
                == Some(&write);
            if !in_prefix {
                self.violation(
                    "ack-before-durable",
                    format!("{write}"),
                    format!(
                        "p{id} acknowledged write {write} of {cell} epoch {epoch} at position \
                         {position}, which the epoch's prefix does not hold"
                    ),
                );
            }
            self.stats.acked_writes += 1;
            self.acks.push(Ack {
                t: self.t,
                cell,
                epoch,
                write,
            });
        }
        self.attempt_succeeded(op, attempt);
    }

    // -----------------------------------------------------------------------
    // Clients

    fn client_new(&mut self) {
        if !self.workload {
            return;
        }
        let cell = self.cells[self.work_rng.below(self.cells.len() as u64) as usize].clone();
        let write = self.work_rng.chance(self.config.write_probability);
        let op = self.new_op(cell, write, self.t + CLIENT_TIMEOUT_MS, false, None);
        self.client_send(op);
        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        self.after(gap, What::ClientNew);
    }

    fn new_op(
        &mut self,
        cell: String,
        write: bool,
        deadline: Time,
        retry: bool,
        ingress: Option<usize>,
    ) -> u64 {
        let op = self.next_op;
        self.next_op += 1;
        self.ops.insert(
            op,
            ClientOp {
                cell,
                write,
                deadline,
                attempt: 0,
                done: false,
                retry,
                ingress,
                succeeded: false,
                dispatch: Dispatch::default(),
            },
        );
        op
    }

    fn client_send(&mut self, op: u64) {
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        if entry.done {
            return;
        }
        entry.attempt += 1;
        let attempt = entry.attempt;
        let ingress = entry.ingress;
        self.stats.requests += 1;
        let candidates: Vec<usize> = match ingress {
            Some(node) => vec![node],
            None => (0..self.nodes.len())
                .filter(|node| self.nodes[*node].proc.is_some())
                .collect(),
        };
        let target = self
            .work_rng
            .pick(&candidates)
            .and_then(|node| self.nodes[*node].proc);
        let timeout = if self.ops[&op].retry {
            5_000
        } else {
            CLIENT_TIMEOUT_MS
        };
        self.after(timeout, What::ClientTimeout { op, attempt });
        match target {
            Some(proc) => {
                let deadline = self.t + OPERATION_DEADLINE_MS;
                self.ops.get_mut(&op).unwrap().dispatch = Dispatch {
                    proc,
                    deadline,
                    ..Dispatch::default()
                };
                let request = self.next_request;
                self.next_request += 1;
                let delay = self.net_rng.range(1, 3);
                self.work(
                    proc,
                    delay,
                    Work::ClientArrive {
                        op,
                        attempt,
                        request,
                        epoch: None,
                    },
                );
            }
            None => self.attempt_failed(op, attempt),
        }
    }

    fn client_arrive(
        &mut self,
        id: ProcId,
        op: u64,
        attempt: u32,
        request: RequestId,
        via: Option<Epoch>,
    ) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        let (cell, write) = (entry.cell.clone(), entry.write);
        self.procs.get_mut(&id).unwrap().requests.insert(
            request,
            ReqCtx {
                op,
                attempt,
                cell: cell.clone(),
                epoch: None,
                via,
                write,
                committed: None,
                aborted: false,
                output_sent: false,
                activity_finished: false,
                finish_on_release: false,
            },
        );
        let event = if via == Some(0) {
            Event::CapacityRequestAt {
                request,
                cell,
                now_mono_ms: 0,
            }
        } else {
            Event::RequestAt {
                request,
                cell,
                now_mono_ms: 0,
            }
        };
        self.drive(id, event);
    }

    fn client_timeout(&mut self, op: u64, attempt: u32) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        self.attempt_failed(op, attempt);
    }

    fn attempt_failed(&mut self, op: u64, attempt: u32) {
        let t = self.t;
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        if entry.retry && t < entry.deadline {
            // Bump the attempt so late answers to this one are ignored.
            entry.attempt += 1;
            let delay = self.work_rng.range(200, 500);
            self.after(delay, What::ClientRetry { op });
        } else {
            entry.done = true;
        }
    }

    fn attempt_succeeded(&mut self, op: u64, attempt: u32) {
        if let Some(entry) = self.ops.get_mut(&op) {
            if !entry.done && entry.attempt == attempt {
                entry.done = true;
                entry.succeeded = true;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Faults and phases

    fn fault(&mut self, kind: FaultKind) {
        if !self.chaos {
            return;
        }
        let faults = self.config.faults.clone();
        let live: Vec<usize> = (0..self.config.nodes)
            .filter(|node| self.nodes[*node].proc.is_some())
            .collect();
        if let Some(&node) = self.fault_rng.pick(&live) {
            let id = self.nodes[node].proc.unwrap();
            match kind {
                FaultKind::Crash => {
                    let wipe = self.fault_rng.chance(faults.wipe_probability);
                    self.stats.crashes += 1;
                    self.crash(id, wipe);
                }
                FaultKind::Pause => {
                    let duration = self.fault_rng.range(10, faults.max_pause_ms.max(10));
                    let suspend = self.fault_rng.chance(faults.suspend_probability);
                    let t = self.t;
                    let proc = self.procs.get_mut(&id).unwrap();
                    if proc.paused.is_none() {
                        proc.paused = Some(suspend);
                        if suspend {
                            proc.suspend_started = Some(t);
                        }
                        self.stats.pauses += 1;
                        self.note(|| {
                            format!(
                                "{} p{id} for {duration} ms",
                                if suspend { "suspend" } else { "pause" }
                            )
                        });
                        self.after(duration, What::Resume { proc: id });
                    }
                }
                FaultKind::Partition => {
                    if self.nodes[node].partition.is_none() {
                        let mode = if self.fault_rng.chance(0.5) {
                            PartitionMode::Hang
                        } else {
                            PartitionMode::Refuse
                        };
                        let partition = self.next_partition;
                        self.next_partition += 1;
                        self.nodes[node].partition = Some((partition, mode));
                        let duration = self.fault_rng.range(100, faults.max_partition_ms.max(100));
                        self.stats.partitions += 1;
                        self.note(|| format!("partition n{node} ({mode:?}) for {duration} ms"));
                        self.after(duration, What::Heal { node, partition });
                    }
                }
                FaultKind::ClockStep => {
                    let step = faults.max_clock_step_ms as i64;
                    let delta = self.fault_rng.range(0, 2 * step as u64) as i64 - step;
                    self.procs.get_mut(&id).unwrap().wall_step_ms += delta;
                    self.note(|| format!("step wall clock of p{id} by {delta} ms"));
                }
            }
        }
        let rate = match kind {
            FaultKind::Crash => faults.crashes,
            FaultKind::Pause => faults.pauses,
            FaultKind::Partition => faults.partitions,
            FaultKind::ClockStep => faults.clock_steps,
        };
        let gap = self.fault_gap(rate);
        self.after(gap, What::Fault(kind));
    }

    fn evict_tick(&mut self) {
        let Some((lo, hi)) = self.config.evict_gap_ms else {
            return;
        };
        let gap = self.work_rng.range(lo, hi);
        self.after(gap, What::EvictTick);
        if self.audit {
            return;
        }
        let live: Vec<ProcId> = self
            .procs
            .iter()
            .filter(|(_, proc)| proc.paused.is_none())
            .map(|(id, _)| *id)
            .collect();
        let Some(&id) = self.work_rng.pick(&live) else {
            return;
        };
        let residents = self.procs[&id].state.residents();
        if let Some(cell) = self.work_rng.pick(&residents).cloned() {
            self.stats.evictions_requested += 1;
            self.drive(id, Event::Evict { cell });
        }
    }

    fn chaos_end(&mut self) {
        self.chaos = false;
        self.note(|| "chaos ends: faults heal".into());
        for node in &mut self.nodes {
            node.partition = None;
        }
        let paused: Vec<ProcId> = self
            .procs
            .iter()
            .filter(|(_, proc)| proc.paused.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in paused {
            self.resume(id);
        }
    }

    fn probe(&mut self) {
        self.workload = false;
        self.note(|| "probe: every cell must take a write".into());
        let deadline = self.t + PROBE_WINDOW_MS;
        for cell in self.cells.clone() {
            let op = self.new_op(cell, true, deadline, true, None);
            self.probe_ops.push(op);
            self.client_send(op);
        }
    }

    fn start_audit(&mut self) {
        self.audit = true;
        self.note(|| "audit: the fleet dies; a fresh node restores every cell".into());
        let procs: Vec<ProcId> = self.procs.keys().copied().collect();
        for id in procs {
            self.crash(id, true);
        }
        let node = self.nodes.len();
        self.nodes.push(Node {
            name: "audit".into(),
            disk: Disk::default(),
            proc: None,
            partition: None,
            skew_ms: 0,
            restart_pending: false,
        });
        self.restart(node);
        let deadline = self.t + 4 * self.config.ttl_ms + PROBE_WINDOW_MS;
        for cell in self.cells.clone() {
            let op = self.new_op(cell, false, deadline, true, Some(node));
            self.audit_ops.push(op);
            let delay = self.work_rng.range(10, 100);
            self.after(delay, What::ClientRetry { op });
        }
    }

    // -----------------------------------------------------------------------
    // Final checks

    fn finish(mut self) -> Outcome {
        if self.panic.is_none() {
            self.check_restores();
            for (phase, ops) in [
                ("probe", self.probe_ops.clone()),
                ("audit", self.audit_ops.clone()),
            ] {
                for op in ops {
                    let entry = &self.ops[&op];
                    if !entry.succeeded {
                        let cell = entry.cell.clone();
                        self.violation(
                            "liveness",
                            format!("{phase}/{cell}"),
                            format!("{phase}: {cell} did not serve a request within the window after faults healed"),
                        );
                    }
                }
            }
        }
        let _ = write!(self.hash, "{:?}", self.violations);
        Outcome {
            seed: self.seed,
            violations: self.violations,
            stats: self.stats,
            trace_hash: self.hash.0,
            trace: self.trace,
            panic: self.panic,
        }
    }

    /// No lost acknowledged write: every restore at epoch `E` holds every
    /// write acknowledged at an epoch below `E`. An acknowledgement verified
    /// the ownership record after its upload, so a later claim, and the
    /// restore that follows it, reads a lineage that contains the write,
    /// whenever the acknowledgement itself is delivered.
    fn check_restores(&mut self) {
        let mut acks: BTreeMap<&str, Vec<&Ack>> = BTreeMap::new();
        for ack in &self.acks {
            acks.entry(ack.cell.as_str()).or_default().push(ack);
        }
        let mut found = Vec::new();
        for restore in &self.restores {
            let Some(cell_acks) = acks.get(restore.cell.as_str()) else {
                continue;
            };
            let missing: Vec<&&Ack> = cell_acks
                .iter()
                .filter(|ack| ack.epoch < restore.epoch && !restore.content.contains(&ack.write))
                .collect();
            if let Some(first) = missing.first() {
                found.push((
                    format!("{}/{}/{}", restore.cell, restore.epoch, restore.proc),
                    restore.t,
                    format!(
                        "p{} restored {} epoch {} from {} at t={} without {} acknowledged \
                         write(s), first {} (epoch {}, acknowledged at t={})",
                        restore.proc,
                        restore.cell,
                        restore.epoch,
                        restore.source,
                        restore.t,
                        missing.len(),
                        first.write,
                        first.epoch,
                        first.t
                    ),
                ));
            }
        }
        for (key, t, detail) in found {
            if self.flagged.insert(format!("lost-write/{key}")) {
                self.violations.push(Violation {
                    at_ms: t,
                    kind: "lost-write",
                    detail,
                });
            }
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
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::OutputAt { now_mono_ms, .. }
        | Event::LoadSampled { now_mono_ms, .. }
        | Event::EvictRequested { now_mono_ms, .. } => *now_mono_ms = mono,
        Event::OwnerRead { now_ms, .. } | Event::NodeLeaseRead { now_ms, .. } => *now_ms = wall,
        _ => {}
    }
}
