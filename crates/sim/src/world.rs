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
//! - `gates`: what the handler's outputs ask of the output gate, and the
//!   handler's calls held there.
//! - `faults`: what goes wrong, and the supervisor that restarts a node.
//! - `operator`: what an operator and the rest of a node do unprompted.
//! - `shutdown`: graceful shutdowns, which drain or preserve a node.
//! - `handoff`: draining and rebalancing, which hand cells to peers.
//! - `sockets`: WebSockets the workload opens, and the messages on them.
//! - `workers`: top-level Worker requests, which can borrow a resident.
//! - `alarms`: alarms in each database, their wake entries and the scan.
//! - `node_log`: the fleet log: followers, fleet proofs and recovery.
//! - `liveness`: the probe and the audit that end every run.
//!
//! A node is a [`Machine`] with a disk. The [`Process`] on it can die and
//! be restarted. Each start is a new *incarnation* with a fresh core, fresh
//! clocks and no memory. Work addressed to a process carries its
//! incarnation and is dropped if that process has died since: a dead
//! process's timers never fire and its bucket answers reach nobody.

mod alarms;
mod clients;
mod faults;
mod gates;
mod handoff;
mod liveness;
mod node_log;
mod operator;
mod replicator;
mod shell;
mod shutdown;
mod sockets;
mod workers;

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};

use celld_logic::{
    on_event, pressure, Config, Epoch, Event, NodeLeaseSpec, OpId, OwnershipOnEvict, RequestId,
    State, Timer, WebSocketId,
};

use crate::checks::Checker;
use crate::store::Store;
use crate::{Mutation, Outcome, Rng, SimConfig, Stats, WriteId};

use alarms::{AlarmWork, ProcAlarms};
use clients::{Bounce, ClientWork, Op, Req, Via};
use faults::{Fault, Partition};
use gates::Gate;
use handoff::{Adoption, Answer, HandoffWork, Offer};
use liveness::Goal;
use node_log::{Log, LogWork, Session};
use operator::{Act, OperatorWork};
use replicator::{Disk, Replica, ReplicaWork};
use shell::{Slot, StoreReq};
use shutdown::{Shutdown, ShutdownWork};
use sockets::{Socket, SocketWork};

type Time = u64;
/// An item's place on the agenda, to cancel it by.
type AgendaKey = (Time, u64);

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
    /// A client stops waiting for its request.
    GiveUp { op: u64 },
    /// A request reaches a node: from its client, or forwarded by the
    /// ingress. Not tied to a process: it reaches whichever
    /// process runs on the node by then, or none.
    Arrive { node: usize, op: u64, via: Via },
    /// A forward came back to the ingress without running.
    Bounced { op: u64, why: Bounce },
    /// A donor's offer of a released cell reaches a peer. Like `Arrive`,
    /// it reaches whichever process runs there by then.
    Offer { node: usize, offer: Offer },
    /// The next fault of this kind.
    Fault(Fault),
    /// The next act of this kind.
    Operator(Act),
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

/// What a process does later. Each feature module schedules and runs its
/// own kind; only what several modules share is listed here.
enum Work {
    /// A completion the shell reports some time after the effect started.
    Deliver(Event),
    /// A timer the core armed. `ordinal` tells a live arm from one that a
    /// later `ScheduleTimer` for the same slot replaced.
    Timer {
        slot: Slot,
        ordinal: u64,
    },
    /// The process samples its load and tells its core.
    LoadSample,
    /// An activity on the cell ends: report its alarm, then finish it.
    ActivityDone {
        request: RequestId,
        cell: String,
    },
    /// A top-level Worker request reaches the node.
    WorkerRequest,
    /// Nothing: a failure nobody waits for.
    Nothing,
    Replica(ReplicaWork),
    Client(ClientWork),
    Operator(OperatorWork),
    Shutdown(ShutdownWork),
    Handoff(HandoffWork),
    Socket(SocketWork),
    Alarm(AlarmWork),
    Log(LogWork),
}

/// One machine of the fleet: what outlives its process. Its disk, its
/// wall clock's offset and its network belong to the machine, so a restart
/// keeps them; everything else lives in the process, and a crash drops it.
struct Machine {
    name: String,
    /// Which start of this machine's process is the latest one.
    inc: u64,
    /// The supervisor will never start another process here.
    retired: bool,
    /// How far this machine's wall clock is off: its fixed skew plus every
    /// step since.
    wall_offset_ms: i64,
    disk: Disk,
    /// The network partition the machine is in, while it lasts.
    partition: Option<(u64, Partition)>,
    /// The clean-reload marker a preserved process left: its generation.
    reload: Option<String>,
    /// A boot is waiting for its predecessor's log to be recovered. It
    /// serves its follower disk meanwhile, as `recover_with_follower_listener`
    /// does, so a fleet-wide restart cannot wait on itself.
    booting: bool,
    proc: Option<Process>,
}

impl Machine {
    fn new(index: usize, wall_offset_ms: i64) -> Self {
        Self {
            name: format!("n{index}"),
            inc: 0,
            retired: false,
            wall_offset_ms,
            disk: Disk::default(),
            partition: None,
            reload: None,
            booting: false,
            proc: None,
        }
    }

    fn up(&self) -> bool {
        self.proc.is_some()
    }

    fn proc(&self) -> &Process {
        self.proc.as_ref().expect("a process is running")
    }

    fn proc_mut(&mut self) -> &mut Process {
        self.proc.as_mut().expect("a process is running")
    }
}

/// The process running on a machine: one incarnation, with a fresh core,
/// fresh clocks and no memory of the last one.
struct Process {
    /// The core asked to halt; the process exits after this step.
    halting: bool,
    /// While paused, the process runs nothing: its work waits until here.
    paused_until: Option<Time>,
    /// When a VM suspend began: the monotonic clock stops with it.
    suspended_at: Option<Time>,
    state: State,
    /// Where this process's monotonic clock starts. Processes start at
    /// different moments, so their monotonic clocks disagree.
    mono_base: u64,
    /// Armed timers: the arm's ordinal, the timer, and its monotonic due time.
    timers: BTreeMap<Slot, (u64, Timer, u64)>,
    next_ordinal: u64,
    replicas: BTreeMap<(String, Epoch), Replica>,
    /// Fleet-confirmed writes of an epoch that ended without its final
    /// upload (`dirty_tails`). The bundle loop still flushes them, and the
    /// cell's next activation here folds them into the bucket first.
    tails: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// When the last upload on this process landed, on its monotonic clock:
    /// production's `node_proof_ms`.
    node_proof_ms: u64,
    /// The runtime registry: cells this process serves, at which epoch.
    published: BTreeMap<String, Epoch>,
    /// What each request id this process minted stands for.
    pending: BTreeMap<RequestId, Pending>,
    /// A graceful shutdown under way.
    shutdown: Option<Shutdown>,
    /// Released cells this process is finding a successor for.
    adoptions: BTreeMap<OpId, Adoption>,
    /// The WebSockets this process holds open.
    sockets: BTreeMap<WebSocketId, Socket>,
    /// The application generation this process runs new isolates on.
    generation: u64,
    /// What this process knows of its cells' alarms.
    alarms: ProcAlarms,
    /// The generation each cell's runtime last started on, and when
    /// anything last used the cell here: for the watched properties.
    runtime_generations: BTreeMap<String, u64>,
    touched: BTreeMap<String, Time>,
    /// The log this process leads, in the fleet posture.
    log: Option<Log>,
}

impl Process {
    fn new(name: &str, mono_base: u64, config: Config) -> Self {
        Self {
            state: State::new(name.to_string(), config),
            halting: false,
            paused_until: None,
            suspended_at: None,
            mono_base,
            timers: BTreeMap::new(),
            next_ordinal: 0,
            replicas: BTreeMap::new(),
            tails: BTreeMap::new(),
            node_proof_ms: 0,
            published: BTreeMap::new(),
            pending: BTreeMap::new(),
            shutdown: None,
            adoptions: BTreeMap::new(),
            sockets: BTreeMap::new(),
            generation: 0,
            alarms: ProcAlarms::default(),
            runtime_generations: BTreeMap::new(),
            touched: BTreeMap::new(),
            log: None,
        }
    }
}

/// What a request id stands for. One counter mints every id a process
/// gives its core, so one map holds them, and a completion finds its kind by
/// one lookup.
enum Pending {
    /// A client's request, or a forward of one.
    Client(Req),
    /// A handler's call to a third party, held at the output gate.
    Gate(Gate),
    /// A donor's offer the core is deciding on.
    Offer(Offer),
    /// An operator eviction the core has not answered yet, and its cell.
    Eviction(String),
    /// A Worker request waiting for the core to pick an isolate.
    Worker,
}

impl Process {
    fn client(&self, request: RequestId) -> Option<&Req> {
        match self.pending.get(&request)? {
            Pending::Client(req) => Some(req),
            _ => None,
        }
    }

    fn client_mut(&mut self, request: RequestId) -> Option<&mut Req> {
        match self.pending.get_mut(&request)? {
            Pending::Client(req) => Some(req),
            _ => None,
        }
    }

    fn take_client(&mut self, request: RequestId) -> Option<Req> {
        self.client(request)?;
        match self.pending.remove(&request) {
            Some(Pending::Client(req)) => Some(req),
            _ => unreachable!(),
        }
    }

    fn gate(&self, request: RequestId) -> Option<&Gate> {
        match self.pending.get(&request)? {
            Pending::Gate(gate) => Some(gate),
            _ => None,
        }
    }

    fn take_gate(&mut self, request: RequestId) -> Option<Gate> {
        self.gate(request)?;
        match self.pending.remove(&request) {
            Some(Pending::Gate(gate)) => Some(gate),
            _ => unreachable!(),
        }
    }

    fn offers(&self) -> impl Iterator<Item = (RequestId, &Offer)> {
        self.pending.iter().filter_map(|(request, p)| match p {
            Pending::Offer(offer) => Some((*request, offer)),
            _ => None,
        })
    }

    fn take_offer(&mut self, request: RequestId) -> Option<Offer> {
        match self.pending.get(&request)? {
            Pending::Offer(_) => match self.pending.remove(&request) {
                Some(Pending::Offer(offer)) => Some(offer),
                _ => unreachable!(),
            },
            _ => None,
        }
    }

    fn take_eviction(&mut self, request: RequestId) -> Option<String> {
        match self.pending.get(&request)? {
            Pending::Eviction(_) => match self.pending.remove(&request) {
                Some(Pending::Eviction(cell)) => Some(cell),
                _ => unreachable!(),
            },
            _ => None,
        }
    }

    fn take_worker(&mut self, request: RequestId) -> bool {
        let worker = matches!(self.pending.get(&request), Some(Pending::Worker));
        if worker {
            self.pending.remove(&request);
        }
        worker
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
    machines: Vec<Machine>,
    next_partition: u64,
    ops: BTreeMap<u64, Op>,
    next_op: u64,
    next_request: RequestId,
    next_write: WriteId,
    next_socket: WebSocketId,
    /// What each alarm write did: when it makes the alarm due, or `None`
    /// when it clears it.
    alarm_writes: BTreeMap<WriteId, Option<i64>>,
    net_rng: Rng,
    work_rng: Rng,
    fault_rng: Rng,
    op_rng: Rng,
    checker: Checker,
    stats: Stats,
    hash: Fnv,
    trace: Vec<String>,
    halts: Vec<String>,
    goals: Vec<Goal>,
    /// When the last fault, and the last restart one caused, is over.
    quiet_from: Time,
    /// Cells whose runtime a step stopped, for good: judged once the step
    /// is over, when every socket it closed has closed.
    stopped: Vec<(usize, String)>,
    /// Every log session that opened, by its lease generation.
    sessions: BTreeMap<String, Session>,
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
        let op_rng = master.fork();
        let skew = config.faults.max_skew_ms;
        let machines = (0..config.nodes)
            .map(|index| Machine::new(index, fault_rng.range(0, 2 * skew) as i64 - skew as i64))
            .collect();
        let core = core_config(&config);
        let store = Store::new(config.mutation);
        Self {
            seed,
            config,
            t: 0,
            seq: 0,
            core,
            agenda: BTreeMap::new(),
            store,
            machines,
            next_partition: 1,
            ops: BTreeMap::new(),
            next_op: 1,
            next_request: 1,
            next_write: 1,
            next_socket: 1,
            alarm_writes: BTreeMap::new(),
            net_rng,
            work_rng,
            fault_rng,
            op_rng,
            checker: Checker::default(),
            stats: Stats::default(),
            hash: Fnv(0xcbf2_9ce4_8422_2325),
            trace: Vec::new(),
            halts: Vec::new(),
            goals: Vec::new(),
            quiet_from: 0,
            stopped: Vec::new(),
            sessions: BTreeMap::new(),
            panic: None,
        }
    }

    fn after(&mut self, delay: u64, item: Item) -> AgendaKey {
        self.seq += 1;
        let key = (self.t + delay, self.seq);
        self.agenda.insert(key, item);
        key
    }

    /// Drop an item that is no longer needed, so it never runs.
    fn cancel(&mut self, key: AgendaKey) {
        self.agenda.remove(&key);
    }

    /// Schedule work for the process now running on `node`.
    fn work(&mut self, node: usize, delay: u64, work: Work) -> AgendaKey {
        let inc = self.machines[node].inc;
        self.after(delay, Item::Proc { node, inc, work })
    }

    /// Mint a request id on `node`'s process, and hold `pending` under it.
    fn pend(&mut self, node: usize, pending: Pending) -> RequestId {
        let request = self.next_request;
        self.next_request += 1;
        self.machines[node]
            .proc_mut()
            .pending
            .insert(request, pending);
        request
    }

    fn deliver(&mut self, node: usize, delay: u64, event: Event) {
        self.work(node, delay, Work::Deliver(event));
    }

    /// The process on `node`, by name and incarnation: `n0#2`.
    fn who(&self, node: usize) -> String {
        self.who_at(node, self.machines[node].inc)
    }

    /// Incarnation `inc` of the process on `node`.
    fn who_at(&self, node: usize, inc: u64) -> String {
        format!("{}#{inc}", self.machines[node].name)
    }

    /// The machine's wall clock: true time, plus its skew and steps.
    fn wall(&self, node: usize) -> u64 {
        (WALL_EPOCH_MS as i64 + self.t as i64 + self.machines[node].wall_offset_ms) as u64
    }

    fn mono(&self, node: usize) -> u64 {
        self.machines[node].proc().mono_base + mono_ms(self.config.mutation, self.t)
    }

    /// How long the agenda takes for a process's monotonic clock to move
    /// by `mono_ms`.
    fn agenda_ms(&self, mono_ms: u64) -> u64 {
        match self.config.mutation {
            Some(Mutation::SlowMonoClock) => 4 * mono_ms,
            _ => mono_ms,
        }
    }

    pub fn run(mut self) -> Outcome {
        for node in 0..self.machines.len() {
            let delay = self.net_rng.range(0, 50);
            self.after(delay, Item::Restart { node });
        }
        self.start_clients();
        self.start_faults();
        self.start_operator();
        self.start_liveness();
        while let Some(((t, _), item)) = self.agenda.pop_first() {
            self.t = t;
            if matches!(item, Item::End) {
                break;
            }
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
                let m = &self.machines[node];
                let Some(proc) = m.proc.as_ref().filter(|_| m.inc == inc) else {
                    return;
                };
                match proc.paused_until {
                    // A paused process runs this when it resumes, in order.
                    Some(until) => {
                        self.after(until - self.t, Item::Proc { node, inc, work });
                    }
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
            Item::GiveUp { op } => self.give_up(op),
            Item::Arrive { node, op, via } => self.arrive(node, op, via),
            Item::Bounced { op, why } => self.bounced(op, why),
            Item::Offer { node, offer } => self.offer_arrives(node, offer),
            Item::Fault(fault) => self.fault(fault),
            Item::Operator(act) => self.operator(act),
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
            Work::LoadSample => self.sample_load(node),
            Work::ActivityDone { request, cell } => self.activity_done(node, request, &cell),
            Work::WorkerRequest => self.worker_request(node),
            Work::Nothing => {}
            Work::Replica(work) => self.replica_work(node, work),
            Work::Client(work) => self.client_work(node, work),
            Work::Operator(work) => self.operator_work(node, work),
            Work::Shutdown(work) => self.shutdown_work(node, work),
            Work::Handoff(work) => self.handoff_work(node, work),
            Work::Socket(work) => self.socket_work(node, work),
            Work::Alarm(work) => self.alarm_work(node, work),
            Work::Log(work) => self.log_work(node, work),
        }
    }

    /// Start a new process on `node`: a fresh core that first acquires its
    /// node lease.
    fn restart(&mut self, node: usize) {
        let m = &self.machines[node];
        if m.up() || m.retired {
            return;
        }
        if self.recover_predecessor(node).is_err() {
            // The boot waits until its predecessor's log is recovered.
            self.machines[node].booting = true;
            self.note(|| format!("boot n{node} waits for its predecessor's log"));
            self.after(1_000, Item::Restart { node });
            return;
        }
        self.machines[node].booting = false;
        // Large enough that a suspend can hold the clock back without it
        // ever reaching zero.
        let mono_base = self.net_rng.range(1_000_000_000, 2_000_000_000);
        let m = &mut self.machines[node];
        m.inc += 1;
        m.proc = Some(Process::new(&m.name, mono_base, self.core));
        let inc = m.inc;
        let resume_generation = m.reload.take();
        self.note(|| format!("start n{node}#{inc} (resuming {resume_generation:?})"));
        if resume_generation.is_some() {
            // The predecessor handed its epochs over: they are this
            // process's to run and to upload into now.
            let (old, new) = (self.who_at(node, inc - 1), self.who(node));
            self.checker.reloaded(&old, &new);
        }
        self.open_log(node);
        let spec = NodeLeaseSpec {
            addr: self.machines[node].name.clone(),
            peer_protocol: PEER_PROTOCOL,
            generation: shutdown::generation(node, inc),
            resume_generation,
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
        self.start_wake_scans(node);
    }

    /// Record that something used `cell` on `node` just now.
    fn touch(&mut self, node: usize, cell: &str) {
        let t = self.t;
        self.machines[node]
            .proc_mut()
            .touched
            .insert(cell.to_string(), t);
    }

    /// Watched: in a quiet fleet, a cell nothing has used for well over
    /// `idle_evict_ms` has been given back. The allowance covers a refused
    /// eviction, which waits out another idle period, and the sampling
    /// period. A cell with work in flight here is not idle: a request
    /// routed to it, or an output of its handler held at the gate. A cell
    /// with a socket open or an armed alarm is excused: the core may keep
    /// either resident.
    fn judge_idle(&mut self, node: usize) {
        let Some(idle_ms) = self.config.idle_evict_ms else {
            return;
        };
        // The whole allowance must have passed in a quiet fleet: a process
        // that just woke from a pause has had no chance to evict.
        let allowance = 2 * idle_ms + 2 * LOAD_SAMPLE_MS + 2_000;
        let p = self.machines[node].proc();
        if self.t < self.settled_from() + allowance || p.shutdown.is_some() {
            return;
        }
        let kept: Vec<(String, u64)> = p
            .published
            .iter()
            .filter(|(cell, _)| !p.sockets.values().any(|s| s.cell == **cell))
            .filter(|(cell, _)| {
                !p.pending.values().any(|pending| match pending {
                    Pending::Client(req) => req.cell == **cell && req.epoch.is_some(),
                    Pending::Gate(gate) => gate.reveal.cell == **cell,
                    _ => false,
                })
            })
            .filter(|(cell, epoch)| {
                self.seen(node, cell, **epoch)
                    .is_some_and(|content| self.alarm_of(content, **epoch).at_ms().is_none())
            })
            .filter_map(|(cell, _)| {
                let idle = self.t - p.touched.get(cell).copied().unwrap_or(0);
                (idle > allowance).then(|| (cell.clone(), idle))
            })
            .collect();
        let who = self.who(node);
        for (cell, idle) in kept {
            self.checker.idle_kept(self.t, &who, &cell, idle);
        }
    }

    /// Production's periodic sampler. The simulation has no memory to
    /// measure, so only the cell count varies, and the node never reads as
    /// pressured; the sample still drives idle eviction.
    fn sample_load(&mut self, node: usize) {
        self.judge_idle(node);
        let load = pressure::Load {
            resident_cells: self.machines[node].proc().state.occupied(),
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
    /// fails, and the supervisor starts a new process later, unless the
    /// machine is retired.
    fn crash(&mut self, node: usize, wipe: bool) {
        let who = self.who(node);
        if !self.machines[node].up() {
            return;
        }
        self.judge_death(node);
        let dead = self.machines[node].proc.take().expect("up");
        self.note(|| format!("crash {who}{}", if wipe { " (disk wiped)" } else { "" }));
        self.close_replicas(node, dead.replicas);
        if wipe {
            self.machines[node].disk = Disk::default();
        }
        let (mut requests, mut offers) = (BTreeMap::new(), Vec::new());
        for (request, pending) in dead.pending {
            match pending {
                Pending::Client(req) => {
                    requests.insert(request, req);
                }
                Pending::Offer(offer) => offers.push(offer),
                Pending::Gate(_) | Pending::Eviction(_) | Pending::Worker => {}
            }
        }
        self.fail_requests(node, requests);
        // Each donor waiting on this process sees its connection drop.
        for offer in offers {
            let delay = self.net_rng.range(1, 5);
            self.answer_offer(offer, Answer::Failed, delay);
        }
        if !self.machines[node].retired {
            self.schedule_restart(node);
        }
    }

    /// Run one event through a node's core and carry out its effects. Some
    /// effects complete inside the same step, as they do in `Actor::drive`;
    /// their events queue behind the current one.
    fn drive(&mut self, node: usize, event: Event) {
        let mut queue = VecDeque::from([event]);
        while let Some(mut event) = queue.pop_front() {
            match &self.machines[node].proc {
                Some(proc) if !proc.halting => {
                    // Everything for a process comes through `Item::Proc`
                    // or `arrive`, which both hold it back during a pause.
                    debug_assert!(proc.paused_until.is_none(), "drove a paused process");
                }
                _ => break,
            }
            shell::stamp(&mut event, self.wall(node), self.mono(node));
            self.record(node, "<-", &event);
            self.stats.events += 1;
            let effects = on_event(&mut self.machines[node].proc_mut().state, event);
            for effect in effects {
                self.record(node, "->", &effect);
                self.execute(node, effect, &mut queue);
            }
            if self.machines[node]
                .proc
                .as_ref()
                .is_some_and(|p| p.offers().next().is_some())
            {
                self.settle_offers(node);
            }
        }
        // A halt is the process exiting: as far as the fleet can tell, a
        // crash that leaves its disk alone.
        if self.machines[node].proc.as_ref().is_some_and(|p| p.halting) {
            self.halts.push(self.who(node));
            self.crash(node, false);
        }
        // After the halt: a process that exits closes every socket it held.
        self.judge_stopped(node);
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
            self.judge_alarms(true);
        }
        let authoritative = self
            .machines
            .iter()
            .filter(|m| {
                m.proc
                    .as_ref()
                    .is_some_and(|p| p.state.node_authoritative())
            })
            .map(|m| m.name.clone())
            .collect();
        Outcome {
            seed: self.seed,
            trace_hash: self.hash.0,
            halts: self.halts,
            authoritative,
            trace: self.trace,
            panic: self.panic,
            violations: self.checker.violations,
            watched: self.checker.watched,
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
        // Production keeps a cell resident for an alarm due within the hour
        // (`CELLD_ALARM_RESIDENT_MS`). One second lets most alarms here be
        // evicted behind a wake entry, so a scan must find them.
        alarm_resident_ms: 1_000,
        idle_evict_ms: config.idle_evict_ms,
        pressure: pressure::PressureConfig::from_limits(None, None),
    }
}

/// How far every process's monotonic clock has run by simulated time `t`,
/// before its own base and any suspend.
fn mono_ms(mutation: Option<Mutation>, t: Time) -> u64 {
    match mutation {
        Some(Mutation::SlowMonoClock) => t / 4,
        _ => t,
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
