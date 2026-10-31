//! The simulated world: nodes, the bucket, the network between them, the
//! clients, and the shell that turns each core's effects into later events.
//!
//! Everything runs on one simulated millisecond clock. The agenda is an
//! ordered map keyed by `(time, sequence)`. Two items due at the same
//! instant run in the order they were scheduled, so the whole run is a
//! function of the seed.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::panic::{catch_unwind, AssertUnwindSafe};

use celld_logic::{
    on_event, pressure, CasGuard, CasOutcome, Channel, Config, Effect, Epoch, Event, Failure,
    LeaseCasOutcome, NodeLeaseRecord, NodeLeaseSpec, OpId, OwnerRecord, OwnershipOnEvict,
    ProofSource, RequestError, RequestId, RestoreOutcome, RestoreSpec, Route, State, Timer,
};

use crate::checks::Checker;
use crate::store::Store;
use crate::{Outcome, Rng, SimConfig, WriteId};

type Time = u64;

/// Simulated time zero on the wall clock.
const WALL_EPOCH_MS: u64 = 1_800_000_000_000;
const PEER_PROTOCOL: u16 = 1;
/// When clients start sending. Nodes need a few round trips for their
/// leases first; a request before that is refused, which proves nothing.
const CLIENTS_START_MS: u64 = 200;
/// When clients stop, so that requests in flight can finish before the end.
const CLIENTS_STOP_BEFORE_END_MS: u64 = 2_000;

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
    /// A client sends its next request.
    ClientNew,
    /// A handler commits its write.
    HandlerCommit { node: usize, request: RequestId },
    /// A handler returns its response.
    HandlerEnd { node: usize, request: RequestId },
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

/// A request to the bucket. The core's own requests carry its `op`; the
/// replicator's uploads and restore reads are the shell's.
#[derive(Debug)]
enum StoreReq {
    ReadSelfLease {
        op: OpId,
    },
    CasLease {
        op: OpId,
        guard: CasGuard,
        record: NodeLeaseRecord,
    },
    ReadOwner {
        op: OpId,
        cell: String,
    },
    CasOwner {
        op: OpId,
        cell: String,
        guard: CasGuard,
        epoch: Epoch,
    },
    /// `VerifyOwnership`: read the owner record and compare it here.
    Verify {
        op: OpId,
        cell: String,
        epoch: Epoch,
    },
    /// The replicator uploads the epoch's whole database.
    Upload {
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    /// A restore reads the newest chain below its epoch.
    ReadChain {
        op: OpId,
        cell: String,
        epoch: Epoch,
    },
}

/// What the bucket answered, filled in when the request applies.
#[derive(Debug)]
enum StoreRes {
    Lease(Option<NodeLeaseRecord>),
    LeaseCas(LeaseCasOutcome),
    Owner(Option<OwnerRecord>),
    Cas(CasOutcome),
    Put,
    Chain(Option<Vec<WriteId>>),
}

struct Call {
    node: usize,
    req: StoreReq,
    res: Option<StoreRes>,
}

/// A cell's open database on one node, at one epoch.
struct Replica {
    content: Vec<WriteId>,
    /// Length when the epoch opened: the chain it was restored from.
    start_len: usize,
    /// Length the bucket is known to hold for this epoch.
    uploaded: usize,
    uploading: bool,
    /// Gate proofs waiting for `uploaded` to reach a position.
    waiters: Vec<(OpId, u64)>,
}

/// A client request this node is serving.
struct Req {
    cell: String,
    write: bool,
    /// The epoch the request ran at, once routed locally.
    epoch: Option<Epoch>,
    /// The write the handler committed and its position.
    committed: Option<(WriteId, u64)>,
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
    calls: BTreeMap<u64, Call>,
    next_call: u64,
    next_request: RequestId,
    next_write: WriteId,
    net_rng: Rng,
    work_rng: Rng,
    checker: Checker,
    hash: Fnv,
    trace: Vec<String>,
    events: u64,
    requests: u64,
    answered: u64,
    failed: u64,
    acked_writes: u64,
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
            calls: BTreeMap::new(),
            next_call: 0,
            next_request: 1,
            next_write: 1,
            net_rng,
            work_rng,
            checker: Checker::default(),
            hash: Fnv(0xcbf2_9ce4_8422_2325),
            trace: Vec::new(),
            events: 0,
            requests: 0,
            answered: 0,
            failed: 0,
            acked_writes: 0,
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
            self.drive(
                node,
                Event::StartNodeLease {
                    now_ms: 0,
                    now_mono_ms: 0,
                    spec,
                },
            );
        }
        self.after(CLIENTS_START_MS, Item::ClientNew);
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
                Item::ClientNew => self.client_new(),
                Item::HandlerCommit { node, request } => self.handler_commit(node, request),
                Item::HandlerEnd { node, request } => self.handler_end(node, request),
            }
        }
        self.finish()
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
            stamp(&mut event, self.wall(), self.mono(node));
            self.record(node, "<-", &event);
            self.events += 1;
            let state = &mut self.nodes[node].state;
            // `on_event` runs `State::validate` after every event in builds
            // with debug assertions, so a broken core invariant arrives here
            // as a panic, and is reported with its seed.
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
                self.execute(node, effect, &mut queue);
            }
        }
    }

    /// The shell: carry out one effect, as `Actor::execute` does. An effect
    /// either finishes now (its event goes on `immediate`) or starts work
    /// whose completion is scheduled on the agenda.
    fn execute(&mut self, node: usize, effect: Effect, immediate: &mut VecDeque<Event>) {
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
            Effect::ReadSelfNodeLease { op } => {
                self.store_call(node, StoreReq::ReadSelfLease { op })
            }
            Effect::CasNodeLease {
                op, guard, record, ..
            } => self.store_call(node, StoreReq::CasLease { op, guard, record }),
            Effect::ReadLocalCells => {
                // No disk yet, so no local cells. Production scans on a
                // blocking task, so the answer arrives a little later.
                let delay = self.net_rng.range(1, 5);
                self.deliver(node, delay, Event::LocalCellsRead { result: Ok(vec![]) });
            }
            Effect::ReadOwner { op, cell } => {
                self.store_call(node, StoreReq::ReadOwner { op, cell })
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                ..
            } => self.store_call(
                node,
                StoreReq::CasOwner {
                    op,
                    cell,
                    guard,
                    epoch,
                },
            ),
            Effect::Restore { op, cell, spec } => self.restore(node, op, cell, spec),
            Effect::StartRuntime { op, cell, epoch } => {
                let name = self.nodes[node].name.clone();
                self.checker.runtime_started(self.t, &name, &cell, epoch);
                let result = if self.nodes[node].replicas.contains_key(&(cell, epoch)) {
                    Ok(())
                } else {
                    Err(Failure::Definite)
                };
                let generation = self.nodes[node].state.current_generation();
                let delay = self.net_rng.range(1, 10);
                let event = Event::RuntimeStarted {
                    op,
                    isolate: None,
                    generation,
                    result,
                };
                self.deliver(node, delay, event);
            }
            Effect::Publish { op, cell, epoch } => {
                // Production inserts into its registry inside the same step.
                self.nodes[node].published.insert(cell, epoch);
                immediate.push_back(Event::Published { op, result: Ok(()) });
            }
            Effect::Complete { request, result } => self.complete(node, request, result),
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => match self.nodes[node].replicas.get_mut(&(cell.clone(), epoch)) {
                Some(replica) => {
                    replica.waiters.push((op, position));
                    self.settle_waiters(node, &cell, epoch);
                    self.upload(node, &cell, epoch);
                }
                None => {
                    let event = Event::DurableReached {
                        op,
                        result: Err(Failure::Ambiguous),
                        source: ProofSource::Bucket,
                    };
                    self.deliver(node, 1, event);
                }
            },
            Effect::VerifyOwnership { op, cell, epoch } => {
                self.store_call(node, StoreReq::Verify { op, cell, epoch })
            }
            Effect::Release {
                request,
                channel,
                result,
            } => self.release(node, request, channel, result),
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
    fn store_call(&mut self, node: usize, req: StoreReq) {
        let call = self.next_call;
        self.next_call += 1;
        self.calls.insert(
            call,
            Call {
                node,
                req,
                res: None,
            },
        );
        let delay = self.latency();
        self.after(delay, Item::StoreApply { call });
    }

    fn latency(&mut self) -> u64 {
        let (lo, hi) = self.config.store_latency_ms;
        self.net_rng.range(lo, hi)
    }

    /// The request reaches the bucket: apply it now, at this one instant.
    fn store_apply(&mut self, call: u64) {
        let Call { node, req, .. } = &self.calls[&call];
        let name = self.nodes[*node].name.clone();
        let res = match req {
            StoreReq::ReadSelfLease { .. } => StoreRes::Lease(self.store.read_lease(&name)),
            StoreReq::CasLease { guard, record, .. } => {
                StoreRes::LeaseCas(self.store.cas_lease(guard, record))
            }
            StoreReq::ReadOwner { cell, .. } | StoreReq::Verify { cell, .. } => {
                StoreRes::Owner(self.store.read_owner(cell))
            }
            StoreReq::CasOwner {
                cell, guard, epoch, ..
            } => {
                let outcome = self.store.cas_owner(cell, guard, *epoch, &name);
                if outcome == CasOutcome::Applied {
                    self.checker.claim_applied(cell, *epoch, &name);
                }
                StoreRes::Cas(outcome)
            }
            StoreReq::Upload {
                cell,
                epoch,
                content,
            } => {
                self.store.put(cell, *epoch, content);
                StoreRes::Put
            }
            StoreReq::ReadChain { cell, epoch, .. } => {
                StoreRes::Chain(self.store.read_chain(cell, *epoch))
            }
        };
        self.calls.get_mut(&call).unwrap().res = Some(res);
        let delay = self.latency();
        self.after(delay, Item::StoreReply { call });
    }

    /// The response reaches the node: turn it into the core's event, or
    /// into the replicator's own bookkeeping.
    fn store_reply(&mut self, call: u64) {
        let Call { node, req, res } = self.calls.remove(&call).unwrap();
        let res = res.expect("a reply follows its apply");
        let event = match (req, res) {
            (StoreReq::ReadSelfLease { op }, StoreRes::Lease(record)) => Event::SelfNodeLeaseRead {
                op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Ok(record),
            },
            (StoreReq::CasLease { op, .. }, StoreRes::LeaseCas(outcome)) => {
                Event::NodeLeaseCasCompleted {
                    op,
                    now_mono_ms: 0,
                    result: Ok(outcome),
                    // The bucket posture: lease bodies carry no node-log state.
                    stamped_log_state: None,
                }
            }
            (StoreReq::ReadOwner { op, .. }, StoreRes::Owner(record)) => Event::OwnerRead {
                op,
                now_ms: 0,
                result: Ok(record),
            },
            (StoreReq::CasOwner { op, .. }, StoreRes::Cas(outcome)) => Event::OwnerCasCompleted {
                op,
                result: Ok(outcome),
            },
            (StoreReq::Verify { op, epoch, .. }, StoreRes::Owner(record)) => {
                let name = &self.nodes[node].name;
                let ours =
                    record.is_some_and(|r| r.node.as_ref() == Some(name) && r.epoch == epoch);
                Event::OwnershipVerified {
                    op,
                    result: if ours { Ok(()) } else { Err(Failure::Definite) },
                }
            }
            (
                StoreReq::Upload {
                    cell,
                    epoch,
                    content,
                },
                StoreRes::Put,
            ) => {
                if let Some(replica) = self.nodes[node].replicas.get_mut(&(cell.clone(), epoch)) {
                    replica.uploading = false;
                    replica.uploaded = replica.uploaded.max(content.len());
                    self.settle_waiters(node, &cell, epoch);
                    self.upload(node, &cell, epoch);
                }
                return;
            }
            (StoreReq::ReadChain { op, cell, epoch }, StoreRes::Chain(chain)) => {
                let restored = chain.is_some();
                self.open_replica(node, cell, epoch, chain.unwrap_or_default());
                restore_completed(op, restored)
            }
            (req, res) => unreachable!("{res:?} does not answer {req:?}"),
        };
        self.drive(node, event);
    }

    // -----------------------------------------------------------------------
    // The replicator: restore, upload, durability proofs (`ltx_repl.rs`)

    fn restore(&mut self, node: usize, op: OpId, cell: String, spec: RestoreSpec) {
        if spec.fresh {
            // Epoch one of a new cell: there is nothing to restore.
            self.open_replica(node, cell, spec.epoch, Vec::new());
            let delay = self.net_rng.range(1, 5);
            self.deliver(node, delay, restore_completed(op, false));
        } else {
            // No disk yet, so no local snapshot to reuse: always read the
            // chain from the bucket.
            let epoch = spec.epoch;
            self.store_call(node, StoreReq::ReadChain { op, cell, epoch });
        }
    }

    fn open_replica(&mut self, node: usize, cell: String, epoch: Epoch, content: Vec<WriteId>) {
        let len = content.len();
        let replica = Replica {
            content,
            start_len: len,
            // What it opened with is already in the bucket, below this epoch.
            uploaded: len,
            uploading: false,
            waiters: Vec::new(),
        };
        self.nodes[node].replicas.insert((cell, epoch), replica);
    }

    /// Upload the epoch's database if the bucket is behind it and no upload
    /// is in flight. One upload at a time; its reply starts the next.
    fn upload(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let Some(replica) = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            return;
        };
        if replica.uploading || replica.content.len() <= replica.uploaded {
            return;
        }
        replica.uploading = true;
        let req = StoreReq::Upload {
            cell: cell.to_string(),
            epoch,
            content: replica.content.clone(),
        };
        self.store_call(node, req);
    }

    /// Answer every gate proof the bucket now covers. The answer is
    /// scheduled, not driven, because this can run inside `execute`.
    fn settle_waiters(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let Some(replica) = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            return;
        };
        let uploaded = replica.uploaded as u64;
        let mut proven = Vec::new();
        replica.waiters.retain(|&(op, position)| {
            let covered = position <= uploaded;
            if covered {
                proven.push(op);
            }
            !covered
        });
        for op in proven {
            let event = Event::DurableReached {
                op,
                result: Ok(uploaded),
                source: ProofSource::Bucket,
            };
            self.deliver(node, 0, event);
        }
    }

    // -----------------------------------------------------------------------
    // Clients and the scripted handler

    /// A client sends a request for a random cell. Each cell has one home
    /// node that receives all its requests; forwarding between nodes is
    /// not modeled yet.
    fn client_new(&mut self) {
        let cell = self.work_rng.below(self.config.cells as u64) as usize;
        let node = cell % self.nodes.len();
        let write = self.work_rng.chance(self.config.write_probability);
        let request = self.next_request;
        self.next_request += 1;
        self.requests += 1;
        let cell = format!("c{cell}");
        self.nodes[node].requests.insert(
            request,
            Req {
                cell: cell.clone(),
                write,
                epoch: None,
                committed: None,
            },
        );
        let event = Event::RequestAt {
            request,
            cell,
            now_mono_ms: 0,
        };
        let delay = self.net_rng.range(1, 3);
        self.deliver(node, delay, event);

        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        if self.t + gap + CLIENTS_STOP_BEFORE_END_MS < self.config.duration_ms {
            self.after(gap, Item::ClientNew);
        }
    }

    /// The core routed a request. Locally, run the handler: maybe commit a
    /// write after a while, then answer.
    fn complete(&mut self, node: usize, request: RequestId, result: Result<Route, RequestError>) {
        let n = &mut self.nodes[node];
        let Some(req) = n.requests.get_mut(&request) else {
            return;
        };
        match result {
            Ok(Route::Local) => {
                let epoch = n.published.get(&req.cell).copied();
                let (name, cell, write) = (n.name.clone(), req.cell.clone(), req.write);
                self.checker
                    .routed_local(self.t, &name, &cell, epoch.is_some());
                let Some(epoch) = epoch else {
                    self.nodes[node].requests.remove(&request);
                    self.failed += 1;
                    return;
                };
                self.nodes[node].requests.get_mut(&request).unwrap().epoch = Some(epoch);
                let commit = self.work_rng.range(0, 10);
                let end = commit + self.work_rng.range(1, 30);
                if write {
                    self.after(commit, Item::HandlerCommit { node, request });
                }
                self.after(end, Item::HandlerEnd { node, request });
            }
            Ok(Route::Remote { .. }) => {
                unimplemented!("a cell's home node is its only owner until forwarding exists")
            }
            Err(_) => {
                n.requests.remove(&request);
                self.failed += 1;
            }
        }
    }

    /// The handler's write: append to the open database, then let the
    /// replicator know there is something to upload.
    fn handler_commit(&mut self, node: usize, request: RequestId) {
        let n = &mut self.nodes[node];
        let Some(req) = n.requests.get_mut(&request) else {
            return;
        };
        let key = (req.cell.clone(), req.epoch.unwrap());
        let Some(replica) = n.replicas.get_mut(&key) else {
            return;
        };
        replica.content.push(self.next_write);
        req.committed = Some((self.next_write, replica.content.len() as u64));
        self.next_write += 1;
        self.upload(node, &key.0, key.1);
    }

    /// The handler returns. Its response goes to the output gate, which
    /// holds it until the write it could reveal is durable; then the
    /// handler's activity is over.
    fn handler_end(&mut self, node: usize, request: RequestId) {
        let n = &self.nodes[node];
        let Some(req) = n.requests.get(&request) else {
            return;
        };
        let position = req.committed.map(|(_, position)| position);
        // A read reports what it saw above the epoch's opening content, so
        // the gate can hold it behind another handler's unproven write.
        let observed = match position {
            Some(_) => None,
            None => n
                .replicas
                .get(&(req.cell.clone(), req.epoch.unwrap()))
                .filter(|replica| replica.content.len() > replica.start_len)
                .map(|replica| replica.content.len() as u64),
        };
        self.drive(
            node,
            Event::OutputAt {
                request,
                channel: Channel::Response,
                position,
                observed,
                epoch: None,
                now_mono_ms: 0,
            },
        );
        self.drive(node, Event::ActivityFinished { request });
    }

    /// The gate released or refused the response: the client hears back.
    fn release(
        &mut self,
        node: usize,
        request: RequestId,
        channel: Channel,
        result: Result<(), RequestError>,
    ) {
        assert_eq!(channel, Channel::Response, "handlers only answer");
        let Some(req) = self.nodes[node].requests.remove(&request) else {
            return;
        };
        if result.is_err() {
            self.failed += 1;
            return;
        }
        self.answered += 1;
        if let (Some((write, position)), Some(epoch)) = (req.committed, req.epoch) {
            self.acked_writes += 1;
            self.checker
                .write_acked(self.t, &self.store, &req.cell, epoch, write, position);
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
        Outcome {
            seed: self.seed,
            trace_hash: self.hash.0,
            trace: self.trace,
            events: self.events,
            halts: names(&self.nodes, |n| n.halted),
            authoritative: names(&self.nodes, |n| n.state.node_authoritative()),
            panic: self.panic,
            violations: self.checker.violations,
            requests: self.requests,
            answered: self.answered,
            failed: self.failed,
            acked_writes: self.acked_writes,
        }
    }
}

fn restore_completed(op: OpId, restored: bool) -> Event {
    Event::RestoreCompleted {
        op,
        result: Ok(RestoreOutcome {
            restored,
            alarm: None,
        }),
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
        Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::OutputAt { now_mono_ms, .. } => *now_mono_ms = mono,
        Event::OwnerRead { now_ms, .. } => *now_ms = wall,
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
