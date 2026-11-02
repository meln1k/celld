//! The shell: carry out each effect a core returns, as `Actor::execute` in
//! `crates/celld/actor.rs` does, and talk to the bucket.

use std::collections::VecDeque;

use celld_logic::log_tier::LogState;
use celld_logic::wake::AlarmSnapshot;
use celld_logic::{
    CasGuard, CasOutcome, Effect, Epoch, Event, Failure, LeaseCasOutcome, NodeLeaseRecord, OpId,
    StopCause, Timer,
};

use super::alarms::{AlarmWork, Waiter};
use super::faults::Partition;
use super::gc::GcReq;
use super::node_log::{Flushed, LogWork};
use super::replicator::ReplicaWork;
use super::waker::WakerReq;
use super::{Item, Pending, Work, World, WALL_EPOCH_MS};
use crate::store::{Load, NoBase, Row};
use crate::{Mutation, WriteId};

/// The bucket client's timeout for one attempt (`bucket.rs`).
pub(super) const STORE_TIMEOUT_MS: u64 = 15_000;

/// Which armed timer a new arm replaces. This follows `TimerSlot` in
/// `crates/celld/actor.rs`: the lease timers coalesce, and the rest are
/// keyed so that arming one never disarms another.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Slot {
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

/// What happens to one bucket call.
pub(super) enum Fate {
    /// It applies after `request_ms`, and its answer comes back
    /// `response_ms` later.
    Answered { request_ms: u64, response_ms: u64 },
    /// The caller hears `failure` after `at`. With `applies_after`, the
    /// request still reaches the bucket then; nobody hears its answer.
    Failed {
        failure: Failure,
        at: u64,
        applies_after: Option<u64>,
    },
}

/// A request on its way to the bucket. The core's own requests carry its
/// `op`; uploads and chain reads belong to the replicator.
#[derive(Debug)]
pub(super) enum StoreReq {
    ReadSelfLease {
        op: OpId,
    },
    CasLease {
        op: OpId,
        guard: CasGuard,
        record: NodeLeaseRecord,
        load: Load,
        /// The log epoch whose record the body carries with
        /// `may_have_fleet_acks` set, once an activation asked for it.
        acks: Option<u64>,
    },
    /// The fleet's leases, to place an unowned cell on a peer with room.
    ReadCapacityPeers {
        op: OpId,
    },
    ReadOwner {
        op: OpId,
        cell: String,
    },
    /// Another node's lease, to judge whether the owner it names is live.
    ReadLease {
        op: OpId,
        node: String,
    },
    CasOwner {
        op: OpId,
        cell: String,
        guard: CasGuard,
        epoch: Epoch,
        /// The shutdown its process was in when it sent the claim.
        shutting_down: Option<&'static str>,
    },
    /// Publish the cell as unowned, keeping its epoch, if this node still
    /// owns it at `epoch`.
    ReleaseOwner {
        op: OpId,
        cell: String,
        epoch: Epoch,
    },
    /// `VerifyOwnership`: read the owner record and compare it.
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
    /// A paged activation's marker (`seed_continuation`): the epoch
    /// continues the chain it restored, and the cell serves only once the
    /// marker is in the bucket.
    PutMarker {
        op: OpId,
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    /// A restore reads the newest chain below its epoch, after folding the
    /// cell's tail into the bucket when an epoch here left one
    /// (`fold_cell`).
    ReadChain {
        op: OpId,
        cell: String,
        epoch: Epoch,
        fold: bool,
    },
    /// One flush of a session's bundle loop, at a log epoch.
    Bundle {
        session: String,
        log_epoch: u64,
        rows: Vec<Row>,
    },
    /// A compaction round: fold `session`'s bundle rows of a cell epoch
    /// into its per-cell prefix (`compact_cell`, `BundleOverlayClient`).
    Compact {
        session: String,
        cell: String,
        epoch: Epoch,
    },
    /// Bundle GC deletes bundles of `session` it found covered
    /// (`delete_many`).
    DeleteBundles {
        session: String,
        seqs: Vec<u64>,
    },
    /// A maintain pass reads the peers' leases (`read_capacity_peers`).
    ReadPeers,
    /// A degraded shipper's lease write that reopens its log at the epoch
    /// after `from` with `ensemble`.
    Reopen {
        session: String,
        from: u64,
        ensemble: Vec<usize>,
    },
    /// One step of an owner's epoch GC pass.
    Gc(GcReq),
    /// One step of a wake loop.
    Waker(WakerReq),
    /// Publish the wake entry of the cell's alarm. With `retire`, then
    /// retire the entries it makes obsolete, as `maintain_wake_entry` does.
    Wake {
        cell: String,
        alarm: AlarmSnapshot,
        report: bool,
        retire: bool,
        /// The reply that waits for this write (the arm gate).
        gate: Option<Waiter>,
    },
}

impl World {
    /// Carry out one effect. It either finishes now (its event goes on
    /// `immediate`, as in `Actor::drive`) or starts work whose completion
    /// is scheduled on the agenda.
    pub(super) fn execute(&mut self, node: usize, effect: Effect, immediate: &mut VecDeque<Event>) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                let mono = self.mono(node);
                let n = self.machines[node].proc_mut();
                let slot = Slot::of(&timer);
                let ordinal = n.next_ordinal;
                n.next_ordinal += 1;
                n.timers.insert(slot.clone(), (ordinal, timer, at_mono_ms));
                let delay = self.agenda_ms(at_mono_ms.saturating_sub(mono));
                self.work(node, delay, Work::Timer { slot, ordinal });
            }
            Effect::ReadSelfNodeLease { op } => {
                self.store_call(node, StoreReq::ReadSelfLease { op })
            }
            Effect::CasNodeLease {
                op,
                guard,
                mut record,
                authority_expires_ms,
            } => {
                // The shell stamps the body with its own log slot when it
                // writes it, whatever the core believed (`stamped_log_state`).
                let p = self.machines[node].proc();
                record.log_state = p.log.is_some().then_some(LogState::Open);
                // The lease body carries the node's load, sampled now.
                let p = self.machines[node].proc();
                let load = Load {
                    // Activating cells hold a slot too, as production reports.
                    resident: p.state.occupied(),
                    owned: p.state.owned_cells(),
                    sampled_ms: self.wall(node),
                    memory: p.memory,
                    pressured: p.state.shedding(),
                    memory_headroom: p.state.memory_headroom(),
                };
                // A renewal must come back while authority remains, so the
                // shell bounds it to half of what is left, as production
                // does (`actor/effects.rs`), and a timeout is ambiguous.
                let timeout = authority_expires_ms.map_or(STORE_TIMEOUT_MS, |expires_ms| {
                    let remaining = expires_ms.saturating_sub(self.wall(node));
                    (remaining / 2).clamp(250, 2_500)
                });
                let acks = self.activation_stamp(node);
                let req = StoreReq::CasLease {
                    op,
                    guard,
                    record,
                    load,
                    acks,
                };
                self.store_call_within(node, req, timeout)
            }
            Effect::ReadCapacityPeers { op, .. } => {
                self.store_call(node, StoreReq::ReadCapacityPeers { op })
            }
            Effect::ReadLocalCells => {
                // Production scans the disk on a blocking task, so the
                // answer arrives a little later.
                let cells = self.local_cells(node);
                let delay = self.net_rng.range(1, 5);
                self.deliver(node, delay, Event::LocalCellsRead { result: Ok(cells) });
            }
            Effect::RecoverNodeLog { op, owner, .. } => {
                // Recovery reads and writes the bucket, so it fails as a
                // bucket call does. It is idempotent, so one that fails can
                // still have applied.
                let (apply_ms, answer_ms) = match self.fate(node, STORE_TIMEOUT_MS) {
                    Fate::Answered {
                        request_ms,
                        response_ms,
                    } => (Some(request_ms), Some(response_ms)),
                    Fate::Failed {
                        failure,
                        at,
                        applies_after,
                    } => {
                        let result = Err(failure);
                        self.deliver(node, at.max(1), Event::NodeLogRecovered { op, result });
                        (applies_after, None)
                    }
                };
                if let Some(apply_ms) = apply_ms {
                    let work = LogWork::Recover {
                        op,
                        owner,
                        answer_ms,
                    };
                    self.work(node, apply_ms, Work::Log(work));
                }
            }
            Effect::ReadOwner { op, cell } => {
                self.store_call(node, StoreReq::ReadOwner { op, cell })
            }
            Effect::ReadNodeLease { op, owner, .. } => {
                self.store_call(node, StoreReq::ReadLease { op, node: owner })
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                ..
            } => {
                let shutting_down = self.shutdown_phase(node);
                let req = StoreReq::CasOwner {
                    op,
                    cell,
                    guard,
                    epoch,
                    shutting_down,
                };
                self.store_call(node, req)
            }
            Effect::Restore { op, cell, spec } => self.restore(node, op, cell, spec),
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
                bounded,
            } => {
                self.alarm_gates_closed(node, &cell, epoch);
                let evict = matches!(cause, StopCause::Evict { .. });
                if evict {
                    self.judge_eviction(node, &cell, epoch);
                }
                // Production drops the runtime from its registry in the
                // same step. The planted bug leaves an evicted one there.
                if !(evict && self.config.mutation == Some(Mutation::StaleRegistry)) {
                    self.machines[node].proc_mut().published.remove(&cell);
                }
                if cause != StopCause::Swap {
                    let p = self.machines[node].proc_mut();
                    p.watch.stopped.push(cell.clone());
                }
                // The realm leaves its heap as the runtime closes.
                self.machines[node].proc_mut().isolates.unplace(&cell);
                self.stop_runtime(node, op, cell, epoch, cause, bounded);
            }
            Effect::StartRuntime { op, cell, epoch } => {
                let (name, who) = (self.machines[node].name.clone(), self.who(node));
                self.checker
                    .runtime_started(self.t, &name, &who, &cell, epoch);
                let p = self.machines[node].proc_mut();
                p.watch.generations.insert(cell.clone(), p.generation);
                let result = if self.machines[node]
                    .proc()
                    .replicas
                    .contains_key(&(cell.clone(), epoch))
                {
                    Ok(())
                } else {
                    Err(Failure::Definite)
                };
                // The generation and the heap the runtime was placed on.
                let generation = self.machines[node].proc().generation;
                let isolate = result.is_ok().then(|| self.place_cell(node, &cell));
                let delay = self.net_rng.range(1, 10);
                let event = Event::RuntimeStarted {
                    op,
                    isolate,
                    generation,
                    result,
                };
                self.deliver(node, delay, event);
            }
            Effect::Publish { op, cell, epoch } => {
                // Production inserts into its registry inside the same step.
                if self.config.mutation != Some(Mutation::SkipPublish) {
                    let p = self.machines[node].proc_mut();
                    p.watch.touched.insert(cell.clone(), self.t);
                    p.published.insert(cell, epoch);
                }
                immediate.push_back(Event::Published { op, result: Ok(()) });
            }
            Effect::Complete { request, result } => {
                match self.machines[node].proc().pending.get(&request) {
                    Some(Pending::Offer(_)) => self.offer_routed(node, request, &result, immediate),
                    Some(Pending::Gate(_)) => self.gate_routed(node, request, &result),
                    _ => self.complete(node, request, result, immediate),
                }
            }
            Effect::CompleteWorker { request, route } => {
                self.worker_routed(node, request, route, immediate)
            }
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => {
                self.alarm_gate_opened(node, op, &cell, epoch, position);
                self.await_durable(node, op, &cell, epoch, position)
            }
            Effect::EnsureDurable {
                op, cell, epoch, ..
            } => self.ensure_durable(node, op, &cell, epoch),
            Effect::AbandonEvictionStop { op, .. } => self.abandon_stop(node, op),
            Effect::ReleaseOwner { op, cell, epoch } => {
                self.store_call(node, StoreReq::ReleaseOwner { op, cell, epoch })
            }
            Effect::AdoptReleased {
                op,
                cell,
                released_epoch,
                rebalance,
            } => self.adopt_released(node, op, cell, released_epoch, rebalance),
            Effect::CancelCellActivity {
                cell,
                requests,
                websockets,
                alarm,
                keep_hibernatable,
            } => {
                self.abort_requests(node, requests);
                if let Some(op) = alarm {
                    self.abort_alarm(node, op);
                }
                self.close_sockets(node, &cell, websockets, immediate);
                if !keep_hibernatable {
                    // A handoff: the cell is leaving, so its alarm must be
                    // covered by a wake entry first.
                    self.refresh_coverage(node, &cell, immediate);
                }
            }
            Effect::FireAlarm {
                op, cell, epoch, ..
            } => self.fire_alarm(node, op, cell, epoch),
            Effect::ReconcileWakeEntry { cell, alarm } => self.reconcile_wake(node, &cell, alarm),
            Effect::CloseWebSocket { cell, websocket } => {
                self.close_sockets(node, &cell, vec![websocket], immediate)
            }
            Effect::EvictionAdmission { request, result } => {
                self.eviction_admitted(node, request, result)
            }
            Effect::EvictionFinished { request, result } => {
                self.eviction_finished(node, request, result)
            }
            Effect::VerifyOwnership { op, .. }
                if self.config.mutation == Some(Mutation::BlindVerify) =>
            {
                immediate.push_back(Event::OwnershipVerified { op, result: Ok(()) });
            }
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
                self.machines[node].proc_mut().halting = true;
            }
        }
    }

    pub(super) fn timer(&mut self, node: usize, slot: Slot, ordinal: u64) {
        let mono = self.mono(node);
        let n = self.machines[node].proc_mut();
        let Some(&(armed, _, at_mono)) = n.timers.get(&slot) else {
            return;
        };
        if armed != ordinal {
            // A later arm replaced this one.
            return;
        }
        if mono < at_mono {
            // A suspend held the monotonic clock back: not due yet.
            let delay = self.agenda_ms(at_mono - mono);
            self.work(node, delay, Work::Timer { slot, ordinal });
            return;
        }
        let (_, timer, _) = n.timers.remove(&slot).unwrap();
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
    /// its answer arrives after another, so other events can happen in
    /// between, on this node and on others.
    pub(super) fn store_call(&mut self, node: usize, req: StoreReq) {
        self.store_call_within(node, req, STORE_TIMEOUT_MS);
    }

    /// Draw what happens to one call, then send it.
    ///
    /// - It may fail definitely: it did not apply, and the caller knows.
    /// - Its request may be lost: it did not apply, and the caller times out.
    /// - Its response may be lost: it applied, and the caller times out.
    /// - It may be slow: if both hops take longer than the timeout, the
    ///   caller gives up first, and the request still applies, late.
    /// - A partitioned node's calls hang or are refused, and never apply.
    ///
    /// A failed call's answer is scheduled here, before the request moves.
    /// A late or lost answer to a call that did apply is simply never sent.
    ///
    /// This is deliberately harsher than production. Its bucket client
    /// retries a failed read internally (two retries within 30 s) before
    /// the core hears anything; here the core sees every failure of the
    /// one attempt, so its own retry paths run far more often.
    fn store_call_within(&mut self, node: usize, req: StoreReq, timeout: u64) {
        let inc = self.machines[node].inc;
        let (request_ms, answer_ms) = match self.fate(node, timeout) {
            Fate::Answered {
                request_ms,
                response_ms,
            } => (request_ms, Some(response_ms)),
            Fate::Failed {
                failure,
                at,
                applies_after,
            } => {
                self.stats.store_failures += 1;
                let work = self.store_failed(node, &req, failure);
                self.work(node, at.max(1), work);
                match applies_after {
                    Some(request_ms) => (request_ms, None),
                    None => return,
                }
            }
        };
        let item = Item::StoreApply {
            node,
            inc,
            req,
            answer_ms,
        };
        self.after(request_ms, item);
    }

    /// An upload of `content` into a cell epoch's prefix lands, written by
    /// process `writer`. Every write into a prefix goes through here, so
    /// the oracle sees each one: who wrote it, and how far the prefix grew.
    pub(super) fn land_upload(
        &mut self,
        writer: &str,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
    ) {
        self.checker.upload_applied(self.t, writer, cell, epoch);
        let at_ms = WALL_EPOCH_MS + self.t;
        self.store.put(cell, epoch, content, at_ms);
        self.checker.bucket_holds(self.t, cell, epoch, content);
        self.judge_discoverable(cell);
    }

    /// Draw the fate of one call: its latencies, then the faults that
    /// override them. A partition overrides everything: the call never
    /// gets out.
    pub(super) fn fate(&mut self, node: usize, timeout: u64) -> Fate {
        let mut request_ms = self.latency();
        let mut response_ms = self.latency();
        // The failure the caller hears, and whether the call applied anyway.
        let mut lost = None;
        if self.t < self.config.chaos_ms {
            let f = &self.config.faults;
            if self.net_rng.chance(f.store_spike_probability) {
                request_ms += self.net_rng.range(0, f.store_spike_ms);
            }
            if self.net_rng.chance(f.store_spike_probability) {
                response_ms += self.net_rng.range(0, f.store_spike_ms);
            }
            lost = if self.net_rng.chance(f.store_definite) {
                Some((Failure::Definite, false))
            } else if self.net_rng.chance(f.store_lost_request) {
                Some((Failure::Ambiguous, false))
            } else if self.net_rng.chance(f.store_lost_response) {
                Some((Failure::Ambiguous, true))
            } else {
                None
            };
        }
        let lost = match self.machines[node].partition {
            Some((_, Partition::Refuse)) => Some((Failure::Definite, false)),
            Some((_, Partition::Hang)) => Some((Failure::Ambiguous, false)),
            None => lost,
        };
        // Too slow: the caller gives up, and the request still applies, late.
        let slow = request_ms + response_ms > timeout;
        let lost = lost.or(slow.then_some((Failure::Ambiguous, true)));
        match lost {
            None => Fate::Answered {
                request_ms,
                response_ms,
            },
            Some((failure, applies)) => Fate::Failed {
                failure,
                at: match failure {
                    Failure::Definite => request_ms.min(timeout),
                    Failure::Ambiguous => timeout,
                },
                applies_after: applies.then_some(request_ms),
            },
        }
    }

    /// What the caller hears when a call fails.
    fn store_failed(&self, node: usize, req: &StoreReq, failure: Failure) -> Work {
        let core = Work::Deliver;
        match req {
            StoreReq::ReadSelfLease { op } => core(Event::SelfNodeLeaseRead {
                op: *op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Err(failure),
            }),
            StoreReq::CasLease { op, record, .. } => core(Event::NodeLeaseCasCompleted {
                op: *op,
                now_mono_ms: 0,
                result: Err(failure),
                stamped_log_state: record.log_state,
            }),
            StoreReq::ReadCapacityPeers { op } => core(Event::CapacityPeersRead {
                op: *op,
                now_ms: self.wall(node),
                result: Err(failure),
            }),
            StoreReq::ReadOwner { op, .. } => core(Event::OwnerRead {
                op: *op,
                now_ms: 0,
                result: Err(failure),
            }),
            StoreReq::ReadLease { op, .. } => core(Event::NodeLeaseRead {
                op: *op,
                now_ms: 0,
                result: Err(failure),
            }),
            StoreReq::CasOwner { op, .. } => core(Event::OwnerCasCompleted {
                op: *op,
                result: Err(failure),
            }),
            StoreReq::ReleaseOwner { op, .. } => core(Event::OwnerReleased {
                op: *op,
                result: Err(failure),
            }),
            StoreReq::Verify { op, .. } => core(Event::OwnershipVerified {
                op: *op,
                result: Err(failure),
            }),
            StoreReq::Upload { cell, epoch, .. } => Work::Replica(ReplicaWork::UploadFailed {
                cell: cell.clone(),
                epoch: *epoch,
            }),
            // Production reports every failed restore as definite
            // (`Effect::Restore` in `actor.rs`): nothing was opened.
            StoreReq::ReadChain { op, .. } | StoreReq::PutMarker { op, .. } => {
                core(Event::RestoreCompleted {
                    op: *op,
                    result: Err(Failure::Definite),
                })
            }
            // The arm gate turns its response into a failure.
            StoreReq::Wake {
                gate: Some(waiter), ..
            } => Work::Alarm(AlarmWork::GateFailed { waiter: *waiter }),
            // Logged and left for a later scan or reconcile.
            StoreReq::Wake { gate: None, .. } => Work::Nothing,
            // The pass ends; the next tick starts another.
            StoreReq::Gc(_) => Work::Nothing,
            StoreReq::Bundle { .. } => Work::Log(LogWork::Bundled(Flushed::Failed)),
            StoreReq::Reopen { .. } => Work::Log(LogWork::Reopened(None)),
            StoreReq::Compact { cell, epoch, .. } => Work::Replica(ReplicaWork::Compacted {
                cell: cell.clone(),
                epoch: *epoch,
                len: None,
            }),
            // The next tick lists them again.
            StoreReq::DeleteBundles { .. } => Work::Nothing,
            StoreReq::ReadPeers => Work::Log(LogWork::PeersRead(None)),
            StoreReq::Waker(req) => World::waker_failed(req),
        }
    }

    fn latency(&mut self) -> u64 {
        let (lo, hi) = self.config.store_latency_ms;
        self.net_rng.range(lo, hi)
    }

    /// The request reaches the bucket: apply it now, at this one instant,
    /// and send the answer back.
    pub(super) fn store_apply(
        &mut self,
        node: usize,
        inc: u64,
        req: StoreReq,
        answer_ms: Option<u64>,
    ) {
        let name = self.machines[node].name.clone();
        let sender = self.who_at(node, inc);
        let core = Work::Deliver;
        let reply = match req {
            StoreReq::ReadSelfLease { op } => core(Event::SelfNodeLeaseRead {
                op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Ok(self.store.read_lease(&name)),
            }),
            StoreReq::CasLease {
                op,
                guard,
                record,
                load,
                acks,
            } => {
                let result = self.store.cas_lease(&guard, &record, load);
                let opened = matches!(result, LeaseCasOutcome::Applied { .. })
                    && record.log_state == Some(LogState::Open);
                let acks = acks.filter(|_| opened);
                if let Some(log_epoch) = acks {
                    self.apply_activation(&record.generation, log_epoch);
                }
                let event = Event::NodeLeaseCasCompleted {
                    op,
                    now_mono_ms: 0,
                    result: Ok(result),
                    stamped_log_state: record.log_state,
                };
                match opened {
                    true => Work::Log(LogWork::LeaseApplied { event, acks }),
                    false => core(event),
                }
            }
            StoreReq::ReadOwner { op, cell } => core(Event::OwnerRead {
                op,
                now_ms: 0,
                result: Ok(self.store.read_owner(&cell)),
            }),
            StoreReq::ReadCapacityPeers { op } => core(Event::CapacityPeersRead {
                op,
                // The core judges the sample at the instant it was taken.
                now_ms: self.wall(node),
                result: Ok(self.store.capacity_peers()),
            }),
            StoreReq::ReadLease { op, node: owner } => core(Event::NodeLeaseRead {
                op,
                now_ms: 0,
                result: Ok(self.store.read_lease(&owner)),
            }),
            StoreReq::CasOwner {
                op,
                cell,
                guard,
                epoch,
                shutting_down,
            } => {
                let prior = self
                    .store
                    .owner(&cell)
                    .and_then(|(owner, _)| owner.map(str::to_string));
                let outcome = self.store.cas_owner(&cell, &guard, epoch, &name);
                // The oracle believes the bucket, not the answer: a claim
                // is the epoch the record names this node at afterwards.
                let installed = match self.store.owner(&cell) {
                    Some((Some(owner), epoch)) if owner == name => Some(epoch),
                    _ => None,
                };
                if let (CasOutcome::Applied, Some(epoch)) = (outcome, installed) {
                    self.checker.claim_applied(self.t, &cell, epoch, &name);
                    if let Some(phase) = shutting_down {
                        self.judge_claim(&sender, &name, &cell, epoch, prior, phase);
                    }
                }
                core(Event::OwnerCasCompleted {
                    op,
                    result: Ok(outcome),
                })
            }
            StoreReq::ReleaseOwner { op, cell, epoch } => core(Event::OwnerReleased {
                op,
                result: Ok(self.store.release_owner(&cell, epoch, &name)),
            }),
            StoreReq::Verify { op, cell, epoch } => {
                let ours = self
                    .store
                    .read_owner(&cell)
                    .is_some_and(|r| r.node.as_ref() == Some(&name) && r.epoch == epoch);
                if ours {
                    self.checker.ownership_read(self.t, &sender, &cell, epoch);
                }
                core(Event::OwnershipVerified {
                    op,
                    result: if ours { Ok(()) } else { Err(Failure::Definite) },
                })
            }
            StoreReq::Upload {
                cell,
                epoch,
                content,
            } => {
                self.land_upload(&sender, &cell, epoch, &content);
                Work::Replica(ReplicaWork::Uploaded {
                    cell,
                    epoch,
                    len: content.len(),
                })
            }
            StoreReq::PutMarker {
                op,
                cell,
                epoch,
                content,
            } => {
                self.checker.upload_applied(self.t, &sender, &cell, epoch);
                self.store
                    .put_marker(&cell, epoch, &content, WALL_EPOCH_MS + self.t);
                self.checker.bucket_holds(self.t, &cell, epoch, &content);
                self.judge_discoverable(&cell);
                Work::Replica(ReplicaWork::Marked {
                    op,
                    cell,
                    epoch,
                    content,
                })
            }
            StoreReq::ReadChain {
                op,
                cell,
                epoch,
                fold,
            } => match fold.then(|| self.fold_tail(node, inc, &cell)) {
                Some(Err(failure)) => core(Event::RestoreCompleted {
                    op,
                    result: Err(failure),
                }),
                // `epoch_chain` lists every epoch and refuses a restore into
                // an epoch some later writer already uploaded at: this
                // claim was superseded before its restore got here.
                _ => Work::Replica(ReplicaWork::ChainRead {
                    op,
                    chain: match self.store.used_since(&cell, epoch) {
                        true => Err(Failure::Definite),
                        false => self.store.read_chain(&cell, epoch).map_err(|NoBase| {
                            // The restore fails, as `EpochChain::build`'s
                            // `TxNotAvailable` fails the activation.
                            self.checker.unrestorable(self.t, &name, &cell, epoch);
                            Failure::Definite
                        }),
                    },
                    cell,
                    epoch,
                }),
            },
            StoreReq::Gc(req) => self.gc_apply(node, req),
            StoreReq::Bundle {
                session,
                log_epoch,
                rows,
            } => Work::Log(LogWork::Bundled(
                self.apply_bundle(&name, &session, log_epoch, rows),
            )),
            StoreReq::Compact {
                session,
                cell,
                epoch,
            } => {
                let len = self.apply_compaction(&sender, &session, &cell, epoch);
                Work::Replica(ReplicaWork::Compacted {
                    cell,
                    epoch,
                    len: Some(len),
                })
            }
            StoreReq::DeleteBundles { session, seqs } => {
                self.delete_bundles(&session, &seqs);
                Work::Nothing
            }
            StoreReq::ReadPeers => {
                let peers = (self.store.capacity_peers().into_iter())
                    .filter_map(|peer| {
                        let machine = self.machines.iter().position(|m| m.name == peer.node)?;
                        Some((machine, peer.expires_ms))
                    })
                    .collect();
                Work::Log(LogWork::PeersRead(Some(peers)))
            }
            StoreReq::Reopen {
                session,
                from,
                ensemble,
            } => Work::Log(LogWork::Reopened(
                self.apply_reopen(&name, &session, from, ensemble),
            )),
            StoreReq::Waker(req) => self.waker_apply(node, req),
            StoreReq::Wake {
                cell,
                alarm,
                report,
                retire,
                gate,
            } => {
                self.store.publish_wake(&cell, alarm);
                let retirement = retire
                    .then(|| self.retirement_proven(node, inc, &cell, alarm))
                    .flatten();
                if let Some(retirement) = retirement {
                    self.store.retire_wake(&cell, retirement);
                }
                self.judge_discoverable(&cell);
                Work::Alarm(AlarmWork::Maintained {
                    cell,
                    alarm,
                    report,
                    gate,
                    retired: retirement.is_some(),
                })
            }
        };
        // The answer goes back to the process that asked. If it has died
        // since, or the answer is lost, nobody hears it, but what it answers
        // has still happened.
        if let Some(delay) = answer_ms {
            let work = reply;
            self.after(delay, Item::Proc { node, inc, work });
        }
    }
}

/// Fill in the clocks an event carries, sampled on the receiving node at
/// the instant it is delivered. Production samples them at the same point.
pub(super) fn stamp(event: &mut Event, wall: u64, mono: u64) {
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
        | Event::NudgeNodeLease {
            now_ms,
            now_mono_ms,
        }
        | Event::HandoffRequestAt {
            now_ms,
            now_mono_ms,
            ..
        }
        | Event::WebSocketRequestAt {
            now_ms,
            now_mono_ms,
            ..
        }
        | Event::AlarmObserved {
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
        Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::OutputAt { now_mono_ms, .. }
        | Event::LoadSampled { now_mono_ms, .. }
        | Event::EvictRequested { now_mono_ms, .. }
        | Event::GateRequestAt { now_mono_ms, .. }
        | Event::GenerationChanged { now_mono_ms, .. }
        | Event::WakeHintAt { now_mono_ms, .. } => *now_mono_ms = mono,
        Event::OwnerRead { now_ms, .. } | Event::NodeLeaseRead { now_ms, .. } => *now_ms = wall,
        // Stamped where it is built: the core judges a capacity sample at
        // the instant the bucket took it, not when the answer arrives.
        Event::CapacityPeersRead { .. } => {}
        // No clock. Listed by name, so a new event is classified here
        // before it compiles.
        Event::LocalCellsRead { .. }
        | Event::Request { .. }
        | Event::WorkerRequest { .. }
        | Event::BeginPreserve
        | Event::Cancel { .. }
        | Event::ActivityFinished { .. }
        | Event::Output { .. }
        | Event::DurableReached { .. }
        | Event::OwnershipVerified { .. }
        | Event::WebSocketOpened { .. }
        | Event::WebSocketClosed { .. }
        | Event::WakeHint { .. }
        | Event::NodeLogRecovered { .. }
        | Event::OwnerCasCompleted { .. }
        | Event::OwnerReleased { .. }
        | Event::SuccessorAdopted { .. }
        | Event::RestoreCompleted { .. }
        | Event::RuntimeStarted { .. }
        | Event::Published { .. }
        | Event::DurabilityChecked { .. }
        | Event::RuntimeStopped { .. }
        | Event::RuntimeStopFailed { .. }
        | Event::RuntimeStopCancelled { .. }
        | Event::Evict { .. }
        | Event::InvalidateRemote { .. }
        | Event::NodeFenced
        | Event::ReleaseAll
        | Event::Rebalance { .. } => {}
    }
}
