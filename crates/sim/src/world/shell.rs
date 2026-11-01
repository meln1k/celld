//! The shell: carry out each effect a core returns, as `Actor::execute` in
//! `crates/celld/actor.rs` does, and talk to the bucket.

use std::collections::VecDeque;

use celld_logic::{
    CasGuard, CasOutcome, Effect, Epoch, Event, Failure, NodeLeaseRecord, OpId, StopCause, Timer,
};

use super::faults::Partition;
use super::{Item, Work, World};
use crate::store::Load;
use crate::{Mutation, WriteId};

/// The bucket client's timeout for one attempt (`bucket.rs`).
const STORE_TIMEOUT_MS: u64 = 15_000;

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
enum Fate {
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
    /// A restore reads the newest chain below its epoch.
    ReadChain {
        op: OpId,
        cell: String,
        epoch: Epoch,
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
                record,
                authority_expires_ms,
            } => {
                // The lease body carries the node's load, sampled now.
                let state = &self.machines[node].proc().state;
                let load = Load {
                    // Activating cells hold a slot too, as production reports.
                    resident: state.occupied(),
                    owned: state.owned_cells(),
                    sampled_ms: self.wall(node),
                };
                // A renewal must come back while authority remains, so the
                // shell bounds it to half of what is left, as production
                // does (`actor/effects.rs`), and a timeout is ambiguous.
                let timeout = authority_expires_ms.map_or(STORE_TIMEOUT_MS, |expires_ms| {
                    let remaining = expires_ms.saturating_sub(self.wall(node));
                    (remaining / 2).clamp(250, 2_500)
                });
                let req = StoreReq::CasLease {
                    op,
                    guard,
                    record,
                    load,
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
            Effect::RecoverNodeLog { op, .. } => {
                // No fleet log in this model: a lease never carries log
                // state, so a dead owner never leaves a tail to recover.
                immediate.push_back(Event::NodeLogRecovered { op, result: Ok(()) });
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
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
                bounded,
            } => {
                let leak = matches!(cause, StopCause::Evict { .. })
                    && self.config.mutation == Some(Mutation::StaleRegistry);
                if !leak {
                    self.machines[node].proc_mut().published.remove(&cell);
                }
                self.stop_runtime(node, op, cell, epoch, cause, bounded);
            }
            Effect::StartRuntime { op, cell, epoch } => {
                let (name, who) = (self.machines[node].name.clone(), self.who(node));
                self.checker
                    .runtime_started(self.t, &name, &who, &cell, epoch);
                let result = if self.machines[node]
                    .proc()
                    .replicas
                    .contains_key(&(cell, epoch))
                {
                    Ok(())
                } else {
                    Err(Failure::Definite)
                };
                let generation = self.machines[node].proc().state.current_generation();
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
                if self.config.mutation != Some(Mutation::SkipPublish) {
                    self.machines[node].proc_mut().published.insert(cell, epoch);
                }
                immediate.push_back(Event::Published { op, result: Ok(()) });
            }
            Effect::Complete { request, result } => {
                if !self.offer_routed(node, request, &result, immediate) {
                    self.complete(node, request, result, immediate)
                }
            }
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => self.await_durable(node, op, &cell, epoch, position),
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
                ..
            } => {
                self.abort_requests(node, requests);
                for websocket in websockets {
                    immediate.push_back(Event::WebSocketClosed {
                        cell: cell.clone(),
                        websocket,
                    });
                }
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
            other => unimplemented!("the simulated shell does not carry out {other:?} yet"),
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

    /// Draw the fate of one call: its latencies, then the faults that
    /// override them. A partition overrides everything: the call never
    /// gets out.
    fn fate(&mut self, node: usize, timeout: u64) -> Fate {
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
            StoreReq::CasLease { op, .. } => core(Event::NodeLeaseCasCompleted {
                op: *op,
                now_mono_ms: 0,
                result: Err(failure),
                stamped_log_state: None,
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
            StoreReq::Upload { cell, epoch, .. } => Work::UploadFailed {
                cell: cell.clone(),
                epoch: *epoch,
            },
            // Production reports every failed restore as definite
            // (`Effect::Restore` in `actor.rs`): nothing was opened.
            StoreReq::ReadChain { op, .. } => core(Event::RestoreCompleted {
                op: *op,
                result: Err(Failure::Definite),
            }),
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
        let sender = format!("{name}#{inc}");
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
            } => core(Event::NodeLeaseCasCompleted {
                op,
                now_mono_ms: 0,
                result: Ok(self.store.cas_lease(&guard, &record, load)),
                // The bucket posture: lease bodies carry no node-log state.
                stamped_log_state: None,
            }),
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
            } => {
                let outcome = self.store.cas_owner(&cell, &guard, epoch, &name);
                // The oracle believes the bucket, not the answer: a claim
                // is the epoch the record names this node at afterwards.
                let installed = match self.store.owner(&cell) {
                    Some((Some(owner), epoch)) if owner == name => Some(epoch),
                    _ => None,
                };
                if let (CasOutcome::Applied, Some(epoch)) = (outcome, installed) {
                    self.checker.claim_applied(self.t, &cell, epoch, &name);
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
                self.checker.upload_applied(self.t, &sender, &cell, epoch);
                self.store.put(&cell, epoch, &content);
                let held = self.store.prefix(&cell, epoch).len();
                self.checker.prefix_grew(self.t, &cell, epoch, held);
                Work::Uploaded {
                    cell,
                    epoch,
                    len: content.len(),
                }
            }
            StoreReq::ReadChain { op, cell, epoch } => Work::ChainRead {
                op,
                chain: self.store.read_chain(&cell, epoch),
                cell,
                epoch,
            },
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
