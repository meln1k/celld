//! The shell: carry out each effect a core returns, as `Actor::execute` in
//! `crates/celld/actor.rs` does, and talk to the bucket.

use std::collections::VecDeque;

use celld_logic::{
    CasGuard, CasOutcome, Effect, Epoch, Event, Failure, NodeLeaseRecord, OpId, ProofSource, Timer,
};

use super::{Item, World};
use crate::store::Load;
use crate::WriteId;

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
            } => {
                // The lease body carries the node's load, sampled now.
                let state = &self.nodes[node].state;
                let load = Load {
                    resident: state.residents().len(),
                    owned: state.owned_cells(),
                    sampled_ms: self.wall(),
                };
                let req = StoreReq::CasLease {
                    op,
                    guard,
                    record,
                    load,
                };
                self.store_call(node, req)
            }
            Effect::ReadCapacityPeers { op, .. } => {
                self.store_call(node, StoreReq::ReadCapacityPeers { op })
            }
            Effect::ReadLocalCells => {
                // No disk yet, so no local cells. Production scans on a
                // blocking task, so the answer arrives a little later.
                let delay = self.net_rng.range(1, 5);
                self.deliver(node, delay, Event::LocalCellsRead { result: Ok(vec![]) });
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
            } => self.await_durable(node, op, cell, epoch, position),
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

    pub(super) fn timer(&mut self, node: usize, slot: Slot, ordinal: u64) {
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
    /// its answer arrives after another, so other events can happen in
    /// between, on this node and on others.
    pub(super) fn store_call(&mut self, node: usize, req: StoreReq) {
        let delay = self.latency();
        self.after(delay, Item::StoreApply { node, req });
    }

    fn latency(&mut self) -> u64 {
        let (lo, hi) = self.config.store_latency_ms;
        self.net_rng.range(lo, hi)
    }

    /// The request reaches the bucket: apply it now, at this one instant,
    /// and send the answer back. A lost response will be an answer that is
    /// never scheduled; the write it answers has still happened.
    pub(super) fn store_apply(&mut self, node: usize, req: StoreReq) {
        let name = self.nodes[node].name.clone();
        let answer = match req {
            StoreReq::ReadSelfLease { op } => Event::SelfNodeLeaseRead {
                op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Ok(self.store.read_lease(&name)),
            },
            StoreReq::CasLease {
                op,
                guard,
                record,
                load,
            } => Event::NodeLeaseCasCompleted {
                op,
                now_mono_ms: 0,
                result: Ok(self.store.cas_lease(&guard, &record, load)),
                // The bucket posture: lease bodies carry no node-log state.
                stamped_log_state: None,
            },
            StoreReq::ReadOwner { op, cell } => Event::OwnerRead {
                op,
                now_ms: 0,
                result: Ok(self.store.read_owner(&cell)),
            },
            StoreReq::ReadCapacityPeers { op } => Event::CapacityPeersRead {
                op,
                // The core judges the sample at the instant it was taken.
                now_ms: self.wall(),
                result: Ok(self.store.capacity_peers()),
            },
            StoreReq::ReadLease { op, node } => Event::NodeLeaseRead {
                op,
                now_ms: 0,
                result: Ok(self.store.read_lease(&node)),
            },
            StoreReq::CasOwner {
                op,
                cell,
                guard,
                epoch,
            } => {
                let outcome = self.store.cas_owner(&cell, &guard, epoch, &name);
                if outcome == CasOutcome::Applied {
                    self.checker.claim_applied(&cell, epoch, &name);
                }
                Event::OwnerCasCompleted {
                    op,
                    result: Ok(outcome),
                }
            }
            StoreReq::Verify { op, cell, epoch } => {
                let ours = self
                    .store
                    .read_owner(&cell)
                    .is_some_and(|r| r.node.as_ref() == Some(&name) && r.epoch == epoch);
                Event::OwnershipVerified {
                    op,
                    result: if ours { Ok(()) } else { Err(Failure::Definite) },
                }
            }
            StoreReq::Upload {
                cell,
                epoch,
                content,
            } => {
                self.checker.upload_applied(self.t, &name, &cell, epoch);
                self.store.put(&cell, epoch, &content);
                let len = content.len();
                let delay = self.latency();
                return self.after(
                    delay,
                    Item::Uploaded {
                        node,
                        cell,
                        epoch,
                        len,
                    },
                );
            }
            StoreReq::ReadChain { op, cell, epoch } => {
                let chain = self.store.read_chain(&cell, epoch);
                let delay = self.latency();
                return self.after(
                    delay,
                    Item::ChainRead {
                        node,
                        op,
                        cell,
                        epoch,
                        chain,
                    },
                );
            }
        };
        let delay = self.latency();
        self.deliver(node, delay, answer);
    }

    /// Ask the replicator to prove `position` durable. With no open
    /// database there is nothing to prove, and the proof fails.
    fn await_durable(&mut self, node: usize, op: OpId, cell: String, epoch: Epoch, position: u64) {
        if !self.wait_for_upload(node, &cell, epoch, op, position) {
            let event = Event::DurableReached {
                op,
                result: Err(Failure::Ambiguous),
                source: ProofSource::Bucket,
            };
            self.deliver(node, 1, event);
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
        } => {
            *now_ms = wall;
            *now_mono_ms = mono;
        }
        Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::OutputAt { now_mono_ms, .. } => *now_mono_ms = mono,
        Event::OwnerRead { now_ms, .. } | Event::NodeLeaseRead { now_ms, .. } => *now_ms = wall,
        _ => {}
    }
}
