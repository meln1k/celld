//! The shell: carry out each effect a core returns, as `Actor::execute` in
//! `crates/celld/actor.rs` does. Requests to the bucket go through its
//! client (`bucket.rs`).

use std::collections::VecDeque;

use celld_logic::log_tier::LogState;
use celld_logic::{Effect, Event, Failure, StopCause, Timer};

use super::bucket::{Fate, StoreReq, STORE_TIMEOUT_MS};
use super::node_log::LogWork;
use super::{Pending, Work, World};
use crate::store::Load;
use crate::Mutation;

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

impl World {
    /// Carry out one effect. It either finishes now (its event goes on
    /// `immediate`, as in `Actor::drive`) or starts work whose completion
    /// is scheduled on the agenda.
    pub(super) fn execute(&mut self, node: usize, effect: Effect, immediate: &mut VecDeque<Event>) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                let mono = self.mono(node);
                let n = self.machines[node].proc_mut();
                if self.config.mutation == Some(Mutation::DropLeaseRetry)
                    && matches!(timer, Timer::NodeLeaseRenew { .. })
                    && !n.state.node_authoritative()
                {
                    return;
                }
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
                    restoring: p.state.activation_backlog() as u64,
                    draining: p.state.draining(),
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
                let key = (cell.clone(), epoch);
                // The second half of a swap runs the new generation's code,
                // which may not load (`Faults::start_failures`).
                let swap = p.swapped.remove(&key);
                let new_code_fails = swap
                    && self.t < self.config.chaos_ms
                    && self.config.faults.start_failures > 0.0
                    && (self.start_rng).chance(self.config.faults.start_failures);
                let result = match self.machines[node].proc().replicas.contains_key(&key) {
                    true if !new_code_fails => Ok(()),
                    _ => Err(Failure::Definite),
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
