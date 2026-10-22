//! Eviction stops, and the operator's evictions that ask for them.
//!
//! `report_stopped_when_released` in `crates/celld/actor.rs` reports a
//! cell's runtime stopped only once the host has let the cell go, and tries
//! again until it has. An eviction's pass (`LtxRepl::evict`) first gives the
//! isolate turn back and captures the handoff snapshot, which a request for
//! the cell can still take back, and then publishes it, which nothing can.
//! Here an attempt is a capture of up to `Stops::max_capture_ms` and then
//! the cell's final upload. A wedged host fails every attempt until it
//! recovers, and only a failed attempt reads a bounded stop's deadline, as
//! production's loop does.
//!
//! An operator's eviction (`POST /evict`) is answered once: at admission
//! when the core settles it there, or when the operation it joined ends.
//! The checker holds each success to what the process still runs.

use super::*;
use celld_logic::{EvictError, EvictSuccess, EvictionAdmission};

/// `CELL_RELEASE_RETRY_BASE_MS`: the pause before a failed stop tries again.
const RELEASE_RETRY_MS: u64 = 50;

#[derive(Debug)]
pub(super) struct Stop {
    pub op: OpId,
    /// The database moves to `.evicted` instead of being removed.
    pub preserve: bool,
    pub phase: StopPhase,
    /// An eviction captures a snapshot before its upload. A cleanup does not.
    evict: bool,
    /// A bounded stop's deadline, on its process's monotonic clock. Only a
    /// bounded eviction carries the abandon signal.
    deadline_mono: Option<u64>,
    /// A request reached the cell: `EvictionAbandon::request`.
    abandon: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StopPhase {
    /// Giving the isolate turn back and capturing the snapshot. An
    /// abandonment takes effect when this ends.
    Capturing,
    /// The final upload, past the point an abandonment can take back.
    Publishing,
    /// The pause after a failed attempt.
    Retrying,
}

impl Stop {
    /// Whether the stop finishes once its final upload lands.
    pub(super) fn publishing(&self) -> bool {
        self.phase == StopPhase::Publishing
    }
}

/// An operator's eviction waiting for its answer.
#[derive(Debug)]
pub(super) struct Asked {
    cell: String,
    t: Time,
}

impl World {
    fn stop_mut(&mut self, id: ProcId, cell: &str, epoch: Epoch) -> Option<&mut Stop> {
        self.procs
            .get_mut(&id)?
            .replicas
            .get_mut(&(cell.to_string(), epoch))?
            .stop
            .as_mut()
    }

    /// `StopRuntime` for a stop that releases through the replicator: a
    /// cleanup (`StopMode::Rebase`) or an eviction.
    pub(super) fn begin_stop(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: &str,
        epoch: Epoch,
        cause: StopCause,
        bounded: bool,
    ) {
        let deadline_mono = bounded.then(|| self.mono(id) + OPERATION_DEADLINE_MS);
        let key = (cell.to_string(), epoch);
        let replica = self
            .procs
            .get_mut(&id)
            .unwrap()
            .replicas
            .get_mut(&key)
            .unwrap();
        replica.stop = Some(Stop {
            op,
            preserve: !matches!(cause, StopCause::Evict { rebalance: true }),
            phase: StopPhase::Capturing,
            evict: matches!(cause, StopCause::Evict { .. }),
            deadline_mono,
            abandon: false,
        });
        self.attempt_stop(id, cell, epoch);
    }

    /// `Work::StopStep`: the capture ended, or the retry pause did.
    pub(super) fn stop_step(&mut self, id: ProcId, cell: &str, epoch: Epoch, op: OpId) {
        let phase = self
            .stop_mut(id, cell, epoch)
            .filter(|stop| stop.op == op)
            .map(|stop| stop.phase);
        match phase {
            Some(StopPhase::Capturing) => self.captured(id, cell, epoch),
            Some(StopPhase::Retrying) => self.attempt_stop(id, cell, epoch),
            Some(StopPhase::Publishing) | None => {}
        }
    }

    /// One pass of `report_stopped_when_released`'s loop.
    fn attempt_stop(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let max_capture_ms = self.config.stops.max_capture_ms;
        let stop = self.stop_mut(id, cell, epoch).unwrap();
        if stop.abandon {
            return self.abandoned(id, cell, epoch);
        }
        stop.phase = StopPhase::Capturing;
        let (op, evict) = (stop.op, stop.evict);
        let capture_ms = if evict && max_capture_ms > 0 {
            self.net_rng.range(0, max_capture_ms + 1)
        } else {
            0
        };
        if capture_ms == 0 {
            self.captured(id, cell, epoch);
        } else {
            let (cell, epoch) = (cell.to_string(), epoch);
            self.work(id, capture_ms, Work::StopStep { cell, epoch, op });
        }
    }

    /// The snapshot is captured: the last point the eviction can stop.
    fn captured(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let wedged = self.t < self.procs[&id].wedged_until;
        let stop = self.stop_mut(id, cell, epoch).unwrap();
        if stop.abandon {
            return self.abandoned(id, cell, epoch);
        }
        if wedged {
            return self.stop_attempt_failed(id, cell, epoch);
        }
        stop.phase = StopPhase::Publishing;
        self.settle_waiters(id, cell, epoch);
        self.kick_upload(id, cell, epoch);
    }

    /// The host did not let the cell go. Only now does a bounded stop read
    /// its deadline.
    fn stop_attempt_failed(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let mono = self.mono(id);
        let key = (cell.to_string(), epoch);
        let replica = self
            .procs
            .get_mut(&id)
            .unwrap()
            .replicas
            .get_mut(&key)
            .unwrap();
        let stop = replica.stop.as_mut().unwrap();
        let op = stop.op;
        if stop.deadline_mono.is_some_and(|deadline| mono >= deadline) {
            // The runtime is gone and the database stays open: the core
            // restarts the cell in place.
            replica.stop = None;
            self.work(id, 1, Work::Deliver(Event::RuntimeStopFailed { op }));
            return;
        }
        stop.phase = StopPhase::Retrying;
        self.stats.stop_retries += 1;
        let (cell, epoch) = (cell.to_string(), epoch);
        self.work(id, RELEASE_RETRY_MS, Work::StopStep { cell, epoch, op });
    }

    /// `EvictionAbandoned`: the cell stays resident at its epoch, and the
    /// core starts it again in place.
    fn abandoned(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        let replica = self
            .procs
            .get_mut(&id)
            .unwrap()
            .replicas
            .get_mut(&key)
            .unwrap();
        let stop = replica.stop.take().unwrap();
        if self.config.mutation != Mutation::SilentAbandon {
            let op = stop.op;
            self.work(id, 1, Work::Deliver(Event::RuntimeStopCancelled { op }));
        }
    }

    /// `AbandonEvictionStop`. It only sets the bit; the stop reads it where
    /// it can still act on it, and a stop past that point ignores it.
    pub(super) fn abandon_stop(&mut self, id: ProcId, op: OpId, cell: &str) {
        let proc = self.procs.get_mut(&id).unwrap();
        let stop = proc
            .replicas
            .iter_mut()
            .filter(|((name, _), _)| name == cell)
            .find_map(|(_, replica)| replica.stop.as_mut().filter(|stop| stop.op == op));
        if let Some(stop) = stop.filter(|stop| stop.evict && stop.deadline_mono.is_some()) {
            stop.abandon = true;
        }
    }

    // -----------------------------------------------------------------------
    // The operator's evictions

    /// An evict tick the way `POST /evict` asks (`Message::Evict`): any
    /// cell, resident here or not.
    pub(super) fn operator_evict(&mut self, id: ProcId) {
        let Some(cell) = self.work_rng.pick(&self.cells).cloned() else {
            return;
        };
        self.stats.operator_evictions += 1;
        if self.preserving(id) {
            // The actor refuses it without asking the core.
            return;
        }
        let request = self.next_request;
        self.next_request += 1;
        let asked = Asked {
            cell: cell.clone(),
            t: self.t,
        };
        self.procs
            .get_mut(&id)
            .unwrap()
            .evictions
            .insert(request, asked);
        let event = Event::EvictRequested {
            request,
            cell,
            now_mono_ms: 0,
        };
        self.drive(id, event);
    }

    /// `EvictionAdmission`: a pending admission waits for the operation it
    /// joined to finish.
    pub(super) fn eviction_admitted(
        &mut self,
        id: ProcId,
        request: RequestId,
        result: Result<EvictionAdmission, EvictError>,
    ) {
        let answer = match result {
            Ok(EvictionAdmission::Pending)
                if self.config.mutation != Mutation::AnswerOnAdmission =>
            {
                return;
            }
            Ok(EvictionAdmission::Pending) => Ok(EvictSuccess::Evicted),
            Ok(EvictionAdmission::Absent) => Ok(EvictSuccess::AlreadyAbsent),
            Err(error) => Err(error),
        };
        self.eviction_answered(id, request, answer);
    }

    /// The operator has its answer. A request answered already, or by a
    /// process since replaced, has no one waiting.
    pub(super) fn eviction_answered(
        &mut self,
        id: ProcId,
        request: RequestId,
        result: Result<EvictSuccess, EvictError>,
    ) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(asked) = proc.evictions.remove(&request) else {
            return;
        };
        let running = proc.published.contains_key(&asked.cell)
            || proc
                .replicas
                .iter()
                .any(|((cell, _), replica)| *cell == asked.cell && replica.running);
        if let Ok(success) = result {
            let (checker, view) = self.checks();
            checker.on_eviction_succeeded(view, id, &asked.cell, success, running);
        }
    }

    /// At the audit: the evictions every live process still owes an answer,
    /// of those asked before the faults healed.
    pub(super) fn check_evictions_answered(&mut self) {
        let healed = self.config.chaos_ms;
        let waiting: Vec<(ProcId, String)> = self
            .procs
            .iter()
            .flat_map(|(id, proc)| {
                proc.evictions
                    .values()
                    .filter(|asked| asked.t < healed)
                    .map(|asked| (*id, asked.cell.clone()))
            })
            .collect();
        let (checker, view) = self.checks();
        checker.on_evictions_unanswered(view, &waiting);
    }
}
