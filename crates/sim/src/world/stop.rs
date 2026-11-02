//! A clean stop (an eviction or a cleanup), as
//! `report_stopped_when_released` in `crates/celld/actor.rs` runs it:
//! attempt after attempt until one lets the cell go.
//!
//! An eviction's attempt first captures its handoff snapshot
//! (`LtxRepl::evict`), and an abandonment (`EvictionAbandon`) takes effect
//! only before that capture ends; past it the attempt publishes, and
//! nothing takes it back. An attempt fails when its final upload does not
//! land in time, or as soon as an upload of it fails (`sync_wait` answers
//! `Failed`, and `evict` returns the error). Only then does a bounded stop
//! read its deadline; any other stop tries again after a pause.

use celld_logic::{Epoch, Event, OpId, StopCause};

use super::{AgendaKey, Work, World, OPERATION_DEADLINE_MS};

/// `CELL_RELEASE_RETRY_BASE_MS`: the pause before a failed stop tries again.
const RELEASE_RETRY_MS: u64 = 50;
/// How long one attempt of a clean stop waits for its final upload: an
/// eviction's `CELLD_LTX_DURABILITY_TIMEOUT_SECS`, and a cleanup's
/// `final_durability_barrier`, 10 s each.
const STOP_ATTEMPT_MS: u64 = 10_000;
/// The longest an eviction's capture of its handoff snapshot takes.
const MAX_CAPTURE_MS: u64 = 300;

pub(super) struct Stop {
    pub(super) op: OpId,
    /// An eviction captures a snapshot before it publishes; a cleanup
    /// does not.
    evict: bool,
    /// Whether the closed database stays on the node as an eviction
    /// snapshot.
    pub(super) snapshot: bool,
    /// Only a bounded eviction carries the abandon signal
    /// (`(evicting && bounded).then(...)` in `actor.rs`).
    abandonable: bool,
    /// A bounded stop's deadline, on the process's monotonic clock.
    deadline_mono: Option<u64>,
    /// A request reached the cell (`AbandonEvictionStop`).
    abandon: bool,
    phase: Phase,
    /// When the attempt under way runs out of time, on the agenda: its
    /// budget covers the capture and the publish (`evict` starts its
    /// deadline before `prepare_handoff_snapshot`).
    attempt_until: u64,
    /// The step the stop waits for next, cancelled when it moves on.
    pub(super) step: Option<AgendaKey>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Capturing the handoff snapshot; an abandonment still takes effect.
    Capturing,
    /// Publishing: the final upload, past the point of abandoning.
    Publishing,
    /// The pause after a failed attempt.
    Retrying,
}

impl Stop {
    /// Whether the stop finishes once its final upload lands.
    pub(super) fn publishing(&self) -> bool {
        self.phase == Phase::Publishing
    }
}

/// The step a stop waits for.
#[derive(Debug)]
pub(super) enum StopWork {
    /// The eviction's snapshot is captured.
    Captured { cell: String, epoch: Epoch },
    /// The attempt's final upload did not land in time.
    TimedOut { cell: String, epoch: Epoch },
    /// The pause after a failed attempt is over.
    Retry { cell: String, epoch: Epoch },
}

impl World {
    /// `StopRuntime` for an eviction or a cleanup of an open database.
    pub(super) fn begin_stop(
        &mut self,
        node: usize,
        op: OpId,
        cell: &str,
        epoch: Epoch,
        cause: StopCause,
        bounded: bool,
    ) {
        let deadline_mono = bounded.then(|| self.mono(node) + OPERATION_DEADLINE_MS);
        let evict = matches!(cause, StopCause::Evict { .. });
        self.replica_mut(node, cell, epoch).stop = Some(Stop {
            op,
            evict,
            snapshot: cause != (StopCause::Evict { rebalance: true }),
            abandonable: evict && bounded,
            deadline_mono,
            abandon: false,
            phase: Phase::Retrying,
            attempt_until: 0,
            step: None,
        });
        self.attempt_stop(node, cell, epoch);
    }

    /// `AbandonEvictionStop`: it only sets the bit. The stop reads it where
    /// it can still act on it.
    pub(super) fn abandon_stop(&mut self, node: usize, op: OpId) {
        let stop = (self.machines[node].proc_mut().replicas.values_mut())
            .find_map(|replica| replica.stop.as_mut().filter(|stop| stop.op == op));
        if let Some(stop) = stop.filter(|stop| stop.abandonable) {
            stop.abandon = true;
        }
    }

    pub(super) fn stop_work(&mut self, node: usize, work: StopWork) {
        match work {
            StopWork::Captured { cell, epoch } => {
                match self.stop_mut(node, &cell, epoch).map(|stop| stop.abandon) {
                    Some(true) => self.abandoned(node, &cell, epoch),
                    Some(false) => self.publish_stop(node, &cell, epoch),
                    None => {}
                }
            }
            StopWork::TimedOut { cell, epoch } => {
                if self.stop_mut(node, &cell, epoch).is_some() {
                    self.stop_attempt_failed(node, &cell, epoch);
                }
            }
            StopWork::Retry { cell, epoch } => {
                if self.stop_mut(node, &cell, epoch).is_some() {
                    self.attempt_stop(node, &cell, epoch);
                }
            }
        }
    }

    fn stop_mut(&mut self, node: usize, cell: &str, epoch: Epoch) -> Option<&mut Stop> {
        let replicas = &mut self.machines[node].proc_mut().replicas;
        replicas.get_mut(&(cell.to_string(), epoch))?.stop.as_mut()
    }

    /// Wait for `work`, in place of whatever step the stop waited for.
    fn stop_step(&mut self, node: usize, cell: &str, epoch: Epoch, delay: u64, work: StopWork) {
        let key = self.work(node, delay, Work::Stop(work));
        let stop = self.stop_mut(node, cell, epoch).expect("a stop");
        if let Some(old) = stop.step.replace(key) {
            self.cancel(old);
        }
    }

    /// One attempt of `report_stopped_when_released`'s loop.
    fn attempt_stop(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let attempt_until = self.t + self.agenda_ms(STOP_ATTEMPT_MS);
        let stop = self.stop_mut(node, cell, epoch).expect("a stop");
        if stop.abandon {
            return self.abandoned(node, cell, epoch);
        }
        stop.phase = Phase::Capturing;
        stop.attempt_until = attempt_until;
        if !stop.evict {
            return self.publish_stop(node, cell, epoch);
        }
        let capture_ms = self.stop_rng.range(1, MAX_CAPTURE_MS);
        let (cell, epoch) = (cell.to_string(), epoch);
        let work = StopWork::Captured {
            cell: cell.clone(),
            epoch,
        };
        self.stop_step(node, &cell, epoch, capture_ms, work);
    }

    /// Publish: upload what the bucket lacks, and give the attempt its
    /// time.
    fn publish_stop(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let stop = self.stop_mut(node, cell, epoch).expect("a stop");
        stop.phase = Phase::Publishing;
        let delay = stop.attempt_until.saturating_sub(self.t);
        let work = StopWork::TimedOut {
            cell: cell.to_string(),
            epoch,
        };
        self.stop_step(node, cell, epoch, delay, work);
        self.settle_proofs(node, cell, epoch);
        self.upload(node, cell, epoch);
    }

    /// An upload failed while the stop publishes: the attempt fails at
    /// once. Returns whether a stop took the failure.
    pub(super) fn stop_upload_failed(&mut self, node: usize, cell: &str, epoch: Epoch) -> bool {
        let publishing = self
            .stop_mut(node, cell, epoch)
            .is_some_and(|stop| stop.publishing());
        if publishing {
            self.stop_attempt_failed(node, cell, epoch);
        }
        publishing
    }

    /// The attempt did not let the cell go. A bounded stop past its
    /// deadline gives up: the core restarts the cell on the database it
    /// kept open. Any other stop tries again after a pause.
    fn stop_attempt_failed(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let mono = self.mono(node);
        let stop = self.stop_mut(node, cell, epoch).expect("a stop");
        let op = stop.op;
        if stop.deadline_mono.is_some_and(|deadline| mono >= deadline) {
            let stop = self.replica_mut(node, cell, epoch).stop.take();
            if let Some(step) = stop.and_then(|stop| stop.step) {
                self.cancel(step);
            }
            return self.deliver(node, 1, Event::RuntimeStopFailed { op });
        }
        stop.phase = Phase::Retrying;
        self.stats.stop_retries += 1;
        let work = StopWork::Retry {
            cell: cell.to_string(),
            epoch,
        };
        self.stop_step(node, cell, epoch, RELEASE_RETRY_MS, work);
    }

    /// `EvictionAbandoned`: the cell stays open at its epoch, and the core
    /// starts it again in place.
    fn abandoned(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let stop = self
            .replica_mut(node, cell, epoch)
            .stop
            .take()
            .expect("a stop");
        if let Some(step) = stop.step {
            self.cancel(step);
        }
        self.stats.stops_abandoned += 1;
        self.deliver(node, 1, Event::RuntimeStopCancelled { op: stop.op });
    }
}
