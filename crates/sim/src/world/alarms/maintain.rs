//! `WakeFlusher::maintain`, behind `ReconcileWakeEntry` and `MaintainWake`:
//! publish, prove the source durable, read the owner record, advance the
//! retirement record, then delete what it retires.

use super::*;
use celld_logic::wake::Retirement;

#[derive(Debug)]
pub(super) struct Maintain {
    cell: String,
    alarm: AlarmSnapshot,
    source: AlarmSource,
    stage: Stage,
    attempts: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Publish,
    Prove,
    Owner,
    ReadRetirement,
    CasRetirement { previous: Option<Retirement> },
}

/// Where a maintenance pass stands after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
enum Step {
    /// Parked under its task key, waiting for a result.
    Parked,
    /// Done, by success or failure.
    Ended,
}

impl Maintain {
    fn proof(&self) -> Retirement {
        Retirement {
            id: self.source.id,
            at_ms: self.alarm.at_ms(),
        }
    }
}

impl World {
    /// `ReconcileWakeEntry`, or a collector's `MaintainWake` with the
    /// runtime's current snapshot.
    pub(in crate::world) fn maintain_wake(
        &mut self,
        id: ProcId,
        cell: String,
        alarm: AlarmSnapshot,
    ) {
        let Some(source) = alarm.source() else {
            return;
        };
        let settled = self.procs[&id]
            .wake
            .settled
            .get(&cell)
            .is_some_and(|settled| *settled >= source.id);
        if settled || !self.runtime_alarm(id, &cell).same_installation(alarm) {
            self.report_alarm_coverage(id, &cell, alarm);
            return;
        }
        let maintain = Maintain {
            cell: cell.clone(),
            alarm,
            source,
            stage: Stage::Publish,
            attempts: 0,
        };
        let key = self.task_key();
        self.work(id, OPERATION_DEADLINE_MS, Work::TaskDeadline { task: key });
        if self.maintain_publish(id, key, maintain) == Step::Ended {
            self.report_alarm_coverage(id, &cell, alarm);
        }
    }

    /// A store result for a parked pass.
    pub(super) fn maintain_reply(
        &mut self,
        id: ProcId,
        key: u64,
        maintain: Maintain,
        result: Result<StoreRes, Failure>,
    ) {
        let (cell, alarm) = (maintain.cell.clone(), maintain.alarm);
        if self.maintain_step(id, key, maintain, result) == Step::Ended {
            self.report_alarm_coverage(id, &cell, alarm);
        }
    }

    /// The durability proof for a parked pass resolved.
    pub(super) fn maintain_proven(&mut self, id: ProcId, key: u64, maintain: Maintain, ok: bool) {
        if maintain.stage != Stage::Prove {
            self.park(id, key, Task::Maintain(maintain));
            return;
        }
        let (cell, alarm) = (maintain.cell.clone(), maintain.alarm);
        if !ok || self.maintain_owner(id, key, maintain) == Step::Ended {
            self.report_alarm_coverage(id, &cell, alarm);
        }
    }

    /// The pass ran out of time and was dropped.
    pub(super) fn maintain_dropped(&mut self, id: ProcId, maintain: Maintain) {
        self.report_alarm_coverage(id, &maintain.cell, maintain.alarm);
    }

    fn maintain_publish(&mut self, id: ProcId, key: u64, maintain: Maintain) -> Step {
        let publication = self.procs[&id]
            .wake
            .core
            .publication(&maintain.cell, maintain.alarm);
        let Some(publication) = publication else {
            return self.maintain_prove(id, key, maintain);
        };
        self.park(id, key, Task::Maintain(maintain));
        self.store_call(
            id,
            StoreReq::WakePut {
                key: publication.key,
            },
            Reply::Task { task: key },
        );
        Step::Parked
    }

    fn maintain_prove(&mut self, id: ProcId, key: u64, mut maintain: Maintain) -> Step {
        if self.config.mutation == Mutation::RetireUnproven {
            return self.maintain_retire(id, key, maintain);
        }
        maintain.stage = Stage::Prove;
        let (cell, epoch, position) = (
            maintain.cell.clone(),
            maintain.source.id.epoch,
            maintain.source.position,
        );
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(replica) = proc.replicas.get_mut(&(cell.clone(), epoch)) else {
            // No replicator for that epoch here: the proof fails.
            return Step::Ended;
        };
        replica.waiters.push(Waiter::Task {
            task: key,
            position,
        });
        self.park(id, key, Task::Maintain(maintain));
        self.settle_waiters(id, &cell, epoch);
        self.kick_upload(id, &cell, epoch);
        Step::Parked
    }

    fn maintain_owner(&mut self, id: ProcId, key: u64, mut maintain: Maintain) -> Step {
        maintain.stage = Stage::Owner;
        let cell = maintain.cell.clone();
        self.park(id, key, Task::Maintain(maintain));
        self.store_call(id, StoreReq::ReadOwner { cell }, Reply::Task { task: key });
        Step::Parked
    }

    fn maintain_retire(&mut self, id: ProcId, key: u64, mut maintain: Maintain) -> Step {
        if maintain.attempts >= 3 {
            // "wake retirement CAS remained contended"
            return Step::Ended;
        }
        maintain.attempts += 1;
        maintain.stage = Stage::ReadRetirement;
        let cell = maintain.cell.clone();
        self.park(id, key, Task::Maintain(maintain));
        self.store_call(
            id,
            StoreReq::ReadRetirement { cell },
            Reply::Task { task: key },
        );
        Step::Parked
    }

    fn maintain_step(
        &mut self,
        id: ProcId,
        key: u64,
        maintain: Maintain,
        result: Result<StoreRes, Failure>,
    ) -> Step {
        // Every failure ends the pass; a later scan retries.
        let Ok(result) = result else {
            return Step::Ended;
        };
        match (maintain.stage, result) {
            (Stage::Publish, StoreRes::Done) => {
                self.procs
                    .get_mut(&id)
                    .unwrap()
                    .wake
                    .core
                    .confirm(&maintain.cell, maintain.alarm);
                self.maintain_prove(id, key, maintain)
            }
            (Stage::Owner, StoreRes::Owner(record)) => {
                let node = self.node_name(id);
                let owns = record.is_some_and(|record| {
                    record.epoch == maintain.source.id.epoch
                        && record.node.as_deref() == Some(node.as_str())
                });
                if owns {
                    self.maintain_retire(id, key, maintain)
                } else {
                    Step::Ended
                }
            }
            (Stage::ReadRetirement, StoreRes::Retirement(observed)) => {
                let previous = observed.map(|(record, _)| record);
                let proof = maintain.proof();
                if previous.is_some_and(|old| old.id >= proof.id) {
                    // Equal: already published. Newer: never move back.
                    self.maintain_finish(id, maintain, previous);
                    return Step::Ended;
                }
                let cell = maintain.cell.clone();
                let guard = observed.map(|(_, etag)| etag);
                let maintain = Maintain {
                    stage: Stage::CasRetirement { previous },
                    ..maintain
                };
                self.park(id, key, Task::Maintain(maintain));
                self.store_call(
                    id,
                    StoreReq::CasRetirement {
                        cell,
                        guard,
                        retirement: proof,
                    },
                    Reply::Task { task: key },
                );
                Step::Parked
            }
            (Stage::CasRetirement { previous }, StoreRes::Bool(applied)) => {
                if applied {
                    self.maintain_finish(id, maintain, previous);
                    Step::Ended
                } else {
                    self.maintain_retire(id, key, maintain)
                }
            }
            (stage, result) => unreachable!("maintenance {stage:?} got {result:?}"),
        }
    }

    /// The certificate stands: delete the names it retires, eagerly. A lost
    /// delete is the collector's to repeat.
    fn maintain_finish(&mut self, id: ProcId, maintain: Maintain, previous: Option<Retirement>) {
        let proof = maintain.proof();
        let cell = maintain.cell;
        let proc = self.procs.get_mut(&id).unwrap();
        let obsolete = proc.wake.core.retire(&cell, proof, previous);
        let settled = proc.wake.settled.entry(cell).or_insert(proof.id);
        *settled = (*settled).max(proof.id);
        for key in obsolete {
            self.store_call(id, StoreReq::WakeDelete { key }, Reply::Ignore);
        }
    }

    /// `report_alarm_coverage`, after maintenance ends either way: once the
    /// confirmed entry covers the runtime's current installation, the core
    /// hears so. Nothing else reports a restored alarm covered.
    fn report_alarm_coverage(&mut self, id: ProcId, cell: &str, maintained: AlarmSnapshot) {
        if self.config.mutation == Mutation::SilentWakeCoverage {
            return;
        }
        let alarm = self.runtime_alarm(id, cell);
        if alarm != maintained || alarm.at_ms().is_none() || !self.alarm_covered(id, cell, alarm) {
            return;
        }
        self.work(
            id,
            0,
            Work::Deliver(Event::AlarmObserved {
                cell: cell.to_string(),
                alarm,
                covered: true,
                now_ms: 0,
                now_mono_ms: 0,
            }),
        );
    }
}
