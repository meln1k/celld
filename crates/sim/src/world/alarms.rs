//! Alarms and their discovery.
//!
//! A cell's alarm lives in its own SQLite: a write can install, change or
//! delete the row, and `_cf_WAKE` gives every change a publication identity
//! of the opening writer epoch and a sequence that counts the changes
//! (`crates/celld/storage/wake_record.rs`). Here a write carries an
//! [`AlarmOp`], and folding a database's content in order yields the row and
//! its identity, so a restore anywhere sees exactly the alarm its content
//! implies.
//!
//! The rest follows the production shell:
//!
//! - the handler, `RuntimeManager::fire_alarm` and `finish_alarm_handler`:
//!   only a due row runs, success consumes it, failure takes the
//!   `alarm_retry` backoff, and the consuming commit's position goes back to
//!   the core for its output gate;
//! - the arm gate, `launch_arm_gate`: the reply of an event that armed an
//!   alarm waits for its wake-entry PUT;
//! - `WakeFlusher::maintain` behind `ReconcileWakeEntry` and `MaintainWake`:
//!   publish, prove the source durable, read the owner record, advance the
//!   retirement record, then delete what it retires;
//! - the waker, `crates/celld/wake.rs`: a boot scan, the due scan with the
//!   advisory waker role, and the cleanup collector, each hinting the core.

mod handler;
mod maintain;
mod row;
mod waker;

use super::*;
use celld_logic::wake::{AlarmSnapshot, AlarmSource, PublicationId, WakeCore};
use handler::{AlarmRun, GateKey, GateWrite};
use maintain::Maintain;
pub(super) use row::{AlarmLedger, AlarmOp, AlarmRow};
use waker::Scan;
pub(super) use waker::ScanKind;

/// What an event that armed an alarm waits on before it may answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArmGate {
    None,
    Pending,
    Done { ok: bool },
}

/// Per-process wake state: `WakeFlusher` and the waker loops.
#[derive(Default)]
pub(super) struct WakeShell {
    core: WakeCore,
    settled: BTreeMap<String, PublicationId>,
    tasks: BTreeMap<u64, Task>,
    runs: BTreeMap<OpId, AlarmRun>,
    /// Handler commits whose `AlarmFinished` has not reached the core yet.
    writes: BTreeMap<OpId, (GateKey, GateWrite)>,
    /// The commit whose `AlarmFinished` the core is taking now. The core
    /// opens the commit's gate inside that step, or not at all.
    answering: Option<(GateKey, GateWrite)>,
    /// Core gate ops proving a handler commit.
    gates: BTreeMap<OpId, (String, GateWrite)>,
}

#[derive(Debug)]
enum Task {
    Arm {
        cell: String,
        alarm: AlarmSnapshot,
        waiter: ArmWaiter,
    },
    Maintain(Maintain),
    Scan(Scan),
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ArmWaiter {
    Request(RequestId),
    Alarm(OpId),
}

impl World {
    // -----------------------------------------------------------------------
    // The alarm row

    pub(super) fn alarm_row(&self, content: &[WriteId]) -> AlarmRow {
        self.alarm_ops.row(content)
    }

    /// The snapshot a committed connection samples: the row, its identity
    /// at the opening epoch, and the write position.
    fn snapshot_of(&self, epoch: Epoch, content: &[WriteId]) -> AlarmSnapshot {
        let row = self.alarm_row(content);
        AlarmSnapshot::committed(
            row.at_ms,
            AlarmSource {
                id: PublicationId {
                    epoch,
                    sequence: row.sequence,
                },
                position: content.len() as u64,
            },
        )
    }

    /// `with_alarm`: the snapshot of the cell's starting or published
    /// runtime, or no alarm when the process holds none.
    pub(super) fn runtime_alarm(&self, id: ProcId, cell: &str) -> AlarmSnapshot {
        let proc = &self.procs[&id];
        proc.replicas
            .iter()
            .find(|((name, _), replica)| name == cell && replica.running)
            .map(|((_, epoch), replica)| self.snapshot_of(*epoch, &replica.content))
            .unwrap_or_else(|| AlarmSnapshot::without_wake(None))
    }

    /// `alarm_covered`: no alarm needs no entry; an armed one is covered
    /// once this process confirmed its publication.
    fn alarm_covered(&self, id: ProcId, cell: &str, alarm: AlarmSnapshot) -> bool {
        alarm.at_ms().is_none() || self.procs[&id].wake.core.covered(cell, alarm)
    }

    /// What a restore tells the core about the alarm in the content it
    /// loaded. Production reads the row by path and asks the flusher about
    /// a snapshot with no source, so a restored alarm is never covered.
    pub(super) fn restored_alarm(&self, content: &[WriteId]) -> Option<celld_logic::RestoredAlarm> {
        self.alarm_row(content)
            .at_ms
            .filter(|at_ms| *at_ms >= 0)
            .map(|at_ms| celld_logic::RestoredAlarm {
                at_ms,
                covered: false,
            })
    }

    /// The alarm observer: the runtime reports its opening snapshot as it
    /// adopts the database, before the start completes.
    pub(super) fn observe_opened(&mut self, id: ProcId, cell: &str, epoch: Epoch, delay: u64) {
        let Some(replica) = self.procs[&id].replicas.get(&(cell.to_string(), epoch)) else {
            return;
        };
        let alarm = self.snapshot_of(epoch, &replica.content);
        self.work(
            id,
            delay,
            Work::Deliver(Event::AlarmObserved {
                cell: cell.to_string(),
                alarm,
                covered: false,
                now_ms: 0,
                now_mono_ms: 0,
            }),
        );
    }

    /// The `ActivityGuard`'s report: the runtime's alarm and its coverage,
    /// then the end of the activity.
    pub(super) fn finish_activity(&mut self, id: ProcId, request: RequestId, cell: &str) {
        let alarm = self.runtime_alarm(id, cell);
        let covered = self.alarm_covered(id, cell, alarm);
        self.drive(
            id,
            Event::AlarmObserved {
                cell: cell.to_string(),
                alarm,
                covered,
                now_ms: 0,
                now_mono_ms: 0,
            },
        );
        if self.procs.contains_key(&id) {
            self.drive(id, Event::ActivityFinished { request });
        }
    }

    /// Commit `op` into the running replica of `cell` at `epoch`, as the
    /// alarm reporter sees it: a moved deadline reaches the core at once.
    /// Returns the write, its position and the snapshot after it.
    pub(super) fn commit_alarm_write(
        &mut self,
        id: ProcId,
        cell: &str,
        epoch: Epoch,
        op: Option<AlarmOp>,
    ) -> Option<(WriteId, u64, AlarmSnapshot)> {
        let key = (cell.to_string(), epoch);
        let replica = self.procs[&id].replicas.get(&key)?;
        if !replica.running {
            return None;
        }
        let before = self.alarm_row(&replica.content).at_ms;
        let write = self.next_write;
        self.next_write += 1;
        if let Some(op) = op {
            self.alarm_ops.record(write, op);
            match op {
                AlarmOp::Set(_) => self.stats.alarms_armed += 1,
                AlarmOp::Retry { .. } => self.stats.alarm_retries += 1,
                _ => {}
            }
        }
        let replica = self
            .procs
            .get_mut(&id)
            .unwrap()
            .replicas
            .get_mut(&key)
            .unwrap();
        replica.content.push(write);
        let position = replica.content.len() as u64;
        let content = replica.content.clone();
        self.stats.writes += 1;
        let alarm = self.snapshot_of(epoch, &content);
        if alarm.at_ms() != before {
            self.work(
                id,
                0,
                Work::Deliver(Event::AlarmObserved {
                    cell: cell.to_string(),
                    alarm,
                    covered: false,
                    now_ms: 0,
                    now_mono_ms: 0,
                }),
            );
        }
        self.log_commit(id, cell, epoch);
        self.kick_upload(id, cell, epoch);
        Some((write, position, alarm))
    }

    // -----------------------------------------------------------------------
    // Tasks

    fn new_task(&mut self, id: ProcId, task: Task) -> u64 {
        let key = self.task_key();
        self.park(id, key, task);
        key
    }

    fn task_key(&mut self) -> u64 {
        let key = self.next_task;
        self.next_task += 1;
        key
    }

    /// Keep a task until its next result arrives.
    fn park(&mut self, id: ProcId, key: u64, task: Task) {
        self.procs
            .get_mut(&id)
            .unwrap()
            .wake
            .tasks
            .insert(key, task);
    }

    pub(super) fn task_reply(&mut self, id: ProcId, key: u64, result: Result<StoreRes, Failure>) {
        let Some(task) = self.procs.get_mut(&id).unwrap().wake.tasks.remove(&key) else {
            return;
        };
        match task {
            Task::Arm {
                cell,
                alarm,
                waiter,
            } => self.arm_done(id, cell, alarm, waiter, result.is_ok()),
            Task::Maintain(maintain) => self.maintain_reply(id, key, maintain, result),
            Task::Scan(scan) => self.scan_step(id, key, scan, result),
        }
    }

    /// A durability waiter for a maintenance task resolved.
    pub(super) fn task_proven(&mut self, id: ProcId, key: u64, ok: bool) {
        if let Some(Task::Maintain(maintain)) =
            self.procs.get_mut(&id).unwrap().wake.tasks.remove(&key)
        {
            self.maintain_proven(id, key, maintain, ok);
        }
    }

    /// `schedule_wake_maintenance`'s timeout dropped the future.
    pub(super) fn task_deadline(&mut self, id: ProcId, key: u64) {
        if let Some(Task::Maintain(maintain)) =
            self.procs.get_mut(&id).unwrap().wake.tasks.remove(&key)
        {
            self.maintain_dropped(id, maintain);
        }
    }

    // -----------------------------------------------------------------------
    // The alarm audit

    /// After the fleet died, every alarm the bucket can find must have run
    /// on the audit node, which restored it from the bucket alone.
    pub(super) fn audit_alarms(&mut self, audit: ProcId, grace_ms: u64) {
        let Some(proc) = self.procs.get(&audit) else {
            return;
        };
        let now_ms = self.wall(audit) as i64;
        let contents = self
            .cells
            .iter()
            .filter_map(|cell| {
                proc.replicas
                    .iter()
                    .filter(|((name, _), _)| name == cell)
                    .map(|(_, replica)| replica.content.clone())
                    .next_back()
                    .or_else(|| self.store.read_chain(cell, Epoch::MAX))
                    .map(|content| (cell.clone(), content))
            })
            .collect();
        let view = View {
            t: self.t,
            store: &self.store,
            nodes: &self.nodes,
            ledger: &self.alarm_ops,
        };
        self.checker
            .check_alarms_fired(view, audit, now_ms, grace_ms, contents, &mut self.stats);
    }
}

impl WakeShell {
    /// `forget_wake_entry`: this node no longer writes the cell.
    pub(super) fn forget(&mut self, cell: &str) {
        self.core.forget(cell);
        self.settled.remove(cell);
    }
}
