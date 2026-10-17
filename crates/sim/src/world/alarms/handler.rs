//! The alarm handler (`RuntimeManager::fire_alarm` and
//! `finish_alarm_handler`), the gate on its commit, and the arm gate
//! (`launch_arm_gate`).

use super::*;
use celld_logic::wake::entry_key;
use checks::FireId;

/// One alarm handler in flight on a process.
#[derive(Debug)]
pub(super) struct AlarmRun {
    cell: String,
    /// The replica the handler runs against, and the epoch the core fired at.
    replica_epoch: Epoch,
    epoch: Epoch,
    armed_by: WriteId,
    aborted: bool,
    fire: FireId,
    /// Set once the handler committed: the snapshot it answers with and the
    /// position of its commit.
    answer: Option<(AlarmSnapshot, u64)>,
}

/// The handler commit behind one core alarm gate.
#[derive(Clone, Debug)]
pub(super) struct GateWrite {
    /// The replica the commit landed in.
    replica_epoch: Epoch,
    write: WriteId,
    fire: FireId,
}

/// What names the gate the core opens for a handler's commit: the cell and
/// epoch the firing was dispatched against, and the commit's position.
pub(super) type GateKey = (String, Epoch, u64);

impl World {
    // -----------------------------------------------------------------------
    // The handler

    pub(in crate::world) fn fire_alarm(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: String,
        epoch: Epoch,
        scheduled_ms: i64,
    ) {
        let now_ms = self.wall(id) as i64;
        let replica = self.procs[&id]
            .replicas
            .iter()
            .find(|((name, _), replica)| *name == cell && replica.running)
            .map(|((_, epoch), replica)| (*epoch, self.alarm_row(&replica.content)));
        let delay = self.net_rng.range(1, 5);
        let Some((replica_epoch, row)) = replica.filter(|_| now_ms >= scheduled_ms) else {
            // No isolate holds the cell, or the dispatch came early: the
            // shell reports a failed dispatch.
            self.alarm_answer(id, op, cell, epoch, Err(Failure::Definite), delay);
            return;
        };
        let (Some(at_ms), Some(armed_by)) = (row.at_ms, row.armed_by) else {
            let alarm = self.runtime_alarm(id, &cell);
            let covered = self.alarm_covered(id, &cell, alarm);
            self.alarm_answer(id, op, cell, epoch, Ok((alarm, covered, None)), delay);
            return;
        };
        if at_ms > now_ms {
            // Nothing is due: another dispatch ran it, or a write moved it.
            let alarm = self.runtime_alarm(id, &cell);
            let covered = self.alarm_covered(id, &cell, alarm);
            self.alarm_answer(id, op, cell, epoch, Ok((alarm, covered, None)), delay);
            return;
        }
        let fire = self.record_fire(id, &cell, replica_epoch, armed_by);
        self.procs.get_mut(&id).unwrap().wake.runs.insert(
            op,
            AlarmRun {
                cell,
                replica_epoch,
                epoch,
                armed_by,
                aborted: false,
                fire,
                answer: None,
            },
        );
        let duration = if self.work_rng.chance(0.05) {
            self.work_rng.range(500, 5_000)
        } else {
            self.work_rng.range(1, 60)
        };
        self.work(id, duration, Work::AlarmEnd { op });
    }

    fn record_fire(&mut self, id: ProcId, cell: &str, epoch: Epoch, armed_by: WriteId) -> FireId {
        self.stats.alarms_fired += 1;
        self.note(|| format!("p{id} fires {cell} alarm of write {armed_by} at epoch {epoch}"));
        let node = self.node_name(id);
        let view = View {
            t: self.t,
            store: &self.store,
            ledger: &self.alarm_ops,
        };
        self.checker
            .on_fire(view, id, &node, cell, epoch, armed_by, &mut self.stats)
    }

    /// `CancelCellActivity` names the firing: the handler ends now, as a
    /// failure.
    pub(in crate::world) fn abort_alarm(&mut self, id: ProcId, op: OpId) {
        let proc = self.procs.get_mut(&id).unwrap();
        if let Some(run) = proc.wake.runs.get_mut(&op) {
            if run.answer.is_none() && !run.aborted {
                run.aborted = true;
                self.work(id, 0, Work::AlarmEnd { op });
            }
        }
    }

    pub(in crate::world) fn alarm_end(&mut self, id: ProcId, op: OpId) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(run) = proc.wake.runs.get(&op) else {
            return;
        };
        if run.answer.is_some() {
            return;
        }
        let (cell, replica_epoch, epoch, armed_by, aborted) = (
            run.cell.clone(),
            run.replica_epoch,
            run.epoch,
            run.armed_by,
            run.aborted,
        );
        let now_ms = self.wall(id) as i64;
        let succeeded = !aborted && (!self.chaos || self.work_rng.chance(0.85));
        let rearm = succeeded && self.work_rng.chance(0.25);
        let alarm_op = if rearm {
            AlarmOp::Set(now_ms + self.work_rng.range(300, 6_000) as i64)
        } else if succeeded {
            AlarmOp::Consume { armed_by }
        } else {
            AlarmOp::Retry { armed_by, now_ms }
        };
        let Some((write, position, alarm)) =
            self.commit_alarm_write(id, &cell, replica_epoch, Some(alarm_op))
        else {
            // The runtime went away under the handler: its reply is dropped.
            self.procs.get_mut(&id).unwrap().wake.runs.remove(&op);
            self.alarm_answer(id, op, cell, epoch, Err(Failure::Definite), 0);
            return;
        };
        let proc = self.procs.get_mut(&id).unwrap();
        let run = proc.wake.runs.get_mut(&op).unwrap();
        run.answer = Some((alarm, position));
        let fire = run.fire;
        proc.wake.writes.insert(
            op,
            (
                (cell.clone(), epoch, position),
                GateWrite {
                    replica_epoch,
                    write,
                    fire,
                },
            ),
        );
        if rearm && self.launch_arm_gate(id, &cell, alarm, ArmWaiter::Alarm(op)) {
            // The answer is held behind the re-arm's wake-entry PUT.
            return;
        }
        self.send_alarm_answer(id, op);
    }

    fn send_alarm_answer(&mut self, id: ProcId, op: OpId) {
        let Some(run) = self.procs.get_mut(&id).unwrap().wake.runs.remove(&op) else {
            return;
        };
        let (alarm, position) = run.answer.expect("the handler committed");
        let covered = self.alarm_covered(id, &run.cell, alarm);
        self.alarm_answer(
            id,
            op,
            run.cell,
            run.epoch,
            Ok((alarm, covered, Some(position))),
            0,
        );
    }

    fn alarm_answer(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: String,
        epoch: Epoch,
        result: Result<(AlarmSnapshot, bool, Option<u64>), Failure>,
        delay: u64,
    ) {
        self.work(
            id,
            delay,
            Work::AlarmAnswer(Event::AlarmFinished {
                op,
                cell,
                epoch,
                now_ms: 0,
                now_mono_ms: 0,
                result,
            }),
        );
    }

    /// Hand the core a handler's answer. A commit it carries is what the
    /// gate the core opens in this step proves.
    pub(in crate::world) fn deliver_alarm_answer(&mut self, id: ProcId, event: Event) {
        let Event::AlarmFinished { op, .. } = event else {
            unreachable!("an alarm answer is an AlarmFinished");
        };
        let wake = &mut self.procs.get_mut(&id).unwrap().wake;
        wake.answering = wake.writes.remove(&op);
        self.drive(id, event);
        if let Some(proc) = self.procs.get_mut(&id) {
            proc.wake.answering = None;
        }
    }

    /// `AwaitDurable`: the gate for the commit being answered, if this is it.
    pub(in crate::world) fn alarm_gate_opened(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: &str,
        epoch: Epoch,
        position: u64,
    ) {
        let wake = &mut self.procs.get_mut(&id).unwrap().wake;
        if wake
            .answering
            .as_ref()
            .is_some_and(|((c, e, p), _)| c == cell && *e == epoch && *p == position)
        {
            let ((cell, _, _), gate) = wake.answering.take().unwrap();
            wake.gates.insert(op, (cell, gate));
        }
    }

    /// The gate's ownership check resolved. A verified one settles the
    /// handler commit: it is in its epoch's prefix and the owner record
    /// named that epoch afterwards, which is what acknowledges any write.
    pub(in crate::world) fn alarm_gate_verified(&mut self, id: ProcId, op: OpId, verified: bool) {
        let Some((cell, gate)) = self.procs.get_mut(&id).unwrap().wake.gates.remove(&op) else {
            return;
        };
        if !verified {
            return;
        }
        self.stats.alarms_settled += 1;
        let (checker, view) = self.checks();
        checker.on_handler_commit_proven(view, gate.fire, &cell, gate.replica_epoch, gate.write);
    }
    // -----------------------------------------------------------------------
    // The arm gate

    /// `launch_arm_gate`. Returns whether the waiter must wait: an
    /// installation this process already published needs no PUT, and the
    /// `AckBeforeWakeEntry` mutation issues the PUT without waiting for it.
    pub(in crate::world) fn launch_arm_gate(
        &mut self,
        id: ProcId,
        cell: &str,
        alarm: AlarmSnapshot,
        waiter: ArmWaiter,
    ) -> bool {
        let Some(at_ms) = alarm.at_ms() else {
            return false;
        };
        let Some(source) = alarm.source() else {
            return false;
        };
        let Some(publication) = self.procs[&id].wake.core.publication(cell, alarm) else {
            return false;
        };
        debug_assert_eq!(publication.key, entry_key(at_ms, cell, source.id));
        let task = self.new_task(
            id,
            Task::Arm {
                cell: cell.to_string(),
                alarm,
                waiter,
            },
        );
        self.store_call(
            id,
            StoreReq::WakePut {
                key: publication.key,
            },
            Reply::Task { task },
        );
        self.config.mutation != Mutation::AckBeforeWakeEntry
    }

    pub(super) fn arm_done(
        &mut self,
        id: ProcId,
        cell: String,
        alarm: AlarmSnapshot,
        waiter: ArmWaiter,
        ok: bool,
    ) {
        if ok {
            self.procs
                .get_mut(&id)
                .unwrap()
                .wake
                .core
                .confirm(&cell, alarm);
        }
        match waiter {
            ArmWaiter::Request(request) => self.arm_gate_settled(id, request, ok),
            // Under `AckBeforeWakeEntry` the answer already left.
            ArmWaiter::Alarm(op) => self.send_alarm_answer(id, op),
        }
    }
}
