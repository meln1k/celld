//! Alarms. A cell's alarm lives in its database: a write can install one,
//! due at a wall-clock instant, or clear it. The database also holds the
//! alarm's installation id, `(epoch, sequence)`, as `wake_record.rs` keeps
//! it: every alarm write bumps the sequence, and opening the database at a
//! new epoch moves the id into that epoch, keeping the sequence. So whatever
//! opens a database, by restore, clean reload or swap, finds the alarm
//! where its writes left it, under an id no earlier epoch could have named.
//!
//! The rest follows the shell in `actor.rs` and `wake_entry.rs`:
//!
//! - Installing an alarm also writes its wake entry to the bucket (the arm
//!   gate). Until that lands, the alarm is not *covered*: a node that gave
//!   the cell up now could leave nobody to wake it.
//! - Every activity reports the cell's alarm before it finishes
//!   (`AlarmObserved`, then `ActivityFinished`), as the activity guard does.
//! - The core arms a timer, and when it fires on a resident cell asks the
//!   shell to run the handler (`FireAlarm`). The handler consumes the alarm,
//!   or re-arms it, with one write, and reports where (`AlarmFinished`).
//! - `ReconcileWakeEntry` brings the bucket's wake entries into line with
//!   the alarm the core settled on.
//! - Each process scans the bucket's due wake entries every second and
//!   hints the core (`WakeHintAt`). The elected waker, here the lowest live
//!   node, also hints cells whose owner is dead or absent.

use std::collections::VecDeque;

use celld_logic::wake::{elected_hint_needed, AlarmSnapshot, AlarmSource, PublicationId};
use celld_logic::{Epoch, Event, Failure, OpId, RequestId, WakeHintScope};

use super::shell::StoreReq;
use super::{Work, World, WALL_EPOCH_MS};
use crate::{Mutation, WriteId};

/// How far ahead a handler sets an alarm.
const ALARM_AHEAD_MS: (u64, u64) = (50, 3_000);
/// How long an alarm handler runs.
const ALARM_RUN_MS: (u64, u64) = (1, 30);
/// The wake scan's period (`run_due_scans`).
const WAKE_TICK_MS: u64 = 1_000;
/// An alarm that came due this long before the probe's deadline must have
/// fired by then.
pub(super) const ALARM_SLACK_MS: u64 = 10_000;

impl World {
    /// The alarm a database opened at `epoch` holds: what its last alarm
    /// write left, identified by the open epoch and the number of alarm
    /// writes, sampled at the database's position. `None` when no write ever
    /// touched the alarm.
    pub(super) fn alarm_of(&self, content: &[WriteId], epoch: Epoch) -> Option<AlarmSnapshot> {
        let mut writes = content.iter().filter_map(|w| self.alarm_writes.get(w));
        let at_ms = *writes.next_back()?;
        let source = AlarmSource {
            id: PublicationId {
                epoch,
                sequence: writes.count() as u64 + 1,
            },
            position: content.len() as u64,
        };
        Some(AlarmSnapshot::committed(at_ms, source))
    }

    /// Record that `write`, committed at `epoch`, sets the cell's alarm
    /// `ahead_ms` from now (or clears it), and start its arm gate.
    pub(super) fn alarm_written(
        &mut self,
        node: usize,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
        ahead_ms: Option<u64>,
    ) -> AlarmSnapshot {
        let at_ms = ahead_ms.map(|ahead| (self.wall(node) + ahead) as i64);
        self.alarm_writes.insert(write, at_ms);
        if at_ms.is_some() {
            self.stats.alarms_set += 1;
        }
        let content = self.seen(node, cell, epoch).expect("an open database");
        let alarm = self.alarm_of(content, epoch).expect("just written");
        if at_ms.is_some() {
            self.maintain_wake(node, cell, alarm, false);
        }
        alarm
    }

    /// What the shell tells the core about an alarm: what it is, unless a
    /// planted bug hides it.
    pub(super) fn reported(&self, alarm: AlarmSnapshot) -> AlarmSnapshot {
        match self.config.mutation {
            Some(Mutation::AlarmsUnreported) => AlarmSnapshot::without_wake(None),
            _ => alarm,
        }
    }

    /// A handler's write may set or clear the alarm: how far ahead, if so.
    pub(super) fn draw_alarm(&mut self) -> Option<Option<u64>> {
        if !self.work_rng.chance(self.config.alarm_probability) {
            return None;
        }
        let (lo, hi) = ALARM_AHEAD_MS;
        Some((!self.work_rng.chance(0.2)).then(|| self.work_rng.range(lo, hi)))
    }

    /// The host's view of the cell's alarm: from its open database, and
    /// covered once its wake entry is known to have landed.
    fn observation(&self, node: usize, cell: &str) -> Option<(AlarmSnapshot, bool)> {
        let p = self.machines[node].proc();
        let epoch = p.published.get(cell)?;
        let alarm = self
            .alarm_of(self.seen(node, cell, *epoch)?, *epoch)
            .map(|alarm| self.reported(alarm))
            .unwrap_or(AlarmSnapshot::without_wake(None));
        let covered = match (alarm.at_ms(), alarm.source()) {
            (Some(_), Some(source)) => p.covered.contains(&(cell.to_string(), source.id)),
            _ => true,
        };
        Some((alarm, covered))
    }

    /// An activity on the cell is over: report its alarm, then finish it.
    pub(super) fn activity_done(&mut self, node: usize, request: RequestId, cell: &str) {
        if let Some((alarm, covered)) = self.observation(node, cell) {
            let event = Event::AlarmObserved {
                cell: cell.to_string(),
                alarm,
                covered,
                now_ms: 0,
                now_mono_ms: 0,
            };
            self.drive(node, event);
        }
        self.drive(node, Event::ActivityFinished { request });
    }

    /// Write the wake entry for an alarm, or retire old ones. When `report`
    /// is set, tell the core the coverage once it lands, as a handoff's
    /// refresh does.
    pub(super) fn maintain_wake(
        &mut self,
        node: usize,
        cell: &str,
        alarm: AlarmSnapshot,
        report: bool,
    ) {
        let req = StoreReq::Wake {
            cell: cell.to_string(),
            alarm,
            report,
        };
        self.store_call(node, req);
    }

    /// A wake entry write landed and its answer came back.
    pub(super) fn wake_maintained(
        &mut self,
        node: usize,
        cell: String,
        alarm: AlarmSnapshot,
        report: bool,
    ) {
        if let (Some(_), Some(source)) = (alarm.at_ms(), alarm.source()) {
            let p = self.machines[node].proc_mut();
            p.covered.insert((cell.clone(), source.id));
        }
        if report {
            if let Some((alarm, covered)) = self.observation(node, &cell) {
                let event = Event::AlarmObserved {
                    cell,
                    alarm,
                    covered,
                    now_ms: 0,
                    now_mono_ms: 0,
                };
                self.drive(node, event);
            }
        }
    }

    /// A handoff is about to give the cell up. Its alarm must be covered
    /// first: report it now if it is, or write its entry and report then.
    pub(super) fn refresh_coverage(
        &mut self,
        node: usize,
        cell: &str,
        immediate: &mut VecDeque<Event>,
    ) {
        let Some((alarm, covered)) = self.observation(node, cell) else {
            return;
        };
        if covered || alarm.at_ms().is_none() {
            immediate.push_back(Event::AlarmObserved {
                cell: cell.to_string(),
                alarm,
                covered,
                now_ms: 0,
                now_mono_ms: 0,
            });
        } else {
            self.maintain_wake(node, cell, alarm, true);
        }
    }

    /// The core fires the cell's alarm. It must run on this node's runtime
    /// of the epoch it names.
    pub(super) fn fire_alarm(&mut self, node: usize, op: OpId, cell: String, epoch: Epoch) {
        let published = self.machines[node].proc().published.get(&cell) == Some(&epoch);
        let name = self.machines[node].name.clone();
        self.checker.routed_local(self.t, &name, &cell, published);
        self.machines[node]
            .proc_mut()
            .firing
            .insert(op, (cell, epoch));
        let (lo, hi) = ALARM_RUN_MS;
        let run = self.work_rng.range(lo, hi);
        self.work(node, run, Work::AlarmRun { op });
    }

    /// The alarm handler ran. If the alarm was due, it consumed it, or
    /// re-armed it, with one write.
    pub(super) fn alarm_run(&mut self, node: usize, op: OpId) {
        let Some((cell, epoch)) = self.machines[node].proc_mut().firing.remove(&op) else {
            return;
        };
        let now = self.wall(node) as i64;
        let result = match self.seen(node, &cell, epoch) {
            None => Err(Failure::Definite),
            Some(content) => {
                let current = self.alarm_of(content, epoch);
                let due = current
                    .and_then(AlarmSnapshot::at_ms)
                    .is_some_and(|at| at <= now);
                if due {
                    let rearm = self
                        .work_rng
                        .chance(0.3)
                        .then(|| self.work_rng.range(ALARM_AHEAD_MS.0, ALARM_AHEAD_MS.1));
                    let (write, position) = self
                        .commit_write(node, &cell, epoch)
                        .expect("open database");
                    let alarm = self.alarm_written(node, &cell, epoch, write, rearm);
                    let alarm = self.reported(alarm);
                    self.stats.alarms_fired += 1;
                    Ok((alarm, rearm.is_none(), Some(position)))
                } else {
                    let (alarm, covered) = self
                        .observation(node, &cell)
                        .unwrap_or((AlarmSnapshot::without_wake(None), true));
                    Ok((alarm, covered, None))
                }
            }
        };
        let event = Event::AlarmFinished {
            op,
            cell,
            epoch,
            now_ms: 0,
            now_mono_ms: 0,
            result,
        };
        self.drive(node, event);
    }

    /// A handoff aborted the alarm handler before it ran to the end.
    pub(super) fn abort_alarm(&mut self, node: usize, op: OpId) {
        if let Some((cell, epoch)) = self.machines[node].proc_mut().firing.remove(&op) {
            let event = Event::AlarmFinished {
                op,
                cell,
                epoch,
                now_ms: 0,
                now_mono_ms: 0,
                result: Err(Failure::Definite),
            };
            self.deliver(node, 1, event);
        }
    }

    /// Start the process's periodic wake scan.
    pub(super) fn start_wake_scans(&mut self, node: usize) {
        let delay = self.net_rng.range(1, WAKE_TICK_MS);
        self.work(node, delay, Work::WakeScan);
    }

    /// List the due wake entries and hint the core about each.
    pub(super) fn wake_scan(&mut self, node: usize) {
        self.work(node, WAKE_TICK_MS, Work::WakeScan);
        let p = self.machines[node].proc();
        if self.machines[node].partition.is_some()
            || !p.state.node_authoritative()
            || self.preserving(node)
        {
            return;
        }
        let now = self.wall(node);
        let me = self.machines[node].name.clone();
        let live = |w: &World, name: &str| {
            w.store
                .read_lease(name)
                .is_some_and(|lease| lease.expires_ms > now)
        };
        // The waker lease, simplified: the lowest live node holds it.
        let elected = self
            .machines
            .iter()
            .find(|m| live(self, &m.name))
            .is_some_and(|m| m.name == me);
        let mut due = self.store.wake_due(now as i64);
        due.dedup_by(|a, b| a.0 == b.0);
        for (cell, entry_ms) in due {
            let scope = if elected {
                let owner = self.store.owner(&cell).and_then(|(owner, _)| owner);
                let owner_live = owner.is_some_and(|owner| live(self, owner));
                if !elected_hint_needed(owner, &me, owner_live) {
                    continue;
                }
                WakeHintScope::Fleet
            } else {
                WakeHintScope::Owned
            };
            self.stats.wake_hints += 1;
            let event = Event::WakeHintAt {
                cell,
                entry_ms,
                scope,
                now_mono_ms: 0,
            };
            self.drive(node, event);
        }
    }

    /// **Every durable alarm fires.** When the probe ends, no cell's newest
    /// durable database may still hold an alarm that came due long before.
    pub(super) fn judge_alarms(&mut self) {
        let now = (WALL_EPOCH_MS + self.t) as i64;
        for cell in 0..self.config.cells {
            let cell = format!("c{cell}");
            let Some((epoch, content)) = self.store.newest(&cell) else {
                continue;
            };
            let at_ms = self.alarm_of(content, epoch).and_then(AlarmSnapshot::at_ms);
            if let Some(at_ms) = at_ms.filter(|at| *at + (ALARM_SLACK_MS as i64) < now) {
                self.checker.alarm_unfired(self.t, &cell, now - at_ms);
            }
        }
    }
}
