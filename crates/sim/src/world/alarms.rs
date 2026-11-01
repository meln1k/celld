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
//!   the cell up now could leave nobody to wake it. The response of the
//!   handler that set it waits for the write, and fails if the write fails
//!   (`send_answer_after_arm_gates` in `js.rs`), so an alarm a client was
//!   told was set is always covered. An alarm handler that re-arms waits
//!   the same way: the core hears `AlarmFinished` only once the write has
//!   landed or failed, with the coverage that left. A handler's call to a
//!   third party waits on the output gate alone, as in production.
//! - Every activity reports the cell's alarm before it finishes
//!   (`AlarmObserved`, then `ActivityFinished`), as the activity guard does.
//! - The core arms a timer, and when it fires on a resident cell asks the
//!   shell to run the handler (`FireAlarm`). The handler consumes the alarm,
//!   or re-arms it, with one write, and reports where (`AlarmFinished`).
//! - `ReconcileWakeEntry` writes the wake entry of the alarm the core
//!   settled on. Once that alarm is proven durable and owned, it raises the
//!   cell's retirement certificate to it, and every entry below goes. Only
//!   a certificate deletes entries, so an old one wakes the cell until a
//!   newer installation is safely published.
//! - Each process scans the bucket's due wake entries every second and
//!   hints the core (`WakeHintAt`). The elected waker, here the lowest live
//!   node, also hints cells whose owner is dead or absent.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use celld_logic::wake::{
    elected_hint_needed, AlarmSnapshot, AlarmSource, PublicationId, Retirement,
};
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

/// What one process knows of its cells' alarms.
#[derive(Default)]
pub(super) struct ProcAlarms {
    /// Alarm installations whose wake entry this process saw land.
    covered: BTreeSet<(String, PublicationId)>,
    /// Alarm handlers running: the cell and epoch each fires on.
    firing: BTreeMap<OpId, (String, Epoch)>,
    /// Alarm handlers that re-armed and finished, whose reply waits on its
    /// arm gate: the cell, the epoch, the alarm and the write's position.
    held: BTreeMap<OpId, (String, Epoch, AlarmSnapshot, u64)>,
}

/// Whose reply an arm gate holds.
#[derive(Clone, Copy, Debug)]
pub(super) enum Waiter {
    /// A client request's response.
    Request(RequestId),
    /// An alarm handler's `AlarmFinished`.
    Alarm(OpId),
}

/// The alarm work of one process.
pub(super) enum AlarmWork {
    /// An alarm handler finishes.
    Run { op: OpId },
    /// A wake entry write landed.
    Maintained {
        cell: String,
        alarm: AlarmSnapshot,
        report: bool,
        gate: Option<Waiter>,
    },
    /// The wake entry write an arm gate waits on failed.
    GateFailed { waiter: Waiter },
    /// The process lists the bucket's due wake entries.
    Scan,
}

impl World {
    pub(super) fn alarm_work(&mut self, node: usize, work: AlarmWork) {
        match work {
            AlarmWork::Run { op } => self.alarm_run(node, op),
            AlarmWork::Maintained {
                cell,
                alarm,
                report,
                gate,
            } => {
                self.wake_maintained(node, cell, alarm, report);
                if let Some(waiter) = gate {
                    self.arm_gate_settled(node, waiter, true);
                }
            }
            AlarmWork::GateFailed { waiter } => self.arm_gate_settled(node, waiter, false),
            AlarmWork::Scan => self.wake_scan(node),
        }
    }

    /// The alarm a database opened at `epoch` holds: what its last alarm
    /// write left, identified by the open epoch and the number of alarm
    /// writes, sampled at the database's position. A database no alarm
    /// write touched holds none, at sequence 0, as `wake_record::initialize`
    /// leaves it.
    pub(super) fn alarm_of(&self, content: &[WriteId], epoch: Epoch) -> AlarmSnapshot {
        let writes: Vec<Option<i64>> = content
            .iter()
            .filter_map(|w| self.alarm_writes.get(w).copied())
            .collect();
        let source = AlarmSource {
            id: PublicationId {
                epoch,
                sequence: writes.len() as u64,
            },
            position: content.len() as u64,
        };
        AlarmSnapshot::committed(writes.last().copied().flatten(), source)
    }

    /// Record that `write`, committed at `epoch`, sets the cell's alarm
    /// `ahead_ms` from now (or clears it), and start its arm gate: the wake
    /// entry write that `gate`, if any, waits on.
    pub(super) fn alarm_written(
        &mut self,
        node: usize,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
        ahead_ms: Option<u64>,
        gate: Option<Waiter>,
    ) -> AlarmSnapshot {
        let at_ms = ahead_ms.map(|ahead| (self.wall(node) + ahead) as i64);
        self.alarm_writes.insert(write, at_ms);
        if at_ms.is_some() {
            self.stats.alarms_set += 1;
        }
        let content = self.seen(node, cell, epoch).expect("an open database");
        let alarm = self.alarm_of(content, epoch);
        if at_ms.is_some() {
            self.publish_wake(node, cell, alarm, false, gate);
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
        let alarm = self.reported(self.alarm_of(self.seen(node, cell, *epoch)?, *epoch));
        Some((alarm, self.known_covered(node, cell, alarm)))
    }

    /// Whether this process has seen a wake entry for `alarm` land, as the
    /// flusher's coverage cache knows (`alarm_covered` in production).
    fn known_covered(&self, node: usize, cell: &str, alarm: AlarmSnapshot) -> bool {
        let covered = &self.machines[node].proc().alarms.covered;
        match (alarm.at_ms(), alarm.source()) {
            (Some(_), Some(source)) => covered.contains(&(cell.to_string(), source.id)),
            _ => true,
        }
    }

    /// An arm gate settled: its wake entry write landed or failed. The reply
    /// it held goes now.
    fn arm_gate_settled(&mut self, node: usize, waiter: Waiter, landed: bool) {
        match waiter {
            Waiter::Request(request) => self.response_arm_settled(node, request, landed),
            Waiter::Alarm(op) => {
                if let Some(event) = self.alarm_reply(node, op) {
                    self.drive(node, event);
                }
            }
        }
    }

    /// Send the `AlarmFinished` a re-arming handler's arm gate held, with
    /// the coverage the process now knows: covered once the entry landed,
    /// not if it failed. Production reports `Ok` either way.
    fn alarm_reply(&mut self, node: usize, op: OpId) -> Option<Event> {
        let held = self.machines[node].proc_mut().alarms.held.remove(&op);
        let (cell, epoch, alarm, position) = held?;
        let covered = self.known_covered(node, &cell, alarm);
        Some(Event::AlarmFinished {
            op,
            cell,
            epoch,
            now_ms: 0,
            now_mono_ms: 0,
            result: Ok((alarm, covered, Some(position))),
        })
    }

    /// Whether `cell`'s runtime at `epoch` holds an armed alarm that no
    /// wake entry in the bucket comes due for by its time. A runtime a newer
    /// epoch superseded gives up nothing: its entries were rightly retired.
    pub(super) fn alarm_uncovered(&self, node: usize, cell: &str, epoch: Epoch) -> bool {
        let current = self.store.owner(cell).is_none_or(|(_, e)| e <= epoch);
        current
            && self
                .seen(node, cell, epoch)
                .and_then(|content| self.alarm_of(content, epoch).at_ms())
                .is_some_and(|at_ms| !self.store.wake_covers(cell, at_ms))
    }

    /// The process on `node` is about to die: every alarm a client was told
    /// was set must be covered by a wake entry.
    pub(super) fn judge_death(&mut self, node: usize) {
        let who = self.who(node);
        let published: Vec<(String, Epoch)> = self.machines[node]
            .proc()
            .published
            .iter()
            .map(|(cell, epoch)| (cell.clone(), *epoch))
            .collect();
        for (cell, epoch) in published {
            if !self.alarm_uncovered(node, &cell, epoch) {
                continue;
            }
            let content = self.seen(node, &cell, epoch).expect("uncovered: open");
            let last = content
                .iter()
                .rev()
                .find(|w| self.alarm_writes.contains_key(w))
                .copied()
                .expect("uncovered: armed");
            if self.checker.acknowledged(&cell, last) {
                self.checker.died_uncovered(self.t, &who, &cell, last);
            }
        }
    }

    /// An activity on the cell is over: report its alarm, then finish it.
    pub(super) fn activity_done(&mut self, node: usize, request: RequestId, cell: &str) {
        self.touch(node, cell);
        if let Some((alarm, covered)) = self.observation(node, cell) {
            self.drive(node, observed(cell, alarm, covered));
        }
        self.drive(node, Event::ActivityFinished { request });
    }

    /// Write the wake entry for an alarm. When `report` is set, tell the
    /// core the coverage once it lands, as a handoff's refresh does. When
    /// `gate` is set, its reply waits for the write.
    pub(super) fn publish_wake(
        &mut self,
        node: usize,
        cell: &str,
        alarm: AlarmSnapshot,
        report: bool,
        gate: Option<Waiter>,
    ) {
        let req = StoreReq::Wake {
            cell: cell.to_string(),
            alarm,
            report,
            retire: false,
            gate,
        };
        self.store_call(node, req);
    }

    /// `ReconcileWakeEntry`: write the alarm's wake entry, then retire the
    /// entries it makes obsolete, if this owner can prove it may.
    pub(super) fn reconcile_wake(&mut self, node: usize, cell: &str, alarm: AlarmSnapshot) {
        let req = StoreReq::Wake {
            cell: cell.to_string(),
            alarm,
            report: false,
            retire: true,
            gate: None,
        };
        self.store_call(node, req);
    }

    /// The certificate `WakeFlusher::maintain` may publish for `alarm`, or
    /// `None` if one of its proofs fails: the alarm is still the one the
    /// process's open database holds, the bucket holds it durably, and the
    /// owner record names this node at its epoch. The sim checks all three
    /// at the instant the bucket applies the request.
    pub(super) fn retirement_proven(
        &self,
        node: usize,
        inc: u64,
        cell: &str,
        alarm: AlarmSnapshot,
    ) -> Option<Retirement> {
        let source = alarm.source()?;
        let epoch = source.id.epoch;
        let m = &self.machines[node];
        let current = m.up()
            && m.inc == inc
            && self
                .seen(node, cell, epoch)
                .is_some_and(|content| self.alarm_of(content, epoch).same_installation(alarm));
        let durable = self.store.durable_len(cell, epoch) as u64 >= source.position;
        let owned = self.store.owner(cell) == Some((Some(m.name.as_str()), epoch));
        (current && durable && owned).then_some(Retirement {
            id: source.id,
            at_ms: alarm.at_ms(),
        })
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
            p.alarms.covered.insert((cell.clone(), source.id));
        }
        if report {
            if let Some((alarm, covered)) = self.observation(node, &cell) {
                self.drive(node, observed(&cell, alarm, covered));
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
            immediate.push_back(observed(cell, alarm, covered));
        } else {
            self.publish_wake(node, cell, alarm, true, None);
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
            .alarms
            .firing
            .insert(op, (cell, epoch));
        let (lo, hi) = ALARM_RUN_MS;
        let run = self.work_rng.range(lo, hi);
        self.work(node, run, Work::Alarm(AlarmWork::Run { op }));
    }

    /// The alarm handler ran. If the alarm was due, it consumed it, or
    /// re-armed it, with one write.
    pub(super) fn alarm_run(&mut self, node: usize, op: OpId) {
        let Some((cell, epoch)) = self.machines[node].proc_mut().alarms.firing.remove(&op) else {
            return;
        };
        self.touch(node, &cell);
        let now = self.wall(node) as i64;
        let result = match self.seen(node, &cell, epoch) {
            None => Err(Failure::Definite),
            Some(content) => {
                let due = self
                    .alarm_of(content, epoch)
                    .at_ms()
                    .is_some_and(|at| at <= now);
                if due {
                    let rearm = self
                        .work_rng
                        .chance(0.3)
                        .then(|| self.work_rng.range(ALARM_AHEAD_MS.0, ALARM_AHEAD_MS.1));
                    let (write, position) = self
                        .commit_write(node, &cell, epoch)
                        .expect("open database");
                    let gate = Some(Waiter::Alarm(op));
                    let alarm = self.alarm_written(node, &cell, epoch, write, rearm, gate);
                    let alarm = self.reported(alarm);
                    self.stats.alarms_fired += 1;
                    if rearm.is_some() {
                        // The re-arm's arm gate holds the reply.
                        let held = (cell, epoch, alarm, position);
                        self.machines[node].proc_mut().alarms.held.insert(op, held);
                        return;
                    }
                    Ok((alarm, true, Some(position)))
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

    /// A handoff aborted the alarm handler before it ran to the end. One
    /// that already finished and waits on its arm gate stops waiting, and
    /// reports what it knows: production drops the gated reply and reads
    /// the coverage cache.
    pub(super) fn abort_alarm(&mut self, node: usize, op: OpId) {
        if let Some(event) = self.alarm_reply(node, op) {
            return self.deliver(node, 1, event);
        }
        if let Some((cell, epoch)) = self.machines[node].proc_mut().alarms.firing.remove(&op) {
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
        self.work(node, delay, Work::Alarm(AlarmWork::Scan));
    }

    /// List the due wake entries and hint the core about each.
    ///
    /// The listing and the lease reads behind the election read the bucket
    /// directly, at this instant: no latency and no failed calls, only a
    /// partition stops them. Production's scan retries on its own timer and
    /// never reports to the core, so a failed listing only delays a hint.
    pub(super) fn wake_scan(&mut self, node: usize) {
        self.work(node, WAKE_TICK_MS, Work::Alarm(AlarmWork::Scan));
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
        for (cell, entry_ms) in self.store.wake_due(now as i64) {
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
    /// When the audit ends, the same is watched.
    pub(super) fn judge_alarms(&mut self, audit: bool) {
        let now = (WALL_EPOCH_MS + self.t) as i64;
        for cell in 0..self.config.cells {
            let cell = format!("c{cell}");
            let Some((epoch, content)) = self.store.newest(&cell) else {
                continue;
            };
            let at_ms = self.alarm_of(content, epoch).at_ms();
            if let Some(at_ms) = at_ms.filter(|at| *at + (ALARM_SLACK_MS as i64) < now) {
                if audit {
                    self.checker.audit_alarm_unfired(self.t, &cell, now - at_ms);
                } else {
                    self.checker.alarm_unfired(self.t, &cell, now - at_ms);
                }
            }
        }
    }
}

/// The runtime's report of a cell's alarm (`AlarmObserved`). `stamp` sets
/// its clocks when it reaches the core.
pub(super) fn observed(cell: &str, alarm: AlarmSnapshot, covered: bool) -> Event {
    Event::AlarmObserved {
        cell: cell.to_string(),
        alarm,
        covered,
        now_ms: 0,
        now_mono_ms: 0,
    }
}
