//! The checkers. The shell reports what happened, one hook per fact, and
//! nothing here drives the fleet: a checker reads the bucket and the alarm
//! ledger and keeps its own records.

use super::*;
use alarms::AlarmLedger;

/// What a checker may read when a fact arrives.
#[derive(Clone, Copy)]
pub(super) struct View<'a> {
    pub t: Time,
    pub store: &'a Store,
    pub ledger: &'a AlarmLedger,
}

#[derive(Debug)]
struct Ack {
    t: Time,
    cell: String,
    epoch: Epoch,
    write: WriteId,
}

#[derive(Debug)]
struct RestoreRecord {
    t: Time,
    proc: ProcId,
    cell: String,
    epoch: Epoch,
    source: &'static str,
    content: BTreeSet<WriteId>,
}

/// One handler run.
#[derive(Debug)]
struct Fire {
    t: Time,
    proc: ProcId,
    epoch: Epoch,
    armed_by: WriteId,
}

/// Names one handler run, so its proven commit can settle it.
#[derive(Clone, Copy, Debug)]
pub(super) struct FireId(usize);

#[derive(Default)]
pub(super) struct Checker {
    flagged: BTreeSet<String>,
    violations: Vec<Violation>,
    acks: Vec<Ack>,
    restores: Vec<RestoreRecord>,
    runtime_owner: BTreeMap<(String, Epoch), ProcId>,
    /// Alarm installations the protocol owes a run: armed by a write a
    /// client saw acknowledged, or by the proven commit of a handler that
    /// ran such an alarm.
    promised: BTreeSet<WriteId>,
    fires: Vec<Fire>,
    /// The first proven run of each alarm installation, by its arming write.
    alarms_settled: BTreeMap<WriteId, FireId>,
}

impl Checker {
    pub(super) fn violations(&self) -> &[Violation] {
        &self.violations
    }

    pub(super) fn into_violations(self) -> Vec<Violation> {
        self.violations
    }

    fn flag(&mut self, t: Time, kind: &'static str, key: String, detail: String) {
        if self.flagged.insert(format!("{kind}/{key}")) {
            self.violations.push(Violation {
                at_ms: t,
                kind,
                detail,
            });
        }
    }

    // -----------------------------------------------------------------------
    // One writer per epoch

    /// A PUT landed in a cell epoch's prefix, from `writers` processes so far.
    pub(super) fn on_upload(&mut self, view: View, cell: &str, epoch: Epoch, writers: usize) {
        if writers > 1 {
            self.flag(
                view.t,
                "two-writers",
                format!("prefix/{cell}/{epoch}"),
                format!("{writers} processes uploaded into the prefix of {cell} epoch {epoch}"),
            );
        }
        self.check_discoverable(view, cell);
    }

    /// A process started a runtime: only at an epoch its own claim installed,
    /// and only one process per epoch.
    pub(super) fn on_runtime_start(&mut self, view: View, id: ProcId, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        match self.runtime_owner.get(&key) {
            Some(other) if *other != id => {
                let other = *other;
                self.flag(
                    view.t,
                    "two-writers",
                    format!("runtime/{cell}/{epoch}"),
                    format!("p{id} started {cell} epoch {epoch}, which p{other} already ran"),
                );
            }
            _ => {
                self.runtime_owner.insert(key, id);
            }
        }
        let claimed = view
            .store
            .claims
            .iter()
            .any(|claim| claim.cell == cell && claim.epoch == epoch && claim.proc == id);
        if !claimed {
            self.flag(
                view.t,
                "unclaimed-runtime",
                format!("{cell}/{epoch}/{id}"),
                format!("p{id} started {cell} epoch {epoch} without an applied claim of its own"),
            );
        }
    }

    pub(super) fn on_local_route_without_runtime(
        &mut self,
        t: Time,
        id: ProcId,
        request: RequestId,
        cell: &str,
    ) {
        self.flag(
            t,
            "local-without-runtime",
            format!("{id}/{request}"),
            format!("p{id} routed request {request} for {cell} locally with no published runtime"),
        );
    }

    // -----------------------------------------------------------------------
    // Acknowledged writes

    /// A process opened a replica of `cell` at `epoch` holding `content`.
    pub(super) fn on_restore(
        &mut self,
        t: Time,
        id: ProcId,
        cell: &str,
        epoch: Epoch,
        source: &'static str,
        content: &[WriteId],
    ) {
        self.restores.push(RestoreRecord {
            t,
            proc: id,
            cell: cell.to_string(),
            epoch,
            source,
            content: content.iter().copied().collect(),
        });
    }

    /// A client is about to see `write` acknowledged: it must already be at
    /// `position` in its epoch's prefix.
    pub(super) fn on_reply_acked(
        &mut self,
        view: View,
        id: ProcId,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
        position: u64,
    ) {
        let in_prefix = view
            .store
            .prefix(cell, epoch)
            .and_then(|prefix| prefix.entries.get(&position))
            == Some(&write);
        if !in_prefix {
            self.flag(
                view.t,
                "ack-before-durable",
                format!("{write}"),
                format!(
                    "p{id} acknowledged write {write} of {cell} epoch {epoch} at position \
                     {position}, which the epoch's prefix does not hold"
                ),
            );
        }
        self.acknowledge(view, cell, epoch, write, true);
    }

    /// A write is proven at its epoch: the lost-write check now holds it,
    /// and the discovery check too when it carries a promised alarm.
    fn acknowledge(
        &mut self,
        view: View,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
        promised: bool,
    ) {
        if promised && view.ledger.carries(write) {
            self.promised.insert(write);
        }
        self.check_discoverable(view, cell);
        self.acks.push(Ack {
            t: view.t,
            cell: cell.to_string(),
            epoch,
            write,
        });
    }

    /// No lost acknowledged write: every restore at epoch `E` holds every
    /// write acknowledged at an epoch below `E`. An acknowledgement verified
    /// the ownership record after its upload, so a later claim, and the
    /// restore that follows it, reads a lineage that contains the write,
    /// whenever the acknowledgement itself is delivered.
    fn check_restores(&mut self) {
        let mut acks: BTreeMap<&str, Vec<&Ack>> = BTreeMap::new();
        for ack in &self.acks {
            acks.entry(ack.cell.as_str()).or_default().push(ack);
        }
        let mut found = Vec::new();
        for restore in &self.restores {
            let Some(cell_acks) = acks.get(restore.cell.as_str()) else {
                continue;
            };
            let missing: Vec<&&Ack> = cell_acks
                .iter()
                .filter(|ack| ack.epoch < restore.epoch && !restore.content.contains(&ack.write))
                .collect();
            if let Some(first) = missing.first() {
                found.push((
                    format!("{}/{}/{}", restore.cell, restore.epoch, restore.proc),
                    restore.t,
                    format!(
                        "p{} restored {} epoch {} from {} at t={} without {} acknowledged \
                         write(s), first {} (epoch {}, acknowledged at t={})",
                        restore.proc,
                        restore.cell,
                        restore.epoch,
                        restore.source,
                        restore.t,
                        missing.len(),
                        first.write,
                        first.epoch,
                        first.t
                    ),
                ));
            }
        }
        for (key, t, detail) in found {
            self.flag(t, "lost-write", key, detail);
        }
    }

    // -----------------------------------------------------------------------
    // Alarms

    /// A handler ran the alarm `armed_by` installed. Alarms are at least
    /// once, so a repeat is counted; only the current owner repeating a run
    /// whose consume was proven is a violation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_fire(
        &mut self,
        view: View,
        id: ProcId,
        node: &str,
        cell: &str,
        epoch: Epoch,
        armed_by: WriteId,
        stats: &mut Stats,
    ) -> FireId {
        if self.fires.iter().any(|fire| fire.armed_by == armed_by) {
            // An earlier run's consume was never proven, or is still being
            // proven.
            stats.alarm_reruns += 1;
        }
        if let Some(settled) = self.alarms_settled.get(&armed_by) {
            let current = view
                .store
                .read_owner(cell)
                .is_some_and(|owner| owner.epoch == epoch && owner.node.as_deref() == Some(node));
            if current {
                let first = &self.fires[settled.0];
                let detail = format!(
                    "p{id}, the owner of {cell} at epoch {epoch}, ran the alarm installed by \
                     write {armed_by}, whose run by p{} at epoch {} (t={}) was already proven \
                     consumed",
                    first.proc, first.epoch, first.t
                );
                self.flag(
                    view.t,
                    "alarm-refired",
                    format!("{cell}/{armed_by}/{epoch}"),
                    detail,
                );
            } else {
                // A superseded owner that still believes its lease, as a
                // suspended VM does on resume: its commit can never be
                // acknowledged, but the handler's side effects repeat.
                stats.stale_alarm_runs += 1;
            }
        }
        self.fires.push(Fire {
            t: view.t,
            proc: id,
            epoch,
            armed_by,
        });
        FireId(self.fires.len() - 1)
    }

    /// The core's gate proved a handler's commit `write`: it is in its
    /// epoch's prefix and the owner record named that epoch afterwards,
    /// which is what acknowledges any write.
    pub(super) fn on_handler_commit_proven(
        &mut self,
        view: View,
        fire: FireId,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
    ) {
        let armed_by = self.fires[fire.0].armed_by;
        self.alarms_settled.entry(armed_by).or_insert(fire);
        let promised = self.promised.contains(&armed_by);
        self.acknowledge(view, cell, epoch, write, promised);
    }

    /// The bucket's view of `cell` changed. An acknowledged arm must stay
    /// discoverable: whenever the alarm a restore would load is armed by an
    /// acknowledged write, some wake entry for the cell survives its
    /// retirement record. Without one, an evicted or orphaned cell waits for
    /// a request that may never come.
    pub(super) fn check_discoverable(&mut self, view: View, cell: &str) {
        let Some(content) = view.store.read_chain(cell, Epoch::MAX) else {
            return;
        };
        let row = view.ledger.row(&content);
        let (Some(at_ms), Some(armed_by)) = (row.at_ms, row.armed_by) else {
            return;
        };
        if !self.promised.contains(&armed_by) || view.store.discoverable(cell) {
            return;
        }
        let detail = format!(
            "{cell}'s durable alarm at {at_ms}, armed by promised write {armed_by}, \
             has no wake entry its retirement record leaves standing"
        );
        self.flag(
            view.t,
            "alarm-undiscoverable",
            format!("{cell}/{armed_by}"),
            detail,
        );
    }

    /// Every alarm the bucket still holds and can find must fire with no
    /// request to wake its cell. `contents` is what the audit node, which
    /// restored the fleet from the bucket alone, holds for each cell, and
    /// `now_ms` its wall clock.
    pub(super) fn check_alarms_fired(
        &mut self,
        view: View,
        audit: ProcId,
        now_ms: i64,
        grace_ms: u64,
        contents: Vec<(String, Vec<WriteId>)>,
        stats: &mut Stats,
    ) {
        let mut lost = Vec::new();
        for (cell, content) in contents {
            let row = view.ledger.row(&content);
            let (Some(at_ms), Some(armed_by)) = (row.at_ms, row.armed_by) else {
                continue;
            };
            if at_ms + grace_ms as i64 > now_ms {
                continue;
            }
            if !self.promised.contains(&armed_by) && !view.store.discoverable(&cell) {
                // Committed, never promised and never published: the
                // protocol owes it nothing.
                stats.alarms_stranded += 1;
                continue;
            }
            let ran = self
                .fires
                .iter()
                .any(|fire| fire.proc == audit && fire.armed_by == armed_by);
            if !ran {
                lost.push((cell, at_ms, armed_by));
            }
        }
        stats.audit_alarm_runs +=
            self.fires.iter().filter(|fire| fire.proc == audit).count() as u64;
        for (cell, at_ms, armed_by) in lost {
            let late = now_ms - at_ms;
            let detail = format!(
                "{cell}'s alarm armed by write {armed_by} was due {late} ms ago and no node \
                 fired it after the fleet restarted from the bucket"
            );
            self.flag(view.t, "alarm-lost", format!("{cell}/{armed_by}"), detail);
        }
    }

    // -----------------------------------------------------------------------
    // The end of the run

    /// The final checks. `served` lists each probe and audit operation as
    /// `(phase, cell, succeeded)`.
    pub(super) fn finish(&mut self, t: Time, served: Vec<(&'static str, String, bool)>) {
        self.check_restores();
        for (phase, cell, succeeded) in served {
            if !succeeded {
                self.flag(
                    t,
                    "liveness",
                    format!("{phase}/{cell}"),
                    format!(
                        "{phase}: {cell} did not serve a request within the window after faults \
                         healed"
                    ),
                );
            }
        }
    }
}
