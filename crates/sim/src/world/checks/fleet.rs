//! The checker's ledger of the node log: what each follower fsynced, which
//! bundles a recovery still reads, which writes fleet credit truly proved,
//! and which of those recovery recorded as lost.

use super::*;
use crate::store::Session;
use celld_logic::log_tier::LogState;

#[derive(Default)]
pub(in crate::world) struct FleetLedger {
    /// Every frame a follower fsynced: member, session, log epoch, row.
    fsynced: BTreeSet<(String, Session, Epoch, Row)>,
    /// The rows of bundles that landed while their session's record was
    /// open, so a recovery still gathers them.
    gatherable: BTreeSet<Row>,
    /// Rows fleet credit covered while every member's disk held them, by
    /// session and log epoch.
    proven: BTreeMap<(Session, Epoch), Vec<Row>>,
    /// The writes of every proven row, lost or not: an acknowledgement
    /// they covered was durable when it was given.
    proven_writes: BTreeSet<WriteId>,
    /// The proven rows a restore can still load: every one not excused.
    loadable: BTreeSet<Row>,
    /// Proven writes no surviving disk or object holds, and their cells,
    /// whose alarms may then be anything the lost writes replaced.
    excused: BTreeSet<WriteId>,
    excused_cells: BTreeSet<String>,
}

impl FleetLedger {
    /// Follower `member` fsynced `row` into its fragment of `session`'s log
    /// epoch `epoch`.
    pub(in crate::world) fn on_fsync(
        &mut self,
        member: &str,
        session: &Session,
        epoch: Epoch,
        row: &Row,
    ) {
        self.fsynced
            .insert((member.to_string(), session.clone(), epoch, row.clone()));
    }

    /// A bundle of `session` carrying `rows` landed.
    pub(in crate::world) fn on_bundle(&mut self, view: View, session: &Session, rows: &[Row]) {
        let open = view
            .store
            .read_lease_log(&session.node)
            .is_some_and(|(lease, log)| {
                session.owns(&lease) && log.is_some_and(|log| log.record.state == LogState::Open)
            });
        if open {
            self.gatherable.extend(rows.iter().cloned());
        }
    }

    /// `session`'s fleet credit at log epoch `epoch` covered `row`. It
    /// proves the row only if every member of the session's published
    /// record fsynced it in a fragment of the record's epoch.
    pub(in crate::world) fn on_credit(
        &mut self,
        view: View,
        session: &Session,
        epoch: Epoch,
        row: &Row,
    ) {
        let Some((lease, Some(log))) = view.store.read_lease_log(&session.node) else {
            return;
        };
        let record = &log.record;
        let held = session.owns(&lease)
            && !record.ensemble.is_empty()
            && record.ensemble.iter().all(|member| {
                self.fsynced
                    .contains(&(member.clone(), session.clone(), record.epoch, row.clone()))
            });
        if held {
            self.proven_writes.insert(row.write);
            self.loadable.insert(row.clone());
            self.proven
                .entry((session.clone(), epoch))
                .or_default()
                .push(row.clone());
        }
    }

    /// Recovery recorded a loss for `session`'s log epoch `epoch`. The
    /// proven rows outside the bucket that no disk holds any more are
    /// beyond what the protocol promises; returns how many there are, and
    /// how many a disk still holds.
    pub(in crate::world) fn on_loss_record(
        &mut self,
        view: View,
        session: &Session,
        epoch: Epoch,
    ) -> (usize, usize) {
        let (on_disk, lost): (Vec<Row>, Vec<Row>) = self
            .proven
            .get(&(session.clone(), epoch))
            .into_iter()
            .flatten()
            .filter(|row| !view.store.holds(row))
            .cloned()
            .partition(|row| {
                view.nodes
                    .iter()
                    .any(|node| node.disk.holds(session, epoch, row))
            });
        // What a restore can load no longer includes them.
        self.loadable.retain(|row| !lost.contains(row));
        let counts = (lost.len(), on_disk.len());
        for row in lost {
            self.excused.insert(row.write);
            self.excused_cells.insert(row.cell);
        }
        counts
    }

    /// Whether the node log made `row` durable: a bundle its session's
    /// recovery will read carries it, or every follower fsynced it.
    pub(super) fn covers(&self, row: &Row) -> bool {
        self.gatherable.contains(row) || self.proven_writes.contains(&row.write)
    }

    pub(super) fn excused(&self, write: WriteId) -> bool {
        self.excused.contains(&write)
    }

    pub(super) fn excused_cell(&self, cell: &str) -> bool {
        self.excused_cells.contains(cell)
    }

    /// What a restore of `cell` loads once recovery or a fold gathered every
    /// tail: the newest per-cell content, extended by the rows bundles and
    /// fleet proofs hold for that epoch, or a newer one they alone hold.
    pub(super) fn durable_content(&self, view: View, cell: &str) -> Option<Vec<WriteId>> {
        let tails = || {
            view.store.bundled_rows(cell).chain(
                self.loadable
                    .range(Row::first(cell)..)
                    .take_while(|row| row.cell == cell),
            )
        };
        let newest = view.store.newest_prefix(cell);
        let tail = tails().map(|row| row.epoch).max();
        let (epoch, base) = match (newest, tail) {
            (Some((epoch, content)), Some(tail)) if tail == epoch => (epoch, content),
            (newest, Some(tail)) if newest.as_ref().is_none_or(|(epoch, _)| tail > *epoch) => {
                (tail, newest.map(|(_, content)| content).unwrap_or_default())
            }
            (newest, _) => return newest.map(|(_, content)| content),
        };
        let mut entries: BTreeMap<u64, WriteId> = (1..).zip(base).collect();
        for row in tails() {
            if row.epoch == epoch {
                entries.insert(row.position, row.write);
            }
        }
        Some(
            entries
                .into_iter()
                .zip(1..)
                .take_while(|((position, _), expected)| position == expected)
                .map(|((_, write), _)| write)
                .collect(),
        )
    }
}
