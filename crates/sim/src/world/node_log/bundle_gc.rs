//! Moving bundle rows into the per-cell layout, and deleting the bundles
//! that layout covers, as `ltx_repl.rs` and `node_log.rs` do.
//!
//! - Compaction (`compact_cell`): once credited bundles cover enough rows
//!   of an open cell epoch past its per-cell coverage, its owner folds the
//!   longest of its session's bundle rows of that epoch into the per-cell
//!   prefix, and raises its own coverage (`compacted_txid`). The call
//!   applies even if its sender has died since.
//! - Bundle GC (`gc_bundles`): on the maintenance tick a leader deletes
//!   every bundle of its own session whose rows the per-cell layout covers
//!   (`bundle_deletable`). It judges an epoch it holds open by its own
//!   coverage, and any other by the bucket: the epoch's prefix and base, or
//!   its retired mark (`covered_txid`).
//! - A boot whose predecessor's log is recovered deletes the bundles of
//!   every other generation of its node (`gc_stale_generation_bundles`).
//!   Production lists and deletes after the recovery's answer; here the
//!   sweep happens at that instant and cannot fail.

use super::super::bucket::StoreReq;
use super::super::{shutdown, World};
use crate::{Mutation, WriteId};
use celld_logic::Epoch;

impl World {
    /// The maintenance tick's bundle GC: delete every bundle of this
    /// process's session whose rows the per-cell layout covers.
    pub(super) fn gc_bundles(&mut self, node: usize) {
        let p = self.machines[node].proc();
        let Some(log) = p.log.as_ref().filter(|_| p.shutdown.is_none()) else {
            return;
        };
        let session = log.session.clone();
        let everything = self.config.mutation == Some(Mutation::GcUncovered);
        let seqs: Vec<u64> = (self.store.bundles_of(&session).into_iter())
            .filter(|(_, rows)| {
                everything
                    || (rows.iter())
                        .all(|(cell, epoch, len)| *len <= self.covered_txid(node, cell, *epoch))
            })
            .map(|(seq, _)| seq)
            .collect();
        if !seqs.is_empty() {
            self.store_call(node, StoreReq::DeleteBundles { session, seqs });
        }
    }

    /// `covered_txid`: an epoch this process holds open answers from its
    /// own coverage, which can only be lower than the bucket's; any other
    /// from the bucket, a retired epoch as covered whole.
    fn covered_txid(&self, node: usize, cell: &str, epoch: Epoch) -> usize {
        (self.percell_here(node, cell, epoch))
            .unwrap_or_else(|| self.store.percell_len(cell, epoch))
    }

    /// Delete bundles of `session`, at the instant the delete reaches the
    /// bucket, and judge every row they carried: an acknowledged write in
    /// one must be in the per-cell layout, in its epoch's prefix or in the
    /// cell's newest database. A row no credit counted, such as a fenced
    /// leader's late flush, may hold writes nobody acknowledged.
    pub(in crate::world) fn delete_bundles(&mut self, session: &str, seqs: &[u64]) {
        let before = self.store.bundles_of(session).len();
        let rows = self.store.delete_bundles(session, seqs);
        self.stats.bundles_deleted += (before - self.store.bundles_of(session).len()) as u64;
        for (cell, epoch, content) in rows {
            let covered = self.store.percell_len(&cell, epoch).min(content.len());
            let Ok(newest) = self.store.newest(&cell) else {
                // A broken chain restores nothing at all: report that, not
                // each write as lost.
                self.checker.unrestorable(self.t, session, &cell, epoch);
                continue;
            };
            let newest = newest.unwrap_or_default();
            let lost: Vec<WriteId> = (content[covered..].iter())
                .filter(|&&w| self.checker.answered(&cell, w) && !newest.contains(&w))
                .copied()
                .collect();
            self.checker
                .bundle_deleted(self.t, session, &cell, epoch, &lost);
        }
    }

    /// A boot recovered its predecessor: every bundle of an earlier
    /// generation of this node is garbage.
    pub(in crate::world) fn gc_stale_generations(&mut self, node: usize) {
        for session in self.store.bundle_sessions(&shutdown::generations_of(node)) {
            let seqs: Vec<u64> = (self.store.bundles_of(&session).into_iter())
                .map(|(seq, _)| seq)
                .collect();
            self.delete_bundles(&session, &seqs);
        }
    }

    /// A compaction round reaches the bucket: fold the longest of
    /// `session`'s bundle rows of the cell epoch into its prefix. Returns
    /// how much of the epoch the prefix holds after.
    pub(in crate::world) fn apply_compaction(
        &mut self,
        writer: &str,
        session: &str,
        cell: &str,
        epoch: Epoch,
    ) -> usize {
        let rows = self.store.bundle_rows(session, Some(cell));
        let prefix = self.store.prefix_len(cell, epoch);
        match rows.get(&(cell.to_string(), epoch)) {
            Some(row) if row.len() > prefix => {
                self.stats.compactions += 1;
                self.land_upload(writer, cell, epoch, row);
                row.len()
            }
            _ => prefix,
        }
    }
}
