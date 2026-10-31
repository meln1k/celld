//! The simulated bucket.
//!
//! It provides what `docs/guarantees.md` requires of a real bucket and
//! nothing more: conditional writes on ETags and read-after-write
//! consistency. Each operation applies atomically at one simulated instant.
//! Latency and loss around that instant belong to the world, not here.
//!
//! It holds three kinds of object:
//!
//! - node leases, `nodes/<node>.json`;
//! - ownership records, `cells/<cell>/own.json`;
//! - replica prefixes, `cells/<cell>/ltx/e<epoch>/`.

use std::collections::BTreeMap;

use celld_logic::{CasGuard, CasOutcome, Epoch, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord};

use crate::WriteId;

#[derive(Default)]
pub struct Store {
    next_etag: u64,
    leases: BTreeMap<String, (NodeLeaseRecord, u64)>,
    /// Cell → (owner node, epoch, etag).
    owners: BTreeMap<String, (Option<String>, Epoch, u64)>,
    /// One prefix per cell epoch. A cell's database is a list of writes, so
    /// a prefix holds the longest list any upload into it carried. Real LTX
    /// files are immutable and named by transaction range: a late or repeated
    /// upload adds files and never rolls the prefix back. Keeping the longest
    /// list has the same effect.
    prefixes: BTreeMap<(String, Epoch), Vec<WriteId>>,
}

impl Store {
    fn etag(&mut self) -> u64 {
        self.next_etag += 1;
        self.next_etag
    }

    pub fn read_lease(&self, node: &str) -> Option<NodeLeaseRecord> {
        self.leases.get(node).map(|(record, etag)| NodeLeaseRecord {
            etag: etag.to_string(),
            ..record.clone()
        })
    }

    pub fn cas_lease(&mut self, guard: &CasGuard, record: &NodeLeaseRecord) -> LeaseCasOutcome {
        let current = self.leases.get(&record.node).map(|(_, etag)| *etag);
        if !guard_matches(guard, current) {
            return LeaseCasOutcome::Rejected;
        }
        let etag = self.etag();
        self.leases
            .insert(record.node.clone(), (record.clone(), etag));
        LeaseCasOutcome::Applied {
            etag: etag.to_string(),
        }
    }

    pub fn read_owner(&self, cell: &str) -> Option<OwnerRecord> {
        self.owners
            .get(cell)
            .map(|(node, epoch, etag)| OwnerRecord {
                node: node.clone(),
                epoch: *epoch,
                etag: etag.to_string(),
            })
    }

    pub fn cas_owner(
        &mut self,
        cell: &str,
        guard: &CasGuard,
        epoch: Epoch,
        node: &str,
    ) -> CasOutcome {
        let current = self.owners.get(cell).map(|(_, _, etag)| *etag);
        if !guard_matches(guard, current) {
            return CasOutcome::Rejected;
        }
        let etag = self.etag();
        self.owners
            .insert(cell.to_string(), (Some(node.to_string()), epoch, etag));
        CasOutcome::Applied
    }

    /// Upload the whole database of one epoch into its prefix.
    pub fn put(&mut self, cell: &str, epoch: Epoch, content: &[WriteId]) {
        let prefix = self.prefixes.entry((cell.to_string(), epoch)).or_default();
        if content.len() > prefix.len() {
            *prefix = content.to_vec();
        }
    }

    /// What a restore at `epoch` reads: the newest prefix below it that
    /// holds data. Every upload carries the whole database, so that one
    /// prefix is the whole chain.
    pub fn read_chain(&self, cell: &str, epoch: Epoch) -> Option<Vec<WriteId>> {
        self.prefixes
            .range((cell.to_string(), 0)..(cell.to_string(), epoch))
            .rev()
            .find(|(_, writes)| !writes.is_empty())
            .map(|(_, writes)| writes.clone())
    }

    pub fn prefix(&self, cell: &str, epoch: Epoch) -> &[WriteId] {
        self.prefixes
            .get(&(cell.to_string(), epoch))
            .map_or(&[], Vec::as_slice)
    }
}

fn guard_matches(guard: &CasGuard, current: Option<u64>) -> bool {
    match (guard, current) {
        (CasGuard::Absent, None) => true,
        (CasGuard::Match(etag), Some(current)) => *etag == current.to_string(),
        _ => false,
    }
}
