//! The simulated bucket.
//!
//! It provides exactly what `docs/guarantees.md` requires of a real one and
//! nothing more: conditional create and conditional overwrite on ETags,
//! read-after-write consistency, and unconditional PUTs for replica data.
//! Every operation is applied atomically at one simulated instant; the
//! latency and loss around that instant belong to the world.

use celld_logic::{CasGuard, CasOutcome, Epoch, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord};
use std::collections::BTreeMap;

use crate::{Mutation, ProcId, WriteId};

/// One replica prefix, `cells/<cell>/ltx/e<epoch>/`.
///
/// LTX files are immutable objects named by their transaction range, so a
/// late or repeated upload adds files and never rolls the prefix back. The
/// model keeps that property: each upload contributes the database content
/// by position, and a position, once written, is only ever rewritten with
/// the same write by the epoch's single writer.
#[derive(Clone, Debug, Default)]
pub struct Prefix {
    pub entries: BTreeMap<u64, WriteId>,
    /// Every process that ever uploaded into this prefix. More than one is
    /// two writers in one epoch.
    pub writers: Vec<ProcId>,
}

impl Prefix {
    /// The database a restore of this prefix produces: the contiguous run
    /// of positions from the first.
    pub fn content(&self) -> Vec<WriteId> {
        let mut content = Vec::new();
        for (position, write) in &self.entries {
            if *position != content.len() as u64 + 1 {
                break;
            }
            content.push(*write);
        }
        content
    }
}

#[derive(Clone, Debug)]
struct OwnerEntry {
    node: Option<String>,
    epoch: Epoch,
    etag: u64,
}

#[derive(Clone, Debug)]
pub struct LeaseEntry {
    pub record: NodeLeaseRecord,
    pub etag: u64,
    /// Load the writing process reported with this record.
    pub resident: usize,
    pub owned: usize,
    pub sampled_ms: u64,
}

/// One applied ownership install, in store order.
#[derive(Clone, Debug)]
pub struct Claim {
    pub cell: String,
    pub epoch: Epoch,
    pub proc: ProcId,
}

pub struct Store {
    mutation: Mutation,
    next_etag: u64,
    owners: BTreeMap<String, OwnerEntry>,
    leases: BTreeMap<String, LeaseEntry>,
    prefixes: BTreeMap<(String, Epoch), Prefix>,
    pub claims: Vec<Claim>,
}

impl Store {
    pub fn new(mutation: Mutation) -> Self {
        Self {
            mutation,
            next_etag: 1,
            owners: BTreeMap::new(),
            leases: BTreeMap::new(),
            prefixes: BTreeMap::new(),
            claims: Vec::new(),
        }
    }

    fn etag(&mut self) -> u64 {
        let etag = self.next_etag;
        self.next_etag += 1;
        etag
    }

    fn guard_matches(&self, guard: &CasGuard, current: Option<u64>) -> bool {
        if self.mutation == Mutation::IgnoreCasGuard {
            return true;
        }
        match (guard, current) {
            (CasGuard::Absent, None) => true,
            (CasGuard::Match(etag), Some(current)) => *etag == current.to_string(),
            _ => false,
        }
    }

    pub fn read_owner(&self, cell: &str) -> Option<OwnerRecord> {
        self.owners.get(cell).map(|entry| OwnerRecord {
            node: entry.node.clone(),
            epoch: entry.epoch,
            etag: entry.etag.to_string(),
        })
    }

    pub fn owner_epoch(&self, cell: &str) -> Option<Epoch> {
        self.owners.get(cell).map(|entry| entry.epoch)
    }

    pub fn cas_owner(
        &mut self,
        cell: &str,
        guard: &CasGuard,
        epoch: Epoch,
        node: &str,
        proc: ProcId,
    ) -> CasOutcome {
        let current = self.owners.get(cell).map(|entry| entry.etag);
        if !self.guard_matches(guard, current) {
            return CasOutcome::Rejected;
        }
        let etag = self.etag();
        self.owners.insert(
            cell.to_string(),
            OwnerEntry {
                node: Some(node.to_string()),
                epoch,
                etag,
            },
        );
        self.claims.push(Claim {
            cell: cell.to_string(),
            epoch,
            proc,
        });
        CasOutcome::Applied
    }

    /// `release_owner` in `ownership_store.rs`: read, then conditionally
    /// overwrite with an unowned record that keeps the epoch.
    pub fn release_owner(&mut self, cell: &str, epoch: Epoch, node: &str) -> CasOutcome {
        let Some(current) = self.owners.get(cell).cloned() else {
            return CasOutcome::Rejected;
        };
        if current.node.as_deref() != Some(node) || current.epoch != epoch {
            return CasOutcome::Rejected;
        }
        let etag = self.etag();
        self.owners.insert(
            cell.to_string(),
            OwnerEntry {
                node: None,
                epoch,
                etag,
            },
        );
        CasOutcome::Applied
    }

    pub fn read_lease(&self, node: &str) -> Option<NodeLeaseRecord> {
        self.leases.get(node).map(|entry| {
            let mut record = entry.record.clone();
            record.etag = entry.etag.to_string();
            record
        })
    }

    pub fn cas_lease(
        &mut self,
        guard: &CasGuard,
        record: &NodeLeaseRecord,
        resident: usize,
        owned: usize,
        sampled_ms: u64,
    ) -> LeaseCasOutcome {
        let current = self.leases.get(&record.node).map(|entry| entry.etag);
        if !self.guard_matches(guard, current) {
            return LeaseCasOutcome::Rejected;
        }
        let etag = self.etag();
        let mut stored = record.clone();
        stored.etag = String::new();
        self.leases.insert(
            record.node.clone(),
            LeaseEntry {
                record: stored,
                etag,
                resident,
                owned,
                sampled_ms,
            },
        );
        LeaseCasOutcome::Applied {
            etag: etag.to_string(),
        }
    }

    pub fn leases(&self) -> impl Iterator<Item = &LeaseEntry> {
        self.leases.values()
    }

    fn prefix_key(&self, cell: &str, epoch: Epoch) -> (String, Epoch) {
        if self.mutation == Mutation::SharedPrefix {
            // The broken layout: one prefix for every epoch, so a stale
            // owner's late upload lands in its successor's lineage.
            (cell.to_string(), 0)
        } else {
            (cell.to_string(), epoch)
        }
    }

    /// Upload the database content of `epoch` by position. Returns how many
    /// distinct processes have now written this prefix.
    pub fn put_ltx(
        &mut self,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
        proc: ProcId,
    ) -> usize {
        let key = self.prefix_key(cell, epoch);
        let prefix = self.prefixes.entry(key).or_default();
        for (index, write) in content.iter().enumerate() {
            prefix.entries.insert(index as u64 + 1, *write);
        }
        if !prefix.writers.contains(&proc) {
            prefix.writers.push(proc);
        }
        prefix.writers.len()
    }

    /// The content a restore at `epoch` reads: the newest prefix below it
    /// that holds data. Every upload carries the whole database, so that
    /// prefix alone is the chain.
    pub fn read_chain(&self, cell: &str, epoch: Epoch) -> Option<Vec<WriteId>> {
        if self.mutation == Mutation::SharedPrefix {
            return self
                .prefixes
                .get(&(cell.to_string(), 0))
                .map(Prefix::content);
        }
        self.prefixes
            .range((cell.to_string(), 0)..(cell.to_string(), epoch))
            .rev()
            .find(|(_, prefix)| !prefix.entries.is_empty())
            .map(|(_, prefix)| prefix.content())
    }

    pub fn prefix(&self, cell: &str, epoch: Epoch) -> Option<&Prefix> {
        self.prefixes.get(&self.prefix_key(cell, epoch))
    }
}
