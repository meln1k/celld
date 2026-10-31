//! The simulated bucket.
//!
//! It provides what `docs/guarantees.md` requires of a real bucket and
//! nothing more: conditional writes on ETags and read-after-write
//! consistency. Each operation applies atomically at one simulated instant.
//! Latency and loss around that instant belong to the world, not here.
//!
//! For now it holds only node leases (`nodes/<node>.json`).

use std::collections::BTreeMap;

use celld_logic::{CasGuard, LeaseCasOutcome, NodeLeaseRecord};

#[derive(Default)]
pub struct Store {
    next_etag: u64,
    leases: BTreeMap<String, (NodeLeaseRecord, u64)>,
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
}

fn guard_matches(guard: &CasGuard, current: Option<u64>) -> bool {
    match (guard, current) {
        (CasGuard::Absent, None) => true,
        (CasGuard::Match(etag), Some(current)) => *etag == current.to_string(),
        _ => false,
    }
}
