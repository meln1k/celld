//! The simulated bucket.
//!
//! It provides what `docs/guarantees.md` requires of a real bucket and
//! nothing more: conditional writes on ETags and read-after-write
//! consistency. Each operation applies atomically at one simulated instant.
//! Latency and loss around that instant belong to the world, not here.
//!
//! It holds four kinds of object:
//!
//! - node leases, `nodes/<node>.json`;
//! - ownership records, `cells/<cell>/own.json`;
//! - replica prefixes, `cells/<cell>/ltx/e<epoch>/`;
//! - wake entries, `wake/<minute>/<cell>/<id>`: one per alarm installation
//!   that some node must wake its cell for.

use std::collections::BTreeMap;

use celld_logic::wake::{AlarmSnapshot, PublicationId, Retirement};
use celld_logic::{
    format::BUCKET_FORMAT, CapacityPeer, CasGuard, CasOutcome, Epoch, LeaseCasOutcome,
    NodeLeaseRecord, OwnerRecord,
};

use crate::{Mutation, WriteId};

/// The load a node reports in its lease body, sampled when it writes it.
/// Peers read it to choose where an unowned cell should land.
#[derive(Clone, Debug)]
pub struct Load {
    pub resident: usize,
    pub owned: usize,
    pub sampled_ms: u64,
}

#[derive(Default)]
pub struct Store {
    /// A planted bug in the bucket itself, if any.
    mutation: Option<Mutation>,
    next_etag: u64,
    leases: BTreeMap<String, (NodeLeaseRecord, u64, Load)>,
    /// Cell → (owner node, epoch, etag).
    owners: BTreeMap<String, (Option<String>, Epoch, u64)>,
    /// One prefix per cell epoch. A cell's database is a list of writes, so
    /// a prefix holds the longest list any upload into it carried. Real LTX
    /// files are immutable and named by transaction range: a late or repeated
    /// upload adds files and never rolls the prefix back. Keeping the longest
    /// list has the same effect.
    prefixes: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// Wake entries: (cell, installation) → when it is due, wall clock ms.
    wake: BTreeMap<(String, PublicationId), i64>,
    /// Each cell's retirement certificate, `wake/retired/<cell>`: the newest
    /// installation an owner proved durable, below which every wake entry is
    /// obsolete.
    retirements: BTreeMap<String, Retirement>,
}

impl Store {
    pub fn new(mutation: Option<Mutation>) -> Self {
        Self {
            mutation,
            ..Self::default()
        }
    }

    fn etag(&mut self) -> u64 {
        self.next_etag += 1;
        self.next_etag
    }

    pub fn read_lease(&self, node: &str) -> Option<NodeLeaseRecord> {
        self.leases
            .get(node)
            .map(|(record, etag, _)| NodeLeaseRecord {
                etag: etag.to_string(),
                ..record.clone()
            })
    }

    pub fn cas_lease(
        &mut self,
        guard: &CasGuard,
        record: &NodeLeaseRecord,
        load: Load,
    ) -> LeaseCasOutcome {
        let current = self.leases.get(&record.node).map(|(_, etag, _)| *etag);
        if !guard_matches(guard, current) {
            return LeaseCasOutcome::Rejected;
        }
        let etag = self.etag();
        self.leases
            .insert(record.node.clone(), (record.clone(), etag, load));
        LeaseCasOutcome::Applied {
            etag: etag.to_string(),
        }
    }

    /// Every node's lease with its load, scanned at this instant. Production
    /// reads a shared fleet sample that can be a few intervals old; that
    /// staleness is not modeled yet. Only the fields the simulation varies
    /// are measured; the rest say every node is healthy with memory to spare.
    pub fn capacity_peers(&self) -> Vec<CapacityPeer> {
        self.leases
            .values()
            .map(|(record, _, load)| CapacityPeer {
                node: record.node.clone(),
                addr: record.addr.clone(),
                expires_ms: record.expires_ms,
                peer_protocol: record.peer_protocol,
                sampled_ms: load.sampled_ms,
                owned_cells: Some(load.owned),
                placement_weight: Some(1),
                bucket_format: Some(BUCKET_FORMAT),
                resident_cells: load.resident,
                host_websockets: 0,
                rss_bytes: 0,
                in_use_bytes: Some(0),
                pressured: false,
                memory_headroom: Some(true),
                restoring: 0,
                paced_handoff: true,
                rebalance_paused: false,
                draining: false,
            })
            .collect()
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
        match self.mutation {
            Some(Mutation::IgnoreCasGuard) => {}
            // Answered as if it applied, but nothing is written.
            Some(Mutation::PhantomClaim) => return CasOutcome::Applied,
            Some(Mutation::RejectClaims) => return CasOutcome::Rejected,
            _ if !guard_matches(guard, current) => return CasOutcome::Rejected,
            _ => {}
        }
        let epoch = match (self.mutation, self.owners.get(cell)) {
            (Some(Mutation::StaleEpoch), Some((_, current, _))) => *current,
            _ => epoch,
        };
        let etag = self.etag();
        self.owners
            .insert(cell.to_string(), (Some(node.to_string()), epoch, etag));
        CasOutcome::Applied
    }

    /// `release_owner` in `ownership_store.rs`: if `node` still owns the
    /// cell at `epoch`, publish it as unowned at the same epoch. Production
    /// reads, then writes conditionally on what it read; one atomic step
    /// gives the same outcomes.
    pub fn release_owner(&mut self, cell: &str, epoch: Epoch, node: &str) -> CasOutcome {
        let ours = self
            .owners
            .get(cell)
            .is_some_and(|(owner, e, _)| owner.as_deref() == Some(node) && *e == epoch);
        if !ours {
            return CasOutcome::Rejected;
        }
        let etag = self.etag();
        self.owners.insert(cell.to_string(), (None, epoch, etag));
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
        // The planted bug skips the previous epoch's prefix, the usual source.
        let epoch = match self.mutation {
            Some(Mutation::StaleRestore) => epoch.saturating_sub(1),
            _ => epoch,
        };
        let chain = self.chain_below(cell, epoch).map(<[WriteId]>::to_vec);
        match (self.mutation, chain) {
            (Some(Mutation::TornRestore), Some(chain)) if chain.len() >= 2 => {
                Some(chain[1..].to_vec())
            }
            (_, chain) => chain,
        }
    }

    /// The newest non-empty prefix below `epoch`, as the bucket truly holds
    /// it. The oracle reads this, so a planted bug never bends its view.
    fn chain_below(&self, cell: &str, epoch: Epoch) -> Option<&[WriteId]> {
        self.newest_below(cell, epoch).map(|(_, writes)| writes)
    }

    fn newest_below(&self, cell: &str, epoch: Epoch) -> Option<(Epoch, &[WriteId])> {
        self.prefixes
            .range((cell.to_string(), 0)..(cell.to_string(), epoch))
            .rev()
            .find(|(_, writes)| !writes.is_empty())
            .map(|((_, epoch), writes)| (*epoch, writes.as_slice()))
    }

    /// How much of epoch `epoch`'s database the bucket holds: its own prefix
    /// once anything was uploaded into it, or else the chain it was
    /// restored from, which lives below it.
    pub fn durable_len(&self, cell: &str, epoch: Epoch) -> usize {
        match self.prefix(cell, epoch) {
            [] => self.chain_below(cell, epoch).map_or(0, <[WriteId]>::len),
            prefix => prefix.len(),
        }
    }

    /// The cell's newest durable database, and the epoch that opened it.
    pub fn newest(&self, cell: &str) -> Option<(Epoch, &[WriteId])> {
        self.newest_below(cell, Epoch::MAX)
    }

    /// `WakeFlusher::publish`: write the wake entry of an armed alarm.
    pub fn publish_wake(&mut self, cell: &str, alarm: AlarmSnapshot) {
        if let (Some(at_ms), Some(source)) = (alarm.at_ms(), alarm.source()) {
            if self.mutation != Some(Mutation::LostWakeEntry) {
                self.wake.insert((cell.to_string(), source.id), at_ms);
            }
        }
    }

    /// `publish_retirement` and the collector: raise the cell's certificate
    /// to `retirement` unless it is already newer, then delete every entry
    /// the certificate retires.
    pub fn retire_wake(&mut self, cell: &str, retirement: Retirement) {
        let certificate = self
            .retirements
            .entry(cell.to_string())
            .or_insert(retirement);
        if certificate.id < retirement.id {
            *certificate = retirement;
        }
        let certificate = *certificate;
        self.wake
            .retain(|(c, id), _| c != cell || !certificate.retires(*id));
    }

    /// Whether a scan will wake `cell` by `at_ms`: some wake entry for it, of
    /// any installation, is due by then. The woken owner reads the alarm its
    /// database really holds. Only a certificate deletes an entry, and only
    /// once a newer installation is published and durable.
    pub fn wake_covers(&self, cell: &str, at_ms: i64) -> bool {
        self.wake
            .iter()
            .any(|((c, _), due)| c == cell && *due <= at_ms)
    }

    /// `due_scan`: every wake entry due by `now_ms`, as (cell, due).
    pub fn wake_due(&self, now_ms: i64) -> Vec<(String, i64)> {
        self.wake
            .iter()
            .filter(|(_, at_ms)| **at_ms <= now_ms)
            .map(|((cell, _), at_ms)| (cell.clone(), *at_ms))
            .collect()
    }

    /// Who the bucket's owner record names for `cell`, and at which epoch.
    pub fn owner(&self, cell: &str) -> Option<(Option<&str>, Epoch)> {
        self.owners
            .get(cell)
            .map(|(node, epoch, _)| (node.as_deref(), *epoch))
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
