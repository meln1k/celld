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

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::epoch_gc::ListedEpoch;
use celld_logic::log_tier::LogState;
use celld_logic::wake::{waker_may_claim, AlarmSnapshot, PublicationId, Retirement};
use celld_logic::{
    format::BUCKET_FORMAT, CapacityPeer, CasGuard, CasOutcome, Epoch, LeaseCasOutcome,
    NodeLeaseRecord, OwnerRecord,
};

use crate::{Mutation, WriteId};

/// One row of a bundle: a cell epoch's database as the flush carried it.
pub type Row = (String, Epoch, Vec<WriteId>);

/// The load a node reports in its lease body, sampled when it writes it.
/// Peers read it to choose where an unowned cell should land.
#[derive(Clone, Debug)]
pub struct Load {
    pub resident: usize,
    pub owned: usize,
    pub sampled_ms: u64,
    /// The memory the node last measured, whether it is shedding, and
    /// whether that measurement left headroom below every resume line.
    pub memory: u64,
    pub pressured: bool,
    pub memory_headroom: bool,
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
    /// How long a chain each epoch was restored from. An older owner can
    /// still extend the prefix below after the restore read it, and those
    /// writes are not in this epoch's database.
    bases: BTreeMap<(String, Epoch), usize>,
    /// When the newest upload into each prefix landed, true wall clock ms:
    /// what epoch GC's grace reads.
    uploaded_ms: BTreeMap<(String, Epoch), u64>,
    /// The longest database of each cell epoch the bucket ever held, in
    /// its prefix or in a bundle, deleted since or not: the oracle's view
    /// (`prefix`). Epoch GC deletes a prefix whose rows production's retired
    /// mark keeps counting as covered.
    held: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// Node bundles, `log/<session>/bundle/<seq>`: the rows one flush of a
    /// session's bundle loop carried, each a cell epoch's database. No
    /// restore reads them; a fold or a recovery copies them into prefixes.
    bundles: BTreeMap<(String, u64), Vec<Row>>,
    /// Wake entries: (cell, installation) → when it is due, wall clock ms.
    wake: BTreeMap<(String, PublicationId), i64>,
    /// Each cell's retirement certificate, `wake/retired/<cell>`: the newest
    /// installation an owner proved durable, below which every wake entry is
    /// obsolete.
    retirements: BTreeMap<String, Retirement>,
    /// The waker role, `wake/waker.json`: its holder and when it expires.
    waker: Option<(String, i64)>,
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

    /// Recovery, or a graceful shutdown, seals the log folded into `node`'s
    /// lease body, if the lease still names session `generation`. A write
    /// like any other: the etag moves, so the session's own next renewal
    /// fails. Returns whether it sealed.
    pub fn seal_log(&mut self, node: &str, generation: &str) -> bool {
        let etag = self.etag();
        let Some((record, current, _)) = self.leases.get_mut(node) else {
            return false;
        };
        if record.generation != generation {
            return false;
        }
        record.log_state = Some(LogState::Sealed);
        *current = etag;
        true
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
                rss_bytes: load.memory,
                in_use_bytes: Some(load.memory),
                pressured: load.pressured,
                memory_headroom: Some(load.memory_headroom),
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
    /// A late upload into a deleted prefix recreates it with what it carries.
    pub fn put(&mut self, cell: &str, epoch: Epoch, content: &[WriteId], at_ms: u64) {
        let key = (cell.to_string(), epoch);
        self.uploaded_ms.insert(key.clone(), at_ms);
        self.hold(cell, epoch, content);
        let prefix = self.prefixes.entry(key).or_default();
        if content.len() > prefix.len() {
            *prefix = content.to_vec();
        }
    }

    fn hold(&mut self, cell: &str, epoch: Epoch, content: &[WriteId]) {
        let held = self.held.entry((cell.to_string(), epoch)).or_default();
        if content.len() > held.len() {
            *held = content.to_vec();
        }
    }

    /// `put_bundle`: one flush of `session`'s bundle loop.
    pub fn put_bundle(&mut self, session: &str, rows: Vec<Row>) {
        for (cell, epoch, content) in &rows {
            self.hold(cell, *epoch, content);
        }
        let range = (session.to_string(), 0)..=(session.to_string(), u64::MAX);
        let seq = self
            .bundles
            .range(range)
            .next_back()
            .map_or(0, |(k, _)| k.1)
            + 1;
        self.bundles.insert((session.to_string(), seq), rows);
    }

    /// The longest row of each cell epoch in `session`'s bundles: of `cell`
    /// alone, when given (`uncovered_bundle_rows_for`).
    pub fn bundle_rows(
        &self,
        session: &str,
        cell: Option<&str>,
    ) -> BTreeMap<(String, Epoch), Vec<WriteId>> {
        let mut rows: BTreeMap<(String, Epoch), Vec<WriteId>> = BTreeMap::new();
        let range = (session.to_string(), 0)..=(session.to_string(), u64::MAX);
        for (_, bundle) in self.bundles.range(range) {
            for (c, epoch, content) in bundle {
                if cell.is_some_and(|cell| cell != c) {
                    continue;
                }
                let longest = rows.entry((c.clone(), *epoch)).or_default();
                if content.len() > longest.len() {
                    *longest = content.clone();
                }
            }
        }
        rows
    }

    /// Whether a prefix at `epoch` or above holds objects.
    pub fn used_since(&self, cell: &str, epoch: Epoch) -> bool {
        (self
            .prefixes
            .range((cell.to_string(), epoch)..=(cell.to_string(), Epoch::MAX)))
        .any(|(_, writes)| !writes.is_empty())
    }

    /// `scan_epochs`: the restore chain's epochs, and every epoch that holds
    /// objects, with its newest upload's time. Every upload carries the
    /// whole database, so the chain is the newest prefix alone.
    pub fn list_epochs(&self, cell: &str) -> (Vec<Epoch>, Vec<ListedEpoch>) {
        let Some((base, _)) = self.newest(cell) else {
            return (Vec::new(), Vec::new());
        };
        let cell_range = (cell.to_string(), 0)..=(cell.to_string(), Epoch::MAX);
        let listed = (self.prefixes.range(cell_range))
            .filter(|(_, writes)| !writes.is_empty())
            .map(|(key, _)| ListedEpoch {
                epoch: key.1,
                newest_ms: self.uploaded_ms[key],
            })
            .collect();
        (vec![base], listed)
    }

    /// Delete every object of one epoch prefix.
    pub fn delete_epoch(&mut self, cell: &str, epoch: Epoch) {
        self.prefixes.remove(&(cell.to_string(), epoch));
    }

    /// What a restore at `epoch` reads: the newest prefix below it that
    /// holds data. Every upload carries the whole database, so that one
    /// prefix is the whole chain.
    pub fn read_chain(&mut self, cell: &str, epoch: Epoch) -> Option<Vec<WriteId>> {
        let chain = self.chain(cell, epoch);
        let len = chain.as_ref().map_or(0, Vec::len);
        let base = self.bases.entry((cell.to_string(), epoch)).or_default();
        *base = (*base).max(len);
        chain
    }

    fn chain(&self, cell: &str, epoch: Epoch) -> Option<Vec<WriteId>> {
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

    /// An activation opened `epoch` from an eviction snapshot of `len`
    /// writes, all in the bucket below it: the chain it restored from.
    pub fn reused(&mut self, cell: &str, epoch: Epoch, len: usize) {
        let base = self.bases.entry((cell.to_string(), epoch)).or_default();
        *base = (*base).max(len);
    }

    /// How much of epoch `epoch`'s database the bucket holds: what its own
    /// prefix or a bundle held once anything was uploaded for it, or else
    /// the chain it was restored from, as long as the restore read it.
    pub fn durable_len(&self, cell: &str, epoch: Epoch) -> usize {
        self.covered(self.prefix(cell, epoch).len(), cell, epoch)
    }

    /// How much of the epoch's database the per-cell layout holds, which is
    /// all a restore reads (`percell_coverage`): bundles do not count.
    pub fn percell_len(&self, cell: &str, epoch: Epoch) -> usize {
        let live = self
            .prefixes
            .get(&(cell.to_string(), epoch))
            .map_or(0, Vec::len);
        self.covered(live, cell, epoch)
    }

    fn covered(&self, len: usize, cell: &str, epoch: Epoch) -> usize {
        match len {
            0 => (self.bases.get(&(cell.to_string(), epoch)).copied()).unwrap_or(0),
            len => len,
        }
    }

    /// The cell's newest database in the per-cell layout, and the epoch
    /// that opened it.
    pub fn newest(&self, cell: &str) -> Option<(Epoch, &[WriteId])> {
        self.newest_below(cell, Epoch::MAX)
    }

    /// The cell's newest database the bucket ever held, in a prefix or a
    /// bundle: the oracle's view of what a fold or a recovery restores.
    pub fn newest_held(&self, cell: &str) -> Option<(Epoch, &[WriteId])> {
        (self
            .held
            .range((cell.to_string(), 0)..=(cell.to_string(), Epoch::MAX)))
        .rev()
        .find(|(_, writes)| !writes.is_empty())
        .map(|((_, epoch), writes)| (*epoch, writes.as_slice()))
    }

    /// `WakeFlusher::publish`: write the wake entry of an armed alarm.
    pub fn publish_wake(&mut self, cell: &str, alarm: AlarmSnapshot) {
        if let (Some(at_ms), Some(source)) = (alarm.at_ms(), alarm.source()) {
            if self.mutation != Some(Mutation::LostWakeEntry) {
                self.wake.insert((cell.to_string(), source.id), at_ms);
            }
        }
    }

    /// `publish_retirement`: raise the cell's certificate to `retirement`
    /// unless it is already newer. The entries it retires stay until a
    /// delete removes them.
    pub fn retire_wake(&mut self, cell: &str, retirement: Retirement) {
        let certificate = self
            .retirements
            .entry(cell.to_string())
            .or_insert(retirement);
        if certificate.id < retirement.id {
            *certificate = retirement;
        }
    }

    /// The cell's retirement certificate, `wake/retired/<cell>.json`.
    pub fn retirement(&self, cell: &str) -> Option<Retirement> {
        self.retirements.get(cell).copied()
    }

    /// Delete every wake entry of `cell` its certificate retires.
    pub fn delete_retired(&mut self, cell: &str) {
        if let Some(certificate) = self.retirement(cell) {
            self.wake
                .retain(|(c, id), _| c != cell || !certificate.retires(*id));
        }
    }

    /// Delete one wake entry.
    pub fn delete_wake(&mut self, cell: &str, id: PublicationId) {
        self.wake.remove(&(cell.to_string(), id));
    }

    /// Every wake entry, by cell: its installation and the minute it is
    /// filed under.
    pub fn wake_entries(&self) -> BTreeMap<String, Vec<(PublicationId, i64)>> {
        let mut cells: BTreeMap<String, Vec<(PublicationId, i64)>> = BTreeMap::new();
        for ((cell, id), at_ms) in &self.wake {
            let minute_ms = at_ms.div_euclid(60_000) * 60_000;
            cells
                .entry(cell.clone())
                .or_default()
                .push((*id, minute_ms));
        }
        cells
    }

    /// `try_hold_waker`: take or renew the waker role, `wake/waker.json`,
    /// unless another node holds it unexpired.
    pub fn try_hold_waker(&mut self, node: &str, now_ms: i64, ttl_ms: i64) -> bool {
        if let Some((holder, expires_ms)) = &self.waker {
            if !waker_may_claim(holder == node, *expires_ms, now_ms) {
                return false;
            }
        }
        self.waker = Some((node.to_string(), now_ms + ttl_ms));
        true
    }

    /// The nodes whose lease has not expired at `now_ms` (`live_nodes`).
    pub fn live_nodes(&self, now_ms: u64) -> BTreeSet<String> {
        (self.leases.iter())
            .filter(|(_, (record, _, _))| record.expires_ms > now_ms)
            .map(|(node, _)| node.clone())
            .collect()
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
    /// Entries are filed by the minute they fall due in, so one is listed
    /// from the start of that minute, under it. Each cell is listed once, at
    /// its earliest minute.
    pub fn wake_due(&self, now_ms: i64) -> Vec<(String, i64)> {
        let mut due: Vec<(String, i64)> = self
            .wake
            .iter()
            .map(|((cell, _), at_ms)| (cell.clone(), at_ms.div_euclid(60_000) * 60_000))
            .filter(|(_, minute_ms)| *minute_ms <= now_ms)
            .collect();
        due.sort();
        due.dedup_by(|a, b| a.0 == b.0);
        due
    }

    /// Who the bucket's owner record names for `cell`, and at which epoch.
    pub fn owner(&self, cell: &str) -> Option<(Option<&str>, Epoch)> {
        self.owners
            .get(cell)
            .map(|(node, epoch, _)| (node.as_deref(), *epoch))
    }

    /// The longest database of the cell epoch the bucket ever held, in its
    /// prefix or a bundle, deleted or not: the oracle's view of what the
    /// bucket proved durable.
    pub fn prefix(&self, cell: &str, epoch: Epoch) -> &[WriteId] {
        (self.held.get(&(cell.to_string(), epoch))).map_or(&[], Vec::as_slice)
    }
}

fn guard_matches(guard: &CasGuard, current: Option<u64>) -> bool {
    match (guard, current) {
        (CasGuard::Absent, None) => true,
        (CasGuard::Match(etag), Some(current)) => *etag == current.to_string(),
        _ => false,
    }
}
