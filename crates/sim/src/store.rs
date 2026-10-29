//! The simulated bucket.
//!
//! It provides exactly what `docs/guarantees.md` requires of a real one and
//! nothing more: conditional create and conditional overwrite on ETags,
//! read-after-write consistency, and unconditional PUTs for replica data
//! and wake entries. Every operation is applied atomically at one simulated
//! instant; the latency and loss around that instant belong to the world.

use celld_logic::drain::DrainToken;
use celld_logic::epoch_gc::{self, ListedEpoch};
use celld_logic::log_tier::LogRecord;
use celld_logic::wake::{parse_entry_key, Retirement};
use celld_logic::{CasGuard, CasOutcome, Epoch, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord};
use std::collections::{BTreeMap, BTreeSet};

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
    /// two writers in one epoch. Epoch GC deletes the objects, not this.
    pub writers: Vec<ProcId>,
    /// The bucket's time of the newest upload, which the GC grace reads.
    pub newest_ms: u64,
    /// What epoch GC deleted. Only `prefix_holds` reads it: a write proven
    /// durable here stays proven once a later base carries it.
    gone: BTreeMap<u64, WriteId>,
    /// A paged epoch continues the chain below it from this position, and
    /// holds only the positions above it. Zero opens at TXID 1.
    pub cut: u64,
    /// The paged epoch's zero-page marker, uploaded before the cell serves.
    marker: bool,
}

impl Prefix {
    /// Whether a listing finds the prefix: it holds an object.
    fn listed(&self) -> bool {
        self.marker || !self.entries.is_empty()
    }

    /// The span's first TXID.
    fn lo(&self) -> u64 {
        if self.cut > 0 {
            self.cut + 1
        } else {
            self.entries.keys().next().copied().unwrap_or(1)
        }
    }

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

/// The `log` object folded into `nodes/<node>.json`
/// (`ownership_store::NodeLogWire`): the session's node-log record, and
/// whether the session may have acknowledged a write through its fleet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogWire {
    pub record: LogRecord,
    pub active: bool,
}

/// A node-log session: one process's lease generation on its node, and
/// the `log/<node>/<generation>/` objects that process writes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Session {
    pub node: String,
    pub proc: ProcId,
}

impl Session {
    /// The session whose process holds `lease`.
    pub fn of_lease(lease: &NodeLeaseRecord) -> Self {
        let proc = lease
            .generation
            .strip_prefix('g')
            .and_then(|proc| proc.parse().ok())
            .expect("generations name their process");
        Session {
            node: lease.node.clone(),
            proc,
        }
    }

    /// The lease generation this session's process writes.
    pub fn generation(&self) -> String {
        format!("g{}", self.proc)
    }

    /// Whether `lease` is still this session's.
    pub fn owns(&self, lease: &NodeLeaseRecord) -> bool {
        lease.node == self.node && lease.generation == self.generation()
    }
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/g{}", self.node, self.proc)
    }
}

/// One write as the node log carries it: a cell's database content at one
/// position of one epoch.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub cell: String,
    pub epoch: Epoch,
    pub position: u64,
    pub write: WriteId,
}

impl Row {
    /// Sorts before every row of `cell`.
    pub fn first(cell: &str) -> Self {
        Row {
            cell: cell.to_string(),
            epoch: 0,
            position: 0,
            write: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LeaseEntry {
    pub record: NodeLeaseRecord,
    pub log: Option<LogWire>,
    pub etag: u64,
    pub load: LeaseLoad,
    pub sampled_ms: u64,
}

/// The advisory load a process writes into its node lease, as its peers
/// read it to choose where cells go.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaseLoad {
    pub resident: usize,
    pub owned: usize,
    /// Cold routes that have not finished.
    pub restoring: u64,
    /// The process accepts a signed handoff.
    pub paced_handoff: bool,
    /// The process is handing its cells away.
    pub draining: bool,
    /// The memory model's sample, when the run has one: the process is
    /// shedding, its last sample cleared every resume line, and its bytes.
    pub memory: Option<(bool, bool, u64)>,
}

/// One applied ownership install, in store order.
#[derive(Clone, Debug)]
pub struct Claim {
    pub cell: String,
    pub epoch: Epoch,
    pub node: String,
    pub proc: ProcId,
}

pub struct Store {
    mutation: Mutation,
    next_etag: u64,
    owners: BTreeMap<String, OwnerEntry>,
    leases: BTreeMap<String, LeaseEntry>,
    prefixes: BTreeMap<(String, Epoch), Prefix>,
    /// `cells/<cell>/ltx/retired.json`: epoch GC's retired mark.
    retired: BTreeMap<String, Epoch>,
    /// The bucket's clock, set before each operation applies.
    pub now_ms: u64,
    pub claims: Vec<Claim>,
    /// `log/<node>/<generation>/bundle/…`: every row each session's bundle
    /// flushes carried, in flush order.
    bundles: BTreeMap<Session, Vec<Row>>,
    /// Every row any bundle carries.
    bundled: BTreeSet<Row>,
    /// `fleet/drain-token.json`, and its ETag.
    drain_token: Option<(DrainToken, u64)>,
    /// `wake/entries/<minute>/<cell>/<identity>`, by full key.
    wake_entries: BTreeSet<String>,
    /// `wake/retired/<cell>.json`: the retirement frontier and its ETag.
    retirements: BTreeMap<String, (Retirement, u64)>,
    /// `wake/waker.json`: the advisory waker role.
    waker: Option<(String, i64, u64)>,
}

impl Store {
    pub fn new(mutation: Mutation) -> Self {
        Self {
            mutation,
            next_etag: 1,
            owners: BTreeMap::new(),
            leases: BTreeMap::new(),
            prefixes: BTreeMap::new(),
            retired: BTreeMap::new(),
            now_ms: 0,
            claims: Vec::new(),
            bundles: BTreeMap::new(),
            bundled: BTreeSet::new(),
            drain_token: None,
            wake_entries: BTreeSet::new(),
            retirements: BTreeMap::new(),
            waker: None,
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
            node: node.to_string(),
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
        log: Option<&LogWire>,
        load: LeaseLoad,
        sampled_ms: u64,
    ) -> LeaseCasOutcome {
        let current = self.leases.get(&record.node).map(|entry| entry.etag);
        if !self.guard_matches(guard, current) {
            return LeaseCasOutcome::Rejected;
        }
        let etag = self.etag();
        let mut stored = record.clone();
        stored.etag = String::new();
        stored.log_state = log.map(|log| log.record.state);
        self.leases.insert(
            record.node.clone(),
            LeaseEntry {
                record: stored,
                log: log.cloned(),
                etag,
                load,
                sampled_ms,
            },
        );
        LeaseCasOutcome::Applied {
            etag: etag.to_string(),
        }
    }

    pub fn read_drain_token(&self) -> Option<(DrainToken, String)> {
        self.drain_token
            .as_ref()
            .map(|(token, etag)| (token.clone(), etag.to_string()))
    }

    /// Write the drain token under `guard`. Returns the new ETag, or `None`
    /// when the guard missed.
    pub fn cas_drain_token(&mut self, guard: &CasGuard, token: DrainToken) -> Option<String> {
        let current = self.drain_token.as_ref().map(|(_, etag)| *etag);
        if !self.guard_matches(guard, current) {
            return None;
        }
        let etag = self.etag();
        self.drain_token = Some((token, etag));
        Some(etag.to_string())
    }

    pub fn leases(&self) -> impl Iterator<Item = &LeaseEntry> {
        self.leases.values()
    }

    /// A node's lease with its folded log, as a recovery reads it.
    pub fn read_lease_log(&self, node: &str) -> Option<(NodeLeaseRecord, Option<LogWire>)> {
        let entry = self.leases.get(node)?;
        Some((self.read_lease(node)?, entry.log.clone()))
    }

    /// `write_dead_record`: replace a dead session's folded log under the
    /// lease's ETag, carrying every authority field through, expiry too.
    /// Returns the new ETag, or `None` when the guard missed.
    pub fn cas_lease_log(&mut self, node: &str, etag: &str, log: LogWire) -> Option<String> {
        let current = self.leases.get(node).map(|entry| entry.etag);
        if !self.guard_matches(&CasGuard::Match(etag.to_string()), current) {
            return None;
        }
        let etag = self.etag();
        let entry = self.leases.get_mut(node)?;
        entry.record.log_state = Some(log.record.state);
        entry.log = Some(log);
        entry.etag = etag;
        Some(etag.to_string())
    }

    pub fn put_bundle(&mut self, session: &Session, rows: &[Row]) {
        self.bundled.extend(rows.iter().cloned());
        self.bundles
            .entry(session.clone())
            .or_default()
            .extend_from_slice(rows);
    }

    pub fn bundle_rows(&self, session: &Session) -> Vec<Row> {
        self.bundles.get(session).cloned().unwrap_or_default()
    }

    /// Recovery's merged L0 for one cell epoch: the gathered rows on top of
    /// what the prefix, or failing that the chain below it, already holds.
    /// `writer` is the dead session's process, whose epoch this still is.
    /// Returns every process that has now written the prefix.
    pub fn put_recovered(
        &mut self,
        cell: &str,
        epoch: Epoch,
        rows: &BTreeMap<u64, WriteId>,
        writer: ProcId,
    ) -> Vec<ProcId> {
        let paged = self
            .prefix(cell, epoch)
            .is_some_and(|prefix| prefix.cut > 0);
        let base = match self.prefix(cell, epoch).map(Prefix::content) {
            _ if paged => Vec::new(),
            Some(content) if !content.is_empty() => content,
            _ => self
                .read_chain(cell, epoch)
                .ok()
                .flatten()
                .unwrap_or_default(),
        };
        let key = self.prefix_key(cell, epoch);
        let prefix = self.prefixes.entry(key).or_default();
        for (index, write) in base.iter().enumerate() {
            prefix.entries.entry(index as u64 + 1).or_insert(*write);
        }
        for (position, write) in rows {
            prefix.entries.insert(*position, *write);
        }
        prefix.newest_ms = self.now_ms;
        if !prefix.writers.contains(&writer) {
            prefix.writers.push(writer);
        }
        prefix.writers.clone()
    }

    /// Every row any bundle carries for `cell`, in epoch and position order.
    pub fn bundled_rows<'a>(&'a self, cell: &'a str) -> impl Iterator<Item = &'a Row> + 'a {
        self.bundled
            .range(Row::first(cell)..)
            .take_while(move |row| row.cell == cell)
    }

    /// The newest epoch of `cell`'s restore chain, and the content a
    /// restore above it produces.
    pub fn newest_prefix(&self, cell: &str) -> Option<(Epoch, Vec<WriteId>)> {
        let spans = self.spans(cell, Epoch::MAX).ok()?;
        Some((spans.first()?.0, self.assemble(cell, &spans)))
    }

    /// `EpochChain::build` over the epochs below `below`, newest first: walk
    /// down from the newest listed epoch, clip each lower epoch to below the
    /// span above it, link it only when it ends exactly at that cut, and
    /// stop at the first span that opens at TXID 1. `Err` when objects are
    /// listed but no span opens at 1 (`TxNotAvailable`); `Ok` and empty when
    /// nothing is listed.
    fn spans(&self, cell: &str, below: Epoch) -> Result<Vec<(Epoch, u64, u64)>, ()> {
        let mut spans: Vec<(Epoch, u64, u64)> = Vec::new();
        let range = (cell.to_string(), 0)..(cell.to_string(), below);
        for ((_, epoch), prefix) in self.prefixes.range(range).rev() {
            if !prefix.listed() {
                continue;
            }
            let limit = spans.last().map(|(_, lo, _)| *lo);
            let lo = prefix.lo();
            if !prefix.marker && limit.is_some_and(|limit| lo >= limit) {
                continue;
            }
            let mut ends = lo - 1;
            while prefix.entries.contains_key(&(ends + 1))
                && limit.is_none_or(|limit| ends + 1 < limit)
            {
                ends += 1;
            }
            // Linking on anything but the exact cut takes a fenced owner's
            // late object for the base and loses what lies between it and
            // the cut (`BreakLinkByOrder`).
            if limit.is_some_and(|limit| ends + 1 != limit) {
                continue;
            }
            spans.push((*epoch, lo, ends));
            if lo == 1 {
                return Ok(spans);
            }
        }
        if spans.is_empty() {
            Ok(spans)
        } else {
            Err(())
        }
    }

    /// The database the spans restore: each span's positions, base first.
    fn assemble(&self, cell: &str, spans: &[(Epoch, u64, u64)]) -> Vec<WriteId> {
        spans
            .iter()
            .rev()
            .flat_map(|(epoch, lo, ends)| {
                let entries = &self.prefixes[&(cell.to_string(), *epoch)].entries;
                (*lo..=*ends).map(|position| entries[&position])
            })
            .collect()
    }

    /// Whether `row` is at its position in its epoch's prefix, or was until
    /// epoch GC deleted the prefix.
    pub fn prefix_holds(&self, row: &Row) -> bool {
        self.prefix(&row.cell, row.epoch).is_some_and(|prefix| {
            [&prefix.entries, &prefix.gone]
                .iter()
                .any(|entries| entries.get(&row.position) == Some(&row.write))
        })
    }

    /// Whether the per-cell layout covers `row`: its prefix holds it, or
    /// epoch GC retired its epoch (`covered_txid`).
    pub fn covers(&self, row: &Row) -> bool {
        self.prefix_holds(row) || epoch_gc::retired(row.epoch, self.retired.get(&row.cell).copied())
    }

    /// Whether the bucket holds `row`: the per-cell layout covers it, or
    /// some session's bundle carries it.
    pub fn holds(&self, row: &Row) -> bool {
        self.covers(row) || self.bundled.contains(row)
    }

    /// `scan_epochs`: the restore chain's epochs, oldest first, every epoch
    /// below its base with objects, and the retired mark. No chain when it
    /// is unrestorable.
    pub fn scan_epochs(&self, cell: &str) -> (Option<Vec<Epoch>>, Vec<ListedEpoch>, Option<Epoch>) {
        let chain: Option<Vec<Epoch>> = self
            .spans(cell, Epoch::MAX)
            .ok()
            .filter(|spans| !spans.is_empty())
            .map(|spans| spans.iter().rev().map(|(epoch, _, _)| *epoch).collect());
        let base = chain.as_ref().and_then(|chain| chain.first().copied());
        let held: Vec<(Epoch, u64)> = self
            .prefixes
            .range((cell.to_string(), 0)..=(cell.to_string(), Epoch::MAX))
            .filter(|(_, prefix)| prefix.listed())
            .map(|((_, epoch), prefix)| (*epoch, prefix.newest_ms))
            .collect();
        let listed = held
            .iter()
            .filter(|(epoch, _)| Some(*epoch) < base)
            .map(|(epoch, newest_ms)| ListedEpoch {
                epoch: *epoch,
                newest_ms: *newest_ms,
            })
            .collect();
        (chain, listed, self.retired.get(cell).copied())
    }

    /// An unconditional PUT of the retired mark.
    pub fn put_retired(&mut self, cell: &str, mark: Epoch) {
        self.retired.insert(cell.to_string(), mark);
    }

    /// `delete_all` on each epoch prefix.
    pub fn delete_epochs(&mut self, cell: &str, epochs: &[Epoch]) {
        for epoch in epochs {
            if let Some(prefix) = self.prefixes.get_mut(&(cell.to_string(), *epoch)) {
                let deleted = std::mem::take(&mut prefix.entries);
                prefix.gone.extend(deleted);
                prefix.marker = false;
            }
        }
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

    /// Upload the database content of `epoch` by position, from above the
    /// paged epoch's cut. Returns every process that has now written this
    /// prefix.
    pub fn put_ltx(
        &mut self,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
        proc: ProcId,
    ) -> Vec<ProcId> {
        let key = self.prefix_key(cell, epoch);
        let prefix = self.prefixes.entry(key).or_default();
        for (index, write) in content.iter().enumerate().skip(prefix.cut as usize) {
            prefix.entries.insert(index as u64 + 1, *write);
        }
        prefix.newest_ms = self.now_ms;
        if !prefix.writers.contains(&proc) {
            prefix.writers.push(proc);
        }
        prefix.writers.clone()
    }

    /// A paged activation's zero-page marker: the epoch continues the chain
    /// below it from `cut`. Returns every process that has now written this
    /// prefix.
    pub fn put_marker(&mut self, cell: &str, epoch: Epoch, cut: u64, proc: ProcId) -> Vec<ProcId> {
        let key = self.prefix_key(cell, epoch);
        let prefix = self.prefixes.entry(key).or_default();
        prefix.cut = cut;
        prefix.marker = true;
        prefix.newest_ms = self.now_ms;
        if !prefix.writers.contains(&proc) {
            prefix.writers.push(proc);
        }
        prefix.writers.clone()
    }

    /// Every cell whose owner record names `node`.
    pub fn owned_by(&self, node: &str) -> Vec<String> {
        self.owners
            .iter()
            .filter(|(_, entry)| entry.node.as_deref() == Some(node))
            .map(|(cell, _)| cell.clone())
            .collect()
    }

    /// The content a restore at `epoch` reads, through the chain of the
    /// epochs below it: `None` when none is listed, `Err` when the chain has
    /// no base.
    pub fn read_chain(&self, cell: &str, epoch: Epoch) -> Result<Option<Vec<WriteId>>, ()> {
        if self.mutation == Mutation::SharedPrefix {
            return Ok(self
                .prefixes
                .get(&(cell.to_string(), 0))
                .map(Prefix::content));
        }
        if self.mutation == Mutation::NewestBase {
            return Ok(self
                .prefixes
                .range((cell.to_string(), 0)..(cell.to_string(), epoch))
                .rev()
                .find(|(_, prefix)| prefix.cut == 0 && !prefix.entries.is_empty())
                .map(|(_, prefix)| prefix.content()));
        }
        let spans = self.spans(cell, epoch)?;
        Ok((!spans.is_empty()).then(|| self.assemble(cell, &spans)))
    }

    pub fn prefix(&self, cell: &str, epoch: Epoch) -> Option<&Prefix> {
        self.prefixes.get(&self.prefix_key(cell, epoch))
    }

    pub fn put_wake_entry(&mut self, key: &str) {
        self.wake_entries.insert(key.to_string());
    }

    pub fn delete_wake_entry(&mut self, key: &str) {
        self.wake_entries.remove(key);
    }

    pub fn wake_entries(&self) -> Vec<String> {
        self.wake_entries.iter().cloned().collect()
    }

    pub fn read_retirement(&self, cell: &str) -> Option<(Retirement, u64)> {
        self.retirements.get(cell).copied()
    }

    /// `put_cas` on `wake/retired/<cell>.json`: `None` creates only if
    /// absent, `Some(etag)` overwrites only that version.
    pub fn cas_retirement(
        &mut self,
        cell: &str,
        guard: Option<u64>,
        retirement: Retirement,
    ) -> bool {
        let current = self.retirements.get(cell).map(|(_, etag)| *etag);
        let guard = guard.map_or(CasGuard::Absent, |etag| CasGuard::Match(etag.to_string()));
        if !self.guard_matches(&guard, current) {
            return false;
        }
        let etag = self.etag();
        self.retirements
            .insert(cell.to_string(), (retirement, etag));
        true
    }

    /// `try_hold_waker`'s read and conditional write, applied as one step:
    /// the role is advisory, so nothing depends on the two halves racing.
    pub fn claim_waker(&mut self, node: &str, now_ms: i64, ttl_ms: i64) -> bool {
        let may = match &self.waker {
            None => true,
            Some((holder, expires_ms, _)) => {
                celld_logic::wake::waker_may_claim(holder == node, *expires_ms, now_ms)
            }
        };
        if may {
            let etag = self.etag();
            self.waker = Some((node.to_string(), now_ms + ttl_ms, etag));
        }
        may
    }

    /// Whether the collector can still find this cell: some wake entry for
    /// it that its retirement record does not retire.
    pub fn discoverable(&self, cell: &str) -> bool {
        let retirement = self.retirements.get(cell).map(|(record, _)| *record);
        self.wake_entries.iter().any(|key| {
            parse_entry_key(key).is_some_and(|entry| {
                entry.cell == cell && !retirement.is_some_and(|record| record.retires(entry.id))
            })
        })
    }
}
