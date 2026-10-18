//! The simulated bucket.
//!
//! It provides exactly what `docs/guarantees.md` requires of a real one and
//! nothing more: conditional create and conditional overwrite on ETags,
//! read-after-write consistency, and unconditional PUTs for replica data
//! and wake entries. Every operation is applied atomically at one simulated
//! instant; the latency and loss around that instant belong to the world.

use celld_logic::drain::DrainToken;
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
        let base = match self.prefix(cell, epoch).map(Prefix::content) {
            Some(content) if !content.is_empty() => content,
            _ => self.read_chain(cell, epoch).unwrap_or_default(),
        };
        let key = self.prefix_key(cell, epoch);
        let prefix = self.prefixes.entry(key).or_default();
        for (index, write) in base.iter().enumerate() {
            prefix.entries.entry(index as u64 + 1).or_insert(*write);
        }
        for (position, write) in rows {
            prefix.entries.insert(*position, *write);
        }
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

    /// The newest prefix of `cell` that holds data: its epoch and the
    /// content a restore of it produces.
    pub fn newest_prefix(&self, cell: &str) -> Option<(Epoch, Vec<WriteId>)> {
        self.prefixes
            .range((cell.to_string(), 0)..=(cell.to_string(), Epoch::MAX))
            .rev()
            .find(|(_, prefix)| !prefix.entries.is_empty())
            .map(|((_, epoch), prefix)| (*epoch, prefix.content()))
    }

    /// Whether `row` is at its position in its epoch's prefix.
    pub fn prefix_holds(&self, row: &Row) -> bool {
        self.prefix(&row.cell, row.epoch)
            .is_some_and(|prefix| prefix.entries.get(&row.position) == Some(&row.write))
    }

    /// Whether the bucket holds `row`: in its prefix, or in some session's
    /// bundle.
    pub fn holds(&self, row: &Row) -> bool {
        self.prefix_holds(row) || self.bundled.contains(row)
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
    ) -> Vec<ProcId> {
        let key = self.prefix_key(cell, epoch);
        let prefix = self.prefixes.entry(key).or_default();
        for (index, write) in content.iter().enumerate() {
            prefix.entries.insert(index as u64 + 1, *write);
        }
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
