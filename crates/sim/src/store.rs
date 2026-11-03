//! The simulated bucket.
//!
//! It provides what `docs/guarantees.md` requires of a real bucket and
//! nothing more: conditional writes on ETags and read-after-write
//! consistency. Each operation applies atomically at one simulated instant.
//! Latency and loss around that instant belong to the world, not here.
//!
//! It holds:
//!
//! - node leases, `nodes/<node>.json`, which carry each node's log state;
//! - ownership records, `cells/<cell>/own.json`;
//! - replica prefixes, `cells/<cell>/ltx/e<epoch>/`, and the marker a paged
//!   epoch continues the chain below it from;
//! - node bundles, `log/<session>/bundle/<seq>`;
//! - wake entries, `wake/<minute>/<cell>/<id>`: one per alarm installation
//!   that some node must wake its cell for, each cell's retirement
//!   certificate, `wake/retired/<cell>.json`, and the waker role,
//!   `wake/waker.json`;
//! - the fleet drain token, `drain/token.json`.
//!
//! What the bucket ever held, deleted since or not, is the oracle's
//! (`Checker::held`), not the bucket's.

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::drain::DrainToken;
use celld_logic::epoch_chain::{Step, Walk, Wants};
use celld_logic::epoch_gc::ListedEpoch;
use celld_logic::log_tier::{self, LogRecord, LogState};
use celld_logic::wake::{waker_may_claim, AlarmSnapshot, PublicationId, Retirement};
use celld_logic::{
    format::BUCKET_FORMAT, CapacityPeer, CasGuard, CasOutcome, Epoch, LeaseCasOutcome,
    NodeLeaseRecord, OwnerRecord,
};

use crate::{Mutation, WriteId};

/// One row of a bundle: a cell epoch's database as the flush carried it.
pub type Row = (String, Epoch, Vec<WriteId>);

/// A bundle row without its writes: its cell epoch and how many it holds.
pub type RowLen = (String, Epoch, usize);

/// A restore found objects for a cell but no chain down to its first
/// position: what the epochs above continue was deleted or never written.
#[derive(Debug)]
pub struct NoBase;

/// One epoch of a restore chain: the positions above `cut` up to `ends`,
/// the last of them at txid `hi`.
struct Span {
    epoch: Epoch,
    cut: usize,
    ends: usize,
    hi: u64,
}

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
    /// Cold activations waiting or running, and whether it is draining.
    pub restoring: u64,
    pub draining: bool,
}

/// One epoch prefix of a cell, `cells/<cell>/ltx/e<epoch>/`.
#[derive(Default)]
struct Prefix {
    /// A cell's database is a list of writes, so a prefix holds the longest
    /// list any upload into it carried. Real LTX files are immutable and
    /// named by transaction range: a late or repeated upload adds files and
    /// never rolls the prefix back. Keeping the longest list has the same
    /// effect.
    writes: Vec<WriteId>,
    /// A paged epoch (`CELLD_LTX_PAGED`): the position it continues the
    /// chain below it from, and whether its marker is still in the bucket.
    /// Its objects hold only the positions above the cut; the ones below are
    /// the chain's (`Store::spans`). 0 for an epoch that opens at the first
    /// position.
    cut: usize,
    marker: bool,
    /// The epoch the paged activation continued, as its marker names it.
    source: Option<Epoch>,
    /// The marker's txid, one past the chain's last (`seed_continuation`):
    /// a marker takes a transaction of its own, so a paged epoch's writes
    /// follow it, and txids run ahead of positions by one per paged epoch
    /// in the chain. 0 for an epoch that opens at txid 1.
    marker_txid: u64,
}

impl Prefix {
    /// The txid of the write at `position` (from 1), above the cut.
    fn txid(&self, position: usize) -> u64 {
        match self.marker_txid {
            0 => position as u64,
            marker => marker + (position - self.cut) as u64,
        }
    }

    /// The position of the write at `txid`, the inverse of `txid`; the
    /// marker's txid is the cut.
    fn position(&self, txid: u64) -> usize {
        match self.marker_txid {
            0 => txid as usize,
            marker => self.cut + (txid - marker) as usize,
        }
    }

    /// Whether a listing finds objects in the prefix: its marker, or a
    /// position above its cut.
    fn listed(&self) -> bool {
        self.marker || self.writes.len() > self.cut
    }
}

/// What a recovery's claim step found.
#[derive(Debug, PartialEq, Eq)]
pub enum Claim {
    /// The log is sealed, or the lease no longer names the session.
    Done,
    /// This node holds the claim now: it held it already, or claimed an
    /// open log.
    Held,
    /// This node took over the stale claim of the node named.
    TookOver(String),
    /// Another node holds a live claim.
    Live(String),
}

#[derive(Default)]
pub struct Store {
    /// A planted bug in the bucket itself, if any.
    mutation: Option<Mutation>,
    next_etag: u64,
    leases: BTreeMap<String, (NodeLeaseRecord, u64, Load)>,
    /// The recovery claim folded into each lease body while its log is
    /// `Recovering`: the claimant node and its last heartbeat, its own wall
    /// clock ms.
    claims: BTreeMap<String, (String, u64)>,
    /// Cell → (owner node, epoch, etag).
    owners: BTreeMap<String, (Option<String>, Epoch, u64)>,
    /// One prefix per cell epoch. A deleted prefix keeps its entry, empty and
    /// without its marker, so a late upload lands above the same cut.
    prefixes: BTreeMap<(String, Epoch), Prefix>,
    /// How long a chain each epoch was restored from. An older owner can
    /// still extend the prefix below after the restore read it, and those
    /// writes are not in this epoch's database.
    bases: BTreeMap<(String, Epoch), usize>,
    /// When the newest upload into each prefix landed, true wall clock ms:
    /// what epoch GC's grace reads.
    uploaded_ms: BTreeMap<(String, Epoch), u64>,
    /// Epochs epoch GC deleted: production's retired mark, which makes
    /// per-cell coverage count every row of them as covered.
    retired: BTreeSet<(String, Epoch)>,
    /// Node bundles, `log/<session>/bundle/<seq>`: the rows one flush of a
    /// session's bundle loop carried, each a cell epoch's database. No
    /// restore reads them; a fold, a recovery or a compaction copies them
    /// into prefixes, and bundle GC deletes them.
    bundles: BTreeMap<(String, u64), Vec<Row>>,
    /// Each session's last bundle sequence number: the flusher's counter,
    /// so a deleted bundle's key is never used again.
    bundle_seq: BTreeMap<String, u64>,
    /// Wake entries: (cell, installation) → when it is due, wall clock ms.
    wake: BTreeMap<(String, PublicationId), i64>,
    /// Each cell's retirement certificate, `wake/retired/<cell>`: the newest
    /// installation an owner proved durable, below which every wake entry is
    /// obsolete.
    retirements: BTreeMap<String, Retirement>,
    /// The waker role, `wake/waker.json`: its holder and when it expires.
    waker: Option<(String, i64)>,
    /// The fleet drain token, `drain/token.json`, and its ETag.
    drain: Option<(DrainToken, u64)>,
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
        self.claims.remove(&record.node);
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
        self.claims.remove(node);
        true
    }

    /// `recover_serial`'s first step on `node`'s lease body, if it still
    /// names session `generation`: claim the recovery, keep a claim this
    /// node already holds, or take over one whose heartbeat is older than
    /// `stale_ms` by `now_ms`. The decisions are `log_tier`'s. Every write
    /// moves the etag, so it fences the session's own renewals too.
    pub fn claim_recovery(
        &mut self,
        node: &str,
        generation: &str,
        claimant: &str,
        now_ms: u64,
        stale_ms: u64,
    ) -> Claim {
        let Some(record) = self.log_record(node, generation) else {
            return Claim::Done;
        };
        let (claimed, outcome) = match record.state {
            LogState::Sealed => return Claim::Done,
            LogState::Open => (
                log_tier::start_recovery(&record, claimant, now_ms),
                Claim::Held,
            ),
            LogState::Recovering if log_tier::recovery_claimed_by(&record, claimant) => {
                return Claim::Held
            }
            LogState::Recovering if log_tier::recovery_claim_live(&record, now_ms, stale_ms) => {
                return Claim::Live(record.claimant.unwrap_or_default())
            }
            LogState::Recovering => (
                log_tier::take_over_recovery(&record, claimant, now_ms, stale_ms),
                Claim::TookOver(record.claimant.clone().unwrap_or_default()),
            ),
        };
        let claimed = claimed.expect("log_tier allows the claim it was asked for");
        self.write_log_record(node, &claimed);
        outcome
    }

    /// The claimant's heartbeat (`beat_claim`). False when the claim is no
    /// longer `claimant`'s: another node took it over, or the log sealed.
    pub fn beat_recovery(
        &mut self,
        node: &str,
        generation: &str,
        claimant: &str,
        now_ms: u64,
    ) -> bool {
        let Some(record) = self.log_record(node, generation) else {
            return true;
        };
        match log_tier::refresh_recovery(&record, claimant, now_ms) {
            Some(refreshed) => {
                self.write_log_record(node, &refreshed);
                true
            }
            None => false,
        }
    }

    /// Recovery's last step: seal the log, only while `claimant` still holds
    /// the claim. Ok when it sealed, or the log was sealed or replaced
    /// already; an error when another node took the claim over.
    pub fn finish_recovery(
        &mut self,
        node: &str,
        generation: &str,
        claimant: &str,
    ) -> Result<(), ()> {
        let Some(record) = self.log_record(node, generation) else {
            return Ok(());
        };
        if record.state == LogState::Sealed {
            return Ok(());
        }
        if !log_tier::recovery_claimed_by(&record, claimant) {
            return Err(());
        }
        let done = log_tier::finish_recovery(&record, 0).expect("a recovering log seals");
        self.write_log_record(node, &done);
        Ok(())
    }

    /// The log fields of `node`'s lease body as `log_tier` reads them, while
    /// the lease names session `generation` and has a log. The sim carries
    /// no epoch, ensemble or tiered offset in the record.
    fn log_record(&self, node: &str, generation: &str) -> Option<LogRecord> {
        let (lease, _, _) = self.leases.get(node)?;
        if lease.generation != generation {
            return None;
        }
        let claim = self.claims.get(node);
        Some(LogRecord {
            epoch: 0,
            ensemble: BTreeSet::new(),
            tiered: 0,
            state: lease.log_state?,
            claimant: claim.map(|(claimant, _)| claimant.clone()),
            claimed_ms: claim.map(|(_, at)| *at),
        })
    }

    fn write_log_record(&mut self, node: &str, record: &LogRecord) {
        let etag = self.etag();
        let (lease, current, _) = self.leases.get_mut(node).expect("read just now");
        lease.log_state = Some(record.state);
        *current = etag;
        match (&record.claimant, record.claimed_ms) {
            (Some(claimant), Some(at)) => {
                self.claims.insert(node.to_string(), (claimant.clone(), at));
            }
            _ => {
                self.claims.remove(node);
            }
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
                rss_bytes: load.memory,
                in_use_bytes: Some(load.memory),
                pressured: load.pressured,
                memory_headroom: Some(load.memory_headroom),
                restoring: load.restoring,
                paced_handoff: true,
                rebalance_paused: false,
                draining: load.draining,
            })
            .collect()
    }

    /// The drain token and its ETag, if the object exists.
    pub fn drain_token(&self) -> Option<&(DrainToken, u64)> {
        self.drain.as_ref()
    }

    /// Write the drain token if its ETag is still `guard` (`None`: only if
    /// absent). Returns the new ETag, or `None` when the guard lost.
    pub fn cas_drain_token(&mut self, guard: Option<u64>, token: DrainToken) -> Option<u64> {
        if self.drain.as_ref().map(|(_, etag)| *etag) != guard {
            return None;
        }
        let etag = self.etag();
        self.drain = Some((token, etag));
        Some(etag)
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
        let prefix = self.prefixes.entry(key).or_default();
        if content.len() > prefix.writes.len() {
            prefix.writes = content.to_vec();
        }
    }

    /// `put_bundle`: one flush of `session`'s bundle loop.
    pub fn put_bundle(&mut self, session: &str, rows: Vec<Row>) {
        let seq = self.bundle_seq.entry(session.to_string()).or_default();
        *seq += 1;
        self.bundles.insert((session.to_string(), *seq), rows);
    }

    /// Every bundle of `session` still in the bucket: its sequence number,
    /// and each row's cell epoch and length.
    pub fn bundles_of(&self, session: &str) -> Vec<(u64, Vec<RowLen>)> {
        let range = (session.to_string(), 0)..=(session.to_string(), u64::MAX);
        (self.bundles.range(range))
            .map(|((_, seq), rows)| {
                let rows = (rows.iter())
                    .map(|(cell, epoch, content)| (cell.clone(), *epoch, content.len()))
                    .collect();
                (*seq, rows)
            })
            .collect()
    }

    /// Delete bundles of `session` (`delete_many`), returning the rows the
    /// deleted ones carried. A bundle already gone is skipped.
    pub fn delete_bundles(&mut self, session: &str, seqs: &[u64]) -> Vec<Row> {
        (seqs.iter())
            .filter_map(|seq| self.bundles.remove(&(session.to_string(), *seq)))
            .flatten()
            .collect()
    }

    /// Every session with bundles whose name starts with `prefix`.
    pub fn bundle_sessions(&self, prefix: &str) -> Vec<String> {
        let mut sessions: Vec<String> = (self.bundles.keys())
            .filter(|(session, _)| session.starts_with(prefix))
            .map(|(session, _)| session.clone())
            .collect();
        sessions.dedup();
        sessions
    }

    /// How long the live prefix of a cell epoch is: what its objects hold,
    /// not counting the base below it or a retired mark.
    pub fn prefix_len(&self, cell: &str, epoch: Epoch) -> usize {
        (self.prefixes.get(&(cell.to_string(), epoch))).map_or(0, |prefix| prefix.writes.len())
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
        let range = (cell.to_string(), epoch)..=(cell.to_string(), Epoch::MAX);
        (self.prefixes.range(range)).any(|(_, prefix)| prefix.listed())
    }

    /// `scan_epochs`: the restore chain's epochs, oldest first, and every
    /// epoch that holds objects, with its newest upload's time.
    pub fn list_epochs(&self, cell: &str) -> (Vec<Epoch>, Vec<ListedEpoch>) {
        let chain = match self.spans(cell, Epoch::MAX) {
            Ok(spans) => spans.iter().rev().map(|span| span.epoch).collect(),
            Err(NoBase) => Vec::new(),
        };
        let cell_range = (cell.to_string(), 0)..=(cell.to_string(), Epoch::MAX);
        let listed = (self.prefixes.range(cell_range))
            .filter(|(_, prefix)| prefix.listed())
            .map(|(key, _)| ListedEpoch {
                epoch: key.1,
                newest_ms: self.uploaded_ms[key],
            })
            .collect();
        (chain, listed)
    }

    /// Delete every object of one epoch prefix, its marker too.
    pub fn delete_epoch(&mut self, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        if let Some(prefix) = self.prefixes.get_mut(&key) {
            prefix.writes.clear();
            prefix.marker = false;
        }
        self.retired.insert(key);
    }

    /// A paged activation's marker (`seed_continuation`): `epoch` continues
    /// the chain it restored, `content`, from its last position. Uploaded
    /// before the cell serves, and it counts as covering that position
    /// (`note_covered`). `head` is the chain's newest epoch and last txid
    /// (`chain_head`): the marker names that epoch, and takes the next txid.
    pub fn put_marker(
        &mut self,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
        head: Option<(Epoch, u64)>,
        at_ms: u64,
    ) {
        let prefix = self.prefixes.entry((cell.to_string(), epoch)).or_default();
        prefix.cut = content.len();
        prefix.marker = true;
        prefix.source = head.map(|(source, _)| source);
        // A planted bug can restore content no chain holds; its marker
        // follows the content.
        prefix.marker_txid = head.map_or(content.len() as u64, |(_, hi)| hi) + 1;
        self.put(cell, epoch, content, at_ms);
    }

    /// The newest epoch of the chain a restore at `epoch` reads, and the
    /// chain's last txid (`EpochChain::spans`, `max_txid`): what a paged
    /// activation's marker names and follows.
    pub fn chain_head(&self, cell: &str, epoch: Epoch) -> Option<(Epoch, u64)> {
        let spans = self.spans(cell, epoch).ok()?;
        spans.first().map(|span| (span.epoch, span.hi))
    }

    /// What a restore at `epoch` reads: the chain below it. `None` when no
    /// epoch below holds objects.
    pub fn read_chain(&mut self, cell: &str, epoch: Epoch) -> Result<Option<Vec<WriteId>>, NoBase> {
        let chain = self.chain(cell, epoch)?;
        let len = chain.as_ref().map_or(0, Vec::len);
        let base = self.bases.entry((cell.to_string(), epoch)).or_default();
        *base = (*base).max(len);
        Ok(chain)
    }

    fn chain(&self, cell: &str, epoch: Epoch) -> Result<Option<Vec<WriteId>>, NoBase> {
        let epoch = match self.mutation {
            // The planted bug skips the previous epoch's prefix, the usual
            // source.
            Some(Mutation::StaleRestore) => epoch.saturating_sub(1),
            // The planted bug restores the newest epoch that opens at the
            // first position, as if no epoch continued a chain.
            Some(Mutation::NewestBase) => {
                let range = (cell.to_string(), 0)..(cell.to_string(), epoch);
                return Ok((self.prefixes.range(range).rev())
                    .find(|(_, prefix)| prefix.cut == 0 && !prefix.writes.is_empty())
                    .map(|(_, prefix)| prefix.writes.clone()));
            }
            _ => epoch,
        };
        let chain = self.chain_below(cell, epoch)?;
        Ok(match (self.mutation, chain) {
            (Some(Mutation::TornRestore), Some(chain)) if chain.len() >= 2 => {
                Some(chain[1..].to_vec())
            }
            (_, chain) => chain,
        })
    }

    /// The database the chain below `epoch` restores, as the bucket truly
    /// holds it. The oracle reads this, so a planted bug never bends its
    /// view.
    fn chain_below(&self, cell: &str, epoch: Epoch) -> Result<Option<Vec<WriteId>>, NoBase> {
        let spans = self.spans(cell, epoch)?;
        Ok((!spans.is_empty()).then(|| self.assemble(cell, &spans)))
    }

    /// `EpochChain::build` over the epochs below `below`, newest first, with
    /// production's walk (`celld_logic::epoch_chain::Walk`), over the
    /// objects' txids (`Prefix::txid`): an epoch's objects hold the writes
    /// above its cut, and a paged epoch's marker has a txid of its own just
    /// below them, as production's does. Empty when nothing is listed
    /// (production's `TxNotAvailable`), `NoBase` when objects are but no
    /// span opens at the first position: production builds that chain and
    /// fails later, when its restore plan finds no first position
    /// (`calc_restore_plan`).
    fn spans(&self, cell: &str, below: Epoch) -> Result<Vec<Span>, NoBase> {
        let mut spans: Vec<Span> = Vec::new();
        let mut walk = Walk::new();
        let range = (cell.to_string(), 0)..(cell.to_string(), below);
        for (key, prefix) in self.prefixes.range(range).rev() {
            match walk.wants(key.1) {
                Wants::List => {}
                Wants::Pass => continue,
                Wants::Done => break,
            }
            if !prefix.listed() {
                continue;
            }
            // One object per write above the cut, and the marker at its own
            // txid.
            let writes =
                (prefix.cut + 1..=prefix.writes.len()).map(|position| prefix.txid(position));
            let marker = prefix.marker.then_some(prefix.marker_txid);
            let files = writes.chain(marker).map(|txid| (txid, txid));
            let Step::Link {
                lo,
                hi,
                base,
                marker: at_lo,
            } = walk.visit(key.1, files)
            else {
                continue;
            };
            // The object at `lo` is the marker only if it is still listed;
            // a write there names nothing, as production reads it.
            if let (true, Some(source)) = (at_lo && marker == Some(lo), prefix.source) {
                walk.continues_from(source);
            }
            spans.push(Span {
                epoch: key.1,
                cut: prefix.cut,
                ends: prefix.position(hi),
                hi,
            });
            if base {
                return Ok(spans);
            }
        }
        match spans.is_empty() {
            true => Ok(spans),
            false => Err(NoBase),
        }
    }

    /// The database the spans restore: each span's positions, base first.
    fn assemble(&self, cell: &str, spans: &[Span]) -> Vec<WriteId> {
        (spans.iter().rev())
            .flat_map(|span| {
                let prefix = &self.prefixes[&(cell.to_string(), span.epoch)];
                &prefix.writes[span.cut..span.ends]
            })
            .copied()
            .collect()
    }

    /// An activation opened `epoch` from an eviction snapshot of `len`
    /// writes, all in the bucket below it: the chain it restored from.
    pub fn reused(&mut self, cell: &str, epoch: Epoch, len: usize) {
        let base = self.bases.entry((cell.to_string(), epoch)).or_default();
        *base = (*base).max(len);
    }

    /// How long a chain the epoch was restored from, as long as the restore
    /// read it.
    pub fn base_len(&self, cell: &str, epoch: Epoch) -> usize {
        (self.bases.get(&(cell.to_string(), epoch)).copied()).unwrap_or(0)
    }

    /// How much of the epoch's database the per-cell layout holds, which is
    /// all a restore reads (`percell_coverage`): bundles do not count, and
    /// a retired epoch counts as covered whole.
    pub fn percell_len(&self, cell: &str, epoch: Epoch) -> usize {
        if self.retired.contains(&(cell.to_string(), epoch)) {
            return usize::MAX;
        }
        self.covered(self.prefix_len(cell, epoch), cell, epoch)
    }

    fn covered(&self, len: usize, cell: &str, epoch: Epoch) -> usize {
        match len {
            0 => self.base_len(cell, epoch),
            len => len,
        }
    }

    /// The cell's newest database in the per-cell layout, as its restore
    /// chain holds it. `None` when nothing is listed, `NoBase` when the
    /// chain is broken.
    pub fn newest(&self, cell: &str) -> Result<Option<Vec<WriteId>>, NoBase> {
        self.chain_below(cell, Epoch::MAX)
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
}

fn guard_matches(guard: &CasGuard, current: Option<u64>) -> bool {
    match (guard, current) {
        (CasGuard::Absent, None) => true,
        (CasGuard::Match(etag), Some(current)) => *etag == current.to_string(),
        _ => false,
    }
}
