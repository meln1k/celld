//! The oracle.
//!
//! The world tells the checker facts as they happen: a claim applied at the
//! bucket, a runtime started, a response released, a database restored. The
//! checker keeps the few records it needs (ghost state no node can see) and
//! tests each property when its fact arrives. The shell never decides
//! whether something is a violation: every property lives in this file,
//! or, when its facts are the fleet's state at a moment (which cells a
//! process serves, what it holds open), in `world/judges.rs`, which picks
//! out those facts and their thresholds and hands each hit here.
//!
//! Some properties are only *watched*: a hit is recorded in
//! `Outcome::watched` but never fails a run. A property starts there, and is
//! enforced only once every hit a sweep finds traces to a bug in the core
//! rather than in the simulation or in the property itself.

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::Epoch;

use crate::store::Store;
use crate::WriteId;

/// One broken property, with when it broke.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub at_ms: u64,
    pub kind: &'static str,
    pub detail: String,
}

/// What an output that left a cell revealed: the cell's database as its
/// handler saw it, the write it committed, if any, and when its client sent
/// the request.
pub struct Reveal {
    pub cell: String,
    pub epoch: Epoch,
    /// The handler's write and its position.
    pub committed: Option<(WriteId, u64)>,
    pub seen: Vec<WriteId>,
    pub sent_at: u64,
}

#[derive(Default)]
pub struct Checker {
    /// Every ownership claim the bucket applied: (cell, epoch, node). A
    /// claim stays here after a later epoch supersedes it; the record says
    /// a node once won this epoch, not that it still owns the cell.
    claims: BTreeSet<(String, Epoch, String)>,
    /// The one process (`node#incarnation`) that ran each cell epoch, and
    /// the one that uploaded into its prefix. A process, not a node: a
    /// restarted process is a different writer from the one that died, and
    /// the dead one can still have an upload on the wire.
    runtimes: BTreeMap<(String, Epoch), String>,
    uploaders: BTreeMap<(String, Epoch), String>,
    /// Each clean-reload process, and the preserved one it took over from.
    /// The two are one writer: they share the node's database files.
    predecessors: BTreeMap<String, String>,
    /// What each cell epoch's database held when a node opened it. The
    /// newest open of an epoch wins, so a crashed epoch's reopen counts.
    opened: BTreeMap<(String, Epoch), BTreeSet<WriteId>>,
    /// Every write a client was told succeeded: (cell, epoch, write).
    acked: BTreeSet<(String, Epoch, WriteId)>,
    /// Every alarm handler's commit the output gate proved: (cell, epoch,
    /// write). No client hears of it, but the core acts on it as durable:
    /// it settles the alarm, and deletes the consumed alarm's wake entry.
    handler_proven: BTreeSet<(String, Epoch, WriteId)>,
    /// The acknowledged writes whose handler's own answer told its client:
    /// (cell, write). Only the answer waits for the arm gate.
    answered: BTreeSet<(String, WriteId)>,
    /// Each cell's acknowledged writes, with the instant each client heard.
    acked_at: BTreeMap<String, Vec<(u64, WriteId)>>,
    /// Every database that ever ran each cell epoch, as the list of writes
    /// it held: what it opened with, then each commit. A restore must
    /// produce a state one of them actually passed through.
    lineages: BTreeMap<(String, Epoch), Vec<Vec<WriteId>>>,
    /// The newest epoch the bucket ever recorded for each cell.
    newest_epoch: BTreeMap<String, Epoch>,
    /// When each cell epoch's prefix first held each length: index `n` is
    /// the instant it reached `n + 1` writes.
    durable_at: BTreeMap<(String, Epoch), Vec<u64>>,
    /// The last instant a process's ownership read found the record naming
    /// it at a cell epoch: (process, cell, epoch).
    verified_at: BTreeMap<(String, String, Epoch), u64>,
    /// The followers of each process's log.
    ensembles: BTreeMap<String, Vec<usize>>,
    /// The longest database of each cell epoch each follower ever fsynced
    /// for a leader process: (leader, follower, cell, epoch).
    fsynced: BTreeMap<(String, usize, String, Epoch), Vec<WriteId>>,
    /// Acknowledged writes that only the process's fleet held when they
    /// were acknowledged: (process, cell, epoch, write). A declared loss of
    /// that process's log may lose them, and nothing else.
    fleet_acked: BTreeSet<(String, String, Epoch, WriteId)>,
    pub violations: Vec<Violation>,
    /// Hits on watched properties, the first per kind and subject.
    pub watched: Vec<Violation>,
    watched_keys: BTreeSet<(&'static str, String)>,
}

impl Checker {
    fn violation(&mut self, at_ms: u64, kind: &'static str, detail: String) {
        self.violations.push(Violation {
            at_ms,
            kind,
            detail,
        });
    }

    /// Record a hit on a watched property, once per `subject`.
    fn watch(&mut self, at_ms: u64, kind: &'static str, subject: String, detail: String) {
        if self.watched_keys.insert((kind, subject)) {
            self.watched.push(Violation {
                at_ms,
                kind,
                detail,
            });
        }
    }

    /// The handler of `write` of `cell` answered its client that it
    /// succeeded: not a `fetch()` it made, and not an error.
    pub fn answer_acknowledged(&mut self, cell: &str, write: WriteId) {
        self.answered.insert((cell.to_string(), write));
    }

    /// Whether the handler of `write` of `cell` answered its client that it
    /// succeeded. A `fetch()` the handler made may have revealed it sooner.
    pub fn answered(&self, cell: &str, write: WriteId) -> bool {
        self.answered.contains(&(cell.to_string(), write))
    }

    /// The output gate proved alarm handler `who`'s commit `write`: through
    /// the fleet, or through the bucket and an ownership read.
    pub fn handler_commit_proven(
        &mut self,
        store: &Store,
        who: &str,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
        fleet: bool,
    ) {
        self.handler_proven.insert((cell.to_string(), epoch, write));
        if fleet && !store.prefix(cell, epoch).contains(&write) {
            let key = (who.to_string(), cell.to_string(), epoch, write);
            self.fleet_acked.insert(key);
        }
    }

    /// Watched: **an acknowledged alarm stays discoverable.** Cleanup
    /// deletes only older installations or a proven consumed one, and
    /// retires an armed one only behind a confirmed replacement
    /// (`docs/guarantees.md`). So whenever the alarm a restore would load
    /// was set by a write whose handler answered its client, a wake entry
    /// due by its time stands in the bucket.
    pub fn alarm_undiscoverable(&mut self, t: u64, cell: &str, write: WriteId, at_ms: i64) {
        self.watch(
            t,
            "alarm-undiscoverable",
            format!("{cell} {write}"),
            format!(
                "{cell}'s durable alarm at {at_ms}, set by acknowledged write {write}, has no \
                 wake entry due by then"
            ),
        );
    }

    /// Watched: **a handoff ends at a claim.** A peer answers a donor that
    /// it accepted a cell only once its own node's claim installed the
    /// epoch it names. Owner records name nodes, so a restarted process
    /// holds what an earlier process on its node claimed.
    pub fn handoff_accepted(&mut self, t: u64, who: &str, node: &str, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch, node.to_string());
        if !self.claims.contains(&key) {
            self.watch(
                t,
                "unclaimed-handoff",
                format!("{who} {cell} {epoch}"),
                format!(
                    "{who} accepted a handoff of {cell} at epoch {epoch} without {node}'s claim"
                ),
            );
        }
    }

    /// Watched: **a resume never follows a takeover.** A clean reload opens
    /// its predecessor's file without reading the owner record
    /// (`docs/guarantees.md`). That is safe only while the record still
    /// names this node at the file's epoch.
    pub fn resumed(&mut self, t: u64, who: &str, cell: &str, epoch: Epoch, owner: String) {
        self.watch(
            t,
            "resumed-superseded",
            format!("{who} {cell}"),
            format!("{who} resumed {cell} epoch {epoch} while the owner record names {owner}"),
        );
    }

    /// **An acknowledged alarm outlives its process.** The arm gate holds a
    /// response until a wake entry covers the alarm (README), so when a
    /// process dies, every alarm whose handler answered its client must have
    /// a wake entry due by its time. Its `fetch()`es are not held, as in
    /// production, so a third party may learn of the alarm sooner.
    pub fn died_uncovered(&mut self, t: u64, who: &str, cell: &str, write: WriteId) {
        self.violation(
            t,
            "crash-uncovered-alarm",
            format!(
                "{who} died holding {cell}'s alarm, set by acknowledged write {write}, with no \
                 wake entry due by then"
            ),
        );
    }

    /// Watched: **alarms survive the fleet.** After the audit's full
    /// restart, the fresh node must still fire every overdue durable alarm:
    /// the waker wakes cells whose owner has stopped (README).
    pub fn audit_alarm_unfired(&mut self, t: u64, cell: &str, overdue_ms: i64) {
        self.watch(
            t,
            "audit-alarm-not-fired",
            cell.to_string(),
            format!("{cell}'s durable alarm is still armed {overdue_ms} ms after it came due"),
        );
    }

    /// Watched: **a draining node claims nothing.** A draining ingress
    /// refuses new ownership of an unowned cell or another node's
    /// (`docs/guarantees.md`). Judged when the claim applies, for a claim
    /// sent once the shutdown had begun: one sent before can land after.
    pub fn claimed_during_shutdown(&mut self, t: u64, who: &str, cell: &str, detail: String) {
        self.watch(
            t,
            "claim-during-shutdown",
            format!("{who} {cell}"),
            format!("{who} took {cell} with a claim sent while {detail}"),
        );
    }

    /// Watched: **a deploy finishes.** After `max_age_ms`, celld forces a
    /// resident cell onto the new generation (README).
    pub fn deploy_stuck(&mut self, t: u64, who: &str, cell: &str, detail: String) {
        self.watch(
            t,
            "deploy-stuck",
            format!("{who} {cell}"),
            format!("{who} still runs {cell} {detail}"),
        );
    }

    /// Watched: **an idle cell is given back.** celld removes a cell from
    /// memory after `CELLD_IDLE_EVICT_S` without use (README).
    pub fn idle_kept(&mut self, t: u64, who: &str, cell: &str, idle_ms: u64) {
        self.watch(
            t,
            "idle-not-evicted",
            format!("{who} {cell}"),
            format!("{who} still holds {cell}, unused for {idle_ms} ms"),
        );
    }

    /// Watched: **memory pressure is relieved.** A node over its memory
    /// ceiling (`CELLD_MAX_RSS_MB`) sheds cells until it is back below its
    /// resume line, and admits cells again.
    pub fn memory_kept(&mut self, t: u64, who: &str, bytes: u64, shedding_ms: u64) {
        self.watch(
            t,
            "memory-not-relieved",
            who.to_string(),
            format!("{who} has shed for {shedding_ms} ms and still holds {bytes} bytes"),
        );
    }

    /// Watched: **an outbound socket closes when its cell moves**
    /// (`docs/limitations.md`): none stays open on a process whose runtime
    /// of the cell has stopped.
    pub fn socket_outlived(&mut self, t: u64, who: &str, cell: &str, websocket: u64) {
        self.watch(
            t,
            "socket-outlives-runtime",
            format!("{who} {websocket}"),
            format!(
                "{who} keeps outbound socket {websocket} of {cell} open after its runtime stopped"
            ),
        );
    }

    /// Process `who` opened a log that `ensemble` follows.
    pub fn log_opened(&mut self, who: &str, ensemble: &[usize]) {
        self.ensembles.insert(who.to_string(), ensemble.to_vec());
    }

    /// A follower fsynced a cell epoch's database for leader `who`.
    pub fn follower_fsynced(
        &mut self,
        who: &str,
        follower: usize,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
    ) {
        let key = (who.to_string(), follower, cell.to_string(), epoch);
        let held = self.fsynced.entry(key).or_default();
        if content.len() > held.len() {
            *held = content.to_vec();
        }
    }

    /// Whether every follower of `who`'s log has fsynced `seen`, write for
    /// write, at this cell epoch. Such a write needs no ownership read: a
    /// takeover recovers the log first, and recovery seals each follower
    /// before it gathers what they hold.
    fn fleet_holds(&self, who: &str, cell: &str, epoch: Epoch, seen: &[WriteId]) -> bool {
        self.ensembles.get(who).is_some_and(|ensemble| {
            ensemble.iter().all(|&f| {
                let key = (who.to_string(), f, cell.to_string(), epoch);
                self.fsynced
                    .get(&key)
                    .is_some_and(|held| held.starts_with(seen))
            })
        })
    }

    /// Recovery found every follower of `who`'s log gone with its disk and
    /// declared a bounded loss, as production may. The acknowledged writes
    /// only that log held, and the bucket still lacks, are gone; forget
    /// them, so no later check expects them. Returns how many.
    pub fn declare_loss(&mut self, who: &str, store: &Store) -> u64 {
        let lost: Vec<(String, Epoch, WriteId)> = self
            .fleet_acked
            .iter()
            .filter(|(w, cell, epoch, write)| {
                w == who && !store.prefix(cell, *epoch).contains(write)
            })
            .map(|(_, cell, epoch, write)| (cell.clone(), *epoch, *write))
            .collect();
        for (cell, epoch, write) in &lost {
            self.acked.remove(&(cell.clone(), *epoch, *write));
            self.handler_proven.remove(&(cell.clone(), *epoch, *write));
            if let Some(acks) = self.acked_at.get_mut(cell) {
                acks.retain(|(_, w)| w != write);
            }
        }
        lost.len() as u64
    }

    /// An owner compare-and-swap applied at the bucket. Recorded when it
    /// applies, not when the node hears back: a node can lose the response
    /// to a claim that did take effect.
    ///
    /// **The epoch only grows.** Every activation advances it
    /// (`docs/guarantees.md`), so a claim at an epoch the cell already had
    /// gives one epoch two lineages, whether or not both ever run.
    pub fn claim_applied(&mut self, t: u64, cell: &str, epoch: Epoch, node: &str) {
        self.claims
            .insert((cell.to_string(), epoch, node.to_string()));
        let newest = self.newest_epoch.entry(cell.to_string()).or_insert(0);
        if epoch <= *newest {
            let newest = *newest;
            self.violation(
                t,
                "epoch-reused",
                format!(
                    "{node} claimed {cell} at epoch {epoch}, but it already had epoch {newest}"
                ),
            );
        } else {
            *newest = epoch;
        }
    }

    /// The bucket now holds `len` writes in this cell epoch's prefix.
    pub fn prefix_grew(&mut self, t: u64, cell: &str, epoch: Epoch, len: usize) {
        let times = self
            .durable_at
            .entry((cell.to_string(), epoch))
            .or_default();
        while times.len() < len {
            times.push(t);
        }
    }

    /// A process's ownership read found the record naming it at `epoch`.
    pub fn ownership_read(&mut self, t: u64, who: &str, cell: &str, epoch: Epoch) {
        let key = (who.to_string(), cell.to_string(), epoch);
        self.verified_at.insert(key, t);
    }

    /// A database committed `write` on the lineage `replica_opened` named.
    pub fn committed(&mut self, cell: &str, epoch: Epoch, lineage: usize, write: WriteId) {
        let key = (cell.to_string(), epoch);
        self.lineages.get_mut(&key).expect("opened")[lineage].push(write);
    }

    /// **No runtime without its own claim.** A node may run a cell at an
    /// epoch only if its own compare-and-swap installed that epoch. Anything
    /// else means two nodes can believe they own the same epoch.
    ///
    /// It is also one half of **one writer per epoch**: only one node ever
    /// runs a cell epoch, and only one ever uploads into its prefix. Two
    /// would be two histories of the cell under one name, and a restore
    /// would read whichever it found.
    pub fn runtime_started(&mut self, t: u64, node: &str, who: &str, cell: &str, epoch: Epoch) {
        let writer = self.writer(who);
        if let Some(first) = first_writer(&mut self.runtimes, &writer, cell, epoch) {
            self.violation(
                t,
                "two-writers",
                format!("{who} ran {cell} epoch {epoch}, which {first} already ran"),
            );
        }
        let key = (cell.to_string(), epoch, node.to_string());
        if !self.claims.contains(&key) {
            self.violation(
                t,
                "unclaimed-runtime",
                format!("{node} started {cell} epoch {epoch} without an applied claim of its own"),
            );
        }
    }

    /// An upload landed in the prefix of `cell` epoch `epoch`: the other
    /// half of **one writer per epoch**. Checked when the upload lands, not
    /// when it is sent, because a node that lost the cell can still have an
    /// upload on the wire.
    pub fn upload_applied(&mut self, t: u64, who: &str, cell: &str, epoch: Epoch) {
        let writer = self.writer(who);
        if let Some(first) = first_writer(&mut self.uploaders, &writer, cell, epoch) {
            self.violation(
                t,
                "two-writers",
                format!("{who} uploaded into {cell} epoch {epoch}, which {first} already wrote"),
            );
        }
    }

    /// **An eviction that evicted left nothing running.** When the core
    /// tells an operator a cell was evicted, the node no longer serves it.
    pub fn evicted(&mut self, t: u64, who: &str, cell: &str, serving: bool) {
        if serving {
            self.violation(
                t,
                "evicted-but-serving",
                format!("{who} reported {cell} evicted with its runtime still published"),
            );
        }
    }

    /// A preserved process `old` handed its epochs to its replacement
    /// `new`, which resumes them in place. The two count as one writer, so
    /// `new` may run and upload into `old`'s epochs, and an upload `old` left
    /// on the wire may still land.
    pub fn reloaded(&mut self, old: &str, new: &str) {
        self.predecessors.insert(new.to_string(), old.to_string());
    }

    /// The first process of `who`'s chain of clean reloads.
    fn writer(&self, who: &str) -> String {
        let mut who = who;
        while let Some(old) = self.predecessors.get(who) {
            who = old;
        }
        who.to_string()
    }

    /// **An open transport pins its cell.** A regular or outbound WebSocket
    /// cannot move with ownership, so the core must not evict a cell that
    /// holds one open; a handoff closes them first.
    ///
    /// **An evicted alarm can be found.** A cell given up with an armed
    /// alarm must have a wake entry for it in the bucket, or no node will
    /// wake it when it comes due.
    pub fn evicting(&mut self, t: u64, who: &str, cell: &str, pinned: bool, uncovered: bool) {
        if pinned {
            self.violation(
                t,
                "evicted-pinned",
                format!("{who} evicted {cell} with a regular or outbound WebSocket open"),
            );
        }
        if uncovered {
            self.violation(
                t,
                "uncovered-alarm",
                format!("{who} evicted {cell} with an armed alarm and no wake entry due by then"),
            );
        }
    }

    /// **Every durable alarm fires.** Once faults heal, an alarm the bucket
    /// holds is fired and consumed soon after it comes due.
    pub fn alarm_unfired(&mut self, t: u64, cell: &str, overdue_ms: i64) {
        self.violation(
            t,
            "alarm-not-fired",
            format!("{cell}'s durable alarm is still armed {overdue_ms} ms after it came due"),
        );
    }

    /// **Liveness.** Once faults have healed, the fleet must serve every
    /// cell before the deadline.
    pub fn unserved(&mut self, t: u64, phase: &str, cell: &str) {
        self.violation(
            t,
            "liveness",
            format!("{phase}: {cell} was not served before its deadline"),
        );
    }

    /// **No local route without a runtime.** When the core tells a request
    /// to run here, a runtime for the cell must be published here.
    pub fn routed_local(&mut self, t: u64, node: &str, cell: &str, published: bool) {
        if !published {
            self.violation(
                t,
                "local-without-runtime",
                format!("{node} routed a request for {cell} locally with no published runtime"),
            );
        }
    }

    /// **Nothing is revealed before it is durable.** A response reveals
    /// everything its handler saw: the cell's database up to `saw` when it
    /// answered, which includes its own write if it made one. When the
    /// response leaves, the bucket must hold all of that for the epoch, and
    /// a write must sit at the position it was committed at.
    ///
    /// `saw` comes from the simulated database, not from what the shell
    /// told the core, so a shell that understates what a handler saw
    /// cannot fool the core and this check together.
    ///
    /// **No stale read.** Whatever a response reveals must include every
    /// write a client had already been told succeeded when this request was
    /// sent. Otherwise two nodes served the cell at once: the one that
    /// acknowledged and the one that answered from older state.
    ///
    /// **No ack without a fresh ownership read.** A write proven through
    /// the bucket is acknowledged only after a read of the owner record,
    /// made once the bucket held the write, named this process's node at
    /// this epoch (`docs/guarantees.md`).
    pub fn response_released(&mut self, t: u64, store: &Store, who: &str, reveal: &Reveal) {
        let Reveal {
            cell,
            epoch,
            committed,
            seen,
            sent_at,
        } = reveal;
        let (cell, epoch, committed, sent_at) = (cell.as_str(), *epoch, *committed, *sent_at);
        let saw = seen.len() as u64;
        let missed = self
            .acked_at
            .get(cell)
            .into_iter()
            .flatten()
            .find(|(at, write)| *at < sent_at && !seen.contains(write))
            .copied();
        if let Some((at, write)) = missed {
            self.violation(
                t,
                "stale-read",
                format!(
                    "{who} answered a request for {cell} sent at {sent_at} from epoch {epoch} \
                     without write {write}, acknowledged at {at}"
                ),
            );
        }
        if let Some((write, position)) = committed {
            let durable_since = self
                .durable_at
                .get(&(cell.to_string(), epoch))
                .and_then(|times| times.get(position as usize - 1))
                .copied();
            let read = self
                .verified_at
                .get(&(who.to_string(), cell.to_string(), epoch))
                .copied();
            let fresh = matches!((durable_since, read), (Some(d), Some(r)) if r >= d);
            let proved = &seen[..(position as usize).min(seen.len())];
            if !fresh && !self.fleet_holds(who, cell, epoch, proved) {
                self.violation(
                    t,
                    "unverified-ack",
                    format!(
                        "{who} acknowledged write {write} of {cell} epoch {epoch} with no \
                         ownership read since the bucket held it at {durable_since:?} \
                         (last read {read:?})"
                    ),
                );
            }
        }
        let durable = store.durable_len(cell, epoch) as u64;
        // Durable in the bucket, or on every follower of the process's log.
        let fleet = self.fleet_holds(who, cell, epoch, seen);
        let Some((write, position)) = committed else {
            if durable < saw && !fleet {
                self.violation(
                    t,
                    "read-before-durable",
                    format!(
                        "a read of {cell} epoch {epoch} saw {saw} writes and was released, \
                         but the bucket holds only {durable}"
                    ),
                );
            }
            return;
        };
        let prefix = store.prefix(cell, epoch);
        let at = position.checked_sub(1).and_then(|i| prefix.get(i as usize));
        if (at != Some(&write) || durable < saw) && !fleet {
            self.violation(
                t,
                "ack-before-durable",
                format!(
                    "write {write} of {cell} epoch {epoch} at position {position} was \
                     acknowledged after its handler saw {saw} writes, but the bucket holds \
                     {durable} and {at:?} there"
                ),
            );
        }
        // **No ack after a takeover.** A later epoch that already opened
        // without this write has lost it, even though this epoch's prefix
        // holds it: the ack came too late.
        let missing = self
            .opened
            .range((cell.to_string(), epoch + 1)..(cell.to_string(), Epoch::MAX))
            .find(|(_, content)| !content.contains(&write))
            .map(|((_, later), _)| *later);
        if let Some(later) = missing {
            self.violation(
                t,
                "late-ack",
                format!(
                    "write {write} of {cell} epoch {epoch} was acknowledged after epoch \
                     {later} had already opened without it"
                ),
            );
        }
        self.acked.insert((cell.to_string(), epoch, write));
        if fleet && !store.prefix(cell, epoch).contains(&write) {
            let key = (who.to_string(), cell.to_string(), epoch, write);
            self.fleet_acked.insert(key);
        }
        self.acked_at
            .entry(cell.to_string())
            .or_default()
            .push((t, write));
    }

    /// **No acknowledged write is lost.** A database opened at `epoch` must
    /// hold every write acknowledged so far at that epoch or an earlier one.
    /// That is what the acknowledgement promised: whoever owns the cell next
    /// sees the write. Reopening the same epoch, after a crash, is held to
    /// the same promise.
    ///
    /// **A restore is a real state.** What it opens with must be a state
    /// some database of an earlier epoch actually held: a prefix of one of
    /// its lineages. A hole, a reordering or a foreign write is not. Only a
    /// clean reload, which `resumed` the predecessor's file, may open with a
    /// state of this same epoch.
    ///
    /// Returns the lineage this database starts, for `committed`.
    pub fn replica_opened(
        &mut self,
        t: u64,
        node: &str,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
        resumed: bool,
    ) -> usize {
        let below = epoch + u64::from(resumed);
        let real = content.is_empty()
            || self
                .lineages
                .range((cell.to_string(), 0)..(cell.to_string(), below))
                .flat_map(|(_, lineages)| lineages)
                .any(|lineage| lineage.starts_with(content));
        if !real {
            self.violation(
                t,
                "torn-restore",
                format!(
                    "{node} opened {cell} epoch {epoch} with {} writes that no earlier \
                     database ever held in that order",
                    content.len()
                ),
            );
        }
        let lineages = self.lineages.entry((cell.to_string(), epoch)).or_default();
        lineages.push(content.to_vec());
        let lineage = lineages.len() - 1;
        let content: BTreeSet<WriteId> = content.iter().copied().collect();
        let lost: Vec<WriteId> = self
            .acked
            .range((cell.to_string(), 0, 0)..=(cell.to_string(), epoch, WriteId::MAX))
            .map(|(_, _, write)| *write)
            .filter(|write| !content.contains(write))
            .collect();
        if let Some(first) = lost.first() {
            self.violation(
                t,
                "lost-write",
                format!(
                    "{node} opened {cell} epoch {epoch} without {} acknowledged write(s), \
                     first {first}",
                    lost.len()
                ),
            );
        }
        // Watched: **no proven alarm commit is lost** either. Losing one
        // runs the handler again, or brings back an alarm whose wake entry
        // the core already deleted.
        let lost: Vec<WriteId> = self
            .handler_proven
            .range((cell.to_string(), 0, 0)..=(cell.to_string(), epoch, WriteId::MAX))
            .map(|(_, _, write)| *write)
            .filter(|write| !content.contains(write))
            .collect();
        if let Some(first) = lost.first() {
            self.watch(
                t,
                "lost-handler-write",
                format!("{cell} {epoch}"),
                format!(
                    "{node} opened {cell} epoch {epoch} without {} proven alarm handler \
                     write(s), first {first}",
                    lost.len()
                ),
            );
        }
        self.opened.insert((cell.to_string(), epoch), content);
        lineage
    }
}

/// Record `node` as the writer of `cell` epoch `epoch` unless another node
/// already is; return that other node.
fn first_writer(
    writers: &mut BTreeMap<(String, Epoch), String>,
    node: &str,
    cell: &str,
    epoch: Epoch,
) -> Option<String> {
    let first = writers
        .entry((cell.to_string(), epoch))
        .or_insert_with(|| node.to_string());
    (first != node).then(|| first.clone())
}
