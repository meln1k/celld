//! The oracle.
//!
//! The world tells the checker facts as they happen: a claim applied at the
//! bucket, a runtime started, a response released, a database restored. The
//! checker keeps the few records it needs (ghost state no node can see) and
//! tests each property when its fact arrives. The shell never decides
//! whether something is a violation; every property lives in this file.

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
    pub violations: Vec<Violation>,
}

impl Checker {
    fn violation(&mut self, at_ms: u64, kind: &'static str, detail: String) {
        self.violations.push(Violation {
            at_ms,
            kind,
            detail,
        });
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
    #[allow(clippy::too_many_arguments)]
    pub fn response_released(
        &mut self,
        t: u64,
        store: &Store,
        who: &str,
        cell: &str,
        epoch: Epoch,
        committed: Option<(WriteId, u64)>,
        seen: &[WriteId],
        sent_at: u64,
    ) {
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
            if !fresh {
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
        let Some((write, position)) = committed else {
            if durable < saw {
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
        if at != Some(&write) || durable < saw {
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
