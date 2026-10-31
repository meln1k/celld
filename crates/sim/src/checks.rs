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
    /// What each cell epoch's database held when a node opened it. The
    /// newest open of an epoch wins, so a crashed epoch's reopen counts.
    opened: BTreeMap<(String, Epoch), BTreeSet<WriteId>>,
    /// Every write a client was told succeeded: (cell, epoch, write).
    acked: BTreeSet<(String, Epoch, WriteId)>,
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
    pub fn claim_applied(&mut self, cell: &str, epoch: Epoch, node: &str) {
        self.claims
            .insert((cell.to_string(), epoch, node.to_string()));
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
        if let Some(first) = first_writer(&mut self.runtimes, who, cell, epoch) {
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
        if let Some(first) = first_writer(&mut self.uploaders, who, cell, epoch) {
            self.violation(
                t,
                "two-writers",
                format!("{who} uploaded into {cell} epoch {epoch}, which {first} already wrote"),
            );
        }
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
    pub fn response_released(
        &mut self,
        t: u64,
        store: &Store,
        cell: &str,
        epoch: Epoch,
        committed: Option<(WriteId, u64)>,
        saw: u64,
    ) {
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
        // A later epoch that already opened without this write has lost it,
        // even though this epoch's prefix holds it: the ack came too late.
        let missing = self
            .opened
            .range((cell.to_string(), epoch + 1)..(cell.to_string(), Epoch::MAX))
            .find(|(_, content)| !content.contains(&write))
            .map(|((_, later), _)| *later);
        if let Some(later) = missing {
            self.violation(
                t,
                "lost-write",
                format!(
                    "write {write} of {cell} epoch {epoch} was acknowledged after epoch \
                     {later} had already opened without it"
                ),
            );
        }
        self.acked.insert((cell.to_string(), epoch, write));
    }

    /// **No acknowledged write is lost.** A database opened at `epoch` must
    /// hold every write acknowledged so far at that epoch or an earlier one.
    /// That is what the acknowledgement promised: whoever owns the cell next
    /// sees the write. Reopening the same epoch, after a crash, is held to
    /// the same promise.
    pub fn replica_opened(
        &mut self,
        t: u64,
        node: &str,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
    ) {
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
