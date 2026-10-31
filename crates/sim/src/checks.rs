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
    /// The one node that ran each cell epoch, and the one that uploaded
    /// into its prefix.
    runtimes: BTreeMap<(String, Epoch), String>,
    uploaders: BTreeMap<(String, Epoch), String>,
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
    pub fn runtime_started(&mut self, t: u64, node: &str, cell: &str, epoch: Epoch) {
        if let Some(first) = first_writer(&mut self.runtimes, node, cell, epoch) {
            self.violation(
                t,
                "two-writers",
                format!("{node} ran {cell} epoch {epoch}, which {first} already ran"),
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
    pub fn upload_applied(&mut self, t: u64, node: &str, cell: &str, epoch: Epoch) {
        if let Some(first) = first_writer(&mut self.uploaders, node, cell, epoch) {
            self.violation(
                t,
                "two-writers",
                format!("{node} uploaded into {cell} epoch {epoch}, which {first} already wrote"),
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

    /// **Nothing is revealed before it is durable.** When a response leaves,
    /// the bucket must already hold everything it can reveal, in its epoch's
    /// prefix:
    ///
    /// - a write's response reveals that write, at the position it was
    ///   committed at;
    /// - a read reveals every write up to the position it `observed`.
    ///
    /// An acknowledged write is remembered for [`Self::replica_opened`].
    pub fn response_released(
        &mut self,
        t: u64,
        store: &Store,
        cell: &str,
        epoch: Epoch,
        committed: Option<(WriteId, u64)>,
        observed: Option<u64>,
    ) {
        let prefix = store.prefix(cell, epoch);
        if let Some((write, position)) = committed {
            self.acked.insert((cell.to_string(), epoch, write));
            let at = position.checked_sub(1).and_then(|i| prefix.get(i as usize));
            if at != Some(&write) {
                self.violation(
                    t,
                    "ack-before-durable",
                    format!(
                        "write {write} of {cell} epoch {epoch} was acknowledged, but the bucket \
                         does not hold it at position {position}"
                    ),
                );
            }
        }
        if let Some(observed) = observed {
            if (prefix.len() as u64) < observed {
                self.violation(
                    t,
                    "read-before-durable",
                    format!(
                        "a read of {cell} epoch {epoch} saw position {observed} and was \
                         released, but the bucket holds only {}",
                        prefix.len()
                    ),
                );
            }
        }
    }

    /// **No acknowledged write is lost.** A database opened at `epoch` must
    /// hold every write acknowledged at an earlier epoch of the cell. This is
    /// what the acknowledgement promised: whoever owns the cell next sees
    /// the write.
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
            .range((cell.to_string(), 0, 0)..(cell.to_string(), epoch, 0))
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
