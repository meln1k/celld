//! The oracle.
//!
//! The world tells the checker facts as they happen: a claim applied at the
//! bucket, a runtime started, a client told its write succeeded. The checker
//! keeps the few records it needs (ghost state no node can see) and tests
//! each property at the moment its fact arrives. The shell never decides
//! whether something is a violation; every property lives in this file.

use std::collections::BTreeSet;

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
    /// Every ownership claim the bucket applied: (cell, epoch, node).
    claims: BTreeSet<(String, Epoch, String)>,
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
    pub fn runtime_started(&mut self, t: u64, node: &str, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch, node.to_string());
        if !self.claims.contains(&key) {
            self.violation(
                t,
                "unclaimed-runtime",
                format!("{node} started {cell} epoch {epoch} without an applied claim of its own"),
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

    /// **No acknowledgement before durability.** The instant a client is
    /// told a write succeeded, the write must already be in its epoch's
    /// prefix in the bucket, at the position it was committed at. If the
    /// node died right now, a restore would still find it.
    pub fn write_acked(
        &mut self,
        t: u64,
        store: &Store,
        cell: &str,
        epoch: Epoch,
        write: WriteId,
        position: u64,
    ) {
        let durable = store.prefix(cell, epoch).get(position as usize - 1) == Some(&write);
        if !durable {
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
}
