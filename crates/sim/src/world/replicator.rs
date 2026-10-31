//! The replicator: each node's open databases, their uploads to the bucket,
//! and the durability proofs the output gate waits on. It follows
//! `crates/celld/ltx_repl.rs`.
//!
//! A cell's database is a list of write ids. An upload carries the whole
//! list into the epoch's prefix, and one upload runs at a time per epoch.

use celld_logic::{Epoch, Event, OpId, ProofSource, RestoreOutcome, RestoreSpec};

use super::shell::StoreReq;
use super::World;
use crate::WriteId;

/// A cell's open database on one node, at one epoch.
pub(super) struct Replica {
    content: Vec<WriteId>,
    /// Length when the epoch opened: the chain it was restored from.
    start_len: usize,
    /// Length the bucket is known to hold for this epoch.
    uploaded: usize,
    uploading: bool,
    /// Gate proofs waiting for `uploaded` to reach a position.
    waiters: Vec<(OpId, u64)>,
}

impl World {
    pub(super) fn restore(&mut self, node: usize, op: OpId, cell: String, spec: RestoreSpec) {
        if spec.fresh {
            // Epoch one of a new cell: there is nothing to restore.
            self.open_replica(node, cell, spec.epoch, Vec::new());
            let delay = self.net_rng.range(1, 5);
            self.deliver(node, delay, restore_completed(op, false));
        } else {
            // No disk yet, so no local snapshot to reuse: always read the
            // chain from the bucket.
            let epoch = spec.epoch;
            self.store_call(node, StoreReq::ReadChain { op, cell, epoch });
        }
    }

    /// The bucket answered a restore. Open the database it describes.
    pub(super) fn chain_read(
        &mut self,
        node: usize,
        op: OpId,
        cell: String,
        epoch: Epoch,
        chain: Option<Vec<WriteId>>,
    ) {
        let restored = chain.is_some();
        self.open_replica(node, cell, epoch, chain.unwrap_or_default());
        self.drive(node, restore_completed(op, restored));
    }

    fn open_replica(&mut self, node: usize, cell: String, epoch: Epoch, content: Vec<WriteId>) {
        let name = self.nodes[node].name.clone();
        self.checker
            .replica_opened(self.t, &name, &cell, epoch, &content);
        let len = content.len();
        let replica = Replica {
            content,
            start_len: len,
            // What it opened with is already in the bucket, below this epoch.
            uploaded: len,
            uploading: false,
            waiters: Vec::new(),
        };
        self.nodes[node].replicas.insert((cell, epoch), replica);
    }

    /// A handler commits one write. Returns the write and its position, or
    /// `None` when the epoch has no open database here.
    pub(super) fn commit_write(
        &mut self,
        node: usize,
        cell: &str,
        epoch: Epoch,
    ) -> Option<(WriteId, u64)> {
        let replica = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))?;
        let write = self.next_write;
        self.next_write += 1;
        replica.content.push(write);
        let position = replica.content.len() as u64;
        self.upload(node, cell, epoch);
        Some((write, position))
    }

    /// The epoch's database length when it opened, and now: what a handler
    /// answering now has seen.
    pub(super) fn positions(&self, node: usize, cell: &str, epoch: Epoch) -> (u64, u64) {
        let replica = &self.nodes[node].replicas[&(cell.to_string(), epoch)];
        (replica.start_len as u64, replica.content.len() as u64)
    }

    /// Wait for the bucket to hold `position`. Returns false when the epoch
    /// has no open database here.
    pub(super) fn wait_for_upload(
        &mut self,
        node: usize,
        cell: &str,
        epoch: Epoch,
        op: OpId,
        position: u64,
    ) -> bool {
        let Some(replica) = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            return false;
        };
        replica.waiters.push((op, position));
        self.settle_waiters(node, cell, epoch);
        self.upload(node, cell, epoch);
        true
    }

    /// Upload the epoch's database if the bucket is behind it and no upload
    /// is in flight. One upload at a time; its answer starts the next.
    fn upload(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let Some(replica) = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            return;
        };
        if replica.uploading || replica.content.len() <= replica.uploaded {
            return;
        }
        replica.uploading = true;
        let req = StoreReq::Upload {
            cell: cell.to_string(),
            epoch,
            content: replica.content.clone(),
        };
        self.store_call(node, req);
    }

    /// The bucket confirmed an upload of `len` writes.
    pub(super) fn uploaded(&mut self, node: usize, cell: String, epoch: Epoch, len: usize) {
        if let Some(replica) = self.nodes[node].replicas.get_mut(&(cell.clone(), epoch)) {
            replica.uploading = false;
            replica.uploaded = replica.uploaded.max(len);
            self.settle_waiters(node, &cell, epoch);
            self.upload(node, &cell, epoch);
        }
    }

    /// Answer every gate proof the bucket now covers. The answer is
    /// scheduled, not driven, because this can run inside `execute`.
    fn settle_waiters(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let Some(replica) = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            return;
        };
        let uploaded = replica.uploaded as u64;
        let mut proven = Vec::new();
        replica.waiters.retain(|&(op, position)| {
            let covered = position <= uploaded;
            if covered {
                proven.push(op);
            }
            !covered
        });
        for op in proven {
            let event = Event::DurableReached {
                op,
                result: Ok(uploaded),
                source: ProofSource::Bucket,
            };
            self.deliver(node, 0, event);
        }
    }
}

fn restore_completed(op: OpId, restored: bool) -> Event {
    Event::RestoreCompleted {
        op,
        result: Ok(RestoreOutcome {
            restored,
            alarm: None,
        }),
    }
}
