//! The replicator: each node's open databases, their uploads to the bucket,
//! and the durability proofs the output gate waits on. It follows
//! `crates/celld/ltx_repl.rs`.
//!
//! A cell's database is a list of write ids. An upload carries the whole
//! list into the epoch's prefix, and one upload runs at a time per epoch.
//!
//! An open database lives in its process; the files under it live on the
//! node's disk. A process that dies leaves its databases on the disk just
//! as they were, with whatever it committed and never uploaded.

use std::collections::BTreeMap;

use celld_logic::{
    Epoch, Event, Failure, LocalCell, OpId, ProofSource, RestoreOutcome, RestoreSpec, StopCause,
};

use super::shell::StoreReq;
use super::{Work, World, OPERATION_DEADLINE_MS};
use crate::{Mutation, WriteId};

/// How long the replicator works on one durability proof before it says it
/// could not give it.
const PROOF_DEADLINE_MS: u64 = 10_000;

/// A node's disk: the database files no process has open.
#[derive(Default)]
pub(super) struct Disk {
    live: BTreeMap<(String, Epoch), Vec<WriteId>>,
}

/// A cell's open database on one node, at one epoch.
pub(super) struct Replica {
    content: Vec<WriteId>,
    /// Length when the epoch opened: the chain it was restored from.
    start_len: usize,
    /// Length the bucket is known to hold for this epoch.
    uploaded: usize,
    uploading: bool,
    /// Proofs waiting for `uploaded` to reach a position.
    waiters: Vec<Waiter>,
    /// An eviction stop waiting for its final upload: the operation that
    /// asked for it.
    stop: Option<OpId>,
}

/// A durability proof someone is waiting on.
enum Waiter {
    /// The output gate's proof for one response (`AwaitDurable`).
    Gate { op: OpId, position: u64 },
    /// An eviction's proof that everything so far is durable
    /// (`EnsureDurable`).
    Evict { op: OpId, position: u64 },
}

impl Waiter {
    fn op(&self) -> OpId {
        match self {
            Waiter::Gate { op, .. } | Waiter::Evict { op, .. } => *op,
        }
    }

    fn position(&self) -> u64 {
        match self {
            Waiter::Gate { position, .. } | Waiter::Evict { position, .. } => *position,
        }
    }

    /// The event that answers this proof.
    fn answer(&self, result: Result<u64, Failure>) -> Event {
        match *self {
            Waiter::Gate { op, .. } => Event::DurableReached {
                op,
                result,
                source: ProofSource::Bucket,
            },
            Waiter::Evict { op, .. } => Event::DurabilityChecked {
                op,
                result: result.map(|_| ()),
            },
        }
    }
}

impl World {
    /// The databases on the node's disk, as the startup scan reports them.
    pub(super) fn local_cells(&self, node: usize) -> Vec<LocalCell> {
        self.disks[node]
            .live
            .keys()
            .map(|(cell, epoch)| LocalCell {
                id: cell.clone(),
                epoch: *epoch,
                live: true,
            })
            .collect()
    }

    /// A dead process's open databases go back to the disk as they were.
    pub(super) fn close_replicas(
        &mut self,
        node: usize,
        replicas: BTreeMap<(String, Epoch), Replica>,
    ) {
        for (key, replica) in replicas {
            self.disks[node].live.insert(key, replica.content);
        }
    }

    /// Stop a cell's runtime and close its database, as `cause` says.
    pub(super) fn stop_runtime(
        &mut self,
        node: usize,
        op: OpId,
        cell: String,
        epoch: Epoch,
        cause: StopCause,
        bounded: bool,
    ) {
        let key = (cell, epoch);
        if let StopCause::Evict { .. } | StopCause::Cleanup = cause {
            // A clean stop uploads everything first, then closes. The
            // eviction snapshot production keeps on disk serves only sticky
            // ownership, which is not modeled: a released cell is restored
            // from the bucket by whoever takes it next.
            let Some(replica) = self.nodes[node].replicas.get_mut(&key) else {
                return self.deliver(node, 1, Event::RuntimeStopped { op });
            };
            replica.stop = Some(op);
            if bounded {
                // After its deadline the stop gives up; the core restarts
                // the cell on the database it kept open.
                let (cell, epoch) = key.clone();
                self.work(
                    node,
                    OPERATION_DEADLINE_MS,
                    Work::ProofDeadline { cell, epoch, op },
                );
            }
            self.settle_waiters(node, &key.0, epoch);
            return self.upload(node, &key.0, epoch);
        }
        if let Some(replica) = self.nodes[node].replicas.remove(&key) {
            // Its proofs can no longer be given here.
            for waiter in &replica.waiters {
                let event = waiter.answer(Err(Failure::Ambiguous));
                self.deliver(node, 0, event);
            }
            match cause {
                // The node lost its authority: close in place, with no
                // final upload. The file stays on disk.
                StopCause::Fence => {
                    self.disks[node].live.insert(key, replica.content);
                }
                // A durability proof failed, so this database may hold
                // writes the bucket never got and the client was told
                // failed. Keep nothing; the next activation restores.
                StopCause::Reset => {}
                StopCause::Swap => unimplemented!("generations are not modeled"),
                StopCause::Evict { .. } | StopCause::Cleanup => unreachable!(),
            }
        }
        let delay = self.net_rng.range(1, 5);
        self.deliver(node, delay, Event::RuntimeStopped { op });
    }

    pub(super) fn restore(&mut self, node: usize, op: OpId, cell: String, spec: RestoreSpec) {
        assert!(!spec.resume_local, "no clean reload is modeled: {spec:?}");
        if spec.fresh {
            // Epoch one of a new cell: there is nothing to restore.
            self.open_replica(node, cell, spec.epoch, Vec::new());
            let delay = self.net_rng.range(1, 5);
            self.deliver(node, delay, restore_completed(op, false));
        } else {
            // No eviction snapshots yet, so nothing on disk is ever a safe
            // source: always read the chain from the bucket.
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
        self.stats.restored += 1;
        self.open_replica(node, cell, epoch, chain.unwrap_or_default());
        self.drive(node, restore_completed(op, restored));
    }

    fn open_replica(&mut self, node: usize, cell: String, epoch: Epoch, content: Vec<WriteId>) {
        // Opening an epoch replaces any file a dead process left for it.
        self.disks[node].live.remove(&(cell.clone(), epoch));
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
            stop: None,
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
    /// `None` when the runtime was stopped and the database closed.
    pub(super) fn positions(&self, node: usize, cell: &str, epoch: Epoch) -> Option<(u64, u64)> {
        let replica = self.nodes[node].replicas.get(&(cell.to_string(), epoch))?;
        Some((replica.start_len as u64, replica.content.len() as u64))
    }

    /// The output gate's proof: answer once the bucket holds `position`.
    pub(super) fn await_durable(
        &mut self,
        node: usize,
        op: OpId,
        cell: &str,
        epoch: Epoch,
        position: u64,
    ) {
        if self.config.mutation == Some(Mutation::AckBeforeUpload) {
            let event = Waiter::Gate { op, position }.answer(Ok(position));
            return self.deliver(node, 1, event);
        }
        self.wait_for_upload(node, cell, epoch, Waiter::Gate { op, position });
    }

    /// An eviction's proof: answer once the bucket holds every write
    /// committed so far.
    pub(super) fn ensure_durable(&mut self, node: usize, op: OpId, cell: &str, epoch: Epoch) {
        let position = self.nodes[node]
            .replicas
            .get(&(cell.to_string(), epoch))
            .map_or(0, |replica| replica.content.len() as u64);
        self.wait_for_upload(node, cell, epoch, Waiter::Evict { op, position });
    }

    /// Give a stop that has not finished its final upload back to the
    /// core, which restarts the cell on its open database.
    pub(super) fn abandon_stop(&mut self, node: usize, op: OpId) {
        let pending = self.nodes[node]
            .replicas
            .values_mut()
            .find(|replica| replica.stop == Some(op));
        if let Some(replica) = pending {
            replica.stop = None;
            self.deliver(node, 1, Event::RuntimeStopCancelled { op });
        }
    }

    fn wait_for_upload(&mut self, node: usize, cell: &str, epoch: Epoch, waiter: Waiter) {
        let Some(replica) = self.nodes[node]
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            // No open database here: there is nothing to prove from.
            let event = waiter.answer(Err(Failure::Ambiguous));
            return self.deliver(node, 1, event);
        };
        let op = waiter.op();
        replica.waiters.push(waiter);
        let deadline = Work::ProofDeadline {
            cell: cell.to_string(),
            epoch,
            op,
        };
        self.work(node, PROOF_DEADLINE_MS, deadline);
        self.settle_waiters(node, cell, epoch);
        self.upload(node, cell, epoch);
    }

    /// A proof, or a stop's final upload, ran out of time.
    pub(super) fn proof_deadline(&mut self, node: usize, cell: String, epoch: Epoch, op: OpId) {
        let Some(replica) = self.nodes[node].replicas.get_mut(&(cell, epoch)) else {
            return;
        };
        let event = if replica.stop == Some(op) {
            replica.stop = None;
            Event::RuntimeStopFailed { op }
        } else if let Some(i) = replica.waiters.iter().position(|w| w.op() == op) {
            replica.waiters.remove(i).answer(Err(Failure::Ambiguous))
        } else {
            return;
        };
        self.drive(node, event);
    }

    /// Upload the epoch's database if the bucket is behind it and no upload
    /// is in flight. One upload at a time; its answer starts the next.
    pub(super) fn upload(&mut self, node: usize, cell: &str, epoch: Epoch) {
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

    /// An upload failed, or nobody knows whether it applied. Upload again
    /// after a backoff: an upload is idempotent.
    pub(super) fn upload_failed(&mut self, node: usize, cell: String, epoch: Epoch) {
        if let Some(replica) = self.nodes[node].replicas.get_mut(&(cell.clone(), epoch)) {
            replica.uploading = false;
            let delay = self.net_rng.range(50, 300);
            self.work(node, delay, Work::Upload { cell, epoch });
        }
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

    /// Answer every proof the bucket now covers, and finish a stop whose
    /// final upload landed. Answers are scheduled, not driven, because this
    /// can run inside `execute`.
    fn settle_waiters(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        let Some(replica) = self.nodes[node].replicas.get_mut(&key) else {
            return;
        };
        let uploaded = replica.uploaded as u64;
        let mut answers = Vec::new();
        replica.waiters.retain(|waiter| {
            let covered = waiter.position() <= uploaded;
            if covered {
                answers.push(waiter.answer(Ok(uploaded)));
            }
            !covered
        });
        if let Some(op) = replica
            .stop
            .filter(|_| uploaded >= replica.content.len() as u64)
        {
            let replica = self.nodes[node].replicas.remove(&key).unwrap();
            for waiter in &replica.waiters {
                answers.push(waiter.answer(Err(Failure::Ambiguous)));
            }
            answers.push(Event::RuntimeStopped { op });
        }
        for event in answers {
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
