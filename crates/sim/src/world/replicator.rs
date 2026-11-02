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

use celld_logic::durability::{proof_deadline, ProofProgress, ProofWait};
use celld_logic::restore::previous_epoch_reusable;
use celld_logic::{
    Epoch, Event, Failure, LocalCell, OpId, ProofSource, RestoreOutcome, RestoreSpec,
    RestoredAlarm, StopCause,
};

use super::node_log::{FollowerDisk, Upload};
use super::shell::StoreReq;
use super::{AgendaKey, Work, World, OPERATION_DEADLINE_MS};
use crate::store::Row;
use crate::{Mutation, WriteId};

/// The budget for one durability proof (`DEFAULT_DURABILITY_TIMEOUT_SECS`
/// in `ltx_repl.rs`). The gate's proof spends it as
/// `celld_logic::durability` says; an eviction's waits it out once.
const PROOF_BUDGET_MS: u64 = 10_000;

/// A node's disk: the database files no process has open.
#[derive(Default)]
pub(super) struct Disk {
    live: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// Eviction snapshots (`db.evicted`): databases an idle eviction or a
    /// cleanup closed once the bucket held all of them. The next activation
    /// here may open one in place of a bucket read.
    evicted: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// What this machine keeps as a follower of other nodes' logs.
    pub(super) log: FollowerDisk,
}

impl Disk {
    /// Delete every live file but these, the databases the process holds
    /// open (`prune_stale_live`). Eviction snapshots stay, and are still
    /// listed: the prune succeeds only if what remains is exactly `keep`.
    pub(super) fn prune_except(&mut self, keep: &[(String, Epoch)]) -> bool {
        self.live.retain(|key, _| keep.contains(key));
        self.evicted.keys().all(|key| keep.contains(key))
    }
}

/// What an epoch that ended without its final upload left
/// (`RetainedTail`): how far it may have acknowledged, and how far the
/// bucket covers it, which an upload or a bundle in flight when it ended
/// still raises.
#[derive(Clone, Copy)]
pub(super) struct Tail {
    acked: usize,
    durable: usize,
}

impl Tail {
    /// The bucket covers all it may have acknowledged: a reopen need not
    /// wait for it (`all_fragment_rows_tiered`).
    pub(super) fn tiered(&self) -> bool {
        self.durable >= self.acked
    }
}

/// A cell's open database on one node, at one epoch.
pub(super) struct Replica {
    content: Vec<WriteId>,
    /// The checker's name for this database's history (`Checker::committed`).
    lineage: usize,
    /// Length when the epoch opened: the chain it was restored from.
    start_len: usize,
    /// Length the bucket is known to hold for this epoch in its prefix.
    uploaded: usize,
    /// Length a credited bundle flush covers (`durable_txid`): durable for
    /// the output gate, but not in the per-cell layout a restore reads.
    bundled: usize,
    /// Length every follower of the process's log confirmed holding.
    fleet: usize,
    uploading: bool,
    /// The longest upload begun so far, and when it began on the monotonic
    /// clock: production's `capture_seq` and `capture_started_ms`.
    capture_len: u64,
    capture_started_ms: u64,
    /// Proofs waiting for `uploaded` to reach a position.
    waiters: Vec<Waiter>,
    /// Each waiter's pending deadline, cancelled once it is answered.
    deadlines: BTreeMap<OpId, AgendaKey>,
    /// An eviction stop waiting for its final upload: the operation that
    /// asked for it.
    stop: Option<OpId>,
    /// Whether that stop keeps the closed database as an eviction snapshot.
    snapshot: bool,
}

/// A durability proof someone is waiting on.
enum Waiter {
    /// The output gate's proof for one response (`AwaitDurable`). Its
    /// ticket is the position.
    Gate { op: OpId, wait: ProofWait },
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
            Waiter::Gate { wait, .. } => wait.ticket,
            Waiter::Evict { position, .. } => *position,
        }
    }

    /// The event that answers this proof.
    fn answer(&self, result: Result<u64, Failure>) -> Event {
        self.answer_from(result, ProofSource::Bucket)
    }

    /// The event that answers this proof, proved by `source`.
    fn answer_from(&self, result: Result<u64, Failure>, source: ProofSource) -> Event {
        match *self {
            Waiter::Gate { op, .. } => Event::DurableReached { op, result, source },
            Waiter::Evict { op, .. } => Event::DurabilityChecked {
                op,
                result: result.map(|_| ()),
            },
        }
    }
}

/// The replicator's work for one process.
pub(super) enum ReplicaWork {
    /// The bucket confirmed an upload of `len` writes.
    Uploaded {
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    /// An upload failed, or its answer was lost.
    UploadFailed { cell: String, epoch: Epoch },
    /// Upload whatever the bucket does not have yet.
    Upload { cell: String, epoch: Epoch },
    /// The replicator gives up on a proof, or on a stop's final upload.
    ProofDeadline {
        cell: String,
        epoch: Epoch,
        op: OpId,
    },
    /// The bucket answered a restore's chain read, after any fold: with
    /// the chain, or refusing the restore.
    ChainRead {
        op: OpId,
        cell: String,
        epoch: Epoch,
        chain: Result<Option<Vec<WriteId>>, Failure>,
    },
}

impl World {
    pub(super) fn replica_work(&mut self, node: usize, work: ReplicaWork) {
        match work {
            ReplicaWork::Uploaded { cell, epoch, len } => self.uploaded(node, cell, epoch, len),
            ReplicaWork::UploadFailed { cell, epoch } => self.upload_failed(node, cell, epoch),
            ReplicaWork::Upload { cell, epoch } => self.upload(node, &cell, epoch),
            ReplicaWork::ProofDeadline { cell, epoch, op } => {
                self.proof_deadline(node, cell, epoch, op)
            }
            ReplicaWork::ChainRead {
                op,
                cell,
                epoch,
                chain,
            } => self.chain_read(node, op, cell, epoch, chain),
        }
    }

    /// The databases on the node's disk, as the startup scan reports them.
    pub(super) fn local_cells(&self, node: usize) -> Vec<LocalCell> {
        let disk = &self.machines[node].disk;
        let live = disk.live.keys().map(|key| (key, true));
        let evicted = disk.evicted.keys().map(|key| (key, false));
        live.chain(evicted)
            .map(|((cell, epoch), live)| LocalCell {
                id: cell.clone(),
                epoch: *epoch,
                live,
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
            self.machines[node].disk.live.insert(key, replica.content);
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
            // A clean stop uploads everything first, then closes. All but a
            // rebalance keep the closed database as an eviction snapshot
            // (`preserve_local: !rebalance`; a cleanup is `release`).
            let Some(replica) = self.machines[node].proc_mut().replicas.get_mut(&key) else {
                return self.deliver(node, 1, Event::RuntimeStopped { op });
            };
            replica.stop = Some(op);
            replica.snapshot = cause != (StopCause::Evict { rebalance: true });
            if bounded {
                // After its deadline the stop gives up; the core restarts
                // the cell on the database it kept open.
                let (cell, epoch) = key.clone();
                self.work(
                    node,
                    OPERATION_DEADLINE_MS,
                    Work::Replica(ReplicaWork::ProofDeadline { cell, epoch, op }),
                );
            }
            self.settle_proofs(node, &key.0, epoch);
            return self.upload(node, &key.0, epoch);
        }
        if cause == StopCause::Swap {
            // The runtime leaves its isolate; its database stays open, and
            // the core starts it again at the same epoch.
            let delay = self.net_rng.range(1, 5);
            return self.deliver(node, delay, Event::RuntimeStopped { op });
        }
        if let Some(replica) = self.machines[node].proc_mut().replicas.remove(&key) {
            // Its proofs can no longer be given here.
            for waiter in &replica.waiters {
                let event = waiter.answer(Err(Failure::Ambiguous));
                self.deliver(node, 0, event);
            }
            for deadline in replica.deadlines.values() {
                self.cancel(*deadline);
            }
            // What the log shipped or a bundle covers may be acknowledged
            // and is not in the per-cell layout yet: keep it as a tail
            // (`note_undrained_tail`).
            let acked = self.shipped_len(node, &key.0, epoch).max(replica.bundled);
            if acked > replica.uploaded {
                let durable = replica.uploaded.max(replica.bundled);
                let tail = Tail { acked, durable };
                self.machines[node]
                    .proc_mut()
                    .tails
                    .insert(key.clone(), tail);
            }
            match cause {
                // The node lost its authority: close in place, with no
                // final upload. The file stays on disk.
                StopCause::Fence => {
                    self.machines[node].disk.live.insert(key, replica.content);
                }
                // A durability proof failed, so this database may hold
                // writes the bucket never got and the client was told
                // failed. Keep nothing; the next activation restores.
                StopCause::Reset => {}
                StopCause::Evict { .. } | StopCause::Cleanup | StopCause::Swap => unreachable!(),
            }
        }
        let delay = self.net_rng.range(1, 5);
        self.deliver(node, delay, Event::RuntimeStopped { op });
    }

    pub(super) fn restore(&mut self, node: usize, op: OpId, cell: String, spec: RestoreSpec) {
        if spec.resume_local {
            // A clean reload: open the predecessor's file where it is.
            let file = self.machines[node]
                .disk
                .live
                .remove(&(cell.clone(), spec.epoch));
            let delay = self.net_rng.range(1, 5);
            let Some(content) = file else {
                let result = Err(Failure::Definite);
                return self.deliver(node, delay, Event::RestoreCompleted { op, result });
            };
            self.stats.resumed += 1;
            self.judge_resume(node, &cell, spec.epoch);
            self.open_replica(node, cell.clone(), spec.epoch, content, true);
            let event = self.restore_completed(node, op, &cell, spec.epoch, false);
            return self.deliver(node, delay, event);
        }
        if spec.fresh {
            // Epoch one of a new cell: there is nothing to restore.
            self.open_replica(node, cell.clone(), spec.epoch, Vec::new(), false);
            let delay = self.net_rng.range(1, 5);
            let event = self.restore_completed(node, op, &cell, spec.epoch, false);
            self.deliver(node, delay, event);
        } else if let Some(content) = self.take_snapshot(node, &cell, &spec) {
            // The snapshot an eviction here left is the previous epoch's
            // database: renamed into place, with no bucket read and no fold.
            self.stats.reused += 1;
            self.store.reused(&cell, spec.epoch, content.len());
            self.open_replica(node, cell.clone(), spec.epoch, content, false);
            let delay = self.net_rng.range(1, 5);
            let event = self.restore_completed(node, op, &cell, spec.epoch, true);
            self.deliver(node, delay, event);
        } else {
            let epoch = spec.epoch;
            let fold = (self.machines[node].proc().tails.keys()).any(|(c, _)| *c == cell);
            let req = StoreReq::ReadChain {
                op,
                cell,
                epoch,
                fold,
            };
            self.store_call(node, req);
        }
    }

    /// The eviction snapshot a restore may open instead of reading the
    /// bucket, taken off the disk: the previous epoch's, unless the cell was
    /// taken over (`previous_epoch_reusable`).
    fn take_snapshot(
        &mut self,
        node: usize,
        cell: &str,
        spec: &RestoreSpec,
    ) -> Option<Vec<WriteId>> {
        let evicted = &mut self.machines[node].disk.evicted;
        let key = if self.config.mutation == Some(Mutation::StaleSnapshot) {
            // The planted bug takes the newest snapshot of any earlier
            // epoch, even after a takeover.
            let below = (cell.to_string(), 0)..(cell.to_string(), spec.epoch);
            evicted.range(below).next_back()?.0.clone()
        } else if previous_epoch_reusable(spec.epoch, spec.took_over) {
            (cell.to_string(), spec.epoch - 1)
        } else {
            return None;
        };
        evicted.remove(&key)
    }

    /// The bucket answered a restore. Open the database it describes.
    pub(super) fn chain_read(
        &mut self,
        node: usize,
        op: OpId,
        cell: String,
        epoch: Epoch,
        chain: Result<Option<Vec<WriteId>>, Failure>,
    ) {
        // The fold applied with the read: the cell's tails are gone, as
        // `fold_cell`'s success clears its `dirty_tails` entry before
        // `epoch_chain` runs.
        (self.machines[node].proc_mut().tails).retain(|(c, _), _| *c != cell);
        let chain = match chain {
            Ok(chain) => chain,
            Err(failure) => {
                let result = Err(failure);
                return self.drive(node, Event::RestoreCompleted { op, result });
            }
        };
        let restored = chain.is_some();
        self.stats.restored += 1;
        let chain = chain.unwrap_or_default();
        self.open_replica(node, cell.clone(), epoch, chain, false);
        let event = self.restore_completed(node, op, &cell, epoch, restored);
        self.drive(node, event);
    }

    /// Open `cell` at `epoch` with `content`: read from the bucket, or
    /// `resumed` from the predecessor's file by a clean reload.
    fn open_replica(
        &mut self,
        node: usize,
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
        resumed: bool,
    ) {
        // Opening an epoch replaces any file a dead process left for it.
        self.machines[node].disk.live.remove(&(cell.clone(), epoch));
        let name = self.machines[node].name.clone();
        let lineage = self
            .checker
            .replica_opened(self.t, &name, &cell, epoch, &content, resumed);
        let len = content.len();
        let replica = Replica {
            content,
            lineage,
            start_len: len,
            // What it opened with is already in the bucket, below this epoch.
            uploaded: len,
            bundled: 0,
            fleet: 0,
            uploading: false,
            capture_len: 0,
            capture_started_ms: 0,
            waiters: Vec::new(),
            deadlines: BTreeMap::new(),
            stop: None,
            snapshot: false,
        };
        self.machines[node]
            .proc_mut()
            .replicas
            .insert((cell, epoch), replica);
    }

    /// A handler commits one write. Returns the write and its position, or
    /// `None` when the epoch has no open database here.
    pub(super) fn commit_write(
        &mut self,
        node: usize,
        cell: &str,
        epoch: Epoch,
    ) -> Option<(WriteId, u64)> {
        let replica = self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&(cell.to_string(), epoch))?;
        let write = self.next_write;
        self.next_write += 1;
        replica.content.push(write);
        let position = replica.content.len() as u64;
        let lineage = replica.lineage;
        self.checker.committed(cell, epoch, lineage, write);
        self.ship(node, cell, epoch);
        if self.upload_due(node, Upload::Commit) {
            self.upload(node, cell, epoch);
        }
        Some((write, position))
    }

    /// What a handler answering now has seen: every write in the epoch's
    /// database. `None` when the runtime was stopped and the database closed.
    pub(super) fn seen(&self, node: usize, cell: &str, epoch: Epoch) -> Option<&[WriteId]> {
        let key = (cell.to_string(), epoch);
        let replica = self.machines[node].proc().replicas.get(&key)?;
        Some(&replica.content)
    }

    /// The epoch's database length when it opened, and now.
    /// `None` when the runtime was stopped and the database closed.
    pub(super) fn positions(&self, node: usize, cell: &str, epoch: Epoch) -> Option<(u64, u64)> {
        let replica = self.machines[node]
            .proc()
            .replicas
            .get(&(cell.to_string(), epoch))?;
        Some((replica.start_len as u64, replica.content.len() as u64))
    }

    /// Every open database's writes are all in the bucket.
    pub(super) fn all_uploaded(&self, node: usize) -> bool {
        self.machines[node]
            .proc()
            .replicas
            .values()
            .all(|replica| !replica.uploading && replica.uploaded >= replica.content.len())
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
        let wait = ProofWait {
            started_ms: self.mono(node),
            ticket: position,
            budget_ms: PROOF_BUDGET_MS,
        };
        let waiter = Waiter::Gate { op, wait };
        if self.config.mutation == Some(Mutation::AckBeforeUpload) {
            return self.deliver(node, 1, waiter.answer(Ok(position)));
        }
        self.wait_for_upload(node, cell, epoch, waiter);
    }

    /// An eviction's proof: answer once the bucket holds every write
    /// committed so far.
    pub(super) fn ensure_durable(&mut self, node: usize, op: OpId, cell: &str, epoch: Epoch) {
        let position = self.machines[node]
            .proc()
            .replicas
            .get(&(cell.to_string(), epoch))
            .map_or(0, |replica| replica.content.len() as u64);
        self.wait_for_upload(node, cell, epoch, Waiter::Evict { op, position });
    }

    /// Give a stop that has not finished its final upload back to the
    /// core, which restarts the cell on its open database.
    pub(super) fn abandon_stop(&mut self, node: usize, op: OpId) {
        let pending = self.machines[node]
            .proc_mut()
            .replicas
            .values_mut()
            .find(|replica| replica.stop == Some(op));
        if let Some(replica) = pending {
            replica.stop = None;
            self.deliver(node, 1, Event::RuntimeStopCancelled { op });
        }
    }

    fn wait_for_upload(&mut self, node: usize, cell: &str, epoch: Epoch, waiter: Waiter) {
        let evict = matches!(waiter, Waiter::Evict { .. });
        let kick = self.upload_due(node, Upload::Wait { evict });
        let Some(replica) = self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            // No open database here: there is nothing to prove from.
            let event = waiter.answer(Err(Failure::Ambiguous));
            return self.deliver(node, 1, event);
        };
        let op = waiter.op();
        replica.waiters.push(waiter);
        let deadline = Work::Replica(ReplicaWork::ProofDeadline {
            cell: cell.to_string(),
            epoch,
            op,
        });
        // A gate's deadline only ever moves later, so its first wake is at
        // the earliest it can be: one budget from now.
        let key = self.work(node, PROOF_BUDGET_MS, deadline);
        self.replica_mut(node, cell, epoch)
            .deadlines
            .insert(op, key);
        self.settle_proofs(node, cell, epoch);
        if kick {
            self.upload(node, cell, epoch);
        }
    }

    /// Every follower now holds `fleet` writes of the cell epoch.
    pub(super) fn fleet_reached(&mut self, node: usize, cell: &str, epoch: Epoch, fleet: usize) {
        let key = (cell.to_string(), epoch);
        if let Some(replica) = self.machines[node].proc_mut().replicas.get_mut(&key) {
            replica.fleet = replica.fleet.max(fleet);
            self.settle_proofs(node, cell, epoch);
        }
    }

    /// A proof, or a stop's final upload, ran out of time.
    pub(super) fn proof_deadline(&mut self, node: usize, cell: String, epoch: Epoch, op: OpId) {
        let mono = self.mono(node);
        let p = self.machines[node].proc_mut();
        let node_proof_ms = p.node_proof_ms;
        let Some(replica) = p.replicas.get_mut(&(cell.clone(), epoch)) else {
            return;
        };
        let event = if replica.stop == Some(op) {
            replica.stop = None;
            Event::RuntimeStopFailed { op }
        } else if let Some(i) = replica.waiters.iter().position(|w| w.op() == op) {
            replica.deadlines.remove(&op);
            if let Waiter::Gate { wait, .. } = &replica.waiters[i] {
                // As production does on every wake: recompute, and fail only
                // a deadline still in the past.
                let progress = ProofProgress {
                    node_proof_ms,
                    capture_seq: replica.capture_len,
                    capture_started_ms: replica.capture_started_ms,
                };
                let deadline = proof_deadline(wait, &progress);
                if mono < deadline {
                    let work = Work::Replica(ReplicaWork::ProofDeadline {
                        cell: cell.clone(),
                        epoch,
                        op,
                    });
                    let delay = self.agenda_ms(deadline - mono);
                    let key = self.work(node, delay, work);
                    self.replica_mut(node, &cell, epoch)
                        .deadlines
                        .insert(op, key);
                    return;
                }
            }
            replica.waiters.remove(i).answer(Err(Failure::Ambiguous))
        } else {
            return;
        };
        self.drive(node, event);
    }

    /// Upload the epoch's database if the bucket is behind it and no upload
    /// is in flight. One upload at a time; its answer starts the next.
    pub(super) fn upload(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let mono = self.mono(node);
        let Some(replica) = self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&(cell.to_string(), epoch))
        else {
            return;
        };
        if replica.uploading || replica.content.len() <= replica.uploaded {
            return;
        }
        replica.uploading = true;
        let len = replica.content.len() as u64;
        if len > replica.capture_len {
            // A retry of the same writes keeps the budget it already spent.
            replica.capture_len = len;
            replica.capture_started_ms = mono;
        }
        let req = StoreReq::Upload {
            cell: cell.to_string(),
            epoch,
            content: replica.content.clone(),
        };
        self.store_call(node, req);
    }

    /// A bundle flush landed and was `credited`: the gate may count the
    /// rows it carried as durable.
    pub(super) fn bundled(&mut self, node: usize, rows: Vec<(String, Epoch, usize)>) {
        let mono = self.mono(node);
        let p = self.machines[node].proc_mut();
        p.node_proof_ms = p.node_proof_ms.max(mono);
        let who = self.who(node);
        for (cell, epoch, len) in rows {
            self.checker
                .bundle_credited(self.t, &who, &cell, epoch, len);
            let key = (cell.clone(), epoch);
            let p = self.machines[node].proc_mut();
            if let Some(tail) = p.tails.get_mut(&key) {
                tail.durable = tail.durable.max(len);
            }
            if let Some(replica) = p.replicas.get_mut(&key) {
                replica.bundled = replica.bundled.max(len);
                self.settle_proofs(node, &cell, epoch);
            }
        }
    }

    /// How much of the cell epoch the bucket covers for this process: its
    /// uploads and its credited bundles (`durable_txid`).
    pub(super) fn durable_here(&self, node: usize, cell: &str, epoch: Epoch) -> usize {
        let replicas = &self.machines[node].proc().replicas;
        (replicas.get(&(cell.to_string(), epoch))).map_or(0, |r| r.uploaded.max(r.bundled))
    }

    /// The rows the next bundle flush carries: each open database with
    /// writes past what the bucket covers (`req_seq > synced_seq`).
    pub(super) fn flush_rows(&self, node: usize) -> Vec<Row> {
        (self.machines[node].proc().replicas.iter())
            .filter(|(_, r)| r.content.len() > r.uploaded.max(r.bundled))
            .map(|((cell, epoch), r)| (cell.clone(), *epoch, r.content.clone()))
            .collect()
    }

    /// An upload failed, or nobody knows whether it applied. Upload again
    /// after a backoff: an upload is idempotent.
    pub(super) fn upload_failed(&mut self, node: usize, cell: String, epoch: Epoch) {
        if let Some(replica) = self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&(cell.clone(), epoch))
        {
            replica.uploading = false;
            let delay = self.net_rng.range(50, 300);
            self.work(
                node,
                delay,
                Work::Replica(ReplicaWork::Upload { cell, epoch }),
            );
        }
    }

    /// The bucket confirmed an upload of `len` writes.
    pub(super) fn uploaded(&mut self, node: usize, cell: String, epoch: Epoch, len: usize) {
        // A proof landed: the node is making progress (`note_proof`).
        let mono = self.mono(node);
        let p = self.machines[node].proc_mut();
        p.node_proof_ms = p.node_proof_ms.max(mono);
        let key = (cell.clone(), epoch);
        if p.tails.get(&key).is_some_and(|tail| tail.acked <= len) {
            p.tails.remove(&key);
        }
        if let Some(replica) = self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&(cell.clone(), epoch))
        {
            replica.uploading = false;
            replica.uploaded = replica.uploaded.max(len);
            let stopping = replica.stop.is_some();
            self.settle_proofs(node, &cell, epoch);
            if self.upload_due(node, Upload::Landed { stopping }) {
                self.upload(node, &cell, epoch);
            }
        }
    }

    /// Every cell epoch whose database the process on `node` holds open.
    pub(super) fn open_epochs(&self, node: usize) -> Vec<(String, Epoch)> {
        self.machines[node]
            .proc()
            .replicas
            .keys()
            .cloned()
            .collect()
    }

    /// Every open database that is not stopping (`resident_epochs`).
    pub(super) fn resident_epochs(&self, node: usize) -> Vec<(String, Epoch)> {
        let replicas = &self.machines[node].proc().replicas;
        (replicas.iter())
            .filter(|(_, replica)| replica.stop.is_none())
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// The open database of `cell` at `epoch`, which the caller knows is
    /// there.
    fn replica_mut(&mut self, node: usize, cell: &str, epoch: Epoch) -> &mut Replica {
        let key = (cell.to_string(), epoch);
        self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&key)
            .expect("an open database")
    }

    /// Answer every proof the bucket now covers, and finish a stop whose
    /// final upload landed. Answers are scheduled, not driven, because this
    /// can run inside `execute`.
    fn settle_proofs(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        let Some(replica) = self.machines[node].proc_mut().replicas.get_mut(&key) else {
            return;
        };
        let uploaded = replica.uploaded as u64;
        let durable = replica.uploaded.max(replica.bundled) as u64;
        let fleet = replica.fleet as u64;
        let mut answers = Vec::new();
        let mut settled = Vec::new();
        let mut fleet_proved = false;
        replica.waiters.retain(|waiter| {
            // Either proof releases it; the fleet's is preferred, as it
            // spares the ownership read (`wait_for_durability_ticket`).
            let answer = if waiter.position() <= fleet {
                fleet_proved = true;
                waiter.answer_from(Ok(fleet), ProofSource::Fleet)
            } else if waiter.position() <= durable {
                waiter.answer(Ok(durable))
            } else {
                return true;
            };
            answers.push(answer);
            settled.extend(replica.deadlines.remove(&waiter.op()));
            false
        });
        if fleet_proved {
            self.stats.fleet_proofs += 1;
        }
        let Some(replica) = self.machines[node].proc_mut().replicas.get_mut(&key) else {
            return;
        };
        if let Some(op) = replica
            .stop
            .filter(|_| uploaded >= replica.content.len() as u64)
        {
            let replica = self.machines[node]
                .proc_mut()
                .replicas
                .remove(&key)
                .unwrap();
            for waiter in &replica.waiters {
                answers.push(waiter.answer(Err(Failure::Ambiguous)));
            }
            settled.extend(replica.deadlines.values());
            if replica.snapshot {
                self.machines[node]
                    .disk
                    .evicted
                    .insert(key, replica.content);
            }
            answers.push(Event::RuntimeStopped { op });
        }
        for deadline in settled {
            self.cancel(deadline);
        }
        for event in answers {
            self.deliver(node, 0, event);
        }
    }
}

impl World {
    /// A restore opened `cell` at `epoch`: report it with the alarm its
    /// database holds armed. Never covered, as in production: the open moved
    /// the alarm into this epoch, and no wake entry of this epoch can have
    /// landed yet.
    fn restore_completed(
        &self,
        node: usize,
        op: OpId,
        cell: &str,
        epoch: Epoch,
        restored: bool,
    ) -> Event {
        let alarm = self
            .seen(node, cell, epoch)
            .and_then(|content| self.reported(self.alarm_of(content, epoch)).at_ms())
            .map(|at_ms| RestoredAlarm {
                at_ms,
                covered: false,
            });
        Event::RestoreCompleted {
            op,
            result: Ok(RestoreOutcome { restored, alarm }),
        }
    }
}
