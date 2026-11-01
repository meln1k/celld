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
use celld_logic::{
    Epoch, Event, Failure, LocalCell, OpId, ProofSource, RestoreOutcome, RestoreSpec,
    RestoredAlarm, StopCause,
};

use super::shell::StoreReq;
use super::{AgendaKey, Work, World, OPERATION_DEADLINE_MS};
use crate::{Mutation, WriteId};

/// The budget for one durability proof (`DEFAULT_DURABILITY_TIMEOUT_SECS`
/// in `ltx_repl.rs`). The gate's proof spends it as
/// `celld_logic::durability` says; an eviction's waits it out once.
const PROOF_BUDGET_MS: u64 = 10_000;

/// A node's disk: the database files no process has open.
#[derive(Default)]
pub(super) struct Disk {
    live: BTreeMap<(String, Epoch), Vec<WriteId>>,
}

impl Disk {
    /// Delete every file but these (`prune_stale_live`).
    pub(super) fn prune_except(&mut self, keep: &[(String, Epoch)]) {
        self.live.retain(|key, _| keep.contains(key));
    }
}

/// A cell's open database on one node, at one epoch.
pub(super) struct Replica {
    content: Vec<WriteId>,
    /// The checker's name for this database's history (`Checker::committed`).
    lineage: usize,
    /// Length when the epoch opened: the chain it was restored from.
    start_len: usize,
    /// Length the bucket is known to hold for this epoch.
    uploaded: usize,
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
    /// The bucket answered a restore's chain read.
    ChainRead {
        op: OpId,
        cell: String,
        epoch: Epoch,
        chain: Option<Vec<WriteId>>,
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
        self.machines[node]
            .disk
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
            // A clean stop uploads everything first, then closes. The
            // eviction snapshot production keeps on disk serves only sticky
            // ownership, which is not modeled: a released cell is restored
            // from the bucket by whoever takes it next.
            let Some(replica) = self.machines[node].proc_mut().replicas.get_mut(&key) else {
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
            let name = self.machines[node].name.clone();
            let owner = self.store.owner(&cell);
            if owner != Some((Some(name.as_str()), spec.epoch)) {
                let who = self.who(node);
                let owner = format!("{owner:?}");
                self.checker.resumed(self.t, &who, &cell, spec.epoch, owner);
            }
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
            uploading: false,
            capture_len: 0,
            capture_started_ms: 0,
            waiters: Vec::new(),
            deadlines: BTreeMap::new(),
            stop: None,
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
        self.upload(node, cell, epoch);
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
        self.upload(node, cell, epoch);
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
        if let Some(replica) = self.machines[node]
            .proc_mut()
            .replicas
            .get_mut(&(cell.clone(), epoch))
        {
            replica.uploading = false;
            replica.uploaded = replica.uploaded.max(len);
            self.settle_proofs(node, &cell, epoch);
            self.upload(node, &cell, epoch);
        }
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
        let mut answers = Vec::new();
        let mut settled = Vec::new();
        replica.waiters.retain(|waiter| {
            let covered = waiter.position() <= uploaded;
            if covered {
                answers.push(waiter.answer(Ok(uploaded)));
                settled.extend(replica.deadlines.remove(&waiter.op()));
            }
            !covered
        });
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
