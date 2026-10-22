//! A process's open databases: restore, the runtime on top, and the
//! replicator that uploads each epoch's writes and proves them durable
//! (`crates/celld/ltx_repl.rs`).

use super::*;

/// A durability proof someone waits on: the replica must be durable
/// through `position`.
#[derive(Debug)]
pub(super) enum Waiter {
    /// `AwaitDurable`, an output gate: a fleet proof, or a bucket proof from
    /// an upload or a credited bundle.
    Await { op: OpId, position: u64 },
    /// `EnsureDurable`, before an eviction: the cell's own prefix only.
    Ensure { op: OpId, position: u64 },
    /// Wake maintenance proving an alarm source durable, by any proof.
    Task { task: u64, position: u64 },
}

/// What a core operation waits to see durable.
#[derive(Clone, Copy, Debug)]
pub(super) enum Proof {
    /// An output gate: the write at `position`.
    Output { position: u64 },
    /// An eviction: everything the replica holds now.
    Everything,
}

/// What a resolved waiter becomes.
enum Resolution {
    Event(Event),
    Task { task: u64, ok: bool },
}

impl Waiter {
    fn position(&self) -> u64 {
        match self {
            Waiter::Await { position, .. }
            | Waiter::Ensure { position, .. }
            | Waiter::Task { position, .. } => *position,
        }
    }

    /// The core operation a durability deadline fails. A task keeps its own.
    fn op(&self) -> Option<OpId> {
        match self {
            Waiter::Await { op, .. } | Waiter::Ensure { op, .. } => Some(*op),
            Waiter::Task { .. } => None,
        }
    }

    /// The proof the replica gives this waiter now, if any.
    fn proof(&self, replica: &Replica) -> Option<(u64, ProofSource)> {
        let position = self.position() as usize;
        let bucket = replica.bucket();
        let proven = match self {
            // The ticket race prefers the fleet.
            Waiter::Await { .. } if replica.fleet >= position => {
                (replica.fleet, ProofSource::Fleet)
            }
            Waiter::Await { .. } => (bucket, ProofSource::Bucket),
            Waiter::Ensure { .. } => (replica.uploaded, ProofSource::Bucket),
            Waiter::Task { .. } => (bucket.max(replica.fleet), ProofSource::Bucket),
        };
        (proven.0 >= position).then_some((proven.0 as u64, proven.1))
    }

    /// The answer, given the proof or why there never will be one.
    fn resolve(self, proof: Result<(u64, ProofSource), Failure>) -> Resolution {
        match self {
            Waiter::Await { op, .. } => {
                let (result, source) = match proof {
                    Ok((position, source)) => (Ok(position), source),
                    Err(failure) => (Err(failure), ProofSource::Bucket),
                };
                Resolution::Event(Event::DurableReached { op, result, source })
            }
            Waiter::Ensure { op, .. } => Resolution::Event(Event::DurabilityChecked {
                op,
                result: proof.map(|_| ()),
            }),
            Waiter::Task { task, .. } => Resolution::Task {
                task,
                ok: proof.is_ok(),
            },
        }
    }
}

impl Replica {
    /// Position the bucket proves: the cell's own uploads, or a credited
    /// bundle.
    pub(super) fn bucket(&self) -> usize {
        self.uploaded.max(self.bundled)
    }

    /// A credited bundle carries `position`, which extends the bucket
    /// proof if it continues the bucket's run. Returns whether it did.
    pub(super) fn credit_bundle(&mut self, position: usize) -> bool {
        let extends = position <= self.bucket() + 1;
        if extends {
            self.bundled = self.bundled.max(position);
        }
        extends
    }

    /// Every ensemble member confirmed `position`, which extends the fleet
    /// proof if it continues a proven run. Returns whether it did.
    pub(super) fn credit_fleet(&mut self, position: usize) -> bool {
        let extends = position <= self.bucket().max(self.fleet) + 1;
        if extends {
            self.fleet = self.fleet.max(position);
        }
        extends
    }
}

impl World {
    // -----------------------------------------------------------------------
    // Restore, runtime, replication

    pub(super) fn restore(&mut self, id: ProcId, op: OpId, cell: String, spec: RestoreSpec) {
        let node = self.procs[&id].node;
        let epoch = spec.epoch;
        let disk = &mut self.nodes[node].disk;
        let snapshot = if spec.fresh || spec.resume_local {
            None
        } else if self.config.mutation == Mutation::ReuseOlderSnapshot {
            disk.evicted
                .range((cell.clone(), 0)..(cell.clone(), epoch))
                .next_back()
                .map(|(key, _)| key.clone())
        } else if celld_logic::restore::previous_epoch_reusable(epoch, spec.took_over) {
            Some((cell.clone(), epoch - 1)).filter(|key| disk.evicted.contains_key(key))
        } else {
            None
        };
        let delay = self.net_rng.range(1, 5);
        if spec.resume_local {
            let content = disk
                .live
                .get(&(cell.clone(), epoch))
                .cloned()
                .unwrap_or_default();
            self.stats.restores_local += 1;
            let alarm = self.restored_alarm(&content);
            self.open_replica(id, &cell, epoch, content, "resume");
            self.restore_done(id, op, delay, true, alarm);
        } else if let Some(key) = snapshot {
            let content = disk.evicted.remove(&key).unwrap();
            self.stats.restores_local += 1;
            let alarm = self.restored_alarm(&content);
            self.open_replica(id, &cell, epoch, content, "snapshot");
            self.restore_done(id, op, delay, true, alarm);
        } else if !spec.fresh && self.undrained(id, &cell) {
            self.fold(id, op, cell, spec);
        } else if !spec.fresh {
            self.store_call(
                id,
                StoreReq::ReadChain {
                    cell: cell.clone(),
                    epoch,
                },
                Reply::Restore { op, cell, spec },
            );
        } else {
            self.stats.restores_fresh += 1;
            self.open_replica(id, &cell, epoch, Vec::new(), "fresh");
            self.restore_done(id, op, delay, false, None);
        }
    }

    fn restore_done(
        &mut self,
        id: ProcId,
        op: OpId,
        delay: u64,
        restored: bool,
        alarm: Option<celld_logic::RestoredAlarm>,
    ) {
        self.work(
            id,
            delay,
            Work::Deliver(Event::RestoreCompleted {
                op,
                result: Ok(RestoreOutcome { restored, alarm }),
            }),
        );
    }

    pub(super) fn open_replica(
        &mut self,
        id: ProcId,
        cell: &str,
        epoch: Epoch,
        content: Vec<WriteId>,
        source: &'static str,
    ) {
        self.checker
            .on_restore(self.t, id, cell, epoch, source, &content);
        let node = self.procs[&id].node;
        self.nodes[node]
            .disk
            .live
            .remove(&(cell.to_string(), epoch));
        let len = content.len();
        self.procs.get_mut(&id).unwrap().replicas.insert(
            (cell.to_string(), epoch),
            Replica {
                content,
                start_len: len,
                // The opening content is the chain this epoch was restored
                // from, already in the bucket below this epoch.
                uploaded: len,
                bundled: len,
                fleet: len,
                queued: len,
                uploading: false,
                running: false,
                waiters: Vec::new(),
                stop: None,
            },
        );
    }

    pub(super) fn start_runtime(&mut self, id: ProcId, op: OpId, cell: String, epoch: Epoch) {
        let key = (cell.clone(), epoch);
        let (checker, view) = self.checks();
        checker.on_runtime_start(view, id, &cell, epoch);
        let generation = self.procs[&id].state.current_generation();
        let proc = self.procs.get_mut(&id).unwrap();
        let result = match proc.replicas.get_mut(&key) {
            Some(replica) => {
                replica.running = true;
                Ok(())
            }
            None => Err(Failure::Definite),
        };
        let delay = self.net_rng.range(1, 10);
        if result.is_ok() {
            // The isolate adopts the database and reports its alarm first.
            self.observe_opened(id, &cell, epoch, delay);
        }
        self.work(
            id,
            delay,
            Work::Deliver(Event::RuntimeStarted {
                op,
                isolate: None,
                generation,
                result,
            }),
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn stop_runtime(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: String,
        epoch: Epoch,
        cause: StopCause,
        bounded: bool,
        immediate: &mut VecDeque<Event>,
    ) {
        let key = (cell.clone(), epoch);
        let node = self.procs[&id].node;
        let proc = self.procs.get_mut(&id).unwrap();
        proc.published.remove(&cell);
        let Some(replica) = proc.replicas.get_mut(&key) else {
            immediate.push_back(Event::RuntimeStopped { op });
            return;
        };
        replica.running = false;
        match cause {
            StopCause::Swap => immediate.push_back(Event::RuntimeStopped { op }),
            StopCause::Fence => {
                // Close in place: no durability pass, the file stays live.
                let replica = proc.replicas.remove(&key).unwrap();
                self.note_undrained_tail(id, &cell, &replica);
                self.nodes[node].disk.live.insert(key, replica.content);
                let delay = self.net_rng.range(1, 5);
                self.work(id, delay, Work::Deliver(Event::RuntimeStopped { op }));
            }
            StopCause::Reset => {
                let replica = proc.replicas.remove(&key).unwrap();
                self.note_undrained_tail(id, &cell, &replica);
                let delay = self.net_rng.range(1, 5);
                self.work(id, delay, Work::Deliver(Event::RuntimeStopped { op }));
            }
            StopCause::Cleanup | StopCause::Evict { .. } => {
                self.begin_stop(id, op, &cell, epoch, cause, bounded);
            }
        }
    }

    pub(super) fn kick_upload(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        let Some(replica) = self.procs.get(&id).and_then(|proc| proc.replicas.get(&key)) else {
            return;
        };
        if replica.uploading
            || replica.content.len() <= replica.uploaded
            || !self.per_cell_sync(id, replica)
        {
            return;
        }
        let replica = self
            .procs
            .get_mut(&id)
            .unwrap()
            .replicas
            .get_mut(&key)
            .unwrap();
        replica.uploading = true;
        let content = replica.content.clone();
        let len = content.len();
        self.store_call(
            id,
            StoreReq::Put {
                cell: cell.to_string(),
                epoch,
                content,
            },
            Reply::Upload {
                cell: cell.to_string(),
                epoch,
                len,
            },
        );
    }

    /// `op` waits for `cell` at `epoch` to be durable through `proof`, or
    /// fails with `missing` when no replica holds that epoch.
    pub(super) fn await_durable(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: &str,
        epoch: Epoch,
        proof: Proof,
        missing: Failure,
    ) {
        let proc = self.procs.get_mut(&id).unwrap();
        let replica = proc.replicas.get_mut(&(cell.to_string(), epoch));
        let waiter = match proof {
            Proof::Output { position } => Waiter::Await { op, position },
            Proof::Everything => Waiter::Ensure {
                op,
                position: replica
                    .as_ref()
                    .map_or(0, |replica| replica.content.len() as u64),
            },
        };
        let Some(replica) = replica else {
            if let Resolution::Event(event) = waiter.resolve(Err(missing)) {
                self.work(id, 1, Work::Deliver(event));
            }
            return;
        };
        replica.waiters.push(waiter);
        self.work(
            id,
            DURABILITY_TIMEOUT_MS,
            Work::WaiterDeadline {
                cell: cell.to_string(),
                epoch,
                op,
            },
        );
        self.settle_waiters(id, cell, epoch);
        self.kick_upload(id, cell, epoch);
    }

    pub(super) fn settle_waiters(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let key = (cell.to_string(), epoch);
        let node = self.procs[&id].node;
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(replica) = proc.replicas.get_mut(&key) else {
            return;
        };
        let mut resolved = Vec::new();
        let mut fleet_gates = Vec::new();
        for waiter in std::mem::take(&mut replica.waiters) {
            match waiter.proof(replica) {
                Some(proof) => {
                    if let (Waiter::Await { op, .. }, ProofSource::Fleet) = (&waiter, proof.1) {
                        self.stats.fleet_acks += 1;
                        fleet_gates.push(*op);
                    }
                    resolved.push(waiter.resolve(Ok(proof)));
                }
                None => replica.waiters.push(waiter),
            }
        }
        let stopped = replica.stop.as_ref().is_some_and(Stop::publishing)
            && replica.uploaded >= replica.content.len();
        if stopped {
            let replica = proc.replicas.remove(&key).unwrap();
            let stop = replica.stop.unwrap();
            let disk = &mut self.nodes[node].disk;
            disk.live.remove(&key);
            if stop.preserve {
                disk.evicted.insert(key, replica.content);
            }
            resolved.push(Resolution::Event(Event::RuntimeStopped { op: stop.op }));
            // Waiters on a stopped replica can no longer be proven here.
            resolved.extend(
                replica
                    .waiters
                    .into_iter()
                    .map(|waiter| waiter.resolve(Err(Failure::Ambiguous))),
            );
        }
        // A fleet proof settles its gate with no ownership read, so the
        // commit it proves settles here.
        for op in fleet_gates {
            self.alarm_gate_verified(id, op, true);
        }
        let (events, tasks): (Vec<_>, Vec<_>) = resolved
            .into_iter()
            .partition(|resolution| matches!(resolution, Resolution::Event(_)));
        for resolution in events.into_iter().chain(tasks) {
            let delay = self.net_rng.range(0, 2);
            let work = match resolution {
                Resolution::Event(event) => Work::Deliver(event),
                Resolution::Task { task, ok } => Work::TaskProven { task, ok },
            };
            self.work(id, delay, work);
        }
    }

    pub(super) fn waiter_deadline(&mut self, id: ProcId, cell: &str, epoch: Epoch, op: OpId) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(replica) = proc.replicas.get_mut(&(cell.to_string(), epoch)) else {
            return;
        };
        let Some(index) = replica
            .waiters
            .iter()
            .position(|waiter| waiter.op() == Some(op))
        else {
            return;
        };
        let resolution = replica
            .waiters
            .remove(index)
            .resolve(Err(Failure::Ambiguous));
        let Resolution::Event(event) = resolution else {
            unreachable!("a task has no core op")
        };
        self.drive(id, event);
    }
}
