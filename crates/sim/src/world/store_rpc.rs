//! Calls to the bucket over the network: latency, loss and partitions on
//! the way there and back, and what each answer becomes in the caller.

use super::*;
use crate::store::{LeaseLoad, LogWire, Row, Session};
use celld_logic::drain::DrainToken;
use celld_logic::log_tier::LogState;
use celld_logic::wake::Retirement;

#[derive(Debug)]
pub(super) enum StoreReq {
    ReadOwner {
        cell: String,
    },
    CasOwner {
        cell: String,
        guard: CasGuard,
        epoch: Epoch,
        takeover: bool,
    },
    ReleaseOwner {
        cell: String,
        epoch: Epoch,
    },
    ReadLease {
        node: String,
    },
    CasLease {
        guard: CasGuard,
        record: NodeLeaseRecord,
        /// The folded log the shell stamped into the body.
        log: Option<LogWire>,
        load: LeaseLoad,
    },
    ReadPeers,
    Put {
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    ReadChain {
        cell: String,
        epoch: Epoch,
    },
    /// A paged activation's marker.
    PutMarker {
        cell: String,
        epoch: Epoch,
        cut: u64,
    },
    /// Epoch GC's listing of a cell's epoch prefixes.
    ScanEpochs {
        cell: String,
    },
    PutRetired {
        cell: String,
        mark: Epoch,
    },
    DeleteEpochs {
        cell: String,
        epochs: Vec<Epoch>,
    },
    WakePut {
        key: String,
    },
    WakeDelete {
        key: String,
    },
    WakeList,
    ReadRetirement {
        cell: String,
    },
    ReadRetirements {
        cells: Vec<String>,
    },
    CasRetirement {
        cell: String,
        guard: Option<u64>,
        retirement: Retirement,
    },
    WakerClaim {
        now_ms: i64,
        ttl_ms: i64,
    },
    LiveNodes {
        now_ms: u64,
    },
    ReadOwners {
        cells: Vec<String>,
    },
    /// A node's lease with its folded log.
    ReadLeaseLog {
        node: String,
    },
    /// Replace a dead session's folded log under the lease's ETag.
    CasLeaseLog {
        node: String,
        etag: String,
        log: LogWire,
    },
    /// Every node lease, for the ensemble and the eager sweep.
    LogPeers,
    ReadLeases {
        nodes: Vec<String>,
    },
    PutBundle {
        session: Session,
        rows: Vec<Row>,
    },
    ReadBundles {
        session: Session,
    },
    /// Recovery's merged uploads, one per cell epoch.
    PutRecovered {
        groups: BTreeMap<(String, Epoch), BTreeMap<u64, WriteId>>,
        writer: ProcId,
    },
    /// An object nothing reads back, such as a loss record.
    PutObject {
        key: String,
    },
    ReadDrainToken,
    CasDrainToken {
        guard: CasGuard,
        token: DrainToken,
    },
}

#[derive(Debug)]
pub(super) enum StoreRes {
    Owner(Option<OwnerRecord>),
    Cas(CasOutcome),
    Lease(Option<NodeLeaseRecord>),
    LeaseCas(LeaseCasOutcome),
    Peers {
        view_ms: u64,
        peers: Vec<CapacityPeer>,
    },
    Put,
    Chain(Result<Option<Vec<WriteId>>, ()>),
    /// The restore chain, the epochs below its base, and the retired mark.
    Epochs(
        Option<Vec<Epoch>>,
        Vec<celld_logic::epoch_gc::ListedEpoch>,
        Option<Epoch>,
    ),
    Done,
    Keys(Vec<String>),
    Retirement(Option<(Retirement, u64)>),
    Retirements(BTreeMap<String, Retirement>),
    Bool(bool),
    Live(BTreeSet<String>),
    Owners(Vec<Option<OwnerRecord>>),
    LeaseLog(Option<(NodeLeaseRecord, Option<LogWire>)>),
    /// The new ETag, or `None` when the guard missed.
    LeaseLogCas(Option<String>),
    LogPeers(Vec<NodeLeaseRecord>),
    Leases(Vec<NodeLeaseRecord>),
    Rows(Vec<Row>),
    DrainToken(Option<(DrainToken, String)>),
    /// The new ETag, or `None` when the guard missed.
    DrainTokenCas(Option<String>),
}

impl StoreRes {
    fn owner(self) -> Option<OwnerRecord> {
        match self {
            StoreRes::Owner(record) => record,
            other => mismatch("an owner record", other),
        }
    }

    fn cas(self) -> CasOutcome {
        match self {
            StoreRes::Cas(outcome) => outcome,
            other => mismatch("an ownership CAS outcome", other),
        }
    }

    fn lease(self) -> Option<NodeLeaseRecord> {
        match self {
            StoreRes::Lease(record) => record,
            other => mismatch("a node lease", other),
        }
    }

    fn lease_cas(self) -> LeaseCasOutcome {
        match self {
            StoreRes::LeaseCas(outcome) => outcome,
            other => mismatch("a lease CAS outcome", other),
        }
    }
}

/// A reply route met the answer to a different request: a shell bug.
fn mismatch(expected: &str, got: StoreRes) -> ! {
    unreachable!("expected {expected}, got {got:?}")
}

/// What the reply becomes in the calling process.
#[derive(Debug)]
pub(super) enum Reply {
    OwnerRead(OpId),
    OwnerCas(OpId),
    OwnerReleased(OpId),
    SelfLease(OpId),
    NodeLease(OpId),
    LeaseCas {
        op: OpId,
        /// The folded log state the body carried, and the own-log sequence
        /// an applied write publishes.
        stamped: Option<LogState>,
        seq: u64,
    },
    Peers(OpId),
    Verify {
        op: OpId,
        epoch: Epoch,
    },
    Upload {
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    Restore {
        op: OpId,
        cell: String,
        spec: RestoreSpec,
    },
    /// A paged activation's marker landed: the cell may serve `content`.
    Marker {
        op: OpId,
        cell: String,
        spec: RestoreSpec,
        content: Vec<WriteId>,
    },
    /// A step of a shell task (`alarms::Task`).
    Task {
        task: u64,
    },
    /// A step of a node-log task.
    Log {
        task: u64,
    },
    /// A step of a shutdown, the readiness gate, or balancing.
    Life {
        task: u64,
    },
    /// The peer directory an `AdoptReleased` ranks.
    Handoff {
        op: OpId,
    },
    /// Nobody waits for the answer.
    Ignore,
}

#[derive(Debug)]
pub(super) struct Call {
    proc: ProcId,
    node: usize,
    req: StoreReq,
    reply: Reply,
    /// Filled when the request reaches the store.
    result: Option<StoreRes>,
    /// The caller's view: a definite failure, a lost exchange, or the result.
    failure: Option<Failure>,
}

impl World {
    // -----------------------------------------------------------------------
    // The bucket and the network to it

    pub(super) fn store_call(&mut self, id: ProcId, req: StoreReq, reply: Reply) {
        let call = self.next_call;
        self.next_call += 1;
        let node = self.procs[&id].node;
        let faults = &self.config.faults;
        let (lo, hi) = faults.store_latency_ms;
        let mut request_ms = self.net_rng.range(lo, hi);
        let mut response_ms = self.net_rng.range(lo, hi);
        let timeout = faults.store_timeout_ms;
        let mut failure = None;
        let mut applies = true;
        if self.chaos {
            if self.net_rng.chance(faults.store_spike_probability) {
                request_ms += self.net_rng.range(0, faults.store_spike_ms);
            }
            if self.net_rng.chance(faults.store_spike_probability) {
                response_ms += self.net_rng.range(0, faults.store_spike_ms);
            }
            if self.net_rng.chance(faults.store_definite) {
                failure = Some(Failure::Definite);
                applies = false;
            } else if self.net_rng.chance(faults.store_lost_request) {
                failure = Some(Failure::Ambiguous);
                applies = false;
            } else if self.net_rng.chance(faults.store_lost_response) {
                failure = Some(Failure::Ambiguous);
            }
        }
        match self.nodes[node].partition {
            Some((_, PartitionMode::Refuse)) => {
                failure = Some(Failure::Definite);
                applies = false;
            }
            Some((_, PartitionMode::Hang)) => {
                failure = Some(Failure::Ambiguous);
                applies = false;
            }
            None => {}
        }
        if request_ms + response_ms > timeout && failure.is_none() {
            // The client gives up first. The request may still land later.
            failure = Some(Failure::Ambiguous);
        }
        let reply_ms = match failure {
            None => request_ms + response_ms,
            Some(Failure::Definite) => request_ms.min(timeout),
            Some(Failure::Ambiguous) => timeout,
        };
        if applies {
            self.after(request_ms, What::StoreApply { call });
        }
        self.calls.insert(
            call,
            Call {
                proc: id,
                node,
                req,
                reply,
                result: None,
                failure,
            },
        );
        self.work(id, reply_ms.max(1), Work::StoreReply { call });
    }

    pub(super) fn store_apply(&mut self, call: u64) {
        let Some(entry) = self.calls.get(&call) else {
            return;
        };
        // The writer may be gone by now; its request is still on the wire.
        let proc = entry.proc;
        let sender = entry.node;
        let node = self.nodes[sender].name.clone();
        self.store.now_ms = WALL_EPOCH_MS + self.t;
        let result = match &entry.req {
            StoreReq::ReadOwner { cell } => StoreRes::Owner(self.store.read_owner(cell)),
            StoreReq::CasOwner {
                cell,
                guard,
                epoch,
                takeover,
            } => {
                let (cell, guard, epoch, takeover) =
                    (cell.clone(), guard.clone(), *epoch, *takeover);
                let outcome = self.store.cas_owner(&cell, &guard, epoch, &node, proc);
                if outcome == CasOutcome::Applied && takeover {
                    self.stats.takeovers += 1;
                }
                StoreRes::Cas(outcome)
            }
            StoreReq::ReleaseOwner { cell, epoch } => {
                let (cell, epoch) = (cell.clone(), *epoch);
                StoreRes::Cas(self.store.release_owner(&cell, epoch, &node))
            }
            StoreReq::ReadLease { node } => StoreRes::Lease(self.store.read_lease(node)),
            StoreReq::CasLease {
                guard,
                record,
                log,
                load,
            } => {
                let sampled_ms = record.expires_ms.saturating_sub(self.config.ttl_ms);
                let (guard, record, log, load) =
                    (guard.clone(), record.clone(), log.clone(), *load);
                StoreRes::LeaseCas(self.store.cas_lease(
                    &guard,
                    &record,
                    log.as_ref(),
                    load,
                    sampled_ms,
                ))
            }
            StoreReq::ReadPeers => {
                let view_ms = self.call_wall(proc, sender);
                let peers = self
                    .store
                    .leases()
                    .map(|lease| CapacityPeer {
                        node: lease.record.node.clone(),
                        addr: lease.record.addr.clone(),
                        expires_ms: lease.record.expires_ms,
                        peer_protocol: lease.record.peer_protocol,
                        sampled_ms: lease.sampled_ms,
                        owned_cells: Some(lease.load.owned),
                        placement_weight: Some(1),
                        bucket_format: Some(BUCKET_FORMAT),
                        resident_cells: lease.load.resident,
                        host_websockets: 0,
                        rss_bytes: lease.load.memory.map_or(0, |(_, _, bytes)| bytes),
                        in_use_bytes: Some(lease.load.memory.map_or(0, |(_, _, bytes)| bytes)),
                        pressured: lease.load.memory.is_some_and(|(shedding, _, _)| shedding),
                        memory_headroom: Some(
                            lease.load.memory.is_none_or(|(_, headroom, _)| headroom),
                        ),
                        restoring: lease.load.restoring,
                        paced_handoff: lease.load.paced_handoff,
                        rebalance_paused: false,
                        draining: lease.load.draining,
                    })
                    .collect();
                StoreRes::Peers { view_ms, peers }
            }
            StoreReq::Put {
                cell,
                epoch,
                content,
            } => {
                let (cell, epoch, content) = (cell.clone(), *epoch, content.clone());
                if self
                    .store
                    .owner_epoch(&cell)
                    .is_some_and(|owner| owner > epoch)
                {
                    self.stats.stale_uploads += 1;
                }
                let writers = self.store.put_ltx(&cell, epoch, &content, proc);
                let (checker, view) = self.checks();
                checker.on_upload(view, &cell, epoch, &writers);
                StoreRes::Put
            }
            StoreReq::ReadChain { cell, epoch } => {
                StoreRes::Chain(self.store.read_chain(cell, *epoch))
            }
            StoreReq::PutMarker { cell, epoch, cut } => {
                let (cell, epoch) = (cell.clone(), *epoch);
                let writers = self.store.put_marker(&cell, epoch, *cut, proc);
                let (checker, view) = self.checks();
                checker.on_upload(view, &cell, epoch, &writers);
                StoreRes::Done
            }
            StoreReq::ScanEpochs { cell } => {
                let (chain, listed, mark) = self.store.scan_epochs(cell);
                StoreRes::Epochs(chain, listed, mark)
            }
            StoreReq::PutRetired { cell, mark } => {
                self.store.put_retired(cell, *mark);
                StoreRes::Done
            }
            StoreReq::DeleteEpochs { cell, epochs } => {
                self.store.delete_epochs(cell, epochs);
                StoreRes::Done
            }
            StoreReq::WakePut { key } => {
                let key = key.clone();
                self.store.put_wake_entry(&key);
                self.stats.wake_entries += 1;
                StoreRes::Done
            }
            StoreReq::WakeDelete { key } => {
                let key = key.clone();
                self.store.delete_wake_entry(&key);
                if let Some(entry) = celld_logic::wake::parse_entry_key(&key) {
                    let (checker, view) = self.checks();
                    checker.check_discoverable(view, &entry.cell);
                }
                StoreRes::Done
            }
            StoreReq::WakeList => StoreRes::Keys(self.store.wake_entries()),
            StoreReq::ReadRetirement { cell } => {
                StoreRes::Retirement(self.store.read_retirement(cell))
            }
            StoreReq::ReadRetirements { cells } => StoreRes::Retirements(
                cells
                    .iter()
                    .filter_map(|cell| {
                        self.store
                            .read_retirement(cell)
                            .map(|(record, _)| (cell.clone(), record))
                    })
                    .collect(),
            ),
            StoreReq::CasRetirement {
                cell,
                guard,
                retirement,
            } => {
                let (cell, guard, retirement) = (cell.clone(), *guard, *retirement);
                let applied = self.store.cas_retirement(&cell, guard, retirement);
                if applied {
                    self.stats.wake_retirements += 1;
                    let (checker, view) = self.checks();
                    checker.check_discoverable(view, &cell);
                }
                StoreRes::Bool(applied)
            }
            StoreReq::WakerClaim { now_ms, ttl_ms } => {
                let (now_ms, ttl_ms) = (*now_ms, *ttl_ms);
                StoreRes::Bool(self.store.claim_waker(&node, now_ms, ttl_ms))
            }
            StoreReq::LiveNodes { now_ms } => StoreRes::Live(
                self.store
                    .leases()
                    .filter(|lease| lease.record.expires_ms > *now_ms)
                    .map(|lease| lease.record.node.clone())
                    .collect(),
            ),
            StoreReq::ReadOwners { cells } => StoreRes::Owners(
                cells
                    .iter()
                    .map(|cell| self.store.read_owner(cell))
                    .collect(),
            ),
            StoreReq::ReadLeaseLog { node } => StoreRes::LeaseLog(self.store.read_lease_log(node)),
            StoreReq::CasLeaseLog { node, etag, log } => {
                let (node, etag, log) = (node.clone(), etag.clone(), log.clone());
                StoreRes::LeaseLogCas(self.store.cas_lease_log(&node, &etag, log))
            }
            StoreReq::LogPeers => StoreRes::LogPeers(
                self.store
                    .leases()
                    .map(|lease| lease.record.clone())
                    .collect(),
            ),
            StoreReq::ReadLeases { nodes } => StoreRes::Leases(
                nodes
                    .iter()
                    .filter_map(|node| self.store.read_lease(node))
                    .collect(),
            ),
            StoreReq::PutBundle { session, rows } => {
                let (session, rows) = (session.clone(), rows.clone());
                self.store.put_bundle(&session, &rows);
                let (checker, view) = self.checks();
                checker.fleet.on_bundle(view, &session, &rows);
                StoreRes::Done
            }
            StoreReq::ReadBundles { session } => StoreRes::Rows(self.store.bundle_rows(session)),
            StoreReq::PutRecovered { groups, writer } => {
                let (groups, writer) = (groups.clone(), *writer);
                for ((cell, epoch), rows) in groups {
                    let writers = self.store.put_recovered(&cell, epoch, &rows, writer);
                    let (checker, view) = self.checks();
                    checker.on_upload(view, &cell, epoch, &writers);
                }
                StoreRes::Done
            }
            StoreReq::ReadDrainToken => StoreRes::DrainToken(self.store.read_drain_token()),
            StoreReq::CasDrainToken { guard, token } => {
                let (guard, token) = (guard.clone(), token.clone());
                StoreRes::DrainTokenCas(self.store.cas_drain_token(&guard, token))
            }
            StoreReq::PutObject { key } => {
                let key = key.clone();
                self.note(|| format!("put {key}"));
                StoreRes::Done
            }
        };
        self.calls.get_mut(&call).unwrap().result = Some(result);
    }

    fn call_wall(&self, proc: ProcId, node: usize) -> u64 {
        if self.procs.contains_key(&proc) {
            self.wall(proc)
        } else {
            (WALL_EPOCH_MS as i64 + self.t as i64 + self.nodes[node].skew_ms).max(0) as u64
        }
    }

    pub(super) fn store_reply(&mut self, id: ProcId, call: u64) {
        let Some(entry) = self.calls.remove(&call) else {
            return;
        };
        let Call {
            reply,
            result,
            failure,
            ..
        } = entry;
        // A lost response still applied; the caller cannot know it did.
        let result = match failure {
            Some(failure) => Err(failure),
            None => Ok(result.expect("a successful call reached the store")),
        };
        let event = match reply {
            Reply::Task { task } => {
                self.task_reply(id, task, result);
                return;
            }
            Reply::Log { task } => {
                self.log_reply(id, task, result);
                return;
            }
            Reply::Life { task } => {
                self.life_reply(id, task, result);
                return;
            }
            Reply::Handoff { op } => {
                self.handoff_peers(id, op, result);
                return;
            }
            Reply::Ignore => return,
            Reply::OwnerRead(op) => Event::OwnerRead {
                op,
                now_ms: 0,
                result: result.map(StoreRes::owner),
            },
            Reply::OwnerCas(op) => Event::OwnerCasCompleted {
                op,
                result: result.map(StoreRes::cas),
            },
            Reply::OwnerReleased(op) => Event::OwnerReleased {
                op,
                result: result.map(StoreRes::cas),
            },
            Reply::SelfLease(op) => Event::SelfNodeLeaseRead {
                op,
                now_ms: 0,
                now_mono_ms: 0,
                result: result.map(StoreRes::lease),
            },
            Reply::NodeLease(op) => Event::NodeLeaseRead {
                op,
                now_ms: 0,
                result: result.map(StoreRes::lease),
            },
            Reply::LeaseCas { op, stamped, seq } => {
                let result = result.map(StoreRes::lease_cas);
                let applied = matches!(result, Ok(LeaseCasOutcome::Applied { .. }));
                self.drive(
                    id,
                    Event::NodeLeaseCasCompleted {
                        op,
                        now_mono_ms: 0,
                        result,
                        stamped_log_state: stamped,
                    },
                );
                if applied && self.procs.contains_key(&id) {
                    self.lease_applied(id, seq);
                }
                return;
            }
            Reply::Peers(op) => {
                let wall = self.wall(id);
                match result {
                    Ok(StoreRes::Peers { view_ms, peers }) => Event::CapacityPeersRead {
                        op,
                        now_ms: view_ms,
                        result: Ok(peers),
                    },
                    Err(failure) => Event::CapacityPeersRead {
                        op,
                        now_ms: wall,
                        result: Err(failure),
                    },
                    Ok(_) => unreachable!(),
                }
            }
            Reply::Verify { op, epoch } => {
                let node = self.node_name(id);
                let result = match result {
                    Ok(StoreRes::Owner(Some(record)))
                        if record.node.as_deref() == Some(node.as_str())
                            && record.epoch == epoch =>
                    {
                        Ok(())
                    }
                    Ok(_) => Err(Failure::Definite),
                    Err(failure) => Err(failure),
                };
                self.alarm_gate_verified(id, op, result.is_ok());
                Event::OwnershipVerified { op, result }
            }
            Reply::Upload { cell, epoch, len } => {
                let proc = self.procs.get_mut(&id).unwrap();
                if let Some(replica) = proc.replicas.get_mut(&(cell.clone(), epoch)) {
                    replica.uploading = false;
                    if result.is_ok() {
                        replica.uploaded = replica.uploaded.max(len);
                        self.settle_waiters(id, &cell, epoch);
                        self.kick_upload(id, &cell, epoch);
                    } else {
                        let delay = self.net_rng.range(50, 300);
                        self.work(id, delay, Work::UploadKick { cell, epoch });
                    }
                }
                return;
            }
            Reply::Restore { op, cell, spec } => match result {
                Ok(StoreRes::Chain(Err(()))) => {
                    self.checker.on_unrestorable(self.t, id, &cell, spec.epoch);
                    Event::RestoreCompleted {
                        op,
                        result: Err(Failure::Definite),
                    }
                }
                Ok(StoreRes::Chain(Ok(chain))) => {
                    self.stats.restores_bucket += 1;
                    let restored = chain.is_some();
                    let content = chain.unwrap_or_default();
                    // A paged activation serves only once its marker is in
                    // the epoch's prefix.
                    if !content.is_empty()
                        && self.config.paged > 0.0
                        && self.net_rng.chance(self.config.paged)
                    {
                        let req = StoreReq::PutMarker {
                            cell: cell.clone(),
                            epoch: spec.epoch,
                            cut: content.len() as u64,
                        };
                        let reply = Reply::Marker {
                            op,
                            cell,
                            spec,
                            content,
                        };
                        return self.store_call(id, req, reply);
                    }
                    let alarm = self.restored_alarm(&content);
                    self.open_replica(id, &cell, spec.epoch, content, "bucket");
                    Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome { restored, alarm }),
                    }
                }
                Ok(_) => unreachable!(),
                Err(_) => Event::RestoreCompleted {
                    op,
                    result: Err(Failure::Definite),
                },
            },
            Reply::Marker {
                op,
                cell,
                spec,
                content,
            } => match result {
                Ok(_) => {
                    self.stats.paged_restores += 1;
                    let alarm = self.restored_alarm(&content);
                    self.open_replica(id, &cell, spec.epoch, content, "paged");
                    let key = (cell, spec.epoch);
                    self.procs
                        .get_mut(&id)
                        .unwrap()
                        .replicas
                        .get_mut(&key)
                        .unwrap()
                        .paged = true;
                    Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored: true,
                            alarm,
                        }),
                    }
                }
                Err(_) => Event::RestoreCompleted {
                    op,
                    result: Err(Failure::Definite),
                },
            },
        };
        self.drive(id, event);
    }
}
