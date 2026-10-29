//! Recovery of a dead session's log (`recover_serial`), and the live
//! session's fold of a cell's tail before that cell restores again
//! (`fold_cell`).
//!
//! Both gather the same way: read the record, ask every ensemble member for
//! its retained fragment, read the session's bundles, and upload the rows
//! into the per-cell prefixes. Recovery fences the record first, seals the
//! members rather than reading them, and seals the record last.

use super::*;
use celld_logic::RestoreSpec;

/// A seal or tail request's deadline.
const PEER_TIMEOUT_MS: u64 = 5_000;
/// How long another node's recovery claim stays live without a heartbeat.
const CLAIM_TTL_MS: u64 = 30_000;
const RECOVERY_PASSES: u32 = 5;

/// Who asked for a recovery, and so who hears how it ended.
#[derive(Clone, Copy, Debug)]
pub(in crate::world) enum RecoverMode {
    /// Startup, for the node's previous session.
    Boot,
    /// The takeover interlock's `RecoverNodeLog`.
    Takeover(OpId),
    /// Maintenance's eager sweep of dead sessions.
    Sweep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Ended {
    /// The log needs nothing more: absent, sealed, or just recovered.
    Done,
    /// Another node's live claim is recovering it.
    Elsewhere,
    Failed,
}

#[derive(Debug)]
enum Job {
    /// Recover a dead session's log, for everyone who asked.
    Recover { modes: Vec<RecoverMode> },
    /// Fold this session's tail of `cell` into its prefix, then restore.
    Fold {
        op: OpId,
        cell: String,
        spec: RestoreSpec,
    },
}

impl Job {
    /// Whether the job fences the log: claims the record, seals the members
    /// rather than reading their tails, records a loss, and seals the
    /// record. A fold reads its own live log and fences nothing.
    fn fences(&self) -> bool {
        matches!(self, Job::Recover { .. })
    }

    /// The one cell the job uploads, if it is limited to one.
    fn only_cell(&self) -> Option<&str> {
        match self {
            Job::Recover { .. } => None,
            Job::Fold { cell, .. } => Some(cell),
        }
    }
}

#[derive(Debug)]
pub(in crate::world) struct Recovery {
    /// The node whose lease carries the log.
    node: String,
    job: Job,
    passes: u32,
    /// The members this pass asked, while they answer.
    asking: Option<Asking>,
}

/// The log being gathered, as the record read found it.
#[derive(Clone, Debug)]
pub(super) struct Found {
    session: Session,
    lease: NodeLeaseRecord,
    wire: LogWire,
}

#[derive(Debug)]
struct Asking {
    found: Found,
    leases: BTreeMap<String, NodeLeaseRecord>,
    pending: BTreeSet<String>,
    witness: Option<TailAnswer>,
    inconclusive: usize,
}

/// A recovery's store call in flight, and what its reply continues.
#[derive(Debug)]
pub(super) enum Step {
    Read,
    Claim(Found),
    Members(Found),
    Loss(Found, Vec<Row>),
    Bundles(Found, Vec<Row>),
    Upload(Found, u64),
    Finish,
}

impl World {
    /// Recover `node`'s session for `mode`, joining a recovery of it that
    /// already runs here.
    pub(in crate::world) fn recover(&mut self, id: ProcId, node: String, mode: RecoverMode) {
        if self.config.mutation == Mutation::SkipLogRecovery {
            match mode {
                RecoverMode::Boot => {}
                RecoverMode::Takeover(op) => {
                    let event = Event::NodeLogRecovered { op, result: Ok(()) };
                    return self.work(id, 0, Work::Deliver(event));
                }
                RecoverMode::Sweep => return,
            }
        }
        let log = self.log(id);
        if let Some(key) = log.recovering.get(&node) {
            if let Some(Recovery {
                job: Job::Recover { modes },
                ..
            }) = log.recoveries.get_mut(key)
            {
                modes.push(mode);
            }
            return;
        }
        let job = Job::Recover { modes: vec![mode] };
        let key = self.begin_recovery(id, node.clone(), job);
        self.log(id).recovering.insert(node, key);
    }

    /// `fold_cell`: gather the acknowledged tail this session left outside
    /// `cell`'s prefix when an epoch of it ended, then restore.
    pub(in crate::world) fn fold(&mut self, id: ProcId, op: OpId, cell: String, spec: RestoreSpec) {
        let node = self.node_name(id);
        self.begin_recovery(id, node, Job::Fold { op, cell, spec });
    }

    /// How long a member's lease must have been dead before recovery takes
    /// a connection error from it as proof its disk is gone.
    pub(in crate::world) fn dead_member_grace(&self) -> u64 {
        (3 * self.config.ttl_ms).max(20_000)
    }

    fn begin_recovery(&mut self, id: ProcId, node: String, job: Job) -> u64 {
        let key = self.next_task;
        self.next_task += 1;
        let recovery = Recovery {
            node,
            job,
            passes: 0,
            asking: None,
        };
        self.log(id).recoveries.insert(key, recovery);
        self.recovery_pass(id, key);
        key
    }

    fn job(&mut self, id: ProcId, key: u64) -> &Job {
        &self.log(id).recoveries[&key].job
    }

    /// Start over from the record read.
    fn recovery_pass(&mut self, id: ProcId, key: u64) {
        let Some(recovery) = self.log(id).recoveries.get_mut(&key) else {
            return;
        };
        recovery.passes += 1;
        recovery.asking = None;
        if recovery.passes > RECOVERY_PASSES {
            return self.recovery_ended(id, key, Ended::Failed);
        }
        let node = recovery.node.clone();
        self.recovery_call(id, key, Step::Read, StoreReq::ReadLeaseLog { node });
    }

    fn recovery_call(&mut self, id: ProcId, key: u64, step: Step, req: StoreReq) {
        self.log_store_call(
            id,
            req,
            LogTask::Recover {
                key,
                step: Box::new(step),
            },
        );
    }

    pub(super) fn recovery_reply(
        &mut self,
        id: ProcId,
        key: u64,
        step: Step,
        result: Result<StoreRes, Failure>,
    ) {
        if !self.log(id).recoveries.contains_key(&key) {
            return;
        }
        let Ok(result) = result else {
            // A claim or a seal whose answer was lost may still have landed:
            // the next pass reads what did.
            return match step {
                Step::Claim(_) | Step::Finish => self.recovery_pass(id, key),
                _ => self.recovery_ended(id, key, Ended::Failed),
            };
        };
        match (step, result) {
            (Step::Read, StoreRes::LeaseLog(found)) => self.record_read(id, key, found),
            (Step::Claim(mut found), StoreRes::LeaseLogCas(etag)) => match etag {
                Some(etag) => {
                    found.lease.etag = etag;
                    self.gather_members(id, key, found);
                }
                None => self.recovery_pass(id, key),
            },
            (Step::Members(found), StoreRes::Leases(leases)) => {
                self.ask_members(id, key, found, leases)
            }
            (Step::Loss(found, frames), StoreRes::Done) => {
                self.stats.log_loss_records += 1;
                self.read_bundles(id, key, found, frames);
            }
            (Step::Bundles(found, frames), StoreRes::Rows(rows)) => {
                self.upload_gathered(id, key, found, frames, rows)
            }
            (Step::Upload(found, end), StoreRes::Done) => self.gathered(id, key, found, end),
            (Step::Finish, StoreRes::LeaseLogCas(etag)) => match etag {
                Some(_) => {
                    self.stats.log_recoveries += 1;
                    self.recovery_ended(id, key, Ended::Done);
                }
                None => self.recovery_pass(id, key),
            },
            (step, result) => unreachable!("recovery step {step:?} got {result:?}"),
        }
    }

    /// R1, the record: nothing to do, a claim to make, or members to ask.
    fn record_read(
        &mut self,
        id: ProcId,
        key: u64,
        read: Option<(NodeLeaseRecord, Option<LogWire>)>,
    ) {
        let (lease, wire) = match read {
            Some((lease, Some(wire))) => (lease, wire),
            // A takeover seals a foreign lease that never opened a log: its
            // session cannot open one afterwards through a stale renewal.
            Some((lease, None))
                if self.job(id, key).fences() && lease.node != self.node_name(id) =>
            {
                if lease.expires_ms > self.wall(id) {
                    return self.recovery_ended(id, key, Ended::Failed);
                }
                let req = StoreReq::CasLeaseLog {
                    node: lease.node,
                    etag: lease.etag,
                    log: LogWire {
                        record: log_tier::sealed_absent(),
                        active: false,
                    },
                };
                return self.recovery_call(id, key, Step::Finish, req);
            }
            _ => return self.recovery_ended(id, key, Ended::Done),
        };
        let found = Found {
            session: Session::of_lease(&lease),
            lease,
            wire,
        };
        if !self.job(id, key).fences() {
            if found.session.proc != id {
                return self.recovery_ended(id, key, Ended::Failed);
            }
            return self.gather_members(id, key, found);
        }
        let record = &found.wire.record;
        let now = self.wall(id);
        let me = self.node_name(id);
        let claimed = match record.state {
            LogState::Sealed => return self.recovery_ended(id, key, Ended::Done),
            // A live lease is a live session: its log is not ours to fence.
            _ if found.lease.expires_ms > now => {
                return self.recovery_ended(id, key, Ended::Failed)
            }
            LogState::Recovering if log_tier::recovery_claimed_by(record, &me) => {
                return self.gather_members(id, key, found);
            }
            LogState::Recovering if log_tier::recovery_claim_live(record, now, CLAIM_TTL_MS) => {
                return self.recovery_ended(id, key, Ended::Elsewhere);
            }
            LogState::Recovering => log_tier::take_over_recovery(record, &me, now, CLAIM_TTL_MS),
            LogState::Open => log_tier::start_recovery(record, &me, now),
        };
        let mut found = found;
        found.wire.record = claimed.expect("the state allows the claim");
        let req = StoreReq::CasLeaseLog {
            node: found.lease.node.clone(),
            etag: found.lease.etag.clone(),
            log: found.wire.clone(),
        };
        self.recovery_call(id, key, Step::Claim(found), req);
    }

    fn gather_members(&mut self, id: ProcId, key: u64, found: Found) {
        let nodes = found.wire.record.ensemble.iter().cloned().collect();
        self.recovery_call(
            id,
            key,
            Step::Members(found),
            StoreReq::ReadLeases { nodes },
        );
    }

    /// R3: seal every member (a fold reads their tails instead).
    fn ask_members(&mut self, id: ProcId, key: u64, found: Found, leases: Vec<NodeLeaseRecord>) {
        let seal = self.job(id, key).fences();
        let from = self.procs[&id].node;
        let epoch = found.wire.record.epoch;
        let mut inconclusive = 0;
        let mut pending = BTreeSet::new();
        for member in &found.wire.record.ensemble {
            // A member with no lease has no address to ask.
            if !leases.iter().any(|lease| lease.node == *member) {
                inconclusive += 1;
                continue;
            }
            pending.insert(member.clone());
        }
        let leases = leases
            .into_iter()
            .map(|lease| (lease.node.clone(), lease))
            .collect();
        let session = found.session.clone();
        let members: Vec<String> = pending.iter().cloned().collect();
        self.log(id).recoveries.get_mut(&key).unwrap().asking = Some(Asking {
            found,
            leases,
            pending,
            witness: None,
            inconclusive,
        });
        if members.is_empty() {
            return self.members_answered(id, key);
        }
        for member in members {
            let to = self.node_index(&member).expect("members are nodes");
            let delay = self.net_rng.range(1, 5);
            let ask = LogWork::Gather {
                recoverer: id,
                key,
                session: session.clone(),
                epoch,
                seal,
            };
            let failed = LogWork::GatherFailed { key, member };
            match self.peer_send(from, to, delay, ask) {
                Ok(()) | Err(Failure::Ambiguous) => {
                    self.work(id, PEER_TIMEOUT_MS, Work::Log(failed));
                }
                Err(Failure::Definite) => self.work(id, delay, Work::Log(failed)),
            }
        }
    }

    /// A member answers a seal or a tail read.
    pub(super) fn gather_arrived(
        &mut self,
        id: ProcId,
        recoverer: ProcId,
        key: u64,
        session: &Session,
        epoch: Epoch,
        seal: bool,
    ) {
        let node = self.procs[&id].node;
        let answer = self.follower_answer(node, session, epoch, seal);
        let Some(back) = self.procs.get(&recoverer).map(|proc| proc.node) else {
            return;
        };
        let reply = LogWork::Gathered {
            key,
            member: self.node_name(id),
            answer,
        };
        let delay = self.net_rng.range(1, 5);
        let _ = self.peer_send(node, back, delay, reply);
    }

    /// One member's answer, or its failure to give one.
    pub(super) fn member_answered(
        &mut self,
        id: ProcId,
        key: u64,
        member: &str,
        answer: Option<TailAnswer>,
    ) {
        let now = self.wall(id);
        let grace = self.dead_member_grace();
        let Some(Recovery {
            asking:
                Some(Asking {
                    found,
                    leases,
                    pending,
                    witness,
                    inconclusive,
                }),
            ..
        }) = self.log(id).recoveries.get_mut(&key)
        else {
            return;
        };
        if !pending.remove(member) {
            return;
        }
        match answer {
            Some(answer) if answer.held == found.wire.record.epoch => {
                if witness.as_ref().is_none_or(|best| best.end < answer.end) {
                    *witness = Some(answer);
                }
            }
            Some(_) => {}
            // A connection error proves nothing about the disk, unless the
            // member has been dead long enough to count as gone.
            None if leases[member].expires_ms + grace < now => {}
            None => *inconclusive += 1,
        }
        if pending.is_empty() {
            self.members_answered(id, key);
        }
    }

    /// R4: a witness, a loss record, or a failure.
    fn members_answered(&mut self, id: ProcId, key: u64) {
        let recovery = self.log(id).recoveries.get_mut(&key).unwrap();
        let fences = recovery.job.fences();
        let Some(Asking {
            found,
            witness,
            inconclusive,
            ..
        }) = recovery.asking.take()
        else {
            unreachable!("members answer while they are asked");
        };
        let unwitnessed =
            witness.is_none() && found.wire.active && !found.wire.record.ensemble.is_empty();
        let frames = witness.map_or(Vec::new(), |answer| answer.frames);
        if !unwitnessed {
            return self.read_bundles(id, key, found, frames);
        }
        // Once the epoch is active, a fleet proof may exist only on the
        // followers: no witness is a failure unless every member is
        // conclusively gone, and then the loss is a record.
        if !fences || inconclusive > 0 {
            return self.recovery_ended(id, key, Ended::Failed);
        }
        self.excuse_losses(&found);
        let loss = format!(
            "log/{}.e{}.loss.json",
            found.session, found.wire.record.epoch
        );
        let req = StoreReq::PutObject { key: loss };
        self.recovery_call(id, key, Step::Loss(found, frames), req);
    }

    /// Fleet-proven writes the recovery cannot find because every disk that
    /// held them is gone are beyond what the protocol promises.
    fn excuse_losses(&mut self, found: &Found) {
        let epoch = found.wire.record.epoch;
        let (checker, view) = self.checks();
        let (lost, on_disk) = checker.fleet.on_loss_record(view, &found.session, epoch);
        self.note(|| {
            format!(
                "loss record for {} epoch {epoch}: {lost} proven rows on no disk, {on_disk} \
                 still on one",
                found.session
            )
        });
        self.stats.fleet_losses_excused += lost as u64;
    }

    fn read_bundles(&mut self, id: ProcId, key: u64, found: Found, frames: Vec<Row>) {
        let session = found.session.clone();
        self.recovery_call(
            id,
            key,
            Step::Bundles(found, frames),
            StoreReq::ReadBundles { session },
        );
    }

    /// R5: `upload_gathered`, one merged upload per cell epoch.
    fn upload_gathered(
        &mut self,
        id: ProcId,
        key: u64,
        found: Found,
        frames: Vec<Row>,
        rows: Vec<Row>,
    ) {
        let end = frames.len() as u64;
        let only = self.job(id, key).only_cell().map(str::to_string);
        let mut groups: BTreeMap<(String, Epoch), BTreeMap<u64, WriteId>> = BTreeMap::new();
        for row in rows.into_iter().chain(frames) {
            if only.as_ref().is_none_or(|cell| *cell == row.cell) {
                groups
                    .entry((row.cell, row.epoch))
                    .or_default()
                    .insert(row.position, row.write);
            }
        }
        if groups.is_empty() {
            return self.gathered(id, key, found, end);
        }
        let writer = found.session.proc;
        let req = StoreReq::PutRecovered { groups, writer };
        self.recovery_call(id, key, Step::Upload(found, end), req);
    }

    /// R6: the gathered tail is in the bucket, so the record seals.
    fn gathered(&mut self, id: ProcId, key: u64, found: Found, end: u64) {
        if !self.job(id, key).fences() {
            return self.recovery_ended(id, key, Ended::Done);
        }
        let record = &found.wire.record;
        let cert = log_tier::certify(record.tiered, end);
        let Some(sealed) = log_tier::finish_recovery(record, cert) else {
            return self.recovery_pass(id, key);
        };
        let req = StoreReq::CasLeaseLog {
            node: found.lease.node.clone(),
            etag: found.lease.etag.clone(),
            log: LogWire {
                record: sealed,
                active: found.wire.active,
            },
        };
        self.recovery_call(id, key, Step::Finish, req);
    }

    fn recovery_ended(&mut self, id: ProcId, key: u64, ended: Ended) {
        let log = self.log(id);
        let recovery = log.recoveries.remove(&key).unwrap();
        match recovery.job {
            Job::Recover { modes } => {
                let node = recovery.node;
                log.recovering.remove(&node);
                self.note(|| format!("p{id} recovery of {node}: {ended:?}"));
                for mode in modes {
                    match mode {
                        RecoverMode::Boot => self.boot_recovered(id, ended),
                        RecoverMode::Takeover(op) => {
                            let result = match ended {
                                Ended::Done => Ok(()),
                                Ended::Elsewhere | Ended::Failed => Err(Failure::Ambiguous),
                            };
                            let event = Event::NodeLogRecovered { op, result };
                            self.work(id, 0, Work::Deliver(event));
                        }
                        RecoverMode::Sweep => {}
                    }
                }
            }
            Job::Fold { op, cell, spec } => {
                if ended == Ended::Done {
                    log.dirty.remove(&cell);
                    let epoch = spec.epoch;
                    let req = StoreReq::ReadChain {
                        cell: cell.clone(),
                        epoch,
                    };
                    self.store_call(id, req, Reply::Restore { op, cell, spec });
                } else {
                    let event = Event::RestoreCompleted {
                        op,
                        result: Err(Failure::Definite),
                    };
                    self.work(id, 1, Work::Deliver(event));
                }
            }
        }
    }
}
