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
/// How often startup polls a recovery another node is running.
const CLAIM_POLL_MS: u64 = 250;

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
enum Ended {
    /// The log needs nothing more: absent, sealed, or just recovered.
    Done,
    /// Another node's live claim is recovering it.
    Elsewhere,
    Failed,
}

#[derive(Debug)]
enum Job {
    /// Recover a dead session of `node`, for everyone who asked.
    Recover {
        node: String,
        modes: Vec<RecoverMode>,
    },
    /// Fold this session's tail of `cell` into its prefix, then restore.
    Fold {
        op: OpId,
        cell: String,
        spec: RestoreSpec,
    },
}

#[derive(Debug)]
pub(in crate::world) struct Recovery {
    job: Job,
    passes: u32,
    step: Step,
}

/// The log being gathered, as the record read found it.
#[derive(Clone, Debug)]
struct Found {
    session: String,
    /// The process that wrote the session's epochs.
    writer: ProcId,
    lease: NodeLeaseRecord,
    wire: LogWire,
}

#[derive(Debug)]
enum Step {
    Read,
    Claim(Found),
    Members(Found),
    Peers {
        found: Found,
        leases: BTreeMap<String, NodeLeaseRecord>,
        pending: BTreeSet<String>,
        witness: Option<TailAnswer>,
        inconclusive: usize,
    },
    Loss(Found, Vec<Row>),
    Bundles(Found, Vec<Row>),
    Upload(Found, u64),
    Finish,
}

impl Recovery {
    fn fold(&self) -> bool {
        matches!(self.job, Job::Fold { .. })
    }
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
                job: Job::Recover { modes, .. },
                ..
            }) = log.recoveries.get_mut(key)
            {
                modes.push(mode);
            }
            return;
        }
        let job = Job::Recover {
            node: node.clone(),
            modes: vec![mode],
        };
        let key = self.begin_recovery(id, job);
        self.log(id).recovering.insert(node, key);
    }

    /// `fold_cell`: gather the acknowledged tail this session left outside
    /// `cell`'s prefix when an epoch of it ended, then restore.
    pub(in crate::world) fn fold(&mut self, id: ProcId, op: OpId, cell: String, spec: RestoreSpec) {
        self.begin_recovery(id, Job::Fold { op, cell, spec });
    }

    fn begin_recovery(&mut self, id: ProcId, job: Job) -> u64 {
        let key = self.next_task;
        self.next_task += 1;
        let recovery = Recovery {
            job,
            passes: 0,
            step: Step::Read,
        };
        self.log(id).recoveries.insert(key, recovery);
        self.recovery_pass(id, key);
        key
    }

    /// Start over from the record read.
    pub(super) fn recovery_pass(&mut self, id: ProcId, key: u64) {
        let Some(recovery) = self.log(id).recoveries.get_mut(&key) else {
            return;
        };
        recovery.passes += 1;
        recovery.step = Step::Read;
        if recovery.passes > RECOVERY_PASSES {
            return self.recovery_ended(id, key, Ended::Failed);
        }
        let node = match &recovery.job {
            Job::Recover { node, .. } => node.clone(),
            Job::Fold { .. } => self.node_name(id),
        };
        self.recovery_call(id, key, StoreReq::ReadLeaseLog { node });
    }

    fn recovery_call(&mut self, id: ProcId, key: u64, req: StoreReq) {
        let task = self.next_task;
        self.next_task += 1;
        self.log(id).tasks.insert(task, LogTask::Recover { key });
        self.store_call(id, req, Reply::Log { task });
    }

    fn set_step(&mut self, id: ProcId, key: u64, step: Step) {
        self.log(id).recoveries.get_mut(&key).unwrap().step = step;
    }

    pub(super) fn recovery_reply(
        &mut self,
        id: ProcId,
        key: u64,
        result: Result<StoreRes, Failure>,
    ) {
        let Some(recovery) = self.log(id).recoveries.get_mut(&key) else {
            return;
        };
        let fold = recovery.fold();
        let step = std::mem::replace(&mut recovery.step, Step::Read);
        let Ok(result) = result else {
            // A claim or a seal whose answer was lost may still have landed:
            // the next pass reads what did.
            return match step {
                Step::Claim(_) | Step::Finish => self.recovery_pass(id, key),
                _ => self.recovery_ended(id, key, Ended::Failed),
            };
        };
        match (step, result) {
            (Step::Read, StoreRes::LeaseLog(found)) => self.record_read(id, key, fold, found),
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
        fold: bool,
        read: Option<(NodeLeaseRecord, Option<LogWire>)>,
    ) {
        let Some((lease, Some(wire))) = read else {
            return self.recovery_ended(id, key, Ended::Done);
        };
        let writer = lease
            .generation
            .strip_prefix('g')
            .and_then(|id| id.parse().ok())
            .expect("generations name their process");
        let found = Found {
            session: format!("{}/{}", lease.node, lease.generation),
            writer,
            lease,
            wire,
        };
        if fold {
            if found.writer != id {
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
        self.set_step(id, key, Step::Claim(found));
        self.recovery_call(id, key, req);
    }

    fn gather_members(&mut self, id: ProcId, key: u64, found: Found) {
        let nodes = found.wire.record.ensemble.iter().cloned().collect();
        self.set_step(id, key, Step::Members(found));
        self.recovery_call(id, key, StoreReq::ReadLeases { nodes });
    }

    /// R3: seal every member (a fold reads their tails instead).
    fn ask_members(&mut self, id: ProcId, key: u64, found: Found, leases: Vec<NodeLeaseRecord>) {
        let seal = !self.log(id).recoveries[&key].fold();
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
        self.set_step(
            id,
            key,
            Step::Peers {
                found,
                leases,
                pending,
                witness: None,
                inconclusive,
            },
        );
        if members.is_empty() {
            return self.members_answered(id, key);
        }
        for member in members {
            let to = self.node_index(&member).expect("members are nodes");
            let delay = self.net_rng.range(1, 5);
            let ask = LogWork::Gather {
                recoverer: id,
                task: key,
                session: session.clone(),
                epoch,
                seal,
            };
            let failed = LogWork::GatherFailed { task: key, member };
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
        task: u64,
        session: &str,
        epoch: Epoch,
        seal: bool,
    ) {
        let node = self.procs[&id].node;
        let answer = self.follower_answer(node, session, epoch, seal);
        let Some(back) = self.procs.get(&recoverer).map(|proc| proc.node) else {
            return;
        };
        let reply = LogWork::Gathered {
            task,
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
        let grace = (3 * self.config.ttl_ms).max(20_000);
        let Some(Recovery {
            step:
                Step::Peers {
                    found,
                    leases,
                    pending,
                    witness,
                    inconclusive,
                },
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
        let fold = recovery.fold();
        let Step::Peers {
            found,
            witness,
            inconclusive,
            ..
        } = std::mem::replace(&mut recovery.step, Step::Read)
        else {
            unreachable!("members answer while the peers are asked");
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
        if fold || inconclusive > 0 {
            return self.recovery_ended(id, key, Ended::Failed);
        }
        self.excuse_losses(&found);
        let loss = format!(
            "log/{}.e{}.loss.json",
            found.session, found.wire.record.epoch
        );
        self.set_step(id, key, Step::Loss(found, frames));
        self.recovery_call(id, key, StoreReq::PutObject { key: loss });
    }

    /// Fleet-proven writes the recovery cannot find because every disk that
    /// held them is gone are beyond what the protocol promises.
    fn excuse_losses(&mut self, found: &Found) {
        let epoch = found.wire.record.epoch;
        let (on_disk, lost): (Vec<Row>, Vec<Row>) = self
            .checker
            .fleet_proven(&found.session, epoch)
            .into_iter()
            .filter(|row| !self.row_in_bucket(row))
            .partition(|row| {
                (0..self.nodes.len())
                    .any(|index| self.disk_holds(index, &found.session, epoch, row))
            });
        self.note(|| {
            format!(
                "loss record for {} epoch {epoch}: {} proven rows on no disk, {} still on one",
                found.session,
                lost.len(),
                on_disk.len()
            )
        });
        self.stats.fleet_losses_excused += lost.len() as u64;
        self.checker.excuse(lost);
    }

    fn read_bundles(&mut self, id: ProcId, key: u64, found: Found, frames: Vec<Row>) {
        let session = found.session.clone();
        self.set_step(id, key, Step::Bundles(found, frames));
        self.recovery_call(id, key, StoreReq::ReadBundles { session });
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
        let only = match &self.log(id).recoveries[&key].job {
            Job::Fold { cell, .. } => Some(cell.clone()),
            Job::Recover { .. } => None,
        };
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
        let writer = found.writer;
        self.set_step(id, key, Step::Upload(found, end));
        self.recovery_call(id, key, StoreReq::PutRecovered { groups, writer });
    }

    /// R6: the gathered tail is in the bucket, so the record seals.
    fn gathered(&mut self, id: ProcId, key: u64, found: Found, end: u64) {
        if self.log(id).recoveries[&key].fold() {
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
        self.set_step(id, key, Step::Finish);
        self.recovery_call(id, key, req);
    }

    fn recovery_ended(&mut self, id: ProcId, key: u64, ended: Ended) {
        let log = self.log(id);
        let recovery = log.recoveries.remove(&key).unwrap();
        match recovery.job {
            Job::Recover { node, modes } => {
                log.recovering.remove(&node);
                self.note(|| format!("p{id} recovery of {node}: {ended:?}"));
                for mode in modes {
                    match mode {
                        RecoverMode::Boot => self.boot_recovered(
                            id,
                            match ended {
                                Ended::Done => None,
                                Ended::Elsewhere => Some(CLAIM_POLL_MS),
                                Ended::Failed => Some(MAINTAIN_REPAIR_MS),
                            },
                        ),
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
