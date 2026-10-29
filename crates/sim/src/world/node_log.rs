//! The node log, fleet durability (`crates/celld/node_log.rs`).
//!
//! In the fleet posture each process session ships every commit to up to
//! two followers. A write is fleet durable once every member of the
//! ensemble fsynced it (write-all, ack-all), and a fleet proof settles its
//! output gate without the ownership read a bucket proof needs. The
//! session's log record rides in its node lease (the lease fold), so the
//! next owner of one of its cells can tell whether the session may have
//! acknowledged past the bucket. Bundles carry the same rows to the bucket
//! every flush interval and credit bucket proofs while the record stays
//! open; a cell's own prefix gets its rows only at a drain point, an
//! eviction or a stop, or from recovery.
//!
//! Shipping and the followers are in [`ship`], recovery of a dead session
//! in [`recovery`]; this module keeps the session's own record, the
//! maintenance loop that installs ensembles, startup and graceful close.
//!
//! What the model leaves out: one frame per commit and one round per
//! commit, with no batching, hedging or pipelining limits; a lane delivers
//! in order and never tears a batch; followers never truncate or collect
//! fragments; a recovery claim never needs its heartbeat.

mod recovery;
mod ship;

use super::*;
use crate::store::{LogWire, Row, Session};
use celld_logic::log_tier::{self, FollowerLog, LogRecord, LogState, MaintainStep};

use recovery::{Ended, Step};
pub(super) use recovery::{RecoverMode, Recovery};
use ship::{Shipper, TailAnswer};

/// `RECRUIT_FOLLOWERS`.
const RECRUIT_FOLLOWERS: usize = 2;
/// How long a live write of the session's own log waits for a lease write
/// to carry it before the session gives the change up.
const PUBLISH_TIMEOUT_MS: u64 = 10_000;
/// `CELLD_LOG_FLUSH_MS`.
const FLUSH_MS: u64 = 2_000;
const MAINTAIN_HEALTHY_MS: u64 = 30_000;
const MAINTAIN_REPAIR_MS: u64 = 1_000;
/// How often startup polls a recovery another node is running.
const CLAIM_POLL_MS: u64 = 250;
/// How long a graceful close waits for every shipped row to reach its
/// cell's prefix before it leaves the record open.
const CLOSE_MS: u64 = 10_000;
/// How long before the audit the fleet starts closing its logs.
pub(super) const CLOSE_LEAD_MS: u64 = CLOSE_MS + 1_000;

/// One follower's copy of one session's log, on the follower's disk.
#[derive(Clone, Debug)]
pub(super) struct Fragment {
    log: FollowerLog,
    frames: BTreeMap<u64, Row>,
}

impl Disk {
    /// Whether this disk holds `row` in `session`'s fragment of `epoch`.
    pub(super) fn holds(&self, session: &Session, epoch: Epoch, row: &Row) -> bool {
        self.peerlog.get(session).is_some_and(|fragment| {
            fragment.log.fragment_epoch == epoch
                && fragment.frames.values().any(|frame| frame == row)
        })
    }
}

/// Per-process node-log state.
#[derive(Default)]
pub(super) struct LogShell {
    /// The folded log the next lease write carries, and its sequence.
    own: Option<LogWire>,
    own_seq: u64,
    publishes: Vec<Publish>,
    shipper: Option<Shipper>,
    /// Rows no credited bundle carries yet, in commit order.
    unbundled: Vec<Row>,
    /// The bundle flush in flight: the shipper epoch it credits against,
    /// and its rows.
    flush: Option<(Epoch, Vec<Row>)>,
    tasks: BTreeMap<u64, LogTask>,
    recoveries: BTreeMap<u64, Recovery>,
    /// The recovery running here for each dead node, for single flight.
    recovering: BTreeMap<String, u64>,
    /// Cells an epoch of which ended with acknowledged rows outside their
    /// prefix (`dirty_tails`): their next restore folds the tail first.
    dirty: BTreeSet<String>,
    /// The maintenance tick that counts; earlier ones are superseded.
    maintain_tick: u64,
    /// Startup recovery of the node's previous session has not finished:
    /// the process serves seals but neither appends nor its own lease.
    booting: bool,
    closing: bool,
    /// The graceful close is over: the record sealed, or it was left open.
    closed: bool,
    maintaining: bool,
}

#[derive(Debug)]
struct Publish {
    seq: u64,
    then: Then,
}

/// What a live write of the session's own log was for.
#[derive(Debug)]
enum Then {
    Install {
        epoch: Epoch,
        ensemble: BTreeSet<String>,
    },
    Activate {
        epoch: Epoch,
    },
    Seal,
}

#[derive(Debug)]
enum LogTask {
    /// Maintenance read the fleet's leases.
    MaintainPeers,
    /// Maintenance read its own record before stepping the epoch.
    MaintainRecord {
        ensemble: BTreeSet<String>,
    },
    /// A bundle PUT, then the record read that may credit it.
    Flush,
    FlushCheck,
    /// A step of the recovery under `key`.
    Recover {
        key: u64,
        step: Box<Step>,
    },
}

/// Node-log work that runs inside a process.
#[derive(Debug)]
pub(in crate::world) enum LogWork {
    Append {
        leader: ProcId,
        session: Session,
        epoch: Epoch,
        frames: Vec<(u64, Row)>,
    },
    Appended {
        epoch: Epoch,
        last: u64,
        member: String,
        answer: Option<(Epoch, u64)>,
    },
    RoundDeadline {
        epoch: Epoch,
        last: u64,
    },
    /// A seal, or with `seal` false a tail read, of a session's fragment.
    Gather {
        recoverer: ProcId,
        key: u64,
        session: Session,
        epoch: Epoch,
        seal: bool,
    },
    Gathered {
        key: u64,
        member: String,
        answer: TailAnswer,
    },
    /// A gather request failed to connect, or its deadline passed.
    GatherFailed {
        key: u64,
        member: String,
    },
    PublishDeadline {
        seq: u64,
    },
    FlushTick,
    MaintainTick {
        tick: u64,
    },
    CloseCheck {
        deadline: Time,
    },
    BootRetry,
}

impl World {
    /// The node-log session of a process.
    pub(super) fn session_of(&self, id: ProcId) -> Session {
        Session {
            node: self.node_name(id),
            proc: id,
        }
    }

    fn node_index(&self, name: &str) -> Option<usize> {
        self.nodes.iter().position(|node| node.name == name)
    }

    fn log(&mut self, id: ProcId) -> &mut LogShell {
        &mut self.procs.get_mut(&id).unwrap().log
    }

    fn log_store_call(&mut self, id: ProcId, req: StoreReq, task: LogTask) -> u64 {
        let key = self.next_task;
        self.next_task += 1;
        self.log(id).tasks.insert(key, task);
        self.store_call(id, req, Reply::Log { task: key });
        key
    }

    pub(super) fn log_work(&mut self, id: ProcId, work: LogWork) {
        match work {
            LogWork::Append {
                leader,
                session,
                epoch,
                frames,
            } => self.append_arrived(id, leader, &session, epoch, frames),
            LogWork::Appended {
                epoch,
                last,
                member,
                answer,
            } => self.append_answered(id, epoch, last, &member, answer),
            LogWork::RoundDeadline { epoch, last } => self.round_deadline(id, epoch, last),
            LogWork::Gather {
                recoverer,
                key,
                session,
                epoch,
                seal,
            } => self.gather_arrived(id, recoverer, key, &session, epoch, seal),
            LogWork::Gathered {
                key,
                member,
                answer,
            } => self.member_answered(id, key, &member, Some(answer)),
            LogWork::GatherFailed { key, member } => self.member_answered(id, key, &member, None),
            LogWork::PublishDeadline { seq } => self.publish_deadline(id, seq),
            LogWork::FlushTick => self.flush_tick(id),
            LogWork::MaintainTick { tick } => self.maintain_tick(id, tick),
            LogWork::CloseCheck { deadline } => self.close_check(id, deadline),
            LogWork::BootRetry => self.boot_recover(id),
        }
    }

    pub(super) fn log_reply(&mut self, id: ProcId, key: u64, result: Result<StoreRes, Failure>) {
        let Some(task) = self.log(id).tasks.remove(&key) else {
            return;
        };
        match task {
            LogTask::MaintainPeers => self.maintain_peers(id, result),
            LogTask::MaintainRecord { ensemble } => self.maintain_record(id, ensemble, result),
            LogTask::Flush => self.flush_landed(id, result.is_ok()),
            LogTask::FlushCheck => {
                let record = self.own_record(id, result);
                self.flush_checked(id, record);
            }
            LogTask::Recover { key, step } => self.recovery_reply(id, key, *step, result),
        }
    }

    /// The session's own record from a lease read, if the lease is still
    /// this session's; `Err` when the read failed or the lease moved on.
    fn own_record(
        &self,
        id: ProcId,
        result: Result<StoreRes, Failure>,
    ) -> Result<Option<LogRecord>, Failure> {
        let session = self.session_of(id);
        match result? {
            StoreRes::LeaseLog(Some((lease, log))) if session.owns(&lease) => {
                Ok(log.map(|log| log.record))
            }
            StoreRes::LeaseLog(_) => Err(Failure::Definite),
            other => unreachable!("expected a lease, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // The lease fold

    /// What the next lease write serializes: the session's folded log, and
    /// the sequence an applied write proves published.
    pub(super) fn stamp_log(&self, id: ProcId) -> (Option<LogWire>, u64) {
        let log = &self.procs[&id].log;
        (log.own.clone(), log.own_seq)
    }

    /// A lease write carrying own-log sequence `seq` landed.
    pub(super) fn lease_applied(&mut self, id: ProcId, seq: u64) {
        let log = self.log(id);
        let (done, waiting) = std::mem::take(&mut log.publishes)
            .into_iter()
            .partition(|publish| publish.seq <= seq);
        log.publishes = waiting;
        for publish in done {
            self.published(id, publish.then, true);
        }
    }

    /// `OwnLog::write`: replace the session's folded log, nudge the core to
    /// renew now, and wait for a lease write that carries it.
    fn publish_own(&mut self, id: ProcId, wire: LogWire, then: Then) {
        let log = self.log(id);
        log.own = Some(wire);
        log.own_seq += 1;
        let seq = log.own_seq;
        log.publishes.push(Publish { seq, then });
        self.work(
            id,
            PUBLISH_TIMEOUT_MS,
            Work::Log(LogWork::PublishDeadline { seq }),
        );
        self.drive(
            id,
            Event::NudgeNodeLease {
                now_ms: 0,
                now_mono_ms: 0,
            },
        );
    }

    fn publish_deadline(&mut self, id: ProcId, seq: u64) {
        let log = self.log(id);
        let Some(index) = log.publishes.iter().position(|publish| publish.seq == seq) else {
            return;
        };
        let publish = log.publishes.remove(index);
        self.published(id, publish.then, false);
    }

    fn published(&mut self, id: ProcId, then: Then, ok: bool) {
        match then {
            Then::Install { epoch, ensemble } => {
                if ok {
                    self.install_shipper(id, epoch, ensemble);
                }
                let delay = if ok {
                    MAINTAIN_HEALTHY_MS
                } else {
                    MAINTAIN_REPAIR_MS
                };
                self.maintain_done(id, delay);
            }
            Then::Activate { epoch } => self.activated(id, epoch, ok),
            Then::Seal => self.log(id).closed = true,
        }
    }

    // -----------------------------------------------------------------------
    // Maintenance: the ensemble, and the eager sweep of dead sessions

    /// Run maintenance `delay` from now, instead of whenever it would have.
    pub(super) fn schedule_maintain(&mut self, id: ProcId, delay: u64) {
        let log = self.log(id);
        log.maintain_tick += 1;
        let tick = log.maintain_tick;
        self.work(id, delay, Work::Log(LogWork::MaintainTick { tick }));
    }

    fn maintain_tick(&mut self, id: ProcId, tick: u64) {
        let proc = &self.procs[&id];
        if tick != proc.log.maintain_tick || proc.log.maintaining || proc.log.closing {
            return;
        }
        if !proc.state.node_authoritative() {
            return self.schedule_maintain(id, MAINTAIN_REPAIR_MS);
        }
        self.log(id).maintaining = true;
        self.log_store_call(id, StoreReq::LogPeers, LogTask::MaintainPeers);
    }

    fn maintain_done(&mut self, id: ProcId, delay: u64) {
        self.log(id).maintaining = false;
        self.schedule_maintain(id, delay);
    }

    fn maintain_peers(&mut self, id: ProcId, result: Result<StoreRes, Failure>) {
        let peers = match result {
            Ok(StoreRes::LogPeers(peers)) => peers,
            Ok(other) => unreachable!("expected the leases, got {other:?}"),
            Err(_) => return self.maintain_done(id, MAINTAIN_REPAIR_MS),
        };
        let own = self.node_name(id);
        let now = self.wall(id);
        // The eager sweep: every dead session that may hold acknowledged
        // writes outside the bucket.
        for lease in &peers {
            let unsealed = !matches!(lease.log_state, None | Some(LogState::Sealed));
            if lease.node != own && lease.expires_ms <= now && unsealed {
                self.recover(id, lease.node.clone(), RecoverMode::Sweep);
            }
        }
        let mut live: Vec<String> = peers
            .into_iter()
            .filter(|lease| lease.node != own && lease.expires_ms > now)
            .map(|lease| lease.node)
            .collect();
        // Production reads the leases unordered; any order is one it can take.
        let mut ensemble = BTreeSet::new();
        while !live.is_empty() && ensemble.len() < RECRUIT_FOLLOWERS {
            let index = self.net_rng.below(live.len() as u64) as usize;
            ensemble.insert(live.swap_remove(index));
        }
        let (healthy, width) = self.procs[&id]
            .log
            .shipper
            .as_ref()
            .map_or((false, 0), |shipper| (shipper.healthy(), shipper.width()));
        if ensemble.is_empty() || (healthy && ensemble.len() <= width) {
            let delay = if healthy {
                MAINTAIN_HEALTHY_MS
            } else {
                MAINTAIN_REPAIR_MS
            };
            return self.maintain_done(id, delay);
        }
        // Reconfigure: stop fleet acknowledgements, and step the epoch only
        // once every shipped frame is in the bucket, so the new members'
        // emptiness is covered.
        self.degrade(id, "maintenance swaps the ensemble");
        let log = &self.procs[&id].log;
        let covered = log
            .shipper
            .as_ref()
            .is_none_or(|shipper| shipper.rows().all(|row| self.store.holds(row)));
        if !log_tier::may_reconfigure(log.flush.is_some(), covered) {
            return self.maintain_done(id, MAINTAIN_REPAIR_MS);
        }
        self.log_store_call(
            id,
            StoreReq::ReadLeaseLog { node: own },
            LogTask::MaintainRecord { ensemble },
        );
    }

    fn maintain_record(
        &mut self,
        id: ProcId,
        ensemble: BTreeSet<String>,
        result: Result<StoreRes, Failure>,
    ) {
        let Ok(record) = self.own_record(id, result) else {
            return self.maintain_done(id, MAINTAIN_REPAIR_MS);
        };
        let record = match log_tier::maintain_step(record.as_ref()) {
            MaintainStep::CreateFresh => log_tier::create_record(ensemble.clone(), 0),
            MaintainStep::Reopen(epoch) => Some(LogRecord {
                epoch,
                ensemble: ensemble.clone(),
                tiered: 0,
                state: LogState::Open,
                claimant: None,
                claimed_ms: None,
            }),
            MaintainStep::Wait => None,
        };
        let Some(record) = record else {
            return self.maintain_done(id, MAINTAIN_REPAIR_MS);
        };
        let epoch = record.epoch;
        let wire = LogWire {
            record,
            active: false,
        };
        self.publish_own(id, wire, Then::Install { epoch, ensemble });
    }

    // -----------------------------------------------------------------------
    // Startup and graceful close

    /// A process in the fleet posture recovers its node's previous session
    /// before its own lease can replace that session's record.
    pub(super) fn boot(&mut self, id: ProcId) {
        self.log(id).booting = true;
        self.boot_recover(id);
    }

    fn boot_recover(&mut self, id: ProcId) {
        let node = self.node_name(id);
        self.recover(id, node, RecoverMode::Boot);
    }

    fn boot_recovered(&mut self, id: ProcId, ended: Ended) {
        let retry_ms = match ended {
            Ended::Done => None,
            Ended::Elsewhere => Some(CLAIM_POLL_MS),
            Ended::Failed => Some(MAINTAIN_REPAIR_MS),
        };
        if let Some(delay) = retry_ms {
            return self.work(id, delay, Work::Log(LogWork::BootRetry));
        }
        self.log(id).booting = false;
        self.start_session(id);
        self.work(id, FLUSH_MS, Work::Log(LogWork::FlushTick));
        self.schedule_maintain(id, MAINTAIN_REPAIR_MS);
    }

    /// Every running process closes its node log, as a fleet shutdown does.
    pub(super) fn close_logs(&mut self) {
        let procs: Vec<ProcId> = self.procs.keys().copied().collect();
        for id in procs {
            self.close_gracefully(id);
        }
    }

    /// `close_gracefully`: stop fleet acknowledgements, drain every shipped
    /// row into its cell's prefix, and seal the record if that finishes in
    /// time. A record left open is recovered by whoever comes next.
    pub(super) fn close_gracefully(&mut self, id: ProcId) {
        let log = self.log(id);
        if log.closing {
            return;
        }
        log.closing = true;
        self.degrade(id, "the node closes");
        let deadline = self.t + CLOSE_MS;
        self.work(id, 100, Work::Log(LogWork::CloseCheck { deadline }));
    }

    /// Whether a graceful close has finished.
    pub(super) fn log_closed(&self, id: ProcId) -> bool {
        self.procs[&id].log.closed
    }

    fn close_check(&mut self, id: ProcId, deadline: Time) {
        let proc = &self.procs[&id];
        let (Some(shipper), Some(own)) = (&proc.log.shipper, &proc.log.own) else {
            // No record to seal.
            self.log(id).closed = true;
            return;
        };
        // Only the per-cell layout counts: a sealed record tells every
        // future recovery there is nothing to gather. The retained bundle
        // scan covers the cells this process no longer holds a replica of.
        let in_prefix = |row: &Row| self.store.covers(row);
        let per_cell_complete = proc
            .replicas
            .values()
            .all(|replica| replica.uploaded >= replica.content.len())
            && shipper.rows().all(in_prefix)
            && proc.log.unbundled.iter().all(in_prefix)
            && (self.config.mutation == Mutation::SealOverBundles
                || self
                    .store
                    .bundle_rows(&self.session_of(id))
                    .iter()
                    .all(in_prefix));
        if log_tier::graceful_seal_allowed(
            &own.record,
            shipper.epoch(),
            proc.log.flush.is_some(),
            per_cell_complete,
        ) {
            let wire = LogWire {
                record: LogRecord {
                    state: LogState::Sealed,
                    ..own.record.clone()
                },
                active: own.active,
            };
            self.publish_own(id, wire, Then::Seal);
        } else if self.t < deadline {
            self.work(id, 100, Work::Log(LogWork::CloseCheck { deadline }));
        } else {
            self.log(id).closed = true;
        }
    }
}
