//! The fleet log, as `node_log.rs` and `log_tier.rs` run it in the fleet
//! posture (`CELLD_DURABILITY=fleet`).
//!
//! - A process opens a log when it starts: a *session*, named by its lease
//!   generation, with up to two followers on other machines. Its lease
//!   writes carry the log's state (`Open`) in their body, and it proves
//!   nothing through the fleet until one of them has landed.
//! - Each commit ships the cell's database to every follower, which keeps
//!   it on its disk. Once every follower has confirmed a position, the
//!   output gate may release it: `DurableReached { source: Fleet }`, with
//!   no read of the owner record. The bucket gets the same writes only on
//!   the bundle loop's tick, as one bundle of the session
//!   (`log/<session>/bundle/<seq>`) for every open database with writes
//!   past what the bucket covers. A bundle counts as durable for the gate
//!   only if the record is still open at its log epoch once it lands
//!   (`bundle_credit_allowed`), and otherwise degrades its shipper: a
//!   fenced leader's late bundle proves nothing. No restore reads bundles; a stop's final upload, a fold and a
//!   recovery copy their rows into the per-cell prefixes.
//! - When a node takes over a cell from a dead owner whose lease says its
//!   log is not sealed, the core asks for `RecoverNodeLog` first. Recovery
//!   seals every follower it can reach, so the dead leader can never again
//!   prove a write through them, uploads what they hold into the bucket,
//!   and marks the lease body `Sealed`. A restart does the same for its own
//!   predecessor before it takes the lease over.
//! - A cell epoch that ends without its final upload keeps what its log
//!   shipped or a bundle covers as a tail (`Process::tails`).
//!   The cell's next restore here first folds in what the followers and
//!   the session's bundles hold of it (`fold_cell`); a recovery drains every
//!   bundle of the session.
//! - A follower is a complete witness when it holds every frame of the
//!   session's current log epoch up to the highest it received. Without
//!   one, a session that may have fleet acks at that epoch fails a fold,
//!   and makes recovery wait for every follower that might still come
//!   back; once all are conclusively gone, recovery declares a bounded
//!   loss, as production does. A booting process answers for its disk
//!   while it waits for its own predecessor
//!   (`recover_with_follower_listener`).
//! - A member that has not confirmed a frame within the backstop degrades
//!   the shipper: nothing ships, and the gate rides the bundle loop. Once
//!   the bucket holds everything the log shipped, of open databases and
//!   tails (`may_reconfigure`), the degraded leader reopens the log at the
//!   next log epoch with up to two live peers, by a lease write that
//!   applies only while the lease is still its own and open. The new epoch
//!   has no fleet acks yet, and its fragments start where the old epoch's
//!   shipping stopped: a joiner holds only the rows from there on, and a
//!   follower drops what it held of an earlier epoch when it adopts a new
//!   one. Fold and recovery read the current epoch's members only.
//!
//! Not modelled: gray-failure eviction (`log_evict`'s latency verdicts and
//! quarantine), the swap rate cap, recruiting a healthy log that runs below
//! its width, reading peers' leases through a call that can fail (the
//! reopen reads the bucket directly), compaction of bundle rows into
//! per-cell objects, and bundle GC (bundles are never deleted).

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::log_tier::LogState;
use celld_logic::{Epoch, Event, Failure, OpId};

use super::replicator::Tail;
use super::shell::{Fate, StoreReq};
use super::{shutdown, Item, Work, World, OPERATION_DEADLINE_MS};
use crate::store::Row;
use crate::{Mutation, WriteId};

/// How often the bundle loop flushes to the bucket (`CELLD_LOG_FLUSH_MS`).
const FLUSH_MS: u64 = 2_000;
/// Followers per session.
const FOLLOWERS: usize = 2;
/// One-way latency between two nodes.
const PEER_MS: (u64, u64) = (1, 5);
/// How long an append may stay unconfirmed before its member counts as
/// failed and the shipper degrades (`log_evict`'s backstop).
const APPEND_BACKSTOP_MS: u64 = 1_500;
/// How often a degraded shipper tries to re-recruit its ensemble
/// (`maintain`, from the posture watch).
const MAINTAIN_MS: u64 = 1_000;

/// The log a process leads.
pub(super) struct Log {
    session: String,
    /// The log epoch the shipper ships at: the record's, once the leader
    /// heard its reopen applied.
    epoch: u64,
    /// A lease write carrying `Open` landed: fleet proofs may begin.
    open: bool,
    /// A member failed an append: nothing ships until a reopen installs
    /// a new ensemble, and the gate rides the bundle loop.
    degraded: bool,
    /// A reopen's lease write is in flight.
    reopening: bool,
    /// The members that confirmed each frame of this epoch still inside
    /// its backstop.
    pending: BTreeMap<u64, BTreeSet<usize>>,
    /// The longest database of each cell epoch each follower confirmed.
    confirmed: BTreeMap<(usize, String, Epoch), usize>,
    /// The longest database of each cell epoch shipped so far.
    shipped: BTreeMap<(String, Epoch), usize>,
    /// Where this log epoch's fragment of each cell epoch starts: what
    /// was shipped when it reopened, which its barrier put in the bucket.
    /// A joiner holds only the rows from there on.
    floor: BTreeMap<(String, Epoch), usize>,
    /// A bundle flush is in flight: the loop awaits it before its next.
    bundling: bool,
}

/// What a machine's disk holds as a follower. A wipe loses all of it.
#[derive(Default)]
pub(super) struct FollowerDisk {
    /// Sessions this disk joined, and how many of their frames it holds
    /// and the highest it received. Holding every frame up to the highest
    /// makes it a complete witness, as a ranged tail that covers its range
    /// does (`range.complete`). A frame a partition dropped leaves a gap
    /// for good; a wipe forgets the session, so the frames after it do too.
    joined: BTreeMap<String, Frames>,
    /// Sessions recovery sealed here: it refuses their appends forever.
    sealed: BTreeSet<String>,
    copies: BTreeMap<(String, String, Epoch), Vec<WriteId>>,
    /// Where each copy's fragment starts: the rows below it are the
    /// bucket's to supply.
    floors: BTreeMap<(String, String, Epoch), usize>,
}

/// The frames of one log epoch of a session a follower holds.
#[derive(Clone, Copy, Default)]
struct Frames {
    epoch: u64,
    held: u64,
    highest: u64,
}

/// One session as its log record names it: its leader, its log epoch and
/// that epoch's followers, how many frames it has shipped at it, and
/// whether the fleet may have proved a write at it (`may_have_fleet_acks`,
/// set as the first credit is, cleared by a reopen).
pub(super) struct Session {
    node: usize,
    inc: u64,
    epoch: u64,
    ensemble: Vec<usize>,
    frames: u64,
    proved: bool,
}

/// What the members of a session that answer hold of it.
struct Gathered {
    answering: Vec<usize>,
    /// The longest copy of each cell epoch among them.
    copies: BTreeMap<(String, Epoch), Vec<WriteId>>,
    /// One of them holds everything the session shipped.
    witnessed: bool,
    /// A member that did not answer might still come back.
    undecided: bool,
}

/// When the replicator uploads a cell epoch to the bucket.
pub(super) enum Upload {
    /// A handler committed a write.
    Commit,
    /// A proof waits on the bucket: an output gate's, or an eviction's.
    Wait { evict: bool },
    /// An upload landed, and the epoch may still have more to send; its
    /// stop may be waiting for the last of it.
    Landed { stopping: bool },
}

/// How a bundle flush ended.
pub(super) enum Flushed {
    /// The PUT failed, or nobody knows whether it applied.
    Failed,
    /// It landed with the record still open at its log epoch: the rows,
    /// each a cell epoch and its length, count for the gate.
    Credited(Vec<(String, Epoch, usize)>),
    /// It landed after the record moved under it: the shipper of
    /// `log_epoch` degrades.
    Refused { log_epoch: u64 },
}

pub(super) enum LogWork {
    /// A leader asks this follower to join its session.
    Join { session: String },
    /// A leader ships a cell epoch's database to this follower.
    Append {
        session: String,
        leader: (usize, u64),
        log_epoch: u64,
        /// The frame's place in the log epoch, from 1.
        frame: u64,
        cell: String,
        epoch: Epoch,
        /// Where the fragment starts (`Log::floor`).
        from: usize,
        content: Vec<WriteId>,
    },
    /// A follower confirmed holding `len` writes of a cell epoch.
    Confirmed {
        follower: usize,
        log_epoch: u64,
        frame: u64,
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    /// A lease write carrying `Open` applied: the log is open, and the core
    /// hears of the write.
    LeaseOpened(Event),
    /// The bundle loop's tick.
    Flush,
    /// A bundle flush ended.
    Bundled(Flushed),
    /// A frame's backstop ran out: every member must have confirmed it.
    Backstop { log_epoch: u64, frame: u64 },
    /// The degraded shipper's maintenance tick.
    Maintain,
    /// A reopen's lease write answered: the record's epoch and ensemble
    /// if it moved past the shipper's, or nothing.
    Reopened(Option<(u64, Vec<usize>)>),
    /// `RecoverNodeLog` for the cells of `owner`, answered `answer_ms`
    /// later unless the answer is lost.
    Recover {
        op: OpId,
        owner: String,
        answer_ms: Option<u64>,
    },
}

impl World {
    pub(super) fn log_work(&mut self, node: usize, work: LogWork) {
        match work {
            LogWork::Join { session } => {
                let joined = &mut self.machines[node].disk.log.joined;
                joined.entry(session).or_default();
            }
            LogWork::Append {
                session,
                leader,
                log_epoch,
                frame,
                cell,
                epoch,
                from,
                content,
            } => {
                let at = (log_epoch, frame);
                self.append(node, session, leader, at, cell, epoch, from, content)
            }
            LogWork::Confirmed {
                follower,
                log_epoch,
                frame,
                cell,
                epoch,
                len,
            } => self.confirmed(node, follower, (log_epoch, frame), cell, epoch, len),
            LogWork::Backstop { log_epoch, frame } => self.backstop(node, log_epoch, frame),
            LogWork::Maintain => self.maintain(node),
            LogWork::Reopened(record) => self.reopened(node, record),
            LogWork::LeaseOpened(event) => {
                self.log_opened(node);
                self.drive(node, event);
            }
            LogWork::Flush => {
                self.work(node, FLUSH_MS, Work::Log(LogWork::Flush));
                let rows = self.flush_rows(node);
                let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
                    return;
                };
                if log.bundling || rows.is_empty() {
                    return;
                }
                log.bundling = true;
                let req = StoreReq::Bundle {
                    session: log.session.clone(),
                    log_epoch: log.epoch,
                    rows,
                };
                self.store_call(node, req);
            }
            LogWork::Bundled(flushed) => {
                let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
                    return;
                };
                log.bundling = false;
                match flushed {
                    Flushed::Failed => {}
                    Flushed::Credited(rows) => self.bundled(node, rows),
                    // Only the shipper that owned the flush's epoch.
                    Flushed::Refused { log_epoch } if log_epoch == log.epoch => {
                        self.degrade(node, "the record moved under a bundle flush");
                    }
                    Flushed::Refused { .. } => {}
                }
            }
            LogWork::Recover {
                op,
                owner,
                answer_ms,
            } => {
                let result = match self.config.mutation {
                    Some(Mutation::SkipLogRecovery) => Ok(()),
                    _ => self.recover(node, &owner),
                };
                if let Some(delay) = answer_ms {
                    self.deliver(node, delay, Event::NodeLogRecovered { op, result });
                }
            }
        }
    }

    /// The process on `node` starts: in the fleet posture, open its log
    /// with up to two followers among the other machines. A machine alone
    /// opens none (`create_record` refuses an empty ensemble).
    pub(super) fn open_log(&mut self, node: usize) {
        if !self.config.fleet {
            return;
        }
        let mut peers: Vec<usize> = (0..self.machines.len())
            .filter(|&m| m != node && !self.machines[m].retired)
            .collect();
        let mut ensemble = Vec::new();
        while ensemble.len() < FOLLOWERS && !peers.is_empty() {
            let i = self.net_rng.below(peers.len() as u64) as usize;
            ensemble.push(peers.remove(i));
        }
        if ensemble.is_empty() {
            return;
        }
        let inc = self.machines[node].inc;
        let session = shutdown::generation(node, inc);
        for &follower in &ensemble {
            let work = Work::Log(LogWork::Join {
                session: session.clone(),
            });
            self.peer_send(node, follower, None, work);
        }
        let entry = Session {
            node,
            inc,
            epoch: 1,
            ensemble: ensemble.clone(),
            frames: 0,
            proved: false,
        };
        self.sessions.insert(session.clone(), entry);
        let who = self.who(node);
        self.checker.log_opened(&who, &ensemble);
        self.machines[node].proc_mut().log = Some(Log {
            session,
            epoch: 1,
            open: false,
            degraded: false,
            reopening: false,
            pending: BTreeMap::new(),
            confirmed: BTreeMap::new(),
            shipped: BTreeMap::new(),
            floor: BTreeMap::new(),
            bundling: false,
        });
        self.work(node, FLUSH_MS, Work::Log(LogWork::Flush));
    }

    /// Whether the replicator uploads now. In the bucket posture every
    /// write goes up at once. A process that leads a log flushes on the
    /// bundle loop's tick instead; only an eviction's proof kicks the
    /// flush (`handoff_wait`), and a stop's final upload runs to the end.
    pub(super) fn upload_due(&self, node: usize, why: Upload) -> bool {
        self.machines[node].proc().log.is_none()
            || match why {
                Upload::Commit => false,
                Upload::Wait { evict } => evict,
                Upload::Landed { stopping } => stopping,
            }
    }

    /// The longest database of the cell epoch the process's log shipped.
    pub(super) fn shipped_len(&self, node: usize, cell: &str, epoch: Epoch) -> usize {
        let log = self.machines[node].proc().log.as_ref();
        log.and_then(|log| log.shipped.get(&(cell.to_string(), epoch)))
            .copied()
            .unwrap_or(0)
    }

    /// The fleet just proved a write for this process: its session may
    /// have fleet acks from now on.
    pub(super) fn fleet_proved(&mut self, node: usize) {
        self.stats.fleet_proofs += 1;
        if let Some(log) = self.machines[node].proc().log.as_ref() {
            let session = log.session.clone();
            self.session_mut(&session).proved = true;
        }
    }

    fn session(&self, session: &str) -> &Session {
        self.sessions
            .get(session)
            .expect("an open log names a session that opened")
    }

    fn session_mut(&mut self, session: &str) -> &mut Session {
        self.sessions
            .get_mut(session)
            .expect("an open log names a session that opened")
    }

    /// The lease now says the log is open: ship everything open.
    fn log_opened(&mut self, node: usize) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.open {
            return;
        }
        log.open = true;
        let open: Vec<_> = self.open_epochs(node);
        for (cell, epoch) in open {
            self.ship(node, &cell, epoch);
        }
    }

    /// Send work to a process on another machine: the one at `inc`, or by
    /// default whichever runs there now. A partition on either side loses
    /// it: the sender's when it sends, the receiver's when it arrives.
    fn peer_send(&mut self, from: usize, to: usize, inc: Option<u64>, work: Work) {
        if self.machines[from].partition.is_some() {
            return;
        }
        let delay = self.net_rng.range(PEER_MS.0, PEER_MS.1);
        let inc = inc.unwrap_or(self.machines[to].inc);
        self.after(
            delay,
            Item::Peer {
                node: to,
                inc,
                work,
            },
        );
    }

    /// Ship a cell epoch's database to every follower.
    pub(super) fn ship(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let Some(content) = self.seen(node, cell, epoch).map(<[WriteId]>::to_vec) else {
            return;
        };
        let leader = (node, self.machines[node].inc);
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if !log.open || log.degraded {
            return;
        }
        let len = log.shipped.entry((cell.to_string(), epoch)).or_default();
        *len = (*len).max(content.len());
        let (session, log_epoch) = (log.session.clone(), log.epoch);
        let from = log.floor.get(&(cell.to_string(), epoch)).copied();
        let from = from.unwrap_or(0);
        let entry = self.session_mut(&session);
        entry.frames += 1;
        let (frame, ensemble) = (entry.frames, entry.ensemble.clone());
        let backstop = Work::Log(LogWork::Backstop { log_epoch, frame });
        self.work(node, APPEND_BACKSTOP_MS, backstop);
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        log.pending.insert(frame, BTreeSet::new());
        for follower in ensemble {
            let work = Work::Log(LogWork::Append {
                session: session.clone(),
                leader,
                log_epoch,
                frame,
                cell: cell.to_string(),
                epoch,
                from,
                content: content.clone(),
            });
            self.peer_send(node, follower, None, work);
        }
    }

    /// A follower receives a cell epoch's database. It keeps it on its disk
    /// and confirms, unless recovery sealed the session here or it already
    /// follows a later log epoch of it. A follower that does not know the
    /// session joins it now; one that knows an earlier epoch adopts this
    /// one and drops what it held of the earlier (`adopt`).
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        node: usize,
        session: String,
        leader: (usize, u64),
        (log_epoch, frame): (u64, u64),
        cell: String,
        epoch: Epoch,
        from: usize,
        content: Vec<WriteId>,
    ) {
        let ignore_seal = self.config.mutation == Some(Mutation::FollowerIgnoresSeal);
        let disk = &mut self.machines[node].disk.log;
        if disk.sealed.contains(&session) && !ignore_seal {
            return;
        }
        let frames = disk.joined.entry(session.clone()).or_default();
        if log_epoch < frames.epoch {
            return;
        }
        if log_epoch > frames.epoch {
            *frames = Frames {
                epoch: log_epoch,
                ..Frames::default()
            };
            disk.copies.retain(|(s, _, _), _| *s != session);
            disk.floors.retain(|(s, _, _), _| *s != session);
        }
        let frames = disk.joined.get_mut(&session).expect("joined");
        frames.held += 1;
        frames.highest = frames.highest.max(frame);
        let key = (session.clone(), cell.clone(), epoch);
        disk.floors.entry(key).or_insert(from);
        let copy = disk
            .copies
            .entry((session.clone(), cell.clone(), epoch))
            .or_default();
        if content.len() > copy.len() {
            *copy = content;
            if self.config.mutation == Some(Mutation::FollowerCorrupts) {
                *copy.last_mut().expect("longer than empty") += 1_000_000;
            }
        }
        let held = copy.clone();
        let len = held.len();
        let leader_who = self.who_at(leader.0, leader.1);
        // The oracle counts a copy of the record's epoch only: what a
        // member holds of an earlier one, the reopen's barrier tiered.
        if self.session(&session).epoch == log_epoch {
            self.checker
                .follower_fsynced(&leader_who, node, &cell, epoch, &held);
        }
        let work = Work::Log(LogWork::Confirmed {
            follower: node,
            log_epoch,
            frame,
            cell,
            epoch,
            len,
        });
        self.peer_send(node, leader.0, Some(leader.1), work);
    }

    /// A follower's confirmation reaches its leader. Once every follower
    /// holds a position, the fleet has proved it. A confirmation of an
    /// earlier log epoch proves nothing.
    fn confirmed(
        &mut self,
        node: usize,
        follower: usize,
        (log_epoch, frame): (u64, u64),
        cell: String,
        epoch: Epoch,
        len: usize,
    ) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.epoch != log_epoch {
            return;
        }
        if let Some(confirmers) = log.pending.get_mut(&frame) {
            confirmers.insert(follower);
        }
        let held = log
            .confirmed
            .entry((follower, cell.clone(), epoch))
            .or_default();
        *held = (*held).max(len);
        let log = self.machines[node].proc().log.as_ref().expect("checked");
        let fleet = (self.session(&log.session).ensemble.iter())
            .map(|f| {
                let key = (*f, cell.clone(), epoch);
                log.confirmed.get(&key).copied().unwrap_or(0)
            })
            .min()
            .unwrap_or(0);
        self.fleet_reached(node, &cell, epoch, fleet);
    }

    /// A frame's backstop ran out. A member that has not confirmed it
    /// failed its append: degrade (`degrade_shared`).
    fn backstop(&mut self, node: usize, log_epoch: u64, frame: u64) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        // Frames restart at 1 in each log epoch: an earlier epoch's
        // backstop leaves this one's frames alone.
        if log.epoch != log_epoch {
            return;
        }
        let Some(confirmers) = log.pending.remove(&frame) else {
            return;
        };
        let session = log.session.clone();
        if confirmers.len() < self.session(&session).ensemble.len() {
            self.degrade(node, &format!("frame {frame} unconfirmed"));
        }
    }

    /// Stop shipping: the gate rides the bundle loop until a reopen
    /// (`degrade_shared`).
    fn degrade(&mut self, node: usize, why: &str) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.degraded {
            return;
        }
        log.degraded = true;
        log.pending.clear();
        let (session, epoch) = (log.session.clone(), log.epoch);
        self.note(|| format!("n{node} degrades {session} epoch {epoch}: {why}"));
        self.work(node, MAINTAIN_MS, Work::Log(LogWork::Maintain));
    }

    /// A bundle's PUT applies, and its rows are in the bucket whatever
    /// follows. They count for the gate only if the record is still open
    /// at the flush's log epoch (`bundle_credit_allowed`), and as held only
    /// while a recovery can still drain them: once one has sealed the
    /// session, no one lists its bundles again.
    pub(super) fn apply_bundle(
        &mut self,
        name: &str,
        session: &str,
        log_epoch: u64,
        rows: Vec<Row>,
    ) -> Flushed {
        let lens: Vec<_> = (rows.iter())
            .map(|(cell, epoch, content)| (cell.clone(), *epoch, content.len()))
            .collect();
        let lease = self.store.read_lease(name);
        let state = lease.filter(|lease| lease.generation == session);
        let state = state.map(|lease| lease.log_state);
        let drainable = state.is_some_and(|state| state != Some(LogState::Sealed));
        self.store.put_bundle(session, rows, drainable);
        for (cell, epoch, _) in &lens {
            let held = self.store.held(cell, *epoch).len();
            self.checker.prefix_grew(self.t, cell, *epoch, held);
            self.judge_discoverable(cell);
        }
        let open = state == Some(Some(LogState::Open)) && self.session(session).epoch == log_epoch;
        if open || self.config.mutation == Some(Mutation::CreditAfterSeal) {
            Flushed::Credited(lens)
        } else {
            Flushed::Refused { log_epoch }
        }
    }

    /// The degraded shipper's tick (`maintain`): once every database's
    /// shipped rows are in the bucket, its own and its tails'
    /// (`may_reconfigure`), reopen the log at the next epoch with up to two
    /// live peers. Until then, and while a reopen is in flight, try again.
    fn maintain(&mut self, node: usize) {
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        if !log.degraded {
            return;
        }
        let reopening = log.reopening;
        self.work(node, MAINTAIN_MS, Work::Log(LogWork::Maintain));
        let untiered = self.config.mutation == Some(Mutation::ReopenUntiered);
        if reopening || !(untiered || self.all_shipped_tiered(node)) {
            return;
        }
        let now = self.wall(node);
        let mut peers: Vec<usize> = (0..self.machines.len())
            .filter(|&m| m != node && !self.machines[m].retired)
            .filter(|&m| {
                let lease = self.store.read_lease(&self.machines[m].name);
                lease.is_some_and(|lease| lease.expires_ms > now)
            })
            .collect();
        let mut ensemble = Vec::new();
        while ensemble.len() < FOLLOWERS && !peers.is_empty() {
            let i = self.net_rng.below(peers.len() as u64) as usize;
            ensemble.push(peers.remove(i));
        }
        if ensemble.is_empty() {
            return;
        }
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        log.reopening = true;
        let req = StoreReq::Reopen {
            session: log.session.clone(),
            from: log.epoch,
            ensemble,
        };
        self.store_call(node, req);
    }

    /// `may_reconfigure`'s barrier: every row the log shipped of an open
    /// database, and every tail, is in the bucket, in a prefix or a bundle.
    fn all_shipped_tiered(&self, node: usize) -> bool {
        let p = self.machines[node].proc();
        let Some(log) = p.log.as_ref() else {
            return true;
        };
        let tiered = |cell: &str, epoch: Epoch| {
            let shipped = log.shipped.get(&(cell.to_string(), epoch)).copied();
            let durable = self.durable_here(node, cell, epoch);
            durable >= shipped.unwrap_or(0)
        };
        p.replicas.keys().all(|(cell, epoch)| tiered(cell, *epoch))
            && p.tails.values().all(Tail::tiered)
    }

    /// The reopen's lease write applies: if the lease is still this
    /// session's and open, and the record still at `from`, move it to the
    /// next epoch with `ensemble`, with no fleet acks yet.
    pub(super) fn apply_reopen(
        &mut self,
        name: &str,
        session: &str,
        from: u64,
        ensemble: Vec<usize>,
    ) -> Option<(u64, Vec<usize>)> {
        let lease = self.store.read_lease(name)?;
        if lease.generation != session || lease.log_state != Some(LogState::Open) {
            return None;
        }
        let entry = self.session_mut(session);
        if entry.epoch == from {
            entry.epoch += 1;
            entry.ensemble = ensemble;
            entry.frames = 0;
            entry.proved = false;
        }
        (entry.epoch > from).then(|| (entry.epoch, entry.ensemble.clone()))
    }

    /// A reopen answered. If the record moved, install a shipper for its
    /// epoch and ensemble, and ship every open database to it.
    ///
    /// A failed answer is not the end of the write: production publishes
    /// the record in memory and waits for any applied lease write to carry
    /// it (`OwnLog::write`), so a lost response still succeeds. Read the
    /// record back: if the write applied, it moved.
    fn reopened(&mut self, node: usize, record: Option<(u64, Vec<usize>)>) {
        let record = record.or_else(|| self.applied_record(node));
        let who = self.who(node);
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        log.reopening = false;
        let Some((epoch, ensemble)) = record.filter(|(epoch, _)| *epoch > log.epoch) else {
            return;
        };
        log.epoch = epoch;
        log.degraded = false;
        log.confirmed.clear();
        log.floor = log.shipped.clone();
        let session = log.session.clone();
        self.note(|| format!("n{node} reopens {session} at epoch {epoch} with {ensemble:?}"));
        self.checker.log_opened(&who, &ensemble);
        for (cell, epoch) in self.open_epochs(node) {
            self.ship(node, &cell, epoch);
        }
    }

    /// This process's record as the bucket has it, while its lease is
    /// still this session's and open.
    fn applied_record(&self, node: usize) -> Option<(u64, Vec<usize>)> {
        let session = &self.machines[node].proc().log.as_ref()?.session;
        let lease = self.store.read_lease(&self.machines[node].name)?;
        if lease.generation != *session || lease.log_state != Some(LogState::Open) {
            return None;
        }
        let entry = self.session(session);
        Some((entry.epoch, entry.ensemble.clone()))
    }

    /// `RecoverNodeLog`, at the instant it reaches the bucket and the
    /// followers: recover the log of the session `owner`'s lease names, if
    /// it is not sealed.
    fn recover(&mut self, by: usize, owner: &str) -> Result<(), Failure> {
        let Some(lease) = self.store.read_lease(owner) else {
            return Ok(());
        };
        if matches!(lease.log_state, None | Some(LogState::Sealed)) {
            return Ok(());
        }
        self.recover_session(by, owner, &lease.generation)
    }

    /// Before a restart takes the lease over, it recovers the session its
    /// predecessor left open (`ensure_predecessors_recovered`): a call like
    /// any other, which can fail, apply late or never be answered. Until it
    /// succeeds the boot retries, serving its follower disk.
    pub(super) fn boot(&mut self, node: usize) {
        let m = &mut self.machines[node];
        m.booting = true;
        m.boot += 1;
        let boot = m.boot;
        let (apply, answer, failed_at) = match self.fate(node, OPERATION_DEADLINE_MS) {
            Fate::Answered {
                request_ms,
                response_ms,
            } => (Some(request_ms), Some(response_ms), None),
            Fate::Failed {
                at, applies_after, ..
            } => (applies_after, None, Some(at)),
        };
        if let Some(apply_ms) = apply {
            let item = Item::BootRecover {
                node,
                boot,
                answer_ms: answer,
            };
            self.after(apply_ms, item);
        }
        if let Some(at) = failed_at {
            let item = Item::Booted {
                node,
                boot,
                recovered: false,
            };
            self.after(at, item);
        }
    }

    pub(super) fn boot_recover(&mut self, node: usize, boot: u64, answer_ms: Option<u64>) {
        let name = self.machines[node].name.clone();
        let recovered = self.recover(node, &name).is_ok();
        if let Some(delay) = answer_ms {
            let item = Item::Booted {
                node,
                boot,
                recovered,
            };
            self.after(delay, item);
        }
    }

    pub(super) fn booted(&mut self, node: usize, boot: u64, recovered: bool) {
        let m = &self.machines[node];
        if m.boot != boot || !m.booting || m.retired {
            return;
        }
        if recovered {
            self.start(node);
        } else {
            self.note(|| format!("boot n{node} waits for its predecessor's log"));
            self.after(1_000, Item::Restart { node });
        }
    }

    /// A crash kills a process still booting. It had not started, so it
    /// held nothing but its disk; the supervisor boots another.
    pub(super) fn kill_boot(&mut self, node: usize, wipe: bool) {
        let m = &mut self.machines[node];
        m.booting = false;
        m.boot += 1;
        if wipe {
            m.disk = Default::default();
        }
        self.note(|| {
            format!(
                "crash booting n{node}{}",
                if wipe { " (disk wiped)" } else { "" }
            )
        });
        if !self.machines[node].retired {
            self.schedule_restart(node);
        }
    }

    /// What the members of `session` that answer now hold of it: of
    /// `cell` alone, when given. A member that answers holding every frame
    /// up to the highest it received is a witness; one that answers without it, or whose machine
    /// is retired, is conclusively gone; one that does not answer is
    /// undecided.
    fn gather(&self, session: &str, cell: Option<&str>) -> Gathered {
        let mut gathered = Gathered {
            answering: Vec::new(),
            copies: BTreeMap::new(),
            witnessed: false,
            undecided: false,
        };
        let entry = self.session(session);
        for &member in &entry.ensemble {
            let m = &self.machines[member];
            if !m.serves_follower_disk() {
                gathered.undecided |= !m.retired;
                continue;
            }
            gathered.answering.push(member);
            // A member still on an earlier log epoch holds nothing of this
            // one; what it holds of the earlier, the reopen tiered.
            let frames = m.disk.log.joined.get(session);
            if frames.is_some_and(|f| f.epoch < entry.epoch) {
                continue;
            }
            gathered.witnessed |= frames.is_some_and(|f| f.held == f.highest);
            for (key @ (s, c, epoch), copy) in &m.disk.log.copies {
                if s != session || cell.is_some_and(|cell| cell != c) {
                    continue;
                }
                // A fragment that starts past what the bucket holds leaves
                // a hole: its rows cannot be restored.
                let from = m.disk.log.floors.get(key).copied().unwrap_or(0);
                if self.store.held(c, *epoch).len() < from {
                    continue;
                }
                let longest = gathered.copies.entry((c.clone(), *epoch)).or_default();
                if copy.len() > longest.len() {
                    *longest = copy.clone();
                }
            }
        }
        gathered
    }

    /// Seal every follower of `session` that can be reached, upload what
    /// they hold, and mark the lease body sealed. The upload is the dead
    /// leader's own writes, so the oracle counts it as that process's.
    ///
    /// Without a witness, a session that may have fleet acks is missing
    /// some: recovery waits for every undecided member, then declares the
    /// bounded loss, as production does. A session that never proved
    /// anything has nothing to lose, and seals what it gathered.
    fn recover_session(&mut self, by: usize, owner: &str, session: &str) -> Result<(), Failure> {
        if self.machines[by].partition.is_some() {
            return Err(Failure::Ambiguous);
        }
        let entry = self.session(session);
        let (dead, proved) = (self.who_at(entry.node, entry.inc), entry.proved);
        let gathered = self.gather(session, None);
        if proved && !gathered.witnessed {
            if gathered.undecided {
                return Err(Failure::Ambiguous);
            }
            let lost = self.checker.declare_loss(&dead, &self.store);
            self.stats.fleet_losses += lost;
        }
        for member in gathered.answering {
            let disk = &mut self.machines[member].disk.log;
            disk.sealed.insert(session.to_string());
        }
        // Every bundle of the session, then what the followers hold.
        for ((cell, epoch), content) in self.store.bundle_rows(session, None) {
            self.land_upload(&dead, &cell, epoch, &content);
        }
        for ((cell, epoch), content) in gathered.copies {
            self.land_upload(&dead, &cell, epoch, &content);
        }
        self.store.seal_log(owner, session);
        self.stats.log_recoveries += 1;
        Ok(())
    }

    /// `fold_cell`, as a restore here reaches the bucket: upload what the
    /// followers of this process's session hold of `cell`, the tail an
    /// epoch that ended without its final upload left. Without a witness,
    /// a session that may have fleet acks fails the restore rather than
    /// open a database that may miss acknowledged writes.
    pub(super) fn fold_tail(&mut self, node: usize, inc: u64, cell: &str) -> Result<(), Failure> {
        let session = shutdown::generation(node, inc);
        if !self.sessions.contains_key(&session) {
            return Ok(());
        }
        if self.machines[node].partition.is_some() {
            return Err(Failure::Ambiguous);
        }
        let gathered = self.gather(&session, Some(cell));
        if self.session(&session).proved && !gathered.witnessed {
            self.note(|| format!("fold of {cell} in {session} finds no complete follower"));
            return Err(Failure::Ambiguous);
        }
        let writer = self.who_at(node, inc);
        for ((cell, epoch), content) in self.store.bundle_rows(&session, Some(cell)) {
            self.land_upload(&writer, &cell, epoch, &content);
        }
        for ((cell, epoch), content) in gathered.copies {
            self.land_upload(&writer, &cell, epoch, &content);
        }
        Ok(())
    }

    /// A process exits gracefully. If the per-cell layout holds everything
    /// its log shipped and its bundles carried, and no tail waits for a
    /// fold, it seals its own log, and its successor has nothing to
    /// recover. A bundle does not count (`graceful_seal_allowed`).
    pub(super) fn seal_own_log(&mut self, node: usize) {
        let p = self.machines[node].proc();
        let Some(log) = p.log.as_ref() else {
            return;
        };
        let covered = |(cell, epoch): &(String, Epoch), len: usize| {
            self.store.percell_len(cell, *epoch) >= len
        };
        let bundles = self.store.bundle_rows(&log.session, None);
        let flushed = p.tails.is_empty()
            && log.shipped.iter().all(|(key, len)| covered(key, *len))
            && bundles.iter().all(|(key, rows)| covered(key, rows.len()));
        if flushed {
            let (name, session) = (self.machines[node].name.clone(), log.session.clone());
            self.store.seal_log(&name, &session);
        }
    }
}
