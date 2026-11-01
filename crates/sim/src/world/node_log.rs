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
//!   the bundle loop's tick.
//! - When a node takes over a cell from a dead owner whose lease says its
//!   log is not sealed, the core asks for `RecoverNodeLog` first. Recovery
//!   seals every follower it can reach, so the dead leader can never again
//!   prove a write through them, uploads what they hold into the bucket,
//!   and marks the lease body `Sealed`. A restart does the same for its own
//!   predecessor before it takes the lease over.
//! - A cell epoch that ends without its final upload keeps what its
//!   followers confirmed as a tail (`Process::tails`), which the bundle
//!   loop flushes. The cell's next restore here first folds in what the
//!   followers hold of it (`fold_cell`).
//! - A follower is a complete witness when it holds every frame of the
//!   session up to the highest it received. Without one, a session that
//!   may have fleet acks fails a fold, and makes recovery wait for every
//!   follower that might still come back; once all are conclusively gone,
//!   recovery declares a bounded loss, as production does. A booting
//!   process answers for its disk while it waits for its own predecessor
//!   (`recover_with_follower_listener`).
//!
//! Not modelled: reconfiguring the ensemble when a follower dies (the
//! session's acks fall back to the bucket instead), and the bundles
//! themselves (a flush uploads into the cell's prefix).

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::log_tier::LogState;
use celld_logic::{Epoch, Event, Failure, OpId};

use super::shell::{Fate, StoreReq};
use super::{shutdown, Item, Work, World, OPERATION_DEADLINE_MS};
use crate::{Mutation, WriteId};

/// How often the bundle loop flushes to the bucket (`CELLD_LOG_FLUSH_MS`).
const FLUSH_MS: u64 = 2_000;
/// Followers per session.
const FOLLOWERS: usize = 2;
/// One-way latency between two nodes.
const PEER_MS: (u64, u64) = (1, 5);

/// The log a process leads.
pub(super) struct Log {
    session: String,
    /// A lease write carrying `Open` landed: fleet proofs may begin.
    open: bool,
    /// The longest database of each cell epoch each follower confirmed.
    confirmed: BTreeMap<(usize, String, Epoch), usize>,
    /// The longest database of each cell epoch shipped so far.
    shipped: BTreeMap<(String, Epoch), usize>,
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
}

/// The frames of one session a follower holds.
#[derive(Clone, Copy, Default)]
struct Frames {
    held: u64,
    highest: u64,
}

/// One session as its log record names it: its leader, its followers,
/// how many frames it has shipped, and whether the fleet may have proved a
/// write for it (`may_have_fleet_acks`, set as the first credit is).
pub(super) struct Session {
    node: usize,
    inc: u64,
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

pub(super) enum LogWork {
    /// A leader asks this follower to join its session.
    Join { session: String },
    /// A leader ships a cell epoch's database to this follower.
    Append {
        session: String,
        leader: (usize, u64),
        /// The frame's place in the session, from 1.
        frame: u64,
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    /// A follower confirmed holding `len` writes of a cell epoch.
    Confirmed {
        follower: usize,
        cell: String,
        epoch: Epoch,
        len: usize,
    },
    /// A lease write carrying `Open` applied: the log is open, and the core
    /// hears of the write.
    LeaseOpened(Event),
    /// The bundle loop's tick.
    Flush,
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
                frame,
                cell,
                epoch,
                content,
            } => self.append(node, session, leader, frame, cell, epoch, content),
            LogWork::Confirmed {
                follower,
                cell,
                epoch,
                len,
            } => self.confirmed(node, follower, cell, epoch, len),
            LogWork::LeaseOpened(event) => {
                self.log_opened(node);
                self.drive(node, event);
            }
            LogWork::Flush => {
                self.work(node, FLUSH_MS, Work::Log(LogWork::Flush));
                let open: Vec<_> = self.open_epochs(node);
                for (cell, epoch) in open {
                    self.upload(node, &cell, epoch);
                }
                let tails = self.machines[node].proc().tails.clone();
                for ((cell, epoch), content) in tails {
                    let req = StoreReq::Upload {
                        cell,
                        epoch,
                        content,
                    };
                    self.store_call(node, req);
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
            ensemble: ensemble.clone(),
            frames: 0,
            proved: false,
        };
        self.sessions.insert(session.clone(), entry);
        let who = self.who(node);
        self.checker.log_opened(&who, &ensemble);
        self.machines[node].proc_mut().log = Some(Log {
            session,
            open: false,
            confirmed: BTreeMap::new(),
            shipped: BTreeMap::new(),
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
        if !log.open {
            return;
        }
        let len = log.shipped.entry((cell.to_string(), epoch)).or_default();
        *len = (*len).max(content.len());
        let session = log.session.clone();
        let entry = self.session_mut(&session);
        entry.frames += 1;
        let (frame, ensemble) = (entry.frames, entry.ensemble.clone());
        for follower in ensemble {
            let work = Work::Log(LogWork::Append {
                session: session.clone(),
                leader,
                frame,
                cell: cell.to_string(),
                epoch,
                content: content.clone(),
            });
            self.peer_send(node, follower, None, work);
        }
    }

    /// A follower receives a cell epoch's database. It keeps it on its disk
    /// and confirms, unless recovery sealed the session here. A follower
    /// that does not know the session joins it now.
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        node: usize,
        session: String,
        leader: (usize, u64),
        frame: u64,
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    ) {
        let ignore_seal = self.config.mutation == Some(Mutation::FollowerIgnoresSeal);
        let disk = &mut self.machines[node].disk.log;
        if disk.sealed.contains(&session) && !ignore_seal {
            return;
        }
        let frames = disk.joined.entry(session.clone()).or_default();
        frames.held += 1;
        frames.highest = frames.highest.max(frame);
        let copy = disk
            .copies
            .entry((session, cell.clone(), epoch))
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
        self.checker
            .follower_fsynced(&leader_who, node, &cell, epoch, &held);
        let work = Work::Log(LogWork::Confirmed {
            follower: node,
            cell,
            epoch,
            len,
        });
        self.peer_send(node, leader.0, Some(leader.1), work);
    }

    /// A follower's confirmation reaches its leader. Once every follower
    /// holds a position, the fleet has proved it.
    fn confirmed(&mut self, node: usize, follower: usize, cell: String, epoch: Epoch, len: usize) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
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
        for &member in &self.session(session).ensemble {
            let m = &self.machines[member];
            if !m.serves_follower_disk() {
                gathered.undecided |= !m.retired;
                continue;
            }
            gathered.answering.push(member);
            gathered.witnessed |= (m.disk.log.joined.get(session))
                .is_some_and(|frames| frames.held == frames.highest);
            for ((s, c, epoch), copy) in &m.disk.log.copies {
                if s != session || cell.is_some_and(|cell| cell != c) {
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
        for ((cell, epoch), content) in gathered.copies {
            self.land_upload(&writer, &cell, epoch, &content);
        }
        Ok(())
    }

    /// A process exits gracefully. If the bucket holds everything its log
    /// shipped, it seals its own log, and its successor has nothing to
    /// recover.
    pub(super) fn seal_own_log(&mut self, node: usize) {
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        let flushed = log
            .shipped
            .iter()
            .all(|((cell, epoch), len)| self.store.durable_len(cell, *epoch) >= *len);
        if flushed {
            let (name, session) = (self.machines[node].name.clone(), log.session.clone());
            self.store.seal_log(&name, &session);
        }
    }
}
