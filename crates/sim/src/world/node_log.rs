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
//!   followers confirmed as a tail (`Process::tails`), flushed with the
//!   rest and folded into the bucket by the cell's next restore here.
//! - A follower that might still come back makes recovery fail, to be
//!   retried. A booting process answers for its disk while it waits for its
//!   own predecessor (`recover_with_follower_listener`). When every
//!   follower is conclusively gone, recovery declares a bounded loss, as
//!   production does, and seals what the bucket has.
//!
//! Not modelled: reconfiguring the ensemble when a follower dies (the
//! session's acks fall back to the bucket instead), and the bundles
//! themselves (a flush uploads into the cell's prefix).

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::log_tier::LogState;
use celld_logic::{Epoch, Event, Failure, LeaseCasOutcome, OpId};

use super::shell::StoreReq;
use super::{shutdown, Item, Work, World};
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
    ensemble: Vec<usize>,
    /// A lease write carrying `Open` landed: fleet proofs may begin.
    open: bool,
    /// The longest database of each cell epoch each follower confirmed.
    confirmed: BTreeMap<(usize, String, Epoch), usize>,
    /// The longest database of each cell epoch shipped so far.
    shipped: BTreeMap<(String, Epoch), usize>,
    /// The fleet has proved a write for this session
    /// (`may_have_fleet_acks`).
    pub(super) proved: bool,
}

/// What a machine's disk holds as a follower. A wipe loses all of it.
#[derive(Default)]
pub(super) struct FollowerDisk {
    /// Sessions this disk joined, and whether it holds everything each has
    /// proved: it joined before the session proved anything, and has not
    /// been wiped since. A wipe forgets the session; an append then
    /// rejoins it, incomplete if the leader had already proved writes.
    joined: BTreeMap<String, bool>,
    /// Sessions recovery sealed here: it refuses their appends forever.
    sealed: BTreeSet<String>,
    copies: BTreeMap<(String, String, Epoch), Vec<WriteId>>,
}

/// One session's followers, as its log record names them.
pub(super) struct Session {
    node: usize,
    inc: u64,
    ensemble: Vec<usize>,
}

pub(super) enum LogWork {
    /// A leader asks this follower to join its session.
    Join { session: String },
    /// A leader ships a cell epoch's database to this follower.
    Append {
        session: String,
        leader: (usize, u64),
        /// The leader had already proved writes through the fleet.
        proved: bool,
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
    /// A lease write came back. If it carried `Open` and applied, the log
    /// is open.
    LeaseWritten(Event),
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
                if self.machines[node].partition.is_none() {
                    let joined = &mut self.machines[node].disk.log.joined;
                    joined.entry(session).or_insert(true);
                }
            }
            LogWork::Append {
                session,
                leader,
                proved,
                cell,
                epoch,
                content,
            } => self.append(node, session, leader, proved, cell, epoch, content),
            LogWork::Confirmed {
                follower,
                cell,
                epoch,
                len,
            } => self.confirmed(node, follower, cell, epoch, len),
            LogWork::LeaseWritten(event) => {
                if let Event::NodeLeaseCasCompleted {
                    result: Ok(LeaseCasOutcome::Applied { .. }),
                    stamped_log_state: Some(LogState::Open),
                    ..
                } = &event
                {
                    self.log_opened(node);
                }
                self.drive(node, event);
            }
            LogWork::Flush => {
                self.work(node, FLUSH_MS, Work::Log(LogWork::Flush));
                let open: Vec<_> = self.machines[node]
                    .proc()
                    .replicas
                    .keys()
                    .cloned()
                    .collect();
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
            self.peer_send(node, follower, work);
        }
        let entry = Session {
            node,
            inc,
            ensemble: ensemble.clone(),
        };
        self.sessions.insert(session.clone(), entry);
        self.machines[node].proc_mut().log = Some(Log {
            session,
            ensemble,
            open: false,
            confirmed: BTreeMap::new(),
            shipped: BTreeMap::new(),
            proved: false,
        });
        self.work(node, FLUSH_MS, Work::Log(LogWork::Flush));
    }

    /// The log state this process's next lease write must carry.
    pub(super) fn log_slot(&self, node: usize) -> Option<LogState> {
        self.machines[node]
            .proc()
            .log
            .as_ref()
            .map(|_| LogState::Open)
    }

    /// Whether this process flushes to the bucket on the bundle loop's tick
    /// rather than after every commit.
    pub(super) fn paced(&self, node: usize) -> bool {
        self.machines[node].proc().log.is_some()
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
        let open: Vec<_> = self.machines[node]
            .proc()
            .replicas
            .keys()
            .cloned()
            .collect();
        for (cell, epoch) in open {
            self.ship(node, &cell, epoch);
        }
    }

    /// Send work to the process on another machine. A partition on either
    /// side loses it.
    fn peer_send(&mut self, from: usize, to: usize, work: Work) {
        if self.machines[from].partition.is_some() {
            return;
        }
        let delay = self.net_rng.range(PEER_MS.0, PEER_MS.1);
        let inc = self.machines[to].inc;
        self.after(
            delay,
            Item::Proc {
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
        let (session, ensemble, proved) = (log.session.clone(), log.ensemble.clone(), log.proved);
        for follower in ensemble {
            let work = Work::Log(LogWork::Append {
                session: session.clone(),
                leader,
                proved,
                cell: cell.to_string(),
                epoch,
                content: content.clone(),
            });
            self.peer_send(node, follower, work);
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
        proved: bool,
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    ) {
        let ignore_seal = self.config.mutation == Some(Mutation::FollowerIgnoresSeal);
        let disk = &mut self.machines[node].disk.log;
        if (disk.sealed.contains(&session) && !ignore_seal)
            || self.machines[node].partition.is_some()
        {
            return;
        }
        let disk = &mut self.machines[node].disk.log;
        disk.joined.entry(session.clone()).or_insert(!proved);
        let copy = disk
            .copies
            .entry((session, cell.clone(), epoch))
            .or_default();
        if content.len() > copy.len() {
            *copy = content;
        }
        let len = copy.len();
        let leader_who = self.who_at(leader.0, leader.1);
        self.checker
            .follower_fsynced(&leader_who, node, &cell, epoch, len);
        let work = Work::Log(LogWork::Confirmed {
            follower: node,
            cell,
            epoch,
            len,
        });
        let (to, inc) = leader;
        if self.machines[node].partition.is_none() {
            let delay = self.net_rng.range(PEER_MS.0, PEER_MS.1);
            self.after(
                delay,
                Item::Proc {
                    node: to,
                    inc,
                    work,
                },
            );
        }
    }

    /// A follower's confirmation reaches its leader. Once every follower
    /// holds a position, the fleet has proved it.
    fn confirmed(&mut self, node: usize, follower: usize, cell: String, epoch: Epoch, len: usize) {
        if self.machines[node].partition.is_some() {
            return;
        }
        let p = self.machines[node].proc_mut();
        let Some(log) = p.log.as_mut() else {
            return;
        };
        let held = log
            .confirmed
            .entry((follower, cell.clone(), epoch))
            .or_default();
        *held = (*held).max(len);
        let fleet = log
            .ensemble
            .iter()
            .map(|f| {
                let key = (*f, cell.clone(), epoch);
                log.confirmed.get(&key).copied().unwrap_or(0)
            })
            .min()
            .unwrap_or(0);
        self.fleet_reached(node, &cell, epoch, fleet);
    }

    /// The followers of this process's log.
    pub(super) fn ensemble(&self, node: usize) -> Vec<usize> {
        let log = self.machines[node].proc().log.as_ref();
        log.map_or_else(Vec::new, |log| log.ensemble.clone())
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

    /// Before a restart takes the lease over, recover the session its
    /// predecessor left open (`ensure_predecessors_recovered`).
    pub(super) fn recover_predecessor(&mut self, node: usize) -> Result<(), Failure> {
        let name = self.machines[node].name.clone();
        self.recover(node, &name)
    }

    /// Seal every follower of `session` that can be reached, upload what
    /// they hold, and mark the lease body sealed. The upload is the dead
    /// leader's own writes, so the oracle counts it as that process's.
    fn recover_session(&mut self, by: usize, owner: &str, session: &str) -> Result<(), Failure> {
        if self.machines[by].partition.is_some() {
            return Err(Failure::Ambiguous);
        }
        let Some(Session {
            node,
            inc,
            ensemble,
        }) = self.sessions.get(session)
        else {
            unreachable!("an open log names a session that opened");
        };
        let (dead, ensemble) = (self.who_at(*node, *inc), ensemble.clone());
        // A member that answers holding the whole session is a witness; one
        // that answers without it, or whose machine is retired, is
        // conclusively gone; one that does not answer is undecided. Recovery
        // gathers from every member that answers.
        let (mut answering, mut witnessed, mut undecided) = (Vec::new(), false, false);
        for member in ensemble {
            let m = &self.machines[member];
            let answers = m.partition.is_none()
                && ((m.booting && !m.retired)
                    || m.proc.as_ref().is_some_and(|p| p.paused_until.is_none()));
            if answers {
                answering.push(member);
                witnessed |= m.disk.log.joined.get(session) == Some(&true);
            } else if !m.retired {
                undecided = true;
            }
        }
        if !witnessed && undecided {
            return Err(Failure::Ambiguous);
        }
        if !witnessed {
            // Every follower is gone with its disk: declare the bounded loss.
            let lost = self.checker.declare_loss(self.t, &dead, &self.store);
            self.stats.fleet_losses += lost;
        }
        let mut gathered: BTreeMap<(String, Epoch), Vec<WriteId>> = BTreeMap::new();
        for member in answering {
            let disk = &mut self.machines[member].disk.log;
            disk.sealed.insert(session.to_string());
            for ((s, cell, epoch), copy) in &disk.copies {
                if s != session {
                    continue;
                }
                let longest = gathered.entry((cell.clone(), *epoch)).or_default();
                if copy.len() > longest.len() {
                    *longest = copy.clone();
                }
            }
        }
        for ((cell, epoch), content) in gathered {
            self.checker.upload_applied(self.t, &dead, &cell, epoch);
            self.store.put(&cell, epoch, &content);
            let held = self.store.prefix(&cell, epoch).len();
            self.checker.prefix_grew(self.t, &cell, epoch, held);
        }
        self.store.seal_log(owner, session);
        self.stats.log_recoveries += 1;
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
