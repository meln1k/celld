//! Shipping: the leader's rounds to its ensemble, the followers that fsync
//! them, fleet credit, and the bundle flushes that carry the same rows to
//! the bucket.

use super::*;
use celld_logic::log_tier::LeaderView;

/// One append round's deadline: the eviction backstop plus slack.
const APPEND_TIMEOUT_MS: u64 = 1_600;

/// The ship loop of one log epoch.
#[derive(Debug)]
pub(in crate::world) struct Shipper {
    view: LeaderView,
    activation: Activation,
    /// Any member failure stops fleet acknowledgements for good; the next
    /// healthy ensemble is a new epoch.
    degraded: bool,
    /// Every frame shipped at this epoch, by sequence from 1.
    frames: Vec<Row>,
    /// Each member's confirmed contiguous end.
    confirmed: BTreeMap<String, u64>,
    /// Rounds not yet confirmed by every member, by their last sequence.
    rounds: BTreeMap<u64, BTreeSet<String>>,
    /// When each member's lane last delivers, so a lane stays in order.
    lanes: BTreeMap<String, Time>,
    credited: u64,
}

/// The activation fence: an epoch publishes `active` before its first
/// fleet credit, so a recovery that finds no witness knows it must record
/// a loss rather than seal the bucket as complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Activation {
    Inactive,
    Publishing,
    Active,
}

impl Shipper {
    pub(super) fn epoch(&self) -> Epoch {
        self.view.epoch
    }

    pub(super) fn healthy(&self) -> bool {
        !self.degraded
    }

    pub(super) fn width(&self) -> usize {
        self.view.ensemble.len()
    }

    pub(super) fn rows(&self) -> impl Iterator<Item = &Row> {
        self.frames.iter()
    }
}

/// A follower's answer to a seal or a tail read: the fragment epoch it
/// holds, its contiguous end, and its retained frames.
#[derive(Clone, Debug)]
pub(in crate::world) struct TailAnswer {
    pub(super) held: Epoch,
    pub(super) end: u64,
    pub(super) frames: Vec<Row>,
}

impl World {
    // -----------------------------------------------------------------------
    // The network between nodes

    /// Send `work` from node `from` to the process on node `to`, `delay`
    /// from now. A refused connection fails at once and a hung one never
    /// answers: the sender's deadline covers it.
    pub(super) fn peer_send(
        &mut self,
        from: usize,
        to: usize,
        delay: u64,
        work: LogWork,
    ) -> Result<(), Failure> {
        for node in [from, to] {
            match self.nodes[node].partition {
                Some((_, PartitionMode::Refuse)) => return Err(Failure::Definite),
                Some((_, PartitionMode::Hang)) => return Err(Failure::Ambiguous),
                None => {}
            }
        }
        let Some(target) = self.nodes[to].proc else {
            return Err(Failure::Definite);
        };
        self.work(target, delay, Work::Log(work));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // The leader

    pub(super) fn install_shipper(&mut self, id: ProcId, epoch: Epoch, ensemble: BTreeSet<String>) {
        self.stats.log_ensembles += 1;
        self.note(|| format!("p{id} ships log epoch {epoch} to {ensemble:?}"));
        let proc = self.procs.get_mut(&id).unwrap();
        proc.log.shipper = Some(Shipper {
            view: LeaderView { epoch, ensemble },
            activation: Activation::Inactive,
            degraded: false,
            frames: Vec::new(),
            confirmed: BTreeMap::new(),
            rounds: BTreeMap::new(),
            lanes: BTreeMap::new(),
            credited: 0,
        });
        // The first round carries every row above what the bucket covers,
        // so fleet credit always extends a contiguous run.
        let mut rows = Vec::new();
        for ((cell, cell_epoch), replica) in &mut proc.replicas {
            for (index, write) in replica.content.iter().enumerate().skip(replica.bucket()) {
                let row = Row {
                    cell: cell.clone(),
                    epoch: *cell_epoch,
                    position: index as u64 + 1,
                    write: *write,
                };
                if index >= replica.queued {
                    proc.log.unbundled.push(row.clone());
                }
                rows.push(row);
            }
            replica.queued = replica.queued.max(replica.content.len());
        }
        if !rows.is_empty() {
            self.ship(id, rows);
        }
    }

    /// A commit, as the ship loop picks it up.
    pub(in crate::world) fn log_commit(&mut self, id: ProcId, cell: &str, epoch: Epoch) {
        let proc = self.procs.get_mut(&id).unwrap();
        if proc.log.shipper.is_none() {
            return;
        }
        let replica = proc.replicas.get_mut(&(cell.to_string(), epoch)).unwrap();
        let row = Row {
            cell: cell.to_string(),
            epoch,
            position: replica.content.len() as u64,
            write: *replica.content.last().unwrap(),
        };
        replica.queued = replica.content.len();
        proc.log.unbundled.push(row.clone());
        self.ship(id, vec![row]);
    }

    fn ship(&mut self, id: ProcId, rows: Vec<Row>) {
        let node = self.procs[&id].node;
        let session = self.session_of(id);
        let t = self.t;
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(shipper) = proc.log.shipper.as_mut().filter(|s| !s.degraded) else {
            return;
        };
        let first = shipper.frames.len() as u64 + 1;
        shipper.frames.extend(rows.iter().cloned());
        let last = shipper.frames.len() as u64;
        let frames: Vec<(u64, Row)> = (first..).zip(rows).collect();
        let epoch = shipper.view.epoch;
        shipper.rounds.insert(last, shipper.view.ensemble.clone());
        let mut sends = Vec::new();
        for member in &shipper.view.ensemble {
            let lane = shipper.lanes.get(member).map_or(t, |at| at + 1);
            let at = lane.max(t + self.net_rng.range(1, 5));
            shipper.lanes.insert(member.clone(), at);
            sends.push((member.clone(), at - t));
        }
        self.work(
            id,
            APPEND_TIMEOUT_MS,
            Work::Log(LogWork::RoundDeadline { epoch, last }),
        );
        for (member, delay) in sends {
            let to = self.node_index(&member).expect("members are nodes");
            let work = LogWork::Append {
                leader: id,
                session: session.clone(),
                epoch,
                frames: frames.clone(),
            };
            if let Err(Failure::Definite) = self.peer_send(node, to, delay, work) {
                let refused = LogWork::Appended {
                    epoch,
                    last,
                    member,
                    answer: None,
                };
                self.work(id, delay, Work::Log(refused));
            }
        }
    }

    pub(super) fn append_answered(
        &mut self,
        id: ProcId,
        epoch: Epoch,
        last: u64,
        member: &str,
        answer: Option<(Epoch, u64)>,
    ) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(shipper) = proc.log.shipper.as_mut() else {
            return;
        };
        if shipper.view.epoch != epoch || shipper.degraded {
            return;
        }
        // `append_confirms`: the member's own fragment epoch, through `last`.
        match answer {
            Some((held, end)) if held == epoch && end >= last => {
                let confirmed = shipper.confirmed.entry(member.to_string()).or_default();
                *confirmed = (*confirmed).max(end);
                if let Some(waiting) = shipper.rounds.get_mut(&last) {
                    waiting.remove(member);
                    if waiting.is_empty() {
                        shipper.rounds.remove(&last);
                    }
                }
                self.credit_fleet(id);
            }
            _ => self.degrade(id, &format!("{member} refused an append")),
        }
    }

    pub(super) fn round_deadline(&mut self, id: ProcId, epoch: Epoch, last: u64) {
        let late = self.procs[&id].log.shipper.as_ref().is_some_and(|shipper| {
            shipper.view.epoch == epoch && shipper.rounds.contains_key(&last)
        });
        if late {
            self.degrade(id, "an append round timed out");
        }
    }

    /// Stop fleet acknowledgements: every later proof waits for the bucket
    /// until maintenance installs a healthy ensemble.
    pub(super) fn degrade(&mut self, id: ProcId, why: &str) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(shipper) = proc.log.shipper.as_mut().filter(|s| !s.degraded) else {
            return;
        };
        shipper.degraded = true;
        shipper.rounds.clear();
        let epoch = shipper.view.epoch;
        let keys: Vec<(String, Epoch)> = proc.replicas.keys().cloned().collect();
        self.stats.log_degrades += 1;
        self.note(|| format!("p{id} log epoch {epoch} degrades: {why}"));
        for (cell, cell_epoch) in keys {
            self.kick_upload(id, &cell, cell_epoch);
        }
        self.schedule_maintain(id, MAINTAIN_REPAIR_MS);
    }

    /// Credit every frame the whole ensemble confirmed, once the epoch is
    /// active: the fleet proofs.
    fn credit_fleet(&mut self, id: ProcId) {
        let one_member = self.config.mutation == Mutation::AckOneFollower;
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(shipper) = proc.log.shipper.as_mut().filter(|s| !s.degraded) else {
            return;
        };
        let allowed = |next: u64| {
            if one_member {
                shipper.confirmed.values().any(|end| *end >= next)
            } else {
                log_tier::ack_fleet_allowed(&shipper.view, &shipper.confirmed, next)
            }
        };
        let through = (shipper.credited + 1..=shipper.frames.len() as u64)
            .take_while(|next| allowed(*next))
            .last();
        let Some(through) = through else {
            return;
        };
        match shipper.activation {
            Activation::Active => {}
            Activation::Publishing => return,
            Activation::Inactive => {
                shipper.activation = Activation::Publishing;
                let epoch = shipper.view.epoch;
                let own = proc
                    .log
                    .own
                    .clone()
                    .expect("a shipper's record is published");
                let wire = LogWire {
                    active: true,
                    ..own
                };
                return self.publish_own(id, wire, Then::Activate { epoch });
            }
        }
        let rows = shipper.frames[shipper.credited as usize..through as usize].to_vec();
        shipper.credited = through;
        let epoch = shipper.view.epoch;
        let session = self.session_of(id);
        let mut touched = BTreeSet::new();
        for row in &rows {
            let (checker, view) = self.checks();
            checker.fleet.on_credit(view, &session, epoch, row);
            let key = (row.cell.clone(), row.epoch);
            let replica = self.procs.get_mut(&id).unwrap().replicas.get_mut(&key);
            if replica.is_some_and(|replica| replica.credit_fleet(row.position as usize)) {
                touched.insert(key);
            }
        }
        for (cell, cell_epoch) in touched {
            self.settle_waiters(id, &cell, cell_epoch);
        }
    }

    /// The activation publish landed, or never did.
    pub(super) fn activated(&mut self, id: ProcId, epoch: Epoch, ok: bool) {
        let log = self.log(id);
        let Some(shipper) = log.shipper.as_mut().filter(|s| s.view.epoch == epoch) else {
            return;
        };
        if ok {
            shipper.activation = Activation::Active;
            self.credit_fleet(id);
        } else {
            shipper.activation = Activation::Inactive;
            self.degrade(id, "its activation never reached the lease");
        }
    }

    // -----------------------------------------------------------------------
    // Followers

    /// `FollowerStore::append_batch`, then the answer back to the leader.
    pub(super) fn append_arrived(
        &mut self,
        id: ProcId,
        leader: ProcId,
        session: &Session,
        epoch: Epoch,
        frames: Vec<(u64, Row)>,
    ) {
        let node = self.procs[&id].node;
        // Startup serves seals and tails only.
        let answer = if self.procs[&id].log.booting {
            None
        } else {
            self.follower_accept(node, session, epoch, &frames)
        };
        let Some(leader_node) = self.procs.get(&leader).map(|proc| proc.node) else {
            return;
        };
        let reply = LogWork::Appended {
            epoch,
            last: frames.last().map_or(0, |(seq, _)| *seq),
            member: self.node_name(id),
            answer,
        };
        let delay = self.net_rng.range(1, 5);
        let _ = self.peer_send(node, leader_node, delay, reply);
    }

    /// The follower's fsynced (fragment epoch, end) after `frames`, or
    /// `None` for a refusal.
    fn follower_accept(
        &mut self,
        node: usize,
        session: &Session,
        epoch: Epoch,
        frames: &[(u64, Row)],
    ) -> Option<(Epoch, u64)> {
        let after_seal = self.config.mutation == Mutation::AppendAfterSeal;
        let held = self.nodes[node]
            .disk
            .peerlog
            .get(session)
            .map(|f| f.log.clone());
        if held.as_ref().map(|log| log.fragment_epoch) != Some(epoch) {
            // `adopt`: a new fragment epoch only from its first frame, above
            // the seal mark, against the leader's record open at that epoch.
            let open = self
                .store
                .read_lease_log(&session.node)
                .is_some_and(|(lease, log)| {
                    session.owns(&lease)
                        && log.is_some_and(|log| {
                            log.record.epoch == epoch && log.record.state == LogState::Open
                        })
                });
            let sealed_to = held.as_ref().map_or(0, |log| log.sealed_to);
            let first = frames.first().map(|(seq, _)| *seq);
            if !open || (sealed_to >= epoch && !after_seal) || first != Some(1) {
                return None;
            }
            let fragment = Fragment {
                log: FollowerLog {
                    sealed_to,
                    ..FollowerLog::join(epoch, 0)
                },
                frames: BTreeMap::new(),
            };
            self.nodes[node]
                .disk
                .peerlog
                .insert(session.clone(), fragment);
        }
        let member = self.nodes[node].name.clone();
        let fragment = self.nodes[node].disk.peerlog.get_mut(session).unwrap();
        for (seq, row) in frames {
            if *seq <= fragment.log.end {
                continue;
            }
            let accepted = if after_seal {
                epoch == fragment.log.fragment_epoch && *seq == fragment.log.end + 1
            } else {
                fragment.log.accept_append(epoch, *seq)
            };
            if !accepted {
                return None;
            }
            fragment.log.end = *seq;
            fragment.frames.insert(*seq, row.clone());
            self.checker.fleet.on_fsync(&member, session, epoch, row);
        }
        Some((fragment.log.fragment_epoch, fragment.log.end))
    }

    /// A seal (`seal: true`) or a tail read of `session`'s fragment. A seal
    /// persists its mark before it answers, and an absent fragment answers
    /// epoch 0: conclusive, but no witness.
    pub(super) fn follower_answer(
        &mut self,
        node: usize,
        session: &Session,
        epoch: Epoch,
        seal: bool,
    ) -> TailAnswer {
        let peerlog = &mut self.nodes[node].disk.peerlog;
        if seal {
            let fragment = peerlog.entry(session.clone()).or_insert_with(|| Fragment {
                log: FollowerLog::join(0, 0),
                frames: BTreeMap::new(),
            });
            fragment.log.seal(epoch);
        }
        peerlog.get(session).map_or(
            TailAnswer {
                held: 0,
                end: 0,
                frames: Vec::new(),
            },
            |fragment| TailAnswer {
                held: fragment.log.fragment_epoch,
                end: fragment.log.end,
                frames: fragment.frames.values().cloned().collect(),
            },
        )
    }

    // -----------------------------------------------------------------------
    // Per-cell uploads and bundles

    /// Whether the per-cell sync loop uploads this replica now. A healthy
    /// shipper leaves its rows to the fleet and the bundles until a drain
    /// point asks for the cell's own prefix.
    pub(in crate::world) fn per_cell_sync(&self, id: ProcId, replica: &Replica) -> bool {
        let log = &self.procs[&id].log;
        log.closing
            || log.shipper.as_ref().is_none_or(|shipper| shipper.degraded)
            || replica.stop.as_ref().is_some_and(Stop::publishing)
            || replica
                .waiters
                .iter()
                .any(|waiter| matches!(waiter, Waiter::Ensure { .. }))
    }

    /// `note_undrained_tail`: an epoch of `cell` ended with rows the node
    /// log took past what its prefix holds.
    pub(in crate::world) fn note_undrained_tail(
        &mut self,
        id: ProcId,
        cell: &str,
        replica: &Replica,
    ) {
        let taken = replica.queued.max(replica.bundled).max(replica.fleet);
        if taken > replica.uploaded {
            self.log(id).dirty.insert(cell.to_string());
        }
    }

    /// Whether `cell`'s next restore must fold an undrained tail first.
    pub(in crate::world) fn undrained(&self, id: ProcId, cell: &str) -> bool {
        self.procs[&id].log.dirty.contains(cell)
    }

    pub(super) fn flush_tick(&mut self, id: ProcId) {
        self.work(id, FLUSH_MS, Work::Log(LogWork::FlushTick));
        let session = self.session_of(id);
        let log = self.log(id);
        if log.closing || log.flush.is_some() || log.unbundled.is_empty() {
            return;
        }
        let Some(epoch) = log.shipper.as_ref().map(Shipper::epoch) else {
            return;
        };
        let rows = std::mem::take(&mut log.unbundled);
        log.flush = Some((epoch, rows.clone()));
        self.log_store_call(id, StoreReq::PutBundle { session, rows }, LogTask::Flush);
    }

    pub(super) fn flush_landed(&mut self, id: ProcId, ok: bool) {
        if !ok {
            self.flush_settled(id, false);
        } else {
            // `bundle_credit_allowed`: the PUT proves nothing until the
            // record, read after it, is still open at the shipper's epoch.
            let node = self.node_name(id);
            self.log_store_call(id, StoreReq::ReadLeaseLog { node }, LogTask::FlushCheck);
        }
    }

    pub(super) fn flush_checked(&mut self, id: ProcId, record: Result<Option<LogRecord>, Failure>) {
        let Some((epoch, _)) = self.procs[&id].log.flush else {
            return;
        };
        let allowed =
            record.is_ok_and(|record| log_tier::bundle_credit_allowed(record.as_ref(), epoch));
        self.flush_settled(id, allowed);
    }

    fn flush_settled(&mut self, id: ProcId, credit: bool) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some((_, rows)) = proc.log.flush.take() else {
            return;
        };
        if !credit {
            // The rows go in the next flush again, ahead of later ones.
            proc.log.unbundled.splice(0..0, rows);
            return;
        }
        self.stats.bundle_credits += 1;
        let mut touched = BTreeSet::new();
        for row in rows {
            let key = (row.cell.clone(), row.epoch);
            let replica = proc.replicas.get_mut(&key);
            if replica.is_some_and(|replica| replica.credit_bundle(row.position as usize)) {
                touched.insert(key);
            }
        }
        for (cell, epoch) in touched {
            self.settle_waiters(id, &cell, epoch);
        }
    }
}
