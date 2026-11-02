//! Reconfiguring a session's ensemble, as `node_log.rs`'s posture watch
//! and `maintain` do.
//!
//! - A member that has not confirmed a frame within the backstop degrades
//!   the shipper: nothing ships, and the gate rides the bundle loop.
//! - `maintain` runs from the posture watch while the shipper is degraded,
//!   about once a second, and from the 30 s maintenance tick in any
//!   posture. It reads the peers' leases, a bucket call that can fail, and
//!   recruits the first two live by this node's clock. A healthy log
//!   reconfigures only to grow wider (denoland/celld#245).
//! - Once the bucket holds everything the log shipped, of open databases
//!   and tails, and no round is outstanding (`may_reconfigure`), the
//!   degraded leader reopens the log at
//!   the next log epoch with them, by a lease write that applies only while
//!   the lease is still its own and open.
//! - The new epoch has no fleet acks yet, and its fragments start where the
//!   old epoch's shipping stopped.

use super::super::replicator::Tail;
use super::super::shell::StoreReq;
use super::super::{Work, World};
use super::{Activation, LogWork, Posture, FOLLOWERS};
use crate::Mutation;
use celld_logic::log_tier::LogState;
use celld_logic::Epoch;

/// The posture watch's poll.
pub(super) const WATCH_MS: u64 = 250;
/// How often the maintenance loop runs `maintain`, healthy or not
/// (`LOG_MAINTENANCE_TICK_MS`).
pub(super) const TICK_MS: u64 = 30_000;
/// The repair cadence while degraded, and its cap.
pub(super) const REPAIR_MS: u64 = 1_000;
const REPAIR_MAX_MS: u64 = 10_000;

impl World {
    /// A frame's backstop ran out. A member that has not confirmed it
    /// failed its append: degrade (`degrade_shared`).
    pub(super) fn backstop(&mut self, node: usize, log_epoch: u64, frame: u64) {
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        // Frames restart at 1 in each log epoch: an earlier epoch's
        // backstop leaves this one's frames alone.
        if log.epoch != log_epoch {
            return;
        }
        // The lanes that never answered give up: a failed append.
        let late: Vec<(usize, u64, String)> = (log.sent.iter())
            .filter(|((f, _), _)| *f == frame)
            .map(|(&(_, member), &started)| (member, started, self.machines[member].name.clone()))
            .collect();
        let mono = self.mono(node);
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        for (member, started, name) in &late {
            log.sent.remove(&(frame, *member));
            log.health.append_completed(name, mono, mono - started);
        }
        if !late.is_empty() {
            self.degrade(node, &format!("frame {frame} unconfirmed"));
        }
    }

    /// Stop shipping: the gate rides the bundle loop until a reopen
    /// (`degrade_shared`).
    pub(super) fn degrade(&mut self, node: usize, why: &str) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.posture != Posture::Shipping {
            return;
        }
        log.posture = Posture::Degraded;
        let (session, epoch) = (log.session.clone(), log.epoch);
        self.note(|| format!("n{node} degrades {session} epoch {epoch}: {why}"));
    }

    /// The posture watch's poll (`node_log_posture_watch`): while the
    /// shipper is degraded, try a repair about once a second, backing off
    /// to the swap cadence after passes that stepped the epoch and still
    /// left it degraded.
    pub(super) fn watch(&mut self, node: usize) {
        self.work(node, WATCH_MS, Work::Log(LogWork::Watch));
        // The watch awaits its own passes: nothing else runs until one ends.
        let waiting = self.machines[node].proc().log.as_ref();
        if waiting.is_some_and(|log| matches!(log.pass, Some((_, true)))) {
            return;
        }
        self.probe_followers(node);
        self.evict_gray_followers(node);
        let t = self.t;
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.posture == Posture::Shipping {
            log.repair_ms = REPAIR_MS;
            return;
        }
        if t.saturating_sub(log.repair_tried) >= log.repair_ms {
            log.repair_tried = t;
            self.maintain(node, true);
        }
    }

    /// `maintain`, from the posture watch (`from_watch`) or the
    /// maintenance tick. A
    /// healthy log at full width has nothing to do. Otherwise read the
    /// peers' leases, a bucket call that can fail; the pass goes on in
    /// `peers_read`.
    pub(super) fn maintain(&mut self, node: usize, from_watch: bool) {
        let p = self.machines[node].proc();
        // A process shutting down repairs nothing (`task_stop`).
        let Some(log) = p.log.as_ref().filter(|_| p.shutdown.is_none()) else {
            return;
        };
        let width = self.session(&log.session).ensemble.len();
        if log.pass.is_some() || (log.posture == Posture::Shipping && width >= FOLLOWERS) {
            return;
        }
        // Self-suspicion parks recruitment: opening epochs into our own
        // partition proves nothing.
        if log.suspect_self {
            return;
        }
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        log.pass = Some((log.epoch, from_watch));
        self.store_call(node, StoreReq::ReadPeers);
    }

    /// The pass's peer read answered, with every lease's node and expiry,
    /// in the bucket's listing order. Recruit the first two peers live by
    /// this node's own clock. A healthy shipper with as many members has
    /// nothing to gain. Otherwise degrade it, and once no frame waits on
    /// its members and every database's shipped rows are in the bucket,
    /// its own and its tails' (`may_reconfigure`), reopen the log at the
    /// next epoch with them.
    pub(super) fn peers_read(&mut self, node: usize, peers: Option<Vec<(usize, u64)>>) {
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        let Some(peers) = peers else {
            return self.pass_ended(node);
        };
        let now = self.wall(node);
        let members: Vec<usize> = (peers.into_iter())
            .filter(|&(peer, expires_ms)| peer != node && expires_ms > now)
            .map(|(peer, _)| peer)
            .take(FOLLOWERS)
            .collect();
        let width = self.session(&log.session).ensemble.len();
        if log.posture == Posture::Shipping {
            if members.len() <= width {
                return self.pass_ended(node);
            }
            self.degrade(node, "a wider ensemble is available");
        }
        let untiered = self.config.mutation == Some(Mutation::ReopenUntiered);
        let barrier = !self.outstanding(node) && (untiered || self.all_shipped_tiered(node));
        // The write needs a running log (`transition.is_running()`).
        let stopping = self.machines[node].proc().shutdown.is_some();
        if !barrier || members.is_empty() || stopping {
            return self.pass_ended(node);
        }
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        let req = StoreReq::Reopen {
            session: log.session.clone(),
            from: log.epoch,
            ensemble: members,
        };
        self.store_call(node, req);
    }

    /// A maintain pass ended. After a pass the watch began, the repair
    /// cadence resets while healthy, and doubles, up to the swap cadence,
    /// after a pass that stepped the epoch but left the shipper degraded.
    /// The tick's passes leave the watch's cadence alone.
    fn pass_ended(&mut self, node: usize) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        let Some((before, true)) = log.pass.take() else {
            return;
        };
        log.repair_ms = if log.posture != Posture::Shipping && log.epoch != before {
            (2 * log.repair_ms).min(REPAIR_MAX_MS)
        } else {
            REPAIR_MS
        };
    }

    /// The maintenance loop's tick.
    pub(super) fn tick(&mut self, node: usize) {
        self.work(node, TICK_MS, Work::Log(LogWork::Tick));
        self.maintain(node, false);
    }

    /// Whether a round is outstanding (`outstanding`): a frame some lane
    /// still waits on, or proofs waiting on the activation write, which
    /// production's round awaits before it completes.
    fn outstanding(&self, node: usize) -> bool {
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return false;
        };
        log.sent.keys().any(|&(frame, _)| frame != 0)
            || matches!(log.activation, Activation::Waiting(_))
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
    pub(in crate::world) fn apply_reopen(
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
            entry.credited = false;
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
    pub(super) fn reopened(&mut self, node: usize, record: Option<(u64, Vec<usize>)>) {
        let record = record.or_else(|| self.applied_record(node));
        let who = self.who(node);
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        log.posture = Posture::Degraded;
        let Some((epoch, ensemble)) = record.filter(|(epoch, _)| *epoch > log.epoch) else {
            return self.pass_ended(node);
        };
        log.epoch = epoch;
        log.health.reset();
        log.sent.clear();
        log.posture = Posture::Shipping;
        log.activation = Activation::Idle;
        log.confirmed.clear();
        log.floor = log.shipped.clone();
        let session = log.session.clone();
        self.note(|| format!("n{node} reopens {session} at epoch {epoch} with {ensemble:?}"));
        self.checker.log_opened(&who, &ensemble);
        self.pass_ended(node);
        for (cell, epoch) in self.open_epochs(node) {
            self.ship(node, &cell, epoch);
        }
    }

    /// Whether this process's log should be reopening now: degraded, with
    /// everything it shipped in the bucket, and a peer that could follow it
    /// running, reachable and not shutting down.
    pub(in crate::world) fn reopen_due(&self, node: usize) -> bool {
        let m = &self.machines[node];
        let Some(log) = m.proc().log.as_ref() else {
            return false;
        };
        let peer = (self.machines.iter().enumerate()).any(|(i, peer)| {
            i != node
                && !peer.retired
                && peer.partition.is_none()
                && (peer.proc.as_ref())
                    .is_some_and(|p| p.paused_until.is_none() && p.shutdown.is_none())
        });
        log.posture != Posture::Shipping
            && m.partition.is_none()
            && m.proc().shutdown.is_none()
            && self.all_shipped_tiered(node)
            && peer
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
}
