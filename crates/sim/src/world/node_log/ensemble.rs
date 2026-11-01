//! Reconfiguring a session's ensemble, as `node_log.rs`'s posture watch
//! and `maintain` do.
//!
//! - A member that has not confirmed a frame within the backstop degrades
//!   the shipper: nothing ships, and the gate rides the bundle loop.
//! - Once the bucket holds everything the log shipped, of open databases
//!   and tails (`may_reconfigure`), the degraded leader reopens the log at
//!   the next log epoch with up to two live peers, by a lease write that
//!   applies only while the lease is still its own and open.
//! - The new epoch has no fleet acks yet, and its fragments start where the
//!   old epoch's shipping stopped.

use super::super::replicator::Tail;
use super::super::shell::StoreReq;
use super::super::{Work, World};
use super::{LogWork, Posture, FOLLOWERS};
use crate::Mutation;
use celld_logic::log_tier::LogState;
use celld_logic::Epoch;

/// How often a degraded shipper tries to re-recruit its ensemble
/// (`maintain`, from the posture watch).
const MAINTAIN_MS: u64 = 1_000;

impl World {
    /// A frame's backstop ran out. A member that has not confirmed it
    /// failed its append: degrade (`degrade_shared`).
    pub(super) fn backstop(&mut self, node: usize, log_epoch: u64, frame: u64) {
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
        self.work(node, MAINTAIN_MS, Work::Log(LogWork::Maintain));
    }

    /// The degraded shipper's tick (`maintain`): once no frame is still
    /// waiting on its members and every database's shipped rows are in the
    /// bucket, its own and its tails' (`may_reconfigure`), reopen the log
    /// at the next epoch with up to two live peers. Until then, and while a
    /// reopen is in flight, try again.
    pub(super) fn maintain(&mut self, node: usize) {
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        if log.posture == Posture::Shipping {
            return;
        }
        let waiting = log.posture == Posture::Reopening || !log.pending.is_empty();
        self.work(node, MAINTAIN_MS, Work::Log(LogWork::Maintain));
        let untiered = self.config.mutation == Some(Mutation::ReopenUntiered);
        if waiting || !(untiered || self.all_shipped_tiered(node)) {
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
        log.posture = Posture::Reopening;
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
            return;
        };
        log.epoch = epoch;
        log.posture = Posture::Shipping;
        log.confirmed.clear();
        log.floor = log.shipped.clone();
        let session = log.session.clone();
        self.note(|| format!("n{node} reopens {session} at epoch {epoch} with {ensemble:?}"));
        self.checker.log_opened(&who, &ensemble);
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
