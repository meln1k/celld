//! The activation fence, as `LiveLogTransitions::activate` and
//! `OwnLog::write` run it: the first fleet proof of each log epoch waits
//! until a lease write has carried that epoch's record with
//! `may_have_fleet_acks` set, so that a recovery reading the record knows
//! the fleet may hold acknowledged writes.

use super::super::{Work, World};
use super::LogWork;
use crate::Mutation;
use celld_logic::{Epoch, Event};

/// How long an activation waits for a lease write to carry it before the
/// session counts as fenced (`OwnLog::write`).
const ACTIVATION_MS: u64 = 10_000;

/// The activation fence (`LiveLogTransitions::activate`): before the first
/// fleet credit of a log epoch, a lease write must carry its record with
/// `may_have_fleet_acks` set, so that a recovery that reads the record
/// knows the fleet may hold acknowledged writes.
pub(super) enum Activation {
    /// No fleet proof yet at this log epoch.
    Idle,
    /// The record is published in memory and the core nudged to renew; the
    /// proofs made meanwhile wait for an applied lease write to carry it:
    /// each a cell epoch and how many of its writes the fleet holds.
    Waiting(Vec<(String, Epoch, usize)>),
    /// The wait ran out: the record in memory keeps the flag, so every
    /// later lease write carries it, but no proof credited. The next proof
    /// writes it again and waits (`activate` finds the record unchanged).
    Stamped,
    /// A lease write carrying it applied, and its answer came back.
    Done,
}

impl World {
    /// The fleet holds `fleet` writes of a cell epoch. Credit them if this
    /// log epoch's activation applied; otherwise hold them for it, and begin
    /// it if this is the epoch's first proof.
    pub(super) fn fleet_proof(&mut self, node: usize, cell: &str, epoch: Epoch, fleet: usize) {
        let early = self.config.mutation == Some(Mutation::CreditBeforeActivation);
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        let log_epoch = log.epoch;
        match &mut log.activation {
            Activation::Done => return self.fleet_credit(node, cell, epoch, fleet),
            Activation::Waiting(held) => held.push((cell.to_string(), epoch, fleet)),
            Activation::Idle | Activation::Stamped => {
                // Publish the record with the flag and nudge the core into
                // renewing now; the next applied lease write carries it.
                log.activation = Activation::Waiting(vec![(cell.to_string(), epoch, fleet)]);
                let timeout = Work::Log(LogWork::ActivationTimeout { log_epoch });
                self.work(node, ACTIVATION_MS, timeout);
                let nudge = Event::NudgeNodeLease {
                    now_ms: 0,
                    now_mono_ms: 0,
                };
                self.deliver(node, 1, nudge);
            }
        }
        if early {
            self.fleet_credit(node, cell, epoch, fleet);
        }
    }

    /// An activation at `log_epoch` ran out of time.
    pub(super) fn activation_timeout(&mut self, node: usize, log_epoch: u64) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.epoch == log_epoch && matches!(log.activation, Activation::Waiting(_)) {
            // The proofs waiting on it never credit; the next one
            // tries again (`activated` stays false).
            log.activation = Activation::Stamped;
            self.degrade(node, "activation transition not applied");
        }
    }

    /// The log epoch a lease write's body marks as maybe having fleet acks:
    /// the current one, once its activation began.
    pub(in crate::world) fn activation_stamp(&self, node: usize) -> Option<u64> {
        let log = self.machines[node].proc().log.as_ref()?;
        (!matches!(log.activation, Activation::Idle)).then_some(log.epoch)
    }

    /// A lease write carrying `log_epoch`'s record with
    /// `may_have_fleet_acks` applies: a recovery reads it from now on, if
    /// the record is still at that epoch.
    pub(in crate::world) fn apply_activation(&mut self, session: &str, log_epoch: u64) {
        if let Some(entry) = self.sessions.get_mut(session) {
            if entry.epoch == log_epoch {
                entry.proved = true;
            }
        }
    }

    /// The answer to a lease write that carried the activation came back:
    /// credit every proof that waited on it.
    pub(super) fn activated(&mut self, node: usize, log_epoch: u64) {
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return;
        };
        if log.epoch != log_epoch {
            return;
        }
        let held = match std::mem::replace(&mut log.activation, Activation::Done) {
            Activation::Waiting(held) => held,
            // Nothing waited on this write: it changes nothing.
            other => {
                log.activation = other;
                return;
            }
        };
        for (cell, epoch, fleet) in held {
            self.fleet_credit(node, &cell, epoch, fleet);
        }
    }

    /// The fleet holds `fleet` writes of a cell epoch, and the record says
    /// it may have fleet acks: credit them to the gate.
    fn fleet_credit(&mut self, node: usize, cell: &str, epoch: Epoch, fleet: usize) {
        let who = self.who(node);
        if let Some(content) = self.seen(node, cell, epoch) {
            let proved = content[..fleet.min(content.len())].to_vec();
            self.checker.fleet_proof(self.t, &who, cell, epoch, &proved);
        }
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("a log credits");
        // A completed round proves this node is not cut off.
        log.suspect_self = false;
        let (session, log_epoch) = (log.session.clone(), log.epoch);
        let entry = self.session_mut(&session);
        if entry.epoch == log_epoch {
            entry.credited = true;
        }
        self.fleet_reached(node, cell, epoch, fleet);
    }
}
