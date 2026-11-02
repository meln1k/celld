//! Dead-session recovery (`recover_serial`) and its claim.
//!
//! Recovery runs over time, one bucket step after another, as production's
//! does:
//!
//! 1. **Claim.** Refuse while the dead session's lease is live by this
//!    node's clock. Otherwise CAS the lease body from `Open` to `Recovering`
//!    with this node as claimant, which also fences the session's own
//!    renewals. Behind another node's live claim, poll every 250 ms (a
//!    boot, or a takeover that needs the seal, cannot go on without it)
//!    until the log seals or the claim's heartbeat is 30 s stale, then take
//!    the claim over.
//! 2. **Gather.** Seal every follower that answers, upload every bundle of
//!    the session and what the followers hold, and declare a bounded loss
//!    if no witness is left. It takes the round trips production's does.
//!    While it runs the claimant beats its heartbeat every 5 s, and stops
//!    once it finds the claim taken over.
//! 3. **Seal.** Seal the log, only while this node still holds the claim.
//!
//! A partitioned, crashed or paused recoverer stops between steps and
//! leaves its claim behind; another node takes it over only once its
//! heartbeat is stale. A second call from the same process for the same
//! session joins the one under way, as production's per-session lock makes
//! it wait for it. Every claim decision is `log_tier`'s.
//!
//! Not modelled: the maintenance sweep (`sweep_dead_leaders`), the one
//! recoverer that leaves a live claim alone, and the progress record that
//! lets a takeover skip cells already uploaded (uploads are idempotent).
//! A step's read or write of the record does not fail on its own; only a
//! partition fails it.

use celld_logic::log_tier::LogState;
use celld_logic::{Event, Failure, OpId};

use super::super::{Item, Work, World};
use crate::store::Claim;
use crate::Mutation;

/// A claim whose heartbeat is older than this is stale (`RECOVERY_CLAIM_TTL`).
pub(in crate::world) const CLAIM_TTL_MS: u64 = 30_000;
/// How often a recovery behind a live claim reads the record again.
const CLAIM_POLL_MS: u64 = 250;
/// How often a claimant beats its heartbeat while it gathers.
const HEARTBEAT_MS: u64 = 5_000;

/// One recovery under way.
pub(in crate::world) struct Recovery {
    by: usize,
    runner: Runner,
    /// The node whose lease folds the log, and its session's generation.
    owner: String,
    generation: String,
    /// Who waits for the answer: the core's operations, or the boot.
    waiters: Vec<Waiter>,
    waited: bool,
}

/// What runs a recovery: a process, or a boot that has no process yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::world) enum Runner {
    Process(u64),
    Boot(u64),
}

/// One caller's answer: to the core's `op`, or to the boot, after
/// `answer_ms`, or never when its response is lost.
pub(in crate::world) struct Waiter {
    pub(in crate::world) op: Option<OpId>,
    pub(in crate::world) answer_ms: Option<u64>,
}

#[derive(Debug)]
pub(in crate::world) enum Step {
    Claim,
    /// A heartbeat while the gather runs, which ends at `gather_at`.
    Beat {
        gather_at: u64,
    },
    Gather,
    Seal,
}

impl World {
    /// Recover the log `owner`'s lease names, for `runner` on `by`, unless
    /// it is sealed or there is none: join the recovery this runner has
    /// under way for it, or start one.
    pub(in crate::world) fn recover(
        &mut self,
        by: usize,
        runner: Runner,
        owner: &str,
        waiter: Waiter,
    ) {
        // The planted bug lets the core's takeovers skip recovery.
        let skip = self.config.mutation == Some(Mutation::SkipLogRecovery)
            && matches!(runner, Runner::Process(_));
        let lease = self.store.read_lease(owner).filter(|_| !skip);
        let generation = match lease {
            Some(lease)
                if matches!(lease.log_state, Some(LogState::Open | LogState::Recovering)) =>
            {
                lease.generation
            }
            _ => return self.answer_recovery(by, runner, waiter, Ok(())),
        };
        let joined = (self.recoveries.values_mut()).find(|r| {
            r.by == by && r.runner == runner && r.owner == owner && r.generation == generation
        });
        if let Some(recovery) = joined {
            recovery.waiters.push(waiter);
            return;
        }
        let id = self.next_recovery;
        self.next_recovery += 1;
        let recovery = Recovery {
            by,
            runner,
            owner: owner.to_string(),
            generation,
            waiters: vec![waiter],
            waited: false,
        };
        self.recoveries.insert(id, recovery);
        self.recovery_step(id, Step::Claim);
    }

    pub(in crate::world) fn recovery_step(&mut self, id: u64, step: Step) {
        let Some(r) = self.recoveries.get(&id) else {
            return;
        };
        let (by, runner) = (r.by, r.runner);
        let m = &self.machines[by];
        let alive = match runner {
            Runner::Process(inc) => m.inc == inc && m.up(),
            Runner::Boot(boot) => m.booting && m.boot == boot && !m.up(),
        };
        if !alive {
            // The process died, or the boot gave up: its claim stays.
            self.recoveries.remove(&id);
            return;
        }
        if let Some(until) = m.proc.as_ref().and_then(|p| p.paused_until) {
            self.after(until - self.t, Item::Recovery { id, step });
            return;
        }
        // Every step reads or writes the bucket or the followers.
        if m.partition.is_some() {
            return self.end_recovery(id, Err(Failure::Ambiguous));
        }
        let (me, now) = (m.name.clone(), self.wall(by));
        let (owner, generation) = (r.owner.clone(), r.generation.clone());
        match step {
            Step::Claim => {
                // `ensure!(wire.expires_ms <= now)`: a live lease is an
                // error, not a skip.
                let lease = self.store.read_lease(&owner);
                if lease.is_some_and(|l| l.generation == generation && l.expires_ms > now) {
                    return self.end_recovery(id, Err(Failure::Ambiguous));
                }
                match self
                    .store
                    .claim_recovery(&owner, &generation, &me, now, CLAIM_TTL_MS)
                {
                    Claim::Done => self.end_recovery(id, Ok(())),
                    Claim::Live(claimant) => {
                        let r = self.recoveries.get_mut(&id).expect("looked up above");
                        if !std::mem::replace(&mut r.waited, true) {
                            self.stats.recovery_waits += 1;
                            self.note(|| {
                                format!("{me} waits behind {claimant}'s claim on {owner}")
                            });
                        }
                        self.after(
                            CLAIM_POLL_MS,
                            Item::Recovery {
                                id,
                                step: Step::Claim,
                            },
                        );
                    }
                    Claim::TookOver(stale) => {
                        self.stats.recovery_takeovers += 1;
                        self.note(|| format!("{me} takes over {stale}'s stale claim on {owner}"));
                        self.start_gather(id);
                    }
                    Claim::Held => self.start_gather(id),
                }
            }
            Step::Beat { gather_at } => {
                if !self.store.beat_recovery(&owner, &generation, &me, now) {
                    // `beat_claim`: taken over, or sealed meanwhile.
                    return self.end_recovery(id, Err(Failure::Ambiguous));
                }
                self.next_beat(id, gather_at);
            }
            Step::Gather => match self.gather_session(&generation) {
                Ok(()) => {
                    let delay = self.latency();
                    self.after(
                        delay,
                        Item::Recovery {
                            id,
                            step: Step::Seal,
                        },
                    );
                }
                Err(failure) => self.end_recovery(id, Err(failure)),
            },
            Step::Seal => {
                let sealed = self.store.finish_recovery(&owner, &generation, &me);
                if sealed.is_ok() {
                    self.stats.log_recoveries += 1;
                }
                self.end_recovery(id, sealed.map_err(|()| Failure::Ambiguous));
            }
        }
    }

    /// The claim is this node's: gather, beating the heartbeat until then.
    /// The gather takes as many bucket round trips as production's: a lease
    /// read and a seal for each member in turn, then a listing, the bundle
    /// reads and the uploads, each of those concurrent.
    fn start_gather(&mut self, id: u64) {
        let generation = self.recoveries[&id].generation.clone();
        let members = self.session(&generation).ensemble.len() as u64;
        let (lo, hi) = self.config.store_latency_ms;
        let gather_ms = (0..2 * members + 3)
            .map(|_| self.recovery_rng.range(lo, hi))
            .sum::<u64>();
        self.next_beat(id, self.t + gather_ms);
    }

    fn next_beat(&mut self, id: u64, gather_at: u64) {
        let step = match gather_at > self.t + HEARTBEAT_MS {
            true => Step::Beat { gather_at },
            false => Step::Gather,
        };
        let delay = match step {
            Step::Beat { .. } => HEARTBEAT_MS,
            _ => gather_at - self.t,
        };
        self.after(delay, Item::Recovery { id, step });
    }

    /// Seal every follower of `session` that can be reached, upload what
    /// they hold and every bundle of the session. The upload is the dead
    /// leader's own writes, so the oracle counts it as that process's.
    ///
    /// Without a witness, a session that may have fleet acks is missing
    /// some: recovery waits for every undecided member, then declares the
    /// bounded loss, as production does. A session that never proved
    /// anything has nothing to lose, and seals what it gathered.
    fn gather_session(&mut self, session: &str) -> Result<(), Failure> {
        let entry = self.session(session);
        let (dead, proved) = (self.who_at(entry.node, entry.inc), entry.proved);
        self.judge_activated(session);
        let gathered = self.gather(session, None);
        let loss = proved && !gathered.witnessed;
        let wait = self.config.mutation != Some(Mutation::LossWithoutWaiting);
        if loss && gathered.undecided && wait {
            return Err(Failure::Ambiguous);
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
        // Declared once what the recovery restores is in, so it forgives
        // only what it could not restore.
        if loss {
            let on_disk: Vec<_> = (self.machines.iter())
                .filter(|m| !m.retired)
                .flat_map(|m| &m.disk.log.copies)
                .filter(|((s, _, _), _)| s == session)
                .map(|((_, cell, epoch), copy)| (cell.clone(), *epoch, copy.rows.as_slice()))
                .collect();
            let lost = self.checker.declare_loss(self.t, &dead, &on_disk);
            self.stats.fleet_losses += lost;
        }
        Ok(())
    }

    fn end_recovery(&mut self, id: u64, result: Result<(), Failure>) {
        let r = self.recoveries.remove(&id).expect("a recovery under way");
        for waiter in r.waiters {
            self.answer_recovery(r.by, r.runner, waiter, result);
        }
    }

    fn answer_recovery(
        &mut self,
        node: usize,
        runner: Runner,
        waiter: Waiter,
        result: Result<(), Failure>,
    ) {
        let Some(delay) = waiter.answer_ms else {
            return;
        };
        let item = match (runner, waiter.op) {
            (Runner::Process(inc), Some(op)) => Item::Proc {
                node,
                inc,
                work: Work::Deliver(Event::NodeLogRecovered { op, result }),
            },
            (Runner::Boot(boot), _) => Item::Booted {
                node,
                boot,
                recovered: result.is_ok(),
            },
            (Runner::Process(_), None) => unreachable!("a process's recovery answers an op"),
        };
        self.after(delay, item);
    }
}
