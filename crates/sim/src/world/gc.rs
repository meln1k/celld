//! Epoch GC (`LtxRepl::gc_superseded_epochs`): an owner deletes the epoch
//! prefixes of a cell below its restore chain's base.
//!
//! Every few seconds a process visits each database it holds open, in
//! production's order, because the order is the fence: list the cell's
//! epochs and plan with `epoch_gc::plan`, which decides nothing unless the
//! newest listed epoch is the owner's own; only then read the owner record;
//! then delete each planned prefix. Each step is its own bucket call, so a
//! fence, a takeover or a late upload can land between any two of them.
//!
//! Every upload here carries the whole database, so each listed prefix is a
//! base of its own and the chain is the newest one. Listings are consistent,
//! so the retired mark, which guards against a lagging one, is not modelled;
//! the oracle goes on counting a deleted prefix's writes as durable, as the
//! mark makes production's coverage do.

use celld_logic::epoch_gc::{self, ListedEpoch};
use celld_logic::Epoch;

use super::shell::{Fate, STORE_TIMEOUT_MS};
use super::{Work, World};
use crate::Mutation;

/// How often a process visits the cells it holds open.
const GC_TICK_MS: u64 = 5_000;

#[derive(Debug)]
pub(super) enum GcWork {
    Tick,
    /// One call of a cell's pass reaches the bucket. Its answer comes back
    /// `answer_ms` later, unless the caller already gave up on it.
    Apply {
        cell: String,
        epoch: Epoch,
        call: GcCall,
        answer_ms: Option<u64>,
    },
    /// The listing came back.
    Listed {
        cell: String,
        epoch: Epoch,
        chain: Vec<Epoch>,
        listed: Vec<ListedEpoch>,
    },
    /// The owner record came back: whether it names this node at `epoch`.
    Owned {
        cell: String,
        epoch: Epoch,
        owns: bool,
        delete: Vec<Epoch>,
    },
}

#[derive(Debug)]
pub(super) enum GcCall {
    List,
    ReadOwner {
        delete: Vec<Epoch>,
    },
    /// Delete the first prefix, then the rest one call at a time.
    Delete {
        victims: Vec<Epoch>,
    },
}

impl World {
    /// Start the process's epoch GC, if the fleet runs it
    /// (`CELLD_LTX_RETENTION_SECS`).
    pub(super) fn start_epoch_gc(&mut self, node: usize) {
        if self.config.epoch_gc_grace_ms.is_some() {
            let delay = self.net_rng.range(1, GC_TICK_MS);
            self.work(node, delay, Work::Gc(GcWork::Tick));
        }
    }

    pub(super) fn gc_work(&mut self, node: usize, work: GcWork) {
        match work {
            GcWork::Tick => {
                self.work(node, GC_TICK_MS, Work::Gc(GcWork::Tick));
                for (cell, epoch) in self.open_epochs(node) {
                    self.gc_call(node, cell, epoch, GcCall::List);
                }
            }
            GcWork::Apply {
                cell,
                epoch,
                call,
                answer_ms,
            } => self.gc_apply(node, cell, epoch, call, answer_ms),
            GcWork::Listed {
                cell,
                epoch,
                chain,
                listed,
            } => {
                let chain = if self.config.mutation == Some(Mutation::GcUnlisted) {
                    // The planted bug skips the proof that the owner's own
                    // epoch is listed: it plans as if it were the base.
                    vec![epoch]
                } else {
                    chain
                };
                let grace_ms = self.config.epoch_gc_grace_ms.expect("epoch GC runs");
                let now_ms = self.wall(node);
                let plan = epoch_gc::plan(epoch, &chain, &listed, None, false, now_ms, grace_ms);
                if let Some(plan) = plan.filter(|plan| !plan.delete.is_empty()) {
                    let call = GcCall::ReadOwner {
                        delete: plan.delete,
                    };
                    self.gc_call(node, cell, epoch, call);
                }
            }
            GcWork::Owned {
                cell,
                epoch,
                owns,
                delete,
            } => {
                if owns {
                    self.gc_call(node, cell, epoch, GcCall::Delete { victims: delete });
                }
            }
        }
    }

    /// Send one call of a pass. A pass whose call fails ends; the next tick
    /// starts a new one.
    fn gc_call(&mut self, node: usize, cell: String, epoch: Epoch, call: GcCall) {
        let (apply_ms, answer_ms) = match self.fate(node, STORE_TIMEOUT_MS) {
            Fate::Answered {
                request_ms,
                response_ms,
            } => (Some(request_ms), Some(response_ms)),
            Fate::Failed { applies_after, .. } => (applies_after, None),
        };
        if let Some(apply_ms) = apply_ms {
            let work = GcWork::Apply {
                cell,
                epoch,
                call,
                answer_ms,
            };
            self.work(node, apply_ms, Work::Gc(work));
        }
    }

    fn gc_apply(
        &mut self,
        node: usize,
        cell: String,
        epoch: Epoch,
        call: GcCall,
        answer_ms: Option<u64>,
    ) {
        let answer = match call {
            GcCall::List => {
                let (chain, listed) = self.store.list_epochs(&cell);
                GcWork::Listed {
                    cell,
                    epoch,
                    chain,
                    listed,
                }
            }
            GcCall::ReadOwner { delete } => {
                let name = &self.machines[node].name;
                let owns = self.store.owner(&cell) == Some((Some(name.as_str()), epoch));
                GcWork::Owned {
                    cell,
                    epoch,
                    owns,
                    delete,
                }
            }
            GcCall::Delete { mut victims } => {
                let victim = victims.remove(0);
                self.stats.epochs_deleted += 1;
                self.note(|| format!("epoch GC deletes {cell} epoch {victim}"));
                let base = self.store.newest(&cell).map(|(base, _)| base);
                self.checker.epoch_deleted(self.t, &cell, victim, base);
                self.store.delete_epoch(&cell, victim);
                self.judge_discoverable(&cell);
                if victims.is_empty() {
                    return;
                }
                // The next delete goes once this one is answered.
                GcWork::Owned {
                    cell,
                    epoch,
                    owns: true,
                    delete: victims,
                }
            }
        };
        if let Some(answer_ms) = answer_ms {
            self.work(node, answer_ms, Work::Gc(answer));
        }
    }
}
