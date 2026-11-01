//! Epoch GC (`LtxRepl::gc_superseded_epochs`): an owner deletes the epoch
//! prefixes of a cell below its restore chain's base.
//!
//! Every few seconds a process visits each database it holds open and is
//! not stopping, in
//! production's order, because the order is the fence: list the cell's
//! epochs and plan with `epoch_gc::plan`, which decides nothing unless the
//! newest listed epoch is the owner's own; only then read the owner record;
//! then delete each planned prefix. Each step is its own bucket call, so a
//! fence, a takeover or a late upload can land between any two of them, and
//! a call already sent applies even if its process dies.
//!
//! Every upload here carries the whole database, so each listed prefix is a
//! base of its own and the chain is the newest one. Listings are consistent,
//! so the retired mark's guard against a lagging one is not modelled. Its
//! effect on coverage is: a deleted epoch counts as covered whole
//! (`Store::percell_len`), and the oracle goes on counting its writes as
//! durable. Not modelled either: production runs one pass at a time, and
//! here a tick starts the next pass while the last one's calls may still be
//! in flight.

use celld_logic::epoch_gc::{self, ListedEpoch};
use celld_logic::Epoch;

use super::shell::StoreReq;
use super::{Work, World};
use crate::Mutation;

/// How often a process visits the cells it holds open.
const GC_TICK_MS: u64 = 5_000;

/// One bucket call of a cell's pass, by the owner of `epoch`.
#[derive(Debug)]
pub(super) struct GcReq {
    cell: String,
    epoch: Epoch,
    call: GcCall,
}

#[derive(Debug)]
enum GcCall {
    List,
    ReadOwner {
        delete: Vec<Epoch>,
    },
    /// Delete the first prefix; the rest go one call at a time after it.
    Delete {
        victims: Vec<Epoch>,
    },
}

#[derive(Debug)]
pub(super) enum GcWork {
    Tick,
    /// The listing came back.
    Listed {
        cell: String,
        epoch: Epoch,
        chain: Vec<Epoch>,
        listed: Vec<ListedEpoch>,
    },
    /// The owner record named this node at `epoch`, or a delete landed:
    /// delete the next prefix.
    Delete {
        cell: String,
        epoch: Epoch,
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
                for (cell, epoch) in self.resident_epochs(node) {
                    self.gc_call(node, cell, epoch, GcCall::List);
                }
            }
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
            GcWork::Delete {
                cell,
                epoch,
                victims,
            } => {
                if !victims.is_empty() {
                    self.gc_call(node, cell, epoch, GcCall::Delete { victims });
                }
            }
        }
    }

    /// Send one call of a pass. A pass whose call fails ends; the next tick
    /// starts a new one.
    fn gc_call(&mut self, node: usize, cell: String, epoch: Epoch, call: GcCall) {
        self.store_call(node, StoreReq::Gc(GcReq { cell, epoch, call }));
    }

    /// The call reaches the bucket. Returns the work its answer does.
    pub(super) fn gc_apply(&mut self, node: usize, req: GcReq) -> Work {
        let GcReq { cell, epoch, call } = req;
        let work = match call {
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
                if self.store.owner(&cell) != Some((Some(name.as_str()), epoch)) {
                    return Work::Nothing;
                }
                GcWork::Delete {
                    cell,
                    epoch,
                    victims: delete,
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
                GcWork::Delete {
                    cell,
                    epoch,
                    victims,
                }
            }
        };
        Work::Gc(work)
    }
}
