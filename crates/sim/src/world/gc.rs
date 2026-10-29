//! Epoch GC (`LtxRepl::gc_superseded_epochs`): an owner deletes the epoch
//! prefixes of a cell below its restore chain's base.
//!
//! The pass keeps production's order, because the order is the fence: list
//! the epochs and plan with `epoch_gc::plan`, which decides nothing unless
//! the chain's newest epoch is the owner's own; only then read `own.json`;
//! record the retired mark; delete. Each step is its own bucket call, so a
//! fence, a takeover or a late upload can land between any two of them.

use super::*;
use celld_logic::epoch_gc::{self, Plan};

/// How often an owner visits its cells.
pub(super) const GC_TICK_MS: u64 = 5_000;

#[derive(Debug)]
pub(in crate::world) enum GcStep {
    Scanned,
    OwnerRead(Plan),
    Recorded(Vec<Epoch>),
}

impl World {
    pub(super) fn gc_tick(&mut self, id: ProcId) {
        self.work(id, GC_TICK_MS, Work::GcTick);
        let proc = &self.procs[&id];
        if proc.shutdown.is_some() || !proc.state.node_authoritative() {
            return;
        }
        let cells: Vec<(String, Epoch)> = proc
            .published
            .iter()
            .map(|(cell, epoch)| (cell.clone(), *epoch))
            .collect();
        for (cell, epoch) in cells {
            let req = StoreReq::ScanEpochs { cell: cell.clone() };
            let step = GcStep::Scanned;
            self.life_call(id, req, LifeTask::Gc { cell, epoch, step });
        }
    }

    pub(super) fn gc_step(
        &mut self,
        id: ProcId,
        cell: String,
        epoch: Epoch,
        step: GcStep,
        result: Result<StoreRes, Failure>,
    ) {
        // Every step re-checks residency: the pass is the owner's.
        if self.procs[&id].published.get(&cell) != Some(&epoch) {
            return;
        }
        match (step, result) {
            (GcStep::Scanned, Ok(StoreRes::Epochs(chain, listed, recorded))) => {
                let Some(chain) = chain else {
                    return;
                };
                let (own, chain) = if self.config.mutation == Mutation::GcBelowOwn {
                    (epoch + 1, vec![epoch + 1])
                } else {
                    (epoch, chain)
                };
                // ponytail: the background fill is off, so a paged cell
                // reads its chain for its whole residency; model the fill's
                // completion if GC under paging needs it.
                let pinned = self.procs[&id]
                    .replicas
                    .get(&(cell.clone(), epoch))
                    .is_some_and(|replica| replica.paged);
                let grace_ms = self.config.epoch_gc.unwrap();
                let now_ms = self.wall(id);
                let Some(plan) =
                    epoch_gc::plan(own, &chain, &listed, recorded, pinned, now_ms, grace_ms)
                else {
                    return;
                };
                if !plan.record && plan.delete.is_empty() {
                    return;
                }
                let req = StoreReq::ReadOwner { cell: cell.clone() };
                let step = GcStep::OwnerRead(plan);
                self.life_call(id, req, LifeTask::Gc { cell, epoch, step });
            }
            (GcStep::OwnerRead(plan), Ok(StoreRes::Owner(record))) => {
                let node = self.node_name(id);
                let owns = record.is_some_and(|record| {
                    record.node.as_deref() == Some(node.as_str()) && record.epoch == epoch
                });
                if !owns {
                    return;
                }
                if plan.record {
                    let req = StoreReq::PutRetired {
                        cell: cell.clone(),
                        mark: plan.retired_below,
                    };
                    let step = GcStep::Recorded(plan.delete);
                    self.life_call(id, req, LifeTask::Gc { cell, epoch, step });
                } else {
                    self.gc_delete(id, cell, plan.delete);
                }
            }
            (GcStep::Recorded(delete), Ok(_)) => self.gc_delete(id, cell, delete),
            _ => {}
        }
    }

    fn gc_delete(&mut self, id: ProcId, cell: String, epochs: Vec<Epoch>) {
        if epochs.is_empty() {
            return;
        }
        self.stats.epochs_deleted += epochs.len() as u64;
        self.note(|| format!("p{id} deletes {cell} epochs {epochs:?}"));
        self.store_call(id, StoreReq::DeleteEpochs { cell, epochs }, Reply::Ignore);
    }
}
