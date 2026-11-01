//! What happens to a node besides its requests and its faults: an operator
//! evicting a cell by hand, deploying a new version or shutting a node
//! down, and the parts of the node outside the core that talk to it
//! unprompted, such as the log tier nudging its lease and the balancer
//! giving cells away.
//!
//! Each act arrives at its own rate per simulated minute across the fleet,
//! on a random node that is up, while clients are sending.

use celld_logic::{Event, EvictError, EvictSuccess, EvictionAdmission, RequestId};

use super::clients::CLIENTS_STOP_BEFORE_END_MS;
use super::shutdown::{Mode, ShutdownWork};
use super::{Item, Pending, Work, World};

#[derive(Clone, Copy, Debug)]
pub(super) enum Act {
    /// `POST /evict/<cell>` on one node: `Message::Evict` in `actor.rs`.
    Evict,
    /// `Message::NudgeNodeLease`: renew the lease now.
    Nudge,
    /// A graceful shutdown (`SIGTERM`): drain, exit, and be restarted.
    Drain,
    /// A planned replacement on the same machine: preserve, exit, and
    /// reload in place.
    Preserve,
    /// `Message::Rebalance`: give a few idle cells to emptier peers.
    Rebalance,
    /// A new application version: the node adopts a new generation
    /// (`Message::GenerationChanged`).
    Deploy,
}

/// An act reaching the process it was aimed at.
pub(super) enum OperatorWork {
    /// An operator's eviction reaches the core.
    Evict { cell: String },
    /// The log tier asks the core to renew its lease now.
    Nudge,
    /// The balancer asks the core to give some idle cells away.
    Rebalance { cells: usize },
    /// The node adopts a new application generation.
    Deploy { max_age_ms: u64 },
}

impl World {
    pub(super) fn operator_work(&mut self, node: usize, work: OperatorWork) {
        match work {
            // A preserving actor ignores the balancer, and refuses evictions.
            OperatorWork::Rebalance { .. } | OperatorWork::Evict { .. }
                if self.preserving(node) => {}
            OperatorWork::Evict { cell } => self.evict(node, cell),
            OperatorWork::Nudge => {
                let event = Event::NudgeNodeLease {
                    now_ms: 0,
                    now_mono_ms: 0,
                };
                self.drive(node, event)
            }
            OperatorWork::Rebalance { cells } => self.drive(node, Event::Rebalance { cells }),
            OperatorWork::Deploy { max_age_ms } => self.deploy(node, max_age_ms),
        }
    }

    pub(super) fn start_operator(&mut self) {
        for act in [
            Act::Evict,
            Act::Nudge,
            Act::Drain,
            Act::Preserve,
            Act::Rebalance,
            Act::Deploy,
        ] {
            if self.act_rate(act) > 0.0 {
                let gap = self.act_gap(act);
                self.after(gap, Item::Operator(act));
            }
        }
    }

    fn act_rate(&self, act: Act) -> f64 {
        let operator = &self.config.operator;
        match act {
            Act::Evict => operator.evictions,
            Act::Nudge => operator.nudges,
            Act::Drain => operator.drains,
            Act::Preserve => operator.preserves,
            Act::Rebalance => operator.rebalances,
            Act::Deploy => operator.deploys,
        }
    }

    fn act_gap(&mut self, act: Act) -> u64 {
        let mean = (60_000.0 / self.act_rate(act)) as u64;
        self.op_rng.range(1, 2 * mean.max(1))
    }

    pub(super) fn operator(&mut self, act: Act) {
        if self.t + CLIENTS_STOP_BEFORE_END_MS >= self.config.duration_ms {
            return;
        }
        let up: Vec<usize> = (0..self.machines.len())
            .filter(|&node| self.machines[node].up())
            .collect();
        if let Some(&node) = self.op_rng.pick(&up) {
            let work = match act {
                Act::Evict => {
                    let cell = format!("c{}", self.op_rng.below(self.config.cells as u64));
                    Work::Operator(OperatorWork::Evict { cell })
                }
                Act::Nudge => Work::Operator(OperatorWork::Nudge),
                Act::Drain => Work::Shutdown(ShutdownWork::Begin(Mode::Drain)),
                Act::Preserve => Work::Shutdown(ShutdownWork::Begin(Mode::Preserve)),
                Act::Rebalance => Work::Operator(OperatorWork::Rebalance {
                    cells: self.op_rng.range(1, 3) as usize,
                }),
                Act::Deploy => Work::Operator(OperatorWork::Deploy {
                    max_age_ms: *self.op_rng.pick(&[0, 1_000, 10_000]).unwrap(),
                }),
            };
            // Through the process's queue, so a pause holds it back.
            self.work(node, 0, work);
        }
        let gap = self.act_gap(act);
        self.after(gap, Item::Operator(act));
    }

    /// The operator's eviction reaches the core.
    pub(super) fn evict(&mut self, node: usize, cell: String) {
        self.stats.evictions += 1;
        let request = self.pend(node, Pending::Eviction(cell.clone()));
        let event = Event::EvictRequested {
            request,
            cell,
            now_mono_ms: 0,
        };
        self.drive(node, event);
    }

    /// The node adopts the next application generation. Resident cells
    /// move to it at their next safe point, or by force after `max_age_ms`.
    pub(super) fn deploy(&mut self, node: usize, max_age_ms: u64) {
        self.stats.deploys += 1;
        let p = self.machines[node].proc_mut();
        p.generation += 1;
        let event = Event::GenerationChanged {
            generation: p.generation,
            now_mono_ms: 0,
            max_age_ms,
            eager_classes: Vec::new(),
        };
        self.drive(node, event);
    }

    /// The core admitted an eviction, or refused it at once.
    pub(super) fn eviction_admitted(
        &mut self,
        node: usize,
        request: RequestId,
        result: Result<EvictionAdmission, EvictError>,
    ) {
        if result != Ok(EvictionAdmission::Pending) {
            self.machines[node].proc_mut().take_eviction(request);
        }
    }

    /// An admitted eviction is over. One that evicted has stopped the
    /// cell's runtime here.
    pub(super) fn eviction_finished(
        &mut self,
        node: usize,
        request: RequestId,
        result: Result<EvictSuccess, EvictError>,
    ) {
        let p = self.machines[node].proc_mut();
        let Some(cell) = p.take_eviction(request) else {
            return;
        };
        if result == Ok(EvictSuccess::Evicted) {
            self.stats.evicted += 1;
            let serving = p.published.contains_key(&cell);
            let who = self.who(node);
            self.checker.evicted(self.t, &who, &cell, serving);
        }
    }
}
