//! What happens to a node besides its requests and its faults: an operator
//! evicting a cell by hand, and the parts of the node outside the core that
//! talk to it unprompted, such as the log tier nudging its lease.
//!
//! Each act arrives at its own rate per simulated minute across the fleet,
//! on a random node that is up, while clients are sending.

use celld_logic::{EvictError, EvictSuccess, EvictionAdmission, RequestId};

use super::clients::CLIENTS_STOP_BEFORE_END_MS;
use super::{Item, Work, World};

#[derive(Clone, Copy, Debug)]
pub(super) enum Act {
    /// `POST /evict/<cell>` on one node: `Message::Evict` in `actor.rs`.
    Evict,
    /// `Message::NudgeNodeLease`: renew the lease now.
    Nudge,
}

impl World {
    pub(super) fn start_operator(&mut self) {
        for act in [Act::Evict, Act::Nudge] {
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
                    Work::Evict { cell }
                }
                Act::Nudge => Work::Nudge,
            };
            // Through the process's queue, so a pause holds it back.
            self.work(node, 0, work);
        }
        let gap = self.act_gap(act);
        self.after(gap, Item::Operator(act));
    }

    /// The operator's eviction reaches the core.
    pub(super) fn evict(&mut self, node: usize, cell: String) {
        let request = self.next_request;
        self.next_request += 1;
        self.stats.evictions += 1;
        let p = self.machines[node].proc_mut();
        p.evictions.insert(request, cell.clone());
        let event = celld_logic::Event::EvictRequested {
            request,
            cell,
            now_mono_ms: 0,
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
            self.machines[node].proc_mut().evictions.remove(&request);
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
        let Some(cell) = p.evictions.remove(&request) else {
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
