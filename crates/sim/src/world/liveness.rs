//! The end of a run: does the fleet still work once the faults are gone?
//!
//! Safety checks run all along. They pass trivially for a fleet that
//! refuses everything, so a run ends with two liveness phases:
//!
//! 1. **Probe.** The workload stops, every fault has long healed, and every
//!    cell must take a write within `PROBE_WINDOW_MS`.
//! 2. **Audit.** Every process dies and every disk is wiped. One fresh node
//!    joins and must serve every cell, restoring it from the bucket alone.
//!    It can only take a cell over once its old owner's lease has expired,
//!    so its window is a few lease lifetimes longer. Each of those restores
//!    is also checked for lost writes, like any other.
//!
//! A phase is a set of goals: one per cell, retried until it is served or
//! its deadline passes. A goal that is never served is a violation.

use super::replicator::Disk;
use super::{Item, Node, Time, World};

/// How long every cell has to take a write once the probe starts.
pub(super) const PROBE_WINDOW_MS: u64 = 30_000;

/// One thing the fleet must manage before a deadline.
pub(super) struct Goal {
    phase: &'static str,
    cell: String,
    write: bool,
    /// The one node to send to, or `None` for any node that is up.
    ingress: Option<usize>,
    deadline: Time,
    served: bool,
}

impl World {
    /// Schedule the two phases and the end of the run.
    pub(super) fn start_liveness(&mut self) {
        let probe = self.config.duration_ms;
        let audit = probe + PROBE_WINDOW_MS;
        self.after(probe, Item::Probe);
        self.after(audit, Item::Audit);
        self.after(audit + self.audit_window(), Item::End);
    }

    fn audit_window(&self) -> u64 {
        4 * self.config.ttl_ms + PROBE_WINDOW_MS
    }

    pub(super) fn probe(&mut self) {
        self.note(|| "probe: every cell must take a write".into());
        let deadline = self.t + PROBE_WINDOW_MS;
        for cell in 0..self.config.cells {
            self.add_goal("probe", cell, true, None, deadline);
        }
    }

    pub(super) fn audit(&mut self) {
        self.note(|| "audit: the fleet dies; a fresh node restores every cell".into());
        self.auditing = true;
        for node in 0..self.nodes.len() {
            self.crash(node, true);
        }
        let node = self.nodes.len();
        self.nodes.push(Node::new(node, 0, 0, self.core));
        self.disks.push(Disk::default());
        self.skews.push(0);
        self.partitions.push(None);
        self.restart(node);
        let deadline = self.t + self.audit_window();
        for cell in 0..self.config.cells {
            self.add_goal("audit", cell, false, Some(node), deadline);
        }
    }

    fn add_goal(
        &mut self,
        phase: &'static str,
        cell: usize,
        write: bool,
        ingress: Option<usize>,
        deadline: Time,
    ) {
        let goal = self.goals.len();
        self.goals.push(Goal {
            phase,
            cell: format!("c{cell}"),
            write,
            ingress,
            deadline,
            served: false,
        });
        let delay = self.work_rng.range(1, 100);
        self.after(delay, Item::GoalSend { goal });
    }

    /// One attempt at a goal.
    pub(super) fn goal_send(&mut self, goal: usize) {
        let g = &self.goals[goal];
        if g.served {
            return;
        }
        let (cell, write, ingress) = (g.cell.clone(), g.write, g.ingress);
        let ingress = match ingress {
            Some(node) => self.nodes[node].up.then_some(node),
            None => self.pick_ingress(),
        };
        self.send(cell, write, ingress, Some(goal));
    }

    /// An attempt at a goal finished. A failed one is tried again, a little
    /// later, until the deadline.
    pub(super) fn goal_settled(&mut self, goal: usize, served: bool) {
        let g = &mut self.goals[goal];
        if served {
            g.served = true;
        } else if self.t < g.deadline {
            let delay = self.work_rng.range(200, 500);
            self.after(delay, Item::GoalSend { goal });
        }
    }

    /// Every goal that was never served is a violation.
    pub(super) fn judge_goals(&mut self) {
        for g in &self.goals {
            if !g.served {
                self.checker.unserved(g.deadline, g.phase, &g.cell);
            }
        }
    }
}
