//! The end of a run: does the fleet still work once the faults are gone?
//!
//! Safety checks run all along. They pass trivially for a fleet that
//! refuses everything, so a run ends with two liveness phases:
//!
//! 1. **Probe.** The workload stops, every fault has healed and every
//!    restart it caused has happened, and every cell must take a write
//!    within `PROBE_WINDOW_MS`.
//! 2. **Audit.** Every machine retires: its process dies and never comes
//!    back, so nothing on its disk can help. One fresh node joins and must
//!    serve every cell, restoring it from the bucket alone.
//!    It can only take a cell over once its old owner's lease has expired,
//!    so its window is a few lease lifetimes longer. Each of those restores
//!    is also checked for lost writes, like any other.
//!
//! A phase is a set of goals: one per cell, retried until it is served or
//! its deadline passes. A goal that is never served is a violation.

use super::{Item, Machine, Time, World};

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
    /// The probe comes once the workload ends; each phase schedules the
    /// next when it begins.
    pub(super) fn start_liveness(&mut self) {
        self.after(self.config.duration_ms, Item::Probe);
    }

    /// A fault, or the restart that follows it, still disturbs the fleet
    /// until `until`: the probe must not start before then.
    pub(super) fn disturbed_until(&mut self, until: Time) {
        self.quiet_from = self.quiet_from.max(until);
    }

    fn audit_window(&self) -> u64 {
        4 * self.config.ttl_ms + PROBE_WINDOW_MS
    }

    pub(super) fn probe(&mut self) {
        if self.t < self.quiet_from {
            // A late pause, partition or restart is still under way. What
            // the fleet does before it ends is the fault's doing.
            self.after(self.quiet_from - self.t, Item::Probe);
            return;
        }
        self.note(|| "probe: every cell must take a write".into());
        let deadline = self.t + PROBE_WINDOW_MS;
        for cell in 0..self.config.cells {
            self.add_goal("probe", cell, true, None, deadline);
        }
        self.after(PROBE_WINDOW_MS, Item::Audit);
    }

    pub(super) fn audit(&mut self) {
        // The probe is over: every alarm due well before now has fired.
        self.judge_alarms();
        self.note(|| "audit: the fleet dies; a fresh node restores every cell".into());
        for node in 0..self.machines.len() {
            self.machines[node].retired = true;
            self.crash(node, false);
        }
        let node = self.machines.len();
        self.machines.push(Machine::new(node, 0));
        self.restart(node);
        let deadline = self.t + self.audit_window();
        for cell in 0..self.config.cells {
            self.add_goal("audit", cell, false, Some(node), deadline);
        }
        self.after(self.audit_window(), Item::End);
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
            Some(node) => self.machines[node].up().then_some(node),
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
