//! What goes wrong, and the supervisor that puts it right.
//!
//! Faults are drawn from their own random stream, at a rate per simulated
//! minute across the whole fleet, and only until `chaos_ms`. After that the
//! fleet runs undisturbed, so whatever is still broken then is the
//! system's doing, not the fault's.

use super::{Item, World};

impl World {
    pub(super) fn start_faults(&mut self) {
        if self.config.faults.crashes > 0.0 {
            let gap = self.fault_gap();
            self.after(gap, Item::Fault);
        }
    }

    /// Time to the next fault: uniform around the mean the rate sets.
    fn fault_gap(&mut self) -> u64 {
        let mean = (60_000.0 / self.config.faults.crashes) as u64;
        self.fault_rng.range(1, 2 * mean.max(1))
    }

    pub(super) fn fault(&mut self) {
        if self.t >= self.config.chaos_ms {
            return;
        }
        let up: Vec<usize> = (0..self.nodes.len())
            .filter(|&node| self.nodes[node].up)
            .collect();
        if let Some(&node) = self.fault_rng.pick(&up) {
            let wipe = self.fault_rng.chance(self.config.faults.wipe_probability);
            self.stats.crashes += 1;
            self.crash(node, wipe);
        }
        let gap = self.fault_gap();
        self.after(gap, Item::Fault);
    }

    /// The supervisor restarts a dead process after one to three lease
    /// lifetimes. Waiting at least one means the dead process's lease has
    /// expired by the time the new one starts, as with a real restart that
    /// takes a while.
    pub(super) fn schedule_restart(&mut self, node: usize) {
        let ttl = self.config.ttl_ms;
        let delay = self.fault_rng.range(ttl, 3 * ttl);
        self.after(delay, Item::Restart { node });
    }
}
