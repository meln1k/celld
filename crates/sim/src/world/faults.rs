//! What goes wrong, and the supervisor that puts it right.
//!
//! Each kind of fault arrives at its own rate per simulated minute across
//! the fleet, drawn from the fault stream, and only until `chaos_ms`. After
//! that the fleet runs undisturbed, so whatever is still broken then is the
//! system's doing, not the fault's. Bucket call failures are drawn per call
//! in `shell.rs`; a fixed clock skew per node is drawn in `World::new`.

use super::{Item, World};

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Crash,
    Pause,
    Partition,
    ClockStep,
}

/// How a partitioned node's calls fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Partition {
    /// Packets vanish: every call hangs until it times out, and nobody
    /// knows whether it applied. (It did not.)
    Hang,
    /// Connections are refused at once: the call definitely did not apply.
    Refuse,
}

impl World {
    pub(super) fn start_faults(&mut self) {
        for fault in [
            Fault::Crash,
            Fault::Pause,
            Fault::Partition,
            Fault::ClockStep,
        ] {
            if self.rate(fault) > 0.0 {
                let gap = self.fault_gap(fault);
                self.after(gap, Item::Fault(fault));
            }
        }
    }

    fn rate(&self, fault: Fault) -> f64 {
        let faults = &self.config.faults;
        match fault {
            Fault::Crash => faults.crashes,
            Fault::Pause => faults.pauses,
            Fault::Partition => faults.partitions,
            Fault::ClockStep => faults.clock_steps,
        }
    }

    /// Time to the next fault of this kind: uniform around the mean its
    /// rate sets.
    fn fault_gap(&mut self, fault: Fault) -> u64 {
        let mean = (60_000.0 / self.rate(fault)) as u64;
        self.fault_rng.range(1, 2 * mean.max(1))
    }

    pub(super) fn fault(&mut self, fault: Fault) {
        if self.t >= self.config.chaos_ms {
            return;
        }
        let up: Vec<usize> = (0..self.nodes.len())
            .filter(|&node| self.nodes[node].up)
            .collect();
        if let Some(&node) = self.fault_rng.pick(&up) {
            match fault {
                Fault::Crash => {
                    let wipe = self.fault_rng.chance(self.config.faults.wipe_probability);
                    self.stats.crashes += 1;
                    self.crash(node, wipe);
                }
                Fault::Pause => self.pause(node),
                Fault::Partition => self.partition(node),
                Fault::ClockStep => {
                    let max = self.config.faults.max_clock_step_ms;
                    let step = self.fault_rng.range(0, 2 * max) as i64 - max as i64;
                    self.nodes[node].wall_step_ms += step;
                    self.note(|| format!("n{node}'s wall clock steps by {step} ms"));
                }
            }
        }
        let gap = self.fault_gap(fault);
        self.after(gap, Item::Fault(fault));
    }

    /// Freeze the process. Its work waits until it resumes; on a suspend
    /// its monotonic clock waits too.
    fn pause(&mut self, node: usize) {
        if self.nodes[node].paused_until.is_some() {
            return;
        }
        let duration = self
            .fault_rng
            .range(10, self.config.faults.max_pause_ms.max(10));
        let suspend = self
            .fault_rng
            .chance(self.config.faults.suspend_probability);
        let n = &mut self.nodes[node];
        n.paused_until = Some(self.t + duration);
        n.suspended_at = suspend.then_some(self.t);
        let inc = n.inc;
        self.stats.pauses += 1;
        let kind = if suspend { "suspend" } else { "pause" };
        self.note(|| format!("{kind} n{node}#{inc} for {duration} ms"));
        self.after(duration, Item::Resume { node, inc });
    }

    pub(super) fn resume(&mut self, node: usize, inc: u64) {
        let t = self.t;
        let n = &mut self.nodes[node];
        if !n.up || n.inc != inc {
            return;
        }
        n.paused_until = None;
        if let Some(at) = n.suspended_at.take() {
            // The monotonic clock did not count the time it was suspended.
            n.mono_base -= t - at;
        }
        self.note(|| format!("resume n{node}#{inc}"));
    }

    /// Cut the node off from the bucket and from its peers for a while.
    fn partition(&mut self, node: usize) {
        if self.partitions[node].is_some() {
            return;
        }
        let mode = if self.fault_rng.chance(0.5) {
            Partition::Hang
        } else {
            Partition::Refuse
        };
        let id = self.next_partition;
        self.next_partition += 1;
        self.partitions[node] = Some((id, mode));
        let max = self.config.faults.max_partition_ms.max(100);
        let duration = self.fault_rng.range(100, max);
        self.stats.partitions += 1;
        self.note(|| format!("partition n{node} ({mode:?}) for {duration} ms"));
        self.after(
            duration,
            Item::Heal {
                node,
                partition: id,
            },
        );
    }

    pub(super) fn heal(&mut self, node: usize, partition: u64) {
        if self.partitions[node].is_some_and(|(id, _)| id == partition) {
            self.partitions[node] = None;
            self.note(|| format!("heal n{node}"));
        }
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
