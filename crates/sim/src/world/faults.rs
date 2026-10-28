//! Faults, and the phases that end them: the probe and the audit.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) enum FaultKind {
    Crash,
    Pause,
    Partition,
    ClockStep,
    Wedge,
    Deploy,
}

impl World {
    // -----------------------------------------------------------------------
    // Faults and phases

    pub(super) fn fault(&mut self, kind: FaultKind) {
        if !self.chaos {
            return;
        }
        let faults = self.config.faults.clone();
        let live: Vec<usize> = (0..self.config.nodes)
            .filter(|node| self.nodes[*node].proc.is_some())
            .collect();
        if let Some(&node) = self.fault_rng.pick(&live) {
            let id = self.nodes[node].proc.unwrap();
            match kind {
                FaultKind::Crash => {
                    let wipe = self.fault_rng.chance(faults.wipe_probability);
                    self.stats.crashes += 1;
                    self.crash(id, wipe);
                }
                FaultKind::Pause => {
                    let duration = self.fault_rng.range(10, faults.max_pause_ms.max(10));
                    let suspend = self.fault_rng.chance(faults.suspend_probability);
                    let t = self.t;
                    let proc = self.procs.get_mut(&id).unwrap();
                    if proc.pause == Pause::Running {
                        proc.pause = if suspend {
                            Pause::Suspended { since: t }
                        } else {
                            Pause::Paused
                        };
                        self.stats.pauses += 1;
                        self.note(|| {
                            format!(
                                "{} p{id} for {duration} ms",
                                if suspend { "suspend" } else { "pause" }
                            )
                        });
                        self.after(duration, What::Resume { proc: id });
                    }
                }
                FaultKind::Partition => {
                    if self.nodes[node].partition.is_none() {
                        let mode = if self.fault_rng.chance(0.5) {
                            PartitionMode::Hang
                        } else {
                            PartitionMode::Refuse
                        };
                        let partition = self.next_partition;
                        self.next_partition += 1;
                        self.nodes[node].partition = Some((partition, mode));
                        let duration = self.fault_rng.range(100, faults.max_partition_ms.max(100));
                        self.stats.partitions += 1;
                        self.note(|| format!("partition n{node} ({mode:?}) for {duration} ms"));
                        self.after(duration, What::Heal { node, partition });
                    }
                }
                FaultKind::ClockStep => {
                    let step = faults.max_clock_step_ms as i64;
                    let delta = self.fault_rng.range(0, 2 * step as u64) as i64 - step;
                    self.procs.get_mut(&id).unwrap().wall_step_ms += delta;
                    self.note(|| format!("step wall clock of p{id} by {delta} ms"));
                }
                FaultKind::Wedge => {
                    let duration = self.fault_rng.range(1_000, 2 * OPERATION_DEADLINE_MS);
                    self.procs.get_mut(&id).unwrap().wedged_until = self.t + duration;
                    self.note(|| format!("wedge the host of p{id} for {duration} ms"));
                }
                FaultKind::Deploy => self.deploy(id),
            }
        }
        let rate = match kind {
            FaultKind::Crash => faults.crashes,
            FaultKind::Pause => faults.pauses,
            FaultKind::Partition => faults.partitions,
            FaultKind::ClockStep => faults.clock_steps,
            FaultKind::Wedge => self.config.stops.wedges,
            FaultKind::Deploy => self.config.deploys.rate,
        };
        let gap = self.fault_gap(rate);
        self.after(gap, What::Fault(kind));
    }

    pub(super) fn evict_tick(&mut self) {
        let Some((lo, hi)) = self.config.evict_gap_ms else {
            return;
        };
        let gap = self.work_rng.range(lo, hi);
        self.after(gap, What::EvictTick);
        if self.audit {
            return;
        }
        let live: Vec<ProcId> = self
            .procs
            .iter()
            .filter(|(_, proc)| proc.pause == Pause::Running)
            .map(|(id, _)| *id)
            .collect();
        let Some(&id) = self.work_rng.pick(&live) else {
            return;
        };
        if self.config.stops.operator {
            return self.operator_evict(id);
        }
        if self.preserving(id) {
            // The actor refuses an eviction once a preserve began.
            return;
        }
        let residents = self.procs[&id].state.residents();
        if let Some(cell) = self.work_rng.pick(&residents).cloned() {
            self.stats.evictions_requested += 1;
            self.drive(id, Event::Evict { cell });
        }
    }

    /// One node adopts the next generation, as `adopt_deployment` does.
    fn deploy(&mut self, id: ProcId) {
        let deploys = self.config.deploys.clone();
        let proc = self.procs.get_mut(&id).unwrap();
        let generation = proc.state.current_generation() + 1;
        proc.deployed_at = self.t;
        let eager_classes = if deploys.eager {
            self.cells.clone()
        } else {
            Vec::new()
        };
        self.stats.deploys += 1;
        self.note(|| format!("p{id} adopts generation {generation}"));
        let now_mono_ms = self.procs[&id].mono(self.t);
        self.drive(
            id,
            Event::GenerationChanged {
                generation,
                now_mono_ms,
                max_age_ms: deploys.max_age_ms,
                eager_classes,
            },
        );
    }

    /// Every resident runtime on a node whose last deploy is long past its
    /// maximum age runs that node's generation.
    pub(super) fn check_generations(&mut self) {
        let settled = self.config.deploys.max_age_ms + GENERATION_SLACK_MS;
        let mut stale = Vec::new();
        for (id, proc) in &self.procs {
            let current = proc.state.current_generation();
            if current == 0 || self.t < proc.deployed_at + settled || proc.shutdown.is_some() {
                continue;
            }
            for cell in proc.state.residents() {
                let generation = proc.state.cell_generation(&cell).unwrap_or(current);
                if generation != current {
                    stale.push((*id, cell, generation, current));
                }
            }
        }
        let (checker, view) = self.checks();
        checker.on_stale_generations(view, &stale);
    }

    pub(super) fn chaos_end(&mut self) {
        self.chaos = false;
        self.note(|| "chaos ends: faults heal".into());
        for node in &mut self.nodes {
            node.partition = None;
        }
        for proc in self.procs.values_mut() {
            proc.wedged_until = 0;
        }
        let paused: Vec<ProcId> = self
            .procs
            .iter()
            .filter(|(_, proc)| proc.pause != Pause::Running)
            .map(|(id, _)| *id)
            .collect();
        for id in paused {
            self.resume(id);
        }
    }

    pub(super) fn probe(&mut self) {
        self.workload = false;
        self.note(|| "probe: every cell must take a write".into());
        let deadline = self.t + PROBE_WINDOW_MS;
        let audit = self.audit_start();
        for cell in self.cells.clone() {
            // Each probe also arms an alarm for just after the fleet dies,
            // which only the bucket's wake entries can then bring back.
            let alarm = AlarmIntent::Set {
                due: audit + self.work_rng.range(500, 2_500),
            };
            let op = self.new_op(cell, true, Some(alarm), deadline, true, None);
            self.probe_ops.push(op);
            self.client_send(op);
        }
    }

    pub(super) fn start_audit(&mut self) {
        self.audit = true;
        self.note(|| "audit: the fleet dies; a fresh node restores every cell".into());
        self.check_evictions_answered();
        self.check_generations();
        let procs: Vec<ProcId> = self.procs.keys().copied().collect();
        for id in procs {
            self.crash(id, true);
        }
        let node = self.nodes.len();
        self.nodes.push(Node {
            name: "audit".into(),
            disk: Disk::default(),
            proc: None,
            partition: None,
            skew_ms: 0,
            restart_pending: false,
        });
        self.restart(node);
        self.audit_proc = self.nodes[node].proc;
    }

    /// After the alarm window, a read through the audit node for every cell.
    pub(super) fn audit_reads(&mut self) {
        let node = self.nodes.len() - 1;
        let deadline = self.t + 4 * self.config.ttl_ms + PROBE_WINDOW_MS;
        for cell in self.cells.clone() {
            let op = self.new_op(cell, false, None, deadline, true, Some(node));
            self.audit_ops.push(op);
            let delay = self.work_rng.range(10, 100);
            self.after(delay, What::ClientRetry { op });
        }
    }
}
