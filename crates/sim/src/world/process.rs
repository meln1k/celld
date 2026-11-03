//! A machine's process: the supervisor starts it, it samples its load, and
//! it crashes. A crash drops everything the process held; the machine's
//! disk survives it unless the crash wipes it too.

use std::collections::BTreeMap;

use celld_logic::{pressure, Event, NodeLeaseSpec};

use super::handoff::Answer;
use super::replicator::Disk;
use super::{shutdown, Pending, Process, Work, World, LOAD_SAMPLE_MS, PEER_PROTOCOL};

impl World {
    /// The supervisor boots a process on `node`. In the fleet posture the
    /// boot first recovers its predecessor's log (`node_log`).
    pub(super) fn restart(&mut self, node: usize) {
        let m = &self.machines[node];
        if m.up() || m.retired {
            return;
        }
        if self.config.fleet {
            self.boot(node);
        } else {
            self.start(node);
        }
    }

    /// Start a new process on `node`: a fresh core that first acquires its
    /// node lease.
    pub(super) fn start(&mut self, node: usize) {
        self.machines[node].booting = false;
        // Large enough that a suspend can hold the clock back without it
        // ever reaching zero.
        let mono_base = self.net_rng.range(1_000_000_000, 2_000_000_000);
        let m = &mut self.machines[node];
        m.inc += 1;
        m.proc = Some(Process::new(&m.name, mono_base, self.core));
        let inc = m.inc;
        let resume_generation = m.reload.take();
        self.note(|| format!("start n{node}#{inc} (resuming {resume_generation:?})"));
        if resume_generation.is_some() {
            // The predecessor handed its epochs over: they are this
            // process's to run and to upload into now.
            let (old, new) = (self.who_at(node, inc - 1), self.who(node));
            self.checker.reloaded(&old, &new);
        }
        self.open_log(node);
        let spec = NodeLeaseSpec {
            addr: self.machines[node].name.clone(),
            peer_protocol: PEER_PROTOCOL,
            generation: shutdown::generation(node, inc),
            resume_generation,
            ttl_ms: self.config.ttl_ms,
        };
        // Production folds a sample before the first lease, so the lease
        // reports the node's memory headroom (`State::memory_headroom`).
        self.fold_load_sample(node);
        let event = Event::StartNodeLease {
            now_ms: 0,
            now_mono_ms: 0,
            spec,
        };
        self.drive(node, event);
        let delay = self.net_rng.range(1, LOAD_SAMPLE_MS);
        self.work(node, delay, Work::LoadSample);
        self.start_wake_loops(node);
        self.start_epoch_gc(node);
        self.start_ready_gate(node);
    }

    /// Production's periodic sampler.
    pub(super) fn sample_load(&mut self, node: usize) {
        self.judge_idle(node);
        self.fold_load_sample(node);
        self.work(node, LOAD_SAMPLE_MS, Work::LoadSample);
    }

    /// Measure the process's memory (`SimConfig::memory`) and hand the
    /// sample to its core. Without a memory model it measures nothing, and
    /// only the cell count varies; the sample still drives idle eviction.
    fn fold_load_sample(&mut self, node: usize) {
        let memory = self.config.memory.map_or(0, |m| {
            let p = self.machines[node].proc();
            let (heaps, open) = (p.isolates.heaps() as u64, p.replicas.len() as u64);
            let surge = match p.surge {
                Some((bytes, until)) if self.t < until => bytes,
                _ => 0,
            };
            m.base + m.heap * heaps + m.cell * open + surge
        });
        self.machines[node].proc_mut().memory = memory;
        let load = pressure::Load {
            resident_cells: self.machines[node].proc().state.occupied(),
            rss_bytes: memory,
            in_use_bytes: memory,
            cgroup_working_set_bytes: None,
            cgroup_current_bytes: None,
            container_reserved_bytes: 0,
        };
        let event = Event::LoadSampled {
            load,
            now_mono_ms: 0,
        };
        self.drive(node, event);
        let Some(p) = self.machines[node].proc.as_ref() else {
            return;
        };
        if p.state.shedding() {
            self.stats.shedding_samples += 1;
        }
        self.judge_memory(node);
        self.judge_ready(node);
        self.judge_reopen(node);
        self.judge_zombie(node);
        self.judge_resolved(node);
    }

    /// The process on `node` dies. Its open databases stay on disk as it
    /// left them, unless the disk is wiped too. Every request it held
    /// fails, and the supervisor starts a new process later, unless the
    /// machine is retired.
    pub(super) fn crash(&mut self, node: usize, wipe: bool) {
        let who = self.who(node);
        if !self.machines[node].up() {
            if self.machines[node].booting {
                self.kill_boot(node, wipe);
            }
            return;
        }
        self.judge_death(node);
        let dead = self.machines[node].proc.take().expect("up");
        self.count_probes(&dead.state);
        self.note(|| format!("crash {who}{}", if wipe { " (disk wiped)" } else { "" }));
        self.close_replicas(node, dead.replicas);
        if wipe {
            self.machines[node].disk = Disk::default();
        }
        let (mut requests, mut offers) = (BTreeMap::new(), Vec::new());
        for (request, pending) in dead.pending {
            match pending {
                Pending::Client(req) => {
                    requests.insert(request, req);
                }
                Pending::Offer(offer) => offers.push(offer),
                Pending::Gate(_) | Pending::Eviction(_) | Pending::Worker => {}
            }
        }
        self.fail_requests(node, requests);
        // Each donor waiting on this process sees its connection drop.
        for offer in offers {
            let delay = self.net_rng.range(1, 5);
            self.answer_offer(offer, Answer::Failed, delay);
        }
        if !self.machines[node].retired {
            self.schedule_restart(node);
        }
    }
}
