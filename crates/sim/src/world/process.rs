//! Processes: their start, crash, pause and clocks, and the work they run.

use super::*;

impl World {
    // -----------------------------------------------------------------------
    // Processes

    pub(super) fn restart(&mut self, node: usize) {
        self.nodes[node].restart_pending = false;
        if self.nodes[node].proc.is_some() || (self.audit && node < self.config.nodes) {
            return;
        }
        let id = self.next_proc;
        self.next_proc += 1;
        let name = self.nodes[node].name.clone();
        let config = self.core_config();
        let mono_base = self.net_rng.range(1_000, 1_000_000);
        // `take_clean_reload_generation`: the certificate is read once.
        let resume_generation = self.nodes[node].disk.clean_reload.take();
        if let Some(generation) = &resume_generation {
            let predecessor = generation[1..]
                .parse()
                .expect("generations name their process");
            self.stats.clean_reloads += 1;
            self.checker.on_clean_reload(predecessor, id);
        }
        self.procs.insert(
            id,
            Proc {
                node,
                state: State::new(name.clone(), config),
                pause: Pause::Running,
                deferred: Vec::new(),
                start_t: self.t,
                mono_base,
                suspended_ms: 0,
                wall_step_ms: 0,
                timers: BTreeMap::new(),
                next_ordinal: 0,
                replicas: BTreeMap::new(),
                published: BTreeMap::new(),
                requests: BTreeMap::new(),
                wake: WakeShell::default(),
                log: LogShell::default(),
                shutdown: None,
                resume_generation,
                life_tasks: BTreeMap::new(),
                fleet_ready: false,
                rebalance_settled_ms: 0,
                handoffs: BTreeMap::new(),
                directory: None,
                accepts: BTreeMap::new(),
                halting: false,
            },
        );
        self.nodes[node].proc = Some(id);
        self.note(|| format!("start p{id} on {name}"));
        if self.config.fleet {
            self.boot(id);
        } else {
            self.start_session(id);
        }
    }

    /// Install the process's lease and start its loops.
    pub(super) fn start_session(&mut self, id: ProcId) {
        let name = self.node_name(id);
        let spec = NodeLeaseSpec {
            addr: name,
            peer_protocol: PEER_PROTOCOL,
            generation: self.session_of(id).generation(),
            resume_generation: self.procs[&id].resume_generation.clone(),
            ttl_ms: self.config.ttl_ms,
        };
        self.drive(
            id,
            Event::StartNodeLease {
                now_ms: 0,
                now_mono_ms: 0,
                spec,
            },
        );
        let tick = self.net_rng.range(1, LOAD_SAMPLE_MS);
        self.work(id, tick, Work::LoadTick);
        self.work(id, 10, Work::BootScan { waited_ms: 0 });
        if self.config.lifecycle.rebalance {
            let tick = self.net_rng.range(1, LOAD_SAMPLE_MS);
            self.work(id, tick, Work::RebalanceTick);
        }
    }

    fn core_config(&self) -> Config {
        Config {
            max_resident: self.config.max_resident,
            max_activations: self.config.max_activations,
            max_evictions: 2,
            max_releases: 2,
            max_outbound_websockets: 32,
            ownership_on_evict: if self.config.sticky_eviction {
                OwnershipOnEvict::Sticky
            } else {
                OwnershipOnEvict::Release
            },
            require_node_lease: true,
            peer_protocol: PEER_PROTOCOL,
            operation_deadline_ms: Some(OPERATION_DEADLINE_MS),
            owner_log_recovery_backoff_ms: 1_000,
            owner_log_recovery_attempts: 240,
            alarm_resident_ms: self.config.alarm_resident_ms,
            idle_evict_ms: self.config.idle_evict_ms,
            pressure: pressure::PressureConfig::from_limits(None, Some(0)),
        }
    }

    pub(super) fn crash(&mut self, id: ProcId, wipe: bool) {
        self.stop_process(id, wipe, None);
    }

    /// The process ends. A crash restarts after the supervisor's wait; a
    /// clean exit names its own.
    pub(super) fn stop_process(&mut self, id: ProcId, wipe: bool, restart_ms: Option<u64>) {
        let Some(proc) = self.procs.remove(&id) else {
            return;
        };
        let node = proc.node;
        self.nodes[node].proc = None;
        // The open databases stay on disk exactly as the process left them,
        // with whatever it committed and never uploaded.
        for ((cell, epoch), replica) in proc.replicas {
            self.nodes[node]
                .disk
                .live
                .insert((cell, epoch), replica.content);
        }
        if wipe {
            self.nodes[node].disk = Disk::default();
        }
        if restart_ms.is_none() {
            self.note(|| format!("crash p{id}{}", if wipe { " (disk wiped)" } else { "" }));
        }
        if !self.audit && !self.nodes[node].restart_pending {
            // The supervisor waits at least one lease lifetime.
            let ttl = self.config.ttl_ms;
            let delay = match restart_ms {
                Some(delay) => delay,
                None => self.fault_rng.range(ttl, 3 * ttl),
            };
            self.nodes[node].restart_pending = true;
            self.after(delay, What::Restart { node });
        }
    }

    pub(super) fn resume(&mut self, id: ProcId) {
        let t = self.t;
        let Some(proc) = self.procs.get_mut(&id) else {
            return;
        };
        match proc.pause {
            Pause::Running => return,
            Pause::Paused => {}
            Pause::Suspended { since } => proc.suspended_ms += t - since,
        }
        proc.pause = Pause::Running;
        let deferred = std::mem::take(&mut proc.deferred);
        self.note(|| format!("resume p{id}"));
        for work in deferred {
            self.work(id, 0, work);
        }
    }

    pub(super) fn wall(&self, id: ProcId) -> u64 {
        let proc = &self.procs[&id];
        let wall = WALL_EPOCH_MS as i64
            + self.t as i64
            + self.nodes[proc.node].skew_ms
            + proc.wall_step_ms;
        wall.max(0) as u64
    }

    pub(super) fn mono(&self, id: ProcId) -> u64 {
        self.procs[&id].mono(self.t)
    }

    pub(super) fn proc_work(&mut self, id: ProcId, work: Work) {
        let Some(proc) = self.procs.get_mut(&id) else {
            return;
        };
        if proc.pause != Pause::Running {
            proc.deferred.push(work);
            return;
        }
        match work {
            Work::Deliver(event) => self.drive(id, event),
            Work::Timer { slot, ordinal } => self.timer(id, slot, ordinal),
            Work::StoreReply { call } => self.store_reply(id, call),
            Work::HandlerCommit { request } => self.handler_commit(id, request),
            Work::HandlerEnd { request } => self.handler_end(id, request),
            Work::ActivityFinished { request } => {
                let proc = self.procs.get_mut(&id).unwrap();
                if let Some(ctx) = proc.requests.get_mut(&request) {
                    if ctx.activity != Activity::Finished {
                        ctx.activity = Activity::Finished;
                        let cell = ctx.cell.clone();
                        self.finish_activity(id, request, &cell);
                    }
                }
            }
            Work::UploadKick { cell, epoch } => self.kick_upload(id, &cell, epoch),
            Work::WaiterDeadline { cell, epoch, op } => self.waiter_deadline(id, &cell, epoch, op),
            Work::LoadTick => {
                let proc = &self.procs[&id];
                let load = pressure::Load {
                    resident_cells: proc.state.residents().len(),
                    rss_bytes: 0,
                    in_use_bytes: 0,
                    cgroup_working_set_bytes: None,
                    cgroup_current_bytes: None,
                    container_reserved_bytes: 0,
                };
                self.drive(
                    id,
                    Event::LoadSampled {
                        load,
                        now_mono_ms: 0,
                    },
                );
                self.work(id, LOAD_SAMPLE_MS, Work::LoadTick);
            }
            Work::ClientArrive {
                op,
                attempt,
                request,
                epoch,
            } => self.client_arrive(id, op, attempt, request, epoch),
            Work::StaleRoute {
                op,
                attempt,
                cell,
                node,
                epoch,
            } => self.stale_route(id, op, attempt, cell, node, epoch),
            Work::Redispatch { op, attempt } => self.redispatch(id, op, attempt),
            Work::AlarmEnd { op } => self.alarm_end(id, op),
            Work::AlarmAnswer(event) => self.deliver_alarm_answer(id, event),
            Work::TaskProven { task, ok } => self.task_proven(id, task, ok),
            Work::TaskDeadline { task } => self.task_deadline(id, task),
            Work::BootScan { waited_ms } => self.boot_scan(id, waited_ms),
            Work::Log(work) => self.log_work(id, work),
            Work::ShutdownTick => self.shutdown_tick(id),
            Work::TokenRetry => self.token_retry_due(id),
            Work::RebalanceTick => self.rebalance_tick(id),
            Work::HandoffRetry { op } => self.handoff_scan(id, op),
            Work::HandoffDeadline { op } => self.handoff_deadline(id, op),
            Work::HandoffTimeout { op, attempt } => self.handoff_answered(id, op, attempt, None),
            Work::HandoffArrive {
                donor,
                op,
                attempt,
                cell,
                released_epoch,
            } => self.handoff_arrived(id, donor, op, attempt, cell, released_epoch),
            Work::HandoffAnswer {
                op,
                attempt,
                answer,
            } => self.handoff_answered(id, op, attempt, answer),
            Work::WakeTick { cleanup } => self.start_scan(
                id,
                if cleanup {
                    ScanKind::Cleanup
                } else {
                    ScanKind::Due
                },
            ),
        }
    }

    fn timer(&mut self, id: ProcId, slot: Slot, ordinal: u64) {
        let mono = self.mono(id);
        let proc = self.procs.get_mut(&id).unwrap();
        let Some((armed, _, at_mono)) = proc.timers.get(&slot) else {
            return;
        };
        if *armed != ordinal {
            return;
        }
        if mono < *at_mono {
            // The process clock fell behind the world, by a suspend.
            let delay = *at_mono - mono;
            self.work(id, delay, Work::Timer { slot, ordinal });
            return;
        }
        let (_, timer, _) = proc.timers.remove(&slot).unwrap();
        self.drive(
            id,
            Event::TimerFired {
                timer,
                now_ms: 0,
                now_mono_ms: 0,
            },
        );
    }
}
