//! Nodes leaving on purpose, as `crates/celld/main.rs` shuts down, and the
//! balancing loop that moves idle cells between the nodes that stay.
//!
//! A shutdown either hands every cell to the fleet or preserves ownership
//! for a same-node reload. A handoff claims the fleet drain token, lets the
//! admitted requests finish, sends `ReleaseAll`, and waits until the core
//! has released and handed off every cell or the deadline passes. A
//! preserve stops cold work with `BeginPreserve`, lets the residents finish
//! their requests, uploads each database, and leaves the clean-reload
//! certificate for its replacement, which resumes every cell in place at
//! the epoch it had. Either way the fleet posture seals the node log on the
//! way out. A node that misses its deadline simply exits, and its cells
//! follow the lease-expiry takeover a crash would.

use super::*;
use crate::store::LeaseLoad;
use celld_logic::drain::{self, DrainToken};

/// The drain token's lifetime (`drain_token::TOKEN_TTL_MS`).
const TOKEN_TTL_MS: u64 = 120_000;
/// How long a donor waits for the token before it drains unserialized.
const TOKEN_WAIT_MS: u64 = 5_000;
const TOKEN_RETRY_MS: u64 = 1_000;
/// A shutdown that makes no progress for this long gives up and exits.
const STALL_WINDOW_MS: u64 = 10_000;
/// The whole shutdown, drained or not.
const SHUTDOWN_DEADLINE_MS: u64 = 30_000;
const SHUTDOWN_TICK_MS: u64 = 50;
/// `DEFAULT_REBALANCE_BATCH_CELLS`.
const REBALANCE_BATCH: usize = 32;
const REBALANCE_TICK_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ShutdownMode {
    /// Hand every cell to the fleet.
    Handoff,
    /// Keep ownership for the replacement on this node.
    Preserve,
}

#[derive(Debug)]
pub(super) struct Shutdown {
    pub(super) mode: ShutdownMode,
    step: Step,
    deadline: Time,
    stall_deadline: Time,
    handed_off: u64,
    /// The drain token: `None` until the claim settles, then whether it is
    /// held and under which ETag.
    token: Option<Option<String>>,
    token_since: Time,
    /// Whether the core drained before the deadline.
    drained: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// The admitted requests finish before the cells move.
    PreDrain,
    Draining,
    /// The node log seals what the bucket covers.
    Quiescing {
        until: Time,
    },
    /// Each preserved database is uploaded before the certificate.
    Preparing,
}

/// A lifecycle step waiting on the bucket.
#[derive(Debug)]
pub(super) enum LifeTask {
    TokenRead,
    TokenClaim,
    ReadyToken,
    ReadyPeers {
        token: Option<DrainToken>,
    },
    RebalancePeers,
    /// Epoch GC, one step of a pass over `cell` at the owner's `epoch`.
    Gc {
        cell: String,
        epoch: Epoch,
        step: super::gc::GcStep,
    },
}

impl World {
    /// The load the next lease write reports. Only the handoff and the
    /// balancing loop read the restore backlog and the handoff flag, so a
    /// fleet without them reports neither.
    pub(super) fn lease_load(&self, id: ProcId) -> LeaseLoad {
        let state = &self.procs[&id].state;
        let hands_off = self.config.lifecycle.hands_off();
        LeaseLoad {
            resident: state.residents().len(),
            owned: state.owned_cells(),
            restoring: if hands_off {
                state.activation_backlog() as u64
            } else {
                0
            },
            paced_handoff: hands_off,
            draining: state.draining(),
            memory: self.config.memory.as_ref().map(|_| {
                let bytes = self.memory_bytes(id);
                (state.shedding(), state.memory_headroom(), bytes)
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Shutdowns

    pub(super) fn shutdown_fault(&mut self) {
        let lifecycle = self.config.lifecycle.clone();
        if !self.chaos || lifecycle.shutdowns <= 0.0 {
            return;
        }
        let gap = self.fault_gap(lifecycle.shutdowns);
        self.after(gap, What::Shutdown);
        let ready: Vec<ProcId> = self
            .procs
            .iter()
            .filter(|(_, proc)| {
                proc.pause == Pause::Running
                    && proc.shutdown.is_none()
                    && proc.state.node_authoritative()
            })
            .map(|(id, _)| *id)
            .collect();
        let Some(&id) = self.fault_rng.pick(&ready) else {
            return;
        };
        let mode = if self.fault_rng.chance(lifecycle.preserve_probability) {
            ShutdownMode::Preserve
        } else {
            ShutdownMode::Handoff
        };
        self.begin_shutdown(id, mode);
    }

    fn begin_shutdown(&mut self, id: ProcId, mode: ShutdownMode) {
        self.stats.shutdowns += 1;
        self.note(|| format!("p{id} shuts down ({mode:?})"));
        let t = self.t;
        let proc = self.procs.get_mut(&id).unwrap();
        proc.shutdown = Some(Shutdown {
            mode,
            step: Step::PreDrain,
            deadline: t + SHUTDOWN_DEADLINE_MS,
            stall_deadline: t + STALL_WINDOW_MS,
            handed_off: 0,
            token: None,
            token_since: t,
            drained: false,
        });
        match mode {
            ShutdownMode::Handoff => self.claim_token(id),
            ShutdownMode::Preserve => {
                self.shutdown_mut(id).step = Step::Draining;
                self.drive(id, Event::BeginPreserve);
            }
        }
        self.work(id, SHUTDOWN_TICK_MS, Work::ShutdownTick);
    }

    fn shutdown_mut(&mut self, id: ProcId) -> &mut Shutdown {
        self.procs.get_mut(&id).unwrap().shutdown.as_mut().unwrap()
    }

    /// Whether this process refuses a new request: its listener closed when
    /// the shutdown began, and a preserving node refuses peers too.
    pub(super) fn refuses(&self, id: ProcId, forwarded: bool) -> bool {
        self.procs[&id]
            .shutdown
            .as_ref()
            .is_some_and(|shutdown| !forwarded || shutdown.mode == ShutdownMode::Preserve)
    }

    pub(super) fn preserving(&self, id: ProcId) -> bool {
        self.procs[&id]
            .shutdown
            .as_ref()
            .is_some_and(|shutdown| shutdown.mode == ShutdownMode::Preserve)
    }

    pub(super) fn shutdown_tick(&mut self, id: ProcId) {
        let t = self.t;
        let Some(shutdown) = self.procs[&id].shutdown.as_ref() else {
            return;
        };
        let (mode, step, deadline) = (shutdown.mode, shutdown.step, shutdown.deadline);
        let state = &self.procs[&id].state;
        let open: Vec<&ReqCtx> = self.procs[&id]
            .requests
            .values()
            .filter(|request| !request.replied)
            .collect();
        match step {
            Step::PreDrain => {
                // `public_in_flight`: what clients sent here before the
                // listener closed. Peers still forward to a handoff donor,
                // and the drain does not wait for those.
                let admitted = open.iter().any(|request| request.via.is_none());
                let token_settled = shutdown.token.is_some();
                if !admitted && token_settled {
                    self.shutdown_mut(id).step = Step::Draining;
                    self.drive(id, Event::ReleaseAll);
                } else if t >= shutdown.stall_deadline.min(deadline) {
                    // The admitted requests outlived the window: no handoff.
                    self.quiesce(id);
                }
            }
            Step::Draining => {
                let drained = match mode {
                    ShutdownMode::Handoff => {
                        self.config.mutation == Mutation::DrainEarly
                            || state.occupied() == 0
                                && state.activating() == 0
                                && state.quiescing() == 0
                                && state.evicting() == 0
                                && state.releasing() == 0
                                && state.adopting() == 0
                    }
                    ShutdownMode::Preserve => {
                        open.is_empty() && state.activating() == 0 && state.evicting() == 0
                    }
                };
                let handed_off = state.handed_off();
                let shutdown = self.shutdown_mut(id);
                if handed_off > shutdown.handed_off {
                    shutdown.handed_off = handed_off;
                    shutdown.stall_deadline = (t + STALL_WINDOW_MS).min(deadline);
                }
                if drained {
                    shutdown.drained = true;
                    self.quiesce(id);
                } else if t >= shutdown.stall_deadline.min(deadline) {
                    self.quiesce(id);
                }
            }
            Step::Quiescing { until } => {
                let closed = !self.config.fleet || self.log_closed(id);
                if closed || t >= until.min(deadline) {
                    self.prepare(id, closed);
                }
            }
            Step::Preparing => {
                let uploaded = self.procs[&id]
                    .replicas
                    .values()
                    .all(|replica| replica.uploaded >= replica.content.len());
                if uploaded {
                    self.certify(id);
                    return self.exit(id);
                }
                if t >= deadline {
                    return self.exit(id);
                }
            }
        }
        if self.procs.contains_key(&id) {
            self.work(id, SHUTDOWN_TICK_MS, Work::ShutdownTick);
        }
    }

    /// The drain is over, or out of time: the node log seals what the
    /// bucket already covers.
    fn quiesce(&mut self, id: ProcId) {
        let until = self.t + SHUTDOWN_DEADLINE_MS;
        self.shutdown_mut(id).step = Step::Quiescing { until };
        if self.config.fleet {
            self.close_gracefully(id);
        }
    }

    /// A preserve that drained and sealed prepares the clean reload; every
    /// other shutdown exits now.
    fn prepare(&mut self, id: ProcId, quiesced: bool) {
        let shutdown = self.shutdown_mut(id);
        if shutdown.mode == ShutdownMode::Preserve && shutdown.drained && quiesced {
            shutdown.step = Step::Preparing;
            let keys: Vec<(String, Epoch)> = self.procs[&id].replicas.keys().cloned().collect();
            for (cell, epoch) in keys {
                self.kick_upload(id, &cell, epoch);
            }
        } else {
            self.exit(id);
        }
    }

    /// `prepare_clean_reload`'s pruning, then `write_clean_reload_marker`
    /// once the node still holds its lease. Every live database the core
    /// does not hold resident is a stale epoch the successor must not
    /// resume; the certificate is abandoned unless what remains on disk is
    /// exactly the resident set.
    fn certify(&mut self, id: ProcId) {
        let keep: BTreeSet<(String, Epoch)> = self.procs[&id]
            .state
            .presence_snapshot()
            .cells
            .into_iter()
            .map(|cell| (cell.id, cell.epoch))
            .collect();
        let node = self.procs[&id].node;
        let disk = &mut self.nodes[node].disk;
        let prune = self.config.mutation != Mutation::ResumeStaleLive;
        // The planted bug prunes only what shadows a newer epoch of the
        // same cell, so every cell keeps one live database.
        let shadowed = |(cell, epoch): &(String, Epoch)| {
            keep.iter().any(|(kept, _)| kept == cell)
                || disk
                    .live
                    .keys()
                    .any(|(other, newer)| other == cell && newer > epoch)
        };
        let stale: Vec<(String, Epoch)> = disk
            .live
            .keys()
            .filter(|key| !keep.contains(*key) && (prune || shadowed(key)))
            .cloned()
            .collect();
        for key in &stale {
            disk.live.remove(key);
            disk.evicted.remove(key);
        }
        let remaining: BTreeSet<(String, Epoch)> = disk
            .live
            .keys()
            .chain(disk.evicted.keys())
            .cloned()
            .collect();
        if prune && remaining != keep {
            self.note(|| format!("p{id} abandons the clean reload: inventory mismatch"));
            return;
        }
        if !self.procs[&id].state.node_authoritative() {
            return;
        }
        let generation = self.session_of(id).generation();
        let node = self.procs[&id].node;
        self.note(|| format!("p{id} leaves a clean-reload certificate"));
        self.nodes[node].disk.clean_reload = Some(generation);
    }

    /// The process ends. A drained node's replacement starts soon after;
    /// within a lease lifetime, a preserved one resumes its cells.
    fn exit(&mut self, id: ProcId) {
        let shutdown = self.procs[&id].shutdown.as_ref().unwrap();
        let (mode, drained, token) = (shutdown.mode, shutdown.drained, shutdown.token.clone());
        if let Some(Some(etag)) = token {
            // `drain_token::release`: expire the claim, keep the baseline.
            let token = DrainToken {
                node: self.node_name(id),
                expires_ms: 0,
                restoration_baseline: Vec::new(),
            };
            let guard = CasGuard::Match(etag);
            self.store_call(id, StoreReq::CasDrainToken { guard, token }, Reply::Ignore);
        }
        if mode == ShutdownMode::Handoff && drained {
            self.stats.drains += 1;
            let node = self.node_name(id);
            let state = &self.procs[&id].state;
            let held: Vec<String> = self
                .store
                .owned_by(&node)
                .into_iter()
                .filter(|cell| state.accepted_epoch(cell) == self.store.owner_epoch(cell))
                .collect();
            let (checker, view) = self.checks();
            checker.on_drained(view, &node, &held);
        }
        self.note(|| format!("p{id} exits ({mode:?}, drained: {drained})"));
        let restart = self.fault_rng.range(20, self.config.ttl_ms / 2);
        self.stop_process(id, false, Some(restart));
    }

    // -----------------------------------------------------------------------
    // The drain token

    fn claim_token(&mut self, id: ProcId) {
        self.life_call(id, StoreReq::ReadDrainToken, LifeTask::TokenRead);
    }

    fn token_read(&mut self, id: ProcId, result: Result<StoreRes, Failure>) {
        let current = match result {
            Ok(StoreRes::DrainToken(current)) => current,
            Ok(other) => unreachable!("expected the drain token, got {other:?}"),
            Err(_) => return self.token_retry(id),
        };
        let node = self.node_name(id);
        let now = self.wall(id);
        if !drain::may_claim(current.as_ref().map(|(token, _)| token), &node, now) {
            return self.token_retry(id);
        }
        let guard = match current {
            Some((_, etag)) => CasGuard::Match(etag),
            None => CasGuard::Absent,
        };
        let token = DrainToken {
            node,
            expires_ms: now + TOKEN_TTL_MS,
            restoration_baseline: Vec::new(),
        };
        let req = StoreReq::CasDrainToken { guard, token };
        self.life_call(id, req, LifeTask::TokenClaim);
    }

    fn token_claimed(&mut self, id: ProcId, result: Result<StoreRes, Failure>) {
        match result {
            Ok(StoreRes::DrainTokenCas(Some(etag))) => {
                self.note(|| format!("p{id} holds the drain token"));
                self.shutdown_mut(id).token = Some(Some(etag));
            }
            Ok(StoreRes::DrainTokenCas(None)) | Err(_) => self.token_retry(id),
            Ok(other) => unreachable!("expected a drain token CAS, got {other:?}"),
        }
    }

    /// Try again, or drain unserialized once the wait is spent.
    fn token_retry(&mut self, id: ProcId) {
        let t = self.t;
        let shutdown = self.shutdown_mut(id);
        if t >= shutdown.token_since + TOKEN_WAIT_MS {
            shutdown.token = Some(None);
            self.note(|| format!("p{id} drains without the token"));
            return;
        }
        self.work(id, TOKEN_RETRY_MS, Work::TokenRetry);
    }

    pub(super) fn token_retry_due(&mut self, id: ProcId) {
        if self.procs[&id].shutdown.is_some() {
            self.claim_token(id);
        }
    }

    // -----------------------------------------------------------------------
    // Balancing

    /// The first readiness gate, then one balancing plan per tick.
    pub(super) fn rebalance_tick(&mut self, id: ProcId) {
        self.work(id, REBALANCE_TICK_MS, Work::RebalanceTick);
        let proc = &self.procs[&id];
        if proc.shutdown.is_some() || !proc.state.node_authoritative() {
            return;
        }
        if proc.fleet_ready {
            self.life_call(id, StoreReq::ReadPeers, LifeTask::RebalancePeers);
        } else {
            self.life_call(id, StoreReq::ReadDrainToken, LifeTask::ReadyToken);
        }
    }

    fn ready_peers(
        &mut self,
        id: ProcId,
        token: Option<DrainToken>,
        result: Result<StoreRes, Failure>,
    ) {
        let Ok(StoreRes::Peers { peers, .. }) = result else {
            return;
        };
        let node = self.node_name(id);
        let now = self.wall(id);
        let max_restoring = self.config.max_activations as u64;
        if drain::fleet_status(token.as_ref(), &node, now, &peers, max_restoring).is_ok() {
            self.note(|| format!("p{id} is ready"));
            self.procs.get_mut(&id).unwrap().fleet_ready = true;
        }
    }

    fn rebalance_peers(&mut self, id: ProcId, result: Result<StoreRes, Failure>) {
        let Ok(StoreRes::Peers { view_ms, peers }) = result else {
            return;
        };
        let proc = &self.procs[&id];
        if proc.shutdown.is_some() {
            return;
        }
        let state = &proc.state;
        let in_flight = state.quiescing() + state.evicting() + state.releasing() + state.adopting();
        if in_flight >= REBALANCE_BATCH {
            return;
        }
        let node = self.node_name(id);
        let cells = celld_logic::rebalance::surplus(
            &peers,
            &node,
            view_ms,
            self.config.ttl_ms,
            proc.rebalance_settled_ms,
            REBALANCE_BATCH - in_flight,
        );
        let Some(cells) = cells else {
            return;
        };
        self.stats.rebalances += 1;
        let settled = self.wall(id) + LOAD_SAMPLE_MS;
        self.procs.get_mut(&id).unwrap().rebalance_settled_ms = settled;
        self.drive(id, Event::Rebalance { cells });
    }

    // -----------------------------------------------------------------------
    // Bucket calls

    pub(super) fn life_call(&mut self, id: ProcId, req: StoreReq, task: LifeTask) {
        let key = self.next_task;
        self.next_task += 1;
        self.procs
            .get_mut(&id)
            .unwrap()
            .life_tasks
            .insert(key, task);
        self.store_call(id, req, Reply::Life { task: key });
    }

    pub(super) fn life_reply(&mut self, id: ProcId, key: u64, result: Result<StoreRes, Failure>) {
        let Some(task) = self.procs.get_mut(&id).unwrap().life_tasks.remove(&key) else {
            return;
        };
        let shutting = self.procs[&id].shutdown.is_some();
        match task {
            LifeTask::TokenRead if shutting => self.token_read(id, result),
            LifeTask::TokenClaim if shutting => self.token_claimed(id, result),
            LifeTask::TokenRead | LifeTask::TokenClaim => {}
            LifeTask::ReadyToken => {
                let token = match result {
                    Ok(StoreRes::DrainToken(current)) => current.map(|(token, _)| token),
                    _ => return,
                };
                let task = LifeTask::ReadyPeers { token };
                self.life_call(id, StoreReq::ReadPeers, task);
            }
            LifeTask::ReadyPeers { token } => self.ready_peers(id, token, result),
            LifeTask::RebalancePeers => self.rebalance_peers(id, result),
            LifeTask::Gc { cell, epoch, step } => self.gc_step(id, cell, epoch, step, result),
        }
    }
}
