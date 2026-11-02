//! The bucket client and the network to the bucket: what happens to one
//! request (`Fate`), and how each request applies at its one instant.

use celld_logic::log_tier::LogState;
use celld_logic::wake::AlarmSnapshot;
use celld_logic::{
    CasGuard, CasOutcome, Epoch, Event, Failure, LeaseCasOutcome, NodeLeaseRecord, OpId,
};

use super::alarms::{AlarmWork, Waiter};
use super::drain::DrainReq;
use super::faults::Partition;
use super::gc::GcReq;
use super::node_log::{Flushed, LogWork};
use super::replicator::ReplicaWork;
use super::waker::WakerReq;
use super::{Item, Work, World, WALL_EPOCH_MS};
use crate::store::{Load, NoBase, Row};
use crate::WriteId;

/// The bucket client's timeout for one attempt (`crates/celld/bucket.rs`).
pub(super) const STORE_TIMEOUT_MS: u64 = 15_000;

/// What happens to one bucket call.
pub(super) enum Fate {
    /// It applies after `request_ms`, and its answer comes back
    /// `response_ms` later.
    Answered { request_ms: u64, response_ms: u64 },
    /// The caller hears `failure` after `at`. With `applies_after`, the
    /// request still reaches the bucket then; nobody hears its answer.
    Failed {
        failure: Failure,
        at: u64,
        applies_after: Option<u64>,
    },
}

/// A request on its way to the bucket. The core's own requests carry its
/// `op`; uploads and chain reads belong to the replicator.
#[derive(Debug)]
pub(super) enum StoreReq {
    ReadSelfLease {
        op: OpId,
    },
    CasLease {
        op: OpId,
        guard: CasGuard,
        record: NodeLeaseRecord,
        load: Load,
        /// The log epoch whose record the body carries with
        /// `may_have_fleet_acks` set, once an activation asked for it.
        acks: Option<u64>,
    },
    /// The fleet's leases, to place an unowned cell on a peer with room.
    ReadCapacityPeers {
        op: OpId,
    },
    ReadOwner {
        op: OpId,
        cell: String,
    },
    /// Another node's lease, to judge whether the owner it names is live.
    ReadLease {
        op: OpId,
        node: String,
    },
    CasOwner {
        op: OpId,
        cell: String,
        guard: CasGuard,
        epoch: Epoch,
        /// The shutdown its process was in when it sent the claim.
        shutting_down: Option<&'static str>,
    },
    /// Publish the cell as unowned, keeping its epoch, if this node still
    /// owns it at `epoch`.
    ReleaseOwner {
        op: OpId,
        cell: String,
        epoch: Epoch,
    },
    /// `VerifyOwnership`: read the owner record and compare it.
    Verify {
        op: OpId,
        cell: String,
        epoch: Epoch,
    },
    /// The replicator uploads the epoch's whole database.
    Upload {
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    /// A paged activation's marker (`seed_continuation`): the epoch
    /// continues the chain it restored, and the cell serves only once the
    /// marker is in the bucket.
    PutMarker {
        op: OpId,
        cell: String,
        epoch: Epoch,
        content: Vec<WriteId>,
    },
    /// A restore reads the newest chain below its epoch, after folding the
    /// cell's tail into the bucket when an epoch here left one
    /// (`fold_cell`).
    ReadChain {
        op: OpId,
        cell: String,
        epoch: Epoch,
        fold: bool,
    },
    /// One flush of a session's bundle loop, at a log epoch.
    Bundle {
        session: String,
        log_epoch: u64,
        rows: Vec<Row>,
    },
    /// A compaction round: fold `session`'s bundle rows of a cell epoch
    /// into its per-cell prefix (`compact_cell`, `BundleOverlayClient`).
    Compact {
        session: String,
        cell: String,
        epoch: Epoch,
    },
    /// Bundle GC deletes bundles of `session` it found covered
    /// (`delete_many`).
    DeleteBundles {
        session: String,
        seqs: Vec<u64>,
    },
    /// A maintain pass reads the peers' leases (`read_capacity_peers`).
    ReadPeers,
    /// A degraded shipper's lease write that reopens its log at the epoch
    /// after `from` with `ensemble`.
    Reopen {
        session: String,
        from: u64,
        ensemble: Vec<usize>,
    },
    /// One step of an owner's epoch GC pass.
    Gc(GcReq),
    /// One step of a wake loop.
    Waker(WakerReq),
    /// The drain token, or the readiness gate's read of it.
    Drain(DrainReq),
    /// Publish the wake entry of the cell's alarm. With `retire`, then
    /// retire the entries it makes obsolete, as `maintain_wake_entry` does.
    Wake {
        cell: String,
        alarm: AlarmSnapshot,
        report: bool,
        retire: bool,
        /// The reply that waits for this write (the arm gate).
        gate: Option<Waiter>,
    },
}

impl World {
    /// Send a request to the bucket. It applies after one drawn latency and
    /// its answer arrives after another, so other events can happen in
    /// between, on this node and on others.
    pub(super) fn store_call(&mut self, node: usize, req: StoreReq) {
        self.store_call_within(node, req, STORE_TIMEOUT_MS);
    }

    /// Draw what happens to one call, then send it.
    ///
    /// - It may fail definitely: it did not apply, and the caller knows.
    /// - Its request may be lost: it did not apply, and the caller times out.
    /// - Its response may be lost: it applied, and the caller times out.
    /// - It may be slow: if both hops take longer than the timeout, the
    ///   caller gives up first, and the request still applies, late.
    /// - A partitioned node's calls hang or are refused, and never apply.
    ///
    /// A failed call's answer is scheduled here, before the request moves.
    /// A late or lost answer to a call that did apply is simply never sent.
    ///
    /// This is deliberately harsher than production. Its bucket client
    /// retries a failed read internally (two retries within 30 s) before
    /// the core hears anything; here the core sees every failure of the
    /// one attempt, so its own retry paths run far more often.
    pub(super) fn store_call_within(&mut self, node: usize, req: StoreReq, timeout: u64) {
        let inc = self.machines[node].inc;
        let (request_ms, answer_ms) = match self.fate(node, timeout) {
            Fate::Answered {
                request_ms,
                response_ms,
            } => (request_ms, Some(response_ms)),
            Fate::Failed {
                failure,
                at,
                applies_after,
            } => {
                self.stats.store_failures += 1;
                let work = self.store_failed(node, &req, failure);
                self.work(node, at.max(1), work);
                match applies_after {
                    Some(request_ms) => (request_ms, None),
                    None => return,
                }
            }
        };
        let item = Item::StoreApply {
            node,
            inc,
            req,
            answer_ms,
        };
        self.after(request_ms, item);
    }

    /// An upload of `content` into a cell epoch's prefix lands, written by
    /// process `writer`. Every write into a prefix goes through here, so
    /// the oracle sees each one: who wrote it, and how far the prefix grew.
    pub(super) fn land_upload(
        &mut self,
        writer: &str,
        cell: &str,
        epoch: Epoch,
        content: &[WriteId],
    ) {
        self.checker.upload_applied(self.t, writer, cell, epoch);
        let at_ms = WALL_EPOCH_MS + self.t;
        self.store.put(cell, epoch, content, at_ms);
        self.checker.bucket_holds(self.t, cell, epoch, content);
        self.judge_discoverable(cell);
    }

    /// Draw the fate of one call: its latencies, then the faults that
    /// override them. A partition overrides everything: the call never
    /// gets out.
    pub(super) fn fate(&mut self, node: usize, timeout: u64) -> Fate {
        let mut request_ms = self.latency();
        let mut response_ms = self.latency();
        // The failure the caller hears, and whether the call applied anyway.
        let mut lost = None;
        if self.t < self.config.chaos_ms {
            let f = &self.config.faults;
            if self.net_rng.chance(f.store_spike_probability) {
                request_ms += self.net_rng.range(0, f.store_spike_ms);
            }
            if self.net_rng.chance(f.store_spike_probability) {
                response_ms += self.net_rng.range(0, f.store_spike_ms);
            }
            lost = if self.net_rng.chance(f.store_definite) {
                Some((Failure::Definite, false))
            } else if self.net_rng.chance(f.store_lost_request) {
                Some((Failure::Ambiguous, false))
            } else if self.net_rng.chance(f.store_lost_response) {
                Some((Failure::Ambiguous, true))
            } else {
                None
            };
        }
        let lost = match self.machines[node].partition {
            Some((_, Partition::Refuse)) => Some((Failure::Definite, false)),
            Some((_, Partition::Hang)) => Some((Failure::Ambiguous, false)),
            None => lost,
        };
        // Too slow: the caller gives up, and the request still applies, late.
        let slow = request_ms + response_ms > timeout;
        let lost = lost.or(slow.then_some((Failure::Ambiguous, true)));
        match lost {
            None => Fate::Answered {
                request_ms,
                response_ms,
            },
            Some((failure, applies)) => Fate::Failed {
                failure,
                at: match failure {
                    Failure::Definite => request_ms.min(timeout),
                    Failure::Ambiguous => timeout,
                },
                applies_after: applies.then_some(request_ms),
            },
        }
    }

    /// What the caller hears when a call fails.
    fn store_failed(&self, node: usize, req: &StoreReq, failure: Failure) -> Work {
        let core = Work::Deliver;
        match req {
            StoreReq::ReadSelfLease { op } => core(Event::SelfNodeLeaseRead {
                op: *op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Err(failure),
            }),
            StoreReq::CasLease { op, record, .. } => core(Event::NodeLeaseCasCompleted {
                op: *op,
                now_mono_ms: 0,
                result: Err(failure),
                stamped_log_state: record.log_state,
            }),
            StoreReq::ReadCapacityPeers { op } => core(Event::CapacityPeersRead {
                op: *op,
                now_ms: self.wall(node),
                result: Err(failure),
            }),
            StoreReq::ReadOwner { op, .. } => core(Event::OwnerRead {
                op: *op,
                now_ms: 0,
                result: Err(failure),
            }),
            StoreReq::ReadLease { op, .. } => core(Event::NodeLeaseRead {
                op: *op,
                now_ms: 0,
                result: Err(failure),
            }),
            StoreReq::CasOwner { op, .. } => core(Event::OwnerCasCompleted {
                op: *op,
                result: Err(failure),
            }),
            StoreReq::ReleaseOwner { op, .. } => core(Event::OwnerReleased {
                op: *op,
                result: Err(failure),
            }),
            StoreReq::Verify { op, .. } => core(Event::OwnershipVerified {
                op: *op,
                result: Err(failure),
            }),
            StoreReq::Upload { cell, epoch, .. } => Work::Replica(ReplicaWork::UploadFailed {
                cell: cell.clone(),
                epoch: *epoch,
            }),
            // Production reports every failed restore as definite
            // (`Effect::Restore` in `actor.rs`): nothing was opened.
            StoreReq::ReadChain { op, .. } | StoreReq::PutMarker { op, .. } => {
                core(Event::RestoreCompleted {
                    op: *op,
                    result: Err(Failure::Definite),
                })
            }
            // The arm gate turns its response into a failure.
            StoreReq::Wake {
                gate: Some(waiter), ..
            } => Work::Alarm(AlarmWork::GateFailed { waiter: *waiter }),
            // Logged and left for a later scan or reconcile.
            StoreReq::Wake { gate: None, .. } => Work::Nothing,
            // The pass ends; the next tick starts another.
            StoreReq::Gc(_) => Work::Nothing,
            StoreReq::Bundle { .. } => Work::Log(LogWork::Bundled(Flushed::Failed)),
            StoreReq::Reopen { .. } => Work::Log(LogWork::Reopened(None)),
            StoreReq::Compact { cell, epoch, .. } => Work::Replica(ReplicaWork::Compacted {
                cell: cell.clone(),
                epoch: *epoch,
                len: None,
            }),
            // The next tick lists them again.
            StoreReq::DeleteBundles { .. } => Work::Nothing,
            StoreReq::ReadPeers => Work::Log(LogWork::PeersRead(None)),
            StoreReq::Waker(req) => World::waker_failed(req),
            StoreReq::Drain(req) => World::drain_failed(req),
        }
    }

    pub(in crate::world) fn latency(&mut self) -> u64 {
        let (lo, hi) = self.config.store_latency_ms;
        self.net_rng.range(lo, hi)
    }

    /// The request reaches the bucket: apply it now, at this one instant,
    /// and send the answer back.
    pub(super) fn store_apply(
        &mut self,
        node: usize,
        inc: u64,
        req: StoreReq,
        answer_ms: Option<u64>,
    ) {
        let name = self.machines[node].name.clone();
        let sender = self.who_at(node, inc);
        let core = Work::Deliver;
        let reply = match req {
            StoreReq::ReadSelfLease { op } => core(Event::SelfNodeLeaseRead {
                op,
                now_ms: 0,
                now_mono_ms: 0,
                result: Ok(self.store.read_lease(&name)),
            }),
            StoreReq::CasLease {
                op,
                guard,
                record,
                load,
                acks,
            } => {
                let result = self.store.cas_lease(&guard, &record, load);
                let opened = matches!(result, LeaseCasOutcome::Applied { .. })
                    && record.log_state == Some(LogState::Open);
                let acks = acks.filter(|_| opened);
                if let Some(log_epoch) = acks {
                    self.apply_activation(&record.generation, log_epoch);
                }
                let event = Event::NodeLeaseCasCompleted {
                    op,
                    now_mono_ms: 0,
                    result: Ok(result),
                    stamped_log_state: record.log_state,
                };
                match opened {
                    true => Work::Log(LogWork::LeaseApplied { event, acks }),
                    false => core(event),
                }
            }
            StoreReq::ReadOwner { op, cell } => core(Event::OwnerRead {
                op,
                now_ms: 0,
                result: Ok(self.store.read_owner(&cell)),
            }),
            StoreReq::ReadCapacityPeers { op } => core(Event::CapacityPeersRead {
                op,
                // The core judges the sample at the instant it was taken.
                now_ms: self.wall(node),
                result: Ok(self.store.capacity_peers()),
            }),
            StoreReq::ReadLease { op, node: owner } => core(Event::NodeLeaseRead {
                op,
                now_ms: 0,
                result: Ok(self.store.read_lease(&owner)),
            }),
            StoreReq::CasOwner {
                op,
                cell,
                guard,
                epoch,
                shutting_down,
            } => {
                let prior = self
                    .store
                    .owner(&cell)
                    .and_then(|(owner, _)| owner.map(str::to_string));
                let outcome = self.store.cas_owner(&cell, &guard, epoch, &name);
                // The oracle believes the bucket, not the answer: a claim
                // is the epoch the record names this node at afterwards.
                let installed = match self.store.owner(&cell) {
                    Some((Some(owner), epoch)) if owner == name => Some(epoch),
                    _ => None,
                };
                if let (CasOutcome::Applied, Some(epoch)) = (outcome, installed) {
                    self.checker.claim_applied(self.t, &cell, epoch, &name);
                    if let Some(phase) = shutting_down {
                        self.judge_claim(&sender, &name, &cell, epoch, prior, phase);
                    }
                }
                core(Event::OwnerCasCompleted {
                    op,
                    result: Ok(outcome),
                })
            }
            StoreReq::ReleaseOwner { op, cell, epoch } => core(Event::OwnerReleased {
                op,
                result: Ok(self.store.release_owner(&cell, epoch, &name)),
            }),
            StoreReq::Verify { op, cell, epoch } => {
                let ours = self
                    .store
                    .read_owner(&cell)
                    .is_some_and(|r| r.node.as_ref() == Some(&name) && r.epoch == epoch);
                if ours {
                    self.checker.ownership_read(self.t, &sender, &cell, epoch);
                }
                core(Event::OwnershipVerified {
                    op,
                    result: if ours { Ok(()) } else { Err(Failure::Definite) },
                })
            }
            StoreReq::Upload {
                cell,
                epoch,
                content,
            } => {
                self.land_upload(&sender, &cell, epoch, &content);
                Work::Replica(ReplicaWork::Uploaded {
                    cell,
                    epoch,
                    len: content.len(),
                })
            }
            StoreReq::PutMarker {
                op,
                cell,
                epoch,
                content,
            } => {
                self.checker.upload_applied(self.t, &sender, &cell, epoch);
                self.store
                    .put_marker(&cell, epoch, &content, WALL_EPOCH_MS + self.t);
                self.checker.bucket_holds(self.t, &cell, epoch, &content);
                self.judge_discoverable(&cell);
                Work::Replica(ReplicaWork::Marked {
                    op,
                    cell,
                    epoch,
                    content,
                })
            }
            StoreReq::ReadChain {
                op,
                cell,
                epoch,
                fold,
            } => match fold.then(|| self.fold_tail(node, inc, &cell)) {
                Some(Err(failure)) => core(Event::RestoreCompleted {
                    op,
                    result: Err(failure),
                }),
                // `epoch_chain` lists every epoch and refuses a restore into
                // an epoch some later writer already uploaded at: this
                // claim was superseded before its restore got here.
                _ => Work::Replica(ReplicaWork::ChainRead {
                    op,
                    chain: match self.store.used_since(&cell, epoch) {
                        true => Err(Failure::Definite),
                        false => self.store.read_chain(&cell, epoch).map_err(|NoBase| {
                            // The restore fails. Production builds such a
                            // chain, then fails the activation when its
                            // restore plan finds no first position
                            // (`calc_restore_plan`).
                            self.checker.unrestorable(self.t, &name, &cell, epoch);
                            Failure::Definite
                        }),
                    },
                    cell,
                    epoch,
                }),
            },
            StoreReq::Gc(req) => self.gc_apply(node, req),
            StoreReq::Bundle {
                session,
                log_epoch,
                rows,
            } => Work::Log(LogWork::Bundled(
                self.apply_bundle(&name, &session, log_epoch, rows),
            )),
            StoreReq::Compact {
                session,
                cell,
                epoch,
            } => {
                let len = self.apply_compaction(&sender, &session, &cell, epoch);
                Work::Replica(ReplicaWork::Compacted {
                    cell,
                    epoch,
                    len: Some(len),
                })
            }
            StoreReq::DeleteBundles { session, seqs } => {
                self.delete_bundles(&session, &seqs);
                Work::Nothing
            }
            StoreReq::ReadPeers => {
                let peers = (self.store.capacity_peers().into_iter())
                    .filter_map(|peer| {
                        let machine = self.machines.iter().position(|m| m.name == peer.node)?;
                        Some((machine, peer.expires_ms))
                    })
                    .collect();
                Work::Log(LogWork::PeersRead(Some(peers)))
            }
            StoreReq::Reopen {
                session,
                from,
                ensemble,
            } => Work::Log(LogWork::Reopened(
                self.apply_reopen(&name, &session, from, ensemble),
            )),
            StoreReq::Waker(req) => self.waker_apply(node, req),
            StoreReq::Drain(req) => self.drain_apply(req),
            StoreReq::Wake {
                cell,
                alarm,
                report,
                retire,
                gate,
            } => {
                self.store.publish_wake(&cell, alarm);
                let retirement = retire
                    .then(|| self.retirement_proven(node, inc, &cell, alarm))
                    .flatten();
                if let Some(retirement) = retirement {
                    self.store.retire_wake(&cell, retirement);
                }
                self.judge_discoverable(&cell);
                Work::Alarm(AlarmWork::Maintained {
                    cell,
                    alarm,
                    report,
                    gate,
                    retired: retirement.is_some(),
                })
            }
        };
        // The answer goes back to the process that asked. If it has died
        // since, or the answer is lost, nobody hears it, but what it answers
        // has still happened.
        if let Some(delay) = answer_ms {
            let work = reply;
            self.after(delay, Item::Proc { node, inc, work });
        }
    }
}
