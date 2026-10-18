//! The shell: each core effect, carried out as `crates/celld/actor.rs` does.

use super::*;

impl World {
    // -----------------------------------------------------------------------
    // Effects

    pub(super) fn execute(&mut self, id: ProcId, effect: Effect, immediate: &mut VecDeque<Event>) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                let mono = self.mono(id);
                let proc = self.procs.get_mut(&id).unwrap();
                let slot = Slot::of(&timer);
                let ordinal = proc.next_ordinal;
                proc.next_ordinal += 1;
                proc.timers
                    .insert(slot.clone(), (ordinal, timer, at_mono_ms));
                self.work(
                    id,
                    at_mono_ms.saturating_sub(mono),
                    Work::Timer { slot, ordinal },
                );
            }
            Effect::ReadSelfNodeLease { op } => {
                let node = self.node_name(id);
                self.store_call(id, StoreReq::ReadLease { node }, Reply::SelfLease(op));
            }
            Effect::CasNodeLease {
                op, guard, record, ..
            } => {
                let state = &self.procs[&id].state;
                let (resident, owned) = (state.residents().len(), state.owned_cells());
                // The lease fold: the body carries the session's log as it
                // stands now, whatever the core believed it was.
                let (log, seq) = self.stamp_log(id);
                let stamped = log.as_ref().map(|log| log.record.state);
                self.store_call(
                    id,
                    StoreReq::CasLease {
                        guard,
                        record,
                        log,
                        resident,
                        owned,
                    },
                    Reply::LeaseCas { op, stamped, seq },
                );
            }
            Effect::ReadLocalCells => {
                let node = self.procs[&id].node;
                let disk = &self.nodes[node].disk;
                let mut cells: Vec<LocalCell> = disk
                    .live
                    .keys()
                    .map(|(cell, epoch)| LocalCell {
                        id: cell.clone(),
                        epoch: *epoch,
                        live: true,
                    })
                    .chain(disk.evicted.keys().map(|(cell, epoch)| LocalCell {
                        id: cell.clone(),
                        epoch: *epoch,
                        live: false,
                    }))
                    .collect();
                cells.sort();
                let delay = self.net_rng.range(1, 5);
                self.work(
                    id,
                    delay,
                    Work::Deliver(Event::LocalCellsRead { result: Ok(cells) }),
                );
            }
            Effect::ReadOwner { op, cell } => {
                self.store_call(id, StoreReq::ReadOwner { cell }, Reply::OwnerRead(op))
            }
            Effect::ReadNodeLease { op, owner, .. } => self.store_call(
                id,
                StoreReq::ReadLease { node: owner },
                Reply::NodeLease(op),
            ),
            Effect::RecoverNodeLog { op, owner, .. } => {
                if self.config.fleet {
                    self.recover(id, owner, RecoverMode::Takeover(op));
                } else {
                    // Without the fleet posture no lease carries a log.
                    immediate.push_back(Event::NodeLogRecovered { op, result: Ok(()) });
                }
            }
            Effect::ReadCapacityPeers { op, .. } => {
                self.store_call(id, StoreReq::ReadPeers, Reply::Peers(op))
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                takeover,
            } => self.store_call(
                id,
                StoreReq::CasOwner {
                    cell,
                    guard,
                    epoch,
                    takeover,
                },
                Reply::OwnerCas(op),
            ),
            Effect::ReconcileWakeEntry { cell, alarm } => self.maintain_wake(id, cell, alarm),
            Effect::ReleaseOwner { op, cell, epoch } => self.store_call(
                id,
                StoreReq::ReleaseOwner { cell, epoch },
                Reply::OwnerReleased(op),
            ),
            Effect::AdoptReleased { op, .. } => {
                // As the in-memory adapter does: no successor acknowledges a
                // handoff, so released cells wait for their next request.
                immediate.push_back(Event::SuccessorAdopted {
                    op,
                    result: Err(Failure::Definite),
                });
            }
            Effect::CancelCellActivity {
                requests, alarm, ..
            } => {
                if let Some(op) = alarm {
                    self.abort_alarm(id, op);
                }
                let proc = self.procs.get_mut(&id).unwrap();
                let mut ended = Vec::new();
                for request in requests {
                    if let Some(ctx) = proc.requests.get_mut(&request) {
                        ctx.aborted = true;
                        if ctx.answer != Answer::Sent {
                            ended.push(request);
                        }
                    }
                }
                for request in ended {
                    self.work(id, 0, Work::HandlerEnd { request });
                }
            }
            Effect::Restore { op, cell, spec } => self.restore(id, op, cell, spec),
            Effect::StartRuntime { op, cell, epoch } => self.start_runtime(id, op, cell, epoch),
            Effect::Publish { op, cell, epoch } => {
                let others = self
                    .procs
                    .iter()
                    .any(|(other, proc)| *other != id && proc.published.contains_key(&cell));
                if others {
                    self.stats.concurrent_serving += 1;
                }
                self.procs
                    .get_mut(&id)
                    .unwrap()
                    .published
                    .insert(cell, epoch);
                immediate.push_back(Event::Published { op, result: Ok(()) });
            }
            Effect::EvictionAdmission { .. } | Effect::EvictionFinished { .. } => {}
            Effect::EnsureDurable {
                op, cell, epoch, ..
            } => self.await_durable(id, op, &cell, epoch, Proof::Everything, Failure::Definite),
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => {
                self.alarm_gate_opened(id, op, &cell, epoch, position);
                if self.config.mutation == Mutation::AckBeforeUpload {
                    self.work(
                        id,
                        1,
                        Work::Deliver(Event::DurableReached {
                            op,
                            result: Ok(position),
                            source: ProofSource::Bucket,
                        }),
                    );
                    return;
                }
                let proof = Proof::Output { position };
                self.await_durable(id, op, &cell, epoch, proof, Failure::Ambiguous);
            }
            Effect::VerifyOwnership { op, cell, epoch } => {
                if self.config.mutation == Mutation::SkipVerifyOwnership {
                    self.alarm_gate_verified(id, op, true);
                    immediate.push_back(Event::OwnershipVerified { op, result: Ok(()) });
                } else {
                    self.store_call(
                        id,
                        StoreReq::ReadOwner { cell },
                        Reply::Verify { op, epoch },
                    );
                }
            }
            Effect::Release {
                request,
                channel,
                result,
            } => self.release(id, request, channel, result.is_ok()),
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
                bounded,
            } => self.stop_runtime(id, op, cell, epoch, cause, bounded, immediate),
            Effect::AbandonEvictionStop { op, cell } => {
                let proc = self.procs.get_mut(&id).unwrap();
                let found = proc
                    .replicas
                    .iter_mut()
                    .find(|((name, _), replica)| {
                        *name == cell && replica.stop.as_ref().is_some_and(|stop| stop.op == op)
                    })
                    .map(|(_, replica)| replica);
                if let Some(replica) = found {
                    // The final sync has not finished, so no handoff snapshot
                    // is visible yet: the stop can still give the cell back.
                    replica.stop = None;
                    self.work(id, 1, Work::Deliver(Event::RuntimeStopCancelled { op }));
                }
            }
            Effect::FireAlarm {
                op,
                cell,
                epoch,
                scheduled_ms,
            } => self.fire_alarm(id, op, cell, epoch, scheduled_ms),
            Effect::Complete { request, result } => self.complete(id, request, result),
            Effect::CompleteWorker { .. } | Effect::CloseWebSocket { .. } => {}
            Effect::Halt { .. } => {
                self.procs.get_mut(&id).unwrap().halting = true;
            }
        }
    }

    pub(super) fn node_name(&self, id: ProcId) -> String {
        self.nodes[self.procs[&id].node].name.clone()
    }
}
