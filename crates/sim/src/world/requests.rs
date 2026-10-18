//! Requests on a process: routing, forwarding, the handler and its reply.

use super::*;

impl World {
    // -----------------------------------------------------------------------
    // Requests and handlers

    pub(super) fn complete(
        &mut self,
        id: ProcId,
        request: RequestId,
        result: Result<Route, celld_logic::RequestError>,
    ) {
        if self.handoff_routed(id, request, &result) {
            return;
        }
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        match result {
            Ok(Route::Local) => {
                self.stats.local_routes += 1;
                let epoch = proc.published.get(&ctx.cell).copied();
                let Some(epoch) = epoch else {
                    let cell = ctx.cell.clone();
                    self.checker
                        .on_local_route_without_runtime(self.t, id, request, &cell);
                    // Finish the request so the core is not left waiting.
                    self.work(id, 0, Work::HandlerEnd { request });
                    return;
                };
                ctx.epoch = Some(epoch);
                let write = ctx.write;
                let commit = self.work_rng.range(0, 10);
                let end = commit + self.work_rng.range(1, 30);
                if write {
                    self.work(id, commit, Work::HandlerCommit { request });
                }
                self.work(id, end, Work::HandlerEnd { request });
            }
            Ok(Route::Remote { node, epoch, .. }) => {
                self.stats.remote_routes += 1;
                let ctx = proc.requests.remove(&request).unwrap();
                // The cell lives elsewhere: its wake entry is no longer this
                // node's to track (`forget_wake_entry`).
                proc.wake.forget(&ctx.cell);
                match ctx.via {
                    None => self.forward(id, ctx, node, epoch),
                    Some(via) => {
                        let own = self.node_name(id);
                        self.reply_stale(ctx, own, via);
                    }
                }
            }
            Err(error) => {
                self.stats.route_errors += 1;
                let ctx = proc.requests.remove(&request).unwrap();
                if let (Some(via), celld_logic::RequestError::CapacityExhausted) = (ctx.via, error)
                {
                    // The peer answers with the same stale-route header.
                    let own = self.node_name(id);
                    self.reply_stale(ctx, own, via);
                } else {
                    self.attempt_failed(ctx.op, ctx.attempt);
                }
            }
        }
    }

    /// The peer's `409 stale route` travels back to the ingress.
    fn reply_stale(&mut self, ctx: ReqCtx, node: String, epoch: Epoch) {
        let Some(entry) = self.ops.get(&ctx.op) else {
            return;
        };
        let ingress = entry.dispatch.proc;
        let delay = self.net_rng.range(1, 5);
        self.work(
            ingress,
            delay,
            Work::StaleRoute {
                op: ctx.op,
                attempt: ctx.attempt,
                cell: ctx.cell,
                node,
                epoch,
            },
        );
    }

    pub(super) fn stale_route(
        &mut self,
        id: ProcId,
        op: u64,
        attempt: u32,
        cell: String,
        node: String,
        epoch: Epoch,
    ) {
        let t = self.t;
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        if t >= entry.dispatch.deadline {
            self.attempt_failed(op, attempt);
            return;
        }
        let wait = if epoch == 0 {
            // A refused candidate did not run the request and costs no
            // retry budget; invalidation excludes that load sample.
            25
        } else if !entry.dispatch.retried_not_owner {
            entry.dispatch.retried_not_owner = true;
            let wait = entry.dispatch.wait_ms.max(10);
            entry.dispatch.wait_ms = (wait * 2).min(250);
            wait
        } else {
            self.attempt_failed(op, attempt);
            return;
        };
        self.drive(id, Event::InvalidateRemote { cell, node, epoch });
        self.work(id, wait, Work::Redispatch { op, attempt });
    }

    pub(super) fn redispatch(&mut self, id: ProcId, op: u64, attempt: u32) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        let request = self.next_request;
        self.next_request += 1;
        self.client_arrive(id, op, attempt, request, None);
    }

    fn forward(&mut self, from: ProcId, ctx: ReqCtx, node: String, epoch: Epoch) {
        let target = self.nodes.iter().position(|n| n.name == node);
        let from_node = self.procs[&from].node;
        let reachable = target.and_then(|index| {
            let blocked =
                self.nodes[index].partition.is_some() || self.nodes[from_node].partition.is_some();
            if blocked {
                None
            } else {
                self.nodes[index].proc
            }
        });
        match reachable {
            Some(proc) => {
                let request = self.next_request;
                self.next_request += 1;
                let delay = self.net_rng.range(1, 5);
                self.work(
                    proc,
                    delay,
                    Work::ClientArrive {
                        op: ctx.op,
                        attempt: ctx.attempt,
                        request,
                        epoch: Some(epoch),
                    },
                );
            }
            None => {
                // The connection was never established, so nothing ran:
                // one redispatch after invalidating the route.
                let Some(entry) = self.ops.get_mut(&ctx.op) else {
                    return;
                };
                if entry.done || entry.attempt != ctx.attempt {
                    return;
                }
                if entry.dispatch.retried_never_connected {
                    self.attempt_failed(ctx.op, ctx.attempt);
                    return;
                }
                entry.dispatch.retried_never_connected = true;
                let cell = ctx.cell.clone();
                self.drive(from, Event::InvalidateRemote { cell, node, epoch });
                let delay = self.net_rng.range(1, 20);
                self.work(
                    from,
                    delay,
                    Work::Redispatch {
                        op: ctx.op,
                        attempt: ctx.attempt,
                    },
                );
            }
        }
    }

    pub(super) fn handler_commit(&mut self, id: ProcId, request: RequestId) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        let Some(epoch) = ctx.epoch else {
            return;
        };
        if ctx.aborted || proc.published.get(&ctx.cell) != Some(&epoch) {
            return;
        }
        let (cell, intent) = (ctx.cell.clone(), ctx.alarm);
        let op = intent.map(|intent| match intent {
            AlarmIntent::Set { due } => {
                let at_ms = self.wall(id) as i64 + due as i64 - self.t as i64;
                AlarmOp::Set(at_ms)
            }
            AlarmIntent::Clear => AlarmOp::Clear,
        });
        let Some((write, position, alarm)) = self.commit_alarm_write(id, &cell, epoch, op) else {
            return;
        };
        let ctx = self
            .procs
            .get_mut(&id)
            .unwrap()
            .requests
            .get_mut(&request)
            .unwrap();
        ctx.committed = Some((write, position));
        if matches!(op, Some(AlarmOp::Set(_)))
            && self.launch_arm_gate(id, &cell, alarm, ArmWaiter::Request(request))
        {
            let ctx = self
                .procs
                .get_mut(&id)
                .unwrap()
                .requests
                .get_mut(&request)
                .unwrap();
            ctx.arm_gate = ArmGate::Pending;
        }
    }

    /// The arm gate's PUT resolved. A failed one fails the reply.
    pub(super) fn arm_gate_settled(&mut self, id: ProcId, request: RequestId, ok: bool) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        if ctx.arm_gate != ArmGate::Pending {
            return;
        }
        ctx.arm_gate = ArmGate::Done { ok };
        if ctx.answer == Answer::HeldByArmGate {
            self.handler_end(id, request);
        }
    }

    pub(super) fn handler_end(&mut self, id: ProcId, request: RequestId) {
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        if ctx.answer == Answer::Sent {
            return;
        }
        if ctx.arm_gate == ArmGate::Pending && !ctx.aborted {
            // The reply waits behind the wake-entry PUT.
            ctx.answer = Answer::HeldByArmGate;
            return;
        }
        ctx.answer = Answer::Sent;
        if ctx.aborted || ctx.epoch.is_none() {
            ctx.activity = Activity::Finished;
            ctx.replied = true;
            let (op, attempt, cell) = (ctx.op, ctx.attempt, ctx.cell.clone());
            self.finish_activity(id, request, &cell);
            self.attempt_failed(op, attempt);
            return;
        }
        let position = ctx.committed.map(|(_, position)| position);
        let observed = if position.is_some() {
            None
        } else {
            let epoch = ctx.epoch.unwrap();
            proc.replicas
                .get(&(ctx.cell.clone(), epoch))
                .filter(|replica| replica.content.len() > replica.start_len)
                .map(|replica| replica.content.len() as u64)
        };
        // Whether the activity ends before the reply is released, or after.
        let finish_first = self.work_rng.chance(0.5);
        ctx.activity = if finish_first {
            Activity::Finished
        } else {
            Activity::OnRelease
        };
        self.drive(
            id,
            Event::OutputAt {
                request,
                channel: Channel::Response,
                position,
                observed,
                epoch: None,
                now_mono_ms: 0,
            },
        );
        if finish_first {
            let cell = self.procs[&id].requests[&request].cell.clone();
            self.finish_activity(id, request, &cell);
        }
    }

    pub(super) fn release(&mut self, id: ProcId, request: RequestId, channel: Channel, ok: bool) {
        if channel != Channel::Response {
            return;
        }
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&request) else {
            return;
        };
        ctx.replied = true;
        let (op, attempt, cell, epoch, committed) = (
            ctx.op,
            ctx.attempt,
            ctx.cell.clone(),
            ctx.epoch,
            ctx.committed,
        );
        let gate_failed = ctx.arm_gate == ArmGate::Done { ok: false };
        if ctx.activity == Activity::OnRelease {
            let delay = self.work_rng.range(0, 5);
            self.work(id, delay, Work::ActivityFinished { request });
        }
        if !ok {
            self.stats.refused_outputs += 1;
            self.attempt_failed(op, attempt);
            return;
        }
        if gate_failed {
            // "wake-entry gate": the handler's reply became an error.
            self.attempt_failed(op, attempt);
            return;
        }
        if let (Some((write, position)), Some(epoch)) = (committed, epoch) {
            self.stats.acked_writes += 1;
            let (checker, view) = self.checks();
            checker.on_reply_acked(view, id, &cell, epoch, write, position);
        }
        self.attempt_succeeded(op, attempt);
    }
}
