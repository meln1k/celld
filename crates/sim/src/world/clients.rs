//! The workload: clients, the ingress that forwards their requests to a
//! cell's owner, and the scripted handler that stands in for a cell's code.
//!
//! V8 is not simulated. A handler may commit one write, runs for a while,
//! and answers through the output gate.
//!
//! A client sends each request to a random node, its ingress. When the
//! ingress's core routes the request to another node, the ingress forwards
//! it there, as `dispatch_do_call` in `crates/celld/main.rs` does. A
//! forwarded request is never forwarded again: the peer side,
//! `dispatch_forwarded_fetch`, answers `409 stale route` when it does not
//! own the cell or has no room for it.
//!
//! A forward can come back without running ([`Bounce`]). What the ingress
//! does then follows its retry loop in `dispatch_do_call` and
//! `RemoteRouteRetry`, which spends the shared budget of
//! `celld_logic::routing::Dispatcher`.

use celld_logic::routing::{Attempt, Dispatcher};
use std::collections::VecDeque;

use celld_logic::{Channel, Epoch, Event, RequestError, RequestId, Route, WebSocketId};

use super::faults::Partition;
use super::{alarms, Item, Pending, Time, Work, World, OPERATION_DEADLINE_MS};
use crate::checks::Reveal;
use crate::WriteId;

/// When clients start sending. Nodes need a few round trips for their
/// leases first; a request before that is refused, which proves nothing.
const CLIENTS_START_MS: u64 = 200;
/// When clients stop, so that requests in flight can finish before the end.
pub(super) const CLIENTS_STOP_BEFORE_END_MS: u64 = 2_000;
/// How long the ingress paces a retry after a capacity candidate refused.
const CAPACITY_REFUSED_WAIT_MS: u64 = 25;
/// The longest a client that gives up waits first.
const MAX_PATIENCE_MS: u64 = 500;
/// How long a forward waits to connect before giving up
/// (`PEER_CONNECT_TIMEOUT` in `machine.rs`).
const PEER_CONNECT_TIMEOUT_MS: u64 = 3_000;

/// One client request, from the client's side, while the client still
/// waits for it. Its answer removes it from `World::ops`, so work for a
/// request that is no longer there finds nobody waiting.
pub(super) struct Op {
    cell: String,
    write: bool,
    /// The node the client sent it to.
    ingress: usize,
    retry: RouteRetry,
    /// The liveness goal this request is an attempt at.
    goal: Option<usize>,
    /// When the client sent it.
    sent_at: Time,
    /// The socket it was sent on, which lives on its ingress.
    socket: Option<WebSocketId>,
    /// Its handler upgrades it to a WebSocket.
    upgrade: bool,
}

/// Why a forward came back without running.
#[derive(Clone, Copy, Debug)]
pub(super) enum Bounce {
    /// The peer answered `409 stale route`: it does not own the cell, or,
    /// asked to take an unowned cell, has no room.
    Stale,
    /// No process was listening: the connection was never established.
    Unreachable,
}

/// How a request reached the node it is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Via {
    /// From its client, through the load balancer.
    Client,
    /// Forwarded by the ingress to the cell's owner at this epoch.
    Owner(Epoch),
    /// Forwarded by the ingress to a peer with room, to take an unowned
    /// cell. The peer must refuse at once if it has none.
    Capacity,
}

impl Via {
    /// The route a forward follows. Epoch zero names a capacity candidate,
    /// not an owner, as it does in the core's `Route::Remote`.
    fn forward(epoch: Epoch) -> Self {
        match epoch {
            0 => Via::Capacity,
            epoch => Via::Owner(epoch),
        }
    }
}

/// The ingress's retry state for one request: `RemoteRouteRetry` in
/// `crates/celld/main.rs`. It keeps the shared one-retry budget, but does
/// not spend that retry on an owner a peer already rejected. Instead it
/// waits, with backoff, for the route to change.
struct RouteRetry {
    dispatcher: Dispatcher,
    /// The `(node, epoch)` route the budget belongs to.
    generation: Option<(String, Epoch)>,
    blocked: bool,
    deadline: Time,
    next_wait_ms: u64,
}

impl RouteRetry {
    const INITIAL_WAIT_MS: u64 = 10;
    const MAX_WAIT_MS: u64 = 250;

    fn new(now: Time) -> Self {
        Self {
            dispatcher: Dispatcher::default(),
            generation: None,
            blocked: false,
            deadline: now + OPERATION_DEADLINE_MS,
            next_wait_ms: Self::INITIAL_WAIT_MS,
        }
    }

    /// True when this route is already known not to be usable. A new route
    /// gets a fresh budget.
    fn observe(&mut self, node: &str, epoch: Epoch) -> bool {
        let changed = self
            .generation
            .as_ref()
            .is_none_or(|(n, e)| n != node || *e != epoch);
        if changed {
            self.generation = Some((node.to_string(), epoch));
            self.blocked = false;
            self.dispatcher = Dispatcher::default();
            self.next_wait_ms = Self::INITIAL_WAIT_MS;
        }
        self.blocked
    }

    /// A forward on the current route came back without running: how long
    /// to wait before resolving again, or `None` to give up. Production's
    /// `RemoteRouteRetry::failed` and `wait`, plus the capacity refusal
    /// that `dispatch_do_call` handles before them.
    fn failed(&mut self, why: Bounce, now: Time) -> Option<u64> {
        let (_, epoch) = self.generation.as_ref().expect("a route was tried");
        match (why, Via::forward(*epoch)) {
            // A capacity candidate's refusal spends no budget and has no
            // deadline: invalidate, pace, retry.
            (Bounce::Stale, Via::Capacity) => Some(CAPACITY_REFUSED_WAIT_MS),
            // The owner said it is not the owner. Spend the one retry, and
            // do not send to that same route again until it changes.
            (Bounce::Stale, _) => {
                if !self.dispatcher.redispatch(Attempt::NotOwner) {
                    return None;
                }
                self.blocked = true;
                self.wait(now)
            }
            // Nobody listened, so nothing ran: one retry, at once.
            (Bounce::Unreachable, _) => self
                .dispatcher
                .redispatch(Attempt::NeverConnected)
                .then_some(0),
        }
    }

    /// How long to wait before resolving again, or `None` once the
    /// deadline has passed.
    fn wait(&mut self, now: Time) -> Option<u64> {
        let remaining = self.deadline.saturating_sub(now);
        if remaining == 0 {
            return None;
        }
        let wait = remaining.min(self.next_wait_ms);
        self.next_wait_ms = (self.next_wait_ms * 2).min(Self::MAX_WAIT_MS);
        Some(wait)
    }
}

/// One delivery of a client request to one node's core.
pub(super) struct Req {
    op: u64,
    /// The cell and whether the handler writes: a copy of the client's, so
    /// the handler runs on even after the client stopped waiting.
    pub(super) cell: String,
    write: bool,
    upgrade: bool,
    pub(super) sent_at: Time,
    /// How the request got here. A forwarded request is never forwarded on.
    via: Via,
    /// The epoch the request ran at, once routed locally.
    pub(super) epoch: Option<Epoch>,
    /// The handler has answered: its activity is over, and its response
    /// waits at the output gate.
    answered: bool,
    /// The write the handler committed and its position.
    pub(super) committed: Option<(WriteId, u64)>,
    /// The handler's write also sets the cell's alarm this far ahead, or
    /// clears it.
    alarm: Option<Option<u64>>,
    /// The cell's database when the handler answered: everything its
    /// response can reveal.
    seen: Vec<WriteId>,
    /// The arm gate: the wake entry write for the alarm the handler set,
    /// which its response waits on, as `send_answer_after_arm_gates` does.
    arm_gate: Option<ArmGate>,
    /// A response the output gate released while the arm gate was pending.
    held: bool,
}

/// Where a response's arm gate has got to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ArmGate {
    Pending,
    Landed,
    /// The wake entry write failed, so the response fails too.
    Failed,
}

/// The client side's work for one process: its handlers, and the ingress
/// resolving a route again.
pub(super) enum ClientWork {
    /// A handler commits its write.
    Commit { request: RequestId },
    /// A handler calls out to a third party.
    Effect { request: RequestId },
    /// A handler returns its response.
    End { request: RequestId },
    /// The ingress resolves a forwarded request's route again.
    Redispatch { op: u64 },
}

impl World {
    pub(super) fn client_work(&mut self, node: usize, work: ClientWork) {
        match work {
            ClientWork::Commit { request } => self.handler_commit(node, request),
            ClientWork::Effect { request } => self.handler_effect(node, request),
            ClientWork::End { request } => self.handler_end(node, request),
            ClientWork::Redispatch { op } => self.redispatch(op),
        }
    }

    pub(super) fn start_clients(&mut self) {
        self.after(CLIENTS_START_MS, Item::ClientNew);
    }

    /// A client sends a request for a random cell to a random node.
    pub(super) fn client_new(&mut self) {
        let cell = format!("c{}", self.work_rng.below(self.config.cells as u64));
        let ingress = self.pick_ingress();
        if self.work_rng.chance(self.config.worker_probability) {
            // A top-level Worker request, for no cell.
            if let Some(node) = ingress {
                let delay = self.net_rng.range(1, 3);
                self.work(node, delay, Work::WorkerRequest);
            }
        } else {
            let write = self.work_rng.chance(self.config.write_probability);
            self.send(cell, write, ingress, None);
        }

        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        if self.t + gap + CLIENTS_STOP_BEFORE_END_MS < self.config.duration_ms {
            self.after(gap, Item::ClientNew);
        }
    }

    /// The load balancer in front of the fleet only picks nodes that are
    /// ready: up, holding their node lease, and not shutting down, as the
    /// health endpoint reports.
    pub(super) fn pick_ingress(&mut self) -> Option<usize> {
        let up: Vec<usize> = (0..self.machines.len())
            .filter(|&node| {
                let m = &self.machines[node];
                m.up() && m.proc().shutdown.is_none() && m.proc().state.node_authoritative()
            })
            .collect();
        self.work_rng.pick(&up).copied()
    }

    /// Send one request to `ingress`. With no ingress it fails at once. A
    /// request for a liveness goal reports to that goal instead of to the
    /// workload's counts.
    pub(super) fn send(
        &mut self,
        cell: String,
        write: bool,
        ingress: Option<usize>,
        goal: Option<usize>,
    ) {
        if goal.is_none() {
            self.stats.requests += 1;
        }
        let Some(ingress) = ingress else {
            return self.answer(goal, false);
        };
        let upgrade = goal.is_none() && self.work_rng.chance(self.config.websocket_probability);
        self.open_op(cell, write, ingress, goal, None, upgrade);
    }

    /// A client sends a request on its WebSocket. It reaches the socket's
    /// node, which acts as its ingress.
    pub(super) fn send_on_socket(
        &mut self,
        cell: String,
        write: bool,
        node: usize,
        websocket: WebSocketId,
    ) {
        self.stats.requests += 1;
        self.open_op(cell, write, node, None, Some(websocket), false);
    }

    fn open_op(
        &mut self,
        cell: String,
        write: bool,
        ingress: usize,
        goal: Option<usize>,
        socket: Option<WebSocketId>,
        upgrade: bool,
    ) {
        let op = self.next_op;
        self.next_op += 1;
        let entry = Op {
            cell,
            write,
            ingress,
            retry: RouteRetry::new(self.t),
            goal,
            sent_at: self.t,
            socket,
            upgrade,
        };
        self.ops.insert(op, entry);
        if goal.is_none() && self.work_rng.chance(self.config.cancel_probability) {
            let patience = self.work_rng.range(1, MAX_PATIENCE_MS);
            self.after(patience, Item::GiveUp { op });
        }
        let delay = self.net_rng.range(1, 3);
        let via = Via::Client;
        self.after(
            delay,
            Item::Arrive {
                node: ingress,
                op,
                via,
            },
        );
    }

    /// The request reaches a node: hand it to the core.
    pub(super) fn arrive(&mut self, node: usize, op: u64, via: Via) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        let (ingress, cell, write, sent_at) = (
            entry.ingress,
            entry.cell.clone(),
            entry.write,
            entry.sent_at,
        );
        let (socket, upgrade) = (entry.socket, entry.upgrade);
        let paused = self.machines[node]
            .proc
            .as_ref()
            .and_then(|p| p.paused_until);
        if let Some(until) = paused {
            // The connection is accepted, but nothing reads it yet.
            self.after(until - self.t, Item::Arrive { node, op, via });
            return;
        }
        // A forward needs the network on both ends; a client reaches its
        // ingress through the load balancer either way.
        let partitions = [node, ingress].map(|n| self.machines[n].partition.map(|(_, mode)| mode));
        if via != Via::Client && partitions.iter().any(Option::is_some) {
            // A refused connection fails at once; a hung one only when the
            // connect timeout gives up. Either way it never connected.
            let delay = if partitions.contains(&Some(Partition::Hang)) {
                PEER_CONNECT_TIMEOUT_MS
            } else {
                self.net_rng.range(1, 5)
            };
            return self.bounce_after(op, Bounce::Unreachable, delay);
        }
        if !self.machines[node].up() {
            match via {
                // The client's own connection is refused.
                Via::Client => self.settle(op, false),
                _ => self.bounce(op, Bounce::Unreachable),
            }
            return;
        }
        let shutdown = self.machines[node].proc().shutdown.is_some();
        if (via == Via::Client && socket.is_none() && shutdown) || self.preserving(node) {
            // A shutting-down node admits no new public request
            // (`admit_public`), though a socket it holds still runs its last
            // messages; a preserving actor refuses every request.
            return self.settle(op, false);
        }
        let req = Req {
            op,
            cell: cell.clone(),
            write,
            upgrade,
            sent_at,
            via,
            epoch: None,
            answered: false,
            committed: None,
            alarm: None,
            seen: Vec::new(),
            arm_gate: None,
            held: false,
        };
        let request = self.pend(node, Pending::Client(req));
        let event = if via == Via::Capacity {
            Event::CapacityRequestAt {
                request,
                cell,
                now_mono_ms: 0,
            }
        } else if let (Via::Client, Some(websocket)) = (via, socket) {
            Event::WebSocketRequestAt {
                request,
                cell,
                websocket,
                now_ms: 0,
                now_mono_ms: 0,
            }
        } else {
            Event::RequestAt {
                request,
                cell,
                now_mono_ms: 0,
            }
        };
        self.drive(node, event);
    }

    /// The core routed a request.
    pub(super) fn complete(
        &mut self,
        node: usize,
        request: RequestId,
        result: Result<Route, RequestError>,
        immediate: &mut VecDeque<Event>,
    ) {
        let Some(req) = self.machines[node].proc().client(request) else {
            // Its caller is gone. A local route nobody takes still holds the
            // cell active, so production finishes the activity at once.
            if result == Ok(Route::Local) {
                immediate.push_back(Event::ActivityFinished { request });
            }
            return;
        };
        let (op, via) = (req.op, req.via);
        match result {
            Ok(Route::Local) => self.run_handler(node, request),
            Ok(Route::Remote {
                node: owner, epoch, ..
            }) => {
                self.machines[node].proc_mut().take_client(request);
                match via {
                    Via::Client => self.forward(op, owner, epoch),
                    // The ingress forwarded here, but this node routes the
                    // cell elsewhere: `409 stale route`.
                    _ => self.bounce(op, Bounce::Stale),
                }
            }
            Err(RequestError::CapacityExhausted) if via != Via::Client => {
                // The peer has no room: the same `409 stale route`.
                self.machines[node].proc_mut().take_client(request);
                self.bounce(op, Bounce::Stale);
            }
            Err(_) => {
                self.machines[node].proc_mut().take_client(request);
                self.settle(op, false);
            }
        }
    }

    /// The ingress sends the request on to the node its core named, unless
    /// a peer already said that route is stale.
    fn forward(&mut self, op: u64, owner: String, epoch: Epoch) {
        let t = self.t;
        let Some(Op { retry, .. }) = self.ops.get_mut(&op) else {
            return;
        };
        if retry.observe(&owner, epoch) {
            // Wait for the ingress's view of the owner to change.
            match retry.wait(t) {
                Some(wait) => self.redispatch_after(op, wait),
                None => self.settle(op, false),
            }
            return;
        }
        let target = self
            .machines
            .iter()
            .position(|m| m.name == owner)
            .expect("routes name fleet nodes");
        self.stats.forwarded += 1;
        let delay = self.net_rng.range(1, 5);
        self.after(
            delay,
            Item::Arrive {
                node: target,
                op,
                via: Via::forward(epoch),
            },
        );
    }

    /// The forward comes back to the ingress without having run.
    fn bounce(&mut self, op: u64, why: Bounce) {
        let delay = self.net_rng.range(1, 5);
        self.bounce_after(op, why, delay);
    }

    fn bounce_after(&mut self, op: u64, why: Bounce, delay: u64) {
        self.stats.bounced += 1;
        self.after(delay, Item::Bounced { op, why });
    }

    /// The ingress decides whether to try again, as `dispatch_do_call`
    /// does. Nothing ran on the peer, so trying again is safe; the question
    /// is only whether it can help.
    pub(super) fn bounced(&mut self, op: u64, why: Bounce) {
        let t = self.t;
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        match entry.retry.failed(why, t) {
            Some(wait) => self.redispatch_after(op, wait),
            None => self.settle(op, false),
        }
    }

    /// Resolve again after `wait`, on the ingress process: if it dies
    /// first, the request already failed with it.
    fn redispatch_after(&mut self, op: u64, wait: u64) {
        let ingress = self.ops[&op].ingress;
        self.work(ingress, wait, Work::Client(ClientWork::Redispatch { op }));
    }

    /// Retire the failed route in the ingress's core and resolve again.
    pub(super) fn redispatch(&mut self, op: u64) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        let (ingress, cell) = (entry.ingress, entry.cell.clone());
        let (node, epoch) = entry.retry.generation.clone().expect("a route was tried");
        self.drive(ingress, Event::InvalidateRemote { cell, node, epoch });
        self.arrive(ingress, op, Via::Client);
    }

    /// A process died. Every request it was running fails, and so does
    /// every request it was the ingress for: their clients' connections
    /// were to it.
    pub(super) fn fail_requests(
        &mut self,
        node: usize,
        requests: std::collections::BTreeMap<RequestId, Req>,
    ) {
        for req in requests.into_values() {
            self.settle(req.op, false);
        }
        let lost: Vec<u64> = self
            .ops
            .iter()
            .filter(|(_, entry)| entry.ingress == node)
            .map(|(op, _)| *op)
            .collect();
        for op in lost {
            self.settle(op, false);
            // Their forwards lose the connection back to the ingress.
            self.cancel_routes(op);
        }
    }

    /// A cell is being handed off: abort the work it listed, as
    /// `cancel_core_request` and `abort_activity` do. A handler that has
    /// not answered dies, its client hears a failure, and its activity
    /// guard reports it finished. A response already at the output gate is
    /// left for the core to release or refuse.
    pub(super) fn abort_requests(&mut self, node: usize, requests: Vec<RequestId>) {
        for request in requests {
            let p = self.machines[node].proc_mut();
            let Some(req) = p.client(request) else {
                continue;
            };
            if req.answered {
                continue;
            }
            let routed = req.epoch.is_some();
            let req = p.take_client(request).unwrap();
            self.settle(req.op, false);
            let delay = self.net_rng.range(0, 3);
            if routed {
                let cell = req.cell;
                self.work(node, delay, Work::ActivityDone { request, cell });
            } else {
                self.deliver(node, delay, Event::Cancel { request });
            }
        }
    }

    /// The client stops waiting. Nobody is told the request worked, or
    /// that it failed.
    pub(super) fn give_up(&mut self, op: u64) {
        if self.ops.remove(&op).is_some() {
            self.stats.cancelled += 1;
            self.cancel_routes(op);
        }
    }

    /// The connection a request came in on is gone. Wherever it still
    /// waits for a route, its route future is dropped, and its
    /// `RouteCancelGuard` tells the core (`Message::CancelRoute`). A request
    /// already running locally runs on: its handler answers nobody.
    fn cancel_routes(&mut self, op: u64) {
        for node in 0..self.machines.len() {
            let Some(p) = self.machines[node].proc.as_mut() else {
                continue;
            };
            let waiting: Vec<RequestId> = p
                .pending
                .iter()
                .filter_map(|(request, pending)| match pending {
                    Pending::Client(req) if req.op == op && req.epoch.is_none() => Some(*request),
                    _ => None,
                })
                .collect();
            for request in waiting {
                self.machines[node].proc_mut().take_client(request);
                let delay = self.net_rng.range(0, 3);
                self.deliver(node, delay, Event::Cancel { request });
            }
        }
    }

    /// The request runs here: maybe commit a write after a while, then
    /// answer.
    fn run_handler(&mut self, node: usize, request: RequestId) {
        let req = self.machines[node].proc().client(request).unwrap();
        let (op, cell, write) = (req.op, req.cell.clone(), req.write);
        let epoch = self.machines[node].proc().published.get(&cell).copied();
        let name = self.machines[node].name.clone();
        self.checker
            .routed_local(self.t, &name, &cell, epoch.is_some());
        let Some(epoch) = epoch else {
            self.machines[node].proc_mut().take_client(request);
            self.settle(op, false);
            return;
        };
        self.machines[node]
            .proc_mut()
            .client_mut(request)
            .unwrap()
            .epoch = Some(epoch);
        let commit = self.work_rng.range(0, 10);
        let end = commit + self.work_rng.range(1, 30);
        if write {
            // Liveness goals leave the alarm alone.
            if self.ops.get(&op).is_some_and(|o| o.goal.is_none()) {
                let alarm = self.draw_alarm();
                self.machines[node]
                    .proc_mut()
                    .client_mut(request)
                    .unwrap()
                    .alarm = alarm;
            }
            self.work(node, commit, Work::Client(ClientWork::Commit { request }));
        }
        if self.work_rng.chance(self.config.effect_probability) {
            let at = self.work_rng.range(commit, end);
            self.work(node, at, Work::Client(ClientWork::Effect { request }));
        }
        self.work(node, end, Work::Client(ClientWork::End { request }));
    }

    /// The handler's write goes into the open database, and the replicator
    /// starts uploading it.
    /// If the runtime was stopped under it, the database is closed and the
    /// handler fails instead.
    pub(super) fn handler_commit(&mut self, node: usize, request: RequestId) {
        let Some(req) = self.machines[node].proc().client(request) else {
            return;
        };
        let (cell, epoch, alarm) = (req.cell.clone(), req.epoch.unwrap(), req.alarm);
        match self.commit_write(node, &cell, epoch) {
            Some(committed) => {
                let req = self.machines[node].proc_mut().client_mut(request).unwrap();
                req.committed = Some(committed);
                if let Some(ahead_ms) = alarm {
                    // The write sets or clears the alarm, and the runtime's
                    // alarm observer reports the change. Setting it opens
                    // the arm gate.
                    if ahead_ms.is_some() {
                        let req = self.machines[node].proc_mut().client_mut(request).unwrap();
                        req.arm_gate = Some(ArmGate::Pending);
                    }
                    let gate = Some(alarms::Waiter::Request(request));
                    let alarm = self.alarm_written(node, &cell, epoch, committed.0, ahead_ms, gate);
                    let alarm = self.reported(alarm);
                    self.deliver(node, 0, alarms::observed(&cell, alarm, false));
                }
            }
            None => self.handler_failed(node, request),
        }
    }

    /// The handler threw: the request fails and its activity ends.
    fn handler_failed(&mut self, node: usize, request: RequestId) {
        let req = self.machines[node].proc_mut().take_client(request).unwrap();
        self.settle(req.op, false);
        self.activity_done(node, request, &req.cell);
    }

    /// The handler returns. Its response goes to the output gate, which
    /// holds it until every write it could reveal is durable; then the
    /// handler's activity is over.
    pub(super) fn handler_end(&mut self, node: usize, request: RequestId) {
        let Some(req) = self.machines[node].proc().client(request) else {
            return;
        };
        let cell = req.cell.clone();
        let wrote = req.committed.is_some();
        let Some(egress) = self.egress(node, &cell, req.epoch.unwrap(), wrote) else {
            return self.handler_failed(node, request);
        };
        let req = self.machines[node].proc_mut().client_mut(request).unwrap();
        req.seen = egress.seen;
        req.answered = true;
        if req.upgrade {
            // The handler accepts a WebSocket before it answers.
            self.open_socket(node, cell.clone());
        }
        self.drive(
            node,
            Event::OutputAt {
                request,
                channel: Channel::Response,
                position: egress.position,
                observed: egress.observed,
                epoch: None,
                now_mono_ms: 0,
            },
        );
        self.activity_done(node, request, &cell);
    }

    /// The gate released or refused the response: the client hears back.
    pub(super) fn release(
        &mut self,
        node: usize,
        request: RequestId,
        channel: Channel,
        result: Result<(), RequestError>,
    ) {
        if channel == Channel::Fetch {
            return self.gate_released(node, request, result);
        }
        assert_eq!(channel, Channel::Response, "handlers answer or fetch");
        let p = self.machines[node].proc_mut();
        if let Some(req) = p.client_mut(request) {
            if result.is_ok() && req.arm_gate == Some(ArmGate::Pending) {
                // The arm gate still holds it: it goes once the wake entry
                // lands.
                req.held = true;
                return;
            }
        }
        let Some(req) = p.take_client(request) else {
            return;
        };
        let arm_failed = req.arm_gate == Some(ArmGate::Failed);
        if result.is_err() || arm_failed || !self.ops.contains_key(&req.op) {
            // Refused, or released to a client that already gave up when
            // its connection died: either way nobody was told it worked.
            self.settle(req.op, false);
            return;
        }
        let epoch = req.epoch.expect("only a routed request answers");
        if req.committed.is_some() {
            self.stats.acked_writes += 1;
        }
        let who = self.who(node);
        let op = req.op;
        let reveal = Reveal {
            cell: req.cell,
            epoch,
            committed: req.committed,
            seen: req.seen,
            sent_at: req.sent_at,
            answer: true,
        };
        self.checker
            .response_released(self.t, &self.store, &who, &reveal);
        self.settle(op, true);
    }

    /// The wake entry write the response's arm gate waits on landed or
    /// failed. A response the output gate already released goes now.
    pub(super) fn response_arm_settled(&mut self, node: usize, request: RequestId, landed: bool) {
        let Some(req) = self.machines[node].proc_mut().client_mut(request) else {
            return;
        };
        req.arm_gate = Some(if landed {
            ArmGate::Landed
        } else {
            ArmGate::Failed
        });
        if std::mem::take(&mut req.held) {
            self.release(node, request, Channel::Response, Ok(()));
        }
    }

    /// The client hears its answer. Only the first one counts: a request
    /// whose ingress died has already failed, whatever happens to it later.
    fn settle(&mut self, op: u64, answered: bool) {
        if let Some(entry) = self.ops.remove(&op) {
            self.answer(entry.goal, answered);
        }
    }

    /// Count an answer, or report it to the liveness goal it was for.
    fn answer(&mut self, goal: Option<usize>, answered: bool) {
        match goal {
            Some(goal) => self.goal_settled(goal, answered),
            None if answered => self.stats.answered += 1,
            None => self.stats.failed += 1,
        }
    }
}
