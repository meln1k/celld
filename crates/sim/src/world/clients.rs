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
use celld_logic::{Channel, Epoch, Event, RequestError, RequestId, Route};

use super::{Item, Time, Work, World, OPERATION_DEADLINE_MS};
use crate::{Mutation, WriteId};

/// When clients start sending. Nodes need a few round trips for their
/// leases first; a request before that is refused, which proves nothing.
const CLIENTS_START_MS: u64 = 200;
/// When clients stop, so that requests in flight can finish before the end.
const CLIENTS_STOP_BEFORE_END_MS: u64 = 2_000;
/// How long the ingress paces a retry after a capacity candidate refused.
const CAPACITY_REFUSED_WAIT_MS: u64 = 25;

/// One client request, from the client's side.
pub(super) struct Op {
    cell: String,
    write: bool,
    /// The node the client sent it to.
    ingress: usize,
    retry: RouteRetry,
    /// The liveness goal this request is an attempt at.
    goal: Option<usize>,
    done: bool,
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
    /// The route the ingress forwarded this request on, `None` when it
    /// arrived from the client. A forwarded request is never forwarded on.
    via: Option<Epoch>,
    /// The epoch the request ran at, once routed locally.
    epoch: Option<Epoch>,
    /// The write the handler committed and its position.
    committed: Option<(WriteId, u64)>,
    /// How many writes the cell's database held when the handler answered:
    /// everything its response can reveal.
    saw: u64,
}

impl World {
    pub(super) fn start_clients(&mut self) {
        self.after(CLIENTS_START_MS, Item::ClientNew);
    }

    /// A client sends a request for a random cell to a random node.
    /// A client sends a request for a random cell to a random node.
    pub(super) fn client_new(&mut self) {
        let cell = format!("c{}", self.work_rng.below(self.config.cells as u64));
        let ingress = self.pick_ingress();
        let write = self.work_rng.chance(self.config.write_probability);
        self.send(cell, write, ingress, None);

        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        if self.t + gap + CLIENTS_STOP_BEFORE_END_MS < self.config.duration_ms {
            self.after(gap, Item::ClientNew);
        }
    }

    /// The load balancer in front of the fleet only picks nodes whose
    /// process is up.
    pub(super) fn pick_ingress(&mut self) -> Option<usize> {
        let up: Vec<usize> = (0..self.nodes.len())
            .filter(|&node| self.nodes[node].up)
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
        let op = self.next_op;
        self.next_op += 1;
        if goal.is_none() {
            self.stats.requests += 1;
        }
        self.ops.insert(
            op,
            Op {
                cell,
                write,
                ingress: ingress.unwrap_or(0),
                retry: RouteRetry::new(self.t),
                goal,
                done: false,
            },
        );
        match ingress {
            Some(node) => {
                let delay = self.net_rng.range(1, 3);
                self.after(
                    delay,
                    Item::Arrive {
                        node,
                        op,
                        via: None,
                    },
                );
            }
            None => self.settle(op, false),
        }
    }

    /// The request reaches a node: hand it to the core. A forward on a
    /// capacity route (epoch 0) asks the peer to take an unowned cell, and
    /// the peer must refuse at once if it has no room.
    pub(super) fn arrive(&mut self, node: usize, op: u64, via: Option<Epoch>) {
        if self.ops[&op].done {
            return;
        }
        if let Some(until) = self.nodes[node].paused_until {
            // The connection is accepted, but nothing reads it yet.
            return self.after(until - self.t, Item::Arrive { node, op, via });
        }
        // A forward needs the network on both ends; a client reaches its
        // ingress through the load balancer either way.
        let ingress = self.ops[&op].ingress;
        let cut_off = self.partitions[node].is_some() || self.partitions[ingress].is_some();
        if via.is_some() && cut_off {
            return self.bounce(op, Bounce::Unreachable);
        }
        if !self.nodes[node].up {
            match via {
                // The client's own connection is refused.
                None => self.settle(op, false),
                Some(_) => self.bounce(op, Bounce::Unreachable),
            }
            return;
        }
        let cell = self.ops[&op].cell.clone();
        let request = self.next_request;
        self.next_request += 1;
        let req = Req {
            op,
            via,
            epoch: None,
            committed: None,
            saw: 0,
        };
        self.nodes[node].requests.insert(request, req);
        let event = if via == Some(0) {
            Event::CapacityRequestAt {
                request,
                cell,
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
    ) {
        let Some(req) = self.nodes[node].requests.get(&request) else {
            return;
        };
        let (op, via) = (req.op, req.via);
        match result {
            Ok(Route::Local) => self.run_handler(node, request),
            Ok(Route::Remote {
                node: owner, epoch, ..
            }) => {
                self.nodes[node].requests.remove(&request);
                match via {
                    // The ingress forwarded here, but this node routes the
                    // cell elsewhere: `409 stale route`.
                    Some(_) => self.bounce(op, Bounce::Stale),
                    None => self.forward(op, owner, epoch),
                }
            }
            Err(RequestError::CapacityExhausted) if via.is_some() => {
                // The peer has no room: the same `409 stale route`.
                self.nodes[node].requests.remove(&request);
                self.bounce(op, Bounce::Stale);
            }
            Err(_) => {
                self.nodes[node].requests.remove(&request);
                self.settle(op, false);
            }
        }
    }

    /// The ingress sends the request on to the node its core named, unless
    /// a peer already said that route is stale.
    fn forward(&mut self, op: u64, owner: String, epoch: Epoch) {
        let t = self.t;
        let retry = &mut self.ops.get_mut(&op).unwrap().retry;
        if retry.observe(&owner, epoch) {
            // Wait for the ingress's view of the owner to change.
            match retry.wait(t) {
                Some(wait) => self.after(wait, Item::Redispatch { op }),
                None => self.settle(op, false),
            }
            return;
        }
        let target = self
            .nodes
            .iter()
            .position(|n| n.name == owner)
            .expect("routes name fleet nodes");
        self.stats.forwarded += 1;
        let delay = self.net_rng.range(1, 5);
        self.after(
            delay,
            Item::Arrive {
                node: target,
                op,
                via: Some(epoch),
            },
        );
    }

    /// The forward comes back to the ingress without having run.
    fn bounce(&mut self, op: u64, why: Bounce) {
        self.stats.bounced += 1;
        let delay = self.net_rng.range(1, 5);
        self.after(delay, Item::Bounced { op, why });
    }

    /// The ingress decides whether to try again, as `dispatch_do_call`
    /// does. Nothing ran on the peer, so trying again is safe; the question
    /// is only whether it can help.
    pub(super) fn bounced(&mut self, op: u64, why: Bounce) {
        let t = self.t;
        let entry = self.ops.get_mut(&op).unwrap();
        if entry.done {
            return;
        }
        let retry = &mut entry.retry;
        let (_, epoch) = retry.generation.clone().expect("a route was tried");
        let wait = match why {
            // Epoch zero is a capacity candidate, not an owner. Its refusal
            // spends no budget and has no deadline: invalidate, pace, retry.
            Bounce::Stale if epoch == 0 => Some(CAPACITY_REFUSED_WAIT_MS),
            // The owner said it is not the owner. Spend the one retry, and
            // do not send to that same route again until it changes.
            Bounce::Stale => match retry.dispatcher.redispatch(Attempt::NotOwner) {
                true => {
                    retry.blocked = true;
                    retry.wait(t)
                }
                false => None,
            },
            // Nobody listened, so nothing ran: one retry, at once.
            Bounce::Unreachable => retry
                .dispatcher
                .redispatch(Attempt::NeverConnected)
                .then_some(0),
        };
        match wait {
            Some(wait) => self.after(wait, Item::Redispatch { op }),
            None => self.settle(op, false),
        }
    }

    /// Retire the failed route in the ingress's core and resolve again.
    pub(super) fn redispatch(&mut self, op: u64) {
        let entry = &self.ops[&op];
        if entry.done {
            return;
        }
        let (ingress, cell) = (entry.ingress, entry.cell.clone());
        let (node, epoch) = entry.retry.generation.clone().expect("a route was tried");
        self.drive(ingress, Event::InvalidateRemote { cell, node, epoch });
        self.arrive(ingress, op, None);
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
            .filter(|(_, entry)| entry.ingress == node && !entry.done)
            .map(|(op, _)| *op)
            .collect();
        for op in lost {
            self.settle(op, false);
        }
    }

    /// The request runs here: maybe commit a write after a while, then
    /// answer.
    fn run_handler(&mut self, node: usize, request: RequestId) {
        let op = self.nodes[node].requests[&request].op;
        let (cell, write) = (self.ops[&op].cell.clone(), self.ops[&op].write);
        let n = &mut self.nodes[node];
        let epoch = n.published.get(&cell).copied();
        let name = n.name.clone();
        self.checker
            .routed_local(self.t, &name, &cell, epoch.is_some());
        let Some(epoch) = epoch else {
            self.nodes[node].requests.remove(&request);
            self.settle(op, false);
            return;
        };
        self.nodes[node].requests.get_mut(&request).unwrap().epoch = Some(epoch);
        let commit = self.work_rng.range(0, 10);
        let end = commit + self.work_rng.range(1, 30);
        if write {
            self.work(node, commit, Work::HandlerCommit { request });
        }
        self.work(node, end, Work::HandlerEnd { request });
    }

    /// The handler's write goes into the open database, and the replicator
    /// starts uploading it.
    /// If the runtime was stopped under it, the database is closed and the
    /// handler fails instead.
    pub(super) fn handler_commit(&mut self, node: usize, request: RequestId) {
        let Some(req) = self.nodes[node].requests.get(&request) else {
            return;
        };
        let (cell, epoch) = (self.ops[&req.op].cell.clone(), req.epoch.unwrap());
        match self.commit_write(node, &cell, epoch) {
            Some(committed) => {
                let req = self.nodes[node].requests.get_mut(&request).unwrap();
                req.committed = Some(committed);
            }
            None => self.handler_failed(node, request),
        }
    }

    /// The handler threw: the request fails and its activity ends.
    fn handler_failed(&mut self, node: usize, request: RequestId) {
        let req = self.nodes[node].requests.remove(&request).unwrap();
        self.settle(req.op, false);
        self.drive(node, Event::ActivityFinished { request });
    }

    /// The handler returns. Its response goes to the output gate, which
    /// holds it until every write it could reveal is durable; then the
    /// handler's activity is over.
    pub(super) fn handler_end(&mut self, node: usize, request: RequestId) {
        let Some(req) = self.nodes[node].requests.get(&request) else {
            return;
        };
        let cell = self.ops[&req.op].cell.clone();
        let Some((opened, saw)) = self.positions(node, &cell, req.epoch.unwrap()) else {
            return self.handler_failed(node, request);
        };
        // As production samples at egress: a handler that wrote asks for
        // everything committed when it answered, its own write and any
        // other handler's before it. A read reports what it saw above the
        // epoch's opening content, so the gate holds it behind writes that
        // are not proven yet.
        let (position, observed) = match req.committed {
            Some(_) => (Some(saw), None),
            None => (None, (saw > opened).then_some(saw)),
        };
        let observed = observed.filter(|_| self.config.mutation != Some(Mutation::ReadsBypassGate));
        self.nodes[node].requests.get_mut(&request).unwrap().saw = saw;
        self.drive(
            node,
            Event::OutputAt {
                request,
                channel: Channel::Response,
                position,
                observed,
                epoch: None,
                now_mono_ms: 0,
            },
        );
        self.drive(node, Event::ActivityFinished { request });
    }

    /// The gate released or refused the response: the client hears back.
    pub(super) fn release(
        &mut self,
        node: usize,
        request: RequestId,
        channel: Channel,
        result: Result<(), RequestError>,
    ) {
        assert_eq!(channel, Channel::Response, "handlers only answer");
        let Some(req) = self.nodes[node].requests.remove(&request) else {
            return;
        };
        if result.is_err() || self.ops[&req.op].done {
            // Refused, or released to a client that already gave up when
            // its connection died: either way nobody was told it worked.
            self.settle(req.op, false);
            return;
        }
        let epoch = req.epoch.expect("only a routed request answers");
        let cell = self.ops[&req.op].cell.clone();
        if req.committed.is_some() {
            self.stats.acked_writes += 1;
        }
        self.checker
            .response_released(self.t, &self.store, &cell, epoch, req.committed, req.saw);
        self.settle(req.op, true);
    }

    /// The client hears its answer. Only the first one counts: a request
    /// whose ingress died has already failed, whatever happens to it later.
    fn settle(&mut self, op: u64, answered: bool) {
        let entry = self.ops.get_mut(&op).unwrap();
        if entry.done {
            return;
        }
        entry.done = true;
        match entry.goal {
            Some(goal) => self.goal_settled(goal, answered),
            None if answered => self.stats.answered += 1,
            None => self.stats.failed += 1,
        }
    }
}
