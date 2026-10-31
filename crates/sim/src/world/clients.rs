//! The workload: clients, the ingress that forwards their requests to a
//! cell's owner, and the scripted handler that stands in for a cell's code.
//!
//! V8 is not simulated. A handler may commit one write, runs for a while,
//! and answers through the output gate.
//!
//! A client sends each request to a random node, its ingress. When the
//! ingress's core routes the request to another node, the ingress forwards
//! it there, as `dispatch_fetch` in `crates/celld/main.rs` does. A
//! forwarded request is never forwarded again: a node that does not own the
//! cell answers `409 stale route`, and the ingress decides whether to
//! resolve the route again. That retry policy follows `RemoteRouteRetry`
//! in `main.rs` and shares its budget, `celld_logic::routing::Dispatcher`.

use celld_logic::routing::{Attempt, Dispatcher};
use celld_logic::{Channel, Epoch, Event, RequestError, RequestId, Route};

use super::{Item, Time, World};
use crate::WriteId;

/// When clients start sending. Nodes need a few round trips for their
/// leases first; a request before that is refused, which proves nothing.
const CLIENTS_START_MS: u64 = 200;
/// When clients stop, so that requests in flight can finish before the end.
const CLIENTS_STOP_BEFORE_END_MS: u64 = 2_000;
/// The ingress's deadline for one request, `operation_deadline_ms`.
const DISPATCH_DEADLINE_MS: u64 = 15_000;

/// One client request, from the client's side.
pub(super) struct Op {
    cell: String,
    write: bool,
    ingress: usize,
    retry: RouteRetry,
    done: bool,
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
    /// For a read, the committed position it saw when it answered.
    observed: Option<u64>,
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
            deadline: now + DISPATCH_DEADLINE_MS,
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

    /// The peer answered that it is not the owner. `Some(wait)` when the
    /// ingress may resolve again after `wait`.
    fn not_owner(&mut self, now: Time) -> Option<u64> {
        if !self.dispatcher.redispatch(Attempt::NotOwner) {
            return None;
        }
        self.blocked = true;
        self.wait(now)
    }

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

impl World {
    pub(super) fn start_clients(&mut self) {
        self.after(CLIENTS_START_MS, Item::ClientNew);
    }

    /// A client sends a request for a random cell to a random node.
    pub(super) fn client_new(&mut self) {
        let cell = format!("c{}", self.work_rng.below(self.config.cells as u64));
        let ingress = self.work_rng.below(self.nodes.len() as u64) as usize;
        let write = self.work_rng.chance(self.config.write_probability);
        let op = self.next_op;
        self.next_op += 1;
        self.stats.requests += 1;
        let retry = RouteRetry::new(self.t);
        self.ops.insert(
            op,
            Op {
                cell,
                write,
                ingress,
                retry,
                done: false,
            },
        );
        let delay = self.net_rng.range(1, 3);
        self.after(
            delay,
            Item::Arrive {
                node: ingress,
                op,
                via: None,
            },
        );

        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        if self.t + gap + CLIENTS_STOP_BEFORE_END_MS < self.config.duration_ms {
            self.after(gap, Item::ClientNew);
        }
    }

    /// The request reaches a node: hand it to the core. A forward on a
    /// capacity route (epoch 0) asks the peer to take an unowned cell, and
    /// the peer must refuse at once if it has no room.
    pub(super) fn arrive(&mut self, node: usize, op: u64, via: Option<Epoch>) {
        let cell = self.ops[&op].cell.clone();
        let request = self.next_request;
        self.next_request += 1;
        let req = Req {
            op,
            via,
            epoch: None,
            committed: None,
            observed: None,
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
                    Some(_) => self.reply_stale(op),
                    None => self.forward(op, owner, epoch),
                }
            }
            Err(RequestError::CapacityExhausted) if via == Some(0) => {
                // A refused capacity handoff answers with the same header.
                self.nodes[node].requests.remove(&request);
                self.reply_stale(op);
            }
            Err(_) => {
                self.nodes[node].requests.remove(&request);
                self.settle(op, false);
            }
        }
    }

    /// The ingress sends the request on to the node its core named.
    fn forward(&mut self, op: u64, owner: String, epoch: Epoch) {
        let t = self.t;
        let entry = self.ops.get_mut(&op).unwrap();
        if entry.retry.observe(&owner, epoch) {
            // A peer already said this route is stale. Wait for the
            // ingress's view of the owner to change before trying again.
            match entry.retry.wait(t) {
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

    /// A peer's `409 stale route` travels back to the ingress.
    fn reply_stale(&mut self, op: u64) {
        self.stats.stale_routes += 1;
        let delay = self.net_rng.range(1, 5);
        self.after(delay, Item::Stale { op });
    }

    /// The ingress hears the route was stale: spend the one retry on it, or
    /// give up.
    pub(super) fn stale(&mut self, op: u64) {
        let t = self.t;
        match self.ops.get_mut(&op).unwrap().retry.not_owner(t) {
            Some(wait) => self.after(wait, Item::Redispatch { op }),
            None => self.settle(op, false),
        }
    }

    /// Retire the stale route in the ingress's core and resolve again.
    pub(super) fn redispatch(&mut self, op: u64) {
        let entry = &self.ops[&op];
        let (ingress, cell) = (entry.ingress, entry.cell.clone());
        let (node, epoch) = entry.retry.generation.clone().expect("a route was tried");
        self.drive(ingress, Event::InvalidateRemote { cell, node, epoch });
        self.arrive(ingress, op, None);
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
            self.after(commit, Item::HandlerCommit { node, request });
        }
        self.after(end, Item::HandlerEnd { node, request });
    }

    /// The handler's write goes into the open database, and the replicator
    /// starts uploading it.
    pub(super) fn handler_commit(&mut self, node: usize, request: RequestId) {
        let req = &self.nodes[node].requests[&request];
        let (cell, epoch) = (self.ops[&req.op].cell.clone(), req.epoch.unwrap());
        let committed = self
            .commit_write(node, &cell, epoch)
            .expect("a routed request's epoch is open");
        self.nodes[node]
            .requests
            .get_mut(&request)
            .unwrap()
            .committed = Some(committed);
    }

    /// The handler returns. Its response goes to the output gate, which
    /// holds it until every write it could reveal is durable; then the
    /// handler's activity is over.
    pub(super) fn handler_end(&mut self, node: usize, request: RequestId) {
        let req = &self.nodes[node].requests[&request];
        let cell = self.ops[&req.op].cell.clone();
        let position = req.committed.map(|(_, position)| position);
        // A read reports what it saw above the epoch's opening content, so
        // the gate can hold it behind another handler's unproven write.
        let observed = match position {
            Some(_) => None,
            None => self.unproven_position(node, &cell, req.epoch.unwrap()),
        };
        self.nodes[node]
            .requests
            .get_mut(&request)
            .unwrap()
            .observed = observed;
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
        if result.is_err() {
            self.settle(req.op, false);
            return;
        }
        let epoch = req.epoch.expect("only a routed request answers");
        let cell = self.ops[&req.op].cell.clone();
        if req.committed.is_some() {
            self.stats.acked_writes += 1;
        }
        self.checker.response_released(
            self.t,
            &self.store,
            &cell,
            epoch,
            req.committed,
            req.observed,
        );
        self.settle(req.op, true);
    }

    /// The client hears its answer, once.
    fn settle(&mut self, op: u64, answered: bool) {
        let entry = self.ops.get_mut(&op).unwrap();
        assert!(!entry.done, "request {op} finished twice");
        entry.done = true;
        if answered {
            self.stats.answered += 1;
        } else {
            self.stats.failed += 1;
        }
    }
}
