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
//! What the ingress does with a stale route (`RemoteRouteRetry` in
//! `main.rs`) is not modeled yet. A calm fleet never moves ownership, so it
//! never produces one.

use celld_logic::{Channel, Epoch, Event, RequestError, RequestId, Route};

use super::{Item, World};
use crate::WriteId;

/// When clients start sending. Nodes need a few round trips for their
/// leases first; a request before that is refused, which proves nothing.
const CLIENTS_START_MS: u64 = 200;
/// When clients stop, so that requests in flight can finish before the end.
const CLIENTS_STOP_BEFORE_END_MS: u64 = 2_000;
/// One client request, from the client's side.
pub(super) struct Op {
    cell: String,
    write: bool,
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
    /// How many writes the cell's database held when the handler answered:
    /// everything its response can reveal.
    saw: u64,
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
        self.ops.insert(
            op,
            Op {
                cell,
                write,
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
                    Some(_) => self.reply_stale(op),
                    None => self.forward(op, owner, epoch),
                }
            }
            Err(RequestError::CapacityExhausted) if via.is_some() => {
                // The peer has no room: the same `409 stale route`.
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

    /// A peer answers `409 stale route`.
    fn reply_stale(&mut self, op: u64) {
        unimplemented!("request {op}: the ingress's stale-route retry is not modeled yet")
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
        let (opened, saw) = self.positions(node, &cell, req.epoch.unwrap());
        // As production samples at egress: a handler that wrote asks for
        // everything committed when it answered, its own write and any
        // other handler's before it. A read reports what it saw above the
        // epoch's opening content, so the gate holds it behind writes that
        // are not proven yet.
        let (position, observed) = match req.committed {
            Some(_) => (Some(saw), None),
            None => (None, (saw > opened).then_some(saw)),
        };
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
        if result.is_err() {
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
