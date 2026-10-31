//! The workload: clients that send requests, and the scripted handler that
//! stands in for a cell's code. V8 is not simulated. A handler may commit
//! one write, runs for a while, and answers through the output gate.

use celld_logic::{Channel, Epoch, Event, RequestError, RequestId, Route};

use super::{Item, World};
use crate::WriteId;

/// When clients start sending. Nodes need a few round trips for their
/// leases first; a request before that is refused, which proves nothing.
const CLIENTS_START_MS: u64 = 200;
/// When clients stop, so that requests in flight can finish before the end.
const CLIENTS_STOP_BEFORE_END_MS: u64 = 2_000;

/// A client request this node is serving.
pub(super) struct Req {
    cell: String,
    write: bool,
    /// The epoch the request ran at, once routed locally.
    epoch: Option<Epoch>,
    /// The write the handler committed and its position.
    committed: Option<(WriteId, u64)>,
    /// For a read, the committed position it saw when it answered.
    observed: Option<u64>,
}

impl World {
    pub(super) fn start_clients(&mut self) {
        self.after(CLIENTS_START_MS, Item::ClientNew);
    }

    /// A client sends a request for a random cell. Each cell has one home
    /// node that receives all its requests; forwarding between nodes is
    /// not modeled yet.
    pub(super) fn client_new(&mut self) {
        let cell = self.work_rng.below(self.config.cells as u64) as usize;
        let node = cell % self.nodes.len();
        let write = self.work_rng.chance(self.config.write_probability);
        let request = self.next_request;
        self.next_request += 1;
        self.stats.requests += 1;
        let cell = format!("c{cell}");
        self.nodes[node].requests.insert(
            request,
            Req {
                cell: cell.clone(),
                write,
                epoch: None,
                committed: None,
                observed: None,
            },
        );
        let event = Event::RequestAt {
            request,
            cell,
            now_mono_ms: 0,
        };
        let delay = self.net_rng.range(1, 3);
        self.deliver(node, delay, event);

        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        if self.t + gap + CLIENTS_STOP_BEFORE_END_MS < self.config.duration_ms {
            self.after(gap, Item::ClientNew);
        }
    }

    /// The core routed a request. Locally, run the handler: maybe commit a
    /// write after a while, then answer.
    pub(super) fn complete(
        &mut self,
        node: usize,
        request: RequestId,
        result: Result<Route, RequestError>,
    ) {
        let n = &mut self.nodes[node];
        let Some(req) = n.requests.get_mut(&request) else {
            return;
        };
        match result {
            Ok(Route::Local) => {
                let epoch = n.published.get(&req.cell).copied();
                let (name, cell, write) = (n.name.clone(), req.cell.clone(), req.write);
                self.checker
                    .routed_local(self.t, &name, &cell, epoch.is_some());
                let Some(epoch) = epoch else {
                    self.nodes[node].requests.remove(&request);
                    self.stats.failed += 1;
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
            Ok(Route::Remote { .. }) => {
                unimplemented!("a cell's home node is its only owner until forwarding exists")
            }
            Err(_) => {
                n.requests.remove(&request);
                self.stats.failed += 1;
            }
        }
    }

    /// The handler's write goes into the open database, and the replicator
    /// starts uploading it.
    pub(super) fn handler_commit(&mut self, node: usize, request: RequestId) {
        let Some(req) = self.nodes[node].requests.get(&request) else {
            return;
        };
        let (cell, epoch) = (req.cell.clone(), req.epoch.unwrap());
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
        let Some(req) = self.nodes[node].requests.get(&request) else {
            return;
        };
        let position = req.committed.map(|(_, position)| position);
        // A read reports what it saw above the epoch's opening content, so
        // the gate can hold it behind another handler's unproven write.
        let observed = match position {
            Some(_) => None,
            None => self.unproven_position(node, &req.cell, req.epoch.unwrap()),
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
            self.stats.failed += 1;
            return;
        }
        self.stats.answered += 1;
        let epoch = req.epoch.expect("only a routed request answers");
        if req.committed.is_some() {
            self.stats.acked_writes += 1;
        }
        self.checker.response_released(
            self.t,
            &self.store,
            &req.cell,
            epoch,
            req.committed,
            req.observed,
        );
    }
}
