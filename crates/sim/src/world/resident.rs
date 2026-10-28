//! Work on resident runtimes besides routed requests: client sockets, the
//! output gates handlers raise, and top-level Worker requests the core runs
//! on an idle resident.

use super::*;
use celld_logic::{WebSocketKind, WorkerRoute};

#[derive(Debug)]
pub(super) struct Socket {
    cell: String,
    kind: WebSocketKind,
}

impl World {
    fn resident_gap(&mut self, per_minute: f64) -> u64 {
        let mean = (60_000.0 / per_minute) as u64;
        self.work_rng.range(1, 2 * mean.max(1))
    }

    /// A running process and a cell its handlers or residents can reach:
    /// with `handler`, a cell where a handler runs now, as only running
    /// code accepts an upgrade or opens a socket; otherwise any process.
    fn resident_target(&mut self, handler: bool) -> Option<(ProcId, String)> {
        let targets: Vec<(ProcId, String)> = self
            .procs
            .iter()
            .filter(|(id, proc)| proc.pause == Pause::Running && !self.refuses(**id, false))
            .flat_map(|(id, proc)| {
                let cells: Vec<String> = if handler {
                    proc.requests
                        .values()
                        .filter(|ctx| {
                            ctx.answer == Answer::Pending
                                && !ctx.aborted
                                && ctx.epoch.is_some()
                                && proc.published.get(&ctx.cell) == ctx.epoch.as_ref()
                        })
                        .map(|ctx| ctx.cell.clone())
                        .collect()
                } else {
                    vec![String::new()]
                };
                cells.into_iter().map(move |cell| (*id, cell))
            })
            .collect();
        self.work_rng.pick(&targets).cloned()
    }

    // -----------------------------------------------------------------------
    // Sockets

    pub(super) fn socket_tick(&mut self) {
        let gap = self.resident_gap(self.config.resident.sockets);
        self.after(gap, What::SocketTick);
        if !self.workload || self.audit {
            return;
        }
        let Some((id, cell)) = self.resident_target(true) else {
            return;
        };
        let kind = *self
            .work_rng
            .pick(&[
                WebSocketKind::Hibernatable,
                WebSocketKind::Hibernatable,
                WebSocketKind::Regular,
                WebSocketKind::Outbound,
            ])
            .unwrap();
        let websocket = self.next_websocket;
        self.next_websocket += 1;
        self.stats.sockets_opened += 1;
        self.note(|| format!("p{id} opens {kind:?} socket {websocket} on {cell}"));
        self.procs.get_mut(&id).unwrap().sockets.insert(
            websocket,
            Socket {
                cell: cell.clone(),
                kind,
            },
        );
        let life = self
            .work_rng
            .range(100, self.config.resident.max_socket_ms.max(100));
        self.work(id, life, Work::SocketClose { websocket });
        let first = self.work_rng.range(10, 500);
        self.work(id, first, Work::SocketMessage { websocket });
        if self.config.mutation == Mutation::UnreportedSocket && kind != WebSocketKind::Hibernatable
        {
            return;
        }
        self.drive(
            id,
            Event::WebSocketOpened {
                cell,
                websocket,
                kind,
            },
        );
    }

    /// The client sends one message, an ordinary client operation that
    /// enters at its socket (`WebSocketRequestAt`).
    pub(super) fn socket_message(&mut self, id: ProcId, websocket: u64) {
        let Some(socket) = self.procs[&id].sockets.get(&websocket) else {
            return;
        };
        let cell = socket.cell.clone();
        if !self.workload {
            return;
        }
        let write = self.work_rng.chance(self.config.write_probability);
        let op = self.new_op(cell, write, None, self.t + CLIENT_TIMEOUT_MS, false, None);
        self.ops.get_mut(&op).unwrap().socket = Some((id, websocket));
        self.client_send(op);
        let gap = self.work_rng.range(100, 1_000);
        self.work(id, gap, Work::SocketMessage { websocket });
    }

    pub(super) fn socket_close(&mut self, id: ProcId, websocket: u64) {
        if let Some(socket) = self.procs.get_mut(&id).unwrap().sockets.remove(&websocket) {
            self.drive(
                id,
                Event::WebSocketClosed {
                    cell: socket.cell,
                    websocket,
                },
            );
        }
    }

    /// The core closed a socket: refused it, or cancelled its cell's
    /// activity. The shell drops it and reports it gone.
    pub(super) fn socket_dropped(
        &mut self,
        id: ProcId,
        cell: String,
        websocket: u64,
        immediate: &mut VecDeque<Event>,
    ) {
        if self
            .procs
            .get_mut(&id)
            .unwrap()
            .sockets
            .remove(&websocket)
            .is_some()
        {
            immediate.push_back(Event::WebSocketClosed { cell, websocket });
        }
    }

    pub(super) fn socket_refused(
        &mut self,
        id: ProcId,
        cell: String,
        websocket: u64,
        immediate: &mut VecDeque<Event>,
    ) {
        self.stats.sockets_refused += 1;
        self.socket_dropped(id, cell, websocket, immediate);
    }

    // -----------------------------------------------------------------------
    // Workers

    pub(super) fn worker_tick(&mut self) {
        let gap = self.resident_gap(self.config.resident.workers);
        self.after(gap, What::WorkerTick);
        if !self.workload || self.audit {
            return;
        }
        let Some((id, _)) = self.resident_target(false) else {
            return;
        };
        let request = self.next_request;
        self.next_request += 1;
        self.drive(id, Event::WorkerRequest { request });
    }

    pub(super) fn complete_worker(
        &mut self,
        id: ProcId,
        request: RequestId,
        route: Option<WorkerRoute>,
        immediate: &mut VecDeque<Event>,
    ) {
        // `None`: the stateless pool serves it, which the sim leaves out.
        let Some(WorkerRoute { cell, epoch }) = route else {
            return;
        };
        if self.procs[&id].published.get(&cell) != Some(&epoch) {
            self.checker
                .on_local_route_without_runtime(self.t, id, request, &cell);
            immediate.push_back(Event::ActivityFinished { request });
            return;
        }
        self.stats.workers_routed += 1;
        self.procs
            .get_mut(&id)
            .unwrap()
            .workers
            .insert(request, (cell, epoch));
        if self.config.mutation == Mutation::UnpinnedWorker {
            immediate.push_back(Event::ActivityFinished { request });
        }
        let run = self.work_rng.range(20, 2_000);
        self.work(id, run, Work::WorkerEnd { request });
    }

    pub(super) fn worker_end(&mut self, id: ProcId, request: RequestId) {
        if self
            .procs
            .get_mut(&id)
            .unwrap()
            .workers
            .remove(&request)
            .is_some()
            && self.config.mutation != Mutation::UnpinnedWorker
        {
            self.drive(id, Event::ActivityFinished { request });
        }
    }

    /// `CancelCellActivity` names a running Worker request: it is aborted,
    /// and its guard drops.
    pub(super) fn worker_cancelled(
        &mut self,
        id: ProcId,
        request: RequestId,
        immediate: &mut VecDeque<Event>,
    ) {
        if self
            .procs
            .get_mut(&id)
            .unwrap()
            .workers
            .remove(&request)
            .is_some()
            && self.config.mutation != Mutation::UnpinnedWorker
        {
            immediate.push_back(Event::ActivityFinished { request });
        }
    }

    // -----------------------------------------------------------------------
    // Output gates

    /// A handler's write raised an output gate; its reply waits for it.
    pub(super) fn raise_gate(&mut self, id: ProcId, request: RequestId, cell: String) {
        let gate = self.next_request;
        self.next_request += 1;
        let proc = self.procs.get_mut(&id).unwrap();
        proc.gates.insert(gate, request);
        proc.requests.get_mut(&request).unwrap().gate = Some(gate);
        self.drive(
            id,
            Event::GateRequestAt {
                request: gate,
                cell,
                now_mono_ms: 0,
            },
        );
    }

    /// Whether `request` is a gate, which the core routes like a request.
    pub(super) fn gate_routed(
        &mut self,
        id: ProcId,
        gate: RequestId,
        result: &Result<Route, celld_logic::RequestError>,
    ) -> bool {
        let Some(handler) = self.procs.get_mut(&id).unwrap().gates.remove(&gate) else {
            return false;
        };
        let local = matches!(result, Ok(Route::Local));
        if local {
            // A local gate is active work until its effect leaves.
            let delay = self.work_rng.range(0, 3);
            self.work(id, delay, Work::GateEnd { gate });
        }
        let proc = self.procs.get_mut(&id).unwrap();
        let Some(ctx) = proc.requests.get_mut(&handler) else {
            return true;
        };
        ctx.gate = None;
        let (cell, epoch) = (ctx.cell.clone(), ctx.epoch);
        if local && proc.published.get(&cell) == epoch.as_ref() {
            self.stats.gates_admitted += 1;
        } else {
            if local {
                self.checker
                    .on_local_route_without_runtime(self.t, id, gate, &cell);
            }
            // The effect cannot leave, so the handler fails.
            ctx.aborted = true;
        }
        if ctx.answer == Answer::HeldByGate {
            ctx.answer = Answer::Pending;
            self.handler_end(id, handler);
        }
        true
    }

    // -----------------------------------------------------------------------
    // Runtime stops

    /// A runtime of `cell` at `epoch` stops. An eviction or a swap must
    /// find no live transport and no Worker request on it; whatever runs
    /// there dies with the isolate.
    pub(super) fn resident_work_stops(
        &mut self,
        id: ProcId,
        cell: &str,
        epoch: Epoch,
        cause: StopCause,
        immediate: &mut VecDeque<Event>,
    ) {
        let proc = &self.procs[&id];
        let sockets: Vec<u64> = proc
            .sockets
            .iter()
            .filter(|(_, socket)| socket.cell == cell && socket.kind != WebSocketKind::Hibernatable)
            .map(|(websocket, _)| *websocket)
            .collect();
        let workers: Vec<RequestId> = proc
            .workers
            .iter()
            .filter(|(_, (c, e))| c == cell && *e == epoch)
            .map(|(request, _)| *request)
            .collect();
        if matches!(cause, StopCause::Evict { .. } | StopCause::Swap)
            && (!sockets.is_empty() || !workers.is_empty())
        {
            let t = self.t;
            self.checker
                .on_runtime_cut(t, id, cell, epoch, sockets.len(), workers.len());
        }
        // A reset or a fence forgets the cell's sockets and work in the core
        // already; any other stop hears of them.
        let report = !matches!(cause, StopCause::Reset | StopCause::Fence);
        let proc = self.procs.get_mut(&id).unwrap();
        if !report {
            proc.sockets.retain(|_, socket| socket.cell != cell);
        }
        for websocket in sockets {
            if proc.sockets.remove(&websocket).is_some() && report {
                immediate.push_back(Event::WebSocketClosed {
                    cell: cell.to_string(),
                    websocket,
                });
            }
        }
        for request in workers {
            proc.workers.remove(&request);
            if report && self.config.mutation != Mutation::UnpinnedWorker {
                immediate.push_back(Event::ActivityFinished { request });
            }
        }
    }
}
