//! The output gate, from the shell's side. Whatever leaves a cell can reveal
//! what its handler saw, so it waits at the gate until every write it could
//! reveal is durable: a response (`OutputAt` on `Channel::Response`), and a
//! handler's call to a third party (a `fetch()`), which the shell first
//! routes as a gate request (`GateRequestAt`) and then holds as an output
//! on `Channel::Fetch`.
//!
//! When the gate lets one go, the oracle judges what it revealed.

use celld_logic::{Channel, Epoch, Event, RequestError, RequestId, Route};

use super::{Pending, Work, World};
use crate::checks::Reveal;
use crate::{Mutation, WriteId};

/// What a handler's output asks the gate for, sampled as it leaves.
pub(super) struct Egress {
    /// For a handler that wrote: everything committed when it answered,
    /// its own write and any other handler's before it.
    pub(super) position: Option<u64>,
    /// For a read: what it saw above the epoch's opening content, so the
    /// gate holds it behind writes that are not proven yet.
    pub(super) observed: Option<u64>,
    /// The cell's database at that moment: everything the output reveals.
    pub(super) seen: Vec<WriteId>,
}

/// A handler's call to a third party, held at the gate: what it can
/// reveal, sampled when the handler raised it.
pub(super) struct Gate {
    pub(super) reveal: Reveal,
    position: Option<u64>,
    observed: Option<u64>,
}

impl World {
    /// Sample an output leaving `cell` at `epoch`, as production does at
    /// egress. `None` when the runtime was stopped and the database closed.
    pub(super) fn egress(
        &self,
        node: usize,
        cell: &str,
        epoch: Epoch,
        wrote: bool,
    ) -> Option<Egress> {
        let (opened, saw) = self.positions(node, cell, epoch)?;
        let (position, observed) = match wrote {
            true => (Some(saw), None),
            false => (None, (saw > opened).then_some(saw)),
        };
        let observed = observed.filter(|_| self.config.mutation != Some(Mutation::ReadsBypassGate));
        let seen = self.seen(node, cell, epoch)?.to_vec();
        Some(Egress {
            position,
            observed,
            seen,
        })
    }

    /// The handler calls out to a third party: route a gate request for
    /// the cell, and hold the call until the core releases it.
    pub(super) fn handler_effect(&mut self, node: usize, request: RequestId) {
        let Some(req) = self.machines[node].proc().client(request) else {
            return;
        };
        let (cell, epoch, committed, sent_at) = (
            req.cell.clone(),
            req.epoch.unwrap(),
            req.committed,
            req.sent_at,
        );
        let Some(egress) = self.egress(node, &cell, epoch, committed.is_some()) else {
            return;
        };
        let held = Gate {
            reveal: Reveal {
                cell: cell.clone(),
                epoch,
                committed,
                seen: egress.seen,
                sent_at,
                answer: false,
            },
            position: egress.position,
            observed: egress.observed,
        };
        let gate = self.pend(node, Pending::Gate(held));
        let event = Event::GateRequestAt {
            request: gate,
            cell,
            now_mono_ms: 0,
        };
        self.drive(node, event);
    }

    /// The core routed a gate request. Routed here, the held call becomes
    /// an output; routed anywhere else, the cell left mid-call and the call
    /// fails closed.
    pub(super) fn gate_routed(
        &mut self,
        node: usize,
        request: RequestId,
        result: &Result<Route, RequestError>,
    ) {
        let p = self.machines[node].proc_mut();
        let gate = p.gate(request).expect("a gate request");
        let delay = self.net_rng.range(0, 2);
        match result {
            Ok(Route::Local) => {
                let event = Event::OutputAt {
                    request,
                    channel: Channel::Fetch,
                    position: gate.position,
                    observed: gate.observed,
                    epoch: Some(gate.reveal.epoch),
                    now_mono_ms: 0,
                };
                self.deliver(node, delay, event);
            }
            Ok(Route::Remote { .. }) => {
                p.pending.remove(&request);
                self.deliver(node, delay, Event::ActivityFinished { request });
            }
            Err(_) => {
                p.pending.remove(&request);
            }
        }
    }

    /// The gate let a handler's call leave, or refused it. One that left
    /// revealed what its handler had seen, so the oracle judges it as it
    /// judges a response. Either way the gate's pin on the cell ends.
    pub(super) fn gate_released(
        &mut self,
        node: usize,
        request: RequestId,
        result: Result<(), RequestError>,
    ) {
        let Some(gate) = self.machines[node].proc_mut().take_gate(request) else {
            return;
        };
        if result.is_ok() {
            self.stats.effects += 1;
            let who = self.who(node);
            self.checker
                .response_released(self.t, &self.store, &who, &gate.reveal);
        }
        let delay = self.net_rng.range(0, 2);
        let cell = gate.reveal.cell;
        self.work(node, delay, Work::ActivityDone { request, cell });
    }
}
