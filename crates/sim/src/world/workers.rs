//! Top-level Worker requests: stateless scripts that need no cell, but may
//! borrow an idle resident cell's isolate instead of a cold one from the
//! stateless pool (`WorkerRequest`). The core picks the resident and pins
//! it while the Worker runs (`CompleteWorker`); with none to spare, the
//! request runs in the pool and the core never hears of it again.

use std::collections::VecDeque;

use celld_logic::{Event, RequestId, WorkerRoute};

use super::{Work, World};

/// How long a Worker runs.
const WORKER_RUN_MS: (u64, u64) = (1, 30);

impl World {
    /// A Worker request reaches the node.
    pub(super) fn worker_request(&mut self, node: usize) {
        self.stats.workers += 1;
        if self.preserving(node) {
            // A preserving actor sends it to the pool without asking.
            return;
        }
        let request = self.next_request;
        self.next_request += 1;
        self.machines[node].proc_mut().workers.insert(request);
        self.drive(node, Event::WorkerRequest { request });
    }

    /// The core chose an isolate for a Worker request. A resident one must
    /// be a cell published here at that epoch, and stays pinned until the
    /// Worker ends.
    pub(super) fn worker_routed(
        &mut self,
        node: usize,
        request: RequestId,
        route: Option<WorkerRoute>,
        immediate: &mut VecDeque<Event>,
    ) {
        let waiting = self.machines[node].proc_mut().workers.remove(&request);
        let Some(WorkerRoute { cell, epoch }) = route else {
            return;
        };
        if !waiting {
            // Its caller is gone: release the pin at once, as `actor.rs` does.
            immediate.push_back(Event::ActivityFinished { request });
            return;
        }
        let published = self.machines[node].proc().published.get(&cell) == Some(&epoch);
        let name = self.machines[node].name.clone();
        self.checker.routed_local(self.t, &name, &cell, published);
        self.stats.workers_resident += 1;
        let (lo, hi) = WORKER_RUN_MS;
        let run = self.work_rng.range(lo, hi);
        self.work(node, run, Work::ActivityDone { request, cell });
    }
}
