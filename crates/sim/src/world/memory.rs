//! What a process's memory is made of (`SimConfig::memory`): a fixed base,
//! one heap per isolate, a realm per cell, and surges no cell owns. Load
//! samples report it, so the core's pressure latch and walk down run against
//! it, and the shell tells the core which heap each runtime started in, as
//! `pool.rs` does.

use super::*;
use celld_logic::isolate::HeapId;

pub(super) const MB: u64 = 1024 * 1024;
/// How long a shedding process may go without starting an eviction.
const WEDGE_MS: u64 = 10_000;

#[derive(Debug, Default)]
pub(super) struct ProcMemory {
    /// The heap each runtime started in, by cell epoch.
    heap_of: BTreeMap<(String, Epoch), u64>,
    next_heap: u64,
    /// A surge's size in MB, and when it passes.
    surge: Option<(u64, Time)>,
    /// When the process last stood relieved or started an eviction.
    progress: Time,
}

impl World {
    /// The runtimes still running, by heap.
    fn heap_cells(&self, id: ProcId) -> BTreeMap<u64, usize> {
        let proc = &self.procs[&id];
        let mut heaps = BTreeMap::new();
        for (key, heap) in &proc.memory.heap_of {
            if proc
                .replicas
                .get(key)
                .is_some_and(|replica| replica.running)
            {
                *heaps.entry(*heap).or_default() += 1;
            }
        }
        heaps
    }

    /// `place_cell`: the fullest heap with room, or a new one. `None` when
    /// the run has no memory model, or the shell forgets to say.
    pub(super) fn place_runtime(&mut self, id: ProcId, cell: &str, epoch: Epoch) -> Option<HeapId> {
        let per_heap = self.config.memory.as_ref()?.cells_per_heap;
        let key = (cell.to_string(), epoch);
        self.procs.get_mut(&id).unwrap().memory.heap_of.remove(&key);
        let heap = self
            .heap_cells(id)
            .into_iter()
            .filter(|(_, cells)| *cells < per_heap)
            .max_by_key(|(heap, cells)| (*cells, std::cmp::Reverse(*heap)))
            .map(|(heap, _)| heap);
        let memory = &mut self.procs.get_mut(&id).unwrap().memory;
        let heap = heap.unwrap_or_else(|| {
            memory.next_heap += 1;
            memory.next_heap
        });
        memory.heap_of.insert(key, heap);
        (self.config.mutation != Mutation::UnplacedHeaps).then(|| HeapId::new(heap))
    }

    /// Forget stopped runtimes, counting the heaps whose last cell left.
    pub(super) fn prune_heaps(&mut self, id: ProcId) {
        let live = self.heap_cells(id);
        let memory = &mut self.procs.get_mut(&id).unwrap().memory;
        let before: BTreeSet<u64> = memory.heap_of.values().copied().collect();
        memory.heap_of.retain(|_, heap| live.contains_key(heap));
        let after: BTreeSet<u64> = memory.heap_of.values().copied().collect();
        self.stats.heaps_freed += before.difference(&after).count() as u64;
    }

    /// What the next load sample reads, in bytes.
    pub(super) fn memory_bytes(&self, id: ProcId) -> u64 {
        let Some(model) = &self.config.memory else {
            return 0;
        };
        let heaps = self.heap_cells(id);
        let cells: usize = heaps.values().sum();
        let surge = match self.procs[&id].memory.surge {
            Some((mb, until)) if until > self.t => mb,
            _ => 0,
        };
        (model.base_mb + heaps.len() as u64 * model.heap_mb + cells as u64 * model.cell_mb + surge)
            * MB
    }

    /// Memory no cell owns grows for a few seconds.
    pub(super) fn surge(&mut self, id: ProcId) {
        let Some(max_mb) = self.config.memory.as_ref().map(|m| m.max_surge_mb) else {
            return;
        };
        let mb = self.fault_rng.range(20, max_mb);
        let until = self.t + self.fault_rng.range(1_000, 5_000);
        self.procs.get_mut(&id).unwrap().memory.surge = Some((mb, until));
        self.note(|| format!("surge p{id} by {mb} MB"));
    }

    /// The walk down moved: an eviction started.
    pub(super) fn shed_progress(&mut self, id: ProcId) {
        let t = self.t;
        if let Some(proc) = self.procs.get_mut(&id) {
            proc.memory.progress = t;
        }
    }

    /// After a load sample: once faults healed, a shedding process must keep
    /// evicting while it holds more than one heap. Only memory its cells hold
    /// is left then, and one full heap fits under the resume line, so a walk
    /// down that stops there has a heap it could still give back.
    pub(super) fn check_pressure(&mut self, id: ProcId) {
        let Some(proc) = self.procs.get(&id) else {
            return;
        };
        if self.config.memory.is_none() {
            return;
        }
        if !proc.state.shedding() {
            self.shed_progress(id);
            return;
        }
        self.stats.shed_samples += 1;
        let heaps = self.heap_cells(id).len();
        let since = self.t.saturating_sub(proc.memory.progress);
        if self.chaos || proc.shutdown.is_some() || heaps < 2 || since < WEDGE_MS {
            return;
        }
        let (mb, residents) = (self.memory_bytes(id) / MB, proc.state.residents().len());
        self.checker.on_pressure_wedge(self.t, id, mb, residents);
    }
}
