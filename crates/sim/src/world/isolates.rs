//! The cell isolate pool: which V8 heap each resident cell's realm lives in.
//!
//! As `runtime.rs` does, a runtime that starts is placed once, by
//! `isolate::place_cell`: into the fullest heap of the process's current
//! generation that still has room, or a new heap. The core hears the heap
//! in `RuntimeStarted`, and its walk-down evicts from the heap closest to
//! empty, because only the cut that takes a heap's last cell gives the heap
//! back. A runtime that stops leaves its heap, and the pool frees a heap
//! the moment it holds no cell. Each heap costs memory of its own
//! (`Memory::heap`).

use std::collections::BTreeMap;

use celld_logic::isolate::{self, HeapId, IsolateLoad, Placement, PoolLimits, PoolLoad};

use super::World;

/// One process's cell heaps.
#[derive(Default)]
pub(super) struct Pool {
    next: u64,
    /// Each live heap: the generation it runs, and how many cells it holds.
    heaps: BTreeMap<HeapId, (u64, usize)>,
    placed: BTreeMap<String, HeapId>,
}

impl Pool {
    /// The heaps the process holds.
    pub(super) fn heaps(&self) -> usize {
        self.heaps.len()
    }

    /// A cell's realm leaves its heap; a heap left empty is freed.
    pub(super) fn unplace(&mut self, cell: &str) {
        let Some(heap) = self.placed.remove(cell) else {
            return;
        };
        let (_, cells) = self.heaps.get_mut(&heap).expect("a placed heap");
        *cells -= 1;
        if *cells == 0 {
            self.heaps.remove(&heap);
        }
    }
}

impl World {
    /// Place `cell`'s realm on a heap of the process's current generation.
    pub(super) fn place_cell(&mut self, node: usize, cell: &str) -> HeapId {
        let max_cells = self.config.cells_per_heap;
        let p = self.machines[node].proc_mut();
        let generation = p.generation;
        let pool = &mut p.isolates;
        pool.unplace(cell);
        let candidates: Vec<HeapId> = (pool.heaps.iter())
            .filter(|(_, (g, _))| *g == generation)
            .map(|(heap, _)| *heap)
            .collect();
        let load = PoolLoad {
            isolates: (candidates.iter())
                .map(|heap| IsolateLoad {
                    cells: pool.heaps[heap].1,
                    ..IsolateLoad::default()
                })
                .collect(),
            limits: PoolLimits {
                grow_at: 2,
                shrink_under: 1,
                max_stateless: 1,
                max_requests: None,
                max_cells,
            },
        };
        let heap = match isolate::place_cell(&load) {
            Placement::Existing(index) => candidates[index],
            Placement::Grow => {
                pool.next += 1;
                let heap = HeapId::new(pool.next);
                pool.heaps.insert(heap, (generation, 0));
                heap
            }
        };
        pool.heaps.get_mut(&heap).expect("a heap").1 += 1;
        pool.placed.insert(cell.to_string(), heap);
        heap
    }
}
