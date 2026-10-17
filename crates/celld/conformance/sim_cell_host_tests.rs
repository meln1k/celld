// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The scripted cell host, `crate::conformance_sim_cell_host`.
//
// The deterministic worlds drive the actor through `CellHost::Scripted`: a
// host that runs cells without V8, over the real replicator and a simulated
// bucket and disk. A cell is a SQLite database with celld's own schema,
// opened through the node disk's VFS, and its handler is a script:
//
// - `POST` appends the request body to the cell's log and answers with the
//   write position, which the shell holds behind the output gate.
// - `GET` answers the whole log, comma-separated.
//
// Everything below the handler is the production path: `restore_cell`
// activates the epoch through `LtxRepl`, the output gate waits on the
// replicator's durability proof, and a stop evicts, rebases, closes or
// discards the replica exactly as `RuntimeManager::stop_cell` does.
//
// The host does not run alarms yet: it reports none, and refuses to fire.

// Only the in-crate tests drive an actor through it.
#[cfg(test)]
#[path = "sim_cell_host/host.rs"]
mod host;

#[cfg(test)]
pub use host::SimCellHost;
