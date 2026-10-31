//! Deterministic simulation of the celld coordination core.
//!
//! `celld-logic` makes every decision and performs none of them. Each input
//! is an [`celld_logic::Event`] and each output is an
//! [`celld_logic::Effect`]. This crate runs several of those cores in one
//! thread, on one simulated clock, against one simulated bucket. It carries
//! out their effects the way the production shell (`crates/celld/actor.rs`)
//! does and turns the results into later events.
//!
//! A seed fixes the whole run, so a failing seed replays exactly.
//!
//! V8 is not part of the simulation. A cell is a scripted handler that may
//! commit one write, runs for a while, and answers through the output gate.
//! The properties the run is checked against are in `checks.rs`.

mod checks;
mod rng;
mod store;
mod world;

pub use checks::Violation;
pub use rng::Rng;

/// One committed write. Every write in a run gets its own id, so whether a
/// write survived is a question of membership.
pub type WriteId = u64;

/// What one run varies.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub cells: usize,
    /// Node lease lifetime. The core renews every third of it.
    pub ttl_ms: u64,
    /// Simulated time the run lasts.
    pub duration_ms: u64,
    /// One-way latency to the bucket, drawn per request and per response.
    pub store_latency_ms: (u64, u64),
    /// Gap between client requests across the whole fleet.
    pub request_gap_ms: (u64, u64),
    /// Share of requests whose handler commits a write.
    pub write_probability: f64,
    /// Keep a human-readable line for every event and effect.
    pub trace: bool,
}

impl SimConfig {
    pub fn calm() -> Self {
        Self {
            nodes: 3,
            cells: 6,
            ttl_ms: 3_000,
            duration_ms: 30_000,
            store_latency_ms: (2, 20),
            request_gap_ms: (5, 60),
            write_probability: 0.6,
            trace: false,
        }
    }
}

/// Counts of what a run did. A check that never had anything to look at
/// proves nothing, so the tests read these too.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub events: u64,
    pub requests: u64,
    /// Requests whose response was released to the client.
    pub answered: u64,
    /// Requests the core refused, or whose response it refused to release.
    pub failed: u64,
    /// Writes a client was told succeeded.
    pub acked_writes: u64,
    /// Requests an ingress forwarded to another node.
    pub forwarded: u64,
    /// Forwards a peer answered with `409 stale route`.
    pub stale_routes: u64,
}

/// What a run produced.
#[derive(Debug)]
pub struct Outcome {
    pub seed: u64,
    /// A hash of every event and effect, in order, with its time. Two runs
    /// of one seed must produce the same hash.
    pub trace_hash: u64,
    pub trace: Vec<String>,
    /// Nodes that halted (self-fenced). A calm run has none.
    pub halts: Vec<String>,
    /// Nodes that held a live node lease when the run ended.
    pub authoritative: Vec<String>,
    /// A panic anywhere in the run, in a core or in the simulated shell,
    /// with its seed and time. A failed `State::validate` is one.
    pub panic: Option<String>,
    pub violations: Vec<Violation>,
    pub stats: Stats,
}

impl Outcome {
    pub fn passed(&self) -> bool {
        self.panic.is_none() && self.violations.is_empty()
    }
}

pub fn run(seed: u64, config: &SimConfig) -> Outcome {
    world::World::new(seed, config.clone()).run()
}
