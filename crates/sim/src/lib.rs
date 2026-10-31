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

mod rng;
mod store;
mod world;

pub use rng::Rng;

/// What one run varies.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    /// Node lease lifetime. The core renews every third of it.
    pub ttl_ms: u64,
    /// Simulated time the run lasts.
    pub duration_ms: u64,
    /// One-way latency to the bucket, drawn per request and per response.
    pub store_latency_ms: (u64, u64),
    /// Keep a human-readable line for every event and effect.
    pub trace: bool,
}

impl SimConfig {
    pub fn calm() -> Self {
        Self {
            nodes: 3,
            ttl_ms: 3_000,
            duration_ms: 30_000,
            store_latency_ms: (2, 20),
            trace: false,
        }
    }
}

/// What a run produced.
#[derive(Debug)]
pub struct Outcome {
    pub seed: u64,
    /// A hash of every event and effect, in order, with its time. Two runs
    /// of one seed must produce the same hash.
    pub trace_hash: u64,
    pub trace: Vec<String>,
    pub events: u64,
    /// Nodes that halted (self-fenced). A calm run has none.
    pub halts: Vec<String>,
    /// Nodes that held a live node lease when the run ended.
    pub authoritative: Vec<String>,
    /// A core panic or a failed `State::validate`, with its time and node.
    pub panic: Option<String>,
}

pub fn run(seed: u64, config: &SimConfig) -> Outcome {
    world::World::new(seed, config.clone()).run()
}
