//! Deterministic simulation of the celld coordination core.
//!
//! `celld-logic` decides everything and performs nothing: every input is an
//! [`celld_logic::Event`] and every output an [`celld_logic::Effect`]. This
//! crate runs several of those cores as a fleet in one thread, on one
//! simulated clock, against one simulated bucket, and carries out their
//! effects the way the production shell does. A seed fixes the whole
//! schedule: store latency and loss, crashes, pauses, partitions, clock
//! steps and the client workload. A failing seed replays exactly.
//!
//! What it checks:
//!
//! - **One writer per epoch.** No two processes run a runtime for, or
//!   upload into, the same cell epoch.
//! - **No runtime without a claim.** A process starts a runtime only at an
//!   epoch its own ownership compare-and-swap installed.
//! - **No lost acknowledged write.** Every restore at epoch `E` contains every
//!   write acknowledged at an epoch below `E`.
//! - **No acknowledgement before durability.** An acknowledged write is in
//!   its epoch's bucket prefix at the instant it is acknowledged.
//! - **No local route without a runtime.** A request routed locally finds a
//!   published runtime.
//! - **Core invariants.** `State::validate` after every event.
//! - **Liveness.** After the faults heal, every cell serves a write, and a
//!   fresh node that restores every cell from the bucket alone serves it too.
//!
//! [`Mutation`] plants a known bug in the simulated shell or bucket, so the
//! tests can show each checker catches the failure it exists for.
//!
//! V8 is not part of the simulation. A cell is a scripted handler that
//! commits writes, runs for a while, and answers through the output gate.

mod rng;
mod store;
mod world;

pub use rng::Rng;

pub type ProcId = u64;
pub type WriteId = u64;

/// A known bug planted in the simulated shell or bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Mutation {
    None,
    /// `VerifyOwnership` answers `Ok` without reading the record, so a
    /// bucket proof acknowledges into an orphaned epoch.
    SkipVerifyOwnership,
    /// Restore reuses the newest local eviction snapshot below the epoch,
    /// whichever epoch it is from and whether or not the cell was taken over.
    /// Another node may have written the cell in between.
    ReuseOlderSnapshot,
    /// Every epoch of a cell uploads into one prefix, so a stale owner's late
    /// upload overwrites its successor's lineage.
    SharedPrefix,
    /// The bucket ignores conditional-write guards.
    IgnoreCasGuard,
    /// `AwaitDurable` reports the position durable before any upload.
    AckBeforeUpload,
}

impl Mutation {
    pub const ALL: [Mutation; 5] = [
        Mutation::SkipVerifyOwnership,
        Mutation::ReuseOlderSnapshot,
        Mutation::SharedPrefix,
        Mutation::IgnoreCasGuard,
        Mutation::AckBeforeUpload,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "none" => Mutation::None,
            "skip-verify-ownership" => Mutation::SkipVerifyOwnership,
            "reuse-older-snapshot" => Mutation::ReuseOlderSnapshot,
            "shared-prefix" => Mutation::SharedPrefix,
            "ignore-cas-guard" => Mutation::IgnoreCasGuard,
            "ack-before-upload" => Mutation::AckBeforeUpload,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Mutation::None => "none",
            Mutation::SkipVerifyOwnership => "skip-verify-ownership",
            Mutation::ReuseOlderSnapshot => "reuse-older-snapshot",
            Mutation::SharedPrefix => "shared-prefix",
            Mutation::IgnoreCasGuard => "ignore-cas-guard",
            Mutation::AckBeforeUpload => "ack-before-upload",
        }
    }
}

/// What one run varies. [`SimConfig::swarm`] draws a different mix per seed.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub cells: usize,
    pub ttl_ms: u64,
    /// How long faults are injected. Workload runs through it and past it.
    pub chaos_ms: u64,
    /// Quiet time after the faults heal, before the liveness probe.
    pub settle_ms: u64,
    pub request_gap_ms: (u64, u64),
    pub write_probability: f64,
    pub max_resident: usize,
    pub max_activations: usize,
    pub sticky_eviction: bool,
    pub idle_evict_ms: Option<u64>,
    pub evict_gap_ms: Option<(u64, u64)>,
    pub faults: Faults,
    pub mutation: Mutation,
    /// Keep a human-readable trace of every event and effect.
    pub trace: bool,
}

/// Fault rates, per simulated minute across the whole fleet, and the bucket's
/// per-operation loss.
#[derive(Clone, Debug)]
pub struct Faults {
    pub crashes: f64,
    pub wipe_probability: f64,
    pub pauses: f64,
    /// Longest pause. Longer than a TTL lets a peer take cells over while
    /// the paused owner still believes it holds them.
    pub max_pause_ms: u64,
    pub suspend_probability: f64,
    pub partitions: f64,
    pub max_partition_ms: u64,
    pub clock_steps: f64,
    pub max_clock_step_ms: u64,
    /// Fixed per-node wall clock offset, up to this much either way.
    pub max_skew_ms: u64,
    pub store_definite: f64,
    pub store_lost_request: f64,
    pub store_lost_response: f64,
    pub store_latency_ms: (u64, u64),
    pub store_spike_probability: f64,
    pub store_spike_ms: u64,
    pub store_timeout_ms: u64,
}

impl Faults {
    pub fn none() -> Self {
        Self {
            crashes: 0.0,
            wipe_probability: 0.0,
            pauses: 0.0,
            max_pause_ms: 0,
            suspend_probability: 0.0,
            partitions: 0.0,
            max_partition_ms: 0,
            clock_steps: 0.0,
            max_clock_step_ms: 0,
            max_skew_ms: 0,
            store_definite: 0.0,
            store_lost_request: 0.0,
            store_lost_response: 0.0,
            store_latency_ms: (2, 20),
            store_spike_probability: 0.0,
            store_spike_ms: 0,
            store_timeout_ms: 5_000,
        }
    }
}

impl SimConfig {
    /// A small fleet with no faults: the baseline every fault run builds on.
    pub fn calm() -> Self {
        Self {
            nodes: 3,
            cells: 6,
            ttl_ms: 3_000,
            chaos_ms: 30_000,
            settle_ms: 12_000,
            request_gap_ms: (5, 60),
            write_probability: 0.6,
            max_resident: 4,
            max_activations: 4,
            sticky_eviction: true,
            idle_evict_ms: None,
            evict_gap_ms: Some((200, 2_000)),
            faults: Faults::none(),
            mutation: Mutation::None,
            trace: false,
        }
    }

    /// Draw one configuration from the seed. Each run enables a different
    /// subset of faults and sizes, so the seed space covers combinations a
    /// single fixed mix would rarely reach (swarm testing).
    pub fn swarm(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x5157_4152_4d00_0000);
        let on = |rng: &mut Rng, p: f64, lo: u64, hi: u64| {
            let rate = rng.range(lo, hi) as f64;
            if rng.chance(p) {
                rate
            } else {
                0.0
            }
        };
        let ttl_ms = *rng.pick(&[2_000, 3_000, 5_000, 10_000]).unwrap();
        let faults = Faults {
            crashes: on(&mut rng, 0.6, 1, 6),
            wipe_probability: if rng.chance(0.5) { 0.3 } else { 0.0 },
            pauses: on(&mut rng, 0.6, 1, 8),
            max_pause_ms: ttl_ms * rng.range(1, 4),
            suspend_probability: if rng.chance(0.5) { 0.5 } else { 0.0 },
            partitions: on(&mut rng, 0.5, 1, 6),
            max_partition_ms: ttl_ms * rng.range(1, 3),
            clock_steps: on(&mut rng, 0.3, 1, 4),
            max_clock_step_ms: rng.range(100, 3_000),
            max_skew_ms: *rng.pick(&[0, 50, 300]).unwrap(),
            store_definite: if rng.chance(0.5) { 0.02 } else { 0.0 },
            store_lost_request: if rng.chance(0.5) { 0.02 } else { 0.0 },
            store_lost_response: if rng.chance(0.5) { 0.03 } else { 0.0 },
            store_latency_ms: (1, *rng.pick(&[10, 40, 120]).unwrap()),
            store_spike_probability: if rng.chance(0.5) { 0.01 } else { 0.0 },
            store_spike_ms: rng.range(500, 4_000),
            store_timeout_ms: rng.range(1_000, 6_000),
        };
        Self {
            nodes: rng.range(2, 5) as usize,
            cells: rng.range(2, 10) as usize,
            ttl_ms,
            chaos_ms: 40_000,
            settle_ms: 4 * ttl_ms,
            request_gap_ms: (2, rng.range(20, 120)),
            write_probability: 0.6,
            max_resident: rng.range(2, 8) as usize,
            max_activations: rng.range(1, 4) as usize,
            sticky_eviction: rng.chance(0.5),
            idle_evict_ms: if rng.chance(0.3) {
                Some(rng.range(500, 5_000))
            } else {
                None
            },
            evict_gap_ms: if rng.chance(0.7) {
                Some((50, rng.range(200, 3_000)))
            } else {
                None
            },
            faults,
            mutation: Mutation::None,
            trace: false,
        }
    }
}

/// One checker's finding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub at_ms: u64,
    pub kind: &'static str,
    pub detail: String,
}

/// Counters that show what a run exercised. A checker that never sees the
/// situation it guards proves nothing, so the tests read these too.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub events: u64,
    pub requests: u64,
    pub local_routes: u64,
    pub remote_routes: u64,
    pub route_errors: u64,
    pub writes: u64,
    pub acked_writes: u64,
    pub refused_outputs: u64,
    pub restores_fresh: u64,
    pub restores_local: u64,
    pub restores_bucket: u64,
    pub takeovers: u64,
    pub halts: u64,
    pub crashes: u64,
    pub pauses: u64,
    pub partitions: u64,
    pub evictions_requested: u64,
    pub stale_uploads: u64,
    /// Publishes that found the cell already published by another process.
    pub concurrent_serving: u64,
}

impl Stats {
    /// Add another run's counters to these.
    pub fn absorb(&mut self, other: &Stats) {
        let Stats {
            events,
            requests,
            local_routes,
            remote_routes,
            route_errors,
            writes,
            acked_writes,
            refused_outputs,
            restores_fresh,
            restores_local,
            restores_bucket,
            takeovers,
            halts,
            crashes,
            pauses,
            partitions,
            evictions_requested,
            stale_uploads,
            concurrent_serving,
        } = other;
        self.events += events;
        self.requests += requests;
        self.local_routes += local_routes;
        self.remote_routes += remote_routes;
        self.route_errors += route_errors;
        self.writes += writes;
        self.acked_writes += acked_writes;
        self.refused_outputs += refused_outputs;
        self.restores_fresh += restores_fresh;
        self.restores_local += restores_local;
        self.restores_bucket += restores_bucket;
        self.takeovers += takeovers;
        self.halts += halts;
        self.crashes += crashes;
        self.pauses += pauses;
        self.partitions += partitions;
        self.evictions_requested += evictions_requested;
        self.stale_uploads += stale_uploads;
        self.concurrent_serving += concurrent_serving;
    }
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub seed: u64,
    pub violations: Vec<Violation>,
    pub stats: Stats,
    /// A digest of every event and effect in order. Equal seeds and
    /// configurations must produce equal digests.
    pub trace_hash: u64,
    pub trace: Vec<String>,
    /// The core panicked: `State::validate` or an internal assertion failed.
    pub panic: Option<String>,
}

impl Outcome {
    pub fn failed(&self) -> bool {
        self.panic.is_some() || !self.violations.is_empty()
    }
}

/// Run one seed under `config`.
pub fn run(seed: u64, config: &SimConfig) -> Outcome {
    world::run(seed, config)
}
