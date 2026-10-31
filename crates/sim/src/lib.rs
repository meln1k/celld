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
    /// Faults happen only before this instant.
    pub chaos_ms: u64,
    pub faults: Faults,
    /// One-way latency to the bucket, drawn per request and per response.
    pub store_latency_ms: (u64, u64),
    /// Gap between client requests across the whole fleet.
    pub request_gap_ms: (u64, u64),
    /// Share of requests whose handler commits a write.
    pub write_probability: f64,
    /// The core's residency cap: residents plus cells being activated.
    pub max_resident: usize,
    /// Cold activations one node runs at once.
    pub max_activations: usize,
    /// An evicted cell keeps its ownership record (`Sticky`) instead of
    /// being released to the fleet.
    pub sticky_eviction: bool,
    /// Give back a cell nobody has used for this long.
    pub idle_evict_ms: Option<u64>,
    /// A bug planted in the simulated shell or bucket.
    pub mutation: Option<Mutation>,
    /// Keep a human-readable line for every event and effect.
    pub trace: bool,
}

/// A known bug planted in the simulated shell or bucket, never in the core.
/// Each one breaks a promise the core relies on, so the check that guards
/// the matching property must fire. A check that stays quiet under its
/// mutation proves nothing when it stays quiet on real code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mutation {
    /// The bucket applies every owner write whatever its guard, so two
    /// nodes can both win one epoch. Caught by `two-writers`.
    IgnoreCasGuard,
    /// An owner write answers `Applied` but never reaches the bucket. Caught
    /// by `unclaimed-runtime`.
    PhantomClaim,
    /// `Publish` answers success without registering the runtime. Caught by
    /// `local-without-runtime`.
    SkipPublish,
    /// `AwaitDurable` answers at once, before any upload. Caught by
    /// `ack-before-durable` and `read-before-durable`.
    AckBeforeUpload,
    /// A restore reads the prefix below the newest one, missing the last
    /// owner's writes. Caught by `lost-write`.
    StaleRestore,
    /// A read answers without telling the gate what it saw, so it can
    /// reveal writes that are not durable. Caught by `read-before-durable`.
    ReadsBypassGate,
    /// `VerifyOwnership` answers `Ok` without reading the owner record, so a
    /// node that lost the cell still acknowledges into its old epoch.
    /// Caught by `late-ack`.
    BlindVerify,
    /// The bucket rejects every owner write: nothing is unsafe, but no cell
    /// is ever served. Caught by `liveness`.
    RejectClaims,
}

impl Mutation {
    pub const ALL: [Mutation; 8] = [
        Mutation::IgnoreCasGuard,
        Mutation::PhantomClaim,
        Mutation::SkipPublish,
        Mutation::AckBeforeUpload,
        Mutation::StaleRestore,
        Mutation::ReadsBypassGate,
        Mutation::BlindVerify,
        Mutation::RejectClaims,
    ];
}

/// What goes wrong, and how often. Rates are per simulated minute across
/// the whole fleet; zero turns a fault off.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    pub crashes: f64,
    /// Chance that a crash also loses the node's disk.
    pub wipe_probability: f64,
    /// A process stops running for a while, then carries on.
    pub pauses: f64,
    /// Longest pause. Longer than a TTL lets a peer take cells over while
    /// the paused owner still believes it holds them.
    pub max_pause_ms: u64,
    /// Chance that a pause is a VM suspend, during which the process's
    /// monotonic clock stops too.
    pub suspend_probability: f64,
    /// A node loses the network: the bucket and its peers.
    pub partitions: f64,
    pub max_partition_ms: u64,
    /// A process's wall clock jumps, either way, by up to the maximum.
    pub clock_steps: f64,
    pub max_clock_step_ms: u64,
    /// Each node's wall clock is off by a fixed amount, up to this either
    /// way.
    pub max_skew_ms: u64,
    /// Chance per bucket call: it fails and did not apply; its request was
    /// lost; or it applied and its response was lost.
    pub store_definite: f64,
    pub store_lost_request: f64,
    pub store_lost_response: f64,
    /// Chance per hop of a latency spike of up to `store_spike_ms`.
    pub store_spike_probability: f64,
    pub store_spike_ms: u64,
}

impl SimConfig {
    pub fn calm() -> Self {
        Self {
            nodes: 3,
            cells: 6,
            ttl_ms: 3_000,
            duration_ms: 30_000,
            chaos_ms: 0,
            faults: Faults::default(),
            store_latency_ms: (2, 20),
            request_gap_ms: (5, 60),
            write_probability: 0.6,
            max_resident: 4,
            max_activations: 4,
            sticky_eviction: false,
            idle_evict_ms: None,
            mutation: None,
            trace: false,
        }
    }

    /// Processes crash `per_minute` times a minute for the first 20 s, a
    /// third of them losing their disk. The run then goes on undisturbed
    /// long enough for every restart and takeover to finish.
    pub fn crashing(per_minute: f64) -> Self {
        Self {
            duration_ms: 45_000,
            chaos_ms: 20_000,
            faults: Faults {
                crashes: per_minute,
                wipe_probability: 0.3,
                ..Faults::default()
            },
            ..Self::calm()
        }
    }

    /// Every fault at once for the first 20 s.
    pub fn chaos() -> Self {
        let ttl_ms = Self::calm().ttl_ms;
        Self {
            faults: Faults {
                crashes: 3.0,
                wipe_probability: 0.3,
                pauses: 4.0,
                max_pause_ms: 2 * ttl_ms,
                suspend_probability: 0.3,
                partitions: 3.0,
                max_partition_ms: 2 * ttl_ms,
                clock_steps: 2.0,
                max_clock_step_ms: 1_000,
                max_skew_ms: 300,
                store_definite: 0.01,
                store_lost_request: 0.01,
                store_lost_response: 0.01,
                store_spike_probability: 0.02,
                store_spike_ms: 3_000,
            },
            ..Self::crashing(3.0)
        }
    }

    /// One configuration drawn from the seed (swarm testing). Each seed
    /// turns on a different subset of faults at different strengths, and
    /// sizes the fleet and the core differently, so a sweep reaches
    /// combinations that one fixed mix would rarely produce: a run with
    /// only partitions finds what a run full of crashes hides.
    ///
    /// The draw uses its own stream, so the run itself replays exactly as
    /// it would with the same config passed by hand.
    pub fn swarm(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x5357_4152_4d00_0000);
        let r = &mut rng;
        // A fault rate, or zero: each fault is on in about half the seeds.
        let maybe = |r: &mut Rng, lo: u64, hi: u64| {
            let rate = r.range(lo, hi) as f64;
            if r.chance(0.5) {
                rate
            } else {
                0.0
            }
        };
        let either = |r: &mut Rng, p: f64| if r.chance(0.5) { p } else { 0.0 };
        let ttl_ms = *r.pick(&[2_000, 3_000, 5_000, 10_000]).unwrap();
        let faults = Faults {
            crashes: maybe(r, 1, 6),
            wipe_probability: either(r, 0.3),
            pauses: maybe(r, 1, 8),
            max_pause_ms: ttl_ms * r.range(1, 3),
            suspend_probability: either(r, 0.5),
            partitions: maybe(r, 1, 6),
            max_partition_ms: ttl_ms * r.range(1, 3),
            clock_steps: maybe(r, 1, 4),
            max_clock_step_ms: r.range(100, 3_000),
            max_skew_ms: *r.pick(&[0, 50, 300]).unwrap(),
            store_definite: either(r, 0.02),
            store_lost_request: either(r, 0.02),
            store_lost_response: either(r, 0.03),
            store_spike_probability: either(r, 0.02),
            store_spike_ms: r.range(500, 4_000),
        };
        Self {
            nodes: r.range(2, 5) as usize,
            cells: r.range(2, 10) as usize,
            ttl_ms,
            duration_ms: 45_000,
            chaos_ms: 20_000,
            faults,
            store_latency_ms: (1, *r.pick(&[10, 40, 120]).unwrap()),
            request_gap_ms: (2, r.range(20, 120)),
            write_probability: 0.6,
            max_resident: r.range(2, 8) as usize,
            max_activations: r.range(1, 4) as usize,
            sticky_eviction: r.chance(0.5),
            idle_evict_ms: r.chance(0.3).then(|| r.range(500, 5_000)),
            mutation: None,
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
    /// Forwards that came back without running: stale, full, unreachable.
    pub bounced: u64,
    pub crashes: u64,
    pub pauses: u64,
    pub partitions: u64,
    /// Bucket calls that failed, timed out or lost their answer.
    pub store_failures: u64,
    /// Databases restored from the bucket: each one is checked for lost
    /// writes.
    pub restored: u64,
}

/// What a run produced.
#[derive(Debug)]
pub struct Outcome {
    pub seed: u64,
    /// A hash of every event and effect, in order, with its time. Two runs
    /// of one seed must produce the same hash.
    pub trace_hash: u64,
    pub trace: Vec<String>,
    /// Processes that halted (self-fenced), as `node#incarnation`. A calm
    /// run has none.
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
