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
pub mod coverage;
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
    /// Faults begin only before this instant. A clock step, like a node's
    /// skew, lasts: the clock it moved stays where it put it.
    pub chaos_ms: u64,
    pub faults: Faults,
    /// One-way latency to the bucket, drawn per request and per response.
    pub store_latency_ms: (u64, u64),
    /// Gap between client requests across the whole fleet.
    pub request_gap_ms: (u64, u64),
    /// Share of requests whose handler commits a write.
    pub write_probability: f64,
    /// Share of requests whose client gives up before it hears back.
    pub cancel_probability: f64,
    /// Share of requests whose handler upgrades them to a WebSocket.
    pub websocket_probability: f64,
    /// Share of handlers that call out to a third party mid-request.
    pub effect_probability: f64,
    /// Share of handlers that throw inside their turn, after any write they
    /// made. The error answer can quote what the handler read, so it
    /// reveals the cell's state as a success does (`FailedInTurn`).
    pub error_probability: f64,
    /// Share of writes that also set the cell's alarm, or clear it.
    pub alarm_probability: f64,
    /// Share of client sends that are top-level Worker requests, for no
    /// cell.
    pub worker_probability: f64,
    /// What the operator and the rest of each node do unprompted.
    pub operator: Operator,
    /// The core's residency cap: residents plus cells being activated.
    pub max_resident: usize,
    /// Cold activations one node runs at once.
    pub max_activations: usize,
    /// An evicted cell keeps its ownership record (`Sticky`) instead of
    /// being released to the fleet.
    pub sticky_eviction: bool,
    /// Give back a cell nobody has used for this long.
    pub idle_evict_ms: Option<u64>,
    /// The fleet posture (`CELLD_DURABILITY=fleet`): each process ships its
    /// writes to followers on other machines and acknowledges once they all
    /// hold them, and flushes to the bucket only every couple of seconds.
    pub fleet: bool,
    /// The memory each process holds, and the machine it has. `None`
    /// reports no memory, so the core never sheds on pressure.
    pub memory: Option<Memory>,
    /// A bug planted in the simulated shell or bucket.
    pub mutation: Option<Mutation>,
    /// Keep a human-readable line for every event and effect.
    pub trace: bool,
}

/// What a process's memory costs, in bytes. Its in-use memory is `base`
/// plus `cell` for every database it holds open: a cell's runtime and its
/// heap. The machine has `total`, so the core's ceiling is 80% of it and its
/// absolute cap 95% (`PressureConfig::from_limits`). A process's RSS is its
/// in-use memory: no allocator slack, no cgroup and no containers.
#[derive(Clone, Copy, Debug)]
pub struct Memory {
    pub total: u64,
    pub base: u64,
    pub cell: u64,
}

impl Memory {
    const MB: u64 = 1 << 20;

    /// A machine whose ceiling a process crosses with somewhat more than
    /// `room` cells open, and whose shedding brings it back to `room`.
    pub fn with_room(room: u64) -> Self {
        let (base, cell) = (256 * Self::MB, 128 * Self::MB);
        // The low watermark, 80% of the ceiling, sits half a cell above
        // `room` cells.
        let low = base + cell * room + cell / 2;
        let ceiling = low * 5 / 4;
        Self {
            total: ceiling * 5 / 4,
            base,
            cell,
        }
    }
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
    /// Caught by `unverified-ack`, and by `late-ack` once a successor has
    /// opened.
    BlindVerify,
    /// The bucket rejects every owner write: nothing is unsafe, but no cell
    /// is ever served. Caught by `liveness`.
    RejectClaims,
    /// A restore loses the first write of its chain: a hole no database
    /// ever held. Caught by `torn-restore`.
    TornRestore,
    /// An owner write keeps the epoch the record already had. Caught by
    /// `epoch-reused`.
    StaleEpoch,
    /// Each process's monotonic clock runs at a quarter speed, so its core
    /// believes in its node lease long after peers think it expired. Caught
    /// by `stale-read`.
    SlowMonoClock,
    /// A WebSocket the core asks to close is reported closed but stays
    /// open. Caught by `evicted-pinned` when a drain then evicts its cell.
    PhantomSocketClose,
    /// The shell never tells the core about a cell's alarm: every report
    /// and every restore says there is none. Caught by `alarm-not-fired`.
    AlarmsUnreported,
    /// A follower confirms as many writes as it was sent, but fsyncs a
    /// different last write. Caught by `unverified-ack`, which compares
    /// what the fleet holds write for write, not only its length.
    FollowerCorrupts,
    /// A handler's error answer goes straight to its client, past the
    /// output gate, as it did before `FailedInTurn`. Caught by
    /// `read-before-durable`.
    ErrorsBypassGate,
    /// A wake entry write answers success but writes nothing. Caught by
    /// `uncovered-alarm` when the cell is then evicted, and by
    /// `crash-uncovered-alarm` when its process dies.
    LostWakeEntry,
    /// An eviction stops the runtime but leaves it in the registry. Caught
    /// by `evicted-but-serving`.
    StaleRegistry,
    /// `RecoverNodeLog` answers at once without recovering anything, so a
    /// takeover restores a bucket that lacks the dead owner's fleet-acked
    /// writes. Caught by `lost-write`.
    SkipLogRecovery,
    /// A follower accepts appends after recovery sealed it, so a stale
    /// owner can still prove writes through the fleet. Caught by
    /// `late-ack`.
    FollowerIgnoresSeal,
}

impl Mutation {
    pub const ALL: [Mutation; 19] = [
        Mutation::IgnoreCasGuard,
        Mutation::PhantomClaim,
        Mutation::SkipPublish,
        Mutation::AckBeforeUpload,
        Mutation::StaleRestore,
        Mutation::ReadsBypassGate,
        Mutation::BlindVerify,
        Mutation::RejectClaims,
        Mutation::TornRestore,
        Mutation::StaleEpoch,
        Mutation::SlowMonoClock,
        Mutation::StaleRegistry,
        Mutation::PhantomSocketClose,
        Mutation::LostWakeEntry,
        Mutation::AlarmsUnreported,
        Mutation::SkipLogRecovery,
        Mutation::FollowerIgnoresSeal,
        Mutation::ErrorsBypassGate,
        Mutation::FollowerCorrupts,
    ];
}

/// What happens to a node besides its requests and its faults, and how
/// often. Rates are per simulated minute across the whole fleet; zero turns
/// one off.
#[derive(Clone, Debug, Default)]
pub struct Operator {
    /// An operator asks a node to evict a cell (`POST /evict/<cell>`).
    pub evictions: f64,
    /// The node's log tier asks its core to renew the lease now
    /// (`Message::NudgeNodeLease`).
    pub nudges: f64,
    /// An operator shuts a node down gracefully: it hands every cell to a
    /// peer, exits, and is restarted.
    pub drains: f64,
    /// An operator replaces a node's process in place: the replacement
    /// resumes its databases where they are.
    pub preserves: f64,
    /// The balancer asks a node to give a few idle cells to emptier peers.
    pub rebalances: f64,
    /// A node adopts a new application generation, and moves its resident
    /// cells onto it.
    pub deploys: f64,
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
            cancel_probability: 0.0,
            websocket_probability: 0.02,
            effect_probability: 0.1,
            error_probability: 0.0,
            worker_probability: 0.1,
            alarm_probability: 0.05,
            operator: Operator {
                evictions: 30.0,
                nudges: 6.0,
                // A drain fails the requests it cuts off; the calm fleet
                // answers every one.
                drains: 0.0,
                preserves: 0.0,
                rebalances: 6.0,
                // A forced swap cancels the requests it cuts off.
                deploys: 0.0,
            },
            max_resident: 4,
            max_activations: 4,
            sticky_eviction: false,
            idle_evict_ms: None,
            fleet: false,
            memory: None,
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
            cancel_probability: 0.05,
            error_probability: 0.05,
            // Rare: a reset of a cell holding a socket trips a known core
            // invariant, and ends the run.
            websocket_probability: 0.002,
            operator: Operator {
                drains: 2.0,
                preserves: 2.0,
                deploys: 2.0,
                ..Self::calm().operator
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
        let ttl_ms = *r.pick(&[2_000, 3_000, 5_000, 10_000]).unwrap();
        let faults = Faults::draw(r, ttl_ms);
        Self {
            nodes: r.range(2, 5) as usize,
            cells: r.range(2, 10) as usize,
            ttl_ms,
            faults,
            store_latency_ms: (1, *r.pick(&[10, 40, 120]).unwrap()),
            request_gap_ms: (2, r.range(20, 120)),
            max_resident: r.range(2, 8) as usize,
            max_activations: r.range(1, 4) as usize,
            sticky_eviction: r.chance(0.5),
            idle_evict_ms: r.chance(0.3).then(|| r.range(500, 5_000)),
            // Drawn last, so the draws above keep the shapes they had.
            cancel_probability: *r.pick(&[0.0, 0.02, 0.1]).unwrap(),
            // A quarter of the seeds: see `chaos`.
            websocket_probability: *r.pick(&[0.0, 0.0, 0.0, 0.005]).unwrap(),
            effect_probability: *r.pick(&[0.0, 0.1, 0.5]).unwrap(),
            worker_probability: *r.pick(&[0.0, 0.1, 0.3]).unwrap(),
            alarm_probability: *r.pick(&[0.0, 0.05, 0.2]).unwrap(),
            operator: Operator::draw(r),
            fleet: r.chance(0.5),
            memory: r.chance(0.5).then(|| Memory::with_room(r.range(1, 4))),
            error_probability: *r.pick(&[0.0, 0.05, 0.2]).unwrap(),
            // The same 20 s of faults and 25 s of calm as `crashing`.
            ..Self::crashing(0.0)
        }
    }
}

impl Faults {
    /// A fault mix for one swarm seed: each fault on in about half the
    /// seeds, at a drawn strength. Pauses and partitions last up to three
    /// lease lifetimes, long enough for a peer to take cells over.
    fn draw(r: &mut Rng, ttl_ms: u64) -> Self {
        let maybe = |r: &mut Rng, lo: u64, hi: u64| {
            let rate = r.range(lo, hi) as f64;
            if r.chance(0.5) {
                rate
            } else {
                0.0
            }
        };
        let either = |r: &mut Rng, p: f64| if r.chance(0.5) { p } else { 0.0 };
        Self {
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
        }
    }
}

impl Operator {
    /// An operator for one swarm seed: each act on in about half the
    /// seeds, at a drawn rate.
    fn draw(r: &mut Rng) -> Self {
        let mut maybe = |lo: u64, hi: u64| {
            let rate = r.range(lo, hi) as f64;
            if r.chance(0.5) {
                rate
            } else {
                0.0
            }
        };
        Self {
            evictions: maybe(1, 30),
            nudges: maybe(1, 30),
            drains: maybe(1, 4),
            rebalances: maybe(1, 30),
            preserves: maybe(1, 4),
            deploys: maybe(1, 6),
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
    /// Requests whose client gave up first.
    pub cancelled: u64,
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
    /// Evictions an operator asked for, and those that evicted.
    pub evictions: u64,
    pub evicted: u64,
    /// Graceful shutdowns, and cells a peer acquired from their donor.
    pub drains: u64,
    pub handed_off: u64,
    /// Planned replacements, and databases a replacement resumed in place.
    pub preserves: u64,
    pub resumed: u64,
    /// WebSockets handlers opened.
    pub sockets: u64,
    /// New application generations nodes adopted.
    pub deploys: u64,
    /// Handlers' calls to third parties the gate let out.
    pub effects: u64,
    /// Worker requests, and those that ran in a resident cell's isolate.
    pub workers: u64,
    pub workers_resident: u64,
    /// Alarms handlers set, alarms that fired and were consumed, handler
    /// commits the gate proved, and the wake hints scans sent.
    pub alarms_set: u64,
    pub alarms_fired: u64,
    pub alarms_proven: u64,
    pub wake_hints: u64,
    /// Durability proofs the fleet gave, logs recovered after their leader
    /// died, and acknowledged writes lost to a declared bounded loss: every
    /// follower of a session gone with its disk.
    pub fleet_proofs: u64,
    pub log_recoveries: u64,
    pub fleet_losses: u64,
    /// Handlers' error answers that reached their clients.
    pub errors: u64,
    /// Load samples after which a node was shedding on memory pressure.
    pub shedding_samples: u64,
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
    /// Hits on properties that are watched, not enforced: they never fail
    /// a run. See `checks.rs`.
    pub watched: Vec<Violation>,
    pub stats: Stats,
    /// The phase transitions the cores took (`coverage.rs`).
    pub coverage: coverage::Coverage,
}

impl Outcome {
    pub fn passed(&self) -> bool {
        self.panic.is_none() && self.violations.is_empty()
    }
}

pub fn run(seed: u64, config: &SimConfig) -> Outcome {
    world::World::new(seed, config.clone()).run()
}
