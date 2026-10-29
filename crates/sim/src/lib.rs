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
//! Half the swarm runs in the fleet posture (`CELLD_DURABILITY=fleet`):
//! each process ships its writes to up to two followers, acknowledges on
//! their fsyncs, bundles the same rows to the bucket, and a takeover of a
//! dead session's cells recovers its log first.
//!
//! Half the swarm also stops nodes on purpose ([`Lifecycle`]). A drain
//! claims the fleet's drain token, sends `ReleaseAll`, and hands each cell
//! to a peer, which claims it before it answers. A preserve keeps the
//! owner records and leaves a clean-reload certificate that the next
//! process on the node resumes from. Nodes also give idle cells to peers
//! below their share of the fleet.
//!
//! Half the swarm also stops cells the way production does ([`Stops`]).
//! An eviction captures its snapshot before its final upload, and a
//! request during the capture abandons it. Hosts wedge and refuse to
//! release, and the operator's evictions wait for their answers. Half
//! deploys new generations, which swap resident cells to fresh isolates at
//! the same epoch on the open database ([`Deploys`]). Half runs work on
//! resident runtimes besides routed requests ([`ResidentWork`]): WebSockets
//! that pin or hibernate with their cell, output gates, and top-level
//! Worker requests. Half the swarm's owners delete superseded epoch
//! prefixes (`epoch_gc`).
//!
//! What it checks:
//!
//! - **One writer per epoch.** No two processes run a runtime for, or
//!   upload into, the same cell epoch.
//! - **No runtime without a claim.** A process starts a runtime only at an
//!   epoch its own ownership compare-and-swap installed. A clean reload
//!   continues its predecessor's claims and its writer.
//! - **No handoff without a claim.** A successor accepts a cell only at an
//!   epoch a compare-and-swap installed for its node.
//! - **A finished drain owns nothing.** A node that reports its drain done
//!   holds no owner record its core still holds the cell at.
//! - **No lost acknowledged write.** Every restore at epoch `E` contains every
//!   write acknowledged at an epoch below `E`, and a clean reload that
//!   resumes `E` holds every write acknowledged at `E` before it.
//! - **No acknowledgement before durability.** An acknowledged write is in
//!   its epoch's bucket prefix at the instant it is acknowledged, or in a
//!   bundle its session's recovery will read, or fsynced by every follower
//!   of its session's published log record.
//! - **No local route without a runtime.** A request routed locally finds a
//!   published runtime.
//! - **An eviction answers once, and truly.** Every operator eviction is
//!   answered, and one answered `Evicted` or `AlreadyAbsent` leaves no
//!   runtime of the cell on its process.
//! - **Every resident cell reaches its node's generation.** Once a deploy is
//!   older than its maximum age and the faults have healed, no resident
//!   runtime still runs an older generation.
//! - **No runtime stops under its own work.** An eviction or a swap never
//!   stops a runtime that a regular or outbound socket, or a Worker request
//!   the core routed to it, still holds.
//! - **Core invariants.** `State::validate` after every event.
//! - **An acknowledged alarm stays discoverable.** Whenever the alarm a
//!   restore would load was armed by an acknowledged write, the bucket holds
//!   a wake entry for the cell that its retirement record leaves standing.
//! - **A proven alarm never runs again on its owner.** Alarms are
//!   at-least-once: a run whose consuming commit was never proven may repeat
//!   on a later owner, and the simulator counts those. Once a run's commit is
//!   verified durable at its epoch, no current owner runs that alarm
//!   installation again. A superseded owner that still trusts its lease (a
//!   suspended VM, whose monotonic clock stood still) can, and those runs are
//!   counted as `stale_alarm_runs`.
//! - **Liveness.** After the faults heal, every cell serves a write, and a
//!   fresh node that restores every cell from the bucket alone fires every
//!   overdue alarm the bucket can find, with no request to wake its cell,
//!   and then serves every cell.
//!
//! Recovery may declare a loss when every follower of a log epoch is
//! conclusively gone. A fleet-acknowledged write that no disk or bucket
//! object holds any more is beyond any protocol and is counted as
//! `fleet_losses_excused`; one a surviving disk still holds is a lost write.
//!
//! [`Mutation`] plants a known bug in the simulated shell or bucket, so the
//! tests can show each checker catches the failure it exists for.
//!
//! V8 is not part of the simulation. A cell is a scripted handler that
//! commits writes, sets or deletes its alarm, runs for a while, and answers
//! through the output gate; its alarm handler consumes the alarm, re-arms
//! it, or fails into the retry backoff.

mod coverage;
mod known;
mod rng;
mod store;
mod world;

pub use coverage::Coverage;
pub use known::{KnownFailures, Posture, Verdict};
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
    /// An event that armed an alarm answers without waiting for the alarm's
    /// wake-entry PUT.
    AckBeforeWakeEntry,
    /// Wake maintenance retires older entries without first proving the new
    /// alarm state durable or reading the owner record.
    RetireUnproven,
    /// The elected waker hints only cells its own node owns, so nothing
    /// brings a dead owner's cell back for its alarm.
    NoOrphanWake,
    /// Wake maintenance never tells the core it published an entry, so a
    /// restored alarm pins its cell until some request finishes.
    SilentWakeCoverage,
    /// A fleet proof needs one follower's confirmation instead of every
    /// member's.
    AckOneFollower,
    /// `RecoverNodeLog` answers success without recovering, so a takeover
    /// restores from the per-cell prefixes alone.
    SkipLogRecovery,
    /// A follower accepts appends at or below its seal mark, so a fenced
    /// leader keeps completing fleet proofs after recovery cut its log.
    AppendAfterSeal,
    /// The graceful seal skips the retained bundle scan, so a sealed record
    /// orphans the acknowledged rows of a cell that left without uploading.
    SealOverBundles,
    /// A clean reload keeps the live database of every cell the node no
    /// longer holds, so the successor resumes an epoch its node gave up.
    ResumeStaleLive,
    /// A successor answers a handoff at the next epoch without claiming it.
    AcceptUnclaimed,
    /// A drain counts as finished as soon as it releases everything,
    /// without waiting for the releases to land.
    DrainEarly,
    /// The shell answers an operator's eviction as done once the core
    /// admits it, before the cell has stopped.
    AnswerOnAdmission,
    /// An abandoned eviction gives the cell back without telling the core.
    SilentAbandon,
    /// A swap reopens the database from its last upload instead of the open
    /// handle, dropping the tail it had not uploaded.
    SwapFromUpload,
    /// The host reports a restarted runtime on the generation before the
    /// current one.
    StaleGeneration,
    /// The shell never tells the core a regular or outbound socket opened.
    UnreportedSocket,
    /// The shell reports a resident Worker request finished as soon as it is
    /// routed, while it still runs.
    UnpinnedWorker,
    /// Epoch GC plans one epoch above the owner's: it takes the owner's
    /// epoch for the chain's base and keeps no margin below it, although
    /// that epoch may have nothing in its prefix yet.
    GcBelowOwn,
    /// The shell never tells the core which heap a runtime started in, so a
    /// walk down cannot see that emptying a heap returns most of a cell's
    /// cost.
    UnplacedHeaps,
    /// A restore takes the newest epoch that opens at TXID 1 for the whole
    /// chain, and ignores the paged epochs that continue it.
    NewestBase,
}

impl Mutation {
    pub const ALL: [Mutation; 25] = [
        Mutation::SkipVerifyOwnership,
        Mutation::ReuseOlderSnapshot,
        Mutation::SharedPrefix,
        Mutation::IgnoreCasGuard,
        Mutation::AckBeforeUpload,
        Mutation::AckBeforeWakeEntry,
        Mutation::RetireUnproven,
        Mutation::NoOrphanWake,
        Mutation::SilentWakeCoverage,
        Mutation::AckOneFollower,
        Mutation::SkipLogRecovery,
        Mutation::AppendAfterSeal,
        Mutation::SealOverBundles,
        Mutation::ResumeStaleLive,
        Mutation::AcceptUnclaimed,
        Mutation::DrainEarly,
        Mutation::AnswerOnAdmission,
        Mutation::SilentAbandon,
        Mutation::SwapFromUpload,
        Mutation::StaleGeneration,
        Mutation::UnreportedSocket,
        Mutation::UnpinnedWorker,
        Mutation::GcBelowOwn,
        Mutation::UnplacedHeaps,
        Mutation::NewestBase,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        std::iter::once(Mutation::None)
            .chain(Mutation::ALL)
            .find(|mutation| mutation.name() == name)
    }

    pub fn name(self) -> &'static str {
        match self {
            Mutation::None => "none",
            Mutation::SkipVerifyOwnership => "skip-verify-ownership",
            Mutation::ReuseOlderSnapshot => "reuse-older-snapshot",
            Mutation::SharedPrefix => "shared-prefix",
            Mutation::IgnoreCasGuard => "ignore-cas-guard",
            Mutation::AckBeforeUpload => "ack-before-upload",
            Mutation::AckBeforeWakeEntry => "ack-before-wake-entry",
            Mutation::RetireUnproven => "retire-unproven",
            Mutation::NoOrphanWake => "no-orphan-wake",
            Mutation::SilentWakeCoverage => "silent-wake-coverage",
            Mutation::AckOneFollower => "ack-one-follower",
            Mutation::SkipLogRecovery => "skip-log-recovery",
            Mutation::AppendAfterSeal => "append-after-seal",
            Mutation::SealOverBundles => "seal-over-bundles",
            Mutation::ResumeStaleLive => "resume-stale-live",
            Mutation::AcceptUnclaimed => "accept-unclaimed",
            Mutation::DrainEarly => "drain-early",
            Mutation::AnswerOnAdmission => "answer-on-admission",
            Mutation::SilentAbandon => "silent-abandon",
            Mutation::SwapFromUpload => "swap-from-upload",
            Mutation::StaleGeneration => "stale-generation",
            Mutation::UnreportedSocket => "unreported-socket",
            Mutation::UnpinnedWorker => "unpinned-worker",
            Mutation::GcBelowOwn => "gc-below-own",
            Mutation::UnplacedHeaps => "unplaced-heaps",
            Mutation::NewestBase => "newest-base",
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
    /// The share of writes that also set or delete the cell's alarm.
    pub alarm_probability: f64,
    /// `alarm_resident_ms`: an alarm due this soon holds its cell resident.
    pub alarm_resident_ms: u64,
    /// `CELLD_WAKER_TICK_MS`: the due scan and collector period.
    pub wake_tick_ms: u64,
    pub max_resident: usize,
    pub max_activations: usize,
    pub sticky_eviction: bool,
    pub idle_evict_ms: Option<u64>,
    pub evict_gap_ms: Option<(u64, u64)>,
    /// `CELLD_DURABILITY=fleet`: each node ships its writes to up to two
    /// followers and flushes them to the bucket in bundles. Off, every proof
    /// is a per-cell upload.
    pub fleet: bool,
    pub lifecycle: Lifecycle,
    pub stops: Stops,
    pub deploys: Deploys,
    pub resident: ResidentWork,
    /// Owners delete superseded epoch prefixes this long after their last
    /// upload (`CELLD_LTX_RETENTION_SECS`), or never.
    pub epoch_gc: Option<u64>,
    /// Memory the nodes hold against the pressure ceilings, or none: then
    /// every sample reports nothing and pressure shedding never engages.
    pub memory: Option<Memory>,
    /// The share of bucket restores that page the cell (`CELLD_LTX_PAGED`):
    /// the epoch continues the chain below it from the restored position
    /// instead of opening at TXID 1. The background fill is off, so a paged
    /// cell reads the chain for its whole residency.
    pub paged: f64,
    pub faults: Faults,
    pub mutation: Mutation,
    /// Keep a human-readable trace of every event and effect.
    pub trace: bool,
}

/// How nodes leave the fleet on purpose, and how ownership moves between
/// the ones that stay.
#[derive(Clone, Debug)]
pub struct Lifecycle {
    /// Graceful shutdowns per simulated minute across the fleet, during the
    /// chaos window. Zero leaves every node to run until it crashes.
    pub shutdowns: f64,
    /// The share of shutdowns that preserve ownership for a same-node
    /// reload (`/shutdown?handoff=preserve`); the rest drain, handing every
    /// cell to a peer.
    pub preserve_probability: f64,
    /// Each node gives idle cells to peers below their share of the fleet
    /// (`CELLD_REBALANCE_INTERVAL_MS`).
    pub rebalance: bool,
}

impl Lifecycle {
    /// No shutdowns and no balancing: cells move only by eviction, crash
    /// and takeover.
    pub fn off() -> Self {
        Self {
            shutdowns: 0.0,
            preserve_probability: 0.0,
            rebalance: false,
        }
    }

    /// Whether processes accept a signed handoff from a draining or
    /// balancing peer.
    pub fn hands_off(&self) -> bool {
        self.shutdowns > 0.0 || self.rebalance
    }
}

/// How an eviction stop lets its cell go, and how an operator asks for one.
#[derive(Clone, Debug)]
pub struct Stops {
    /// Longest an eviction takes to give its isolate turn back and capture
    /// the handoff snapshot (`LtxRepl::evict` before the publish). Until the
    /// capture ends, a request for the cell takes the eviction back.
    pub max_capture_ms: u64,
    /// Host wedges per simulated minute across the fleet, during the chaos
    /// window. A wedged host refuses to release any cell for up to 30 s:
    /// every stop attempt fails and is retried, and a bounded stop still
    /// failing at its deadline restarts the cell in place.
    pub wedges: f64,
    /// The evict ticks ask the way an operator does (`POST /evict`, which
    /// sends `EvictRequested`) and check the answer, instead of driving the
    /// policy's `Event::Evict`.
    pub operator: bool,
    /// During the chaos window the workload leaves every other cell alone,
    /// alternating every 20 s, so a stop of a quiet cell meets no request
    /// that could take it back, and a wedged one runs to its deadline.
    pub quiet: bool,
}

impl Stops {
    /// Every stop finishes once its final upload lands, and only the policy
    /// evicts.
    pub fn off() -> Self {
        Self {
            max_capture_ms: 0,
            wedges: 0.0,
            operator: false,
            quiet: false,
        }
    }
}

/// New application generations, which move resident cells to fresh
/// isolates (`Event::GenerationChanged`).
#[derive(Clone, Debug)]
pub struct Deploys {
    /// Node reloads per simulated minute across the fleet, during the chaos
    /// window. Each one adopts the next generation on one node, as a node's
    /// pointer watcher does.
    pub rate: f64,
    /// How long a stale cell waits for a safe point before its swap is
    /// forced (`generation::max_age`).
    pub max_age_ms: u64,
    /// Name every cell's class eager, so every swap is forced at once.
    pub eager: bool,
}

impl Deploys {
    pub fn off() -> Self {
        Self {
            rate: 0.0,
            max_age_ms: 0,
            eager: false,
        }
    }
}

/// What a node's memory is made of, and the ceilings it is held to
/// (`CELLD_MAX_RSS_MB` and the machine's size). Cells share V8 heaps, and a
/// heap comes back only when its last cell leaves, so most of what a walk
/// down returns comes from emptying heaps.
#[derive(Clone, Debug)]
pub struct Memory {
    /// What the process holds with no cell at all.
    pub base_mb: u64,
    /// One isolate's heap.
    pub heap_mb: u64,
    /// One cell's realm inside its heap.
    pub cell_mb: u64,
    /// `PoolLimits::max_cells`: placement packs this many cells into a heap
    /// before it builds another.
    pub cells_per_heap: usize,
    /// `CELLD_MAX_RSS_MB`, the ordinary ceiling.
    pub max_rss_mb: u64,
    /// The machine. The absolute cap is 95% of it.
    pub total_mb: u64,
    /// Surges per simulated minute across the fleet, during the chaos
    /// window: memory no cell owns, held for a few seconds.
    pub surges: f64,
    pub max_surge_mb: u64,
}

/// Work that runs on a resident runtime besides routed requests: WebSockets,
/// output gates and top-level Worker requests.
#[derive(Clone, Debug)]
pub struct ResidentWork {
    /// Sockets opened per simulated minute across the fleet, each on a cell
    /// a process runs: hibernatable, regular or outbound. A socket sends
    /// messages, which enter as `WebSocketRequestAt`, until its client
    /// closes it.
    pub sockets: f64,
    /// Longest a client keeps a socket open.
    pub max_socket_ms: u64,
    /// Top-level Worker requests per simulated minute across the fleet,
    /// which the core may run on an idle resident (`WorkerRequest`).
    pub workers: f64,
    /// The chance that a handler's write raises an output gate
    /// (`GateRequestAt`), which its reply waits for.
    pub gates: f64,
}

impl ResidentWork {
    pub fn off() -> Self {
        Self {
            sockets: 0.0,
            max_socket_ms: 0,
            workers: 0.0,
            gates: 0.0,
        }
    }
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
            alarm_probability: 0.3,
            alarm_resident_ms: 1_000,
            wake_tick_ms: 1_000,
            max_resident: 4,
            max_activations: 4,
            sticky_eviction: true,
            idle_evict_ms: None,
            evict_gap_ms: Some((200, 2_000)),
            fleet: false,
            lifecycle: Lifecycle::off(),
            stops: Stops::off(),
            deploys: Deploys::off(),
            resident: ResidentWork::off(),
            epoch_gc: None,
            memory: None,
            paged: 0.0,
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
        let mut config = Self {
            nodes: rng.range(2, 5) as usize,
            cells: rng.range(2, 10) as usize,
            ttl_ms,
            chaos_ms: 40_000,
            settle_ms: 4 * ttl_ms,
            request_gap_ms: (2, rng.range(20, 120)),
            write_probability: 0.6,
            alarm_probability: 0.0,
            alarm_resident_ms: 0,
            wake_tick_ms: 0,
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
            fleet: false,
            lifecycle: Lifecycle::off(),
            stops: Stops::off(),
            deploys: Deploys::off(),
            resident: ResidentWork::off(),
            epoch_gc: None,
            memory: None,
            paged: 0.0,
            faults,
            mutation: Mutation::None,
            trace: false,
        };
        // Drawn last, so the draws above stay what they were before alarms.
        config.alarm_probability = *rng.pick(&[0.1, 0.3, 0.6]).unwrap();
        config.alarm_resident_ms = *rng.pick(&[0, 2_000, 3_600_000]).unwrap();
        config.wake_tick_ms = *rng.pick(&[500, 1_000, 3_000]).unwrap();
        config.fleet = rng.chance(0.5);
        if rng.chance(0.5) {
            config.lifecycle = Lifecycle {
                shutdowns: rng.range(1, 4) as f64,
                preserve_probability: *rng.pick(&[0.0, 0.5, 1.0]).unwrap(),
                rebalance: rng.chance(0.5),
            };
        }
        if rng.chance(0.5) {
            config.stops = Stops {
                max_capture_ms: *rng.pick(&[50, 300, 2_000]).unwrap(),
                wedges: *rng.pick(&[0.0, 2.0, 6.0]).unwrap(),
                operator: rng.chance(0.7),
                quiet: rng.chance(0.5),
            };
        }
        if rng.chance(0.5) {
            config.deploys = Deploys {
                rate: *rng.pick(&[2.0, 6.0, 20.0]).unwrap(),
                max_age_ms: *rng.pick(&[0, 1_000, 5_000]).unwrap(),
                eager: rng.chance(0.2),
            };
        }
        if rng.chance(0.5) {
            config.resident = ResidentWork {
                sockets: *rng.pick(&[6.0, 30.0]).unwrap(),
                max_socket_ms: *rng.pick(&[2_000, 20_000]).unwrap(),
                workers: *rng.pick(&[0.0, 30.0, 120.0]).unwrap(),
                gates: *rng.pick(&[0.0, 0.3]).unwrap(),
            };
        }
        if rng.chance(0.5) {
            config.epoch_gc = Some(*rng.pick(&[0, 2_000, 10_000]).unwrap());
        }
        if rng.chance(0.5) {
            let (base_mb, heap_mb, cell_mb) = (200, 48, 6);
            let cells_per_heap = *rng.pick(&[1, 2, 4]).unwrap();
            // The resume line (80% of the ceiling) leaves room for at least
            // one full heap, so a walk down can always get under it once
            // a surge passes. A lower ceiling latches for good, which the
            // core does on purpose.
            let resume_mb = base_mb
                + heap_mb * rng.range(1, 4)
                + cell_mb * cells_per_heap as u64
                + rng.range(0, 40);
            let max_rss_mb = resume_mb * 5 / 4;
            config.memory = Some(Memory {
                base_mb,
                heap_mb,
                cell_mb,
                cells_per_heap,
                max_rss_mb,
                total_mb: max_rss_mb * 5 / 4,
                surges: *rng.pick(&[0.0, 3.0, 10.0]).unwrap(),
                max_surge_mb: *rng.pick(&[100, 400]).unwrap(),
            });
        }
        if rng.chance(0.5) {
            config.paged = *rng.pick(&[0.3, 1.0]).unwrap();
        }
        config
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
    /// Writes that installed an alarm.
    pub alarms_armed: u64,
    /// Alarm handler runs.
    pub alarms_fired: u64,
    /// Runs of an alarm installation that had already run: at-least-once.
    pub alarm_reruns: u64,
    /// Handler commits proven durable at their epoch.
    pub alarms_settled: u64,
    /// Runs of an already proven alarm by a superseded owner.
    pub stale_alarm_runs: u64,
    /// Failed handlers rescheduled by the retry backoff.
    pub alarm_retries: u64,
    /// Overdue alarms in the bucket that were never acknowledged or
    /// published, which the protocol does not promise to find.
    pub alarms_stranded: u64,
    /// Wake-entry PUTs that reached the bucket.
    pub wake_entries: u64,
    /// Retirement records advanced.
    pub wake_retirements: u64,
    /// Wake hints delivered to a core.
    pub wake_hints: u64,
    /// Alarms the audit node fired from the bucket alone, with no request.
    pub audit_alarm_runs: u64,
    /// Writes acknowledged on a fleet proof.
    pub fleet_acks: u64,
    /// Node-log ensembles installed.
    pub log_ensembles: u64,
    /// Shippers that degraded to bucket proofs.
    pub log_degrades: u64,
    /// Bundle flushes credited.
    pub bundle_credits: u64,
    /// Dead node-log sessions recovery sealed.
    pub log_recoveries: u64,
    /// Recoveries that found no witness and recorded a loss.
    pub log_loss_records: u64,
    /// Acknowledged writes lost because every disk that held them was
    /// wiped, which no protocol can recover.
    pub fleet_losses_excused: u64,
    /// Graceful shutdowns begun.
    pub shutdowns: u64,
    /// Shutdowns whose drain released every cell before the process exited.
    pub drains: u64,
    /// Processes that started from a clean-reload certificate.
    pub clean_reloads: u64,
    /// Balancing batches the core was asked to move.
    pub rebalances: u64,
    /// Handoff requests a donor sent to a successor.
    pub handoff_requests: u64,
    /// Handoffs a successor acknowledged.
    pub handoffs_accepted: u64,
    /// Evictions asked for the way an operator does.
    pub operator_evictions: u64,
    /// Eviction stop attempts the host failed, which the shell retried.
    pub stop_retries: u64,
    /// Generations nodes adopted.
    pub deploys: u64,
    /// Sockets clients opened, and those the core refused.
    pub sockets_opened: u64,
    pub sockets_refused: u64,
    /// Worker requests the core ran on a resident.
    pub workers_routed: u64,
    /// Output gates the core admitted to the work that raised them.
    pub gates_admitted: u64,
    /// Epoch prefixes epoch GC deleted.
    pub epochs_deleted: u64,
    /// Load samples that left a node shedding on memory pressure.
    pub shed_samples: u64,
    /// Heaps whose last cell left.
    pub heaps_freed: u64,
    /// Bucket restores that paged their cell.
    pub paged_restores: u64,
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
            alarms_armed,
            alarms_fired,
            alarm_reruns,
            alarms_settled,
            stale_alarm_runs,
            alarm_retries,
            alarms_stranded,
            wake_entries,
            wake_retirements,
            wake_hints,
            audit_alarm_runs,
            fleet_acks,
            log_ensembles,
            log_degrades,
            bundle_credits,
            log_recoveries,
            log_loss_records,
            fleet_losses_excused,
            shutdowns,
            drains,
            clean_reloads,
            rebalances,
            handoff_requests,
            handoffs_accepted,
            operator_evictions,
            stop_retries,
            deploys,
            sockets_opened,
            sockets_refused,
            workers_routed,
            gates_admitted,
            epochs_deleted,
            shed_samples,
            heaps_freed,
            paged_restores,
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
        self.alarms_armed += alarms_armed;
        self.alarms_fired += alarms_fired;
        self.alarm_reruns += alarm_reruns;
        self.alarms_settled += alarms_settled;
        self.stale_alarm_runs += stale_alarm_runs;
        self.alarm_retries += alarm_retries;
        self.alarms_stranded += alarms_stranded;
        self.wake_entries += wake_entries;
        self.wake_retirements += wake_retirements;
        self.wake_hints += wake_hints;
        self.audit_alarm_runs += audit_alarm_runs;
        self.fleet_acks += fleet_acks;
        self.log_ensembles += log_ensembles;
        self.log_degrades += log_degrades;
        self.bundle_credits += bundle_credits;
        self.log_recoveries += log_recoveries;
        self.log_loss_records += log_loss_records;
        self.fleet_losses_excused += fleet_losses_excused;
        self.shutdowns += shutdowns;
        self.drains += drains;
        self.clean_reloads += clean_reloads;
        self.rebalances += rebalances;
        self.handoff_requests += handoff_requests;
        self.handoffs_accepted += handoffs_accepted;
        self.operator_evictions += operator_evictions;
        self.stop_retries += stop_retries;
        self.deploys += deploys;
        self.sockets_opened += sockets_opened;
        self.sockets_refused += sockets_refused;
        self.workers_routed += workers_routed;
        self.gates_admitted += gates_admitted;
        self.epochs_deleted += epochs_deleted;
        self.shed_samples += shed_samples;
        self.heaps_freed += heaps_freed;
        self.paged_restores += paged_restores;
    }
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub seed: u64,
    pub violations: Vec<Violation>,
    pub stats: Stats,
    /// How often each core input and output occurred.
    pub coverage: Coverage,
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
