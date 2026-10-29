//! The simulator's own contract: a clean fleet passes, every planted bug is
//! caught, and a seed replays exactly.
//!
//! Replay a failure with `cargo run -p celld-sim -- --seed N --trace`, plus
//! `--mutation NAME` for a mutation run. `CELLD_SIM_SEEDS` widens the swarm
//! sweep here; the CLI runs any range (`--seeds 0..10000`).

use celld_sim::{
    run, Coverage, Deploys, Lifecycle, Mutation, Outcome, ResidentWork, SimConfig, Stops,
};

fn report(outcome: &Outcome) -> String {
    let mut lines = vec![format!("seed {}", outcome.seed)];
    lines.extend(
        outcome
            .panic
            .iter()
            .map(|panic| format!("  panic: {panic}")),
    );
    lines.extend(
        outcome
            .violations
            .iter()
            .take(5)
            .map(|v| format!("  t={} {}: {}", v.at_ms, v.kind, v.detail)),
    );
    lines.join("\n")
}

#[test]
fn a_calm_fleet_passes_and_exercises_every_path() {
    for seed in 0..4 {
        let outcome = run(seed, &SimConfig::calm());
        assert!(!outcome.failed(), "{}", report(&outcome));
        let stats = &outcome.stats;
        // A checker that never sees its situation proves nothing.
        assert!(stats.acked_writes > 100, "{stats:?}");
        assert!(stats.remote_routes > 0, "{stats:?}");
        assert!(stats.restores_local > 0, "{stats:?}");
        assert!(stats.restores_bucket > 0, "{stats:?}");
        assert!(stats.evictions_requested > 0, "{stats:?}");
        assert!(stats.alarms_fired > 0, "{stats:?}");
        assert!(stats.alarms_settled > 0, "{stats:?}");
        assert!(stats.wake_retirements > 0, "{stats:?}");
        assert!(stats.audit_alarm_runs > 0, "{stats:?}");
    }
}

#[test]
fn a_calm_fleet_acknowledges_on_its_followers() {
    for seed in 0..4 {
        let config = SimConfig {
            fleet: true,
            ..SimConfig::calm()
        };
        let outcome = run(seed, &config);
        assert!(!outcome.failed(), "{}", report(&outcome));
        let stats = &outcome.stats;
        assert!(stats.fleet_acks > 100, "{stats:?}");
        assert!(stats.bundle_credits > 0, "{stats:?}");
        // The audit node took every cell over from sessions that sealed
        // their logs on the way down, so no takeover waited for recovery.
        assert!(stats.takeovers > 0, "{stats:?}");
        assert_eq!(stats.log_recoveries, 0, "{stats:?}");
    }
}

#[test]
fn swarm_seeds_pass() {
    let seeds: u64 = std::env::var("CELLD_SIM_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(32);
    let mut failures = Vec::new();
    let mut totals = celld_sim::Stats::default();
    for seed in 0..seeds {
        let outcome = run(seed, &SimConfig::swarm(seed));
        if outcome.failed() {
            failures.push(report(&outcome));
        }
        totals.absorb(&outcome.stats);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // The sweep must actually reach the failures the checkers guard.
    assert!(totals.crashes > 0, "{totals:?}");
    assert!(totals.pauses > 0, "{totals:?}");
    assert!(totals.partitions > 0, "{totals:?}");
    assert!(totals.halts > 0, "{totals:?}");
    assert!(totals.takeovers > 0, "{totals:?}");
    assert!(totals.refused_outputs > 0, "{totals:?}");
    assert!(totals.alarm_retries > 0, "{totals:?}");
    assert!(totals.alarm_reruns > 0, "{totals:?}");
    assert!(totals.wake_hints > 0, "{totals:?}");
    assert!(totals.audit_alarm_runs > 0, "{totals:?}");
    assert!(totals.fleet_acks > 0, "{totals:?}");
    assert!(totals.log_degrades > 0, "{totals:?}");
    assert!(totals.log_recoveries > 0, "{totals:?}");
    assert!(totals.drains > 0, "{totals:?}");
    assert!(totals.clean_reloads > 0, "{totals:?}");
    assert!(totals.rebalances > 0, "{totals:?}");
    assert!(totals.handoffs_accepted > 0, "{totals:?}");
}

/// Whether some swarm seed below `limit` fails the planted bug on the check
/// that exists for it.
fn caught(mutation: Mutation, limit: u64, kinds: &[&str]) -> bool {
    caught_in(SimConfig::swarm, mutation, 0..limit, kinds)
}

fn caught_in(
    configure: impl Fn(u64) -> SimConfig,
    mutation: Mutation,
    seeds: std::ops::Range<u64>,
    kinds: &[&str],
) -> bool {
    seeds.into_iter().any(|seed| {
        let mut config = configure(seed);
        config.mutation = mutation;
        let outcome = run(seed, &config);
        assert!(outcome.panic.is_none(), "{}", report(&outcome));
        outcome
            .violations
            .iter()
            .any(|violation| kinds.contains(&violation.kind))
    })
}

#[test]
fn skipping_ownership_verification_loses_a_write() {
    assert!(caught(Mutation::SkipVerifyOwnership, 40, &["lost-write"]));
}

#[test]
fn reusing_an_older_snapshot_loses_a_write() {
    assert!(caught(Mutation::ReuseOlderSnapshot, 40, &["lost-write"]));
}

#[test]
fn a_shared_epoch_prefix_has_two_writers() {
    assert!(caught(Mutation::SharedPrefix, 10, &["two-writers"]));
}

#[test]
fn a_bucket_that_ignores_conditions_lets_two_nodes_run_one_epoch() {
    assert!(caught(Mutation::IgnoreCasGuard, 10, &["two-writers"]));
}

#[test]
fn acknowledging_before_upload_is_caught_at_the_ack() {
    assert!(caught(
        Mutation::AckBeforeUpload,
        10,
        &["ack-before-durable"]
    ));
}

#[test]
fn acknowledging_an_alarm_before_its_wake_entry_is_caught_at_the_ack() {
    assert!(caught(
        Mutation::AckBeforeWakeEntry,
        10,
        &["alarm-undiscoverable"]
    ));
}

#[test]
fn retiring_a_wake_entry_without_proof_strands_an_alarm() {
    assert!(caught(
        Mutation::RetireUnproven,
        10,
        &["alarm-undiscoverable"]
    ));
}

fn fleet(seed: u64) -> SimConfig {
    SimConfig {
        fleet: true,
        ..SimConfig::swarm(seed)
    }
}

#[test]
fn a_fleet_proof_from_one_follower_is_caught_at_the_ack() {
    assert!(caught_in(
        fleet,
        Mutation::AckOneFollower,
        0..10,
        &["ack-before-durable"]
    ));
}

#[test]
fn a_takeover_that_skips_log_recovery_loses_a_write() {
    assert!(caught_in(
        fleet,
        Mutation::SkipLogRecovery,
        0..10,
        &["lost-write"]
    ));
}

#[test]
fn a_follower_that_appends_after_its_seal_loses_a_write() {
    assert!(caught_in(
        fleet,
        Mutation::AppendAfterSeal,
        0..20,
        &["lost-write"]
    ));
}

/// Nodes leave on purpose, half of them for a same-node reload. No policy
/// evictions, no balancing and no slow stops: those reach open core findings
/// (docs/testing.md) that would fail a seed before its plant is reached. No
/// resident work either, which moves the schedules the plants were found on.
fn shutdowns(seed: u64) -> SimConfig {
    SimConfig {
        lifecycle: Lifecycle {
            shutdowns: 3.0,
            preserve_probability: 0.5,
            rebalance: false,
        },
        evict_gap_ms: None,
        stops: Stops::off(),
        resident: ResidentWork::off(),
        ..SimConfig::swarm(seed)
    }
}

/// Every shutdown hands its cells to a peer.
fn drains(seed: u64) -> SimConfig {
    let mut config = shutdowns(seed);
    config.lifecycle.preserve_probability = 0.0;
    config
}

fn drains_on_the_fleet(seed: u64) -> SimConfig {
    SimConfig {
        fleet: true,
        ..drains(seed)
    }
}

#[test]
fn a_seal_over_retained_bundles_loses_a_write() {
    // The first schedule that leaves a bundle-only tail behind a seal.
    assert!(caught_in(
        drains_on_the_fleet,
        Mutation::SealOverBundles,
        60..80,
        &["lost-write"]
    ));
}

#[test]
fn a_reload_that_keeps_stale_epochs_runs_one_epoch_twice() {
    assert!(caught_in(
        shutdowns,
        Mutation::ResumeStaleLive,
        0..30,
        &["two-writers"]
    ));
}

#[test]
fn a_successor_that_accepts_before_claiming_is_caught() {
    assert!(caught_in(
        drains,
        Mutation::AcceptUnclaimed,
        0..10,
        &["unclaimed-handoff"]
    ));
}

#[test]
fn a_drain_that_ends_before_its_releases_is_caught() {
    assert!(caught_in(
        drains,
        Mutation::DrainEarly,
        0..10,
        &["drain-left-owner"]
    ));
}

/// Every evict tick asks the operator's way, every eviction takes long
/// enough to be abandoned, and hosts wedge. No shutdowns and no balancing,
/// whose open core findings would fail a seed before its plant is reached.
fn stopping(seed: u64) -> SimConfig {
    SimConfig {
        evict_gap_ms: Some((50, 300)),
        lifecycle: Lifecycle::off(),
        stops: Stops {
            max_capture_ms: 300,
            wedges: 6.0,
            operator: true,
            quiet: true,
        },
        ..SimConfig::swarm(seed)
    }
}

#[test]
fn stopping_fleets_pass_and_reach_every_stop_outcome() {
    let mut coverage = Coverage::default();
    let mut totals = celld_sim::Stats::default();
    for seed in 0..16 {
        let outcome = run(seed, &stopping(seed));
        assert!(!outcome.failed(), "{}", report(&outcome));
        coverage.absorb(&outcome.coverage);
        totals.absorb(&outcome.stats);
    }
    for event in [
        "EvictRequested",
        "RuntimeStopCancelled",
        "RuntimeStopFailed",
    ] {
        assert!(coverage.events.contains_key(event), "{event}: {coverage:?}");
    }
    for effect in ["EvictionAdmission", "EvictionFinished"] {
        assert!(
            coverage.effects.contains_key(effect),
            "{effect}: {coverage:?}"
        );
    }
    assert!(totals.stop_retries > 0, "{totals:?}");
}

#[test]
fn answering_an_eviction_at_its_admission_is_caught() {
    assert!(caught_in(
        stopping,
        Mutation::AnswerOnAdmission,
        0..5,
        &["eviction-misreported"]
    ));
}

#[test]
fn an_abandonment_the_core_never_hears_of_leaves_an_eviction_unanswered() {
    assert!(caught_in(
        stopping,
        Mutation::SilentAbandon,
        0..12,
        &["eviction-unanswered"]
    ));
}

/// Nodes adopt a new generation every few seconds. No shutdowns and no
/// balancing, whose open core findings would fail a seed before its plant
/// is reached.
fn deploying(seed: u64) -> SimConfig {
    SimConfig {
        lifecycle: Lifecycle::off(),
        deploys: Deploys {
            rate: 20.0,
            max_age_ms: 1_000,
            eager: false,
        },
        ..SimConfig::swarm(seed)
    }
}

#[test]
fn deploying_fleets_pass_and_swap_their_cells() {
    let mut coverage = Coverage::default();
    for seed in 0..12 {
        let outcome = run(seed, &deploying(seed));
        assert!(!outcome.failed(), "{}", report(&outcome));
        coverage.absorb(&outcome.coverage);
    }
    assert!(
        coverage.events.contains_key("GenerationChanged"),
        "{coverage:?}"
    );
}

/// From seed 3: on seed 2 the reopened database reports an alarm older
/// than one the core already holds, which no real database can, and the
/// core's invariants stop the run first.
#[test]
fn a_swap_that_reopens_from_the_upload_loses_a_write() {
    assert!(caught_in(
        deploying,
        Mutation::SwapFromUpload,
        3..8,
        &["lost-write", "ack-before-durable"]
    ));
}

#[test]
fn a_runtime_reported_on_the_old_generation_stays_stale() {
    assert!(caught_in(
        deploying,
        Mutation::StaleGeneration,
        0..8,
        &["generation-stale"]
    ));
}

/// Sockets, gates and Worker requests on every resident. No shutdowns and
/// no balancing, whose open core findings would fail a seed before its
/// plant is reached.
fn busy(seed: u64) -> SimConfig {
    SimConfig {
        lifecycle: Lifecycle::off(),
        resident: ResidentWork {
            sockets: 30.0,
            max_socket_ms: 20_000,
            workers: 120.0,
            gates: 0.3,
        },
        ..SimConfig::swarm(seed)
    }
}

#[test]
fn busy_residents_pass_and_reach_their_events() {
    let mut coverage = Coverage::default();
    for seed in 0..12 {
        let outcome = run(seed, &busy(seed));
        assert!(!outcome.failed(), "{}", report(&outcome));
        coverage.absorb(&outcome.coverage);
    }
    for event in [
        "WebSocketOpened",
        "WebSocketClosed",
        "WebSocketRequestAt",
        "GateRequestAt",
        "WorkerRequest",
    ] {
        assert!(coverage.events.contains_key(event), "{event}: {coverage:?}");
    }
    for effect in ["CompleteWorker", "CloseWebSocket"] {
        assert!(
            coverage.effects.contains_key(effect),
            "{effect}: {coverage:?}"
        );
    }
}

#[test]
fn a_socket_the_core_never_hears_of_is_cut() {
    assert!(caught_in(
        busy,
        Mutation::UnreportedSocket,
        0..4,
        &["runtime-cut"]
    ));
}

#[test]
fn a_worker_reported_finished_too_early_is_cut() {
    assert!(caught_in(
        busy,
        Mutation::UnpinnedWorker,
        0..4,
        &["runtime-cut"]
    ));
}

/// Owners delete superseded epochs as soon as they may, on the fleet, where
/// writes reach the per-cell layout late. No shutdowns and no balancing,
/// whose open core findings would fail a seed before its plant is reached.
fn collecting(seed: u64) -> SimConfig {
    SimConfig {
        lifecycle: Lifecycle::off(),
        epoch_gc: Some(0),
        fleet: true,
        // Frequent evictions restore often, which is when a deleted
        // epoch shows.
        evict_gap_ms: Some((100, 600)),
        ..SimConfig::swarm(seed)
    }
}

#[test]
fn collecting_fleets_pass_and_delete_epochs() {
    let mut totals = celld_sim::Stats::default();
    for seed in 0..8 {
        let outcome = run(seed, &collecting(seed));
        assert!(!outcome.failed(), "{}", report(&outcome));
        totals.absorb(&outcome.stats);
    }
    assert!(totals.epochs_deleted > 0, "{totals:?}");
}

#[test]
fn epoch_gc_that_takes_its_own_epoch_for_the_base_loses_a_write() {
    assert!(caught_in(
        collecting,
        Mutation::GcBelowOwn,
        0..16,
        &["lost-write", "ack-before-durable"]
    ));
}

#[test]
fn a_waker_that_ignores_orphans_loses_an_alarm() {
    assert!(caught(Mutation::NoOrphanWake, 10, &["alarm-lost"]));
}

/// One slot per node and alarms an hour from eviction: a restored alarm
/// that never reports its wake entry pins the slot until a request finishes.
fn crowded(seed: u64) -> SimConfig {
    let mut config = SimConfig::swarm(seed);
    config.nodes = 2;
    config.cells = 10;
    config.max_resident = 1;
    config.alarm_probability = 0.6;
    config.alarm_resident_ms = 3_600_000;
    config.faults.crashes = config.faults.crashes.max(3.0);
    // Cells move only by eviction, crash and takeover, so a pinned slot
    // stays pinned.
    config.lifecycle = Lifecycle::off();
    config
}

#[test]
fn a_crowded_fleet_passes() {
    for seed in 0..8 {
        let outcome = run(seed, &crowded(seed));
        assert!(!outcome.failed(), "{}", report(&outcome));
    }
}

#[test]
fn unreported_wake_coverage_pins_a_cell() {
    assert!(caught_in(
        crowded,
        Mutation::SilentWakeCoverage,
        0..10,
        &["liveness"]
    ));
}

#[test]
fn a_seed_replays_exactly() {
    let seed = 7;
    let mut config = SimConfig::swarm(seed);
    let first = run(seed, &config);
    let again = run(seed, &config);
    assert_eq!(first.trace_hash, again.trace_hash);
    assert_eq!(first.stats.events, again.stats.events);
    // Tracing observes the run; it must not perturb it.
    config.trace = true;
    let traced = run(seed, &config);
    assert_eq!(first.trace_hash, traced.trace_hash);
    assert!(!traced.trace.is_empty());
    let other = run(seed + 1, &SimConfig::swarm(seed));
    assert_ne!(first.trace_hash, other.trace_hash);
}
