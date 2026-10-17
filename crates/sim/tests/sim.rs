//! The simulator's own contract: a clean fleet passes, every planted bug is
//! caught, and a seed replays exactly.
//!
//! Replay a failure with `cargo run -p celld-sim -- --seed N --trace`, plus
//! `--mutation NAME` for a mutation run. `CELLD_SIM_SEEDS` widens the swarm
//! sweep here; the CLI runs any range (`--seeds 0..10000`).

use celld_sim::{run, Mutation, Outcome, SimConfig};

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
    }
}

#[test]
fn swarm_seeds_pass() {
    let seeds: u64 = std::env::var("CELLD_SIM_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(24);
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
}

/// Whether some swarm seed below `limit` fails the planted bug on the check
/// that exists for it.
fn caught(mutation: Mutation, limit: u64, kinds: &[&str]) -> bool {
    (0..limit).any(|seed| {
        let mut config = SimConfig::swarm(seed);
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
