use celld_sim::{run, SimConfig};

#[test]
fn calm_fleet_serves_every_request_and_breaks_nothing() {
    for seed in 0..20 {
        let outcome = run(seed, &SimConfig::calm());
        assert!(outcome.passed(), "seed {seed}: {outcome:?}");
        assert!(outcome.halts.is_empty(), "seed {seed}");
        assert_eq!(outcome.authoritative, ["n0", "n1", "n2"], "seed {seed}");
        // With no faults every request is answered, and the run did real
        // work: if nothing was acknowledged, the checks had nothing to see.
        assert_eq!(
            outcome.stats.answered, outcome.stats.requests,
            "seed {seed}"
        );
        assert!(outcome.stats.acked_writes > 100, "seed {seed}: {outcome:?}");
    }
}

#[test]
fn a_bucket_slower_than_the_lease_never_grants_authority() {
    // A lease write that takes longer than the TTL lands already expired.
    // The core must notice and keep retrying, never believing it holds a
    // lease that peers already consider dead. With nothing held, there is
    // nothing to fence, so no node halts either, and nothing is served.
    let config = SimConfig {
        store_latency_ms: (1_600, 1_600),
        ..SimConfig::calm()
    };
    let outcome = run(7, &config);
    assert!(outcome.passed(), "{outcome:?}");
    assert!(outcome.authoritative.is_empty());
    assert!(outcome.halts.is_empty());
    assert_eq!(outcome.stats.answered, 0);
    assert_eq!(outcome.stats.acked_writes, 0);
}

#[test]
fn a_seed_replays_exactly() {
    let traced = SimConfig {
        trace: true,
        ..SimConfig::calm()
    };
    for seed in [0, 1, 17, 12_345] {
        let first = run(seed, &SimConfig::calm());
        assert_eq!(first.trace_hash, run(seed, &SimConfig::calm()).trace_hash);
        // Tracing only records; it must not change the schedule.
        assert_eq!(first.trace_hash, run(seed, &traced).trace_hash);
    }
    // And the seed matters: a different seed draws a different run.
    assert_ne!(
        run(0, &SimConfig::calm()).trace_hash,
        run(1, &SimConfig::calm()).trace_hash
    );
}
