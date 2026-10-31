use celld_sim::{run, SimConfig};

#[test]
fn calm_fleet_holds_its_leases() {
    for seed in 0..20 {
        let outcome = run(seed, &SimConfig::calm());
        assert_eq!(outcome.panic, None, "seed {seed}");
        assert_eq!(outcome.halts, Vec::<String>::new(), "seed {seed}");
        assert_eq!(outcome.authoritative, ["n0", "n1", "n2"], "seed {seed}");
    }
}

#[test]
fn a_bucket_slower_than_the_lease_never_grants_authority() {
    // A lease write that takes longer than the TTL lands already expired.
    // The core must notice and keep retrying, never believing it holds a
    // lease that peers already consider dead. With nothing held, there is
    // nothing to fence, so no node halts either.
    let config = SimConfig {
        store_latency_ms: (1_600, 1_600),
        ..SimConfig::calm()
    };
    let outcome = run(7, &config);
    assert_eq!(outcome.panic, None);
    assert!(outcome.authoritative.is_empty());
    assert!(outcome.halts.is_empty());
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
    // And the seed matters: a different seed draws different latencies.
    assert_ne!(
        run(0, &SimConfig::calm()).trace_hash,
        run(1, &SimConfig::calm()).trace_hash
    );
}
