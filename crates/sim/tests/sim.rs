use celld_sim::{run, Mutation, SimConfig};

#[test]
fn calm_fleet_serves_every_request_and_breaks_nothing() {
    for seed in 0..20 {
        let outcome = run(seed, &SimConfig::calm());
        assert!(outcome.passed(), "seed {seed}: {outcome:?}");
        assert!(outcome.halts.is_empty(), "seed {seed}");
        // The audit killed the fleet; only the fresh node is left, and it
        // restored every cell from the bucket.
        assert_eq!(outcome.authoritative, ["n3"], "seed {seed}");
        assert!(
            outcome.stats.restored >= 6,
            "seed {seed}: {:?}",
            outcome.stats
        );
        // With no faults every request is answered, and the run did real
        // work: if nothing was acknowledged, the checks had nothing to see.
        assert_eq!(
            outcome.stats.answered, outcome.stats.requests,
            "seed {seed}"
        );
        assert!(outcome.stats.acked_writes > 100, "seed {seed}: {outcome:?}");
        // Clients pick a random ingress, so most requests run on a node
        // that is not their ingress.
        assert!(
            outcome.stats.forwarded > outcome.stats.requests / 2,
            "seed {seed}: {:?}",
            outcome.stats
        );
    }
}

#[test]
fn crashes_lose_no_acknowledged_write() {
    // Processes die, some with their disks, and other nodes take their
    // cells over and restore them from the bucket. Every restore must hold
    // every write a client was told succeeded.
    let mut restored = 0;
    for seed in 0..20 {
        let outcome = run(seed, &SimConfig::crashing(6.0));
        assert!(outcome.passed(), "seed {seed}: {outcome:?}");
        assert!(
            outcome.stats.crashes > 0,
            "seed {seed}: {:?}",
            outcome.stats
        );
        assert!(
            outcome.stats.acked_writes > 100,
            "seed {seed}: {:?}",
            outcome.stats
        );
        restored += outcome.stats.restored;
    }
    // Takeovers happened, so the lost-write check had restores to judge.
    assert!(restored > 100, "only {restored} restores from the bucket");
}

#[test]
fn a_bucket_slower_than_the_lease_never_grants_authority() {
    // A lease write that takes longer than the TTL lands already expired.
    // The core must notice and keep retrying, never believing it holds a
    // lease that peers already consider dead. With nothing held, there is
    // nothing to fence, so no node halts either, and nothing is served.
    // That breaks no safety property, but the liveness checks must say so:
    // a fleet that serves nothing is not working.
    let config = SimConfig {
        store_latency_ms: (1_600, 1_600),
        ..SimConfig::calm()
    };
    let outcome = run(7, &config);
    assert_eq!(outcome.panic, None);
    let kinds: Vec<_> = outcome.violations.iter().map(|v| v.kind).collect();
    // Every cell, once in the probe and once in the audit.
    assert_eq!(kinds, ["liveness"; 12], "{:?}", outcome.violations);
    assert!(outcome.authoritative.is_empty());
    assert!(outcome.halts.is_empty());
    assert_eq!(outcome.stats.answered, 0);
    assert_eq!(outcome.stats.acked_writes, 0);
}

#[test]
fn a_seed_replays_exactly() {
    for config in [SimConfig::calm(), SimConfig::crashing(6.0)] {
        let traced = SimConfig {
            trace: true,
            ..config.clone()
        };
        for seed in [0, 1, 17, 12_345] {
            let first = run(seed, &config);
            assert_eq!(first.trace_hash, run(seed, &config).trace_hash);
            // Tracing only records; it must not change the schedule.
            assert_eq!(first.trace_hash, run(seed, &traced).trace_hash);
        }
        // And the seed matters: a different seed draws a different run.
        assert_ne!(run(0, &config).trace_hash, run(1, &config).trace_hash);
    }
}

#[test]
fn a_swarm_seed_replays_its_configuration_and_its_run() {
    // In a swarm the seed also draws the configuration, so one number
    // still reproduces a failure.
    for seed in [0, 3, 1_562] {
        let config = SimConfig::swarm(seed);
        assert_eq!(
            format!("{config:?}"),
            format!("{:?}", SimConfig::swarm(seed))
        );
        assert_eq!(run(seed, &config).trace_hash, run(seed, &config).trace_hash);
    }
    let shapes: std::collections::BTreeSet<_> = (0..20)
        .map(|seed| {
            let config = SimConfig::swarm(seed);
            (
                config.nodes,
                config.cells,
                config.ttl_ms,
                config.max_resident,
            )
        })
        .collect();
    assert!(shapes.len() > 10, "swarm barely varies: {shapes:?}");
}

#[test]
fn every_planted_bug_is_caught_by_its_check() {
    // A check that never fires proves nothing by staying quiet. Each
    // mutation breaks one promise of the shell or bucket, and the check for
    // that property must catch it within a few seeds of the same crashing
    // fleet that `crashes_lose_no_acknowledged_write` shows clean.
    for mutation in Mutation::ALL {
        let expected: &[&str] = match mutation {
            Mutation::IgnoreCasGuard => &["two-writers"],
            Mutation::PhantomClaim => &["unclaimed-runtime"],
            Mutation::SkipPublish => &["local-without-runtime"],
            Mutation::AckBeforeUpload => &["ack-before-durable", "read-before-durable"],
            Mutation::StaleRestore => &["lost-write"],
            Mutation::RejectClaims => &["liveness"],
        };
        let config = SimConfig {
            mutation: Some(mutation),
            ..SimConfig::crashing(6.0)
        };
        let caught = (0..20).find(|&seed| {
            run(seed, &config)
                .violations
                .iter()
                .any(|v| expected.contains(&v.kind))
        });
        assert!(caught.is_some(), "{mutation:?} went unnoticed for 20 seeds");
    }
}
