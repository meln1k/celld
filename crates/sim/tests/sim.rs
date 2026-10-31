use celld_sim::{run, Faults, Mutation, Outcome, SimConfig};

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
fn crashes_and_pauses_lose_no_acknowledged_write() {
    // Processes die, some with their disks, or stall past their leases, and
    // other nodes take their cells over and restore them from the bucket.
    // Every restore must hold every write a client was told succeeded.
    let mut restored = 0;
    for seed in 0..20 {
        let outcome = run(seed, &crashing_and_pausing());
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

/// The crashing fleet, whose processes also pause past their leases: an
/// owner that wakes up not knowing it lost its cells. No seed of it fails.
fn crashing_and_pausing() -> SimConfig {
    let base = SimConfig::crashing(6.0);
    SimConfig {
        faults: Faults {
            pauses: 6.0,
            max_pause_ms: 2 * base.ttl_ms,
            suspend_probability: 0.3,
            ..base.faults.clone()
        },
        ..base
    }
}

/// Two `State::validate` failures in this core, both fixed upstream on the
/// `dst` branch (`ea471ee`, `e46a685`) but deliberately not here. A seed
/// that hits one stops there; anything else is a new failure.
const KNOWN_CORE_PANICS: [&str; 2] = ["has no owning slot", "left the owner read in Fenced"];

/// The run passed, or stopped on a known core panic with no violation of
/// the sim's own properties before it.
fn passed_but_for_known_bugs(outcome: &Outcome) -> bool {
    let known = outcome
        .panic
        .as_ref()
        .is_none_or(|panic| KNOWN_CORE_PANICS.iter().any(|bug| panic.contains(bug)));
    known && outcome.violations.is_empty()
}

#[test]
fn every_fault_at_once_breaks_nothing_but_the_known_core_bugs() {
    // Chaos runs every fault together; the swarm draws a different mix and
    // fleet shape per seed. Both must break no property, and the faults
    // must actually have happened, or the runs proved nothing.
    let configs = (0..20)
        .map(|seed| (seed, SimConfig::chaos()))
        .chain((0..40).map(|seed| (seed, SimConfig::swarm(seed))));
    let (mut clean, mut total) = (0, 0);
    let mut faulted = celld_sim::Stats::default();
    for (seed, config) in configs {
        let outcome = run(seed, &config);
        assert!(
            passed_but_for_known_bugs(&outcome),
            "seed {seed}: {:?} {:?}",
            outcome.panic,
            outcome.violations
        );
        total += 1;
        clean += u32::from(outcome.passed());
        faulted.pauses += outcome.stats.pauses;
        faulted.partitions += outcome.stats.partitions;
        faulted.store_failures += outcome.stats.store_failures;
        faulted.crashes += outcome.stats.crashes;
    }
    // Most seeds run to the end and through the liveness audit.
    assert!(
        clean * 4 > total * 3,
        "only {clean} of {total} seeds ran clean"
    );
    assert!(faulted.pauses > 20, "{faulted:?}");
    assert!(faulted.partitions > 10, "{faulted:?}");
    assert!(faulted.store_failures > 500, "{faulted:?}");
    assert!(faulted.crashes > 10, "{faulted:?}");
}

#[test]
fn every_planted_bug_is_caught_by_its_check() {
    // A check that never fires proves nothing by staying quiet. Each
    // mutation breaks one promise of the shell or bucket, and the check for
    // that property must catch it within a few seeds of a fleet that no
    // seed fails without it.
    for mutation in Mutation::ALL {
        let expected: &[&str] = match mutation {
            Mutation::IgnoreCasGuard => &["two-writers"],
            Mutation::PhantomClaim => &["unclaimed-runtime"],
            Mutation::SkipPublish => &["local-without-runtime"],
            Mutation::AckBeforeUpload => &["ack-before-durable", "read-before-durable"],
            Mutation::StaleRestore => &["lost-write"],
            Mutation::ReadsBypassGate => &["read-before-durable"],
            Mutation::BlindVerify => &["late-ack"],
            Mutation::RejectClaims => &["liveness"],
        };
        let config = SimConfig {
            mutation: Some(mutation),
            ..crashing_and_pausing()
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
