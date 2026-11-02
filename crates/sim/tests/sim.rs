use celld_sim::known::{Failure, KnownFailures, Verdict};
use celld_sim::{run, Faults, Mutation, Operator, Outcome, SimConfig};

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
        // Handlers' calls left through the gate, Worker requests borrowed
        // resident isolates, and alarms fired.
        assert!(
            outcome.stats.effects > 20,
            "seed {seed}: {:?}",
            outcome.stats
        );
        assert!(
            outcome.stats.workers_resident > 20,
            "seed {seed}: {:?}",
            outcome.stats
        );
        assert!(
            outcome.stats.alarms_fired > 5,
            "seed {seed}: {:?}",
            outcome.stats
        );
    }
}

#[test]
fn crashes_and_pauses_lose_no_acknowledged_write() {
    // Processes die, some with their disks, or stall past their leases, and
    // other nodes take their cells over and restore them from the bucket.
    // Every restore must hold every write a client was told succeeded,
    // whether the bucket proved it or, in the fleet posture, the followers.
    let (mut restored, mut fleet_proofs, mut log_recoveries) = (0, 0, 0);
    let (mut retried_recoveries, mut failed) = (0, 0);
    let mut judge = Judge::new();
    for (seed, fleet) in (0..20).flat_map(|seed| [(seed, false), (seed, true)]) {
        let config = SimConfig {
            fleet,
            ..crashing_and_pausing()
        };
        let outcome = run(seed, &config);
        let name = if fleet { "crashes-fleet" } else { "crashes" };
        judge.check(seed, name, &outcome);
        if !outcome.passed() {
            // A known core bug, as `crashing_and_pausing` describes.
            failed += 1;
            continue;
        }
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
        fleet_proofs += outcome.stats.fleet_proofs;
        log_recoveries += outcome.stats.log_recoveries;
        retried_recoveries += outcome
            .coverage
            .get(&("recovering_owner_log", "waiting_owner_log_recovery"))
            .copied()
            .unwrap_or(0);
    }
    judge.finish();
    // Most runs must pass: a run a known bug stopped early proves nothing
    // about lost writes.
    assert!(failed <= 4, "{failed} of 40 runs failed on a known bug");
    // Takeovers happened, so the lost-write check had restores to judge;
    // followers proved writes, and dead leaders' logs were recovered.
    assert!(restored > 100, "only {restored} restores from the bucket");
    assert!(fleet_proofs > 1_000, "only {fleet_proofs} fleet proofs");
    assert!(log_recoveries > 20, "only {log_recoveries} log recoveries");
    // A recovery failed and the cell waited to retry it.
    assert!(retried_recoveries > 0, "no failed log recovery");
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
        let first = run(seed, &SimConfig::swarm(seed));
        assert_eq!(
            first.trace_hash,
            run(seed, &SimConfig::swarm(seed)).trace_hash
        );
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
/// owner that wakes up not knowing it lost its cells. Of its first 300
/// seeds, two read stale after a suspend (192 and 236), and two stop on the
/// known `is firing an alarm while not resident` (9 and 224): a paused owner
/// wakes, fires a stale alarm, and its ownership read fails while the
/// handler's re-arm waits on its arm gate.
/// It opens no WebSockets: a pause can reset a cell, and a reset of a cell
/// holding one trips a known core invariant.
fn crashing_and_pausing() -> SimConfig {
    let base = SimConfig::crashing(6.0);
    SimConfig {
        websocket_probability: 0.0,
        faults: Faults {
            pauses: 6.0,
            max_pause_ms: 2 * base.ttl_ms,
            suspend_probability: 0.3,
            ..base.faults.clone()
        },
        ..base
    }
}

/// The seeds known to fail, and exactly how (`crates/sim/src/known.rs`).
const KNOWN_FAILURES: &str = include_str!("../known-failures.txt");

/// Judges each run against the known failures. It fails once, at the end,
/// with every run that did not fail as listed and the line the list needs
/// for it, so one run of the suite shows the whole change to the list.
struct Judge {
    known: KnownFailures,
    wrong: Vec<String>,
}

impl Judge {
    fn new() -> Self {
        let known = KnownFailures::parse(KNOWN_FAILURES).expect("known-failures.txt");
        Self {
            known,
            wrong: Vec::new(),
        }
    }

    /// Judge one run of `config`; whether it passed or failed as listed.
    fn check(&mut self, seed: u64, config: &str, outcome: &Outcome) -> bool {
        let verdict = self.known.judge(seed, config, outcome);
        let needed = Failure::of(outcome).map_or("nothing".into(), |f| f.line(seed, config));
        match &verdict {
            Verdict::New { .. } => self.wrong.push(format!(
                "NEW: {needed}: {:?} {:?}",
                outcome.panic, outcome.violations
            )),
            Verdict::Stale { listed } => self.wrong.push(format!(
                "STALE: passed, but listed: {}",
                listed.line(seed, config)
            )),
            Verdict::Passed | Verdict::Known => {}
        }
        verdict.ok()
    }

    fn finish(self) {
        assert!(self.wrong.is_empty(), "{}", self.wrong.join("\n"));
    }
}

/// The `State::validate` failures of this core, deliberately not fixed
/// here. The known failures may stop on these and no other.
///
/// - The first two are fixed upstream on the `dst` branch (`ea471ee`,
///   `e46a685`).
/// - A cancel of the last request for a cell whose ownership a restarted
///   node is still confirming drops the cell to `Inactive` without leaving
///   the confirming set.
/// - A drain that begins while an eviction is already in flight counts it
///   against `max_releases` on top of a full batch.
/// - A node that loses its lease while a cell's handoff is in flight, and
///   then gets a request for that cell, fences the cell inside the pipeline.
/// - A reset (a failed proof or ownership read) of a cell holding a regular
///   or outbound WebSocket moves it to `Cleaning` with the transport still
///   counted.
/// - An ownership read that fails while the cell's alarm handler runs
///   leaves the alarm firing on a cell that is no longer resident.
/// - A cell's alarm timer that fires after its node evicted it and released
///   its owner record puts the handoff cell in `WaitingActivation`.
/// - A request that brings a cell back to the node that just released it,
///   with a wake hint for it during the owner read, does the same in
///   `ReadingOwner`.
/// - A startup scan that finds two live epochs of one cell on disk counts
///   both as occupied, and can queue the cell for activation twice.
const KNOWN_CORE_PANICS: [&str; 11] = [
    "has no owning slot",
    "left the owner read in Fenced",
    "left the owner read in Inactive",
    "exceeds release ceiling",
    "is outside the pipeline in Fenced",
    "has a live transport while not resident",
    "is firing an alarm while not resident",
    "is outside the pipeline in WaitingActivation",
    "is outside the pipeline in ReadingOwner",
    "disagrees with the walk",
    "is queued twice",
];

/// The properties the core's known bugs break (`docs/testing.md`). The
/// known failures may break these and no other.
///
/// - `stale-read`: a clean read trusts the node's belief in its lease,
///   which faulty clocks (a skew, a step, a suspend) or a slow clean reload
///   break; or a restore lost an acknowledged write, and reads miss it.
/// - `lost-write` and `late-ack`, in the fleet posture: after a slow clean
///   reload, a clock fault, or a failed tail fold that hands the cell away.
/// - `liveness` and `alarm-not-fired`: a node whose memory walk-down wedged
///   refuses every new cell, and a fleet at its residency cap evicts nothing
///   to make room.
/// - `evicted-pinned`, with validation compiled out: the core evicts a cell
///   whose live transport `State::validate` would have caught.
const KNOWN_CORE_KINDS: [&str; 6] = [
    "stale-read",
    "lost-write",
    "late-ack",
    "liveness",
    "alarm-not-fired",
    "evicted-pinned",
];

/// The configuration a list line names.
fn config_named(name: &str, seed: u64) -> SimConfig {
    match name {
        "swarm" => SimConfig::swarm(seed),
        "chaos" => SimConfig::chaos(),
        "true-clocks" => chaos_with_true_clocks(),
        "crashes" => crashing_and_pausing(),
        "crashes-fleet" => SimConfig {
            fleet: true,
            ..crashing_and_pausing()
        },
        _ => panic!("the list names an unknown configuration {name:?}"),
    }
}

#[test]
fn the_known_failures_are_known_core_bugs() {
    // Each listed failure must be one of the documented bugs: a new kind of
    // failure is traced and described before it is listed, and a known
    // kind is listed only where its bug can happen.
    let known = KnownFailures::parse(KNOWN_FAILURES).expect("known-failures.txt");
    for (seed, name, failure) in known.failures() {
        let panic_known = (failure.panic.as_ref())
            .is_none_or(|panic| KNOWN_CORE_PANICS.iter().any(|bug| panic.contains(bug)));
        let kinds_known = (failure.kinds.iter()).all(|k| KNOWN_CORE_KINDS.contains(&k.as_str()));
        assert!(panic_known && kinds_known, "{seed} {name}: {failure:?}");
        let config = config_named(name, seed);
        let faults = &config.faults;
        let faulty_clocks = faults.max_skew_ms > 0
            || faults.clock_steps > 0.0
            || (faults.pauses > 0.0 && faults.suspend_probability > 0.0);
        let clean_reloads = config.operator.deploys > 0.0 || config.operator.preserves > 0.0;
        for kind in &failure.kinds {
            let possible = match kind.as_str() {
                "stale-read" => {
                    faulty_clocks || clean_reloads || failure.kinds.contains("lost-write")
                }
                "lost-write" | "late-ack" => config.fleet,
                "evicted-pinned" => !cfg!(debug_assertions),
                _ => true,
            };
            assert!(possible, "{seed} {name}: {kind} cannot be a known bug here");
        }
    }
}

/// Chaos with true clocks: no skew, no steps, no suspends.
fn chaos_with_true_clocks() -> SimConfig {
    let chaos = SimConfig::chaos();
    SimConfig {
        faults: Faults {
            max_skew_ms: 0,
            clock_steps: 0.0,
            suspend_probability: 0.0,
            ..chaos.faults.clone()
        },
        ..chaos
    }
}

#[test]
fn every_fault_at_once_breaks_nothing_but_the_known_core_bugs() {
    // Chaos runs every fault together, and once more with true clocks, so
    // no stale read is excused; the swarm draws a different mix and fleet
    // shape per seed. All must break no property, and the faults must
    // actually have happened, or the runs proved nothing.
    let configs = (0..20)
        .map(|seed| (seed, "chaos", SimConfig::chaos()))
        .chain((0..10).map(|seed| (seed, "true-clocks", chaos_with_true_clocks())))
        .chain((0..40).map(|seed| (seed, "swarm", SimConfig::swarm(seed))));
    let mut judge = Judge::new();
    let (mut clean, mut total) = (0, 0);
    let mut faulted = celld_sim::Stats::default();
    let mut entered = std::collections::BTreeSet::new();
    for (seed, name, config) in configs {
        let outcome = run(seed, &config);
        entered.extend(outcome.coverage.keys().map(|(_, to)| *to));
        judge.check(seed, name, &outcome);
        total += 1;
        clean += u32::from(outcome.passed());
        faulted.pauses += outcome.stats.pauses;
        faulted.partitions += outcome.stats.partitions;
        faulted.store_failures += outcome.stats.store_failures;
        faulted.crashes += outcome.stats.crashes;
        faulted.cancelled += outcome.stats.cancelled;
        faulted.evicted += outcome.stats.evicted;
        faulted.drains += outcome.stats.drains;
        faulted.handed_off += outcome.stats.handed_off;
        faulted.resumed += outcome.stats.resumed;
        faulted.sockets += outcome.stats.sockets;
        faulted.deploys += outcome.stats.deploys;
        faulted.alarms_fired += outcome.stats.alarms_fired;
        faulted.alarms_proven += outcome.stats.alarms_proven;
        faulted.wake_hints += outcome.stats.wake_hints;
        faulted.shedding_samples += outcome.stats.shedding_samples;
        faulted.errors += outcome.stats.errors;
    }
    judge.finish();
    // Most seeds run to the end and through the liveness audit.
    assert!(
        clean * 4 > total * 3,
        "only {clean} of {total} seeds ran clean"
    );
    assert!(faulted.pauses > 20, "{faulted:?}");
    assert!(faulted.partitions > 10, "{faulted:?}");
    assert!(faulted.store_failures > 500, "{faulted:?}");
    assert!(faulted.crashes > 10, "{faulted:?}");
    // So did the operator's work: clients gave up, cells were evicted,
    // drained nodes handed cells to their peers, and preserved nodes'
    // replacements resumed databases in place, handlers opened WebSockets,
    // nodes moved their cells onto new application generations, alarms
    // fired, wake scans hinted cells whose alarms came due, nodes over
    // their memory ceiling shed cells, and handlers answered with errors.
    assert!(faulted.cancelled > 500, "{faulted:?}");
    assert!(faulted.evicted > 20, "{faulted:?}");
    assert!(faulted.drains > 10, "{faulted:?}");
    assert!(faulted.handed_off > 20, "{faulted:?}");
    assert!(faulted.resumed > 10, "{faulted:?}");
    assert!(faulted.sockets > 20, "{faulted:?}");
    assert!(faulted.deploys > 20, "{faulted:?}");
    assert!(faulted.alarms_fired > 100, "{faulted:?}");
    // Most handler commits were proven, so `lost-handler-write` had
    // something to judge.
    assert!(
        faulted.alarms_proven * 2 > faulted.alarms_fired,
        "{faulted:?}"
    );
    assert!(faulted.wake_hints > 100, "{faulted:?}");
    assert!(faulted.shedding_samples > 100, "{faulted:?}");
    assert!(faulted.errors > 100, "{faulted:?}");
    // And between them, the cores went through every phase a cell has, so
    // every check had each phase to judge. A failed log recovery is too rare
    // here; the crash test covers it.
    let never: Vec<&str> = celld_sim::coverage::PHASES
        .into_iter()
        .filter(|phase| *phase != "waiting_owner_log_recovery" && !entered.contains(phase))
        .collect();
    assert!(never.is_empty(), "phases never entered: {never:?}");
    // And `coverage::name` names no phase outside the list, which this
    // check would otherwise never ask about.
    let unlisted: Vec<&str> = (entered.iter().copied())
        .filter(|phase| !celld_sim::coverage::PHASES.contains(phase))
        .collect();
    assert!(
        unlisted.is_empty(),
        "phases missing from PHASES: {unlisted:?}"
    );
}

#[test]
fn every_planted_bug_is_caught_by_its_check() {
    // A check that never fires proves nothing by staying quiet. Each
    // mutation breaks one promise of the shell or bucket, and the check for
    // that property must catch it within a few seeds of a fleet that no
    // seed fails without it.
    for mutation in Mutation::ALL {
        // Every kind listed must fire, so each check is pinned by a bug.
        let expected: &[&str] = match mutation {
            Mutation::IgnoreCasGuard => &["two-writers"],
            Mutation::PhantomClaim => &["unclaimed-runtime"],
            Mutation::SkipPublish => &["local-without-runtime"],
            Mutation::AckBeforeUpload => &["ack-before-durable", "read-before-durable"],
            Mutation::StaleRestore => &["lost-write"],
            Mutation::ReadsBypassGate => &["read-before-durable"],
            Mutation::BlindVerify => &["late-ack", "unverified-ack"],
            Mutation::RejectClaims => &["liveness"],
            Mutation::TornRestore => &["torn-restore"],
            Mutation::StaleEpoch => &["epoch-reused"],
            Mutation::SlowMonoClock => &["stale-read"],
            Mutation::StaleRegistry => &["evicted-but-serving"],
            Mutation::PhantomSocketClose => &["evicted-pinned"],
            Mutation::LostWakeEntry => &["uncovered-alarm", "crash-uncovered-alarm"],
            Mutation::AlarmsUnreported => &["alarm-not-fired"],
            Mutation::SkipLogRecovery => &["lost-write"],
            Mutation::FollowerIgnoresSeal => &["late-ack"],
            Mutation::ErrorsBypassGate => &["read-before-durable"],
            Mutation::FollowerCorrupts => &["unverified-ack", "fleet-proof-unbacked"],
            Mutation::AcceptUnclaimed => &["unclaimed-handoff"],
            Mutation::RetireUnproven => &["alarm-undiscoverable"],
            Mutation::StaleSnapshot => &["lost-write"],
            Mutation::GcUnlisted => &["base-deleted"],
            Mutation::CollectUnretired => &["alarm-undiscoverable"],
            Mutation::CreditAfterSeal => &["bundle-credit-unheld"],
            Mutation::ReopenUntiered => &["lost-write"],
            Mutation::LossWithoutWaiting => &["unjustified-loss"],
            Mutation::CreditBeforeActivation => &["credit-before-activation"],
            Mutation::GcUncovered => &["bundle-deleted-uncovered"],
            Mutation::NewestBase => &["lost-write"],
            Mutation::GcChainHead => &["unrestorable"],
            Mutation::IgnoreDrainToken => &["donors-overlap"],
            Mutation::GateReadsReleasedClaim => &["ready-stuck"],
            Mutation::GateStopsAsking => &["ready-stuck"],
        };
        let base = match mutation {
            // Only a drain closes a socket and then evicts its cell.
            Mutation::PhantomSocketClose => SimConfig {
                websocket_probability: 0.05,
                operator: Operator {
                    drains: 6.0,
                    ..SimConfig::calm().operator
                },
                ..SimConfig::calm()
            },
            // Only a drain or a rebalance offers cells to peers.
            Mutation::AcceptUnclaimed => SimConfig {
                operator: Operator {
                    drains: 6.0,
                    ..SimConfig::calm().operator
                },
                ..SimConfig::calm()
            },
            // Only the fleet posture has a log to recover or seal.
            Mutation::SkipLogRecovery
            | Mutation::FollowerIgnoresSeal
            | Mutation::FollowerCorrupts
            | Mutation::LossWithoutWaiting
            | Mutation::GcUncovered => SimConfig {
                fleet: true,
                ..crashing_and_pausing()
            },
            // An early credit is read only if its leader dies before the
            // activation's lease write applies: slow bucket calls hold
            // that write open, and crashes end leaders.
            Mutation::CreditBeforeActivation => SimConfig {
                fleet: true,
                websocket_probability: 0.0,
                faults: Faults {
                    crashes: 10.0,
                    store_spike_probability: 0.3,
                    store_spike_ms: 3_000,
                    ..SimConfig::crashing(6.0).faults
                },
                ..SimConfig::crashing(6.0)
            },
            // Only a shipper whose follower is cut off reopens its log, and
            // the rows below the new fragments are lost only if the leader
            // dies before a bundle covers them: lost bucket calls stall the
            // bundle loop, and crashes end leaders.
            Mutation::ReopenUntiered => SimConfig {
                ttl_ms: 3_000,
                fleet: true,
                websocket_probability: 0.0,
                faults: Faults {
                    crashes: 6.0,
                    partitions: 6.0,
                    max_partition_ms: 6_000,
                    store_lost_request: 0.1,
                    ..Faults::default()
                },
                ..SimConfig::crashing(0.0)
            },
            // Only a bundle that lands after recovery sealed its session
            // credits late: a leader suspended past its lease wakes before
            // it notices, with its peer's recovery already done.
            Mutation::CreditAfterSeal => SimConfig {
                nodes: 2,
                cells: 4,
                ttl_ms: 2_000,
                fleet: true,
                websocket_probability: 0.0,
                faults: Faults {
                    pauses: 20.0,
                    max_pause_ms: 4_000,
                    suspend_probability: 1.0,
                    ..Faults::default()
                },
                ..SimConfig::crashing(0.0)
            },
            // A stale snapshot loses writes only once its cell has moved
            // away and back: rebalances move cells, and a full node evicts.
            Mutation::StaleSnapshot => SimConfig {
                operator: Operator {
                    rebalances: 14.0,
                    ..SimConfig::calm().operator
                },
                max_resident: 2,
                sticky_eviction: true,
                ..crashing_and_pausing()
            },
            // Only a fleet that runs epoch GC deletes prefixes, and only an
            // epoch that has uploaded nothing yet is not its chain's base:
            // one a snapshot or a restore opened, with no write since. A
            // full node evicts often, so cells reopen often.
            Mutation::GcUnlisted => SimConfig {
                epoch_gc_grace_ms: Some(0),
                sticky_eviction: true,
                max_resident: 2,
                ..crashing_and_pausing()
            },
            // Only drains that overlap contend for the token.
            Mutation::IgnoreDrainToken => SimConfig {
                nodes: 4,
                operator: Operator {
                    drains: 12.0,
                    ..SimConfig::calm().operator
                },
                ..SimConfig::calm()
            },
            // Only a fleet with a balancer runs the readiness gate, and only
            // a process that starts after another's drain reads its claim.
            Mutation::GateReadsReleasedClaim | Mutation::GateStopsAsking => SimConfig {
                operator: Operator {
                    drains: 6.0,
                    rebalances: 6.0,
                    ..SimConfig::calm().operator
                },
                ..crashing_and_pausing()
            },
            // Only a paged restore continues a chain.
            Mutation::NewestBase => SimConfig {
                paged: 1.0,
                ..crashing_and_pausing()
            },
            // And only a chain whose fill completed lets epoch GC plan over
            // it: cells move often, and each move pages the next epoch.
            Mutation::GcChainHead => SimConfig {
                paged: 1.0,
                epoch_gc_grace_ms: Some(0),
                sticky_eviction: true,
                ..crashing_and_pausing()
            },
            // Only a handler that throws answers with an error.
            Mutation::ErrorsBypassGate => SimConfig {
                error_probability: 0.2,
                ..crashing_and_pausing()
            },
            _ => crashing_and_pausing(),
        };
        let config = SimConfig {
            mutation: Some(mutation),
            ..base.clone()
        };
        // A watched property counts too: its bug shows it can fire.
        let fired = |config: &SimConfig, seed: u64| -> Vec<&'static str> {
            let outcome = run(seed, config);
            let hits = outcome.violations.iter().chain(&outcome.watched);
            hits.map(|v| v.kind).collect()
        };
        let mut missing: Vec<&str> = expected.to_vec();
        for seed in 0..40 {
            let caught: Vec<&str> = (fired(&config, seed).into_iter())
                .filter(|kind| missing.contains(kind))
                .collect();
            if caught.is_empty() {
                continue;
            }
            // The bug did it only if the same seed without it does not.
            let unplanted = fired(&base, seed);
            missing.retain(|kind| !caught.contains(kind) || unplanted.contains(kind));
            if missing.is_empty() {
                break;
            }
        }
        assert!(
            missing.is_empty(),
            "{mutation:?}: {missing:?} never fired in 40 seeds, on a seed it passes unplanted"
        );
    }
}
