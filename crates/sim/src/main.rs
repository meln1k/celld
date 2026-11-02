//! `celld-sim [--seed N | --seeds A..B] [--crashes PER_MINUTE | --chaos | --swarm]
//! [--fleet] [--memory ROOM] [--mutation NAME] [--coverage] [--trace]
//! [--known FILE | --record]`
//!
//! One seed prints what happened. A range prints one line per failing seed,
//! with its first violation or panic, and one line per watched property a
//! seed hit, then counts. Watched hits never fail a seed. `--swarm` draws each
//! seed's configuration from the seed itself. `--mutation` plants a known
//! bug, named as in `Mutation`, to watch a check catch it. `--memory` gives
//! each node a memory ceiling that about `ROOM` cells fit under.
//! `--coverage` prints every phase transition a range took, with how many
//! seeds took it.
//!
//! A swarm range can be judged against the known failures
//! (`crates/sim/src/known.rs`): `--known FILE` prints `NEW` for a seed that
//! fails other than as listed and `STALE` for a listed seed that fails less
//! or passes, and fails if there is either. `--record` prints the list line
//! of every failing seed instead.

// A developer tool's report, not celld's stdout contract.
#![allow(clippy::disallowed_macros)]

use celld_sim::known::{Failure, KnownFailures, Verdict};
use celld_sim::{run, Memory, Mutation, SimConfig};

fn main() {
    let mut seed = 0;
    let mut seeds = None;
    let mut config = SimConfig::calm();
    let (mut swarm, mut trace, mut fleet) = (false, false, false);
    let mut mutation = None;
    let mut coverage = false;
    let mut memory = None;
    let (mut known, mut record) = (None, false);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seed" => seed = args.next().and_then(|s| s.parse().ok()).expect("--seed N"),
            "--seeds" => {
                let range = args.next().expect("--seeds A..B");
                let (a, b) = range.split_once("..").expect("--seeds A..B");
                seeds = Some(a.parse::<u64>().unwrap()..b.parse::<u64>().unwrap());
            }
            "--mutation" => {
                let name = args.next().expect("--mutation NAME");
                let found = Mutation::ALL
                    .iter()
                    .find(|m| format!("{m:?}") == name)
                    .copied()
                    .unwrap_or_else(|| panic!("no mutation {name}: {:?}", Mutation::ALL));
                mutation = Some(found);
            }
            "--trace" => trace = true,
            // A sweep prints every phase transition its seeds took.
            "--coverage" => coverage = true,
            "--swarm" => swarm = true,
            // The fleet posture for a fixed configuration; a swarm seed
            // draws its own.
            "--fleet" => fleet = true,
            // A memory model whose ceiling lets about ROOM cells stay open.
            "--memory" => {
                let room = args.next().and_then(|s| s.parse().ok());
                memory = Some(Memory::with_room(room.expect("--memory ROOM")));
            }
            "--known" => {
                let path = args.next().expect("--known FILE");
                // A developer tool reading its input, not node storage.
                #[allow(clippy::disallowed_methods)]
                let text = std::fs::read_to_string(&path).expect("read the known failures");
                known = Some(KnownFailures::parse(&text).unwrap_or_else(|e| panic!("{path}: {e}")));
            }
            "--record" => record = true,
            "--chaos" => config = SimConfig::chaos(),
            "--crashes" => {
                let rate = args.next().and_then(|s| s.parse().ok());
                config = SimConfig::crashing(rate.expect("--crashes PER_MINUTE"));
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let config_for = |seed| {
        let base = if swarm {
            SimConfig::swarm(seed)
        } else {
            config.clone()
        };
        SimConfig {
            trace,
            mutation,
            fleet: base.fleet || fleet,
            memory: memory.or(base.memory),
            ..base
        }
    };
    assert!(
        swarm || (known.is_none() && !record),
        "the known failures list swarm seeds: add --swarm"
    );
    if let Some(seeds) = seeds {
        // A core panic is caught and reported per seed; the default hook
        // would also print each one to stderr.
        std::panic::set_hook(Box::new(|_| {}));
        let (total, mut failed, mut unexpected) = (seeds.end - seeds.start, 0, 0);
        let mut watched = std::collections::BTreeMap::<&str, u64>::new();
        // Per transition: the seeds that took it, and how often in all.
        let mut covered = std::collections::BTreeMap::<(&str, &str), (u64, u64)>::new();
        for seed in seeds {
            let outcome = run(seed, &config_for(seed));
            for (transition, times) in &outcome.coverage {
                let entry = covered.entry(*transition).or_default();
                entry.0 += 1;
                entry.1 += times;
            }
            let mut kinds = std::collections::BTreeSet::new();
            for w in &outcome.watched {
                if kinds.insert(w.kind) {
                    *watched.entry(w.kind).or_default() += 1;
                    println!("WATCH {seed}: t={} {}: {}", w.at_ms, w.kind, w.detail);
                }
            }
            let first = outcome.panic.clone().or_else(|| {
                let v = outcome.violations.first()?;
                Some(format!("t={} {}: {}", v.at_ms, v.kind, v.detail))
            });
            if let Some(first) = &first {
                failed += 1;
                if record {
                    let failure = Failure::of(&outcome).expect("failed");
                    println!("{}", failure.line(seed, "swarm"));
                } else if known.is_none() {
                    println!("FAIL {seed}: {first}");
                }
            }
            let line = |failure: &Failure| failure.line(seed, "swarm");
            match known.as_ref().map(|k| k.judge(seed, "swarm", &outcome)) {
                Some(Verdict::New { listed }) => {
                    unexpected += 1;
                    let now = line(&Failure::of(&outcome).expect("failed"));
                    let was = listed.as_ref().map_or("nothing".into(), line);
                    println!("NEW {seed}: {now} (listed: {was})");
                }
                Some(Verdict::Stale { listed }) => {
                    unexpected += 1;
                    println!("STALE {seed}: passed, but listed: {}", line(&listed));
                }
                _ => {}
            }
        }
        for (kind, seeds) in &watched {
            println!("{seeds} of {total} seeds hit watched {kind}");
        }
        if coverage {
            for ((from, to), (seeds, times)) in &covered {
                println!("COVER {from} -> {to}: {seeds} seeds, {times} times");
            }
        }
        let never: Vec<&str> = celld_sim::coverage::PHASES
            .into_iter()
            .filter(|phase| !covered.keys().any(|(_, to)| to == phase))
            .collect();
        println!(
            "{} phase transitions taken; phases never entered: {never:?}",
            covered.len()
        );
        println!("{failed} of {total} seeds failed");
        if known.is_some() {
            println!("{unexpected} of {total} seeds did not fail as listed");
            std::process::exit(i32::from(unexpected > 0));
        }
        std::process::exit(i32::from(failed > 0 && !record));
    }
    let config = config_for(seed);
    if swarm {
        println!("{config:?}");
    }
    let outcome = run(seed, &config);
    for line in &outcome.trace {
        println!("{line}");
    }
    let stats = &outcome.stats;
    println!(
        "seed {seed}: {} events, hash {:016x}, authoritative {:?}, halted {:?}",
        stats.events, outcome.trace_hash, outcome.authoritative, outcome.halts
    );
    println!(
        "requests {}: answered {}, failed {}, cancelled {}, writes acknowledged {}, \
         forwarded {}, bounced {}",
        stats.requests,
        stats.answered,
        stats.failed,
        stats.cancelled,
        stats.acked_writes,
        stats.forwarded,
        stats.bounced
    );
    println!(
        "crashes {}, pauses {}, partitions {}, bucket failures {}, restores from the bucket {} \
         ({} paged), from an eviction snapshot {}\n\
         clean stop retries {}, evictions abandoned {}, memory surges {}\n\
         drain token claims {}, bypassed {}, processes ready {}",
        stats.crashes,
        stats.pauses,
        stats.partitions,
        stats.store_failures,
        stats.restored,
        stats.paged,
        stats.reused,
        stats.stop_retries,
        stats.stops_abandoned,
        stats.surges,
        stats.drain_claims,
        stats.drain_bypasses,
        stats.ready
    );
    println!(
        "evictions asked {}, evicted {}, drains {}, cells handed off {}, preserves {}, \
         databases resumed {}, websockets {}\n\
         handler calls released {}, worker requests {} ({} on a resident), deploys {}\n\
         alarms set {}, alarms fired {} ({} proven), wake hints {}\n\
         fleet proofs {}, logs recovered {} ({} waited behind a claim, {} took one over), \
         writes lost to a declared loss {}, \
         epochs deleted {}, compactions {}, bundles deleted {}\n\
         load samples shedding on memory {}",
        stats.evictions,
        stats.evicted,
        stats.drains,
        stats.handed_off,
        stats.preserves,
        stats.resumed,
        stats.sockets,
        stats.effects,
        stats.workers,
        stats.workers_resident,
        stats.deploys,
        stats.alarms_set,
        stats.alarms_fired,
        stats.alarms_proven,
        stats.wake_hints,
        stats.fleet_proofs,
        stats.log_recoveries,
        stats.recovery_waits,
        stats.recovery_takeovers,
        stats.fleet_losses,
        stats.epochs_deleted,
        stats.compactions,
        stats.bundles_deleted,
        stats.shedding_samples
    );
    for w in &outcome.watched {
        println!("WATCH t={} {}: {}", w.at_ms, w.kind, w.detail);
    }
    for v in &outcome.violations {
        println!("VIOLATION t={} {}: {}", v.at_ms, v.kind, v.detail);
    }
    if !outcome.violations.is_empty() {
        std::process::exit(1);
    }
    if let Some(panic) = outcome.panic {
        println!("PANIC {panic}");
        std::process::exit(1);
    }
}
