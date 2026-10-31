//! `celld-sim [--seed N | --seeds A..B] [--crashes PER_MINUTE | --chaos | --swarm]
//! [--mutation NAME] [--trace]`
//!
//! One seed prints what happened. A range prints one line per failing seed,
//! with its first violation or panic, then a count. `--swarm` draws each
//! seed's configuration from the seed itself. `--mutation` plants a known
//! bug, named as in `Mutation`, to watch a check catch it.

// A developer tool's report, not celld's stdout contract.
#![allow(clippy::disallowed_macros)]

use celld_sim::{run, Mutation, SimConfig};

fn main() {
    let mut seed = 0;
    let mut seeds = None;
    let mut config = SimConfig::calm();
    let (mut swarm, mut trace) = (false, false);
    let mut mutation = Mutation::None;
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
                mutation = *Mutation::ALL
                    .iter()
                    .find(|m| format!("{m:?}") == name)
                    .unwrap_or_else(|| panic!("no mutation {name}: {:?}", Mutation::ALL));
            }
            "--trace" => trace = true,
            "--swarm" => swarm = true,
            "--chaos" => config = SimConfig::chaos(),
            "--crashes" => {
                let rate = args.next().and_then(|s| s.parse().ok());
                config = SimConfig::crashing(rate.expect("--crashes PER_MINUTE"));
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let config_for = |seed| SimConfig {
        trace,
        mutation,
        ..if swarm {
            SimConfig::swarm(seed)
        } else {
            config.clone()
        }
    };
    if let Some(seeds) = seeds {
        // A core panic is caught and reported per seed; the default hook
        // would also print each one to stderr.
        std::panic::set_hook(Box::new(|_| {}));
        let (total, mut failed) = (seeds.end - seeds.start, 0);
        for seed in seeds {
            let outcome = run(seed, &config_for(seed));
            let first = outcome.panic.clone().or_else(|| {
                let v = outcome.violations.first()?;
                Some(format!("t={} {}: {}", v.at_ms, v.kind, v.detail))
            });
            if let Some(first) = first {
                failed += 1;
                println!("FAIL {seed}: {first}");
            }
        }
        println!("{failed} of {total} seeds failed");
        std::process::exit(i32::from(failed > 0));
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
        "requests {}: answered {}, failed {}, writes acknowledged {}, forwarded {}, bounced {}",
        stats.requests,
        stats.answered,
        stats.failed,
        stats.acked_writes,
        stats.forwarded,
        stats.bounced
    );
    println!(
        "crashes {}, pauses {}, partitions {}, bucket failures {}, restores from the bucket {}",
        stats.crashes, stats.pauses, stats.partitions, stats.store_failures, stats.restored
    );
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
