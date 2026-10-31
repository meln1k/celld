//! `celld-sim [--seed N] [--crashes PER_MINUTE | --chaos] [--trace]`: run one seed and print what happened.

// A developer tool's report, not celld's stdout contract.
#![allow(clippy::disallowed_macros)]

use celld_sim::{run, SimConfig};

fn main() {
    let mut seed = 0;
    let mut config = SimConfig::calm();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seed" => seed = args.next().and_then(|s| s.parse().ok()).expect("--seed N"),
            "--trace" => config.trace = true,
            "--chaos" => config = SimConfig::chaos(),
            "--crashes" => {
                let rate = args.next().and_then(|s| s.parse().ok());
                config = SimConfig::crashing(rate.expect("--crashes PER_MINUTE"));
            }
            other => panic!("unknown argument {other}"),
        }
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
