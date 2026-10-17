//! Run simulation seeds from the command line.
//!
//! ```text
//! celld-sim --seeds 0..1000            # swarm configurations, report failures
//! celld-sim --seed 17 --trace          # replay one seed with its full trace
//! celld-sim --seeds 0..200 --mutation shared-prefix
//! ```

// A developer tool's report, not celld's stdout contract.
#![allow(clippy::disallowed_macros)]

use celld_sim::{run, Mutation, SimConfig};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut seeds = 0..100u64;
    let mut trace = false;
    let mut calm = false;
    let mut mutation = Mutation::None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seed" => {
                let seed: u64 = args.next().and_then(|s| s.parse().ok()).expect("--seed N");
                seeds = seed..seed + 1;
            }
            "--seeds" => {
                let range = args.next().expect("--seeds A..B");
                let (a, b) = range.split_once("..").expect("--seeds A..B");
                seeds = a.parse().expect("seed")..b.parse().expect("seed");
            }
            "--trace" => trace = true,
            "--calm" => calm = true,
            "--mutation" => {
                let name = args.next().expect("--mutation NAME");
                mutation =
                    Mutation::parse(&name).unwrap_or_else(|| panic!("unknown mutation {name}"));
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let mut failed = 0;
    let mut totals = celld_sim::Stats::default();
    let count = seeds.end - seeds.start;
    for seed in seeds {
        let mut config = if calm {
            SimConfig::calm()
        } else {
            SimConfig::swarm(seed)
        };
        config.mutation = mutation;
        config.trace = trace;
        let outcome = run(seed, &config);
        totals.absorb(&outcome.stats);
        if trace {
            for line in &outcome.trace {
                println!("{line}");
            }
        }
        if outcome.failed() {
            failed += 1;
            println!("seed {seed}: FAILED {:?}", config_summary(&config));
            if let Some(panic) = &outcome.panic {
                println!("  panic: {panic}");
            }
            for violation in outcome.violations.iter().take(5) {
                println!(
                    "  t={} {}: {}",
                    violation.at_ms, violation.kind, violation.detail
                );
            }
        } else if count == 1 || trace {
            println!(
                "seed {seed}: ok hash={:016x} {:?}",
                outcome.trace_hash, outcome.stats
            );
        }
    }
    if count > 1 {
        println!("totals {totals:?}");
    }
    println!("{failed} of {count} seeds failed");
    if failed > 0 {
        std::process::exit(1);
    }
}

fn config_summary(config: &SimConfig) -> String {
    format!(
        "nodes={} cells={} ttl={} sticky={} faults={:?}",
        config.nodes, config.cells, config.ttl_ms, config.sticky_eviction, config.faults
    )
}
