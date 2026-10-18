//! Run simulation seeds from the command line.
//!
//! ```text
//! celld-sim --seeds 0..1000            # swarm configurations, report failures
//! celld-sim --seed 17 --trace          # replay one seed with its full trace
//! celld-sim --seeds 0..200 --mutation shared-prefix
//! celld-sim --seeds 0..100000 --jobs 16 # one worker per core by default
//! celld-sim --seeds 0..500 --fleet on   # every seed in the fleet posture
//! celld-sim --seeds 0..500 --coverage   # which core events and effects ran
//! ```

// A developer tool's report, not celld's stdout contract.
#![allow(clippy::disallowed_macros)]

use celld_sim::{run, Coverage, Mutation, Outcome, SimConfig};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

fn main() {
    let mut args = std::env::args().skip(1);
    let mut seeds = 0..100u64;
    let mut trace = false;
    let mut calm = false;
    let mut mutation = Mutation::None;
    let mut fleet = None;
    let mut coverage = false;
    let mut jobs = std::thread::available_parallelism().map_or(1, usize::from);
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
            "--coverage" => coverage = true,
            "--calm" => calm = true,
            "--mutation" => {
                let name = args.next().expect("--mutation NAME");
                mutation =
                    Mutation::parse(&name).unwrap_or_else(|| panic!("unknown mutation {name}"));
            }
            "--fleet" => {
                fleet = match args.next().as_deref() {
                    Some("on") => Some(true),
                    Some("off") => Some(false),
                    _ => panic!("--fleet on|off"),
                };
            }
            "--jobs" => {
                jobs = args.next().and_then(|n| n.parse().ok()).expect("--jobs N");
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let config_for = |seed| {
        let mut config = if calm {
            SimConfig::calm()
        } else {
            SimConfig::swarm(seed)
        };
        config.mutation = mutation;
        config.fleet = fleet.unwrap_or(config.fleet);
        config.trace = trace;
        config
    };
    let count = seeds.end - seeds.start;
    let mut report = Report {
        count,
        trace,
        totals: celld_sim::Stats::default(),
        coverage: Coverage::default(),
        failed: 0,
    };
    // Each seed is independent, so workers take the next one as they free
    // up, and the report follows seed order whatever order they finish in.
    let next = AtomicU64::new(seeds.start);
    let (done, outcomes) = mpsc::channel::<(u64, SimConfig, Outcome)>();
    std::thread::scope(|scope| {
        for _ in 0..jobs.clamp(1, count.max(1) as usize) {
            let (next, done, end, config_for) = (&next, done.clone(), seeds.end, &config_for);
            scope.spawn(move || loop {
                let seed = next.fetch_add(1, Ordering::Relaxed);
                if seed >= end {
                    return;
                }
                let config = config_for(seed);
                let outcome = run(seed, &config);
                if done.send((seed, config, outcome)).is_err() {
                    return;
                }
            });
        }
        drop(done);
        let mut pending = BTreeMap::new();
        let mut expected = seeds.start;
        for (seed, config, outcome) in outcomes {
            pending.insert(seed, (config, outcome));
            while let Some((config, outcome)) = pending.remove(&expected) {
                report.seed(expected, &config, &outcome);
                expected += 1;
            }
        }
    });
    if count > 1 {
        println!("totals {:?}", report.totals);
    }
    if coverage {
        for (kind, counts) in [
            ("event", &report.coverage.events),
            ("effect", &report.coverage.effects),
        ] {
            for (name, count) in counts {
                println!("{kind} {name} {count}");
            }
        }
        println!("never reached: {:?}", report.coverage.missing());
    }
    println!("{} of {count} seeds failed", report.failed);
    if report.failed > 0 {
        std::process::exit(1);
    }
}

/// What a sweep has reported so far.
struct Report {
    count: u64,
    trace: bool,
    totals: celld_sim::Stats,
    coverage: Coverage,
    failed: u64,
}

impl Report {
    fn seed(&mut self, seed: u64, config: &SimConfig, outcome: &Outcome) {
        self.totals.absorb(&outcome.stats);
        self.coverage.absorb(&outcome.coverage);
        if self.trace {
            for line in &outcome.trace {
                println!("{line}");
            }
        }
        if outcome.failed() {
            self.failed += 1;
            println!("seed {seed}: FAILED {:?}", config_summary(config));
            if let Some(panic) = &outcome.panic {
                println!("  panic: {panic}");
            }
            for violation in outcome.violations.iter().take(5) {
                println!(
                    "  t={} {}: {}",
                    violation.at_ms, violation.kind, violation.detail
                );
            }
        } else if self.count == 1 || self.trace {
            println!(
                "seed {seed}: ok hash={:016x} {:?}",
                outcome.trace_hash, outcome.stats
            );
        }
    }
}

fn config_summary(config: &SimConfig) -> String {
    format!(
        "nodes={} cells={} ttl={} max_resident={} sticky={} alarms={} alarm_resident_ms={} \
         wake_tick_ms={} fleet={} faults={:?}",
        config.nodes,
        config.cells,
        config.ttl_ms,
        config.max_resident,
        config.sticky_eviction,
        config.alarm_probability,
        config.alarm_resident_ms,
        config.wake_tick_ms,
        config.fleet,
        config.faults
    )
}
