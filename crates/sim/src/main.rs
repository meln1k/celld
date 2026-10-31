//! `celld-sim [--seed N] [--trace]`: run one seed and print what happened.

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
            other => panic!("unknown argument {other}"),
        }
    }
    let outcome = run(seed, &config);
    for line in &outcome.trace {
        println!("{line}");
    }
    println!(
        "seed {seed}: {} events, hash {:016x}, authoritative {:?}, halted {:?}",
        outcome.events, outcome.trace_hash, outcome.authoritative, outcome.halts
    );
    if let Some(panic) = outcome.panic {
        println!("PANIC {panic}");
        std::process::exit(1);
    }
}
