# celld

`crates/logic` owns every decision (`on_event` returns effects);
`crates/celld` only carries effects out. `crates/sim` is the deterministic
simulator (DST) that drives `celld_logic::on_event` under faults and checks
every property in `crates/sim/src/checks.rs`. `docs/testing.md` explains what
it models, what it checks and which core bugs are known.

## Running the DST

Run it on every change to `crates/logic` or `crates/sim`, in both modes. The
two modes find different bugs: with validation on, `State::validate` stops a
run at the first broken invariant; with it off, the run goes on, as it does in
production, and shows what that broken state leads to.

```sh
# The suite, as CI runs it (.github/workflows/sim.yml): debug, validation on.
cargo fmt -p celld-sim -- --check
cargo clippy -p celld-sim --all-targets --locked -- -D warnings
cargo test -p celld-sim --locked

# The same suite with validation compiled out, as the image build runs it.
cargo test --release -p celld-sim --locked
```

A sweep runs many seeds and prints one line per failing seed (`FAIL`) and
per watched hit (`WATCH`). Build both modes optimized, in their own target
directories:

```sh
# Validation on.
CARGO_TARGET_DIR=target/sweep CARGO_PROFILE_DEV_OPT_LEVEL=2 \
  CARGO_PROFILE_DEV_DEBUG=0 cargo build -p celld-sim
# Validation off: no debug assertions, no overflow checks.
CARGO_TARGET_DIR=target/sweep-nodebug CARGO_PROFILE_DEV_OPT_LEVEL=2 \
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_DEV_DEBUG_ASSERTIONS=false \
  CARGO_PROFILE_DEV_OVERFLOW_CHECKS=false cargo build -p celld-sim

B=target/sweep/debug/celld-sim             # or target/sweep-nodebug/...
$B --swarm --seeds 0..1000                 # a quick sweep
seq 0 31 | xargs -P32 -I{} sh -c \
  "$B --swarm --seeds \$(({}*3125))..\$((({}+1)*3125)) > q.{}.txt"  # 100k
$B --swarm --seed 92204 --trace            # replay one seed, event by event
$B --swarm --seeds 0..1000 --coverage      # every phase transition taken
```

A seed is the whole run: the same seed and binary give the same trace hash.
`--swarm` draws each seed's configuration from the seed. A 100k sweep takes
about five minutes on 32 cores.

## When a change touches the core

- A new `Effect` or `Event`: the simulated shell (`crates/sim/src/world/`)
  must carry the effect out and send the event as production does. The
  shell's match on `Effect` and `coverage::name` are exhaustive, so a new
  variant stops the build until it is modelled. A new `Phase` must also be
  added to `coverage::PHASES`; a test checks that.
- Compare a 100k sweep in both modes before and after the change. A new
  failure kind is either a core bug or a simulator bug; trace it with
  `--trace` before calling it either.
- A simulator refactor must not change behavior: the trace hashes of a set
  of seeds must match before and after.

## Adding a check

- Every property lives in `checks.rs`, or, when it reads the fleet's state
  at a moment, in `world/judges.rs`.
- A new property starts as *watched* (`Checker::watch`): it prints but fails
  nothing. Enforce it only once every hit in a 100k sweep traces to the core.
- An enforced check needs a planted bug (`Mutation` in `crates/sim/src/lib.rs`)
  that it catches, listed in `every_planted_bug_is_caught_by_its_check`.

## Known core bugs

`crates/sim/known-failures.txt` lists every seed known to fail, exactly how,
in each mode: the suite's own configurations, and swarm seeds 0..10000. The
suite and `--known` fail on any other failure, and on a listed seed that no
longer fails as listed. Each listed failure must be a documented core bug:
`KNOWN_CORE_PANICS` and `KNOWN_CORE_KINDS` in `crates/sim/tests/sim.rs`,
described in `docs/testing.md`.

```sh
crates/sim/sweep.sh $B 0 10000      # judge, on every core, as CI does
$B --swarm --seeds 0..10000 --record | rg '^[0-9]+ swarm '         # lines
```

CI (`.github/workflows/sim.yml`) runs that sweep in both modes on every
change to `crates/logic` or `crates/sim`.

A failing suite test prints the lines it needs. Never regenerate the list
blindly: trace every seed whose line changes, and name the bug or the
simulator change behind it in the commit. When a core fix lands, its seeds
stop failing and their lines go, so the suite guards against the bug's
return. Add a new kind or invariant to the two constants only with a traced
seed and a description in `docs/testing.md`.
