# Testing

celld makes three promises:

- An acknowledged write is durable.
- A cell has one writer at a time.
- Code written for Cloudflare Workers and Durable Objects operates the same on
  celld.

We test the API contract by differential execution against workerd, the
coordination protocol by model checking and deterministic simulation, and the
full system by fault injection on live fleets.

## Conformance: two runtimes, one output

We run the same Workers and Durable Objects programs on workerd, Cloudflare's
production runtime, and on celld, and the outputs must match. workerd supplies
the reference output, so the expected behavior does not depend on celld.

Each new API adds fixtures to the corpus. We also port the workerd Durable
Objects contract tests, its web-platform globals tests, and the upstream Web
Platform Tests. Before a release, we replay storage, SQL, alarm, stream,
WebSocket, and lifecycle scenarios through the full `celld` binary in each
deployment mode.

## Specification: exhaustive at small size

The coordination protocol is specified in TLA+. Heyang Zhou wrote the
specifications against celld v0.1.0. His model checking found four bugs and a
split-brain that lost an acknowledged write. All are fixed. None had surfaced
in our own review or testing.

At a small configuration, the checker visits every reachable state. The model
grants a linearizable object store and perfect shared clocks, so a violation
needs no clock skew or storage anomaly. The invariants are one writer for each
epoch and no lost acknowledged write. The checker also verifies that the epoch
in the key stops a stale owner's late writes from losing an acknowledged write.

Each configuration pins an expected verdict, and most verdicts are failures:
each models a past protocol bug or a deliberately broken checker, and must
produce its counterexample.

A redesign of the write-acknowledgment fence was checked before it was built.
The check found an eight-state counterexample against the old version: a
dormant cell resumes at its old epoch while its release is in flight,
acknowledges a write, and the following takeover restores without it. The fix
shipped, and the counterexample stays as a pinned failure.

The checker also removed code. celld once sealed a cell's durable history at
restore. The verdicts showed that the seal only prevented the return of an
unacknowledged write, which celld does not promise, and that it could turn a
recoverable ordering error into permanent data loss.

We update the specifications by hand with protocol changes. They do not run in
continuous integration. A separate record tracks what the model does not yet
describe, including guarantees weaker than the code's.

## Simulation: the protocol under adversarial schedules

The dangerous bugs are in coordination: a crash during an ownership handoff, a
lease renewal that races a takeover, an alarm against a partially restored
cell. These windows are too narrow and too rare to wait for.

The coordination protocol is therefore a
[pure decision core](https://github.com/denoland/celld/tree/main/crates/logic)
with no I/O. The clock, the randomness, and the object store are interfaces
that a simulator drives. The simulated store injects latency, compare-and-swap
races, and lost responses. The clocks drift, and a node can crash at each
await point. Scripted adversaries play the cells, such as a handler that never
returns or a write stream that stops halfway. V8 is not deterministic, so it
stays out of the simulation.

A seeded scheduler drives each run, so a failure reproduces exactly. We check
safety (two writers in one epoch, a lost acknowledged write, an expired lease
that returns) and liveness (each armed alarm fires, ownership settles on one
node after a crash). A property must survive tens of thousands of seeds, and
the core protocols have run through millions of schedules. Deliberately broken
protocol variants verify that the checkers detect the faults.

In this repository the simulator is [`crates/sim`](../crates/sim). It runs a
fleet of `celld-logic` cores against a simulated bucket, with a shell that
follows `crates/celld/actor.rs` effect by effect, and checks one writer per
epoch, no runtime without its own claim, no acknowledgement before the write is
in its epoch's prefix, no lost acknowledged write, `State::validate` after
every event, and that every cell serves again once the faults heal. Each seed
draws its own mix of crashes, disk wipes, pauses and suspends, partitions,
clock steps and skew, and bucket loss and latency.

Requests also arm and clear alarms. The shell keeps the alarm row in each
cell's content and models the handler, the wake-entry gate on a reply, wake
maintenance and retirement, and the waker's boot, due, and cleanup scans. The
alarm checks are:

- **alarm-undiscoverable**: an acknowledged alarm has no wake entry left that
  its retirement record does not retire.
- **alarm-lost**: after the faults heal, a fresh node that restores from the
  bucket alone never ran an acknowledged alarm overdue by more than a lease
  and two scan ticks. No request wakes the cell, so only the waker can.
- **alarm-refired**: the current owner runs an alarm again after its
  completion was proven. A stale owner, such as one resuming from suspend
  that judges its own lease on a monotonic clock, can rerun one. The run
  counts that as `stale_alarm_runs` instead of failing, because alarms are
  at least once.

```
cargo test -p celld-sim                                   # the suite
cargo run -p celld-sim --profile sim -- --seeds 0..10000  # a sweep
cargo run -p celld-sim -- --seed 17 --trace               # replay one seed
cargo run -p celld-sim -- --seeds 0..100 --mutation shared-prefix
cargo run -p celld-sim --profile sim -- --seeds 0..2000 \
  --known crates/sim/known-failures.txt                   # as CI sweeps
```

A sweep runs one seed per core at a time and reports in seed order, so its
output does not depend on `--jobs N`.

Each mutation plants a protocol bug and must fail its check: `ack-before-wake-entry`,
`retire-unproven`, `no-orphan-wake`, and `silent-wake-coverage` cover the
alarm path.

Half the swarm runs in the fleet posture (`--fleet on` forces it). Each
process ships its commits to up to two followers and acknowledges once every
follower has fsynced them. It folds its log record into its node lease and
flushes bundles. A takeover recovers the dead session's log before it
restores: it fences the record, seals the followers, gathers their fragments
and the bundles, and seals the record. An acknowledgement must then be in
the prefix, in a bundle that recovery will read, or fsynced by every follower
of the published record. A loss that recovery declares counts against the
run unless no disk or object still holds the write. `ack-one-follower`,
`skip-log-recovery`, and `append-after-seal` cover this path.

Half the swarm also stops nodes on purpose and balances the rest. A drain
claims the fleet drain token, waits for the requests clients sent before the
listener closed, and hands every cell to a peer. The peer claims the cell
before it answers, and the donor follows redirects to the current owner. A
preserve keeps the owner records, prunes the live databases it no longer
holds, seals its log once the per-cell layout covers every retained bundle,
and leaves a clean-reload certificate that the next process resumes from.
Nodes also give idle cells to peers below their share. The checks are:

- **unclaimed-handoff**: a successor accepts a cell only at an epoch that a
  claim installed for its node.
- **drain-left-owner**: a node that finished its drain holds no owner record
  that its core still holds the cell at.
- A clean reload continues its predecessor's writer, so one-writer and the
  claim check follow the reload, and a resumed epoch holds every write
  acknowledged at it.

`seal-over-bundles`, `resume-stale-live`, `accept-unclaimed`, and
`drain-early` cover this path.

Half the swarm also stops cells the way production does. An eviction gives
its isolate turn back and captures its handoff snapshot before the final
upload. A request that arrives during the capture abandons it, and the cell
starts again at its own epoch. Hosts wedge during the chaos window and refuse
every release. A stop retries until the host recovers, and a bounded
eviction that is still failing at its deadline restarts the cell in place.
While the faults last, the workload leaves every other cell alone, so a
quiet cell's stop meets no request that could abandon it. Evict ticks ask
the way an operator does (`EvictRequested`), and the checks are:

- **eviction-misreported**: an eviction answered `Evicted` or
  `AlreadyAbsent` while the process still runs the cell.
- **eviction-unanswered**: an eviction asked before the faults healed is
  still waiting at the audit.

`answer-on-admission` and `silent-abandon` cover this path. `--coverage`
lists how often a sweep reached each core event and effect, and the ones it
never reached.

It does not yet model WebSockets, generation swaps, deletion, or memory
pressure.

### The shell on a simulated runtime

`crates/sim` models the shell; the shell itself can also run simulated. Every
clock, task, timer, random draw and filesystem call in `crates/celld` goes
through `celld::asyncrt`, and a build with `--cfg celld_internal_tests` swaps
the Tokio facade for a deterministic one. That build fills its harness slots,
the `include!(env!("CELLD_INTERNAL_…"))` and `include!(env!("CELLD_CONFORMANCE_…"))`
sites, from `crates/celld/internal/` and `crates/celld/conformance/`; a slot
with no file there gets an empty one, and a slot set in the environment wins.

The simulated facade runs every task of every node on one thread in virtual
time. Each node is a domain with its own services, clocks, counters and
random streams; the seed picks which ready task runs next, time jumps to the
next timer when nothing is ready, and a crash drops a domain's tasks where
they stand. `composed_simulation` drives real `Actor`s on it.

A node can also own its cells for real. The bucket is `SimStore`, an
in-memory object store the nodes share. It applies compare-and-swap writes,
stamps objects with virtual time, and makes each request take seeded time.
A request can fail before the store applies it, or after, with its response
lost. Each node keeps its cells on a `SimDisk`. SQLite reaches the disk
through a VFS of its own, and the replicator through `asyncrt::fs()`. A
process crash leaves the disk as it was, and the dead process's open files
go inert. A power loss also takes every byte that was never synced, and a
wipe takes the whole disk.
`SimCellHost` runs scripted cells without V8 on the real replicator, so
ownership, restore and the output gate are production code.
`an_acknowledged_write_survives_a_power_loss` cuts a node's power in the
middle of a write stream. Then either the node restarts on its own disk or
another node restores from the bucket. Every write the client saw
acknowledged must come back. An output gate that acknowledges a write
before its proof fails this test.

`composed_simulation::fleet` runs three such nodes on one bucket. Clients
append to and read six cells through any node and follow its redirects.
Each seed draws process crashes, power losses, machines that return with an
empty disk, pauses, partitions from the bucket, wall-clock steps, and
failed, lost and slow store requests. Most faults land on a node that runs
cells. A node that fences itself exits, and a supervisor restarts it. The
checks are:

- **lost-write**: a read that began after a write was acknowledged holds
  it.
- **fork**: any two reads of a cell agree on their common prefix, and no
  read repeats a value.
- **one-writer**: no two processes run a cell at one epoch.
- **liveness**: once the faults heal, every node serves every cell.

Two planted bugs must fail them. An output gate that acknowledges before
its proof loses a write. A bucket that ignores compare-and-swap guards lets
two nodes run one epoch. The fleet does not yet run alarms, the node log,
or drains.

```
RUSTFLAGS="--cfg celld_internal_tests" CARGO_TARGET_DIR=target/internal \
  CARGO_PROFILE_DEV_DEBUG=0 cargo test -p celld --lib
```

A sweep runs on demand. `CELLD_SIM_MUTATION=early-ack` or `blind-cas`
plants a bug, and `CELLD_SIM_TRACE=1` prints a failing seed's trace:

```
CELLD_SIM_SEEDS=0..200 RUSTFLAGS="--cfg celld_internal_tests" \
  CARGO_TARGET_DIR=target/internal CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test -p celld --lib fleet_sweep -- --ignored --nocapture
```

A separate target directory keeps the flagged build from evicting the normal
one, and dropping debug info keeps the link of the V8-sized test binary short.

### Two layers

The two simulators split the work. `celld-sim` models the shell, so it can
be fast: a seed takes a fraction of a second, and a sweep covers tens of
thousands of seeds. It also covers what the shell simulation does not yet
run: alarms and their discovery, and bugs planted inside the protocol, such
as a skipped ownership check or a reused snapshot. Because its shell is
written by hand after `actor.rs`, it can drift from the shell it models.
The shell simulation cannot drift, because it runs the shell itself, but a
seed takes seconds, so it sweeps fewer and deeper seeds. A `celld-sim`
finding that depends on shell code belongs in the shell simulation as soon
as it runs that code.

[`simulation.yml`](../.github/workflows/simulation.yml) runs both on every
change: the core suite and 2,000 `celld-sim` seeds, and the flagged suite
and 20 fleet seeds. Nightly it runs 50,000 and 300.

## Live fleets: what simulation cannot see

Simulation cannot see real S3 tail latency, real kernel and filesystem
behavior, or V8 under memory pressure. A permanent fleet lab runs standard VMs
from standard providers against a real bucket. Workloads rotate: chat rooms
with many WebSocket connections, working sets that shift across tens of
thousands of checksummed cells, deployment cutovers under load, and nodes
filled to the memory limit. The lab qualifies each release. We archive each
run's configuration, verification sweeps, node journals, kernel logs, and
phase timings, including failed runs.

Faults land between verification passes. A pass fetches each cell through
different nodes and compares the status, the body, and the full message
ledger. A cell can be briefly unavailable while ownership moves, but its
committed state must stay complete and a live node must serve it again.

The scenarios:

- `SIGKILL` a node mid-write-stream and delete its local database, so recovery
  comes only from the bucket. Every acknowledged write returns, because the
  output gate held each response until the write was durable.
- Freeze an owner, write to its cells through other nodes, and unfreeze it.
  The node sees that its lease moved and refuses to serve the old state. Each
  write lands exactly once.
- Cut a node off from the bucket. It fences itself.
- Throttle the bucket to 429 on every request. The engine slows to the store's
  rate and does not amplify the throttle.
- Stop a full host at the provider level mid-workload. Its cells move to other
  nodes, and the returning host rejoins with no duplicate residency.

Across every run of every scenario, the verification sweeps show zero body
faults, zero status faults, and zero lost messages.

## A few numbers we trust

- **The epoch fence holds under contention.** Five hundred concurrent
  claimants made 5,500 attempts on the same cells: one writer for each epoch,
  zero violations.
- **A warm resident request is local.** A request to a resident cell does zero
  bucket operations and returns in p50 ~1.1 ms and p99 ~7 ms (a fixed-host
  measurement). Only a cold activation touches object storage.
- **A durable write waits for a durability proof.** A single node proves each
  write through the bucket, so one storage round trip is the minimum. A fleet
  of two or more nodes can instead prove a write when each follower holds it
  on disk. A lab fleet measured about 600 ms for a bucket proof and about 25 ms
  for a fleet proof. The bucket upload races every fleet proof, so a slow
  follower cannot make a write slower than a bucket proof. Concurrent writes
  to one cell share one upload.
- **A restore is normal work.** Placement treats the restore of an inactive
  cell as ordinary work. The measured restore times come from the retired
  external replicator, so this page gives no number until a fleet run measures
  `celld-ltx`.
- **Ten small nodes held real scale.** Ten nodes, each with 4 vCPU and 8 GB,
  held 10,000 resident cells and 20,000 concurrent WebSocket connections. With
  two of the ten nodes stopped, every cell was available again on another node
  in ~11 s at the tail (with reserve headroom).

## Known failure edges

A fleet at its resident limit has no space for a lost node's cells, so losing
multiple nodes degrades service. We test recovery with and without reserve
capacity to measure this limit.

### Open simulation findings

These findings fail seeds against the current code.
[`known-failures.txt`](../crates/sim/known-failures.txt) lists every seed
below 50,000 that fails in the swarm posture, with the finding and the way it
fails. The CI sweep runs with `--known` on that list: a seed that fails as
listed passes, and any other failure fails the sweep, as does a listed seed
that passes. Fixing a finding therefore means deleting its section. A few
listed failures are not yet attributed to a finding. Replay a seed with
`celld-sim --seed N --trace`, adding the `--fleet` setting where shown.

- **A takeover does not fence a session that has no log record yet**
  (863, 9258, 15037). A peer reads a node lease as
  expired, from clock skew or a renewal that lands late. The lease carries
  no log state, so the takeover claims the cell without fencing anything.
  The old owner still holds its lease by its own clock, so it opens its
  log through a lease write that still matches, and its fleet proofs
  acknowledge writes into the superseded epoch. A bucket proof would fail
  its ownership read; a fleet proof makes none.
- **Recovery declares a loss that a surviving disk could still repair**
  (812, 1626, 2750). A follower that is
  unreachable and whose lease has been dead for 3×TTL counts as
  conclusively gone. When every member counts as gone, recovery records a
  loss and seals the log, although the follower's disk, and often the old
  owner's own disk, still holds the acknowledged rows.
  [The guarantees](guarantees.md#the-takeover-recovery-gate) say the
  activation fails instead.
- **A failed wake-entry write for a re-arm reads as success**: `fire_alarm`
  folds it into the handler's success, so the alarm can become
  undiscoverable. Seed 2157 found it; since the swarm drew slow stops, no
  seed below 50,000 reaches it.
- **A full fleet forgets a refused capacity handoff** (`--fleet off`:
  8120): the next load report clears the refusal, and the cell never
  settles.
- **A release can orphan an unfolded tail** (357, 421, 1150). An epoch that
  ends without its final upload, such as a reset after an ownership read
  fails, leaves its acknowledged tail in the node's own bundles and
  followers. The node's next restore of the cell folds that tail first, and
  fails the activation if the fold fails. The cell is then `Dormant`, and a
  rebalance or a drain releases it. The adopter restores from the per-cell
  prefixes and loses the acknowledged rows.
- **An eviction during a drain overruns the release ceiling** (25, 120, 122).
  A policy eviction, or one the capacity pump starts, takes an eviction
  permit without counting the drain's releases, and `State::validate`
  reports more handoffs than `CELLD_RELEASES`.
- **A cell marked for handoff can leave the handoff pipeline** (108, 135,
  300). A wake hint, the capacity pump or a fence moves it to
  `WaitingActivation`, `ReadingOwner` or `Fenced` with the mark still set,
  and `State::validate` fails.
- **A preserve during the startup inventory strands a confirming cell**
  (45, 494, 564). `BeginPreserve` cancels the queued owner read but leaves
  the cell in the confirmation set, and `State::validate` fails.

Report a schedule that breaks a guarantee, an untested fault, or a measurement
that you cannot reproduce in the
[issue tracker](https://github.com/denoland/celld/issues).
