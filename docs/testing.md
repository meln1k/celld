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

### The simulator in this repository

[`crates/sim`](../crates/sim) covers the core of this design. It runs several
`celld-logic` cores in one thread, on one simulated clock, against one
simulated bucket. A simulated shell carries out each effect the way
`crates/celld/actor.rs` does. A cell is a scripted handler that can commit one
write and answers through the output gate. A handler can also call a third
party mid-request, which waits at the gate too, and can upgrade its request to
a WebSocket, whose messages are requests of their own. Top-level Worker
requests borrow idle resident isolates. Some clients give up before they hear
back, an operator evicts cells by hand and shuts nodes down, a balancer gives
idle cells away, and the log tier nudges node leases. A node that shuts
down or gives a cell away offers it to its peers until one acquires it, as
`AdoptReleased` and the peer's handoff endpoint do in production. A node can
also be replaced in place: its process preserves its cells and exits, and the
replacement resumes each database at the same epoch without the bucket. An
idle eviction or a cleanup keeps the closed database on its node as an
eviction snapshot, and the node's next activation of the cell opens it
instead of reading the bucket, unless that activation took the cell over. In
half of the swarm seeds, owners also run epoch GC every 5 s: list the cell's
epochs, plan with `epoch_gc::plan`, read the owner record, then delete each
epoch prefix below the restore chain's base, one bucket call per step. A
deploy moves a node's resident cells onto a new application generation, which
restarts each runtime on its open database. A write can set or clear its
cell's alarm, which lives in the database like any other write, with its
installation id: opening the database at a new epoch moves the id into that
epoch, as production's wake record does. Its wake
entry goes to the bucket, a timer fires it on the owner, and each node scans
the due wake entries every second to wake cells nobody holds. One node holds
the advisory waker role, a lease in the bucket, and hints the cells whose
owner is dead or absent. Old entries go only below a retirement certificate,
which an owner raises once it has proven its current alarm durable and its
ownership current. Its eager delete of them can fail, and every node's
collector lists the entries each second, deletes those the certificate
retires, and wakes the due cells it does not. Each of these bucket calls can
fail like any other.

The simulated shell carries out every effect the core can return, and sends
every event production sends, in its timed form.

Half of the swarm seeds run the fleet posture (`CELLD_DURABILITY=fleet`,
`--fleet`), modelled in `crates/sim/src/world/node_log.rs`:

- Each process leads a log with up to two followers on other machines.
  Each commit ships to them, and once both have confirmed it, the output gate
  releases the response with no read of the owner record. Every 2 s the
  bundle loop puts one bundle per session in the bucket, holding every open
  database with writes past what the bucket covers. A bundle counts for the
  gate only if the record is still open at its log epoch when it lands;
  otherwise its shipper degrades. It counts for no restore: a stop's final
  upload, a fold or a recovery copies its rows into the cell's prefix.
- The first fleet proof of each log epoch waits for the activation fence:
  a lease write must carry the record with `may_have_fleet_acks` set before
  any proof of that epoch credits. Without one within 10 s, the shipper
  degrades; the record keeps the flag, and the next proof waits again.
- A follower that has not confirmed a frame within 1.6 s degrades its
  leader: nothing ships, and the gate waits for bundles. So does a member
  that `log_evict` judges gray from the latencies of its appends and of
  the idle probes the posture watch sends, and every member stalling at
  once, which the leader takes for its own partition: it then recruits no
  one until a member answers. While degraded, the posture watch runs
  `maintain` about once a second, backing off to 10 s, and the 30 s tick
  runs it in any posture. It reads the peers' leases, a bucket call that
  can fail. Once no round is outstanding and the bucket holds everything
  the log shipped, the leader reopens the log at the next log epoch with
  the first two peers live by its own clock. A healthy log reopens only to
  grow wider. The new epoch has no fleet acks yet, and its followers hold
  only the rows shipped after the reopen.
- A cell epoch that ends without its final upload, after a reset or a fence,
  keeps what its log shipped or a bundle covered as a tail. The cell's next
  activation on that process first folds in the session's bundles and what
  its current followers hold of the cell, as `fold_cell` does, and drops the
  tail. If the session may have fleet acks and no follower that answers
  holds the whole log epoch, the activation fails rather than open a
  database missing acknowledged writes.
- Taking over a cell whose dead owner's log is not sealed, or restarting
  after such a process, first recovers that log. Recovery seals every
  follower that answers, so the dead leader can prove nothing more, and
  uploads what they hold. A booting process answers for its follower disk
  while it waits, and a crash can kill it there. A session that never
  proved a write through the fleet has nothing to lose, and seals what it
  gathers. One that may have (`may_have_fleet_acks`) and has no follower
  holding the whole session waits for every follower that might come back,
  then declares a bounded loss, and the oracle forgets what that loss
  covers. Recovery drains every bundle of the session first. It reads and
  writes the bucket, so it fails, times out, or applies with its answer
  lost, as a bucket call does, at boot too.
- A graceful exit seals its own log if its prefixes hold everything it
  shipped and bundled, and no tail waits for a fold.

Most swarm seeds, and the chaos runs, have some handlers throw at the end of
their turn, after any write they made. As in production, the error answer
takes the same output-gate ticket as a success (`answer_ticket`), and waits
for its arm gate. A handler stopped under it, by a closed database, fails
outside its turn and reveals nothing.

Half of the swarm seeds also model memory (`--memory ROOM`, `Memory` in
`crates/sim/src/lib.rs`). A process holds a fixed base, a fixed amount for
each V8 heap its cells' realms live in, and a fixed amount for each database
it has open, and each load sample reports that to the core, as it is sampled
before the first lease and every second after. The machine is sized so that
the core's ceiling (`from_limits`: 80% of the machine) is crossed a little
above ROOM open cells in one heap and its resume line sits just above ROOM.
A node over the ceiling sheds, and its lease tells peers it is pressured and
whether it has headroom.

Every seed places each starting runtime on a heap, as `runtime.rs` does with
`isolate::place_cell`: the fullest heap of the current generation with room,
or a new one. The core hears the heap and evicts from the heap closest to
empty, and a heap is freed, with its memory, when its last cell leaves.
Production lets a heap hold 32 cells, which a fleet this small never fills,
so the swarm also draws a limit of 1, 2 or 3.

A seed fixes the whole run. With `--swarm`, the seed also draws the run's
configuration: the fleet size, the cell count, the lease lifetime, the
residency caps, the eviction policy, and which faults happen. The faults are
crashes with or without the disk, pauses and VM suspends, partitions that hang
or refuse, clock steps and skew, bucket calls that fail, time out, lose
their answer, or arrive late, and gray log followers, which stay up but
for a while write appends and answer probes late, in order, sometimes
past the append deadline.

The checks, in `crates/sim/src/checks.rs`:

- **two-writers**: two processes ran one cell epoch, or uploaded into its
  prefix.
- **unclaimed-runtime**: a node ran a cell epoch that its own claim did not
  install in the bucket.
- **local-without-runtime**: the core routed a request locally with no
  runtime published.
- **ack-before-durable** and **read-before-durable**: a response left before
  the bucket, or every follower of its node's log, held every write its
  handler saw. An error a handler threw inside its turn is a response too:
  its message can quote what the handler read, so it reveals the cell's
  state, its own unacknowledged write included (`FailedInTurn`).
- **lost-write**: a restore opened without an acknowledged write.
- **torn-restore**: a restore opened with a state that no earlier database
  ever held, such as one with a hole or a reordering.
- **late-ack**: a write was acknowledged after a later epoch had opened
  without it.
- **unverified-ack**: a write was acknowledged with no read of the owner
  record, naming this node at this epoch, since the bucket held the write,
  and no fleet proof covered it.
- **epoch-reused**: an ownership claim did not advance the cell's epoch.
- **evicted-but-serving**: the core reported an eviction while the cell's
  runtime was still published.
- **evicted-pinned**: the core evicted a cell that held a regular or outbound
  WebSocket open.
- **uncovered-alarm**: the core evicted a cell with an armed alarm, and no
  wake entry in the bucket for that cell comes due by the alarm's time, so
  no node would wake it in time. An entry from an older installation counts,
  because the woken owner reads the alarm its database really holds. A
  runtime that a newer epoch already superseded is exempt: its entries were
  rightly retired.
- **crash-uncovered-alarm**: a process died holding an alarm whose handler
  told its client it was set, with no wake entry due by its time. As in
  production, a handler that sets an alarm answers only once the alarm's wake
  entry lands, and fails if that write fails. Its `fetch()` calls are not
  held, so with a fast fleet proof a third party can learn of the write
  before the entry lands, and the alarm waits for the next activation.
- **alarm-not-fired**: when the liveness probe ends, a cell's newest durable
  database still holds an alarm that came due more than 10 s earlier.
- **stale-read**: a response missed a write that a client had already been
  told succeeded when the request was sent.
- **liveness**: once every fault is over, each cell takes a write within
  30 s. Then the whole fleet stops for good, and one fresh node must restore
  every cell from the bucket alone.

In debug builds, the core's own `State::validate` runs after every event, and
a failure ends the run.

A check can judge only the phases the core actually reaches. The simulator
reads every cell's phase before and after each event
(`crates/sim/src/coverage.rs`). A sweep ends with the number of distinct
transitions its seeds took and the phases none of them entered, and
`--coverage` prints every transition with how many seeds took it. The fault tests require their runs to enter every
phase of `Phase`, so a simulator change that stops reaching one fails a test.
A phase the core enters and leaves within one event is not seen.

Each check has a planted bug (`Mutation` in `crates/sim/src/lib.rs`) in the
simulated shell or bucket. A test requires every check to catch its bug, so a
check cannot go blind without a test failing.

Some properties are only watched. A hit is printed (`WATCH`) but never fails a
run. A property is enforced only once every hit a sweep finds traces to the
core, not to the simulator or to the property itself.

- **resumed-superseded**: a clean reload resumed a database while the owner
  record named another node or a newer epoch.
- **audit-alarm-not-fired**: when the audit ends, a cell's newest durable
  database still holds an alarm that came due more than 10 s earlier.
- **claim-during-shutdown**: a claim sent while its node was draining or
  preserving took a cell that the record did not name that node for.
- **deploy-stuck**: 10 s after a deploy's maximum age, a cell still runs on an
  older generation.
- **idle-not-evicted**: a cell with nothing in flight, no socket and no alarm
  stayed resident long past its idle limit.
- **socket-outlives-runtime**: an outbound WebSocket stayed open after its
  cell's runtime stopped.
- **memory-not-relieved**: a node with no socket open stayed latched on
  memory pressure for over 10 s.
- **lost-handler-write**: a database opened without an alarm handler's
  commit that the output gate had proven. No client hears of such a write,
  so **lost-write** does not cover it, but the core acts on it: it settles the
  alarm and deletes the consumed alarm's wake entry.
- **alarm-undiscoverable**: after a change to the bucket, the alarm a restore
  of a cell would load was set by an acknowledged write, and no wake entry
  is due by its time.
- **unclaimed-handoff**: a peer told a donor it accepted a cell at an epoch
  that no claim by the peer's node installed.
- **base-deleted**: epoch GC deleted the epoch prefix a restore of the cell
  would read.
- **unjustified-loss**: a declared loss forgave an acknowledged write that a
  follower disk on a machine still in the fleet holds. The loss is declared
  after recovery uploads what it gathered, so it forgives only what that
  recovery could not restore.
- **reopen-stuck**: in a quiet fleet, a degraded log stayed degraded 10 s
  after it could have reopened: the bucket held everything it shipped, and
  a peer was running and reachable.
- **bundle-credit-unheld**: a bundle credit counted writes the bucket does
  not hold, neither in a prefix nor in a bundle a recovery can still drain.
- **fleet-proof-unbacked**: a fleet proof covered writes that a follower of
  the log's newest ensemble never fsynced.
- **credit-before-activation**: a recovery or a fold read a log record as
  having no fleet acks at a log epoch at which the fleet proved writes.

**deploy-stuck**, **idle-not-evicted**, **memory-not-relieved** and
**reopen-stuck** judge only
once the fleet is quiet, 15 s after the faults end, so that a bucket call lost
just before the end has timed out. The others judge at the moment their fact
arrives. **unclaimed-handoff**, **base-deleted**, **unjustified-loss**,
**bundle-credit-unheld**, **fleet-proof-unbacked** and
**credit-before-activation** have never fired on the
current simulator, and **alarm-undiscoverable** fires only after a known
bug lost the write behind it, so each has a planted bug that the test suite
requires it to catch, as it does for an enforced check. The sweep below is the evidence that the others can
fire, except **deploy-stuck**, which has never fired, so nothing yet shows
that it can. In 100,000 swarm seeds:

- 383 seeds kept an idle cell resident, 343 of them in a run where a
  walk-down had wedged: the core evicts idle cells only while it is not
  shedding.
- 320 seeds claimed during a preserve or the close that follows it, never
  during a drain. An eviction already in flight finishes, or an owner read
  lands, and the core claims the cell, though `begin_preserve` retires cold
  work.
- 54 seeds (60 with validation compiled out) resumed a superseded epoch:
  after faulty clocks, the slow clean reload below, or a claim during a
  preserve. Fewer seeds reload cleanly than before: a node that has evicted a
  cell to a snapshot never does (the shell bug below).
- 13 seeds (14 with validation compiled out) left an acknowledged alarm with
  no wake entry due by its time: all but one after a failed activation
  handed its cell away (below) and lost the write that set it, and one
  after the slow clean reload below (swarm 16143).
- 5 seeds (8 with validation compiled out) left a durable alarm unfired
  when the audit ended, every one in a run where a memory walk-down wedged.
- 145 seeds lost a proven alarm handler commit (183 with validation
  compiled out), all after a failed activation handed the cell away, the
  slow clean reload below, a clock fault, or a drain that released a
  swapped cell: the same failures that lose acknowledged writes, seen
  through an alarm.
- 129 seeds (169 with validation compiled out) left a degraded log
  unreopened. In 122 (160), `maintain` found no peer live by its own wall
  clock while a peer kept renewing, the clock-skew shell bug below; in 6
  (8), self-suspicion parked recruitment while every member was down,
  the third shell bug below. In one (swarm 71053), the leader had no lease
  of its own: its first lease write was lost and a bucket hang held its
  retries, so no reopen could apply.
- With validation compiled out, 1,859 seeds kept an outbound socket open
  after its runtime stopped: the live transport that a failed proof or
  ownership read leaves on a reset cell, which the live-transport invariant
  below catches. Of 45 such seeds traced in an earlier sweep, the same run
  with validation on
  stopped on that invariant at or before the hit in 43, and on another
  invariant first in 2.

The simulator does not yet model V8, or, in the fleet log, the recovery
claim, the incapable-peer quarantine, hedged retransmission of a slow
append, recruitment at startup, compaction of bundle rows, and bundle GC
(`crates/sim/src/world/node_log.rs` says how each is approximated). Its memory has no allocator slack, cgroup or
containers, no noise between samples, and returns a cell's whole share when
the cell goes, and its heap's the moment the heap is empty.

The core on `main` fails some seeds.

- **Stale reads.** A clean read is released without a read of the owner
  record, so it is only as fresh as its node's belief that it still owns the
  cell. A peer whose wall clock runs ahead by a large part of a lease (in
  one seed a 1.3 s step against a 2 s lease) can judge a lease expired
  while its holder's late renewal is still in flight.
  The renewal then lands, the holder keeps its lease, and nothing tells it
  that the peer took its cells. It answers reads of them from the old epoch
  until a write fails its ownership read, 8 s later in one seed. A VM
  suspend, which `CLOCK_MONOTONIC` does not count, does the same. Writes
  stay safe. The test suite excuses a stale read only in a run with skewed,
  stepped or suspended clocks, and runs chaos with true clocks too.
- **Stale reads after a slow clean reload.** A replacement process decides
  to resume its databases in place when it reads its predecessor's lease
  unexpired, not when its own lease write lands. If that write lands
  after the old lease expired, a peer may already have taken a cell over
  at a newer epoch, and the replacement serves reads from the old one.
  One seed in 100,000 (swarm 77423) needs no clock fault for this; the
  suite's seeds do not hit it, so one that did would fail. In the fleet
  posture it loses writes too (**late-ack**, **lost-write**): a fleet proof
  needs no read of the owner record, so the replacement acknowledges writes
  at the superseded epoch. A peer that read the predecessor's sealed lease
  takes the cell over without recovering the replacement's log (swarm
  58613, 77423).
- **Lost writes with faulty clocks in the fleet posture.** A peer whose wall
  clock has stepped ahead by most of a lease judges a live owner's lease
  expired, recovers its log and later claims the cell. With bucket proofs the
  owner's next write would fail its ownership read; a fleet proof needs
  none, so the owner goes on acknowledging writes the claim then restores
  without (swarm 38836, after a 0.9 s step against a 5 s lease). A VM
  suspend does the same (swarm 40737).
- **A failed activation hands the cell away** (**lost-write**). A cell
  epoch that ended without its final upload leaves acknowledged writes that
  only its node's followers and bundles hold, and only that node's tail
  names. When the cell next activates there, `fold_cell` finds no complete
  follower (the one that has every frame is down) and fails the
  activation, as it must, or the restore's bucket read fails, or a drain
  stops the activation before its restore runs (swarm 89631). The core then
  gives the cell away, by rebalance or release, and the peer that adopts
  it restores from the bucket alone: nothing folds the first node's tail
  for it, so the acknowledged writes are gone (swarm 92204, 32015).
  Production's healing pass, which may repair this later, is not modelled.
- **A drain releases a cell it stopped for a generation swap**
  (**lost-write**). A swap stop is the first half of moving a cell to a new
  isolate: it runs no durability barrier and leaves the database open
  (`StopCause::Swap` in `crates/celld/actor.rs`), because the same epoch
  starts again at once. If the node begins draining between the two
  halves, `runtime_stopped` parks the cell `Dormant`, and the drain releases
  its owner record like any other dormant cell's. Nothing uploaded the
  writes that only the log or a bundle proved, so the peer that takes the
  cell restores from the bucket without them (swarm 9331, 105 writes).
- **A degraded log waits forever on a tail** (**liveness**,
  **alarm-not-fired**). A leader whose follower missed a frame degrades,
  and reopens its log only once the bucket holds every row it shipped
  (`all_fragment_rows_tiered`), of stopped cells too. A cell reset while
  degraded leaves a tail whose shipped rows no bundle carried: the bundle
  loop flushes only open databases, and only the fold uploads a tail. The
  fold needs a complete follower, and the follower that missed the frame
  never becomes one, so the fold fails, the log never reopens, and the cell
  never activates on that node again (swarm 1809, 2864, 5099).
- **Eleven `State::validate` invariants:**
  - `indexed cell op N has no owning slot`.
  - `confirming cell … left the owner read in Fenced`.
  - The same in `Inactive`: a client gives up on the only request for a cell
    that a restarted node is still confirming it owns.
  - `handoffs N exceeds release ceiling N`: a drain begins while an
    eviction is already in flight.
  - `handoff cell … is outside the pipeline in Fenced`: a node loses its
    lease while it is handing a cell off, then gets a request for that cell.
  - `cell … has a live transport while not resident`: a failed proof or
    ownership read resets a cell that holds a regular or outbound WebSocket.
    One failed bucket read is enough, so the simulator opens few sockets
    under faults.
  - `cell … is firing an alarm while not resident`: an ownership read fails
    while the cell's alarm handler is running, or while its reply waits for
    the wake entry of the alarm it re-armed.
  - `handoff cell … is outside the pipeline in WaitingActivation`: a cell's
    alarm timer fires after its node evicted it and released its owner
    record.
  - The same in `ReadingOwner`: a request brings a cell back to the node
    that just released it, and the elected waker hints the cell while its
    owner read is in flight.
  - `occupied counter N disagrees with the walk N`: the startup scan finds
    two live epochs of one cell on disk, which production leaves until
    `prune_stale_live`.
  - `activation waiter … is queued twice`: the same scan, for a cell that
    a request is also waiting on.
- **A memory walk-down that wedges.** A node latches on pressure while a cell
  is still activating: the cell counts as occupied but holds no memory yet.
  The walk-down evicts one cell, the activating cell's memory lands, and the
  next sample is unchanged. The futility stop
  (`shedding_can_reach_resume_line`) reads that as zero yield per eviction
  and stops shedding with the latch held, so the node refuses every new cell
  and never evicts an idle one. Nothing measures differently again, so it
  stays that way (**memory-not-relieved**, and **liveness** or
  **alarm-not-fired** when the cells it refuses have nowhere else to go).
- **A capacity livelock.** When every node is at its residency cap, a new
  cell is refused with `CapacityExhausted` indefinitely: no full node evicts
  an idle resident to make room, so the cell is never served (seed 1227).

Three shell bugs fail no check, and the simulator models each as production
behaves.

**An eviction snapshot abandons every clean reload.**
`prune_stale_live` deletes stale live databases, then requires the local
inventory to be exactly the resident set, but the inventory also lists
eviction snapshots (`db.evicted`), which it keeps. Once a node has evicted
a cell to a snapshot, its graceful exits write no clean-reload marker, and
its replacement restores from the bucket.

**Clock skew stops a degraded log from recruiting** (watched as
**reopen-stuck**). `NodeLogManager::maintain` in `crates/celld/node_log.rs`
counts a peer as recruitable only if `peer.expires_ms > now`, where `now` is
its own wall clock and `expires_ms` was stamped by the peer's. Once the two
clocks are stepped apart by more than the time a renewed lease has left,
which is less than the TTL, a node whose clock runs ahead, or whose peers'
clocks run behind, finds every peer expired and reports a fleet shortfall
on every tick. Its log stays degraded for as long as the skew lasts. Nothing
is lost and no check fails: the output gate rides the bundle loop, so writes
are acknowledged at the bucket's latency instead of the fleet's, and the
node's acknowledged writes have only the bucket as their witness. In 100,000
swarm seeds this held 122 logs degraded (160 with validation compiled out):
in each, `maintain` found no peer live by its own clock while a peer went
on renewing its lease. A fix would
judge a peer's liveness without comparing two machines' wall clocks, for
example by a successful probe, or by how long ago this node saw the peer's
lease change on its own monotonic clock.

**Self-suspicion parks recruitment while every member is down** (watched
as **reopen-stuck**). When every member of a leader's ensemble stalls past
the backstop at once, `evict_gray_followers` in `crates/celld/node_log.rs`
takes it for the leader's own partition: it degrades and sets
`suspect_self`, and `maintain` recruits no one until a member answers a
probe. Members that are paused or down answer nothing until they come
back, so the log stays degraded while live peers could follow it: 42 s in
swarm 36729, where both members paused and then crashed. As with the skew above, writes
go on at the bucket's latency and nothing is lost. In 100,000 swarm seeds
this held 6 logs degraded (8 with validation compiled out). A fix would
let an answer from any peer, such as a lease read that shows peers
renewing, lift the suspicion.

In 100,000 swarm seeds, 12,048 failed in a debug build: 10,343 on the
invariants (3,199 of them on live transports, 2,630 on alarms firing while
not resident), 1,017 on liveness or `alarm-not-fired`, 361 on stale reads,
321 on lost writes and 6 on late acknowledgements in the fleet posture. Of
the liveness and alarm failures, 802 are seeds where a node's memory
walk-down wedged (the node stayed latched, **memory-not-relieved**), 183 a
degraded log waiting on a tail, and 32 the capacity livelock; 1,254 seeds
kept a node latched on memory pressure in all. Of the lost writes, 320
followed a failed activation (306 a failed fold, 14 a failed bucket read)
and 1 a drain that released a swapped cell; every late acknowledgement
followed a clock step or a superseded resume. With validation compiled
out, 2,169 failed: 1,201 on liveness or `alarm-not-fired` (945 wedged, 217
waiting on a tail, 39 the capacity livelock), 436 on stale reads, 126 on
`evicted-pinned` (the core goes on to evict a cell whose live transport
the invariant would have caught), 400 on lost writes in the fleet posture
(399 after a failed activation, 1 after a drain released a swapped cell),
and 6 on late acknowledgements. Between them, the seeds entered every
phase.

Lost writes in the fleet posture rise and fall with how long logs stay
degraded, since a failed activation needs a tail that only followers and
bundles hold. Gray followers degrade logs more often.

The known failures are listed seed by seed in
[`crates/sim/known-failures.txt`](../crates/sim/known-failures.txt): every
failing seed of the suite's configurations and of swarm seeds 0..10000, in
both modes, with every property kind it breaks and the invariant it stops on.
A run is known only if it fails exactly as listed, so a new bug that fails
like a known one in another seed, or adds to a known seed's failure, still
fails the suite and the sweep.

```
cargo test -p celld-sim                                    # the suite
cargo run -p celld-sim -- --seeds 0..1000 --swarm          # a sweep
cargo run -p celld-sim -- --seed 1562 --swarm --trace      # replay one seed
cargo run -p celld-sim -- --seeds 0..20 --crashes 6 --mutation IgnoreCasGuard
cargo run -p celld-sim -- --seeds 0..1000 --swarm --coverage  # every transition
cargo run -p celld-sim -- --seeds 0..10000 --swarm --known crates/sim/known-failures.txt
cargo run -p celld-sim -- --seeds 0..10000 --swarm --record   # the list's lines
```

A sweep prints one line for each failing seed. A large sweep runs faster with
`CARGO_PROFILE_DEV_OPT_LEVEL=2`, which keeps debug assertions on. The
[Simulation workflow](../.github/workflows/sim.yml) runs the suite in the
debug profile on every change to `crates/logic` or `crates/sim`, and judges
swarm seeds 0..10000 against the known failures in both modes
(`crates/sim/sweep.sh`).

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

Report a schedule that breaks a guarantee, an untested fault, or a measurement
that you cannot reproduce in the
[issue tracker](https://github.com/denoland/celld/issues).
