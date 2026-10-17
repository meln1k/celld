// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A fleet of storage nodes under faults, its clients, and the checks on
//! what the clients saw.
//!
//! Three nodes share one bucket, and each keeps its cells on its own disk.
//! Clients outside every node append to and read a few cells through
//! whichever node they pick, and follow a redirect to the owner. While they
//! run, the seed draws faults, most of them on a node that runs cells: a
//! process crash, a power loss, a machine that returns with an empty disk,
//! a pause long or short against the lease, a partition from the bucket, a
//! wall-clock step, and throughout, failed, lost and slow store requests.
//! A dead node comes back on its disk after a while, and a node that fences
//! itself exits and is restarted, as a supervisor would. Then every fault
//! heals, and every node must serve every cell.
//!
//! The checks:
//!
//! - **lost-write**: a read that began after a write was acknowledged
//!   holds that write.
//! - **fork**: any two reads of a cell agree, one a prefix of the other,
//!   and no read holds a value twice or one nobody wrote.
//! - **one-writer**: no two processes start a runtime for one cell at one
//!   epoch.
//! - **liveness**: once the faults heal, each node serves each cell.
//!
//! Each `Mutation` plants a bug that one of these must catch.

use super::storage::{serve, start_storage_node, Runtimes, Served, StorageNode};
use super::History;
use crate::asyncrt::{self, sim::Simulation};
use crate::conformance_sim_store::{SimDisk, SimStore, StoreFaults};
use rand::{Rng as _, SeedableRng as _};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NODES: [&str; 3] = ["n0", "n1", "n2"];
const CELLS: [&str; 6] = ["do:c0", "do:c1", "do:c2", "do:c3", "do:c4", "do:c5"];
const CLIENTS: usize = 8;
/// Virtual time with clients and faults, then with neither.
const RUN_MS: u64 = 40_000;
const HEAL_MS: u64 = 30_000;

/// A planted bug the checks must catch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mutation {
    /// The output gate releases an answer before its durability proof.
    EarlyAck,
    /// The bucket's compare-and-swap writes apply whatever the tag.
    BlindCas,
}

/// One operation as its client saw it, timed on the simulation's clock.
#[derive(Clone, Debug)]
struct Op {
    cell: &'static str,
    /// The value appended, for a write.
    write: Option<String>,
    start_ms: i64,
    end_ms: i64,
    /// The answer: `Ok` with the log for a read, `Ok` with nothing for a
    /// write, the failure otherwise.
    result: Result<Option<Vec<String>>, String>,
}

/// What one fleet run did and what the checks found.
pub(super) struct FleetRun {
    pub(super) violations: Vec<String>,
    pub(super) acknowledged: usize,
    pub(super) reads: usize,
    pub(super) faults: Vec<String>,
    /// Every fault, operation and core event, for a replay.
    pub(super) trace: Vec<String>,
}

/// The nodes alive now, by name.
type Directory = Arc<Mutex<BTreeMap<String, StorageNode>>>;

struct Fleet {
    sim: Simulation,
    store: SimStore,
    history: History,
    directory: Directory,
    disks: BTreeMap<&'static str, SimDisk>,
    mutation: Option<Mutation>,
    rng: rand::rngs::StdRng,
    /// The faults still to heal, by when, in the order they were drawn.
    agenda: BTreeMap<(u64, usize), Heal>,
    drawn: usize,
    /// Every process started, by node and incarnation, with the runtimes
    /// it started.
    processes: Vec<(String, Runtimes)>,
    faults: Vec<String>,
}

/// How a node dies.
#[derive(Clone, Copy, Debug)]
enum Death {
    /// The process: its disk stays as it was.
    Crash,
    /// The machine: its disk keeps only what was synced.
    PowerLoss,
    /// The machine, which comes back with an empty disk, so only the
    /// bucket remembers its cells.
    Wipe,
}

/// A fault that ends later, and how.
#[derive(Clone, Copy, Debug)]
enum Heal {
    Restart(&'static str),
    Resume(&'static str),
    Reconnect(&'static str),
}

impl Fleet {
    fn note(&mut self, fault: String) {
        self.faults.push(format!("@{} {fault}", self.sim.now_ms()));
    }

    fn start(&mut self, name: &'static str) {
        let node = start_storage_node(
            &self.sim,
            name,
            &self.store,
            &self.disks[name],
            &self.history,
        );
        let incarnation = self
            .processes
            .iter()
            .filter(|(process, _)| process.split('#').next() == Some(name))
            .count();
        self.processes
            .push((format!("{name}#{incarnation}"), node.runtimes.clone()));
        if self.mutation == Some(Mutation::EarlyAck) {
            node.app
                .scripted_activity_host
                .as_ref()
                .expect("a storage node has a host")
                .set_ack_before_proof(true);
        }
        self.directory
            .lock()
            .unwrap()
            .insert(name.to_string(), node);
    }

    /// Kill `name` by `death`. Returns whether it was alive.
    fn kill(&mut self, name: &'static str, death: Death) -> bool {
        let Some(node) = self.directory.lock().unwrap().remove(name) else {
            return false;
        };
        let disk = &self.disks[name];
        match death {
            Death::Crash => disk.crash(),
            Death::PowerLoss => disk.power_loss(),
            Death::Wipe => disk.wipe(),
        }
        node.domain.crash();
        true
    }

    fn node(&self, name: &str) -> Option<StorageNode> {
        self.directory.lock().unwrap().get(name).cloned()
    }

    fn schedule(&mut self, at: u64, heal: Heal) {
        self.drawn += 1;
        self.agenda.insert((at, self.drawn), heal);
    }

    /// Run until `until`, reaping each node that exits and healing each
    /// fault as it comes due. A supervisor starts a node that exited again
    /// on its own disk, as systemd would.
    fn advance_to(&mut self, until: u64) {
        while self.sim.now_ms() < until {
            let due = self.agenda.keys().next().map_or(u64::MAX, |(at, _)| *at);
            let next = (self.sim.now_ms() + 100).min(due).min(until);
            self.sim.run_for(next - self.sim.now_ms());
            let exited: Vec<(&'static str, String)> = NODES
                .into_iter()
                .filter_map(|name| Some((name, self.node(name)?.exited.lock().unwrap().clone()?)))
                .collect();
            for (name, reason) in exited {
                self.kill(name, Death::Crash);
                let after = self.rng.gen_range(500..3_000);
                self.note(format!(
                    "{name} exited: {reason}; the supervisor restarts it in {after} ms"
                ));
                self.schedule(self.sim.now_ms() + after, Heal::Restart(name));
            }
            while let Some(entry) = self.agenda.first_entry() {
                if entry.key().0 > self.sim.now_ms() {
                    break;
                }
                let heal = entry.remove();
                self.heal(heal);
            }
        }
    }

    /// Heal every fault now.
    fn heal_all(&mut self) {
        for heal in std::mem::take(&mut self.agenda).into_values() {
            self.heal(heal);
        }
    }

    fn heal(&mut self, heal: Heal) {
        match heal {
            Heal::Restart(name) => {
                if self.node(name).is_none() {
                    self.start(name);
                    self.note(format!("restart {name}"));
                }
            }
            Heal::Resume(name) => {
                if let Some(node) = self.node(name) {
                    node.domain.set_paused(false);
                    self.note(format!("resume {name}"));
                }
            }
            Heal::Reconnect(name) => {
                self.store.set_partitioned(name, false);
                self.note(format!("reconnect {name}"));
            }
        }
    }

    /// Draw one fault, apply it, and schedule its healing.
    fn inject(&mut self) {
        if let Some((at, heal)) = self.draw() {
            self.schedule(at, heal);
        }
    }

    fn draw(&mut self) -> Option<(u64, Heal)> {
        // An adversary strikes where it hurts: most faults land on a node
        // that runs cells.
        let owners: Vec<&'static str> = NODES
            .into_iter()
            .filter(|name| {
                self.node(name).is_some_and(|node| {
                    let host = node.app.scripted_activity_host.as_ref();
                    !host
                        .expect("a storage node has a host")
                        .resident_cells()
                        .is_empty()
                })
            })
            .collect();
        let name = if !owners.is_empty() && self.rng.gen_bool(0.75) {
            owners[self.rng.gen_range(0..owners.len())]
        } else {
            NODES[self.rng.gen_range(0..NODES.len())]
        };
        // Shorter and longer than the 10 s lease, so some faults move
        // ownership and some do not.
        let lasts = self.rng.gen_range(1_000..15_000);
        let until = self.sim.now_ms() + lasts;
        match self.rng.gen_range(0..5) {
            0 | 1 => {
                let death = [Death::Crash, Death::PowerLoss, Death::Wipe][self.rng.gen_range(0..3)];
                if !self.kill(name, death) {
                    return None;
                }
                self.note(format!("{death:?} {name} for {lasts} ms"));
                Some((until, Heal::Restart(name)))
            }
            2 => {
                let node = self.node(name)?;
                node.domain.set_paused(true);
                self.note(format!("pause {name} for {lasts} ms"));
                Some((until, Heal::Resume(name)))
            }
            3 => {
                self.store.set_partitioned(name, true);
                self.note(format!("partition {name} for {lasts} ms"));
                Some((until, Heal::Reconnect(name)))
            }
            _ => {
                let node = self.node(name)?;
                let step = self.rng.gen_range(-2_000..2_000);
                node.domain.step_wall_clock(step);
                self.note(format!("step {name}'s wall clock by {step} ms"));
                None
            }
        }
    }
}

/// One request from a client: travel to `node`, have it served there, and
/// follow redirects to the owner.
async fn request(
    directory: &Directory,
    mut node: String,
    cell: &str,
    method: &'static str,
    body: &str,
) -> Result<String, String> {
    let mut rng = asyncrt::rng("network");
    for _ in 0..3 {
        asyncrt::sleep(Duration::from_millis(rng.gen_range(1..5))).await;
        let Some(target) = directory.lock().unwrap().get(&node).cloned() else {
            return Err(format!("{node} is unreachable"));
        };
        let (app, cell, body) = (target.app.clone(), cell.to_string(), body.to_string());
        // The request runs on the node, so it dies with the node.
        let served = target
            .domain
            .spawn(async move { serve(&app, &cell, method, &body).await });
        let served = asyncrt::timeout(Duration::from_secs(10), served).await;
        asyncrt::sleep(Duration::from_millis(rng.gen_range(1..5))).await;
        match served {
            Ok(Ok(Served::Answer(answer))) => return Ok(answer),
            Ok(Ok(Served::Elsewhere(owner))) => node = owner,
            Ok(Ok(Served::Failed(error))) => return Err(error),
            Ok(Err(_)) => return Err(format!("{node} died mid-request")),
            Err(_) => return Err(format!("{node} timed out")),
        }
    }
    Err("too many redirects".into())
}

fn log_of(answer: &str) -> Vec<String> {
    answer
        .split(',')
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

/// A client: think, pick a cell and a node, append or read, record it.
async fn client(
    index: usize,
    directory: Directory,
    ops: Arc<Mutex<Vec<Op>>>,
    stop: Arc<AtomicBool>,
) {
    let mut rng = asyncrt::rng("client");
    let mut writes = 0;
    while !stop.load(Ordering::SeqCst) {
        asyncrt::sleep(Duration::from_millis(rng.gen_range(20..200))).await;
        let cell = CELLS[rng.gen_range(0..CELLS.len())];
        let node = NODES[rng.gen_range(0..NODES.len())].to_string();
        let write = rng.gen_bool(0.7).then(|| {
            writes += 1;
            format!("k{index}.{writes}")
        });
        let start_ms = asyncrt::sim::world_wall_ms();
        let result = match &write {
            Some(value) => request(&directory, node, cell, "POST", value)
                .await
                .map(|_| None),
            None => request(&directory, node, cell, "GET", "")
                .await
                .map(|answer| Some(log_of(&answer))),
        };
        ops.lock().unwrap().push(Op {
            cell,
            write,
            start_ms,
            end_ms: asyncrt::sim::world_wall_ms(),
            result,
        });
    }
}

/// Run the fleet for `seed`, with `mutation` planted, and check it.
pub(super) fn run(seed: u64, mutation: Option<Mutation>) -> FleetRun {
    let sim = Simulation::new(seed);
    let store = SimStore::new(StoreFaults {
        latency_ms: 5..40,
        unavailable_per_mille: 5,
        lost_response_per_mille: 5,
        slow_per_mille: 20,
    });
    store.set_blind_writes(mutation == Some(Mutation::BlindCas));
    let mut fleet = Fleet {
        rng: rand::rngs::StdRng::seed_from_u64(seed ^ 0x5eed_f1ee7),
        agenda: BTreeMap::new(),
        drawn: 0,
        processes: Vec::new(),
        sim: sim.clone(),
        store,
        history: History::default(),
        directory: Directory::default(),
        disks: NODES.iter().map(|name| (*name, SimDisk::new())).collect(),
        mutation,
        faults: Vec::new(),
    };
    for name in NODES {
        fleet.start(name);
    }

    let ops = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    for index in 0..CLIENTS {
        sim.domain(&format!("client{index}"))
            .spawn(client(
                index,
                fleet.directory.clone(),
                ops.clone(),
                stop.clone(),
            ))
            .detach();
    }

    while sim.now_ms() < RUN_MS {
        let next_fault = sim.now_ms() + fleet.rng.gen_range(2_000..6_000);
        fleet.advance_to(next_fault.min(RUN_MS));
        if sim.now_ms() < RUN_MS {
            fleet.inject();
        }
    }

    // Heal everything, let the clients stop, and let ownership settle.
    stop.store(true, Ordering::SeqCst);
    fleet.heal_all();
    fleet.store.set_faults(StoreFaults::default());
    fleet.note("every fault healed".into());
    fleet.advance_to(RUN_MS + HEAL_MS);

    // Liveness: every node serves every cell.
    let mut violations = Vec::new();
    let checker = sim.domain("checker");
    for name in NODES {
        for cell in CELLS {
            let directory = fleet.directory.clone();
            let start_ms = checker.enter(asyncrt::sim::world_wall_ms);
            let served = sim.block_on(&checker, async move {
                let deadline = asyncrt::mono_ms() + 20_000;
                loop {
                    let answer = request(&directory, name.to_string(), cell, "GET", "").await;
                    if answer.is_ok() || asyncrt::mono_ms() >= deadline {
                        return answer;
                    }
                    asyncrt::sleep(Duration::from_millis(500)).await;
                }
            });
            match &served {
                Ok(_) => {}
                Err(error) => violations.push(format!(
                    "liveness: {name} never served {cell} after healing: {error}"
                )),
            }
            ops.lock().unwrap().push(Op {
                cell,
                write: None,
                start_ms,
                end_ms: checker.enter(asyncrt::sim::world_wall_ms),
                result: served.map(|answer| Some(log_of(&answer))),
            });
        }
    }

    let ops = ops.lock().unwrap().clone();
    violations.extend(check(&ops));
    violations.extend(one_writer_per_epoch(&fleet.processes));
    let mut trace = fleet.faults.clone();
    trace.extend(ops.iter().map(|op| format!("{op:?}")));
    trace.extend(fleet.history.lock().unwrap().iter().cloned());
    FleetRun {
        violations,
        acknowledged: ops
            .iter()
            .filter(|op| op.write.is_some() && op.result.is_ok())
            .count(),
        reads: ops
            .iter()
            .filter(|op| matches!(op.result, Ok(Some(_))))
            .count(),
        faults: fleet.faults,
        trace,
    }
}

/// No two processes ever started a runtime for one cell at one epoch.
fn one_writer_per_epoch(processes: &[(String, Runtimes)]) -> Vec<String> {
    let mut writers: BTreeMap<(String, u64), BTreeSet<&str>> = BTreeMap::new();
    for (process, runtimes) in processes {
        for claim in runtimes.lock().unwrap().iter() {
            writers.entry(claim.clone()).or_default().insert(process);
        }
    }
    writers
        .into_iter()
        .filter(|(_, processes)| processes.len() > 1)
        .map(|((cell, epoch), processes)| {
            format!("one-writer: {processes:?} all ran {cell} at epoch {epoch}")
        })
        .collect()
}

/// The lost-write and fork checks over every operation.
fn check(ops: &[Op]) -> Vec<String> {
    let mut violations = Vec::new();
    let written: BTreeSet<&str> = ops.iter().filter_map(|op| op.write.as_deref()).collect();
    for cell in CELLS {
        let acked: Vec<(&str, i64)> = ops
            .iter()
            .filter(|op| op.cell == cell && op.result.is_ok())
            .filter_map(|op| Some((op.write.as_deref()?, op.end_ms)))
            .collect();
        let reads: Vec<(&Vec<String>, i64)> = ops
            .iter()
            .filter(|op| op.cell == cell)
            .filter_map(|op| match &op.result {
                Ok(Some(log)) => Some((log, op.start_ms)),
                _ => None,
            })
            .collect();
        for (log, start_ms) in &reads {
            for (value, acked_ms) in &acked {
                if acked_ms < start_ms && !log.iter().any(|seen| seen == value) {
                    violations.push(format!(
                        "lost-write: {cell} acknowledged {value} at {acked_ms}, \
                         a read from {start_ms} lacks it"
                    ));
                }
            }
            let distinct: BTreeSet<&String> = log.iter().collect();
            if distinct.len() != log.len() {
                violations.push(format!(
                    "fork: a read of {cell} holds a value twice: {log:?}"
                ));
            }
            if let Some(stranger) = log.iter().find(|value| !written.contains(value.as_str())) {
                violations.push(format!(
                    "fork: a read of {cell} holds {stranger}, never written"
                ));
            }
        }
        for (i, (a, _)) in reads.iter().enumerate() {
            for (b, _) in &reads[i + 1..] {
                let shared = a.len().min(b.len());
                if a[..shared] != b[..shared] {
                    violations.push(format!(
                        "fork: two reads of {cell} disagree: {a:?} and {b:?}"
                    ));
                }
            }
        }
    }
    violations.sort();
    violations.dedup();
    violations
}

/// Under every fault the seed draws, no acknowledged write is lost, the
/// cells' histories never fork, no epoch has two writers, and the fleet
/// serves again once healed.
#[test]
fn a_fleet_under_faults_keeps_every_acknowledged_write() {
    for seed in 0..3 {
        let outcome = run(seed, None);
        assert!(
            outcome.violations.is_empty(),
            "seed {seed}: {} violations, first {:?}\nfaults: {:#?}",
            outcome.violations.len(),
            &outcome.violations[..outcome.violations.len().min(5)],
            outcome.faults
        );
        assert!(
            outcome.acknowledged >= 100 && outcome.reads >= 50 && outcome.faults.len() >= 6,
            "seed {seed} exercised too little: {} acknowledged writes, {} reads, faults {:#?}",
            outcome.acknowledged,
            outcome.reads,
            outcome.faults
        );
    }
}

/// Each planted bug fails the check meant for it within a few seeds, so
/// the checks see what they claim to.
#[test]
fn every_planted_bug_is_caught() {
    for (mutation, check) in [
        (Mutation::EarlyAck, "lost-write"),
        (Mutation::BlindCas, "one-writer"),
    ] {
        let caught = (0..8).any(|seed| {
            let outcome = run(seed, Some(mutation));
            outcome
                .violations
                .iter()
                .any(|violation| violation.starts_with(check))
        });
        assert!(caught, "{mutation:?} passed {check} for eight seeds");
    }
}

/// `CELLD_SIM_SEEDS=0..200 cargo test … fleet_sweep -- --ignored` runs a
/// sweep, `CELLD_SIM_MUTATION=early-ack` or `blind-cas` plants a bug in
/// it, and `CELLD_SIM_TRACE=1` prints a failing seed's whole trace.
#[test]
#[ignore = "a sweep, run on demand"]
fn fleet_sweep() {
    let seeds = std::env::var("CELLD_SIM_SEEDS").unwrap_or_else(|_| "0..20".into());
    let (from, to) = seeds.split_once("..").expect("CELLD_SIM_SEEDS is FROM..TO");
    let (from, to): (u64, u64) = (from.parse().unwrap(), to.parse().unwrap());
    let mutation = std::env::var("CELLD_SIM_MUTATION")
        .ok()
        .map(|name| match name.as_str() {
            "early-ack" => Mutation::EarlyAck,
            "blind-cas" => Mutation::BlindCas,
            other => panic!("no mutation {other}"),
        });
    let mut failed = Vec::new();
    for seed in from..to {
        let outcome = run(seed, mutation);
        let exits = outcome
            .faults
            .iter()
            .filter(|fault| fault.contains("exited"))
            .count();
        eprintln!(
            "seed {seed}: {} violations, {} acknowledged writes, {} reads, {} faults, {exits} exits",
            outcome.violations.len(),
            outcome.acknowledged,
            outcome.reads,
            outcome.faults.len(),
        );
        if !outcome.violations.is_empty() {
            eprintln!("{:#?}\nfaults: {:#?}", outcome.violations, outcome.faults);
            if std::env::var_os("CELLD_SIM_TRACE").is_some() {
                eprintln!("{}", outcome.trace.join("\n"));
            }
            failed.push(seed);
        }
    }
    assert!(failed.is_empty(), "failing seeds: {failed:?}");
}
