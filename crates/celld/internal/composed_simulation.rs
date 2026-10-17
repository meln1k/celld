// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The composed simulation: real celld actors on simulated domains.
//
// Each node is an `Actor` driven by `drive`, the same start-step loop as
// `Actor::run`, with its effects spawned on the node's domain and its timers
// armed on the domain's monotonic clock. The node's core events go to an
// observer, so a run's history is the events every node applied, in order.

#[path = "composed_simulation/storage.rs"]
mod storage;

#[path = "composed_simulation/fleet.rs"]
mod fleet;

use crate::actor::{
    Actor, ActorIdentity, ActorInput, AdmissionLimits, Message, StepOutput, TimerArm, TimerSlots,
};
use crate::asyncrt::{self, sim::Domain, sim::Simulation};
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Every core event a node applied, as `node: event`, in order.
type History = Arc<Mutex<Vec<String>>>;

/// The production start-step loop, on the simulated runtime.
async fn drive(mut actor: Actor, mut messages: mpsc::UnboundedReceiver<Message>) {
    let mut out = StepOutput::default();
    let mut effects = FuturesUnordered::new();
    let mut armed: BTreeMap<(u64, u64), TimerArm> = BTreeMap::new();
    let mut slots = TimerSlots::<(u64, u64)>::default();
    actor.start(&mut out);
    loop {
        for effect in out.take_effects() {
            let task = asyncrt::spawn(effect);
            effects.push(async move { task.await.expect("actor effect task panicked") });
        }
        for arm in out.timers.drain(..) {
            let key = (arm.at_mono_ms, arm.ordinal);
            if let Some(displaced) = slots.install(&arm, key) {
                armed.remove(&displaced);
            }
            armed.insert(key, arm);
        }
        let next = armed.keys().next().copied();
        let timer = async move {
            match next {
                Some((at_mono_ms, _)) => asyncrt::sleep_until(at_mono_ms).await,
                None => std::future::pending().await,
            }
        };
        let input = asyncrt::select! {
            message = messages.recv() => match message {
                Some(message) => ActorInput::Message(message),
                None => return,
            },
            Some(completed) = effects.next(), if !effects.is_empty() => {
                ActorInput::Completed(completed)
            }
            () = timer => {
                let (_, arm) = armed.pop_first().expect("a timer was armed");
                if slots.fire(&arm.slot, arm.ordinal).is_none() {
                    continue;
                }
                ActorInput::TimerFired(arm.timer)
            }
        };
        actor.step(input, &mut out);
    }
}

/// Start node `name` on `sim`, recording each core event it applies and the
/// effects the core answered with. Returns its domain and the sender that
/// keeps it running.
fn start_node(
    sim: &Simulation,
    name: &str,
    history: &History,
) -> (Domain, mpsc::UnboundedSender<Message>) {
    let domain = sim.domain(name);
    let (messages, inbox) = mpsc::unbounded_channel();
    let (fence, _fenced) = mpsc::unbounded_channel();
    let node = name.to_string();
    let history = history.clone();
    let actor = sim.block_on(&domain, async move {
        let mut actor = Actor::from_environment(
            AdmissionLimits {
                resident: 4,
                activations: 2,
                evictions: 2,
                releases: 2,
                placement_weight: 1,
            },
            false,
            fence,
            None,
            None,
            ActorIdentity {
                node: node.clone(),
                advertise: format!("{node}:4000"),
                region: "sim".into(),
            },
            None,
        )
        .await
        .expect("the actor builds");
        actor.observe_core_for_test(Arc::new(move |event, effects| {
            history
                .lock()
                .unwrap()
                .push(format!("{node}: {event:?} => {effects:?}"));
        }));
        actor
    });
    domain.spawn(drive(actor, inbox)).detach();
    (domain, messages)
}

/// A client on `domain` that sends requests for six cells at seeded
/// intervals and records every answer.
fn start_client(domain: &Domain, messages: mpsc::UnboundedSender<Message>, history: &History) {
    let history = history.clone();
    let node = domain.name().to_string();
    domain
        .spawn(async move {
            use rand::Rng as _;
            let mut rng = asyncrt::rng("client");
            for request in 1.. {
                asyncrt::sleep(std::time::Duration::from_millis(rng.gen_range(20..400))).await;
                let cell = format!("c{}", rng.gen_range(0..6));
                let (reply, answer) = tokio::sync::oneshot::channel();
                let sent = messages.send(Message::Request {
                    request,
                    cell: cell.clone(),
                    capacity_handoff: false,
                    handoff_accept: None,
                    resident_work: None,
                    reply,
                });
                if sent.is_err() {
                    return;
                }
                let history = history.clone();
                let node = node.clone();
                asyncrt::spawn(async move {
                    let answer = answer.await.map(|routed| routed.map(|routed| routed.route));
                    history.lock().unwrap().push(format!(
                        "{node}: request {request} for {cell} answered {answer:?} @{}",
                        asyncrt::mono_ms()
                    ));
                })
                .detach();
            }
        })
        .detach();
}

/// The proof workload: three nodes with clients, n1 crashing at 20 s and
/// coming back at 25 s, for a minute of virtual time. Returns the core
/// history, every scheduling decision, and the run's totals.
fn fleet_run(seed: u64) -> Vec<String> {
    let sim = Simulation::new(seed);
    sim.record_trace();
    let history = History::default();
    let mut nodes = Vec::new();
    for name in ["n0", "n1", "n2"] {
        let (domain, messages) = start_node(&sim, name, &history);
        start_client(&domain, messages.clone(), &history);
        nodes.push((domain, messages));
    }
    sim.run_for(20_000);
    let dropped = nodes[1].0.crash();
    history.lock().unwrap().push(format!(
        "crash n1: {dropped} tasks dropped @{}",
        sim.now_ms()
    ));
    sim.run_for(5_000);
    let (domain, messages) = start_node(&sim, "n1", &history);
    start_client(&domain, messages.clone(), &history);
    nodes[1] = (domain, messages);
    sim.run_for(35_000);

    let mut lines = vec!["== history".to_string()];
    lines.extend(history.lock().unwrap().iter().cloned());
    lines.push("== schedule".into());
    lines.extend(sim.take_trace());
    lines.push(format!(
        "== totals: seed {seed} polls {} now {} tasks {} fingerprint {:016x}",
        sim.polls(),
        sim.now_ms(),
        sim.task_count(),
        sim.fingerprint()
    ));
    lines
}

/// One node with in-memory ownership, for `ms` of virtual time.
fn run_one_node(seed: u64, ms: u64) -> Vec<String> {
    let sim = Simulation::new(seed);
    let history = History::default();
    let (_node, _messages) = start_node(&sim, "n0", &history);
    sim.run_for(ms);
    let history = history.lock().unwrap().clone();
    history
}

#[test]
fn a_node_keeps_its_lease_in_virtual_time() {
    let history = run_one_node(1, 60_000);
    let renewals = history
        .iter()
        .filter(|event| event.starts_with("n0: NodeLeaseCasCompleted") && event.contains("Applied"))
        .count();
    // The default TTL is 10 s, so a minute holds several renewals, and the
    // node must never have lost authority on the way.
    assert!(
        renewals >= 6,
        "{renewals} renewals in a minute:\n{}",
        history.join("\n")
    );
    assert!(
        !history
            .iter()
            .any(|event| event.starts_with("n0: TimerFired { timer: NodeLeaseFence")),
        "the node fenced:\n{}",
        history.join("\n")
    );
}

#[test]
fn a_seed_replays_the_same_run_in_process() {
    let first = fleet_run(5);
    assert!(
        first.len() > 1_000,
        "the workload ran {} lines",
        first.len()
    );
    assert_eq!(first, fleet_run(5));
    assert_ne!(first, fleet_run(6));

    let first = storage::power_loss_run(5, "n1").history;
    assert!(
        first.len() > 1_000,
        "the power-loss run ran {} lines",
        first.len()
    );
    assert_eq!(first, storage::power_loss_run(5, "n1").history);
    assert_ne!(first, storage::power_loss_run(6, "n1").history);

    let first = fleet::run(5, None).trace;
    assert!(
        first.len() > 1_000,
        "the faulted fleet ran {} lines",
        first.len()
    );
    assert_eq!(first, fleet::run(5, None).trace);
    assert_ne!(first, fleet::run(6, None).trace);
}

/// The child half of `a_seed_replays_across_processes`: write the run of
/// `CELLD_SIM_SEED` to `CELLD_SIM_TRACE_OUT`.
#[test]
#[ignore = "run by a_seed_replays_across_processes in a fresh process"]
// The trace file is the test's output on the host, outside the simulation.
#[allow(clippy::disallowed_methods)]
fn replay_child() {
    let seed = std::env::var("CELLD_SIM_SEED")
        .expect("CELLD_SIM_SEED")
        .parse()
        .unwrap();
    let out = std::env::var("CELLD_SIM_TRACE_OUT").expect("CELLD_SIM_TRACE_OUT");
    let mut lines = fleet_run(seed);
    lines.push("== power loss".into());
    let run = storage::power_loss_run(seed, "n1");
    lines.extend(run.history);
    lines.push("== bucket".into());
    lines.extend(run.objects);
    lines.push("== faulted fleet".into());
    lines.extend(fleet::run(seed, None).trace);
    let mut text = lines.join("\n");
    text.push('\n');
    std::fs::write(&out, text).unwrap();
    // What this process had that a seed must not depend on, beside the run.
    let host = {
        use std::hash::BuildHasher as _;
        let keys = std::collections::hash_map::RandomState::new().hash_one("probe");
        let heap = Box::new(0u8);
        let clock = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!(
            "pid {} hashmap-keys {keys:016x} heap {:p} host-clock-ns {clock} thread {:?}\n",
            std::process::id(),
            heap,
            std::thread::current().id()
        )
    };
    std::fs::write(format!("{out}.host"), host).unwrap();
}

/// A fresh process differs in everything a seed must not depend on: its
/// pid, its address layout, the keys of every `HashMap`, the host clock.
/// Two of them must still write the same run, byte for byte.
#[test]
// The children's traces are files on the host, outside the simulation.
#[allow(clippy::disallowed_methods)]
fn a_seed_replays_across_processes() {
    let directory = tempfile::tempdir().unwrap();
    let run = |seed: u64, name: &str| {
        let out = directory.path().join(name);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "composed_simulation::replay_child"])
            .env("CELLD_SIM_SEED", seed.to_string())
            .env("CELLD_SIM_TRACE_OUT", &out)
            .status()
            .unwrap();
        assert!(status.success(), "the child run of seed {seed} failed");
        std::fs::read(out).unwrap()
    };
    let first = run(11, "first");
    assert_eq!(
        first,
        run(11, "second"),
        "seed 11 ran differently in two processes"
    );
    assert_ne!(first, run(12, "other"), "seeds 11 and 12 ran the same");
}
