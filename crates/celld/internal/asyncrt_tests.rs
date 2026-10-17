// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Contract tests for the simulated `asyncrt`: what a world may rely on.

use crate::asyncrt::{self, sim::Simulation};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Tasks that sleep, wake each other and log the order they ran in.
fn racing_log(seed: u64) -> (Vec<String>, u64, u64) {
    let sim = Simulation::new(seed);
    let node = sim.domain("n0");
    let log = Arc::new(Mutex::new(Vec::new()));
    let notify = Arc::new(tokio::sync::Notify::new());
    for worker in 0..6u64 {
        let log = log.clone();
        let notify = notify.clone();
        node.spawn(async move {
            for round in 0..4u64 {
                if worker % 2 == 0 {
                    asyncrt::sleep(Duration::from_millis(10 * (worker + round))).await;
                } else {
                    notify.notified().await;
                }
                log.lock()
                    .unwrap()
                    .push(format!("{worker}.{round}@{}", asyncrt::mono_ms()));
                notify.notify_waiters();
            }
        })
        .detach();
    }
    sim.run_until_idle(100_000);
    let log = log.lock().unwrap().clone();
    (log, sim.fingerprint(), sim.now_ms())
}

#[test]
fn a_seed_fixes_the_schedule() {
    let first = racing_log(7);
    assert_eq!(first, racing_log(7));
    assert!(!first.0.is_empty());
    // Six workers ready at once: some seed must order them differently.
    let orders: std::collections::BTreeSet<Vec<String>> =
        (0..8).map(|seed| racing_log(seed).0).collect();
    assert!(orders.len() > 1, "every seed ran the same interleaving");
}

#[test]
fn time_moves_only_to_the_next_timer() {
    let sim = Simulation::new(1);
    let node = sim.domain("n0");
    let fired = Arc::new(Mutex::new(Vec::new()));
    for (label, ms) in [
        ("late", 3_000u64),
        ("early", 250),
        ("tie-a", 1_000),
        ("tie-b", 1_000),
    ] {
        let fired = fired.clone();
        node.spawn(async move {
            asyncrt::sleep(Duration::from_millis(ms)).await;
            fired.lock().unwrap().push((label, asyncrt::mono_ms()));
        })
        .detach();
    }
    sim.run_until_idle(10_000);
    let fired = fired.lock().unwrap().clone();
    assert_eq!(fired[0], ("early", 250));
    assert_eq!(fired[3], ("late", 3_000));
    assert!(fired[1..3].iter().all(|(_, at)| *at == 1_000));
    assert_eq!(sim.now_ms(), 3_000);
}

#[test]
fn run_for_stops_at_its_horizon() {
    let sim = Simulation::new(1);
    let node = sim.domain("n0");
    let done = Arc::new(Mutex::new(false));
    let flag = done.clone();
    node.spawn(async move {
        asyncrt::sleep(Duration::from_millis(5_000)).await;
        *flag.lock().unwrap() = true;
    })
    .detach();
    sim.run_for(4_999);
    assert_eq!(sim.now_ms(), 4_999);
    assert!(!*done.lock().unwrap());
    sim.run_for(1);
    assert!(*done.lock().unwrap());
}

#[test]
fn a_timeout_elapses_at_its_deadline() {
    let sim = Simulation::new(1);
    let node = sim.domain("n0");
    let (early, late) = sim.block_on(&node, async {
        let early = asyncrt::timeout(Duration::from_millis(500), async {
            asyncrt::sleep(Duration::from_millis(499)).await;
        })
        .await;
        let started = asyncrt::mono_ms();
        let late = asyncrt::timeout(Duration::from_millis(500), std::future::pending::<()>()).await;
        (early, (late, asyncrt::mono_ms() - started))
    });
    assert!(early.is_ok());
    assert_eq!(late, (Err(asyncrt::Elapsed), 500));
}

#[test]
fn each_domain_keeps_its_own_clocks() {
    let sim = Simulation::new(1);
    let first = sim.domain("n0");
    sim.run_for(1_000);
    let second = sim.domain("n1");
    second.step_wall_clock(-200);
    sim.run_for(500);
    let read =
        |domain: &asyncrt::sim::Domain| domain.enter(|| (asyncrt::mono_ms(), asyncrt::wall_ms()));
    let wall = asyncrt::sim::WALL_EPOCH_MS + 1_500;
    assert_eq!(read(&first), (1_500, wall));
    assert_eq!(read(&second), (500, wall - 200));
    // A domain's deadline is on its own monotonic clock.
    let woke = sim.block_on(&second, async {
        asyncrt::sleep_until(800).await;
        asyncrt::mono_ms()
    });
    assert_eq!((woke, sim.now_ms()), (800, 1_800));
}

#[test]
fn a_crash_drops_every_task_of_its_domain() {
    let sim = Simulation::new(1);
    let doomed = sim.domain("n0");
    let survivor = sim.domain("n1");
    let waiting = doomed.spawn(async {
        asyncrt::sleep(Duration::from_millis(60_000)).await;
        "woke"
    });
    let token = doomed.services().domain_token().expect("bound token");
    let other = survivor.spawn(async {
        asyncrt::sleep(Duration::from_millis(100)).await;
        "other"
    });
    sim.run_for(10);
    assert_eq!(doomed.crash(), 1);
    // The dropped sleep's timer is gone, so time stops at the survivor's.
    sim.run_until_idle(1_000);
    assert_eq!(sim.now_ms(), 100);
    let outputs = sim.block_on(&survivor, async move { (waiting.await, other.await) });
    assert!(
        outputs.0.is_err(),
        "a crashed task's handle reports it cancelled"
    );
    assert_eq!(outputs.1, Ok("other"));
    assert!(token.mono_ms().is_err());
    assert!(token.spawn_detached("after", async {}).is_err());
}

#[test]
fn a_panic_reaches_only_its_own_handle() {
    let sim = Simulation::new(1);
    let node = sim.domain("n0");
    let (panicked, fine) = sim.block_on(&node, async {
        let panicked = asyncrt::spawn(async { panic!("injected") });
        let fine = asyncrt::spawn(async { 7 });
        (panicked.await, fine.await)
    });
    assert_eq!(panicked.unwrap_err().message, "injected");
    assert_eq!(fine, Ok(7));
}

#[test]
fn random_streams_are_seeded_by_label_and_domain() {
    use rand::RngCore as _;
    let draw = |seed: u64, domain: &str, label: &'static str| {
        let sim = Simulation::new(seed);
        sim.domain(domain).enter(|| {
            let mut first = asyncrt::rng(label);
            let mut second = asyncrt::rng(label);
            (first.next_u64(), second.next_u64())
        })
    };
    let base = draw(3, "n0", "lease");
    assert_eq!(base, draw(3, "n0", "lease"));
    assert_ne!(base.0, base.1, "a consumer's second stream is a new one");
    assert_ne!(base, draw(3, "n0", "jitter"));
    assert_ne!(base, draw(3, "n1", "lease"));
    assert_ne!(base, draw(4, "n0", "lease"));
}

#[test]
fn each_domain_has_its_own_services_and_counters() {
    let sim = Simulation::new(1);
    let first = sim.domain("n0");
    let second = sim.domain("n1");
    let services = |domain: &asyncrt::sim::Domain| domain.enter(asyncrt::services);
    assert!(Arc::ptr_eq(&services(&first), &first.services()));
    assert!(!Arc::ptr_eq(&services(&first), &services(&second)));
    let requests = |domain: &asyncrt::sim::Domain| {
        domain.enter(|| (asyncrt::next_core_request(), asyncrt::next_core_request()))
    };
    assert_eq!(requests(&first), (1, 2));
    assert_eq!(requests(&second), (1, 2));
    assert_ne!(
        first.enter(asyncrt::process_tag),
        second.enter(asyncrt::process_tag)
    );
}

#[test]
#[should_panic(expected = "outside a simulated domain")]
fn the_facade_refuses_calls_outside_a_domain() {
    asyncrt::mono_ms();
}
