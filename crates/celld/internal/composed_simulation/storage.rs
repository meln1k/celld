// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Nodes that own their cells through the bucket and keep them on a disk,
//! and the power-loss test that reads them back.

use super::{drive, History};
use crate::actor::{Actor, ActorIdentity, AdmissionLimits, AppHandle, Ownership};
use crate::asyncrt::{self, sim::Domain, sim::DomainOptions, sim::Simulation};
use crate::conformance_sim_cell_host::SimCellHost;
use crate::conformance_sim_store::{SimDisk, SimStore, StoreFaults};
use crate::engine_api::{RequestBody, RuntimeFetch};
use anyhow::Context as _;
use celld_logic::{Effect, Route};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// A node that owns cells through the shared bucket and keeps them on its
/// own disk, and the shell's handle to it.
#[derive(Clone)]
pub(super) struct StorageNode {
    pub(super) domain: Domain,
    pub(super) app: AppHandle,
    /// Why the process exited, once it has. A node that halts, or whose
    /// actor stops, exits as `celld`'s main loop does: everything on it
    /// stops at once, and a supervisor may start it again.
    pub(super) exited: Arc<Mutex<Option<String>>>,
    pub(super) runtimes: Runtimes,
}

/// Each cell and epoch a process started a runtime for.
pub(super) type Runtimes = Arc<Mutex<Vec<(String, u64)>>>;

/// Start node `name` over `store` and `disk`: a scripted cell host on the
/// real replicator, bucket ownership, and the actor driven as in
/// `start_node`.
pub(super) fn start_storage_node(
    sim: &Simulation,
    name: &str,
    store: &SimStore,
    disk: &SimDisk,
    history: &History,
) -> StorageNode {
    let domain = sim.domain_with(
        name,
        DomainOptions {
            filesystem: Some(disk.filesystem()),
            ..Default::default()
        },
    );
    let (messages, inbox) = mpsc::unbounded_channel();
    let (fence, mut fenced) = mpsc::unbounded_channel();
    let node = name.to_string();
    let history = history.clone();
    let (store, disk) = (store.clone(), disk.clone());
    let runtimes = Runtimes::default();
    let started = runtimes.clone();
    let (actor, host) = sim.block_on(&domain, async move {
        let host = SimCellHost::start(std::path::Path::new("/data"), &store, &disk)
            .expect("the cell host starts");
        let bucket = crate::conformance_sim_store::bucket(&store, "");
        // The probe key doubles as the lease generation, so each process
        // draws its own, as a restarted node does.
        let ownership = crate::ownership_store::BucketOwnership::new(
            bucket.clone(),
            bucket,
            node.clone(),
            crate::machine::random_process_generation(),
        )
        .with_lease_ttl_ms(10_000);
        let mut actor = Actor::from_environment_with_scripted_host(
            AdmissionLimits {
                resident: 4,
                activations: 2,
                evictions: 2,
                releases: 2,
                placement_weight: 1,
            },
            false,
            fence,
            host.clone(),
            Some(Ownership::Bucket(Arc::new(ownership))),
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
            for effect in effects {
                if let Effect::StartRuntime { cell, epoch, .. } = effect {
                    started.lock().unwrap().push((cell.clone(), *epoch));
                }
            }
            history
                .lock()
                .unwrap()
                .push(format!("{node}: {event:?} => {effects:?}"));
        }));
        (actor, host)
    });
    let app = AppHandle {
        tx: messages,
        runtime: None,
        scripted_activity_host: Some(host),
        reload: crate::generation::reload_channel().0,
        peer_http: reqwest::Client::new(),
        peer_auth: Arc::new(crate::peer_auth::PeerAuth::new([0; 32], name).unwrap()),
        advertise: format!("{name}:4000"),
        websockets: mpsc::unbounded_channel().0,
        max_outbound_websockets: 0,
        draining: Arc::default(),
        fleet_ready: Arc::default(),
        public_in_flight: Arc::default(),
        drain_pins: actor.drain_pins_for_test(),
        follower: None,
        trust_forwarded_headers: false,
        max_request_body_bytes: 1 << 20,
        operation_deadline_ms: 30_000,
    };
    let exited = Arc::new(Mutex::new(None));
    let (process, exit) = (domain.clone(), exited.clone());
    domain
        .spawn(async move {
            let reason = asyncrt::select! {
                code = fenced.recv() => format!("halted with code {code:?}"),
                () = drive(actor, inbox) => "its actor stopped".to_string(),
            };
            // Nothing else on the node runs after this; the fleet reaps it.
            process.set_paused(true);
            *exit.lock().unwrap() = Some(reason);
        })
        .detach();
    StorageNode {
        domain,
        app,
        exited,
        runtimes,
    }
}

/// How a node answered one request.
#[derive(Debug)]
pub(super) enum Served {
    /// The cell ran here, and its answer passed the output gate.
    Answer(String),
    /// Another node owns the cell.
    Elsewhere(String),
    /// Routing, the handler or the output gate refused.
    Failed(String),
}

/// One request to `cell` through the shell's local path: route, run the
/// handler, and hold the answer behind the output gate.
pub(super) async fn serve(app: &AppHandle, cell: &str, method: &str, body: &str) -> Served {
    let routed = match app.request(cell.to_string()).await {
        Ok(routed) => routed,
        Err(error) => return Served::Failed(format!("route: {error:?}")),
    };
    if let Route::Remote { node, .. } = routed.route {
        return Served::Elsewhere(node);
    }
    let completed = app
        .local_request(routed.request, cell.to_string(), None, "sim")
        .run(async {
            let host = app.execution_host().context("no cell host")?;
            host.fetch_cell(
                cell.to_string(),
                None,
                RuntimeFetch {
                    url: format!("http://sim/{cell}"),
                    method: method.to_string(),
                    body: RequestBody::Bytes(body.to_string().into()),
                    headers: Vec::new(),
                    request_id: None,
                    order: None,
                    parent: None,
                },
                None,
            )
            .await
        })
        .await;
    match completed.result {
        Ok(response) => Served::Answer(String::from_utf8_lossy(&response.body).into_owned()),
        Err(failure) => Served::Failed(format!(
            "{:#}",
            crate::cell_dispatch::local_request_error(failure)
        )),
    }
}

/// [`serve`], where only an answer is a success.
pub(super) async fn call(
    app: &AppHandle,
    cell: &str,
    method: &str,
    body: &str,
) -> anyhow::Result<String> {
    match serve(app, cell, method, body).await {
        Served::Answer(answer) => Ok(answer),
        other => anyhow::bail!("{other:?}"),
    }
}

/// Read `cell`'s log through `node`, retrying while ownership settles.
pub(super) fn read_log(sim: &Simulation, node: &StorageNode, cell: &str) -> Vec<String> {
    let app = node.app.clone();
    let cell = cell.to_string();
    sim.block_on(&node.domain, async move {
        for _ in 0..200 {
            match call(&app, &cell, "GET", "").await {
                Ok(log) => {
                    return log
                        .split(',')
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .collect();
                }
                Err(_) => asyncrt::sleep(std::time::Duration::from_millis(500)).await,
            }
        }
        panic!("{cell} never served a read");
    })
}

/// What a power-loss run did.
pub(super) struct PowerLossRun {
    /// Per cell, the writes n0 acknowledged and the log the successor read.
    pub(super) cells: Vec<(Vec<String>, Vec<String>)>,
    /// Every core event of both nodes, then every change to the bucket.
    pub(super) history: Vec<String>,
    /// Every object left in the bucket, with a hash of its bytes. The bytes
    /// carry SQLite's WAL salts, which come from a process-wide generator,
    /// so they repeat only in a process of their own.
    pub(super) objects: Vec<String>,
}

/// One power-loss run: three clients append to their own cells on n0, n0
/// loses power at a seeded instant mid-stream, and after its lease runs
/// out `successor` serves the cells, from n0's disk when it is n0 itself
/// and from the bucket alone otherwise.
pub(super) fn power_loss_run(seed: u64, successor: &str) -> PowerLossRun {
    const CELLS: [&str; 3] = ["do:c0", "do:c1", "do:c2"];
    let sim = Simulation::new(seed);
    let store = SimStore::new(StoreFaults {
        latency_ms: 5..40,
        unavailable_per_mille: 10,
        lost_response_per_mille: 10,
        ..Default::default()
    });
    let history = History::default();
    let disk = SimDisk::new();
    let n0 = start_storage_node(&sim, "n0", &store, &disk, &history);
    let acked: Vec<Arc<Mutex<Vec<String>>>> = CELLS.iter().map(|_| Arc::default()).collect();
    for (cell, writes) in CELLS.into_iter().zip(&acked) {
        let (app, writes) = (n0.app.clone(), writes.clone());
        n0.domain
            .spawn(async move {
                let mut write = 0;
                loop {
                    write += 1;
                    let value = format!("{cell}/w{write}");
                    match call(&app, cell, "POST", &value).await {
                        Ok(_) => writes.lock().unwrap().push(value),
                        // A node refuses at once until it holds its lease,
                        // and a client waits before it tries again.
                        Err(_) => asyncrt::sleep(std::time::Duration::from_millis(50)).await,
                    }
                }
            })
            .detach();
    }
    sim.run_for(2_000 + seed.wrapping_mul(7_919) % 3_000);
    disk.power_loss();
    n0.domain.crash();
    drop(n0);

    // Past the dead node's lease, so a successor may take the cells.
    sim.run_for(15_000);
    let disk = if successor == "n0" {
        disk
    } else {
        SimDisk::new()
    };
    let next = start_storage_node(&sim, successor, &store, &disk, &history);
    let cells = CELLS
        .iter()
        .zip(&acked)
        .map(|(cell, acked)| (acked.lock().unwrap().clone(), read_log(&sim, &next, cell)))
        .collect();
    let mut history = history.lock().unwrap().clone();
    history.extend(
        store
            .mutations()
            .iter()
            .map(|mutation| format!("{mutation:?}")),
    );
    let objects = store
        .keys("")
        .into_iter()
        .map(|key| {
            use std::hash::{Hash as _, Hasher as _};
            let mut hasher = std::hash::DefaultHasher::new();
            store.object(&key).unwrap().hash(&mut hasher);
            format!("{key} {:016x}", hasher.finish())
        })
        .collect();
    PowerLossRun {
        cells,
        history,
        objects,
    }
}

/// Writes acknowledged before a power loss come back, whether the node
/// restarts on what its disk kept or another node restores from the
/// bucket. The node dies mid-stream, so some writes were in flight.
#[test]
fn an_acknowledged_write_survives_a_power_loss() {
    let mut acknowledged = 0;
    for seed in 0..6 {
        for successor in ["n0", "n1"] {
            for (acked, log) in power_loss_run(seed, successor).cells {
                acknowledged += acked.len();
                let lost: Vec<&String> =
                    acked.iter().filter(|value| !log.contains(value)).collect();
                assert!(
                    lost.is_empty(),
                    "seed {seed}: {successor} lost acknowledged writes {lost:?}; read {log:?}"
                );
            }
        }
    }
    assert!(
        acknowledged >= 1_000,
        "only {acknowledged} writes were acknowledged"
    );
}
