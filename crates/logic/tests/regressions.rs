//! Scripted reproductions of core bugs the deterministic simulator
//! (`crates/sim`) found. Each drives the core through a minimal shell that
//! answers every effect at once, except the ones a test holds back to
//! recreate the interleaving.
//!
//! `State::validate` runs after every event in debug builds, which is what
//! these tests rely on; they also call it directly so a release build still
//! checks the final state.

use celld_logic::wake::{AlarmSnapshot, AlarmSource, PublicationId};
use celld_logic::{
    on_event, pressure, CasOutcome, Channel, Config, Effect, Event, Failure, LeaseCasOutcome,
    LocalCell, NodeLeaseSpec, OwnerRecord, OwnershipOnEvict, ProofSource, RestoreOutcome, State,
    Timer,
};
use std::collections::{BTreeMap, VecDeque};

const NOW_MS: u64 = 1_800_000_000_000;
const MONO_MS: u64 = 1_000;
const TTL_MS: u64 = 10_000;

fn config(max_resident: usize) -> Config {
    Config {
        max_resident,
        max_activations: 4,
        max_evictions: 2,
        max_releases: 2,
        max_outbound_websockets: 32,
        ownership_on_evict: OwnershipOnEvict::Release,
        require_node_lease: true,
        peer_protocol: 1,
        operation_deadline_ms: Some(15_000),
        owner_log_recovery_backoff_ms: 1_000,
        owner_log_recovery_attempts: 240,
        alarm_resident_ms: 3_600_000,
        idle_evict_ms: None,
        pressure: pressure::PressureConfig::from_limits(None, Some(0)),
    }
}

/// A shell that completes every effect immediately against an in-memory
/// ownership table, except those `hold` selects, which it keeps unanswered.
struct Shell {
    state: State,
    owners: BTreeMap<String, OwnerRecord>,
    next_etag: u64,
    local_cells: Vec<LocalCell>,
    hold: fn(&Effect) -> bool,
    held: Vec<Effect>,
    timers: Vec<Timer>,
}

impl Shell {
    fn new(max_resident: usize, hold: fn(&Effect) -> bool) -> Self {
        Self {
            state: State::new("n0", config(max_resident)),
            owners: BTreeMap::new(),
            next_etag: 1,
            local_cells: Vec::new(),
            hold,
            held: Vec::new(),
            timers: Vec::new(),
        }
    }

    fn start(&mut self) {
        self.send(Event::StartNodeLease {
            now_ms: NOW_MS,
            now_mono_ms: MONO_MS,
            spec: NodeLeaseSpec {
                addr: "n0".into(),
                peer_protocol: 1,
                generation: "g1".into(),
                resume_generation: None,
                ttl_ms: TTL_MS,
            },
        });
        assert!(self.state.node_authoritative());
    }

    fn send(&mut self, event: Event) -> Vec<Effect> {
        let mut seen = Vec::new();
        let mut queue = VecDeque::from([event]);
        while let Some(event) = queue.pop_front() {
            for effect in on_event(&mut self.state, event) {
                self.state.validate().expect("state invariant");
                if (self.hold)(&effect) {
                    self.held.push(effect.clone());
                } else if let Some(reply) = self.answer(&effect) {
                    queue.push_back(reply);
                }
                seen.push(effect);
            }
            self.state.validate().expect("state invariant");
        }
        seen
    }

    fn answer(&mut self, effect: &Effect) -> Option<Event> {
        Some(match effect.clone() {
            Effect::ScheduleTimer { timer, .. } => {
                self.timers.push(timer);
                return None;
            }
            Effect::ReadSelfNodeLease { op } => Event::SelfNodeLeaseRead {
                op,
                now_ms: NOW_MS,
                now_mono_ms: MONO_MS,
                result: Ok(None),
            },
            Effect::CasNodeLease { op, .. } => Event::NodeLeaseCasCompleted {
                op,
                now_mono_ms: MONO_MS,
                result: Ok(LeaseCasOutcome::Applied {
                    etag: "lease".into(),
                }),
                stamped_log_state: None,
            },
            // A fleet of one: no peer can take a cell this node cannot hold.
            Effect::ReadCapacityPeers { op, .. } => Event::CapacityPeersRead {
                op,
                now_ms: NOW_MS,
                result: Ok(Vec::new()),
            },
            Effect::ReadLocalCells => Event::LocalCellsRead {
                result: Ok(self.local_cells.clone()),
            },
            Effect::ReadOwner { op, cell } => Event::OwnerRead {
                op,
                now_ms: NOW_MS,
                result: Ok(self.owners.get(&cell).cloned()),
            },
            Effect::CasOwner {
                op, cell, epoch, ..
            } => {
                let etag = self.etag();
                self.owners.insert(
                    cell,
                    OwnerRecord {
                        node: Some("n0".into()),
                        epoch,
                        etag,
                    },
                );
                Event::OwnerCasCompleted {
                    op,
                    result: Ok(CasOutcome::Applied),
                }
            }
            Effect::ReleaseOwner { op, cell, epoch } => {
                let current = self.owners.get(&cell).cloned();
                let applies = current.is_some_and(|record| {
                    record.node.as_deref() == Some("n0") && record.epoch == epoch
                });
                if applies {
                    let etag = self.etag();
                    self.owners.insert(
                        cell,
                        OwnerRecord {
                            node: None,
                            epoch,
                            etag,
                        },
                    );
                }
                Event::OwnerReleased {
                    op,
                    result: Ok(if applies {
                        CasOutcome::Applied
                    } else {
                        CasOutcome::Rejected
                    }),
                }
            }
            Effect::Restore { op, .. } => Event::RestoreCompleted {
                op,
                result: Ok(RestoreOutcome {
                    restored: false,
                    alarm: None,
                }),
            },
            Effect::StartRuntime { op, .. } => Event::RuntimeStarted {
                op,
                isolate: None,
                generation: self.state.current_generation(),
                result: Ok(()),
            },
            Effect::Publish { op, .. } => Event::Published { op, result: Ok(()) },
            Effect::EnsureDurable { op, .. } => Event::DurabilityChecked { op, result: Ok(()) },
            Effect::StopRuntime { op, .. } => Event::RuntimeStopped { op },
            Effect::Complete { .. }
            | Effect::Release { .. }
            | Effect::Halt { .. }
            | Effect::ReconcileWakeEntry { .. }
            | Effect::EvictionAdmission { .. }
            | Effect::EvictionFinished { .. } => return None,
            other => panic!("unexpected effect {other:?}"),
        })
    }

    fn etag(&mut self) -> String {
        self.next_etag += 1;
        self.next_etag.to_string()
    }

    /// Report `cell`'s alarm at `at_ms` as its runtime does, covered by a
    /// wake entry.
    fn observe_alarm(&mut self, cell: &str, at_ms: i64) {
        let epoch = self.owners[cell].epoch;
        let alarm = AlarmSnapshot::committed(
            Some(at_ms),
            AlarmSource {
                id: PublicationId { epoch, sequence: 1 },
                position: 1,
            },
        );
        self.send(Event::AlarmObserved {
            cell: cell.into(),
            alarm,
            covered: true,
            now_ms: NOW_MS,
            now_mono_ms: MONO_MS,
        });
    }

    /// Route one request locally and finish its activity.
    fn serve(&mut self, request: u64, cell: &str) {
        let effects = self.send(Event::RequestAt {
            request,
            cell: cell.into(),
            now_mono_ms: MONO_MS,
        });
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                Effect::Complete { request: r, result: Ok(celld_logic::Route::Local) } if *r == request
            )),
            "request {request} was not served locally: {effects:?}"
        );
        self.send(Event::ActivityFinished { request });
    }
}

/// A full node sheds a resident to admit another cell, and with
/// `ownership_on_evict = Release` the shed publishes the record as unowned.
/// A request reactivates the shed cell while that release is still in
/// flight, and a second shed ends the new residency first. The second
/// release used to overwrite the first in the cell without retiring it from
/// the operation index: `validate` failed, and release builds leaked the
/// entry.
#[test]
fn a_second_release_supersedes_one_still_in_flight() {
    let releases_of = |held: &[Effect], cell: &str| {
        held.iter()
            .filter(|effect| matches!(effect, Effect::ReleaseOwner { cell: c, .. } if c == cell))
            .count()
    };
    let mut shell = Shell::new(1, |effect| matches!(effect, Effect::ReleaseOwner { .. }));
    shell.start();
    shell.serve(1, "a");
    shell.serve(2, "b");
    assert_eq!(releases_of(&shell.held, "a"), 1, "admitting b sheds a");
    shell.serve(3, "a");
    shell.serve(4, "b");
    assert_eq!(
        releases_of(&shell.held, "a"),
        2,
        "a is shed again while its first release is in flight"
    );

    // The store applies them in order: a's first release finds its epoch
    // superseded, and every later one releases its record.
    let held = std::mem::take(&mut shell.held);
    shell.hold = |_| false;
    for effect in held {
        if let Some(reply) = shell.answer(&effect) {
            shell.send(reply);
        }
    }
    shell.state.validate().expect("state invariant");
    assert_eq!(shell.owners["a"].node, None);
    assert_eq!(shell.owners["a"].epoch, 2);
}

/// A restarted node reads the ownership record of every cell on its disk.
/// If the node lease fences while one of those reads is in flight, the fence
/// used to leave the cell marked as confirming in `Fenced`, and `validate`
/// failed on the very event that halts the node.
#[test]
fn a_fence_settles_startup_confirmation_reads() {
    let mut shell = Shell::new(4, |effect| matches!(effect, Effect::ReadOwner { .. }));
    shell.local_cells = vec![LocalCell {
        id: "c".into(),
        epoch: 3,
        live: false,
    }];
    shell.start();
    assert_eq!(shell.held.len(), 1, "the confirmation read is in flight");
    assert!(!shell.state.ownership_confirmed());

    let fence = shell
        .timers
        .iter()
        .find(|timer| matches!(timer, Timer::NodeLeaseFence { .. }))
        .cloned()
        .expect("the held lease arms its fence");
    let effects = shell.send(Event::TimerFired {
        timer: fence,
        now_ms: NOW_MS + TTL_MS,
        now_mono_ms: MONO_MS + TTL_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { .. })));
    shell.state.validate().expect("state invariant");
}

/// A request's durability proof fails while an alarm handler is running on
/// the same runtime. The reset discards the runtime, and it used to leave the
/// alarm `Firing` on a cell that was no longer resident: `validate` failed,
/// and the firing op stayed half retired until the stop completed.
#[test]
fn a_failed_proof_settles_an_alarm_still_running() {
    let mut shell = Shell::new(4, |effect| {
        matches!(
            effect,
            Effect::AwaitDurable { .. } | Effect::FireAlarm { .. }
        )
    });
    shell.start();
    let effects = shell.send(Event::RequestAt {
        request: 1,
        cell: "a".into(),
        now_mono_ms: MONO_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Complete { request: 1, .. })));
    shell.observe_alarm("a", NOW_MS as i64 - 1);
    shell.send(Event::OutputAt {
        request: 1,
        channel: Channel::Response,
        position: Some(1),
        observed: None,
        epoch: None,
        now_mono_ms: MONO_MS,
    });
    let alarm = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::CellAlarm { .. }))
        .cloned()
        .expect("the observed alarm arms its timer");
    shell.send(Event::TimerFired {
        timer: alarm,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
    });
    let held = std::mem::take(&mut shell.held);
    let fire = held
        .iter()
        .find_map(|effect| match effect {
            Effect::FireAlarm {
                op, cell, epoch, ..
            } => Some((*op, cell.clone(), *epoch)),
            _ => None,
        })
        .expect("the due alarm fires");
    let proof = held
        .iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the write waits for its proof");

    shell.hold = |_| false;
    shell.send(Event::DurableReached {
        op: proof,
        result: Err(Failure::Ambiguous),
        source: ProofSource::Bucket,
    });
    // The handler's answer arrives after its runtime is gone.
    let (op, cell, epoch) = fire;
    shell.send(Event::AlarmFinished {
        op,
        cell,
        epoch,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
        result: Ok((AlarmSnapshot::without_wake(None), true, Some(2))),
    });
    shell.state.validate().expect("state invariant");
}
