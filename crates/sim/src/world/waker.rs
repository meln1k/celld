//! The wake loops (`wake::run_periodic`): every second each process runs a
//! due scan and a cleanup pass over the bucket's wake entries, and the
//! advisory waker role decides which one of them speaks for the fleet.
//!
//! - The role is `wake/waker.json`, a lease whose holder renews it each tick
//!   (`try_hold_waker`). Correctness never depends on it: two holders only
//!   hint the same cells twice, and none only delays a hint.
//! - The due scan lists the due entries. A process that holds the role reads
//!   the node leases and each due cell's owner record, and hints the cells
//!   a live owner will not wake itself (`elected_due`), keeping an entry
//!   whose read failed. Any other process hints every due entry, which the
//!   core takes only for cells it owns.
//! - The cleanup pass (`Collector::pass`) lists every entry, reads each
//!   cell's retirement certificate, deletes the entries it retires, and
//!   refreshes the due ones it does not: the owner, if resident here,
//!   maintains the entry again, and the cell is hinted as by the due scan.
//!
//! Each step is a bucket call with its own fate: it can fail or lose its
//! answer, and once sent it applies even if its process dies. A failed
//! listing skips the pass, a failed delete leaves its entry for the next
//! one. The one listing reads every entry: the fleet here holds far fewer
//! than a page.

use std::collections::{BTreeMap, BTreeSet};

use celld_logic::wake::{elected_hint_needed, PublicationId};
use celld_logic::{Event, WakeHintScope};

use super::shell::StoreReq;
use super::{Work, World};
use crate::Mutation;

/// The wake loops' period (`run_due_scans`, `run_cleanup`).
const WAKE_TICK_MS: u64 = 1_000;

/// Which of the two loops a step belongs to.
#[derive(Clone, Debug)]
pub(super) enum Pass {
    Scan,
    /// The due cells the collector found unretired, waiting for the role.
    Collect {
        refresh: Vec<(String, i64)>,
    },
}

/// One bucket call of a wake loop.
#[derive(Debug)]
pub(super) enum WakerReq {
    /// `try_hold_waker`.
    Hold { pass: Pass },
    /// The node leases, read by the role's holder: who is live.
    Leases { pass: Pass },
    /// `due_scan`.
    ListDue { live: Option<BTreeSet<String>> },
    /// One due cell's owner record (`probe_entries`).
    Probe {
        cell: String,
        entry_ms: i64,
        live: Option<BTreeSet<String>>,
        refresh: bool,
    },
    /// The collector's listing of every entry.
    ListEntries,
    /// One cell's retirement certificate, with the cell's listed entries.
    ReadRetirement {
        cell: String,
        entries: Vec<(PublicationId, i64)>,
    },
    /// Delete one entry: the collector's, or `maintain`'s eager delete of
    /// every entry the cell's certificate retires.
    Delete {
        cell: String,
        id: Option<PublicationId>,
    },
}

pub(super) enum WakerWork {
    Tick,
    Held {
        pass: Pass,
        elected: bool,
    },
    Live {
        pass: Pass,
        live: BTreeSet<String>,
    },
    Due {
        live: Option<BTreeSet<String>>,
        due: Vec<(String, i64)>,
    },
    Probed {
        cell: String,
        entry_ms: i64,
        hint: bool,
        refresh: bool,
    },
    Listed {
        cells: BTreeMap<String, Vec<(PublicationId, i64)>>,
    },
    /// A cell's certificate was read, or its read failed: delete these
    /// entries, and refresh the cell if it has a due one left.
    Retired {
        cell: String,
        delete: Vec<PublicationId>,
        refresh: Option<i64>,
    },
}

/// What a process's wake loops have in flight. A loop starts no new pass
/// while one runs, as each loop awaits its pass before its next tick.
#[derive(Default)]
pub(super) struct ProcWaker {
    scanning: bool,
    /// The cleanup pass: certificate reads still out, and the cells found
    /// to refresh so far.
    collecting: Option<(usize, Vec<(String, i64)>)>,
}

impl World {
    /// Start the process's wake loops.
    pub(super) fn start_wake_loops(&mut self, node: usize) {
        let delay = self.net_rng.range(1, WAKE_TICK_MS);
        self.work(node, delay, Work::Waker(WakerWork::Tick));
    }

    pub(super) fn waker_work(&mut self, node: usize, work: WakerWork) {
        match work {
            WakerWork::Tick => {
                self.work(node, WAKE_TICK_MS, Work::Waker(WakerWork::Tick));
                let p = self.machines[node].proc_mut();
                if !p.state.node_authoritative() {
                    return;
                }
                if !p.waker.scanning {
                    p.waker.scanning = true;
                    self.waker_call(node, WakerReq::Hold { pass: Pass::Scan });
                }
                let p = self.machines[node].proc_mut();
                if p.waker.collecting.is_none() {
                    p.waker.collecting = Some((0, Vec::new()));
                    self.waker_call(node, WakerReq::ListEntries);
                }
            }
            WakerWork::Held { pass, elected } => {
                if elected {
                    self.waker_call(node, WakerReq::Leases { pass });
                } else {
                    self.waker_live(node, pass, None);
                }
            }
            WakerWork::Live { pass, live } => self.waker_live(node, pass, Some(live)),
            WakerWork::Due { live, due } => {
                self.machines[node].proc_mut().waker.scanning = false;
                for (cell, entry_ms) in due {
                    if live.is_some() {
                        let req = WakerReq::Probe {
                            cell,
                            entry_ms,
                            live: live.clone(),
                            refresh: false,
                        };
                        self.waker_call(node, req);
                    } else {
                        self.wake_hint(node, cell, entry_ms, WakeHintScope::Owned);
                    }
                }
            }
            WakerWork::Probed {
                cell,
                entry_ms,
                hint,
                refresh,
            } => {
                if !hint {
                    return;
                }
                if refresh {
                    // `MaintainWake`: a resident owner writes its entry again.
                    if let Some((alarm, _)) = self.observation(node, &cell) {
                        self.reconcile_wake(node, &cell, alarm);
                    }
                }
                self.wake_hint(node, cell, entry_ms, WakeHintScope::Fleet);
            }
            WakerWork::Listed { cells } => {
                let count = cells.len();
                for (cell, entries) in cells {
                    self.waker_call(node, WakerReq::ReadRetirement { cell, entries });
                }
                self.collected(node, Some(count), None);
            }
            WakerWork::Retired {
                cell,
                delete,
                refresh,
            } => {
                for id in delete {
                    let cell = cell.clone();
                    self.waker_call(node, WakerReq::Delete { cell, id: Some(id) });
                }
                self.collected(node, None, refresh.map(|minute| (cell, minute)));
            }
        }
    }

    /// The role is settled for this pass, and the holder knows who is live.
    fn waker_live(&mut self, node: usize, pass: Pass, live: Option<BTreeSet<String>>) {
        match pass {
            Pass::Scan => self.waker_call(node, WakerReq::ListDue { live }),
            Pass::Collect { refresh } => {
                for (cell, entry_ms) in refresh {
                    let req = WakerReq::Probe {
                        cell,
                        entry_ms,
                        live: live.clone(),
                        refresh: true,
                    };
                    self.waker_call(node, req);
                }
            }
        }
    }

    /// The cleanup pass listed `listed` cells, or one certificate read came
    /// back with the cell to refresh, if any. Once no read is out, the pass
    /// settles the role for its refreshes, and the next tick may start
    /// another.
    fn collected(&mut self, node: usize, listed: Option<usize>, refresh: Option<(String, i64)>) {
        let p = self.machines[node].proc_mut();
        let Some((waiting, cells)) = p.waker.collecting.as_mut() else {
            return;
        };
        match listed {
            Some(count) => *waiting = count,
            None => *waiting -= 1,
        }
        cells.extend(refresh);
        if *waiting > 0 {
            return;
        }
        let (_, refresh) = p.waker.collecting.take().expect("a pass");
        if !refresh.is_empty() {
            let pass = Pass::Collect { refresh };
            self.waker_call(node, WakerReq::Hold { pass });
        }
    }

    fn wake_hint(&mut self, node: usize, cell: String, entry_ms: i64, scope: WakeHintScope) {
        // A preserving actor drops every hint.
        if self.preserving(node) {
            return;
        }
        self.stats.wake_hints += 1;
        let event = Event::WakeHintAt {
            cell,
            entry_ms,
            scope,
            now_mono_ms: 0,
        };
        self.drive(node, event);
    }

    pub(super) fn waker_call(&mut self, node: usize, req: WakerReq) {
        self.store_call(node, StoreReq::Waker(req));
    }

    /// The call reaches the bucket. Returns the work its answer does.
    pub(super) fn waker_apply(&mut self, node: usize, req: WakerReq) -> Work {
        let now_ms = self.wall(node);
        let me = self.machines[node].name.clone();
        let work = match req {
            WakerReq::Hold { pass } => {
                let ttl_ms = 3 * WAKE_TICK_MS as i64;
                let elected = self.store.try_hold_waker(&me, now_ms as i64, ttl_ms);
                WakerWork::Held { pass, elected }
            }
            WakerReq::Leases { pass } => WakerWork::Live {
                pass,
                live: self.store.live_nodes(now_ms),
            },
            WakerReq::ListDue { live } => WakerWork::Due {
                live,
                due: self.store.wake_due(now_ms as i64),
            },
            WakerReq::Probe {
                cell,
                entry_ms,
                live,
                refresh,
            } => {
                let owner = self.store.owner(&cell).and_then(|(owner, _)| owner);
                let hint = owner == Some(me.as_str())
                    || live.is_some_and(|live| {
                        let owner_live = owner.is_some_and(|owner| live.contains(owner));
                        elected_hint_needed(owner, &me, owner_live)
                    });
                WakerWork::Probed {
                    cell,
                    entry_ms,
                    hint,
                    refresh,
                }
            }
            WakerReq::ListEntries => WakerWork::Listed {
                cells: self.store.wake_entries(),
            },
            WakerReq::ReadRetirement { cell, entries } => {
                let certificate = self.store.retirement(&cell);
                let retired = |id: PublicationId| {
                    self.config.mutation == Some(Mutation::CollectUnretired)
                        || certificate.is_some_and(|c| c.retires(id))
                };
                let delete = (entries.iter())
                    .filter(|(id, _)| retired(*id))
                    .map(|(id, _)| *id)
                    .collect();
                let refresh = (entries.iter())
                    .filter(|(id, minute_ms)| !retired(*id) && *minute_ms <= now_ms as i64)
                    .map(|(_, minute_ms)| *minute_ms)
                    .min();
                WakerWork::Retired {
                    cell,
                    delete,
                    refresh,
                }
            }
            WakerReq::Delete { cell, id } => {
                match id {
                    Some(id) => self.store.delete_wake(&cell, id),
                    None => self.store.delete_retired(&cell),
                }
                self.judge_discoverable(&cell);
                return Work::Nothing;
            }
        };
        Work::Waker(work)
    }

    /// A call failed: the answer its loop hears instead.
    pub(super) fn waker_failed(req: &WakerReq) -> Work {
        let work = match req {
            WakerReq::Hold { pass } => WakerWork::Held {
                pass: pass.clone(),
                elected: false,
            },
            // An unreadable lease counts as not live.
            WakerReq::Leases { pass } => WakerWork::Live {
                pass: pass.clone(),
                live: BTreeSet::new(),
            },
            WakerReq::ListDue { live } => WakerWork::Due {
                live: live.clone(),
                due: Vec::new(),
            },
            // Only the role's holder keeps an entry whose read failed.
            WakerReq::Probe {
                cell,
                entry_ms,
                live,
                refresh,
            } => WakerWork::Probed {
                cell: cell.clone(),
                entry_ms: *entry_ms,
                hint: live.is_some(),
                refresh: *refresh,
            },
            WakerReq::ListEntries => WakerWork::Listed {
                cells: BTreeMap::new(),
            },
            WakerReq::ReadRetirement { cell, .. } => WakerWork::Retired {
                cell: cell.clone(),
                delete: Vec::new(),
                refresh: None,
            },
            WakerReq::Delete { .. } => return Work::Nothing,
        };
        Work::Waker(work)
    }
}
