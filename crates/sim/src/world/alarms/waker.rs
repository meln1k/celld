//! The waker (`crates/celld/wake.rs`): the boot scan, the due scan with the
//! advisory waker role, and the cleanup collector, each hinting the core.

use super::*;
use celld_logic::wake::{
    classify_listed, due_entries, parse_entry_key, probe_hint, Listed, ListedEntry, ProbedOwner,
};
use celld_logic::WakeHintScope;

#[derive(Debug)]
pub(super) struct Scan {
    kind: ScanKind,
    stage: ScanStage,
    started: Time,
    elected: bool,
    live: BTreeSet<String>,
    entries: Vec<(String, i64)>,
    /// The collector's page: every entry key and what it names.
    listed: Vec<(String, ListedEntry)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::world) enum ScanKind {
    /// The startup orphan scan, hinting for the fleet.
    Boot,
    /// `run_due_scans`.
    Due,
    /// `run_cleanup`: the collector pass and its refresh.
    Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanStage {
    Claim,
    Live,
    List,
    Retirements,
    Owners,
}

/// How long the boot scan waits for node authority before the process
/// gives up, as `main.rs` does for an ordinary start.
const BOOT_AUTHORITY_WAIT_MS: u64 = 10_000;

impl World {
    /// After the node holds authority, the boot scan, then the periodic
    /// loops. A process that never gains authority exits.
    pub(in crate::world) fn boot_scan(&mut self, id: ProcId, waited_ms: u64) {
        if !self.procs[&id].state.node_authoritative() {
            if waited_ms >= BOOT_AUTHORITY_WAIT_MS {
                self.note(|| format!("p{id} exits: no node authority before the wake scan"));
                self.crash(id, false);
            } else {
                self.work(
                    id,
                    10,
                    Work::BootScan {
                        waited_ms: waited_ms + 10,
                    },
                );
            }
            return;
        }
        self.start_scan(id, ScanKind::Boot);
    }

    pub(in crate::world) fn start_scan(&mut self, id: ProcId, kind: ScanKind) {
        let scan = Scan {
            kind,
            stage: match kind {
                ScanKind::Boot => ScanStage::Live,
                ScanKind::Due => ScanStage::Claim,
                ScanKind::Cleanup => ScanStage::List,
            },
            started: self.t,
            elected: kind == ScanKind::Boot,
            live: BTreeSet::new(),
            entries: Vec::new(),
            listed: Vec::new(),
        };
        let key = self.task_key();
        self.scan_issue(id, key, scan);
    }

    fn scan_issue(&mut self, id: ProcId, key: u64, scan: Scan) {
        let now_ms = self.wall(id);
        let req = match scan.stage {
            ScanStage::Claim => StoreReq::WakerClaim {
                now_ms: now_ms as i64,
                ttl_ms: 3 * self.config.wake_tick_ms as i64,
            },
            ScanStage::Live => StoreReq::LiveNodes { now_ms },
            ScanStage::List => StoreReq::WakeList,
            ScanStage::Retirements => StoreReq::ReadRetirements {
                cells: scan
                    .listed
                    .iter()
                    .map(|(_, entry)| entry.cell.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            },
            ScanStage::Owners => StoreReq::ReadOwners {
                cells: scan.entries.iter().map(|(cell, _)| cell.clone()).collect(),
            },
        };
        self.park(id, key, Task::Scan(scan));
        self.store_call(id, req, Reply::Task { task: key });
    }

    pub(super) fn scan_step(
        &mut self,
        id: ProcId,
        key: u64,
        mut scan: Scan,
        result: Result<StoreRes, Failure>,
    ) {
        let now_ms = self.wall(id) as i64;
        match scan.stage {
            ScanStage::Claim => {
                scan.elected = matches!(result, Ok(StoreRes::Bool(true)));
                scan.stage = if scan.elected {
                    ScanStage::Live
                } else {
                    match scan.kind {
                        ScanKind::Cleanup => ScanStage::Owners,
                        _ => ScanStage::List,
                    }
                };
            }
            ScanStage::Live => {
                scan.live = match result {
                    Ok(StoreRes::Live(live)) => live,
                    _ => BTreeSet::new(),
                };
                scan.stage = match scan.kind {
                    ScanKind::Cleanup => ScanStage::Owners,
                    _ => ScanStage::List,
                };
            }
            ScanStage::List => {
                let keys = match result {
                    Ok(StoreRes::Keys(keys)) => keys,
                    _ if scan.kind == ScanKind::Cleanup => {
                        // The page failed: skip the tick.
                        self.scan_done(id, scan);
                        return;
                    }
                    _ => Vec::new(),
                };
                let listed: Vec<(String, ListedEntry)> = keys
                    .into_iter()
                    .filter_map(|key| parse_entry_key(&key).map(|entry| (key, entry)))
                    .collect();
                if scan.kind == ScanKind::Cleanup {
                    if listed.is_empty() {
                        self.scan_done(id, scan);
                        return;
                    }
                    scan.listed = listed;
                    scan.stage = ScanStage::Retirements;
                } else {
                    let due = due_entries(listed.into_iter().map(|(_, entry)| entry), now_ms);
                    if !scan.elected {
                        self.hint(id, due, WakeHintScope::Owned);
                        self.scan_done(id, scan);
                        return;
                    }
                    if due.is_empty() {
                        self.scan_done(id, scan);
                        return;
                    }
                    scan.entries = due;
                    scan.stage = ScanStage::Owners;
                }
            }
            ScanStage::Retirements => {
                let Ok(StoreRes::Retirements(records)) = result else {
                    self.scan_done(id, scan);
                    return;
                };
                // Delete what each record retires; refresh each cell with a
                // due entry it leaves standing, from its earliest minute.
                let mut refresh: BTreeMap<String, i64> = BTreeMap::new();
                for (key, entry) in std::mem::take(&mut scan.listed) {
                    let record = records.get(&entry.cell).copied();
                    match classify_listed(record, &entry, now_ms) {
                        Listed::Retired => {
                            self.store_call(id, StoreReq::WakeDelete { key }, Reply::Ignore)
                        }
                        Listed::Due(minute) => {
                            let slot = refresh.entry(entry.cell).or_insert(minute);
                            *slot = (*slot).min(minute);
                        }
                        Listed::Waiting => {}
                    }
                }
                if refresh.is_empty() {
                    self.scan_done(id, scan);
                    return;
                }
                scan.entries = refresh.into_iter().collect();
                scan.stage = ScanStage::Claim;
            }
            ScanStage::Owners => {
                let owners = match result {
                    Ok(StoreRes::Owners(owners)) => Some(owners),
                    _ => None,
                };
                let node = self.node_name(id);
                let elected = scan.elected && self.config.mutation != Mutation::NoOrphanWake;
                let entries = std::mem::take(&mut scan.entries);
                let hinted: Vec<(String, i64)> = entries
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, (cell, entry_ms))| {
                        let owner = match &owners {
                            None => ProbedOwner::Unread,
                            Some(owners) => ProbedOwner::Read(
                                owners[index]
                                    .as_ref()
                                    .and_then(|record| record.node.as_deref()),
                            ),
                        };
                        probe_hint(owner, &node, &scan.live, elected).then_some((cell, entry_ms))
                    })
                    .collect();
                if scan.kind == ScanKind::Cleanup {
                    for (cell, _) in &hinted {
                        let alarm = self.runtime_alarm(id, cell);
                        self.maintain_wake(id, cell.clone(), alarm);
                        if !self.procs.contains_key(&id) {
                            return;
                        }
                    }
                }
                self.hint(id, hinted, WakeHintScope::Fleet);
                self.scan_done(id, scan);
                return;
            }
        }
        self.scan_issue(id, key, scan);
    }

    fn hint(&mut self, id: ProcId, entries: Vec<(String, i64)>, scope: WakeHintScope) {
        for (cell, entry_ms) in entries {
            if !self.procs.contains_key(&id) {
                return;
            }
            self.stats.wake_hints += 1;
            self.drive(
                id,
                Event::WakeHintAt {
                    cell,
                    entry_ms,
                    scope,
                    now_mono_ms: 0,
                },
            );
        }
    }

    fn scan_done(&mut self, id: ProcId, scan: Scan) {
        if !self.procs.contains_key(&id) {
            return;
        }
        let tick = self.config.wake_tick_ms;
        let next = |started: Time, now: Time| (started + tick).max(now + 1) - now;
        match scan.kind {
            ScanKind::Boot => {
                self.work(id, tick, Work::WakeTick { cleanup: false });
                self.work(id, tick, Work::WakeTick { cleanup: true });
            }
            ScanKind::Due => {
                let delay = next(scan.started, self.t);
                self.work(id, delay, Work::WakeTick { cleanup: false });
            }
            ScanKind::Cleanup => {
                let delay = next(scan.started, self.t);
                self.work(id, delay, Work::WakeTick { cleanup: true });
            }
        }
    }
}
