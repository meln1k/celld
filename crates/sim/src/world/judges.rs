//! The properties judged on the fleet's state at a moment: which cells a
//! process serves, what it holds open, what their alarms are. Handing all of
//! that to the checker as it changes would copy the world into it, so each
//! judge here picks out the facts its property needs, and the checker in
//! `checks.rs` decides and records the hit.
//!
//! A judge runs at the moment its property applies: when a process is
//! about to die, a runtime is evicted or stopped, a claim or a clean reload
//! applies, a load sample or a deploy comes due, or the run ends. What the
//! judges remember of a process between those moments is its [`Watch`].

use std::collections::BTreeMap;

use celld_logic::{Epoch, WebSocketKind};

use super::alarms::ALARM_SLACK_MS;
use super::{Pending, Time, World, LOAD_SAMPLE_MS, WALL_EPOCH_MS};

/// How long a quiet node may stay over its memory ceiling.
const MEMORY_RELIEF_MS: u64 = 10_000;
/// How long after a deploy's `max_age_ms` its forced moves may still take.
pub(super) const DEPLOY_SLACK_MS: u64 = 10_000;

/// What the judges remember of one process.
#[derive(Default)]
pub(super) struct Watch {
    /// The generation each cell's runtime last started on.
    pub(super) generations: BTreeMap<String, u64>,
    /// When anything last used each cell here.
    pub(super) touched: BTreeMap<String, Time>,
    /// Since when the core has been shedding without a break.
    pub(super) shedding_since: Option<Time>,
    /// Cells whose runtime this step stopped, for good: judged once the
    /// step is over, when every socket it closed has closed.
    pub(super) stopped: Vec<String>,
}

impl World {
    /// Record that something used `cell` on `node` just now.
    pub(super) fn touch(&mut self, node: usize, cell: &str) {
        let t = self.t;
        self.machines[node]
            .proc_mut()
            .watch
            .touched
            .insert(cell.to_string(), t);
    }

    /// The core is evicting `cell`: what it leaves behind must be safe to
    /// leave. No transport pins it, and its alarm has a wake entry.
    pub(super) fn judge_eviction(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let who = self.who(node);
        let pinned = self.pinned(node, cell);
        let uncovered = self.alarm_uncovered(node, cell, epoch);
        self.checker.evicting(self.t, &who, cell, pinned, uncovered);
    }

    /// The process on `node` is about to die: every alarm a client was told
    /// was set must be covered by a wake entry.
    pub(super) fn judge_death(&mut self, node: usize) {
        let who = self.who(node);
        let published: Vec<(String, Epoch)> = self.machines[node]
            .proc()
            .published
            .iter()
            .map(|(cell, epoch)| (cell.clone(), *epoch))
            .collect();
        for (cell, epoch) in published {
            if !self.alarm_uncovered(node, &cell, epoch) {
                continue;
            }
            let content = self.seen(node, &cell, epoch).expect("uncovered: open");
            let last = content
                .iter()
                .rev()
                .find(|w| self.alarm_writes.contains_key(w))
                .copied()
                .expect("uncovered: armed");
            if self.checker.answered(&cell, last) {
                self.checker.died_uncovered(self.t, &who, &cell, last);
            }
        }
    }

    /// The step is over: no cell whose runtime it stopped, and that it did
    /// not start again, may keep an outbound socket open here.
    pub(super) fn judge_stopped(&mut self, node: usize) {
        let Some(p) = self.machines[node].proc.as_mut() else {
            return;
        };
        let mut hits = Vec::new();
        for cell in std::mem::take(&mut p.watch.stopped) {
            if p.published.contains_key(&cell) {
                continue;
            }
            for (websocket, socket) in &p.sockets {
                if socket.cell == cell && socket.kind == WebSocketKind::Outbound {
                    hits.push((cell.clone(), *websocket));
                }
            }
        }
        let who = self.who(node);
        for (cell, websocket) in hits {
            self.checker.socket_outlived(self.t, &who, &cell, websocket);
        }
    }

    /// Watched: the bucket changed under `cell`. If the alarm a restore
    /// would load was set by a write whose handler answered its client, a
    /// wake entry must still be due by its time.
    pub(super) fn judge_discoverable(&mut self, cell: &str) {
        let Some((epoch, content)) = self.store.newest(cell) else {
            return;
        };
        let Some(at_ms) = self.alarm_of(content, epoch).at_ms() else {
            return;
        };
        let set_by = content
            .iter()
            .rev()
            .find(|w| self.alarm_writes.contains_key(w))
            .copied()
            .expect("armed");
        if self.checker.answered(cell, set_by) && !self.store.wake_covers(cell, at_ms) {
            self.checker
                .alarm_undiscoverable(self.t, cell, set_by, at_ms);
        }
    }

    /// Watched: a draining node claims nothing new. Reclaiming a cell the
    /// record named this node for is not new ownership.
    pub(super) fn judge_claim(
        &mut self,
        sender: &str,
        name: &str,
        cell: &str,
        epoch: Epoch,
        prior: Option<String>,
        phase: &str,
    ) {
        if prior.as_deref() != Some(name) {
            let detail = format!("{phase}: epoch {epoch} over {prior:?}");
            self.checker
                .claimed_during_shutdown(self.t, sender, cell, detail);
        }
    }

    /// Watched: a clean reload opens its predecessor's file only while the
    /// owner record still names this node at the file's epoch.
    pub(super) fn judge_resume(&mut self, node: usize, cell: &str, epoch: Epoch) {
        let name = self.machines[node].name.clone();
        let owner = self.store.owner(cell);
        if owner != Some((Some(name.as_str()), epoch)) {
            let who = self.who(node);
            let owner = format!("{owner:?}");
            self.checker.resumed(self.t, &who, cell, epoch, owner);
        }
    }

    /// Every runtime of a cell still served here must have moved to the
    /// deploy's generation, unless a later deploy has superseded it.
    pub(super) fn judge_deploy(&mut self, node: usize, generation: u64, max_age_ms: u64) {
        let p = self.machines[node].proc();
        if p.generation != generation || p.shutdown.is_some() {
            return;
        }
        let stuck: Vec<(String, u64)> = p
            .published
            .keys()
            .filter_map(|cell| {
                let on = p.watch.generations.get(cell).copied().unwrap_or(0);
                (on < generation).then(|| (cell.clone(), on))
            })
            .collect();
        let who = self.who(node);
        for (cell, on) in stuck {
            let detail = format!(
                "on generation {on}, {DEPLOY_SLACK_MS} ms after generation {generation}'s \
                 max age of {max_age_ms} ms"
            );
            self.checker.deploy_stuck(self.t, &who, &cell, detail);
        }
    }

    /// Watched: in a quiet fleet, a cell nothing has used for well over
    /// `idle_evict_ms` has been given back. The allowance covers a refused
    /// eviction, which waits out another idle period, and the sampling
    /// period. A cell with work in flight here is not idle: a request
    /// routed to it, or an output of its handler held at the gate. A cell
    /// with a socket open or an armed alarm is excused: the core may keep
    /// either resident.
    pub(super) fn judge_idle(&mut self, node: usize) {
        let Some(idle_ms) = self.config.idle_evict_ms else {
            return;
        };
        // The whole allowance must have passed in a quiet fleet: a process
        // that just woke from a pause has had no chance to evict.
        let allowance = 2 * idle_ms + 2 * LOAD_SAMPLE_MS + 2_000;
        let p = self.machines[node].proc();
        if self.t < self.settled_from() + allowance || p.shutdown.is_some() {
            return;
        }
        let kept: Vec<(String, u64)> = p
            .published
            .iter()
            .filter(|(cell, _)| !p.sockets.values().any(|s| s.cell == **cell))
            .filter(|(cell, _)| {
                !p.pending.values().any(|pending| match pending {
                    Pending::Client(req) => req.cell == **cell && req.epoch.is_some(),
                    Pending::Gate(gate) => gate.reveal.cell == **cell,
                    _ => false,
                })
            })
            .filter(|(cell, epoch)| {
                self.seen(node, cell, **epoch)
                    .is_some_and(|content| self.alarm_of(content, **epoch).at_ms().is_none())
            })
            .filter_map(|(cell, _)| {
                let idle = self.t - p.watch.touched.get(cell).copied().unwrap_or(0);
                (idle > allowance).then(|| (cell.clone(), idle))
            })
            .collect();
        let who = self.who(node);
        for (cell, idle) in kept {
            self.checker.idle_kept(self.t, &who, &cell, idle);
        }
    }

    /// Watched: in a quiet fleet, a node that latched on memory pressure
    /// sheds down to its resume line, 80% of the ceiling, and releases the
    /// latch within a few samples. Shedding cuts a tenth of the residents
    /// per sample (`release_target`), and here every cell's memory comes
    /// back when it goes, so 10 s is plenty. A node holding a socket is
    /// excused: a socket can pin its cell resident.
    pub(super) fn judge_memory(&mut self, node: usize) {
        let t = self.t;
        let p = self.machines[node].proc_mut();
        if !p.state.shedding() || !p.sockets.is_empty() {
            p.watch.shedding_since = None;
            return;
        }
        let since = *p.watch.shedding_since.get_or_insert(t);
        let memory = p.memory;
        if t >= self.settled_from() + MEMORY_RELIEF_MS && t - since > MEMORY_RELIEF_MS {
            let who = self.who(node);
            self.checker.memory_kept(t, &who, memory, t - since);
        }
    }

    /// **Every durable alarm fires.** When the probe ends, no cell's newest
    /// durable database may still hold an alarm that came due long before.
    /// When the audit ends, the same is watched.
    pub(super) fn judge_alarms(&mut self, audit: bool) {
        let now = (WALL_EPOCH_MS + self.t) as i64;
        for cell in 0..self.config.cells {
            let cell = format!("c{cell}");
            let Some((epoch, content)) = self.store.newest(&cell) else {
                continue;
            };
            let at_ms = self.alarm_of(content, epoch).at_ms();
            if let Some(at_ms) = at_ms.filter(|at| *at + (ALARM_SLACK_MS as i64) < now) {
                if audit {
                    self.checker.audit_alarm_unfired(self.t, &cell, now - at_ms);
                } else {
                    self.checker.alarm_unfired(self.t, &cell, now - at_ms);
                }
            }
        }
    }
}
