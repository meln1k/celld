//! Gray-failure eviction, as `node_log.rs`'s posture watch runs it with
//! `log_evict`: idle probes keep every member's latency sampled, and a
//! member judged gray, or every member stalled at once, degrades the
//! shipper.

use super::super::{Work, World};
use super::{append_deadline_ms, LogWork, Posture};
use celld_logic::log_evict::{EvictionPolicy, Verdict};

/// How long a member must be quiet before it owes an idle probe.
const PROBE_QUIET_MS: u64 = 2_000;

impl World {
    /// A follower answers a leader's idle probe, after its disk's delay.
    pub(super) fn answer_probe(&mut self, node: usize, leader: (usize, u64), started: u64) {
        let work = Work::Log(LogWork::ProbeAnswered {
            follower: node,
            started,
        });
        let fsync = self.gray_delay(node);
        self.peer_send_after(node, leader.0, Some(leader.1), work, fsync);
    }

    /// `probe_followers`: an idle probe to each member with nothing in
    /// flight and no answer in the last 2 s, degraded or not.
    pub(super) fn probe_followers(&mut self, node: usize) {
        let mono = self.mono(node);
        let leader = (node, self.machines[node].inc);
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        let members: Vec<(usize, String)> = (self.session(&log.session).ensemble.iter())
            .map(|&m| (m, self.machines[m].name.clone()))
            .collect();
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        let mut due = Vec::new();
        for (member, name) in members {
            if log.health.probe_due(&name, mono, PROBE_QUIET_MS) {
                log.health.append_started(&name, mono);
                log.sent.insert((0, member), mono);
                due.push(member);
            }
        }
        for member in due {
            let work = Work::Log(LogWork::Probe {
                leader,
                started: mono,
            });
            self.peer_send(node, member, None, work);
            let deadline = Work::Log(LogWork::ProbeDeadline {
                follower: member,
                started: mono,
            });
            self.work(node, append_deadline_ms(), deadline);
        }
    }

    /// A probe's answer: proof this node is not cut off.
    pub(super) fn probed(&mut self, node: usize, follower: usize, started: u64) {
        if self.probe_settled(node, follower, started) {
            let log = self.machines[node]
                .proc_mut()
                .log
                .as_mut()
                .expect("settled");
            log.suspect_self = false;
        }
    }

    /// A probe answered or ran out its lane deadline, whichever came
    /// first: the lane records the sample. Returns whether this settled it.
    pub(super) fn probe_settled(&mut self, node: usize, follower: usize, started: u64) -> bool {
        let mono = self.mono(node);
        let name = self.machines[follower].name.clone();
        let Some(log) = self.machines[node].proc_mut().log.as_mut() else {
            return false;
        };
        if log.sent.get(&(0, follower)) != Some(&started) {
            return false;
        }
        log.sent.remove(&(0, follower));
        log.health
            .append_completed(&name, mono, mono.saturating_sub(started));
        true
    }

    /// `evict_gray_followers`: on a shipping log, every member stalled past
    /// the backstop at once is our own fault, so degrade and suspect
    /// ourselves; otherwise, at most once per swap interval, degrade on the
    /// first member `log_evict` judges gray.
    pub(super) fn evict_gray_followers(&mut self, node: usize) {
        let mono = self.mono(node);
        let Some(log) = self.machines[node].proc().log.as_ref() else {
            return;
        };
        if log.posture != Posture::Shipping {
            return;
        }
        let members: Vec<String> = (self.session(&log.session).ensemble.iter())
            .map(|&m| self.machines[m].name.clone())
            .collect();
        let policy = EvictionPolicy::default();
        let log = self.machines[node]
            .proc_mut()
            .log
            .as_mut()
            .expect("checked");
        if log.health.correlated_stall(&policy, &members, mono) {
            log.suspect_self = true;
            return self.degrade(node, "correlated member stall; suspecting ourselves");
        }
        if !log.health.swap_allowed(&policy, mono) {
            return;
        }
        for member in &members {
            if log.health.verdict(&policy, member, &members, mono) == Verdict::Evict {
                log.health.evicted(&policy, member, mono);
                return self.degrade(node, &format!("gray follower {member} evicted"));
            }
        }
    }
}
