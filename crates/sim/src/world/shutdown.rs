//! Graceful shutdowns, as the operator asks for them in `main.rs`.
//!
//! - A **drain** hands every cell to a peer (`ReleaseAll`), then the
//!   process exits once the core reports nothing left, and the supervisor
//!   starts a fresh one later.
//! - A **preserve** is a planned replacement on the same machine
//!   (`BeginPreserve`). Cells stay owned and resident while requests
//!   already admitted finish. Then each open database uploads its final
//!   position and closes in place, and the process leaves a clean-reload
//!   marker naming its generation. The replacement starts at once with that
//!   generation (`resume_generation`); if it takes the node lease over
//!   before the old one expires, its core resumes each database where it
//!   is, at the same epoch, without reading the bucket.
//!
//! Either way, the shutdown has a deadline. A process that reaches it exits
//! as it stands, and its cells recover as a crashed node's do.

use celld_logic::Event;

use super::{Time, Work, World};

/// How long a shutdown may take before the process exits anyway.
const SHUTDOWN_DEADLINE_MS: u64 = 20_000;
/// How often the shell asks whether the shutdown is done.
const SHUTDOWN_POLL_MS: u64 = 50;

/// A shutdown under way, and when it gives up.
#[derive(Clone, Copy)]
pub(super) struct Shutdown {
    pub(super) mode: Mode,
    deadline: Time,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Drain,
    Preserve,
    /// The preserve drained: final uploads before the marker.
    Closing,
}

impl World {
    pub(super) fn shut_down(&mut self, node: usize, mode: Mode) {
        let p = self.machines[node].proc_mut();
        if p.shutdown.is_some() {
            return;
        }
        let deadline = self.t + SHUTDOWN_DEADLINE_MS;
        p.shutdown = Some(Shutdown { mode, deadline });
        // Until the restart that follows it is over, the fleet is disturbed.
        self.disturbed_until(deadline + 3 * self.config.ttl_ms);
        let who = self.who(node);
        match mode {
            Mode::Drain => {
                self.stats.drains += 1;
                self.note(|| format!("drain {who}"));
                self.drive(node, Event::ReleaseAll);
            }
            Mode::Preserve | Mode::Closing => {
                self.stats.preserves += 1;
                self.note(|| format!("preserve {who}"));
                self.drive(node, Event::BeginPreserve);
            }
        }
        self.work(node, SHUTDOWN_POLL_MS, Work::ShutdownPoll);
    }

    pub(super) fn shutdown_poll(&mut self, node: usize) {
        let p = self.machines[node].proc();
        let Shutdown { mode, deadline } = p.shutdown.expect("shutting down");
        let s = &p.state;
        if self.t >= deadline {
            return self.exit(node, None);
        }
        match mode {
            // `core_drained` for a handoff in `main.rs`.
            Mode::Drain => {
                let drained = s.occupied() == 0
                    && s.activating() == 0
                    && s.quiescing() == 0
                    && s.evicting() == 0
                    && s.releasing() == 0
                    && s.adopting() == 0;
                if drained {
                    return self.exit(node, None);
                }
            }
            // A preserve waits for its requests and its cold work.
            Mode::Preserve => {
                if p.requests.is_empty() && s.activating() == 0 && s.evicting() == 0 {
                    // `prepare_clean_reload`: upload each database's final
                    // position, and keep its file.
                    let open: Vec<_> = p.replicas.keys().cloned().collect();
                    self.machines[node].proc_mut().shutdown = Some(Shutdown {
                        mode: Mode::Closing,
                        deadline,
                    });
                    for (cell, epoch) in open {
                        self.upload(node, &cell, epoch);
                    }
                }
            }
            Mode::Closing => {
                if self.all_uploaded(node) {
                    // The marker is written only while the node still holds
                    // its lease.
                    let marker = s
                        .node_authoritative()
                        .then(|| generation(node, self.machines[node].inc));
                    return self.exit(node, marker);
                }
            }
        }
        self.work(node, SHUTDOWN_POLL_MS, Work::ShutdownPoll);
    }

    /// The process exits. With a clean-reload marker, the files of every
    /// database it did not keep open are pruned, and the replacement starts
    /// at once.
    fn exit(&mut self, node: usize, marker: Option<String>) {
        let who = self.who(node);
        let clean = marker.is_some();
        self.note(|| format!("exit {who} (clean reload: {clean})"));
        if clean {
            let keep: Vec<_> = self.machines[node]
                .proc()
                .replicas
                .keys()
                .cloned()
                .collect();
            self.machines[node].disk.prune_except(&keep);
        }
        self.machines[node].reload = marker;
        self.crash(node, false);
    }
}

/// The generation a process announces in its node lease.
pub(super) fn generation(node: usize, inc: u64) -> String {
    format!("g{node}.{inc}")
}
