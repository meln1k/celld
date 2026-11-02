//! The fleet drain token (`crates/celld/drain_token.rs`) and the
//! first-readiness gate that reads it (`main.rs`), with
//! `celld_logic::drain`'s decisions.
//!
//! A draining donor snapshots the live nodes' cold work, then claims the
//! token, one bucket object written under its ETag, before it hands any
//! cell off: it reads the token, and writes a claim only if `may_claim`
//! allows it. A lost race or a failed call tries again a second later, for
//! up to three quarters of the shutdown budget; after that the donor hands
//! off unserialized. The core hears nothing while it waits; the process
//! already refuses new requests and handoffs. It releases its claim on the
//! way out, deadline or not, by writing an expired record under its ETag; a
//! crashed holder's claim lapses only after the token's lifetime.
//!
//! A fresh process opens its readiness once `fleet_status` finds the fleet
//! settled, reading the token and the node leases every second. Readiness
//! is one-shot, and in production it gates the balancer: here, a process
//! that is not ready, or is draining, ignores the operator's rebalance. The
//! gate runs only in seeds with a balancer, the one thing it gates here.
//!
//! Not modelled: the claim's renewal, which no drain here lives long enough
//! to need (`renew_due` is half of a 120 s lifetime), and the reserve
//! before the deadline that production keeps for the release.

use celld_logic::drain::{self, DrainToken, RestorationBaseline};
use celld_logic::{CapacityPeer, Event};

use super::bucket::StoreReq;
use super::{Time, Work, World};
use crate::Mutation;

/// How long one claim lives without a renewal (`TOKEN_TTL_MS`).
pub(super) const TOKEN_TTL_MS: u64 = 120_000;
/// Between claim attempts while another donor holds the token.
const RETRY_MS: u64 = 1_000;
/// Between readiness checks.
const GATE_TICK_MS: u64 = 1_000;

/// A process's part in the drain token: whether it is ready, since when it
/// runs, and its claim while it drains.
#[derive(Default)]
pub(super) struct ProcDrain {
    ready: bool,
    started: Time,
    donor: Option<Donor>,
}

/// A donor's claim on the token.
struct Donor {
    started_mono: u64,
    wait_ms: u64,
    baseline: Vec<RestorationBaseline>,
    /// The wall clock when its current read began. Production samples
    /// `now_ms` before the peer read and before each claim's read, and
    /// judges what comes back by that sample.
    read_ms: u64,
    /// The ETag of the claim it holds, once it holds one.
    hold: Option<u64>,
    /// The token is decided, and the core is handing cells off.
    handing_off: bool,
}

/// One bucket call about the token.
#[derive(Debug)]
pub(super) enum DrainReq {
    /// The donor's snapshot of the live nodes' cold work.
    Baseline,
    /// `try_claim`'s read of the token.
    Read,
    /// The conditional write of a claim, over `guard` (absent: none).
    Claim {
        guard: Option<u64>,
        token: DrainToken,
    },
    /// The release: an expired record under the claim's ETag.
    Release { etag: u64, token: DrainToken },
    /// The readiness gate reads the token and the node leases.
    Gate,
}

#[derive(Debug)]
pub(super) enum DrainWork {
    Baselined(Vec<CapacityPeer>),
    /// The token, with its ETag; `None` when absent or unreadable.
    Read(Option<(DrainToken, u64)>),
    /// The claim landed with this ETag, or did not.
    Claimed(Option<u64>),
    Retry,
    GateTick,
    /// The token and the leases; `None` when unreadable.
    GateRead(Option<(Option<DrainToken>, Vec<CapacityPeer>)>),
    Released,
}

impl World {
    /// A process starts: it is not ready yet.
    pub(super) fn start_ready_gate(&mut self, node: usize) {
        let t = self.t;
        self.machines[node].proc_mut().drain.started = t;
        if self.config.operator.rebalances > 0.0 {
            self.work(node, 0, Work::Drain(DrainWork::GateTick));
        }
    }

    /// Whether the balancer may run here: the process is ready and not
    /// shutting down.
    pub(super) fn may_rebalance(&self, node: usize) -> bool {
        let p = self.machines[node].proc();
        p.drain.ready && p.shutdown.is_none()
    }

    /// A drain begins: claim the token within `wait_ms` before handing off.
    pub(super) fn claim_token(&mut self, node: usize, wait_ms: u64) {
        let (started_mono, read_ms) = (self.mono(node), self.wall(node));
        self.machines[node].proc_mut().drain.donor = Some(Donor {
            started_mono,
            wait_ms,
            baseline: Vec::new(),
            read_ms,
            hold: None,
            handing_off: false,
        });
        self.store_call(node, StoreReq::Drain(DrainReq::Baseline));
    }

    /// The process exits: release the claim it holds.
    pub(super) fn release_token(&mut self, node: usize) {
        let me = self.machines[node].name.clone();
        let donor = self.machines[node].proc_mut().drain.donor.take();
        if let Some(Donor {
            hold: Some(etag),
            baseline,
            ..
        }) = donor
        {
            let token = DrainToken {
                node: me,
                expires_ms: 0,
                restoration_baseline: baseline,
            };
            self.store_call(node, StoreReq::Drain(DrainReq::Release { etag, token }));
        }
    }

    pub(super) fn drain_work(&mut self, node: usize, work: DrainWork) {
        let me = self.machines[node].name.clone();
        match work {
            DrainWork::Baselined(peers) => {
                let Some(now_ms) = self.read_ms(node) else {
                    return;
                };
                let mut baseline: Vec<RestorationBaseline> = (peers.into_iter())
                    .filter(|peer| peer.expires_ms > now_ms)
                    .map(|peer| RestorationBaseline {
                        node: peer.node,
                        restoring: peer.restoring,
                    })
                    .collect();
                baseline.sort_by(|a, b| a.node.cmp(&b.node));
                if let Some(donor) = self.machines[node].proc_mut().drain.donor.as_mut() {
                    donor.baseline = baseline;
                }
                self.read_token(node);
            }
            DrainWork::Read(current) => {
                let Some(now_ms) = self.read_ms(node) else {
                    return;
                };
                let held = current.as_ref().map(|(token, _)| token);
                let may = self.config.mutation == Some(Mutation::IgnoreDrainToken)
                    || drain::may_claim(held, &me, now_ms);
                let donor = self.machines[node].proc().drain.donor.as_ref();
                let donor = donor.expect("read_ms found the donor");
                if !may {
                    return self.claim_failed(node);
                }
                let token = DrainToken {
                    node: me,
                    expires_ms: now_ms + TOKEN_TTL_MS,
                    restoration_baseline: donor.baseline.clone(),
                };
                let guard = current.map(|(_, etag)| etag);
                self.store_call(node, StoreReq::Drain(DrainReq::Claim { guard, token }));
            }
            DrainWork::Claimed(Some(etag)) => {
                if let Some(donor) = self.machines[node].proc_mut().drain.donor.as_mut() {
                    donor.hold = Some(etag);
                    self.stats.drain_claims += 1;
                    self.hand_off(node);
                }
            }
            DrainWork::Claimed(None) => self.claim_failed(node),
            DrainWork::Retry => self.read_token(node),
            DrainWork::GateTick => {
                if !self.machines[node].proc().drain.ready {
                    self.store_call(node, StoreReq::Drain(DrainReq::Gate));
                }
            }
            DrainWork::GateRead(read) => self.gate_read(node, read),
            DrainWork::Released => {}
        }
    }

    /// When the donor's current read began, on its wall clock: what
    /// production judges the answer by. `None` once the donor is gone.
    fn read_ms(&self, node: usize) -> Option<u64> {
        let donor = self.machines[node].proc().drain.donor.as_ref();
        donor.map(|donor| donor.read_ms)
    }

    /// `try_claim`: sample the clock, then read the token.
    fn read_token(&mut self, node: usize) {
        let wall = self.wall(node);
        if let Some(donor) = self.machines[node].proc_mut().drain.donor.as_mut() {
            donor.read_ms = wall;
        }
        self.store_call(node, StoreReq::Drain(DrainReq::Read));
    }

    /// No claim this time: try again a second later, or, once the wait is
    /// spent, hand off unserialized.
    fn claim_failed(&mut self, node: usize) {
        let mono = self.mono(node);
        let Some(donor) = self.machines[node].proc().drain.donor.as_ref() else {
            return;
        };
        let waited = mono.saturating_sub(donor.started_mono);
        if waited >= donor.wait_ms {
            self.stats.drain_bypasses += 1;
            return self.hand_off(node);
        }
        let delay = self.agenda_ms(RETRY_MS.min(donor.wait_ms - waited));
        self.work(node, delay, Work::Drain(DrainWork::Retry));
    }

    /// Whether the process may finish its drain: it is not still waiting
    /// on the token.
    pub(super) fn token_decided(&self, node: usize) -> bool {
        let donor = self.machines[node].proc().drain.donor.as_ref();
        donor.is_none_or(|donor| donor.handing_off)
    }

    /// The token is decided: hand every cell off.
    fn hand_off(&mut self, node: usize) {
        let Some(donor) = self.machines[node].proc_mut().drain.donor.as_mut() else {
            return;
        };
        if donor.handing_off {
            return;
        }
        donor.handing_off = true;
        self.judge_donors(node);
        self.drive(node, Event::ReleaseAll);
    }

    /// The gate's read came back: open readiness if the fleet is settled.
    fn gate_read(&mut self, node: usize, read: Option<(Option<DrainToken>, Vec<CapacityPeer>)>) {
        let me = self.machines[node].name.clone();
        let max_restoring = self.config.max_activations as u64;
        // Judged after the I/O, as production does.
        let now_ms = self.wall(node);
        let status = match &read {
            Some((token, peers)) => {
                let mut token = token.clone();
                if self.config.mutation == Some(Mutation::GateReadsReleasedClaim) {
                    // The planted bug reads another node's released claim
                    // as live.
                    if let Some(token) = token.as_mut().filter(|token| token.node != me) {
                        token.expires_ms = u64::MAX;
                    }
                }
                drain::fleet_status(token.as_ref(), &me, now_ms, peers, max_restoring)
                    .map_err(|why| format!("{why:?}"))
            }
            None => Err("Unreadable".to_string()),
        };
        match status {
            Ok(()) => {
                self.machines[node].proc_mut().drain.ready = true;
                self.stats.ready += 1;
            }
            // The planted bug stops asking after one unsettled read.
            Err(_) if self.config.mutation == Some(Mutation::GateStopsAsking) => {}
            Err(_) => {
                let delay = self.agenda_ms(GATE_TICK_MS);
                self.work(node, delay, Work::Drain(DrainWork::GateTick));
            }
        }
    }

    /// Whether the process runs a readiness gate that has not opened yet,
    /// and since when it runs.
    pub(super) fn unready_since(&self, node: usize) -> Option<Time> {
        let p = self.machines[node].proc.as_ref()?;
        let gated = self.config.operator.rebalances > 0.0;
        (gated && !p.drain.ready && p.shutdown.is_none()).then_some(p.drain.started)
    }

    /// Whether the process is handing cells off under a claim of its own.
    pub(super) fn donor_under_claim(&self, node: usize) -> bool {
        (self.machines[node].proc.as_ref())
            .and_then(|p| p.drain.donor.as_ref())
            .is_some_and(|donor| donor.handing_off && donor.hold.is_some())
    }

    /// The call reaches the bucket. Returns the work its answer does.
    pub(super) fn drain_apply(&mut self, req: DrainReq) -> Work {
        let work = match req {
            DrainReq::Baseline => DrainWork::Baselined(self.store.capacity_peers()),
            DrainReq::Read => DrainWork::Read(self.store.drain_token().cloned()),
            DrainReq::Claim { guard, token } => {
                DrainWork::Claimed(self.store.cas_drain_token(guard, token))
            }
            DrainReq::Release { etag, token } => {
                self.store.cas_drain_token(Some(etag), token);
                DrainWork::Released
            }
            DrainReq::Gate => DrainWork::GateRead(Some((
                self.store.drain_token().map(|(token, _)| token.clone()),
                self.store.capacity_peers(),
            ))),
        };
        Work::Drain(work)
    }

    /// The call failed: what its caller makes of that. An unreadable token
    /// counts as absent for a claim and as unsettled for the gate.
    pub(super) fn drain_failed(req: &DrainReq) -> Work {
        let work = match req {
            DrainReq::Baseline => DrainWork::Baselined(Vec::new()),
            DrainReq::Read => DrainWork::Read(None),
            DrainReq::Claim { .. } => DrainWork::Claimed(None),
            DrainReq::Release { .. } => DrainWork::Released,
            DrainReq::Gate => DrainWork::GateRead(None),
        };
        Work::Drain(work)
    }
}
