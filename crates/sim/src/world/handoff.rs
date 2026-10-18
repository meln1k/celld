//! A released cell's successor: the donor's `AdoptReleased` task and the
//! successor's `/peer/handoff`, as `crates/celld/actor.rs` and
//! `internal_handoff` run them.
//!
//! The donor ranks the live peers that accept a paced handoff and asks each
//! in turn to acquire the cell at an epoch above the one it released. A peer
//! that knows a newer owner names it, and the donor follows that name a few
//! hops. The successor runs the request through its core as
//! `HandoffRequestAt`: the owner record moves now, and the runtime waits for
//! real traffic. It answers as soon as its core has accepted the next epoch,
//! and cancels the synthetic request. A drain retries until the process
//! ends; a balancing move gives up at the operation deadline and leaves the
//! cell unowned for its next request.

use super::*;
use celld_logic::AdoptedCell;

/// `HANDOFF_DIRECTORY_MAX_AGE_MS`.
const DIRECTORY_MAX_AGE_MS: u64 = 500;
/// `HANDOFF_RETRY_DELAY`.
const RETRY_MS: u64 = 250;
/// How long the donor waits on one peer's answer.
const PEER_TIMEOUT_MS: u64 = 5_000;
/// How many owners the donor follows from one candidate.
const HOPS: usize = 4;

/// One `AdoptReleased` in flight on the donor.
#[derive(Debug)]
pub(super) struct Handoff {
    cell: String,
    released_epoch: Epoch,
    rebalance: bool,
    candidates: VecDeque<CapacityPeer>,
    chain: Option<Chain>,
    /// Numbers each request, so a timed-out answer that arrives late is
    /// ignored.
    attempt: u64,
}

/// The owners one candidate led to.
#[derive(Debug)]
struct Chain {
    target: AdoptedCell,
    visited: BTreeSet<String>,
}

/// A successor's answer (`HandoffAttempt`); `None` is a failed request.
#[derive(Clone, Debug)]
pub(super) enum Answer {
    Accepted(AdoptedCell),
    CurrentOwner(AdoptedCell),
    Refused,
}

/// A handoff request on the successor.
#[derive(Debug)]
pub(super) struct Accept {
    cell: String,
    released_epoch: Epoch,
    donor: ProcId,
    op: OpId,
    attempt: u64,
    /// The request already retried once past a stale route to the donor.
    retried: bool,
}

impl World {
    // -----------------------------------------------------------------------
    // The donor

    pub(super) fn adopt_released(
        &mut self,
        id: ProcId,
        op: OpId,
        cell: String,
        released_epoch: Epoch,
        rebalance: bool,
        immediate: &mut VecDeque<Event>,
    ) {
        if !self.config.lifecycle.hands_off() {
            // No peer here accepts a handoff, as the in-memory adapter's.
            immediate.push_back(Event::SuccessorAdopted {
                op,
                result: Err(Failure::Definite),
            });
            return;
        }
        let handoff = Handoff {
            cell,
            released_epoch,
            rebalance,
            candidates: VecDeque::new(),
            chain: None,
            attempt: 0,
        };
        self.procs
            .get_mut(&id)
            .unwrap()
            .handoffs
            .insert(op, handoff);
        if rebalance {
            self.work(id, OPERATION_DEADLINE_MS, Work::HandoffDeadline { op });
        }
        self.handoff_scan(id, op);
    }

    /// Rank the peers again, from the shared directory while it is fresh.
    pub(super) fn handoff_scan(&mut self, id: ProcId, op: OpId) {
        if !self.procs[&id].handoffs.contains_key(&op) {
            return;
        }
        let t = self.t;
        let fresh = self.procs[&id]
            .directory
            .as_ref()
            .filter(|(sampled, _)| t < sampled + DIRECTORY_MAX_AGE_MS)
            .map(|(_, peers)| peers.clone());
        match fresh {
            Some(peers) => self.handoff_ranked(id, op, peers),
            None => self.store_call(id, StoreReq::ReadPeers, Reply::Handoff { op }),
        }
    }

    pub(super) fn handoff_peers(
        &mut self,
        id: ProcId,
        op: OpId,
        result: Result<StoreRes, Failure>,
    ) {
        let peers = match result {
            Ok(StoreRes::Peers { peers, .. }) => {
                self.procs.get_mut(&id).unwrap().directory = Some((self.t, peers.clone()));
                peers
            }
            Ok(other) => unreachable!("expected the peers, got {other:?}"),
            Err(_) => Vec::new(),
        };
        self.handoff_ranked(id, op, peers);
    }

    /// `rank_handoff_candidates`, narrowed to the receivers for a balancing
    /// move.
    fn handoff_ranked(&mut self, id: ProcId, op: OpId, peers: Vec<CapacityPeer>) {
        let Some(handoff) = self.procs[&id].handoffs.get(&op) else {
            return;
        };
        let source = self.node_name(id);
        let now = self.wall(id);
        let cell = handoff.cell.clone();
        let mut ranked: Vec<CapacityPeer> = peers
            .iter()
            .filter(|peer| {
                peer.node != source
                    && peer.expires_ms > now
                    && !peer.addr.is_empty()
                    && peer.peer_protocol == PEER_PROTOCOL
                    && peer.sampled_ms != 0
                    && peer.reports_adoption_capacity()
                    && peer.paced_handoff
            })
            .cloned()
            .collect();
        ranked.sort_by_key(|peer| {
            (
                peer.resident_cells,
                peer.host_websockets,
                peer.in_use_bytes.unwrap_or(peer.rss_bytes),
                tie_break(&cell, &peer.node),
                peer.node.clone(),
            )
        });
        if handoff.rebalance {
            let receivers =
                celld_logic::rebalance::receivers(&peers, &source, now, self.config.ttl_ms);
            ranked.retain(|peer| receivers.contains(&peer.node));
            ranked.sort_by_key(|peer| receivers.iter().position(|node| *node == peer.node));
        }
        let handoff = self
            .procs
            .get_mut(&id)
            .unwrap()
            .handoffs
            .get_mut(&op)
            .unwrap();
        handoff.candidates = ranked.into();
        handoff.chain = None;
        self.handoff_next(id, op);
    }

    /// Ask the next owner in the chain, or the next candidate, or wait and
    /// rank again once every candidate refused.
    fn handoff_next(&mut self, id: ProcId, op: OpId) {
        let source = self.node_name(id);
        let handoff = self
            .procs
            .get_mut(&id)
            .unwrap()
            .handoffs
            .get_mut(&op)
            .unwrap();
        loop {
            if let Some(chain) = handoff.chain.as_mut() {
                let target = &chain.target;
                let valid = target.node != source
                    && !target.addr.is_empty()
                    && target.peer_protocol == PEER_PROTOCOL
                    && chain.visited.len() < HOPS
                    && chain.visited.insert(target.node.clone());
                if valid {
                    break;
                }
                handoff.chain = None;
            }
            let Some(peer) = handoff.candidates.pop_front() else {
                self.work(id, RETRY_MS, Work::HandoffRetry { op });
                return;
            };
            handoff.chain = Some(Chain {
                target: AdoptedCell {
                    node: peer.node,
                    addr: peer.addr,
                    epoch: 0,
                    peer_protocol: peer.peer_protocol,
                },
                visited: BTreeSet::new(),
            });
        }
        handoff.attempt += 1;
        let attempt = handoff.attempt;
        let (cell, released_epoch) = (handoff.cell.clone(), handoff.released_epoch);
        let target = handoff.chain.as_ref().unwrap().target.node.clone();
        self.stats.handoff_requests += 1;
        let from = self.procs[&id].node;
        let delay = self.net_rng.range(1, 5);
        let to = self.nodes.iter().position(|node| node.name == target);
        let sent = to.map_or(Err(Failure::Definite), |to| {
            self.peer_reach(from, to).map(|target| {
                let work = Work::HandoffArrive {
                    donor: id,
                    op,
                    attempt,
                    cell,
                    released_epoch,
                };
                self.work(target, delay, work);
            })
        });
        let timeout = match sent {
            Ok(()) | Err(Failure::Ambiguous) => PEER_TIMEOUT_MS,
            Err(Failure::Definite) => delay,
        };
        self.work(id, timeout, Work::HandoffTimeout { op, attempt });
    }

    /// The process a peer request from node `from` reaches on node `to`.
    pub(super) fn peer_reach(&self, from: usize, to: usize) -> Result<ProcId, Failure> {
        for node in [from, to] {
            match self.nodes[node].partition {
                Some((_, PartitionMode::Refuse)) => return Err(Failure::Definite),
                Some((_, PartitionMode::Hang)) => return Err(Failure::Ambiguous),
                None => {}
            }
        }
        self.nodes[to].proc.ok_or(Failure::Definite)
    }

    pub(super) fn handoff_answered(
        &mut self,
        id: ProcId,
        op: OpId,
        attempt: u64,
        answer: Option<Answer>,
    ) {
        let Some(handoff) = self.procs.get_mut(&id).unwrap().handoffs.get_mut(&op) else {
            return;
        };
        if handoff.attempt != attempt {
            return;
        }
        // Answers to this attempt that arrive later are stale.
        handoff.attempt += 1;
        match answer {
            Some(Answer::Accepted(adopted)) if adopted.epoch > handoff.released_epoch => {
                self.stats.handoffs_accepted += 1;
                self.procs.get_mut(&id).unwrap().handoffs.remove(&op);
                self.drive(
                    id,
                    Event::SuccessorAdopted {
                        op,
                        result: Ok(adopted),
                    },
                );
            }
            Some(Answer::CurrentOwner(owner)) => {
                if let Some(chain) = handoff.chain.as_mut() {
                    chain.target = owner;
                }
                self.handoff_next(id, op);
            }
            Some(Answer::Accepted(_) | Answer::Refused) | None => {
                handoff.chain = None;
                self.handoff_next(id, op);
            }
        }
    }

    /// A balancing move found no successor in time.
    pub(super) fn handoff_deadline(&mut self, id: ProcId, op: OpId) {
        if self
            .procs
            .get_mut(&id)
            .unwrap()
            .handoffs
            .remove(&op)
            .is_some()
        {
            let result = Err(Failure::Definite);
            self.drive(id, Event::SuccessorAdopted { op, result });
        }
    }

    // -----------------------------------------------------------------------
    // The successor

    /// `internal_handoff`: a draining node refuses, as does a preserving
    /// one; any other runs `accept_handoff`.
    pub(super) fn handoff_arrived(
        &mut self,
        id: ProcId,
        donor: ProcId,
        op: OpId,
        attempt: u64,
        cell: String,
        released_epoch: Epoch,
    ) {
        if self.procs[&id].shutdown.is_some() {
            return self.handoff_answer(id, donor, op, attempt, Some(Answer::Refused));
        }
        let accept = Accept {
            cell,
            released_epoch,
            donor,
            op,
            attempt,
            retried: false,
        };
        if self.config.mutation == Mutation::AcceptUnclaimed {
            return self.accepted(id, &accept, released_epoch + 1);
        }
        self.accept_handoff(id, accept);
    }

    fn accept_handoff(&mut self, id: ProcId, accept: Accept) {
        let request = self.next_request;
        self.next_request += 1;
        let cell = accept.cell.clone();
        self.procs
            .get_mut(&id)
            .unwrap()
            .accepts
            .insert(request, accept);
        self.drive(
            id,
            Event::HandoffRequestAt {
                request,
                cell,
                now_ms: 0,
                now_mono_ms: 0,
            },
        );
    }

    /// `settle_handoff_accepts`, after every step of the core: answer each
    /// request whose cell the core now holds above the released epoch, and
    /// cancel its synthetic route.
    pub(super) fn settle_accepts(&mut self, id: ProcId) {
        let Some(proc) = self.procs.get(&id) else {
            return;
        };
        let accepted: Vec<(RequestId, Epoch)> = proc
            .accepts
            .iter()
            .filter_map(|(request, accept)| {
                proc.state
                    .accepted_epoch(&accept.cell)
                    .filter(|epoch| *epoch > accept.released_epoch)
                    .map(|epoch| (*request, epoch))
            })
            .collect();
        for (request, epoch) in accepted {
            // A cancel below steps the core, which settles again.
            let Some(accept) = self
                .procs
                .get_mut(&id)
                .and_then(|proc| proc.accepts.remove(&request))
            else {
                continue;
            };
            self.accepted(id, &accept, epoch);
            self.drive(id, Event::Cancel { request });
        }
    }

    fn accepted(&mut self, id: ProcId, accept: &Accept, epoch: Epoch) {
        let node = self.node_name(id);
        let (checker, view) = self.checks();
        checker.on_handoff_accepted(view, id, &node, &accept.cell, epoch);
        let adopted = AdoptedCell {
            addr: node.clone(),
            node,
            epoch,
            peer_protocol: PEER_PROTOCOL,
        };
        let (donor, op, attempt) = (accept.donor, accept.op, accept.attempt);
        self.handoff_answer(id, donor, op, attempt, Some(Answer::Accepted(adopted)));
    }

    /// The core routed a handoff request before it accepted the epoch.
    /// Returns whether `request` was one.
    pub(super) fn handoff_routed(
        &mut self,
        id: ProcId,
        request: RequestId,
        result: &Result<Route, celld_logic::RequestError>,
    ) -> bool {
        let Some(accept) = self.procs.get_mut(&id).unwrap().accepts.remove(&request) else {
            return false;
        };
        let answer = match result {
            Ok(Route::Local) => {
                // A repeated request can find the cell published already.
                self.drive(id, Event::ActivityFinished { request });
                let epoch = match self
                    .procs
                    .get(&id)
                    .and_then(|p| p.state.phase(&accept.cell))
                {
                    Some(Phase::Resident { epoch })
                    | Some(Phase::EnsuringDurability { epoch, .. }) => Some(*epoch),
                    _ => None,
                };
                match epoch.filter(|epoch| *epoch > accept.released_epoch) {
                    Some(epoch) => {
                        self.accepted(id, &accept, epoch);
                        return true;
                    }
                    None => None,
                }
            }
            Ok(Route::Remote {
                node,
                addr,
                epoch,
                peer_protocol,
            }) => {
                if *epoch <= accept.released_epoch && !accept.retried {
                    // A cached route to the donor itself: retire it and ask
                    // the owner record again.
                    let (cell, node, epoch) = (accept.cell.clone(), node.clone(), *epoch);
                    self.drive(id, Event::InvalidateRemote { cell, node, epoch });
                    if self.procs.contains_key(&id) {
                        let accept = Accept {
                            retried: true,
                            ..accept
                        };
                        self.accept_handoff(id, accept);
                    }
                    return true;
                }
                Some(Answer::CurrentOwner(AdoptedCell {
                    node: node.clone(),
                    addr: addr.clone(),
                    epoch: *epoch,
                    peer_protocol: *peer_protocol,
                }))
            }
            Err(_) => None,
        };
        let answer = answer.or(Some(Answer::Refused));
        let (donor, op, attempt) = (accept.donor, accept.op, accept.attempt);
        self.handoff_answer(id, donor, op, attempt, answer);
        true
    }

    /// The answer travels back to the donor, if it can.
    fn handoff_answer(
        &mut self,
        from: ProcId,
        donor: ProcId,
        op: OpId,
        attempt: u64,
        answer: Option<Answer>,
    ) {
        let Some(to) = self.procs.get(&donor).map(|proc| proc.node) else {
            return;
        };
        let from = self.procs[&from].node;
        if self.peer_reach(from, to) != Ok(donor) {
            return;
        }
        let delay = self.net_rng.range(1, 5);
        let work = Work::HandoffAnswer {
            op,
            attempt,
            answer,
        };
        self.work(donor, delay, work);
    }
}

/// `handoff_tie_break`: spread equal candidates by cell.
fn tie_break(cell: &str, node: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in cell.bytes().chain([0xff]).chain(node.bytes()) {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
