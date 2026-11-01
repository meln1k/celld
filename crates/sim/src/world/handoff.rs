//! Handing cells to peers. A draining node releases every cell, and a dense
//! node gives a few idle ones away (`Rebalance`). Each released cell is
//! offered to peers until one acquires it (`AdoptReleased`, and
//! `HandoffRequestAt` on the peer).
//!
//! This follows `crates/celld/actor.rs`: `handoff_candidates` and the
//! `AdoptReleased` loop on the donor, and `accept_handoff` and
//! `settle_handoff_accepts` on the peer.

use std::collections::{BTreeSet, VecDeque};

use celld_logic::{AdoptedCell, Epoch, Event, Failure, OpId, RequestId, Route};

use super::faults::Partition;
use super::{Item, Pending, Time, Work, World, OPERATION_DEADLINE_MS, PEER_PROTOCOL};

/// How long the donor waits before searching again when no peer took a
/// cell (`HANDOFF_RETRY_DELAY`).
const HANDOFF_RETRY_MS: u64 = 250;
/// Owner redirects one offer follows before trying the next peer.
const MAX_HOPS: usize = 4;
/// How long an offer waits to connect to a peer that does not answer.
const PEER_CONNECT_TIMEOUT_MS: u64 = 3_000;

/// The donor's search for a successor for one released cell.
pub(super) struct Adoption {
    cell: String,
    released_epoch: Epoch,
    /// A balancing move gives up at its deadline; a drain never does.
    deadline: Option<Time>,
    /// Peers still to offer the cell to, best first.
    queue: VecDeque<String>,
    /// The peer the current offer goes to next, after a redirect.
    target: Option<String>,
    /// Peers this offer already reached through redirects.
    visited: BTreeSet<String>,
}

/// An offer a peer is working on: the donor's process, and the donor's
/// operation to answer.
pub(super) struct Offer {
    cell: String,
    released_epoch: Epoch,
    donor: usize,
    donor_inc: u64,
    op: OpId,
}

/// What the peer told the donor.
pub(super) enum Answer {
    Accepted(AdoptedCell),
    /// The cell is owned elsewhere: offer it there.
    CurrentOwner(String),
    /// Refused, failed, or never connected.
    Failed,
}

/// A donor's work in handing a cell off.
pub(super) enum HandoffWork {
    /// The donor looks for peers to offer a released cell to.
    AdoptSearch { op: OpId },
    /// A peer answered the donor's offer.
    OfferAnswered { op: OpId, answer: Answer },
}

impl World {
    pub(super) fn handoff_work(&mut self, node: usize, work: HandoffWork) {
        match work {
            HandoffWork::AdoptSearch { op } => self.adopt_search(node, op),
            HandoffWork::OfferAnswered { op, answer } => self.offer_answered(node, op, answer),
        }
    }

    /// The core released a cell and wants a successor to acquire it.
    pub(super) fn adopt_released(
        &mut self,
        node: usize,
        op: OpId,
        cell: String,
        released_epoch: Epoch,
        rebalance: bool,
    ) {
        let adoption = Adoption {
            cell,
            released_epoch,
            deadline: rebalance.then_some(self.t + OPERATION_DEADLINE_MS),
            queue: VecDeque::new(),
            target: None,
            visited: BTreeSet::new(),
        };
        self.machines[node]
            .proc_mut()
            .adoptions
            .insert(op, adoption);
        self.work(node, 0, Work::Handoff(HandoffWork::AdoptSearch { op }));
    }

    /// Rank the peers that can take the cell, from the fleet's leases, and
    /// offer it to each in turn.
    pub(super) fn adopt_search(&mut self, node: usize, op: OpId) {
        let source = self.machines[node].name.clone();
        let now = self.wall(node);
        // The lease listing is a bucket read; a partitioned node gets none.
        let peers = match self.machines[node].partition {
            Some(_) => Vec::new(),
            None => self.store.capacity_peers(),
        };
        let p = self.machines[node].proc_mut();
        let Some(a) = p.adoptions.get_mut(&op) else {
            return;
        };
        let mut ranked: Vec<_> = peers
            .iter()
            .filter(|peer| {
                peer.node != source && peer.expires_ms > now && peer.peer_protocol == PEER_PROTOCOL
            })
            .collect();
        ranked.sort_by_key(|peer| (peer.resident_cells, peer.node.clone()));
        let mut queue: Vec<String> = ranked.into_iter().map(|peer| peer.node.clone()).collect();
        if a.deadline.is_some() {
            // A balancing move only goes to a peer below its target.
            let receivers =
                celld_logic::rebalance::receivers(&peers, &source, now, self.config.ttl_ms);
            queue.retain(|peer| receivers.contains(peer));
            queue.sort_by_key(|peer| receivers.iter().position(|r| r == peer));
        }
        a.queue = queue.into();
        self.offer_next(node, op);
    }

    /// Offer the cell to the current redirect target, or to the next peer.
    fn offer_next(&mut self, node: usize, op: OpId) {
        let source = self.machines[node].name.clone();
        let t = self.t;
        let p = self.machines[node].proc_mut();
        let Some(a) = p.adoptions.get_mut(&op) else {
            return;
        };
        let target = loop {
            let next = match a.target.take() {
                Some(owner) if a.visited.len() < MAX_HOPS => Some(owner),
                _ => {
                    a.visited.clear();
                    a.queue.pop_front()
                }
            };
            match next {
                Some(peer) if peer != source && a.visited.insert(peer.clone()) => break Some(peer),
                Some(_) => continue,
                None => break None,
            }
        };
        let Some(target) = target else {
            if a.deadline.is_some_and(|deadline| t >= deadline) {
                p.adoptions.remove(&op);
                let result = Err(Failure::Definite);
                return self.drive(node, Event::SuccessorAdopted { op, result });
            }
            // Nobody can take it yet; search again.
            let work = Work::Handoff(HandoffWork::AdoptSearch { op });
            self.work(node, HANDOFF_RETRY_MS, work);
            return;
        };
        let (cell, released_epoch) = (a.cell.clone(), a.released_epoch);
        let peer = self
            .machines
            .iter()
            .position(|m| m.name == target)
            .expect("leases name fleet nodes");
        let offer = Offer {
            cell,
            released_epoch,
            donor: node,
            donor_inc: self.machines[node].inc,
            op,
        };
        let delay = self.net_rng.range(1, 5);
        self.after(delay, Item::Offer { node: peer, offer });
    }

    /// An offer reaches a peer: hand it to the core as a handoff request.
    pub(super) fn offer_arrives(&mut self, node: usize, offer: Offer) {
        let paused = self.machines[node]
            .proc
            .as_ref()
            .and_then(|p| p.paused_until);
        if let Some(until) = paused {
            self.after(until - self.t, Item::Offer { node, offer });
            return;
        }
        let partitions =
            [node, offer.donor].map(|n| self.machines[n].partition.map(|(_, mode)| mode));
        if partitions.iter().any(Option::is_some) || !self.machines[node].up() {
            let delay = if partitions.contains(&Some(Partition::Hang)) {
                PEER_CONNECT_TIMEOUT_MS
            } else {
                self.net_rng.range(1, 5)
            };
            return self.answer_offer(offer, Answer::Failed, delay);
        }
        if self.machines[node].proc().shutdown.is_some() {
            // A draining peer answers `503` (`internal_handoff`), and a
            // preserving one refuses every request.
            let delay = self.net_rng.range(1, 5);
            return self.answer_offer(offer, Answer::Failed, delay);
        }
        let cell = offer.cell.clone();
        let request = self.pend(node, Pending::Offer(offer));
        let event = Event::HandoffRequestAt {
            request,
            cell,
            now_ms: 0,
            now_mono_ms: 0,
        };
        self.drive(node, event);
    }

    /// After every event: answer each offer whose cell this core now owns
    /// at a newer epoch than the donor released (`settle_handoff_accepts`).
    pub(super) fn settle_offers(&mut self, node: usize) {
        let Some(p) = self.machines[node].proc.as_ref() else {
            return;
        };
        let accepted: Vec<(RequestId, Epoch)> = p
            .offers()
            .filter_map(|(request, offer)| {
                p.state
                    .accepted_epoch(&offer.cell)
                    .filter(|epoch| *epoch > offer.released_epoch)
                    .map(|epoch| (request, epoch))
            })
            .collect();
        for (request, epoch) in accepted {
            let adopted = self.adopted(node, epoch);
            self.finish_offer(node, request, Answer::Accepted(adopted));
        }
    }

    /// The core routed a handoff request before it acquired the cell.
    pub(super) fn offer_routed(
        &mut self,
        node: usize,
        request: RequestId,
        result: &Result<Route, celld_logic::RequestError>,
        immediate: &mut VecDeque<Event>,
    ) {
        let p = self.machines[node].proc();
        let Some(Pending::Offer(offer)) = p.pending.get(&request) else {
            unreachable!("an offer request");
        };
        let answer = match result {
            Ok(Route::Local) => {
                // Already resident here. The activity it took is dropped.
                immediate.push_back(Event::ActivityFinished { request });
                let epoch = p
                    .state
                    .accepted_epoch(&offer.cell)
                    .filter(|epoch| *epoch > offer.released_epoch);
                match epoch {
                    Some(epoch) => Answer::Accepted(self.adopted(node, epoch)),
                    None => Answer::Failed,
                }
            }
            Ok(Route::Remote { node: owner, .. }) => Answer::CurrentOwner(owner.clone()),
            Err(_) => Answer::Failed,
        };
        self.finish_offer(node, request, answer);
    }

    /// The offer is decided: answer the donor and retire the request, as
    /// `accept_handoff` does with `Message::CancelRoute`.
    fn finish_offer(&mut self, node: usize, request: RequestId, answer: Answer) {
        let offer = self.machines[node].proc_mut().take_offer(request).unwrap();
        let delay = self.net_rng.range(1, 5);
        self.answer_offer(offer, answer, delay);
        let delay = self.net_rng.range(0, 3);
        self.deliver(node, delay, Event::Cancel { request });
    }

    fn adopted(&self, node: usize, epoch: Epoch) -> AdoptedCell {
        let name = self.machines[node].name.clone();
        AdoptedCell {
            node: name.clone(),
            addr: name,
            epoch,
            peer_protocol: PEER_PROTOCOL,
        }
    }

    /// Send the peer's answer back to the donor process that asked.
    pub(super) fn answer_offer(&mut self, offer: Offer, answer: Answer, delay: u64) {
        let (node, inc, op) = (offer.donor, offer.donor_inc, offer.op);
        let work = Work::Handoff(HandoffWork::OfferAnswered { op, answer });
        self.after(delay, Item::Proc { node, inc, work });
    }

    /// The donor hears back from a peer.
    pub(super) fn offer_answered(&mut self, node: usize, op: OpId, answer: Answer) {
        let p = self.machines[node].proc_mut();
        let Some(a) = p.adoptions.get_mut(&op) else {
            return;
        };
        match answer {
            Answer::Accepted(adopted) if adopted.epoch > a.released_epoch => {
                p.adoptions.remove(&op);
                self.stats.handed_off += 1;
                let result = Ok(adopted);
                return self.drive(node, Event::SuccessorAdopted { op, result });
            }
            Answer::CurrentOwner(owner) => a.target = Some(owner),
            // A stale acceptance or a failure ends this peer's chain.
            Answer::Accepted(_) | Answer::Failed => {}
        }
        self.offer_next(node, op);
    }
}
