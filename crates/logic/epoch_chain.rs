// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Which epochs a restore chain links, decided sans-IO.
//!
//! A paged activation continues the chain it paged in: its first object
//! follows the cut's last txid instead of opening the epoch with a
//! whole-database snapshot. The objects a restore needs can therefore span
//! epochs. The walk goes down from the newest epoch until one that opens at
//! txid 1. It clips each lower epoch to the txids before the first one the
//! epoch above serves (a fold that landed after the cut was never part of
//! it), and links that epoch only when what remains ends exactly at the cut.
//! An epoch below that ends elsewhere is skipped: a fenced owner's late
//! snapshot can land in an intermediate epoch, whole but older than the
//! cut, and taking it as the base loses the writes between it and the cut
//! (CelldPersistencePaged.tla, `BreakLinkByOrder`).
//!
//! Ending at the cut is not enough either. A fenced owner's late snapshot
//! can also end exactly at the cut, in an epoch that was empty when the
//! continuation restored and so was never its source: the same length,
//! other writes. Linking it opens a database no owner ever held (swarm
//! 240955). So a continuation's marker names the epoch it continued
//! (`continues_from`), and the walk links only that epoch below it. A
//! marker that names none, written before markers did, keeps the old rule.
//!
//! The listing is I/O, so the caller does it: `celld_ltx`'s `EpochChain`
//! over the bucket, and the simulator over its own. Both walk with this
//! type, so the chain the simulator checks is the chain production builds.

pub type Epoch = u64;
pub type Txid = u64;

/// One epoch's place in the chain, the walk's verdict on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Not part of the chain: nothing listed below the cut, or what is
    /// listed does not end at it.
    Skip,
    /// Part of the chain, serving `lo` through `hi`. `base` when it opens
    /// at txid 1: the walk is over.
    Link { lo: Txid, hi: Txid, base: bool },
}

/// The walk down one cell's epochs, newest first.
#[derive(Debug, Clone, Default)]
pub struct Walk {
    /// The first txid the newest linked epoch serves.
    above: Option<Txid>,
    /// The epoch the newest linked epoch continued, when its marker names
    /// it: the only epoch the walk may link next.
    source: Option<Epoch>,
}

impl Walk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether an object ending at `max_txid`, in the epoch visited next,
    /// belongs to the chain: it ends before the first txid the epoch above
    /// serves.
    pub fn keeps(&self, max_txid: Txid) -> bool {
        self.above.is_none_or(|lo| max_txid < lo)
    }

    /// The epoch just linked continued `source`, as its marker names it.
    pub fn continues_from(&mut self, source: Epoch) {
        self.source = Some(source);
    }

    /// Decide on `epoch`, the next one down, given the txid range
    /// `(min, max)` of each of its objects. The walk clips them (`keeps`)
    /// and links the epoch when what remains ends exactly at the cut. An
    /// object may end one short of where it starts: an opener that takes no
    /// txid.
    pub fn visit(&mut self, epoch: Epoch, files: impl IntoIterator<Item = (Txid, Txid)>) -> Step {
        let kept = files.into_iter().filter(|(_, max)| self.keeps(*max));
        let span = kept.fold(None, |span: Option<(Txid, Txid)>, (min, max)| {
            Some(span.map_or((min, max), |(lo, hi)| (lo.min(min), hi.max(max))))
        });
        let Some((lo, hi)) = span else {
            return Step::Skip;
        };
        if self.source.is_some_and(|source| source != epoch) {
            return Step::Skip;
        }
        if self
            .above
            .is_some_and(|above| hi.checked_add(1) != Some(above))
        {
            return Step::Skip;
        }
        self.above = Some(lo);
        self.source = None;
        Step::Link {
            lo,
            hi,
            base: lo == 1,
        }
    }
}
