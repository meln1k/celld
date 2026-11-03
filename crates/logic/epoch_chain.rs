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
//! (`continues_from`), and the walk links only that epoch below it: it
//! passes over the epochs between without listing them, and stops when it
//! has passed the named one, since the chain is then broken. A marker
//! written before markers named their source names none and keeps the old
//! rule.
//!
//! The listing and the marker's read are I/O, so the caller does them:
//! `celld_ltx`'s `EpochChain` over the bucket, and the simulator over its
//! own. Both walk with this type, so the chain the simulator checks is the
//! chain production builds. Per epoch, newest first:
//!
//! 1. `wants(epoch)`: list it, pass over it, or stop.
//! 2. `visit(epoch, objects)` with every listed object's txid range.
//! 3. On a `Link` with `marker`, read the marker and, if it names its
//!    source, `continues_from(source)`.

pub type Epoch = u64;
pub type Txid = u64;

/// Whether the walk needs the next epoch down listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wants {
    List,
    /// The last linked epoch's marker named an older epoch than this one.
    Pass,
    /// It named a newer one, which the walk passed without linking: no
    /// epoch below continues the chain, so it ends without a base.
    Done,
}

/// The walk's verdict on one listed epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Not part of the chain: nothing listed below the cut, or what is
    /// listed does not end at it. Its marker, if it has one, is not read:
    /// the cut and any source named above stand, so skipping the epoch a
    /// marker named ends the walk (`wants` answers `Done` next).
    Skip,
    /// Part of the chain, serving `lo` through `hi`: the caller keeps the
    /// objects that end at or before `hi`. `base` when it opens at txid 1,
    /// and the walk is over. `marker` when it continues the chain from an
    /// object of its own at `lo` alone, a paged activation's marker, which
    /// may name its source.
    Link {
        lo: Txid,
        hi: Txid,
        base: bool,
        marker: bool,
    },
}

/// The walk down one cell's epochs, newest first.
#[derive(Debug, Clone, Default)]
pub struct Walk {
    /// The first txid the last linked epoch serves.
    above: Option<Txid>,
    /// The epoch the last linked epoch continued, when its marker names
    /// it: the only epoch the walk may link next.
    source: Option<Epoch>,
    /// The last linked epoch has a marker that `continues_from` may read.
    marker: bool,
}

impl Walk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether to list `epoch`, the next one down.
    pub fn wants(&self, epoch: Epoch) -> Wants {
        match self.source {
            Some(source) if epoch > source => Wants::Pass,
            Some(source) if epoch < source => Wants::Done,
            _ => Wants::List,
        }
    }

    /// Decide on `epoch`, given the txid range `(min, max)` of each of its
    /// listed objects. The walk keeps those that end before the first txid
    /// the epoch above serves, and links the epoch when what it kept ends
    /// exactly at that cut.
    pub fn visit(&mut self, epoch: Epoch, files: impl IntoIterator<Item = (Txid, Txid)>) -> Step {
        debug_assert_eq!(self.wants(epoch), Wants::List, "epoch {epoch} not wanted");
        let mut span: Option<(Txid, Txid)> = None;
        let mut singles = Vec::new();
        for (min, max) in files {
            if self.above.is_some_and(|above| max >= above) {
                continue;
            }
            if min == max {
                singles.push(min);
            }
            span = Some(span.map_or((min, max), |(lo, hi)| (lo.min(min), hi.max(max))));
        }
        self.marker = false;
        let Some((lo, hi)) = span else {
            return Step::Skip;
        };
        if self
            .above
            .is_some_and(|above| hi.checked_add(1) != Some(above))
        {
            return Step::Skip;
        }
        let base = lo == 1;
        let marker = !base && singles.contains(&lo);
        self.above = Some(lo);
        self.source = None;
        self.marker = marker;
        Step::Link {
            lo,
            hi,
            base,
            marker,
        }
    }

    /// The epoch just linked continued `source`, as its marker names it.
    /// Only after a `Link` with `marker`.
    pub fn continues_from(&mut self, source: Epoch) {
        debug_assert!(self.marker, "continues_from without a marker");
        self.marker = false;
        self.source = Some(source);
    }
}
