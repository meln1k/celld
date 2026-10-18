//! Which core inputs and outputs a run produced.
//!
//! A checker that never sees the situation it guards proves nothing, and a
//! core path the shell never drives is a path no checker sees. Every
//! [`Event`] the cores received and every [`Effect`] they emitted is counted
//! by variant. The variant lists below are one macro invocation with the
//! match that names them, so a variant added to the core fails to compile
//! here until it is listed, and the sweep's coverage test then demands that
//! the swarm reach it.

use celld_logic::{Effect, Event};
use std::collections::BTreeMap;

macro_rules! variants {
    ($ty:ident, $name:ident, $all:ident, [$($variant:ident),* $(,)?]) => {
        /// Every variant, by name.
        pub const $all: &[&str] = &[$(stringify!($variant)),*];

        fn $name(value: &$ty) -> &'static str {
            match value {
                $($ty::$variant { .. } => stringify!($variant),)*
            }
        }
    };
}

variants!(
    Event,
    event_name,
    EVENTS,
    [
        StartNodeLease,
        SelfNodeLeaseRead,
        NodeLeaseCasCompleted,
        LocalCellsRead,
        TimerFired,
        Request,
        RequestAt,
        CapacityRequestAt,
        HandoffRequestAt,
        WebSocketRequestAt,
        GateRequestAt,
        WorkerRequest,
        BeginPreserve,
        Cancel,
        ActivityFinished,
        Output,
        OutputAt,
        DurableReached,
        OwnershipVerified,
        WebSocketOpened,
        WebSocketClosed,
        AlarmObserved,
        AlarmFinished,
        WakeHint,
        WakeHintAt,
        OwnerRead,
        NodeLogRecovered,
        NudgeNodeLease,
        NodeLeaseRead,
        CapacityPeersRead,
        OwnerCasCompleted,
        OwnerReleased,
        SuccessorAdopted,
        RestoreCompleted,
        RuntimeStarted,
        GenerationChanged,
        Published,
        DurabilityChecked,
        RuntimeStopped,
        RuntimeStopFailed,
        RuntimeStopCancelled,
        Evict,
        EvictRequested,
        LoadSampled,
        InvalidateRemote,
        NodeFenced,
        ReleaseAll,
        Rebalance,
    ]
);

variants!(
    Effect,
    effect_name,
    EFFECTS,
    [
        EvictionAdmission,
        EvictionFinished,
        ScheduleTimer,
        ReadSelfNodeLease,
        CasNodeLease,
        ReadLocalCells,
        ReadOwner,
        ReadNodeLease,
        RecoverNodeLog,
        ReadCapacityPeers,
        CasOwner,
        ReconcileWakeEntry,
        ReleaseOwner,
        AdoptReleased,
        CancelCellActivity,
        Restore,
        StartRuntime,
        Publish,
        EnsureDurable,
        AwaitDurable,
        VerifyOwnership,
        StopRuntime,
        AbandonEvictionStop,
        FireAlarm,
        Complete,
        Release,
        CompleteWorker,
        CloseWebSocket,
        Halt,
    ]
);

/// The untimed forms of production events. The production shell sends only
/// the timed variant beside each, so the simulator does too; the core
/// handles both through one path.
pub const UNTIMED_EVENTS: &[&str] = &["Request", "Output", "WakeHint"];

/// How many times each core input and output occurred.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub events: BTreeMap<&'static str, u64>,
    pub effects: BTreeMap<&'static str, u64>,
}

impl Coverage {
    pub(crate) fn event(&mut self, event: &Event) {
        *self.events.entry(event_name(event)).or_default() += 1;
    }

    pub(crate) fn effect(&mut self, effect: &Effect) {
        *self.effects.entry(effect_name(effect)).or_default() += 1;
    }

    /// Add another run's counts to these.
    pub fn absorb(&mut self, other: &Coverage) {
        for (name, count) in &other.events {
            *self.events.entry(name).or_default() += count;
        }
        for (name, count) in &other.effects {
            *self.effects.entry(name).or_default() += count;
        }
    }

    /// Every variant the production shell can produce that never occurred:
    /// events first, then effects.
    pub fn missing(&self) -> Vec<&'static str> {
        let events = EVENTS
            .iter()
            .filter(|name| !UNTIMED_EVENTS.contains(name))
            .filter(|name| !self.events.contains_key(*name));
        let effects = EFFECTS
            .iter()
            .filter(|name| !self.effects.contains_key(*name));
        events.chain(effects).copied().collect()
    }
}
