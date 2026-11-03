//! Code probes: rare branches of the core, named and counted when taken.
//!
//! A probe decides nothing. It records that a branch ran, so a simulator or
//! a fleet can tell which rare paths its runs reached and which none did: a
//! fix whose seeds stop failing proves little if no seed reaches the path it
//! changed. Each probe marks one branch; the list below is the whole set,
//! and [`Probe::ALL`] names every one, so a report can list those never hit.

macro_rules! probes {
    ($($(#[doc = $doc:literal])* $name:ident,)*) => {
        /// One probed branch of the core.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Probe {
            $($(#[doc = $doc])* $name,)*
        }

        impl Probe {
            /// Every probe, in declaration order.
            pub const ALL: &'static [Probe] = &[$(Probe::$name,)*];
            /// How many probes there are.
            pub const COUNT: usize = Self::ALL.len();

            /// The probe's name, as declared.
            pub fn name(self) -> &'static str {
                match self {
                    $(Probe::$name => stringify!($name),)*
                }
            }
        }
    };
}

probes! {
    // The output gate.
    /// An output was refused because the node held no lease.
    GateRefusedUnauthoritative,
    /// An output sampled at an epoch its cell no longer runs was refused.
    GateRefusedEpochMoved,
    /// A read that observed an unproven write waits for a proof of it.
    GateReadHeldOnObserved,
    /// A fleet proof released a gate with no ownership read.
    GateProvenByFleet,
    /// A bucket proof was confirmed by an ownership read.
    GateProvenByBucket,
    /// A proof covered less than the position it was asked to prove.
    GateProofShort,
    /// A proof failed outright.
    GateProofFailed,
    /// An ownership read refused a bucket proof.
    GateOwnershipRefused,
    /// A gate outlived the operation deadline.
    GateDeadline,

    // Resets: a failed proof stops the runtime.
    /// A reset stops a cell that holds a WebSocket.
    ResetWithWebSocket,
    /// A reset stops a cell while its alarm handler is firing.
    ResetWhileAlarmFiring,
    /// A reset fails an alarm handler's commit still at the gate.
    ResetFailsAlarmCommit,

    // Alarms.
    /// A handler's commit finished after its cell left the epoch it fired at.
    AlarmFinishedSuperseded,
    /// A handler failed, or its commit was not proven: the alarm re-arms.
    AlarmFinishedFailed,
    /// A handler settled on an observation that arrived while it ran.
    AlarmSettledOnStagedObservation,
    /// An observation arrived while the alarm was firing and was staged.
    AlarmObservationStaged,
    /// An observation from a runtime being reset was ignored.
    AlarmObservationDuringReset,
    /// An observation older than one already taken was ignored.
    AlarmObservationStale,
    /// An alarm change cancelled the cell's eviction.
    AlarmCancelledEviction,
    /// A restore reported no alarm while the core held one armed, so the
    /// armed one stays.
    RestoreKeepsArmedAlarm,
    /// An alarm timer woke a cell that was not resident here.
    AlarmTimerWokeCell,
    /// An alarm timer on a quiescing cell of a draining node was put off.
    AlarmTimerDeferredForDrain,
    /// An alarm timer on a fenced cell was dropped.
    AlarmTimerDroppedFenced,
    /// An alarm timer on a cell a draining node gave up was dropped.
    AlarmTimerDroppedDraining,
    /// An alarm timer fired while the cell was between phases, and retried.
    AlarmTimerRetried,
    /// A firing alarm cancelled the cell's eviction.
    AlarmFireCancelledEviction,
    /// A firing alarm outlived the operation deadline.
    AlarmDeadline,

    // Activation.
    /// A restore failed: the cell parks dormant.
    RestoreFailed,
    /// A restore failed for a cell a clean reload was resuming.
    ResumeFailed,
    /// A clean reload found its cell's record no longer naming this node at
    /// its database's epoch: a peer took it while the process restarted.
    ResumeSuperseded,
    /// A runtime start outlived its deadline.
    StartTimedOut,
    /// A publish failed.
    PublishFailed,
    /// A dead owner's log recovery failed, and the activation backs off.
    LogRecoveryFailed,
    /// A dead owner's log recovery failed too often, and the requests fail.
    LogRecoveryGaveUp,
    /// An activation's owner read outlived its deadline.
    OwnerReadDeadline,
    /// A node lease read outlived its deadline.
    NodeLeaseReadDeadline,
    /// A log recovery outlived its deadline.
    LogRecoveryDeadline,
    /// A capacity read outlived its deadline.
    CapacityReadDeadline,
    /// An ownership claim outlived its deadline.
    ClaimDeadline,
    /// The read that settles an ambiguous claim outlived its deadline.
    ReconcileDeadline,

    // Stops and handoffs.
    /// A swap stop ended on a draining node: the cell starts again at the
    /// same epoch, for the drain to prove and evict.
    SwapStopWhileDraining,
    /// A swap's restart failed: the runtime is gone, and its database is
    /// still open at the epoch the swap stopped without a proof, so a
    /// cleanup stop proves and closes it.
    SwapRestartFailed,
    /// A request arrived mid-eviction, so the cell activates again.
    EvictionTurnedAround,
    /// An eviction made for room released the cell's owner record.
    EvictionReleasedOwner,
    /// An eviction's durability proof failed, and the cell stays.
    EvictionRefused,
    /// An eviction's durability proof outlived its deadline.
    EvictionDeadline,
    /// A bounded stop gave up, and the runtime restarts in place.
    StopFailedRestarts,
    /// A dormant cell's owner record was released to hand it off.
    DormantReleased,
    /// A dormant cell with WebSockets parked on it was released.
    DormantReleasedWithSockets,
    /// A drain or the balancer kept a dormant cell whose tail no restore
    /// here has folded, rather than release a record a peer would restore
    /// without it.
    DormantKeptUnfolded,
    /// Of those, a cell whose claim here landed with no restore, so the
    /// tail an earlier epoch left in this node's log stays unfolded.
    DormantKeptUnrestored,
    /// An owner record release outlived its deadline.
    ReleaseDeadline,
    /// A successor's adoption outlived its deadline.
    AdoptDeadline,

    // The node lease.
    /// The first lease write failed, and the core retries it.
    InitialLeaseFailed,
    /// The node fenced itself and halts.
    NodeFenced,
}
