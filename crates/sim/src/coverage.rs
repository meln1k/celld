//! Which of the core's cell phases a run reached, and which transitions
//! between them it took.
//!
//! A check that never sees a phase cannot judge what happens there, so a
//! sweep reports what its seeds covered. The world reads every cell's phase
//! before and after each event, so a transition here is one event's net
//! change: a phase the core enters and leaves within one event is not seen.

use std::collections::BTreeMap;

use celld_logic::{Phase, StopCause};

/// How many times a run took each transition, `(from, to)`.
pub type Coverage = BTreeMap<(&'static str, &'static str), u64>;

/// Every phase name `name` can return, in the order `Phase` declares them.
pub const PHASES: [&str; 21] = [
    "inactive",
    "waiting_activation",
    "reading_owner",
    "reading_node_lease",
    "recovering_owner_log",
    "waiting_owner_log_recovery",
    "reading_capacity",
    "waiting_capacity",
    "acquiring",
    "reconciling_acquire",
    "restoring",
    "starting",
    "publishing",
    "ensuring_durability",
    "swapping",
    "cleaning",
    "dormant",
    "adopting",
    "resident",
    "remote",
    "fenced",
];

/// The core's operator name for a phase (`phase_census`), which the core
/// does not export. The match is exhaustive, so a new phase stops the build
/// until it is named here. A cell the core has never seen is `inactive`.
pub fn name(phase: Option<&Phase>) -> &'static str {
    let Some(phase) = phase else {
        return "inactive";
    };
    match phase {
        Phase::Inactive => "inactive",
        Phase::WaitingActivation => "waiting_activation",
        Phase::ReadingOwner { .. } => "reading_owner",
        Phase::ReadingNodeLease { .. } => "reading_node_lease",
        Phase::RecoveringOwnerLog { .. } => "recovering_owner_log",
        Phase::WaitingOwnerLogRecovery { .. } => "waiting_owner_log_recovery",
        Phase::ReadingCapacity { .. } => "reading_capacity",
        Phase::WaitingCapacity => "waiting_capacity",
        Phase::Acquiring { .. } => "acquiring",
        Phase::ReconcilingAcquire { .. } => "reconciling_acquire",
        Phase::Restoring { .. } => "restoring",
        Phase::Starting { .. } => "starting",
        Phase::Publishing { .. } => "publishing",
        Phase::EnsuringDurability { .. } => "ensuring_durability",
        Phase::Cleaning {
            cause: StopCause::Swap,
            ..
        } => "swapping",
        Phase::Cleaning { .. } => "cleaning",
        Phase::Dormant { .. } => "dormant",
        Phase::Adopting { .. } => "adopting",
        Phase::Resident { .. } => "resident",
        Phase::Remote { .. } => "remote",
        Phase::Fenced => "fenced",
    }
}
