// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Test controls for `LtxRepl`, included into `ltx_repl.rs`: the upload slot
// count, pauses at an activation's install and at a replica's close, and an
// injected panic in a release's close.

static SYNC_CONCURRENCY_FOR_WORLD: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Narrow the upload slots of every `LtxRepl` built after this call, or
/// restore the default with `None`.
pub fn set_sync_concurrency_for_world(slots: Option<usize>) {
    SYNC_CONCURRENCY_FOR_WORLD.store(slots.unwrap_or(0), Ordering::SeqCst);
}

fn sync_concurrency_for_world() -> Option<usize> {
    match SYNC_CONCURRENCY_FOR_WORLD.load(Ordering::SeqCst) {
        0 => None,
        slots => Some(slots),
    }
}

/// An activation held just before it installs its managed handle.
pub struct LtxActivationInstallPause {
    reached: tokio::sync::watch::Sender<bool>,
    released: tokio::sync::watch::Sender<bool>,
}

impl LtxActivationInstallPause {
    fn new() -> Self {
        Self {
            reached: tokio::sync::watch::channel(false).0,
            released: tokio::sync::watch::channel(false).0,
        }
    }

    /// Wait until the activation reaches the pause.
    pub async fn reached(&self) {
        let mut reached = self.reached.subscribe();
        let _ = reached.wait_for(|reached| *reached).await;
    }

    /// Let the activation install.
    pub fn release(&self) {
        self.released.send_replace(true);
    }
}

/// A replica close held on its blocking thread before it closes.
pub struct LtxReplicaClosePause {
    state: Mutex<ClosePauseState>,
    changed: std::sync::Condvar,
}

#[derive(Default)]
struct ClosePauseState {
    reached: bool,
    released: bool,
}

impl LtxReplicaClosePause {
    fn new() -> Self {
        Self {
            state: Mutex::new(ClosePauseState::default()),
            changed: std::sync::Condvar::new(),
        }
    }

    /// Whether the close reached the pause.
    pub fn is_reached(&self) -> bool {
        self.state.lock().unwrap().reached
    }

    /// Let the close run.
    pub fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}

impl Drop for LtxReplicaClosePause {
    fn drop(&mut self) {
        self.changed.notify_all();
    }
}

/// Hold a close at its pause until the test releases it.
fn pause_replica_close_for_world(pause: Option<Arc<LtxReplicaClosePause>>) {
    let Some(pause) = pause else {
        return;
    };
    let mut state = pause.state.lock().unwrap();
    state.reached = true;
    pause.changed.notify_all();
    while !state.released {
        state = pause.changed.wait(state).unwrap();
    }
}

impl LtxRepl {
    /// Hold the next activation before it installs its managed handle.
    pub fn pause_next_activation_install_for_world(&self) -> Arc<LtxActivationInstallPause> {
        let pause = Arc::new(LtxActivationInstallPause::new());
        *self.activation_install_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    async fn pause_activation_install_for_world(&self) {
        let pause = self.activation_install_pause.lock().unwrap().take();
        let Some(pause) = pause else {
            return;
        };
        pause.reached.send_replace(true);
        let mut released = pause.released.subscribe();
        let _ = released.wait_for(|released| *released).await;
    }

    /// Hold the next replica close, by a release or by local shutdown.
    pub fn pause_next_replica_close_for_world(&self) -> Arc<LtxReplicaClosePause> {
        let pause = Arc::new(LtxReplicaClosePause::new());
        *self.close_local_replicas_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    fn take_replica_close_pause_for_world(&self) -> Option<Arc<LtxReplicaClosePause>> {
        self.close_local_replicas_pause.lock().unwrap().take()
    }

    /// Make the next release's close panic on its blocking thread.
    pub fn panic_next_release_close_for_world(&self) {
        self.panic_next_release_close.store(true, Ordering::SeqCst);
    }

    fn take_release_close_panic_for_world(&self) -> bool {
        self.panic_next_release_close.swap(false, Ordering::SeqCst)
    }
}

// ── receipts, for the in-crate suites ───────────────────────────────────────
//
// Each cell handle records what its durability path did, in order: the
// tickets callers took, the per-cell upload rounds, the fleet captures that
// carried the cell's frames, and the fleet credits applied to it. A test
// reads them through `LtxRepl::receipts_for_world`.

/// A durability ticket a caller took at a write position.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtxDurabilityTicketReceiptForWorldV1 {
    pub position: u64,
    pub ticket: u64,
}

/// How a per-cell upload round ended, or `Started` while it runs.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LtxUploadRoundStatusForWorldV1 {
    Started,
    Failed,
    Completed,
}

/// One per-cell upload of the L0 files from `min_txid` to `max_txid`, for
/// the tickets up to `captured`.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtxUploadRoundReceiptForWorldV1 {
    pub captured: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub status: LtxUploadRoundStatusForWorldV1,
}

/// A fleet credit: the tickets and position it released, and the fleet
/// sequence that proved it, `None` for an ordered empty round.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtxFleetCreditReceiptForWorldV1 {
    pub tickets: u64,
    pub position: u64,
    pub proof_last_seq: Option<u64>,
}

/// How a fleet capture's round ended, or `Submitted` while it is in flight.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LtxFleetCaptureStatusForWorld {
    Submitted,
    Failed,
    Applied,
}

/// A fleet round that carried `entries` of this cell's frames at the
/// shipper's `epoch`, crediting `tickets` up to `position`.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtxFleetCaptureReceiptForWorld {
    pub tickets: u64,
    pub position: u64,
    pub entries: usize,
    pub epoch: u64,
    pub status: LtxFleetCaptureStatusForWorld,
}

/// A fleet capture in flight; it settles its receipt when its round does.
#[cfg(test)]
pub struct LtxFleetCaptureForWorld {
    handle: CellHandle,
    ordinal: usize,
}

#[cfg(test)]
impl LtxFleetCaptureForWorld {
    fn finish(&self, status: LtxFleetCaptureStatusForWorld) {
        self.handle.fleet_capture_receipts.lock().unwrap()[self.ordinal].status = status;
    }
}

/// Everything one cell handle recorded.
#[cfg(test)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LtxReceiptsForWorld {
    pub durability_tickets: Vec<LtxDurabilityTicketReceiptForWorldV1>,
    pub upload_rounds: Vec<LtxUploadRoundReceiptForWorldV1>,
    pub fleet_captures: Vec<LtxFleetCaptureReceiptForWorld>,
    pub fleet_credits: Vec<LtxFleetCreditReceiptForWorldV1>,
}

/// A cell's sync held after it credited a per-cell upload.
#[cfg(test)]
pub type SyncCreditPauseForWorld = LtxActivationInstallPause;

/// A compaction held after its fetch, before it applies its output.
#[cfg(test)]
#[derive(Default)]
pub struct CompactionFinishPauseForWorld {
    pub reached: AtomicBool,
    pub release: Notify,
}

#[cfg(test)]
impl Cell {
    fn record_durability_ticket_for_world(&self, position: u64, ticket: u64) {
        self.durability_ticket_receipts
            .lock()
            .unwrap()
            .push(LtxDurabilityTicketReceiptForWorldV1 { position, ticket });
    }

    fn begin_upload_round_for_world(&self, captured: u64, min_txid: u64, max_txid: u64) -> usize {
        let mut rounds = self.upload_round_receipts.lock().unwrap();
        rounds.push(LtxUploadRoundReceiptForWorldV1 {
            captured,
            min_txid,
            max_txid,
            status: LtxUploadRoundStatusForWorldV1::Started,
        });
        rounds.len() - 1
    }

    fn finish_upload_round_for_world(
        &self,
        ordinal: usize,
        status: LtxUploadRoundStatusForWorldV1,
    ) {
        self.upload_round_receipts.lock().unwrap()[ordinal].status = status;
    }

    fn begin_fleet_capture_for_world(
        self: &Arc<Self>,
        tickets: u64,
        position: u64,
        entries: usize,
        epoch: u64,
    ) -> LtxFleetCaptureForWorld {
        let mut captures = self.fleet_capture_receipts.lock().unwrap();
        captures.push(LtxFleetCaptureReceiptForWorld {
            tickets,
            position,
            entries,
            epoch,
            status: LtxFleetCaptureStatusForWorld::Submitted,
        });
        LtxFleetCaptureForWorld {
            handle: self.clone(),
            ordinal: captures.len() - 1,
        }
    }

    fn record_fleet_credit_for_world(
        &self,
        tickets: u64,
        position: u64,
        proof_last_seq: Option<u64>,
    ) {
        self.fleet_credit_receipts
            .lock()
            .unwrap()
            .push(LtxFleetCreditReceiptForWorldV1 {
                tickets,
                position,
                proof_last_seq,
            });
    }

    async fn pause_after_sync_credit_for_world(&self) {
        let pause = self.sync_credit_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.reached.send_replace(true);
            let mut released = pause.released.subscribe();
            let _ = released.wait_for(|released| *released).await;
        }
    }

    fn receipts_for_world(&self) -> LtxReceiptsForWorld {
        LtxReceiptsForWorld {
            durability_tickets: self.durability_ticket_receipts.lock().unwrap().clone(),
            upload_rounds: self.upload_round_receipts.lock().unwrap().clone(),
            fleet_captures: self.fleet_capture_receipts.lock().unwrap().clone(),
            fleet_credits: self.fleet_credit_receipts.lock().unwrap().clone(),
        }
    }
}

#[cfg(test)]
impl LtxRepl {
    fn cell_for_world(&self, cell: &str, epoch: u64) -> Option<CellHandle> {
        self.cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
    }

    /// What the resident handle of `cell` at `epoch` recorded so far.
    pub fn receipts_for_world(&self, cell: &str, epoch: u64) -> Option<LtxReceiptsForWorld> {
        self.cell_for_world(cell, epoch)
            .map(|handle| handle.receipts_for_world())
    }

    /// Hold the cell's next sync after it credits a per-cell upload.
    pub fn pause_next_sync_credit_for_world(
        &self,
        cell: &str,
        epoch: u64,
    ) -> Option<Arc<SyncCreditPauseForWorld>> {
        let handle = self.cell_for_world(cell, epoch)?;
        let pause = Arc::new(LtxActivationInstallPause::new());
        *handle.sync_credit_pause.lock().unwrap() = Some(pause.clone());
        Some(pause)
    }

    /// Hold the cell's next compaction before it applies its output.
    pub fn pause_next_compaction_finish_for_world(
        &self,
        cell: &str,
        epoch: u64,
    ) -> Option<Arc<CompactionFinishPauseForWorld>> {
        let handle = self.cell_for_world(cell, epoch)?;
        let compaction = handle.compaction.as_ref()?;
        let pause = Arc::new(CompactionFinishPauseForWorld::default());
        *compaction.finish_pause.lock().unwrap() = Some(pause.clone());
        Some(pause)
    }
}
