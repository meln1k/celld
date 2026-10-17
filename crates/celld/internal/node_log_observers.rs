// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Test pauses for the node log manager, included into `node_log.rs`: the
// graceful close's final transition, and a maintenance pass before its
// remote publish and after it installs the opened record.

/// A transition held at its pause until the test releases it.
pub(crate) struct NodeLogTransitionPause {
    reached: tokio::sync::watch::Sender<bool>,
    released: tokio::sync::watch::Sender<bool>,
}

impl NodeLogTransitionPause {
    fn new() -> Self {
        Self {
            reached: tokio::sync::watch::channel(false).0,
            released: tokio::sync::watch::channel(false).0,
        }
    }

    /// Wait until the transition reaches the pause.
    pub(crate) async fn reached(&self) {
        let mut reached = self.reached.subscribe();
        let _ = reached.wait_for(|reached| *reached).await;
    }

    /// Let the transition continue.
    pub(crate) fn release(&self) {
        self.released.send_replace(true);
    }

    async fn hold(&self) {
        self.reached.send_replace(true);
        let mut released = self.released.subscribe();
        let _ = released.wait_for(|released| *released).await;
    }
}

async fn hold_node_log_pause(slot: &Mutex<Option<Arc<NodeLogTransitionPause>>>) {
    let pause = slot.lock().unwrap().take();
    if let Some(pause) = pause {
        pause.hold().await;
    }
}

fn arm_node_log_pause(
    slot: &Mutex<Option<Arc<NodeLogTransitionPause>>>,
) -> Arc<NodeLogTransitionPause> {
    let pause = Arc::new(NodeLogTransitionPause::new());
    *slot.lock().unwrap() = Some(pause.clone());
    pause
}

impl NodeLogManager {
    /// Hold the graceful close before its final transition.
    pub(crate) fn pause_next_close_transition_for_world(&self) -> Arc<NodeLogTransitionPause> {
        arm_node_log_pause(&self.close_transition_pause)
    }

    /// Hold the next maintenance pass just before its remote publish.
    pub(crate) fn pause_next_maintenance_publish_for_world(&self) -> Arc<NodeLogTransitionPause> {
        arm_node_log_pause(&self.maintenance_publish_pause)
    }

    /// Hold the next maintenance pass after it installs the opened record.
    pub(crate) fn pause_next_maintenance_install_for_world(&self) -> Arc<NodeLogTransitionPause> {
        arm_node_log_pause(&self.maintenance_install_pause)
    }

    async fn pause_close_transition_for_world(&self) {
        hold_node_log_pause(&self.close_transition_pause).await;
    }

    async fn pause_maintenance_publish_for_world(&self) {
        hold_node_log_pause(&self.maintenance_publish_pause).await;
    }

    async fn pause_maintenance_install_for_world(&self) {
        hold_node_log_pause(&self.maintenance_install_pause).await;
    }
}
