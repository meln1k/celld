// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Test observers for the actor, included into `actor.rs`.
//
// `ActorState` is the core's `State` with an optional observer: every
// event the actor applies, and the effects it produced, go to the observer
// after the core takes them. `HandoffTransportForTest` replaces the peer
// HTTP client for handoff requests, so a test can answer as the peer would
// without a socket.

/// The core state, observed.
pub(crate) struct ActorState {
    state: State,
    observer: Option<CoreObserver>,
}

/// Receives each applied event and the effects the core returned for it.
pub(crate) type CoreObserver = Arc<dyn Fn(&Event, &[Effect]) + Send + Sync>;

impl From<State> for ActorState {
    fn from(state: State) -> Self {
        Self {
            state,
            observer: None,
        }
    }
}

impl std::ops::Deref for ActorState {
    type Target = State;

    fn deref(&self) -> &State {
        &self.state
    }
}

impl std::ops::DerefMut for ActorState {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.state
    }
}

impl ActorState {
    /// Observe every later event, replacing any earlier observer.
    pub(crate) fn observe(&mut self, observer: CoreObserver) {
        self.observer = Some(observer);
    }
}

fn apply_core_event(state: &mut ActorState, event: Event) -> Vec<Effect> {
    let Some(observer) = state.observer.clone() else {
        return on_event(&mut state.state, event);
    };
    let observed = event.clone();
    let effects = on_event(&mut state.state, event);
    observer(&observed, &effects);
    effects
}

impl Actor {
    /// Observe every event this actor applies from now on.
    pub(crate) fn observe_core_for_test(&mut self, observer: CoreObserver) {
        self.state.observe(observer);
    }

    /// The pins this actor shares with the request shell, for a world that
    /// builds the shell's `AppHandle` itself.
    pub(crate) fn drain_pins_for_test(&self) -> DrainPinRegistry {
        self.drain_pins.clone()
    }
}

/// Carries a signed peer handoff request in place of the peer HTTP client.
/// The response goes through the donor's own decoding, so a transport can
/// only answer as a peer could.
#[async_trait::async_trait]
pub(crate) trait HandoffTransportForTest: Send + Sync {
    async fn send(&self, request: reqwest::Request) -> anyhow::Result<reqwest::Response>;
}
