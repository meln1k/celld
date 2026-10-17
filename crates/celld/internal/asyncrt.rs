// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The simulated execution facade, `crate::asyncrt` under
// `--cfg celld_internal_tests`.
//
// It has the production facade's surface (`crates/celld/asyncrt.rs`) and
// answers every call from the simulated domain of the code running now:
// tasks run on the deterministic executor in `asyncrt/executor.rs`, clocks
// are virtual, every random stream is seeded by label, and each process has
// its own host services and counters. Calling it outside a domain panics,
// because there is no ambient world to fall back on. Worlds build domains
// through `asyncrt::sim`.

#[path = "asyncrt/executor.rs"]
mod executor;

/// The world-facing API: simulations, domains and their controls.
pub mod sim {
    pub use super::executor::{Domain, DomainOptions, Simulation, WALL_EPOCH_MS};

    /// The name of the domain running now, or `None` outside every domain.
    pub fn current_domain() -> Option<String> {
        super::executor::current().map(|(_, domain)| domain.name.clone())
    }

    /// The simulation's own wall clock, which no domain offset skews: the
    /// time a shared service such as the bucket stamps on what it stores.
    pub fn world_wall_ms() -> i64 {
        let (sim, _) = super::executor::current_or_panic("sim::world_wall_ms");
        WALL_EPOCH_MS.saturating_add(super::executor::now_ms(&sim) as i64)
    }
}

use crate::host_services::HostServices;
use executor::{current_or_panic, DomainInner, JoinSlot, Timer};
use std::cell::RefCell;
use std::fmt;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

thread_local! {
    static SPAWNS: RefCell<Vec<Spawn>> = const { RefCell::new(Vec::new()) };
}

/// A cloneable capability for scheduling work on one simulated domain.
#[derive(Clone)]
pub(crate) struct DomainToken {
    inner: Weak<DomainInner>,
}

/// The execution domain no longer accepts work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DomainClosed;

impl fmt::Display for DomainClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("execution domain is closed")
    }
}

impl std::error::Error for DomainClosed {}

impl DomainToken {
    fn new(inner: &Arc<DomainInner>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
        }
    }

    fn owner(&self) -> Result<(Arc<executor::SimInner>, Arc<DomainInner>), DomainClosed> {
        let domain = self.inner.upgrade().ok_or(DomainClosed)?;
        if domain.is_closed() {
            return Err(DomainClosed);
        }
        let sim = domain.sim.upgrade().ok_or(DomainClosed)?;
        Ok((sim, domain))
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn mono_ms(&self) -> Result<u64, DomainClosed> {
        let (sim, domain) = self.owner()?;
        Ok(domain.mono_ms(&sim))
    }

    pub(crate) fn sleep_until(&self, deadline_ms: u64) -> Result<Sleep, DomainClosed> {
        let (sim, domain) = self.owner()?;
        Ok(Box::pin(Timer::new(
            &sim,
            domain.started_at_ms.saturating_add(deadline_ms),
        )))
    }

    #[track_caller]
    pub(crate) fn spawn_detached<T, F>(
        &self,
        root: impl Into<String>,
        future: F,
    ) -> Result<TaskHandle<T>, DomainClosed>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let (sim, domain) = self.owner()?;
        let (handle, task) = executor::joinable(future);
        executor::spawn_task(
            &sim,
            &domain,
            Some(root.into()),
            std::panic::Location::caller(),
            task,
        );
        Ok(handle)
    }
}

/// A normalized task panic returned by a typed task handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskPanic {
    pub message: String,
}

impl fmt::Display for TaskPanic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TaskPanic {}

/// A typed join handle for one simulated task. Dropping it detaches the
/// task; a crash of its domain settles it as cancelled.
pub struct TaskHandle<T> {
    slot: Arc<Mutex<JoinSlot<T>>>,
}

impl<T> Unpin for TaskHandle<T> {}

impl<T> TaskHandle<T> {
    /// Detach the task from its join handle.
    pub fn detach(self) {}
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, TaskPanic>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slot = self.slot.lock().unwrap();
        match slot.output.take() {
            Some(output) => Poll::Ready(output),
            None => {
                slot.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// The simulated runtime has no host runtime to install.
pub fn set_host_handle(_handle: tokio::runtime::Handle) {
    panic!("asyncrt::set_host_handle: a simulated build runs on asyncrt::sim domains");
}

/// The simulated runtime runs no V8 arm.
pub fn op_handle() -> tokio::runtime::Handle {
    panic!("asyncrt::op_handle: a simulated build runs no engine");
}

#[track_caller]
pub fn spawn<T, F>(future: F) -> TaskHandle<T>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let (sim, domain) = current_or_panic("spawn");
    let (handle, task) = executor::joinable(future);
    executor::spawn_task(&sim, &domain, None, std::panic::Location::caller(), task);
    handle
}

/// Run `operation` as a task of its own. It runs on the simulation thread
/// when the task is first polled, so it must not wait for other tasks.
#[track_caller]
pub fn blocking<T, F>(operation: F) -> TaskHandle<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    spawn(async move { operation() })
}

/// Nested blocking would stall the only thread every task runs on.
pub fn block_on<F>(_future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    panic!("asyncrt::block_on: drive the simulation with Simulation::block_on instead");
}

/// The node filesystem of the current domain.
pub fn fs() -> Arc<dyn celld_ltx::FileSystem> {
    current_or_panic("fs").1.filesystem.clone()
}

/// Return the maximum component length for the filesystem at `path`.
pub fn filesystem_name_max(path: &std::path::Path) -> std::io::Result<Option<i64>> {
    let directory = std::fs::File::open(path)?;
    nix::unistd::fpathconf(&directory, nix::unistd::PathconfVar::NAME_MAX).map_err(Into::into)
}

/// The current domain's wall clock: the simulation's, plus the domain's
/// offset.
pub fn wall_ms() -> i64 {
    let (sim, domain) = current_or_panic("wall_ms");
    executor::WALL_EPOCH_MS
        .saturating_add(executor::now_ms(&sim) as i64)
        .saturating_add(domain.wall_offset_ms.get())
}

pub fn mono_ms() -> u64 {
    let (sim, domain) = current_or_panic("mono_ms");
    domain.mono_ms(&sim)
}

/// Monotonic time in microseconds. Virtual time has millisecond grain.
pub fn mono_us() -> u64 {
    mono_ms().saturating_mul(1_000)
}

/// A random stream for `consumer`, seeded by the simulation seed, the
/// domain and how many streams the consumer drew before.
pub fn rng(consumer: &'static str) -> rng::Stream {
    let (_, domain) = current_or_panic("rng");
    let draw = {
        let mut streams = domain.rng_streams.lock().unwrap();
        let draw = streams.entry(consumer).or_insert(0);
        *draw += 1;
        *draw
    };
    rng::Stream::seeded(executor::derive_seed(
        domain.seed,
        &[consumer.as_bytes(), &draw.to_le_bytes()],
    ))
}

/// The first branch index for one unbiased select poll, drawn from the
/// schedule stream.
pub fn select_start(branches: u32) -> u32 {
    debug_assert!(branches > 0);
    let (sim, _) = current_or_panic("select_start");
    executor::schedule_draw(&sim, branches)
}

pub fn process_tag() -> u64 {
    current_or_panic("process_tag").1.process_tag
}

pub fn next_core_request() -> u64 {
    current_or_panic("next_core_request")
        .1
        .next_core_request
        .fetch_add(1, Ordering::Relaxed)
}

pub fn services() -> Arc<HostServices> {
    current_or_panic("services").1.services.clone()
}

pub(crate) fn runtime_services() -> Arc<HostServices> {
    services()
}

pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Sleep until the current domain's monotonic clock reads `deadline_ms`.
pub fn sleep_until(deadline_ms: u64) -> Sleep {
    let (sim, domain) = current_or_panic("sleep_until");
    Box::pin(Timer::new(
        &sim,
        domain.started_at_ms.saturating_add(deadline_ms),
    ))
}

pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(mono_ms().saturating_add(duration.as_millis() as u64))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

pub async fn timeout_at<F>(deadline_ms: u64, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut deadline = sleep_until(deadline_ms);
    poll_fn(move |context| {
        if let Poll::Ready(output) = future.as_mut().poll(context) {
            return Poll::Ready(Ok(output));
        }
        if deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Elapsed));
        }
        Poll::Pending
    })
    .await
}

pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    timeout_at(
        mono_ms().saturating_add(duration.as_millis() as u64),
        future,
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissedTickBehavior {
    Burst,
    Delay,
    Skip,
}

pub struct Interval {
    next_ms: u64,
    period_ms: u64,
    behavior: MissedTickBehavior,
}

impl Interval {
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    pub async fn tick(&mut self) {
        sleep_until(self.next_ms).await;
        let now = mono_ms();
        self.next_ms = match self.behavior {
            MissedTickBehavior::Burst => self.next_ms.saturating_add(self.period_ms),
            MissedTickBehavior::Delay => now.saturating_add(self.period_ms),
            MissedTickBehavior::Skip => {
                let missed = now.saturating_sub(self.next_ms) / self.period_ms.max(1);
                self.next_ms
                    .saturating_add(missed.saturating_add(1).saturating_mul(self.period_ms))
            }
        };
    }
}

pub fn interval(period: Duration) -> Interval {
    Interval {
        next_ms: mono_ms(),
        period_ms: period.as_millis().max(1) as u64,
        behavior: MissedTickBehavior::Burst,
    }
}

pub fn interval_at(start_ms: u64, period: Duration) -> Interval {
    Interval {
        next_ms: start_ms,
        period_ms: period.as_millis().max(1) as u64,
        behavior: MissedTickBehavior::Burst,
    }
}

// ── JavaScript operations, as in production ────────────────────────────────

/// The output of one asynchronous JavaScript operation.
pub enum OpOut {
    Str(String),
    Bytes(Vec<u8>),
}

impl From<String> for OpOut {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<Vec<u8>> for OpOut {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

pub type OpFuture = Pin<Box<dyn Future<Output = Result<OpOut, String>> + Send>>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OpLifetime {
    Handler,
    IoContext,
    Unrefed,
}

struct Spawn {
    id: u64,
    future: OpFuture,
    lifetime: OpLifetime,
    owner: Option<u64>,
}

fn enqueue_with_lifetime<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
    lifetime: OpLifetime,
) -> u64 {
    let id = current_or_panic("enqueue")
        .1
        .next_async_op
        .fetch_add(1, Ordering::Relaxed);
    let future: OpFuture = Box::pin(async move { future.await.map(Into::into) });
    SPAWNS.with(|spawns| {
        spawns.borrow_mut().push(Spawn {
            id,
            future,
            lifetime,
            owner: None,
        })
    });
    id
}

pub fn spawn_count() -> usize {
    SPAWNS.with(|spawns| spawns.borrow().len())
}

pub fn attribute_spawns(from: usize, owner: u64) {
    SPAWNS.with(|spawns| {
        for spawn in spawns.borrow_mut().iter_mut().skip(from) {
            spawn.owner.get_or_insert(owner);
        }
    });
}

pub fn enqueue<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> u64 {
    enqueue_with_lifetime(future, OpLifetime::Handler)
}

pub(crate) fn enqueue_io_context<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> u64 {
    enqueue_with_lifetime(future, OpLifetime::IoContext)
}

pub(crate) fn enqueue_unrefed<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> u64 {
    enqueue_with_lifetime(future, OpLifetime::Unrefed)
}

pub fn drain_spawns() -> Vec<(u64, OpFuture, OpLifetime, Option<u64>)> {
    SPAWNS.with(|spawns| {
        spawns
            .borrow_mut()
            .drain(..)
            .map(|spawn| (spawn.id, spawn.future, spawn.lifetime, spawn.owner))
            .collect()
    })
}

pub use crate::__celld_domain_select as select;
pub use crate::__celld_domain_select_biased as select_biased;

pub mod rng {
    use rand::{CryptoRng, RngCore, SeedableRng as _};

    /// A seeded random stream for one consumer.
    pub struct Stream {
        inner: rand::rngs::StdRng,
    }

    impl Stream {
        pub(super) fn seeded(seed: u64) -> Self {
            Self {
                inner: rand::rngs::StdRng::seed_from_u64(seed),
            }
        }
    }

    impl RngCore for Stream {
        fn next_u32(&mut self) -> u32 {
            self.inner.next_u32()
        }

        fn next_u64(&mut self) -> u64 {
            self.inner.next_u64()
        }

        fn fill_bytes(&mut self, destination: &mut [u8]) {
            self.inner.fill_bytes(destination);
        }

        fn try_fill_bytes(&mut self, destination: &mut [u8]) -> Result<(), rand::Error> {
            self.inner.try_fill_bytes(destination)
        }
    }

    // A simulation needs reproducible streams, not secret ones.
    impl CryptoRng for Stream {}
}
