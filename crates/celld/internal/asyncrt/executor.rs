// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The deterministic executor behind the simulated `asyncrt`.
//!
//! One [`Simulation`] runs every task of every simulated process on the
//! calling thread, in virtual time. Each process is a [`Domain`]: its own
//! host services, monotonic clock, wall-clock offset, filesystem, counters
//! and random streams. A task belongs to the domain that spawned it, and the
//! facade's free functions (`spawn`, `mono_ms`, `services`, …) answer for
//! the domain of the task being polled.
//!
//! Scheduling is seeded. Among the tasks that are ready, the next one to
//! poll is drawn from the simulation's schedule stream, so one seed fixes
//! one interleaving and different seeds explore different ones. Time moves
//! only when nothing is ready: the clock jumps to the earliest timer and
//! wakes every timer due by then, in deadline order.
//!
//! Crashing a domain drops all of its tasks at once, as a process death
//! drops its futures, and closes its token so nothing can schedule on it
//! again.

use super::TaskPanic;
use crate::host_services::HostServices;
use futures_util::FutureExt as _;
use rand::{Rng as _, SeedableRng as _};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, Location};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};

/// The wall clock of a simulation at virtual time zero, in Unix ms.
pub const WALL_EPOCH_MS: i64 = 1_800_000_000_000;

type TaskFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A deterministic world of simulated processes.
#[derive(Clone)]
pub struct Simulation {
    inner: Arc<SimInner>,
}

pub(super) struct SimInner {
    seed: u64,
    state: Mutex<SimState>,
    /// Tasks woken since they were last polled. A waker can fire from any
    /// thread, so this is the only state a waker touches.
    ready: Mutex<BTreeSet<u64>>,
}

struct SimState {
    now_ms: u64,
    next_task: u64,
    next_timer: u64,
    tasks: BTreeMap<u64, Task>,
    timers: BTreeMap<(u64, u64), Option<Waker>>,
    schedule: rand::rngs::StdRng,
    /// FNV-1a over every scheduling decision: which task ran, and every
    /// clock advance. Equal seeds must produce equal fingerprints.
    fingerprint: u64,
    polls: u64,
    /// Every scheduling decision as a line, when recording is on.
    trace: Option<Vec<String>>,
    /// How many domains each name has started, so a restarted process
    /// draws its own randomness rather than its predecessor's.
    incarnations: BTreeMap<String, u64>,
}

struct Task {
    /// `None` while the task is being polled.
    future: Option<TaskFuture>,
    domain: Arc<DomainInner>,
    site: &'static Location<'static>,
    root: Option<String>,
}

thread_local! {
    static CURRENT: RefCell<Option<(Arc<SimInner>, Arc<DomainInner>)>> =
        const { RefCell::new(None) };
}

/// Restores the previous current domain when dropped.
struct Entered(Option<(Arc<SimInner>, Arc<DomainInner>)>);

impl Drop for Entered {
    fn drop(&mut self) {
        let previous = self.0.take();
        CURRENT.with(|current| *current.borrow_mut() = previous);
    }
}

fn enter(sim: Arc<SimInner>, domain: Arc<DomainInner>) -> Entered {
    Entered(CURRENT.with(|current| current.borrow_mut().replace((sim, domain))))
}

/// The simulation and domain of the code running now, if any.
pub(super) fn current() -> Option<(Arc<SimInner>, Arc<DomainInner>)> {
    CURRENT.with(|current| current.borrow().clone())
}

/// The current domain, or a panic that names the call outside the world.
#[track_caller]
pub(super) fn current_or_panic(what: &str) -> (Arc<SimInner>, Arc<DomainInner>) {
    current().unwrap_or_else(|| {
        panic!("asyncrt::{what} ran outside a simulated domain; enter or spawn on one first")
    })
}

fn fnv(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// A stable 64-bit hash of `parts`, for deriving seeds by label.
pub(super) fn derive_seed(seed: u64, parts: &[&[u8]]) -> u64 {
    parts
        .iter()
        .fold(fnv(FNV_OFFSET, &seed.to_le_bytes()), |hash, part| {
            fnv(fnv(hash, part), &[0xff])
        })
}

impl Simulation {
    pub fn new(seed: u64) -> Self {
        // SQLite draws WAL salts from one process-wide generator, and the
        // salts reach every LTX header the replicator uploads. Seeding it
        // here makes a run's bytes reproducible in a process of its own;
        // another thread's SQLite still draws from the same generator.
        // SAFETY: PRNG_SEED takes an int seed and a null connection.
        unsafe {
            rusqlite::ffi::sqlite3_test_control(
                rusqlite::ffi::SQLITE_TESTCTRL_PRNG_SEED,
                derive_seed(seed, &[b"sqlite"]) as u32 as std::os::raw::c_int,
                std::ptr::null_mut::<rusqlite::ffi::sqlite3>(),
            );
        }
        Self {
            inner: Arc::new(SimInner {
                seed,
                state: Mutex::new(SimState {
                    now_ms: 0,
                    next_task: 1,
                    next_timer: 1,
                    tasks: BTreeMap::new(),
                    timers: BTreeMap::new(),
                    schedule: rand::rngs::StdRng::seed_from_u64(derive_seed(seed, &[b"schedule"])),
                    fingerprint: FNV_OFFSET,
                    polls: 0,
                    trace: None,
                    incarnations: BTreeMap::new(),
                }),
                ready: Mutex::new(BTreeSet::new()),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.inner.seed
    }

    /// Virtual time since the simulation began.
    pub fn now_ms(&self) -> u64 {
        self.inner.state.lock().unwrap().now_ms
    }

    /// The hash of every scheduling decision so far.
    pub fn fingerprint(&self) -> u64 {
        self.inner.state.lock().unwrap().fingerprint
    }

    /// How many task polls ran so far.
    /// Record every scheduling decision from now on: each poll with its
    /// task, domain and spawn site, and each clock advance.
    pub fn record_trace(&self) {
        self.inner
            .state
            .lock()
            .unwrap()
            .trace
            .get_or_insert_with(Vec::new);
    }

    /// The decisions recorded so far, leaving recording on.
    pub fn take_trace(&self) -> Vec<String> {
        self.inner
            .state
            .lock()
            .unwrap()
            .trace
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    pub fn polls(&self) -> u64 {
        self.inner.state.lock().unwrap().polls
    }

    /// Tasks alive in every domain.
    pub fn task_count(&self) -> usize {
        self.inner.state.lock().unwrap().tasks.len()
    }

    /// Start a process named `name`. Its monotonic clock starts now.
    pub fn domain(&self, name: &str) -> Domain {
        self.domain_with(name, DomainOptions::default())
    }

    pub fn domain_with(&self, name: &str, options: DomainOptions) -> Domain {
        let now_ms = self.now_ms();
        let incarnation = {
            let mut state = self.inner.state.lock().unwrap();
            let count = state.incarnations.entry(name.to_string()).or_insert(0);
            *count += 1;
            *count - 1
        };
        let seed = match incarnation {
            0 => derive_seed(self.inner.seed, &[b"domain", name.as_bytes()]),
            later => derive_seed(
                self.inner.seed,
                &[b"domain", name.as_bytes(), &later.to_le_bytes()],
            ),
        };
        let inner = Arc::new(DomainInner {
            sim: Arc::downgrade(&self.inner),
            name: name.to_string(),
            started_at_ms: now_ms,
            wall_offset_ms: AtomicI64Cell::new(options.wall_offset_ms),
            services: Arc::new(HostServices::scripted()),
            filesystem: options
                .filesystem
                .unwrap_or_else(|| Arc::new(celld_ltx::DirectFileSystem)),
            next_core_request: AtomicU64::new(1),
            next_async_op: AtomicU64::new(1),
            process_tag: seed,
            seed,
            rng_streams: Mutex::new(BTreeMap::new()),
            closed: AtomicBool::new(false),
        });
        inner.services.bind_domain(super::DomainToken::new(&inner));
        Domain { inner }
    }

    /// Poll one ready task, or advance the clock to the next timer. Returns
    /// false when nothing is ready and no timer is armed.
    pub fn step(&self) -> bool {
        self.step_within(u64::MAX)
    }

    /// [`Self::step`], except that the clock never passes `limit_ms`: with
    /// nothing ready and the next timer later than that, nothing happens.
    fn step_within(&self, limit_ms: u64) -> bool {
        let ready: Vec<u64> = std::mem::take(&mut *self.inner.ready.lock().unwrap())
            .into_iter()
            .collect();
        let mut state = self.inner.state.lock().unwrap();
        let mut runnable: Vec<u64> = ready
            .into_iter()
            .filter(|id| state.tasks.contains_key(id))
            .collect();
        if runnable.is_empty() {
            let Some((&(deadline, _), _)) = state.timers.iter().next() else {
                return false;
            };
            if deadline > limit_ms {
                return false;
            }
            state.now_ms = state.now_ms.max(deadline);
            state.fingerprint = fnv(state.fingerprint, b"t");
            state.fingerprint = fnv(state.fingerprint, &state.now_ms.to_le_bytes());
            let now_ms = state.now_ms;
            if let Some(trace) = state.trace.as_mut() {
                trace.push(format!("advance @{now_ms}"));
            }
            let due: Vec<(u64, u64)> = state
                .timers
                .range(..(now_ms + 1, 0))
                .map(|(key, _)| *key)
                .collect();
            // A fired timer leaves the map: its future reads the clock when
            // polled, so nothing is left to wake twice.
            let wakers: Vec<Waker> = due
                .into_iter()
                .filter_map(|key| state.timers.remove(&key).flatten())
                .collect();
            drop(state);
            for waker in wakers {
                waker.wake();
            }
            return true;
        }
        let index = state.schedule.gen_range(0..runnable.len());
        let id = runnable.swap_remove(index);
        // The rest stay ready for a later draw.
        self.inner.ready.lock().unwrap().extend(runnable);
        state.fingerprint = fnv(state.fingerprint, &id.to_le_bytes());
        state.polls += 1;
        let SimState {
            tasks,
            trace,
            polls,
            now_ms,
            ..
        } = &mut *state;
        let task = tasks.get_mut(&id).expect("runnable task exists");
        let mut future = task.future.take().expect("a task is polled once at a time");
        let domain = task.domain.clone();
        if let Some(trace) = trace.as_mut() {
            trace.push(format!(
                "poll {polls} task {id} {} {} @{now_ms}",
                domain.name, task.site
            ));
        }
        drop(state);

        let waker = Waker::from(Arc::new(TaskWaker {
            sim: Arc::downgrade(&self.inner),
            task: id,
        }));
        let poll = {
            let _entered = enter(self.inner.clone(), domain);
            future.as_mut().poll(&mut Context::from_waker(&waker))
        };
        let mut state = self.inner.state.lock().unwrap();
        match poll {
            Poll::Ready(()) => {
                state.tasks.remove(&id);
                drop(state);
                drop(future);
            }
            Poll::Pending => match state.tasks.get_mut(&id) {
                Some(task) => task.future = Some(future),
                // The domain crashed while the task ran.
                None => {
                    drop(state);
                    drop(future);
                }
            },
        }
        true
    }

    /// Run until nothing is ready and no timer is armed, or `max_steps`.
    /// Returns the steps taken.
    pub fn run_until_idle(&self, max_steps: u64) -> u64 {
        let mut steps = 0;
        while steps < max_steps && self.step() {
            steps += 1;
        }
        steps
    }

    /// Run every task and timer due up to `now_ms() + ms`, then set the
    /// clock there.
    pub fn run_for(&self, ms: u64) {
        let until = self.now_ms().saturating_add(ms);
        while self.step_within(until) {}
        let mut state = self.inner.state.lock().unwrap();
        if state.now_ms < until {
            state.now_ms = until;
            state.fingerprint = fnv(state.fingerprint, b"t");
            state.fingerprint = fnv(state.fingerprint, &until.to_le_bytes());
        }
    }

    /// Run until `done` holds, checking after every step. Returns whether it
    /// held before the world went idle or `max_steps` ran out.
    pub fn run_until(&self, max_steps: u64, mut done: impl FnMut() -> bool) -> bool {
        for _ in 0..max_steps {
            if done() {
                return true;
            }
            if !self.step() {
                return done();
            }
        }
        done()
    }

    /// Run `future` on `domain` to completion and return its output.
    ///
    /// Panics if the world goes idle first, which means the future waits for
    /// something nothing will ever do.
    pub fn block_on<T, F>(&self, domain: &Domain, future: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let mut handle = domain.spawn(future);
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = Pin::new(&mut handle).poll(&mut context) {
                return output.unwrap_or_else(|panic| panic!("{panic}"));
            }
            assert!(
                self.step(),
                "the simulation went idle before the blocked-on future finished"
            );
        }
    }

    fn spawn_on(
        &self,
        domain: &Arc<DomainInner>,
        root: Option<String>,
        site: &'static Location<'static>,
        future: TaskFuture,
    ) -> u64 {
        spawn_task(&self.inner, domain, root, site, future)
    }
}

pub(super) fn spawn_task(
    sim: &Arc<SimInner>,
    domain: &Arc<DomainInner>,
    root: Option<String>,
    site: &'static Location<'static>,
    future: TaskFuture,
) -> u64 {
    let mut state = sim.state.lock().unwrap();
    let id = state.next_task;
    state.next_task += 1;
    state.tasks.insert(
        id,
        Task {
            future: Some(future),
            domain: domain.clone(),
            site,
            root,
        },
    );
    drop(state);
    sim.ready.lock().unwrap().insert(id);
    id
}

struct TaskWaker {
    sim: Weak<SimInner>,
    task: u64,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(sim) = self.sim.upgrade() {
            sim.ready.lock().unwrap().insert(self.task);
        }
    }
}

// ── timers ──────────────────────────────────────────────────────────────────

/// A future that completes once virtual time reaches `deadline_ms`, counted
/// from the simulation's start.
pub(super) struct Timer {
    sim: Weak<SimInner>,
    deadline_ms: u64,
    key: Option<(u64, u64)>,
}

impl Timer {
    pub(super) fn new(sim: &Arc<SimInner>, deadline_ms: u64) -> Self {
        Self {
            sim: Arc::downgrade(sim),
            deadline_ms,
            key: None,
        }
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // A timer outliving its world never fires.
        let Some(sim) = this.sim.upgrade() else {
            return Poll::Pending;
        };
        let mut state = sim.state.lock().unwrap();
        if state.now_ms >= this.deadline_ms {
            if let Some(key) = this.key.take() {
                state.timers.remove(&key);
            }
            return Poll::Ready(());
        }
        let key = match this.key {
            Some(key) => key,
            None => {
                let key = (this.deadline_ms, state.next_timer);
                state.next_timer += 1;
                this.key = Some(key);
                key
            }
        };
        state.timers.insert(key, Some(context.waker().clone()));
        Poll::Pending
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let (Some(key), Some(sim)) = (self.key, self.sim.upgrade()) {
            sim.state.lock().unwrap().timers.remove(&key);
        }
    }
}

pub(super) fn now_ms(sim: &SimInner) -> u64 {
    sim.state.lock().unwrap().now_ms
}

pub(super) fn schedule_draw(sim: &SimInner, below: u32) -> u32 {
    sim.state.lock().unwrap().schedule.gen_range(0..below)
}

// ── domains ─────────────────────────────────────────────────────────────────

/// How a domain starts.
#[derive(Default)]
pub struct DomainOptions {
    /// The domain's wall clock minus the simulation's, in ms.
    pub wall_offset_ms: i64,
    /// The node filesystem; the host's own by default.
    pub filesystem: Option<Arc<dyn celld_ltx::FileSystem>>,
}

/// One simulated process.
#[derive(Clone)]
pub struct Domain {
    pub(super) inner: Arc<DomainInner>,
}

/// An `i64` cell the domain's wall offset lives in.
pub(super) struct AtomicI64Cell(std::sync::atomic::AtomicI64);

impl AtomicI64Cell {
    fn new(value: i64) -> Self {
        Self(std::sync::atomic::AtomicI64::new(value))
    }

    pub(super) fn get(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }

    fn add(&self, delta: i64) {
        self.0.fetch_add(delta, Ordering::SeqCst);
    }
}

pub(super) struct DomainInner {
    pub(super) sim: Weak<SimInner>,
    pub(super) name: String,
    pub(super) started_at_ms: u64,
    pub(super) wall_offset_ms: AtomicI64Cell,
    pub(super) services: Arc<HostServices>,
    pub(super) filesystem: Arc<dyn celld_ltx::FileSystem>,
    pub(super) next_core_request: AtomicU64,
    pub(super) next_async_op: AtomicU64,
    pub(super) process_tag: u64,
    pub(super) seed: u64,
    /// How many streams each consumer label drew so far.
    pub(super) rng_streams: Mutex<BTreeMap<&'static str, u64>>,
    pub(super) closed: AtomicBool,
}

impl DomainInner {
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.sim.strong_count() == 0
    }

    /// This domain's monotonic clock: virtual time since it started.
    pub(super) fn mono_ms(&self, sim: &SimInner) -> u64 {
        now_ms(sim).saturating_sub(self.started_at_ms)
    }
}

impl Domain {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn services(&self) -> Arc<HostServices> {
        self.inner.services.clone()
    }

    pub fn is_crashed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Run `f` as code of this domain, outside any task: setup that reads
    /// the clock, the services or a random stream.
    pub fn enter<T>(&self, f: impl FnOnce() -> T) -> T {
        let sim = self.inner.sim.upgrade().expect("the simulation is alive");
        let _entered = enter(sim, self.inner.clone());
        f()
    }

    /// Spawn `future` on this domain.
    #[track_caller]
    pub fn spawn<T, F>(&self, future: F) -> super::TaskHandle<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let sim = self.inner.sim.upgrade().expect("the simulation is alive");
        let (handle, task) = joinable(future);
        Simulation { inner: sim }.spawn_on(&self.inner, None, Location::caller(), task);
        handle
    }

    /// Move this domain's wall clock by `delta_ms`.
    pub fn step_wall_clock(&self, delta_ms: i64) {
        self.inner.wall_offset_ms.add(delta_ms);
    }

    /// The process dies: every task it owns is dropped where it stands, and
    /// its token refuses new work. Returns how many tasks were dropped.
    pub fn crash(&self) -> usize {
        self.inner.closed.store(true, Ordering::SeqCst);
        let Some(sim) = self.inner.sim.upgrade() else {
            return 0;
        };
        let mut state = sim.state.lock().unwrap();
        let doomed: Vec<u64> = state
            .tasks
            .iter()
            .filter(|(_, task)| Arc::ptr_eq(&task.domain, &self.inner))
            .map(|(id, _)| *id)
            .collect();
        let dropped: Vec<Task> = doomed
            .iter()
            .filter_map(|id| state.tasks.remove(id))
            .collect();
        drop(state);
        let count = dropped.len();
        // Dropping a future can drop timers and wake join handles, which
        // take the state lock themselves.
        drop(dropped);
        count
    }

    /// The spawn sites and roots of this domain's live tasks, in spawn order.
    pub fn tasks(&self) -> Vec<(String, Option<String>)> {
        let Some(sim) = self.inner.sim.upgrade() else {
            return Vec::new();
        };
        let state = sim.state.lock().unwrap();
        state
            .tasks
            .values()
            .filter(|task| Arc::ptr_eq(&task.domain, &self.inner))
            .map(|task| (task.site.to_string(), task.root.clone()))
            .collect()
    }
}

// ── join handles ────────────────────────────────────────────────────────────

pub(super) struct JoinSlot<T> {
    pub(super) output: Option<Result<T, TaskPanic>>,
    pub(super) waker: Option<Waker>,
}

/// Wrap `future` so its output, panic or cancellation reaches a handle.
pub(super) fn joinable<T, F>(future: F) -> (super::TaskHandle<T>, TaskFuture)
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let slot = Arc::new(Mutex::new(JoinSlot {
        output: None,
        waker: None,
    }));
    let mut guard = Cancelled(Some(slot.clone()));
    let task = Box::pin(async move {
        let output = AssertUnwindSafe(future).catch_unwind().await;
        let output = output.map_err(|panic| TaskPanic {
            message: panic_message(&panic),
        });
        if let Some(slot) = guard.0.take() {
            finish(&slot, output);
        }
    });
    (super::TaskHandle { slot }, task)
}

/// Settles the handle as cancelled when the task is dropped unfinished.
struct Cancelled<T>(Option<Arc<Mutex<JoinSlot<T>>>>);

impl<T> Drop for Cancelled<T> {
    fn drop(&mut self) {
        if let Some(slot) = self.0.take() {
            finish(
                &slot,
                Err(TaskPanic {
                    message: "task was cancelled".into(),
                }),
            );
        }
    }
}

fn finish<T>(slot: &Mutex<JoinSlot<T>>, output: Result<T, TaskPanic>) {
    let waker = {
        let mut slot = slot.lock().unwrap();
        slot.output = Some(output);
        slot.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "task panicked".into())
}
