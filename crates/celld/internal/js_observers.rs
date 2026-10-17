// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Test ops, included into `js.rs`: what a script can read about its own
// isolate.

/// `__loader_count()`: the dynamic loaders the process holds.
fn op_loader_count(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let count = loader_registry().lock().unwrap().len();
    rv.set(v8::Number::new(scope, count as f64).into());
}

/// `__test_heap_share()`: live heap use as a share of the isolate's limit,
/// the figure heap admission compares against.
fn op_test_heap_share(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let limit = scope
        .get_slot::<Arc<HeapLimitState>>()
        .map_or(0, |state| state.limit);
    let share = heap_share(scope, limit);
    rv.set(v8::Number::new(scope, share).into());
}

/// `__test_external_memory()`: the bytes V8 accounts outside its heap.
fn op_test_external_memory(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let external = scope.get_heap_statistics().external_memory();
    rv.set(v8::Number::new(scope, external as f64).into());
}

// ── in-crate suites ─────────────────────────────────────────────────────────

#[cfg(test)]
thread_local! {
    /// Thread CPU readings to return before the platform clock, in order.
    /// `None` stands for a platform without the clock.
    static THREAD_CPU_NANOS_SAMPLES_FOR_TEST: std::cell::RefCell<std::collections::VecDeque<Option<u64>>> =
        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
    static UNIX_NOW_MS_FOR_TEST: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
    static SETTLING_HEAP_FOR_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static HEAP_COLLECTIONS_FOR_TEST: std::cell::Cell<HeapCollectionsForTest> =
        const { std::cell::Cell::new(HeapCollectionsForTest { total: 0, settling: 0 }) };
}

/// Queue thread CPU readings for this thread's next `thread_cpu_nanos` calls.
#[cfg(test)]
pub(crate) fn push_thread_cpu_nanos_for_test(samples: impl IntoIterator<Item = Option<u64>>) {
    THREAD_CPU_NANOS_SAMPLES_FOR_TEST.with(|queue| queue.borrow_mut().extend(samples));
}

/// Pin the wall clock scripts on this thread read, or release it with `None`.
#[cfg(test)]
pub(crate) fn set_unix_now_ms_for_test(timestamp_ms: Option<i64>) {
    UNIX_NOW_MS_FOR_TEST.with(|now| now.set(timestamp_ms));
}

#[cfg(test)]
fn unix_now_ms_for_test() -> Option<i64> {
    UNIX_NOW_MS_FOR_TEST.with(std::cell::Cell::get)
}

/// Full collections on this thread since an observer was installed: all of
/// them, and those inside a heap settlement.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HeapCollectionsForTest {
    pub total: u64,
    pub settling: u64,
}

#[cfg(test)]
pub(crate) fn heap_collections_for_test() -> HeapCollectionsForTest {
    HEAP_COLLECTIONS_FOR_TEST.with(std::cell::Cell::get)
}

/// Marks a heap settlement on this thread for as long as it lives.
#[cfg(test)]
struct HeapSettlementObservationForTest;

#[cfg(test)]
impl HeapSettlementObservationForTest {
    fn begin() -> Self {
        SETTLING_HEAP_FOR_TEST.with(|settling| settling.set(true));
        Self
    }
}

#[cfg(test)]
impl Drop for HeapSettlementObservationForTest {
    fn drop(&mut self) {
        SETTLING_HEAP_FOR_TEST.with(|settling| settling.set(false));
    }
}

/// Count this isolate's full collections from now on, so setup's own
/// collections never count as a settlement's.
#[cfg(test)]
fn install_heap_collection_observer_for_test(isolate: &mut v8::Isolate) {
    unsafe extern "C" fn collected(
        _isolate: v8::UnsafeRawIsolatePtr,
        _type: v8::GCType,
        _flags: v8::GCCallbackFlags,
        _data: *mut std::ffi::c_void,
    ) {
        let settling = SETTLING_HEAP_FOR_TEST.with(std::cell::Cell::get);
        HEAP_COLLECTIONS_FOR_TEST.with(|collections| {
            let mut counts = collections.get();
            counts.total += 1;
            counts.settling += u64::from(settling);
            collections.set(counts);
        });
    }
    HEAP_COLLECTIONS_FOR_TEST
        .with(|collections| collections.set(HeapCollectionsForTest::default()));
    isolate.add_gc_epilogue_callback(
        collected,
        std::ptr::null_mut(),
        v8::GCType::kGCTypeMarkSweepCompact,
    );
}
