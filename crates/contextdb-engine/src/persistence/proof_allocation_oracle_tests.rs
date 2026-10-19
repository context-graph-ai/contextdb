//! An allocator oracle beside the accountant: what the proof paths really
//! allocate, measured independently of the counters they keep about
//! themselves. The accountant can only prove what it was told; this measures
//! what the process actually held.

use super::*;
use crate::memory_accounting::MemoryAccountant;
use crate::persistence::{OriginalMutationVector, OriginalPersistenceFrame};
use serial_test::serial;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

struct MeasuredAllocator;
// Measurement is scoped to the THREAD under test, not to the process. A
// process-global switch measures every other test thread running beside this
// one and reports their allocations as this subsystem's, which is exactly the
// contamination that makes an allocator oracle meaningless. The window is a
// const-initialised thread-local, so reading it inside the allocator neither
// allocates nor runs a destructor; deallocation uses the window recorded in the
// allocation's own header, so a block freed on another thread still leaves the
// measured total correct. Earlier windows' surviving blocks must not subtract
// from a later window's freshly reset total. The mutex makes checking window
// identity and updating its counters atomic with starting/ending a window.
thread_local! {
    static MEASURING_HERE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    // Existing oracle records the unmodified allocator request, independent
    // of its measurement header. No extra allocator or allocating ledger.
    static WATCH_PAYLOAD_FREE: std::cell::Cell<(usize, bool, bool)> = const { std::cell::Cell::new((0, false, false)) };
    static WATCH_NODE_PROCESS_FREE: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    static WATCH_NODE_FREE: std::cell::Cell<(usize, usize, usize, usize, bool)> = const { std::cell::Cell::new((0, 0, 0, 0, false)) };
    static LAST_REQUEST: std::cell::Cell<(usize, usize, usize)> = const { std::cell::Cell::new((0, 0, 0)) };
    static WATCH_PAYLOAD_ACCOUNTANT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PAYLOAD_FREE_CENSUS: std::cell::Cell<(usize, usize, usize, usize)> = const { std::cell::Cell::new((0, 0, 0, 0)) };
    static LAST_REQUEST_POINTER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static NATIVE_REQUESTS: std::cell::Cell<[(usize, usize, usize); 256]> = const { std::cell::Cell::new([(0, 0, 0); 256]) };
    static DECLARATION_OBSERVATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FUNDING_CENSUS: std::cell::Cell<(usize, usize, usize, usize)> = const { std::cell::Cell::new((0, 0, 0, 0)) };
    static READER_STANDING_FUNDING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SOURCE_COUNTER_IS_READER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static SOURCE_REQUEST_COUNTER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SOURCE_REQUEST_CREDIT: std::cell::Cell<[usize; 32]> = const { std::cell::Cell::new([0; 32]) };
    static NATIVE_REQUEST_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static NATIVE_REQUEST_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Static observation storage has no allocator footprint and survives concurrent
// final-owner contenders. Counter-free observation never reads the freed counter.
struct NativeFreeWatch {
    pointer: AtomicUsize,
    window: AtomicUsize,
    counter: AtomicUsize,
    before: AtomicUsize,
    after: AtomicUsize,
    count: AtomicUsize,
    bytes: AtomicUsize,
    live_at_free: AtomicUsize,
}
impl NativeFreeWatch {
    const fn new() -> Self {
        Self {
            pointer: AtomicUsize::new(0),
            window: AtomicUsize::new(0),
            counter: AtomicUsize::new(0),
            before: AtomicUsize::new(0),
            after: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            live_at_free: AtomicUsize::new(0),
        }
    }
}
static NATIVE_FREE: [NativeFreeWatch; 128] = [const { NativeFreeWatch::new() }; 128];

pub(crate) struct DeclarationObservation;
pub(crate) fn declaration_observation() -> DeclarationObservation {
    DECLARATION_OBSERVATION.with(|slot| slot.set(true));
    DeclarationObservation
}
impl Drop for DeclarationObservation {
    fn drop(&mut self) {
        DECLARATION_OBSERVATION.with(|slot| slot.set(false));
    }
}

// Native requested bytes, excluding the oracle's own headers, are compared
// with the originating writer before System.alloc, including overlapping clones.
pub(crate) fn funded_requests<T>(
    writer: &MemoryAccountant,
    operation: impl FnOnce() -> T,
) -> (T, usize) {
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            FUNDING_CENSUS.with(|slot| slot.set((0, 0, 0, 0)));
        }
    }
    FUNDING_CENSUS.with(|slot| {
        slot.set((
            writer.writer_registration_observation_for_test().4,
            writer.usage().used,
            0,
            0,
        ));
    });
    let guard = Guard;
    let (value, _, _) = measured(operation);
    let gap = FUNDING_CENSUS.with(|slot| slot.get().3);
    drop(guard);
    (value, gap)
}

// Extend the mapped oracle's fixed storage; no allocating observation ledger.
pub(crate) fn source_requests_for_test<T>(
    counter: usize,
    operation: impl FnOnce() -> T,
) -> (T, [(usize, usize, usize); 256], usize) {
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            SOURCE_REQUEST_COUNTER.with(|c| c.set(0));
        }
    }
    SOURCE_REQUEST_CREDIT.with(|credits| credits.set([0; 32]));
    SOURCE_REQUEST_COUNTER.with(|c| c.set(counter));
    let guard = Guard;
    let (value, _, count) = native_requests_for_test(operation);
    drop(guard);
    (value, NATIVE_REQUESTS.with(std::cell::Cell::get), count)
}
// Reuse the physical source oracle for the actual operation child's u64
// counter. The caller keeps that child alive for the complete measurement.
pub(crate) fn reader_requests_for_test<T>(counter: usize, action: impl FnOnce() -> T) -> T {
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) { SOURCE_COUNTER_IS_READER.with(|slot| slot.set(false)); }
    }
    READER_STANDING_FUNDING.with(|slot| slot.set(
        unsafe { &*(counter as *const std::sync::atomic::AtomicU64) }.load(AtomicOrdering::SeqCst) as usize));
    SOURCE_COUNTER_IS_READER.with(|slot| slot.set(true));
    let guard = Guard;
    let (value, _, _) = source_requests_for_test(counter, action);
    drop(guard);
    value
}
fn observed_source_funding(counter: usize) -> usize {
    if SOURCE_COUNTER_IS_READER.with(std::cell::Cell::get) {
        (unsafe { &*(counter as *const std::sync::atomic::AtomicU64) }
            .load(AtomicOrdering::SeqCst) as usize)
            .saturating_sub(READER_STANDING_FUNDING.with(std::cell::Cell::get))
    } else {
        unsafe { &*(counter as *const AtomicUsize) }.load(AtomicOrdering::SeqCst)
    }
}
// Native payload only: measurement headers are oracle overhead. Observe
// overlapping allocations and funding returned before their actual free.
pub(crate) fn source_unfunded_peak_for_test() -> (usize, (usize, usize, usize, bool)) {
    let measurement = MEASUREMENT.lock().unwrap();
    (
        measurement.source_unfunded_peak,
        measurement.source_unfunded_request,
    )
}
pub(crate) fn source_request_credit_for_test() -> [usize; 32] {
    SOURCE_REQUEST_CREDIT.with(std::cell::Cell::get)
}
pub(crate) fn watch_source_free_for_test(requests: &[(usize, usize, usize)], counter: usize) {
    for watch in &NATIVE_FREE {
        watch.pointer.store(0, AtomicOrdering::SeqCst);
        watch.count.store(0, AtomicOrdering::SeqCst);
    }
    append_source_free_for_test(requests, counter);
}
fn append_source_free_for_test(requests: &[(usize, usize, usize)], counter: usize) {
    for request in requests {
        let watch = NATIVE_FREE
            .iter()
            .find(|w| {
                w.pointer.load(AtomicOrdering::SeqCst) == 0
                    && w.count.load(AtomicOrdering::SeqCst) == 0
            })
            .expect("existing free watch has a vacant slot");

        assert_ne!(request.0, 0);
        let header = unsafe {
            (request.0 as *const u8)
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .read()
        };
        assert_ne!(header.window, 0);
        watch.bytes.store(request.1, AtomicOrdering::SeqCst);
        watch.live_at_free.store(0, AtomicOrdering::SeqCst);
        watch.window.store(header.window, AtomicOrdering::SeqCst);
        watch.before.store(0, AtomicOrdering::SeqCst);
        watch.after.store(0, AtomicOrdering::SeqCst);
        watch.count.store(0, AtomicOrdering::SeqCst);
        watch.counter.store(counter, AtomicOrdering::SeqCst);
        watch.pointer.store(request.0, AtomicOrdering::SeqCst);
    }
}
pub(crate) fn source_free_for_test(count: usize) -> [(usize, usize, usize); 128] {
    assert!(count <= NATIVE_FREE.len());
    std::array::from_fn(|index| {
        let w = &NATIVE_FREE[index];
        (
            w.before.load(AtomicOrdering::SeqCst),
            w.after.load(AtomicOrdering::SeqCst),
            w.count.load(AtomicOrdering::SeqCst),
        )
    })
}

pub(crate) fn native_requests_for_test<T>(
    operation: impl FnOnce() -> T,
) -> (T, [(usize, usize, usize); 3], usize) {
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            NATIVE_REQUEST_ACTIVE.with(|active| active.set(false));
        }
    }
    NATIVE_REQUESTS.with(|requests| requests.set([(0, 0, 0); 256]));
    NATIVE_REQUEST_COUNT.with(|count| count.set(0));
    NATIVE_REQUEST_ACTIVE.with(|active| active.set(true));
    let guard = Guard;
    let (value, _, _) = measured(operation);
    drop(guard);
    (
        value,
        NATIVE_REQUESTS.with(|requests| requests.get()[..3].try_into().unwrap()),
        NATIVE_REQUEST_COUNT.with(std::cell::Cell::get),
    )
}

pub(crate) fn watch_native_free_for_test(requests: [(usize, usize, usize); 3], counter: usize) {
    for (index, watch) in NATIVE_FREE.iter().take(3).enumerate() {
        // This is our oracle's own header, read while the allocation is live.
        // Pair address with its unique window so address reuse on a contender
        // thread cannot manufacture an extra free for a previous owner.
        let header = unsafe {
            (requests[index].0 as *const u8)
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .read()
        };
        watch.window.store(header.window, AtomicOrdering::SeqCst);
        watch.before.store(0, AtomicOrdering::SeqCst);
        watch.after.store(0, AtomicOrdering::SeqCst);
        watch.count.store(0, AtomicOrdering::SeqCst);
        watch
            .counter
            .store(if index == 0 { 0 } else { counter }, AtomicOrdering::SeqCst);
        watch
            .pointer
            .store(requests[index].0, AtomicOrdering::SeqCst);
    }
}

pub(crate) fn native_free_observation_for_test() -> [(usize, usize, usize); 3] {
    std::array::from_fn(|index| {
        let watch = &NATIVE_FREE[index];
        (
            watch.before.load(AtomicOrdering::SeqCst),
            watch.after.load(AtomicOrdering::SeqCst),
            watch.count.load(AtomicOrdering::SeqCst),
        )
    })
}
struct Measurement {
    last_window: usize,
    active_window: usize,
    live: usize,
    peak: usize,
    payload_live: usize,
    source_unfunded_peak: usize,
    source_unfunded_request: (usize, usize, usize, bool),
}
static MEASUREMENT: Mutex<Measurement> = Mutex::new(Measurement {
    last_window: 0,
    active_window: 0,
    live: 0,
    peak: 0,
    payload_live: 0,
    source_unfunded_peak: 0,
    source_unfunded_request: (0, 0, 0, false),
});

#[repr(C)]
struct Header {
    window: usize,
    bytes: usize,
}

fn allocation_layout(layout: Layout) -> (Layout, usize) {
    let alignment = layout.align().max(std::mem::align_of::<Header>());
    let offset = std::mem::size_of::<Header>().next_multiple_of(alignment);
    (
        Layout::from_size_align(layout.size() + offset, alignment).expect("measured layout"),
        offset,
    )
}

unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if NATIVE_REQUEST_ACTIVE
            .try_with(std::cell::Cell::get)
            .unwrap_or(false)
            && crate::vector_read_memory::native_ownership_proof::refuse_selected_live_layout(layout)
            || crate::vector_read_memory::native_ownership_proof::refuse_allocation(layout) {
            return std::ptr::null_mut();
        }
        let (allocation, offset) = allocation_layout(layout);
        let source_counter = SOURCE_REQUEST_COUNTER
            .try_with(std::cell::Cell::get)
            .unwrap_or(0);
        if source_counter != 0 {
            let index = NATIVE_REQUEST_COUNT.with(std::cell::Cell::get);
            if index < 32 {
                SOURCE_REQUEST_CREDIT.with(|credits| {
                    let mut entries = credits.get();
                    entries[index] = observed_source_funding(source_counter);
                    credits.set(entries);
                });
            }
        }
        FUNDING_CENSUS.with(|slot| {
            let (writer, baseline, live, gap) = slot.get();
            if writer != 0 && DECLARATION_OBSERVATION.with(std::cell::Cell::get) {
                let live = live + layout.size();
                let admitted = unsafe { &*(writer as *const AtomicUsize) }
                    .load(AtomicOrdering::SeqCst)
                    .saturating_sub(baseline);
                slot.set((
                    writer,
                    baseline,
                    live,
                    gap.max(live.saturating_sub(admitted)),
                ));
            }
        });
        let base = unsafe { System.alloc(allocation) };
        if base.is_null() {
            return base;
        }
        let pointer = unsafe { base.add(offset) };
        let window = MEASURING_HERE.try_with(std::cell::Cell::get).unwrap_or(0);
        if NATIVE_REQUEST_ACTIVE
            .try_with(std::cell::Cell::get)
            .unwrap_or(false)
        {
            NATIVE_REQUEST_COUNT.with(|count| {
                let index = count.get();
                if index < 256 {
                    NATIVE_REQUESTS.with(|requests| {
                        let mut entries = requests.get();
                        entries[index] = (pointer as usize, layout.size(), layout.align());
                        requests.set(entries);
                    });
                }
                count.set(index + 1);
            });
        }
        unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .write(Header {
                    window,
                    bytes: allocation.size(),
                });
        }
        if window != 0 {
            LAST_REQUEST_POINTER.with(|last| last.set(pointer as usize));
            LAST_REQUEST.with(|last| {
                let (_, _, count) = last.get();
                last.set((layout.size(), layout.align(), count + 1));
            });
            let mut measurement = MEASUREMENT.lock().unwrap();
            if measurement.active_window == window {
                measurement.live += allocation.size();
                measurement.peak = measurement.peak.max(measurement.live);
                measurement.payload_live += layout.size();
                if source_counter != 0 {
                    let funded = observed_source_funding(source_counter);
                    let gap = measurement.payload_live.saturating_sub(funded);
                    if gap > measurement.source_unfunded_peak {
                        measurement.source_unfunded_peak = gap;
                        measurement.source_unfunded_request =
                            (layout.size(), measurement.payload_live, funded, false);
                    }
                }
            }
        }
        maintenance_payload_allocated(pointer as usize, layout.size());
        super::super::writer_row_heads::staging_witness::allocated(pointer as usize, layout.size());
        crate::vector_read_memory::native_ownership_proof::allocated(pointer, layout, window);
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let (allocation, offset) = allocation_layout(layout);
        let header = unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .read()
        };
        if header.window != 0 {
            let mut measurement = MEASUREMENT.lock().unwrap();
            if measurement.active_window == header.window {
                let source_counter = SOURCE_REQUEST_COUNTER
                    .try_with(std::cell::Cell::get)
                    .unwrap_or(0);
                if source_counter != 0 {
                    let funded = observed_source_funding(source_counter);
                    let gap = measurement.payload_live.saturating_sub(funded);
                    if gap > measurement.source_unfunded_peak {
                        measurement.source_unfunded_peak = gap;
                        measurement.source_unfunded_request =
                            (layout.size(), measurement.payload_live, funded, true);
                    }
                }
                measurement.payload_live -= layout.size();
                measurement.live -= header.bytes;
            }
        }
        if header.window != 0 && MEASURING_HERE.with(std::cell::Cell::get) == header.window {
            FUNDING_CENSUS.with(|slot| {
                let (writer, baseline, live, gap) = slot.get();
                if writer != 0 && DECLARATION_OBSERVATION.with(std::cell::Cell::get) {
                    slot.set((writer, baseline, live - layout.size(), gap));
                }
            });
        }
        let payload_watch = WATCH_PAYLOAD_FREE.with(std::cell::Cell::get);
        let watching_payload = payload_watch.0 == pointer as usize && payload_watch.0 != 0;
        let watch = WATCH_NODE_FREE.with(std::cell::Cell::get);
        let watching_node = watch.0 == pointer as usize && watch.0 != 0;
        let process_before_free = if watching_node {
            crate::memory_accounting::process_aggregate_bytes_for_test()
        } else {
            0
        };
        let used_before_free = if watching_node {
            unsafe { &*(watch.1 as *const AtomicUsize) }.load(AtomicOrdering::SeqCst)
        } else {
            0
        };
        let native_watch = NATIVE_FREE.iter().find(|watch| {
            watch.pointer.load(AtomicOrdering::SeqCst) == pointer as usize
                && watch.window.load(AtomicOrdering::SeqCst) == header.window
        });
        if let Some(watch) = native_watch {
            let counter = watch.counter.load(AtomicOrdering::SeqCst);
            let before = if counter == 0 {
                crate::memory_accounting::process_aggregate_bytes_for_test()
            } else {
                unsafe { &*(counter as *const AtomicUsize) }.load(AtomicOrdering::SeqCst)
            };
            let live = NATIVE_FREE
                .iter()
                .filter(|w| {
                    w.pointer.load(AtomicOrdering::SeqCst) != 0
                        && w.counter.load(AtomicOrdering::SeqCst) == counter
                })
                .map(|w| w.bytes.load(AtomicOrdering::SeqCst))
                .sum();
            watch.live_at_free.store(live, AtomicOrdering::SeqCst);
            watch.before.store(before, AtomicOrdering::SeqCst);
        }
        super::super::writer_row_heads::staging_witness::freed(pointer as usize, layout.size());
        crate::vector_read_memory::native_ownership_proof::before_free(
            pointer,
            layout,
            header.window,
        );
        unsafe {
            System.dealloc(pointer.sub(offset), allocation);
        }
        crate::vector_read_memory::native_ownership_proof::after_free(pointer);
        maintenance_payload_freed(pointer as usize);
        if let Some(watch) = native_watch {
            let counter = watch.counter.load(AtomicOrdering::SeqCst);
            let after = if counter == 0 {
                crate::memory_accounting::process_aggregate_bytes_for_test()
            } else {
                unsafe { &*(counter as *const AtomicUsize) }.load(AtomicOrdering::SeqCst)
            };
            watch.after.store(after, AtomicOrdering::SeqCst);
            watch.count.fetch_add(1, AtomicOrdering::SeqCst);
            watch.pointer.store(0, AtomicOrdering::SeqCst);
        }
        graph_buffer_deallocated(pointer as usize);
        if watching_payload {
            WATCH_PAYLOAD_FREE.with(|slot| slot.set((payload_watch.0, true, false)));
            let observer = WATCH_PAYLOAD_ACCOUNTANT.with(std::cell::Cell::get);
            if observer != 0 {
                // External observer Arc remains alive for this exact payload.
                // The node pointer is never dereferenced after physical free.
                let accountant = unsafe { &*(observer as *const MemoryAccountant) };
                let registration = accountant.writer_registration_observation_for_test();
                PAYLOAD_FREE_CENSUS.with(|slot| {
                    slot.set((
                        registration.2,
                        registration.3,
                        accountant.writer_fixed_ownership().2,
                        accountant.usage().used,
                    ))
                });
            }
        }
        if watching_node {
            WATCH_NODE_PROCESS_FREE.with(|slot| {
                slot.set((
                    process_before_free,
                    crate::memory_accounting::process_aggregate_bytes_for_test(),
                ))
            });
            let payload_done = WATCH_PAYLOAD_FREE.with(std::cell::Cell::get);
            if payload_done.0 != 0 {
                WATCH_PAYLOAD_FREE
                    .with(|slot| slot.set((payload_done.0, payload_done.1, payload_done.1)));
            }
            let used_after_free =
                unsafe { &*(watch.1 as *const AtomicUsize) }.load(AtomicOrdering::SeqCst);
            WATCH_NODE_FREE
                .with(|slot| slot.set((watch.0, watch.1, used_before_free, used_after_free, true)));
        }
    }
}

#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

/// Run `operation` with every allocation on this thread measured, returning
/// its peak live bytes and what it still held when it ended.
// Native allocator requests, excluding the oracle's private measurement
// headers. This measures physical capacities rather than accountant events.
pub(crate) fn measured<T>(operation: impl FnOnce() -> T) -> (T, usize, usize) {
    // All oracle callers are serial tests. Reject overlap without holding the
    // allocator's mutex while constructing a panic.
    let window = {
        let mut measurement = MEASUREMENT.lock().unwrap();
        if measurement.active_window != 0 {
            None
        } else {
            measurement.last_window += 1;
            measurement.active_window = measurement.last_window;
            measurement.live = 0;
            measurement.peak = 0;
            measurement.payload_live = 0;
            measurement.source_unfunded_peak = 0;
            measurement.source_unfunded_request = (0, 0, 0, false);
            Some(measurement.active_window)
        }
    }
    .expect("allocation measurement windows cannot overlap");
    struct WindowGuard;
    impl Drop for WindowGuard {
        fn drop(&mut self) {
            MEASURING_HERE.with(|measuring| measuring.set(0));
            MEASUREMENT.lock().unwrap().active_window = 0;
        }
    }
    let guard = WindowGuard;
    MEASURING_HERE.with(|measuring| measuring.set(window));
    let value = operation();
    let (peak, live) = {
        let measurement = MEASUREMENT.lock().unwrap();
        (measurement.peak, measurement.live)
    };
    drop(guard);
    (value, peak, live)
}

#[test]
#[serial]
fn an_earlier_windows_delayed_free_does_not_change_the_next_window() {
    let layout = Layout::from_size_align(4096, 1).unwrap();
    let bytes = allocation_layout(layout).0.size();
    let (old, first_peak, first_live) = measured(|| unsafe { ALLOCATOR.alloc(layout) });
    assert!(!old.is_null());
    assert_eq!((first_peak, first_live), (bytes, bytes));

    let (current, peak, live) = measured(|| unsafe {
        ALLOCATOR.dealloc(old, layout);
        ALLOCATOR.alloc(layout)
    });
    assert!(!current.is_null());
    assert_eq!((peak, live), (bytes, bytes));
    unsafe { ALLOCATOR.dealloc(current, layout) };
}

#[test]
#[serial]
fn a_cross_thread_free_is_attributed_only_to_its_allocation_window() {
    let layout = Layout::from_size_align(4096, 1).unwrap();
    let bytes = allocation_layout(layout).0.size();
    let (old, _, _) = measured(|| unsafe { ALLOCATOR.alloc(layout) });
    assert!(!old.is_null());
    let old = old as usize;
    // Spawn outside measurement so the thread's own allocation lifetime is
    // irrelevant; barriers drive both frees while the next window is active.
    let ready = Arc::new(std::sync::Barrier::new(2));
    let finished = Arc::new(std::sync::Barrier::new(2));
    let current = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let neighbour = {
        let ready = Arc::clone(&ready);
        let finished = Arc::clone(&finished);
        let current = Arc::clone(&current);
        std::thread::spawn(move || {
            ready.wait();
            unsafe {
                ALLOCATOR.dealloc(old as *mut u8, layout);
                ALLOCATOR.dealloc(current.load(AtomicOrdering::SeqCst) as *mut u8, layout);
            }
            finished.wait();
        })
    };
    let ((), peak, live) = measured(|| {
        let pointer = unsafe { ALLOCATOR.alloc(layout) };
        assert!(!pointer.is_null());
        current.store(pointer as usize, AtomicOrdering::SeqCst);
        ready.wait();
        finished.wait();
    });
    neighbour.join().unwrap();
    assert_eq!((peak, live), (bytes, 0));
    crate::vector_read_memory::native_ownership_proof::run_native_rows();
}

/// The oracle measures this thread and no other. A neighbour allocating
/// megabytes throughout the measurement must not appear in it - that is the
/// difference between an attributed measurement and a process census.
#[test]
#[serial]
fn the_allocation_oracle_measures_only_the_thread_under_test() {
    let running = Arc::new(AtomicBool::new(true));
    let neighbour = {
        let running = Arc::clone(&running);
        std::thread::spawn(move || {
            let mut held: Vec<Vec<u8>> = Vec::new();
            while running.load(AtomicOrdering::SeqCst) {
                held.push(vec![0u8; 1024 * 1024]);
                if held.len() > 8 {
                    held.clear();
                }
                std::thread::yield_now();
            }
        })
    };
    let ((), peak, _) = measured(|| {
        let small = vec![0u8; 4096];
        std::hint::black_box(&small);
        std::thread::sleep(std::time::Duration::from_millis(50));
    });
    running.store(false, AtomicOrdering::SeqCst);
    neighbour.join().expect("the neighbour thread finishes");
    assert!(
        peak < 1024 * 1024,
        "a neighbouring thread's allocations are not attributed to the measured work: peak={peak}"
    );
}

fn wide_posting(row: u64, key_bytes: usize) -> ContentIdentityMutation {
    let key = crate::persistence::DurableRelationalIndexKey::new(
        vec![contextdb_core::DirectedValue::Asc(
            contextdb_core::TotalOrdAsc(Value::Text(format!("{row:012}{}", "w".repeat(key_bytes)))),
        )],
        RowId(row),
        TxId(0),
    )
    .expect("encode ordered posting key");
    relational_index_posting_mutation("wide", "by_key", &key.0)
}

fn accountant(limit: usize) -> Arc<MemoryAccountant> {
    Arc::new(MemoryAccountant::with_budget(limit))
}

/// The writer path, with keys above one chunk: what the mutation really
/// allocates has to stay inside what it admitted, and a refusal has to give
/// back both.
#[test]
#[serial]
fn a_wide_key_writer_allocates_no_more_than_it_admitted() {
    vector_identity_directory_refunds_only_after_its_buffer_is_freed();
    vector_row_encoding_has_exact_writer_admission();
    vector_identity_values_release_payloads_after_hashing();
    let root = tempfile::tempdir().unwrap();
    let database = ProofFixtureDatabase::create(root.path().join("writer.db")).unwrap();
    let records = (0..24_u64)
        .map(|row| wide_posting(row, 70 * 1024))
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    crate::persistence::erasure_frontiers::initialize(&write).unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();

    let replacements = records
        .iter()
        .map(|record| {
            ContentIdentityMutation::put(
                record.family,
                record.logical_key.clone(),
                b"replaced".to_vec(),
            )
        })
        .collect::<Vec<_>>();

    // Control: the same key bytes handed to the same storage engine with no
    // proof maintenance at all. Redb's staging of the values it is given is
    // not this code's allocation to admit, so the control is what separates
    // the storage cost from the proof cost.
    let control_store = redb::Database::create(root.path().join("control.db")).unwrap();
    let control_write = control_store.begin_write().unwrap();
    let (_, control_peak, _) = measured(|| {
        let mut table = control_write
            .open_table(CONTENT_IDENTITY_NODES_TABLE)
            .unwrap();
        for record in &records {
            let blob = std::iter::once(0)
                .chain(record.logical_key.iter().copied())
                .collect::<Vec<_>>();
            let key = node_storage_key(record.family, &key_hash(&record.logical_key));
            table.insert(key.as_slice(), blob.as_slice()).unwrap();
        }
    });
    drop(control_write);

    let budget = accountant(64 * 1024 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&budget).into()));
    reset_work_for_test();
    let (result, peak, retained) = measured(|| {
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &replacements,
            &mut admission,
        )
    });
    result.expect("an admitted wide-key transaction commits");
    let admitted = admission.peak();
    let handed_to_storage = take_work_bytes_for_test().1 as usize;
    eprintln!(
        "writer allocation oracle: measured_peak={peak} retained_at_end={retained} \
         admitted_peak={admitted} storage_control_peak={control_peak} \
         handed_to_storage={handed_to_storage}"
    );
    // What the process really held decomposes into three things this test can
    // name: the proof memory this transaction admitted, what the storage engine
    // costs for the same key bytes (the control), and the bytes the transaction
    // handed the storage engine to stage. A global allocator cannot attribute
    // an allocation to a subsystem, so this is a bound, not an equality - the
    // discriminating parts of this oracle are the refusal unwind below and the
    // sufficiency of the admission itself.
    assert!(
        peak <= admitted
            .saturating_add(control_peak)
            .saturating_add(handed_to_storage),
        "the mutation allocated beyond its admission and the storage it drove: \
         measured={peak} admitted={admitted} control={control_peak} \
         storage={handed_to_storage}"
    );
    // The other direction, and the one that stops over-admission from making
    // the bound above easy to pass: the admission must never charge for bytes
    // the process did not actually hold. Storage is attributed once - the
    // control measures the engine's cost for the same key blobs, and
    // `handed_to_storage` the distinct chunk bytes this path stages - and
    // neither is allowed to pad the admission.
    assert!(
        admitted <= peak,
        "the admission charges for memory that was never allocated: \
         admitted={admitted} measured={peak}"
    );
    drop(admission);
    drop(write);
    assert_eq!(budget.usage().used, 0, "the admission is returned");

    // The admission is a faithful bound on what the work needs: a budget the
    // size of the admitted peak is enough to run it, and half of it is not.
    let exact = accountant(admitted + 64 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&exact).into()));
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &replacements,
        &mut admission,
    )
    .expect("a budget the size of the admission runs the transaction");
    drop(admission);
    drop(write);
    let halved = accountant(admitted / 2);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&halved).into()));
    assert!(
        matches!(
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &replacements,
                &mut admission,
            ),
            Err(Error::MemoryBudgetExceeded { .. })
        ),
        "half the admitted peak is not enough, so the admission is not a phantom"
    );
    drop(admission);
    drop(write);

    // A refusal unwinds: nothing admitted and nothing still allocated.
    let tight = accountant(192 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&tight).into()));
    let (refused, refusal_peak, refusal_retained) = measured(|| {
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &replacements,
            &mut admission,
        )
    });
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "{refused:?}"
    );
    drop(admission);
    drop(write);
    eprintln!(
        "writer refusal oracle: measured_peak={refusal_peak} retained_at_end={refusal_retained} accountant_after={}",
        tight.usage().used
    );
    assert_eq!(
        tight.usage().used,
        0,
        "a refused transaction admits nothing"
    );
    assert!(
        refusal_retained <= refusal_peak / 2,
        "a refused transaction does not keep what it allocated: retained={refusal_retained} peak={refusal_peak}"
    );
}

/// The reader path over the same keys: the authenticated walk's real
/// allocation stays inside the reservation its caller admitted, and ends
/// holding nothing.
#[test]
#[serial]
fn a_wide_key_reader_allocates_no_more_than_its_admitted_pool() {
    let root = tempfile::tempdir().unwrap();
    let database = ProofFixtureDatabase::create(root.path().join("reader.db")).unwrap();
    let records = (0..24_u64)
        .map(|row| wide_posting(row, 70 * 1024))
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();

    let read = database.begin_read().unwrap();
    let budget = accountant(64 * 1024 * 1024);
    let verifier = ContentIdentityReadVerifier::open(
        &read,
        std::path::Path::new("reader.db"),
        Some(Arc::clone(&budget).into()),
    )
    .unwrap();
    let baseline = budget.usage().used;

    let proof = ProofMemory::admitted(Some(Arc::clone(&budget).into()), 8 * 1024 * 1024);
    let mut delivered = 0;
    let (visited, peak, retained) = measured(|| {
        verifier.visit_relational_index_postings::<Error>(
            "wide",
            "by_key",
            std::ops::Bound::Unbounded,
            false,
            &proof,
            &mut || Ok(()),
            &mut |_| {
                delivered += 1;
                Ok(true)
            },
        )
    });
    visited.unwrap();
    assert_eq!(delivered, records.len());
    let admitted = proof.peak();
    eprintln!(
        "reader allocation oracle: measured_peak={peak} retained_at_end={retained} admitted_peak={admitted} accountant_peak={}",
        budget.high_water_for_test()
    );
    assert!(
        peak <= admitted,
        "the walk allocated more than its pool admitted: measured={peak} admitted={admitted}"
    );
    drop(proof);

    // The other direction: the admission is not a phantom. Half the pool it
    // admitted cannot carry the same walk to the end, so those bytes stood for
    // memory the walk genuinely needed.
    let halved = ProofMemory::admitted(Some(Arc::clone(&budget).into()), admitted / 2);
    let mut partial = 0;
    verifier
        .visit_relational_index_postings::<Error>(
            "wide",
            "by_key",
            std::ops::Bound::Unbounded,
            false,
            &halved,
            &mut || Ok(()),
            &mut |_| {
                partial += 1;
                Ok(true)
            },
        )
        .expect("a walk that runs out of pool stops, it does not fail");
    assert!(
        partial < delivered,
        "half the admitted pool cannot carry the whole walk: partial={partial} whole={delivered}"
    );
    drop(halved);

    drop(verifier);
    assert_eq!(
        budget.usage().used,
        baseline,
        "the reader returns every admitted byte"
    );
}

// ---------------------------------------------------------------------------
// Family reset: bounded, admitted, inside the same write transaction.
//
// A reset must delete the selected families' proof nodes in bounded batches it
// has admitted, never by materialising every selected key at once. The oracle
// measures what the reset really holds; the accountant says what it admitted.
// Two scales of committed proof nodes separate a bounded peak from one that
// grows with the store.
// ---------------------------------------------------------------------------

fn reset_family_fixture(path: &std::path::Path, nodes: u64) -> ProofFixtureDatabase {
    let database = ProofFixtureDatabase::create(path).unwrap();
    // The family this test resets, plus a second family that must survive it
    // untouched.
    let mut records = (0..nodes)
        .map(|row| wide_posting(row, 512))
        .collect::<Vec<_>>();
    records.extend((0..16_u64).map(|row| {
        ContentIdentityMutation::put(
            LogicalFamily::Graph,
            format!("edge-{row:08}").into_bytes(),
            b"kept".to_vec(),
        )
    }));
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();
    database
}

fn family_root(database: &redb::Database, family: LogicalFamily) -> Vec<u8> {
    let read = database.begin_read().unwrap();
    let roots = read.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
    roots
        .get(family as u8)
        .unwrap()
        .expect("a stored family root")
        .value()
        .to_vec()
}

fn family_node_count(database: &redb::Database, family: LogicalFamily) -> usize {
    let read = database.begin_read().unwrap();
    let nodes = read.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    nodes
        .iter()
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .0
                .value()
                .first()
                .is_some_and(|tag| *tag == family as u8)
        })
        .count()
}

#[test]
#[serial]
fn a_family_reset_admits_its_batches_and_its_peak_does_not_grow_with_the_store() {
    let mut measurements = Vec::new();
    for nodes in [256_u64, 1024] {
        let root = tempfile::tempdir().unwrap();
        let database = reset_family_fixture(&root.path().join("reset.db"), nodes);
        let selected = LogicalFamily::SchemaAndIndexes;
        let kept_root = family_root(&database, LogicalFamily::Graph);
        let kept_nodes = family_node_count(&database, LogicalFamily::Graph);
        let selected_nodes = family_node_count(&database, selected);
        assert!(
            selected_nodes > 0,
            "the family under reset has stored nodes"
        );

        let budget = accountant(64 * 1024 * 1024);
        let mut admission = database.admission(Some(Arc::clone(&budget).into()));
        let write = database.begin_write().unwrap();
        let (result, peak, _) = measured(|| {
            RedbPersistence::reset_content_identity_families_in_write(
                &write,
                &[selected],
                &mut admission,
            )
        });
        result.expect("an admitted family reset succeeds");
        let admitted = admission.peak();
        write.commit().unwrap();
        eprintln!(
            "family reset oracle: nodes={nodes} stored_nodes={selected_nodes} \
             measured_peak={peak} admitted_peak={admitted}"
        );
        // The keys the reset holds are admitted before they are cloned, so the
        // accountant reports a real charge rather than nothing at all.
        assert!(
            admitted > 0,
            "the reset admitted nothing for the keys it held: nodes={nodes}"
        );
        // And the admission is not a phantom: it never charges for memory the
        // process did not actually hold.
        assert!(
            admitted <= peak,
            "the reset charges for memory it never held: nodes={nodes} \
             admitted={admitted} measured={peak}"
        );
        assert_eq!(
            family_node_count(&database, selected),
            0,
            "every selected node is deleted"
        );
        assert_eq!(
            family_root(&database, selected),
            encode_root(&empty_root(selected)),
            "the selected family's root is the canonical empty one"
        );
        assert_eq!(
            family_root(&database, LogicalFamily::Graph),
            kept_root,
            "an unselected family's root is untouched"
        );
        assert_eq!(
            family_node_count(&database, LogicalFamily::Graph),
            kept_nodes,
            "an unselected family keeps every node"
        );
        drop(admission);
        assert_eq!(budget.usage().used, 0, "the reset returns its admission");
        measurements.push((nodes, peak, admitted));
    }

    let (small_nodes, small_peak, small_admitted) = measurements[0];
    let (large_nodes, large_peak, large_admitted) = measurements[1];
    // Four times the proof nodes must not cost four times the memory: the
    // deletion walks the family in bounded batches inside the same write
    // transaction.
    assert!(
        large_peak <= small_peak.saturating_mul(2),
        "the reset's peak grows with the store instead of staying bounded: \
         {small_nodes} nodes peaked at {small_peak}, {large_nodes} nodes at {large_peak}"
    );
    assert!(
        large_admitted <= small_admitted.saturating_mul(2),
        "the reset's admission grows with the store: {small_nodes} nodes admitted \
         {small_admitted}, {large_nodes} nodes admitted {large_admitted}"
    );
}

/// The batch charge is real: a budget that cannot admit one batch refuses with
/// the existing typed error, the transaction is abandoned, and not one stored
/// byte of the proof changes.
#[test]
#[serial]
fn a_refused_family_reset_changes_no_stored_byte_and_leaves_nothing_admitted() {
    let root = tempfile::tempdir().unwrap();
    let database = reset_family_fixture(&root.path().join("refused-reset.db"), 512);
    let selected = LogicalFamily::SchemaAndIndexes;
    let before_root = family_root(&database, selected);
    let before_nodes = family_node_count(&database, selected);

    let budget = accountant(64);
    let mut admission = database.admission(Some(Arc::clone(&budget).into()));
    let write = database.begin_write().unwrap();
    let refused = RedbPersistence::reset_content_identity_families_in_write(
        &write,
        &[selected],
        &mut admission,
    );
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "a reset that cannot admit its batch is the typed budget refusal: {refused:?}"
    );
    drop(write);
    drop(admission);
    assert_eq!(
        budget.usage().used,
        0,
        "a refused reset leaves nothing admitted"
    );
    assert_eq!(family_root(&database, selected), before_root);
    assert_eq!(family_node_count(&database, selected), before_nodes);
}

// Fixed inline address bookkeeping extends the existing allocator oracle.
// Allocator callbacks do not allocate, borrow heap state, or synthesize frees.
#[derive(Clone, Copy, Debug)]
struct ExportIdentitySlot {
    original: u64,
    artifact: u64,
    storage: u64,
    is_base: bool,
    digest: [u8; 32],
    generation_key: [u8; 512],
    generation_len: usize,
    root_key: [u8; 512],
    root_len: usize,
}
impl ExportIdentitySlot {
    const EMPTY: Self = Self {
        original: 0,
        artifact: 0,
        storage: 0,
        is_base: false,
        digest: [0; 32],
        generation_key: [0; 512],
        generation_len: 0,
        root_key: [0; 512],
        root_len: 0,
    };
}
#[derive(Clone, Copy, Debug)]
struct GraphAllocationState {
    identities: [ExportIdentitySlot; 8],
    identity_count: usize,
    pointers: [usize; 8],
    capacities: [usize; 8],
    freed: [bool; 8],
    count: usize,
    originals: usize,
    selected_base: usize,
    selected_change: usize,
    deallocations: usize,
    normal: usize,
    repair: usize,
    retry: usize,
    violation: bool,
    overflow: bool,
}
impl GraphAllocationState {
    const EMPTY: Self = Self {
        identities: [ExportIdentitySlot::EMPTY; 8],
        identity_count: 0,
        pointers: [0; 8],
        capacities: [0; 8],
        freed: [false; 8],
        count: 0,
        originals: 0,
        selected_base: 0,
        selected_change: 0,
        deallocations: 0,
        normal: 0,
        repair: 0,
        retry: 0,
        violation: false,
        overflow: false,
    };
}
thread_local! {
    static GRAPH_ALLOCATIONS: std::cell::Cell<Option<GraphAllocationState>> = const { std::cell::Cell::new(None) };
}
fn graph_buffer_deallocated(pointer: usize) {
    let _ = GRAPH_ALLOCATIONS.try_with(|slot| {
        if let Some(mut state) = slot.get() {
            let mut watched = false;
            for index in 0..state.count {
                if state.pointers[index] == pointer && !state.freed[index] {
                    state.freed[index] = true;
                    watched = true;
                }
            }
            if watched {
                state.deallocations += 1;
            }
            slot.set(Some(state));
        }
    });
}
pub(crate) struct GraphAllocationGuard(Option<GraphAllocationState>);
impl Drop for GraphAllocationGuard {
    fn drop(&mut self) {
        GRAPH_ALLOCATIONS.with(|slot| slot.set(self.0.take()));
    }
}
pub(crate) fn watch_graph_allocations() -> GraphAllocationGuard {
    GraphAllocationGuard(
        GRAPH_ALLOCATIONS.with(|slot| slot.replace(Some(GraphAllocationState::EMPTY))),
    )
}
pub(crate) fn graph_workspace_event(
    event: &crate::vector_observations::VectorGraphWorkspaceEventForTest,
) {
    use crate::vector_observations::{
        VectorGraphBufferKindForTest as Kind, VectorGraphWorkspaceEventForTest as Event,
    };
    GRAPH_ALLOCATIONS.with(|slot| {
        let mut state = slot.get().expect("allocator observation scope");
        match *event {
            Event::GraphBuffers {
                kind,
                pointer,
                capacity,
            } => {
                if state.count == state.pointers.len() {
                    state.overflow = true;
                } else {
                    state.pointers[state.count] = pointer;
                    state.capacities[state.count] = capacity;
                    state.count += 1;
                }
                match kind {
                    Kind::OriginalEnvelope => state.originals += 1,
                    Kind::SelectedBase => state.selected_base += 1,
                    Kind::SelectedChange => state.selected_change += 1,
                }
            }
            Event::ExportIdentity {
                original_generation_id,
                artifact_generation_id,
                storage_id,
                is_base,
                source_digest,
                generation_key_pointer,
                generation_key_len,
                root_key_pointer,
                root_key_len,
            } => {
                if state.identity_count == state.identities.len()
                    || generation_key_len > 512
                    || root_key_len > 512
                {
                    state.overflow = true;
                } else {
                    let mut identity = ExportIdentitySlot::EMPTY;
                    identity.original = original_generation_id;
                    identity.artifact = artifact_generation_id;
                    identity.storage = storage_id;
                    identity.is_base = is_base;
                    identity.digest = source_digest;
                    identity.generation_len = generation_key_len;
                    identity.root_len = root_key_len;
                    // The producer lends exact key bytes only for this synchronous callback.
                    unsafe {
                        identity.generation_key[..generation_key_len].copy_from_slice(
                            std::slice::from_raw_parts(
                                generation_key_pointer as *const u8,
                                generation_key_len,
                            ),
                        );
                        identity.root_key[..root_key_len].copy_from_slice(
                            std::slice::from_raw_parts(root_key_pointer as *const u8, root_key_len),
                        );
                    }
                    state.identities[state.identity_count] = identity;
                    state.identity_count += 1;
                }
            }
            Event::WorkspaceResize {
                previous,
                next,
                operation,
            } => {
                if next >= previous {
                    state.violation = true;
                }
                if state.freed[..state.count].iter().any(|freed| !freed) {
                    state.violation = true;
                }
                match operation {
                    "normal_cleanup" => state.normal += 1,
                    "repair_cleanup" => state.repair += 1,
                    "retry" => state.retry += 1,
                    _ => state.violation = true,
                }
            }
        }
        slot.set(Some(state));
    });
}
pub(crate) fn assert_graph_freed_before_workspace_refund(repair: bool) {
    GRAPH_ALLOCATIONS.with(|slot| {
        let state = slot.get().expect("allocator observation scope");
        assert!(
            !state.overflow,
            "fixed oracle slots cover this smallest fixture: {state:?}"
        );
        assert!(
            state.originals > 0 && state.selected_base > 0,
            "actual graph addresses observed: {state:?}"
        );
        assert!(
            state.capacities[..state.count]
                .iter()
                .all(|capacity| *capacity > 0)
        );
        assert!(
            state.deallocations > 0 && state.freed[..state.count].iter().all(|freed| *freed),
            "actual allocator retirements observed: {state:?}"
        );
        assert!(
            if repair {
                state.repair > 0
            } else {
                state.normal > 0
            },
            "requested cleanup branch reached its actual refund: {state:?}"
        );
        assert!(
            !state.violation,
            "every actual graph allocation retires BEFORE every reservation decrease: {state:?}"
        );
    });
}

pub(crate) fn assert_saved_export_identities(expected: usize) {
    GRAPH_ALLOCATIONS.with(|slot| {
        let state = slot.get().expect("identity observation scope");
        assert_eq!(state.identity_count, expected, "four failed exports and one real successful export");
        let identities = &state.identities[..state.identity_count];
        for (attempt, identity) in identities.iter().enumerate() {
            assert_eq!(identity.artifact, identity.storage);
            assert!(identity.artifact > identity.original);
            assert_eq!(identity.original, identities[0].original);
            assert_eq!(identity.digest, identities[0].digest, "captured producer topology digest stays identical");
            assert!(identity.is_base && identity.generation_len > 0 && identity.root_len > 0);
            for previous in &identities[..attempt] {
                assert!(identity.artifact > previous.artifact);
                assert_ne!(&identity.generation_key[..identity.generation_len], &previous.generation_key[..previous.generation_len]);
                assert_ne!(&identity.root_key[..identity.root_len], &previous.root_key[..previous.root_len]);
            }
            eprintln!("RUNTIME_PROOF_IDENTITY attempt={} original={} artifact={} storage={} source_digest={:?} generation_key={:?} root_key={:?}", attempt, identity.original, identity.artifact, identity.storage, identity.digest, &identity.generation_key[..identity.generation_len], &identity.root_key[..identity.root_len]);
        }
    });
}

// Owner-authorized bounded proof exception: canonical vector tests do not
// observe allocator Layout. This uses the existing aggregate GlobalAlloc
// oracle and preserves every existing test/assertion.
#[test]
#[serial]
fn writer_adapter_arc_request_matches_pinned_layout() {
    crate::memory_accounting::native_allocation_lifetime_rows_for_test();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let native_process_before = crate::memory_accounting::resident_insertion_runtime_snapshot()
        .map_or(0, |runtime| runtime.1);
    let constructor_process_baseline = crate::memory_accounting::process_aggregate_bytes_for_test();
    LAST_REQUEST.with(|last| last.set((0, 0, 0)));
    let (root, _, _) = measured(|| {
        crate::memory_accounting::WriterBudget::from_accountant(accountant.clone()).unwrap()
    });
    let root_request = LAST_REQUEST.with(std::cell::Cell::get);
    let root_pointer = LAST_REQUEST_POINTER.with(std::cell::Cell::get);
    assert!(
        root_request.2 >= 2,
        "the constructor observes registration and startup Arc requests"
    );
    let constructor_process_after = crate::memory_accounting::process_aggregate_bytes_for_test();
    let expected = crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
        .writer_vector_adapter_layout_for_test();
    let (node, node_bytes, epoch, users, used_counter) =
        accountant.writer_registration_observation_for_test();
    assert_ne!(epoch, 0);
    assert_eq!(
        users, 2,
        "construction root and its one adapter are counted"
    );
    assert_eq!(
        (root_request.0, root_request.1),
        (expected.size(), expected.align()),
        "the actual final startup request is its typed adapter backing"
    );
    assert_eq!(
        constructor_process_after,
        constructor_process_baseline + node_bytes + root_request.0,
        "startup registration and the actual root adapter invoice join the aggregate"
    );
    assert_eq!(
        accountant.writer_fixed_ownership().2,
        0,
        "the startup adapter is independently process-funded"
    );
    // Nested mapped rows may install their own retained native runtime; capture
    // their completed process baseline separately from this root's liabilities.
    let process_before_extra_adapter = crate::memory_accounting::process_aggregate_bytes_for_test();
    let process_final_baseline = process_before_extra_adapter - node_bytes - root_request.0;
    let native_process_after = crate::memory_accounting::resident_insertion_runtime_snapshot()
        .map_or(0, |runtime| runtime.1);
    assert_eq!(
        process_final_baseline,
        constructor_process_baseline + native_process_after - native_process_before,
        "the nested mapped rows retain only their separately invoiced process runtime"
    );
    WATCH_NODE_FREE.with(|slot| slot.set((node, used_counter, 0, 0, false)));
    LAST_REQUEST.with(|last| last.set((0, 0, 0)));
    let (adapter, _, _) = measured(|| {
        crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
            .vector_budget()
            .unwrap()
    });
    let (size, align, count) = LAST_REQUEST.with(std::cell::Cell::get);
    assert!(
        count >= 1,
        "existing oracle observed actual allocator requests"
    );
    // Test-seam admission formats its ledger name before construction. The
    // factory's final allocating operation is Arc::new(adapter); record its
    // unfiltered actual request, not a request selected by expected Layout.
    assert_eq!((size, align), (expected.size(), expected.align()));
    assert_eq!(accountant.writer_fixed_ownership().2, expected.size());
    assert_eq!(
        crate::memory_accounting::process_aggregate_bytes_for_test(),
        process_before_extra_adapter + size
    );
    watch_registry_free_for_test(root_pointer, used_counter);
    drop(root);
    let root_free = registry_free_observation_for_test(true);
    assert_eq!(
        root_free,
        (node_bytes + size, node_bytes + size, true),
        "the original writer remains funded through the actual process-funded startup Arc free"
    );
    // This fixed ordinary std Arc uses its recorded immediate-before-free
    // correction refund; native accountant/limit/counter rows require free first.
    let process_after_root = process_before_extra_adapter + size - root_request.0;
    assert_eq!(
        registry_process_free_observation_for_test(),
        (process_after_root, process_after_root)
    );
    WATCH_NODE_FREE.with(|slot| slot.set((node, used_counter, 0, 0, false)));
    println!(
        "ROOT_ADAPTER_CUSTODY_WITNESS actual_request={root_request:?} actual_pointer={root_pointer} actual_free={root_free:?} process_startup_delta={} nested_native_invoice={} original_writer_extra_single_layout=true",
        node_bytes + root_request.0,
        native_process_after - native_process_before
    );
    assert_eq!(
        crate::memory_accounting::process_aggregate_bytes_for_test(),
        process_before_extra_adapter + size - root_request.0
    );
    assert_ne!(
        accountant.writer_fixed_ownership().0,
        0,
        "escaped adapter keeps epoch live"
    );
    drop(adapter);
    assert_eq!(accountant.writer_fixed_ownership(), (0, 0, 0));
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(
        crate::memory_accounting::process_aggregate_bytes_for_test(),
        process_final_baseline
    );
    let (_, _, before_free, after_free, freed) = WATCH_NODE_FREE.with(std::cell::Cell::get);
    WATCH_NODE_FREE.with(|slot| slot.set((0, 0, 0, 0, false)));
    assert!(
        freed,
        "existing oracle saw the actual registration Box deallocation"
    );
    assert_eq!(before_free, node_bytes + expected.size());
    assert_eq!(
        after_free,
        node_bytes + expected.size(),
        "node and adapter remain charged until node physical free returns"
    );
    let reopened =
        crate::memory_accounting::WriterBudget::from_accountant(accountant.clone()).unwrap();
    assert_ne!(
        accountant.writer_registration_observation_for_test().2,
        epoch
    );
    drop(reopened);
    assert_eq!(accountant.usage().used, 0);
    // Registration/control custody rows share this allocator witness. The
    // unchanged canonical tests cannot observe the zero-byte escape lifetime.
    let reader_adapter = crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
        .vector_budget()
        .unwrap();
    assert_eq!(accountant.writer_registration_observation_for_test().2, 0);
    drop(reader_adapter);
    accountant.try_allocate(37).unwrap();
    let first =
        crate::memory_accounting::WriterBudget::from_accountant(accountant.clone()).unwrap();
    let second =
        crate::memory_accounting::WriterBudget::from_accountant(accountant.clone()).unwrap();
    assert_eq!(accountant.writer_fixed_ownership().0, node_bytes);
    let mut escape =
        crate::memory_accounting::OwnedMemoryReservation::new(accountant.clone().into());
    drop(first);
    drop(second);
    assert_ne!(accountant.writer_registration_observation_for_test().2, 0);
    assert_eq!(accountant.usage().used, 37 + node_bytes);
    escape
        .try_grow_for(19, "writer", "retained_reservation", "test")
        .unwrap();
    drop(escape);
    assert_eq!(accountant.writer_registration_observation_for_test().2, 0);
    assert_eq!(accountant.usage().used, 37);
    accountant.release(37);
    assert_eq!(accountant.underflow_count_for_test(), 0);
    println!(
        "WRITER_REGISTRATION_FREE_WITNESS node_bytes={node_bytes} used_before_free={before_free} used_after_free={after_free} registration_refund_after_deallocation=true fresh_epoch=true"
    );
    println!(
        "WRITER_ADAPTER_LAYOUT_WITNESS request_size={size} request_align={align} request_count={count} helper_size={} helper_align={} final_used=0",
        expected.size(),
        expected.align()
    );
}

// Addenda 5-7 mapped rows are reached through the authorized layout seam.
// They return with their temporary owners gone and measurement disabled,
// before the unchanged aggregate witness begins its own measurement.
pub(crate) fn r1_mapped_control_rows() {
    r1_node_custody_rows();
    crate::database::r1_database_cleanup_rows_for_test();
    crate::persistence::writer_source::r1_schema_control_rows_for_test();
    use crate::memory_accounting::{
        OwnedMemoryReservation, StoreAccountantHandle, TransferredRowReservation, WriterBudget,
        writer_adapter_type_layout,
    };
    fn send_sync<T: Send + Sync>() {}
    send_sync::<StoreAccountantHandle>();
    send_sync::<Arc<MemoryAccountant>>();
    assert_eq!(
        std::mem::size_of::<StoreAccountantHandle>(),
        std::mem::size_of::<Arc<MemoryAccountant>>()
    );
    assert_eq!(
        std::mem::align_of::<StoreAccountantHandle>(),
        std::mem::align_of::<Arc<MemoryAccountant>>()
    );
    assert_eq!(
        std::mem::size_of::<Option<StoreAccountantHandle>>(),
        std::mem::size_of::<Option<Arc<MemoryAccountant>>>()
    );
    let owner = Arc::new(MemoryAccountant::no_limit());
    owner.try_allocate(17).unwrap();
    let untagged = StoreAccountantHandle::new(owner.clone().into());
    let root = WriterBudget::from_accountant(owner.clone()).unwrap();
    let epoch = owner.writer_registration_observation_for_test().2;
    let users = owner.writer_registration_observation_for_test().3;
    let plain_clone = untagged.clone();
    assert_eq!(
        owner.writer_registration_observation_for_test().3,
        users,
        "reader handle clone cannot acquire a later writer epoch"
    );
    drop(plain_clone);
    let tagged = StoreAccountantHandle::new(owner.clone().into());
    let tagged_clone = tagged.clone();
    assert_eq!(
        owner.writer_registration_observation_for_test().3,
        users + 2
    );
    assert_eq!(tagged.as_handle().as_ptr(), Arc::as_ptr(&owner));
    drop(root);
    assert_eq!(owner.writer_registration_observation_for_test().3, 2);
    drop(tagged);
    assert_eq!(owner.writer_registration_observation_for_test().3, 1);
    drop(tagged_clone);
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert_eq!(owner.usage().used, 17);
    let reopened = WriterBudget::from_accountant(owner.clone()).unwrap();
    assert_ne!(owner.writer_registration_observation_for_test().2, epoch);
    drop(reopened);
    drop(untagged);
    assert_eq!(owner.usage().used, 17);
    owner.release(17);
    assert_eq!(owner.usage().used, 0);
    println!(
        "CDB127_R1_ROW tagged_handle pointer_niche=true reader_clone_stays_reader=true original_funder=true fresh_epoch=true host_baseline=17"
    );

    let owner = Arc::new(MemoryAccountant::no_limit());
    let root = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    let baseline_controls = owner.writer_fixed_ownership().2;
    let reservation = OwnedMemoryReservation::try_new_for(
        owner.clone().into(),
        31,
        "proof",
        "r1_mapped_reservation",
        "proof",
    )
    .unwrap();
    LAST_REQUEST.with(|slot| slot.set((0, 0, 0)));
    let (allocation, _, _) = measured(|| reservation.into_admitted_arc().unwrap());
    let request = LAST_REQUEST.with(std::cell::Cell::get);
    let expected = writer_adapter_type_layout::<OwnedMemoryReservation>();
    assert_eq!(request, (expected.size(), expected.align(), 1));
    assert_eq!(
        owner.writer_fixed_ownership().2,
        baseline_controls + expected.size()
    );
    assert_eq!(owner.usage().used, baseline + 31 + expected.size());
    let cloned = allocation.clone();
    drop(allocation);
    assert_eq!(owner.usage().used, baseline + 31 + expected.size());
    drop(cloned);
    assert_eq!(owner.usage().used, baseline);
    assert_eq!(owner.writer_fixed_ownership().2, baseline_controls);
    println!(
        "CDB127_R1_ROW reservation_arc actual_size={} actual_align={} one_request=true payload=31 clone_single_debit=true final_baseline=true",
        request.0, request.1
    );

    let reservation = OwnedMemoryReservation::new(owner.clone().into());
    LAST_REQUEST.with(|slot| slot.set((0, 0, 0)));
    let (allocation, _, _) = measured(|| reservation.into_admitted_mutex_arc().unwrap());
    let request = LAST_REQUEST.with(std::cell::Cell::get);
    let expected = writer_adapter_type_layout::<parking_lot::Mutex<OwnedMemoryReservation>>();
    assert_eq!(request, (expected.size(), expected.align(), 1));
    assert_eq!(owner.usage().used, baseline + expected.size());
    drop(root);
    assert_ne!(
        owner.writer_registration_observation_for_test().2,
        0,
        "zero payload control retains its epoch"
    );
    drop(allocation);
    assert_eq!(owner.usage().used, 0);
    println!(
        "CDB127_R1_ROW reservation_mutex_arc actual_size={} actual_align={} zero_payload_keeps_epoch=true final_zero=true",
        request.0, request.1
    );

    let root = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    owner.try_allocate(23).unwrap();
    let control = TransferredRowReservation::prepare_control(
        &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
    )
    .unwrap();
    LAST_REQUEST.with(|slot| slot.set((0, 0, 0)));
    let (allocation, _, _) = measured(|| {
        TransferredRowReservation::from_admitted_with_control(owner.clone().into(), 23, control)
    });
    let request = LAST_REQUEST.with(std::cell::Cell::get);
    let expected = writer_adapter_type_layout::<TransferredRowReservation>();
    assert_eq!(request, (expected.size(), expected.align(), 1));
    assert_eq!(owner.usage().used, baseline + 23 + expected.size());
    let cloned = allocation.clone();
    drop(allocation);
    assert_eq!(owner.usage().used, baseline + 23 + expected.size());
    drop(cloned);
    assert_eq!(owner.usage().used, baseline);
    drop(root);
    assert_eq!(owner.usage().used, 0);
    println!(
        "CDB127_R1_ROW transferred_reservation actual_size={} actual_align={} original_credit=23 clone_single_debit=true final_zero=true",
        request.0, request.1
    );
    let reader = Arc::new(MemoryAccountant::no_limit());
    let reader_control = OwnedMemoryReservation::new(reader.clone().into())
        .into_admitted_arc()
        .unwrap();
    assert_eq!(reader.writer_registration_observation_for_test().2, 0);
    let reader_bytes = reader.usage().used;
    let later_writer = WriterBudget::from_accountant(reader.clone()).unwrap();
    let later_controls = reader.writer_fixed_ownership().2;
    let later_used = reader.usage().used;
    drop(reader_control);
    assert_eq!(
        reader.writer_fixed_ownership().2,
        later_controls,
        "an original reader control never subtracts a later writer epoch's receipt"
    );
    assert_eq!(reader.usage().used, later_used - reader_bytes);
    drop(later_writer);
    assert_eq!(reader.usage().used, 0);
    assert_eq!(reader.underflow_count_for_test(), 0);
    println!(
        "CDB127_R1_ROW control_admission_epoch original_reader_stays_reader=true later_writer_receipt_unchanged=true original_funder_final_zero=true"
    );
}

// Mapped custody/debit rows reuse this allocator and the original node layout.
fn r1_node_custody_rows() {
    use crate::memory_accounting::{
        StoreAccountantHandle, WriterBudget, writer_adapter_type_layout,
    };
    println!(
        "CDB127_R1_LAYOUT AdmittedBytes size={} align={} accountant={} bytes={}",
        std::mem::size_of::<AdmittedBytes>(),
        std::mem::align_of::<AdmittedBytes>(),
        std::mem::offset_of!(AdmittedBytes, accountant),
        std::mem::offset_of!(AdmittedBytes, bytes)
    );
    println!(
        "CDB127_R1_LAYOUT OptionAdmittedBytes size={} align={}",
        std::mem::size_of::<Option<AdmittedBytes>>(),
        std::mem::align_of::<Option<AdmittedBytes>>()
    );
    println!(
        "CDB127_R1_LAYOUT FloorCharge size={} align={} floor={} bytes={}",
        std::mem::size_of::<FloorCharge>(),
        std::mem::align_of::<FloorCharge>(),
        std::mem::offset_of!(FloorCharge, floor),
        std::mem::offset_of!(FloorCharge, bytes)
    );
    println!(
        "CDB127_R1_LAYOUT OptionFloorCharge size={} align={}",
        std::mem::size_of::<Option<FloorCharge>>(),
        std::mem::align_of::<Option<FloorCharge>>()
    );
    println!(
        "CDB127_R1_LAYOUT VerifiedReadNode size={} align={} node={} bytes={} charge={} floor={} allowance={}",
        std::mem::size_of::<VerifiedReadNode>(),
        std::mem::align_of::<VerifiedReadNode>(),
        std::mem::offset_of!(VerifiedReadNode, node),
        std::mem::offset_of!(VerifiedReadNode, bytes),
        std::mem::offset_of!(VerifiedReadNode, _charge),
        std::mem::offset_of!(VerifiedReadNode, _floor),
        VERIFIED_READ_NODE_HANDLE_BYTES
    );
    let owner = Arc::new(MemoryAccountant::no_limit());
    let root = WriterBudget::from_accountant(owner.clone()).unwrap();
    let registration = owner.writer_registration_observation_for_test();
    let baseline = owner.usage().used;
    let floor = Arc::new(AtomicUsize::new(64));
    let payload = Arc::new(Node {
        level: 0,
        entries: vec![PageEntry {
            key: vec![9],
            value_hash: [0; 32],
        }],
        subtree_hash: [0; 32],
    });
    let payload_allocation = (Arc::as_ptr(&payload) as usize) - 2 * std::mem::size_of::<usize>();
    WATCH_PAYLOAD_FREE.with(|slot| slot.set((payload_allocation, false, false)));
    WATCH_NODE_FREE.with(|slot| slot.set((registration.0, registration.4, 0, 0, false)));
    let source = StoreAccountantHandle::retain_if_writer(
        &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
    )
    .unwrap();
    LAST_REQUEST.with(|slot| slot.set((0, 0, 0)));
    let (unaccounted, _, _) = measured(|| {
        Arc::new(VerifiedReadNode {
            node: payload.clone(),
            bytes: 64,
            _charge: AdmittedBytes::with_source(Some(source), None),
            _floor: None,
        })
    });
    let request = LAST_REQUEST.with(std::cell::Cell::get);
    let expected = writer_adapter_type_layout::<VerifiedReadNode>();
    assert_eq!(request, (expected.size(), expected.align(), 1));
    assert_eq!(owner.usage().used, baseline, "zero debit is custody only");
    let retained = Arc::new(VerifiedReadNode {
        node: payload.clone(),
        bytes: 64,
        _charge: AdmittedBytes::with_source(
            unaccounted
                ._charge
                .as_ref()
                .and_then(AdmittedBytes::source_custody),
            None,
        ),
        _floor: Some(FloorCharge {
            floor: floor.clone(),
            bytes: 64,
        }),
    });
    drop(unaccounted);
    drop(payload);
    drop(root);
    assert_ne!(
        owner.writer_registration_observation_for_test().2,
        0,
        "floor borrower retains source after source/cache owners leave"
    );
    assert_eq!(floor.load(AtomicOrdering::SeqCst), 64);
    drop(retained);
    assert_eq!(floor.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(owner.usage().used, 0);
    let payload_watch = WATCH_PAYLOAD_FREE.with(std::cell::Cell::get);
    assert!(
        payload_watch.1 && payload_watch.2,
        "actual payload Arc free precedes final source-use registration free"
    );
    assert!(WATCH_NODE_FREE.with(std::cell::Cell::get).4);
    WATCH_NODE_FREE.with(|slot| slot.set((0, 0, 0, 0, false)));
    WATCH_PAYLOAD_FREE.with(|slot| slot.set((0, false, false)));
    println!(
        "CDB127_R1_ROW node_custody actual_size={} actual_align={} one_request=true zero_debit=true floor_borrower=true payload_free_before_source_use=true final_original_funder_zero=true",
        request.0, request.1
    );

    let root = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    let mut admission = ProofAdmission::new(Some(owner.clone().into()));
    admission.admit(64).unwrap();
    let source = StoreAccountantHandle::retain_if_writer(
        &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
    );
    let charge = AdmittedBytes::with_source(source, Some(admission.detach(64))).unwrap();
    assert_eq!(
        owner.usage().used,
        baseline + 64,
        "coalescing cannot refund the positive debit early"
    );
    assert_eq!(charge.bytes, 64);
    drop(root);
    assert_ne!(owner.writer_registration_observation_for_test().2, 0);
    drop(charge);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!(
        "CDB127_R1_ROW node_positive_debit source_payer_identity=true original_debit=64 early_refund=false final_zero=true underflow=0"
    );
}

pub(crate) fn last_request_for_test(reset: bool) -> (usize, usize, usize) {
    let request = LAST_REQUEST.with(std::cell::Cell::get);
    if reset {
        LAST_REQUEST.with(|slot| slot.set((0, 0, 0)));
    }
    request
}

// Fixed, nonallocating metadata in the existing oracle. The caller selects
// the sole actual allocation in a measured production operation; identity is
// never selected by matching an expected layout.
pub(crate) fn last_request_pointer_for_test() -> usize {
    LAST_REQUEST_POINTER.with(std::cell::Cell::get)
}
pub(crate) fn watch_registry_free_for_test(pointer: usize, counter: usize) {
    WATCH_NODE_FREE.with(|slot| slot.set((pointer, counter, 0, 0, false)));
}
pub(crate) fn registry_free_observation_for_test(clear: bool) -> (usize, usize, bool) {
    let observed = WATCH_NODE_FREE.with(std::cell::Cell::get);
    if clear {
        WATCH_NODE_FREE.with(|slot| slot.set((0, 0, 0, 0, false)));
    }
    (observed.2, observed.3, observed.4)
}

// Production-dead observation through the existing allocator, keyed by the
// initialized candidate Vec pointer, immediately after System.dealloc returns.
pub(crate) fn watch_candidate_payload_for_test(pointer: usize, accountant: &MemoryAccountant) {
    WATCH_PAYLOAD_FREE.with(|slot| slot.set((pointer, false, false)));
    WATCH_PAYLOAD_ACCOUNTANT.with(|slot| slot.set(accountant as *const MemoryAccountant as usize));
    PAYLOAD_FREE_CENSUS.with(|slot| slot.set((0, 0, 0, 0)));
    let registration = accountant.writer_registration_observation_for_test();
    WATCH_NODE_FREE.with(|slot| slot.set((registration.0, registration.4, 0, 0, false)));
}
pub(crate) fn candidate_payload_free_observation_for_test() -> (
    (usize, usize, usize, usize),
    (bool, bool),
    (usize, usize, bool),
) {
    let census = PAYLOAD_FREE_CENSUS.with(std::cell::Cell::get);
    let payload = WATCH_PAYLOAD_FREE.with(std::cell::Cell::get);
    let node = WATCH_NODE_FREE.with(std::cell::Cell::get);
    WATCH_PAYLOAD_FREE.with(|slot| slot.set((0, false, false)));
    WATCH_PAYLOAD_ACCOUNTANT.with(|slot| slot.set(0));
    WATCH_NODE_FREE.with(|slot| slot.set((0, 0, 0, 0, false)));
    (census, (payload.1, payload.2), (node.2, node.3, node.4))
}

// No allocation, counter mutation or pointer dereference in this process invoice.
pub(crate) fn registry_process_free_observation_for_test() -> (usize, usize) {
    WATCH_NODE_PROCESS_FREE.with(std::cell::Cell::get)
}

// A forwarding test capability, rather than a second accountant: every
// invoice is paid by the native child supplied by the fixture. The existing
// allocator oracle can read `used` while observing actual payload frees.
#[derive(Debug)]
pub(crate) struct MaintenanceReaderSpy {
    pub(crate) source: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    pub(crate) writer: crate::memory_accounting::AccountantHandle,
    pub(crate) used: AtomicUsize,
    pub(crate) peak: AtomicUsize,
    pub(crate) writer_peak: AtomicUsize,
    pub(crate) requests: AtomicUsize,
    pub(crate) decode_requests: AtomicUsize,
    pub(crate) wrong_funder: AtomicUsize,
    pub(crate) fail_at: AtomicUsize,
    pub(crate) retained_at_failure: AtomicUsize,
    pub(crate) live_payload_owners_at_failure: AtomicUsize,
    pub(crate) payload_allocations: AtomicUsize,
    pub(crate) payload_frees: AtomicUsize,
    pub(crate) early_payload_refunds: AtomicUsize,
    pub(crate) allocation_watch_overflow: AtomicUsize,
    owners: parking_lot::Mutex<Vec<std::sync::Weak<crate::read_image_memory::ImageMemory>>>,
}

impl MaintenanceReaderSpy {
    pub(crate) fn assert_payload_lifetime(&self) {
        assert_eq!(
            self.allocation_watch_overflow.load(AtomicOrdering::SeqCst),
            0,
            "the fixed native allocation observer never loses a live block"
        );
        assert!(
            self.payload_allocations.load(AtomicOrdering::SeqCst) > 0,
            "observe actual allocations paired with successful native-child invoices"
        );
        assert_eq!(
            self.payload_allocations.load(AtomicOrdering::SeqCst),
            self.payload_frees.load(AtomicOrdering::SeqCst),
            "every observed temporary native allocation has physically freed"
        );
        assert_eq!(
            self.early_payload_refunds.load(AtomicOrdering::SeqCst),
            0,
            "each image owner refunds its child only after its own observed payload allocations physically free"
        );
    }

    pub(crate) fn new(
        source: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Arc<Self> {
        Arc::new(Self {
            source,
            writer,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            writer_peak: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            decode_requests: AtomicUsize::new(0),
            wrong_funder: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(0),
            retained_at_failure: AtomicUsize::new(0),
            live_payload_owners_at_failure: AtomicUsize::new(0),
            payload_allocations: AtomicUsize::new(0),
            payload_frees: AtomicUsize::new(0),
            early_payload_refunds: AtomicUsize::new(0),
            allocation_watch_overflow: AtomicUsize::new(0),
            owners: parking_lot::Mutex::new(Vec::with_capacity(64)),
        })
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for MaintenanceReaderSpy {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        let request = self.requests.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        self.writer_peak
            .fetch_max(self.writer.usage().used, AtomicOrdering::SeqCst);
        if subsystem == "reader" && operation == "materialize_answer" {
            self.decode_requests.fetch_add(1, AtomicOrdering::SeqCst);
            if crate::read_image_memory::current_reader_budget().is_some() {
                let owner = crate::read_image_memory::active_owner()?;
                let funder = owner.reader_budget().expect("reader-funded image");
                if !std::ptr::eq(
                    Arc::as_ptr(&funder) as *const (),
                    self as *const Self as *const (),
                ) {
                    self.wrong_funder.fetch_add(1, AtomicOrdering::SeqCst);
                }
                maintenance_owner_control(Arc::as_ptr(&owner) as usize, self);
                let mut owners = self.owners.lock();
                owners.retain(|owner| owner.strong_count() != 0);
                if !owners
                    .iter()
                    .any(|seen| seen.as_ptr() == Arc::as_ptr(&owner))
                {
                    assert!(
                        owners.len() < owners.capacity(),
                        "fixed spy slots cover live image owners"
                    );
                    owners.push(Arc::downgrade(&owner));
                }
            }
        }
        let live_payload_owners = self
            .owners
            .lock()
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .filter(|owner| owner.retained_payload_bytes() != 0)
            .count();
        let fail_at = self.fail_at.load(AtomicOrdering::SeqCst);
        if fail_at == request || (fail_at == usize::MAX && live_payload_owners >= 2) {
            MAINTENANCE_INVOICES.with(|tickets| tickets.set([(0, 0, 0); 16]));
            self.retained_at_failure.store(
                self.used.load(AtomicOrdering::SeqCst),
                AtomicOrdering::SeqCst,
            );
            self.live_payload_owners_at_failure
                .store(live_payload_owners, AtomicOrdering::SeqCst);
            return Err(contextdb_core::Error::Other(
                "injected reader invoice failure".into(),
            ));
        }
        // A failed invoice's typed error belongs to its caller, not to the
        // decoded row. Do not identify those error strings as row payloads.
        let pending_invoices =
            MAINTENANCE_INVOICES.with(|tickets| tickets.replace([(0, 0, 0); 16]));
        self.source.try_reserve(bytes, subsystem, operation, hint)?;
        MAINTENANCE_INVOICES.with(|tickets| tickets.set(pending_invoices));
        let used = self.used.fetch_add(bytes, AtomicOrdering::SeqCst) + bytes;
        self.peak.fetch_max(used, AtomicOrdering::SeqCst);
        if subsystem == "reader"
            && operation == "materialize_answer"
            && crate::read_image_memory::current_reader_budget().is_some()
        {
            let owner = crate::read_image_memory::active_owner()?;
            // The first reserve on an empty image is codec or image control,
            // not payload. Later heap admissions are matched to actual native
            // allocations, including a Vec allocated after its element strings.
            if owner.retained_payload_bytes() != 0 {
                MAINTENANCE_INVOICES.with(|tickets| {
                    let mut pending = tickets.get();
                    if let Some(ticket) = pending.iter_mut().find(|ticket| ticket.0 == 0) {
                        *ticket = (
                            Arc::as_ptr(&owner) as usize,
                            self as *const Self as usize,
                            bytes,
                        );
                    } else {
                        self.allocation_watch_overflow
                            .fetch_add(1, AtomicOrdering::SeqCst);
                    }
                    tickets.set(pending);
                });
            }
        }
        Ok(())
    }

    fn release(&self, bytes: usize) {
        // Codec control returns when decoding finishes. Any unmatched
        // conservative heap admission cannot tag later storage writes.
        MAINTENANCE_INVOICES.with(|tickets| tickets.set([(0, 0, 0); 16]));
        // During ImageMemory::drop its weak pointer has zero strong owners,
        // but the spy and the native child are still alive. Check the actual
        // allocation census BEFORE forwarding that owner's refund.
        self.owners.lock().retain(|owner| {
            if owner.strong_count() == 0 {
                maintenance_owner_refund(owner.as_ptr() as usize, self);
                false
            } else {
                true
            }
        });
        let before = self.used.fetch_sub(bytes, AtomicOrdering::SeqCst);
        assert!(before >= bytes, "each actual reader invoice refunds once");
        self.source.release(bytes);
    }

    fn funding_owner_for_test(
        &self,
    ) -> Option<&Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        Some(&self.source)
    }
}

// Extend this allocator's existing observation surface. A ticket pairs the
// *actual successful native-child invoice* with an admitted native allocation
// request. In these fixtures that identifies decoded strings and component Vecs;
// it never tags later redb page buffers or guesses a native Arc layout.
#[derive(Clone, Copy)]
struct MaintenancePayloadSlot {
    pointer: usize,
    bytes: usize,
    owner: usize,
    funder: usize,
}
impl MaintenancePayloadSlot {
    const EMPTY: Self = Self {
        pointer: 0,
        bytes: 0,
        owner: 0,
        funder: 0,
    };
}
thread_local! {
    static MAINTENANCE_INVOICES: std::cell::Cell<[(usize, usize, usize); 16]> = const { std::cell::Cell::new([(0, 0, 0); 16]) };
    static MAINTENANCE_PAYLOADS: std::cell::Cell<[MaintenancePayloadSlot; 512]> = const { std::cell::Cell::new([MaintenancePayloadSlot::EMPTY; 512]) };
}
fn maintenance_payload_allocated(pointer: usize, bytes: usize) {
    let ticket = MAINTENANCE_INVOICES
        .try_with(|tickets| {
            let mut pending = tickets.get();
            // Exact owned-string invoices take precedence over a conservative Vec
            // admission that precedes decoding those strings. Both comparisons use
            // actual producer invoices and actual allocator requests, not layouts.
            let chosen = pending
                .iter()
                .position(|ticket| ticket.0 != 0 && ticket.2 == bytes)
                .or_else(|| {
                    pending
                        .iter()
                        .position(|ticket| ticket.0 != 0 && bytes <= ticket.2)
                });
            let ticket = chosen
                .map(|index| {
                    let ticket = pending[index];
                    pending[index] = (0, 0, 0);
                    ticket
                })
                .unwrap_or((0, 0, 0));
            tickets.set(pending);
            ticket
        })
        .unwrap_or((0, 0, 0));
    if ticket.0 == 0 {
        return;
    }
    let spy = unsafe { &*(ticket.1 as *const MaintenanceReaderSpy) };
    MAINTENANCE_PAYLOADS.with(|slots| {
        let mut entries = slots.get();
        if let Some(slot) = entries.iter_mut().find(|slot| slot.pointer == 0) {
            *slot = MaintenancePayloadSlot {
                pointer,
                bytes,
                owner: ticket.0,
                funder: ticket.1,
            };
            spy.payload_allocations.fetch_add(1, AtomicOrdering::SeqCst);
        } else {
            spy.allocation_watch_overflow
                .fetch_add(1, AtomicOrdering::SeqCst);
        }
        slots.set(entries);
    });
}
fn maintenance_owner_control(owner: usize, spy: &MaintenanceReaderSpy) {
    MAINTENANCE_PAYLOADS.with(|slots| {
        let mut entries = slots.get();
        // Identify an image-control block by the initialized owner's actual
        // address lying inside its observed allocation, with no header offset.
        if let Some(slot) = entries.iter_mut().find(|slot| {
            slot.pointer != 0
                && slot.funder == spy as *const _ as usize
                && slot.pointer <= owner
                && owner < slot.pointer.saturating_add(slot.bytes)
        }) {
            *slot = MaintenancePayloadSlot::EMPTY;
            spy.payload_allocations.fetch_sub(1, AtomicOrdering::SeqCst);
        }
        slots.set(entries);
    });
}
fn maintenance_payload_freed(pointer: usize) {
    let _ = MAINTENANCE_PAYLOADS.try_with(|slots| {
        let mut entries = slots.get();
        if let Some(slot) = entries
            .iter_mut()
            .find(|slot| slot.pointer == pointer && pointer != 0)
        {
            // The reader scope retains this capability while its payload
            // drops. System.dealloc has already returned at this observation.
            let spy = unsafe { &*(slot.funder as *const MaintenanceReaderSpy) };
            spy.payload_frees.fetch_add(1, AtomicOrdering::SeqCst);
            *slot = MaintenancePayloadSlot::EMPTY;
        }
        slots.set(entries);
    });
}
fn maintenance_owner_refund(owner: usize, spy: &MaintenanceReaderSpy) {
    MAINTENANCE_PAYLOADS.with(|slots| {
        let live = slots
            .get()
            .iter()
            .filter(|slot| {
                slot.pointer != 0 && slot.owner == owner && slot.funder == spy as *const _ as usize
            })
            .count();
        spy.early_payload_refunds
            .fetch_add(live, AtomicOrdering::SeqCst);
    });
}

// Manager-authorized declaration: complete general frames and actual native
// original/caller invoices cannot be represented by the adapter-layout row.
#[test]
#[serial]
fn general_checkpoint_preserves_original_native_ownership() {
    crate::memory_accounting::general_checkpoint_native_rows();
}

pub(crate) fn disarm_source_free_watch() {
    for watch in &NATIVE_FREE {
        watch.pointer.store(0, AtomicOrdering::SeqCst);
        watch.counter.store(0, AtomicOrdering::SeqCst);
    }
}

pub(crate) fn arm_inverse_window() {
    NATIVE_REQUEST_COUNT.with(|count| count.set(0));
    NATIVE_REQUEST_ACTIVE.with(|active| {
        assert!(!active.get());
        active.set(true);
    });
}
pub(crate) fn disarm_inverse_window() -> usize {
    NATIVE_REQUEST_ACTIVE.with(|active| active.set(false));
    NATIVE_REQUEST_COUNT.with(std::cell::Cell::get)
}
#[cfg(feature = "test-seams")]
mod row_check_cache_ownership_contract {
    use crate::executor::bounded_read_test_support as bounded;
    use crate::{Database, MaintenancePolicy};
    use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
    use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;
    const MAX_MAINTENANCE_CYCLES: usize = 16;
    fn empty() -> HashMap<String, Value> {
        HashMap::new()
    }

    fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
        pairs
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect()
    }

    fn index(table: &str) -> VectorIndexRef {
        VectorIndexRef::new(table, "embedding")
    }

    fn partition(key: &str) -> VectorPartitionKey {
        VectorPartitionKey::from_values(&[Value::Text(key.to_owned())])
            .expect("TEXT keys have one canonical partition identity")
    }

    fn vector(axis: usize) -> Vec<f32> {
        match axis {
            0 => vec![1.0, 0.0, 0.0],
            1 => vec![0.0, 1.0, 0.0],
            _ => vec![0.0, 0.0, 1.0],
        }
    }

    fn drive_maintenance(db: &Database, table: &str, scope: &str) {
        let index = index(table);
        let partition = partition(scope);
        for _ in 0..MAX_MAINTENANCE_CYCLES {
            let mut partition_observation_owner_1 = crate::vector_read_memory::admission(
                db.declared_reader_memory()
                    .expect("admit the partition observation reader"),
            );

            if db
                .vector_store_for_test()
                .partition_info(&index, &partition, &mut partition_observation_owner_1)
                .unwrap()
                .is_some_and(|info| info.graph_available)
            {
                return;
            }
            db.run_maintenance_cycle()
                .expect("one finite caller-driven maintenance cycle returns");
        }
        panic!("fixture route was not maintained within the fixed finite cycle count");
    }

    #[derive(Clone, Copy)]
    struct FrozenClock;

    impl DeadlineClock for FrozenClock {
        fn now_ms(&self) -> u64 {
            0
        }

        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }

    fn roomy_read_limits() -> ReadLimits {
        ReadLimits {
            result_rows: 1_000,
            result_bytes: 16 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 1_000_000,
            memory: 128 * 1024 * 1024,
            cursor_page_rows: 128,
            cursor_page_bytes: 4 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 100_000,
        }
    }

    /// Records the real `VectorCandidates` reservation that the bounded kernel
    /// makes before the row-check cache asks the allocator for its backing table.
    /// This is the mapped original reader balance, not a second allocation meter.
    #[derive(Default)]
    struct RowCheckCacheFundingProbe {
        reservations: Mutex<Vec<(u64, u64)>>,
        row_touches: AtomicU64,
    }

    impl RowCheckCacheFundingProbe {
        fn reservation_after(&self, charged_bytes: usize) -> u64 {
            let charged_bytes =
                u64::try_from(charged_bytes).expect("cache charge fits test receipt");
            let reservations = self
                .reservations
                .lock()
                .expect("reservation observation lock");
            let matches = reservations
                .iter()
                .filter_map(|(reserved, held_after)| {
                    (*reserved == charged_bytes).then_some(*held_after)
                })
                .collect::<Vec<_>>();
            assert_eq!(
                matches.len(),
                1,
                "the cache charge maps to exactly one real operation-reader reservation: {reservations:?}"
            );
            matches[0]
        }
    }

    impl bounded::ExecutionProbe for RowCheckCacheFundingProbe {
        fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

        fn before_source_touch(&self, touch: bounded::TestSourceTouch, _completed_items: u64) {
            if touch == bounded::TestSourceTouch::TableRow {
                self.row_touches.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn before_temporary_reservation(
            &self,
            source: bounded::TestWorkSource,
            requested: u64,
            held: u64,
        ) {
            if source == bounded::TestWorkSource::VectorCandidates && PHASE.get() > 0 {
                PHASES.with(|p| {
                    let mut a = p.get();
                    let n = PHASE.get() - 1;
                    if a[n].0 == requested as usize && a[n].2 == 0 {
                        a[n].2 = held as usize;
                        p.set(a);
                    }
                });
            }
        }
        fn after_temporary_reservation(
            &self,
            source: bounded::TestWorkSource,
            reserved_bytes: u64,
            held_temporary_bytes: u64,
        ) {
            if source == bounded::TestWorkSource::VectorCandidates && PHASE.get() > 0 {
                PHASES.with(|p| {
                    let mut a = p.get();
                    let n = PHASE.get() - 1;
                    if a[n].0 == reserved_bytes as usize && a[n].3 == 0 {
                        a[n].3 = reserved_bytes as usize;
                        p.set(a);
                    }
                });
            }
            if source == bounded::TestWorkSource::VectorCandidates {
                self.reservations
                    .lock()
                    .expect("reservation observation lock")
                    .push((reserved_bytes, held_temporary_bytes));
            }
        }

        fn cancellation_observed(&self, _completed_work: u64) {}
    }

    fn create_row_check_cache_table(db: &Database) {
        db.execute(
            "CREATE TABLE row_check_cache_items (\
         id UUID PRIMARY KEY, \
         scope TEXT NOT NULL, \
         tag TEXT NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_SEARCH = 6)\
         )",
            &empty(),
        )
        .expect("create the row-check cache fixture");
        db.execute(
            "CREATE INDEX row_check_cache_filter ON row_check_cache_items (scope, tag)",
            &empty(),
        )
        .expect("support the unchanged bounded scope/tag candidate route");
    }

    fn insert_row_check_cache_item(db: &Database, id: u128, tag: &str, _axis: usize) {
        db.execute(
            "INSERT INTO row_check_cache_items (id, scope, tag, embedding) \
         VALUES ($id, 'alpha', $tag, $embedding)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(id))),
                ("tag", Value::Text(tag.to_owned())),
                (
                    "embedding",
                    Value::Vector(vec![1.0, ((id - 0xD000) as f32) / 256.0, 0.0]),
                ),
            ]),
        )
        .expect("commit one row-check-cache fixture row");
    }

    fn row_check_cache_request(memory: u64) -> bounded::BoundedReadRequest {
        let mut limits = roomy_read_limits();
        limits.memory = memory;
        bounded::BoundedReadRequest::new(
            "SELECT id FROM row_check_cache_items \
         WHERE scope = 'alpha' AND tag = 'keep' \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            params([("query", Value::Vector(vector(0)))]),
            limits,
            Arc::new(FrozenClock),
        )
    }

    // Phase metadata only. All physical Layouts and payer samples remain in
    // the sole allocator oracle's existing request and free-watch storage.
    thread_local! {
        static LIFT: std::cell::Cell<(usize,usize,usize,bool)> = const { std::cell::Cell::new((0,0,0,false)) };
        static CACHE_PAYER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static PHASE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static PHASES: std::cell::Cell<[(usize,usize,usize,usize,usize,usize);32]> = const { std::cell::Cell::new([(0,0,0,0,0,0);32]) };
        static PREDICATES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static KIND: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static CANCELLATION: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static REFUSE_PHASE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static NULL_PHASE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static NULL_LAYOUT: std::cell::Cell<(usize,usize)> = const { std::cell::Cell::new((0,0)) };
        static PHASE_LAYOUTS: std::cell::Cell<[(usize,usize);32]> = const { std::cell::Cell::new([(0,0);32]) };
        static PHASE_REQUEST_START: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static MIGRATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static CANCEL_DURING_MIGRATION_AFTER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }
    fn paid() -> usize {
        unsafe { &*(CACHE_PAYER.get() as *const super::AtomicUsize) }
            .load(super::AtomicOrdering::SeqCst)
    }
    fn cache_phase(event: u8, bytes: usize) {
        match event {
            0 | 3 => {
                let n = PHASE.get();
                assert!(n < 32);
                PHASE.set(n + 1);
                KIND.set(event as usize);
                PHASES.with(|p| {
                    let mut a = p.get();
                    a[n] = (bytes, paid(), 0, 0, PREDICATES.get(), event as usize);
                    p.set(a);
                });
            }
            1 => {
                if NULL_PHASE.get() == PHASE.get() {
                    let (size, align) = NULL_LAYOUT.get();
                    crate::vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(
                        std::alloc::Layout::from_size_align(size, align).expect("recorded cache layout"),
                    );
                }
                super::SOURCE_REQUEST_COUNTER.set(CACHE_PAYER.get());
                PHASE_REQUEST_START.set(super::NATIVE_REQUEST_COUNT.get());
                super::NATIVE_REQUEST_ACTIVE.set(true);
            }
            2 => {
                let selected_live_layout_was_refused = NULL_PHASE.get() == PHASE.get();
                super::NATIVE_REQUEST_ACTIVE.set(false);
                super::SOURCE_REQUEST_COUNTER.set(0);
                if selected_live_layout_was_refused {
                    // The intentional null has no cache pointer. Do not let
                    // later error construction enter the cache allocation census.
                    super::NATIVE_REQUEST_COUNT.set(PHASE_REQUEST_START.get());
                    return;
                }
                let n = super::NATIVE_REQUEST_COUNT.get();
                if n == PHASE_REQUEST_START.get() {
                    return;
                }
                assert!(n <= 32);
                let request = super::NATIVE_REQUESTS.get()[n - 1];
                PHASE_LAYOUTS.with(|layouts| {
                    let mut recorded = layouts.get();
                    recorded[PHASE.get() - 1] = (request.1, request.2);
                    layouts.set(recorded);
                });
                let phase = PHASES.get()[PHASE.get() - 1];
                assert!(
                    phase.0 >= request.1,
                    "phase admission must cover actual native Layout"
                );
                assert!(
                    super::SOURCE_REQUEST_CREDIT.get()[n - 1] >= phase.1 + request.1,
                    "entire new allocation must be prepaid while old remains live"
                );
                let old_live: usize = super::NATIVE_FREE
                    .iter()
                    .filter(|w| w.pointer.load(super::AtomicOrdering::SeqCst) != 0)
                    .map(|w| w.bytes.load(super::AtomicOrdering::SeqCst))
                    .sum();
                assert!(
                    super::SOURCE_REQUEST_CREDIT.get()[n - 1] >= old_live + request.1,
                    "full old/new native overlap is paid"
                );
                eprintln!(
                    "CACHE_PHASE number={} kind={} new_layout={} old_live={old_live} admission={} payer_before={} payer_at_allocation={}",
                    PHASE.get(),
                    phase.5,
                    request.1,
                    phase.0,
                    phase.1,
                    super::SOURCE_REQUEST_CREDIT.get()[n - 1]
                );
                super::append_source_free_for_test(&[request], CACHE_PAYER.get());
                if REFUSE_PHASE.get() == PHASE.get() {
                    unsafe {
                        &*(CANCELLATION.get()
                            as *const contextdb_core::read_contract::OwnerReadCancellation)
                    }
                    .cancel();
                }
            }
            6 => {
                PREDICATES.set(PREDICATES.get() + 1);
                if LIFT.get().0 != 0 {
                    let (a, b, c, _) = LIFT.get();
                    LIFT.set((a, b, c, true));
                }
            }
            4 => {
                let migrated = MIGRATED.get() + 1;
                MIGRATED.set(migrated);
                if CANCEL_DURING_MIGRATION_AFTER.get() == migrated {
                    unsafe {
                        &*(CANCELLATION.get()
                            as *const contextdb_core::read_contract::OwnerReadCancellation)
                    }
                    .cancel();
                }
            }
            7 => {
                let bytes = super::NATIVE_FREE
                    .iter()
                    .skip(1)
                    .filter(|w| w.pointer.load(super::AtomicOrdering::SeqCst) != 0)
                    .map(|w| w.bytes.load(super::AtomicOrdering::SeqCst))
                    .sum();
                LIFT.set((paid(), bytes, 0, false));
            }
            8 => {
                let (before, bytes, _, _) = LIFT.get();
                let after = paid();
                assert_eq!(
                    before - after,
                    bytes,
                    "AUTO refunds physical cache before exact predicate work"
                );
                LIFT.set((before, bytes, after, false));
            }

            _ => {}
        }
    }
    fn fixture() -> Database {
        let db = Database::open_memory();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_row_check_cache_table(&db);
        for id in 0..256_u128 {
            insert_row_check_cache_item(&db, 0xD000 + id, "keep", (id as usize) % 3);
        }
        drive_maintenance(&db, "row_check_cache_items", "alpha");
        db
    }
    fn run_case(
        db: &Database,
        cap: u64,
        fail_phase: usize,
    ) -> (
        bool,
        [(usize, usize, usize, usize, usize, usize); 32],
        usize,
        Arc<super::MaintenanceReaderSpy>,
    ) {
        run_case_with_mode(db, cap, fail_phase, false)
    }
    fn run_case_with_mode(
        db: &Database,
        cap: u64,
        fail_phase: usize,
        auto: bool,
    ) -> (
        bool,
        [(usize, usize, usize, usize, usize, usize); 32],
        usize,
        Arc<super::MaintenanceReaderSpy>,
    ) {
        let probe = Arc::new(RowCheckCacheFundingProbe::default());
        let mut request = row_check_cache_request(cap);
        if auto {
            request.sql = request.sql.replace("USE VECTOR INDEXED", "USE VECTOR AUTO");
        }
        LIFT.set((0, 0, 0, false));
        let payer = super::MaintenanceReaderSpy::new(
            db.declared_reader_memory().unwrap(),
            db.writer_accountant_for_test().clone(),
        );
        CANCELLATION.set(&request.cancellation as *const _ as usize);
        CACHE_PAYER.set(&payer.used as *const _ as usize);
        PHASE.set(0);
        PREDICATES.set(0);
        PHASES.set([(0, 0, 0, 0, 0, 0); 32]);
        REFUSE_PHASE.set(fail_phase);
        PHASE_LAYOUTS.set([(0, 0); 32]);
        MIGRATED.set(0);
        super::NATIVE_REQUEST_COUNT.set(0);
        super::NATIVE_REQUESTS.set([(0, 0, 0); 256]);
        super::SOURCE_REQUEST_CREDIT.set([0; 32]);
        super::disarm_source_free_watch();
        for w in &super::NATIVE_FREE {
            w.count.store(0, super::AtomicOrdering::SeqCst);
        }
        contextdb_vector::test_seam::set_row_check_cache_observer_for_test(Some(cache_phase));
        let (result, _, _) = super::measured(|| {
            crate::executor::execute_bounded(
                db,
                &request.sql,
                &request.params,
                payer.clone(),
                request.limits,
                request.clock.clone(),
                request.cancellation.clone(),
                Some(bounded::kernel_probe(probe.clone())),
            )
        });
        contextdb_vector::test_seam::set_row_check_cache_observer_for_test(None);
        let success = result.is_ok();
        if !success {
            if fail_phase != 0 || CANCEL_DURING_MIGRATION_AFTER.get() != 0 {
                assert!(
                    matches!(
                        &result,
                        Err(crate::executor::BoundedExecutionError::Cancelled)
                    ),
                    "typed cancellation: {result:?}"
                );
            } else if NULL_PHASE.get() != 0 {
                assert_eq!(
                    cap,
                    roomy_read_limits().memory,
                    "allocator-null proof keeps the configured 128 MiB read limit"
                );
                let expected_diagnostic = if KIND.get() == 3 {
                    "row-check counters allocation failed"
                } else {
                    "row-check verdict cache allocation failed"
                };
                assert!(
                    matches!(
                        &result,
                        Err(crate::executor::BoundedExecutionError::Engine(
                            contextdb_core::Error::Other(message)
                        )) if message == expected_diagnostic
                    ),
                    "physical malloc failure for selected row-check storage: {result:?}"
                );
            } else {
                assert!(
                    matches!(
                        &result,
                        Err(crate::executor::BoundedExecutionError::Refused(_))
                    ),
                    "typed cap refusal: {result:?}"
                );
            }
        }
        let count = super::NATIVE_REQUEST_COUNT.get();
        let free = super::source_free_for_test(count);
        for i in 0..count {
            assert_eq!(free[i].2, 1, "each actual allocation physically frees once");
            assert_eq!(
                free[i].0, free[i].1,
                "same original payer survives physical free"
            );
            assert!(
                free[i].0
                    >= super::NATIVE_FREE[i]
                        .live_at_free
                        .load(super::AtomicOrdering::SeqCst),
                "native allocation paid through free"
            );
        }
        drop(result);
        let remaining = payer.used.load(Ordering::SeqCst);
        // The database owns an operation-created retained control; compare
        // this exact balance across successful and refused trials.
        eprintln!(
            "CACHE_CASE cap={cap} success={success} phases={} allocations={count} predicates={} remaining={remaining} metadata={:?} frees={:?}",
            PHASE.get(),
            PREDICATES.get(),
            &PHASES.get()[..PHASE.get()],
            &free[..count]
        );
        super::disarm_source_free_watch();
        CACHE_PAYER.set(0);
        (success, PHASES.get(), PHASE.get(), payer)
    }
    // Two thin rows share the unchanged SQL/fixture and native oracle.
    // Layout/overlap follows the qualified R*32 RED. The distinct admission row
    // is necessary because a global query minimum can exceed the cache phase.
    fn exercise_cache_memory_contract(boundary: bool) {
        let db = fixture();
        let (ok, phases, n, payer1) = run_case(&db, roomy_read_limits().memory, 0);
        assert!(ok);
        assert!(n > 3, "actual incremental replacement executes");
        let mut payers = vec![payer1];
        if boundary {
            let target = (0..n)
                .filter(|i| phases[*i].5 == 0)
                .max_by_key(|i| phases[*i].2 + phases[*i].0)
                .unwrap();
            let cap = (phases[target].2 + phases[target].0) as u64;
            let (_, fit, fit_n, payer) = run_case(&db, cap, 0);
            payers.push(payer);
            let fit_reached = fit_n > target && fit[target].3 == phases[target].0;
            let (short, short_phases, short_n, payer) = run_case(&db, cap - 1, 0);
            payers.push(payer);
            let short_reached = short_n > target
                && short_phases[target].3 == 0
                && PREDICATES.get() == short_phases[target].4;
            let (retry, _, _, payer) = run_case(&db, roomy_read_limits().memory, 0);
            payers.push(payer);
            assert!(retry);
            drop(db);
            for payer in payers {
                assert_eq!(
                    payer.used.load(Ordering::SeqCst),
                    0,
                    "all actual original payer invoices refund"
                );
            }
            assert!(
                fit_reached,
                "actual cache exact-fit phase unreachable under its aggregate cap; distinguish earlier query minimum"
            );
            assert!(
                !short && short_reached,
                "one-byte-short reaches same cache admission and stops before predicate"
            );
        } else {
            let (cancelled, phases, cancel_n, payer) = run_case(&db, roomy_read_limits().memory, 4);
            payers.push(payer);
            assert!(!cancelled);
            assert_eq!(cancel_n, 4);
            assert!(phases[3].4 > 0);
            let (retry, _, _, payer) = run_case(&db, roomy_read_limits().memory, 0);
            payers.push(payer);
            assert!(retry);
            drop(db);
            for payer in payers {
                assert_eq!(
                    payer.used.load(Ordering::SeqCst),
                    0,
                    "all actual original payer invoices refund"
                );
            }
        }
    }
    #[test]
    #[serial]
    fn row_check_cache_native_layout_is_funded_by_vector_candidates() {
        exercise_cache_memory_contract(false);
    }
    #[test]
    #[serial]
    fn row_check_cache_exact_admission_boundary_stops_before_predicate() {
        exercise_cache_memory_contract(true);
    }
    #[test]
    #[serial]
    fn row_check_cache_auto_lift_refunds_before_exact_predicate() {
        let db = Database::open_memory();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_row_check_cache_table(&db);
        for id in 0..256_u128 {
            insert_row_check_cache_item(
                &db,
                0xD000 + id,
                if id < 128 { "skip" } else { "keep" },
                (id as usize) % 3,
            );
        }
        drive_maintenance(&db, "row_check_cache_items", "alpha");
        // Existing graph-shortfall control applies AFTER real graph traversal.
        let _shortfall = db
            .vector_store_for_test()
            .cap_graph_candidates_for_test(&index("row_check_cache_items"), 0);
        let (ok, _, _, payer) = run_case_with_mode(&db, roomy_read_limits().memory, 0, true);
        let lift = LIFT.get();
        eprintln!("CACHE_AUTO_LIFT observed={lift:?}");
        drop(db);
        assert_eq!(payer.used.load(Ordering::SeqCst), 0);
        assert!(ok);
        assert!(
            lift.1 > 0 && lift.3,
            "AUTO must lift an allocated cache and enter the exact predicate pass"
        );
    }

    #[test]
    #[serial]
    fn row_check_cache_allocator_null_refuses_each_live_layout_then_retries() {
        let db = fixture();
        let (ok, _, phases, payer) = run_case(&db, roomy_read_limits().memory, 0);
        assert!(ok);
        let layouts = PHASE_LAYOUTS.get();
        let targets = [
            0,
            (0..phases).find(|phase| PHASES.get()[*phase].5 == 0).expect("initial map phase"),
            (0..phases).filter(|phase| PHASES.get()[*phase].5 == 0).last().expect("replacement phase"),
        ];
        let mut payers = vec![payer];
        for target in targets {
            NULL_PHASE.set(target + 1);
            NULL_LAYOUT.set(layouts[target]);
            let (ok, _, _, payer) = run_case(&db, roomy_read_limits().memory, 0);
            payers.push(payer);
            assert!(!ok, "the selected live allocation is a typed refusal");
            assert!(
                !crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test(),
                "only the selected live allocation consumes the native-null seam"
            );
            NULL_PHASE.set(0);
            NULL_LAYOUT.set((0, 0));
            let (retry, _, _, payer) = run_case(&db, roomy_read_limits().memory, 0);
            payers.push(payer);
            assert!(retry, "the same database remains queryable after allocator null");
        }
        drop(db);
        for payer in payers {
            assert_eq!(payer.used.load(Ordering::SeqCst), 0, "all original payer invoices settle");
        }
    }

    #[test]
    #[serial]
    fn row_check_cache_cancellation_after_migration_entries_retries() {
        let db = fixture();
        CANCEL_DURING_MIGRATION_AFTER.set(2);
        let (ok, _, _, payer) = run_case(&db, roomy_read_limits().memory, 0);
        CANCEL_DURING_MIGRATION_AFTER.set(0);
        assert!(!ok);
        assert!(MIGRATED.get() >= 2, "cancellation follows copied cache entries");
        let (retry, _, _, retry_payer) = run_case(&db, roomy_read_limits().memory, 0);
        assert!(retry);
        drop(db);
        assert_eq!(payer.used.load(Ordering::SeqCst), 0);
        assert_eq!(retry_payer.used.load(Ordering::SeqCst), 0);
    }
}

// The writer needs only family-tagged value digests after producing vector
// identity mutations. Compare exact roots with the independent canonical-byte
// control and use this existing allocator oracle for the retained ownership row.
fn vector_identity_values_release_payloads_after_hashing() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let mut retained = Vec::new();
        for dimensions in [768, 1536] {
            let entry = VectorEntry {
                index: VectorIndexRef::new("vectors", "embedding"),
                row_id: RowId(7),
                vector: (0..dimensions)
                    .map(|axis| ((axis % 31) as f32 - 15.0) / 16.0)
                    .collect(),
                created_tx: TxId(11),
                deleted_tx: None,
                lsn: Lsn(13),
            };
            let ((current, historical), _, live) = measured(|| {
                (
                    vector_mutation(&entry, quantization),
                    vector_version_mutation(&entry, quantization),
                )
            });
            retained.push(live);
            let stored = contextdb_vector::stored_vector_value(&entry.vector, quantization);
            let mut canonical = (stored.len() as u64).to_le_bytes().to_vec();
            for value in &stored {
                canonical.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            let mut forged_bytes = canonical.clone();
            forged_bytes[8] ^= 1;
            let forged = ContentIdentityMutation::put(
                vector_family(quantization),
                vector_key(&entry.index, entry.row_id),
                forged_bytes,
            );
            let encoded = [
                ContentIdentityMutation::put(
                    vector_family(quantization),
                    vector_key(&entry.index, entry.row_id),
                    canonical.clone(),
                ),
                ContentIdentityMutation::put(
                    LogicalFamily::ChangeAndDdl,
                    vector_version_key(
                        &entry.index,
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        quantization,
                    ),
                    canonical,
                ),
            ];
            let root = tempfile::tempdir().unwrap();
            let actual = ProofFixtureDatabase::create(root.path().join("actual.db")).unwrap();
            let control = ProofFixtureDatabase::create(root.path().join("control.db")).unwrap();
            let install = |database: &ProofFixtureDatabase,
                           mutations: &[ContentIdentityMutation]| {
                let write = database.begin_write().unwrap();
                crate::persistence::erasure_frontiers::initialize(&write).unwrap();
                RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
                let digest = RedbPersistence::apply_content_identity_mutations_in_write(
                    &write,
                    mutations,
                    &mut database.admission(None),
                )
                .unwrap();
                write.commit().unwrap();
                digest
            };
            let produced = [current, historical];
            assert_eq!(
                install(&actual, &produced),
                install(&control, &encoded),
                "canonical vector roots must remain exact for {quantization:?}"
            );
            let read = actual.begin_read().unwrap();
            let verifier = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
            for mutation in &encoded {
                verifier.verify(mutation).unwrap();
            }
            assert!(
                verifier.verify(&forged).is_err(),
                "the selected body's bytes remain the read-authentication evidence"
            );
        }
        assert_eq!(
            retained[0], retained[1],
            "retained vector identity ownership must not contain dimension-sized payloads ({quantization:?})"
        );
    }
}

// Existing serial allocation oracle: row-local does not mean uncharged.
fn vector_row_encoding_has_exact_writer_admission() {
    let row = VersionedRow {
        row_id: RowId(7),
        values: HashMap::from([
            ("id".to_owned(), Value::Int64(7)),
            ("embedding".to_owned(), Value::Vector(vec![0.25; 768])),
        ]),
        created_tx: TxId(11),
        deleted_tx: None,
        lsn: Lsn(13),
        created_at: None,
    };
    let run = |limit| {
        let funding = accountant(limit);
        let owner = crate::read_image_memory::ImageMemory::operation_writer(funding.clone().into())
            .unwrap();
        let (result, native_peak, _) = measured(|| {
            owner.scope(|| {
                let mut encoded = Vec::new();
                let result = RedbPersistence::encode_versioned_row_with_f32_overlay_into(
                    &row,
                    None,
                    None,
                    None,
                    &mut encoded,
                );
                (result, encoded)
            })
        });
        let high = funding.high_water_for_test();
        let (result, encoded) = result;
        if result.is_ok() {
            let verifier = crate::read_image_memory::ImageMemory::operation_writer(
                accountant(usize::MAX).into(),
            )
            .unwrap();
            verifier.scope(|| {
                let decoded = RedbPersistence::decode_versioned_row(&encoded, None).unwrap();
                assert_eq!(decoded.values, row.values);
            });
        }
        drop(encoded);
        drop(owner);
        assert_eq!(
            funding.usage().used,
            0,
            "row scratch returns its credit after physical output destruction"
        );
        (result, high, native_peak)
    };
    let (result, high, native_peak) = run(usize::MAX);
    result.unwrap();
    assert!(
        high >= 768 * std::mem::size_of::<f32>(),
        "the F32 row body has a writer admission before allocation"
    );
    assert!(
        native_peak <= high,
        "row-native peak {native_peak} exceeds admitted peak {high}"
    );
    let (fit, fit_high, _) = run(high);
    fit.unwrap();
    assert_eq!(fit_high, high);
    let (short, _, _) = run(high - 1);
    match short {
        Err(Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        }) => {
            assert_eq!(budget_limit_bytes, high - 1);
            assert_eq!(requested_bytes, available_bytes + 1);
        }
        other => panic!("one-byte-short row admission must refuse typed: {other:?}"),
    }
}

// One native free observation covers each replaced mutation directory's credit.
fn vector_identity_directory_refunds_only_after_its_buffer_is_freed() {
    let invoice = std::mem::size_of::<ContentIdentityMutation>();
    for short in [None, Some(false), Some(true)] {
        let writer = MemoryAccountant::writer_budget(usize::MAX).unwrap();
        let funding = writer.accountant();
        let original = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            funding.clone(),
            0,
            "persistence",
            "prepare_commit",
            "test writer",
        )
        .unwrap();
        let frame = OriginalPersistenceFrame::new(&original).unwrap();
        let before = funding.usage().used;
        if let Some(short) = short {
            funding
                .set_budget(Some(before + 12 * invoice - usize::from(short)))
                .unwrap();
        }
        let mut directory = OriginalMutationVector::new(&frame);
        for _ in 0..4 {
            directory
                .push(ContentIdentityMutation::remove(
                    LogicalFamily::F32Vectors,
                    [],
                ))
                .unwrap();
        }
        let prior_capacity = directory.values.capacity();
        assert_eq!(prior_capacity, 4);
        watch_candidate_payload_for_test(directory.values.as_ptr() as usize, &funding);
        let result = directory.push(ContentIdentityMutation::remove(
            LogicalFamily::F32Vectors,
            [],
        ));
        let (at_free, (freed, _), _) = candidate_payload_free_observation_for_test();
        if short == Some(true) {
            match result {
                Err(Error::MemoryBudgetExceeded {
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    ..
                }) => {
                    assert_eq!(requested_bytes, 8 * invoice);
                    assert_eq!(available_bytes, requested_bytes - 1);
                    assert_eq!(budget_limit_bytes, before + 12 * invoice - 1);
                }
                other => panic!("one-byte-short directory growth refuses typed: {other:?}"),
            }
            assert!(!freed, "a refused growth retains the original directory");
            assert_eq!(directory.values.len(), 4);
            assert_eq!(directory.values.capacity(), prior_capacity);
            assert_eq!(frame.work.borrow().bytes(), 4 * invoice);
        } else {
            result.unwrap();
            assert!(freed);
            assert_eq!(
                at_free.3,
                before + 12 * invoice,
                "old and successor storage remain funded at physical old-buffer free"
            );
            assert_eq!(directory.values.len(), 5);
            assert_eq!(directory.values.capacity(), 8);
            assert_eq!(
                frame.work.borrow().bytes(),
                8 * invoice,
                "the old directory invoice is released after its storage disappears"
            );
        }
        watch_candidate_payload_for_test(directory.values.as_ptr() as usize, &funding);
        let held = funding.usage().used;
        drop(directory);
        let (at_free, (freed, _), _) = candidate_payload_free_observation_for_test();
        assert!(freed);
        assert_eq!(
            at_free.3, held,
            "the retained directory is still funded at physical free"
        );
        drop(frame);
        assert_eq!(funding.usage().used, before);

    }
}


/// Inspect the existing window at an admission boundary, without allocating
/// another observation ledger or measuring another thread's allocations.
#[cfg(feature = "test-seams")]
pub(crate) fn measured_live_bytes_for_test() -> usize {
    let window = MEASURING_HERE.with(std::cell::Cell::get);
    let measurement = MEASUREMENT.lock().unwrap();
    if window != 0 && measurement.active_window == window {
        measurement.live
    } else {
        0
    }
}
