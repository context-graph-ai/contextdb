//! Shared test allocator observation; each integration binary installs it once.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Counters {
    live: AtomicUsize,
    peak: AtomicUsize,
    funding: Option<Arc<AtomicUsize>>,
    unfunded: AtomicUsize,
    allocations: AtomicUsize,
}
thread_local! {
    static ACTIVE: Cell<*const Counters> = const { Cell::new(std::ptr::null()) };
}
struct Header {
    counters: *const Counters,
}
fn expanded(layout: Layout) -> (Layout, usize) {
    let alignment = layout.align().max(std::mem::align_of::<Header>());
    let offset = std::mem::size_of::<Header>().next_multiple_of(alignment);
    (
        Layout::from_size_align(layout.size() + offset, alignment).unwrap(),
        offset,
    )
}
struct Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (allocation, offset) = expanded(layout);
        let base = unsafe { System.alloc(allocation) };
        if base.is_null() {
            return base;
        }
        let pointer = unsafe { base.add(offset) };
        let counters = ACTIVE.try_with(Cell::get).unwrap_or(std::ptr::null());
        unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .write(Header { counters });
        }
        if !counters.is_null() {
            // Retain the counters without allocating in the allocator hook.
            unsafe { Arc::increment_strong_count(counters) };
            let counters = unsafe { &*counters };
            let live = counters.live.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            counters.peak.fetch_max(live, Ordering::SeqCst);
            if let Some(funding) = &counters.funding {
                // No caller code runs between System.alloc and this sample.
                // Observe the funding at this allocation boundary, including
                // both buffers of a realloc; never infer order from a peak.
                counters.allocations.fetch_add(1, Ordering::SeqCst);
                counters.unfunded.fetch_max(
                    live.saturating_sub(funding.load(Ordering::SeqCst)),
                    Ordering::SeqCst,
                );
            }
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let (allocation, offset) = expanded(layout);
        let header = unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .read()
        };
        if !header.counters.is_null() {
            let counters = unsafe { Arc::from_raw(header.counters) };
            counters.live.fetch_sub(layout.size(), Ordering::SeqCst);
        }
        unsafe { System.dealloc(pointer.sub(offset), allocation) };
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.set(std::ptr::null()));
    }
}
pub(super) fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let counters = Arc::new(Counters::default());
    ACTIVE.with(|active| {
        assert!(active.get().is_null());
        active.set(Arc::as_ptr(&counters));
    });
    let reset = Reset;
    let value = operation();
    drop(reset);
    (value, counters.peak.load(Ordering::SeqCst))
}

pub(crate) fn measure_funding<T>(
    funding: Arc<AtomicUsize>,
    operation: impl FnOnce() -> T,
) -> (T, usize, usize, usize) {
    let counters = Arc::new(Counters {
        funding: Some(funding),
        ..Counters::default()
    });
    ACTIVE.with(|active| {
        assert!(active.get().is_null());
        active.set(Arc::as_ptr(&counters));
    });
    let reset = Reset;
    let value = operation();
    drop(reset);
    (
        value,
        counters.peak.load(Ordering::SeqCst),
        counters.unfunded.load(Ordering::SeqCst),
        counters.allocations.load(Ordering::SeqCst),
    )
}
