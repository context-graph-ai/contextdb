//! Independently observes a separately compiled, non-test engine with its
//! explicit diagnostic feature. The default production library has no pointer seam.
#![cfg(feature = "test-seams")]

use contextdb_engine::Database;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

const CAPACITY: usize = 4096;
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<usize> = const { Cell::new(0) };
    static REQUESTS: Cell<[(usize, usize, usize); CAPACITY]> = const { Cell::new([(0, 0, 0); CAPACITY]) };
}
struct Watch {
    pointer: AtomicUsize,
    count: AtomicUsize,
    sequence: AtomicUsize,
}
impl Watch {
    const fn new() -> Self {
        Self {
            pointer: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            sequence: AtomicUsize::new(0),
        }
    }
}
static WATCH: [Watch; 3] = [Watch::new(), Watch::new(), Watch::new()];
static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
struct Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && ACTIVE.try_with(Cell::get).unwrap_or(false) {
            COUNT.with(|count| {
                let index = count.get();
                if index < CAPACITY {
                    REQUESTS.with(|requests| {
                        let mut rows = requests.get();
                        rows[index] = (pointer as usize, layout.size(), layout.align());
                        requests.set(rows);
                    });
                }
                count.set(index + 1);
            });
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if ACTIVE.try_with(Cell::get).unwrap_or(false) {
            REQUESTS.with(|requests| {
                let mut rows = requests.get();
                if let Some(row) = rows.iter_mut().find(|row| row.0 == pointer as usize) {
                    *row = (0, 0, 0);
                }
                requests.set(rows);
            });
        }
        // Consume the matching address before free so subsequent address reuse
        // cannot be counted as a second free. No observed pointer is dereferenced.
        let watching = WATCH.iter().find(|watch| {
            watch
                .pointer
                .compare_exchange(pointer as usize, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        });
        unsafe {
            System.dealloc(pointer, layout);
        }
        if let Some(watch) = watching {
            watch.sequence.store(
                SEQUENCE.fetch_add(1, Ordering::SeqCst) + 1,
                Ordering::SeqCst,
            );
            watch.count.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

#[test]
fn non_test_engine_native_controls_match_actual_allocator_requests_and_final_frees() {
    // Initialize every thread-local observation slot before the constructor.
    COUNT.with(|slot| slot.set(0));
    REQUESTS.with(|slot| slot.set([(0, 0, 0); CAPACITY]));
    ACTIVE.with(|slot| slot.set(true));
    let database = Database::open_memory();
    ACTIVE.with(|slot| slot.set(false));
    let count = COUNT.with(Cell::get);
    assert!(
        count <= CAPACITY,
        "constructor observation storage must not overflow"
    );
    let requests = REQUESTS.with(Cell::get);
    let (native, controls) = database.__native_writer_backing_for_test();
    assert!(native);
    let mut actual = [(0, 0, 0); 3];
    for (index, (payload, layout)) in controls.iter().enumerate() {
        let mut matches = requests[..count]
            .iter()
            .filter(|request| request.0 < *payload && *payload < request.0 + request.1);
        let request = *matches
            .next()
            .expect("each live control belongs to an actual allocation");
        assert!(
            matches.next().is_none(),
            "each live control has exactly one allocation invoice"
        );
        assert_eq!((request.1, request.2), (layout.size(), layout.align()));
        actual[index] = request;
        WATCH[index].pointer.store(request.0, Ordering::SeqCst);
    }
    assert_eq!(
        WATCH
            .each_ref()
            .map(|watch| watch.count.load(Ordering::SeqCst)),
        [0; 3]
    );
    drop(database);
    let free = WATCH.each_ref().map(|watch| {
        (
            watch.count.load(Ordering::SeqCst),
            watch.sequence.load(Ordering::SeqCst),
        )
    });
    assert_eq!(free.map(|entry| entry.0), [1; 3]);
    assert!(
        free[2].1 > free[0].1 && free[2].1 > free[1].1,
        "actual counter backing remains until accountant and limit frees complete"
    );
    println!(
        "NON_TEST_NATIVE_ALLOCATOR_WITNESS profile=debug features=test-seams,iroh requests={actual:?} free={free:?} actual_count_each=1 counter_physical_free_last=true"
    );
}
