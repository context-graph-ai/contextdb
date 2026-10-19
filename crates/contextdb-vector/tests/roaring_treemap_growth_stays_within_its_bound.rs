//! An EXPLAIN that narrows by a residual filter admits its `RoaringTreemap`
//! before building it, at `ROARING_TREEMAP_BYTES_PER_ID_UPPER_BOUND` bytes per
//! id, and the same again for the intersection that consumes it. That bound is
//! only as good as this measurement: a counting allocator records the real
//! peak a treemap reaches while ids are inserted, for ids spread one per
//! high-32 bucket (the worst case), one per container inside one bucket, and
//! dense, and for the output of an intersection. One test in its own binary,
//! because it installs a global allocator.

use contextdb_vector::mem::ROARING_TREEMAP_BYTES_PER_ID_UPPER_BOUND as BOUND;
use roaring::RoaringTreemap;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

struct Counting;

static ON: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn grow(bytes: isize) {
    let live = LIVE.fetch_add(bytes, Ordering::SeqCst) + bytes;
    PEAK.fetch_max(live, Ordering::SeqCst);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && ON.load(Ordering::SeqCst) {
            grow(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if ON.load(Ordering::SeqCst) {
            LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        }
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() && ON.load(Ordering::SeqCst) {
            grow(new_size as isize - layout.size() as isize);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak bytes `measure` allocates beyond what was live when it began.
fn peak_of<T>(measure: impl FnOnce() -> T) -> (T, usize) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ON.store(true, Ordering::SeqCst);
    let value = measure();
    ON.store(false, Ordering::SeqCst);
    (value, PEAK.load(Ordering::SeqCst).max(0) as usize)
}

/// A named way of spreading ids over the treemap's buckets and containers.
type IdPattern = (&'static str, fn(u64) -> u64);

#[test]
fn roaring_treemap_growth_stays_within_its_declared_bound_per_id() {
    let patterns: [IdPattern; 3] = [
        ("one id per high-32 bucket", |i| i << 32),
        ("one id per container", |i| i << 16),
        ("dense", |i| i),
    ];
    for (pattern, id) in patterns {
        for count in [1u64, 2, 3, 7, 11, 12, 100, 1_000, 10_000] {
            let (built, peak) = peak_of(|| {
                let mut ids = RoaringTreemap::new();
                for ordinal in 0..count {
                    ids.insert(id(ordinal));
                }
                ids
            });
            let per_id = peak.div_ceil(count as usize);
            eprintln!("BUILD pattern={pattern:?} ids={count} peak={peak} per_id={per_id}");
            assert!(
                peak <= count as usize * BOUND,
                "{pattern}, {count} ids: building peaked at {peak} bytes > {count} x {BOUND}"
            );
            // The intersection output, built while both inputs stay alive.
            let other = built.clone();
            let (output, peak) = peak_of(|| &built & &other);
            assert_eq!(output.len(), count);
            eprintln!("INTERSECT pattern={pattern:?} ids={count} peak={peak}");
            assert!(
                peak <= count as usize * BOUND,
                "{pattern}, {count} ids: the intersection peaked at {peak} bytes > {count} x {BOUND}"
            );
            drop((built, other, output));
        }
    }
}
