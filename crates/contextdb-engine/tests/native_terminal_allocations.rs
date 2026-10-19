//! A native delivery terminal is encoded without a single heap allocation:
//! every typed refusal reachable on the native route is built first, then its
//! borrowed encoding is measured alone under a counting global allocator.
#![cfg(feature = "test-seams")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
        }
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn every_typed_native_terminal_encodes_without_a_heap_allocation() {
    let mut results = Vec::new();
    contextdb_engine::transport::measure_native_terminal_encodings_for_test(&mut |name, encode| {
        ALLOCATIONS.with(|count| count.set(0));
        COUNTING.with(|counting| counting.set(true));
        let length = encode();
        COUNTING.with(|counting| counting.set(false));
        let allocations = ALLOCATIONS.with(Cell::get);
        results.push((name, length, allocations));
    });
    println!("S4_TERMINAL_ALLOCATIONS {results:?}");
    assert_eq!(results.len(), 5);
    for (name, length, allocations) in results {
        assert!(length.is_some(), "{name}: encoded");
        assert_eq!(allocations, 0, "{name}: the encoder allocated");
    }
}
