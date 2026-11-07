//! A writer's startup cache settle frees the read index it replaces, and a
//! statement's page window records at most half of the live index arena.
//!
//! Promise: every byte the cache holds for its index is named in the index it
//! reports. When the writer's startup settle rebuilds the read index for a
//! different capacity, the arena it replaces (control block, entries and
//! free slots) is physically freed during the settle, so nothing the cache
//! still holds escapes the reported index bytes. A statement's residency
//! window samples at most half of the live arena's slots.
//!
//! The witness is a global allocator that records, while armed, every free
//! made on this thread; the cache's own allocation listing names the arena
//! before and after the settle. Run with `--features contextdb-internal,test-seams`.

#![cfg(all(feature = "contextdb-internal", feature = "test-seams"))]

use redb::{ReadableDatabase, TableDefinition};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};

/// (address, size) for every free made on this thread while armed. The log is
/// reserved before arming, so recording never allocates.
struct Witness;
thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
    static FREES: RefCell<Vec<(usize, usize)>> = const { RefCell::new(Vec::new()) };
    static OVERFLOWED: Cell<bool> = const { Cell::new(false) };
}

fn witness_free(address: usize, size: usize) {
    let _ = ARMED.try_with(|armed| {
        if !armed.get() || IN_HOOK.with(|hook| hook.replace(true)) {
            return;
        }
        FREES.with(|frees| {
            let mut frees = frees.borrow_mut();
            if frees.len() < frees.capacity() {
                frees.push((address, size));
            } else {
                OVERFLOWED.with(|flag| flag.set(true));
            }
        });
        IN_HOOK.with(|hook| hook.set(false));
    });
}

unsafe impl GlobalAlloc for Witness {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        witness_free(pointer as usize, layout.size());
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            witness_free(pointer as usize, layout.size());
        }
        resized
    }
}

#[global_allocator]
static WITNESS: Witness = Witness;

fn record_frees<T>(operation: impl FnOnce() -> T) -> (T, Vec<(usize, usize)>) {
    FREES.with(|frees| {
        let mut frees = frees.borrow_mut();
        frees.clear();
        frees.reserve(1 << 16);
    });
    OVERFLOWED.with(|flag| flag.set(false));
    ARMED.with(|armed| armed.set(true));
    let result = operation();
    ARMED.with(|armed| armed.set(false));
    assert!(
        !OVERFLOWED.with(Cell::get),
        "the free log overflowed; the witness cannot vouch for this operation"
    );
    (
        result,
        FREES.with(|frees| std::mem::take(&mut *frees.borrow_mut())),
    )
}

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("settle_rows");
const ROWS: u64 = 512;
const VALUE_BYTES: usize = 512;

fn write_rows(database: &redb::Database) {
    let write = database.begin_write().expect("begin the fixture write");
    {
        let mut table = write.open_table(TABLE).expect("open the fixture table");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..ROWS {
            table
                .insert(key, value.as_slice())
                .expect("insert a fixture row");
        }
    }
    write.commit().expect("commit the fixture rows");
}

/// The cache's index allocations as (address, size, align).
fn cache_allocations(
    visit: impl FnOnce(&mut dyn FnMut((usize, usize, usize))),
) -> Vec<(usize, usize, usize)> {
    let mut allocations = Vec::new();
    visit(&mut |allocation| allocations.push(allocation));
    allocations
}

/// Settling the writer's cache to a different capacity rebuilds its read
/// index; every allocation of the arena it replaces is freed during the
/// settle, and the index the cache reports afterwards is the one it holds.
#[test]
fn a_startup_settle_frees_the_cache_index_it_replaces() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("settle.redb");
    let mut builder = redb::Builder::new();
    builder.set_cache_size(64 << 20);
    let database = builder.create(&path).expect("create the writer");
    write_rows(&database);
    let before = cache_allocations(|visit| {
        database.cache_allocations_for_test(visit);
    });

    let (settled, frees) = record_frees(|| database.settle_startup_cache_budget(1 << 20));
    settled.expect("the writer settles its startup cache to 1 MiB");

    let after = cache_allocations(|visit| {
        database.cache_allocations_for_test(visit);
    });
    let replaced: Vec<_> = before
        .iter()
        .filter(|allocation| !after.contains(allocation))
        .collect();
    assert!(
        !replaced.is_empty(),
        "settling from 64 MiB to 1 MiB replaces the read index arena"
    );
    for &&(address, size, _) in &replaced {
        assert!(
            frees.contains(&(address, size)),
            "the replaced cache index allocation at {address:#x} ({size} bytes) must be freed \
             during the settle; nothing the cache holds may escape its reported index bytes \
             (replaced: {replaced:?})"
        );
    }
}

/// A statement that touches far more distinct pages than the window holds
/// records exactly half of the live arena's slots.
#[test]
fn a_statement_window_records_at_most_half_the_index_arena() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("half.redb");
    let database = redb::Builder::new()
        .create(&path)
        .expect("create the fixture store");
    write_rows(&database);
    drop(database);
    let mut builder = redb::Builder::new();
    builder.set_cache_size(64 << 10);
    let reader = builder
        .open_read_only(&path)
        .expect("reopen the fixture store for reading");
    let allocations = cache_allocations(|visit| {
        reader.cache_allocations_for_test(visit);
    });
    // The listing is: two cache counters, the write buffer, the stripe array,
    // then the arena's control block, entries and free slots, then each
    // stripe's heads. The free slots are one u32 per arena slot.
    let &(_, slot_bytes, slot_align) = allocations
        .get(6)
        .expect("the cache lists its index arena's free slots seventh");
    assert_eq!(slot_align, std::mem::align_of::<u32>());
    let arena_slots = slot_bytes / std::mem::size_of::<u32>();
    let ((), fraction) = reader.with_read_cache_page_window(|| {
        let read = reader.begin_read().expect("begin a reader");
        let table = read.open_table(TABLE).expect("open the fixture table");
        for key in 0..ROWS {
            table
                .get(key)
                .expect("a fixture key is readable")
                .expect("every fixture key is present");
        }
    });
    assert!(
        fraction.is_some(),
        "a statement that read pages is measured"
    );
    assert_eq!(
        reader.page_window_entries_visited_for_test(),
        arena_slots / 2,
        "a statement over every row of the table records half of the {arena_slots}-slot arena"
    );
}
