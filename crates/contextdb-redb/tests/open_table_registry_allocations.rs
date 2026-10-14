//! The open-table registry's allocations are exactly what its allocator
//! reports, so a caller can fund them before they happen.
//!
//! Every registry array allocation and free is recorded by the registry's own
//! allocator (by construction: the map has no other allocator) and, as an
//! independent witness, by a counting global allocator on this thread. Arrays
//! are matched by address and generation; key boxes are matched by address.
//! Run with `--features contextdb-internal,test-seams`.

#![cfg(all(feature = "contextdb-internal", feature = "test-seams"))]

use redb::backends::InMemoryBackend;
use redb::{RegistryAllocationRecord, RegistryReserveError, TableDefinition, WriteTransaction};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};

/// (address, size, align, allocated) for every allocation and free made on
/// this thread while armed. The vector is reserved up front, so recording
/// never allocates; a full log is reported rather than silently truncated.
struct Witness;
thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
    static LOG: RefCell<Vec<(usize, usize, usize, bool)>> = const { RefCell::new(Vec::new()) };
    static OVERFLOWED: Cell<bool> = const { Cell::new(false) };
}
fn witness(address: usize, layout: Layout, allocated: bool) {
    let _ = ARMED.try_with(|armed| {
        if !armed.get() || IN_HOOK.with(|hook| hook.replace(true)) {
            return;
        }
        LOG.with(|log| {
            let mut log = log.borrow_mut();
            if log.len() < log.capacity() {
                log.push((address, layout.size(), layout.align(), allocated));
            } else {
                OVERFLOWED.with(|flag| flag.set(true));
            }
        });
        IN_HOOK.with(|hook| hook.set(false));
    });
}
unsafe impl GlobalAlloc for Witness {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            witness(pointer as usize, layout, true);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            witness(pointer as usize, layout, true);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        witness(pointer as usize, layout, false);
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            witness(pointer as usize, layout, false);
            witness(
                resized as usize,
                Layout::from_size_align(size, layout.align()).unwrap(),
                true,
            );
        }
        resized
    }
}
#[global_allocator]
static WITNESS: Witness = Witness;

fn arm() {
    LOG.with(|log| {
        let mut log = log.borrow_mut();
        log.clear();
        log.reserve(1 << 20);
    });
    OVERFLOWED.with(|flag| flag.set(false));
    ARMED.with(|armed| armed.set(true));
}
fn disarm() -> Vec<(usize, usize, usize, bool)> {
    ARMED.with(|armed| armed.set(false));
    assert!(
        !OVERFLOWED.with(Cell::get),
        "the witness log held every allocation"
    );
    LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

fn database() -> redb::Database {
    redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .expect("an in-memory store")
}

fn definition(name: &str) -> TableDefinition<'_, u64, u64> {
    TableDefinition::new(name)
}

/// The registry array's live bytes, reconstructed from its own records.
fn live_array_bytes(records: &[RegistryAllocationRecord]) -> usize {
    let mut live = 0usize;
    for record in records {
        if record.allocated {
            live += record.size;
        } else {
            live -= record.size;
        }
    }
    live
}

/// Every registry array allocation and free has the same address and layout
/// in the independent witness, in the same order.
fn assert_witnessed(
    records: &[RegistryAllocationRecord],
    witnessed: &[(usize, usize, usize, bool)],
) {
    let mut cursor = 0usize;
    for record in records {
        let found = witnessed[cursor..]
            .iter()
            .position(|&(address, size, align, allocated)| {
                address == record.address
                    && size == record.size
                    && align == record.align
                    && allocated == record.allocated
            })
            .unwrap_or_else(|| panic!("the global allocator saw {record:?}"));
        cursor += found + 1;
    }
}

/// Fund `additional` more open tables exactly: learn the layout by refusal,
/// check that one byte less is refused with that layout and changes nothing,
/// then pay it.
fn reserve_exactly(write: &WriteTransaction, additional: usize) -> Option<Layout> {
    let layout = write
        .open_table_registry_cost_to_reserve(additional)
        .expect("a representable reserve")?;
    let before = write.open_table_registry_bytes();
    assert_eq!(
        write.open_table_registry_reserve(additional, layout.size() - 1),
        Err(RegistryReserveError::AllocError(layout)),
        "one byte short is refused with the exact layout"
    );
    assert_eq!(
        write.open_table_registry_bytes(),
        before,
        "a refused reserve leaves the registry's allocation unchanged"
    );
    let freed = write
        .open_table_registry_reserve(additional, layout.size())
        .expect("the exact credit pays for the growth");
    assert_eq!(freed, before, "growth frees exactly the previous array");
    assert_eq!(write.open_table_registry_bytes(), layout.size());
    Some(layout)
}

#[test]
fn registry_growth_is_learned_exactly_paid_exactly_and_witnessed() {
    for baseline in [0usize, 5, 37, 700] {
        let db = database();
        let write = db.begin_write().expect("begin");
        // Recording starts before the baseline exists, so its arrays and keys
        // are observed when they are built.
        write.open_table_registry_record_for_test();
        arm();
        let names: Vec<String> = (0..baseline + 2_048).map(|n| format!("t{n:05}")).collect();
        let mut held = Vec::new();
        for name in &names[..baseline] {
            held.push(write.open_table(definition(name)).expect("open"));
        }
        let monitor = write.open_table_registry_monitor();
        // Seeded interleaving: grow by funded reserves, close and reopen to
        // leave tombstones, and never let an open grow unfunded.
        let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ baseline as u64;
        let mut open = Vec::new();
        for name in &names[baseline..] {
            reserve_exactly(&write, 1);
            open.push(write.open_table(definition(name)).expect("open"));
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            if state % 3 == 0 && open.len() > 1 {
                let index = (state >> 8) as usize % open.len();
                drop(open.swap_remove(index));
            }
        }
        assert_eq!(
            monitor.overdraft_bytes(),
            0,
            "every growth during the funded sequence was paid for"
        );
        let keys = write.open_table_registry_keys_for_test();
        assert_eq!(keys.len(), baseline + open.len());
        drop(open);
        // Put the baseline back into an array sized for it, prepared ahead.
        let replacement = match write
            .open_table_registry_cost_to_hold(baseline)
            .expect("representable")
        {
            Some(layout) => Some(
                write
                    .open_table_registry_prepare(baseline, layout.size())
                    .expect("the exact credit prepares the replacement"),
            ),
            None => None,
        };
        let grown = write.open_table_registry_bytes();
        let freed = write
            .open_table_registry_install(replacement)
            .unwrap_or_else(|(error, _)| panic!("install: {error:?}"));
        assert_eq!(freed, grown, "installing frees the grown array");
        assert_eq!(monitor.overdraft_bytes(), 0, "installing allocated nothing");
        drop(monitor);
        let (records, live, peak) = write.open_table_registry_records_for_test();
        let witnessed = disarm();
        assert_eq!(live, live_array_bytes(&records));
        assert_eq!(write.open_table_registry_bytes(), live);
        assert!(peak >= grown);
        assert_witnessed(&records, &witnessed);
        // Each key box is an allocation of exactly its name's length.
        for (address, len) in keys {
            assert!(
                witnessed
                    .iter()
                    .any(|&(at, size, _, allocated)| allocated && at == address && size == len),
                "key box at {address:#x} of {len} bytes was allocated as itself"
            );
        }
        drop(held);
        drop(write);
    }
}

#[test]
fn a_refused_probe_or_preparation_changes_nothing_and_overflow_is_distinguished() {
    let db = database();
    let write = db.begin_write().expect("begin");
    let mut held = Vec::new();
    for n in 0..100 {
        reserve_exactly(&write, 1);
        held.push(write.open_table(definition(&format!("t{n}"))).expect("open"));
    }
    let before = write.open_table_registry_bytes();
    assert_eq!(
        write.open_table_registry_cost_to_reserve(usize::MAX),
        Err(RegistryReserveError::CapacityOverflow),
        "an unrepresentable reserve is an overflow, never a stale layout"
    );
    let layout = write
        .open_table_registry_cost_to_hold(100)
        .expect("representable")
        .expect("a non-empty array");
    assert_eq!(
        write
            .open_table_registry_prepare(100, layout.size() - 1)
            .err(),
        Some(RegistryReserveError::AllocError(layout))
    );
    assert_eq!(write.open_table_registry_bytes(), before);
    // A too-small replacement is handed back with the registry unchanged.
    let small = write
        .open_table_registry_prepare(1, usize::MAX)
        .expect("a small replacement");
    let (error, returned) = write
        .open_table_registry_install(Some(small))
        .expect_err("a replacement smaller than the open tables is refused");
    assert_eq!(error, RegistryReserveError::ReplacementTooSmall);
    assert!(returned.is_some());
    assert_eq!(write.open_table_registry_bytes(), before);
    assert_eq!(write.open_table_registry_len(), 100);
    drop(held);
}

#[test]
fn an_unfunded_growth_while_monitored_is_counted_as_an_overdraft() {
    let db = database();
    let write = db.begin_write().expect("begin");
    let monitor = write.open_table_registry_monitor();
    let mut held = Vec::new();
    for n in 0..64 {
        held.push(write.open_table(definition(&format!("t{n}"))).expect("open"));
    }
    assert!(
        monitor.overdraft_bytes() > 0,
        "growth outside a funded reserve is detected"
    );
    drop(monitor);
    drop(held);
}

#[test]
fn restaging_an_already_staged_table_allocates_no_key() {
    let db = database();
    let write = db.begin_write().expect("begin");
    drop(write.open_table(definition("staged")).expect("open"));
    assert!(write.pending_table_update_contains("staged"));
    let (entries, _) = write.pending_table_updates_for_test();
    let table = write.open_table(definition("staged")).expect("reopen");
    arm();
    drop(table);
    let witnessed = disarm();
    assert!(
        !witnessed
            .iter()
            .any(|&(_, size, align, allocated)| allocated && align == 1 && size == "staged".len()),
        "closing a staged table restages it in place: {witnessed:?}"
    );
    assert_eq!(write.pending_table_updates_for_test().0, entries);
}
