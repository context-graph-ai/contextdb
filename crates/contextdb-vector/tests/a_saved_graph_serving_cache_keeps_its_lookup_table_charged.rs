//! A saved search graph's serving cache is admitted, and stays retained, for
//! every allocation it holds while it is live -- including the hash table
//! that finds a cache slot from a page offset (`slot_by_offset`).
//!
//! `PagedVectorPageCache::new` builds that table with `HashMap::with_capacity(slots)`
//! before the cache answers a single lookup, and it grows with the number of
//! serving-cache slots -- but the admission bound and the retained-bytes
//! figure price a cache only by its slot directory and its page bodies, never
//! by this table. A graph's own topology dwarfs the table at any one cache
//! size, which would hide a modest omission in an absolute comparison. So
//! this compares the reported retained figure's own GROWTH, as the cache is
//! given more slots, against the process's real allocated-byte GROWTH for
//! the same slot increase, measured with a counting allocator, right after
//! opening a fresh reader and before it answers a single lookup. The graph
//! itself is identical for every slot count, so its topology cancels out of
//! both sides of that comparison; the reserved page-body bytes are not yet
//! real heap at this point (pages are read lazily) and cancel out too, once
//! excluded from both sides the same way; only what genuinely scales with
//! slots survives, and the table is the one real, slot-scaling allocation
//! neither side is currently pricing.
//!
//! The only slot count under test here is 8 and 96, the two ends: the real
//! table grows by 1,904 bytes over that span (measured independently below:
//! a `HashMap<usize, usize>` built the same way for 8 vs. 96 slots, touching
//! nothing this test owns). The reader is opened through a bare page source
//! that tracks nothing and is never even read from at this point; the source
//! itself is the only thing this test allocates in the measured window, and
//! it is the same fixed, tiny size at every slot count, so it cancels
//! exactly out of the cross-size comparison. The only real growth left to
//! explain is the reader's own.
//!
//! This runs in its own test binary because it installs a counting allocator.

use contextdb_core::{Error, Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};
use contextdb_vector::VectorIndexLayout;
use contextdb_vector::hnsw::{
    HnswIndex, ReadOnlyVectorGraphPageReader, ReadOnlyVectorGraphPageSource,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

/// The serving page size the reader aligns its cache to.
const PAGE: usize = 64 * 1024;
const DIMENSION: usize = 384;
/// Large enough that the encoded generation spans at least the largest
/// cache size under test (checked below), so `page_slots` is never capped
/// down by the generation's own length.
const POPULATION: usize = 4_500;

struct CountingAllocator;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::SeqCst);
            LIVE_BYTES.fetch_add(new_size as isize, Ordering::SeqCst);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// A page source that tracks nothing of its own. It is only ever asked for
/// a `reader()` handle by production code, and this proof never triggers an
/// actual page read, so this struct's own (fixed, one-time) allocation is
/// the only thing it can possibly add, and it is built before every
/// baseline below, so even that is excluded from what gets measured.
struct Source {
    bytes: Arc<Vec<u8>>,
}

struct Reader {
    bytes: Arc<Vec<u8>>,
}

impl ReadOnlyVectorGraphPageSource for Source {
    fn reader(&self) -> contextdb_core::Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
        Ok(Box::new(Reader {
            bytes: Arc::clone(&self.bytes),
        }))
    }
}

impl ReadOnlyVectorGraphPageReader for Reader {
    fn read_range(&mut self, offset: usize, len: usize) -> contextdb_core::Result<Vec<u8>> {
        self.bytes
            .get(offset..offset.saturating_add(len))
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Error::Other("page out of range".to_string().into()))
    }
}

fn vector(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

/// What a `HashMap<usize, usize>` built with this requested capacity really
/// allocates -- the same type, and the same `with_capacity` call,
/// `PagedVectorPageCache::new` uses for `slot_by_offset`. This never
/// touches a reader or any production admission/retained figure; it is
/// ground truth for the table alone.
fn real_hashmap_bytes(capacity: usize) -> isize {
    let baseline = LIVE_BYTES.load(Ordering::SeqCst);
    let map: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::with_capacity(capacity);
    let after = LIVE_BYTES.load(Ordering::SeqCst);
    drop(map);
    after - baseline
}

/// Opens a fresh paged reader at this many serving-cache slots and returns
/// what the process had really allocated by the time it returned, together
/// with what the reader itself reports retained and admitted. No page is
/// ever read: page bodies are resolved lazily, so `cache_bytes` worth of
/// reservation is not real heap yet at this point, on either side of the
/// comparison this feeds.
fn open(bytes: &Arc<Vec<u8>>, m: usize, slots: usize) -> (isize, usize, usize) {
    let cache_bytes = slots * PAGE;
    let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
        bytes.as_slice(),
        DIMENSION,
        VectorQuantization::F32,
        m,
        cache_bytes,
    )
    .unwrap();

    let baseline = LIVE_BYTES.load(Ordering::SeqCst);
    // The only allocation this test owns in the measured window: a fixed,
    // tiny, constant size at every slot count (it does not depend on
    // `slots`), so it cancels exactly out of the cross-size comparison
    // below. It is built after the baseline and moves into `paged`, so its
    // matching free (when `paged` drops) stays inside this same window too.
    let source: Arc<dyn ReadOnlyVectorGraphPageSource> = Arc::new(Source {
        bytes: Arc::clone(bytes),
    });
    let paged = HnswIndex::decode_paged_durable_generation(
        bytes.as_slice(),
        source,
        DIMENSION,
        VectorQuantization::F32,
        usize::MAX,
        cache_bytes,
    )
    .unwrap();
    let after_open = LIVE_BYTES.load(Ordering::SeqCst);
    let actual_measured = after_open - baseline;
    let retained = paged.estimated_resident_bytes();
    assert!(
        retained <= admitted,
        "slots={slots}: retained {retained} bytes exceeds the admitted bound of {admitted} bytes"
    );
    drop(paged);
    let after_drop = LIVE_BYTES.load(Ordering::SeqCst);
    assert_eq!(
        after_drop,
        baseline,
        "slots={slots}: dropping the reader left {} bytes still allocated",
        after_drop - baseline
    );
    println!(
        "lookup_table_charge slots={slots} cache_bytes={cache_bytes} actual_measured={actual_measured} \
         retained={retained} admitted={admitted}"
    );
    (actual_measured, retained, admitted)
}

#[test]
fn a_saved_graph_serving_cache_keeps_its_lookup_table_charged() {
    let index = VectorIndexRef::new("items", "embedding");
    let entries = (0..POPULATION)
        .map(|ordinal| VectorEntry {
            index: index.clone(),
            row_id: RowId(ordinal as u64 + 1),
            vector: vector(ordinal),
            created_tx: TxId(ordinal as u64 + 1),
            deleted_tx: None,
            lsn: Lsn(ordinal as u64 + 1),
        })
        .collect::<Vec<_>>();
    let policy = VectorIndexLayout::unpartitioned(DIMENSION, VectorQuantization::F32)
        .resolve_policy(POPULATION, 10);
    let graph = HnswIndex::from_vector_entries_with_policy(
        &entries,
        DIMENSION,
        VectorQuantization::F32,
        policy,
    );
    let (m, _, _) = graph.policy_values();
    let bytes = Arc::new(graph.encode_durable_generation().unwrap());
    drop(graph);

    // The two ends only: the widest span this fixture supports, so the
    // missing table's own growth (measured independently below) clears any
    // slack by a wide margin.
    const SMALL_SLOTS: usize = 8;
    const LARGE_SLOTS: usize = 96;
    let largest_cache_bytes = LARGE_SLOTS * PAGE;
    assert!(
        bytes.len() >= largest_cache_bytes,
        "the fixture must span at least the largest cache under test: {} bytes vs a \
         {largest_cache_bytes}-byte cache; grow POPULATION or DIMENSION",
        bytes.len()
    );

    // Ground truth, independent of the reader entirely: what a
    // `HashMap<usize, usize>` built exactly the way `PagedVectorPageCache`
    // builds its lookup table really allocates, at each slot count.
    let table_bytes_small = real_hashmap_bytes(SMALL_SLOTS);
    let table_bytes_large = real_hashmap_bytes(LARGE_SLOTS);
    let table_growth_independent = table_bytes_large - table_bytes_small;
    println!(
        "lookup_table_independent small_slots={SMALL_SLOTS} table_bytes={table_bytes_small} \
         large_slots={LARGE_SLOTS} table_bytes={table_bytes_large} \
         growth={table_growth_independent}"
    );
    assert!(
        table_growth_independent > 0,
        "the lookup table itself must really grow with slot count, or this fixture proves \
         nothing: {table_growth_independent}"
    );

    let (small_actual, small_retained, _) = open(&bytes, m, SMALL_SLOTS);
    let (large_actual, large_retained, _) = open(&bytes, m, LARGE_SLOTS);
    let real_growth = large_actual - small_actual;
    // Every slot's reservation that is not yet real heap at this point (no
    // page has been read, so no page body and no shared handle exists):
    // the page body's own `cache_bytes` share, and the handle a page would
    // carry once installed -- two `Arc` reference counts and a boxed byte
    // slice's own fat pointer, the same shape the reader's own page-body
    // handle has (computed from `size_of`, not measured, since nothing
    // here ever installs a page). Excluded identically from both sides, so
    // only what is genuinely real right now, and scales with slots,
    // survives the comparison.
    let handle_bytes_per_slot = 2 * std::mem::size_of::<usize>() + std::mem::size_of::<Box<[u8]>>();
    let unread_reservation_per_slot = (PAGE + handle_bytes_per_slot) as isize;
    let reported_growth = (large_retained as isize
        - LARGE_SLOTS as isize * unread_reservation_per_slot)
        - (small_retained as isize - SMALL_SLOTS as isize * unread_reservation_per_slot);
    println!(
        "lookup_table_growth {SMALL_SLOTS}->{LARGE_SLOTS} real_growth={real_growth} \
         reported_growth={reported_growth} table_growth_independent={table_growth_independent} \
         handle_bytes_per_slot={handle_bytes_per_slot}"
    );

    // Zero variable slack: nothing this test owns allocates inside the
    // measured window on either side, so real growth beyond the unread
    // reservation must equal reported growth exactly, up to a handful of
    // bytes of ordinary allocator bookkeeping.
    const SLACK_BYTES: isize = 32;
    assert!(
        real_growth <= reported_growth + SLACK_BYTES,
        "{SMALL_SLOTS}->{LARGE_SLOTS} slots: the process really allocated {real_growth} more \
         bytes opening the cache (beyond its reserved page bytes), but the reader's own \
         retained figure only grew by {reported_growth} bytes there -- {} bytes of real, \
         per-slot-scaling growth are unaccounted, close to the lookup table's own independently \
         measured {table_growth_independent}-byte growth over the same span",
        real_growth - reported_growth
    );

    // Internal-bound evidence, not the public pre-allocation refusal promise
    // (that promise is proved at the engine's own admission gate, in
    // `a_declared_graph_degree_loads_when_its_memory_fits.rs`, which refuses
    // through `MemoryBudgetExceeded` before this decoder ever runs): a
    // budget one byte short of what the fresh largest-cache reader retains must
    // still be refused by the decoder's own bound check once it decodes,
    // rather than silently opening an oversized reader. This leg always
    // runs, never gated on a charge that happened to measure as zero.
    let small_cache_bytes = LARGE_SLOTS * PAGE;
    let admitted_full = HnswIndex::durable_reader_resident_bound_for_cache(
        bytes.as_slice(),
        DIMENSION,
        VectorQuantization::F32,
        m,
        small_cache_bytes,
    )
    .unwrap();
    assert!(
        admitted_full > 0,
        "the admission bound for a live cache size must be positive"
    );
    let control = HnswIndex::decode_paged_durable_generation(
        bytes.as_slice(),
        Arc::new(Source {
            bytes: Arc::clone(&bytes),
        }),
        DIMENSION,
        VectorQuantization::F32,
        admitted_full,
        small_cache_bytes,
    )
    .unwrap();
    let control_resident = control.estimated_resident_bytes();
    drop(control);
    let starved_budget = control_resident - 1;
    let refused = HnswIndex::decode_paged_durable_generation(
        bytes.as_slice(),
        Arc::new(Source {
            bytes: Arc::clone(&bytes),
        }),
        DIMENSION,
        VectorQuantization::F32,
        starved_budget,
        small_cache_bytes,
    );
    match refused {
        Err(Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        }) => {
            assert_eq!(requested_bytes, control_resident);
            assert_eq!(available_bytes, requested_bytes - 1);
            assert_eq!(budget_limit_bytes, requested_bytes - 1);
        }
        Err(other) => panic!(
            "a budget of {starved_budget} bytes, one byte below the {control_resident}-byte \
             resident size of the fresh control, must be refused with the typed error, not: {other}"
        ),
        Ok(_) => panic!(
            "a budget of {starved_budget} bytes, one byte below the {control_resident}-byte \
             resident size of the fresh control, must be refused, not opened"
        ),
    }
}
