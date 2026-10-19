//! Two searches over one generation stay inside the serving cache's declared
//! size, counted in live page allocations rather than in cache misses.
//!
//! Each search keeps its own directory of the pages it has resolved, so it can
//! come back to a page without taking the shared cache's lock. That directory
//! must not keep a page's memory reserved once the cache has let the page go:
//! if it does, a search paused while another one cycles the cache holds a
//! second cacheful that nothing charged, and the declared ceiling is broken
//! with no change at all in what either search returns or how often it misses.
//!
//! So this counts the page buffers that are actually allocated. It runs in its
//! own test binary because it installs a counting allocator: search A is
//! parked inside its own distance callback after it has resolved pages, search
//! B runs to completion from there and replaces the shared cache, and the
//! page-sized allocations still live at that moment are compared with the
//! cache's size. B is then run again and cancelled on its first distance, and
//! both searches must give back exactly what they were charged.

use contextdb_core::{Error, Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};
use contextdb_vector::VectorIndexLayout;
use contextdb_vector::hnsw::{
    HnswAllowedSearchLimits, HnswIndex, ReadOnlyVectorGraphPageReader,
    ReadOnlyVectorGraphPageSource, durable_reader_cache_ceiling,
};
use hnsw_rs::hnsw::HnswSearchScratchEvent;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

/// The serving page size. A page buffer is an allocation of about this many
/// bytes; nothing else this test allocates is that size.
const PAGE: usize = 64 * 1024;
const DIMENSION: usize = 768;
/// About twenty-one vectors a page, so this spans about thirty pages -- far
/// more than the cache below holds.
const POPULATION: usize = 640;
/// The cache holds four pages of the thirty.
const CACHE_PAGES: usize = 4;

struct CountingAllocator;

static LIVE_PAGE_BYTES: AtomicUsize = AtomicUsize::new(0);

fn is_page_sized(layout: Layout) -> bool {
    // A page body, with or without a reference-count header in front of it.
    (PAGE..=PAGE + 64).contains(&layout.size())
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && is_page_sized(layout) {
            LIVE_PAGE_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if is_page_sized(layout) {
            LIVE_PAGE_BYTES.fetch_sub(layout.size(), Ordering::SeqCst);
        }
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if is_page_sized(layout) {
                LIVE_PAGE_BYTES.fetch_sub(layout.size(), Ordering::SeqCst);
            }
            if is_page_sized(Layout::from_size_align(new_size, layout.align()).unwrap()) {
                LIVE_PAGE_BYTES.fetch_add(new_size, Ordering::SeqCst);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Source {
    bytes: Arc<Vec<u8>>,
    reads: Arc<AtomicUsize>,
}

struct Reader {
    bytes: Arc<Vec<u8>>,
    reads: Arc<AtomicUsize>,
}

impl ReadOnlyVectorGraphPageSource for Source {
    fn reader(&self) -> contextdb_core::Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
        Ok(Box::new(Reader {
            bytes: Arc::clone(&self.bytes),
            reads: Arc::clone(&self.reads),
        }))
    }
}

impl ReadOnlyVectorGraphPageReader for Reader {
    fn read_range(&mut self, offset: usize, len: usize) -> contextdb_core::Result<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
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

/// What one search was charged and gave back, in bytes.
#[derive(Default)]
struct Charge {
    held: Cell<isize>,
    peak: Cell<isize>,
}

impl Charge {
    fn acquire(&self, event: HnswSearchScratchEvent) {
        let bytes = match event {
            HnswSearchScratchEvent::Reserve {
                previous_capacity,
                requested_capacity,
                element_bytes,
                ..
            } => (requested_capacity - previous_capacity) * element_bytes,
            HnswSearchScratchEvent::Reconcile {
                requested_capacity,
                actual_capacity,
                element_bytes,
                ..
            } => actual_capacity.saturating_sub(requested_capacity) * element_bytes,
            HnswSearchScratchEvent::Release { .. } => panic!("a release sent as an acquire"),
        };
        self.held.set(self.held.get() + bytes as isize);
        self.peak.set(self.peak.get().max(self.held.get()));
    }

    fn release(&self, event: HnswSearchScratchEvent) {
        let HnswSearchScratchEvent::Release {
            capacity,
            element_bytes,
            ..
        } = event
        else {
            panic!("an acquire sent as a release");
        };
        self.held
            .set(self.held.get() - (capacity * element_bytes) as isize);
    }
}

static PAUSED_LIVE: AtomicIsize = AtomicIsize::new(-1);

#[test]
fn a_paused_search_holds_no_page_the_serving_cache_has_let_go() {
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
    let span = bytes.len();
    drop(graph);
    assert!(
        span > 6 * CACHE_PAGES * PAGE,
        "the partition must be many cachefuls wide: {span} bytes"
    );
    let cache_bytes = durable_reader_cache_ceiling(Some(CACHE_PAGES * PAGE), None, span);
    assert_eq!(cache_bytes, CACHE_PAGES * PAGE);
    let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
        bytes.as_slice(),
        DIMENSION,
        VectorQuantization::F32,
        m,
        cache_bytes,
    )
    .unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let paged = HnswIndex::decode_paged_durable_generation(
        bytes.as_slice(),
        Arc::new(Source {
            bytes: Arc::clone(&bytes),
            reads: Arc::clone(&reads),
        }),
        DIMENSION,
        VectorQuantization::F32,
        admitted,
        cache_bytes,
    )
    .unwrap();

    let search = |query: &[f32],
                  charge: &Charge,
                  mut on_distance: Box<dyn FnMut() -> Result<(), Error> + '_>| {
        paged
            .search_allowed_with_bounded_memory::<Error, _, _, _, _, _>(
                &index,
                query,
                10,
                POPULATION,
                HnswAllowedSearchLimits {
                    max_visited_nodes: usize::MAX,
                    max_vector_evaluations: usize::MAX,
                },
                |_| true,
                || Ok(()),
                &mut on_distance,
                |event| {
                    charge.acquire(event);
                    Ok(())
                },
                |event| charge.release(event),
            )
            .map(|answered| {
                // The caller owns the result's own storage until it drops the
                // rows; handing it back here is the caller consuming its answer.
                charge
                    .held
                    .set(charge.held.get() - answered.retained_bytes as isize);
                answered.rows
            })
    };
    let query_a = vector(1_000_001);
    let query_b = vector(2_000_002);

    // What each search answers on its own, with nothing interleaved.
    let alone = Charge::default();
    let a_alone = search(&query_a, &alone, Box::new(|| Ok(()))).unwrap();
    let b_alone = search(&query_b, &alone, Box::new(|| Ok(()))).unwrap();
    assert_eq!(
        alone.held.get(),
        0,
        "two searches in turn give back all they took"
    );

    // A pauses deep into its walk, after it has resolved many pages; B runs
    // to completion from inside that pause and cycles the shared cache.
    let a_charge = Charge::default();
    let b_charge = Charge::default();
    let distances = Cell::new(0_usize);
    let b_inside = Cell::new(None);
    let a_interleaved = search(
        &query_a,
        &a_charge,
        Box::new(|| {
            distances.set(distances.get() + 1);
            if distances.get() == 200 {
                let answered = search(&query_b, &b_charge, Box::new(|| Ok(()))).unwrap();
                PAUSED_LIVE.store(
                    LIVE_PAGE_BYTES.load(Ordering::SeqCst) as isize,
                    Ordering::SeqCst,
                );
                b_inside.set(Some(answered));
            }
            Ok(())
        }),
    )
    .unwrap();

    let paused_live = PAUSED_LIVE.load(Ordering::SeqCst);
    assert!(paused_live >= 0, "search A reached its pause");
    println!(
        "paused_live_page_bytes={paused_live} cache_bytes={cache_bytes} \
         a_peak_charge={} b_peak_charge={} source_reads={}",
        a_charge.peak.get(),
        b_charge.peak.get(),
        reads.load(Ordering::SeqCst),
    );
    // While A is parked between two distances it is reading no page, and B
    // has ended, so every page buffer still allocated must be one the cache
    // holds. A reference-count header is allowed per page; a second page is
    // not.
    assert!(
        paused_live as usize <= CACHE_PAGES * (PAGE + 64),
        "with search A parked and search B finished, {paused_live} bytes of page buffers are \
         still allocated against a {cache_bytes}-byte serving cache: pages the cache let go are \
         being kept by a search's own directory and nothing charged them"
    );
    assert_eq!(
        a_interleaved, a_alone,
        "search A answers the same whether or not another search cycled the cache under it"
    );
    assert_eq!(
        b_inside.take(),
        Some(b_alone.clone()),
        "search B answers the same run inside another search's pause"
    );
    assert_eq!(
        a_charge.held.get(),
        0,
        "search A gives back exactly what it took"
    );
    assert_eq!(
        b_charge.held.get(),
        0,
        "search B gives back exactly what it took"
    );

    // A search cancelled on its very first distance also gives back exactly
    // what it took.
    let cancelled = Charge::default();
    let refused = search(
        &query_b,
        &cancelled,
        Box::new(|| Err(Error::Other("cancelled".to_string().into()))),
    );
    assert!(
        refused.is_err(),
        "a cancelled search reports its cancellation"
    );
    assert_eq!(
        cancelled.held.get(),
        0,
        "a search cancelled at its first distance gives back exactly what it took"
    );

    // Once no search is running, nothing but the cache holds a page.
    drop(paged);
    assert_eq!(
        LIVE_PAGE_BYTES.load(Ordering::SeqCst),
        0,
        "dropping the generation frees every page buffer"
    );
}
