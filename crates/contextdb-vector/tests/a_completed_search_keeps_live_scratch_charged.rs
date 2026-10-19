//! A graph search keeps every allocation it is still using charged until that
//! allocation is freed or handed to the caller.
//!
//! A caller that shares one memory allowance between searches admits the next
//! allocation against what the current one has given back. If a search gives
//! a charge back while the memory it covers is still allocated -- a directory
//! still owned by the walk, or a result heap the answer is still being copied
//! out of -- another search can be admitted into memory that is in use, and the
//! declared limit stops being true without any total ever going wrong.
//!
//! So this compares what is ALLOCATED with what is CHARGED, at every point the
//! search reports to its caller: each reservation, each release, each distance
//! and each source entry. It does so for a search that answers, and again for
//! a search whose caller refuses each of its reservations in turn, because a
//! refused search must stop inside its allowance too. It covers three routes:
//! the unfiltered walk, a filtered walk whose filter admits every row, and one
//! whose filter admits so few rows that the walk has to step through excluded
//! ones and then try again through all of them. It runs in its own test
//! binary, with one test in it, because it installs a counting allocator.

use contextdb_core::{Error, Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};
use contextdb_vector::VectorIndexLayout;
use contextdb_vector::hnsw::{
    HnswAllowedSearchLimits, HnswIndex, ReadOnlyVectorGraphPageReader,
    ReadOnlyVectorGraphPageSource, durable_reader_cache_ceiling,
};
use hnsw_rs::hnsw::{HnswSearchScratchContainer, HnswSearchScratchEvent};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::{Arc, Weak};

const DIMENSION: usize = 768;
const POPULATION: usize = 1_250;
/// A declared degree large enough that one point's neighbour list is
/// kilobytes, so a list the walk copies without asking cannot hide inside the
/// uncharged allowance below.
const DECLARED_DEGREE: u32 = 64;
/// The restrictive filter admits one row in this many.
const ADMIT_ONE_IN: u64 = 125;
/// Memory a search allocates that no reservation covers by design, item by
/// item. There is one: the page reader its source opens, boxed -- here the
/// test's `Reader`, one pointer. Neighbour lists are read in place, never
/// copied, so the declared degree adds nothing here. Anything a search holds
/// past this is scratch that has been given back while still allocated, or
/// allocated without being charged.
const UNCHARGED_ALLOWANCE: isize = std::mem::size_of::<Reader>() as isize;

/// What one page-directory entry itself allocates. The directory's charge per
/// entry is larger: it also covers the handle of a page the serving cache
/// installed, which the cache allocated before this search began, so it is
/// never this search's allocation and cannot excuse one.
const DIRECTORY_ENTRY_ALLOCATION: usize = std::mem::size_of::<Option<(usize, Weak<()>)>>();

struct CountingAllocator;

/// Whether allocations are being counted. Only the searches below turn it on,
/// and this binary holds one test, so nothing else allocates while it is on.
static COUNTING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && COUNTING.load(Ordering::SeqCst) {
            LIVE.fetch_add(layout.size() as isize, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if COUNTING.load(Ordering::SeqCst) {
            LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        }
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() && COUNTING.load(Ordering::SeqCst) {
            LIVE.fetch_add(new_size as isize - layout.size() as isize, Ordering::SeqCst);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Source(Arc<Vec<u8>>);
struct Reader(Arc<Vec<u8>>);

impl ReadOnlyVectorGraphPageSource for Source {
    fn reader(&self) -> contextdb_core::Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
        Ok(Box::new(Reader(Arc::clone(&self.0))))
    }
}

impl ReadOnlyVectorGraphPageReader for Reader {
    fn read_range(&mut self, offset: usize, len: usize) -> contextdb_core::Result<Vec<u8>> {
        self.0
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

fn named(container: HnswSearchScratchContainer, releasing: bool) -> &'static str {
    use HnswSearchScratchContainer as C;
    match (container, releasing) {
        (C::CandidateHeap, false) => "reserving the candidate heap",
        (C::CandidateHeap, true) => "after releasing the candidate heap",
        (C::ReturnHeap, false) => "reserving the result heap",
        (C::ReturnHeap, true) => "after releasing the result heap",
        (C::VisitedPoints, false) => "reserving the reached points",
        (C::VisitedPoints, true) => "after releasing the reached points",
        (C::Result, false) => "reserving result rows",
        (C::Result, true) => "after releasing result rows",
        (C::MergeResults, false) => "reserving merged results",
        (C::MergeResults, true) => "after releasing merged results",
        (C::SeenRows, false) => "reserving seen rows",
        (C::SeenRows, true) => "after releasing seen rows",
        (C::QueryBytes, false) => "reserving query bytes",
        (C::QueryBytes, true) => "after releasing query bytes",
        (C::ExactKey, false) => "reserving the exact key",
        (C::ExactKey, true) => "after releasing the exact key",
        (C::ServingPageDirectory, false) => "reserving the page directory",
        (C::ServingPageDirectory, true) => "after releasing the page directory",
    }
}

/// Of one charged element, the bytes that cover an allocation the search makes.
fn covered_bytes(container: HnswSearchScratchContainer, element_bytes: usize) -> usize {
    if container == HnswSearchScratchContainer::ServingPageDirectory {
        assert!(element_bytes >= DIRECTORY_ENTRY_ALLOCATION);
        DIRECTORY_ENTRY_ALLOCATION
    } else {
        element_bytes
    }
}

/// What the search has charged, the worst moment it was using more than that,
/// and every point at which that excess changed.
struct Ledger {
    charged: Cell<isize>,
    /// The part of `charged` that covers memory this search allocates: all of
    /// it but the page handles a directory's charge covers for the cache.
    covering: Cell<isize>,
    worst: Cell<isize>,
    worst_at: Cell<&'static str>,
    last: Cell<isize>,
    /// Allocated before counting starts and never grown past its capacity,
    /// so recording into it allocates nothing.
    trace: RefCell<Vec<(&'static str, isize)>>,
    /// The reservation, counted from zero, this caller refuses; none if it
    /// admits every one.
    refuse_at: Option<usize>,
    reservations: Cell<usize>,
    refused: Cell<bool>,
    /// How many graph walks the search started: two means the filtered walk
    /// discarded a first attempt and tried again.
    walks: Cell<usize>,
}

impl Ledger {
    fn new() -> Self {
        Self::refusing(None)
    }

    fn refusing(refuse_at: Option<usize>) -> Self {
        Self {
            charged: Cell::new(0),
            covering: Cell::new(0),
            worst: Cell::new(isize::MIN),
            worst_at: Cell::new("nowhere"),
            last: Cell::new(0),
            trace: RefCell::new(Vec::with_capacity(1 << 16)),
            refuse_at,
            reservations: Cell::new(0),
            refused: Cell::new(false),
            walks: Cell::new(0),
        }
    }

    fn look(&self, at: &'static str) {
        let over = LIVE.load(Ordering::SeqCst) - self.covering.get();
        if over != self.last.get() {
            self.last.set(over);
            let mut trace = self.trace.borrow_mut();
            if trace.len() < trace.capacity() {
                trace.push((at, over));
            }
        }
        if over > self.worst.get() {
            self.worst.set(over);
            self.worst_at.set(at);
        }
    }

    fn acquire(&self, event: HnswSearchScratchEvent) -> Result<(), Error> {
        let (container, bytes, covering) = match event {
            HnswSearchScratchEvent::Reserve {
                container,
                previous_capacity,
                requested_capacity,
                element_bytes,
            } => (
                container,
                (requested_capacity - previous_capacity) * element_bytes,
                (requested_capacity - previous_capacity) * covered_bytes(container, element_bytes),
            ),
            HnswSearchScratchEvent::Reconcile {
                container,
                requested_capacity,
                actual_capacity,
                element_bytes,
            } => (
                container,
                actual_capacity.saturating_sub(requested_capacity) * element_bytes,
                actual_capacity.saturating_sub(requested_capacity)
                    * covered_bytes(container, element_bytes),
            ),
            HnswSearchScratchEvent::Release { .. } => panic!("a release sent as an acquire"),
        };
        // Before the charge lands: the allocation it announces has not been
        // made yet, so this looks at the moment just after whatever came before.
        self.look(named(container, false));
        let reservation = self.reservations.get();
        self.reservations.set(reservation + 1);
        if self.refuse_at == Some(reservation) {
            self.refused.set(true);
            // A refusal that allocates nothing, so the ledger measures only
            // what the search itself still holds.
            return Err(Error::ReadCancelled);
        }
        if matches!(event, HnswSearchScratchEvent::Reserve { .. })
            && container == HnswSearchScratchContainer::CandidateHeap
        {
            self.walks.set(self.walks.get() + 1);
        }
        self.charged.set(self.charged.get() + bytes as isize);
        self.covering.set(self.covering.get() + covering as isize);
        Ok(())
    }

    /// Every point the search was over the allowance, so a failure names each
    /// premature release rather than only the worst one; a run of steps at the
    /// same kind of point is shown once.
    fn show(&self, route: &str) {
        let mut shown = "";
        for (at, over) in self.trace.borrow().iter() {
            if *over > UNCHARGED_ALLOWANCE && *at != shown {
                println!("trace route={route} over={over} {at}");
                shown = at;
            }
        }
    }

    fn release(&self, event: HnswSearchScratchEvent) {
        let HnswSearchScratchEvent::Release {
            container,
            capacity,
            element_bytes,
        } = event
        else {
            panic!("an acquire sent as a release");
        };
        self.charged
            .set(self.charged.get() - (capacity * element_bytes) as isize);
        self.covering.set(
            self.covering.get() - (capacity * covered_bytes(container, element_bytes)) as isize,
        );
        // After the charge is given back: the memory it covered must be gone.
        self.look(named(container, true));
    }
}

#[test]
fn a_completed_search_keeps_every_live_allocation_charged() {
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
    // A declared breadth of 400, so the result heap is thousands of bytes and
    // a heap released early cannot hide inside the uncharged allowance.
    let policy = VectorIndexLayout::unpartitioned(DIMENSION, VectorQuantization::F32)
        .with_policy(None, Some(DECLARED_DEGREE), None, Some(400), 0)
        .resolve_policy(POPULATION, 10);
    let graph = HnswIndex::from_vector_entries_with_policy(
        &entries,
        DIMENSION,
        VectorQuantization::F32,
        policy,
    );
    let (m, _, ef_search) = graph.policy_values();
    assert_eq!(ef_search, 400);
    assert_eq!(m, DECLARED_DEGREE as usize);
    let bytes = Arc::new(graph.encode_durable_generation().unwrap());
    drop(graph);
    let cache_bytes = durable_reader_cache_ceiling(None, None, bytes.len());
    let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
        bytes.as_slice(),
        DIMENSION,
        VectorQuantization::F32,
        m,
        cache_bytes,
    )
    .unwrap();
    let paged = HnswIndex::decode_paged_durable_generation(
        bytes.as_slice(),
        Arc::new(Source(Arc::clone(&bytes))),
        DIMENSION,
        VectorQuantization::F32,
        admitted,
        cache_bytes,
    )
    .unwrap();
    let query = vector(1_000_003);
    let admitted_rows = (1..=POPULATION as u64)
        .filter(|row| row % ADMIT_ONE_IN == 0)
        .count();

    let unfiltered = |ledger: &Ledger| {
        paged
            .search_with_bounded_memory::<Error, _, _, _, _>(
                &index,
                &query,
                10,
                || {
                    ledger.look("at a source entry");
                    Ok(())
                },
                || {
                    ledger.look("at a distance");
                    Ok(())
                },
                |event| ledger.acquire(event),
                |event| ledger.release(event),
            )
            .map(|answered| (answered.rows, answered.retained_bytes))
    };
    let filtered_by = |ledger: &Ledger, allowed_count: usize, allow: fn(RowId) -> bool| {
        paged
            .search_allowed_with_bounded_memory::<Error, _, _, _, _, _>(
                &index,
                &query,
                10,
                allowed_count,
                HnswAllowedSearchLimits {
                    max_visited_nodes: usize::MAX,
                    max_vector_evaluations: usize::MAX,
                },
                allow,
                || {
                    ledger.look("at a source entry");
                    Ok(())
                },
                || {
                    ledger.look("at a distance");
                    Ok(())
                },
                |event| ledger.acquire(event),
                |event| ledger.release(event),
            )
            .map(|answered| (answered.rows, answered.retained_bytes))
    };
    let filtered = |ledger: &Ledger| filtered_by(ledger, POPULATION, |_| true);
    // Ten scattered rows, out of a scope the caller counts at twice that --
    // the rest of its admitted rows live outside this graph, in a newer layer.
    // The walk between admitted rows cannot reach the count it was promised,
    // so it gives up that first attempt and tries again navigating through
    // every row.
    let restrictive =
        |ledger: &Ledger| filtered_by(ledger, 2 * admitted_rows, |row| row.0 % ADMIT_ONE_IN == 0);

    // Every broken promise on every route, so one run names all of them.
    let mut broken = Vec::new();
    for (route, search) in [
        ("filtered", &filtered as &dyn Fn(&Ledger) -> _),
        ("restrictive", &restrictive),
        ("unfiltered", &unfiltered),
    ] {
        // Warm the serving cache first: pages it installs belong to the
        // generation's own admission, not to any one search.
        search(&Ledger::new()).expect("the route answers");

        let ledger = Ledger::new();
        LIVE.store(0, Ordering::SeqCst);
        COUNTING.store(true, Ordering::SeqCst);
        let answered = search(&ledger);
        ledger.look("when the search returned");
        COUNTING.store(false, Ordering::SeqCst);
        let (rows, retained) = answered.expect("the route answers");
        // The unfiltered route hands back its whole candidate pool for the
        // caller to rank; the filtered ones the ten they were asked for.
        assert!(rows.len() >= 10, "{route}: the search answers in full");
        if route == "restrictive" {
            assert_eq!(
                ledger.walks.get(),
                2,
                "restrictive: the filter must force the walk to discard a first attempt and \
                 try again, or this route proves nothing about that retry"
            );
        }
        ledger.show(route);
        println!(
            "live_over_charged route={route} worst={} at=\"{}\" charged={} retained={retained}",
            ledger.worst.get(),
            ledger.worst_at.get(),
            ledger.charged.get(),
        );
        if ledger.worst.get() > UNCHARGED_ALLOWANCE {
            broken.push(format!(
                "{route}: {} bytes were allocated beyond what the search had charged, {} -- a \
                 charge was given back while the memory it covers was still in use, or memory \
                 was allocated without a charge",
                ledger.worst.get(),
                ledger.worst_at.get()
            ));
        }
        if ledger.charged.get() != retained as isize {
            broken.push(format!(
                "{route}: an answered search holds exactly its returned result, {retained} \
                 bytes, but {} are still charged",
                ledger.charged.get()
            ));
        }
        let reservations = ledger.reservations.get();
        drop(rows);

        // Refuse each reservation in turn. However far the search got, it
        // must give back everything it was charged, and never while the memory
        // is still allocated.
        let mut worst_refused = isize::MIN;
        for refuse_at in 0..reservations {
            let ledger = Ledger::refusing(Some(refuse_at));
            LIVE.store(0, Ordering::SeqCst);
            COUNTING.store(true, Ordering::SeqCst);
            let answered = search(&ledger);
            ledger.look("when the refused search returned");
            COUNTING.store(false, Ordering::SeqCst);
            assert!(
                ledger.refused.get(),
                "{route}: reservation {refuse_at} of {reservations} was never asked for"
            );
            let refused = answered.is_err();
            drop(answered);
            assert!(
                refused,
                "{route}: refusing reservation {refuse_at} fails the search"
            );
            if ledger.worst.get() > UNCHARGED_ALLOWANCE || ledger.charged.get() != 0 {
                ledger.show(route);
            }
            if ledger.worst.get() > UNCHARGED_ALLOWANCE {
                broken.push(format!(
                    "{route}: refusing reservation {refuse_at}: {} bytes were allocated beyond \
                     what the search had charged, {}",
                    ledger.worst.get(),
                    ledger.worst_at.get()
                ));
            }
            if ledger.charged.get() != 0 {
                broken.push(format!(
                    "{route}: refusing reservation {refuse_at}: a refused search gives back \
                     every charge it was granted, but {} bytes are still charged",
                    ledger.charged.get()
                ));
            }
            worst_refused = worst_refused.max(ledger.worst.get());
        }
        println!("refused route={route} reservations={reservations} worst={worst_refused}");
    }
    assert!(broken.is_empty(), "{}", broken.join("\n"));
}
