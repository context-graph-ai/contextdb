#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.
//! What the serving-page charge proofs share: a counting allocator, a budget
//! the test can fill, a saved paged graph installed in a vector store, and
//! graph searches that can be parked at chosen points of their page reads.
//!
//! Each proof that uses this is its own test binary, because this installs a
//! global counting allocator. Live heap bytes are counted over every thread,
//! so a binary's proofs are run one at a time through [`serial`].

#![allow(dead_code)]

#[path = "../fixtures/read_owner_test_support.rs"]
mod read_owner_test_support;

use contextdb_core::{
    Error, Lsn, RowId, SnapshotId, TxId, VectorEntry, VectorIndexRef, VectorPartitionKey,
    VectorQuantization, VectorSearchMode,
};
use contextdb_tx::{TransactionManager, WriteSet, WriteSetApplicator};
use contextdb_vector::hnsw::{
    HnswAllowedSearchLimits, HnswIndex, ReadOnlyVectorGraphPageReader,
    ReadOnlyVectorGraphPageSource,
};
use contextdb_vector::mem::BoundedHnswOutcome;
use contextdb_vector::store::{LoadedVectorGraphGeneration, VectorGraphGeneration};
use contextdb_vector::{
    MemVectorExecutor, MemoryBudget, VectorCopyAdmission, VectorIndexLayout, VectorPartitionRef,
    VectorStore,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

pub const PAGE: usize = 64 * 1024;
pub const DIMENSION: usize = 768;
/// About twenty-one vectors a page, so about thirty pages: far more than the
/// serving cache holds.
pub const POPULATION: usize = 640;
/// The serving cache holds eight of those pages.
pub const CACHE_PAGES: usize = 8;
/// What the comparisons allow for allocator rounding and a page's small
/// shared handle: far below one page, so one page allocated without a charge
/// cannot hide inside it.
pub const ALLOWANCE: isize = 4 * 1024;
const PARK_TIMEOUT: Duration = Duration::from_secs(60);

struct CountingAllocator;

/// Heap bytes allocated and not yet freed, over every thread.
static LIVE: AtomicIsize = AtomicIsize::new(0);

std::thread_local! {
    /// Set on a thread while the allocations it makes are being counted.
    pub static WATCHED: Cell<bool> = const { Cell::new(false) };
    /// Bytes allocated on this thread while it was watched.
    pub static ALLOCATED_WHILE_WATCHED: Cell<usize> = const { Cell::new(0) };
    /// Set by the page reader on the thread that just read a serving page,
    /// with the offset it read.
    static FILLED: Cell<Option<usize>> = const { Cell::new(None) };
    /// Called by the page reader with a read's offset and length before it
    /// reads anything.
    static BEFORE_READ: RefCell<Option<Box<dyn FnMut(usize, usize)>>> = const { RefCell::new(None) };
}

fn note_allocated(bytes: usize) {
    let _ = WATCHED.try_with(|watched| {
        if watched.get() {
            let _ = ALLOCATED_WHILE_WATCHED.try_with(|total| total.set(total.get() + bytes));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::SeqCst);
            note_allocated(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::SeqCst);
            note_allocated(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            LIVE.fetch_add(new_size as isize - layout.size() as isize, Ordering::SeqCst);
            // A reallocation may place the new block before it frees the old
            // one, so the whole new size counts as allocated.
            note_allocated(new_size);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

pub fn live() -> isize {
    LIVE.load(Ordering::SeqCst)
}

/// Run one proof at a time in this binary: live bytes are process-wide.
pub fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A declared budget whose limit the test moves, and which counts any release
/// of bytes it was not holding.
#[derive(Debug)]
pub struct Budget {
    pub limit: AtomicUsize,
    used: AtomicUsize,
    pub underflows: AtomicUsize,
}

impl Budget {
    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        loop {
            let used = self.used.load(Ordering::SeqCst);
            let limit = self.limit.load(Ordering::SeqCst);
            let available = limit.saturating_sub(used);
            if bytes > available {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_string(),
                    operation: operation.to_string(),
                    requested_bytes: bytes,
                    available_bytes: available,
                    budget_limit_bytes: limit,
                    hint: hint.to_string(),
                });
            }
            if self
                .used
                .compare_exchange(used, used + bytes, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release(&self, bytes: usize) {
        let _ = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                if bytes > used {
                    self.underflows.fetch_add(1, Ordering::SeqCst);
                }
                Some(used.saturating_sub(bytes))
            });
    }
}

pub struct TestStore {
    vector: Arc<VectorStore>,
    body_owner: Arc<Budget>,
}

impl WriteSetApplicator for TestStore {
    fn apply(
        &self,
        ws: &WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> contextdb_core::Result<()> {
        let executing_owner = read_owner_test_support::budget(read_memory);
        // Capture the exact committed body before this fixture publishes inserts.
        let mut before_owner = VectorCopyAdmission::new(executing_owner.clone());
        before_owner.reserve(
            ws.vector_deletes
                .len()
                .saturating_mul(std::mem::size_of::<(VectorIndexRef, RowId, TxId, TxId, Lsn)>())
                .saturating_add(
                    ws.vector_deletes
                        .iter()
                        .map(|(index, _, _)| index.table.len().saturating_add(index.column.len()))
                        .sum::<usize>(),
                ),
        )?;
        let mut deletes = Vec::with_capacity(ws.vector_deletes.len());
        for (index, row, tx) in &ws.vector_deletes {
            let before = self
                .vector
                .live_entry_for_row(
                    index,
                    *row,
                    contextdb_core::SnapshotId(tx.0),
                    &mut before_owner,
                )?
                .expect("the fixture deletes an existing committed body");
            deletes.push((index.clone(), *row, *tx, before.created_tx, before.lsn));
        }

        let bytes = ws
            .vector_inserts
            .iter()
            .map(|entry| entry.vector.len() * std::mem::size_of::<f32>())
            .sum();
        self.body_owner
            .try_allocate_for(bytes, "test", "caller_vector_body", "test")?;
        let result = self.vector.apply_inserts(
            ws.vector_inserts.clone(),
            VectorCopyAdmission::new(executing_owner.clone()),
        );
        if result.is_err() {
            self.body_owner.release(bytes);
        }
        result?;
        self.vector
            .apply_deletes(deletes, VectorCopyAdmission::new(executing_owner.clone()))?;
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(1)
    }
}

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
        let hook = BEFORE_READ.with(|hook| hook.borrow_mut().take());
        if let Some(mut hook) = hook {
            hook(offset, len);
            BEFORE_READ.with(|slot| *slot.borrow_mut() = Some(hook));
        }
        let page = self
            .bytes
            .get(offset..offset.saturating_add(len))
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Error::Other("page out of range".to_string()));
        FILLED.with(|filled| filled.set(Some(offset)));
        page
    }
}

pub fn vector(ordinal: usize) -> Vec<f32> {
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

/// Where a parked search is, told to the main thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parked {
    /// Inside a page read, before the page is allocated.
    InItsRead,
    /// Has just read a page and offered it to the serving cache, and is
    /// handing back the charge it read the page under. A page the cache kept
    /// is still held by the search here; a copy the cache did not keep must
    /// already be gone, since that charge is all it has.
    HandedBackItsRead,
    /// Has let that page go and not yet taken another.
    PastItsPage,
}

pub struct Parking {
    arrived: mpsc::Sender<Parked>,
    resume: mpsc::Receiver<()>,
}

impl Parking {
    fn park(&self, at: Parked) {
        self.arrived.send(at).expect("the main thread is waiting");
        self.resume
            .recv_timeout(PARK_TIMEOUT)
            .expect("the main thread resumes a parked search");
    }
}

pub struct Handle {
    arrived: mpsc::Receiver<Parked>,
    resume: mpsc::Sender<()>,
}

impl Handle {
    pub fn wait(&self, expected: Parked) {
        let at = self
            .arrived
            .recv_timeout(PARK_TIMEOUT)
            .expect("the search parks instead of blocking on the other one");
        assert_eq!(at, expected);
    }

    pub fn resume(&self) {
        self.resume.send(()).expect("the parked search is waiting");
    }
}

pub fn parking() -> (Parking, Handle) {
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    (
        Parking {
            arrived: arrived_tx,
            resume: resume_rx,
        },
        Handle {
            arrived: arrived_rx,
            resume: resume_tx,
        },
    )
}

/// What is held charged against what is live, at one moment.
#[derive(Debug, Clone, Copy)]
pub struct Reading {
    pub charged: isize,
    pub live: isize,
}

/// A vector store serving one saved paged graph whose serving cache holds
/// `CACHE_PAGES` pages, charged to `budget`.
pub struct Fixture {
    pub budget: Arc<Budget>,
    pub store: Arc<VectorStore>,
    tx_mgr: Arc<TransactionManager<TestStore>>,
    exec: MemVectorExecutor<TestStore>,
    index: VectorIndexRef,
}

pub fn fixture() -> Fixture {
    let index = VectorIndexRef::new("items", "embedding");
    let partition = VectorPartitionRef::new(index.clone(), VectorPartitionKey::unpartitioned());
    let entries = (0..POPULATION)
        .map(|ordinal| VectorEntry {
            index: index.clone(),
            row_id: RowId(ordinal as u64 + 1),
            vector: vector(ordinal),
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
        })
        .collect::<Vec<_>>();

    let budget = Arc::new(Budget {
        limit: AtomicUsize::new(usize::MAX),
        used: AtomicUsize::new(0),
        underflows: AtomicUsize::new(0),
    });
    let hnsw = Arc::new(OnceLock::new());
    let store = Arc::new(VectorStore::new(hnsw.clone()));
    let layout = VectorIndexLayout::unpartitioned(DIMENSION, VectorQuantization::F32).with_policy(
        Some(1),
        Some(16),
        Some(64),
        Some(64),
        1,
    );
    store
        .register_index_with_layout(
            index.clone(),
            layout.clone(),
            budget.clone(),
            &mut VectorCopyAdmission::new(budget.clone()),
        )
        .unwrap();
    let tx_mgr = Arc::new(TransactionManager::new(TestStore {
        vector: store.clone(),
        body_owner: budget.clone(),
    }));
    let exec =
        MemVectorExecutor::new_with_accountant(store.clone(), tx_mgr.clone(), hnsw, budget.clone());
    budget
        .try_allocate_for(
            entries
                .iter()
                .map(|entry| entry.vector.len() * std::mem::size_of::<f32>())
                .sum(),
            "test",
            "caller_vector_body",
            "test",
        )
        .unwrap();
    store
        .apply_inserts_with_accountant(
            entries.clone(),
            Some(budget.as_ref()),
            VectorCopyAdmission::new(Arc::new(Budget {
                limit: AtomicUsize::new(usize::MAX),
                used: AtomicUsize::new(0),
                underflows: AtomicUsize::new(0),
            })),
        )
        .unwrap();

    let policy = layout.resolve_policy(POPULATION, 10);
    let built = HnswIndex::from_vector_entries_with_policy(
        &entries,
        DIMENSION,
        VectorQuantization::F32,
        policy,
    );
    let (m, ef_construction, _) = built.policy_values();
    let bytes = Arc::new(built.encode_durable_generation().unwrap());
    drop(built);
    drop(entries);
    let span = bytes.len();
    assert!(
        span > 3 * CACHE_PAGES * PAGE,
        "the partition must be several cachefuls wide: {span} bytes"
    );
    let cache_bytes = CACHE_PAGES * PAGE;
    let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
        bytes.as_slice(),
        DIMENSION,
        VectorQuantization::F32,
        m,
        cache_bytes,
    )
    .unwrap();
    let graph = HnswIndex::decode_paged_durable_generation(
        bytes.as_slice(),
        Arc::new(Source {
            bytes: Arc::clone(&bytes),
        }),
        DIMENSION,
        VectorQuantization::F32,
        admitted,
        cache_bytes,
    )
    .unwrap();
    assert_eq!(graph.serving_cache_slots(), Some(CACHE_PAGES));
    let resident_bytes = graph.estimated_resident_bytes();
    budget
        .try_allocate_for(resident_bytes, "vector_test", "generation", "")
        .unwrap();
    store
        .install_loaded_partition_generation(
            &partition,
            true,
            VectorGraphGeneration {
                generation_id: 1,
                covered_tx: TxId(1),
                covered_lsn: Lsn(1),
                durable_bytes: span as u64,
                policy_revision: 1,
                hnsw_m: m as u32,
                hnsw_ef_construction: ef_construction as u32,
            },
            LoadedVectorGraphGeneration {
                graph,
                resident_bytes,
                accountant: budget.clone(),
                caller_adopted_bytes: 0,
                fresh_tail: None,
            },
        )
        .unwrap();
    Fixture {
        budget,
        store,
        tx_mgr,
        exec,
        index,
    }
}

/// Where one search parks. Every point is optional.
#[derive(Default)]
pub struct Plan {
    /// Park inside the page read numbered this (counting from one), and tell
    /// `read` the offset and length of that read.
    pub in_read: usize,
    pub read: Option<Arc<(AtomicUsize, AtomicUsize)>>,
    /// Park when handing back the charge of the page read numbered this.
    pub holding_fill: usize,
    /// Park when handing back the charge of the first page read from this
    /// offset.
    pub holding_offset: Option<usize>,
    /// After that, park again at the search's next step.
    pub past_it: bool,
}

/// One graph search over the fixture's generation, charging its working
/// memory to `held`. Returns its rows and how many pages it read.
pub fn search(
    fixture: &Fixture,
    query: &[f32],
    held: &AtomicIsize,
    plan: Plan,
    parking: Parking,
) -> (Vec<(RowId, f32)>, usize) {
    let parking = Rc::new(parking);
    let fills = Cell::new(0_usize);
    let park_next = Cell::new(false);
    let parked_at_offset = Cell::new(false);
    if plan.in_read != 0 {
        let parking = Rc::clone(&parking);
        let in_read = plan.in_read;
        let read = plan.read.clone();
        let mut count = 0_usize;
        let hook: Box<dyn FnMut(usize, usize)> = Box::new(move |offset, len| {
            count += 1;
            if count == in_read {
                if let Some(read) = &read {
                    read.0.store(offset, Ordering::SeqCst);
                    read.1.store(len, Ordering::SeqCst);
                }
                parking.park(Parked::InItsRead);
            }
        });
        BEFORE_READ.with(|slot| *slot.borrow_mut() = Some(hook));
    }
    let checkpoint = || {
        if park_next.replace(false) {
            parking.park(Parked::PastItsPage);
        }
    };
    let outcome = fixture
        .exec
        .bounded_hnsw_search::<Error>(
            &fixture.index,
            query,
            10,
            None,
            SnapshotId(1),
            VectorSearchMode::Indexed,
            POPULATION,
            HnswAllowedSearchLimits {
                max_visited_nodes: usize::MAX,
                max_vector_evaluations: usize::MAX,
            },
            || {
                checkpoint();
                Ok(())
            },
            || {
                checkpoint();
                Ok(())
            },
            || {
                checkpoint();
                Ok(())
            },
            |bytes| {
                checkpoint();
                held.fetch_add(bytes as isize, Ordering::SeqCst);
                Ok(())
            },
            |bytes| {
                held.fetch_sub(bytes as isize, Ordering::SeqCst);
                // The release that follows a page read is the walk handing
                // back the charge it read that page under, once the page has
                // been offered to the cache; the walk still holds the page.
                if let Some(offset) = FILLED.with(|filled| filled.take()) {
                    fills.set(fills.get() + 1);
                    let at_offset = plan.holding_offset == Some(offset) && !parked_at_offset.get();
                    if fills.get() == plan.holding_fill || at_offset {
                        parked_at_offset.set(parked_at_offset.get() || at_offset);
                        parking.park(Parked::HandedBackItsRead);
                        park_next.set(plan.past_it);
                    }
                }
            },
            &mut VectorCopyAdmission::new(fixture.budget.clone()),
        )
        .expect("the graph search answers");
    BEFORE_READ.with(|slot| slot.borrow_mut().take());
    let BoundedHnswOutcome::Complete(answered) = outcome else {
        panic!("the saved graph answers the search: {outcome:?}");
    };
    assert!(
        answered.trace.used_hnsw(),
        "the search walked the saved graph"
    );
    // The caller owns the rows' storage until it drops them; handing it back
    // here is the caller consuming its answer.
    held.fetch_sub(answered.retained_bytes as isize, Ordering::SeqCst);
    (answered.rows, fills.get())
}

/// Drop the store once every search is done: it releases no more than it
/// frees, and nothing is released twice.
pub fn drop_and_check(fixture: Fixture) {
    let budget = Arc::clone(&fixture.budget);
    let before = Reading {
        charged: budget.used() as isize,
        live: live(),
    };
    drop(fixture);
    let after = Reading {
        charged: budget.used() as isize,
        live: live(),
    };
    println!(
        "dropping the store: charged {:+} live {:+} underflows {}",
        after.charged - before.charged,
        after.live - before.live,
        budget.underflows.load(Ordering::SeqCst)
    );
    assert!(
        after.charged - before.charged >= after.live - before.live - ALLOWANCE,
        "dropping the store released {} bytes of charge but freed only {}",
        before.charged - after.charged,
        before.live - after.live,
    );
    assert_eq!(
        budget.underflows.load(Ordering::SeqCst),
        0,
        "no charge is released twice"
    );
}
