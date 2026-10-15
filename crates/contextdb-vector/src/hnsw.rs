#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.
use crate::quantized::{StoredVector, StoredVectorEntry, quantized_hnsw_distance};
use anndists::dist::distances::Distance;
use contextdb_core::{Error, Result, RowId, VectorEntry, VectorIndexRef, VectorQuantization};
use hnsw_rs::hnsw::{
    Hnsw, HnswAllowedSearchResult, HnswBoundedSearchError, HnswSearchResult,
    HnswSearchScratchContainer, HnswSearchScratchEvent, LoadedHnsw, Neighbour, PointId,
};
pub use hnsw_rs::hnsw::{
    HnswAllowedSearchIncomplete, HnswAllowedSearchLimits, HnswAllowedSearchStatus,
};
use hnsw_rs::hnswio::{decode_owned_graph, encode_owned_graph_into};
use parking_lot::{Mutex, RwLock};
#[cfg(feature = "test-seams")]
use std::cell::Cell;
#[cfg(any(test, feature = "test-seams"))]
use std::cell::RefCell;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

mod unfiltered;
mod union;
pub use union::UnionTerminal;
#[cfg(feature = "test-seams")]
pub(crate) use union::record_union_search;
pub(crate) use union::{
    HnswUnionLayer, HnswUnionSearchResult, search_union_allowed_bounded, union_candidate_budget,
};
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use union::{UnionSearchReceipt, with_union_search_receipts};

pub struct HnswIndex {
    hnsw: HnswInner,
    id_to_row: RwLock<HashMap<usize, RowId>>,
    row_to_point: RwLock<BTreeMap<RowId, hnsw_rs::hnsw::PointId>>,
    exact_rows: RwLock<HashMap<Vec<u8>, Vec<RowId>>>,
    next_id: AtomicUsize,
    build_serial: u64,
    dimension: usize,
    quantization: VectorQuantization,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
}

/// Fully validated and encoded work for one fresh-graph insertion. The engine
/// retains this value and its admission until the row transaction is durable.
pub(crate) struct PreparedHnswInsertion {
    build_serial: u64,
    entry: StoredVectorEntry,
    encoded: Option<Vec<u8>>,
    exact_key: Option<Vec<u8>>,
}

/// Final rows from the bounded HNSW path.  `retained_bytes` covers only the
/// returned row vector's observable capacity; every HNSW scratch container
/// has already emitted its matching release event.
#[derive(Debug)]
pub struct HnswSearchMemoryResult {
    pub rows: Vec<(RowId, f32)>,
    pub retained_bytes: usize,
}

/// Final rows and honest traversal state for an allowed-row graph search.
/// Rows accompanying `Incomplete` are diagnostic only and must not be
/// published as a query answer.
#[derive(Debug)]
pub struct HnswAllowedSearchMemoryResult {
    pub rows: Vec<(RowId, f32)>,
    pub retained_bytes: usize,
    pub status: HnswAllowedSearchStatus,
    pub visited_nodes: usize,
    pub vector_evaluations: usize,
    pub allowed_admissions: usize,
}

/// Where an allowed-row walk of one layer starts, and how many of that
/// layer's rows the walk may admit.
struct AllowedSeedDirectory {
    points: Vec<PointId>,
    layer_allowed_count: usize,
    required_results: usize,
    supplied_seed_rows: bool,
}

static NEXT_HNSW_BUILD_SERIAL: AtomicUsize = AtomicUsize::new(1);
const HNSW_BUILD_SEED: u64 = 0x55c8_6f2d_21a4_7bd3;
// The frozen F32 10k build/recall matrix selected this pair for the fewest
// construction distances at recall@10 1.00. Insertion remains seeded and sequential.
const F32_HNSW_EXTEND_CANDIDATES: bool = false;
const F32_HNSW_KEEPING_PRUNED: bool = false;

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub const fn f32_hnsw_build_options_for_test() -> (bool, bool) {
    (F32_HNSW_EXTEND_CANDIDATES, F32_HNSW_KEEPING_PRUNED)
}

enum HnswInner {
    F32Mutable(Hnsw<'static, f32, HnswF32Distance>),
    F32Sealed(LoadedHnsw<f32, HnswF32Distance>),
    QuantizedMutable(Hnsw<'static, u8, DistQuantizedCosine>),
    QuantizedSealed(LoadedHnsw<u8, DistQuantizedCosine>),
    F32Paged(PagedHnsw),
}

/// Snapshot-bound source for immutable graph point values. Implementations
/// return only the requested graph-envelope range; callers charge the result
/// as query scratch before it becomes resident.
pub trait ReadOnlyVectorGraphPageSource: Send + Sync {
    fn reader(&self) -> Result<Box<dyn ReadOnlyVectorGraphPageReader>>;

    /// Bytes in one independently authenticated source-page body. The serving
    /// cache aligns to this boundary so one cache fill normally resolves one
    /// immutable page instead of two adjacent page records.
    fn page_bytes(&self) -> usize {
        PAGED_VECTOR_SOURCE_PAGE_BYTES
    }

    /// Heap bytes retained exclusively by this source and its immutable page
    /// map. Shared database handles remain owned by the committed read image.
    fn retained_bytes(&self) -> usize {
        0
    }

    /// Bytes of page body this source holds in total, when it knows. A serving
    /// cache smaller than this cannot hold the partition a walk may touch.
    fn total_page_bytes(&self) -> Option<usize> {
        None
    }
}

/// A reader travels with the statement that opened it, and a suspended
/// bounded cursor resumes on another thread, so a page reader moves between
/// threads with its cursor.
pub trait ReadOnlyVectorGraphPageReader: Send {
    fn read_range(&mut self, offset: usize, len: usize) -> Result<Vec<u8>>;
}

/// One search's own directory of the serving pages it has already resolved.
///
/// The serving cache is shared between searches, so every look into it takes a
/// mutex. A 64 KiB serving page holds about twenty-one 768-dimension vectors
/// and a walk comes back to a page it has already resolved many times over, so
/// taking that mutex inside the per-point decode pays it once per VISIT where
/// the work it protects is once per PAGE.
///
/// This keeps a non-owning reference to each page the search has resolved. A
/// page that is still resident answers straight from that reference without
/// the mutex; a page the serving cache has since evicted has no owner left, so
/// the reference is dead and the search goes back to the cache and re-reads it
/// exactly as it does today.
///
/// A dead reference still keeps the allocation it points into, so what it
/// points into is only the page's small shared handle: the page buffer is a
/// separate allocation the handle owns, freed the moment the last strong
/// owner lets go. A dead entry therefore keeps [`ServingPageHandle`]'s few
/// words and never a page, and those words are part of what each entry is
/// charged. A cache smaller than its partition still cycles pages through
/// itself, and a search parked while another one cycles the cache still lives
/// inside the size the session declared.
pub(crate) struct ServingPages {
    reader: Box<dyn ReadOnlyVectorGraphPageReader>,
    /// One entry per serving-cache slot, so the directory is bounded by the
    /// declared cache rather than by the partition, and it is the caller's
    /// charged scratch. Two pages that land on the same entry displace each
    /// other here and take the mutex again; a search opened with no entries at
    /// all asks the cache every time, which is what this was before.
    resolved: Vec<Option<(usize, Weak<PageBody>)>>,
}

/// One serving page's bytes, owned apart from the handle that shares them.
///
/// Pages are shared as `Arc<PageBody>` rather than `Arc<[u8]>` on purpose. An
/// `Arc<[u8]>` puts the counts and the bytes in ONE allocation, and a `Weak`
/// keeps that allocation reserved until the last `Weak` goes, so a search's
/// directory would keep every page the cache evicted under it. Here the
/// shared allocation holds the counts and this box's pointer; dropping the
/// last strong owner drops the box and frees the page, whatever `Weak`s remain.
pub(crate) struct PageBody(Box<[u8]>);

impl std::ops::Deref for PageBody {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

/// What a page's shared handle allocates beside the page itself: the two
/// reference counts and the pointer to the page buffer. A dead directory entry
/// keeps exactly this until the entry is overwritten or the search ends.
const SERVING_PAGE_HANDLE_BYTES: usize =
    2 * std::mem::size_of::<usize>() + std::mem::size_of::<PageBody>();

/// What a serving cache of this many slots holds besides its page bodies:
/// the slot, its recency stamp, the shared handle each installed page
/// carries, and the lookup table that finds a slot from a page offset
/// (`PagedVectorPageCache::slot_by_offset`). That table is a real
/// `HashMap<usize, usize>`, reserved with `HashMap::with_capacity(page_slots)`
/// before the cache answers a single lookup, so it is charged the same way a
/// scratch hash set is: by its buckets and control bytes, not by
/// `page_slots * size_of::<(usize, usize)>()`, which would leave out both.
fn serving_slot_directory_bytes(page_slots: usize) -> usize {
    page_slots
        .saturating_mul(
            std::mem::size_of::<Option<ResidentPage>>()
                + std::mem::size_of::<u64>()
                + SERVING_PAGE_HANDLE_BYTES,
        )
        .saturating_add(set_allocation_bytes::<(usize, usize)>(page_slots))
}

/// What one directory entry costs the search that opened it: the entry, and
/// the page handle a dead entry may still be keeping.
pub(crate) const SERVING_PAGE_DIRECTORY_ENTRY_BYTES: usize =
    std::mem::size_of::<Option<(usize, Weak<PageBody>)>>() + SERVING_PAGE_HANDLE_BYTES;

impl ServingPages {
    pub(crate) fn new(reader: Box<dyn ReadOnlyVectorGraphPageReader>, entries: usize) -> Self {
        Self {
            reader,
            resolved: (0..entries).map(|_| None).collect(),
        }
    }

    /// Lets every call site keep the `page_reader.as_mut()` it already writes.
    fn as_mut(&mut self) -> &mut Self {
        self
    }

    fn entry(&self, offset: usize, page_bytes: usize) -> Option<usize> {
        let entries = self.resolved.len();
        (entries != 0).then(|| offset / page_bytes.max(1) % entries)
    }

    /// The page at this offset if this search has resolved it and it is still
    /// resident somewhere. `None` means ask the serving cache.
    fn held(&self, offset: usize, page_bytes: usize) -> Option<Arc<PageBody>> {
        let entry = self.entry(offset, page_bytes)?;
        match self.resolved.get(entry)? {
            Some((held, page)) if *held == offset => page.upgrade(),
            _ => None,
        }
    }

    fn hold(&mut self, offset: usize, page: &Arc<PageBody>, page_bytes: usize) {
        let Some(entry) = self.entry(offset, page_bytes) else {
            return;
        };
        if let Some(slot) = self.resolved.get_mut(entry) {
            *slot = Some((offset, Arc::downgrade(page)));
        }
    }
}

struct PagedHnsw {
    points: Vec<Vec<PagedPoint>>,
    entry_point: Option<hnsw_rs::hnsw::PointId>,
    source: Arc<dyn ReadOnlyVectorGraphPageSource>,
    dimension: usize,
    /// The degree this generation was built at, which is what decides whether a
    /// filter still leaves the rows it admits connected to each other.
    max_nb_connection: usize,
    /// The most neighbours a point of this generation keeps on the bottom
    /// layer, which is the most a walk evaluates per point it expands there.
    /// The saved topology records only the declared degree, so this is
    /// derived once at load from the build's own definition of that layer.
    layer0_max_degree: usize,
    /// What this generation holds charged: its topology, row mappings and
    /// serving cache. It falls only when the serving cache gives back slots
    /// whose pages nothing else still holds.
    resident_bytes: AtomicUsize,
    graph_len: usize,
    source_page_bytes: usize,
    /// How many pages the serving cache holds at once, which is also how many
    /// entries one search's own page directory is given.
    page_slots: usize,
    page_cache: Mutex<PagedVectorPageCache>,
}

/// One page body a walk may read while the serving cache is unlocked, and
/// which stays alive for that walk even if a give-back takes its slot first.
struct ResidentPage {
    offset: usize,
    bytes: Arc<PageBody>,
}

/// The serving cache is only a directory of resident pages. It is locked to
/// find a page and to install one, and never while a page is being read,
/// decoded or scored, so two searches over the same generation do not
/// serialize on each other's work.
struct PagedVectorPageCache {
    /// Which serving slot holds which page offset. A walk asks this once per
    /// stored vector, so it may not be a scan of every slot.
    slot_by_offset: HashMap<usize, usize>,
    slots: Vec<Option<ResidentPage>>,
    last_used: Vec<u64>,
    clock: u64,
}

impl PagedVectorPageCache {
    fn new(slots: usize) -> Self {
        Self {
            slot_by_offset: HashMap::with_capacity(slots),
            slots: (0..slots).map(|_| None).collect(),
            last_used: vec![0; slots],
            clock: 0,
        }
    }

    /// The resident page at this offset, as a handle the caller keeps while
    /// the cache is unlocked.
    fn get(&mut self, offset: usize) -> Option<Arc<PageBody>> {
        let slot = *self.slot_by_offset.get(&offset)?;
        self.clock = self.clock.wrapping_add(1);
        let page = self.slots.get(slot)?.as_ref()?;
        let bytes = Arc::clone(&page.bytes);
        if let Some(recency) = self.last_used.get_mut(slot) {
            *recency = self.clock;
        }
        Some(bytes)
    }

    fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// What this cache is charged for besides its page bodies, read from what
    /// it has really allocated rather than from a per-slot figure: the slot
    /// and recency arrays by the capacity each holds, the lookup table by its
    /// buckets and control bytes, and the shared handle a page installed in
    /// each slot carries. A cache fresh from decode answers exactly what its
    /// generation was admitted with for it,
    /// [`serving_slot_directory_bytes`] of its slot count.
    fn directory_bytes(&self) -> usize {
        self.slots
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<ResidentPage>>())
            .saturating_add(
                self.last_used
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(self.slots.len().saturating_mul(SERVING_PAGE_HANDLE_BYTES))
            .saturating_add(set_allocation_bytes::<(usize, usize)>(
                self.slot_by_offset.capacity(),
            ))
    }

    /// Give slots back down to `keep` (never fewer than one), keeping the
    /// pages used most recently, and return how many slots went.
    ///
    /// Only a slot no walk holds is given back: an empty one, or one whose
    /// page the cache holds the last strong handle to, which is freed here on
    /// the spot. A page a walk is still reading -- through the handle it
    /// resolved, or one its own page directory upgraded -- is still
    /// allocated, and its slot's charge is the only charge it has, so its
    /// slot stays, past `keep` if it has to, until a later give-back finds the
    /// page no longer held. Taking the handle is one atomic step, so a
    /// directory cannot upgrade a page in the moment it is freed.
    ///
    /// Nothing is allocated. This runs because an allocation was refused, so
    /// it may not need memory of its own: the kept pages are moved to the
    /// front where they are, and the directory keeps the capacity it was
    /// admitted with rather than being rebuilt smaller.
    fn give_back_unheld(&mut self, keep: usize) -> usize {
        let keep = keep.max(1);
        let old = self.slots.len();
        if old <= keep {
            return 0;
        }
        let resident = self.slots.iter().filter(|slot| slot.is_some()).count();
        let kept = resident.min(keep);
        let used_since = |slots: &[Option<ResidentPage>], last_used: &[u64], stamp: u64| {
            slots
                .iter()
                .zip(last_used)
                .filter(|(slot, used)| slot.is_some() && **used >= stamp)
                .count()
        };
        // The oldest recency stamp that still makes the cut: the largest
        // stamp at or after which at least `kept` resident pages were used.
        let (mut low, mut high) = (0_u64, u64::MAX);
        while low < high {
            let middle = low + (high - low) / 2 + 1;
            if used_since(&self.slots, &self.last_used, middle) >= kept {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let newer = if low == u64::MAX {
            0
        } else {
            used_since(&self.slots, &self.last_used, low + 1)
        };
        let mut ties = kept.saturating_sub(newer);
        let mut next = 0;
        for slot in 0..old {
            let wanted = match (&self.slots[slot], self.last_used[slot]) {
                (Some(_), used) if used > low => true,
                (Some(_), used) if used == low && ties > 0 => {
                    ties -= 1;
                    true
                }
                _ => false,
            };
            if wanted && next < kept {
                self.slots.swap(next, slot);
                self.last_used.swap(next, slot);
                next += 1;
            }
        }
        // Past the most recent: a page nothing else holds is freed, and one a
        // walk holds is moved up beside the kept ones with its slot.
        let first_unkept = next;
        for slot in first_unkept..old {
            let Some(ResidentPage { offset, bytes }) = self.slots[slot].take() else {
                continue;
            };
            match Arc::try_unwrap(bytes) {
                Ok(body) => drop(body),
                Err(bytes) => {
                    self.slots[slot] = Some(ResidentPage { offset, bytes });
                    self.slots.swap(next, slot);
                    self.last_used.swap(next, slot);
                    next += 1;
                }
            }
        }
        let remaining = next.max(keep);
        for used in &mut self.last_used[next..] {
            *used = 0;
        }
        self.slots.truncate(remaining);
        self.last_used.truncate(remaining);
        // Cleared, not replaced: the table keeps its admitted buckets and
        // never holds more entries than it did.
        self.slot_by_offset.clear();
        for (slot, page) in self.slots.iter().enumerate() {
            if let Some(page) = page {
                self.slot_by_offset.insert(page.offset, slot);
            }
        }
        old - remaining
    }

    /// Make a page the caller has already read and charged resident, and say
    /// whether the cache kept it. The slot it takes carries its charge from
    /// then on; a page the cache did not keep stays the caller's, charged to
    /// the caller until it drops it.
    ///
    /// Two walks that missed the same page at the same time both hold a
    /// correct copy; only the first one installed is kept, so the cache never
    /// holds the same page twice.
    ///
    /// A full cache replaces its least recently used page that no walk is
    /// reading. A page a walk is still reading is still allocated, and the
    /// slot's charge is the only charge it has, so that slot is never handed
    /// to another page: the page is taken out only when the cache's own
    /// handle is the last one, which frees it on the spot, and put back
    /// otherwise. When every page is being read, the new page is not kept.
    fn install(&mut self, offset: usize, bytes: Arc<PageBody>) -> bool {
        if self.slot_by_offset.contains_key(&offset) {
            return false;
        }
        self.clock = self.clock.wrapping_add(1);
        let slot = match self.slots.iter().position(Option::is_none) {
            Some(slot) => slot,
            None => {
                let mut freed = None;
                for _ in 0..self.slots.len() {
                    let Some(victim) = self
                        .last_used
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, used)| **used)
                        .map(|(slot, _)| slot)
                    else {
                        break;
                    };
                    let Some(ResidentPage {
                        offset: previous,
                        bytes: page,
                    }) = self.slots[victim].take()
                    else {
                        freed = Some(victim);
                        break;
                    };
                    match Arc::try_unwrap(page) {
                        Ok(body) => {
                            drop(body);
                            self.slot_by_offset.remove(&previous);
                            freed = Some(victim);
                            break;
                        }
                        Err(page) => {
                            // Being read right now, so it is as recent as
                            // anything the cache holds.
                            self.slots[victim] = Some(ResidentPage {
                                offset: previous,
                                bytes: page,
                            });
                            self.last_used[victim] = self.clock;
                        }
                    }
                }
                let Some(slot) = freed else {
                    return false;
                };
                slot
            }
        };
        self.slots[slot] = Some(ResidentPage { offset, bytes });
        self.slot_by_offset.insert(offset, slot);
        self.last_used[slot] = self.clock;
        true
    }
}

const PAGED_VECTOR_SOURCE_PAGE_BYTES: usize = 64 * 1024;
/// What a paged decode holds per row to find it: its id-to-row entry,
/// row-to-point tree node, and compact exact-vector fingerprint entry,
/// including the maps' buckets and nodes.
const DURABLE_READER_ROW_MAPPING_BYTES: usize = 384;
/// The workspace a saved-graph load is admitted with beside what it keeps,
/// returned once the graph is decoded.
pub const DURABLE_READER_LOAD_WORKSPACE_BYTES: usize = 64 * 1024;
const PAGED_VECTOR_MIN_CACHE_BYTES: usize = PAGED_VECTOR_SOURCE_PAGE_BYTES;
/// What the crate's own fixtures assume a serving cache can hold. They build
/// graphs far smaller than this; it stands in for "a partition that fits".
#[cfg(test)]
const PAGED_VECTOR_CACHE_TEST_SPAN: usize = 16 * 1024 * 1024;

/// The ceiling on one serving page cache.
///
/// It is not a constant. A reader that was given a size uses it; otherwise the
/// ceiling is the smaller of the session declaration and the partition's page
/// span. The generation loader divides actual admitted cache bytes among the
/// selected layers after charging their topology and later least loads; a
/// second fixed division here can keep one large partition short even when
/// every selected graph's pages fit the declaration together.
///
/// Whichever leg answers, the partition caps it: a cache larger than the pages
/// it serves buys nothing and cannot be filled, so reserving for it takes
/// memory from the next partition's load for slots that will stay empty. The
/// partition is a fact about the data, not a policy, so it bounds every leg
/// including a size the reader was handed.
pub fn durable_reader_cache_ceiling(
    declared_cache_bytes: Option<usize>,
    session_memory_limit: Option<usize>,
    partition_page_bytes: usize,
) -> usize {
    // A declared zero is not a declaration of zero bytes -- nothing can serve
    // from an empty cache -- so it means the same as declaring nothing.
    let ceiling = match (
        declared_cache_bytes.filter(|bytes| *bytes != 0),
        session_memory_limit,
    ) {
        (Some(declared), _) => declared,
        (None, Some(limit)) => limit,
        (None, None) => partition_page_bytes,
    };
    ceiling
        .min(partition_page_bytes.max(PAGED_VECTOR_MIN_CACHE_BYTES))
        .max(PAGED_VECTOR_MIN_CACHE_BYTES)
        .next_multiple_of(PAGED_VECTOR_SOURCE_PAGE_BYTES)
}

/// A serving page one walk is reading, and the part of it still charged to
/// that walk: nothing for a page the cache holds (its slot carries the
/// charge), the page's bytes for a copy the cache did not keep. The walk's
/// handle is dropped first and that charge given back after, so a copy
/// stays charged for exactly as long as this walk keeps it allocated.
struct ReadPage<'r, R: FnMut(HnswSearchScratchEvent)> {
    page: Option<Arc<PageBody>>,
    charged: usize,
    release: &'r mut R,
}

impl<'r, R: FnMut(HnswSearchScratchEvent)> ReadPage<'r, R> {
    fn new(page: Arc<PageBody>, charged: usize, release: &'r mut R) -> Self {
        Self {
            page: Some(page),
            charged,
            release,
        }
    }
}

impl<R: FnMut(HnswSearchScratchEvent)> std::ops::Deref for ReadPage<'_, R> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.page.as_deref().map_or(&[], |page| &**page)
    }
}

impl<R: FnMut(HnswSearchScratchEvent)> Drop for ReadPage<'_, R> {
    fn drop(&mut self) {
        drop(self.page.take());
        if self.charged != 0 {
            (self.release)(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::QueryBytes,
                capacity: self.charged,
                element_bytes: 1,
            });
        }
    }
}

/// The scratch one graph walk has been admitted for and still owes back.
///
/// Every reservation a walk makes goes through [`Self::admit`], and every way
/// out of the walk -- answered, refused, cancelled at any distance -- ends in
/// [`Self::give_back`]. So each admitted charge is returned exactly once and a
/// refused reservation is never returned at all. A refusal part-way through
/// admission gives nothing back by itself: what was admitted before it may
/// already be allocated -- the page directory is -- so the code that owns
/// those allocations drops them first and then calls [`Self::give_back`].
/// Storage whose ownership passes to the caller -- the returned result -- is
/// not admitted here and stays charged to the caller.
#[derive(Default)]
struct AdmittedScratch {
    held: [Option<(HnswSearchScratchContainer, usize, usize)>; 4],
}

impl AdmittedScratch {
    fn admit<E, A>(
        &mut self,
        container: HnswSearchScratchContainer,
        capacity: usize,
        element_bytes: usize,
        acquire: &mut A,
    ) -> std::result::Result<(), HnswBoundedSearchError<E>>
    where
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    {
        // A refusal returns nothing here: the caller still holds what the
        // earlier admissions cover, and gives them back once it has let go.
        let Some(slot) = self.held.iter().position(Option::is_none) else {
            return Err(HnswBoundedSearchError::CapacityInvariant(
                "graph walk scratch admissions",
            ));
        };
        if let Err(error) = acquire(HnswSearchScratchEvent::Reserve {
            container,
            previous_capacity: 0,
            requested_capacity: capacity,
            element_bytes,
        }) {
            return Err(HnswBoundedSearchError::Callback(error));
        }
        self.held[slot] = Some((container, capacity, element_bytes));
        Ok(())
    }

    /// Give back one container's charge, once the allocation it covers has
    /// been dropped or moved into what the walk returns -- never before, so a
    /// caller sharing its allowance is never told memory still in use is free.
    fn give_back_only<R>(&mut self, container: HnswSearchScratchContainer, release: &mut R)
    where
        R: FnMut(HnswSearchScratchEvent),
    {
        for held in &mut self.held {
            if matches!(held, Some((admitted, _, _)) if *admitted == container)
                && let Some((container, capacity, element_bytes)) = held.take()
            {
                release(HnswSearchScratchEvent::Release {
                    container,
                    capacity,
                    element_bytes,
                });
            }
        }
    }

    fn give_back<R>(&mut self, release: &mut R)
    where
        R: FnMut(HnswSearchScratchEvent),
    {
        for held in self.held.iter_mut().rev() {
            if let Some((container, capacity, element_bytes)) = held.take() {
                release(HnswSearchScratchEvent::Release {
                    container,
                    capacity,
                    element_bytes,
                });
            }
        }
    }
}

/// Which points a walk has already reached.
///
/// A walk asks this once for every graph edge it considers -- about twenty
/// times for each point it scores -- so the answer has to cost the same
/// whether the walk has reached ten points or ten thousand. Kept as one bit
/// per point of the generation, indexed rather than scanned, and sixty-four
/// times smaller than the list of reached ids it replaces.
struct ReachedPoints {
    marks: Vec<u64>,
    /// Where each layer's ranks begin, with a closing total, so one bit index
    /// covers every layer and a rank past its own layer is refused rather than
    /// answering for the next layer's point.
    layer_base: Vec<usize>,
    reached: usize,
    capacity: usize,
}

impl ReachedPoints {
    /// How many 64-bit words this generation's marks occupy, plus the layer
    /// offsets beside them -- what the walk charges before it allocates.
    fn words_for(points: &[Vec<PagedPoint>]) -> usize {
        let total: usize = points.iter().map(Vec::len).sum();
        total
            .div_ceil(u64::BITS as usize)
            .saturating_add(points.len())
            .saturating_add(1)
    }

    fn words_for_counts(counts: &[usize]) -> usize {
        let total: usize = counts.iter().sum();
        total
            .div_ceil(u64::BITS as usize)
            .saturating_add(counts.len())
            .saturating_add(1)
    }

    fn new(points: &[Vec<PagedPoint>], capacity: usize) -> Self {
        let mut layer_base = Vec::with_capacity(points.len() + 1);
        let mut base = 0_usize;
        for layer in points {
            layer_base.push(base);
            base = base.saturating_add(layer.len());
        }
        layer_base.push(base);
        Self {
            marks: vec![0; base.div_ceil(u64::BITS as usize)],
            layer_base,
            reached: 0,
            capacity,
        }
    }

    fn from_counts(counts: &[usize], capacity: usize) -> Self {
        let mut layer_base = Vec::with_capacity(counts.len() + 1);
        let mut base = 0_usize;
        for &count in counts {
            layer_base.push(base);
            base = base.saturating_add(count);
        }
        layer_base.push(base);
        Self {
            marks: vec![0; base.div_ceil(u64::BITS as usize)],
            layer_base,
            reached: 0,
            capacity,
        }
    }

    fn bit(&self, id: PointId) -> Option<usize> {
        let layer = usize::from(id.0);
        let base = *self.layer_base.get(layer)?;
        let end = *self.layer_base.get(layer + 1)?;
        let bit = base.checked_add(usize::try_from(id.1).ok()?)?;
        (bit < end).then_some(bit)
    }

    fn contains(&self, id: PointId) -> bool {
        self.bit(id).is_some_and(|bit| {
            self.marks
                .get(bit / u64::BITS as usize)
                .is_some_and(|word| word & (1 << (bit % u64::BITS as usize)) != 0)
        })
    }

    /// Mark a point reached. `false` means it was already reached, or that it
    /// is not a point of this generation.
    fn insert(&mut self, id: PointId) -> bool {
        let Some(bit) = self.bit(id) else {
            return false;
        };
        let Some(word) = self.marks.get_mut(bit / u64::BITS as usize) else {
            return false;
        };
        let mask = 1_u64 << (bit % u64::BITS as usize);
        if *word & mask != 0 {
            return false;
        }
        *word |= mask;
        self.reached += 1;
        true
    }

    fn len(&self) -> usize {
        self.reached
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

struct PagedPoint {
    origin_id: usize,
    vector_offset: usize,
    neighbours: Vec<Vec<PagedNeighbour>>,
}

#[derive(Clone, Copy)]
struct PagedNeighbour {
    origin_id: u32,
    point_id: PointId,
    distance: f32,
}

#[derive(Clone, Copy)]
struct PagedQueuePoint {
    origin_id: usize,
    point_id: PointId,
    distance: f32,
}

impl PartialEq for PagedQueuePoint {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for PagedQueuePoint {}

impl PartialOrd for PagedQueuePoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PagedQueuePoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.point_id.cmp(&other.point_id))
            .then_with(|| self.origin_id.cmp(&other.origin_id))
    }
}

/// The one unfiltered F32 traversal reads identities and distances from a
/// source. A resident graph supplies borrowed points; a saved graph supplies
/// topology and pages its vector coordinates on demand.
trait UnfilteredGraphSource<E> {
    fn entry_point(&self) -> Option<PointId>;
    fn layer_points(&self, layer: usize) -> usize;
    fn origin_id(&self, id: PointId) -> Option<usize>;
    fn neighbour_count(&self, id: PointId, layer: usize) -> Option<usize>;
    fn neighbour_identity(
        &self,
        id: PointId,
        layer: usize,
        position: usize,
    ) -> Option<(PointId, usize)>;
    #[allow(clippy::too_many_arguments)]
    fn distance<S, D, A, R>(
        &mut self,
        id: PointId,
        query: &[f32],
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<f32, HnswBoundedSearchError<E>>
    where
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent);
}

struct PagedGraphSource<'a> {
    graph: &'a PagedHnsw,
    pages: &'a mut ServingPages,
    query_norm: f64,
}

impl<E: From<Error>> UnfilteredGraphSource<E> for PagedGraphSource<'_> {
    fn entry_point(&self) -> Option<PointId> {
        self.graph.entry_point
    }

    fn layer_points(&self, layer: usize) -> usize {
        self.graph.points.get(layer).map_or(0, Vec::len)
    }

    fn origin_id(&self, id: PointId) -> Option<usize> {
        self.graph.point(id).map(|point| point.origin_id)
    }

    fn neighbour_count(&self, id: PointId, layer: usize) -> Option<usize> {
        self.graph.point(id)?.neighbours.get(layer).map(Vec::len)
    }

    fn neighbour_identity(
        &self,
        id: PointId,
        layer: usize,
        position: usize,
    ) -> Option<(PointId, usize)> {
        let edge = self.graph.point(id)?.neighbours.get(layer)?.get(position)?;
        Some((edge.point_id, edge.origin_id as usize))
    }

    fn distance<S, D, A, R>(
        &mut self,
        id: PointId,
        query: &[f32],
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<f32, HnswBoundedSearchError<E>>
    where
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        let point = self
            .graph
            .point(id)
            .ok_or(HnswBoundedSearchError::SearchInvariant("saved graph point"))?;
        self.graph.distance(
            point,
            query,
            self.query_norm,
            before_source_entry,
            before_distance,
            acquire,
            release,
            self.pages,
        )
    }
}

struct ResidentGraphSource<'a, D> {
    points: &'a hnsw_rs::hnsw::PointIndexation<'static, f32>,
    distance: &'a D,
}

impl<E, D: Distance<f32>> UnfilteredGraphSource<E> for ResidentGraphSource<'_, D> {
    fn entry_point(&self) -> Option<PointId> {
        self.points.entry_point_id()
    }

    fn layer_points(&self, layer: usize) -> usize {
        self.points.get_layer_nb_point(layer)
    }

    fn origin_id(&self, id: PointId) -> Option<usize> {
        self.points
            .resident_point(&id)
            .map(|point| point.get_origin_id())
    }

    fn neighbour_count(&self, id: PointId, layer: usize) -> Option<usize> {
        self.points
            .resident_point(&id)
            .map(|point| point.neighbour_count(layer))
    }

    fn neighbour_identity(
        &self,
        id: PointId,
        layer: usize,
        position: usize,
    ) -> Option<(PointId, usize)> {
        self.points
            .resident_point(&id)?
            .neighbour_identity(layer, position)
    }

    fn distance<S, C, A, R>(
        &mut self,
        id: PointId,
        query: &[f32],
        _before_source_entry: &mut S,
        before_distance: &mut C,
        _acquire: &mut A,
        _release: &mut R,
    ) -> std::result::Result<f32, HnswBoundedSearchError<E>>
    where
        S: FnMut() -> std::result::Result<(), E>,
        C: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        before_distance().map_err(HnswBoundedSearchError::Callback)?;
        self.points
            .with_point_data(&id, |point| self.distance.eval(query, point))
            .ok_or(HnswBoundedSearchError::SearchInvariant(
                "resident graph point",
            ))
    }
}

#[allow(clippy::too_many_arguments)]
fn search_resident_f32<E, S, D, A, R, Dist>(
    points: &hnsw_rs::hnsw::PointIndexation<'static, f32>,
    distance: &Dist,
    query: &[f32],
    knbn: usize,
    ef: usize,
    before_source_entry: S,
    before_distance: D,
    acquire: A,
    mut release: R,
) -> std::result::Result<HnswSearchResult, HnswBoundedSearchError<E>>
where
    Dist: Distance<f32>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
{
    let mut source = ResidentGraphSource { points, distance };
    let mut admitted = AdmittedScratch::default();
    let result = unfiltered::search(
        &mut source,
        query,
        knbn,
        ef,
        before_source_entry,
        before_distance,
        acquire,
        &mut release,
        &mut admitted,
    );
    admitted.give_back(&mut release);
    result
}

impl PagedHnsw {
    fn point(&self, id: PointId) -> Option<&PagedPoint> {
        usize::try_from(id.1)
            .ok()
            .and_then(|rank| self.points.get(id.0 as usize)?.get(rank))
    }

    #[allow(clippy::too_many_arguments)]
    fn visit_exact_rows<E, S, A, R, F>(
        &self,
        key: &[u8],
        query: &[f32],
        exact_hash_rows: &HashMap<Vec<u8>, Vec<RowId>>,
        row_to_point: &BTreeMap<RowId, PointId>,
        limit: usize,
        before_source_entry: &mut S,
        acquire: &mut A,
        release: &mut R,
        mut visit: F,
    ) -> std::result::Result<(), HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId, &mut S, &mut A, &mut R) -> std::result::Result<bool, E>,
    {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let Some(candidates) = exact_hash_rows.get(&hasher.finish().to_le_bytes().to_vec()) else {
            return Ok(());
        };
        let mut admitted = AdmittedScratch::default();
        admitted.admit(
            HnswSearchScratchContainer::ServingPageDirectory,
            self.page_slots,
            SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            acquire,
        )?;
        let checked = (|| {
            let reader = self
                .source
                .reader()
                .map_err(|error| HnswBoundedSearchError::Callback(E::from(error)))?;
            let mut pages = ServingPages::new(reader, self.page_slots);
            let mut found = 0;
            for row in candidates {
                if found == limit {
                    break;
                }
                let point = row_to_point.get(row).and_then(|id| self.point(*id)).ok_or(
                    HnswBoundedSearchError::SearchInvariant("durable HNSW exact row"),
                )?;
                let mut equal = true;
                self.fold_point_coordinates(
                    point,
                    query,
                    before_source_entry,
                    &mut || Ok::<(), E>(()),
                    acquire,
                    release,
                    &mut pages,
                    |left, right| equal &= left.to_bits() == right.to_bits(),
                )?;
                if equal
                    && visit(*row, before_source_entry, acquire, release)
                        .map_err(HnswBoundedSearchError::Callback)?
                {
                    found += 1;
                }
            }
            Ok(())
        })();
        admitted.give_back(release);
        checked
    }

    /// The query's own norm, which every point in one walk shares. Summed in
    /// coordinate order so it is bit-for-bit the norm the per-visit
    /// accumulation used to produce.
    fn query_norm(query: &[f32]) -> f64 {
        #[cfg(any(test, feature = "test-seams"))]
        observe_paged_vector(|observer| observer.query_norm_computed());
        let mut norm = 0.0_f64;
        for &right in query {
            norm += f64::from(right * right);
        }
        norm
    }

    /// Walk one stored point's coordinates out of the serving pages, handing
    /// each query/stored pair to `visit` in coordinate order. The graph walk's
    /// own distance and a candidate's published score share this one page
    /// path, so both authenticate whole pages and reuse the same cache.
    /// `false` means the column stores no coordinates at all.
    #[allow(clippy::too_many_arguments)]
    fn fold_point_coordinates<E, S, D, A, R, C>(
        &self,
        point: &PagedPoint,
        query: &[f32],
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
        pages: &mut ServingPages,
        mut visit: C,
    ) -> std::result::Result<bool, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        C: FnMut(f32, f32),
    {
        before_source_entry().map_err(HnswBoundedSearchError::Callback)?;
        before_distance().map_err(HnswBoundedSearchError::Callback)?;
        let requested =
            self.dimension
                .checked_mul(4)
                .ok_or(HnswBoundedSearchError::CapacityOverflow(
                    "paged vector byte length",
                ))?;
        // VECTOR(0) is accepted by the existing schema contract. Its empty
        // cosine operands have zero norm, so they produce the same finite
        // answer as every other zero-norm pair without entering the page
        // loop (whose inclusive last-page arithmetic assumes at least one
        // byte).
        if requested == 0 {
            return Ok(false);
        }
        #[cfg(any(test, feature = "test-seams"))]
        observe_paged_vector(|observer| observer.vector_decoded());
        let vector_end = point.vector_offset.checked_add(requested).ok_or(
            HnswBoundedSearchError::CapacityOverflow("paged vector range"),
        )?;
        let first_page = point.vector_offset / self.source_page_bytes * self.source_page_bytes;
        let last_page =
            (vector_end.saturating_sub(1)) / self.source_page_bytes * self.source_page_bytes;
        let mut coordinate = 0_usize;
        // A stored vector may begin in one serving page and end in the next.
        // Only the four bytes straddling that boundary are carried; everything
        // else is decoded four bytes at a time straight out of the page.
        let mut carry = [0_u8; 4];
        let mut carry_len = 0_usize;
        let mut page_offset = first_page;
        loop {
            // A page this search has already resolved and that is still
            // resident answers without the shared mutex.
            let resident = match pages.held(page_offset, self.source_page_bytes) {
                held @ Some(_) => held,
                None => {
                    let mut cache = self.page_cache.lock();
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_paged_vector(|observer| observer.page_cache_locked());
                    cache.get(page_offset)
                }
            };
            contextdb_core::find_attribution::note_page_lookup(resident.is_some());
            #[cfg(any(test, feature = "test-seams"))]
            observe_paged_vector(|observer| observer.page_looked_up(resident.is_some()));
            let page = if let Some(page) = resident {
                ReadPage::new(page, 0, &mut *release)
            } else {
                let page_len = self
                    .source_page_bytes
                    .min(self.graph_len.saturating_sub(page_offset));
                acquire(HnswSearchScratchEvent::Reserve {
                    container: HnswSearchScratchContainer::QueryBytes,
                    previous_capacity: 0,
                    requested_capacity: page_len,
                    element_bytes: 1,
                })
                .map_err(HnswBoundedSearchError::Callback)?;
                let page = match pages.reader.read_range(page_offset, page_len) {
                    Ok(page) => page,
                    Err(error) => {
                        release(HnswSearchScratchEvent::Release {
                            container: HnswSearchScratchContainer::QueryBytes,
                            capacity: page_len,
                            element_bytes: 1,
                        });
                        return Err(HnswBoundedSearchError::Callback(E::from(error)));
                    }
                };
                contextdb_core::find_attribution::note_page_fill(page.len());
                #[cfg(any(test, feature = "test-seams"))]
                observe_paged_vector(|observer| observer.page_filled());
                let actual = page.capacity();
                if let Err(error) = acquire(HnswSearchScratchEvent::Reconcile {
                    container: HnswSearchScratchContainer::QueryBytes,
                    requested_capacity: page_len,
                    actual_capacity: actual,
                    element_bytes: 1,
                }) {
                    release(HnswSearchScratchEvent::Release {
                        container: HnswSearchScratchContainer::QueryBytes,
                        capacity: page_len,
                        element_bytes: 1,
                    });
                    return Err(HnswBoundedSearchError::Callback(error));
                }
                if page.len() != page_len {
                    release(HnswSearchScratchEvent::Release {
                        container: HnswSearchScratchContainer::QueryBytes,
                        capacity: actual,
                        element_bytes: 1,
                    });
                    return Err(HnswBoundedSearchError::CapacityInvariant(
                        "paged vector cache page",
                    ));
                }
                // Everything above this point -- the read itself and the
                // proof of what came back -- happened with the cache
                // unlocked, and a page that failed either is never installed.
                let shared = Arc::new(PageBody(page.into_boxed_slice()));
                let kept = {
                    let mut cache = self.page_cache.lock();
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_paged_vector(|observer| observer.page_cache_locked());
                    cache.install(page_offset, Arc::clone(&shared))
                };
                // A page the cache kept is carried by its slot's charge from
                // here on. One it did not keep -- another walk installed the
                // same page first, or every resident page is being read --
                // is this walk's alone, and stays charged to it until the
                // walk drops it.
                if kept {
                    release(HnswSearchScratchEvent::Release {
                        container: HnswSearchScratchContainer::QueryBytes,
                        capacity: actual,
                        element_bytes: 1,
                    });
                    ReadPage::new(shared, 0, &mut *release)
                } else {
                    ReadPage::new(shared, actual, &mut *release)
                }
            };
            if let Some(shared) = page.page.as_ref() {
                pages.hold(page_offset, shared, self.source_page_bytes);
            }
            let page_len = page.len();
            let value_start = point.vector_offset.max(page_offset);
            let value_end = vector_end.min(page_offset.saturating_add(page_len));
            let within_start = value_start.saturating_sub(page_offset);
            let within_end = value_end.saturating_sub(page_offset);
            let mut rest = page.get(within_start..within_end).ok_or(
                HnswBoundedSearchError::CapacityInvariant("paged vector coordinate range"),
            )?;
            if carry_len != 0 {
                let needed = carry.len() - carry_len;
                let taken = needed.min(rest.len());
                carry[carry_len..carry_len + taken].copy_from_slice(&rest[..taken]);
                carry_len += taken;
                rest = &rest[taken..];
                if carry_len == carry.len() {
                    let right =
                        *query
                            .get(coordinate)
                            .ok_or(HnswBoundedSearchError::CapacityInvariant(
                                "paged vector coordinate count",
                            ))?;
                    visit(right, f32::from_le_bytes(carry));
                    coordinate += 1;
                    carry_len = 0;
                }
            }
            // A carry this page could not complete stays carried; only a
            // settled boundary lets the rest of the page decode in place.
            if carry_len == 0 {
                let mut whole = rest.chunks_exact(4);
                for encoded in &mut whole {
                    let right =
                        *query
                            .get(coordinate)
                            .ok_or(HnswBoundedSearchError::CapacityInvariant(
                                "paged vector coordinate count",
                            ))?;
                    visit(
                        right,
                        f32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]),
                    );
                    coordinate += 1;
                }
                let remainder = whole.remainder();
                carry[..remainder.len()].copy_from_slice(remainder);
                carry_len = remainder.len();
            }
            if page_offset == last_page {
                break;
            }
            page_offset = page_offset.checked_add(self.source_page_bytes).ok_or(
                HnswBoundedSearchError::CapacityOverflow("paged vector page offset"),
            )?;
        }
        if coordinate != query.len() || carry_len != 0 {
            return Err(HnswBoundedSearchError::CapacityInvariant(
                "paged vector coordinate count",
            ));
        }
        Ok(true)
    }

    /// Score one stored point against the query from serving pages this walk
    /// already holds. The caller owns the serving cache for the whole walk and
    /// supplies the query norm, so a visit whose page is resident costs its
    /// own distance arithmetic and nothing else.
    #[allow(clippy::too_many_arguments)]
    fn distance<E, S, D, A, R>(
        &self,
        point: &PagedPoint,
        query: &[f32],
        right_norm: f64,
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
        pages: &mut ServingPages,
    ) -> std::result::Result<f32, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        // The walk's one graph-distance evaluation, reported exactly where the
        // resident graph's distance function reports its own: once, before
        // the arithmetic. A candidate's published score out of these same
        // pages (`stored_similarity`) is not a walk evaluation and does not
        // come through here.
        #[cfg(feature = "test-seams")]
        observe_hnsw_candidate_distance();
        let mut dot = 0.0_f64;
        let mut left_norm = 0.0_f64;
        if !self.fold_point_coordinates(
            point,
            query,
            before_source_entry,
            before_distance,
            acquire,
            release,
            pages,
            |right, left| {
                dot += f64::from(left * right);
                left_norm += f64::from(left * left);
            },
        )? {
            return Ok(1.0);
        }
        // One check for the whole stored vector instead of one per coordinate:
        // a sum of squares is non-finite exactly when some coordinate was, and
        // the sum of 768 squared f32 values cannot itself overflow f64.
        if !left_norm.is_finite() {
            return Err(HnswBoundedSearchError::Callback(E::from(Error::Other(
                "durable HNSW point contains a non-finite coordinate".to_string(),
            ))));
        }
        Ok(f32_graph_cosine_distance(dot, left_norm, right_norm))
    }

    /// The stored vector's cosine similarity to the query, summed exactly as a
    /// resident row body sums it: single precision, in coordinate order,
    /// against the query norm the caller already totalled. A candidate scored
    /// out of serving pages is therefore the same number, bit for bit, that
    /// the per-row route publishes for that vector.
    #[allow(clippy::too_many_arguments)]
    fn stored_similarity<E, S, D, A, R>(
        &self,
        point: &PagedPoint,
        query: &[f32],
        query_norm: f32,
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
        pages: &mut ServingPages,
    ) -> std::result::Result<f32, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        // A returned candidate's published score, read out of these pages
        // instead of out of the row's stored body. It is the same step the
        // stored-body route reports as a native score, so it is reported the
        // same way, once, before the arithmetic. It is not a walk evaluation
        // and never reaches the graph observer.
        #[cfg(feature = "test-seams")]
        crate::mem::observe_native_vector_score_for_test();
        let mut dot = 0.0_f32;
        let mut stored_norm = 0.0_f32;
        self.fold_point_coordinates(
            point,
            query,
            before_source_entry,
            before_distance,
            acquire,
            release,
            pages,
            |right, left| {
                dot += right * left;
                stored_norm += left * left;
            },
        )?;
        if query_norm == 0.0 || stored_norm == 0.0 {
            return Ok(0.0);
        }
        Ok(dot / (query_norm.sqrt() * stored_norm.sqrt()))
    }

    /// The unfiltered paged walk. The search's own page directory is charged
    /// here, before it is allocated, and given back here exactly once however
    /// the walk ends -- answered, refused, or cancelled at any distance.
    #[allow(clippy::too_many_arguments)]
    fn search_with_bounded_control<E, S, D, A, R>(
        &self,
        query: &[f32],
        knbn: usize,
        ef: usize,
        mut before_source_entry: S,
        mut before_distance: D,
        mut acquire: A,
        mut release: R,
    ) -> std::result::Result<HnswSearchResult, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        if self.entry_point.is_none() {
            return Ok(HnswSearchResult {
                neighbours: Vec::new(),
            });
        }
        let mut admitted = AdmittedScratch::default();
        admitted.admit(
            HnswSearchScratchContainer::ServingPageDirectory,
            self.page_slots,
            SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            &mut acquire,
        )?;
        let walked = match self.source.reader() {
            Ok(reader) => {
                let mut pages = ServingPages::new(reader, self.page_slots);
                let mut source = PagedGraphSource {
                    graph: self,
                    pages: &mut pages,
                    query_norm: Self::query_norm(query),
                };
                unfiltered::search(
                    &mut source,
                    query,
                    knbn,
                    ef,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                    &mut admitted,
                )
            }
            Err(error) => Err(HnswBoundedSearchError::Callback(E::from(error))),
        };
        // On an answered walk this is already empty; on any other way out it
        // is exactly what the walk was admitted for and has not yet returned.
        admitted.give_back(&mut release);
        walked
    }

    #[allow(clippy::too_many_arguments)]
    fn search_allowed_with_bounded_control<E, S, D, A, R, F>(
        &self,
        query: &[f32],
        required_results: usize,
        ef: usize,
        limits: HnswAllowedSearchLimits,
        seed_points: &[PointId],
        allowed_count: usize,
        score_seed_points_directly: bool,
        mut candidate_key: impl FnMut(usize) -> u64,
        mut allowed: F,
        mut before_source_entry: S,
        mut before_distance: D,
        mut acquire: A,
        mut release: R,
    ) -> std::result::Result<HnswAllowedSearchResult, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(usize) -> bool,
    {
        // When the bounded seed directory contains the complete relational
        // candidate set, score it directly. This avoids losing a sparse set
        // behind unrelated graph neighbours while preserving graph traversal
        // for candidate sets larger than the declared search effort.
        if score_seed_points_directly
            && !seed_points.is_empty()
            && seed_points.len() >= allowed_count
        {
            let capacity = seed_points.len().min(allowed_count);
            acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::Result,
                previous_capacity: 0,
                requested_capacity: capacity,
                element_bytes: std::mem::size_of::<Neighbour>(),
            })
            .map_err(HnswBoundedSearchError::Callback)?;
            let mut neighbours: Vec<Neighbour> = Vec::new();
            if neighbours.try_reserve_exact(capacity).is_err() {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::Result,
                    capacity,
                    element_bytes: std::mem::size_of::<Neighbour>(),
                });
                return Err(HnswBoundedSearchError::AllocationFailed(
                    "paged allowed seed results",
                ));
            }
            let actual_capacity = neighbours.capacity();
            if let Err(error) = acquire(HnswSearchScratchEvent::Reconcile {
                container: HnswSearchScratchContainer::Result,
                requested_capacity: capacity,
                actual_capacity,
                element_bytes: std::mem::size_of::<Neighbour>(),
            }) {
                // Each allocation goes before its charge does.
                drop(neighbours);
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::Result,
                    capacity,
                    element_bytes: std::mem::size_of::<Neighbour>(),
                });
                return Err(HnswBoundedSearchError::Callback(error));
            }
            // Seeds are scored in stored order, so about twenty-one of them
            // in a row come out of the same page; the directory is what stops
            // each of those twenty-one taking the shared mutex.
            if let Err(error) = acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::ServingPageDirectory,
                previous_capacity: 0,
                requested_capacity: self.page_slots,
                element_bytes: SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            }) {
                drop(neighbours);
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::Result,
                    capacity: actual_capacity,
                    element_bytes: std::mem::size_of::<Neighbour>(),
                });
                return Err(HnswBoundedSearchError::Callback(error));
            }
            let mut page_reader = match self
                .source
                .reader()
                .map(|reader| ServingPages::new(reader, self.page_slots))
            {
                Ok(reader) => reader,
                Err(error) => {
                    drop(neighbours);
                    release(HnswSearchScratchEvent::Release {
                        container: HnswSearchScratchContainer::ServingPageDirectory,
                        capacity: self.page_slots,
                        element_bytes: SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
                    });
                    release(HnswSearchScratchEvent::Release {
                        container: HnswSearchScratchContainer::Result,
                        capacity: actual_capacity,
                        element_bytes: std::mem::size_of::<Neighbour>(),
                    });
                    return Err(HnswBoundedSearchError::Callback(E::from(error)));
                }
            };
            let right_norm = Self::query_norm(query);
            let mut visited_nodes = 0usize;
            let mut vector_evaluations = 0usize;
            let mut allowed_admissions = 0usize;
            let mut incomplete = None;
            for point_id in seed_points {
                let Some(point) = self.point(*point_id) else {
                    continue;
                };
                if !allowed(point.origin_id) {
                    continue;
                }
                if visited_nodes >= limits.max_visited_nodes {
                    incomplete = Some(HnswAllowedSearchIncomplete::VisitLimitReached);
                    break;
                }
                if vector_evaluations >= limits.max_vector_evaluations {
                    incomplete = Some(HnswAllowedSearchIncomplete::VectorEvaluationLimitReached);
                    break;
                }
                let distance = match self.distance(
                    point,
                    query,
                    right_norm,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                    page_reader.as_mut(),
                ) {
                    Ok(distance) => distance,
                    Err(error) => {
                        drop(page_reader);
                        drop(neighbours);
                        release(HnswSearchScratchEvent::Release {
                            container: HnswSearchScratchContainer::ServingPageDirectory,
                            capacity: self.page_slots,
                            element_bytes: SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
                        });
                        release(HnswSearchScratchEvent::Release {
                            container: HnswSearchScratchContainer::Result,
                            capacity: actual_capacity,
                            element_bytes: std::mem::size_of::<Neighbour>(),
                        });
                        return Err(error);
                    }
                };
                visited_nodes += 1;
                vector_evaluations += 1;
                allowed_admissions += 1;
                let key = candidate_key(point.origin_id);
                if let Some(existing) = neighbours
                    .iter_mut()
                    .find(|candidate| candidate_key(candidate.d_id) == key)
                {
                    if distance < existing.distance {
                        *existing = Neighbour::new(point.origin_id, distance, *point_id);
                    }
                } else {
                    neighbours.push(Neighbour::new(point.origin_id, distance, *point_id));
                }
                if neighbours.len() >= allowed_count {
                    break;
                }
            }
            drop(page_reader);
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::ServingPageDirectory,
                capacity: self.page_slots,
                element_bytes: SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            });
            neighbours.sort_unstable_by(|left, right| left.distance.total_cmp(&right.distance));
            neighbours.truncate(required_results.min(neighbours.len()));
            let status = incomplete.map_or_else(
                || {
                    if neighbours.len() >= required_results || allowed_count == 0 {
                        HnswAllowedSearchStatus::Complete
                    } else {
                        HnswAllowedSearchStatus::Incomplete(
                            HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                        )
                    }
                },
                HnswAllowedSearchStatus::Incomplete,
            );
            return Ok(HnswAllowedSearchResult {
                neighbours,
                status,
                visited_nodes,
                vector_evaluations,
                allowed_admissions,
            });
        }
        // The walk itself keeps the filter in hand. Searching the whole graph
        // and dropping the rows the reader may not see afterwards returns
        // whatever of the graph's own nearest neighbours happened to be
        // admitted, which for a filter that keeps a small share of the rows is
        // not the nearest admitted rows and is often nothing at all.
        let first = self.allowed_attempt(
            query,
            required_results,
            ef,
            limits,
            seed_points,
            allowed_count,
            false,
            &mut allowed,
            &mut before_source_entry,
            &mut before_distance,
            &mut acquire,
            &mut release,
        )?;
        if !matches!(
            first.status,
            HnswAllowedSearchStatus::Incomplete(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates
            )
        ) {
            return Ok(first);
        }
        // The admitted rows did not reach each other. Walk again through the
        // excluded ones as navigation, inside what is left of the same budget.
        // The first attempt's rows are discarded, so they are dropped and their
        // charge given back before the second attempt asks for anything: the
        // retry never spends allowance on an answer nobody will read.
        let HnswAllowedSearchResult {
            neighbours: discarded,
            visited_nodes: first_visited_nodes,
            vector_evaluations: first_vector_evaluations,
            allowed_admissions: first_allowed_admissions,
            ..
        } = first;
        let discarded_rows = discarded.len();
        drop(discarded);
        if discarded_rows != 0 {
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::Result,
                capacity: discarded_rows,
                element_bytes: std::mem::size_of::<Neighbour>(),
            });
        }
        let remaining = HnswAllowedSearchLimits {
            max_visited_nodes: limits.max_visited_nodes.saturating_sub(first_visited_nodes),
            max_vector_evaluations: limits
                .max_vector_evaluations
                .saturating_sub(first_vector_evaluations),
        };
        let mut result = self.allowed_attempt(
            query,
            required_results,
            ef,
            remaining,
            seed_points,
            allowed_count,
            true,
            &mut allowed,
            &mut before_source_entry,
            &mut before_distance,
            &mut acquire,
            &mut release,
        )?;
        result.visited_nodes += first_visited_nodes;
        result.vector_evaluations += first_vector_evaluations;
        result.allowed_admissions += first_allowed_admissions;
        Ok(result)
    }

    /// One walk over this generation that keeps only rows the filter admits.
    ///
    /// `allow_bridges` decides what the walk may step on. Off, it moves only
    /// between admitted points and takes one hop through an excluded neighbour
    /// where the admitted points would otherwise stop reaching each other. On,
    /// it navigates the whole graph and keeps what it is allowed to keep, which
    /// is the only thing that answers a filter selective enough to break the
    /// admitted set into pieces.
    #[allow(clippy::too_many_arguments)]
    fn allowed_attempt<E, S, D, A, R, F>(
        &self,
        query: &[f32],
        required_results: usize,
        ef: usize,
        limits: HnswAllowedSearchLimits,
        seed_points: &[PointId],
        allowed_count: usize,
        allow_bridges: bool,
        allowed: &mut F,
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<HnswAllowedSearchResult, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(usize) -> bool,
    {
        let point_count = self.points.iter().map(Vec::len).sum::<usize>();
        let empty = |incomplete: Option<HnswAllowedSearchIncomplete>| HnswAllowedSearchResult {
            neighbours: Vec::new(),
            status: incomplete
                .map(HnswAllowedSearchStatus::Incomplete)
                .unwrap_or(HnswAllowedSearchStatus::Complete),
            visited_nodes: 0,
            vector_evaluations: 0,
            allowed_admissions: 0,
        };
        if required_results == 0 || allowed_count == 0 {
            return Ok(empty(None));
        }
        let Some(entry_id) = self.entry_point else {
            return Ok(empty(Some(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
            )));
        };
        // Everything this walk is charged for, given back on every way out.
        // The search's own page directory comes first, charged before it is
        // allocated and sized by the serving cache it shadows.
        let mut admitted = AdmittedScratch::default();
        admitted.admit(
            HnswSearchScratchContainer::ServingPageDirectory,
            self.page_slots,
            SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            acquire,
        )?;
        let mut page_reader = match self.source.reader() {
            Ok(reader) => ServingPages::new(reader, self.page_slots),
            Err(error) => {
                admitted.give_back(release);
                return Err(HnswBoundedSearchError::Callback(E::from(error)));
            }
        };
        let right_norm = Self::query_norm(query);
        let mut visited_nodes = 0usize;
        let mut vector_evaluations = 0usize;
        let mut allowed_admissions = 0usize;
        let mut work = PagedWalkWork::default();
        let mut incomplete = None;
        let result_capacity = ef.max(required_results).min(point_count.max(1));

        // The descent can stop on the caller's own refusal or cancellation at
        // any distance; whichever way it ends, the directory charged above is
        // given back exactly once.
        let descended = (|| -> std::result::Result<(PointId, f32), HnswBoundedSearchError<E>> {
            let mut pivot_id = entry_id;
            let mut pivot_distance = self.distance(
                self.point(pivot_id)
                    .ok_or(HnswBoundedSearchError::SearchInvariant("paged entry point"))?,
                query,
                right_norm,
                before_source_entry,
                before_distance,
                acquire,
                release,
                page_reader.as_mut(),
            )?;
            visited_nodes += 1;
            vector_evaluations += 1;
            work.evaluations += 1;
            for layer in (1..=entry_id.0).rev() {
                loop {
                    let mut changed = false;
                    let neighbours = self
                        .point(pivot_id)
                        .and_then(|point| point.neighbours.get(layer as usize))
                        .ok_or(HnswBoundedSearchError::SearchInvariant(
                            "paged upper-layer neighbours",
                        ))?;
                    for neighbour in neighbours {
                        work.edges += 1;
                        if !allow_bridges && !allowed(neighbour.origin_id as usize) {
                            continue;
                        }
                        if visited_nodes >= limits.max_visited_nodes
                            || vector_evaluations >= limits.max_vector_evaluations
                        {
                            break;
                        }
                        let candidate = self
                            .point(neighbour.point_id)
                            .ok_or(HnswBoundedSearchError::SearchInvariant("paged graph edge"))?;
                        let distance = self.distance(
                            candidate,
                            query,
                            right_norm,
                            before_source_entry,
                            before_distance,
                            acquire,
                            release,
                            page_reader.as_mut(),
                        )?;
                        visited_nodes += 1;
                        vector_evaluations += 1;
                        work.evaluations += 1;
                        if distance < pivot_distance {
                            pivot_id = neighbour.point_id;
                            pivot_distance = distance;
                            changed = true;
                        }
                    }
                    if !changed {
                        break;
                    }
                }
            }
            Ok((pivot_id, pivot_distance))
        })();
        let (pivot_id, pivot_distance) = match descended {
            Ok(pivot) => pivot,
            Err(error) => {
                drop(page_reader);
                admitted.give_back(release);
                return Err(error);
            }
        };

        let traversal_capacity = point_count.max(1);
        let reached_words = ReachedPoints::words_for(&self.points);
        for (container, capacity, element_bytes) in [
            (
                HnswSearchScratchContainer::CandidateHeap,
                traversal_capacity,
                std::mem::size_of::<Neighbour>(),
            ),
            (
                HnswSearchScratchContainer::ReturnHeap,
                result_capacity + 1,
                std::mem::size_of::<Neighbour>(),
            ),
            (
                HnswSearchScratchContainer::VisitedPoints,
                reached_words,
                std::mem::size_of::<u64>(),
            ),
        ] {
            // A refusal here gives back the directory and whatever of these
            // was admitted before it, once the directory is dropped.
            if let Err(error) = admitted.admit(container, capacity, element_bytes, acquire) {
                drop(page_reader);
                admitted.give_back(release);
                return Err(error);
            }
        }
        let mut candidates: Vec<Neighbour> = Vec::with_capacity(traversal_capacity);
        // One more than the breadth: a new point is pushed before the worst is
        // evicted, and a push past capacity would grow it past its charge.
        let mut found: Vec<Neighbour> = Vec::with_capacity(result_capacity + 1);
        let mut visited = ReachedPoints::new(&self.points, traversal_capacity);
        let mut converged = false;

        let walked = (|| -> std::result::Result<(), HnswBoundedSearchError<E>> {
            let entry = self
                .point(pivot_id)
                .ok_or(HnswBoundedSearchError::SearchInvariant("paged entry point"))?;
            visited.insert(pivot_id);
            candidates.push(Neighbour::new(entry.origin_id, pivot_distance, pivot_id));
            if allowed(entry.origin_id) {
                found.push(Neighbour::new(entry.origin_id, pivot_distance, pivot_id));
                allowed_admissions += 1;
            }
            for seed in seed_points {
                work.reached_entries += 1;
                if visited.contains(*seed) {
                    continue;
                }
                let Some(point) = self.point(*seed) else {
                    continue;
                };
                if !allowed(point.origin_id) {
                    continue;
                }
                if visited_nodes >= limits.max_visited_nodes {
                    incomplete = Some(HnswAllowedSearchIncomplete::VisitLimitReached);
                    return Ok(());
                }
                if vector_evaluations >= limits.max_vector_evaluations {
                    incomplete = Some(HnswAllowedSearchIncomplete::VectorEvaluationLimitReached);
                    return Ok(());
                }
                let distance = self.distance(
                    point,
                    query,
                    right_norm,
                    before_source_entry,
                    before_distance,
                    acquire,
                    release,
                    page_reader.as_mut(),
                )?;
                visited_nodes += 1;
                vector_evaluations += 1;
                work.evaluations += 1;
                visited.insert(*seed);
                candidates.push(Neighbour::new(point.origin_id, distance, *seed));
                found.push(Neighbour::new(point.origin_id, distance, *seed));
                allowed_admissions += 1;
                if found.len() > result_capacity {
                    work.result_entries += found.len() as u64;
                    let worst = found
                        .iter()
                        .enumerate()
                        .max_by(|(_, left), (_, right)| left.distance.total_cmp(&right.distance))
                        .map(|(position, _)| position)
                        .unwrap_or(0);
                    found.swap_remove(worst);
                }
            }

            while !candidates.is_empty() {
                work.frontier_entries += candidates.len() as u64;
                let best = candidates
                    .iter()
                    .enumerate()
                    .min_by(|(_, left), (_, right)| left.distance.total_cmp(&right.distance))
                    .map(|(position, _)| position)
                    .unwrap_or(0);
                let candidate = candidates.swap_remove(best);
                if found.len() >= allowed_count {
                    converged = true;
                    break;
                }
                work.result_entries += found.len() as u64;
                let worst = found
                    .iter()
                    .map(|item| item.distance)
                    .max_by(f32::total_cmp)
                    .unwrap_or(f32::INFINITY);
                if found.len() >= result_capacity && candidate.distance > worst {
                    converged = true;
                    break;
                }
                // Read in place from the decoded graph, never copied: a list
                // grows with the declared degree, and a copy would be memory
                // the walk holds without having been admitted for it.
                let neighbours = self
                    .point(candidate.p_id)
                    .and_then(|point| point.neighbours.first())
                    .ok_or(HnswBoundedSearchError::SearchInvariant(
                        "paged layer-zero neighbours",
                    ))?;
                for neighbour in neighbours {
                    let neighbour_is_allowed = allowed(neighbour.origin_id as usize);
                    if let Some(reason) = self.consider_paged_point(
                        neighbour.point_id,
                        query,
                        right_norm,
                        result_capacity,
                        limits,
                        allow_bridges,
                        allowed,
                        &mut visited,
                        &mut candidates,
                        &mut found,
                        &mut visited_nodes,
                        &mut vector_evaluations,
                        &mut allowed_admissions,
                        &mut work,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                        page_reader.as_mut(),
                    )? {
                        incomplete = Some(reason);
                        return Ok(());
                    }
                    if allow_bridges || neighbour_is_allowed {
                        continue;
                    }
                    // The excluded neighbour is where the admitted points stop
                    // reaching each other. Step through it once, as navigation.
                    let Some(bridges) = self
                        .point(neighbour.point_id)
                        .and_then(|point| point.neighbours.first())
                    else {
                        continue;
                    };
                    for bridge in bridges {
                        if let Some(reason) = self.consider_paged_point(
                            bridge.point_id,
                            query,
                            right_norm,
                            result_capacity,
                            limits,
                            allow_bridges,
                            allowed,
                            &mut visited,
                            &mut candidates,
                            &mut found,
                            &mut visited_nodes,
                            &mut vector_evaluations,
                            &mut allowed_admissions,
                            &mut work,
                            before_source_entry,
                            before_distance,
                            acquire,
                            release,
                            page_reader.as_mut(),
                        )? {
                            incomplete = Some(reason);
                            return Ok(());
                        }
                    }
                }
            }
            Ok(())
        })();

        // Each charge goes back only once what it covers is gone; the result
        // heap stays charged until the answer has been copied out of it.
        drop(candidates);
        drop(visited);
        drop(page_reader);
        admitted.give_back_only(HnswSearchScratchContainer::CandidateHeap, release);
        admitted.give_back_only(HnswSearchScratchContainer::VisitedPoints, release);
        admitted.give_back_only(HnswSearchScratchContainer::ServingPageDirectory, release);
        #[cfg(any(test, feature = "test-seams"))]
        observe_paged_vector(|observer| observer.walk_compared(work));
        if let Err(error) = walked {
            drop(found);
            admitted.give_back(release);
            return Err(error);
        }

        found.sort_unstable_by(|left, right| left.distance.total_cmp(&right.distance));
        found.truncate(required_results.min(found.len()));
        if let Err(error) = acquire(HnswSearchScratchEvent::Reserve {
            container: HnswSearchScratchContainer::Result,
            previous_capacity: 0,
            requested_capacity: found.len(),
            element_bytes: std::mem::size_of::<Neighbour>(),
        }) {
            drop(found);
            admitted.give_back(release);
            return Err(HnswBoundedSearchError::Callback(error));
        }
        let mut neighbours = Vec::with_capacity(found.len());
        neighbours.extend(found);
        admitted.give_back(release);

        // A walk that ran out of frontier, or that converged inside a filter so
        // sparse that the admitted rows no longer reach each other, has not
        // searched this query's breadth: a filter keeping a fraction `p` of a
        // graph of degree `M` leaves each admitted point about `p * M` admitted
        // neighbours, and below one the admitted set is no longer one graph. A
        // walk that already navigates through excluded points is the remedy and
        // is never judged by this.
        let fragmented = allowed_count.saturating_mul(self.max_nb_connection) < point_count;
        let starved = !allow_bridges
            && allowed_admissions < allowed_count
            && allowed_count < point_count
            && (!converged || fragmented);
        let status = incomplete.map_or_else(
            || {
                if neighbours.len() < required_results || starved {
                    HnswAllowedSearchStatus::Incomplete(
                        HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                    )
                } else {
                    HnswAllowedSearchStatus::Complete
                }
            },
            HnswAllowedSearchStatus::Incomplete,
        );
        Ok(HnswAllowedSearchResult {
            neighbours,
            status,
            visited_nodes,
            vector_evaluations,
            allowed_admissions,
        })
    }

    /// Reach one point of a paged walk: skip it if it has been reached or the
    /// filter excludes it, otherwise score it, make it a candidate, and keep it
    /// if the filter admits it. Returns the reason the walk must stop, if a
    /// declared limit was reached.
    #[allow(clippy::too_many_arguments)]
    fn consider_paged_point<E, S, D, A, R, F>(
        &self,
        point_id: PointId,
        query: &[f32],
        right_norm: f64,
        result_capacity: usize,
        limits: HnswAllowedSearchLimits,
        allow_bridges: bool,
        allowed: &mut F,
        visited: &mut ReachedPoints,
        candidates: &mut Vec<Neighbour>,
        found: &mut Vec<Neighbour>,
        visited_nodes: &mut usize,
        vector_evaluations: &mut usize,
        allowed_admissions: &mut usize,
        work: &mut PagedWalkWork,
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
        pages: &mut ServingPages,
    ) -> std::result::Result<Option<HnswAllowedSearchIncomplete>, HnswBoundedSearchError<E>>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(usize) -> bool,
    {
        // Counted where it is paid: one entry looked at per graph edge, which
        // is what makes this constant-time rather than a scan of everything
        // the walk has already reached.
        work.edges += 1;
        work.reached_entries += 1;
        if visited.contains(point_id) || visited.len() >= visited.capacity() {
            return Ok(None);
        }
        let Some(point) = self.point(point_id) else {
            return Ok(None);
        };
        let admitted = allowed(point.origin_id);
        if !allow_bridges && !admitted {
            return Ok(None);
        }
        if *visited_nodes >= limits.max_visited_nodes {
            return Ok(Some(HnswAllowedSearchIncomplete::VisitLimitReached));
        }
        if *vector_evaluations >= limits.max_vector_evaluations {
            return Ok(Some(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        }
        let distance = self.distance(
            point,
            query,
            right_norm,
            before_source_entry,
            before_distance,
            acquire,
            release,
            pages,
        )?;
        *visited_nodes += 1;
        *vector_evaluations += 1;
        work.evaluations += 1;
        visited.insert(point_id);
        if candidates.len() < candidates.capacity() {
            candidates.push(Neighbour::new(point.origin_id, distance, point_id));
        }
        if !admitted {
            return Ok(None);
        }
        *allowed_admissions += 1;
        found.push(Neighbour::new(point.origin_id, distance, point_id));
        if found.len() > result_capacity {
            work.result_entries += found.len() as u64;
            let worst = found
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| left.distance.total_cmp(&right.distance))
                .map(|(position, _)| position)
                .unwrap_or(0);
            found.swap_remove(worst);
        }
        Ok(None)
    }
}

const DURABLE_ENVELOPE_MAGIC: [u8; 8] = *b"CDHNSW\0\x01";
const DURABLE_ENVELOPE_VERSION: u16 = 1;

/// Cosine distance for an F32 graph walk. A zero-norm query or stored vector
/// has distance 1, matching the published cosine score of 0. Saved pages and
/// resident points both finish their arithmetic here, so admission, pruning
/// and stopping compare one meaning.
fn f32_graph_cosine_distance(dot: f64, left_norm_sq: f64, right_norm_sq: f64) -> f32 {
    if left_norm_sq == 0.0 || right_norm_sq == 0.0 {
        1.0
    } else {
        ((1.0 - dot / (left_norm_sq.sqrt() * right_norm_sq.sqrt())).max(0.0)) as f32
    }
}

#[derive(Debug, Clone, Copy)]
struct HnswF32Distance;

impl Distance<f32> for HnswF32Distance {
    fn eval(&self, va: &[f32], vb: &[f32]) -> f32 {
        #[cfg(feature = "test-seams")]
        observe_hnsw_candidate_distance();
        let mut dot = 0.0_f64;
        let mut left_norm = 0.0_f64;
        let mut right_norm = 0.0_f64;
        for (left, right) in va.iter().zip(vb.iter()) {
            dot += f64::from(*left * *right);
            left_norm += f64::from(*left * *left);
            right_norm += f64::from(*right * *right);
        }
        f32_graph_cosine_distance(dot, left_norm, right_norm)
    }
}

fn hnsw_f32_distance() -> HnswF32Distance {
    HnswF32Distance
}

/// What a standard `HashSet` reporting this capacity has allocated: a slot
/// per bucket, a control byte per bucket, and one trailing group of control
/// bytes. The capacity a set reports is derived from its bucket count, so the
/// buckets are recovered from it the way the set sizes them. Scratch sets are
/// charged this, not `capacity * size_of::<T>()`, which leaves out every
/// control byte.
fn set_allocation_bytes<T>(capacity: usize) -> usize {
    const GROUP_BYTES: usize = 16;
    if capacity == 0 {
        return 0;
    }
    let buckets = if capacity < 8 {
        if capacity < 4 { 4 } else { 8 }
    } else {
        (capacity.saturating_mul(8) / 7).next_power_of_two()
    };
    buckets
        .saturating_mul(std::mem::size_of::<T>())
        .next_multiple_of(GROUP_BYTES.max(std::mem::align_of::<T>()))
        .saturating_add(buckets)
        .saturating_add(GROUP_BYTES)
}

struct VectorSearchScratch<'a, E> {
    acquire: &'a mut dyn FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    release: &'a mut dyn FnMut(HnswSearchScratchEvent),
}

impl<E> VectorSearchScratch<'_, E>
where
    E: From<Error>,
{
    fn next_capacity(&self, current: usize, required: usize) -> std::result::Result<usize, E> {
        if current == 0 {
            return Ok(required);
        }
        Ok(current
            .checked_mul(2)
            .ok_or_else(|| E::from(Error::Other("bounded HNSW capacity overflow".to_string())))?
            .max(required))
    }

    fn release_capacity(
        &mut self,
        container: HnswSearchScratchContainer,
        capacity: usize,
        element_bytes: usize,
    ) {
        if capacity != 0 && element_bytes != 0 {
            (self.release)(HnswSearchScratchEvent::Release {
                container,
                capacity,
                element_bytes,
            });
        }
    }

    fn allocate_vec<T>(
        &mut self,
        container: HnswSearchScratchContainer,
        requested_capacity: usize,
        operation: &str,
    ) -> std::result::Result<Vec<T>, E> {
        if requested_capacity == 0 || std::mem::size_of::<T>() == 0 {
            return Ok(Vec::new());
        }
        let element_bytes = std::mem::size_of::<T>();
        (self.acquire)(HnswSearchScratchEvent::Reserve {
            container,
            previous_capacity: 0,
            requested_capacity,
            element_bytes,
        })?;
        let mut values = Vec::new();
        if values.try_reserve_exact(requested_capacity).is_err() {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} allocation failed"
            ))));
        }
        let actual_capacity = values.capacity();
        if actual_capacity < requested_capacity {
            drop(values);
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} capacity moved backwards"
            ))));
        }
        if let Err(error) = (self.acquire)(HnswSearchScratchEvent::Reconcile {
            container,
            requested_capacity,
            actual_capacity,
            element_bytes,
        }) {
            // The allocation exists already; it goes before its charge does.
            drop(values);
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(error);
        }
        Ok(values)
    }

    /// Grow a scratch vector. Each migrated element is charged through the
    /// caller's work control first, exactly as the set migration below does,
    /// so a doubling chain stays inside the work budget and stays cancellable.
    fn ensure_vec_capacity<T: Copy>(
        &mut self,
        values: &mut Vec<T>,
        container: HnswSearchScratchContainer,
        required: usize,
        operation: &str,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if required <= values.capacity() {
            return Ok(());
        }
        let previous_capacity = values.capacity();
        let requested_capacity = self.next_capacity(previous_capacity, required)?;
        let mut replacement = self.allocate_vec(container, requested_capacity, operation)?;
        let mut position = 0usize;
        while position < values.len() {
            if let Err(error) = before_work() {
                self.free_vec(replacement, container);
                return Err(error);
            }
            let value = match values.get(position) {
                Some(value) => *value,
                None => {
                    self.free_vec(replacement, container);
                    return Err(E::from(Error::Other(format!(
                        "bounded HNSW {operation} changed during migration"
                    ))));
                }
            };
            position = match position.checked_add(1) {
                Some(position) => position,
                None => {
                    self.free_vec(replacement, container);
                    return Err(E::from(Error::Other(format!(
                        "bounded HNSW {operation} migration position overflow"
                    ))));
                }
            };
            replacement.push(value);
        }
        let old = std::mem::replace(values, replacement);
        self.free_vec(old, container);
        Ok(())
    }

    fn allocate_set<T>(
        &mut self,
        container: HnswSearchScratchContainer,
        requested_capacity: usize,
        operation: &str,
    ) -> std::result::Result<HashSet<T>, E>
    where
        T: std::hash::Hash + Eq,
    {
        if requested_capacity == 0 || std::mem::size_of::<T>() == 0 {
            return Ok(HashSet::new());
        }
        // Charged in bytes: a set allocates a control byte per bucket and a
        // trailing group beside its slots, which a count of elements misses.
        let requested_bytes = set_allocation_bytes::<T>(requested_capacity);
        (self.acquire)(HnswSearchScratchEvent::Reserve {
            container,
            previous_capacity: 0,
            requested_capacity: requested_bytes,
            element_bytes: 1,
        })?;
        let mut values = HashSet::new();
        if values.try_reserve(requested_capacity).is_err() {
            self.release_capacity(container, requested_bytes, 1);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} allocation failed"
            ))));
        }
        let actual_capacity = values.capacity();
        if actual_capacity < requested_capacity {
            drop(values);
            self.release_capacity(container, requested_bytes, 1);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} capacity moved backwards"
            ))));
        }
        let actual_bytes = set_allocation_bytes::<T>(actual_capacity);
        if let Err(error) = (self.acquire)(HnswSearchScratchEvent::Reconcile {
            container,
            requested_capacity: requested_bytes,
            actual_capacity: actual_bytes,
            element_bytes: 1,
        }) {
            drop(values);
            self.release_capacity(container, requested_bytes, 1);
            return Err(error);
        }
        Ok(values)
    }

    fn ensure_set_capacity<T>(
        &mut self,
        values: &mut HashSet<T>,
        container: HnswSearchScratchContainer,
        required: usize,
        operation: &str,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E>
    where
        T: std::hash::Hash + Eq + Copy,
    {
        if required <= values.capacity() {
            return Ok(());
        }
        let previous_capacity = values.capacity();
        let requested_capacity = self.next_capacity(previous_capacity, required)?;
        let mut replacement = self.allocate_set(container, requested_capacity, operation)?;
        let mut remaining = values.len();
        let mut source = values.iter();
        while remaining != 0 {
            if let Err(error) = before_work() {
                self.free_set(replacement, container);
                return Err(error);
            }
            let Some(value) = source.next().copied() else {
                self.free_set(replacement, container);
                return Err(E::from(Error::Other(format!(
                    "bounded HNSW {operation} changed during migration"
                ))));
            };
            remaining = match remaining.checked_sub(1) {
                Some(remaining) => remaining,
                None => {
                    self.free_set(replacement, container);
                    return Err(E::from(Error::Other(format!(
                        "bounded HNSW {operation} migration underflow"
                    ))));
                }
            };
            replacement.insert(value);
        }
        let old = std::mem::replace(values, replacement);
        self.free_set(old, container);
        Ok(())
    }

    /// Drop a scratch vector, then give back its charge -- in that order, so
    /// the charge never returns while the memory it covers is still in use.
    fn free_vec<T>(&mut self, values: Vec<T>, container: HnswSearchScratchContainer) {
        let capacity = values.capacity();
        drop(values);
        self.release_capacity(container, capacity, std::mem::size_of::<T>());
    }

    /// Drop a scratch set, then give back its charge.
    fn free_set<T>(&mut self, values: HashSet<T>, container: HnswSearchScratchContainer) {
        let bytes = set_allocation_bytes::<T>(values.capacity());
        drop(values);
        self.release_capacity(container, bytes, 1);
    }
}

fn map_core_bounded_error<E>(error: HnswBoundedSearchError<E>) -> E
where
    E: From<Error>,
{
    match error {
        HnswBoundedSearchError::Callback(error) => error,
        HnswBoundedSearchError::CapacityOverflow(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} capacity overflow"
        ))),
        HnswBoundedSearchError::AllocationFailed(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} allocation failed"
        ))),
        HnswBoundedSearchError::CapacityInvariant(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} capacity moved backwards"
        ))),
        HnswBoundedSearchError::SearchInvariant(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} violated a search invariant"
        ))),
    }
}

fn quantized_query_bounds(query: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = query.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}

fn encode_bounded_quantized_query<E>(
    query: &[f32],
    quantization: VectorQuantization,
    scratch: &mut VectorSearchScratch<'_, E>,
) -> std::result::Result<Vec<u8>, E>
where
    E: From<Error>,
{
    let payload_len = match quantization {
        VectorQuantization::SQ8 => query.len(),
        VectorQuantization::SQ4 => query.len().div_ceil(2),
        VectorQuantization::F32 => {
            return Err(E::from(Error::Other(
                "bounded HNSW requested a quantized query for F32 data".to_string(),
            )));
        }
    };
    let encoded_len = 12usize
        .checked_add(payload_len)
        .ok_or_else(|| E::from(Error::Other("bounded HNSW query size overflow".to_string())))?;
    let mut encoded = scratch.allocate_vec(
        HnswSearchScratchContainer::QueryBytes,
        encoded_len,
        "quantized query",
    )?;
    let (min, max) = quantized_query_bounds(query);
    encoded.extend_from_slice(&(query.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&min.to_le_bytes());
    encoded.extend_from_slice(&max.to_le_bytes());
    let range = max - min;
    match quantization {
        VectorQuantization::SQ8 => {
            for value in query {
                let quantized = if range <= f32::EPSILON {
                    0
                } else {
                    (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                };
                encoded.push(quantized);
            }
        }
        VectorQuantization::SQ4 => {
            let mut values = query.iter();
            while let Some(high) = values.next() {
                let quantize = |value: f32| {
                    if range <= f32::EPSILON {
                        0
                    } else {
                        (((value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                    }
                };
                let high = quantize(*high) & 0x0f;
                let low = values.next().map_or(0, |value| quantize(*value)) & 0x0f;
                encoded.push((high << 4) | low);
            }
        }
        VectorQuantization::F32 => {
            scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
            return Err(E::from(Error::Other(
                "bounded HNSW reached the quantized encoder with F32 data".to_string(),
            )));
        }
    }
    Ok(encoded)
}

fn bounded_f32_exact_key<E>(
    query: &[f32],
    scratch: &mut VectorSearchScratch<'_, E>,
) -> std::result::Result<Vec<u8>, E>
where
    E: From<Error>,
{
    let bytes = query
        .len()
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| E::from(Error::Other("bounded HNSW exact-key overflow".to_string())))?;
    let mut key = scratch.allocate_vec(HnswSearchScratchContainer::ExactKey, bytes, "exact key")?;
    for value in query {
        key.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    Ok(key)
}

fn bounded_exact_query_score<E>(
    query: &[f32],
    quantization: VectorQuantization,
    encoded: Option<&[u8]>,
    before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<Option<f32>, E>
where
    E: From<Error>,
{
    before_distance()?;
    #[cfg(feature = "test-seams")]
    observe_hnsw_query_self_score(query.len(), quantization);
    let (mut dot, mut query_norm, mut vector_norm) = (0.0_f32, 0.0_f32, 0.0_f32);
    match quantization {
        VectorQuantization::F32 => {
            for value in query {
                dot += *value * *value;
                query_norm += *value * *value;
                vector_norm += *value * *value;
            }
        }
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            let Some(encoded) = encoded else {
                return Ok(None);
            };
            let (min, max) = quantized_query_bounds(query);
            let range = max - min;
            let levels = if matches!(quantization, VectorQuantization::SQ8) {
                255.0
            } else {
                15.0
            };
            let scale = if range <= f32::EPSILON {
                0.0
            } else {
                range / levels
            };
            let payload = encoded.get(12..).unwrap_or_default();
            for (index, value) in query.iter().copied().enumerate() {
                let quantized = match quantization {
                    VectorQuantization::SQ8 => payload.get(index).copied().unwrap_or(0),
                    VectorQuantization::SQ4 => {
                        let byte = payload.get(index / 2).copied().unwrap_or(0);
                        if index.is_multiple_of(2) {
                            byte >> 4
                        } else {
                            byte & 0x0f
                        }
                    }
                    VectorQuantization::F32 => {
                        return Err(E::from(Error::Other(
                            "bounded HNSW scored a quantized query against F32 data".to_string(),
                        )));
                    }
                };
                let stored = if scale == 0.0 {
                    min
                } else {
                    min + quantized as f32 * scale
                };
                dot += value * stored;
                query_norm += value * value;
                vector_norm += stored * stored;
            }
        }
    }
    let score = if query_norm == 0.0 || vector_norm == 0.0 {
        0.0
    } else {
        dot / (query_norm.sqrt() * vector_norm.sqrt())
    };
    Ok((score.is_finite() && score > 0.0).then_some(score))
}

#[derive(Debug, Clone, Copy)]
struct BoundedHnswQuerySelfScore {
    dimension: usize,
    score: Option<f32>,
}

/// Request-local exact self scores for HNSW duplicate correction. The query
/// slice fixes the cache to one request; separate representation slots prevent
/// an F32 score or one quantizer's reconstruction from being reused by another.
pub(crate) struct BoundedHnswQuerySelfScores<'query> {
    query: &'query [f32],
    f32: Option<BoundedHnswQuerySelfScore>,
    sq8: Option<BoundedHnswQuerySelfScore>,
    sq4: Option<BoundedHnswQuerySelfScore>,
}

impl<'query> BoundedHnswQuerySelfScores<'query> {
    pub(crate) fn new(query: &'query [f32]) -> Self {
        Self {
            query,
            f32: None,
            sq8: None,
            sq4: None,
        }
    }

    fn score<E>(
        &mut self,
        query: &[f32],
        quantization: VectorQuantization,
        encoded: Option<&[u8]>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        if self.query.len() != query.len() || !std::ptr::eq(self.query.as_ptr(), query.as_ptr()) {
            return Err(E::from(Error::Other(
                "bounded HNSW query self-score cache belongs to another query".to_string(),
            )));
        }
        let cached = match quantization {
            VectorQuantization::F32 => self.f32,
            VectorQuantization::SQ8 => self.sq8,
            VectorQuantization::SQ4 => self.sq4,
        };
        if let Some(cached) = cached {
            if cached.dimension != query.len() {
                return Err(E::from(Error::Other(
                    "bounded HNSW query self-score cache dimension changed".to_string(),
                )));
            }
            return Ok(cached.score);
        }

        let score = bounded_exact_query_score(query, quantization, encoded, before_distance)?;
        let cached = Some(BoundedHnswQuerySelfScore {
            dimension: query.len(),
            score,
        });
        match quantization {
            VectorQuantization::F32 => self.f32 = cached,
            VectorQuantization::SQ8 => self.sq8 = cached,
            VectorQuantization::SQ4 => self.sq4 = cached,
        }
        Ok(score)
    }
}

/// Proof-only evidence emitted from the real HNSW distance-evaluation path.
///
/// The private field is intentional: code above the vector source cannot
/// manufacture an event after an eager `Hnsw::search` and pretend that it
/// stopped before candidate work.  Only this module can construct the event,
/// immediately before the candidate distance calculation below.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct HnswCandidateDistanceEvent {
    completed_candidates: u64,
    _source_loop_provenance: (),
}

#[cfg(feature = "test-seams")]
impl HnswCandidateDistanceEvent {
    pub const fn completed_candidates(&self) -> u64 {
        self.completed_candidates
    }
}

/// Behavior-neutral handoff for bounded-source poison and budget proofs.
/// Production builds have no observer branch.  Under `test-seams`, installing
/// no observer is also the ordinary path and produces byte-for-byte identical
/// distance results.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait HnswCandidateObserver: Send + Sync {
    fn before_candidate_distance(&self, event: HnswCandidateDistanceEvent);
    fn after_residual_rejection(&self, _event: HnswResidualRejectionEvent) {}
}

/// Emitted at the indexed union walk's residual rejection, before it can
/// examine a replacement neighbour. Only the source walk can construct it.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct HnswResidualRejectionEvent {
    row_id: RowId,
    _source_loop_provenance: (),
}

#[cfg(feature = "test-seams")]
impl HnswResidualRejectionEvent {
    pub const fn row_id(&self) -> RowId {
        self.row_id
    }
}

/// Proof-only evidence emitted where bounded HNSW computes the query's exact
/// self score for duplicate correction. The representation fields let tests
/// distinguish valid quantized reuse from an untagged scalar shortcut.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswQuerySelfScoreEvent {
    dimension: usize,
    quantization: VectorQuantization,
    _arithmetic_provenance: (),
}

#[cfg(feature = "test-seams")]
impl HnswQuerySelfScoreEvent {
    pub const fn dimension(&self) -> usize {
        self.dimension
    }

    pub const fn quantization(&self) -> VectorQuantization {
        self.quantization
    }
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait HnswQuerySelfScoreObserver: Send + Sync {
    fn after_query_self_score_charge(&self, event: HnswQuerySelfScoreEvent);
}

/// Proof-only evidence for what one visit to a paged graph point actually
/// pays besides its distance arithmetic: how often the serving page cache is
/// locked, how often a page body is faulted in from the snapshot source, how
/// often a stored vector is decoded, and how often the query's own norm is
/// recomputed. Production builds have no observer branch.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub trait PagedVectorObserver: Send + Sync {
    /// One acquisition of the serving page cache's lock.
    fn page_cache_locked(&self) {}
    /// One page body read from the snapshot source into a serving slot.
    fn page_filled(&self) {}
    /// One serving-slot lookup, and whether it found a resident page.
    fn page_looked_up(&self, _hit: bool) {}
    /// One stored vector decoded from page bytes for a distance evaluation.
    fn vector_decoded(&self) {}
    /// One computation of the query vector's own norm.
    fn query_norm_computed(&self) {}
    /// What one walk compared besides the vectors themselves, reported once
    /// when that walk ends.
    fn walk_compared(&self, _work: PagedWalkWork) {}
}

/// What a graph walk spends outside its distance arithmetic, counted as
/// ENTRIES SCANNED rather than as calls: the same walk that scores a few
/// thousand vectors can compare millions of list entries deciding which point
/// to reach next, and a per-evaluation cost that is two or three times an
/// exhaustive comparison is decided by these, not by the arithmetic.
///
/// Accumulated in local counters and reported once per walk, so counting it
/// costs the walk an integer addition and never an observer call.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PagedWalkWork {
    /// Entries scanned testing whether a point had already been reached.
    pub reached_entries: u64,
    /// Entries scanned choosing the next candidate to expand.
    pub frontier_entries: u64,
    /// Entries scanned keeping the result set at its declared breadth.
    pub result_entries: u64,
    /// Graph edges the walk considered.
    pub edges: u64,
    /// Points the walk scored against the query.
    pub evaluations: u64,
}

/// Install a paged-graph observer for the dynamic extent of one call on this
/// thread. Nested calls and unwinding restore the previous observer.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn with_paged_vector_observer<R>(
    observer: Arc<dyn PagedVectorObserver>,
    action: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<Arc<dyn PagedVectorObserver>>);

    impl Drop for Restore {
        fn drop(&mut self) {
            PAGED_VECTOR_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }

    let _restore = Restore(PAGED_VECTOR_OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}

#[cfg(any(test, feature = "test-seams"))]
fn observe_paged_vector(report: impl Fn(&dyn PagedVectorObserver)) {
    let observer = PAGED_VECTOR_OBSERVER.with(|slot| slot.borrow().as_ref().map(Arc::clone));
    if let Some(observer) = observer {
        report(observer.as_ref());
    }
}

#[cfg(any(test, feature = "test-seams"))]
std::thread_local! {
    static PAGED_VECTOR_OBSERVER: RefCell<Option<Arc<dyn PagedVectorObserver>>> =
        RefCell::new(None);
}

#[cfg(feature = "test-seams")]
std::thread_local! {
    static HNSW_CANDIDATE_OBSERVER: RefCell<Option<Arc<dyn HnswCandidateObserver>>> =
        RefCell::new(None);
    static HNSW_COMPLETED_CANDIDATES: Cell<u64> = const { Cell::new(0) };
    static HNSW_BUILD_OBSERVER: RefCell<Option<Arc<dyn HnswBuildObserver>>> = RefCell::new(None);
    static HNSW_QUERY_SELF_SCORE_OBSERVER: RefCell<Option<Arc<dyn HnswQuerySelfScoreObserver>>> =
        RefCell::new(None);
}

/// Proof-only observer for a fresh graph construction. Durable graph decoding
/// deliberately never emits this event.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait HnswBuildObserver: Send + Sync {
    fn before_hnsw_build(&self);
}

/// Install an observer for graph construction on the calling thread.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_hnsw_build_observer<R>(
    observer: Arc<dyn HnswBuildObserver>,
    action: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<Arc<dyn HnswBuildObserver>>);

    impl Drop for Restore {
        fn drop(&mut self) {
            HNSW_BUILD_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }

    let _restore = Restore(HNSW_BUILD_OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}

#[cfg(feature = "test-seams")]
fn observe_hnsw_build() {
    let observer = HNSW_BUILD_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.before_hnsw_build();
    }
}

/// Install an observer only for the dynamic extent of one bounded vector
/// source call.  Nested calls and unwinding restore the prior observer and
/// ordinal, so a test request cannot leak state into a later ordinary search.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_hnsw_candidate_observer<R>(
    observer: Arc<dyn HnswCandidateObserver>,
    search: impl FnOnce() -> R,
) -> R {
    struct ObserverGuard {
        prior_observer: Option<Arc<dyn HnswCandidateObserver>>,
        prior_completed: u64,
    }

    impl Drop for ObserverGuard {
        fn drop(&mut self) {
            HNSW_CANDIDATE_OBSERVER.with(|slot| {
                slot.replace(self.prior_observer.take());
            });
            HNSW_COMPLETED_CANDIDATES.with(|completed| {
                completed.set(self.prior_completed);
            });
        }
    }

    let prior_observer = HNSW_CANDIDATE_OBSERVER.with(|slot| slot.replace(Some(observer)));
    let prior_completed = HNSW_COMPLETED_CANDIDATES.with(|completed| completed.replace(0));
    let _guard = ObserverGuard {
        prior_observer,
        prior_completed,
    };
    search()
}

/// Install an observer for actual bounded query self-score computations on
/// this thread. Nested calls and unwinding restore the previous observer.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_hnsw_query_self_score_observer<R>(
    observer: Arc<dyn HnswQuerySelfScoreObserver>,
    search: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<Arc<dyn HnswQuerySelfScoreObserver>>);

    impl Drop for Restore {
        fn drop(&mut self) {
            HNSW_QUERY_SELF_SCORE_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }

    let _restore =
        Restore(HNSW_QUERY_SELF_SCORE_OBSERVER.with(|slot| slot.replace(Some(observer))));
    search()
}

#[cfg(feature = "test-seams")]
fn observe_hnsw_query_self_score(dimension: usize, quantization: VectorQuantization) {
    let observer =
        HNSW_QUERY_SELF_SCORE_OBSERVER.with(|slot| slot.borrow().as_ref().map(Arc::clone));
    if let Some(observer) = observer {
        observer.after_query_self_score_charge(HnswQuerySelfScoreEvent {
            dimension,
            quantization,
            _arithmetic_provenance: (),
        });
    }
}

#[cfg(feature = "test-seams")]
fn observe_hnsw_candidate_distance() {
    let observer = HNSW_CANDIDATE_OBSERVER.with(|slot| slot.borrow().as_ref().map(Arc::clone));
    let Some(observer) = observer else {
        return;
    };
    let completed_candidates = HNSW_COMPLETED_CANDIDATES.with(|completed| {
        let current = completed.get();
        completed.set(current.saturating_add(1));
        current
    });
    observer.before_candidate_distance(HnswCandidateDistanceEvent {
        completed_candidates,
        _source_loop_provenance: (),
    });
}

#[cfg(feature = "test-seams")]
pub(super) fn observe_hnsw_residual_rejection(row_id: RowId) {
    let observer = HNSW_CANDIDATE_OBSERVER.with(|slot| slot.borrow().as_ref().map(Arc::clone));
    if let Some(observer) = observer {
        observer.after_residual_rejection(HnswResidualRejectionEvent {
            row_id,
            _source_loop_provenance: (),
        });
    }
}

#[derive(Debug, Clone, Copy)]
struct DistQuantizedCosine {
    quantization: VectorQuantization,
}

impl Distance<u8> for DistQuantizedCosine {
    fn eval(&self, va: &[u8], vb: &[u8]) -> f32 {
        #[cfg(feature = "test-seams")]
        observe_hnsw_candidate_distance();
        quantized_hnsw_distance(va, vb, self.quantization)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswGraphStats {
    pub point_count: usize,
    pub layer0_points: usize,
    pub layer0_neighbor_edges: usize,
    pub max_level_observed: u8,
    pub dimension: usize,
}

impl HnswIndex {
    /// Build a fresh mutable graph with the caller's resolved column policy.
    /// Restart uses this for the journal suffix after a sealed generation.
    #[doc(hidden)]
    pub fn from_vector_entries_with_policy(
        entries: &[VectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
    ) -> Self {
        let stored = entries
            .iter()
            .map(|entry| StoredVectorEntry::from_vector_entry_ref(entry, quantization))
            .collect::<Vec<_>>();
        Self::new_with_policy(&stored, dimension, quantization, policy)
    }

    #[doc(hidden)]
    pub fn from_vector_entries_with_progress(
        entries: &[VectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
        mut progress: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        progress(0)?;
        let stored = entries
            .iter()
            .map(|entry| StoredVectorEntry::from_vector_entry_ref(entry, quantization))
            .collect::<Vec<_>>();
        Self::new_with_policy_and_progress(&stored, dimension, quantization, policy, progress)
    }

    #[cfg(test)]
    pub(crate) fn new(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Self {
        let policy = crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
            .resolve_policy(entries.len(), 1);
        Self::new_with_policy(entries, dimension, quantization, policy)
    }

    pub(crate) fn new_with_policy(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
    ) -> Self {
        Self::new_with_policy_and_progress(entries, dimension, quantization, policy, |_| Ok(()))
            .expect("infallible graph progress callback")
    }

    pub(crate) fn new_with_policy_and_progress(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
        mut progress: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        progress(0)?;
        let mut sorted_entries = entries
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .collect::<Vec<_>>();
        sorted_entries.sort_by_key(|entry| {
            (
                entry.lsn,
                entry.created_tx,
                insertion_key(entry),
                entry.row_id,
            )
        });
        #[cfg(feature = "test-seams")]
        if !sorted_entries.is_empty() {
            observe_hnsw_build();
        }

        let m = policy.hnsw_m;
        let ef_construction = policy.hnsw_ef_construction;
        let ef_search = policy.hnsw_ef_search;
        let max_elements = sorted_entries.len().max(1);
        let hnsw = match quantization {
            VectorQuantization::F32 => {
                let mut hnsw = Hnsw::new_with_seed(
                    m,
                    max_elements,
                    16,
                    ef_construction,
                    hnsw_f32_distance(),
                    HNSW_BUILD_SEED,
                );
                hnsw.set_extend_candidates(F32_HNSW_EXTEND_CANDIDATES);
                hnsw.set_keeping_pruned(F32_HNSW_KEEPING_PRUNED);
                HnswInner::F32Mutable(hnsw)
            }
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                let mut hnsw = Hnsw::new_with_seed(
                    m,
                    max_elements,
                    16,
                    ef_construction,
                    DistQuantizedCosine { quantization },
                    HNSW_BUILD_SEED,
                );
                hnsw.set_extend_candidates(true);
                hnsw.set_keeping_pruned(true);
                HnswInner::QuantizedMutable(hnsw)
            }
        };
        let mut id_to_row = HashMap::with_capacity(sorted_entries.len());
        let mut row_to_point = BTreeMap::new();
        let mut exact_rows = HashMap::<Vec<u8>, Vec<RowId>>::with_capacity(sorted_entries.len());
        let mut inserted_count = 0usize;

        match &hnsw {
            HnswInner::F32Mutable(index) => {
                for entry in &sorted_entries {
                    let Some(vector) = entry.vector.as_f32_slice() else {
                        continue;
                    };
                    let data_id = inserted_count;
                    inserted_count += 1;
                    id_to_row.insert(data_id, entry.row_id);
                    if let Some(key) = exact_key_for_stored_vector(&entry.vector) {
                        exact_rows.entry(key).or_default().push(entry.row_id);
                    }
                    row_to_point.insert(
                        entry.row_id,
                        index.insert_slice_with_point_id((vector, data_id)),
                    );
                    progress(inserted_count)?;
                }
            }
            HnswInner::QuantizedMutable(index) => {
                for entry in &sorted_entries {
                    let encoded = entry.vector.to_hnsw_u8();
                    if encoded.is_empty() {
                        continue;
                    }
                    let data_id = inserted_count;
                    inserted_count += 1;
                    id_to_row.insert(data_id, entry.row_id);
                    if let Some(key) = exact_key_for_stored_vector(&entry.vector) {
                        exact_rows.entry(key).or_default().push(entry.row_id);
                    }
                    row_to_point.insert(
                        entry.row_id,
                        index.insert_slice_with_point_id((encoded.as_slice(), data_id)),
                    );
                    progress(inserted_count)?;
                }
            }
            HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_) | HnswInner::F32Paged(_) => {
                unreachable!("a freshly constructed HNSW graph is always mutable")
            }
        }

        for row_ids in exact_rows.values_mut() {
            row_ids.sort_unstable();
            row_ids.dedup();
        }

        Ok(Self {
            hnsw,
            id_to_row: RwLock::new(id_to_row),
            row_to_point: RwLock::new(row_to_point),
            exact_rows: RwLock::new(exact_rows),
            next_id: AtomicUsize::new(inserted_count),
            build_serial: NEXT_HNSW_BUILD_SERIAL.fetch_add(1, Ordering::SeqCst) as u64,
            dimension,
            quantization,
            m,
            ef_construction,
            ef_search,
        })
    }

    /// Validate and encode an insertion without changing the fresh graph.
    /// The caller admits and retains this work until transaction publication.
    pub(crate) fn prepare_insert(
        &self,
        entry: &StoredVectorEntry,
        additional_inserts: usize,
    ) -> Result<PreparedHnswInsertion> {
        if entry.deleted_tx.is_some() {
            return Err(Error::Other(
                "cannot insert a deleted vector into a mutable HNSW graph".to_string(),
            ));
        }
        if entry.vector.len() != self.dimension {
            return Err(Error::Other(format!(
                "cannot insert vector dimension {} into HNSW dimension {}",
                entry.vector.len(),
                self.dimension
            )));
        }
        let encoded = match (&entry.vector, self.quantization) {
            (StoredVector::F32(_), VectorQuantization::F32) => None,
            (StoredVector::SQ8 { .. }, VectorQuantization::SQ8)
            | (StoredVector::SQ4 { .. }, VectorQuantization::SQ4) => {
                let encoded = entry.vector.to_hnsw_u8();
                if encoded.is_empty() {
                    return Err(Error::Other(
                        "cannot insert an empty quantized vector into HNSW".to_string(),
                    ));
                }
                Some(encoded)
            }
            _ => {
                return Err(Error::Other(
                    "stored vector quantization does not match its HNSW graph".to_string(),
                ));
            }
        };
        let exact_key = exact_key_for_stored_vector(&entry.vector);

        self.next_id
            .load(Ordering::Acquire)
            .checked_add(additional_inserts)
            .ok_or_else(|| Error::Other("HNSW data-id space exhausted".to_string()))?;
        if matches!(
            &self.hnsw,
            HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_)
        ) {
            return Err(Error::Other(
                "cannot insert into a sealed durable HNSW graph".to_string(),
            ));
        }
        Ok(PreparedHnswInsertion {
            build_serial: self.build_serial,
            entry: entry.clone(),
            encoded,
            exact_key,
        })
    }

    pub(crate) fn publish_insert(&self, prepared: PreparedHnswInsertion) {
        let PreparedHnswInsertion {
            build_serial,
            entry,
            encoded,
            exact_key,
        } = prepared;
        assert_eq!(
            self.build_serial, build_serial,
            "prepared insertion belongs to this fresh graph"
        );
        // The mapping lock serializes id assignment and stays held through the
        // vendor insertion. A concurrent search that sees the new graph point
        // therefore cannot observe it without its external row mapping.
        let mut id_to_row = self.id_to_row.write();
        let mut exact_rows = self.exact_rows.write();
        let data_id = self.next_id.load(Ordering::Relaxed);
        let next_id = data_id + 1;

        match (&self.hnsw, &entry.vector, encoded.as_deref()) {
            (HnswInner::F32Mutable(index), StoredVector::F32(vector), None) => {
                self.row_to_point.write().insert(
                    entry.row_id,
                    index.insert_slice_with_point_id((vector, data_id)),
                );
            }
            (HnswInner::QuantizedMutable(index), _, Some(vector)) => {
                self.row_to_point.write().insert(
                    entry.row_id,
                    index.insert_slice_with_point_id((vector, data_id)),
                );
            }
            _ => unreachable!("prepared HNSW insertion retains its validated representation"),
        }

        id_to_row.insert(data_id, entry.row_id);
        if let Some(key) = exact_key {
            let rows = exact_rows.entry(key).or_default();
            if let Err(position) = rows.binary_search(&entry.row_id) {
                rows.insert(position, entry.row_id);
            }
        }
        self.next_id.store(next_id, Ordering::Release);
    }

    #[allow(dead_code)]
    pub(crate) fn insert(&self, entry: &StoredVectorEntry) -> Result<()> {
        let prepared = self.prepare_insert(entry, 1)?;
        self.publish_insert(prepared);
        Ok(())
    }

    /// Checked upper bound for the one durable codec envelope. The vendor
    /// codec uses fixed-width integers, sixteen layer counts, at most 2*M
    /// bottom links and M links on each of the other fifteen layers. Its
    /// neighbour record is 8 + 1 + 4 + 4 bytes. Exact keys use the stored representation.
    /// This bounds encoded bytes, not construction scratch or resident maps.
    #[doc(hidden)]
    pub fn durable_generation_capacity_bound(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
    ) -> Result<usize> {
        let overflow =
            || Error::Other("durable HNSW codec capacity exceeds the process range".into());
        let values = match quantization {
            VectorQuantization::F32 => dimension.checked_mul(4),
            VectorQuantization::SQ8 => dimension.checked_add(12),
            VectorQuantization::SQ4 => dimension.div_ceil(2).checked_add(12),
        }
        .ok_or_else(overflow)?;
        let links = m
            .checked_mul(17)
            .and_then(|n| n.checked_mul(17))
            .ok_or_else(overflow)?;
        // origin, point id, value length, neighbour layer length and counts,
        // row map entry; plus exact-map key length, list length and one row.
        let point = 8usize + 5 + 8 + 8 + 16 * 8 + 16;
        let exact = values.checked_add(24).ok_or_else(overflow)?;
        // Envelope header + graph definition + layer sizes + optional entry
        // point + row/exact-map counts. 512 exceeds their fixed 259 bytes.
        point
            .checked_add(values)
            .and_then(|n| n.checked_add(links))
            .and_then(|n| n.checked_add(exact))
            .and_then(|n| n.checked_mul(count))
            .and_then(|n| n.checked_add(512))
            .ok_or_else(overflow)
    }

    /// Refine the codec bound using the completed builder's actual edges and
    /// exact-key lists, without materializing a generation-sized payload.
    fn durable_generation_encoding_capacity(&self) -> Result<usize> {
        let overflow =
            || Error::Other("durable HNSW codec capacity exceeds the process range".into());
        let values = match self.quantization {
            VectorQuantization::F32 => self.dimension.checked_mul(4),
            VectorQuantization::SQ8 => self.dimension.checked_add(12),
            VectorQuantization::SQ4 => self.dimension.div_ceil(2).checked_add(12),
        }
        .ok_or_else(overflow)?;
        let mut capacity = 512usize;
        let mut admit_point = |neighbours: Vec<Vec<Neighbour>>| -> Result<()> {
            let links = neighbours
                .iter()
                .try_fold(0usize, |sum, layer| sum.checked_add(layer.len()))
                .ok_or_else(overflow)?;
            let point = (8usize + 5 + 8 + 8 + 16 * 8 + 16)
                .checked_add(values)
                .and_then(|n| links.checked_mul(17).and_then(|links| n.checked_add(links)))
                .ok_or_else(overflow)?;
            capacity = capacity.checked_add(point).ok_or_else(overflow)?;
            Ok(())
        };
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => {
                for point in graph.get_point_indexation() {
                    admit_point(point.get_neighborhood_id())?;
                }
            }
            HnswInner::QuantizedMutable(graph) => {
                for point in graph.get_point_indexation() {
                    admit_point(point.get_neighborhood_id())?;
                }
            }
            _ => {
                return Err(Error::Other(
                    "a sealed durable HNSW graph cannot be re-encoded".into(),
                ));
            }
        }
        for (key, rows) in self.exact_rows.read().iter() {
            capacity = rows
                .len()
                .checked_mul(8)
                .and_then(|n| n.checked_add(key.len()))
                .and_then(|n| n.checked_add(16))
                .and_then(|n| n.checked_add(capacity))
                .ok_or_else(overflow)?;
        }
        Ok(capacity)
    }

    /// Produce one ContextDB durable envelope for a graph built in this
    /// process.  It includes the vendor topology and point values together
    /// with the row and exact-vector maps, so loading never scans the vector
    /// store or rebuilds a graph.
    #[doc(hidden)]
    pub fn encode_durable_generation(&self) -> Result<Vec<u8>> {
        // One buffer owns the envelope, graph and maps. Backfill the length
        // and checksum after appending their bytes; never retain separate
        // graph, payload and envelope copies at the same time.
        let capacity = self.durable_generation_encoding_capacity()?;
        let bound = Self::durable_generation_capacity_bound(
            self.len(),
            self.dimension,
            self.quantization,
            self.m,
        )?;
        if capacity > bound {
            return Err(Error::Other(
                "durable HNSW codec exceeds its admitted capacity bound".into(),
            ));
        }
        let mut envelope = Vec::with_capacity(capacity);
        envelope.extend_from_slice(&DURABLE_ENVELOPE_MAGIC);
        envelope.extend_from_slice(&DURABLE_ENVELOPE_VERSION.to_le_bytes());
        let checksum_offset = envelope.len();
        write_u64(&mut envelope, 0);
        let payload_offset = envelope.len();
        write_u64(&mut envelope, self.dimension as u64);
        write_u8(&mut envelope, quantization_tag(self.quantization));
        write_u64(&mut envelope, self.ef_search as u64);
        let graph_length_offset = envelope.len();
        write_u64(&mut envelope, 0);
        let graph_offset = envelope.len();
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => encode_owned_graph_into(graph, &mut envelope),
            HnswInner::QuantizedMutable(graph) => encode_owned_graph_into(graph, &mut envelope),
            HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_) | HnswInner::F32Paged(_) => {
                return Err(Error::Other(
                    "a sealed durable HNSW graph is already a generation and cannot be re-encoded"
                        .to_string(),
                ));
            }
        }
        .map_err(|error| Error::Other(format!("could not encode durable HNSW graph: {error}")))?;
        let graph_length = u64::try_from(envelope.len() - graph_offset)
            .map_err(|_| Error::Other("durable HNSW byte length exceeds u64".to_string()))?;
        envelope[graph_length_offset..graph_offset].copy_from_slice(&graph_length.to_le_bytes());
        let id_to_row = self.id_to_row.read();
        let mut rows = id_to_row
            .iter()
            .map(|(data_id, row_id)| (*data_id, row_id.0))
            .collect::<Vec<_>>();
        rows.sort_unstable_by_key(|(data_id, _)| *data_id);
        let exact_rows = self.exact_rows.read();
        // Construction and insertion keep each row list sorted. Borrow the
        // keys and lists while ordering map entries for deterministic bytes.
        let mut exact = exact_rows.iter().collect::<Vec<_>>();
        exact.sort_unstable_by(|left, right| left.0.cmp(right.0));

        write_u64(&mut envelope, rows.len() as u64);
        for (data_id, row_id) in rows {
            write_u64(&mut envelope, data_id as u64);
            write_u64(&mut envelope, row_id);
        }
        write_u64(&mut envelope, exact.len() as u64);
        for (key, row_ids) in exact {
            write_bytes(&mut envelope, key)?;
            write_u64(&mut envelope, row_ids.len() as u64);
            for row_id in row_ids {
                write_u64(&mut envelope, row_id.0);
            }
        }
        let checksum = durable_checksum(&envelope[payload_offset..]);
        envelope[checksum_offset..payload_offset].copy_from_slice(&checksum.to_le_bytes());
        Ok(envelope)
    }

    /// Load and seal one ContextDB durable graph generation.  A caller must
    /// provide the column definition it is loading for; incompatible
    /// dimension or quantization is refused before a graph is exposed.
    #[doc(hidden)]
    pub fn decode_durable_generation(
        bytes: &[u8],
        expected_dimension: usize,
        expected_quantization: VectorQuantization,
    ) -> Result<Self> {
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(DURABLE_ENVELOPE_MAGIC.len())? != DURABLE_ENVELOPE_MAGIC.as_slice() {
            return Err(Error::Other(
                "invalid durable HNSW envelope magic".to_string(),
            ));
        }
        if reader.read_u16()? != DURABLE_ENVELOPE_VERSION {
            return Err(Error::Other(
                "unsupported durable HNSW envelope version".to_string(),
            ));
        }
        let expected_checksum = reader.read_u64()?;
        let payload = reader.remaining();
        if read_checksum(payload, "graph_decode_hashed_bytes") != expected_checksum {
            return Err(Error::Other(
                "durable HNSW envelope checksum mismatch".to_string(),
            ));
        }
        let mut payload = DurableReader::new(payload);
        let dimension = payload.read_usize()?;
        if dimension != expected_dimension {
            return Err(Error::Other(format!(
                "durable HNSW dimension {dimension} does not match expected dimension {expected_dimension}"
            )));
        }
        let quantization = quantization_from_tag(payload.read_u8()?)?;
        if quantization != expected_quantization {
            return Err(Error::Other(
                "durable HNSW quantization does not match the vector column".to_string(),
            ));
        }
        let ef_search = payload.read_usize()?;
        if ef_search == 0 {
            return Err(Error::Other(
                "durable HNSW has an invalid search breadth".to_string(),
            ));
        }
        let graph_bytes = payload.read_bytes()?;
        let row_count = payload.read_usize()?;
        if row_count > payload.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW row mapping length is invalid".to_string(),
            ));
        }
        let mut id_to_row = HashMap::with_capacity(row_count);
        let mut previous_data_id = None;
        for _ in 0..row_count {
            let data_id = payload.read_usize()?;
            if previous_data_id.is_some_and(|previous| data_id <= previous)
                || id_to_row
                    .insert(data_id, RowId(payload.read_u64()?))
                    .is_some()
            {
                return Err(Error::Other(
                    "durable HNSW has invalid row mappings".to_string(),
                ));
            }
            previous_data_id = Some(data_id);
        }
        let exact_count = payload.read_usize()?;
        if exact_count > payload.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW exact mapping length is invalid".to_string(),
            ));
        }
        let mut exact_rows = HashMap::with_capacity(exact_count);
        let mut previous_key: Option<Vec<u8>> = None;
        for _ in 0..exact_count {
            let key = payload.read_bytes()?.to_vec();
            if key.is_empty()
                || previous_key
                    .as_ref()
                    .is_some_and(|previous| key <= *previous)
            {
                return Err(Error::Other(
                    "durable HNSW has invalid exact-vector mappings".to_string(),
                ));
            }
            let count = payload.read_usize()?;
            if count > payload.remaining().len() / 8 {
                return Err(Error::Other(
                    "durable HNSW exact row length is invalid".to_string(),
                ));
            }
            let mut rows = Vec::with_capacity(count);
            let mut previous_row = None;
            for _ in 0..count {
                let row = RowId(payload.read_u64()?);
                if previous_row.is_some_and(|previous| row <= previous) {
                    return Err(Error::Other(
                        "durable HNSW exact-vector rows are not ordered".to_string(),
                    ));
                }
                previous_row = Some(row);
                rows.push(row);
            }
            if rows.is_empty() || exact_rows.insert(key.clone(), rows).is_some() {
                return Err(Error::Other(
                    "durable HNSW has duplicate exact-vector mappings".to_string(),
                ));
            }
            previous_key = Some(key);
        }
        if !payload.is_empty() {
            return Err(Error::Other(
                "durable HNSW envelope has trailing payload bytes".to_string(),
            ));
        }
        let hnsw = match quantization {
            VectorQuantization::F32 => HnswInner::F32Sealed(
                decode_owned_graph(graph_bytes, hnsw_f32_distance()).map_err(|error| {
                    Error::Other(format!("could not load durable F32 HNSW graph: {error}"))
                })?,
            ),
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => HnswInner::QuantizedSealed(
                decode_owned_graph(graph_bytes, DistQuantizedCosine { quantization }).map_err(
                    |error| {
                        Error::Other(format!(
                            "could not load durable quantized HNSW graph: {error}"
                        ))
                    },
                )?,
            ),
        };
        let graph_points = match &hnsw {
            HnswInner::F32Sealed(graph) => graph.get_nb_point(),
            HnswInner::QuantizedSealed(graph) => graph.get_nb_point(),
            HnswInner::F32Mutable(_) | HnswInner::QuantizedMutable(_) | HnswInner::F32Paged(_) => {
                unreachable!()
            }
        };
        if graph_points != id_to_row.len() {
            return Err(Error::Other(
                "durable HNSW graph point count does not match row mappings".to_string(),
            ));
        }
        let mapped_points_match = match &hnsw {
            HnswInner::F32Sealed(graph) => graph
                .get_point_indexation()
                .into_iter()
                .all(|point| id_to_row.contains_key(&point.get_origin_id())),
            HnswInner::QuantizedSealed(graph) => graph
                .get_point_indexation()
                .into_iter()
                .all(|point| id_to_row.contains_key(&point.get_origin_id())),
            HnswInner::F32Mutable(_) | HnswInner::QuantizedMutable(_) | HnswInner::F32Paged(_) => {
                unreachable!()
            }
        };
        if !mapped_points_match {
            return Err(Error::Other(
                "durable HNSW graph points do not match row mappings".to_string(),
            ));
        }
        let mut row_to_point = BTreeMap::new();
        match &hnsw {
            HnswInner::F32Sealed(graph) => {
                for point in graph.get_point_indexation() {
                    row_to_point.insert(id_to_row[&point.get_origin_id()], point.get_point_id());
                }
            }
            HnswInner::QuantizedSealed(graph) => {
                for point in graph.get_point_indexation() {
                    row_to_point.insert(id_to_row[&point.get_origin_id()], point.get_point_id());
                }
            }
            _ => unreachable!(),
        }
        let (m, ef_construction) = match &hnsw {
            HnswInner::F32Sealed(graph) => {
                (graph.get_max_nb_connection(), graph.get_ef_construction())
            }
            HnswInner::QuantizedSealed(graph) => {
                (graph.get_max_nb_connection(), graph.get_ef_construction())
            }
            _ => unreachable!(),
        };
        Ok(Self {
            hnsw,
            id_to_row: RwLock::new(id_to_row),
            row_to_point: RwLock::new(row_to_point),
            exact_rows: RwLock::new(exact_rows),
            next_id: AtomicUsize::new(graph_points),
            build_serial: NEXT_HNSW_BUILD_SERIAL.fetch_add(1, Ordering::SeqCst) as u64,
            dimension,
            quantization,
            m,
            ef_construction,
            ef_search,
        })
    }

    /// Read the allocation bound from the existing envelope without decoding
    /// or retaining points. Old catalogs may contain an earlier charge formula.
    #[doc(hidden)]
    pub fn durable_resident_bound(
        bytes: &[u8],
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
    ) -> Result<usize> {
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(8)? != DURABLE_ENVELOPE_MAGIC
            || reader.read_u16()? != DURABLE_ENVELOPE_VERSION
        {
            return Err(Error::Other("invalid durable HNSW envelope".to_string()));
        }
        let checksum = reader.read_u64()?;
        if read_checksum(reader.remaining(), "graph_sizing_hashed_bytes") != checksum {
            return Err(Error::Other(
                "durable HNSW envelope checksum mismatch".to_string(),
            ));
        }
        if reader.read_usize()? != dimension
            || quantization_from_tag(reader.read_u8()?)? != quantization
        {
            return Err(Error::Other(
                "durable HNSW declaration mismatch".to_string(),
            ));
        }
        reader.read_usize()?;
        let graph = reader.read_bytes()?;
        // The pinned owned-graph codec starts with its u16 version then its
        // fixed-width usize connection limit. Check it before graph allocation.
        let mut topology = DurableReader::new(graph);
        topology.read_u16()?;
        if topology.read_usize()? != m {
            return Err(Error::Other(
                "durable HNSW topology disagrees with its catalog".to_string(),
            ));
        }
        let count = reader.read_usize()?;
        if count > reader.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW row mapping length is invalid".to_string(),
            ));
        }
        Ok(Self::estimated_resident_bytes_with_m(
            count,
            dimension,
            quantization,
            m,
        ))
    }

    /// Conservative ownership bound for a retained, immutable generation.
    ///
    /// Read from the saved generation itself before anything is decoded: each
    /// point's containers and each neighbour list at its saved length -- so a
    /// larger declared degree is charged for the longer lists it saved -- the
    /// row mappings, the serving cache and its slot directory, and a fixed
    /// allowance for the layer headers and small state around them.
    #[doc(hidden)]
    pub fn durable_reader_resident_bound(
        bytes: &[u8],
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
    ) -> Result<usize> {
        Self::durable_reader_resident_bound_for_cache(
            bytes,
            dimension,
            quantization,
            m,
            PAGED_VECTOR_MIN_CACHE_BYTES
                .max(bytes.len().next_multiple_of(PAGED_VECTOR_SOURCE_PAGE_BYTES)),
        )
    }

    #[doc(hidden)]
    /// An estimate, from the catalog alone, of what a committed-image
    /// generation holds once it is paged in with its least serving cache,
    /// plus the workspace its load is admitted with.
    ///
    /// It is the part of that footprint the catalog can know: each point's
    /// record, its layer headers and its row mappings, one serving slot and
    /// the fixed allowances -- never the vectors, which a paged generation
    /// does not hold. The neighbour lists are left out: how many a point keeps
    /// depends on the data (about eight per point on small low-dimensional
    /// fixtures, about fifty on 768-dimension passages at the same declared
    /// degree) and cannot be counted before the saved graph is read. So this
    /// is a floor, never an over-reservation: the load itself is admitted at
    /// what it reads from the saved topology
    /// (`durable_reader_resident_bound_for_cache`), and when that is more than
    /// was kept for it, resident serving pages give way to it. `m` is kept for
    /// the day the catalog records a graph's degree actually used.
    /// The most points a paged saved graph of `durable_bytes` bytes can hold.
    /// Every point of a paged graph stores its full-precision vector, so no
    /// layer holds more points than its bytes have room for vectors: a change
    /// layer sealed over a few new rows is priced at about those rows, not at
    /// every row of its partition.
    pub fn paged_reader_points_bound(durable_bytes: u64, dimension: usize) -> usize {
        let per_point = dimension.saturating_mul(std::mem::size_of::<f32>()).max(1) as u64;
        usize::try_from(durable_bytes / per_point).unwrap_or(usize::MAX)
    }

    pub fn paged_reader_resident_bound(live_rows: usize, m: usize) -> usize {
        let _declared_degree = m;
        let per_point = std::mem::size_of::<PagedPoint>()
            .saturating_add(16 * std::mem::size_of::<Vec<PagedNeighbour>>())
            .saturating_add(DURABLE_READER_ROW_MAPPING_BYTES);
        std::mem::size_of::<Self>()
            .saturating_add(96 * 1024)
            .saturating_add(PAGED_VECTOR_MIN_CACHE_BYTES)
            .saturating_add(serving_slot_directory_bytes(1))
            .saturating_add(DURABLE_READER_LOAD_WORKSPACE_BYTES)
            .saturating_add(live_rows.saturating_mul(per_point))
    }

    pub fn durable_reader_cache_bounds(ceiling: usize) -> (usize, usize) {
        (
            PAGED_VECTOR_MIN_CACHE_BYTES,
            ceiling.max(PAGED_VECTOR_MIN_CACHE_BYTES),
        )
    }

    /// Extra directory bytes above the one-page cache when reserving slots
    /// for `cache_bytes`. The loader reserves this along with page bodies;
    /// otherwise the last page can fit while its lookup slot cannot.
    pub fn durable_reader_cache_directory_growth(graph_bytes: usize, cache_bytes: usize) -> usize {
        let slots = cache_bytes
            .min(graph_bytes)
            .div_ceil(PAGED_VECTOR_SOURCE_PAGE_BYTES);
        let minimum_slots = PAGED_VECTOR_MIN_CACHE_BYTES
            .min(graph_bytes)
            .div_ceil(PAGED_VECTOR_SOURCE_PAGE_BYTES);
        serving_slot_directory_bytes(slots)
            .saturating_sub(serving_slot_directory_bytes(minimum_slots))
    }

    #[doc(hidden)]
    pub fn durable_reader_resident_bound_for_cache(
        bytes: &[u8],
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
        cache_bytes: usize,
    ) -> Result<usize> {
        if cache_bytes < PAGED_VECTOR_MIN_CACHE_BYTES
            || !cache_bytes.is_multiple_of(PAGED_VECTOR_SOURCE_PAGE_BYTES)
        {
            return Err(Error::Other(
                "durable HNSW reader cache size is outside its supported bounds".to_string(),
            ));
        }
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(8)? != DURABLE_ENVELOPE_MAGIC
            || reader.read_u16()? != DURABLE_ENVELOPE_VERSION
        {
            return Err(Error::Other("invalid durable HNSW envelope".to_string()));
        }
        // Sizing reads lengths and hashes nothing. The bytes it sizes are
        // checked once, by the decode that follows -- or, for a saved graph,
        // by the digest its record was read under -- and every length read
        // here is bounds-checked, so damaged bytes can make this bound wrong
        // but never make it read outside the envelope.
        let _checksum = reader.read_u64()?;
        if reader.read_usize()? != dimension
            || quantization_from_tag(reader.read_u8()?)? != quantization
        {
            return Err(Error::Other(
                "durable HNSW declaration mismatch".to_string(),
            ));
        }
        if reader.read_usize()? == 0 {
            return Err(Error::Other(
                "durable HNSW has an invalid search breadth".to_string(),
            ));
        }
        let graph = reader.read_bytes()?;
        let mut topology = DurableReader::new(graph);
        if topology.read_u16()? != 1 {
            return Err(Error::Other(
                "unsupported ContextDB HNSW graph format version".to_string(),
            ));
        }
        if topology.read_usize()? != m {
            return Err(Error::Other(
                "durable HNSW topology disagrees with its catalog".to_string(),
            ));
        }
        let points_resident = Self::durable_reader_points_resident(&mut topology, m)?;
        let count = reader.read_usize()?;
        if count > reader.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW row mapping length is invalid".to_string(),
            ));
        }
        let containers = count
            .checked_mul(DURABLE_READER_ROW_MAPPING_BYTES)
            .and_then(|mappings| mappings.checked_add(points_resident))
            .ok_or_else(|| Error::Other("durable HNSW container residency overflow".to_string()))?;
        // The serving cache and the directory that finds its pages, sized the
        // way the decode sizes them for this cache and this generation.
        let page_slots = cache_bytes
            .min(bytes.len())
            .div_ceil(PAGED_VECTOR_SOURCE_PAGE_BYTES);
        // The scalar graph header is already part of Self.
        std::mem::size_of::<Self>()
            .checked_add(96 * 1024)
            .and_then(|bound| bound.checked_add(cache_bytes))
            .and_then(|bound| bound.checked_add(serving_slot_directory_bytes(page_slots)))
            .and_then(|bound| bound.checked_add(containers))
            .ok_or_else(|| Error::Other("durable HNSW reader residency overflow".to_string()))
    }

    /// What a paged decode will hold for this generation's points, read from
    /// the saved topology before anything is allocated: each point's record
    /// and its sixteen layer headers, and each neighbour list at the length it
    /// was saved with. A list longer than twice the declared degree is refused
    /// here exactly as the decode refuses it, so the declared degree bounds
    /// every list, and a graph that uses fewer links than its degree allows is
    /// charged only for the links it has.
    fn durable_reader_points_resident(topology: &mut DurableReader<'_>, m: usize) -> Result<usize> {
        let overflow = || Error::Other("durable HNSW point residency overflow".to_string());
        let _ef_construction = topology.read_usize()?;
        let _extend_candidates = topology.read_u8()?;
        let _keep_pruned = topology.read_u8()?;
        let _max_layer = topology.read_usize()?;
        let dimension = topology.read_usize()?;
        let _scale = topology.read_f64()?;
        let layer_count = topology.read_usize()?;
        if layer_count > 16 {
            return Err(Error::Other("durable HNSW topology is invalid".to_string()));
        }
        let longest_list = m.checked_mul(2).ok_or_else(overflow)?;
        let mut total = layer_count
            .checked_mul(std::mem::size_of::<Vec<PagedPoint>>())
            .ok_or_else(overflow)?;
        for _ in 0..layer_count {
            let count = topology.read_usize()?;
            for _ in 0..count {
                let _origin_id = topology.read_usize()?;
                let _point_id = topology.read_point_id()?;
                let _dimension = topology.read_usize()?;
                topology.skip(dimension.checked_mul(4).ok_or_else(overflow)?)?;
                let neighbour_layers = topology.read_usize()?;
                total = neighbour_layers
                    .checked_mul(std::mem::size_of::<Vec<PagedNeighbour>>())
                    .and_then(|headers| headers.checked_add(std::mem::size_of::<PagedPoint>()))
                    .and_then(|point| total.checked_add(point))
                    .ok_or_else(overflow)?;
                for _ in 0..neighbour_layers {
                    let neighbours = topology.read_usize()?;
                    if neighbours > longest_list {
                        return Err(Error::Other(
                            "durable HNSW graph has too many neighbours".to_string(),
                        ));
                    }
                    total = neighbours
                        .checked_mul(std::mem::size_of::<PagedNeighbour>())
                        .and_then(|list| total.checked_add(list))
                        .ok_or_else(overflow)?;
                    // Each saved edge is its origin, its point id and its
                    // distance: eight, five and four bytes.
                    topology.skip(neighbours.checked_mul(17).ok_or_else(overflow)?)?;
                }
            }
        }
        Ok(total)
    }

    /// Decode immutable topology while retaining point values in the saved
    /// generation. `source` addresses byte ranges within this envelope.
    #[doc(hidden)]
    pub fn decode_paged_durable_generation(
        bytes: &[u8],
        source: Arc<dyn ReadOnlyVectorGraphPageSource>,
        expected_dimension: usize,
        expected_quantization: VectorQuantization,
        admitted_resident_bytes: usize,
        cache_bytes: usize,
    ) -> Result<Self> {
        Self::decode_paged_generation(
            bytes,
            source,
            expected_dimension,
            expected_quantization,
            admitted_resident_bytes,
            cache_bytes,
            EnvelopeCheck::Verify,
        )
    }

    /// The same decode for an envelope whose every byte the caller has
    /// already checked against a digest recorded when it was written -- a
    /// saved graph whose record was read under that record's own digest. The
    /// envelope checksum would be a second full pass over the same bytes and
    /// could catch nothing the first did not, so it is not repeated; every
    /// length and cross-reference is still checked.
    #[doc(hidden)]
    pub fn decode_paged_authenticated_generation(
        bytes: &[u8],
        source: Arc<dyn ReadOnlyVectorGraphPageSource>,
        expected_dimension: usize,
        expected_quantization: VectorQuantization,
        admitted_resident_bytes: usize,
        cache_bytes: usize,
    ) -> Result<Self> {
        Self::decode_paged_generation(
            bytes,
            source,
            expected_dimension,
            expected_quantization,
            admitted_resident_bytes,
            cache_bytes,
            EnvelopeCheck::AuthenticatedByCaller,
        )
    }

    fn decode_paged_generation(
        bytes: &[u8],
        source: Arc<dyn ReadOnlyVectorGraphPageSource>,
        expected_dimension: usize,
        expected_quantization: VectorQuantization,
        admitted_resident_bytes: usize,
        cache_bytes: usize,
        check: EnvelopeCheck,
    ) -> Result<Self> {
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(8)? != DURABLE_ENVELOPE_MAGIC
            || reader.read_u16()? != DURABLE_ENVELOPE_VERSION
        {
            return Err(Error::Other("invalid durable HNSW envelope".to_string()));
        }
        let checksum = reader.read_u64()?;
        if check == EnvelopeCheck::Verify
            && read_checksum(reader.remaining(), "graph_decode_hashed_bytes") != checksum
        {
            return Err(Error::Other(
                "durable HNSW envelope checksum mismatch".to_string(),
            ));
        }
        let dimension = reader.read_usize()?;
        let quantization = quantization_from_tag(reader.read_u8()?)?;
        if dimension != expected_dimension || quantization != expected_quantization {
            return Err(Error::Other(
                "durable HNSW declaration mismatch".to_string(),
            ));
        }
        if quantization != VectorQuantization::F32 {
            return Err(Error::Other(
                "paged durable HNSW currently requires F32 vectors".to_string(),
            ));
        }
        let ef_search = reader.read_usize()?;
        if ef_search == 0 {
            return Err(Error::Other(
                "durable HNSW has an invalid search breadth".to_string(),
            ));
        }
        let graph_length = reader.read_usize()?;
        let graph_offset = reader.position;
        let graph_bytes = reader.read_exact(graph_length)?;
        let mut graph = DurableReader::new(graph_bytes);
        if graph.read_u16()? != 1 {
            return Err(Error::Other(
                "unsupported ContextDB HNSW graph format version".to_string(),
            ));
        }
        let m = graph.read_usize()?;
        let ef_construction = graph.read_usize()?;
        let _extend_candidates = graph.read_u8()?;
        let _keep_pruned = graph.read_u8()?;
        let max_layer = graph.read_usize()?;
        if m == 0 || max_layer == 0 || max_layer > 16 {
            return Err(Error::Other("durable HNSW topology is invalid".to_string()));
        }
        if graph.read_usize()? != dimension {
            return Err(Error::Other(
                "durable HNSW graph point dimension does not match its definition".to_string(),
            ));
        }
        let scale = graph.read_f64()?;
        if !scale.is_finite() || scale <= 0.0 {
            return Err(Error::Other(
                "durable HNSW graph has an invalid level scale".to_string(),
            ));
        }
        let layer_count = graph.read_usize()?;
        if layer_count != max_layer {
            return Err(Error::Other(
                "durable HNSW graph layer count does not match its definition".to_string(),
            ));
        }
        let mut points = Vec::with_capacity(layer_count);
        let mut point_count = 0usize;
        for layer in 0..layer_count {
            let count = graph.read_usize()?;
            let mut decoded = Vec::with_capacity(count);
            for rank in 0..count {
                let origin_id = graph.read_usize()?;
                let point_id = graph.read_point_id()?;
                if point_id != hnsw_rs::hnsw::PointId(layer as u8, rank as i32) {
                    return Err(Error::Other(
                        "durable HNSW graph has an inconsistent point position".to_string(),
                    ));
                }
                if graph.read_usize()? != dimension {
                    return Err(Error::Other(
                        "durable HNSW graph point dimension does not match its definition"
                            .to_string(),
                    ));
                }
                let vector_offset = graph_offset
                    .checked_add(graph.position)
                    .ok_or_else(|| Error::Other("durable HNSW offset overflow".to_string()))?;
                graph.skip(dimension.checked_mul(4).ok_or_else(|| {
                    Error::Other("durable HNSW point byte length overflow".to_string())
                })?)?;
                let neighbour_layers = graph.read_usize()?;
                if neighbour_layers != 16 {
                    return Err(Error::Other(
                        "durable HNSW graph has an invalid neighbour layer count".to_string(),
                    ));
                }
                let mut neighbours = Vec::with_capacity(neighbour_layers);
                for neighbour_layer in 0..neighbour_layers {
                    let neighbour_count = graph.read_usize()?;
                    if neighbour_count > hnsw_rs::hnsw::layer0_max_nb_connection(m)
                        || (neighbour_layer >= max_layer && neighbour_count != 0)
                    {
                        return Err(Error::Other(
                            "durable HNSW graph has too many neighbours".to_string(),
                        ));
                    }
                    let mut layer_neighbours = Vec::with_capacity(neighbour_count);
                    for _ in 0..neighbour_count {
                        let origin_id = graph.read_usize()?;
                        let neighbour = graph.read_point_id()?;
                        let distance = graph.read_f32()?;
                        if !distance.is_finite() {
                            return Err(Error::Other(
                                "durable HNSW graph has a non-finite edge distance".to_string(),
                            ));
                        }
                        layer_neighbours.push(PagedNeighbour {
                            origin_id: u32::try_from(origin_id).map_err(|_| {
                                Error::Other(
                                    "durable HNSW edge origin exceeds the paged graph limit"
                                        .to_string(),
                                )
                            })?,
                            point_id: neighbour,
                            distance,
                        });
                    }
                    neighbours.push(layer_neighbours);
                }
                decoded.push(PagedPoint {
                    origin_id,
                    vector_offset,
                    neighbours,
                });
                point_count = point_count
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("durable HNSW point count overflow".to_string()))?;
            }
            points.push(decoded);
        }
        let entry_point = match graph.read_u8()? {
            0 => None,
            1 => Some(graph.read_point_id()?),
            _ => {
                return Err(Error::Other(
                    "durable HNSW entry point is invalid".to_string(),
                ));
            }
        };
        if !graph.is_empty() {
            return Err(Error::Other(
                "ContextDB HNSW graph has trailing bytes".to_string(),
            ));
        }
        let point_at = |id: hnsw_rs::hnsw::PointId| {
            usize::try_from(id.1)
                .ok()
                .and_then(|rank| points.get(id.0 as usize)?.get(rank))
        };
        if (point_count == 0) != entry_point.is_none()
            || entry_point.is_some_and(|id| point_at(id).is_none())
        {
            return Err(Error::Other(
                "durable HNSW graph has an invalid entry point".to_string(),
            ));
        }
        for point in points.iter().flatten() {
            for edge in point.neighbours.iter().flatten() {
                let neighbour = point_at(edge.point_id).ok_or_else(|| {
                    Error::Other("durable HNSW graph edge refers to a missing point".to_string())
                })?;
                if neighbour.origin_id != edge.origin_id as usize {
                    return Err(Error::Other(
                        "durable HNSW graph edge has an inconsistent origin id".to_string(),
                    ));
                }
            }
        }
        let row_count = reader.read_usize()?;
        if row_count != point_count || row_count > reader.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW graph point count does not match row mappings".to_string(),
            ));
        }
        let mut id_to_row = HashMap::with_capacity(row_count);
        let mut row_to_point = BTreeMap::new();
        let mut previous_data_id = None;
        for _ in 0..row_count {
            let data_id = reader.read_usize()?;
            let row_id = RowId(reader.read_u64()?);
            if previous_data_id.is_some_and(|previous| data_id <= previous)
                || id_to_row.insert(data_id, row_id).is_some()
            {
                return Err(Error::Other(
                    "durable HNSW has invalid row mappings".to_string(),
                ));
            }
            previous_data_id = Some(data_id);
        }
        for (layer, layer_points) in points.iter().enumerate() {
            for (rank, point) in layer_points.iter().enumerate() {
                let row_id = *id_to_row.get(&point.origin_id).ok_or_else(|| {
                    Error::Other("durable HNSW graph points do not match row mappings".to_string())
                })?;
                let rank = i32::try_from(rank).map_err(|_| {
                    Error::Other("durable HNSW graph point rank exceeds i32".to_string())
                })?;
                if row_to_point
                    .insert(row_id, PointId(layer as u8, rank))
                    .is_some()
                {
                    return Err(Error::Other(
                        "durable HNSW has invalid row mappings".to_string(),
                    ));
                }
            }
        }
        let exact_count = reader.read_usize()?;
        if exact_count > reader.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW exact-vector mapping count is invalid".to_string(),
            ));
        }
        // The paged graph retains only a compact key fingerprint and row IDs.
        // Each possible match is checked against its saved vector bytes.
        let mut exact_hash_rows: HashMap<Vec<u8>, Vec<RowId>> = HashMap::with_capacity(exact_count);
        let mut previous_key: Option<&[u8]> = None;
        for _ in 0..exact_count {
            let key = reader.read_bytes()?;
            if key.is_empty() || previous_key.is_some_and(|previous| key <= previous) {
                return Err(Error::Other(
                    "durable HNSW has invalid exact-vector mappings".to_string(),
                ));
            }
            let count = reader.read_usize()?;
            if count == 0 || count > reader.remaining().len() / 8 {
                return Err(Error::Other(
                    "durable HNSW exact row length is invalid".to_string(),
                ));
            }
            let mut previous_row = None;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            let rows = exact_hash_rows
                .entry(hasher.finish().to_le_bytes().to_vec())
                .or_default();
            for _ in 0..count {
                let row = reader.read_u64()?;
                if previous_row.is_some_and(|previous| row <= previous) {
                    return Err(Error::Other(
                        "durable HNSW exact-vector rows are not ordered".to_string(),
                    ));
                }
                previous_row = Some(row);
                rows.push(RowId(row));
            }
            previous_key = Some(key);
        }
        for rows in exact_hash_rows.values_mut() {
            rows.sort_unstable();
            rows.dedup();
        }
        if !reader.is_empty() {
            return Err(Error::Other(
                "durable HNSW envelope has trailing payload bytes".to_string(),
            ));
        }
        let topology_bytes = points.iter().fold(
            points.capacity() * std::mem::size_of::<Vec<PagedPoint>>(),
            |total, layer| {
                layer.iter().fold(
                    total.saturating_add(layer.capacity() * std::mem::size_of::<PagedPoint>()),
                    |point_total, point| {
                        point.neighbours.iter().fold(
                            point_total.saturating_add(
                                point.neighbours.capacity()
                                    * std::mem::size_of::<Vec<PagedNeighbour>>(),
                            ),
                            |edge_total, edges| {
                                edge_total.saturating_add(
                                    edges.capacity() * std::mem::size_of::<PagedNeighbour>(),
                                )
                            },
                        )
                    },
                )
            },
        );
        let source_page_bytes = source.page_bytes();
        if source_page_bytes == 0 || source_page_bytes > PAGED_VECTOR_SOURCE_PAGE_BYTES {
            return Err(Error::Other(
                "retained graph source page exceeds its serving cache".to_string(),
            ));
        }
        if cache_bytes < PAGED_VECTOR_MIN_CACHE_BYTES
            || !cache_bytes.is_multiple_of(PAGED_VECTOR_SOURCE_PAGE_BYTES)
        {
            return Err(Error::Other(
                "durable HNSW reader cache size is outside its supported bounds".to_string(),
            ));
        }
        let page_capacity = cache_bytes.min(bytes.len());
        let page_slots = page_capacity.div_ceil(source_page_bytes);
        let cache_bytes = page_slots
            .checked_mul(source_page_bytes)
            .ok_or_else(|| Error::Other("retained graph page cache size overflow".to_string()))?;
        // The serving cache is admitted for every slot it may fill, up front
        // and in full, exactly as when those slots were one pre-allocated
        // arena: the page bodies, the shared handle each one carries, and the
        // directory that finds them. Page bodies are allocated as they are
        // first read, and every one is charged for as long as it is
        // allocated: a page the cache holds by its slot, and a page read but
        // not kept by the walk that read it, until that walk drops it. A slot
        // whose page a walk is still reading is never handed to another page
        // and never given back, so its charge stays for as long as the page
        // does.
        let slot_directory_bytes = serving_slot_directory_bytes(page_slots);
        let actual_resident = std::mem::size_of::<Self>()
            .saturating_add(topology_bytes)
            .saturating_add(row_count.saturating_mul(DURABLE_READER_ROW_MAPPING_BYTES))
            .saturating_add(cache_bytes)
            .saturating_add(slot_directory_bytes)
            .saturating_add(source.retained_bytes());
        if actual_resident > admitted_resident_bytes {
            return Err(Error::Other(
                "paged durable HNSW exceeded its admitted resident bound".to_string(),
            ));
        }
        Ok(Self {
            hnsw: HnswInner::F32Paged(PagedHnsw {
                points,
                entry_point,
                source,
                dimension,
                max_nb_connection: m,
                layer0_max_degree: hnsw_rs::hnsw::layer0_max_nb_connection(m),
                resident_bytes: AtomicUsize::new(actual_resident),
                graph_len: bytes.len(),
                source_page_bytes,
                page_slots,
                page_cache: Mutex::new(PagedVectorPageCache::new(page_slots)),
            }),
            id_to_row: RwLock::new(id_to_row),
            row_to_point: RwLock::new(row_to_point),
            exact_rows: RwLock::new(exact_hash_rows),
            next_id: AtomicUsize::new(point_count),
            build_serial: NEXT_HNSW_BUILD_SERIAL.fetch_add(1, Ordering::SeqCst) as u64,
            dimension,
            quantization,
            m,
            ef_construction,
            ef_search,
        })
    }

    /// Number of vectors currently indexed in the HNSW graph.
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[doc(hidden)]
    pub fn policy_values(&self) -> (usize, usize, usize) {
        (self.m, self.ef_construction, self.ef_search)
    }

    /// The same ownership estimate used for graph admission. Durable catalog
    /// metadata records this value so lazy restart can reserve memory before
    /// decoding graph bytes.
    #[doc(hidden)]
    /// How many serving slots a paged generation's cache holds; `None` for a
    /// graph that holds its vectors itself, or whose cache a walk is using
    /// right now.
    pub fn serving_cache_slots(&self) -> Option<usize> {
        match &self.hnsw {
            HnswInner::F32Paged(graph) => Some(graph.page_cache.try_lock()?.slot_count()),
            _ => None,
        }
    }

    /// What one serving slot of this generation is charged besides the
    /// directory: a page body and the shared handle it is installed with.
    /// This is what giving the slot up frees. `None` for a graph that holds
    /// its vectors itself.
    pub fn serving_slot_bytes(&self) -> Option<usize> {
        match &self.hnsw {
            HnswInner::F32Paged(graph) => Some(graph.source_page_bytes + SERVING_PAGE_HANDLE_BYTES),
            _ => None,
        }
    }

    /// Give serving slots back until at most `keep` remain (never fewer than
    /// one), keeping the most recently used pages. The topology is untouched,
    /// so every search still reaches every point; a page given back is read
    /// and authenticated again the next time a walk needs it.
    ///
    /// Returns the bytes this freed, for the caller to release, and this
    /// generation's own charge falls by the same amount. Only slots no walk
    /// holds are given back, so every byte returned is memory that is really
    /// gone: each such slot's page body, and whatever the cache's directory
    /// charge fell by, read from what the directory has allocated. A slot
    /// whose page a walk is still reading keeps its page and its charge.
    /// Nothing here allocates.
    pub fn give_back_serving_slots(&self, keep: usize) -> usize {
        let HnswInner::F32Paged(graph) = &self.hnsw else {
            return 0;
        };
        // Tried, not waited for: a walk holding this cache keeps it, and the
        // caller's allocation may be that walk's own.
        let Some(mut cache) = graph.page_cache.try_lock() else {
            return 0;
        };
        let directory_before = cache.directory_bytes();
        let given = cache.give_back_unheld(keep);
        let directory_after = cache.directory_bytes();
        drop(cache);
        let freed = given
            .saturating_mul(graph.source_page_bytes)
            .saturating_add(directory_before.saturating_sub(directory_after));
        let _held = graph
            .resident_bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                Some(held.saturating_sub(freed))
            });
        freed
    }

    pub fn estimated_resident_bytes(&self) -> usize {
        match &self.hnsw {
            HnswInner::F32Paged(graph) => graph.resident_bytes.load(Ordering::SeqCst),
            _ => Self::estimated_resident_bytes_with_m(
                self.len(),
                self.dimension,
                self.quantization,
                self.m,
            ),
        }
    }

    #[doc(hidden)]
    pub fn estimated_build_reservation(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
    ) -> usize {
        crate::mem::estimate_hnsw_build_reservation(count, dimension, quantization, policy)
    }

    #[doc(hidden)]
    pub fn estimated_resident_bytes_for_count(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> usize {
        let policy = crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
            .resolve_policy(count, 1);
        Self::estimated_resident_bytes_with_m(count, dimension, quantization, policy.hnsw_m)
    }

    /// Conservative retained ownership bound: point values and lookup maps,
    /// all possible layer headers, and neighbour capacity for the resolved M.
    /// The bottom layer keeps 2*M links; the other fifteen keep M each.
    #[doc(hidden)]
    pub fn estimated_resident_bytes_with_m(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
    ) -> usize {
        let entry_bytes = match quantization {
            VectorQuantization::F32 => quantization.storage_bytes(dimension),
            VectorQuantization::SQ8 => dimension.saturating_add(12),
            VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(12),
        };
        let exact_key_bytes = entry_bytes
            .saturating_add(std::mem::size_of::<RowId>())
            .saturating_add(64);
        std::mem::size_of::<Self>()
            .saturating_add(16 * std::mem::size_of::<Vec<usize>>())
            .saturating_add(
                count.saturating_mul(
                    entry_bytes
                        .saturating_mul(3)
                        .saturating_add(exact_key_bytes)
                        .saturating_add(256)
                        .saturating_add(16 * std::mem::size_of::<Vec<usize>>())
                        .saturating_add(
                            m.saturating_mul(17)
                                .saturating_mul(2 * std::mem::size_of::<usize>()),
                        ),
                ),
            )
    }

    #[doc(hidden)]
    pub fn graph_stats(&self) -> HnswGraphStats {
        let (point_count, layer0_neighbor_edges, max_level_observed) = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw_stats(hnsw),
            HnswInner::F32Sealed(hnsw) => hnsw_loaded_stats(hnsw),
            HnswInner::QuantizedMutable(hnsw) => hnsw_stats(hnsw),
            HnswInner::QuantizedSealed(hnsw) => hnsw_loaded_stats(hnsw),
            HnswInner::F32Paged(hnsw) => (
                hnsw.points.iter().map(Vec::len).sum(),
                hnsw.points
                    .iter()
                    .flatten()
                    .map(|point| point.neighbours.first().map_or(0, Vec::len))
                    .sum(),
                hnsw.points
                    .iter()
                    .enumerate()
                    .filter(|(_, points)| !points.is_empty())
                    .map(|(layer, _)| layer as u8)
                    .max()
                    .unwrap_or(0),
            ),
        };

        HnswGraphStats {
            point_count,
            layer0_points: point_count,
            layer0_neighbor_edges,
            max_level_observed,
            dimension: self.dimension,
        }
    }

    fn original_query_score(&self, point: &hnsw_rs::hnsw::PointId, query: &[f32]) -> Option<f32> {
        let score = |stored: &[f32]| {
            #[cfg(feature = "test-seams")]
            observe_hnsw_candidate_distance();
            crate::cosine_similarity(stored, query)
        };
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => {
                graph.get_point_indexation().with_point_data(point, score)
            }
            HnswInner::F32Sealed(graph) => {
                graph.get_point_indexation().with_point_data(point, score)
            }
            HnswInner::QuantizedMutable(graph) => graph
                .get_point_indexation()
                .with_point_data(point, |stored| {
                    #[cfg(feature = "test-seams")]
                    observe_hnsw_candidate_distance();
                    crate::quantized::quantized_hnsw_query_score(stored, query, self.quantization)
                })
                .flatten(),
            HnswInner::QuantizedSealed(graph) => graph
                .get_point_indexation()
                .with_point_data(point, |stored| {
                    #[cfg(feature = "test-seams")]
                    observe_hnsw_candidate_distance();
                    crate::quantized::quantized_hnsw_query_score(stored, query, self.quantization)
                })
                .flatten(),
            HnswInner::F32Paged(_) => None,
        }
    }

    pub fn search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        self.search_with_ef_search(index, query, k, self.ef_search)
    }

    pub(crate) fn search_with_ef_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let got = query.len();
        if got != self.dimension {
            return Err(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: got,
            });
        }

        let ef = hnsw_search_ef(ef_search, k);
        let neighbors = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => {
                search_resident_f32(
                    hnsw.get_point_indexation(),
                    hnsw.get_distance(),
                    query,
                    ef,
                    ef,
                    || Ok::<(), Error>(()),
                    || Ok::<(), Error>(()),
                    |_| Ok::<(), Error>(()),
                    |_| {},
                )
                .map_err(map_core_bounded_error)?
                .neighbours
            }
            HnswInner::F32Sealed(hnsw) => {
                search_resident_f32(
                    hnsw.get_point_indexation(),
                    hnsw.get_distance(),
                    query,
                    ef,
                    ef,
                    || Ok::<(), Error>(()),
                    || Ok::<(), Error>(()),
                    |_| Ok::<(), Error>(()),
                    |_| {},
                )
                .map_err(map_core_bounded_error)?
                .neighbours
            }
            HnswInner::QuantizedMutable(hnsw) => {
                let encoded = StoredVector::from_f32(query, self.quantization).to_hnsw_u8();
                hnsw.search(&encoded, ef, ef)
            }
            HnswInner::QuantizedSealed(hnsw) => {
                let encoded = StoredVector::from_f32(query, self.quantization).to_hnsw_u8();
                hnsw.search(&encoded, ef, ef)
            }
            HnswInner::F32Paged(hnsw) => {
                hnsw.search_with_bounded_control(
                    query,
                    ef,
                    ef,
                    || Ok::<_, Error>(()),
                    || Ok::<_, Error>(()),
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
                .map_err(map_core_bounded_error)?
                .neighbours
            }
        };
        let id_to_row = self.id_to_row.read();
        let cap = hnsw_search_candidate_cap(ef_search, k);
        let mut serving_pages = self.serving_page_reader().transpose()?;
        let query_norm = Self::stored_query_norm(query);
        let mut scored = Vec::with_capacity(
            cap.saturating_add(neighbors.len())
                .min(cap.saturating_mul(2)),
        );
        if let Some((key, exact_score)) = exact_key_for_query(query, self.quantization) {
            if let HnswInner::F32Paged(paged) = &self.hnsw {
                paged
                    .visit_exact_rows(
                        &key,
                        query,
                        &self.exact_rows.read(),
                        &self.row_to_point.read(),
                        cap,
                        &mut || Ok::<(), Error>(()),
                        &mut |_| Ok::<(), Error>(()),
                        &mut |_| {},
                        |row_id, _, _, _| {
                            scored.push((row_id, exact_score));
                            Ok(true)
                        },
                    )
                    .map_err(map_core_bounded_error)?;
            } else if let Some(row_ids) = self.exact_rows.read().get(&key) {
                scored.extend(
                    row_ids
                        .iter()
                        .take(cap)
                        .map(|row_id| (*row_id, exact_score)),
                );
            }
        }

        for neighbor in neighbors {
            let Some(row_id) = id_to_row.get(&neighbor.d_id).copied() else {
                continue;
            };
            let score = if let Some(pages) = serving_pages.as_mut() {
                self.serving_page_similarity(
                    row_id,
                    query,
                    query_norm,
                    &mut || Ok::<(), Error>(()),
                    &mut || Ok::<(), Error>(()),
                    &mut |_| Ok::<(), Error>(()),
                    &mut |_| {},
                    pages,
                )?
                .unwrap_or(1.0 - neighbor.distance)
            } else {
                self.original_query_score(&neighbor.p_id, query)
                    .unwrap_or(1.0 - neighbor.distance)
            };
            scored.push((row_id, score));
        }
        scored.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut seen = HashSet::new();
        scored.retain(|(row_id, _)| seen.insert(*row_id));
        scored.truncate(cap);
        Ok(scored)
    }

    /// Additive bounded HNSW search with transactional, exact-capacity events.
    #[allow(clippy::too_many_arguments)]
    pub fn search_with_bounded_memory<E, S, D, A, R>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        before_source_entry: S,
        before_distance: D,
        acquire: A,
        release: R,
    ) -> std::result::Result<HnswSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        self.search_with_bounded_memory_at_ef(
            index,
            query,
            k,
            self.ef_search,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_with_bounded_memory_at_ef<E, S, D, A, R>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        ef_search: usize,
        before_source_entry: S,
        before_distance: D,
        acquire: A,
        release: R,
    ) -> std::result::Result<HnswSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        let mut query_self_scores = BoundedHnswQuerySelfScores::new(query);
        self.search_bounded_at_ef_with_query_scores(
            index,
            query,
            k,
            ef_search,
            &mut query_self_scores,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_bounded_at_ef_with_query_scores<E, S, D, A, R>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        ef_search: usize,
        query_self_scores: &mut BoundedHnswQuerySelfScores<'_>,
        mut before_source_entry: S,
        mut before_distance: D,
        mut acquire: A,
        mut release: R,
    ) -> std::result::Result<HnswSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        if k == 0 {
            return Ok(HnswSearchMemoryResult {
                rows: Vec::new(),
                retained_bytes: 0,
            });
        }
        if query.len() != self.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: query.len(),
            }));
        }

        let ef = hnsw_search_ef(ef_search, k);
        let mut encoded_query = None;
        if matches!(
            &self.hnsw,
            HnswInner::QuantizedMutable(_) | HnswInner::QuantizedSealed(_)
        ) {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            encoded_query = Some(encode_bounded_quantized_query(
                query,
                self.quantization,
                &mut scratch,
            )?);
        }
        let searched = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => search_resident_f32(
                hnsw.get_point_indexation(),
                hnsw.get_distance(),
                query,
                ef,
                ef,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::F32Sealed(hnsw) => search_resident_f32(
                hnsw.get_point_indexation(),
                hnsw.get_distance(),
                query,
                ef,
                ef,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::QuantizedMutable(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_with_bounded_control(
                    encoded,
                    ef,
                    ef,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
            HnswInner::QuantizedSealed(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_with_bounded_control(
                    encoded,
                    ef,
                    ef,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
            HnswInner::F32Paged(hnsw) => hnsw.search_with_bounded_control(
                query,
                ef,
                ef,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
        };
        let raw = match searched {
            Ok(raw) => raw,
            Err(error) => {
                if let Some(encoded) = encoded_query.take() {
                    let mut scratch = VectorSearchScratch {
                        acquire: &mut acquire,
                        release: &mut release,
                    };
                    scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
                }
                return Err(map_core_bounded_error(error));
            }
        };
        let mut scored = Vec::new();
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        let mut exact_key = None;
        let cap = hnsw_search_candidate_cap(ef_search, k);
        let query_norm = Self::stored_query_norm(query);
        let merge = (|| -> std::result::Result<(), E> {
            let mut serving_pages = self.serving_page_reader().transpose().map_err(E::from)?;
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            let exact_score = query_self_scores.score(
                query,
                self.quantization,
                encoded_query.as_deref(),
                &mut before_distance,
            )?;
            if let Some(exact_score) = exact_score {
                if matches!(self.quantization, VectorQuantization::F32) {
                    exact_key = Some(bounded_f32_exact_key(query, &mut scratch)?);
                }
                let key = exact_key
                    .as_deref()
                    .or(encoded_query.as_deref())
                    .unwrap_or_default();
                before_source_entry()?;
                let exact_rows = self.exact_rows.read();
                if let HnswInner::F32Paged(paged) = &self.hnsw {
                    paged
                        .visit_exact_rows(
                            key,
                            query,
                            &exact_rows,
                            &self.row_to_point.read(),
                            cap,
                            &mut before_source_entry,
                            &mut scratch.acquire,
                            &mut scratch.release,
                            |row_id, before_source_entry, acquire, release| {
                                let required = scored.len().checked_add(1).ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded HNSW merge-result length overflow".to_string(),
                                    ))
                                })?;
                                let mut row_scratch = VectorSearchScratch {
                                    acquire: &mut **acquire,
                                    release: &mut **release,
                                };
                                row_scratch.ensure_vec_capacity(
                                    &mut scored,
                                    HnswSearchScratchContainer::MergeResults,
                                    required,
                                    "merge results",
                                    before_source_entry,
                                )?;
                                scored.push((row_id, exact_score));
                                Ok(true)
                            },
                        )
                        .map_err(map_core_bounded_error)?;
                } else if let Some(row_ids) = exact_rows.get(key) {
                    let row_limit = cap.min(row_ids.len());
                    let mut row_position = 0usize;
                    while row_position < row_limit {
                        before_source_entry()?;
                        let row_id = *row_ids.get(row_position).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW exact-row position changed".to_string(),
                            ))
                        })?;
                        row_position = row_position.checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW exact-row position overflow".to_string(),
                            ))
                        })?;
                        let required = scored.len().checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW merge-result length overflow".to_string(),
                            ))
                        })?;
                        scratch.ensure_vec_capacity(
                            &mut scored,
                            HnswSearchScratchContainer::MergeResults,
                            required,
                            "merge results",
                            &mut before_source_entry,
                        )?;
                        scored.push((row_id, exact_score));
                    }
                }
            }
            before_source_entry()?;
            let id_to_row = self.id_to_row.read();
            let mut neighbour_position = 0usize;
            while neighbour_position < raw.neighbours.len() {
                before_source_entry()?;
                let neighbour = raw.neighbours.get(neighbour_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW raw-neighbour position changed".to_string(),
                    ))
                })?;
                neighbour_position = neighbour_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW raw-neighbour position overflow".to_string(),
                    ))
                })?;
                let Some(row_id) = id_to_row.get(&neighbour.d_id).copied() else {
                    continue;
                };
                let required = scored.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW merge-result length overflow".to_string(),
                    ))
                })?;
                scratch.ensure_vec_capacity(
                    &mut scored,
                    HnswSearchScratchContainer::MergeResults,
                    required,
                    "merge results",
                    &mut before_source_entry,
                )?;
                let score = if let Some(pages) = serving_pages.as_mut() {
                    self.serving_page_similarity(
                        row_id,
                        query,
                        query_norm,
                        &mut before_source_entry,
                        &mut before_distance,
                        &mut scratch.acquire,
                        &mut scratch.release,
                        pages,
                    )?
                    .unwrap_or(1.0 - neighbour.distance)
                } else {
                    before_distance()?;
                    self.original_query_score(&neighbour.p_id, query)
                        .unwrap_or(1.0 - neighbour.distance)
                };
                scored.push((row_id, score));
            }
            scored.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let mut scored_position = 0usize;
            while scored_position < scored.len() {
                before_source_entry()?;
                let (row_id, score) = *scored.get(scored_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW scored-row position changed".to_string(),
                    ))
                })?;
                scored_position = scored_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW scored-row position overflow".to_string(),
                    ))
                })?;
                if seen.contains(&row_id) {
                    continue;
                }
                let required = seen.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW seen-row length overflow".to_string(),
                    ))
                })?;
                scratch.ensure_set_capacity(
                    &mut seen,
                    HnswSearchScratchContainer::SeenRows,
                    required,
                    "seen rows",
                    &mut before_source_entry,
                )?;
                seen.insert(row_id);
                if rows.len() == cap {
                    break;
                }
                let required = rows.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW result length overflow".to_string(),
                    ))
                })?;
                scratch.ensure_vec_capacity(
                    &mut rows,
                    HnswSearchScratchContainer::Result,
                    required,
                    "result rows",
                    &mut before_source_entry,
                )?;
                rows.push((row_id, score));
            }
            Ok(())
        })();
        let retained_bytes = rows
            .capacity()
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
            .ok_or_else(|| {
                E::from(Error::Other(
                    "bounded HNSW result capacity exceeds the native address space".to_string(),
                ))
            });
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        if let Some(key) = exact_key.take() {
            scratch.free_vec(key, HnswSearchScratchContainer::ExactKey);
        }
        if let Some(encoded) = encoded_query.take() {
            scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
        }
        scratch.free_vec(raw.neighbours, HnswSearchScratchContainer::Result);
        scratch.free_vec(scored, HnswSearchScratchContainer::MergeResults);
        scratch.free_set(seen, HnswSearchScratchContainer::SeenRows);
        match merge {
            Ok(()) => {
                let retained_bytes = match retained_bytes {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        scratch.free_vec(rows, HnswSearchScratchContainer::Result);
                        return Err(error);
                    }
                };
                Ok(HnswSearchMemoryResult {
                    rows,
                    retained_bytes,
                })
            }
            Err(error) => {
                scratch.free_vec(rows, HnswSearchScratchContainer::Result);
                Err(error)
            }
        }
    }

    /// Bounded indexed search for a relationally supplied allowed-row set.
    /// The predicate is consulted while graph candidates are admitted, not as
    /// a second pass over the graph or vector store.  When the hard traversal
    /// limits cannot yield the requested number of allowed rows, `status` is
    /// `Incomplete` and callers must use their typed refusal path.
    #[allow(clippy::too_many_arguments)]
    pub fn search_allowed_with_bounded_memory<E, S, D, A, R, F>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        limits: HnswAllowedSearchLimits,
        is_allowed: F,
        before_source_entry: S,
        before_distance: D,
        acquire: A,
        release: R,
    ) -> std::result::Result<HnswAllowedSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId) -> bool,
    {
        self.search_allowed_with_bounded_memory_at_ef(
            index,
            query,
            k,
            allowed_count,
            limits,
            self.ef_search,
            std::iter::empty,
            false,
            is_allowed,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )
    }

    /// A reader for this generation's serving pages, when the generation
    /// serves from pages at all. One reader is opened per statement, so the
    /// pages a statement authenticates answer every candidate that follows.
    ///
    /// It is opened with no page directory of its own: this is the sealed
    /// scoring path, which visits whatever candidates its caller hands it
    /// rather than walking the graph, so it has no charged scratch to give a
    /// directory and asks the serving cache for every page, as before.
    pub(crate) fn serving_page_reader(&self) -> Option<Result<ServingPages>> {
        match &self.hnsw {
            HnswInner::F32Paged(paged) => Some(
                paged
                    .source
                    .reader()
                    .map(|reader| ServingPages::new(reader, 0)),
            ),
            _ => None,
        }
    }

    /// The query's own norm, summed the way a resident row body sums it. It is
    /// the same for every candidate in one statement, so it is totalled once.
    pub(crate) fn stored_query_norm(query: &[f32]) -> f32 {
        query
            .iter()
            .fold(0.0_f32, |norm, value| norm + value * value)
    }

    /// One row's cosine similarity, read out of this generation's serving
    /// pages instead of out of that row's own stored body. `None` when the
    /// generation does not serve from pages or does not hold the row.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn serving_page_similarity<E, S, D, A, R>(
        &self,
        row_id: RowId,
        query: &[f32],
        query_norm: f32,
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
        pages: &mut ServingPages,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        let HnswInner::F32Paged(paged) = &self.hnsw else {
            return Ok(None);
        };
        if query.len() != self.dimension {
            return Ok(None);
        }
        let Some(point_id) = self.row_to_point.read().get(&row_id).copied() else {
            return Ok(None);
        };
        let Some(point) = paged.point(point_id) else {
            return Ok(None);
        };
        paged
            .stored_similarity(
                point,
                query,
                query_norm,
                before_source_entry,
                before_distance,
                acquire,
                release,
                pages,
            )
            .map(Some)
            .map_err(map_core_bounded_error)
    }

    /// Whether this generation serves its point bodies from pages rather than
    /// from a resident copy of the whole graph.
    pub(crate) fn serves_from_pages(&self) -> bool {
        matches!(self.hnsw, HnswInner::F32Paged(_))
    }

    /// The page offsets this generation's serving cache currently holds.
    /// Proof-only: production code never asks what is resident.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn resident_page_offsets_for_test(&self) -> Vec<usize> {
        let HnswInner::F32Paged(paged) = &self.hnsw else {
            return Vec::new();
        };
        let cache = paged.page_cache.lock();
        cache
            .slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(|page| page.offset))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_allowed_with_bounded_memory_at_ef<E, S, D, A, R, F, I>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        limits: HnswAllowedSearchLimits,
        ef_search: usize,
        seed_rows: impl Fn() -> I,
        seed_all: bool,
        is_allowed: F,
        before_source_entry: S,
        before_distance: D,
        acquire: A,
        release: R,
    ) -> std::result::Result<HnswAllowedSearchMemoryResult, E>
    where
        I: Iterator<Item = u64>,
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId) -> bool,
    {
        let mut query_self_scores = BoundedHnswQuerySelfScores::new(query);
        self.search_allowed_bounded_at_ef_with_query_scores(
            index,
            query,
            k,
            allowed_count,
            limits,
            ef_search,
            &mut query_self_scores,
            seed_rows,
            seed_all,
            is_allowed,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )
    }

    /// The seed directory an allowed-row walk starts from, beside the graph's
    /// own entry point: bounded, dispersed rows of the whole directory when
    /// every row may be admitted, or evenly spaced eligible rows of an explicit
    /// candidate directory. Shared by the single-layer walk and the union of
    /// many layers, so both start a layer from the same points.
    #[allow(clippy::too_many_arguments)]
    fn allowed_seed_directory<E, S, A, R, F, I>(
        &self,
        k: usize,
        required_results: usize,
        allowed_count: usize,
        ef: usize,
        seed_rows: &impl Fn() -> I,
        seed_all: bool,
        is_allowed: &mut F,
        before_source_entry: &mut S,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<AllowedSeedDirectory, E>
    where
        I: Iterator<Item = u64>,
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId) -> bool,
    {
        let mut required_results = required_results;
        let mut seed_points = Vec::new();
        let mut supplied_seed_rows = false;
        let mut layer_allowed_count = allowed_count.min(self.row_to_point.read().len());
        let seeds = (|| -> std::result::Result<(), E> {
            let mut scratch = VectorSearchScratch {
                acquire: &mut *acquire,
                release: &mut *release,
            };
            let row_to_point = self.row_to_point.read();
            if seed_all {
                // Seek bounded, dispersed logical keys directly. Walking every
                // key to pick an ordinal sample would make graph startup linear.
                before_source_entry()?;
                let dispersed =
                    dispersed_seed_budget(required_results, allowed_count, row_to_point.len(), ef);
                if dispersed != 0
                    && let (Some((first, _)), Some((last, _))) = (
                        row_to_point.first_key_value(),
                        row_to_point.last_key_value(),
                    )
                {
                    let capacity = dispersed;
                    scratch.ensure_vec_capacity(
                        &mut seed_points,
                        HnswSearchScratchContainer::Result,
                        capacity,
                        "graph seed directory",
                        &mut *before_source_entry,
                    )?;
                    if row_to_point.len() <= ef {
                        // A small logical directory fits entirely inside the
                        // declared graph effort, including widely spaced keys.
                        for (row, point) in row_to_point.iter() {
                            before_source_entry()?;
                            if seed_points.len() == capacity {
                                break;
                            }
                            if is_allowed(*row) {
                                seed_points.push(*point);
                            }
                        }
                    } else {
                        for slot in 0..capacity {
                            before_source_entry()?;
                            let raw = first.0 as u128
                                + (last.0 - first.0) as u128 * slot as u128
                                    / capacity.saturating_sub(1).max(1) as u128;
                            if let Some((row, point)) =
                                row_to_point.range(RowId(raw as u64)..).next()
                                && is_allowed(*row)
                                && !seed_points.contains(point)
                            {
                                seed_points.push(*point);
                            }
                        }
                    }
                }
            } else {
                // The candidate directory spans the whole requested scope,
                // but this graph can contribute only its own eligible rows.
                // Count by direct membership probes, never by walking excluded
                // graph points. A base with one current row must not retry for
                // rows that exist only in the change graph or fresh tail.
                let mut members = 0usize;
                for raw in seed_rows() {
                    supplied_seed_rows = true;
                    before_source_entry()?;
                    if row_to_point.contains_key(&RowId(raw)) && is_allowed(RowId(raw)) {
                        members += 1;
                    }
                }
                if supplied_seed_rows {
                    layer_allowed_count = members;
                    required_results = k.min(members);
                }
                let capacity = ef.min(members);
                scratch.ensure_vec_capacity(
                    &mut seed_points,
                    HnswSearchScratchContainer::Result,
                    capacity,
                    "graph seed directory",
                    &mut *before_source_entry,
                )?;
                let mut selected = 0usize;
                for raw in seed_rows() {
                    if seed_points.len() == capacity {
                        break;
                    }
                    before_source_entry()?;
                    let Some(point) = row_to_point.get(&RowId(raw)) else {
                        continue;
                    };
                    if !is_allowed(RowId(raw)) {
                        continue;
                    }
                    // Evenly spaced eligible ordinals supply exactly capacity
                    // seeds, including when members is not divisible by EF.
                    // This covers small layers completely and supplies at least
                    // k distinct candidates in larger, disconnected layers.
                    let next =
                        (seed_points.len() as u128 * members as u128 / capacity as u128) as usize;
                    if selected == next {
                        seed_points.push(*point);
                    }
                    selected += 1;
                }
            }
            Ok(())
        })();
        if let Err(error) = seeds {
            let mut scratch = VectorSearchScratch { acquire, release };
            scratch.free_vec(seed_points, HnswSearchScratchContainer::Result);
            return Err(error);
        }
        Ok(AllowedSeedDirectory {
            points: seed_points,
            layer_allowed_count,
            required_results,
            supplied_seed_rows,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_allowed_bounded_at_ef_with_query_scores<E, S, D, A, R, F, I>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        limits: HnswAllowedSearchLimits,
        ef_search: usize,
        query_self_scores: &mut BoundedHnswQuerySelfScores<'_>,
        seed_rows: impl Fn() -> I,
        seed_all: bool,
        mut is_allowed: F,
        mut before_source_entry: S,
        mut before_distance: D,
        mut acquire: A,
        mut release: R,
    ) -> std::result::Result<HnswAllowedSearchMemoryResult, E>
    where
        I: Iterator<Item = u64>,
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId) -> bool,
    {
        let required_results = k.min(allowed_count);
        let candidate_capacity = hnsw_search_ef(ef_search, k);
        if required_results == 0 {
            return Ok(HnswAllowedSearchMemoryResult {
                rows: Vec::new(),
                retained_bytes: 0,
                status: HnswAllowedSearchStatus::Complete,
                visited_nodes: 0,
                vector_evaluations: 0,
                allowed_admissions: 0,
            });
        }
        if query.len() != self.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: query.len(),
            }));
        }

        let mut encoded_query = None;
        if matches!(
            &self.hnsw,
            HnswInner::QuantizedMutable(_) | HnswInner::QuantizedSealed(_)
        ) {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            encoded_query = Some(encode_bounded_quantized_query(
                query,
                self.quantization,
                &mut scratch,
            )?);
        }
        let ef = hnsw_search_ef(ef_search, required_results);
        let id_to_row = self.id_to_row.read();
        let AllowedSeedDirectory {
            points: seed_points,
            layer_allowed_count,
            required_results,
            supplied_seed_rows,
        } = match self.allowed_seed_directory(
            k,
            required_results,
            allowed_count,
            ef,
            &seed_rows,
            seed_all,
            &mut is_allowed,
            &mut before_source_entry,
            &mut acquire,
            &mut release,
        ) {
            Ok(directory) => directory,
            Err(error) => {
                if let Some(encoded) = encoded_query.take() {
                    let mut scratch = VectorSearchScratch {
                        acquire: &mut acquire,
                        release: &mut release,
                    };
                    scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
                }
                return Err(error);
            }
        };
        // An explicit candidate directory with no point in this layer proves
        // the layer contributes nothing. Traversing excluded points cannot
        // discover a missing identity and would charge unrelated history.
        if !seed_all && supplied_seed_rows && seed_points.is_empty() {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            scratch.free_vec(seed_points, HnswSearchScratchContainer::Result);
            if let Some(encoded) = encoded_query.take() {
                scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
            }
            return Ok(HnswAllowedSearchMemoryResult {
                rows: Vec::new(),
                retained_bytes: 0,
                status: HnswAllowedSearchStatus::Complete,
                visited_nodes: 0,
                vector_evaluations: 0,
                allowed_admissions: 0,
            });
        }
        let searched = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw.search_allowed_with_bounded_control(
                query,
                required_results,
                ef,
                limits,
                &seed_points,
                layer_allowed_count,
                |data_id| id_to_row[&data_id].0,
                |data_id| {
                    id_to_row
                        .get(&data_id)
                        .is_some_and(|row_id| is_allowed(*row_id))
                },
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::F32Sealed(hnsw) => hnsw.search_allowed_with_bounded_control(
                query,
                required_results,
                ef,
                limits,
                &seed_points,
                layer_allowed_count,
                |data_id| id_to_row[&data_id].0,
                |data_id| {
                    id_to_row
                        .get(&data_id)
                        .is_some_and(|row_id| is_allowed(*row_id))
                },
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::QuantizedMutable(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_allowed_with_bounded_control(
                    encoded,
                    required_results,
                    ef,
                    limits,
                    &seed_points,
                    layer_allowed_count,
                    |data_id| id_to_row[&data_id].0,
                    |data_id| {
                        id_to_row
                            .get(&data_id)
                            .is_some_and(|row_id| is_allowed(*row_id))
                    },
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
            HnswInner::QuantizedSealed(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_allowed_with_bounded_control(
                    encoded,
                    required_results,
                    ef,
                    limits,
                    &seed_points,
                    layer_allowed_count,
                    |data_id| id_to_row[&data_id].0,
                    |data_id| {
                        id_to_row
                            .get(&data_id)
                            .is_some_and(|row_id| is_allowed(*row_id))
                    },
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
            HnswInner::F32Paged(hnsw) => hnsw.search_allowed_with_bounded_control(
                query,
                required_results,
                ef,
                limits,
                &seed_points,
                layer_allowed_count,
                !seed_all,
                |data_id| id_to_row[&data_id].0,
                |data_id| {
                    id_to_row
                        .get(&data_id)
                        .is_some_and(|row_id| is_allowed(*row_id))
                },
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
        };
        VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        }
        .free_vec(seed_points, HnswSearchScratchContainer::Result);
        let raw = match searched {
            Ok(raw) => raw,
            Err(error) => {
                if let Some(encoded) = encoded_query.take() {
                    let mut scratch = VectorSearchScratch {
                        acquire: &mut acquire,
                        release: &mut release,
                    };
                    scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
                }
                return Err(map_core_bounded_error(error));
            }
        };
        let raw_status = raw.status;
        let raw_retained = raw.neighbours;
        let map_rows = (|| -> std::result::Result<(Vec<(RowId, f32)>, bool), E> {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            let mut rows = scratch.allocate_vec(
                HnswSearchScratchContainer::Result,
                candidate_capacity,
                "allowed result rows",
            )?;
            // The rows are admitted and not yet the caller's, so a refusal of
            // the set beside them gives them back.
            let mut seen = match scratch.allocate_set(
                HnswSearchScratchContainer::SeenRows,
                candidate_capacity,
                "allowed result rows",
            ) {
                Ok(seen) => seen,
                Err(error) => {
                    scratch.free_vec(rows, HnswSearchScratchContainer::Result);
                    return Err(error);
                }
            };
            let mut exact_key = None;
            let mut exact_bucket_completes = false;
            let mapped = (|| -> std::result::Result<(), E> {
                if let Some(exact_score) = query_self_scores.score(
                    query,
                    self.quantization,
                    encoded_query.as_deref(),
                    &mut before_distance,
                )? {
                    if matches!(self.quantization, VectorQuantization::F32) {
                        exact_key = Some(bounded_f32_exact_key(query, &mut scratch)?);
                    }
                    let key = exact_key
                        .as_deref()
                        .or(encoded_query.as_deref())
                        .unwrap_or_default();
                    before_source_entry()?;
                    let exact_rows = self.exact_rows.read();
                    if let HnswInner::F32Paged(paged) = &self.hnsw {
                        let mut seed_exact_rows = seed_rows().peekable();
                        paged
                            .visit_exact_rows(
                                key,
                                query,
                                &exact_rows,
                                &self.row_to_point.read(),
                                candidate_capacity,
                                &mut before_source_entry,
                                &mut scratch.acquire,
                                &mut scratch.release,
                                |row_id, before_source_entry, _, _| {
                                    before_source_entry()?;
                                    if supplied_seed_rows {
                                        while seed_exact_rows
                                            .peek()
                                            .is_some_and(|raw| *raw < row_id.0)
                                        {
                                            seed_exact_rows.next();
                                        }
                                        if seed_exact_rows.peek().copied() != Some(row_id.0) {
                                            return Ok(false);
                                        }
                                    }
                                    if is_allowed(row_id) && seen.insert(row_id) {
                                        rows.push((row_id, exact_score));
                                        Ok(true)
                                    } else {
                                        Ok(false)
                                    }
                                },
                            )
                            .map_err(map_core_bounded_error)?;
                    } else if let Some(row_ids) = exact_rows.get(key) {
                        if supplied_seed_rows {
                            // Candidate ids and exact buckets are both ordered.
                            // Probe the bucket from the allowed directory, so
                            // excluded equal-vector history cannot turn the
                            // canonical tie correction into a complete pass.
                            for raw in seed_rows() {
                                if rows.len() == candidate_capacity {
                                    break;
                                }
                                before_source_entry()?;
                                let row_id = RowId(raw);
                                if row_ids.binary_search(&row_id).is_ok()
                                    && is_allowed(row_id)
                                    && seen.insert(row_id)
                                {
                                    rows.push((row_id, exact_score));
                                }
                            }
                        } else {
                            let mut exact_position = 0usize;
                            while exact_position < row_ids.len() && rows.len() < candidate_capacity
                            {
                                before_source_entry()?;
                                let row_id = *row_ids.get(exact_position).ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded allowed HNSW exact-row position changed"
                                            .to_string(),
                                    ))
                                })?;
                                exact_position =
                                    exact_position.checked_add(1).ok_or_else(|| {
                                        E::from(Error::Other(
                                            "bounded allowed HNSW exact-row position overflow"
                                                .to_string(),
                                        ))
                                    })?;
                                if is_allowed(row_id) && seen.insert(row_id) {
                                    rows.push((row_id, exact_score));
                                }
                            }
                        }
                    }
                    exact_bucket_completes = rows.len() == candidate_capacity;
                }

                let mut position = 0usize;
                while !exact_bucket_completes && position < raw_retained.len() {
                    before_source_entry()?;
                    let neighbour = raw_retained.get(position).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded allowed HNSW neighbour position changed".to_string(),
                        ))
                    })?;
                    position = position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded allowed HNSW neighbour position overflow".to_string(),
                        ))
                    })?;
                    let Some(row_id) = id_to_row.get(&neighbour.d_id).copied() else {
                        continue;
                    };
                    if !seen.insert(row_id) {
                        continue;
                    }
                    before_distance()?;
                    rows.push((
                        row_id,
                        self.original_query_score(&neighbour.p_id, query)
                            .unwrap_or(1.0 - neighbour.distance),
                    ));
                    if rows.len() == candidate_capacity {
                        break;
                    }
                }
                rows.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                Ok(())
            })();
            if let Some(key) = exact_key.take() {
                scratch.free_vec(key, HnswSearchScratchContainer::ExactKey);
            }
            scratch.free_set(seen, HnswSearchScratchContainer::SeenRows);
            match mapped {
                Ok(()) => Ok((rows, exact_bucket_completes)),
                Err(error) => {
                    scratch.free_vec(rows, HnswSearchScratchContainer::Result);
                    Err(error)
                }
            }
        })();
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        if let Some(encoded) = encoded_query.take() {
            scratch.free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
        }
        scratch.free_vec(raw_retained, HnswSearchScratchContainer::Result);
        let (rows, exact_bucket_completes) = map_rows?;
        let retained_bytes = match rows
            .capacity()
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
        {
            Some(bytes) => bytes,
            None => {
                scratch.free_vec(rows, HnswSearchScratchContainer::Result);
                return Err(E::from(Error::Other(
                    "bounded allowed HNSW result capacity exceeds the native address space"
                        .to_string(),
                )));
            }
        };
        let status = if exact_bucket_completes {
            HnswAllowedSearchStatus::Complete
        } else if rows.len() >= required_results {
            raw_status
        } else {
            HnswAllowedSearchStatus::Incomplete(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
            )
        };
        Ok(HnswAllowedSearchMemoryResult {
            rows,
            retained_bytes,
            status,
            visited_nodes: raw.visited_nodes,
            vector_evaluations: raw.vector_evaluations,
            allowed_admissions: raw.allowed_admissions,
        })
    }

    /// Ordinary-reader counterpart to the request-accounted allowed-id walk
    /// over ONE graph. No reader route calls it: every find over a column,
    /// however many partitions and layers it admits, is one union search.
    /// It stays as the single-graph oracle this module's own tests compare
    /// the bounded walk against.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_allowed_at_ef<F, A, R, I>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        ef_search: usize,
        seed_rows: impl Fn() -> I,
        seed_all: bool,
        is_allowed: F,
        acquire: A,
        release: R,
    ) -> Result<HnswAllowedSearchMemoryResult>
    where
        I: Iterator<Item = u64>,
        F: FnMut(RowId) -> bool,
        A: FnMut(HnswSearchScratchEvent) -> Result<()>,
        R: FnMut(HnswSearchScratchEvent),
    {
        self.search_allowed_with_bounded_memory_at_ef(
            index,
            query,
            k,
            allowed_count,
            HnswAllowedSearchLimits {
                max_visited_nodes: usize::MAX,
                max_vector_evaluations: usize::MAX,
            },
            ef_search,
            seed_rows,
            seed_all,
            is_allowed,
            || Ok::<(), Error>(()),
            || Ok::<(), Error>(()),
            acquire,
            release,
        )
    }

    #[doc(hidden)]
    pub fn raw_entry_count_for_row(&self, row_id: RowId) -> usize {
        self.id_to_row
            .read()
            .values()
            .filter(|indexed_row| **indexed_row == row_id)
            .count()
    }

    #[doc(hidden)]
    pub fn build_serial_for_test(&self) -> u64 {
        self.build_serial
    }

    #[doc(hidden)]
    pub fn graph_topology_digest_for_test(&self) -> u64 {
        match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw_topology_digest(hnsw),
            HnswInner::F32Sealed(hnsw) => hnsw_loaded_topology_digest(hnsw),
            HnswInner::QuantizedMutable(hnsw) => hnsw_topology_digest(hnsw),
            HnswInner::QuantizedSealed(hnsw) => hnsw_loaded_topology_digest(hnsw),
            HnswInner::F32Paged(hnsw) => paged_topology_digest(hnsw),
        }
    }
}

fn exact_key_for_stored_vector(vector: &StoredVector) -> Option<Vec<u8>> {
    match vector {
        StoredVector::F32(values) => Some(f32_exact_key(values)),
        StoredVector::SQ8 { .. } | StoredVector::SQ4 { .. } => {
            let encoded = vector.to_hnsw_u8();
            (!encoded.is_empty()).then_some(encoded)
        }
    }
}

fn quantization_tag(quantization: VectorQuantization) -> u8 {
    match quantization {
        VectorQuantization::F32 => 0,
        VectorQuantization::SQ8 => 1,
        VectorQuantization::SQ4 => 2,
    }
}

fn quantization_from_tag(tag: u8) -> Result<VectorQuantization> {
    match tag {
        0 => Ok(VectorQuantization::F32),
        1 => Ok(VectorQuantization::SQ8),
        2 => Ok(VectorQuantization::SQ4),
        _ => Err(Error::Other(
            "durable HNSW has an unknown quantization".to_string(),
        )),
    }
}

/// Whether a paged decode hashes its envelope, or its caller already checked
/// every byte of it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EnvelopeCheck {
    Verify,
    AuthenticatedByCaller,
}

/// The envelope checksum as a read computes it to check saved bytes; `name`
/// says which step of a load paid for the pass.
fn read_checksum(bytes: &[u8], name: &'static str) -> u64 {
    let _hashing = contextdb_core::find_attribution::stage("graph_load_checksum");
    contextdb_core::find_attribution::note_hash_pass(name, bytes.len());
    durable_checksum(bytes)
}

/// A deterministic checksum for a generation envelope.  The envelope also
/// checks every length and cross-reference before allocation/exposure; this
/// checksum catches damaged but otherwise decodable bytes.
fn durable_checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u64::try_from(value.len())
        .map_err(|_| Error::Other("durable HNSW byte length exceeds u64".to_string()))?;
    write_u64(out, length);
    out.extend_from_slice(value);
    Ok(())
}

struct DurableReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> DurableReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| Error::Other("durable HNSW envelope length overflow".to_string()))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| Error::Other("durable HNSW envelope is truncated".to_string()))?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| Error::Other("durable HNSW u16 is truncated".to_string()))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| Error::Other("durable HNSW u32 is truncated".to_string()))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| Error::Other("durable HNSW i32 is truncated".to_string()))?;
        Ok(i32::from_le_bytes(bytes))
    }

    fn read_f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.read_u32()?))
    }

    fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_u64()?))
    }

    fn read_point_id(&mut self) -> Result<hnsw_rs::hnsw::PointId> {
        Ok(hnsw_rs::hnsw::PointId(self.read_u8()?, self.read_i32()?))
    }

    fn skip(&mut self, length: usize) -> Result<()> {
        self.read_exact(length).map(|_| ())
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| Error::Other("durable HNSW u64 is truncated".to_string()))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_u64()?).map_err(|_| {
            Error::Other("durable HNSW value exceeds this platform address space".to_string())
        })
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.read_usize()?;
        self.read_exact(length)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn exact_key_for_query(query: &[f32], quantization: VectorQuantization) -> Option<(Vec<u8>, f32)> {
    let exact_score = exact_query_score(query, quantization)?;
    match quantization {
        VectorQuantization::F32 => Some((f32_exact_key(query), exact_score)),
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            let encoded = StoredVector::from_f32(query, quantization).to_hnsw_u8();
            (!encoded.is_empty()).then_some((encoded, exact_score))
        }
    }
}

fn exact_query_score(query: &[f32], quantization: VectorQuantization) -> Option<f32> {
    let score = StoredVector::from_f32(query, quantization).cosine_similarity(query);
    (score.is_finite() && score > 0.0).then_some(score)
}

fn f32_exact_key(values: &[f32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        key.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    key
}

fn hnsw_stats<T, D>(hnsw: &Hnsw<'_, T, D>) -> (usize, usize, u8)
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let indexation = hnsw.get_point_indexation();
    let layer0_neighbor_edges = indexation
        .get_layer_iterator(0)
        .map(|point| {
            point
                .get_neighborhood_id()
                .first()
                .map_or(0, |neighbors| neighbors.len())
        })
        .sum();
    (
        hnsw.get_nb_point(),
        layer0_neighbor_edges,
        hnsw.get_max_level_observed(),
    )
}

fn hnsw_loaded_stats<T, D>(hnsw: &LoadedHnsw<T, D>) -> (usize, usize, u8)
where
    T: Clone + Send + Sync + 'static,
    D: Distance<T> + Send + Sync,
{
    hnsw_stats_from_parts(
        hnsw.get_nb_point(),
        hnsw.get_max_level_observed(),
        hnsw.get_point_indexation(),
    )
}

fn hnsw_stats_from_parts<T>(
    point_count: usize,
    max_level_observed: u8,
    indexation: &hnsw_rs::hnsw::PointIndexation<'_, T>,
) -> (usize, usize, u8)
where
    T: Clone + Send + Sync,
{
    let layer0_neighbor_edges = indexation
        .get_layer_iterator(0)
        .map(|point| {
            point
                .get_neighborhood_id()
                .first()
                .map_or(0, |neighbors| neighbors.len())
        })
        .sum();
    (point_count, layer0_neighbor_edges, max_level_observed)
}

fn hnsw_topology_digest<T, D>(hnsw: &Hnsw<'_, T, D>) -> u64
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    digest_u64(&mut digest, hnsw.get_nb_point() as u64);
    digest_u64(&mut digest, hnsw.get_max_level() as u64);
    digest_u64(&mut digest, hnsw.get_max_level_observed() as u64);

    let indexation = hnsw.get_point_indexation();
    for layer in 0..hnsw.get_max_level() {
        digest_u64(&mut digest, layer as u64);
        for point in indexation.get_layer_iterator(layer) {
            let point_id = point.get_point_id();
            digest_u64(&mut digest, point.get_origin_id() as u64);
            digest_u64(&mut digest, point_id.0 as u64);
            digest_i32(&mut digest, point_id.1);
            let neighborhoods = point.get_neighborhood_id();
            for (neighbor_layer, neighbors) in neighborhoods.iter().enumerate() {
                digest_u64(&mut digest, neighbor_layer as u64);
                digest_u64(&mut digest, neighbors.len() as u64);
                for neighbor in neighbors {
                    digest_u64(&mut digest, neighbor.d_id as u64);
                    digest_u64(&mut digest, neighbor.distance.to_bits() as u64);
                    digest_u64(&mut digest, neighbor.p_id.0 as u64);
                    digest_i32(&mut digest, neighbor.p_id.1);
                }
            }
        }
    }
    digest
}

fn hnsw_loaded_topology_digest<T, D>(hnsw: &LoadedHnsw<T, D>) -> u64
where
    T: Clone + Send + Sync + 'static,
    D: Distance<T> + Send + Sync,
{
    hnsw_topology_digest_from_parts(
        hnsw.get_nb_point(),
        hnsw.get_max_level(),
        hnsw.get_max_level_observed(),
        hnsw.get_point_indexation(),
    )
}

fn paged_topology_digest(hnsw: &PagedHnsw) -> u64 {
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    digest_u64(
        &mut digest,
        hnsw.points.iter().map(Vec::len).sum::<usize>() as u64,
    );
    digest_u64(&mut digest, hnsw.points.len() as u64);
    digest_u64(
        &mut digest,
        hnsw.points
            .iter()
            .enumerate()
            .filter(|(_, points)| !points.is_empty())
            .map(|(layer, _)| layer as u8)
            .max()
            .unwrap_or(0) as u64,
    );
    for (layer, points) in hnsw.points.iter().enumerate() {
        digest_u64(&mut digest, layer as u64);
        for (rank, point) in points.iter().enumerate() {
            digest_u64(&mut digest, point.origin_id as u64);
            digest_u64(&mut digest, layer as u64);
            digest_i32(&mut digest, rank as i32);
            for (neighbour_layer, neighbours) in point.neighbours.iter().enumerate() {
                digest_u64(&mut digest, neighbour_layer as u64);
                digest_u64(&mut digest, neighbours.len() as u64);
                for neighbour in neighbours {
                    digest_u64(&mut digest, neighbour.origin_id as u64);
                    digest_u64(&mut digest, neighbour.distance.to_bits() as u64);
                    digest_u64(&mut digest, neighbour.point_id.0 as u64);
                    digest_i32(&mut digest, neighbour.point_id.1);
                }
            }
        }
    }
    digest
}

fn hnsw_topology_digest_from_parts<T>(
    point_count: usize,
    max_level: usize,
    max_level_observed: u8,
    indexation: &hnsw_rs::hnsw::PointIndexation<'_, T>,
) -> u64
where
    T: Clone + Send + Sync,
{
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    digest_u64(&mut digest, point_count as u64);
    digest_u64(&mut digest, max_level as u64);
    digest_u64(&mut digest, max_level_observed as u64);
    for layer in 0..max_level {
        digest_u64(&mut digest, layer as u64);
        for point in indexation.get_layer_iterator(layer) {
            let point_id = point.get_point_id();
            digest_u64(&mut digest, point.get_origin_id() as u64);
            digest_u64(&mut digest, point_id.0 as u64);
            digest_i32(&mut digest, point_id.1);
            let neighborhoods = point.get_neighborhood_id();
            for (neighbor_layer, neighbors) in neighborhoods.iter().enumerate() {
                digest_u64(&mut digest, neighbor_layer as u64);
                digest_u64(&mut digest, neighbors.len() as u64);
                for neighbor in neighbors {
                    digest_u64(&mut digest, neighbor.d_id as u64);
                    digest_u64(&mut digest, neighbor.distance.to_bits() as u64);
                    digest_u64(&mut digest, neighbor.p_id.0 as u64);
                    digest_i32(&mut digest, neighbor.p_id.1);
                }
            }
        }
    }
    digest
}

fn digest_i32(digest: &mut u64, value: i32) {
    digest_u64(digest, value as u32 as u64);
}

fn digest_u64(digest: &mut u64, value: u64) {
    *digest ^= value;
    *digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
}

pub(crate) fn hnsw_search_candidate_cap_for_ef(ef_search: usize, k: usize) -> usize {
    hnsw_search_candidate_cap(ef_search, k)
}

/// How many extra dispersed entry points a walk over a whole graph needs
/// beside the graph's own entry point.
///
/// A scope that admits every live point needs none: the graph is connected and
/// its entry point reaches all of it. A selective filter is different — it can
/// leave the admitted points scattered across regions one entry never reaches —
/// and the number of entries it needs scales with how little of the live set it
/// keeps: a walk that admits a fraction `allowed/live` of what it touches needs
/// about `required * live / allowed` entries to still collect `required`
/// admitted rows. The declared search breadth caps that, so a filter can never
/// buy more entries than one query's breadth.
fn dispersed_seed_budget(
    required_results: usize,
    allowed_count: usize,
    live_count: usize,
    ef: usize,
) -> usize {
    if live_count == 0 || allowed_count == 0 {
        return 0;
    }
    if allowed_count >= live_count {
        // Nothing is excluded, so the graph's own entry point reaches all of
        // it; a handful of dispersed entries beside it only guard against a
        // descent that lands in one region of a graph whose upper layers are
        // shallow, which is what `k` of them are for.
        return required_results.min(live_count);
    }
    required_results
        .saturating_mul(live_count)
        .div_ceil(allowed_count)
        .min(ef)
        .min(allowed_count)
}

fn hnsw_search_candidate_cap(ef_search: usize, k: usize) -> usize {
    hnsw_search_ef(ef_search, k).max(k)
}

fn hnsw_search_ef(ef_search: usize, k: usize) -> usize {
    ef_search.max(k).max(1)
}

fn insertion_key(entry: &StoredVectorEntry) -> u64 {
    let mut x = entry.row_id.0 ^ entry.lsn.0 ^ entry.created_tx.0;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{Lsn, TxId};

    #[test]
    fn durable_codec_capacity_bounds_all_stored_representations_and_checks_overflow() {
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            for (count, dimension, m) in [(0usize, 1, 4), (2, 9, 16), (31, 9, 24), (31, 768, 64)] {
                let entries = (0..count)
                    .map(|i| {
                        let values = (0..dimension)
                            .map(|d| ((i * 13 + d * 7) % 29) as f32 / 29.0)
                            .collect::<Vec<_>>();
                        entry_with_quantization(
                            RowId(i as u64),
                            &values,
                            TxId(i as u64 + 1),
                            quantization,
                        )
                    })
                    .collect::<Vec<_>>();
                let mut policy =
                    crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
                        .resolve_policy(count, 1);
                policy.hnsw_m = m;
                let graph = HnswIndex::new_with_policy(&entries, dimension, quantization, policy);
                let bound = HnswIndex::durable_generation_capacity_bound(
                    count,
                    dimension,
                    quantization,
                    graph.m,
                )
                .unwrap();
                let encoded = graph.encode_durable_generation().unwrap();
                assert!(encoded.len() <= encoded.capacity());
                assert!(
                    encoded.capacity() <= bound,
                    "{quantization:?} count={count} dimension={dimension} M={m}"
                );
                let decoded =
                    HnswIndex::decode_durable_generation(&encoded, dimension, quantization)
                        .unwrap();
                assert_eq!(decoded.len(), count);
            }
            assert!(
                HnswIndex::durable_generation_capacity_bound(usize::MAX, 9, quantization, 24)
                    .is_err()
            );
            assert!(
                HnswIndex::durable_generation_capacity_bound(31, usize::MAX, quantization, 24)
                    .is_err()
            );
            assert!(
                HnswIndex::durable_generation_capacity_bound(31, 9, quantization, usize::MAX)
                    .is_err()
            );
        }
    }

    #[test]
    fn prepared_insert_validation_fails_without_changing_graph_points() {
        let initial = entry(RowId(1), &[1.0, 0.0], TxId(1));
        let graph = HnswIndex::new(&[initial], 2, VectorQuantization::F32);
        let topology = graph.graph_topology_digest_for_test();
        let invalid = entry(RowId(2), &[1.0, 0.0, 0.0], TxId(2));
        assert!(graph.prepare_insert(&invalid, 1).is_err());
        assert_eq!(graph.len(), 1);
        assert_eq!(graph.graph_topology_digest_for_test(), topology);
        graph.next_id.store(usize::MAX, Ordering::SeqCst);
        let valid = entry(RowId(2), &[0.0, 1.0], TxId(2));
        assert!(graph.prepare_insert(&valid, 1).is_err());
        assert_eq!(graph.graph_topology_digest_for_test(), topology);
    }

    #[test]
    fn resolved_search_breadth_is_raised_only_to_the_requested_result_count() {
        assert_eq!(hnsw_search_ef(32, 10), 32);
        assert_eq!(hnsw_search_candidate_cap_for_ef(32, 10), 32);
        assert_eq!(hnsw_search_ef(4, 10), 10);
        assert_eq!(hnsw_search_ef(0, 0), 1);
    }

    #[test]
    fn predicate_search_without_seed_rows_still_traverses_but_absent_candidates_do_not() {
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let entries = [
                entry_with_quantization(RowId(1), &[1.0, 0.0, 0.0], TxId(1), quantization),
                entry_with_quantization(RowId(2), &[0.0, 1.0, 0.0], TxId(2), quantization),
            ];
            let graph = HnswIndex::new(&entries, 3, quantization);
            let index = VectorIndexRef::new("items", "embedding");
            let query = [0.9, 0.1, 0.0];
            let limits = HnswAllowedSearchLimits {
                max_visited_nodes: 1024,
                max_vector_evaluations: 1024,
            };
            let result = graph
                .search_allowed_with_bounded_memory(
                    &index,
                    &query,
                    1,
                    2,
                    limits,
                    |_| true,
                    || Ok::<(), Error>(()),
                    || Ok::<(), Error>(()),
                    |_| Ok::<(), Error>(()),
                    |_| {},
                )
                .unwrap();
            assert_eq!(result.rows[0].0, RowId(1));
            assert!(result.visited_nodes > 0);
            let result = graph
                .search_allowed_with_bounded_memory_at_ef(
                    &index,
                    &query,
                    1,
                    1,
                    limits,
                    32,
                    || std::iter::once(99),
                    false,
                    |row| row == RowId(99),
                    || Ok::<(), Error>(()),
                    || panic!("a layer with no allowed identity must not evaluate vectors"),
                    |_| Ok::<(), Error>(()),
                    |_| {},
                )
                .unwrap();
            assert!(result.rows.is_empty());
            assert_eq!(result.visited_nodes, 0);
        }
    }

    fn entry(row_id: RowId, vector: &[f32], tx: TxId) -> StoredVectorEntry {
        entry_with_quantization(row_id, vector, tx, VectorQuantization::F32)
    }

    fn entry_with_quantization(
        row_id: RowId,
        vector: &[f32],
        tx: TxId,
        quantization: VectorQuantization,
    ) -> StoredVectorEntry {
        StoredVectorEntry {
            row_id,
            vector: StoredVector::from_f32(vector, quantization),
            created_tx: tx,
            deleted_tx: None,
            lsn: Lsn(tx.0),
        }
    }

    #[test]
    fn mutable_graph_insertion_preserves_existing_points_and_build_identity() {
        let original = entry(RowId(11), &[1.0, 0.0, 0.0], TxId(1));
        let graph = HnswIndex::new(&[original], 3, VectorQuantization::F32);
        let build_serial = graph.build_serial_for_test();

        graph
            .insert(&entry(RowId(11), &[0.0, 1.0, 0.0], TxId(2)))
            .unwrap();
        graph
            .insert(&entry(RowId(29), &[0.0, 0.0, 1.0], TxId(3)))
            .unwrap();

        assert_eq!(graph.build_serial_for_test(), build_serial);
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.graph_stats().point_count, 3);
        assert_eq!(graph.raw_entry_count_for_row(RowId(11)), 2);
        assert_eq!(graph.raw_entry_count_for_row(RowId(29)), 1);

        let index = VectorIndexRef::new("items", "embedding");
        assert_eq!(
            graph.search(&index, &[1.0, 0.0, 0.0], 1).unwrap()[0].0,
            RowId(11)
        );
        assert_eq!(
            graph.search(&index, &[0.0, 1.0, 0.0], 1).unwrap()[0].0,
            RowId(11)
        );
        assert_eq!(
            graph.search(&index, &[0.0, 0.0, 1.0], 1).unwrap()[0].0,
            RowId(29)
        );
    }

    #[test]
    fn mutable_quantized_graphs_insert_without_rebuilding() {
        for quantization in [VectorQuantization::SQ8, VectorQuantization::SQ4] {
            let original =
                entry_with_quantization(RowId(11), &[1.0, 0.0, 0.0], TxId(1), quantization);
            let graph = HnswIndex::new(&[original], 3, quantization);
            let build_serial = graph.build_serial_for_test();

            graph
                .insert(&entry_with_quantization(
                    RowId(29),
                    &[0.0, 1.0, 0.0],
                    TxId(2),
                    quantization,
                ))
                .unwrap();

            assert_eq!(graph.build_serial_for_test(), build_serial);
            assert_eq!(graph.len(), 2);
            assert_eq!(graph.raw_entry_count_for_row(RowId(11)), 1);
            assert_eq!(graph.raw_entry_count_for_row(RowId(29)), 1);
            assert_eq!(
                graph
                    .search(
                        &VectorIndexRef::new("items", "embedding"),
                        &[0.0, 1.0, 0.0],
                        1,
                    )
                    .unwrap()[0]
                    .0,
                RowId(29)
            );
        }
    }

    #[test]
    fn one_buffer_generation_preserves_canonical_bytes_for_every_encoding() {
        use hnsw_rs::hnswio::encode_owned_graph;
        // The previous independent envelope assembly is the byte contract.
        let previous_envelope = |this: &HnswIndex| -> Result<Vec<u8>> {
            let graph = match &this.hnsw {
                HnswInner::F32Mutable(graph) => encode_owned_graph(graph),
                HnswInner::QuantizedMutable(graph) => encode_owned_graph(graph),
                HnswInner::F32Sealed(_)
                | HnswInner::QuantizedSealed(_)
                | HnswInner::F32Paged(_) => {
                    return Err(Error::Other(
                        "a sealed durable HNSW graph is already a generation and cannot be re-encoded"
                            .to_string(),
                    ));
                }
            }
            .map_err(|error| Error::Other(format!("could not encode durable HNSW graph: {error}")))?;
            let id_to_row = this.id_to_row.read();
            let mut rows = id_to_row
                .iter()
                .map(|(data_id, row_id)| (*data_id, row_id.0))
                .collect::<Vec<_>>();
            rows.sort_unstable_by_key(|(data_id, _)| *data_id);
            let exact_rows = this.exact_rows.read();
            let mut exact = exact_rows
                .iter()
                .map(|(key, row_ids)| {
                    let mut row_ids = row_ids.iter().map(|row_id| row_id.0).collect::<Vec<_>>();
                    row_ids.sort_unstable();
                    (key.clone(), row_ids)
                })
                .collect::<Vec<_>>();
            exact.sort_unstable_by(|left, right| left.0.cmp(&right.0));

            let mut payload = Vec::new();
            write_u64(&mut payload, this.dimension as u64);
            write_u8(&mut payload, quantization_tag(this.quantization));
            write_u64(&mut payload, this.ef_search as u64);
            write_bytes(&mut payload, &graph)?;
            write_u64(&mut payload, rows.len() as u64);
            for (data_id, row_id) in rows {
                write_u64(&mut payload, data_id as u64);
                write_u64(&mut payload, row_id);
            }
            write_u64(&mut payload, exact.len() as u64);
            for (key, row_ids) in exact {
                write_bytes(&mut payload, &key)?;
                write_u64(&mut payload, row_ids.len() as u64);
                for row_id in row_ids {
                    write_u64(&mut payload, row_id);
                }
            }
            let checksum = durable_checksum(&payload);
            let mut envelope = Vec::with_capacity(
                DURABLE_ENVELOPE_MAGIC
                    .len()
                    .saturating_add(2)
                    .saturating_add(8)
                    .saturating_add(payload.len()),
            );
            envelope.extend_from_slice(&DURABLE_ENVELOPE_MAGIC);
            envelope.extend_from_slice(&DURABLE_ENVELOPE_VERSION.to_le_bytes());
            write_u64(&mut envelope, checksum);
            envelope.extend_from_slice(&payload);
            Ok(envelope)
        };
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let entries = (0..192_u64)
                .map(|ordinal| {
                    let vector = (0..64)
                        .map(|component| ((ordinal % 23 + component) % 31) as f32 / 31.0)
                        .collect::<Vec<_>>();
                    entry_with_quantization(
                        RowId(192 - ordinal),
                        &vector,
                        TxId(ordinal + 1),
                        quantization,
                    )
                })
                .collect::<Vec<_>>();
            let graph = HnswIndex::new(&entries, 64, quantization);
            // Exercise insertion's sorted exact-row membership as well.
            graph
                .insert(&entry_with_quantization(
                    RowId(300),
                    &[0.25; 64],
                    TxId(300),
                    quantization,
                ))
                .unwrap();
            let expected = previous_envelope(&graph).unwrap();
            let actual = graph.encode_durable_generation().unwrap();
            assert_eq!(actual, expected, "canonical bytes for {quantization:?}");
            let loaded = HnswIndex::decode_durable_generation(&actual, 64, quantization).unwrap();
            assert_eq!(
                loaded.graph_topology_digest_for_test(),
                graph.graph_topology_digest_for_test()
            );
            assert_eq!(loaded.len(), graph.len());
        }
    }

    #[test]
    fn durable_generation_round_trip_keeps_search_maps_and_seals_the_graph() {
        let entries = [
            entry(RowId(11), &[1.0, 0.0, 0.0], TxId(1)),
            entry(RowId(29), &[0.0, 1.0, 0.0], TxId(2)),
        ];
        let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
        let topology = graph.graph_topology_digest_for_test();
        let bytes = graph.encode_durable_generation().unwrap();
        let loaded =
            HnswIndex::decode_durable_generation(&bytes, 3, VectorQuantization::F32).unwrap();

        assert_eq!(loaded.graph_topology_digest_for_test(), topology);
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded
                .search(
                    &VectorIndexRef::new("items", "embedding"),
                    &[0.0, 1.0, 0.0],
                    1
                )
                .unwrap()[0]
                .0,
            RowId(29)
        );
        assert!(
            loaded
                .insert(&entry(RowId(31), &[0.0, 0.0, 1.0], TxId(3)))
                .is_err()
        );
    }

    #[derive(Clone)]
    struct TestPageSource {
        bytes: Arc<Vec<u8>>,
        reads: Arc<Mutex<Vec<(usize, usize)>>>,
    }

    struct TestPageReader {
        bytes: Arc<Vec<u8>>,
        reads: Arc<Mutex<Vec<(usize, usize)>>>,
    }

    impl ReadOnlyVectorGraphPageSource for TestPageSource {
        fn reader(&self) -> Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
            Ok(Box::new(TestPageReader {
                bytes: Arc::clone(&self.bytes),
                reads: Arc::clone(&self.reads),
            }))
        }
    }

    impl ReadOnlyVectorGraphPageReader for TestPageReader {
        fn read_range(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> {
            self.reads.lock().push((offset, len));
            self.bytes
                .get(offset..offset.saturating_add(len))
                .map(<[u8]>::to_vec)
                .ok_or_else(|| Error::Other("test graph page is out of range".to_string()))
        }
    }

    /// Four deliberately placed points, with an upper-level A -> B -> C
    /// chain. Only C's bottom-level neighbourhood reaches the nearest D.
    /// The bytes are authored here rather than obtained from the HNSW builder,
    /// whose random layer assignment cannot guarantee this topology.
    fn two_hop_upper_descent_generation() -> Arc<Vec<u8>> {
        fn point_id(out: &mut Vec<u8>, id: PointId) {
            write_u8(out, id.0);
            out.extend_from_slice(&id.1.to_le_bytes());
        }
        fn point(
            out: &mut Vec<u8>,
            origin: u64,
            id: PointId,
            vector: [f32; 2],
            bottom: &[PointId],
            upper: &[PointId],
        ) {
            write_u64(out, origin);
            point_id(out, id);
            write_u64(out, 2);
            for value in vector {
                out.extend_from_slice(&value.to_le_bytes());
            }
            write_u64(out, 16);
            for edges in [bottom, upper] {
                write_u64(out, edges.len() as u64);
                for edge in edges {
                    let edge_origin = if edge.0 == 0 { 0 } else { edge.1 as u64 + 1 };
                    write_u64(out, edge_origin);
                    point_id(out, *edge);
                    // Saved topology validates finiteness; search measures
                    // the point vector rather than trusting this edge score.
                    out.extend_from_slice(&0.0_f32.to_le_bytes());
                }
            }
            for _ in 2..16 {
                write_u64(out, 0);
            }
        }

        let d = PointId(0, 0);
        let a = PointId(1, 0);
        let b = PointId(1, 1);
        let c = PointId(1, 2);
        let mut graph = Vec::new();
        graph.extend_from_slice(&1_u16.to_le_bytes());
        write_u64(&mut graph, 2); // M
        write_u64(&mut graph, 8); // construction breadth
        write_u8(&mut graph, 0); // extend candidates
        write_u8(&mut graph, 0); // keep pruned
        write_u64(&mut graph, 16); // layer capacity
        write_u64(&mut graph, 2); // dimension
        graph.extend_from_slice(&1.0_f64.to_le_bytes());
        write_u64(&mut graph, 16); // layer count
        write_u64(&mut graph, 1);
        point(&mut graph, 0, d, [0.99, 0.1], &[], &[]);
        write_u64(&mut graph, 3);
        point(&mut graph, 1, a, [-1.0, 0.0], &[], &[b]);
        point(&mut graph, 2, b, [0.0, 1.0], &[], &[c]);
        point(&mut graph, 3, c, [0.8, 0.6], &[d], &[]);
        for _ in 2..16 {
            write_u64(&mut graph, 0);
        }
        write_u8(&mut graph, 1);
        point_id(&mut graph, a);

        let mut envelope = Vec::new();
        envelope.extend_from_slice(&DURABLE_ENVELOPE_MAGIC);
        envelope.extend_from_slice(&DURABLE_ENVELOPE_VERSION.to_le_bytes());
        let checksum_offset = envelope.len();
        write_u64(&mut envelope, 0);
        let payload_offset = envelope.len();
        write_u64(&mut envelope, 2);
        write_u8(&mut envelope, quantization_tag(VectorQuantization::F32));
        write_u64(&mut envelope, 8);
        write_u64(&mut envelope, graph.len() as u64);
        envelope.extend_from_slice(&graph);
        write_u64(&mut envelope, 4);
        for (origin, row) in [(0, 10), (1, 11), (2, 12), (3, 13)] {
            write_u64(&mut envelope, origin);
            write_u64(&mut envelope, row);
        }
        write_u64(&mut envelope, 0); // no exact-query aliases
        let checksum = durable_checksum(&envelope[payload_offset..]);
        envelope[checksum_offset..payload_offset].copy_from_slice(&checksum.to_le_bytes());
        Arc::new(envelope)
    }

    #[test]
    fn unfiltered_upper_descent_finishes_each_level_before_searching_bottom() {
        let bytes = two_hop_upper_descent_generation();
        let resident =
            HnswIndex::decode_durable_generation(bytes.as_slice(), 2, VectorQuantization::F32)
                .unwrap();
        let cache = durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN);
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            2,
            VectorQuantization::F32,
            2,
            cache,
        )
        .unwrap();
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::new(Mutex::new(Vec::new())),
            }),
            2,
            VectorQuantization::F32,
            admitted,
            cache,
        )
        .unwrap();
        let index = VectorIndexRef::new("items", "embedding");
        let mut observed = Vec::new();
        let mut refusals = Vec::new();
        for (route, graph) in [("resident", &resident), ("saved-page", &paged)] {
            let found = graph
                .search_with_bounded_memory_at_ef::<Error, _, _, _, _>(
                    &index,
                    &[1.0, 0.0],
                    1,
                    1,
                    || Ok(()),
                    || Ok(()),
                    |_| Ok(()),
                    |_| {},
                )
                .unwrap();
            observed.push((
                route,
                found.rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            ));

            let distances = std::cell::Cell::new(0);
            let reservations = std::cell::Cell::new(0);
            let refused = graph.search_with_bounded_memory_at_ef::<Error, _, _, _, _>(
                &index,
                &[1.0, 0.0],
                1,
                1,
                || Ok(()),
                || {
                    distances.set(distances.get() + 1);
                    if distances.get() > 2 {
                        Err(Error::Other("upper descent work limit".to_string()))
                    } else {
                        Ok(())
                    }
                },
                |_| {
                    reservations.set(reservations.get() + 1);
                    Ok(())
                },
                |_| {},
            );
            refusals.push((
                route,
                matches!(refused, Err(Error::Other(message)) if message == "upper descent work limit"),
                distances.get(),
                reservations.get(),
            ));
        }
        assert_eq!(
            observed,
            [
                ("resident", vec![RowId(10)]),
                ("saved-page", vec![RowId(10)])
            ],
            "both representations must reach C and then D"
        );
        for (route, refused, distances, reservations) in refusals {
            assert!(
                refused,
                "{route}: the third upper-level distance must refuse"
            );
            assert_eq!(distances, 3, "{route}");
            assert_eq!(
                reservations,
                usize::from(route == "saved-page"),
                "{route}: refusal must precede bottom-level scratch admission"
            );
        }
    }

    #[test]
    fn zero_dimension_paged_distance_is_finite_and_reads_no_page() {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let source = Arc::new(TestPageSource {
            bytes: Arc::new(Vec::new()),
            reads: Arc::clone(&reads),
        });
        let paged = PagedHnsw {
            points: Vec::new(),
            entry_point: None,
            source,
            dimension: 0,
            max_nb_connection: 1,
            layer0_max_degree: hnsw_rs::hnsw::layer0_max_nb_connection(1),
            resident_bytes: AtomicUsize::new(0),
            graph_len: 0,
            source_page_bytes: PAGED_VECTOR_SOURCE_PAGE_BYTES,
            page_slots: 1,
            page_cache: Mutex::new(PagedVectorPageCache::new(1)),
        };
        let point = PagedPoint {
            origin_id: 0,
            vector_offset: 0,
            neighbours: Vec::new(),
        };
        let mut reader = ServingPages::new(paged.source.reader().unwrap(), 1);
        let distance = paged
            .distance(
                &point,
                &[],
                0.0,
                &mut || Ok::<_, Error>(()),
                &mut || Ok::<_, Error>(()),
                &mut |_| Ok::<_, Error>(()),
                &mut |_| {},
                reader.as_mut(),
            )
            .expect("a zero-dimensional paged point terminates");
        assert_eq!(distance, 1.0);
        assert!(distance.is_finite());
        assert!(
            reads.lock().is_empty(),
            "an empty vector has no source page to fault"
        );
    }

    /// A serving cache smaller than the vectors it serves cycles pages through
    /// itself; one large enough stops reading the source once it is warm.
    ///
    /// This is the promise the declared size exists to keep, and it runs in the
    /// default suite. Its seeded sibling
    /// (`a_graph_search_stays_inside_its_serving_cache`) is `#[ignore]`d because
    /// it builds a 1,250 x 768 store. This focused check catches a serving
    /// cache that effectively holds only one page.
    /// `decode_paged_durable_generation` takes `cache_bytes` directly, so the
    /// same property is provable here with no fixture at all.
    #[test]
    fn a_serving_cache_smaller_than_its_vectors_cycles_pages_through_itself() {
        #[derive(Default)]
        struct Pages {
            filled: std::sync::atomic::AtomicU64,
            misses: std::sync::atomic::AtomicU64,
        }
        impl PagedVectorObserver for Pages {
            fn page_filled(&self) {
                self.filled
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            fn page_looked_up(&self, hit: bool) {
                if !hit {
                    self.misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        // Wide enough that one vector is most of a page, so a handful of points
        // span several pages and a one-page cache cannot hold two of them.
        const DIMENSION: usize = PAGED_VECTOR_SOURCE_PAGE_BYTES / 8;
        const POINTS: u64 = 8;
        let vectors = (1..=POINTS)
            .map(|ordinal| {
                (0..DIMENSION)
                    .map(|coordinate| (((coordinate as u64 + ordinal) % 977) as f32 / 977.0) - 0.5)
                    .collect::<Vec<f32>>()
            })
            .collect::<Vec<_>>();
        let entries = vectors
            .iter()
            .enumerate()
            .map(|(index, vector)| entry(RowId(index as u64 + 1), vector, TxId(index as u64 + 1)))
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let span = bytes.len();
        let query = vectors[0].clone();

        let sweep = |cache_bytes: usize| -> (u64, u64) {
            let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
                bytes.as_slice(),
                DIMENSION,
                VectorQuantization::F32,
                graph.m,
                cache_bytes,
            )
            .unwrap();
            let paged = HnswIndex::decode_paged_durable_generation(
                bytes.as_slice(),
                Arc::new(TestPageSource {
                    bytes: Arc::clone(&bytes),
                    reads: Arc::new(Mutex::new(Vec::new())),
                }),
                DIMENSION,
                VectorQuantization::F32,
                admitted,
                cache_bytes,
            )
            .unwrap();
            let HnswInner::F32Paged(inner) = &paged.hnsw else {
                panic!("a decoded paged generation serves from pages");
            };
            let pages = Arc::new(Pages::default());
            let right_norm = PagedHnsw::query_norm(&query);
            with_paged_vector_observer(pages.clone(), || {
                let mut reader = ServingPages::new(inner.source.reader().unwrap(), 1);
                // Two passes: the second one is where a cache that holds the
                // whole span stops reading the source and a smaller one does not.
                for _ in 0..2 {
                    // Layer zero holds every point; the upper layers are a
                    // subset, so this visits the whole vector span.
                    for rank in 0..POINTS as i32 {
                        let Some(point) = inner.point(PointId(0, rank)) else {
                            continue;
                        };
                        inner
                            .distance(
                                point,
                                &query,
                                right_norm,
                                &mut || Ok::<_, Error>(()),
                                &mut || Ok::<_, Error>(()),
                                &mut |_| Ok::<_, Error>(()),
                                &mut |_| {},
                                reader.as_mut(),
                            )
                            .expect("a decoded point scores");
                    }
                }
            });
            (
                pages.filled.load(std::sync::atomic::Ordering::Relaxed),
                pages.misses.load(std::sync::atomic::Ordering::Relaxed),
            )
        };

        let (roomy_fills, _) = sweep(span.next_multiple_of(PAGED_VECTOR_SOURCE_PAGE_BYTES));
        let (tight_fills, _) = sweep(PAGED_VECTOR_SOURCE_PAGE_BYTES);
        assert!(
            tight_fills > roomy_fills,
            "a one-page cache over a {span}-byte span must re-read what it evicted: \
             {tight_fills} source reads against {roomy_fills} with room for the whole span"
        );
    }

    /// A search takes the serving cache's lock once per PAGE it resolves, not
    /// once per vector it scores -- and a cache too small to hold those pages
    /// takes it again, because the search's own directory keeps no page alive.
    /// What a paused directory still allocates is counted page by page in
    /// `tests/concurrent_searches_stay_inside_their_serving_cache.rs`.
    ///
    /// This runs in the default suite on purpose. Its seeded sibling
    /// (`a_find_locks_its_serving_cache_once_per_page` in contextdb-engine)
    /// needs a 1,250 x 768 store, so the property would otherwise be proved
    /// only by something nothing runs.
    #[test]
    fn a_search_locks_its_serving_cache_once_per_page_it_resolves() {
        #[derive(Default)]
        struct Locks {
            taken: std::sync::atomic::AtomicU64,
        }
        impl PagedVectorObserver for Locks {
            fn page_cache_locked(&self) {
                self.taken
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Narrow enough that a page holds many vectors, so scoring them all
        // resolves a handful of pages and visits them hundreds of times.
        const DIMENSION: usize = 128;
        const POINTS: i32 = 256;
        let vectors = (1..=POINTS as u64)
            .map(|ordinal| {
                (0..DIMENSION)
                    .map(|coordinate| (((coordinate as u64 + ordinal) % 977) as f32 / 977.0) - 0.5)
                    .collect::<Vec<f32>>()
            })
            .collect::<Vec<_>>();
        let entries = vectors
            .iter()
            .enumerate()
            .map(|(index, vector)| entry(RowId(index as u64 + 1), vector, TxId(index as u64 + 1)))
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let span = bytes.len();
        let query = vectors[0].clone();

        // Ranks in an order that alternates between the two halves of the
        // span, so a cache with room for one page really does evict between
        // consecutive visits rather than running down one page at a time.
        let order = (0..POINTS / 2).flat_map(|rank| [rank, rank + POINTS / 2]);

        let sweep = |cache_bytes: usize, directory: bool| -> (u64, usize, Vec<f32>) {
            let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
                bytes.as_slice(),
                DIMENSION,
                VectorQuantization::F32,
                graph.m,
                cache_bytes,
            )
            .unwrap();
            let paged = HnswIndex::decode_paged_durable_generation(
                bytes.as_slice(),
                Arc::new(TestPageSource {
                    bytes: Arc::clone(&bytes),
                    reads: Arc::new(Mutex::new(Vec::new())),
                }),
                DIMENSION,
                VectorQuantization::F32,
                admitted,
                cache_bytes,
            )
            .unwrap();
            let HnswInner::F32Paged(inner) = &paged.hnsw else {
                panic!("a decoded paged generation serves from pages");
            };
            let locks = Arc::new(Locks::default());
            let right_norm = PagedHnsw::query_norm(&query);
            let mut scores = Vec::new();
            let mut visits = 0usize;
            with_paged_vector_observer(locks.clone(), || {
                let mut reader = ServingPages::new(
                    inner.source.reader().unwrap(),
                    if directory { inner.page_slots } else { 0 },
                );
                // Twice, so the second pass is over pages the search has
                // already resolved.
                for _ in 0..2 {
                    for rank in order.clone() {
                        let Some(point) = inner.point(PointId(0, rank)) else {
                            continue;
                        };
                        visits += 1;
                        scores.push(
                            inner
                                .distance(
                                    point,
                                    &query,
                                    right_norm,
                                    &mut || Ok::<_, Error>(()),
                                    &mut || Ok::<_, Error>(()),
                                    &mut |_| Ok::<_, Error>(()),
                                    &mut |_| {},
                                    reader.as_mut(),
                                )
                                .expect("a decoded point scores"),
                        );
                    }
                }
            });
            (
                locks.taken.load(std::sync::atomic::Ordering::Relaxed),
                visits,
                scores,
            )
        };

        let roomy = span.next_multiple_of(PAGED_VECTOR_SOURCE_PAGE_BYTES);
        let distinct_pages = roomy / PAGED_VECTOR_SOURCE_PAGE_BYTES;
        let (with_directory, visits, scored) = sweep(roomy, true);
        let (without_directory, _, unchanged) = sweep(roomy, false);
        let (evicting, _, evicted_and_reread) = sweep(PAGED_VECTOR_SOURCE_PAGE_BYTES, true);

        assert_eq!(
            scored, unchanged,
            "how often a search locks its serving cache does not change what it scores"
        );
        assert_eq!(
            scored, evicted_and_reread,
            "a page re-read after eviction scores what it scored before"
        );
        // One lock to find the page and one to install it, for each page.
        assert!(
            with_directory <= 2 * distinct_pages as u64,
            "{visits} visits over {distinct_pages} pages took the serving-cache lock              {with_directory} times; resolving each page once is at most {} for this span",
            2 * distinct_pages
        );
        assert!(
            without_directory >= visits as u64,
            "without a page directory the lock is taken per visit ({without_directory} over              {visits} visits); a bound that holds either way is not proving anything"
        );
        assert!(
            evicting > with_directory,
            "a cache with room for one page of a {span}-byte span must go back to the cache              for pages it evicted: {evicting} acquisitions against {with_directory} with room              for the whole span means the directory is pinning pages the cache gave up"
        );
    }

    /// A caller's scratch allowance for bounded searches, kept the way an
    /// engine caller keeps it: a fixed allowance, what is charged against it,
    /// and what each container holds. It can refuse one chosen reservation,
    /// and it refuses any reservation that would overdraw the allowance --
    /// so a charge a failed search forgot to give back is found by the next
    /// search on the same allowance being refused.
    struct ScratchAllowance {
        allowance: isize,
        held: std::cell::Cell<isize>,
        peak: std::cell::Cell<isize>,
        by_container: std::cell::RefCell<Vec<(HnswSearchScratchContainer, isize)>>,
        went_negative: std::cell::Cell<bool>,
        reservations: std::cell::Cell<usize>,
        refuse_reservation: std::cell::Cell<Option<usize>>,
    }

    impl ScratchAllowance {
        fn new(allowance: isize) -> Self {
            Self {
                allowance,
                held: std::cell::Cell::new(0),
                peak: std::cell::Cell::new(0),
                by_container: std::cell::RefCell::new(Vec::new()),
                went_negative: std::cell::Cell::new(false),
                reservations: std::cell::Cell::new(0),
                refuse_reservation: std::cell::Cell::new(None),
            }
        }

        fn charge(&self, container: HnswSearchScratchContainer, bytes: isize) {
            self.held.set(self.held.get() + bytes);
            self.peak.set(self.peak.get().max(self.held.get()));
            let mut by_container = self.by_container.borrow_mut();
            let slot = match by_container.iter().position(|(held, _)| *held == container) {
                Some(slot) => slot,
                None => {
                    by_container.push((container, 0));
                    by_container.len() - 1
                }
            };
            by_container[slot].1 += bytes;
            if by_container[slot].1 < 0 || self.held.get() < 0 {
                self.went_negative.set(true);
            }
        }

        fn acquire(&self, event: HnswSearchScratchEvent) -> Result<()> {
            let (container, bytes) = match event {
                HnswSearchScratchEvent::Reserve {
                    container,
                    previous_capacity,
                    requested_capacity,
                    element_bytes,
                } => {
                    let reservation = self.reservations.get() + 1;
                    self.reservations.set(reservation);
                    if self.refuse_reservation.get() == Some(reservation) {
                        return Err(Error::Other(format!("refused reservation {reservation}")));
                    }
                    (
                        container,
                        ((requested_capacity - previous_capacity) * element_bytes) as isize,
                    )
                }
                HnswSearchScratchEvent::Reconcile {
                    container,
                    requested_capacity,
                    actual_capacity,
                    element_bytes,
                } => (
                    container,
                    (actual_capacity.saturating_sub(requested_capacity) * element_bytes) as isize,
                ),
                HnswSearchScratchEvent::Release { .. } => {
                    panic!("a release sent through the acquire callback")
                }
            };
            if self.held.get() + bytes > self.allowance {
                return Err(Error::Other("over the declared allowance".to_string()));
            }
            self.charge(container, bytes);
            Ok(())
        }

        fn release(&self, event: HnswSearchScratchEvent) {
            let HnswSearchScratchEvent::Release {
                container,
                capacity,
                element_bytes,
            } = event
            else {
                panic!("an acquire sent through the release callback");
            };
            self.charge(container, -((capacity * element_bytes) as isize));
        }

        /// The caller drops a returned answer, giving back its storage.
        fn drop_answer(&self, retained: usize) {
            self.charge(HnswSearchScratchContainer::Result, -(retained as isize));
        }

        fn assert_square(&self, what: &str) {
            assert!(
                !self.went_negative.get(),
                "{what}: something was given back that was never charged, or given back twice"
            );
            assert_eq!(
                self.held.get(),
                0,
                "{what}: charges still held {:?}",
                self.by_container
            );
            assert!(
                self.by_container
                    .borrow()
                    .iter()
                    .all(|(_, held)| *held == 0),
                "{what}: a container is still charged {:?}",
                self.by_container
            );
        }
    }

    fn paged_proof_generation(population: usize, dimension: usize) -> HnswIndex {
        let entries = (0..population)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &paged_proof_vector(ordinal, dimension),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, dimension, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let ceiling = durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN);
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            dimension,
            VectorQuantization::F32,
            graph.m,
            ceiling,
        )
        .unwrap();
        HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::new(Mutex::new(Vec::new())),
            }),
            dimension,
            VectorQuantization::F32,
            admitted,
            ceiling,
        )
        .unwrap()
    }

    /// Refuse every reservation a search makes, one at a time, then fail every
    /// distance it takes, one at a time; after each, every charge the search
    /// acquired must have come back exactly once, the caller must see the
    /// error it raised, and a following search on the same allowance -- sized
    /// to what one clean search needs at its peak -- must answer in full.
    fn a_refused_or_cancelled_search_gives_back_all_it_was_charged(
        route: &str,
        search: &dyn Fn(&ScratchAllowance, Option<usize>) -> Result<(Vec<(RowId, f32)>, usize)>,
    ) {
        // Warm the serving cache, so every run below makes the same
        // reservations in the same order.
        let warm = ScratchAllowance::new(isize::MAX);
        let (_, retained) = search(&warm, None).expect("the route answers");
        warm.drop_answer(retained);

        let clean = ScratchAllowance::new(isize::MAX);
        let distances = std::cell::Cell::new(0_usize);
        let counted = |allowance: &ScratchAllowance, fail_at: Option<usize>| {
            distances.set(0);
            search(allowance, fail_at)
        };
        let (rows, retained) = counted(&clean, None).expect("the route answers");
        clean.drop_answer(retained);
        clean.assert_square(&format!("{route}: a clean search"));
        let reservations = clean.reservations.get();
        let allowance = clean.peak.get();
        assert!(
            reservations >= 4,
            "{route}: {reservations} reservations is too few to prove"
        );

        let follow_on = |allowance_state: &ScratchAllowance, what: &str| {
            allowance_state.refuse_reservation.set(None);
            let (again, retained) = search(allowance_state, None).unwrap_or_else(|error| {
                panic!(
                    "{what}: the next search on the same allowance was refused ({error:?}); \
                     the failed one kept a charge it never gave back"
                )
            });
            assert_eq!(again, rows, "{what}: the next search answers in full");
            allowance_state.drop_answer(retained);
            allowance_state.assert_square(&format!("{what}, then a clean search"));
        };

        for refuse in 1..=reservations {
            let what = format!("{route}: refusing reservation {refuse} of {reservations}");
            let state = ScratchAllowance::new(allowance);
            state.refuse_reservation.set(Some(refuse));
            let error = search(&state, None).expect_err(&what);
            assert!(
                format!("{error:?}").contains(&format!("refused reservation {refuse}")),
                "{what}: the caller sees the refusal it raised, not {error:?}"
            );
            state.assert_square(&what);
            follow_on(&state, &what);
        }

        let taken = SEARCH_DISTANCES.with(|count| count.get());
        assert!(
            taken > 20,
            "{route}: {taken} distances is too short a walk to prove"
        );
        for fail_at in 1..=taken {
            let what = format!("{route}: cancelling at distance {fail_at} of {taken}");
            let state = ScratchAllowance::new(allowance);
            let error = search(&state, Some(fail_at)).expect_err(&what);
            assert!(
                format!("{error:?}").contains(&format!("cancelled at distance {fail_at}")),
                "{what}: the caller sees the cancellation it raised, not {error:?}"
            );
            state.assert_square(&what);
            follow_on(&state, &what);
        }
    }

    std::thread_local! {
        /// Distances the most recent uncancelled proof search took.
        static SEARCH_DISTANCES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    fn cancelling_at(fail_at: Option<usize>) -> impl FnMut() -> Result<()> {
        let mut taken = 0_usize;
        move || {
            taken += 1;
            if fail_at.is_none() {
                SEARCH_DISTANCES.with(|count| count.set(taken));
            }
            if Some(taken) == fail_at {
                return Err(Error::Other(format!("cancelled at distance {taken}")));
            }
            Ok(())
        }
    }

    /// The unfiltered paged walk returns every scratch charge on every exit.
    #[test]
    fn an_unfiltered_search_refused_or_cancelled_gives_back_all_it_was_charged() {
        let paged = paged_proof_generation(1_250, 768);
        let index = VectorIndexRef::new("items", "embedding");
        let query = paged_proof_vector(1_263, 768);
        a_refused_or_cancelled_search_gives_back_all_it_was_charged(
            "unfiltered",
            &|allowance, fail_at| {
                paged
                    .search_with_bounded_memory::<Error, _, _, _, _>(
                        &index,
                        &query,
                        10,
                        || Ok(()),
                        cancelling_at(fail_at),
                        |event| allowance.acquire(event),
                        |event| allowance.release(event),
                    )
                    .map(|answered| (answered.rows, answered.retained_bytes))
            },
        );
    }

    /// The filtered paged walk returns every scratch charge on every exit,
    /// including a refusal of any one of its reservations after others were
    /// admitted.
    #[test]
    fn a_filtered_search_refused_or_cancelled_gives_back_all_it_was_charged() {
        let paged = paged_proof_generation(1_250, 768);
        let index = VectorIndexRef::new("items", "embedding");
        let query = paged_proof_vector(1_263, 768);
        a_refused_or_cancelled_search_gives_back_all_it_was_charged(
            "filtered",
            &|allowance, fail_at| {
                paged
                    .search_allowed_with_bounded_memory::<Error, _, _, _, _, _>(
                        &index,
                        &query,
                        10,
                        1_250,
                        HnswAllowedSearchLimits {
                            max_visited_nodes: usize::MAX,
                            max_vector_evaluations: usize::MAX,
                        },
                        |_| true,
                        || Ok(()),
                        cancelling_at(fail_at),
                        |event| allowance.acquire(event),
                        |event| allowance.release(event),
                    )
                    .map(|answered| (answered.rows, answered.retained_bytes))
            },
        );
    }

    /// A cache larger than the partition it serves is memory reserved for
    /// slots that can never fill, so the partition bounds every leg of the
    /// ceiling -- including a session limit and a size the reader was
    /// handed.
    #[test]
    fn a_serving_cache_is_never_sized_past_the_partition_it_serves() {
        let partition = 8 * PAGED_VECTOR_SOURCE_PAGE_BYTES;
        // A generous declaration does not buy slots the partition cannot fill.
        assert_eq!(
            durable_reader_cache_ceiling(None, Some(4 * 1024 * 1024 * 1024), partition),
            partition
        );
        assert_eq!(
            durable_reader_cache_ceiling(Some(1024 * 1024 * 1024), None, partition),
            partition
        );
        // The session limit bounds the ceiling directly. The graph loader
        // charges what it actually admits for this generation.
        assert_eq!(
            durable_reader_cache_ceiling(None, Some(16 * 1024 * 1024), partition),
            partition,
            "a session limit above the partition lands on the partition"
        );
        let small = 4 * PAGED_VECTOR_SOURCE_PAGE_BYTES;
        assert_eq!(
            durable_reader_cache_ceiling(None, Some(small), partition),
            small
        );
        assert_eq!(
            durable_reader_cache_ceiling(Some(small), None, partition),
            small,
            "a declared size below the partition is honoured"
        );
        // Never below one page, whatever the partition or the declaration say.
        assert_eq!(
            durable_reader_cache_ceiling(Some(1), None, 0),
            PAGED_VECTOR_SOURCE_PAGE_BYTES
        );
    }

    /// A score read out of the serving pages is the score the row's own body
    /// produces -- bit for bit, not nearly.
    ///
    /// This is the single claim the page-scoring route rests on: the graph
    /// route resolves its candidates from pages, and the value it publishes
    /// has to be the one the per-row route would have published for the same
    /// vector. `stored_similarity` sums an f32 dot and an f32 stored norm in
    /// coordinate order and divides by `sqrt(query_norm) * sqrt(stored_norm)`,
    /// which is `cosine_from_iter`'s arithmetic exactly -- equal by
    /// construction, so it is pinned here rather than left to be equal by
    /// luck. Runs in the default suite: no fixture, no file, no seeding.
    #[test]
    fn a_page_scored_vector_matches_the_score_its_own_body_gives() {
        // Three lengths on purpose: one well inside a page, one that ends
        // exactly on a page boundary, and one that straddles two pages, since
        // the straddle is where the carry arithmetic lives.
        for dimension in [
            96_usize,
            PAGED_VECTOR_SOURCE_PAGE_BYTES / 4,
            PAGED_VECTOR_SOURCE_PAGE_BYTES / 4 + 129,
        ] {
            let rows = (1..=5_u64)
                .map(|ordinal| {
                    (
                        RowId(ordinal),
                        (0..dimension)
                            .map(|coordinate| {
                                (((coordinate as u64 * 31 + ordinal * 17) % 1021) as f32 / 1021.0)
                                    - 0.5
                            })
                            .collect::<Vec<f32>>(),
                    )
                })
                .collect::<Vec<_>>();
            let query = (0..dimension)
                .map(|coordinate| (((coordinate * 23 + 5) % 1013) as f32 / 1013.0) - 0.5)
                .collect::<Vec<_>>();
            let entries = rows
                .iter()
                .map(|(row_id, vector)| entry(*row_id, vector, TxId(row_id.0)))
                .collect::<Vec<_>>();
            let graph = HnswIndex::new(&entries, dimension, VectorQuantization::F32);
            let bytes = Arc::new(graph.encode_durable_generation().unwrap());
            let ceiling = durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN);
            let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
                bytes.as_slice(),
                dimension,
                VectorQuantization::F32,
                graph.m,
                ceiling,
            )
            .unwrap();
            let paged = HnswIndex::decode_paged_durable_generation(
                bytes.as_slice(),
                Arc::new(TestPageSource {
                    bytes: Arc::clone(&bytes),
                    reads: Arc::new(Mutex::new(Vec::new())),
                }),
                dimension,
                VectorQuantization::F32,
                admitted,
                ceiling,
            )
            .unwrap();

            let query_norm = HnswIndex::stored_query_norm(&query);
            let mut reader = paged
                .serving_page_reader()
                .expect("a paged generation serves from pages")
                .unwrap();
            for (row_id, vector) in &rows {
                let from_pages = paged
                    .serving_page_similarity::<Error, _, _, _, _>(
                        *row_id,
                        &query,
                        query_norm,
                        &mut || Ok(()),
                        &mut || Ok(()),
                        &mut |_| Ok(()),
                        &mut |_| {},
                        reader.as_mut(),
                    )
                    .expect("the page route scores a row the generation holds")
                    .expect("the generation holds this row");
                let from_body = crate::quantized::StoredVector::F32(vector.clone().into())
                    .cosine_similarity(&query);
                assert_eq!(
                    from_pages.to_bits(),
                    from_body.to_bits(),
                    "dimension {dimension}, {row_id:?}: a page-scored vector must publish the \
                     same number its own stored body publishes"
                );
            }
        }
    }

    fn exact_cosine_distance(left: &[f32], right: &[f32]) -> f32 {
        let mut dot = 0.0_f64;
        let mut left_norm = 0.0_f64;
        let mut right_norm = 0.0_f64;
        for (&left, &right) in left.iter().zip(right) {
            dot += f64::from(left) * f64::from(right);
            left_norm += f64::from(left) * f64::from(left);
            right_norm += f64::from(right) * f64::from(right);
        }
        (1.0 - dot / (left_norm.sqrt() * right_norm.sqrt())) as f32
    }

    #[test]
    fn paged_wide_vector_scores_every_middle_page_and_partial_tail() {
        const DIMENSION: usize = PAGED_VECTOR_SOURCE_PAGE_BYTES * 2 / 4 + 257;
        let vector = (0..DIMENSION)
            .map(|coordinate| ((coordinate * 37 + 11) % 997) as f32 / 997.0)
            .collect::<Vec<_>>();
        let query = (0..DIMENSION)
            .map(|coordinate| ((coordinate * 19 + 7) % 991) as f32 / 991.0)
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(
            &[entry(RowId(1), &vector, TxId(1))],
            DIMENSION,
            VectorQuantization::F32,
        );
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::clone(&reads),
            }),
            DIMENSION,
            VectorQuantization::F32,
            admitted,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        let HnswInner::F32Paged(paged) = &paged.hnsw else {
            panic!("the retained generation must use the paged representation");
        };
        let point = paged.points.iter().flatten().next().unwrap();
        let vector_end = point.vector_offset + DIMENSION * std::mem::size_of::<f32>();
        let spanned_pages = (vector_end - 1) / PAGED_VECTOR_SOURCE_PAGE_BYTES
            - point.vector_offset / PAGED_VECTOR_SOURCE_PAGE_BYTES
            + 1;
        assert!(
            spanned_pages >= 3,
            "the proof vector must span at least three pages"
        );
        assert_ne!(
            DIMENSION * std::mem::size_of::<f32>() % PAGED_VECTOR_SOURCE_PAGE_BYTES,
            0,
            "the proof vector must end in a partial source page"
        );
        let mut reader = ServingPages::new(paged.source.reader().unwrap(), 1);
        let actual = paged
            .distance(
                point,
                &query,
                PagedHnsw::query_norm(&query),
                &mut || Ok::<_, Error>(()),
                &mut || Ok::<_, Error>(()),
                &mut |_| Ok::<_, Error>(()),
                &mut |_| {},
                reader.as_mut(),
            )
            .unwrap();
        let expected = exact_cosine_distance(&vector, &query);
        assert!(
            (actual - expected).abs() <= 1.0e-6,
            "paged score {actual} must equal the full stored-coordinate score {expected}"
        );
        assert!(
            reads.lock().len() >= spanned_pages,
            "every source page crossed by the vector must be read"
        );
    }

    #[cfg(feature = "test-seams")]
    #[derive(Default)]
    struct PagedVisitCounters {
        cache_locks: AtomicUsize,
        page_fills: AtomicUsize,
        page_lookups: AtomicUsize,
        page_hits: AtomicUsize,
        vector_decodes: AtomicUsize,
        query_norms: AtomicUsize,
    }

    #[cfg(feature = "test-seams")]
    impl PagedVectorObserver for PagedVisitCounters {
        fn page_cache_locked(&self) {
            self.cache_locks.fetch_add(1, Ordering::SeqCst);
        }

        fn page_filled(&self) {
            self.page_fills.fetch_add(1, Ordering::SeqCst);
        }

        fn page_looked_up(&self, hit: bool) {
            self.page_lookups.fetch_add(1, Ordering::SeqCst);
            if hit {
                self.page_hits.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn vector_decoded(&self) {
            self.vector_decodes.fetch_add(1, Ordering::SeqCst);
        }

        fn query_norm_computed(&self) {
            self.query_norms.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn paged_proof_vector(ordinal: usize, dimension: usize) -> Vec<f32> {
        (0..dimension)
            .map(|coordinate| {
                let seed = (ordinal
                    .wrapping_mul(0x9e37_79b9)
                    .wrapping_add(coordinate.wrapping_mul(0x85eb_ca6b)))
                    % 10_007;
                (seed as f32 / 10_007.0) - 0.5
            })
            .collect()
    }

    /// A visit to a point whose page is already resident must pay one distance
    /// evaluation and nothing else. It must not take a fresh serving-cache
    /// lock, scan the serving slots, or recompute the query norm for each point.
    /// A second search over the same durable generation must not wait for the
    /// first one's page reads. The serving cache is a directory, not a place
    /// to do work: it is locked to find a page and to make one resident, never
    /// while a page is being read from the snapshot source, decoded or scored.
    #[cfg(feature = "test-seams")]
    #[test]
    fn a_page_read_does_not_stop_another_search_over_the_same_generation() {
        use std::sync::atomic::AtomicBool;
        use std::time::Duration;

        const DIMENSION: usize = 768;
        const POPULATION: usize = 600;

        #[derive(Default)]
        struct Rendezvous {
            reached: parking_lot::Mutex<(bool, bool)>,
            changed: parking_lot::Condvar,
        }

        impl Rendezvous {
            fn announce_parked(&self) {
                self.reached.lock().0 = true;
                self.changed.notify_all();
            }

            fn release(&self) {
                self.reached.lock().1 = true;
                self.changed.notify_all();
            }

            fn wait_for(&self, parked: bool, what: &str) {
                let mut reached = self.reached.lock();
                loop {
                    if (parked && reached.0) || (!parked && reached.1) {
                        return;
                    }
                    assert!(
                        !self
                            .changed
                            .wait_for(&mut reached, Duration::from_secs(30))
                            .timed_out(),
                        "waiting for {what} timed out, which is what a serialized page \\
                         cache looks like"
                    );
                }
            }
        }

        /// Parks the first page read this thread performs until the other
        /// search says it is finished.
        struct ParkOnFirstFill {
            rendezvous: Arc<Rendezvous>,
            parked: AtomicBool,
        }

        impl PagedVectorObserver for ParkOnFirstFill {
            fn page_filled(&self) {
                if self.parked.swap(true, Ordering::SeqCst) {
                    return;
                }
                self.rendezvous.announce_parked();
                self.rendezvous
                    .wait_for(false, "the other search to finish");
            }
        }

        let entries = (0..POPULATION)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &paged_proof_vector(ordinal, DIMENSION),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        let build = || {
            HnswIndex::decode_paged_durable_generation(
                bytes.as_slice(),
                Arc::new(TestPageSource {
                    bytes: Arc::clone(&bytes),
                    reads: Arc::new(Mutex::new(Vec::new())),
                }),
                DIMENSION,
                VectorQuantization::F32,
                admitted,
                durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
            )
            .unwrap()
        };
        let query = paged_proof_vector(POPULATION + 3, DIMENSION);
        let search = |paged: &HnswIndex| {
            paged
                .search_allowed_with_bounded_memory_at_ef(
                    &VectorIndexRef::new("items", "embedding"),
                    &query,
                    10,
                    POPULATION,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    100,
                    std::iter::empty,
                    true,
                    |_| true,
                    || Ok::<_, Error>(()),
                    || Ok::<_, Error>(()),
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
                .expect("the paged allowed walk completes")
        };

        let alone = search(&build());

        let shared = Arc::new(build());
        let rendezvous = Arc::new(Rendezvous::default());
        let parked_search = {
            let shared = Arc::clone(&shared);
            let rendezvous = Arc::clone(&rendezvous);
            let query = query.clone();
            std::thread::spawn(move || {
                let observer = Arc::new(ParkOnFirstFill {
                    rendezvous,
                    parked: AtomicBool::new(false),
                });
                with_paged_vector_observer(observer, || {
                    shared
                        .search_allowed_with_bounded_memory_at_ef(
                            &VectorIndexRef::new("items", "embedding"),
                            &query,
                            10,
                            POPULATION,
                            HnswAllowedSearchLimits {
                                max_visited_nodes: usize::MAX,
                                max_vector_evaluations: usize::MAX,
                            },
                            100,
                            std::iter::empty,
                            true,
                            |_| true,
                            || Ok::<_, Error>(()),
                            || Ok::<_, Error>(()),
                            |_| Ok::<_, Error>(()),
                            |_| {},
                        )
                        .expect("the parked walk completes once it is released")
                })
            })
        };

        rendezvous.wait_for(true, "the first search to park inside a page read");
        let concurrent = search(&shared);
        rendezvous.release();
        let parked = parked_search.join().expect("the parked search finished");

        assert_eq!(
            concurrent.rows, alone.rows,
            "a search that ran while another held a page answers the same rows"
        );
        assert_eq!(
            parked.rows, alone.rows,
            "the parked search answers the same rows once it resumes"
        );
        let offsets = shared.resident_page_offsets_for_test();
        let mut distinct = offsets.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            offsets.len(),
            distinct.len(),
            "two searches that missed the same page leave one copy resident, not two"
        );
    }

    /// A page whose read did not produce what was asked for is never made
    /// resident: the next search reads it again rather than scoring whatever
    /// the failed read left behind.
    #[cfg(feature = "test-seams")]
    #[test]
    fn a_failed_page_read_leaves_nothing_resident() {
        const DIMENSION: usize = 768;
        const POPULATION: usize = 200;

        struct ShortReadSource {
            bytes: Arc<Vec<u8>>,
            fail: Arc<Mutex<bool>>,
        }

        struct ShortReadReader {
            bytes: Arc<Vec<u8>>,
            fail: Arc<Mutex<bool>>,
        }

        impl ReadOnlyVectorGraphPageSource for ShortReadSource {
            fn reader(&self) -> Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
                Ok(Box::new(ShortReadReader {
                    bytes: Arc::clone(&self.bytes),
                    fail: Arc::clone(&self.fail),
                }))
            }
        }

        impl ReadOnlyVectorGraphPageReader for ShortReadReader {
            fn read_range(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> {
                let mut fail = self.fail.lock();
                let short = *fail;
                *fail = false;
                drop(fail);
                let end = offset.saturating_add(if short { len.saturating_sub(1) } else { len });
                self.bytes
                    .get(offset..end.min(self.bytes.len()))
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| Error::Other("test graph page is out of range".to_string()))
            }
        }

        let entries = (0..POPULATION)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &paged_proof_vector(ordinal, DIMENSION),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        let fail = Arc::new(Mutex::new(true));
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(ShortReadSource {
                bytes: Arc::clone(&bytes),
                fail: Arc::clone(&fail),
            }),
            DIMENSION,
            VectorQuantization::F32,
            admitted,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        let query = paged_proof_vector(POPULATION + 5, DIMENSION);
        let refused = paged.search_allowed_with_bounded_memory_at_ef(
            &VectorIndexRef::new("items", "embedding"),
            &query,
            10,
            POPULATION,
            HnswAllowedSearchLimits {
                max_visited_nodes: usize::MAX,
                max_vector_evaluations: usize::MAX,
            },
            100,
            std::iter::empty,
            true,
            |_| true,
            || Ok::<_, Error>(()),
            || Ok::<_, Error>(()),
            |_| Ok::<_, Error>(()),
            |_| {},
        );
        assert!(
            refused.is_err(),
            "a page read that returned the wrong number of bytes is refused"
        );
        assert!(
            paged.resident_page_offsets_for_test().is_empty(),
            "a refused page read leaves nothing resident"
        );

        let answered = paged
            .search_allowed_with_bounded_memory_at_ef(
                &VectorIndexRef::new("items", "embedding"),
                &query,
                10,
                POPULATION,
                HnswAllowedSearchLimits {
                    max_visited_nodes: usize::MAX,
                    max_vector_evaluations: usize::MAX,
                },
                100,
                std::iter::empty,
                true,
                |_| true,
                || Ok::<_, Error>(()),
                || Ok::<_, Error>(()),
                |_| Ok::<_, Error>(()),
                |_| {},
            )
            .expect("the next search reads the page again and answers");
        assert_eq!(answered.rows.len(), 10);
    }

    /// Reaching a point costs the same whether a walk has reached ten points
    /// or ten thousand.
    ///
    /// A walk asks "have I already reached this point" once for every graph
    /// edge it considers, so a membership test that scans what it has already
    /// reached makes the walk quadratic in the points it visits. Measured at
    /// 10,000 vectors, one silent-profile find
    /// compared 70,172,425 list entries answering that question, against 8,506
    /// vectors actually scored -- which is why a graph-walk evaluation cost
    /// 8.7 us where an exhaustive comparison of the same vector costs about 4.
    ///
    /// The bar is one entry looked at per edge. It is an inequality, not a
    /// count of edges, so a walk that reaches fewer points still passes.
    #[cfg(feature = "test-seams")]
    #[test]
    fn reaching_a_point_costs_the_same_however_much_a_walk_has_reached() {
        #[derive(Default)]
        struct Work {
            seen: Mutex<PagedWalkWork>,
        }
        impl PagedVectorObserver for Work {
            fn walk_compared(&self, work: PagedWalkWork) {
                let mut seen = self.seen.lock();
                seen.reached_entries += work.reached_entries;
                seen.frontier_entries += work.frontier_entries;
                seen.result_entries += work.result_entries;
                seen.edges += work.edges;
                seen.evaluations += work.evaluations;
            }
        }

        const DIMENSION: usize = 768;
        const POPULATION: usize = 1_250;
        let entries = (0..POPULATION)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &paged_proof_vector(ordinal, DIMENSION),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let ceiling = durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN);
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
            ceiling,
        )
        .unwrap();
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::new(Mutex::new(Vec::new())),
            }),
            DIMENSION,
            VectorQuantization::F32,
            admitted,
            ceiling,
        )
        .unwrap();

        let query = paged_proof_vector(POPULATION + 11, DIMENSION);
        let work = Arc::new(Work::default());
        let found = with_paged_vector_observer(work.clone(), || {
            paged.search_allowed_with_bounded_memory_at_ef(
                &VectorIndexRef::new("items", "embedding"),
                &query,
                10,
                POPULATION,
                HnswAllowedSearchLimits {
                    max_visited_nodes: usize::MAX,
                    max_vector_evaluations: usize::MAX,
                },
                400,
                std::iter::empty,
                true,
                |_| true,
                || Ok::<_, Error>(()),
                || Ok::<_, Error>(()),
                |_| Ok::<_, Error>(()),
                |_| {},
            )
        })
        .expect("the paged allowed walk completes");
        assert_eq!(found.rows.len(), 10, "the walk answers the whole page");

        let seen = *work.seen.lock();
        println!(
            "walk_work population={POPULATION} edges={} evaluations={} reached_entries={} \
             frontier_entries={} result_entries={}",
            seen.edges,
            seen.evaluations,
            seen.reached_entries,
            seen.frontier_entries,
            seen.result_entries,
        );
        assert!(
            seen.edges > seen.evaluations,
            "a walk considers more edges than it scores points, or this proves nothing: \
             {} edges against {} evaluations",
            seen.edges,
            seen.evaluations
        );
        assert!(
            seen.reached_entries <= seen.edges,
            "reaching a point looks at one entry, not at everything already reached: \
             {} entries over {} edges",
            seen.reached_entries,
            seen.edges
        );
    }

    #[cfg(feature = "test-seams")]
    #[test]
    fn a_resident_paged_visit_pays_only_its_own_distance() {
        const DIMENSION: usize = 768;
        const POPULATION: usize = 1_250;
        let entries = (0..POPULATION)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &paged_proof_vector(ordinal, DIMENSION),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::clone(&reads),
            }),
            DIMENSION,
            VectorQuantization::F32,
            admitted,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        assert!(
            matches!(paged.hnsw, HnswInner::F32Paged(_)),
            "the durable generation must load through the paged representation"
        );

        let query = paged_proof_vector(POPULATION + 7, DIMENSION);
        let run = |label: &str| {
            let counters = Arc::new(PagedVisitCounters::default());
            // Counted here rather than read back from the result: the paged
            // route's own `vector_evaluations` reports the neighbours it
            // returned, not the distances it computed.
            let distance_callbacks = Arc::new(AtomicUsize::new(0));
            let charged = Arc::clone(&distance_callbacks);
            let found = with_paged_vector_observer(counters.clone(), || {
                paged.search_allowed_with_bounded_memory_at_ef(
                    &VectorIndexRef::new("items", "embedding"),
                    &query,
                    10,
                    POPULATION,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    100,
                    std::iter::empty,
                    true,
                    |_| true,
                    || Ok::<_, Error>(()),
                    || {
                        charged.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(())
                    },
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
            })
            .expect("the paged allowed walk completes");
            println!(
                "paged_visit query={label} rows={} distances={} locks={} fills={} lookups={} hits={} decodes={} query_norms={}",
                found.rows.len(),
                distance_callbacks.load(Ordering::SeqCst),
                counters.cache_locks.load(Ordering::SeqCst),
                counters.page_fills.load(Ordering::SeqCst),
                counters.page_lookups.load(Ordering::SeqCst),
                counters.page_hits.load(Ordering::SeqCst),
                counters.vector_decodes.load(Ordering::SeqCst),
                counters.query_norms.load(Ordering::SeqCst),
            );
            (found, counters, distance_callbacks.load(Ordering::SeqCst))
        };

        let (first, first_counters, first_distances) = run("first");
        assert_eq!(first.rows.len(), 10, "the walk answers the whole page");
        assert!(
            first_distances > 0,
            "the walk must reach at least one paged distance"
        );
        assert_eq!(
            first_counters.query_norms.load(Ordering::SeqCst),
            1,
            "one statement computes the query's own norm exactly once"
        );
        let first_locks = first_counters.cache_locks.load(Ordering::SeqCst);
        let first_lookups = first_counters.page_lookups.load(Ordering::SeqCst);
        let first_fills = first_counters.page_fills.load(Ordering::SeqCst);
        assert!(
            first_locks <= first_lookups + first_fills,
            "the serving cache is locked to resolve a page the walk is not already holding \
             and once more to make a read page resident, and never for anything else: \
             {first_locks} acquisitions over {first_lookups} lookups and {first_fills} fills"
        );
        assert!(
            first_locks - first_fills < first_lookups,
            "a walk that comes back to a page it has already resolved answers from its own \
             directory and does not lock the shared cache again: {first_locks} acquisitions \
             less {first_fills} fills, against {first_lookups} lookups"
        );
        let decodes = first_counters.vector_decodes.load(Ordering::SeqCst);
        assert!(
            decodes <= first_distances,
            "a charged distance decodes at most one stored vector: decodes={decodes} \
             distances={first_distances}"
        );
        assert!(
            first_distances - decodes <= first.rows.len() + 1,
            "every charged distance that decoded nothing is a publication score or the \
             query's own self score: decodes={decodes} distances={first_distances}"
        );
        assert!(
            first_counters.page_lookups.load(Ordering::SeqCst) <= decodes.saturating_mul(2),
            "a stored vector spans at most two 64 KiB serving pages, so one \
             distance needs at most two serving-slot lookups"
        );
        assert_eq!(
            first_counters.page_fills.load(Ordering::SeqCst),
            reads.lock().len(),
            "every page fill is one read of the snapshot source"
        );
        assert!(
            first_counters.page_fills.load(Ordering::SeqCst)
                < first_counters.vector_decodes.load(Ordering::SeqCst),
            "a 64 KiB page holds many 3 KiB vectors, so fills are far fewer than decodes"
        );

        let source_reads_after_first = reads.lock().len();
        let (second, second_counters, _) = run("second");
        assert_eq!(
            second.rows, first.rows,
            "the second identical query answers identically"
        );
        assert_eq!(
            second_counters.page_fills.load(Ordering::SeqCst),
            0,
            "a second identical query faults no page that is already resident"
        );
        assert_eq!(
            reads.lock().len(),
            source_reads_after_first,
            "a second identical query reads the snapshot source no further"
        );
        assert_eq!(
            second_counters.query_norms.load(Ordering::SeqCst),
            1,
            "the second statement also computes the query norm exactly once"
        );
        let second_locks = second_counters.cache_locks.load(Ordering::SeqCst);
        let second_lookups = second_counters.page_lookups.load(Ordering::SeqCst);
        assert!(
            second_locks < second_lookups,
            "a statement that faults no page locks the serving cache once for each page it \
             resolves, not once for each look into it: {second_locks} acquisitions over \
             {second_lookups} lookups"
        );
    }

    #[test]
    fn minimum_one_page_cache_decodes_a_cross_boundary_vector_with_balanced_scratch() {
        use std::cell::Cell;

        const DIMENSION: usize = 768;
        let vectors = (0..48_usize)
            .map(|ordinal| {
                (0..DIMENSION)
                    .map(|coordinate| ((ordinal * 41 + coordinate * 17 + 3) % 1009) as f32 / 1009.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let entries = vectors
            .iter()
            .enumerate()
            .map(|(ordinal, vector)| {
                entry(RowId(ordinal as u64 + 1), vector, TxId(ordinal as u64 + 1))
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
            PAGED_VECTOR_MIN_CACHE_BYTES,
        )
        .unwrap();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::clone(&reads),
            }),
            DIMENSION,
            VectorQuantization::F32,
            admitted,
            PAGED_VECTOR_MIN_CACHE_BYTES,
        )
        .unwrap();
        let HnswInner::F32Paged(paged) = &paged.hnsw else {
            panic!("the retained generation must use the paged representation");
        };
        assert_eq!(
            paged.page_cache.lock().slots.len(),
            1,
            "the minimum configuration must really exercise one serving slot"
        );
        let point = paged
            .points
            .iter()
            .flatten()
            .find(|point| {
                let end = point.vector_offset + DIMENSION * std::mem::size_of::<f32>();
                point.vector_offset / PAGED_VECTOR_SOURCE_PAGE_BYTES
                    != (end - 1) / PAGED_VECTOR_SOURCE_PAGE_BYTES
            })
            .expect("the fixture contains a vector crossing a source-page boundary");
        let query = &vectors[point.origin_id];
        let charged = Cell::new(0_usize);
        let peak = Cell::new(0_usize);
        let mut acquire = |event| {
            let additional = match event {
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
                HnswSearchScratchEvent::Release { .. } => {
                    panic!("release events use the release callback")
                }
            };
            let next = charged.get() + additional;
            assert!(
                next <= PAGED_VECTOR_SOURCE_PAGE_BYTES,
                "pagewise decoding must fit one transient source page under pressure"
            );
            charged.set(next);
            peak.set(peak.get().max(next));
            Ok::<_, Error>(())
        };
        let mut release = |event| {
            let HnswSearchScratchEvent::Release {
                capacity,
                element_bytes,
                ..
            } = event
            else {
                panic!("acquire events use the acquire callback");
            };
            charged.set(charged.get() - capacity * element_bytes);
        };
        let mut reader = ServingPages::new(paged.source.reader().unwrap(), 1);
        let distance = paged
            .distance(
                point,
                query,
                PagedHnsw::query_norm(query),
                &mut || Ok::<_, Error>(()),
                &mut || Ok::<_, Error>(()),
                &mut acquire,
                &mut release,
                reader.as_mut(),
            )
            .unwrap();
        assert!(
            distance.abs() <= 1.0e-6,
            "a vector must score exactly against itself"
        );
        assert_eq!(
            charged.get(),
            0,
            "all transient page charges must be released"
        );
        assert!(
            peak.get() > 0,
            "the proof must observe real scratch admission"
        );
        assert!(
            reads.lock().iter().all(|(offset, len)| {
                offset % PAGED_VECTOR_SOURCE_PAGE_BYTES == 0
                    && *len <= PAGED_VECTOR_SOURCE_PAGE_BYTES
            }),
            "minimum-cache faults must stay aligned to source pages"
        );
    }

    #[test]
    fn paged_durable_generation_keeps_topology_and_nearest_neighbor() {
        let entries = [
            entry(RowId(11), &[1.0, 0.0, 0.0], TxId(1)),
            entry(RowId(29), &[0.0, 1.0, 0.0], TxId(2)),
            entry(RowId(31), &[0.0, 0.0, 1.0], TxId(3)),
        ];
        let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
        let topology = graph.graph_topology_digest_for_test();
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        let admitted = HnswIndex::durable_reader_resident_bound(
            bytes.as_slice(),
            3,
            VectorQuantization::F32,
            graph.m,
        )
        .unwrap();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads,
            }),
            3,
            VectorQuantization::F32,
            admitted,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();
        assert_eq!(paged.graph_topology_digest_for_test(), topology);
        assert!(paged.estimated_resident_bytes() <= admitted);
        assert_eq!(
            paged
                .search(
                    &VectorIndexRef::new("items", "embedding"),
                    &[0.0, 1.0, 0.0],
                    1,
                )
                .unwrap()[0]
                .0,
            RowId(29)
        );
    }

    fn paired_saved_graph(
        entries: &[StoredVectorEntry],
        dimension: usize,
    ) -> (HnswIndex, HnswIndex) {
        let built = HnswIndex::new(entries, dimension, VectorQuantization::F32);
        let bytes = Arc::new(built.encode_durable_generation().unwrap());
        let owned = HnswIndex::decode_durable_generation(
            bytes.as_slice(),
            dimension,
            VectorQuantization::F32,
        )
        .unwrap();
        let cache = durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN);
        let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
            bytes.as_slice(),
            dimension,
            VectorQuantization::F32,
            built.m,
            cache,
        )
        .unwrap();
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::new(Mutex::new(Vec::new())),
            }),
            dimension,
            VectorQuantization::F32,
            admitted,
            cache,
        )
        .unwrap();
        assert!(matches!(paged.hnsw, HnswInner::F32Paged(_)));
        (owned, paged)
    }

    fn saved_graph_vector(ordinal: usize, dimension: usize) -> Vec<f32> {
        (0..dimension)
            .map(|axis| {
                let mixed = (ordinal as u64 + 17)
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    .wrapping_add((axis as u64 + 31).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                ((mixed ^ (mixed >> 29)) % 100_003) as f32 / 100_003.0 - 0.5
            })
            .collect()
    }

    #[test]
    fn saved_graph_pages_preserve_owned_neighbor_ids_and_order_across_search_breadths() {
        const DIMENSION: usize = 16;
        let index = VectorIndexRef::new("items", "embedding");
        for population in [64_usize, 5_000] {
            let entries = (0..population)
                .map(|ordinal| {
                    entry(
                        RowId(ordinal as u64 + 1),
                        &saved_graph_vector(ordinal, DIMENSION),
                        TxId(ordinal as u64 + 1),
                    )
                })
                .collect::<Vec<_>>();
            let (owned, paged) = paired_saved_graph(&entries, DIMENSION);
            for probe in 0..200 {
                let query = saved_graph_vector(population + probe, DIMENSION);
                for ef in [10, 50, 200] {
                    let expected = owned
                        .search_with_ef_search(&index, &query, 10, ef)
                        .unwrap()
                        .into_iter()
                        .take(10)
                        .map(|(row, _)| row)
                        .collect::<Vec<_>>();
                    let actual = paged
                        .search_with_ef_search(&index, &query, 10, ef)
                        .unwrap()
                        .into_iter()
                        .take(10)
                        .map(|(row, _)| row)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, expected,
                        "population={population} probe={probe} ef={ef}"
                    );
                }
            }
        }
    }

    #[test]
    fn saved_graph_pages_preserve_multi_partition_union_neighbor_order() {
        const DIMENSION: usize = 16;
        let mut owned = Vec::new();
        let mut paged = Vec::new();
        for partition in 0..2 {
            let entries = (0..64)
                .map(|ordinal| {
                    let row = partition * 64 + ordinal;
                    entry(
                        RowId(row as u64 + 1),
                        &saved_graph_vector(row, DIMENSION),
                        TxId(row as u64 + 1),
                    )
                })
                .collect::<Vec<_>>();
            let (resident, pages) = paired_saved_graph(&entries, DIMENSION);
            owned.push(resident);
            paged.push(pages);
        }
        for probe in 0..200 {
            let query = saved_graph_vector(128 + probe, DIMENSION);
            for ef in [10, 50, 200] {
                let mut answers = Vec::new();
                for graphs in [&owned, &paged] {
                    let layers = graphs
                        .iter()
                        .enumerate()
                        .map(|(partition, graph)| HnswUnionLayer {
                            graph,
                            partition,
                            allowed_count: 64,
                            live_count: 64,
                            seed_rows: None,
                        })
                        .collect::<Vec<_>>();
                    let mut query_scores = BoundedHnswQuerySelfScores::new(&query);
                    let answer = search_union_allowed_bounded(
                        &layers,
                        &query,
                        10,
                        ef,
                        10_000,
                        HnswAllowedSearchLimits {
                            max_visited_nodes: usize::MAX,
                            max_vector_evaluations: usize::MAX,
                        },
                        true,
                        false,
                        &mut query_scores,
                        |_, _| true,
                        |_, _, _, _| Ok::<_, Error>(None),
                        || Ok::<(), Error>(()),
                        || Ok::<(), Error>(()),
                        |_| Ok::<(), Error>(()),
                        |_| {},
                    )
                    .unwrap();
                    answers.push((
                        answer.terminal,
                        answer
                            .candidates
                            .into_iter()
                            .map(|candidate| (candidate.partition, candidate.row_id))
                            .collect::<Vec<_>>(),
                    ));
                }
                assert_eq!(answers[1], answers[0], "probe={probe} ef={ef}");
            }
        }
    }

    #[test]
    fn mixed_saved_base_and_resident_tail_keep_exact_row_id_ties_at_breadth_one() {
        use std::cell::{Cell, RefCell};

        let mut wrong_answers = Vec::new();
        // The published cosine score is zero for every pair involving a zero
        // vector. The saved and resident layers must therefore use the same
        // ordering distance before the one-place heap can discard a row.
        for (case, query, saved_vector, tail_vector) in [
            (
                "zero query",
                [0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
            ),
            (
                "zero stored vectors",
                [1.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
            ),
            (
                "ordinary nonzero vectors",
                [0.0, 1.0, 0.0],
                [1.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
            ),
        ] {
            let mut exact = [(RowId(1), saved_vector), (RowId(2), tail_vector)]
                .map(|(row, vector)| (row, crate::cosine_similarity(&vector, &query)));
            exact.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            assert_eq!(exact[0].0, RowId(1), "{case}: exact tie contract");
            assert_eq!(exact[0].1, exact[1].1, "{case}: published scores tie");

            let (_, saved) = paired_saved_graph(&[entry(RowId(1), &saved_vector, TxId(1))], 3);
            let tail = HnswIndex::new(
                &[entry(RowId(2), &tail_vector, TxId(2))],
                3,
                VectorQuantization::F32,
            );
            let layers = [
                HnswUnionLayer {
                    graph: &saved,
                    partition: 0,
                    allowed_count: 2,
                    live_count: 2,
                    seed_rows: None,
                },
                HnswUnionLayer {
                    graph: &tail,
                    partition: 0,
                    allowed_count: 2,
                    live_count: 2,
                    seed_rows: None,
                },
            ];
            let distance_work = Cell::new(0);
            let scratch_events = RefCell::new(Vec::new());
            let mut query_scores = BoundedHnswQuerySelfScores::new(&query);
            let answer = search_union_allowed_bounded(
                &layers,
                &query,
                1,
                1,
                32,
                HnswAllowedSearchLimits {
                    max_visited_nodes: 32,
                    max_vector_evaluations: 32,
                },
                true,
                false,
                &mut query_scores,
                |_, _| true,
                |_, _, _, _| Ok::<_, Error>(None),
                || Ok::<(), Error>(()),
                || {
                    distance_work.set(distance_work.get() + 1);
                    Ok::<(), Error>(())
                },
                |event| {
                    scratch_events.borrow_mut().push(event);
                    Ok::<(), Error>(())
                },
                |event| scratch_events.borrow_mut().push(event),
            )
            .unwrap();
            assert_eq!(answer.terminal, UnionTerminal::AnswerComplete, "{case}");
            assert_eq!(answer.layers_admitted, 2, "{case}");
            assert!(answer.admission_evaluations >= 2, "{case}");
            assert!(
                answer.admission_evaluations + answer.expansion_evaluations <= 32,
                "{case}"
            );
            assert!(
                distance_work.get() >= answer.admission_evaluations + answer.expansion_evaluations,
                "{case}"
            );
            assert!(
                scratch_events.borrow().iter().any(|event| matches!(
                    event,
                    HnswSearchScratchEvent::Reserve {
                        container: HnswSearchScratchContainer::ReturnHeap,
                        requested_capacity: 1,
                        ..
                    }
                )),
                "{case}: one-result heap must be charged"
            );
            let actual = answer
                .candidates
                .iter()
                .map(|candidate| candidate.row_id)
                .collect::<Vec<_>>();
            if actual != [exact[0].0] {
                wrong_answers.push(format!(
                    "{case}: indexed {actual:?}, EXACT {:?} at breadth one",
                    exact[0].0
                ));
            }
        }
        assert!(wrong_answers.is_empty(), "{}", wrong_answers.join("; "));
    }

    #[test]
    fn saved_graph_pages_recover_exact_top_ten_when_search_breadth_covers_every_row() {
        const DIMENSION: usize = 16;
        const POPULATION: usize = 64;
        let vectors = (0..POPULATION)
            .map(|row| saved_graph_vector(row, DIMENSION))
            .collect::<Vec<_>>();
        let entries = vectors
            .iter()
            .enumerate()
            .map(|(ordinal, vector)| {
                entry(RowId(ordinal as u64 + 1), vector, TxId(ordinal as u64 + 1))
            })
            .collect::<Vec<_>>();
        let (_, paged) = paired_saved_graph(&entries, DIMENSION);
        let index = VectorIndexRef::new("items", "embedding");
        for probe in 0..200 {
            let query = saved_graph_vector(POPULATION + probe, DIMENSION);
            let mut exact = vectors
                .iter()
                .enumerate()
                .map(|(ordinal, vector)| {
                    (
                        RowId(ordinal as u64 + 1),
                        crate::cosine_similarity(vector, &query),
                    )
                })
                .collect::<Vec<_>>();
            exact.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            let expected = exact
                .into_iter()
                .take(10)
                .map(|(row, _)| row)
                .collect::<Vec<_>>();
            let actual = paged
                .search_with_ef_search(&index, &query, 10, POPULATION)
                .unwrap()
                .into_iter()
                .take(10)
                .map(|(row, _)| row)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "probe={probe}");
        }
    }

    #[test]
    fn saved_graph_pages_return_the_exact_lowest_row_ids_for_equal_vectors() {
        let entries = (1..=64)
            .map(|row| entry(RowId(row), &[1.0, 0.0, 0.0], TxId(row)))
            .collect::<Vec<_>>();
        let (_, paged) = paired_saved_graph(&entries, 3);
        let index = VectorIndexRef::new("items", "embedding");
        let expected = (1..=10).map(RowId).collect::<Vec<_>>();
        for ef in [64, 10] {
            let actual = paged
                .search_with_ef_search(&index, &[1.0, 0.0, 0.0], 10, ef)
                .unwrap()
                .into_iter()
                .take(10)
                .map(|(row, _)| row)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "ef={ef}");
        }
    }

    #[test]
    fn saved_graph_pages_refuse_the_same_distance_budget_as_owned_graphs() {
        const DIMENSION: usize = 16;
        let entries = (0..128)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &saved_graph_vector(ordinal, DIMENSION),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let (owned, paged) = paired_saved_graph(&entries, DIMENSION);
        let index = VectorIndexRef::new("items", "embedding");
        for probe in 0..20 {
            let query = saved_graph_vector(128 + probe, DIMENSION);
            for ef in [10, 50, 200] {
                for limit in [1, 5, 20] {
                    let mut outcomes = Vec::new();
                    for graph in [&owned, &paged] {
                        let mut distances = 0;
                        let outcome = graph
                            .search_with_bounded_memory_at_ef(
                                &index,
                                &query,
                                10,
                                ef,
                                || Ok::<(), Error>(()),
                                || {
                                    distances += 1;
                                    if distances > limit {
                                        Err(Error::Other("distance budget exhausted".into()))
                                    } else {
                                        Ok(())
                                    }
                                },
                                |_| Ok::<(), Error>(()),
                                |_| {},
                            )
                            .map(|result| {
                                result
                                    .rows
                                    .into_iter()
                                    .take(10)
                                    .map(|(row, _)| row)
                                    .collect::<Vec<_>>()
                            })
                            .map_err(|error: Error| error.to_string());
                        outcomes.push((distances, outcome));
                    }
                    assert_eq!(
                        outcomes[1], outcomes[0],
                        "probe={probe} ef={ef} limit={limit}"
                    );
                }
            }
        }
    }

    #[test]
    fn saved_graph_pages_charge_the_same_distance_expansions_as_owned_graphs() {
        const DIMENSION: usize = 16;
        let entries = (0..128)
            .map(|ordinal| {
                entry(
                    RowId(ordinal as u64 + 1),
                    &saved_graph_vector(ordinal, DIMENSION),
                    TxId(ordinal as u64 + 1),
                )
            })
            .collect::<Vec<_>>();
        let (owned, paged) = paired_saved_graph(&entries, DIMENSION);
        let index = VectorIndexRef::new("items", "embedding");
        for probe in 0..50 {
            let query = saved_graph_vector(128 + probe, DIMENSION);
            for ef in [10, 50, 200] {
                let mut counts = Vec::new();
                for graph in [&owned, &paged] {
                    let mut distances = 0;
                    let result = graph
                        .search_with_bounded_memory_at_ef(
                            &index,
                            &query,
                            10,
                            ef,
                            || Ok::<(), Error>(()),
                            || {
                                distances += 1;
                                Ok::<(), Error>(())
                            },
                            |_| Ok::<(), Error>(()),
                            |_| {},
                        )
                        .unwrap();
                    counts.push((
                        distances,
                        result
                            .rows
                            .into_iter()
                            .take(10)
                            .map(|(row, _)| row)
                            .collect::<Vec<_>>(),
                    ));
                }
                assert_eq!(counts[1], counts[0], "probe={probe} ef={ef}");
            }
        }
    }

    /// The resident bound a load is admitted against must cover what the
    /// decoded graph actually holds, whatever degree the consumer declared. A
    /// neighbour list grows with the declared degree, so a bound that prices
    /// every row the same refuses to open a small graph declared with a large
    /// degree even when the memory it really needs is available.
    #[test]
    fn a_large_declared_degree_opens_under_its_own_resident_bound() {
        const DIMENSION: usize = 16;
        let entries = (0..600_u64)
            .map(|ordinal| {
                let vector = (0..DIMENSION as u64)
                    .map(|d| (((ordinal + 7) * (d + 13) * 131 + d * d) % 997) as f32 / 997.0)
                    .collect::<Vec<_>>();
                entry(RowId(ordinal + 1), &vector, TxId(ordinal + 1))
            })
            .collect::<Vec<_>>();
        let mut refused = Vec::new();
        for declared_degree in [16_usize, 64, 128] {
            let policy = crate::store::ResolvedVectorPolicy {
                auto_index_at: 1,
                hnsw_m: declared_degree,
                hnsw_ef_construction: 64,
                hnsw_ef_search: 64,
                policy_revision: 1,
                auto_index_at_source: "declared",
                ef_search_source: "declared",
            };
            let graph =
                HnswIndex::new_with_policy(&entries, DIMENSION, VectorQuantization::F32, policy);
            assert_eq!(graph.m, declared_degree);
            let bytes = Arc::new(graph.encode_durable_generation().unwrap());
            // A cache the size of this partition's own pages, as a load with
            // nothing declared sizes it -- so the bound is priced by its rows,
            // not by a cache far larger than the graph.
            let cache_bytes = durable_reader_cache_ceiling(None, None, bytes.len());
            let source = || {
                Arc::new(TestPageSource {
                    bytes: Arc::clone(&bytes),
                    reads: Arc::new(Mutex::new(Vec::new())),
                })
            };
            // What the decoded graph really holds, measured with no ceiling.
            let actual = HnswIndex::decode_paged_durable_generation(
                bytes.as_slice(),
                source(),
                DIMENSION,
                VectorQuantization::F32,
                usize::MAX,
                cache_bytes,
            )
            .unwrap()
            .estimated_resident_bytes();
            let bound = HnswIndex::durable_reader_resident_bound_for_cache(
                bytes.as_slice(),
                DIMENSION,
                VectorQuantization::F32,
                declared_degree,
                cache_bytes,
            )
            .unwrap();
            println!("resident_bound degree={declared_degree} actual={actual} bound={bound}");
            if actual > bound {
                refused.push(format!(
                    "degree {declared_degree}: the graph holds {actual} bytes but is admitted \
                     against {bound}"
                ));
                continue;
            }
            let opened = HnswIndex::decode_paged_durable_generation(
                bytes.as_slice(),
                source(),
                DIMENSION,
                VectorQuantization::F32,
                bound,
                cache_bytes,
            );
            assert!(
                opened.is_ok(),
                "degree {declared_degree}: a graph whose {actual} resident bytes fit its \
                 {bound}-byte bound refused to open: {:?}",
                opened.err()
            );
        }
        assert!(refused.is_empty(), "{}", refused.join("\n"));
    }

    #[test]
    fn paged_traversal_retains_independently_authenticated_source_pages() {
        const DIMENSION: usize = 768;
        let entries = (0..192_u64)
            .map(|ordinal| {
                let vector = (0..DIMENSION)
                    .map(|component| {
                        (((ordinal + 17) * (component as u64 + 31) + component as u64 * 7) % 997)
                            as f32
                            / 997.0
                    })
                    .collect::<Vec<_>>();
                entry(RowId(ordinal + 1), &vector, TxId(ordinal + 1))
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, DIMENSION, VectorQuantization::F32);
        let bytes = Arc::new(graph.encode_durable_generation().unwrap());
        assert!(bytes.len() > PAGED_VECTOR_SOURCE_PAGE_BYTES * 2);
        let admitted = HnswIndex::durable_reader_resident_bound(
            bytes.as_slice(),
            DIMENSION,
            VectorQuantization::F32,
            graph.m,
        )
        .unwrap();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let paged = HnswIndex::decode_paged_durable_generation(
            bytes.as_slice(),
            Arc::new(TestPageSource {
                bytes: Arc::clone(&bytes),
                reads: Arc::clone(&reads),
            }),
            DIMENSION,
            VectorQuantization::F32,
            admitted,
            durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
        )
        .unwrap();

        let query = vec![0.314_159; DIMENSION];
        assert!(
            !paged
                .search(&VectorIndexRef::new("items", "embedding"), &query, 10)
                .unwrap()
                .is_empty()
        );
        let reads_after_first_search = reads.lock().len();
        assert!(
            !paged
                .search(&VectorIndexRef::new("items", "embedding"), &query, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(reads.lock().len(), reads_after_first_search);
        let selected = (0..10_u64)
            .map(|ordinal| 1 + ordinal * 19)
            .collect::<Vec<_>>();
        let filtered = paged
            .search_allowed_with_bounded_memory_at_ef(
                &VectorIndexRef::new("items", "embedding"),
                &query,
                10,
                selected.len(),
                HnswAllowedSearchLimits {
                    max_visited_nodes: 128,
                    max_vector_evaluations: 128,
                },
                16,
                || selected.iter().copied(),
                false,
                |row| selected.binary_search(&row.0).is_ok(),
                || Ok::<_, Error>(()),
                || Ok::<_, Error>(()),
                |_| Ok::<_, Error>(()),
                |_| {},
            )
            .unwrap();
        assert_eq!(filtered.status, HnswAllowedSearchStatus::Complete);
        assert_eq!(filtered.rows.len(), selected.len());
        assert!(
            filtered
                .rows
                .iter()
                .all(|(row, _)| selected.binary_search(&row.0).is_ok())
        );
        let reads = reads.lock();
        assert!(
            !reads.is_empty(),
            "the traversal must fill its serving cache"
        );
        assert!(
            reads.iter().any(|(offset, len)| {
                offset % PAGED_VECTOR_SOURCE_PAGE_BYTES == 0
                    && *len == PAGED_VECTOR_SOURCE_PAGE_BYTES
            }),
            "the traversal must perform at least one full aligned source-page read"
        );
        for &(offset, len) in reads.iter() {
            let end = offset + len;
            let aligned_page = offset % PAGED_VECTOR_SOURCE_PAGE_BYTES == 0
                && len <= PAGED_VECTOR_SOURCE_PAGE_BYTES;
            let narrow_boundary_value = len == DIMENSION * std::mem::size_of::<f32>()
                && offset / PAGED_VECTOR_SOURCE_PAGE_BYTES
                    != (end - 1) / PAGED_VECTOR_SOURCE_PAGE_BYTES;
            assert!(
                aligned_page || narrow_boundary_value,
                "unaligned full-page source read retained: offset={offset}, len={len}"
            );
        }
    }

    #[test]
    fn durable_generation_refuses_corruption_and_incompatible_column_definition() {
        let graph = HnswIndex::new(
            &[entry(RowId(11), &[1.0, 0.0], TxId(1))],
            2,
            VectorQuantization::F32,
        );
        let bytes = graph.encode_durable_generation().unwrap();
        assert!(
            HnswIndex::decode_durable_generation(
                &bytes[..bytes.len() - 1],
                2,
                VectorQuantization::F32
            )
            .is_err()
        );
        assert!(HnswIndex::decode_durable_generation(&bytes, 3, VectorQuantization::F32).is_err());
        assert!(HnswIndex::decode_durable_generation(&bytes, 2, VectorQuantization::SQ8).is_err());
    }

    #[test]
    fn embedded_filtered_breadth_matches_the_same_walk_without_caller_work_limits() {
        let entries = (0..512_u64)
            .map(|row| {
                let vector = (0..32)
                    .map(|d| (((row + 11) * (d + 37) * 177 + d * d) % 997) as f32 / 997.0)
                    .collect::<Vec<_>>();
                entry(RowId(row + 1), &vector, TxId(row + 1))
            })
            .collect::<Vec<_>>();
        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1,
            hnsw_m: 2,
            hnsw_ef_construction: 64,
            hnsw_ef_search: 64,
            policy_revision: 1,
            auto_index_at_source: "declared",
            ef_search_source: "declared",
        };
        let graph = HnswIndex::new_with_policy(&entries, 32, VectorQuantization::F32, policy);
        let index = VectorIndexRef::new("items", "embedding");
        for probe in 0..8_u64 {
            let query = (0..32)
                .map(|d| (((probe + 1003) * (d + 37) * 177 + d * d) % 997) as f32 / 997.0)
                .collect::<Vec<_>>();
            let embedded = graph
                .search_allowed_at_ef(
                    &index,
                    &query,
                    10,
                    256,
                    64,
                    || (1..=512).filter(|id| id % 2 == 0),
                    false,
                    |id| id.0 % 2 == 0,
                    |_| Ok(()),
                    |_| {},
                )
                .unwrap();
            let bounded = graph
                .search_allowed_with_bounded_memory_at_ef(
                    &index,
                    &query,
                    10,
                    256,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    64,
                    || (1..=512).filter(|id| id % 2 == 0),
                    false,
                    |id| id.0 % 2 == 0,
                    || Ok::<_, Error>(()),
                    || Ok::<_, Error>(()),
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
                .unwrap();
            eprintln!(
                "probe={probe} visits={} distances={} status={:?}",
                embedded.visited_nodes, embedded.vector_evaluations, embedded.status
            );
            assert_eq!(embedded.status, bounded.status);
            assert_eq!(embedded.rows, bounded.rows);
            assert_eq!(embedded.visited_nodes, bounded.visited_nodes);
            assert!(embedded.vector_evaluations < entries.len());
        }
    }

    #[test]
    fn layered_allowed_membership_bounds_history_work_and_preserves_results() {
        // The one current base row is surrounded by excluded history; the
        // other nine current rows belong to later graphs. The query is held
        // out, so an exact-vector bucket cannot hide a failed graph walk.
        let index = VectorIndexRef::new("items", "embedding");
        let query = [0.83, 0.41, 0.19];
        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1,
            hnsw_m: 8,
            hnsw_ef_construction: 32,
            hnsw_ef_search: 16,
            policy_revision: 1,
            auto_index_at_source: "declared",
            ef_search_source: "declared",
        };
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let current = (1..=10_u64)
                .map(|row| {
                    entry_with_quantization(
                        RowId(row),
                        &[row as f32 + 0.5, 11.0 - row as f32, 1.0],
                        TxId(row),
                        quantization,
                    )
                })
                .collect::<Vec<_>>();
            let mut expected = None;
            for history in [0, 64, 512, 2048_u64] {
                let mut base_entries = vec![current[0].clone()];
                base_entries.extend((0..history).map(|row| {
                    entry_with_quantization(
                        RowId(100 + row),
                        &[0.2, 0.3 + row as f32 / 4096.0, 0.9],
                        TxId(1),
                        quantization,
                    )
                }));
                let seal = |entries: &[StoredVectorEntry]| {
                    let built = HnswIndex::new_with_policy(entries, 3, quantization, policy);
                    HnswIndex::decode_durable_generation(
                        &built.encode_durable_generation().unwrap(),
                        3,
                        quantization,
                    )
                    .unwrap()
                };
                let layers = [
                    seal(&base_entries),
                    seal(&current[1..5]),
                    HnswIndex::new_with_policy(&current[5..], 3, quantization, policy),
                ];
                let mut merged = Vec::new();
                let mut visits = 0usize;
                let mut distances = 0usize;
                for (layer, expected_ids) in layers.iter().zip([1..=1, 2..=5, 6..=10]) {
                    let embedded = layer
                        .search_allowed_at_ef(
                            &index,
                            &query,
                            10,
                            10,
                            16,
                            || 1..=10,
                            false,
                            |row| row.0 <= 10,
                            |_| Ok(()),
                            |_| {},
                        )
                        .unwrap();
                    let mut source_work = 0usize;
                    let mut distance_work = 0usize;
                    let bounded = layer
                        .search_allowed_with_bounded_memory_at_ef(
                            &index,
                            &query,
                            10,
                            10,
                            HnswAllowedSearchLimits {
                                max_visited_nodes: 128,
                                max_vector_evaluations: 128,
                            },
                            16,
                            || 1..=10,
                            false,
                            |row| row.0 <= 10,
                            || {
                                source_work += 1;
                                Ok::<_, Error>(())
                            },
                            || {
                                distance_work += 1;
                                Ok::<_, Error>(())
                            },
                            |_| Ok::<_, Error>(()),
                            |_| {},
                        )
                        .unwrap();
                    assert_eq!(embedded.status, HnswAllowedSearchStatus::Complete);
                    assert_eq!(bounded.status, HnswAllowedSearchStatus::Complete);
                    assert_eq!(embedded.rows, bounded.rows);
                    assert_eq!(embedded.visited_nodes, bounded.visited_nodes);
                    assert_eq!(embedded.vector_evaluations, bounded.vector_evaluations);
                    assert_eq!(
                        embedded
                            .rows
                            .iter()
                            .map(|(row, _)| row.0)
                            .collect::<HashSet<_>>(),
                        expected_ids.collect::<HashSet<_>>()
                    );
                    // These fixed bounds include upper-level entry navigation,
                    // seed probes, exact-key checks and scratch migration.
                    assert!(source_work < 4096, "history={history} source={source_work}");
                    assert!(
                        distance_work < 128,
                        "history={history} distances={distance_work}"
                    );
                    println!(
                        "layer quantization={quantization:?} history={history} rows={} source={source_work} distances={distance_work} visits={}",
                        embedded.rows.len(),
                        embedded.visited_nodes
                    );
                    visits += embedded.visited_nodes;
                    distances += embedded.vector_evaluations;
                    merged.extend(embedded.rows);
                }
                merged.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                assert_eq!(merged.len(), 10);
                if let Some(expected) = &expected {
                    assert_eq!(&merged, expected);
                } else {
                    expected = Some(merged);
                }
                assert!(visits < 384, "history={history} visits={visits}");
                assert!(distances < 384, "history={history} distances={distances}");
            }
        }
    }

    #[test]
    fn restricted_exact_ties_probe_only_allowed_layer_membership() {
        for history in [0, 64, 2048_u64] {
            let entries = (1..=history)
                .chain(10_000..10_010)
                .map(|row| entry(RowId(row), &[1.0, 0.0, 0.0], TxId(1)))
                .collect::<Vec<_>>();
            let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
            let mut source_work = 0usize;
            let found = graph
                .search_allowed_with_bounded_memory_at_ef(
                    &VectorIndexRef::new("items", "embedding"),
                    &[1.0, 0.0, 0.0],
                    10,
                    10,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: 128,
                        max_vector_evaluations: 128,
                    },
                    16,
                    || 10_000..10_010,
                    false,
                    |row| row.0 >= 10_000,
                    || {
                        source_work += 1;
                        Ok::<_, Error>(())
                    },
                    || Ok::<_, Error>(()),
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
                .unwrap();
            assert_eq!(found.status, HnswAllowedSearchStatus::Complete);
            assert_eq!(
                found.rows.iter().map(|(row, _)| row.0).collect::<Vec<_>>(),
                (10_000..10_010).collect::<Vec<_>>()
            );
            assert!(source_work < 1024, "history={history} source={source_work}");
            assert!(found.visited_nodes < 128);
            assert!(found.vector_evaluations < 128);
            println!(
                "ties history={history} rows={} source={source_work} visits={} distances={}",
                found.rows.len(),
                found.visited_nodes,
                found.vector_evaluations
            );
        }
    }

    #[test]
    fn layer_seed_spacing_fills_declared_breadth_with_uneven_membership() {
        let entries = (1..=17)
            .map(|row| entry(RowId(row), &[row as f32, 19.0 - row as f32, 1.0], TxId(1)))
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
        let found = graph
            .search_allowed_at_ef(
                &VectorIndexRef::new("items", "embedding"),
                &[0.9, 0.2, 0.1],
                10,
                513,
                10,
                || 1..=513,
                false,
                |_| true,
                |_| Ok(()),
                |_| {},
            )
            .unwrap();
        assert_eq!(found.status, HnswAllowedSearchStatus::Complete);
        assert_eq!(found.rows.len(), 10);
        assert!(found.rows.iter().all(|(id, _)| (1..=17).contains(&id.0)));
    }

    /// A filtered graph search must return the rows an exhaustive comparison
    /// of the same allowed set would return, however selective the filter is.
    /// Where the filter refuses most of what the walk meets, its neighbours are
    /// never visited and the candidate frontier can die a few steps from where
    /// it entered, leaving it holding the points it entered through rather than
    /// the query's neighbourhood.
    #[test]
    fn a_selective_filter_does_not_cost_the_graph_its_answer() {
        const DIMENSIONS: usize = 32;
        const EF_SEARCH: usize = 100;
        const K: usize = 10;
        const HELD_OUT: usize = 100;

        fn spread(role: u64, ordinal: usize, dims: usize) -> Vec<f32> {
            (0..dims)
                .map(|axis| {
                    let seed = (ordinal as u64)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add((axis as u64).wrapping_mul(0x85eb_ca6b_ba21_1f3d))
                        .wrapping_add(role.wrapping_mul(0xc2b2_ae35_9d5e_e1b1))
                        % 10_007;
                    (seed as f32 / 10_007.0) - 0.5
                })
                .collect()
        }

        fn exact_top_k(
            vectors: &[Vec<f32>],
            allowed: impl Fn(usize) -> bool,
            query: &[f32],
            k: usize,
        ) -> Vec<u64> {
            let mut scored = vectors
                .iter()
                .enumerate()
                .filter(|(ordinal, _)| allowed(*ordinal))
                .map(|(ordinal, vector)| {
                    (ordinal as u64 + 1, crate::cosine_similarity(vector, query))
                })
                .collect::<Vec<_>>();
            scored.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            scored.truncate(k);
            scored.into_iter().map(|(row, _)| row).collect()
        }

        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1_000,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: EF_SEARCH,
            policy_revision: 1,
            auto_index_at_source: "compatibility_profile",
            ef_search_source: "compatibility_profile",
        };

        let mut failures = Vec::new();
        for population in [1_250_usize, 10_000] {
            let vectors = (0..population)
                .map(|ordinal| spread(1, ordinal, DIMENSIONS))
                .collect::<Vec<_>>();
            let entries = vectors
                .iter()
                .enumerate()
                .map(|(ordinal, vector)| {
                    entry(RowId(ordinal as u64 + 1), vector, TxId(ordinal as u64 + 1))
                })
                .collect::<Vec<_>>();
            let resident =
                HnswIndex::new_with_policy(&entries, DIMENSIONS, VectorQuantization::F32, policy);
            // The same generation as the file route serves it: a reader opens
            // the durable bytes and pages them in, matching the measured
            // file-reader route.
            let durable = Arc::new(resident.encode_durable_generation().unwrap());
            let admitted = HnswIndex::durable_reader_resident_bound_for_cache(
                durable.as_slice(),
                DIMENSIONS,
                VectorQuantization::F32,
                resident.m,
                durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
            )
            .unwrap();
            let paged = HnswIndex::decode_paged_durable_generation(
                durable.as_slice(),
                Arc::new(TestPageSource {
                    bytes: Arc::clone(&durable),
                    reads: Arc::new(Mutex::new(Vec::new())),
                }),
                DIMENSIONS,
                VectorQuantization::F32,
                admitted,
                durable_reader_cache_ceiling(None, None, PAGED_VECTOR_CACHE_TEST_SPAN),
            )
            .unwrap();
            assert!(matches!(paged.hnsw, HnswInner::F32Paged(_)));
            for (route, graph) in [("resident", &resident), ("paged", &paged)] {
                // Every hundredth, tenth, second, ninth-of-ten and all of them.
                for admitted_in_every in [1_usize, 2, 10, 20, 50, 100] {
                    let allowed = |ordinal: usize| ordinal.is_multiple_of(admitted_in_every);
                    let allowed_count = (0..population).filter(|o| allowed(*o)).count();
                    let mut matched = 0usize;
                    let mut worst_visits = 0usize;
                    let mut total_visits = 0usize;
                    let mut incomplete_probes = 0usize;
                    for probe in 0..HELD_OUT {
                        let query = spread(2, probe, DIMENSIONS);
                        let found = graph
                            .search_allowed_with_bounded_memory_at_ef(
                                &VectorIndexRef::new("items", "embedding"),
                                &query,
                                K,
                                allowed_count,
                                HnswAllowedSearchLimits {
                                    max_visited_nodes: usize::MAX,
                                    max_vector_evaluations: usize::MAX,
                                },
                                EF_SEARCH,
                                std::iter::empty,
                                true,
                                |row_id| allowed(row_id.0 as usize - 1),
                                || Ok::<_, Error>(()),
                                || Ok::<_, Error>(()),
                                |_| Ok::<_, Error>(()),
                                |_| {},
                            )
                            .expect("the bounded allowed walk completes");
                        // Rows that come back beside an incomplete status are
                        // diagnostic only, never an answer, so a cell whose recall
                        // rests on them proves nothing about what a reader is
                        // served.
                        if found.status != HnswAllowedSearchStatus::Complete {
                            incomplete_probes += 1;
                        }
                        worst_visits = worst_visits.max(found.visited_nodes);
                        total_visits += found.visited_nodes;
                        let mut rows = found.rows.clone();
                        rows.sort_by(|left, right| {
                            right
                                .1
                                .total_cmp(&left.1)
                                .then_with(|| left.0.cmp(&right.0))
                        });
                        rows.truncate(K);
                        let expected = exact_top_k(&vectors, allowed, &query, K);
                        matched += rows
                            .iter()
                            .filter(|(row_id, _)| expected.contains(&row_id.0))
                            .count();
                    }
                    let possible = HELD_OUT * K.min(allowed_count);
                    println!(
                        "selective_recall route={route} population={population} \
                     allowed={allowed_count} probes={HELD_OUT} worst_visits={worst_visits} \
                     mean_visits={} recall={}/{possible} incomplete={incomplete_probes}",
                        total_visits / HELD_OUT,
                        matched,
                    );
                    if incomplete_probes != 0 {
                        failures.push(format!(
                        "route={route} population={population} allowed={allowed_count} reported \
                         {incomplete_probes} of {HELD_OUT} walks incomplete, so those rows are \
                         diagnostic and never an answer"
                    ));
                    }
                    // A scope wider than one query's declared breadth is what the
                    // maintained graph exists for, and it is where the walk must
                    // stay inside that breadth. A scope at or below it is cheaper
                    // to compare exhaustively, and is the route the automatic mode
                    // already takes, so the walk's own cost there is bounded by the
                    // scope rather than by the breadth.
                    if matched * 100 < possible * 95 {
                        failures.push(format!(
                            "route={route} population={population} allowed={allowed_count} \
                         recall@{K} is {matched}/{possible}, under the required .95"
                        ));
                    }
                }
            }

            // A budget that cannot pay for the walk this filter needs must come
            // back as the declared limit it ran into, never as a short answer
            // dressed as a complete one.
            let tight = 200usize;
            let allowed = |ordinal: usize| ordinal.is_multiple_of(50);
            let allowed_count = (0..population).filter(|o| allowed(*o)).count();
            for (route, graph) in [("resident", &resident), ("paged", &paged)] {
                let refused = graph
                    .search_allowed_with_bounded_memory_at_ef(
                        &VectorIndexRef::new("items", "embedding"),
                        &spread(2, 0, DIMENSIONS),
                        K,
                        allowed_count,
                        HnswAllowedSearchLimits {
                            max_visited_nodes: tight,
                            max_vector_evaluations: tight,
                        },
                        EF_SEARCH,
                        std::iter::empty,
                        true,
                        |row_id| allowed(row_id.0 as usize - 1),
                        || Ok::<_, Error>(()),
                        || Ok::<_, Error>(()),
                        |_| Ok::<_, Error>(()),
                        |_| {},
                    )
                    .expect("a budget that runs out is a status, not an error");
                println!(
                    "selective_recall route={route} population={population} \
                     allowed={allowed_count} budget={tight} status={:?} visits={}",
                    refused.status, refused.visited_nodes,
                );
                if !matches!(refused.status, HnswAllowedSearchStatus::Incomplete(_)) {
                    failures.push(format!(
                        "route={route} population={population} allowed={allowed_count} answered \
                         as complete inside {tight} visits, which this filter cannot be searched in"
                    ));
                }
                if refused.visited_nodes > tight.saturating_mul(3) {
                    failures.push(format!(
                        "route={route} population={population} allowed={allowed_count} spent \
                         {} visits against a declared ceiling of {tight}",
                        refused.visited_nodes,
                    ));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "the maintained graph must keep its recall at every selectivity, on the route a \
             reader really opens:\n{}",
            failures.join("\n")
        );
    }

    #[test]
    fn ordinary_allowed_search_excluded_history_adds_no_visit_or_distance_work() {
        let mut entries = Vec::new();
        for ordinal in 0..8_u64 {
            entries.push(entry(
                RowId(ordinal + 1),
                &[ordinal as f32 + 1.0, 9.0 - ordinal as f32, 1.0],
                TxId(ordinal + 1),
            ));
        }
        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1,
            hnsw_m: 8,
            hnsw_ef_construction: 32,
            hnsw_ef_search: 16,
            policy_revision: 1,
            auto_index_at_source: "declared",
            ef_search_source: "declared",
        };
        let graph = HnswIndex::new_with_policy(&entries, 3, VectorQuantization::F32, policy);
        let search = |graph: &HnswIndex| {
            graph
                .search_allowed_at_ef(
                    &VectorIndexRef::new("items", "embedding"),
                    &[1.0, 8.0, 1.0],
                    4,
                    8,
                    16,
                    std::iter::empty,
                    false,
                    |row_id| row_id.0 <= 8,
                    |_| Ok(()),
                    |_| {},
                )
                .expect("ordinary allowed-id traversal completes or refuses within its fixed work")
        };
        let before_history = search(&graph);
        for ordinal in 8..2_000_u64 {
            graph
                .insert(&entry(
                    RowId(ordinal + 1),
                    &[ordinal as f32 + 1.0, 2_001.0 - ordinal as f32, 1.0],
                    TxId(ordinal + 1),
                ))
                .expect("append excluded history to the maintained graph");
        }
        let after_history = search(&graph);

        assert!(
            after_history.visited_nodes <= before_history.visited_nodes,
            "excluded history does not add allowed graph visits"
        );
        assert!(
            after_history.vector_evaluations <= before_history.vector_evaluations,
            "excluded history does not add allowed vector distances"
        );
        assert!(after_history.rows.iter().all(|(row_id, _)| row_id.0 <= 8));
    }
}
