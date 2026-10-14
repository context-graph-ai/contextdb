use crate::ScratchBackend;
use crate::transaction_tracker::TransactionId;
#[cfg(feature = "contextdb-internal")]
use crate::transactions::DurablePublication;
use crate::transactions::{AllocatorStateKey, AllocatorStateTree, AllocatorStateTreeMut};
use crate::tree_store::btree_base::{BtreeHeader, Checksum};
use crate::tree_store::page_store::allocator_pager::AllocatorPager;
use crate::tree_store::page_store::base::{MAX_PAGE_INDEX, PageHint};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::PagedCachedFile;
use crate::tree_store::page_store::fast_hash::PageNumberHashSet;
use crate::tree_store::page_store::header::{DB_HEADER_SIZE, DatabaseHeader, MAGICNUMBER};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{
    Allocators, FREE_TOTALS_ENCODED_LEN, FreeTotals, RegionTracker, SavedBitmapWindow, layout_pages,
};
use crate::tree_store::page_store::{PageImpl, PageMut, hash128_with_seed};
use crate::tree_store::{Page, PageNumber, PageTrackerPolicy};
use crate::{CacheStats, StorageBackend};
use crate::{DatabaseError, Result, StorageError};
use std::cmp::{max, min};
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::convert::TryInto;
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
#[cfg(debug_assertions)]
use std::sync::Arc;
use std::sync::Arc as StdArc;
use std::sync::Mutex;

struct SavedAllocatorValue<'a> {
    mem: &'a TransactionalMemory,
    page: PageNumber,
    range: std::ops::Range<usize>,
    hint: PageHint,
}
impl SavedAllocatorValue<'_> {
    fn read(&self, start: usize, out: &mut [u8]) -> io::Result<()> {
        let end = start
            .checked_add(out.len())
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "allocator offset overflow"))?;
        if end > self.range.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "truncated allocator bitmap",
            ));
        }
        let data = self
            .mem
            .get_page_window(
                self.page,
                self.range.start + start..self.range.start + end,
                self.hint,
            )
            .map_err(io::Error::other)?;
        out.copy_from_slice(&data);
        Ok(())
    }
    fn u32(&self, offset: usize) -> io::Result<usize> {
        let mut word = [0; 4];
        self.read(offset, &mut word)?;
        Ok(u32::from_le_bytes(word) as usize)
    }
    fn bitmap_window(
        &self,
        region: Option<u32>,
        order: u8,
        height: u8,
        page: u32,
        out: &mut [u8; 4096],
    ) -> io::Result<()> {
        out.fill(0xFF);
        let invalid = || io::Error::new(ErrorKind::InvalidData, "invalid allocator bitmap offsets");
        let (start, end) = if region.is_some() {
            let mut max_order = [0];
            self.read(0, &mut max_order)?;
            if max_order[0] > MAX_MAX_PAGE_ORDER {
                return Err(invalid());
            }
            if order > max_order[0] {
                return Ok(());
            }
            let start = if order == 0 {
                8 + (usize::from(max_order[0]) + 1) * 4
            } else {
                self.u32(8 + (usize::from(order) - 1) * 4)?
            };
            (start, self.u32(8 + usize::from(order) * 4)?)
        } else {
            let orders = self.u32(0)?;
            if orders != usize::from(MAX_MAX_PAGE_ORDER) + 1 || usize::from(order) >= orders {
                return Err(invalid());
            }
            // Order lengths are fixed-size geometry, not bitmap payload.
            let mut lengths = [0; 84];
            self.read(4, &mut lengths)?;
            let mut start = 4 + orders * 4;
            for field in lengths.chunks_exact(4).take(usize::from(order)) {
                start = start
                    .checked_add(u32::from_le_bytes(field.try_into().unwrap()) as usize)
                    .ok_or_else(invalid)?;
            }
            let field = usize::from(order) * 4;
            let len = u32::from_le_bytes(lengths[field..field + 4].try_into().unwrap()) as usize;
            (start, start.checked_add(len).ok_or_else(invalid)?)
        };
        if start > end || end > self.range.len() {
            return Err(invalid());
        }
        let count = self.u32(start)?;
        if count == 0 || count > 4 || 4 + count * 4 > end - start {
            return Err(invalid());
        }
        if usize::from(height) >= count {
            return Ok(());
        }
        let layer_start = if height == 0 {
            4 + count * 4
        } else {
            self.u32(start + 4 + (usize::from(height) - 1) * 4)?
        };
        let layer_end = self.u32(start + 4 + usize::from(height) * 4)?;
        if layer_start > layer_end || layer_end > end - start || layer_end - layer_start < 4 {
            return Err(invalid());
        }
        let words_start = start + layer_start + 4;
        let words_len = layer_end - layer_start - 4;
        let offset = page as usize * out.len();
        if offset < words_len {
            let len = (words_len - offset).min(out.len());
            self.read(words_start + offset, &mut out[..len])?;
        }
        Ok(())
    }
}
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

// The region header is optional in the v3 file format
// It's an artifact of the v2 file format, so we initialize new databases without headers to save space
const NO_HEADER: u32 = 0;

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
const MAX_USABLE_REGION_SPACE: u64 = 4 * 1024 * 1024 * 1024;
// TODO: remove this constant?
pub(crate) const MAX_MAX_PAGE_ORDER: u8 = 20;
pub(super) const MIN_USABLE_PAGES: u32 = 10;
const MIN_DESIRED_USABLE_BYTES: u64 = 1024 * 1024;

pub(super) const INITIAL_REGIONS: u32 = 1000; // Enough for a 4TiB database

// Original file format. No lengths stored with btrees
pub(crate) const FILE_FORMAT_VERSION1: u8 = 1;
// New file format. All btrees have a separate length stored in their header for constant time access
pub(crate) const FILE_FORMAT_VERSION2: u8 = 2;
// New file format:
// * Allocator state is stored in a system table, instead of in the region headers
// * Freed tree split into two system tables: one for the data tables, and one for the system tables
//   It is no longer stored in a separate tree
// * New "allocated pages table" which tracks the pages allocated, in the data tree, by a transaction.
//   This is a system table. It is only written when a savepoint exists
// * New persistent savepoint format
pub(crate) const FILE_FORMAT_VERSION3: u8 = 3;

#[derive(Copy, Clone)]
pub(crate) enum ShrinkPolicy {
    // Try to shrink the file by the default amount
    Default,
    // Try to shrink the file by the maximum amount
    Maximum,
    // Do not try to shrink the file
    Never,
}

fn ceil_log2(x: usize) -> u8 {
    if x.is_power_of_two() {
        x.trailing_zeros().try_into().unwrap()
    } else {
        x.next_power_of_two().trailing_zeros().try_into().unwrap()
    }
}

pub(crate) fn xxh3_checksum(data: &[u8]) -> Checksum {
    hash128_with_seed(data, 0)
}

#[cfg(feature = "test-seams")]
thread_local! {
    /// Completed free-total recounts on this thread. Production-dead.
    static FREE_TOTAL_RECOUNTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Runs as a recount on this thread begins, so a test can arm a storage
    /// fault that fires inside the count. Production-dead.
    static FREE_TOTAL_RECOUNT_START: std::cell::RefCell<Option<Box<dyn Fn()>>> =
        const { std::cell::RefCell::new(None) };
    /// How the next allocator-state save on this thread treats the free
    /// totals. Production-dead.
    static FREE_TOTALS_SAVE_OVERRIDE: std::cell::RefCell<Option<crate::db::FreeTotalsSaveForTest>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
fn free_totals_save_override() -> Option<crate::db::FreeTotalsSaveForTest> {
    FREE_TOTALS_SAVE_OVERRIDE.with(|slot| slot.borrow().clone())
}

thread_local! {
    /// While set, ordinary allocations on this thread take the lowest free
    /// pages instead of the first free block of their order.
    static LOWEST_PLACEMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Restores the thread's previous placement when dropped.
pub(crate) struct LowestPlacementScope {
    previous: bool,
}

impl Drop for LowestPlacementScope {
    fn drop(&mut self) {
        LOWEST_PLACEMENT.with(|lowest| lowest.set(self.previous));
    }
}

struct InMemoryState {
    header: DatabaseHeader,
    // TODO: we should make this an Option because it is only valid after the Database initializes it
    allocators: Allocators,
}

impl InMemoryState {
    fn new(
        header: DatabaseHeader,
        scratch: Option<Box<dyn ScratchBackend>>,
        cache_size: usize,
    ) -> Result<Self> {
        let allocators = if let Some(scratch) = scratch {
            let mut allocators = Allocators::new_paged(header.layout(), scratch)?;
            allocators.set_window_budget(cache_size)?;
            allocators
        } else {
            Allocators::new(header.layout())
        };
        Ok(Self { header, allocators })
    }

    fn get_region(&mut self, region: u32) -> &BuddyAllocator {
        self.allocators.get_region(region)
    }

    fn get_region_mut(&mut self, region: u32) -> &mut BuddyAllocator {
        self.allocators.get_region_mut(region)
    }

    fn get_region_tracker_mut(&mut self) -> &mut RegionTracker {
        &mut self.allocators.region_tracker
    }
}

// Armed throughout header publication until that same header is installed.
// Drop must remain infallible and independent of every storage mutex.
#[cfg(feature = "contextdb-internal")]
struct PublicationRecoveryGuard<'a> {
    needs_recovery: &'a AtomicBool,
    publication_needs_recovery: &'a AtomicBool,
    armed: bool,
}

#[cfg(feature = "contextdb-internal")]
impl Drop for PublicationRecoveryGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.needs_recovery.store(true, Ordering::Release);
            self.publication_needs_recovery
                .store(true, Ordering::Release);
        }
    }
}

pub(crate) struct TransactionalMemory {
    // Pages allocated since the last commit
    // TODO: maybe this should be moved to WriteTransaction?
    allocated_since_commit: Mutex<PageNumberHashSet>,
    unpersisted: Mutex<PageNumberHashSet>,
    // True if the allocator state was corrupted when the file was opened
    // TODO: maybe we can remove this flag now that CheckedBackend exists?
    needs_recovery: AtomicBool,
    #[cfg(feature = "contextdb-internal")]
    publication_needs_recovery: AtomicBool,
    storage: PagedCachedFile,
    state: Mutex<InMemoryState>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Arc<Mutex<HashSet<PageNumber>>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Arc<Mutex<HashMap<PageNumber, u64>>>,
    // Set of all allocated pages for debugging assertions
    #[cfg(debug_assertions)]
    allocated_pages: Arc<Mutex<PageNumberHashSet>>,
    // Indicates that a non-durable commit has been made, so reads should be served from the secondary meta page
    read_from_secondary: AtomicBool,
    page_size: u32,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: u64,
    region_header_with_padding_size: u64,
}

impl TransactionalMemory {
    // Only the publishing sibling sets this flag: ordinary commit is unchanged.
    // Check before taking state locks, including poisoned locks after unwind.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn check_publication_recovery(&self) -> Result {
        if self.publication_needs_recovery.load(Ordering::Acquire) {
            return Err(StorageError::PreviousIo);
        }
        Ok(())
    }

    pub(crate) fn allocator_resident_bytes(&self) -> usize {
        self.state.lock().unwrap().allocators.resident_bytes()
    }

    pub(crate) fn allocator_region_counts(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (
            state.allocators.loaded_region_count(),
            state.allocators.region_count(),
        )
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn allocator_metadata_bytes_for_test(&self) -> usize {
        self.allocator_resident_bytes()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn allocator_regions_for_test(&self) -> usize {
        self.state.lock().unwrap().allocators.region_count()
    }

    pub(crate) fn new(
        file: Box<dyn StorageBackend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        external_read: bool,
    ) -> Result<Self, DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);
        assert!(!external_read || read_only);
        let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
        let region_size = min(
            region_size,
            (u64::from(MAX_PAGE_INDEX) + 1) * page_size as u64,
        );
        assert!(region_size.is_power_of_two());

        let scratch = if read_only {
            None
        } else {
            file.scratch_backend()?
        };
        // A paged allocator's windows are an allotment of the cache share.
        let allocator_window_bytes = if scratch.is_some() {
            AllocatorPager::cache_bytes_taken(cache_size)
        } else {
            0
        };
        let storage =
            PagedCachedFile::new(file, page_size as u64, cache_size - allocator_window_bytes)?;

        let initial_storage_len = storage.raw_file_len()?;

        let magic_number: [u8; MAGICNUMBER.len()] =
            if initial_storage_len >= MAGICNUMBER.len() as u64 {
                storage
                    .read_direct(0, MAGICNUMBER.len())?
                    .try_into()
                    .unwrap()
            } else {
                [0; MAGICNUMBER.len()]
            };

        if initial_storage_len > 0 {
            // File already exists check that the magic number matches
            if magic_number != MAGICNUMBER {
                return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
            }
        } else {
            // File is empty, check that we're allowed to initialize a new database (i.e. the caller is Database::create() and not open())
            if !allow_initialize {
                return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
            }
        }

        if magic_number != MAGICNUMBER {
            let region_tracker_required_bytes =
                RegionTracker::new(INITIAL_REGIONS, MAX_MAX_PAGE_ORDER + 1)
                    .to_vec()
                    .len();

            // Make sure that there is enough room to allocate the region tracker into a page
            let size: u64 = max(
                MIN_DESIRED_USABLE_BYTES,
                page_size as u64 * u64::from(MIN_USABLE_PAGES),
            );
            let tracker_space =
                (page_size * region_tracker_required_bytes.div_ceil(page_size)) as u64;
            let starting_size = size + tracker_space;

            let page_capacity = (region_size / u64::try_from(page_size).unwrap())
                .try_into()
                .unwrap();
            let layout = DatabaseLayout::calculate(
                starting_size,
                page_capacity,
                NO_HEADER,
                page_size.try_into().unwrap(),
            );

            {
                let file_len = storage.raw_file_len()?;

                if file_len < layout.len() {
                    storage.resize(layout.len())?;
                }
            }

            let mut header = DatabaseHeader::new(layout, TransactionId::new(0));

            header.recovery_required = false;
            header.two_phase_commit = true;
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(false));

            storage.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }
        let header_bytes = storage.read_direct(0, DB_HEADER_SIZE)?;
        let (mut header, repair_info) = DatabaseHeader::from_bytes(&header_bytes)?;
        // Page size is fixed at creation for both native and external reads.
        assert_eq!(header.page_size() as usize, page_size);
        if !external_read {
            assert!(storage.raw_file_len()? >= header.layout().len());
        }
        let needs_recovery =
            header.recovery_required || header.layout().len() != storage.raw_file_len()?;
        // An externally pinned user root does not consult allocator/system
        // state or select a root from this header. Its caller retains every
        // reachable page, even while a different writer owns the file.
        if needs_recovery && !external_read {
            if read_only {
                return Err(DatabaseError::RepairAborted);
            }
            let layout = header.layout();
            let region_max_pages = layout.full_region_layout().num_pages();
            let region_header_pages = layout.full_region_layout().get_header_pages();
            header.set_layout(DatabaseLayout::recalculate(
                storage.raw_file_len()?,
                region_header_pages,
                region_max_pages,
                page_size.try_into().unwrap(),
            ));
            header.pick_primary_for_repair(repair_info)?;
            assert!(!repair_info.invalid_magic_number);
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }
        // All remaining address calculations use this captured geometry.
        let layout = header.layout();
        // Registered roots use fixed region/page geometry. A writer may grow
        // the file after this header read, so its current length is not a
        // consistency condition for the external reader's retained pages.
        if !external_read {
            assert_eq!(layout.len(), storage.raw_file_len()?);
        }
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;
        let state = InMemoryState::new(header, scratch, cache_size)?;

        assert!(page_size >= DB_HEADER_SIZE);

        Ok(Self {
            allocated_since_commit: Mutex::new(Default::default()),
            unpersisted: Mutex::new(Default::default()),
            needs_recovery: AtomicBool::new(needs_recovery),
            #[cfg(feature = "contextdb-internal")]
            publication_needs_recovery: AtomicBool::new(false),
            storage,
            state: Mutex::new(state),
            #[cfg(debug_assertions)]
            open_dirty_pages: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(debug_assertions)]
            read_page_ref_counts: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(debug_assertions)]
            allocated_pages: Arc::new(Mutex::new(Default::default())),
            read_from_secondary: AtomicBool::new(false),
            page_size: page_size.try_into().unwrap(),
            region_size,
            region_header_with_padding_size: region_header_size,
        })
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn settle_startup_cache_budget(&self, bytes: usize) -> Result<usize> {
        // The allocator window set takes its allotment of the share first;
        // the page cache settles on the rest, so the reserve stays one charge.
        let allocator_window_bytes = self
            .state
            .lock()
            .unwrap()
            .allocators
            .set_window_budget(bytes)?;
        let reserve = self
            .storage
            .settle_startup_cache_budget(bytes - allocator_window_bytes)?;
        Ok(reserve + allocator_window_bytes)
    }

    pub(crate) fn cache_index_bytes(&self) -> usize {
        self.storage.index_bytes()
    }

    pub(crate) fn cache_stripe_control_bytes(&self) -> usize {
        self.storage.stripe_control_bytes()
    }

    pub(crate) fn cache_stats(&self) -> CacheStats {
        self.storage.cache_stats()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        if let Some(error) = self
            .state
            .lock()
            .unwrap()
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        self.storage.check_io_errors()
    }

    #[cfg(debug_assertions)]
    pub(crate) fn uses_paged_allocator(&self) -> bool {
        self.state.lock().unwrap().allocators.pager().is_some()
    }

    pub(crate) fn has_saved_allocator_source(&self) -> bool {
        self.state.lock().unwrap().allocators.has_saved_source()
    }

    pub(crate) fn materialize_allocator_source(&self) -> Result {
        let mut state = self.state.lock().unwrap();
        state.allocators.materialize_saved_source()?;
        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn materialize_small_allocator_source(&self) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        let retired = state.allocators.materialize_saved_source_if_small()?;
        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        Ok(retired)
    }

    #[cfg(debug_assertions)]
    pub(crate) fn mark_debug_allocated_page(&self, page: PageNumber) {
        assert!(self.allocated_pages.lock().unwrap().insert(page));
    }

    #[cfg(debug_assertions)]
    pub(crate) fn all_allocated_pages(&self) -> Vec<PageNumber> {
        let pages: Vec<PageNumber> = self
            .allocated_pages
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect();
        pages
    }

    #[cfg(debug_assertions)]
    pub(crate) fn debug_check_allocator_consistency(&self) {
        let mut state = self.state.lock().unwrap();
        if state.allocators.pager().is_some() {
            return;
        }
        let mut region_pages = vec![vec![]; state.allocators.region_count()];
        for p in self.allocated_pages.lock().unwrap().iter() {
            region_pages[p.region as usize].push(*p);
        }
        for (i, pages) in region_pages.iter().enumerate() {
            state
                .get_region(i.try_into().unwrap())
                .check_allocated_pages(i.try_into().unwrap(), pages);
        }
    }

    pub(crate) fn clear_read_cache(&self) {
        self.storage.invalidate_cache_all();
    }

    pub(crate) fn clear_cache_and_reload(&mut self) -> Result<bool, DatabaseError> {
        assert!(self.allocated_since_commit.lock().unwrap().is_empty());

        self.storage.flush()?;
        self.storage.invalidate_cache_all();

        let header_bytes = self.storage.read_direct(0, DB_HEADER_SIZE)?;
        let (mut header, repair_info) = DatabaseHeader::from_bytes(&header_bytes)?;
        // TODO: This ends up always being true because this is called from check_integrity() once the db is already open
        // TODO: Also we should recheck the layout
        let mut was_clean = true;
        if header.recovery_required {
            if !header.pick_primary_for_repair(repair_info)? {
                was_clean = false;
            }
            if repair_info.invalid_magic_number {
                return Err(StorageError::Corrupted("Invalid magic number".to_string()).into());
            }
            self.storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            self.storage.flush()?;
        }

        self.needs_recovery
            .store(header.recovery_required, Ordering::Release);
        let mut state = self.state.lock().unwrap();
        state.allocators.resize_to(header.layout())?;
        state.header = header;

        Ok(was_clean)
    }

    pub(crate) fn begin_writable(&self) -> Result {
        let mut state = self.state.lock().unwrap();
        assert!(!state.header.recovery_required);
        state.header.recovery_required = true;
        self.write_header(&state.header)?;
        self.storage.flush()
    }

    pub(crate) fn used_two_phase_commit(&self) -> bool {
        self.state.lock().unwrap().header.two_phase_commit
    }

    pub(crate) fn allocator_hash(&self) -> u128 {
        self.state.lock().unwrap().allocators.xxh3_hash()
    }

    // TODO: need a clearer distinction between this and needs_repair()
    pub(crate) fn storage_failure(&self) -> bool {
        self.needs_recovery.load(Ordering::Acquire)
    }

    pub(crate) fn repair_primary_corrupted(&self) {
        let mut state = self.state.lock().unwrap();
        state.header.swap_primary_slot();
    }

    pub(crate) fn begin_repair(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let layout = state.header.layout();
        state.allocators.reset_for_repair(layout)?;
        #[cfg(debug_assertions)]
        self.allocated_pages.lock().unwrap().clear();

        Ok(())
    }

    pub(crate) fn mark_page_allocated(&self, page_number: PageNumber) {
        let mut state = self.state.lock().unwrap();
        let region_index = page_number.region;
        let allocator = state.get_region_mut(region_index);
        allocator.record_alloc(page_number.page_index, page_number.page_order);
        #[cfg(debug_assertions)]
        if cfg!(test) || state.allocators.pager().is_none() {
            assert!(self.allocated_pages.lock().unwrap().insert(page_number));
        }
    }

    fn write_header(&self, header: &DatabaseHeader) -> Result {
        self.storage
            .write(0, DB_HEADER_SIZE, true)?
            .mem_mut()
            .copy_from_slice(&header.to_bytes(true));

        Ok(())
    }

    pub(crate) fn end_repair(&self) -> Result<()> {
        self.check_io_errors()?;
        let mut state = self.state.lock().unwrap();
        // Repair re-marked every reachable page without moving the totals.
        // Rebuild them before recovery state clears; a failed recount fails
        // the repair like any other repair I/O failure.
        Self::recount_free_totals(&mut state)?;
        state.header.recovery_required = false;
        self.write_header(&state.header)?;
        let result = self.storage.flush();
        self.needs_recovery.store(false, Ordering::Release);

        result
    }

    pub(crate) fn reserve_allocator_state(
        &self,
        tree: &mut AllocatorStateTreeMut,
        transaction_id: TransactionId,
    ) -> Result<u32> {
        self.check_io_errors()?;
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let num_regions = layout.num_regions();
        let region_tracker_len = state.allocators.region_tracker.encoded_len();
        drop(state);

        for i in 0..num_regions {
            let region_bytes_len = self
                .state
                .lock()
                .unwrap()
                .allocators
                .get_region(i)
                .encoded_len();
            drop(tree.insert_zeroed_reserve(&AllocatorStateKey::Region(i), region_bytes_len)?);
        }

        drop(tree.insert_zeroed_reserve(&AllocatorStateKey::RegionTracker, region_tracker_len)?);
        // Reserved before anything is encoded, so the encode allocates nothing.
        #[cfg(feature = "test-seams")]
        let reserved_totals_len = match free_totals_save_override() {
            Some(crate::db::FreeTotalsSaveForTest::Omit) => None,
            Some(crate::db::FreeTotalsSaveForTest::Write(bytes)) => Some(bytes.len()),
            _ => Some(FREE_TOTALS_ENCODED_LEN),
        };
        #[cfg(not(feature = "test-seams"))]
        let reserved_totals_len = Some(FREE_TOTALS_ENCODED_LEN);
        if let Some(len) = reserved_totals_len {
            drop(tree.insert_zeroed_reserve(&AllocatorStateKey::FreeTotals, len)?);
        }

        tree.insert(
            &AllocatorStateKey::TransactionId,
            &transaction_id.raw_id().to_le_bytes().as_ref(),
        )?;

        Ok(num_regions)
    }

    // Returns true on success, or false if the number of regions has changed
    pub(crate) fn try_save_allocator_state(
        &self,
        tree: &mut AllocatorStateTreeMut,
        num_regions: u32,
    ) -> Result<bool> {
        #[cfg(test)]
        allocator_growth_tests::SAVE_CALLS.with(|calls| calls.set(calls.get() + 1));
        self.check_io_errors()?;
        // Has the number of regions changed since reserve_allocator_state() was called?
        if num_regions != self.state.lock().unwrap().header.layout().num_regions() {
            return Ok(false);
        }

        for i in 0..num_regions {
            let mut destination = tree.get_mut(&AllocatorStateKey::Region(i))?.unwrap();
            let mut state = self.state.lock().unwrap();
            let region = state.allocators.get_region(i);
            if destination.value().len() < region.encoded_len() {
                // The allocator state grew too much since we reserved space
                return Ok(false);
            }
            region.encode_into(destination.value_mut());
            drop(state);
            drop(destination);
        }

        let mut destination = tree.get_mut(&AllocatorStateKey::RegionTracker)?.unwrap();
        let state = self.state.lock().unwrap();
        if destination.value().len() < state.allocators.region_tracker.encoded_len() {
            // The allocator state grew too much since we reserved space
            return Ok(false);
        }
        state
            .allocators
            .region_tracker
            .encode_into(destination.value_mut());
        // The totals describe the same snapshot as the bitmaps above: nothing
        // allocates or frees between those encodes and this commit. Untrusted
        // totals are not written; the reserved value stays all-zero (format
        // 0), which every load rejects, so the next open recounts. That is
        // terminal for this save, not a reason to retry it.
        let encoded_totals = state.allocators.encode_free_totals(&state.header.layout());
        drop(state);
        drop(destination);
        #[cfg(feature = "test-seams")]
        let encoded_totals: Option<Vec<u8>> = match FREE_TOTALS_SAVE_OVERRIDE
            .with(|slot| slot.borrow_mut().take())
        {
            Some(crate::db::FreeTotalsSaveForTest::FailBeforeEncode) => {
                return Err(StorageError::Io(io::Error::other(
                    "injected failure before the free totals were encoded",
                )));
            }
            Some(crate::db::FreeTotalsSaveForTest::FailAfterEncode) => {
                if let Some(encoded) = encoded_totals {
                    let mut destination = tree.get_mut(&AllocatorStateKey::FreeTotals)?.unwrap();
                    destination.value_mut()[..FREE_TOTALS_ENCODED_LEN].copy_from_slice(&encoded);
                }
                return Err(StorageError::Io(io::Error::other(
                    "injected failure after the free totals were encoded",
                )));
            }
            Some(crate::db::FreeTotalsSaveForTest::Omit) => None,
            Some(crate::db::FreeTotalsSaveForTest::Write(bytes)) => Some(bytes),
            None => encoded_totals.map(|encoded| encoded.to_vec()),
        };
        if let Some(encoded) = encoded_totals {
            // Reserved and already written in this transaction, so this
            // borrow allocates nothing.
            let mut destination = tree.get_mut(&AllocatorStateKey::FreeTotals)?.unwrap();
            let len = encoded.len();
            destination.value_mut()[..len].copy_from_slice(&encoded);
        }

        let state = self.state.lock().unwrap();
        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        Ok(true)
    }

    // Returns true if the allocator state table is up to date, or false if it's stale
    pub(crate) fn is_valid_allocator_state(&self, tree: &AllocatorStateTree) -> Result<bool> {
        // See if this is stale allocator state left over from a previous transaction. That won't
        // happen during normal operation, since WriteTransaction::commit() always updates the
        // allocator state table before calling TransactionalMemory::commit(), but there are also
        // a few places where TransactionalMemory::commit() is called directly without using a
        // WriteTransaction. When that happens, any existing allocator state table will be left
        // in place but is no longer valid. (And even if there were no such calls today, it would
        // be an easy mistake to make! So it's good that we check.)
        let Some(value) = tree.get(&AllocatorStateKey::TransactionId)? else {
            return Ok(false);
        };
        let transaction_id =
            TransactionId::new(u64::from_le_bytes(value.value().try_into().unwrap()));

        Ok(transaction_id == self.get_last_committed_transaction_id()?)
    }

    fn saved_free_totals(tree: &AllocatorStateTree) -> Result<Option<Vec<u8>>> {
        Ok(tree
            .get(&AllocatorStateKey::FreeTotals)?
            .map(|value| value.value().to_vec()))
    }

    /// Adopt the saved totals for the committed geometry, or rebuild them
    /// from the loaded bitmaps when the value is absent, unwritten or does not
    /// fit that geometry.
    fn adopt_or_recount_free_totals(state: &mut InMemoryState, saved: Option<&[u8]>) -> Result {
        let layout = state.header.layout();
        match saved.and_then(|bytes| FreeTotals::decode_for_layout(bytes, &layout)) {
            Some(totals) => {
                state.allocators.set_free_totals(totals);
                Ok(())
            }
            None => Self::recount_free_totals(state),
        }
    }

    /// Rebuild the totals from every region's bitmap. The totals become
    /// trusted only if the complete count finished with a healthy pager; a
    /// failed count publishes nothing and returns the storage error, and the
    /// totals stay untrusted for the rest of this open.
    fn recount_free_totals(state: &mut InMemoryState) -> Result {
        state.allocators.mark_free_totals_untrusted();
        #[cfg(feature = "test-seams")]
        FREE_TOTAL_RECOUNT_START.with(|hook| {
            if let Some(hook) = hook.borrow().as_ref() {
                hook();
            }
        });
        let layout = state.header.layout();
        let mut free_pages = 0u64;
        for region in 0..layout.num_regions() {
            free_pages += u64::from(state.get_region(region).count_free_pages());
        }
        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        let region_pages = layout_pages(&layout);
        if free_pages > region_pages {
            return Err(StorageError::Corrupted(
                "allocator free pages exceed the region pages".to_owned(),
            ));
        }
        #[cfg(feature = "test-seams")]
        FREE_TOTAL_RECOUNTS.with(|count| count.set(count.get() + 1));
        state
            .allocators
            .set_recounted_free_totals(free_pages, region_pages);
        Ok(())
    }

    /// Completed free-total recounts on the calling thread, cumulatively.
    #[cfg(feature = "test-seams")]
    pub(crate) fn free_total_recounts_for_test() -> u64 {
        FREE_TOTAL_RECOUNTS.with(std::cell::Cell::get)
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn free_total_geometry_region_visits_for_test() -> u64 {
        crate::tree_store::page_store::region::geometry_region_visits_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn set_free_totals_save_for_test(mode: Option<crate::db::FreeTotalsSaveForTest>) {
        FREE_TOTALS_SAVE_OVERRIDE.with(|slot| *slot.borrow_mut() = mode);
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn set_free_total_recount_start_hook_for_test(hook: Option<Box<dyn Fn()>>) {
        FREE_TOTAL_RECOUNT_START.with(|slot| *slot.borrow_mut() = hook);
    }

    /// Drop trust in the maintained totals, as a poisoned allocator would.
    #[cfg(feature = "test-seams")]
    pub(crate) fn distrust_free_totals_for_test(&self) {
        self.state
            .lock()
            .unwrap()
            .allocators
            .mark_free_totals_untrusted();
    }

    /// Trusted `(free pages, region pages, trailing free pages)`; `None` while
    /// untrusted. The trailing free pages end the last region: a trim gives
    /// them back without relocating anything. Counting them walks the free
    /// buddy blocks that end at the tail, which coalescing bounds by the order
    /// count; `try_shrink` does the same on every shrinking commit.
    pub(crate) fn allocator_free_totals(&self) -> Option<(u64, u64, u64)> {
        let mut state = self.state.lock().unwrap();
        let (free, pages) = state.allocators.free_totals()?;
        let trailing = Self::trailing_free_pages(&mut state)?;
        Some((free, pages, trailing))
    }

    fn trailing_free_pages(state: &mut InMemoryState) -> Option<u64> {
        let last = state.header.layout().num_regions().checked_sub(1)?;
        let trailing = u64::from(state.get_region(last).trailing_free_pages());
        // A pager fault while reading the tail leaves no trusted answer.
        state.allocators.free_totals()?;
        Some(trailing)
    }

    /// Independent order-weighted recount of every region's free bits, for
    /// agreement tests. Leaves the maintained totals untouched.
    /// Per region: `(length, free pages, trailing free pages, highest free
    /// order)`. Reads every region's bitmap. Test diagnostics only.
    #[cfg(feature = "test-seams")]
    pub(crate) fn region_free_pages_for_test(&self) -> Vec<(u32, u32, u32, Option<u8>)> {
        let mut state = self.state.lock().unwrap();
        let regions = state.header.layout().num_regions();
        (0..regions)
            .map(|region| {
                let allocator = state.get_region(region);
                (
                    allocator.len(),
                    allocator.count_free_pages(),
                    allocator.trailing_free_pages(),
                    allocator.highest_free_order(),
                )
            })
            .collect()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn recount_free_totals_for_test(&self) -> Result<(u64, u64, u64)> {
        let mut state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let mut free_pages = 0u64;
        for region in 0..layout.num_regions() {
            free_pages += u64::from(state.get_region(region).count_free_pages());
        }
        let trailing = u64::from(
            state
                .get_region(layout.num_regions() - 1)
                .trailing_free_pages(),
        );
        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        Ok((free_pages, layout_pages(&layout), trailing))
    }

    pub(crate) fn load_allocator_state(&self, tree: &AllocatorStateTree) -> Result {
        assert!(self.is_valid_allocator_state(tree)?);

        if self.state.lock().unwrap().allocators.pager().is_some() {
            let mem = StdArc::downgrade(&tree.mem);
            let root = tree.get_root();
            let hint = tree.hint();
            let source: StdArc<SavedBitmapWindow> =
                StdArc::new(move |region, order, height, page, out| {
                    let mem = mem.upgrade().ok_or_else(|| {
                        io::Error::new(ErrorKind::BrokenPipe, "allocator source database closed")
                    })?;
                    let key =
                        region.map_or(AllocatorStateKey::RegionTracker, AllocatorStateKey::Region);
                    let Some((page_number, range)) =
                        crate::tree_store::btree::fixed_key_value_location::<AllocatorStateKey>(
                            root, &mem, hint, &key,
                        )
                        .map_err(io::Error::other)?
                    else {
                        if region.is_none() {
                            return Err(io::Error::new(
                                ErrorKind::InvalidData,
                                "saved region tracker is missing",
                            ));
                        }
                        return Ok(false);
                    };
                    SavedAllocatorValue {
                        mem: &mem,
                        page: page_number,
                        range,
                        hint,
                    }
                    .bitmap_window(region, order, height, page, out)?;
                    Ok(true)
                });
            // Saved tracker words remain in the durable source and share
            // AllocatorPager's fixed windows with regional bitmap words.
            // Geometry masks slots past the real regions without loading them.
            let saved_totals = Self::saved_free_totals(tree)?;
            let mut state = self.state.lock().unwrap();
            state.allocators.set_saved_source(source)?;
            Self::adopt_or_recount_free_totals(&mut state, saved_totals.as_deref())?;
            state.header.recovery_required = false;
            self.needs_recovery.store(false, Ordering::Release);
            return Ok(());
        }

        // Load the allocator state
        let mut regions = vec![];
        for region in
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))?
        {
            #[cfg(feature = "test-seams")]
            crate::db::allocator_metadata_test_seams::note_loaded_regions(1);
            regions.push(BuddyAllocator::from_bytes(region?.value()));
        }

        let region_tracker = RegionTracker::from_bytes(
            tree.get(&AllocatorStateKey::RegionTracker)?
                .unwrap()
                .value(),
        );

        let saved_totals = Self::saved_free_totals(tree)?;
        let mut state = self.state.lock().unwrap();
        state.allocators = Allocators {
            region_tracker,
            regions,
            paged: None,
            free_totals: FreeTotals::untrusted(),
        };

        // Resize the allocators to match the current file size
        let layout = state.header.layout();
        state.allocators.resize_to(layout)?;
        Self::adopt_or_recount_free_totals(&mut state, saved_totals.as_deref())?;
        drop(state);

        self.state.lock().unwrap().header.recovery_required = false;
        self.needs_recovery.store(false, Ordering::Release);

        Ok(())
    }

    #[cfg_attr(not(debug_assertions), expect(unused_variables))]
    pub(crate) fn is_allocated(&self, page: PageNumber) -> bool {
        #[cfg(debug_assertions)]
        {
            let mut state = self.state.lock().unwrap();
            // Unit tests ask whether this exact extent is still present after
            // relocation. A covering larger extent is not the same identity.
            if state.allocators.pager().is_some() && !cfg!(test) {
                state.get_region(page.region).is_allocated(page)
            } else {
                let allocated = self.allocated_pages.lock().unwrap();
                allocated.contains(&page)
            }
        }
        #[cfg(not(debug_assertions))]
        {
            unreachable!()
        }
    }

    // Commit all outstanding changes and make them visible as the primary
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        two_phase: bool,
        shrink_policy: ShrinkPolicy,
    ) -> Result {
        let result = self.commit_inner(
            data_root,
            system_root,
            transaction_id,
            two_phase,
            shrink_policy,
        );
        if result.is_err() {
            self.needs_recovery.store(true, Ordering::Release);
        }
        result
    }

    #[cfg(feature = "contextdb-internal")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_publishing(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        two_phase: bool,
        shrink_policy: ShrinkPolicy,
        marker: &DurablePublication,
    ) -> Result {
        marker.validate(self, transaction_id)?;
        let result = self.commit_inner_publishing(
            data_root,
            system_root,
            transaction_id,
            two_phase,
            shrink_policy,
            marker,
        );
        if result.is_err() {
            self.needs_recovery.store(true, Ordering::Release);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_inner(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        two_phase: bool,
        shrink_policy: ShrinkPolicy,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery.load(Ordering::Acquire));

        let mut state = self.state.lock().unwrap();
        // Trim surplus file space, before finalizing the commit
        let shrunk = if !matches!(shrink_policy, ShrinkPolicy::Never) {
            Self::try_shrink(&mut state, matches!(shrink_policy, ShrinkPolicy::Maximum))?
        } else {
            false
        };
        // Copy the header so that we can release the state lock, while we flush the file
        let mut header = state.header.clone();
        drop(state);
        self.check_io_errors()?;

        let old_transaction_id = header.secondary_slot().transaction_id;
        let secondary = header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;

        self.write_header(&header)?;

        // Use 2-phase commit, if checksums are disabled
        if two_phase {
            self.storage.flush()?;
        }

        // Make our new commit the primary, and record whether it was a 2-phase commit.
        // These two bits need to be written atomically
        header.swap_primary_slot();
        header.two_phase_commit = two_phase;

        // Write the new header to disk
        self.write_header(&header)?;
        self.storage.flush()?;

        if shrunk {
            let result = self.storage.resize(header.layout().len());
            if result.is_err() {
                // TODO: it would be nice to have a more cohesive approach to setting this.
                // we do it in commit() & rollback() on failure, but there are probably other places that need it
                self.needs_recovery.store(true, Ordering::Release);
                return result;
            }
        }
        let mut allocated_since_commit = self.allocated_since_commit.lock().unwrap();
        allocated_since_commit.clear();
        allocated_since_commit.shrink_to_fit();
        let mut unpersisted = self.unpersisted.lock().unwrap();
        unpersisted.clear();
        unpersisted.shrink_to_fit();

        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.header.secondary_slot().transaction_id,
            old_transaction_id
        );
        state.header = header;
        self.read_from_secondary.store(false, Ordering::Release);
        // Hold lock until read_from_secondary is set to false, so that the new primary state is read.
        // TODO: maybe we can remove the whole read_from_secondary flag?
        drop(state);

        Ok(())
    }

    #[cfg(feature = "contextdb-internal")]
    #[allow(clippy::too_many_arguments)]
    fn commit_inner_publishing(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        two_phase: bool,
        shrink_policy: ShrinkPolicy,
        marker: &DurablePublication,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery.load(Ordering::Acquire));

        let mut state = self.state.lock().unwrap();
        // Trim surplus file space, before finalizing the commit
        let shrunk = if !matches!(shrink_policy, ShrinkPolicy::Never) {
            Self::try_shrink(&mut state, matches!(shrink_policy, ShrinkPolicy::Maximum))?
        } else {
            false
        };
        // Copy the header so that we can release the state lock, while we flush the file
        let mut header = state.header.clone();
        drop(state);
        self.check_io_errors()?;

        let old_transaction_id = header.secondary_slot().transaction_id;
        let secondary = header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;

        let mut recovery_guard = PublicationRecoveryGuard {
            needs_recovery: &self.needs_recovery,
            publication_needs_recovery: &self.publication_needs_recovery,
            armed: true,
        };
        marker.header_begun();
        self.write_header(&header)?;

        // Use 2-phase commit, if checksums are disabled
        if two_phase {
            self.storage.flush()?;
        }

        // Make our new commit the primary, and record whether it was a 2-phase commit.
        // These two bits need to be written atomically
        header.swap_primary_slot();
        header.two_phase_commit = two_phase;

        // Write the new header to disk
        self.write_header(&header)?;
        self.storage.flush()?;
        marker.durable();

        if shrunk {
            let result = self.storage.resize(header.layout().len());
            if result.is_err() {
                // TODO: it would be nice to have a more cohesive approach to setting this.
                // we do it in commit() & rollback() on failure, but there are probably other places that need it
                self.needs_recovery.store(true, Ordering::Release);
                return result;
            }
        }
        let mut allocated_since_commit = self.allocated_since_commit.lock().unwrap();
        allocated_since_commit.clear();
        allocated_since_commit.shrink_to_fit();
        let mut unpersisted = self.unpersisted.lock().unwrap();
        unpersisted.clear();
        unpersisted.shrink_to_fit();

        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.header.secondary_slot().transaction_id,
            old_transaction_id
        );
        state.header = header;
        self.read_from_secondary.store(false, Ordering::Release);
        recovery_guard.armed = false;
        // Hold lock until read_from_secondary is set to false, so that the new primary state is read.
        // TODO: maybe we can remove the whole read_from_secondary flag?
        drop(state);

        Ok(())
    }

    // Make changes visible, without a durability guarantee
    pub(crate) fn non_durable_commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery.load(Ordering::Acquire));
        self.check_io_errors()?;

        let mut unpersisted = self.unpersisted.lock().unwrap();
        let mut allocated_since_commit = self.allocated_since_commit.lock().unwrap();
        unpersisted.extend(allocated_since_commit.drain());
        allocated_since_commit.shrink_to_fit();
        self.storage.write_barrier()?;

        let mut state = self.state.lock().unwrap();
        let secondary = state.header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;

        // TODO: maybe we can remove this flag and just update the in-memory DatabaseHeader state?
        self.read_from_secondary.store(true, Ordering::Release);

        Ok(())
    }

    pub(crate) fn rollback_uncommitted_writes(&self) -> Result {
        let result = self.rollback_uncommitted_writes_inner();
        if result.is_err() {
            self.needs_recovery.store(true, Ordering::Release);
        }
        result
    }

    fn rollback_uncommitted_writes_inner(&self) -> Result {
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(
                dirty_pages.is_empty(),
                "Dirty pages outstanding: {dirty_pages:?}"
            );
        }
        assert!(!self.needs_recovery.load(Ordering::Acquire));
        let mut state = self.state.lock().unwrap();
        let mut guard = self.allocated_since_commit.lock().unwrap();
        for page_number in guard.iter() {
            let region_index = page_number.region;
            state
                .get_region_tracker_mut()
                .mark_free(page_number.page_order, region_index);
            state
                .get_region_mut(region_index)
                .free(page_number.page_index, page_number.page_order);
            state.allocators.note_freed(page_number.page_order);
            #[cfg(debug_assertions)]
            if cfg!(test) || state.allocators.pager().is_none() {
                assert!(self.allocated_pages.lock().unwrap().remove(page_number));
            }

            let address = page_number.address_range(
                self.page_size.into(),
                self.region_size,
                self.region_header_with_padding_size,
                self.page_size,
            );
            let len: usize = (address.end - address.start).try_into().unwrap();
            self.storage.invalidate_cache(address.start, len);
            self.storage.cancel_pending_write(address.start, len);
        }
        guard.clear();
        guard.shrink_to_fit();

        Ok(())
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (range.end - range.start).try_into().unwrap();
        let mem = self.storage.read(range.start, len, hint)?;

        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(!dirty_pages.contains(&page_number), "{page_number:?}");
            *(self
                .read_page_ref_counts
                .lock()
                .unwrap()
                .entry(page_number)
                .or_default()) += 1;
            drop(dirty_pages);
        }

        Ok(PageImpl {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            open_pages: self.read_page_ref_counts.clone(),
        })
    }

    pub(crate) fn get_page_window(
        &self,
        page: PageNumber,
        range: std::ops::Range<usize>,
        hint: PageHint,
    ) -> Result<super::cached_file::PageMemory> {
        let address = page.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let extent_len = usize::try_from(address.end - address.start).unwrap();
        self.storage
            .read_window(address.start, extent_len, range, hint)
    }

    // NOTE: the caller must ensure that the read cache has been invalidated or stale reads my occur
    pub(crate) fn get_page_mut<'txn>(&self, page_number: PageNumber) -> Result<PageMut<'txn>> {
        #[cfg(debug_assertions)]
        {
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page_number)
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
        }

        let address_range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();
        let mem = self.storage.write(address_range.start, len, false)?;

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));
        }

        Ok(PageMut {
            mem,
            page_number,
            _lifetime: PhantomData,
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    pub(crate) fn get_version(&self) -> u8 {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().version
        } else {
            state.header.primary_slot().version
        }
    }

    pub(crate) fn get_data_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().user_root
        } else {
            state.header.primary_slot().user_root
        }
    }

    pub(crate) fn get_system_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().system_root
        } else {
            state.header.primary_slot().system_root
        }
    }

    pub(crate) fn get_last_committed_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock()?;
        if self.read_from_secondary.load(Ordering::Acquire) {
            Ok(state.header.secondary_slot().transaction_id)
        } else {
            Ok(state.header.primary_slot().transaction_id)
        }
    }

    pub(crate) fn get_last_durable_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock()?;
        Ok(state.header.primary_slot().transaction_id)
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &mut PageTrackerPolicy) {
        self.allocated_since_commit.lock().unwrap().remove(&page);
        self.free_helper(page, allocated);
    }

    fn free_helper(&self, page: PageNumber, allocated: &mut PageTrackerPolicy) {
        #[cfg(debug_assertions)]
        {
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page)
            );
            if cfg!(test) || !self.uses_paged_allocator() {
                assert!(self.allocated_pages.lock().unwrap().remove(&page));
            }
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page));
        }
        allocated.remove(page);
        let mut state = self.state.lock().unwrap();
        let region_index = page.region;
        // Free in the regional allocator
        state
            .get_region_mut(region_index)
            .free(page.page_index, page.page_order);
        state.allocators.note_freed(page.page_order);
        // Ensure that the region is marked as having free space
        state
            .get_region_tracker_mut()
            .mark_free(page.page_order, region_index);

        let address_range = page.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();
        self.storage.invalidate_cache(address_range.start, len);
        self.storage.cancel_pending_write(address_range.start, len);
    }

    // Frees the page if no durable commit has occurred, since it was allocated. Returns true, if the page was freed
    pub(crate) fn free_if_unpersisted(
        &self,
        page: PageNumber,
        allocated: &mut PageTrackerPolicy,
    ) -> bool {
        if self.unpersisted.lock().unwrap().remove(&page) {
            self.free_helper(page, allocated);
            true
        } else {
            false
        }
    }

    // Frees the page if it was allocated since the last commit. Returns true, if the page was freed
    pub(crate) fn free_if_uncommitted(
        &self,
        page: PageNumber,
        allocated: &mut PageTrackerPolicy,
    ) -> bool {
        if self.allocated_since_commit.lock().unwrap().remove(&page) {
            self.free_helper(page, allocated);
            true
        } else {
            false
        }
    }

    // Page has not been committed
    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.lock().unwrap().contains(&page)
    }

    pub(crate) fn unpersisted(&self, page: PageNumber) -> bool {
        self.unpersisted.lock().unwrap().contains(&page)
    }

    pub(crate) fn allocate_helper<'txn>(
        &self,
        allocation_size: usize,
        lowest: bool,
        transactional: bool,
    ) -> Result<PageMut<'txn>> {
        let required_pages = allocation_size.div_ceil(self.get_page_size());
        let required_order = ceil_log2(required_pages);

        let mut state = self.state.lock().unwrap();

        let page_number = if let Some(page_number) =
            Self::allocate_helper_retry(&mut state, required_order, lowest)?
        {
            page_number
        } else {
            self.grow(&mut state, required_order)?;
            let page = Self::allocate_helper_retry(&mut state, required_order, lowest)?;
            if let Some(error) = state
                .allocators
                .pager()
                .and_then(|pager| pager.storage_failure())
            {
                return Err(error);
            }
            page.ok_or_else(|| {
                StorageError::Corrupted("allocator growth yielded no free page".into())
            })?
        };

        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }

        #[cfg(debug_assertions)]
        {
            if cfg!(test) || state.allocators.pager().is_none() {
                assert!(self.allocated_pages.lock().unwrap().insert(page_number));
            }
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page_number),
                "Allocated a page that is still referenced! {page_number:?}"
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
        }

        if transactional {
            self.allocated_since_commit
                .lock()
                .unwrap()
                .insert(page_number);
        }

        let address_range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();

        #[allow(unused_mut)]
        let mut mem = self.storage.write(address_range.start, len, true)?;
        debug_assert!(mem.mem().len() >= allocation_size);

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));

            // Poison the memory in debug mode to help detect uninitialized reads
            mem.mem_mut().fill(0xFF);
        }

        Ok(PageMut {
            mem,
            page_number,
            _lifetime: PhantomData,
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    fn allocate_helper_retry(
        state: &mut InMemoryState,
        required_order: u8,
        lowest: bool,
    ) -> Result<Option<PageNumber>> {
        loop {
            let Some(candidate_region) = state.get_region_tracker_mut().find_free(required_order)
            else {
                return Ok(None);
            };
            let region = state.get_region_mut(candidate_region);
            let r = if lowest {
                region.alloc_lowest(required_order)
            } else {
                region.alloc(required_order)
            };
            let highest_free = r.map(|_| region.highest_free_order());
            if let Some(error) = state
                .allocators
                .pager()
                .and_then(|pager| pager.storage_failure())
            {
                return Err(error);
            }
            if let Some(page) = r {
                state.allocators.note_allocated(required_order);
                // Keep the tracker exact for the region just used: a region
                // this allocation filled is recorded full now, not left for a
                // later allocation (or the next open, from the saved tracker)
                // to discover by loading and probing it.
                let tracker = state.get_region_tracker_mut();
                match highest_free.flatten().map(|order| order.checked_add(1)) {
                    None => tracker.mark_full(0, candidate_region),
                    Some(Some(above)) if usize::from(above) < tracker.orders() => {
                        tracker.mark_full(above, candidate_region);
                    }
                    Some(_) => {}
                }
                return Ok(Some(PageNumber::new(
                    candidate_region,
                    page,
                    required_order,
                )));
            }
            // Mark the region, if it's full
            state
                .get_region_tracker_mut()
                .mark_full(required_order, candidate_region);
        }
    }

    fn try_shrink(state: &mut InMemoryState, force: bool) -> Result<bool> {
        let layout = state.header.layout();
        let last_region_index = layout.num_regions() - 1;
        let last_allocator = state.get_region(last_region_index);
        let trailing_free = last_allocator.trailing_free_pages();
        let last_allocator_len = last_allocator.len();
        if trailing_free == 0 {
            return Ok(false);
        }
        if trailing_free < last_allocator_len / 2 && !force {
            return Ok(false);
        }
        let reduce_by = if layout.num_regions() > 1 && trailing_free == last_allocator_len {
            trailing_free
        } else if force {
            // Do not shrink the database to zero size
            min(last_allocator_len - 1, trailing_free)
        } else {
            trailing_free / 2
        };

        let mut new_layout = layout;
        new_layout.reduce_last_region(reduce_by);
        state.allocators.resize_to(new_layout)?;
        assert!(new_layout.len() <= layout.len());
        state.header.set_layout(new_layout);

        Ok(true)
    }

    fn grow(&self, state: &mut InMemoryState, required_order_allocation: u8) -> Result<()> {
        let layout = state.header.layout();
        let required_growth =
            2u64.pow(required_order_allocation.into()) * u64::from(state.header.page_size());
        let max_region_size = u64::from(state.header.layout().full_region_layout().num_pages())
            * u64::from(state.header.page_size());
        let next_desired_size = if layout.num_full_regions() > 0 {
            if let Some(trailing) = layout.trailing_region_layout() {
                if 2 * required_growth < max_region_size - trailing.usable_bytes() {
                    // Fill out the trailing region
                    layout.usable_bytes() + (max_region_size - trailing.usable_bytes())
                } else {
                    // Fill out trailing & Grow by 1 region
                    layout.usable_bytes() + 2 * max_region_size - trailing.usable_bytes()
                }
            } else {
                // Grow by 1 region
                layout.usable_bytes() + max_region_size
            }
        } else {
            max(
                layout.usable_bytes() * 2,
                layout.usable_bytes() + required_growth * 2,
            )
        };
        let new_layout = DatabaseLayout::calculate(
            next_desired_size,
            state.header.layout().full_region_layout().num_pages(),
            state
                .header
                .layout()
                .full_region_layout()
                .get_header_pages(),
            self.page_size,
        );
        assert!(new_layout.len() >= layout.len());

        let result = self.storage.resize(new_layout.len());
        if result.is_err() {
            if matches!(
                &result,
                Err(StorageError::Io(error))
                    if error.kind() == std::io::ErrorKind::QuotaExceeded
            ) {
                return result;
            }
            // TODO: it would be nice to have a more cohesive approach to setting this.
            // we do it in commit() & rollback() on failure, but there are probably other places that need it
            self.needs_recovery.store(true, Ordering::Release);
            return result;
        }

        state.allocators.resize_to(new_layout)?;
        state.header.set_layout(new_layout);
        Ok(())
    }

    pub(crate) fn allocate<'txn>(
        &self,
        allocation_size: usize,
        allocated: &mut PageTrackerPolicy,
    ) -> Result<PageMut<'txn>> {
        let lowest = LOWEST_PLACEMENT.with(std::cell::Cell::get);
        let result = self.allocate_helper(allocation_size, lowest, true);
        if let Ok(ref page) = result {
            allocated.insert(page.get_page_number());
        }
        result
    }

    /// Place this thread's ordinary allocations at the lowest free pages
    /// until the returned scope drops. The quick-repair save uses it, so the
    /// saved allocator state never lands in the trailing free run, where it
    /// would stop the same commit's trim and pin the end of the file.
    pub(crate) fn lowest_placement_scope() -> LowestPlacementScope {
        LowestPlacementScope {
            previous: LOWEST_PLACEMENT.with(|lowest| lowest.replace(true)),
        }
    }

    pub(crate) fn allocate_lowest<'txn>(&self, allocation_size: usize) -> Result<PageMut<'txn>> {
        self.allocate_helper(allocation_size, true, true)
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        let mut state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_allocated_pages());
        }

        Ok(count)
    }

    pub(crate) fn count_free_pages(&self) -> Result<u64> {
        let mut state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_free_pages());
        }

        Ok(count)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.page_size.try_into().unwrap()
    }

    /// Make the committed root readable by a read-only opener while this
    /// backend remains alive for existing readers. The caller has already
    /// stopped every writer and finished allocator-state housekeeping.
    pub(crate) fn publish_read_only_handoff(&self) -> Result {
        if self.needs_recovery.load(Ordering::Acquire) || thread::panicking() {
            return Err(StorageError::Corrupted(
                "cannot publish a read-only handoff before recovery completes".to_owned(),
            ));
        }
        let mut state = self.state.lock()?;
        self.storage.flush()?;
        state.header.recovery_required = false;
        self.write_header(&state.header)?;
        self.storage.flush()?;
        Ok(())
    }

    pub(crate) fn read_only_handoff_is_clean(&self) -> bool {
        !self.needs_recovery.load(Ordering::Acquire)
            && self
                .state
                .lock()
                .is_ok_and(|state| !state.header.recovery_required)
    }

    pub(crate) fn close_after_read_only_handoff(&self) -> Result {
        debug_assert!(self.read_only_handoff_is_clean());
        self.storage.close()
    }

    pub(crate) fn close(&self) -> Result {
        if !self.needs_recovery.load(Ordering::Acquire)
            && !thread::panicking()
            && self.check_io_errors().is_ok()
        {
            let mut state = self.state.lock()?;
            if self.storage.flush().is_ok() {
                state.header.recovery_required = false;
                self.write_header(&state.header)?;
                self.storage.flush()?;
            }
        }

        self.storage.close()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::page_manager::INITIAL_REGIONS;
    use crate::{Database, TableDefinition};

    // Test that the region tracker expansion code works, by adding more data than fits into the initial max regions
    #[test]
    fn out_of_regions() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u32, &[u8]> = TableDefinition::new("x");
        let page_size = 1024;
        let big_value = vec![0u8; 5 * page_size];

        let db = Database::builder()
            .set_region_size((8 * page_size).try_into().unwrap())
            .set_page_size(page_size)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..=INITIAL_REGIONS {
                table.insert(&i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        drop(db);

        let mut db = Database::builder()
            .set_region_size((8 * page_size).try_into().unwrap())
            .set_page_size(page_size)
            .open(tmpfile.path())
            .unwrap();
        assert!(db.check_integrity().unwrap());
    }

    // Make sure the database remains consistent after a panic
    #[test]
    #[cfg(panic = "unwind")]
    fn panic() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u32, &[u8]> = TableDefinition::new("x");

        let _ = std::panic::catch_unwind(|| {
            let db = Database::create(&tmpfile).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table_definition).unwrap();
            panic!();
        });

        let mut db = Database::open(tmpfile).unwrap();
        assert!(db.check_integrity().unwrap());
    }

    #[test]
    #[cfg(debug_assertions)]
    fn read_reference_tracker_releases_all_history_after_the_last_reference() {
        use crate::tree_store::Page;
        use crate::tree_store::page_store::base::PageHint;

        let tmpfile = crate::create_tempfile();
        let db = Database::create(&tmpfile).unwrap();
        let mem = db.get_memory();
        assert!(mem.read_page_ref_counts.lock().unwrap().is_empty());
        for _ in 0..32 {
            let first = mem.allocate_lowest(mem.get_page_size()).unwrap();
            let first_number = first.get_page_number();
            drop(first);
            let second = mem.allocate_lowest(mem.get_page_size()).unwrap();
            let second_number = second.get_page_number();
            drop(second);
            assert_ne!(first_number, second_number);

            let first = mem.get_page(first_number, PageHint::Clean).unwrap();
            let first_clone = first.clone();
            let second = mem.get_page(second_number, PageHint::Clean).unwrap();
            {
                let refs = mem.read_page_ref_counts.lock().unwrap();
                assert_eq!(refs.len(), 2);
                assert_eq!(refs.get(&first_number), Some(&2));
                assert_eq!(refs.get(&second_number), Some(&1));
            }
            drop(first);
            assert_eq!(
                mem.read_page_ref_counts.lock().unwrap().get(&first_number),
                Some(&1)
            );
            drop(first_clone);
            {
                let refs = mem.read_page_ref_counts.lock().unwrap();
                assert_eq!(refs.len(), 1);
                assert!(!refs.contains_key(&first_number));
                assert_eq!(refs.get(&second_number), Some(&1));
            }
            drop(second);
            let refs = mem.read_page_ref_counts.lock().unwrap();
            assert!(
                refs.is_empty(),
                "no historical page number survives its last reference"
            );
            assert_eq!(
                refs.capacity(),
                0,
                "no historical bucket allocation survives an empty tracker"
            );
        }
    }
    #[test]
    fn committed_write_buffer_releases_all_map_capacity_even_at_zero_cache_cap() {
        use crate::ReadableDatabase;
        for cap in [0, 4096, 4 * 1024 * 1024] {
            let tmpfile = crate::create_tempfile();
            let db = Database::builder()
                .set_cache_size(cap)
                .create(tmpfile.path())
                .unwrap();
            let mem = db.get_memory();
            let index_bytes = mem.storage.index_bytes();
            let definition: TableDefinition<u64, &[u8]> =
                TableDefinition::new("commit_index_probe");
            for round in 0..2_u64 {
                let transaction = db.begin_write().unwrap();
                {
                    let mut table = transaction.open_table(definition).unwrap();
                    for key in 0..16_u64 {
                        let bytes = [u8::try_from(round).unwrap(); 512];
                        table.insert(&key, bytes.as_slice()).unwrap();
                    }
                }
                transaction.commit().unwrap();
                assert_eq!(mem.storage.write_map_for_test(), (0, 0));
                assert_eq!(mem.storage.index_bytes(), index_bytes);
                {
                    let read = db.begin_read().unwrap();
                    let table = read.open_table(definition).unwrap();
                    for key in 0..16_u64 {
                        assert_eq!(
                            table.get(&key).unwrap().unwrap().value(),
                            &[u8::try_from(round).unwrap(); 512]
                        );
                    }
                }
                assert_eq!(mem.storage.write_map_for_test(), (0, 0));
                #[cfg(debug_assertions)]
                {
                    let refs = mem.read_page_ref_counts.lock().unwrap();
                    assert!(refs.is_empty());
                    assert_eq!(refs.capacity(), 0);
                }
            }
        }
    }
}

#[cfg(feature = "contextdb-internal")]
impl TransactionalMemory {
    pub(crate) fn get_native_page_memory(
        &self,
        page: PageNumber,
    ) -> std::result::Result<super::cached_file::PageMemory, crate::NativeColdReadError> {
        use crate::{NativeColdFault, NativeColdReadError};
        let corrupt = || {
            let mut fault = NativeColdFault::new("native page address exceeds allocation layout");
            fault.page = page.to_le_bytes();
            NativeColdReadError::Corrupt(fault)
        };
        if page.page_order > MAX_MAX_PAGE_ORDER {
            return Err(corrupt());
        }
        let pages = 1u64 << page.page_order;
        let end_page = u64::from(page.page_index)
            .checked_add(1)
            .and_then(|n| n.checked_mul(pages))
            .ok_or_else(corrupt)?;
        {
            let state = self.state.lock().unwrap();
            let layout = state.header.layout();
            if page.region >= layout.num_regions()
                || end_page > u64::from(layout.region_layout(page.region).num_pages())
            {
                return Err(corrupt());
            }
        }
        #[cfg(debug_assertions)]
        if self.open_dirty_pages.lock().unwrap().contains(&page) {
            return Err(corrupt());
        }
        let len = pages
            .checked_mul(u64::from(self.page_size))
            .ok_or_else(corrupt)?;
        let offset = u64::from(page.page_index)
            .checked_mul(len)
            .and_then(|n| n.checked_add(self.region_header_with_padding_size))
            .and_then(|n| {
                u64::from(page.region)
                    .checked_mul(self.region_size)
                    .and_then(|r| r.checked_add(n))
            })
            .and_then(|n| n.checked_add(u64::from(self.page_size)))
            .ok_or_else(corrupt)?;
        offset.checked_add(len).ok_or_else(corrupt)?;
        let len = usize::try_from(len).map_err(|_| corrupt())?;
        // Like existing get_page_window, this lends immutable PageMemory.
        // No PageImpl/debug HashMap node or generic get_page is constructed.
        self.storage.read_native_clean_miss(offset, len)
    }
}

/// Allocation work against stores of different sizes. The fixtures build real
/// paged allocator state over 4 KiB-page geometry (no data pages are written):
/// every region is fully allocated, then order-0 holes are punched at a fixed
/// stride so each allocation fills a hole in region 0 while every higher order
/// stays full. Counts are pager work counters, never elapsed time.
#[cfg(test)]
mod allocator_growth_tests {
    use super::{InMemoryState, TransactionalMemory};
    use crate::transaction_tracker::TransactionId;
    use crate::tree_store::page_store::base::MAX_PAGE_INDEX;
    use crate::tree_store::page_store::header::DatabaseHeader;
    use crate::tree_store::page_store::layout::{DatabaseLayout, RegionLayout};
    use crate::tree_store::page_store::region::REGION_VIEWS_BUILT;
    use std::sync::Arc;

    thread_local! {
        /// `try_save_allocator_state` calls on this thread.
        pub(super) static SAVE_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    /// The whole-allocator serialization runs only on quick-repair commits,
    /// which the database issues on close; ordinary commits never call it.
    #[test]
    fn allocator_state_is_serialized_on_close_not_per_commit() {
        use crate::{Database, TableDefinition};
        const TABLE: TableDefinition<u64, u64> = TableDefinition::new("save_calls");
        let file = crate::create_tempfile();
        let db = Database::create(file.path()).unwrap();
        let before = SAVE_CALLS.with(std::cell::Cell::get);
        for key in 0..16 {
            let write = db.begin_write().unwrap();
            write.open_table(TABLE).unwrap().insert(key, key).unwrap();
            write.commit().unwrap();
        }
        let after_commits = SAVE_CALLS.with(std::cell::Cell::get);
        assert_eq!(
            after_commits - before,
            0,
            "ordinary commits serialized allocator state"
        );
        drop(db);
        let after_close = SAVE_CALLS.with(std::cell::Cell::get);
        assert!(
            after_close > after_commits,
            "close must persist allocator state through a quick-repair commit"
        );
    }

    const PAGE_SIZE: u32 = 4096;
    const HOLE_STRIDE: usize = 1024;
    const WARM_ALLOCATIONS: usize = 16;
    const MEASURED_ALLOCATIONS: u64 = 64;
    /// 1 GiB of pages: one partial region.
    const SMALL_STORE_PAGES: u32 = 1 << 18;
    /// 8 GiB of pages: two full 4 GiB regions.
    const LARGE_STORE_FULL_REGIONS: u32 = 2;

    fn layout(full_regions: u32, trailing_pages: Option<u32>) -> DatabaseLayout {
        DatabaseLayout::new(
            full_regions,
            RegionLayout::new(MAX_PAGE_INDEX + 1, 1, PAGE_SIZE),
            trailing_pages.map(|pages| RegionLayout::new(pages, 1, PAGE_SIZE)),
        )
    }

    fn paged_state(layout: DatabaseLayout) -> InMemoryState {
        let scratch = crate::scratch_backend_in_directory(&std::env::temp_dir()).unwrap();
        InMemoryState::new(
            DatabaseHeader::new(layout, TransactionId::new(0)),
            Some(scratch),
            0,
        )
        .unwrap()
    }

    fn fragmented_state(layout: DatabaseLayout) -> InMemoryState {
        let mut state = paged_state(layout);
        for region in 0..layout.num_regions() {
            let allocator = state.get_region_mut(region);
            for order in (0..=allocator.get_max_order()).rev() {
                while allocator.alloc(order).is_some() {}
            }
            for page in (0..allocator.len()).step_by(HOLE_STRIDE) {
                allocator.free(page, 0);
            }
            state.get_region_tracker_mut().mark_free(0, region);
        }
        state
    }

    fn counters(state: &InMemoryState) -> (u64, u64, u64) {
        state.allocators.pager().unwrap().counters_for_test()
    }

    /// (word reads, window loads) for `MEASURED_ALLOCATIONS` order-0
    /// allocations after a fixed warm-up.
    fn allocation_work(state: &mut InMemoryState) -> (u64, u64) {
        let allocate = |state: &mut InMemoryState| {
            let page = TransactionalMemory::allocate_helper_retry(state, 0, false)
                .unwrap()
                .expect("a hole is free");
            assert_eq!(page.region, 0, "every measured allocation fills region 0");
            assert_eq!(page.page_index as usize % HOLE_STRIDE, 0);
        };
        for _ in 0..WARM_ALLOCATIONS {
            allocate(state);
        }
        let (words, loads, _) = counters(state);
        for _ in 0..MEASURED_ALLOCATIONS {
            allocate(state);
        }
        let (words_after, loads_after, _) = counters(state);
        assert!(!state.allocators.pager().unwrap().is_poisoned());
        (words_after - words, loads_after - loads)
    }

    /// Window loads per allocation do not grow with the store. Across a
    /// 1 GiB store (one partial region), an 8 GiB store (two full regions) and
    /// a 16 GiB store (four), the only difference is region geometry: a
    /// partial region of 2^18 pages has empty buddy orders 19 and 20, whose
    /// summary roots an allocation never reads (`first_unset` returns before
    /// any word of an empty level), while a full 2^20-page region reads all 21
    /// roots. At the 32-window floor each of those reads is a load, so the
    /// full-region stores may pay exactly log2(full / partial pages) more
    /// loads per allocation; doubling the full-region store adds nothing
    /// beyond one load per 8 allocations.
    #[test]
    fn allocation_window_loads_do_not_grow_with_store_size() {
        let full_region_geometry_loads =
            u64::from((MAX_PAGE_INDEX + 1).ilog2() - SMALL_STORE_PAGES.ilog2());
        let mut loads = Vec::new();
        for (gib, layout) in [
            (1, layout(0, Some(SMALL_STORE_PAGES))),
            (8, layout(LARGE_STORE_FULL_REGIONS, None)),
            (16, layout(2 * LARGE_STORE_FULL_REGIONS, None)),
        ] {
            let mut state = fragmented_state(layout);
            let (_, window_loads) = allocation_work(&mut state);
            loads.push((gib, window_loads));
        }
        let [(_, one), (_, eight), (_, sixteen)] = loads[..] else {
            unreachable!()
        };
        let report =
            format!("window loads for {MEASURED_ALLOCATIONS} allocations by store GiB: {loads:?}");
        assert!(
            eight <= one + full_region_geometry_loads * MEASURED_ALLOCATIONS,
            "{report}"
        );
        assert!(sixteen <= eight + MEASURED_ALLOCATIONS / 8, "{report}");
    }

    /// At the 32-window floor an allocation's working set stays resident:
    /// every order's one-word summary root is answered without a window, so
    /// what an allocation touches (the order-0 descent and the tracker words
    /// it changes) fits the floor, and steady-state allocations reload almost
    /// nothing at any store size: at most one window per 4 allocations, which
    /// covers order-0 leaf windows the allocations walk into.
    #[test]
    fn allocation_window_loads_at_the_floor_stay_resident() {
        let mut work = Vec::new();
        for (gib, layout) in [
            (1, layout(0, Some(SMALL_STORE_PAGES))),
            (8, layout(LARGE_STORE_FULL_REGIONS, None)),
            (16, layout(2 * LARGE_STORE_FULL_REGIONS, None)),
        ] {
            let mut state = fragmented_state(layout);
            let (words, loads) = allocation_work(&mut state);
            work.push((gib, words, loads));
        }
        println!(
            "(GiB, words, window loads) for {MEASURED_ALLOCATIONS} allocations at the floor: {work:?}"
        );
        for (gib, _, loads) in &work {
            assert!(
                *loads <= MEASURED_ALLOCATIONS / 4,
                "{gib} GiB store reloaded {loads} windows: {work:?}"
            );
        }
    }

    /// Finding the highest free order after an allocation reads a bounded
    /// number of bitmap words: the summary levels answer it, not a leaf sweep
    /// whose length is the region's page count.
    #[test]
    fn allocation_word_reads_do_not_grow_with_store_size() {
        let mut small = fragmented_state(layout(0, Some(SMALL_STORE_PAGES)));
        let mut large = fragmented_state(layout(LARGE_STORE_FULL_REGIONS, None));
        let (small_words, _) = allocation_work(&mut small);
        let (large_words, _) = allocation_work(&mut large);
        assert!(
            large_words <= small_words + 8 * MEASURED_ALLOCATIONS,
            "bitmap words read for {MEASURED_ALLOCATIONS} allocations: 1 GiB store \
             {small_words}, 8 GiB store {large_words}"
        );
        assert!(
            large_words <= 128 * MEASURED_ALLOCATIONS,
            "an allocation read {} words on average",
            large_words / MEASURED_ALLOCATIONS
        );
    }

    /// A declared writer cache share sizes the window set: under the share a
    /// 1.5 GiB writer budget gives (64 MiB) the 8 GiB store's allocation
    /// working set stays resident, so steady-state allocations load nothing,
    /// and the pager's residency stays inside its carve of that share. The
    /// smallest shares keep exactly the former 32-window set.
    #[test]
    fn a_declared_cache_share_sizes_the_window_set() {
        use crate::tree_store::page_store::allocator_pager::{
            AllocatorPager, CACHE_SHARE_DIVISOR, WINDOWS,
        };
        const WRITER_CACHE: usize = 64 << 20;
        assert_eq!(AllocatorPager::capacity_for_cache(0), WINDOWS);
        assert_eq!(AllocatorPager::capacity_for_cache(4 << 20), WINDOWS);
        assert_eq!(AllocatorPager::cache_bytes_taken(4 << 20), 0);
        assert!(AllocatorPager::capacity_for_cache(WRITER_CACHE) > 8 * WINDOWS);

        let mut large = fragmented_state(layout(LARGE_STORE_FULL_REGIONS, None));
        let taken = large.allocators.set_window_budget(WRITER_CACHE).unwrap();
        assert!(taken <= WRITER_CACHE / CACHE_SHARE_DIVISOR);
        let base = paged_state(layout(LARGE_STORE_FULL_REGIONS, None))
            .allocators
            .resident_bytes();
        assert!(
            large.allocators.resident_bytes() <= base + taken,
            "the window set outgrew its carve of the cache share"
        );
        // Measured allocations walk on through at most three order-0 leaf
        // windows they had not reached; nothing already loaded is reloaded.
        let (_, loads) = allocation_work(&mut large);
        assert!(loads <= 3, "a resident working set loaded {loads} windows");

        // Lowering the share spills the surplus and keeps every word.
        assert_eq!(large.allocators.set_window_budget(0).unwrap(), 0);
        assert_eq!(large.allocators.resident_bytes(), base);
        allocation_work(&mut large);
    }

    /// A commit that cannot retire a large saved allocator table must not
    /// rebuild a regional view to rediscover that on every commit.
    #[test]
    fn repeated_small_source_checks_rebuild_no_regional_view() {
        let mut state = paged_state(layout(LARGE_STORE_FULL_REGIONS, None));
        state
            .allocators
            .set_saved_source(Arc::new(|_, _, _, _, _| Ok(false)))
            .unwrap();
        assert!(
            !state
                .allocators
                .materialize_saved_source_if_small()
                .unwrap()
        );
        // Between commits the writer allocates in a later region, so region
        // 0's view is no longer the active one when the next commit checks.
        let mut rebuilt = 0;
        for _ in 0..4 {
            state.allocators.get_region(1);
            let before = REGION_VIEWS_BUILT.with(std::cell::Cell::get);
            assert!(
                !state
                    .allocators
                    .materialize_saved_source_if_small()
                    .unwrap()
            );
            rebuilt += REGION_VIEWS_BUILT.with(std::cell::Cell::get) - before;
        }
        assert_eq!(
            rebuilt, 0,
            "four commits rebuilt {rebuilt} regional views to re-measure the saved table"
        );
        assert!(state.allocators.has_saved_source());
    }
}
