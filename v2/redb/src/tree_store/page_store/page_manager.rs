use crate::transaction_tracker::TransactionId;
use crate::transactions::{AllocatorStateKey, AllocatorStateTree, AllocatorStateTreeMut};
use crate::tree_store::btree_base::{BtreeHeader, Checksum, verify_page_checksum};
use crate::tree_store::page_store::base::{MAX_PAGE_INDEX, PageHint};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::PagedCachedFile;
use crate::tree_store::page_store::fast_hash::PageNumberHashSet;
use crate::tree_store::page_store::header::{DB_HEADER_SIZE, DatabaseHeader, MAGICNUMBER};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{Allocators, RegionTracker};
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
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

pub(super) struct InMemoryState {
    pub(super) header: DatabaseHeader,
    // TODO: we should make this an Option because it is only valid after the Database initializes it
    pub(super) allocators: Allocators,
}

impl InMemoryState {
    fn with_taken(header: DatabaseHeader, taken: bool) -> Self {
        let allocators = Allocators::with_taken(header.layout(), taken);
        Self { header, allocators }
    }

    fn get_region(&self, region: u32) -> &BuddyAllocator {
        &self.allocators.region_allocators[region as usize]
    }

    fn get_region_mut(&mut self, region: u32) -> &mut BuddyAllocator {
        &mut self.allocators.region_allocators[region as usize]
    }

    fn get_region_tracker_mut(&mut self) -> &mut RegionTracker {
        &mut self.allocators.region_tracker
    }
}

pub(crate) struct TransactionalMemory {
    pub(crate) last_freed_pages: AtomicU64,
    // The largest block compaction found with no place below it (order, address), for its next
    // walk
    pub(crate) stuck: Mutex<Option<(u8, u64)>>,
    // Pages allocated since the last commit
    // TODO: maybe this should be moved to WriteTransaction?
    pub(super) allocated_since_commit: Mutex<PageTrackerPolicy>,
    unpersisted: Mutex<PageNumberHashSet>,
    // True if the allocator state was corrupted when the file was opened
    // TODO: maybe we can remove this flag now that CheckedBackend exists?
    pub(super) needs_recovery: AtomicBool,
    pub(super) recovery_end: AtomicU64,
    pub(super) sweep: Mutex<Option<super::sweep::Sweep>>,
    pub(super) incremental_recovery: bool,
    secondary_valid: AtomicBool,
    pub(super) commit_pages: Mutex<Option<PageNumber>>,
    pub(crate) file_size_limit: AtomicU64,
    pub(crate) compaction_position: Mutex<(bool, Option<Vec<u8>>, bool)>,
    // The most pages the open write transaction has taken and not committed, and the last
    // commit's low mark
    pub(crate) taken: Mutex<(u64, Option<u64>)>,
    allocator_state_lowest: AtomicBool,
    storage: PagedCachedFile,
    pub(super) state: Mutex<InMemoryState>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Arc<Mutex<HashSet<PageNumber>>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Arc<Mutex<HashMap<PageNumber, u64>>>,
    // Set of all allocated pages for debugging assertions
    #[cfg(debug_assertions)]
    pub(super) allocated_pages: Arc<Mutex<PageNumberHashSet>>,
    // Indicates that a non-durable commit has been made, so reads should be served from the secondary meta page
    read_from_secondary: AtomicBool,
    pub(super) page_size: u32,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    pub(super) region_size: u64,
    pub(super) region_header_with_padding_size: u64,
}

impl TransactionalMemory {
    pub(crate) fn new(
        file: Box<dyn StorageBackend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        incremental_recovery: bool,
    ) -> Result<Self, DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);

        let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
        let region_size = min(
            region_size,
            (u64::from(MAX_PAGE_INDEX) + 1) * page_size as u64,
        );
        assert!(region_size.is_power_of_two());

        let storage = PagedCachedFile::new(file, page_size as u64, cache_size)?;

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
                    storage.resize(layout.len(), false)?;
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

        assert_eq!(header.page_size() as usize, page_size);
        assert!(storage.raw_file_len()? >= header.layout().len());
        let needs_recovery =
            header.recovery_required || header.layout().len() != storage.raw_file_len()?;
        let mut secondary_valid = !repair_info.secondary_corrupted;
        if needs_recovery {
            if read_only {
                return Err(DatabaseError::RepairAborted);
            }
            header.reconcile_layout(storage.raw_file_len()?);
            if !header.pick_primary_for_repair(repair_info)? {
                secondary_valid = !repair_info.primary_corrupted;
            }
            assert!(!repair_info.invalid_magic_number);
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }

        let layout = header.layout();
        assert_eq!(layout.len(), storage.raw_file_len()?);
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;
        let state = InMemoryState::with_taken(header, needs_recovery && incremental_recovery);

        assert!(page_size >= DB_HEADER_SIZE);

        Ok(Self {
            last_freed_pages: AtomicU64::new(0),
            stuck: Mutex::new(None),
            allocated_since_commit: Mutex::new(PageTrackerPolicy::new_tracking()),
            unpersisted: Mutex::new(Default::default()),
            needs_recovery: AtomicBool::new(needs_recovery),
            recovery_end: AtomicU64::new(0),
            sweep: Mutex::new(None),
            incremental_recovery,
            secondary_valid: AtomicBool::new(secondary_valid),
            commit_pages: Mutex::new(None),
            file_size_limit: AtomicU64::new(u64::MAX),
            compaction_position: Mutex::new((false, None, false)),
            taken: Mutex::new((0, None)),
            allocator_state_lowest: AtomicBool::new(false),
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

    // Opens a database file for reading while another handle may have it open for writing.
    // Nothing is written or repaired, and no allocator state is loaded.
    pub(crate) fn new_beside_writer(
        file: Box<dyn StorageBackend>,
        page_size: usize,
        cache_size: usize,
        incremental_recovery: bool,
    ) -> Result<Self, DatabaseError> {
        let storage = PagedCachedFile::new(file, page_size as u64, cache_size)?;
        if storage.raw_file_len()? < DB_HEADER_SIZE as u64 {
            return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
        }
        let (header, secondary_valid) = Self::read_newest_header(&storage)?;
        assert_eq!(header.page_size() as usize, page_size);
        let layout = header.layout();

        Ok(Self {
            last_freed_pages: AtomicU64::new(0),
            stuck: Mutex::new(None),
            allocated_since_commit: Mutex::new(PageTrackerPolicy::new_tracking()),
            unpersisted: Mutex::new(Default::default()),
            needs_recovery: AtomicBool::new(false),
            recovery_end: AtomicU64::new(0),
            sweep: Mutex::new(None),
            incremental_recovery,
            secondary_valid: AtomicBool::new(secondary_valid),
            commit_pages: Mutex::new(None),
            file_size_limit: AtomicU64::new(u64::MAX),
            compaction_position: Mutex::new((false, None, false)),
            taken: Mutex::new((0, None)),
            allocator_state_lowest: AtomicBool::new(false),
            storage,
            // A reader never allocates, so it builds no allocators
            state: Mutex::new(InMemoryState {
                header,
                allocators: Allocators {
                    region_tracker: RegionTracker::new(0, 0),
                    region_allocators: Vec::new(),
                },
            }),
            #[cfg(debug_assertions)]
            open_dirty_pages: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(debug_assertions)]
            read_page_ref_counts: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(debug_assertions)]
            allocated_pages: Arc::new(Mutex::new(Default::default())),
            read_from_secondary: AtomicBool::new(false),
            page_size: page_size.try_into().unwrap(),
            region_size: layout.full_region_layout().len(),
            region_header_with_padding_size: layout.full_region_layout().data_section().start,
        })
    }

    // Reads the header of a file that its writer may be committing to, and makes the newest
    // whole commit its primary. The read may catch the writer's rewrite of the header half done:
    // the byte that names the primary slot already written and that slot not yet, or only in
    // part. So the slot is chosen as repair chooses it, by the checksums and transaction ids of
    // the two slots. Every page of a commit is in the file before its slot is written.
    fn read_newest_header(
        storage: &PagedCachedFile,
    ) -> Result<(DatabaseHeader, bool), DatabaseError> {
        let header_bytes = storage.read_direct(0, DB_HEADER_SIZE)?;
        if header_bytes[..MAGICNUMBER.len()] != MAGICNUMBER {
            return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
        }
        let (mut header, repair_info) = DatabaseHeader::from_bytes(&header_bytes)?;
        let secondary_valid = if header.pick_primary_for_repair(repair_info)? {
            !repair_info.secondary_corrupted
        } else {
            !repair_info.primary_corrupted
        };

        Ok((header, secondary_valid))
    }

    // Moves a database opened with new_beside_writer() to the newest commit in the file
    pub(crate) fn load_newest_commit(&self) -> Result<bool, DatabaseError> {
        let (header, secondary_valid) = Self::read_newest_header(&self.storage)?;
        let mut state = self.state.lock().unwrap();
        if header.primary_slot().transaction_id != state.header.primary_slot().transaction_id {
            // The writer may have written again to any page that none of the commits read so far
            // can reach, so nothing cached before this commit is served to a read of it
            self.storage.invalidate_cache_all();
            state.header = header;
            self.secondary_valid
                .store(secondary_valid, Ordering::Release);
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) fn cache_stats(&self) -> CacheStats {
        self.storage.cache_stats()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.storage.check_io_errors()
    }

    pub(crate) fn first_io_error(&self) -> Option<std::io::Error> {
        self.storage.first_io_error()
    }

    #[cfg(debug_assertions)]
    pub(crate) fn mark_debug_allocated_page(&self, page: PageNumber) {
        assert!(self.allocated_pages.lock().unwrap().insert(page));
    }

    #[cfg(debug_assertions)]
    pub(crate) fn all_allocated_pages(&self) -> Vec<PageNumber> {
        self.allocated_pages
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    #[cfg(debug_assertions)]
    pub(crate) fn debug_check_allocator_consistency(&self) {
        let state = self.state.lock().unwrap();
        let mut region_pages = vec![vec![]; state.allocators.region_allocators.len()];
        for p in self.allocated_pages.lock().unwrap().iter() {
            region_pages[p.region as usize].push(*p);
        }
        for (i, allocator) in state.allocators.region_allocators.iter().enumerate() {
            allocator.check_allocated_pages(i.try_into().unwrap(), &region_pages[i]);
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
        if header.layout().len() != self.storage.raw_file_len()? {
            header.reconcile_layout(self.storage.raw_file_len()?);
            was_clean = false;
        }
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
        self.state.lock().unwrap().header = header;

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

    // Closes the handle until reopened: a commit or rollback failed part way
    pub(crate) fn fail(&self) {
        self.storage.file.io_failed.store(true, Ordering::Release);
    }

    // TODO: need a clearer distinction between this and needs_repair()
    pub(crate) fn storage_failure(&self) -> bool {
        self.needs_recovery.load(Ordering::Acquire)
    }

    pub(crate) fn repair_primary_corrupted(&self) -> Result {
        if !self.secondary_valid.load(Ordering::Acquire) {
            return Err(StorageError::Corrupted("All roots are corrupted".into()));
        }
        let mut state = self.state.lock().unwrap();
        state.header.swap_primary_slot();
        Ok(())
    }

    pub(crate) fn begin_repair(&self) -> Result<()> {
        self.sweep.lock().unwrap().take();
        self.recovery_end.store(0, Ordering::Release);
        let mut state = self.state.lock().unwrap();
        state.allocators = Allocators::new(state.header.layout());
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
        assert!(self.allocated_pages.lock().unwrap().insert(page_number));
    }

    fn write_header(&self, header: &DatabaseHeader) -> Result {
        self.storage
            .write(0, DB_HEADER_SIZE, true)?
            .mem_mut()
            .copy_from_slice(&header.to_bytes(true));

        Ok(())
    }

    pub(crate) fn incremental_recovery(&self) -> bool {
        self.incremental_recovery
    }

    pub(crate) fn next_commit_id(&self) -> TransactionId {
        let state = self.state.lock().unwrap();
        let primary = state.header.primary_slot().transaction_id;
        if self.secondary_valid.load(Ordering::Acquire) {
            primary
                .max(state.header.secondary_slot().transaction_id)
                .next()
        } else {
            primary.next()
        }
    }

    pub(crate) fn commit_pages_to_free(&self) -> Option<PageNumber> {
        self.state
            .lock()
            .unwrap()
            .header
            .primary_slot()
            .commit_pages
    }

    pub(super) fn validate_page(&self, page: PageNumber) -> Result {
        let layout = self.state.lock().unwrap().header.layout();
        if page.page_order > MAX_MAX_PAGE_ORDER
            || page.region >= layout.num_regions()
            || ((u64::from(page.page_index) + 1) << page.page_order)
                > u64::from(layout.region_layout(page.region).num_pages())
        {
            return Err(StorageError::Corrupted(
                "Invalid commit page address".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn end_repair(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
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
        lowest: bool,
    ) -> Result<u32> {
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let num_regions = layout.num_regions();
        let region_tracker_len = state.allocators.region_tracker.to_vec().len();
        let region_lens: Vec<usize> = state
            .allocators
            .region_allocators
            .iter()
            .map(|x| x.to_vec().len())
            .collect();
        drop(state);

        let _reset = self.lowest_scope(lowest);

        for i in 0..num_regions {
            let region_bytes_len = region_lens[i as usize];
            tree.insert(
                &AllocatorStateKey::Region(i),
                &vec![0; region_bytes_len].as_ref(),
            )?;
        }

        tree.insert(
            &AllocatorStateKey::RegionTracker,
            &vec![0; region_tracker_len].as_ref(),
        )?;

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
        // Has the number of regions changed since reserve_allocator_state() was called?
        let state = self.state.lock().unwrap();
        if num_regions != state.header.layout().num_regions() {
            return Ok(false);
        }

        for i in 0..num_regions {
            let region_bytes = &state.allocators.region_allocators[i as usize].to_vec();
            if tree
                .get(&AllocatorStateKey::Region(i))?
                .unwrap()
                .value()
                .len()
                < region_bytes.len()
            {
                // The allocator state grew too much since we reserved space
                return Ok(false);
            }
            tree.insert_inplace(&AllocatorStateKey::Region(i), &region_bytes.as_ref())?;
        }

        let region_tracker_bytes = state.allocators.region_tracker.to_vec();
        if tree
            .get(&AllocatorStateKey::RegionTracker)?
            .unwrap()
            .value()
            .len()
            < region_tracker_bytes.len()
        {
            // The allocator state grew too much since we reserved space
            return Ok(false);
        }
        tree.insert_inplace(
            &AllocatorStateKey::RegionTracker,
            &region_tracker_bytes.as_ref(),
        )?;

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

    pub(crate) fn load_allocator_state(&self, tree: &AllocatorStateTree) -> Result {
        assert!(self.is_valid_allocator_state(tree)?);

        // Load the allocator state
        let mut region_allocators = vec![];
        for region in
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))?
        {
            region_allocators.push(BuddyAllocator::from_bytes(region?.value()));
        }

        let region_tracker = RegionTracker::from_bytes(
            tree.get(&AllocatorStateKey::RegionTracker)?
                .unwrap()
                .value(),
        );

        let mut state = self.state.lock().unwrap();
        state.allocators = Allocators {
            region_tracker,
            region_allocators,
        };

        // Resize the allocators to match the current file size
        let layout = state.header.layout();
        state.allocators.resize_to(layout);
        drop(state);

        self.state.lock().unwrap().header.recovery_required = false;
        self.needs_recovery.store(false, Ordering::Release);

        Ok(())
    }

    #[cfg_attr(not(debug_assertions), allow(unused_variables, clippy::unused_self))]
    pub(crate) fn is_allocated(&self, page: PageNumber) -> bool {
        #[cfg(debug_assertions)]
        {
            let allocated = self.allocated_pages.lock().unwrap();
            allocated.contains(&page) || self.recovery_old_page(page)
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

        let proof = *self.commit_pages.lock().unwrap();
        if proof.is_some() {
            super::commit_pages::write(self, proof)?;
        }
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

        // Hand every page of this commit to the file before the header that names it, so that
        // whoever reads the file while it is being committed to never finds a header ahead of
        // its pages. A commit with no list that took pages syncs them first: a check of it
        // reads only its roots
        if self.incremental_recovery
            && proof.is_none()
            && !self.allocated_since_commit.lock().unwrap().is_empty()
        {
            self.storage.flush()?;
        } else {
            self.storage.write_barrier()?;
        }

        let old_transaction_id = header.secondary_slot().transaction_id;
        let secondary = header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;
        secondary.commit_pages = proof;

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
            let result = self.storage.resize(header.layout().len(), false);
            if result.is_err() {
                // TODO: it would be nice to have a more cohesive approach to setting this.
                // we do it in commit() & rollback() on failure, but there are probably other places that need it
                self.needs_recovery.store(true, Ordering::Release);
                return result;
            }
        }
        let mut allocated_since_commit = self.allocated_since_commit.lock().unwrap();
        allocated_since_commit.reset();
        let mut unpersisted = self.unpersisted.lock().unwrap();
        unpersisted.clear();
        unpersisted.shrink_to_fit();

        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.header.secondary_slot().transaction_id,
            old_transaction_id
        );
        state.header = header;
        *self.commit_pages.lock().unwrap() = None;
        self.secondary_valid.store(true, Ordering::Release);
        self.read_from_secondary.store(false, Ordering::Release);
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

        let mut unpersisted = self.unpersisted.lock().unwrap();
        let mut allocated_since_commit = self.allocated_since_commit.lock().unwrap();
        unpersisted.extend(allocated_since_commit.reset().iter());
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
            self.fail();
        }
        result
    }

    fn rollback_uncommitted_writes_inner(&self) -> Result {
        *self.commit_pages.lock().unwrap() = None;
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
            let order = state
                .get_region_mut(region_index)
                .free(page_number.page_index, page_number.page_order);
            state
                .get_region_tracker_mut()
                .mark_free(order, region_index);
            #[cfg(debug_assertions)]
            assert!(self.allocated_pages.lock().unwrap().remove(&page_number));

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
        guard.reset();

        Ok(())
    }

    // Returns the page without checking it. Unless it is already cached, it is read from the
    // file and is not cached.
    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        self.read_page(page_number, |offset, len| {
            self.storage.read(offset, len, hint)
        })
    }

    // Returns the page. `expected` is the checksum that its parent page, or the root of its
    // tree, holds for it. When the page is read from the file, rather than found in the cache,
    // it must match that checksum; a page that does not is reported as corrupted.
    pub(crate) fn get_checked_page(
        &self,
        page_number: PageNumber,
        expected: Checksum,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        hint: PageHint,
    ) -> Result<PageImpl> {
        let check = |memory: &[u8]| {
            // A page of the transaction in progress gets its checksum when that commits
            if self.uncommitted(page_number) {
                return Ok(());
            }
            verify_page_checksum(
                memory,
                page_number,
                expected,
                fixed_key_size,
                fixed_value_size,
            )
        };
        self.read_page(page_number, |offset, len| {
            self.storage.read_checked(offset, len, hint, Some(check))
        })
    }

    fn read_page(
        &self,
        page_number: PageNumber,
        read: impl FnOnce(u64, usize) -> Result<Arc<[u8]>>,
    ) -> Result<PageImpl> {
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (range.end - range.start).try_into().unwrap();
        let mem = read(range.start, len)?;

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

    // The data root of the newest commit, and that commit's id
    pub(crate) fn get_data_root_and_transaction_id(&self) -> (Option<BtreeHeader>, TransactionId) {
        let state = self.state.lock().unwrap();
        let slot = if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot()
        } else {
            state.header.primary_slot()
        };
        (slot.user_root, slot.transaction_id)
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
        self.allocated_since_commit.lock().unwrap().remove(page);
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
            assert!(
                self.allocated_pages.lock().unwrap().remove(&page) || self.recovery_old_page(page)
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page));
        }
        assert!(allocated.remove(page));
        let mut state = self.state.lock().unwrap();
        let region_index = page.region;
        // Free in the regional allocator
        let order = state
            .get_region_mut(region_index)
            .free(page.page_index, page.page_order);
        // Mark the region as having a free block of the order the page merged into
        state
            .get_region_tracker_mut()
            .mark_free(order, region_index);

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
        if self.allocated_since_commit.lock().unwrap().remove(page) {
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
            self.grow(&mut state, required_order, lowest)?
        };

        #[cfg(debug_assertions)]
        {
            assert!(self.allocated_pages.lock().unwrap().insert(page_number));
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
            let mut since = self.allocated_since_commit.lock().unwrap();
            since.insert(page_number);
            let mut taken = self.taken.lock().unwrap();
            taken.0 = taken.0.max(since.counts().0 as u64);
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
            if let Some(page) = r {
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
        state.allocators.resize_to(new_layout);
        assert!(new_layout.len() <= layout.len());
        state.header.set_layout(new_layout);

        Ok(true)
    }

    fn grow(
        &self,
        state: &mut InMemoryState,
        required_order_allocation: u8,
        lowest: bool,
    ) -> Result<PageNumber> {
        let layout = state.header.layout();
        let required_growth =
            2u64.pow(required_order_allocation.into()) * u64::from(state.header.page_size());
        let max_region_size = u64::from(state.header.layout().full_region_layout().num_pages())
            * u64::from(state.header.page_size());
        // The file grows in small steps, so that it stays close to the size of what it holds:
        // by a sixteenth of its usable bytes once they pass 1 MiB, and never by less than twice
        // the allocation that needs the room. A smaller file doubles.
        let usable = layout.usable_bytes();
        let step = max(
            if self.recovery_end.load(Ordering::Acquire) != 0 {
                4 << 20
            } else if usable < (1 << 20) {
                usable
            } else {
                usable / 16
            },
            required_growth * 2,
        );
        let next_desired_size = if layout.num_full_regions() > 0 {
            if let Some(trailing) = layout.trailing_region_layout() {
                let room = max_region_size - trailing.usable_bytes();
                if 2 * required_growth < room {
                    // Grow the trailing region, at most until it is full
                    usable + min(step, room)
                } else {
                    // Fill out trailing & start the next region
                    usable + room + min(step, max_region_size)
                }
            } else {
                // Start the next region
                usable + min(step, max_region_size)
            }
        } else {
            usable + step
        };
        let mut new_layout = DatabaseLayout::calculate(
            next_desired_size,
            state.header.layout().full_region_layout().num_pages(),
            state
                .header
                .layout()
                .full_region_layout()
                .get_header_pages(),
            self.page_size,
        );
        let limit = self.file_size_limit.load(Ordering::Acquire);
        if new_layout.len() > limit {
            new_layout = DatabaseLayout::recalculate(
                limit.max(layout.len()),
                layout.full_region_layout().get_header_pages(),
                layout.full_region_layout().num_pages(),
                self.page_size,
            );
        }
        let no_room = || -> Result<PageNumber> {
            let limit = crate::FileSizeLimit(self.storage.raw_file_len()?, limit);
            Err(std::io::Error::new(ErrorKind::StorageFull, limit).into())
        };
        // The larger layout must hold a free block of the size asked for, not only that many
        // pages: the last region's free run at its end, and every region the growth adds
        let (last, block) = (layout.num_regions() - 1, 1 << required_order_allocation);
        let used = state.get_region(last).len() - state.get_region(last).trailing_free_pages();
        let fits = (last..new_layout.num_regions()).any(|region| {
            let start = if region == last {
                used.next_multiple_of(block)
            } else {
                0
            };
            start + block <= new_layout.region_layout(region).num_pages()
        });
        if !fits {
            return no_room();
        }
        assert!(new_layout.len() >= layout.len());

        self.storage.admit_growth(new_layout.len())?;
        self.storage.resize(new_layout.len(), true)?;

        state.allocators.resize_to(new_layout);
        state.header.set_layout(new_layout);
        match Self::allocate_helper_retry(state, required_order_allocation, lowest)? {
            Some(page) => Ok(page),
            None => no_room(),
        }
    }

    pub(crate) fn allocate<'txn>(
        &self,
        allocation_size: usize,
        allocated: &mut PageTrackerPolicy,
    ) -> Result<PageMut<'txn>> {
        let result = self.allocate_helper(
            allocation_size,
            self.allocator_state_lowest.load(Ordering::Acquire),
            true,
        );
        if let Ok(ref page) = result {
            assert!(allocated.insert(page.get_page_number()));
        }
        result
    }

    pub(crate) fn lowest_scope(&self, lowest: bool) -> impl Drop + '_ {
        struct ResetLowest<'a>(&'a AtomicBool, bool);
        impl Drop for ResetLowest<'_> {
            fn drop(&mut self) {
                self.0.store(self.1, Ordering::Release);
            }
        }
        ResetLowest(
            &self.allocator_state_lowest,
            self.allocator_state_lowest.swap(lowest, Ordering::AcqRel),
        )
    }

    pub(crate) fn allocate_lowest<'txn>(&self, allocation_size: usize) -> Result<PageMut<'txn>> {
        self.allocate_helper(allocation_size, true, true)
    }

    /// The lowest free place of a page of `order`, when one lies within the file's allowed growth
    pub(crate) fn allocate_within<'txn>(&self, order: u8) -> Result<Option<PageMut<'txn>>> {
        match self.allocate_lowest((self.page_size as usize) << order) {
            Err(StorageError::Io(e))
                if e.get_ref().is_some_and(|e| e.is::<crate::FileSizeLimit>()) =>
            {
                Ok(None)
            }
            copy => copy.map(Some),
        }
    }

    /// The lowest free place of `page`'s size, when it lies wholly below `page`
    pub(crate) fn allocate_below<'txn>(&self, page: PageNumber) -> Result<Option<PageMut<'txn>>> {
        let (size, header) = (self.page_size, self.region_header_with_padding_size);
        let start = |p: PageNumber| p.address_range(size.into(), self.region_size, header, size);
        let Some(copy) = self.allocate_within(page.page_order)? else {
            return Ok(None);
        };
        let number = copy.get_page_number();
        if start(number).end <= start(page).start {
            return Ok(Some(copy));
        }
        drop(copy);
        self.free_if_uncommitted(number, &mut PageTrackerPolicy::Ignore);
        Ok(None)
    }

    /// Holds taken, until [`Self::give_back`], the free blocks of the aligned run of `order`
    /// nearest under page `below` with some free pages and no more taken ones than the free pages
    /// below it, so nothing is placed in it; returns the run's pages and the blocks held. The
    /// nearest is the last a commit's lowest places would fill again.
    pub(crate) fn hold_run(&self, order: u8, below: u64) -> (Range<u64>, Vec<PageNumber>) {
        let mut state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let region_pages = u64::from(layout.full_region_layout().num_pages());
        let (mut best, mut lower) = (None, 0);
        for region in 0..layout.num_regions() {
            let first = u64::from(region) * region_pages;
            let end = u32::try_from(below.saturating_sub(first).min(region_pages)).unwrap();
            let allocator = state.get_region(region);
            if let Some((start, blocks)) = allocator.nearest_run(order, end, &mut lower) {
                best = Some((first + u64::from(start), region, blocks));
            }
        }
        let Some((at, region, blocks)) = best else {
            return (0..0, Vec::new());
        };
        let blocks: Vec<_> = (blocks.into_iter())
            .map(|(index, order)| PageNumber::new(region, index, order))
            .collect();
        for page in &blocks {
            let allocator = state.get_region_mut(region);
            allocator.record_alloc(page.page_index, page.page_order);
        }
        (at..at + (1 << order), blocks)
    }

    /// Gives back to the allocator the blocks [`Self::hold_run`] held
    pub(crate) fn give_back(&self, held: Vec<PageNumber>) {
        let mut state = self.state.lock().unwrap();
        for page in held {
            let order = state
                .get_region_mut(page.region)
                .free(page.page_index, page.page_order);
            state.get_region_tracker_mut().mark_free(order, page.region);
        }
    }

    pub(crate) fn count_uncommitted_pages(&self) -> usize {
        self.allocated_since_commit.lock().unwrap().counts().0
    }

    pub(crate) fn has_free_tail(&self) -> Result<bool> {
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let last = state.get_region(layout.num_regions() - 1);
        Ok(last.trailing_free_pages() > 0 && (layout.num_regions() > 1 || last.len() > 1))
    }

    pub(crate) fn file_len(&self) -> Result<u64> {
        self.storage.raw_file_len()
    }

    pub(crate) fn region_pages(&self) -> u64 {
        let state = self.state.lock().unwrap();
        u64::from(state.header.layout().full_region_layout().num_pages())
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_allocated_pages());
        }

        Ok(count)
    }

    pub(crate) fn count_free_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_free_pages());
        }

        Ok(count)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.page_size.try_into().unwrap()
    }

    // Syncs, clears the recovery mark when the free-page map is whole, and closes the file
    // whatever failed before, returning the first failure.
    pub(crate) fn close(&self) -> Result {
        let synced = self.storage.flush().and_then(|()| {
            if self.needs_recovery.load(Ordering::Acquire)
                || self.recovery_end.load(Ordering::Acquire) != 0
                || thread::panicking()
            {
                return Ok(());
            }
            let mut state = self.state.lock()?;
            state.header.recovery_required = false;
            self.write_header(&state.header)?;
            self.storage.flush()
        });
        let closed = self.storage.close();
        synced.and(closed)
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
}
