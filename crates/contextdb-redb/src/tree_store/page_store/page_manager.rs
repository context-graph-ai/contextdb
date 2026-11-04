#[cfg(feature = "contextdb-internal")]
thread_local! {
    static QUERY_PAGE_WINDOW: std::cell::RefCell<Option<(usize, ReadCachePageWindow)>> = const { std::cell::RefCell::new(None) };
}
#[cfg(feature = "contextdb-internal")]
struct QueryPageWindowReset(Option<(usize, ReadCachePageWindow)>);
#[cfg(feature = "contextdb-internal")]
impl Drop for QueryPageWindowReset {
    fn drop(&mut self) {
        QUERY_PAGE_WINDOW.with(|slot| drop(slot.replace(self.0.take())));
    }
}

use crate::CacheStats;
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
#[cfg(feature = "contextdb-internal")]
use crate::tree_store::page_store::cached_file::ReadCachePageWindow;
use crate::tree_store::page_store::fast_hash::PageNumberHashSet;
#[cfg(feature = "contextdb-internal")]
use crate::tree_store::page_store::header::TransactionHeader;
use crate::tree_store::page_store::header::{DB_HEADER_SIZE, DatabaseHeader, MAGICNUMBER};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{
    Allocators, FREE_TOTALS_ENCODED_LEN, FreeTotals, RegionTracker, SavedBitmapWindow,
    SavedRegionHeader, layout_pages,
};
use crate::tree_store::page_store::{PageImpl, PageMut, hash128_with_seed};
use crate::tree_store::{Page, PageNumber, PageTrackerPolicy};
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
            .map_err(storage_error_as_io)?;
        out.copy_from_slice(&data);
        Ok(())
    }
    /// The value's fixed header: `(maximum order, saved page count)`.
    fn header(&self) -> io::Result<(u8, u32)> {
        let mut max_order = [0];
        self.read(0, &mut max_order)?;
        if max_order[0] > MAX_MAX_PAGE_ORDER {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid allocator maximum order",
            ));
        }
        let mut num_pages = [0; 4];
        self.read(4, &mut num_pages)?;
        Ok((max_order[0], u32::from_le_bytes(num_pages)))
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
            for field in lengths.as_chunks::<4>().0.iter().take(usize::from(order)) {
                start = start
                    .checked_add(u32::from_le_bytes(*field) as usize)
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

/// A storage failure carried through the allocator pager's `io::Result` keeps
/// its I/O kind, so a short read stays damage and EIO stays I/O (CDB-207).
fn storage_error_as_io(error: StorageError) -> io::Error {
    match error {
        StorageError::Io(error) => error,
        #[cfg(feature = "contextdb-internal")]
        StorageError::Native(crate::NativeColdReadError::Io(error)) => error,
        error => io::Error::other(error),
    }
}

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

/// A growth ratio `r` in (0, 1] is exactly `mantissa x 2^-shift` for a
/// 53-bit mantissa; both pack into one word so a reader never sees half of
/// an update. 0 means no ratio (upstream growth).
#[cfg(feature = "contextdb-internal")]
const GROWTH_MANTISSA_BITS: u64 = 53;
#[cfg(feature = "contextdb-internal")]
const GROWTH_MANTISSA_MASK: u64 = (1 << GROWTH_MANTISSA_BITS) - 1;

#[cfg(feature = "contextdb-internal")]
fn pack_growth_ratio(ratio: f64) -> u64 {
    if ratio.is_nan() || ratio <= 0.0 {
        return 0;
    }
    let bits = ratio.min(1.0).to_bits();
    let exponent = (bits >> 52) & 0x7ff;
    let fraction = bits & ((1 << 52) - 1);
    let (mantissa, shift) = if exponent == 0 {
        (fraction, 1074)
    } else {
        (fraction | (1 << 52), 1075 - exponent)
    };
    (shift << GROWTH_MANTISSA_BITS) | mantissa
}

#[cfg(feature = "contextdb-internal")]
fn unpack_growth_ratio(packed: u64) -> Option<f64> {
    if packed == 0 {
        return None;
    }
    let (mantissa, shift) = (
        packed & GROWTH_MANTISSA_MASK,
        packed >> GROWTH_MANTISSA_BITS,
    );
    // Recreate the original bits directly: normal values carry the implicit
    // leading bit, while subnormals retain their fraction with exponent zero.
    let bits = if mantissa < (1 << 52) {
        mantissa
    } else {
        ((1075 - shift) << 52) | (mantissa & ((1 << 52) - 1))
    };
    Some(f64::from_bits(bits))
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
    // contextdb: the most one growth may add, as the exact binary value of
    // the caller's declared ratio of the current usable size (never less
    // than twice the request), packed by `pack_growth_ratio`; 0 keeps
    // upstream's growth.
    #[cfg(feature = "contextdb-internal")]
    growth_step_ratio: std::sync::atomic::AtomicU64,
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
    /// While set, every page and window read on this handle is charged in
    /// base pages BEFORE it is made, and a read that would exceed the budget
    /// is refused without reading or allocating anything.
    #[cfg(feature = "contextdb-internal")]
    read_budget: Mutex<Option<ReadBudget>>,
    // The fixed cache invoice also covers allocator windows in state. Free
    // that state before storage releases its final genuine reserve owner.
    storage: PagedCachedFile,
}

/// One commit slot of a crashed store's header.
#[cfg(feature = "contextdb-internal")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct CrashedCommitSlot {
    pub(crate) transaction_id: TransactionId,
    pub(crate) user_root: Option<BtreeHeader>,
    pub(crate) system_root: Option<BtreeHeader>,
}

/// The slot repair selects first, the one it falls back to when that slot's
/// tree fails verification, and whether the selected slot was written with
/// two-phase commit (repair never falls back from such a slot).
#[cfg(feature = "contextdb-internal")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct CrashedCommitCandidates {
    pub(crate) chosen: CrashedCommitSlot,
    pub(crate) alternate: CrashedCommitSlot,
    pub(crate) two_phase: bool,
}

/// A fixed allowance of base-page reads.
#[cfg(feature = "contextdb-internal")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadBudget {
    pub(crate) remaining_pages: u64,
    pub(crate) charged_pages: u64,
    pub(crate) requests: u64,
}

/// Rewrite one field of a CLOSED store's header in place, for proofs.
#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
pub(crate) fn rewrite_header_for_test(
    path: &std::path::Path,
    recovery_required: Option<bool>,
    two_phase_commit: Option<bool>,
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let mut bytes = vec![0; DB_HEADER_SIZE];
    file.read_exact_at(&mut bytes, 0)?;
    let (mut header, _) = DatabaseHeader::from_bytes(&bytes)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if let Some(value) = recovery_required {
        header.recovery_required = value;
    }
    if let Some(value) = two_phase_commit {
        header.two_phase_commit = value;
    }
    file.write_all_at(&header.to_bytes(true), 0)?;
    file.sync_all()
}

impl TransactionalMemory {
    #[cfg(feature = "test-seams")]
    pub(crate) fn write_control_for_test(&self) -> crate::CacheWriteControlForTest {
        self.storage.write_control_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn cached_header_for_test(&self) -> Result<super::cached_file::CachePageForTest> {
        self.storage.cached_header_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn cache_allocations_for_test(
        &self,
        visit: &mut dyn FnMut((usize, usize, usize)),
    ) -> usize {
        self.storage.allocations_for_test(visit)
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn controls_for_test(&self) -> [(usize, usize); 2] {
        self.storage.controls_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn lifetime_aliases_for_test(
        &self,
    ) -> Result<(
        crate::CachePageForTest,
        crate::CachePageForTest,
        crate::CacheWriteForTest,
    )> {
        self.storage.lifetime_aliases_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn detach_cached_memory_for_test(
        &self,
        memory: &super::cached_file::PageMemory,
    ) -> (u64, usize, usize, bool) {
        self.storage.detach_cached_memory_for_test(memory)
    }

    /// The file byte range a page number occupies, for proofs that damage a
    /// closed store in place.
    #[cfg(any(test, all(feature = "test-seams", feature = "contextdb-internal")))]
    pub(crate) fn page_address_for_test(&self, page: PageNumber) -> std::ops::Range<u64> {
        page.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        )
    }

    /// Charge one read of `range` inside an extent of `extent_len` bytes.
    #[cfg(feature = "contextdb-internal")]
    fn charge_read(&self, extent_len: u64, range: std::ops::Range<u64>) -> Result {
        self.charge_read_with(extent_len, range, |reason| {
            StorageError::Corrupted(reason.to_owned())
        })
    }

    #[cfg(feature = "contextdb-internal")]
    fn charge_read_with<E: From<StorageError>>(
        &self,
        extent_len: u64,
        range: std::ops::Range<u64>,
        corrupted: impl Fn(&'static str) -> E,
    ) -> std::result::Result<(), E> {
        let mut budget = self.read_budget.lock().map_err(StorageError::from)?;
        let Some(budget) = budget.as_mut() else {
            return Ok(());
        };
        let refused = || corrupted("admission read exceeds its page budget");
        if range.start >= range.end || range.end > extent_len {
            return Err(corrupted("admission read is outside its page extent"));
        }
        let page = u64::from(self.page_size);
        let first = range.start / page;
        let last = (range.end - 1) / page;
        let cost = last
            .checked_sub(first)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(refused)?;
        budget.remaining_pages = budget
            .remaining_pages
            .checked_sub(cost)
            .ok_or_else(refused)?;
        budget.charged_pages = budget.charged_pages.checked_add(cost).ok_or_else(refused)?;
        budget.requests = budget.requests.checked_add(1).ok_or_else(refused)?;
        Ok(())
    }

    /// While a budget is in force, a page number whose order no valid store
    /// can carry is refused before its extent is even computed for a read.
    #[cfg(feature = "contextdb-internal")]
    fn refuse_invalid_order(&self, page: PageNumber) -> Result {
        if self.read_budget.lock()?.is_some() && page.page_order > MAX_MAX_PAGE_ORDER {
            return Err(StorageError::Corrupted(
                "admission read names a page order no store can carry".to_owned(),
            ));
        }
        Ok(())
    }

    /// Run `read` with every page read charged against `pages`.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn with_read_budget<T>(
        &self,
        pages: u64,
        read: impl FnOnce() -> std::result::Result<T, crate::DatabaseError>,
    ) -> std::result::Result<
        (std::result::Result<T, crate::DatabaseError>, ReadBudget),
        crate::DatabaseError,
    > {
        *self.read_budget.lock().map_err(StorageError::from)? = Some(ReadBudget {
            remaining_pages: pages,
            charged_pages: 0,
            requests: 0,
        });
        let answer = read();
        let spent = self
            .read_budget
            .lock()
            .map_err(StorageError::from)?
            .take()
            .expect("the budget installed above");
        Ok((answer, spent))
    }
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

    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    pub(crate) fn new(
        file: impl Into<crate::db::Backend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        external_read: bool,
        allocator_free: bool,
    ) -> Result<Self, DatabaseError> {
        let file: crate::db::Backend = file.into();
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
        // An externally pinned reader never touches allocator state, so it
        // builds none: its open costs the header, not the store's regions.
        #[cfg(feature = "contextdb-internal")]
        let state = if external_read || allocator_free {
            InMemoryState {
                header,
                allocators: Allocators::empty(),
            }
        } else {
            InMemoryState::new(header, scratch, cache_size)?
        };
        #[cfg(not(feature = "contextdb-internal"))]
        let state = {
            let _ = allocator_free;
            InMemoryState::new(header, scratch, cache_size)?
        };

        assert!(page_size >= DB_HEADER_SIZE);

        Ok(Self {
            allocated_since_commit: Mutex::new(Default::default()),
            unpersisted: Mutex::new(Default::default()),
            needs_recovery: AtomicBool::new(needs_recovery),
            #[cfg(feature = "contextdb-internal")]
            publication_needs_recovery: AtomicBool::new(false),
            #[cfg(feature = "contextdb-internal")]
            growth_step_ratio: std::sync::atomic::AtomicU64::new(0),
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
            #[cfg(feature = "contextdb-internal")]
            read_budget: Mutex::new(None),
        })
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn retire_read_cache_request(&self, request: u64) {
        self.storage.retire_read_cache_request(request);
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

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn retain_cache_reserve(
        &self,
        reserve: crate::native_storage_admission::NativeStorageCredit,
    ) -> super::page_allocation::CacheReserveOwner {
        self.storage.retain_cache_reserve(reserve)
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn installed_cache_capacity_for_test(&self) -> usize {
        self.storage.installed_cache_capacity_for_test()
    }
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn set_read_cache_capacity(&self, bytes: usize) -> Result<usize> {
        self.storage.set_read_cache_capacity(bytes)
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn with_read_cache_capacity<R>(
        &self,
        bytes: usize,
        settle: impl FnOnce() -> R,
    ) -> Result<R> {
        self.storage.with_read_cache_capacity(bytes, settle)
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn installed_cache_capacity(&self) -> usize {
        self.storage.installed_cache_capacity()
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn read_cache_held_bytes(&self) -> usize {
        self.storage.read_cache_held_bytes()
    }

    pub(crate) fn cache_index_bytes(&self) -> usize {
        self.storage.index_bytes()
    }

    pub(crate) fn cache_stripe_control_bytes(&self) -> usize {
        self.storage.stripe_control_bytes()
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn with_page_window<R>(
        &self,
        operation: impl FnOnce() -> R,
    ) -> (R, ReadCachePageWindow) {
        let window = self.storage.begin_page_window();
        let previous = QUERY_PAGE_WINDOW
            .with(|slot| slot.replace(Some((std::ptr::from_ref(self) as usize, window.clone()))));
        let _reset = QueryPageWindowReset(previous);
        (operation(), window)
    }

    #[cfg(feature = "contextdb-internal")]
    fn record_query_page(&self, page: PageNumber) {
        QUERY_PAGE_WINDOW.with(|slot| {
            let current = slot.borrow();
            if let Some((identity, window)) = current.as_ref()
                && *identity == std::ptr::from_ref(self) as usize
            {
                window.record_page(page.to_le_bytes());
            }
        });
    }

    #[cfg(all(feature = "contextdb-internal", feature = "test-seams"))]
    pub(crate) fn page_window_entries_visited_for_test(&self) -> usize {
        self.storage.page_window_entries_visited_for_test()
    }

    #[cfg(feature = "contextdb-internal")]
    #[allow(
        clippy::cast_precision_loss,
        reason = "An advisory fraction permits rounding for very large input slices."
    )]
    pub(crate) fn read_cache_resident_fraction(&self, page_ids: &[[u8; 8]]) -> f64 {
        if page_ids.is_empty() {
            return 0.0;
        }
        let resident = page_ids
            .iter()
            .filter(|id| {
                let Some(page) = PageNumber::validated_from_le_bytes(**id) else {
                    return false;
                };
                if page.to_le_bytes() != **id {
                    return false;
                }
                let len = page.page_size_bytes(self.page_size);
                let Some(regional_start) = u64::from(page.page_index)
                    .checked_mul(len)
                    .and_then(|offset| offset.checked_add(self.region_header_with_padding_size))
                else {
                    return false;
                };
                if regional_start
                    .checked_add(len)
                    .is_none_or(|end| end > self.region_size)
                {
                    return false;
                }
                let Some(offset) = u64::from(page.region)
                    .checked_mul(self.region_size)
                    .and_then(|offset| offset.checked_add(u64::from(self.page_size)))
                    .and_then(|offset| offset.checked_add(regional_start))
                else {
                    return false;
                };
                let Ok(len) = usize::try_from(len) else {
                    return false;
                };
                self.storage.read_cache_contains(offset, len)
            })
            .count();
        resident as f64 / page_ids.len() as f64
    }

    pub(crate) fn cache_stats(&self) -> CacheStats {
        self.storage.cache_stats()
    }

    /// Refuses writers and everything that would write. The latch comes first,
    /// before any lock a caught commit unwind may have poisoned.
    pub(crate) fn check_io_errors(&self) -> Result {
        self.storage.check_commit_latch()?;
        self.check_read_errors()
    }

    /// A held page is not read again, so this reports what a read would have.
    /// A failed commit does not stop committed readers; an I/O failure does.
    pub(crate) fn check_read_errors(&self) -> Result {
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
        self.storage.check_read_errors()
    }

    pub(crate) fn latch_failed_commit(&self) {
        self.storage.latch_failed_commit();
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn commit_failure_latched(&self) -> bool {
        self.storage.commit_failure_latched()
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

    /// Caps every later growth at `ratio` of the usable size at that moment
    /// (rounded down to whole bytes, never up), or twice the request when
    /// that is larger. A ratio that is not positive restores upstream's
    /// doubling; one above 1 is held at 1.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn set_growth_step_ratio(&self, ratio: f64) {
        self.growth_step_ratio
            .store(pack_growth_ratio(ratio), Ordering::Release);
    }

    /// The ratio installed by `set_growth_step_ratio`, exactly, or `None`
    /// for upstream's growth.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn growth_step_ratio(&self) -> Option<f64> {
        unpack_growth_ratio(self.growth_step_ratio.load(Ordering::Acquire))
    }

    /// The most one growth of `usable` bytes may add for a request of
    /// `required` bytes, or `None` for upstream's growth. The ratio term is
    /// `floor(usable x ratio)`, computed exactly from the ratio's binary value.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn growth_step(&self, usable: u64, required: u64) -> Option<u64> {
        let packed = self.growth_step_ratio.load(Ordering::Acquire);
        if packed == 0 {
            return None;
        }
        let (mantissa, shift) = (
            packed & GROWTH_MANTISSA_MASK,
            packed >> GROWTH_MANTISSA_BITS,
        );
        let step = if shift >= 128 {
            0
        } else {
            u64::try_from((u128::from(usable) * u128::from(mantissa)) >> shift).unwrap_or(u64::MAX)
        };
        Some(max(step, 2 * required))
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
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_root_work(
            crate::commit_work::RootWorkKindForTest::HeaderBegin,
            Some("__redb_file_header"),
            None,
            DB_HEADER_SIZE,
        )?;
        self.storage
            .write(0, DB_HEADER_SIZE, true)?
            .mem_mut()
            .copy_from_slice(&header.to_bytes(true));

        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_root_work(
            crate::commit_work::RootWorkKindForTest::HeaderEnd,
            Some("__redb_file_header"),
            None,
            DB_HEADER_SIZE,
        )?;

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
        #[cfg(test)]
        if let Some(hook) = BEFORE_NEXT_ALLOCATOR_SAVE.with(|slot| slot.borrow_mut().take()) {
            hook(self);
        }
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

    /// Invert one order-0 summary bit of region 0 without touching its
    /// leaves; returns `(region, order, height, entry)`, or `None` when
    /// order 0 has no summary level.
    #[cfg(feature = "test-seams")]
    pub(crate) fn plant_allocator_summary_mismatch_for_test(&self) -> Option<(u32, u8, u32, u32)> {
        let (height, entry) = self
            .state
            .lock()
            .unwrap()
            .allocators
            .get_region_mut(0)
            .invert_order_0_summary_bit_for_test()?;
        Some((0, 0, height, entry))
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
            let header: StdArc<SavedRegionHeader> = {
                let mem = mem.clone();
                StdArc::new(move |region| {
                    let mem = mem.upgrade().ok_or_else(|| {
                        io::Error::new(ErrorKind::BrokenPipe, "allocator source database closed")
                    })?;
                    let Some((page_number, range)) =
                        crate::tree_store::btree::fixed_key_value_location::<AllocatorStateKey>(
                            root,
                            &mem,
                            hint,
                            &AllocatorStateKey::Region(region),
                        )
                        .map_err(storage_error_as_io)?
                    else {
                        return Ok(None);
                    };
                    SavedAllocatorValue {
                        mem: &mem,
                        page: page_number,
                        range,
                        hint,
                    }
                    .header()
                    .map(Some)
                })
            };
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
                        .map_err(storage_error_as_io)?
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
            state.allocators.set_saved_source(source, header)?;
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

    #[cfg_attr(
        not(debug_assertions),
        expect(
            unused_variables,
            reason = "KEEP-FEATURE: read only by debug-assertion checks"
        )
    )]
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

        // Every page this commit wrote reaches the file before any header
        // naming it does: the header leaves the buffer last, in the flush
        // below, so a process that dies at any point leaves a header whose
        // commit is complete in the file it names. Same writes, same one sync.
        self.storage.write_barrier()?;
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

        // Every page this commit wrote reaches the file before any header
        // naming it does: the header leaves the buffer last, in the flush
        // below, so a process that dies at any point leaves a header whose
        // commit is complete in the file it names. Same writes, same one sync.
        self.storage.write_barrier()?;
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
        #[cfg(feature = "contextdb-internal")]
        self.record_query_page(page_number);
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        #[cfg(feature = "contextdb-internal")]
        self.refuse_invalid_order(page_number)?;
        #[cfg(feature = "contextdb-internal")]
        self.charge_read(range.end - range.start, 0..(range.end - range.start))?;
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
        #[cfg(feature = "contextdb-internal")]
        self.record_query_page(page);
        let address = page.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        #[cfg(feature = "contextdb-internal")]
        self.refuse_invalid_order(page)?;
        #[cfg(feature = "contextdb-internal")]
        self.charge_read(
            address.end - address.start,
            u64::try_from(range.start).unwrap_or(u64::MAX)
                ..u64::try_from(range.end).unwrap_or(u64::MAX),
        )?;
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
        #[cfg(feature = "test-seams")]
        let observation = crate::allocation_observation::current();
        #[cfg(feature = "test-seams")]
        let plan = if let Some(scope) = &observation {
            let plan = crate::allocation_observation::AllocationPlanForTest {
                requested_bytes: len,
                current_store_bytes: self.storage.raw_file_len()?,
                minimum_growth_bytes: 0,
                reusable_space_withheld: scope.withhold_reusable_space,
            };
            scope.observer().before_existing_write(plan)?;
            scope
                .observer()
                .before_write(plan, page_number.to_le_bytes(), len)?;
            Some(plan)
        } else {
            None
        };
        let mem = match self.storage.write(address_range.start, len, false) {
            Ok(mem) => mem,
            Err(error) => {
                #[cfg(feature = "test-seams")]
                if let (Some(scope), Some(plan)) = (&observation, plan) {
                    scope.observer().refused(plan, &error);
                }
                return Err(error);
            }
        };

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));
        }

        let page = PageMut {
            mem,
            page_number,
            _lifetime: PhantomData,
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        };
        #[cfg(feature = "test-seams")]
        if let (Some(scope), Some(plan)) = (observation, plan) {
            scope
                .observer()
                .written(plan, page_number.to_le_bytes(), len);
        }
        Ok(page)
    }

    pub(crate) fn get_version(&self) -> u8 {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().version
        } else {
            state.header.primary_slot().version
        }
    }

    /// A crashed store's two commit candidates, in the order a writer's
    /// repair tries them: the header is read again (one charged page) and
    /// the slot is chosen by repair's own routine on that copy, which never
    /// touches this handle's state. Nothing is written.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn crashed_commit_candidates(
        &self,
    ) -> std::result::Result<CrashedCommitCandidates, DatabaseError> {
        self.charge_read(u64::from(self.page_size), 0..DB_HEADER_SIZE as u64)?;
        let bytes = self.storage.read_direct(0, DB_HEADER_SIZE)?;
        let (mut header, repair_info) = DatabaseHeader::from_bytes(&bytes)?;
        if repair_info.invalid_magic_number {
            return Err(StorageError::Corrupted("invalid magic number".to_owned()).into());
        }
        header.pick_primary_for_repair(repair_info)?;
        let slot = |slot: &TransactionHeader| CrashedCommitSlot {
            transaction_id: slot.transaction_id,
            user_root: slot.user_root,
            system_root: slot.system_root,
        };
        Ok(CrashedCommitCandidates {
            chosen: slot(header.primary_slot()),
            alternate: slot(header.secondary_slot()),
            two_phase: header.two_phase_commit,
        })
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn capture_native_read_root(&self) -> Result<(TransactionId, Option<BtreeHeader>)> {
        let state = self.state.lock()?;
        let slot = if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot()
        } else {
            state.header.primary_slot()
        };
        Ok((slot.transaction_id, slot.user_root))
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

    #[cfg(feature = "test-seams")]
    pub(crate) fn allocation_plan_for_test(
        &self,
        allocation_size: usize,
        withhold_reusable_space: bool,
    ) -> Result<crate::allocation_observation::AllocationPlanForTest> {
        let required_order = ceil_log2(allocation_size.div_ceil(self.get_page_size()));
        let mut state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let mut reusable = false;
        if !withhold_reusable_space {
            for region in 0..layout.num_regions() {
                if state
                    .get_region(region)
                    .highest_free_order()
                    .is_some_and(|order| order >= required_order)
                {
                    reusable = true;
                    break;
                }
            }
        }
        if let Some(error) = state
            .allocators
            .pager()
            .and_then(|pager| pager.storage_failure())
        {
            return Err(error);
        }
        let current_store_bytes = self.storage.raw_file_len()?;
        let minimum_growth_bytes = if reusable {
            0
        } else {
            self.minimum_growth_layout(&state, required_order)
                .len()
                .saturating_sub(current_store_bytes)
        };
        Ok(crate::allocation_observation::AllocationPlanForTest {
            requested_bytes: allocation_size,
            current_store_bytes,
            minimum_growth_bytes,
            reusable_space_withheld: withhold_reusable_space,
        })
    }

    pub(crate) fn allocate_helper<'txn>(
        &self,
        allocation_size: usize,
        lowest: bool,
        transactional: bool,
    ) -> Result<PageMut<'txn>> {
        let required_pages = allocation_size.div_ceil(self.get_page_size());
        let required_order = ceil_log2(required_pages);

        #[cfg(feature = "test-seams")]
        let observation = crate::allocation_observation::current();
        #[cfg(feature = "test-seams")]
        let plan = if let Some(scope) = &observation {
            let plan =
                self.allocation_plan_for_test(allocation_size, scope.withhold_reusable_space)?;
            // The plan's state guard has been released before external code runs.
            scope.observer().before_allocate(plan)?;
            Some(plan)
        } else {
            None
        };
        let mut state = self.state.lock().unwrap();
        #[cfg(feature = "test-seams")]
        let withheld = observation
            .as_ref()
            .is_some_and(|scope| scope.withhold_reusable_space);
        #[cfg(not(feature = "test-seams"))]
        let withheld = false;

        let placement = (|| -> Result<PageNumber> {
            let page_number = if withheld {
                #[cfg(feature = "test-seams")]
                {
                    self.allocate_after_growth_for_test(&mut state, required_order)?
                }
                #[cfg(not(feature = "test-seams"))]
                {
                    unreachable!()
                }
            } else if let Some(page_number) =
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

            Ok(page_number)
        })();
        let page_number = match placement {
            Ok(page) => page,
            Err(error) => {
                #[cfg(feature = "test-seams")]
                if let (Some(scope), Some(plan)) = (&observation, plan) {
                    drop(state);
                    scope.observer().refused(plan, &error);
                }
                return Err(error);
            }
        };

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

        #[cfg(feature = "test-seams")]
        if let (Some(scope), Some(plan)) = (&observation, plan) {
            drop(state);
            scope
                .observer()
                .before_write(plan, page_number.to_le_bytes(), len)?;
        }
        let mut mem = match self.storage.write(address_range.start, len, true) {
            Ok(mem) => mem,
            Err(error) => {
                #[cfg(feature = "test-seams")]
                if let (Some(scope), Some(plan)) = (&observation, plan) {
                    scope.observer().refused(plan, &error);
                }
                return Err(error);
            }
        };
        debug_assert!(mem.mem().len() >= allocation_size);

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));

            // Poison the memory in debug mode to help detect uninitialized reads
            mem.mem_mut().fill(0xFF);
        }

        let page = PageMut {
            mem,
            page_number,
            _lifetime: PhantomData,
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        };
        #[cfg(feature = "test-seams")]
        if let (Some(scope), Some(plan)) = (observation, plan) {
            // All allocator bookkeeping and physical page backing succeeded.
            scope.observer().allocated(plan, page_number.to_le_bytes());
            scope
                .observer()
                .written(plan, page_number.to_le_bytes(), len);
        }

        Ok(page)
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

    // Shared by ordinary quota retry and the read-only test observation.
    fn minimum_growth_layout(&self, state: &InMemoryState, required_order: u8) -> DatabaseLayout {
        let layout = state.header.layout();
        let required_growth = 2u64.pow(required_order.into()) * u64::from(state.header.page_size());
        DatabaseLayout::calculate(
            layout.usable_bytes() + required_growth * 2,
            layout.full_region_layout().num_pages(),
            layout.full_region_layout().get_header_pages(),
            self.page_size,
        )
    }

    #[cfg(feature = "test-seams")]
    fn allocate_after_growth_for_test(
        &self,
        state: &mut InMemoryState,
        order: u8,
    ) -> Result<PageNumber> {
        let old_layout = state.header.layout();
        let last_region = old_layout.num_regions() - 1;
        let old_pages = state.get_region(last_region).len();
        self.grow(state, order)?;
        let block_pages = 1u32 << order;
        let aligned_page = old_pages.div_ceil(block_pages) * block_pages;
        let (region, page) = if aligned_page
            .checked_add(block_pages)
            .is_some_and(|end| end <= state.get_region(last_region).len())
        {
            (last_region, aligned_page)
        } else {
            (last_region + 1, 0)
        };
        // Only the newly added suffix is eligible. Existing reusable blocks
        // remain free, including after this scope ends or the transaction aborts.
        state
            .get_region_mut(region)
            .record_alloc(page >> order, order);
        state.allocators.note_allocated(order);
        Ok(PageNumber::new(region, page >> order, order))
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
        // contextdb: upstream doubles a single region, and fills out a short
        // trailing region, so the first growth after a compaction extended a
        // 2.16 GB store to 4 GiB. A caller-declared step keeps growth
        // geometric at its own ratio; twice the request still guarantees the
        // requested order's aligned block.
        #[cfg(feature = "contextdb-internal")]
        let next_desired_size = match self.growth_step(layout.usable_bytes(), required_growth) {
            None => next_desired_size,
            Some(step) => min(next_desired_size, layout.usable_bytes() + step),
        };
        let calculate = |desired_size| {
            DatabaseLayout::calculate(
                desired_size,
                state.header.layout().full_region_layout().num_pages(),
                state
                    .header
                    .layout()
                    .full_region_layout()
                    .get_header_pages(),
                self.page_size,
            )
        };
        let mut new_layout = calculate(next_desired_size);
        assert!(new_layout.len() >= layout.len());

        let mut result = self.storage.resize(new_layout.len());
        // Storage refused the preferred growth for quota: retry once at the
        // smallest growth that still guarantees this allocation's block.
        let minimal_layout = self.minimum_growth_layout(state, required_order_allocation);
        if minimal_layout.len() < new_layout.len()
            && result.as_ref().is_err_and(crate::db::backend_quota_refused)
        {
            new_layout = minimal_layout;
            result = self.storage.resize(new_layout.len());
        }
        if result.is_err() {
            if result.as_ref().is_err_and(crate::db::backend_quota_refused) {
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
        // Lowest-first placement: the first free block of an order can sit
        // at the very end of the file (growth from an odd length leaves small
        // buddy blocks at both ends), and one page there stops every trim and
        // makes the free space before it look reclaimable. Taking the lowest
        // free pages keeps the end of the file free for the trim.
        let result = self.allocate_helper(allocation_size, true, true);
        if let Ok(ref page) = result {
            allocated.insert(page.get_page_number());
        }
        result
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
        self.record_query_page(page);
        let (offset, len) = self.native_page_address(page)?;
        self.storage.read_native_clean_miss(offset, len)
    }

    #[allow(
        clippy::result_large_err,
        reason = "Typed native refusals stay inline so an exhausted budget can return its original error without allocating."
    )]
    pub(crate) fn get_prepared_page_memory(
        &self,
        page: PageNumber,
    ) -> std::result::Result<super::cached_file::PageMemory, crate::PreparedReadError> {
        self.record_query_page(page);
        // Preserve the existing read-work checkpoint and its producer error.
        if self
            .read_budget
            .lock()
            .map_err(StorageError::from)?
            .is_some()
            && page.page_order > MAX_MAX_PAGE_ORDER
        {
            return Err(crate::PreparedReadError::StaticCorrupted(
                "admission read names a page order no store can carry",
            ));
        }
        if page.page_order <= MAX_MAX_PAGE_ORDER {
            let len = (1u64 << page.page_order) * u64::from(self.page_size);
            self.charge_read_with(len, 0..len, crate::PreparedReadError::StaticCorrupted)?;
        }
        let (offset, len) = self.native_page_address(page)?;
        self.storage.read_native_prepared(offset, len)
    }

    fn native_page_address(
        &self,
        page: PageNumber,
    ) -> std::result::Result<(u64, usize), crate::NativeColdReadError> {
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
        Ok((offset, len))
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
            .set_saved_source(Arc::new(|_, _, _, _, _| Ok(false)), Arc::new(|_| Ok(None)))
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

#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
impl TransactionalMemory {
    pub(crate) fn native_cache_occupancy_for_test(
        &self,
    ) -> crate::native_page_observations::NativeCacheOccupancyForTest {
        self.storage.native_cache_occupancy_for_test()
    }
    pub(crate) fn evict_unpinned_clean_pages_for_test(
        &self,
    ) -> (
        crate::native_page_observations::NativeCacheOccupancyForTest,
        crate::native_page_observations::NativeCacheOccupancyForTest,
    ) {
        self.storage.evict_unpinned_clean_pages_for_test()
    }
}

/// A test hook run at the entry of one allocator save.
#[cfg(test)]
type AllocatorSaveHook = Box<dyn FnOnce(&TransactionalMemory)>;

#[cfg(test)]
thread_local! {
    /// Runs once, on this thread, at the entry of the next
    /// `try_save_allocator_state`: after the commit's last allocation and
    /// before anything is checked or encoded.
    static BEFORE_NEXT_ALLOCATOR_SAVE: std::cell::RefCell<Option<AllocatorSaveHook>> =
        const { std::cell::RefCell::new(None) };
}

// Test-only inspection of the allocator's summary levels.
#[cfg(test)]
impl TransactionalMemory {
    /// Run `hook` at the entry of the next allocator save on this thread.
    pub(crate) fn before_next_allocator_save_for_test(hook: AllocatorSaveHook) {
        BEFORE_NEXT_ALLOCATOR_SAVE.with(|slot| *slot.borrow_mut() = Some(hook));
    }

    /// Every inexact regional summary bit, as `(region, order, height, entry)`.
    pub(crate) fn summary_mismatches_for_test(&self) -> Vec<(u32, u8, u32, u32)> {
        let mut state = self.state.lock().unwrap();
        let regions = state.header.layout().num_regions();
        let mut mismatches = Vec::new();
        for region in 0..regions {
            mismatches.extend(
                state
                    .allocators
                    .get_region(region)
                    .summary_mismatches_for_test()
                    .into_iter()
                    .map(|(order, height, entry)| (region, order, height, entry)),
            );
        }
        mismatches
    }

    pub(crate) fn plant_summary_bit_for_test(
        &self,
        region: u32,
        order: u8,
        height: u32,
        entry: u32,
        full: bool,
    ) {
        self.state
            .lock()
            .unwrap()
            .allocators
            .get_region_mut(region)
            .plant_summary_bit_for_test(order, height, entry, full);
    }

    pub(crate) fn summary_levels_for_test(&self, region: u32, order: u8) -> Vec<u32> {
        self.state
            .lock()
            .unwrap()
            .allocators
            .get_region(region)
            .summary_levels_for_test(order)
    }

    pub(crate) fn summary_word_for_test(
        &self,
        region: u32,
        order: u8,
        height: u32,
        index: u32,
    ) -> u64 {
        self.state
            .lock()
            .unwrap()
            .allocators
            .get_region(region)
            .summary_word_for_test(order, height, index)
    }

    pub(crate) fn region_max_order_for_test(&self, region: u32) -> u8 {
        self.state
            .lock()
            .unwrap()
            .allocators
            .get_region(region)
            .get_max_order()
    }

    pub(crate) fn region_counted_free_pages_for_test(&self, region: u32) -> u32 {
        self.state
            .lock()
            .unwrap()
            .allocators
            .get_region(region)
            .count_free_pages()
    }

    pub(crate) fn layout_len_for_test(&self) -> u64 {
        self.state.lock().unwrap().header.layout().len()
    }

    pub(crate) fn region_count_for_test(&self) -> u32 {
        self.state.lock().unwrap().header.layout().num_regions()
    }

    /// Whether the region tracker holds `region` as full at `order`.
    pub(crate) fn region_marked_full_for_test(&self, region: u32, order: u8) -> bool {
        self.state
            .lock()
            .unwrap()
            .allocators
            .region_tracker
            .marked_full_for_test(order, region)
    }

    /// Shrink the allocator's resident window budget, spilling to scratch.
    pub(crate) fn set_allocator_window_budget_for_test(&self, bytes: usize) {
        self.state
            .lock()
            .unwrap()
            .allocators
            .set_window_budget(bytes)
            .unwrap();
    }

    /// The allocator pager's `(word reads, window loads, scratch reads)`.
    pub(crate) fn allocator_paging_counters_for_test(&self) -> (u64, u64, u64) {
        self.state
            .lock()
            .unwrap()
            .allocators
            .pager()
            .expect("the paged allocator")
            .counters_for_test()
    }
}

/// A saved allocator installed under a shorter geometry (the trim a
/// quick-repair commit does after saving, then the next open) and every other
/// length transition leave each regional summary exact, count every free page
/// and keep every counted page allocatable.
#[cfg(test)]
mod saved_allocator_install_contract {
    use super::InMemoryState;
    use crate::transaction_tracker::TransactionId;
    use crate::tree_store::page_store::base::MAX_PAGE_INDEX;
    use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
    use crate::tree_store::page_store::header::DatabaseHeader;
    use crate::tree_store::page_store::layout::{DatabaseLayout, RegionLayout};
    use std::io;
    use std::sync::Arc;

    const PAGE_SIZE: u32 = 4096;
    const SAVED_PAGES: u32 = 5120;
    const GROWN_PAGES: u32 = 5400;

    fn layout(pages: u32) -> DatabaseLayout {
        DatabaseLayout::new(
            0,
            RegionLayout::new(MAX_PAGE_INDEX + 1, 1, PAGE_SIZE),
            Some(RegionLayout::new(pages, 1, PAGE_SIZE)),
        )
    }

    fn paged_state(pages: u32) -> InMemoryState {
        let scratch = crate::scratch_backend_in_directory(&std::env::temp_dir()).unwrap();
        InMemoryState::new(
            DatabaseHeader::new(layout(pages), TransactionId::new(0)),
            Some(scratch),
            0,
        )
        .unwrap()
    }

    fn u32_at(bytes: &[u8], at: usize) -> usize {
        u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
    }

    /// One saved bitmap layer window, cut from the encoded value the way
    /// `SavedAllocatorValue::bitmap_window` cuts it from the allocator table.
    fn window(bytes: &[u8], start: usize, height: u8, page: u32, out: &mut [u8; 4096]) {
        let count = u32_at(bytes, start);
        if usize::from(height) >= count {
            return;
        }
        let layer_start = if height == 0 {
            4 + count * 4
        } else {
            u32_at(bytes, start + 4 + (usize::from(height) - 1) * 4)
        };
        let layer_end = u32_at(bytes, start + 4 + usize::from(height) * 4);
        let words_start = start + layer_start + 4;
        let words_len = layer_end - layer_start - 4;
        let offset = page as usize * out.len();
        if offset < words_len {
            let len = (words_len - offset).min(out.len());
            out[..len].copy_from_slice(&bytes[words_start + offset..words_start + offset + len]);
        }
    }

    /// One region's saved value header, `(maximum order, saved page count)`,
    /// read from the encoded value as `SavedAllocatorValue::header` reads
    /// it from the allocator table.
    fn region_header(bytes: &[u8]) -> (u8, u32) {
        (
            bytes[0],
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        )
    }

    fn region_window(bytes: &[u8], order: u8, height: u8, page: u32, out: &mut [u8; 4096]) {
        let max_order = bytes[0];
        if order > max_order {
            return;
        }
        let start = if order == 0 {
            8 + (usize::from(max_order) + 1) * 4
        } else {
            u32_at(bytes, 8 + (usize::from(order) - 1) * 4)
        };
        window(bytes, start, height, page, out);
    }

    fn tracker_window(bytes: &[u8], order: u8, height: u8, page: u32, out: &mut [u8; 4096]) {
        let orders = u32_at(bytes, 0);
        if usize::from(order) >= orders {
            return;
        }
        let mut start = 4 + orders * 4;
        for earlier in 0..usize::from(order) {
            start += u32_at(bytes, 4 + earlier * 4);
        }
        window(bytes, start, height, page, out);
    }

    /// The saved allocator values a quick-repair commit writes: region 0 and
    /// the region tracker, encoded at the geometry they were saved under.
    struct Saved {
        region: Arc<Vec<u8>>,
        tracker: Arc<Vec<u8>>,
    }

    fn save(state: &mut InMemoryState) -> Saved {
        let region = state.allocators.get_region(0);
        let mut region_bytes = vec![0; region.encoded_len()];
        region.encode_into(&mut region_bytes);
        let mut tracker_bytes = vec![0; state.allocators.region_tracker.encoded_len()];
        state
            .allocators
            .region_tracker
            .encode_into(&mut tracker_bytes);
        Saved {
            region: Arc::new(region_bytes),
            tracker: Arc::new(tracker_bytes),
        }
    }

    /// The commit's trim to `cut`, then the next open installing `saved`.
    fn trim_then_install(state: &mut InMemoryState, saved: &Saved, cut: u32) {
        state.allocators.resize_to(layout(cut)).unwrap();
        let (region, tracker) = (Arc::clone(&saved.region), Arc::clone(&saved.tracker));
        state
            .allocators
            .set_saved_source(
                Arc::new(move |which, order, height, page, out| -> io::Result<bool> {
                    out.fill(0xFF);
                    match which {
                        Some(0) => region_window(&region, order, height, page, out),
                        Some(_) => return Ok(false),
                        None => tracker_window(&tracker, order, height, page, out),
                    }
                    Ok(true)
                }),
                {
                    let region = Arc::clone(&saved.region);
                    Arc::new(move |which| Ok((which == 0).then(|| region_header(&region))))
                },
            )
            .unwrap();
    }

    fn exact(state: &mut InMemoryState, context: &str) {
        let mismatches = state.allocators.get_region(0).summary_mismatches_for_test();
        assert!(
            mismatches.is_empty(),
            "{context}: region 0 summary bits disagree with their words at (order, height, entry) {mismatches:?}"
        );
    }

    /// Allocate every page of a fresh `SAVED_PAGES` region, then free `free`.
    fn region_with_free_pages(free: impl IntoIterator<Item = u32>) -> InMemoryState {
        let mut state = paged_state(SAVED_PAGES);
        for _ in 0..SAVED_PAGES {
            state.allocators.get_region_mut(0).alloc_lowest(0).unwrap();
        }
        for page in free {
            state.allocators.get_region_mut(0).free(page, 0);
        }
        state
    }

    /// The install alone, before any growth or other resize, must be exact,
    /// count every free page below the cut and allocate from them. The free
    /// tail merges into one 16-page block (pages 5104-5119, order-4 bit 319)
    /// and the trim cuts it in half at 5112, as `try_shrink` does for a
    /// `trailing_free` of 16. Masking that block at the shorter geometry loses
    /// pages 5104-5111 at every order.
    #[test]
    fn install_under_a_shorter_geometry_is_exact_counts_every_free_page_and_allocates() {
        let lower = [10, 11, 2000];
        let mut state = region_with_free_pages(lower.into_iter().chain(5104..SAVED_PAGES));
        exact(&mut state, "before the save");
        let saved = save(&mut state);
        trim_then_install(&mut state, &saved, 5112);
        exact(
            &mut state,
            "right after installing the saved state under 5112 pages",
        );
        let expected = u32::try_from(lower.len()).unwrap() + (5112 - 5104);
        assert_eq!(
            state.allocators.get_region(0).count_free_pages(),
            expected,
            "the install lost free pages below the cut"
        );
        let mut allocated = Vec::new();
        for _ in 0..expected {
            let region = state.allocators.get_region_mut(0);
            match region.alloc_lowest(0) {
                Some(page) => allocated.push(page),
                None => panic!(
                    "{} counted-free pages are not allocatable after {} allocations (highest free order {:?})",
                    region.count_free_pages(),
                    allocated.len(),
                    region.highest_free_order()
                ),
            }
        }
        allocated.sort_unstable();
        assert_eq!(
            allocated,
            [10, 11, 2000, 5104, 5105, 5106, 5107, 5108, 5109, 5110, 5111]
        );
        assert_eq!(state.allocators.get_region(0).count_free_pages(), 0);
    }

    /// One fragmented region: every page allocated, one page in five below
    /// `cut` freed (deterministic per seed), the tail `[cut, SAVED_PAGES)` free;
    /// saved, trimmed to `cut`, installed, grown, then allocated one page at a
    /// time. Returns the counted-free pages left unallocatable (0 = healthy).
    fn stranded_after_install_and_growth(seed: u64, cut: u32) -> u32 {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut free = Vec::new();
        for page in 0..cut {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if x.is_multiple_of(5) {
                free.push(page);
            }
        }
        let mut state = region_with_free_pages(free.into_iter().chain(cut..SAVED_PAGES));
        let saved = save(&mut state);
        trim_then_install(&mut state, &saved, cut);
        state.allocators.resize_to(layout(GROWN_PAGES)).unwrap();
        loop {
            let region = state.allocators.get_region_mut(0);
            let free = region.count_free_pages();
            if free == 0 {
                return 0;
            }
            if region.alloc_lowest(0).is_none() {
                return free;
            }
        }
    }

    /// Two geometries a masking install strands: after the install and a growth,
    /// 144 counted-free pages there cannot be allocated.
    #[test]
    fn saved_allocator_reloaded_under_shorter_geometry_keeps_free_space_findable() {
        for cut in [5105, 5106] {
            assert_eq!(
                stranded_after_install_and_growth(1, cut),
                0,
                "seed 1, cut {cut}: counted-free pages are unallocatable after install and growth"
            );
        }
    }

    /// The same property over 120 geometries: seeds 1-2, every cut
    /// 5060-5119 (60 cuts in the last order-6 block, which holds 5105-5106).
    #[test]
    fn no_install_geometry_strands_counted_free_pages() {
        const SWEEP_CUTS: std::ops::Range<u32> = 5060..SAVED_PAGES;
        let mut stranded = Vec::new();
        for seed in 1..=2u64 {
            for cut in SWEEP_CUTS {
                let pages = stranded_after_install_and_growth(seed, cut);
                if pages > 0 {
                    stranded.push((seed, cut, pages));
                }
            }
        }
        assert!(
            stranded.is_empty(),
            "{} of 120 geometries strand counted-free pages, (seed, cut, pages): {:?}",
            stranded.len(),
            &stranded[..stranded.len().min(10)]
        );
    }

    /// Every length transition keeps every regional summary exact: the
    /// commit's own trim, growth, the non-paged decode plus resize, the paged
    /// install, materialising the installed source, and spilling the repaired
    /// words to scratch and reading them back.
    #[test]
    fn every_length_transition_keeps_regional_summaries_exact() {
        let mut state = region_with_free_pages([10, 11, 2000].into_iter().chain(5104..SAVED_PAGES));
        let saved = save(&mut state);

        // The non-paged load: decode at the saved length, then resize.
        let mut decoded = BuddyAllocator::from_bytes(&saved.region);
        decoded.resize(5112);
        assert!(
            decoded.summary_mismatches_for_test().is_empty(),
            "non-paged decode + resize: {:?}",
            decoded.summary_mismatches_for_test()
        );

        // The commit's trim, in process.
        state.allocators.resize_to(layout(5112)).unwrap();
        exact(&mut state, "after the in-process trim to 5112");
        state.allocators.resize_to(layout(5300)).unwrap();
        exact(&mut state, "after growing back to 5300");

        // The next open: install under the shorter geometry.
        let mut state = region_with_free_pages([10, 11, 2000].into_iter().chain(5104..SAVED_PAGES));
        let saved = save(&mut state);
        trim_then_install(&mut state, &saved, 5112);
        exact(&mut state, "after the install under 5112");

        // Spill everything to scratch at the minimum window budget, then
        // read every word back.
        state.allocators.set_window_budget(0).unwrap();
        exact(
            &mut state,
            "after spilling the installed words and reading them back",
        );

        // Materialise the installed source and drop it.
        state.allocators.materialize_saved_source().unwrap();
        assert!(!state.allocators.has_saved_source());
        exact(
            &mut state,
            "after materialising and detaching the saved source",
        );
        state.allocators.resize_to(layout(5300)).unwrap();
        exact(&mut state, "after growth on the materialised allocator");
    }

    /// A measurement, not a gate: allocator window loads and word reads, at
    /// the minimum window budget, of a fragmented full region's trim to a
    /// 32-page-shorter length, of installing its saved state under that
    /// length, and of the first full free count over the installed view,
    /// each in its own interval. Run with `--ignored --nocapture`.
    #[test]
    #[ignore = "measurement"]
    fn measure_trim_and_install_paging() {
        let pages = (MAX_PAGE_INDEX + 1).min(1 << 20);
        let scratch = crate::scratch_backend_in_directory(&std::env::temp_dir()).unwrap();
        let mut state = InMemoryState::new(
            DatabaseHeader::new(layout(pages), TransactionId::new(0)),
            Some(scratch),
            0,
        )
        .unwrap();
        for _ in 0..pages {
            state.allocators.get_region_mut(0).alloc_lowest(0).unwrap();
        }
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for page in 0..pages - 64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if x.is_multiple_of(7) {
                state.allocators.get_region_mut(0).free(page, 0);
            }
        }
        for page in pages - 64..pages {
            state.allocators.get_region_mut(0).free(page, 0);
        }
        let saved = save(&mut state);
        state.allocators.set_window_budget(0).unwrap();
        let counters =
            |state: &InMemoryState| state.allocators.pager().unwrap().counters_for_test();
        let delta = |a: (u64, u64, u64), b: (u64, u64, u64)| (b.0 - a.0, b.1 - a.1, b.2 - a.2);
        let before = counters(&state);
        state.allocators.resize_to(layout(pages - 32)).unwrap();
        let after_trim = counters(&state);
        let (region, tracker) = (Arc::clone(&saved.region), Arc::clone(&saved.tracker));
        state
            .allocators
            .set_saved_source(
                Arc::new(move |which, order, height, page, out| -> io::Result<bool> {
                    out.fill(0xFF);
                    match which {
                        Some(0) => region_window(&region, order, height, page, out),
                        Some(_) => return Ok(false),
                        None => tracker_window(&tracker, order, height, page, out),
                    }
                    Ok(true)
                }),
                {
                    let region = Arc::clone(&saved.region);
                    Arc::new(move |which| Ok((which == 0).then(|| region_header(&region))))
                },
            )
            .unwrap();
        let after_install = counters(&state);
        let _ = state.allocators.get_region(0).count_free_pages();
        let after_view = counters(&state);
        println!(
            "ALLOCATOR-PAGING pages={pages} encoded_bytes={} (word_reads, window_loads, scratch_reads): trim={:?} install={:?} first_full_count={:?}",
            saved.region.len(),
            delta(before, after_trim),
            delta(after_trim, after_install),
            delta(after_install, after_view),
        );
    }
}
