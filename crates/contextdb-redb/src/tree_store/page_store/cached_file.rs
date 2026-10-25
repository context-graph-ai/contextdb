use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::header::DB_HEADER_SIZE;
use crate::tree_store::page_store::lru_cache::{EntryArena, LRUCache, WriteCache};
use crate::{CacheStats, DatabaseError, Result, StorageError};
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock};
use super::page_allocation::{CacheShared, cache_array_prepaid};

use super::page_allocation::{CacheCounter, CachePayment, PageAllocation};

/// Every alias retains the same complete physical backing and its payer.
#[derive(Clone)]
pub(crate) enum PageMemory {
    Cached(PageAllocation),
    Charged(PageAllocation),
    // Dirty aliases retain the writer payment. Legacy unscoped reads remain
    // unqualified; the variant itself is never proof of external funding.
    Plain(PageAllocation),
    #[cfg(feature = "contextdb-internal")]
    Native(PageAllocation),
}

// Private representation names retained for source-adoption diagnostics.
#[allow(dead_code)]
type CachedPage = PageAllocation;
#[allow(dead_code)]
type ChargedPage = PageAllocation;

fn cached_page_bytes(len: usize) -> usize {
    PageAllocation::quote(len)
}

fn admitted_page_bytes(len: usize) -> usize {
    // Preserve the pre-existing directory/window allowance independently of
    // the complete concrete allocation, including alignment and checksum.
    cached_page_bytes(len).saturating_add(128)
}

#[cfg(test)]
thread_local! {
    // Computations for Cached/Charged/Plain, followed by Cached/Charged hits.
    static BYTE_CHECKSUM_COUNTS: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
}

impl PageMemory {
    #[cfg(all(test, feature = "contextdb-internal"))]
    pub(crate) fn admitted_bytes_for_test(len: usize) -> usize {
        admitted_page_bytes(len)
    }

    #[cfg(test)]
    pub(crate) fn byte_checksum_counts_for_test() -> [usize; 5] {
        BYTE_CHECKSUM_COUNTS.with(std::cell::Cell::get)
    }

    /// Cache only the computation for variable-width byte nodes. The caller
    /// must validate their layout first and compare its expected checksum on
    /// every visit. This slot grants no authentication or root authority.
    #[cfg(any(test, feature = "contextdb-internal"))]
    pub(crate) fn memoized_byte_node_checksum(
        &self,
        compute: impl FnOnce() -> Result<u128>,
    ) -> Result<u128> {
        let slot = match self {
            Self::Cached(page) => page.checksum(),
            Self::Charged(page) => page.checksum(),
            #[cfg(feature = "contextdb-internal")]
            Self::Native(page) => page.checksum(),
            Self::Plain(_) => {
                #[cfg(test)]
                BYTE_CHECKSUM_COUNTS.with(|counts| {
                    let mut value = counts.get();
                    value[2] += 1;
                    counts.set(value);
                });
                return compute();
            }
        };
        // Only immutable bytes are read by compute; no cache, admission, or
        // page-manager callback runs under this object-local mutex.
        let mut stored = slot.lock().unwrap();
        if let Some(checksum) = *stored {
            #[cfg(test)]
            BYTE_CHECKSUM_COUNTS.with(|counts| {
                let mut value = counts.get();
                value[if matches!(self, Self::Cached(_)) {
                    3
                } else {
                    4
                }] += 1;
                counts.set(value);
            });
            return Ok(checksum);
        }
        #[cfg(test)]
        BYTE_CHECKSUM_COUNTS.with(|counts| {
            let mut value = counts.get();
            value[usize::from(!matches!(self, Self::Cached(_)))] += 1;
            counts.set(value);
        });
        let checksum = compute()?;
        *stored = Some(checksum);
        Ok(checksum)
    }
}

impl std::fmt::Debug for PageMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, len) = match self {
            PageMemory::Cached(page) => ("cached", page.len()),
            PageMemory::Charged(page) => ("charged", page.len()),
            PageMemory::Plain(bytes) => ("plain", bytes.len()),
            #[cfg(feature = "contextdb-internal")]
            PageMemory::Native(page) => ("native", page.len()),
        };
        write!(f, "PageMemory({kind}, {len} bytes)")
    }
}

impl std::ops::Deref for PageMemory {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            PageMemory::Cached(page) => page,
            PageMemory::Charged(page) => page,
            PageMemory::Plain(bytes) => bytes,
            #[cfg(feature = "contextdb-internal")]
            PageMemory::Native(page) => page,
        }
    }
}

pub(super) struct WritablePage {
    buffer: CacheShared<Mutex<LRUWriteCache>>,
    offset: u64,
    data: PageAllocation,
}

impl WritablePage {
    pub(super) fn mem(&self) -> &[u8] {
        &self.data
    }

    pub(super) fn mem_mut(&mut self) -> &mut [u8] {
        PageAllocation::get_mut(&mut self.data).unwrap()
    }
}

impl Drop for WritablePage {
    fn drop(&mut self) {
        self.buffer
            .lock()
            .unwrap()
            .return_value(self.offset, self.data.clone());
    }
}

impl<I: SliceIndex<[u8]>> Index<I> for WritablePage {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.mem().index(index)
    }
}

impl<I: SliceIndex<[u8]>> IndexMut<I> for WritablePage {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.mem_mut().index_mut(index)
    }
}

#[derive(Default)]
struct LRUWriteCache {
    cache: WriteCache<Option<PageAllocation>>,
}

impl LRUWriteCache {
    fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }

    fn insert(&mut self, key: u64, value: PageAllocation) {
        assert!(self.cache.insert(key, Some(value)).is_none());
    }

    fn get(&self, key: u64) -> Option<&PageAllocation> {
        self.cache.get(key).map(|x| x.as_ref().unwrap())
    }

    fn remove(&mut self, key: u64) -> Option<PageAllocation> {
        if let Some(value) = self.cache.remove(key) {
            assert!(value.is_some());
            return value;
        }
        None
    }

    fn return_value(&mut self, key: u64, value: PageAllocation) {
        assert!(self.cache.get_mut(key).unwrap().replace(value).is_none());
    }

    fn take_value(&mut self, key: u64) -> Option<PageAllocation> {
        if let Some(value) = self.cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        None
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, PageAllocation)> {
        for _ in 0..self.cache.len() {
            if let Some((k, v)) = self.cache.pop_lowest_priority() {
                if let Some(v_inner) = v {
                    return Some((k, v_inner));
                }

                // Value is borrowed by take_value(). We can't evict it, so put it back.
                self.cache.insert(k, v);
            } else {
                break;
            }
        }
        None
    }

    fn clear(&mut self) {
        self.cache.clear();
    }
}

#[derive(Debug)]
struct CheckedBackend {
    file: crate::db::Backend,
    io_failed: AtomicBool,
    closed: AtomicBool,
}

impl CheckedBackend {
    fn new(file: impl Into<crate::db::Backend>) -> Self {
        Self {
            file: file.into(),
            io_failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn check_failure(&self) -> Result<()> {
        if self.io_failed.load(Ordering::Acquire) {
            if self.closed.load(Ordering::Acquire) {
                Err(StorageError::DatabaseClosed)
            } else {
                Err(StorageError::PreviousIo)
            }
        } else {
            Ok(())
        }
    }

    fn close(&self) -> Result {
        self.closed.store(true, Ordering::Release);
        self.io_failed.store(true, Ordering::Release);
        self.file.close()?;

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        self.check_failure()?;
        let result = self.file.len();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.read(offset, out);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        } else {
            crate::read_admission::record_backend_read(out.len());
        }
        result
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.check_failure()?;
        let result = self.file.set_len(len);
        // A caller-enforced quota is an admission refusal, not failed I/O.
        // The allocator has not published the projected layout yet, so the
        // same handle remains usable after the caller releases capacity.
        if result
            .as_ref()
            .is_err_and(|error| !crate::db::backend_quota_refused(error))
        {
            self.io_failed.store(true, Ordering::Release);
        }
        result
    }

    fn sync_data(&self) -> Result<()> {
        self.check_failure()?;
        let result = self.file.sync_data();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.write(offset, data);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result
    }
}

pub(super) struct PagedCachedFile {
    no_victim_generation: AtomicU64,
    #[cfg(test)]
    eviction_visits: AtomicUsize,
    file: CheckedBackend,
    page_size: u64,
    // Dynamic cache partitioning.  Three invariants:
    //
    // 1. The write buffer NEVER exceeds 50% of max_cache_size.
    //    Pages beyond this limit are flushed to disk immediately.
    // 2. The write buffer evicts from the read cache only when
    //    write < 50% AND read > 50% (fairness).
    // 3. write + read never exceeds max_cache_size.
    //
    // Together these guarantee that the read cache can grow up to 100% when no
    // writes are in progress, while write-heavy workloads never starve readers
    // below 50%.
    //
    // We track usage with two atomic counters and compute the total on the fly.
    // The resulting read is not perfectly atomic (between loading the two
    // counters a concurrent operation could change one), but the budget is a
    // soft limit and momentary over-/under-counting by one page is harmless.
    // A third "total" counter would add contention on every insert/remove for
    // negligible accuracy gain.
    max_cache_size: AtomicUsize,
    // Rotates the starting stripe for read-cache eviction
    next_eviction_stripe: AtomicUsize,
    #[cfg(feature = "cache_metrics")]
    reads_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    reads_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    evictions: AtomicU64,
    read_cache: Box<[RwLock<LRUCache<PageAllocation>>]>,
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: CacheShared<Mutex<LRUWriteCache>>,
    // Bytes of every page admitted to the read cache that is still alive: in
    // the cache, or evicted while a reader still holds it. A page leaves this
    // count only after its complete allocation has been physically freed.
    // Explicit invalidation drops a map handle, never the live backing's payer.
    // Last fields: the genuine parent must outlive physical index/map/control
    // frees. Detached page and WritablePage aliases retain these same controls.
    read_cache_bytes: CacheCounter,
    write_buffer_bytes: CacheCounter,
}

// Opaque aliases expose only real backing identity/readability to engine tests.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone)]
pub struct CachePageForTest(PageMemory);
#[cfg(feature = "test-seams")]
impl CachePageForTest {
    pub fn bytes(&self) -> &[u8] { &self.0 }
    pub fn payment(&self) -> (&'static str, usize, usize) {
        match &self.0 {
            PageMemory::Cached(p) | PageMemory::Plain(p) | PageMemory::Charged(p) =>
                p.payment_for_test(),
            #[cfg(feature = "contextdb-internal")]
            PageMemory::Native(_) => panic!("expected real legacy cache allocation"),
        }
    }
    pub fn allocation(&self) -> (usize, usize, usize) {
        let page = match &self.0 {
            PageMemory::Cached(p) | PageMemory::Plain(p) | PageMemory::Charged(p) => p,
            #[cfg(feature = "contextdb-internal")]
            PageMemory::Native(_) => panic!("expected real legacy cache allocation"),
        };
        let layout = PageAllocation::layout(page.len()).unwrap().0;
        (PageAllocation::as_ptr(page) as usize, layout.size(), layout.align())
    }
}
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct CacheWriteForTest {
    _buffer: CacheShared<Mutex<LRUWriteCache>>,
    data: PageAllocation,
}
#[cfg(feature = "test-seams")]
impl CacheWriteForTest {
    pub fn bytes(&self) -> &[u8] { &self.data }
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone)]
pub struct CacheWriteControlForTest(CacheShared<Mutex<LRUWriteCache>>);
#[cfg(feature = "test-seams")]
impl CacheWriteControlForTest {
    pub fn allocation(&self) -> (usize, usize, usize) { self.0.allocation_for_test() }
}

impl PagedCachedFile {
    #[cfg(feature = "test-seams")]
    pub(crate) fn write_control_for_test(&self) -> CacheWriteControlForTest {
        CacheWriteControlForTest(self.write_buffer.clone())
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn cached_header_for_test(&self) -> Result<CachePageForTest> {
        let page = self.read(0, DB_HEADER_SIZE, PageHint::Clean)?;
        assert!(matches!(page, PageMemory::Cached(_)));
        Ok(CachePageForTest(page))
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn allocations_for_test(&self, visit: &mut dyn FnMut((usize, usize, usize))) -> usize {
        visit(self.read_cache_bytes.allocation_for_test());
        visit(self.write_buffer_bytes.allocation_for_test());
        visit(self.write_buffer.allocation_for_test());
        let layout = std::alloc::Layout::for_value(self.read_cache.as_ref());
        visit((self.read_cache.as_ptr() as usize, layout.size(), layout.align()));
        for (index, stripe) in self.read_cache.iter().enumerate() {
            stripe.read().unwrap().allocations_for_test(index == 0, visit);
        }
        self.read_cache_bytes.preparation_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn controls_for_test(&self) -> [(usize, usize); 2] {
        [self.read_cache_bytes.observation_for_test(), self.write_buffer_bytes.observation_for_test()]
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn lifetime_aliases_for_test(&self) -> Result<(CachePageForTest, CachePageForTest, CacheWriteForTest)> {
        // Use the authenticated header bytes unchanged; this is the actual
        // read -> COW write -> dirty alias path, without a logical mutation.
        let cached = self.read(0, DB_HEADER_SIZE, PageHint::Clean)?;
        assert!(matches!(cached, PageMemory::Cached(_)));
        let writable = self.write(0, DB_HEADER_SIZE, false)?;
        let control = CacheWriteForTest { _buffer: writable.buffer.clone(), data: writable.data.clone() };
        assert!(!PageAllocation::ptr_eq(match &cached { PageMemory::Cached(p) => p, _ => unreachable!() }, &writable.data));
        // Return the checked-out write before close/flush. The test retains the
        // SAME genuine map Arc and data, never an unreturned None placeholder.
        drop(writable);
        let dirty = self.read(0, DB_HEADER_SIZE, PageHint::None)?;
        assert!(matches!(dirty, PageMemory::Plain(_)));
        assert_eq!(&cached[..], &dirty[..]);
        Ok((CachePageForTest(cached), CachePageForTest(dirty), control))
    }

    pub(super) fn new(
        file: impl Into<crate::db::Backend>,
        page_size: u64,
        max_cache_size: usize,
    ) -> Result<Self, DatabaseError> {
        let (read_cache_bytes, write_buffer_bytes) = super::page_allocation::cache_counters()?;
        crate::read_admission::prepare_cache_allocation(Self::write_buffer_control_layout())?;
        let read_cache =
            Self::prepare_read_index(max_cache_size, usize::try_from(page_size).unwrap(), 0)?;

        Ok(Self {
            file: CheckedBackend::new(file),
            no_victim_generation: AtomicU64::new(0),
            #[cfg(test)]
            eviction_visits: AtomicUsize::new(0),
            page_size,
            read_cache_bytes,
            write_buffer_bytes,
            max_cache_size: AtomicUsize::new(max_cache_size),
            next_eviction_stripe: AtomicUsize::new(0),
            #[cfg(feature = "cache_metrics")]
            reads_total: Default::default(),
            #[cfg(feature = "cache_metrics")]
            reads_hits: Default::default(),
            #[cfg(feature = "cache_metrics")]
            writes_total: Default::default(),
            #[cfg(feature = "cache_metrics")]
            writes_hits: Default::default(),
            #[cfg(feature = "cache_metrics")]
            evictions: Default::default(),
            read_cache,
            write_buffer: CacheShared::new_prepaid(Mutex::new(LRUWriteCache::new()))?,
        })
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn retain_cache_reserve(
        &self,
        reserve: crate::native_storage_admission::NativeStorageCredit,
    ) -> super::page_allocation::CacheReserveOwner {
        super::page_allocation::CacheReserveOwner::install(
            &self.read_cache_bytes,
            &self.write_buffer_bytes,
            reserve,
        )
    }

    fn write_buffer_control_layout() -> std::alloc::Layout {
        CacheShared::<Mutex<LRUWriteCache>>::layout()
    }

    fn read_index_entries(bytes: usize, page_size: usize, held_entries: usize) -> usize {
        // Only the database header can be smaller than a full page, and its
        // offset-zero key occupies at most one slot across all stripes.
        (bytes / cached_page_bytes(page_size))
            .saturating_add(1)
            .max(held_entries)
    }

    fn prepare_read_index(
        bytes: usize,
        page_size: usize,
        held_entries: usize,
    ) -> Result<Box<[RwLock<LRUCache<PageAllocation>>]>> {
        use std::alloc::Layout;
        let entries = Self::read_index_entries(bytes, page_size, held_entries);
        let stripes = usize::try_from(Self::lock_stripes()).unwrap();
        let heads = entries.div_ceil(stripes).next_power_of_two();
        // Every complete replacement is paid while the old index remains
        // physically live. This callback is never a source-page callback.
        for layout in EntryArena::<PageAllocation>::allocation_layouts(entries) {
            crate::read_admission::prepare_cache_allocation(layout)?;
        }
        crate::read_admission::prepare_cache_allocation(
            Layout::array::<RwLock<LRUCache<PageAllocation>>>(stripes)
                .map_err(|_| StorageError::ValueTooLarge(stripes))?,
        )?;
        let layout = Layout::array::<u32>(heads).map_err(|_| StorageError::ValueTooLarge(heads))?;
        for _ in 0..stripes {
            crate::read_admission::prepare_cache_allocation(layout)?;
        }
        Self::try_new_read_index(bytes, page_size, held_entries)
    }

    #[cfg(test)]
    fn new_read_index(bytes: usize, page_size: usize, held_entries: usize)
        -> Box<[RwLock<LRUCache<PageAllocation>>]>
    {
        Self::try_new_read_index(bytes, page_size, held_entries)
            .expect("test cache index allocation")
    }

    fn try_new_read_index(
        bytes: usize,
        page_size: usize,
        held_entries: usize,
    ) -> Result<Box<[RwLock<LRUCache<PageAllocation>>]>> {
        let entries = Self::read_index_entries(bytes, page_size, held_entries);
        let arena = EntryArena::try_new(entries)?;
        let heads = entries
            .div_ceil(usize::try_from(Self::lock_stripes()).unwrap())
            .next_power_of_two();
        cache_array_prepaid(usize::try_from(Self::lock_stripes()).unwrap(), |_| {
            LRUCache::try_new(arena.clone(), heads).map(RwLock::new)
        })
    }

    fn uncached(buffer: PageAllocation) -> PageMemory {
        if buffer.is_charged() {
            PageMemory::Charged(buffer)
        } else {
            PageMemory::Plain(buffer)
        }
    }

    /// The constant per-stripe lock controls inside `index_bytes`: fixed by
    /// the stripe count, independent of the configured cache size.
    pub(crate) fn stripe_control_bytes(&self) -> usize {
        self.read_cache.len() * size_of::<RwLock<LRUCache<PageAllocation>>>()
    }

    pub(crate) fn index_bytes(&self) -> usize {
        let read = self.read_cache[0].read().unwrap();
        2 * CacheCounter::layout().size()
            + Self::write_buffer_control_layout().size()
            + read.arena_index_bytes()
            + read.index_bytes() * self.read_cache.len()
            + self.read_cache.len() * size_of::<RwLock<LRUCache<PageAllocation>>>()
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn cache_stats(&self) -> CacheStats {
        #[cfg(not(feature = "cache_metrics"))]
        {
            CacheStats {
                evictions: 0,
                read_hits: 0,
                read_misses: 0,
                write_hits: 0,
                write_misses: 0,
                used_bytes: 0,
            }
        }

        #[cfg(feature = "cache_metrics")]
        {
            let read_hits = self.reads_hits.load(Ordering::Acquire);
            let read_total = self.reads_total.load(Ordering::Acquire);
            let write_hits = self.writes_hits.load(Ordering::Acquire);
            let write_total = self.writes_total.load(Ordering::Acquire);
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            CacheStats {
                evictions: self.evictions.load(Ordering::Acquire),
                read_hits,
                read_misses: read_total - read_hits,
                write_hits,
                write_misses: write_total - write_hits,
                used_bytes: read_bytes + write_bytes,
            }
        }
    }

    /// Settle the writer's share during its quiescent startup, before source
    /// roots are published. Eviction releases cache ownership only: any page
    /// held elsewhere keeps its original bytes and remains in the live count.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn settle_startup_cache_budget(&self, bytes: usize) -> Result<usize> {
        if self.max_cache_size.load(Ordering::Acquire) != bytes {
            self.flush_write_buffer()?;
        }
        let write = self.write_buffer.lock().unwrap();
        self.max_cache_size.store(bytes, Ordering::Release);
        let retained = self.read_cache_bytes.load(Ordering::Acquire);
        self.evict_from_read_cache(retained.saturating_sub(bytes), &write);
        crate::read_admission::prepare_cache_allocation(
            std::alloc::Layout::array::<std::sync::RwLockWriteGuard<'_, LRUCache<PageAllocation>>>(
                self.read_cache.len(),
            )
            .map_err(|_| StorageError::ValueTooLarge(self.read_cache.len()))?,
        )?;
        let mut locks = self
            .read_cache
            .iter()
            .map(|stripe| stripe.write().unwrap())
            .collect::<Vec<_>>();
        let held_entries = locks.iter().map(|stripe| stripe.len()).sum();
        let required_entries = Self::read_index_entries(
            bytes,
            usize::try_from(self.page_size).expect("page size fits usize"),
            held_entries,
        );
        if locks[0].arena_capacity() != required_entries {
            let next = Self::prepare_read_index(
                bytes,
                usize::try_from(self.page_size).expect("page size fits usize"),
                held_entries,
            )?;
            for (old, replacement) in locks.iter_mut().zip(next.iter()) {
                let mut replacement = replacement.write().unwrap();
                old.transfer_to(&mut replacement);
                std::mem::swap(&mut **old, &mut *replacement);
            }
        }
        drop(locks);
        drop(write);
        Ok(bytes.max(
            self.read_cache_bytes.load(Ordering::Acquire)
                + self.write_buffer_bytes.load(Ordering::Acquire),
        ) + self.index_bytes())
    }

    pub(crate) fn close(&self) -> Result {
        self.file.close()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.file.check_failure()
    }

    pub(crate) fn raw_file_len(&self) -> Result<u64> {
        self.file.len()
    }

    const fn lock_stripes() -> u64 {
        131
    }

    // Evict entries from the read cache to free at least `bytes_needed` bytes.
    // Iterates through cache stripes and pops lowest-priority entries.
    //
    // Caller must hold the write_buffer mutex to maintain the lock ordering
    // invariant (write_buffer lock is always acquired before read_cache locks).
    fn evict_from_read_cache(
        &self,
        bytes_needed: usize,
        _write_lock: &MutexGuard<'_, LRUWriteCache>,
    ) {
        let generation = self.read_cache_bytes.release_generation();
        if self.no_victim_generation.load(Ordering::Acquire) == generation { return; }
        let num_stripes = self.read_cache.len();
        let start = self.next_eviction_stripe.load(Ordering::Relaxed) % num_stripes;
        let mut freed = 0;
        for i in 0..num_stripes {
            if freed >= bytes_needed {
                break;
            }
            let stripe = (start + i) % num_stripes;
            self.next_eviction_stripe.store((stripe + 1) % num_stripes, Ordering::Relaxed);
            let mut lock = self.read_cache[stripe].write().unwrap();
            while freed < bytes_needed {
                if let Some((_, v)) =
                    lock.pop_lowest_priority_where(|page| {
                        #[cfg(test)]
                        self.eviction_visits.fetch_add(1, Ordering::Relaxed);
                        PageAllocation::strong_count(page) == 1
                    })
                {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    // The stripe write lock prevents a new cache holder from
                    // appearing after victim selection. Pinned entries stay
                    // linked, so their later release cannot leave a cache hole.
                    freed += cached_page_bytes(v.len());
                } else {
                    break;
                }
            }
        }
        if freed < bytes_needed {
            self.no_victim_generation.store(generation, Ordering::Release);
        }
    }

    fn flush_write_buffer(&self) -> Result {
        let mut write_buffer = self.write_buffer.lock().unwrap();

        for (offset, buffer) in write_buffer.cache.iter() {
            self.file.write(*offset, buffer.as_ref().unwrap())?;
        }
        // Transfer flushed pages into the read cache so they are available
        // for subsequent reads without a file I/O.  The write buffer is being
        // drained, so the total check only considers the read cache size.
        for (offset, buffer) in write_buffer.cache.iter_mut() {
            let buffer = buffer.take().unwrap();
            let page_bytes = cached_page_bytes(buffer.len());
            let Ok((cache_size, payment)) =
                CachePayment::reserve(&self.read_cache_bytes, page_bytes)
            else {
                break;
            };
            if cache_size.saturating_add(page_bytes) <= self.max_cache_size.load(Ordering::Acquire)
            {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                debug_assert!(
                    buffer.len() >= usize::try_from(self.page_size).unwrap()
                        || (*offset == 0 && buffer.len() == DB_HEADER_SIZE)
                );
                // Reserve before replacing the original write/source payer.
                // A dirty Plain alias follows this same physical owner.
                match lock.try_insert(*offset, buffer.clone()) {
                    Ok(displaced) => {
                        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
                        let leaving_write = if buffer.counted_by(&self.write_buffer_bytes) {
                            page_bytes
                        } else {
                            0
                        };
                        let fixed_room = cache_size
                            .saturating_add(page_bytes)
                            .saturating_add(write_bytes.saturating_sub(leaving_write))
                            <= self.max_cache_size.load(Ordering::Acquire);
                        buffer.transfer_counted(payment, fixed_room);
                        drop(displaced);
                    }
                    Err(rejected) => {
                        drop(rejected);
                        drop(payment);
                    }
                }
            } else {
                drop(payment);
                break;
            }
        }
        // Clearing drops map owners only. Detached dirty aliases retain their
        // original payment until their actual backing is physically freed.
        write_buffer.clear();

        Ok(())
    }

    // Caller should invalidate all cached pages that are no longer valid
    pub(super) fn resize(&self, len: u64) -> Result {
        // Only a page that reaches past the new end can be stale; pages wholly
        // below it keep their bytes, so a truncating compaction step does not
        // discard (and then re-read) the whole working set.
        self.invalidate_cache_beyond(len);

        self.file.set_len(len)
    }

    fn invalidate_cache_beyond(&self, len: u64) {
        for stripe in &self.read_cache {
            let mut lock = stripe.write().unwrap();
            loop {
                let stale = lock
                    .iter()
                    .find(|(offset, page)| *offset + page.len() as u64 > len)
                    .map(|(offset, _)| offset);
                let Some(offset) = stale else {
                    break;
                };
                drop(lock.remove(offset));
            }
        }
    }

    pub(super) fn flush(&self) -> Result {
        self.flush_write_buffer()?;

        self.file.sync_data()
    }

    // Make writes visible to readers, but does not guarantee any durability
    pub(super) fn write_barrier(&self) -> Result {
        // TODO: non-durable commits would be much faster, if this did not issues writes to disk,
        // and instead just made the data visible to readers
        self.flush_write_buffer()
    }

    /// Retire only this failed request's cache misses. A preexisting hit or a
    /// page still held outside the cache keeps its ownership and backing.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn retire_read_cache_request(&self, request: u64) {
        for stripe in &self.read_cache {
            let mut cache = stripe.write().unwrap();
            while let Some((_, page)) = cache.pop_lowest_priority_where(|page|
                page.belongs_to_request(request) && PageAllocation::strong_count(page) == 1)
            {
                drop(page);
            }
        }
    }

    // Read directly from the file, ignoring any cached data
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.read(offset, &mut buffer)?;
        Ok(buffer)
    }

    // Admit the concrete complete allocation before allocation and backend I/O.
    fn read_direct_into_page(&self, offset: u64, len: usize) -> Result<PageAllocation> {
        PageAllocation::layout(len).map_err(|_| StorageError::ValueTooLarge(len))?;
        #[cfg(feature = "contextdb-internal")]
        if !crate::read_admission::has_read_memory_admission()
            && crate::native_storage_admission::native_storage_active()
        {
            let mut page = PageAllocation::zeroed_native(len).map_err(StorageError::Native)?;
            self.file.read(offset, PageAllocation::get_mut(&mut page).unwrap())?;
            return Ok(page);
        }
        let charge = crate::read_admission::admit_read_memory(admitted_page_bytes(len))?;
        let mut page = PageAllocation::zeroed(len, charge)?;
        self.file
            .read(offset, PageAllocation::get_mut(&mut page).unwrap())?;
        Ok(page)
    }

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    pub(super) fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<PageMemory> {
        debug_assert_eq!(0, offset % self.page_size);
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer.lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(PageMemory::Plain(cached.clone()));
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(PageMemory::Cached(cached.clone()));
            }
        }

        let buffer = self.read_direct_into_page(offset, len)?;
        let scan_replace = crate::read_admission::scan_reads_active()
            && !crate::read_admission::declared_scan_reads_active()
            && !crate::read_admission::take_scan_growth(cached_page_bytes(len));
        self.cache_loaded_page(offset, len, buffer, scan_replace)
    }

    fn cache_loaded_page(&self, offset: u64, len: usize, buffer: PageAllocation, scan_replace: bool) -> Result<PageMemory> {
        let page_bytes = cached_page_bytes(len);
        let cache_slot = usize::try_from(offset % Self::lock_stripes()).expect("stripe fits usize");
        if scan_replace {
            let before = self.read_cache_bytes.load(Ordering::Acquire);
            {
                let lock = self.write_buffer.lock().unwrap();
                self.evict_from_read_cache(page_bytes, &lock);
            }
            let after = self.read_cache_bytes.load(Ordering::Acquire);
            if before.saturating_sub(after) < page_bytes {
                return Ok(Self::uncached(buffer));
            }
        } else {
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            let cache_size = self.read_cache_bytes.load(Ordering::Acquire);
            if cache_size
                .saturating_add(page_bytes)
                .saturating_add(write_bytes)
                > self.max_cache_size.load(Ordering::Acquire)
            {
                let lock = self.write_buffer.lock().unwrap();
                self.evict_from_read_cache(
                    cache_size
                        .saturating_add(page_bytes)
                        .saturating_add(write_bytes)
                        - self.max_cache_size.load(Ordering::Acquire),
                    &lock,
                );
            }
        }
        let Ok((previous, payment)) = CachePayment::reserve(&self.read_cache_bytes, page_bytes)
        else {
            return Ok(Self::uncached(buffer));
        };
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        if previous
            .saturating_add(page_bytes)
            .saturating_add(write_bytes)
            > self.max_cache_size.load(Ordering::Acquire)
        {
            drop(payment);
            return Ok(Self::uncached(buffer));
        }
        debug_assert!(
            len >= usize::try_from(self.page_size).unwrap()
                || (offset == 0 && len == DB_HEADER_SIZE)
        );
        let mut write_lock = self.read_cache[cache_slot].write().unwrap();
        let inserted = if scan_replace {
            write_lock.try_insert_lowest_priority(offset, buffer.clone())
        } else {
            write_lock.try_insert(offset, buffer.clone())
        };
        match inserted {
            Ok(displaced) => {
                // All new holders retain this same source payer until transfer.
                // Drop the stripe guard before running the opaque receipt's
                // destructor; no byte/control allocation occurs at transfer.
                drop(write_lock);
                buffer.transfer(payment);
                drop(displaced);
                Ok(PageMemory::Cached(buffer))
            }
            Err(rejected) => {
                drop(rejected);
                drop(write_lock);
                drop(payment);
                Ok(Self::uncached(buffer))
            }
        }
    }

    // A reject-only window never enters the full-page cache. Its admission
    // remains owned until the window is released, including cached sources.
    pub(super) fn read_window(
        &self,
        offset: u64,
        extent_len: usize,
        range: std::ops::Range<usize>,
        hint: PageHint,
    ) -> Result<PageMemory> {
        if range.start > range.end || range.end > extent_len {
            return Err(StorageError::Corrupted(
                "page window exceeds allocation".into(),
            ));
        }
        PageAllocation::layout(range.len())
            .map_err(|_| StorageError::ValueTooLarge(range.len()))?;
        let charge = crate::read_admission::admit_read_memory(admitted_page_bytes(range.len()))?;
        let mut bytes = PageAllocation::zeroed(range.len(), charge)?;
        let output = PageAllocation::get_mut(&mut bytes).unwrap();
        let mut copied = false;
        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer.lock().unwrap();
            if let Some(source) = lock.get(offset) {
                output.copy_from_slice(&source[range.clone()]);
                copied = true;
            }
        }
        if !copied {
            let slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
            let lock = self.read_cache[slot].read().unwrap();
            if let Some(source) = lock.get(offset) {
                output.copy_from_slice(&source[range.clone()]);
                copied = true;
            }
        }
        if !copied {
            self.file.read(offset + range.start as u64, output)?;
        }
        Ok(Self::uncached(bytes))
    }

    // Discard pending writes to the given range
    pub(super) fn cancel_pending_write(&self, offset: u64, _len: usize) {
        assert_eq!(0, offset % self.page_size);
        drop(self.write_buffer.lock().unwrap().remove(offset));
    }

    // Invalidate any caching of the given range. After this call overlapping reads of the range are allowed
    //
    // NOTE: Invalidating a cached region in subsections is permitted, as long as all subsections are invalidated
    pub(super) fn invalidate_cache(&self, offset: u64, len: usize) {
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let mut lock = self.read_cache[cache_slot].write().unwrap();
        if let Some(removed) = lock.remove(offset) {
            assert_eq!(len, removed.len());
        }
    }

    /// Detach only the map entry whose physical allocation the guard retains.
    /// The coordinate comes from the real cache map, never a SQL row ordinal.
    #[cfg(feature = "test-seams")]
    pub(crate) fn detach_cached_memory_for_test(&self, memory: &PageMemory) -> (u64, usize, usize, bool) {
        let PageMemory::Cached(owner) = memory else { panic!("fixture must hold a Cached page") };
        let identity = PageAllocation::as_ptr(owner) as usize;
        let mut coordinate = None;
        for stripe in &self.read_cache {
            for (offset, page) in stripe.read().unwrap().iter() {
                if PageAllocation::ptr_eq(&*page, owner) {
                    assert!(coordinate.replace(offset).is_none(), "one physical cached identity");
                }
            }
        }
        let offset = coordinate.expect("held Cached identity must still be linked");
        let len = owner.len();
        self.invalidate_cache(offset, len);
        let absent = self.read_cache.iter().all(|stripe| stripe.read().unwrap().iter()
            .all(|(_, page)| !PageAllocation::ptr_eq(&*page, owner)));
        assert!(absent, "exact held identity must be absent after invalidation");
        (offset, len, identity, absent)
    }

    pub(super) fn invalidate_cache_all(&self) {
        for cache_slot in 0..self.read_cache.len() {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            while lock.pop_lowest_priority().is_some() {}
        }
    }

    /// Reserve the complete new backing while any old COW backing remains
    /// independently counted. Failed backend writes leave their map owner intact;
    /// the local payment rolls back this not-yet-allocated replacement.
    fn reserve_write_backing(
        &self,
        bytes: usize,
        lock: &mut MutexGuard<'_, LRUWriteCache>,
    ) -> Result<CachePayment> {
        let (previous, payment) = CachePayment::reserve(&self.write_buffer_bytes, bytes)?;
        let half = self.max_cache_size.load(Ordering::Acquire) / 2;
        let mut write_bytes = previous + bytes;
        if write_bytes > half {
            let excess = write_bytes - half;
            let mut flushed = 0;
            while flushed < excess {
                let Some((offset, buffer)) = lock.pop_lowest_priority() else {
                    break;
                };
                let removed_bytes = cached_page_bytes(buffer.len());
                if let Err(error) = self.file.write(offset, &buffer) {
                    lock.insert(offset, buffer);
                    return Err(error);
                }
                drop(buffer);
                #[cfg(feature = "cache_metrics")]
                self.evictions.fetch_add(1, Ordering::Relaxed);
                flushed += removed_bytes;
            }
            // A detached dirty reader can keep flushed backing paid. Use the
            // real counter, not the sum of entries removed from the map.
            write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        }
        let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
        if write_bytes.saturating_add(read_bytes) > self.max_cache_size.load(Ordering::Acquire) {
            self.evict_from_read_cache(
                write_bytes.saturating_add(read_bytes)
                    - self.max_cache_size.load(Ordering::Acquire),
                lock,
            );
        }
        Ok(payment)
    }

    // If overwrite is true, the page is initialized to zero.
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);
        // Reject an impossible complete Layout before reserving or unlinking.
        PageAllocation::layout(len).map_err(|_| StorageError::ValueTooLarge(len))?;
        let mut lock = self.write_buffer.lock().unwrap();
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let existing_read = self.read_cache[cache_slot].write().unwrap().remove(offset);
        if let Some(page) = &existing_read {
            assert_eq!(len, page.len(), "cache inconsistency for offset {offset}");
        }
        // Borrow/clone a dirty source without taking its map value. User
        // admission or backend unwind must not strand a None placeholder.
        let dirty = lock.get(offset).cloned();
        let was_dirty = dirty.is_some();
        let existing = dirty.or(existing_read);
        let is_hit = existing.is_some() || overwrite;
        // A dirty source has one local clone plus its map owner. Neither can
        // escape while this write lock is held; only other owners require COW.
        let needs_copy = existing
            .as_ref()
            .is_none_or(|page| PageAllocation::strong_count(page) != if was_dirty { 2 } else { 1 });
        let needs_payment = needs_copy
            || existing
                .as_ref()
                .is_none_or(|page| !page.counted_by(&self.write_buffer_bytes));
        let payment = if needs_payment {
            Some(self.reserve_write_backing(cached_page_bytes(len), &mut lock)?)
        } else {
            None
        };
        let data = if needs_copy {
            // The old dirty entry stays in the map across fallible admission
            // and copying (or has already been successfully flushed). Locals
            // retain the old payer and roll back the new payment on Err/unwind.
            let charge = crate::read_admission::admit_read_memory(admitted_page_bytes(len))?;
            let mut page = PageAllocation::zeroed(len, charge)?;
            if let Some(source) = &existing {
                PageAllocation::get_mut(&mut page)
                    .unwrap()
                    .copy_from_slice(source);
            } else if !overwrite {
                self.file
                    .read(offset, PageAllocation::get_mut(&mut page).unwrap())?;
            }
            page
        } else {
            existing.unwrap()
        };
        if let Some(payment) = payment {
            let fixed_room = self
                .read_cache_bytes
                .load(Ordering::Acquire)
                .saturating_add(self.write_buffer_bytes.load(Ordering::Acquire))
                <= self.max_cache_size.load(Ordering::Acquire);
            data.transfer_counted(payment, fixed_room);
        }
        // On COW the old alias stays counted until its physical free. No byte
        // alias is detached from the payment when returning writable ownership.
        // No fallible operation remains after removing the old dirty entry.
        // If reserve_write_backing already flushed it, remove is simply empty.
        drop(lock.remove(offset));
        lock.insert(offset, data);
        let mut data = lock.take_value(offset).unwrap();
        PageAllocation::get_mut(&mut data).expect("writable backing is exclusive");
        #[cfg(feature = "cache_metrics")]
        {
            if is_hit {
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
            }
            self.writes_total.fetch_add(1, Ordering::AcqRel);
        }
        #[cfg(not(feature = "cache_metrics"))]
        let _ = is_hit;
        Ok(WritablePage {
            buffer: self.write_buffer.clone(),
            offset,
            data,
        })
    }
}

#[cfg(test)]
impl PagedCachedFile {
    pub(crate) fn write_map_for_test(&self) -> (usize, usize) {
        let write = self.write_buffer.lock().unwrap();
        (write.cache.len(), write.cache.map_capacity_for_test())
    }
}

#[cfg(feature = "contextdb-internal")]
impl PagedCachedFile {
    /// Native clean reads share only natively constructed immutable pages.
    /// A legacy hit is copied under the original native admission before it
    /// enters that cache; no opaque legacy receipt is certified as native.
    pub(super) fn read_native_clean_miss(
        &self, offset: u64, len: usize,
    ) -> std::result::Result<PageMemory, crate::NativeColdReadError> {
        use crate::native_storage_admission::{NativeColdFault, NativeColdReadError};
        if !crate::native_storage_admission::native_storage_active() {
            return Err(NativeColdReadError::AdmissionRefused);
        }
        if self.file.io_failed.load(Ordering::Acquire) {
            return Err(NativeColdReadError::Corrupt(NativeColdFault::new(
                "native read requires an available backend")));
        }
        let stripe = usize::try_from(offset % Self::lock_stripes()).expect("stripe fits usize");
        let cached = self.read_cache[stripe].read().unwrap().get(offset).map(|page| page.clone());
        if let Some(cached) = &cached {
            if cached.len() != len {
                let mut fault = NativeColdFault::new("native cache page length does not match the requested extent");
                fault.expected = len as u128; fault.actual = cached.len() as u128;
                return Err(NativeColdReadError::Corrupt(fault));
            }
            if cached.is_native() {
                let page = PageMemory::Native(cached.clone());
                return Ok(page);
            }
        }
        let mut page = PageAllocation::zeroed_native(len)?;
        let output = PageAllocation::get_mut(&mut page).expect("unpublished native page is unique");
        if let Some(cached) = cached { output.copy_from_slice(&cached); }
        else {
            if let Err(error) = self.file.file.read_cause(offset, output) {
                self.file.io_failed.store(true, Ordering::Release);
                return Err(error.into_native());
            }
            crate::read_admission::record_backend_read(len);
        }
        // The shared cache takes custody under its actual fixed reserve; if
        // it cannot, the page keeps its genuine original source invoice.
        let page = self.cache_loaded_page(offset, len, page, false)
            .map_err(|error| match error {
                StorageError::Native(error) => error,
                StorageError::ValueTooLarge(_) => NativeColdReadError::Corrupt(
                    NativeColdFault::new("native cache capacity overflow")),
                _ => unreachable!("cache insertion retains native allocation failures"),
            })?;
        let page = match page {
            PageMemory::Cached(page) | PageMemory::Charged(page) | PageMemory::Plain(page) | PageMemory::Native(page) => PageMemory::Native(page),
        };
        Ok(page)
    }

    /// Copy the sealed write's unflushed bytes before consulting clean storage.
    /// Old dirty/cache backing retains its original owner throughout this copy;
    /// only the new allocation and control receive preparation's admission.
    pub(super) fn read_native_prepared(
        &self,
        offset: u64,
        len: usize,
    ) -> std::result::Result<PageMemory, crate::PreparedReadError> {
        use crate::native_storage_admission::{NativeColdFault, NativeColdReadError};
        let mut page = PageAllocation::zeroed_native(len)?;
        {
            let write = self.write_buffer.lock().unwrap();
            if let Some(bytes) = write.get(offset) {
                if bytes.len() != len {
                    let mut fault = NativeColdFault::new(
                        "prepared dirty page length does not match the requested extent",
                    );
                    fault.expected = len as u128;
                    fault.actual = bytes.len() as u128;
                    return Err(NativeColdReadError::Corrupt(fault).into());
                }
                PageAllocation::get_mut(&mut page)
                    .expect("unpublished native page is unique")
                    .copy_from_slice(bytes);
                return Ok(PageMemory::Native(page));
            }
        }
        let stripe = (offset % Self::lock_stripes()) as usize;
        {
            let cache = self.read_cache[stripe].read().unwrap();
            if let Some(cell) = cache.get(offset) {
                if cell.len() != len {
                    let mut fault = NativeColdFault::new(
                        "prepared cache page length does not match the requested extent",
                    );
                    fault.expected = len as u128;
                    fault.actual = cell.len() as u128;
                    return Err(NativeColdReadError::Corrupt(fault).into());
                }
                PageAllocation::get_mut(&mut page)
                    .expect("unpublished native page is unique")
                    .copy_from_slice(&cell);
                return Ok(PageMemory::Native(page));
            }
        }
        // Preserve CheckedBackend's PreviousIo/DatabaseClosed/I/O value and
        // read observer; a prepared read must not invent a corruption instead.
        self.file.read(
            offset,
            PageAllocation::get_mut(&mut page)
                .expect("unpublished native page is unique"),
        )?;
        Ok(PageMemory::Native(page))
    }
}

#[cfg(test)]
#[path = "cache_owner_contract_rows.rs"]
mod owner_contract_rows;

#[cfg(test)]
mod test {
    use crate::StorageBackend;
    use crate::backends::InMemoryBackend;
    use crate::tree_store::PageHint;
    use crate::tree_store::page_store::cached_file::{CheckedBackend, PagedCachedFile};
    use std::io;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct RefuseOneGrowth {
        inner: InMemoryBackend,
        refuse: AtomicBool,
    }

    impl StorageBackend for RefuseOneGrowth {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            if !self.refuse.swap(true, Ordering::SeqCst) {
                return Err(io::Error::new(io::ErrorKind::QuotaExceeded, "quota"));
            }
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> io::Result<()> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.inner.write(offset, data)
        }
    }

    struct ReaderCharge {
        held: Arc<AtomicUsize>,
        bytes: usize,
    }
    impl Drop for ReaderCharge {
        fn drop(&mut self) { self.held.fetch_sub(self.bytes, Ordering::SeqCst); }
    }
    fn charged_reader(limit: usize, held: &Arc<AtomicUsize>) -> Arc<dyn Fn(usize) -> io::Result<crate::ReadPageCharge> + Send + Sync> {
        let held = held.clone();
        Arc::new(move |bytes| {
            let previous = held.fetch_add(bytes, Ordering::SeqCst);
            if previous.saturating_add(bytes) > limit {
                held.fetch_sub(bytes, Ordering::SeqCst);
                return Err(io::Error::from(io::ErrorKind::OutOfMemory));
            }
            Ok(Box::new(ReaderCharge { held: held.clone(), bytes }))
        })
    }

    #[test]
    fn declared_stream_recycles_pages_larger_than_its_cache_capacity() {
        let backend = InMemoryBackend::new();
        backend.set_len(128 * 4096).unwrap();
        let page_bytes = super::cached_page_bytes(4096);
        let source_bytes = super::admitted_page_bytes(4096);
        let file = PagedCachedFile::new(Box::new(backend), 4096, 4 * page_bytes).unwrap();
        let held = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let _observe = crate::observe_backend_reads(Arc::new(move |_| { observed.fetch_add(1, Ordering::SeqCst); }));
        crate::with_read_page_charge(charged_reader(5 * source_bytes, &held), || {
            crate::with_scan_reads(|| {
                for ordinal in 0..128 {
                    let page = file.read(ordinal * 4096, 4096, PageHint::Clean).unwrap();
                    assert!(matches!(page, super::PageMemory::Cached(_)));
                    drop(page);
                    assert!(held.load(Ordering::SeqCst) <= 4 * source_bytes);
                    assert!(file.read_cache_bytes.load(Ordering::SeqCst) <= 4 * page_bytes);
                }
            });
            assert_eq!(reads.load(Ordering::SeqCst), 128);
            assert!(file.eviction_visits.load(Ordering::SeqCst) <= 2 * 128);
        });
        // Shared pages keep their original invoice after statement exit.
        assert_eq!(held.load(Ordering::SeqCst), 4 * source_bytes);
        drop(file);
        assert_eq!(held.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn pinned_cache_skips_repeated_victim_walks_and_resumes_after_cross_thread_release() {
        let backend = InMemoryBackend::new();
        backend.set_len(32 * 4096).unwrap();
        let page_bytes = super::cached_page_bytes(4096);
        let source_bytes = super::admitted_page_bytes(4096);
        let file = Arc::new(PagedCachedFile::new(Box::new(backend), 4096, 2 * page_bytes).unwrap());
        let held = Arc::new(AtomicUsize::new(0));
        crate::with_read_page_charge(charged_reader(3 * source_bytes, &held), || {
            let first = file.read(0, 4096, PageHint::Clean).unwrap();
            let second = file.read(4096, 4096, PageHint::Clean).unwrap();
            drop(file.read(8192, 4096, PageHint::Clean).unwrap());
            let initial_visits = file.eviction_visits.load(Ordering::SeqCst);
            assert!(initial_visits > 0);
            for ordinal in 3..24 {
                let miss = file.read(ordinal * 4096, 4096, PageHint::Clean).unwrap();
                assert!(!matches!(miss, super::PageMemory::Cached(_)));
                drop(miss);
            }
            assert_eq!(file.eviction_visits.load(Ordering::SeqCst), initial_visits);
            assert_eq!(held.load(Ordering::SeqCst), 2 * source_bytes);
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let released = barrier.clone();
            let reader = std::thread::spawn(move || { released.wait(); drop(first); });
            barrier.wait();
            reader.join().unwrap();
            // Unpinning did not free or refund the still-cached original page.
            assert_eq!(held.load(Ordering::SeqCst), 2 * source_bytes);
            let replacement = file.read(8192, 4096, PageHint::Clean).unwrap();
            assert!(matches!(replacement, super::PageMemory::Cached(_)));
            assert!(file.eviction_visits.load(Ordering::SeqCst) > initial_visits);
            assert_eq!(held.load(Ordering::SeqCst), 2 * source_bytes);
            assert!(second.iter().all(|byte| *byte == 0));
            drop(replacement);
            drop(second);
        });
        drop(file);
        assert_eq!(held.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn quota_refusal_does_not_poison_the_checked_backend() {
        let backend = CheckedBackend::new(Box::new(RefuseOneGrowth::default()));
        let refusal = backend.set_len(1024).unwrap_err();
        assert!(matches!(
            refusal,
            crate::StorageError::Io(error) if error.kind() == io::ErrorKind::QuotaExceeded
        ));
        backend.set_len(1024).unwrap();
        assert_eq!(backend.len().unwrap(), 1024);
    }

    #[test]
    fn unscoped_scan_reads_use_only_their_shared_cache_allowance() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024).unwrap();
        // Past its growth allowance, an empty cache has nothing to replace:
        // the miss stays uncached.
        let page = crate::read_admission::with_scan_reads(|| {
            assert!(crate::read_admission::take_scan_growth(
                crate::read_admission::SCAN_GROWTH_ALLOWANCE
            ));
            cached_file.read(0, 128, PageHint::None).unwrap()
        });
        assert_eq!(page.len(), 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
        drop(page);
        // Within the allowance a miss uses free room.
        let installed = crate::read_admission::with_scan_reads(|| {
            cached_file.read(384, 128, PageHint::None).unwrap()
        });
        assert!(matches!(installed, super::PageMemory::Cached(_)));
        drop(installed);
        cached_file.invalidate_cache(384, 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
        cached_file.read(128, 128, PageHint::None).unwrap();
        assert_eq!(
            cached_file.read_cache_bytes.load(Ordering::Acquire),
            super::cached_page_bytes(128)
        );
        // A hit is served from the cache.
        let hit = crate::read_admission::with_scan_reads(|| {
            cached_file.read(128, 128, PageHint::None).unwrap()
        });
        assert!(matches!(hit, super::PageMemory::Cached(_)));
        drop(hit);
        // Past the allowance a miss replaces the evictable page; the cache
        // does not grow.
        let replaced = crate::read_admission::with_scan_reads(|| {
            assert!(crate::read_admission::take_scan_growth(
                crate::read_admission::SCAN_GROWTH_ALLOWANCE
            ));
            cached_file.read(256, 128, PageHint::None).unwrap()
        });
        assert!(matches!(replaced, super::PageMemory::Cached(_)));
        assert_eq!(
            cached_file.read_cache_bytes.load(Ordering::Acquire),
            super::cached_page_bytes(128)
        );
    }

    #[test]
    fn cache_leak() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024).unwrap();
        let cached_file = Arc::new(cached_file);

        let t1 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };
        let t2 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();
        cached_file.invalidate_cache(0, 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
    }
    #[test]
    fn immutable_byte_checksum_owners_are_charged_and_release_at_final_holder() {
        super::owner_contract_rows::run();
        struct Charge(Arc<std::sync::atomic::AtomicUsize>, usize);
        impl Drop for Charge {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, Ordering::SeqCst);
            }
        }
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let bytes = super::cached_page_bytes(128);
        let file = PagedCachedFile::new(Box::new(backend), 128, bytes).unwrap();
        let page = file.read(0, 128, PageHint::Clean).unwrap();
        assert!(matches!(page, super::PageMemory::Cached(_)));
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), bytes);
        let clone = page.clone();
        file.invalidate_cache(0, 128);
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), bytes);
        let charged_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let charged = charged_bytes.clone();
        let held = crate::with_read_page_charge(
            Arc::new(move |size| {
                assert_eq!(size, super::admitted_page_bytes(128));
                charged.fetch_add(size, Ordering::SeqCst);
                Ok(Box::new(Charge(charged.clone(), size)))
            }),
            || file.read(128, 128, PageHint::Clean).unwrap(),
        );
        assert!(matches!(held, super::PageMemory::Charged(_)));
        let held_clone = held.clone();
        drop(held);
        assert_eq!(
            charged_bytes.load(Ordering::SeqCst),
            super::admitted_page_bytes(128)
        );
        drop(held_clone);
        assert_eq!(charged_bytes.load(Ordering::SeqCst), 0);
        drop(page);
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), bytes);
        drop(clone);
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
        let replacement = file.read(0, 128, PageHint::Clean).unwrap();
        assert!(matches!(replacement, super::PageMemory::Cached(_)));
        #[cfg(feature = "cache_metrics")]
        assert_eq!(file.cache_stats().used_bytes, bytes);
        eprintln!(
            "owner controls: CachedPage={} ChargedPage={} slot={} cached128={} admission128={}",
            super::PageAllocation::layout(0).unwrap().0.size(),
            super::PageAllocation::layout(0).unwrap().0.size(),
            size_of::<std::sync::Mutex<Option<u128>>>(),
            bytes,
            super::admitted_page_bytes(128)
        );
    }

    #[test]
    fn immutable_byte_checksum_memo_lifecycle_and_plain_exclusion() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let file = PagedCachedFile::new(Box::new(backend), 128, 1024).unwrap();
        let before = super::PageMemory::byte_checksum_counts_for_test();
        let cached = file.read(0, 128, PageHint::Clean).unwrap();
        assert!(
            cached
                .memoized_byte_node_checksum(|| Err(crate::StorageError::Corrupted(
                    "failed layout".into()
                )))
                .is_err()
        );
        assert_eq!(cached.memoized_byte_node_checksum(|| Ok(41)).unwrap(), 41);
        assert_eq!(
            cached
                .clone()
                .memoized_byte_node_checksum(|| panic!("cached computation repeated"))
                .unwrap(),
            41
        );
        file.invalidate_cache(0, 128);
        // Reusing the offset constructs a fresh object, while the old holder
        // retains its bytes, memo, and original native charge.
        let new = file.read(0, 128, PageHint::Clean).unwrap();
        assert_eq!(new.memoized_byte_node_checksum(|| Ok(42)).unwrap(), 42);
        assert_eq!(
            cached
                .memoized_byte_node_checksum(|| panic!("old owner lost its memo"))
                .unwrap(),
            41
        );
        let charged = crate::with_read_page_charge(Arc::new(|_| Ok(Box::new(()))), || {
            let backend = InMemoryBackend::new();
            backend.set_len(128).unwrap();
            PagedCachedFile::new(Box::new(backend), 128, 0)
                .unwrap()
                .read(0, 128, PageHint::Clean)
                .unwrap()
        });
        assert!(matches!(charged, super::PageMemory::Charged(_)));
        assert_eq!(charged.memoized_byte_node_checksum(|| Ok(43)).unwrap(), 43);
        assert_eq!(
            charged
                .clone()
                .memoized_byte_node_checksum(|| panic!("charged computation repeated"))
                .unwrap(),
            43
        );
        let synchronized = file.read(384, 128, PageHint::Clean).unwrap();
        let starts = Arc::new(std::sync::Barrier::new(8));
        let computations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let page = synchronized.clone();
                let starts = starts.clone();
                let computations = computations.clone();
                scope.spawn(move || {
                    starts.wait();
                    assert_eq!(
                        page.memoized_byte_node_checksum(|| {
                            computations.fetch_add(1, Ordering::SeqCst);
                            Ok(47)
                        })
                        .unwrap(),
                        47
                    );
                });
            }
        });
        assert_eq!(computations.load(Ordering::SeqCst), 1);
        let plain = super::PageMemory::Plain(super::PageAllocation::zeroed(128, None).unwrap());
        assert_eq!(plain.memoized_byte_node_checksum(|| Ok(44)).unwrap(), 44);
        assert_eq!(plain.memoized_byte_node_checksum(|| Ok(45)).unwrap(), 45);
        let after = super::PageMemory::byte_checksum_counts_for_test();
        assert_eq!(
            std::array::from_fn::<_, 5, _>(|i| after[i] - before[i]),
            [3, 1, 2, 2, 1]
        );
        eprintln!(
            "memo lifecycle deltas Cached/Charged/Plain computations, Cached/Charged hits: [3,1,2,2,1]"
        );
        // Flushed mutable bytes become a new immutable wrapper, empty memo.
        drop(new);
        drop(cached);
        let mut writable = file.write(256, 128, true).unwrap();
        writable.mem_mut()[0] = 9;
        drop(writable);
        let plain = file.read(256, 128, PageHint::None).unwrap();
        assert!(matches!(plain, super::PageMemory::Plain(_)));
        drop(plain);
        file.write_barrier().unwrap();
        let transferred = file.read(256, 128, PageHint::Clean).unwrap();
        assert!(matches!(transferred, super::PageMemory::Cached(_)));
        assert_eq!(
            transferred.memoized_byte_node_checksum(|| Ok(46)).unwrap(),
            46
        );
    }

    // The writer reopen/doubling acceptance fixture promises that its warmed
    // fixed cache share stays saturated without growth. These controls prove
    // that same occupancy promise with the real native cache and page owners.
    // A uniform native cache fills its declared share to the maximum complete
    // page count. Held branch pages stay resident while CLOCK replaces leaves.
    const UNIFORM_PAGE_BYTES: usize = 4096;
    const UNIFORM_CACHE_CAP: usize = 4 * 1024 * 1024;
    fn uniform_cache_slots() -> usize {
        UNIFORM_CACHE_CAP / super::PageAllocation::layout(UNIFORM_PAGE_BYTES).unwrap().0.size()
    }

    fn uniform_cache_file(extra_pages: usize) -> PagedCachedFile {
        assert_eq!(super::cached_page_bytes(UNIFORM_PAGE_BYTES), super::PageAllocation::layout(UNIFORM_PAGE_BYTES).unwrap().0.size());
        assert_eq!(UNIFORM_CACHE_CAP / super::cached_page_bytes(UNIFORM_PAGE_BYTES), uniform_cache_slots());
        let backend = InMemoryBackend::new();
        backend
            .set_len(((uniform_cache_slots() + extra_pages) * UNIFORM_PAGE_BYTES) as u64)
            .unwrap();
        PagedCachedFile::new(
            Box::new(backend),
            UNIFORM_PAGE_BYTES as u64,
            UNIFORM_CACHE_CAP,
        )
        .unwrap()
    }

    fn uniform_page(file: &PagedCachedFile, ordinal: usize) -> super::PageMemory {
        let page = file
            .read(
                (ordinal * UNIFORM_PAGE_BYTES) as u64,
                UNIFORM_PAGE_BYTES,
                PageHint::Clean,
            )
            .unwrap();
        assert_eq!(page.len(), UNIFORM_PAGE_BYTES);
        page
    }

    // Inspect by borrowing: no get, second-chance bit change, or page clone.
    fn uniform_cache_cut(file: &PagedCachedFile) -> (usize, usize, usize, usize) {
        let mut entries = 0;
        let mut payload = 0;
        let mut charge = 0;
        let mut pinned = 0;
        for stripe in &file.read_cache {
            let stripe = stripe.read().unwrap();
            for (_, page) in stripe.iter() {
                assert_eq!(page.len(), UNIFORM_PAGE_BYTES);
                entries += 1;
                payload += page.len();
                charge += super::cached_page_bytes(page.len());
                pinned += usize::from(super::PageAllocation::strong_count(&*page) > 1);
            }
        }
        (entries, payload, charge, pinned)
    }

    fn assert_uniform_cache_full(file: &PagedCachedFile, pinned: usize) {
        assert_eq!(
            uniform_cache_cut(file),
            (
                uniform_cache_slots(),
                uniform_cache_slots() * UNIFORM_PAGE_BYTES,
                uniform_cache_slots() * super::cached_page_bytes(UNIFORM_PAGE_BYTES),
                pinned,
            ),
            "every funded uniform page remains in the cache map"
        );
        assert_eq!(
            file.read_cache_bytes.load(Ordering::Acquire),
            uniform_cache_slots() * super::cached_page_bytes(UNIFORM_PAGE_BYTES),
            "the ledger reconciles every resident page, including pinned pages"
        );
        assert_eq!(file.write_buffer_bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            file.max_cache_size.load(Ordering::Acquire),
            UNIFORM_CACHE_CAP
        );
        assert!(uniform_cache_slots() * super::cached_page_bytes(UNIFORM_PAGE_BYTES) <= UNIFORM_CACHE_CAP);
        assert!((uniform_cache_slots() + 1) * super::cached_page_bytes(UNIFORM_PAGE_BYTES) > UNIFORM_CACHE_CAP);
    }

    #[test]
    fn uniform_cache_fills_its_complete_page_capacity_and_preserves_it_on_misses() {
        let misses = uniform_cache_slots() * 2;
        let file = uniform_cache_file(misses);
        for ordinal in 0..uniform_cache_slots() {
            let page = uniform_page(&file, ordinal);
            assert!(matches!(page, super::PageMemory::Cached(_)));
            drop(page);
        }
        assert_uniform_cache_full(&file, 0);
        for ordinal in uniform_cache_slots()..uniform_cache_slots() + misses {
            let page = uniform_page(&file, ordinal);
            assert!(matches!(page, super::PageMemory::Cached(_)));
            drop(page);
            assert_uniform_cache_full(&file, 0);
        }
        for stripe in &file.read_cache {
            stripe.write().unwrap().clear();
        }
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn pinned_branch_pages_preserve_uniform_cache_occupancy_through_the_last_miss_and_drop() {
        let misses = uniform_cache_slots() * 2;
        let file = uniform_cache_file(misses);
        // Three branch-like pages in the first CLOCK stripe remain held
        // across more than two complete passes over every stripe's victims.
        let branch_ordinals = [0, 131, 262];
        let branches = branch_ordinals.map(|ordinal| uniform_page(&file, ordinal));
        for page in &branches {
            assert!(matches!(page, super::PageMemory::Cached(_)));
        }
        for ordinal in 0..uniform_cache_slots() {
            drop(uniform_page(&file, ordinal));
        }
        assert_uniform_cache_full(&file, branches.len());
        for ordinal in uniform_cache_slots()..uniform_cache_slots() + misses {
            let page = uniform_page(&file, ordinal);
            assert!(matches!(page, super::PageMemory::Cached(_)));
            drop(page);
            assert_uniform_cache_full(&file, branches.len());
        }
        // No refill read occurs after the last miss: holder drop alone must
        // not lower the funded resident map or its saturated native ledger.
        drop(branches);
        assert_uniform_cache_full(&file, 0);
        #[cfg(feature = "cache_metrics")]
        let hits = file.cache_stats().read_hits;
        for ordinal in branch_ordinals {
            drop(uniform_page(&file, ordinal));
        }
        #[cfg(feature = "cache_metrics")]
        assert_eq!(
            file.cache_stats().read_hits,
            hits + branch_ordinals.len() as u64
        );
        assert_uniform_cache_full(&file, 0);
        for stripe in &file.read_cache {
            stripe.write().unwrap().clear();
        }
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn an_all_pinned_uniform_cache_finishes_misses_without_eviction_or_cache_growth() {
        struct MissCharge(Arc<std::sync::atomic::AtomicUsize>, usize);
        impl Drop for MissCharge {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, Ordering::SeqCst);
            }
        }
        let misses = 5;
        let file = uniform_cache_file(misses);
        let holders = (0..uniform_cache_slots())
            .map(|ordinal| uniform_page(&file, ordinal))
            .collect::<Vec<_>>();
        assert_uniform_cache_full(&file, uniform_cache_slots());
        let resident_identities = || {
            let mut entries = std::collections::BTreeMap::new();
            for stripe in &file.read_cache {
                for (offset, page) in stripe.read().unwrap().iter() {
                    entries.insert(offset, super::PageAllocation::as_ptr(&*page) as usize);
                }
            }
            entries
        };
        let before = resident_identities();
        #[cfg(feature = "cache_metrics")]
        let evictions = file.cache_stats().evictions;
        let charged = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for ordinal in uniform_cache_slots()..uniform_cache_slots() + misses {
            let admission = charged.clone();
            let page = crate::with_read_page_charge(
                Arc::new(move |bytes| {
                    assert_eq!(bytes, super::admitted_page_bytes(UNIFORM_PAGE_BYTES));
                    admission.fetch_add(bytes, Ordering::SeqCst);
                    Ok(Box::new(MissCharge(admission.clone(), bytes)))
                }),
                || uniform_page(&file, ordinal),
            );
            // Reaching this assertion proves the all-pinned victim walk
            // terminated. The miss belongs to its reader, never the cache.
            assert!(matches!(page, super::PageMemory::Charged(_)));
            assert_eq!(
                charged.load(Ordering::SeqCst),
                super::admitted_page_bytes(UNIFORM_PAGE_BYTES)
            );
            assert_eq!(
                resident_identities(),
                before,
                "no pinned entry is removed or replaced"
            );
            assert_uniform_cache_full(&file, uniform_cache_slots());
            #[cfg(feature = "cache_metrics")]
            assert_eq!(file.cache_stats().evictions, evictions);
            drop(page);
            assert_eq!(charged.load(Ordering::SeqCst), 0);
        }
        // Every pinned holder drops after the final miss, with no refill.
        drop(holders);
        assert_uniform_cache_full(&file, 0);
        assert_eq!(resident_identities(), before);
        for stripe in &file.read_cache {
            stripe.write().unwrap().clear();
        }
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
        assert_eq!(charged.load(Ordering::SeqCst), 0);
    }
    // P1: actual index allocation includes the boxed stripe descriptors.
    #[test]
    fn read_index_live_bytes_are_fixed_for_uniform_and_one_stripe_spreads() {
        use crate::tree_store::page_store::lru_cache::arena_index_tests::Watch;
        let mut spread_bytes = [0; 2];
        let page = super::PageAllocation::zeroed(4096, None).unwrap();
        for (spread, skewed) in [false, true].into_iter().enumerate() {
            let watch = Watch::start();
            let stripes = PagedCachedFile::new_read_index(4 * 1024 * 1024, 4096, 0);
            let index_bytes = stripes[0].read().unwrap().arena_index_bytes()
                + stripes
                    .iter()
                    .map(|s| s.read().unwrap().index_bytes())
                    .sum::<usize>()
                + stripes.len()
                    * size_of::<
                        std::sync::RwLock<
                            crate::tree_store::page_store::lru_cache::LRUCache<
                                super::PageAllocation,
                            >,
                        >,
                    >();
            let construction = watch.cut();
            assert_eq!(construction[2], 0);
            assert_eq!(construction[3] - construction[4], index_bytes);
            spread_bytes[spread] = index_bytes;
            watch.reset();
            for round in 0..3_u64 {
                for ordinal in 0..u64::try_from(uniform_cache_slots()).unwrap() {
                    let stripe = if skewed {
                        0
                    } else {
                        usize::try_from(ordinal).unwrap() % stripes.len()
                    };
                    assert!(matches!(
                        stripes[stripe]
                            .write()
                            .unwrap()
                            .try_insert(round * u64::try_from(uniform_cache_slots()).unwrap() + ordinal, page.clone()),
                        Ok(None)
                    ));
                }
                assert_eq!(
                    stripes
                        .iter()
                        .map(|s| s.read().unwrap().len())
                        .sum::<usize>(),
                    uniform_cache_slots()
                );
                for stripe in &stripes {
                    let mut stripe = stripe.write().unwrap();
                    while stripe.pop_lowest_priority().is_some() {}
                }
                assert_eq!(
                    watch.cut(),
                    [0; 5],
                    "only the original fixed index allocations remain live"
                );
            }
            drop(stripes);
            let released = watch.cut();
            assert_eq!(released[0], 0);
            assert_eq!(released[2], 0);
            assert_eq!(released[3], 0);
            assert_eq!(released[4], index_bytes);
        }
        assert_eq!(spread_bytes[0], spread_bytes[1]);
    }

    #[derive(Debug)]
    struct SparseZeroFixture(std::sync::atomic::AtomicU64);
    impl StorageBackend for SparseZeroFixture {
        fn len(&self) -> io::Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.0.store(len, Ordering::SeqCst);
            Ok(())
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            assert!(offset + out.len() as u64 <= self.0.load(Ordering::SeqCst));
            out.fill(0);
            Ok(())
        }
        fn write(&self, _: u64, _: &[u8]) -> io::Result<()> {
            panic!("the read-only sparse pressure fixture is never written")
        }
        fn sync_data(&self) -> io::Result<()> {
            Ok(())
        }
    }

    // P2: offset skew cannot impose a per-stripe occupancy ceiling.
    #[test]
    fn one_stripe_pressure_uses_all_996_shared_read_arena_slots() {
        let cap = 4 * 1024 * 1024;
        assert_eq!(super::cached_page_bytes(4096), super::PageAllocation::layout(4096).unwrap().0.size());
        assert_eq!(cap / super::cached_page_bytes(4096), uniform_cache_slots());
        let pages = 3 * uniform_cache_slots();
        let backend = SparseZeroFixture(std::sync::atomic::AtomicU64::new(
            u64::try_from(pages * 131 * 4096).unwrap(),
        ));
        let file = PagedCachedFile::new(Box::new(backend), 4096, cap).unwrap();
        let index_bytes = file.index_bytes();
        for ordinal in 0..pages {
            let offset = u64::try_from(ordinal * 131 * 4096).unwrap();
            assert_eq!(offset % PagedCachedFile::lock_stripes(), 0);
            let page = file.read(offset, 4096, PageHint::Clean).unwrap();
            assert!(matches!(page, super::PageMemory::Cached(_)));
            assert!(page.iter().all(|byte| *byte == 0));
            drop(page);
            if ordinal + 1 >= uniform_cache_slots() {
                assert_eq!(file.read_cache[0].read().unwrap().len(), uniform_cache_slots());
                assert!(
                    file.read_cache[1..]
                        .iter()
                        .all(|s| s.read().unwrap().len() == 0)
                );
                assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), uniform_cache_slots() * super::cached_page_bytes(4096));
                assert_eq!(file.index_bytes(), index_bytes);
            }
        }
    }

    // P5 and P5b: settle changes its fixed index and preserves live holders.
    #[test]
    #[cfg(feature = "contextdb-internal")]
    fn settle_rebuilds_exact_read_index_and_preserves_above_cap_held_pages() {
        let backend = InMemoryBackend::new();
        backend.set_len(64 * 4096).unwrap();
        let file = PagedCachedFile::new(Box::new(backend), 4096, 67_328).unwrap();
        for cap in [16_832, 134_656, 33_664] {
            assert_eq!(
                file.settle_startup_cache_budget(cap).unwrap(),
                cap + file.index_bytes()
            );
            assert_eq!(
                file.read_cache[0].read().unwrap().arena_capacity_for_test(),
                cap / super::cached_page_bytes(4096) + 1
            );
        }
        let holders = (0_u64..8)
            .map(|ordinal| file.read(ordinal * 4096, 4096, PageHint::Clean).unwrap())
            .collect::<Vec<_>>();
        assert!(
            holders
                .iter()
                .all(|p| matches!(p, super::PageMemory::Cached(_)))
        );
        let before = || {
            file.read_cache
                .iter()
                .flat_map(|stripe| {
                    let stripe = stripe.read().unwrap();
                    stripe
                        .iter()
                        .map(|(key, value)| (key, super::PageAllocation::as_ptr(&*value) as usize))
                        .collect::<Vec<_>>()
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let identities = before();
        let held_bytes = 8 * super::cached_page_bytes(4096);
        let cap = 8_416;
        assert_eq!(
            file.settle_startup_cache_budget(cap).unwrap(),
            held_bytes.max(cap) + file.index_bytes()
        );
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), held_bytes);
        assert_eq!(
            before(),
            identities,
            "settle keeps every held page in its linked stripe index"
        );
        for page in &holders {
            assert_eq!(page.len(), 4096);
            assert!(page.iter().all(|byte| *byte == 0));
        }
        drop(holders);
        let next_cap = cap;
        assert_eq!(
            file.settle_startup_cache_budget(next_cap).unwrap(),
            next_cap + file.index_bytes()
        );
        assert_eq!(
            file.read_cache[0].read().unwrap().arena_capacity_for_test(),
            next_cap / super::cached_page_bytes(4096) + 1
        );
        assert!(file.read_cache_bytes.load(Ordering::SeqCst) <= next_cap);
    }

    #[derive(Debug)]
    struct WritableHeaderFixture {
        len: std::sync::atomic::AtomicU64,
        header: std::sync::Mutex<[u8; 320]>,
    }
    impl WritableHeaderFixture {
        fn new(len: u64) -> Self {
            Self {
                len: std::sync::atomic::AtomicU64::new(len),
                header: std::sync::Mutex::new([0; 320]),
            }
        }
    }
    impl StorageBackend for WritableHeaderFixture {
        fn len(&self) -> io::Result<u64> {
            Ok(self.len.load(Ordering::SeqCst))
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            assert!(offset + out.len() as u64 <= self.len.load(Ordering::SeqCst));
            out.fill(0);
            if offset == 0 {
                let header = self.header.lock().unwrap();
                let count = out.len().min(header.len());
                out[..count].copy_from_slice(&header[..count]);
            }
            Ok(())
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.len.store(len, Ordering::SeqCst);
            Ok(())
        }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            assert!(offset + data.len() as u64 <= self.len.load(Ordering::SeqCst));
            if offset == 0 {
                let mut header = self.header.lock().unwrap();
                let count = data.len().min(header.len());
                header[..count].copy_from_slice(&data[..count]);
            }
            Ok(())
        }
        fn sync_data(&self) -> io::Result<()> {
            Ok(())
        }
    }

    // Actual header writes reach the read cache through the normal flush.
    #[test]
    #[cfg(feature = "contextdb-internal")]
    fn flushed_header_and_full_pages_use_the_unchanged_cache_byte_budget() {
        let header_len = super::super::header::DB_HEADER_SIZE;
        assert_eq!(header_len, 320);
        let full_bytes = super::cached_page_bytes(4096);
        let header_bytes = super::cached_page_bytes(header_len);
        assert_eq!((full_bytes, header_bytes), (super::PageAllocation::layout(4096).unwrap().0.size(), super::PageAllocation::layout(header_len).unwrap().0.size()));
        for cap in [4 * 1024 * 1024, 64 * 1024 * 1024] {
            let control = PagedCachedFile::new_read_index(cap, 4096, 0);
            let backend = Box::new(WritableHeaderFixture::new(4096));
            let write_observer = super::super::lru_cache::arena_index_tests::Watch::start();
            let actual = PagedCachedFile::new(backend, 4096, cap).unwrap();
            let construction = write_observer.cut();
            assert_eq!(construction[2], 0);
            assert_eq!(construction[3], actual.index_bytes(), "the real file constructor allocates exactly its index Layouts");
            drop(write_observer);
            let (write_pointer, write_bytes, write_align) = actual.write_buffer.allocation_for_test();
            let write_control = std::alloc::Layout::from_size_align(write_bytes, write_align).unwrap();
            assert_ne!(write_pointer, 0);
            assert_eq!(write_pointer % write_align, 0);
            // The very same control belongs to this file; constructing a second
            // Arc would certify a different producer and a different ABI.
            assert_eq!(write_control, PagedCachedFile::write_buffer_control_layout());
            let write_alias = actual.write_buffer.clone();
            let counter_alias = actual.write_buffer_bytes.clone();
            let free = super::super::lru_cache::arena_index_tests::FreeWatch::start(
                write_pointer as *const (), &counter_alias);
            drop(actual);
            assert_eq!(free.cut(), [0; 4]);
            drop(write_alias);
            assert_eq!(free.cut(), [1, write_control.size(), 0, 0],
                "the actual native write-control is physically freed once at its producer Layout");
            drop(free);
            drop(counter_alias);
            let expected_index = 2 * super::CacheCounter::layout().size()
                + write_control.size()
                + control[0].read().unwrap().arena_index_bytes()
                + control.iter().map(|stripe| stripe.read().unwrap().index_bytes()).sum::<usize>()
                + std::mem::size_of_val(control.as_ref());
            drop(control);
            for stride in [1, 131] {
                let full_pages = cap / full_bytes;
                let backend = WritableHeaderFixture::new(((full_pages + 1) * stride * 4096) as u64);
                let file = PagedCachedFile::new(Box::new(backend), 4096, cap).unwrap();
                let index_bytes = file.index_bytes();
                let mut writable = file.write(0, header_len, true).unwrap();
                writable.mem_mut().fill(0x7a);
                drop(writable);
                file.flush().unwrap();
                assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), header_bytes);
                assert_eq!(file.write_map_for_test(), (0, 0));
                let header = file.read(0, header_len, PageHint::Clean).unwrap();
                assert!(matches!(header, super::PageMemory::Cached(_)));
                assert!(header.iter().all(|byte| *byte == 0x7a));
                for ordinal in 1..=full_pages {
                    let offset = (ordinal * stride * 4096) as u64;
                    let page = file.read(offset, 4096, PageHint::Clean).unwrap();
                    assert!(
                        matches!(page, super::PageMemory::Cached(_)),
                        "a byte-fitting full page must not exhaust the fixed index"
                    );
                    assert!(page.iter().all(|byte| *byte == 0));
                    drop(page);
                    assert_eq!(file.index_bytes(), index_bytes);
                }
                let resident_bytes = file.read_cache_bytes.load(Ordering::SeqCst);
                assert_eq!(resident_bytes, header_bytes + full_pages * full_bytes);
                assert_eq!(index_bytes, expected_index);
                assert!(resident_bytes <= cap);
                assert!(cap - resident_bytes < full_bytes);
                assert_eq!(
                    file.read_cache
                        .iter()
                        .map(|s| s.read().unwrap().len())
                        .sum::<usize>(),
                    full_pages + 1
                );
                assert_eq!(
                    file.read_cache[0].read().unwrap().arena_capacity_for_test(),
                    full_pages + 1
                );
                if stride == 131 {
                    assert_eq!(file.read_cache[0].read().unwrap().len(), full_pages + 1);
                    assert!(
                        file.read_cache[1..]
                            .iter()
                            .all(|s| s.read().unwrap().len() == 0)
                    );
                }
                assert_eq!(
                    file.settle_startup_cache_budget(cap).unwrap(),
                    cap + index_bytes
                );
                assert_eq!(
                    file.settle_startup_cache_budget(cap).unwrap(),
                    cap + index_bytes
                );
                assert_eq!(file.index_bytes(), index_bytes);
                assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), resident_bytes);
                assert!(header.iter().all(|byte| *byte == 0x7a));
            }
        }
    }

    // Every indexed admission route enforces the same sole-header premise.
    #[test]
    #[cfg(debug_assertions)]
    fn invalid_subpage_normal_scan_and_flush_admissions_fail_loudly() {
        for (offset, len) in [(4096, 320), (0, 319)] {
            for route in ["normal", "scan", "flush"] {
                let backend = WritableHeaderFixture::new(16_384);
                let file = PagedCachedFile::new(Box::new(backend), 4096, 4 * 1024 * 1024).unwrap();
                // Ensure the exhausted scan branch has a real evictable page to replace.
                drop(file.read(8192, 4096, PageHint::Clean).unwrap());
                let invalid =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match route {
                        "normal" => {
                            let _page = file.read(offset, len, PageHint::Clean).unwrap();
                        }
                        "scan" => crate::read_admission::with_scan_reads(|| {
                            assert!(crate::read_admission::take_scan_growth(
                                crate::read_admission::SCAN_GROWTH_ALLOWANCE
                            ));
                            let _page = file.read(offset, len, PageHint::Clean).unwrap();
                        }),
                        "flush" => {
                            let mut page = file.write(offset, len, true).unwrap();
                            page.mem_mut().fill(0x7a);
                            drop(page);
                            file.flush().unwrap();
                        }
                        _ => unreachable!(),
                    }));
                let panic = invalid.expect_err("an invalid subpage admission must panic");
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                assert!(
                    message.contains("DB_HEADER_SIZE"),
                    "route {route} offset {offset} length {len} must fail its actual header invariant, not a fixture panic: {message}"
                );
            }
        }
    }

    // P6: borrowed write pages keep the baseline cap-0 semantics; flush frees the map.
    #[test]
    fn write_buffer_has_zero_entries_and_capacity_after_each_flush() {
        for cap in [0, 4096, 4 * 1024 * 1024] {
            let backend = InMemoryBackend::new();
            backend.set_len(16 * 4096).unwrap();
            let file = PagedCachedFile::new(Box::new(backend), 4096, cap).unwrap();
            let index_bytes = file.index_bytes();
            for transaction in 0..2_u64 {
                let mut first = file.write(0, 4096, true).unwrap();
                let mut second = file.write(4096, 4096, true).unwrap();
                let mut third = file.write(8192, 4096, true).unwrap();
                first.mem_mut()[..8].copy_from_slice(&(transaction + 1).to_le_bytes());
                second.mem_mut()[..8].copy_from_slice(&(transaction + 2).to_le_bytes());
                third.mem_mut()[..8].copy_from_slice(&(transaction + 3).to_le_bytes());
                drop(third);
                drop(second);
                drop(first);
                file.flush().unwrap();
                let write = file.write_buffer.lock().unwrap();
                assert_eq!(write.cache.len(), 0);
                assert_eq!(write.cache.map_capacity_for_test(), 0);
                assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), 0);
                drop(write);
                assert_eq!(file.index_bytes(), index_bytes);
                for ordinal in 0..3_u64 {
                    let page = file.read(ordinal * 4096, 4096, PageHint::Clean).unwrap();
                    assert_eq!(&page[..8], &(transaction + ordinal + 1).to_le_bytes());
                }
            }
        }
    }
}

// Test-only access to the actual dispatch and failure latch, with no substitute
// error producer or observer allocation. Box construction stays outside claims.
#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
#[doc(hidden)]
pub struct CheckedWriterBackendForTest(CheckedBackend);
#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
#[allow(clippy::len_without_is_empty)]
impl CheckedWriterBackendForTest {
    pub fn native(backend: impl crate::NativeStorageBackend) -> Self {
        Self(CheckedBackend::new(crate::db::Backend::Native(Box::new(backend))))
    }
    pub fn legacy(backend: impl crate::StorageBackend) -> Self {
        Self(CheckedBackend::new(Box::new(backend)))
    }
    pub fn len(&self) -> crate::Result<u64> { self.0.len() }
    pub fn read(&self, offset: u64, out: &mut [u8]) -> crate::Result<()> { self.0.read(offset, out) }
    pub fn write(&self, offset: u64, data: &[u8]) -> crate::Result<()> { self.0.write(offset, data) }
    pub fn set_len(&self, len: u64) -> crate::Result<()> { self.0.set_len(len) }
    pub fn sync_data(&self) -> crate::Result<()> { self.0.sync_data() }
}
