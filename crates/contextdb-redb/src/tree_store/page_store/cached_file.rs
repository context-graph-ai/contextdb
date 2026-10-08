use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::header::DB_HEADER_SIZE;
use crate::tree_store::page_store::lru_cache::{EntryArena, LRUCache, WriteCache};
use crate::{CacheStats, DatabaseError, Result, StorageBackend, StorageError};
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;
#[cfg(feature = "cache_metrics")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

// Allocates an `Arc<[u8]>` in one step. `Arc::<[u8]>::from(vec![0; len])` would
// allocate the Vec and then allocate a new Arc and memcpy into it.
fn zero_filled_arc(len: usize) -> Arc<[u8]> {
    // This is documented to do a single allocation: https://doc.rust-lang.org/std/sync/struct.Arc.html#iterators-of-known-length
    std::iter::repeat_n(0u8, len).collect()
}

/// The bytes of one page as handed to a reader. Every page a reader holds is
/// paid for by exactly one owner for as long as it lives.
#[derive(Clone)]
pub(crate) enum PageMemory {
    /// A page that entered the read cache. It counts against the cache's
    /// budget from the moment it is admitted until its last holder drops it
    /// -- including after the cache has evicted it while a reader still holds
    /// it -- so the cache's budget covers every such page for its whole life.
    Cached(Arc<CachedPage>),
    /// A page the cache could not take, carrying the charge its reader's
    /// admission returned until its last holder drops it.
    Charged(Arc<ChargedPage>),
    /// A page no reader charge covers: one still owned by the write buffer
    /// (counted there), or one read outside a charging admission that the
    /// cache could not take, which the caller's own admission covers.
    Plain(Arc<[u8]>),
}

/// A page admitted to the read cache. Its bytes leave the cache's count when
/// the page itself is dropped, not when the cache evicts it.
pub(crate) struct CachedPage {
    bytes: Arc<[u8]>,
    counted: Arc<AtomicUsize>,
    byte_node_checksum: Mutex<Option<u128>>,
}

impl CachedPage {
    /// `counted` must already include payload and immutable owner controls.
    fn admitted(bytes: Arc<[u8]>, counted: &Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            counted: Arc::clone(counted),
            byte_node_checksum: Mutex::new(None),
        })
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl Drop for CachedPage {
    fn drop(&mut self) {
        self.counted
            .fetch_sub(cached_page_bytes(self.bytes.len()), Ordering::AcqRel);
    }
}

pub(crate) struct ChargedPage {
    bytes: Arc<[u8]>,
    _charge: crate::read_admission::ReadPageCharge,
    byte_node_checksum: Mutex<Option<u128>>,
}

// Arc's two reference counters precede its aligned inline value. The byte
// allocation has the same two counters; the slot itself allocates no heap.
const fn arc_control_bytes<T>() -> usize {
    let header = 2 * size_of::<usize>();
    let alignment = align_of::<T>();
    header.next_multiple_of(alignment) + size_of::<T>()
}

const fn cached_page_bytes(len: usize) -> usize {
    len.saturating_add(2 * size_of::<usize>())
        .saturating_add(arc_control_bytes::<CachedPage>())
}

const fn admitted_page_bytes(len: usize) -> usize {
    // Preserve the pre-existing directory/window allowance independently of
    // the actual immutable wrapper and Arc controls; it never funds the memo.
    let controls = if arc_control_bytes::<CachedPage>() > arc_control_bytes::<ChargedPage>() {
        arc_control_bytes::<CachedPage>()
    } else {
        arc_control_bytes::<ChargedPage>()
    };
    len.saturating_add(128)
        .saturating_add(2 * size_of::<usize>())
        .saturating_add(controls)
}

#[cfg(test)]
thread_local! {
    // Computations for Cached/Charged/Plain, followed by Cached/Charged hits.
    static BYTE_CHECKSUM_COUNTS: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
}

impl PageMemory {
    #[cfg(test)]
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
    pub(crate) fn memoized_byte_node_checksum(
        &self,
        compute: impl FnOnce() -> Result<u128>,
    ) -> Result<u128> {
        let slot = match self {
            Self::Cached(page) => &page.byte_node_checksum,
            Self::Charged(page) => &page.byte_node_checksum,
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
            value[if matches!(self, Self::Cached(_)) {
                0
            } else {
                1
            }] += 1;
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
            PageMemory::Charged(page) => ("charged", page.bytes.len()),
            PageMemory::Plain(bytes) => ("plain", bytes.len()),
        };
        write!(f, "PageMemory({kind}, {len} bytes)")
    }
}

impl std::ops::Deref for PageMemory {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            PageMemory::Cached(page) => &page.bytes,
            PageMemory::Charged(page) => &page.bytes,
            PageMemory::Plain(bytes) => bytes,
        }
    }
}

pub(super) struct WritablePage {
    buffer: Arc<Mutex<LRUWriteCache>>,
    offset: u64,
    data: Arc<[u8]>,
}

impl WritablePage {
    pub(super) fn mem(&self) -> &[u8] {
        &self.data
    }

    pub(super) fn mem_mut(&mut self) -> &mut [u8] {
        Arc::get_mut(&mut self.data).unwrap()
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
    cache: WriteCache<Option<Arc<[u8]>>>,
}

impl LRUWriteCache {
    fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }

    fn insert(&mut self, key: u64, value: Arc<[u8]>) {
        assert!(self.cache.insert(key, Some(value)).is_none());
    }

    fn get(&self, key: u64) -> Option<&Arc<[u8]>> {
        self.cache.get(key).map(|x| x.as_ref().unwrap())
    }

    fn remove(&mut self, key: u64) -> Option<Arc<[u8]>> {
        if let Some(value) = self.cache.remove(key) {
            assert!(value.is_some());
            return value;
        }
        None
    }

    fn return_value(&mut self, key: u64, value: Arc<[u8]>) {
        assert!(self.cache.get_mut(key).unwrap().replace(value).is_none());
    }

    fn take_value(&mut self, key: u64) -> Option<Arc<[u8]>> {
        if let Some(value) = self.cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        None
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, Arc<[u8]>)> {
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
    file: Box<dyn StorageBackend>,
    io_failed: AtomicBool,
    closed: AtomicBool,
}

impl CheckedBackend {
    fn new(file: Box<dyn StorageBackend>) -> Self {
        Self {
            file,
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
        result.map_err(StorageError::from)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.read(offset, out);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        } else {
            crate::read_admission::record_backend_read(out.len());
        }
        result.map_err(StorageError::from)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.check_failure()?;
        let result = self.file.set_len(len);
        // A caller-enforced quota is an admission refusal, not failed I/O.
        // The allocator has not published the projected layout yet, so the
        // same handle remains usable after the caller releases capacity.
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != std::io::ErrorKind::QuotaExceeded)
        {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn sync_data(&self) -> Result<()> {
        self.check_failure()?;
        let result = self.file.sync_data();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.write(offset, data);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }
}

pub(super) struct PagedCachedFile {
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
    // Bytes of every page admitted to the read cache that is still alive: in
    // the cache, or evicted while a reader still holds it. A page leaves this
    // count only when it is dropped (`CachedPage`), so it never exceeds the
    // cache budget while any such page is held.
    read_cache_bytes: Arc<AtomicUsize>,
    write_buffer_bytes: AtomicUsize,
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
    read_cache: Box<[RwLock<LRUCache<Arc<CachedPage>>>]>,
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Arc<Mutex<LRUWriteCache>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_cache_size: usize,
    ) -> Result<Self, DatabaseError> {
        let read_cache = Self::new_read_index(max_cache_size, page_size as usize, 0);

        Ok(Self {
            file: CheckedBackend::new(file),
            page_size,
            read_cache_bytes: Arc::new(AtomicUsize::new(0)),
            write_buffer_bytes: AtomicUsize::new(0),
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
            write_buffer: Arc::new(Mutex::new(LRUWriteCache::new())),
        })
    }

    fn read_index_entries(bytes: usize, page_size: usize, held_entries: usize) -> usize {
        // Only the database header can be smaller than a full page, and its
        // offset-zero key occupies at most one slot across all stripes.
        (bytes / cached_page_bytes(page_size))
            .saturating_add(1)
            .max(held_entries)
    }

    fn new_read_index(
        bytes: usize,
        page_size: usize,
        held_entries: usize,
    ) -> Box<[RwLock<LRUCache<Arc<CachedPage>>>]> {
        let entries = Self::read_index_entries(bytes, page_size, held_entries);
        let arena = EntryArena::new(entries);
        let heads = entries
            .div_ceil(Self::lock_stripes() as usize)
            .next_power_of_two();
        (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new(arena.clone(), heads)))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }

    fn uncached(
        buffer: Arc<[u8]>,
        charge: Option<crate::read_admission::ReadPageCharge>,
    ) -> PageMemory {
        match charge {
            Some(charge) => PageMemory::Charged(Arc::new(ChargedPage {
                bytes: buffer,
                _charge: charge,
                byte_node_checksum: Mutex::new(None),
            })),
            None => PageMemory::Plain(buffer),
        }
    }

    pub(crate) fn index_bytes(&self) -> usize {
        let read = self.read_cache[0].read().unwrap();
        read.arena_index_bytes()
            + read.index_bytes() * self.read_cache.len()
            + self.read_cache.len() * size_of::<RwLock<LRUCache<Arc<CachedPage>>>>()
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
        let mut locks = self
            .read_cache
            .iter()
            .map(|stripe| stripe.write().unwrap())
            .collect::<Vec<_>>();
        let held_entries = locks.iter().map(|stripe| stripe.len()).sum();
        let required_entries =
            Self::read_index_entries(bytes, self.page_size as usize, held_entries);
        if locks[0].arena_capacity() != required_entries {
            let next = Self::new_read_index(bytes, self.page_size as usize, held_entries);
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
        let num_stripes = self.read_cache.len();
        let start = self.next_eviction_stripe.fetch_add(1, Ordering::Relaxed) % num_stripes;
        let mut freed = 0;
        for i in 0..num_stripes {
            if freed >= bytes_needed {
                break;
            }
            let stripe = (start + i) % num_stripes;
            let mut lock = self.read_cache[stripe].write().unwrap();
            while freed < bytes_needed {
                if let Some((_, v)) =
                    lock.pop_lowest_priority_where(|page| Arc::strong_count(page) == 1)
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
            let cache_size = self
                .read_cache_bytes
                .fetch_add(page_bytes, Ordering::AcqRel);

            if cache_size + page_bytes <= self.max_cache_size.load(Ordering::Acquire) {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                // A race could replace an existing buffer; dropping it gives
                // back its count once no reader holds it.
                debug_assert!(
                    buffer.len() >= self.page_size as usize
                        || (*offset == 0 && buffer.len() == DB_HEADER_SIZE)
                );
                drop(lock.try_insert(
                    *offset,
                    CachedPage::admitted(buffer, &self.read_cache_bytes),
                ));
            } else {
                self.read_cache_bytes
                    .fetch_sub(page_bytes, Ordering::AcqRel);
                break;
            }
        }
        self.write_buffer_bytes.store(0, Ordering::Release);
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

    // Read directly from the file, ignoring any cached data
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.read(offset, &mut buffer)?;
        Ok(buffer)
    }

    // Like `read_direct`, but writes directly into an `Arc<[u8]>` instead of a
    // `Vec<u8>` that is then copied into an `Arc`. The buffer is zero-filled
    // because `StorageBackend::read` takes `&mut [u8]`.
    fn read_direct_into_arc(
        &self,
        offset: u64,
        len: usize,
    ) -> Result<(Arc<[u8]>, Option<crate::read_admission::ReadPageCharge>)> {
        // The page and its cache entry must be admitted before allocation,
        // including a single value larger than the configured cache.
        let charge = crate::read_admission::admit_read_memory(admitted_page_bytes(len))?;
        let mut arc = zero_filled_arc(len);
        self.file.read(offset, Arc::get_mut(&mut arc).unwrap())?;
        Ok((arc, charge))
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

        let (buffer, charge) = self.read_direct_into_arc(offset, len)?;
        let page_bytes = cached_page_bytes(len);
        if crate::read_admission::scan_reads_active()
            && !crate::read_admission::take_scan_growth(page_bytes)
        {
            // Past its growth allowance a scan's miss replaces evictable
            // cached bytes and never grows the cache. When nothing evictable
            // makes room, the page lives only while its reader holds it, with
            // any admission charge.
            let before = self.read_cache_bytes.load(Ordering::Acquire);
            {
                let lock = self.write_buffer.lock().unwrap();
                self.evict_from_read_cache(page_bytes, &lock);
            }
            let after = self.read_cache_bytes.load(Ordering::Acquire);
            if before.saturating_sub(after) < page_bytes {
                return Ok(match charge {
                    Some(charge) => PageMemory::Charged(Arc::new(ChargedPage {
                        bytes: buffer,
                        _charge: charge,
                        byte_node_checksum: Mutex::new(None),
                    })),
                    None => PageMemory::Plain(buffer),
                });
            }
            // Installed at lowest priority: the scan's next miss replaces it
            // unless it is read again, leaving the warm working set in place.
            let previous = self
                .read_cache_bytes
                .fetch_add(page_bytes, Ordering::AcqRel);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            if previous + page_bytes + write_bytes <= self.max_cache_size.load(Ordering::Acquire) {
                debug_assert!(
                    len >= self.page_size as usize || (offset == 0 && len == DB_HEADER_SIZE)
                );
                let page = CachedPage::admitted(buffer.clone(), &self.read_cache_bytes);
                let mut write_lock = self.read_cache[cache_slot].write().unwrap();
                let inserted = write_lock
                    .try_insert_lowest_priority(offset, page.clone())
                    .is_ok();
                drop(write_lock);
                if inserted {
                    drop(charge);
                    return Ok(PageMemory::Cached(page));
                }
                drop(page);
                return Ok(Self::uncached(buffer, charge));
            }
            self.read_cache_bytes
                .fetch_sub(page_bytes, Ordering::AcqRel);
            return Ok(match charge {
                Some(charge) => PageMemory::Charged(Arc::new(ChargedPage {
                    bytes: buffer,
                    _charge: charge,
                    byte_node_checksum: Mutex::new(None),
                })),
                None => PageMemory::Plain(buffer),
            });
        }

        // Rule 3: make room for the page if the total would exceed the
        // budget. Eviction frees only pages no reader still holds; a held page
        // stays counted until it is dropped.
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        let cache_size = self.read_cache_bytes.load(Ordering::Acquire);
        if cache_size + page_bytes + write_bytes > self.max_cache_size.load(Ordering::Acquire) {
            let lock = self.write_buffer.lock().unwrap();
            self.evict_from_read_cache(
                cache_size + page_bytes + write_bytes - self.max_cache_size.load(Ordering::Acquire),
                &lock,
            );
        }

        // Admit the page to the cache only if its count still fits the
        // budget, so every page the cache's count covers stays inside it.
        let previous = self
            .read_cache_bytes
            .fetch_add(page_bytes, Ordering::AcqRel);
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        if previous + page_bytes + write_bytes <= self.max_cache_size.load(Ordering::Acquire) {
            debug_assert!(
                len >= self.page_size as usize || (offset == 0 && len == DB_HEADER_SIZE)
            );
            let page = CachedPage::admitted(buffer.clone(), &self.read_cache_bytes);
            let mut write_lock = self.read_cache[cache_slot].write().unwrap();
            // A race could replace an existing buffer; dropping it gives back
            // its count once no reader holds it.
            let inserted = write_lock.try_insert(offset, page.clone()).is_ok();
            drop(write_lock);
            if inserted {
                // The native reserve owns a page only after index insertion.
                drop(charge);
                return Ok(PageMemory::Cached(page));
            }
            drop(page);
            return Ok(Self::uncached(buffer, charge));
        }
        self.read_cache_bytes
            .fetch_sub(page_bytes, Ordering::AcqRel);
        Ok(match charge {
            Some(charge) => PageMemory::Charged(Arc::new(ChargedPage {
                bytes: buffer,
                _charge: charge,
                byte_node_checksum: Mutex::new(None),
            })),
            None => PageMemory::Plain(buffer),
        })
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
        let charge = crate::read_admission::admit_read_memory(admitted_page_bytes(range.len()))?;
        let mut bytes = zero_filled_arc(range.len());
        let output = Arc::get_mut(&mut bytes).unwrap();
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
                output.copy_from_slice(&source.bytes[range.clone()]);
                copied = true;
            }
        }
        if !copied {
            self.file.read(offset + range.start as u64, output)?;
        }
        Ok(match charge {
            Some(charge) => PageMemory::Charged(Arc::new(ChargedPage {
                bytes,
                _charge: charge,
                byte_node_checksum: Mutex::new(None),
            })),
            None => PageMemory::Plain(bytes),
        })
    }

    // Discard pending writes to the given range
    pub(super) fn cancel_pending_write(&self, offset: u64, _len: usize) {
        assert_eq!(0, offset % self.page_size);
        if let Some(removed) = self.write_buffer.lock().unwrap().remove(offset) {
            self.write_buffer_bytes
                .fetch_sub(removed.len(), Ordering::Release);
        }
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

    pub(super) fn invalidate_cache_all(&self) {
        for cache_slot in 0..self.read_cache.len() {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            while lock.pop_lowest_priority().is_some() {}
        }
    }

    // If overwrite is true, the page is initialized to zero
    // cache_policy takes the existing data as an argument and returns the priority. The priority should be stable and not change after WritablePage is dropped
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);
        let mut lock = self.write_buffer.lock().unwrap();

        // TODO: allow hint that page is known to be dirty and will not be in the read cache
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let existing = {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let Some(removed) = lock.remove(offset) {
                assert_eq!(
                    len,
                    removed.len(),
                    "cache inconsistency {len} != {} for offset {offset}",
                    removed.len()
                );
                // The write buffer takes the bytes; the cache entry leaves the
                // cache's count when it is dropped here.
                Some(Arc::clone(&removed.bytes))
            } else {
                None
            }
        };

        let data = if let Some(removed) = lock.take_value(offset) {
            #[cfg(feature = "cache_metrics")]
            self.writes_hits.fetch_add(1, Ordering::AcqRel);
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            let mut write_bytes = previous + len;
            let half = self.max_cache_size.load(Ordering::Acquire) / 2;

            // Rule 1: write buffer NEVER exceeds 50%.  Flush excess to disk.
            if write_bytes > half {
                let excess = write_bytes - half;
                let mut flushed = 0;
                while flushed < excess {
                    if let Some((offset, buffer)) = lock.pop_lowest_priority() {
                        let removed_len = buffer.len();
                        let result = self.file.write(offset, &buffer);
                        if result.is_err() {
                            lock.insert(offset, buffer);
                        }
                        result?;
                        self.write_buffer_bytes
                            .fetch_sub(removed_len, Ordering::Release);
                        #[cfg(feature = "cache_metrics")]
                        {
                            self.evictions.fetch_add(1, Ordering::Relaxed);
                        }
                        flushed += removed_len;
                    } else {
                        break;
                    }
                }
                write_bytes -= flushed;
            }

            // Rules 2 + 3: after rule 1, write <= 50%.  If the total still
            // exceeds the budget then read must be > 50%, so evict from the
            // read cache (fairness: we only take from read when read > 50%).
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            if write_bytes + read_bytes > self.max_cache_size.load(Ordering::Acquire) {
                self.evict_from_read_cache(
                    write_bytes + read_bytes - self.max_cache_size.load(Ordering::Acquire),
                    &lock,
                );
            }
            let result = if let Some(data) = existing {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                data
            } else if overwrite {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                zero_filled_arc(len)
            } else {
                // A page read for writing is owned by the write buffer, never
                // by a reader, so no reader charge travels with it.
                self.read_direct_into_arc(offset, len)?.0
            };
            lock.insert(offset, result);
            lock.take_value(offset).unwrap()
        };
        #[cfg(feature = "cache_metrics")]
        self.writes_total.fetch_add(1, Ordering::AcqRel);
        Ok(WritablePage {
            buffer: self.write_buffer.clone(),
            offset,
            data,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::StorageBackend;
    use crate::backends::InMemoryBackend;
    use crate::tree_store::PageHint;
    use crate::tree_store::page_store::cached_file::{CheckedBackend, PagedCachedFile};
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
    fn scan_reads_grow_the_cache_only_within_their_allowance() {
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
            size_of::<super::CachedPage>(),
            size_of::<super::ChargedPage>(),
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
        let plain = super::PageMemory::Plain(super::zero_filled_arc(128));
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
    const UNIFORM_CACHE_SLOTS: usize = 996;

    fn uniform_cache_file(extra_pages: usize) -> PagedCachedFile {
        assert_eq!(super::cached_page_bytes(UNIFORM_PAGE_BYTES), 4208);
        assert_eq!(UNIFORM_CACHE_CAP / 4208, UNIFORM_CACHE_SLOTS);
        let backend = InMemoryBackend::new();
        backend
            .set_len(((UNIFORM_CACHE_SLOTS + extra_pages) * UNIFORM_PAGE_BYTES) as u64)
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
                pinned += usize::from(Arc::strong_count(&*page) > 1);
            }
        }
        (entries, payload, charge, pinned)
    }

    fn assert_uniform_cache_full(file: &PagedCachedFile, pinned: usize) {
        assert_eq!(
            uniform_cache_cut(file),
            (
                UNIFORM_CACHE_SLOTS,
                UNIFORM_CACHE_SLOTS * UNIFORM_PAGE_BYTES,
                UNIFORM_CACHE_SLOTS * 4208,
                pinned,
            ),
            "every funded uniform page remains in the cache map"
        );
        assert_eq!(
            file.read_cache_bytes.load(Ordering::Acquire),
            UNIFORM_CACHE_SLOTS * 4208,
            "the ledger reconciles every resident page, including pinned pages"
        );
        assert_eq!(file.write_buffer_bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            file.max_cache_size.load(Ordering::Acquire),
            UNIFORM_CACHE_CAP
        );
        assert!(UNIFORM_CACHE_SLOTS * 4208 <= UNIFORM_CACHE_CAP);
        assert!((UNIFORM_CACHE_SLOTS + 1) * 4208 > UNIFORM_CACHE_CAP);
    }

    #[test]
    fn uniform_cache_fills_its_complete_page_capacity_and_preserves_it_on_misses() {
        let misses = UNIFORM_CACHE_SLOTS * 2;
        let file = uniform_cache_file(misses);
        for ordinal in 0..UNIFORM_CACHE_SLOTS {
            let page = uniform_page(&file, ordinal);
            assert!(matches!(page, super::PageMemory::Cached(_)));
            drop(page);
        }
        assert_uniform_cache_full(&file, 0);
        for ordinal in UNIFORM_CACHE_SLOTS..UNIFORM_CACHE_SLOTS + misses {
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
        let misses = UNIFORM_CACHE_SLOTS * 2;
        let file = uniform_cache_file(misses);
        // Three branch-like pages in the first CLOCK stripe remain held
        // across more than two complete passes over every stripe's victims.
        let branch_ordinals = [0, 131, 262];
        let branches = branch_ordinals.map(|ordinal| uniform_page(&file, ordinal));
        for page in &branches {
            assert!(matches!(page, super::PageMemory::Cached(_)));
        }
        for ordinal in 0..UNIFORM_CACHE_SLOTS {
            drop(uniform_page(&file, ordinal));
        }
        assert_uniform_cache_full(&file, branches.len());
        for ordinal in UNIFORM_CACHE_SLOTS..UNIFORM_CACHE_SLOTS + misses {
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
        let holders = (0..UNIFORM_CACHE_SLOTS)
            .map(|ordinal| uniform_page(&file, ordinal))
            .collect::<Vec<_>>();
        assert_uniform_cache_full(&file, UNIFORM_CACHE_SLOTS);
        let resident_identities = || {
            let mut entries = std::collections::BTreeMap::new();
            for stripe in &file.read_cache {
                for (offset, page) in stripe.read().unwrap().iter() {
                    entries.insert(offset, Arc::as_ptr(&*page) as usize);
                }
            }
            entries
        };
        let before = resident_identities();
        #[cfg(feature = "cache_metrics")]
        let evictions = file.cache_stats().evictions;
        let charged = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for ordinal in UNIFORM_CACHE_SLOTS..UNIFORM_CACHE_SLOTS + misses {
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
            assert_uniform_cache_full(&file, UNIFORM_CACHE_SLOTS);
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
        let page = super::CachedPage::admitted(
            Arc::<[u8]>::from(vec![0; 4096]),
            &Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );
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
                                Arc<super::CachedPage>,
                            >,
                        >,
                    >();
            let construction = watch.cut();
            assert_eq!(construction[2], 0);
            assert_eq!(construction[3] - construction[4], index_bytes);
            spread_bytes[spread] = index_bytes;
            watch.reset();
            for round in 0..3_u64 {
                for ordinal in 0..996_u64 {
                    let stripe = if skewed {
                        0
                    } else {
                        ordinal as usize % stripes.len()
                    };
                    assert!(matches!(
                        stripes[stripe]
                            .write()
                            .unwrap()
                            .try_insert(round * 996 + ordinal, page.clone()),
                        Ok(None)
                    ));
                }
                assert_eq!(
                    stripes
                        .iter()
                        .map(|s| s.read().unwrap().len())
                        .sum::<usize>(),
                    996
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
        assert_eq!(super::cached_page_bytes(4096), 4208);
        assert_eq!(cap / super::cached_page_bytes(4096), 996);
        let pages = 3 * 996;
        let backend = SparseZeroFixture(std::sync::atomic::AtomicU64::new(
            (pages * 131 * 4096) as u64,
        ));
        let file = PagedCachedFile::new(Box::new(backend), 4096, cap).unwrap();
        let index_bytes = file.index_bytes();
        for ordinal in 0..pages {
            let offset = (ordinal * 131 * 4096) as u64;
            assert_eq!(offset % PagedCachedFile::lock_stripes(), 0);
            let page = file.read(offset, 4096, PageHint::Clean).unwrap();
            assert!(matches!(page, super::PageMemory::Cached(_)));
            assert!(page.iter().all(|byte| *byte == 0));
            drop(page);
            if ordinal >= 995 {
                assert_eq!(file.read_cache[0].read().unwrap().len(), 996);
                assert!(
                    file.read_cache[1..]
                        .iter()
                        .all(|s| s.read().unwrap().len() == 0)
                );
                assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 996 * 4208);
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
        let file = PagedCachedFile::new(Box::new(backend), 4096, 16 * 4208).unwrap();
        for cap in [4 * 4208, 32 * 4208, 8 * 4208] {
            assert_eq!(
                file.settle_startup_cache_budget(cap).unwrap(),
                cap + file.index_bytes()
            );
            assert_eq!(
                file.read_cache[0].read().unwrap().arena_capacity_for_test(),
                cap / super::cached_page_bytes(4096) + 1
            );
        }
        let holders = (0..8)
            .map(|ordinal| {
                file.read((ordinal * 4096) as u64, 4096, PageHint::Clean)
                    .unwrap()
            })
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
                        .map(|(key, value)| (key, Arc::as_ptr(&*value) as usize))
                        .collect::<Vec<_>>()
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let identities = before();
        let held_bytes = 8 * 4208;
        let cap = 2 * 4208;
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
            Self { len: std::sync::atomic::AtomicU64::new(len), header: std::sync::Mutex::new([0; 320]) }
        }
    }
    impl StorageBackend for WritableHeaderFixture {
        fn len(&self) -> io::Result<u64> { Ok(self.len.load(Ordering::SeqCst)) }
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
        fn set_len(&self, len: u64) -> io::Result<()> { self.len.store(len, Ordering::SeqCst); Ok(()) }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            assert!(offset + data.len() as u64 <= self.len.load(Ordering::SeqCst));
            if offset == 0 {
                let mut header = self.header.lock().unwrap();
                let count = data.len().min(header.len());
                header[..count].copy_from_slice(&data[..count]);
            }
            Ok(())
        }
        fn sync_data(&self) -> io::Result<()> { Ok(()) }
    }

    // R115: actual header writes reach the read cache through the normal flush.
    #[test]
    #[cfg(feature = "contextdb-internal")]
    fn flushed_header_and_full_pages_use_the_unchanged_cache_byte_budget() {
        let header_len = super::super::header::DB_HEADER_SIZE;
        assert_eq!(header_len, 320);
        let full_bytes = super::cached_page_bytes(4096);
        let header_bytes = super::cached_page_bytes(header_len);
        assert_eq!((full_bytes, header_bytes), (4208, 432));
        for (cap, expected_index) in [(4 * 1024 * 1024, 63_436), (64 * 1024 * 1024, 903_768)] {
            for stride in [1, 131] {
                let full_pages = cap / full_bytes;
                let backend = WritableHeaderFixture::new(
                    ((full_pages + 1) * stride * 4096) as u64,
                );
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
                    assert!(matches!(page, super::PageMemory::Cached(_)), "a byte-fitting full page must not exhaust the fixed index");
                    assert!(page.iter().all(|byte| *byte == 0));
                    drop(page);
                    assert_eq!(file.index_bytes(), index_bytes);
                }
                let resident_bytes = file.read_cache_bytes.load(Ordering::SeqCst);
                assert_eq!(resident_bytes, header_bytes + full_pages * full_bytes);
                assert_eq!(index_bytes, expected_index);
                assert!(resident_bytes <= cap);
                assert!(cap - resident_bytes < full_bytes);
                assert_eq!(file.read_cache.iter().map(|s| s.read().unwrap().len()).sum::<usize>(), full_pages + 1);
                assert_eq!(file.read_cache[0].read().unwrap().arena_capacity_for_test(), full_pages + 1);
                if stride == 131 {
                    assert_eq!(file.read_cache[0].read().unwrap().len(), full_pages + 1);
                    assert!(file.read_cache[1..].iter().all(|s| s.read().unwrap().len() == 0));
                }
                assert_eq!(file.settle_startup_cache_budget(cap).unwrap(), cap + index_bytes);
                assert_eq!(file.settle_startup_cache_budget(cap).unwrap(), cap + index_bytes);
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
                let invalid = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match route {
                        "normal" => { let _page = file.read(offset, len, PageHint::Clean).unwrap(); }
                        "scan" => crate::read_admission::with_scan_reads(|| {
                            assert!(crate::read_admission::take_scan_growth(crate::read_admission::SCAN_GROWTH_ALLOWANCE));
                            let _page = file.read(offset, len, PageHint::Clean).unwrap();
                        }),
                        "flush" => {
                            let mut page = file.write(offset, len, true).unwrap();
                            page.mem_mut().fill(0x7a);
                            drop(page);
                            file.flush().unwrap();
                        }
                        _ => unreachable!(),
                    }
                }));
                let panic = invalid.expect_err("an invalid subpage admission must panic");
                let message = panic.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("");
                assert!(message.contains("DB_HEADER_SIZE"), "route {route} offset {offset} length {len} must fail its actual header invariant, not a fixture panic: {message}");
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

#[cfg(test)]
impl PagedCachedFile {
    pub(crate) fn write_map_for_test(&self) -> (usize, usize) {
        let write = self.write_buffer.lock().unwrap();
        (write.cache.len(), write.cache.map_capacity_for_test())
    }
}
