use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::lru_cache::LRUCache;
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
}

impl CachedPage {
    /// `counted` must already include this page's length.
    fn admitted(bytes: Arc<[u8]>, counted: &Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            counted: Arc::clone(counted),
        })
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl Drop for CachedPage {
    fn drop(&mut self) {
        self.counted.fetch_sub(self.bytes.len(), Ordering::AcqRel);
    }
}

pub(crate) struct ChargedPage {
    bytes: Arc<[u8]>,
    _charge: crate::read_admission::ReadPageCharge,
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
    cache: LRUCache<Option<Arc<[u8]>>>,
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
    max_cache_size: usize,
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
    read_cache: Vec<RwLock<LRUCache<Arc<CachedPage>>>>,
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Arc<Mutex<LRUWriteCache>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_cache_size: usize,
    ) -> Result<Self, DatabaseError> {
        let read_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new()))
            .collect();

        Ok(Self {
            file: CheckedBackend::new(file),
            page_size,
            read_cache_bytes: Arc::new(AtomicUsize::new(0)),
            write_buffer_bytes: AtomicUsize::new(0),
            max_cache_size,
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
                if let Some((_, v)) = lock.pop_lowest_priority() {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    // A page a reader still holds is not freed by leaving the
                    // cache; it stays counted until that reader drops it.
                    if Arc::strong_count(&v) == 1 {
                        freed += v.len();
                    }
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
            let cache_size = self
                .read_cache_bytes
                .fetch_add(buffer.len(), Ordering::AcqRel);

            if cache_size + buffer.len() <= self.max_cache_size {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                // A race could replace an existing buffer; dropping it gives
                // back its count once no reader holds it.
                drop(lock.insert(
                    *offset,
                    CachedPage::admitted(buffer, &self.read_cache_bytes),
                ));
            } else {
                self.read_cache_bytes
                    .fetch_sub(buffer.len(), Ordering::AcqRel);
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
                    .find(|(offset, page)| **offset + page.len() as u64 > len)
                    .map(|(offset, _)| *offset);
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
        let charge = crate::read_admission::admit_read_memory(len.saturating_add(128))?;
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

        // Rule 3: make room for the page if the total would exceed the
        // budget. Eviction frees only pages no reader still holds; a held page
        // stays counted until it is dropped.
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        let cache_size = self.read_cache_bytes.load(Ordering::Acquire);
        if cache_size + len + write_bytes > self.max_cache_size {
            let lock = self.write_buffer.lock().unwrap();
            self.evict_from_read_cache(cache_size + len + write_bytes - self.max_cache_size, &lock);
        }

        // Admit the page to the cache only if its count still fits the
        // budget, so every page the cache's count covers stays inside it.
        let previous = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        if previous + len + write_bytes <= self.max_cache_size {
            let page = CachedPage::admitted(buffer, &self.read_cache_bytes);
            let mut write_lock = self.read_cache[cache_slot].write().unwrap();
            // A race could replace an existing buffer; dropping it gives back
            // its count once no reader holds it.
            drop(write_lock.insert(offset, page.clone()));
            drop(write_lock);
            // The cache's budget pays for the page from here on, so any
            // reader charge is given back at once.
            drop(charge);
            return Ok(PageMemory::Cached(page));
        }
        self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
        Ok(match charge {
            Some(charge) => PageMemory::Charged(Arc::new(ChargedPage {
                bytes: buffer,
                _charge: charge,
            })),
            None => PageMemory::Plain(buffer),
        })
    }

    // A reject-only window never enters the full-page cache. Its admission
    // remains owned until the window is released, including cached sources.
    #[cfg(feature = "contextdb-internal")]
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
        let charge = crate::read_admission::admit_read_memory(range.len().saturating_add(128))?;
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
            let half = self.max_cache_size / 2;

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
            if write_bytes + read_bytes > self.max_cache_size {
                self.evict_from_read_cache(write_bytes + read_bytes - self.max_cache_size, &lock);
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
}
