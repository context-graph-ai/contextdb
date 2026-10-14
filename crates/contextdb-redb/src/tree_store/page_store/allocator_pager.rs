//! Windowed, non-durable backing for allocator bitmap words.
//!
//! The window set is an allotment of the writer's page-cache share: it holds
//! `capacity` windows of `WINDOW_BYTES`, where `capacity` follows the declared
//! budget (`capacity_for_cache`) and never the file's region count. A store
//! whose live bitmap fits the allotment keeps every touched word resident; a
//! larger one pages windows through scratch. A window is written to scratch
//! before reuse. The caller supplies committed or virtual base words when a
//! scratch record is absent.

use crate::{ScratchBackend, StorageError};
use std::io;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// The paging unit. BAKED-IN: it is the scratch record geometry and the
/// saved-source read granularity, not a workload preference.
pub(super) const WINDOW_BYTES: usize = 4096;
/// The window set every pager keeps whatever its budget: the former fixed
/// allotment, so a store under the smallest budget pages exactly as before.
pub(super) const WINDOWS: usize = 32;
/// One byte in this many of the writer page-cache share funds allocator
/// windows above `WINDOWS`. The bytes are carved out of that already-charged
/// share, so the declared budget covers them without a second charge.
pub(super) const CACHE_SHARE_DIVISOR: usize = 32;
const RECORD_LEN: usize = WINDOW_BYTES + 8;
const RECORD_BYTES: u64 = RECORD_LEN as u64;
const PAGES_PER_ARRAY: u64 = 32;
const HEIGHTS_PER_BITMAP: u64 = 4;
const ORDERS_PER_ENTITY: u64 = 21;
/// Upper bound on the identity index's bytes per window: the open-addressed
/// table holds at most 4 buckets per admitted window (twice the window count,
/// rounded up to a power of two), each an identity and a slot, plus one
/// vacant-slot entry.
const INDEX_BYTES_PER_WINDOW: usize =
    4 * (size_of::<u64>() + size_of::<u32>()) + size_of::<usize>();

pub(super) fn bitmap_array_base(region: Option<u32>, order: u8) -> u64 {
    let entity = region.map_or(0, |region| u64::from(region) + 1);
    (entity * ORDERS_PER_ENTITY + u64::from(order)) * HEIGHTS_PER_BITMAP
}

type BaseLoader = dyn Fn(u64, u32, &mut [u8; WINDOW_BYTES]) -> io::Result<bool> + Send + Sync;

struct Window {
    identity: u64,
    bytes: [u8; WINDOW_BYTES],
    dirty: bool,
}

impl Window {
    fn empty() -> Box<Self> {
        Box::new(Self {
            identity: 0,
            bytes: [0; WINDOW_BYTES],
            dirty: false,
        })
    }
}

/// Window identity to slot, in a table allocated once per admitted window
/// count. Open addressing with linear probing and backward-shift deletion
/// keeps it at most half full and never tombstoned, so evict/insert churn
/// cannot grow or reallocate it. Identity 0 marks an empty bucket.
struct WindowIndex {
    identities: Box<[u64]>,
    slots: Box<[u32]>,
}

impl WindowIndex {
    fn for_windows(windows: usize) -> Self {
        let buckets = (2 * windows).next_power_of_two();
        Self {
            identities: vec![0; buckets].into_boxed_slice(),
            slots: vec![0; buckets].into_boxed_slice(),
        }
    }

    fn mask(&self) -> usize {
        self.identities.len() - 1
    }

    fn home(&self, identity: u64) -> usize {
        // Fibonacci hashing spreads the arithmetic identities over buckets.
        let mixed = identity.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        usize::try_from(mixed >> 32).unwrap_or(usize::MAX) & self.mask()
    }

    fn find(&self, identity: u64) -> Option<usize> {
        let mut bucket = self.home(identity);
        loop {
            match self.identities[bucket] {
                0 => return None,
                found if found == identity => return Some(bucket),
                _ => bucket = (bucket + 1) & self.mask(),
            }
        }
    }

    fn get(&self, identity: u64) -> Option<usize> {
        self.find(identity)
            .map(|bucket| self.slots[bucket] as usize)
    }

    fn insert(&mut self, identity: u64, slot: usize) {
        debug_assert_ne!(identity, 0);
        let mut bucket = self.home(identity);
        while self.identities[bucket] != 0 && self.identities[bucket] != identity {
            bucket = (bucket + 1) & self.mask();
        }
        self.identities[bucket] = identity;
        self.slots[bucket] = u32::try_from(slot).expect("window slot fits the index");
    }

    fn remove(&mut self, identity: u64) {
        if identity == 0 {
            return;
        }
        let Some(mut hole) = self.find(identity) else {
            return;
        };
        // Backward shift: pull each later member of the probe run into the
        // hole unless its home lies cyclically after the hole.
        let mut next = hole;
        loop {
            next = (next + 1) & self.mask();
            let moved = self.identities[next];
            if moved == 0 {
                break;
            }
            let home = self.home(moved);
            let stays = if hole <= next {
                hole < home && home <= next
            } else {
                hole < home || home <= next
            };
            if !stays {
                self.identities[hole] = moved;
                self.slots[hole] = self.slots[next];
                hole = next;
            }
        }
        self.identities[hole] = 0;
    }

    fn clear(&mut self) {
        self.identities.fill(0);
    }
}

struct Inner {
    scratch: Box<dyn ScratchBackend>,
    // Exactly `capacity` windows, all allocated when that count is admitted,
    // so no read or write ever allocates a window.
    windows: Vec<Box<Window>>,
    index: WindowIndex,
    // Slots holding no window identity, reused before any slot is created.
    vacant: Vec<usize>,
    capacity: usize,
    next_victim: usize,
    source: Option<Box<BaseLoader>>,
}

impl Inner {
    fn spill(&self, window: &Window) -> io::Result<()> {
        let offset = (window.identity - 1)
            .checked_mul(RECORD_BYTES)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "scratch offset overflow")
            })?;
        self.scratch.write(offset, &window.identity.to_le_bytes())?;
        self.scratch.write(offset + 8, &window.bytes)
    }
}

/// A scratch I/O fault is sticky: callers must refuse publication and skip
/// clean-header writes after this bit becomes set.
pub(super) struct AllocatorPager {
    inner: Mutex<Inner>,
    poisoned: AtomicBool,
    first_failure: Mutex<Option<(io::ErrorKind, String)>>,
    #[cfg(test)]
    counters: PagerCounters,
}

/// Test-only work counters: words read, window loads after a miss, and
/// scratch read calls those loads issued.
#[cfg(test)]
#[derive(Default)]
pub(super) struct PagerCounters {
    pub(super) word_reads: std::sync::atomic::AtomicU64,
    pub(super) window_loads: std::sync::atomic::AtomicU64,
    pub(super) scratch_reads: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
impl PagerCounters {
    fn bump(counter: &std::sync::atomic::AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

impl AllocatorPager {
    pub(super) fn flush_and_detach_source(
        &self,
        virtual_source: Box<BaseLoader>,
    ) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        for slot in 0..inner.windows.len() {
            if inner.windows[slot].dirty {
                inner
                    .spill(&inner.windows[slot])
                    .inspect_err(|error| self.poison(error))?;
                inner.windows[slot].dirty = false;
            }
        }
        // The durable tree may be reclaimed after this point. Keep the virtual
        // initializer for regions created later, while retaining spill records.
        inner.source = Some(virtual_source);
        Ok(())
    }
    pub(super) fn new(scratch: Box<dyn ScratchBackend>) -> Arc<Self> {
        let mut windows = Vec::with_capacity(WINDOWS);
        windows.resize_with(WINDOWS, Window::empty);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                scratch,
                windows,
                index: WindowIndex::for_windows(WINDOWS),
                vacant: (0..WINDOWS).rev().collect(),
                capacity: WINDOWS,
                next_victim: 0,
                source: None,
            }),
            poisoned: AtomicBool::new(false),
            first_failure: Mutex::new(None),
            #[cfg(test)]
            counters: PagerCounters::default(),
        })
    }

    #[cfg(test)]
    pub(super) fn counters_for_test(&self) -> (u64, u64, u64) {
        (
            self.counters.word_reads.load(Ordering::Relaxed),
            self.counters.window_loads.load(Ordering::Relaxed),
            self.counters.scratch_reads.load(Ordering::Relaxed),
        )
    }

    fn bytes_per_window() -> usize {
        size_of::<Window>() + size_of::<Box<Window>>() + INDEX_BYTES_PER_WINDOW
    }

    /// Windows a writer page-cache share of `cache_bytes` funds.
    pub(super) fn capacity_for_cache(cache_bytes: usize) -> usize {
        (cache_bytes / CACHE_SHARE_DIVISOR / Self::bytes_per_window()).max(WINDOWS)
    }

    /// Bytes of a `cache_bytes` share the window set takes above the
    /// `WINDOWS` every pager holds; the page cache gives these up.
    pub(super) fn cache_bytes_taken(cache_bytes: usize) -> usize {
        (Self::capacity_for_cache(cache_bytes) - WINDOWS) * Self::bytes_per_window()
    }

    /// Admit the window set a `cache_bytes` share funds. A smaller set spills
    /// its surplus dirty windows to scratch before releasing them.
    pub(super) fn set_cache_budget(&self, cache_bytes: usize) -> io::Result<()> {
        let capacity = Self::capacity_for_cache(cache_bytes);
        let mut inner = self.inner.lock().unwrap();
        if capacity == inner.capacity {
            return Ok(());
        }
        let mut released = Vec::new();
        if capacity < inner.windows.len() {
            // Unlink the surplus before any spill can fail, so no index or
            // vacant entry ever names a slot past the retained windows.
            released = inner.windows.split_off(capacity);
            inner.vacant.retain(|slot| *slot < capacity);
            inner.next_victim %= capacity;
        }
        inner.capacity = capacity;
        let missing = capacity - inner.windows.len();
        inner.windows.reserve_exact(missing);
        inner.windows.shrink_to(capacity);
        let missing = capacity.saturating_sub(inner.vacant.len());
        inner.vacant.reserve_exact(missing);
        inner.vacant.shrink_to(capacity);
        // The admitted set is allocated now, at admission, never during the
        // operation whose allocation first touches a new window.
        while inner.windows.len() < capacity {
            let slot = inner.windows.len();
            inner.windows.push(Window::empty());
            inner.vacant.push(slot);
        }
        // The index is sized once per admitted count and rebuilt from the
        // retained windows; churn under this count never reallocates it.
        let mut index = WindowIndex::for_windows(capacity);
        for (slot, window) in inner.windows.iter().enumerate() {
            if window.identity != 0 {
                index.insert(window.identity, slot);
            }
        }
        inner.index = index;
        for window in released.iter().filter(|window| window.dirty) {
            inner
                .spill(window)
                .inspect_err(|error| self.poison(error))?;
        }
        Ok(())
    }

    /// Upper bound on the pager's resident bytes: the admitted window set,
    /// its identity index and the fixed control allocation.
    pub(super) fn resident_bytes(&self) -> usize {
        self.inner.lock().unwrap().capacity * Self::bytes_per_window() + size_of::<Self>()
    }

    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub(super) fn storage_failure(&self) -> Option<StorageError> {
        if !self.is_poisoned() {
            return None;
        }
        Some(
            self.io_failure()
                .map_or(StorageError::PreviousIo, StorageError::Io),
        )
    }

    pub(super) fn io_failure(&self) -> Option<io::Error> {
        self.first_failure
            .lock()
            .unwrap()
            .as_ref()
            .map(|(kind, message)| io::Error::new(*kind, message.clone()))
    }

    pub(super) fn set_source(&self, source: Option<Box<BaseLoader>>) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .scratch
            .set_len(0)
            .inspect_err(|error| self.poison(error))?;
        inner.windows.iter_mut().for_each(|window| {
            window.identity = 0;
            window.dirty = false;
        });
        inner.index.clear();
        let slots = inner.windows.len();
        inner.vacant.clear();
        inner.vacant.extend((0..slots).rev());
        inner.source = source;
        Ok(())
    }

    pub(super) fn truncate_regions(&self, retained: u32) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let first_removed_array =
            (u64::from(retained) + 1) * ORDERS_PER_ENTITY * HEIGHTS_PER_BITMAP;
        let first_removed_key = first_removed_array * PAGES_PER_ARRAY + 1;
        let Inner {
            windows,
            index,
            vacant,
            ..
        } = &mut *inner;
        for (slot, window) in windows.iter_mut().enumerate() {
            if window.identity >= first_removed_key {
                index.remove(window.identity);
                vacant.push(slot);
                window.identity = 0;
                window.dirty = false;
            }
        }
        let limit = (first_removed_key - 1) * RECORD_BYTES;
        if inner
            .scratch
            .len()
            .inspect_err(|error| self.poison(error))?
            > limit
        {
            inner
                .scratch
                .set_len(limit)
                .inspect_err(|error| self.poison(error))?;
        }
        Ok(())
    }

    pub(super) fn read_word(&self, array: u64, word: usize, len: u32, optimistic: bool) -> u64 {
        self.try_read_word(array, word, len, optimistic)
            .unwrap_or_else(|error| {
                self.poison(&error);
                u64::MAX
            })
    }

    pub(super) fn write_word(
        &self,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
        value: u64,
    ) {
        if let Err(error) = self.try_write_word(array, word, len, optimistic, value) {
            self.poison(&error);
        }
    }

    fn try_read_word(
        &self,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
    ) -> io::Result<u64> {
        #[cfg(test)]
        PagerCounters::bump(&self.counters.word_reads);
        let mut inner = self.inner.lock().unwrap();
        let slot = self.find_window(&mut inner, array, word, len, optimistic)?;
        let start = word % (WINDOW_BYTES / 8) * 8;
        Ok(u64::from_le_bytes(
            inner.windows[slot].bytes[start..start + 8]
                .try_into()
                .unwrap(),
        ))
    }

    fn try_write_word(
        &self,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
        value: u64,
    ) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let slot = self.find_window(&mut inner, array, word, len, optimistic)?;
        let start = word % (WINDOW_BYTES / 8) * 8;
        inner.windows[slot].bytes[start..start + 8].copy_from_slice(&value.to_le_bytes());
        inner.windows[slot].dirty = true;
        Ok(())
    }

    #[cfg_attr(not(test), expect(clippy::unused_self))]
    fn find_window(
        &self,
        inner: &mut Inner,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
    ) -> io::Result<usize> {
        let page: u32 = (word / (WINDOW_BYTES / 8)).try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocator word index overflow")
        })?;
        if u64::from(page) >= PAGES_PER_ARRAY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "allocator bitmap exceeds fixed scratch stride",
            ));
        }
        let identity = array
            .checked_mul(PAGES_PER_ARRAY)
            .and_then(|base| base.checked_add(u64::from(page)))
            .and_then(|key| key.checked_add(1))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "allocator key overflow"))?;
        if let Some(slot) = inner.index.get(identity) {
            return Ok(slot);
        }

        #[cfg(test)]
        PagerCounters::bump(&self.counters.window_loads);
        let victim = if let Some(slot) = inner.vacant.pop() {
            slot
        } else {
            // Source-only words can be reloaded without spilling. Keep dirty
            // words in the same owner while any clean window can be reused.
            let windows = inner.windows.len();
            let victim = (0..windows)
                .map(|offset| (inner.next_victim + offset) % windows)
                .find(|index| !inner.windows[*index].dirty)
                .unwrap_or(inner.next_victim);
            inner.next_victim = (victim + 1) % windows;
            victim
        };
        if inner.windows[victim].dirty {
            inner.spill(&inner.windows[victim])?;
            inner.windows[victim].dirty = false;
        }
        let evicted = inner.windows[victim].identity;
        if evicted != 0 {
            inner.index.remove(evicted);
            inner.windows[victim].identity = 0;
        }

        let offset = (identity - 1).checked_mul(RECORD_BYTES).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "scratch offset overflow")
        })?;
        // One read returns the record's identity and its words together.
        let mut record = [0u8; RECORD_LEN];
        let stored = if inner.scratch.len()? >= offset + RECORD_BYTES {
            #[cfg(test)]
            PagerCounters::bump(&self.counters.scratch_reads);
            inner.scratch.read(offset, &mut record)?;
            u64::from_le_bytes(record[..8].try_into().unwrap()) == identity
        } else {
            false
        };
        let Inner {
            windows, source, ..
        } = &mut *inner;
        let prior = &mut windows[victim];
        if stored {
            prior.bytes.copy_from_slice(&record[8..]);
        } else {
            let loaded = if let Some(source) = source {
                source(array, page, &mut prior.bytes)?
            } else {
                false
            };
            if !loaded {
                prior.bytes.fill(0xFF);
                if optimistic {
                    for (index, chunk) in prior.bytes.chunks_exact_mut(8).enumerate() {
                        let first =
                            (usize::try_from(page).unwrap() * WINDOW_BYTES / 8 + index) * 64;
                        let free_bits = (len as usize).saturating_sub(first).min(64);
                        let word = if free_bits == 64 {
                            0
                        } else {
                            u64::MAX << free_bits
                        };
                        chunk.copy_from_slice(&word.to_le_bytes());
                    }
                }
            }
        }
        prior.identity = identity;
        inner.index.insert(identity, victim);
        Ok(victim)
    }

    fn poison(&self, error: &io::Error) {
        let mut first = self.first_failure.lock().unwrap();
        if first.is_none() {
            *first = Some((error.kind(), error.to_string()));
        }
        self.poisoned.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    use super::{AllocatorPager, WINDOWS};
    use crate::ScratchBackend;
    use crate::tree_store::page_store::lru_cache::arena_index_tests::Watch;
    use std::io;

    /// Scratch that stores nothing and allocates nothing, so a churn
    /// measurement sees only the pager's own allocations.
    #[derive(Debug)]
    struct NullScratch {
        fail_writes: bool,
    }

    impl ScratchBackend for NullScratch {
        fn len(&self) -> io::Result<u64> {
            Ok(0)
        }
        fn read(&self, _offset: u64, _out: &mut [u8]) -> io::Result<()> {
            Ok(())
        }
        fn write(&self, _offset: u64, _data: &[u8]) -> io::Result<()> {
            if self.fail_writes {
                return Err(io::Error::other("injected scratch write failure"));
            }
            Ok(())
        }
        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }
    }

    const CHURN_CACHE: usize = 16 << 20;

    /// Endless evict/insert churn through a full budgeted window set, clean
    /// and dirty, allocates and frees nothing: the identity index and the
    /// window slots keep the footprint the admitted count fixed.
    #[test]
    fn window_churn_allocates_nothing() {
        let pager = AllocatorPager::new(Box::new(NullScratch { fail_writes: false }));
        pager.set_cache_budget(CHURN_CACHE).unwrap();
        let capacity = AllocatorPager::capacity_for_cache(CHURN_CACHE) as u64;
        assert!(capacity > 2 * WINDOWS as u64);
        for array in 0..capacity {
            pager.read_word(array, 0, u32::MAX, false);
        }
        let resident = pager.resident_bytes();
        let (_, loads, _) = pager.counters_for_test();
        let watch = Watch::start();
        for array in capacity..capacity + 100_000 {
            if array.is_multiple_of(2) {
                pager.write_word(array, 0, u32::MAX, false, array);
            } else {
                assert_eq!(pager.read_word(array, 0, u32::MAX, false), u64::MAX);
            }
        }
        let cut = watch.cut();
        drop(watch);
        assert_eq!(
            cut, [0; 5],
            "100000 window replacements made [allocs, frees, reallocs, bytes, freed] {cut:?}"
        );
        let (_, loads_after, _) = pager.counters_for_test();
        assert_eq!(loads_after - loads, 100_000);
        assert_eq!(pager.resident_bytes(), resident);
        assert!(!pager.is_poisoned());

        // Shrinking afterwards keeps every retained window reachable.
        pager.set_cache_budget(0).unwrap();
        let floor = AllocatorPager::new(Box::new(NullScratch { fail_writes: false }));
        assert_eq!(pager.resident_bytes(), floor.resident_bytes());
        for array in capacity + 100_000 - 64..capacity + 100_000 {
            pager.read_word(array, 0, u32::MAX, false);
        }
        assert!(!pager.is_poisoned());
    }

    /// A scratch fault while a smaller budget releases dirty surplus windows
    /// poisons the pager, and every window identity stays answerable.
    #[test]
    fn a_scratch_fault_releasing_surplus_windows_poisons_without_dangling_slots() {
        let pager = AllocatorPager::new(Box::new(NullScratch { fail_writes: true }));
        pager.set_cache_budget(CHURN_CACHE).unwrap();
        let dirty = 3 * WINDOWS as u64;
        assert!(AllocatorPager::capacity_for_cache(CHURN_CACHE) as u64 > dirty);
        for array in 0..dirty {
            pager.write_word(array, 0, u32::MAX, false, array);
        }
        assert!(
            !pager.is_poisoned(),
            "the budgeted set held every dirty window"
        );
        assert!(pager.set_cache_budget(0).is_err());
        assert!(pager.is_poisoned());
        for array in 0..dirty {
            pager.read_word(array, 0, u32::MAX, false);
        }
    }

    /// Reloading a spilled window is one scratch read of its record.
    #[test]
    fn reloading_a_spilled_window_is_one_scratch_read() {
        let scratch = crate::scratch_backend_in_directory(&std::env::temp_dir()).unwrap();
        let pager = AllocatorPager::new(scratch);
        // One more dirty window than the pager holds spills the first.
        for array in 0..=WINDOWS {
            pager.write_word(array as u64, 0, u32::MAX, false, array as u64);
        }
        let (_, loads, reads) = pager.counters_for_test();
        assert_eq!(pager.read_word(0, 0, u32::MAX, false), 0);
        let (_, loads_after, reads_after) = pager.counters_for_test();
        assert_eq!(
            loads_after - loads,
            1,
            "the spilled window is reloaded once"
        );
        assert_eq!(
            reads_after - reads,
            1,
            "a spilled window reload issued {} scratch reads",
            reads_after - reads
        );
        assert!(!pager.is_poisoned());
    }
}
