use crate::tree_store::page_store::fast_hash::FastHashMapU64;
use crate::tree_store::page_store::fast_hash::FastHasher64;
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

const NONE: u32 = u32::MAX;

struct Entry<T> {
    key: AtomicU64,
    second_chance: AtomicBool,
    previous: AtomicU32,
    next: AtomicU32,
    chain: AtomicU32,
    value: RwLock<Option<T>>,
}

struct FreeSlots {
    slots: Box<[u32]>,
    len: usize,
}

pub(crate) struct EntryArena<T> {
    entries: Box<[Entry<T>]>,
    free: Mutex<FreeSlots>,
}

impl<T> EntryArena<T> {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity < NONE as usize);
        let entries = (0..capacity)
            .map(|_| Entry {
                key: AtomicU64::new(0),
                second_chance: AtomicBool::new(false),
                previous: AtomicU32::new(NONE),
                next: AtomicU32::new(NONE),
                chain: AtomicU32::new(NONE),
                value: RwLock::new(None),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let slots = (0..capacity as u32)
            .rev()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            entries,
            free: Mutex::new(FreeSlots {
                slots,
                len: capacity,
            }),
        })
    }

    pub(crate) fn index_bytes(&self) -> usize {
        let header = 2 * size_of::<usize>();
        header.next_multiple_of(align_of::<Self>())
            + size_of::<Self>()
            + self.entries.len() * size_of::<Entry<T>>()
            + self.entries.len() * size_of::<u32>()
    }

    fn take(&self) -> Option<u32> {
        let mut free = self.free.lock().unwrap();
        if free.len == 0 {
            return None;
        }
        free.len -= 1;
        Some(free.slots[free.len])
    }

    fn release(&self, slot: u32) {
        let mut free = self.free.lock().unwrap();
        let index = free.len;
        free.slots[index] = slot;
        free.len += 1;
    }
}

pub(crate) struct ValueRef<'a, T>(RwLockReadGuard<'a, Option<T>>);
impl<T> Deref for ValueRef<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.as_ref().unwrap()
    }
}
// Each stripe owns its chain heads and CLOCK order; slots are lent globally.
pub struct LRUCache<T> {
    arena: Arc<EntryArena<T>>,
    heads: Box<[u32]>,
    first: u32,
    last: u32,
    len: usize,
}

impl<T> LRUCache<T> {
    pub(crate) fn new(arena: Arc<EntryArena<T>>, head_count: usize) -> Self {
        assert!(head_count.is_power_of_two());
        Self {
            arena,
            heads: vec![NONE; head_count].into_boxed_slice(),
            first: NONE,
            last: NONE,
            len: 0,
        }
    }

    pub(crate) fn arena_capacity(&self) -> usize {
        self.arena.entries.len()
    }

    pub(crate) fn arena_index_bytes(&self) -> usize {
        self.arena.index_bytes()
    }
    pub(crate) fn index_bytes(&self) -> usize {
        self.heads.len() * size_of::<u32>()
    }
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn bucket(&self, key: u64) -> usize {
        let mut hasher = FastHasher64::default();
        hasher.write_u64(key);
        hasher.finish() as usize & (self.heads.len() - 1)
    }

    fn find(&self, key: u64) -> Option<u32> {
        let mut slot = self.heads[self.bucket(key)];
        while slot != NONE {
            let entry = &self.arena.entries[slot as usize];
            if entry.key.load(Ordering::Relaxed) == key {
                return Some(slot);
            }
            slot = entry.chain.load(Ordering::Relaxed);
        }
        None
    }

    fn insert_at(&mut self, key: u64, value: T, lowest: bool) -> Result<Option<T>, T> {
        if let Some(slot) = self.find(key) {
            let entry = &self.arena.entries[slot as usize];
            entry.second_chance.store(false, Ordering::Release);
            return Ok(entry.value.write().unwrap().replace(value));
        }
        let Some(slot) = self.arena.take() else {
            return Err(value);
        };
        let bucket = self.bucket(key);
        let (previous, next) = if lowest {
            (NONE, self.first)
        } else {
            (self.last, NONE)
        };
        let entry = &self.arena.entries[slot as usize];
        entry.key.store(key, Ordering::Relaxed);
        entry.second_chance.store(false, Ordering::Release);
        entry.previous.store(previous, Ordering::Relaxed);
        entry.next.store(next, Ordering::Relaxed);
        entry.chain.store(self.heads[bucket], Ordering::Relaxed);
        *entry.value.write().unwrap() = Some(value);
        self.heads[bucket] = slot;
        if previous != NONE {
            self.arena.entries[previous as usize]
                .next
                .store(slot, Ordering::Relaxed);
        } else {
            self.first = slot;
        }
        if next != NONE {
            self.arena.entries[next as usize]
                .previous
                .store(slot, Ordering::Relaxed);
        } else {
            self.last = slot;
        }
        self.len += 1;
        Ok(None)
    }

    pub(crate) fn try_insert(&mut self, key: u64, value: T) -> Result<Option<T>, T> {
        self.insert_at(key, value, false)
    }
    pub(crate) fn try_insert_lowest_priority(
        &mut self,
        key: u64,
        value: T,
    ) -> Result<Option<T>, T> {
        self.insert_at(key, value, true)
    }
    #[cfg(test)]
    pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
        match self.try_insert(key, value) {
            Ok(old) => old,
            Err(_) => panic!("fixed read index capacity exceeded"),
        }
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
        let bucket = self.bucket(key);
        let mut slot = self.heads[bucket];
        let mut chain_previous = NONE;
        while slot != NONE {
            let entry = &self.arena.entries[slot as usize];
            if entry.key.load(Ordering::Relaxed) == key {
                break;
            }
            chain_previous = slot;
            slot = entry.chain.load(Ordering::Relaxed);
        }
        if slot == NONE {
            return None;
        }
        let entry = &self.arena.entries[slot as usize];
        let chain = entry.chain.load(Ordering::Relaxed);
        if chain_previous == NONE {
            self.heads[bucket] = chain;
        } else {
            self.arena.entries[chain_previous as usize]
                .chain
                .store(chain, Ordering::Relaxed);
        }
        let previous = entry.previous.load(Ordering::Relaxed);
        let next = entry.next.load(Ordering::Relaxed);
        if previous != NONE {
            self.arena.entries[previous as usize]
                .next
                .store(next, Ordering::Relaxed);
        } else {
            self.first = next;
        }
        if next != NONE {
            self.arena.entries[next as usize]
                .previous
                .store(previous, Ordering::Relaxed);
        } else {
            self.last = previous;
        }
        let value = entry.value.write().unwrap().take().unwrap();
        self.len -= 1;
        self.arena.release(slot);
        Some(value)
    }

    pub(crate) fn get(&self, key: u64) -> Option<ValueRef<'_, T>> {
        let entry = &self.arena.entries[self.find(key)? as usize];
        entry.second_chance.store(true, Ordering::Release);
        Some(ValueRef(entry.value.read().unwrap()))
    }
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (u64, ValueRef<'_, T>)> {
        let mut slot = self.first;
        let arena = &self.arena;
        (0..self.len).map(move |_| {
            let entry = &arena.entries[slot as usize];
            slot = entry.next.load(Ordering::Relaxed);
            (
                entry.key.load(Ordering::Relaxed),
                ValueRef(entry.value.read().unwrap()),
            )
        })
    }
    pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
        self.pop_lowest_priority_where(|_| true)
    }
    pub(crate) fn pop_lowest_priority_where(
        &mut self,
        mut eligible: impl FnMut(&T) -> bool,
    ) -> Option<(u64, T)> {
        for _ in 0..self.len.saturating_mul(2) {
            if self.first == NONE {
                return None;
            }
            let slot = self.first;
            let entry = &self.arena.entries[slot as usize];
            let key = entry.key.load(Ordering::Relaxed);
            if !entry.second_chance.swap(false, Ordering::AcqRel)
                && eligible(entry.value.read().unwrap().as_ref().unwrap())
            {
                return self.remove(key).map(|value| (key, value));
            }
            let next = entry.next.load(Ordering::Relaxed);
            if next != NONE {
                self.arena.entries[next as usize]
                    .previous
                    .store(NONE, Ordering::Relaxed);
                self.arena.entries[self.last as usize]
                    .next
                    .store(slot, Ordering::Relaxed);
                entry.previous.store(self.last, Ordering::Relaxed);
                entry.next.store(NONE, Ordering::Relaxed);
                self.first = next;
                self.last = slot;
            }
        }
        None
    }
    pub(crate) fn transfer_to(&mut self, destination: &mut Self) {
        while self.first != NONE {
            let entry = &self.arena.entries[self.first as usize];
            let key = entry.key.load(Ordering::Relaxed);
            let chance = entry.second_chance.load(Ordering::Acquire);
            let value = self.remove(key).unwrap();
            assert!(destination.try_insert(key, value).is_ok());
            let slot = destination.find(key).unwrap();
            destination.arena.entries[slot as usize]
                .second_chance
                .store(chance, Ordering::Release);
        }
    }

    pub(crate) fn clear(&mut self) {
        while self.first != NONE {
            let key = self.arena.entries[self.first as usize]
                .key
                .load(Ordering::Relaxed);
            drop(self.remove(key));
        }
    }
}
impl<T> Drop for LRUCache<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

struct WriteEntry<T> {
    value: T,
    second_chance: AtomicBool,
    previous: Option<u64>,
    next: Option<u64>,
}

pub struct WriteCache<T> {
    cache: FastHashMapU64<WriteEntry<T>>,
    first: Option<u64>,
    last: Option<u64>,
}

impl<T> Default for WriteCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> WriteCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: Default::default(),
            first: None,
            last: None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    fn insert_at(&mut self, key: u64, value: T, lowest: bool) -> Option<T> {
        if let Some(entry) = self.cache.get_mut(&key) {
            entry.second_chance.store(false, Ordering::Release);
            return Some(std::mem::replace(&mut entry.value, value));
        }
        let (previous, next) = if lowest {
            (None, self.first)
        } else {
            (self.last, None)
        };
        self.cache.insert(
            key,
            WriteEntry {
                value,
                second_chance: AtomicBool::new(false),
                previous,
                next,
            },
        );
        if let Some(previous) = previous {
            self.cache.get_mut(&previous).unwrap().next = Some(key);
        } else {
            self.first = Some(key);
        }
        if let Some(next) = next {
            self.cache.get_mut(&next).unwrap().previous = Some(key);
        } else {
            self.last = Some(key);
        }
        None
    }

    pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
        self.insert_at(key, value, false)
    }

    /// A one-pass page has lowest priority until another access grants a second chance.
    #[expect(
        dead_code,
        reason = "the write buffer retains the original cache implementation"
    )]
    pub(crate) fn insert_lowest_priority(&mut self, key: u64, value: T) -> Option<T> {
        self.insert_at(key, value, true)
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
        let entry = self.cache.remove(&key)?;
        if let Some(previous) = entry.previous {
            self.cache.get_mut(&previous).unwrap().next = entry.next;
        } else {
            self.first = entry.next;
        }
        if let Some(next) = entry.next {
            self.cache.get_mut(&next).unwrap().previous = entry.previous;
        } else {
            self.last = entry.previous;
        }
        // Geometric reclamation keeps metadata proportional to live cache entries
        // without reallocating the queue or rehashing on each page replacement.
        if self.cache.is_empty() {
            self.cache = Default::default();
        } else if self.cache.capacity() > self.cache.len().saturating_mul(4) {
            self.cache.shrink_to_fit();
        }
        Some(entry.value)
    }

    pub(crate) fn get(&self, key: u64) -> Option<&T> {
        let entry = self.cache.get(&key)?;
        entry.second_chance.store(true, Ordering::Release);
        Some(&entry.value)
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        let entry = self.cache.get_mut(&key)?;
        entry.second_chance.store(true, Ordering::Release);
        Some(&mut entry.value)
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &T)> {
        self.cache.iter().map(|(key, entry)| (key, &entry.value))
    }

    pub(crate) fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut T)> {
        self.cache
            .iter_mut()
            .map(|(key, entry)| (key, &mut entry.value))
    }

    pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
        self.pop_lowest_priority_where(|_| true)
    }

    /// Keep ineligible entries linked while looking for a CLOCK victim. Two
    /// passes let each eligible entry spend its second chance without looping
    /// forever when every entry is held by a reader.
    pub(crate) fn pop_lowest_priority_where(
        &mut self,
        mut eligible: impl FnMut(&T) -> bool,
    ) -> Option<(u64, T)> {
        for _ in 0..self.cache.len().saturating_mul(2) {
            let key = self.first?;
            let entry = self.cache.get(&key).unwrap();
            if !entry.second_chance.swap(false, Ordering::AcqRel) && eligible(&entry.value) {
                return self.remove(key).map(|value| (key, value));
            }
            if let Some(next) = entry.next {
                let last = self.last.unwrap();
                self.cache.get_mut(&next).unwrap().previous = None;
                self.cache.get_mut(&last).unwrap().next = Some(key);
                let entry = self.cache.get_mut(&key).unwrap();
                entry.previous = Some(last);
                entry.next = None;
                self.first = Some(next);
                self.last = Some(key);
            }
        }
        None
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::new();
    }
}

#[cfg(test)]
pub(crate) mod arena_index_tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static WATCH: Cell<bool> = const { Cell::new(false) };
        // Successful allocation calls, frees, reallocations, allocated bytes, freed bytes.
        static COUNTS: Cell<[usize; 5]> = const { Cell::new([0; 5]) };
    }
    struct ObservedAllocator;
    #[global_allocator]
    static ALLOCATOR: ObservedAllocator = ObservedAllocator;

    fn note(index: usize, bytes_index: usize, bytes: usize) {
        let _ = WATCH.try_with(|watch| {
            if watch.get() {
                let _ = COUNTS.try_with(|counts| {
                    let mut cut = counts.get();
                    cut[index] += 1;
                    cut[bytes_index] += bytes;
                    counts.set(cut);
                });
            }
        });
    }
    unsafe impl GlobalAlloc for ObservedAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                note(0, 3, layout.size());
            }
            ptr
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc_zeroed(layout) };
            if !ptr.is_null() {
                note(0, 3, layout.size());
            }
            ptr
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            note(1, 4, layout.size());
            unsafe { System.dealloc(ptr, layout) };
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let next = unsafe { System.realloc(ptr, layout, size) };
            if !next.is_null() {
                let _ = WATCH.try_with(|watch| {
                    if watch.get() {
                        let _ = COUNTS.try_with(|counts| {
                            let mut cut = counts.get();
                            cut[2] += 1;
                            cut[3] += size;
                            cut[4] += layout.size();
                            counts.set(cut);
                        });
                    }
                });
            }
            next
        }
    }
    pub(crate) struct Watch;
    impl Watch {
        pub(crate) fn start() -> Self {
            COUNTS.with(|counts| counts.set([0; 5]));
            WATCH.with(|watch| assert!(!watch.replace(true)));
            Self
        }
        pub(crate) fn reset(&self) {
            COUNTS.with(|counts| counts.set([0; 5]));
        }
        pub(crate) fn cut(&self) -> [usize; 5] {
            COUNTS.with(Cell::get)
        }
    }
    impl Drop for Watch {
        fn drop(&mut self) {
            WATCH.with(|watch| watch.set(false));
        }
    }

    #[test]
    fn arena_index_bytes_equal_actual_live_allocation_across_uniform_and_skewed_churn() {
        let mut spread_bytes = [0; 2];
        for (spread, skewed) in [false, true].into_iter().enumerate() {
            let watch = Watch::start();
            let arena = super::EntryArena::<u64>::new(996);
            let mut stripes: [super::LRUCache<u64>; 131] =
                std::array::from_fn(|_| super::LRUCache::new(arena.clone(), 8));
            let index_bytes =
                arena.index_bytes() + stripes.iter().map(|s| s.index_bytes()).sum::<usize>();
            let construction = watch.cut();
            assert_eq!(construction[2], 0);
            assert_eq!(construction[3] - construction[4], index_bytes);
            spread_bytes[spread] = index_bytes;
            watch.reset();
            for round in 0..3_u64 {
                for ordinal in 0..996_u64 {
                    let key = round * 996 + ordinal;
                    let stripe = if skewed { 0 } else { ordinal as usize % 131 };
                    assert_eq!(stripes[stripe].try_insert(key, key), Ok(None));
                }
                assert_eq!(stripes.iter().map(|s| s.len()).sum::<usize>(), 996);
                assert_eq!(stripes[0].try_insert(u64::MAX, 7), Err(7));
                for stripe in &mut stripes {
                    while let Some((key, value)) = stripe.pop_lowest_priority() {
                        assert_eq!(key, value);
                    }
                }
                assert!(stripes.iter().all(|s| s.len() == 0));
                assert_eq!(
                    arena.index_bytes() + stripes.iter().map(|s| s.index_bytes()).sum::<usize>(),
                    index_bytes
                );
                assert_eq!(
                    watch.cut(),
                    [0; 5],
                    "fill, eviction and refill retain exactly the original arena and heads"
                );
            }
            for ordinal in 0..996_u64 {
                assert_eq!(
                    stripes[0].try_insert_lowest_priority(ordinal, ordinal),
                    Ok(None)
                );
            }
            for stripe in &mut stripes {
                stripe.clear();
            }
            assert_eq!(watch.cut(), [0; 5]);
            drop(stripes);
            drop(arena);
            let released = watch.cut();
            assert_eq!(released[0], 0);
            assert_eq!(released[2], 0);
            assert_eq!(released[3], 0);
            assert_eq!(released[4], index_bytes);
        }
        assert_eq!(
            spread_bytes[0], spread_bytes[1],
            "offset spread cannot change index ownership"
        );
    }

    #[test]
    fn arena_clock_victim_trace_matches_the_published_hash_map_cache() {
        let mut old = old_reference::LRUCache::new();
        let mut new = super::LRUCache::new(super::EntryArena::new(32), 8);
        for key in 1..=5_u64 {
            assert_eq!(old.insert(key, key), new.insert(key, key));
        }
        for key in [1, 2, 5] {
            assert_eq!(old.get(key).copied(), new.get(key).map(|v| *v));
        }
        // Key 3 remains pinned while eligible entries spend second chances.
        for _ in 0..2 {
            assert_eq!(
                old.pop_lowest_priority_where(|v| *v != 3),
                new.pop_lowest_priority_where(|v| *v != 3)
            );
        }
        assert_eq!(
            old.insert_lowest_priority(9, 9),
            new.try_insert_lowest_priority(9, 9).unwrap()
        );
        assert_eq!(
            old.pop_lowest_priority_where(|v| *v != 3),
            new.pop_lowest_priority_where(|v| *v != 3)
        );
        assert_eq!(old.remove(2), new.remove(2));
        for key in 10..14 {
            assert_eq!(old.insert(key, key), new.insert(key, key));
        }
        for key in [3, 11, 13] {
            assert_eq!(old.get(key).copied(), new.get(key).map(|v| *v));
        }
        while old.len() > 0 {
            assert_eq!(old.pop_lowest_priority(), new.pop_lowest_priority());
        }
        assert_eq!(new.pop_lowest_priority(), None);
        assert_eq!(new.len(), 0);
    }

    // Independent published pre-arena implementation, SHA f062c990.
    mod old_reference {
        use crate::tree_store::page_store::fast_hash::FastHasher64;
        type FastHashMapU64<T> =
            std::collections::HashMap<u64, T, std::hash::BuildHasherDefault<FastHasher64>>;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Entry<T> {
            value: T,
            second_chance: AtomicBool,
            previous: Option<u64>,
            next: Option<u64>,
        }

        pub struct LRUCache<T> {
            cache: FastHashMapU64<Entry<T>>,
            first: Option<u64>,
            last: Option<u64>,
        }

        impl<T> Default for LRUCache<T> {
            fn default() -> Self {
                Self::new()
            }
        }

        impl<T> LRUCache<T> {
            pub(crate) fn new() -> Self {
                Self {
                    cache: Default::default(),
                    first: None,
                    last: None,
                }
            }

            pub(crate) fn len(&self) -> usize {
                self.cache.len()
            }

            fn insert_at(&mut self, key: u64, value: T, lowest: bool) -> Option<T> {
                if let Some(entry) = self.cache.get_mut(&key) {
                    entry.second_chance.store(false, Ordering::Release);
                    return Some(std::mem::replace(&mut entry.value, value));
                }
                let (previous, next) = if lowest {
                    (None, self.first)
                } else {
                    (self.last, None)
                };
                self.cache.insert(
                    key,
                    Entry {
                        value,
                        second_chance: AtomicBool::new(false),
                        previous,
                        next,
                    },
                );
                if let Some(previous) = previous {
                    self.cache.get_mut(&previous).unwrap().next = Some(key);
                } else {
                    self.first = Some(key);
                }
                if let Some(next) = next {
                    self.cache.get_mut(&next).unwrap().previous = Some(key);
                } else {
                    self.last = Some(key);
                }
                None
            }

            pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
                self.insert_at(key, value, false)
            }

            /// A one-pass page has lowest priority until another access grants a second chance.
            pub(crate) fn insert_lowest_priority(&mut self, key: u64, value: T) -> Option<T> {
                self.insert_at(key, value, true)
            }

            pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
                let entry = self.cache.remove(&key)?;
                if let Some(previous) = entry.previous {
                    self.cache.get_mut(&previous).unwrap().next = entry.next;
                } else {
                    self.first = entry.next;
                }
                if let Some(next) = entry.next {
                    self.cache.get_mut(&next).unwrap().previous = entry.previous;
                } else {
                    self.last = entry.previous;
                }
                // Geometric reclamation keeps metadata proportional to live cache entries
                // without reallocating the queue or rehashing on each page replacement.
                if self.cache.is_empty() {
                    self.cache = Default::default();
                } else if self.cache.capacity() > self.cache.len().saturating_mul(4) {
                    self.cache.shrink_to_fit();
                }
                Some(entry.value)
            }

            pub(crate) fn get(&self, key: u64) -> Option<&T> {
                let entry = self.cache.get(&key)?;
                entry.second_chance.store(true, Ordering::Release);
                Some(&entry.value)
            }

            pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
                let entry = self.cache.get_mut(&key)?;
                entry.second_chance.store(true, Ordering::Release);
                Some(&mut entry.value)
            }

            pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &T)> {
                self.cache.iter().map(|(key, entry)| (key, &entry.value))
            }

            pub(crate) fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut T)> {
                self.cache
                    .iter_mut()
                    .map(|(key, entry)| (key, &mut entry.value))
            }

            pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
                self.pop_lowest_priority_where(|_| true)
            }

            /// Keep ineligible entries linked while looking for a CLOCK victim. Two
            /// passes let each eligible entry spend its second chance without looping
            /// forever when every entry is held by a reader.
            pub(crate) fn pop_lowest_priority_where(
                &mut self,
                mut eligible: impl FnMut(&T) -> bool,
            ) -> Option<(u64, T)> {
                for _ in 0..self.cache.len().saturating_mul(2) {
                    let key = self.first?;
                    let entry = self.cache.get(&key).unwrap();
                    if !entry.second_chance.swap(false, Ordering::AcqRel) && eligible(&entry.value)
                    {
                        return self.remove(key).map(|value| (key, value));
                    }
                    if let Some(next) = entry.next {
                        let last = self.last.unwrap();
                        self.cache.get_mut(&next).unwrap().previous = None;
                        self.cache.get_mut(&last).unwrap().next = Some(key);
                        let entry = self.cache.get_mut(&key).unwrap();
                        entry.previous = Some(last);
                        entry.next = None;
                        self.first = Some(next);
                        self.last = Some(key);
                    }
                }
                None
            }

            pub(crate) fn clear(&mut self) {
                *self = Self::new();
            }
        }
    }
}


#[cfg(test)]
impl<T> LRUCache<T> {
    pub(crate) fn arena_capacity_for_test(&self) -> usize { self.arena_capacity() }
}
#[cfg(test)]
impl<T> WriteCache<T> {
    pub(crate) fn map_capacity_for_test(&self) -> usize { self.cache.capacity() }
}
