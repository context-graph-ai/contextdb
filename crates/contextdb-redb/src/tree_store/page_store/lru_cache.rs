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
