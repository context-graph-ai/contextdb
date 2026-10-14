use super::allocator_pager::AllocatorPager;
use crate::tree_store::page_store::hash128_with_seed;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const HEIGHT_OFFSET: usize = 0;
const END_OFFSETS: usize = HEIGHT_OFFSET + size_of::<u32>();

pub(crate) struct BtreeBitmap {
    heights: Vec<U64GroupedBitmap>,
    // The one-word root summary, kept resident once read and forgotten on any
    // mutation. A fixed 16 bytes per bitmap: free-space questions about an
    // untouched order are answered without loading its root window.
    root_word: AtomicU64,
    root_known: AtomicBool,
}

// Stores a 64-way bit-tree of allocated ids.
//
// Data structure format:
// height: u32
// layer_ends: array of u32, ending offset in bytes of layers.
// layer data: u64s
// ...consecutive layers. Except for the last level, all sub-trees of the root must be complete
impl BtreeBitmap {
    fn from_heights(heights: Vec<U64GroupedBitmap>) -> Self {
        Self {
            heights,
            root_word: AtomicU64::new(0),
            root_known: AtomicBool::new(false),
        }
    }

    fn root(&self) -> u64 {
        if self.root_known.load(Ordering::Relaxed) {
            return self.root_word.load(Ordering::Relaxed);
        }
        let root = self.get_level(0).word(0);
        // A poisoned read answers "full" once and is not remembered.
        if !self.get_level(0).poisoned() {
            self.root_word.store(root, Ordering::Relaxed);
            self.root_known.store(true, Ordering::Relaxed);
        }
        root
    }

    fn forget_root(&self) {
        self.root_known.store(false, Ordering::Relaxed);
    }

    pub(crate) fn poisoned(&self) -> bool {
        self.heights.iter().any(U64GroupedBitmap::poisoned)
    }
    pub(crate) fn materialize_paged_words(&mut self) {
        for height in &mut self.heights {
            height.materialize_paged_words();
        }
        self.forget_root();
    }
    pub(super) fn allocator_metadata_bytes_for_test(&self) -> usize {
        self.heights.capacity() * size_of::<U64GroupedBitmap>()
            + self
                .heights
                .iter()
                .map(|height| height.data.capacity() * size_of::<u64>())
                .sum::<usize>()
    }

    pub(crate) fn count_unset(&self) -> u32 {
        self.get_level(self.get_height() - 1).count_unset()
    }

    // Answered by the maintained fullness summary: one word per level, the
    // same descent `alloc` takes, rather than a sweep of the leaf words whose
    // length is the region's page count. True exactly when `alloc` succeeds.
    pub(crate) fn has_unset(&self) -> bool {
        self.find_first_unset().is_some()
    }

    pub(crate) fn get(&self, i: u32) -> bool {
        self.get_level(self.get_height() - 1).get(i)
    }

    pub(crate) fn len(&self) -> u32 {
        self.get_level(self.get_height() - 1).len()
    }

    pub(crate) fn find_first_unset(&self) -> Option<u32> {
        // The root level holds at most 64 bits, so the resident root word is
        // the whole of `get_level(0).first_unset(0, 64)`.
        let root_entry = if self.get_level(0).len() == 0 {
            None
        } else {
            match self.root().trailing_ones() {
                64 => None,
                free => Some(free),
            }
        };
        if let Some(mut entry) = root_entry {
            let mut height = 0;

            while height < self.get_height() - 1 {
                height += 1;
                entry *= 64;
                entry = self.get_level(height).first_unset(entry, entry + 64)?;
            }

            Some(entry)
        } else {
            None
        }
    }

    fn get_level(&self, i: u32) -> &U64GroupedBitmap {
        assert!(i < self.get_height());
        &self.heights[i as usize]
    }

    fn get_height(&self) -> u32 {
        self.heights.len().try_into().unwrap()
    }

    pub(crate) fn xxh3_hash(&self) -> u128 {
        let mut result = 0;
        for height in &self.heights {
            result ^= height.xxh3_hash();
        }
        result
    }

    pub(crate) fn encoded_len(&self) -> usize {
        END_OFFSETS
            + self.heights.len() * size_of::<u32>()
            + self
                .heights
                .iter()
                .map(U64GroupedBitmap::encoded_len)
                .sum::<usize>()
    }

    pub(crate) fn encode_into(&self, out: &mut [u8]) {
        assert!(out.len() >= self.encoded_len());
        out[..4].copy_from_slice(&u32::try_from(self.heights.len()).unwrap().to_le_bytes());
        let mut offset = END_OFFSETS + self.heights.len() * size_of::<u32>();
        for (height, bitmap) in self.heights.iter().enumerate() {
            let end = offset + bitmap.encoded_len();
            let field = END_OFFSETS + height * size_of::<u32>();
            out[field..field + 4].copy_from_slice(&u32::try_from(end).unwrap().to_le_bytes());
            bitmap.encode_into(&mut out[offset..end]);
            offset = end;
        }
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Self {
        let height = u32::from_le_bytes(
            data[HEIGHT_OFFSET..(HEIGHT_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        let mut metadata = END_OFFSETS;
        let mut data_start = END_OFFSETS + (height as usize) * size_of::<u32>();

        let mut heights = vec![];
        for _ in 0..height {
            let data_end = u32::from_le_bytes(
                data[metadata..(metadata + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            heights.push(U64GroupedBitmap::from_bytes(&data[data_start..data_end]));
            data_start = data_end;
            metadata += size_of::<u32>();
        }

        Self::from_heights(heights)
    }

    // Initializes a new allocator, with no ids free
    pub(crate) fn new(mut num_pages: u32, mut capacity: u32) -> Self {
        let mut heights = vec![];

        // Build from the leaf to root
        loop {
            heights.push(U64GroupedBitmap::new_full(num_pages, capacity));
            if capacity <= 64 {
                break;
            }
            capacity = capacity.div_ceil(64);
            num_pages = num_pages.div_ceil(64);
        }

        // Reverse so that the root is at index 0
        heights.reverse();

        Self::from_heights(heights)
    }

    // Like new(), but pads the tree height for max_capacity so resize()
    // never needs to insert new levels.
    pub(crate) fn new_padded(num_pages: u32, capacity: u32, max_capacity: u32) -> Self {
        let mut result = Self::new(num_pages, capacity);

        let max_height = Self::height_for_capacity(max_capacity);
        while result.heights.len() < max_height {
            let root_len = result.heights[0].len();
            let parent_len = root_len.div_ceil(64);
            result
                .heights
                .insert(0, U64GroupedBitmap::new_full(parent_len, parent_len));
        }

        result
    }

    pub(crate) fn new_padded_paged(
        mut num_pages: u32,
        _capacity: u32,
        max_capacity: u32,
        array_base: u64,
        pager: &Arc<AllocatorPager>,
        optimistic: bool,
    ) -> Self {
        let mut dimensions = Vec::with_capacity(Self::height_for_capacity(max_capacity));
        let mut capacity = max_capacity;
        loop {
            dimensions.push((num_pages, capacity));
            if capacity <= 64 {
                break;
            }
            capacity = capacity.div_ceil(64);
            num_pages = num_pages.div_ceil(64);
        }
        dimensions.reverse();
        let heights = dimensions
            .into_iter()
            .enumerate()
            .map(|(height, (len, capacity))| {
                U64GroupedBitmap::new_paged(
                    len,
                    capacity,
                    array_base + height as u64,
                    pager,
                    optimistic,
                )
            })
            .collect();
        Self::from_heights(heights)
    }

    fn height_for_capacity(mut capacity: u32) -> usize {
        let mut height = 1;
        while capacity > 64 {
            capacity = capacity.div_ceil(64);
            height += 1;
        }
        height
    }

    pub(crate) fn resize(&mut self, mut new_len: u32, full: bool) {
        self.forget_root();
        for height in self.heights.iter_mut().rev() {
            height.resize(new_len, full);
            new_len = new_len.div_ceil(64);
        }
        assert!(self.get_level(0).len() <= 64);
    }

    // Returns the first unset id, and sets it
    pub(crate) fn alloc(&mut self) -> Option<u32> {
        if self.poisoned() {
            return None;
        }
        let entry = self.find_first_unset()?;
        if self.poisoned() {
            return None;
        }
        self.set(entry);
        (!self.poisoned()).then_some(entry)
    }

    pub(crate) fn set(&mut self, i: u32) {
        self.forget_root();
        if self.poisoned() {
            return;
        }
        let full = self.get_level_mut(self.get_height() - 1).set(i);
        if self.poisoned() {
            return;
        }
        self.update_to_root(i, full);
    }

    pub(crate) fn clear(&mut self, i: u32) {
        self.forget_root();
        if self.poisoned() {
            return;
        }
        self.get_level_mut(self.get_height() - 1).clear(i);
        if self.poisoned() {
            return;
        }
        self.update_to_root(i, false);
    }

    fn get_level_mut(&mut self, i: u32) -> &mut U64GroupedBitmap {
        assert!(i < self.get_height());
        &mut self.heights[i as usize]
    }

    // Recursively update to the root, starting at the given entry in the given height
    // full parameter must be set if all bits in the entry's group of u64 are full
    fn update_to_root(&mut self, i: u32, mut full: bool) {
        if self.get_height() == 1 {
            return;
        }

        let mut parent_height = self.get_height() - 2;
        let mut parent_entry = i / 64;
        loop {
            full = if full {
                self.get_level_mut(parent_height).set(parent_entry)
            } else {
                self.get_level_mut(parent_height).clear(parent_entry);
                false
            };

            if parent_height == 0 {
                break;
            }
            parent_height -= 1;
            parent_entry /= 64;
        }
    }
}

// A bitmap which groups consecutive groups of 64bits together
pub(crate) struct U64GroupedBitmap {
    len: u32,
    data: Vec<u64>,
    paged: Option<PagedWords>,
}

struct PagedWords {
    array: u64,
    capacity_words: usize,
    pager: Arc<AllocatorPager>,
    optimistic: bool,
}

impl U64GroupedBitmap {
    fn poisoned(&self) -> bool {
        self.paged
            .as_ref()
            .is_some_and(|paged| paged.pager.is_poisoned())
    }
    fn materialize_paged_words(&mut self) {
        if self.paged.is_some() {
            for index in 0..self.word_count() {
                let value = self.word(index);
                self.set_word(index, value);
            }
        }
    }
    fn required_words(elements: u32) -> usize {
        let words = elements.div_ceil(64);
        words as usize
    }

    pub fn new_full(len: u32, capacity: u32) -> Self {
        let data = vec![u64::MAX; Self::required_words(capacity)];
        Self {
            len,
            data,
            paged: None,
        }
    }

    fn new_paged(
        len: u32,
        capacity: u32,
        array: u64,
        pager: &Arc<AllocatorPager>,
        optimistic: bool,
    ) -> Self {
        Self {
            len,
            data: Vec::new(),
            paged: Some(PagedWords {
                array,
                capacity_words: Self::required_words(capacity),
                pager: Arc::clone(pager),
                optimistic,
            }),
        }
    }

    fn word(&self, index: usize) -> u64 {
        // A saved region can have been larger than the currently published
        // geometry after shrink. Its old tail bits are outside this bitmap and
        // must never advertise a page to the allocator.
        let first_bit = index * 64;
        if first_bit >= self.len as usize {
            return u64::MAX;
        }
        let value = if let Some(paged) = &self.paged {
            assert!(index < paged.capacity_words);
            paged
                .pager
                .read_word(paged.array, index, self.len, paged.optimistic)
        } else {
            self.data[index]
        };
        let valid = (self.len as usize - first_bit).min(64);
        if valid == 64 {
            value
        } else {
            value | (u64::MAX << valid)
        }
    }

    fn set_word(&mut self, index: usize, value: u64) {
        if let Some(paged) = &self.paged {
            assert!(index < paged.capacity_words);
            paged
                .pager
                .write_word(paged.array, index, self.len, paged.optimistic, value);
        } else {
            self.data[index] = value;
        }
    }

    fn word_count(&self) -> usize {
        Self::required_words(self.len)
    }

    pub fn xxh3_hash(&self) -> u128 {
        if self.len == 0 {
            return 0;
        }
        let mut buffer = [0u8; 4096];
        buffer[..4].copy_from_slice(&self.len.to_le_bytes());
        let mut used = 4;
        let mut state = 0u128;
        // This hash is an in-process mutation guard, not a durable checksum.
        // Fold fixed windows so large bitmaps never allocate a full copy.
        let mut add_word = |word: u64| {
            if used + 8 > buffer.len() {
                let seed = u64::from_le_bytes(state.to_le_bytes()[..8].try_into().unwrap());
                state = hash128_with_seed(&buffer[..used], seed) ^ state.rotate_left(17);
                used = 0;
            }
            buffer[used..used + 8].copy_from_slice(&word.to_le_bytes());
            used += 8;
        };
        for index in 0..Self::required_words(self.len) - 1 {
            add_word(self.word(index));
        }
        let (index, bit) = Self::data_index_of(self.len - 1);
        // Select the bit and all lower ones
        let mask = ((1 << bit) - 1) | (1 << bit);
        let group = self.word(index);
        let group = group & mask;
        add_word(group);
        let seed = u64::from_le_bytes(state.to_le_bytes()[..8].try_into().unwrap());
        hash128_with_seed(&buffer[..used], seed) ^ state.rotate_left(17)
    }

    // Format:
    // 4 bytes: number of elements
    // n bytes: serialized groups
    fn encoded_len(&self) -> usize {
        size_of::<u32>() + Self::required_words(self.len) * size_of::<u64>()
    }

    fn encode_into(&self, out: &mut [u8]) {
        assert!(out.len() >= self.encoded_len());
        out[..4].copy_from_slice(&self.len.to_le_bytes());
        for index in 0..Self::required_words(self.len) {
            let start = 4 + index * 8;
            out[start..start + 8].copy_from_slice(&self.word(index).to_le_bytes());
        }
    }

    pub fn from_bytes(serialized: &[u8]) -> Self {
        assert_eq!(0, (serialized.len() - size_of::<u32>()) % size_of::<u64>());
        let len = u32::from_le_bytes(serialized[..size_of::<u32>()].try_into().unwrap());
        let words = (serialized.len() - size_of::<u32>()) / size_of::<u64>();
        let mut data = Vec::with_capacity(words);
        for i in 0..words {
            let start = size_of::<u32>() + i * size_of::<u64>();
            let value = u64::from_le_bytes(
                serialized[start..(start + size_of::<u64>())]
                    .try_into()
                    .unwrap(),
            );
            data.push(value);
        }

        Self {
            len,
            data,
            paged: None,
        }
    }

    fn data_index_of(bit: u32) -> (usize, usize) {
        ((bit as usize) / 64, (bit as usize) % 64)
    }

    fn select_mask(bit: usize) -> u64 {
        1u64 << (bit as u64)
    }

    fn count_unset(&self) -> u32 {
        (0..self.word_count())
            .map(|i| self.word(i).count_zeros())
            .sum()
    }

    fn first_unset(&self, start_bit: u32, end_bit: u32) -> Option<u32> {
        assert_eq!(end_bit, (start_bit - start_bit % 64) + 64);
        if self.len == 0 {
            return None;
        }

        let (index, bit) = Self::data_index_of(start_bit);
        let mask = (1 << bit) - 1;
        let group = self.word(index);
        let group = group | mask;
        match group.trailing_ones() {
            64 => None,
            x => Some(start_bit + x - u32::try_from(bit).unwrap()),
        }
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn resize(&mut self, new_len: u32, full: bool) {
        if self.paged.is_none() && self.data.len() < Self::required_words(new_len) {
            let default_value = if full { u64::MAX } else { 0 };
            self.data
                .resize(Self::required_words(new_len), default_value);
        }
        let old_len = self.len;
        self.len = new_len;
        if old_len < new_len {
            // TODO: optimize this loop to set whole words at a time
            for i in old_len..new_len {
                if full {
                    self.set(i);
                } else {
                    self.clear(i);
                }
            }
        }
    }

    pub fn get(&self, bit: u32) -> bool {
        assert!(bit < self.len);
        let (index, bit_index) = Self::data_index_of(bit);
        let group = self.word(index);
        group & U64GroupedBitmap::select_mask(bit_index) != 0
    }

    // Returns true iff the bit's group is all set
    pub fn set(&mut self, bit: u32) -> bool {
        assert!(bit < self.len);
        let (index, bit_index) = Self::data_index_of(bit);
        let mut group = self.word(index);
        group |= Self::select_mask(bit_index);
        self.set_word(index, group);

        group == u64::MAX
    }

    pub fn clear(&mut self, bit: u32) {
        assert!(bit < self.len, "{bit} must be less than {}", self.len);
        let (index, bit_index) = Self::data_index_of(bit);
        self.set_word(index, self.word(index) & !Self::select_mask(bit_index));
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::bitmap::BtreeBitmap;
    use rand::prelude::IteratorRandom;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use std::collections::HashSet;

    #[test]
    fn alloc() {
        let num_pages = 2;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        for i in 0..num_pages {
            assert_eq!(i, allocator.alloc().unwrap());
        }
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn record_alloc() {
        let mut allocator = BtreeBitmap::new(2, 2);
        allocator.clear(0);
        allocator.clear(1);
        allocator.set(0);
        assert_eq!(1, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn free() {
        let mut allocator = BtreeBitmap::new(1, 1);
        allocator.clear(0);
        assert_eq!(0, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
        allocator.clear(0);
        assert_eq!(0, allocator.alloc().unwrap());
    }

    #[test]
    fn reuse_lowest() {
        let num_pages = 65;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        for i in 0..num_pages {
            assert_eq!(i, allocator.alloc().unwrap());
        }
        allocator.clear(5);
        allocator.clear(15);
        assert_eq!(5, allocator.alloc().unwrap());
        assert_eq!(15, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn all_space_used() {
        let num_pages = 65;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        // Allocate everything
        while allocator.alloc().is_some() {}
        // The last u64 must be used, since the leaf layer is compact
        assert_eq!(
            u64::MAX,
            *allocator.heights.last().unwrap().data.last().unwrap()
        );
    }

    #[test]
    fn find_free() {
        let num_pages = 129;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        assert!(allocator.find_first_unset().is_none());
        allocator.clear(128);
        assert_eq!(allocator.find_first_unset().unwrap(), 128);
        allocator.clear(65);
        assert_eq!(allocator.find_first_unset().unwrap(), 65);
        allocator.clear(8);
        assert_eq!(allocator.find_first_unset().unwrap(), 8);
        allocator.clear(0);
        assert_eq!(allocator.find_first_unset().unwrap(), 0);
    }

    #[test]
    fn resize_beyond_initial_capacity() {
        let mut bm = BtreeBitmap::new_padded(60, 60, 1_048_576);
        let height = bm.heights.len();

        bm.clear(0);
        bm.clear(30);
        bm.clear(59);

        bm.resize(5000, true);

        // Height is pre-padded at construction, stays the same
        assert_eq!(bm.heights.len(), height);

        // Previously cleared bits survive the resize
        assert!(!bm.get(0));
        assert!(!bm.get(30));
        assert!(!bm.get(59));

        // Newly grown region should be marked full (true)
        assert!(bm.get(60));
        assert!(bm.get(4999));

        // Alloc should return the previously cleared entries
        assert_eq!(bm.alloc(), Some(0));
        assert_eq!(bm.alloc(), Some(30));
        assert_eq!(bm.alloc(), Some(59));
        assert!(bm.alloc().is_none());

        // Clear a bit in the newly grown area, alloc should find it
        bm.clear(3000);
        assert_eq!(bm.alloc(), Some(3000));
        assert!(bm.alloc().is_none());
    }

    #[test]
    fn random_pattern() {
        let seed = rand::rng().random();
        // Print the seed to debug for reproducibility, in case this test fails
        println!("seed={seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let num_pages = rng.random_range(2..10000);
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        let mut allocated = HashSet::new();

        for _ in 0..(num_pages * 2) {
            if rng.random_bool(0.75) {
                if let Some(page) = allocator.alloc() {
                    allocated.insert(page);
                } else {
                    assert_eq!(allocated.len(), num_pages as usize);
                }
            } else if let Some(to_free) = allocated.iter().choose(&mut rng).copied() {
                allocator.clear(to_free);
                allocated.remove(&to_free);
            }
        }

        for _ in allocated.len()..(num_pages as usize) {
            allocator.alloc().unwrap();
        }
        assert!(allocator.alloc().is_none());

        for i in 0..num_pages {
            allocator.clear(i);
        }

        for _ in 0..num_pages {
            allocator.alloc().unwrap();
        }
        assert!(allocator.alloc().is_none());
    }
}
