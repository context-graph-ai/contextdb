use super::allocator_pager::{AllocatorPager, bitmap_array_base};
use crate::tree_store::PageNumber;
use crate::tree_store::page_store::bitmap::BtreeBitmap;
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use std::cmp::min;
#[cfg(test)]
use std::collections::HashSet;
use std::mem::size_of;
use std::sync::Arc;

const MAX_ORDER_OFFSET: usize = 0;
const PADDING: usize = 3;
const NUM_PAGES_OFFSET: usize = MAX_ORDER_OFFSET + size_of::<u8>() + PADDING;
const FREE_END_OFFSETS: usize = NUM_PAGES_OFFSET + size_of::<u32>();

fn calculate_usable_order(pages: u32) -> u8 {
    let max_order = (32 - pages.leading_zeros() - 1).try_into().unwrap();
    min(MAX_MAX_PAGE_ORDER, max_order)
}

fn next_higher_order(page_number: u32) -> u32 {
    page_number / 2
}

fn buddy_page(page_number: u32) -> u32 {
    page_number ^ 1
}

#[cfg(test)]
thread_local! {
    /// Blocks `alloc_lowest` allocated at a higher order only to compare
    /// positions, for the work-count test.
    pub(super) static TENTATIVE_ALLOCATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// Handles allocation of dynamically sized pages, supports pages of up to page_size * 2^max_order bytes
//
// Pages are marked free at only a single order, and it must always be the largest order
pub(crate) struct BuddyAllocator {
    free: Vec<BtreeBitmap>,
    len: u32,
    max_order: u8,
}

impl BuddyAllocator {
    fn poisoned(&self) -> bool {
        self.free.first().is_some_and(BtreeBitmap::poisoned)
    }
    pub(crate) fn materialize_paged_words(&mut self) {
        for bitmap in &mut self.free {
            bitmap.materialize_paged_words();
        }
    }
    pub(crate) fn new_paged(
        num_pages: u32,
        max_page_capacity: u32,
        region: u32,
        pager: &Arc<AllocatorPager>,
        initialize_free: bool,
    ) -> Self {
        let max_order = calculate_usable_order(max_page_capacity);
        let mut capacity_for_order = max_page_capacity;
        let mut pages_for_order = num_pages;
        let mut free = Vec::with_capacity(max_order as usize + 1);
        for order in 0..=max_order {
            free.push(BtreeBitmap::new_padded_paged(
                pages_for_order,
                pages_for_order,
                capacity_for_order,
                bitmap_array_base(Some(region), order),
                pager,
                false,
            ));
            pages_for_order = next_higher_order(pages_for_order);
            capacity_for_order = next_higher_order(capacity_for_order);
        }
        if initialize_free {
            let mut accounted_pages = 0;
            for order in (0..=max_order).rev() {
                let order_size = 2u32.pow(order.into());
                while accounted_pages + order_size <= num_pages {
                    let page = accounted_pages / order_size;
                    free[order as usize].clear(page);
                    accounted_pages += order_size;
                }
            }
            assert_eq!(accounted_pages, num_pages);
        }
        Self {
            free,
            len: num_pages,
            max_order,
        }
    }
    pub(super) fn allocator_metadata_bytes_for_test(&self) -> usize {
        self.free.capacity() * size_of::<BtreeBitmap>()
            + self
                .free
                .iter()
                .map(BtreeBitmap::allocator_metadata_bytes_for_test)
                .sum::<usize>()
    }

    pub(crate) fn new(num_pages: u32, max_page_capacity: u32) -> Self {
        let max_order = calculate_usable_order(max_page_capacity);

        let mut capacity_for_order = max_page_capacity;
        let mut pages_for_order = num_pages;
        let mut free = vec![];
        for _ in 0..=max_order {
            // Lazily size each bitmap for the actual current pages, but pad the tree
            // height so resize() can grow to the full region capacity without needing
            // to insert new tree levels.
            free.push(BtreeBitmap::new_padded(
                pages_for_order,
                pages_for_order,
                capacity_for_order,
            ));

            pages_for_order = next_higher_order(pages_for_order);
            capacity_for_order = next_higher_order(capacity_for_order);
        }

        // Mark the available pages, starting with the highest order
        let mut accounted_pages = 0;
        for order in (0..=max_order).rev() {
            let order_size = 2u32.pow(order.into());
            while accounted_pages + order_size <= num_pages {
                let page = accounted_pages / order_size;
                free[order as usize].clear(page);
                accounted_pages += order_size;
            }
        }
        assert_eq!(accounted_pages, num_pages);

        Self {
            free,
            len: num_pages,
            max_order,
        }
    }

    pub(crate) fn xxh3_hash(&self) -> u128 {
        let mut result = 0;
        for x in &self.free {
            result ^= x.xxh3_hash();
        }

        result
    }

    // Data structure format:
    // max_order: u8
    // padding: 3 bytes
    // num_pages: u32
    // free_ends: array of u32, with ending offset for BtreeBitmap structure for the given order
    // ... BtreeBitmap structures
    #[cfg(test)]
    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![0; self.encoded_len()];
        self.encode_into(&mut result);
        result
    }

    pub(crate) fn encoded_len(&self) -> usize {
        let header_bytes = 1 + 3 + size_of::<u32>();
        let offsets_bytes = (self.max_order as usize + 1) * size_of::<u32>();
        header_bytes
            + offsets_bytes
            + self
                .free
                .iter()
                .map(BtreeBitmap::encoded_len)
                .sum::<usize>()
    }

    pub(crate) fn encode_into(&self, out: &mut [u8]) {
        assert!(out.len() >= self.encoded_len());
        let header_bytes = 1 + 3 + size_of::<u32>();
        let offsets_bytes = self.free.len() * size_of::<u32>();
        out[0] = self.max_order;
        out[1..4].fill(0);
        out[4..8].copy_from_slice(&self.len.to_le_bytes());

        let mut data_offset = header_bytes + offsets_bytes;
        for (order, bitmap) in self.free.iter().enumerate() {
            let end = data_offset + bitmap.encoded_len();
            let field = header_bytes + order * size_of::<u32>();
            out[field..field + 4].copy_from_slice(&u32::try_from(end).unwrap().to_le_bytes());
            bitmap.encode_into(&mut out[data_offset..end]);
            data_offset = end;
        }
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Self {
        let max_order = data[MAX_ORDER_OFFSET];
        let num_pages = u32::from_le_bytes(
            data[NUM_PAGES_OFFSET..(NUM_PAGES_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        let mut metadata = FREE_END_OFFSETS;
        let mut data_start = FREE_END_OFFSETS + (max_order as usize + 1) * size_of::<u32>();

        let mut free = vec![];
        for _ in 0..=max_order {
            let data_end = u32::from_le_bytes(
                data[metadata..(metadata + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            free.push(BtreeBitmap::from_bytes(&data[data_start..data_end]));
            data_start = data_end;
            metadata += size_of::<u32>();
        }

        Self {
            free,
            len: num_pages,
            max_order,
        }
    }

    #[inline]
    pub(crate) fn highest_free_order(&self) -> Option<u8> {
        (0..=self.max_order)
            .rev()
            .find(|order| self.get_order_free(*order).has_unset())
    }

    pub(crate) fn count_allocated_pages(&self) -> u32 {
        self.len() - self.count_free_pages()
    }

    pub(crate) fn count_free_pages(&self) -> u32 {
        let mut pages = 0;
        for order in 0..=self.max_order {
            pages += self.get_order_free(order).count_unset() * 2u32.pow(order.into());
        }
        pages
    }

    pub(crate) fn get_max_order(&self) -> u8 {
        self.max_order
    }

    fn find_free_order(&self, mut page: u32) -> Option<u8> {
        for order in 0..=self.max_order {
            if page < self.get_order_free(order).len() && !self.get_order_free(order).get(page) {
                return Some(order);
            }
            page = next_higher_order(page);
        }
        None
    }

    #[cfg(debug_assertions)]
    pub(crate) fn is_allocated(&self, page: PageNumber) -> bool {
        self.find_free_order(page.page_index << page.page_order)
            .is_none()
    }

    pub(crate) fn trailing_free_pages(&self) -> u32 {
        let mut free_pages = 0;
        let mut next_page = self.len() - 1;
        while let Some(order) = self.find_free_order(next_page) {
            let order_size = 2u32.pow(order.into());
            free_pages += order_size;
            if order_size > next_page {
                break;
            }
            next_page -= order_size;
        }

        free_pages
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    #[cfg_attr(not(test), expect(clippy::unused_self))]
    #[cfg_attr(not(test), expect(unused_variables))]
    pub(crate) fn check_allocated_pages(&self, region: u32, allocated_pages: &[PageNumber]) {
        #[cfg(test)]
        // Check the result against the free index to be sure it matches
        {
            let mut free_check = HashSet::new();
            for i in 0..self.len() {
                if self.find_free_order(i).is_none() {
                    free_check.insert(PageNumber::new(region, i, 0));
                }
            }

            let mut check_result = HashSet::new();
            for page in allocated_pages {
                check_result.extend(page.to_order0());
            }
            assert_eq!(free_check, check_result);
        }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    pub(crate) fn resize(&mut self, new_size: u32) {
        if self.poisoned() {
            return;
        }
        self.debug_check_consistency();
        if new_size > self.len() {
            let mut processed_pages = self.len();
            let mut pages_for_order = new_size;
            for order in &mut self.free {
                order.resize(pages_for_order, true);
                pages_for_order = next_higher_order(pages_for_order);
            }
            // Align to the highest order possible
            while processed_pages < new_size {
                let order: u8 = processed_pages.trailing_zeros().try_into().unwrap();
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if order >= self.max_order || processed_pages + order_size > new_size {
                    break;
                }
                self.free_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.max_order).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= new_size {
                    let page = processed_pages / order_size;
                    self.free_inner(page, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, new_size);
        } else {
            let mut processed_pages = new_size;
            // Align to the highest order possible
            while processed_pages < self.len() {
                let order: u8 = processed_pages.trailing_zeros().try_into().unwrap();
                if order >= self.max_order {
                    break;
                }
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if processed_pages + order_size > self.len() {
                    break;
                }
                self.record_alloc_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.max_order).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= self.len() {
                    let page = processed_pages / order_size;
                    self.record_alloc_inner(page, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, self.len());
            let mut pages_for_order = new_size;
            for order in &mut self.free {
                order.resize(pages_for_order, true);
                pages_for_order = next_higher_order(pages_for_order);
            }
        }
        self.len = new_size;
        self.debug_check_consistency();
    }

    #[allow(unused_variables)]
    fn debug_check_consistency(&self) {
        if self.poisoned() {
            return;
        }
        // Don't enable when fuzzing, because this is kind of expensive
        #[cfg(all(debug_assertions, not(fuzzing)))]
        {
            let mut processed = 0;
            // Ensure that no page is free at multiple orders
            while processed < self.len() {
                let mut found = false;
                let mut page = processed;
                for order in 0..=self.max_order {
                    let allocator = &self.free[order as usize];
                    if allocator.len() <= page {
                        break;
                    }
                    if !allocator.get(page) {
                        assert!(!found);
                        found = true;
                    }
                    page = next_higher_order(page);
                }
                processed += 1;
            }

            // Ensure that all buddy pages are merged, except at the highest order
            for order in (0..self.max_order).rev() {
                let order_len = self.len() / (2u32.pow(order.into()));
                let allocator = &self.free[order as usize];
                for page in 0..order_len {
                    if !allocator.get(page) {
                        let buddy = buddy_page(page);
                        if buddy < allocator.len() {
                            let buddy_allocated = allocator.get(buddy);
                            assert!(buddy_allocated, "order={order} page={page} buddy={buddy}",);
                        }
                    }
                }
            }
        }
    }

    // Allocates a page of the given order at the lowest index possible. Will split a higher order page,
    // to get one of lower index
    //
    // Each order's lowest free block is read, never allocated, to compare
    // positions; only the chosen block is taken and split down. A free block
    // of one order never starts where a free block of another order does, so
    // the lowest start is unique.
    pub(crate) fn alloc_lowest(&mut self, order: u8) -> Option<u32> {
        if self.poisoned() || order > self.max_order {
            return None;
        }
        let mut best: Option<(u32, u32, u8)> = None;
        for found in order..=self.max_order {
            if let Some(index) = self.get_order_free(found).find_first_unset() {
                let index_at_order = index << (found - order);
                if best.is_none_or(|(lowest, _, _)| index_at_order < lowest) {
                    best = Some((index_at_order, index, found));
                }
            }
        }
        if self.poisoned() {
            return None;
        }
        let (index_at_order, mut index, mut found) = best?;
        self.get_order_free_mut(found).set(index);
        // Split the page, until we get to the requested order
        while found > order {
            let (free1, free2) = (index * 2, index * 2 + 1);
            let allocator = self.get_order_free_mut(found - 1);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);
            index = free1;
            found -= 1;
        }
        if self.poisoned() {
            return None;
        }
        assert_eq!(index, index_at_order);

        Some(index)
    }

    /// The previous lowest-address search, which allocated a block at every
    /// higher order to compare positions; kept as the equivalence oracle.
    #[cfg(test)]
    fn alloc_lowest_by_trial(&mut self, order: u8) -> Option<u32> {
        let mut best_index_at_order = self.alloc_inner(order)?;
        let mut best = (best_index_at_order, order);
        let mut multiplier = 2;
        for i in (order + 1)..=self.max_order {
            if let Some(index) = self.alloc_inner(i) {
                TENTATIVE_ALLOCATIONS.with(|count| count.set(count.get() + 1));
                let index_at_order = index * multiplier;
                if index_at_order < best_index_at_order {
                    self.free_inner(best.0, best.1);
                    best_index_at_order = index_at_order;
                    best = (index, i);
                } else {
                    self.free_inner(index, i);
                }
            }
            multiplier *= 2;
        }
        while best.1 > order {
            let (best_index, best_order) = best;
            let (free1, free2) = (best_index * 2, best_index * 2 + 1);
            self.get_order_free_mut(best_order - 1).clear(free2);
            best = (free1, best_order - 1);
        }
        Some(best.0)
    }

    pub(crate) fn alloc(&mut self, order: u8) -> Option<u32> {
        self.alloc_inner(order)
    }

    pub(crate) fn alloc_inner(&mut self, order: u8) -> Option<u32> {
        if self.poisoned() {
            return None;
        }
        if order > self.max_order {
            return None;
        }
        let allocator = self.get_order_free_mut(order);
        if let Some(x) = allocator.alloc() {
            Some(x)
        } else {
            // Try to allocate a higher order page and split it
            let upper_page = self.alloc_inner(order + 1)?;
            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            let allocator = self.get_order_free_mut(order);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);

            Some(free1)
        }
    }

    // `page_number` must be free
    pub(crate) fn record_alloc(&mut self, page_number: u32, order: u8) {
        assert!(order <= self.max_order);
        // Split parent pages as necessary, and update the free index
        self.record_alloc_inner(page_number, order);
    }

    pub(crate) fn record_alloc_inner(&mut self, page_number: u32, order: u8) {
        if self.poisoned() {
            return;
        }
        let allocated = self.get_order_free_mut(order).get(page_number);
        if self.poisoned() {
            return;
        }
        if allocated {
            // Need to split parent page
            let upper_page = next_higher_order(page_number);
            self.record_alloc_inner(upper_page, order + 1);
            let allocator = self.get_order_free_mut(order);

            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            debug_assert!(free1 == page_number || free2 == page_number);
            if free1 == page_number {
                allocator.clear(free2);
            } else {
                allocator.clear(free1);
            }
        } else {
            self.get_order_free_mut(order).set(page_number);
        }
    }

    /// data must have been initialized by `Self::init_new()`
    pub(crate) fn free(&mut self, page_number: u32, order: u8) {
        debug_assert!(self.get_order_free_mut(order).get(page_number));

        // Update the free index and merge free pages
        self.free_inner(page_number, order);
    }

    pub(crate) fn free_inner(&mut self, page_number: u32, order: u8) {
        if self.poisoned() {
            return;
        }
        if order == self.max_order {
            let allocator = self.get_order_free_mut(order);
            allocator.clear(page_number);
            return;
        }

        let allocator = self.get_order_free_mut(order);
        let buddy = buddy_page(page_number);
        if buddy >= allocator.len() || allocator.get(buddy) {
            allocator.clear(page_number);
        } else {
            // Merge into higher order page
            allocator.set(buddy);
            self.free_inner(next_higher_order(page_number), order + 1);
        }
    }

    fn get_order_free_mut(&mut self, order: u8) -> &mut BtreeBitmap {
        &mut self.free[order as usize]
    }

    fn get_order_free(&self, order: u8) -> &BtreeBitmap {
        &self.free[order as usize]
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::buddy_allocator::{BuddyAllocator, TENTATIVE_ALLOCATIONS};

    /// A lowest-address allocation decides by reading each order's lowest
    /// free block; it allocates only the block it returns, never one per
    /// higher order to compare positions.
    #[test]
    fn lowest_allocation_writes_only_the_block_it_returns() {
        let mut allocator = BuddyAllocator::new(1024, 1024);
        // Free blocks now sit at every order from 0 upward.
        assert_eq!(allocator.alloc_lowest(0), Some(0));
        let before = TENTATIVE_ALLOCATIONS.with(std::cell::Cell::get);
        assert_eq!(allocator.alloc_lowest(0), Some(1));
        assert_eq!(allocator.alloc_lowest(1), Some(1));
        assert_eq!(allocator.alloc_lowest(0), Some(4));
        let tentative = TENTATIVE_ALLOCATIONS.with(std::cell::Cell::get) - before;
        assert_eq!(
            tentative, 0,
            "three lowest allocations made {tentative} tentative higher-order allocations"
        );
        assert_eq!(allocator.count_allocated_pages(), 5);
    }

    /// The read-only search returns what the trial-allocation search did and
    /// leaves the same free index, across random allocation and free orders.
    #[test]
    fn lowest_allocation_matches_the_trial_allocation_search() {
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};
        let seed = rand::rng().random();
        println!("seed={seed}");
        let mut rng = StdRng::seed_from_u64(seed);
        let pages = 4096;
        let mut allocator = BuddyAllocator::new(pages, pages);
        let mut held: Vec<(u32, u8)> = Vec::new();
        for _ in 0..4000 {
            if held.is_empty() || rng.random_bool(0.6) {
                let order = rng.random_range(0..=4u8);
                let mut oracle = BuddyAllocator::from_bytes(&allocator.to_vec());
                let expected = oracle.alloc_lowest_by_trial(order);
                let actual = allocator.alloc_lowest(order);
                assert_eq!(actual, expected, "order {order}");
                assert_eq!(
                    allocator.to_vec(),
                    oracle.to_vec(),
                    "free index after order {order}"
                );
                if let Some(page) = actual {
                    held.push((page, order));
                }
            } else {
                let (page, order) = held.swap_remove(rng.random_range(0..held.len()));
                allocator.free(page, order);
            }
        }
    }

    #[test]
    fn record_alloc_buddy() {
        let num_pages = 256;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        for page in 0..num_pages {
            allocator.record_alloc(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), num_pages);

        assert!(allocator.alloc(0).is_none());

        for page in 0..num_pages {
            allocator.free(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);
    }

    #[test]
    fn buddy_merge() {
        let num_pages = 256;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        for _ in 0..num_pages {
            allocator.alloc(0).unwrap();
        }
        for page in 0..num_pages {
            allocator.free(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);

        // Test that everything got merged back together, so that we fill order 7 allocations
        for _ in 0..(num_pages / 2u32.pow(7)) {
            allocator.alloc(7).unwrap();
        }
    }

    #[test]
    fn alloc_large() {
        let num_pages = 256;
        let max_order = 7;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        let mut allocated = vec![];
        for order in 0..=max_order {
            allocated.push((allocator.alloc(order).unwrap(), order));
        }
        assert_eq!(allocator.count_allocated_pages(), num_pages - 1);

        for order in 1..=max_order {
            assert!(allocator.alloc(order).is_none());
        }

        for (page, order) in allocated {
            allocator.free(page, order);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);
    }

    #[test]
    fn serialized_size() {
        // Check that serialized size is as expected for a full region
        let max_region_pages = 1024 * 1024;
        let allocator = BuddyAllocator::new(max_region_pages, max_region_pages);
        let max_region_pages = u64::from(max_region_pages);
        // 2x because that's the integral of 1/2^x to account for all the 21 orders
        let allocated_state_bits = 2 * max_region_pages;
        // + u32 * 21 because of the length field
        let allocated_state_bytes = allocated_state_bits / 8 + 4 * 21;

        // Add 2/64 to account for the intermediate index of the bitmap tree
        let free_state_bits = allocated_state_bits + allocated_state_bits * 2 / 64;
        // Add u32s to account for the stored offsets. Might be a bit of an over estimate
        let free_state_bytes = free_state_bits / 8 + 4 * 21 * 3 + 21 * 4;
        // BuddyAllocator overhead
        let buddy_state_bytes = free_state_bytes + allocated_state_bytes + 21 * 2 * 4 + 8;

        assert!((allocator.to_vec().len() as u64) <= buddy_state_bytes);
    }
}
