use super::allocator_pager::{AllocatorPager, bitmap_array_base};
use super::allocator_pager::{WINDOW_BYTES, WINDOWS};
use crate::ScratchBackend;
use crate::tree_store::page_store::base::MAX_REGIONS;
use crate::tree_store::page_store::bitmap::BtreeBitmap;
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::page_manager::{INITIAL_REGIONS, MAX_MAX_PAGE_ORDER};
use std::cmp::{self, max};
use std::io;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

pub(super) type SavedBitmapWindow =
    dyn Fn(u32, u8, u8, u32, &mut [u8; WINDOW_BYTES]) -> io::Result<bool> + Send + Sync;
type PagedBaseLoader = dyn Fn(u64, u32, &mut [u8; WINDOW_BYTES]) -> io::Result<bool> + Send + Sync;

// Tracks the page orders that MAY BE free in each region. This data structure is optimistic, so
// a region may not actually have a page free for a given order
pub(crate) struct RegionTracker {
    order_trackers: Vec<BtreeBitmap>,
}

impl RegionTracker {
    pub(crate) fn new_paged(regions: u32, orders: u8, pager: &Arc<AllocatorPager>) -> Self {
        let mut data = Vec::with_capacity(orders as usize);
        for order in 0..orders {
            data.push(BtreeBitmap::new_padded_paged(
                regions,
                regions,
                MAX_REGIONS,
                bitmap_array_base(None, order),
                pager,
                true,
            ));
        }
        Self {
            order_trackers: data,
        }
    }
    fn allocator_metadata_bytes_for_test(&self) -> usize {
        self.order_trackers.capacity() * size_of::<BtreeBitmap>()
            + self
                .order_trackers
                .iter()
                .map(BtreeBitmap::allocator_metadata_bytes_for_test)
                .sum::<usize>()
    }

    pub(crate) fn new(regions: u32, orders: u8) -> Self {
        let mut data = vec![];
        for _ in 0..orders {
            data.push(BtreeBitmap::new_padded(regions, regions, MAX_REGIONS));
        }
        Self {
            order_trackers: data,
        }
    }

    // Format:
    // num_orders: u32 number of order allocators
    // allocator_lens: u32 length of each allocator
    // data: BtreeBitmap data for each order
    pub(super) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![0; self.encoded_len()];
        self.encode_into(&mut result);
        result
    }

    pub(super) fn encoded_len(&self) -> usize {
        4 + self.order_trackers.len() * 4
            + self
                .order_trackers
                .iter()
                .map(BtreeBitmap::encoded_len)
                .sum::<usize>()
    }

    pub(super) fn encode_into(&self, out: &mut [u8]) {
        assert!(out.len() >= self.encoded_len());
        out[..4].copy_from_slice(
            &u32::try_from(self.order_trackers.len())
                .unwrap()
                .to_le_bytes(),
        );
        let mut offset = 4 + self.order_trackers.len() * 4;
        for (order, bitmap) in self.order_trackers.iter().enumerate() {
            let len = bitmap.encoded_len();
            let field = 4 + order * 4;
            out[field..field + 4].copy_from_slice(&u32::try_from(len).unwrap().to_le_bytes());
            bitmap.encode_into(&mut out[offset..offset + len]);
            offset += len;
        }
    }

    pub(super) fn from_bytes(page: &[u8]) -> Self {
        let orders = u32::from_le_bytes(page[..size_of::<u32>()].try_into().unwrap());
        let mut start = size_of::<u32>();
        let mut allocator_lens = vec![];
        for _ in 0..orders {
            let allocator_len =
                u32::from_le_bytes(page[start..start + size_of::<u32>()].try_into().unwrap())
                    as usize;
            allocator_lens.push(allocator_len);
            start += size_of::<u32>();
        }
        let mut data = vec![];
        for allocator_len in allocator_lens {
            data.push(BtreeBitmap::from_bytes(
                &page[start..(start + allocator_len)],
            ));
            start += allocator_len;
        }

        Self {
            order_trackers: data,
        }
    }

    pub(crate) fn find_free(&self, order: u8) -> Option<u32> {
        self.order_trackers[order as usize].find_first_unset()
    }

    pub(crate) fn mark_free(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        for i in 0..=order {
            self.order_trackers[i].clear(region);
        }
    }

    pub(crate) fn mark_full(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        assert!(order < self.order_trackers.len());
        for i in order..self.order_trackers.len() {
            self.order_trackers[i].set(region);
        }
    }

    fn resize(&mut self, new_capacity: u32) {
        for order in &mut self.order_trackers {
            order.resize(new_capacity, true);
        }
    }

    fn len(&self) -> u32 {
        self.order_trackers[0].len()
    }
}

pub(super) struct Allocators {
    pub(super) region_tracker: RegionTracker,
    pub(super) regions: Vec<BuddyAllocator>,
    pub(super) paged: Option<PagedRegions>,
}

pub(super) struct PagedRegions {
    pager: Arc<AllocatorPager>,
    layout: Arc<Mutex<DatabaseLayout>>,
    saved_region_limit: Arc<AtomicU32>,
    active: Option<(u32, BuddyAllocator)>,
}

impl Allocators {
    /// A paged allocator's operation peak is `W * P + H + C`: W=32 windows,
    /// P=4096 bytes each, H=4096 bytes of stack hashing scratch, and C<32KiB
    /// for tags, pager control, both descriptor trees, one regional view and
    /// source state. The source B-tree page belongs to the existing page owner.
    /// This bound is independent of the file's region count.
    pub(super) fn resident_bytes(&self) -> usize {
        if let Some(paged) = &self.paged {
            let descriptors = self.region_tracker.allocator_metadata_bytes_for_test()
                + paged.active.as_ref().map_or(0, |(_, allocator)| {
                    allocator.allocator_metadata_bytes_for_test()
                })
                + size_of::<Allocators>()
                + 1024;
            debug_assert!(descriptors <= 16 * 1024);
            return AllocatorPager::resident_bytes() + 16 * 1024;
        }
        let resident = self.regions.capacity() * size_of::<BuddyAllocator>()
            + self
                .regions
                .iter()
                .map(BuddyAllocator::allocator_metadata_bytes_for_test)
                .sum::<usize>()
            + self.region_tracker.allocator_metadata_bytes_for_test();
        resident + size_of::<Allocators>()
    }

    pub(super) fn new(layout: DatabaseLayout) -> Self {
        let mut regions = vec![];
        let initial_regions = max(INITIAL_REGIONS, layout.num_regions());
        let mut region_tracker = RegionTracker::new(initial_regions, MAX_MAX_PAGE_ORDER + 1);
        for i in 0..layout.num_regions() {
            let region_layout = layout.region_layout(i);
            let allocator = BuddyAllocator::new(
                region_layout.num_pages(),
                layout.full_region_layout().num_pages(),
            );
            let max_order = allocator.get_max_order();
            region_tracker.mark_free(max_order, i);
            regions.push(allocator);
        }

        Self {
            region_tracker,
            regions,
            paged: None,
        }
    }

    /// Bitmap payload resides in 32 fixed 4096-byte windows. The tracker and
    /// the one active region retain only descriptor vectors; all other region
    /// descriptors are reconstructed from scalar geometry on demand.
    pub(super) fn new_paged(
        layout: DatabaseLayout,
        scratch: Box<dyn ScratchBackend>,
    ) -> io::Result<Self> {
        let pager = AllocatorPager::new(scratch);
        let geometry = Arc::new(Mutex::new(layout));
        let saved_region_limit = Arc::new(AtomicU32::new(0));
        Self::install_paged_source(&pager, &geometry, &saved_region_limit, None)?;
        Ok(Self {
            region_tracker: RegionTracker::new_paged(
                layout.num_regions(),
                MAX_MAX_PAGE_ORDER + 1,
                &pager,
            ),
            regions: Vec::new(),
            paged: Some(PagedRegions {
                pager,
                layout: geometry,
                saved_region_limit,
                active: None,
            }),
        })
    }

    pub(super) fn set_saved_source(&mut self, source: Arc<SavedBitmapWindow>) -> io::Result<()> {
        if let Some(paged) = &mut self.paged {
            paged.active = None;
            paged.saved_region_limit.store(
                paged.layout.lock().unwrap().num_regions(),
                Ordering::Release,
            );
            Self::install_paged_source(
                &paged.pager,
                &paged.layout,
                &paged.saved_region_limit,
                Some(source),
            )?;
        }
        Ok(())
    }

    pub(super) fn reset_for_repair(&mut self, layout: DatabaseLayout) -> io::Result<()> {
        if let Some(paged) = &mut self.paged {
            *paged.layout.lock().unwrap() = layout;
            paged.active = None;
            paged.saved_region_limit.store(0, Ordering::Release);
            Self::install_paged_source(
                &paged.pager,
                &paged.layout,
                &paged.saved_region_limit,
                None,
            )?;
            self.region_tracker = RegionTracker::new_paged(
                layout.num_regions(),
                MAX_MAX_PAGE_ORDER + 1,
                &paged.pager,
            );
        } else {
            *self = Self::new(layout);
        }
        Ok(())
    }

    fn install_paged_source(
        pager: &Arc<AllocatorPager>,
        geometry: &Arc<Mutex<DatabaseLayout>>,
        saved_region_limit: &Arc<AtomicU32>,
        source: Option<Arc<SavedBitmapWindow>>,
    ) -> io::Result<()> {
        pager.set_source(Some(Self::paged_source(
            geometry,
            saved_region_limit,
            source,
        )))
    }

    fn paged_source(
        geometry: &Arc<Mutex<DatabaseLayout>>,
        saved_region_limit: &Arc<AtomicU32>,
        source: Option<Arc<SavedBitmapWindow>>,
    ) -> Box<PagedBaseLoader> {
        let geometry = Arc::clone(geometry);
        let saved_region_limit = Arc::clone(saved_region_limit);
        Box::new(move |array, page, out| {
            let entity = array / (21 * 4);
            if entity == 0 {
                return Ok(false);
            }
            let region: u32 = (entity - 1).try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "allocator region overflow")
            })?;
            let order = ((array % (21 * 4)) / 4) as u8;
            let height = (array % 4) as u8;
            if region < saved_region_limit.load(Ordering::Acquire)
                && let Some(source) = &source
                && source(region, order, height, page, out)?
            {
                return Ok(true);
            }
            let layout = *geometry.lock().unwrap();
            if region >= layout.num_regions() {
                return Ok(false);
            }
            out.fill(0xFF);
            let region_pages = layout.region_layout(region).num_pages();
            if ((region_pages >> order) & 1) == 0 {
                return Ok(true);
            }
            let mut capacity = layout.full_region_layout().num_pages();
            for _ in 0..order {
                capacity = capacity.div_ceil(2);
            }
            let mut leaf_height = 0u8;
            while capacity > 64 {
                capacity = capacity.div_ceil(64);
                leaf_height += 1;
            }
            if height > leaf_height {
                return Ok(true);
            }
            let high_bits = region_pages & !((1u32 << (order + 1)) - 1);
            let block = high_bits >> order;
            let bit = block >> (6 * (leaf_height - height));
            let start = u64::from(page) * (WINDOW_BYTES as u64 / 8) * 64;
            if u64::from(bit) >= start && u64::from(bit) < start + (WINDOW_BYTES as u64 * 8) {
                let relative = usize::try_from(u64::from(bit) - start).unwrap();
                let word = relative / 64;
                let position = relative % 64;
                let offset = word * 8;
                let mut value = u64::from_le_bytes(out[offset..offset + 8].try_into().unwrap());
                value &= !(1u64 << position);
                out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
            Ok(true)
        })
    }

    pub(super) fn pager(&self) -> Option<&Arc<AllocatorPager>> {
        self.paged.as_ref().map(|paged| &paged.pager)
    }

    pub(super) fn has_saved_source(&self) -> bool {
        self.paged
            .as_ref()
            .is_some_and(|paged| paged.saved_region_limit.load(Ordering::Acquire) != 0)
    }

    pub(super) fn materialize_saved_source(&mut self) -> io::Result<()> {
        let Some(paged) = &self.paged else {
            return Ok(());
        };
        let saved = paged.saved_region_limit.load(Ordering::Acquire);
        if saved == 0 {
            return Ok(());
        }
        for region in 0..saved.min(u32::try_from(self.region_count()).unwrap()) {
            self.get_region_mut(region).materialize_paged_words();
        }
        let paged = self.paged.as_ref().unwrap();
        if paged.pager.is_poisoned() {
            return Err(paged
                .pager
                .io_failure()
                .unwrap_or_else(|| io::Error::other("allocator scratch I/O failed")));
        }
        paged.pager.flush_and_detach_source(Self::paged_source(
            &paged.layout,
            &paged.saved_region_limit,
            None,
        ))?;
        paged.saved_region_limit.store(0, Ordering::Release);
        Ok(())
    }

    /// Retire a small saved allocator table during an ordinary commit. The
    /// complete copy fits in one fixed window set; larger tables stay as the
    /// lazy source until quick repair or explicit whole-file compaction materializes them.
    pub(super) fn materialize_saved_source_if_small(&mut self) -> io::Result<bool> {
        let Some(paged) = &self.paged else {
            return Ok(true);
        };
        let saved = paged.saved_region_limit.load(Ordering::Acquire);
        if saved == 0 {
            return Ok(true);
        }
        let current_regions = u32::try_from(self.region_count())
            .map_err(|_| io::Error::other("allocator region count overflow"))?;
        let saved = saved.min(current_regions);
        if saved as usize > WINDOWS {
            return Ok(false);
        }
        let mut encoded_bytes = 0usize;
        for region in 0..saved {
            encoded_bytes = encoded_bytes
                .checked_add(self.get_region(region).encoded_len())
                .ok_or_else(|| io::Error::other("allocator source size overflow"))?;
            if encoded_bytes > WINDOWS * WINDOW_BYTES {
                return Ok(false);
            }
        }
        self.materialize_saved_source()?;
        Ok(true)
    }

    pub(super) fn region_count(&self) -> usize {
        self.paged.as_ref().map_or(self.regions.len(), |paged| {
            paged.layout.lock().unwrap().num_regions() as usize
        })
    }

    pub(super) fn loaded_region_count(&self) -> usize {
        self.paged.as_ref().map_or(self.regions.len(), |paged| {
            usize::from(paged.active.is_some())
        })
    }

    pub(super) fn get_region(&mut self, region: u32) -> &BuddyAllocator {
        self.get_region_mut(region)
    }

    pub(super) fn get_region_mut(&mut self, region: u32) -> &mut BuddyAllocator {
        if let Some(paged) = &mut self.paged {
            if paged.active.as_ref().map(|(loaded, _)| *loaded) != Some(region) {
                let layout = *paged.layout.lock().unwrap();
                let regional = layout.region_layout(region);
                // No holder can borrow the old view across this exclusive
                // call; release its descriptor owner before building another.
                paged.active = None;
                let allocator = BuddyAllocator::new_paged(
                    regional.num_pages(),
                    layout.full_region_layout().num_pages(),
                    region,
                    &paged.pager,
                    false,
                );
                paged.active = Some((region, allocator));
                #[cfg(feature = "test-seams")]
                crate::db::allocator_metadata_test_seams::note_loaded_regions(1);
            }
            &mut paged.active.as_mut().unwrap().1
        } else {
            &mut self.regions[region as usize]
        }
    }

    pub(crate) fn xxh3_hash(&mut self) -> u128 {
        // Ignore the region tracker because it is an optimistic cache, and so may not match
        // between repairs of the allocators
        let mut result = 0;
        for region in 0..self.region_count() {
            result ^= self.get_region(u32::try_from(region).unwrap()).xxh3_hash();
        }
        result
    }

    pub(super) fn resize_to(&mut self, new_layout: DatabaseLayout) -> io::Result<()> {
        if self.paged.is_some() {
            let old_layout = *self.paged.as_ref().unwrap().layout.lock().unwrap();
            match new_layout.num_regions().cmp(&old_layout.num_regions()) {
                cmp::Ordering::Equal => {
                    let last_region = new_layout.num_regions() - 1;
                    let new_len = new_layout.region_layout(last_region).num_pages();
                    if old_layout.region_layout(last_region).num_pages() != new_len {
                        let grew = new_len > old_layout.region_layout(last_region).num_pages();
                        let allocator = self.get_region_mut(last_region);
                        allocator.resize(new_len);
                        if grew {
                            let order = allocator.highest_free_order().ok_or_else(|| {
                                self.paged
                                    .as_ref()
                                    .unwrap()
                                    .pager
                                    .io_failure()
                                    .unwrap_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "grown region has no free page",
                                        )
                                    })
                            })?;
                            self.region_tracker.mark_free(order, last_region);
                        }
                    }
                }
                cmp::Ordering::Greater => {
                    let old_last = old_layout.num_regions() - 1;
                    let new_len = new_layout.region_layout(old_last).num_pages();
                    if old_layout.region_layout(old_last).num_pages() != new_len {
                        let allocator = self.get_region_mut(old_last);
                        allocator.resize(new_len);
                        let order = allocator.highest_free_order().ok_or_else(|| {
                            self.paged
                                .as_ref()
                                .unwrap()
                                .pager
                                .io_failure()
                                .unwrap_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "grown region has no free page",
                                    )
                                })
                        })?;
                        self.region_tracker.mark_free(order, old_last);
                    }
                    for region in old_layout.num_regions()..new_layout.num_regions() {
                        if region >= self.region_tracker.len() {
                            self.region_tracker.resize(region + 1);
                        }
                        let order =
                            u8::try_from(new_layout.region_layout(region).num_pages().ilog2())
                                .unwrap();
                        self.region_tracker.mark_free(order, region);
                    }
                }
                cmp::Ordering::Less => {
                    for region in new_layout.num_regions()..old_layout.num_regions() {
                        self.region_tracker.mark_full(0, region);
                    }
                    if self
                        .paged
                        .as_ref()
                        .unwrap()
                        .active
                        .as_ref()
                        .is_some_and(|(region, _)| *region >= new_layout.num_regions())
                    {
                        self.paged.as_mut().unwrap().active = None;
                    }
                    let last_region = new_layout.num_regions() - 1;
                    let new_len = new_layout.region_layout(last_region).num_pages();
                    if old_layout.region_layout(last_region).num_pages() != new_len {
                        self.get_region_mut(last_region).resize(new_len);
                    }
                }
            }
            let paged = self.paged.as_mut().unwrap();
            if let Some(error) = paged.pager.io_failure() {
                return Err(error);
            }
            if new_layout.num_regions() < old_layout.num_regions() {
                paged.pager.truncate_regions(new_layout.num_regions())?;
                paged
                    .saved_region_limit
                    .fetch_min(new_layout.num_regions(), Ordering::AcqRel);
            }
            *paged.layout.lock().unwrap() = new_layout;
            return Ok(());
        }
        let shrink = match (new_layout.num_regions() as usize).cmp(&self.regions.len()) {
            cmp::Ordering::Less => true,
            cmp::Ordering::Equal => {
                let allocator = self.regions.last().unwrap();
                let last_region = new_layout
                    .trailing_region_layout()
                    .unwrap_or_else(|| new_layout.full_region_layout());
                match last_region.num_pages().cmp(&allocator.len()) {
                    cmp::Ordering::Less => true,
                    cmp::Ordering::Equal => {
                        // No-op
                        return Ok(());
                    }
                    cmp::Ordering::Greater => false,
                }
            }
            cmp::Ordering::Greater => false,
        };

        if shrink {
            // Drop all regions that were removed
            for i in new_layout.num_regions()..(self.regions.len().try_into().unwrap()) {
                self.region_tracker.mark_full(0, i);
            }
            self.regions.drain((new_layout.num_regions() as usize)..);

            // Resize the last region
            let last_region = new_layout
                .trailing_region_layout()
                .unwrap_or_else(|| new_layout.full_region_layout());
            let allocator = self.regions.last_mut().unwrap();
            if allocator.len() > last_region.num_pages() {
                allocator.resize(last_region.num_pages());
            }
        } else {
            let old_num_regions = self.regions.len();
            for i in 0..new_layout.num_regions() {
                let new_region = new_layout.region_layout(i);
                if (i as usize) < old_num_regions {
                    let allocator = &mut self.regions[i as usize];
                    assert!(new_region.num_pages() >= allocator.len());
                    if new_region.num_pages() != allocator.len() {
                        allocator.resize(new_region.num_pages());
                        let highest_free = allocator.highest_free_order().unwrap();
                        self.region_tracker.mark_free(highest_free, i);
                    }
                } else {
                    // brand new region
                    let allocator = BuddyAllocator::new(
                        new_region.num_pages(),
                        new_layout.full_region_layout().num_pages(),
                    );
                    let highest_free = allocator.highest_free_order().unwrap();
                    if i >= self.region_tracker.len() {
                        self.region_tracker.resize(i + 1);
                    }
                    self.region_tracker.mark_free(highest_free, i);
                    self.regions.push(allocator);
                }
            }
        }
        Ok(())
    }
}
