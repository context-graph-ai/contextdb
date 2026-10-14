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

#[cfg(test)]
thread_local! {
    /// Regional descriptor views this thread built, for work-count tests.
    pub(super) static REGION_VIEWS_BUILT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(super) type SavedBitmapWindow =
    dyn Fn(Option<u32>, u8, u8, u32, &mut [u8; WINDOW_BYTES]) -> io::Result<bool> + Send + Sync;
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
            if self.order_trackers[i].get(region) {
                self.order_trackers[i].clear(region);
            }
        }
    }

    pub(crate) fn orders(&self) -> usize {
        self.order_trackers.len()
    }

    pub(crate) fn mark_full(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        assert!(order < self.order_trackers.len());
        for i in order..self.order_trackers.len() {
            // Full marks are upward-closed: `mark_full` sets every order from
            // its own upward and `mark_free` clears every order up to its own,
            // so an order already marked full has every higher order marked
            // too. Stopping there keeps an allocation's tracker update to the
            // orders it actually changes instead of one read per order.
            if self.order_trackers[i].get(region) {
                break;
            }
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
    pub(super) free_totals: FreeTotals,
}

/// Allocator-wide free and region page totals, maintained by net deltas at
/// each successful allocation, free and resize so a compaction decision reads
/// them without visiting a bitmap word or a page. Fixed size: the resident
/// envelope stays independent of the region count.
///
/// Untrusted totals (repair in progress, a poisoned pager, or no valid saved
/// value at load) are never published, saved or consumed. Only a complete
/// recount whose pager stayed healthy makes them trusted again.
#[derive(Clone, Copy, Debug)]
pub(super) struct FreeTotals {
    free_pages: u64,
    region_pages: u64,
    trusted: bool,
}

/// Encoded size of the saved [`FreeTotals`]:
/// `[u32 format][u32 regions][u64 region pages][u64 free pages]`. An all-zero
/// value (format 0) is the reserved-but-unwritten state and never valid.
pub(crate) const FREE_TOTALS_ENCODED_LEN: usize = 24;
const FREE_TOTALS_FORMAT: u32 = 1;

#[cfg(feature = "test-seams")]
thread_local! {
    /// Regions visited to compute free-total geometry on this thread.
    /// Production-dead.
    static GEOMETRY_REGION_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(feature = "test-seams")]
pub(crate) fn geometry_region_visits_for_test() -> u64 {
    GEOMETRY_REGION_VISITS.with(std::cell::Cell::get)
}

/// Total region pages of a layout by arithmetic: every region but the
/// trailing one is full. Visits no region, so its cost is the same at any
/// region count.
pub(super) fn layout_pages(layout: &DatabaseLayout) -> u64 {
    u64::from(layout.num_full_regions()) * u64::from(layout.full_region_layout().num_pages())
        + layout
            .trailing_region_layout()
            .map_or(0, |trailing| u64::from(trailing.num_pages()))
}

impl FreeTotals {
    fn all_free(layout: &DatabaseLayout) -> Self {
        let pages = layout_pages(layout);
        Self {
            free_pages: pages,
            region_pages: pages,
            trusted: true,
        }
    }

    pub(super) fn untrusted() -> Self {
        Self {
            free_pages: 0,
            region_pages: 0,
            trusted: false,
        }
    }

    /// Encode the totals as saved with the allocator state that describes the
    /// same snapshot, or `None` when they must not be saved.
    pub(super) fn encode(&self, layout: &DatabaseLayout) -> Option<[u8; FREE_TOTALS_ENCODED_LEN]> {
        if !self.trusted || self.region_pages != layout_pages(layout) {
            return None;
        }
        let mut out = [0u8; FREE_TOTALS_ENCODED_LEN];
        out[0..4].copy_from_slice(&FREE_TOTALS_FORMAT.to_le_bytes());
        out[4..8].copy_from_slice(&layout.num_regions().to_le_bytes());
        out[8..16].copy_from_slice(&self.region_pages.to_le_bytes());
        out[16..24].copy_from_slice(&self.free_pages.to_le_bytes());
        Some(out)
    }

    /// Adopt saved totals for the committed geometry. The value is written
    /// before the commit's trim, and a trim removes only trailing free pages of
    /// the last region (possibly the whole region), so the committed totals are
    /// `free - (saved pages - committed pages)` over the committed pages. Any
    /// other relation between the saved and the committed geometry rejects it.
    pub(super) fn decode_for_layout(bytes: &[u8], layout: &DatabaseLayout) -> Option<Self> {
        if bytes.len() != FREE_TOTALS_ENCODED_LEN {
            return None;
        }
        let format = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let regions = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let saved_pages = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let saved_free = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let committed_regions = layout.num_regions();
        let committed_pages = layout_pages(layout);
        if format != FREE_TOTALS_FORMAT
            || saved_free > saved_pages
            || !(regions == committed_regions || regions == committed_regions + 1)
            || committed_pages > saved_pages
        {
            return None;
        }
        let trimmed = saved_pages - committed_pages;
        if trimmed > saved_free {
            return None;
        }
        Some(Self {
            free_pages: saved_free - trimmed,
            region_pages: committed_pages,
            trusted: true,
        })
    }
}

pub(super) struct PagedRegions {
    pager: Arc<AllocatorPager>,
    layout: Arc<Mutex<DatabaseLayout>>,
    saved_region_limit: Arc<AtomicU32>,
    active: Option<(u32, BuddyAllocator)>,
    // The saved table measured too large to retire at (saved regions, layout
    // regions, layout bytes, window bytes). Unchanged inputs keep the verdict.
    oversized_source: Option<(u32, u32, u64, usize)>,
}

impl Allocators {
    /// A paged allocator's operation peak is `W * P + H + C`: W windows the
    /// writer's cache share admits (at least 32), P=4096 bytes each plus their
    /// index, H=4096 bytes of stack hashing scratch plus at most one
    /// 4104-byte stack record buffer while a window reloads, and C<32KiB for tags,
    /// pager control, both descriptor trees, one regional view and source
    /// state. The source B-tree page belongs to the existing page owner. This
    /// bound follows the declared budget and is independent of region count.
    pub(super) fn resident_bytes(&self) -> usize {
        if let Some(paged) = &self.paged {
            let descriptors = self.region_tracker.allocator_metadata_bytes_for_test()
                + paged.active.as_ref().map_or(0, |(_, allocator)| {
                    allocator.allocator_metadata_bytes_for_test()
                })
                + size_of::<Allocators>()
                + 1024;
            debug_assert!(descriptors <= 16 * 1024);
            return paged.pager.resident_bytes() + descriptors.max(16 * 1024);
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
            free_totals: FreeTotals::all_free(&layout),
        }
    }

    /// Bitmap payload resides in budget-sized 4096-byte windows. The tracker and
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
                oversized_source: None,
            }),
            free_totals: FreeTotals::all_free(&layout),
        })
    }

    /// Size the window set from the writer's page-cache share. Returns the
    /// bytes it takes from that share, which the page cache gives up.
    pub(super) fn set_window_budget(&mut self, cache_bytes: usize) -> io::Result<usize> {
        let Some(paged) = &self.paged else {
            return Ok(0);
        };
        paged.pager.set_cache_budget(cache_bytes)?;
        Ok(AllocatorPager::cache_bytes_taken(cache_bytes))
    }

    /// Trusted `(free pages, region pages)`, or `None` while the totals are
    /// untrusted. Reads no bitmap word.
    pub(super) fn free_totals(&self) -> Option<(u64, u64)> {
        (self.free_totals.trusted && !self.pager_poisoned())
            .then_some((self.free_totals.free_pages, self.free_totals.region_pages))
    }

    /// Encode the totals for the allocator-state save, or `None` when they
    /// must not be saved.
    pub(super) fn encode_free_totals(
        &self,
        layout: &DatabaseLayout,
    ) -> Option<[u8; FREE_TOTALS_ENCODED_LEN]> {
        if self.pager_poisoned() {
            return None;
        }
        self.free_totals.encode(layout)
    }

    pub(super) fn set_free_totals(&mut self, totals: FreeTotals) {
        self.free_totals = totals;
    }

    /// Publish a completed recount as the trusted totals.
    pub(super) fn set_recounted_free_totals(&mut self, free_pages: u64, region_pages: u64) {
        self.free_totals = FreeTotals {
            free_pages,
            region_pages,
            trusted: true,
        };
    }

    pub(super) fn mark_free_totals_untrusted(&mut self) {
        self.free_totals.trusted = false;
    }

    fn pager_poisoned(&self) -> bool {
        self.paged
            .as_ref()
            .is_some_and(|paged| paged.pager.is_poisoned())
    }

    /// Net effect of a successful extent allocation of `order`. Call only after
    /// the allocation returned a page and the pager reported no failure.
    pub(super) fn note_allocated(&mut self, order: u8) {
        let pages = 1u64 << order;
        match self.free_totals.free_pages.checked_sub(pages) {
            Some(free) if !self.pager_poisoned() => self.free_totals.free_pages = free,
            _ => self.free_totals.trusted = false,
        }
    }

    /// Net effect of freeing an extent of `order`. The buddy free returns
    /// nothing, so a pager poisoned by that call leaves the totals untrusted.
    pub(super) fn note_freed(&mut self, order: u8) {
        let pages = 1u64 << order;
        match self.free_totals.free_pages.checked_add(pages) {
            Some(free) if !self.pager_poisoned() && free <= self.free_totals.region_pages => {
                self.free_totals.free_pages = free;
            }
            _ => self.free_totals.trusted = false,
        }
    }

    pub(super) fn set_saved_source(&mut self, source: Arc<SavedBitmapWindow>) -> io::Result<()> {
        // The saved bitmaps replace the in-memory ones; the loader adopts the
        // saved totals or recounts.
        self.free_totals.trusted = false;
        if let Some(paged) = &mut self.paged {
            paged.active = None;
            paged.oversized_source = None;
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
            self.region_tracker = RegionTracker::new_paged(
                paged.layout.lock().unwrap().num_regions(),
                MAX_MAX_PAGE_ORDER + 1,
                &paged.pager,
            );
        }
        Ok(())
    }

    pub(super) fn reset_for_repair(&mut self, layout: DatabaseLayout) -> io::Result<()> {
        let result = self.reset_for_repair_inner(layout);
        // Repair re-marks every reachable page; `end_repair` recounts.
        self.free_totals.trusted = false;
        result
    }

    fn reset_for_repair_inner(&mut self, layout: DatabaseLayout) -> io::Result<()> {
        if let Some(paged) = &mut self.paged {
            *paged.layout.lock().unwrap() = layout;
            paged.active = None;
            paged.oversized_source = None;
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
            let order = ((array % (21 * 4)) / 4) as u8;
            let height = (array % 4) as u8;
            if entity == 0 {
                return match &source {
                    Some(source) => source(None, order, height, page, out),
                    None => Ok(false),
                };
            }
            let region: u32 = (entity - 1).try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "allocator region overflow")
            })?;
            if region < saved_region_limit.load(Ordering::Acquire)
                && let Some(source) = &source
                && source(Some(region), order, height, page, out)?
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
        // Tracker words have the same saved source as regional words. Spill
        // them through the fixed windows before that source can be reclaimed.
        for bitmap in &mut self.region_tracker.order_trackers {
            bitmap.materialize_paged_words();
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
    /// complete copy fits in the floor's `WINDOWS` windows, whatever the
    /// admitted set: retiring a larger table would read the whole saved bitmap
    /// inside an ordinary commit. Larger tables stay as the lazy source until
    /// quick repair or explicit whole-file compaction materializes them. A
    /// table measured too large stays too large until the layout changes, so
    /// that verdict is kept.
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
        let paged = self.paged.as_ref().unwrap();
        let window_bytes = WINDOWS * WINDOW_BYTES;
        let layout_bytes = paged.layout.lock().unwrap().len();
        let inputs = (saved, current_regions, layout_bytes, window_bytes);
        if paged.oversized_source == Some(inputs) {
            return Ok(false);
        }
        if saved as usize > window_bytes / WINDOW_BYTES
            || !self.saved_source_fits(saved, window_bytes)?
        {
            self.paged.as_mut().unwrap().oversized_source = Some(inputs);
            return Ok(false);
        }
        self.materialize_saved_source()?;
        Ok(true)
    }

    fn saved_source_fits(&mut self, saved: u32, window_bytes: usize) -> io::Result<bool> {
        let mut encoded_bytes = self.region_tracker.encoded_len();
        if encoded_bytes > window_bytes {
            return Ok(false);
        }
        for region in 0..saved {
            encoded_bytes = encoded_bytes
                .checked_add(self.get_region(region).encoded_len())
                .ok_or_else(|| io::Error::other("allocator source size overflow"))?;
            if encoded_bytes > window_bytes {
                return Ok(false);
            }
        }
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
                #[cfg(test)]
                REGION_VIEWS_BUILT.with(|built| built.set(built.get() + 1));
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

    /// Resize every region to `new_layout`. Growth adds free pages and a trim
    /// removes only trailing free pages, so both totals move by the change in
    /// region pages.
    pub(super) fn resize_to(&mut self, new_layout: DatabaseLayout) -> io::Result<()> {
        let old_pages = match &self.paged {
            Some(paged) => layout_pages(&paged.layout.lock().unwrap()),
            // Every region but the last is full, so the old page count is
            // arithmetic over the region count and the last region's length.
            None => self.regions.last().map_or(0, |last| {
                (self.regions.len() as u64 - 1)
                    * u64::from(new_layout.full_region_layout().num_pages())
                    + u64::from(last.len())
            }),
        };
        let result = self.resize_to_inner(new_layout);
        let new_pages = layout_pages(&new_layout);
        let totals = &mut self.free_totals;
        let adjusted = if new_pages >= old_pages {
            totals.free_pages.checked_add(new_pages - old_pages)
        } else {
            totals.free_pages.checked_sub(old_pages - new_pages)
        };
        match adjusted {
            Some(free) if result.is_ok() && free <= new_pages => {
                totals.free_pages = free;
                totals.region_pages = new_pages;
            }
            _ => totals.trusted = false,
        }
        if self.pager_poisoned() {
            self.free_totals.trusted = false;
        }
        result
    }

    fn resize_to_inner(&mut self, new_layout: DatabaseLayout) -> io::Result<()> {
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
