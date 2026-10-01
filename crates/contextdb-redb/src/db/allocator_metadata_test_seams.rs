//! Retained allocator capacity for `writer_reopen_and_doubling_keep_a_fixed_resident_envelope`.

use super::Database;

impl Database {
    /// Existing native read ownership count for one actual storage transaction id.
    /// Does not retain a transaction, allocate, or read storage pages.
    #[doc(hidden)]
    pub fn native_read_transaction_count_for_test(&self, transaction_id: u64) -> u64 {
        self.transaction_tracker
            .native_read_transaction_count_for_test(transaction_id)
    }

    #[doc(hidden)]
    pub fn allocator_regions_for_test(&self) -> usize {
        self.mem.allocator_regions_for_test()
    }

    /// Return retained allocator metadata heap payload bytes from actual vector capacities.
    ///
    /// Counts `Allocators::region_allocators`, each `BuddyAllocator::free`, each
    /// `BtreeBitmap::heights`, every `U64GroupedBitmap::data`, and the same bitmap
    /// owners reached through `RegionTracker::order_trackers`. Outer vector capacities
    /// include inline element bookkeeping and spare slots; nested allocations are
    /// counted only for initialized elements, so no owner is counted twice.
    ///
    /// This is a lower bound on heap residency: allocator rounding/headers are unknown.
    /// It excludes fixed inline `TransactionalMemory` state, page caches, commit page
    /// sets, debug-only page tracking, temporary serialization/open buffers, and any
    /// old/new allocation overlap inside a vector reallocation. Those owners require
    /// separate observations; this settled snapshot is not complete reconciliation.
    ///
    /// Takes only the allocator state mutex, allocates nothing, and reads no storage
    /// pages. It can observe retained state after open or file growth without starting
    /// a transaction or hydrating history. The seam is absent from normal builds.
    #[doc(hidden)]
    pub fn allocator_metadata_bytes_for_test(&self) -> usize {
        self.mem.allocator_metadata_bytes_for_test()
    }
}

thread_local! {
    static REGION_LOADS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}
pub(crate) fn note_loaded_regions(regions: usize) {
    REGION_LOADS.with(|slot| {
        if let Some(count) = slot.get() {
            slot.set(Some(count + regions as u64));
        }
    });
}
/// Observe the actual allocator state's region count after its durable load.
pub fn observe_allocator_region_loads_for_test<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    struct Restore(Option<u64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            REGION_LOADS.with(|slot| slot.set(self.0));
        }
    }
    let restore = Restore(REGION_LOADS.with(|slot| slot.replace(Some(0))));
    let value = operation();
    let count = REGION_LOADS.with(|slot| slot.get().unwrap());
    drop(restore);
    (value, count)
}
