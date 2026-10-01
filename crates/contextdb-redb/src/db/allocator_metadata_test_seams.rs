//! Retained allocator capacity for `writer_reopen_and_doubling_keep_a_fixed_resident_envelope`.

use super::Database;

impl Database {
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
