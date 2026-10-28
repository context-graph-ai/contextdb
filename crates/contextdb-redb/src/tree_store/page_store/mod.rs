mod allocator_pager;
mod backends;
mod base;
mod bitmap;
mod buddy_allocator;
mod cached_file;
mod fast_hash;
pub mod file_backend;
mod header;
mod layout;
mod lru_cache;
mod page_allocation;
mod page_manager;
#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
pub(crate) use page_manager::rewrite_header_for_test;
mod region;
mod savepoint;
#[allow(clippy::pedantic)]
mod xxh3;

pub use backends::InMemoryBackend;
pub(crate) use backends::ReadOnlyBackend;
pub(crate) use base::{
    MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, Page, PageHint, PageNumber, PageTrackerPolicy,
};
pub(crate) use cached_file::PageMemory;
pub(crate) use header::PAGE_SIZE;
pub(crate) use page_manager::{
    FILE_FORMAT_VERSION3, ShrinkPolicy, TransactionalMemory, xxh3_checksum,
};
pub use savepoint::Savepoint;
pub(crate) use savepoint::SerializedSavepoint;

pub(super) use base::{PageImpl, PageMut};
pub(super) use xxh3::hash128_with_seed;

#[cfg(feature = "contextdb-internal")]
pub(super) use xxh3::hash128_from_windows;

#[cfg(feature = "contextdb-internal")]
pub use page_allocation::CacheReserveOwner;

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use cached_file::{CachePageForTest, CacheWriteControlForTest, CacheWriteForTest};

#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
#[doc(hidden)]
pub use cached_file::CheckedWriterBackendForTest;

#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
pub use cached_file::CachePressureFixtureForTest;

#[cfg(feature = "contextdb-internal")]
pub(crate) fn native_dirty_page_layout(bytes: usize) -> Option<std::alloc::Layout> {
    page_allocation::PageAllocation::layout(bytes)
        .ok()
        .map(|(layout, _)| layout)
}
