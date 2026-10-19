#[cfg(feature = "contextdb-internal")]
mod bounded_bytes;
#[cfg(feature = "contextdb-internal")]
pub use bounded_bytes::{BoundedByteCursor, BoundedByteHandle};
mod btree;
#[cfg(feature = "contextdb-internal")]
pub(crate) use btree::{NativeRangeEntry, fixed_key_value_location};
mod btree_base;
mod btree_iters;
mod btree_mutator;
mod page_store;
#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
pub(crate) use page_store::rewrite_header_for_test;
mod table_tree;
mod table_tree_base;

pub(crate) use btree::{
    Btree, BtreeMut, BtreeStats, PagePath, RawBtree, UntypedBtree, UntypedBtreeMut, btree_stats,
};
pub use btree_base::{AccessGuard, AccessGuardMut, AccessGuardMutInPlace};
pub(crate) use btree_base::{
    BRANCH, BranchAccessor, BranchMutator, BtreeHeader, Checksum, DEFERRED, LEAF, LeafAccessor,
    LeafMutator, RawLeafBuilder,
};
pub(crate) use btree_iters::{AllPageNumbersBtreeIter, BtreeExtractIf, BtreeRangeIter};
pub(crate) use page_store::ReadOnlyBackend;
pub(crate) use page_store::{
    FILE_FORMAT_VERSION3, MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, PAGE_SIZE, Page, PageHint, PageNumber,
    PageTrackerPolicy, SerializedSavepoint, ShrinkPolicy, TransactionalMemory,
};
pub use page_store::{InMemoryBackend, Savepoint, file_backend};
pub(crate) use table_tree::{PageListMut, TableTree, TableTreeMut};
pub(crate) use table_tree_base::{InternalTableDefinition, TableType};

#[cfg(feature = "test-seams")]
mod random_state_override;
#[cfg(feature = "test-seams")]
pub(crate) use random_state_override::fixture_random_state;
#[cfg(feature = "test-seams")]
pub use random_state_override::with_random_state_for_test;
