use crate::db::TransactionGuard;
use crate::error::CommitError;
use crate::multimap_table::ReadOnlyUntypedMultimapTable;
use crate::sealed::Sealed;
use crate::table::ReadOnlyUntypedTable;
use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    Btree, BtreeHeader, BtreeMut, InternalTableDefinition, LeafAccessor, MAX_PAIR_LENGTH,
    MAX_VALUE_LENGTH, Page, PageHint, PageListMut, PageNumber, PagePath, PageTrackerPolicy,
    SerializedSavepoint, ShrinkPolicy, TableTree, TableTreeMut, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{
    AccessGuard, AccessGuardMutInPlace, ExtractIf, MultimapTable, MultimapTableDefinition,
    MultimapTableHandle, MutInPlaceValue, Range, ReadOnlyMultimapTable, ReadOnlyTable, Result,
    Savepoint, SavepointError, SetDurabilityError, StorageError, Table, TableDefinition,
    TableError, TableHandle, TransactionError, TypeName, UntypedMultimapTableHandle,
    UntypedTableHandle,
};
#[cfg(feature = "logging")]
use log::{debug, warn};
use std::borrow::Borrow;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{panic, thread};

/// Minimum ordinary release allowance and maximum compaction staging addresses.
pub const RELEASE_PAGES: usize = 4096;
const RELEASE_BATCH: usize = 400;
pub(crate) const CHAIN_PAGINATION: u64 = 1 << 63;
const MAX_RELEASE_PAGES: usize = 131_072;
const COMPACTION_TABLE: SystemTableDefinition<(), &[u8]> =
    SystemTableDefinition::new("compaction_position");
const NEXT_SAVEPOINT_TABLE: SystemTableDefinition<(), SavepointId> =
    SystemTableDefinition::new("next_savepoint_id");
pub(crate) const SAVEPOINT_TABLE: SystemTableDefinition<SavepointId, SerializedSavepoint> =
    SystemTableDefinition::new("persistent_savepoints");
// Pages that were allocated in the data tree by a given transaction. Only updated when a savepoint
// exists
pub(crate) const DATA_ALLOCATED_TABLE: SystemTableDefinition<
    TransactionIdWithPagination,
    PageList,
> = SystemTableDefinition::new("data_pages_allocated");
// Pages in the data tree that are in the pending free state: i.e., they are unreachable from the
// root as of the given transaction.
pub(crate) const DATA_FREED_TABLE: SystemTableDefinition<TransactionIdWithPagination, PageList> =
    SystemTableDefinition::new("data_pages_unreachable");
// Pages in the system tree that are in the pending free state: i.e., they are unreachable from the
// root as of the given transaction.
pub(crate) const SYSTEM_FREED_TABLE: SystemTableDefinition<TransactionIdWithPagination, PageList> =
    SystemTableDefinition::new("system_pages_unreachable");
// The allocator state table is stored in the system table tree, but it's accessed using
// raw btree operations rather than open_system_table(), so there's no SystemTableDefinition
pub(crate) const ALLOCATOR_STATE_TABLE_NAME: &str = "allocator_state";
pub(crate) type AllocatorStateTree = Btree<AllocatorStateKey, &'static [u8]>;
pub(crate) type AllocatorStateTreeMut<'a> = BtreeMut<'a, AllocatorStateKey, &'static [u8]>;
pub(crate) type SystemFreedTree<'a> = BtreeMut<'a, TransactionIdWithPagination, PageList<'static>>;

// Format:
// 2 bytes: length
// length * size_of(PageNumber): array of page numbers
#[derive(Debug)]
pub(crate) struct PageList<'a> {
    data: &'a [u8],
}

impl PageList<'_> {
    fn required_bytes(len: usize) -> usize {
        2 + PageNumber::serialized_size() * len
    }

    pub(crate) fn len(&self) -> usize {
        u16::from_le_bytes(self.data[..size_of::<u16>()].try_into().unwrap()).into()
    }

    pub(crate) fn get(&self, index: usize) -> PageNumber {
        let start = size_of::<u16>() + PageNumber::serialized_size() * index;
        PageNumber::from_le_bytes(
            self.data[start..(start + PageNumber::serialized_size())]
                .try_into()
                .unwrap(),
        )
    }
}

impl Value for PageList<'_> {
    type SelfType<'a>
        = PageList<'a>
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        PageList { data }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'b [u8]
    where
        Self: 'b,
    {
        value.data
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::PageList")
    }
}

impl MutInPlaceValue for PageList<'_> {
    type BaseRefType = PageListMut;

    fn initialize(data: &mut [u8]) {
        assert!(data.len() >= 8);
        // Set the length to zero
        data[..8].fill(0);
    }

    fn from_bytes_mut(data: &mut [u8]) -> &mut Self::BaseRefType {
        unsafe { &mut *(std::ptr::from_mut::<[u8]>(data) as *mut PageListMut) }
    }
}

#[derive(Debug)]
pub(crate) struct TransactionIdWithPagination {
    pub(crate) transaction_id: u64,
    pub(crate) pagination_id: u64,
}

impl Value for TransactionIdWithPagination {
    type SelfType<'a>
        = TransactionIdWithPagination
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 2 * size_of::<u64>()]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(2 * size_of::<u64>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        let transaction_id = u64::from_le_bytes(data[..size_of::<u64>()].try_into().unwrap());
        let pagination_id = u64::from_le_bytes(data[size_of::<u64>()..].try_into().unwrap());
        Self {
            transaction_id,
            pagination_id,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 2 * size_of::<u64>()]
    where
        Self: 'b,
    {
        let mut result = [0u8; 2 * size_of::<u64>()];
        result[..size_of::<u64>()].copy_from_slice(&value.transaction_id.to_le_bytes());
        result[size_of::<u64>()..].copy_from_slice(&value.pagination_id.to_le_bytes());
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::TransactionIdWithPagination")
    }
}

impl Key for TransactionIdWithPagination {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = Self::from_bytes(data1);
        let value2 = Self::from_bytes(data2);

        match value1.transaction_id.cmp(&value2.transaction_id) {
            std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => value1.pagination_id.cmp(&value2.pagination_id),
            std::cmp::Ordering::Less => std::cmp::Ordering::Less,
        }
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) enum AllocatorStateKey {
    Deprecated,
    Region(u32),
    RegionTracker,
    TransactionId,
}

impl Value for AllocatorStateKey {
    type SelfType<'a> = Self;
    type AsBytes<'a> = [u8; 1 + size_of::<u32>()];

    fn fixed_width() -> Option<usize> {
        Some(1 + size_of::<u32>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        match data[0] {
            // 0, 1, 2 were used in redb 2.x and have a different format
            0..=2 => Self::Deprecated,
            3 => Self::Region(u32::from_le_bytes(data[1..].try_into().unwrap())),
            4 => Self::RegionTracker,
            5 => Self::TransactionId,
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut result = Self::AsBytes::default();
        match value {
            Self::Region(region) => {
                result[0] = 3;
                result[1..].copy_from_slice(&u32::to_le_bytes(*region));
            }
            Self::RegionTracker => {
                result[0] = 4;
            }
            Self::TransactionId => {
                result[0] = 5;
            }
            AllocatorStateKey::Deprecated => {
                result[0] = 0;
            }
        }

        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::AllocatorStateKey")
    }
}

impl Key for AllocatorStateKey {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}

pub struct SystemTableDefinition<'a, K: Key + 'static, V: Value + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> SystemTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for SystemTableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Value> Sealed for SystemTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Clone for SystemTableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Value + 'static> Copy for SystemTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Display for SystemTableDefinition<'_, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

/// Informational storage stats about the database
#[derive(Debug)]
pub struct DatabaseStats {
    pub(crate) tree_height: u32,
    pub(crate) allocated_pages: u64,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
    pub(crate) page_size: usize,
}

impl DatabaseStats {
    /// Maximum traversal distance to reach the deepest (key, value) pair, across all tables
    pub fn tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Number of pages allocated
    pub fn allocated_pages(&self) -> u64 {
        self.allocated_pages
    }

    /// Number of leaf pages that store user data
    pub fn leaf_pages(&self) -> u64 {
        self.leaf_pages
    }

    /// Number of branch pages in btrees that store user data
    pub fn branch_pages(&self) -> u64 {
        self.branch_pages
    }

    /// Number of bytes consumed by keys and values that have been inserted.
    /// Does not include indexing overhead
    pub fn stored_bytes(&self) -> u64 {
        self.stored_leaf_bytes
    }

    /// Number of bytes consumed by keys in internal branch pages, plus other metadata
    pub fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }

    /// Number of bytes consumed by fragmentation, both in data pages and internal metadata tables
    pub fn fragmented_bytes(&self) -> u64 {
        self.fragmented_bytes
    }

    /// Number of bytes per page
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum Durability {
    /// Commits with this durability level will not be persisted to disk unless followed by a
    /// commit with [`Durability::Immediate`].
    None,
    /// Commits with this durability level are guaranteed to be persistent as soon as
    /// [`WriteTransaction::commit`] returns.
    Immediate,
}

// These are the actual durability levels used internally. `Durability::Paranoid` is translated
// to `InternalDurability::Immediate`, and also enables 2-phase commit
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InternalDurability {
    None,
    Immediate,
}

// Like a Table but only one may be open at a time to avoid possible races
pub struct SystemTable<'db, 's, K: Key + 'static, V: Value + 'static> {
    name: String,
    namespace: &'s mut SystemNamespace<'db>,
    tree: BtreeMut<'s, K, V>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<'db, 's, K: Key + 'static, V: Value + 'static> SystemTable<'db, 's, K, V> {
    fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        namespace: &'s mut SystemNamespace<'db>,
    ) -> SystemTable<'db, 's, K, V> {
        // No need to track allocations in the system tree. Savepoint restoration only relies on
        // freeing in the data tree
        let ignore = Arc::new(Mutex::new(PageTrackerPolicy::Ignore));
        SystemTable {
            name: name.to_string(),
            namespace,
            tree: BtreeMut::new(table_root, guard.clone(), mem, freed_pages, ignore),
            transaction_guard: guard,
        }
    }

    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<'_, V>>>
    where
        K: 'a,
    {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<'_, K, V>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction_guard.clone()))
    }

    pub fn extract_from_if<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result<ExtractIf<'_, K, V, F>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .extract_from_if(&range, predicate)
            .map(ExtractIf::new)
    }

    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<Option<AccessGuard<'_, V>>> {
        let value_len = V::as_bytes(value.borrow()).as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_len + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len + key_len));
        }
        self.tree.insert(key.borrow(), value.borrow())
    }

    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'_, V>>>
    where
        K: 'a,
    {
        self.tree.remove(key.borrow())
    }
}

impl<K: Key + 'static, V: MutInPlaceValue + 'static> SystemTable<'_, '_, K, V> {
    pub fn insert_reserve<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
        value_length: usize,
    ) -> Result<AccessGuardMutInPlace<'_, V>> {
        if value_length > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_length + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length + key_len));
        }
        self.tree.insert_reserve(key.borrow(), value_length)
    }
}

impl<K: Key + 'static, V: Value + 'static> Drop for SystemTable<'_, '_, K, V> {
    fn drop(&mut self) {
        self.namespace.close_table(
            &self.name,
            &self.tree,
            self.tree.get_root().map(|x| x.length).unwrap_or_default(),
        );
    }
}

struct SystemNamespace<'db> {
    table_tree: TableTreeMut<'db>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<'db> SystemNamespace<'db> {
    fn new(
        root_page: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        // No need to track allocations in the system tree. Savepoint restoration only relies on
        // freeing in the data tree
        let ignore = Arc::new(Mutex::new(PageTrackerPolicy::Ignore));
        let freed_pages = Arc::new(Mutex::new(vec![]));
        Self {
            table_tree: TableTreeMut::new(
                root_page,
                guard.clone(),
                mem,
                freed_pages.clone(),
                ignore,
            ),
            freed_pages,
            transaction_guard: guard.clone(),
        }
    }

    fn system_freed_pages(&self) -> Arc<Mutex<Vec<PageNumber>>> {
        self.freed_pages.clone()
    }

    fn open_system_table<'txn, 's, K: Key + 'static, V: Value + 'static>(
        &'s mut self,
        transaction: &'txn WriteTransaction,
        definition: SystemTableDefinition<K, V>,
    ) -> Result<SystemTable<'db, 's, K, V>> {
        let (root, _) = self
            .table_tree
            .get_or_create_table::<K, V>(definition.name(), TableType::Normal)
            .map_err(|e| {
                e.into_storage_error_or_corrupted("Internal error. System table is corrupted")
            })?;
        transaction.dirty.store(true, Ordering::Release);

        Ok(SystemTable::new(
            definition.name(),
            root,
            self.freed_pages.clone(),
            self.transaction_guard.clone(),
            transaction.mem.clone(),
            self,
        ))
    }

    fn close_table<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        table: &BtreeMut<K, V>,
        length: u64,
    ) {
        self.table_tree
            .stage_update_table_root(name, table.get_root(), length);
    }
}

struct TableNamespace<'db> {
    open_tables: HashMap<String, &'static panic::Location<'static>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    table_tree: TableTreeMut<'db>,
}

impl TableNamespace<'_> {
    fn new(
        root_page: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        let allocated = Arc::new(Mutex::new(PageTrackerPolicy::new_tracking()));
        let freed_pages = Arc::new(Mutex::new(vec![]));
        let table_tree = TableTreeMut::new(
            root_page,
            guard,
            mem,
            // Committed pages which are no longer reachable and will be queued for free'ing
            // These are separated from the system freed pages
            freed_pages.clone(),
            allocated.clone(),
        );
        Self {
            open_tables: Default::default(),
            table_tree,
            freed_pages,
            allocated_pages: allocated,
        }
    }

    fn set_dirty(&mut self, transaction: &WriteTransaction) {
        transaction.dirty.store(true, Ordering::Release);
        if !transaction.transaction_tracker.any_savepoint_exists() {
            // No savepoints exist, and we don't allow savepoints to be created in a dirty transaction
            // so we can disable allocation tracking now
            *self.allocated_pages.lock().unwrap() = PageTrackerPolicy::Ignore;
        }
    }

    fn set_root(&mut self, root: Option<BtreeHeader>) {
        assert!(self.open_tables.is_empty());
        self.table_tree.set_root(root);
    }

    #[track_caller]
    fn inner_open<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<(Option<BtreeHeader>, u64), TableError> {
        if let Some(location) = self.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }

        let root = self
            .table_tree
            .get_or_create_table::<K, V>(name, table_type)?;
        self.open_tables
            .insert(name.to_string(), panic::Location::caller());

        Ok(root)
    }

    #[track_caller]
    pub fn open_multimap_table<'txn, K: Key + 'static, V: Key + 'static>(
        &mut self,
        transaction: &'txn WriteTransaction,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'txn, K, V>, TableError> {
        #[cfg(feature = "logging")]
        debug!("Opening multimap table: {definition}");
        let (root, length) = self.inner_open::<K, V>(definition.name(), TableType::Multimap)?;
        self.set_dirty(transaction);

        Ok(MultimapTable::new(
            definition.name(),
            root,
            length,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            transaction.mem.clone(),
            transaction,
        ))
    }

    #[track_caller]
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &mut self,
        transaction: &'txn WriteTransaction,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'txn, K, V>, TableError> {
        #[cfg(feature = "logging")]
        debug!("Opening table: {definition}");
        let (root, _) = self.inner_open::<K, V>(definition.name(), TableType::Normal)?;
        self.set_dirty(transaction);

        Ok(Table::new(
            definition.name(),
            root,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            transaction.mem.clone(),
            transaction,
        ))
    }

    #[track_caller]
    fn inner_rename(
        &mut self,
        name: &str,
        new_name: &str,
        table_type: TableType,
    ) -> Result<(), TableError> {
        if let Some(location) = self.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }

        self.table_tree.rename_table(name, new_name, table_type)
    }

    #[track_caller]
    fn rename_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
        new_name: &str,
    ) -> Result<(), TableError> {
        #[cfg(feature = "logging")]
        debug!("Renaming table: {name} to {new_name}");
        self.set_dirty(transaction);
        self.inner_rename(name, new_name, TableType::Normal)
    }

    #[track_caller]
    fn rename_multimap_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
        new_name: &str,
    ) -> Result<(), TableError> {
        #[cfg(feature = "logging")]
        debug!("Renaming multimap table: {name} to {new_name}");
        self.set_dirty(transaction);
        self.inner_rename(name, new_name, TableType::Multimap)
    }

    #[track_caller]
    fn inner_delete(&mut self, name: &str, table_type: TableType) -> Result<bool, TableError> {
        if let Some(location) = self.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }

        self.table_tree.delete_table(name, table_type)
    }

    #[track_caller]
    fn delete_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
    ) -> Result<bool, TableError> {
        #[cfg(feature = "logging")]
        debug!("Deleting table: {name}");
        self.set_dirty(transaction);
        self.inner_delete(name, TableType::Normal)
    }

    #[track_caller]
    fn delete_multimap_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
    ) -> Result<bool, TableError> {
        #[cfg(feature = "logging")]
        debug!("Deleting multimap table: {name}");
        self.set_dirty(transaction);
        self.inner_delete(name, TableType::Multimap)
    }

    pub(crate) fn close_table<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        table: &BtreeMut<K, V>,
        length: u64,
    ) {
        self.open_tables.remove(name).unwrap();
        self.table_tree
            .stage_update_table_root(name, table.get_root(), length);
    }
}

// Transaction-local savepoint lifecycle state.
#[derive(Default)]
struct SavepointTransactionState {
    created_persistent: HashSet<(SavepointId, TransactionId)>,
    deleted_persistent: Vec<(SavepointId, TransactionId)>,
    invalidated: BTreeSet<SavepointId>,
}

impl SavepointTransactionState {
    fn record_created(&mut self, id: SavepointId, transaction_id: TransactionId) {
        self.created_persistent.insert((id, transaction_id));
    }

    fn record_deleted(&mut self, id: SavepointId, transaction_id: TransactionId) {
        self.deleted_persistent.push((id, transaction_id));
    }

    fn record_invalidated(&mut self, ids: impl IntoIterator<Item = SavepointId>) {
        self.invalidated.extend(ids);
    }

    fn is_invalidated(&self, id: SavepointId) -> bool {
        self.invalidated.contains(&id)
    }

    fn has_created_or_deleted(&self) -> bool {
        !self.created_persistent.is_empty() || !self.deleted_persistent.is_empty()
    }

    fn apply_on_commit(&mut self, tracker: &TransactionTracker) {
        // Persistent savepoints whose on-disk entry was deleted: release their
        // tracker refcount now that the deletion is durable.
        for (savepoint, transaction) in self.deleted_persistent.drain(..) {
            tracker.deallocate_savepoint(savepoint, transaction);
        }
        // Savepoints that restore_savepoint() invalidated: remove them from the
        // shared valid_savepoints map. For persistent savepoints,
        // deallocate_savepoint above has already removed them; for ephemeral,
        // the user's Savepoint handle still owns the live_read_transactions
        // refcount and will release it on drop.
        tracker.invalidate_savepoints(std::mem::take(&mut self.invalidated));
        // Persistent savepoints created during this transaction stay live:
        // drop them from our bookkeeping without releasing tracker state.
        self.created_persistent.clear();
    }

    fn apply_on_abort(&mut self, tracker: &TransactionTracker) {
        // Persistent savepoints created during this transaction: their
        // on-disk entries will be rolled back by rollback_uncommitted_writes(),
        // but the shared tracker registration must be released explicitly.
        for (savepoint, transaction) in self.created_persistent.drain() {
            tracker.deallocate_savepoint(savepoint, transaction);
        }
        // Deleted-persistent entries will be rolled back on disk, so the
        // tracker state must NOT be released (it is still valid).
        self.deleted_persistent.clear();
        // Invalidations were only staged in this struct and never touched the
        // shared tracker, so dropping them is sufficient.
        self.invalidated.clear();
    }
}

/// The pages one step of [`WriteTransaction::compact_pages`] read, moved and wrote, and whether
/// it ended its pass
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStep {
    /// Pages the walk read to find what moves
    pub read: u64,
    /// Pages moved, each to a place wholly below it
    pub moved: u64,
    /// Pages written: the moved pages and the pages above them
    pub written: u64,
    /// The pass ended, its place cleared: a whole walk moved nothing, or the pass was finished
    pub ended: bool,
}

/// A read/write transaction
///
/// Only a single [`WriteTransaction`] may exist at a time
pub struct WriteTransaction {
    transaction_tracker: Arc<TransactionTracker>,
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    transaction_id: TransactionId,
    tables: Mutex<TableNamespace<'static>>,
    system_tables: Mutex<SystemNamespace<'static>>,
    completed: bool,
    growth_limit: Option<u64>,
    prepared_commit: Option<(Option<BtreeHeader>, u64)>,
    released_pages: Vec<[u8; 8]>,
    // Allowance units the step's early release spent, which the commit's release does not repeat
    release_spent: usize,
    dirty: AtomicBool,
    durability: InternalDurability,
    two_phase_commit: bool,
    shrink_policy: ShrinkPolicy,
    quick_repair: bool,
    // All transaction-local savepoint lifecycle state. See
    // `SavepointTransactionState` for the commit/abort contract.
    savepoint_state: Mutex<SavepointTransactionState>,
    freed_pagination: [Mutex<u64>; 2],
    flushed_freed_pages: [AtomicU64; 2],
    // The free pages and the file's length when the transaction began
    begun: (u64, u64),
}

impl WriteTransaction {
    /// Bounded recovery must lend one list, cap 4096 entries, and limit deferred frees to entries + height.
    pub const DRAIN_BYTES: usize = size_of::<PageNumber>()
        * (400 + 4096 + 3 * (4096usize + 64).next_power_of_two())
        + 2
        + 400 * PageNumber::serialized_size();

    pub(crate) fn new(
        guard: TransactionGuard,
        transaction_tracker: Arc<TransactionTracker>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let transaction_id = guard.id();
        let guard = Arc::new(guard);

        let root_page = mem.get_data_root();
        let system_page = mem.get_system_root();

        let tables = TableNamespace::new(root_page, guard.clone(), mem.clone());
        let system_tables = SystemNamespace::new(system_page, guard.clone(), mem.clone());
        mem.taken.lock().unwrap().0 = 0;

        Ok(Self {
            transaction_tracker,
            mem: mem.clone(),
            transaction_guard: guard.clone(),
            transaction_id,
            tables: Mutex::new(tables),
            system_tables: Mutex::new(system_tables),
            completed: false,
            growth_limit: None,
            prepared_commit: None,
            released_pages: Vec::new(),
            release_spent: 0,
            dirty: AtomicBool::new(false),
            durability: InternalDurability::Immediate,
            two_phase_commit: false,
            quick_repair: false,
            shrink_policy: ShrinkPolicy::Default,
            savepoint_state: Mutex::new(SavepointTransactionState::default()),
            freed_pagination: Default::default(),
            flushed_freed_pages: Default::default(),
            begun: (mem.count_free_pages()?, mem.file_len()?),
        })
    }

    pub(crate) fn set_shrink_policy(&mut self, shrink_policy: ShrinkPolicy) {
        self.shrink_policy = shrink_policy;
    }

    /// Whether maximum trimming can cut an already-free tail from the file.
    pub fn has_free_tail(&self) -> Result<bool> {
        self.mem.has_free_tail()
    }

    /// Whether pending frees remain beyond the newest retained commit proof.
    pub fn pending_free_pages(&self) -> Result<bool> {
        let mut system_tables = self.system_tables.lock().unwrap();
        if system_tables
            .open_system_table(self, DATA_FREED_TABLE)?
            .tree
            .get_root()
            .is_some()
        {
            return Ok(true);
        }
        let system_freed = system_tables.open_system_table(self, SYSTEM_FREED_TABLE)?;
        if let Some((key, _)) = system_freed.tree.first()? {
            // Every commit holds its own bookkeeping pages for the next: only older ones drain.
            let key = key.value();
            return Ok(key.transaction_id < self.mem.get_last_committed_transaction_id()?.raw_id());
        }

        Ok(false)
    }

    #[cfg(debug_assertions)]
    pub fn print_allocated_page_debug(&self) {
        let mut all_allocated: HashSet<PageNumber> =
            HashSet::from_iter(self.mem.all_allocated_pages());

        self.mem.debug_check_allocator_consistency();

        let mut table_pages = vec![];
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .visit_all_pages(|path| {
                table_pages.push(path.page_number());
                Ok(())
            })
            .unwrap();
        println!("Tables");
        for p in table_pages {
            assert!(all_allocated.remove(&p));
            println!("{p:?}");
        }

        let mut system_table_pages = vec![];
        self.system_tables
            .lock()
            .unwrap()
            .table_tree
            .visit_all_pages(|path| {
                system_table_pages.push(path.page_number());
                Ok(())
            })
            .unwrap();
        println!("System tables");
        for p in system_table_pages {
            assert!(all_allocated.remove(&p));
            println!("{p:?}");
        }

        {
            println!("Pending free (in data freed table)");
            let mut system_tables = self.system_tables.lock().unwrap();
            let data_freed = system_tables
                .open_system_table(self, DATA_FREED_TABLE)
                .unwrap();
            for entry in data_freed.range::<TransactionIdWithPagination>(..).unwrap() {
                let (_, entry) = entry.unwrap();
                let value = entry.value();
                for i in 0..value.len() {
                    let p = value.get(i);
                    assert!(all_allocated.remove(&p));
                    println!("{p:?}");
                }
            }
        }
        {
            println!("Pending free (in system freed table)");
            let mut system_tables = self.system_tables.lock().unwrap();
            let system_freed = system_tables
                .open_system_table(self, SYSTEM_FREED_TABLE)
                .unwrap();
            for entry in system_freed
                .range::<TransactionIdWithPagination>(..)
                .unwrap()
            {
                let (_, entry) = entry.unwrap();
                let value = entry.value();
                for i in 0..value.len() {
                    let p = value.get(i);
                    assert!(all_allocated.remove(&p));
                    println!("{p:?}");
                }
            }
        }
        {
            let tables = self.tables.lock().unwrap();
            let pages = tables.freed_pages.lock().unwrap();
            if !pages.is_empty() {
                println!("Pages in in-memory data freed_pages");
                for p in pages.iter() {
                    println!("{p:?}");
                    assert!(all_allocated.remove(p));
                }
            }
        }
        {
            let system_tables = self.system_tables.lock().unwrap();
            let pages = system_tables.freed_pages.lock().unwrap();
            if !pages.is_empty() {
                println!("Pages in in-memory system freed_pages");
                for p in pages.iter() {
                    println!("{p:?}");
                    assert!(all_allocated.remove(p));
                }
            }
        }
        if !all_allocated.is_empty() {
            println!("Leaked pages");
            for p in all_allocated {
                println!("{p:?}");
            }
        }
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database.
    /// This savepoint will exist until it is deleted with `[delete_savepoint()]`.
    ///
    /// Note that while a savepoint exists, pages that become unused after it was created are not freed.
    /// Therefore, the lifetime of a savepoint should be minimized.
    ///
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened),
    /// or `[SavepointError::ImmediateDurabilityRequired]` if the transaction's durability is less than
    /// `[Durability::Immediate]`
    pub fn persistent_savepoint(&self) -> Result<u64, SavepointError> {
        if self.durability != InternalDurability::Immediate {
            return Err(SavepointError::ImmediateDurabilityRequired);
        }

        let mut savepoint = self.ephemeral_savepoint()?;

        let mut system_tables = self.system_tables.lock().unwrap();

        let mut next_table = system_tables.open_system_table(self, NEXT_SAVEPOINT_TABLE)?;
        next_table.insert((), savepoint.get_id().next())?;
        drop(next_table);

        let mut savepoint_table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        savepoint_table.insert(
            savepoint.get_id(),
            SerializedSavepoint::from_savepoint(&savepoint),
        )?;

        savepoint.set_persistent();

        self.savepoint_state
            .lock()
            .unwrap()
            .record_created(savepoint.get_id(), savepoint.get_transaction_id());

        Ok(savepoint.get_id().0)
    }

    pub(crate) fn transaction_guard(&self) -> Arc<TransactionGuard> {
        self.transaction_guard.clone()
    }

    pub(crate) fn next_persistent_savepoint_id(&self) -> Result<Option<SavepointId>> {
        let mut system_tables = self.system_tables.lock().unwrap();
        if system_tables
            .table_tree
            .get_table_untyped(NEXT_SAVEPOINT_TABLE.name(), TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Invalid savepoint table"))?
            .is_none()
        {
            return Ok(None);
        }
        let next_table = system_tables.open_system_table(self, NEXT_SAVEPOINT_TABLE)?;
        let value = next_table.get(())?;
        if let Some(next_id) = value {
            Ok(Some(next_id.value()))
        } else {
            Ok(None)
        }
    }

    /// Get a persistent savepoint given its id
    pub fn get_persistent_savepoint(&self, id: u64) -> Result<Savepoint, SavepointError> {
        let mut system_tables = self.system_tables.lock().unwrap();
        let table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let value = table.get(SavepointId(id))?;

        value
            .map(|x| x.value().to_savepoint(self.transaction_tracker.clone()))
            .ok_or(SavepointError::InvalidSavepoint)
    }

    /// Delete the given persistent savepoint.
    ///
    /// Note that if the transaction is `abort()`'ed this deletion will be rolled back.
    ///
    /// Returns `true` if the savepoint existed
    /// Returns `[SavepointError::ImmediateDurabilityRequired]` if the transaction's durability
    /// is less than `[Durability::Immediate]`
    pub fn delete_persistent_savepoint(&self, id: u64) -> Result<bool, SavepointError> {
        if self.durability != InternalDurability::Immediate {
            return Err(SavepointError::ImmediateDurabilityRequired);
        }
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let savepoint = table.remove(SavepointId(id))?;
        if let Some(serialized) = savepoint {
            let savepoint = serialized
                .value()
                .to_savepoint(self.transaction_tracker.clone());
            self.savepoint_state
                .lock()
                .unwrap()
                .record_deleted(savepoint.get_id(), savepoint.get_transaction_id());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all persistent savepoints
    pub fn list_persistent_savepoints(&self) -> Result<impl Iterator<Item = u64>> {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut savepoints = vec![];
        if system_tables
            .table_tree
            .get_table_untyped(SAVEPOINT_TABLE.name(), TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Invalid savepoint table"))?
            .is_none()
        {
            return Ok(savepoints.into_iter());
        }
        let table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        for savepoint in table.range::<SavepointId>(..)? {
            savepoints.push(savepoint?.0.value().0);
        }
        Ok(savepoints.into_iter())
    }

    // TODO: deduplicate this with the one in Database
    fn allocate_read_transaction(&self) -> Result<TransactionGuard> {
        let id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;

        Ok(TransactionGuard::new_read(
            id,
            self.transaction_tracker.clone(),
        ))
    }

    fn allocate_savepoint(&self) -> Result<(SavepointId, TransactionId)> {
        let transaction_id = self.allocate_read_transaction()?.leak();
        let id = self.transaction_tracker.allocate_savepoint(transaction_id);
        Ok((id, transaction_id))
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database
    ///
    /// This savepoint will be freed as soon as the returned `[Savepoint]` is dropped.
    ///
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened)
    pub fn ephemeral_savepoint(&self) -> Result<Savepoint, SavepointError> {
        if self.dirty.load(Ordering::Acquire) {
            return Err(SavepointError::InvalidSavepoint);
        }

        let (id, transaction_id) = self.allocate_savepoint()?;
        #[cfg(feature = "logging")]
        debug!("Creating savepoint id={id:?}, txn_id={transaction_id:?}");

        let root = self.mem.get_data_root();
        let savepoint = Savepoint::new_ephemeral(
            &self.mem,
            self.transaction_tracker.clone(),
            id,
            transaction_id,
            root,
        );

        Ok(savepoint)
    }

    /// Restore the state of the database to the given [`Savepoint`]
    ///
    /// Calling this method invalidates all [`Savepoint`]s created after savepoint
    pub fn restore_savepoint(&mut self, savepoint: &Savepoint) -> Result<(), SavepointError> {
        // Reject a Savepoint that is from a different Database
        if std::ptr::from_ref(self.transaction_tracker.as_ref()) != savepoint.db_address() {
            return Err(SavepointError::InvalidSavepoint);
        }

        if !self
            .transaction_tracker
            .is_valid_savepoint(savepoint.get_id())
            || self
                .savepoint_state
                .lock()
                .unwrap()
                .is_invalidated(savepoint.get_id())
        {
            return Err(SavepointError::InvalidSavepoint);
        }

        if self.durability != InternalDurability::Immediate
            && self
                .list_persistent_savepoints()?
                .any(|id| id > savepoint.get_id().0)
        {
            return Err(SavepointError::ImmediateDurabilityRequired);
        }
        #[cfg(feature = "logging")]
        debug!(
            "Beginning savepoint restore (id={:?}) in transaction id={:?}",
            savepoint.get_id(),
            self.transaction_id
        );
        // Restoring a savepoint that reverted a file format or checksum type change could corrupt
        // the database
        assert_eq!(self.mem.get_version(), savepoint.get_version());
        self.dirty.store(true, Ordering::Release);

        // Restoring a savepoint needs to accomplish the following:
        // 1) restore the table tree. This is trivial, since we have the old root
        // 1a) we also filter the freed tree to remove any pages referenced by the old root
        // 2) free all pages that were allocated since the savepoint and are unreachable
        //    from the restored table tree root. Here we diff the reachable pages from the old
        //    and new roots
        // 3) update the system tree to remove invalid persistent savepoints.

        // 1) restore the table tree
        {
            self.tables
                .lock()
                .unwrap()
                .set_root(savepoint.get_user_root());
        }

        // 1a) purge all transactions that happened after the savepoint from the data freed tree
        let txn_id = savepoint.get_transaction_id().next().raw_id();
        {
            let lower = TransactionIdWithPagination {
                transaction_id: txn_id,
                pagination_id: 0,
            };
            let mut system_tables = self.system_tables.lock().unwrap();
            let mut data_freed = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
            for entry in data_freed.extract_from_if(lower.., |_, _| true)? {
                entry?;
            }
            *self.freed_pagination[0].lock().unwrap() = 0;
            self.flushed_freed_pages[0].store(0, Ordering::Relaxed);
            // No need to process the system freed table, because it only rolls forward
        }

        // 2) queue all pages that became unreachable
        {
            let tables = self.tables.lock().unwrap();
            for page in tables.allocated_pages.lock().unwrap().reset().iter() {
                debug_assert!(self.mem.uncommitted(page));
                debug_assert!(self.mem.is_allocated(page));
                self.mem.free(page, &mut PageTrackerPolicy::Ignore);
            }
            let mut data_freed_pages = tables.freed_pages.lock().unwrap();
            data_freed_pages.clear();
            let mut system_tables = self.system_tables.lock().unwrap();
            let data_allocated = system_tables.open_system_table(self, DATA_ALLOCATED_TABLE)?;
            let lower = TransactionIdWithPagination {
                transaction_id: txn_id,
                pagination_id: 0,
            };
            for entry in data_allocated.range(lower..)? {
                let (_, value) = entry?;
                for i in 0..value.value().len() {
                    data_freed_pages.push(value.value().get(i));
                }
            }
        }

        // 3) Mark all savepoints newer than the restored one as invalidated for this
        // transaction, to prevent the user from later trying to restore a savepoint
        // "on another timeline". The invalidation is purely per-transaction state -
        // the shared `valid_savepoints` map is only updated if/when commit_inner()
        // runs, so an abort implicitly reverts the invalidation by dropping this set.
        let invalidated = self
            .transaction_tracker
            .list_savepoints_after(savepoint.get_id());
        self.savepoint_state
            .lock()
            .unwrap()
            .record_invalidated(invalidated);
        for persistent_savepoint in self.list_persistent_savepoints()? {
            if persistent_savepoint > savepoint.get_id().0 {
                self.delete_persistent_savepoint(persistent_savepoint)?;
            }
        }

        Ok(())
    }

    /// Set the desired durability level for writes made in this transaction
    /// Defaults to [`Durability::Immediate`]
    ///
    /// If a persistent savepoint has been created or deleted, in this transaction, the durability may not
    /// be reduced below [`Durability::Immediate`]
    pub fn set_durability(&mut self, durability: Durability) -> Result<(), SetDurabilityError> {
        assert!(
            !self.mem.incremental_recovery() || !matches!(durability, Durability::None),
            "Non-durable commits are off while incremental recovery is on"
        );
        let persistent_modified = self
            .savepoint_state
            .lock()
            .unwrap()
            .has_created_or_deleted();
        if persistent_modified && !matches!(durability, Durability::Immediate) {
            return Err(SetDurabilityError::PersistentSavepointModified);
        }

        self.durability = match durability {
            Durability::None => InternalDurability::None,
            Durability::Immediate => InternalDurability::Immediate,
        };

        Ok(())
    }

    /// Enable or disable 2-phase commit (defaults to disabled)
    ///
    /// By default, data is written using the following 1-phase commit algorithm:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Flip the god byte primary bit to activate the newly updated commit slot
    /// 3. Call `fsync` to ensure all writes have been persisted to disk
    ///
    /// All data is written with checksums. When opening the database after a crash, the most
    /// recent of the two commit slots with a valid checksum is used.
    ///
    /// Security considerations: The checksum used is xxhash, a fast, non-cryptographic hash
    /// function with close to perfect collision resistance when used with non-malicious input. An
    /// attacker with an extremely high degree of control over the database's workload, including
    /// the ability to cause the database process to crash, can cause invalid data to be written
    /// with a valid checksum, leaving the database in an invalid, attacker-controlled state.
    ///
    /// Alternatively, you can enable 2-phase commit, which writes data like this:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Call `fsync` to ensure the database slate and commit slot update have been persisted
    /// 3. Flip the god byte primary bit to activate the newly updated commit slot
    /// 4. Call `fsync` to ensure the write to the god byte has been persisted
    ///
    /// This mitigates a theoretical attack where an attacker who
    /// 1. can control the order in which pages are flushed to disk
    /// 2. can introduce crashes during `fsync`,
    /// 3. has knowledge of the database file contents, and
    /// 4. can include arbitrary data in a write transaction
    ///
    /// could cause a transaction to partially commit (some but not all of the data is written).
    /// This is described in the design doc in futher detail.
    ///
    /// Security considerations: Many hard disk drives and SSDs do not actually guarantee that data
    /// has been persisted to disk after calling `fsync`. Even with 2-phase commit, an attacker with
    /// a high degree of control over the database's workload, including the ability to cause the
    /// database process to crash, can cause the database to crash with the god byte primary bit
    /// pointing to an invalid commit slot, leaving the database in an invalid, potentially attacker-
    /// controlled state.
    pub fn set_two_phase_commit(&mut self, enabled: bool) {
        self.two_phase_commit = enabled;
    }

    /// Enable or disable quick-repair (defaults to disabled)
    ///
    /// By default, when reopening the database after a crash, redb needs to do a full repair.
    /// This involves walking the entire database to verify the checksums and reconstruct the
    /// allocator state, so it can be very slow if the database is large.
    ///
    /// Alternatively, you can enable quick-repair. In this mode, redb saves the allocator state
    /// as part of each commit (so it doesn't need to be reconstructed), and enables 2-phase commit
    /// (which guarantees that the primary commit slot is valid without needing to look at the
    /// checksums). This means commits are slower, but recovery after a crash is almost instant.
    pub fn set_quick_repair(&mut self, enabled: bool) {
        self.quick_repair = enabled;
    }

    /// Reads the current uncommitted table without opening it for mutation.
    /// Drop this table and every guard from it before writing, committing or aborting.
    pub fn peek_table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>, TableError> {
        let tables = self.tables.lock().unwrap();
        let name = definition.name();
        if let Some(location) = tables.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }
        let header = tables
            .table_tree
            .get_table::<K, V>(name, TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(name.to_string()))?;
        Ok(ReadOnlyTable::new(
            name.to_string(),
            header.private_get_root(),
            PageHint::None,
            self.transaction_guard.clone(),
            self.mem.clone(),
        )?)
    }

    /// Leaves at least `pages` newly allocated pages free for later work in this transaction.
    pub fn reserve(&mut self, pages: usize) -> Result<()> {
        let mut reserved = Vec::with_capacity(pages);
        for _ in 0..pages {
            let page = self
                .mem
                .allocate(self.mem.get_page_size(), &mut PageTrackerPolicy::Ignore)?;
            reserved.push(page.get_page_number().to_le_bytes());
        }
        for page in reserved {
            self.mem.free_if_uncommitted(
                PageNumber::from_le_bytes(page),
                &mut PageTrackerPolicy::Ignore,
            );
        }
        Ok(())
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &'txn self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'txn, K, V>, TableError> {
        self.tables.lock().unwrap().open_table(self, definition)
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_multimap_table<'txn, K: Key + 'static, V: Key + 'static>(
        &'txn self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'txn, K, V>, TableError> {
        self.tables
            .lock()
            .unwrap()
            .open_multimap_table(self, definition)
    }

    pub(crate) fn close_table<K: Key + 'static, V: Value + 'static>(
        &self,
        name: &str,
        table: &BtreeMut<K, V>,
        length: u64,
    ) {
        self.tables.lock().unwrap().close_table(name, table, length);
    }

    /// Rename the given table
    pub fn rename_table(
        &self,
        definition: impl TableHandle,
        new_name: impl TableHandle,
    ) -> Result<(), TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `Table` to rename, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables
            .lock()
            .unwrap()
            .rename_table(self, &name, new_name.name())
    }

    /// Rename the given multimap table
    pub fn rename_multimap_table(
        &self,
        definition: impl MultimapTableHandle,
        new_name: impl MultimapTableHandle,
    ) -> Result<(), TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `MultimapTable` to rename, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables
            .lock()
            .unwrap()
            .rename_multimap_table(self, &name, new_name.name())
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table(&self, definition: impl TableHandle) -> Result<bool, TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `Table` or `MultimapTable` to delete, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables.lock().unwrap().delete_table(self, &name)
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_multimap_table(
        &self,
        definition: impl MultimapTableHandle,
    ) -> Result<bool, TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `Table` or `MultimapTable` to delete, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables
            .lock()
            .unwrap()
            .delete_multimap_table(self, &name)
    }

    /// List all the tables
    pub fn list_tables(&self) -> Result<impl Iterator<Item = UntypedTableHandle> + '_> {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter().map(UntypedTableHandle::new))
    }

    /// List all the multimap tables
    pub fn list_multimap_tables(
        &self,
    ) -> Result<impl Iterator<Item = UntypedMultimapTableHandle> + '_> {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter().map(UntypedMultimapTableHandle::new))
    }

    /// Commit the transaction
    ///
    /// All writes performed in this transaction will be visible to future transactions, and are
    /// durable as consistent with the [`Durability`] level set by [`Self::set_durability`]
    pub fn commit(mut self) -> Result<(), CommitError> {
        if self.durability == InternalDurability::Immediate && !self.quick_repair {
            let limit = if self.growth_limit.is_some() {
                RELEASE_PAGES
            } else {
                MAX_RELEASE_PAGES
            };
            self.prepare_commit(limit)?;
        }
        // Publication starts only after all refusable metadata work.
        self.completed = true;
        // A refusal for room comes before any header is written: the transaction is refused and
        // aborted on drop, and the file is sound
        self.commit_inner().inspect_err(|error| match error {
            CommitError::Storage(StorageError::Io(e))
                if e.get_ref().is_some_and(|e| e.is::<crate::FileSizeLimit>()) =>
            {
                self.completed = false;
            }
            _ => self.mem.fail(),
        })
    }

    fn commit_inner(&mut self) -> Result<(), CommitError> {
        // Quick-repair requires 2-phase commit
        if self.quick_repair {
            self.two_phase_commit = true;
        }

        if self.prepared_commit.is_none() && self.durability == InternalDurability::Immediate {
            let mem = self.mem.clone();
            let _lowest = mem.lowest_scope(mem.incremental_recovery() || self.shrinks());
            let allowance = self.mem.count_uncommitted_pages();
            self.release_freed_pages(allowance.clamp(RELEASE_PAGES, MAX_RELEASE_PAGES))?;
        }
        let (user_root, freed) = match &self.prepared_commit {
            Some((root, freed)) => (*root, *freed),
            None => self.prepare_user_commit()?,
        };

        #[cfg(feature = "logging")]
        debug!(
            "Committing transaction id={:?} with durability={:?} two_phase={} quick_repair={}",
            self.transaction_id, self.durability, self.two_phase_commit, self.quick_repair
        );
        let freed = freed
            + match self.durability {
                InternalDurability::None => self.non_durable_commit(user_root)?,
                InternalDurability::Immediate => self.durable_commit(user_root)?,
            };

        self.savepoint_state
            .lock()
            .unwrap()
            .apply_on_commit(&self.transaction_tracker);

        assert!(
            self.system_tables
                .lock()
                .unwrap()
                .system_freed_pages()
                .lock()
                .unwrap()
                .is_empty()
        );
        assert!(
            self.tables
                .lock()
                .unwrap()
                .freed_pages
                .lock()
                .unwrap()
                .is_empty()
        );

        #[cfg(feature = "logging")]
        debug!(
            "Finished commit of transaction id={:?}",
            self.transaction_id
        );

        self.mem.last_freed_pages.store(freed, Ordering::Relaxed);
        let mut taken = self.mem.taken.lock().unwrap();
        taken.1 =
            (self.mem.file_len()? <= self.begun.1).then(|| self.begun.0.saturating_sub(taken.0));
        Ok(())
    }

    fn prepare_user_commit(&self) -> Result<(Option<BtreeHeader>, u64)> {
        let (user_root, allocated_pages, data_freed) =
            self.tables.lock().unwrap().table_tree.flush_and_close()?;
        let _lowest = self
            .mem
            .lowest_scope(self.mem.incremental_recovery() || self.shrinks());
        self.retire_commit_pages()?;

        let freed = data_freed.iter().map(|p| 1u64 << p.page_order).sum::<u64>()
            + self
                .flushed_freed_pages
                .iter()
                .map(|n| n.load(Ordering::Relaxed))
                .sum::<u64>();
        self.store_data_freed_pages(data_freed)?;
        self.store_allocated_pages(allocated_pages.iter())?;
        Ok((user_root, freed))
    }

    /// Write out pending freed-page lists at 4,096 entries without committing.
    /// May be called while a user table is open. Abort the transaction on error.
    pub fn flush_freed_pages(&self) -> Result {
        let _lowest = self
            .mem
            .lowest_scope(self.mem.incremental_recovery() || self.shrinks());
        let pages = {
            let tables = self.tables.lock().unwrap();
            let mut pages = tables.freed_pages.lock().unwrap();
            if pages.len() >= 4096 {
                std::mem::take(&mut *pages)
            } else {
                Vec::new()
            }
        };
        if !pages.is_empty() {
            let count = pages.iter().map(|p| 1u64 << p.page_order).sum::<u64>();
            self.store_data_freed_pages(pages)?;
            self.flushed_freed_pages[0].fetch_add(count, Ordering::Relaxed);
        }
        let mut system = self.system_tables.lock().unwrap();
        let pages = system.system_freed_pages();
        if pages.lock().unwrap().len() >= 4096 {
            let count = self.store_system_freed_pages(&mut system.table_tree, pages, None)?;
            self.flushed_freed_pages[1].fetch_add(count, Ordering::Relaxed);
        }
        Ok(())
    }

    fn store_data_freed_pages(&self, mut freed_pages: Vec<PageNumber>) -> Result {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut freed_table = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
        let mut pagination_counter = self.freed_pagination[0].lock().unwrap();
        while !freed_pages.is_empty() {
            let chunk_size = RELEASE_BATCH;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: *pagination_counter,
            };
            let mut access_guard = freed_table.insert_reserve(&key, buffer_size)?;

            let len = freed_pages.len();
            access_guard.as_mut().clear();
            for page in freed_pages.drain(len - min(len, chunk_size)..) {
                // Make sure that the page is currently allocated
                debug_assert!(
                    self.mem.is_allocated(page),
                    "Page is not allocated: {page:?}"
                );
                debug_assert!(!self.mem.uncommitted(page), "Page is uncommitted: {page:?}");
                access_guard.as_mut().push_back(page);
            }

            *pagination_counter += 1;
        }

        Ok(())
    }

    fn store_allocated_pages(
        &self,
        data_allocated_pages: impl Iterator<Item = PageNumber>,
    ) -> Result {
        let mut data_allocated_pages = data_allocated_pages.peekable();
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut allocated_table = system_tables.open_system_table(self, DATA_ALLOCATED_TABLE)?;
        let mut pagination_counter = 0;
        while data_allocated_pages.peek().is_some() {
            let chunk_size = RELEASE_BATCH;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: pagination_counter,
            };
            let mut access_guard = allocated_table.insert_reserve(&key, buffer_size)?;

            access_guard.as_mut().clear();
            for page in data_allocated_pages.by_ref().take(chunk_size) {
                // Make sure that the page is currently allocated. This is to catch scenarios like
                // a page getting allocated, and then deallocated within the same transaction,
                // but errantly being left in the allocated pages list
                debug_assert!(
                    self.mem.is_allocated(page),
                    "Page is not allocated: {page:?}"
                );
                debug_assert!(self.mem.uncommitted(page), "Page is committed: {page:?}");
                access_guard.as_mut().push_back(page);
            }

            pagination_counter += 1;
        }

        // Purge any transactions that are no longer referenced
        let oldest = self
            .transaction_tracker
            .oldest_savepoint()
            .map_or(u64::MAX, |(_, x)| x.raw_id());
        let key = TransactionIdWithPagination {
            transaction_id: oldest,
            pagination_id: 0,
        };
        for entry in allocated_table.extract_from_if(..key, |_, _| true)? {
            entry?;
        }

        Ok(())
    }

    /// Abort the transaction
    ///
    /// All writes performed in this transaction will be rolled back
    pub fn abort(mut self) -> Result {
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        self.abort_inner()
    }

    fn abort_inner(&mut self) -> Result {
        #[cfg(feature = "logging")]
        debug!("Aborting transaction id={:?}", self.transaction_id);
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .clear_root_updates_and_close();
        // Release all transaction-local savepoint state. The on-disk mutations
        // (SAVEPOINT_TABLE / NEXT_SAVEPOINT_TABLE) are reverted by
        // rollback_uncommitted_writes() below; apply_on_abort handles the
        // in-memory TransactionTracker state that rollback cannot see.
        self.savepoint_state
            .lock()
            .unwrap()
            .apply_on_abort(&self.transaction_tracker);
        // The pass's place is read again from the last commit
        self.mem.compaction_position.lock().unwrap().0 = false;
        self.mem.rollback_uncommitted_writes()?;
        for page in self.released_pages.drain(..) {
            self.mem
                .mark_page_allocated(PageNumber::from_le_bytes(page));
        }
        #[cfg(feature = "logging")]
        debug!("Finished abort of transaction id={:?}", self.transaction_id);
        Ok(())
    }

    // A commit that gives back the file's free end takes the lowest places, leaving that end free
    fn shrinks(&self) -> bool {
        matches!(self.shrink_policy, ShrinkPolicy::Maximum)
    }

    fn free_until(&self) -> TransactionId {
        self.transaction_tracker
            .oldest_live_read_transaction()
            .map_or(self.transaction_id, |id| id.next())
    }

    pub(crate) fn durable_commit(&mut self, user_root: Option<BtreeHeader>) -> Result<u64> {
        let mem = self.mem.clone();
        let _lowest = mem.lowest_scope(mem.incremental_recovery() || self.shrinks());

        let mut system_tables = self.system_tables.lock().unwrap();
        let system_freed_pages = system_tables.system_freed_pages();
        let system_tree = system_tables.table_tree.flush_table_root_updates()?;
        system_tree
            .delete_table(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?;

        let mut freed = 0;
        if self.quick_repair {
            system_tree.create_table_and_flush_table_root(
                ALLOCATOR_STATE_TABLE_NAME,
                |system_tree_ref, tree: &mut AllocatorStateTreeMut| {
                    loop {
                        let num_regions = self.mem.reserve_allocator_state(
                            tree,
                            self.transaction_id,
                            matches!(self.shrink_policy, ShrinkPolicy::Maximum),
                        )?;

                        // We can't free pages after the commit, because that would invalidate our
                        // saved allocator state. Everything needs to go through the transactional
                        // free mechanism
                        freed += self.store_system_freed_pages(
                            system_tree_ref,
                            system_freed_pages.clone(),
                            None,
                        )?;

                        if self.mem.try_save_allocator_state(tree, num_regions)? {
                            return Ok(());
                        }

                        // Clear out the table before retrying, just in case the number of regions
                        // has somehow shrunk. Don't use retain_in() for this, since it doesn't
                        // free the pages immediately -- we need to reuse those pages to guarantee
                        // that our retry loop will eventually terminate
                        while let Some(guards) = tree.last()? {
                            let key = guards.0.value();
                            drop(guards);
                            tree.remove(&key)?;
                        }
                    }
                },
            )?;
        }

        let system_root = system_tree.finalize_dirty_checksums()?;

        self.mem.commit(
            user_root,
            system_root,
            self.transaction_id,
            self.two_phase_commit,
            self.shrink_policy,
        )?;

        // Mark any pending non-durable commits as fully committed.
        self.transaction_tracker.clear_pending_non_durable_commits();

        // Bookkeeping pages are recorded for a later commit. Freeing them here, straight after
        // the header, does not ask which commits a reader holds, so a marked reader can see a
        // page of the commit it is checking rewritten and report every root corrupted.
        Ok(freed)
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&mut self, user_root: Option<BtreeHeader>) -> Result<u64> {
        let mem = self.mem.clone();
        let _lowest = mem.lowest_scope(mem.incremental_recovery() || self.shrinks());
        let mut free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_nondurable_transaction()
            .map_or(self.transaction_id, |x| x.next());
        // TODO: refactor the non-durable free'ed processing to remove this
        // The reason it is needed is that non-durable commits edit previous non-durable commits,
        // but they only edit the freed tree of unpersisted pages.
        // The allocated tree, which savepoints rely, is not edited for performance reasons
        // Therefore, we must not edit anything after a savepoint
        // It would be better for non-durable transaction's unpersisted pages to be kept in-memory
        // in a data structure where the allocated list can be efficiently edited
        if let Some((_, oldest_savepoint)) = self.transaction_tracker.oldest_savepoint() {
            free_until_transaction = TransactionId::min(free_until_transaction, oldest_savepoint);
        }
        self.process_freed_pages_nondurable(free_until_transaction)?;

        let mut post_commit_frees = vec![];
        let mut freed = 0;

        let system_root = {
            let mut system_tables = self.system_tables.lock().unwrap();
            let system_freed_pages = system_tables.system_freed_pages();
            system_tables.table_tree.flush_table_root_updates()?;
            for page in system_freed_pages
                .lock()
                .unwrap()
                .extract_if(.., |p| self.mem.unpersisted(*p))
            {
                post_commit_frees.push(page);
            }
            freed += post_commit_frees
                .iter()
                .map(|p| 1u64 << p.page_order)
                .sum::<u64>();
            // Store all freed pages for a future commit(), since we can't free pages during a
            // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
            freed += self.store_system_freed_pages(
                &mut system_tables.table_tree,
                system_freed_pages,
                Some(&mut post_commit_frees),
            )?;

            system_tables
                .table_tree
                .flush_table_root_updates()?
                .finalize_dirty_checksums()?
        };

        self.mem
            .non_durable_commit(user_root, system_root, self.transaction_id)?;
        // Register this as a non-durable transaction to ensure that the freed pages we just pushed
        // are only processed after this has been persisted
        self.transaction_tracker.register_non_durable_commit(
            self.transaction_id,
            self.mem.get_last_durable_transaction_id()?,
        );

        for page in post_commit_frees {
            self.mem.free(page, &mut PageTrackerPolicy::Ignore);
        }

        Ok(freed)
    }

    /// Stages metadata under the clip; a refusal restores released pages after rollback.
    /// Requires immediate durability and quick repair disabled, as compaction uses by default.
    /// Call after staging the step; no further mutations may follow before commit.
    pub fn prepare_compaction_commit(&mut self) -> Result<()> {
        self.shrink_policy = ShrinkPolicy::Maximum;
        assert!(self.durability == InternalDurability::Immediate && !self.quick_repair);
        self.limit_file_growth()?;
        self.prepare_commit(RELEASE_PAGES)
    }

    fn prepare_commit(&mut self, limit: usize) -> Result<()> {
        if self.prepared_commit.is_some() {
            return Ok(());
        }
        let mem = self.mem.clone();
        let _lowest = mem.lowest_scope(mem.incremental_recovery() || self.shrinks());
        let allowance = self
            .mem
            .count_uncommitted_pages()
            .max(RELEASE_PAGES)
            .min(limit);
        // Freed pages are released before the commit places its own, so those can take them
        self.release_freed_pages(allowance.saturating_sub(self.release_spent))?;
        let (root, mut freed) = self.prepare_user_commit()?;
        let mut system = self.system_tables.lock().unwrap();
        system
            .table_tree
            .flush_table_root_updates()?
            .delete_table(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?;
        // Bookkeeping pages wait for the readers' answer like any other.
        let pages = system.system_freed_pages();
        freed += self.store_system_freed_pages(&mut system.table_tree, pages, None)?;
        system
            .table_tree
            .flush_table_root_updates()?
            .finalize_dirty_checksums()?;
        self.mem.prepare_commit_pages()?;
        self.prepared_commit = Some((root, freed));
        Ok(())
    }

    /// Caps this transaction at its entry file length, restoring the declared limit on drop.
    pub fn limit_file_growth(&mut self) -> Result<()> {
        if self.growth_limit.is_none() {
            let end = self.mem.file_len()?;
            self.growth_limit = Some(self.mem.file_size_limit.fetch_min(end, Ordering::AcqRel));
        }
        Ok(())
    }

    /// The place of the pass under way, if one is, and whether its walk has moved a page yet,
    /// recording `moved`; with no place, whether the last whole walk found nothing to move.
    pub fn compaction_position(&self, moved: bool) -> Result<(Option<Vec<u8>>, bool)> {
        let mut position = self.mem.compaction_position.lock().unwrap();
        if !position.0 {
            let tables = self.system_tables.lock().unwrap();
            let record = tables
                .table_tree
                .get_table_untyped(COMPACTION_TABLE.name(), TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Invalid compaction position"))?;
            let root = record.as_ref().and_then(|d| d.private_get_root());
            let tree = Btree::<(), &[u8]>::new(
                root,
                PageHint::None,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )?;
            let stored = tree.get(&())?.map(|v| v.value().to_vec());
            position.2 = record.is_some() && stored.as_ref().is_none_or(|s| s.first() == Some(&1));
            position.1 = stored.map(|s| s.get(1..).unwrap_or_default().to_vec());
            position.0 = true;
        }
        position.2 |= moved;
        Ok((position.1.clone(), position.2))
    }

    // Stores the pass's place and whether its walk moved a page, or with none whether it rests
    fn set_compaction_position(&self, position: Option<&[u8]>, moved: bool) -> Result<()> {
        *self.mem.compaction_position.lock().unwrap() = (true, position.map(Vec::from), moved);
        let mut tables = self.system_tables.lock().unwrap();
        // A rest is the record left empty, which takes no page; with neither, there is no record
        if position.is_none() && !moved {
            tables
                .table_tree
                .delete_table(COMPACTION_TABLE.name(), TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Invalid compaction position"))?;
            return Ok(());
        }
        let mut table = tables.open_system_table(self, COMPACTION_TABLE)?;
        match position {
            Some(position) => {
                drop(table.insert((), [&[u8::from(moved)], position].concat().as_slice())?);
            }
            None => drop(table.remove(())?),
        }
        Ok(())
    }

    // Ends the pass under way, clearing its stored place; with `rest`, records that it rests
    fn finish_compaction(&self, rest: bool) -> Result<CompactionStep> {
        self.set_compaction_position(None, rest)?;
        Ok(CompactionStep {
            ended: true,
            ..CompactionStep::default()
        })
    }

    /// Allocator free, total, last commit's freed pages, and whether the store rests (no pass).
    pub fn compaction_state(&self) -> Result<(u64, u64, u64, bool)> {
        let free = self.mem.count_free_pages()?;
        Ok((
            free,
            free + self.mem.count_allocated_pages()?,
            self.mem.last_freed_pages.load(Ordering::Relaxed),
            self.compaction_position(false)? == (None, true) && !self.releasable()?,
        ))
    }

    /// The allocator snapshot plus eligible pending pages, read only up to churn.
    pub fn compaction_space(&self, allowance: &mut usize) -> Result<(u64, u64, u64, u64, bool)> {
        let (free, total, churn, held) = self.compaction_state()?;
        let until = self.free_until().raw_id();
        let mut eligible = 0;
        let tables = self.system_tables.lock().unwrap();
        let width = TransactionIdWithPagination::fixed_width();
        let threshold = churn.max(1);
        let mut inspect =
            |name: Option<&str>, path: &PagePath, read: &mut bool, page: Option<&[u8]>| {
                if name.is_some_and(|n| n != DATA_FREED_TABLE.name()) {
                    return Ok(false);
                }
                if let Some(page) = page {
                    let leaf = LeafAccessor::new(page, width, PageList::fixed_width());
                    for entry in (0..leaf.num_pairs()).filter_map(|i| leaf.entry(i)) {
                        let key = TransactionIdWithPagination::from_bytes(entry.key());
                        if key.transaction_id >= until || eligible >= threshold {
                            return Ok(false);
                        }
                        let pages = PageList::from_bytes(entry.value());
                        eligible += (0..pages.len())
                            .map(|i| 1u64 << pages.get(i).page_order)
                            .sum::<u64>();
                    }
                } else {
                    *read = true;
                    let cost = 1usize << path.page_number().page_order;
                    if cost > *allowance {
                        return Ok(false);
                    }
                    *allowance -= cost;
                }
                Ok(eligible < threshold)
            };
        tables.table_tree.highest_index_pages(
            (Some(DATA_FREED_TABLE.name()), &[]),
            PageHint::None,
            &mut BTreeMap::new(),
            &mut inspect,
        )?;
        Ok((free, total, eligible, churn, held))
    }

    /// The release mark: the last committed id and the oldest reader, external pins included,
    /// clamped to it; with no reader, the last committed id twice.
    pub fn compaction_mark(&self) -> Result<(u64, u64)> {
        let last = self.mem.get_last_committed_transaction_id()?.raw_id();
        let oldest = self.transaction_tracker.oldest_live_read_transaction();
        Ok((last, oldest.map_or(last, |id| id.raw_id().min(last))))
    }

    /// Whether a live reader holds back freed pages: a freed-page record the reclamation
    /// cutoff keeps that would be released were no reader live.
    pub fn readers_hold_freed_pages(&self) -> Result<bool> {
        let from = self.free_until().raw_id();
        self.freed_in([from, from], [self.transaction_id.raw_id(); 2])
    }

    // Freed pages no live reader holds wait for release, beyond the last commit's own bookkeeping
    fn releasable(&self) -> Result<bool> {
        let until = self.free_until().raw_id();
        let last = self.mem.get_last_committed_transaction_id()?.raw_id();
        self.freed_in([0, 0], [until, until.min(last)])
    }

    fn freed_in(&self, from: [u64; 2], to: [u64; 2]) -> Result<bool> {
        let at = |transaction_id| TransactionIdWithPagination {
            transaction_id,
            pagination_id: 0,
        };
        let mut tables = self.system_tables.lock().unwrap();
        for (definition, (from, to)) in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE]
            .into_iter()
            .zip(from.into_iter().zip(to))
        {
            let table = tables.open_system_table(self, definition)?;
            if table
                .range::<&TransactionIdWithPagination>(&at(from)..&at(to))?
                .next()
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The low mark of the last commit: the free pages at its start less the most pages it had
    /// taken and not committed at any moment; `None` when it lengthened the file.
    pub fn last_commit_low_mark(&self) -> Result<Option<u64>> {
        Ok(self.mem.taken.lock().unwrap().1)
    }

    /// One step of the pass that packs the file, from the place `from`. In tree order it moves
    /// the pages at or past the packed length, each to the lowest place wholly below it, and
    /// passes by a page with no such place. One `allowance` is charged each page read and each
    /// page written once, a moved block both where it was read and where it goes; the first
    /// movable block moves whatever its size. A whole walk that
    /// moved nothing ends the pass. The place to go on from, and whether the walk has moved a
    /// page, are stored in this transaction and returned with what the step did.
    pub fn compact_pages(
        &mut self,
        allowance: usize,
        from: Option<&[u8]>,
    ) -> Result<(CompactionStep, Option<Vec<u8>>)> {
        self.compact_pages_choosing(allowance, from, 0, 0, &mut false)
    }

    /// [`Self::compact_pages`], releasing what is due first, finishing the pass when free pages
    /// are fewer than `stop`, and with `skip` above zero passing by the first `skip - 1`
    /// pages that have a lower place for this attempt and taking the next alone. `selected`
    /// says, also when the step fails, whether it had chosen a page to move.
    pub fn compact_pages_choosing(
        &mut self,
        mut allowance: usize,
        from: Option<&[u8]>,
        stop: u64,
        skip: usize,
        selected: &mut bool,
    ) -> Result<(CompactionStep, Option<Vec<u8>>)> {
        let mut step = CompactionStep::default();
        if self.mem.recovery_state()?.0.is_some() {
            return Ok((step, from.map(Vec::from)));
        }
        self.limit_file_growth()?;
        self.release_freed_pages(RELEASE_PAGES)?;
        let walked = from.is_some() && self.compaction_position(false)?.1;
        self.shrink_policy = ShrinkPolicy::Maximum;
        let (free, cut) = (
            self.mem.count_free_pages()?,
            self.mem.count_allocated_pages()?,
        );
        // With no free page nothing has a place below it, so the store rests.
        if free == 0 || free < stop {
            return Ok((self.finish_compaction(free == 0)?, None));
        }
        let mut pass = skip.saturating_sub(1);
        // A phase distinguishes both namespaces and catalog progress from even an empty table name.
        let (phase, position, name) = from
            .and_then(|b| {
                let depth = usize::from(*b.get(1)?);
                let (path, name) = b.get(2..)?.split_at_checked(4 * depth)?;
                let path = path
                    .chunks_exact(4)
                    .map(|v| u32::from_le_bytes(v.try_into().unwrap()))
                    .collect::<Vec<_>>();
                Some((*b.first()?, path, std::str::from_utf8(name).ok()?)).filter(|v| v.0 < 4)
            })
            .unwrap_or_default();
        let region_pages = self.mem.region_pages();
        let mem = self.mem.clone();
        // A block that found no place below it has the next walk clear an aligned run of its
        // size under it: the smaller pages in the run move below the run, so it comes free.
        let wanted = mem.stuck.lock().unwrap().take();
        let (run, mut held) =
            wanted.map_or((0..0, Vec::new()), |(order, at)| mem.hold_run(order, at));
        let (mut moving, mut above) = (BTreeMap::new(), HashSet::new());
        let mut copies = Vec::new();
        let mut tables = self.tables.lock().unwrap();
        let table_tree = &mut tables.table_tree;
        let mut system_tables = self.system_tables.lock().unwrap();
        let system_table_tree = &mut system_tables.table_tree;
        let mut next = None;
        let mut visited = [BTreeMap::new(), BTreeMap::new()];
        let mut from = ((phase % 2 == 1).then_some(name), position.as_slice());
        let mut select =
            |_: Option<&str>, path: &PagePath, read: &mut bool, page: Option<&[u8]>| {
                if page.is_some() {
                    return Ok(true);
                }
                let number = path.page_number();
                let size = 1usize << number.page_order;
                let address = u64::from(number.region) * region_pages
                    + (u64::from(number.page_index) << number.page_order);
                let order = number.page_order;
                let inside = run.contains(&address) && wanted.is_some_and(|w| order < w.0);
                if address >= cut || inside {
                    let parents = path.parents().iter().filter(|p| !above.contains(*p));
                    // A block is read where it was and written where it goes.
                    let cost = 2 * size + parents.map(|p| 1usize << p.page_order).sum::<usize>();
                    if !moving.is_empty() && (skip > 0 || cost > allowance) {
                        return Ok(false);
                    }
                    let copy = mem.allocate_below(number)?;
                    if copy.is_none() && address >= cut && order > 0 {
                        let mut stuck = mem.stuck.lock().unwrap();
                        *stuck = (*stuck).max(Some((order, address)));
                    }
                    if let Some(copy) = copy {
                        let mut taken = vec![(number, copy.get_page_number())];
                        drop(copy);
                        // The pages above it are copied too, within the file
                        let mut room = true;
                        for parent in path.parents().iter().filter(|p| !above.contains(*p)) {
                            let Some(copy) = mem.allocate_within(parent.page_order)? else {
                                room = false;
                                break;
                            };
                            taken.push((*parent, copy.get_page_number()));
                        }
                        if room && pass == 0 {
                            allowance = allowance.saturating_sub(cost);
                            step.read += size as u64;
                            above.extend(path.parents().iter().copied());
                            moving.insert(number, (path.clone(), taken[0].1));
                            copies.extend(taken.drain(1..));
                            *read = false;
                            return Ok(true);
                        }
                        // Passed by: with no room above it, or for this attempt, as a later one
                        // takes it
                        pass -= usize::from(room);
                        for (_, at) in taken {
                            mem.free_if_uncommitted(at, &mut PageTrackerPolicy::Ignore);
                        }
                    }
                }
                // A page larger than one is a leaf, and a leaf that does not move is not read
                *read &= number.page_order == 0;
                if *read {
                    if allowance == 0 {
                        return Ok(false);
                    }
                    allowance -= 1;
                    step.read += 1;
                }
                Ok(true)
            };
        for namespace in phase / 2..2 {
            let tree = [&*table_tree, &*system_table_tree][usize::from(namespace)];
            let stopped = tree.highest_index_pages(
                from,
                PageHint::None,
                &mut visited[usize::from(namespace)],
                &mut select,
            );
            let stopped = stopped.inspect_err(|_| mem.give_back(std::mem::take(&mut held)))?;
            if let Some((name, path)) = stopped {
                let mut bytes = vec![
                    namespace * 2 + u8::from(name.is_some()),
                    u8::try_from(path.len()).unwrap(),
                ];
                bytes.extend(path.iter().flat_map(|v| v.to_le_bytes()));
                bytes.extend(name.unwrap_or_default().as_bytes());
                next = Some(bytes);
                break;
            }
            from = (None, &[]);
        }
        mem.give_back(held);
        *selected = !moving.is_empty();
        let mut relocation_map = HashMap::new();
        for (number, (_, copy)) in moving {
            step.moved += 1 << number.page_order;
            relocation_map.insert(number, copy);
        }
        // A page above one move that moves itself keeps its own place
        for (parent, copy) in copies {
            match relocation_map.entry(parent) {
                Entry::Occupied(_) => {
                    self.mem
                        .free_if_uncommitted(copy, &mut PageTrackerPolicy::Ignore);
                }
                Entry::Vacant(place) => drop(place.insert(copy)),
            }
        }
        step.written = relocation_map.keys().map(|p| 1 << p.page_order).sum();
        if !relocation_map.is_empty() {
            let [user, system] = visited;
            table_tree.relocate_tables(user, &relocation_map)?;
            system_table_tree.relocate_tables(system, &relocation_map)?;
        }
        drop((tables, system_tables));
        // A walk that moved a page is followed by another; one that moved none ends the pass
        let found = wanted.is_none() && self.mem.stuck.lock().unwrap().is_some();
        let moved = walked || step.moved > 0 || found;
        let (next, moved) = match next {
            Some(at) => (Some(at), moved),
            None => (moved.then(|| vec![0, 0]), !moved),
        };
        step.ended = next.is_none();
        self.set_compaction_position(next.as_deref(), moved)?;
        Ok((step, next))
    }

    fn release_freed_pages(&mut self, allowance: usize) -> Result {
        // A release that can open a lower place ends a rest, and has a pass under way walk again
        let opens = self.releasable()?;
        let mut released = Vec::with_capacity(allowance);
        let result = self.process_freed_pages(self.free_until(), allowance, |page| {
            released.push(page.to_le_bytes());
            self.mem.free(page, &mut PageTrackerPolicy::Ignore);
        });
        self.released_pages.extend(released);
        self.release_spent += allowance - result?;
        let (place, again) = self.compaction_position(false)?;
        if opens && (place.is_some() || again) {
            self.set_compaction_position(place.as_deref(), place.is_some())?;
        }
        Ok(())
    }

    // NOTE: must be called before store_system_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(
        &self,
        free_until: TransactionId,
        mut allowance: usize,
        mut release: impl FnMut(PageNumber),
    ) -> Result<usize> {
        if self.mem.recovery_state()?.0.is_some() {
            return Ok(allowance);
        }
        let mut tables = self.system_tables.lock().unwrap();
        let deferred_chains = tables.system_freed_pages();
        for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
            let mut table = tables.open_system_table(self, definition)?;
            while allowance >= RELEASE_BATCH {
                let Some((key_guard, pages)) = table.tree.first()? else {
                    break;
                };
                let key = key_guard.value();
                drop(key_guard);
                if key.transaction_id >= free_until.raw_id() {
                    break;
                }
                if key.pagination_id == CHAIN_PAGINATION {
                    let number = pages.value().get(0);
                    let next = self.mem.next_commit_page(number)?;
                    drop(pages);
                    table.tree.remove(&key)?;
                    if let Some(next) = next {
                        let mut entry = table
                            .tree
                            .insert_reserve(&key, PageList::required_bytes(1))?;
                        entry.as_mut().clear();
                        entry.as_mut().push_back(next);
                    }
                    allowance -= 1;
                    deferred_chains.lock().unwrap().push(number);
                } else {
                    allowance -= pages.value().len().max(1);
                    for i in 0..pages.value().len() {
                        release(pages.value().get(i));
                    }
                    drop(pages);
                    table.tree.remove(&key)?;
                }
            }
        }
        Ok(allowance)
    }

    fn retire_commit_pages(&self) -> Result {
        if let Some(page) = self.mem.commit_pages_to_free() {
            let mut system = self.system_tables.lock().unwrap();
            let mut table = system.open_system_table(self, SYSTEM_FREED_TABLE)?;
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: CHAIN_PAGINATION,
            };
            let mut entry = table
                .tree
                .insert_reserve(&key, PageList::required_bytes(1))?;
            entry.as_mut().clear();
            entry.as_mut().push_back(page);
        }
        Ok(())
    }

    fn process_freed_pages_nondurable_helper(
        &mut self,
        free_until: TransactionId,
        definition: SystemTableDefinition<TransactionIdWithPagination, PageList>,
    ) -> Result<Vec<TransactionId>> {
        let mut processed = vec![];
        let mut system_tables = self.system_tables.lock().unwrap();

        let last_key = TransactionIdWithPagination {
            transaction_id: free_until.raw_id(),
            pagination_id: 0,
        };
        let oldest_unprocessed = self
            .transaction_tracker
            .oldest_unprocessed_non_durable_commit()
            .map_or(free_until.raw_id(), |x| x.raw_id());
        let first_key = TransactionIdWithPagination {
            transaction_id: oldest_unprocessed,
            pagination_id: 0,
        };
        let mut data_freed = system_tables.open_system_table(self, definition)?;

        let mut candidate_transactions = vec![];
        for entry in data_freed.range(first_key..last_key)? {
            let (key, _) = entry?;
            let transaction_id = TransactionId::new(key.value().transaction_id);
            if self
                .transaction_tracker
                .is_unprocessed_non_durable_commit(transaction_id)
            {
                candidate_transactions.push(transaction_id);
            }
        }
        for transaction_id in candidate_transactions {
            let mut key = TransactionIdWithPagination {
                transaction_id: transaction_id.raw_id(),
                pagination_id: 0,
            };
            loop {
                let Some(entry) = data_freed.get(&key)? else {
                    break;
                };
                let pages = entry.value();
                let mut new_pages = vec![];
                for i in 0..pages.len() {
                    let page = pages.get(i);
                    if !self
                        .mem
                        .free_if_unpersisted(page, &mut PageTrackerPolicy::Ignore)
                    {
                        new_pages.push(page);
                    }
                }
                if new_pages.len() != pages.len() {
                    drop(entry);
                    if new_pages.is_empty() {
                        data_freed.remove(&key)?;
                    } else {
                        let required = PageList::required_bytes(new_pages.len());
                        let mut page_list_mut = data_freed.insert_reserve(&key, required)?;
                        for page in new_pages {
                            page_list_mut.as_mut().push_back(page);
                        }
                    }
                }
                key.pagination_id += 1;
            }
            processed.push(transaction_id);
        }

        Ok(processed)
    }

    // NOTE: must be called before store_system_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    //
    // This method only frees pages that are unpersisted, in non-durable transactions, since
    // it is called from a non-durable commit() and therefore can't modify anything that the
    // on-disk state in the last durable transaction might reference.
    fn process_freed_pages_nondurable(&mut self, free_until: TransactionId) -> Result {
        // We assume below that PageNumber is length 8
        assert_eq!(PageNumber::serialized_size(), 8);

        // Handle the data freed tree
        let mut processed =
            self.process_freed_pages_nondurable_helper(free_until, DATA_FREED_TABLE)?;

        // Handle the system freed tree
        processed
            .extend(self.process_freed_pages_nondurable_helper(free_until, SYSTEM_FREED_TABLE)?);

        for transaction_id in processed {
            self.transaction_tracker
                .mark_unprocessed_non_durable_commit(transaction_id);
        }

        Ok(())
    }

    fn store_system_freed_pages(
        &self,
        system_tree: &mut TableTreeMut,
        system_freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        mut unpersisted_pages: Option<&mut Vec<PageNumber>>,
    ) -> Result<u64> {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8

        let mut freed = 0;
        let mut pagination_counter = self.freed_pagination[1].lock().unwrap();
        system_tree.open_table_and_flush_table_root(
            SYSTEM_FREED_TABLE.name(),
            |system_freed_tree: &mut SystemFreedTree| {
                while !system_freed_pages.lock().unwrap().is_empty() {
                    let chunk_size = 200;
                    let buffer_size = PageList::required_bytes(chunk_size);
                    let key = TransactionIdWithPagination {
                        transaction_id: self.transaction_id.raw_id(),
                        pagination_id: *pagination_counter,
                    };
                    let mut access_guard = system_freed_tree.insert_reserve(&key, buffer_size)?;

                    let mut freed_pages = system_freed_pages.lock().unwrap();
                    let len = freed_pages.len();
                    access_guard.as_mut().clear();
                    for page in freed_pages.drain(len - min(len, chunk_size)..) {
                        freed += 1u64 << page.page_order;
                        if let Some(ref mut unpersisted_pages) = unpersisted_pages
                            && self.mem.unpersisted(page)
                        {
                            unpersisted_pages.push(page);
                        } else {
                            access_guard.as_mut().push_back(page);
                        }
                    }
                    drop(access_guard);

                    *pagination_counter += 1;
                }
                Ok(())
            },
        )?;

        Ok(freed)
    }

    /// Retrieves information about storage usage in the database
    pub fn stats(&self) -> Result<DatabaseStats> {
        let tables = self.tables.lock().unwrap();
        let table_tree = &tables.table_tree;
        let data_tree_stats = table_tree.stats()?;

        let system_tables = self.system_tables.lock().unwrap();
        let system_table_tree = &system_tables.table_tree;
        let system_tree_stats = system_table_tree.stats()?;

        let total_metadata_bytes = data_tree_stats.metadata_bytes()
            + system_tree_stats.metadata_bytes
            + system_tree_stats.stored_leaf_bytes;
        let total_fragmented = data_tree_stats.fragmented_bytes()
            + system_tree_stats.fragmented_bytes
            + self.mem.count_free_pages()? * (self.mem.get_page_size() as u64);

        Ok(DatabaseStats {
            tree_height: data_tree_stats.tree_height(),
            allocated_pages: self.mem.count_allocated_pages()?,
            leaf_pages: data_tree_stats.leaf_pages(),
            branch_pages: data_tree_stats.branch_pages(),
            stored_leaf_bytes: data_tree_stats.stored_bytes(),
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) -> Result {
        // Flush any pending updates to make sure we get the latest root
        let mut tables = self.tables.lock().unwrap();
        if let Some(page) = tables
            .table_tree
            .flush_table_root_updates()
            .unwrap()
            .finalize_dirty_checksums()
            .unwrap()
        {
            eprintln!("Master tree:");
            let master_tree: Btree<&str, InternalTableDefinition> = Btree::new(
                Some(page),
                PageHint::None,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )?;
            master_tree.print_debug(true)?;
        }

        // Flush any pending updates to make sure we get the latest root
        let mut system_tables = self.system_tables.lock().unwrap();
        if let Some(page) = system_tables
            .table_tree
            .flush_table_root_updates()
            .unwrap()
            .finalize_dirty_checksums()
            .unwrap()
        {
            eprintln!("System tree:");
            let master_tree: Btree<&str, InternalTableDefinition> = Btree::new(
                Some(page),
                PageHint::None,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )?;
            master_tree.print_debug(true)?;
        }

        Ok(())
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        if !self.completed && !thread::panicking() && !self.mem.storage_failure() {
            #[allow(unused_variables)]
            if let Err(error) = self.abort_inner() {
                #[cfg(feature = "logging")]
                warn!("Failure automatically aborting transaction: {error}");
            }
        } else if !self.completed && self.mem.storage_failure() {
            self.tables
                .lock()
                .unwrap()
                .table_tree
                .clear_root_updates_and_close();
        }
        if let Some(limit) = self.growth_limit {
            self.mem.file_size_limit.store(limit, Ordering::Release);
        }
    }
}

/// A read-only transaction
///
/// Read-only transactions may exist concurrently with writes
pub struct ReadTransaction {
    mem: Arc<TransactionalMemory>,
    tree: TableTree,
    commit_id: TransactionId,
}

impl ReadTransaction {
    pub(crate) fn new(
        mem: Arc<TransactionalMemory>,
        guard: TransactionGuard,
    ) -> Result<Self, TransactionError> {
        let (root_page, commit_id) = mem.get_data_root_and_transaction_id();
        let guard = Arc::new(guard);
        Ok(Self {
            mem: mem.clone(),
            tree: TableTree::new(root_page, PageHint::Clean, guard, mem)
                .map_err(TransactionError::Storage)?,
            commit_id,
        })
    }

    /// The id of the commit this transaction reads. Ids rise with every commit of a file, and
    /// two transactions that read the same commit give the same id.
    pub fn commit_id(&self) -> u64 {
        self.commit_id.raw_id()
    }

    /// Bytes of the root page that opening this table keeps, read from its header alone.
    pub fn root_bytes(&self, table: impl TableHandle) -> Result<u64, TableError> {
        let (name, page) = (table.name(), self.mem.get_page_size() as u64);
        let header = self.tree.get_table_untyped(name, TableType::Normal)?;
        let root = header.and_then(|header| header.private_get_root());
        Ok(root.map_or(0, |root| page << root.root.page_order))
    }

    /// Open the given table
    pub fn open_table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>, TableError> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal { table_root, .. } => Ok(ReadOnlyTable::new(
                definition.name().to_string(),
                table_root,
                PageHint::Clean,
                self.tree.transaction_guard().clone(),
                self.mem.clone(),
            )?),
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }
    }

    /// Open the given table without a type
    pub fn open_untyped_table(
        &self,
        handle: impl TableHandle,
    ) -> Result<ReadOnlyUntypedTable, TableError> {
        let header = self
            .tree
            .get_table_untyped(handle.name(), TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(handle.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => Ok(ReadOnlyUntypedTable::new(
                table_root,
                PageHint::Clean,
                fixed_key_size,
                fixed_value_size,
                self.mem.clone(),
            )),
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }
    }

    /// Open the given table
    pub fn open_multimap_table<K: Key + 'static, V: Key + 'static>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<ReadOnlyMultimapTable<K, V>, TableError> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Multimap)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal { .. } => unreachable!(),
            InternalTableDefinition::Multimap {
                table_root,
                table_length,
                ..
            } => Ok(ReadOnlyMultimapTable::new(
                table_root,
                table_length,
                PageHint::Clean,
                self.tree.transaction_guard().clone(),
                self.mem.clone(),
            )?),
        }
    }

    /// Open the given table without a type
    pub fn open_untyped_multimap_table(
        &self,
        handle: impl MultimapTableHandle,
    ) -> Result<ReadOnlyUntypedMultimapTable, TableError> {
        let header = self
            .tree
            .get_table_untyped(handle.name(), TableType::Multimap)?
            .ok_or_else(|| TableError::TableDoesNotExist(handle.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal { .. } => unreachable!(),
            InternalTableDefinition::Multimap {
                table_root,
                table_length,
                fixed_key_size,
                fixed_value_size,
                ..
            } => Ok(ReadOnlyUntypedMultimapTable::new(
                table_root,
                table_length,
                PageHint::Clean,
                fixed_key_size,
                fixed_value_size,
                self.mem.clone(),
            )),
        }
    }

    /// List all the tables
    pub fn list_tables(&self) -> Result<impl Iterator<Item = UntypedTableHandle>> {
        self.tree
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter().map(UntypedTableHandle::new))
    }

    /// List all the multimap tables
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = UntypedMultimapTableHandle>> {
        self.tree
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter().map(UntypedMultimapTableHandle::new))
    }

    /// Close the transaction
    ///
    /// Transactions are automatically closed when they and all objects referencing them have been dropped,
    /// so this method does not normally need to be called.
    /// This method can be used to ensure that there are no outstanding objects remaining.
    ///
    /// Returns `ReadTransactionStillInUse` error if a table or other object retrieved from the transaction still references this transaction
    pub fn close(self) -> Result<(), TransactionError> {
        if Arc::strong_count(self.tree.transaction_guard()) > 1 {
            return Err(TransactionError::ReadTransactionStillInUse(Box::new(self)));
        }
        // No-op, just drop ourself
        Ok(())
    }
}

impl Debug for ReadTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadTransaction")
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, TableDefinition};

    const X: TableDefinition<&str, &str> = TableDefinition::new("x");

    #[test]
    fn transaction_id_persistence() {
        let tmpfile = crate::create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert("hello", "world").unwrap();
        }
        let first_txn_id = write_txn.transaction_id;
        write_txn.commit().unwrap();
        drop(db);

        let db2 = Database::create(tmpfile.path()).unwrap();
        let write_txn = db2.begin_write().unwrap();
        assert!(write_txn.transaction_id > first_txn_id);
    }
}
