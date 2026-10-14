use crate::db::TransactionGuard;
use crate::error::CommitError;
use crate::multimap_table::ReadOnlyUntypedMultimapTable;
use crate::sealed::Sealed;
use crate::table::ReadOnlyUntypedTable;
use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    Btree, BtreeHeader, BtreeMut, InternalTableDefinition, MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, Page,
    PageHint, PageListMut, PageNumber, PageTrackerPolicy, SerializedSavepoint, ShrinkPolicy,
    TableTree, TableTreeMut, TableType, TransactionalMemory,
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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, panic, thread};

const MAX_PAGES_PER_COMPACTION: usize = 1_000_000;

/// A bounded scheduling position. No storage pages or read transaction
/// survive a compaction batch; callers may checkpoint its scalar state.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct CompactionCursor {
    pub(crate) phase: u8,
    pub(crate) table: Option<String>,
    pub(crate) advance_table: bool,
    pub(crate) position: Vec<usize>,
    pub(crate) active: bool,
}

impl CompactionCursor {
    pub fn is_active(&self) -> bool {
        self.active
    }

    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn checkpoint(&self) -> (u8, Option<&str>, bool, &[usize], bool) {
        (
            self.phase,
            self.table.as_deref(),
            self.advance_table,
            &self.position,
            self.active,
        )
    }

    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn restore_checkpoint(
        phase: u8,
        table: Option<String>,
        advance_table: bool,
        position: Vec<usize>,
        active: bool,
    ) -> Result<Self, StorageError> {
        if phase > 3 {
            return Err(StorageError::Corrupted(
                "invalid compaction checkpoint phase".into(),
            ));
        }
        Ok(Self {
            phase,
            table,
            advance_table,
            position,
            active,
        })
    }
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CompactionProgress {
    pub pages_examined: usize,
    pub pages_relocated: usize,
    pub complete: bool,
}
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
    open_tables: crate::table_registry::OpenTables,
    registry_funding: Arc<crate::table_registry::RegistryFunding>,
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
        let registry_funding = Arc::new(crate::table_registry::RegistryFunding::default());
        Self {
            open_tables: crate::table_registry::new_open_tables(&registry_funding),
            registry_funding,
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
            .insert(Box::from(name), panic::Location::caller());

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

    /// The exact layout a reserve of `additional` open tables would allocate,
    /// learned by refusing it: the registry keeps its entries and allocation.
    #[cfg(feature = "contextdb-internal")]
    fn registry_cost_to_reserve(
        &mut self,
        additional: usize,
    ) -> Result<Option<std::alloc::Layout>, crate::table_registry::RegistryReserveError> {
        let call = crate::table_registry::FundedCall::enter(&self.registry_funding, 0);
        match self.open_tables.try_reserve(additional) {
            Ok(()) => Ok(None),
            Err(error) => match crate::table_registry::reserve_error(error, &call) {
                crate::table_registry::RegistryReserveError::AllocError(layout) => Ok(Some(layout)),
                other => Err(other),
            },
        }
    }

    /// Reserve room for `additional` open tables, paying with `credit` bytes.
    /// Returns the bytes of the array this replaced, which is now freed.
    #[cfg(feature = "contextdb-internal")]
    fn registry_reserve_with(
        &mut self,
        additional: usize,
        credit: usize,
    ) -> Result<usize, crate::table_registry::RegistryReserveError> {
        let before = self.open_tables.allocation_size();
        let call = crate::table_registry::FundedCall::enter(&self.registry_funding, credit);
        let allocations = call.allocations();
        match self.open_tables.try_reserve(additional) {
            Ok(()) if call.allocations() != allocations => Ok(before),
            Ok(()) => Ok(0),
            Err(error) => Err(crate::table_registry::reserve_error(error, &call)),
        }
    }

    /// The exact layout an empty registry array for `len` tables allocates.
    #[cfg(feature = "contextdb-internal")]
    fn registry_cost_to_hold(
        &self,
        len: usize,
    ) -> Result<Option<std::alloc::Layout>, crate::table_registry::RegistryReserveError> {
        let mut scratch = crate::table_registry::new_open_tables(&self.registry_funding);
        let call = crate::table_registry::FundedCall::enter(&self.registry_funding, 0);
        match scratch.try_reserve(len) {
            Ok(()) => Ok(None),
            Err(error) => match crate::table_registry::reserve_error(error, &call) {
                crate::table_registry::RegistryReserveError::AllocError(layout) => Ok(Some(layout)),
                other => Err(other),
            },
        }
    }

    /// An empty registry array for `len` tables, paid with `credit` bytes.
    #[cfg(feature = "contextdb-internal")]
    fn registry_prepare_replacement(
        &self,
        len: usize,
        credit: usize,
    ) -> Result<
        crate::table_registry::RegistryReplacement,
        crate::table_registry::RegistryReserveError,
    > {
        let mut map = crate::table_registry::new_open_tables(&self.registry_funding);
        let call = crate::table_registry::FundedCall::enter(&self.registry_funding, credit);
        map.try_reserve(len)
            .map_err(|error| crate::table_registry::reserve_error(error, &call))?;
        drop(call);
        Ok(crate::table_registry::RegistryReplacement { map })
    }

    /// Move the open tables into `replacement` (or, with none, into an
    /// unallocated registry when no table is open) and free the current
    /// array, allocating nothing. Returns the freed array's bytes. On refusal
    /// the registry and the replacement are returned unchanged.
    #[cfg(feature = "contextdb-internal")]
    fn registry_install_replacement(
        &mut self,
        replacement: Option<crate::table_registry::RegistryReplacement>,
    ) -> Result<
        usize,
        (
            crate::table_registry::RegistryReserveError,
            Option<crate::table_registry::RegistryReplacement>,
        ),
    > {
        let mut next = match replacement {
            Some(replacement) if replacement.map.capacity() >= self.open_tables.len() => {
                replacement.map
            }
            None if self.open_tables.is_empty() => {
                crate::table_registry::new_open_tables(&self.registry_funding)
            }
            other => {
                return Err((
                    crate::table_registry::RegistryReserveError::ReplacementTooSmall,
                    other,
                ));
            }
        };
        // `next` has room for every entry, so these inserts allocate nothing;
        // a monitored caller would see any allocation as an overdraft.
        for (name, location) in self.open_tables.drain() {
            next.insert(name, location);
        }
        let previous = std::mem::replace(&mut self.open_tables, next);
        let freed = previous.allocation_size();
        drop(previous);
        Ok(freed)
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

/// Transaction-local normal-table root lifecycle. Policy and witness storage belong to callers.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum TableRootEvent {
    BeforeOpen,
    AfterClose,
    BeforeDelete,
    AfterDelete,
    BeforeRelocate,
    AfterRelocate,
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
    dirty: AtomicBool,
    durability: InternalDurability,
    two_phase_commit: bool,
    shrink_policy: ShrinkPolicy,
    quick_repair: bool,
    deferred_free_budget: (usize, usize),
    // An exclusive upper bound on the freeing transaction ids this commit may reclaim, applied on
    // top of the reader floor. Set only by a bounded maintenance reclaim step.
    reclaim_ceiling: Option<TransactionId>,
    // All transaction-local savepoint lifecycle state. See
    // `SavepointTransactionState` for the commit/abort contract.
    savepoint_state: Mutex<SavepointTransactionState>,
    #[cfg(feature = "contextdb-internal")]
    table_root_hook: Option<fn(&Self, &str, TableRootEvent) -> Result<(), StorageError>>,
    #[cfg(feature = "contextdb-internal")]
    table_root_hook_error: Mutex<Option<StorageError>>,
}

/// Caller-owned observation of one actual write's publication interval.
/// Construct with `WriteTransaction::durable_publication` and retain across
/// the consuming `commit_publishing` call, including Err and unwind.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub struct DurablePublication {
    // Retain identity: a dropped database's address cannot be recycled while
    // this marker is alive. Cloning this existing Arc allocates nothing.
    mem: Arc<TransactionalMemory>,
    transaction_id: TransactionId,
    state: std::sync::atomic::AtomicU8,
}

#[cfg(feature = "contextdb-internal")]
impl DurablePublication {
    const UNSTARTED: u8 = 0;
    const HEADER_BEGUN: u8 = 1;
    const DURABLE: u8 = 2;

    #[doc(hidden)]
    pub fn is_unstarted(&self) -> bool {
        self.state.load(Ordering::Acquire) == Self::UNSTARTED
    }

    #[doc(hidden)]
    pub fn is_header_begun(&self) -> bool {
        self.state.load(Ordering::Acquire) == Self::HEADER_BEGUN
    }

    #[doc(hidden)]
    pub fn is_durable(&self) -> bool {
        self.state.load(Ordering::Acquire) == Self::DURABLE
    }

    pub(crate) fn validate(
        &self,
        mem: &TransactionalMemory,
        transaction_id: TransactionId,
    ) -> Result {
        if !std::ptr::eq(self.mem.as_ref(), mem)
            || self.transaction_id != transaction_id
            || !self.is_unstarted()
        {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "durable publication marker does not identify this unstarted write",
            )));
        }
        Ok(())
    }

    pub(crate) fn header_begun(&self) {
        self.state.store(Self::HEADER_BEGUN, Ordering::Release);
    }

    pub(crate) fn durable(&self) {
        self.state.store(Self::DURABLE, Ordering::Release);
    }
}

impl WriteTransaction {
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
        #[cfg(feature = "test-seams")]
        let tables = {
            let mut tables = tables;
            tables.table_tree.table_write_observer =
                transaction_tracker.table_write_observer.clone();
            tables
        };
        let system_tables = SystemNamespace::new(system_page, guard.clone(), mem.clone());

        let deferred_free_budget = transaction_tracker.deferred_free_budget;
        Ok(Self {
            transaction_tracker,
            mem: mem.clone(),
            transaction_guard: guard.clone(),
            transaction_id,
            tables: Mutex::new(tables),
            system_tables: Mutex::new(system_tables),
            completed: false,
            dirty: AtomicBool::new(false),
            durability: InternalDurability::Immediate,
            two_phase_commit: false,
            quick_repair: false,
            deferred_free_budget,
            reclaim_ceiling: None,
            shrink_policy: ShrinkPolicy::Default,
            savepoint_state: Mutex::new(SavepointTransactionState::default()),
            #[cfg(feature = "contextdb-internal")]
            table_root_hook: None,
            #[cfg(feature = "contextdb-internal")]
            table_root_hook_error: Mutex::new(None),
        })
    }

    /// Install fixed transaction-local lifecycle policy; no inventory is retained.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn with_table_root_hook(
        mut self,
        hook: fn(&Self, &str, TableRootEvent) -> Result<(), StorageError>,
    ) -> Self {
        self.table_root_hook = Some(hook);
        self
    }

    #[cfg(feature = "contextdb-internal")]
    fn call_table_root_hook(&self, name: &str, event: TableRootEvent) -> Result<(), StorageError> {
        if let Some(hook) = self.table_root_hook {
            hook(self, name, event)?;
        }
        Ok(())
    }

    /// Distinguish absent from empty using one normal-table directory lookup.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn normal_table_root(&self, name: &str) -> Result<Option<Option<[u8; 32]>>, TableError> {
        let mut tables = self.tables.lock().unwrap();
        tables.table_tree.flush_table_root_updates()?;
        Ok(tables
            .table_tree
            .get_table_untyped(name, TableType::Normal)?
            .map(|table| match table {
                InternalTableDefinition::Normal { table_root, .. } => {
                    table_root.map(BtreeHeader::to_le_bytes)
                }
                InternalTableDefinition::Multimap { .. } => unreachable!(),
            }))
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn record_table_write_for_test(
        &self,
        table: &str,
        operation: crate::TableWriteOperationForTest,
        key: &[u8],
        value_bytes: usize,
    ) {
        crate::table_write_observer::record(
            &self.transaction_tracker.table_write_observer,
            table,
            operation,
            key,
            value_bytes,
        );
    }

    /// Override this commit's inherited deferred reclamation limits. The first
    /// argument counts allocated extents, which may span multiple physical pages;
    /// the second counts free-list entries, including empty lists.
    #[cfg(any(test, feature = "contextdb-internal"))]
    #[doc(hidden)]
    pub fn set_deferred_free_budget(&mut self, extents: usize, entries: usize) {
        self.deferred_free_budget = (extents, entries);
    }

    #[cfg(any(test, feature = "contextdb-internal"))]
    /// Limit this commit's reclamation to entries freed by transactions before `ceiling`, in
    /// addition to the reader floor. The ceiling only ever lowers what may be freed.
    pub(crate) fn set_reclaim_ceiling(&mut self, ceiling: TransactionId) {
        self.reclaim_ceiling = Some(ceiling);
    }

    #[cfg(any(test, feature = "contextdb-internal"))]
    pub(crate) fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[cfg(any(test, feature = "contextdb-internal"))]
    /// Whether either deferred-free tree has an entry freed before `ceiling`.
    pub(crate) fn pending_free_pages_before(&self, ceiling: TransactionId) -> Result<bool> {
        let mut system_tables = self.system_tables.lock().unwrap();
        for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
            let table = system_tables.open_system_table(self, definition)?;
            if table
                .tree
                .first()?
                .is_some_and(|(key, _)| key.value().transaction_id < ceiling.raw_id())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Explicit whole-file compaction retains its upstream full-drain semantics.
    pub(crate) fn reclaim_all_deferred_pages(&mut self) {
        self.deferred_free_budget = (usize::MAX, usize::MAX);
    }

    pub(crate) fn set_shrink_policy(&mut self, shrink_policy: ShrinkPolicy) {
        self.shrink_policy = shrink_policy;
    }

    pub(crate) fn pending_free_pages(&self) -> Result<bool> {
        let mut system_tables = self.system_tables.lock().unwrap();
        if system_tables
            .open_system_table(self, DATA_FREED_TABLE)?
            .tree
            .get_root()
            .is_some()
        {
            return Ok(true);
        }
        if system_tables
            .open_system_table(self, SYSTEM_FREED_TABLE)?
            .tree
            .get_root()
            .is_some()
        {
            return Ok(true);
        }

        Ok(false)
    }

    /// Inspect only the oldest entry in each free tree using the same fence
    /// as `durable_commit`. A live reader protects newer entries, not every
    /// deferred allocation in the file.
    pub(crate) fn eligible_pending_free_pages(&self) -> Result<bool> {
        let free_until = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(self.transaction_id, |id| id.next())
            .raw_id();
        let mut system_tables = self.system_tables.lock().unwrap();
        for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
            let table = system_tables.open_system_table(self, definition)?;
            if table
                .tree
                .first()?
                .is_some_and(|(key, _)| key.value().transaction_id < free_until)
            {
                return Ok(true);
            }
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
        let table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let mut savepoints = vec![];
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
            // No need to process the system freed table, because it only rolls forward
        }

        // 2) queue all pages that became unreachable
        {
            let tables = self.tables.lock().unwrap();
            for page in tables.allocated_pages.lock().unwrap().reset() {
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

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &'txn self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'txn, K, V>, TableError> {
        #[cfg(feature = "contextdb-internal")]
        self.call_table_root_hook(definition.name(), TableRootEvent::BeforeOpen)?;
        self.tables.lock().unwrap().open_table(self, definition)
    }

    /// Check a user family's directory entry without finalizing dirty payload pages.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn table_exists<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<bool, TableError> {
        Ok(self
            .tables
            .lock()
            .unwrap()
            .table_tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?
            .is_some())
    }

    /// Open tables registered in this transaction.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_len(&self) -> usize {
        self.tables.lock().unwrap().open_tables.len()
    }

    /// Bytes of the open-table registry's array.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_bytes(&self) -> usize {
        self.tables.lock().unwrap().open_tables.allocation_size()
    }

    /// The exact layout the registry would allocate to make room for
    /// `additional` more open tables, or `None` when it already has room. The
    /// registry keeps its entries and its allocation (a tombstone-only rehash
    /// may reorder slots without allocating).
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_cost_to_reserve(
        &self,
        additional: usize,
    ) -> Result<Option<std::alloc::Layout>, crate::RegistryReserveError> {
        self.tables
            .lock()
            .unwrap()
            .registry_cost_to_reserve(additional)
    }

    /// Make room for `additional` more open tables with at most `credit`
    /// newly allocated bytes. Returns the bytes of the array this freed (zero
    /// when nothing was reallocated). On refusal the registry is unchanged.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_reserve(
        &self,
        additional: usize,
        credit: usize,
    ) -> Result<usize, crate::RegistryReserveError> {
        self.tables
            .lock()
            .unwrap()
            .registry_reserve_with(additional, credit)
    }

    /// The exact layout of an empty registry array holding `len` tables, or
    /// `None` when such an array allocates nothing.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_cost_to_hold(
        &self,
        len: usize,
    ) -> Result<Option<std::alloc::Layout>, crate::RegistryReserveError> {
        self.tables.lock().unwrap().registry_cost_to_hold(len)
    }

    /// Allocate, with at most `credit` bytes, an empty registry array for
    /// `len` tables to be installed later without allocating.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_prepare(
        &self,
        len: usize,
        credit: usize,
    ) -> Result<crate::RegistryReplacement, crate::RegistryReserveError> {
        self.tables
            .lock()
            .unwrap()
            .registry_prepare_replacement(len, credit)
    }

    /// Move the open tables into `replacement`, or into an unallocated
    /// registry when none is given and no table is open, freeing the current
    /// array without allocating. Returns the freed bytes; on refusal both are
    /// returned unchanged.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_install(
        &self,
        replacement: Option<crate::RegistryReplacement>,
    ) -> Result<
        usize,
        (
            crate::RegistryReserveError,
            Option<crate::RegistryReplacement>,
        ),
    > {
        self.tables
            .lock()
            .unwrap()
            .registry_install_replacement(replacement)
    }

    /// Count registry allocations made outside a funded reserve until the
    /// returned monitor drops.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_table_registry_monitor(&self) -> crate::RegistryMonitor {
        crate::table_registry::RegistryMonitor::start(&self.tables.lock().unwrap().registry_funding)
    }

    /// Whether `name` already has a staged root update in this transaction.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn pending_table_update_contains(&self, name: &str) -> bool {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .pending_table_update_contains(name)
    }

    /// Start recording every registry array allocation and free.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_table_registry_record_for_test(&self) {
        self.tables
            .lock()
            .unwrap()
            .registry_funding
            .start_recording();
    }

    /// The recorded registry allocations and frees, plus live and peak bytes.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_table_registry_records_for_test(
        &self,
    ) -> (Vec<crate::RegistryAllocationRecord>, usize, usize) {
        let tables = self.tables.lock().unwrap();
        let (live, peak) = tables.registry_funding.live_and_peak();
        (tables.registry_funding.take_records(), live, peak)
    }

    /// The address and length of every registry key box.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn open_table_registry_keys_for_test(&self) -> Vec<(usize, usize)> {
        self.tables
            .lock()
            .unwrap()
            .open_tables
            .keys()
            .map(|key| (key.as_ptr() as usize, key.len()))
            .collect()
    }

    /// Staged root updates: (entries, capacity).
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn pending_table_updates_for_test(&self) -> (usize, usize) {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .pending_table_updates_for_test()
    }

    /// Return the finalized root header of a user table after its open handles
    /// have been dropped. A caller can bind this physical root in another table
    /// before the same transaction commits. An empty or absent table has no root.
    #[doc(hidden)]
    pub fn table_root_fingerprint<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<Option<[u8; 32]>, TableError> {
        let mut tables = self.tables.lock().unwrap();
        tables.table_tree.flush_table_root_updates()?;
        let table = tables
            .table_tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?;
        Ok(match table {
            Some(InternalTableDefinition::Normal { table_root, .. }) => {
                table_root.map(BtreeHeader::to_le_bytes)
            }
            Some(InternalTableDefinition::Multimap { .. }) => unreachable!(),
            None => None,
        })
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
        #[cfg(feature = "contextdb-internal")]
        if let Err(error) = self.call_table_root_hook(name, TableRootEvent::AfterClose) {
            let mut failure = self.table_root_hook_error.lock().unwrap();
            if failure.is_none() {
                *failure = Some(error);
            }
        }
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
        #[cfg(feature = "contextdb-internal")]
        {
            self.call_table_root_hook(&name, TableRootEvent::BeforeDelete)?;
            self.call_table_root_hook(new_name.name(), TableRootEvent::BeforeOpen)?;
        }
        self.tables
            .lock()
            .unwrap()
            .rename_table(self, &name, new_name.name())?;
        #[cfg(feature = "contextdb-internal")]
        {
            self.call_table_root_hook(&name, TableRootEvent::AfterDelete)?;
            self.call_table_root_hook(new_name.name(), TableRootEvent::AfterClose)?;
        }
        Ok(())
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
        #[cfg(feature = "contextdb-internal")]
        self.call_table_root_hook(&name, TableRootEvent::BeforeDelete)?;
        let result = self.tables.lock().unwrap().delete_table(self, &name);
        #[cfg(feature = "contextdb-internal")]
        if result.is_ok() {
            self.call_table_root_hook(&name, TableRootEvent::AfterDelete)?;
        }
        #[cfg(not(feature = "test-seams"))]
        {
            result
        }
        #[cfg(feature = "test-seams")]
        {
            if matches!(result, Ok(true)) {
                self.record_table_write_for_test(
                    &name,
                    crate::TableWriteOperationForTest::DeleteTable,
                    &[],
                    0,
                );
            }
            result
        }
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
        #[cfg(feature = "contextdb-internal")]
        if let Some(error) = self.table_root_hook_error.lock().unwrap().take() {
            return Err(error.into());
        }
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        self.commit_inner()
    }

    /// Bind a stack marker to this database instance and actual write id.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn durable_publication(&self) -> DurablePublication {
        DurablePublication {
            mem: Arc::clone(&self.mem),
            transaction_id: self.transaction_id,
            state: std::sync::atomic::AtomicU8::new(DurablePublication::UNSTARTED),
        }
    }

    /// Commit while exposing the exact lower durable-publication point.
    /// A HeaderBegun error retains its original storage error; Durable on Err
    /// or unwind must be settled by the caller without rollback or rewind.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn commit_publishing(mut self, marker: &DurablePublication) -> Result<(), CommitError> {
        marker.validate(&self.mem, self.transaction_id)?;
        if let Some(error) = self.table_root_hook_error.lock().unwrap().take() {
            return Err(error.into());
        }
        // Match consuming commit: no abort of possibly published pages on drop.
        self.completed = true;
        self.commit_inner_publishing(marker)
    }

    fn commit_inner(&mut self) -> Result<(), CommitError> {
        // Quick-repair requires 2-phase commit
        if self.quick_repair {
            self.two_phase_commit = true;
        }

        #[cfg(feature = "test-seams")]
        let mut span_started = std::time::Instant::now();
        let (user_root, allocated_pages, data_freed) =
            self.tables.lock().unwrap().table_tree.flush_and_close()?;
        #[cfg(feature = "test-seams")]
        {
            crate::commit_work::observe_commit_span_count();
            crate::commit_work::observe_commit_span(
                crate::commit_work::CommitSpan::FlushAndClose,
                u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            span_started = std::time::Instant::now();
        }

        let allocated_pages: Vec<_> = allocated_pages.into_iter().collect();
        // Everything from here to the end of the commit is the commit's own
        // system-table bookkeeping, and the pages it copies belong to the next
        // commit's freed list rather than to the one just taken.
        #[cfg(feature = "test-seams")]
        let restore_tag = {
            crate::commit_work::observe_commit(data_freed.len(), allocated_pages.len());
            crate::commit_work::set_tag(crate::commit_work::TAG_COMMIT_BOOKKEEPING)
        };
        if self.transaction_tracker.keep_pace_reclaim {
            // Reclaim at least as much as this commit frees, so an active writer's backlog does not
            // grow while eligible older work exists. Explicit budgets above this stay in force.
            let own = data_freed.len();
            self.deferred_free_budget = (
                self.deferred_free_budget.0.max(own),
                self.deferred_free_budget.1.max(own),
            );
        }
        self.store_data_freed_pages(data_freed)?;
        self.store_allocated_pages(allocated_pages)?;
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_commit_span(
            crate::commit_work::CommitSpan::StorePageLists,
            u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        );

        #[cfg(feature = "logging")]
        debug!(
            "Committing transaction id={:?} with durability={:?} two_phase={} quick_repair={}",
            self.transaction_id, self.durability, self.two_phase_commit, self.quick_repair
        );
        match self.durability {
            InternalDurability::None => self.non_durable_commit(user_root)?,
            InternalDurability::Immediate => self.durable_commit(user_root)?,
        }

        #[cfg(feature = "test-seams")]
        crate::commit_work::set_tag(restore_tag);

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

        Ok(())
    }

    #[cfg(feature = "contextdb-internal")]
    fn commit_inner_publishing(&mut self, marker: &DurablePublication) -> Result<(), CommitError> {
        // Quick-repair requires 2-phase commit
        if self.quick_repair {
            self.two_phase_commit = true;
        }

        #[cfg(feature = "test-seams")]
        let mut span_started = std::time::Instant::now();
        let (user_root, allocated_pages, data_freed) =
            self.tables.lock().unwrap().table_tree.flush_and_close()?;
        #[cfg(feature = "test-seams")]
        {
            crate::commit_work::observe_commit_span_count();
            crate::commit_work::observe_commit_span(
                crate::commit_work::CommitSpan::FlushAndClose,
                u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            span_started = std::time::Instant::now();
        }

        let allocated_pages: Vec<_> = allocated_pages.into_iter().collect();
        // Everything from here to the end of the commit is the commit's own
        // system-table bookkeeping, and the pages it copies belong to the next
        // commit's freed list rather than to the one just taken.
        #[cfg(feature = "test-seams")]
        let restore_tag = {
            crate::commit_work::observe_commit(data_freed.len(), allocated_pages.len());
            crate::commit_work::set_tag(crate::commit_work::TAG_COMMIT_BOOKKEEPING)
        };
        if self.transaction_tracker.keep_pace_reclaim {
            // Reclaim at least as much as this commit frees, so an active writer's backlog does not
            // grow while eligible older work exists. Explicit budgets above this stay in force.
            let own = data_freed.len();
            self.deferred_free_budget = (
                self.deferred_free_budget.0.max(own),
                self.deferred_free_budget.1.max(own),
            );
        }
        self.store_data_freed_pages(data_freed)?;
        self.store_allocated_pages(allocated_pages)?;
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_commit_span(
            crate::commit_work::CommitSpan::StorePageLists,
            u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        );

        #[cfg(feature = "logging")]
        debug!(
            "Committing transaction id={:?} with durability={:?} two_phase={} quick_repair={}",
            self.transaction_id, self.durability, self.two_phase_commit, self.quick_repair
        );
        match self.durability {
            InternalDurability::None => self.non_durable_commit(user_root)?,
            InternalDurability::Immediate => self.durable_commit_publishing(user_root, marker)?,
        }

        #[cfg(feature = "test-seams")]
        crate::commit_work::set_tag(restore_tag);

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

        Ok(())
    }

    fn store_data_freed_pages(&self, mut freed_pages: Vec<PageNumber>) -> Result {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut freed_table = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
        let mut pagination_counter = 0;
        while !freed_pages.is_empty() {
            let chunk_size = 400;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: pagination_counter,
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

            pagination_counter += 1;
        }

        Ok(())
    }

    fn store_allocated_pages(&self, mut data_allocated_pages: Vec<PageNumber>) -> Result {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut allocated_table = system_tables.open_system_table(self, DATA_ALLOCATED_TABLE)?;
        let mut pagination_counter = 0;
        while !data_allocated_pages.is_empty() {
            let chunk_size = 400;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: pagination_counter,
            };
            let mut access_guard = allocated_table.insert_reserve(&key, buffer_size)?;

            let len = data_allocated_pages.len();
            access_guard.as_mut().clear();
            for page in data_allocated_pages.drain(len - min(len, chunk_size)..) {
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
        self.mem.rollback_uncommitted_writes()?;
        #[cfg(feature = "logging")]
        debug!("Finished abort of transaction id={:?}", self.transaction_id);
        Ok(())
    }

    pub(crate) fn durable_commit(&mut self, user_root: Option<BtreeHeader>) -> Result {
        #[cfg(feature = "test-seams")]
        let mut span_started = std::time::Instant::now();
        let mut free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(self.transaction_id, |x| x.next());
        if let Some(ceiling) = self.reclaim_ceiling {
            free_until_transaction = TransactionId::min(free_until_transaction, ceiling);
        }
        self.process_freed_pages(free_until_transaction)?;
        #[cfg(feature = "test-seams")]
        {
            crate::commit_work::observe_commit_span(
                crate::commit_work::CommitSpan::ProcessFreedPages,
                u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            span_started = std::time::Instant::now();
        }

        let mut system_tables = self.system_tables.lock().unwrap();
        let system_freed_pages = system_tables.system_freed_pages();
        let system_tree = system_tables.table_tree.flush_table_root_updates()?;
        let retire_saved_allocator = if self.quick_repair {
            self.mem.materialize_allocator_source()?;
            true
        } else {
            self.mem.materialize_small_allocator_source()?
        };
        if retire_saved_allocator {
            system_tree
                .delete_table(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?;
        }

        if self.quick_repair {
            system_tree.create_table_and_flush_table_root(
                ALLOCATOR_STATE_TABLE_NAME,
                |system_tree_ref, tree: &mut AllocatorStateTreeMut| {
                    let mut pagination_counter = 0;

                    loop {
                        let num_regions = self
                            .mem
                            .reserve_allocator_state(tree, self.transaction_id)?;

                        // We can't free pages after the commit, because that would invalidate our
                        // saved allocator state. Everything needs to go through the transactional
                        // free mechanism
                        self.store_system_freed_pages(
                            system_tree_ref,
                            system_freed_pages.clone(),
                            None,
                            &mut pagination_counter,
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
        #[cfg(feature = "test-seams")]
        {
            crate::commit_work::observe_commit_span(
                crate::commit_work::CommitSpan::SystemTree,
                u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            span_started = std::time::Instant::now();
        }

        #[cfg(all(feature = "test-seams", unix))]
        self.pause_reclaim_step_before_durable_publication_from_env()?;
        self.mem.commit(
            user_root,
            system_root,
            self.transaction_id,
            self.two_phase_commit,
            self.shrink_policy,
        )?;
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_commit_span(
            crate::commit_work::CommitSpan::StorageCommit,
            u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        );

        // Mark any pending non-durable commits as fully committed.
        self.transaction_tracker.clear_pending_non_durable_commits();

        // Immediately free the pages that were freed from the system-tree. These are only
        // accessed by write transactions, so it's safe to free them as soon as the commit is done.
        for page in system_freed_pages.lock().unwrap().drain(..) {
            self.mem.free(page, &mut PageTrackerPolicy::Ignore);
        }

        Ok(())
    }

    #[cfg(feature = "contextdb-internal")]
    fn durable_commit_publishing(
        &mut self,
        user_root: Option<BtreeHeader>,
        marker: &DurablePublication,
    ) -> Result {
        #[cfg(feature = "test-seams")]
        let mut span_started = std::time::Instant::now();
        let mut free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(self.transaction_id, |x| x.next());
        if let Some(ceiling) = self.reclaim_ceiling {
            free_until_transaction = TransactionId::min(free_until_transaction, ceiling);
        }
        self.process_freed_pages(free_until_transaction)?;
        #[cfg(feature = "test-seams")]
        {
            crate::commit_work::observe_commit_span(
                crate::commit_work::CommitSpan::ProcessFreedPages,
                u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            span_started = std::time::Instant::now();
        }

        let mut system_tables = self.system_tables.lock().unwrap();
        let system_freed_pages = system_tables.system_freed_pages();
        let system_tree = system_tables.table_tree.flush_table_root_updates()?;
        let retire_saved_allocator = if self.quick_repair {
            self.mem.materialize_allocator_source()?;
            true
        } else {
            self.mem.materialize_small_allocator_source()?
        };
        if retire_saved_allocator {
            system_tree
                .delete_table(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?;
        }

        if self.quick_repair {
            system_tree.create_table_and_flush_table_root(
                ALLOCATOR_STATE_TABLE_NAME,
                |system_tree_ref, tree: &mut AllocatorStateTreeMut| {
                    let mut pagination_counter = 0;

                    loop {
                        let num_regions = self
                            .mem
                            .reserve_allocator_state(tree, self.transaction_id)?;

                        // We can't free pages after the commit, because that would invalidate our
                        // saved allocator state. Everything needs to go through the transactional
                        // free mechanism
                        self.store_system_freed_pages(
                            system_tree_ref,
                            system_freed_pages.clone(),
                            None,
                            &mut pagination_counter,
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
        #[cfg(feature = "test-seams")]
        {
            crate::commit_work::observe_commit_span(
                crate::commit_work::CommitSpan::SystemTree,
                u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            span_started = std::time::Instant::now();
        }

        #[cfg(all(feature = "test-seams", unix))]
        self.pause_reclaim_step_before_durable_publication_from_env()?;
        self.mem.commit_publishing(
            user_root,
            system_root,
            self.transaction_id,
            self.two_phase_commit,
            self.shrink_policy,
            marker,
        )?;
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_commit_span(
            crate::commit_work::CommitSpan::StorageCommit,
            u64::try_from(span_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        );

        // Mark any pending non-durable commits as fully committed.
        self.transaction_tracker.clear_pending_non_durable_commits();

        // Immediately free the pages that were freed from the system-tree. These are only
        // accessed by write transactions, so it's safe to free them as soon as the commit is done.
        for page in system_freed_pages.lock().unwrap().drain(..) {
            self.mem.free(page, &mut PageTrackerPolicy::Ignore);
        }

        Ok(())
    }

    /// Stop an explicitly armed reclaim transaction after its mutations are
    /// staged but immediately before durable publication so an integration test
    /// can terminate the process there.
    /// This is compiled only into the test-seams Unix test surface.
    #[cfg(all(feature = "test-seams", unix))]
    fn pause_reclaim_step_before_durable_publication_from_env(&self) -> Result {
        const SOCKET_ENV: &str = "WRITECOST_REDB_PAUSE_BEFORE_DURABLE_COMMIT_SOCKET";
        const MARKER: &[u8] = b"RECLAIM_MUTATIONS_STAGED";

        if self.reclaim_ceiling.is_none() {
            return Ok(());
        }
        let Some(socket) = std::env::var_os(SOCKET_ENV) else {
            return Ok(());
        };
        let stream = std::os::unix::net::UnixDatagram::unbound()?;
        stream.connect(socket)?;
        stream.send(MARKER)?;
        let mut release = [0];
        stream.recv(&mut release)?;
        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&mut self, user_root: Option<BtreeHeader>) -> Result {
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
            // Store all freed pages for a future commit(), since we can't free pages during a
            // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
            self.store_system_freed_pages(
                &mut system_tables.table_tree,
                system_freed_pages,
                Some(&mut post_commit_frees),
                &mut 0,
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

        Ok(())
    }

    // Relocate pages to lower number regions/pages
    // Returns true if a page(s) was moved
    pub(crate) fn compact_pages(&mut self) -> Result<bool> {
        // Relocation can replace the allocator-state tree's source pages. Copy
        // its lazy bitmap words to scratch before those pages become reusable.
        self.mem.materialize_allocator_source()?;
        let mut progress = false;

        // Find the 1M highest pages
        let mut highest_pages = BTreeMap::new();
        let mut tables = self.tables.lock().unwrap();
        let table_tree = &mut tables.table_tree;
        table_tree.highest_index_pages(MAX_PAGES_PER_COMPACTION, &mut highest_pages)?;
        let mut system_tables = self.system_tables.lock().unwrap();
        let system_table_tree = &mut system_tables.table_tree;
        system_table_tree.highest_index_pages(MAX_PAGES_PER_COMPACTION, &mut highest_pages)?;

        // Calculate how many of them can be relocated to lower pages, starting from the last page
        let mut relocation_map = HashMap::new();
        for path in highest_pages.into_values().rev() {
            if relocation_map.contains_key(&path.page_number()) {
                continue;
            }
            let old_page = self.mem.get_page(path.page_number(), PageHint::None)?;
            let mut new_page = self.mem.allocate_lowest(old_page.memory().len())?;
            let new_page_number = new_page.get_page_number();
            // We have to copy at least the page type into the new page.
            // Otherwise its cache priority will be calculated incorrectly
            new_page.memory_mut()[0] = old_page.memory()[0];
            drop(new_page);
            // We're able to move this to a lower page, so insert it and rewrite all its parents
            if new_page_number < path.page_number() {
                relocation_map.insert(path.page_number(), new_page_number);
                for parent in path.parents() {
                    if relocation_map.contains_key(parent) {
                        continue;
                    }
                    let old_parent = self.mem.get_page(*parent, PageHint::None)?;
                    let mut new_page = self.mem.allocate_lowest(old_parent.memory().len())?;
                    let new_page_number = new_page.get_page_number();
                    // We have to copy at least the page type into the new page.
                    // Otherwise its cache priority will be calculated incorrectly
                    new_page.memory_mut()[0] = old_parent.memory()[0];
                    drop(new_page);
                    relocation_map.insert(*parent, new_page_number);
                }
            } else {
                self.mem
                    .free(new_page_number, &mut PageTrackerPolicy::Ignore);
                break;
            }
        }

        if !relocation_map.is_empty() {
            progress = true;
        }

        table_tree.relocate_tables(&relocation_map)?;
        system_table_tree.relocate_tables(&relocation_map)?;

        Ok(progress)
    }

    /// Publish one bounded relocation transaction. The owned transaction lets
    /// callers release handle/admission locks before traversing storage pages.
    /// Old committed pages enter ordinary reader-fenced deferred reclamation.
    #[doc(hidden)]
    pub fn compact_step(
        self,
        cursor: &mut CompactionCursor,
        maximum: usize,
        admit: impl FnMut(usize) -> std::io::Result<()>,
    ) -> Result<CompactionProgress, crate::CompactionError> {
        self.compact_step_with_hook(cursor, maximum, admit, |_| Ok(()))
    }

    /// Publish a relocation batch after the caller has bound any table-root
    /// fingerprints changed by page movement in this same transaction.
    #[doc(hidden)]
    pub fn compact_step_with_hook(
        self,
        cursor: &mut CompactionCursor,
        maximum: usize,
        admit: impl FnMut(usize) -> std::io::Result<()>,
        on_batch: impl FnOnce(&Self) -> Result<(), StorageError>,
    ) -> Result<CompactionProgress, crate::CompactionError> {
        self.compact_step_with_cursor_hook(cursor, maximum, admit, |txn, _| on_batch(txn))
    }

    /// Checkpoint the successor cursor in the relocation transaction. The
    /// caller sees the successor in memory only after that commit succeeds.
    #[doc(hidden)]
    pub fn compact_step_with_cursor_hook(
        self,
        cursor: &mut CompactionCursor,
        maximum: usize,
        mut admit: impl FnMut(usize) -> std::io::Result<()>,
        on_batch: impl FnOnce(&Self, &CompactionCursor) -> Result<(), StorageError>,
    ) -> Result<CompactionProgress, crate::CompactionError> {
        self.compact_step_with_cursor_hook_storage(
            cursor,
            maximum,
            |bytes| admit(bytes).map_err(StorageError::from),
            on_batch,
        )
    }

    /// Checkpoint relocation while retaining typed caller admission errors.
    #[doc(hidden)]
    pub fn compact_step_with_cursor_hook_storage(
        mut self,
        cursor: &mut CompactionCursor,
        maximum: usize,
        mut admit: impl FnMut(usize) -> Result<(), StorageError>,
        on_batch: impl FnOnce(&Self, &CompactionCursor) -> Result<(), StorageError>,
    ) -> Result<CompactionProgress, crate::CompactionError> {
        assert!(maximum > 0);
        if self.transaction_tracker.any_savepoint_exists() {
            return Err(crate::CompactionError::EphemeralSavepointExists);
        }
        if self.list_persistent_savepoints()?.next().is_some() {
            return Err(crate::CompactionError::PersistentSavepointExists);
        }
        let mut next = cursor.clone();
        next.active = true;
        let progress = self.compact_page_batch(&mut next, maximum, &mut admit)?;
        on_batch(&self, &next)?;
        self.commit_compaction_frees()
            .map_err(|e| e.into_storage_error())?;
        *cursor = next;
        Ok(progress)
    }

    /// Advance reader-safe reclamation without another page-tree traversal.
    #[doc(hidden)]
    pub fn commit_compaction_frees(mut self) -> Result<(), CommitError> {
        self.set_two_phase_commit(true);
        self.set_shrink_policy(ShrinkPolicy::Maximum);
        self.commit()
    }

    pub(crate) fn compact_page_batch(
        &mut self,
        cursor: &mut CompactionCursor,
        maximum: usize,
        admit: &mut impl FnMut(usize) -> Result<(), StorageError>,
    ) -> Result<CompactionProgress> {
        // The saved allocator tree is a live lazy bitmap source. Keep that
        // one root reachable instead of copying all its words under the writer
        // lock before a finite batch's admission. Relocating the system master
        // tree preserves this table's child root. Quick repair and explicit
        // whole-file compaction detach the source before replacing its pages.
        let protected_table = (cursor.phase >= 2 && self.mem.has_saved_allocator_source())
            .then_some(ALLOCATOR_STATE_TABLE_NAME);
        let mut tables = self.tables.lock().unwrap();
        let mut system_tables = self.system_tables.lock().unwrap();
        let tree = if cursor.phase < 2 {
            &mut tables.table_tree
        } else {
            &mut system_tables.table_tree
        };
        let user_table = cursor.phase < 2;
        let (target, pages, examined) =
            tree.compaction_page_batch(cursor, maximum, protected_table)?;
        drop(system_tables);
        drop(tables);
        #[cfg(feature = "contextdb-internal")]
        if user_table && let Some(name) = target.as_deref() {
            self.call_table_root_hook(name, TableRootEvent::BeforeRelocate)?;
        }
        let mut relocation_map = HashMap::new();
        let pages = pages
            .into_iter()
            .map(|p| (p.page_number(), p))
            .collect::<BTreeMap<_, _>>();
        for path in pages.into_values().rev() {
            if relocation_map.contains_key(&path.page_number()) {
                continue;
            }
            let bytes = self
                .mem
                .get_page_size()
                .checked_shl(u32::from(path.page_number().page_order))
                .ok_or_else(|| StorageError::Corrupted("relocation extent size overflow".into()))?;
            admit(bytes.saturating_add(128))?;
            let old = self.mem.get_page(path.page_number(), PageHint::None)?;
            let mut new = self.mem.allocate_lowest(old.memory().len())?;
            let number = new.get_page_number();
            new.memory_mut()[0] = old.memory()[0];
            drop(new);
            if number >= path.page_number() {
                self.mem.free(number, &mut PageTrackerPolicy::Ignore);
                continue;
            }
            relocation_map.insert(path.page_number(), number);
            for parent in path.parents() {
                if relocation_map.contains_key(parent) {
                    continue;
                }
                let bytes = self
                    .mem
                    .get_page_size()
                    .checked_shl(u32::from(parent.page_order))
                    .ok_or_else(|| {
                        StorageError::Corrupted("relocation extent size overflow".into())
                    })?;
                admit(bytes.saturating_add(128))?;
                let old = self.mem.get_page(*parent, PageHint::None)?;
                let mut new = self.mem.allocate_lowest(old.memory().len())?;
                new.memory_mut()[0] = old.memory()[0];
                relocation_map.insert(*parent, new.get_page_number());
            }
        }
        // Relocation copies committed pages and puts their old addresses in
        // the normal deferred-free tree. process_freed_pages uses the oldest
        // read transaction fence; neither relocation nor subsequent recording
        // can reuse those addresses until that reader releases its snapshot.
        {
            let mut tables = self.tables.lock().unwrap();
            let mut system_tables = self.system_tables.lock().unwrap();
            let tree = if user_table {
                &mut tables.table_tree
            } else {
                &mut system_tables.table_tree
            };
            tree.relocate_compaction_batch(target.as_deref(), &relocation_map)?;
        }
        #[cfg(feature = "contextdb-internal")]
        if user_table
            && !relocation_map.is_empty()
            && let Some(name) = target.as_deref()
        {
            self.call_table_root_hook(name, TableRootEvent::AfterRelocate)?;
        }
        #[cfg(all(test, debug_assertions))]
        online_relocation_tests::RELOCATIONS.with(|pairs| {
            pairs
                .borrow_mut()
                .extend(relocation_map.iter().map(|(from, to)| (*from, *to)));
        });
        let complete = cursor.phase == 4;
        if complete {
            // One automatic sweep is finite even while writers create new
            // relocation opportunities. The next scheduled sweep can reclaim
            // those pages; explicit whole-file compaction still drains fully.
            *cursor = CompactionCursor::default();
        }
        Ok(CompactionProgress {
            pages_examined: examined,
            pages_relocated: relocation_map.len(),
            complete,
        })
    }

    // NOTE: must be called before store_system_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(&mut self, free_until: TransactionId) -> Result {
        let (pages, entries) = self.deferred_free_budget;
        // Give each tree half the shared cap. The odd unit follows the oldest
        // durable pending key, so a one-unit budget cannot starve either tree
        // across reopen or arbitrarily many aborted transactions. Newly created
        // bookkeeping entries sort after the work already waiting.
        let extra_for_data = if pages % 2 != 0 || entries % 2 != 0 {
            let mut tables = self.system_tables.lock().unwrap();
            let mut oldest = |definition: SystemTableDefinition<
                TransactionIdWithPagination,
                PageList,
            >|
             -> Result<Option<(u64, u64)>> {
                let table = tables.open_system_table(self, definition)?;
                Ok(table.tree.first()?.map(|(key, _)| {
                    let key = key.value();
                    (key.transaction_id, key.pagination_id)
                }))
            };
            let data = oldest(DATA_FREED_TABLE)?;
            let system = oldest(SYSTEM_FREED_TABLE)?;
            match (data, system) {
                (Some(data), Some(system)) => data <= system,
                (Some(_), None) => true,
                _ => false,
            }
        } else {
            false
        };
        let data_pages = pages / 2 + usize::from(extra_for_data) * (pages % 2);
        let data_entries = entries / 2 + usize::from(extra_for_data) * (entries % 2);
        let (data_pages_left, data_entries_left) = self.process_freed_pages_bounded(
            free_until,
            DATA_FREED_TABLE,
            data_pages,
            data_entries,
        )?;
        let (system_pages_left, system_entries_left) = self.process_freed_pages_bounded(
            free_until,
            SYSTEM_FREED_TABLE,
            pages - data_pages,
            entries - data_entries,
        )?;
        // The first pass gives both trees their fair share. If one has no
        // eligible work, lend its unused capacity to the other without ever
        // exceeding either caller-owned total budget.
        let pages_left = data_pages_left + system_pages_left;
        let entries_left = data_entries_left + system_entries_left;
        if pages_left == 0 || entries_left == 0 || (pages_left == pages && entries_left == entries)
        {
            return Ok(());
        }
        let (pages_left, entries_left) = self.process_freed_pages_bounded(
            free_until,
            DATA_FREED_TABLE,
            pages_left,
            entries_left,
        )?;
        if pages_left > 0 && entries_left > 0 {
            self.process_freed_pages_bounded(
                free_until,
                SYSTEM_FREED_TABLE,
                pages_left,
                entries_left,
            )?;
        }
        Ok(())
    }

    fn process_freed_pages_bounded(
        &self,
        free_until: TransactionId,
        definition: SystemTableDefinition<TransactionIdWithPagination, PageList>,
        mut pages_remaining: usize,
        mut entries_remaining: usize,
    ) -> Result<(usize, usize)> {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut freed = system_tables.open_system_table(self, definition)?;
        while pages_remaining > 0 && entries_remaining > 0 {
            let next = freed.tree.first()?.map(|(key, _)| key.value());
            let Some(key) = next else { break };
            if key.transaction_id >= free_until.raw_id() {
                break;
            }
            // Only one B-tree value is resident. Mutate its count in place in
            // the transaction's copy-on-write leaf; the retained prefix is a
            // durable continuation across committed writes and restarts.
            // Ordinary abort happens before this commit-only reclamation phase.
            let mut entry = freed.tree.get_mut(&key)?.expect("first entry exists");
            entries_remaining -= 1;
            #[cfg(feature = "test-seams")]
            crate::commit_work::observe_reclaimed_entry();
            // Each PageList item is one allocation extent. Its page order may
            // cover several physical pages, but it consumes one budget unit.
            while pages_remaining > 0 {
                let Some(page) = entry.value_mut().pop_back() else {
                    break;
                };
                self.mem.free(page, &mut PageTrackerPolicy::Ignore);
                pages_remaining -= 1;
                #[cfg(feature = "test-seams")]
                crate::commit_work::observe_reclaimed_page();
            }
            let empty = entry.value().len() == 0;
            drop(entry);
            if empty {
                freed.remove(&key)?;
            }
        }
        Ok((pages_remaining, entries_remaining))
    }

    /// Inspect the exact root-page intersection with the deferred-free table
    /// on demand. The transaction floor may also defer unrelated pages; this
    /// diagnostic deliberately does not inventory them on writer admission.
    #[doc(hidden)]
    pub fn external_snapshot_retention_stats(&self) -> Result<ExternalSnapshotRetentionStats> {
        let snapshots = self
            .transaction_tracker
            .authenticated_external_snapshot_page_sets(&self.mem)?;
        let protected = snapshots
            .iter()
            .flat_map(|(_, pages)| pages.iter().copied())
            .collect::<HashSet<_>>();
        let mut retained = HashSet::new();
        let mut system_tables = self.system_tables.lock().unwrap();
        let data_freed = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
        for entry in data_freed.range::<TransactionIdWithPagination>(..)? {
            let (_, pages) = entry?;
            for index in 0..pages.value().len() {
                let page = pages.value().get(index);
                if protected.contains(&page) {
                    retained.insert(page);
                }
            }
        }
        let page_size = u32::try_from(self.mem.get_page_size())
            .expect("redb page size is represented by a 32-bit header field");
        let reachable_bytes = protected
            .iter()
            .map(|page| page.page_size_bytes(page_size))
            .sum();
        let referenced_pages = snapshots.iter().map(|(_, pages)| pages.len() as u64).sum();
        let referenced_bytes = snapshots
            .iter()
            .flat_map(|(_, pages)| pages.iter())
            .map(|page| page.page_size_bytes(page_size))
            .sum();
        let retained_bytes = retained
            .iter()
            .map(|page| page.page_size_bytes(page_size))
            .sum();
        Ok(ExternalSnapshotRetentionStats {
            snapshots: snapshots.len() as u64,
            reachable_pages: protected.len() as u64,
            referenced_pages,
            retained_pages: retained.len() as u64,
            reachable_bytes,
            referenced_bytes,
            retained_bytes,
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn external_snapshot_retained_pages_for_test(&self) -> Result<Vec<[u8; 8]>> {
        let protected = self
            .transaction_tracker
            .external_snapshot_page_sets(&self.mem)?
            .into_iter()
            .flat_map(|(_, pages)| pages)
            .collect::<HashSet<_>>();
        let mut retained = BTreeSet::new();
        let mut system_tables = self.system_tables.lock().unwrap();
        let data_freed = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
        for entry in data_freed.range::<TransactionIdWithPagination>(..)? {
            let (_, pages) = entry?;
            for index in 0..pages.value().len() {
                let page = pages.value().get(index);
                if protected.contains(&page) {
                    retained.insert(page);
                }
            }
        }
        Ok(retained.into_iter().map(PageNumber::to_le_bytes).collect())
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
        pagination_counter: &mut u64,
    ) -> Result {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8

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

        Ok(())
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

    pub(crate) fn storage_statistics_snapshot(
        &self,
        read: ReadTransaction,
    ) -> Result<StorageStatisticsSnapshot> {
        let system = self.system_tables.lock().unwrap().table_tree.stats()?;
        Ok(StorageStatisticsSnapshot {
            read,
            system,
            free_pages: self.mem.count_free_pages()?,
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
    }
}

/// A read-only transaction
///
/// Read-only transactions may exist concurrently with writes
pub struct ReadTransaction {
    mem: Arc<TransactionalMemory>,
    tree: TableTree,
}

/// Stable identity of the user-tree root captured by one read transaction.
///
/// This is storage identity, not an application content digest. Consumers
/// retaining external snapshot pages use it to name the exact copy-on-write
/// root whose reachability must be preserved.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageSnapshotIdentity {
    pub transaction_id: u64,
    pub user_root: Option<StorageUserRootIdentity>,
}

/// The physical root header of the user table tree.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageUserRootIdentity {
    pub page: [u8; 8],
    pub checksum: u128,
    pub length: u64,
}

/// Supplies the complete authenticated external snapshot set before a writer
/// may allocate or reclaim pages. An error refuses the write without changing
/// storage; an empty set authorizes release of all external holds.
#[doc(hidden)]
pub type ExternalSnapshotProvider =
    Arc<dyn Fn() -> io::Result<Vec<StorageSnapshotIdentity>> + Send + Sync>;

/// On-demand root-page diagnostic for authenticated external snapshots.
///
/// `reachable_bytes` is the union reachable from all roots. `referenced_bytes`
/// counts shared pages once per root. `retained_bytes` is the union of those
/// root pages that current state no longer reaches. The transaction floor can
/// defer additional unrelated pages until the final snapshot releases.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExternalSnapshotRetentionStats {
    pub snapshots: u64,
    pub reachable_pages: u64,
    pub referenced_pages: u64,
    pub retained_pages: u64,
    pub reachable_bytes: u64,
    pub referenced_bytes: u64,
    pub retained_bytes: u64,
}

/// Application pages follow MVCC; allocator/system pages do not. Capture
/// only the system's numeric totals under the short writer boundary, then
/// traverse application data through an ordinary independent read snapshot.
#[doc(hidden)]
pub struct StorageStatisticsSnapshot {
    read: ReadTransaction,
    system: DatabaseStats,
    free_pages: u64,
}

impl StorageStatisticsSnapshot {
    pub fn stats(&self) -> Result<DatabaseStats> {
        let data = self.read.tree.stats()?;
        Ok(DatabaseStats {
            tree_height: data.tree_height(),
            allocated_pages: self.system.allocated_pages(),
            leaf_pages: data.leaf_pages(),
            branch_pages: data.branch_pages(),
            stored_leaf_bytes: data.stored_bytes(),
            metadata_bytes: data.metadata_bytes()
                + self.system.metadata_bytes
                + self.system.stored_leaf_bytes,
            fragmented_bytes: data.fragmented_bytes()
                + self.system.fragmented_bytes
                + self.free_pages * self.system.page_size as u64,
            page_size: self.system.page_size,
        })
    }
}

impl ReadTransaction {
    pub(crate) fn new(
        mem: Arc<TransactionalMemory>,
        guard: TransactionGuard,
    ) -> Result<Self, TransactionError> {
        Self::new_with_hint(mem, guard, PageHint::Clean)
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn new_verified(
        mem: Arc<TransactionalMemory>,
        guard: TransactionGuard,
    ) -> Result<Self, TransactionError> {
        Self::new_with_hint(mem, guard, PageHint::Verified)
    }

    fn new_with_hint(
        mem: Arc<TransactionalMemory>,
        guard: TransactionGuard,
        hint: PageHint,
    ) -> Result<Self, TransactionError> {
        let root_page = mem.get_data_root();
        let guard = Arc::new(guard);
        Ok(Self {
            mem: mem.clone(),
            tree: TableTree::new(root_page, hint, guard, mem).map_err(TransactionError::Storage)?,
        })
    }

    pub(crate) fn new_external(
        mem: Arc<TransactionalMemory>,
        snapshot: StorageSnapshotIdentity,
    ) -> Result<Self, TransactionError> {
        let root = match snapshot.user_root {
            Some(root) => Some(BtreeHeader::new(
                PageNumber::validated_from_le_bytes(root.page).ok_or_else(|| {
                    StorageError::Corrupted(
                        "external snapshot root has an invalid allocation order".to_owned(),
                    )
                })?,
                root.checksum,
                root.length,
            )),
            None => None,
        };
        let guard = Arc::new(crate::db::TransactionGuard::external(TransactionId::new(
            snapshot.transaction_id,
        )));
        Ok(Self {
            mem: mem.clone(),
            tree: TableTree::new(root, PageHint::Verified, guard, mem)
                .map_err(TransactionError::Storage)?,
        })
    }

    /// Name the exact committed user-tree root this transaction reads.
    #[doc(hidden)]
    pub fn storage_snapshot_identity(&self) -> StorageSnapshotIdentity {
        StorageSnapshotIdentity {
            transaction_id: self.tree.transaction_guard().id().raw_id(),
            user_root: self.tree.root().map(|root| StorageUserRootIdentity {
                page: root.root.to_le_bytes(),
                checksum: root.checksum,
                length: root.length,
            }),
        }
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
                self.tree.hint(),
                self.tree.transaction_guard().clone(),
                self.mem.clone(),
            )?),
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }
    }

    /// Open a byte table for reject-only bounded inspection. Selected records
    /// still use ordinary complete-page admission and checksum verification.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_byte_table_bounded(
        &self,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
    ) -> Result<ReadOnlyTable<&'static [u8], &'static [u8]>, TableError> {
        let header = self
            .tree
            .get_table::<&[u8], &[u8]>(definition.name(), TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;
        match header {
            InternalTableDefinition::Normal { table_root, .. } => Ok(ReadOnlyTable::new_bounded(
                definition.name().to_string(),
                table_root,
                self.tree.hint(),
                self.tree.transaction_guard().clone(),
                self.mem.clone(),
            )),
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }
    }

    /// Return the committed root header without reading the table's payload pages.
    #[doc(hidden)]
    pub fn table_root_fingerprint<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<Option<[u8; 32]>, TableError> {
        let table = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?;
        Ok(match table {
            Some(InternalTableDefinition::Normal { table_root, .. }) => {
                table_root.map(BtreeHeader::to_le_bytes)
            }
            Some(InternalTableDefinition::Multimap { .. }) => unreachable!(),
            None => None,
        })
    }

    /// Read one committed normal-table header, distinguishing empty from absent.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn normal_table_root(&self, name: &str) -> Result<Option<Option<[u8; 32]>>, TableError> {
        Ok(self
            .tree
            .get_table_untyped(name, TableType::Normal)?
            .map(|table| match table {
                InternalTableDefinition::Normal { table_root, .. } => {
                    table_root.map(BtreeHeader::to_le_bytes)
                }
                InternalTableDefinition::Multimap { .. } => unreachable!(),
            }))
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
                self.tree.hint(),
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
                self.tree.hint(),
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
                self.tree.hint(),
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
    use crate::backends::FileBackend;
    use crate::tree_store::{Page, PageHint, PageNumber};
    use crate::{
        Database, ExternalSnapshotProvider, ExternalSnapshotRetentionStats, ReadableDatabase,
        TableDefinition,
    };
    use std::collections::{HashMap, HashSet};
    use std::fs::OpenOptions;
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn external_roots_retain_only_reachable_allocation_versions_and_release_them_for_reuse() {
        const VALUES: TableDefinition<u64, &[u8]> = TableDefinition::new("external_values");
        const CHURN: TableDefinition<u64, &[u8]> = TableDefinition::new("post_snapshot_churn");
        const REUSE: TableDefinition<u64, &[u8]> = TableDefinition::new("released_page_reuse");

        let file = crate::create_tempfile();
        let db = Database::create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(VALUES).unwrap();
            for key in 0..256 {
                table
                    .insert(key, vec![(key % 251) as u8; 8192].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        drop(db);

        let read_db = crate::ReadOnlyDatabase::open(file.path()).unwrap();
        let read = read_db.begin_read().unwrap();
        let snapshot = read.storage_snapshot_identity();
        assert_eq!(
            read.open_table(VALUES)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            &[0; 8192]
        );
        drop(read);
        drop(read_db);

        let registered = Arc::new(Mutex::new(vec![snapshot]));
        let provider_state = registered.clone();
        let provider: ExternalSnapshotProvider =
            Arc::new(move || Ok(provider_state.lock().unwrap().clone()));
        let backend = FileBackend::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(file.path())
                .unwrap(),
        )
        .unwrap();
        let db = Database::builder()
            .open_with_backend_and_external_snapshots(backend, provider)
            .unwrap();
        let mem = db.get_memory();

        let write = db.begin_write().unwrap();
        write
            .open_table(VALUES)
            .unwrap()
            .insert(0, &[99; 8192][..])
            .unwrap();
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        write
            .open_table(CHURN)
            .unwrap()
            .insert(0, &[17; 8192][..])
            .unwrap();
        write.commit().unwrap();
        for _ in 0..3 {
            db.begin_write().unwrap().commit().unwrap();
        }

        let retained_ids = db.external_snapshot_retained_pages_for_test().unwrap();
        let baseline = db.external_snapshot_retention_stats().unwrap();
        assert_eq!(baseline.snapshots, 1);
        assert!(baseline.reachable_pages > 0);
        assert!(baseline.retained_pages > 0);
        assert_eq!(baseline.retained_pages, retained_ids.len() as u64);
        assert!(baseline.referenced_pages >= baseline.reachable_pages);
        assert_eq!(baseline.retained_bytes % 4096, 0);
        let retained_pages = retained_ids
            .iter()
            .copied()
            .map(PageNumber::from_le_bytes)
            .collect::<HashSet<_>>();
        let retained_bytes = retained_pages
            .iter()
            .map(|page| {
                assert!(mem.is_allocated(*page));
                (
                    *page,
                    mem.get_page(*page, PageHint::None)
                        .unwrap()
                        .memory()
                        .to_vec(),
                )
            })
            .collect::<HashMap<_, _>>();

        let old = db.begin_external_read(snapshot).unwrap();
        assert_eq!(
            old.open_table(VALUES)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            &[0; 8192],
            "the retained physical root must still expose the pre-writer value"
        );
        drop(old);

        for generation in 1..=24u8 {
            let write = db.begin_write().unwrap();
            write
                .open_table(CHURN)
                .unwrap()
                .insert(0, vec![generation; 8192].as_slice())
                .unwrap();
            write.commit().unwrap();
            let now = db.external_snapshot_retained_pages_for_test().unwrap();
            assert_eq!(
                now, retained_ids,
                "versions born after the registered root must not accumulate behind it"
            );
        }

        registered.lock().unwrap().clear();
        for _ in 0..3 {
            db.begin_write().unwrap().commit().unwrap();
        }
        assert_eq!(
            db.external_snapshot_retention_stats().unwrap(),
            ExternalSnapshotRetentionStats::default()
        );
        let reclaimed = retained_pages
            .iter()
            .filter(|page| !mem.is_allocated(**page))
            .count();
        assert!(
            reclaimed > 0,
            "released external pages must enter the free inventory"
        );

        let mut reused = HashSet::new();
        for batch in 0..64u64 {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(REUSE).unwrap();
                for item in 0..64u64 {
                    let key = batch * 64 + item;
                    table
                        .insert(key, vec![(key % 239) as u8; 8192].as_slice())
                        .unwrap();
                }
            }
            write.commit().unwrap();
            reused.extend(retained_pages.iter().copied().filter(|page| {
                mem.is_allocated(*page)
                    && mem.get_page(*page, PageHint::None).unwrap().memory() != retained_bytes[page]
            }));
            if !reused.is_empty() {
                break;
            }
        }
        assert!(
            !reused.is_empty(),
            "ordinary allocation must actually reuse a released retained extent"
        );
    }

    #[test]
    fn external_root_validation_and_prewriter_registration_order_fail_closed() {
        let file = crate::create_tempfile();
        let db = Database::create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        write
            .open_table(X)
            .unwrap()
            .insert("root", "validated")
            .unwrap();
        write.commit().unwrap();
        drop(db);

        let read_db = crate::ReadOnlyDatabase::open(file.path()).unwrap();
        let read = read_db.begin_read().unwrap();
        let snapshot = read.storage_snapshot_identity();
        drop(read);
        drop(read_db);

        let mut forged = snapshot;
        forged.user_root.as_mut().unwrap().checksum ^= 1;
        let backend = FileBackend::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(file.path())
                .unwrap(),
        )
        .unwrap();
        let error = Database::builder()
            .open_with_backend_and_external_snapshots(backend, Arc::new(move || Ok(vec![forged])))
            .expect_err("a forged physical root must be refused before writer housekeeping");
        assert!(error.to_string().contains("invalid user-tree root"));

        let registered = Arc::new(Mutex::new(Vec::new()));
        let provider_state = registered.clone();
        let backend = FileBackend::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(file.path())
                .unwrap(),
        )
        .unwrap();
        let db = Database::builder()
            .open_with_backend_and_external_snapshots(
                backend,
                Arc::new(move || Ok(provider_state.lock().unwrap().clone())),
            )
            .unwrap();
        registered.lock().unwrap().push(snapshot);
        let Err(error) = db.begin_write() else {
            panic!("an unpinned root cannot register after writer admission completed");
        };
        assert!(
            error
                .to_string()
                .contains("new external snapshots require a live read pin after writer admission")
        );
        registered.lock().unwrap().clear();
        db.begin_write().unwrap().abort().unwrap();
    }
}

#[cfg(all(test, debug_assertions))]
mod online_relocation_tests {
    use super::*;
    use crate::{Database, ReadableDatabase, TableDefinition};

    thread_local! {
        pub(super) static RELOCATIONS: std::cell::RefCell<Vec<(PageNumber, PageNumber)>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    #[test]
    fn live_snapshot_survives_relocated_and_reused_pages() {
        const VALUES: TableDefinition<u64, &[u8]> = TableDefinition::new("values");
        const WRITES: TableDefinition<u64, &[u8]> = TableDefinition::new("recording");
        let file = crate::create_tempfile();
        let db = Database::create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        let mem = write.mem.clone();
        {
            let mut table = write.open_table(VALUES).unwrap();
            for key in 0..600u64 {
                table
                    .insert(key, vec![(key % 251) as u8; 8192].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let initially_allocated: HashSet<_> = mem.all_allocated_pages().into_iter().collect();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(VALUES).unwrap();
            for key in 0..500 {
                table.remove(key).unwrap();
            }
        }
        write.commit().unwrap();
        for _ in 0..3 {
            db.begin_write().unwrap().commit_compaction_frees().unwrap();
        }
        let snapshot = db.begin_read().unwrap();
        let old_table = snapshot.open_table(VALUES).unwrap();
        let pinned: HashSet<_> = mem.all_allocated_pages().into_iter().collect();
        let old_holes: HashSet<_> = initially_allocated.difference(&pinned).copied().collect();
        assert!(!old_holes.is_empty());
        let before: HashMap<_, _> = pinned
            .iter()
            .map(|page| {
                (
                    *page,
                    mem.get_page(*page, PageHint::None)
                        .unwrap()
                        .memory()
                        .to_vec(),
                )
            })
            .collect();
        RELOCATIONS.with(|pairs| pairs.borrow_mut().clear());
        let mut cursor = CompactionCursor::default();
        let mut relocated = 0;
        let mut completed = false;
        for _ in 0..256 {
            let progress = db
                .begin_write()
                .unwrap()
                .compact_step(&mut cursor, 16, |_| Ok(()))
                .unwrap();
            relocated += progress.pages_relocated;
            // Ordinary recording commits while the original read remains pinned.
            let write = db.begin_write().unwrap();
            write
                .open_table(WRITES)
                .unwrap()
                .insert(relocated as u64, &[42u8; 8192][..])
                .unwrap();
            write.commit().unwrap();
            for key in 500..600 {
                assert_eq!(
                    old_table.get(key).unwrap().unwrap().value(),
                    vec![(key % 251) as u8; 8192]
                );
            }
            if progress.complete {
                completed = true;
                break;
            }
        }
        assert!(completed && relocated > 0);
        let pairs = RELOCATIONS.with(|pairs| pairs.borrow().clone());
        let reused_holes = pairs
            .iter()
            .filter(|(_, to)| old_holes.contains(to))
            .count();
        assert!(
            reused_holes > 0,
            "relocation must actually reuse freed lower pages"
        );
        let old_sources: HashSet<_> = pairs
            .iter()
            .filter_map(|(from, _)| pinned.contains(from).then_some(*from))
            .collect();
        assert!(!old_sources.is_empty());
        for page in &old_sources {
            assert!(
                mem.is_allocated(*page),
                "live readers retain relocated source pages"
            );
            assert_eq!(
                mem.get_page(*page, PageHint::None).unwrap().memory(),
                before[page]
            );
        }
        drop(old_table);
        drop(snapshot);
        for _ in 0..3 {
            db.begin_write().unwrap().commit_compaction_frees().unwrap();
        }
        let reclaimed = old_sources
            .iter()
            .filter(|page| !mem.is_allocated(**page))
            .count();
        assert!(
            reclaimed > 0,
            "releasing the snapshot must make relocated source pages reusable"
        );
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(WRITES).unwrap();
            for key in 10000..10600 {
                table.insert(key, &[73u8; 8192][..]).unwrap();
            }
        }
        write.commit().unwrap();
        let source_reuse = old_sources
            .iter()
            .filter(|page| {
                mem.is_allocated(**page)
                    && mem.get_page(**page, PageHint::None).unwrap().memory() != before[page]
            })
            .count();
        assert!(
            source_reuse > 0,
            "recording must actually overwrite released relocation sources"
        );
        eprintln!(
            "relocated={relocated} reused_lower_pages={reused_holes} pinned_sources={} released_sources={reclaimed} reused_sources={source_reuse}",
            old_sources.len()
        );
        drop(mem);
        drop(db);
        let db = Database::open(file.path()).unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_table(VALUES).unwrap();
        for key in 500..600 {
            assert_eq!(
                table.get(key).unwrap().unwrap().value(),
                vec![(key % 251) as u8; 8192]
            );
        }
        assert_eq!(
            read.open_table(WRITES)
                .unwrap()
                .get(10599)
                .unwrap()
                .unwrap()
                .value(),
            &[73u8; 8192]
        );
    }
}

#[cfg(test)]
mod bounded_reclamation_tests {
    use super::*;
    use crate::{Database, ReadableDatabase};

    const SELECTED: TableDefinition<u64, u64> = TableDefinition::new("selected");
    const CHURN: TableDefinition<u64, &[u8]> = TableDefinition::new("churn");

    // Count both free-list entries and allocated extents (PageList items), not
    // physical pages. Empty entries and later cleanup work matter to the drain.
    fn pending(db: &Database, cutoff: Option<u64>) -> [(usize, usize); 2] {
        let write = db.begin_write().unwrap();
        let mut counts = [(0, 0); 2];
        {
            let mut tables = write.system_tables.lock().unwrap();
            for (index, definition) in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE]
                .into_iter()
                .enumerate()
            {
                let table = tables.open_system_table(&write, definition).unwrap();
                for entry in table.range::<TransactionIdWithPagination>(..).unwrap() {
                    let (key, pages) = entry.unwrap();
                    if cutoff.is_none_or(|cutoff| key.value().transaction_id <= cutoff) {
                        counts[index].0 += 1;
                        counts[index].1 += pages.value().len();
                    }
                }
            }
        }
        write.abort().unwrap();
        counts
    }

    fn pending_before(db: &Database, cutoff: u64) -> usize {
        pending(db, Some(cutoff))
            .iter()
            .map(|(_, pages)| pages)
            .sum()
    }

    #[test]
    fn idle_writer_drains_eligible_deferred_pages_with_bounded_commits() {
        let file = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_cache_size(0).set_deferred_free_budget(64, 64);
        let db = builder.create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(CHURN).unwrap();
            for key in 0..128 {
                table.insert(key, &[0; 8192][..]).unwrap();
            }
        }
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        for generation in 1..=4u8 {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(CHURN).unwrap();
                for key in 0..128 {
                    table.insert(key, &[generation; 8192][..]).unwrap();
                }
            }
            write.commit().unwrap();
        }
        assert!(
            pending(&db, None)
                .iter()
                .map(|(_, pages)| pages)
                .sum::<usize>()
                > 64
        );
        drop(read);
        db.drain_deferred_frees().unwrap();
        assert_eq!(pending(&db, None), [(0, 0); 2]);
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(CHURN)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            &[4; 8192]
        );
    }

    #[test]
    fn released_snapshot_cleanup_is_bounded_durable_and_eventually_drains() {
        let file = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_cache_size(0).set_deferred_free_budget(64, 64);
        let db = builder.create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        write.open_table(SELECTED).unwrap().insert(1, 7).unwrap();
        {
            let mut table = write.open_table(CHURN).unwrap();
            for key in 0..160 {
                table.insert(key, &[0; 8192][..]).unwrap();
            }
        }
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        for generation in 1..=8u8 {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(CHURN).unwrap();
                for key in 0..160 {
                    table.insert(key, &[generation; 8192][..]).unwrap();
                }
            }
            write.commit().unwrap();
        }
        assert_eq!(
            read.open_table(SELECTED)
                .unwrap()
                .get(1)
                .unwrap()
                .unwrap()
                .value(),
            7
        );
        let cutoff = db
            .get_memory()
            .get_last_committed_transaction_id()
            .unwrap()
            .raw_id();
        let backlog = pending_before(&db, cutoff);
        assert!(
            backlog > 1024,
            "held read must defer substantial unrelated churn"
        );
        assert_eq!(
            pending(&db, Some(cutoff))[1],
            (0, 0),
            "this fixture leaves only the data free tree eligible, so its unused system share can be lent"
        );
        drop(read);

        let write = db.begin_write().unwrap();
        write.open_table(SELECTED).unwrap().insert(2, 9).unwrap();
        write.commit().unwrap();
        let remaining = pending_before(&db, cutoff);
        assert!(remaining < backlog);
        assert!(
            backlog - remaining > 32,
            "the data tree must borrow the idle system tree's share"
        );
        assert!(
            backlog - remaining <= 64,
            "one small append cannot reclaim more than 64 allocated extents"
        );

        // Ordinary abort never runs the commit-only reclamation stage. Prove
        // both a user write and a COW edit of the durable continuation roll
        // back, without invoking commit-stage allocator frees before abort.
        let before_abort = pending(&db, None);
        let allocated_before_abort = db.get_memory().count_allocated_pages().unwrap();
        let write = db.begin_write().unwrap();
        write.open_table(SELECTED).unwrap().insert(3, 11).unwrap();
        let mut edited = 0;
        {
            let mut tables = write.system_tables.lock().unwrap();
            for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
                let mut table = tables.open_system_table(&write, definition).unwrap();
                if let Some(key) = table.tree.first().unwrap().map(|(key, _)| key.value()) {
                    let mut entry = table.tree.get_mut(&key).unwrap().unwrap();
                    if entry.value_mut().pop_back().is_some() {
                        edited += 1;
                    }
                }
            }
        }
        assert!(edited > 0, "exercise a partially trimmed continuation");
        write.abort().unwrap();
        assert_eq!(pending(&db, None), before_abort);
        assert_eq!(
            db.get_memory().count_allocated_pages().unwrap(),
            allocated_before_abort
        );
        assert!(
            db.begin_read()
                .unwrap()
                .open_table(SELECTED)
                .unwrap()
                .get(3)
                .unwrap()
                .is_none()
        );

        drop(db);
        let db = builder.open(file.path()).unwrap();
        let after_reopen = pending_before(&db, cutoff);
        assert!(
            after_reopen > 0,
            "bounded close/open housekeeping must preserve remaining work"
        );
        assert!(after_reopen <= remaining);
        for _ in 0..backlog {
            if pending(&db, None) == [(0, 0); 2] {
                break;
            }
            db.begin_write().unwrap().commit().unwrap();
        }
        assert_eq!(
            pending(&db, None),
            [(0, 0); 2],
            "later commits drain both entire free trees, including new housekeeping work"
        );
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(SELECTED)
                .unwrap()
                .get(2)
                .unwrap()
                .unwrap()
                .value(),
            9
        );
        assert_eq!(
            read.open_table(CHURN)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            &[8; 8192]
        );
    }

    #[test]
    fn empty_deferred_lists_consume_the_entry_budget() {
        let file = crate::create_tempfile();
        let db = Database::create(file.path()).unwrap();
        let mut write = db.begin_write().unwrap();
        write.set_deferred_free_budget(0, 0);
        {
            let mut tables = write.system_tables.lock().unwrap();
            for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
                let mut table = tables.open_system_table(&write, definition).unwrap();
                for pagination_id in 0..20 {
                    let key = TransactionIdWithPagination {
                        transaction_id: 0,
                        pagination_id,
                    };
                    table
                        .insert_reserve(&key, PageList::required_bytes(1))
                        .unwrap()
                        .as_mut()
                        .clear();
                }
            }
        }
        write.commit().unwrap();
        let mut write = db.begin_write().unwrap();
        write.set_deferred_free_budget(64, 5);
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        let mut remaining = 0;
        {
            let mut tables = write.system_tables.lock().unwrap();
            for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
                let table = tables.open_system_table(&write, definition).unwrap();
                let end = TransactionIdWithPagination {
                    transaction_id: 1,
                    pagination_id: 0,
                };
                for entry in table.range(..end).unwrap() {
                    assert_eq!(entry.unwrap().1.value().len(), 0);
                    remaining += 1;
                }
            }
        }
        write.abort().unwrap();
        assert_eq!(remaining, 35, "empty lists must not bypass the entry cap");
    }

    #[test]
    fn one_unit_budget_progresses_in_both_trees() {
        let file = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_deferred_free_budget(1, 1);
        let db = builder.create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        write.open_table(SELECTED).unwrap().insert(1, 0).unwrap();
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        for generation in 1..10u64 {
            let write = db.begin_write().unwrap();
            write
                .open_table(SELECTED)
                .unwrap()
                .insert(1, generation)
                .unwrap();
            write.commit().unwrap();
        }
        let cutoff = db
            .get_memory()
            .get_last_committed_transaction_id()
            .unwrap()
            .raw_id();
        drop(read);
        let mut previous = pending_before(&db, cutoff);
        assert!(previous > 1);
        for _ in 0..previous * 4 + 100 {
            db.begin_write().unwrap().commit().unwrap();
            let current = pending_before(&db, cutoff);
            assert!(previous - current <= 1);
            previous = current;
            if current == 0 {
                break;
            }
        }
        assert_eq!(previous, 0);
    }

    #[test]
    fn one_unit_budget_progresses_in_both_trees_across_repeated_reopen() {
        let file = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_deferred_free_budget(1, 1);
        let mut db = builder.create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        write.open_table(SELECTED).unwrap().insert(1, 0).unwrap();
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        for generation in 1..=12u64 {
            let mut write = db.begin_write().unwrap();
            write.set_quick_repair(true);
            write
                .open_table(SELECTED)
                .unwrap()
                .insert(1, generation)
                .unwrap();
            write.commit().unwrap();
        }
        let cutoff = db
            .get_memory()
            .get_last_committed_transaction_id()
            .unwrap()
            .raw_id();
        let initial = pending(&db, Some(cutoff));
        assert!(
            initial
                .iter()
                .all(|(entries, pages)| *entries > 0 && *pages > 0),
            "{initial:?}"
        );
        drop(read);
        let bound = initial
            .iter()
            .map(|(entries, pages)| entries + pages)
            .sum::<usize>()
            * 4
            + 100;
        for _ in 0..bound {
            // Each reopen is a new tracker. The aborted inspection also makes
            // transaction-ID parity unsuitable as a fairness mechanism.
            drop(db);
            db = builder.open(file.path()).unwrap();
            if pending(&db, Some(cutoff)) == [(0, 0); 2] {
                break;
            }
        }
        assert_eq!(pending(&db, Some(cutoff)), [(0, 0); 2]);
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(SELECTED)
                .unwrap()
                .get(1)
                .unwrap()
                .unwrap()
                .value(),
            12
        );
    }

    #[test]
    fn explicit_compaction_drains_backlog_with_a_finite_builder_budget() {
        let file = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_deferred_free_budget(1, 1);
        let mut db = builder.create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        write
            .open_table(CHURN)
            .unwrap()
            .insert(0, &[0; 8192][..])
            .unwrap();
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        for generation in 1..=16u8 {
            let mut write = db.begin_write().unwrap();
            write.set_quick_repair(true);
            write
                .open_table(CHURN)
                .unwrap()
                .insert(0, &[generation; 8192][..])
                .unwrap();
            write.commit().unwrap();
        }
        assert!(pending(&db, None).iter().all(|(_, pages)| *pages > 2));
        drop(read);
        db.compact().unwrap();
        assert_eq!(pending(&db, None), [(0, 0); 2]);
        assert_eq!(db.begin_write().unwrap().deferred_free_budget, (1, 1));
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(CHURN)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            &[16; 8192]
        );
        drop(db);
        let db = builder.open(file.path()).unwrap();
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(CHURN)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            &[16; 8192]
        );
    }
}

#[cfg(feature = "contextdb-internal")]
impl ReadTransaction {
    /// Internal exact lookup over THIS transaction's captured directory.
    /// Native qualification is restricted to clean misses; legacy cache cells
    /// are refused rather than retroactively assigned native receipt ownership.
    #[doc(hidden)]
    pub fn get_native_byte_value(
        &self,
        physical_name: &str,
        key: &[u8],
    ) -> std::result::Result<Option<AccessGuard<'static, &'static [u8]>>, crate::NativeColdReadError>
    {
        self.tree.get_native_byte_value(physical_name, key)
    }
}

#[cfg(feature = "contextdb-internal")]
impl ReadTransaction {
    /// Dependency-internal text-key sibling over THIS captured directory.
    /// Existing byte lookup and all ordinary/public table APIs are unchanged.
    #[doc(hidden)]
    pub fn get_native_text_value(
        &self,
        physical_name: &str,
        key: &str,
    ) -> std::result::Result<Option<AccessGuard<'static, &'static [u8]>>, crate::NativeColdReadError>
    {
        self.tree.get_native_text_value(physical_name, key)
    }
}

#[cfg(feature = "contextdb-internal")]
impl ReadTransaction {
    /// Hidden text-key/byte-value range sibling over THIS captured transaction.
    /// Returns the first key >= lower (or > lower), strictly below upper.
    /// Raw endpoint bytes support a non-UTF-8 exclusive prefix successor.
    #[doc(hidden)]
    pub fn next_native_text_range(
        &self,
        physical_name: &str,
        lower: &[u8],
        exclusive: bool,
        upper: Option<&[u8]>,
        before_page: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>,
    ) -> std::result::Result<
        Option<(
            AccessGuard<'static, &'static [u8]>,
            AccessGuard<'static, &'static [u8]>,
        )>,
        crate::NativeColdReadError,
    > {
        self.tree
            .next_native_text_range(physical_name, lower, exclusive, upper, before_page)
    }
}

#[cfg(feature = "contextdb-internal")]
impl ReadTransaction {
    /// Internal byte-key range over this captured transaction; exact raw bounds.
    #[doc(hidden)]
    pub fn next_native_byte_range(
        &self,
        physical_name: &str,
        lower: &[u8],
        exclusive: bool,
        upper: Option<&[u8]>,
        before_page: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>,
    ) -> std::result::Result<
        Option<(
            AccessGuard<'static, &'static [u8]>,
            AccessGuard<'static, &'static [u8]>,
        )>,
        crate::NativeColdReadError,
    > {
        self.tree
            .next_native_byte_range(physical_name, lower, exclusive, upper, before_page)
    }
}
