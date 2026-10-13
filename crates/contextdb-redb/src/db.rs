use crate::transaction_tracker::{TransactionId, TransactionTracker};
use crate::tree_store::{
    BtreeHeader, InternalTableDefinition, PAGE_SIZE, PageHint, PageNumber, ReadOnlyBackend,
    ShrinkPolicy, TableTree, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{
    CompactionError, DatabaseError, Error, ReadOnlyTable, SavepointError, StorageError, TableError,
};
use crate::{ReadTransaction, Result, WriteTransaction};
use std::fmt::{Debug, Display, Formatter};

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::{io, thread};

use crate::error::TransactionError;
use crate::sealed::Sealed;
use crate::transactions::{
    ALLOCATOR_STATE_TABLE_NAME, AllocatorStateKey, AllocatorStateTree, DATA_ALLOCATED_TABLE,
    DATA_FREED_TABLE, ExternalSnapshotProvider, PageList, SYSTEM_FREED_TABLE,
    SystemTableDefinition, TransactionIdWithPagination,
};
use crate::tree_store::file_backend::FileBackend;
#[cfg(feature = "logging")]
use log::{debug, info, warn};

#[cfg(feature = "test-seams")]
pub(crate) mod allocator_metadata_test_seams;

#[allow(clippy::len_without_is_empty)]
/// Implements persistent storage for a database.
pub trait StorageBackend: 'static + Debug + Send + Sync {
    /// Gets the current length of the storage.
    fn len(&self) -> std::result::Result<u64, io::Error>;

    /// Reads the specified array of bytes from the storage.
    ///
    /// If `out.len()` + `offset` exceeds the length of the storage an appropriate `Error` must be returned.
    fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), io::Error>;

    /// Sets the length of the storage.
    ///
    /// New positions in the storage must be initialized to zero.
    fn set_len(&self, len: u64) -> std::result::Result<(), io::Error>;

    /// Syncs all buffered data with the persistent storage.
    fn sync_data(&self) -> std::result::Result<(), io::Error>;

    /// Writes the specified array to the storage.
    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), io::Error>;

    /// Create non-durable allocator scratch storage beside this backend's file.
    /// Backends without a file may retain the in-memory allocator instead.
    fn scratch_backend(&self) -> std::result::Result<Option<Box<dyn ScratchBackend>>, io::Error> {
        Ok(None)
    }

    /// Release any resources held by the backend
    ///
    /// Note: redb will not access the backend after calling this method and will call it exactly
    /// once when the [`Database`] is dropped
    fn close(&self) -> std::result::Result<(), io::Error> {
        Ok(())
    }
}

/// Seekable, non-durable backing for allocator working state. Its contents are
/// discarded when the database closes or reopens; committed system tables stay
/// the durable authority.
#[allow(clippy::len_without_is_empty)]
pub trait ScratchBackend: Debug + Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()>;
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
}

#[derive(Debug)]
struct TemporaryScratch(std::sync::Mutex<File>);

impl ScratchBackend for TemporaryScratch {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.lock().unwrap().metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = self.0.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(out)
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let mut file = self.0.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.lock().unwrap().set_len(len)
    }
}

/// Exact requested heap bytes for the concrete boxed backend returned by
/// [`scratch_backend_in_directory`]. This does not create a file or allocate.
/// File contents and operating-system file resources are not heap payload bytes.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub fn scratch_backend_allocation_size() -> usize {
    std::mem::size_of::<TemporaryScratch>()
}

/// Create an allocator scratch file in the database file's directory.
/// The directory entry is removed immediately. An open handle keeps the
/// scratch usable, and process death cannot leave a named orphan behind.
pub fn scratch_backend_in_directory(directory: &Path) -> io::Result<Box<dyn ScratchBackend>> {
    Ok(Box::new(TemporaryScratch(std::sync::Mutex::new(
        tempfile::tempfile_in(directory)?,
    ))))
}

pub trait TableHandle: Sealed {
    // Returns the name of the table
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedTableHandle {
    name: String,
}

impl UntypedTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl TableHandle for UntypedTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Sealed for UntypedTableHandle {}

pub trait MultimapTableHandle: Sealed {
    // Returns the name of the multimap table
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedMultimapTableHandle {
    name: String,
}

impl UntypedMultimapTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl MultimapTableHandle for UntypedMultimapTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Sealed for UntypedMultimapTableHandle {}

/// Defines the name and types of a table
///
/// A [`TableDefinition`] should be opened for use by calling [`ReadTransaction::open_table`] or [`WriteTransaction::open_table`]
///
/// Note that the lifetime of the `K` and `V` type parameters does not impact the lifetimes of the data
/// that is stored or retreived from the table
pub struct TableDefinition<'a, K: Key + 'static, V: Value + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> TableDefinition<'a, K, V> {
    /// Construct a new table with given `name`
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty. When `name` is a non-empty string literal
    /// this is checked at compile time, but callers that build the name at
    /// runtime are responsible for ensuring it is non-empty.
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for TableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Value> Sealed for TableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Clone for TableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Value + 'static> Copy for TableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Display for TableDefinition<'_, K, V> {
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

/// Defines the name and types of a multimap table
///
/// A [`MultimapTableDefinition`] should be opened for use by calling [`ReadTransaction::open_multimap_table`] or [`WriteTransaction::open_multimap_table`]
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
///
/// Note that the lifetime of the `K` and `V` type parameters does not impact the lifetimes of the data
/// that is stored or retreived from the table
pub struct MultimapTableDefinition<'a, K: Key + 'static, V: Key + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Key + 'static> MultimapTableDefinition<'a, K, V> {
    /// Construct a new multimap table with given `name`
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty. When `name` is a non-empty string literal
    /// this is checked at compile time, but callers that build the name at
    /// runtime are responsible for ensuring it is non-empty.
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Key + 'static> MultimapTableHandle for MultimapTableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Key> Sealed for MultimapTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Key + 'static> Clone for MultimapTableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Key + 'static> Copy for MultimapTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Key + 'static> Display for MultimapTableDefinition<'_, K, V> {
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

/// Information regarding the usage of the in-memory cache
///
/// Note: these metrics are only collected when the "`cache_metrics`" feature is enabled
#[derive(Debug)]
pub struct CacheStats {
    pub(crate) evictions: u64,
    pub(crate) read_hits: u64,
    pub(crate) read_misses: u64,
    pub(crate) write_hits: u64,
    pub(crate) write_misses: u64,
    pub(crate) used_bytes: usize,
}

impl CacheStats {
    /// Number of times that data has been evicted, due to the cache being full
    ///
    /// To increase the cache size use [`Builder::set_cache_size`]
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Number of times that unmodified data has been read from the cache
    pub fn read_hits(&self) -> u64 {
        self.read_hits
    }

    /// Number of times that unmodified data was not in the cache and was read from storage
    pub fn read_misses(&self) -> u64 {
        self.read_misses
    }

    /// Number of times that data modified in a transaction has been read from the cache
    pub fn write_hits(&self) -> u64 {
        self.write_hits
    }

    /// Number of times that data modified in a transaction was not in the cache and was read from storage
    pub fn write_misses(&self) -> u64 {
        self.write_misses
    }

    /// Number of bytes in the cache
    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }
}

pub(crate) struct TransactionGuard {
    transaction_tracker: Option<Arc<TransactionTracker>>,
    transaction_id: Option<TransactionId>,
    write_transaction: bool,
}

impl TransactionGuard {
    pub(crate) fn new_read(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
    ) -> Self {
        Self {
            transaction_tracker: Some(tracker),
            transaction_id: Some(transaction_id),
            write_transaction: false,
        }
    }

    pub(crate) fn new_write(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
    ) -> Self {
        Self {
            transaction_tracker: Some(tracker),
            transaction_id: Some(transaction_id),
            write_transaction: true,
        }
    }

    // TODO: remove this hack
    pub(crate) fn fake() -> Self {
        Self {
            transaction_tracker: None,
            transaction_id: None,
            write_transaction: false,
        }
    }

    pub(crate) fn external(transaction_id: TransactionId) -> Self {
        Self {
            transaction_tracker: None,
            transaction_id: Some(transaction_id),
            write_transaction: false,
        }
    }

    pub(crate) fn id(&self) -> TransactionId {
        self.transaction_id.unwrap()
    }

    pub(crate) fn leak(mut self) -> TransactionId {
        self.transaction_id.take().unwrap()
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        if self.transaction_tracker.is_none() {
            return;
        }
        if let Some(transaction_id) = self.transaction_id {
            if self.write_transaction {
                self.transaction_tracker
                    .as_ref()
                    .unwrap()
                    .end_write_transaction(transaction_id);
            } else {
                self.transaction_tracker
                    .as_ref()
                    .unwrap()
                    .deallocate_read_transaction(transaction_id);
            }
        }
    }
}

pub trait ReadableDatabase {
    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError>;

    /// Information regarding the usage of the in-memory cache
    ///
    /// Note: these metrics are only collected when the "`cache_metrics`" feature is enabled
    fn cache_stats(&self) -> CacheStats;
}

/// A redb database opened in read-only mode
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`] object that can be used to read from the database
///
/// Multiple processes may open a [`ReadOnlyDatabase`], but it may not be opened concurrently
/// with a [`Database`].
///
/// # Examples
///
/// Basic usage:
///
/// ```rust
/// use redb::*;
/// # use tempfile::NamedTempFile;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename)?;
/// let txn = db.begin_write()?;
/// {
///     let mut table = txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// txn.commit()?;
/// drop(db);
///
/// let db = ReadOnlyDatabase::open(filename)?;
/// let txn = db.begin_read()?;
/// {
///     let mut table = txn.open_table(TABLE)?;
///     println!("{}", table.get(&0)?.unwrap().value());
/// }
/// # Ok(())
/// # }
/// ```
pub struct ReadOnlyDatabase {
    mem: Arc<TransactionalMemory>,
    transaction_tracker: Arc<TransactionTracker>,
}

impl ReadableDatabase for ReadOnlyDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;
        #[cfg(feature = "logging")]
        debug!("Beginning read transaction id={id:?}");

        let guard = TransactionGuard::new_read(id, self.transaction_tracker.clone());

        ReadTransaction::new(self.mem.clone(), guard)
    }

    fn cache_stats(&self) -> CacheStats {
        self.mem.cache_stats()
    }
}

impl ReadOnlyDatabase {
    /// Exact fixed cache-index allocation, separate from the configured page cap.
    #[doc(hidden)]
    pub fn cache_index_bytes(&self) -> usize {
        self.mem.cache_index_bytes()
    }

    /// Opens an existing redb database.
    pub fn open(path: impl AsRef<Path>) -> Result<ReadOnlyDatabase, DatabaseError> {
        Builder::new().open_read_only(path)
    }

    /// Begin a checksum-verifying read at the exact user root captured when
    /// this read-only database was established. The backend may be an open
    /// descriptor whose pathname has since been replaced; this method never
    /// consults that pathname or historical system-tree pages.
    #[doc(hidden)]
    pub fn begin_retained_read(
        &self,
        snapshot: crate::StorageSnapshotIdentity,
    ) -> Result<ReadTransaction, TransactionError> {
        let current = self.begin_read()?;
        let actual = current.storage_snapshot_identity();
        drop(current);
        if actual != snapshot {
            return Err(StorageError::Corrupted(
                "retained read-only source does not match the requested snapshot root".to_owned(),
            )
            .into());
        }
        ReadTransaction::new_external(self.mem.clone(), snapshot)
    }

    /// Begin a checksum-verifying read at a root whose lifetime the caller
    /// protects through an authenticated external snapshot registration.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn begin_external_read(
        &self,
        snapshot: crate::StorageSnapshotIdentity,
    ) -> Result<ReadTransaction, TransactionError> {
        ReadTransaction::new_external(self.mem.clone(), snapshot)
    }

    fn new(
        file: Box<dyn StorageBackend>,
        page_size: usize,
        region_size: Option<u64>,
        cache_size: usize,
    ) -> Result<Self, DatabaseError> {
        Self::new_inner(file, page_size, region_size, cache_size, false)
    }

    fn new_inner(
        file: Box<dyn StorageBackend>,
        page_size: usize,
        region_size: Option<u64>,
        cache_size: usize,
        external_read: bool,
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database in read-only {:?}", &file_path);
        let mem = TransactionalMemory::new(
            Box::new(ReadOnlyBackend::new(file)),
            false,
            page_size,
            region_size,
            cache_size,
            true,
            external_read,
        )?;
        let mem = Arc::new(mem);
        // External readers use a registered user root. Allocator state may
        // belong to a concurrently committing writer and is never consulted.
        if !external_read {
            if let Some(tree) = Database::get_allocator_state_table(&mem)? {
                mem.load_allocator_state(&tree)?;
            } else {
                #[cfg(feature = "logging")]
                warn!(
                    "Database {:?} not shutdown cleanly. Repair required",
                    &file_path
                );
                return Err(DatabaseError::RepairAborted);
            }
        }
        // This handle owns only its local read-transaction tracking.
        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();
        let db = Self {
            mem,
            transaction_tracker: Arc::new(TransactionTracker::new(next_transaction_id)),
        };

        Ok(db)
    }
}

/// Opened redb database file
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`] object that can be used to read from the database
/// Use [`Self::begin_write`] to get a [`WriteTransaction`] object that can be used to read or write to the database
///
/// Multiple reads may be performed concurrently, with each other, and with writes. Only a single write
/// may be in progress at a time.
///
/// # Examples
///
/// Basic usage:
///
/// ```rust
/// use redb::*;
/// # use tempfile::NamedTempFile;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename)?;
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// write_txn.commit()?;
/// # Ok(())
/// # }
/// ```
pub struct Database {
    mem: Arc<TransactionalMemory>,
    transaction_tracker: Arc<TransactionTracker>,
    read_only_handoff_prepared: bool,
    #[cfg(feature = "test-seams")]
    deferred_free_commit_io_fault: std::sync::atomic::AtomicBool,
}

impl ReadableDatabase for Database {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let guard = self.allocate_read_transaction()?;
        #[cfg(feature = "logging")]
        debug!("Beginning read transaction id={:?}", guard.id());
        ReadTransaction::new(self.get_memory(), guard)
    }

    fn cache_stats(&self) -> CacheStats {
        self.mem.cache_stats()
    }
}

impl Database {
    /// Capture a native retained read that authenticates every consumed page.
    /// The ordinary native transaction guard remains responsible for retention.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn begin_verified_read(&self) -> Result<ReadTransaction, TransactionError> {
        ReadTransaction::new_verified(self.get_memory(), self.allocate_read_transaction()?)
    }

    /// Exact fixed cache-index allocation, separate from the configured page cap.
    #[doc(hidden)]
    pub fn cache_index_bytes(&self) -> usize {
        self.mem.cache_index_bytes()
    }

    /// The constant per-stripe lock controls within `cache_index_bytes`.
    #[doc(hidden)]
    pub fn cache_stripe_control_bytes(&self) -> usize {
        self.mem.cache_stripe_control_bytes()
    }

    /// Settle a writer cache share before exposing its first reader root.
    /// The caller must exclude concurrent database operations during startup.
    /// The returned reserve covers the capacity and any still-held pages.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn settle_startup_cache_budget(&self, bytes: usize) -> Result<usize, StorageError> {
        self.mem.settle_startup_cache_budget(bytes)
    }

    /// Upper bound on the allocator's resident heap owner. Paged file
    /// backends have a fixed envelope regardless of durable region count.
    pub fn allocator_resident_bytes(&self) -> usize {
        self.mem.allocator_resident_bytes()
    }

    /// Return `(loaded_regions, total_regions)` for allocator state.
    pub fn allocator_region_counts(&self) -> (usize, usize) {
        self.mem.allocator_region_counts()
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub fn create(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Self::builder().create(path)
    }

    /// Opens an existing redb database.
    pub fn open(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Self::builder().open(path)
    }

    pub(crate) fn get_memory(&self) -> Arc<TransactionalMemory> {
        self.mem.clone()
    }

    pub(crate) fn verify_primary_checksums(mem: Arc<TransactionalMemory>) -> Result<bool> {
        let table_tree = TableTree::new(
            mem.get_data_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        if !table_tree.verify_checksums()? {
            return Ok(false);
        }
        let system_table_tree = TableTree::new(
            mem.get_system_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        if !system_table_tree.verify_checksums()? {
            return Ok(false);
        }

        Ok(true)
    }

    /// Force a check of the integrity of the database file, and repair it if possible.
    ///
    /// Note: Calling this function is unnecessary during normal operation. redb will automatically
    /// detect and recover from crashes, power loss, and other unclean shutdowns. This function is
    /// quite slow and should only be used when you suspect the database file may have been modified
    /// externally to redb, or that a redb bug may have left the database in a corrupted state.
    ///
    /// Returns `Ok(true)` if the database passed integrity checks; `Ok(false)` if it failed but was repaired,
    /// and `Err(Corrupted)` if the check failed and the file could not be repaired.
    ///
    /// Returns [`DatabaseError::TransactionInProgress`] if any read or write transaction is still
    /// alive when this method is called.
    pub fn check_integrity(&mut self) -> Result<bool, DatabaseError> {
        if Arc::strong_count(&self.mem) != 1 {
            return Err(DatabaseError::TransactionInProgress);
        }
        // The lazy allocator source holds a weak reference to this memory.
        // Materialize it before Arc::get_mut, which requires no weak owners.
        self.mem.materialize_allocator_source()?;
        let mem = Arc::get_mut(&mut self.mem).ok_or(DatabaseError::TransactionInProgress)?;
        let mut was_clean = mem.clear_cache_and_reload()?;
        // The committed header may be smaller than a free tail left by an
        // aborted write. Compare allocator states at committed geometry.
        let allocator_hash = self.mem.allocator_hash();

        let old_roots = [self.mem.get_data_root(), self.mem.get_system_root()];

        let new_roots = Self::do_repair(&mut self.mem, &|_| {}).map_err(|err| match err {
            DatabaseError::Storage(storage_err) => storage_err,
            _ => unreachable!(),
        })?;

        if old_roots != new_roots || allocator_hash != self.mem.allocator_hash() {
            was_clean = false;
        }

        if !was_clean {
            let next_transaction_id = self.mem.get_last_committed_transaction_id()?.next();
            let [data_root, system_root] = new_roots;
            self.mem.commit(
                data_root,
                system_root,
                next_transaction_id,
                true,
                ShrinkPolicy::Never,
            )?;
        }

        self.mem.begin_writable()?;

        Ok(was_clean)
    }

    /// Compacts the database file
    ///
    /// Returns `true` if compaction was performed, and `false` if no futher compaction was possible
    pub fn compact(&mut self) -> Result<bool, CompactionError> {
        if self
            .transaction_tracker
            .oldest_live_read_transaction()
            .is_some()
        {
            return Err(CompactionError::TransactionInProgress);
        }
        // Commit to free up any pending free pages
        // Use 2-phase commit to avoid any possible security issues. Plus this compaction is going to be so slow that it doesn't matter.
        // Once https://github.com/cberner/redb/issues/829 is fixed, we should upgrade this to use quick-repair -- that way the user
        // can cancel the compaction without requiring a full repair afterwards
        let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        txn.reclaim_all_deferred_pages();
        if txn.list_persistent_savepoints()?.next().is_some() {
            return Err(CompactionError::PersistentSavepointExists);
        }
        if self.transaction_tracker.any_savepoint_exists() {
            return Err(CompactionError::EphemeralSavepointExists);
        }
        txn.set_two_phase_commit(true);
        txn.commit().map_err(|e| e.into_storage_error())?;
        // Repeat, just in case executing list_persistent_savepoints() created a new table
        let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        txn.reclaim_all_deferred_pages();
        txn.set_two_phase_commit(true);
        txn.commit().map_err(|e| e.into_storage_error())?;
        // There can't be any outstanding transactions because we have a `&mut self`, so all pending free pages
        // should have been cleared out by the above commit()
        let txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        assert!(!txn.pending_free_pages()?);
        txn.abort()?;

        let mut compacted = false;
        // Iteratively compact until no progress is made
        loop {
            let mut progress = false;

            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            txn.reclaim_all_deferred_pages();
            if txn.compact_pages()? {
                progress = true;
                txn.commit().map_err(|e| e.into_storage_error())?;
            } else {
                txn.abort()?;
            }

            // Double commit to free up the relocated pages for reuse
            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            txn.reclaim_all_deferred_pages();
            txn.set_two_phase_commit(true);
            // Also shrink the database file by the maximum amount
            txn.set_shrink_policy(ShrinkPolicy::Maximum);
            txn.commit().map_err(|e| e.into_storage_error())?;
            // Triple commit to free up the relocated pages for reuse
            // TODO: this really shouldn't be necessary, but the data freed tree is a system table
            // and so free'ing up its pages causes more deletes from the system tree
            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            txn.reclaim_all_deferred_pages();
            txn.set_two_phase_commit(true);
            // Also shrink the database file by the maximum amount
            txn.set_shrink_policy(ShrinkPolicy::Maximum);
            txn.commit().map_err(|e| e.into_storage_error())?;
            let txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            assert!(!txn.pending_free_pages()?);
            txn.abort()?;

            if !progress {
                break;
            }

            compacted = true;
        }

        Ok(compacted)
    }

    /// Relocate a bounded page selection using copy-on-write publication.
    /// Existing read snapshots keep their pages until their guards end.
    #[doc(hidden)]
    pub fn compact_step(
        &self,
        cursor: &mut crate::CompactionCursor,
        maximum: usize,
        admit: impl FnMut(usize) -> io::Result<()>,
    ) -> Result<crate::CompactionProgress, CompactionError> {
        let progress = self
            .begin_write()
            .map_err(|e| e.into_storage_error())?
            .compact_step(cursor, maximum, admit)?;
        for _ in 0..2 {
            self.begin_write()
                .map_err(|e| e.into_storage_error())?
                .commit_compaction_frees()
                .map_err(|e| e.into_storage_error())?;
        }
        Ok(progress)
    }

    /// Observe actual user-table and catalog writes for this database instance only.
    /// The guard restores the previous observer, including during unwinding.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn observe_table_writes_for_test(
        &self,
        observer: crate::TableWriteObserverForTest,
    ) -> crate::TableWriteObservationGuardForTest {
        crate::table_write_observer::install(
            self.transaction_tracker.table_write_observer.clone(),
            observer,
        )
    }

    #[doc(hidden)]
    pub fn has_live_read_transactions(&self) -> bool {
        self.transaction_tracker.has_native_read_transactions()
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn write_wait_observer(&self) -> impl Fn() -> bool + Send + Sync + 'static + use<> {
        let tracker = self.transaction_tracker.clone();
        move || tracker.has_waiting_write_transactions()
    }

    #[doc(hidden)]
    pub fn storage_statistics_snapshot(&self) -> Result<crate::StorageStatisticsSnapshot> {
        let write = self.begin_write().map_err(|e| e.into_storage_error())?;
        let read = self.begin_read().map_err(|e| e.into_storage_error())?;
        let snapshot = write.storage_statistics_snapshot(read)?;
        write.abort()?;
        Ok(snapshot)
    }

    /// Inspect the registered roots and their deferred pages on demand.
    /// The transaction aborts without publishing any mutation; this full
    /// diagnostic walk is never part of cursor or writer admission.
    #[doc(hidden)]
    pub fn external_snapshot_retention_stats(
        &self,
    ) -> Result<crate::ExternalSnapshotRetentionStats> {
        let write = self
            .begin_write()
            .map_err(|error| error.into_storage_error())?;
        let stats = write.external_snapshot_retention_stats()?;
        write.abort()?;
        Ok(stats)
    }

    /// Count every durable deferred-free entry and extent, including empty
    /// entries and later housekeeping work. This diagnostic neither reclaims
    /// pages nor commits; the writer guard keeps both system roots stable.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn deferred_free_backlog_for_test(&self) -> Result<[(usize, usize); 2]> {
        let write = self
            .begin_write()
            .map_err(|error| error.into_storage_error())?;
        let system_tree = TableTree::new(
            self.mem.get_system_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            self.mem.clone(),
        )?;
        let mut counts = [(0, 0); 2];
        for (index, definition) in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE]
            .into_iter()
            .enumerate()
        {
            let table_name = definition.name();
            let Some(definition) = system_tree
                .get_table::<TransactionIdWithPagination, PageList>(table_name, TableType::Normal)
                .map_err(|error| {
                    error.into_storage_error_or_corrupted("Deferred free table corrupted")
                })?
            else {
                continue;
            };
            let InternalTableDefinition::Normal { table_root, .. } = definition else {
                unreachable!()
            };
            let table: ReadOnlyTable<TransactionIdWithPagination, PageList> = ReadOnlyTable::new(
                table_name.to_string(),
                table_root,
                PageHint::None,
                Arc::new(TransactionGuard::fake()),
                self.mem.clone(),
            )?;
            for entry in table.range::<TransactionIdWithPagination>(..)? {
                let (_, pages) = entry?;
                counts[index].0 += 1;
                counts[index].1 += pages.value().len();
            }
        }
        drop(system_tree);
        write.abort()?;
        Ok(counts)
    }

    /// Install the engine's process-local exact-root leases. Newly supplied
    /// roots require a native pin, like authenticated external registrations.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn install_local_snapshot_provider(&self, provider: ExternalSnapshotProvider) {
        self.transaction_tracker
            .install_local_snapshot_provider(provider);
    }

    /// Begin a read at one root currently supplied by the authenticated
    /// external snapshot provider. This does not register another lifetime;
    /// the provider's owner remains the sole authority for retention.
    #[doc(hidden)]
    pub fn begin_external_read(
        &self,
        snapshot: crate::StorageSnapshotIdentity,
    ) -> Result<ReadTransaction, TransactionError> {
        self.transaction_tracker
            .refresh_external_snapshots(&self.mem)?;
        self.begin_admitted_external_read(snapshot)
    }

    /// Begin an external read without waiting for another provider refresh.
    /// A busy refresh returns an I/O error with `WouldBlock`. The provider
    /// itself remains responsible for nonblocking coordination when required.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn begin_external_read_nonblocking(
        &self,
        snapshot: crate::StorageSnapshotIdentity,
    ) -> Result<ReadTransaction, TransactionError> {
        self.transaction_tracker
            .try_refresh_external_snapshots(&self.mem)?;
        self.begin_admitted_external_read(snapshot)
    }

    /// Whether nonblocking admission met an occupied provider-refresh mutex.
    /// Provider I/O failures, including their own `WouldBlock`, are distinct.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn is_external_snapshot_refresh_busy(error: &TransactionError) -> bool {
        matches!(error,
            TransactionError::Storage(StorageError::Io(error))
                if error.get_ref().is_some_and(|cause|
                    cause.is::<crate::transaction_tracker::ExternalSnapshotRefreshBusy>())
        )
    }

    fn begin_admitted_external_read(
        &self,
        snapshot: crate::StorageSnapshotIdentity,
    ) -> Result<ReadTransaction, TransactionError> {
        if !self.transaction_tracker.has_external_snapshot(snapshot) {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "external snapshot is no longer registered",
            ))
            .into());
        }
        ReadTransaction::new_external(self.mem.clone(), snapshot)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_external_snapshot_refresh_lock_for_test<T>(
        &self,
        operation: impl FnOnce() -> T,
    ) -> T {
        self.transaction_tracker
            .with_external_snapshot_refresh_lock_for_test(operation)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn external_snapshot_retained_pages_for_test(&self) -> Result<Vec<[u8; 8]>> {
        let write = self
            .begin_write()
            .map_err(|error| error.into_storage_error())?;
        let pages = write.external_snapshot_retained_pages_for_test()?;
        write.abort()?;
        Ok(pages)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn external_snapshot_page_allocated_for_test(&self, page: [u8; 8]) -> bool {
        self.mem
            .is_allocated(crate::tree_store::PageNumber::from_le_bytes(page))
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    fn check_repaired_allocated_pages_table(
        system_root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
    ) -> Result {
        let table_tree = TableTree::new(
            system_root,
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        if let Some(table_def) = table_tree
            .get_table::<TransactionIdWithPagination, PageList>(
                DATA_ALLOCATED_TABLE.name(),
                TableType::Normal,
            )
            .map_err(|e| e.into_storage_error_or_corrupted("Allocated pages table corrupted"))?
        {
            let InternalTableDefinition::Normal { table_root, .. } = table_def else {
                unreachable!()
            };
            let table: ReadOnlyTable<TransactionIdWithPagination, PageList> = ReadOnlyTable::new(
                DATA_ALLOCATED_TABLE.name().to_string(),
                table_root,
                PageHint::None,
                Arc::new(TransactionGuard::fake()),
                mem.clone(),
            )?;
            for result in table.range::<TransactionIdWithPagination>(..)? {
                let (_, pages) = result?;
                for i in 0..pages.value().len() {
                    assert!(mem.is_allocated(pages.value().get(i)));
                }
            }
        }

        Ok(())
    }

    fn visit_freed_tree<K: Key, V: Value, F>(
        system_root: Option<BtreeHeader>,
        table_def: SystemTableDefinition<K, V>,
        mem: Arc<TransactionalMemory>,
        mut visitor: F,
    ) -> Result
    where
        F: FnMut(PageNumber) -> Result,
    {
        let fake_guard = Arc::new(TransactionGuard::fake());
        let system_tree = TableTree::new(system_root, PageHint::None, fake_guard, mem.clone())?;
        let table_name = table_def.name();
        let result = match system_tree.get_table::<K, V>(table_name, TableType::Normal) {
            Ok(result) => result,
            Err(TableError::Storage(err)) => {
                return Err(err);
            }
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(());
            }
            Err(_) => {
                return Err(StorageError::Corrupted(format!(
                    "Unable to open {table_name}"
                )));
            }
        };

        if let Some(definition) = result {
            let table_root = match definition {
                InternalTableDefinition::Normal { table_root, .. } => table_root,
                InternalTableDefinition::Multimap { .. } => unreachable!(),
            };
            let table: ReadOnlyTable<TransactionIdWithPagination, PageList<'static>> =
                ReadOnlyTable::new(
                    table_name.to_string(),
                    table_root,
                    PageHint::None,
                    Arc::new(TransactionGuard::fake()),
                    mem.clone(),
                )?;
            for result in table.range::<TransactionIdWithPagination>(..)? {
                let (_, page_list) = result?;
                for i in 0..page_list.value().len() {
                    visitor(page_list.value().get(i))?;
                }
            }
        }

        Ok(())
    }

    #[cfg(debug_assertions)]
    fn mark_allocated_page_for_debug(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
    ) -> Result {
        let data_root = mem.get_data_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let tables = TableTree::new(data_root, PageHint::None, fake, mem.clone())?;
            tables.visit_all_pages(|path| {
                mem.mark_debug_allocated_page(path.page_number());
                Ok(())
            })?;
        }

        let system_root = mem.get_system_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let system_tables = TableTree::new(system_root, PageHint::None, fake, mem.clone())?;
            system_tables.visit_all_pages(|path| {
                mem.mark_debug_allocated_page(path.page_number());
                Ok(())
            })?;
        }

        Self::visit_freed_tree(system_root, DATA_FREED_TABLE, mem.clone(), |page| {
            mem.mark_debug_allocated_page(page);
            Ok(())
        })?;
        Self::visit_freed_tree(system_root, SYSTEM_FREED_TABLE, mem.clone(), |page| {
            mem.mark_debug_allocated_page(page);
            Ok(())
        })?;

        Ok(())
    }

    fn do_repair(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<[Option<BtreeHeader>; 2], DatabaseError> {
        if !Self::verify_primary_checksums(mem.clone())? {
            if mem.used_two_phase_commit() {
                return Err(DatabaseError::Storage(StorageError::Corrupted(
                    "Primary is corrupted despite 2-phase commit".to_string(),
                )));
            }

            // 0.3 because the repair takes 3 full scans and the first is done now
            let mut handle = RepairSession::new(0.3);
            repair_callback(&mut handle);
            if handle.aborted() {
                return Err(DatabaseError::RepairAborted);
            }

            mem.repair_primary_corrupted();
            // We need to invalidate the userspace cache, because walking the tree in verify_primary_checksums() may
            // have poisoned it with pages that just got rolled back by repair_primary_corrupted(), since
            // that rolls back a partially committed transaction.
            mem.clear_read_cache();
            if !Self::verify_primary_checksums(mem.clone())? {
                return Err(DatabaseError::Storage(StorageError::Corrupted(
                    "Failed to repair database. All roots are corrupted".to_string(),
                )));
            }
        }
        // 0.6 because the repair takes 3 full scans and the second is done now
        let mut handle = RepairSession::new(0.6);
        repair_callback(&mut handle);
        if handle.aborted() {
            return Err(DatabaseError::RepairAborted);
        }

        mem.begin_repair()?;

        let data_root = mem.get_data_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let tables = TableTree::new(data_root, PageHint::None, fake, mem.clone())?;
            tables.visit_all_pages(|path| {
                mem.mark_page_allocated(path.page_number());
                Ok(())
            })?;
        }

        // 0.9 because the repair takes 3 full scans and the third is done now. There is just some system tables left
        let mut handle = RepairSession::new(0.9);
        repair_callback(&mut handle);
        if handle.aborted() {
            return Err(DatabaseError::RepairAborted);
        }

        let system_root = mem.get_system_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let system_tables = TableTree::new(system_root, PageHint::None, fake, mem.clone())?;
            system_tables.visit_all_pages(|path| {
                mem.mark_page_allocated(path.page_number());
                Ok(())
            })?;
        }

        Self::visit_freed_tree(system_root, DATA_FREED_TABLE, mem.clone(), |page| {
            mem.mark_page_allocated(page);
            Ok(())
        })?;
        Self::visit_freed_tree(system_root, SYSTEM_FREED_TABLE, mem.clone(), |page| {
            mem.mark_page_allocated(page);
            Ok(())
        })?;
        #[cfg(debug_assertions)]
        {
            Self::check_repaired_allocated_pages_table(system_root, mem.clone())?;
        }

        mem.end_repair()?;

        // We need to invalidate the userspace cache, because we're about to implicitly free the freed table
        // by storing an empty root during the below commit()
        mem.clear_read_cache();

        Ok([data_root, system_root])
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        file: Box<dyn StorageBackend>,
        allow_initialize: bool,
        page_size: usize,
        region_size: Option<u64>,
        cache_size: usize,
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
        external_snapshot_provider: Option<ExternalSnapshotProvider>,
        deferred_free_budget: (usize, usize),
        keep_pace_reclaim: bool,
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database {:?}", &file_path);
        let mem = TransactionalMemory::new(
            file,
            allow_initialize,
            page_size,
            region_size,
            cache_size,
            false,
            false,
        )?;
        let mut mem = Arc::new(mem);
        // If the last transaction used 2-phase commit and updated the allocator state table, then
        // we can just load the allocator state from there. Otherwise, we need a full repair
        if let Some(tree) = Self::get_allocator_state_table(&mem)? {
            #[cfg(feature = "logging")]
            info!("Found valid allocator state, full repair not needed");
            mem.load_allocator_state(&tree)?;
            #[cfg(debug_assertions)]
            // Unit tests exercise the legacy exact-extent debug inventory.
            // Normal file-backed writers keep that inventory empty so it
            // cannot grow with the durable page count.
            if cfg!(test) || !mem.uses_paged_allocator() {
                Self::mark_allocated_page_for_debug(&mut mem)?;
            }
        } else {
            #[cfg(feature = "logging")]
            warn!("Database {:?} not shutdown cleanly. Repairing", &file_path);
            let mut handle = RepairSession::new(0.0);
            repair_callback(&mut handle);
            if handle.aborted() {
                return Err(DatabaseError::RepairAborted);
            }
            let [data_root, system_root] = Self::do_repair(&mut mem, repair_callback)?;
            let next_transaction_id = mem.get_last_committed_transaction_id()?.next();
            mem.commit(
                data_root,
                system_root,
                next_transaction_id,
                true,
                ShrinkPolicy::Never,
            )?;
        }

        mem.begin_writable()?;
        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();

        let db = Database {
            mem,
            transaction_tracker: Arc::new(TransactionTracker::new_with_external_snapshots(
                next_transaction_id,
                external_snapshot_provider,
                deferred_free_budget,
                keep_pace_reclaim,
            )),
            read_only_handoff_prepared: false,
            #[cfg(feature = "test-seams")]
            deferred_free_commit_io_fault: std::sync::atomic::AtomicBool::new(false),
        };

        // Repair reconstructs current and deferred allocation state without
        // reusing pages. Authenticate every external root before the first
        // housekeeping transaction can reclaim or allocate anything.
        db.transaction_tracker.refresh_external_snapshots(&db.mem)?;

        // Restore the tracker state for any persistent savepoints
        let txn = db.begin_write().map_err(|e| e.into_storage_error())?;
        if let Some(next_id) = txn.next_persistent_savepoint_id()? {
            db.transaction_tracker
                .restore_savepoint_counter_state(next_id);
        }
        for id in txn.list_persistent_savepoints()? {
            let savepoint = match txn.get_persistent_savepoint(id) {
                Ok(savepoint) => savepoint,
                Err(err) => match err {
                    SavepointError::InvalidSavepoint
                    | SavepointError::ImmediateDurabilityRequired => unreachable!(),
                    SavepointError::Storage(storage) => {
                        return Err(storage.into());
                    }
                },
            };
            db.transaction_tracker
                .register_persistent_savepoint(&savepoint);
        }
        txn.abort()?;

        Ok(db)
    }

    fn get_allocator_state_table(
        mem: &Arc<TransactionalMemory>,
    ) -> Result<Option<AllocatorStateTree>> {
        // The allocator state table is only valid if the primary was written using 2-phase commit
        if !mem.used_two_phase_commit() {
            return Ok(None);
        }

        // See if it's present in the system table tree
        let system_table_tree = TableTree::new(
            mem.get_system_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        let Some(allocator_state_table) = system_table_tree
            .get_table::<AllocatorStateKey, &[u8]>(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?
        else {
            return Ok(None);
        };

        // Load the allocator state table
        let InternalTableDefinition::Normal { table_root, .. } = allocator_state_table else {
            unreachable!();
        };
        let tree = AllocatorStateTree::new(
            table_root,
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;

        // Make sure this isn't stale allocator state left over from a previous transaction
        if !mem.is_valid_allocator_state(&tree)? {
            return Ok(None);
        }

        Ok(Some(tree))
    }

    fn allocate_read_transaction(&self) -> Result<TransactionGuard> {
        let id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;

        Ok(TransactionGuard::new_read(
            id,
            self.transaction_tracker.clone(),
        ))
    }

    /// Convenience method for [`Builder::new`]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a single
    /// write may be in progress at a time. If a write is in progress, this function will block
    /// until it completes.
    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        if self.read_only_handoff_prepared {
            debug_assert!(
                self.mem.read_only_handoff_is_clean(),
                "a quiesced writer must retain a clean committed header"
            );
            return Err(TransactionError::Storage(StorageError::DatabaseClosed));
        }
        #[cfg(feature = "contextdb-internal")]
        self.mem.check_publication_recovery()?;
        // Fail early if there has been an I/O error -- nothing can be committed in that case
        self.mem.check_io_errors()?;
        let guard = TransactionGuard::new_write(
            self.transaction_tracker.start_write_transaction(),
            self.transaction_tracker.clone(),
        );
        // A writer that waited for the previous transaction must check again
        // after taking exclusive ownership and before touching the live header.
        #[cfg(feature = "contextdb-internal")]
        self.mem.check_publication_recovery()?;
        self.transaction_tracker
            .refresh_external_snapshots(&self.mem)?;
        WriteTransaction::new(guard, self.transaction_tracker.clone(), self.mem.clone())
            .map_err(|e| e.into())
    }

    fn ensure_allocator_state_table_and_trim(&self) -> Result<(), Error> {
        // Make a new quick-repair commit to update the allocator state table
        #[cfg(feature = "logging")]
        debug!("Writing allocator state table");
        let mut tx = self.begin_write()?;
        tx.set_quick_repair(true);
        tx.set_shrink_policy(ShrinkPolicy::Maximum);
        tx.commit()?;

        Ok(())
    }

    /// Settle eligible deferred frees through ordinary bounded commits before
    /// a caller closes an idle writer. Each commit retains the builder's
    /// extent and entry limits; a live snapshot leaves its protected backlog
    /// for a later writer instead of making shutdown wait for that reader.
    #[doc(hidden)]
    pub fn drain_deferred_frees(&self) -> Result<(), Error> {
        loop {
            let mut tx = self.begin_write()?;
            if self.transaction_tracker.any_savepoint_exists()
                || tx.list_persistent_savepoints()?.next().is_some()
                || !tx.eligible_pending_free_pages()?
            {
                tx.abort()?;
                return Ok(());
            }
            tx.set_shrink_policy(ShrinkPolicy::Maximum);
            #[cfg(feature = "test-seams")]
            if self
                .deferred_free_commit_io_fault
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                tx.abort()?;
                return Err(Error::Io(io::Error::other(
                    "injected deferred-free commit I/O failure",
                )));
            }
            tx.commit()?;
        }
    }

    /// Whether another writer is waiting for the write slot.
    #[doc(hidden)]
    pub fn has_waiting_write_transactions(&self) -> bool {
        self.transaction_tracker.has_waiting_write_transactions()
    }

    /// One bounded maintenance reclaim step: an ordinary durable write transaction that frees at
    /// most `extents` allocation extents and visits at most `entries` deferred-free entries, all
    /// freed by transactions before both the reader floor and `boundary`.
    ///
    /// The first step of a cycle passes `None` and samples the boundary as its own write
    /// transaction id, so every entry freed by an already committed transaction — including the
    /// last one — is included, and every later commit's frees, including these steps' own
    /// bookkeeping, are not. Later steps of the cycle pass the returned boundary back. Savepoints
    /// block reclamation exactly as in `drain_deferred_frees`.
    #[cfg(any(test, feature = "contextdb-internal"))]
    #[doc(hidden)]
    pub fn reclaim_deferred_step(
        &self,
        boundary: Option<ReclaimBoundary>,
        extents: usize,
        entries: usize,
    ) -> Result<ReclaimStep, Error> {
        let mut tx = self.begin_write()?;
        if self.transaction_tracker.any_savepoint_exists()
            || tx.list_persistent_savepoints()?.next().is_some()
        {
            tx.abort()?;
            return Ok(ReclaimStep::Blocked);
        }
        let boundary = boundary.map_or_else(|| tx.transaction_id(), |boundary| boundary.0);
        let floor = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(tx.transaction_id(), |id| id.next());
        if !tx.pending_free_pages_before(TransactionId::min(floor, boundary))? {
            let held = tx.pending_free_pages_before(boundary)?;
            tx.abort()?;
            return Ok(if held {
                ReclaimStep::ReaderHeld
            } else {
                ReclaimStep::Drained
            });
        }
        tx.set_deferred_free_budget(extents, entries);
        tx.set_reclaim_ceiling(boundary);
        tx.commit()?;
        Ok(ReclaimStep::Progressed(ReclaimBoundary(boundary)))
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn fail_next_deferred_free_commit_for_test(&self) {
        self.deferred_free_commit_io_fault
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Finish writable housekeeping while the caller still owns its writer
    /// lock, then allow read-only opens over the committed root during a
    /// graceful drain. The caller must prevent all later writes. Drop releases
    /// this handle without another housekeeping or header write.
    pub fn prepare_read_only_handoff(&mut self) -> Result<(), Error> {
        if !self.read_only_handoff_prepared {
            self.ensure_allocator_state_table_and_trim()?;
            self.mem.publish_read_only_handoff()?;
            self.read_only_handoff_prepared = true;
        }
        Ok(())
    }
}

/// The exclusive freeing-transaction boundary a maintenance reclaim cycle sampled on its first step.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimBoundary(TransactionId);

/// What one bounded maintenance reclaim step did.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimStep {
    /// Reclaimed eligible entries below the boundary and committed.
    Progressed(ReclaimBoundary),
    /// Nothing remains below the boundary.
    Drained,
    /// Entries remain below the boundary, but a live reader or snapshot still protects all of them.
    ReaderHeld,
    /// A savepoint exists, so nothing may be reclaimed.
    Blocked,
}

impl Drop for Database {
    fn drop(&mut self) {
        if !thread::panicking()
            && !self.read_only_handoff_prepared
            && self.ensure_allocator_state_table_and_trim().is_err()
        {
            #[cfg(feature = "logging")]
            warn!("Failed to write allocator state table. Repair may be required at restart.");
        }

        let close = if self.read_only_handoff_prepared {
            self.mem.close_after_read_only_handoff()
        } else {
            self.mem.close()
        };
        if close.is_err() {
            #[cfg(feature = "logging")]
            warn!("Failed to flush database file. Repair may be required at restart.");
        }
    }
}

pub struct RepairSession {
    progress: f64,
    aborted: bool,
}

impl RepairSession {
    pub(crate) fn new(progress: f64) -> Self {
        Self {
            progress,
            aborted: false,
        }
    }

    pub(crate) fn aborted(&self) -> bool {
        self.aborted
    }

    /// Abort the repair process. The coorresponding call to [`Builder::open`] or [`Builder::create`] will return an error
    pub fn abort(&mut self) {
        self.aborted = true;
    }

    /// Returns an estimate of the repair progress in the range [0.0, 1.0). At 1.0 the repair is complete.
    pub fn progress(&self) -> f64 {
        self.progress
    }
}

/// Configuration builder of a redb [Database].
pub struct Builder {
    page_size: usize,
    region_size: Option<u64>,
    cache_size: usize,
    repair_callback: Box<dyn Fn(&mut RepairSession)>,
    deferred_free_budget: (usize, usize),
    keep_pace_reclaim: bool,
}

impl Builder {
    /// Construct a new [Builder] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `cache_size_bytes`: 1GiB
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            // Default to 4k pages. Benchmarking showed that this was a good default on all platforms,
            // including MacOS with 16k pages. Therefore, users are not allowed to configure it at the moment.
            // It is part of the file format, so can be enabled in the future.
            page_size: PAGE_SIZE,
            region_size: None,
            // TODO: Default should probably take into account the total system memory
            cache_size: 1024 * 1024 * 1024,
            repair_callback: Box::new(|_| {}),
            deferred_free_budget: (usize::MAX, usize::MAX),
            keep_pace_reclaim: false,
        }
    }

    /// Set a callback which will be invoked periodically in the event that the database file needs
    /// to be repaired.
    ///
    /// The [`RepairSession`] argument can be used to control the repair process.
    ///
    /// If the database file needs repair, the callback will be invoked at least once.
    /// There is no upper limit on the number of times it may be called.
    pub fn set_repair_callback(
        &mut self,
        callback: impl Fn(&mut RepairSession) + 'static,
    ) -> &mut Self {
        self.repair_callback = Box::new(callback);
        self
    }

    /// Set the internal page size of the database
    ///
    /// Valid values are powers of two, greater than or equal to 512
    ///
    /// ## Defaults
    ///
    /// Default to 4 Kib pages.
    #[cfg(any(fuzzing, test))]
    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = std::cmp::max(size, 512);
        self
    }

    /// Bound deferred reclamation in each durable commit, including open and
    /// close housekeeping. Remaining entries stay in the durable free trees.
    /// The first limit counts allocated extents, each of which can span multiple
    /// physical pages. The second counts free-list entries, including empty lists.
    /// These counts do not bound bytes or physical pages reclaimed per commit.
    /// Both trees receive a share; the older pending key receives the extra unit.
    /// Explicit whole-file `Database::compact` still drains all pending frees.
    #[cfg(any(test, feature = "contextdb-internal"))]
    #[doc(hidden)]
    pub fn set_deferred_free_budget(&mut self, extents: usize, entries: usize) -> &mut Self {
        self.deferred_free_budget = (extents, entries);
        self
    }

    /// Let each durable commit reclaim at least as many deferred extents (and entries) as it
    /// freed itself, above the configured budget, so an active writer's backlog cannot grow while
    /// eligible work exists. Reclamation still never passes the reader floor.
    #[cfg(any(test, feature = "contextdb-internal"))]
    #[doc(hidden)]
    pub fn set_keep_pace_reclaim(&mut self, keep_pace: bool) -> &mut Self {
        self.keep_pace_reclaim = keep_pace;
        self
    }

    /// Set the amount of memory (in bytes) used for caching data
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.cache_size = bytes;
        self
    }

    #[cfg(any(test, fuzzing, feature = "test-seams"))]
    pub fn set_region_size(&mut self, size: u64) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Database::new(
            Box::new(FileBackend::new_with_path(file, path)?),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            &self.repair_callback,
            None,
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }

    /// Opens an existing redb database.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let path = path.as_ref();
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Database::new(
            Box::new(FileBackend::new_with_path(file, path)?),
            false,
            self.page_size,
            None,
            self.cache_size,
            &self.repair_callback,
            None,
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }

    /// Open an existing database with the given backend.
    ///
    /// Unlike [`Self::create_with_backend`], this never initializes empty storage.
    /// The caller must keep exclusive access to the backend for the database's lifetime.
    pub fn open_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(backend),
            false,
            self.page_size,
            None,
            self.cache_size,
            &self.repair_callback,
            None,
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }

    /// Open existing storage while consulting a complete authenticated set of
    /// external snapshot roots before every writer allocation/reclaim boundary.
    #[doc(hidden)]
    pub fn open_with_backend_and_external_snapshots(
        &self,
        backend: impl StorageBackend,
        provider: ExternalSnapshotProvider,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(backend),
            false,
            self.page_size,
            None,
            self.cache_size,
            &self.repair_callback,
            Some(provider),
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }

    /// Opens an existing redb database.
    ///
    /// If the file has been opened for writing (i.e. as a [`Database`]) [`DatabaseError::DatabaseAlreadyOpen`]
    /// will be returned on platforms which support file locks (macOS, Windows, Linux). On other platforms,
    /// the caller MUST avoid calling this method when the database is open for writing.
    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let file = OpenOptions::new().read(true).open(path)?;

        ReadOnlyDatabase::new(
            Box::new(FileBackend::new_internal(file, true)?),
            self.page_size,
            None,
            self.cache_size,
        )
    }

    /// Opens an existing read-only database through a caller-owned backend.
    ///
    /// The caller must provide any required coordination and keep the backend's
    /// physical storage stable for the database lifetime.
    #[doc(hidden)]
    pub fn open_read_only_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        ReadOnlyDatabase::new(Box::new(backend), self.page_size, None, self.cache_size)
    }

    /// Open a read-only backend for an externally registered, retained root.
    /// The caller must preserve that root's pages and use `begin_external_read`.
    /// No allocator state or recovery writes are required for this reader.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn open_read_only_external_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        ReadOnlyDatabase::new_inner(
            Box::new(backend),
            self.page_size,
            None,
            self.cache_size,
            true,
        )
    }

    /// Open an existing or create a new database in the given `file`.
    ///
    /// The file must be empty or contain a valid database.
    pub fn create_file(&self, file: File) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(FileBackend::new(file)?),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            &self.repair_callback,
            None,
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }

    /// Create storage with an authenticated external snapshot provider, so
    /// later reader registrations are retained before allocation or reclaim.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn create_with_backend_and_external_snapshots(
        &self,
        backend: impl StorageBackend,
        provider: ExternalSnapshotProvider,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(backend),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            &self.repair_callback,
            Some(provider),
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }

    /// Open an existing or create a new database with the given backend.
    pub fn create_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(backend),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            &self.repair_callback,
            None,
            self.deferred_free_budget,
            self.keep_pace_reclaim,
        )
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

#[cfg(test)]
#[path = "open_with_backend_region_override_tests.rs"]
mod open_with_backend_region_override_tests;

#[cfg(test)]
mod test {
    use crate::backends::FileBackend;
    use crate::{
        CommitError, Database, DatabaseError, Durability, ReadableDatabase, ReadableTable,
        StorageBackend, StorageError, TableDefinition, TransactionError,
    };
    use std::fs::File;
    use std::io::{ErrorKind, Read, Seek, SeekFrom};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(feature = "contextdb-internal")]
    #[test]
    fn nonblocking_external_read_refuses_before_refresh_release() {
        let temp = crate::create_tempfile();
        let calls = Arc::new(AtomicU64::new(0));
        let roots = Arc::new(std::sync::Mutex::new(Vec::new()));
        let provider_calls = Arc::clone(&calls);
        let provider_roots = Arc::clone(&roots);
        let provider = Arc::new(move || {
            provider_calls.fetch_add(1, Ordering::SeqCst);
            Ok(provider_roots.lock().unwrap().clone())
        });
        let db = Database::builder()
            .create_with_backend_and_external_snapshots(
                FileBackend::new(temp.reopen().unwrap()).unwrap(),
                provider,
            )
            .unwrap();
        let read = db.begin_read().unwrap();
        let snapshot = read.storage_snapshot_identity();
        roots.lock().unwrap().push(snapshot);
        let before = calls.load(Ordering::SeqCst);
        db.with_external_snapshot_refresh_lock_for_test(|| {
            drop(db.begin_read().expect("ordinary capture does not refresh"));
            let error = db.begin_external_read_nonblocking(snapshot).err().unwrap();
            assert!(Database::is_external_snapshot_refresh_busy(&error));
            assert!(matches!(error,
                TransactionError::Storage(StorageError::Io(error))
                    if error.kind() == ErrorKind::WouldBlock
                        && error.to_string().contains("external snapshot refresh")
            ));
            assert_eq!(calls.load(Ordering::SeqCst), before);
        });
        drop(db.begin_external_read_nonblocking(snapshot).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), before + 1);
        drop(db.begin_external_read(snapshot).unwrap());
        let provider_error = TransactionError::Storage(StorageError::Io(std::io::Error::new(
            ErrorKind::WouldBlock,
            "provider coordination is unavailable",
        )));
        assert!(!Database::is_external_snapshot_refresh_busy(
            &provider_error
        ));
    }

    #[test]
    fn prepared_read_only_handoff_keeps_reads_and_refuses_later_writes() {
        let (file, path) = crate::create_tempfile().into_parts();
        let lock_view = file
            .try_clone()
            .expect("retain lock descriptor for handoff");
        let backend = FailingBackend::new(
            FileBackend::new(file).expect("open handoff backend"),
            u64::MAX,
        );
        let writes_remaining = Arc::clone(&backend.countdown);
        let mut writer = Database::builder()
            .create_with_backend(backend)
            .expect("create writer");
        let table: TableDefinition<u64, u64> = TableDefinition::new("handoff");
        let tx = writer.begin_write().expect("begin fixture write");
        tx.open_table(table)
            .expect("open fixture table")
            .insert(1, 7)
            .expect("write fixture row");
        tx.commit().expect("commit fixture row");

        writer
            .prepare_read_only_handoff()
            .expect("publish committed read-only root");
        assert!(matches!(
            writer.begin_write(),
            Err(TransactionError::Storage(StorageError::DatabaseClosed))
        ));
        lock_view
            .lock_shared()
            .expect("downgrade caller-owned writer lock for readers");
        let reader = Database::builder()
            .open_read_only_with_backend(
                FileBackend::new_internal(
                    File::open(&path).expect("open retained descriptor"),
                    true,
                )
                .expect("construct retained read backend"),
            )
            .expect("open read-only while writer backend remains alive");
        assert_eq!(
            reader
                .begin_read()
                .expect("read committed root")
                .open_table(table)
                .expect("open committed table")
                .get(1)
                .expect("read committed row")
                .expect("committed row exists")
                .value(),
            7
        );
        let writes_after_handoff = writes_remaining.load(Ordering::SeqCst);
        drop(writer);
        assert_eq!(
            writes_remaining.load(Ordering::SeqCst),
            writes_after_handoff,
            "dropping a quiesced writer performs no backend writes"
        );
        assert_eq!(
            reader
                .begin_read()
                .expect("retained reader remains valid")
                .open_table(table)
                .expect("open retained table")
                .get(1)
                .expect("read retained row")
                .expect("retained row exists")
                .value(),
            7
        );
    }

    #[cfg(feature = "contextdb-internal")]
    #[test]
    fn external_reader_keeps_its_root_when_writer_grows_after_header_read() {
        const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("external_growth");
        #[derive(Debug)]
        struct GrowingBackend {
            inner: std::sync::Mutex<File>,
            writer: Arc<Database>,
            grown: Arc<std::sync::atomic::AtomicBool>,
        }
        impl StorageBackend for GrowingBackend {
            fn len(&self) -> std::io::Result<u64> {
                self.inner
                    .lock()
                    .unwrap()
                    .metadata()
                    .map(|metadata| metadata.len())
            }
            fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
                {
                    let mut file = self.inner.lock().unwrap();
                    file.seek(SeekFrom::Start(offset))?;
                    file.read_exact(out)?;
                }
                if offset == 0 && out.len() > 16 && !self.grown.swap(true, Ordering::SeqCst) {
                    let before = self.len()?;
                    let tx = self.writer.begin_write().unwrap();
                    tx.open_table(TABLE)
                        .unwrap()
                        .insert(2, vec![9; 8 * 1024 * 1024].as_slice())
                        .unwrap();
                    tx.commit().unwrap();
                    assert!(
                        self.len()? > before,
                        "writer must actually grow the file after the reader captured its header"
                    );
                }
                Ok(())
            }
            fn set_len(&self, _: u64) -> std::io::Result<()> {
                panic!("external reader cannot resize")
            }
            fn sync_data(&self) -> std::io::Result<()> {
                panic!("external reader cannot sync")
            }
            fn write(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
                panic!("external reader cannot write")
            }
        }
        let temp = crate::create_tempfile();
        let writer = Arc::new(Database::create(temp.path()).unwrap());
        let tx = writer.begin_write().unwrap();
        tx.open_table(TABLE)
            .unwrap()
            .insert(1, b"original".as_slice())
            .unwrap();
        tx.commit().unwrap();
        let pin = writer.begin_read().unwrap();
        let root = pin.storage_snapshot_identity();
        let grown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = Database::builder()
            .open_read_only_external_with_backend(GrowingBackend {
                inner: std::sync::Mutex::new(File::open(temp.path()).unwrap()),
                writer,
                grown: Arc::clone(&grown),
            })
            .unwrap();
        assert!(grown.load(Ordering::SeqCst));
        let read = reader.begin_external_read(root).unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.get(1).unwrap().unwrap().value(), b"original");
        assert!(table.get(2).unwrap().is_none());
    }

    #[derive(Debug)]
    struct FailingBackend {
        inner: FileBackend,
        countdown: Arc<AtomicU64>,
    }

    impl FailingBackend {
        fn new(backend: FileBackend, countdown: u64) -> Self {
            Self {
                inner: backend,
                countdown: Arc::new(AtomicU64::new(countdown)),
            }
        }

        fn check_countdown(&self) -> Result<(), std::io::Error> {
            if self.countdown.load(Ordering::SeqCst) == 0 {
                return Err(std::io::Error::from(ErrorKind::Other));
            }

            Ok(())
        }

        fn decrement_countdown(&self) -> Result<(), std::io::Error> {
            if self
                .countdown
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                    if x > 0 { Some(x - 1) } else { None }
                })
                .is_err()
            {
                return Err(std::io::Error::from(ErrorKind::Other));
            }

            Ok(())
        }
    }

    impl StorageBackend for FailingBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.check_countdown()?;
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), std::io::Error> {
            self.check_countdown()?;
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            self.decrement_countdown()?;
            self.inner.write(offset, data)
        }
    }

    #[test]
    fn crash_regression4() {
        let tmpfile = crate::create_tempfile();
        let (file, path) = tmpfile.into_parts();

        let backend = FailingBackend::new(FileBackend::new(file).unwrap(), 20);
        let db = Database::builder()
            .set_cache_size(12686)
            .set_page_size(8 * 1024)
            .set_region_size(32 * 4096)
            .create_with_backend(backend)
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        let _savepoint = tx.ephemeral_savepoint().unwrap();
        let _persistent_savepoint = tx.persistent_savepoint().unwrap();
        tx.commit().unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            let _ = table.insert_reserve(118821, 360).unwrap();
        }
        let result = tx.commit();
        assert!(result.is_err());

        drop(db);
        Database::builder()
            .set_cache_size(1024 * 1024)
            .set_page_size(8 * 1024)
            .set_region_size(32 * 4096)
            .create(&path)
            .unwrap();
    }

    #[test]
    fn transient_io_error() {
        let tmpfile = crate::create_tempfile();
        let (file, path) = tmpfile.into_parts();

        let backend = FailingBackend::new(FileBackend::new(file).unwrap(), u64::MAX);
        let countdown = backend.countdown.clone();
        let db = Database::builder()
            .set_cache_size(0)
            .create_with_backend(backend)
            .unwrap();

        let table_def: TableDefinition<u64, u64> = TableDefinition::new("x");

        // Create some garbage
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            table.insert(0, 0).unwrap();
        }
        tx.commit().unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            table.insert(0, 1).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.begin_write().unwrap();
        // Cause an error in the commit
        countdown.store(0, Ordering::SeqCst);
        let result = tx.commit().err().unwrap();
        assert!(matches!(result, CommitError::Storage(StorageError::Io(_))));
        let result = db.begin_write().err().unwrap();
        assert!(matches!(
            result,
            TransactionError::Storage(StorageError::PreviousIo)
        ));
        // Simulate a transient error
        countdown.store(u64::MAX, Ordering::SeqCst);
        drop(db);

        // Check that recovery flag is set, even though the error has "cleared"
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(9)).unwrap();
        let mut god_byte = vec![0u8];
        assert_eq!(file.read(&mut god_byte).unwrap(), 1);
        assert_ne!(god_byte[0] & 2, 0);
    }

    #[test]
    fn small_pages() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
        let txn = db.begin_write().unwrap();
        {
            txn.open_table(table_definition).unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn small_pages2() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint0 = tx.ephemeral_savepoint().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        tx.restore_savepoint(&savepoint0).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&660503, 489).unwrap().as_mut().fill(0xFF);
            assert!(t.remove(&291295).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        tx.restore_savepoint(&savepoint0).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint2 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint0);
        tx.restore_savepoint(&savepoint2).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.get(&2059).unwrap().is_none());
            assert!(t.remove(&145227).unwrap().is_none());
            assert!(t.remove(&145227).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint3 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint1);
        tx.restore_savepoint(&savepoint3).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint4 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint2);
        tx.restore_savepoint(&savepoint3).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.remove(&207936).unwrap().is_none());
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let _savepoint5 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint3);
        // savepoint4 was invalidated by the restore_savepoint(savepoint3) call
        // above, but that transaction was aborted, so the invalidation is
        // reversed and savepoint4 is valid again. Restoring it here invalidates
        // savepoint5 (which is newer), so the next transaction restores
        // savepoint4 again rather than savepoint5.
        tx.restore_savepoint(&savepoint4).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        tx.restore_savepoint(&savepoint4).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();
    }

    #[test]
    fn small_pages3() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let mut tx = db.begin_write().unwrap();
        let _savepoint0 = tx.ephemeral_savepoint().unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 306];
            t.insert(&539717, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        tx.restore_savepoint(&savepoint1).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 2008];
            t.insert(&784384, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();
    }

    #[test]
    fn small_pages4() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.get(&131072).unwrap().is_none());
            let value = vec![0xFF; 1130];
            t.insert(&42394, value.as_slice()).unwrap();
            t.insert_reserve(&744037, 3645).unwrap().as_mut().fill(0xFF);
            assert!(t.get(&0).unwrap().is_none());
        }
        tx.abort().unwrap();

        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&118749, 734).unwrap().as_mut().fill(0xFF);
        }
        tx.abort().unwrap();
    }

    #[test]
    fn dynamic_shrink() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
        let big_value = vec![0u8; 1024];

        let db = Database::builder()
            .set_region_size(1024 * 1024)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..2048 {
                table.insert(&i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let file_size = tmpfile.as_file().metadata().unwrap().len();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..2048 {
                table.remove(&i).unwrap();
            }
        }
        txn.commit().unwrap();

        // Perform a couple more commits to be sure the database has a chance to compact
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            table.insert(0, [].as_slice()).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            table.remove(0).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        txn.commit().unwrap();

        let final_file_size = tmpfile.as_file().metadata().unwrap().len();
        assert!(final_file_size < file_size);
    }

    #[test]
    fn create_new_db_in_empty_file() {
        let tmpfile = crate::create_tempfile();

        let _db = Database::builder()
            .create_file(tmpfile.into_file())
            .unwrap();
    }

    #[test]
    fn open_missing_file() {
        let tmpfile = crate::create_tempfile();

        let err = Database::builder()
            .open(tmpfile.path().with_extension("missing"))
            .unwrap_err();

        match err {
            DatabaseError::Storage(StorageError::Io(err)) if err.kind() == ErrorKind::NotFound => {}
            err => panic!("Unexpected error for empty file: {err}"),
        }
    }

    #[test]
    fn open_empty_file() {
        let tmpfile = crate::create_tempfile();

        let err = Database::builder().open(tmpfile.path()).unwrap_err();

        match err {
            DatabaseError::Storage(StorageError::Io(err))
                if err.kind() == ErrorKind::InvalidData => {}
            err => panic!("Unexpected error for empty file: {err}"),
        }
    }
}
