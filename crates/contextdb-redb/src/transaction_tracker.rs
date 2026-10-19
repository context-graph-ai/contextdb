use crate::db::TransactionGuard;
use crate::transactions::{ExternalSnapshotProvider, StorageSnapshotIdentity};
use crate::tree_store::{BtreeHeader, PageHint, PageNumber, TableTree, TransactionalMemory};
use crate::{Key, Result, Savepoint, StorageError, TypeName, Value};
#[cfg(feature = "logging")]
use log::debug;
use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem;
use std::mem::size_of;
use std::sync::{Arc, Condvar, Mutex};

#[cfg(feature = "contextdb-internal")]
#[derive(Debug)]
pub(crate) struct ExternalSnapshotRefreshBusy;

#[cfg(feature = "contextdb-internal")]
impl std::fmt::Display for ExternalSnapshotRefreshBusy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("external snapshot refresh is held by another operation")
    }
}

#[cfg(feature = "contextdb-internal")]
impl std::error::Error for ExternalSnapshotRefreshBusy {}

#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct TransactionId(u64);

impl TransactionId {
    pub(crate) fn new(value: u64) -> TransactionId {
        Self(value)
    }

    pub(crate) fn raw_id(self) -> u64 {
        self.0
    }

    pub(crate) fn next(self) -> TransactionId {
        TransactionId(self.0 + 1)
    }

    pub(crate) fn increment(&mut self) -> TransactionId {
        let next = self.next();
        *self = next;
        next
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SavepointId(pub u64);

impl SavepointId {
    pub(crate) fn next(self) -> SavepointId {
        SavepointId(self.0 + 1)
    }
}

impl Value for SavepointId {
    type SelfType<'a> = SavepointId;
    type AsBytes<'a> = [u8; size_of::<u64>()];

    fn fixed_width() -> Option<usize> {
        Some(size_of::<u64>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        SavepointId(u64::from_le_bytes(data.try_into().unwrap()))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::SavepointId")
    }
}

impl Key for SavepointId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).0.cmp(&Self::from_bytes(data2).0)
    }
}

// A single registry preserves counts independently from physical backing ownership.
struct ReaderRegistry {
    #[cfg(feature = "contextdb-internal")]
    map: hashbrown::HashMap<
        TransactionId,
        u64,
        std::collections::hash_map::RandomState,
        crate::native_storage_admission::ReaderRegistryAllocator,
    >,
    #[cfg(not(feature = "contextdb-internal"))]
    map: BTreeMap<TransactionId, u64>,
    minimum: Option<TransactionId>,
}
impl Default for ReaderRegistry {
    fn default() -> Self {
        Self {
            #[cfg(feature = "contextdb-internal")]
            map: hashbrown::HashMap::with_hasher_in(
                Default::default(),
                crate::native_storage_admission::ReaderRegistryAllocator::new(false),
            ),
            #[cfg(not(feature = "contextdb-internal"))]
            map: Default::default(),
            minimum: None,
        }
    }
}
impl ReaderRegistry {
    fn increment(&mut self, id: TransactionId) -> Result {
        #[cfg(feature = "contextdb-internal")]
        {
            let native = crate::native_storage_admission::native_storage_active();
            if native && !self.map.allocator().qualified {
                // Prepare a different paid allocation before touching the old map.
                let mut replacement = hashbrown::HashMap::with_hasher_in(
                    self.map.hasher().clone(),
                    crate::native_storage_admission::ReaderRegistryAllocator::new(true),
                );
                replacement
                    .try_reserve(self.map.len() + usize::from(!self.map.contains_key(&id)))
                    .map_err(|_| replacement.allocator().error())?;
                for (&key, &count) in &self.map {
                    replacement.insert(key, count);
                }
                let old = std::mem::replace(&mut self.map, replacement);
                drop(old);
            } else if !self.map.contains_key(&id) {
                // Existing native backing remains paid; future growth uses the
                // executing native owner, or legacy allocation outside admission.
                // A legacy growth cannot claim qualification: replace it explicitly.
                if !native
                    && self.map.allocator().qualified
                    && self.map.len() == self.map.capacity()
                {
                    let mut replacement = Self::default();
                    for (&key, &count) in &self.map {
                        replacement.map.insert(key, count);
                    }
                    replacement.minimum = self.minimum;
                    std::mem::swap(self, &mut replacement);
                    drop(replacement);
                }
                self.map
                    .try_reserve(1)
                    .map_err(|_| self.map.allocator().error())?;
            }
        }
        *self.map.entry(id).or_insert(0) += 1;
        self.minimum = Some(self.minimum.map_or(id, |old| old.min(id)));
        Ok(())
    }
    fn get(&self, id: &TransactionId) -> Option<&u64> {
        self.map.get(id)
    }
    fn get_mut(&mut self, id: &TransactionId) -> Option<&mut u64> {
        self.map.get_mut(id)
    }
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    fn keys(&self) -> impl Iterator<Item = &TransactionId> {
        self.map.keys()
    }
    fn first_key_value(&self) -> Option<(&TransactionId, &u64)> {
        self.minimum.and_then(|id| self.map.get_key_value(&id))
    }
    fn remove(&mut self, id: &TransactionId) {
        self.map.remove(id);
        if self.minimum == Some(*id) {
            self.minimum = self.map.keys().copied().min();
        }
    }
    fn detach_empty(&mut self) -> Option<Self> {
        self.map.is_empty().then(|| std::mem::take(self))
    }
}

// One fixed Box per exact root. The registry holds weak guards, never an
// ancestral read or the exclusive write guard. Removing a pin frees its Box
// before its enclosing read owner can release the prepaid control invoice.
#[cfg(feature = "contextdb-internal")]
pub(crate) struct PreparedPin {
    identity: StorageSnapshotIdentity,
    guard: std::sync::Weak<TransactionGuard>,
    active: Arc<std::sync::atomic::AtomicBool>,
    next: Option<Box<PreparedPin>>,
}

struct State {
    #[cfg(feature = "contextdb-internal")]
    prepared_pins: Option<Box<PreparedPin>>,
    next_savepoint_id: SavepointId,
    // reference count of read transactions per transaction id
    live_read_transactions: ReaderRegistry,
    next_transaction_id: TransactionId,
    live_write_transaction: Option<TransactionId>,
    waiting_write_transactions: usize,
    valid_savepoints: BTreeMap<SavepointId, TransactionId>,
    // Non-durable commits that are still in-memory, and waiting for a durable commit to get flushed
    // We need to make sure that the freed-table does not get processed for these, since they are not durable yet
    // Therefore, we hold a read transaction on their nearest durable ancestor
    //
    // Maps non-durable transaction id -> durable ancestor
    pending_non_durable_commits: HashMap<TransactionId, TransactionId>,
    // Non-durable commits which have NOT been processed in the freed table
    unprocessed_freed_non_durable_commits: BTreeSet<TransactionId>,
    // A registered root pins the transaction floor. Pages freed after that
    // transaction stay in redb's durable deferred-free table until release.
    // Admission does not inventory the root or retain a database-sized set.
    external_snapshots: HashSet<StorageSnapshotIdentity>,
    external_snapshot_admission_complete: bool,
}

pub(crate) struct TransactionTracker {
    #[cfg(feature = "test-seams")]
    pub(crate) table_write_observer: crate::table_write_observer::ObserverSlot,
    state: Mutex<State>,
    live_write_transaction_available: Condvar,
    external_snapshot_provider: Option<ExternalSnapshotProvider>,
    local_snapshot_provider: std::sync::OnceLock<ExternalSnapshotProvider>,
    external_snapshot_refresh: Mutex<()>,
    pub(crate) deferred_free_budget: (usize, usize),
    pub(crate) keep_pace_reclaim: bool,
}

// Transfer an existing native floor through authentication and installation.
// The map entry already exists: retaining it only increments its reference
// count and cannot allocate another transaction entry.
struct SnapshotRefreshReadPin<'a> {
    tracker: &'a TransactionTracker,
    transaction: Option<TransactionId>,
}

impl Drop for SnapshotRefreshReadPin<'_> {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction {
            self.tracker.deallocate_read_transaction(transaction);
        }
    }
}

impl TransactionTracker {
    pub(crate) fn new(next_transaction_id: TransactionId) -> Self {
        Self::new_with_external_snapshots(
            next_transaction_id,
            None,
            (usize::MAX, usize::MAX),
            false,
        )
    }

    pub(crate) fn new_with_external_snapshots(
        next_transaction_id: TransactionId,
        external_snapshot_provider: Option<ExternalSnapshotProvider>,
        deferred_free_budget: (usize, usize),
        keep_pace_reclaim: bool,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                #[cfg(feature = "contextdb-internal")]
                prepared_pins: None,
                next_savepoint_id: SavepointId(0),
                live_read_transactions: Default::default(),
                next_transaction_id,
                live_write_transaction: None,
                waiting_write_transactions: 0,
                valid_savepoints: Default::default(),
                pending_non_durable_commits: Default::default(),
                unprocessed_freed_non_durable_commits: Default::default(),
                external_snapshots: Default::default(),
                external_snapshot_admission_complete: false,
            }),
            live_write_transaction_available: Condvar::new(),
            external_snapshot_provider,
            local_snapshot_provider: std::sync::OnceLock::new(),
            external_snapshot_refresh: Mutex::new(()),
            deferred_free_budget,
            keep_pace_reclaim,
            #[cfg(feature = "test-seams")]
            table_write_observer: Default::default(),
        }
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn install_local_snapshot_provider(&self, provider: ExternalSnapshotProvider) {
        self.local_snapshot_provider.get_or_init(|| provider);
    }

    pub(crate) fn refresh_external_snapshots(&self, mem: &Arc<TransactionalMemory>) -> Result {
        if self.external_snapshot_provider.is_none() && self.local_snapshot_provider.get().is_none()
        {
            return Ok(());
        }
        let provider = self.external_snapshot_provider.as_ref();
        // Dynamic owner registrations may race ordinary writer admission.
        // Serialize provider observations so an older one cannot discard a
        // newly admitted root. No page inventory is built here.
        let _refresh = self.external_snapshot_refresh.lock()?;
        self.refresh_external_snapshots_locked(mem, provider)
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn try_refresh_external_snapshots(&self, mem: &Arc<TransactionalMemory>) -> Result {
        if self.external_snapshot_provider.is_none() && self.local_snapshot_provider.get().is_none()
        {
            return Ok(());
        }
        let provider = self.external_snapshot_provider.as_ref();
        let _refresh = match self.external_snapshot_refresh.try_lock() {
            Ok(refresh) => refresh,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    ExternalSnapshotRefreshBusy,
                )));
            }
            Err(std::sync::TryLockError::Poisoned(error)) => return Err(error.into()),
        };
        self.refresh_external_snapshots_locked(mem, provider)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn with_external_snapshot_refresh_lock_for_test<T>(
        &self,
        operation: impl FnOnce() -> T,
    ) -> T {
        let _refresh = self.external_snapshot_refresh.lock().unwrap();
        operation()
    }

    fn refresh_external_snapshots_locked(
        &self,
        mem: &Arc<TransactionalMemory>,
        provider: Option<&ExternalSnapshotProvider>,
    ) -> Result {
        let mut snapshots = match provider {
            Some(provider) => provider().map_err(StorageError::Io)?,
            None => Vec::new(),
        };
        if let Some(local) = self.local_snapshot_provider.get() {
            snapshots.extend(local().map_err(StorageError::Io)?);
        }
        let supplied = snapshots.iter().copied().collect::<HashSet<_>>();
        #[cfg(feature = "contextdb-internal")]
        let prepared_pins = {
            let state = self.state.lock()?;
            let mut pins = Vec::new();
            let mut next = state.prepared_pins.as_deref();
            while let Some(node) = next {
                // Inactive roots belong exclusively to preparation. Refresh
                // must not retain their guards across a refused write's abort.
                if node.active.load(std::sync::atomic::Ordering::Acquire) {
                    if let Some(guard) = node.guard.upgrade() {
                        pins.push((node.identity, guard));
                    }
                }
                next = node.next.as_deref();
            }
            pins
        };
        let mut native_pin = SnapshotRefreshReadPin {
            tracker: self,
            transaction: None,
        };
        {
            let mut state = self.state.lock()?;
            native_pin.transaction = state
                .live_read_transactions
                .first_key_value()
                .map(|(tx, _)| *tx);
            if let Some(transaction) = native_pin.transaction {
                *state
                    .live_read_transactions
                    .get_mut(&transaction)
                    .expect("the selected floor exists") += 1;
            }
            // A read at F has kept every page retired since F allocated.
            // The same deferred-free floor protects any supplied snapshot at
            // or after F, even if that snapshot's own native capture ended
            // before this refresh. Requiring its exact transaction to remain
            // native rejects a reader whose pages are already protected.
            let retained_floor = state
                .external_snapshots
                .iter()
                .map(|snapshot| snapshot.transaction_id)
                .chain(
                    state
                        .live_read_transactions
                        .first_key_value()
                        .map(|(tx, _)| tx.raw_id()),
                )
                .min();
            #[cfg(feature = "contextdb-internal")]
            let retained_floor = retained_floor.into_iter()
                .chain(prepared_pins.iter().map(|(identity, _)| identity.transaction_id)).min();
            if state.external_snapshot_admission_complete
                && supplied.iter().any(|snapshot| {
                    snapshot.transaction_id != 0
                        && retained_floor.is_none_or(|floor| snapshot.transaction_id < floor)
                })
            {
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "new external snapshots require a live read pin after writer admission",
                )));
            }
        }
        let already_admitted = self.state.lock()?.external_snapshots.clone();
        // Authenticated active prepared pins do not consult the live header:
        // cleanup may have failed or poisoned its lock after physical flush.
        let mut last_committed = None;
        for snapshot in &snapshots {
            if snapshot.transaction_id == 0 && snapshot.user_root.is_some() {
                return Err(StorageError::Corrupted(
                    "provisional external snapshot must not name a user-tree root".to_owned(),
                ));
            }
            if already_admitted.contains(snapshot) {
                continue;
            }
            // Only the exact actual-write-issued root can bypass a stale live
            // header after confirmed durability. Arbitrary future ids still fail.
            #[cfg(feature = "contextdb-internal")]
            if prepared_pins.iter().any(|(identity, guard)| identity == snapshot && guard.prepared_active()) {
                continue;
            }
            let last_committed = match last_committed {
                Some(id) => id,
                None => {
                    let id = mem.get_last_committed_transaction_id()?.raw_id();
                    last_committed = Some(id);
                    id
                }
            };
            if snapshot.transaction_id > last_committed {
                return Err(StorageError::Corrupted(format!(
                    "external snapshot transaction {} is newer than committed transaction {last_committed}",
                    snapshot.transaction_id
                )));
            }
            if let Some(root) = snapshot.user_root {
                let page = PageNumber::validated_from_le_bytes(root.page).ok_or_else(|| {
                    StorageError::Corrupted(
                        "external snapshot root has an invalid allocation order".to_owned(),
                    )
                })?;
                let tree = TableTree::new(
                    Some(BtreeHeader::new(page, root.checksum, root.length)),
                    PageHint::None,
                    Arc::new(TransactionGuard::fake()),
                    mem.clone(),
                )?;
                if !tree.verify_root_checksum()? {
                    return Err(StorageError::Corrupted(format!(
                        "external snapshot transaction {} has an invalid user-tree root",
                        snapshot.transaction_id
                    )));
                }
            }
        }
        let mut state = self.state.lock()?;
        state.external_snapshots = supplied;
        state.external_snapshot_admission_complete = true;
        Ok(())
    }

    pub(crate) fn has_external_snapshot(&self, snapshot: StorageSnapshotIdentity) -> bool {
        let state = self.state.lock().unwrap();
        // The retain-everything entry (transaction zero, no root) keeps every
        // page past every commit, so any committed root is protected while it
        // is supplied, exactly as if that root were listed.
        state.external_snapshots.contains(&snapshot)
            || state.external_snapshots.contains(&StorageSnapshotIdentity {
                transaction_id: 0,
                user_root: None,
            })
    }

    pub(crate) fn external_snapshot_page_sets(
        &self,
        mem: &Arc<TransactionalMemory>,
    ) -> Result<Vec<(StorageSnapshotIdentity, HashSet<PageNumber>)>> {
        let snapshots = self.state.lock()?.external_snapshots.clone();
        snapshots
            .into_iter()
            .map(|snapshot| {
                let root = snapshot.user_root.map(|root| {
                    BtreeHeader::new(
                        PageNumber::validated_from_le_bytes(root.page)
                            .expect("validated at admission"),
                        root.checksum,
                        root.length,
                    )
                });
                let tree = TableTree::new(
                    root,
                    PageHint::None,
                    Arc::new(TransactionGuard::fake()),
                    mem.clone(),
                )?;
                let mut pages = HashSet::new();
                tree.visit_all_pages(|path| {
                    pages.insert(path.page_number());
                    Ok(())
                })?;
                Ok((snapshot, pages))
            })
            .collect()
    }

    /// Page sets of the authenticated external snapshots only. Process-local
    /// roots are pinned through the same set but are not file-reader holds,
    /// so retention diagnostics leave them out.
    pub(crate) fn authenticated_external_snapshot_page_sets(
        &self,
        mem: &Arc<TransactionalMemory>,
    ) -> Result<Vec<(StorageSnapshotIdentity, HashSet<PageNumber>)>> {
        let mut local = match self.local_snapshot_provider.get() {
            Some(local) => local()
                .map_err(StorageError::Io)?
                .into_iter()
                .collect::<HashSet<_>>(),
            None => HashSet::new(),
        };
        if let Some(provider) = &self.external_snapshot_provider {
            for snapshot in provider().map_err(StorageError::Io)? {
                local.remove(&snapshot);
            }
        }
        Ok(self
            .external_snapshot_page_sets(mem)?
            .into_iter()
            .filter(|(snapshot, _)| !local.contains(snapshot))
            .collect())
    }

    pub(crate) fn start_write_transaction(&self) -> TransactionId {
        let mut state = self.state.lock().unwrap();
        while state.live_write_transaction.is_some() {
            state.waiting_write_transactions += 1;
            state = self.live_write_transaction_available.wait(state).unwrap();
            state.waiting_write_transactions -= 1;
        }
        assert!(state.live_write_transaction.is_none());
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.live_write_transaction = Some(transaction_id);

        transaction_id
    }

    pub(crate) fn has_waiting_write_transactions(&self) -> bool {
        self.state.lock().unwrap().waiting_write_transactions > 0
    }

    pub(crate) fn end_write_transaction(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.live_write_transaction.unwrap(), id);
        state.live_write_transaction = None;
        self.live_write_transaction_available.notify_one();
    }

    pub(crate) fn clear_pending_non_durable_commits(&self) {
        let mut state = self.state.lock().unwrap();
        let ids = mem::take(&mut state.pending_non_durable_commits);
        for (_, durable_ancestor) in ids {
            let ref_count = state
                .live_read_transactions
                .get_mut(&durable_ancestor)
                .unwrap();
            *ref_count -= 1;
            if *ref_count == 0 {
                state.live_read_transactions.remove(&durable_ancestor);
            }
        }
        let empty = state.live_read_transactions.detach_empty();
        drop(state);
        drop(empty);
    }

    pub(crate) fn is_unprocessed_non_durable_commit(&self, id: TransactionId) -> bool {
        let state = self.state.lock().unwrap();
        state.unprocessed_freed_non_durable_commits.contains(&id)
    }

    pub(crate) fn mark_unprocessed_non_durable_commit(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        state.unprocessed_freed_non_durable_commits.remove(&id);
    }

    pub(crate) fn oldest_unprocessed_non_durable_commit(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        state
            .unprocessed_freed_non_durable_commits
            .iter()
            .next()
            .copied()
    }

    pub(crate) fn register_non_durable_commit(
        &self,
        id: TransactionId,
        durable_ancestor: TransactionId,
    ) {
        let mut state = self.state.lock().unwrap();
        state
            .live_read_transactions
            .increment(durable_ancestor)
            .expect("legacy registration allocation");
        state
            .pending_non_durable_commits
            .insert(id, durable_ancestor);
        state.unprocessed_freed_non_durable_commits.insert(id);
    }

    pub(crate) fn restore_savepoint_counter_state(&self, next_savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        assert!(state.valid_savepoints.is_empty());
        state.next_savepoint_id = next_savepoint;
    }

    pub(crate) fn register_persistent_savepoint(&self, savepoint: &Savepoint) {
        let mut state = self.state.lock().unwrap();
        state
            .live_read_transactions
            .increment(savepoint.get_transaction_id())
            .expect("legacy savepoint allocation");
        state
            .valid_savepoints
            .insert(savepoint.get_id(), savepoint.get_transaction_id());
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn register_prepared_pin(
        &self, identity: StorageSnapshotIdentity, guard: &Arc<TransactionGuard>,
    ) -> Result {
        let mut node = Box::new(PreparedPin { identity, guard: Arc::downgrade(guard), active: guard.prepared_activation(), next: None });
        let mut state = self.state.lock()?;
        debug_assert_eq!(state.live_write_transaction.map(|id| id.raw_id()), Some(identity.transaction_id));
        node.next = state.prepared_pins.take();
        state.prepared_pins = Some(node);
        Ok(())
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn remove_prepared_pin(&self, id: TransactionId) {
        // Drop must also work when a preceding storage panic poisoned state.
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut link = &mut state.prepared_pins;
        while link.is_some() {
            if link.as_ref().unwrap().identity.transaction_id == id.raw_id() {
                let mut retired = link.take().unwrap();
                *link = retired.next.take();
                drop(state);
                drop(retired);
                return;
            }
            link = &mut link.as_mut().unwrap().next;
        }
        // Registration itself may have refused before linking this guard.
    }

    pub(crate) fn register_read_transaction(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<TransactionId> {
        let mut state = self.state.lock()?;
        let id = mem.get_last_committed_transaction_id()?;
        state.live_read_transactions.increment(id)?;

        Ok(id)
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn capture_native_read(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<(TransactionId, Option<BtreeHeader>)> {
        self.capture_native_read_inner(mem, false)
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn capture_native_publication_read(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<(TransactionId, Option<BtreeHeader>)> {
        self.capture_native_read_inner(mem, true)
    }

    #[cfg(feature = "contextdb-internal")]
    fn capture_native_read_inner(
        &self,
        mem: &TransactionalMemory,
        borrow_existing: bool,
    ) -> Result<(TransactionId, Option<BtreeHeader>)> {
        // Continuous tracker -> PageManagerState lock order: choose one header,
        // then register its retention floor before releasing tracker protection.
        let mut state = self.state.lock()?;
        let captured = mem.capture_native_read_root()?;
        // Publication can borrow an already retained slot without allocating
        // or requalifying its original backing. A new slot still takes the
        // ordinary fully admitted native registration path.
        if borrow_existing && let Some(count) = state.live_read_transactions.get_mut(&captured.0) {
            *count += 1;
        } else {
            state.live_read_transactions.increment(captured.0)?;
        }
        Ok(captured)
    }

    pub(crate) fn deallocate_read_transaction(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        let ref_count = state.live_read_transactions.get_mut(&id).unwrap();
        *ref_count -= 1;
        if *ref_count == 0 {
            state.live_read_transactions.remove(&id);
        }
        let empty = state.live_read_transactions.detach_empty();
        drop(state);
        drop(empty); // backing free and original invoice settlement outside tracker
    }

    pub(crate) fn any_savepoint_exists(&self) -> bool {
        !self.state.lock().unwrap().valid_savepoints.is_empty()
    }

    pub(crate) fn allocate_savepoint(&self, transaction_id: TransactionId) -> SavepointId {
        let mut state = self.state.lock().unwrap();
        let id = state.next_savepoint_id.next();
        state.next_savepoint_id = id;
        state.valid_savepoints.insert(id, transaction_id);
        id
    }

    // Deallocates the given savepoint and its matching reference count on the transcation
    pub(crate) fn deallocate_savepoint(&self, savepoint: SavepointId, transaction: TransactionId) {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .remove(&savepoint);
        self.deallocate_read_transaction(transaction);
    }

    pub(crate) fn is_valid_savepoint(&self, id: SavepointId) -> bool {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .contains_key(&id)
    }

    pub(crate) fn list_savepoints_after(&self, id: SavepointId) -> Vec<SavepointId> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .range((
                std::ops::Bound::Excluded(id),
                std::ops::Bound::Unbounded::<SavepointId>,
            ))
            .map(|(x, _)| *x)
            .collect()
    }

    // Removes the given savepoints from the in-memory `valid_savepoints` map without touching
    // live_read_transactions refs. The caller is responsible for making sure those refs are
    // released by some other means: ephemeral `Savepoint::drop` or `deallocate_savepoint`
    // (called via `delete_persistent_savepoint`) do that for their respective savepoint kinds.
    //
    // Savepoints that have already been removed (for example, by `deallocate_savepoint` earlier
    // in the same transaction) are silently skipped.
    pub(crate) fn invalidate_savepoints(&self, savepoints: impl IntoIterator<Item = SavepointId>) {
        let mut state = self.state.lock().unwrap();
        for id in savepoints {
            state.valid_savepoints.remove(&id);
        }
    }

    pub(crate) fn oldest_savepoint(&self) -> Option<(SavepointId, TransactionId)> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .first_key_value()
            .map(|x| (*x.0, *x.1))
    }

    pub(crate) fn oldest_live_read_transaction(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        let ordinary = state
            .live_read_transactions
            .minimum
            .into_iter()
            .chain(
                state
                    .external_snapshots
                    .iter()
                    .map(|snapshot| TransactionId::new(snapshot.transaction_id)),
            )
            .min();
        #[cfg(feature = "contextdb-internal")]
        {
            let mut oldest = ordinary;
            let mut pin = state.prepared_pins.as_deref();
            while let Some(node) = pin {
                oldest = oldest.into_iter().chain(Some(TransactionId::new(node.identity.transaction_id))).min();
                pin = node.next.as_deref();
            }
            return oldest;
        }
        #[cfg(not(feature = "contextdb-internal"))]
        ordinary
    }

    // A native read owns this handle. External registrations own only a
    // durable root and are re-admitted from the provider after handle recycle.
    pub(crate) fn has_native_read_transactions(&self) -> bool {
        let state = self.state.lock().unwrap();
        #[cfg(feature = "contextdb-internal")]
        if state.prepared_pins.is_some() { return true; }
        !state.live_read_transactions.is_empty()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn native_read_transaction_count_for_test(&self, transaction_id: u64) -> u64 {
        self.state
            .lock()
            .unwrap()
            .live_read_transactions
            .get(&TransactionId::new(transaction_id))
            .copied()
            .unwrap_or(0)
    }

    // Returns the transaction id of the oldest non-durable transaction which has not been processed
    // for freeing, which has live read transactions
    pub(crate) fn oldest_live_read_nondurable_transaction(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        state
            .live_read_transactions
            .keys()
            .filter(|id| state.pending_non_durable_commits.contains_key(id))
            .copied()
            .min()
    }
}
