use crate::commit_directory::{
    CapturedLegacyCursor, CommitDirectory, CommitDirectoryCursor, EngineMemoryCommitDirectory,
    MemoryCommitDirectory, PreparedMemoryCommitRecord, ReadCommitContext, ReadCommitCursor,
    ReadCommitDirectory,
};
use crate::write_set::{WriteSet, WriteSetApplicator};
use contextdb_core::read_contract::OwnerReadCancellation;
use contextdb_core::read_memory::ReadMemoryBudget;
use contextdb_core::{AtomicLsn, AtomicTxId, Error, Lsn, Result, RowId, SnapshotId, TxId};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(feature = "test-seams")]
mod resident_history_test_seams;

#[cfg(test)]
thread_local! {
    static COMMIT_WAIT_ENTERED_FOR_TEST: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    // Identifies only this thread's commit guard; no allocation or retained owner.
    static CURRENT_COMMIT_MANAGER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct OwedPublication {
    sequence: u64,
    reservation: Option<Box<dyn Any + Send>>,
}

#[derive(Default)]
struct CommitRelease {
    epoch: Mutex<u64>,
    changed: Condvar,
}

/// The ordinary manager retains its historical `Arc` control.  Engine-only
/// construction uses the same control payload behind a funded cell, so its
/// header is released before the associated source invoice can refund.
enum CommitReleaseHandle {
    Legacy(Arc<CommitRelease>),
    Owned(FundedArc<CommitRelease>),
}

impl CommitReleaseHandle {
    fn legacy() -> Self {
        Self::Legacy(Arc::new(CommitRelease::default()))
    }

    fn try_new(funder: &SourceFunder) -> Result<Self> {
        let charge = FundedArc::<CommitRelease>::prepare(funder.clone())?;
        Ok(Self::Owned(FundedArc::from_prepared(
            CommitRelease::default(),
            charge,
        )))
    }

    fn with_release<'a, R>(&'a self, f: impl FnOnce(&'a CommitRelease) -> R) -> R {
        match self {
            Self::Legacy(release) => f(release),
            Self::Owned(release) => f(release),
        }
    }
}

impl Clone for CommitReleaseHandle {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy(release) => Self::Legacy(release.clone()),
            Self::Owned(release) => Self::Owned(release.clone()),
        }
    }
}

struct CommitLockGuard<'a, S: WriteSetApplicator> {
    manager: &'a TransactionManager<S>,
    previous_manager: usize,
    lock: Option<MutexGuard<'a, ()>>,
    release: &'a CommitRelease,
}

impl<S: WriteSetApplicator> Drop for CommitLockGuard<'_, S> {
    fn drop(&mut self) {
        // Wake only after the mutex is actually available. The epoch lock
        // closes the gap between a waiter's failed try_lock and its park.
        CURRENT_COMMIT_MANAGER.with(|current| current.set(self.previous_manager));
        drop(self.lock.take());
        // Reservations were admitted before the corresponding durable writes.
        // Completion is infallible and takes a fresh boundary after this one unwinds.
        self.manager.drain_owed_publications();
        let mut epoch = self.release.epoch.lock();
        *epoch = epoch.wrapping_add(1);
        self.release.changed.notify_all();
    }
}

/// The logical frontier and engine-owned root are captured under one lock.
/// Downcast the handle to the engine's admitted shared snapshot descriptor.
#[derive(Clone)]
pub struct PublishedRead {
    pub snapshot: SnapshotId,
    pub lsn: Lsn,
    pub handle: Option<Arc<dyn Any + Send + Sync>>,
}

/// Engine maintenance's borrowed authority under the actual commit guard.
/// Construction is private; reservation and completion cannot escape the
/// guard or move to another thread. Logical transaction identity is unchanged.
#[doc(hidden)]
pub struct MaintenancePublication<'guard, S: WriteSetApplicator> {
    manager: &'guard TransactionManager<S>,
    snapshot: SnapshotId,
    lsn: Lsn,
    reservation: Option<Option<Box<dyn Any + Send>>>,
    completed: bool,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl<S: WriteSetApplicator> MaintenancePublication<'_, S> {
    /// Admit the successor immediately before a nonempty durable mutation.
    /// A completed cycle may be followed by another under this same guard.
    pub fn reserve(&mut self) -> Result<()> {
        if self.reservation.is_some() {
            return Err(Error::InvalidStateTransition(
                "metadata publication already has an active reservation".into(),
            ));
        }
        let reservation = self.manager.store.reserve_metadata_publication()?;
        self.completed = false;
        self.reservation = Some(reservation);
        Ok(())
    }

    /// Abandon a refused durable mutation before starting the next cycle.
    /// The failed storage transaction committed nothing; its admission drops.
    pub fn abandon(&mut self) {
        self.reservation.take();
        self.completed = false;
    }

    /// Durability and memory settlement succeeded; preserve the logical pair
    /// and use the manager's existing publisher before the removal marker ends.
    pub fn complete(&mut self) {
        assert!(!self.completed);
        let reservation = self
            .reservation
            .take()
            .expect("maintenance publication requires its admitted reservation");
        self.manager
            .store
            .complete_metadata_publication(reservation, self.snapshot, self.lsn);
        self.manager.publish_read_snapshot(self.lsn, None);
        self.completed = true;
    }
}

impl std::fmt::Debug for PublishedRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishedRead")
            .field("snapshot", &self.snapshot)
            .field("lsn", &self.lsn)
            .finish_non_exhaustive()
    }
}

/// Captured history cannot implement or acquire the mutable directory trait.
enum CommitHistoryCapability {
    Writable(Arc<dyn CommitDirectory>),
    EngineMemory(EngineMemoryCommitDirectory),
    Captured {
        directory: Arc<dyn ReadCommitDirectory>,
        image_read_memory: Arc<dyn ReadMemoryBudget>,
    },
}

pub struct TransactionManager<S: WriteSetApplicator> {
    next_tx: AtomicTxId,
    committed_watermark: AtomicTxId,
    next_lsn: AtomicLsn,
    committed_lsn: AtomicLsn,
    /// One independently readable final-publication point. The two public
    /// atomics above serve existing callers, but cannot be sampled as a pair
    /// while a commit is between their stores.
    published_read: Mutex<PublishedRead>,
    active_txs: Mutex<ActiveTransactions>,
    commit_index: CommitHistoryCapability,
    commit_mutex: Mutex<()>,
    commit_release: CommitReleaseHandle,
    owed_publication: Mutex<Option<OwedPublication>>,
    direct_commit_sequence: AtomicU64,
    store: S,
}

// Engine ownership cannot pass through the legacy mutable callback doors.
// Only the standalone constructors install Legacy; the opaque engine facade
// accesses Engine directly and never exports its inner manager.
enum ActiveTransactions {
    Legacy(HashMap<TxId, WriteSet>),
    Engine(crate::staged_source::MutableSourceMap<TxId, EngineWriteSet>),
}
impl std::ops::Deref for ActiveTransactions {
    type Target = HashMap<TxId, WriteSet>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Legacy(active) => active,
            Self::Engine(_) => panic!("engine transactions require restricted source doors"),
        }
    }
}
impl std::ops::DerefMut for ActiveTransactions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Legacy(active) => active,
            Self::Engine(_) => panic!("engine transactions require restricted source doors"),
        }
    }
}
impl ActiveTransactions {
    fn engine(&self) -> &crate::staged_source::MutableSourceMap<TxId, EngineWriteSet> {
        match self {
            Self::Engine(active) => active,
            Self::Legacy(_) => panic!("engine active host is not installed"),
        }
    }
    fn engine_mut(&mut self) -> &mut crate::staged_source::MutableSourceMap<TxId, EngineWriteSet> {
        match self {
            Self::Engine(active) => active,
            Self::Legacy(_) => panic!("engine active host is not installed"),
        }
    }
}

#[derive(Debug)]
pub struct CommitFailure {
    pub error: Error,
    pub write_set: Option<Box<WriteSet>>,
}

impl<S: WriteSetApplicator> TransactionManager<S> {
    pub fn new(store: S) -> Self {
        Self::new_with_counters(store, TxId(1), Lsn(1), TxId(0))
    }

    pub fn new_with_counters(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
    ) -> Self {
        Self::new_with_counters_and_commit_index(
            store,
            next_tx,
            next_lsn,
            committed_watermark,
            BTreeMap::new(),
        )
    }

    pub fn new_with_counters_and_commit_index(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        commit_index: BTreeMap<Lsn, TxId>,
    ) -> Self {
        Self::new_with_directory(
            store,
            next_tx,
            next_lsn,
            committed_watermark,
            Arc::new(MemoryCommitDirectory(Mutex::new(commit_index))),
            None,
        )
    }

    /// File writers attach their durable directory and the initial admitted
    /// root here, without loading commit history into transaction memory.
    pub fn new_with_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        directory: Arc<dyn CommitDirectory>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self {
            next_tx: AtomicTxId::new(next_tx),
            committed_watermark: AtomicTxId::new(committed_watermark),
            next_lsn: AtomicLsn::new(next_lsn),
            committed_lsn: AtomicLsn::new(Lsn(next_lsn.0.saturating_sub(1))),
            published_read: Mutex::new(PublishedRead {
                snapshot: SnapshotId::from_tx(committed_watermark),
                lsn: Lsn(next_lsn.0.saturating_sub(1)),
                handle,
            }),
            active_txs: Mutex::new(ActiveTransactions::Legacy(HashMap::new())),
            commit_index: CommitHistoryCapability::Writable(directory),
            commit_mutex: Mutex::new(()),
            commit_release: CommitReleaseHandle::legacy(),
            owed_publication: Mutex::new(None),
            direct_commit_sequence: AtomicU64::new(0),
            store,
        }
    }

    fn new_with_engine_parts(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        commit_index: CommitHistoryCapability,
        handle: Option<Arc<dyn Any + Send + Sync>>,
        commit_release: CommitReleaseHandle,
    ) -> Self {
        Self {
            next_tx: AtomicTxId::new(next_tx),
            committed_watermark: AtomicTxId::new(committed_watermark),
            next_lsn: AtomicLsn::new(next_lsn),
            committed_lsn: AtomicLsn::new(Lsn(next_lsn.0.saturating_sub(1))),
            published_read: Mutex::new(PublishedRead {
                snapshot: SnapshotId::from_tx(committed_watermark),
                lsn: Lsn(next_lsn.0.saturating_sub(1)),
                handle,
            }),
            active_txs: Mutex::new(ActiveTransactions::Engine(Default::default())),
            commit_index,
            commit_mutex: Mutex::new(()),
            commit_release,
            owed_publication: Mutex::new(None),
            direct_commit_sequence: AtomicU64::new(0),
            store,
        }
    }

    /// Internal construction for the sealed committed image. Its directory and
    /// legacy adapters retain the image's already-declared reader child. The
    /// committed frontier is authenticated during fallible open, independently
    /// of the allocator successor (which cannot encode a successor after MAX).
    // The design-approved explicit committed_lsn signature keeps the authenticated
    // committed LSN separate from the allocator successor even at u64::MAX.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn new_with_captured_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        committed_lsn: Lsn,
        directory: Arc<dyn ReadCommitDirectory>,
        image_read_memory: Arc<dyn ReadMemoryBudget>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self {
            next_tx: AtomicTxId::new(next_tx),
            committed_watermark: AtomicTxId::new(committed_watermark),
            next_lsn: AtomicLsn::new(next_lsn),
            committed_lsn: AtomicLsn::new(committed_lsn),
            published_read: Mutex::new(PublishedRead {
                snapshot: SnapshotId::from_tx(committed_watermark),
                lsn: committed_lsn,
                handle,
            }),
            active_txs: Mutex::new(ActiveTransactions::Legacy(HashMap::new())),
            commit_index: CommitHistoryCapability::Captured {
                directory,
                image_read_memory,
            },
            commit_mutex: Mutex::new(()),
            commit_release: CommitReleaseHandle::legacy(),
            owed_publication: Mutex::new(None),
            direct_commit_sequence: AtomicU64::new(0),
            store,
        }
    }

    fn writable_directory(&self) -> Result<&dyn CommitDirectory> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => Ok(directory.as_ref()),
            CommitHistoryCapability::EngineMemory(_) => Err(Error::InvalidStateTransition(
                "engine memory commit history requires restricted source doors".into(),
            )),
            CommitHistoryCapability::Captured { .. } => Err(Error::InvalidStateTransition(
                "captured commit history has no writable transaction capability".into(),
            )),
        }
    }

    fn require_writable(&self) {
        self.ensure_writable()
            .expect("operation requires writable transaction capability");
    }

    fn ensure_writable(&self) -> Result<()> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(_) | CommitHistoryCapability::EngineMemory(_) => {
                Ok(())
            }
            CommitHistoryCapability::Captured { .. } => Err(Error::InvalidStateTransition(
                "captured commit history has no writable transaction capability".into(),
            )),
        }
    }

    fn prepare_engine_directory_record(
        &self,
        lsn: Lsn,
        tx: TxId,
    ) -> Result<Option<PreparedMemoryCommitRecord>> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(_) => Ok(None),
            CommitHistoryCapability::EngineMemory(directory) => {
                directory.prepare_record(lsn, tx).map(Some)
            }
            CommitHistoryCapability::Captured { .. } => self.ensure_writable().map(|()| None),
        }
    }

    fn record_engine_directory(
        &self,
        lsn: Lsn,
        tx: TxId,
        prepared: Option<PreparedMemoryCommitRecord>,
    ) {
        match (&self.commit_index, prepared) {
            (CommitHistoryCapability::Writable(directory), None) => {
                directory.record_applied(lsn, tx)
            }
            (CommitHistoryCapability::EngineMemory(directory), Some(record)) => {
                directory.record_prepared(record)
            }
            _ => panic!("commit directory record did not match its prepared custody"),
        }
    }

    #[doc(hidden)]
    pub fn try_begin(&self) -> Result<TxId> {
        self.writable_directory()?;
        let tx_id = self.next_tx.fetch_add(1, Ordering::SeqCst);
        let mut write_set = WriteSet::new();
        write_set.base_read = Some(self.capture_published_read());
        self.active_txs.lock().insert(tx_id, write_set);
        Ok(tx_id)
    }

    pub fn begin(&self) -> TxId {
        self.try_begin()
            .expect("begin requires writable transaction capability")
    }

    /// The immutable read view held from begin until commit or rollback.
    pub fn transaction_read(&self, tx: TxId) -> Result<PublishedRead> {
        let active = self.active_txs.lock();
        let ws = active.get(&tx).ok_or(Error::TxNotFound(tx))?;
        ws.base_read
            .clone()
            .ok_or_else(|| Error::Other("transaction has no captured read view".into()))
    }

    #[doc(hidden)]
    pub fn row_shadows_committed(&self, tx: TxId, table: &str, row: RowId) -> Result<bool> {
        self.writable_directory()?;
        let active = self.active_txs.lock();
        let ws = active.get(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(ws.ordered_staged_rows.contains_row(table, row)
            || ws
                .relational_deletes
                .iter()
                .any(|(name, id, _)| name == table && *id == row))
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.published_read.lock().snapshot
    }

    /// The last complete commit's snapshot and LSN, copied together without
    /// taking the commit mutex. A bounded reader may register this root while
    /// the next writer is still applying, then inspect only that snapshot.
    pub fn published_read_snapshot(&self) -> (SnapshotId, Lsn) {
        let published = self.published_read.lock();
        (published.snapshot, published.lsn)
    }

    /// Clone a complete descriptor while a durable write is in flight. This
    /// takes neither the writer serialization lock nor an admission/I/O path.
    pub fn capture_published_read(&self) -> PublishedRead {
        self.published_read.lock().clone()
    }

    pub fn with_write_set<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        self.writable_directory()?;
        let mut active = self.active_txs.lock();
        let ws = active.get_mut(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(f(ws))
    }

    /// Walk the maintained index while borrowing its transaction frontier.
    /// The closure admits each row copy before using its returned position;
    /// it must not re-enter transaction mutation while holding this borrow.
    pub fn with_ordered_staged_cursor<F, R>(
        &self,
        tx: TxId,
        table: &str,
        index: &str,
        bounds: crate::ordered_staged::OrderedStagedBounds<'_>,
        reverse: bool,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(Option<crate::ordered_staged::OrderedStagedCursor<'_>>, &WriteSet) -> R,
    {
        let active = self.active_txs.lock();
        let ws = active.get(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(f(
            ws.ordered_staged_rows
                .cursor(table, index, bounds.0, bounds.1, reverse),
            ws,
        ))
    }

    /// Observe the mutex used by `with_write_set` without waiting for it.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn write_set_lock_is_held_for_test(&self) -> bool {
        self.active_txs.try_lock().is_none()
    }

    pub fn with_write_set_detached<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        self.writable_directory()?;
        let mut ws = {
            let mut active = self.active_txs.lock();
            active.remove(&tx).ok_or(Error::TxNotFound(tx))?
        };
        let result = f(&mut ws);
        self.active_txs.lock().insert(tx, ws);
        Ok(result)
    }

    pub fn cloned_write_set(&self, tx: TxId) -> Result<WriteSet> {
        let active = self.active_txs.lock();
        active.get(&tx).cloned().ok_or(Error::TxNotFound(tx))
    }

    pub fn commit(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.commit_with_lsn(tx, read_memory).map(|_| ())
    }

    pub fn commit_with_lsn(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Lsn> {
        self.commit_with_lsn_prepared(tx, read_memory, |_| Ok(()))
            .map(|(lsn, _)| lsn)
            .map_err(|failure| failure.error)
    }

    pub fn commit_with_reserved_lsn_callback<F, G>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_commit: F,
        prepare: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn) -> Result<()>,
        G: FnOnce(&WriteSet) -> Result<()>,
    {
        self.commit_with_reserved_lsn_callback_mut(tx, read_memory, before_commit, |ws| prepare(ws))
    }

    pub fn commit_with_reserved_lsn_callback_mut<F, G>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_commit: F,
        prepare: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        let directory = self.writable_directory().map_err(|error| CommitFailure {
            error,
            write_set: None,
        })?;
        let _lock = self.lock_commit_boundary();
        let reserved_lsn = self.next_lsn.load(Ordering::SeqCst);
        if let Err(error) = before_commit(reserved_lsn) {
            return Err(CommitFailure {
                error,
                write_set: None,
            });
        }

        let mut ws = {
            let mut active = self.active_txs.lock();
            active.remove(&tx).ok_or(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            })?
        };
        ws.canonicalize_final_state();
        if ws.is_empty() {
            return Ok((self.current_lsn(), ws));
        }
        let visibility_tx = self.visibility_tx_for_commit(tx, ws.visibility_floor);
        ws.reassign_tx(tx, visibility_tx);
        ws.finalized_tx = Some(visibility_tx);
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        debug_assert_eq!(lsn, reserved_lsn);
        ws.stamp_lsn(lsn);
        if let Err(error) = prepare(&mut ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }

        // Prepare may replace the write set; the manager remains the identity authority.
        ws.finalized_tx = Some(visibility_tx);
        if let Err(error) = self.store.apply(&ws, read_memory.clone()) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }
        directory.record_applied(lsn, visibility_tx);
        self.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, ws))
    }

    pub fn commit_with_lsn_prepared<F>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        prepare: F,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(&WriteSet) -> Result<()>,
    {
        self.commit_with_lsn_prepared_and_applied(tx, read_memory, prepare, |_, _| {})
    }

    pub fn commit_with_lsn_prepared_and_applied<F, G>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        prepare: F,
        after_apply: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(&WriteSet) -> Result<()>,
        G: FnOnce(Lsn, &WriteSet),
    {
        self.commit_with_lsn_prepared_and_applied_mut(
            tx,
            read_memory,
            |ws| prepare(ws),
            after_apply,
        )
    }

    pub fn commit_with_lsn_prepared_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        prepare: F,
        after_apply: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(&mut WriteSet) -> Result<()>,
        G: FnOnce(Lsn, &WriteSet),
    {
        let directory = self.writable_directory().map_err(|error| CommitFailure {
            error,
            write_set: None,
        })?;
        let _lock = self.lock_commit_boundary();
        let mut ws = {
            let mut active = self.active_txs.lock();
            active.remove(&tx).ok_or(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            })?
        };
        ws.canonicalize_final_state();
        if ws.is_empty() {
            return Ok((self.current_lsn(), ws));
        }
        let visibility_tx = self.visibility_tx_for_commit(tx, ws.visibility_floor);
        ws.reassign_tx(tx, visibility_tx);
        ws.finalized_tx = Some(visibility_tx);
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        ws.stamp_lsn(lsn);
        if let Err(error) = prepare(&mut ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }

        // Prepare may replace the write set; the manager remains the identity authority.
        ws.finalized_tx = Some(visibility_tx);
        if let Err(error) = self.store.apply(&ws, read_memory.clone()) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }
        // Publish engine-owned sidecars and derived state before the MVCC
        // watermark makes the applied rows visible to a fresh snapshot. A
        // reader that can see a row must also be able to see the creation and
        // delete evidence published for the same commit.
        after_apply(lsn, &ws);
        directory.record_applied(lsn, visibility_tx);
        self.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, ws))
    }

    pub fn commit_with_lsn_active_prepare_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        active_prepare: F,
        prepare: G,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn, TxId) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        self.commit_with_lsn_precanonicalized_active_prepare_and_applied_mut(
            tx,
            read_memory,
            || Ok(false),
            active_prepare,
            prepare,
            after_apply,
        )
    }

    /// Internal commit preparation before no-op detection. The projection runs
    /// under the commit guard, without holding the active transaction map.
    #[doc(hidden)]
    pub fn commit_with_lsn_precanonicalized_active_prepare_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: F,
        prepare: G,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn, TxId) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        let directory = self.writable_directory().map_err(|error| CommitFailure {
            error,
            write_set: None,
        })?;
        let _lock = self.lock_commit_boundary();
        if !self.active_txs.lock().contains_key(&tx) {
            return Err(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            });
        }
        let requires_commit = match requires_commit() {
            Ok(required) => required,
            Err(error) => {
                let write_set = self.active_txs.lock().remove(&tx).map(Box::new);
                return Err(CommitFailure { error, write_set });
            }
        };
        let visibility_tx = {
            let mut active = self.active_txs.lock();
            let ws = active.get_mut(&tx).ok_or(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            })?;
            ws.requires_commit_lsn |= requires_commit;
            ws.canonicalize_final_state();
            if ws.is_empty() {
                let ws = active.remove(&tx).expect("checked active tx exists");
                return Ok((self.current_lsn(), ws));
            }
            let visibility_tx = self.visibility_tx_for_commit(tx, ws.visibility_floor);
            ws.reassign_tx(tx, visibility_tx);
            ws.finalized_tx = Some(visibility_tx);
            visibility_tx
        };

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        {
            let mut active = self.active_txs.lock();
            let ws = active.get_mut(&tx).ok_or(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            })?;
            ws.stamp_lsn(lsn);
        }

        if let Err(error) = active_prepare(lsn, visibility_tx) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            let write_set = self.active_txs.lock().remove(&tx).map(Box::new);
            return Err(CommitFailure { error, write_set });
        }

        let mut ws = {
            let mut active = self.active_txs.lock();
            active.remove(&tx).ok_or(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            })?
        };
        ws.canonicalize_final_state();
        ws.reassign_tx(tx, visibility_tx);
        ws.finalized_tx = Some(visibility_tx);
        ws.stamp_lsn(lsn);

        if let Err(error) = prepare(&mut ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }

        // Prepare may replace the write set; the manager remains the identity authority.
        ws.finalized_tx = Some(visibility_tx);
        if let Err(error) = self.store.apply(&ws, read_memory.clone()) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }
        // Keep applied storage private until its after-apply publication is
        // complete. This is the atomic visibility boundary for engine-owned
        // sidecars that are not part of the generic store.
        after_apply(lsn, &ws);
        directory.record_applied(lsn, visibility_tx);
        self.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, ws))
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        self.rollback_write_set(tx).map(|_| ())
    }

    /// Remove every transaction still open and hand back what each staged.
    ///
    /// A store that is closing will never commit them, and their write sets go
    /// with it -- so whatever they staged has to be given back to the memory
    /// accountant first, which is what the caller does with these. Closing
    /// released only the SESSION transaction before this existed, so a
    /// transaction opened with `begin()` and abandoned left its whole
    /// admission charged against a limit nothing was holding any more.
    pub fn drain_active_write_sets(&self) -> Vec<WriteSet> {
        // Captured construction has no active transactions, and try_begin
        // rejects before insertion. Resource release needs no write authority.
        if matches!(&self.commit_index, CommitHistoryCapability::Captured { .. }) {
            return Vec::new();
        }
        self.require_writable();
        let _lock = self.lock_commit_boundary();
        let mut active = self.active_txs.lock();
        active.drain().map(|(_, write_set)| write_set).collect()
    }

    /// Visit every transaction that is still open, so a change to the shape
    /// a staged row was admitted under can settle that row too.
    ///
    /// A staged row lives in its write set until commit, so it is invisible
    /// to anything that reads committed versions -- and it is charged under
    /// the index set the table had when its statement ran, then released
    /// under the one the table has when the transaction ends. The door that
    /// changes the index set uses this to re-price what is staged alongside
    /// what is committed.
    pub fn with_active_write_sets<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HashMap<TxId, WriteSet>) -> R,
    {
        self.require_writable();
        let mut active = self.active_txs.lock();
        f(&mut active)
    }

    pub fn rollback_write_set(&self, tx: TxId) -> Result<WriteSet> {
        self.writable_directory()?;
        let _lock = self.lock_commit_boundary();
        let mut active = self.active_txs.lock();
        active.remove(&tx).ok_or(Error::TxNotFound(tx))
    }

    pub fn rollback_empty_without_commit_lock(&self, tx: TxId) -> Result<()> {
        self.writable_directory()?;
        let mut active = self.active_txs.lock();
        let ws = active.get(&tx).ok_or(Error::TxNotFound(tx))?;
        if !ws.is_empty() {
            return Err(Error::Other(
                "cannot rollback non-empty transaction without commit lock".to_string(),
            ));
        }
        active.remove(&tx);
        Ok(())
    }

    fn visibility_tx_for_commit(&self, tx: TxId, floor: Option<TxId>) -> TxId {
        let committed = self.committed_watermark.load(Ordering::SeqCst);
        let desired = floor.unwrap_or(tx).max(tx);
        if desired.0 > committed.0 {
            desired
        } else {
            self.next_tx
                .fetch_max(TxId(committed.0.saturating_add(1)), Ordering::SeqCst);
            self.next_tx.fetch_add(1, Ordering::SeqCst)
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn new_row_id(&self) -> RowId {
        self.require_writable();
        self.store.new_row_id()
    }

    pub fn current_lsn(&self) -> Lsn {
        self.committed_lsn.load(Ordering::SeqCst)
    }

    fn publish_read_snapshot(&self, lsn: Lsn, visibility_tx: Option<TxId>) {
        self.require_writable();
        // The applicator prepared/reserved the root outside this lock. The
        // descriptor swap alone is the publication critical section. The old
        // root may release pages/accounting, so drop it after unlocking.
        let handle = self.store.publication_handle();
        let prior = {
            let mut published = self.published_read.lock();
            if let Some(tx) = visibility_tx {
                self.committed_watermark.fetch_max(tx, Ordering::SeqCst);
            }
            self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
            let next = PublishedRead {
                snapshot: SnapshotId::from_tx(self.committed_watermark.load(Ordering::SeqCst)),
                lsn,
                handle,
            };
            std::mem::replace(&mut *published, next)
        };
        drop(prior);
    }

    /// Release the writer's root after accepted reads have drained on close.
    /// Outstanding readers retain their independent shared handle.
    pub fn release_publication_handle(&self) {
        let prior = self.published_read.lock().handle.take();
        drop(prior);
    }

    pub fn commit_directory_cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Result<Box<dyn CommitDirectoryCursor>> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.cursor(lower, upper),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.cursor(lower, upper)),
            CommitHistoryCapability::Captured {
                directory,
                image_read_memory,
            } => CapturedLegacyCursor::open(
                directory.as_ref(),
                lower,
                upper,
                image_read_memory.clone(),
            ),
        }
    }

    /// Delivers one pair directly to the engine, which owns output admission.
    #[doc(hidden)]
    pub fn read_commit_directory_cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        context.checkpoint()?;
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => {
                directory.cursor_with_read_context(lower, upper, context)
            }
            CommitHistoryCapability::EngineMemory(directory) => {
                directory.cursor_with_read_context(lower, upper, context)
            }
            CommitHistoryCapability::Captured { directory, .. } => {
                directory.cursor(lower, upper, context)
            }
        }
    }

    /// Legacy collection belongs to Writable callers. Captured consumers use
    /// the context cursor so output allocation is admitted and owned by them.
    pub fn try_commit_index_prefix(&self, up_to: Lsn) -> Result<BTreeMap<Lsn, TxId>> {
        let mut cursor = self.commit_directory_cursor(Bound::Unbounded, Bound::Included(up_to))?;
        let mut entries = BTreeMap::new();
        while let Some((lsn, tx)) = cursor.next_entry()? {
            entries.insert(lsn, tx);
        }
        Ok(entries)
    }

    pub fn commit_index_prefix(&self, up_to: Lsn) -> BTreeMap<Lsn, TxId> {
        self.try_commit_index_prefix(up_to)
            .expect("commit directory read failed")
    }

    pub fn try_replace_commit_index(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.replace(retained),
            CommitHistoryCapability::EngineMemory(directory) => directory.replace(retained),
            CommitHistoryCapability::Captured { .. } => self.ensure_writable().map(|()| 0),
        }
    }

    pub fn replace_commit_index(&self, retained: BTreeMap<Lsn, TxId>) -> u64 {
        self.try_replace_commit_index(retained)
            .expect("commit directory replacement failed")
    }

    pub fn try_remove_commit_index_entries(&self, lsns: &[Lsn]) -> Result<u64> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.remove(lsns),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.remove(lsns)),
            CommitHistoryCapability::Captured { .. } => self.ensure_writable().map(|()| 0),
        }
    }

    pub fn remove_commit_index_entries(&self, lsns: &[Lsn]) -> u64 {
        self.try_remove_commit_index_entries(lsns)
            .expect("commit directory removal failed")
    }

    #[doc(hidden)]
    pub fn try_commit_index_census_with_context(
        &self,
        context: &ReadCommitContext<'_>,
    ) -> Result<(usize, Lsn)> {
        context.checkpoint()?;
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.census(),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.census()),
            CommitHistoryCapability::Captured { directory, .. } => directory.census(context),
        }
    }

    pub fn try_commit_index_census(&self) -> Result<(usize, Lsn)> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.census(),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.census()),
            CommitHistoryCapability::Captured {
                image_read_memory, ..
            } => self.try_commit_index_census_with_context(&ReadCommitContext::new(
                image_read_memory.clone(),
                &|| Ok(()),
            )),
        }
    }

    pub fn commit_index_len(&self) -> usize {
        self.commit_index_census().0
    }

    pub fn commit_index_entries(&self) -> Vec<(Lsn, TxId)> {
        self.commit_index_snapshot().into_iter().collect()
    }

    pub fn commit_index_snapshot(&self) -> BTreeMap<Lsn, TxId> {
        self.commit_index_prefix(Lsn(u64::MAX))
    }

    #[doc(hidden)]
    pub fn try_commit_index_floor_with_context(
        &self,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        context.checkpoint()?;
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.floor(upper),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.floor(upper)),
            CommitHistoryCapability::Captured { directory, .. } => directory.floor(upper, context),
        }
    }

    pub fn try_commit_index_floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.floor(upper),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.floor(upper)),
            CommitHistoryCapability::Captured {
                image_read_memory, ..
            } => self.try_commit_index_floor_with_context(
                upper,
                &ReadCommitContext::new(image_read_memory.clone(), &|| Ok(())),
            ),
        }
    }

    pub fn commit_index_floor_below(&self, lsn: Lsn) -> Option<(Lsn, TxId)> {
        self.require_writable();
        self.try_commit_index_floor(Bound::Excluded(lsn))
            .expect("commit directory seek failed")
    }

    pub fn commit_index_last(&self) -> Option<(Lsn, TxId)> {
        self.require_writable();
        self.try_commit_index_floor(Bound::Unbounded)
            .expect("commit directory seek failed")
    }

    #[doc(hidden)]
    pub fn try_commit_index_get_with_context(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
    ) -> Result<Option<TxId>> {
        context.checkpoint()?;
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => Ok(directory
                .floor(Bound::Included(lsn))?
                .filter(|(entry_lsn, _)| *entry_lsn == lsn)
                .map(|(_, tx)| tx)),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory
                .floor(Bound::Included(lsn))
                .filter(|(entry_lsn, _)| *entry_lsn == lsn)
                .map(|(_, tx)| tx)),
            CommitHistoryCapability::Captured { directory, .. } => directory.get(lsn, context),
        }
    }

    #[doc(hidden)]
    pub fn try_commit_index_contains_with_context(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
    ) -> Result<bool> {
        context.checkpoint()?;
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.contains(lsn),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.contains(lsn)),
            CommitHistoryCapability::Captured { directory, .. } => directory.contains(lsn, context),
        }
    }

    pub fn try_commit_index_contains(&self, lsn: Lsn) -> Result<bool> {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.contains(lsn),
            CommitHistoryCapability::EngineMemory(directory) => Ok(directory.contains(lsn)),
            CommitHistoryCapability::Captured {
                image_read_memory, ..
            } => self.try_commit_index_contains_with_context(
                lsn,
                &ReadCommitContext::new(image_read_memory.clone(), &|| Ok(())),
            ),
        }
    }

    pub fn commit_index_contains(&self, lsn: Lsn) -> bool {
        self.require_writable();
        self.try_commit_index_contains(lsn)
            .expect("commit directory probe failed")
    }

    pub fn commit_index_census(&self) -> (usize, Lsn) {
        self.require_writable();
        self.try_commit_index_census()
            .expect("commit directory bounds failed")
    }

    #[doc(hidden)]
    pub fn try_snapshot_at_lsn_with_context(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
    ) -> Result<SnapshotId> {
        Ok(SnapshotId::from_tx(
            self.try_commit_index_floor_with_context(Bound::Included(lsn), context)?
                .map_or(TxId(0), |(_, tx)| tx),
        ))
    }

    pub fn try_snapshot_at_lsn(&self, lsn: Lsn) -> Result<SnapshotId> {
        Ok(SnapshotId::from_tx(
            self.try_commit_index_floor(Bound::Included(lsn))?
                .map_or(TxId(0), |(_, tx)| tx),
        ))
    }

    pub fn snapshot_at_lsn(&self, lsn: Lsn) -> SnapshotId {
        self.require_writable();
        self.try_snapshot_at_lsn(lsn)
            .expect("commit directory seek failed")
    }

    pub fn allocate_ddl_lsn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        self.writable_directory()?;
        let _lock = self.lock_commit_boundary();
        let publication = self.store.reserve_metadata_publication()?;
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        match f(lsn) {
            Ok(result) => {
                self.store
                    .complete_metadata_publication(publication, self.snapshot(), lsn);
                self.publish_read_snapshot(lsn, None);
                Ok(result)
            }
            Err(err) => {
                self.next_lsn.store(lsn, Ordering::SeqCst);
                Err(err)
            }
        }
    }

    pub fn allocate_ddl_lsn_maybe<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(Lsn) -> Result<Option<R>>,
    {
        self.writable_directory()?;
        let _lock = self.lock_commit_boundary();
        let publication = self.store.reserve_metadata_publication()?;
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        match f(lsn) {
            Ok(Some(result)) => {
                self.store
                    .complete_metadata_publication(publication, self.snapshot(), lsn);
                self.publish_read_snapshot(lsn, None);
                Ok(Some(result))
            }
            Ok(None) => {
                self.next_lsn.store(lsn, Ordering::SeqCst);
                Ok(None)
            }
            Err(err) => {
                self.next_lsn.store(lsn, Ordering::SeqCst);
                Err(err)
            }
        }
    }

    pub fn with_commit_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _lock = self.lock_commit_boundary();
        f()
    }

    /// Engine-only physical maintenance at the existing publication boundary.
    /// Quiet or memory passes need no reservation. The higher-ranked borrow
    /// prevents a reservation or publication authority escaping this guard.
    #[doc(hidden)]
    pub fn with_maintenance_publication<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'guard> FnOnce(&mut MaintenancePublication<'guard, S>) -> Result<R>,
    {
        self.writable_directory()?;
        let _lock = self.lock_commit_boundary();
        let (snapshot, lsn) = self.published_read_snapshot();
        let mut publication = MaintenancePublication {
            manager: self,
            snapshot,
            lsn,
            reservation: None,
            completed: false,
            _same_thread: std::marker::PhantomData,
        };
        let result = f(&mut publication);
        if result.is_ok() {
            assert!(
                publication.reservation.is_none(),
                "successful maintenance must complete its reserved publication"
            );
        }
        result
    }

    fn lock_commit_boundary(&self) -> CommitLockGuard<'_, S> {
        self.commit_release
            .with_release(|release| self.commit_guard(self.commit_mutex.lock(), release))
    }

    fn commit_guard<'a>(
        &'a self,
        lock: MutexGuard<'a, ()>,
        release: &'a CommitRelease,
    ) -> CommitLockGuard<'a, S> {
        let previous_manager =
            CURRENT_COMMIT_MANAGER.with(|current| current.replace(self as *const Self as usize));
        CommitLockGuard {
            manager: self,
            previous_manager,
            lock: Some(lock),
            release,
        }
    }

    /// Internal caller guard identity for helpers used both standalone and
    /// inside ordinary/DDL publication. This never acquires a second lock.
    #[doc(hidden)]
    pub fn commit_boundary_held_by_current_thread(&self) -> bool {
        CURRENT_COMMIT_MANAGER.with(|current| current.get() == self as *const Self as usize)
    }

    /// Internal plumbing for a direct durable write inside a commit callback.
    /// Admission must happen before persistence; an unsuccessful write simply
    /// drops this reservation without recording a publication debt.
    #[doc(hidden)]
    pub fn reserve_deferred_metadata_publication(&self) -> Result<Option<Box<dyn Any + Send>>> {
        self.writable_directory()?;
        self.store.reserve_metadata_publication()
    }

    /// Record a successful direct commit and publish before operation return.
    /// Inside this thread's commit guard, its unwind performs completion instead.
    #[doc(hidden)]
    pub fn owe_metadata_publication(&self, reservation: Option<Box<dyn Any + Send>>) {
        self.require_writable();
        let sequence = self.direct_commit_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let mut owed = self.owed_publication.lock();
        if let Some(existing) = owed.as_mut() {
            existing.sequence = existing.sequence.max(sequence);
            // One prepared successor covers the combined durable writes. The
            // redundant reservation is released, never transferred twice.
            drop(reservation);
        } else {
            *owed = Some(OwedPublication {
                sequence,
                reservation,
            });
        }
        drop(owed);
        if !self.commit_boundary_held_by_current_thread() {
            self.drain_owed_publications();
        }
    }

    fn drain_owed_publications(&self) {
        if self.owed_publication.lock().is_none() {
            return;
        }
        let _lock = self.commit_mutex.lock();
        loop {
            // Take only debts known before preparing this root. An overlapping
            // append records a later debt in the now-empty slot and is drained
            // separately; it cannot be cleared by this publication.
            let Some(owed) = self.owed_publication.lock().take() else {
                break;
            };
            let (snapshot, lsn) = self.published_read_snapshot();
            self.store
                .complete_metadata_publication(owed.reservation, snapshot, lsn);
            self.publish_read_snapshot(lsn, None);
        }
    }

    /// Wait at most `duration` for the commit boundary, then return control to
    /// the caller if another operation still owns it. This is for callers with
    /// an independent cancellation signal: they can remain answerable without
    /// weakening the single-writer boundary or running the operation twice.
    pub fn try_with_commit_lock_for<F, R>(&self, duration: std::time::Duration, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        let _lock = self.commit_release.with_release(|release| {
            self.commit_mutex
                .try_lock_for(duration)
                .map(|lock| self.commit_guard(lock, release))
        })?;
        Some(f())
    }

    /// Wait for the next complete commit boundary, cancellation, or the
    /// supplied remaining allowance. The release epoch and cancellation use
    /// one condition variable, so neither event can disappear between the
    /// failed try_lock and the waiter's park.
    pub fn try_with_commit_lock_cancellable_for<F, R, D>(
        &self,
        duration: Duration,
        cancelled: &OwnerReadCancellation,
        deadline_expired: D,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce() -> R,
        D: Fn() -> bool,
    {
        // Core still owns the listener's callback allocation, but the callback
        // retains this exact paired release control for either construction.
        let wake_on_cancel = self.commit_release.clone();
        let _cancel_listener = cancelled.tell_on_cancel(move || {
            wake_on_cancel.with_release(|release| {
                let mut epoch = release.epoch.lock();
                *epoch = epoch.wrapping_add(1);
                release.changed.notify_all();
            });
        });
        let started = Instant::now();
        self.commit_release.with_release(|release| {
            let mut epoch = release.epoch.lock();
            loop {
                if cancelled.is_cancelled() || deadline_expired() {
                    return None;
                }
                if let Some(lock) = self.commit_mutex.try_lock() {
                    drop(epoch);
                    let _guard = self.commit_guard(lock, release);
                    if cancelled.is_cancelled() {
                        return None;
                    }
                    return Some(f());
                }
                #[cfg(test)]
                COMMIT_WAIT_ENTERED_FOR_TEST.with(|cell| {
                    if let Some(sender) = cell.borrow_mut().take() {
                        sender
                            .send(())
                            .expect("commit-wait observer remains available");
                    }
                });
                #[cfg(feature = "test-seams")]
                crate::commit_wait_test_seams::note_wait();
                let remaining = duration.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return None;
                }
                // The injected read clock can advance without notifying this
                // condition variable. Park briefly to recheck that clock; this
                // cadence never retries the operation or spins on the mutex.
                release
                    .changed
                    .wait_for(&mut epoch, remaining.min(Duration::from_millis(5)));
            }
        })
    }

    /// Returns the highest-committed TxId (the statement-scoped max for user-SQL
    /// bound checks against TXID columns). Reads `committed_watermark` under `SeqCst`.
    #[inline]
    pub fn current_tx_max(&self) -> TxId {
        self.committed_watermark.load(Ordering::SeqCst)
    }

    /// Returns the next transaction id the allocator will issue (peek, no increment).
    #[inline]
    pub fn peek_next_tx(&self) -> TxId {
        self.next_tx.load(Ordering::SeqCst)
    }

    /// Advances the local TxId allocator past an incoming peer TxId seen during
    /// sync-apply. The overflow guard fires on `u64::MAX` before any mutation so
    /// the receiver's counters are unchanged on error.
    pub fn advance_for_sync(&self, tx: TxId, table: &str, incoming: TxId) -> Result<()> {
        self.writable_directory()?;
        if incoming.0 == u64::MAX {
            return Err(Error::TxIdOverflow {
                table: table.to_string(),
                incoming: u64::MAX,
            });
        }
        self.next_tx
            .fetch_max(TxId(incoming.0 + 1), Ordering::SeqCst);
        self.with_write_set(tx, |ws| {
            ws.visibility_floor = Some(ws.visibility_floor.unwrap_or(incoming).max(incoming));
        })?;
        Ok(())
    }
}

impl<S: WriteSetApplicator> contextdb_core::TransactionManager for TransactionManager<S> {
    fn begin(&self) -> TxId {
        TransactionManager::begin(self)
    }

    fn commit(
        &self,
        tx: TxId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        TransactionManager::commit(self, tx, read_memory)
    }

    fn rollback(&self, tx: TxId) -> Result<()> {
        TransactionManager::rollback(self, tx)
    }

    fn snapshot(&self) -> SnapshotId {
        TransactionManager::snapshot(self)
    }
}
#[cfg(test)]
mod commit_wait_tests {
    mod test_read_memory {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/read_memory.rs"
        ));
    }

    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct NoopStore;

    impl WriteSetApplicator for NoopStore {
        fn apply(
            &self,
            _: &WriteSet,
            _read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        ) -> Result<()> {
            Ok(())
        }

        fn new_row_id(&self) -> RowId {
            RowId(1)
        }
    }

    #[test]
    fn cancellation_wakes_a_commit_wait_before_the_writer_releases() {
        let manager = Arc::new(TransactionManager::new(NoopStore));
        let writer_lock = manager.lock_commit_boundary();
        let cancellation = OwnerReadCancellation::new();
        let (entered_send, entered_recv) = std::sync::mpsc::channel();
        let (done_send, done_recv) = std::sync::mpsc::channel();
        let called = Arc::new(AtomicBool::new(false));
        let reader_manager = Arc::clone(&manager);
        let reader_cancellation = cancellation.clone();
        let reader_called = Arc::clone(&called);
        let reader = std::thread::spawn(move || {
            COMMIT_WAIT_ENTERED_FOR_TEST.with(|cell| *cell.borrow_mut() = Some(entered_send));
            let result = reader_manager.try_with_commit_lock_cancellable_for(
                Duration::from_secs(30),
                &reader_cancellation,
                || false,
                || reader_called.store(true, Ordering::SeqCst),
            );
            done_send.send(result).unwrap();
        });
        entered_recv
            .recv_timeout(Duration::from_secs(30))
            .expect("reader reaches commit-boundary wait while writer holds its lock");
        cancellation.cancel();
        assert_eq!(
            done_recv.recv_timeout(Duration::from_secs(30)).unwrap(),
            None,
            "cancelled read answers while writer still holds the commit boundary"
        );
        assert!(!called.load(Ordering::SeqCst));
        drop(writer_lock);
        reader.join().unwrap();
    }

    #[test]
    fn manual_deadline_advance_wakes_commit_wait_without_writer_release() {
        let manager = Arc::new(TransactionManager::new(NoopStore));
        let writer_lock = manager.lock_commit_boundary();
        let cancellation = OwnerReadCancellation::new();
        let clock_ms = Arc::new(AtomicU64::new(0));
        let (entered_send, entered_recv) = std::sync::mpsc::channel();
        let (done_send, done_recv) = std::sync::mpsc::channel();
        let reader_manager = Arc::clone(&manager);
        let reader_clock = Arc::clone(&clock_ms);
        let reader = std::thread::spawn(move || {
            COMMIT_WAIT_ENTERED_FOR_TEST.with(|cell| *cell.borrow_mut() = Some(entered_send));
            let result = reader_manager.try_with_commit_lock_cancellable_for(
                Duration::from_secs(30),
                &cancellation,
                || reader_clock.load(Ordering::SeqCst) > 10,
                || (),
            );
            done_send.send(result).unwrap();
        });
        entered_recv
            .recv_timeout(Duration::from_secs(30))
            .expect("reader reaches commit-boundary wait while writer holds its lock");
        clock_ms.store(11, Ordering::SeqCst);
        assert_eq!(
            done_recv.recv_timeout(Duration::from_secs(30)).unwrap(),
            None,
            "advanced read deadline answers before the writer releases"
        );
        drop(writer_lock);
        reader.join().unwrap();
    }
}

use crate::staged_source::{
    CanonicalStatementRows, CaptureResolver, CapturedIdentity, FundedArc, RowCellHandle,
    RowCustody, SourceBuffer, SourceFunder, SourceFunding, SourceResolver, SourceRoot,
    VectorCellHandle, with_sealed_cell, with_sealed_vector_cell,
};
use crate::write_set::RelationalDeletePredicate;
use contextdb_core::{
    AdjEntry, EdgeType, NodeId, TableName, VectorEntry, VectorIndexRef, VersionedRow,
};

/// Immutable field borrows; no raw WriteSet or eager image can escape here.
#[doc(hidden)]
pub struct EngineRead<'a> {
    pub base_read: &'a Option<PublishedRead>,
    pub relational_inserts: &'a Vec<(TableName, VersionedRow)>,
    pub relational_deletes: &'a Vec<(TableName, RowId, TxId)>,
    pub relational_delete_predicates: &'a Vec<RelationalDeletePredicate>,
    pub adj_inserts: &'a Vec<AdjEntry>,
    pub adj_deletes: &'a Vec<(NodeId, EdgeType, NodeId, TxId)>,
    pub vector_inserts: &'a Vec<VectorEntry>,
    pub vector_deletes: &'a Vec<(VectorIndexRef, RowId, TxId)>,
    pub vector_moves: &'a Vec<(VectorIndexRef, RowId, RowId, TxId)>,
    pub commit_lsn: &'a Option<Lsn>,
    pub finalized_tx: &'a Option<TxId>,
    pub relational_insert_source_lsns: &'a HashMap<TableName, HashMap<RowId, Lsn>>,
    pub relational_insert_source_kinds: &'a HashMap<TableName, HashMap<RowId, u8>>,
    pub relational_insert_admitted_bytes: &'a HashMap<TableName, HashMap<RowId, usize>>,
    pub sync_source_provenance_marks: &'a Vec<(TableName, RowId, Lsn, u8)>,
    pub config_writes: &'a Vec<(String, Vec<u8>)>,
    pub config_deletes: &'a Vec<String>,
    pub config_max_u64_keys: &'a Vec<String>,
    pub requires_commit_lsn: &'a bool,
    pub visibility_floor: &'a Option<TxId>,
    pub propagation_in_progress: &'a bool,
}
impl<'a> EngineRead<'a> {
    fn from_host(host: &'a EngineWriteSet) -> Self {
        Self {
            base_read: &host.state.ws.base_read,
            relational_inserts: &host.state.ws.relational_inserts,
            relational_deletes: &host.state.ws.relational_deletes,
            relational_delete_predicates: &host.state.ws.relational_delete_predicates,
            adj_inserts: &host.state.ws.adj_inserts,
            adj_deletes: &host.state.ws.adj_deletes,
            vector_inserts: &host.state.ws.vector_inserts,
            vector_deletes: &host.state.ws.vector_deletes,
            vector_moves: &host.state.ws.vector_moves,
            commit_lsn: &host.state.ws.commit_lsn,
            finalized_tx: &host.state.ws.finalized_tx,
            relational_insert_source_lsns: &host.state.ws.relational_insert_source_lsns,
            relational_insert_source_kinds: &host.state.ws.relational_insert_source_kinds,
            relational_insert_admitted_bytes: &host.state.ws.relational_insert_admitted_bytes,
            sync_source_provenance_marks: &host.state.ws.sync_source_provenance_marks,
            config_writes: &host.state.ws.config_writes,
            config_deletes: &host.state.ws.config_deletes,
            config_max_u64_keys: &host.state.ws.config_max_u64_keys,
            requires_commit_lsn: &host.state.ws.requires_commit_lsn,
            visibility_floor: &host.state.ws.visibility_floor,
            propagation_in_progress: &host.state.ws.propagation_in_progress,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.relational_inserts.is_empty()
            && self.relational_deletes.is_empty()
            && self.adj_inserts.is_empty()
            && self.adj_deletes.is_empty()
            && self.vector_inserts.is_empty()
            && self.vector_deletes.is_empty()
            && self.vector_moves.is_empty()
            && self.config_writes.is_empty()
            && self.config_deletes.is_empty()
            && self.sync_source_provenance_marks.is_empty()
            && !*self.requires_commit_lsn
    }
}
#[doc(hidden)]
pub struct EngineNonPayloadFields<'a> {
    pub relational_deletes: &'a mut Vec<(TableName, RowId, TxId)>,
    pub relational_delete_predicates: &'a mut Vec<RelationalDeletePredicate>,
    pub adj_inserts: &'a mut Vec<AdjEntry>,
    pub adj_deletes: &'a mut Vec<(NodeId, EdgeType, NodeId, TxId)>,
    pub vector_deletes: &'a mut Vec<(VectorIndexRef, RowId, TxId)>,
    pub vector_moves: &'a mut Vec<(VectorIndexRef, RowId, RowId, TxId)>,
    pub sync_source_provenance_marks: &'a mut Vec<(TableName, RowId, Lsn, u8)>,
    pub config_writes: &'a mut Vec<(String, Vec<u8>)>,
    pub config_deletes: &'a mut Vec<String>,
    pub config_max_u64_keys: &'a mut Vec<String>,
    pub requires_commit_lsn: &'a mut bool,
    pub visibility_floor: &'a mut Option<TxId>,
    pub propagation_in_progress: &'a mut bool,
}
#[doc(hidden)]
pub struct EngineGuard<'a> {
    host: &'a mut EngineWriteSet,
    funder: &'a SourceFunder,
    preparation_scope: u64,
}

/// Construction chooses the semantic envelope before the first mutation.
/// It must not be inferred from whether an eager image happens to exist.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnginePreparation {
    Ordinary,
    Eager,
}
impl EngineGuard<'_> {
    fn eager_preparation(&self) -> bool {
        self.host.preparation == EnginePreparation::Eager
            || self
                .host
                .state
                .ws
                .ordered_staged_rows
                .native_semantic_holders()
                .is_some()
    }
    pub fn read(&self) -> EngineRead<'_> {
        EngineRead::from_host(self.host)
    }
    /// Borrow the complete original callback image within this guard only.
    #[doc(hidden)]
    pub fn with_legacy<R>(&self, f: impl for<'w> FnOnce(&'w WriteSet) -> R) -> R {
        f(&self.host.state.ws)
    }
    pub fn non_payload_fields(&mut self) -> EngineNonPayloadFields<'_> {
        assert!(
            self.host.frames.len() == 0,
            "mutable auxiliary bridge is unavailable in a general checkpoint"
        );
        self.host.revision = self
            .host
            .revision
            .checked_add(1)
            .expect("transaction mutation revision");
        EngineNonPayloadFields {
            relational_deletes: &mut self.host.state.ws.relational_deletes,
            relational_delete_predicates: &mut self.host.state.ws.relational_delete_predicates,
            adj_inserts: &mut self.host.state.ws.adj_inserts,
            adj_deletes: &mut self.host.state.ws.adj_deletes,
            vector_deletes: &mut self.host.state.ws.vector_deletes,
            vector_moves: &mut self.host.state.ws.vector_moves,
            sync_source_provenance_marks: &mut self.host.state.ws.sync_source_provenance_marks,
            config_writes: &mut self.host.state.ws.config_writes,
            config_deletes: &mut self.host.state.ws.config_deletes,
            config_max_u64_keys: &mut self.host.state.ws.config_max_u64_keys,
            requires_commit_lsn: &mut self.host.state.ws.requires_commit_lsn,
            visibility_floor: &mut self.host.state.ws.visibility_floor,
            propagation_in_progress: &mut self.host.state.ws.propagation_in_progress,
        }
    }
}

struct TxState {
    ws: WriteSet,
    root: FundedArc<SourceRoot>,
    ordinary_rows_charge: Option<crate::staged_source::SourceCharge>,
    ordinary_vectors_charge: Option<crate::staged_source::SourceCharge>,
    row_table_charges: SourceBuffer<crate::staged_source::SourceCharge>,
    row_debits: SourceBuffer<crate::staged_source::OriginalRowInvoice>,
    vector_debits: SourceBuffer<crate::staged_source::OriginalVectorInvoice>,
    row_slots: SourceBuffer<RowCellHandle>,
    vector_slots: SourceBuffer<VectorCellHandle>,
    relational_deletes_funding: AuxFunding,
    relational_delete_predicates_funding: AuxFunding,
    adj_inserts_funding: AuxFunding,
    adj_deletes_funding: AuxFunding,
    vector_deletes_funding: AuxFunding,
    vector_moves_funding: AuxFunding,
    sync_source_provenance_marks_funding: AuxFunding,
    config_writes_funding: AuxFunding,
    config_deletes_funding: AuxFunding,
    config_max_u64_keys_funding: AuxFunding,
    sidecars_credit: Option<crate::staged_source::SourceCharge>,
}
struct EngineWriteSet {
    state: TxState,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    checkpoint_depth: usize,
    revision: u64,
    next_frame: u64,
    undo: SourceJournal,
    deferred: RetiredCheckpointNodes,
    frames: SourceBuffer<TxFrame>,
    funder: SourceFunder,
    commit_error: Option<Error>,
    preparation: EnginePreparation,
}
impl EngineWriteSet {
    fn new(
        tx: TxId,
        base: PublishedRead,
        funder: &SourceFunder,
        preparation: EnginePreparation,
    ) -> Result<Self> {
        let root = FundedArc::try_new(SourceRoot::default(), funder.clone())?;
        let row_slots = SourceBuffer::with_capacity(0, funder)?;
        let vector_slots = SourceBuffer::with_capacity(0, funder)?;
        let mut ws = WriteSet::new();
        ws.base_read = Some(base);
        Ok(Self {
            state: TxState {
                ws,
                root,
                ordinary_rows_charge: None,
                ordinary_vectors_charge: None,
                row_table_charges: SourceBuffer::empty(),
                row_debits: SourceBuffer::empty(),
                vector_debits: SourceBuffer::empty(),
                row_slots,
                vector_slots,
                relational_deletes_funding: AuxFunding::empty(),
                relational_delete_predicates_funding: AuxFunding::empty(),
                adj_inserts_funding: AuxFunding::empty(),
                adj_deletes_funding: AuxFunding::empty(),
                vector_deletes_funding: AuxFunding::empty(),
                vector_moves_funding: AuxFunding::empty(),
                sync_source_provenance_marks_funding: AuxFunding::empty(),
                config_writes_funding: AuxFunding::empty(),
                config_deletes_funding: AuxFunding::empty(),
                config_max_u64_keys_funding: AuxFunding::empty(),
                sidecars_credit: None,
            },
            tx,
            generation: tx.0,
            preparation_scope: 0,
            checkpoint_depth: 0,
            revision: 0,
            next_frame: 0,
            frames: SourceBuffer::empty(),
            undo: SourceJournal::empty(),
            deferred: RetiredCheckpointNodes::empty(),
            funder: funder.clone(),
            commit_error: None,
            preparation,
        })
    }
    fn open_guard(&mut self) -> Result<u64> {
        self.preparation_scope = self
            .preparation_scope
            .checked_add(1)
            .ok_or_else(source_identity_error)?;
        Ok(self.preparation_scope)
    }
}

/// Detached custody retains the existing funded AVL node and complete ordinary
/// callback image. It never exports a mutable bare WriteSet.
#[doc(hidden)]
pub struct EngineOwnedWriteSet {
    entry: crate::staged_source::PreparedMapEntry<TxId, EngineWriteSet>,
}
impl EngineOwnedWriteSet {
    pub fn with_legacy<R>(&self, f: impl FnOnce(&WriteSet) -> R) -> R {
        f(&self.entry.value().state.ws)
    }
    pub fn with_engine<R>(
        &mut self,
        f: impl FnOnce(&mut EngineGuard<'_>) -> Result<R>,
    ) -> Result<R> {
        let funder = self.entry.value().funder.clone();
        let host = self.entry.value_mut();
        let preparation_scope = host.open_guard()?;
        let drain = DetachedRetirementDrain { host };
        f(&mut EngineGuard {
            host: &mut *drain.host,
            funder: &funder,
            preparation_scope,
        })
    }
    pub fn commit_lsn(&self) -> Option<Lsn> {
        self.entry.value().state.ws.commit_lsn
    }
    pub fn finalized_tx(&self) -> Option<TxId> {
        self.entry.value().state.ws.finalized_tx
    }
    pub fn is_empty(&self) -> bool {
        self.entry.value().state.ws.is_empty()
    }
}
impl std::fmt::Debug for EngineOwnedWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineOwnedWriteSet")
            .field("tx", self.entry.key())
            .field("commit_lsn", &self.commit_lsn())
            .finish_non_exhaustive()
    }
}
#[doc(hidden)]
#[derive(Debug)]
pub struct EngineCommitFailure {
    custody: FailureCustody,
}
#[derive(Debug)]
enum FailureCustody {
    WithoutHost(Error),
    Owned(EngineOwnedWriteSet),
}
impl EngineCommitFailure {
    fn new(error: Error, write_set: Option<EngineOwnedWriteSet>) -> Self {
        let custody = match write_set {
            Some(mut owned) => {
                // The error slot is part of the node's original admitted
                // Layout. Delivery neither allocates nor rebuilds custody.
                assert!(owned.entry.value().commit_error.is_none());
                owned.entry.value_mut().commit_error = Some(error);
                FailureCustody::Owned(owned)
            }
            None => FailureCustody::WithoutHost(error),
        };
        Self { custody }
    }
    pub fn error(&self) -> &Error {
        match &self.custody {
            FailureCustody::WithoutHost(error) => error,
            FailureCustody::Owned(owned) => owned
                .entry
                .value()
                .commit_error
                .as_ref()
                .expect("owned commit error"),
        }
    }
    pub fn with_write_set<R>(&self, f: impl FnOnce(Option<&EngineOwnedWriteSet>) -> R) -> R {
        f(match &self.custody {
            FailureCustody::WithoutHost(_) => None,
            FailureCustody::Owned(owned) => Some(owned),
        })
    }
    pub fn into_parts(self) -> (Error, Option<EngineOwnedWriteSet>) {
        match self.custody {
            FailureCustody::WithoutHost(error) => (error, None),
            FailureCustody::Owned(mut owned) => {
                let error = owned
                    .entry
                    .value_mut()
                    .commit_error
                    .take()
                    .expect("owned commit error");
                (error, Some(owned))
            }
        }
    }
}
/// Dropping a partially consumed drain destroys remaining funded nodes in
/// place; no collection or callback-image allocation is needed.
#[doc(hidden)]
pub struct EngineOwnedDrain {
    active: crate::staged_source::MutableSourceMap<TxId, EngineWriteSet>,
}
impl Iterator for EngineOwnedDrain {
    type Item = EngineOwnedWriteSet;
    fn next(&mut self) -> Option<Self::Item> {
        let tx = self.active.first_key().copied()?;
        Some(EngineOwnedWriteSet {
            entry: self.active.take_entry(&tx).expect("existing drain node"),
        })
    }
}
impl<S: WriteSetApplicator> EngineTransactions<S> {
    fn take_owned(&self, tx: TxId) -> Option<EngineOwnedWriteSet> {
        let mut active = self.manager.active_txs.lock();
        if let Some(host) = active.engine().get(&tx) {
            assert_eq!(
                host.checkpoint_depth, 0,
                "terminal custody requires consumed checkpoints"
            );
        }
        active
            .engine_mut()
            .take_entry(&tx)
            .map(|entry| EngineOwnedWriteSet { entry })
    }
    pub fn rollback_owned(&self, tx: TxId) -> Result<EngineOwnedWriteSet> {
        self.manager.ensure_writable()?;
        let _lock = self.manager.lock_commit_boundary();
        self.ensure_settled(tx)?;
        self.take_owned(tx).ok_or(Error::TxNotFound(tx))
    }
    pub fn drain_owned(&self) -> EngineOwnedDrain {
        if matches!(
            &self.manager.commit_index,
            CommitHistoryCapability::Captured { .. }
        ) {
            return EngineOwnedDrain {
                active: Default::default(),
            };
        }
        let _lock = self.manager.lock_commit_boundary();
        let active = {
            let mut active = self.manager.active_txs.lock();
            active
                .engine_mut()
                .visit_mut(&mut |_, host| {
                    assert_eq!(
                        host.checkpoint_depth, 0,
                        "drain requires consumed checkpoints"
                    );
                    Ok(())
                })
                .expect("validated drain frames");
            std::mem::take(active.engine_mut())
        };
        EngineOwnedDrain { active }
    }
    pub fn with_engine_detached<R>(
        &self,
        tx: TxId,
        f: impl FnOnce(&mut EngineGuard<'_>) -> Result<R>,
    ) -> Result<R> {
        self.manager.ensure_writable()?;
        self.ensure_settled(tx)?;
        let mut owned = self.take_owned(tx).ok_or(Error::TxNotFound(tx))?;
        let result = owned.with_engine(f);
        // Ordinary Result errors reattach the same node, just as the Legacy
        // detached door does. An unwind destroys detached custody in place.
        self.manager
            .active_txs
            .lock()
            .engine_mut()
            .insert_prepared(owned.entry);
        result
    }
    fn owned_failure(&self, tx: TxId, error: Error) -> EngineCommitFailure {
        EngineCommitFailure::new(error, self.take_owned(tx))
    }
    pub fn commit_owned(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
    ) -> std::result::Result<(Lsn, EngineOwnedWriteSet), EngineCommitFailure> {
        self.commit_owned_prepared_and_applied(tx, read_memory, |_| Ok(()), |_, _| {})
    }
    pub fn commit_owned_prepared_and_applied(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        prepare: impl FnOnce(&mut EngineGuard<'_>) -> Result<()>,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, EngineOwnedWriteSet), EngineCommitFailure> {
        self.commit_owned_active_prepare_and_applied(
            tx,
            read_memory,
            || Ok(false),
            |_, _| Ok(()),
            prepare,
            after_apply,
        )
    }
    pub fn commit_owned_active_prepare_and_applied(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: impl FnOnce(Lsn, TxId) -> Result<()>,
        prepare: impl FnOnce(&mut EngineGuard<'_>) -> Result<()>,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, EngineOwnedWriteSet), EngineCommitFailure> {
        self.manager
            .ensure_writable()
            .map_err(|error| EngineCommitFailure::new(error, None))?;
        let _lock = self.manager.lock_commit_boundary();
        if self.manager.active_txs.lock().engine().get(&tx).is_none() {
            return Err(EngineCommitFailure::new(Error::TxNotFound(tx), None));
        }
        {
            let active = self.manager.active_txs.lock();
            if let Some(host) = active.engine().get(&tx) {
                assert_eq!(
                    host.checkpoint_depth, 0,
                    "commit requires consumed checkpoints"
                );
            }
        }
        let required = requires_commit().map_err(|error| self.owned_failure(tx, error))?;
        self.with_engine(tx, |guard| {
            guard.host.state.ws.requires_commit_lsn |= required;
            let canonical = guard.prepare_canonicalize()?;
            guard.publish(canonical);
            Ok(())
        })
        .map_err(|error| self.owned_failure(tx, error))?;
        let (empty, floor) = self
            .with_read(tx, |read| (read.is_empty(), *read.visibility_floor))
            .map_err(|error| self.owned_failure(tx, error))?;
        if empty {
            return Ok((
                self.manager.current_lsn(),
                self.take_owned(tx).expect("checked active tx"),
            ));
        }
        let visibility_tx = self.manager.visibility_tx_for_commit(tx, floor);
        // Identity preparation preserves individual LSNs until actual LSN
        // allocation, matching the ordinary active-prepare sequence.
        self.with_engine(tx, |guard| {
            let identity = guard.prepare_reassignment(tx, visibility_tx)?;
            guard.publish_identity(identity);
            guard.host.state.ws.finalized_tx = Some(visibility_tx);
            Ok(())
        })
        .map_err(|error| self.owned_failure(tx, error))?;
        let lsn = self.manager.next_lsn.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = self.with_engine(tx, |guard| {
            let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
            guard.publish_identity(identity);
            Ok(())
        }) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(self.owned_failure(tx, error));
        }
        if let Err(error) = active_prepare(lsn, visibility_tx) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(self.owned_failure(tx, error));
        }
        let mut owned = self
            .take_owned(tx)
            .ok_or_else(|| EngineCommitFailure::new(Error::TxNotFound(tx), None))?;
        let preparation = owned
            .with_engine(|guard| {
                let canonical = guard.prepare_canonicalize()?;
                guard.publish(canonical);
                let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
                guard.publish_identity(identity);
                guard.host.state.ws.finalized_tx = Some(visibility_tx);
                Ok(())
            })
            .and_then(|()| owned.with_engine(prepare));
        if let Err(error) = preparation {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineCommitFailure::new(error, Some(owned)));
        }
        // The restricted prepare cannot replace ordinary ownership. The
        // manager still owns the finalized identity scalar.
        owned.entry.value_mut().state.ws.finalized_tx = Some(visibility_tx);
        let directory_record = match self
            .manager
            .prepare_engine_directory_record(lsn, visibility_tx)
        {
            Ok(record) => record,
            Err(error) => {
                self.manager.next_lsn.store(lsn, Ordering::SeqCst);
                return Err(EngineCommitFailure::new(error, Some(owned)));
            }
        };
        if let Err(error) = owned.with_legacy(|ws| self.manager.store.apply(ws, read_memory)) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineCommitFailure::new(error, Some(owned)));
        }
        owned.with_legacy(|ws| after_apply(lsn, ws));
        self.manager
            .record_engine_directory(lsn, visibility_tx, directory_record);
        self.manager.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, owned))
    }
    pub fn commit_owned_with_reserved_lsn_callback(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        before_commit: impl FnOnce(Lsn) -> Result<()>,
        prepare: impl FnOnce(&mut EngineGuard<'_>) -> Result<()>,
    ) -> std::result::Result<(Lsn, EngineOwnedWriteSet), EngineCommitFailure> {
        self.manager
            .ensure_writable()
            .map_err(|error| EngineCommitFailure::new(error, None))?;
        let _lock = self.manager.lock_commit_boundary();
        {
            let active = self.manager.active_txs.lock();
            if let Some(host) = active.engine().get(&tx) {
                assert_eq!(
                    host.checkpoint_depth, 0,
                    "commit requires consumed checkpoints"
                );
            }
        }
        let reserved = self.manager.next_lsn.load(Ordering::SeqCst);
        // As on the Legacy scheduler path, failure here leaves the active
        // host installed and returns no callback image.
        before_commit(reserved).map_err(|error| EngineCommitFailure::new(error, None))?;
        let mut owned = self
            .take_owned(tx)
            .ok_or_else(|| EngineCommitFailure::new(Error::TxNotFound(tx), None))?;
        if let Err(error) = owned.with_engine(|guard| {
            let canonical = guard.prepare_canonicalize()?;
            guard.publish(canonical);
            Ok(())
        }) {
            return Err(EngineCommitFailure::new(error, Some(owned)));
        }
        if owned.is_empty() {
            return Ok((self.manager.current_lsn(), owned));
        }
        let visibility_tx = self
            .manager
            .visibility_tx_for_commit(tx, owned.entry.value().state.ws.visibility_floor);
        if let Err(error) = owned.with_engine(|guard| {
            let identity = guard.prepare_reassignment(tx, visibility_tx)?;
            guard.publish_identity(identity);
            guard.host.state.ws.finalized_tx = Some(visibility_tx);
            Ok(())
        }) {
            return Err(EngineCommitFailure::new(error, Some(owned)));
        }
        let lsn = self.manager.next_lsn.fetch_add(1, Ordering::SeqCst);
        debug_assert_eq!(lsn, reserved);
        let preparation = owned
            .with_engine(|guard| {
                let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
                guard.publish_identity(identity);
                Ok(())
            })
            .and_then(|()| owned.with_engine(prepare));
        if let Err(error) = preparation {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineCommitFailure::new(error, Some(owned)));
        }
        owned.entry.value_mut().state.ws.finalized_tx = Some(visibility_tx);
        let directory_record = match self
            .manager
            .prepare_engine_directory_record(lsn, visibility_tx)
        {
            Ok(record) => record,
            Err(error) => {
                self.manager.next_lsn.store(lsn, Ordering::SeqCst);
                return Err(EngineCommitFailure::new(error, Some(owned)));
            }
        };
        if let Err(error) = owned.with_legacy(|ws| self.manager.store.apply(ws, read_memory)) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineCommitFailure::new(error, Some(owned)));
        }
        self.manager
            .record_engine_directory(lsn, visibility_tx, directory_record);
        self.manager.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, owned))
    }
}

/// Original installed identity; publication reassignment never changes it.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineTerminalIdentity {
    pub original_tx: TxId,
    pub generation: u64,
}

/// The original source node is destroyed while peer custody is still held,
/// including field cleanup if source destruction unwinds.
#[doc(hidden)]
pub struct EngineTerminalWriteSet<P: 'static> {
    source: EngineOwnedWriteSet,
    peer: P,
}
impl<P: 'static> EngineTerminalWriteSet<P> {
    /// Move only the original peer; qualified callers must use a pure,
    /// nonallocating and nonpanicking field-move function.
    pub fn map_peer<Q: 'static>(self, move_peer: fn(P) -> Q) -> EngineTerminalWriteSet<Q> {
        let Self { source, peer } = self;
        EngineTerminalWriteSet {
            source,
            peer: move_peer(peer),
        }
    }

    pub fn identity(&self) -> EngineTerminalIdentity {
        EngineTerminalIdentity {
            original_tx: *self.source.entry.key(),
            generation: self.source.entry.value().generation,
        }
    }
    pub fn with_legacy<R>(&self, f: impl for<'w> FnOnce(&'w WriteSet) -> R) -> R {
        self.source.with_legacy(f)
    }
    pub fn with_engine_peer<R>(
        &mut self,
        f: impl for<'g, 'h, 'p> FnOnce(&'g mut EngineGuard<'h>, &'p mut P) -> Result<R>,
    ) -> Result<R> {
        let Self { source, peer } = self;
        source.with_engine(|guard| f(guard, peer))
    }
    pub fn commit_lsn(&self) -> Option<Lsn> {
        self.source.commit_lsn()
    }
    pub fn finalized_tx(&self) -> Option<TxId> {
        self.source.finalized_tx()
    }
    pub fn is_empty(&self) -> bool {
        self.source.is_empty()
    }
}

/// Failure uses the source node's preadmitted error slot and retains the pair.
#[doc(hidden)]
pub struct EngineTerminalFailure<P: 'static> {
    custody: TerminalFailureCustody<P>,
}
enum TerminalFailureCustody<P: 'static> {
    WithoutHost(Error),
    Owned(EngineTerminalWriteSet<P>),
}
impl<P: 'static> EngineTerminalFailure<P> {
    pub fn from_error(error: Error) -> Self {
        Self::new(error, None)
    }
    pub fn map_error(self, replace: impl FnOnce(Error) -> Error) -> Self {
        let (error, pair) = self.into_parts();
        // Keep the pair local through error replacement, including unwind.
        let error = replace(error);
        Self::new(error, pair)
    }
    /// The qualified peer mapper only moves fields; it never admits or clones.
    pub fn map_peer<Q: 'static>(self, move_peer: fn(P) -> Q) -> EngineTerminalFailure<Q> {
        let (error, pair) = self.into_parts();
        EngineTerminalFailure::new(error, pair.map(|pair| pair.map_peer(move_peer)))
    }

    fn new(error: Error, write_set: Option<EngineTerminalWriteSet<P>>) -> Self {
        let custody = match write_set {
            Some(mut owned) => {
                // Vacancy was checked under active_txs before either unlink.
                // No slot replacement, readmission or failure-image allocation.
                owned.source.entry.value_mut().commit_error = Some(error);
                TerminalFailureCustody::Owned(owned)
            }
            None => TerminalFailureCustody::WithoutHost(error),
        };
        Self { custody }
    }
    pub fn error(&self) -> &Error {
        match &self.custody {
            TerminalFailureCustody::WithoutHost(error) => error,
            TerminalFailureCustody::Owned(owned) => owned
                .source
                .entry
                .value()
                .commit_error
                .as_ref()
                .expect("owned terminal commit error"),
        }
    }
    pub fn with_write_set<R>(
        &self,
        f: impl for<'w> FnOnce(Option<&'w EngineTerminalWriteSet<P>>) -> R,
    ) -> R {
        f(match &self.custody {
            TerminalFailureCustody::WithoutHost(_) => None,
            TerminalFailureCustody::Owned(owned) => Some(owned),
        })
    }
    pub fn into_parts(self) -> (Error, Option<EngineTerminalWriteSet<P>>) {
        match self.custody {
            TerminalFailureCustody::WithoutHost(error) => (error, None),
            TerminalFailureCustody::Owned(mut owned) => {
                let error = owned
                    .source
                    .entry
                    .value_mut()
                    .commit_error
                    .take()
                    .expect("owned terminal commit error");
                (error, Some(owned))
            }
        }
    }
}

/// Scalar validation view only; no source or mutable callback image escapes.
#[doc(hidden)]
pub struct EngineTerminalInventory<'a> {
    active: &'a crate::staged_source::MutableSourceMap<TxId, EngineWriteSet>,
}
impl EngineTerminalInventory<'_> {
    pub fn identity(&self, original_tx: TxId) -> Option<EngineTerminalIdentity> {
        self.active
            .get(&original_tx)
            .map(|host| EngineTerminalIdentity {
                original_tx,
                generation: host.generation,
            })
    }
    pub fn len(&self) -> usize {
        self.active.len()
    }
}

/// Source map stays first, retaining all remaining peers until its destruction
/// finishes. The peer owner sorts original links during consume_all and frees
/// remaining links iteratively in its own destructor.
#[doc(hidden)]
pub struct EnginePairedOwnedDrain<Q: 'static, P: 'static> {
    active: crate::staged_source::MutableSourceMap<TxId, EngineWriteSet>,
    peers: Option<Q>,
    take_peer: fn(&mut Q, EngineTerminalIdentity) -> P,
}
impl<Q: 'static, P: 'static> Iterator for EnginePairedOwnedDrain<Q, P> {
    type Item = EngineTerminalWriteSet<P>;
    fn next(&mut self) -> Option<Self::Item> {
        let peers = self.peers.as_mut()?;
        let tx = self.active.first_key().copied()?;
        let entry = self.active.take_entry_nonfallible(&tx)?;
        let identity = EngineTerminalIdentity {
            original_tx: *entry.key(),
            generation: entry.value().generation,
        };
        let peer = (self.take_peer)(peers, identity);
        Some(EngineTerminalWriteSet {
            source: EngineOwnedWriteSet { entry },
            peer,
        })
    }
}

// Diagnostics happen before peer validation and either unlink. Generation is
// read from the installed host, never reconstructed from a visibility TxId.
fn validate_terminal_source(tx: TxId, host: &EngineWriteSet) -> EngineTerminalIdentity {
    assert_eq!(tx, host.tx, "terminal original transaction identity");
    assert_eq!(
        host.checkpoint_depth, 0,
        "terminal custody requires consumed checkpoints"
    );
    assert_eq!(
        host.frames.len(),
        0,
        "terminal custody requires consumed frames"
    );
    assert_eq!(
        host.undo.len(),
        0,
        "terminal custody requires consumed inverses"
    );
    assert!(
        host.undo.active.0.is_none(),
        "terminal custody requires empty inverse links"
    );
    assert!(
        host.commit_error.is_none(),
        "terminal error slot must be vacant"
    );
    assert_eq!(
        host.undo.spare_len, 0,
        "terminal custody requires consumed spare inverses"
    );
    assert!(
        host.undo.spare.0.is_none(),
        "terminal custody requires empty spare inverse links"
    );
    EngineTerminalIdentity {
        original_tx: tx,
        generation: host.generation,
    }
}

fn validate_temporary_source(tx: TxId, host: &EngineWriteSet) -> Result<EngineTerminalIdentity> {
    if tx != host.tx
        || host.checkpoint_depth != 0
        || host.frames.len() != 0
        || host.undo.len() != 0
        || host.undo.active.0.is_some()
        || host.undo.spare_len != 0
        || host.undo.spare.0.is_some()
        || host.commit_error.is_some()
    {
        return Err(source_identity_error());
    }
    Ok(EngineTerminalIdentity {
        original_tx: tx,
        generation: host.generation,
    })
}

fn validate_temporary_source_return(
    identity: EngineTerminalIdentity,
    owned: &EngineOwnedWriteSet,
) -> Result<()> {
    crate::staged_source::validate_original_singleton(&owned.entry)?;
    if *owned.entry.key() != identity.original_tx
        || validate_temporary_source(identity.original_tx, owned.entry.value())? != identity
    {
        return Err(source_identity_error());
    }
    Ok(())
}

/// A same-thread witness borrowing the actual stack-held commit mutex guard.
/// No constructor, manager accessor or root export is exposed.
#[doc(hidden)]
pub struct EngineCommitScope<'b, 'm, S: WriteSetApplicator> {
    transactions: &'m EngineTransactions<S>,
    held: &'b MutexGuard<'m, ()>,
    _pinned: std::marker::PhantomData<*const ()>,
}

impl<S: WriteSetApplicator> EngineCommitScope<'_, '_, S> {
    /// V/U retain only the matching external journal guard, never source or
    /// payload custody. The qualified consume/install functions only move
    /// validated original links, release guards, and cannot panic or allocate.
    /// A/R are owned and cannot retain either HRTB peer/guard borrow.
    pub fn with_engine_peer_detached<V, U, P: 'static, A, R>(
        &self,
        tx: TxId,
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> Result<V>,
        consume_peer: fn(V) -> P,
        before_guard: impl for<'p> FnOnce(&'p mut P) -> Result<A>,
        prepare: impl for<'g, 'h, 'p> FnOnce(&'g mut EngineGuard<'h>, &'p mut P, A) -> Result<R>,
        validate_return: impl for<'p> FnOnce(EngineTerminalIdentity, &'p P) -> Result<U>,
        reinstall_peer: fn(U, P),
    ) -> std::result::Result<R, EngineTerminalFailure<P>> {
        let _held = self.held;
        self.transactions
            .manager
            .ensure_writable()
            .map_err(EngineTerminalFailure::from_error)?;
        let (identity, mut pair) = {
            let mut active = self.transactions.manager.active_txs.lock();
            let sources = active.engine_mut();
            let host = sources
                .get(&tx)
                .ok_or(Error::TxNotFound(tx))
                .map_err(EngineTerminalFailure::from_error)?;
            let identity =
                validate_temporary_source(tx, host).map_err(EngineTerminalFailure::from_error)?;
            if sources.len() == 0 {
                return Err(EngineTerminalFailure::from_error(source_identity_error()));
            }
            // V retains the matching journal lock through source-first unlink.
            let peer_ticket = validate_peer(identity).map_err(EngineTerminalFailure::from_error)?;
            let Some(entry) = sources.take_entry_nonfallible(&tx) else {
                drop(peer_ticket);
                return Err(EngineTerminalFailure::from_error(Error::TxNotFound(tx)));
            };
            let peer = consume_peer(peer_ticket);
            (
                identity,
                EngineTerminalWriteSet {
                    source: EngineOwnedWriteSet { entry },
                    peer,
                },
            )
        };
        // This pipeline owns A. On every refusal its unused closures/captures
        // and A retire before restoration. with_engine retires deferred nodes
        // and the EngineGuard before returning, with this whole pair held.
        let outcome = before_guard(&mut pair.peer).and_then(|owned| {
            pair.with_engine_peer(|guard, peer| {
                guard.with_tx_checkpoint(|guard| prepare(guard, peer, owned))
            })
        });
        let restoration_error = {
            // Both guards are younger than pair, so validation panic unlocks
            // before source-first native pair cleanup.
            let mut active = self.transactions.manager.active_txs.lock();
            let sources = active.engine_mut();
            match validate_return(identity, &pair.peer) {
                Err(error) => error,
                Ok(peer_ticket) => {
                    match validate_temporary_source_return(identity, &pair.source).and_then(|()| {
                        crate::staged_source::prepare_original_reinstall(
                            sources,
                            identity.original_tx,
                        )
                    }) {
                        Err(error) => {
                            drop(peer_ticket);
                            error
                        }
                        Ok(source_ticket) => {
                            // Both current-map and journal tickets have now
                            // succeeded. Nothing fallible follows first move.
                            let EngineTerminalWriteSet { source, peer } = pair;
                            source_ticket.install_nonfallible(source.entry);
                            reinstall_peer(peer_ticket, peer);
                            drop(active);
                            // Success exits here: pair is installed, never a
                            // terminal custody carrier on a normal refusal.
                            return outcome.map_err(EngineTerminalFailure::from_error);
                        }
                    }
                }
            }
        };
        // Only failed return validation reaches here, always with originals.
        // A callback error has precedence over the validation diagnostic.
        let error = match outcome {
            Err(error) => {
                drop(restoration_error);
                error
            }
            Ok(result) => {
                drop(result);
                restoration_error
            }
        };
        Err(EngineTerminalFailure::new(error, Some(pair)))
    }
}

// Caller retains the actual commit boundary and active guard. The validator
// finishes (including capture destruction) before the first root/link move.
// V may borrow only the external peer journal; consume_peer is a qualified,
// noncapturing function that moves its links and releases its guard.
fn take_terminal_from_active<V, P: 'static>(
    active: &mut crate::staged_source::MutableSourceMap<TxId, EngineWriteSet>,
    tx: TxId,
    validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
    consume_peer: fn(V) -> P,
) -> Option<EngineTerminalWriteSet<P>> {
    let host = active.get(&tx)?;
    let identity = validate_terminal_source(tx, host);
    assert!(active.len() != 0, "installed terminal node count");
    let ticket = validate_peer(identity);
    let Some(entry) = active.take_entry_nonfallible(&tx) else {
        // Impossible for an unchanged validated map. No peer unlink here;
        // qualified ticket destruction only releases the external lock.
        drop(ticket);
        return None;
    };
    let peer = consume_peer(ticket);
    Some(EngineTerminalWriteSet {
        source: EngineOwnedWriteSet { entry },
        peer,
    })
}

impl<S: WriteSetApplicator> EngineTransactions<S> {
    fn take_terminal_locked<V, P: 'static>(
        &self,
        tx: TxId,
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> Option<EngineTerminalWriteSet<P>> {
        let mut active = self.manager.active_txs.lock();
        take_terminal_from_active(active.engine_mut(), tx, validate_peer, consume_peer)
    }
    pub fn rollback_owned_with_peer<V, P: 'static>(
        &self,
        tx: TxId,
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> Result<EngineTerminalWriteSet<P>> {
        self.manager.ensure_writable()?;
        let _lock = self.manager.lock_commit_boundary();
        self.ensure_settled(tx)?;
        self.take_terminal_locked(tx, validate_peer, consume_peer)
            .ok_or(Error::TxNotFound(tx))
    }
    pub fn rollback_empty_owned_with_peer<V, P: 'static>(
        &self,
        tx: TxId,
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> Result<EngineTerminalWriteSet<P>> {
        self.manager.ensure_writable()?;
        let _lock = self.manager.lock_commit_boundary();
        let mut active = self.manager.active_txs.lock();
        let sources = active.engine_mut();
        let host = sources.get(&tx).ok_or(Error::TxNotFound(tx))?;
        host.validate_settled()?;
        if !host.state.ws.is_empty() {
            return Err(Error::Other(
                "cannot rollback non-empty transaction without commit lock".into(),
            ));
        }
        take_terminal_from_active(sources, tx, validate_peer, consume_peer)
            .ok_or(Error::TxNotFound(tx))
    }
    pub fn drain_owned_with_peer<V, Q: 'static, P: 'static>(
        &self,
        validate_all: impl for<'i> FnOnce(EngineTerminalInventory<'i>) -> V,
        consume_all: fn(V) -> Q,
        take_peer: fn(&mut Q, EngineTerminalIdentity) -> P,
    ) -> EnginePairedOwnedDrain<Q, P> {
        if matches!(
            &self.manager.commit_index,
            CommitHistoryCapability::Captured { .. }
        ) {
            return EnginePairedOwnedDrain {
                active: Default::default(),
                peers: None,
                take_peer,
            };
        }
        let _lock = self.manager.lock_commit_boundary();
        let mut active = self.manager.active_txs.lock();
        let sources = active.engine_mut();
        sources
            .visit_mut(&mut |tx, host| {
                validate_terminal_source(*tx, host);
                Ok(())
            })
            .expect("validated terminal source inventory");
        let ticket = validate_all(EngineTerminalInventory { active: sources });
        // Both whole owners move under their guards. consume_all is the
        // qualified original-link mergesort kernel, not a callback producer.
        let sources = std::mem::take(sources);
        let peers = consume_all(ticket);
        EnginePairedOwnedDrain {
            active: sources,
            peers: Some(peers),
            take_peer,
        }
    }
    fn terminal_failure<V, P: 'static, F: FnOnce(EngineTerminalIdentity) -> V>(
        &self,
        tx: TxId,
        error: Error,
        validate_peer: &mut Option<F>,
        consume_peer: fn(V) -> P,
    ) -> EngineTerminalFailure<P> {
        // Take the capturing validator before entering the transfer kernel.
        let validate_peer = validate_peer.take().expect("single terminal validation");
        let pair = self.take_terminal_locked(tx, validate_peer, consume_peer);
        EngineTerminalFailure::new(error, pair)
    }
    pub fn commit_owned_active_prepare_and_applied_with_peer<V, P: 'static>(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: impl FnOnce(Lsn, TxId) -> Result<()>,
        prepare: impl for<'g, 'h, 'p> FnOnce(&'g mut EngineGuard<'h>, &'p mut P) -> Result<()>,
        after_apply: impl for<'w> FnOnce(Lsn, &'w WriteSet),
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> std::result::Result<(Lsn, EngineTerminalWriteSet<P>), EngineTerminalFailure<P>> {
        self.manager
            .ensure_writable()
            .map_err(|error| EngineTerminalFailure::new(error, None))?;
        let _lock = self.manager.lock_commit_boundary();
        let mut validate_peer = Some(validate_peer);
        if self.manager.active_txs.lock().engine().get(&tx).is_none() {
            return Err(EngineTerminalFailure::new(Error::TxNotFound(tx), None));
        }
        {
            let active = self.manager.active_txs.lock();
            if let Some(host) = active.engine().get(&tx) {
                assert_eq!(
                    host.checkpoint_depth, 0,
                    "commit requires consumed checkpoints"
                );
            }
        }
        let required = requires_commit()
            .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        self.with_engine(tx, |guard| {
            guard.host.state.ws.requires_commit_lsn |= required;
            let canonical = guard.prepare_canonicalize()?;
            guard.publish(canonical);
            Ok(())
        })
        .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        let (empty, floor) = self
            .with_read(tx, |read| (read.is_empty(), *read.visibility_floor))
            .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        if empty {
            return Ok((
                self.manager.current_lsn(),
                self.take_terminal_locked(
                    tx,
                    validate_peer.take().expect("single terminal validation"),
                    consume_peer,
                )
                .ok_or_else(|| EngineTerminalFailure::new(Error::TxNotFound(tx), None))?,
            ));
        }
        let visibility_tx = self.manager.visibility_tx_for_commit(tx, floor);
        // Identity preparation preserves individual LSNs until actual LSN
        // allocation, matching the ordinary active-prepare sequence.
        self.with_engine(tx, |guard| {
            let identity = guard.prepare_reassignment(tx, visibility_tx)?;
            guard.publish_identity(identity);
            guard.host.state.ws.finalized_tx = Some(visibility_tx);
            Ok(())
        })
        .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        let lsn = self.manager.next_lsn.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = self.with_engine(tx, |guard| {
            let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
            guard.publish_identity(identity);
            Ok(())
        }) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(self.terminal_failure(tx, error, &mut validate_peer, consume_peer));
        }
        if let Err(error) = active_prepare(lsn, visibility_tx) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(self.terminal_failure(tx, error, &mut validate_peer, consume_peer));
        }
        let mut owned = self
            .take_terminal_locked(
                tx,
                validate_peer.take().expect("single terminal validation"),
                consume_peer,
            )
            .ok_or_else(|| EngineTerminalFailure::new(Error::TxNotFound(tx), None))?;
        let preparation = owned
            .source
            .with_engine(|guard| {
                let canonical = guard.prepare_canonicalize()?;
                guard.publish(canonical);
                let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
                guard.publish_identity(identity);
                guard.host.state.ws.finalized_tx = Some(visibility_tx);
                Ok(())
            })
            .and_then(|()| owned.with_engine_peer(prepare));
        if let Err(error) = preparation {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        // The restricted prepare cannot replace ordinary ownership. The
        // manager still owns the finalized identity scalar.
        owned.source.entry.value_mut().state.ws.finalized_tx = Some(visibility_tx);
        let directory_record = match self
            .manager
            .prepare_engine_directory_record(lsn, visibility_tx)
        {
            Ok(record) => record,
            Err(error) => {
                self.manager.next_lsn.store(lsn, Ordering::SeqCst);
                return Err(EngineTerminalFailure::new(error, Some(owned)));
            }
        };
        if let Err(error) = owned.with_legacy(|ws| self.manager.store.apply(ws, read_memory)) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        owned.with_legacy(|ws| after_apply(lsn, ws));
        self.manager
            .record_engine_directory(lsn, visibility_tx, directory_record);
        self.manager.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, owned))
    }
    pub fn commit_owned_with_reserved_lsn_callback_with_peer<V, P: 'static>(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        before_commit: impl FnOnce(Lsn) -> Result<()>,
        prepare: impl for<'g, 'h, 'p> FnOnce(&'g mut EngineGuard<'h>, &'p mut P) -> Result<()>,
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> std::result::Result<(Lsn, EngineTerminalWriteSet<P>), EngineTerminalFailure<P>> {
        self.manager
            .ensure_writable()
            .map_err(|error| EngineTerminalFailure::new(error, None))?;
        let _lock = self.manager.lock_commit_boundary();
        let mut validate_peer = Some(validate_peer);
        {
            let active = self.manager.active_txs.lock();
            if let Some(host) = active.engine().get(&tx) {
                assert_eq!(
                    host.checkpoint_depth, 0,
                    "commit requires consumed checkpoints"
                );
            }
        }
        let reserved = self.manager.next_lsn.load(Ordering::SeqCst);
        // As on the Legacy scheduler path, failure here leaves the active
        // host installed and returns no callback image.
        before_commit(reserved).map_err(|error| EngineTerminalFailure::new(error, None))?;
        let mut owned = self
            .take_terminal_locked(
                tx,
                validate_peer.take().expect("single terminal validation"),
                consume_peer,
            )
            .ok_or_else(|| EngineTerminalFailure::new(Error::TxNotFound(tx), None))?;
        if let Err(error) = owned.source.with_engine(|guard| {
            let canonical = guard.prepare_canonicalize()?;
            guard.publish(canonical);
            Ok(())
        }) {
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        if owned.is_empty() {
            return Ok((self.manager.current_lsn(), owned));
        }
        let visibility_tx = self
            .manager
            .visibility_tx_for_commit(tx, owned.source.entry.value().state.ws.visibility_floor);
        if let Err(error) = owned.source.with_engine(|guard| {
            let identity = guard.prepare_reassignment(tx, visibility_tx)?;
            guard.publish_identity(identity);
            guard.host.state.ws.finalized_tx = Some(visibility_tx);
            Ok(())
        }) {
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        let lsn = self.manager.next_lsn.fetch_add(1, Ordering::SeqCst);
        debug_assert_eq!(lsn, reserved);
        let preparation = owned
            .source
            .with_engine(|guard| {
                let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
                guard.publish_identity(identity);
                Ok(())
            })
            .and_then(|()| owned.with_engine_peer(prepare));
        if let Err(error) = preparation {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        owned.source.entry.value_mut().state.ws.finalized_tx = Some(visibility_tx);
        let directory_record = match self
            .manager
            .prepare_engine_directory_record(lsn, visibility_tx)
        {
            Ok(record) => record,
            Err(error) => {
                self.manager.next_lsn.store(lsn, Ordering::SeqCst);
                return Err(EngineTerminalFailure::new(error, Some(owned)));
            }
        };
        if let Err(error) = owned.with_legacy(|ws| self.manager.store.apply(ws, read_memory)) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        self.manager
            .record_engine_directory(lsn, visibility_tx, directory_record);
        self.manager.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, owned))
    }
}

impl<S: WriteSetApplicator> EngineTransactions<S> {
    pub fn commit_owned_active_prepare_and_applied_with_peer_scoped<V, P: 'static>(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: impl for<'b, 'm> FnOnce(
            &EngineCommitScope<'b, 'm, S>,
            Lsn,
            TxId,
        )
            -> std::result::Result<(), EngineTerminalFailure<P>>,
        prepare: impl for<'g, 'h, 'p> FnOnce(&'g mut EngineGuard<'h>, &'p mut P) -> Result<()>,
        after_apply: impl for<'w> FnOnce(Lsn, &'w WriteSet),
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> std::result::Result<(Lsn, EngineTerminalWriteSet<P>), EngineTerminalFailure<P>> {
        self.manager
            .ensure_writable()
            .map_err(|error| EngineTerminalFailure::new(error, None))?;
        let _lock = self.manager.lock_commit_boundary();
        let scope = EngineCommitScope {
            transactions: self,
            held: _lock.lock.as_ref().expect("held commit boundary"),
            _pinned: std::marker::PhantomData,
        };
        let mut validate_peer = Some(validate_peer);
        if self.manager.active_txs.lock().engine().get(&tx).is_none() {
            return Err(EngineTerminalFailure::new(Error::TxNotFound(tx), None));
        }
        {
            let active = self.manager.active_txs.lock();
            if let Some(host) = active.engine().get(&tx) {
                assert_eq!(
                    host.checkpoint_depth, 0,
                    "commit requires consumed checkpoints"
                );
            }
        }
        let required = requires_commit()
            .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        self.with_engine(tx, |guard| {
            guard.host.state.ws.requires_commit_lsn |= required;
            let canonical = guard.prepare_canonicalize()?;
            guard.publish(canonical);
            Ok(())
        })
        .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        let (empty, floor) = self
            .with_read(tx, |read| (read.is_empty(), *read.visibility_floor))
            .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        if empty {
            return Ok((
                self.manager.current_lsn(),
                self.take_terminal_locked(
                    tx,
                    validate_peer.take().expect("single terminal validation"),
                    consume_peer,
                )
                .ok_or_else(|| EngineTerminalFailure::new(Error::TxNotFound(tx), None))?,
            ));
        }
        let visibility_tx = self.manager.visibility_tx_for_commit(tx, floor);
        // Identity preparation preserves individual LSNs until actual LSN
        // allocation, matching the ordinary active-prepare sequence.
        self.with_engine(tx, |guard| {
            let identity = guard.prepare_reassignment(tx, visibility_tx)?;
            guard.publish_identity(identity);
            guard.host.state.ws.finalized_tx = Some(visibility_tx);
            Ok(())
        })
        .map_err(|error| self.terminal_failure(tx, error, &mut validate_peer, consume_peer))?;
        let lsn = self.manager.next_lsn.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = self.with_engine(tx, |guard| {
            let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
            guard.publish_identity(identity);
            Ok(())
        }) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(self.terminal_failure(tx, error, &mut validate_peer, consume_peer));
        }
        if let Err(failure) = active_prepare(&scope, lsn, visibility_tx) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            if failure.with_write_set(|pair| pair.is_some()) {
                return Err(failure);
            }
            let (error, pair) = failure.into_parts();
            drop(pair); // None: the temporary pass restored installed custody.
            return Err(self.terminal_failure(tx, error, &mut validate_peer, consume_peer));
        }
        let mut owned = self
            .take_terminal_locked(
                tx,
                validate_peer.take().expect("single terminal validation"),
                consume_peer,
            )
            .ok_or_else(|| EngineTerminalFailure::new(Error::TxNotFound(tx), None))?;
        let preparation = owned
            .source
            .with_engine(|guard| {
                let canonical = guard.prepare_canonicalize()?;
                guard.publish(canonical);
                let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
                guard.publish_identity(identity);
                guard.host.state.ws.finalized_tx = Some(visibility_tx);
                Ok(())
            })
            .and_then(|()| owned.with_engine_peer(prepare));
        if let Err(error) = preparation {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        // The restricted prepare cannot replace ordinary ownership. The
        // manager still owns the finalized identity scalar.
        owned.source.entry.value_mut().state.ws.finalized_tx = Some(visibility_tx);
        let directory_record = match self
            .manager
            .prepare_engine_directory_record(lsn, visibility_tx)
        {
            Ok(record) => record,
            Err(error) => {
                self.manager.next_lsn.store(lsn, Ordering::SeqCst);
                return Err(EngineTerminalFailure::new(error, Some(owned)));
            }
        };
        if let Err(error) = owned.with_legacy(|ws| self.manager.store.apply(ws, read_memory)) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        owned.with_legacy(|ws| after_apply(lsn, ws));
        self.manager
            .record_engine_directory(lsn, visibility_tx, directory_record);
        self.manager.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, owned))
    }
    pub fn commit_owned_with_reserved_lsn_callback_with_peer_scoped<V, P: 'static>(
        &self,
        tx: TxId,
        read_memory: Arc<dyn ReadMemoryBudget>,
        before_commit: impl for<'b, 'm> FnOnce(
            &EngineCommitScope<'b, 'm, S>,
            Lsn,
        )
            -> std::result::Result<(), EngineTerminalFailure<P>>,
        prepare: impl for<'g, 'h, 'p> FnOnce(&'g mut EngineGuard<'h>, &'p mut P) -> Result<()>,
        validate_peer: impl FnOnce(EngineTerminalIdentity) -> V,
        consume_peer: fn(V) -> P,
    ) -> std::result::Result<(Lsn, EngineTerminalWriteSet<P>), EngineTerminalFailure<P>> {
        self.manager
            .ensure_writable()
            .map_err(|error| EngineTerminalFailure::new(error, None))?;
        let _lock = self.manager.lock_commit_boundary();
        let scope = EngineCommitScope {
            transactions: self,
            held: _lock.lock.as_ref().expect("held commit boundary"),
            _pinned: std::marker::PhantomData,
        };
        let mut validate_peer = Some(validate_peer);
        {
            let active = self.manager.active_txs.lock();
            if let Some(host) = active.engine().get(&tx) {
                assert_eq!(
                    host.checkpoint_depth, 0,
                    "commit requires consumed checkpoints"
                );
            }
        }
        let reserved = self.manager.next_lsn.load(Ordering::SeqCst);
        // No LSN has been allocated. Return the carrier unchanged: a normal
        // refusal has restored both nodes; exceptional custody stays detached.
        before_commit(&scope, reserved)?;
        let mut owned = self
            .take_terminal_locked(
                tx,
                validate_peer.take().expect("single terminal validation"),
                consume_peer,
            )
            .ok_or_else(|| EngineTerminalFailure::new(Error::TxNotFound(tx), None))?;
        if let Err(error) = owned.source.with_engine(|guard| {
            let canonical = guard.prepare_canonicalize()?;
            guard.publish(canonical);
            Ok(())
        }) {
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        if owned.is_empty() {
            return Ok((self.manager.current_lsn(), owned));
        }
        let visibility_tx = self
            .manager
            .visibility_tx_for_commit(tx, owned.source.entry.value().state.ws.visibility_floor);
        if let Err(error) = owned.source.with_engine(|guard| {
            let identity = guard.prepare_reassignment(tx, visibility_tx)?;
            guard.publish_identity(identity);
            guard.host.state.ws.finalized_tx = Some(visibility_tx);
            Ok(())
        }) {
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        let lsn = self.manager.next_lsn.fetch_add(1, Ordering::SeqCst);
        debug_assert_eq!(lsn, reserved);
        let preparation = owned
            .source
            .with_engine(|guard| {
                let identity = guard.prepare_identity(tx, visibility_tx, lsn)?;
                guard.publish_identity(identity);
                Ok(())
            })
            .and_then(|()| owned.with_engine_peer(prepare));
        if let Err(error) = preparation {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        owned.source.entry.value_mut().state.ws.finalized_tx = Some(visibility_tx);
        let directory_record = match self
            .manager
            .prepare_engine_directory_record(lsn, visibility_tx)
        {
            Ok(record) => record,
            Err(error) => {
                self.manager.next_lsn.store(lsn, Ordering::SeqCst);
                return Err(EngineTerminalFailure::new(error, Some(owned)));
            }
        };
        if let Err(error) = owned.with_legacy(|ws| self.manager.store.apply(ws, read_memory)) {
            self.manager.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(EngineTerminalFailure::new(error, Some(owned)));
        }
        self.manager
            .record_engine_directory(lsn, visibility_tx, directory_record);
        self.manager.publish_read_snapshot(lsn, Some(visibility_tx));
        Ok((lsn, owned))
    }
}

/// Restricted facade over the same commit mutex, counters, store and directory.
/// It has no Deref, inner-manager getter, or mutable ordinary-write callback.
#[doc(hidden)]
pub struct EngineTransactions<S: WriteSetApplicator> {
    manager: TransactionManager<S>,
    funder: Option<SourceFunder>,
    preparation: EnginePreparation,
}
impl<S: WriteSetApplicator> EngineTransactions<S> {
    fn install(
        mut manager: TransactionManager<S>,
        funder: Option<SourceFunder>,
        preparation: EnginePreparation,
    ) -> Self {
        *manager.active_txs.get_mut() = ActiveTransactions::Engine(Default::default());
        Self {
            manager,
            funder,
            preparation,
        }
    }
    pub fn new(store: S, funder: Arc<dyn SourceFunding>) -> Self {
        Self::new_funded(store, SourceFunder::from_host(funder))
    }
    pub fn new_funded(store: S, funder: SourceFunder) -> Self {
        Self::new_funded_with_preparation(store, funder, EnginePreparation::Ordinary)
    }

    /// Engine construction is fallible because its release and in-memory
    /// history controls are admitted before any native allocation occurs.
    pub fn try_new_funded(store: S, funder: SourceFunder) -> Result<Self> {
        Self::try_new_funded_with_preparation(store, funder, EnginePreparation::Ordinary)
    }

    pub fn try_new_eager_funded(store: S, funder: SourceFunder) -> Result<Self> {
        Self::try_new_funded_with_preparation(store, funder, EnginePreparation::Eager)
    }

    fn try_new_funded_with_preparation(
        store: S,
        funder: SourceFunder,
        preparation: EnginePreparation,
    ) -> Result<Self> {
        let release = CommitReleaseHandle::try_new(&funder)?;
        let directory = EngineMemoryCommitDirectory::try_new(funder.clone())?;
        Ok(Self {
            manager: TransactionManager::new_with_engine_parts(
                store,
                TxId(1),
                Lsn(1),
                TxId(0),
                CommitHistoryCapability::EngineMemory(directory),
                None,
                release,
            ),
            funder: Some(funder),
            preparation,
        })
    }
    #[doc(hidden)]
    pub fn new_funded_with_preparation(
        store: S,
        funder: SourceFunder,
        preparation: EnginePreparation,
    ) -> Self {
        Self::install(TransactionManager::new(store), Some(funder), preparation)
    }
    pub fn new_with_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        directory: Arc<dyn CommitDirectory>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
        funder: Arc<dyn SourceFunding>,
    ) -> Self {
        Self::new_with_funded_directory(
            store,
            next_tx,
            next_lsn,
            committed_watermark,
            directory,
            handle,
            SourceFunder::from_host(funder),
        )
    }
    pub fn new_with_funded_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        directory: Arc<dyn CommitDirectory>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
        funder: SourceFunder,
    ) -> Self {
        Self::install(
            TransactionManager::new_with_directory(
                store,
                next_tx,
                next_lsn,
                committed_watermark,
                directory,
                handle,
            ),
            Some(funder),
            EnginePreparation::Ordinary,
        )
    }

    pub fn try_new_with_funded_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        directory: Arc<dyn CommitDirectory>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
        funder: SourceFunder,
    ) -> Result<Self> {
        let release = CommitReleaseHandle::try_new(&funder)?;
        Ok(Self {
            manager: TransactionManager::new_with_engine_parts(
                store,
                next_tx,
                next_lsn,
                committed_watermark,
                CommitHistoryCapability::Writable(directory),
                handle,
                release,
            ),
            funder: Some(funder),
            preparation: EnginePreparation::Ordinary,
        })
    }

    pub fn try_new_eager_with_funded_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        directory: Arc<dyn CommitDirectory>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
        funder: SourceFunder,
    ) -> Result<Self> {
        let mut transactions = Self::try_new_with_funded_directory(
            store,
            next_tx,
            next_lsn,
            committed_watermark,
            directory,
            handle,
            funder,
        )?;
        transactions.preparation = EnginePreparation::Eager;
        Ok(transactions)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_captured_directory(
        store: S,
        next_tx: TxId,
        next_lsn: Lsn,
        committed_watermark: TxId,
        committed_lsn: Lsn,
        directory: Arc<dyn ReadCommitDirectory>,
        image_read_memory: Arc<dyn ReadMemoryBudget>,
        handle: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self::install(
            TransactionManager::new_with_captured_directory(
                store,
                next_tx,
                next_lsn,
                committed_watermark,
                committed_lsn,
                directory,
                image_read_memory,
                handle,
            ),
            None,
            EnginePreparation::Ordinary,
        )
    }
    pub fn try_begin(&self) -> Result<TxId> {
        self.manager.ensure_writable()?;
        let funder = self.funder.as_ref().expect("writer source funder");
        let tx = self.manager.next_tx.fetch_add(1, Ordering::SeqCst);
        let host = EngineWriteSet::new(
            tx,
            self.manager.capture_published_read(),
            funder,
            self.preparation,
        )?;
        let entry = crate::staged_source::MutableSourceMap::prepare(tx, host, funder)?;
        self.manager
            .active_txs
            .lock()
            .engine_mut()
            .insert_prepared(entry);
        Ok(tx)
    }
    pub fn transaction_read(&self, tx: TxId) -> Result<PublishedRead> {
        self.with_read(tx, |read| read.base_read.clone())?
            .ok_or_else(|| Error::Other("transaction has no captured read view".into()))
    }

    #[doc(hidden)]
    pub fn row_shadows_committed(&self, tx: TxId, table: &str, row: RowId) -> Result<bool> {
        let active = self.manager.active_txs.lock();
        let host = active.engine().get(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(host.state.root.table(table).is_some_and(|root| {
            crate::staged_source::get_by(&root.by_id, |id| row.cmp(id)).is_some()
        }) || host
            .state
            .ws
            .relational_deletes
            .iter()
            .any(|(name, id, _)| name == table && *id == row))
    }
    pub fn with_read<R>(&self, tx: TxId, f: impl FnOnce(EngineRead<'_>) -> R) -> Result<R> {
        let active = self.manager.active_txs.lock();
        let host = active.engine().get(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(f(EngineRead::from_host(host)))
    }
    pub fn with_engine<R>(
        &self,
        tx: TxId,
        f: impl FnOnce(&mut EngineGuard<'_>) -> Result<R>,
    ) -> Result<R> {
        self.manager.ensure_writable()?;
        let mut active = EngineRetirementLock {
            active: Some(self.manager.active_txs.lock()),
            tx: Some(tx),
        };
        let host = active
            .engine_mut()
            .get_mut(&tx)
            .ok_or(Error::TxNotFound(tx))?;
        let preparation_scope = host.open_guard()?;
        f(&mut EngineGuard {
            host,
            funder: self.funder.as_ref().expect("writer source funder"),
            preparation_scope,
        })
    }
    pub fn visit_active(
        &self,
        mut f: impl FnMut(TxId, &mut EngineGuard<'_>) -> Result<()>,
    ) -> Result<()> {
        self.manager.ensure_writable()?;
        let mut active = EngineRetirementLock {
            active: Some(self.manager.active_txs.lock()),
            tx: None,
        };
        let funder = self.funder.as_ref().expect("writer source funder");
        active.engine_mut().visit_mut(&mut |tx, host| {
            let preparation_scope = host.open_guard()?;
            f(
                *tx,
                &mut EngineGuard {
                    host,
                    funder,
                    preparation_scope,
                },
            )
        })
    }
    pub fn advance_for_sync(&self, tx: TxId, table: &str, incoming: TxId) -> Result<()> {
        self.manager.ensure_writable()?;
        if incoming.0 == u64::MAX {
            return Err(Error::TxIdOverflow {
                table: table.to_owned(),
                incoming: u64::MAX,
            });
        }
        self.manager
            .next_tx
            .fetch_max(TxId(incoming.0 + 1), Ordering::SeqCst);
        self.with_engine(tx, |guard| {
            let mut flags = EngineFlags::from_ws(&guard.host.state.ws);
            flags.visibility_floor = Some(flags.visibility_floor.unwrap_or(incoming).max(incoming));
            let prepared = guard.prepare_flags(flags)?;
            guard.publish_tx(prepared);
            Ok(())
        })
    }
    pub fn rollback_empty_without_commit_lock(&self, tx: TxId) -> Result<()> {
        self.manager.ensure_writable()?;
        let mut active = self.manager.active_txs.lock();
        let host = active.engine().get(&tx).ok_or(Error::TxNotFound(tx))?;
        host.validate_settled()?;
        if !host.state.ws.is_empty() {
            return Err(Error::Other(
                "cannot rollback non-empty transaction without commit lock".into(),
            ));
        }
        drop(active.engine_mut().remove(&tx));
        Ok(())
    }
}
impl<S: WriteSetApplicator + 'static> EngineTransactions<S> {
    pub fn capture(
        self: &Arc<Self>,
        tx: TxId,
        table: &str,
        reader: Arc<dyn ReadMemoryBudget>,
    ) -> Result<CanonicalStatementRows> {
        let active = self.manager.active_txs.lock();
        let host = active.engine().get(&tx).ok_or(Error::TxNotFound(tx))?;
        let root = host.state.root.table(table).cloned();
        let base = host
            .state
            .ws
            .base_read
            .clone()
            .ok_or_else(|| Error::Other("transaction has no captured read view".into()))?;
        CanonicalStatementRows::capture(
            root,
            CapturedIdentity {
                tx,
                generation: host.generation,
                base,
            },
            CaptureResolver::Host(self.clone()),
            reader,
        )
    }

    /// Capture only immutable sealed cells. Unlike the legacy capture door,
    /// this descriptor retains no manager Arc: cell identity plus sealed
    /// custody are sufficient after the active host has detached or closed.
    #[doc(hidden)]
    pub fn capture_sealed(
        &self,
        tx: TxId,
        table: &str,
        reader: Arc<dyn ReadMemoryBudget>,
    ) -> Result<CanonicalStatementRows> {
        let active = self.manager.active_txs.lock();
        let host = active.engine().get(&tx).ok_or(Error::TxNotFound(tx))?;
        let root = host.state.root.table(table).cloned();
        let base = host
            .state
            .ws
            .base_read
            .clone()
            .ok_or_else(|| Error::Other("transaction has no captured read view".into()))?;
        CanonicalStatementRows::capture(
            root,
            CapturedIdentity {
                tx,
                generation: host.generation,
                base,
            },
            CaptureResolver::Sealed,
            reader,
        )
    }
    /// Select canonical owners at the same protected frontier as the resolver.
    #[doc(hidden)]
    pub fn capture_vectors(
        &self,
        tx: TxId,
        index: &contextdb_core::VectorIndexRef,
        reader: Arc<dyn ReadMemoryBudget>,
    ) -> Result<crate::staged_source::CanonicalStatementVectors> {
        let active = self.manager.active_txs.lock();
        let host = active.engine().get(&tx).ok_or(Error::TxNotFound(tx))?;
        if host.state.vector_slots.len() != host.state.ws.vector_inserts.len() {
            return Err(source_identity_error());
        }
        let base = host
            .state
            .ws
            .base_read
            .clone()
            .ok_or_else(source_identity_error)?;
        let source = CanonicalStatementRows::capture(
            host.state.root.table(&index.table).cloned(),
            CapturedIdentity {
                tx,
                generation: host.generation,
                base,
            },
            CaptureResolver::Sealed,
            reader.clone(),
        )?;
        let root = host.state.root.vector(&index.table, &index.column);
        let selected = |position: usize, entry: &VectorEntry| {
            if entry.index != *index {
                return None;
            }
            let root = root?;
            let vector = crate::staged_source::get_by(&root.rows, |id| entry.row_id.cmp(id))?;
            let slot = host.state.vector_slots.as_slice().get(position)?;
            vector.owner.ptr_eq(slot).then_some(vector)
        };
        let count = host
            .state
            .ws
            .vector_inserts
            .iter()
            .enumerate()
            .filter(|(position, entry)| selected(*position, entry).is_some())
            .count();
        let bytes = count
            .checked_mul(std::mem::size_of::<crate::staged_source::CanonicalVectorRef>())
            .ok_or_else(source_identity_error)?;
        let credit = contextdb_core::read_memory::ReadCredit::try_new(
            reader,
            bytes,
            "writer_source",
            "statement_vector_capture",
            "Release statement captures or raise READ_MEMORY_LIMIT.",
        )?;
        // The buffer owns no writer data. Each selected native count retains its
        // existing writer debit; the reader pays only its selection storage.
        let mut vectors = Vec::with_capacity(count);
        for (position, entry) in host.state.ws.vector_inserts.iter().enumerate() {
            if let Some(vector) = selected(position, entry) {
                vectors.push(vector.clone());
            }
        }
        Ok(crate::staged_source::CanonicalStatementVectors {
            vectors,
            source,
            _credit: credit,
        })
    }
}

impl<S: WriteSetApplicator> EngineTransactions<S> {
    fn matches_funder(&self, funder: &SourceFunder) -> bool {
        self.funder
            .as_ref()
            .is_some_and(|installed| installed.same_receiver(funder))
    }

    fn has_owned_nested_controls(&self) -> bool {
        matches!(&self.manager.commit_release, CommitReleaseHandle::Owned(_))
            && matches!(
                &self.manager.commit_index,
                CommitHistoryCapability::EngineMemory(_) | CommitHistoryCapability::Writable(_)
            )
    }
}

/// Opaque lower engine owner.  The only exposed operations borrow the
/// restricted facade; neither the manager, its `Arc`, a `Weak`, nor a raw
/// funding receiver can escape.
#[doc(hidden)]
pub struct EngineTransactionHandle<S: WriteSetApplicator> {
    owner: FundedArc<EngineTransactions<S>>,
}

impl<S: WriteSetApplicator> EngineTransactionHandle<S> {
    pub fn try_new(transactions: EngineTransactions<S>, funder: &SourceFunder) -> Result<Self> {
        if !transactions.matches_funder(funder) || !transactions.has_owned_nested_controls() {
            return Err(Error::InvalidStateTransition(
                "engine transaction handle requires native funded controls from the same receiver"
                    .into(),
            ));
        }
        let charge = FundedArc::<EngineTransactions<S>>::prepare(funder.clone())?;
        Ok(Self {
            owner: FundedArc::from_prepared(transactions, charge),
        })
    }

    pub fn matches_funder(&self, funder: &SourceFunder) -> bool {
        self.owner.matches_funder(funder)
    }

    pub fn with_transactions<'h, R>(&'h self, f: impl FnOnce(&'h EngineTransactions<S>) -> R) -> R {
        f(&self.owner)
    }

    pub fn identity(&self) -> usize {
        self.with_transactions(|transactions| {
            &transactions.manager as *const TransactionManager<S> as usize
        })
    }
}

impl<S: WriteSetApplicator> Clone for EngineTransactionHandle<S> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}

impl<S: WriteSetApplicator> SourceResolver for EngineTransactions<S> {
    fn with_cell(
        &self,
        source: &CapturedIdentity,
        cell: &RowCellHandle,
        f: &mut dyn FnMut(&VersionedRow) -> Result<()>,
    ) -> Result<()> {
        with_sealed_cell(source, cell, f)
    }
    fn with_vector_cell(
        &self,
        source: &CapturedIdentity,
        cell: &VectorCellHandle,
        f: &mut dyn FnMut(&VectorEntry) -> Result<()>,
    ) -> Result<()> {
        with_sealed_vector_cell(source, cell, f)
    }
}
fn source_identity_error() -> Error {
    Error::Other("captured staged source identity mismatch".into())
}

impl<S: WriteSetApplicator> EngineTransactions<S> {
    #[doc(hidden)]
    pub fn capture_published_read(&self) -> PublishedRead {
        self.manager.capture_published_read()
    }
    #[doc(hidden)]
    pub fn published_read_snapshot(&self) -> (SnapshotId, Lsn) {
        self.manager.published_read_snapshot()
    }
    #[doc(hidden)]
    pub fn snapshot(&self) -> SnapshotId {
        self.manager.snapshot()
    }
    #[doc(hidden)]
    pub fn current_lsn(&self) -> Lsn {
        self.manager.current_lsn()
    }
    #[doc(hidden)]
    pub fn current_tx_max(&self) -> TxId {
        self.manager.current_tx_max()
    }
    #[doc(hidden)]
    pub fn peek_next_tx(&self) -> TxId {
        self.manager.peek_next_tx()
    }
    #[doc(hidden)]
    pub fn store(&self) -> &S {
        self.manager.store()
    }
    #[doc(hidden)]
    pub fn release_publication_handle(&self) {
        self.manager.release_publication_handle()
    }
    #[doc(hidden)]
    pub fn read_commit_directory_cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        self.manager
            .read_commit_directory_cursor(lower, upper, context)
    }
    #[doc(hidden)]
    pub fn try_commit_index_prefix(&self, up_to: Lsn) -> Result<BTreeMap<Lsn, TxId>> {
        self.manager.try_commit_index_prefix(up_to)
    }
    #[doc(hidden)]
    pub fn commit_index_snapshot(&self) -> BTreeMap<Lsn, TxId> {
        self.manager.commit_index_snapshot()
    }
    #[doc(hidden)]
    pub fn commit_index_len(&self) -> usize {
        self.manager.commit_index_len()
    }
    #[doc(hidden)]
    pub fn try_commit_index_census(&self) -> Result<(usize, Lsn)> {
        self.manager.try_commit_index_census()
    }
    #[doc(hidden)]
    pub fn try_commit_index_census_with_context(
        &self,
        context: &ReadCommitContext<'_>,
    ) -> Result<(usize, Lsn)> {
        self.manager.try_commit_index_census_with_context(context)
    }
    #[doc(hidden)]
    pub fn try_commit_index_contains(&self, lsn: Lsn) -> Result<bool> {
        self.manager.try_commit_index_contains(lsn)
    }
    #[doc(hidden)]
    pub fn try_commit_index_contains_with_context(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
    ) -> Result<bool> {
        self.manager
            .try_commit_index_contains_with_context(lsn, context)
    }
    #[doc(hidden)]
    pub fn try_commit_index_floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>> {
        self.manager.try_commit_index_floor(upper)
    }
    #[doc(hidden)]
    pub fn try_commit_index_floor_with_context(
        &self,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        self.manager
            .try_commit_index_floor_with_context(upper, context)
    }
    #[doc(hidden)]
    pub fn try_commit_index_get_with_context(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
    ) -> Result<Option<TxId>> {
        self.manager.try_commit_index_get_with_context(lsn, context)
    }
    #[doc(hidden)]
    pub fn try_snapshot_at_lsn_with_context(
        &self,
        lsn: Lsn,
        context: &ReadCommitContext<'_>,
    ) -> Result<SnapshotId> {
        self.manager.try_snapshot_at_lsn_with_context(lsn, context)
    }
    #[doc(hidden)]
    pub fn snapshot_at_lsn(&self, lsn: Lsn) -> SnapshotId {
        self.manager.snapshot_at_lsn(lsn)
    }
    #[doc(hidden)]
    pub fn try_remove_commit_index_entries(&self, lsns: &[Lsn]) -> Result<u64> {
        self.manager.try_remove_commit_index_entries(lsns)
    }
    #[doc(hidden)]
    pub fn allocate_ddl_lsn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        self.manager.allocate_ddl_lsn(f)
    }
    #[doc(hidden)]
    pub fn allocate_ddl_lsn_maybe<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(Lsn) -> Result<Option<R>>,
    {
        self.manager.allocate_ddl_lsn_maybe(f)
    }
    #[doc(hidden)]
    pub fn with_commit_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.manager.with_commit_lock(f)
    }
    #[doc(hidden)]
    pub fn try_with_commit_lock_for<F, R>(&self, duration: std::time::Duration, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        self.manager.try_with_commit_lock_for(duration, f)
    }
    #[doc(hidden)]
    pub fn try_with_commit_lock_cancellable_for<F, R, D>(
        &self,
        duration: Duration,
        cancelled: &OwnerReadCancellation,
        deadline_expired: D,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce() -> R,
        D: Fn() -> bool,
    {
        self.manager
            .try_with_commit_lock_cancellable_for(duration, cancelled, deadline_expired, f)
    }
    #[doc(hidden)]
    pub fn with_maintenance_publication<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'guard> FnOnce(&mut MaintenancePublication<'guard, S>) -> Result<R>,
    {
        self.manager.with_maintenance_publication(f)
    }
    #[doc(hidden)]
    pub fn commit_boundary_held_by_current_thread(&self) -> bool {
        self.manager.commit_boundary_held_by_current_thread()
    }
    #[doc(hidden)]
    pub fn reserve_deferred_metadata_publication(&self) -> Result<Option<Box<dyn Any + Send>>> {
        self.manager.reserve_deferred_metadata_publication()
    }
    #[doc(hidden)]
    pub fn owe_metadata_publication(&self, reservation: Option<Box<dyn Any + Send>>) {
        self.manager.owe_metadata_publication(reservation)
    }
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn write_set_lock_is_held_for_test(&self) -> bool {
        self.manager.active_txs.try_lock().is_none()
    }
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn has_captured_commit_directory_for_test(&self) -> bool {
        self.manager.has_captured_commit_directory_for_test()
    }
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn visit_captured_read_memory_for_test(
        &self,
        visit: impl FnOnce(&Arc<dyn ReadMemoryBudget>),
    ) {
        self.manager.visit_captured_read_memory_for_test(visit)
    }
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn retained_commit_directory_bytes_for_test(&self) -> usize {
        self.manager.retained_commit_directory_bytes_for_test()
    }
}

impl EngineGuard<'_> {
    pub fn prepare_schema(
        &self,
        table: &str,
        meta: &contextdb_core::TableMeta,
    ) -> Result<crate::staged_source::SchemaHandle> {
        if let Some(root) = self.host.state.root.table(table)
            && let Some((_, cell)) =
                crate::staged_source::seek_by(&root.by_id, false, |_| std::cmp::Ordering::Equal)
            && cell.identity.schema.matches_all_meta(meta)
        {
            return Ok(cell.identity.schema.clone());
        }
        crate::staged_source::SourceSchema::from_meta(meta, self.funder)
    }
    pub fn with_captured<R>(
        &self,
        source: &CanonicalStatementRows,
        id: RowId,
        f: impl for<'r> FnOnce(Option<&'r VersionedRow>) -> Result<R>,
    ) -> Result<R> {
        if source.identity().tx != self.host.tx
            || source.identity().generation != self.host.generation
            || !self
                .host
                .state
                .ws
                .base_read
                .as_ref()
                .is_some_and(|base| same_published_source(base, &source.identity().base))
        {
            return Err(source_identity_error());
        }
        let Some(cell) = source.cell(id) else {
            return f(None);
        };
        if cell.identity.tx != self.host.tx
            || cell.identity.generation != source.identity().generation
            || !same_published_source(&cell.identity.base, &source.identity().base)
        {
            return Err(source_identity_error());
        }
        let custody = cell.custody.lock();
        match custody.as_ref() {
            Some(RowCustody::Sealed { row, .. }) => {
                if !cell.identity.matches_row(row) {
                    return Err(source_identity_error());
                }
                f(Some(row))
            }
            None => Err(source_identity_error()),
        }
    }
}

struct PreparedOrdinaryRows {
    values: Vec<(String, VersionedRow)>,
    charge: Option<crate::staged_source::SourceCharge>,
}
struct PreparedRowMutation {
    // On refusal/unwind the original body dies before every invoice holder.
    row: crate::staged_source::AdmittedSourceRow,
    table: String,
    table_charge: crate::staged_source::SourceCharge,
    cell: RowCellHandle,
    root: FundedArc<SourceRoot>,
    expected_root: FundedArc<SourceRoot>,
    ordinary: Option<PreparedOrdinaryRows>,
    slots: Option<SourceBuffer<RowCellHandle>>,
    table_charges: Option<SourceBuffer<crate::staged_source::SourceCharge>>,
    debits: Option<SourceBuffer<crate::staged_source::OriginalRowInvoice>>,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    undo: Option<PreparedSourceUndo>,
}
/// A complete admitted successor; dropping it leaves the prior source intact.
#[doc(hidden)]
pub struct PreparedSourceMutation {
    binding: TxBinding,
    eager: Option<crate::ordered_staged::PreparedNativeChange>,
    sidecars: Option<crate::staged_source::AdmittedSidecars>,
    mutation: PreparedSourceMutationKind,
}
enum PreparedSourceMutationKind {
    VectorSlot(PreparedVectorMutation),
    ReplaceSlot(PreparedSlotReplacement),
    VectorPositions(PreparedVectorPositions),
    Row(PreparedRowMutation),
    Remove(PreparedRowRemoval),
    RemoveRows(PreparedRowRemovals),
    Vector(PreparedVectorMutation),
    Canonicalize(PreparedCanonicalization),
    RemoveVector(PreparedVectorRemoval),
}
struct PreparedOrdinaryVectors {
    values: Vec<VectorEntry>,
    charge: Option<crate::staged_source::SourceCharge>,
}
struct PreparedVectorMutation {
    original: crate::staged_source::AdmittedSourceVector,
    cell: VectorCellHandle,
    root: FundedArc<SourceRoot>,
    expected_root: FundedArc<SourceRoot>,
    ordinary: Option<PreparedOrdinaryVectors>,
    slots: Option<SourceBuffer<VectorCellHandle>>,
    debits: Option<SourceBuffer<crate::staged_source::OriginalVectorInvoice>>,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    undo: Option<PreparedSourceUndo>,
}
struct PreparedVectorRemoval {
    root: FundedArc<SourceRoot>,
    expected_root: FundedArc<SourceRoot>,
    name: Option<crate::staged_source::VectorName>,
    row: RowId,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    undo: Option<PreparedSourceUndo>,
}
struct PreparedRowRemovals {
    root: FundedArc<SourceRoot>,
    positions: SourceBuffer<usize>,
    expected_rows: usize,
    undo: Option<PreparedSourceUndo>,
    eager: Option<crate::ordered_staged::PreparedNativeRemovals>,
}
struct PreparedRowRemoval {
    root: FundedArc<SourceRoot>,
    expected_root: FundedArc<SourceRoot>,
    table: Option<crate::staged_source::SourceName>,
    row: RowId,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    undo: Option<PreparedSourceUndo>,
}

impl EngineGuard<'_> {
    /// Build source keys only after admitting their temporary native storage.
    pub fn prepare_row_from_meta(
        &self,
        table: &str,
        row: VersionedRow,
        meta: &contextdb_core::TableMeta,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        struct KeyScratch {
            keys: Vec<(String, contextdb_core::IndexKey)>,
            _credit: crate::staged_source::SourceCharge,
        }
        let mut bytes = meta
            .indexes
            .len()
            .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
            .ok_or_else(source_identity_error)?;
        for index in &meta.indexes {
            bytes = bytes
                .checked_add(index.name.len())
                .and_then(|n| {
                    n.checked_add(
                        index
                            .columns
                            .len()
                            .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                    )
                })
                .ok_or_else(source_identity_error)?;
            for (column, _) in &index.columns {
                let value_bytes = original
                    .row
                    .values
                    .get(column)
                    .map_or(0, contextdb_core::Value::estimated_bytes);
                bytes = bytes
                    .checked_add(
                        value_bytes
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(128))
                            .ok_or_else(source_identity_error)?,
                    )
                    .ok_or_else(source_identity_error)?;
            }
        }
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let mut scratch = KeyScratch {
            keys: Vec::with_capacity(meta.indexes.len()),
            _credit: credit,
        };
        for index in &meta.indexes {
            let mut key = Vec::with_capacity(index.columns.len());
            for (column, direction) in &index.columns {
                let value = original
                    .row
                    .values
                    .get(column)
                    .cloned()
                    .unwrap_or(contextdb_core::Value::Null);
                key.push(match direction {
                    contextdb_core::SortDirection::Asc => {
                        contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                    }
                    contextdb_core::SortDirection::Desc => {
                        contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                    }
                });
            }
            scratch.keys.push((index.name.clone(), key));
        }
        let schema = self.prepare_schema(table, meta)?;
        self.prepare_row(table, original.row, &scratch.keys, schema, original.invoice)
    }
    pub fn prepare_row(
        &self,
        table: &str,
        row: VersionedRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        self.prepare_row_inner(self.eager_preparation(), table, row, keys, schema, invoice)
    }
    fn prepare_row_inner(
        &self,
        eager_enabled: bool,
        table: &str,
        row: VersionedRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        // Pair parameters before even the first validation or allocation.
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let eager_schema = schema.clone();
        let ordinary_row = self.funder.copy_row(&original.row)?;
        let mut bundle = SourceBuffer::with_capacity(keys.len(), self.funder)?;
        for (index, key) in keys {
            let name = schema.index_name(index).ok_or_else(source_identity_error)?;
            let control =
                FundedArc::<crate::staged_source::PostingAtom>::prepare(self.funder.clone())?;
            let key = self.funder.copy_key(key)?;
            let posting = FundedArc::from_prepared(
                crate::staged_source::PostingAtom {
                    position: (key.key, original.row.row_id),
                    _payload: key.charge,
                },
                control,
            );
            bundle.push((name, posting));
        }
        let name = match self.host.state.root.table_name(table) {
            Some(name) => name,
            None => crate::staged_source::SourceName::new(table, self.funder)?,
        };
        let prior = self.host.state.root.table(table);
        let sequence = prior.map_or(0, |root| root.next_sequence);
        let identity = crate::staged_source::RowIdentity {
            tx: self.host.tx,
            table: name.clone(),
            row: original.row.row_id,
            created_tx: original.row.created_tx,
            deleted_tx: original.row.deleted_tx,
            lsn: original.row.lsn,
            created_at: original.row.created_at,
            sequence,
            generation: self.host.generation,
            base: self
                .host
                .state
                .ws
                .base_read
                .clone()
                .ok_or_else(source_identity_error)?,
            schema,
        };
        let position = self.host.state.ws.relational_inserts.len();
        let cell = crate::staged_source::RowCell::sealed(identity, bundle, original, self.funder)?;
        let table_root = match prior {
            Some(prior) => prior.prepare_insert(cell.clone(), self.funder)?,
            None => crate::staged_source::TableRoot::default()
                .prepare_insert(cell.clone(), self.funder)?,
        };
        let root = self
            .host
            .state
            .root
            .prepare_table(name, table_root, self.funder)?;
        let needed = position
            .checked_add(1)
            .ok_or_else(|| Error::Other("source slot count overflow".into()))?;
        let growth = |capacity: usize| -> Result<usize> {
            needed
                .max(
                    capacity
                        .checked_mul(2)
                        .ok_or_else(|| Error::Other("source slot capacity overflow".into()))?,
                )
                .checked_add(0)
                .ok_or_else(source_identity_error)
        };
        let ordinary = if needed > self.host.state.ws.relational_inserts.capacity() {
            let prepared = SourceBuffer::<(String, VersionedRow)>::with_capacity(
                growth(self.host.state.ws.relational_inserts.capacity())?,
                self.funder,
            )?;
            let (values, charge) = prepared.into_parts();
            Some(PreparedOrdinaryRows { values, charge })
        } else {
            None
        };
        let slots = if needed > self.host.state.row_slots.capacity() {
            Some(SourceBuffer::with_capacity(
                growth(self.host.state.row_slots.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let table_charges = if needed > self.host.state.row_table_charges.capacity() {
            Some(SourceBuffer::with_capacity(
                growth(self.host.state.row_table_charges.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let debits = if needed > self.host.state.row_debits.capacity() {
            Some(SourceBuffer::with_capacity(
                growth(self.host.state.row_debits.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let table_charge =
            crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), table.len())?;
        let table_owned = table.to_owned();
        if table_owned.capacity() != table.len() {
            return Err(source_identity_error());
        }
        let eager = if eager_enabled {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_row(
                        table,
                        crate::ordered_staged::StagedPosition {
                            row_id: ordinary_row.row.row_id,
                            created_tx: ordinary_row.row.created_tx,
                            position,
                        },
                        &ordinary_row.row,
                        keys,
                        eager_schema,
                        self.funder,
                    )?
                    .prepare_retain_row(cell.clone(), self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars: None,
            mutation: PreparedSourceMutationKind::Row(PreparedRowMutation {
                undo: self.prepare_undo(5)?,
                row: ordinary_row,
                table: table_owned,
                table_charge,
                cell,
                root,
                expected_root: self.host.state.root.clone(),
                ordinary,
                slots,
                table_charges,
                debits,
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
            }),
        })
    }
    pub fn publish(&mut self, mut prepared: PreparedSourceMutation) {
        self.validate_binding(&prepared.binding);
        if let Some(maps) = &prepared.sidecars {
            assert!(maps.validate(self.funder).is_ok(), "sidecar token origin");
        }
        let eager = if let PreparedSourceMutationKind::RemoveRows(removal) = &mut prepared.mutation
        {
            assert!(
                prepared.eager.is_none(),
                "one prepared bulk eager successor"
            );
            self.validate_bulk_removal(removal, prepared.sidecars.is_some());
            removal.eager.take().map(|p| {
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .validate_native_removals(p)
                    .expect("validated bulk eager source")
            })
        } else {
            prepared.eager.map(|p| {
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .validate_native_change(p)
                    .expect("validated eager source")
            })
        };
        self.publish_source_kind(prepared.mutation);
        if let Some(maps) = prepared.sidecars {
            let old = self.swap_sidecars(maps);
            self.record_undo(SourceUndo::Sidecars(old));
        }
        if let Some(eager) = eager {
            self.host
                .state
                .ws
                .ordered_staged_rows
                .publish_validated_native_change(eager);
        }
        self.host.revision = prepared.binding.next_revision;
    }
    fn publish_source_kind(&mut self, mutation: PreparedSourceMutationKind) {
        let mut p = match mutation {
            PreparedSourceMutationKind::ReplaceSlot(p) => {
                self.publish_slot_replacement(p);
                return;
            }
            PreparedSourceMutationKind::VectorPositions(p) => {
                self.publish_vector_positions(p);
                return;
            }
            PreparedSourceMutationKind::Row(row) => row,
            PreparedSourceMutationKind::Remove(removal) => {
                self.publish_removal(removal);
                return;
            }
            PreparedSourceMutationKind::RemoveRows(removal) => {
                self.publish_bulk_removal(removal);
                return;
            }
            PreparedSourceMutationKind::VectorSlot(vector) => {
                self.publish_vector_slot(vector);
                return;
            }
            PreparedSourceMutationKind::Vector(vector) => {
                self.publish_vector(vector);
                return;
            }
            PreparedSourceMutationKind::RemoveVector(removal) => {
                self.publish_vector_removal(removal);
                return;
            }
            PreparedSourceMutationKind::Canonicalize(canonical) => {
                self.publish_canonicalization(canonical);
                return;
            }
        };
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        self.publish_undo(p.undo.take());
        if let Some(mut replacement) = p.ordinary.take() {
            replacement
                .values
                .append(&mut self.host.state.ws.relational_inserts);
            let old = std::mem::replace(
                &mut self.host.state.ws.relational_inserts,
                replacement.values,
            );
            let old_credit = std::mem::replace(
                &mut self.host.state.ordinary_rows_charge,
                replacement.charge,
            );
            self.record_undo(SourceUndo::RowsStorage(PreparedOrdinaryRows {
                values: old,
                charge: old_credit,
            }));
        }
        if let Some(mut replacement) = p.slots.take() {
            let mut old = std::mem::replace(&mut self.host.state.row_slots, SourceBuffer::empty());
            old.append_into(&mut replacement);
            self.host.state.row_slots = replacement;
            self.record_undo(SourceUndo::RowSlotsStorage(old));
        }
        if let Some(mut replacement) = p.table_charges.take() {
            let mut old = std::mem::replace(
                &mut self.host.state.row_table_charges,
                SourceBuffer::empty(),
            );
            old.append_into(&mut replacement);
            self.host.state.row_table_charges = replacement;
            self.record_undo(SourceUndo::TableChargesStorage(old));
        }
        if let Some(mut replacement) = p.debits.take() {
            self.host.state.row_debits.append_into(&mut replacement);
            let old = std::mem::replace(&mut self.host.state.row_debits, replacement);
            self.record_undo(SourceUndo::RowDebitsStorage(old));
        }
        if self.host.checkpoint_depth > 0 {
            self.host.undo.push(SourceUndo::InsertedRow(
                self.host.state.ws.relational_inserts.len(),
            ));
        }
        self.host
            .state
            .ws
            .relational_inserts
            .push((p.table, p.row.row));
        self.host.state.row_debits.push(p.row.invoice);
        self.host.state.row_table_charges.push(p.table_charge);
        self.host.state.row_slots.push(p.cell);
        self.host.state.root = p.root;
        // The ordinary debit is distinct from the immutable original debit.
    }
}

pub(crate) fn same_published_source(left: &PublishedRead, right: &PublishedRead) -> bool {
    left.snapshot == right.snapshot
        && left.lsn == right.lsn
        && match (&left.handle, &right.handle) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
}

impl EngineGuard<'_> {
    pub fn prepare_remove(&self, table: &str, id: RowId) -> Result<PreparedSourceMutation> {
        let native = self.eager_preparation();
        // Genuine original O admission precedes every caller C preparation.
        let admission = if native {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_removal_admission(table, id)?,
            )
        } else {
            None
        };
        let undo = self.prepare_undo(
            self.host
                .state
                .ws
                .relational_inserts
                .iter()
                .filter(|(name, row)| name == table && row.row_id == id)
                .count()
                .checked_add(1)
                .ok_or_else(source_identity_error)?,
        )?;
        let maps = self.sidecars();
        let sidecars = if maps.lsns.get(table).is_some_and(|m| m.contains_key(&id))
            || maps.kinds.get(table).is_some_and(|m| m.contains_key(&id))
            || maps
                .admitted
                .get(table)
                .is_some_and(|m| m.contains_key(&id))
        {
            Some(
                self.funder
                    .copy_sidecars(maps, Some((table, id, None, None)))?,
            )
        } else {
            None
        };
        let table_name = self.host.state.root.table_name(table);
        let root = if let Some(name) = &table_name {
            let table_root = self
                .host
                .state
                .root
                .table(table)
                .expect("source name has its table");
            let next = table_root.prepare_remove(id, self.funder)?;
            self.host
                .state
                .root
                .prepare_table(name.clone(), next, self.funder)?
        } else {
            self.host.state.root.clone()
        };
        let eager = if let Some(admission) = admission {
            let positions = self.removal_position_map(table, id)?;
            Some(
                admission
                    .prepare(self.funder)?
                    .prepare_rebind(&positions.maps, self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars,
            mutation: PreparedSourceMutationKind::Remove(PreparedRowRemoval {
                undo,
                root,
                expected_root: self.host.state.root.clone(),
                table: table_name,
                row: id,
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
            }),
        })
    }
    /// Prepare one bulk successor without publishing any selected identity.
    pub fn prepare_remove_rows(
        &self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) -> Result<PreparedSourceMutation> {
        let mut lower = None;
        let eager = if self.eager_preparation() {
            let admission = self
                .host
                .state
                .ws
                .ordered_staged_rows
                .prepare_native_bulk_removal_admission(selected)?;
            // The companion retains collection-first and every table O grant
            // before it invokes this sole lower caller C preparation door.
            Some(admission.prepare(self.funder, || {
                lower = Some(self.prepare_bulk_removal(selected)?);
                Ok(())
            })?)
        } else {
            None
        };
        // No selected native table needs no O owner and does not invoke C.
        // Ordinary-only identities and sidecars still use their genuine door.
        let (mut removal, sidecars) = match lower {
            Some(lower) => lower,
            None => self.prepare_bulk_removal(selected)?,
        };
        removal.eager = match eager {
            Some(eager) if eager.needs_rebind() => {
                let positions = self.position_map(|_, name, id| {
                    !selected
                        .iter()
                        .any(|(table, row)| table == name && *row == id)
                })?;
                Some(eager.prepare_rebind(&positions.maps, self.funder)?)
            }
            eager => eager,
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager: None,
            sidecars,
            mutation: PreparedSourceMutationKind::RemoveRows(removal),
        })
    }
    fn prepare_bulk_removal(
        &self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) -> Result<(
        PreparedRowRemovals,
        Option<crate::staged_source::AdmittedSidecars>,
    )> {
        let selected_row = |table: &str, id: RowId| {
            selected
                .iter()
                .any(|(name, row)| name == table && *row == id)
        };
        let rows = &self.host.state.ws.relational_inserts;
        if rows.len() != self.host.state.row_debits.len()
            || rows.len() != self.host.state.row_table_charges.len()
            || rows.len() != self.host.state.row_slots.len()
        {
            return Err(source_identity_error());
        }
        let count = rows
            .iter()
            .filter(|(table, row)| selected_row(table, row.row_id))
            .count();
        let maps = self.sidecars();
        let changed_sidecars = selected.iter().any(|(table, id)| {
            maps.lsns.get(table).is_some_and(|m| m.contains_key(id))
                || maps.kinds.get(table).is_some_and(|m| m.contains_key(id))
                || maps.admitted.get(table).is_some_and(|m| m.contains_key(id))
        });
        let undo = self.prepare_undo(
            count
                .checked_add(usize::from(changed_sidecars))
                .ok_or_else(source_identity_error)?,
        )?;
        let mut positions = SourceBuffer::with_capacity(count, self.funder)?;
        for (position, (table, row)) in rows.iter().enumerate() {
            if selected_row(table, row.row_id) {
                positions.push(position);
            }
        }
        let sidecars = if changed_sidecars {
            // Copy once under the existing admitted carrier. Filter only the
            // unpublished maps; old full keys, values and capacities survive.
            let mut maps = self.funder.copy_sidecars(maps, None)?;
            for (table, id) in selected {
                if let Some(rows) = maps.lsns.get_mut(table) {
                    rows.remove(id);
                    if rows.is_empty() {
                        maps.lsns.remove(table);
                    }
                }
                if let Some(rows) = maps.kinds.get_mut(table) {
                    rows.remove(id);
                    if rows.is_empty() {
                        maps.kinds.remove(table);
                    }
                }
                if let Some(rows) = maps.admitted.get_mut(table) {
                    rows.remove(id);
                    if rows.is_empty() {
                        maps.admitted.remove(table);
                    }
                }
            }
            Some(maps)
        } else {
            None
        };
        let mut root = self.host.state.root.clone();
        for (table, id) in selected {
            let Some(name) = root.table_name(table) else {
                continue;
            };
            let prior = root.table(table).expect("source name has its table");
            if crate::staged_source::get_by(&prior.by_id, |row| id.cmp(row)).is_none() {
                continue;
            }
            let next = prior.prepare_remove(*id, self.funder)?;
            root = root.prepare_table(name, next, self.funder)?;
        }
        Ok((
            PreparedRowRemovals {
                root,
                positions,
                expected_rows: rows.len(),
                undo,
                eager: None,
            },
            sidecars,
        ))
    }
    fn validate_bulk_removal(&self, p: &PreparedRowRemovals, sidecars: bool) {
        let rows = self.host.state.ws.relational_inserts.len();
        assert_eq!(p.expected_rows, rows, "prepared bulk ordinary rows changed");
        assert_eq!(rows, self.host.state.row_slots.len());
        assert_eq!(rows, self.host.state.row_debits.len());
        assert_eq!(rows, self.host.state.row_table_charges.len());
        let mut previous = None;
        for &position in p.positions.as_slice() {
            assert!(
                position < rows && previous.is_none_or(|old| old < position),
                "prepared bulk positions changed"
            );
            previous = Some(position);
        }
        if let Some(undo) = &p.undo {
            assert_eq!(undo.expected_depth, self.host.checkpoint_depth);
            assert_eq!(undo.expected_len, self.host.undo.len());
            assert_eq!(undo.records, p.positions.len() + usize::from(sidecars));
            let mut nodes = undo.nodes.0.as_ref();
            let mut prepared = 0;
            while let Some(node) = nodes {
                let value = node.get();
                assert!(value.undo.is_none());
                prepared += 1;
                nodes = value.next.0.as_ref();
            }
            assert_eq!(
                prepared, undo.records,
                "prepared bulk journal nodes changed"
            );
        } else {
            assert_eq!(self.host.checkpoint_depth, 0);
        }
    }
    fn publish_bulk_removal(&mut self, mut p: PreparedRowRemovals) {
        assert!(
            p.eager.is_none(),
            "bulk eager successor validated before publication"
        );
        self.publish_undo(p.undo.take());
        // Exact original positions descend; existing reverse-chronology
        // inverses restore them ascending into the original native capacity.
        for &position in p.positions.as_slice().iter().rev() {
            self.remove_ordinary_row(position);
        }
        self.host.state.root = p.root;
    }
    fn publish_removal(&mut self, mut p: PreparedRowRemoval) {
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        self.publish_undo(p.undo.take());
        if let Some(table) = &p.table {
            let mut position = 0;
            while position < self.host.state.ws.relational_inserts.len() {
                let (name, row) = &self.host.state.ws.relational_inserts[position];
                if name != table.as_str() || row.row_id != p.row {
                    position += 1;
                    continue;
                }
                self.remove_ordinary_row(position);
            }
        }
        self.host.state.root = p.root;
    }
}

impl EngineGuard<'_> {
    pub fn prepare_vector(
        &self,
        entry: VectorEntry,
        scalar: Option<(RowId, contextdb_core::ColName)>,
        invoice: crate::staged_source::OriginalVectorInvoice,
    ) -> Result<PreparedSourceMutation> {
        self.prepare_vector_inner(self.eager_preparation(), entry, scalar, invoice)
    }
    fn prepare_vector_inner(
        &self,
        eager_enabled: bool,
        entry: VectorEntry,
        scalar: Option<(RowId, contextdb_core::ColName)>,
        invoice: crate::staged_source::OriginalVectorInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceVector::new(entry, invoice);
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let ordinary_vector = self.funder.copy_vector(&original.entry)?;
        let name = match crate::staged_source::entry_by(&self.host.state.root.vectors, |name| {
            (
                original.entry.index.table.as_str(),
                original.entry.index.column.as_str(),
            )
                .cmp(&(name.table.as_str(), name.column.as_str()))
        }) {
            Some(entry) => entry.key.clone(),
            None => crate::staged_source::VectorName {
                table: crate::staged_source::SourceName::new(
                    &original.entry.index.table,
                    self.funder,
                )?,
                column: crate::staged_source::SourceName::new(
                    &original.entry.index.column,
                    self.funder,
                )?,
            },
        };
        let removed = self
            .host
            .state
            .ws
            .vector_inserts
            .iter()
            .filter(|entry| {
                entry.index == original.entry.index && entry.row_id == original.entry.row_id
            })
            .count();
        let position = self.host.state.ws.vector_inserts.len() - removed;
        let identity = crate::staged_source::VectorIdentity {
            tx: self.host.tx,
            index: name.clone(),
            row: original.entry.row_id,
            created_tx: original.entry.created_tx,
            deleted_tx: original.entry.deleted_tx,
            lsn: original.entry.lsn,
            generation: self.host.generation,
            base: self
                .host
                .state
                .ws
                .base_read
                .clone()
                .ok_or_else(source_identity_error)?,
        };
        let scalar_cell = if let Some((row, column)) = scalar {
            if row != original.entry.row_id || column != original.entry.index.column {
                return Err(source_identity_error());
            }
            self.host.state.root.table(&original.entry.index.table)
                .and_then(|root| crate::staged_source::get_by(&root.by_id, |key| row.cmp(key)))
                .filter(|row| {
                    let custody = row.custody.lock();
                    let body = custody.as_ref().map(|RowCustody::Sealed { row: body, .. }| body);
                    body.is_some_and(|body| matches!(body.values.get(&column),
                        Some(contextdb_core::Value::Vector(values)) if values.as_slice() == original.entry.vector.as_slice()))
                }).cloned()
        } else {
            None
        };
        let cell = crate::staged_source::VectorCell::sealed(identity, original, self.funder)?;
        let reference = match scalar_cell {
            Some(row) => crate::staged_source::CanonicalVectorRef::scalar(
                ordinary_vector.entry.row_id,
                cell.clone(),
                row,
                name.column.clone(),
            ),
            None => crate::staged_source::CanonicalVectorRef::owned(
                ordinary_vector.entry.row_id,
                cell.clone(),
            ),
        };
        let prior = self.host.state.root.vector(
            &ordinary_vector.entry.index.table,
            &ordinary_vector.entry.index.column,
        );
        let vector_root = match prior {
            Some(root) => root.prepare_insert(reference, self.funder)?,
            None => crate::staged_source::VectorRoot::default()
                .prepare_insert(reference, self.funder)?,
        };
        let root = self
            .host
            .state
            .root
            .prepare_vector(name, vector_root, self.funder)?;
        let needed = position
            .checked_add(1)
            .ok_or_else(|| Error::Other("source vector count overflow".into()))?;
        let grow = |capacity: usize| -> Result<usize> {
            Ok(needed.max(
                capacity
                    .checked_mul(2)
                    .ok_or_else(|| Error::Other("source vector capacity overflow".into()))?,
            ))
        };
        let ordinary = if needed > self.host.state.ws.vector_inserts.capacity() {
            let (values, charge) = SourceBuffer::<VectorEntry>::with_capacity(
                grow(self.host.state.ws.vector_inserts.capacity())?,
                self.funder,
            )?
            .into_parts();
            Some(PreparedOrdinaryVectors { values, charge })
        } else {
            None
        };
        let slots = if needed > self.host.state.vector_slots.capacity() {
            Some(SourceBuffer::with_capacity(
                grow(self.host.state.vector_slots.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let debits = if needed > self.host.state.vector_debits.capacity() {
            Some(SourceBuffer::with_capacity(
                grow(self.host.state.vector_debits.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let eager = if eager_enabled {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_owned_vector(&ordinary_vector.entry, self.funder)?
                    .prepare_retain_vector(cell.clone(), self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars: None,
            mutation: PreparedSourceMutationKind::Vector(PreparedVectorMutation {
                undo: self
                    .prepare_undo(removed.checked_add(4).ok_or_else(source_identity_error)?)?,
                original: ordinary_vector,
                cell,
                root,
                expected_root: self.host.state.root.clone(),
                ordinary,
                slots,
                debits,
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
            }),
        })
    }
    fn publish_vector(&mut self, mut p: PreparedVectorMutation) {
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        self.publish_undo(p.undo.take());
        let mut position = 0;
        while position < self.host.state.ws.vector_inserts.len() {
            let entry = &self.host.state.ws.vector_inserts[position];
            if entry.index != p.original.entry.index || entry.row_id != p.original.entry.row_id {
                position += 1;
                continue;
            }
            self.remove_ordinary_vector(position);
        }
        if let Some(mut replacement) = p.ordinary.take() {
            replacement
                .values
                .append(&mut self.host.state.ws.vector_inserts);
            let old = std::mem::replace(&mut self.host.state.ws.vector_inserts, replacement.values);
            let old_credit = std::mem::replace(
                &mut self.host.state.ordinary_vectors_charge,
                replacement.charge,
            );
            self.record_undo(SourceUndo::VectorsStorage(PreparedOrdinaryVectors {
                values: old,
                charge: old_credit,
            }));
        }
        if let Some(mut replacement) = p.slots.take() {
            let mut old =
                std::mem::replace(&mut self.host.state.vector_slots, SourceBuffer::empty());
            old.append_into(&mut replacement);
            self.host.state.vector_slots = replacement;
            self.record_undo(SourceUndo::VectorSlotsStorage(old));
        }
        if let Some(mut replacement) = p.debits.take() {
            self.host.state.vector_debits.append_into(&mut replacement);
            let old = std::mem::replace(&mut self.host.state.vector_debits, replacement);
            self.record_undo(SourceUndo::VectorDebitsStorage(old));
        }
        if self.host.checkpoint_depth > 0 {
            self.host.undo.push(SourceUndo::InsertedVector(
                self.host.state.ws.vector_inserts.len(),
            ));
        }
        self.host.state.ws.vector_inserts.push(p.original.entry);
        self.host.state.vector_debits.push(p.original.invoice);
        self.host.state.vector_slots.push(p.cell);
        self.host.state.root = p.root;
    }
}

impl EngineGuard<'_> {
    pub fn prepare_remove_vector(
        &self,
        index: &VectorIndexRef,
        row: RowId,
    ) -> Result<PreparedSourceMutation> {
        let name = crate::staged_source::entry_by(&self.host.state.root.vectors, |name| {
            (index.table.as_str(), index.column.as_str())
                .cmp(&(name.table.as_str(), name.column.as_str()))
        })
        .map(|entry| entry.key.clone());
        let root = match &name {
            Some(name) => {
                let next = self
                    .host
                    .state
                    .root
                    .vector(&index.table, &index.column)
                    .expect("existing vector domain")
                    .prepare_remove(row, self.funder)?;
                self.host
                    .state
                    .root
                    .prepare_vector(name.clone(), next, self.funder)?
            }
            None => self.host.state.root.clone(),
        };
        let removed = self
            .host
            .state
            .ws
            .vector_inserts
            .iter()
            .filter(|entry| entry.index == *index && entry.row_id == row)
            .count();
        let eager = if self.eager_preparation() {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_vector_remove(index, row, self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars: None,
            mutation: PreparedSourceMutationKind::RemoveVector(PreparedVectorRemoval {
                root,
                expected_root: self.host.state.root.clone(),
                name,
                row,
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
                undo: self.prepare_undo(removed)?,
            }),
        })
    }
    fn publish_vector_removal(&mut self, mut p: PreparedVectorRemoval) {
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        self.publish_undo(p.undo.take());
        if let Some(name) = &p.name {
            let mut position = 0;
            while position < self.host.state.ws.vector_inserts.len() {
                let entry = &self.host.state.ws.vector_inserts[position];
                if entry.index.table != name.table.as_str()
                    || entry.index.column != name.column.as_str()
                    || entry.row_id != p.row
                {
                    position += 1;
                    continue;
                }
                self.remove_ordinary_vector(position);
            }
        }
        self.host.state.root = p.root;
    }
}

struct PreparedRowIdentity {
    old: RowCellHandle,
    new: RowCellHandle,
    position: usize,
}
struct PreparedVectorIdentity {
    old: VectorCellHandle,
    new: VectorCellHandle,
    position: usize,
}
/// Identity successors and any genuine copied bodies are fully admitted while
/// the original host is locked. The token cannot publish through a later guard.
#[doc(hidden)]
pub struct PreparedSourceIdentity {
    binding: TxBinding,
    headers: Option<HeaderImage>,
    undo: Option<PreparedSourceUndo>,
    eager: Option<crate::ordered_staged::PreparedNativeChange>,
    rows: SourceBuffer<PreparedRowIdentity>,
    vectors: SourceBuffer<PreparedVectorIdentity>,
    root: FundedArc<SourceRoot>,
    expected_root: FundedArc<SourceRoot>,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    from: TxId,
    to: TxId,
    lsn: Option<Lsn>,
}
impl EngineGuard<'_> {
    pub fn prepare_identity(
        &self,
        from: TxId,
        to: TxId,
        lsn: Lsn,
    ) -> Result<PreparedSourceIdentity> {
        self.prepare_identity_inner(from, to, Some(lsn))
    }
    fn prepare_reassignment(&self, from: TxId, to: TxId) -> Result<PreparedSourceIdentity> {
        self.prepare_identity_inner(from, to, None)
    }
    fn prepare_identity_inner(
        &self,
        from: TxId,
        to: TxId,
        lsn: Option<Lsn>,
    ) -> Result<PreparedSourceIdentity> {
        let row_changes = self
            .host
            .state
            .row_slots
            .as_slice()
            .iter()
            .filter(|cell| {
                (from != to
                    && (cell.identity.created_tx == from || cell.identity.deleted_tx == Some(from)))
                    || lsn.is_some_and(|lsn| cell.identity.lsn != lsn)
            })
            .count();
        let vector_changes = self
            .host
            .state
            .vector_slots
            .as_slice()
            .iter()
            .filter(|cell| {
                (from != to
                    && (cell.identity.created_tx == from || cell.identity.deleted_tx == Some(from)))
                    || lsn.is_some_and(|lsn| cell.identity.lsn != lsn)
            })
            .count();
        let mut rows = SourceBuffer::with_capacity(row_changes, self.funder)?;
        let mut vectors = SourceBuffer::with_capacity(vector_changes, self.funder)?;
        for (position, old) in self.host.state.row_slots.as_slice().iter().enumerate() {
            let (table, body) = self
                .host
                .state
                .ws
                .relational_inserts
                .get(position)
                .ok_or_else(source_identity_error)?;
            if table != old.identity.table.as_str() || !old.identity.matches_row(body) {
                return Err(source_identity_error());
            }
            if !(from != to && (body.created_tx == from || body.deleted_tx == Some(from)))
                && lsn.is_none_or(|lsn| body.lsn == lsn)
            {
                continue;
            }
            let mut copy = self.funder.copy_row(body)?;
            let copied_lsn = lsn.unwrap_or(copy.row.lsn);
            reassign_source_row(&mut copy.row, from, to, copied_lsn);
            let mut identity = old.identity.clone();
            if identity.created_tx == from {
                identity.created_tx = to;
            }
            if identity.deleted_tx == Some(from) {
                identity.deleted_tx = Some(to);
            }
            if let Some(lsn) = lsn {
                identity.lsn = lsn;
            }
            let mut keys = SourceBuffer::with_capacity(old.keys.len(), self.funder)?;
            for (name, posting) in old.keys.as_slice() {
                keys.push((name.clone(), posting.clone()));
            }
            let new = crate::staged_source::RowCell::sealed(identity, keys, copy, self.funder)?;
            rows.push(PreparedRowIdentity {
                old: old.clone(),
                new,
                position,
            });
        }
        for (position, old) in self.host.state.vector_slots.as_slice().iter().enumerate() {
            let body = self
                .host
                .state
                .ws
                .vector_inserts
                .get(position)
                .ok_or_else(source_identity_error)?;
            if !old.identity.matches_entry(body) {
                return Err(source_identity_error());
            }
            if !(from != to && (body.created_tx == from || body.deleted_tx == Some(from)))
                && lsn.is_none_or(|lsn| body.lsn == lsn)
            {
                continue;
            }
            let mut copy = self.funder.copy_vector(body)?;
            let copied_lsn = lsn.unwrap_or(copy.entry.lsn);
            reassign_source_vector(&mut copy.entry, from, to, copied_lsn);
            let mut identity = old.identity.clone();
            if identity.created_tx == from {
                identity.created_tx = to;
            }
            if identity.deleted_tx == Some(from) {
                identity.deleted_tx = Some(to);
            }
            if let Some(lsn) = lsn {
                identity.lsn = lsn;
            }
            let new = crate::staged_source::VectorCell::sealed(identity, copy, self.funder)?;
            vectors.push(PreparedVectorIdentity {
                old: old.clone(),
                new,
                position,
            });
        }
        let mut root = self.host.state.root.clone();
        for plan in rows.as_slice() {
            let table = plan.old.identity.table.as_str();
            let prior = root.table(table).ok_or_else(source_identity_error)?;
            // Superseded ordinary slots are retained until canonicalization;
            // they must not displace the latest captured identity.
            if crate::staged_source::get_by(&prior.by_id, |key| plan.old.identity.row.cmp(key))
                .is_some_and(|cell| cell.ptr_eq(&plan.old))
            {
                let next = prior.prepare_identity(plan.new.clone(), self.funder)?;
                root = root.prepare_table(plan.old.identity.table.clone(), next, self.funder)?;
            }
        }
        for plan in vectors.as_slice() {
            let name = &plan.old.identity.index;
            let prior = root
                .vector(name.table.as_str(), name.column.as_str())
                .ok_or_else(source_identity_error)?;
            let reference =
                crate::staged_source::get_by(&prior.rows, |key| plan.old.identity.row.cmp(key))
                    .ok_or_else(source_identity_error)?;
            if !reference.owner.ptr_eq(&plan.old) {
                return Err(source_identity_error());
            }
            let next = prior.prepare_insert(reference.with_owner(plan.new.clone()), self.funder)?;
            root = root.prepare_vector(name.clone(), next, self.funder)?;
        }
        let headers = if self.host.checkpoint_depth != 0 {
            Some(self.prepare_headers()?)
        } else {
            None
        };
        let undo = self.prepare_undo(1)?;
        let mut eager = if self.eager_preparation() {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_reassign(from, to, self.funder)?,
            )
        } else {
            None
        };
        if let Some(mut native) = eager.take() {
            for plan in rows.as_slice() {
                native = native.prepare_rekey_row(&plan.old, &plan.new, self.funder)?;
            }
            for plan in vectors.as_slice() {
                native = native.prepare_rekey_vector(&plan.old, &plan.new, self.funder)?;
            }
            eager = Some(native);
        }
        Ok(PreparedSourceIdentity {
            binding: self.binding()?,
            headers,
            undo,
            eager,
            rows,
            vectors,
            root,
            expected_root: self.host.state.root.clone(),
            tx: self.host.tx,
            generation: self.host.generation,
            preparation_scope: self.preparation_scope,
            from,
            to,
            lsn,
        })
    }
    pub fn publish_identity(&mut self, mut p: PreparedSourceIdentity) {
        self.validate_binding(&p.binding);
        let eager = p.eager.take().map(|e| {
            self.host
                .state
                .ws
                .ordered_staged_rows
                .validate_native_change(e)
                .expect("validated eager identity")
        });
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        // Authenticate all original slots before the first payload transition.
        for plan in p.rows.as_slice() {
            assert!(self.host.state.row_slots.as_slice()[plan.position].ptr_eq(&plan.old));
            assert!(
                plan.old
                    .identity
                    .matches_row(&self.host.state.ws.relational_inserts[plan.position].1)
            );
        }
        for plan in p.vectors.as_slice() {
            assert!(self.host.state.vector_slots.as_slice()[plan.position].ptr_eq(&plan.old));
            assert!(
                plan.old
                    .identity
                    .matches_entry(&self.host.state.ws.vector_inserts[plan.position])
            );
        }
        self.publish_undo(p.undo.take());
        if let Some(headers) = p.headers.take() {
            self.record_undo(SourceUndo::Headers(headers));
        }
        self.host.revision = p.binding.next_revision;
        if let Some(eager) = eager {
            self.host
                .state
                .ws
                .ordered_staged_rows
                .publish_validated_native_change(eager);
        }
        for plan in p.rows.values_mut() {
            let body = &mut self.host.state.ws.relational_inserts[plan.position].1;
            reassign_source_row(body, p.from, p.to, p.lsn.unwrap_or(body.lsn));
            self.host
                .state
                .row_slots
                .replace_at(plan.position, plan.new.clone());
        }
        for plan in p.vectors.values_mut() {
            let body = &mut self.host.state.ws.vector_inserts[plan.position];
            reassign_source_vector(body, p.from, p.to, p.lsn.unwrap_or(body.lsn));
            self.host
                .state
                .vector_slots
                .replace_at(plan.position, plan.new.clone());
        }
        let ws = &mut self.host.state.ws;
        for (_, _, tx) in &mut ws.relational_deletes {
            if *tx == p.from {
                *tx = p.to;
            }
        }
        for entry in &mut ws.adj_inserts {
            if entry.created_tx == p.from {
                entry.created_tx = p.to;
            }
            if entry.deleted_tx == Some(p.from) {
                entry.deleted_tx = Some(p.to);
            }
            if let Some(lsn) = p.lsn {
                entry.lsn = lsn;
            }
        }
        for (_, _, _, tx) in &mut ws.adj_deletes {
            if *tx == p.from {
                *tx = p.to;
            }
        }
        for (_, _, tx) in &mut ws.vector_deletes {
            if *tx == p.from {
                *tx = p.to;
            }
        }
        for (_, _, _, tx) in &mut ws.vector_moves {
            if *tx == p.from {
                *tx = p.to;
            }
        }
        if ws.finalized_tx == Some(p.from) {
            ws.finalized_tx = Some(p.to);
        }
        if let Some(lsn) = p.lsn {
            ws.commit_lsn = Some(lsn);
        }
        self.host.state.root = p.root;
    }
}
fn reassign_source_row(row: &mut VersionedRow, from: TxId, to: TxId, lsn: Lsn) {
    if row.created_tx == from {
        row.created_tx = to;
    }
    if row.deleted_tx == Some(from) {
        row.deleted_tx = Some(to);
    }
    row.lsn = lsn;
}
fn reassign_source_vector(entry: &mut VectorEntry, from: TxId, to: TxId, lsn: Lsn) {
    if entry.created_tx == from {
        entry.created_tx = to;
    }
    if entry.deleted_tx == Some(from) {
        entry.deleted_tx = Some(to);
    }
    entry.lsn = lsn;
}

struct PreparedCanonicalization {
    expected_root: FundedArc<SourceRoot>,
    tx: TxId,
    generation: u64,
    preparation_scope: u64,
    undo: Option<PreparedSourceUndo>,
}
impl EngineGuard<'_> {
    pub fn prepare_canonicalize(&self) -> Result<PreparedSourceMutation> {
        let eager = if self.eager_preparation() {
            let positions = self.canonical_position_map()?;
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_rebind(&positions.maps, self.funder)?
                    .prepare_prune_canonical_slots(self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars: None,
            mutation: PreparedSourceMutationKind::Canonicalize(PreparedCanonicalization {
                undo: self.prepare_undo(self.host.state.ws.relational_inserts.len())?,
                expected_root: self.host.state.root.clone(),
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
            }),
        })
    }
    fn publish_canonicalization(&mut self, mut p: PreparedCanonicalization) {
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        self.publish_undo(p.undo.take());
        let mut position = 0;
        while position < self.host.state.ws.relational_inserts.len() {
            let (table, row) = &self.host.state.ws.relational_inserts[position];
            let current = self.host.state.root.table(table).and_then(|root| {
                crate::staged_source::get_by(&root.by_id, |key| row.row_id.cmp(key))
            });
            if current
                .is_some_and(|cell| cell.ptr_eq(&self.host.state.row_slots.as_slice()[position]))
            {
                position += 1;
                continue;
            }
            self.remove_ordinary_row(position);
        }
    }
}

impl<S: WriteSetApplicator> Drop for EngineTransactions<S> {
    fn drop(&mut self) {
        // Exclusive final destruction frees ordinary images under their own
        // debits; retained captures already own immutable canonical originals.
        let active = self.manager.active_txs.get_mut().engine_mut();
        while let Some(tx) = active.first_key().copied() {
            drop(active.remove(&tx));
        }
    }
}

struct AuxFunding {
    invoices: SourceBuffer<crate::staged_source::OriginalAuxInvoice>,
    buffer_credit: Option<crate::staged_source::SourceCharge>,
}
impl AuxFunding {
    fn empty() -> Self {
        Self {
            invoices: SourceBuffer::empty(),
            buffer_credit: None,
        }
    }
}
struct TxBinding {
    tx: TxId,
    generation: u64,
    scope: u64,
    revision: u64,
    depth: usize,
    next_revision: u64,
    root: FundedArc<SourceRoot>,
}
#[doc(hidden)]
pub struct EngineFlags {
    pub commit_lsn: Option<Lsn>,
    pub finalized_tx: Option<TxId>,
    pub requires_commit_lsn: bool,
    pub visibility_floor: Option<TxId>,
    pub propagation_in_progress: bool,
}
impl EngineFlags {
    fn from_ws(ws: &WriteSet) -> Self {
        Self {
            commit_lsn: ws.commit_lsn,
            finalized_tx: ws.finalized_tx,
            requires_commit_lsn: ws.requires_commit_lsn,
            visibility_floor: ws.visibility_floor,
            propagation_in_progress: ws.propagation_in_progress,
        }
    }
    fn install(self, ws: &mut WriteSet) {
        ws.commit_lsn = self.commit_lsn;
        ws.finalized_tx = self.finalized_tx;
        ws.requires_commit_lsn = self.requires_commit_lsn;
        ws.visibility_floor = self.visibility_floor;
        ws.propagation_in_progress = self.propagation_in_progress;
    }
}
#[doc(hidden)]
pub struct TxCheckpointId {
    tx: TxId,
    generation: u64,
    nonce: u64,
    depth: usize,
}
struct TxFrame {
    id: TxCheckpointId,
    marker: usize,
    root: FundedArc<SourceRoot>,
    eager: Option<crate::ordered_staged::NativeOrderedCheckpoint>,
    displaced_eager: Option<crate::ordered_staged::OrderedStagedRows>,
    retirement: Option<RetiredCheckpointNodeOwner>,
    flags: EngineFlags,
    base: Option<PublishedRead>,
    storage: Option<SourceBuffer<TxFrame>>,
}
#[doc(hidden)]
pub struct PreparedTxCheckpoint {
    binding: TxBinding,
    frame: TxFrame,
    frames: Option<SourceBuffer<TxFrame>>,
}
struct RowHeader {
    created_tx: TxId,
    deleted_tx: Option<TxId>,
    lsn: Lsn,
    created_at: Option<contextdb_core::Wallclock>,
}
struct VectorHeader {
    created_tx: TxId,
    deleted_tx: Option<TxId>,
    lsn: Lsn,
}
struct HeaderImage {
    rows: SourceBuffer<RowHeader>,
    vectors: SourceBuffer<VectorHeader>,
    row_slots: SourceBuffer<RowCellHandle>,
    vector_slots: SourceBuffer<VectorCellHandle>,
    relational_deletes: SourceBuffer<TxId>,
    adj: SourceBuffer<VectorHeader>,
    adj_deletes: SourceBuffer<TxId>,
    vector_deletes: SourceBuffer<TxId>,
    vector_moves: SourceBuffer<TxId>,
    flags: EngineFlags,
}
enum SourceUndo {
    InsertedRow(usize),
    RemovedRow {
        position: usize,
        body: (String, VersionedRow),
        debit: crate::staged_source::OriginalRowInvoice,
        table_charge: crate::staged_source::SourceCharge,
        slot: RowCellHandle,
    },
    InsertedVector(usize),
    RemovedVector {
        position: usize,
        body: VectorEntry,
        debit: crate::staged_source::OriginalVectorInvoice,
        slot: VectorCellHandle,
    },
    RowsStorage(PreparedOrdinaryRows),
    VectorsStorage(PreparedOrdinaryVectors),
    RowSlotsStorage(SourceBuffer<RowCellHandle>),
    VectorSlotsStorage(SourceBuffer<VectorCellHandle>),
    RowDebitsStorage(SourceBuffer<crate::staged_source::OriginalRowInvoice>),
    VectorDebitsStorage(SourceBuffer<crate::staged_source::OriginalVectorInvoice>),
    TableChargesStorage(SourceBuffer<crate::staged_source::SourceCharge>),
    Headers(HeaderImage),
    Flags(EngineFlags),
    Sidecars(crate::staged_source::AdmittedSidecars),
    Aux(AuxInverse),
    Adopt {
        state: TxState,
        eager: crate::ordered_staged::NativeOrderedCheckpoint,
    },
    RetiredAdopt {
        state: TxState,
        displaced_eager: crate::ordered_staged::OrderedStagedRows,
    },
}
// Each inverse node is admitted at its real native layout before publication.
// Its same Box and original grant become retirement custody on close.
struct SourceJournalNode {
    undo: Option<SourceUndo>,
    next: SourceJournalLinks,
}
type SourceJournalNodeOwner = crate::staged_source::FundedBox<SourceJournalNode>;
impl crate::staged_source::FundedBox<SourceJournalNode> {
    fn prepare(funder: &SourceFunder) -> Result<Self> {
        Self::try_new(
            SourceJournalNode {
                undo: None,
                next: SourceJournalLinks::empty(),
            },
            funder,
        )
    }
}
struct SourceJournalLinks(Option<SourceJournalNodeOwner>);
impl SourceJournalLinks {
    fn empty() -> Self {
        Self(None)
    }
    fn push(&mut self, mut node: SourceJournalNodeOwner) {
        node.get_mut().next.0 = self.0.take();
        self.0 = Some(node);
    }
    fn pop(&mut self) -> Option<SourceJournalNodeOwner> {
        let mut node = self.0.take()?;
        self.0 = node.get_mut().next.0.take();
        Some(node)
    }
}
impl Drop for SourceJournalLinks {
    fn drop(&mut self) {
        while let Some(node) = self.pop() {
            drop(node);
        }
    }
}
struct SourceJournal {
    active: SourceJournalLinks,
    spare: SourceJournalLinks,
    len: usize,
    spare_len: usize,
}
impl SourceJournal {
    fn empty() -> Self {
        Self {
            active: SourceJournalLinks::empty(),
            spare: SourceJournalLinks::empty(),
            len: 0,
            spare_len: 0,
        }
    }
    fn len(&self) -> usize {
        self.len
    }
    fn push(&mut self, undo: SourceUndo) {
        let mut node = self.spare.pop().expect("preadmitted source inverse node");
        self.spare_len -= 1;
        node.get_mut().undo = Some(undo);
        self.active.push(node);
        self.len += 1;
    }
    fn pop(&mut self) -> Option<SourceJournalNodeOwner> {
        let node = self.active.pop()?;
        self.len -= 1;
        Some(node)
    }
}
// Stack-only replay dimensions validate every native insert/append before
// either half changes. They carry no payload or independently funded state.
#[derive(Clone, Copy)]
struct RestoreDimensions {
    len: usize,
    capacity: usize,
}
impl RestoreDimensions {
    fn vector<T>(values: &Vec<T>) -> Self {
        Self {
            len: values.len(),
            capacity: values.capacity(),
        }
    }
    fn buffer<T>(values: &SourceBuffer<T>) -> Self {
        Self {
            len: values.len(),
            capacity: values.capacity(),
        }
    }
    fn remove(&mut self, position: usize) {
        assert!(position < self.len, "prepared inverse removal position");
        self.len -= 1;
    }
    fn insert(&mut self, position: usize) {
        assert!(
            position <= self.len && self.len < self.capacity,
            "prepared inverse insertion capacity"
        );
        self.len += 1;
    }
    fn storage(&mut self, old: Self) {
        assert!(
            old.len == 0 && old.capacity >= self.len,
            "preserved inverse storage capacity"
        );
        self.capacity = old.capacity;
    }
}
struct RestoreInventory {
    rows: [RestoreDimensions; 4],
    vectors: [RestoreDimensions; 3],
    aux: [(RestoreDimensions, RestoreDimensions); 10],
}
impl RestoreInventory {
    fn from_state(state: &TxState) -> Self {
        let ws = &state.ws;
        Self {
            rows: [
                RestoreDimensions::vector(&ws.relational_inserts),
                RestoreDimensions::buffer(&state.row_debits),
                RestoreDimensions::buffer(&state.row_table_charges),
                RestoreDimensions::buffer(&state.row_slots),
            ],
            vectors: [
                RestoreDimensions::vector(&ws.vector_inserts),
                RestoreDimensions::buffer(&state.vector_debits),
                RestoreDimensions::buffer(&state.vector_slots),
            ],
            aux: [
                (
                    RestoreDimensions::vector(&ws.relational_deletes),
                    RestoreDimensions::buffer(&state.relational_deletes_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.relational_delete_predicates),
                    RestoreDimensions::buffer(&state.relational_delete_predicates_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.adj_inserts),
                    RestoreDimensions::buffer(&state.adj_inserts_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.adj_deletes),
                    RestoreDimensions::buffer(&state.adj_deletes_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.vector_deletes),
                    RestoreDimensions::buffer(&state.vector_deletes_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.vector_moves),
                    RestoreDimensions::buffer(&state.vector_moves_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.sync_source_provenance_marks),
                    RestoreDimensions::buffer(&state.sync_source_provenance_marks_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.config_writes),
                    RestoreDimensions::buffer(&state.config_writes_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.config_deletes),
                    RestoreDimensions::buffer(&state.config_deletes_funding.invoices),
                ),
                (
                    RestoreDimensions::vector(&ws.config_max_u64_keys),
                    RestoreDimensions::buffer(&state.config_max_u64_keys_funding.invoices),
                ),
            ],
        }
    }
    fn validate_alignment(&self) {
        assert!(self.rows.iter().all(|field| field.len == self.rows[0].len));
        assert!(
            self.vectors
                .iter()
                .all(|field| field.len == self.vectors[0].len)
        );
        assert!(
            self.aux
                .iter()
                .all(|(body, invoice)| body.len == invoice.len)
        );
    }
    fn inverse(&mut self, inverse: &SourceUndo) {
        match inverse {
            SourceUndo::InsertedRow(position) => {
                for field in &mut self.rows {
                    field.remove(*position);
                }
            }
            SourceUndo::RemovedRow { position, .. } => {
                for field in &mut self.rows {
                    field.insert(*position);
                }
            }
            SourceUndo::InsertedVector(position) => {
                for field in &mut self.vectors {
                    field.remove(*position);
                }
            }
            SourceUndo::RemovedVector { position, .. } => {
                for field in &mut self.vectors {
                    field.insert(*position);
                }
            }
            SourceUndo::RowsStorage(old) => {
                self.rows[0].storage(RestoreDimensions::vector(&old.values))
            }
            SourceUndo::VectorsStorage(old) => {
                self.vectors[0].storage(RestoreDimensions::vector(&old.values))
            }
            SourceUndo::RowDebitsStorage(old) => {
                self.rows[1].storage(RestoreDimensions::buffer(old))
            }
            SourceUndo::TableChargesStorage(old) => {
                self.rows[2].storage(RestoreDimensions::buffer(old))
            }
            SourceUndo::RowSlotsStorage(old) => {
                self.rows[3].storage(RestoreDimensions::buffer(old))
            }
            SourceUndo::VectorDebitsStorage(old) => {
                self.vectors[1].storage(RestoreDimensions::buffer(old))
            }
            SourceUndo::VectorSlotsStorage(old) => {
                self.vectors[2].storage(RestoreDimensions::buffer(old))
            }
            SourceUndo::Headers(image) => {
                self.validate_alignment();
                assert_eq!(self.rows[0].len, image.rows.len());
                assert_eq!(self.rows[0].len, image.row_slots.len());
                assert_eq!(self.vectors[0].len, image.vectors.len());
                assert_eq!(self.vectors[0].len, image.vector_slots.len());
                assert_eq!(self.aux[0].0.len, image.relational_deletes.len());
                assert_eq!(self.aux[2].0.len, image.adj.len());
                assert_eq!(self.aux[3].0.len, image.adj_deletes.len());
                assert_eq!(self.aux[4].0.len, image.vector_deletes.len());
                assert_eq!(self.aux[5].0.len, image.vector_moves.len());
            }
            SourceUndo::Aux(inverse) => match inverse {
                AuxInverse::RelationalDeletes(v) => validate_restore_splice(v, &mut self.aux[0]),
                AuxInverse::Predicates(v) => validate_restore_splice(v, &mut self.aux[1]),
                AuxInverse::AdjInserts(v) => validate_restore_splice(v, &mut self.aux[2]),
                AuxInverse::AdjDeletes(v) => validate_restore_splice(v, &mut self.aux[3]),
                AuxInverse::VectorDeletes(v) => validate_restore_splice(v, &mut self.aux[4]),
                AuxInverse::VectorMoves(v) => validate_restore_splice(v, &mut self.aux[5]),
                AuxInverse::Provenance(v) => validate_restore_splice(v, &mut self.aux[6]),
                AuxInverse::ConfigWrites(v) => validate_restore_splice(v, &mut self.aux[7]),
                AuxInverse::ConfigDeletes(v) => validate_restore_splice(v, &mut self.aux[8]),
                AuxInverse::ConfigMaxU64Keys(v) => validate_restore_splice(v, &mut self.aux[9]),
            },
            SourceUndo::Adopt { state, .. } => {
                *self = Self::from_state(state);
                self.validate_alignment();
            }
            SourceUndo::Flags(_) | SourceUndo::Sidecars(_) => {}
            SourceUndo::RetiredAdopt { .. } => {
                panic!("retired custody cannot be an active inverse")
            }
        }
    }
}
fn validate_restore_splice<T>(
    inverse: &VecInverse<T>,
    fields: &mut (RestoreDimensions, RestoreDimensions),
) {
    let (body, invoice) = fields;
    assert_eq!(
        body.len, invoice.len,
        "original auxiliary invoice alignment"
    );
    assert!(
        inverse.at <= body.len && inverse.installed <= body.len - inverse.at,
        "installed auxiliary splice range"
    );
    assert!(
        inverse.inserted.values.is_empty()
            && inverse.inserted.values.capacity() >= inverse.installed,
        "prepaid auxiliary retirement bodies"
    );
    assert!(
        inverse.inserted.invoices.len() == 0
            && inverse.inserted.invoices.capacity() >= inverse.installed,
        "prepaid auxiliary retirement invoices"
    );
    for _ in 0..inverse.installed {
        body.remove(inverse.at);
        invoice.remove(inverse.at);
    }
    for removed in inverse.removed.as_slice().iter().rev() {
        body.insert(removed.position);
        invoice.insert(removed.position);
    }
    if let Some(old) = &inverse.storage {
        body.storage(RestoreDimensions::vector(&old.values));
    }
    if let Some(old) = &inverse.invoice_storage {
        invoice.storage(RestoreDimensions::buffer(old));
    }
}
struct RetiredCheckpointNode {
    custody: Option<RetiredTransactionCheckpoint>,
    next: RetiredCheckpointNodes,
}
type RetiredCheckpointNodeOwner = crate::staged_source::FundedBox<RetiredCheckpointNode>;
impl crate::staged_source::FundedBox<RetiredCheckpointNode> {
    fn prepare(funder: &SourceFunder) -> Result<Self> {
        Self::try_new(
            RetiredCheckpointNode {
                custody: None,
                next: RetiredCheckpointNodes::empty(),
            },
            funder,
        )
    }
    fn validate_empty(&self) {
        let node = self.get();
        assert!(
            node.custody.is_none() && node.next.0.is_none(),
            "unused original retirement node"
        );
    }
}
struct RetiredCheckpointNodes(Option<RetiredCheckpointNodeOwner>);
impl RetiredCheckpointNodes {
    fn empty() -> Self {
        Self(None)
    }
    fn push(&mut self, mut node: RetiredCheckpointNodeOwner) {
        node.get_mut().next.0 = self.0.take();
        self.0 = Some(node);
    }
    fn pop(&mut self) -> Option<RetiredCheckpointNodeOwner> {
        let mut node = self.0.take()?;
        self.0 = node.get_mut().next.0.take();
        Some(node)
    }
    fn append_from(&mut self, source: &mut Self) {
        while let Some(node) = source.pop() {
            self.push(node);
        }
    }
}
impl Drop for RetiredCheckpointNodes {
    fn drop(&mut self) {
        while let Some(node) = self.pop() {
            drop(node);
        }
    }
}
// Drain only the touched host (or visit_active's already selected whole map).
// The same existing active-map mutex is released before any retired owner dies,
// including when the operation callback unwinds.
struct EngineRetirementLock<'a> {
    active: Option<MutexGuard<'a, ActiveTransactions>>,
    tx: Option<TxId>,
}
impl std::ops::Deref for EngineRetirementLock<'_> {
    type Target = ActiveTransactions;
    fn deref(&self) -> &Self::Target {
        self.active.as_deref().expect("active retirement lock")
    }
}
impl std::ops::DerefMut for EngineRetirementLock<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.active.as_deref_mut().expect("active retirement lock")
    }
}
impl Drop for EngineRetirementLock<'_> {
    fn drop(&mut self) {
        let mut retired = RetiredCheckpointNodes::empty();
        if let Some(mut active) = self.active.take() {
            if let Some(tx) = self.tx {
                if let Some(host) = active.engine_mut().get_mut(&tx) {
                    retired.append_from(&mut host.deferred);
                }
            } else {
                // The internal visitor only moves prepaid links; it cannot refuse.
                let _ = active.engine_mut().visit_mut(&mut |_, host| {
                    retired.append_from(&mut host.deferred);
                    Ok(())
                });
            }
            drop(active);
        }
        drop(retired);
    }
}
struct DetachedRetirementDrain<'a> {
    host: &'a mut EngineWriteSet,
}
impl Drop for DetachedRetirementDrain<'_> {
    fn drop(&mut self) {
        drop(self.host.deferred.0.take());
    }
}
struct RetiredTransactionCheckpoint {
    inverses: SourceJournalLinks,
    spare: SourceJournalLinks,
    frame: Option<TxFrame>,
    frame_storage: Option<SourceBuffer<TxFrame>>,
}
impl RetiredTransactionCheckpoint {
    fn empty() -> Self {
        Self {
            inverses: SourceJournalLinks::empty(),
            spare: SourceJournalLinks::empty(),
            frame: None,
            frame_storage: None,
        }
    }
}
struct PreparedSourceUndo {
    nodes: SourceJournalLinks,
    expected_len: usize,
    expected_depth: usize,
    records: usize,
}
impl EngineGuard<'_> {
    fn binding(&self) -> Result<TxBinding> {
        let next_revision = self
            .host
            .revision
            .checked_add(1)
            .ok_or_else(source_identity_error)?;
        next_revision
            .checked_add(
                u64::try_from(self.host.checkpoint_depth).map_err(|_| source_identity_error())?,
            )
            .ok_or_else(source_identity_error)?;
        Ok(TxBinding {
            tx: self.host.tx,
            generation: self.host.generation,
            scope: self.preparation_scope,
            revision: self.host.revision,
            depth: self.host.checkpoint_depth,
            next_revision,
            root: self.host.state.root.clone(),
        })
    }
    fn validate_binding(&self, p: &TxBinding) {
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.scope == self.preparation_scope
                && p.scope == self.host.preparation_scope
                && p.revision == self.host.revision
                && p.depth == self.host.checkpoint_depth
                && p.root.ptr_eq(&self.host.state.root),
            "prepared transaction binding changed"
        );
    }
    fn record_undo(&mut self, undo: SourceUndo) {
        if self.host.checkpoint_depth != 0 {
            self.host.undo.push(undo);
        } else {
            drop(undo);
        }
    }
    fn prepare_undo(&self, records: usize) -> Result<Option<PreparedSourceUndo>> {
        if self.host.checkpoint_depth == 0 {
            return Ok(None);
        }
        self.host
            .undo
            .len()
            .checked_add(records)
            .ok_or_else(source_identity_error)?;
        self.host
            .undo
            .spare_len
            .checked_add(records)
            .ok_or_else(source_identity_error)?;
        let mut nodes = SourceJournalLinks::empty();
        for _ in 0..records {
            nodes.push(SourceJournalNodeOwner::prepare(self.funder)?);
        }
        Ok(Some(PreparedSourceUndo {
            nodes,
            expected_len: self.host.undo.len(),
            expected_depth: self.host.checkpoint_depth,
            records,
        }))
    }
    fn publish_undo(&mut self, prepared: Option<PreparedSourceUndo>) {
        if let Some(mut p) = prepared {
            assert_eq!(p.expected_depth, self.host.checkpoint_depth);
            assert_eq!(p.expected_len, self.host.undo.len());
            while let Some(node) = p.nodes.pop() {
                self.host.undo.spare.push(node);
            }
            self.host.undo.spare_len += p.records;
        } else {
            assert_eq!(self.host.checkpoint_depth, 0);
        }
    }
    fn remove_ordinary_row(&mut self, position: usize) {
        let body = self.host.state.ws.relational_inserts.remove(position);
        let debit = self.host.state.row_debits.remove(position);
        let table_charge = self.host.state.row_table_charges.remove(position);
        let slot = self.host.state.row_slots.remove(position);
        self.record_undo(SourceUndo::RemovedRow {
            position,
            body,
            debit,
            table_charge,
            slot,
        });
    }
    fn remove_ordinary_vector(&mut self, position: usize) {
        let body = self.host.state.ws.vector_inserts.remove(position);
        let debit = self.host.state.vector_debits.remove(position);
        let slot = self.host.state.vector_slots.remove(position);
        self.record_undo(SourceUndo::RemovedVector {
            position,
            body,
            debit,
            slot,
        });
    }
    pub fn prepare_checkpoint(&self) -> Result<PreparedTxCheckpoint> {
        self.validate_aux_inventory()?;
        let binding = self.binding()?;
        let depth = self
            .host
            .checkpoint_depth
            .checked_add(1)
            .ok_or_else(source_identity_error)?;
        binding
            .next_revision
            .checked_add(u64::try_from(depth).map_err(|_| source_identity_error())?)
            .ok_or_else(source_identity_error)?;
        let nonce = self
            .host
            .next_frame
            .checked_add(1)
            .ok_or_else(source_identity_error)?;
        let frames = if depth > self.host.frames.capacity() {
            Some(SourceBuffer::with_capacity(
                depth.max(
                    self.host
                        .frames
                        .capacity()
                        .checked_mul(2)
                        .ok_or_else(source_identity_error)?,
                ),
                self.funder,
            )?)
        } else {
            None
        };
        let eager = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_checkpoint(self.funder)?;
        let frame = TxFrame {
            id: TxCheckpointId {
                tx: self.host.tx,
                generation: self.host.generation,
                nonce,
                depth,
            },
            marker: self.host.undo.len(),
            root: self.host.state.root.clone(),
            eager: Some(eager),
            displaced_eager: None,
            retirement: Some(RetiredCheckpointNodeOwner::prepare(self.funder)?),
            flags: EngineFlags::from_ws(&self.host.state.ws),
            base: self.host.state.ws.base_read.clone(),
            storage: None,
        };
        Ok(PreparedTxCheckpoint {
            binding,
            frame,
            frames,
        })
    }
    fn validate_prepared_checkpoint(&self, p: &PreparedTxCheckpoint) {
        self.validate_binding(&p.binding);
        assert_eq!(self.host.next_frame.checked_add(1), Some(p.frame.id.nonce));
        assert_eq!(
            self.host.frames.len().checked_add(1),
            Some(p.frame.id.depth)
        );
        assert_eq!(p.frame.id.tx, self.host.tx);
        assert_eq!(p.frame.id.generation, self.host.generation);
        assert_eq!(p.frame.marker, self.host.undo.len());
        assert!(
            p.frame.eager.is_some()
                && p.frame.displaced_eager.is_none()
                && p.frame.storage.is_none()
                && p.frame.retirement.is_some()
        );
        p.frame
            .retirement
            .as_ref()
            .expect("prepared retirement node")
            .validate_empty();
        let capacity = p
            .frames
            .as_ref()
            .map_or(self.host.frames.capacity(), |frames| {
                assert_eq!(frames.len(), 0, "prepared empty frame storage");
                frames.capacity()
            });
        assert!(capacity >= p.frame.id.depth, "prepared frame capacity");
    }
    pub fn begin_checkpoint(&mut self, mut p: PreparedTxCheckpoint) -> TxCheckpointId {
        self.validate_prepared_checkpoint(&mut p);
        self.install_prepared_checkpoint(p)
    }
    fn install_prepared_checkpoint(&mut self, mut p: PreparedTxCheckpoint) -> TxCheckpointId {
        if let Some(mut replacement) = p.frames.take() {
            self.host.frames.append_into(&mut replacement);
            p.frame.storage = Some(std::mem::replace(&mut self.host.frames, replacement));
        }
        let id = TxCheckpointId {
            tx: p.frame.id.tx,
            generation: p.frame.id.generation,
            nonce: p.frame.id.nonce,
            depth: p.frame.id.depth,
        };
        self.host.next_frame = id.nonce;
        self.host.checkpoint_depth = id.depth;
        self.host.revision = p.binding.next_revision;
        self.host.frames.push(p.frame);
        id
    }
    fn validate_checkpoint(&self, id: &TxCheckpointId, settle: bool) {
        let top = self
            .host
            .frames
            .as_slice()
            .last()
            .expect("active transaction frame");
        assert!(
            id.tx == self.host.tx
                && id.generation == self.host.generation
                && id.depth == self.host.frames.len()
                && id.depth == self.host.checkpoint_depth
                && id.nonce == top.id.nonce
                && id.depth == top.id.depth
                && id.tx == top.id.tx
                && id.generation == top.id.generation,
            "transaction checkpoint must consume its original top frame"
        );
        assert!(
            top.marker <= self.host.undo.len(),
            "prepared journal marker"
        );
        assert!(self.host.revision < u64::MAX, "prepaid checkpoint revision");
        assert!(
            top.eager.is_some() && top.displaced_eager.is_none() && top.retirement.is_some(),
            "original eager frame"
        );
        top.retirement
            .as_ref()
            .expect("original retirement node")
            .validate_empty();
        if let Some(storage) = &top.storage {
            assert!(
                storage.len() == 0 && storage.capacity() >= self.host.frames.len() - 1,
                "preserved frame storage"
            );
        }
        let mut dimensions = RestoreInventory::from_state(&self.host.state);
        dimensions.validate_alignment();
        let mut cursor = self.host.undo.active.0.as_ref();
        for remaining in (0..self.host.undo.len()).rev() {
            let node = cursor.expect("original source journal node").get();
            let undo = node.undo.as_ref().expect("original inverse body");
            if !settle && remaining >= top.marker {
                dimensions.inverse(undo);
            }
            cursor = node.next.0.as_ref();
        }
        assert!(cursor.is_none(), "original source journal length");
        dimensions.validate_alignment();
    }
    fn take_frame(&mut self, id: TxCheckpointId, settle: bool) -> TxFrame {
        self.validate_checkpoint(&id, settle);
        self.host.frames.pop().expect("validated frame")
    }
    fn finish_frame_storage(
        &mut self,
        storage: Option<SourceBuffer<TxFrame>>,
    ) -> Option<SourceBuffer<TxFrame>> {
        let retired = storage.map(|mut old| {
            self.host.frames.append_into(&mut old);
            std::mem::replace(&mut self.host.frames, old)
        });
        self.host.checkpoint_depth = self.host.frames.len();
        self.host.revision += 1;
        retired
    }
    fn consume_checkpoint_retired(
        &mut self,
        id: TxCheckpointId,
        settle: bool,
    ) -> RetiredCheckpointNodeOwner {
        let mut frame = self.take_frame(id, settle);
        let mut owner = frame.retirement.take().expect("validated retirement node");
        let mut retired = RetiredTransactionCheckpoint::empty();
        if !settle {
            while self.host.undo.len() > frame.marker {
                let mut node = self.host.undo.pop().expect("prepared transaction inverse");
                let inverse = node.get_mut().undo.take().expect("prepared inverse body");
                node.get_mut().undo = self.restore_inverse(inverse);
                retired.inverses.push(node);
            }
            std::mem::swap(&mut self.host.state.root, &mut frame.root);
            frame.displaced_eager =
                Some(std::mem::take(&mut self.host.state.ws.ordered_staged_rows));
            self.host
                .state
                .ws
                .ordered_staged_rows
                .restore_native_checkpoint(frame.eager.take().expect("prepared eager restore"));
            std::mem::swap(&mut self.host.state.ws.base_read, &mut frame.base);
            let flags =
                std::mem::replace(&mut frame.flags, EngineFlags::from_ws(&self.host.state.ws));
            flags.install(&mut self.host.state.ws);
        } else if self.host.frames.len() == 0 {
            // Child settle preserves every inverse for the parent's restore.
            retired.inverses.0 = self.host.undo.active.0.take();
            self.host.undo.len = 0;
        }
        if self.host.frames.len() == 0 {
            retired.spare.0 = self.host.undo.spare.0.take();
            self.host.undo.spare_len = 0;
        }
        retired.frame_storage = self.finish_frame_storage(frame.storage.take());
        retired.frame = Some(frame);
        owner.get_mut().custody = Some(retired);
        owner
    }
    pub fn restore_checkpoint(&mut self, id: TxCheckpointId) {
        let retired = self.consume_checkpoint_retired(id, false);
        self.host.deferred.push(retired);
    }
    pub fn settle_checkpoint(&mut self, id: TxCheckpointId) {
        let retired = self.consume_checkpoint_retired(id, true);
        self.host.deferred.push(retired);
    }
    pub fn with_tx_checkpoint<R>(
        &mut self,
        f: impl FnOnce(&mut EngineGuard<'_>) -> Result<R>,
    ) -> Result<R> {
        let p = self.prepare_checkpoint()?;
        let id = self.begin_checkpoint(p);
        let mut scope = GuardCheckpoint {
            guard: self,
            id: Some(id),
        };
        let result = f(scope.guard);
        if result.is_ok() {
            let id = scope.id.take().expect("active checkpoint");
            scope.guard.settle_checkpoint(id);
        }
        result
    }
    fn swap_sidecars(
        &mut self,
        mut maps: crate::staged_source::AdmittedSidecars,
    ) -> crate::staged_source::AdmittedSidecars {
        std::mem::swap(
            &mut self.host.state.ws.relational_insert_source_lsns,
            &mut maps.lsns,
        );
        std::mem::swap(
            &mut self.host.state.ws.relational_insert_source_kinds,
            &mut maps.kinds,
        );
        std::mem::swap(
            &mut self.host.state.ws.relational_insert_admitted_bytes,
            &mut maps.admitted,
        );
        std::mem::swap(&mut self.host.state.sidecars_credit, &mut maps.credit);
        maps
    }
    fn restore_inverse(&mut self, undo: SourceUndo) -> Option<SourceUndo> {
        let retired = match undo {
            SourceUndo::InsertedRow(position) => SourceUndo::RemovedRow {
                position,
                body: self.host.state.ws.relational_inserts.remove(position),
                debit: self.host.state.row_debits.remove(position),
                table_charge: self.host.state.row_table_charges.remove(position),
                slot: self.host.state.row_slots.remove(position),
            },
            SourceUndo::RemovedRow {
                position,
                body,
                debit,
                table_charge,
                slot,
            } => {
                self.host.state.ws.relational_inserts.insert(position, body);
                self.host.state.row_debits.insert(position, debit);
                self.host
                    .state
                    .row_table_charges
                    .insert(position, table_charge);
                self.host.state.row_slots.insert(position, slot);
                return None;
            }
            SourceUndo::InsertedVector(position) => SourceUndo::RemovedVector {
                position,
                body: self.host.state.ws.vector_inserts.remove(position),
                debit: self.host.state.vector_debits.remove(position),
                slot: self.host.state.vector_slots.remove(position),
            },
            SourceUndo::RemovedVector {
                position,
                body,
                debit,
                slot,
            } => {
                self.host.state.ws.vector_inserts.insert(position, body);
                self.host.state.vector_debits.insert(position, debit);
                self.host.state.vector_slots.insert(position, slot);
                return None;
            }
            SourceUndo::RowsStorage(mut old) => {
                old.values
                    .append(&mut self.host.state.ws.relational_inserts);
                std::mem::swap(&mut self.host.state.ws.relational_inserts, &mut old.values);
                std::mem::swap(&mut self.host.state.ordinary_rows_charge, &mut old.charge);
                SourceUndo::RowsStorage(old)
            }
            SourceUndo::VectorsStorage(mut old) => {
                old.values.append(&mut self.host.state.ws.vector_inserts);
                std::mem::swap(&mut self.host.state.ws.vector_inserts, &mut old.values);
                std::mem::swap(
                    &mut self.host.state.ordinary_vectors_charge,
                    &mut old.charge,
                );
                SourceUndo::VectorsStorage(old)
            }
            SourceUndo::RowSlotsStorage(mut old) => {
                self.host.state.row_slots.append_into(&mut old);
                SourceUndo::RowSlotsStorage(std::mem::replace(&mut self.host.state.row_slots, old))
            }
            SourceUndo::VectorSlotsStorage(mut old) => {
                self.host.state.vector_slots.append_into(&mut old);
                SourceUndo::VectorSlotsStorage(std::mem::replace(
                    &mut self.host.state.vector_slots,
                    old,
                ))
            }
            SourceUndo::RowDebitsStorage(mut old) => {
                self.host.state.row_debits.append_into(&mut old);
                SourceUndo::RowDebitsStorage(std::mem::replace(
                    &mut self.host.state.row_debits,
                    old,
                ))
            }
            SourceUndo::VectorDebitsStorage(mut old) => {
                self.host.state.vector_debits.append_into(&mut old);
                SourceUndo::VectorDebitsStorage(std::mem::replace(
                    &mut self.host.state.vector_debits,
                    old,
                ))
            }
            SourceUndo::TableChargesStorage(mut old) => {
                self.host.state.row_table_charges.append_into(&mut old);
                SourceUndo::TableChargesStorage(std::mem::replace(
                    &mut self.host.state.row_table_charges,
                    old,
                ))
            }
            SourceUndo::Headers(mut image) => {
                self.restore_headers(&mut image);
                SourceUndo::Headers(image)
            }
            SourceUndo::Flags(flags) => {
                flags.install(&mut self.host.state.ws);
                return None;
            }
            SourceUndo::Sidecars(maps) => SourceUndo::Sidecars(self.swap_sidecars(maps)),
            SourceUndo::Aux(inverse) => SourceUndo::Aux(self.restore_aux(inverse)),
            SourceUndo::Adopt { mut state, eager } => {
                let displaced_eager = std::mem::take(&mut state.ws.ordered_staged_rows);
                state
                    .ws
                    .ordered_staged_rows
                    .restore_native_checkpoint(eager);
                SourceUndo::RetiredAdopt {
                    state: std::mem::replace(&mut self.host.state, state),
                    displaced_eager,
                }
            }
            SourceUndo::RetiredAdopt { .. } => {
                unreachable!("retired state is never an active inverse")
            }
        };
        Some(retired)
    }
    fn prepare_headers(&self) -> Result<HeaderImage> {
        let ws = &self.host.state.ws;
        let mut rows = SourceBuffer::with_capacity(ws.relational_inserts.len(), self.funder)?;
        for (_, row) in &ws.relational_inserts {
            rows.push(RowHeader {
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
                lsn: row.lsn,
                created_at: row.created_at,
            });
        }
        let mut vectors = SourceBuffer::with_capacity(ws.vector_inserts.len(), self.funder)?;
        for v in &ws.vector_inserts {
            vectors.push(VectorHeader {
                created_tx: v.created_tx,
                deleted_tx: v.deleted_tx,
                lsn: v.lsn,
            });
        }
        let mut row_slots =
            SourceBuffer::with_capacity(self.host.state.row_slots.len(), self.funder)?;
        for cell in self.host.state.row_slots.as_slice() {
            row_slots.push(cell.clone());
        }
        let mut vector_slots =
            SourceBuffer::with_capacity(self.host.state.vector_slots.len(), self.funder)?;
        for cell in self.host.state.vector_slots.as_slice() {
            vector_slots.push(cell.clone());
        }
        let mut relational_deletes =
            SourceBuffer::with_capacity(ws.relational_deletes.len(), self.funder)?;
        for (_, _, tx) in &ws.relational_deletes {
            relational_deletes.push(*tx);
        }
        let mut adj = SourceBuffer::with_capacity(ws.adj_inserts.len(), self.funder)?;
        for v in &ws.adj_inserts {
            adj.push(VectorHeader {
                created_tx: v.created_tx,
                deleted_tx: v.deleted_tx,
                lsn: v.lsn,
            });
        }
        let mut adj_deletes = SourceBuffer::with_capacity(ws.adj_deletes.len(), self.funder)?;
        for (_, _, _, tx) in &ws.adj_deletes {
            adj_deletes.push(*tx);
        }
        let mut vector_deletes = SourceBuffer::with_capacity(ws.vector_deletes.len(), self.funder)?;
        for (_, _, tx) in &ws.vector_deletes {
            vector_deletes.push(*tx);
        }
        let mut vector_moves = SourceBuffer::with_capacity(ws.vector_moves.len(), self.funder)?;
        for (_, _, _, tx) in &ws.vector_moves {
            vector_moves.push(*tx);
        }
        Ok(HeaderImage {
            rows,
            vectors,
            row_slots,
            vector_slots,
            relational_deletes,
            adj,
            adj_deletes,
            vector_deletes,
            vector_moves,
            flags: EngineFlags::from_ws(ws),
        })
    }
    fn restore_headers(&mut self, image: &mut HeaderImage) {
        let ws = &mut self.host.state.ws;
        assert_eq!(ws.relational_inserts.len(), image.rows.len());
        assert_eq!(ws.vector_inserts.len(), image.vectors.len());
        for ((_, row), old) in ws.relational_inserts.iter_mut().zip(image.rows.as_slice()) {
            row.created_tx = old.created_tx;
            row.deleted_tx = old.deleted_tx;
            row.lsn = old.lsn;
            row.created_at = old.created_at;
        }
        for (v, old) in ws.vector_inserts.iter_mut().zip(image.vectors.as_slice()) {
            v.created_tx = old.created_tx;
            v.deleted_tx = old.deleted_tx;
            v.lsn = old.lsn;
        }
        assert_eq!(ws.relational_deletes.len(), image.relational_deletes.len());
        for ((_, _, tx), old) in ws
            .relational_deletes
            .iter_mut()
            .zip(image.relational_deletes.as_slice())
        {
            *tx = *old;
        }
        assert_eq!(ws.adj_inserts.len(), image.adj.len());
        for (v, old) in ws.adj_inserts.iter_mut().zip(image.adj.as_slice()) {
            v.created_tx = old.created_tx;
            v.deleted_tx = old.deleted_tx;
            v.lsn = old.lsn;
        }
        assert_eq!(ws.adj_deletes.len(), image.adj_deletes.len());
        for ((_, _, _, tx), old) in ws.adj_deletes.iter_mut().zip(image.adj_deletes.as_slice()) {
            *tx = *old;
        }
        assert_eq!(ws.vector_deletes.len(), image.vector_deletes.len());
        for ((_, _, tx), old) in ws
            .vector_deletes
            .iter_mut()
            .zip(image.vector_deletes.as_slice())
        {
            *tx = *old;
        }
        assert_eq!(ws.vector_moves.len(), image.vector_moves.len());
        for ((_, _, _, tx), old) in ws
            .vector_moves
            .iter_mut()
            .zip(image.vector_moves.as_slice())
        {
            *tx = *old;
        }
        let flags = std::mem::replace(&mut image.flags, EngineFlags::from_ws(ws));
        flags.install(ws);
        for (current, saved) in self
            .host
            .state
            .row_slots
            .values_mut()
            .iter_mut()
            .zip(image.row_slots.values_mut())
        {
            std::mem::swap(current, saved);
        }
        for (current, saved) in self
            .host
            .state
            .vector_slots
            .values_mut()
            .iter_mut()
            .zip(image.vector_slots.values_mut())
        {
            std::mem::swap(current, saved);
        }
    }
}
struct GuardCheckpoint<'a, 'b> {
    guard: &'a mut EngineGuard<'b>,
    id: Option<TxCheckpointId>,
}
impl Drop for GuardCheckpoint<'_, '_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.guard.restore_checkpoint(id);
        }
    }
}
#[doc(hidden)]
pub struct EngineCheckpoint<'a, S: WriteSetApplicator> {
    transactions: &'a EngineTransactions<S>,
    id: Option<TxCheckpointId>,
}
impl<S: WriteSetApplicator> EngineCheckpoint<'_, S> {
    pub fn restore(mut self) {
        let id = self.id.take().expect("active checkpoint");
        self.transactions.consume_checkpoint(id, false);
    }
    pub fn settle(mut self) {
        let id = self.id.take().expect("active checkpoint");
        self.transactions.consume_checkpoint(id, true);
    }
    #[doc(hidden)]
    pub fn restore_with_peer<V, P>(
        self,
        validate_peer: impl FnOnce() -> V,
        consume_peer: impl FnOnce(V) -> P,
    ) -> P {
        self.close_with_peer(false, validate_peer, consume_peer)
    }
    #[doc(hidden)]
    pub fn settle_with_peer<V, P>(
        self,
        validate_peer: impl FnOnce() -> V,
        consume_peer: impl FnOnce(V) -> P,
    ) -> P {
        self.close_with_peer(true, validate_peer, consume_peer)
    }
    fn close_with_peer<V, P>(
        self,
        settle: bool,
        validate_peer: impl FnOnce() -> V,
        consume_peer: impl FnOnce(V) -> P,
    ) -> P {
        // Suppress the lower-only destructor, without consuming the installed id.
        // A validation unwind leaves both frames intact and cannot split-close.
        let mut checkpoint = std::mem::ManuallyDrop::new(self);
        let transactions = checkpoint.transactions;
        let retired;
        let peer;
        {
            let mut active = transactions.manager.active_txs.lock();
            let id = checkpoint.id.as_ref().expect("active paired checkpoint");
            let host = active
                .engine_mut()
                .get_mut(&id.tx)
                .expect("paired checkpoint original node remains active");
            let scope = host.preparation_scope;
            let funder = host.funder.clone();
            let mut guard = EngineGuard {
                host,
                funder: &funder,
                preparation_scope: scope,
            };
            guard.validate_checkpoint(id, settle);
            let ticket = validate_peer(); // retains the peer lock through both mutations
            let id = checkpoint.id.take().expect("validated paired checkpoint");
            retired = guard.consume_checkpoint_retired(id, settle);
            peer = consume_peer(ticket); // peer returns custody; its lock ends here
        }
        drop(retired); // active-map and peer locks have both ended
        peer
    }
}
impl<S: WriteSetApplicator> Drop for EngineCheckpoint<'_, S> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.transactions.consume_checkpoint(id, false);
        }
    }
}
impl<S: WriteSetApplicator> EngineTransactions<S> {
    pub fn checkpoint(&self, tx: TxId) -> Result<EngineCheckpoint<'_, S>> {
        let id = self.with_engine(tx, |guard| {
            let p = guard.prepare_checkpoint()?;
            Ok(guard.begin_checkpoint(p))
        })?;
        // with_engine released the active map before callers run arbitrary hooks.
        Ok(EngineCheckpoint {
            transactions: self,
            id: Some(id),
        })
    }
    fn consume_checkpoint(&self, id: TxCheckpointId, settle: bool) {
        let retired;
        {
            let mut active = self.manager.active_txs.lock();
            let host = active
                .engine_mut()
                .get_mut(&id.tx)
                .expect("checkpoint original node remains active");
            let scope = host.preparation_scope;
            let funder = host.funder.clone();
            let mut guard = EngineGuard {
                host,
                funder: &funder,
                preparation_scope: scope,
            };
            retired = guard.consume_checkpoint_retired(id, settle);
        }
        drop(retired);
    }
}
/// The existing restricted private caller shape uses the general frame and
/// journal; its wrapper adds no independent checkpoint storage.
#[doc(hidden)]
pub struct EngineSourceCheckpoint<'a, 'b> {
    guard: &'a mut EngineGuard<'b>,
}
impl EngineSourceCheckpoint<'_, '_> {
    pub fn read(&self) -> EngineRead<'_> {
        self.guard.read()
    }
    pub fn prepare_schema(
        &self,
        table: &str,
        meta: &contextdb_core::TableMeta,
    ) -> Result<crate::staged_source::SchemaHandle> {
        self.guard.prepare_schema(table, meta)
    }
    pub fn prepare_row(
        &self,
        table: &str,
        row: VersionedRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        self.guard.prepare_row(table, row, keys, schema, invoice)
    }
    pub fn prepare_vector(
        &self,
        entry: VectorEntry,
        scalar: Option<(RowId, contextdb_core::ColName)>,
        invoice: crate::staged_source::OriginalVectorInvoice,
    ) -> Result<PreparedSourceMutation> {
        self.guard.prepare_vector(entry, scalar, invoice)
    }
    pub fn prepare_remove(&self, table: &str, id: RowId) -> Result<PreparedSourceMutation> {
        self.guard.prepare_remove(table, id)
    }
    pub fn prepare_remove_rows(
        &self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) -> Result<PreparedSourceMutation> {
        self.guard.prepare_remove_rows(selected)
    }
    pub fn prepare_remove_vector(
        &self,
        index: &VectorIndexRef,
        row: RowId,
    ) -> Result<PreparedSourceMutation> {
        self.guard.prepare_remove_vector(index, row)
    }
    pub fn prepare_canonicalize(&self) -> Result<PreparedSourceMutation> {
        self.guard.prepare_canonicalize()
    }
    pub fn prepare_identity(
        &self,
        from: TxId,
        to: TxId,
        lsn: Lsn,
    ) -> Result<PreparedSourceIdentity> {
        self.guard.prepare_identity(from, to, lsn)
    }
    pub fn publish_identity(&mut self, p: PreparedSourceIdentity) {
        self.guard.publish_identity(p)
    }
    pub fn publish(&mut self, p: PreparedSourceMutation) {
        self.guard.publish(p)
    }
    pub fn with_captured<R>(
        &self,
        source: &CanonicalStatementRows,
        id: RowId,
        f: impl for<'r> FnOnce(Option<&'r VersionedRow>) -> Result<R>,
    ) -> Result<R> {
        self.guard.with_captured(source, id, f)
    }
    pub fn with_source_checkpoint<R>(
        &mut self,
        f: impl FnOnce(&mut EngineSourceCheckpoint<'_, '_>) -> Result<R>,
    ) -> Result<R> {
        self.guard.with_source_checkpoint(f)
    }
}
impl EngineGuard<'_> {
    pub fn with_source_checkpoint<R>(
        &mut self,
        f: impl FnOnce(&mut EngineSourceCheckpoint<'_, '_>) -> Result<R>,
    ) -> Result<R> {
        self.with_tx_checkpoint(|guard| f(&mut EngineSourceCheckpoint { guard }))
    }
}

struct AuxStorage<T> {
    values: Vec<T>,
    credit: Option<crate::staged_source::SourceCharge>,
}
struct RemovedAux<T> {
    position: usize,
    body: T,
    invoice: crate::staged_source::OriginalAuxInvoice,
}
struct VecInverse<T> {
    at: usize,
    installed: usize,
    removed: SourceBuffer<RemovedAux<T>>,
    inserted: AuxInput<T>,
    storage: Option<AuxStorage<T>>,
    invoice_storage: Option<SourceBuffer<crate::staged_source::OriginalAuxInvoice>>,
}
struct AuxInput<T> {
    values: Vec<T>,
    invoices: SourceBuffer<crate::staged_source::OriginalAuxInvoice>,
    credit: Option<crate::staged_source::SourceCharge>,
}
struct PreparedSplice<T> {
    at: usize,
    selection: SourceBuffer<usize>,
    insert: Vec<T>,
    invoices: SourceBuffer<crate::staged_source::OriginalAuxInvoice>,
    input_credit: Option<crate::staged_source::SourceCharge>,
    removed: SourceBuffer<RemovedAux<T>>,
    storage: Option<AuxStorage<T>>,
    invoice_storage: Option<SourceBuffer<crate::staged_source::OriginalAuxInvoice>>,
}
fn prepare_splice<T>(
    values: &Vec<T>,
    funding: &AuxFunding,
    at: usize,
    remove: &[usize],
    input: AuxInput<T>,
    funder: &SourceFunder,
) -> Result<PreparedSplice<T>> {
    if values.len() != funding.invoices.len() || input.values.len() != input.invoices.len() {
        return Err(source_identity_error());
    }
    let mut selection = SourceBuffer::with_capacity(remove.len(), funder)?;
    for position in remove {
        if *position >= values.len() {
            return Err(source_identity_error());
        }
        selection.push(*position);
    }
    selection.values_mut().sort_unstable();
    if selection.as_slice().windows(2).any(|p| p[0] == p[1]) {
        return Err(source_identity_error());
    }
    let surviving = values
        .len()
        .checked_sub(remove.len())
        .ok_or_else(source_identity_error)?;
    if at > surviving {
        return Err(source_identity_error());
    }
    let needed = surviving
        .checked_add(input.values.len())
        .ok_or_else(source_identity_error)?;
    // Growth must also hold pre-edit contents until removals run.
    let capacity = needed.max(values.len());
    let storage = if needed > values.capacity() {
        let (values, credit) = SourceBuffer::<T>::with_capacity(capacity, funder)?.into_parts();
        Some(AuxStorage { values, credit })
    } else {
        None
    };
    let invoice_storage = if needed > funding.invoices.capacity() {
        Some(SourceBuffer::with_capacity(capacity, funder)?)
    } else {
        None
    };
    let removed = SourceBuffer::with_capacity(remove.len(), funder)?;
    Ok(PreparedSplice {
        at,
        selection,
        insert: input.values,
        invoices: input.invoices,
        input_credit: input.credit,
        removed,
        storage,
        invoice_storage,
    })
}
fn publish_splice<T>(
    values: &mut Vec<T>,
    funding: &mut AuxFunding,
    mut p: PreparedSplice<T>,
) -> VecInverse<T> {
    let storage = p.storage.take().map(|mut replacement| {
        replacement.values.append(values);
        AuxStorage {
            values: std::mem::replace(values, replacement.values),
            credit: std::mem::replace(&mut funding.buffer_credit, replacement.credit),
        }
    });
    let invoice_storage = p.invoice_storage.take().map(|mut replacement| {
        funding.invoices.append_into(&mut replacement);
        std::mem::replace(&mut funding.invoices, replacement)
    });
    for position in p.selection.as_slice().iter().rev() {
        let body = values.remove(*position);
        let invoice = funding.invoices.remove(*position);
        p.removed.push(RemovedAux {
            position: *position,
            body,
            invoice,
        });
    }
    let installed = p.insert.len();
    for (offset, body) in p.insert.drain(..).enumerate() {
        assert!(values.len() < values.capacity());
        values.insert(p.at + offset, body);
        let invoice = p.invoices.remove(0);
        funding.invoices.insert(p.at + offset, invoice);
    }
    VecInverse {
        at: p.at,
        installed,
        removed: p.removed,
        inserted: AuxInput {
            values: p.insert,
            invoices: p.invoices,
            credit: p.input_credit,
        },
        storage,
        invoice_storage,
    }
}

fn restore_splice<T>(
    values: &mut Vec<T>,
    funding: &mut AuxFunding,
    mut inverse: VecInverse<T>,
) -> VecInverse<T> {
    for _ in 0..inverse.installed {
        inverse.inserted.values.push(values.remove(inverse.at));
        inverse
            .inserted
            .invoices
            .push(funding.invoices.remove(inverse.at));
    }
    while let Some(RemovedAux {
        position,
        body,
        invoice,
    }) = inverse.removed.pop()
    {
        values.insert(position, body);
        funding.invoices.insert(position, invoice);
    }
    if let Some(mut old) = inverse.storage.take() {
        old.values.append(values);
        std::mem::swap(values, &mut old.values);
        std::mem::swap(&mut funding.buffer_credit, &mut old.credit);
        inverse.storage = Some(old);
    }
    if let Some(mut old) = inverse.invoice_storage.take() {
        funding.invoices.append_into(&mut old);
        inverse.invoice_storage = Some(std::mem::replace(&mut funding.invoices, old));
    }
    inverse
}

enum PreparedAux {
    RelationalDeletes(
        PreparedSplice<(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    Predicates(PreparedSplice<crate::write_set::RelationalDeletePredicate>),
    AdjInserts(PreparedSplice<contextdb_core::AdjEntry>),
    AdjDeletes(
        PreparedSplice<(
            contextdb_core::NodeId,
            contextdb_core::EdgeType,
            contextdb_core::NodeId,
            contextdb_core::TxId,
        )>,
    ),
    VectorDeletes(
        PreparedSplice<(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    VectorMoves(
        PreparedSplice<(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    Provenance(
        PreparedSplice<(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::Lsn,
            u8,
        )>,
    ),
    ConfigWrites(PreparedSplice<(String, Vec<u8>)>),
    ConfigDeletes(PreparedSplice<String>),
    ConfigMaxU64Keys(PreparedSplice<String>),
}
enum AuxInverse {
    RelationalDeletes(
        VecInverse<(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    Predicates(VecInverse<crate::write_set::RelationalDeletePredicate>),
    AdjInserts(VecInverse<contextdb_core::AdjEntry>),
    AdjDeletes(
        VecInverse<(
            contextdb_core::NodeId,
            contextdb_core::EdgeType,
            contextdb_core::NodeId,
            contextdb_core::TxId,
        )>,
    ),
    VectorDeletes(
        VecInverse<(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    VectorMoves(
        VecInverse<(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    Provenance(
        VecInverse<(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::Lsn,
            u8,
        )>,
    ),
    ConfigWrites(VecInverse<(String, Vec<u8>)>),
    ConfigDeletes(VecInverse<String>),
    ConfigMaxU64Keys(VecInverse<String>),
}
#[doc(hidden)]
pub struct PreparedTxMutation {
    binding: TxBinding,
    kind: TxMutationKind,
    undo: Option<PreparedSourceUndo>,
}
enum TxMutationKind {
    ConditionalSlice(PreparedConditionalSlice),
    RowWithSidecars(
        PreparedSourceMutation,
        crate::staged_source::AdmittedSidecars,
    ),
    Aux(PreparedAux),
    Sidecars(crate::staged_source::AdmittedSidecars),
    Flags(EngineFlags),
}
impl EngineGuard<'_> {
    fn validate_aux_inventory(&self) -> Result<()> {
        if self.host.state.ws.relational_deletes.len()
            != self.host.state.relational_deletes_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.relational_delete_predicates.len()
            != self
                .host
                .state
                .relational_delete_predicates_funding
                .invoices
                .len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.adj_inserts.len()
            != self.host.state.adj_inserts_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.adj_deletes.len()
            != self.host.state.adj_deletes_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.vector_deletes.len()
            != self.host.state.vector_deletes_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.vector_moves.len()
            != self.host.state.vector_moves_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.sync_source_provenance_marks.len()
            != self
                .host
                .state
                .sync_source_provenance_marks_funding
                .invoices
                .len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.config_writes.len()
            != self.host.state.config_writes_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.config_deletes.len()
            != self.host.state.config_deletes_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }
        if self.host.state.ws.config_max_u64_keys.len()
            != self.host.state.config_max_u64_keys_funding.invoices.len()
        {
            return Err(Error::Other(
                "unfunded mutable auxiliary image requires producer migration".into(),
            ));
        }

        if (!self.host.state.ws.relational_insert_source_lsns.is_empty()
            || !self.host.state.ws.relational_insert_source_kinds.is_empty()
            || !self
                .host
                .state
                .ws
                .relational_insert_admitted_bytes
                .is_empty())
            && self.host.state.sidecars_credit.is_none()
        {
            return Err(Error::Other(
                "unfunded mutable sidecars require producer migration".into(),
            ));
        }
        Ok(())
    }
    /// Move an original debit into a singleton auxiliary batch before publication.
    pub fn prepare_owned_aux(
        &self,
        body: crate::staged_source::AuxBody,
        invoice: crate::staged_source::OriginalAuxInvoice,
    ) -> Result<crate::staged_source::AdmittedAuxBatch> {
        struct OriginalAux {
            body: crate::staged_source::AuxBody,
            invoice: crate::staged_source::OriginalAuxInvoice,
        }
        let original = OriginalAux { body, invoice };
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let body_bytes = match &original.body {
            crate::staged_source::AuxBody::RelationalDelete(_) => std::mem::size_of::<(
                contextdb_core::TableName,
                contextdb_core::RowId,
                contextdb_core::TxId,
            )>(),
            crate::staged_source::AuxBody::Predicate(_) => {
                std::mem::size_of::<crate::write_set::RelationalDeletePredicate>()
            }
            crate::staged_source::AuxBody::AdjInsert(_) => {
                std::mem::size_of::<contextdb_core::AdjEntry>()
            }
            crate::staged_source::AuxBody::AdjDelete(_) => std::mem::size_of::<(
                contextdb_core::NodeId,
                contextdb_core::EdgeType,
                contextdb_core::NodeId,
                contextdb_core::TxId,
            )>(),
            crate::staged_source::AuxBody::VectorDelete(_) => std::mem::size_of::<(
                contextdb_core::VectorIndexRef,
                contextdb_core::RowId,
                contextdb_core::TxId,
            )>(),
            crate::staged_source::AuxBody::VectorMove(_) => std::mem::size_of::<(
                contextdb_core::VectorIndexRef,
                contextdb_core::RowId,
                contextdb_core::RowId,
                contextdb_core::TxId,
            )>(),
            crate::staged_source::AuxBody::Provenance(_) => std::mem::size_of::<(
                contextdb_core::TableName,
                contextdb_core::RowId,
                contextdb_core::Lsn,
                u8,
            )>(),
            crate::staged_source::AuxBody::ConfigWrite(_) => {
                std::mem::size_of::<(String, Vec<u8>)>()
            }
            crate::staged_source::AuxBody::ConfigDelete(_) => std::mem::size_of::<String>(),
            crate::staged_source::AuxBody::ConfigMaxU64Key(_) => std::mem::size_of::<String>(),
        };
        let bytes = body_bytes
            .checked_add(std::mem::size_of::<crate::staged_source::OriginalAuxInvoice>())
            .ok_or_else(source_identity_error)?;
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let bodies = match original.body {
            crate::staged_source::AuxBody::RelationalDelete(body) => {
                crate::staged_source::AuxBodies::RelationalDeletes(vec![body])
            }
            crate::staged_source::AuxBody::Predicate(body) => {
                crate::staged_source::AuxBodies::Predicates(vec![body])
            }
            crate::staged_source::AuxBody::AdjInsert(body) => {
                crate::staged_source::AuxBodies::AdjInserts(vec![body])
            }
            crate::staged_source::AuxBody::AdjDelete(body) => {
                crate::staged_source::AuxBodies::AdjDeletes(vec![body])
            }
            crate::staged_source::AuxBody::VectorDelete(body) => {
                crate::staged_source::AuxBodies::VectorDeletes(vec![body])
            }
            crate::staged_source::AuxBody::VectorMove(body) => {
                crate::staged_source::AuxBodies::VectorMoves(vec![body])
            }
            crate::staged_source::AuxBody::Provenance(body) => {
                crate::staged_source::AuxBodies::Provenance(vec![body])
            }
            crate::staged_source::AuxBody::ConfigWrite(body) => {
                crate::staged_source::AuxBodies::ConfigWrites(vec![body])
            }
            crate::staged_source::AuxBody::ConfigDelete(body) => {
                crate::staged_source::AuxBodies::ConfigDeletes(vec![body])
            }
            crate::staged_source::AuxBody::ConfigMaxU64Key(body) => {
                crate::staged_source::AuxBodies::ConfigMaxU64Keys(vec![body])
            }
        };
        Ok(crate::staged_source::AdmittedAuxBatch::from_parts(
            bodies,
            vec![original.invoice],
            credit,
        ))
    }
    pub fn copy_aux(
        &self,
        bodies: crate::staged_source::AuxBorrow<'_>,
    ) -> Result<crate::staged_source::AdmittedAuxBatch> {
        self.funder.copy_aux(bodies)
    }
    pub fn prepare_aux_splice(
        &self,
        at: usize,
        remove: &[usize],
        insert: crate::staged_source::AdmittedAuxBatch,
    ) -> Result<PreparedTxMutation> {
        insert.validate(self.funder)?;
        self.validate_aux_inventory()?;
        let binding = self.binding()?;
        let undo = self.prepare_undo(1)?;
        let crate::staged_source::AdmittedAuxBatch {
            bodies,
            invoices,
            buffer_credit,
        } = insert;
        let kind = match bodies {
            crate::staged_source::AuxBodies::RelationalDeletes(values) => {
                PreparedAux::RelationalDeletes(prepare_splice(
                    &self.host.state.ws.relational_deletes,
                    &self.host.state.relational_deletes_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::Predicates(values) => {
                PreparedAux::Predicates(prepare_splice(
                    &self.host.state.ws.relational_delete_predicates,
                    &self.host.state.relational_delete_predicates_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::AdjInserts(values) => {
                PreparedAux::AdjInserts(prepare_splice(
                    &self.host.state.ws.adj_inserts,
                    &self.host.state.adj_inserts_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::AdjDeletes(values) => {
                PreparedAux::AdjDeletes(prepare_splice(
                    &self.host.state.ws.adj_deletes,
                    &self.host.state.adj_deletes_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::VectorDeletes(values) => {
                PreparedAux::VectorDeletes(prepare_splice(
                    &self.host.state.ws.vector_deletes,
                    &self.host.state.vector_deletes_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::VectorMoves(values) => {
                PreparedAux::VectorMoves(prepare_splice(
                    &self.host.state.ws.vector_moves,
                    &self.host.state.vector_moves_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::Provenance(values) => {
                PreparedAux::Provenance(prepare_splice(
                    &self.host.state.ws.sync_source_provenance_marks,
                    &self.host.state.sync_source_provenance_marks_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::ConfigWrites(values) => {
                PreparedAux::ConfigWrites(prepare_splice(
                    &self.host.state.ws.config_writes,
                    &self.host.state.config_writes_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::ConfigDeletes(values) => {
                PreparedAux::ConfigDeletes(prepare_splice(
                    &self.host.state.ws.config_deletes,
                    &self.host.state.config_deletes_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
            crate::staged_source::AuxBodies::ConfigMaxU64Keys(values) => {
                PreparedAux::ConfigMaxU64Keys(prepare_splice(
                    &self.host.state.ws.config_max_u64_keys,
                    &self.host.state.config_max_u64_keys_funding,
                    at,
                    remove,
                    AuxInput {
                        values,
                        invoices,
                        credit: buffer_credit,
                    },
                    self.funder,
                )?)
            }
        };
        Ok(PreparedTxMutation {
            binding,
            kind: TxMutationKind::Aux(kind),
            undo,
        })
    }
    pub fn prepare_sidecars(
        &self,
        table: &str,
        row: RowId,
        source: Option<(Lsn, u8)>,
        admitted: Option<usize>,
    ) -> Result<PreparedTxMutation> {
        let maps = self
            .funder
            .copy_sidecars(self.sidecars(), Some((table, row, source, admitted)))?;
        Ok(PreparedTxMutation {
            binding: self.binding()?,
            kind: TxMutationKind::Sidecars(maps),
            undo: self.prepare_undo(1)?,
        })
    }
    fn sidecars(&self) -> crate::staged_source::SidecarsBorrow<'_> {
        let ws = &self.host.state.ws;
        crate::staged_source::SidecarsBorrow {
            lsns: &ws.relational_insert_source_lsns,
            kinds: &ws.relational_insert_source_kinds,
            admitted: &ws.relational_insert_admitted_bytes,
        }
    }
    pub fn prepare_flags(&self, flags: EngineFlags) -> Result<PreparedTxMutation> {
        Ok(PreparedTxMutation {
            binding: self.binding()?,
            kind: TxMutationKind::Flags(flags),
            undo: self.prepare_undo(1)?,
        })
    }
    pub fn publish_tx(&mut self, mut p: PreparedTxMutation) {
        self.validate_binding(&p.binding);
        if let TxMutationKind::ConditionalSlice(slice) = p.kind {
            self.publish_conditional_slice(slice, p.undo.take());
            self.host.revision = p.binding.next_revision;
            return;
        }
        if let TxMutationKind::RowWithSidecars(row, maps) = p.kind {
            self.publish(row);
            let old = self.swap_sidecars(maps);
            self.record_undo(SourceUndo::Sidecars(old));
            return;
        }
        self.publish_undo(p.undo.take());
        self.host.revision = p.binding.next_revision;
        match p.kind {
            TxMutationKind::ConditionalSlice(_) => unreachable!(),
            TxMutationKind::RowWithSidecars(_, _) => unreachable!(),
            TxMutationKind::Flags(flags) => {
                let old = EngineFlags::from_ws(&self.host.state.ws);
                self.record_undo(SourceUndo::Flags(old));
                flags.install(&mut self.host.state.ws);
            }
            TxMutationKind::Sidecars(maps) => {
                let old = self.swap_sidecars(maps);
                self.record_undo(SourceUndo::Sidecars(old));
            }
            TxMutationKind::Aux(aux) => {
                let inverse = match aux {
                    PreparedAux::RelationalDeletes(p) => {
                        AuxInverse::RelationalDeletes(publish_splice(
                            &mut self.host.state.ws.relational_deletes,
                            &mut self.host.state.relational_deletes_funding,
                            p,
                        ))
                    }
                    PreparedAux::Predicates(p) => AuxInverse::Predicates(publish_splice(
                        &mut self.host.state.ws.relational_delete_predicates,
                        &mut self.host.state.relational_delete_predicates_funding,
                        p,
                    )),
                    PreparedAux::AdjInserts(p) => AuxInverse::AdjInserts(publish_splice(
                        &mut self.host.state.ws.adj_inserts,
                        &mut self.host.state.adj_inserts_funding,
                        p,
                    )),
                    PreparedAux::AdjDeletes(p) => AuxInverse::AdjDeletes(publish_splice(
                        &mut self.host.state.ws.adj_deletes,
                        &mut self.host.state.adj_deletes_funding,
                        p,
                    )),
                    PreparedAux::VectorDeletes(p) => AuxInverse::VectorDeletes(publish_splice(
                        &mut self.host.state.ws.vector_deletes,
                        &mut self.host.state.vector_deletes_funding,
                        p,
                    )),
                    PreparedAux::VectorMoves(p) => AuxInverse::VectorMoves(publish_splice(
                        &mut self.host.state.ws.vector_moves,
                        &mut self.host.state.vector_moves_funding,
                        p,
                    )),
                    PreparedAux::Provenance(p) => AuxInverse::Provenance(publish_splice(
                        &mut self.host.state.ws.sync_source_provenance_marks,
                        &mut self.host.state.sync_source_provenance_marks_funding,
                        p,
                    )),
                    PreparedAux::ConfigWrites(p) => AuxInverse::ConfigWrites(publish_splice(
                        &mut self.host.state.ws.config_writes,
                        &mut self.host.state.config_writes_funding,
                        p,
                    )),
                    PreparedAux::ConfigDeletes(p) => AuxInverse::ConfigDeletes(publish_splice(
                        &mut self.host.state.ws.config_deletes,
                        &mut self.host.state.config_deletes_funding,
                        p,
                    )),
                    PreparedAux::ConfigMaxU64Keys(p) => {
                        AuxInverse::ConfigMaxU64Keys(publish_splice(
                            &mut self.host.state.ws.config_max_u64_keys,
                            &mut self.host.state.config_max_u64_keys_funding,
                            p,
                        ))
                    }
                };
                self.record_undo(SourceUndo::Aux(inverse));
            }
        }
    }
    fn restore_aux(&mut self, inverse: AuxInverse) -> AuxInverse {
        match inverse {
            AuxInverse::RelationalDeletes(inverse) => {
                AuxInverse::RelationalDeletes(restore_splice(
                    &mut self.host.state.ws.relational_deletes,
                    &mut self.host.state.relational_deletes_funding,
                    inverse,
                ))
            }
            AuxInverse::Predicates(inverse) => AuxInverse::Predicates(restore_splice(
                &mut self.host.state.ws.relational_delete_predicates,
                &mut self.host.state.relational_delete_predicates_funding,
                inverse,
            )),
            AuxInverse::AdjInserts(inverse) => AuxInverse::AdjInserts(restore_splice(
                &mut self.host.state.ws.adj_inserts,
                &mut self.host.state.adj_inserts_funding,
                inverse,
            )),
            AuxInverse::AdjDeletes(inverse) => AuxInverse::AdjDeletes(restore_splice(
                &mut self.host.state.ws.adj_deletes,
                &mut self.host.state.adj_deletes_funding,
                inverse,
            )),
            AuxInverse::VectorDeletes(inverse) => AuxInverse::VectorDeletes(restore_splice(
                &mut self.host.state.ws.vector_deletes,
                &mut self.host.state.vector_deletes_funding,
                inverse,
            )),
            AuxInverse::VectorMoves(inverse) => AuxInverse::VectorMoves(restore_splice(
                &mut self.host.state.ws.vector_moves,
                &mut self.host.state.vector_moves_funding,
                inverse,
            )),
            AuxInverse::Provenance(inverse) => AuxInverse::Provenance(restore_splice(
                &mut self.host.state.ws.sync_source_provenance_marks,
                &mut self.host.state.sync_source_provenance_marks_funding,
                inverse,
            )),
            AuxInverse::ConfigWrites(inverse) => AuxInverse::ConfigWrites(restore_splice(
                &mut self.host.state.ws.config_writes,
                &mut self.host.state.config_writes_funding,
                inverse,
            )),
            AuxInverse::ConfigDeletes(inverse) => AuxInverse::ConfigDeletes(restore_splice(
                &mut self.host.state.ws.config_deletes,
                &mut self.host.state.config_deletes_funding,
                inverse,
            )),
            AuxInverse::ConfigMaxU64Keys(inverse) => AuxInverse::ConfigMaxU64Keys(restore_splice(
                &mut self.host.state.ws.config_max_u64_keys,
                &mut self.host.state.config_max_u64_keys_funding,
                inverse,
            )),
        }
    }
}

#[doc(hidden)]
pub struct PreparedSourceAdoption {
    state: TxState,
    eager: crate::ordered_staged::NativeOrderedCheckpoint,
    binding: TxBinding,
    undo: Option<PreparedSourceUndo>,
    retired: RetiredCheckpointNodes,
}
#[doc(hidden)]
pub type PreparedTxAdoption = PreparedSourceAdoption;
impl EngineGuard<'_> {
    pub fn prepare_adoption(
        &self,
        build: impl FnOnce(&mut EngineGuard<'_>) -> Result<()>,
    ) -> Result<PreparedSourceAdoption> {
        self.prepare_general_adoption(build)
    }
    pub fn prepare_general_adoption(
        &self,
        build: impl FnOnce(&mut EngineGuard<'_>) -> Result<()>,
    ) -> Result<PreparedTxAdoption> {
        let binding = self.binding()?;
        let undo = self.prepare_undo(1)?;
        let eager = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_checkpoint(self.funder)?;
        let base = self
            .host
            .state
            .ws
            .base_read
            .clone()
            .ok_or_else(source_identity_error)?;
        let mut candidate =
            EngineWriteSet::new(self.host.tx, base, self.funder, self.host.preparation)?;
        candidate.generation = self.host.generation;
        candidate.preparation_scope = self.preparation_scope;
        build(&mut EngineGuard {
            host: &mut candidate,
            funder: self.funder,
            preparation_scope: self.preparation_scope,
        })?;
        candidate.state.ws.base_read = self.host.state.ws.base_read.clone();
        candidate.validate_settled()?;
        EngineGuard {
            host: &mut candidate,
            funder: self.funder,
            preparation_scope: self.preparation_scope,
        }
        .validate_aux_inventory()?;
        Ok(PreparedSourceAdoption {
            state: candidate.state,
            eager,
            binding,
            undo,
            retired: candidate.deferred,
        })
    }
    pub fn publish_adoption(&mut self, p: PreparedSourceAdoption) {
        self.publish_general_adoption(p)
    }
    pub fn publish_general_adoption(&mut self, mut p: PreparedTxAdoption) {
        self.validate_binding(&p.binding);
        self.host.deferred.append_from(&mut p.retired);
        self.publish_undo(p.undo.take());
        let mut old = std::mem::replace(&mut self.host.state, p.state);
        // Replaced state is bookkeeping custody. Its protected native backing
        // stays in the prepared nonsemantic pin until restoration.
        drop(std::mem::take(&mut old.ws.ordered_staged_rows));
        self.record_undo(SourceUndo::Adopt {
            state: old,
            eager: p.eager,
        });
        self.host.revision = p.binding.next_revision;
    }
}
impl EngineWriteSet {
    fn validate_settled(&self) -> Result<()> {
        if self.checkpoint_depth != 0 {
            Err(Error::Other(
                "transaction has an outstanding checkpoint".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl EngineGuard<'_> {
    pub fn prepare_eager_row(
        &self,
        table: &str,
        row: VersionedRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let position = crate::ordered_staged::StagedPosition {
            row_id: original.row.row_id,
            created_tx: original.row.created_tx,
            position: self.host.state.ws.relational_inserts.len(),
        };
        let schema_copy = schema.clone();
        let mut prepared =
            self.prepare_row_inner(false, table, original.row, keys, schema, original.invoice)?;
        let PreparedSourceMutationKind::Row(body) = &prepared.mutation else {
            unreachable!()
        };
        prepared.eager = Some(
            self.host
                .state
                .ws
                .ordered_staged_rows
                .prepare_native_row(
                    table,
                    position,
                    &body.row.row,
                    keys,
                    schema_copy,
                    self.funder,
                )?
                .prepare_retain_row(body.cell.clone(), self.funder)?,
        );
        Ok(prepared)
    }
    pub fn prepare_eager_vector(
        &self,
        entry: VectorEntry,
        scalar: Option<(RowId, contextdb_core::ColName)>,
        invoice: crate::staged_source::OriginalVectorInvoice,
    ) -> Result<PreparedSourceMutation> {
        let mut prepared = self.prepare_vector_inner(false, entry, scalar, invoice)?;
        let PreparedSourceMutationKind::Vector(body) = &prepared.mutation else {
            unreachable!()
        };
        prepared.eager = Some(
            self.host
                .state
                .ws
                .ordered_staged_rows
                .prepare_native_owned_vector(&body.original.entry, self.funder)?
                .prepare_retain_vector(body.cell.clone(), self.funder)?,
        );
        Ok(prepared)
    }
}

impl<S: WriteSetApplicator> EngineTransactions<S> {
    fn ensure_settled(&self, tx: TxId) -> Result<()> {
        let active = self.manager.active_txs.lock();
        active
            .engine()
            .get(&tx)
            .ok_or(Error::TxNotFound(tx))?
            .validate_settled()
    }
}

#[doc(hidden)]
pub struct EngineCapturedWriteSet {
    host: EngineWriteSet,
}
impl EngineCapturedWriteSet {
    pub fn with_legacy<R>(&self, f: impl FnOnce(&WriteSet) -> R) -> R {
        f(&self.host.state.ws)
    }
    /// Mutate the distinct funded capture without reacquiring a transaction map
    /// or commit boundary. The capture retains every original copy invoice.
    #[doc(hidden)]
    pub fn with_engine<R>(
        &mut self,
        f: impl for<'g, 'h> FnOnce(&'g mut EngineGuard<'h>) -> Result<R>,
    ) -> Result<R> {
        let funder = self.host.funder.clone();
        let preparation_scope = self.host.open_guard()?;
        let drain = DetachedRetirementDrain {
            host: &mut self.host,
        };
        f(&mut EngineGuard {
            host: &mut *drain.host,
            funder: &funder,
            preparation_scope,
        })
    }
}
impl EngineGuard<'_> {
    pub fn prepare_row_with_sidecars(
        &self,
        table: &str,
        row: VersionedRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        invoice: crate::staged_source::OriginalRowInvoice,
        source: Option<(Lsn, u8)>,
        admitted: Option<usize>,
    ) -> Result<PreparedTxMutation> {
        let mut row = self.prepare_row(table, row, keys, schema, invoice)?;
        let PreparedSourceMutationKind::Row(body) = &mut row.mutation else {
            unreachable!()
        };
        let maps = self.funder.copy_sidecars(
            self.sidecars(),
            Some((table, body.row.row.row_id, source, admitted)),
        )?;
        body.undo = self.prepare_undo(6)?;
        Ok(PreparedTxMutation {
            binding: self.binding()?,
            kind: TxMutationKind::RowWithSidecars(row, maps),
            undo: None,
        })
    }
    pub fn capture_private(&self) -> Result<EngineCapturedWriteSet> {
        self.validate_aux_inventory()?;
        let base = self
            .host
            .state
            .ws
            .base_read
            .clone()
            .ok_or_else(source_identity_error)?;
        let mut copy = EngineWriteSet::new(self.host.tx, base, self.funder, self.host.preparation)?;
        // Distinct ordinary body copies have genuine independent invoices.
        let (rows, rows_charge) = SourceBuffer::<(String, VersionedRow)>::with_capacity(
            self.host.state.ws.relational_inserts.len(),
            self.funder,
        )?
        .into_parts();
        copy.state.ws.relational_inserts = rows;
        copy.state.ordinary_rows_charge = rows_charge;
        copy.state.row_debits =
            SourceBuffer::with_capacity(self.host.state.row_debits.len(), self.funder)?;
        copy.state.row_table_charges =
            SourceBuffer::with_capacity(self.host.state.row_table_charges.len(), self.funder)?;
        copy.state.row_slots =
            SourceBuffer::with_capacity(self.host.state.row_slots.len(), self.funder)?;
        for ((table, body), cell) in self
            .host
            .state
            .ws
            .relational_inserts
            .iter()
            .zip(self.host.state.row_slots.as_slice())
        {
            let row = self.funder.copy_row(body)?;
            let credit =
                crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), table.len())?;
            let name = table.as_str().to_owned();
            copy.state.ws.relational_inserts.push((name, row.row));
            copy.state.row_debits.push(row.invoice);
            copy.state.row_table_charges.push(credit);
            copy.state.row_slots.push(cell.clone());
        }
        let (vectors, vectors_charge) = SourceBuffer::<VectorEntry>::with_capacity(
            self.host.state.ws.vector_inserts.len(),
            self.funder,
        )?
        .into_parts();
        copy.state.ws.vector_inserts = vectors;
        copy.state.ordinary_vectors_charge = vectors_charge;
        copy.state.vector_debits =
            SourceBuffer::with_capacity(self.host.state.vector_debits.len(), self.funder)?;
        copy.state.vector_slots =
            SourceBuffer::with_capacity(self.host.state.vector_slots.len(), self.funder)?;
        for (body, cell) in self
            .host
            .state
            .ws
            .vector_inserts
            .iter()
            .zip(self.host.state.vector_slots.as_slice())
        {
            let vector = self.funder.copy_vector(body)?;
            copy.state.ws.vector_inserts.push(vector.entry);
            copy.state.vector_debits.push(vector.invoice);
            copy.state.vector_slots.push(cell.clone());
        }
        self.copy_aux_state(&mut copy.state)?;
        let maps = self.funder.copy_sidecars(self.sidecars(), None)?;
        copy.state.ws.relational_insert_source_lsns = maps.lsns;
        copy.state.ws.relational_insert_source_kinds = maps.kinds;
        copy.state.ws.relational_insert_admitted_bytes = maps.admitted;
        copy.state.sidecars_credit = maps.credit;
        EngineFlags::from_ws(&self.host.state.ws).install(&mut copy.state.ws);
        copy.state.root = self.host.state.root.clone();
        // The native directory's nonsemantic pin is promoted into exactly one
        // genuine immutable capture owner. No public Clone/header is retained.
        let native = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_checkpoint(self.funder)?;
        copy.state
            .ws
            .ordered_staged_rows
            .restore_native_checkpoint(native);
        Ok(EngineCapturedWriteSet { host: copy })
    }
}

impl EngineGuard<'_> {
    fn copy_aux_state(&self, copy: &mut TxState) -> Result<()> {
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::RelationalDeletes(
                &self.host.state.ws.relational_deletes,
            ))?;
        let crate::staged_source::AuxBodies::RelationalDeletes(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.relational_deletes = values;
        copy.relational_deletes_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::Predicates(
                &self.host.state.ws.relational_delete_predicates,
            ))?;
        let crate::staged_source::AuxBodies::Predicates(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.relational_delete_predicates = values;
        copy.relational_delete_predicates_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::AdjInserts(
                &self.host.state.ws.adj_inserts,
            ))?;
        let crate::staged_source::AuxBodies::AdjInserts(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.adj_inserts = values;
        copy.adj_inserts_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::AdjDeletes(
                &self.host.state.ws.adj_deletes,
            ))?;
        let crate::staged_source::AuxBodies::AdjDeletes(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.adj_deletes = values;
        copy.adj_deletes_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::VectorDeletes(
                &self.host.state.ws.vector_deletes,
            ))?;
        let crate::staged_source::AuxBodies::VectorDeletes(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.vector_deletes = values;
        copy.vector_deletes_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::VectorMoves(
                &self.host.state.ws.vector_moves,
            ))?;
        let crate::staged_source::AuxBodies::VectorMoves(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.vector_moves = values;
        copy.vector_moves_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::Provenance(
                &self.host.state.ws.sync_source_provenance_marks,
            ))?;
        let crate::staged_source::AuxBodies::Provenance(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.sync_source_provenance_marks = values;
        copy.sync_source_provenance_marks_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::ConfigWrites(
                &self.host.state.ws.config_writes,
            ))?;
        let crate::staged_source::AuxBodies::ConfigWrites(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.config_writes = values;
        copy.config_writes_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::ConfigDeletes(
                &self.host.state.ws.config_deletes,
            ))?;
        let crate::staged_source::AuxBodies::ConfigDeletes(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.config_deletes = values;
        copy.config_deletes_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        let batch = self
            .funder
            .copy_aux(crate::staged_source::AuxBorrow::ConfigMaxU64Keys(
                &self.host.state.ws.config_max_u64_keys,
            ))?;
        let crate::staged_source::AuxBodies::ConfigMaxU64Keys(values) = batch.bodies else {
            return Err(source_identity_error());
        };
        copy.ws.config_max_u64_keys = values;
        copy.config_max_u64_keys_funding = AuxFunding {
            invoices: batch.invoices,
            buffer_credit: batch.buffer_credit,
        };
        Ok(())
    }
}

impl EngineGuard<'_> {
    fn canonical_position_map(&self) -> Result<CanonicalPositions<'_>> {
        self.position_map(|position, table, row| self.is_current_row(position, table, row))
    }
    fn removal_position_map(&self, table: &str, row: RowId) -> Result<CanonicalPositions<'_>> {
        self.position_map(|_, name, id| name != table || id != row)
    }
    fn position_map(
        &self,
        keep: impl Fn(usize, &str, RowId) -> bool,
    ) -> Result<CanonicalPositions<'_>> {
        let mut tables = 0usize;
        let mut bytes = 0usize;
        let mut after: Option<&str> = None;
        while let Some((name, _)) =
            crate::staged_source::seek_by(&self.host.state.root.tables, false, |name| {
                if after.is_some_and(|after| name.as_str() <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        {
            let count = self
                .host
                .state
                .ws
                .relational_inserts
                .iter()
                .enumerate()
                .filter(|(position, (table, row))| {
                    table == name.as_str() && keep(*position, table, row.row_id)
                })
                .count();
            if count != 0 {
                tables = tables.checked_add(1).ok_or_else(source_identity_error)?;
                bytes = bytes
                    .checked_add(crate::staged_source::native_map_bytes::<
                        RowId,
                        (usize, TxId),
                    >(count)?)
                    .ok_or_else(source_identity_error)?;
            }
            after = Some(name.as_str());
        }
        bytes = bytes
            .checked_add(crate::staged_source::native_map_bytes::<
                &str,
                HashMap<RowId, (usize, TxId)>,
            >(tables)?)
            .ok_or_else(source_identity_error)?;
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let mut maps = HashMap::with_capacity(tables);
        after = None;
        while let Some((name, _)) =
            crate::staged_source::seek_by(&self.host.state.root.tables, false, |name| {
                if after.is_some_and(|after| name.as_str() <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        {
            let count = self
                .host
                .state
                .ws
                .relational_inserts
                .iter()
                .enumerate()
                .filter(|(position, (table, row))| {
                    table == name.as_str() && keep(*position, table, row.row_id)
                })
                .count();
            if count != 0 {
                let mut rows = HashMap::with_capacity(count);
                let mut final_position = 0usize;
                for (position, (table, row)) in
                    self.host.state.ws.relational_inserts.iter().enumerate()
                {
                    if !keep(position, table, row.row_id) {
                        continue;
                    }
                    if table == name.as_str() {
                        rows.insert(row.row_id, (final_position, row.created_tx));
                    }
                    final_position = final_position
                        .checked_add(1)
                        .expect("prechecked native positions");
                }
                maps.insert(name.as_str(), rows);
            }
            after = Some(name.as_str());
        }
        Ok(CanonicalPositions {
            maps,
            _credit: credit,
        })
    }
    fn is_current_row(&self, position: usize, table: &str, row: RowId) -> bool {
        self.host
            .state
            .root
            .table(table)
            .and_then(|root| crate::staged_source::get_by(&root.by_id, |id| row.cmp(id)))
            .is_some_and(|cell| cell.ptr_eq(&self.host.state.row_slots.as_slice()[position]))
    }
}
struct CanonicalPositions<'a> {
    maps: HashMap<&'a str, HashMap<RowId, (usize, TxId)>>,
    _credit: crate::staged_source::SourceCharge,
}

impl PreparedTxCheckpoint {
    /// Read-only node/frame binding for the coupled engine metadata prepare.
    pub fn identity(&self) -> (TxId, u64, u64, usize) {
        let id = &self.frame.id;
        (id.tx, id.generation, id.nonce, id.depth)
    }
}
impl<S: WriteSetApplicator> EngineCheckpoint<'_, S> {
    pub fn identity(&self) -> (TxId, u64, u64, usize) {
        let id = self.id.as_ref().expect("active checkpoint");
        (id.tx, id.generation, id.nonce, id.depth)
    }
}
impl<S: WriteSetApplicator> EngineTransactions<S> {
    /// Only an engine metadata preparation callback belongs here. Both halves
    /// prepare before the transaction frame installs; user callbacks run after
    /// this method returns and its active-map lock has been released.
    pub fn checkpoint_with_peer<P>(
        &self,
        tx: TxId,
        prepare_peer: impl FnOnce(&PreparedTxCheckpoint) -> Result<P>,
    ) -> Result<(EngineCheckpoint<'_, S>, P)> {
        let (id, peer) = self.with_engine(tx, |guard| {
            let prepared = guard.prepare_checkpoint()?;
            guard.validate_prepared_checkpoint(&prepared);
            let peer = prepare_peer(&prepared)?;
            // Engine P retains its pending lock through this already validated install.
            let id = guard.install_prepared_checkpoint(prepared);
            Ok((id, peer))
        })?;
        Ok((
            EngineCheckpoint {
                transactions: self,
                id: Some(id),
            },
            peer,
        ))
    }
}

// Private observation rows: a real checkpoint is installed before its isolated
// generation mismatch is constructed. No normal checkpoint algorithm changes.
#[cfg(any(test, feature = "test-seams"))]
fn checkpoint_id_with_wrong_generation(id: &TxCheckpointId) -> TxCheckpointId {
    TxCheckpointId {
        tx: id.tx,
        generation: id.generation.checked_add(1).expect("test generation"),
        nonce: id.nonce,
        depth: id.depth,
    }
}

#[cfg(any(test, feature = "test-seams"))]
fn checkpoint_generation_refusal_rows(guard: &mut EngineGuard<'_>) -> Result<()> {
    let prepared = guard.prepare_checkpoint()?;
    let original_identity = prepared.identity();
    let actual = guard.begin_checkpoint(prepared);
    let wrong_generation = checkpoint_id_with_wrong_generation(&actual);
    let root = guard.host.state.root.clone();
    let revision = guard.host.revision;
    let marker = guard.host.undo.len();
    let depth = guard.host.frames.len();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || guard.restore_checkpoint(wrong_generation)
        ))
        .is_err()
    );
    assert!(guard.host.state.root.ptr_eq(&root));
    assert_eq!(
        (
            guard.host.revision,
            guard.host.undo.len(),
            guard.host.frames.len()
        ),
        (revision, marker, depth)
    );
    let top = &guard
        .host
        .frames
        .as_slice()
        .last()
        .expect("original frame")
        .id;
    assert_eq!(
        (top.tx, top.generation, top.nonce, top.depth),
        original_identity
    );
    guard.restore_checkpoint(actual);
    Ok(())
}

// Inputs to this private row are observations of actual engine accountants and
// the actual compiled native caller provider. This constructs no grant/funder.
#[cfg(any(test, feature = "test-seams"))]
struct NativeRemovalOwners<'a> {
    caller: &'a SourceFunder,
    original_used: &'a dyn Fn() -> usize,
    caller_used: &'a dyn Fn() -> usize,
    original_admitted: &'a dyn Fn() -> usize,
    caller_admitted: &'a dyn Fn() -> usize,
    set_original_limit: &'a dyn Fn(Option<usize>),
    set_caller_limit: &'a dyn Fn(Option<usize>),
}

#[cfg(any(test, feature = "test-seams"))]
fn distinct_native_removal_owner_rows(
    guard: &mut EngineGuard<'_>,
    owners: NativeRemovalOwners<'_>,
    table: &str,
    rows: [RowId; 2],
) -> Result<()> {
    assert_ne!(rows[0], rows[1]);
    assert!(
        guard
            .host
            .state
            .ws
            .ordered_staged_rows
            .native_semantic_holders()
            .is_some()
    );
    let original_root = guard.host.state.root.clone();
    let ordinary = guard.host.state.ws.relational_inserts.clone();
    let lsns = guard.host.state.ws.relational_insert_source_lsns.clone();
    let kinds = guard.host.state.ws.relational_insert_source_kinds.clone();
    let admitted = guard.host.state.ws.relational_insert_admitted_bytes.clone();
    let retained = guard.capture_private()?;
    retained.with_legacy(|ws| {
        let source = ws
            .ordered_staged_rows
            .statement_rows(table)
            .expect("actual positive native source");
        for id in rows {
            assert!(source.row(id).is_some());
        }
    });
    // Only this cfg helper can select a distinct caller for the already real
    // original host. All preparation/publication algorithms remain production.
    let mut caller = EngineGuard {
        host: &mut *guard.host,
        funder: owners.caller,
        preparation_scope: guard.preparation_scope,
    };
    for bulk in [false, true] {
        let prepare = |g: &EngineGuard<'_>| {
            if bulk {
                g.prepare_remove_rows(
                    &[(table.to_owned(), rows[0]), (table.to_owned(), rows[1])].into(),
                )
            } else {
                g.prepare_remove(table, rows[0])
            }
        };
        let original_before = (owners.original_used)();
        let caller_before = (owners.caller_used)();
        let caller_admitted = (owners.caller_admitted)();
        (owners.set_original_limit)(Some(original_before));
        (owners.set_caller_limit)(None);
        let outcome = prepare(&caller);
        (owners.set_original_limit)(None);
        assert!(matches!(outcome, Err(Error::MemoryBudgetExceeded { .. })));
        assert_eq!(
            (owners.caller_admitted)(),
            caller_admitted,
            "original refusal must never enter C"
        );
        assert_eq!(
            ((owners.original_used)(), (owners.caller_used)()),
            (original_before, caller_before)
        );
        assert!(caller.host.state.root.ptr_eq(&original_root));
        assert_eq!(caller.host.state.ws.relational_inserts, ordinary);
        assert_eq!(caller.host.state.ws.relational_insert_source_lsns, lsns);
        assert_eq!(caller.host.state.ws.relational_insert_source_kinds, kinds);
        assert_eq!(
            caller.host.state.ws.relational_insert_admitted_bytes,
            admitted
        );
        let original_admitted = (owners.original_admitted)();
        (owners.set_caller_limit)(Some(caller_before));
        let outcome = prepare(&caller);
        (owners.set_caller_limit)(None);
        assert!(matches!(outcome, Err(Error::MemoryBudgetExceeded { .. })));
        assert!(
            (owners.original_admitted)() > original_admitted,
            "real O reserve_copy must precede actual C refusal"
        );
        assert_eq!((owners.caller_admitted)(), caller_admitted);
        assert_eq!(
            ((owners.original_used)(), (owners.caller_used)()),
            (original_before, caller_before)
        );
        assert!(caller.host.state.root.ptr_eq(&original_root));
        assert_eq!(caller.host.state.ws.relational_inserts, ordinary);
        assert_eq!(caller.host.state.ws.relational_insert_source_lsns, lsns);
        assert_eq!(caller.host.state.ws.relational_insert_source_kinds, kinds);
        assert_eq!(
            caller.host.state.ws.relational_insert_admitted_bytes,
            admitted
        );
        retained.with_legacy(|ws| {
            let source = ws.ordered_staged_rows.statement_rows(table).unwrap();
            for id in rows {
                assert!(source.row(id).is_some());
            }
        });
        let prepared = caller.prepare_checkpoint()?;
        let checkpoint = caller.begin_checkpoint(prepared);
        let prepared = prepare(&caller)?;
        caller.publish(prepared);
        for id in rows {
            if bulk || id == rows[0] {
                assert!(
                    !caller
                        .host
                        .state
                        .ws
                        .relational_inserts
                        .iter()
                        .any(|(name, row)| name == table && row.row_id == id)
                );
                assert!(!caller.host.state.root.table(table).is_some_and(|root| {
                    crate::staged_source::get_by(&root.by_id, |row| id.cmp(row)).is_some()
                }));
                assert!(
                    !caller
                        .host
                        .state
                        .ws
                        .relational_insert_source_lsns
                        .get(table)
                        .is_some_and(|m| m.contains_key(&id))
                );
                assert!(
                    !caller
                        .host
                        .state
                        .ws
                        .relational_insert_source_kinds
                        .get(table)
                        .is_some_and(|m| m.contains_key(&id))
                );
                assert!(
                    !caller
                        .host
                        .state
                        .ws
                        .relational_insert_admitted_bytes
                        .get(table)
                        .is_some_and(|m| m.contains_key(&id))
                );
            }
        }
        let before_restore = ((owners.original_admitted)(), (owners.caller_admitted)());
        caller.restore_checkpoint(checkpoint);
        assert_eq!(
            ((owners.original_admitted)(), (owners.caller_admitted)()),
            before_restore
        );
        assert!(caller.host.state.root.ptr_eq(&original_root));
        assert_eq!(caller.host.state.ws.relational_inserts, ordinary);
        assert_eq!(caller.host.state.ws.relational_insert_source_lsns, lsns);
        assert_eq!(caller.host.state.ws.relational_insert_source_kinds, kinds);
        assert_eq!(
            caller.host.state.ws.relational_insert_admitted_bytes,
            admitted
        );
    }
    checkpoint_generation_refusal_rows(&mut caller)?;
    Ok(())
}

#[cfg(feature = "test-seams")]
impl EngineGuard<'_> {
    #[doc(hidden)]
    pub fn __test_distinct_native_checkpoint_rows(
        &mut self,
        caller: &SourceFunder,
        table: &str,
        rows: [RowId; 2],
        original_used: &dyn Fn() -> usize,
        caller_used: &dyn Fn() -> usize,
        original_admitted: &dyn Fn() -> usize,
        caller_admitted: &dyn Fn() -> usize,
        set_original_limit: &dyn Fn(Option<usize>),
        set_caller_limit: &dyn Fn(Option<usize>),
    ) -> contextdb_core::Result<()> {
        distinct_native_removal_owner_rows(
            self,
            NativeRemovalOwners {
                caller,
                original_used,
                caller_used,
                original_admitted,
                caller_admitted,
                set_original_limit,
                set_caller_limit,
            },
            table,
            rows,
        )
    }
}

impl EngineGuard<'_> {
    pub fn prepare_replace_row_at_from_meta(
        &self,
        position: usize,
        table: &str,
        row: VersionedRow,
        meta: &contextdb_core::TableMeta,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        if !self.eager_preparation() {
            return self.prepare_replace_row_at_from_meta_ordinary(
                position,
                table,
                original.row,
                meta,
                original.invoice,
            );
        }
        self.prepare_replace_row_slot_from_meta(
            position,
            table,
            original.row,
            meta,
            original.invoice,
        )
    }
    fn prepare_replace_row_at_from_meta_ordinary(
        &self,
        position: usize,
        table: &str,
        row: VersionedRow,
        meta: &contextdb_core::TableMeta,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        self.validate_slot_inventory()?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        struct KeyScratch {
            keys: Vec<(String, contextdb_core::IndexKey)>,
            _credit: crate::staged_source::SourceCharge,
        }
        let mut bytes = meta
            .indexes
            .len()
            .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
            .ok_or_else(source_identity_error)?;
        for index in &meta.indexes {
            bytes = bytes
                .checked_add(index.name.len())
                .and_then(|n| {
                    n.checked_add(
                        index
                            .columns
                            .len()
                            .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                    )
                })
                .ok_or_else(source_identity_error)?;
            for (column, _) in &index.columns {
                let value_bytes = original
                    .row
                    .values
                    .get(column)
                    .map_or(0, contextdb_core::Value::estimated_bytes);
                bytes = bytes
                    .checked_add(
                        value_bytes
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(128))
                            .ok_or_else(source_identity_error)?,
                    )
                    .ok_or_else(source_identity_error)?;
            }
        }
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let mut scratch = KeyScratch {
            keys: Vec::with_capacity(meta.indexes.len()),
            _credit: credit,
        };
        for index in &meta.indexes {
            let mut key = Vec::with_capacity(index.columns.len());
            for (column, direction) in &index.columns {
                let value = original
                    .row
                    .values
                    .get(column)
                    .cloned()
                    .unwrap_or(contextdb_core::Value::Null);
                key.push(match direction {
                    contextdb_core::SortDirection::Asc => {
                        contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                    }
                    contextdb_core::SortDirection::Desc => {
                        contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                    }
                });
            }
            scratch.keys.push((index.name.clone(), key));
        }
        let schema = self.prepare_schema(table, meta)?;
        self.prepare_replace_row_at_inner(position, table, original, &scratch.keys, schema)
    }
}

/// Exact native ordinals of the seven conditional operation collections.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteSetCounts {
    pub relational_inserts: usize,
    pub relational_deletes: usize,
    pub adj_inserts: usize,
    pub adj_deletes: usize,
    pub vector_inserts: usize,
    pub vector_deletes: usize,
    pub vector_moves: usize,
}
struct PreparedSlotReplacement {
    position: usize,
    row: crate::staged_source::AdmittedSourceRow,
    table: String,
    table_charge: crate::staged_source::SourceCharge,
    cell: RowCellHandle,
    root: FundedArc<SourceRoot>,
    undo: Option<PreparedSourceUndo>,
}
struct PreparedVectorPositions {
    positions: SourceBuffer<usize>,
    root: FundedArc<SourceRoot>,
    undo: Option<PreparedSourceUndo>,
}
struct PreparedConditionalSlice {
    eager: Option<crate::ordered_staged::PreparedNativeChange>,
    rows: SourceBuffer<usize>,
    vectors: SourceBuffer<usize>,
    root: FundedArc<SourceRoot>,
    relational_deletes: PreparedSplice<(contextdb_core::TableName, RowId, TxId)>,
    adj_inserts: PreparedSplice<contextdb_core::AdjEntry>,
    adj_deletes: PreparedSplice<(
        contextdb_core::NodeId,
        contextdb_core::EdgeType,
        contextdb_core::NodeId,
        TxId,
    )>,
    vector_deletes: PreparedSplice<(VectorIndexRef, RowId, TxId)>,
    vector_moves: PreparedSplice<(VectorIndexRef, RowId, RowId, TxId)>,
}
fn prepare_exact_positions(
    positions: &[usize],
    length: usize,
    funder: &SourceFunder,
) -> Result<SourceBuffer<usize>> {
    let mut selected = SourceBuffer::with_capacity(positions.len(), funder)?;
    for &position in positions {
        if position >= length {
            return Err(source_identity_error());
        }
        selected.push(position);
    }
    selected.values_mut().sort_unstable();
    if selected
        .as_slice()
        .windows(2)
        .any(|pair| pair[0] == pair[1])
    {
        return Err(source_identity_error());
    }
    Ok(selected)
}
fn prepare_empty_slice<T>(
    values: &Vec<T>,
    funding: &AuxFunding,
    start: usize,
    end: usize,
    funder: &SourceFunder,
) -> Result<PreparedSplice<T>> {
    if start > end || end > values.len() {
        return Err(source_identity_error());
    }
    let mut positions = SourceBuffer::with_capacity(end - start, funder)?;
    for position in start..end {
        positions.push(position);
    }
    prepare_splice(
        values,
        funding,
        start,
        positions.as_slice(),
        AuxInput {
            values: Vec::new(),
            invoices: SourceBuffer::empty(),
            credit: None,
        },
        funder,
    )
}
impl EngineGuard<'_> {
    fn validate_slot_inventory(&self) -> Result<()> {
        let state = &self.host.state;
        let rows = &state.ws.relational_inserts;
        let vectors = &state.ws.vector_inserts;
        if rows.len() != state.row_slots.len()
            || rows.len() != state.row_debits.len()
            || rows.len() != state.row_table_charges.len()
            || vectors.len() != state.vector_slots.len()
            || vectors.len() != state.vector_debits.len()
        {
            return Err(source_identity_error());
        }
        for (position, (table, row)) in rows.iter().enumerate() {
            let cell = &state.row_slots.as_slice()[position];
            if cell.identity.table.as_str() != table
                || !cell.identity.matches_row(row)
                || cell.identity.tx != self.host.tx
                || cell.identity.generation != self.host.generation
                || !state.row_debits.as_slice()[position].matches(self.funder)
                || !state.row_table_charges.as_slice()[position].matches_funder(self.funder)
                || !state
                    .ws
                    .base_read
                    .as_ref()
                    .is_some_and(|base| same_published_source(base, &cell.identity.base))
            {
                return Err(source_identity_error());
            }
            let custody = cell.custody.lock();
            if !matches!(custody.as_ref(), Some(RowCustody::Sealed { row: body, .. }) if cell.identity.matches_row(body))
            {
                return Err(source_identity_error());
            }
        }
        for (position, entry) in vectors.iter().enumerate() {
            let cell = &state.vector_slots.as_slice()[position];
            if !cell.identity.matches_entry(entry)
                || cell.identity.tx != self.host.tx
                || cell.identity.generation != self.host.generation
                || !state.vector_debits.as_slice()[position].matches(self.funder)
                || !state
                    .ws
                    .base_read
                    .as_ref()
                    .is_some_and(|base| same_published_source(base, &cell.identity.base))
            {
                return Err(source_identity_error());
            }
            let custody = cell.custody.lock();
            if !matches!(custody.as_ref(), Some(crate::staged_source::VectorCustody::Sealed { entry: body, .. }) if cell.identity.matches_entry(body))
            {
                return Err(source_identity_error());
            }
        }
        Ok(())
    }
    fn prepare_replace_row_at_inner(
        &self,
        position: usize,
        table: &str,
        original: crate::staged_source::AdmittedSourceRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
    ) -> Result<PreparedSourceMutation> {
        let old = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        if old.identity.table.as_str() != table || !old.identity.matches_row(&original.row) {
            return Err(source_identity_error());
        }
        let ordinary = self.funder.copy_row(&original.row)?;
        let mut bundle = SourceBuffer::with_capacity(keys.len(), self.funder)?;
        for (index, key) in keys {
            let name = schema.index_name(index).ok_or_else(source_identity_error)?;
            let control =
                FundedArc::<crate::staged_source::PostingAtom>::prepare(self.funder.clone())?;
            let key = self.funder.copy_key(key)?;
            bundle.push((
                name,
                FundedArc::from_prepared(
                    crate::staged_source::PostingAtom {
                        position: (key.key, original.row.row_id),
                        _payload: key.charge,
                    },
                    control,
                ),
            ));
        }
        let mut identity = old.identity.clone();
        identity.schema = schema;
        let cell = crate::staged_source::RowCell::sealed(identity, bundle, original, self.funder)?;
        let prior = self
            .host
            .state
            .root
            .table(table)
            .ok_or_else(source_identity_error)?;
        let current = crate::staged_source::get_by(&prior.by_id, |id| cell.identity.row.cmp(id))
            .ok_or_else(source_identity_error)?;
        let root = if current.ptr_eq(old) {
            let next = prior.prepare_replace(cell.clone(), self.funder)?;
            self.host
                .state
                .root
                .prepare_table(old.identity.table.clone(), next, self.funder)?
        } else {
            self.host.state.root.clone()
        };
        let table_charge =
            crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), table.len())?;
        let table_owned = table.to_owned();
        if table_owned.capacity() != table.len() {
            return Err(source_identity_error());
        }
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager: None,
            sidecars: None,
            mutation: PreparedSourceMutationKind::ReplaceSlot(PreparedSlotReplacement {
                position,
                row: ordinary,
                table: table_owned,
                table_charge,
                cell,
                root,
                undo: self.prepare_undo(2)?,
            }),
        })
    }
    fn publish_slot_replacement(&mut self, mut p: PreparedSlotReplacement) {
        let state = &self.host.state;
        let length = state.ws.relational_inserts.len();
        assert!(p.position < length);
        assert_eq!(length, state.row_slots.len());
        assert_eq!(length, state.row_debits.len());
        assert_eq!(length, state.row_table_charges.len());
        assert!(length <= state.ws.relational_inserts.capacity());
        assert!(length <= state.row_slots.capacity());
        assert!(length <= state.row_debits.capacity());
        assert!(length <= state.row_table_charges.capacity());
        self.publish_undo(p.undo.take());
        self.remove_ordinary_row(p.position);
        self.host
            .state
            .ws
            .relational_inserts
            .insert(p.position, (p.table, p.row.row));
        self.host.state.row_debits.insert(p.position, p.row.invoice);
        self.host
            .state
            .row_table_charges
            .insert(p.position, p.table_charge);
        self.host.state.row_slots.insert(p.position, p.cell);
        self.record_undo(SourceUndo::InsertedRow(p.position));
        self.host.state.root = p.root;
    }
    fn prepare_root_without_positions(
        &self,
        rows: &[usize],
        vectors: &[usize],
    ) -> Result<FundedArc<SourceRoot>> {
        use crate::staged_source::{Entry, get_by, insert};
        let mut row_winners = None;
        if !rows.is_empty() {
            for (ordinal, cell) in self.host.state.row_slots.as_slice().iter().enumerate() {
                if rows.binary_search(&ordinal).is_ok() {
                    continue;
                }
                row_winners = insert(
                    &row_winners,
                    FundedArc::try_new(
                        Entry {
                            key: (cell.identity.table.clone(), cell.identity.row),
                            value: cell.clone(),
                        },
                        self.funder.clone(),
                    )?,
                    self.funder,
                )?;
            }
        }
        let mut vector_winners = None;
        if !vectors.is_empty() {
            for (ordinal, cell) in self.host.state.vector_slots.as_slice().iter().enumerate() {
                if vectors.binary_search(&ordinal).is_ok() {
                    continue;
                }
                vector_winners = insert(
                    &vector_winners,
                    FundedArc::try_new(
                        Entry {
                            key: (cell.identity.index.clone(), cell.identity.row),
                            value: cell.clone(),
                        },
                        self.funder.clone(),
                    )?,
                    self.funder,
                )?;
            }
        }
        let mut root = self.host.state.root.clone();
        for &position in rows {
            let old = &self.host.state.row_slots.as_slice()[position];
            let table = old.identity.table.as_str();
            let id = old.identity.row;
            let prior = root.table(table).ok_or_else(source_identity_error)?;
            let current = crate::staged_source::get_by(&prior.by_id, |row| id.cmp(row));
            if !current.is_some_and(|cell| cell.ptr_eq(old)) {
                continue;
            }
            let survivor = get_by(
                &row_winners,
                |key: &(crate::staged_source::SourceName, RowId)| {
                    (table, id).cmp(&(key.0.as_str(), key.1))
                },
            );
            let next = match survivor {
                Some(cell) => prior.prepare_restore_slot(cell.clone(), self.funder)?,
                None => prior.prepare_remove(id, self.funder)?,
            };
            root = root.prepare_table(old.identity.table.clone(), next, self.funder)?;
        }
        for &position in vectors {
            let old = &self.host.state.vector_slots.as_slice()[position];
            let name = &old.identity.index;
            let prior = root
                .vector(name.table.as_str(), name.column.as_str())
                .ok_or_else(source_identity_error)?;
            let current =
                crate::staged_source::get_by(&prior.rows, |row| old.identity.row.cmp(row));
            if !current.is_some_and(|reference| reference.owner.ptr_eq(old)) {
                continue;
            }
            let survivor = get_by(
                &vector_winners,
                |key: &(crate::staged_source::VectorName, RowId)| {
                    (name, old.identity.row).cmp(&(&key.0, key.1))
                },
            );
            let next = match survivor {
                Some(cell) => prior.prepare_insert(
                    crate::staged_source::CanonicalVectorRef::owned(
                        cell.identity.row,
                        cell.clone(),
                    ),
                    self.funder,
                )?,
                None => prior.prepare_remove(old.identity.row, self.funder)?,
            };
            root = root.prepare_vector(name.clone(), next, self.funder)?;
        }
        Ok(root)
    }
    /// Remove exactly the selected insertion slots, including older same-key slots.
    #[doc(hidden)]
    pub fn prepare_remove_vector_positions(
        &self,
        positions: &[usize],
    ) -> Result<PreparedSourceMutation> {
        if !self.eager_preparation() {
            return self.prepare_remove_vector_positions_ordinary(positions);
        }
        self.validate_slot_inventory()?;
        // Sorting stays in caller preparation, after genuine original admissions.
        if positions
            .iter()
            .any(|slot| *slot >= self.host.state.vector_slots.len())
        {
            return Err(source_identity_error());
        }
        let admission = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_exact_admission(
                self.host.state.row_slots.as_slice(),
                self.host.state.vector_slots.as_slice(),
                crate::ordered_staged::NativeExactRowChange::Unchanged,
            )?;
        let (mut prepared, eager) = admission.prepare(
            self.funder,
            || {
                Ok((
                    self.prepare_remove_vector_positions_ordinary(positions)?,
                    None,
                ))
            },
            |p| match &p.mutation {
                PreparedSourceMutationKind::VectorPositions(p) => (&[], p.positions.as_slice()),
                _ => unreachable!(),
            },
        )?;
        prepared.eager = Some(eager);
        Ok(prepared)
    }
    fn prepare_remove_vector_positions_ordinary(
        &self,
        positions: &[usize],
    ) -> Result<PreparedSourceMutation> {
        self.validate_slot_inventory()?;
        let positions =
            prepare_exact_positions(positions, self.host.state.vector_slots.len(), self.funder)?;
        let root = self.prepare_root_without_positions(&[], positions.as_slice())?;
        let undo = self.prepare_undo(positions.len())?;
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager: None,
            sidecars: None,
            mutation: PreparedSourceMutationKind::VectorPositions(PreparedVectorPositions {
                positions,
                root,
                undo,
            }),
        })
    }
    fn publish_vector_positions(&mut self, mut p: PreparedVectorPositions) {
        let state = &self.host.state;
        let length = state.ws.vector_inserts.len();
        assert_eq!(length, state.vector_slots.len());
        assert_eq!(length, state.vector_debits.len());
        validate_exact_positions(p.positions.as_slice(), length);
        self.publish_undo(p.undo.take());
        for &position in p.positions.as_slice().iter().rev() {
            self.remove_ordinary_vector(position);
        }
        self.host.state.root = p.root;
    }
    /// Prepare all seven exact ranges against the same held source root.
    #[doc(hidden)]
    pub fn prepare_remove_conditional_slice(
        &self,
        before: WriteSetCounts,
        after: WriteSetCounts,
    ) -> Result<PreparedTxMutation> {
        if !self.eager_preparation() {
            return self.prepare_remove_conditional_slice_ordinary(before, after);
        }
        self.validate_slot_inventory()?;
        self.validate_aux_inventory()?;
        let ws = &self.host.state.ws;
        for (start, end, len) in [
            (
                before.relational_inserts,
                after.relational_inserts,
                ws.relational_inserts.len(),
            ),
            (
                before.relational_deletes,
                after.relational_deletes,
                ws.relational_deletes.len(),
            ),
            (before.adj_inserts, after.adj_inserts, ws.adj_inserts.len()),
            (before.adj_deletes, after.adj_deletes, ws.adj_deletes.len()),
            (
                before.vector_inserts,
                after.vector_inserts,
                ws.vector_inserts.len(),
            ),
            (
                before.vector_deletes,
                after.vector_deletes,
                ws.vector_deletes.len(),
            ),
            (
                before.vector_moves,
                after.vector_moves,
                ws.vector_moves.len(),
            ),
        ] {
            if start > end || end > len {
                return Err(source_identity_error());
            }
        }
        let admission = ws.ordered_staged_rows.prepare_native_exact_admission(
            self.host.state.row_slots.as_slice(),
            self.host.state.vector_slots.as_slice(),
            crate::ordered_staged::NativeExactRowChange::RemoveRange {
                start: before.relational_inserts,
                end: after.relational_inserts,
            },
        )?;
        let (mut prepared, eager) = admission.prepare(
            self.funder,
            || {
                Ok((
                    self.prepare_remove_conditional_slice_ordinary(before, after)?,
                    None,
                ))
            },
            |p| match &p.kind {
                TxMutationKind::ConditionalSlice(p) => (p.rows.as_slice(), p.vectors.as_slice()),
                _ => unreachable!(),
            },
        )?;
        let TxMutationKind::ConditionalSlice(slice) = &mut prepared.kind else {
            unreachable!()
        };
        slice.eager = Some(eager);
        Ok(prepared)
    }
    fn prepare_remove_conditional_slice_ordinary(
        &self,
        before: WriteSetCounts,
        after: WriteSetCounts,
    ) -> Result<PreparedTxMutation> {
        self.validate_slot_inventory()?;
        self.validate_aux_inventory()?;
        let ws = &self.host.state.ws;
        let ranges = [
            (
                before.relational_inserts,
                after.relational_inserts,
                ws.relational_inserts.len(),
            ),
            (
                before.relational_deletes,
                after.relational_deletes,
                ws.relational_deletes.len(),
            ),
            (before.adj_inserts, after.adj_inserts, ws.adj_inserts.len()),
            (before.adj_deletes, after.adj_deletes, ws.adj_deletes.len()),
            (
                before.vector_inserts,
                after.vector_inserts,
                ws.vector_inserts.len(),
            ),
            (
                before.vector_deletes,
                after.vector_deletes,
                ws.vector_deletes.len(),
            ),
            (
                before.vector_moves,
                after.vector_moves,
                ws.vector_moves.len(),
            ),
        ];
        for (start, end, length) in ranges {
            if start > end || end > length {
                return Err(source_identity_error());
            }
        }
        let mut rows = SourceBuffer::with_capacity(
            after.relational_inserts - before.relational_inserts,
            self.funder,
        )?;
        for position in before.relational_inserts..after.relational_inserts {
            rows.push(position);
        }
        let mut vectors =
            SourceBuffer::with_capacity(after.vector_inserts - before.vector_inserts, self.funder)?;
        for position in before.vector_inserts..after.vector_inserts {
            vectors.push(position);
        }
        let root = self.prepare_root_without_positions(rows.as_slice(), vectors.as_slice())?;
        let state = &self.host.state;
        let slice = PreparedConditionalSlice {
            eager: None,
            root,
            relational_deletes: prepare_empty_slice(
                &ws.relational_deletes,
                &state.relational_deletes_funding,
                before.relational_deletes,
                after.relational_deletes,
                self.funder,
            )?,
            adj_inserts: prepare_empty_slice(
                &ws.adj_inserts,
                &state.adj_inserts_funding,
                before.adj_inserts,
                after.adj_inserts,
                self.funder,
            )?,
            adj_deletes: prepare_empty_slice(
                &ws.adj_deletes,
                &state.adj_deletes_funding,
                before.adj_deletes,
                after.adj_deletes,
                self.funder,
            )?,
            vector_deletes: prepare_empty_slice(
                &ws.vector_deletes,
                &state.vector_deletes_funding,
                before.vector_deletes,
                after.vector_deletes,
                self.funder,
            )?,
            vector_moves: prepare_empty_slice(
                &ws.vector_moves,
                &state.vector_moves_funding,
                before.vector_moves,
                after.vector_moves,
                self.funder,
            )?,
            rows,
            vectors,
        };
        let records = slice
            .rows
            .len()
            .checked_add(slice.vectors.len())
            .and_then(|n| n.checked_add(5))
            .ok_or_else(source_identity_error)?;
        Ok(PreparedTxMutation {
            binding: self.binding()?,
            kind: TxMutationKind::ConditionalSlice(slice),
            undo: self.prepare_undo(records)?,
        })
    }
    fn publish_conditional_slice(
        &mut self,
        mut p: PreparedConditionalSlice,
        undo: Option<PreparedSourceUndo>,
    ) {
        let eager = p.eager.take().map(|p| {
            self.host
                .state
                .ws
                .ordered_staged_rows
                .validate_native_change(p)
                .expect("validated exact native compound source")
        });
        let state = &self.host.state;
        assert_eq!(state.ws.relational_inserts.len(), state.row_slots.len());
        assert_eq!(state.ws.relational_inserts.len(), state.row_debits.len());
        assert_eq!(
            state.ws.relational_inserts.len(),
            state.row_table_charges.len()
        );
        assert_eq!(state.ws.vector_inserts.len(), state.vector_slots.len());
        assert_eq!(state.ws.vector_inserts.len(), state.vector_debits.len());
        validate_exact_positions(p.rows.as_slice(), state.row_slots.len());
        validate_exact_positions(p.vectors.as_slice(), state.vector_slots.len());
        validate_empty_slice(
            &state.ws.relational_deletes,
            &state.relational_deletes_funding,
            &p.relational_deletes,
        );
        validate_empty_slice(
            &state.ws.adj_inserts,
            &state.adj_inserts_funding,
            &p.adj_inserts,
        );
        validate_empty_slice(
            &state.ws.adj_deletes,
            &state.adj_deletes_funding,
            &p.adj_deletes,
        );
        validate_empty_slice(
            &state.ws.vector_deletes,
            &state.vector_deletes_funding,
            &p.vector_deletes,
        );
        validate_empty_slice(
            &state.ws.vector_moves,
            &state.vector_moves_funding,
            &p.vector_moves,
        );
        self.publish_undo(undo);
        for &position in p.rows.as_slice().iter().rev() {
            self.remove_ordinary_row(position);
        }
        for &position in p.vectors.as_slice().iter().rev() {
            self.remove_ordinary_vector(position);
        }
        let state = &mut self.host.state;
        let deletes = publish_splice(
            &mut state.ws.relational_deletes,
            &mut state.relational_deletes_funding,
            p.relational_deletes,
        );
        let adj_inserts = publish_splice(
            &mut state.ws.adj_inserts,
            &mut state.adj_inserts_funding,
            p.adj_inserts,
        );
        let adj_deletes = publish_splice(
            &mut state.ws.adj_deletes,
            &mut state.adj_deletes_funding,
            p.adj_deletes,
        );
        let vector_deletes = publish_splice(
            &mut state.ws.vector_deletes,
            &mut state.vector_deletes_funding,
            p.vector_deletes,
        );
        let vector_moves = publish_splice(
            &mut state.ws.vector_moves,
            &mut state.vector_moves_funding,
            p.vector_moves,
        );
        self.record_undo(SourceUndo::Aux(AuxInverse::RelationalDeletes(deletes)));
        self.record_undo(SourceUndo::Aux(AuxInverse::AdjInserts(adj_inserts)));
        self.record_undo(SourceUndo::Aux(AuxInverse::AdjDeletes(adj_deletes)));
        self.record_undo(SourceUndo::Aux(AuxInverse::VectorDeletes(vector_deletes)));
        self.record_undo(SourceUndo::Aux(AuxInverse::VectorMoves(vector_moves)));
        self.host.state.root = p.root;
        if let Some(eager) = eager {
            self.host
                .state
                .ws
                .ordered_staged_rows
                .publish_validated_native_change(eager);
        }
    }
}

fn validate_exact_positions(positions: &[usize], length: usize) {
    assert!(positions.iter().all(|position| *position < length));
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
}
fn validate_empty_slice<T>(values: &Vec<T>, funding: &AuxFunding, p: &PreparedSplice<T>) {
    assert_eq!(values.len(), funding.invoices.len());
    validate_exact_positions(p.selection.as_slice(), values.len());
    assert!(p.at <= values.len() - p.selection.len());
    assert!(p.insert.is_empty() && p.invoices.len() == 0);
    assert!(p.storage.is_none() && p.invoice_storage.is_none());
    assert!(p.removed.len() == 0 && p.removed.capacity() >= p.selection.len());
}

impl EngineGuard<'_> {
    pub(crate) fn prepare_append_vector_slot(
        &self,
        entry: VectorEntry,
        scalar: Option<(RowId, contextdb_core::ColName)>,
        invoice: crate::staged_source::OriginalVectorInvoice,
    ) -> Result<PreparedSourceMutation> {
        self.prepare_vector_slot_inner(self.eager_preparation(), entry, scalar, invoice)
    }
    fn prepare_vector_slot_inner(
        &self,
        eager_enabled: bool,
        entry: VectorEntry,
        scalar: Option<(RowId, contextdb_core::ColName)>,
        invoice: crate::staged_source::OriginalVectorInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceVector::new(entry, invoice);
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let ordinary_vector = self.funder.copy_vector(&original.entry)?;
        let name = match crate::staged_source::entry_by(&self.host.state.root.vectors, |name| {
            (
                original.entry.index.table.as_str(),
                original.entry.index.column.as_str(),
            )
                .cmp(&(name.table.as_str(), name.column.as_str()))
        }) {
            Some(entry) => entry.key.clone(),
            None => crate::staged_source::VectorName {
                table: crate::staged_source::SourceName::new(
                    &original.entry.index.table,
                    self.funder,
                )?,
                column: crate::staged_source::SourceName::new(
                    &original.entry.index.column,
                    self.funder,
                )?,
            },
        };
        let removed = 0usize;
        let position = self.host.state.ws.vector_inserts.len() - removed;
        let identity = crate::staged_source::VectorIdentity {
            tx: self.host.tx,
            index: name.clone(),
            row: original.entry.row_id,
            created_tx: original.entry.created_tx,
            deleted_tx: original.entry.deleted_tx,
            lsn: original.entry.lsn,
            generation: self.host.generation,
            base: self
                .host
                .state
                .ws
                .base_read
                .clone()
                .ok_or_else(source_identity_error)?,
        };
        let scalar_cell = if let Some((row, column)) = scalar {
            if row != original.entry.row_id || column != original.entry.index.column {
                return Err(source_identity_error());
            }
            self.host.state.root.table(&original.entry.index.table)
                .and_then(|root| crate::staged_source::get_by(&root.by_id, |key| row.cmp(key)))
                .filter(|row| {
                    let custody = row.custody.lock();
                    let body = custody.as_ref().map(|RowCustody::Sealed { row: body, .. }| body);
                    body.is_some_and(|body| matches!(body.values.get(&column),
                        Some(contextdb_core::Value::Vector(values)) if values.as_slice() == original.entry.vector.as_slice()))
                }).cloned()
        } else {
            None
        };
        let cell = crate::staged_source::VectorCell::sealed(identity, original, self.funder)?;
        let reference = match scalar_cell {
            Some(row) => crate::staged_source::CanonicalVectorRef::scalar(
                ordinary_vector.entry.row_id,
                cell.clone(),
                row,
                name.column.clone(),
            ),
            None => crate::staged_source::CanonicalVectorRef::owned(
                ordinary_vector.entry.row_id,
                cell.clone(),
            ),
        };
        let prior = self.host.state.root.vector(
            &ordinary_vector.entry.index.table,
            &ordinary_vector.entry.index.column,
        );
        let vector_root = match prior {
            Some(root) => root.prepare_insert(reference, self.funder)?,
            None => crate::staged_source::VectorRoot::default()
                .prepare_insert(reference, self.funder)?,
        };
        let root = self
            .host
            .state
            .root
            .prepare_vector(name, vector_root, self.funder)?;
        let needed = position
            .checked_add(1)
            .ok_or_else(|| Error::Other("source vector count overflow".into()))?;
        let grow = |capacity: usize| -> Result<usize> {
            Ok(needed.max(
                capacity
                    .checked_mul(2)
                    .ok_or_else(|| Error::Other("source vector capacity overflow".into()))?,
            ))
        };
        let ordinary = if needed > self.host.state.ws.vector_inserts.capacity() {
            let (values, charge) = SourceBuffer::<VectorEntry>::with_capacity(
                grow(self.host.state.ws.vector_inserts.capacity())?,
                self.funder,
            )?
            .into_parts();
            Some(PreparedOrdinaryVectors { values, charge })
        } else {
            None
        };
        let slots = if needed > self.host.state.vector_slots.capacity() {
            Some(SourceBuffer::with_capacity(
                grow(self.host.state.vector_slots.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let debits = if needed > self.host.state.vector_debits.capacity() {
            Some(SourceBuffer::with_capacity(
                grow(self.host.state.vector_debits.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let eager = if eager_enabled {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_owned_vector(&ordinary_vector.entry, self.funder)?
                    .prepare_retain_vector_slot(cell.clone(), self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars: None,
            mutation: PreparedSourceMutationKind::VectorSlot(PreparedVectorMutation {
                undo: self
                    .prepare_undo(removed.checked_add(4).ok_or_else(source_identity_error)?)?,
                original: ordinary_vector,
                cell,
                root,
                expected_root: self.host.state.root.clone(),
                ordinary,
                slots,
                debits,
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
            }),
        })
    }
    fn publish_vector_slot(&mut self, mut p: PreparedVectorMutation) {
        assert!(
            p.tx == self.host.tx
                && p.generation == self.host.generation
                && p.preparation_scope == self.preparation_scope
                && p.expected_root.ptr_eq(&self.host.state.root),
            "captured staged source identity mismatch"
        );
        self.publish_undo(p.undo.take());
        if let Some(mut replacement) = p.ordinary.take() {
            replacement
                .values
                .append(&mut self.host.state.ws.vector_inserts);
            let old = std::mem::replace(&mut self.host.state.ws.vector_inserts, replacement.values);
            let old_credit = std::mem::replace(
                &mut self.host.state.ordinary_vectors_charge,
                replacement.charge,
            );
            self.record_undo(SourceUndo::VectorsStorage(PreparedOrdinaryVectors {
                values: old,
                charge: old_credit,
            }));
        }
        if let Some(mut replacement) = p.slots.take() {
            let mut old =
                std::mem::replace(&mut self.host.state.vector_slots, SourceBuffer::empty());
            old.append_into(&mut replacement);
            self.host.state.vector_slots = replacement;
            self.record_undo(SourceUndo::VectorSlotsStorage(old));
        }
        if let Some(mut replacement) = p.debits.take() {
            self.host.state.vector_debits.append_into(&mut replacement);
            let old = std::mem::replace(&mut self.host.state.vector_debits, replacement);
            self.record_undo(SourceUndo::VectorDebitsStorage(old));
        }
        if self.host.checkpoint_depth > 0 {
            self.host.undo.push(SourceUndo::InsertedVector(
                self.host.state.ws.vector_inserts.len(),
            ));
        }
        self.host.state.ws.vector_inserts.push(p.original.entry);
        self.host.state.vector_debits.push(p.original.invoice);
        self.host.state.vector_slots.push(p.cell);
        self.host.state.root = p.root;
    }
}

impl EngineGuard<'_> {
    pub(crate) fn prepare_replace_row_slot_from_meta(
        &self,
        position: usize,
        table: &str,
        row: VersionedRow,
        meta: &contextdb_core::TableMeta,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        if !self.eager_preparation() {
            return self.prepare_replace_row_slot_from_meta_ordinary(
                position,
                table,
                original.row,
                meta,
                original.invoice,
            );
        }
        self.validate_slot_inventory()?;
        let old = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?
            .clone();
        if old.identity.table.as_str() != table || !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let admission = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_exact_admission(
                self.host.state.row_slots.as_slice(),
                self.host.state.vector_slots.as_slice(),
                crate::ordered_staged::NativeExactRowChange::Replace {
                    ordinal: position,
                    row: original.row.row_id,
                },
            )?;
        let (mut prepared, eager) = admission.prepare(
            self.funder,
            || {
                let prepared = self.prepare_replace_row_slot_from_meta_ordinary(
                    position,
                    table,
                    original.row,
                    meta,
                    original.invoice,
                )?;
                let PreparedSourceMutationKind::ReplaceSlot(slot) = &prepared.mutation else {
                    unreachable!()
                };
                let body = crate::statement_rows::NativeEagerRow::prepare_exact_cell(
                    &slot.cell,
                    self.funder,
                )?;
                let replacement = crate::ordered_staged::NativeExactReplacement {
                    old,
                    cell: slot.cell.clone(),
                    body,
                };
                Ok((prepared, Some(replacement)))
            },
            |_| (&[], &[]),
        )?;
        prepared.eager = Some(eager);
        Ok(prepared)
    }
    fn prepare_replace_row_slot_from_meta_ordinary(
        &self,
        position: usize,
        table: &str,
        row: VersionedRow,
        meta: &contextdb_core::TableMeta,
        invoice: crate::staged_source::OriginalRowInvoice,
    ) -> Result<PreparedSourceMutation> {
        let original = crate::staged_source::AdmittedSourceRow::new(row, invoice);
        self.validate_slot_inventory()?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        struct KeyScratch {
            keys: Vec<(String, contextdb_core::IndexKey)>,
            _credit: crate::staged_source::SourceCharge,
        }
        let mut bytes = meta
            .indexes
            .len()
            .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
            .ok_or_else(source_identity_error)?;
        for index in &meta.indexes {
            bytes = bytes
                .checked_add(index.name.len())
                .and_then(|n| {
                    n.checked_add(
                        index
                            .columns
                            .len()
                            .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                    )
                })
                .ok_or_else(source_identity_error)?;
            for (column, _) in &index.columns {
                let value_bytes = original
                    .row
                    .values
                    .get(column)
                    .map_or(0, contextdb_core::Value::estimated_bytes);
                bytes = bytes
                    .checked_add(
                        value_bytes
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(128))
                            .ok_or_else(source_identity_error)?,
                    )
                    .ok_or_else(source_identity_error)?;
            }
        }
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let mut scratch = KeyScratch {
            keys: Vec::with_capacity(meta.indexes.len()),
            _credit: credit,
        };
        for index in &meta.indexes {
            let mut key = Vec::with_capacity(index.columns.len());
            for (column, direction) in &index.columns {
                let value = original
                    .row
                    .values
                    .get(column)
                    .cloned()
                    .unwrap_or(contextdb_core::Value::Null);
                key.push(match direction {
                    contextdb_core::SortDirection::Asc => {
                        contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                    }
                    contextdb_core::SortDirection::Desc => {
                        contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                    }
                });
            }
            scratch.keys.push((index.name.clone(), key));
        }
        let schema = self.prepare_schema(table, meta)?;
        self.prepare_replace_row_slot_inner(position, table, original, &scratch.keys, schema)
    }
    fn prepare_replace_row_slot_inner(
        &self,
        position: usize,
        table: &str,
        original: crate::staged_source::AdmittedSourceRow,
        keys: &[(String, contextdb_core::IndexKey)],
        schema: crate::staged_source::SchemaHandle,
    ) -> Result<PreparedSourceMutation> {
        let old = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        if old.identity.table.as_str() != table {
            return Err(source_identity_error());
        }
        let ordinary = self.funder.copy_row(&original.row)?;
        let mut bundle = SourceBuffer::with_capacity(keys.len(), self.funder)?;
        for (index, key) in keys {
            let name = schema.index_name(index).ok_or_else(source_identity_error)?;
            let control =
                FundedArc::<crate::staged_source::PostingAtom>::prepare(self.funder.clone())?;
            let key = self.funder.copy_key(key)?;
            bundle.push((
                name,
                FundedArc::from_prepared(
                    crate::staged_source::PostingAtom {
                        position: (key.key, original.row.row_id),
                        _payload: key.charge,
                    },
                    control,
                ),
            ));
        }
        let mut identity = old.identity.clone();
        identity.schema = schema;
        identity.row = original.row.row_id;
        identity.created_tx = original.row.created_tx;
        identity.deleted_tx = original.row.deleted_tx;
        identity.lsn = original.row.lsn;
        identity.created_at = original.row.created_at;
        let cell = crate::staged_source::RowCell::sealed(identity, bundle, original, self.funder)?;
        let prior = self
            .host
            .state
            .root
            .table(table)
            .ok_or_else(source_identity_error)?;
        let mut old_winner = None;
        let mut new_winner = None;
        for (ordinal, have) in self.host.state.row_slots.as_slice().iter().enumerate() {
            let have = if ordinal == position { &cell } else { have };
            if have.identity.table.as_str() != table {
                continue;
            }
            if have.identity.row == old.identity.row {
                old_winner = Some(have.clone());
            }
            if have.identity.row == cell.identity.row {
                new_winner = Some(have.clone());
            }
        }
        let mut next = None;
        for (id, winner) in [
            (old.identity.row, old_winner),
            (cell.identity.row, new_winner),
        ] {
            let current_table = next.as_ref().map_or(
                &**prior,
                |table: &FundedArc<crate::staged_source::TableRoot>| &**table,
            );
            let current = crate::staged_source::get_by(&current_table.by_id, |key| id.cmp(key));
            if current
                .zip(winner.as_ref())
                .is_some_and(|(a, b)| a.ptr_eq(b))
                || current.is_none() && winner.is_none()
            {
                continue;
            }
            next = Some(match winner {
                Some(winner) => current_table.prepare_restore_slot(winner, self.funder)?,
                None => current_table.prepare_remove(id, self.funder)?,
            });
        }
        let root = match next {
            Some(next) => {
                self.host
                    .state
                    .root
                    .prepare_table(old.identity.table.clone(), next, self.funder)?
            }
            None => self.host.state.root.clone(),
        };
        let table_charge =
            crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), table.len())?;
        let table_owned = table.to_owned();
        if table_owned.capacity() != table.len() {
            return Err(source_identity_error());
        }
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager: None,
            sidecars: None,
            mutation: PreparedSourceMutationKind::ReplaceSlot(PreparedSlotReplacement {
                position,
                row: ordinary,
                table: table_owned,
                table_charge,
                cell,
                root,
                undo: self.prepare_undo(2)?,
            }),
        })
    }
}

impl EngineGuard<'_> {
    /// Borrow the genuine selected archive receiver; never copy a winner in
    /// place of an earlier original slot with the same table and row identity.
    #[doc(hidden)]
    pub fn copy_existing_row_at(
        &self,
        position: usize,
    ) -> Result<crate::staged_source::AdmittedSourceRow> {
        let binding = self.binding()?;
        self.validate_binding(&binding);
        self.validate_slot_inventory()?;
        let cell = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        self.host
            .state
            .ws
            .ordered_staged_rows
            .copy_exact_original_row_for(cell, self.funder)
    }

    /// Keep the original row and its invoice paired through exact preparation.
    #[doc(hidden)]
    pub fn prepare_replace_admitted_row_at_from_meta(
        &self,
        position: usize,
        table: &str,
        original: crate::staged_source::AdmittedSourceRow,
        meta: &contextdb_core::TableMeta,
    ) -> Result<PreparedSourceMutation> {
        let cell = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        if cell.identity.table.as_str() != table
            || !cell.identity.matches_row(&original.row)
            || !cell.identity.schema.matches_all_meta(meta)
        {
            return Err(source_identity_error());
        }
        let schema = cell.identity.schema.clone();
        self.prepare_replace_row_at_with_selected_schema(position, table, original, meta, schema)
    }

    // Reuse the exact replacement kernels while keeping the selected schema.
    // The legacy meta entry points retain their existing schema-selection body.
    fn prepare_replace_row_at_with_selected_schema(
        &self,
        position: usize,
        table: &str,
        original: crate::staged_source::AdmittedSourceRow,
        meta: &contextdb_core::TableMeta,
        schema: crate::staged_source::SchemaHandle,
    ) -> Result<PreparedSourceMutation> {
        self.validate_slot_inventory()?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let row_id = original.row.row_id;
        let prepare = || {
            struct KeyScratch {
                keys: Vec<(String, contextdb_core::IndexKey)>,
                _credit: crate::staged_source::SourceCharge,
            }
            let mut bytes = meta
                .indexes
                .len()
                .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
                .ok_or_else(source_identity_error)?;
            for index in &meta.indexes {
                bytes = bytes
                    .checked_add(index.name.len())
                    .and_then(|n| {
                        n.checked_add(
                            index
                                .columns
                                .len()
                                .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                        )
                    })
                    .ok_or_else(source_identity_error)?;
                for (column, _) in &index.columns {
                    let value_bytes = original
                        .row
                        .values
                        .get(column)
                        .map_or(0, contextdb_core::Value::estimated_bytes);
                    bytes = bytes
                        .checked_add(
                            value_bytes
                                .checked_mul(4)
                                .and_then(|n| n.checked_add(128))
                                .ok_or_else(source_identity_error)?,
                        )
                        .ok_or_else(source_identity_error)?;
                }
            }
            let credit =
                crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
            let mut scratch = KeyScratch {
                keys: Vec::with_capacity(meta.indexes.len()),
                _credit: credit,
            };
            for index in &meta.indexes {
                let mut key = Vec::with_capacity(index.columns.len());
                for (column, direction) in &index.columns {
                    let value = original
                        .row
                        .values
                        .get(column)
                        .cloned()
                        .unwrap_or(contextdb_core::Value::Null);
                    key.push(match direction {
                        contextdb_core::SortDirection::Asc => {
                            contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                        }
                        contextdb_core::SortDirection::Desc => {
                            contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                        }
                    });
                }
                scratch.keys.push((index.name.clone(), key));
            }
            if self.eager_preparation() {
                self.prepare_replace_row_slot_inner(
                    position,
                    table,
                    original,
                    &scratch.keys,
                    schema,
                )
            } else {
                self.prepare_replace_row_at_inner(position, table, original, &scratch.keys, schema)
            }
        };
        if !self.eager_preparation() {
            return prepare();
        }
        let old = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?
            .clone();
        let admission = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_exact_admission(
                self.host.state.row_slots.as_slice(),
                self.host.state.vector_slots.as_slice(),
                crate::ordered_staged::NativeExactRowChange::Replace {
                    ordinal: position,
                    row: row_id,
                },
            )?;
        let (mut prepared, eager) = admission.prepare(
            self.funder,
            || {
                let prepared = prepare()?;
                let PreparedSourceMutationKind::ReplaceSlot(slot) = &prepared.mutation else {
                    unreachable!()
                };
                let body = crate::statement_rows::NativeEagerRow::prepare_exact_cell(
                    &slot.cell,
                    self.funder,
                )?;
                let replacement = crate::ordered_staged::NativeExactReplacement {
                    old,
                    cell: slot.cell.clone(),
                    body,
                };
                Ok((prepared, Some(replacement)))
            },
            |_| (&[], &[]),
        )?;
        prepared.eager = Some(eager);
        Ok(prepared)
    }
}

impl EngineGuard<'_> {
    /// Dispatch through the selected original archive, not its canonical winner.
    #[doc(hidden)]
    pub fn copy_existing_vector_at(
        &self,
        position: usize,
    ) -> Result<crate::staged_source::AdmittedEagerSourceVector> {
        let binding = self.binding()?;
        self.validate_binding(&binding);
        self.validate_slot_inventory()?;
        let cell = self
            .host
            .state
            .vector_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        let entry = self
            .host
            .state
            .ws
            .vector_inserts
            .get(position)
            .ok_or_else(source_identity_error)?;
        self.host
            .state
            .ws
            .ordered_staged_rows
            .copy_exact_original_vector_for(cell, entry, self.funder)
    }
}

impl EngineGuard<'_> {
    /// Return only the body/invoice pair admitted by the selected row's O.
    #[doc(hidden)]
    pub fn copy_vector_from_original_row_at(
        &self,
        position: usize,
        column: &str,
        values: &[f32],
        meta: &contextdb_core::TableMeta,
    ) -> Result<crate::staged_source::AdmittedEagerSourceVector> {
        let binding = self.binding()?;
        self.validate_binding(&binding);
        self.validate_slot_inventory()?;
        let cell = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        if !cell.identity.schema.matches_all_meta(meta) {
            return Err(source_identity_error());
        }
        self.host
            .state
            .ws
            .ordered_staged_rows
            .copy_exact_original_vector_input_for_row(cell, column, values, self.funder)
    }

    /// Scalar retargeting cannot allocate a new payload or lose its O invoice.
    #[doc(hidden)]
    pub fn prepare_append_retargeted_original_vector(
        &self,
        mut original: crate::staged_source::AdmittedEagerSourceVector,
        row: RowId,
        tx: TxId,
        lsn: contextdb_core::Lsn,
    ) -> Result<PreparedSourceMutation> {
        original.entry.row_id = row;
        original.entry.created_tx = tx;
        original.entry.deleted_tx = None;
        original.entry.lsn = lsn;
        self.prepare_vector_slot_with_selected_original(original)
    }
}

impl EngineGuard<'_> {
    fn prepare_vector_slot_with_selected_original(
        &self,
        eager_original: crate::staged_source::AdmittedEagerSourceVector,
    ) -> Result<PreparedSourceMutation> {
        eager_original.validate_original_pairing(self.funder)?;
        let eager_enabled = self.eager_preparation();
        let scalar: Option<(RowId, contextdb_core::ColName)> = None;
        let original = eager_original
            .original_copy
            .copy_original_vector_for(&eager_original.entry, self.funder)?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        let ordinary_vector = self.funder.copy_vector(&original.entry)?;
        let name = match crate::staged_source::entry_by(&self.host.state.root.vectors, |name| {
            (
                original.entry.index.table.as_str(),
                original.entry.index.column.as_str(),
            )
                .cmp(&(name.table.as_str(), name.column.as_str()))
        }) {
            Some(entry) => entry.key.clone(),
            None => crate::staged_source::VectorName {
                table: crate::staged_source::SourceName::new(
                    &original.entry.index.table,
                    self.funder,
                )?,
                column: crate::staged_source::SourceName::new(
                    &original.entry.index.column,
                    self.funder,
                )?,
            },
        };
        let removed = 0usize;
        let position = self.host.state.ws.vector_inserts.len() - removed;
        let identity = crate::staged_source::VectorIdentity {
            tx: self.host.tx,
            index: name.clone(),
            row: original.entry.row_id,
            created_tx: original.entry.created_tx,
            deleted_tx: original.entry.deleted_tx,
            lsn: original.entry.lsn,
            generation: self.host.generation,
            base: self
                .host
                .state
                .ws
                .base_read
                .clone()
                .ok_or_else(source_identity_error)?,
        };
        let scalar_cell = if let Some((row, column)) = scalar {
            if row != original.entry.row_id || column != original.entry.index.column {
                return Err(source_identity_error());
            }
            self.host.state.root.table(&original.entry.index.table)
                .and_then(|root| crate::staged_source::get_by(&root.by_id, |key| row.cmp(key)))
                .filter(|row| {
                    let custody = row.custody.lock();
                    let body = custody.as_ref().map(|RowCustody::Sealed { row: body, .. }| body);
                    body.is_some_and(|body| matches!(body.values.get(&column),
                        Some(contextdb_core::Value::Vector(values)) if values.as_slice() == original.entry.vector.as_slice()))
                }).cloned()
        } else {
            None
        };
        let cell = crate::staged_source::VectorCell::sealed(identity, original, self.funder)?;
        let reference = match scalar_cell {
            Some(row) => crate::staged_source::CanonicalVectorRef::scalar(
                ordinary_vector.entry.row_id,
                cell.clone(),
                row,
                name.column.clone(),
            ),
            None => crate::staged_source::CanonicalVectorRef::owned(
                ordinary_vector.entry.row_id,
                cell.clone(),
            ),
        };
        let prior = self.host.state.root.vector(
            &ordinary_vector.entry.index.table,
            &ordinary_vector.entry.index.column,
        );
        let vector_root = match prior {
            Some(root) => root.prepare_insert(reference, self.funder)?,
            None => crate::staged_source::VectorRoot::default()
                .prepare_insert(reference, self.funder)?,
        };
        let root = self
            .host
            .state
            .root
            .prepare_vector(name, vector_root, self.funder)?;
        let needed = position
            .checked_add(1)
            .ok_or_else(|| Error::Other("source vector count overflow".into()))?;
        let grow = |capacity: usize| -> Result<usize> {
            Ok(needed.max(
                capacity
                    .checked_mul(2)
                    .ok_or_else(|| Error::Other("source vector capacity overflow".into()))?,
            ))
        };
        let ordinary = if needed > self.host.state.ws.vector_inserts.capacity() {
            let (values, charge) = SourceBuffer::<VectorEntry>::with_capacity(
                grow(self.host.state.ws.vector_inserts.capacity())?,
                self.funder,
            )?
            .into_parts();
            Some(PreparedOrdinaryVectors { values, charge })
        } else {
            None
        };
        let slots = if needed > self.host.state.vector_slots.capacity() {
            Some(SourceBuffer::with_capacity(
                grow(self.host.state.vector_slots.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let debits = if needed > self.host.state.vector_debits.capacity() {
            Some(SourceBuffer::with_capacity(
                grow(self.host.state.vector_debits.capacity())?,
                self.funder,
            )?)
        } else {
            None
        };
        let eager = if eager_enabled {
            Some(
                self.host
                    .state
                    .ws
                    .ordered_staged_rows
                    .prepare_native_admitted_original_vector(
                        &ordinary_vector.entry,
                        eager_original,
                        self.funder,
                    )?
                    .prepare_retain_vector_slot(cell.clone(), self.funder)?,
            )
        } else {
            None
        };
        Ok(PreparedSourceMutation {
            binding: self.binding()?,
            eager,
            sidecars: None,
            mutation: PreparedSourceMutationKind::VectorSlot(PreparedVectorMutation {
                undo: self
                    .prepare_undo(removed.checked_add(4).ok_or_else(source_identity_error)?)?,
                original: ordinary_vector,
                cell,
                root,
                expected_root: self.host.state.root.clone(),
                ordinary,
                slots,
                debits,
                tx: self.host.tx,
                generation: self.host.generation,
                preparation_scope: self.preparation_scope,
            }),
        })
    }
}

impl EngineGuard<'_> {
    /// The engine borrows a C-admitted replacement; the selected archive O
    /// copies both retained bodies. Expected identity remains the old slot.
    #[doc(hidden)]
    pub fn prepare_changed_row_at_from_meta(
        &self,
        position: usize,
        table: &str,
        expected: &VersionedRow,
        replacement: &VersionedRow,
        meta: &contextdb_core::TableMeta,
    ) -> Result<PreparedSourceMutation> {
        if replacement.created_tx != expected.created_tx
            || replacement.deleted_tx != expected.deleted_tx
            || replacement.lsn != expected.lsn
            || replacement.created_at != expected.created_at
        {
            return Err(source_identity_error());
        }
        let binding = self.binding()?;
        self.validate_binding(&binding);
        self.validate_slot_inventory()?;
        let cell = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        let (current_table, current) = self
            .host
            .state
            .ws
            .relational_inserts
            .get(position)
            .ok_or_else(source_identity_error)?;
        if current_table != table
            || cell.identity.table.as_str() != table
            || !cell.identity.matches_row(expected)
            || !crate::staged_source::changed_original_rows_identical(current, expected)
            || !cell.identity.schema.matches_all_meta(meta)
        {
            return Err(source_identity_error());
        }
        let schema = cell.identity.schema.clone();
        let (original, eager_original) = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .copy_changed_exact_original_rows_for(cell, expected, replacement, self.funder)?;
        self.prepare_changed_row_with_selected_schema(
            position,
            table,
            original,
            eager_original,
            meta,
            schema,
        )
    }
}

impl EngineGuard<'_> {
    fn prepare_changed_row_with_selected_schema(
        &self,
        position: usize,
        table: &str,
        original: crate::staged_source::AdmittedSourceRow,
        eager_original: crate::staged_source::AdmittedEagerSourceRow,
        meta: &contextdb_core::TableMeta,
        schema: crate::staged_source::SchemaHandle,
    ) -> Result<PreparedSourceMutation> {
        self.validate_slot_inventory()?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        eager_original.validate_exact_funder(self.funder)?;
        if !crate::staged_source::changed_original_rows_identical(
            &eager_original.row,
            &original.row,
        ) {
            return Err(source_identity_error());
        }
        let row_id = original.row.row_id;
        let prepare = || {
            struct KeyScratch {
                keys: Vec<(String, contextdb_core::IndexKey)>,
                _credit: crate::staged_source::SourceCharge,
            }
            let mut bytes = meta
                .indexes
                .len()
                .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
                .ok_or_else(source_identity_error)?;
            for index in &meta.indexes {
                bytes = bytes
                    .checked_add(index.name.len())
                    .and_then(|n| {
                        n.checked_add(
                            index
                                .columns
                                .len()
                                .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                        )
                    })
                    .ok_or_else(source_identity_error)?;
                for (column, _) in &index.columns {
                    let value_bytes = original
                        .row
                        .values
                        .get(column)
                        .map_or(0, contextdb_core::Value::estimated_bytes);
                    bytes = bytes
                        .checked_add(
                            value_bytes
                                .checked_mul(4)
                                .and_then(|n| n.checked_add(128))
                                .ok_or_else(source_identity_error)?,
                        )
                        .ok_or_else(source_identity_error)?;
                }
            }
            let credit =
                crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
            let mut scratch = KeyScratch {
                keys: Vec::with_capacity(meta.indexes.len()),
                _credit: credit,
            };
            for index in &meta.indexes {
                let mut key = Vec::with_capacity(index.columns.len());
                for (column, direction) in &index.columns {
                    let value = original
                        .row
                        .values
                        .get(column)
                        .cloned()
                        .unwrap_or(contextdb_core::Value::Null);
                    key.push(match direction {
                        contextdb_core::SortDirection::Asc => {
                            contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                        }
                        contextdb_core::SortDirection::Desc => {
                            contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                        }
                    });
                }
                scratch.keys.push((index.name.clone(), key));
            }
            self.prepare_replace_row_slot_inner(position, table, original, &scratch.keys, schema)
        };
        let old = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?
            .clone();
        let admission = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .prepare_native_exact_admission(
                self.host.state.row_slots.as_slice(),
                self.host.state.vector_slots.as_slice(),
                crate::ordered_staged::NativeExactRowChange::Replace {
                    ordinal: position,
                    row: row_id,
                },
            )?;
        let (mut prepared, eager) = admission.prepare(
            self.funder,
            || {
                let prepared = prepare()?;
                let PreparedSourceMutationKind::ReplaceSlot(slot) = &prepared.mutation else {
                    unreachable!()
                };
                let body = crate::statement_rows::NativeEagerRow::prepare_changed_exact_cell(
                    &slot.cell,
                    eager_original,
                    self.funder,
                )?;
                let replacement = crate::ordered_staged::NativeExactReplacement {
                    old,
                    cell: slot.cell.clone(),
                    body,
                };
                Ok((prepared, Some(replacement)))
            },
            |_| (&[], &[]),
        )?;
        prepared.eager = Some(eager);
        Ok(prepared)
    }
}

impl EngineGuard<'_> {
    #[doc(hidden)]
    pub fn prepare_new_commit_row_from_meta(
        &self,
        table: &str,
        replacement: &VersionedRow,
        meta: &contextdb_core::TableMeta,
    ) -> Result<PreparedSourceMutation> {
        if (replacement.created_tx != self.host.tx
            && self.host.state.ws.finalized_tx != Some(replacement.created_tx))
            || replacement.deleted_tx.is_some()
            || replacement.lsn
                != self
                    .host
                    .state
                    .ws
                    .commit_lsn
                    .unwrap_or(contextdb_core::Lsn(0))
            || replacement.created_at.is_none()
        {
            return Err(source_identity_error());
        }
        let binding = self.binding()?;
        self.validate_binding(&binding);
        self.validate_slot_inventory()?;
        let eager = self.funder.copy_eager_row(replacement)?;
        let original = eager
            .original_copy
            .copy_original_row_for(replacement, self.funder)?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        struct KeyScratch {
            keys: Vec<(String, contextdb_core::IndexKey)>,
            _credit: crate::staged_source::SourceCharge,
        }
        let mut bytes = meta
            .indexes
            .len()
            .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
            .ok_or_else(source_identity_error)?;
        for index in &meta.indexes {
            bytes = bytes
                .checked_add(index.name.len())
                .and_then(|n| {
                    n.checked_add(
                        index
                            .columns
                            .len()
                            .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                    )
                })
                .ok_or_else(source_identity_error)?;
            for (column, _) in &index.columns {
                let value_bytes = original
                    .row
                    .values
                    .get(column)
                    .map_or(0, contextdb_core::Value::estimated_bytes);
                bytes = bytes
                    .checked_add(
                        value_bytes
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(128))
                            .ok_or_else(source_identity_error)?,
                    )
                    .ok_or_else(source_identity_error)?;
            }
        }
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let mut scratch = KeyScratch {
            keys: Vec::with_capacity(meta.indexes.len()),
            _credit: credit,
        };
        for index in &meta.indexes {
            let mut key = Vec::with_capacity(index.columns.len());
            for (column, direction) in &index.columns {
                let value = original
                    .row
                    .values
                    .get(column)
                    .cloned()
                    .unwrap_or(contextdb_core::Value::Null);
                key.push(match direction {
                    contextdb_core::SortDirection::Asc => {
                        contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                    }
                    contextdb_core::SortDirection::Desc => {
                        contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                    }
                });
            }
            scratch.keys.push((index.name.clone(), key));
        }
        let schema = self.prepare_schema(table, meta)?;
        let eager_schema = schema.clone();
        let mut prepared = self.prepare_row_inner(
            false,
            table,
            original.row,
            &scratch.keys,
            schema,
            original.invoice,
        )?;
        let PreparedSourceMutationKind::Row(body) = &prepared.mutation else {
            unreachable!()
        };
        let position = crate::ordered_staged::StagedPosition {
            row_id: body.row.row.row_id,
            created_tx: body.row.row.created_tx,
            position: self.host.state.ws.relational_inserts.len(),
        };
        prepared.eager = Some(
            self.host
                .state
                .ws
                .ordered_staged_rows
                .prepare_native_admitted_original_row(
                    table,
                    position,
                    &body.row.row,
                    eager,
                    &scratch.keys,
                    eager_schema,
                    self.funder,
                )?
                .prepare_retain_row(body.cell.clone(), self.funder)?,
        );
        Ok(prepared)
    }
}

impl EngineGuard<'_> {
    #[doc(hidden)]
    pub fn prepare_append_changed_row_at_from_meta(
        &self,
        position: usize,
        table: &str,
        expected: &VersionedRow,
        replacement: &VersionedRow,
        meta: &contextdb_core::TableMeta,
    ) -> Result<PreparedSourceMutation> {
        if (replacement.created_tx != self.host.tx
            && self.host.state.ws.finalized_tx != Some(replacement.created_tx))
            || replacement.deleted_tx.is_some()
            || replacement.lsn
                != self
                    .host
                    .state
                    .ws
                    .commit_lsn
                    .unwrap_or(contextdb_core::Lsn(0))
            || replacement.created_at.is_none()
            || replacement.row_id != expected.row_id
        {
            return Err(source_identity_error());
        }
        let binding = self.binding()?;
        self.validate_binding(&binding);
        self.validate_slot_inventory()?;
        let cell = self
            .host
            .state
            .row_slots
            .as_slice()
            .get(position)
            .ok_or_else(source_identity_error)?;
        let (current_table, current) = self
            .host
            .state
            .ws
            .relational_inserts
            .get(position)
            .ok_or_else(source_identity_error)?;
        if current_table != table
            || !crate::staged_source::changed_original_rows_identical(current, expected)
            || !cell.identity.matches_row(expected)
            || !cell.identity.schema.matches_all_meta(meta)
        {
            return Err(source_identity_error());
        }
        let selected_schema = cell.identity.schema.clone();
        let (original, eager) = self
            .host
            .state
            .ws
            .ordered_staged_rows
            .copy_changed_exact_original_rows_for(cell, expected, replacement, self.funder)?;
        if !original.invoice.matches(self.funder) {
            return Err(source_identity_error());
        }
        struct KeyScratch {
            keys: Vec<(String, contextdb_core::IndexKey)>,
            _credit: crate::staged_source::SourceCharge,
        }
        let mut bytes = meta
            .indexes
            .len()
            .checked_mul(std::mem::size_of::<(String, contextdb_core::IndexKey)>())
            .ok_or_else(source_identity_error)?;
        for index in &meta.indexes {
            bytes = bytes
                .checked_add(index.name.len())
                .and_then(|n| {
                    n.checked_add(
                        index
                            .columns
                            .len()
                            .checked_mul(std::mem::size_of::<contextdb_core::DirectedValue>())?,
                    )
                })
                .ok_or_else(source_identity_error)?;
            for (column, _) in &index.columns {
                let value_bytes = original
                    .row
                    .values
                    .get(column)
                    .map_or(0, contextdb_core::Value::estimated_bytes);
                bytes = bytes
                    .checked_add(
                        value_bytes
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(128))
                            .ok_or_else(source_identity_error)?,
                    )
                    .ok_or_else(source_identity_error)?;
            }
        }
        let credit = crate::staged_source::SourceCharge::admit_funded(self.funder.clone(), bytes)?;
        let mut scratch = KeyScratch {
            keys: Vec::with_capacity(meta.indexes.len()),
            _credit: credit,
        };
        for index in &meta.indexes {
            let mut key = Vec::with_capacity(index.columns.len());
            for (column, direction) in &index.columns {
                let value = original
                    .row
                    .values
                    .get(column)
                    .cloned()
                    .unwrap_or(contextdb_core::Value::Null);
                key.push(match direction {
                    contextdb_core::SortDirection::Asc => {
                        contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(value))
                    }
                    contextdb_core::SortDirection::Desc => {
                        contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(value))
                    }
                });
            }
            scratch.keys.push((index.name.clone(), key));
        }
        let schema = selected_schema;
        let eager_schema = schema.clone();
        let mut prepared = self.prepare_row_inner(
            false,
            table,
            original.row,
            &scratch.keys,
            schema,
            original.invoice,
        )?;
        let PreparedSourceMutationKind::Row(body) = &prepared.mutation else {
            unreachable!()
        };
        let position = crate::ordered_staged::StagedPosition {
            row_id: body.row.row.row_id,
            created_tx: body.row.row.created_tx,
            position: self.host.state.ws.relational_inserts.len(),
        };
        prepared.eager = Some(
            self.host
                .state
                .ws
                .ordered_staged_rows
                .prepare_native_admitted_original_row(
                    table,
                    position,
                    &body.row.row,
                    eager,
                    &scratch.keys,
                    eager_schema,
                    self.funder,
                )?
                .prepare_retain_row(body.cell.clone(), self.funder)?,
        );
        Ok(prepared)
    }
}
