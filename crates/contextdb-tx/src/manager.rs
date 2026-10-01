use crate::write_set::{WriteSet, WriteSetApplicator};
use contextdb_core::read_contract::OwnerReadCancellation;
use contextdb_core::{AtomicLsn, AtomicTxId, Error, Lsn, Result, RowId, SnapshotId, TxId};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[cfg(feature = "test-seams")]
mod resident_history_test_seams;

#[cfg(test)]
thread_local! {
    static COMMIT_WAIT_ENTERED_FOR_TEST: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Default)]
struct CommitRelease {
    epoch: Mutex<u64>,
    changed: Condvar,
}

struct CommitLockGuard<'a> {
    lock: Option<MutexGuard<'a, ()>>,
    release: &'a CommitRelease,
}

impl Drop for CommitLockGuard<'_> {
    fn drop(&mut self) {
        // Wake only after the mutex is actually available. The epoch lock
        // closes the gap between a waiter's failed try_lock and its park.
        drop(self.lock.take());
        let mut epoch = self.release.epoch.lock();
        *epoch = epoch.wrapping_add(1);
        self.release.changed.notify_all();
    }
}

pub struct TransactionManager<S: WriteSetApplicator> {
    next_tx: AtomicTxId,
    committed_watermark: AtomicTxId,
    next_lsn: AtomicLsn,
    committed_lsn: AtomicLsn,
    /// One independently readable final-publication point. The two public
    /// atomics above serve existing callers, but cannot be sampled as a pair
    /// while a commit is between their stores.
    published_read: Mutex<(SnapshotId, Lsn)>,
    active_txs: Mutex<HashMap<TxId, WriteSet>>,
    commit_index: Mutex<BTreeMap<Lsn, TxId>>,
    commit_mutex: Mutex<()>,
    commit_release: Arc<CommitRelease>,
    store: S,
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
        Self {
            next_tx: AtomicTxId::new(next_tx),
            committed_watermark: AtomicTxId::new(committed_watermark),
            next_lsn: AtomicLsn::new(next_lsn),
            committed_lsn: AtomicLsn::new(Lsn(next_lsn.0.saturating_sub(1))),
            published_read: Mutex::new((
                SnapshotId::from_tx(committed_watermark),
                Lsn(next_lsn.0.saturating_sub(1)),
            )),
            active_txs: Mutex::new(HashMap::new()),
            commit_index: Mutex::new(commit_index),
            commit_mutex: Mutex::new(()),
            commit_release: Arc::new(CommitRelease::default()),
            store,
        }
    }

    pub fn begin(&self) -> TxId {
        let tx_id = self.next_tx.fetch_add(1, Ordering::SeqCst);
        let mut active = self.active_txs.lock();
        active.insert(tx_id, WriteSet::new());
        tx_id
    }

    pub fn snapshot(&self) -> SnapshotId {
        SnapshotId::from_tx(self.committed_watermark.load(Ordering::SeqCst))
    }

    /// The last complete commit's snapshot and LSN, copied together without
    /// taking the commit mutex. A bounded reader may register this root while
    /// the next writer is still applying, then inspect only that snapshot.
    pub fn published_read_snapshot(&self) -> (SnapshotId, Lsn) {
        *self.published_read.lock()
    }

    pub fn with_write_set<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        let mut active = self.active_txs.lock();
        let ws = active.get_mut(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(f(ws))
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

    pub fn commit(&self, tx: TxId) -> Result<()> {
        self.commit_with_lsn(tx).map(|_| ())
    }

    pub fn commit_with_lsn(&self, tx: TxId) -> Result<Lsn> {
        self.commit_with_lsn_prepared(tx, |_| Ok(()))
            .map(|(lsn, _)| lsn)
            .map_err(|failure| failure.error)
    }

    pub fn commit_with_reserved_lsn_callback<F, G>(
        &self,
        tx: TxId,
        before_commit: F,
        prepare: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn) -> Result<()>,
        G: FnOnce(&WriteSet) -> Result<()>,
    {
        self.commit_with_reserved_lsn_callback_mut(tx, before_commit, |ws| prepare(ws))
    }

    pub fn commit_with_reserved_lsn_callback_mut<F, G>(
        &self,
        tx: TxId,
        before_commit: F,
        prepare: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
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

        if let Err(error) = self.store.apply(&ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }
        self.commit_index.lock().insert(lsn, visibility_tx);
        self.committed_watermark
            .fetch_max(visibility_tx, Ordering::SeqCst);
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
        self.publish_read_snapshot(lsn);
        Ok((lsn, ws))
    }

    pub fn commit_with_lsn_prepared<F>(
        &self,
        tx: TxId,
        prepare: F,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(&WriteSet) -> Result<()>,
    {
        self.commit_with_lsn_prepared_and_applied(tx, prepare, |_, _| {})
    }

    pub fn commit_with_lsn_prepared_and_applied<F, G>(
        &self,
        tx: TxId,
        prepare: F,
        after_apply: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(&WriteSet) -> Result<()>,
        G: FnOnce(Lsn, &WriteSet),
    {
        self.commit_with_lsn_prepared_and_applied_mut(tx, |ws| prepare(ws), after_apply)
    }

    pub fn commit_with_lsn_prepared_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        prepare: F,
        after_apply: G,
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(&mut WriteSet) -> Result<()>,
        G: FnOnce(Lsn, &WriteSet),
    {
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
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        ws.stamp_lsn(lsn);
        if let Err(error) = prepare(&mut ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }

        if let Err(error) = self.store.apply(&ws) {
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
        self.commit_index.lock().insert(lsn, visibility_tx);
        self.committed_watermark
            .fetch_max(visibility_tx, Ordering::SeqCst);
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
        self.publish_read_snapshot(lsn);
        Ok((lsn, ws))
    }

    pub fn commit_with_lsn_active_prepare_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        active_prepare: F,
        prepare: G,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn, TxId) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        let _lock = self.lock_commit_boundary();
        let visibility_tx = {
            let mut active = self.active_txs.lock();
            let ws = active.get_mut(&tx).ok_or(CommitFailure {
                error: Error::TxNotFound(tx),
                write_set: None,
            })?;
            ws.canonicalize_final_state();
            if ws.is_empty() {
                let ws = active.remove(&tx).expect("checked active tx exists");
                return Ok((self.current_lsn(), ws));
            }
            let visibility_tx = self.visibility_tx_for_commit(tx, ws.visibility_floor);
            ws.reassign_tx(tx, visibility_tx);
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
        ws.stamp_lsn(lsn);

        if let Err(error) = prepare(&mut ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(ws)),
            });
        }

        if let Err(error) = self.store.apply(&ws) {
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
        self.commit_index.lock().insert(lsn, visibility_tx);
        self.committed_watermark
            .fetch_max(visibility_tx, Ordering::SeqCst);
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
        self.publish_read_snapshot(lsn);
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
        let mut active = self.active_txs.lock();
        f(&mut active)
    }

    pub fn rollback_write_set(&self, tx: TxId) -> Result<WriteSet> {
        let _lock = self.lock_commit_boundary();
        let mut active = self.active_txs.lock();
        active.remove(&tx).ok_or(Error::TxNotFound(tx))
    }

    pub fn rollback_empty_without_commit_lock(&self, tx: TxId) -> Result<()> {
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
        self.store.new_row_id()
    }

    pub fn current_lsn(&self) -> Lsn {
        self.committed_lsn.load(Ordering::SeqCst)
    }

    fn publish_read_snapshot(&self, lsn: Lsn) {
        *self.published_read.lock() = (self.snapshot(), lsn);
    }

    /// Commit-index entries at or below `up_to`. Read this inside the same
    /// commit-mutex window (see [`TransactionManager::with_commit_lock`]) that captures
    /// the committed watermark and committed LSN, so the returned prefix is
    /// consistent with both. Snapshot-capture seam for checkpoint export.
    pub fn commit_index_prefix(&self, up_to: Lsn) -> BTreeMap<Lsn, TxId> {
        self.commit_index
            .lock()
            .range(..=up_to)
            .map(|(lsn, tx)| (*lsn, *tx))
            .collect()
    }

    /// Replace the commit index with `retained`, returning how many entries
    /// were removed. Retention pruning calls this to stop the index growing
    /// with every commit ever made; the caller owns the floor policy, because
    /// only it knows which LSNs a consumer can still name.
    pub fn replace_commit_index(&self, retained: BTreeMap<Lsn, TxId>) -> u64 {
        let mut commit_index = self.commit_index.lock();
        let before = commit_index.len();
        *commit_index = retained;
        (before.saturating_sub(commit_index.len())) as u64
    }

    /// Remove exactly the named entries, candidate-only — the scoped
    /// counterpart of `replace_commit_index`: a version-cleanup pass that
    /// only touched a handful of LSNs removes only those candidates (each
    /// already proven safe to drop by the caller's own floor computation,
    /// e.g. `retained_commit_index_after_prune`), never re-derives the whole
    /// index. Returns how many of the named entries were actually present.
    pub fn remove_commit_index_entries(&self, lsns: &[Lsn]) -> u64 {
        let mut commit_index = self.commit_index.lock();
        let mut removed = 0u64;
        for lsn in lsns {
            if commit_index.remove(lsn).is_some() {
                removed = removed.saturating_add(1);
            }
        }
        removed
    }

    /// A copy of the commit index, for a caller computing a retention floor.
    /// How many committed LSNs the commit index holds.
    pub fn commit_index_len(&self) -> usize {
        self.commit_index.lock().len()
    }

    /// The commit index in LSN order, copied once into exactly the space it
    /// needs.
    pub fn commit_index_entries(&self) -> Vec<(Lsn, TxId)> {
        let index = self.commit_index.lock();
        let mut entries = Vec::with_capacity(index.len());
        entries.extend(index.iter().map(|(lsn, tx)| (*lsn, *tx)));
        entries
    }

    pub fn commit_index_snapshot(&self) -> BTreeMap<Lsn, TxId> {
        self.commit_index
            .lock()
            .iter()
            .map(|(lsn, tx)| (*lsn, *tx))
            .collect()
    }

    /// The single commit-index entry immediately below `lsn` (exclusive), if
    /// any — a targeted range query that never clones the whole index,
    /// unlike [`Self::commit_index_snapshot`]. Used by a scoped
    /// cleanup/retention pass to find its anchor entry (the fallback a
    /// query below the lowest still-nameable LSN resolves against) without
    /// paying for the whole index's size, which — unlike the change log a
    /// scoped pass actually cleans — is never itself scoped to the
    /// cleaned table.
    pub fn commit_index_floor_below(&self, lsn: Lsn) -> Option<(Lsn, TxId)> {
        self.commit_index
            .lock()
            .range(..lsn)
            .next_back()
            .map(|(lsn, tx)| (*lsn, *tx))
    }

    /// The single most recent commit-index entry, if any — the same
    /// targeted-lookup rationale as [`Self::commit_index_floor_below`],
    /// used for the degenerate "nothing in the change log survives" anchor
    /// fallback.
    pub fn commit_index_last(&self) -> Option<(Lsn, TxId)> {
        self.commit_index
            .lock()
            .iter()
            .next_back()
            .map(|(lsn, tx)| (*lsn, *tx))
    }

    /// Whether `lsn` is currently present in the commit index — a targeted
    /// point lookup, never a clone of the whole index.
    pub fn commit_index_contains(&self, lsn: Lsn) -> bool {
        self.commit_index.lock().contains_key(&lsn)
    }

    /// Entry count and the lowest LSN still retained. `Lsn(0)` when the index
    /// is empty — an index that has never been written to has no lowest entry
    /// to name.
    pub fn commit_index_census(&self) -> (usize, Lsn) {
        let commit_index = self.commit_index.lock();
        let lowest = commit_index.keys().next().copied().unwrap_or(Lsn(0));
        (commit_index.len(), lowest)
    }

    pub fn snapshot_at_lsn(&self, lsn: Lsn) -> SnapshotId {
        let commit_index = self.commit_index.lock();
        let tx = commit_index
            .range(..=lsn)
            .next_back()
            .map(|(_, tx)| *tx)
            .unwrap_or(TxId(0));
        SnapshotId::from_tx(tx)
    }

    pub fn allocate_ddl_lsn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        let _lock = self.lock_commit_boundary();
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        match f(lsn) {
            Ok(result) => {
                self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
                self.publish_read_snapshot(lsn);
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
        let _lock = self.lock_commit_boundary();
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        match f(lsn) {
            Ok(Some(result)) => {
                self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
                self.publish_read_snapshot(lsn);
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

    fn lock_commit_boundary(&self) -> CommitLockGuard<'_> {
        CommitLockGuard {
            lock: Some(self.commit_mutex.lock()),
            release: &self.commit_release,
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
        let _lock = CommitLockGuard {
            lock: Some(self.commit_mutex.try_lock_for(duration)?),
            release: &self.commit_release,
        };
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
        let release = Arc::clone(&self.commit_release);
        let wake_on_cancel = Arc::clone(&release);
        let _cancel_listener = cancelled.tell_on_cancel(move || {
            let mut epoch = wake_on_cancel.epoch.lock();
            *epoch = epoch.wrapping_add(1);
            wake_on_cancel.changed.notify_all();
        });
        let started = Instant::now();
        let mut epoch = release.epoch.lock();
        loop {
            if cancelled.is_cancelled() || deadline_expired() {
                return None;
            }
            if let Some(lock) = self.commit_mutex.try_lock() {
                drop(epoch);
                let _guard = CommitLockGuard {
                    lock: Some(lock),
                    release: &release,
                };
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

    fn commit(&self, tx: TxId) -> Result<()> {
        TransactionManager::commit(self, tx)
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
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct NoopStore;

    impl WriteSetApplicator for NoopStore {
        fn apply(&self, _: &WriteSet) -> Result<()> {
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
