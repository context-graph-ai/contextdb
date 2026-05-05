use crate::write_set::{WriteSet, WriteSetApplicator};
use contextdb_core::{AtomicLsn, AtomicTxId, Error, Lsn, Result, RowId, SnapshotId, TxId};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;

pub struct TxManager<S: WriteSetApplicator> {
    next_tx: AtomicTxId,
    committed_watermark: AtomicTxId,
    next_lsn: AtomicLsn,
    committed_lsn: AtomicLsn,
    active_txs: Mutex<HashMap<TxId, WriteSet>>,
    commit_index: Mutex<BTreeMap<Lsn, TxId>>,
    commit_mutex: Mutex<()>,
    store: S,
}

#[derive(Debug)]
pub struct CommitFailure {
    pub error: Error,
    pub write_set: Option<Box<WriteSet>>,
}

impl<S: WriteSetApplicator> TxManager<S> {
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
            active_txs: Mutex::new(HashMap::new()),
            commit_index: Mutex::new(commit_index),
            commit_mutex: Mutex::new(()),
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

    pub fn with_write_set<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        let mut active = self.active_txs.lock();
        let ws = active.get_mut(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(f(ws))
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
        let _lock = self.commit_mutex.lock();
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

        let applied_ws = ws.clone();
        if let Err(error) = self.store.apply(ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(applied_ws)),
            });
        }
        self.commit_index.lock().insert(lsn, visibility_tx);
        self.committed_watermark
            .fetch_max(visibility_tx, Ordering::SeqCst);
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
        Ok((lsn, applied_ws))
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
        let _lock = self.commit_mutex.lock();
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

        let applied_ws = ws.clone();
        if let Err(error) = self.store.apply(ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(applied_ws)),
            });
        }
        self.commit_index.lock().insert(lsn, visibility_tx);
        self.committed_watermark
            .fetch_max(visibility_tx, Ordering::SeqCst);
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
        after_apply(lsn, &applied_ws);
        Ok((lsn, applied_ws))
    }

    pub fn commit_with_lsn_active_prepare_and_applied_mut<F, G>(
        &self,
        tx: TxId,
        active_prepare: F,
        prepare: G,
        after_apply: impl FnOnce(Lsn, &WriteSet),
    ) -> std::result::Result<(Lsn, WriteSet), CommitFailure>
    where
        F: FnOnce(Lsn) -> Result<()>,
        G: FnOnce(&mut WriteSet) -> Result<()>,
    {
        let _lock = self.commit_mutex.lock();
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

        if let Err(error) = active_prepare(lsn) {
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

        let applied_ws = ws.clone();
        if let Err(error) = self.store.apply(ws) {
            self.next_lsn.store(lsn, Ordering::SeqCst);
            return Err(CommitFailure {
                error,
                write_set: Some(Box::new(applied_ws)),
            });
        }
        self.commit_index.lock().insert(lsn, visibility_tx);
        self.committed_watermark
            .fetch_max(visibility_tx, Ordering::SeqCst);
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
        after_apply(lsn, &applied_ws);
        Ok((lsn, applied_ws))
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        self.rollback_write_set(tx).map(|_| ())
    }

    pub fn rollback_write_set(&self, tx: TxId) -> Result<WriteSet> {
        let _lock = self.commit_mutex.lock();
        let mut active = self.active_txs.lock();
        active.remove(&tx).ok_or(Error::TxNotFound(tx))
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
        let _lock = self.commit_mutex.lock();
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        match f(lsn) {
            Ok(result) => {
                self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
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
        let _lock = self.commit_mutex.lock();
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        match f(lsn) {
            Ok(Some(result)) => {
                self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
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
        let _lock = self.commit_mutex.lock();
        f()
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

impl<S: WriteSetApplicator> contextdb_core::TransactionManager for TxManager<S> {
    fn begin(&self) -> TxId {
        TxManager::begin(self)
    }

    fn commit(&self, tx: TxId) -> Result<()> {
        TxManager::commit(self, tx)
    }

    fn rollback(&self, tx: TxId) -> Result<()> {
        TxManager::rollback(self, tx)
    }

    fn snapshot(&self) -> SnapshotId {
        TxManager::snapshot(self)
    }
}
