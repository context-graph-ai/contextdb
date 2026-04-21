use crate::write_set::{WriteSet, WriteSetApplicator};
use contextdb_core::{AtomicLsn, AtomicTxId, Error, Lsn, Result, RowId, SnapshotId, TxId};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

pub struct TxManager<S: WriteSetApplicator> {
    next_tx: AtomicTxId,
    committed_watermark: AtomicTxId,
    next_lsn: AtomicLsn,
    active_txs: Mutex<HashMap<TxId, WriteSet>>,
    commit_mutex: Mutex<()>,
    store: S,
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
        Self {
            next_tx: AtomicTxId::new(next_tx),
            committed_watermark: AtomicTxId::new(committed_watermark),
            next_lsn: AtomicLsn::new(next_lsn),
            active_txs: Mutex::new(HashMap::new()),
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

    pub fn cloned_write_set(&self, tx: TxId) -> Result<WriteSet> {
        let active = self.active_txs.lock();
        active.get(&tx).cloned().ok_or(Error::TxNotFound(tx))
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        self.commit_with_lsn(tx).map(|_| ())
    }

    pub fn commit_with_lsn(&self, tx: TxId) -> Result<Lsn> {
        let _lock = self.commit_mutex.lock();
        let mut ws = self.cloned_write_set(tx)?;
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        ws.stamp_lsn(lsn);
        self.store.apply(ws)?;
        let mut active = self.active_txs.lock();
        active.remove(&tx).ok_or(Error::TxNotFound(tx))?;
        self.committed_watermark.fetch_max(tx, Ordering::SeqCst);
        Ok(lsn)
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        let mut active = self.active_txs.lock();
        active.remove(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(())
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn new_row_id(&self) -> RowId {
        self.store.new_row_id()
    }

    pub fn current_lsn(&self) -> Lsn {
        let raw = self.next_lsn.load(Ordering::SeqCst).0.saturating_sub(1);
        Lsn(raw)
    }

    pub fn allocate_ddl_lsn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Lsn) -> R,
    {
        let _lock = self.commit_mutex.lock();
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        f(lsn)
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
    pub fn advance_for_sync(&self, table: &str, incoming: TxId) -> Result<()> {
        if incoming.0 == u64::MAX {
            return Err(Error::TxIdOverflow {
                table: table.to_string(),
                incoming: u64::MAX,
            });
        }
        self.next_tx
            .fetch_max(TxId(incoming.0 + 1), Ordering::SeqCst);
        self.committed_watermark
            .fetch_max(incoming, Ordering::SeqCst);
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
