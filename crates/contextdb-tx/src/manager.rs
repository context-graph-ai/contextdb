use crate::write_set::{WriteSet, WriteSetApplicator};
use contextdb_core::{Error, Result, RowId, SnapshotId, TxId};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct TxManager<S: WriteSetApplicator> {
    next_tx: AtomicU64,
    committed_watermark: AtomicU64,
    next_lsn: AtomicU64,
    active_txs: Mutex<HashMap<TxId, WriteSet>>,
    commit_mutex: Mutex<()>,
    store: S,
}

impl<S: WriteSetApplicator> TxManager<S> {
    pub fn new(store: S) -> Self {
        Self {
            next_tx: AtomicU64::new(1),
            committed_watermark: AtomicU64::new(0),
            next_lsn: AtomicU64::new(1),
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
        self.committed_watermark.load(Ordering::SeqCst)
    }

    pub fn with_write_set<F, R>(&self, tx: TxId, f: F) -> Result<R>
    where
        F: FnOnce(&mut WriteSet) -> R,
    {
        let mut active = self.active_txs.lock();
        let ws = active.get_mut(&tx).ok_or(Error::TxNotFound(tx))?;
        Ok(f(ws))
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        let mut ws = {
            let mut active = self.active_txs.lock();
            active.remove(&tx).ok_or(Error::TxNotFound(tx))?
        };

        let _lock = self.commit_mutex.lock();
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        ws.stamp_lsn(lsn);
        self.store.apply(ws)?;
        self.committed_watermark.fetch_max(tx, Ordering::SeqCst);
        Ok(())
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

    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst).saturating_sub(1)
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.fetch_add(1, Ordering::SeqCst)
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
