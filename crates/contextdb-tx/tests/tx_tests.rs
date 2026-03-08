use contextdb_core::{Error, RowId, TxId};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct MockStore {
    applied: Mutex<Vec<WriteSet>>,
    next_row_id: AtomicU64,
}

impl WriteSetApplicator for MockStore {
    fn apply(&self, ws: WriteSet) -> contextdb_core::Result<()> {
        self.applied.lock().push(ws);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.next_row_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[test]
fn begin_is_monotonic() {
    let store = MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    assert_eq!(txm.begin(), 1);
    assert_eq!(txm.begin(), 2);
    assert_eq!(txm.begin(), 3);
}

#[test]
fn snapshot_advances_on_commit() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    assert_eq!(txm.snapshot(), 0);
    let tx = txm.begin();
    txm.commit(tx).expect("commit should succeed");
    assert_eq!(txm.snapshot(), tx);
}

#[test]
fn rollback_does_not_advance_snapshot() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx = txm.begin();
    txm.rollback(tx).expect("rollback should succeed");
    assert_eq!(txm.snapshot(), 0);
}

#[test]
fn commit_updates_watermark() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let t1 = txm.begin();
    let t2 = txm.begin();
    txm.commit(t1).expect("first commit should succeed");
    assert_eq!(txm.snapshot(), t1);
    txm.commit(t2).expect("second commit should succeed");
    assert_eq!(txm.snapshot(), t2);
}

#[test]
fn double_commit_returns_tx_not_found() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx: TxId = txm.begin();
    txm.commit(tx).expect("first commit should succeed");
    let err = txm.commit(tx).expect_err("second commit should fail");
    assert!(matches!(err, Error::TxNotFound(t) if t == tx));
}

#[test]
fn double_rollback_returns_tx_not_found() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx: TxId = txm.begin();
    txm.rollback(tx).expect("first rollback should succeed");
    let err = txm
        .rollback(tx)
        .expect_err("second rollback should fail");
    assert!(matches!(err, Error::TxNotFound(t) if t == tx));
}
