use contextdb_core::{Error, Lsn, RowId, SnapshotId, TxId, VersionedRow};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use parking_lot::Mutex;
use std::collections::HashMap;
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
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }
}

#[test]
fn begin_is_monotonic() {
    let store = MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    assert_eq!(txm.begin(), TxId(1));
    assert_eq!(txm.begin(), TxId(2));
    assert_eq!(txm.begin(), TxId(3));
}

#[test]
fn snapshot_advances_on_commit() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    assert_eq!(txm.snapshot(), SnapshotId(0));
    let tx = txm.begin();
    txm.with_write_set(tx, |ws| {
        ws.relational_inserts
            .push(("items".to_string(), versioned_row(RowId(1), tx)));
    })
    .unwrap();
    txm.commit(tx).expect("commit should succeed");
    assert_eq!(txm.snapshot(), SnapshotId::from_tx(tx));
}

#[test]
fn rollback_does_not_advance_snapshot() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx = txm.begin();
    txm.rollback(tx).expect("rollback should succeed");
    assert_eq!(txm.snapshot(), SnapshotId(0));
}

#[test]
fn commit_updates_watermark() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let t1 = txm.begin();
    let t2 = txm.begin();
    txm.with_write_set(t1, |ws| {
        ws.relational_inserts
            .push(("items".to_string(), versioned_row(RowId(1), t1)));
    })
    .unwrap();
    txm.commit(t1).expect("first commit should succeed");
    assert_eq!(txm.snapshot(), SnapshotId::from_tx(t1));
    txm.with_write_set(t2, |ws| {
        ws.relational_inserts
            .push(("items".to_string(), versioned_row(RowId(2), t2)));
    })
    .unwrap();
    txm.commit(t2).expect("second commit should succeed");
    assert_eq!(txm.snapshot(), SnapshotId::from_tx(t2));
}

#[test]
fn empty_commit_does_not_advance_snapshot_or_lsn() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx = txm.begin();
    let before_snapshot = txm.snapshot();
    let before_lsn = txm.current_lsn();

    txm.commit(tx).expect("empty commit should succeed");

    assert_eq!(txm.snapshot(), before_snapshot);
    assert_eq!(txm.current_lsn(), before_lsn);
    assert!(txm.store().applied.lock().is_empty());
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
    let err = txm.rollback(tx).expect_err("second rollback should fail");
    assert!(matches!(err, Error::TxNotFound(t) if t == tx));
}

#[test]
fn late_lower_tx_is_reassigned_above_watermark() {
    let txm = TxManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let t1 = txm.begin();
    let t2 = txm.begin();

    txm.with_write_set(t2, |ws| {
        ws.relational_inserts
            .push(("items".to_string(), versioned_row(RowId(2), t2)));
    })
    .unwrap();
    txm.commit(t2).expect("higher tx commits first");
    assert_eq!(txm.snapshot(), SnapshotId::from_tx(t2));

    txm.with_write_set(t1, |ws| {
        ws.relational_inserts
            .push(("items".to_string(), versioned_row(RowId(1), t1)));
    })
    .unwrap();
    txm.commit(t1).expect("late lower tx still commits");

    let applied = txm.store().applied.lock();
    let late_row = &applied[1].relational_inserts[0].1;
    assert!(
        late_row.created_tx.0 > t2.0,
        "late tx must be stamped above the existing watermark, got {:?}",
        late_row.created_tx
    );
    assert_eq!(txm.snapshot(), SnapshotId::from_tx(late_row.created_tx));
}

fn versioned_row(row_id: RowId, tx: TxId) -> VersionedRow {
    VersionedRow {
        row_id,
        values: HashMap::new(),
        created_tx: tx,
        deleted_tx: None,
        lsn: Lsn(0),
        created_at: None,
    }
}
