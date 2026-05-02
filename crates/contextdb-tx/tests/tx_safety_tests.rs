use contextdb_core::{Error, Lsn, Result, RowId, TxId, Value, VersionedRow};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

#[derive(Default)]
struct FailingStore {
    applied: Mutex<Vec<WriteSet>>,
    next_row_id: AtomicU64,
    fail_apply: AtomicBool,
}

impl WriteSetApplicator for FailingStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        if self.fail_apply.load(Ordering::SeqCst) {
            return Err(Error::Other("apply failed".to_string()));
        }
        self.applied.lock().push(ws);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Default)]
struct BlockingStore {
    applied: Mutex<Vec<WriteSet>>,
    next_row_id: AtomicU64,
    apply_entered: AtomicBool,
    release_apply: AtomicBool,
    state: Mutex<()>,
    waiters: Condvar,
}

impl BlockingStore {
    fn wait_until_apply_entered(&self) {
        let mut state = self.state.lock();
        while !self.apply_entered.load(Ordering::SeqCst) {
            self.waiters.wait(&mut state);
        }
    }

    fn release_apply(&self) {
        self.release_apply.store(true, Ordering::SeqCst);
        self.waiters.notify_all();
    }
}

impl WriteSetApplicator for BlockingStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        self.apply_entered.store(true, Ordering::SeqCst);
        self.waiters.notify_all();

        let mut state = self.state.lock();
        while !self.release_apply.load(Ordering::SeqCst) {
            self.waiters.wait(&mut state);
        }
        drop(state);

        self.applied.lock().push(ws);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }
}

fn stage_empty_row<S: WriteSetApplicator>(txm: &TxManager<S>, tx: TxId, row_id: u64) {
    txm.with_write_set(tx, |ws| {
        ws.relational_inserts.push((
            "items".to_string(),
            VersionedRow {
                row_id: RowId(row_id),
                values: HashMap::new(),
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(0),
                created_at: None,
            },
        ));
    })
    .expect("write set exists before commit");
}

#[test]
fn tx_01_failed_apply_removes_transaction_under_commit_lock() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(true),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    txm.advance_for_sync(tx, "items", TxId(100))
        .expect("sync floor should stage before commit");
    stage_empty_row(&txm, tx, 1);

    let err = txm.commit(tx).expect_err("commit should fail");
    assert!(
        err.to_string().contains("apply failed"),
        "expected applicator failure, got {err}"
    );

    txm.store().fail_apply.store(false, Ordering::SeqCst);
    assert!(
        matches!(txm.commit(tx), Err(Error::TxNotFound(missing)) if missing == tx),
        "failed commit must not leave a retryable active transaction"
    );
    assert!(
        matches!(txm.rollback(tx), Err(Error::TxNotFound(missing)) if missing == tx),
        "failed commit must clean up the active transaction before returning"
    );
    assert!(
        txm.store().applied.lock().is_empty(),
        "failed commit must not apply any writes"
    );
}

#[test]
fn tx_02_commit_failure_does_not_advance_watermark() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(true),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    let before = txm.snapshot();
    stage_empty_row(&txm, tx, 1);

    let _ = txm.commit(tx).expect_err("commit should fail");

    assert_eq!(
        txm.snapshot(),
        before,
        "failed commit must not advance the committed watermark"
    );
}

#[test]
fn tx_03_sync_txid_floor_is_not_visible_until_commit() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(false),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    txm.advance_for_sync(tx, "items", TxId(100))
        .expect("sync floor should stage on active tx");
    stage_empty_row(&txm, tx, 1);

    assert_eq!(
        txm.snapshot().0,
        0,
        "staging a peer TxId must not advance public visibility before commit"
    );

    txm.commit(tx).expect("commit should succeed");
    assert_eq!(
        txm.snapshot().0,
        100,
        "public visibility should advance to the staged peer TxId after commit"
    );
}

#[test]
fn tx_04_failed_apply_rewinds_lsn_allocator() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(true),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    let before = txm.current_lsn();
    stage_empty_row(&txm, tx, 1);

    let _ = txm.commit(tx).expect_err("commit should fail");

    assert_eq!(
        txm.current_lsn(),
        before,
        "failed apply must not leave an uncommitted LSN gap"
    );
}

#[test]
fn tx_05_commit_removes_active_transaction_before_apply() {
    let store = BlockingStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    };
    let txm = Arc::new(TxManager::new(store));
    let tx = txm.begin();
    stage_empty_row(&txm, tx, 1);

    let committing = Arc::clone(&txm);
    let handle = thread::spawn(move || committing.commit(tx));

    txm.store().wait_until_apply_entered();
    assert!(
        matches!(txm.with_write_set(tx, |_| ()), Err(Error::TxNotFound(missing)) if missing == tx),
        "commit must make a transaction non-mutable before store apply starts"
    );

    txm.store().release_apply();
    handle
        .join()
        .expect("commit thread should not panic")
        .expect("commit should succeed");
    assert_eq!(
        txm.store().applied.lock().len(),
        1,
        "commit should apply the captured write set exactly once"
    );
}

#[test]
fn tx_06_failed_ddl_lsn_allocation_does_not_advance_current_lsn() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(false),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let before = txm.current_lsn();

    let err = txm
        .allocate_ddl_lsn(|_| Err::<(), _>(Error::Other("ddl log failed".to_string())))
        .expect_err("failed DDL logging should surface");

    assert!(
        err.to_string().contains("ddl log failed"),
        "unexpected DDL allocation error: {err}"
    );
    assert_eq!(
        txm.current_lsn(),
        before,
        "failed DDL logging must not advance the committed LSN watermark"
    );
}

#[test]
fn tx_07_prepare_runs_after_transaction_is_frozen() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(false),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    txm.advance_for_sync(tx, "items", TxId(100))
        .expect("sync floor should stage before commit");
    txm.with_write_set(tx, |ws| {
        ws.relational_inserts.push((
            "items".to_string(),
            VersionedRow {
                row_id: RowId(1),
                values: HashMap::new(),
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(999),
                created_at: None,
            },
        ));
    })
    .expect("write set exists before commit");

    let (_, committed_ws) = txm
        .commit_with_lsn_prepared(tx, |ws| {
            assert_eq!(
                ws.commit_lsn,
                Some(Lsn(1)),
                "prepare must receive the stamped frozen write set"
            );
            assert_eq!(
                ws.relational_inserts[0].1.created_tx,
                TxId(100),
                "prepare must receive the same visibility TxId that apply will store"
            );
            assert!(
                matches!(txm.with_write_set(tx, |_| ()), Err(Error::TxNotFound(missing)) if missing == tx),
                "same-tx writes must be rejected before prepare runs"
            );
            Ok(())
        })
        .expect("commit should succeed");

    assert_eq!(
        committed_ws.commit_lsn,
        Some(Lsn(1)),
        "returned write set must be the stamped committed payload"
    );
    assert_eq!(
        committed_ws.relational_inserts[0].1.created_tx,
        TxId(100),
        "returned write set must match the prepared visibility payload"
    );
}

#[test]
fn tx_08_prepare_apply_and_return_see_canonical_final_rows() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(false),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    txm.with_write_set(tx, |ws| {
        ws.relational_inserts.push((
            "items".to_string(),
            VersionedRow {
                row_id: RowId(1),
                values: HashMap::from([("v".to_string(), Value::Text("old".to_string()))]),
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(0),
                created_at: None,
            },
        ));
        ws.relational_inserts.push((
            "items".to_string(),
            VersionedRow {
                row_id: RowId(1),
                values: HashMap::from([("v".to_string(), Value::Text("new".to_string()))]),
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(0),
                created_at: None,
            },
        ));
    })
    .expect("write set exists before commit");

    let (_, committed_ws) = txm
        .commit_with_lsn_prepared(tx, |ws| {
            assert_eq!(ws.relational_inserts.len(), 1);
            assert_eq!(
                ws.relational_inserts[0].1.values.get("v"),
                Some(&Value::Text("new".to_string()))
            );
            Ok(())
        })
        .expect("commit should succeed");

    assert_eq!(committed_ws.relational_inserts.len(), 1);
    let applied = txm.store().applied.lock();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].relational_inserts.len(), 1);
    assert_eq!(
        applied[0].relational_inserts[0].1.values.get("v"),
        Some(&Value::Text("new".to_string()))
    );
}
