use contextdb_core::{Error, Lsn, Result, RowId};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

#[test]
fn tx_01_failed_apply_preserves_transaction() {
    let store = FailingStore {
        next_row_id: AtomicU64::new(1),
        fail_apply: AtomicBool::new(true),
        ..Default::default()
    };
    let txm = TxManager::new(store);
    let tx = txm.begin();
    txm.with_write_set(tx, |ws| {
        ws.commit_lsn = Some(Lsn(999));
    })
    .expect("write set exists before commit");

    let err = txm.commit(tx).expect_err("commit should fail");
    assert!(
        err.to_string().contains("apply failed"),
        "expected applicator failure, got {err}"
    );

    txm.rollback(tx)
        .expect("failed commit must preserve the transaction for rollback");
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

    let _ = txm.commit(tx).expect_err("commit should fail");

    assert_eq!(
        txm.snapshot(),
        before,
        "failed commit must not advance the committed watermark"
    );
}
