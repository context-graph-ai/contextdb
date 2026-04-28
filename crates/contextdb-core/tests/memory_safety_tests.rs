use contextdb_core::{
    AdjEntry, Lsn, MemoryAccountant, RowId, SnapshotId, TxId, Value, VectorEntry, VectorIndexRef,
    VersionedRow,
};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;
use uuid::Uuid;

// RACE: this exercises the current two-atomic budget representation. The bug
// window is small, so the loop count is intentionally high to make the failure
// observable without turning the test into a hard blocker if the scheduler does
// not hit the interleaving on a given machine.
#[test]
fn mem_01_set_budget_none_concurrent_allocate() {
    let mut saw_spurious_failure = false;

    for _ in 0..200 {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant
            .set_budget(Some(1024))
            .expect("test setup should allow adding a removable runtime budget");
        let barrier = Arc::new(Barrier::new(2));

        let alloc_acc = accountant.clone();
        let alloc_barrier = barrier.clone();
        let allocator = thread::spawn(move || {
            alloc_barrier.wait();
            let mut failures = 0usize;
            let mut observed_unbounded = false;
            for _ in 0..20_000 {
                if alloc_acc.usage().limit.is_none() {
                    observed_unbounded = true;
                }
                if observed_unbounded && alloc_acc.try_allocate(1).is_err() {
                    failures += 1;
                }
            }
            failures
        });

        let budget_acc = accountant.clone();
        let toggler = thread::spawn(move || {
            barrier.wait();
            budget_acc
                .set_budget(None)
                .expect("removing the budget should succeed");
        });

        toggler.join().expect("budget thread must finish");
        let failures = allocator.join().expect("allocator thread must finish");
        if failures > 0 {
            saw_spurious_failure = true;
            break;
        }
    }

    assert!(
        !saw_spurious_failure,
        "allocator observed MemoryBudgetExceeded after set_budget(None)"
    );
}

#[test]
fn mem_02_visible_at_uses_option_combinator() {
    let row = VersionedRow {
        row_id: RowId(1),
        values: HashMap::new(),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(0),
        created_at: None,
    };
    assert!(row.visible_at(SnapshotId(1)));
    assert!(row.visible_at(SnapshotId(99)));

    let deleted_row = VersionedRow {
        deleted_tx: Some(TxId(5)),
        ..row.clone()
    };
    assert!(deleted_row.visible_at(SnapshotId(3)));
    assert!(!deleted_row.visible_at(SnapshotId(5)));
    assert!(!deleted_row.visible_at(SnapshotId(7)));

    let edge = AdjEntry {
        source: Uuid::new_v4(),
        target: Uuid::new_v4(),
        edge_type: "CITES".to_string(),
        properties: HashMap::new(),
        created_tx: TxId(1),
        deleted_tx: Some(TxId(5)),
        lsn: Lsn(0),
    };
    assert!(edge.visible_at(SnapshotId(3)));
    assert!(!edge.visible_at(SnapshotId(5)));
    assert!(!edge.visible_at(SnapshotId(7)));

    let vector = VectorEntry {
        index: VectorIndexRef::default(),
        row_id: RowId(1),
        vector: vec![1.0, 2.0, 3.0],
        created_tx: TxId(1),
        deleted_tx: Some(TxId(5)),
        lsn: Lsn(0),
    };
    assert!(vector.visible_at(SnapshotId(3)));
    assert!(!vector.visible_at(SnapshotId(5)));
    assert!(!vector.visible_at(SnapshotId(7)));

    let _ = Value::Null;
}
