use contextdb_core::{
    AdjEntry, Lsn, RowId, SnapshotId, TxId, Value, VectorEntry, VectorIndexRef, VersionedRow,
};
use std::collections::HashMap;
use uuid::Uuid;

// The `set_budget(None)`-races-`try_allocate` TOCTOU repro that used to live
// here (`mem_01_set_budget_none_concurrent_allocate`) was a scheduler-race that
// could not actually fail: it only allocated after the limit was already
// removed, so `used` stayed 0, the over-budget branch was never entered, and
// the re-check guard it claimed to test was never exercised. Its deterministic
// replacement is the unit test
// `memory::tests::set_budget_none_racing_allocate_never_spuriously_fails`,
// which forces the exact interleaving via a `#[cfg(test)]`-gated hook (an
// integration test cannot reach such a hook, since it compiles the crate
// without `cfg(test)`).

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
