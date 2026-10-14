#![cfg(feature = "test-seams")]
//! Trigger audit history is a read: every entry it decodes and returns is
//! charged once, to the caller's declared reader -- not to a private reader at
//! the shipped default, not a second time to the writer.
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::memory_accounting::{
    take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_read_memory::{
    reader_parents_created_for_test, take_reader_budget_ledger_for_test,
    watch_reader_budget_ledger_for_test,
};
use std::collections::HashMap;

#[test]
fn trigger_audit_history_is_charged_once_to_the_declared_reader() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("audit.db")).unwrap();
    db.execute(
        "CREATE TABLE fire (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER fire_guard ON fire WHEN INSERT",
        &HashMap::new(),
    )
    .unwrap();
    db.register_trigger_callback("fire_guard", |_, _| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
    for id in 0..32i64 {
        db.execute(
            "INSERT INTO fire (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let parents = reader_parents_created_for_test();
    watch_budget_ledger_for_test();
    watch_reader_budget_ledger_for_test();
    let history = db.trigger_audit_history(Default::default()).unwrap();
    let writer = take_budget_ledger_for_test();
    let reader = take_reader_budget_ledger_for_test();
    assert_eq!(history.len(), 32);
    assert_eq!(
        reader_parents_created_for_test(),
        parents,
        "no private reader decodes the history"
    );
    assert!(
        !writer
            .iter()
            .any(|entry| entry.what == "writer_source/decode_stored_value"),
        "no audit entry is decoded on the writer: {writer:?}"
    );
    assert!(
        reader
            .iter()
            .any(|entry| entry.what == "reader/materialize_answer"),
        "the entries are decoded under the declared reader: {reader:?}"
    );
}
