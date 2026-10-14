#![cfg(feature = "test-seams")]
//! A read is charged to the reader limit its consumer declared and a write to
//! the declared writer limit, never one for the other. Audit history, a
//! snapshot inspection and an indexed vector search with a pending suffix
//! decode only under the reader; an import's write work decodes only under
//! the writer; a reader too small for a read refuses with the reader's own
//! label and returns what it took.
use contextdb_core::Value;
use contextdb_engine::database::SnapshotInspector;
use contextdb_engine::memory_accounting::{
    BudgetLedgerEntry, take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_read_memory::{
    DeclaredReadMemory, take_reader_budget_ledger_for_test, watch_reader_budget_ledger_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const WRITER_DECODE: &str = "writer_source/decode_stored_value";

fn open(path: &std::path::Path, writer: usize, reader: u64) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(writer),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = reader;
    Database::open_with_options(path, options).unwrap()
}

fn vector(id: i64) -> Vec<f32> {
    let mut values: Vec<f32> = (0..32).map(|lane| ((id * 31 + lane) % 17) as f32).collect();
    values[(id % 32) as usize] += 100.0;
    values
}

fn insert_point(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("vector".into(), Value::Vector(vector(id))),
        ]),
    )
    .unwrap();
}

/// Fired triggers for audit history, and a built vector route with one
/// pending row so a search reads both its graph and its suffix.
fn store(path: &std::path::Path, writer: usize, reader: u64) -> Database {
    let db = open(path, writer, reader);
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
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(32) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)", &HashMap::new()).unwrap();
    db.complete_initialization().unwrap();
    for id in 0..32i64 {
        db.execute(
            "INSERT INTO fire (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Int64(id))]),
        )
        .unwrap();
    }
    for id in 0..64 {
        insert_point(&db, id);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    insert_point(&db, 1000);
    db
}

fn writer_decodes(ledger: &[BudgetLedgerEntry]) -> Vec<&BudgetLedgerEntry> {
    ledger
        .iter()
        .filter(|entry| entry.what == WRITER_DECODE)
        .collect()
}

#[test]
fn reads_decode_only_under_the_declared_reader_never_the_writer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("reads.db");
    let db = store(&path, 64 * 1024 * 1024, 1 << 30);
    let artifact = root.path().join("reads.snapshot");
    db.export_snapshot(&artifact).unwrap();
    let parent = DeclaredReadMemory::from_reader_limit(1 << 30);
    let inspector =
        SnapshotInspector::open(&artifact, parent.child().unwrap(), parent.clone()).unwrap();

    watch_budget_ledger_for_test();
    watch_reader_budget_ledger_for_test();
    let history = db.trigger_audit_history(Default::default()).unwrap();
    let nearest = db
        .execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(vector(1000)))]),
        )
        .unwrap()
        .rows;
    let inspection = inspector.inspect_sync_apply_state();
    let writer = take_budget_ledger_for_test();
    let reader = take_reader_budget_ledger_for_test();
    assert_eq!(history.len(), 32);
    assert_eq!(nearest, vec![vec![Value::Int64(1000)]]);
    assert!(inspection.is_ok(), "{inspection:?}");
    assert!(
        writer_decodes(&writer).is_empty(),
        "no read decodes on the writer: {:?}",
        writer_decodes(&writer)
    );
    assert!(
        reader
            .iter()
            .any(|entry| entry.what == "reader/materialize_answer"),
        "the reads decode under the declared reader: {reader:?}"
    );
}

#[test]
fn import_writes_decode_only_under_the_declared_writer_never_the_reader() {
    let root = tempfile::tempdir().unwrap();
    let db = store(&root.path().join("writes.db"), 64 * 1024 * 1024, 1 << 30);
    for id in 2000..2032 {
        insert_point(&db, id);
    }
    let reader_before = db.declared_read_memory_for_test().retained_bytes_for_test();
    watch_budget_ledger_for_test();
    watch_reader_budget_ledger_for_test();
    assert!(db.persist_import_vector_tails().unwrap());
    let writer = take_budget_ledger_for_test();
    let reader = take_reader_budget_ledger_for_test();
    assert!(
        !writer_decodes(&writer).is_empty(),
        "the import's decodes are charged to the writer"
    );
    assert!(
        reader.is_empty(),
        "the import's write work admits nothing on the reader: {reader:?}"
    );
    assert_eq!(
        db.declared_read_memory_for_test().retained_bytes_for_test(),
        reader_before
    );
}

#[test]
fn a_reader_too_small_for_a_read_refuses_with_its_own_label_and_returns_its_charge() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("tight-reader.db");
    let db = store(&path, 64 * 1024 * 1024, 1 << 30);
    db.close().unwrap();
    // Reopen with a reader that can open the store but not hold the history.
    let db = open(&path, 64 * 1024 * 1024, 1024);
    let before = db.declared_read_memory_for_test().retained_bytes_for_test();
    let refused = db
        .trigger_audit_history(Default::default())
        .expect_err("1 KiB cannot hold 32 audit entries");
    assert!(
        matches!(
            &refused,
            contextdb_core::Error::MemoryBudgetExceeded { subsystem, .. } if subsystem == "reader"
        ),
        "{refused:?}"
    );
    assert_eq!(
        db.declared_read_memory_for_test().retained_bytes_for_test(),
        before,
        "a refused read returns every charge it took"
    );
}
