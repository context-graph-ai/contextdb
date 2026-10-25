use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;

#[test]
fn a_file_session_reads_a_commit_even_when_its_writer_closed_between_statements() {
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000));
    let _clock = contextdb_core::Wallclock::test_clock_guard({
        let clock = clock.clone();
        move || clock.load(std::sync::atomic::Ordering::SeqCst)
    });
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("between-statements.db");
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
    writer
        .execute("INSERT INTO items VALUES (1)", &HashMap::new())
        .unwrap();
    let session = ReadSession::open(&path).unwrap();
    writer.close().unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)]]
    );
    assert_eq!(session.route(), ReadRoute::File);
    let first_snapshot = session.snapshot_at().unwrap();

    let replacement = Database::open(&path).unwrap();
    replacement
        .execute("INSERT INTO items VALUES (2)", &HashMap::new())
        .unwrap();
    replacement.close().unwrap();
    clock.store(2000, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
    assert_eq!(session.route(), ReadRoute::File);
    assert!(session.snapshot_at().unwrap() > first_snapshot);
}

#[test]
fn a_copied_file_without_a_companion_follows_its_first_writer() {
    let directory = tempfile::tempdir().unwrap();
    let original = directory.path().join("original.db");
    let path = directory.path().join("copy.db");
    let writer = Database::open(&original).unwrap();
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
    writer
        .execute("INSERT INTO items VALUES (1)", &HashMap::new())
        .unwrap();
    writer.close().unwrap();
    std::fs::copy(&original, &path).unwrap();
    assert!(!contextdb_core::store_companion_path(&path).exists());
    let session = ReadSession::open(&path).unwrap();
    assert_eq!(session.route(), ReadRoute::File);
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)]]
    );
    let writer = Database::open(&path).unwrap();
    writer
        .execute("INSERT INTO items VALUES (2)", &HashMap::new())
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
    assert_eq!(session.route(), ReadRoute::Owner);
    writer.close().unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
    assert_eq!(session.route(), ReadRoute::File);
}
