use contextdb_core::read_contract::ReadRoute;
use contextdb_core::{Value, store_companion_path};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;

#[test]
fn a_companionless_reader_adopts_an_unchanged_first_writer() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.db");
    let path = directory.path().join("copy.db");
    let writer = Database::open(&source).unwrap();
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
    std::fs::copy(&source, &path).unwrap();
    assert!(!store_companion_path(&path).exists());
    let session = ReadSession::open(&path).unwrap();
    assert_eq!(session.route(), ReadRoute::File);
    let snapshot_lsn = session.snapshot_lsn().unwrap();
    let writer = Database::open(&path).unwrap();
    assert_eq!(writer.current_lsn(), snapshot_lsn);
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)]]
    );
    assert_eq!(
        session.route(),
        ReadRoute::Owner,
        "equal committed frontiers permit adoption"
    );
    drop(session);
    writer.close().unwrap();
}
