use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;

#[test]
fn a_file_session_reads_a_commit_even_when_its_writer_closed_between_statements() {
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
    assert!(session.snapshot_at().is_some());

    let replacement = Database::open(&path).unwrap();
    replacement
        .execute("INSERT INTO items VALUES (2)", &HashMap::new())
        .unwrap();
    replacement.close().unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
    assert_eq!(session.route(), ReadRoute::File);
    assert!(session.snapshot_at().is_some());
}
