use contextdb_core::{Error, Value, read_contract::ReadFailureKind};
use contextdb_engine::{Database, ReadSession};
use redb::TableDefinition;
use std::collections::HashMap;

const COMMIT_INDEX: TableDefinition<u64, u64> = TableDefinition::new("commit_index");

#[test]
fn writable_recovery_truncates_only_commit_index_entries_past_the_committed_lsn() {
    let root = tempfile::tempdir().expect("task-scoped store");
    let path = root.path().join("interrupted-history.db");
    let database = Database::open(&path).expect("create store");
    database
        .execute(
            "CREATE TABLE rows (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("commit schema");
    database
        .execute("INSERT INTO rows VALUES (7)", &HashMap::new())
        .expect("commit row");
    let committed = database.commit_index_for_test();
    let (last_lsn, last_tx) = *committed.last().expect("committed history exists");
    database.close().expect("close fixture owner");

    let raw = redb::Database::open(&path).expect("open physical store");
    let write = raw.begin_write().expect("stage interrupted suffix");
    {
        let mut table = write.open_table(COMMIT_INDEX).expect("open physical index");
        table
            .insert(last_lsn.0 + 1, last_tx.0 + 1)
            .expect("first uncommitted entry");
        table
            .insert(last_lsn.0 + 2, last_tx.0 + 2)
            .expect("second uncommitted entry");
    }
    write
        .commit()
        .expect("leave physical suffix without identity publication");
    drop(raw);

    assert!(matches!(
        ReadSession::open(&path),
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::DirectReadRequiresWriter
    ));
    let recovered = Database::open(&path).expect("writable open repairs uncommitted suffix");
    assert_eq!(recovered.commit_index_for_test(), committed);
    assert_eq!(
        recovered
            .execute("SELECT id FROM rows", &HashMap::new())
            .expect("committed row remains readable")
            .rows(),
        vec![vec![Value::Int64(7)]]
    );
    recovered.close().expect("close recovered owner");
    let file = ReadSession::open(&path).expect("direct read after recovery");
    assert_eq!(
        file.execute("SELECT id FROM rows", &HashMap::new())
            .expect("complete file read after recovery")
            .rows(),
        vec![vec![Value::Int64(7)]]
    );
}
