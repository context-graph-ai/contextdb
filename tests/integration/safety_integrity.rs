use super::helpers::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn si_02_invalidation_transitions() {
    let db = setup_ontology_db();
    let id = uuid::Uuid::new_v4();

    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("pending".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![("id", Value::Uuid(id)), ("status", Value::Text("acknowledged".into()))]),
    )
    .unwrap();

    let err = db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![("id", Value::Uuid(id)), ("status", Value::Text("pending".into()))]),
    ).unwrap_err();
    assert!(matches!(err, Error::InvalidStateTransition(_)));
}

#[test]
fn si_03_observation_immutability() {
    let db = setup_ontology_db();
    let err = db
        .execute(
            "UPDATE observations SET data='x'",
            &std::collections::HashMap::new(),
        )
        .unwrap_err();
    assert!(matches!(err, Error::ImmutableTable(_)));
}

#[test]
#[ignore = "requires WAL recovery"]
fn si_01_ignored_wal_recovery() {}

#[test]
#[ignore = "requires graph crash recovery"]
fn si_07_ignored_graph_crash_recovery() {}

#[test]
#[ignore = "requires vector crash recovery"]
fn si_08_ignored_vector_crash_recovery() {}

#[test]
fn si_09_reopen_preserves_configured_memory_limit() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("memory-limit.db");
    let db = Database::open(&path).unwrap();
    db.execute("SET MEMORY_LIMIT '1K'", &HashMap::new())
        .unwrap();
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    let show = reopened
        .execute("SHOW MEMORY_LIMIT", &HashMap::new())
        .unwrap();
    assert_eq!(show.rows.len(), 1);
    assert_eq!(show.rows[0][0], Value::Int64(1024));

    let err = reopened
        .execute(
            "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .and_then(|_| {
            reopened.execute(
                "INSERT INTO big (id, payload) VALUES ($id, $payload)",
                &make_params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("payload", Value::Text("x".repeat(4096))),
                ]),
            )
        })
        .unwrap_err();
    assert!(matches!(err, Error::MemoryBudgetExceeded { .. }));
}
