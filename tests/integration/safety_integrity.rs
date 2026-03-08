use super::helpers::*;
use contextdb_core::{Error, Value};

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
#[ignore = "requires memory accounting"]
fn si_09_ignored_memory_accounting() {}
