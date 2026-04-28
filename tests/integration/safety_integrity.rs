use super::helpers::*;
use contextdb_core::Lsn;
use contextdb_core::{Direction, Error, Value};
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
fn si_01_mixed_recovery_survives_crash_and_hides_uncommitted_work() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("recovery.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();

    let committed_parent = Uuid::new_v4();
    let committed_child = Uuid::new_v4();
    let tx = db.begin();
    let committed_parent_rid = db
        .insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(committed_parent)),
                (
                    "name".to_string(),
                    Value::Text("committed-parent".to_string()),
                ),
            ]),
        )
        .unwrap();
    let committed_child_rid = db
        .insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(committed_child)),
                (
                    "name".to_string(),
                    Value::Text("committed-child".to_string()),
                ),
            ]),
        )
        .unwrap();
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("entities", "embedding"),
        committed_parent_rid,
        vec![1.0, 0.0, 0.0],
    )
    .unwrap();
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("entities", "embedding"),
        committed_child_rid,
        vec![0.0, 1.0, 0.0],
    )
    .unwrap();
    db.insert_edge(
        tx,
        committed_parent,
        committed_child,
        "DEPENDS_ON".to_string(),
        HashMap::new(),
    )
    .unwrap();
    db.commit(tx).unwrap();
    assert_eq!(db.change_log_since(Lsn(0)).len(), 5);
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let snapshot = reopened.snapshot();
    assert_eq!(reopened.scan("entities", snapshot).unwrap().len(), 2);
    let parent = reopened
        .point_lookup("entities", "id", &Value::Uuid(committed_parent), snapshot)
        .unwrap()
        .unwrap();
    let child = reopened
        .point_lookup("entities", "id", &Value::Uuid(committed_child), snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(
        parent.values.get("name"),
        Some(&Value::Text("committed-parent".into()))
    );
    assert_eq!(
        child.values.get("name"),
        Some(&Value::Text("committed-child".into()))
    );

    let bfs = reopened
        .query_bfs(
            committed_parent,
            Some(&["DEPENDS_ON".to_string()]),
            Direction::Outgoing,
            1,
            snapshot,
        )
        .unwrap();
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, committed_child);

    let committed_vectors = reopened
        .query_vector(
            contextdb_core::VectorIndexRef::new("entities", "embedding"),
            &[1.0, 0.0, 0.0],
            10,
            None,
            snapshot,
        )
        .unwrap();
    assert_eq!(committed_vectors.len(), 2);
    assert!(
        committed_vectors
            .iter()
            .any(|(rid, _)| *rid == committed_parent_rid)
    );
    assert!(
        committed_vectors
            .iter()
            .any(|(rid, _)| *rid == committed_child_rid)
    );
    assert_eq!(reopened.change_log_since(Lsn(0)).len(), 5);

    let tx2 = reopened.begin();
    let ghost = Uuid::new_v4();
    let ghost_rid = reopened
        .insert_row(
            tx2,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ghost)),
                ("name".to_string(), Value::Text("uncommitted".to_string())),
            ]),
        )
        .unwrap();
    reopened
        .insert_vector(
            tx2,
            contextdb_core::VectorIndexRef::new("entities", "embedding"),
            ghost_rid,
            vec![0.0, 0.0, 1.0],
        )
        .unwrap();
    reopened
        .insert_edge(
            tx2,
            ghost,
            committed_child,
            "DEPENDS_ON".to_string(),
            HashMap::new(),
        )
        .unwrap();
    drop(reopened);

    let reopened_again = Database::open(&path).unwrap();
    let snapshot = reopened_again.snapshot();
    assert_eq!(reopened_again.scan("entities", snapshot).unwrap().len(), 2);
    assert!(
        reopened_again
            .point_lookup("entities", "id", &Value::Uuid(ghost), snapshot)
            .unwrap()
            .is_none()
    );
    let ghost_vectors = reopened_again
        .query_vector(
            contextdb_core::VectorIndexRef::new("entities", "embedding"),
            &[0.0, 0.0, 1.0],
            10,
            None,
            snapshot,
        )
        .unwrap();
    assert!(ghost_vectors.iter().all(|(rid, _)| *rid != ghost_rid));
    assert_eq!(reopened_again.change_log_since(Lsn(0)).len(), 5);
}

// si_07 and si_08 are retired as separate placeholders.
// Their graph/vector crash-reopen intent is covered by si_01 and the existing p24/p25 persistence guards.

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
