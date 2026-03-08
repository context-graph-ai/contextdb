use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

#[test]
fn create_insert_select_roundtrip() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE test (id UUID PRIMARY KEY, name TEXT)", &HashMap::new())
        .unwrap();

    db.execute(
        "INSERT INTO test (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("hello".into())),
        ]),
    )
    .unwrap();

    let q = db.execute("SELECT * FROM test", &HashMap::new()).unwrap();
    assert_eq!(q.rows.len(), 1);
}

#[test]
fn insert_on_conflict_do_update() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![("id", Value::Uuid(id)), ("name", Value::Text("a".into()))]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name) ON CONFLICT (id) DO UPDATE SET name=$name",
        &params(vec![("id", Value::Uuid(id)), ("name", Value::Text("b".into()))]),
    )
    .unwrap();

    let rows = db.scan("entities", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("name"), Some(&Value::Text("b".into())));
}

#[test]
fn vector_search_and_explain() {
    let db = Database::open_memory();
    let tx = db.begin();
    let r1 = db
        .insert_row(
            tx,
            "observations",
            params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    let r2 = db
        .insert_row(
            tx,
            "observations",
            params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    db.insert_vector(tx, r1, vec![1.0, 0.0]).unwrap();
    db.insert_vector(tx, r2, vec![0.0, 1.0]).unwrap();
    db.commit(tx).unwrap();

    let out = db
        .execute(
            "SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);

    let explain = db
        .explain("SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 1")
        .unwrap();
    assert!(explain.contains("VectorSearch"));
}

#[test]
fn immutable_observations_update_delete_error() {
    let db = Database::open_memory();

    let e1 = db.execute("UPDATE observations SET data='x'", &HashMap::new());
    assert!(matches!(e1, Err(Error::ImmutableTable(_))));

    let e2 = db.execute("DELETE FROM observations", &HashMap::new());
    assert!(matches!(e2, Err(Error::ImmutableTable(_))));
}

#[test]
fn invalidation_state_machine_sql_enforced() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("pending".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("acknowledged".into())),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("status", Value::Text("pending".into())),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidStateTransition(_)));
}

#[test]
fn parameter_binding_and_uuid_text_coercion() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Text(id.to_string())),
            ("name", Value::Text("n1".into())),
        ]),
    )
    .unwrap();

    let out = db
        .execute(
            "SELECT * FROM entities WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
}

#[test]
fn vector_dimension_validation_via_sql() {
    let db = Database::open_memory();

    db.execute(
        "INSERT INTO observations (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "INSERT INTO observations (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::VectorDimensionMismatch { .. }));
}

#[test]
fn duplicate_uuid_without_conflict_clause_errors() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![("id", Value::Uuid(id)), ("name", Value::Text("a".into()))]),
    )
    .unwrap();

    let err = db
        .execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &params(vec![("id", Value::Uuid(id)), ("name", Value::Text("b".into()))]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::UniqueViolation { .. }));
}

#[test]
fn archive_intention_with_active_decisions_is_blocked() {
    let db = Database::open_memory();
    let intention_id = Uuid::new_v4();
    let decision_id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        params(vec![
            ("id", Value::Uuid(intention_id)),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        params(vec![
            ("id", Value::Uuid(decision_id)),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    db.insert_edge(
        tx,
        decision_id,
        intention_id,
        "SERVES".to_string(),
        HashMap::new(),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let err = db
        .execute(
            "UPDATE intentions SET status='archived' WHERE id = $id",
            &params(vec![("id", Value::Uuid(intention_id))]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidStateTransition(_)));
}

#[test]
fn insert_edge_table_maintains_adjacency() {
    let db = Database::open_memory();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();

    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source", Value::Uuid(source)),
            ("target", Value::Uuid(target)),
            ("type", Value::Text("RELATES_TO".into())),
        ]),
    )
    .unwrap();

    let bfs = db
        .query_bfs(source, None, contextdb_core::Direction::Outgoing, 1, db.snapshot())
        .unwrap();
    assert_eq!(bfs.nodes.len(), 1);
}

#[test]
fn begin_commit_rollback_session_sql() {
    let db = Database::open_memory();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![("id", Value::Uuid(id1)), ("name", Value::Text("a".into()))]),
    )
    .unwrap();
    db.execute("COMMIT", &HashMap::new()).unwrap();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![("id", Value::Uuid(id2)), ("name", Value::Text("b".into()))]),
    )
    .unwrap();
    db.execute("ROLLBACK", &HashMap::new()).unwrap();

    let rows = db.scan("entities", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1);
}
