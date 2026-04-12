use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn setup_sql_db() -> Database {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3)) IMMUTABLE",
        &empty,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
        &empty,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty,
    )
    .unwrap();
    db
}

#[test]
fn create_insert_select_roundtrip() {
    let db = setup_sql_db();
    db.execute(
        "CREATE TABLE test (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
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
    let db = setup_sql_db();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("a".into())),
        ]),
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
    let db = setup_sql_db();
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
    let db = setup_sql_db();

    let e1 = db.execute("UPDATE observations SET data='x'", &HashMap::new());
    assert!(matches!(e1, Err(Error::ImmutableTable(_))));

    let e2 = db.execute("DELETE FROM observations", &HashMap::new());
    assert!(matches!(e2, Err(Error::ImmutableTable(_))));
}

#[test]
fn invalidation_state_machine_sql_enforced() {
    let db = setup_sql_db();
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
    let db = setup_sql_db();
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
    let db = setup_sql_db();

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
    let db = setup_sql_db();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("a".into())),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("b".into())),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::UniqueViolation { .. }));
}

#[test]
fn test_insert_duplicate_unique_value_is_noop() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT UNIQUE)",
        &HashMap::new(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO people (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("alex".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO people (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("alex".into())),
        ]),
    )
    .unwrap();

    assert_eq!(db.scan("people", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn test_insert_duplicate_composite_unique_tuple_is_noop() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE relationships (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, UNIQUE (source_id, target_id, edge_type))",
        &HashMap::new(),
    )
    .unwrap();

    let source_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();
    let edge_type = "RELATES_TO";

    db.execute(
        "INSERT INTO relationships (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(source_id)),
            ("target_id", Value::Uuid(target_id)),
            ("edge_type", Value::Text(edge_type.into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO relationships (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(source_id)),
            ("target_id", Value::Uuid(target_id)),
            ("edge_type", Value::Text(edge_type.into())),
        ]),
    )
    .unwrap();

    assert_eq!(db.scan("relationships", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn test_insert_distinct_composite_unique_tuple_still_creates_second_row() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE relationships (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, UNIQUE (source_id, target_id, edge_type))",
        &HashMap::new(),
    )
    .unwrap();

    let source_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();

    db.execute(
        "INSERT INTO relationships (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(source_id)),
            ("target_id", Value::Uuid(target_id)),
            ("edge_type", Value::Text("RELATES_TO".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO relationships (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(source_id)),
            ("target_id", Value::Uuid(target_id)),
            ("edge_type", Value::Text("DEPENDS_ON".into())),
        ]),
    )
    .unwrap();

    assert_eq!(db.scan("relationships", db.snapshot()).unwrap().len(), 2);
}

#[test]
fn test_insert_with_missing_foreign_key_fails() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id))",
        &HashMap::new(),
    )
    .unwrap();

    let err = db
        .execute(
            "INSERT INTO children (id, parent_id) VALUES ($id, $parent_id)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("parent_id", Value::Uuid(Uuid::new_v4())),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::ForeignKeyViolation { .. }));
}

#[test]
fn test_update_with_missing_foreign_key_fails() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id))",
        &HashMap::new(),
    )
    .unwrap();

    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO parents (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(parent_id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO children (id, parent_id) VALUES ($id, $parent_id)",
        &params(vec![
            ("id", Value::Uuid(child_id)),
            ("parent_id", Value::Uuid(parent_id)),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "UPDATE children SET parent_id = $parent_id WHERE id = $id",
            &params(vec![
                ("id", Value::Uuid(child_id)),
                ("parent_id", Value::Uuid(Uuid::new_v4())),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::ForeignKeyViolation { .. }));
}

#[test]
fn test_insert_with_existing_foreign_key_succeeds() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id))",
        &HashMap::new(),
    )
    .unwrap();

    let parent_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO parents (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(parent_id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO children (id, parent_id) VALUES ($id, $parent_id)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("parent_id", Value::Uuid(parent_id)),
        ]),
    )
    .unwrap();
}

#[test]
fn insert_edge_table_maintains_adjacency() {
    let db = setup_sql_db();
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
        .query_bfs(
            source,
            None,
            contextdb_core::Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(bfs.nodes.len(), 1);
}

#[test]
fn begin_commit_rollback_session_sql() {
    let db = setup_sql_db();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(id1)),
            ("name", Value::Text("a".into())),
        ]),
    )
    .unwrap();
    db.execute("COMMIT", &HashMap::new()).unwrap();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(id2)),
            ("name", Value::Text("b".into())),
        ]),
    )
    .unwrap();
    db.execute("ROLLBACK", &HashMap::new()).unwrap();

    let rows = db.scan("entities", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_empty_database_has_no_tables() {
    let db = Database::open_memory();
    assert!(db.table_names().is_empty());
}

#[test]
fn test_create_table_then_insert() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t1 (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t1 (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("n".into())),
        ]),
    )
    .unwrap();
    assert_eq!(db.scan("t1", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn test_insert_into_nonexistent_table_fails() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "INSERT INTO missing (id) VALUES ($id)",
            &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::TableNotFound(_)));
}

#[test]
fn test_immutable_via_create_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE foo (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
        &HashMap::new(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO foo (id, data) VALUES ($id, $data)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("data", Value::Text("x".into())),
        ]),
    )
    .unwrap();
    let err = db.execute("DELETE FROM foo", &HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::ImmutableTable(_)));
}

#[test]
fn test_state_machine_via_create_table() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();
    db.execute(
        "CREATE TABLE sm (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved])",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO sm (id, status) VALUES ($id, $status)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("pending".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO sm (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("acknowledged".into())),
        ]),
    )
    .unwrap();
    let err = db
        .execute(
            "INSERT INTO sm (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("status", Value::Text("pending".into())),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidStateTransition(_)));
}

#[test]
fn test_drop_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE to_drop (id UUID PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("DROP TABLE to_drop", &HashMap::new()).unwrap();
    let err = db
        .execute(
            "INSERT INTO to_drop (id) VALUES ($id)",
            &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::TableNotFound(_)));
}
