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

// ======== T9 ========
#[test]
fn test_create_table_txid_column_parses_and_roundtrips() {
    use contextdb_core::table_meta::ColumnType;
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, contextdb_core::Value> = HashMap::new();

    // Case 1: uppercase TXID, NOT NULL
    let db1 = Database::open_memory();
    db1.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse after stubs land");
    let meta1 = db1
        .table_meta("t")
        .expect("table_meta(\"t\") must be Some after CREATE TABLE");
    let col1 = meta1
        .columns
        .iter()
        .find(|c| c.name == "x")
        .expect("column \"x\" must exist in table meta");
    assert_eq!(
        col1.column_type,
        ColumnType::TxId,
        "column \"x\" type must be ColumnType::TxId (uppercase TXID)",
    );
    assert!(
        !col1.nullable,
        "column \"x\" declared NOT NULL must have nullable == false",
    );

    // Case 2: lowercase txid, implicit nullable
    let db2 = Database::open_memory();
    db2.execute("CREATE TABLE t2 (x txid)", &empty)
        .expect("CREATE TABLE t2 (x txid) must parse (lowercase)");
    let meta2 = db2
        .table_meta("t2")
        .expect("table_meta(\"t2\") must be Some after CREATE TABLE");
    let col2 = meta2
        .columns
        .iter()
        .find(|c| c.name == "x")
        .expect("column \"x\" must exist in t2");
    assert_eq!(
        col2.column_type,
        ColumnType::TxId,
        "column \"x\" type must be ColumnType::TxId (lowercase txid)",
    );
    assert!(
        col2.nullable,
        "column \"x\" with no NOT NULL must have nullable == true",
    );

    // Case 3: mixed-case Txid
    let db3 = Database::open_memory();
    db3.execute("CREATE TABLE t3 (x Txid)", &empty)
        .expect("CREATE TABLE t3 (x Txid) must parse (mixed case)");
    let meta3 = db3
        .table_meta("t3")
        .expect("table_meta(\"t3\") must be Some after CREATE TABLE");
    let col3 = meta3
        .columns
        .iter()
        .find(|c| c.name == "x")
        .expect("column \"x\" must exist in t3");
    assert_eq!(
        col3.column_type,
        ColumnType::TxId,
        "column \"x\" type must be ColumnType::TxId (mixed-case Txid)",
    );
}

// ======== T10 ========
#[test]
fn test_schema_render_txid_roundtrips() {
    use contextdb_core::table_meta::ColumnType;
    use contextdb_engine::Database;
    use contextdb_engine::cli_render::render_table_meta;
    use std::collections::HashMap;

    let empty: HashMap<String, contextdb_core::Value> = HashMap::new();

    // Database A: create the table with a TXID column.
    let db_a = Database::open_memory();
    db_a.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE with TXID must parse after stubs land");
    let meta_a = db_a
        .table_meta("t")
        .expect("table_meta(\"t\") must be Some after CREATE TABLE");
    let original_col_type = meta_a
        .columns
        .iter()
        .find(|c| c.name == "x")
        .map(|c| c.column_type.clone())
        .expect("column \"x\" must exist in t");
    assert_eq!(
        original_col_type,
        ColumnType::TxId,
        "sanity: original column type must be ColumnType::TxId",
    );

    // Render via the CLI surface.
    let rendered = render_table_meta("t", &meta_a);
    assert!(
        rendered.contains("TXID"),
        "rendered .schema output must contain \"TXID\", got: {rendered}",
    );
    assert!(
        !rendered.contains("BROKEN"),
        "rendered .schema output must not contain stub value \"BROKEN\", got: {rendered}",
    );

    // Database B: execute the rendered DDL on a fresh database.
    let db_b = Database::open_memory();
    db_b.execute(&rendered, &empty).unwrap_or_else(|e| {
        panic!("rendered DDL must parse back on fresh db: {e:?}\nrendered:\n{rendered}")
    });
    let meta_b = db_b
        .table_meta("t")
        .expect("table_meta(\"t\") must be Some on db_b after re-executing rendered DDL");
    let roundtripped_col_type = meta_b
        .columns
        .iter()
        .find(|c| c.name == "x")
        .map(|c| c.column_type.clone())
        .expect("column \"x\" must exist in t on db_b");
    assert_eq!(
        roundtripped_col_type, original_col_type,
        "round-tripped column type must equal the original ColumnType::TxId",
    );
}

// ======== T12 ========
#[test]
fn test_insert_value_txid_within_watermark_succeeds() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;
    use uuid::Uuid;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();

    // Drive the committed watermark forward with regular commits on an unrelated table.
    db.execute("CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)", &empty)
        .expect("CREATE TABLE bump must succeed");
    for n in 0..5i64 {
        let mut row: HashMap<String, Value> = HashMap::new();
        row.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
        row.insert("n".to_string(), Value::Int64(n));
        db.execute("INSERT INTO bump (id, n) VALUES ($id, $n)", &row)
            .unwrap_or_else(|e| panic!("bump insert {n} must succeed: {e:?}"));
    }
    let watermark = db.committed_watermark();
    assert!(
        watermark.0 >= 5,
        "committed_watermark must be >= 5 after five regular commits, got TxId({})",
        watermark.0,
    );

    // Create the TXID table.
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    // Insert Value::TxId(TxId(2)) — strictly below the watermark — via library API.
    let tx = db.begin();
    let mut values: HashMap<String, Value> = HashMap::new();
    values.insert("x".to_string(), Value::TxId(TxId(2)));
    let insert_result = db.insert_row(tx, "t", values);
    assert!(
        insert_result.is_ok(),
        "insert Value::TxId(TxId(2)) into TXID column with watermark >= 5 must succeed; got {insert_result:?}",
    );
    db.commit(tx)
        .expect("commit of Value::TxId(TxId(2)) insert must succeed");

    // SELECT * FROM t must return exactly one row with x == Value::TxId(TxId(2)).
    let result = db
        .execute("SELECT * FROM t", &empty)
        .expect("SELECT * FROM t must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one row must be stored after the single insert",
    );
    let x_idx = result
        .columns
        .iter()
        .position(|c| c == "x")
        .expect("result must have column \"x\"");
    assert_eq!(
        result.rows[0][x_idx],
        Value::TxId(TxId(2)),
        "stored value must round-trip equal to Value::TxId(TxId(2))",
    );
}

// ======== T13 ========
#[test]
fn test_insert_value_txid_beyond_watermark_rejected() {
    use contextdb_core::{Error, TxId, Value};
    use contextdb_engine::Database;

    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &Default::default())
        .expect("CREATE TABLE with TXID column must parse after stubs land");

    // Read the committed watermark as the max-allowed TxId baseline.
    let baseline = db.committed_watermark();

    // Attempt to insert Value::TxId(u64::MAX) — must fail with TxIdOutOfRange.
    // Stubs: coerce_value_for_column returns Err(ColumnTypeMismatch { actual: "STUB" }) unconditionally
    //        for a TXID column, so this test currently fails at the `match` arm below.
    // Impl must: positive-accept value <= current_tx_max; reject value > current_tx_max
    //           with TxIdOutOfRange { table, column, value, max } populated from the
    //           threaded statement-scoped snapshot.
    let tx = db.begin();
    let mut row = std::collections::HashMap::new();
    row.insert("x".to_string(), Value::TxId(TxId(u64::MAX)));
    let err = db
        .insert_row(tx, "t", row)
        .expect_err("insert must reject TxId(u64::MAX)");
    let _ = db.rollback(tx);

    match err {
        Error::TxIdOutOfRange {
            table,
            column,
            value,
            max,
        } => {
            assert_eq!(table, "t", "error.table must be the target table name");
            assert_eq!(column, "x", "error.column must be the target column name");
            assert_eq!(value, u64::MAX, "error.value must be the offending raw u64");
            assert_eq!(
                max, baseline.0,
                "error.max must equal the committed watermark at statement entry"
            );
        }
        other => panic!("expected Error::TxIdOutOfRange, got {other:?}"),
    }

    // Confirm no row was committed.
    let rows = db
        .execute("SELECT * FROM t", &Default::default())
        .expect("SELECT must succeed")
        .rows;
    assert_eq!(rows.len(), 0, "rejected insert must not commit any row");
}

// ======== T17 ========
#[test]
fn test_insert_wrong_variant_into_txid_column_rejected() {
    use contextdb_core::{Error, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;
    use uuid::Uuid;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    #[allow(clippy::approx_constant)]
    let fixtures: Vec<(Value, &'static str)> = vec![
        (Value::Timestamp(0), "Timestamp"),
        (Value::Int64(42), "Int64"),
        (Value::Float64(3.14), "Float64"),
        (Value::Text("x".into()), "Text"),
        (Value::Uuid(Uuid::nil()), "Uuid"),
        (Value::Bool(true), "Bool"),
        (Value::Json(serde_json::Value::Null), "Json"),
        (Value::Vector(vec![1.0]), "Vector"),
    ];

    for (value, expected_actual) in fixtures {
        let tx = db.begin();
        let mut row: HashMap<String, Value> = HashMap::new();
        row.insert("x".to_string(), value.clone());
        let err = db.insert_row(tx, "t", row).expect_err(&format!(
            "insert of {value:?} into TXID NOT NULL column must be rejected",
        ));
        let _ = db.rollback(tx);

        match err {
            Error::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            } => {
                assert_eq!(table, "t", "error.table must be \"t\" for {value:?}");
                assert_eq!(column, "x", "error.column must be \"x\" for {value:?}");
                assert_eq!(
                    expected, "TXID",
                    "error.expected must be \"TXID\" for {value:?}",
                );
                assert_eq!(
                    actual, expected_actual,
                    "error.actual must be {expected_actual:?} for {value:?}",
                );
            }
            other => panic!("expected Error::ColumnTypeMismatch for {value:?}, got {other:?}",),
        }
    }
}

// ======== T18 ========
#[test]
fn test_insert_null_respects_txid_nullability() {
    use contextdb_core::{Error, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();

    // Case (a): TXID NULL accepts Value::Null, stores it back.
    db.execute("CREATE TABLE t_null (x TXID NULL)", &empty)
        .expect("CREATE TABLE t_null (x TXID NULL) must parse");
    let tx_a = db.begin();
    let mut row_a: HashMap<String, Value> = HashMap::new();
    row_a.insert("x".to_string(), Value::Null);
    db.insert_row(tx_a, "t_null", row_a)
        .expect("Value::Null into TXID NULL column must succeed");
    db.commit(tx_a).expect("commit of null row must succeed");

    let result = db
        .execute("SELECT * FROM t_null", &empty)
        .expect("SELECT * FROM t_null must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one row must be stored in t_null",
    );
    let x_idx = result
        .columns
        .iter()
        .position(|c| c == "x")
        .expect("result must have column \"x\"");
    assert_eq!(
        result.rows[0][x_idx],
        Value::Null,
        "stored value must round-trip as Value::Null",
    );

    // Case (b): TXID NOT NULL rejects Value::Null with ColumnNotNullable, not ColumnTypeMismatch.
    db.execute("CREATE TABLE t_nn (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t_nn (x TXID NOT NULL) must parse");
    let tx_b = db.begin();
    let mut row_b: HashMap<String, Value> = HashMap::new();
    row_b.insert("x".to_string(), Value::Null);
    let err = db
        .insert_row(tx_b, "t_nn", row_b)
        .expect_err("Value::Null into TXID NOT NULL must be rejected");
    let _ = db.rollback(tx_b);

    match err {
        Error::ColumnNotNullable { table, column } => {
            assert_eq!(table, "t_nn", "error.table must be \"t_nn\"");
            assert_eq!(column, "x", "error.column must be \"x\"");
        }
        Error::ColumnTypeMismatch { .. } => panic!(
            "Value::Null into NOT NULL must produce ColumnNotNullable, not ColumnTypeMismatch: {err:?}",
        ),
        other => panic!("expected Error::ColumnNotNullable, got {other:?}",),
    }
}

// ======== T19 ========
#[test]
fn test_insert_value_txid_into_non_txid_columns_rejected() {
    use contextdb_core::{Error, TxId, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();

    let fixtures: Vec<(&'static str, &'static str, &'static str)> = vec![
        ("t_int", "INTEGER", "INTEGER"),
        ("t_ts", "TIMESTAMP", "TIMESTAMP"),
        ("t_text", "TEXT", "TEXT"),
        ("t_real", "REAL", "REAL"),
        ("t_bool", "BOOLEAN", "BOOLEAN"),
        ("t_uuid", "UUID", "UUID"),
        ("t_json", "JSON", "JSON"),
        ("t_vec", "VECTOR(3)", "VECTOR(3)"),
    ];

    for (table, col_type_ddl, expected_str) in fixtures {
        let db = Database::open_memory();
        let create = format!("CREATE TABLE {table} (x {col_type_ddl} NOT NULL)");
        db.execute(&create, &empty)
            .unwrap_or_else(|e| panic!("CREATE TABLE {table} ({col_type_ddl}) must parse: {e:?}"));

        let tx = db.begin();
        let mut row: HashMap<String, Value> = HashMap::new();
        row.insert("x".to_string(), Value::TxId(TxId(42)));
        let err = db.insert_row(tx, table, row).expect_err(&format!(
            "insert of Value::TxId into {col_type_ddl} column must be rejected",
        ));
        let _ = db.rollback(tx);

        match err {
            Error::ColumnTypeMismatch {
                table: t,
                column,
                expected,
                actual,
            } => {
                assert_eq!(t, table, "error.table must be {table:?}");
                assert_eq!(column, "x", "error.column must be \"x\" for {table}");
                assert_eq!(
                    expected, expected_str,
                    "error.expected must be {expected_str:?} for column type {col_type_ddl}",
                );
                assert_eq!(
                    actual, "TxId",
                    "error.actual must be \"TxId\" for Value::TxId into {col_type_ddl}",
                );
            }
            other => {
                panic!("expected Error::ColumnTypeMismatch for {col_type_ddl}, got {other:?}",)
            }
        }
    }
}

// ======== TU8 ========
#[test]
fn coerce_value_for_column_exhaustive_no_catch_all() {
    use regex::Regex;

    let executor_rs = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("executor.rs");
    let body = std::fs::read_to_string(&executor_rs)
        .unwrap_or_else(|e| panic!("read {}: {e}", executor_rs.display()));

    // Locate the metadata-borrowing coercion helper and extract its body by brace matching.
    let fn_start = body
        .find("fn coerce_value_for_column_with_meta")
        .expect("coerce_value_for_column_with_meta must exist in executor.rs");
    let tail = &body[fn_start..];
    let body_start = tail.find('{').expect("fn has opening brace");
    let mut depth = 0i32;
    let mut end = body_start;
    for (i, ch) in tail[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = body_start + i + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    let fn_body = &tail[body_start..end];

    // Locate the `match col.column_type { ... }` block inside the fn body.
    let match_start = fn_body
        .find("match col.column_type")
        .expect("match on col.column_type must exist");
    let match_tail = &fn_body[match_start..];
    let match_brace = match_tail.find('{').expect("match has opening brace");
    let mut depth = 0i32;
    let mut match_end = match_brace;
    for (i, ch) in match_tail[match_brace..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    match_end = match_brace + i + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    let match_body = &match_tail[match_brace..match_end];

    // Catch-all rejection: no `_ =>` arm at the match's top level.
    let catchall_re = Regex::new(r"(?m)^\s*_\s*=>").expect("catchall regex compiles");
    assert!(
        !catchall_re.is_match(match_body),
        "coerce_value_for_column_with_meta's match on col.column_type must not contain a `_ =>` catch-all arm; body:\n{match_body}"
    );

    // Runtime leg: inserting Value::Text("x") into a VECTOR(3) column must not silently succeed.
    use contextdb_core::Value;
    use contextdb_engine::Database;

    let db = Database::open_memory();
    db.execute("CREATE TABLE v (e VECTOR(3))", &Default::default())
        .expect("CREATE TABLE v must succeed");

    let mut row = std::collections::HashMap::new();
    row.insert("e".to_string(), Value::Text("x".to_string()));
    let tx = db.begin();
    let result = db.insert_row(tx, "v", row);
    let _ = db.rollback(tx);
    assert!(
        result.is_err(),
        "insert Value::Text('x') into VECTOR(3) must be rejected, got {result:?}"
    );
}
