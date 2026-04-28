use super::common::*;
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::Database;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use uuid::Uuid;

fn assert_send_sync<T: Send + Sync>() {}

/// I added contextdb-engine as a Rust dependency, created a table, inserted a row, and queried it back.
#[test]
fn f41_use_contextdb_engine_as_a_rust_dependency() {
    assert_send_sync::<Database>();
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create table");
    db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("hello".into())),
        ]),
    )
    .expect("insert row");
    let result = db
        .execute("SELECT * FROM t", &empty_params())
        .expect("select");
    assert_eq!(result.rows.len(), 1);
}

/// I shared one Database across 10 threads doing concurrent inserts, and all 100 rows landed without panics.
#[test]
fn f42_embed_contextdb_in_an_actix_or_axum_style_server() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, worker INTEGER)",
        &empty_params(),
    )
    .expect("create table");
    let mut handles = Vec::new();
    for worker in 0..10_i64 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                db.execute(
                    "INSERT INTO t (id, worker) VALUES ($id, $worker)",
                    &params(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("worker", Value::Int64(worker)),
                    ]),
                )
                .expect("thread insert");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker should not panic");
    }
    assert_eq!(query_count(&db, "SELECT count(*) FROM t"), 100);
}

/// I triggered parse errors, immutable violations, missing tables, and dimension mismatches, and each came back as a distinct typed error variant.
#[test]
fn f43_library_user_gets_typed_errors_not_strings() {
    let db = Database::open_memory();
    let parse = db
        .execute("SELET * FROM nope", &empty_params())
        .expect_err("parse error");
    assert!(matches!(parse, Error::ParseError(_)));

    db.execute(
        "CREATE TABLE immutable_rows (id UUID PRIMARY KEY) IMMUTABLE",
        &empty_params(),
    )
    .expect("create immutable table");
    let immutable = db
        .execute(
            "UPDATE immutable_rows SET id = $id",
            &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .expect_err("immutable update should fail");
    assert!(matches!(immutable, Error::ImmutableTable(_)));

    let missing = db
        .execute("SELECT * FROM missing_table", &empty_params())
        .expect_err("missing table should error");
    assert!(matches!(
        missing,
        Error::TableNotFound(_) | Error::PlanError(_)
    ));

    db.execute(
        "CREATE TABLE vectors (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create vectors table");
    let mismatch = db
        .execute(
            "INSERT INTO vectors (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("embedding", Value::Vector(vec![1.0, 2.0])),
            ]),
        )
        .expect_err("dimension mismatch should error");
    assert!(matches!(mismatch, Error::VectorDimensionMismatch { .. }));
}

/// I ran 10 threads each committing 100 rows in separate transactions, and all 1000 rows were visible afterward.
#[test]
fn f43c_mvcc_from_concurrent_threads() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, worker INTEGER)",
        &empty_params(),
    )
    .expect("create table");
    let mut handles = Vec::new();
    for worker in 0..10_i64 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let tx = db.begin();
            for _ in 0..100 {
                db.insert_row(
                    tx,
                    "t",
                    values(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("worker", Value::Int64(worker)),
                    ]),
                )
                .expect("insert in tx");
            }
            db.commit(tx).expect("commit tx");
        }));
    }
    for handle in handles {
        handle.join().expect("thread should not panic");
    }
    assert_eq!(query_count(&db, "SELECT count(*) FROM t"), 1000);
}

/// I had one writer inserting 1000 rows while 10 readers queried concurrently, and nobody panicked or deadlocked.
#[test]
fn f98_concurrent_readers_and_one_writer_on_embedded_engine() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, score INTEGER, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create observations");
    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        for idx in 0..1000_i64 {
            writer_db
                .execute(
                    "INSERT INTO observations (id, score, embedding) VALUES ($id, $score, $embedding)",
                    &params(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("score", Value::Int64(idx)),
                        ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
                    ]),
                )
                .expect("writer insert");
        }
    });
    let mut readers = Vec::new();
    for _ in 0..10 {
        let reader_db = db.clone();
        readers.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = reader_db.execute("SELECT count(*) FROM observations", &empty_params());
                let _ = reader_db.query_vector(
                    contextdb_core::VectorIndexRef::new("observations", "embedding"),
                    &[1.0, 0.0, 0.0],
                    5,
                    None,
                    reader_db.snapshot(),
                );
            }
        }));
    }
    writer.join().expect("writer");
    for reader in readers {
        reader.join().expect("reader");
    }
}

/// I inserted a row with every column type, queried it back, and each value came back as its proper typed variant, not a string.
#[test]
fn f99_database_execute_returns_structured_results_not_just_strings() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE typed (id UUID PRIMARY KEY, note TEXT, count INTEGER, reading REAL, enabled BOOLEAN, happened_at TIMESTAMP, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create typed table");
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO typed (id, note, count, reading, enabled, happened_at, embedding) VALUES ($id, $note, $count, $reading, $enabled, $ts, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("note", Value::Text("hello".into())),
            ("count", Value::Int64(7)),
            ("reading", Value::Float64(3.5)),
            ("enabled", Value::Bool(true)),
            ("ts", Value::Timestamp(123)),
            ("embedding", Value::Vector(vec![1.0, 2.0, 3.0])),
        ]),
    )
    .expect("insert typed row");
    let result = db
        .execute("SELECT * FROM typed", &empty_params())
        .expect("select typed row");
    assert_eq!(
        result.columns,
        vec![
            "id",
            "note",
            "count",
            "reading",
            "enabled",
            "happened_at",
            "embedding"
        ]
    );
    assert!(matches!(result.rows[0][0], Value::Uuid(_)));
    assert!(matches!(result.rows[0][1], Value::Text(_)));
    assert!(matches!(result.rows[0][2], Value::Int64(_)));
    assert!(matches!(result.rows[0][3], Value::Float64(_)));
    assert!(matches!(result.rows[0][4], Value::Bool(_)));
    assert!(matches!(result.rows[0][5], Value::Timestamp(_)));
    assert!(matches!(result.rows[0][6], Value::Vector(_)));
}

/// I created a table and called table_names/table_meta, and I got back the table name and its column definitions.
#[test]
fn f100_agent_can_introspect_schema_programmatically() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)",
        &empty_params(),
    )
    .expect("create sensors");
    let tables = db.table_names();
    assert!(tables.contains(&"sensors".to_string()));
    let meta = db.table_meta("sensors").expect("table metadata");
    assert_eq!(meta.columns.len(), 3);
    assert_eq!(meta.columns[0].name, "id");
}

/// I inserted a 768-dim vector into a VECTOR(384) column, and the error message told me exactly which dimensions were expected vs. provided.
#[test]
fn f102_vector_dimension_mismatch_produces_a_clear_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(384))",
        &empty_params(),
    )
    .expect("create obs");
    let err = db
        .execute(
            "INSERT INTO obs (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("embedding", Value::Vector(vec![0.0; 768])),
            ]),
        )
        .expect_err("dimension mismatch");
    let message = err.to_string();
    assert!(message.contains("expected 384"));
    assert!(message.contains("got 768"));
}

/// I opened two separate database files in one process, inserted different data, and each database had only its own rows.
#[test]
fn f106_multiple_database_instances_in_one_process() {
    let tmp = TempDir::new().expect("tempdir");
    let db_a = Database::open(tmp.path().join("a.db")).expect("db a");
    let db_b = Database::open(tmp.path().join("b.db")).expect("db b");
    db_a.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create t in a");
    db_b.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create t in b");
    db_a.execute(
        "INSERT INTO t (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("alpha".into())),
        ]),
    )
    .expect("insert a");
    db_b.execute(
        "INSERT INTO t (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("beta".into())),
        ]),
    )
    .expect("insert b");
    let a_rows = db_a
        .execute("SELECT name FROM t", &empty_params())
        .expect("select a");
    let b_rows = db_b
        .execute("SELECT name FROM t", &empty_params())
        .expect("select b");
    assert_eq!(a_rows.rows[0][0], Value::Text("alpha".into()));
    assert_eq!(b_rows.rows[0][0], Value::Text("beta".into()));
}

/// I inserted a JSON object and queried it back, and the full structured payload round-tripped intact.
#[test]
fn f108_agent_can_store_and_query_json_payloads() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE events (id UUID PRIMARY KEY, payload JSON)",
        &empty_params(),
    )
    .expect("create events");
    let payload = serde_json::json!({"tool": "search", "query": "weather", "confidence": 0.92});
    db.execute(
        "INSERT INTO events (id, payload) VALUES ($id, $payload)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("payload", Value::Json(payload.clone())),
        ]),
    )
    .expect("insert JSON row");
    let result = db
        .execute(
            "SELECT payload FROM events WHERE payload IS NOT NULL",
            &empty_params(),
        )
        .expect("query JSON row");
    assert_eq!(result.rows[0][0], Value::Json(payload));
}

/// I opened a database with a file path, with open_memory(), and with ":memory:", and all three worked without creating spurious files.
#[test]
fn f112_connection_string_api_is_obvious_for_common_cases() {
    let tmp = TempDir::new().expect("tempdir");
    Database::open(tmp.path().join("agent-memory.db")).expect("file-backed open");
    Database::open_memory();
    let in_memory = Database::open(":memory:").expect("sqlite style in-memory open");
    in_memory
        .execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty_params())
        .expect("create table in sqlite-style memory db");
    // Clean up the literal file if it was created (current bug: open treats ":memory:" as a path)
    let literal_file = std::path::Path::new(":memory:");
    let created_file = literal_file.exists();
    if created_file {
        let _ = std::fs::remove_file(literal_file);
    }
    assert!(
        !created_file,
        "Database::open(\":memory:\") should create an ephemeral DB, not a file named ':memory:'"
    );
}

// Named vector index RED tests from named-vector-indexes-tests.md.
/// I created an `embeddings` table with a column literally named `embedding`, and a sibling `other` table with a
/// column named `foo`. Each table got a vector that, if routing collapsed by row-id only, would WIN the wrong
/// table's search at top-1 — proving routing is by (table, column) identity, not by the column name `embedding`
/// or by row-id-only globality.
#[test]
fn f110_single_vector_column_preserves_single_index_semantics_with_disjoint_columns() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR(8))",
        &empty_params(),
    )
    .expect("create embeddings");
    db.execute(
        "CREATE TABLE other (id UUID PRIMARY KEY, foo VECTOR(8))",
        &empty_params(),
    )
    .expect("create other");

    // Insert a SINGLE vector into each table so each table's only candidate is unambiguous. Routing by row-id
    // only would still return one result per table; we therefore additionally probe with a vector that, in a
    // collapsed global store, would prefer the OTHER table's row id over the FROM-scoped one.
    let probe = vec![1.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let opposite = vec![0.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0];

    let em_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO embeddings (id, embedding) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(em_id)),
            ("v", Value::Vector(probe.clone())),
        ]),
    )
    .expect("insert embeddings");

    let foo_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO other (id, foo) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(foo_id)),
            ("v", Value::Vector(opposite.clone())),
        ]),
    )
    .expect("insert other");

    // Probing embeddings.embedding with `probe` MUST return em_id. A no-op global store with both rows would
    // also return em_id (it's closer), so this assertion alone is necessary but not sufficient.
    let r1 = db
        .execute(
            "SELECT id FROM embeddings ORDER BY embedding <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(probe.clone()))]),
        )
        .expect("search embeddings");
    let id_idx = r1.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(r1.rows[0][id_idx], Value::Uuid(em_id));

    // Probing other.foo with `probe` MUST return foo_id. A no-op global store would return em_id (closer to
    // probe than opposite); this assertion is the routing-discriminator.
    let r2 = db
        .execute(
            "SELECT id FROM other ORDER BY foo <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(probe))]),
        )
        .expect("search other");
    assert_eq!(
        r2.rows[0][id_idx],
        Value::Uuid(foo_id),
        "search of `other.foo` must return only `other`'s row, not the closer-by-cosine row in `embeddings.embedding`"
    );
}

/// I inserted a 5-dim vector into evidence.vector_text VECTOR(4) on a table with two declared vector columns; the engine
/// rejected the row with VectorIndexDimensionMismatch carrying the offending (table, column) — so cg can attribute the
/// error to the right embedding space without ambiguity.
#[test]
fn f114_vector_index_dimension_mismatch_carries_index_identity() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let result = db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("t", Value::Vector(vec![0.0_f32; 5])), // wrong: vector_text is dim 4
            ("v", Value::Vector(vec![0.0_f32; 8])), // correct
        ]),
    );
    match result {
        Err(Error::VectorIndexDimensionMismatch {
            index,
            expected,
            actual,
        }) => {
            assert_eq!(index, VectorIndexRef::new("evidence", "vector_text"));
            assert_eq!(expected, 4);
            assert_eq!(actual, 5);
        }
        other => panic!("expected VectorIndexDimensionMismatch with index identity, got {other:?}"),
    }
    let count = db
        .execute("SELECT id FROM evidence", &empty_params())
        .expect("select")
        .rows
        .len();
    assert_eq!(count, 0, "rejected row must not be partially inserted");
}
/// On a table with two registered vector columns, I queried by an absent column; the error named that absent column,
/// not a registered sibling.
#[test]
fn f115_query_against_unknown_vector_index_returns_typed_error_with_multi_column_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_known VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let result = db.execute(
        "SELECT id FROM evidence ORDER BY vector_unknown <=> $q LIMIT 1",
        &params(vec![("q", Value::Vector(vec![0.0_f32; 4]))]),
    );
    match result {
        Err(Error::UnknownVectorIndex { index }) => {
            assert_eq!(index, VectorIndexRef::new("evidence", "vector_unknown"));
        }
        other => {
            panic!("expected UnknownVectorIndex {{ ('evidence','vector_unknown') }}, got {other:?}")
        }
    }
}
