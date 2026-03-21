use super::common::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use uuid::Uuid;

fn assert_send_sync<T: Send + Sync>() {}

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
                let _ = reader_db.query_vector(&[1.0, 0.0, 0.0], 5, None, reader_db.snapshot());
            }
        }));
    }
    writer.join().expect("writer");
    for reader in readers {
        reader.join().expect("reader");
    }
}

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
