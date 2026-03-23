use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use tempfile::TempDir;

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn col_index(result: &QueryResult, name: &str) -> Option<usize> {
    result.columns.iter().position(|c| c == name)
}

#[test]
fn at01_add_column_existing_rows_show_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
        &empty_params(),
    )
    .unwrap();

    db.execute("ALTER TABLE t ADD COLUMN score REAL", &empty_params())
        .unwrap();

    let result = db.execute("SELECT * FROM t", &empty_params()).unwrap();
    let idx = col_index(&result, "score").expect("column 'score' must exist after ADD COLUMN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][idx],
        Value::Null,
        "existing row must have NULL for added column"
    );
}

#[test]
fn at02_add_column_new_insert_uses_it() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN score REAL", &empty_params())
        .unwrap();

    db.execute(
        "INSERT INTO t (id, name, score) VALUES (1, 'bob', 3.14)",
        &empty_params(),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM t WHERE id = 1", &empty_params())
        .unwrap();
    let idx = col_index(&result, "score").expect("column 'score' must exist");
    assert_eq!(result.rows[0][idx], Value::Float64(314_f64 / 100.0));
}

#[test]
fn at03_drop_column_disappears_from_select_star() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, extra TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, name, extra) VALUES (1, 'alice', 'remove-me')",
        &empty_params(),
    )
    .unwrap();

    db.execute("ALTER TABLE t DROP COLUMN extra", &empty_params())
        .unwrap();

    let result = db.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result, "extra").is_none(),
        "'extra' must not appear after DROP COLUMN"
    );
    for row in &result.rows {
        for val in row {
            assert_ne!(
                *val,
                Value::Text("remove-me".to_string()),
                "dropped column data must not appear"
            );
        }
    }
}

#[test]
fn at04_drop_column_remaining_data_intact() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, name, age) VALUES (2, 'bob', 25)",
        &empty_params(),
    )
    .unwrap();

    db.execute("ALTER TABLE t DROP COLUMN age", &empty_params())
        .unwrap();

    let result_all = db.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result_all, "age").is_none(),
        "'age' must not appear after DROP COLUMN"
    );

    let result = db
        .execute("SELECT name FROM t WHERE id = 1", &empty_params())
        .unwrap();
    let idx = col_index(&result, "name").expect("'name' column must exist");
    assert_eq!(result.rows[0][idx], Value::Text("alice".to_string()));

    let result2 = db
        .execute("SELECT name FROM t WHERE id = 2", &empty_params())
        .unwrap();
    assert_eq!(
        result2.rows[0][col_index(&result2, "name").unwrap()],
        Value::Text("bob".to_string())
    );
}

#[test]
fn at05_drop_primary_key_errors() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .unwrap();

    let result = db.execute("ALTER TABLE t DROP COLUMN id", &empty_params());
    assert!(result.is_err(), "dropping primary key must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("primary key"),
        "error must mention 'primary key', got: {err}"
    );
}

#[test]
fn at06_rename_column_new_name_returns_data() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, temp REAL)",
        &empty_params(),
    )
    .unwrap();
    db.execute("INSERT INTO t (id, temp) VALUES (1, 98.6)", &empty_params())
        .unwrap();

    db.execute(
        "ALTER TABLE t RENAME COLUMN temp TO temperature",
        &empty_params(),
    )
    .unwrap();

    let result = db
        .execute("SELECT temperature FROM t WHERE id = 1", &empty_params())
        .unwrap();
    let idx = col_index(&result, "temperature").expect("'temperature' must exist after RENAME");
    assert_eq!(
        result.rows[0][idx],
        Value::Float64(98.6),
        "renamed column must retain data"
    );
}

#[test]
fn at07_rename_column_old_name_gone() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, old_name) VALUES (1, 'data')",
        &empty_params(),
    )
    .unwrap();

    db.execute(
        "ALTER TABLE t RENAME COLUMN old_name TO new_name",
        &empty_params(),
    )
    .unwrap();

    let result = db.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result, "old_name").is_none(),
        "'old_name' must not appear after RENAME"
    );
    let idx = col_index(&result, "new_name").expect("'new_name' must exist");
    assert_eq!(result.rows[0][idx], Value::Text("data".to_string()));
}

#[test]
fn at08_alter_nonexistent_table_errors() {
    let db = Database::open_memory();
    let result = db.execute("ALTER TABLE nonexistent ADD COLUMN x TEXT", &empty_params());
    assert!(result.is_err(), "ALTER on nonexistent table must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "error must say 'not found', got: {err}"
    );
    assert!(
        err.contains("nonexistent"),
        "error must name the table 'nonexistent', got: {err}"
    );
}

#[test]
fn at09_add_existing_column_errors() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .unwrap();

    let result = db.execute("ALTER TABLE t ADD COLUMN name TEXT", &empty_params());
    assert!(result.is_err(), "adding existing column must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("already exists"),
        "error must say 'already exists', got: {err}"
    );
    assert!(
        err.contains("name"),
        "error must name the column 'name', got: {err}"
    );
}

#[test]
fn at10_drop_nonexistent_column_errors() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
        .unwrap();

    let result = db.execute("ALTER TABLE t DROP COLUMN ghost", &empty_params());
    assert!(result.is_err(), "dropping nonexistent column must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("does not exist"),
        "error must say 'does not exist', got: {err}"
    );
    assert!(
        err.contains("ghost"),
        "error must name the column 'ghost', got: {err}"
    );
}

#[test]
fn at11_rename_nonexistent_column_errors() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
        .unwrap();

    let result = db.execute(
        "ALTER TABLE t RENAME COLUMN ghost TO new_ghost",
        &empty_params(),
    );
    assert!(result.is_err(), "renaming nonexistent column must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("does not exist"),
        "error must say 'does not exist', got: {err}"
    );
    assert!(
        err.contains("ghost"),
        "error must name the column 'ghost', got: {err}"
    );
}

#[test]
fn at12_rename_to_existing_name_errors() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
        &empty_params(),
    )
    .unwrap();

    let result = db.execute("ALTER TABLE t RENAME COLUMN a TO b", &empty_params());
    assert!(result.is_err(), "renaming to existing column must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("already exists"),
        "error must say 'already exists', got: {err}"
    );
    assert!(
        err.contains("b"),
        "error must name the target column 'b', got: {err}"
    );
}

#[test]
fn at13_add_column_persists_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("at13.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
            .unwrap();
        db.execute("INSERT INTO t (id) VALUES (1)", &empty_params())
            .unwrap();
        db.execute("ALTER TABLE t ADD COLUMN note TEXT", &empty_params())
            .unwrap();
        db.close().unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let result = db2.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result, "note").is_some(),
        "'note' must persist after reopen"
    );
    let idx = col_index(&result, "note").unwrap();
    assert_eq!(
        result.rows[0][idx],
        Value::Null,
        "old row must have NULL for added column after reopen"
    );
}

#[test]
fn at14_drop_column_persists_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("at14.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, extra TEXT)",
            &empty_params(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO t (id, extra) VALUES (1, 'gone')",
            &empty_params(),
        )
        .unwrap();
        db.execute("ALTER TABLE t DROP COLUMN extra", &empty_params())
            .unwrap();
        db.close().unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let result = db2.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result, "extra").is_none(),
        "'extra' must stay dropped after reopen"
    );
    for row in &result.rows {
        for val in row {
            assert_ne!(
                *val,
                Value::Text("gone".to_string()),
                "dropped column data must not reappear"
            );
        }
    }
}

#[test]
fn at15_rename_column_persists_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("at15.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, old TEXT)",
            &empty_params(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO t (id, old) VALUES (1, 'kept')",
            &empty_params(),
        )
        .unwrap();
        db.execute("ALTER TABLE t RENAME COLUMN old TO new", &empty_params())
            .unwrap();
        db.close().unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let result = db2.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result, "old").is_none(),
        "'old' must not reappear after reopen"
    );
    let idx = col_index(&result, "new").expect("'new' must persist after reopen");
    assert_eq!(
        result.rows[0][idx],
        Value::Text("kept".to_string()),
        "data must survive rename + reopen"
    );
}

#[test]
fn at16_multiple_alters_in_sequence() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, a, b) VALUES (1, 'alpha', 'beta')",
        &empty_params(),
    )
    .unwrap();

    db.execute("ALTER TABLE t ADD COLUMN c REAL", &empty_params())
        .unwrap();
    db.execute("ALTER TABLE t RENAME COLUMN a TO first", &empty_params())
        .unwrap();
    db.execute("ALTER TABLE t DROP COLUMN b", &empty_params())
        .unwrap();

    let result = db.execute("SELECT * FROM t", &empty_params()).unwrap();
    assert!(
        col_index(&result, "first").is_some(),
        "'first' must exist (renamed from 'a')"
    );
    assert!(col_index(&result, "c").is_some(), "'c' must exist (added)");
    assert!(
        col_index(&result, "a").is_none(),
        "'a' must be gone (renamed)"
    );
    assert!(
        col_index(&result, "b").is_none(),
        "'b' must be gone (dropped)"
    );

    let first_idx = col_index(&result, "first").unwrap();
    let c_idx = col_index(&result, "c").unwrap();
    assert_eq!(result.rows[0][first_idx], Value::Text("alpha".to_string()));
    assert_eq!(result.rows[0][c_idx], Value::Null);
    for val in &result.rows[0] {
        assert_ne!(
            *val,
            Value::Text("beta".to_string()),
            "dropped column data 'beta' must not appear"
        );
    }
}

#[test]
fn at17_add_without_column_keyword() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
        .unwrap();

    db.execute("ALTER TABLE t ADD score REAL", &empty_params())
        .unwrap();

    db.execute(
        "INSERT INTO t (id, score) VALUES (1, 42.0)",
        &empty_params(),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM t WHERE id = 1", &empty_params())
        .unwrap();
    let idx =
        col_index(&result, "score").expect("'score' must exist after ADD without COLUMN keyword");
    assert_eq!(
        result.rows[0][idx],
        Value::Float64(42.0),
        "column added without COLUMN keyword must be usable"
    );
}

#[test]
fn at18_explain_alter_table() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
        .unwrap();

    let output = db.explain("ALTER TABLE t ADD COLUMN x TEXT").unwrap();
    assert!(
        output.contains("AlterTable"),
        "EXPLAIN must show AlterTable plan, got: {output}"
    );
}

#[test]
fn at19_add_column_non_text_types_preserved() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
        .unwrap();

    db.execute("ALTER TABLE t ADD COLUMN flag BOOLEAN", &empty_params())
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN ref_id UUID", &empty_params())
        .unwrap();

    let test_uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let params = HashMap::from([
        ("id".to_string(), Value::Int64(1)),
        ("flag".to_string(), Value::Bool(true)),
        ("ref_id".to_string(), Value::Uuid(test_uuid)),
    ]);
    db.execute(
        "INSERT INTO t (id, flag, ref_id) VALUES ($id, $flag, $ref_id)",
        &params,
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM t WHERE id = 1", &empty_params())
        .unwrap();
    let flag_idx = col_index(&result, "flag").expect("'flag' column must exist");
    let ref_idx = col_index(&result, "ref_id").expect("'ref_id' column must exist");

    assert_eq!(
        result.rows[0][flag_idx],
        Value::Bool(true),
        "BOOLEAN type must be preserved, not stored as TEXT"
    );
    match &result.rows[0][ref_idx] {
        Value::Uuid(u) => assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000"),
        other => panic!("UUID type must be preserved, got: {other:?}"),
    }
}

#[test]
fn at20_column_type_survives_persistence() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("at20.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &empty_params())
            .unwrap();
        db.execute("ALTER TABLE t ADD COLUMN score REAL", &empty_params())
            .unwrap();
        db.execute(
            "INSERT INTO t (id, score) VALUES (1, 2.718)",
            &empty_params(),
        )
        .unwrap();
        db.close().unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let result = db2
        .execute("SELECT * FROM t WHERE id = 1", &empty_params())
        .unwrap();
    let idx = col_index(&result, "score").expect("'score' must persist after reopen");
    assert_eq!(
        result.rows[0][idx],
        Value::Float64(2718_f64 / 1000.0),
        "column type must survive persistence - value must be Float64, not Text"
    );
}
