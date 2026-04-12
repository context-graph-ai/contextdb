use contextdb_core::{MemoryAccountant, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn disk_limit_kib_for_path(path: &std::path::Path, extra_kib: u64) -> u64 {
    let bytes = std::fs::metadata(path).expect("metadata").len();
    bytes.div_ceil(1024) + extra_kib
}

// ============================================================
// Group 1: Comparison Operators
// ============================================================

#[test]
fn cmp_01_less_than_integer() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, score INTEGER)",
        &empty(),
    )
    .unwrap();
    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    for (id, score) in ids.iter().zip(&[10i64, 20, 30]) {
        db.execute(
            "INSERT INTO items (id, score) VALUES ($id, $score)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("score", Value::Int64(*score)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM items WHERE score < 20", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 1, "expected 1 row with score < 20");
    let score_idx = result.columns.iter().position(|c| c == "score").unwrap();
    assert_eq!(result.rows[0][score_idx], Value::Int64(10));
}

#[test]
fn cmp_02_gte_float() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE readings (id UUID PRIMARY KEY, value REAL)",
        &empty(),
    )
    .unwrap();
    for val in &[1.5f64, 2.5, 3.5] {
        db.execute(
            "INSERT INTO readings (id, value) VALUES ($id, $value)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("value", Value::Float64(*val)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM readings WHERE value >= 2.5", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2, "expected 2 rows with value >= 2.5");
    let val_idx = result.columns.iter().position(|c| c == "value").unwrap();
    let values: Vec<&Value> = result.rows.iter().map(|r| &r[val_idx]).collect();
    assert!(values.contains(&&Value::Float64(2.5)));
    assert!(values.contains(&&Value::Float64(3.5)));
}

#[test]
fn cmp_03_gt_text_lexicographic() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE words (id UUID PRIMARY KEY, word TEXT)",
        &empty(),
    )
    .unwrap();
    for w in &["apple", "banana", "cherry"] {
        db.execute(
            "INSERT INTO words (id, word) VALUES ($id, $word)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("word", Value::Text(w.to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM words WHERE word > 'banana'", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let word_idx = result.columns.iter().position(|c| c == "word").unwrap();
    assert_eq!(result.rows[0][word_idx], Value::Text("cherry".to_string()));
}

#[test]
fn cmp_04_cross_type_int_vs_float() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE mixed (id UUID PRIMARY KEY, val INTEGER)",
        &empty(),
    )
    .unwrap();
    for v in &[2i64, 3, 4] {
        db.execute(
            "INSERT INTO mixed (id, val) VALUES ($id, $val)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("val", Value::Int64(*v)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM mixed WHERE val > 2.5", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2, "expected val=3 and val=4");
    let val_idx = result.columns.iter().position(|c| c == "val").unwrap();
    let values: Vec<&Value> = result.rows.iter().map(|r| &r[val_idx]).collect();
    assert!(values.contains(&&Value::Int64(3)));
    assert!(values.contains(&&Value::Int64(4)));
}

#[test]
fn cmp_05_timestamp_vs_int() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE events (id UUID PRIMARY KEY, ts TIMESTAMP)",
        &empty(),
    )
    .unwrap();
    for ts in &[1000i64, 2000, 3000] {
        db.execute(
            "INSERT INTO events (id, ts) VALUES ($id, $ts)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("ts", Value::Timestamp(*ts)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM events WHERE ts >= 2000", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2, "expected ts=2000 and ts=3000");
    let ts_idx = result.columns.iter().position(|c| c == "ts").unwrap();
    let ts_values: Vec<_> = result.rows.iter().map(|r| r[ts_idx].clone()).collect();
    assert!(ts_values.contains(&Value::Timestamp(2000)));
    assert!(ts_values.contains(&Value::Timestamp(3000)));
    assert!(!ts_values.contains(&Value::Timestamp(1000)));
}

#[test]
fn cmp_06_null_eq_null_is_false() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullable (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullable (id, val) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullable (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("hello".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM nullable WHERE val = NULL", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 0, "NULL = NULL must be false in SQL");
}

#[test]
fn cmp_07_neq_null_is_false() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullable2 (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    for v in &[None, Some("hello"), Some("world")] {
        let mut p = vec![("id", Value::Uuid(Uuid::new_v4()))];
        if let Some(s) = v {
            p.push(("val", Value::Text(s.to_string())));
        }
        let sql = if v.is_some() {
            "INSERT INTO nullable2 (id, val) VALUES ($id, $val)"
        } else {
            "INSERT INTO nullable2 (id, val) VALUES ($id, NULL)"
        };
        db.execute(sql, &params(p)).unwrap();
    }

    let result = db
        .execute("SELECT * FROM nullable2 WHERE val <> NULL", &empty())
        .unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "col <> NULL must be false for all rows in SQL"
    );
}

// ============================================================
// Group 2: Logical Operators
// ============================================================

fn setup_products_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE products (id UUID PRIMARY KEY, price INTEGER, category TEXT)",
        &empty(),
    )
    .unwrap();
    let data = vec![(10i64, "food"), (20, "food"), (10, "drink"), (30, "drink")];
    for (price, cat) in data {
        db.execute(
            "INSERT INTO products (id, price, category) VALUES ($id, $price, $category)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("price", Value::Int64(price)),
                ("category", Value::Text(cat.into())),
            ]),
        )
        .unwrap();
    }
    db
}

#[test]
fn log_01_and_combines_filters() {
    let db = setup_products_db();
    let result = db
        .execute(
            "SELECT * FROM products WHERE price <= 10 AND category = 'food'",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let price_idx = result.columns.iter().position(|c| c == "price").unwrap();
    let cat_idx = result.columns.iter().position(|c| c == "category").unwrap();
    assert_eq!(result.rows[0][price_idx], Value::Int64(10));
    assert_eq!(result.rows[0][cat_idx], Value::Text("food".into()));
}

#[test]
fn log_02_or_matches_either() {
    let db = setup_products_db();
    let result = db
        .execute(
            "SELECT * FROM products WHERE price = 30 OR category = 'food'",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3, "food(10), food(20), drink(30)");
    let price_idx = result.columns.iter().position(|c| c == "price").unwrap();
    let cat_idx = result.columns.iter().position(|c| c == "category").unwrap();
    let rows: Vec<(Value, Value)> = result
        .rows
        .iter()
        .map(|r| (r[price_idx].clone(), r[cat_idx].clone()))
        .collect();
    assert!(rows.contains(&(Value::Int64(10), Value::Text("food".into()))));
    assert!(rows.contains(&(Value::Int64(20), Value::Text("food".into()))));
    assert!(rows.contains(&(Value::Int64(30), Value::Text("drink".into()))));
    // drink(10) must NOT be present
    assert!(!rows.contains(&(Value::Int64(10), Value::Text("drink".into()))));
}

#[test]
fn log_03_not_negates() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE flags (id UUID PRIMARY KEY, active BOOLEAN)",
        &empty(),
    )
    .unwrap();
    for b in &[true, false, true] {
        db.execute(
            "INSERT INTO flags (id, active) VALUES ($id, $active)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("active", Value::Bool(*b)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM flags WHERE NOT active = true", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let active_idx = result.columns.iter().position(|c| c == "active").unwrap();
    assert_eq!(result.rows[0][active_idx], Value::Bool(false));
}

#[test]
fn log_04_null_propagation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullcheck (id UUID PRIMARY KEY, a TEXT, b TEXT)",
        &empty(),
    )
    .unwrap();
    // id1: a=NULL, b='x'
    db.execute(
        "INSERT INTO nullcheck (id, a, b) VALUES ($id, NULL, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("b", Value::Text("x".into())),
        ]),
    )
    .unwrap();
    // id2: a='y', b='x'
    db.execute(
        "INSERT INTO nullcheck (id, a, b) VALUES ($id, $a, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Text("y".into())),
            ("b", Value::Text("x".into())),
        ]),
    )
    .unwrap();
    // id3: a=NULL, b=NULL
    db.execute(
        "INSERT INTO nullcheck (id, a, b) VALUES ($id, NULL, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();

    // false AND anything = false
    let r1 = db
        .execute(
            "SELECT * FROM nullcheck WHERE a = 'missing' AND b = 'x'",
            &empty(),
        )
        .unwrap();
    assert_eq!(r1.rows.len(), 0, "false AND anything = false");

    // OR: a='y' matches id2; b='x' matches id1,id2
    let r2 = db
        .execute("SELECT * FROM nullcheck WHERE a = 'y' OR b = 'x'", &empty())
        .unwrap();
    assert_eq!(r2.rows.len(), 2, "id1 and id2 match via OR");
}

// ============================================================
// Group 3: Expression Operators
// ============================================================

fn setup_colors_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE colors (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    for c in &["red", "green", "blue", "yellow"] {
        db.execute(
            "INSERT INTO colors (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text(c.to_string())),
            ]),
        )
        .unwrap();
    }
    db
}

#[test]
fn expr_01_in_list() {
    let db = setup_colors_db();
    let result = db
        .execute(
            "SELECT * FROM colors WHERE name IN ('red', 'blue')",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[name_idx]).collect();
    assert!(names.contains(&&Value::Text("red".into())));
    assert!(names.contains(&&Value::Text("blue".into())));
}

#[test]
fn expr_02_not_in() {
    let db = setup_colors_db();
    let result = db
        .execute(
            "SELECT * FROM colors WHERE name NOT IN ('red', 'blue')",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[name_idx]).collect();
    assert!(names.contains(&&Value::Text("green".into())));
    assert!(names.contains(&&Value::Text("yellow".into())));
}

#[test]
fn expr_03_in_subquery() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE departments (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE employees (id UUID PRIMARY KEY, dept TEXT, name TEXT)",
        &empty(),
    )
    .unwrap();
    for dept in &["engineering", "sales"] {
        db.execute(
            "INSERT INTO departments (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text(dept.to_string())),
            ]),
        )
        .unwrap();
    }
    let emp_data = vec![
        ("engineering", "alice"),
        ("marketing", "bob"),
        ("sales", "carol"),
    ];
    for (dept, name) in emp_data {
        db.execute(
            "INSERT INTO employees (id, dept, name) VALUES ($id, $dept, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("dept", Value::Text(dept.into())),
                ("name", Value::Text(name.into())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute(
            "SELECT * FROM employees WHERE dept IN (SELECT name FROM departments)",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[name_idx]).collect();
    assert!(names.contains(&&Value::Text("alice".into())));
    assert!(names.contains(&&Value::Text("carol".into())));
}

#[test]
fn expr_04_like_percent() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE files (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    for f in &["report.pdf", "report.docx", "invoice.pdf", "notes.txt"] {
        db.execute(
            "INSERT INTO files (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text(f.to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM files WHERE name LIKE 'report%'", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[name_idx]).collect();
    assert!(names.contains(&&Value::Text("report.pdf".into())));
    assert!(names.contains(&&Value::Text("report.docx".into())));
}

#[test]
fn expr_05_like_underscore() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE codes (id UUID PRIMARY KEY, code TEXT)",
        &empty(),
    )
    .unwrap();
    for c in &["cat", "bat", "at", "cart"] {
        db.execute(
            "INSERT INTO codes (id, code) VALUES ($id, $code)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("code", Value::Text(c.to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM codes WHERE code LIKE '_at'", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let code_idx = result.columns.iter().position(|c| c == "code").unwrap();
    let codes: Vec<&Value> = result.rows.iter().map(|r| &r[code_idx]).collect();
    assert!(codes.contains(&&Value::Text("cat".into())));
    assert!(codes.contains(&&Value::Text("bat".into())));
}

#[test]
fn expr_06_not_like() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE files2 (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    for f in &["report.pdf", "report.docx", "invoice.pdf", "notes.txt"] {
        db.execute(
            "INSERT INTO files2 (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text(f.to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM files2 WHERE name NOT LIKE '%.pdf'", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[name_idx]).collect();
    assert!(names.contains(&&Value::Text("report.docx".into())));
    assert!(names.contains(&&Value::Text("notes.txt".into())));
}

#[test]
fn expr_07_between_inclusive() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE temps (id UUID PRIMARY KEY, celsius INTEGER)",
        &empty(),
    )
    .unwrap();
    for c in &[10i64, 20, 25, 30, 40] {
        db.execute(
            "INSERT INTO temps (id, celsius) VALUES ($id, $c)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("c", Value::Int64(*c)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute(
            "SELECT * FROM temps WHERE celsius BETWEEN 20 AND 30",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3, "20, 25, 30 are in range");
    let c_idx = result.columns.iter().position(|c| c == "celsius").unwrap();
    let vals: Vec<&Value> = result.rows.iter().map(|r| &r[c_idx]).collect();
    assert!(vals.contains(&&Value::Int64(20)));
    assert!(vals.contains(&&Value::Int64(25)));
    assert!(vals.contains(&&Value::Int64(30)));
}

#[test]
fn expr_08_is_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE optional (id UUID PRIMARY KEY, note TEXT)",
        &empty(),
    )
    .unwrap();
    let null_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO optional (id, note) VALUES ($id, $note)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("note", Value::Text("hello".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO optional (id, note) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(null_id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO optional (id, note) VALUES ($id, $note)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("note", Value::Text("world".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM optional WHERE note IS NULL", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let id_idx = result.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(result.rows[0][id_idx], Value::Uuid(null_id));
}

#[test]
fn expr_09_is_not_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE optional2 (id UUID PRIMARY KEY, note TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO optional2 (id, note) VALUES ($id, $note)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("note", Value::Text("hello".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO optional2 (id, note) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO optional2 (id, note) VALUES ($id, $note)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("note", Value::Text("world".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM optional2 WHERE note IS NOT NULL", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let note_idx = result.columns.iter().position(|c| c == "note").unwrap();
    let notes: Vec<&Value> = result.rows.iter().map(|r| &r[note_idx]).collect();
    assert!(notes.contains(&&Value::Text("hello".into())));
    assert!(notes.contains(&&Value::Text("world".into())));
}

// ============================================================
// Group 4: Aggregation
// ============================================================

#[test]
fn agg_01_count_star() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE counttable (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    for v in &["a", "b", "c"] {
        db.execute(
            "INSERT INTO counttable (id, val) VALUES ($id, $val)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("val", Value::Text(v.to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT COUNT(*) FROM counttable", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 1, "aggregate returns single row");
    assert_eq!(result.rows[0][0], Value::Int64(3));
    assert!(
        result.columns.iter().any(|c| c == "COUNT"),
        "column name must preserve SQL case"
    );
}

#[test]
fn agg_02_count_expr_excludes_nulls() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullcount (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcount (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("a".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcount (id, val) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcount (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("c".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcount (id, val) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();

    let result = db
        .execute("SELECT COUNT(val) FROM nullcount", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::Int64(2),
        "COUNT(val) excludes NULLs"
    );
}

#[test]
fn agg_03_mixed_aggregate_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE mixagg (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO mixagg (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("a".into())),
        ]),
    )
    .unwrap();

    let result = db.execute("SELECT COUNT(*), val FROM mixagg", &empty());
    assert!(
        result.is_err(),
        "mixed aggregate + bare column without GROUP BY must error"
    );
}

// ============================================================
// Group 5: Functions
// ============================================================

#[test]
fn fn_01_coalesce_first_non_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE coaltable (id UUID PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
        &empty(),
    )
    .unwrap();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    db.execute(
        "INSERT INTO coaltable (id, a, b, c) VALUES ($id, NULL, NULL, $c)",
        &params(vec![
            ("id", Value::Uuid(id1)),
            ("c", Value::Text("fallback".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO coaltable (id, a, b, c) VALUES ($id, NULL, $b, $c)",
        &params(vec![
            ("id", Value::Uuid(id2)),
            ("b", Value::Text("second".into())),
            ("c", Value::Text("third".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT COALESCE(a, b, c) FROM coaltable ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    // We check both rows have the correct first-non-null value.
    // Order depends on UUID, so we collect and check set membership.
    let values: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(values.contains(&&Value::Text("fallback".into())));
    assert!(values.contains(&&Value::Text("second".into())));
}

#[test]
fn fn_02_now_without_from() {
    let db = Database::open_memory();
    let result = db.execute("SELECT NOW()", &empty()).unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Timestamp(ts) => {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(
                (now_secs - ts).abs() < 5,
                "NOW() should be within 5s of system time"
            );
        }
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

#[test]
fn fn_03_now_with_from() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE dummy (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO dummy (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();

    let result = db.execute("SELECT NOW() FROM dummy", &empty()).unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Timestamp(ts) => {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(
                (now_secs - ts).abs() < 5,
                "NOW() should be within 5s of system time"
            );
        }
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

// ============================================================
// Group 5b: SQL Correctness Regression Cases
// ============================================================

#[test]
fn sql_01_default_now_produces_timestamp() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE events (id UUID PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW())",
        &empty(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO events (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT created_at FROM events WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Timestamp(ts) => assert!(
            *ts > 1_700_000_000,
            "created_at should be a recent unix timestamp, got {ts}"
        ),
        other => panic!("expected TIMESTAMP from DEFAULT NOW(), got {:?}", other),
    }
}

#[test]
fn sql_02_edge_insert_routes_to_graph_once() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    for _ in 0..2 {
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("source_id", Value::Uuid(source)),
                ("target_id", Value::Uuid(target)),
                ("edge_type", Value::Text("RELATES_TO".to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute(
            "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:RELATES_TO]->(b) WHERE a.id = $source_id COLUMNS (b.id AS b_id))",
            &params(vec![("source_id", Value::Uuid(source))]),
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "graph traversal should see exactly one reachable target after duplicate SQL edge inserts"
    );
    assert_eq!(result.rows[0][0], Value::Uuid(target));
}

#[test]
fn sql_03_group_by_clean_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, tag TEXT)",
        &empty(),
    )
    .unwrap();

    let err = db
        .execute("SELECT tag, COUNT(*) FROM items GROUP BY tag", &empty())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("GROUP BY") && err.contains("not supported"),
        "expected a clean GROUP BY not-supported error, got: {err}"
    );
}

#[test]
fn sql_04_join_ambiguous_column_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE left_t (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE right_t (id UUID PRIMARY KEY, name TEXT, value INT)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO left_t (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("left".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO right_t (id, name, value) VALUES ($id, $name, $value)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("right".into())),
            ("value", Value::Int64(1)),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "SELECT name FROM left_t INNER JOIN right_t ON left_t.id = right_t.id",
            &empty(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("ambiguous"),
        "expected ambiguous column error, got: {err}"
    );
}

#[test]
fn sql_05_scenario3_multi_hop_cascade() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived, paused, superseded])",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, embedding VECTOR(128)) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &empty(),
    )
    .unwrap();

    let intention_id = Uuid::new_v4();
    let decision_a = Uuid::new_v4();
    let decision_b = Uuid::new_v4();

    db.execute(
        "INSERT INTO intentions (id, description, status) VALUES ($id, $description, $status)",
        &params(vec![
            ("id", Value::Uuid(intention_id)),
            ("description", Value::Text("root".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO decisions (id, description, status, intention_id) VALUES ($id, $description, $status, $intention_id)",
        &params(vec![
            ("id", Value::Uuid(decision_a)),
            ("description", Value::Text("a".into())),
            ("status", Value::Text("active".into())),
            ("intention_id", Value::Uuid(intention_id)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO decisions (id, description, status, intention_id) VALUES ($id, $description, $status, $intention_id)",
        &params(vec![
            ("id", Value::Uuid(decision_b)),
            ("description", Value::Text("b".into())),
            ("status", Value::Text("active".into())),
            ("intention_id", Value::Uuid(intention_id)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(decision_b)),
            ("target_id", Value::Uuid(decision_a)),
            ("edge_type", Value::Text("CITES".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "UPDATE intentions SET status = 'archived' WHERE id = $id",
        &params(vec![("id", Value::Uuid(intention_id))]),
    )
    .unwrap();

    let decision_a_row = db
        .execute(
            "SELECT status FROM decisions WHERE id = $id",
            &params(vec![("id", Value::Uuid(decision_a))]),
        )
        .unwrap();
    let decision_b_row = db
        .execute(
            "SELECT status FROM decisions WHERE id = $id",
            &params(vec![("id", Value::Uuid(decision_b))]),
        )
        .unwrap();
    assert_eq!(decision_a_row.rows[0][0], Value::Text("invalidated".into()));
    assert_eq!(decision_b_row.rows[0][0], Value::Text("invalidated".into()));
}

#[test]
fn sql_06_uuid_column_validation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    let result = db.execute("INSERT INTO items VALUES ('not-a-uuid', 'test')", &empty());
    assert!(
        result.is_err(),
        "invalid UUID literal should be rejected, but insert succeeded"
    );
}

#[test]
fn sql_07_join_double_qualification() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE employees (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE orders (id UUID PRIMARY KEY, name TEXT, employee_id UUID)",
        &empty(),
    )
    .unwrap();

    let employee_id = Uuid::new_v4();
    let order_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO employees (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(employee_id)),
            ("name", Value::Text("employee".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO orders (id, name, employee_id) VALUES ($id, $name, $employee_id)",
        &params(vec![
            ("id", Value::Uuid(order_id)),
            ("name", Value::Text("order".into())),
            ("employee_id", Value::Uuid(employee_id)),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT e.name, o.name FROM employees e INNER JOIN orders o ON e.id = o.employee_id",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("employee".into()));
    assert_eq!(result.rows[0][1], Value::Text("order".into()));
}

#[test]
fn sql_08_distinct_not_quadratic() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, tag TEXT)",
        &empty(),
    )
    .unwrap();

    for i in 0..5_000 {
        let tag = format!("tag-{}", i % 100);
        db.execute(
            "INSERT INTO items (id, tag) VALUES ($id, $tag)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("tag", Value::Text(tag)),
            ]),
        )
        .unwrap();
    }

    let started = Instant::now();
    let result = db
        .execute("SELECT DISTINCT tag FROM items", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 100);
    assert!(
        started.elapsed().as_secs_f32() < 5.0,
        "SELECT DISTINCT should finish within 5 seconds on 5k rows"
    );
}

// ============================================================
// Group 6: Query Shaping
// ============================================================

#[test]
fn shp_01_distinct_removes_duplicates() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE tags (id UUID PRIMARY KEY, tag TEXT)",
        &empty(),
    )
    .unwrap();
    for t in &["rust", "python", "rust", "go", "python"] {
        db.execute(
            "INSERT INTO tags (id, tag) VALUES ($id, $tag)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("tag", Value::Text(t.to_string())),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT DISTINCT tag FROM tags", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 3, "3 distinct tags: rust, python, go");
    let tags: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(tags.contains(&&Value::Text("rust".into())));
    assert!(tags.contains(&&Value::Text("python".into())));
    assert!(tags.contains(&&Value::Text("go".into())));
}

#[test]
fn shp_02_column_alias() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO people (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("alice".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute("SELECT name AS person_name FROM people", &empty())
        .unwrap();
    assert!(
        result.columns.contains(&"person_name".to_string()),
        "output column should be aliased"
    );
    assert!(
        !result.columns.contains(&"name".to_string()),
        "original name should not appear"
    );
    assert_eq!(result.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn shp_03_order_by_asc() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sorted (id UUID PRIMARY KEY, num INTEGER)",
        &empty(),
    )
    .unwrap();
    for n in &[30i64, 10, 20] {
        db.execute(
            "INSERT INTO sorted (id, num) VALUES ($id, $num)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("num", Value::Int64(*n)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM sorted ORDER BY num ASC", &empty())
        .unwrap();
    let num_idx = result.columns.iter().position(|c| c == "num").unwrap();
    assert_eq!(result.rows[0][num_idx], Value::Int64(10));
    assert_eq!(result.rows[1][num_idx], Value::Int64(20));
    assert_eq!(result.rows[2][num_idx], Value::Int64(30));
}

#[test]
fn shp_04_order_by_multi_column_mixed() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE ranked (id UUID PRIMARY KEY, group_name TEXT, score INTEGER)",
        &empty(),
    )
    .unwrap();
    let data = vec![("alpha", 10i64), ("alpha", 20), ("beta", 10), ("beta", 20)];
    for (g, s) in data {
        db.execute(
            "INSERT INTO ranked (id, group_name, score) VALUES ($id, $g, $s)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("g", Value::Text(g.into())),
                ("s", Value::Int64(s)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute(
            "SELECT * FROM ranked ORDER BY group_name ASC, score DESC",
            &empty(),
        )
        .unwrap();
    let g_idx = result
        .columns
        .iter()
        .position(|c| c == "group_name")
        .unwrap();
    let s_idx = result.columns.iter().position(|c| c == "score").unwrap();
    assert_eq!(result.rows[0][g_idx], Value::Text("alpha".into()));
    assert_eq!(result.rows[0][s_idx], Value::Int64(20));
    assert_eq!(result.rows[1][g_idx], Value::Text("alpha".into()));
    assert_eq!(result.rows[1][s_idx], Value::Int64(10));
    assert_eq!(result.rows[2][g_idx], Value::Text("beta".into()));
    assert_eq!(result.rows[2][s_idx], Value::Int64(20));
    assert_eq!(result.rows[3][g_idx], Value::Text("beta".into()));
    assert_eq!(result.rows[3][s_idx], Value::Int64(10));
}

#[test]
fn shp_05_order_by_asc_nulls_last() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullsort (id UUID PRIMARY KEY, val INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullsort (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Int64(3)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullsort (id, val) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullsort (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Int64(1)),
        ]),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM nullsort ORDER BY val ASC", &empty())
        .unwrap();
    let val_idx = result.columns.iter().position(|c| c == "val").unwrap();
    assert_eq!(result.rows[0][val_idx], Value::Int64(1));
    assert_eq!(result.rows[1][val_idx], Value::Int64(3));
    assert_eq!(result.rows[2][val_idx], Value::Null);
}

#[test]
fn shp_06_order_by_desc_nulls_first() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullsort2 (id UUID PRIMARY KEY, val INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullsort2 (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Int64(3)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullsort2 (id, val) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullsort2 (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Int64(1)),
        ]),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM nullsort2 ORDER BY val DESC", &empty())
        .unwrap();
    let val_idx = result.columns.iter().position(|c| c == "val").unwrap();
    assert_eq!(result.rows[0][val_idx], Value::Null);
    assert_eq!(result.rows[1][val_idx], Value::Int64(3));
    assert_eq!(result.rows[2][val_idx], Value::Int64(1));
}

#[test]
fn shp_07_limit() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE big (id UUID PRIMARY KEY, num INTEGER)",
        &empty(),
    )
    .unwrap();
    for n in 1..=5i64 {
        db.execute(
            "INSERT INTO big (id, num) VALUES ($id, $num)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("num", Value::Int64(n)),
            ]),
        )
        .unwrap();
    }

    let result = db
        .execute("SELECT * FROM big ORDER BY num ASC LIMIT 3", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    let num_idx = result.columns.iter().position(|c| c == "num").unwrap();
    assert_eq!(result.rows[0][num_idx], Value::Int64(1));
    assert_eq!(result.rows[1][num_idx], Value::Int64(2));
    assert_eq!(result.rows[2][num_idx], Value::Int64(3));
}

// ============================================================
// Group 7: JOIN
// ============================================================

fn setup_authors_books_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE authors (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE books (id UUID PRIMARY KEY, title TEXT, author_id UUID)",
        &empty(),
    )
    .unwrap();
    db
}

#[test]
fn jn_01_inner_join() {
    let db = setup_authors_books_db();
    let aid1 = Uuid::new_v4();
    let aid2 = Uuid::new_v4();
    db.execute(
        "INSERT INTO authors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(aid1)),
            ("name", Value::Text("alice".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO authors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(aid2)),
            ("name", Value::Text("bob".into())),
        ]),
    )
    .unwrap();
    // Two books by alice, one by nonexistent author
    db.execute(
        "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("title", Value::Text("book_a".into())),
            ("aid", Value::Uuid(aid1)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("title", Value::Text("book_b".into())),
            ("aid", Value::Uuid(aid1)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("title", Value::Text("book_c".into())),
            ("aid", Value::Uuid(Uuid::new_v4())), // nonexistent author
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT a.name, b.title FROM authors a INNER JOIN books b ON a.id = b.author_id",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2, "only alice's 2 books match");
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let title_idx = result.columns.iter().position(|c| c == "title").unwrap();
    for row in &result.rows {
        assert_eq!(row[name_idx], Value::Text("alice".into()));
    }
    let titles: Vec<&Value> = result.rows.iter().map(|r| &r[title_idx]).collect();
    assert!(titles.contains(&&Value::Text("book_a".into())));
    assert!(titles.contains(&&Value::Text("book_b".into())));
}

#[test]
fn jn_02_left_join_unmatched_nulls() {
    let db = setup_authors_books_db();
    let aid1 = Uuid::new_v4();
    let aid2 = Uuid::new_v4();
    db.execute(
        "INSERT INTO authors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(aid1)),
            ("name", Value::Text("alice".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO authors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(aid2)),
            ("name", Value::Text("bob".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("title", Value::Text("book_a".into())),
            ("aid", Value::Uuid(aid1)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("title", Value::Text("book_b".into())),
            ("aid", Value::Uuid(aid1)),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT a.name, b.title FROM authors a LEFT JOIN books b ON a.id = b.author_id",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        3,
        "alice's 2 books + bob with NULL title"
    );
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let title_idx = result.columns.iter().position(|c| c == "title").unwrap();
    // Find bob's row
    let bob_rows: Vec<_> = result
        .rows
        .iter()
        .filter(|r| r[name_idx] == Value::Text("bob".into()))
        .collect();
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(
        bob_rows[0][title_idx],
        Value::Null,
        "unmatched left row has NULL for right columns"
    );
}

#[test]
fn jn_03_disambiguated_columns() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE left_t (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE right_t (id UUID PRIMARY KEY, val TEXT, left_id UUID)",
        &empty(),
    )
    .unwrap();
    let lid = Uuid::new_v4();
    db.execute(
        "INSERT INTO left_t (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(lid)),
            ("val", Value::Text("left_val".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO right_t (id, val, left_id) VALUES ($id, $val, $lid)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("right_val".into())),
            ("lid", Value::Uuid(lid)),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT l.val, r.val FROM left_t l INNER JOIN right_t r ON l.id = r.left_id",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("left_val".into()));
    assert_eq!(result.rows[0][1], Value::Text("right_val".into()));
}

#[test]
fn jn_04_cte_graph_join_relational() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let eid1 = Uuid::new_v4();
    let eid2 = Uuid::new_v4();
    let eid3 = Uuid::new_v4();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(eid1)),
            ("name", Value::Text("root".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(eid2)),
            ("name", Value::Text("child".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(eid3)),
            ("name", Value::Text("orphan".into())),
        ]),
    )
    .unwrap();

    // Insert edge via transaction (graph subsystem)
    let tx = db.begin();
    db.insert_edge(tx, eid1, eid2, "DEPENDS_ON".into(), HashMap::new())
        .unwrap();
    // Also insert into edges table for relational consistency
    db.insert_row(tx, "edges", {
        let mut m = HashMap::new();
        m.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
        m.insert("source_id".to_string(), Value::Uuid(eid1));
        m.insert("target_id".to_string(), Value::Uuid(eid2));
        m.insert("edge_type".to_string(), Value::Text("DEPENDS_ON".into()));
        m
    })
    .unwrap();
    db.commit(tx).unwrap();

    let result = db
        .execute(
            "WITH reachable AS (
            SELECT b_id FROM GRAPH_TABLE(
                edges MATCH (a)-[:DEPENDS_ON]->(b)
                WHERE a.id = $root_id
                COLUMNS (b.id AS b_id)
            )
        )
        SELECT e.name FROM entities e INNER JOIN reachable r ON e.id = r.b_id",
            &params(vec![("root_id", Value::Uuid(eid1))]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    assert_eq!(result.rows[0][name_idx], Value::Text("child".into()));
}

#[test]
fn jn_05_inner_join_no_matches_empty() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE table_a (id UUID PRIMARY KEY, key_col TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE table_b (id UUID PRIMARY KEY, ref_key TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO table_a (id, key_col) VALUES ($id, $key)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("key", Value::Text("x".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO table_b (id, ref_key) VALUES ($id, $key)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("key", Value::Text("y".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT * FROM table_a a INNER JOIN table_b b ON a.key_col = b.ref_key",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 0, "no matching keys means empty result");
}

#[test]
fn jn_06_cte_filtered_vector_ordering_executes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3), is_deprecated BOOLEAN)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let root = Uuid::new_v4();
    let near = Uuid::new_v4();
    let far = Uuid::new_v4();

    for (id, name, embedding) in [
        (root, "root", vec![0.0, 0.0, 1.0]),
        (near, "near", vec![1.0, 0.0, 0.0]),
        (far, "far", vec![0.0, 1.0, 0.0]),
    ] {
        db.execute(
            "INSERT INTO entities (id, name, embedding, is_deprecated) VALUES ($id, $name, $embedding, $deprecated)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.into())),
                ("embedding", Value::Vector(embedding)),
                ("deprecated", Value::Bool(false)),
            ]),
        )
        .unwrap();
    }

    let tx = db.begin();
    for target in [near, far] {
        db.insert_edge(tx, root, target, "RELATES_TO".into(), HashMap::new())
            .unwrap();
        db.insert_row(tx, "edges", {
            let mut m = HashMap::new();
            m.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
            m.insert("source_id".to_string(), Value::Uuid(root));
            m.insert("target_id".to_string(), Value::Uuid(target));
            m.insert("edge_type".to_string(), Value::Text("RELATES_TO".into()));
            m
        })
        .unwrap();
    }
    db.commit(tx).unwrap();

    let result = db
        .execute(
            "WITH neighborhood AS (
                SELECT b_id FROM GRAPH_TABLE(
                    edges MATCH (a)-[:RELATES_TO]->(b)
                    WHERE a.id = $root_id
                    COLUMNS (b.id AS b_id)
                )
            ),
            filtered AS (
                SELECT id, name, embedding
                FROM entities e
                INNER JOIN neighborhood n ON e.id = n.b_id
                WHERE e.is_deprecated = FALSE
            )
            SELECT id, name FROM filtered ORDER BY embedding <=> $query LIMIT 2",
            &params(vec![
                ("root_id", Value::Uuid(root)),
                ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("CTE-backed vector ordering should execute");

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Uuid(near));
    assert_eq!(result.rows[0][1], Value::Text("near".into()));
    assert_eq!(result.rows[1][0], Value::Uuid(far));
    assert_eq!(result.rows[1][1], Value::Text("far".into()));
}

// ============================================================
// Group 8: Constraints
// ============================================================

#[test]
fn con_01_not_null_rejects_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE strict (id UUID PRIMARY KEY, name TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();

    let result = db.execute(
        "INSERT INTO strict (id, name) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    );
    assert!(result.is_err(), "NOT NULL column must reject NULL insert");
}

#[test]
fn con_02_unique_duplicate_is_noop() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE uniq (id UUID PRIMARY KEY, email TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO uniq (id, email) VALUES ($id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO uniq (id, email) VALUES ($id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();

    let rows = db.scan("uniq", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1, "duplicate UNIQUE insert must be a no-op");
}

#[test]
fn con_03_backward_compat_column_def_serde() {
    // Simulate old serialized ColumnDef without `unique` or `default` fields.
    // The core ColumnDef currently has: name, column_type, nullable, primary_key.
    // After adding `unique: bool` and `default: Option<String>` with #[serde(default)],
    // old data must still deserialize.
    let old_json = r#"{
        "columns": [
            {"name": "id", "column_type": "Uuid", "nullable": false, "primary_key": true},
            {"name": "val", "column_type": "Text", "nullable": true, "primary_key": false}
        ],
        "immutable": false,
        "state_machine": null,
        "dag_edge_types": [],
        "natural_key_column": null,
        "propagation_rules": []
    }"#;

    let meta: contextdb_core::TableMeta = serde_json::from_str(old_json).unwrap();
    assert_eq!(meta.columns.len(), 2);
    assert_eq!(meta.columns[0].name, "id");
    assert_eq!(meta.columns[1].name, "val");
    // After implementation adds `unique` field with serde(default), this should default to false.
    // For now, this test verifies the current schema deserializes without error.
}

#[test]
fn con_04_composite_unique_duplicate_is_noop() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memberships (id UUID PRIMARY KEY, org_id UUID NOT NULL, email TEXT NOT NULL, UNIQUE (org_id, email))",
        &empty(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO memberships (id, org_id, email) VALUES ($id, $org_id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("org_id", Value::Uuid(Uuid::from_u128(1))),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO memberships (id, org_id, email) VALUES ($id, $org_id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("org_id", Value::Uuid(Uuid::from_u128(1))),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();

    let rows = db.scan("memberships", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1, "duplicate composite tuple must be a no-op");
}

#[test]
fn con_05_composite_unique_allows_distinct_tuple() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memberships (id UUID PRIMARY KEY, org_id UUID NOT NULL, email TEXT NOT NULL, UNIQUE (org_id, email))",
        &empty(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO memberships (id, org_id, email) VALUES ($id, $org_id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("org_id", Value::Uuid(Uuid::from_u128(1))),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO memberships (id, org_id, email) VALUES ($id, $org_id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("org_id", Value::Uuid(Uuid::from_u128(2))),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();

    let rows = db.scan("memberships", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 2);
}

// ============================================================
// Group 9: Other
// ============================================================

#[test]
fn idx_01_create_index_accepted() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE indexed (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO indexed (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("alice".into())),
        ]),
    )
    .unwrap();

    let result = db.execute("CREATE INDEX idx_name ON indexed (name)", &empty());
    assert!(result.is_ok(), "CREATE INDEX should be accepted (no-op)");

    // Verify queries still work after index creation
    let q = db
        .execute("SELECT * FROM indexed WHERE name = 'alice'", &empty())
        .unwrap();
    assert_eq!(q.rows.len(), 1);
}

#[test]
fn dual_01_select_without_from() {
    let db = Database::open_memory();
    let result = db.execute("SELECT NOW()", &empty()).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(
        matches!(result.rows[0][0], Value::Timestamp(_)),
        "SELECT NOW() without FROM should return timestamp"
    );
}

#[test]
fn err_01_triple_not_evaluates_correctly() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE errtest (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO errtest (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("hello".into())),
        ]),
    )
    .unwrap();

    // NOT NOT NOT (val = 'hello') = NOT NOT false = NOT true = false => 0 rows
    let result = db
        .execute(
            "SELECT * FROM errtest WHERE NOT NOT NOT val = 'hello'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "triple NOT of true = false, so 0 rows"
    );
}

// ---------------------------------------------------------------------------
// cmt_01 — Line comment stripped
// ---------------------------------------------------------------------------
#[test]
fn cmt_01_line_comment() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, val TEXT)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO t (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("hello".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT * FROM t -- this is a comment\nWHERE val = 'hello'",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let val_idx = result.columns.iter().position(|c| c == "val").unwrap();
    assert_eq!(result.rows[0][val_idx], Value::Text("hello".into()));
}

// ---------------------------------------------------------------------------
// cmt_02 — Block comment stripped
// ---------------------------------------------------------------------------
#[test]
fn cmt_02_block_comment() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, val TEXT)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO t (id, val) VALUES ($id, $val)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("val", Value::Text("hello".into())),
        ]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT /* all columns */ * FROM t WHERE val = 'hello'",
            &empty(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let val_idx = result.columns.iter().position(|c| c == "val").unwrap();
    assert_eq!(result.rows[0][val_idx], Value::Text("hello".into()));
}

// ---------------------------------------------------------------------------
// upd_01 — UPDATE SET count = count + 1
// ---------------------------------------------------------------------------
#[test]
fn upd_01_update_with_arithmetic() {
    let db = Database::open_memory();
    let id1 = Uuid::new_v4();
    db.execute(
        "CREATE TABLE counters (id UUID PRIMARY KEY, count INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO counters (id, count) VALUES ($id, $count)",
        &params(vec![("id", Value::Uuid(id1)), ("count", Value::Int64(10))]),
    )
    .unwrap();

    db.execute(
        "UPDATE counters SET count = count + 1 WHERE id = $id",
        &params(vec![("id", Value::Uuid(id1))]),
    )
    .unwrap();

    let row = db
        .point_lookup("counters", "id", &Value::Uuid(id1), db.snapshot())
        .unwrap()
        .expect("row must exist");
    assert_eq!(
        row.values.get("count"),
        Some(&Value::Int64(11)),
        "count must be 11 after +1"
    );
}

// ---------------------------------------------------------------------------
// upd_02 — UPDATE SET ts = NOW()
// ---------------------------------------------------------------------------
#[test]
fn upd_02_update_with_now() {
    let db = Database::open_memory();
    let id1 = Uuid::new_v4();
    db.execute(
        "CREATE TABLE events (id UUID PRIMARY KEY, ts TIMESTAMP)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO events (id, ts) VALUES ($id, $ts)",
        &params(vec![("id", Value::Uuid(id1)), ("ts", Value::Timestamp(0))]),
    )
    .unwrap();

    db.execute(
        "UPDATE events SET ts = NOW() WHERE id = $id",
        &params(vec![("id", Value::Uuid(id1))]),
    )
    .unwrap();

    let row = db
        .point_lookup("events", "id", &Value::Uuid(id1), db.snapshot())
        .unwrap()
        .expect("row must exist");
    match row.values.get("ts") {
        Some(Value::Timestamp(t)) => assert!(*t > 0, "NOW() must produce a timestamp > 0"),
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

#[test]
fn upd_03_update_embedding_changes_vector_recall_immediately() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    db.execute(
        "INSERT INTO observations (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id_a)),
            ("embedding", Value::Vector(vec![0.95, 0.05, 0.0])),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO observations (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id_b)),
            ("embedding", Value::Vector(vec![0.9, 0.1, 0.0])),
        ]),
    )
    .unwrap();

    let before = db
        .execute(
            "SELECT id FROM observations ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.rows[0][0], Value::Uuid(id_a));

    db.execute(
        "UPDATE observations SET embedding = $embedding WHERE id = $id",
        &params(vec![
            ("id", Value::Uuid(id_a)),
            ("embedding", Value::Vector(vec![-1.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();

    let after = db
        .execute(
            "SELECT id FROM observations ORDER BY embedding <=> $query LIMIT 2",
            &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(after.rows.len(), 2);
    assert_eq!(after.rows[0][0], Value::Uuid(id_b));
    assert_eq!(after.rows[1][0], Value::Uuid(id_a));
}

fn setup_cte_sensor_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, status TEXT, reading REAL)",
        &empty(),
    )
    .unwrap();

    for (id, name, status, reading) in [
        (
            Uuid::from_u128(1),
            "sensor-north",
            "active",
            Value::Float64(42.0),
        ),
        (
            Uuid::from_u128(2),
            "sensor-south",
            "inactive",
            Value::Float64(10.0),
        ),
        (
            Uuid::from_u128(3),
            "widget-east",
            "active",
            Value::Float64(99.0),
        ),
    ] {
        db.execute(
            "INSERT INTO sensors (id, name, status, reading) VALUES ($id, $name, $status, $reading)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
                ("status", Value::Text(status.to_string())),
                ("reading", reading),
            ]),
        )
        .unwrap();
    }

    db
}

#[test]
fn cte_01_outer_where_filters_cte_rows() {
    let db = setup_cte_sensor_db();

    let result = db
        .execute(
            "WITH active AS (SELECT id, name, reading FROM sensors WHERE status = 'active') \
             SELECT id, name FROM active WHERE name LIKE 'sensor%'",
            &empty(),
        )
        .unwrap();

    assert_eq!(
        result.rows.len(),
        1,
        "outer WHERE on CTE should keep only sensor-prefixed active rows"
    );
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let names: Vec<_> = result
        .rows
        .iter()
        .map(|row| row[name_idx].clone())
        .collect();
    assert_eq!(names, vec![Value::Text("sensor-north".to_string())]);
    assert!(
        !names.contains(&Value::Text("widget-east".to_string())),
        "widget-east is active but should be filtered out by outer LIKE predicate"
    );
}

#[test]
fn cte_02_outer_where_comparison_filters_cte_rows() {
    let db = setup_cte_sensor_db();

    let result = db
        .execute(
            "WITH high AS (SELECT id, name, reading FROM sensors WHERE status = 'active') \
             SELECT id, name, reading FROM high WHERE reading > 50.0",
            &empty(),
        )
        .unwrap();

    assert_eq!(
        result.rows.len(),
        1,
        "outer WHERE on CTE should keep only rows above the reading threshold"
    );
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    let reading_idx = result.columns.iter().position(|c| c == "reading").unwrap();
    assert_eq!(
        result.rows[0][name_idx],
        Value::Text("widget-east".to_string())
    );
    assert_eq!(result.rows[0][reading_idx], Value::Float64(99.0));
}

#[test]
fn dag_01_cycle_rejected_via_insert_sql() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE dependencies (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('DEPENDS_ON')",
        &empty(),
    )
    .unwrap();

    let a = Uuid::from_u128(101);
    let b = Uuid::from_u128(102);

    db.execute(
        "INSERT INTO dependencies (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(201))),
            ("source", Value::Uuid(a)),
            ("target", Value::Uuid(b)),
            ("edge_type", Value::Text("DEPENDS_ON".to_string())),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "INSERT INTO dependencies (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(202))),
                ("source", Value::Uuid(b)),
                ("target", Value::Uuid(a)),
                ("edge_type", Value::Text("DEPENDS_ON".to_string())),
            ]),
        )
        .unwrap_err();

    assert!(
        err.to_string().to_lowercase().contains("cycle"),
        "expected cycle rejection on reverse insert, got {err}"
    );
}

#[test]
fn dag_02_self_loop_rejected_via_insert_sql() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE dependencies (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('DEPENDS_ON')",
        &empty(),
    )
    .unwrap();

    let node = Uuid::from_u128(103);
    let err = db
        .execute(
            "INSERT INTO dependencies (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(203))),
                ("source", Value::Uuid(node)),
                ("target", Value::Uuid(node)),
                ("edge_type", Value::Text("DEPENDS_ON".to_string())),
            ]),
        )
        .unwrap_err();

    assert!(
        err.to_string().to_lowercase().contains("cycle"),
        "expected self-loop insert to be rejected as a cycle, got {err}"
    );
}

#[test]
fn prop_01_fk_propagate_fires_on_update() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [archived, completed])",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, status TEXT) STATE MACHINE (status: active -> [invalidated, superseded])",
        &empty(),
    )
    .unwrap();

    let intention_id = Uuid::from_u128(301);
    let decision_id = Uuid::from_u128(302);

    db.execute(
        "INSERT INTO intentions (id, status) VALUES ($id, $status)",
        &params(vec![
            ("id", Value::Uuid(intention_id)),
            ("status", Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO decisions (id, intention_id, status) VALUES ($id, $intention_id, $status)",
        &params(vec![
            ("id", Value::Uuid(decision_id)),
            ("intention_id", Value::Uuid(intention_id)),
            ("status", Value::Text("active".to_string())),
        ]),
    )
    .unwrap();

    db.execute(
        "UPDATE intentions SET status = 'archived' WHERE id = $id",
        &params(vec![("id", Value::Uuid(intention_id))]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT status FROM decisions WHERE id = $id",
            &params(vec![("id", Value::Uuid(decision_id))]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::Text("invalidated".to_string()),
        "child decision should be invalidated after parent UPDATE archives intention"
    );
}

#[test]
fn prop_02_fk_propagate_cascades_multiple_children() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [archived, completed])",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, status TEXT) STATE MACHINE (status: active -> [invalidated, superseded])",
        &empty(),
    )
    .unwrap();

    let intention_id = Uuid::from_u128(401);
    let decision_a = Uuid::from_u128(402);
    let decision_b = Uuid::from_u128(403);

    db.execute(
        "INSERT INTO intentions (id, status) VALUES ($id, $status)",
        &params(vec![
            ("id", Value::Uuid(intention_id)),
            ("status", Value::Text("active".to_string())),
        ]),
    )
    .unwrap();

    for decision_id in [decision_a, decision_b] {
        db.execute(
            "INSERT INTO decisions (id, intention_id, status) VALUES ($id, $intention_id, $status)",
            &params(vec![
                ("id", Value::Uuid(decision_id)),
                ("intention_id", Value::Uuid(intention_id)),
                ("status", Value::Text("active".to_string())),
            ]),
        )
        .unwrap();
    }

    db.execute(
        "UPDATE intentions SET status = 'archived' WHERE id = $id",
        &params(vec![("id", Value::Uuid(intention_id))]),
    )
    .unwrap();

    let result = db
        .execute("SELECT id, status FROM decisions ORDER BY id", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    let id_idx = result.columns.iter().position(|c| c == "id").unwrap();
    let status_idx = result.columns.iter().position(|c| c == "status").unwrap();

    assert_eq!(result.rows[0][id_idx], Value::Uuid(decision_a));
    assert_eq!(
        result.rows[0][status_idx],
        Value::Text("invalidated".to_string())
    );
    assert_eq!(result.rows[1][id_idx], Value::Uuid(decision_b));
    assert_eq!(
        result.rows[1][status_idx],
        Value::Text("invalidated".to_string())
    );
}

fn ddl_name(change: &contextdb_engine::sync_types::DdlChange) -> String {
    match change {
        contextdb_engine::sync_types::DdlChange::CreateTable { name, .. }
        | contextdb_engine::sync_types::DdlChange::DropTable { name }
        | contextdb_engine::sync_types::DdlChange::AlterTable { name, .. } => name.clone(),
    }
}

fn max_non_ddl_lsn(changes: &contextdb_engine::sync_types::ChangeSet) -> Option<u64> {
    let row_max = changes.rows.iter().map(|r| r.lsn).max();
    let edge_max = changes.edges.iter().map(|e| e.lsn).max();
    let vector_max = changes.vectors.iter().map(|v| v.lsn).max();
    row_max.into_iter().chain(edge_max).chain(vector_max).max()
}

#[test]
fn sql_15_ddl_dml_lsn_causal_ordering() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE shared (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();

    let workers = 4;
    let iterations = 40;
    let barrier = Arc::new(Barrier::new(workers + 1));
    let done = Arc::new(AtomicBool::new(false));
    let expected_tables: Vec<String> = (0..workers)
        .flat_map(|worker| (0..iterations).map(move |i| format!("lsn_race_{worker}_{i}")))
        .collect();
    let poller_expected = Arc::new(expected_tables.clone());
    let poller_db = db.clone();
    let poller_done = done.clone();
    let poller_barrier = barrier.clone();

    let poller = thread::spawn(move || {
        let mut watermark = 0_u64;
        let mut seen_creates = std::collections::HashSet::new();
        let mut row_before_create = Vec::new();
        let mut idle_after_done_since = None;
        poller_barrier.wait();

        loop {
            let expected_len = poller_expected.len();
            let changes = poller_db.changes_since(watermark);
            let before_seen = seen_creates.len();
            if !changes.ddl.is_empty() || !changes.rows.is_empty() {
                for ddl in &changes.ddl {
                    if matches!(
                        ddl,
                        contextdb_engine::sync_types::DdlChange::CreateTable { .. }
                    ) {
                        seen_creates.insert(ddl_name(ddl));
                    }
                }
                for row in &changes.rows {
                    if row.table.starts_with("lsn_race_") && !seen_creates.contains(&row.table) {
                        row_before_create.push(row.table.clone());
                    }
                }
                if let Some(lsn) = max_non_ddl_lsn(&changes) {
                    watermark = lsn;
                }
            } else {
                thread::yield_now();
            }

            if poller_done.load(Ordering::SeqCst) && seen_creates.len() >= expected_len {
                break;
            }
            if poller_done.load(Ordering::SeqCst) {
                if seen_creates.len() == before_seen {
                    idle_after_done_since.get_or_insert_with(Instant::now);
                } else {
                    idle_after_done_since = None;
                }
                if idle_after_done_since
                    .is_some_and(|started| started.elapsed() > Duration::from_secs(3))
                {
                    break;
                }
            }
        }

        let expected = poller_expected.as_ref().clone();
        (row_before_create, seen_creates, expected)
    });

    let mut handles = Vec::new();
    for worker in 0..workers {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..iterations {
                let table = format!("lsn_race_{worker}_{i}");
                db.execute(
                    &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, val TEXT)"),
                    &empty(),
                )
                .unwrap();
                db.execute(
                    &format!("INSERT INTO {table} (id, val) VALUES ($id, 'data')"),
                    &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
                )
                .unwrap();
                db.execute(
                    "INSERT INTO shared (id, val) VALUES ($id, 'shared')",
                    &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
                )
                .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    done.store(true, Ordering::SeqCst);
    let (row_before_create, seen_creates, expected) = poller.join().unwrap();

    assert!(
        row_before_create.is_empty(),
        "sync consumer observed row changes before CREATE TABLE DDL for tables: {:?}",
        row_before_create
    );
    for table in expected {
        assert!(
            seen_creates.contains(&table),
            "sync consumer missed CREATE TABLE DDL for {table}"
        );
    }
}

#[test]
fn sql_16_ddl_lsn_no_duplicates_under_contention() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE shared (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();

    let workers = 4;
    let iterations = 25;
    let barrier = Arc::new(Barrier::new(workers + 1));
    let done = Arc::new(AtomicBool::new(false));
    let expected_columns: Vec<String> = (0..workers)
        .flat_map(|worker| (0..iterations).map(move |i| format!("c_{worker}_{i}")))
        .collect();
    let poller_expected = Arc::new(expected_columns.clone());
    let poller_db = db.clone();
    let poller_done = done.clone();
    let poller_barrier = barrier.clone();

    let poller = thread::spawn(move || {
        let mut watermark = 0_u64;
        let mut seen_columns = std::collections::HashSet::new();
        let mut idle_after_done_since = None;
        poller_barrier.wait();

        loop {
            let expected_len = poller_expected.len();
            let changes = poller_db.changes_since(watermark);
            let before_seen = seen_columns.len();
            if !changes.ddl.is_empty() || !changes.rows.is_empty() {
                for ddl in &changes.ddl {
                    if let contextdb_engine::sync_types::DdlChange::AlterTable {
                        name, columns, ..
                    } = ddl
                        && name == "shared"
                    {
                        for (column, _) in columns {
                            if column.starts_with("c_") {
                                seen_columns.insert(column.clone());
                            }
                        }
                    }
                }
                if let Some(lsn) = max_non_ddl_lsn(&changes) {
                    watermark = lsn;
                }
            } else {
                thread::yield_now();
            }

            if poller_done.load(Ordering::SeqCst) && seen_columns.len() >= expected_len {
                break;
            }
            if poller_done.load(Ordering::SeqCst) {
                if seen_columns.len() == before_seen {
                    idle_after_done_since.get_or_insert_with(Instant::now);
                } else {
                    idle_after_done_since = None;
                }
                if idle_after_done_since
                    .is_some_and(|started| started.elapsed() > Duration::from_secs(3))
                {
                    break;
                }
            }
        }

        let expected = poller_expected.len();
        (seen_columns, poller_expected.as_ref().clone(), expected)
    });

    let mut handles = Vec::new();
    for worker in 0..workers {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..iterations {
                let col = format!("c_{worker}_{i}");
                db.execute(
                    "INSERT INTO shared (id, val) VALUES ($id, 'data')",
                    &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
                )
                .unwrap();
                db.execute(
                    &format!("ALTER TABLE shared ADD COLUMN {col} TEXT"),
                    &empty(),
                )
                .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    done.store(true, Ordering::SeqCst);
    let (seen_columns, expected_columns, expected) = poller.join().unwrap();

    assert_eq!(expected_columns.len(), expected);
    for column in expected_columns {
        assert!(
            seen_columns.contains(&column),
            "sync consumer missed ALTER TABLE DDL for added column {column}"
        );
    }
}

#[test]
fn sql_17_sync_watermark_does_not_skip_ddl() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE shared (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));
    let expected_tables: Vec<String> = (0..120).map(|i| format!("watermark_race_{i}")).collect();

    let poller_db = db.clone();
    let poller_done = done.clone();
    let poller_expected = Arc::new(expected_tables.clone());
    let poller_barrier = barrier.clone();
    let poller = thread::spawn(move || {
        let mut watermark = 0_u64;
        let mut seen_tables = std::collections::HashSet::new();
        let mut idle_after_done_since = None;
        poller_barrier.wait();

        loop {
            let expected_len = poller_expected.len();
            let changes = poller_db.changes_since(watermark);
            let before_seen = seen_tables.len();
            if !changes.ddl.is_empty() || !changes.rows.is_empty() {
                for ddl in &changes.ddl {
                    if matches!(
                        ddl,
                        contextdb_engine::sync_types::DdlChange::CreateTable { .. }
                    ) {
                        seen_tables.insert(ddl_name(ddl));
                    }
                }
                if let Some(lsn) = max_non_ddl_lsn(&changes) {
                    watermark = lsn;
                }
            } else {
                thread::yield_now();
            }

            if poller_done.load(Ordering::SeqCst) && seen_tables.len() >= expected_len {
                break;
            }
            if poller_done.load(Ordering::SeqCst) {
                if seen_tables.len() == before_seen {
                    idle_after_done_since.get_or_insert_with(Instant::now);
                } else {
                    idle_after_done_since = None;
                }
                if idle_after_done_since
                    .is_some_and(|started| started.elapsed() > Duration::from_secs(3))
                {
                    break;
                }
            }
        }

        let expected = poller_expected.as_ref().clone();
        (seen_tables, expected)
    });

    let ddl_db = db.clone();
    let ddl_barrier = barrier.clone();
    let ddl_thread = thread::spawn(move || {
        ddl_barrier.wait();
        for i in 0..120 {
            let table = format!("watermark_race_{i}");
            ddl_db
                .execute(
                    &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, val TEXT)"),
                    &empty(),
                )
                .unwrap();
        }
    });

    let dml_db = db.clone();
    let dml_barrier = barrier.clone();
    let dml_thread = thread::spawn(move || {
        dml_barrier.wait();
        for _ in 0..400 {
            dml_db
                .execute(
                    "INSERT INTO shared (id, val) VALUES ($id, 'shared')",
                    &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
                )
                .unwrap();
        }
    });

    ddl_thread.join().unwrap();
    dml_thread.join().unwrap();
    done.store(true, Ordering::SeqCst);
    let (seen_tables, expected) = poller.join().unwrap();

    for table in expected {
        assert!(
            seen_tables.contains(&table),
            "watermark advance skipped CREATE TABLE DDL for {table}"
        );
    }
}

#[test]
fn disk_01_set_disk_limit_parses() {
    let db = Database::open_memory();
    let result = db.execute("SET DISK_LIMIT '1G'", &empty());
    assert!(result.is_ok(), "SET DISK_LIMIT must parse: {result:?}");
}

#[test]
fn disk_02_show_disk_limit_parses() {
    let db = Database::open_memory();
    let result = db.execute("SHOW DISK_LIMIT", &empty()).unwrap();
    assert_eq!(
        result.columns,
        vec!["limit", "used", "available", "startup_ceiling"]
    );
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn disk_03_disk_limit_noop_for_memory() {
    let db = Database::open_memory();
    db.execute("SET DISK_LIMIT '1M'", &empty()).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, payload TEXT)",
        &empty(),
    )
    .unwrap();
    let payload = "x".repeat(128 * 1024);
    let insert = db.execute(
        "INSERT INTO items (id, payload) VALUES ($id, $payload)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("payload", Value::Text(payload)),
        ]),
    );
    assert!(
        insert.is_ok(),
        "in-memory databases must ignore disk limits: {insert:?}"
    );

    let result = db.execute("SHOW DISK_LIMIT", &empty()).unwrap();
    assert_eq!(
        result.columns,
        vec!["limit", "used", "available", "startup_ceiling"]
    );
    assert!(
        match &result.rows[0][0] {
            Value::Null => true,
            Value::Text(s) if s == "none" => true,
            _ => false,
        },
        "in-memory SHOW DISK_LIMIT must report no active limit: {:?}",
        result.rows[0]
    );
}

#[test]
fn disk_04_insert_rejected_when_over_disk_budget() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("disk_04.db");
    let db = Database::open(&db_path).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, payload TEXT)",
        &empty(),
    )
    .unwrap();

    let limit_kib = disk_limit_kib_for_path(&db_path, 64);
    db.execute(&format!("SET DISK_LIMIT '{limit_kib}K'"), &empty())
        .unwrap();

    let payload = "x".repeat(16 * 1024);
    let mut inserted = 0usize;
    let mut failure = None;
    for _ in 0..64 {
        match db.execute(
            "INSERT INTO items (id, payload) VALUES ($id, $payload)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("payload", Value::Text(payload.clone())),
            ]),
        ) {
            Ok(_) => inserted += 1,
            Err(err) => {
                failure = Some(err.to_string());
                break;
            }
        }
    }

    assert!(
        inserted > 0,
        "disk budget should allow at least one insert before rejecting writes"
    );
    let err = failure.expect("eventually expected disk budget rejection");
    assert!(
        err.to_lowercase().contains("disk budget"),
        "error must mention disk budget, got: {err}"
    );
}

#[test]
fn disk_05_disk_limit_persists_across_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("disk_05.db");
    let configured_limit_bytes = {
        let db = Database::open(&db_path).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, payload TEXT)",
            &empty(),
        )
        .unwrap();
        let limit_kib = disk_limit_kib_for_path(&db_path, 64);
        let configured_limit_bytes = (limit_kib * 1024) as i64;
        db.execute(&format!("SET DISK_LIMIT '{limit_kib}K'"), &empty())
            .unwrap();
        let before = db.execute("SHOW DISK_LIMIT", &empty()).unwrap();
        assert!(
            before.rows[0].contains(&Value::Int64(configured_limit_bytes)),
            "SHOW DISK_LIMIT must reflect configured limit before reopen: {:?}",
            before.rows
        );
        db.close().unwrap();
        configured_limit_bytes
    };

    let reopened = Database::open(&db_path).unwrap();
    let after = reopened.execute("SHOW DISK_LIMIT", &empty()).unwrap();
    assert!(
        after.rows[0].contains(&Value::Int64(configured_limit_bytes)),
        "SHOW DISK_LIMIT must reflect persisted limit after reopen: {:?}",
        after.rows
    );

    let payload = "x".repeat(16 * 1024);
    let mut failure = None;
    for _ in 0..64 {
        match reopened.execute(
            "INSERT INTO items (id, payload) VALUES ($id, $payload)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("payload", Value::Text(payload.clone())),
            ]),
        ) {
            Ok(_) => {}
            Err(err) => {
                failure = Some(err.to_string());
                break;
            }
        }
    }
    let err = failure.expect("persisted disk limit must still reject writes after reopen");
    assert!(
        err.to_lowercase().contains("disk budget"),
        "reopened file-backed database must still enforce persisted disk limit: {err}"
    );
}

#[test]
fn disk_06_sync_pull_rejected_when_over_disk_budget() {
    let edge_tmp = TempDir::new().expect("edge tempdir");
    let server_tmp = TempDir::new().expect("server tempdir");
    let edge_path = edge_tmp.path().join("edge.db");
    let server_path = server_tmp.path().join("server.db");

    let edge = Database::open(&edge_path).unwrap();
    edge.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, payload TEXT)",
        &empty(),
    )
    .unwrap();
    for _ in 0..24 {
        edge.execute(
            "INSERT INTO items (id, payload) VALUES ($id, $payload)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("payload", Value::Text("x".repeat(8 * 1024))),
            ]),
        )
        .unwrap();
    }
    let changes = edge.changes_since(0);

    let server = Database::open(&server_path).unwrap();
    server
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, payload TEXT)",
            &empty(),
        )
        .unwrap();
    server
        .execute(
            "INSERT INTO items (id, payload) VALUES ($id, $payload)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("payload", Value::Text("prime".repeat(1024))),
            ]),
        )
        .unwrap();
    let limit_kib = (std::fs::metadata(&server_path).unwrap().len() / 1024).max(1);
    server
        .execute(&format!("SET DISK_LIMIT '{limit_kib}K'"), &empty())
        .unwrap();
    server.close().unwrap();

    let server = Database::open(&server_path).unwrap();

    let result = server.apply_changes(
        changes,
        &contextdb_engine::sync_types::ConflictPolicies::uniform(
            contextdb_engine::sync_types::ConflictPolicy::LatestWins,
        ),
    );
    assert!(
        result.is_err(),
        "sync pull must fail when disk budget blocks persistence"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("disk budget"),
        "sync pull failure must mention disk budget, got: {err}"
    );

    let count = server
        .execute("SELECT COUNT(*) FROM items", &empty())
        .unwrap()
        .rows[0][0]
        .clone();
    assert_eq!(
        count,
        Value::Int64(1),
        "failed sync pull must not make remote rows visible on the server"
    );
}

#[test]
fn sql_09_vector_text_coercion() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id_a = Uuid::from_u128(9001);
    let id_b = Uuid::from_u128(9002);

    db.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id_a)),
            ("embedding", Value::Vector(vec![0.1, 0.2, 0.3])),
        ]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, '[0.4, 0.5, 0.6]')",
        &params(vec![("id", Value::Uuid(id_b))]),
    )
    .unwrap();

    let rows = db
        .execute("SELECT id, embedding FROM docs ORDER BY id", &empty())
        .unwrap();
    let id_idx = rows.columns.iter().position(|c| c == "id").unwrap();
    let embedding_idx = rows.columns.iter().position(|c| c == "embedding").unwrap();

    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0][id_idx], Value::Uuid(id_a));
    assert!(matches!(rows.rows[0][embedding_idx], Value::Vector(_)));
    assert_eq!(rows.rows[1][id_idx], Value::Uuid(id_b));
    assert!(
        matches!(rows.rows[1][embedding_idx], Value::Vector(_)),
        "quoted vector literal should be coerced to Value::Vector, got {:?}",
        rows.rows[1][embedding_idx]
    );

    let search = db
        .execute(
            "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 2",
            &params(vec![("query", Value::Vector(vec![0.4, 0.5, 0.6]))]),
        )
        .unwrap();
    let search_id_idx = search.columns.iter().position(|c| c == "id").unwrap();
    let ids: Vec<Value> = search
        .rows
        .iter()
        .map(|r| r[search_id_idx].clone())
        .collect();
    assert!(ids.contains(&Value::Uuid(id_a)));
    assert!(ids.contains(&Value::Uuid(id_b)));
}

#[test]
fn sql_10_edge_dedup_no_relational_leak() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let source = Uuid::from_u128(9101);
    let target = Uuid::from_u128(9102);

    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'CITES')",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(9103))),
            ("source", Value::Uuid(source)),
            ("target", Value::Uuid(target)),
        ]),
    )
    .unwrap();

    let second = db
        .execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'CITES')",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(9104))),
                ("source", Value::Uuid(source)),
                ("target", Value::Uuid(target)),
            ]),
        )
        .unwrap();

    let count = db
        .execute(
            "SELECT COUNT(*) FROM edges WHERE source_id = $source AND target_id = $target AND edge_type = 'CITES'",
            &params(vec![
                ("source", Value::Uuid(source)),
                ("target", Value::Uuid(target)),
            ]),
        )
        .unwrap();

    assert_eq!(
        count.rows[0][0],
        Value::Int64(1),
        "duplicate logical edge should not leave a second relational row"
    );
    assert_eq!(
        second.rows_affected, 0,
        "deduped second edge insert should report zero affected rows"
    );
}

#[test]
fn sql_11_upsert_set_clause_values() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE kv (id UUID PRIMARY KEY, val TEXT)", &empty())
        .unwrap();

    let id = Uuid::from_u128(9201);
    db.execute(
        "INSERT INTO kv (id, val) VALUES ($id, 'original')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO kv (id, val) VALUES ($id, 'from-insert') ON CONFLICT (id) DO UPDATE SET val = 'from-update'",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let out = db
        .execute(
            "SELECT val FROM kv WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(
        out.rows[0][0],
        Value::Text("from-update".to_string()),
        "upsert should apply SET clause values, not the INSERT value map"
    );
}

#[test]
fn sql_12_not_between() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE scores (id UUID PRIMARY KEY, val REAL)",
        &empty(),
    )
    .unwrap();

    for (id, val) in [
        (Uuid::from_u128(9301), 0.1_f64),
        (Uuid::from_u128(9302), 0.5_f64),
        (Uuid::from_u128(9303), 0.9_f64),
    ] {
        db.execute(
            "INSERT INTO scores (id, val) VALUES ($id, $val)",
            &params(vec![("id", Value::Uuid(id)), ("val", Value::Float64(val))]),
        )
        .unwrap();
    }

    let out = db
        .execute(
            "SELECT val FROM scores WHERE val NOT BETWEEN 0.3 AND 0.7 ORDER BY val",
            &empty(),
        )
        .unwrap();

    let vals: Vec<Value> = out.rows.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(vals, vec![Value::Float64(0.1), Value::Float64(0.9)]);
}

#[test]
fn sql_13_null_in_nullable_uuid() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE refs (id UUID PRIMARY KEY, parent_id UUID)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(9401);
    db.execute(
        "INSERT INTO refs (id, parent_id) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let out = db
        .execute(
            "SELECT parent_id FROM refs WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Value::Null);
}

#[test]
fn integrity_01_text_memory_estimate_not_pathologically_high() {
    let accountant = Arc::new(MemoryAccountant::with_budget(32 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, body TEXT)",
        &empty(),
    )
    .unwrap();

    let body = "x".repeat(1024);
    let result = db.execute(
        "INSERT INTO docs (id, body) VALUES ($id, $body)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("body", Value::Text(body)),
        ]),
    );
    assert!(
        result.is_ok(),
        "1KiB TEXT insert should fit within a 32KiB budget: {result:?}"
    );
    assert!(
        accountant.usage().used < 16 * 1024,
        "1KiB TEXT row should not consume pathological memory, got {} bytes",
        accountant.usage().used
    );
}

#[test]
fn integrity_02_upsert_noop_does_not_leak_memory() {
    let accountant = Arc::new(MemoryAccountant::with_budget(12 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute("CREATE TABLE kv (id UUID PRIMARY KEY, val TEXT)", &empty())
        .unwrap();

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO kv (id, val) VALUES ($id, 'same')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();
    let baseline = accountant.usage().used;

    for _ in 0..20 {
        db.execute(
            "INSERT INTO kv (id, val) VALUES ($id, 'same') ON CONFLICT (id) DO UPDATE SET val = 'same'",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    }

    let used = accountant.usage().used;
    assert!(
        used <= baseline + 256,
        "noop upserts must not leak memory: baseline={baseline}, used={used}"
    );
}

#[test]
fn integrity_03_retain_pruning_releases_memory() {
    let accountant = Arc::new(MemoryAccountant::with_budget(256 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, data TEXT) RETAIN 1 SECONDS",
        &empty(),
    )
    .unwrap();

    let baseline = accountant.usage().used;
    db.execute(
        "INSERT INTO obs (id, data) VALUES ($id, $data)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Text("x".repeat(4096))),
        ]),
    )
    .unwrap();
    let used_after_insert = accountant.usage().used;
    assert!(used_after_insert > baseline);

    std::thread::sleep(Duration::from_millis(1100));
    let pruned = db.run_pruning_cycle();
    assert_eq!(pruned, 1, "expired row must be pruned");

    let used_after_prune = accountant.usage().used;
    assert!(
        used_after_prune + 512 < used_after_insert,
        "pruning must release row memory: before={used_after_insert}, after={used_after_prune}"
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM obs", &empty())
            .unwrap()
            .rows[0][0],
        Value::Int64(0)
    );
}

#[test]
fn integrity_04_edge_delete_releases_memory() {
    let accountant = Arc::new(MemoryAccountant::with_budget(256 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());

    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let baseline = accountant.usage().used;

    let tx = db.begin();
    assert!(
        db.insert_edge(tx, source, target, "REL".to_string(), HashMap::new())
            .unwrap()
    );
    db.commit(tx).unwrap();

    let used_after_insert = accountant.usage().used;
    assert!(used_after_insert > baseline);

    let tx = db.begin();
    db.delete_edge(tx, source, target, "REL").unwrap();
    db.commit(tx).unwrap();

    let used_after_delete = accountant.usage().used;
    assert!(
        used_after_delete + 128 < used_after_insert,
        "edge delete must release adjacency memory: before={used_after_insert}, after={used_after_delete}"
    );
}

#[test]
fn integrity_05_drop_table_releases_edge_memory() {
    let accountant = Arc::new(MemoryAccountant::with_budget(256 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'REL')",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source", Value::Uuid(source)),
            ("target", Value::Uuid(target)),
        ]),
    )
    .unwrap();
    let used_after_insert = accountant.usage().used;

    db.execute("DROP TABLE edges", &empty()).unwrap();

    assert!(
        accountant.usage().used + 128 < used_after_insert,
        "DROP TABLE must release edge allocations: before={used_after_insert}, after={}",
        accountant.usage().used
    );
    let bfs = db
        .query_bfs(
            source,
            Some(&["REL".to_string()]),
            contextdb_core::Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        bfs.nodes.len(),
        0,
        "dropped edge table must not leave graph edges behind"
    );
}

#[test]
fn integrity_06_create_table_honors_disk_budget() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("integrity-create-table.db");
    let db = Database::open(&path).unwrap();

    let limit_kib = disk_limit_kib_for_path(&path, 0);
    db.execute(&format!("SET DISK_LIMIT '{limit_kib}K'"), &empty())
        .unwrap();

    let result = db.execute("CREATE TABLE blocked (id UUID PRIMARY KEY)", &empty());
    assert!(
        result.is_err(),
        "CREATE TABLE must fail when disk budget is already exhausted"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("disk budget"),
        "disk-budget rejection must mention disk budget, got: {err}"
    );
    assert!(
        db.table_meta("blocked").is_none(),
        "failed CREATE TABLE must not leave table metadata behind"
    );
}

#[test]
fn integrity_07_alter_table_honors_disk_budget() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("integrity-alter-table.db");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY)", &empty())
        .unwrap();

    let limit_kib = disk_limit_kib_for_path(&path, 0);
    db.execute(&format!("SET DISK_LIMIT '{limit_kib}K'"), &empty())
        .unwrap();

    let result = db.execute("ALTER TABLE items ADD COLUMN note TEXT", &empty());
    assert!(
        result.is_err(),
        "ALTER TABLE must fail when disk budget is already exhausted"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("disk budget"),
        "disk-budget rejection must mention disk budget, got: {err}"
    );
    let meta = db.table_meta("items").unwrap();
    assert!(
        meta.columns.iter().all(|c| c.name != "note"),
        "failed ALTER TABLE must not mutate schema"
    );
}

#[test]
fn integrity_08_upsert_insert_indexes_vector() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, '[1.0, 0.0, 0.0]')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, '[0.0, 1.0, 0.0]') ON CONFLICT (id) DO UPDATE SET embedding = '[0.0, 1.0, 0.0]'",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![0.0, 1.0, 0.0]))]),
        )
        .unwrap()
        .rows;
    assert_eq!(
        result.len(),
        1,
        "vector search must still find the upserted row"
    );
    assert_eq!(
        result[0][0],
        Value::Uuid(id),
        "vector search must resolve to the row updated by ON CONFLICT DO UPDATE"
    );
}

#[test]
fn integrity_09_drop_table_removes_vectors() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, '[0.1, 0.2, 0.3]')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();
    let row_id = db
        .point_lookup("docs", "id", &Value::Uuid(id), db.snapshot())
        .unwrap()
        .expect("row must exist")
        .row_id;
    assert!(
        db.live_vector_entry(row_id, db.snapshot()).is_some(),
        "vector must exist before DROP TABLE"
    );

    db.execute("DROP TABLE docs", &empty()).unwrap();

    assert!(
        db.live_vector_entry(row_id, db.snapshot()).is_none(),
        "DROP TABLE must remove vector entries for dropped rows"
    );
}

#[test]
fn integrity_10_failed_insert_does_not_leak_memory() {
    let accountant = Arc::new(MemoryAccountant::with_budget(12 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'dup')",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    let baseline = accountant.usage().used;

    for _ in 0..20 {
        db.execute(
            "INSERT INTO items (id, name) VALUES ($id, 'dup')",
            &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    }

    let used = accountant.usage().used;
    assert!(
        used <= baseline + 256,
        "duplicate no-op inserts must not leak memory: baseline={baseline}, used={used}"
    );
}
