#[cfg_attr(not(feature = "test-seams"), allow(unused_imports))] #[rustfmt::skip]
use contextdb_core::{ContextId, Error, Lsn, MemoryAccountant, Principal, RowId, ScopeLabel, Value, VectorIndexRef};
use contextdb_engine::{Database, sync_types as sync};
use std::collections::{BTreeSet, HashMap};
#[cfg_attr(not(feature = "test-seams"), allow(unused_imports))] #[rustfmt::skip]
use std::sync::{atomic::{AtomicBool, Ordering}, mpsc::{self, TryRecvError}, Arc, Barrier};
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

    for i in 0..1_000 {
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

    let result = db
        .execute("SELECT DISTINCT tag FROM items", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 100);
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
    let tx = db.begin_or_panic();
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

    let tx = db.begin_or_panic();
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

fn sarg_uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn sarg_contexts(contexts: &[Uuid]) -> BTreeSet<ContextId> {
    contexts.iter().copied().map(ContextId::new).collect()
}

fn sarg_labels(labels: &[&str]) -> BTreeSet<ScopeLabel> {
    labels.iter().copied().map(ScopeLabel::new).collect()
}

fn sarg_insert_edge(db: &Database, source: Uuid, target: Uuid, edge_type: &str) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(source)),
            ("target_id", Value::Uuid(target)),
            ("edge_type", Value::Text(edge_type.to_string())),
        ]),
    )
    .unwrap();
}

fn sarg_insert_context_edge(db: &Database, source: Uuid, target: Uuid, edge_type: &str, ctx: Uuid) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type, context_id) VALUES ($id, $source_id, $target_id, $edge_type, $ctx)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source_id", Value::Uuid(source)),
            ("target_id", Value::Uuid(target)),
            ("edge_type", Value::Text(edge_type.to_string())),
            ("ctx", Value::Uuid(ctx)),
        ]),
    )
    .unwrap();
}

fn sarg_insert_node(db: &Database, id: Uuid, ctx: Option<Uuid>, kind: Option<&str>) {
    match (ctx, kind) {
        (Some(ctx), Some(kind)) => db
            .execute(
                "INSERT INTO nodes (id, context_id, kind) VALUES ($id, $ctx, $kind)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("ctx", Value::Uuid(ctx)),
                    ("kind", Value::Text(kind.to_string())),
                ]),
            )
            .unwrap(),
        (Some(ctx), None) => db
            .execute(
                "INSERT INTO nodes (id, context_id) VALUES ($id, $ctx)",
                &params(vec![("id", Value::Uuid(id)), ("ctx", Value::Uuid(ctx))]),
            )
            .unwrap(),
        (None, Some(kind)) => db
            .execute(
                "INSERT INTO nodes (id, kind) VALUES ($id, $kind)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("kind", Value::Text(kind.to_string())),
                ]),
            )
            .unwrap(),
        (None, None) => db
            .execute(
                "INSERT INTO nodes (id) VALUES ($id)",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .unwrap(),
    };
}

fn sarg_uuid_column_set(result: &contextdb_engine::QueryResult, column: &str) -> BTreeSet<Uuid> {
    let idx = result
        .columns
        .iter()
        .position(|c| c == column || c.rsplit('.').next() == Some(column))
        .unwrap_or_else(|| panic!("column {column} not found in {:?}", result.columns));
    result
        .rows
        .iter()
        .map(|row| match &row[idx] {
            Value::Uuid(id) => *id,
            other => panic!("expected UUID in column {column}, got {other:?}"),
        })
        .collect()
}

fn sarg_row_id_for_uuid(db: &Database, table: &str, id: Uuid) -> RowId {
    db.scan(table, db.snapshot())
        .unwrap()
        .into_iter()
        .find(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .map(|row| row.row_id)
        .unwrap_or_else(|| panic!("row {id} not found in {table}"))
}

fn sarg_expect_context_violation(err: Error, requested: Uuid, allowed: &[Uuid]) {
    match err {
        Error::ContextScopeViolation {
            requested: got_requested,
            allowed: got_allowed,
        } => {
            assert_eq!(got_requested, ContextId::new(requested));
            assert_eq!(got_allowed, sarg_contexts(allowed));
        }
        other => panic!("expected ContextScopeViolation, got {other:?}"),
    }
}

fn sarg_expect_scope_violation(err: Error, requested: &str, allowed: &[&str]) {
    match err {
        Error::ScopeLabelViolation {
            requested: got_requested,
            allowed: got_allowed,
        } => {
            assert_eq!(got_requested, ScopeLabel::new(requested));
            assert_eq!(got_allowed, sarg_labels(allowed));
        }
        other => panic!("expected ScopeLabelViolation, got {other:?}"),
    }
}

fn sarg_expect_acl_denied(err: Error, table: &str, row_id: RowId, principal: &str) {
    match err {
        Error::AclDenied {
            table: got_table,
            row_id: got_row_id,
            principal: got_principal,
        } => {
            assert_eq!(got_table, table);
            assert_eq!(got_row_id, row_id);
            assert_eq!(got_principal, Principal::Agent(principal.to_string()));
        }
        other => panic!("expected AclDenied, got {other:?}"),
    }
}

fn sarg_insert_acl_grant(db: &Database, id: Uuid, principal: &str, acl: Uuid) {
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', $principal, $acl)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("principal", Value::Text(principal.to_string())),
            ("acl", Value::Uuid(acl)),
        ]),
    )
    .unwrap();
}

#[test]
fn scoped_anchor_read_reports_context_scope_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let ctx_c = sarg_uuid(0xC);
    for (id, ctx, data) in [
        (sarg_uuid(1), ctx_a, "a1"),
        (sarg_uuid(2), ctx_a, "a2"),
        (sarg_uuid(3), ctx_b, "b"),
        (sarg_uuid(4), ctx_c, "c"),
    ] {
        db.execute(
            "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, $data)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("ctx", Value::Uuid(ctx)),
                ("data", Value::Text(data.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let err_b = scoped
        .execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(3)))]),
        )
        .expect_err("hidden ctx-b anchor must be refused");
    sarg_expect_context_violation(err_b, ctx_b, &[ctx_a]);
    let err_c = scoped
        .execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(4)))]),
        )
        .expect_err("hidden ctx-c anchor must be refused");
    sarg_expect_context_violation(err_c, ctx_c, &[ctx_a]);
}

#[test]
fn scoped_anchor_read_reports_scope_label_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, scope TEXT SCOPE_LABEL_READ ('a','b','c') WRITE ('a','b','c'), data TEXT)",
        &empty(),
    )
    .unwrap();
    for (id, scope) in [(1, "a"), (2, "b"), (3, "c")] {
        db.execute(
            "INSERT INTO t (id, scope, data) VALUES ($id, $scope, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("scope", Value::Text(scope.to_string())),
                ("data", Value::Text(scope.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_constraints(None, Some(sarg_labels(&["a"])), None);
    for (id, label) in [(2, "b"), (3, "c")] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE id = $id",
                &params(vec![("id", Value::Uuid(sarg_uuid(id)))]),
            )
            .expect_err("hidden scope-label anchor must be refused");
        sarg_expect_scope_violation(err, label, &["a"]);
    }
}

#[test]
fn scoped_anchor_read_reports_acl_denied() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), data TEXT)",
        &empty(),
    )
    .unwrap();
    let principal = "agent-a";
    let acl_visible = sarg_uuid(0xA1);
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', $principal, $acl)",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(0xAA))),
            ("principal", Value::Text(principal.to_string())),
            ("acl", Value::Uuid(acl_visible)),
        ]),
    )
    .unwrap();
    for (id, acl, data) in [
        (sarg_uuid(1), acl_visible, "visible"),
        (sarg_uuid(2), sarg_uuid(0xB1), "hidden-b"),
        (sarg_uuid(3), sarg_uuid(0xC1), "hidden-c"),
    ] {
        db.execute(
            "INSERT INTO t (id, acl_id, data) VALUES ($id, $acl, $data)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("acl", Value::Uuid(acl)),
                ("data", Value::Text(data.to_string())),
            ]),
        )
        .unwrap();
    }
    let row_2 = sarg_row_id_for_uuid(&db, "t", sarg_uuid(2));
    let row_3 = sarg_row_id_for_uuid(&db, "t", sarg_uuid(3));
    let scoped =
        db.scoped_with_constraints(None, None, Some(Principal::Agent(principal.to_string())));
    for (id, row_id) in [(sarg_uuid(2), row_2), (sarg_uuid(3), row_3)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE id = $id",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect_err("ungranted ACL anchor must be refused");
        sarg_expect_acl_denied(err, "t", row_id, principal);
    }
}

#[test]
fn unique_column_anchor_read_under_constraint_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, uniq_col TEXT UNIQUE, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let ctx_c = sarg_uuid(0xC);
    for (id, ctx, key) in [(1, ctx_a, "a"), (2, ctx_b, "b"), (3, ctx_c, "c")] {
        db.execute(
            "INSERT INTO t (id, context_id, uniq_col, data) VALUES ($id, $ctx, $key, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("ctx", Value::Uuid(ctx)),
                ("key", Value::Text(key.to_string())),
                ("data", Value::Text(key.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    for (key, ctx) in [("b", ctx_b), ("c", ctx_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE uniq_col = $key",
                &params(vec![("key", Value::Text(key.to_string()))]),
            )
            .expect_err("hidden unique anchor must be refused");
        sarg_expect_context_violation(err, ctx, &[ctx_a]);
    }
}

#[test]
fn scope_label_unique_anchor_read_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, scope TEXT SCOPE_LABEL_READ ('a','b','c') WRITE ('a','b','c'), uniq_col TEXT UNIQUE, data TEXT)",
        &empty(),
    )
    .unwrap();
    for (id, scope, key) in [(1, "a", "a"), (2, "b", "b"), (3, "c", "c")] {
        db.execute(
            "INSERT INTO t (id, scope, uniq_col, data) VALUES ($id, $scope, $key, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("scope", Value::Text(scope.to_string())),
                ("key", Value::Text(key.to_string())),
                ("data", Value::Text(key.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_constraints(None, Some(sarg_labels(&["a"])), None);
    for (key, label) in [("b", "b"), ("c", "c")] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE uniq_col = $key",
                &params(vec![("key", Value::Text(key.to_string()))]),
            )
            .expect_err("hidden scope-label unique anchor must be refused");
        sarg_expect_scope_violation(err, label, &["a"]);
    }
}

#[test]
fn acl_unique_anchor_read_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), uniq_col TEXT UNIQUE, data TEXT)",
        &empty(),
    )
    .unwrap();
    let principal = "agent-a";
    let acl_visible = sarg_uuid(0xA1);
    sarg_insert_acl_grant(&db, sarg_uuid(0xAA), principal, acl_visible);
    for (id, acl, key) in [
        (sarg_uuid(1), acl_visible, "a"),
        (sarg_uuid(2), sarg_uuid(0xB1), "b"),
        (sarg_uuid(3), sarg_uuid(0xC1), "c"),
    ] {
        db.execute(
            "INSERT INTO t (id, acl_id, uniq_col, data) VALUES ($id, $acl, $key, $data)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("acl", Value::Uuid(acl)),
                ("key", Value::Text(key.to_string())),
                ("data", Value::Text(key.to_string())),
            ]),
        )
        .unwrap();
    }
    let row_b = sarg_row_id_for_uuid(&db, "t", sarg_uuid(2));
    let row_c = sarg_row_id_for_uuid(&db, "t", sarg_uuid(3));
    let scoped =
        db.scoped_with_constraints(None, None, Some(Principal::Agent(principal.to_string())));
    for (key, row_id) in [("b", row_b), ("c", row_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE uniq_col = $key",
                &params(vec![("key", Value::Text(key.to_string()))]),
            )
            .expect_err("hidden ACL unique anchor must be refused");
        sarg_expect_acl_denied(err, "t", row_id, principal);
    }
}

#[test]
fn composite_unique_anchor_read_under_constraint_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, u_a INTEGER, u_b INTEGER, data TEXT, UNIQUE (u_a, u_b))",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let ctx_c = sarg_uuid(0xC);
    for (id, ctx, u_a, u_b, data) in [
        (1, ctx_b, 1, 1, "b"),
        (2, ctx_c, 1, 2, "c"),
        (3, ctx_a, 2, 1, "a"),
    ] {
        db.execute(
            "INSERT INTO t (id, context_id, u_a, u_b, data) VALUES ($id, $ctx, $u_a, $u_b, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("ctx", Value::Uuid(ctx)),
                ("u_a", Value::Int64(u_a)),
                ("u_b", Value::Int64(u_b)),
                ("data", Value::Text(data.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    for (u_b, ctx) in [(1, ctx_b), (2, ctx_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE u_a = $u_a AND u_b = $u_b",
                &params(vec![("u_a", Value::Int64(1)), ("u_b", Value::Int64(u_b))]),
            )
            .expect_err("hidden composite unique anchor must be refused");
        sarg_expect_context_violation(err, ctx, &[ctx_a]);
    }
}

#[test]
fn scope_label_composite_unique_anchor_read_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, scope TEXT SCOPE_LABEL_READ ('a','b','c') WRITE ('a','b','c'), u_a INTEGER, u_b INTEGER, data TEXT, UNIQUE (u_a, u_b))",
        &empty(),
    )
    .unwrap();
    for (id, scope, u_a, u_b, data) in [
        (1, "b", 1, 1, "b"),
        (2, "c", 1, 2, "c"),
        (3, "a", 2, 1, "a"),
    ] {
        db.execute(
            "INSERT INTO t (id, scope, u_a, u_b, data) VALUES ($id, $scope, $u_a, $u_b, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("scope", Value::Text(scope.to_string())),
                ("u_a", Value::Int64(u_a)),
                ("u_b", Value::Int64(u_b)),
                ("data", Value::Text(data.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_constraints(None, Some(sarg_labels(&["a"])), None);
    for (u_b, label) in [(1, "b"), (2, "c")] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE u_a = $u_a AND u_b = $u_b",
                &params(vec![("u_a", Value::Int64(1)), ("u_b", Value::Int64(u_b))]),
            )
            .expect_err("hidden scope-label composite unique anchor must be refused");
        sarg_expect_scope_violation(err, label, &["a"]);
    }
}

#[test]
fn acl_composite_unique_anchor_read_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), u_a INTEGER, u_b INTEGER, data TEXT, UNIQUE (u_a, u_b))",
        &empty(),
    )
    .unwrap();
    let principal = "agent-a";
    let acl_visible = sarg_uuid(0xA1);
    sarg_insert_acl_grant(&db, sarg_uuid(0xAA), principal, acl_visible);
    for (id, acl, u_a, u_b, data) in [
        (sarg_uuid(1), sarg_uuid(0xB1), 1, 1, "b"),
        (sarg_uuid(2), sarg_uuid(0xC1), 1, 2, "c"),
        (sarg_uuid(3), acl_visible, 2, 1, "a"),
    ] {
        db.execute(
            "INSERT INTO t (id, acl_id, u_a, u_b, data) VALUES ($id, $acl, $u_a, $u_b, $data)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("acl", Value::Uuid(acl)),
                ("u_a", Value::Int64(u_a)),
                ("u_b", Value::Int64(u_b)),
                ("data", Value::Text(data.to_string())),
            ]),
        )
        .unwrap();
    }
    let row_b = sarg_row_id_for_uuid(&db, "t", sarg_uuid(1));
    let row_c = sarg_row_id_for_uuid(&db, "t", sarg_uuid(2));
    let scoped =
        db.scoped_with_constraints(None, None, Some(Principal::Agent(principal.to_string())));
    for (u_b, row_id) in [(1, row_b), (2, row_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE u_a = $u_a AND u_b = $u_b",
                &params(vec![("u_a", Value::Int64(1)), ("u_b", Value::Int64(u_b))]),
            )
            .expect_err("hidden ACL composite unique anchor must be refused");
        sarg_expect_acl_denied(err, "t", row_id, principal);
    }
}

#[test]
fn null_context_id_row_anchor_read_under_scoped_handle_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    db.execute(
        "INSERT INTO t (id, data) VALUES ($id, 'null-context')",
        &params(vec![("id", Value::Uuid(sarg_uuid(1)))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, 'ctx-b')",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(2))),
            ("ctx", Value::Uuid(ctx_b)),
        ]),
    )
    .unwrap();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let err_null = scoped
        .execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(1)))]),
        )
        .expect_err("NULL context anchor must be refused");
    sarg_expect_context_violation(err_null, Uuid::from_u128(u128::MAX), &[ctx_a]);
    let err_b = scoped
        .execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(2)))]),
        )
        .expect_err("foreign context anchor must be refused");
    sarg_expect_context_violation(err_b, ctx_b, &[ctx_a]);
}

#[test]
fn stacked_constraints_anchor_read_reports_first_violating_gate_context_first() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, scope TEXT SCOPE_LABEL_READ ('a','c','d') WRITE ('a','c','d'), acl_id UUID ACL REFERENCES acl_grants(acl_id), data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let ctx_c = sarg_uuid(0xC);
    for (id, ctx, scope, acl) in [
        (1, ctx_b, "c", sarg_uuid(0xB1)),
        (2, ctx_c, "d", sarg_uuid(0xC1)),
    ] {
        db.execute(
            "INSERT INTO t (id, context_id, scope, acl_id, data) VALUES ($id, $ctx, $scope, $acl, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("ctx", Value::Uuid(ctx)),
                ("scope", Value::Text(scope.to_string())),
                ("acl", Value::Uuid(acl)),
                ("data", Value::Text(scope.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_constraints(
        Some(sarg_contexts(&[ctx_a])),
        Some(sarg_labels(&["a"])),
        Some(Principal::Agent("agent-a".to_string())),
    );
    for (id, ctx) in [(1, ctx_b), (2, ctx_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE id = $id",
                &params(vec![("id", Value::Uuid(sarg_uuid(id)))]),
            )
            .expect_err("first violating gate must be context");
        sarg_expect_context_violation(err, ctx, &[ctx_a]);
    }
}

#[test]
fn scoped_list_query_continues_to_filter_hidden_rows_silently_across_shapes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, category TEXT, score INTEGER)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    for (id, ctx, category, score) in [
        (1, ctx_a, "x", 10),
        (2, ctx_a, "x", 90),
        (3, ctx_b, "x", 80),
        (4, ctx_b, "x", 81),
        (5, ctx_b, "y", 82),
        (6, ctx_b, "y", 83),
    ] {
        db.execute(
            "INSERT INTO t (id, context_id, category, score) VALUES ($id, $ctx, $category, $score)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("ctx", Value::Uuid(ctx)),
                ("category", Value::Text(category.to_string())),
                ("score", Value::Int64(score)),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let all = scoped.execute("SELECT id FROM t", &empty()).unwrap();
    assert_eq!(
        sarg_uuid_column_set(&all, "id"),
        BTreeSet::from([sarg_uuid(1), sarg_uuid(2)])
    );
    let x = scoped
        .execute("SELECT id FROM t WHERE category = 'x'", &empty())
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&x, "id"),
        BTreeSet::from([sarg_uuid(1), sarg_uuid(2)])
    );
    let range = scoped
        .execute("SELECT id FROM t WHERE score > 50", &empty())
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&range, "id"),
        BTreeSet::from([sarg_uuid(2)])
    );
    let y = scoped
        .execute("SELECT id FROM t WHERE category = 'y'", &empty())
        .unwrap();
    assert!(y.rows.is_empty());
    let limit = scoped
        .execute("SELECT id FROM t LIMIT 1", &empty())
        .unwrap();
    assert_eq!(limit.rows.len(), 1);
    assert!(
        sarg_uuid_column_set(&limit, "id").is_subset(&BTreeSet::from([sarg_uuid(1), sarg_uuid(2)]))
    );
    let ordered = scoped
        .execute("SELECT id FROM t ORDER BY id LIMIT 1", &empty())
        .unwrap();
    assert_eq!(ordered.rows, vec![vec![Value::Uuid(sarg_uuid(1))]]);
}

#[test]
fn scoped_anchor_read_returns_empty_when_row_does_not_exist() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    db.execute(
        "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, 'a')",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(1))),
            ("ctx", Value::Uuid(ctx_a)),
        ]),
    )
    .unwrap();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let result = scoped
        .execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(999)))]),
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn in_list_against_primary_key_under_constraint_returns_visible_subset_no_typed_err() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    for (id, ctx) in [(1, ctx_a), (2, ctx_b), (3, ctx_b)] {
        db.execute(
            "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, $data)",
            &params(vec![
                ("id", Value::Uuid(sarg_uuid(id))),
                ("ctx", Value::Uuid(ctx)),
                ("data", Value::Text(id.to_string())),
            ]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let result = scoped
        .execute(
            "SELECT id FROM t WHERE id IN ($a, $b, $c)",
            &params(vec![
                ("a", Value::Uuid(sarg_uuid(1))),
                ("b", Value::Uuid(sarg_uuid(2))),
                ("c", Value::Uuid(sarg_uuid(3))),
            ]),
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "id"),
        BTreeSet::from([sarg_uuid(1)])
    );
}

#[test]
fn update_delete_predicate_against_hidden_only_rows_preserves_rows_affected_zero() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    db.execute(
        "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, 'b')",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(2))),
            ("ctx", Value::Uuid(ctx_b)),
        ]),
    )
    .unwrap();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let update = scoped
        .execute(
            "UPDATE t SET data = $data WHERE id = $id",
            &params(vec![
                ("data", Value::Text("new".to_string())),
                ("id", Value::Uuid(sarg_uuid(2))),
            ]),
        )
        .unwrap();
    assert_eq!(update.rows_affected, 0);
    let delete = scoped
        .execute(
            "DELETE FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(2)))]),
        )
        .unwrap();
    assert_eq!(delete.rows_affected, 0);
}

#[test]
fn admin_handle_anchor_read_under_no_constraints_returns_ok_rows() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, 'b')",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(2))),
            ("ctx", Value::Uuid(sarg_uuid(0xB))),
        ]),
    )
    .unwrap();
    let result = db
        .execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(2)))]),
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Text("b".to_string())]]);
}

#[test]
fn graph_table_execute_at_snapshot_uses_pinned_snapshot_for_traversal() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n3 = sarg_uuid(3);
    for id in [n1, n2, n3] {
        sarg_insert_node(&db, id, None, None);
    }
    sarg_insert_edge(&db, n1, n2, "T");
    let snapshot = db.snapshot();
    sarg_insert_edge(&db, n1, n3, "T");
    let result = db
        .execute_at_snapshot(
            "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->(b) WHERE a.id = $start COLUMNS (b.id AS target))",
            &params(vec![("start", Value::Uuid(n1))]),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "target"),
        BTreeSet::from([n2])
    );
}

#[test]
fn graph_table_execute_at_snapshot_uses_pinned_snapshot_for_filter_derived_start_nodes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, kind TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n4 = sarg_uuid(4);
    let n5 = sarg_uuid(5);
    sarg_insert_node(&db, n1, None, Some("seed"));
    sarg_insert_node(&db, n2, None, None);
    sarg_insert_node(&db, n4, None, Some("other"));
    sarg_insert_node(&db, n5, None, None);
    sarg_insert_edge(&db, n1, n2, "T");
    sarg_insert_edge(&db, n4, n5, "T");
    let snapshot = db.snapshot();
    db.execute(
        "UPDATE nodes SET kind = 'seed' WHERE id = $id",
        &params(vec![("id", Value::Uuid(n4))]),
    )
    .unwrap();
    let result = db
        .execute_at_snapshot(
            "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->(b) WHERE a.kind = 'seed' COLUMNS (b.id AS target))",
            &empty(),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "target"),
        BTreeSet::from([n2])
    );
}

#[test]
fn graph_table_execute_at_snapshot_uses_pinned_snapshot_for_subquery_derived_start_nodes() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE seeds (id UUID PRIMARY KEY, node_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n3 = sarg_uuid(3);
    let n4 = sarg_uuid(4);
    let n5 = sarg_uuid(5);
    for id in [n1, n2, n3, n4, n5] {
        sarg_insert_node(&db, id, None, None);
    }
    sarg_insert_edge(&db, n1, n2, "T");
    sarg_insert_edge(&db, n4, n5, "T");
    db.execute(
        "INSERT INTO seeds (id, node_id) VALUES ($id, $node)",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(10))),
            ("node", Value::Uuid(n1)),
        ]),
    )
    .unwrap();
    let snapshot = db.snapshot();
    sarg_insert_edge(&db, n1, n3, "T");
    db.execute(
        "INSERT INTO seeds (id, node_id) VALUES ($id, $node)",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(11))),
            ("node", Value::Uuid(n4)),
        ]),
    )
    .unwrap();
    let result = db
        .execute_at_snapshot(
            "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->(b) WHERE a.id IN (SELECT node_id FROM seeds) COLUMNS (b.id AS target))",
            &empty(),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "target"),
        BTreeSet::from([n2])
    );
}

#[test]
fn graph_table_execute_at_snapshot_pins_multi_hop_bfs_depth() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n3 = sarg_uuid(3);
    let n4 = sarg_uuid(4);
    for id in [n1, n2, n3, n4] {
        sarg_insert_node(&db, id, None, None);
    }
    sarg_insert_edge(&db, n1, n2, "T");
    sarg_insert_edge(&db, n2, n3, "T");
    let snapshot = db.snapshot();
    sarg_insert_edge(&db, n2, n4, "T");
    let result = db
        .execute_at_snapshot(
            "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->{1,2}(b) WHERE a.id = $start COLUMNS (b.id AS target))",
            &params(vec![("start", Value::Uuid(n1))]),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "target"),
        BTreeSet::from([n2, n3])
    );
}

#[test]
fn graph_table_execute_at_snapshot_uses_pinned_snapshot_for_unbound_start_fallback() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n3 = sarg_uuid(3);
    let n4 = sarg_uuid(4);
    for id in [n1, n2, n3, n4] {
        sarg_insert_node(&db, id, None, None);
    }
    sarg_insert_edge(&db, n1, n2, "T");
    let snapshot = db.snapshot();
    let tx = db.begin_or_panic();
    db.delete_edge(tx, n1, n2, "T").unwrap();
    db.commit(tx).unwrap();
    sarg_insert_edge(&db, n3, n4, "T");
    let result = db
        .execute_at_snapshot(
            "SELECT source, target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->(b) COLUMNS (a.id AS source, b.id AS target))",
            &empty(),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "source"),
        BTreeSet::from([n1])
    );
    assert_eq!(
        sarg_uuid_column_set(&result, "target"),
        BTreeSet::from([n2])
    );
}

#[test]
fn graph_table_execute_at_snapshot_under_scoped_handle_uses_pinned_snapshot() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, context_id UUID CONTEXT_ID)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let src = sarg_uuid(1);
    let pre_a = sarg_uuid(2);
    let post_a = sarg_uuid(3);
    let pre_b = sarg_uuid(4);
    let post_b = sarg_uuid(5);
    let gate_flip = sarg_uuid(6);
    for (id, ctx) in [
        (src, ctx_a),
        (pre_a, ctx_a),
        (post_a, ctx_a),
        (pre_b, ctx_b),
        (post_b, ctx_b),
        (gate_flip, ctx_a),
    ] {
        sarg_insert_node(&db, id, Some(ctx), None);
    }
    sarg_insert_context_edge(&db, src, pre_a, "T", ctx_a);
    sarg_insert_context_edge(&db, src, pre_b, "T", ctx_b);
    sarg_insert_context_edge(&db, src, gate_flip, "T", ctx_b);
    let snapshot = db.snapshot();
    db.execute(
        "UPDATE edges SET context_id = $ctx WHERE source_id = $src AND target_id = $target AND edge_type = 'T'",
        &params(vec![
            ("ctx", Value::Uuid(ctx_a)),
            ("src", Value::Uuid(src)),
            ("target", Value::Uuid(gate_flip)),
        ]),
    )
    .unwrap();
    sarg_insert_context_edge(&db, src, post_a, "T", ctx_a);
    sarg_insert_context_edge(&db, src, post_b, "T", ctx_b);
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let result = scoped
        .execute_at_snapshot(
            "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->(b) WHERE a.id = $start COLUMNS (b.id AS target))",
            &params(vec![("start", Value::Uuid(src))]),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "target"),
        BTreeSet::from([pre_a])
    );
}

#[test]
fn multi_cte_graph_table_composition_at_pinned_snapshot_via_in_select() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, data TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n3 = sarg_uuid(3);
    let n4 = sarg_uuid(4);
    let n5 = sarg_uuid(5);
    for id in [n1, n2, n3, n4, n5] {
        db.execute(
            "INSERT INTO nodes (id, data) VALUES ($id, $data)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("data", Value::Text(id.to_string())),
            ]),
        )
        .unwrap();
    }
    sarg_insert_edge(&db, n2, n1, "BASED_ON");
    sarg_insert_edge(&db, n3, n1, "ABOUT");
    let snapshot = db.snapshot();
    sarg_insert_edge(&db, n4, n1, "BASED_ON");
    sarg_insert_edge(&db, n5, n1, "ABOUT");
    let sql = "WITH based AS (
            SELECT target AS src FROM GRAPH_TABLE (edges MATCH (b)<-[:BASED_ON]-(a) WHERE b.id = $anchor COLUMNS (a.id AS target))
        ), about AS (
            SELECT target AS src FROM GRAPH_TABLE (edges MATCH (b)<-[:ABOUT]-(a) WHERE b.id = $anchor COLUMNS (a.id AS target))
        )
        SELECT id, data FROM nodes
        WHERE id IN (SELECT src FROM based) OR id IN (SELECT src FROM about)";
    let result = db
        .execute_at_snapshot(sql, &params(vec![("anchor", Value::Uuid(n1))]), snapshot)
        .unwrap();
    assert_eq!(
        sarg_uuid_column_set(&result, "id"),
        BTreeSet::from([n2, n3])
    );
    let based = db
        .execute_at_snapshot(
            "WITH based AS (SELECT target AS src FROM GRAPH_TABLE (edges MATCH (b)<-[:BASED_ON]-(a) WHERE b.id = $anchor COLUMNS (a.id AS target))) SELECT src FROM based",
            &params(vec![("anchor", Value::Uuid(n1))]),
            snapshot,
        )
        .unwrap();
    assert_eq!(sarg_uuid_column_set(&based, "src"), BTreeSet::from([n2]));
    let about = db
        .execute_at_snapshot(
            "WITH about AS (SELECT target AS src FROM GRAPH_TABLE (edges MATCH (b)<-[:ABOUT]-(a) WHERE b.id = $anchor COLUMNS (a.id AS target))) SELECT src FROM about",
            &params(vec![("anchor", Value::Uuid(n1))]),
            snapshot,
        )
        .unwrap();
    assert_eq!(sarg_uuid_column_set(&about, "src"), BTreeSet::from([n3]));
}

#[test]
fn graph_table_at_pinned_snapshot_with_no_pre_pin_edges_returns_ok_empty() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
    let n1 = sarg_uuid(1);
    let n2 = sarg_uuid(2);
    let n3 = sarg_uuid(3);
    for id in [n1, n2, n3] {
        sarg_insert_node(&db, id, None, None);
    }
    let snapshot = db.snapshot();
    sarg_insert_edge(&db, n1, n2, "T");
    sarg_insert_edge(&db, n1, n3, "T");
    let result = db
        .execute_at_snapshot(
            "SELECT target FROM GRAPH_TABLE (edges MATCH (a)-[:T]->(b) WHERE a.id = $start COLUMNS (b.id AS target))",
            &params(vec![("start", Value::Uuid(n1))]),
            snapshot,
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn scoped_anchor_read_at_pinned_snapshot_reports_context_scope_violation_at_pin_state() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    db.execute(
        "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, 'b')",
        &params(vec![
            ("id", Value::Uuid(sarg_uuid(2))),
            ("ctx", Value::Uuid(ctx_b)),
        ]),
    )
    .unwrap();
    let snapshot = db.snapshot();
    db.execute(
        "DELETE FROM t WHERE id = $id",
        &params(vec![("id", Value::Uuid(sarg_uuid(2)))]),
    )
    .unwrap();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let err = scoped
        .execute_at_snapshot(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(2)))]),
            snapshot,
        )
        .expect_err("hidden row at pinned snapshot must be refused");
    sarg_expect_context_violation(err, ctx_b, &[ctx_a]);
}

#[test]
fn scoped_multi_cte_graph_table_with_cross_scope_anchor_parameter_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, context_id UUID CONTEXT_ID)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let visible_anchor = sarg_uuid(1);
    let visible_source = sarg_uuid(2);
    let hidden_anchor = sarg_uuid(3);
    let hidden_source = sarg_uuid(4);
    for (id, ctx, data) in [
        (visible_anchor, ctx_a, "visible-anchor"),
        (visible_source, ctx_a, "visible-source"),
        (hidden_anchor, ctx_b, "hidden-anchor"),
        (hidden_source, ctx_b, "hidden-source"),
    ] {
        db.execute(
            "INSERT INTO nodes (id, context_id, data) VALUES ($id, $ctx, $data)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("ctx", Value::Uuid(ctx)),
                ("data", Value::Text(data.to_string())),
            ]),
        )
        .unwrap();
    }
    sarg_insert_context_edge(&db, visible_source, visible_anchor, "VISIBLE", ctx_a);
    sarg_insert_context_edge(&db, hidden_source, hidden_anchor, "HIDDEN", ctx_b);
    let snapshot = db.snapshot();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    let sql = "WITH visible AS (
            SELECT target AS src FROM GRAPH_TABLE (edges MATCH (b)<-[:VISIBLE]-(a) WHERE b.id = $visible_anchor COLUMNS (a.id AS target))
        ), hidden AS (
            SELECT target AS src FROM GRAPH_TABLE (edges MATCH (b)<-[:HIDDEN]-(a) WHERE b.id = $hidden_anchor COLUMNS (a.id AS target))
        )
        SELECT id FROM nodes
        WHERE id IN (SELECT src FROM visible) OR id IN (SELECT src FROM hidden)";
    let err = scoped
        .execute_at_snapshot(
            sql,
            &params(vec![
                ("visible_anchor", Value::Uuid(visible_anchor)),
                ("hidden_anchor", Value::Uuid(hidden_anchor)),
            ]),
            snapshot,
        )
        .expect_err("cross-scope graph anchor in any CTE must be refused");
    sarg_expect_context_violation(err, ctx_b, &[ctx_a]);
}

#[test]
fn anchor_read_of_sync_replicated_row_under_scoped_handle_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let ctx_c = sarg_uuid(0xC);
    db.apply_changes(
        sync::ChangeSet {
            rows: vec![
                sync::RowChange {
                    table: "t".to_string(),
                    natural_key: sync::NaturalKey {
                        column: "id".to_string(),
                        value: Value::Uuid(sarg_uuid(2)),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(sarg_uuid(2))),
                        ("context_id".to_string(), Value::Uuid(ctx_b)),
                        ("data".to_string(), Value::Text("b".to_string())),
                    ]),
                    deleted: false,
                    lsn: Lsn(1),
                },
                sync::RowChange {
                    table: "t".to_string(),
                    natural_key: sync::NaturalKey {
                        column: "id".to_string(),
                        value: Value::Uuid(sarg_uuid(3)),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(sarg_uuid(3))),
                        ("context_id".to_string(), Value::Uuid(ctx_c)),
                        ("data".to_string(), Value::Text("c".to_string())),
                    ]),
                    deleted: false,
                    lsn: Lsn(2),
                },
            ],
            ..sync::ChangeSet::default()
        },
        &sync::ConflictPolicies::uniform(sync::ConflictPolicy::LatestWins),
    )
    .unwrap();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    for (id, ctx) in [(sarg_uuid(2), ctx_b), (sarg_uuid(3), ctx_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE id = $id",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect_err("sync-replicated hidden anchor must be refused");
        sarg_expect_context_violation(err, ctx, &[ctx_a]);
    }
}

#[test]
fn typed_error_display_messages_describe_hidden_by_scope_with_actionable_phrasing() {
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let context = Error::ContextScopeViolation {
        requested: ContextId::new(ctx_b),
        allowed: sarg_contexts(&[ctx_a]),
    };
    let msg = context.to_string();
    assert!(msg.to_lowercase().contains("hidden") && msg.to_lowercase().contains("scope"));
    assert!(msg.contains(&ctx_b.to_string()));
    assert!(msg.contains('{') && msg.contains(&ctx_a.to_string()) && msg.contains('}'));
    assert!(!msg.contains("ContextId(") && !msg.contains("Uuid(") && !msg.contains("BTreeSet"));

    let missing = Error::ContextScopeViolation {
        requested: ContextId::new(Uuid::from_u128(u128::MAX)),
        allowed: sarg_contexts(&[ctx_a]),
    }
    .to_string();
    assert!(missing.to_lowercase().contains("no context"));

    let scope = Error::ScopeLabelViolation {
        requested: ScopeLabel::new("b"),
        allowed: sarg_labels(&["a"]),
    };
    let msg = scope.to_string();
    assert!(msg.to_lowercase().contains("hidden") && msg.to_lowercase().contains("scope label"));
    assert!(msg.contains("{a}"));
    assert!(!msg.contains("ScopeLabel(") && !msg.contains("BTreeSet"));

    let acl = Error::AclDenied {
        table: "t".to_string(),
        row_id: RowId(7),
        principal: Principal::Agent("agent-a".to_string()),
    };
    let msg = acl.to_string();
    assert!(msg.to_lowercase().contains("hidden") && msg.to_lowercase().contains("acl"));
    assert!(msg.contains("t"));
}

#[test]
fn scoped_anchor_read_with_degenerate_input_returns_error_or_empty_no_panic() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    let scoped = db.scoped_with_contexts(sarg_contexts(&[sarg_uuid(0xA)]));
    let missing = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scoped.execute(
            "SELECT data FROM nonexistent_table WHERE id = $id",
            &params(vec![("id", Value::Uuid(sarg_uuid(1)))]),
        )
    }))
    .expect("nonexistent table query must not panic");
    assert!(matches!(missing, Err(Error::TableNotFound(_))));

    let wrong_type = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scoped.execute(
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Text("not-a-uuid".to_string()))]),
        )
    }))
    .expect("wrong typed parameter query must not panic")
    .unwrap();
    assert!(wrong_type.rows.is_empty());
}

#[test]
fn trigger_written_cross_context_row_anchor_read_under_scoped_handle_reports_typed_violation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, kind TEXT, context_id UUID CONTEXT_ID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE TRIGGER tr ON obs WHEN INSERT", &empty())
        .unwrap();
    let ctx_a = sarg_uuid(0xA);
    let ctx_b = sarg_uuid(0xB);
    let ctx_c = sarg_uuid(0xC);
    db.register_trigger_callback("tr", move |db_handle, ctx| {
        let id = ctx
            .row_values
            .get("id")
            .and_then(Value::as_uuid)
            .copied()
            .ok_or_else(|| Error::Other("obs id missing".to_string()))?;
        let target_ctx = if id == sarg_uuid(21) { ctx_b } else { ctx_c };
        db_handle.insert_row(
            ctx.tx,
            "t",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("context_id".to_string(), Value::Uuid(target_ctx)),
                ("data".to_string(), Value::Text("triggered".to_string())),
            ]),
        )?;
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();
    for id in [sarg_uuid(21), sarg_uuid(22)] {
        db.execute(
            "INSERT INTO obs (id, kind, context_id) VALUES ($id, 'k', $ctx)",
            &params(vec![("id", Value::Uuid(id)), ("ctx", Value::Uuid(ctx_a))]),
        )
        .unwrap();
    }
    let scoped = db.scoped_with_contexts(sarg_contexts(&[ctx_a]));
    for (id, ctx) in [(sarg_uuid(21), ctx_b), (sarg_uuid(22), ctx_c)] {
        let err = scoped
            .execute(
                "SELECT data FROM t WHERE id = $id",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect_err("trigger-written hidden anchor must be refused");
        sarg_expect_context_violation(err, ctx, &[ctx_a]);
    }
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

#[test]
fn upd_04_stale_conditional_update_in_explicit_tx_commits_as_noop_atomically() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();
    db.execute(
        "CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO tasks (id, status) VALUES ($id, 'pending')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let tx_a = db.begin_or_panic();
    let tx_b = db.begin_or_panic();
    let stale_predicate = params(vec![
        ("id", Value::Uuid(id)),
        ("pending", Value::Text("pending".to_string())),
        ("done", Value::Text("done".to_string())),
    ]);

    let a = db
        .execute_in_tx(
            tx_a,
            "UPDATE tasks SET status = $done WHERE id = $id AND status = $pending",
            &stale_predicate,
        )
        .unwrap();
    let b = db
        .execute_in_tx(
            tx_b,
            "UPDATE tasks SET status = $done WHERE id = $id AND status = $pending",
            &stale_predicate,
        )
        .unwrap();
    assert_eq!(a.rows_affected, 1);
    assert_eq!(b.rows_affected, 1);

    db.commit(tx_a).expect("first conditional update commits");
    db.commit(tx_b)
        .expect("ordinary stale conditional update commits as commit-time no-op");

    let row = db
        .point_lookup("tasks", "id", &Value::Uuid(id), db.snapshot())
        .unwrap()
        .expect("task must still exist");
    assert_eq!(
        row.values.get("status"),
        Some(&Value::Text("done".to_string()))
    );
}

#[test]
fn upd_05_row_condition_guard_in_explicit_tx_fails_commit_atomically() {
    let db = Database::open_memory();
    let id = Uuid::new_v4();
    db.execute(
        "CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE task_audit (id UUID PRIMARY KEY, task_id UUID, note TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO tasks (id, status) VALUES ($id, 'active')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let tx_guarded = db.begin_or_panic();
    assert!(
        db.guard_row_conditions_in_tx(
            tx_guarded,
            "tasks",
            "id",
            &Value::Uuid(id),
            &[("status".to_string(), Value::Text("active".to_string()))],
        )
        .expect("register guard")
    );
    db.execute_in_tx(
        tx_guarded,
        "INSERT INTO task_audit (id, task_id, note) VALUES ($id, $task_id, 'guarded side effect')",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("task_id", Value::Uuid(id)),
        ]),
    )
    .unwrap();

    db.execute(
        "UPDATE tasks SET status = 'archived' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let err = db
        .commit(tx_guarded)
        .expect_err("stale guarded transaction must fail");
    assert!(
        matches!(err, Error::ConditionalUpdateConflict { count: 1 }),
        "expected ConditionalUpdateConflict, got {err:?}"
    );
    let audit_rows = db
        .execute(
            "SELECT id FROM task_audit WHERE task_id = $task_id",
            &params(vec![("task_id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(
        audit_rows.rows.len(),
        0,
        "guarded side effects must roll back with the stale transaction"
    );
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
        contextdb_engine::sync_types::DdlChange::CreateIndex { table, .. }
        | contextdb_engine::sync_types::DdlChange::DropIndex { table, .. } => table.clone(),
        contextdb_engine::sync_types::DdlChange::CreateTrigger { table, .. } => table.clone(),
        contextdb_engine::sync_types::DdlChange::DropTrigger { name } => name.clone(),
        contextdb_engine::sync_types::DdlChange::CreateEventType { table, .. } => table.clone(),
        contextdb_engine::sync_types::DdlChange::CreateSink { name, .. }
        | contextdb_engine::sync_types::DdlChange::CreateRoute { name, .. }
        | contextdb_engine::sync_types::DdlChange::DropRoute { name, .. } => name.clone(),
    }
}

fn max_non_ddl_lsn(changes: &contextdb_engine::sync_types::ChangeSet) -> Option<Lsn> {
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
        let mut watermark = Lsn(0);
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
        let mut watermark = Lsn(0);
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
        let mut watermark = Lsn(0);
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
    let changes = edge.changes_since(Lsn(0));

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

    let tx = db.begin_or_panic();
    assert!(
        db.insert_edge(tx, source, target, "REL".to_string(), HashMap::new())
            .unwrap()
    );
    db.commit(tx).unwrap();

    let used_after_insert = accountant.usage().used;
    assert!(used_after_insert > baseline);

    let tx = db.begin_or_panic();
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

    db.execute("SET DISK_LIMIT '1K'", &empty()).unwrap();

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

    db.execute("SET DISK_LIMIT '1K'", &empty()).unwrap();

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

// ======== T20 ========
#[test]
fn test_coerce_uuid_name_based_still_works_post_catchall_removal() {
    use contextdb_core::Value;
    use contextdb_engine::Database;
    use std::collections::HashMap;
    use uuid::Uuid;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id INTEGER, user_id INTEGER, other_id INTEGER, plain INTEGER)",
        &empty,
    )
    .expect("CREATE TABLE with four INTEGER columns must succeed");

    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    let expected_uuid = Uuid::parse_str(uuid_str).expect("fixture uuid must parse");

    let mut row: HashMap<String, Value> = HashMap::new();
    row.insert("id".to_string(), Value::Text(uuid_str.into()));
    row.insert("user_id".to_string(), Value::Text(uuid_str.into()));
    row.insert("other_id".to_string(), Value::Text(uuid_str.into()));
    row.insert("plain".to_string(), Value::Text(uuid_str.into()));

    db.execute(
        "INSERT INTO t (id, user_id, other_id, plain) VALUES ($id, $user_id, $other_id, $plain)",
        &row,
    )
    .expect("INSERT must succeed under UUID name-based coercion");

    let result = db
        .execute("SELECT * FROM t", &empty)
        .expect("SELECT * FROM t must succeed");
    assert_eq!(result.rows.len(), 1, "exactly one row expected");
    let row_vals = &result.rows[0];
    let col_idx = |name: &str| {
        result
            .columns
            .iter()
            .position(|c| c == name)
            .unwrap_or_else(|| panic!("column {name:?} must exist in result"))
    };

    assert_eq!(
        row_vals[col_idx("id")],
        Value::Uuid(expected_uuid),
        "id column must coerce Text → Uuid",
    );
    assert_eq!(
        row_vals[col_idx("user_id")],
        Value::Uuid(expected_uuid),
        "user_id column must coerce Text → Uuid (name ends in _id)",
    );
    assert_eq!(
        row_vals[col_idx("other_id")],
        Value::Uuid(expected_uuid),
        "other_id column must coerce Text → Uuid (name ends in _id)",
    );
    assert_eq!(
        row_vals[col_idx("plain")],
        Value::Text(uuid_str.into()),
        "plain column (no _id suffix) must retain Value::Text without coercion",
    );
}

// ======== T21 ========
#[test]
fn where_txid_bound_int64_returns_rows() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    // Drive watermark past 200 so inserts are allowed.
    db.execute("CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)", &empty)
        .unwrap();
    for n in 0..250i64 {
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("id".to_string(), Value::Uuid(uuid::Uuid::new_v4()));
        r.insert("n".to_string(), Value::Int64(n));
        db.execute("INSERT INTO bump (id, n) VALUES ($id, $n)", &r)
            .unwrap();
    }

    // Insert the three TxId rows via library API.
    for tx_val in &[TxId(10), TxId(50), TxId(200)] {
        let tx = db.begin_or_panic();
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("x".to_string(), Value::TxId(*tx_val));
        db.insert_row(tx, "t", r)
            .unwrap_or_else(|e| panic!("insert Value::TxId({tx_val:?}) must succeed: {e:?}"));
        db.commit(tx).expect("commit must succeed");
    }

    let mut bind: HashMap<String, Value> = HashMap::new();
    bind.insert("bound".to_string(), Value::Int64(100));
    let result = db
        .execute("SELECT * FROM t WHERE x > $bound", &bind)
        .expect("SELECT with bound Int64 must succeed on TXID column");

    assert_eq!(
        result.rows.len(),
        1,
        "exactly one row must match x > 100 (only TxId(200))",
    );
    let x_idx = result
        .columns
        .iter()
        .position(|c| c == "x")
        .expect("result must have column \"x\"");
    assert_eq!(
        result.rows[0][x_idx],
        Value::TxId(TxId(200)),
        "the one matching row must be Value::TxId(TxId(200))",
    );
}

// ======== T22 ========
#[test]
fn where_txid_negative_literal_below_all() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    // Bump watermark past 200.
    db.execute("CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)", &empty)
        .unwrap();
    for n in 0..250i64 {
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("id".to_string(), Value::Uuid(uuid::Uuid::new_v4()));
        r.insert("n".to_string(), Value::Int64(n));
        db.execute("INSERT INTO bump (id, n) VALUES ($id, $n)", &r)
            .unwrap();
    }

    for tx_val in &[TxId(10), TxId(50), TxId(200)] {
        let tx = db.begin_or_panic();
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("x".to_string(), Value::TxId(*tx_val));
        db.insert_row(tx, "t", r)
            .unwrap_or_else(|e| panic!("insert Value::TxId({tx_val:?}) must succeed: {e:?}"));
        db.commit(tx).expect("commit must succeed");
    }

    let mut bind: HashMap<String, Value> = HashMap::new();
    bind.insert("bound".to_string(), Value::Int64(-1));
    let result = db
        .execute("SELECT COUNT(*) AS c FROM t WHERE x > $bound", &bind)
        .expect("COUNT(*) with negative bound must succeed on TXID column");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(*) result must have exactly one row",
    );
    let c_idx = result
        .columns
        .iter()
        .position(|c| c == "c")
        .expect("result must have the aliased count column \"c\"");
    assert_eq!(
        result.rows[0][c_idx],
        Value::Int64(3),
        "COUNT(*) of TxIds > -1 must equal 3 (all three rows match)",
    );
}

// ======== T23 ========
#[test]
fn where_txid_text_literal_returns_no_rows() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    // Bump watermark past 42.
    db.execute("CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)", &empty)
        .unwrap();
    for n in 0..50i64 {
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("id".to_string(), Value::Uuid(uuid::Uuid::new_v4()));
        r.insert("n".to_string(), Value::Int64(n));
        db.execute("INSERT INTO bump (id, n) VALUES ($id, $n)", &r)
            .unwrap();
    }

    let tx = db.begin_or_panic();
    let mut r: HashMap<String, Value> = HashMap::new();
    r.insert("x".to_string(), Value::TxId(TxId(42)));
    db.insert_row(tx, "t", r)
        .expect("insert Value::TxId(TxId(42)) must succeed");
    db.commit(tx).expect("commit must succeed");

    // Positive control: prove the row is actually present with x = Value::TxId(TxId(42))
    // before we test the text-bind negative case. This prevents a stub or regression
    // that silently drops inserts from trivially satisfying the "0 rows" assertion below.
    let unfiltered = db
        .execute("SELECT x FROM t", &empty)
        .expect("SELECT x FROM t (no filter) must succeed");
    assert_eq!(
        unfiltered.rows.len(),
        1,
        "positive control: exactly one row must be present after insert",
    );
    assert_eq!(
        unfiltered.rows[0][0],
        Value::TxId(TxId(42)),
        "positive control: the stored x column must be Value::TxId(TxId(42))",
    );

    let mut bind: HashMap<String, Value> = HashMap::new();
    bind.insert("bound".to_string(), Value::Text("42".into()));
    let result = db
        .execute("SELECT * FROM t WHERE x = $bound", &bind)
        .expect("SELECT with Text bound on TXID column must not error — just return empty");

    assert_eq!(
        result.rows.len(),
        0,
        "no rows must be returned — Text must never coerce to TxId; got rows: {:?}",
        result.rows,
    );
}

// ======== T24 ========
#[test]
fn insert_sql_literal_int_into_txid_rejected() {
    use contextdb_core::{Error, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    let err = db
        .execute("INSERT INTO t (x) VALUES (42)", &empty)
        .expect_err("INSERT INTO t (x) VALUES (42) must be rejected — literal Int64 into TXID");

    match err {
        Error::ColumnTypeMismatch {
            table,
            column,
            expected,
            actual,
        } => {
            assert_eq!(table, "t", "error.table must be \"t\"");
            assert_eq!(column, "x", "error.column must be \"x\"");
            assert_eq!(expected, "TXID", "error.expected must be \"TXID\"");
            assert_eq!(
                actual, "Int64",
                "error.actual must be \"Int64\" (SQL literal 42 parses to Value::Int64)",
            );
        }
        other => panic!("expected Error::ColumnTypeMismatch, got {other:?}",),
    }
}

// ======== T25 ========
#[test]
fn orderby_txid_asc_desc() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use std::collections::HashMap;

    let empty: HashMap<String, Value> = HashMap::new();
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (x TXID NOT NULL)", &empty)
        .expect("CREATE TABLE t (x TXID NOT NULL) must parse");

    // Bump watermark past 42.
    db.execute("CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)", &empty)
        .unwrap();
    for n in 0..50i64 {
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("id".to_string(), Value::Uuid(uuid::Uuid::new_v4()));
        r.insert("n".to_string(), Value::Int64(n));
        db.execute("INSERT INTO bump (id, n) VALUES ($id, $n)", &r)
            .unwrap();
    }

    // Insert out of order: 7, 1, 42, 3.
    for tx_val in &[TxId(7), TxId(1), TxId(42), TxId(3)] {
        let tx = db.begin_or_panic();
        let mut r: HashMap<String, Value> = HashMap::new();
        r.insert("x".to_string(), Value::TxId(*tx_val));
        db.insert_row(tx, "t", r)
            .unwrap_or_else(|e| panic!("insert Value::TxId({tx_val:?}) must succeed: {e:?}"));
        db.commit(tx).expect("commit must succeed");
    }

    // ASC
    let asc = db
        .execute("SELECT x FROM t ORDER BY x ASC", &empty)
        .expect("SELECT ... ORDER BY x ASC must succeed");
    let x_idx = asc
        .columns
        .iter()
        .position(|c| c == "x")
        .expect("asc result must have column \"x\"");
    let asc_vals: Vec<Value> = asc.rows.iter().map(|r| r[x_idx].clone()).collect();
    assert_eq!(
        asc_vals,
        vec![
            Value::TxId(TxId(1)),
            Value::TxId(TxId(3)),
            Value::TxId(TxId(7)),
            Value::TxId(TxId(42)),
        ],
        "ORDER BY x ASC must sort TxIds by native u64::cmp",
    );

    // DESC
    let desc = db
        .execute("SELECT x FROM t ORDER BY x DESC", &empty)
        .expect("SELECT ... ORDER BY x DESC must succeed");
    let x_idx_d = desc
        .columns
        .iter()
        .position(|c| c == "x")
        .expect("desc result must have column \"x\"");
    let desc_vals: Vec<Value> = desc.rows.iter().map(|r| r[x_idx_d].clone()).collect();
    assert_eq!(
        desc_vals,
        vec![
            Value::TxId(TxId(42)),
            Value::TxId(TxId(7)),
            Value::TxId(TxId(3)),
            Value::TxId(TxId(1)),
        ],
        "ORDER BY x DESC must reverse the ASC sequence",
    );
}

#[cfg(feature = "test-seams")]
const PER_INDEX_SQL_ROWS: usize = 1024;
const PER_INDEX_SQL_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "test-seams")]
fn per_index_ranked3(rank: usize) -> Vec<f32> {
    let score = (1.0 - rank as f32 * 0.0005).clamp(0.05, 1.0);
    vec![score, (1.0 - score * score).max(0.0).sqrt(), 0.0]
}

fn per_index_axis3(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis.min(2)] = 1.0;
    vector
}

fn per_index_top_id(db: &Database, table: &str, column: &str, query: Vec<f32>) -> Uuid {
    let sql = format!("SELECT id FROM {table} ORDER BY {column} <=> $query LIMIT 1");
    let result = db
        .execute(&sql, &params(vec![("query", Value::Vector(query))]))
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0][0] {
        Value::Uuid(id) => id,
        ref other => panic!("expected UUID id, got {other:?}"),
    }
}

#[cfg(feature = "test-seams")]
fn seed_two_vector_evidence(db: &Database) -> Vec<Uuid> {
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(3), vector_vision VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let ids = (0..PER_INDEX_SQL_ROWS)
        .map(|i| Uuid::from_u128(10_000 + i as u128))
        .collect::<Vec<_>>();
    for (i, id) in ids.iter().copied().enumerate() {
        db.execute(
            "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $text, $vision)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("text", Value::Vector(per_index_ranked3(i))),
                (
                    "vision",
                    Value::Vector(vec![0.0, 1.0 - i as f32 * 0.0001, 0.0]),
                ),
            ]),
        )
        .unwrap();
    }
    ids
}

#[test]
fn multi_ref_tx_mid_apply_state_invisible_post_commit_state_visible() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE table_text (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE table_face (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let id_text = Uuid::from_u128(1);
    let id_face = Uuid::from_u128(2);
    let pause = db.pause_after_relational_apply_for_test();
    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        let tx = writer_db.begin().unwrap();
        writer_db
            .execute_in_tx(
                tx,
                "INSERT INTO table_text (id, embedding) VALUES ($id, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(id_text)),
                    ("embedding", Value::Vector(per_index_axis3(0))),
                ]),
            )
            .unwrap();
        writer_db
            .execute_in_tx(
                tx,
                "INSERT INTO table_face (id, embedding) VALUES ($id, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(id_face)),
                    ("embedding", Value::Vector(per_index_axis3(1))),
                ]),
            )
            .unwrap();
        writer_db.commit(tx).unwrap();
    });

    assert!(pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));
    assert_eq!(
        db.execute("SELECT id FROM table_text", &empty())
            .unwrap()
            .rows
            .len(),
        0
    );
    assert_eq!(
        db.execute("SELECT id FROM table_face", &empty())
            .unwrap()
            .rows
            .len(),
        0
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM table_text ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(per_index_axis3(0)))])
        )
        .unwrap()
        .rows
        .len(),
        0
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM table_face ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(per_index_axis3(1)))])
        )
        .unwrap()
        .rows
        .len(),
        0
    );

    pause.release();
    writer.join().unwrap();
    assert_eq!(
        per_index_top_id(&db, "table_text", "embedding", per_index_axis3(0)),
        id_text
    );
    assert_eq!(
        per_index_top_id(&db, "table_face", "embedding", per_index_axis3(1)),
        id_face
    );
}

#[test]
fn multi_ref_tx_abort_leaves_no_vectors_visible() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE table_text (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE table_face (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO table_text (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(11))),
            ("embedding", Value::Vector(per_index_axis3(0))),
        ]),
    )
    .unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO table_face (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(12))),
            ("embedding", Value::Vector(per_index_axis3(1))),
        ]),
    )
    .unwrap();
    db.rollback(tx).unwrap();

    assert_eq!(
        db.execute("SELECT id FROM table_text", &empty())
            .unwrap()
            .rows
            .len(),
        0
    );
    assert_eq!(
        db.execute("SELECT id FROM table_face", &empty())
            .unwrap()
            .rows
            .len(),
        0
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM table_text ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(per_index_axis3(0)))])
        )
        .unwrap()
        .rows
        .len(),
        0
    );
}

#[test]
fn concurrent_distinct_table_sql_updates_yield_correct_vector_recall() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE table_text (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE table_face (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let text_id = Uuid::from_u128(21);
    let face_id = Uuid::from_u128(22);
    db.execute(
        "INSERT INTO table_text (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(text_id)),
            ("embedding", Value::Vector(per_index_axis3(1))),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO table_face (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(face_id)),
            ("embedding", Value::Vector(per_index_axis3(0))),
        ]),
    )
    .unwrap();

    let db_text = db.clone();
    let text_worker = thread::spawn(move || {
        db_text
            .execute(
                "UPDATE table_text SET embedding = $embedding WHERE id = $id",
                &params(vec![
                    ("id", Value::Uuid(text_id)),
                    ("embedding", Value::Vector(per_index_axis3(0))),
                ]),
            )
            .unwrap();
    });
    let db_face = db.clone();
    let face_worker = thread::spawn(move || {
        db_face
            .execute(
                "UPDATE table_face SET embedding = $embedding WHERE id = $id",
                &params(vec![
                    ("id", Value::Uuid(face_id)),
                    ("embedding", Value::Vector(per_index_axis3(1))),
                ]),
            )
            .unwrap();
    });
    text_worker.join().unwrap();
    face_worker.join().unwrap();

    assert_eq!(
        per_index_top_id(&db, "table_text", "embedding", per_index_axis3(0)),
        text_id
    );
    assert_eq!(
        per_index_top_id(&db, "table_face", "embedding", per_index_axis3(1)),
        face_id
    );
}

#[cfg(feature = "test-seams")]
#[test]
fn same_table_vision_sql_update_and_search_completes_while_text_build_paused() {
    use contextdb_vector::test_seam::PauseWindow;

    let db = Arc::new(Database::open_memory());
    let ids = seed_two_vector_evidence(&db);
    let text_ref = VectorIndexRef::new("evidence", "vector_text");
    let vision_ref = VectorIndexRef::new("evidence", "vector_vision");
    let vector_store = db.vector_store_for_test();
    let text_pause = vector_store.arm_maintenance_pause_for_test(&text_ref, PauseWindow::Build);
    let (done_text_tx, done_text_rx) = mpsc::channel();
    let db_text = db.clone();
    thread::spawn(move || {
        done_text_tx
            .send(per_index_top_id(
                &db_text,
                "evidence",
                "vector_text",
                per_index_axis3(0),
            ))
            .unwrap();
    });
    assert!(text_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));

    let target_id = ids[3];
    let (done_vision_tx, done_vision_rx) = mpsc::channel();
    let db_vision = db.clone();
    thread::spawn(move || {
        db_vision
            .execute(
                "UPDATE evidence SET vector_vision = $vision WHERE id = $id",
                &params(vec![
                    ("id", Value::Uuid(target_id)),
                    ("vision", Value::Vector(per_index_axis3(2))),
                ]),
            )
            .unwrap();
        done_vision_tx
            .send(per_index_top_id(
                &db_vision,
                "evidence",
                "vector_vision",
                per_index_axis3(2),
            ))
            .unwrap();
    });
    let vision_first = done_vision_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);
    let vision_hnsw_before_release = vector_store.has_hnsw_index_for(&vision_ref);
    let text_still_paused = done_text_rx.try_recv();
    text_pause.release();
    let _ = done_text_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT).unwrap();
    let vision_id = match vision_first {
        Ok(id) => id,
        Err(_) => done_vision_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT).unwrap(),
    };

    assert_eq!(vision_id, target_id);
    assert!(vision_hnsw_before_release);
    assert!(matches!(text_still_paused, Err(TryRecvError::Empty)));
    assert!(vector_store.has_hnsw_index_for(&text_ref));
}

#[cfg(feature = "test-seams")]
#[test]
fn alter_table_drop_vector_column_waits_for_inflight_build_then_removes_index() {
    use contextdb_vector::test_seam::PauseWindow;

    let db = Arc::new(Database::open_memory());
    seed_two_vector_evidence(&db);
    let text_ref = VectorIndexRef::new("evidence", "vector_text");
    let vector_store = db.vector_store_for_test();
    let build_pause = vector_store.arm_maintenance_pause_for_test(&text_ref, PauseWindow::Build);
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let db_build = db.clone();
    thread::spawn(move || {
        done_build_tx
            .send(db_build.execute(
                "SELECT id FROM evidence ORDER BY vector_text <=> $query LIMIT 1",
                &params(vec![("query", Value::Vector(per_index_axis3(0)))]),
            ))
            .unwrap();
    });
    assert!(build_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));

    let ddl_pause = vector_store.arm_maintenance_pause_for_test(&text_ref, PauseWindow::Ddl);
    let (started_drop_tx, started_drop_rx) = mpsc::channel();
    let (done_drop_tx, done_drop_rx) = mpsc::channel();
    let db_drop = db.clone();
    thread::spawn(move || {
        started_drop_tx.send(()).unwrap();
        let result = db_drop.execute("ALTER TABLE evidence DROP COLUMN vector_text", &empty());
        done_drop_tx.send(result).unwrap();
    });
    assert!(started_drop_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT).is_ok());
    assert!(ddl_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));
    let old_shape_during_pause = db
        .execute("SELECT vector_text FROM evidence LIMIT 1", &empty())
        .is_ok();
    let vision_shape_during_pause = db
        .execute("SELECT vector_vision FROM evidence LIMIT 1", &empty())
        .is_ok();
    let ref_present_during_pause = vector_store
        .index_infos()
        .iter()
        .any(|info| info.index == text_ref);
    assert!(matches!(done_drop_rx.try_recv(), Err(TryRecvError::Empty)));
    ddl_pause.release();
    let drop_done_before_build_release = done_drop_rx.try_recv();
    let old_shape_after_ddl_release = db
        .execute("SELECT vector_text FROM evidence LIMIT 1", &empty())
        .is_ok();
    build_pause.release();
    let build_result = done_build_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);
    let drop_result = done_drop_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);

    assert!(old_shape_during_pause);
    assert!(vision_shape_during_pause);
    assert!(ref_present_during_pause);
    assert!(matches!(
        drop_done_before_build_release,
        Err(TryRecvError::Empty)
    ));
    assert!(old_shape_after_ddl_release);
    assert!(
        db.execute("SELECT vector_text FROM evidence LIMIT 1", &empty())
            .is_err()
    );
    build_result.unwrap().unwrap();
    drop_result.unwrap().unwrap();
    assert_eq!(
        per_index_top_id(&db, "evidence", "vector_vision", per_index_axis3(1)),
        Uuid::from_u128(10_000)
    );
    assert!(!vector_store.has_hnsw_index_for(&text_ref));
}

#[cfg(feature = "test-seams")]
#[test]
fn alter_table_rename_vector_column_waits_for_inflight_build_then_moves_index() {
    use contextdb_vector::test_seam::PauseWindow;

    let db = Arc::new(Database::open_memory());
    seed_two_vector_evidence(&db);
    let old_ref = VectorIndexRef::new("evidence", "vector_text");
    let new_ref = VectorIndexRef::new("evidence", "vector_text_v2");
    let vector_store = db.vector_store_for_test();
    let build_pause = vector_store.arm_maintenance_pause_for_test(&old_ref, PauseWindow::Build);
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let db_build = db.clone();
    thread::spawn(move || {
        done_build_tx
            .send(db_build.execute(
                "SELECT id FROM evidence ORDER BY vector_text <=> $query LIMIT 1",
                &params(vec![("query", Value::Vector(per_index_axis3(0)))]),
            ))
            .unwrap();
    });
    assert!(build_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));

    let ddl_pause = vector_store.arm_maintenance_pause_for_test(&old_ref, PauseWindow::Ddl);
    let (started_rename_tx, started_rename_rx) = mpsc::channel();
    let (done_rename_tx, done_rename_rx) = mpsc::channel();
    let db_rename = db.clone();
    thread::spawn(move || {
        started_rename_tx.send(()).unwrap();
        let result = db_rename.execute(
            "ALTER TABLE evidence RENAME COLUMN vector_text TO vector_text_v2",
            &empty(),
        );
        done_rename_tx.send(result).unwrap();
    });
    assert!(
        started_rename_rx
            .recv_timeout(PER_INDEX_SQL_TIMEOUT)
            .is_ok()
    );
    assert!(ddl_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));
    let old_shape_during_pause = db
        .execute("SELECT vector_text FROM evidence LIMIT 1", &empty())
        .is_ok();
    let new_shape_during_pause = db
        .execute("SELECT vector_text_v2 FROM evidence LIMIT 1", &empty())
        .is_ok();
    let old_ref_present_during_pause = vector_store
        .index_infos()
        .iter()
        .any(|info| info.index == old_ref);
    assert!(matches!(
        done_rename_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    ddl_pause.release();
    let rename_done_before_build_release = done_rename_rx.try_recv();
    let old_shape_after_ddl_release = db
        .execute("SELECT vector_text FROM evidence LIMIT 1", &empty())
        .is_ok();
    let new_shape_after_ddl_release = db
        .execute("SELECT vector_text_v2 FROM evidence LIMIT 1", &empty())
        .is_ok();
    build_pause.release();
    let build_result = done_build_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);
    let rename_result = done_rename_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);

    assert!(old_shape_during_pause);
    assert!(!new_shape_during_pause);
    assert!(old_ref_present_during_pause);
    assert!(matches!(
        rename_done_before_build_release,
        Err(TryRecvError::Empty)
    ));
    assert!(old_shape_after_ddl_release);
    assert!(!new_shape_after_ddl_release);
    assert!(
        db.execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(per_index_axis3(0)))])
        )
        .is_err()
    );
    build_result.unwrap().unwrap();
    rename_result.unwrap().unwrap();
    assert_eq!(
        per_index_top_id(&db, "evidence", "vector_text_v2", per_index_axis3(0)),
        Uuid::from_u128(10_000)
    );
    assert!(!vector_store.has_hnsw_index_for(&old_ref));
    assert!(vector_store.has_hnsw_index_for(&new_ref));
}

#[test]
fn sync_alter_table_drop_vector_column_removes_receiver_index() {
    let origin = Database::open_memory();
    let receiver = Database::open_memory();
    let schema = "CREATE TABLE evidence (
        id UUID PRIMARY KEY,
        vector_text VECTOR(3),
        vector_vision VECTOR(8)
    )";
    origin.execute(schema, &empty()).unwrap();
    receiver.execute(schema, &empty()).unwrap();

    let id = Uuid::from_u128(42);
    for db in [&origin, &receiver] {
        db.execute(
            "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $text, $vision)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("text", Value::Vector(per_index_axis3(0))),
                (
                    "vision",
                    Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                ),
            ]),
        )
        .unwrap();
    }

    let watermark = origin.current_lsn();
    origin
        .execute("ALTER TABLE evidence DROP COLUMN vector_vision", &empty())
        .unwrap();
    receiver
        .apply_changes(
            origin.changes_since(watermark),
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    let indexes = receiver.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let columns = indexes
        .rows
        .iter()
        .map(|row| row[column_idx].clone())
        .collect::<Vec<_>>();
    assert!(columns.contains(&Value::Text("vector_text".to_string())));
    assert!(!columns.contains(&Value::Text("vector_vision".to_string())));
    assert_eq!(
        per_index_top_id(&receiver, "evidence", "vector_text", per_index_axis3(0)),
        id
    );
    assert!(matches!(
        receiver.execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![0.0_f32; 8]))]),
        ),
        Err(Error::UnknownVectorIndex { index })
            if index == VectorIndexRef::new("evidence", "vector_vision")
    ));
}

#[test]
fn sync_alter_table_rename_vector_column_moves_receiver_index() {
    let origin = Database::open_memory();
    let receiver = Database::open_memory();
    let schema = "CREATE TABLE evidence (
        id UUID PRIMARY KEY,
        vector_text VECTOR(3),
        vector_vision VECTOR(8)
    )";
    origin.execute(schema, &empty()).unwrap();
    receiver.execute(schema, &empty()).unwrap();

    let id = Uuid::from_u128(43);
    let vision = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    for db in [&origin, &receiver] {
        db.execute(
            "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $text, $vision)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("text", Value::Vector(per_index_axis3(0))),
                ("vision", Value::Vector(vision.clone())),
            ]),
        )
        .unwrap();
    }

    let watermark = origin.current_lsn();
    origin
        .execute(
            "ALTER TABLE evidence RENAME COLUMN vector_vision TO vector_image",
            &empty(),
        )
        .unwrap();
    receiver
        .apply_changes(
            origin.changes_since(watermark),
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    let indexes = receiver.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let columns = indexes
        .rows
        .iter()
        .map(|row| row[column_idx].clone())
        .collect::<Vec<_>>();
    assert!(columns.contains(&Value::Text("vector_image".to_string())));
    assert!(!columns.contains(&Value::Text("vector_vision".to_string())));
    assert_eq!(
        per_index_top_id(&receiver, "evidence", "vector_image", vision),
        id
    );
    assert!(matches!(
        receiver.execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![0.0_f32; 8]))]),
        ),
        Err(Error::UnknownVectorIndex { index })
            if index == VectorIndexRef::new("evidence", "vector_vision")
    ));
}

#[test]
fn commit_rejects_staged_vector_insert_after_same_ref_sync_shape_change() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO evidence (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(44))),
            ("embedding", Value::Vector(per_index_axis3(0))),
        ]),
    )
    .unwrap();

    db.apply_changes(
        sync::ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![sync::DdlChange::AlterTable {
                name: "evidence".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                    ("embedding".to_string(), "VECTOR(4)".to_string()),
                ],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(db.current_lsn().0 + 1)],
        },
        &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
    )
    .unwrap();

    let err = db
        .commit(tx)
        .expect_err("commit must reject stale 3D vector staged for reshaped 4D index");
    assert!(matches!(
        err,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 4,
            actual: 3,
        } if index == VectorIndexRef::new("evidence", "embedding")
    ));
    let _ = db.rollback(tx);
    assert!(matches!(
        db.execute(
            "SELECT id FROM evidence ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        ),
        Ok(result) if result.rows.is_empty()
    ));
}

#[test]
fn commit_rejects_staged_vector_update_after_same_ref_sync_shape_change() {
    let db = Database::open_memory();
    let id = Uuid::from_u128(45);
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO evidence (id, note, embedding) VALUES ($id, 'old', $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("embedding", Value::Vector(per_index_axis3(0))),
        ]),
    )
    .unwrap();

    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE evidence SET note = 'new' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    db.apply_changes(
        sync::ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![sync::DdlChange::AlterTable {
                name: "evidence".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                    ("note".to_string(), "TEXT".to_string()),
                    ("embedding".to_string(), "VECTOR(4)".to_string()),
                ],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(db.current_lsn().0 + 1)],
        },
        &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
    )
    .unwrap();

    let err = db
        .commit(tx)
        .expect_err("commit must reject stale 3D vector carried by UPDATE row image");
    assert!(matches!(
        err,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 4,
            actual: 3,
        } if index == VectorIndexRef::new("evidence", "embedding")
    ));
    let _ = db.rollback(tx);
}

#[test]
fn sync_alter_table_shape_change_accepts_following_vector_payload() {
    let receiver = Database::open_memory();
    let id = Uuid::from_u128(46);
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "INSERT INTO evidence (id, note, embedding) VALUES ($id, 'kept', $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(per_index_axis3(0))),
            ]),
        )
        .unwrap();

    let reshaped = vec![0.0_f32, 1.0, 0.0, 0.0];
    receiver
        .apply_changes(
            sync::ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: vec![sync::VectorChange {
                    index: VectorIndexRef::new("evidence", "embedding"),
                    row_id: RowId(1),
                    vector: reshaped.clone(),
                    lsn: Lsn(11),
                }],
                ddl: vec![sync::DdlChange::AlterTable {
                    name: "evidence".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("note".to_string(), "TEXT".to_string()),
                        ("embedding".to_string(), "VECTOR(4)".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
            },
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert_eq!(
        per_index_top_id(&receiver, "evidence", "embedding", reshaped),
        id
    );
    let row = receiver
        .execute(
            "SELECT note FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(row.rows[0][0], Value::Text("kept".to_string()));
}

#[test]
fn sync_partial_vector_shape_alter_does_not_drop_omitted_side_columns() {
    let receiver = Database::open_memory();
    let id = Uuid::from_u128(47);
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "INSERT INTO evidence (id, note, embedding) VALUES ($id, 'kept', $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(per_index_axis3(0))),
            ]),
        )
        .unwrap();

    receiver
        .apply_changes(
            sync::ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: vec![sync::DdlChange::AlterTable {
                    name: "evidence".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("embedding".to_string(), "VECTOR(4)".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
            },
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    let row = receiver
        .execute(
            "SELECT note FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(row.rows[0][0], Value::Text("kept".to_string()));
    assert!(matches!(
        receiver.execute(
            "SELECT id FROM evidence ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        ),
        Err(Error::VectorIndexDimensionMismatch {
            expected: 3,
            actual: 4,
            ..
        })
    ));
}

#[test]
fn sync_additive_vector_alter_with_existing_non_vector_columns_keeps_old_vector_index() {
    let receiver = Database::open_memory();
    let id = Uuid::from_u128(48);
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, vector_text VECTOR(3))",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "INSERT INTO evidence (id, note, vector_text) VALUES ($id, 'kept', $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(per_index_axis3(0))),
            ]),
        )
        .unwrap();

    receiver
        .apply_changes(
            sync::ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: vec![sync::DdlChange::AlterTable {
                    name: "evidence".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("note".to_string(), "TEXT".to_string()),
                        ("vector_vision".to_string(), "VECTOR(8)".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
            },
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert_eq!(
        per_index_top_id(&receiver, "evidence", "vector_text", per_index_axis3(0)),
        id
    );
    let indexes = receiver.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let columns = indexes
        .rows
        .iter()
        .map(|row| row[column_idx].clone())
        .collect::<Vec<_>>();
    assert!(columns.contains(&Value::Text("vector_text".to_string())));
    assert!(columns.contains(&Value::Text("vector_vision".to_string())));
}

#[test]
fn sync_additive_non_vector_alter_does_not_drop_omitted_vector_index() {
    let receiver = Database::open_memory();
    let id = Uuid::from_u128(4801);
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, vector_text VECTOR(3))",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "INSERT INTO evidence (id, note, vector_text) VALUES ($id, 'kept', $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(per_index_axis3(0))),
            ]),
        )
        .unwrap();

    receiver
        .apply_changes(
            sync::ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: vec![sync::DdlChange::AlterTable {
                    name: "evidence".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("note".to_string(), "TEXT".to_string()),
                        ("tag".to_string(), "TEXT".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
            },
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert_eq!(
        per_index_top_id(&receiver, "evidence", "vector_text", per_index_axis3(0)),
        id
    );
    let row = receiver
        .execute(
            "SELECT tag FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(row.rows[0][0], Value::Null);
}

#[test]
fn sync_vector_rename_persists_row_image_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("receiver.db");
    let origin = Database::open_memory();
    let id = Uuid::from_u128(49);
    let vision = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let schema = "CREATE TABLE evidence (
        id UUID PRIMARY KEY,
        note TEXT,
        vector_text VECTOR(3),
        vector_vision VECTOR(8)
    )";

    {
        let receiver = Database::open(&db_path).unwrap();
        origin.execute(schema, &empty()).unwrap();
        receiver.execute(schema, &empty()).unwrap();
        for db in [&origin, &receiver] {
            db.execute(
                "INSERT INTO evidence (id, note, vector_text, vector_vision) VALUES ($id, 'kept', $text, $vision)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("text", Value::Vector(per_index_axis3(0))),
                    ("vision", Value::Vector(vision.clone())),
                ]),
            )
            .unwrap();
        }
        let watermark = origin.current_lsn();
        origin
            .execute(
                "ALTER TABLE evidence RENAME COLUMN vector_vision TO vector_image",
                &empty(),
            )
            .unwrap();
        receiver
            .apply_changes(
                origin.changes_since(watermark),
                &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
            )
            .unwrap();
        receiver.close().unwrap();
    }

    let reopened = Database::open(&db_path).unwrap();
    let row = reopened
        .execute(
            "SELECT note, vector_image FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(row.rows[0][0], Value::Text("kept".to_string()));
    assert_eq!(row.rows[0][1], Value::Vector(vision));
    assert!(matches!(
        reopened.execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![0.0_f32; 8]))]),
        ),
        Err(Error::UnknownVectorIndex { index })
            if index == VectorIndexRef::new("evidence", "vector_vision")
    ));
}

#[test]
fn sync_single_vector_column_rename_moves_receiver_index() {
    let origin = Database::open_memory();
    let receiver = Database::open_memory();
    let id = Uuid::from_u128(50);
    let schema = "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, embedding VECTOR(3))";
    origin.execute(schema, &empty()).unwrap();
    receiver.execute(schema, &empty()).unwrap();
    for db in [&origin, &receiver] {
        db.execute(
            "INSERT INTO evidence (id, note, embedding) VALUES ($id, 'kept', $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(per_index_axis3(0))),
            ]),
        )
        .unwrap();
    }

    let watermark = origin.current_lsn();
    origin
        .execute(
            "ALTER TABLE evidence RENAME COLUMN embedding TO embedding_v2",
            &empty(),
        )
        .unwrap();
    receiver
        .apply_changes(
            origin.changes_since(watermark),
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert_eq!(
        per_index_top_id(&receiver, "evidence", "embedding_v2", per_index_axis3(0)),
        id
    );
    let indexes = receiver.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let columns = indexes
        .rows
        .iter()
        .map(|row| row[column_idx].clone())
        .collect::<Vec<_>>();
    assert!(columns.contains(&Value::Text("embedding_v2".to_string())));
    assert!(!columns.contains(&Value::Text("embedding".to_string())));
    assert!(matches!(
        receiver.execute(
            "SELECT id FROM evidence ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(per_index_axis3(0)))]),
        ),
        Err(Error::UnknownVectorIndex { index })
            if index == VectorIndexRef::new("evidence", "embedding")
    ));
}

#[test]
fn sync_additive_same_shape_vector_alter_does_not_rename_omitted_vector_index() {
    let receiver = Database::open_memory();
    let id = Uuid::from_u128(51);
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, note TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "INSERT INTO evidence (id, note, embedding) VALUES ($id, 'kept', $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(per_index_axis3(0))),
            ]),
        )
        .unwrap();

    receiver
        .apply_changes(
            sync::ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: vec![sync::DdlChange::AlterTable {
                    name: "evidence".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("note".to_string(), "TEXT".to_string()),
                        ("thumbnail".to_string(), "VECTOR(3)".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
            },
            &sync::ConflictPolicies::uniform(sync::ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert_eq!(
        per_index_top_id(&receiver, "evidence", "embedding", per_index_axis3(0)),
        id
    );
    let indexes = receiver.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let columns = indexes
        .rows
        .iter()
        .map(|row| row[column_idx].clone())
        .collect::<Vec<_>>();
    assert!(columns.contains(&Value::Text("embedding".to_string())));
    assert!(columns.contains(&Value::Text("thumbnail".to_string())));
    let added_search = receiver
        .execute(
            "SELECT id FROM evidence ORDER BY thumbnail <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(per_index_axis3(0)))]),
        )
        .unwrap();
    assert!(added_search.rows.is_empty());
}

#[cfg(feature = "test-seams")]
#[test]
fn drop_table_waits_for_inflight_vector_builds_then_removes_all_indexes() {
    use contextdb_vector::test_seam::PauseWindow;

    let db = Arc::new(Database::open_memory());
    seed_two_vector_evidence(&db);
    let text_ref = VectorIndexRef::new("evidence", "vector_text");
    let vision_ref = VectorIndexRef::new("evidence", "vector_vision");
    let table_ref = VectorIndexRef::new("evidence", "*");
    let vector_store = db.vector_store_for_test();
    let build_pause = vector_store.arm_maintenance_pause_for_test(&text_ref, PauseWindow::Build);
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let db_build = db.clone();
    thread::spawn(move || {
        done_build_tx
            .send(db_build.execute(
                "SELECT id FROM evidence ORDER BY vector_text <=> $query LIMIT 1",
                &params(vec![("query", Value::Vector(per_index_axis3(0)))]),
            ))
            .unwrap();
    });
    assert!(build_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));

    let ddl_pause = vector_store.arm_maintenance_pause_for_test(&table_ref, PauseWindow::Ddl);
    let (started_drop_tx, started_drop_rx) = mpsc::channel();
    let (done_drop_tx, done_drop_rx) = mpsc::channel();
    let db_drop = db.clone();
    thread::spawn(move || {
        started_drop_tx.send(()).unwrap();
        let result = db_drop.execute("DROP TABLE evidence", &empty());
        done_drop_tx.send(result).unwrap();
    });
    assert!(started_drop_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT).is_ok());
    assert!(ddl_pause.wait_until_reached(PER_INDEX_SQL_TIMEOUT));
    let table_shape_during_pause = db
        .execute("SELECT id FROM evidence LIMIT 1", &empty())
        .is_ok();
    assert!(matches!(done_drop_rx.try_recv(), Err(TryRecvError::Empty)));
    ddl_pause.release();
    let drop_done_before_build_release = done_drop_rx.try_recv();
    let table_shape_after_ddl_release = db
        .execute("SELECT id FROM evidence LIMIT 1", &empty())
        .is_ok();
    build_pause.release();
    let _ = done_build_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);
    let drop_result = done_drop_rx.recv_timeout(PER_INDEX_SQL_TIMEOUT);

    assert!(table_shape_during_pause);
    assert!(matches!(
        drop_done_before_build_release,
        Err(TryRecvError::Empty)
    ));
    assert!(table_shape_after_ddl_release);
    drop_result.unwrap().unwrap();
    assert!(
        db.execute("SELECT id FROM evidence LIMIT 1", &empty())
            .is_err()
    );
    assert!(!vector_store.has_hnsw_index_for(&text_ref));
    assert!(!vector_store.has_hnsw_index_for(&vision_ref));
}
