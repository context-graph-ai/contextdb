//! SQL surface operators, join shapes, and the cases that cannot share that
//! skeleton (NOW(), DEFAULT NOW(), and a boolean predicate on a scan versus a
//! join).
#![allow(clippy::type_complexity)]

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn exec(db: &Database, sql: &str, row: &str) -> contextdb_engine::QueryResult {
    db.execute(sql, &empty())
        .unwrap_or_else(|e| panic!("{row}: {sql}: {e:?}"))
}

fn col(result: &contextdb_engine::QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("column {name} not in {:?}", result.columns))
}

fn seed_text(db: &Database, table: &str, column: &str, values: &[&str], row: &str) {
    for value in values {
        db.execute(
            &format!("INSERT INTO {table} (id, {column}) VALUES ($id, $v)"),
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("v", Value::Text((*value).into())),
            ]),
        )
        .unwrap_or_else(|e| panic!("{row}: seeding {table}: {e:?}"));
    }
}

fn seed_i64(db: &Database, table: &str, column: &str, values: &[i64], row: &str) {
    for value in values {
        db.execute(
            &format!("INSERT INTO {table} (id, {column}) VALUES ($id, $v)"),
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("v", Value::Int64(*value)),
            ]),
        )
        .unwrap_or_else(|e| panic!("{row}: seeding {table}: {e:?}"));
    }
}

#[derive(Clone, Copy, Debug)]
enum Cell {
    I64(i64),
    F64(f64),
    Ts(i64),
    Text(&'static str),
    Bool(bool),
}

impl Cell {
    fn value(self) -> Value {
        match self {
            Cell::I64(v) => Value::Int64(v),
            Cell::F64(v) => Value::Float64(v),
            Cell::Ts(v) => Value::Timestamp(v),
            Cell::Text(v) => Value::Text(v.into()),
            Cell::Bool(v) => Value::Bool(v),
        }
    }
}

fn assert_contains(
    result: &contextdb_engine::QueryResult,
    column: &str,
    expected: &[Cell],
    row: &str,
) {
    let idx = col(result, column);
    let got: Vec<&Value> = result.rows.iter().map(|r| &r[idx]).collect();
    for cell in expected {
        let value = cell.value();
        assert!(
            got.contains(&&value),
            "{row}: missing {value:?} in {column}, have {got:?}"
        );
    }
}

/// Which projected rows a `CountRow` result must hold beyond its row count.
enum Projected {
    Unchecked,
    /// Exactly these tuples over `columns`, in any order.
    AnyOrder(&'static [&'static str], &'static [&'static [Cell]]),
    /// Exactly these tuples over `columns`, in this order.
    InOrder(&'static [&'static str], &'static [&'static [Cell]]),
}

fn project(result: &contextdb_engine::QueryResult, columns: &[&str]) -> Vec<Vec<Value>> {
    let idx: Vec<usize> = columns.iter().map(|c| col(result, c)).collect();
    result
        .rows
        .iter()
        .map(|r| idx.iter().map(|i| r[*i].clone()).collect())
        .collect()
}

fn expected_tuples(tuples: &[&[Cell]]) -> Vec<Vec<Value>> {
    tuples
        .iter()
        .map(|t| t.iter().map(|c| c.value()).collect())
        .collect()
}

fn assert_projected(result: &contextdb_engine::QueryResult, projected: &Projected, row: &str) {
    match projected {
        Projected::Unchecked => {}
        Projected::InOrder(columns, tuples) => {
            assert_eq!(
                project(result, columns),
                expected_tuples(tuples),
                "{row}: {columns:?} in order"
            );
        }
        Projected::AnyOrder(columns, tuples) => {
            let mut got = project(result, columns);
            let expected = expected_tuples(tuples);
            for tuple in &expected {
                let at = got.iter().position(|g| g == tuple).unwrap_or_else(|| {
                    panic!("{row}: missing {columns:?} = {tuple:?}, have {got:?}")
                });
                got.remove(at);
            }
            assert!(
                got.is_empty(),
                "{row}: unexpected {columns:?} tuples {got:?}"
            );
        }
    }
}

struct CountRow {
    name: &'static str,
    ddl: &'static [&'static str],
    seed: fn(&Database, &str),
    sql: &'static str,
    projected: Projected,
    count: usize,
    contains: &'static [(&'static str, &'static [Cell])],
}

#[test]
fn comparison_logical_expression_and_shaping_operators() {
    #[rustfmt::skip]
    let rows: &[CountRow] = &[
        CountRow {
            name: "cmp_01_less_than_integer",
            ddl: &["CREATE TABLE items (id UUID PRIMARY KEY, score INTEGER)"],
            seed: |db, row| seed_i64(db, "items", "score", &[10, 20, 30], row),
            sql: "SELECT * FROM items WHERE score < 20",
            projected: Projected::Unchecked,
            count: 1,
            contains: &[("score", &[Cell::I64(10)])],
        },
        CountRow {
            name: "cmp_02_gte_float",
            ddl: &["CREATE TABLE readings (id UUID PRIMARY KEY, value REAL)"],
            seed: |db, row| {
                for val in [1.5f64, 2.5, 3.5] {
                    db.execute(
                        "INSERT INTO readings (id, value) VALUES ($id, $value)",
                        &params(vec![
                            ("id", Value::Uuid(Uuid::new_v4())),
                            ("value", Value::Float64(val)),
                        ]),
                    )
                    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
                }
            },
            sql: "SELECT * FROM readings WHERE value >= 2.5",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("value", &[Cell::F64(2.5), Cell::F64(3.5)])],
        },
        CountRow {
            name: "cmp_03_gt_text_lexicographic",
            ddl: &["CREATE TABLE words (id UUID PRIMARY KEY, word TEXT)"],
            seed: |db, row| seed_text(db, "words", "word", &["apple", "banana", "cherry"], row),
            sql: "SELECT * FROM words WHERE word > 'banana'",
            projected: Projected::Unchecked,
            count: 1,
            contains: &[("word", &[Cell::Text("cherry")])],
        },
        CountRow {
            name: "cmp_04_cross_type_int_vs_float",
            ddl: &["CREATE TABLE mixed (id UUID PRIMARY KEY, val INTEGER)"],
            seed: |db, row| seed_i64(db, "mixed", "val", &[2, 3, 4], row),
            sql: "SELECT * FROM mixed WHERE val > 2.5",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("val", &[Cell::I64(3), Cell::I64(4)])],
        },
        CountRow {
            name: "cmp_05_timestamp_vs_int",
            ddl: &["CREATE TABLE events (id UUID PRIMARY KEY, ts TIMESTAMP)"],
            seed: |db, row| {
                for ts in [1000i64, 2000, 3000] {
                    db.execute(
                        "INSERT INTO events (id, ts) VALUES ($id, $ts)",
                        &params(vec![
                            ("id", Value::Uuid(Uuid::new_v4())),
                            ("ts", Value::Timestamp(ts)),
                        ]),
                    )
                    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
                }
            },
            sql: "SELECT * FROM events WHERE ts >= 2000",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("ts", &[Cell::Ts(2000), Cell::Ts(3000)])],
        },
        CountRow {
            name: "cmp_06_null_eq_null_is_false",
            ddl: &["CREATE TABLE nullable (id UUID PRIMARY KEY, val TEXT)"],
            seed: |db, row| {
                db.execute(
                    "INSERT INTO nullable (id, val) VALUES ($id, NULL)",
                    &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
                )
                .unwrap_or_else(|e| panic!("{row}: {e:?}"));
                db.execute(
                    "INSERT INTO nullable (id, val) VALUES ($id, $val)",
                    &params(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("val", Value::Text("hello".into())),
                    ]),
                )
                .unwrap_or_else(|e| panic!("{row}: {e:?}"));
            },
            sql: "SELECT * FROM nullable WHERE val = NULL",
            projected: Projected::Unchecked,
            count: 0,
            contains: &[],
        },
        CountRow {
            name: "cmp_07_neq_null_is_false",
            ddl: &["CREATE TABLE nullable2 (id UUID PRIMARY KEY, val TEXT)"],
            seed: |db, row| {
                for v in [None, Some("hello"), Some("world")] {
                    let mut p = vec![("id", Value::Uuid(Uuid::new_v4()))];
                    let sql = if let Some(s) = v {
                        p.push(("val", Value::Text(s.into())));
                        "INSERT INTO nullable2 (id, val) VALUES ($id, $val)"
                    } else {
                        "INSERT INTO nullable2 (id, val) VALUES ($id, NULL)"
                    };
                    db.execute(sql, &params(p)).unwrap_or_else(|e| panic!("{row}: {e:?}"));
                }
            },
            sql: "SELECT * FROM nullable2 WHERE val <> NULL",
            projected: Projected::Unchecked,
            count: 0,
            contains: &[],
        },
        CountRow {
            name: "log_01_and_combines_filters",
            ddl: &["CREATE TABLE products (id UUID PRIMARY KEY, price INTEGER, category TEXT)"],
            seed: seed_products,
            sql: "SELECT * FROM products WHERE price <= 10 AND category = 'food'",
            projected: Projected::Unchecked,
            count: 1,
            contains: &[
                ("price", &[Cell::I64(10)]),
                ("category", &[Cell::Text("food")]),
            ],
        },
        CountRow {
            name: "log_02_or_matches_either",
            ddl: &["CREATE TABLE products (id UUID PRIMARY KEY, price INTEGER, category TEXT)"],
            seed: seed_products,
            sql: "SELECT * FROM products WHERE price = 30 OR category = 'food'",
            projected: Projected::AnyOrder(
                &["price", "category"],
                &[
                    &[Cell::I64(10), Cell::Text("food")],
                    &[Cell::I64(20), Cell::Text("food")],
                    &[Cell::I64(30), Cell::Text("drink")],
                ],
            ),
            count: 3,
            contains: &[
                ("price", &[Cell::I64(10), Cell::I64(20), Cell::I64(30)]),
            ],
        },
        CountRow {
            name: "log_03_not_negates",
            ddl: &["CREATE TABLE flags (id UUID PRIMARY KEY, active BOOLEAN)"],
            seed: |db, row| {
                for b in [true, false, true] {
                    db.execute(
                        "INSERT INTO flags (id, active) VALUES ($id, $active)",
                        &params(vec![
                            ("id", Value::Uuid(Uuid::new_v4())),
                            ("active", Value::Bool(b)),
                        ]),
                    )
                    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
                }
            },
            sql: "SELECT * FROM flags WHERE NOT active = true",
            projected: Projected::Unchecked,
            count: 1,
            contains: &[("active", &[Cell::Bool(false)])],
        },
        CountRow {
            name: "expr_01_in_list",
            ddl: &["CREATE TABLE colors (id UUID PRIMARY KEY, name TEXT)"],
            seed: |db, row| seed_text(db, "colors", "name", &["red", "green", "blue", "yellow"], row),
            sql: "SELECT * FROM colors WHERE name IN ('red', 'blue')",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("name", &[Cell::Text("red"), Cell::Text("blue")])],
        },
        CountRow {
            name: "expr_02_not_in",
            ddl: &["CREATE TABLE colors (id UUID PRIMARY KEY, name TEXT)"],
            seed: |db, row| seed_text(db, "colors", "name", &["red", "green", "blue", "yellow"], row),
            sql: "SELECT * FROM colors WHERE name NOT IN ('red', 'blue')",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("name", &[Cell::Text("green"), Cell::Text("yellow")])],
        },
        CountRow {
            name: "expr_04_like_percent",
            ddl: &["CREATE TABLE files (id UUID PRIMARY KEY, name TEXT)"],
            seed: |db, row| seed_text(db, "files", "name", &["report.pdf", "report.docx", "invoice.pdf", "notes.txt"], row),
            sql: "SELECT * FROM files WHERE name LIKE 'report%'",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("name", &[Cell::Text("report.pdf"), Cell::Text("report.docx")])],
        },
        CountRow {
            name: "expr_05_like_underscore",
            ddl: &["CREATE TABLE codes (id UUID PRIMARY KEY, code TEXT)"],
            seed: |db, row| seed_text(db, "codes", "code", &["cat", "bat", "at", "cart"], row),
            sql: "SELECT * FROM codes WHERE code LIKE '_at'",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("code", &[Cell::Text("cat"), Cell::Text("bat")])],
        },
        CountRow {
            name: "expr_06_not_like",
            ddl: &["CREATE TABLE files2 (id UUID PRIMARY KEY, name TEXT)"],
            seed: |db, row| seed_text(db, "files2", "name", &["report.pdf", "report.docx", "invoice.pdf", "notes.txt"], row),
            sql: "SELECT * FROM files2 WHERE name NOT LIKE '%.pdf'",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("name", &[Cell::Text("report.docx"), Cell::Text("notes.txt")])],
        },
        CountRow {
            name: "expr_07_between_inclusive",
            ddl: &["CREATE TABLE temps (id UUID PRIMARY KEY, celsius INTEGER)"],
            seed: |db, row| seed_i64(db, "temps", "celsius", &[10, 20, 25, 30, 40], row),
            sql: "SELECT * FROM temps WHERE celsius BETWEEN 20 AND 30",
            projected: Projected::Unchecked,
            count: 3,
            contains: &[("celsius", &[Cell::I64(20), Cell::I64(25), Cell::I64(30)])],
        },
        CountRow {
            name: "expr_09_is_not_null",
            ddl: &["CREATE TABLE optional2 (id UUID PRIMARY KEY, note TEXT)"],
            seed: seed_optional_notes,
            sql: "SELECT * FROM optional2 WHERE note IS NOT NULL",
            projected: Projected::Unchecked,
            count: 2,
            contains: &[("note", &[Cell::Text("hello"), Cell::Text("world")])],
        },
        CountRow {
            name: "shp_01_distinct_removes_duplicates",
            ddl: &["CREATE TABLE tags (id UUID PRIMARY KEY, tag TEXT)"],
            seed: |db, row| seed_text(db, "tags", "tag", &["rust", "python", "rust", "go", "python"], row),
            sql: "SELECT DISTINCT tag FROM tags",
            projected: Projected::AnyOrder(
                &["tag"],
                &[&[Cell::Text("rust")], &[Cell::Text("python")], &[Cell::Text("go")]],
            ),
            count: 3,
            contains: &[],
        },
        CountRow {
            name: "shp_07_limit",
            ddl: &["CREATE TABLE big (id UUID PRIMARY KEY, num INTEGER)"],
            seed: |db, row| seed_i64(db, "big", "num", &[1, 2, 3, 4, 5], row),
            sql: "SELECT * FROM big ORDER BY num ASC LIMIT 3",
            projected: Projected::InOrder(
                &["num"],
                &[&[Cell::I64(1)], &[Cell::I64(2)], &[Cell::I64(3)]],
            ),
            count: 3,
            contains: &[("num", &[Cell::I64(1), Cell::I64(2), Cell::I64(3)])],
        },
        CountRow {
            name: "jn_05_inner_join_no_matches_empty",
            ddl: &[
                "CREATE TABLE table_a (id UUID PRIMARY KEY, key_col TEXT)",
                "CREATE TABLE table_b (id UUID PRIMARY KEY, ref_key TEXT)",
            ],
            seed: |db, row| {
                db.execute(
                    "INSERT INTO table_a (id, key_col) VALUES ($id, $key)",
                    &params(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("key", Value::Text("x".into())),
                    ]),
                )
                .unwrap_or_else(|e| panic!("{row}: {e:?}"));
                db.execute(
                    "INSERT INTO table_b (id, ref_key) VALUES ($id, $key)",
                    &params(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("key", Value::Text("y".into())),
                    ]),
                )
                .unwrap_or_else(|e| panic!("{row}: {e:?}"));
            },
            sql: "SELECT * FROM table_a a INNER JOIN table_b b ON a.key_col = b.ref_key",
            projected: Projected::Unchecked,
            count: 0,
            contains: &[],
        },
        CountRow {
            name: "sql_12_not_between",
            ddl: &["CREATE TABLE scores (id UUID PRIMARY KEY, val REAL)"],
            seed: |db, row| {
                for (id, val) in [
                    (Uuid::from_u128(9301), 0.1_f64),
                    (Uuid::from_u128(9302), 0.5_f64),
                    (Uuid::from_u128(9303), 0.9_f64),
                ] {
                    db.execute(
                        "INSERT INTO scores (id, val) VALUES ($id, $val)",
                        &params(vec![("id", Value::Uuid(id)), ("val", Value::Float64(val))]),
                    )
                    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
                }
            },
            sql: "SELECT val FROM scores WHERE val NOT BETWEEN 0.3 AND 0.7 ORDER BY val",
            projected: Projected::InOrder(&["val"], &[&[Cell::F64(0.1)], &[Cell::F64(0.9)]]),
            count: 2,
            contains: &[("val", &[Cell::F64(0.1), Cell::F64(0.9)])],
        },
    ];

    for row in rows {
        let db = Database::open_memory();
        for ddl in row.ddl {
            db.execute(ddl, &empty())
                .unwrap_or_else(|e| panic!("{}: {ddl}: {e:?}", row.name));
        }
        (row.seed)(&db, row.name);
        let result = exec(&db, row.sql, row.name);
        assert_eq!(
            result.rows.len(),
            row.count,
            "{}: expected {} rows, got {}",
            row.name,
            row.count,
            result.rows.len()
        );
        for (column, values) in row.contains {
            assert_contains(&result, column, values, row.name);
        }
        assert_projected(&result, &row.projected, row.name);
    }
}

fn seed_products(db: &Database, row: &str) {
    for (price, cat) in [(10i64, "food"), (20, "food"), (10, "drink"), (30, "drink")] {
        db.execute(
            "INSERT INTO products (id, price, category) VALUES ($id, $price, $category)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("price", Value::Int64(price)),
                ("category", Value::Text(cat.into())),
            ]),
        )
        .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    }
}

fn seed_optional_notes(db: &Database, row: &str) {
    db.execute(
        "INSERT INTO optional2 (id, note) VALUES ($id, $note)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("note", Value::Text("hello".into())),
        ]),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    db.execute(
        "INSERT INTO optional2 (id, note) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    db.execute(
        "INSERT INTO optional2 (id, note) VALUES ($id, $note)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("note", Value::Text("world".into())),
        ]),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
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
    seed_text(
        &db,
        "departments",
        "name",
        &["engineering", "sales"],
        "expr_03_in_subquery",
    );
    for (dept, name) in [
        ("engineering", "alice"),
        ("marketing", "bob"),
        ("sales", "carol"),
    ] {
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
    let result = exec(
        &db,
        "SELECT * FROM employees WHERE dept IN (SELECT name FROM departments)",
        "expr_03_in_subquery",
    );
    assert_eq!(result.rows.len(), 2, "expr_03_in_subquery");
    assert_contains(
        &result,
        "name",
        &[Cell::Text("alice"), Cell::Text("carol")],
        "expr_03_in_subquery",
    );
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
    let result = exec(
        &db,
        "SELECT * FROM optional WHERE note IS NULL",
        "expr_08_is_null",
    );
    assert_eq!(result.rows.len(), 1, "expr_08_is_null");
    assert_eq!(result.rows[0][col(&result, "id")], Value::Uuid(null_id));
}

#[test]
fn log_04_null_propagation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nullcheck (id UUID PRIMARY KEY, a TEXT, b TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcheck (id, a, b) VALUES ($id, NULL, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("b", Value::Text("x".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcheck (id, a, b) VALUES ($id, $a, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Text("y".into())),
            ("b", Value::Text("x".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullcheck (id, a, b) VALUES ($id, NULL, NULL)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    let r1 = exec(
        &db,
        "SELECT * FROM nullcheck WHERE a = 'missing' AND b = 'x'",
        "log_04_null_propagation",
    );
    assert_eq!(r1.rows.len(), 0, "log_04_null_propagation AND");
    let r2 = exec(
        &db,
        "SELECT * FROM nullcheck WHERE a = 'y' OR b = 'x'",
        "log_04_null_propagation",
    );
    assert_eq!(r2.rows.len(), 2, "log_04_null_propagation OR");
}

#[test]
fn join_shapes_inner_left_and_disambiguated() {
    let rows: &[(&str, fn(&Database, &str))] = &[
        ("jn_01_inner_join", |db, row| {
            seed_authors_books(db, true, row);
            let result = exec(
                db,
                "SELECT a.name, b.title FROM authors a INNER JOIN books b ON a.id = b.author_id",
                row,
            );
            assert_eq!(result.rows.len(), 2, "jn_01_inner_join");
            let name_idx = col(&result, "name");
            for row in &result.rows {
                assert_eq!(
                    row[name_idx],
                    Value::Text("alice".into()),
                    "jn_01_inner_join"
                );
            }
            assert_contains(
                &result,
                "title",
                &[Cell::Text("book_a"), Cell::Text("book_b")],
                "jn_01_inner_join",
            );
        }),
        ("jn_02_left_join_unmatched_nulls", |db, row| {
            seed_authors_books(db, false, row);
            let result = exec(
                db,
                "SELECT a.name, b.title FROM authors a LEFT JOIN books b ON a.id = b.author_id",
                row,
            );
            assert_eq!(result.rows.len(), 3, "jn_02_left_join_unmatched_nulls");
            let name_idx = col(&result, "name");
            let title_idx = col(&result, "title");
            let bob: Vec<_> = result
                .rows
                .iter()
                .filter(|r| r[name_idx] == Value::Text("bob".into()))
                .collect();
            assert_eq!(bob.len(), 1, "jn_02_left_join_unmatched_nulls");
            assert_eq!(
                bob[0][title_idx],
                Value::Null,
                "jn_02_left_join_unmatched_nulls unmatched left row has NULL for right columns"
            );
        }),
        ("jn_03_disambiguated_columns", |db, row| {
            db.execute(
                "CREATE TABLE left_t (id UUID PRIMARY KEY, val TEXT)",
                &empty(),
            )
            .unwrap_or_else(|e| panic!("{row}: {e:?}"));
            db.execute(
                "CREATE TABLE right_t (id UUID PRIMARY KEY, val TEXT, left_id UUID)",
                &empty(),
            )
            .unwrap_or_else(|e| panic!("{row}: {e:?}"));
            let lid = Uuid::new_v4();
            db.execute(
                "INSERT INTO left_t (id, val) VALUES ($id, $val)",
                &params(vec![
                    ("id", Value::Uuid(lid)),
                    ("val", Value::Text("left_val".into())),
                ]),
            )
            .unwrap_or_else(|e| panic!("{row}: {e:?}"));
            db.execute(
                "INSERT INTO right_t (id, val, left_id) VALUES ($id, $val, $lid)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("val", Value::Text("right_val".into())),
                    ("lid", Value::Uuid(lid)),
                ]),
            )
            .unwrap_or_else(|e| panic!("{row}: {e:?}"));
            let result = exec(
                db,
                "SELECT l.val, r.val FROM left_t l INNER JOIN right_t r ON l.id = r.left_id",
                row,
            );
            assert_eq!(result.rows.len(), 1, "jn_03_disambiguated_columns");
            assert_eq!(
                result.rows[0][0],
                Value::Text("left_val".into()),
                "jn_03_disambiguated_columns"
            );
            assert_eq!(
                result.rows[0][1],
                Value::Text("right_val".into()),
                "jn_03_disambiguated_columns"
            );
        }),
    ];
    for (name, run) in rows {
        let db = Database::open_memory();
        run(&db, name);
    }
}

fn seed_authors_books(db: &Database, orphan_book: bool, row: &str) -> (Uuid, Uuid) {
    db.execute(
        "CREATE TABLE authors (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    db.execute(
        "CREATE TABLE books (id UUID PRIMARY KEY, title TEXT, author_id UUID)",
        &empty(),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    db.execute(
        "INSERT INTO authors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(alice)),
            ("name", Value::Text("alice".into())),
        ]),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    db.execute(
        "INSERT INTO authors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(bob)),
            ("name", Value::Text("bob".into())),
        ]),
    )
    .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    for title in ["book_a", "book_b"] {
        db.execute(
            "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("title", Value::Text(title.into())),
                ("aid", Value::Uuid(alice)),
            ]),
        )
        .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    }
    if orphan_book {
        db.execute(
            "INSERT INTO books (id, title, author_id) VALUES ($id, $title, $aid)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("title", Value::Text("book_c".into())),
                ("aid", Value::Uuid(Uuid::new_v4())),
            ]),
        )
        .unwrap_or_else(|e| panic!("{row}: {e:?}"));
    }
    (alice, bob)
}

#[test]
fn aggregation_count_and_mixed_refusal() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE counttable (id UUID PRIMARY KEY, val TEXT)",
        &empty(),
    )
    .unwrap();
    seed_text(
        &db,
        "counttable",
        "val",
        &["a", "b", "c"],
        "agg_01_count_star",
    );
    let star = exec(&db, "SELECT COUNT(*) FROM counttable", "agg_01_count_star");
    assert_eq!(star.rows.len(), 1, "agg_01_count_star");
    assert_eq!(star.rows[0][0], Value::Int64(3), "agg_01_count_star");
    assert!(
        star.columns.iter().any(|c| c == "COUNT"),
        "agg_01_count_star column name must preserve SQL case"
    );

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
    let counted = exec(
        &db,
        "SELECT COUNT(val) FROM nullcount",
        "agg_02_count_expr_excludes_nulls",
    );
    assert_eq!(counted.rows.len(), 1, "agg_02_count_expr_excludes_nulls");
    assert_eq!(
        counted.rows[0][0],
        Value::Int64(2),
        "agg_02_count_expr_excludes_nulls"
    );

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
    assert!(
        db.execute("SELECT COUNT(*), val FROM mixagg", &empty())
            .is_err(),
        "agg_03_mixed_aggregate_error"
    );
}

#[test]
fn query_shaping_alias_and_order_by() {
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
    let aliased = exec(
        &db,
        "SELECT name AS person_name FROM people",
        "shp_02_column_alias",
    );
    assert!(
        aliased.columns.contains(&"person_name".to_string()),
        "shp_02_column_alias"
    );
    assert!(
        !aliased.columns.contains(&"name".to_string()),
        "shp_02_column_alias original name should not appear"
    );
    assert_eq!(aliased.rows[0][0], Value::Text("alice".into()));

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sorted (id UUID PRIMARY KEY, num INTEGER)",
        &empty(),
    )
    .unwrap();
    seed_i64(&db, "sorted", "num", &[30, 10, 20], "shp_03_order_by_asc");
    let ordered = exec(
        &db,
        "SELECT * FROM sorted ORDER BY num ASC",
        "shp_03_order_by_asc",
    );
    let num_idx = col(&ordered, "num");
    assert_eq!(
        (
            ordered.rows[0][num_idx].clone(),
            ordered.rows[1][num_idx].clone(),
            ordered.rows[2][num_idx].clone()
        ),
        (Value::Int64(10), Value::Int64(20), Value::Int64(30)),
        "shp_03_order_by_asc"
    );

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE ranked (id UUID PRIMARY KEY, group_name TEXT, score INTEGER)",
        &empty(),
    )
    .unwrap();
    for (g, s) in [("alpha", 10i64), ("alpha", 20), ("beta", 10), ("beta", 20)] {
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
    let mixed = exec(
        &db,
        "SELECT * FROM ranked ORDER BY group_name ASC, score DESC",
        "shp_04_order_by_multi_column_mixed",
    );
    let g_idx = col(&mixed, "group_name");
    let s_idx = col(&mixed, "score");
    assert_eq!(
        mixed.rows[0][g_idx],
        Value::Text("alpha".into()),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[0][s_idx],
        Value::Int64(20),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[1][g_idx],
        Value::Text("alpha".into()),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[1][s_idx],
        Value::Int64(10),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[2][g_idx],
        Value::Text("beta".into()),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[2][s_idx],
        Value::Int64(20),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[3][g_idx],
        Value::Text("beta".into()),
        "shp_04_order_by_multi_column_mixed"
    );
    assert_eq!(
        mixed.rows[3][s_idx],
        Value::Int64(10),
        "shp_04_order_by_multi_column_mixed"
    );

    for (name, sql, first, second, third) in [
        (
            "shp_05_order_by_asc_nulls_last",
            "SELECT * FROM nullsort ORDER BY val ASC",
            Value::Int64(1),
            Value::Int64(3),
            Value::Null,
        ),
        (
            "shp_06_order_by_desc_nulls_first",
            "SELECT * FROM nullsort ORDER BY val DESC",
            Value::Null,
            Value::Int64(3),
            Value::Int64(1),
        ),
    ] {
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
        let result = exec(&db, sql, name);
        let val_idx = col(&result, "val");
        assert_eq!(result.rows[0][val_idx], first, "{name}");
        assert_eq!(result.rows[1][val_idx], second, "{name}");
        assert_eq!(result.rows[2][val_idx], third, "{name}");
    }
}

#[test]
fn constraint_rows_refuse_or_accept_as_declared() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE strict (id UUID PRIMARY KEY, name TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    assert!(
        db.execute(
            "INSERT INTO strict (id, name) VALUES ($id, NULL)",
            &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .is_err(),
        "con_01_not_null_rejects_null"
    );

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
    assert_eq!(
        db.scan("uniq", db.snapshot()).unwrap().len(),
        1,
        "con_02_unique_duplicate_is_noop"
    );

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
    assert_eq!(
        meta.columns.len(),
        2,
        "con_03_backward_compat_column_def_serde"
    );
    assert_eq!(meta.columns[0].name, "id");
    assert_eq!(meta.columns[1].name, "val");

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
    let err = db
        .execute(
            "INSERT INTO memberships (id, org_id, email) VALUES ($id, $org_id, $email)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("org_id", Value::Uuid(Uuid::from_u128(1))),
                ("email", Value::Text("alice@example.com".into())),
            ]),
        )
        .unwrap_err();
    assert!(
        matches!(&err, Error::UniqueViolation { table, .. } if table == "memberships"),
        "con_04_composite_unique_duplicate_is_refused got {err:?}"
    );
    assert_eq!(
        db.scan("memberships", db.snapshot()).unwrap().len(),
        1,
        "con_04_composite_unique_duplicate_is_refused"
    );
    db.execute(
        "INSERT INTO memberships (id, org_id, email) VALUES ($id, $org_id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("org_id", Value::Uuid(Uuid::from_u128(2))),
            ("email", Value::Text("alice@example.com".into())),
        ]),
    )
    .unwrap();
    assert_eq!(
        db.scan("memberships", db.snapshot()).unwrap().len(),
        2,
        "con_05_composite_unique_allows_distinct_tuple"
    );
}

fn assert_now_is_recent(value: &Value, row: &str) {
    match value {
        Value::Timestamp(ts) => {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(
                (now_secs - ts).abs() < 5,
                "{row}: NOW() should be within 5s of system time, got {ts}"
            );
        }
        other => panic!("{row}: expected Timestamp, got {other:?}"),
    }
}

#[test]
fn fn_01_coalesce_first_non_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE coaltable (id UUID PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO coaltable (id, a, b, c) VALUES ($id, NULL, NULL, $c)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("c", Value::Text("fallback".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO coaltable (id, a, b, c) VALUES ($id, NULL, $b, $c)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("b", Value::Text("second".into())),
            ("c", Value::Text("third".into())),
        ]),
    )
    .unwrap();
    let result = exec(
        &db,
        "SELECT COALESCE(a, b, c) FROM coaltable ORDER BY id ASC",
        "fn_01_coalesce_first_non_null",
    );
    assert_eq!(result.rows.len(), 2, "fn_01_coalesce_first_non_null");
    let values: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(values.contains(&&Value::Text("fallback".into())));
    assert!(values.contains(&&Value::Text("second".into())));
}

#[test]
fn fn_02_now_without_from() {
    let result = exec(
        &Database::open_memory(),
        "SELECT NOW()",
        "fn_02_now_without_from",
    );
    assert_eq!(result.rows.len(), 1, "fn_02_now_without_from");
    assert_now_is_recent(&result.rows[0][0], "fn_02_now_without_from");
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
    let result = exec(&db, "SELECT NOW() FROM dummy", "fn_03_now_with_from");
    assert_eq!(result.rows.len(), 1, "fn_03_now_with_from");
    assert_now_is_recent(&result.rows[0][0], "fn_03_now_with_from");
}

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
    assert_eq!(
        result.rows.len(),
        1,
        "sql_01_default_now_produces_timestamp"
    );
    match &result.rows[0][0] {
        Value::Timestamp(ts) => assert!(
            *ts > 1_700_000_000,
            "sql_01_default_now_produces_timestamp created_at should be a recent unix timestamp, got {ts}"
        ),
        other => panic!("sql_01_default_now_produces_timestamp expected TIMESTAMP, got {other:?}"),
    }
}

/// A bare boolean column is a legal predicate on a scan, on a CTE filter,
/// and on JOIN ON. All three keep the rows where the column is TRUE and
/// exclude FALSE and NULL.
#[test]
fn a_boolean_column_predicate_selects_the_same_rows_on_a_scan_and_on_a_join() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE flags (id UUID PRIMARY KEY, flag BOOLEAN)",
        &empty(),
    )
    .unwrap();
    let yes = Uuid::new_v4();
    let no = Uuid::new_v4();
    let unset = Uuid::new_v4();
    db.execute(
        "INSERT INTO flags (id, flag) VALUES ($id, $flag)",
        &params(vec![("id", Value::Uuid(yes)), ("flag", Value::Bool(true))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO flags (id, flag) VALUES ($id, $flag)",
        &params(vec![("id", Value::Uuid(no)), ("flag", Value::Bool(false))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO flags (id, flag) VALUES ($id, NULL)",
        &params(vec![("id", Value::Uuid(unset))]),
    )
    .unwrap();

    let scan = db
        .execute("SELECT id FROM flags WHERE flag", &empty())
        .expect("a scan WHERE on a boolean column is a legal predicate");
    assert_eq!(scan.rows.len(), 1, "WHERE flag keeps only flag = TRUE");
    assert_eq!(scan.rows[0][0], Value::Uuid(yes));

    let cte = db
        .execute(
            "WITH nested AS (SELECT id, flag FROM flags) SELECT id FROM nested WHERE flag",
            &empty(),
        )
        .expect("a CTE WHERE on a boolean column uses the same predicate rules as a scan");
    assert_eq!(
        cte.rows, scan.rows,
        "the CTE filter path must keep the same row the scan path kept"
    );

    let joined = db
        .execute(
            "SELECT a.id FROM flags a INNER JOIN flags b ON a.flag AND a.id = b.id",
            &empty(),
        )
        .expect("JOIN ON a boolean column uses the same predicate rules as a scan WHERE");
    assert_eq!(
        joined.rows, scan.rows,
        "the JOIN ON path must keep the same row the scan path kept"
    );
}

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
    assert!(result.is_ok(), "idx_01_create_index_accepted");
    let q = exec(
        &db,
        "SELECT * FROM indexed WHERE name = 'alice'",
        "idx_01_create_index_accepted",
    );
    assert_eq!(q.rows.len(), 1, "idx_01_create_index_accepted");
}
