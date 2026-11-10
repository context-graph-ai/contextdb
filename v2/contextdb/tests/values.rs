//! Values: what is stored in a column of each type comes back equal, before and after the store
//! is closed and reopened.

mod support;

use contextdb::serde_json::json;
use contextdb::{Uuid, Value, params};

#[test]
fn every_column_type_comes_back_as_it_went_in_after_reopen() {
    let folder = support::folder("values-come-back");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE v (id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, b BOOLEAN, u UUID, \
         ts TIMESTAMP, j JSON, e VECTOR(3))",
    );

    let text = |s: &str| Value::Text(s.to_owned());
    // Long enough to be kept beside its row rather than in it.
    let long = "long text, ".repeat(400);
    let stored: Vec<[Value; 8]> = vec![
        [
            Value::Int64(0),
            Value::Float64(0.0),
            text(""),
            Value::Bool(false),
            Value::Uuid(Uuid::from_u128(0)),
            Value::Timestamp(0),
            Value::Json(json!({})),
            Value::Vector(vec![0.0, 0.0, 0.0]),
        ],
        [
            Value::Int64(i64::MIN),
            Value::Float64(f64::MIN_POSITIVE),
            text("plain"),
            Value::Bool(true),
            Value::Uuid(Uuid::from_u128(1)),
            Value::Timestamp(1_700_000_000),
            Value::Json(json!({ "k": [1, 2.5, "x", null, true], "nested": { "a": {} } })),
            Value::Vector(vec![0.1, -0.2, 0.3]),
        ],
        [
            Value::Int64(i64::MAX),
            Value::Float64(f64::MAX),
            text("it's \"quoted\"; na\u{ef}ve \u{65e5}\u{672c}\u{8a9e} \u{1f642}\nsecond line"),
            Value::Bool(true),
            Value::Uuid(Uuid::from_u128(u128::MAX)),
            Value::Timestamp(253_402_300_799),
            Value::Json(json!([long.as_str(), 42, -1.5])),
            Value::Vector(vec![f32::MAX, f32::MIN_POSITIVE, -1.0]),
        ],
        [
            Value::Int64(-1),
            Value::Float64(-1.5),
            text(&long),
            Value::Bool(false),
            Value::Uuid(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)),
            Value::Timestamp(1),
            Value::Json(json!("text")),
            Value::Vector(vec![1.0, 2.0, 3.0]),
        ],
        // No value in any column.
        [const { Value::Null }; 8],
    ];
    for (id, [i, r, s, b, u, ts, j, e]) in (1_i64..).zip(stored.iter().cloned()) {
        db.execute(
            "INSERT INTO v (id, i, r, s, b, u, ts, j, e) \
             VALUES ($id, $i, $r, $s, $b, $u, $ts, $j, $e)",
            &params! {
                "id" => id, "i" => i, "r" => r, "s" => s, "b" => b, "u" => u, "ts" => ts,
                "j" => j, "e" => e,
            },
        )
        .unwrap_or_else(|error| panic!("row {id} was refused: {error:?}"));
    }

    let select = "SELECT i, r, s, b, u, ts, j, e FROM v ORDER BY id";
    assert_eq!(
        support::rows(&db, select),
        stored,
        "read back from the open store"
    );
    support::close(db);
    assert_eq!(
        support::rows(&support::open(&folder), select),
        stored,
        "read back after reopening"
    );
}

#[test]
fn bound_values_insert_a_row_and_a_bound_boolean_filters() {
    let folder = support::folder("bound-values");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE pairs (a INTEGER PRIMARY KEY, b TEXT)");
    for (a, b) in [(1_i64, "one"), (2, "two")] {
        let stored = db
            .execute(
                "INSERT INTO pairs (a, b) VALUES ($a, $b)",
                &params! { "a" => a, "b" => b },
            )
            .expect("insert bound values");
        assert_eq!(stored.rows_affected(), 1);
    }
    let pair = |a: i64, b: &str| [Value::Int64(a), Value::Text(b.to_owned())];
    assert_eq!(
        support::rows(&db, "SELECT a, b FROM pairs ORDER BY a"),
        [pair(1, "one"), pair(2, "two")]
    );

    // A bound boolean is the whole filter: true keeps every row, false keeps none.
    let filtered = "SELECT a, b FROM pairs WHERE $flag ORDER BY a";
    let kept = db
        .execute(filtered, &params! { "flag" => true })
        .expect("filter by a bound true");
    assert_eq!(kept.rows(), [pair(1, "one"), pair(2, "two")]);
    let kept = db
        .execute(filtered, &params! { "flag" => false })
        .expect("filter by a bound false");
    assert!(
        kept.rows().is_empty(),
        "a bound false kept {:?}",
        kept.rows()
    );
}

#[test]
fn nan_sorts_above_every_finite_real() {
    let folder = support::folder("nan-order");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE reals (id INTEGER PRIMARY KEY, r REAL)");
    support::run(&db, "CREATE INDEX reals_r ON reals (r)");
    let stored = [
        (1_i64, Value::Float64(2.5)),
        (2, Value::Float64(f64::NAN)),
        (3, Value::Float64(-1.5)),
        (4, Value::Null),
        (5, Value::Float64(f64::MAX)),
        (6, Value::Float64(0.0)),
        (7, Value::Float64(f64::MIN)),
    ];
    for (id, r) in stored {
        db.execute(
            "INSERT INTO reals (id, r) VALUES ($id, $r)",
            &params! { "id" => id, "r" => r },
        )
        .unwrap_or_else(|error| panic!("row {id} was refused: {error:?}"));
    }
    let ids = |sql: &str| -> Vec<i64> {
        support::rows(&db, sql)
            .iter()
            .map(|row| match row.as_slice() {
                // The value that is not a number comes back as one that is not a number.
                [Value::Int64(2), Value::Float64(r)] if r.is_nan() => 2,
                [Value::Int64(id), _] if *id != 2 => *id,
                other => panic!("{sql}\nreturned the row {other:?}"),
            })
            .collect()
    };

    // Going up: the finite values in order, then the one that is not a number, then no value.
    assert_eq!(
        ids("SELECT id, r FROM reals ORDER BY r"),
        [7, 3, 6, 1, 5, 2, 4]
    );
    // Going down: the reverse.
    assert_eq!(
        ids("SELECT id, r FROM reals ORDER BY r DESC"),
        [4, 2, 5, 1, 6, 3, 7]
    );
}
