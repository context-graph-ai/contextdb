//! FLOAT compares the way PostgreSQL's float8 does on every route: `-0.0` and
//! `0.0` are one value, every NaN equals every other NaN, and NaN sorts above
//! every number. The index range, the declared count, the transaction's own
//! view, a scan with no index, and the store after reopen all give one answer.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).into(), value.clone()))
        .collect()
}

fn id(n: u128) -> Value {
    Value::Uuid(Uuid::from_u128(n))
}

fn open(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the float fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn count(answer: QueryResult) -> i64 {
    match answer.rows.as_slice() {
        [row] => match row.as_slice() {
            [Value::Int64(count)] => *count,
            other => panic!("a count is one integer: {other:?}"),
        },
        other => panic!("a count is one row: {other:?}"),
    }
}

const INSERT: &str = "INSERT INTO {t} (id, score) VALUES ($id, $score)";

fn schema(db: &Database) {
    for statement in [
        "CREATE TABLE indexed (id UUID PRIMARY KEY, score FLOAT NOT NULL) SYNC OFF",
        "CREATE INDEX idx_indexed_score ON indexed (score, id)",
        "ALTER TABLE indexed SET MAINTAIN COUNT (score)",
        "CREATE TABLE plain (id UUID PRIMARY KEY, score FLOAT NOT NULL) SYNC OFF",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
}

fn insert(db: &Database, tx: Option<contextdb_core::TxId>, n: u128, score: f64) {
    for table in ["indexed", "plain"] {
        let sql = INSERT.replace("{t}", table);
        let bound = params(&[("id", id(n)), ("score", Value::Float64(score))]);
        match tx {
            Some(tx) => db.execute_in_tx(tx, &sql, &bound).unwrap(),
            None => db.execute(&sql, &bound).unwrap(),
        };
    }
}

/// Every route's count of rows equal to `probe`, labelled.
fn equal_counts(
    db: &Database,
    tx: Option<contextdb_core::TxId>,
    probe: f64,
) -> Vec<(&'static str, i64)> {
    let bound = params(&[("score", Value::Float64(probe))]);
    let run = |sql: &str| {
        count(match tx {
            Some(tx) => db.execute_in_tx(tx, sql, &bound).unwrap(),
            None => db.execute(sql, &bound).unwrap(),
        })
    };
    vec![
        (
            "declared count",
            run("SELECT COUNT(*) FROM indexed WHERE score = $score"),
        ),
        (
            "index range",
            run("SELECT COUNT(id) FROM indexed WHERE score = $score"),
        ),
        (
            "unindexed scan",
            run("SELECT COUNT(id) FROM plain WHERE score = $score"),
        ),
    ]
}

fn assert_all(label: &str, counts: Vec<(&'static str, i64)>, expected: i64) {
    for (route, actual) in &counts {
        assert_eq!(
            *actual, expected,
            "{label}: the {route} answered {actual}, every route must answer {expected}: {counts:?}"
        );
    }
}

#[test]
fn float_equality_and_order_follow_postgresql_on_every_route() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("float8.redb");
    let db = open(&path);
    schema(&db);
    insert(&db, None, 1, 0.0);
    insert(&db, None, 2, -0.0);
    insert(&db, None, 3, f64::NAN);
    insert(&db, None, 4, -f64::NAN);
    insert(&db, None, 5, 1.0);

    assert_all("committed 0.0", equal_counts(&db, None, 0.0), 2);
    assert_all("committed -0.0", equal_counts(&db, None, -0.0), 2);
    assert_all("committed NaN", equal_counts(&db, None, f64::NAN), 2);
    assert_all("committed -NaN", equal_counts(&db, None, -f64::NAN), 2);

    // Ordering: -0.0 is not below 0.0, and NaN is above every number.
    let below_zero = count(
        db.execute(
            "SELECT COUNT(id) FROM plain WHERE score < $score",
            &params(&[("score", Value::Float64(0.0))]),
        )
        .unwrap(),
    );
    assert_eq!(below_zero, 0, "-0.0 is not less than 0.0");
    let above_max = count(
        db.execute(
            "SELECT COUNT(id) FROM plain WHERE score > $score",
            &params(&[("score", Value::Float64(f64::MAX))]),
        )
        .unwrap(),
    );
    assert_eq!(above_max, 2, "both NaNs sort above every number");

    // The writer's own transaction, served and ordinary, then after commit.
    let tx = db.begin().unwrap();
    insert(&db, Some(tx), 6, -0.0);
    insert(&db, Some(tx), 7, f64::NAN);
    assert_all("in-transaction 0.0", equal_counts(&db, Some(tx), 0.0), 3);
    assert_all("in-transaction NaN", equal_counts(&db, Some(tx), f64::NAN), 3);
    db.commit(tx).unwrap();
    assert_all("post-commit 0.0", equal_counts(&db, None, 0.0), 3);
    assert_all("post-commit NaN", equal_counts(&db, None, f64::NAN), 3);
    db.close().unwrap();

    let db = open(&path);
    assert_all("reopened 0.0", equal_counts(&db, None, 0.0), 3);
    assert_all("reopened NaN", equal_counts(&db, None, f64::NAN), 3);
    db.close().unwrap();
}

/// Rows from the writer, from inside a transaction, from the closed store's
/// read-only file route, and from the reopened writer.
fn rows_on_every_route(
    path: &std::path::Path,
    sql: &str,
    bound: &HashMap<String, Value>,
) -> Vec<(&'static str, Vec<Vec<Value>>)> {
    let db = open(path);
    let committed = db.execute(sql, bound).unwrap().rows;
    let tx = db.begin().unwrap();
    let in_transaction = db.execute_in_tx(tx, sql, bound).unwrap().rows;
    db.rollback(tx).unwrap();
    db.close().unwrap();
    drop(db);
    let reader = contextdb_engine::ReadSession::open(path).expect("read-only open");
    let file_route = reader.execute(sql, bound).unwrap().rows;
    drop(reader);
    let db = open(path);
    let reopened = db.execute(sql, bound).unwrap().rows;
    db.close().unwrap();
    vec![
        ("committed", committed),
        ("in-transaction", in_transaction),
        ("file route", file_route),
        ("reopened", reopened),
    ]
}

#[test]
fn distinct_keeps_one_row_per_float8_value() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("float8-distinct.redb");
    let db = open(&path);
    schema(&db);
    for (n, score) in [0.0, -0.0, f64::NAN, -f64::NAN, 1.0].into_iter().enumerate() {
        insert(&db, None, n as u128 + 1, score);
    }
    db.close().unwrap();
    drop(db);

    let none = HashMap::new();
    for (route, rows) in rows_on_every_route(&path, "SELECT DISTINCT score FROM plain", &none) {
        assert_eq!(
            rows.len(),
            3,
            "{route}: DISTINCT over {{0, -0, NaN, -NaN, 1}} is {{0, NaN, 1}}: {rows:?}"
        );
    }
}

#[test]
fn an_indexed_float_range_with_one_bound_answers_like_a_scan() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("float8-range.redb");
    let db = open(&path);
    for statement in [
        "CREATE TABLE grouped (id UUID PRIMARY KEY, grp INTEGER NOT NULL, score FLOAT NOT NULL) SYNC OFF",
        "CREATE INDEX idx_grouped_grp_score ON grouped (grp, score)",
        "CREATE TABLE loose (id UUID PRIMARY KEY, grp INTEGER NOT NULL, score FLOAT NOT NULL) SYNC OFF",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    for (n, score) in [0.0, -0.0, f64::NAN, -f64::NAN, 1.0, f64::NEG_INFINITY]
        .into_iter()
        .enumerate()
    {
        for table in ["grouped", "loose"] {
            for grp in [1_i64, 2] {
                db.execute(
                    &format!("INSERT INTO {table} (id, grp, score) VALUES ($id, $grp, $score)"),
                    &params(&[
                        ("id", id(n as u128 + 1 + 100 * grp as u128)),
                        ("grp", Value::Int64(grp)),
                        ("score", Value::Float64(score)),
                    ]),
                )
                .unwrap();
            }
        }
    }
    db.close().unwrap();
    drop(db);

    // Under float8 order: -inf < -0 = 0 < 1 < NaN = -NaN. The open edge of a
    // one-bound range after the `grp` prefix is the column's least value.
    for (operator, expected) in [("<=", 4), ("<", 3), (">=", 3), (">", 2)] {
        let bound = params(&[("grp", Value::Int64(1)), ("score", Value::Float64(1.0))]);
        for table in ["grouped", "loose"] {
            let sql = format!("SELECT id FROM {table} WHERE grp = $grp AND score {operator} $score");
            for (route, rows) in rows_on_every_route(&path, &sql, &bound) {
                assert_eq!(
                    rows.len(),
                    expected,
                    "{route}: {table} WHERE grp = 1 AND score {operator} 1.0 answered {} rows",
                    rows.len()
                );
            }
        }
    }
    let db = open(&path);
    let plan = db
        .execute(
            "SELECT id FROM grouped WHERE grp = $grp AND score <= $score",
            &params(&[("grp", Value::Int64(1)), ("score", Value::Float64(1.0))]),
        )
        .unwrap()
        .trace
        .physical_plan;
    assert_eq!(plan, "IndexScan", "the one-bound range reads the index");
    db.close().unwrap();
}

#[test]
fn a_rank_policy_joins_float_keys_by_float8_equality() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("float8-rank.redb");
    let db = open(&path);
    for statement in [
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, bucket REAL, weight REAL) SYNC OFF",
        "CREATE INDEX outcomes_bucket_idx ON outcomes (bucket)",
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            bucket REAL,
            embedding VECTOR(2) RANK_POLICY (
                JOIN outcomes ON bucket,
                FORMULA 'coalesce({weight}, 0.0)',
                SORT_KEY effective
            )
        ) SYNC OFF",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    // Each decision's bucket is float8-equal, but not bit-equal, to exactly
    // one outcome's: -NaN to NaN and -0.0 to 0.0. 5.0 is a bit-equal control.
    for (n, bucket, embedding) in [
        (1, -f64::NAN, vec![0.6_f32, 0.8]),
        (2, -0.0, vec![0.8, 0.6]),
        (3, 5.0, vec![0.5, 0.866_025_4]),
    ] {
        db.execute(
            "INSERT INTO decisions (id, bucket, embedding) VALUES ($id, $bucket, $embedding)",
            &params(&[
                ("id", id(n)),
                ("bucket", Value::Float64(bucket)),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .unwrap();
    }
    for (n, bucket, weight) in [(11, f64::NAN, 10.0), (12, 0.0, 5.0), (13, 5.0, 20.0)] {
        db.execute(
            "INSERT INTO outcomes (id, bucket, weight) VALUES ($id, $bucket, $weight)",
            &params(&[
                ("id", id(n)),
                ("bucket", Value::Float64(bucket)),
                ("weight", Value::Float64(weight)),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
    drop(db);

    // Joined weights 20, 10, 5 order the decisions 3, 1, 2. Had only the
    // bit-equal control joined, the order would be 3, 2, 1; had nothing
    // joined, similarity alone would give 2, 1, 3.
    let query = params(&[("q", Value::Vector(vec![1.0, 0.0]))]);
    for (route, rows) in rows_on_every_route(
        &path,
        "SELECT id FROM decisions ORDER BY embedding <=> $q USE RANK effective LIMIT 3",
        &query,
    ) {
        assert_eq!(
            rows,
            vec![vec![id(3)], vec![id(1)], vec![id(2)]],
            "{route}: every decision joins the outcome whose bucket it equals"
        );
    }
}
