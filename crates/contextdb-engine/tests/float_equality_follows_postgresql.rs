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
