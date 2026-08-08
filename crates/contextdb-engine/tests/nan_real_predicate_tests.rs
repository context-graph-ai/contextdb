//! The zero-sampling doc tester reported that "inserting bare `nan` into an
//! INDEXED REAL column panics the process ... instead of returning a typed
//! error". Verified before authoring: a plain `INSERT` of `Value::Float64
//! (f64::NAN)` into an indexed REAL column (single insert, second insert
//! after an existing row, two NaN rows, an `UPDATE` to NaN) never panics --
//! every one of those returns `Ok`. The REAL, reproducible panic is a
//! QUERY: a bounded range predicate (`BETWEEN $lo AND $hi`, which compiles
//! to the same lower/upper-bound `BTreeMap::range` walk any two-sided
//! comparison would) against an INDEXED REAL column, where one bound is
//! `NaN`, panics with "range start is greater than range end in BTreeMap"
//! -- `f64::total_cmp` (which the index's `DirectedValue`/`value_total_cmp`
//! ordering uses, `contextdb-core/src/types.rs`) places the canonical NaN
//! constant AFTER every other `f64`, so `NaN` as the lower bound sorts
//! ABOVE a normal upper bound, and `BTreeMap::range` refuses an inverted
//! bound pair outright (a Rust std panic, not one of this engine's own
//! typed errors). The identical query against a table with NO index on the
//! column does not panic -- it returns an empty result (the residual
//! predicate `x > NaN` / `x < NaN` is always `false` under IEEE-754, so a
//! full scan naturally excludes every row), which is the sane, consistent
//! behavior the indexed path should match instead of panicking.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn seed(db: &Database, values: &[f64]) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, score REAL)",
        &params(vec![]),
    )
    .expect("create t");
    for (i, value) in values.iter().enumerate() {
        db.execute(
            "INSERT INTO t (id, score) VALUES ($id, $score)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(i as u128 + 1))),
                ("score", Value::Float64(*value)),
            ]),
        )
        .unwrap_or_else(|err| panic!("insert score={value}: {err:?}"));
    }
}

/// Green guard, checked FIRST per the brief: a plain `INSERT` of `NaN` into
/// an INDEXED REAL column does not panic and does not itself refuse --
/// `NaN` is a structurally valid `REAL` value to store, the defect is only
/// in a later RANGE QUERY against it. This is not the bug; it establishes
/// that INSERT was never the problem so the RED below is not duplicating
/// coverage that already exists.
#[test]
fn inserting_nan_into_an_indexed_real_column_does_not_panic() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, score REAL)",
        &params(vec![]),
    )
    .expect("create t");
    db.execute("CREATE INDEX idx_score ON t(score)", &params(vec![]))
        .expect("create index on score");

    let id = Uuid::from_u128(1);
    db.execute(
        "INSERT INTO t (id, score) VALUES ($id, $score)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("score", Value::Float64(f64::NAN)),
        ]),
    )
    .expect("inserting NaN into an indexed REAL column must not panic or refuse");
}

/// Green guard: the same bounded range query with a NaN lower bound,
/// against a table with NO index on `score`, does not panic -- it returns
/// an empty result set. This is the "presumably sane" behavior the brief
/// asks to pin consistency with.
#[test]
fn between_query_with_nan_lower_bound_on_a_non_indexed_real_column_returns_empty() {
    let db = Database::open_memory();
    seed(&db, &[1.0, 2.0, 3.0]);

    let result = db
        .execute(
            "SELECT id FROM t WHERE score BETWEEN $lo AND $hi",
            &params(vec![
                ("lo", Value::Float64(f64::NAN)),
                ("hi", Value::Float64(5.0)),
            ]),
        )
        .expect("a NaN-bounded range query on a non-indexed column must not error");
    assert!(
        result.rows.is_empty(),
        "a NaN lower bound must match nothing (every comparison against NaN is false under \
         IEEE-754), not silently include rows: {:?}",
        result.rows
    );
}

/// RED, live repro: the identical query, against an INDEXED `score`
/// column, must not panic the process -- it should behave the same way the
/// non-indexed twin above already does (empty result), or refuse with a
/// typed error; either is acceptable, but a raw Rust panic is not.
#[test]
fn between_query_with_nan_lower_bound_on_an_indexed_real_column_does_not_panic() {
    let db = Database::open_memory();
    seed(&db, &[1.0, 2.0, 3.0]);
    db.execute("CREATE INDEX idx_score ON t(score)", &params(vec![]))
        .expect("create index on score");

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        db.execute(
            "SELECT id FROM t WHERE score BETWEEN $lo AND $hi",
            &params(vec![
                ("lo", Value::Float64(f64::NAN)),
                ("hi", Value::Float64(5.0)),
            ]),
        )
    }));

    let result = match outcome {
        Ok(result) => result,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            panic!(
                "a NaN-bounded BETWEEN query on an INDEXED REAL column must return a typed \
                 result (Ok with the sane empty set, or a typed Err), not panic the process -- \
                 the index's DirectedValue ordering (f64::total_cmp) sorts the canonical NaN \
                 constant after every other f64, so a NaN lower bound sorts above a normal \
                 upper bound and BTreeMap::range refuses the inverted bound pair with a raw \
                 std panic. Panic payload: {message}"
            );
        }
    };

    match result {
        Ok(query_result) => assert!(
            query_result.rows.is_empty(),
            "a NaN lower bound must match nothing, matching the non-indexed column's own \
             behavior, not silently include rows: {:?}",
            query_result.rows
        ),
        Err(err) => {
            // A typed refusal is an acceptable alternative to the sane
            // empty-result behavior -- either way, the process must stay
            // alive and the caller gets something to handle, not a panic.
            eprintln!(
                "NaN-bounded indexed range query returned a typed error instead of an empty \
                 result: {err:?}"
            );
        }
    }
}
