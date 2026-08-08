//! Pins the fix (`3ac3eac`, "return empty instead of panicking on an
//! inverted indexed range bound pair") for a real NaN panic
//! (`nan_real_predicate_tests.rs`) that added `range_is_empty` -- a guard
//! that compares a range predicate's lower/upper `DirectedValue` bounds and
//! skips the `BTreeMap::range` walk if they form an inverted pair.
//!
//! The guard runs on the bounds AFTER they are each independently wrapped
//! with the index's declared sort direction
//! (`wrap`/`wrap_with_dir`, `executor.rs` ~5050), but `TotalOrdDesc::cmp`
//! REVERSES comparison (`value_total_cmp(...).reverse()`,
//! `contextdb-core/src/types.rs`) -- so for a DESCENDING index, the
//! wrapped SQL-lower-bound key sorts ABOVE the wrapped SQL-upper-bound key
//! even for a perfectly ordinary, non-inverted range like `BETWEEN 1 AND
//! 5`. The guard was written to catch a GENUINELY inverted pair (a NaN
//! bound); it cannot tell that shape apart from a legitimate DESC-index
//! range, so it silently treats every DESC-indexed range query as
//! empty -- turning a would-be panic into a silent WRONG ANSWER, which is
//! worse: `SELECT score FROM t WHERE score BETWEEN 1 AND 5 ORDER BY score`
//! against a `DESC`-indexed `score` returns `[]` instead of `[1, 3, 5]`.
//! `Database::explain` reports only the coarse `Project -> Scan(table=t)`
//! shape for this query; whether the row walk actually routed through the
//! index is visible in the runtime `QueryTrace` (`physical_plan:
//! "IndexScan"`, `index_used: Some(<name>)`), which stays `IndexScan` with
//! `rows_examined: 0` under the bug -- the guard empties the walk, it does
//! not stop the planner from choosing the index.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn seed(db: &Database, values: &[i64]) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, score INTEGER)",
        &params(vec![]),
    )
    .expect("create t");
    for (i, value) in values.iter().enumerate() {
        db.execute(
            "INSERT INTO t (id, score) VALUES ($id, $score)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(i as u128 + 1))),
                ("score", Value::Int64(*value)),
            ]),
        )
        .unwrap_or_else(|err| panic!("insert score={value}: {err:?}"));
    }
}

fn query_scores(db: &Database, sql: &str, extra: Vec<(&str, Value)>) -> Vec<i64> {
    let result = db
        .execute(sql, &params(extra))
        .unwrap_or_else(|err| panic!("query `{sql}` must not error: {err:?}"));
    result
        .rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Int64(n) => n,
            ref other => panic!("expected an INTEGER score, got {other:?}"),
        })
        .collect()
}

/// RED: `BETWEEN` on a DESC-indexed INTEGER column must return
/// the matching rows, not the empty set the inverted-range guard now
/// wrongly returns.
#[test]
fn between_on_a_desc_indexed_integer_column_returns_matching_rows() {
    let db = Database::open_memory();
    seed(&db, &[1, 3, 5]);
    db.execute("CREATE INDEX idx_score ON t(score DESC)", &params(vec![]))
        .expect("create DESC index on score");

    let mut scores = query_scores(
        &db,
        "SELECT score FROM t WHERE score BETWEEN $lo AND $hi ORDER BY score",
        vec![("lo", Value::Int64(1)), ("hi", Value::Int64(5))],
    );
    scores.sort_unstable();
    assert_eq!(
        scores,
        vec![1, 3, 5],
        "BETWEEN 1 AND 5 on a DESC-indexed column must return all three rows -- the \
         inverted-range guard added for the NaN fix cannot tell a legitimate DESC-index range \
         apart from a genuinely inverted bound pair, so it silently treats this query as empty"
    );
}

/// Green guard: the identical query on an ASC-indexed twin already works
/// today -- this pins that the regression is DESC-specific, not a general
/// break of range queries.
#[test]
fn between_on_an_asc_indexed_integer_column_returns_matching_rows() {
    let db = Database::open_memory();
    seed(&db, &[1, 3, 5]);
    db.execute("CREATE INDEX idx_score ON t(score ASC)", &params(vec![]))
        .expect("create ASC index on score");

    let mut scores = query_scores(
        &db,
        "SELECT score FROM t WHERE score BETWEEN $lo AND $hi ORDER BY score",
        vec![("lo", Value::Int64(1)), ("hi", Value::Int64(5))],
    );
    scores.sort_unstable();
    assert_eq!(scores, vec![1, 3, 5]);
}

/// RED: one-sided `>` / `<` on a DESC index must also return the correct
/// rows -- these compile to the same `Range { lower, upper }` shape with
/// one side `Unbounded`, so the guard's DESC blindness should hit them
/// identically whenever the bounded side alone forms a "greater" key than
/// the unbounded side would imply (checked here as a full round-trip
/// through real results, not by reasoning about the guard in isolation).
#[test]
fn one_sided_comparisons_on_a_desc_indexed_integer_column_return_matching_rows() {
    let db = Database::open_memory();
    seed(&db, &[1, 3, 5]);
    db.execute("CREATE INDEX idx_score ON t(score DESC)", &params(vec![]))
        .expect("create DESC index on score");

    let mut gt = query_scores(
        &db,
        "SELECT score FROM t WHERE score > $x ORDER BY score",
        vec![("x", Value::Int64(1))],
    );
    gt.sort_unstable();
    assert_eq!(
        gt,
        vec![3, 5],
        "score > 1 on a DESC index must return 3 and 5"
    );

    let mut lt = query_scores(
        &db,
        "SELECT score FROM t WHERE score < $x ORDER BY score",
        vec![("x", Value::Int64(5))],
    );
    lt.sort_unstable();
    assert_eq!(
        lt,
        vec![1, 3],
        "score < 5 on a DESC index must return 1 and 3"
    );
}

/// RED: exclusive bounds (`BETWEEN`'s open-interval equivalent, spelled as
/// `> lo AND < hi`) on a DESC index must return only the strictly-interior
/// rows -- not the empty set.
#[test]
fn exclusive_bounds_on_a_desc_indexed_integer_column_return_matching_rows() {
    let db = Database::open_memory();
    seed(&db, &[1, 3, 5]);
    db.execute("CREATE INDEX idx_score ON t(score DESC)", &params(vec![]))
        .expect("create DESC index on score");

    let mut scores = query_scores(
        &db,
        "SELECT score FROM t WHERE score > $lo AND score < $hi ORDER BY score",
        vec![("lo", Value::Int64(1)), ("hi", Value::Int64(5))],
    );
    scores.sort_unstable();
    assert_eq!(
        scores,
        vec![3],
        "score > 1 AND score < 5 on a DESC index must return only 3"
    );
}

/// Green guard: a single-point `BETWEEN x AND x` (bounds equal, both
/// inclusive) on a DESC index already returns the exact-match row today --
/// the guard's `Ordering::Equal` arm only excludes an empty range when at
/// least one side is `Excluded`, and both sides here are `Included`, so
/// this shape does not hit the DESC-blindness the RED tests above pin.
/// Coverage of the bound-shape matrix per the brief.
#[test]
fn single_point_between_on_a_desc_indexed_integer_column_returns_the_matching_row() {
    let db = Database::open_memory();
    seed(&db, &[1, 3, 5]);
    db.execute("CREATE INDEX idx_score ON t(score DESC)", &params(vec![]))
        .expect("create DESC index on score");

    let scores = query_scores(
        &db,
        "SELECT score FROM t WHERE score BETWEEN $x AND $x",
        vec![("x", Value::Int64(3))],
    );
    assert_eq!(
        scores,
        vec![3],
        "BETWEEN 3 AND 3 on a DESC index must return the single matching row"
    );
}

/// Green guard: the original NaN fix's promise must survive the re-fix --
/// a NaN-bounded range query on a DESC-indexed REAL column must still
/// return empty without panicking (mirrors
/// `nan_real_predicate_tests.rs`'s ASC-index / non-indexed coverage, this
/// pin is the DESC-index leg of that same promise).
#[test]
fn nan_bounded_range_query_on_a_desc_indexed_real_column_returns_empty_without_panic() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE r (id UUID PRIMARY KEY, score REAL)",
        &params(vec![]),
    )
    .expect("create r");
    for (i, value) in [1.0_f64, 2.0, 3.0].iter().enumerate() {
        db.execute(
            "INSERT INTO r (id, score) VALUES ($id, $score)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(i as u128 + 1))),
                ("score", Value::Float64(*value)),
            ]),
        )
        .expect("seed r");
    }
    db.execute("CREATE INDEX idx_r_score ON r(score DESC)", &params(vec![]))
        .expect("create DESC index on r.score");

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        db.execute(
            "SELECT id FROM r WHERE score BETWEEN $lo AND $hi",
            &params(vec![
                ("lo", Value::Float64(f64::NAN)),
                ("hi", Value::Float64(5.0)),
            ]),
        )
    }));

    let result = outcome.unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        panic!(
            "a NaN-bounded range query on a DESC-indexed REAL column must not panic (the \
             original NaN fix's promise must survive the DESC re-fix): {message}"
        )
    });
    let result = result.expect("a NaN-bounded range query must not return a typed error either");
    assert!(
        result.rows.is_empty(),
        "a NaN-bounded range query on a DESC index must return empty, matching the ASC/\
         non-indexed behavior: {:?}",
        result.rows
    );
}

fn seed_composite(db: &Database, rows: &[(i64, i64)]) {
    db.execute(
        "CREATE TABLE c (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &params(vec![]),
    )
    .expect("create c");
    for (i, (a, b)) in rows.iter().enumerate() {
        db.execute(
            "INSERT INTO c (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(i as u128 + 1))),
                ("a", Value::Int64(*a)),
                ("b", Value::Int64(*b)),
            ]),
        )
        .unwrap_or_else(|err| panic!("insert a={a} b={b}: {err:?}"));
    }
}

fn query_a_via_composite(
    db: &Database,
    sql: &str,
    extra: Vec<(&str, Value)>,
) -> (Vec<i64>, contextdb_engine::QueryTrace) {
    let result = db
        .execute(sql, &params(extra))
        .unwrap_or_else(|err| panic!("query `{sql}` must not error: {err:?}"));
    let values = result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int64(n) => n,
            ref other => panic!("expected an INTEGER a, got {other:?}"),
        })
        .collect();
    (values, result.trace)
}

/// The DESC-leading-column fix has two arms in the executor: single-column
/// (used by every test above) and composite, when the DESC-indexed column
/// leads a multi-column index. This pins the composite arm the same way --
/// inclusive, exclusive, and one-sided bounds on the leading `a DESC`
/// column of a `(a DESC, b ASC)` index must return the matching rows, and
/// the runtime trace must show the walk actually routed through that index
/// (not a fallback full scan, which would pass on rows-correctness alone
/// while silently hiding a planner regression).
#[test]
fn composite_index_with_a_desc_leading_column_range_returns_matching_rows_via_index_scan() {
    let db = Database::open_memory();
    seed_composite(&db, &[(1, 100), (3, 200), (5, 300)]);
    db.execute(
        "CREATE INDEX idx_c_a_b ON c(a DESC, b ASC)",
        &params(vec![]),
    )
    .expect("create composite DESC-leading index on c");

    let (mut inclusive, inclusive_trace) = query_a_via_composite(
        &db,
        "SELECT a FROM c WHERE a BETWEEN $lo AND $hi ORDER BY a",
        vec![("lo", Value::Int64(1)), ("hi", Value::Int64(5))],
    );
    inclusive.sort_unstable();
    assert_eq!(
        inclusive,
        vec![1, 3, 5],
        "BETWEEN 1 AND 5 on the DESC-leading column of a composite index must return all \
         three rows"
    );
    assert_eq!(
        inclusive_trace.physical_plan, "IndexScan",
        "the inclusive range must route through IndexScan, not a fallback full scan: {:?}",
        inclusive_trace
    );
    assert_eq!(
        inclusive_trace.index_used.as_deref(),
        Some("idx_c_a_b"),
        "the inclusive range must route through the composite index: {:?}",
        inclusive_trace
    );

    let (mut exclusive, exclusive_trace) = query_a_via_composite(
        &db,
        "SELECT a FROM c WHERE a > $lo AND a < $hi ORDER BY a",
        vec![("lo", Value::Int64(1)), ("hi", Value::Int64(5))],
    );
    exclusive.sort_unstable();
    assert_eq!(
        exclusive,
        vec![3],
        "a > 1 AND a < 5 on the DESC-leading column of a composite index must return only 3"
    );
    assert_eq!(exclusive_trace.physical_plan, "IndexScan");
    assert_eq!(exclusive_trace.index_used.as_deref(), Some("idx_c_a_b"));

    let (mut gt, gt_trace) = query_a_via_composite(
        &db,
        "SELECT a FROM c WHERE a > $x ORDER BY a",
        vec![("x", Value::Int64(1))],
    );
    gt.sort_unstable();
    assert_eq!(
        gt,
        vec![3, 5],
        "a > 1 (one-sided) on the DESC-leading column of a composite index must return 3 and 5"
    );
    assert_eq!(gt_trace.physical_plan, "IndexScan");
    assert_eq!(gt_trace.index_used.as_deref(), Some("idx_c_a_b"));

    let (mut lt, lt_trace) = query_a_via_composite(
        &db,
        "SELECT a FROM c WHERE a < $x ORDER BY a",
        vec![("x", Value::Int64(5))],
    );
    lt.sort_unstable();
    assert_eq!(
        lt,
        vec![1, 3],
        "a < 5 (one-sided) on the DESC-leading column of a composite index must return 1 and 3"
    );
    assert_eq!(lt_trace.physical_plan, "IndexScan");
    assert_eq!(lt_trace.index_used.as_deref(), Some("idx_c_a_b"));

    let (inverted, inverted_trace) = query_a_via_composite(
        &db,
        "SELECT a FROM c WHERE a > $hi AND a < $lo ORDER BY a",
        vec![("lo", Value::Int64(1)), ("hi", Value::Int64(5))],
    );
    assert!(
        inverted.is_empty(),
        "a > 5 AND a < 1 is a genuinely inverted bound pair and must return empty on the \
         composite index, matching the single-column guard's promise: {:?}",
        inverted
    );
    assert_eq!(inverted_trace.physical_plan, "IndexScan");
    assert_eq!(inverted_trace.index_used.as_deref(), Some("idx_c_a_b"));
}
