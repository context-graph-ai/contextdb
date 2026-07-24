//! ORDER BY on an unknown, ambiguous, or non-column sort key must be a
//! plan-time error — never a silently unsorted (or wrongly sorted) `Ok`
//! result. This is the same contract WHERE already holds for its predicate
//! expressions, extended to ORDER BY: the `PhysicalPlan::Sort` comparator in
//! `crates/contextdb-engine/src/executor.rs` swallows ANY sort-key
//! evaluation error into `Ordering::Equal` (`Err(_) => return
//! Ordering::Equal`), and a non-`Expr::Column` sort key is rejected even
//! earlier, before evaluation is attempted (`let Expr::Column(column_ref) =
//! &key.expr else { return Ordering::Equal; }`) — both paths silently no-op
//! the sort instead of erroring or (for a legal SELECT-list alias)
//! resolving it.
//!
//! The contract has two halves, and both must hold: alias substitution (so
//! a legal SELECT-list alias in ORDER BY actually sorts) AND validation (so
//! an unknown/ambiguous/unsupported sort key errors loudly) — without
//! widening into rejecting standard SQL that happens to look similar.
//!
//! All fixtures insert values in DESCENDING order (10, 9, 8) so insertion
//! order and sorted order differ. An ascending fixture lets a broken (no-op)
//! sort and a working sort return the identical rows, passing for the wrong
//! reason — confirmed to nearly happen during this defect's own
//! investigation.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn seed_t_descending(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .expect("CREATE TABLE t");
    for b in [10i64, 9, 8] {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(1)),
                ("b", Value::Int64(b)),
            ]),
        )
        .expect("INSERT t");
    }
}

fn int_col(value: &Value) -> i64 {
    match value {
        Value::Int64(v) => *v,
        other => panic!("expected Value::Int64, got {other:?}"),
    }
}

// ============================================================================
// ORDER BY a nonexistent column on a NON-EMPTY table must error,
// never silently return rows in insertion (unsorted) order.
// ============================================================================
#[test]
fn order_by_nonexistent_column_on_nonempty_table_is_column_not_found_error() {
    let db = Database::open_memory();
    seed_t_descending(&db);

    let result = db.execute("SELECT b FROM t ORDER BY ghost", &empty());
    let err = result.expect_err(
        "ORDER BY referencing a nonexistent column must be an error, not a \
         silently unsorted Ok",
    );
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "ghost", "ColumnNotFound.column mismatch");
        }
        other => panic!("expected Error::ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// The same reference against an EMPTY table: pins genuine plan-time
// validation rather than a check that only runs while comparing real rows
// during the sort (mirrors the equivalent WHERE-clause empty-table case in
// `where_clause_column_validation_tests.rs`).
// ============================================================================
#[test]
fn order_by_nonexistent_column_on_empty_table_is_column_not_found_error() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .expect("CREATE TABLE t");
    // Deliberately no rows inserted.

    let result = db.execute("SELECT b FROM t ORDER BY ghost", &empty());
    let err = result.expect_err(
        "ORDER BY on an empty table must still validate the sort key at plan \
         time, not only while comparing rows during a sort that never runs",
    );
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "t", "ColumnNotFound.table mismatch");
            assert_eq!(column, "ghost", "ColumnNotFound.column mismatch");
        }
        other => panic!("expected Error::ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// Positive control that forces alias substitution rather than a
// blanket reject: `SELECT b AS x FROM t ORDER BY x` is standard SQL (ORDER
// BY may name a SELECT-list output alias) and must actually SORT.
//
// `Sort` is planned BELOW `Project` in `plan_select_body`, so the alias `x`
// is not among the Sort input's columns at all — a naive lookup for `x`
// fails exactly like the lookup for a genuinely unknown column would. A
// validator that rejects any sort key not literally present in the Sort
// input (the convenient fix matching only the nonexistent-column cases
// above) would turn this into a hard refusal of valid SQL instead of a
// working sort — this test requires the harder, correct behavior: substitute
// the alias against the SELECT list before validating/sorting.
// ============================================================================
#[test]
fn order_by_select_list_alias_must_sort_not_silently_pass_through() {
    let db = Database::open_memory();
    seed_t_descending(&db);

    let r = db
        .execute("SELECT b AS x FROM t ORDER BY x", &empty())
        .expect("ORDER BY a SELECT-list alias is standard SQL and must succeed");
    let got: Vec<i64> = r.rows.iter().map(|row| int_col(&row[0])).collect();
    assert_eq!(
        got,
        vec![8, 9, 10],
        "ORDER BY x must actually sort by the aliased column b; got {:?} \
         (insertion order [10, 9, 8] would mean the alias silently failed to \
         sort)",
        r.rows
    );
}

// ============================================================================
// ORDER BY an arithmetic expression (`a + 1`) is standard SQL and must
// actually SORT by the evaluated value — widening this contract from an
// earlier revision of this test, which pinned `a + 1` as a typed refusal.
// That guard's PURPOSE — never silently accept a sort key and leave the
// result no-op-unsorted — is still honored: the engine now genuinely
// evaluates the expression per row (the same arithmetic a SELECT-list
// expression already evaluates) rather than either refusing it or silently
// ignoring it. A genuinely unsupported ORDER BY form still gets the typed
// refusal (see the nonexistent-column and ambiguous-column cases in this
// file).
//
// Fixture: `a` is inserted in an order that does NOT match `a`'s own
// ascending order (3, 1, 2), so a no-op "sort" would be observably wrong —
// the same discipline this file's header describes for the descending `b`
// fixtures.
// ============================================================================
#[test]
fn order_by_arithmetic_expression_sorts_by_the_evaluated_value() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .expect("CREATE TABLE t");
    for (a, b) in [(3i64, 10i64), (1, 9), (2, 8)] {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(a)),
                ("b", Value::Int64(b)),
            ]),
        )
        .expect("INSERT t");
    }

    let r = db
        .execute("SELECT b FROM t ORDER BY a + 1", &empty())
        .expect("ORDER BY a + 1 is standard SQL arithmetic and must succeed");
    let got: Vec<i64> = r.rows.iter().map(|row| int_col(&row[0])).collect();
    assert_eq!(
        got,
        vec![9, 8, 10],
        "must sort ascending by a + 1 = [2, 3, 4] (a = [1, 2, 3]), got {:?}",
        r.rows
    );
}

// ============================================================================
// An unqualified column reference in ORDER BY that is AMBIGUOUS
// across a JOIN (both joined tables declare a same-named column, `v`) must
// surface an ambiguity error — never silently swallow it into an unsorted
// result. Descending insertion order on the left table makes a no-op sort
// observably wrong rather than merely untested; the qualified positive
// control (`ORDER BY jl.v`) sorts correctly (verified separately, not
// asserted here to keep this test focused).
//
// Asserts only `is_err()` for the same reason as the non-column-expression
// case above — the expected classification is ambiguity (mirroring the
// existing `Error::PlanError("ambiguous column reference: ...")` a
// Project-column resolution already produces), not `ColumnNotFound`, but the
// exact error shape is an implementation detail outside this test's
// contract.
// ============================================================================
#[test]
fn ambiguous_order_by_column_across_join_is_an_error_not_silently_unsorted() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE jl (id UUID PRIMARY KEY, v INTEGER)", &empty())
        .expect("CREATE TABLE jl");
    db.execute("CREATE TABLE jr (id UUID PRIMARY KEY, v INTEGER)", &empty())
        .expect("CREATE TABLE jr");
    let mut ids = Vec::new();
    for v in [10i64, 9, 8] {
        let id = Uuid::new_v4();
        db.execute(
            "INSERT INTO jl (id, v) VALUES ($id, $v)",
            &params(vec![("id", Value::Uuid(id)), ("v", Value::Int64(v))]),
        )
        .expect("INSERT jl");
        ids.push(id);
    }
    for (id, v) in ids.iter().zip([100i64, 200, 300]) {
        db.execute(
            "INSERT INTO jr (id, v) VALUES ($id, $v)",
            &params(vec![("id", Value::Uuid(*id)), ("v", Value::Int64(v))]),
        )
        .expect("INSERT jr");
    }

    let result = db.execute(
        "SELECT jl.v AS lv, jr.v AS rv FROM jl INNER JOIN jr ON jl.id = jr.id ORDER BY v",
        &empty(),
    );
    assert!(
        result.is_err(),
        "an unqualified ORDER BY column that is ambiguous across a JOIN (both \
         `jl` and `jr` declare `v`) must be an error, not silently returned \
         unsorted: got {:?}",
        result
    );
}

// ============================================================================
// Regression lock (documentary; no new test authored here): a
// vector-similarity ORDER BY (`embedding <=> $q USE RANK ...`) is a
// structurally distinct code path — planned as `VectorSearch`/`HnswSearch`,
// never the generic `Sort` node the tests above exercise (`plan_select_body`
// gates the plain `Sort` branch on `!uses_vector_search`) — so plain-column
// ORDER BY validation must not regress it. Already covered by
// `tests/integration/rank_policy_tests.rs::api01_use_rank_parses_and_orders`
// (`SELECT id FROM decisions ORDER BY embedding <=> $q USE RANK
// effective_confidence LIMIT 3`, asserting the exact expected ordering) —
// not duplicated here.
// ============================================================================
