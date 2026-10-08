#![allow(clippy::unusual_byte_groupings)] // Existing compatibility and test seam shape.
use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::executor::{
    FinitePlannerValueOperations, execute_eager_for_test, finite_planner_operation_guard,
    finite_planner_shared_equality_for_test,
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const SMALL_LIST_LEN: usize = 10_000;
const LARGE_LIST_LEN: usize = 40_000;
const PLANNER_OPERATION_CEILING: u64 = 6_000_000;
const MAX_FOURFOLD_OPERATION_GROWTH: u64 = 6;

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 1024 * 1024,
        work: 100_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 64,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn seed_ordering_fixture(index_direction: &str, staged_middle: bool) -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE finite_order (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the finite-run ordering table");
    db.execute(
        &format!(
            "CREATE INDEX finite_order_a_b ON finite_order (a {index_direction}, b {index_direction})"
        ),
        &empty(),
    )
    .expect("create the declared composite ordering index");

    let committed = if staged_middle {
        &[1_i64, 3_i64][..]
    } else {
        &[1_i64, 2_i64, 3_i64][..]
    };
    for b in committed {
        db.execute(
            "INSERT INTO finite_order VALUES ($id, 7, $b)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(*b)),
                ("b".to_owned(), Value::Int64(*b)),
            ]),
        )
        .expect("insert one committed ordering row");
    }
    if staged_middle {
        db.execute("BEGIN", &empty())
            .expect("open the ordering transaction");
        db.execute("INSERT INTO finite_order VALUES (2, 7, 2)", &empty())
            .expect("stage the row between the committed runs");
    }
    db
}

fn assert_ordered_result(
    result: &contextdb_engine::QueryResult,
    expected: &[i64],
    route: &str,
    index_direction: &str,
    transaction: &str,
    order_by: &str,
    expected_sort_elided: bool,
) {
    let actual = result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(value)] => *value,
            other => panic!("expected one INTEGER ordering value, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "{route} {transaction} read over the {index_direction} index must honor {order_by}"
    );
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some("finite_order_a_b"),
        "the composite index must supply the {route} {transaction} read: {:?}",
        result.trace
    );
    assert_eq!(
        result.trace.predicates_pushed.len(),
        2,
        "both finite prefix columns must remain pushed: {:?}",
        result.trace
    );
    assert_eq!(
        result.trace.sort_elided, expected_sort_elided,
        "{route} must report whether its physical index walk supplies {order_by}: {:?}",
        result.trace,
    );
}

fn seed_null_intersection_fixture() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE finite_nulls (id INTEGER PRIMARY KEY, lead INTEGER, suffix INTEGER, marker INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the nullable finite-constraint table");
    db.execute(
        "CREATE INDEX finite_nulls_lead_suffix ON finite_nulls (lead, suffix)",
        &empty(),
    )
    .expect("create the nullable composite index");
    for statement in [
        "INSERT INTO finite_nulls VALUES (1, NULL, 7, 0)",
        "INSERT INTO finite_nulls VALUES (2, 1, NULL, 0)",
        "INSERT INTO finite_nulls VALUES (3, NULL, NULL, 0)",
        "INSERT INTO finite_nulls VALUES (4, 1, 7, 0)",
    ] {
        db.execute(statement, &empty())
            .expect("insert one nullable finite-constraint row");
    }
    db
}

fn contradictory_null_predicates() -> [&'static str; 8] {
    [
        "lead IS NULL AND lead = 1",
        "lead = 1 AND lead IS NULL",
        "lead IS NULL AND lead IN (1, 2)",
        "lead IN (1, 2) AND lead IS NULL",
        "lead = 1 AND suffix IS NULL AND suffix = 7",
        "lead = 1 AND suffix = 7 AND suffix IS NULL",
        "lead = 1 AND suffix IS NULL AND suffix IN (7, 8)",
        "lead = 1 AND suffix IN (7, 8) AND suffix IS NULL",
    ]
}

fn sorted_ids(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    let mut ids = result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("expected one INTEGER id, got {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn assert_read_paths_match(
    db: &Database,
    predicate: &str,
    expected_ids: &[i64],
    expected_pushed: usize,
) {
    let sql = format!("SELECT id FROM finite_nulls WHERE {predicate}");
    let eager = execute_eager_for_test(
        db,
        &sql,
        &empty(),
        db.declared_reader_memory()
            .expect("admit the eager fixture reader"),
    )
    .expect("the eager executor must preserve NULL predicate semantics");
    let ordinary = db
        .execute(&sql, &empty())
        .expect("ordinary execution must preserve NULL predicate semantics");
    let bounded = bounded::execute(
        db,
        &bounded::BoundedReadRequest::new(sql, empty(), limits(), Arc::new(FrozenClock)),
    )
    .expect("bounded execution must preserve NULL predicate semantics");

    for (route, result) in [
        ("eager", &eager),
        ("ordinary", &ordinary),
        ("bounded", &bounded.result),
    ] {
        assert_eq!(
            sorted_ids(result),
            expected_ids,
            "{route} execution returned the wrong rows for {predicate}"
        );
        assert_eq!(
            result.trace.index_used.as_deref(),
            Some("finite_nulls_lead_suffix"),
            "{route} execution must exercise the nullable composite index for {predicate}: {:?}",
            result.trace
        );
        assert_eq!(
            result.trace.predicates_pushed.len(),
            expected_pushed,
            "{route} execution pushed the wrong finite prefix for {predicate}: {:?}",
            result.trace
        );
    }
}

#[test]
fn is_null_beside_equality_crashes_the_query() {
    let db = seed_null_intersection_fixture();
    for predicate in contradictory_null_predicates() {
        assert_read_paths_match(
            &db,
            predicate,
            &[],
            usize::from(predicate.contains("suffix")) + 1,
        );

        let update = db
            .execute(
                &format!("UPDATE finite_nulls SET marker = 99 WHERE {predicate}"),
                &empty(),
            )
            .expect("a contradictory nullable indexed UPDATE must complete");
        assert_eq!(
            update.rows_affected, 0,
            "a contradictory nullable indexed UPDATE changed rows for {predicate}"
        );

        let delete = db
            .execute(
                &format!("DELETE FROM finite_nulls WHERE {predicate}"),
                &empty(),
            )
            .expect("a contradictory nullable indexed DELETE must complete");
        assert_eq!(
            delete.rows_affected, 0,
            "a contradictory nullable indexed DELETE changed rows for {predicate}"
        );
    }

    let unchanged = db
        .execute("SELECT marker FROM finite_nulls", &empty())
        .expect("read rows after contradictory writes");
    assert_eq!(unchanged.rows.len(), 4);
    assert!(
        unchanged.rows.iter().all(|row| row == &[Value::Int64(0)]),
        "contradictory UPDATE/DELETE predicates must leave every row unchanged: {:?}",
        unchanged.rows
    );
}

#[test]
fn repeated_is_null_constraints_keep_matching_null_rows() {
    let db = seed_null_intersection_fixture();
    assert_read_paths_match(&db, "lead IS NULL AND lead IS NULL", &[1, 3], 1);
    assert_read_paths_match(
        &db,
        "lead = 1 AND suffix IS NULL AND suffix IS NULL",
        &[2],
        2,
    );
}

#[test]
fn unsorted_suffix_in_runs_preserve_requested_order_across_read_paths_and_transactions() {
    for index_direction in ["ASC", "DESC"] {
        for staged_middle in [false, true] {
            let db = seed_ordering_fixture(index_direction, staged_middle);
            let transaction = if staged_middle {
                "in-transaction"
            } else {
                "committed"
            };
            for (order_by, order_direction, expected) in [
                ("ORDER BY b ASC", "ASC", &[1_i64, 2, 3][..]),
                ("ORDER BY a ASC, b ASC", "ASC", &[1_i64, 2, 3][..]),
                ("ORDER BY b DESC", "DESC", &[3_i64, 2, 1][..]),
                ("ORDER BY a DESC, b DESC", "DESC", &[3_i64, 2, 1][..]),
            ] {
                let sql =
                    format!("SELECT b FROM finite_order WHERE a = 7 AND b IN (3, 1, 2) {order_by}");
                let eager = execute_eager_for_test(
                    &db,
                    &sql,
                    &empty(),
                    db.declared_reader_memory()
                        .expect("admit the eager fixture reader"),
                )
                .expect("the eager executor must answer the finite runs");
                assert_ordered_result(
                    &eager,
                    expected,
                    "eager",
                    index_direction,
                    transaction,
                    order_by,
                    order_direction == index_direction,
                );

                let ordinary = db
                    .execute(&sql, &empty())
                    .expect("ordinary execution must answer the finite runs");
                assert_ordered_result(
                    &ordinary,
                    expected,
                    "ordinary",
                    index_direction,
                    transaction,
                    order_by,
                    true,
                );

                let bounded = bounded::execute(
                    &db,
                    &bounded::BoundedReadRequest::new(
                        sql,
                        empty(),
                        limits(),
                        Arc::new(FrozenClock),
                    ),
                )
                .expect("bounded execution must answer the same finite runs");
                assert_ordered_result(
                    &bounded.result,
                    expected,
                    "bounded",
                    index_direction,
                    transaction,
                    order_by,
                    true,
                );
                assert_eq!(bounded.result.rows, ordinary.rows);
            }
            if staged_middle {
                db.execute("ROLLBACK", &empty())
                    .expect("discard the staged ordering row");
            }
        }
    }
}

fn planning_cost_fixture() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE finite_cost (id INTEGER PRIMARY KEY, lead INTEGER NOT NULL, scope INTEGER NOT NULL, suffix UUID NOT NULL)",
        &empty(),
    )
    .expect("create the finite-list cost table");
    for ddl in [
        "CREATE INDEX finite_cost_lead ON finite_cost (lead)",
        "CREATE INDEX finite_cost_lead_scope ON finite_cost (lead, scope)",
        "CREATE INDEX finite_cost_scope_suffix ON finite_cost (scope, suffix)",
        "CREATE INDEX finite_cost_scope_suffix_lead ON finite_cost (scope, suffix, lead)",
    ] {
        db.execute(ddl, &empty())
            .expect("create a competing finite-list index");
    }

    let suffix_base = 0xabcde000_0000_0000_0000_0000_0000_0000_u128;
    for (id, lead, scope, suffix) in [
        (1_i64, 4_321_i64, 7_i64, suffix_base + 4_321),
        (2_i64, 80_000_i64, 7_i64, suffix_base + 80_000),
        (3_i64, 80_001_i64, 8_i64, suffix_base + 4_321),
    ] {
        db.execute(
            "INSERT INTO finite_cost VALUES ($id, $lead, $scope, $suffix)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("lead".to_owned(), Value::Int64(lead)),
                ("scope".to_owned(), Value::Int64(scope)),
                ("suffix".to_owned(), Value::Uuid(Uuid::from_u128(suffix))),
            ]),
        )
        .expect("insert a finite-list cost row");
    }
    db
}

fn permuted_ordinals(list_len: usize) -> impl Iterator<Item = usize> {
    (0..list_len).map(move |ordinal| (ordinal * 7_919) % list_len)
}

fn leading_list_statement(list_len: usize) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::with_capacity(list_len);
    let mut placeholders = Vec::with_capacity(list_len);
    for (position, value) in permuted_ordinals(list_len).enumerate() {
        let name = format!("lead_{position}");
        params.insert(name.clone(), Value::Int64(value as i64));
        placeholders.push(format!("${name}"));
    }
    (
        format!(
            "SELECT id FROM finite_cost WHERE lead IN ({})",
            placeholders.join(", ")
        ),
        params,
    )
}

fn suffix_list_statement(list_len: usize) -> (String, HashMap<String, Value>) {
    let suffix_base = 0xabcde000_0000_0000_0000_0000_0000_0000_u128;
    let mut params = HashMap::with_capacity(list_len + 2);
    params.insert("scope".to_owned(), Value::Int64(7));
    let mut placeholders = Vec::with_capacity(list_len + 1);
    for (position, value) in permuted_ordinals(list_len).enumerate() {
        let name = format!("suffix_{position}");
        let spelling = Uuid::from_u128(suffix_base + value as u128).to_string();
        params.insert(name.clone(), Value::Text(spelling));
        placeholders.push(format!("${name}"));
    }
    let equivalent = Uuid::from_u128(suffix_base + 4_321)
        .to_string()
        .to_uppercase();
    params.insert("suffix_equivalent".to_owned(), Value::Text(equivalent));
    placeholders.push("$suffix_equivalent".to_owned());
    (
        format!(
            "SELECT id FROM finite_cost WHERE scope = $scope AND suffix IN ({})",
            placeholders.join(", ")
        ),
        params,
    )
}

fn assert_cost_result(result: &contextdb_engine::QueryResult, expected_index: &str, pushed: usize) {
    assert_eq!(result.rows, vec![vec![Value::Int64(1)]]);
    assert_eq!(result.trace.index_used.as_deref(), Some(expected_index));
    assert_eq!(result.trace.predicates_pushed.len(), pushed);
    assert_eq!(
        result.trace.rows_examined, 1,
        "only the one stored key in the 10,000-value finite scope is examined"
    );
}

fn execute_with_comparison_bound(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,
    expected_index: &str,
    pushed: usize,
) -> (FinitePlannerValueOperations, FinitePlannerValueOperations) {
    let ordinary_guard = finite_planner_operation_guard(PLANNER_OPERATION_CEILING);
    let ordinary = db
        .execute(sql, params)
        .expect("ordinary execution must plan and answer the large finite scope");
    assert_cost_result(&ordinary, expected_index, pushed);
    let ordinary_operations = ordinary_guard.operations();
    assert!(ordinary_operations.comparisons() > 0);
    assert!(ordinary_operations.equality_checks() > 0);
    drop(ordinary_guard);

    let bounded_guard = finite_planner_operation_guard(PLANNER_OPERATION_CEILING);
    let bounded = bounded::execute(
        db,
        &bounded::BoundedReadRequest::new(sql, params.clone(), limits(), Arc::new(FrozenClock)),
    )
    .expect("bounded execution must plan and answer the large finite scope");
    assert_cost_result(&bounded.result, expected_index, pushed);
    assert_eq!(bounded.result.rows, ordinary.rows);
    let bounded_operations = bounded_guard.operations();
    assert!(bounded_operations.comparisons() > 0);
    assert!(bounded_operations.equality_checks() > 0);
    drop(bounded_guard);

    (ordinary_operations, bounded_operations)
}

#[test]
fn ten_to_forty_thousand_value_lists_have_subquadratic_shared_planning_work() {
    let db = planning_cost_fixture();
    let mut observations = Vec::new();
    for list_len in [SMALL_LIST_LEN, LARGE_LIST_LEN] {
        let (leading_sql, leading_params) = leading_list_statement(list_len);
        let leading = execute_with_comparison_bound(
            &db,
            &leading_sql,
            &leading_params,
            "finite_cost_lead",
            1,
        );

        let (suffix_sql, suffix_params) = suffix_list_statement(list_len);
        let suffix = execute_with_comparison_bound(
            &db,
            &suffix_sql,
            &suffix_params,
            "finite_cost_scope_suffix",
            2,
        );
        observations.push((list_len, leading, suffix));
    }

    let (_, small_leading, small_suffix) = observations[0];
    let (_, large_leading, large_suffix) = observations[1];
    for (shape, small, large) in [
        ("leading ordinary", small_leading.0, large_leading.0),
        ("leading bounded", small_leading.1, large_leading.1),
        ("suffix ordinary", small_suffix.0, large_suffix.0),
        ("suffix bounded", small_suffix.1, large_suffix.1),
    ] {
        assert!(
            large.total() > small.total(),
            "the 40,000-value {shape} observation must include more planner work"
        );
        assert!(
            large.total() <= small.total() * MAX_FOURFOLD_OPERATION_GROWTH,
            "four times as many values must stay below {MAX_FOURFOLD_OPERATION_GROWTH}x shared planner work for {shape}: 10k={small:?}, 40k={large:?}"
        );
    }

    for (list_len, leading, suffix) in observations {
        println!(
            "finite_list_planning values={list_len} leading={{ordinary:{:?},bounded:{:?}}} suffix={{ordinary:{:?},bounded:{:?}}}",
            leading.0, leading.1, suffix.0, suffix.1
        );
    }
}

#[test]
#[should_panic(
    expected = "finite-list planning exceeded its deterministic value-operation ceiling"
)]
fn quadratic_equality_work_outside_the_finite_comparator_fails_the_shared_bound() {
    let values = (0..256).map(Value::Int64).collect::<Vec<_>>();
    let _guard = finite_planner_operation_guard(32_000);
    for left in &values {
        for right in &values {
            let _ = finite_planner_shared_equality_for_test(left, right);
        }
    }
}
