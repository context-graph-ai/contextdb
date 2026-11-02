//! Native SQL companions of the kernel candidate-port guards. Observation
//! bindings are production-owned; UNBOUND is not a behavioral RED verdict.
use super::support::{
    drive_until_serving, empty, int_ids, observe_search, open_caller_driven, params,
};
use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};

fn vector(id: i64) -> Vec<f32> {
    vec![1.0, id as f32 / 16.0]
}
fn query() -> std::collections::HashMap<String, Value> {
    params([("q", Value::Vector(vec![1.0, 0.0]))])
}
fn rank_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE outcomes (id INT PRIMARY KEY, decision_id INT, success_rate REAL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE docs (id INT PRIMARY KEY, allowed BOOLEAN, scope INT NOT NULL, v VECTOR(2) PARTITION_KEY (scope) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '{vector_score} * coalesce({success_rate}, 1.0)', SORT_KEY weighted)) HISTORY ALL",
        &empty(),
    ).unwrap();
    for id in 0..8 {
        db.execute(
            "INSERT INTO docs VALUES ($id, $allowed, 0, $v)",
            &params([
                ("id", Value::Int64(id)),
                ("allowed", Value::Bool(id >= 6)),
                ("v", Value::Vector(vector(id))),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO outcomes VALUES ($id, $id, $boost)",
            &params([
                ("id", Value::Int64(id)),
                ("boost", Value::Float64(if id == 7 { 10.0 } else { 0.0 })),
            ]),
        )
        .unwrap();
    }
    drive_until_serving(db, 8, 1, 10);
}
fn exact_sql(k: usize) -> String {
    format!(
        "SELECT id FROM docs WHERE scope = 0 ORDER BY v <=> $q USE VECTOR EXACT USE RANK weighted LIMIT {k}"
    )
}
fn exact(db: &Database, k: usize) -> QueryResult {
    let (result, work) = observe_search(db, || db.execute(&exact_sql(k), &query()).unwrap());
    assert_eq!(
        work.quantized_evaluations, 0,
        "EXACT has no int8 shortlist prepass"
    );
    assert_eq!(work.centroid_evaluations, 0);
    assert_eq!(
        work.full_precision_comparisons(),
        8,
        "all allowed stored vectors are scored once"
    );
    result
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn native_indexed_residual_selects_eligible_nearest_rows_before_limit() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("eligible.db"));
    rank_fixture(&db);
    let (result, work) = observe_search(&db, || {
        db.execute("SELECT id FROM docs WHERE scope = 0 AND allowed = TRUE ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 2", &query()).unwrap()
    });
    assert!(
        work.clustered_route,
        "the real maintained native route must run"
    );
    assert_eq!(int_ids(&result), [6, 7]);
    assert_eq!(
        work.quantized_evaluations, 2,
        "rejected list identities receive no distance work"
    );
    assert_eq!(work.full_precision_comparisons(), 2);
}

#[test]
fn native_exact_formula_evaluates_beyond_distance_shortlists_and_preserves_ties_and_nulls() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("formula.db"));
    rank_fixture(&db);
    assert_eq!(
        int_ids(&exact(&db, 1)),
        [7],
        "farthest vector wins the declared formula"
    );
    assert_eq!(
        int_ids(&exact(&db, 2)),
        [7, 0],
        "equal zero formula values retain canonical row-ID order"
    );
    db.execute("DELETE FROM outcomes WHERE decision_id = 1", &empty())
        .unwrap();
    assert_eq!(
        int_ids(&exact(&db, 2)),
        [7, 1],
        "missing joined value uses existing coalesce semantics"
    );
}

#[test]
fn native_exact_formula_uses_transaction_join_overlay_without_publishing_it() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("formula-overlay.db"));
    rank_fixture(&db);
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "UPDATE outcomes SET success_rate = 20 WHERE decision_id = 6",
        &empty(),
    )
    .unwrap();
    let (result, work) = observe_search(&db, || {
        db.execute_in_tx(tx, &exact_sql(1), &query()).unwrap()
    });
    assert_eq!(
        int_ids(&result),
        [6],
        "formula sees its own staged joined-row update"
    );
    assert_eq!(work.full_precision_comparisons(), 8);
    assert_eq!(work.quantized_evaluations + work.centroid_evaluations, 0);
    assert_eq!(
        int_ids(&exact(&db, 1)),
        [7],
        "other statements retain committed formula inputs"
    );
    db.rollback(tx).unwrap();
    assert_eq!(int_ids(&exact(&db, 1)), [7]);
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn native_pinned_residual_uses_captured_rows_after_a_predicate_change() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("predicate-snapshot.db"));
    rank_fixture(&db);
    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot).unwrap();
    let sql = "SELECT id FROM docs WHERE scope = 0 AND allowed = TRUE ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 2";
    db.execute("UPDATE docs SET allowed = FALSE WHERE id = 6", &empty())
        .unwrap();
    db.execute("UPDATE docs SET allowed = TRUE WHERE id = 0", &empty())
        .unwrap();
    let (old, old_work) = observe_search(&db, || {
        db.execute_at_snapshot(sql, &query(), snapshot).unwrap()
    });
    assert!(old_work.clustered_route);
    assert_eq!(
        int_ids(&old),
        [6, 7],
        "old candidate identity uses its captured residual projection"
    );
    let (new, new_work) = observe_search(&db, || db.execute(sql, &query()).unwrap());
    assert!(new_work.clustered_route);
    assert_eq!(int_ids(&new), [0, 7]);
    drop(pin);
}
