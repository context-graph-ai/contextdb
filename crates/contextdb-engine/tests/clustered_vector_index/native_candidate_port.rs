//! Native SQL companions of the kernel candidate-port guards. Observation
//! bindings are production-owned; UNBOUND is not a behavioral RED verdict.
use super::support::{empty, int_ids, observe_search, open_caller_driven, params};
use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};

fn vector(id: i64) -> Vec<f32> {
    vec![1.0, id as f32 / 16.0]
}
fn query() -> std::collections::HashMap<String, Value> {
    params([("q", Value::Vector(vec![1.0, 0.0]))])
}
/// The ranked fixture committed under `search_mode`, with no index build.
fn rank_rows(db: &Database, search_mode: &str) {
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
        &format!("CREATE TABLE docs (id INT PRIMARY KEY, allowed BOOLEAN, scope INT NOT NULL, v VECTOR(2) PARTITION_KEY (scope) SEARCH_MODE {search_mode} SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '{{vector_score}} * coalesce({{success_rate}}, 1.0)', SORT_KEY weighted)) HISTORY ALL"),
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
}
fn exact_sql(k: usize) -> String {
    format!(
        "SELECT id FROM docs WHERE scope = 0 ORDER BY v <=> $q USE VECTOR EXACT USE RANK weighted LIMIT {k}"
    )
}
fn exact_answer(db: &Database, k: usize) -> QueryResult {
    db.execute(&exact_sql(k), &query()).unwrap()
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
fn native_exact_formula_evaluates_beyond_distance_shortlists_and_preserves_ties_and_nulls() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("formula.db"));
    rank_rows(&db, "AUTO");
    assert_eq!(
        int_ids(&exact_answer(&db, 1)),
        [7],
        "farthest vector wins the declared formula"
    );
    assert_eq!(
        int_ids(&exact_answer(&db, 2)),
        [7, 0],
        "equal zero formula values retain canonical row-ID order"
    );
    db.execute("DELETE FROM outcomes WHERE decision_id = 1", &empty())
        .unwrap();
    assert_eq!(
        int_ids(&exact_answer(&db, 2)),
        [7, 1],
        "missing joined value uses existing coalesce semantics"
    );
}

#[test]
fn native_exact_formula_uses_transaction_join_overlay_without_publishing_it() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("formula-overlay.db"));
    rank_rows(&db, "AUTO");
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "UPDATE outcomes SET success_rate = 20 WHERE decision_id = 6",
        &empty(),
    )
    .unwrap();
    assert_eq!(
        int_ids(&db.execute_in_tx(tx, &exact_sql(1), &query()).unwrap()),
        [6],
        "formula sees its own staged joined-row update"
    );
    assert_eq!(
        int_ids(&exact_answer(&db, 1)),
        [7],
        "other statements retain committed formula inputs"
    );
    db.rollback(tx).unwrap();
    assert_eq!(int_ids(&exact_answer(&db, 1)), [7]);
}

/// The native work counts of the two exact-formula tests above; their
/// answers stay active there.
#[test]
#[ignore = "exact work counters for the int8 scan are unavailable"]
fn native_exact_formula_scores_every_allowed_vector_once() {
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("formula-work.db"));
    rank_rows(&db, "AUTO");
    assert_eq!(int_ids(&exact(&db, 1)), [7]);
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
    assert_eq!(int_ids(&result), [6]);
    assert_eq!(work.full_precision_comparisons(), 8);
    assert_eq!(work.quantized_evaluations + work.centroid_evaluations, 0);
    db.rollback(tx).unwrap();
}
