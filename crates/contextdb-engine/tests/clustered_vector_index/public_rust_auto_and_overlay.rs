//! Public automatic searches and transaction deltas share the ready native index.
use super::support::{
    BuildWriteEvent, CalibrationBasis, ObjectiveStatus, SearchRoute, calibration_receipt, empty,
    int_ids, maintenance_cycles, observe_build_writes, observe_native_ivf_descriptor,
    observe_search, open_caller_driven, params, unit_vector,
};
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::{Database, SearchResult, SemanticQuery, VectorSearchRoute};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const ROWS: usize = 128;
const DIM: usize = 8;

fn stored_vector(id: u64) -> Vec<f32> {
    if (8..=26).contains(&id) {
        let mut vector = unit_vector(7, DIM);
        vector[0] += (id - 7) as f32 * 0.001;
        vector
    } else {
        unit_vector(id, DIM)
    }
}

fn ready_file(path: &std::path::Path, require_calibration: bool) -> Database {
    let db = open_caller_driven(path);
    db.execute(
        "CREATE TABLE questions (id INT PRIMARY KEY, embedding VECTOR(8) SEARCH_MODE EXACT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO questions VALUES (1, $v)",
        &params([("v", Value::Vector(unit_vector(7, DIM)))]),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(8) SEARCH_MODE AUTO AUTO_INDEX_AT 1 SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 128) IVF (LISTS = 4) WITH (training_samples = 128, training_iterations = 1, calibration_samples = 1, calibration_depths = [1], calibration_queries = questions.embedding))",
        &empty(),
    ).expect("declare real calibration and native search policy");
    for id in 0..ROWS {
        db.execute(
            "INSERT INTO items VALUES ($id, $v)",
            &params([
                ("id", Value::Int64(id as i64)),
                ("v", Value::Vector(stored_vector(id as u64))),
            ]),
        )
        .unwrap();
    }
    let mut saw_ready = false;
    let mut last_receipt = None;
    for _ in 0..maintenance_cycles(ROWS + 1, 1, 1) {
        let report = db
            .run_maintenance_cycle()
            .expect("advance real caller-driven maintenance");
        assert!(
            report.vector.first_failure.is_none(),
            "maintenance setup refused: {report:?}"
        );
        if report.vector.ready_partitions == 1 && report.vector.remaining_partitions == 0 {
            assert_eq!(report.vector.ready_indexes, 1);
            assert_eq!(report.vector.remaining_indexes, 0);
            saw_ready = true;
            if !require_calibration {
                return db;
            }
            last_receipt = calibration_receipt(&db, "items", "embedding", &[]);
            if let Some(receipt) = &last_receipt {
                assert!(
                    matches!(receipt.basis, CalibrationBasis::Queries { .. }),
                    "maintenance uses the declared real query source: {receipt:?}"
                );
                if [1].into_iter().all(|depth| {
                    receipt
                        .depths
                        .iter()
                        .any(|row| row.depth == depth && row.qualified && row.sample_queries == 1)
                }) {
                    return db;
                }
            }
        }
    }
    panic!(
        "native maintenance must publish a ready qualified Auto index; ready={saw_ready}, real calibration receipt={last_receipt:?}"
    );
}

fn ids(rows: &[SearchResult]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values.get("id") {
            Some(Value::Int64(id)) => *id,
            other => panic!("the result must retain the visible primary key: {other:?}"),
        })
        .collect()
}

fn query(mode: VectorSearchMode) -> SemanticQuery {
    let mut query = SemanticQuery::new("items", "embedding", unit_vector(7, DIM), 1);
    query.search_mode = Some(mode);
    query
}

fn sql(mode: &str) -> String {
    format!(
        "SELECT id FROM items ORDER BY embedding <=> '{:?}' USE VECTOR {mode} LIMIT 1",
        unit_vector(7, DIM)
    )
}

// Physical build writes remain observable even if an attempted build later refuses.
fn without_build_writes<T>(operation: impl FnOnce() -> T) -> T {
    let writes = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&writes);
    let result = observe_build_writes(
        Arc::new(move |event| {
            if matches!(event, BuildWriteEvent::Written { .. }) {
                sink.fetch_add(1, Ordering::SeqCst);
            }
        }),
        operation,
    );
    assert_eq!(
        writes.load(Ordering::SeqCst),
        0,
        "search and explain never write build or repair data"
    );
    result
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn ready_file_auto_semantic_search_executes_the_native_route_reported_by_explain() {
    let root = tempfile::tempdir().unwrap();
    let db = ready_file(&root.path().join("automatic.db"), true);
    // Capture only this fresh semantic call; a later SQL control cannot supply its route.
    let (answer, executed) = without_build_writes(|| {
        observe_native_ivf_descriptor(&db, || db.semantic_search(query(VectorSearchMode::Auto)))
    });
    let explanation = without_build_writes(|| db.explain_output(&sql("AUTO")))
        .expect("the equivalent SQL has a public explanation")
        .vector_search
        .expect("the explanation includes its vector route");
    let (control, control_work) =
        observe_search(&db, || db.execute(&sql("AUTO"), &empty()).unwrap());
    assert_eq!(int_ids(&control), vec![7]);
    assert!(
        control_work.clustered_route,
        "the real SQL Auto control is qualified for the native index: {control_work:?}"
    );
    assert_eq!(control_work.route, Some(SearchRoute::Indexed));
    assert_eq!(control_work.objective_status, Some(ObjectiveStatus::Met));
    let answer = answer.expect("public Auto returns a complete answer");
    assert_eq!(ids(&answer), vec![7]);
    assert!(executed.capture_complete && !executed.overflow);
    assert_eq!(executed.index_builds, 0);
    assert_eq!(
        executed.route,
        Some(SearchRoute::Indexed),
        "public Auto must execute the ready native index; explain={explanation:?}"
    );
    assert_eq!(explanation.requested_mode, VectorSearchMode::Auto);
    assert_eq!(explanation.resolved_mode, VectorSearchMode::Indexed);
    assert_eq!(explanation.route, Some(VectorSearchRoute::Indexed));
    assert!(
        explanation.refusal.is_none(),
        "explain agrees with the successful native route: {explanation:?}"
    );
}

fn overlay() {
    let mode = VectorSearchMode::Indexed;
    let root = tempfile::tempdir().unwrap();
    let db = ready_file(&root.path().join("overlay.db"), false);
    // Prove the committed population has a real native Indexed route before staging.
    let (control, work) = observe_search(&db, || db.execute(&sql("INDEXED"), &empty()).unwrap());
    assert_eq!(int_ids(&control), vec![7]);
    assert!(work.clustered_route);
    assert_eq!(work.route, Some(SearchRoute::Indexed));
    let tx = db.begin().unwrap();
    db.execute_in_tx(tx, "DELETE FROM items WHERE id = 7", &empty())
        .unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO items VALUES (10000, $v)",
        &params([("v", Value::Vector(unit_vector(7, DIM)))]),
    )
    .unwrap();
    // This existing seam enters the production transaction-aware semantic kernel.
    // It does not synthesize staged rows or choose the committed search route.
    let mut overlay_query = query(mode);
    overlay_query.limit = 4;
    let (semantic, semantic_work) = without_build_writes(|| {
        observe_native_ivf_descriptor(&db, || {
            db.__semantic_search_in_tx_for_test(tx, overlay_query)
        })
    });
    let (sql_result, sql_work) = without_build_writes(|| {
        observe_search(&db, || {
            db.execute_in_tx(tx, &sql("INDEXED").replace("LIMIT 1", "LIMIT 4"), &empty())
        })
    });
    let exact = db
        .execute_in_tx(tx, &sql("EXACT").replace("LIMIT 1", "LIMIT 4"), &empty())
        .unwrap();
    let expected = int_ids(&exact);
    assert_eq!(expected.len(), 4);
    assert_eq!(expected[0], 10000);
    assert!(!expected.contains(&7));
    assert!(
        expected[1..].iter().all(|id| (0..ROWS as i64).contains(id)),
        "unchanged committed neighbors remain in the answer"
    );
    db.rollback(tx)
        .expect("discard the staged overlay through the real transaction path");
    let sql_result = sql_result.expect("public SQL sees the staged overlay");
    assert_eq!(int_ids(&sql_result), expected);
    assert!(sql_work.clustered_route);
    assert_eq!(sql_work.route, Some(SearchRoute::Indexed));
    assert_eq!(
        int_ids(&db.execute(&sql("EXACT"), &empty()).unwrap()),
        vec![7],
        "rollback keeps staged rows private and restores the committed nearest"
    );
    let semantic = semantic.unwrap_or_else(|error| {
        panic!("{mode:?} semantic overlay keeps staged rows and native committed search: {error:?}")
    });
    assert_eq!(
        ids(&semantic),
        expected,
        "the staged nearest and unchanged committed neighbors form one answer"
    );
    assert!(semantic_work.capture_complete && !semantic_work.overflow);
    assert_eq!(semantic_work.index_builds, 0);
    assert_eq!(
        semantic_work.route,
        Some(SearchRoute::Indexed),
        "the committed remainder uses the native index"
    );
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn ready_file_indexed_transaction_overlay_preserves_staged_rows_and_native_committed_search() {
    overlay();
}
