//! A maintained file index serves the public Rust query API without building on a read.
use super::support::{
    BuildWriteEvent, SearchRoute, empty, int_ids, maintenance_cycles, observe_build_writes,
    observe_native_ivf_descriptor, observe_search, open_caller_driven, params, unit_vector,
};
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::{SemanticQuery, VectorSearchRoute};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn ready_file_index_serves_fresh_public_rust_indexed_search_without_building() {
    const ROWS: usize = 64;
    const DIM: usize = 8;
    let directory = tempfile::tempdir().expect("temporary file-backed store");
    let db = open_caller_driven(&directory.path().join("public-rust.db"));
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(8) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 64) IVF (LISTS = 4, PROBES = 4, REFINE_FACTOR = 1.00) WITH (training_samples = 64, training_iterations = 1))",
        &empty(),
    )
    .expect("declare the maintained clustered index");
    for id in 0..ROWS {
        db.execute(
            "INSERT INTO items VALUES ($id, $v)",
            &params([
                ("id", Value::Int64(id as i64)),
                ("v", Value::Vector(unit_vector(id as u64, DIM))),
            ]),
        )
        .expect("commit one canonical vector");
    }

    let writes = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&writes);
    let observer = Arc::new(move |event: &BuildWriteEvent| {
        if matches!(event, BuildWriteEvent::Written { .. }) {
            sink.fetch_add(1, Ordering::SeqCst);
        }
    });
    let ready = observe_build_writes(observer.clone(), || {
        for _ in 0..maintenance_cycles(ROWS, 1, 1) {
            let report = db
                .run_maintenance_cycle()
                .expect("advance real maintenance");
            assert!(
                report.vector.first_failure.is_none(),
                "maintenance cannot hide a setup refusal: {report:?}"
            );
            if report.vector.ready_partitions == 1 && report.vector.remaining_partitions == 0 {
                return report.vector;
            }
        }
        panic!("caller-driven maintenance must publish the file partition");
    });
    assert_eq!(ready.ready_indexes, 1);
    assert_eq!(ready.remaining_indexes, 0);
    let built_writes = writes.load(Ordering::SeqCst);
    assert!(
        built_writes > 0,
        "real maintenance exercised the build-write observer"
    );

    // This is the first search on this handle after maintenance reports readiness.
    let vector = unit_vector(7, DIM);
    let mut query = SemanticQuery::new("items", "embedding", vector.clone(), ROWS);
    query.search_mode = Some(VectorSearchMode::Indexed);
    let (answer, work) = observe_build_writes(observer.clone(), || {
        observe_native_ivf_descriptor(&db, || db.semantic_search(query))
    });
    assert!(work.capture_complete && !work.overflow);
    assert_eq!(
        work.index_builds, 0,
        "a fresh Rust query never starts an index build"
    );
    assert_eq!(
        writes.load(Ordering::SeqCst),
        built_writes,
        "a fresh Rust query performs no build or repair writes"
    );

    let sql = format!(
        "SELECT id FROM items ORDER BY embedding <=> '{vector:?}' USE VECTOR INDEXED LIMIT {ROWS}"
    );
    let explanation = observe_build_writes(observer.clone(), || db.explain_output(&sql))
        .expect("the public explain surface describes the ready index");
    let disclosure = explanation
        .vector_search
        .expect("vector route is explained");
    assert_eq!(disclosure.requested_mode, VectorSearchMode::Indexed);
    assert_eq!(disclosure.resolved_mode, VectorSearchMode::Indexed);
    assert_eq!(disclosure.route, Some(VectorSearchRoute::Indexed));
    assert_eq!(
        writes.load(Ordering::SeqCst),
        built_writes,
        "explain performs no build or repair writes"
    );

    // The matching SQL door is a positive control, executed only after the fresh Rust call.
    let (control, sql_work) = observe_search(&db, || db.execute(&sql, &empty()).unwrap());
    assert!(sql_work.clustered_route);
    assert_eq!(sql_work.route, Some(SearchRoute::Indexed));
    let expected = int_ids(&control);
    assert_eq!(expected.len(), ROWS);
    let mut identities = expected.clone();
    identities.sort_unstable();
    assert_eq!(identities, (0..ROWS as i64).collect::<Vec<_>>());

    let answer = answer.unwrap_or_else(|error| {
        panic!("a ready file index serves public semantic_search(Indexed): {error:?}; explain: {disclosure:?}")
    });
    assert!(
        disclosure.refusal.is_none(),
        "a ready index has no predicted refusal: {disclosure:?}"
    );
    assert_eq!(answer.len(), ROWS, "the public Rust answer is complete");
    assert_eq!(
        answer
            .iter()
            .map(|row| match row.values.get("id") {
                Some(Value::Int64(id)) => *id,
                other => panic!("the public result retains its row id: {other:?}"),
            })
            .collect::<Vec<_>>(),
        expected,
        "Rust and SQL return the same complete indexed answer"
    );
    assert_eq!(work.route, Some(SearchRoute::Indexed));
}
