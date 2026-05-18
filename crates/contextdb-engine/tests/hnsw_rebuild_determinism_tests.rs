use contextdb_core::{RowId, Value, VectorIndexRef};
use contextdb_engine::Database;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use tempfile::TempDir;
use uuid::Uuid;

const REOPEN_ROWS: usize = 1024;
const TOP_K: usize = 50;
const SINGLE_PAIR_ATTEMPTS: usize = 32;
const IN_PROCESS_REBUILD_ATTEMPTS: usize = 32;
const PARALLEL_PRESSURE_ATTEMPTS: usize = 8;
const REPEAT_REOPEN_ITERATIONS: usize = 25;
const SENTINEL_ID: u128 = 999_999_999;

#[derive(Debug)]
struct HnswObservation {
    raw_top_k: Vec<(RowId, f32)>,
    topology_digest: u64,
    build_serial: u64,
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn index_ref(table: &str) -> VectorIndexRef {
    VectorIndexRef::new(table, "embedding")
}

fn ranked3(rank: usize) -> Vec<f32> {
    let score = (1.0 - rank as f32 * 0.0005).clamp(0.05, 1.0);
    vec![score, (1.0 - score * score).max(0.0).sqrt(), 0.0]
}

fn axis3(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis.min(2)] = 1.0;
    vector
}

fn create_single_table(db: &Database) {
    db.execute(
        "CREATE TABLE det_one (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
}

fn create_reopen_tables(db: &Database) {
    db.execute(
        "CREATE TABLE table_text (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE table_face (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
}

fn row_ids_for_table(db: &Database, table: &str) -> HashSet<RowId> {
    db.scan(table, db.snapshot())
        .unwrap()
        .into_iter()
        .map(|row| row.row_id)
        .collect()
}

fn row_id_for_uuid(db: &Database, table: &str, id: Uuid) -> RowId {
    db.scan(table, db.snapshot())
        .unwrap()
        .into_iter()
        .find_map(|row| match row.values.get("id") {
            Some(Value::Uuid(value)) if *value == id => Some(row.row_id),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{table}: row with UUID {id} not found"))
}

fn seed_single_table(db: &Database) -> HashSet<RowId> {
    for i in 0..REOPEN_ROWS {
        db.execute(
            "INSERT INTO det_one (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(100_000 + i as u128))),
                ("embedding", Value::Vector(ranked3(i))),
            ]),
        )
        .unwrap();
    }
    row_ids_for_table(db, "det_one")
}

fn seed_reopen_tables(db: &Database) -> (HashSet<RowId>, HashSet<RowId>) {
    for i in 0..REOPEN_ROWS {
        db.execute(
            "INSERT INTO table_text (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(100_000 + i as u128))),
                ("embedding", Value::Vector(ranked3(i))),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO table_face (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(200_000 + i as u128))),
                ("embedding", Value::Vector(ranked3(i))),
            ]),
        )
        .unwrap();
    }
    (
        row_ids_for_table(db, "table_text"),
        row_ids_for_table(db, "table_face"),
    )
}

fn search(
    db: &Database,
    table: &str,
    expected_hnsw_len: usize,
    context: &str,
) -> Vec<(RowId, f32)> {
    let hits = db
        .query_vector(index_ref(table), &axis3(0), TOP_K, None, db.snapshot())
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_public_hnsw_trace(db, table, expected_hnsw_len, &hits, context);
    hits
}

fn search_with_query(
    db: &Database,
    table: &str,
    query: &[f32],
    expected_hnsw_len: usize,
    context: &str,
) -> Vec<(RowId, f32)> {
    let hits = db
        .query_vector(index_ref(table), query, TOP_K, None, db.snapshot())
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_public_hnsw_trace(db, table, expected_hnsw_len, &hits, context);
    hits
}

fn assert_public_hnsw_trace(
    db: &Database,
    table: &str,
    expected_hnsw_len: usize,
    hits: &[(RowId, f32)],
    context: &str,
) {
    let trace = db
        .__debug_last_query_vector_trace_for_test()
        .unwrap_or_else(|| panic!("{context}: missing public query_vector debug trace"));
    assert!(
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "{context}: public query_vector must use the HNSW strategy for the all-visible no-candidate top-k path"
    );
    assert!(
        trace.used_hnsw,
        "{context}: public query_vector reported fallback instead of HNSW; trace={trace:?}"
    );
    assert_eq!(
        trace.index,
        index_ref(table),
        "{context}: trace was recorded for the wrong vector index; trace={trace:?}"
    );
    assert_eq!(
        trace.hnsw_len,
        Some(expected_hnsw_len),
        "{context}: public HNSW search saw the wrong graph size; trace={trace:?}"
    );
    assert_eq!(
        trace.fallback_reason, None,
        "{context}: public HNSW search recorded a fallback reason; trace={trace:?}"
    );
    assert!(
        trace.hnsw_candidate_count >= TOP_K,
        "{context}: public HNSW search returned fewer raw candidates than top-k; trace={trace:?}"
    );
    assert_eq!(
        trace.hnsw_candidate_count,
        trace.hnsw_candidate_row_ids.len(),
        "{context}: raw HNSW candidate count and candidate rows diverged; trace={trace:?}"
    );
    let final_row_ids = hits.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>();
    assert_eq!(
        trace.final_row_ids, final_row_ids,
        "{context}: public trace rows do not match query_vector output; trace={trace:?}, hits={hits:?}"
    );
    let candidate_rows = trace
        .hnsw_candidate_row_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    assert_eq!(
        candidate_rows.len(),
        trace.hnsw_candidate_row_ids.len(),
        "{context}: raw HNSW candidates contain duplicate rows; trace={trace:?}"
    );
    if trace.supplemented_row_count > 0 {
        assert!(
            trace.hnsw_candidate_count.saturating_add(64) >= expected_hnsw_len,
            "{context}: public HNSW search supplemented results without a near-complete candidate set; trace={trace:?}"
        );
        assert!(
            trace.supplemented_row_count
                <= expected_hnsw_len.saturating_sub(trace.hnsw_candidate_count),
            "{context}: public HNSW trace supplemented more rows than the graph candidate gap; trace={trace:?}"
        );
    }
    let mut supplemented_final_rows = 0usize;
    for row_id in final_row_ids {
        if !candidate_rows.contains(&row_id) {
            supplemented_final_rows += 1;
        }
    }
    if supplemented_final_rows > 0 {
        assert!(
            trace.supplemented_row_count >= supplemented_final_rows,
            "{context}: public HNSW trace did not account for final rows supplied by the supplement path; trace={trace:?}"
        );
        assert!(
            trace.hnsw_candidate_count.saturating_add(64) >= expected_hnsw_len,
            "{context}: public rows came from a supplement even though HNSW did not return a near-complete candidate set; trace={trace:?}"
        );
    }
}

fn assert_results_belong_to_index(
    hits: &[(RowId, f32)],
    expected_rows: &HashSet<RowId>,
    context: &str,
) {
    let mut seen = HashSet::with_capacity(hits.len());
    for (row_id, score) in hits {
        assert!(
            expected_rows.contains(row_id),
            "{context}: row {row_id} is not owned by this vector index; hits={hits:?}"
        );
        assert!(
            seen.insert(*row_id),
            "{context}: duplicate row {row_id} in search results; hits={hits:?}"
        );
        assert!(
            !score.is_nan(),
            "{context}: search score is NaN for {row_id}"
        );
    }
}

fn assert_hnsw_built_and_used(
    db: &Database,
    table: &str,
    expected_rows: &HashSet<RowId>,
    expected_hnsw_len: usize,
    context: &str,
) -> HnswObservation {
    assert_hnsw_built_and_used_for_query(
        db,
        table,
        expected_rows,
        expected_hnsw_len,
        &axis3(0),
        context,
    )
}

fn assert_hnsw_built_and_used_for_query(
    db: &Database,
    table: &str,
    expected_rows: &HashSet<RowId>,
    expected_hnsw_len: usize,
    query: &[f32],
    context: &str,
) -> HnswObservation {
    let index = index_ref(table);
    let stats = db
        .__debug_vector_hnsw_stats(index.clone())
        .unwrap_or_else(|| panic!("{context}: expected a materialized HNSW graph for {table}"));
    assert_eq!(
        stats.point_count, expected_hnsw_len,
        "{context}: HNSW graph must contain every live vector for {table}; stats={stats:?}"
    );
    assert_eq!(
        stats.layer0_points, expected_hnsw_len,
        "{context}: HNSW base layer must contain every live vector for {table}; stats={stats:?}"
    );
    assert_eq!(
        stats.dimension, 3,
        "{context}: HNSW graph dimension must match the vector column; stats={stats:?}"
    );
    assert!(
        stats.layer0_neighbor_edges > 0,
        "{context}: HNSW graph must expose real neighbor edges, not only a synthetic point count; stats={stats:?}"
    );

    let raw_hnsw_hits = db
        .__debug_vector_hnsw_raw_search_for_test(index, query, TOP_K)
        .unwrap_or_else(|| panic!("{context}: expected raw HNSW search for {table}"));
    assert!(
        !raw_hnsw_hits.is_empty(),
        "{context}: raw HNSW search returned no rows for {table}"
    );
    assert!(
        raw_hnsw_hits.len() >= TOP_K,
        "{context}: raw HNSW search returned fewer than top-k rows for {table}; raw_hnsw_hits={raw_hnsw_hits:?}"
    );
    let raw_top_k = raw_hnsw_hits.into_iter().take(TOP_K).collect::<Vec<_>>();
    assert_results_belong_to_index(&raw_top_k, expected_rows, context);

    let topology_digest = db
        .__debug_vector_hnsw_topology_digest_for_test(index_ref(table))
        .unwrap_or_else(|| panic!("{context}: expected an HNSW topology digest for {table}"));
    assert_ne!(
        topology_digest, 0,
        "{context}: HNSW topology digest must not be the empty digest"
    );
    let build_serial = db
        .__debug_vector_hnsw_build_serial_for_test(index_ref(table))
        .unwrap_or_else(|| panic!("{context}: expected an HNSW build serial for {table}"));
    assert_ne!(
        build_serial, 0,
        "{context}: HNSW build serial must not be zero"
    );

    let explain = db
        .explain(&format!(
            "SELECT id FROM {table} ORDER BY embedding <=> $q LIMIT {TOP_K}"
        ))
        .unwrap();
    assert!(
        explain.contains("HNSWSearch") && explain.contains(table),
        "{context}: public plan must choose HNSWSearch for {table}; explain={explain}"
    );

    HnswObservation {
        raw_top_k,
        topology_digest,
        build_serial,
    }
}

fn assert_hnsw_cleared(db: &Database, table: &str, context: &str) {
    assert_eq!(
        db.__debug_vector_hnsw_len(index_ref(table)),
        None,
        "{context}: sentinel mutation must clear the materialized HNSW graph before the next query"
    );
}

fn assert_search_sequence_identical(
    actual: &[(RowId, f32)],
    expected: &[(RowId, f32)],
    context: &str,
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{context}: top-k lengths differ"
    );
    assert!(
        actual.iter().all(|(_, score)| !score.is_nan()),
        "{context}: actual search scores contain NaN: {actual:?}"
    );
    assert!(
        expected.iter().all(|(_, score)| !score.is_nan()),
        "{context}: expected search scores contain NaN: {expected:?}"
    );

    let actual_bits = actual
        .iter()
        .map(|(row_id, score)| (*row_id, score.to_bits()))
        .collect::<Vec<_>>();
    let expected_bits = expected
        .iter()
        .map(|(row_id, score)| (*row_id, score.to_bits()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual_bits, expected_bits,
        "{context}: search sequence differs\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

fn assert_hnsw_observation_identical(
    actual: &HnswObservation,
    expected: &HnswObservation,
    context: &str,
) {
    assert_eq!(
        actual.topology_digest, expected.topology_digest,
        "{context}: HNSW topology digest differs"
    );
    assert_search_sequence_identical(&actual.raw_top_k, &expected.raw_top_k, context);
}

fn assert_newer_hnsw_build(actual: &HnswObservation, previous: &HnswObservation, context: &str) {
    assert!(
        actual.build_serial > previous.build_serial,
        "{context}: expected a fresh HNSW build; previous serial={}, actual serial={}",
        previous.build_serial,
        actual.build_serial
    );
}

fn open_seeded_single_table(path: &std::path::Path) -> (Database, HashSet<RowId>) {
    let db = Database::open(path).unwrap();
    create_single_table(&db);
    let row_ids = seed_single_table(&db);
    (db, row_ids)
}

fn insert_sentinel(db: &Database) -> RowId {
    let sentinel_id = Uuid::from_u128(SENTINEL_ID);
    db.execute(
        "INSERT INTO det_one (id, embedding) VALUES ($id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(sentinel_id)),
            ("embedding", Value::Vector(axis3(1))),
        ]),
    )
    .unwrap();
    row_id_for_uuid(db, "det_one", sentinel_id)
}

fn delete_sentinel(db: &Database) {
    let sentinel_id = Uuid::from_u128(SENTINEL_ID);
    db.execute(
        "DELETE FROM det_one WHERE id = $id",
        &params(vec![("id", Value::Uuid(sentinel_id))]),
    )
    .unwrap();
}

fn with_parallel_pressure(run: impl FnOnce() + std::panic::UnwindSafe) {
    let done = Arc::new(AtomicBool::new(false));
    let worker_count = num_cpus::get().max(2);
    let workers = (0..worker_count)
        .map(|seed| {
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut work = seed as u64 + 1;
                while !done.load(Ordering::Relaxed) {
                    work = work.wrapping_mul(6364136223846793005).wrapping_add(1);
                    std::hint::black_box(work);
                }
            })
        })
        .collect::<Vec<_>>();

    let result = std::panic::catch_unwind(run);
    done.store(true, Ordering::Relaxed);
    for worker in workers {
        worker.join().unwrap();
    }
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn consecutive_hnsw_builds_in_one_process_return_bitwise_identical_search_sequence() {
    for attempt in 0..SINGLE_PAIR_ATTEMPTS {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(format!("hnsw-rebuild-d01-{attempt}.db"));
        let (db, expected_rows) = open_seeded_single_table(&path);
        let first = search(&db, "det_one", REOPEN_ROWS, "first close-reopen build");
        assert_eq!(first.len(), TOP_K);
        assert_results_belong_to_index(&first, &expected_rows, "first close-reopen build");
        let first_raw = assert_hnsw_built_and_used(
            &db,
            "det_one",
            &expected_rows,
            REOPEN_ROWS,
            "first close-reopen build",
        );
        db.close().unwrap();
        drop(db);

        let reopened = Database::open(&path).unwrap();
        let second = search(
            &reopened,
            "det_one",
            REOPEN_ROWS,
            "second close-reopen build",
        );
        assert_results_belong_to_index(&second, &expected_rows, "second close-reopen build");
        let second_raw = assert_hnsw_built_and_used(
            &reopened,
            "det_one",
            &expected_rows,
            REOPEN_ROWS,
            "second close-reopen build",
        );
        assert_newer_hnsw_build(
            &second_raw,
            &first_raw,
            &format!("close-reopen rebuild attempt {attempt}"),
        );
        assert_hnsw_observation_identical(
            &second_raw,
            &first_raw,
            &format!("raw close-reopen rebuild attempt {attempt}"),
        );
        assert_search_sequence_identical(
            &second,
            &first,
            &format!("close-reopen rebuild attempt {attempt}"),
        );
    }
}

#[test]
fn consecutive_hnsw_builds_within_one_open_database_are_stable() {
    with_parallel_pressure(|| {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("hnsw-rebuild-d02.db");
        let (db, expected_rows) = open_seeded_single_table(&path);
        let first = search(&db, "det_one", REOPEN_ROWS, "initial in-process build");
        assert_eq!(first.len(), TOP_K);
        assert_results_belong_to_index(&first, &expected_rows, "initial in-process build");
        let first_raw = assert_hnsw_built_and_used(
            &db,
            "det_one",
            &expected_rows,
            REOPEN_ROWS,
            "initial in-process build",
        );
        assert!(
            first[0].1 >= 0.999,
            "top row cosine too low for seeded data: {first:?}"
        );

        for attempt in 0..IN_PROCESS_REBUILD_ATTEMPTS {
            let sentinel_row_id = insert_sentinel(&db);
            assert_hnsw_cleared(
                &db,
                "det_one",
                &format!("live-sentinel rebuild attempt {attempt}"),
            );
            let mut live_rows = expected_rows.clone();
            assert!(
                live_rows.insert(sentinel_row_id),
                "live-sentinel rebuild attempt {attempt}: sentinel row id must be new"
            );
            let live = search_with_query(
                &db,
                "det_one",
                &axis3(1),
                REOPEN_ROWS + 1,
                &format!("live-sentinel rebuild attempt {attempt}"),
            );
            assert_eq!(
                live.first().map(|(row_id, _)| *row_id),
                Some(sentinel_row_id),
                "live-sentinel rebuild attempt {attempt}: sentinel vector must be the public top hit; live={live:?}"
            );
            assert_results_belong_to_index(
                &live,
                &live_rows,
                &format!("live-sentinel rebuild attempt {attempt}"),
            );
            let live_raw = assert_hnsw_built_and_used_for_query(
                &db,
                "det_one",
                &live_rows,
                REOPEN_ROWS + 1,
                &axis3(1),
                &format!("live-sentinel rebuild attempt {attempt}"),
            );
            assert_eq!(
                live_raw.raw_top_k.first().map(|(row_id, _)| *row_id),
                Some(sentinel_row_id),
                "live-sentinel rebuild attempt {attempt}: sentinel vector must be the raw HNSW top hit; raw={:?}",
                live_raw.raw_top_k
            );
            assert_ne!(
                live_raw.topology_digest, first_raw.topology_digest,
                "live-sentinel rebuild attempt {attempt}: live sentinel build must create a distinct graph"
            );
            assert_newer_hnsw_build(
                &live_raw,
                &first_raw,
                &format!("live-sentinel rebuild attempt {attempt}"),
            );
            delete_sentinel(&db);
            assert_hnsw_cleared(
                &db,
                "det_one",
                &format!("post-delete in-process rebuild attempt {attempt}"),
            );
            let second = search(
                &db,
                "det_one",
                REOPEN_ROWS,
                &format!("in-process rebuild attempt {attempt}"),
            );
            assert_results_belong_to_index(
                &second,
                &expected_rows,
                &format!("in-process rebuild attempt {attempt}"),
            );
            let second_raw = assert_hnsw_built_and_used(
                &db,
                "det_one",
                &expected_rows,
                REOPEN_ROWS,
                &format!("in-process rebuild attempt {attempt}"),
            );
            assert_newer_hnsw_build(
                &second_raw,
                &live_raw,
                &format!("in-process rebuild attempt {attempt}"),
            );
            assert_hnsw_observation_identical(
                &second_raw,
                &first_raw,
                &format!("raw in-process rebuild attempt {attempt}"),
            );
            assert_search_sequence_identical(
                &second,
                &first,
                &format!("in-process rebuild attempt {attempt}"),
            );
        }
    });
}

#[test]
fn per_index_independent_progress_reopen_flake_is_fixed_under_repeat() {
    for iter in 0..REPEAT_REOPEN_ITERATIONS {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(format!("hnsw-rebuild-d03-{iter}.db"));
        let (text_rows, face_rows, pre_text, pre_text_raw, pre_face, pre_face_raw) = {
            let db = Database::open(&path).unwrap();
            create_reopen_tables(&db);
            let (text_rows, face_rows) = seed_reopen_tables(&db);
            assert!(
                text_rows.is_disjoint(&face_rows),
                "iteration {iter}: table_text and table_face row ids must be disjoint"
            );
            let pre_text = search(&db, "table_text", REOPEN_ROWS, "pre-reopen table_text");
            assert_results_belong_to_index(&pre_text, &text_rows, "pre-reopen table_text");
            let pre_text_raw = assert_hnsw_built_and_used(
                &db,
                "table_text",
                &text_rows,
                REOPEN_ROWS,
                "pre-reopen table_text",
            );
            let pre_face = search(&db, "table_face", REOPEN_ROWS, "pre-reopen table_face");
            assert_results_belong_to_index(&pre_face, &face_rows, "pre-reopen table_face");
            let pre_face_raw = assert_hnsw_built_and_used(
                &db,
                "table_face",
                &face_rows,
                REOPEN_ROWS,
                "pre-reopen table_face",
            );
            db.close().unwrap();
            (
                text_rows,
                face_rows,
                pre_text,
                pre_text_raw,
                pre_face,
                pre_face_raw,
            )
        };

        let reopened = Database::open(&path).unwrap();
        let post_face = search(
            &reopened,
            "table_face",
            REOPEN_ROWS,
            "post-reopen table_face",
        );
        assert_results_belong_to_index(&post_face, &face_rows, "post-reopen table_face");
        let post_face_raw = assert_hnsw_built_and_used(
            &reopened,
            "table_face",
            &face_rows,
            REOPEN_ROWS,
            "post-reopen table_face",
        );
        let post_text = search(
            &reopened,
            "table_text",
            REOPEN_ROWS,
            "post-reopen table_text",
        );
        assert_results_belong_to_index(&post_text, &text_rows, "post-reopen table_text");
        let post_text_raw = assert_hnsw_built_and_used(
            &reopened,
            "table_text",
            &text_rows,
            REOPEN_ROWS,
            "post-reopen table_text",
        );
        assert_newer_hnsw_build(
            &post_text_raw,
            &pre_text_raw,
            &format!("iteration {iter}, table_text"),
        );
        assert_newer_hnsw_build(
            &post_face_raw,
            &pre_face_raw,
            &format!("iteration {iter}, table_face"),
        );
        assert_hnsw_observation_identical(
            &post_text_raw,
            &pre_text_raw,
            &format!("iteration {iter}, raw table_text"),
        );
        assert_search_sequence_identical(
            &post_text,
            &pre_text,
            &format!("iteration {iter}, table_text"),
        );
        assert_hnsw_observation_identical(
            &post_face_raw,
            &pre_face_raw,
            &format!("iteration {iter}, raw table_face"),
        );
        assert_search_sequence_identical(
            &post_face,
            &pre_face,
            &format!("iteration {iter}, table_face"),
        );
    }
}

#[test]
fn hnsw_build_invariant_under_parallel_pressure_does_not_drift() {
    with_parallel_pressure(|| {
        for attempt in 0..PARALLEL_PRESSURE_ATTEMPTS {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join(format!("hnsw-rebuild-d04-{attempt}.db"));
            let (db, expected_rows) = open_seeded_single_table(&path);
            let first = search(&db, "det_one", REOPEN_ROWS, "parallel-pressure first build");
            assert_eq!(first.len(), TOP_K);
            assert_results_belong_to_index(&first, &expected_rows, "parallel-pressure first build");
            let first_raw = assert_hnsw_built_and_used(
                &db,
                "det_one",
                &expected_rows,
                REOPEN_ROWS,
                "parallel-pressure first build",
            );
            db.close().unwrap();
            drop(db);

            let reopened = Database::open(&path).unwrap();
            let second = search(
                &reopened,
                "det_one",
                REOPEN_ROWS,
                "parallel-pressure second build",
            );
            assert_results_belong_to_index(
                &second,
                &expected_rows,
                "parallel-pressure second build",
            );
            let second_raw = assert_hnsw_built_and_used(
                &reopened,
                "det_one",
                &expected_rows,
                REOPEN_ROWS,
                "parallel-pressure second build",
            );
            assert_newer_hnsw_build(
                &second_raw,
                &first_raw,
                &format!("parallel-pressure rebuild attempt {attempt}"),
            );
            assert_hnsw_observation_identical(
                &second_raw,
                &first_raw,
                &format!("raw parallel-pressure rebuild attempt {attempt}"),
            );
            assert_search_sequence_identical(
                &second,
                &first,
                &format!("parallel-pressure rebuild attempt {attempt}"),
            );
        }
    });
}
