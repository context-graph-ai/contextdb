#![allow(clippy::assertions_on_constants)] // Existing compatibility and test seam shape.
use super::reader_proof_support as fixture;
use contextdb_core::{ContextId, Value, VectorSearchMode};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::{Database, ReadSession};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn params(context: usize, query_row: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("context".into(), Value::Uuid(fixture::context_id(context))),
        ("q".into(), Value::Vector(fixture::vector(query_row))),
    ])
}

fn exact(reader: &ReadSession, context: usize, query_row: usize) -> Vec<Vec<Value>> {
    let answer = reader
        .execute(
            "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10",
            &params(context, query_row),
        )
        .expect("bounded exact search reads raw bodies on demand");
    assert_eq!(
        answer.trace().vector_search.as_ref().unwrap().resolved_mode,
        VectorSearchMode::Exact
    );
    answer.rows().to_vec()
}

fn exact_ground_truth(context: usize, query_row: usize) -> Vec<Vec<Value>> {
    let query = fixture::vector(query_row);
    let start = context * fixture::ROWS_PER_CONTEXT;
    let mut scored = (start..start + fixture::ROWS_PER_CONTEXT)
        .map(|row| {
            (
                row,
                contextdb_vector::cosine_similarity(&fixture::vector(row), &query),
            )
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left_row, left), (right_row, right)| {
        right.total_cmp(left).then(left_row.cmp(right_row))
    });
    scored
        .into_iter()
        .take(10)
        .map(|(row, _)| vec![Value::Uuid(fixture::row_id(row))])
        .collect()
}

fn bounded_options() -> contextdb_engine::ReadSessionOptions {
    let mut options = fixture::options();
    options.contexts = Some(BTreeSet::from([
        ContextId(fixture::context_id(0)),
        ContextId(fixture::context_id(1)),
    ]));
    options
}

#[test]
fn mixed_file_reads_point_load_exact_vectors_and_keep_their_snapshot_after_recycle() {
    assert!(fixture::ROWS * (fixture::TEXT + fixture::DIM * 4) > fixture::SESSION_BYTES);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("raw-vector-on-demand.db");
    fixture::seed_working_set(&path);

    let routes = Arc::new(fixture::Routes::default());
    let expected_open_hash_bytes = fixture::expected_open_history_hash_bytes(&path);
    let open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let reader = fixture::open_file(&path, bounded_options(), routes.clone());
    fixture::assert_open_proves_history_and_startup_digest(
        open_scope.counters(),
        expected_open_hash_bytes,
    );
    drop(open_scope);
    let snapshot = reader.file_snapshot_for_test().unwrap();

    assert_eq!(
        reader
            .execute(
                "SELECT payload FROM reader_work WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(fixture::row_id(0)))])
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Text(fixture::payload(0))]]
    );
    assert!(
        reader
            .execute(
                "SELECT id FROM reader_work WHERE context_id = $context",
                &HashMap::from([("context".into(), Value::Uuid(fixture::context_id(2)))])
            )
            .unwrap()
            .rows()
            .is_empty(),
        "the direct source must preserve declared context authorization"
    );
    assert!(
        reader
            .execute(
                "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10",
                &params(2, fixture::ROWS_PER_CONTEXT * 2),
            )
            .unwrap()
            .rows()
            .is_empty(),
        "an exact vector search must not load or return an unauthorized partition"
    );

    let exact_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let expected = exact_ground_truth(0, 17);
    assert_eq!(exact(&reader, 0, 17), expected);
    let exact_work = exact_scope.counters();
    drop(exact_scope);
    assert_eq!(exact_work.decoded_vectors, fixture::ROWS_PER_CONTEXT as u64);
    assert!(exact_work.decoded_vectors < fixture::ROWS as u64);
    let (_, _, peak, _) = reader.file_memory_for_test().unwrap();
    assert!(peak <= fixture::SESSION_BYTES);

    let mut indexed_options = bounded_options();
    indexed_options.memory_limit = Some(128 * 1024 * 1024);
    indexed_options.limits.memory = 128 * 1024 * 1024;
    let indexed_reader =
        fixture::open_file(&path, indexed_options, Arc::new(fixture::Routes::default()));
    let indexed = indexed_reader
        .execute(
            "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR AUTO LIMIT 10",
            &params(0, 17),
        )
        .expect("the saved indexed route remains complete when its graph working set fits");
    assert_eq!(
        indexed
            .trace()
            .vector_search
            .as_ref()
            .unwrap()
            .resolved_mode,
        VectorSearchMode::Exact
    );
    assert_eq!(indexed.rows(), expected);
    assert!(matches!(indexed_reader.execute(
        "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 10",
        &params(0, 17),
    ), Err(contextdb_core::Error::VectorIndexedRouteUnavailable { .. })));
    drop(indexed_reader);
    let working = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_eq!(working.file_snapshot_for_test(), Some(snapshot));
    for context in 0..fixture::CONTEXTS {
        assert_eq!(
            exact(&working, context, 17),
            exact_ground_truth(context, 17)
        );
        let rows = working.execute(
            "SELECT id, payload, embedding FROM reader_work WHERE context_id = $context ORDER BY id",
            &params(context, 17),
        ).expect("one complete Context fits the unchanged working-set declaration");
        assert_eq!(rows.rows().len(), fixture::ROWS_PER_CONTEXT);
        for (offset, row) in rows.rows().iter().enumerate() {
            let ordinal = context * fixture::ROWS_PER_CONTEXT + offset;
            assert_eq!(
                row,
                &vec![
                    Value::Uuid(fixture::row_id(ordinal)),
                    Value::Text(fixture::payload(ordinal)),
                    Value::Vector(fixture::vector(ordinal))
                ]
            );
        }
        drop(rows);
        let (usage, held, peak, _) = working.file_memory_for_test().unwrap();
        assert_eq!(usage.limit, Some(fixture::SESSION_BYTES));
        assert!(held <= usage.used && usage.used <= peak && peak <= fixture::SESSION_BYTES);
        assert_eq!(working.file_snapshot_for_test(), Some(snapshot));
    }
    let all_sql = "SELECT id FROM reader_work WHERE context_id IN ($c0,$c1,$c2,$c3,$c4,$c5,$c6,$c7) ORDER BY embedding <=> $q USE VECTOR AUTO LIMIT 10";
    let all = working
        .execute(all_sql, &fixture::all_context_params())
        .unwrap();
    let all_exact = working
        .execute(
            &all_sql.replace("AUTO", "EXACT"),
            &fixture::all_context_params(),
        )
        .unwrap();
    assert_eq!(all.rows().len(), 10);
    assert_eq!(all.rows(), all_exact.rows());
    assert_eq!(
        all.trace().vector_search.as_ref().unwrap().resolved_mode,
        VectorSearchMode::Exact
    );
    drop(all);
    drop(all_exact);
    let (_, _, peak, _) = working.file_memory_for_test().unwrap();
    assert!(peak <= fixture::SESSION_BYTES);
    drop(working);

    let backend_bytes = Arc::new(AtomicU64::new(0));
    let observed = backend_bytes.clone();
    let opening_observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_add(bytes as u64, Ordering::SeqCst);
    }));
    let reload_open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let pinned_reload = reader.open_cursor(
        "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10",
        &params(1, fixture::ROWS_PER_CONTEXT),
    ).unwrap();
    assert!(pinned_reload.first_page().has_more());
    let reload_open_work = reload_open_scope.counters();
    drop(reload_open_scope);
    drop(opening_observation);
    let pinned_first = reader.open_cursor(
        "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10",
        &params(0, 17),
    ).unwrap();
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "CREATE TABLE recycle_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .unwrap();
    writer
        .execute(
            "INSERT INTO recycle_probe VALUES (1, 'unrelated-write')",
            &HashMap::new(),
        )
        .unwrap();
    let compacted = writer.compact_now().unwrap();
    assert!(compacted.ran && compacted.handle_recycled);
    writer.close().unwrap();

    let observed = backend_bytes.clone();
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_add(bytes as u64, Ordering::SeqCst);
    }));
    let reload_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    assert_eq!(
        fixture::drain_cursor_rows(pinned_reload),
        exact_ground_truth(1, fixture::ROWS_PER_CONTEXT)
    );
    let reload_work = reload_scope.counters();
    drop(reload_scope);
    drop(observation);
    assert_eq!(
        reload_open_work.decoded_vectors + reload_work.decoded_vectors,
        fixture::ROWS_PER_CONTEXT as u64
    );
    assert!(backend_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture::drain_cursor_rows(pinned_first), expected);
    assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    routes.file_only();
    drop(reader);

    let newer = fixture::open_file(
        &path,
        bounded_options(),
        Arc::new(fixture::Routes::default()),
    );
    let newer_snapshot = newer.file_snapshot_for_test().unwrap();
    assert_ne!(newer_snapshot, snapshot);
    assert_eq!(exact(&newer, 0, 17), expected);
    assert_eq!(
        newer
            .execute(
                "SELECT payload FROM recycle_probe WHERE id = 1",
                &HashMap::new()
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Text("unrelated-write".into())]]
    );
    let pinned_newer = newer.open_cursor(
        "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10",
        &params(0, 17),
    ).unwrap();
    assert!(pinned_newer.first_page().has_more());
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "UPDATE reader_work SET embedding = $embedding WHERE id = $id",
            &HashMap::from([
                (
                    "embedding".into(),
                    Value::Vector(fixture::vector(17).into_iter().map(|x| -x).collect()),
                ),
                ("id".into(), Value::Uuid(fixture::row_id(17))),
            ]),
        )
        .unwrap();
    writer.close().unwrap();
    assert_eq!(fixture::drain_cursor_rows(pinned_newer), expected);
    assert_eq!(newer.file_snapshot_for_test(), Some(newer_snapshot));
    assert_ne!(
        exact(&newer, 0, 17).first(),
        expected.first(),
        "later statements follow the changed writer state"
    );
    drop(newer);
    let changed = fixture::open_file(
        &path,
        bounded_options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_ne!(exact(&changed, 0, 17).first(), expected.first());
    println!(
        "bounded raw-vector evidence: logical_bytes={}, session_bytes={}, peak_bytes={}, exact_decoded={}, reload_decoded={}, reload_backend_bytes={}",
        fixture::ROWS * (fixture::TEXT + fixture::DIM * 4),
        fixture::SESSION_BYTES,
        peak,
        exact_work.decoded_vectors,
        reload_open_work.decoded_vectors + reload_work.decoded_vectors,
        backend_bytes.load(Ordering::SeqCst),
    );
}
