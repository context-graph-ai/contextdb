#![cfg(feature = "test-seams")]
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::vector_observations::{VectorJournalReplayWork, observe_journal_replay};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, VectorPartitionRef, WriterVectorSession,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn options() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    }
}
fn partition() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("points", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap(),
    )
}
fn append(db: &Database, id: i64, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $embedding)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("embedding".into(), Value::Vector(embedding)),
        ]),
    )
    .unwrap();
}
fn seed(path: &Path, history: usize) {
    let db = Database::open_with_options(path, options()).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)", &HashMap::new()).unwrap();
    for id in 0..16 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    loop {
        let report = db.run_maintenance_cycle().unwrap().vector;
        if report.remaining_partitions == 0 {
            break;
        }
        assert!(report.built_partitions > 0, "{report:?}");
    }
    for id in 16..16 + history {
        append(&db, id as i64, vec![0.0, 1.0, id as f32 + 1.0]);
    }
}
fn population(history: usize, intervening_writer: bool) -> usize {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("writer-tail.db");
    seed(&path, history);
    let session = WriterVectorSession::default();
    let route = partition();
    let db = Database::open_with_writer_vector_session(&path, options(), &session).unwrap();
    // Charge the first cold preparation before measuring subsequent batches.
    append(&db, 10_000, vec![0.0, 1.0, 1.0]);
    db.detach_writer_vector_session(&session, &route);
    drop(db);
    if intervening_writer {
        // This must acquire the genuine store while the carried graph lives.
        let other = Database::open_with_options(&path, options()).unwrap();
        append(&other, 20_000, vec![1.0, 0.0, 0.0]);
    }
    let records = Arc::new(AtomicUsize::new(0));
    let observed = records.clone();
    let _observer = observe_journal_replay(Arc::new(move |event| {
        if matches!(event.work, VectorJournalReplayWork::Record { .. }) {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    for wave in 0..4 {
        let db = Database::open_with_writer_vector_session(&path, options(), &session).unwrap();
        append(&db, 10_001 + wave, vec![0.0, 1.0, wave as f32 + 1.0]);
        if intervening_writer && wave == 0 {
            assert!(
                records.load(Ordering::SeqCst) >= history + 2,
                "the first write preparation must replay the changed durable frontier before any query"
            );
            let result = db
                .execute(
                    "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $query LIMIT 1",
                    &HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
                )
                .unwrap();
            assert_eq!(result.rows, vec![vec![Value::Int64(20_000)]]);
        }
        db.detach_writer_vector_session(&session, &route);
        drop(db);
    }
    session.clear();
    records.load(Ordering::SeqCst)
}

#[test]
fn repeated_writer_opens_reuse_tail_without_reexamining_n_or_four_n_history() {
    let work = [64, 256].map(|history| population(history, false));
    eprintln!("writer_reopen tail_history=64/256 replayed_records={work:?}");
    assert_eq!(
        work,
        [0, 0],
        "warm physical reopen must not replay stored tail history"
    );
}

#[test]
fn another_writer_between_opens_invalidates_tail_and_its_answer_is_exact() {
    let work = population(64, true);
    eprintln!("intervening_writer replayed_records={work}");
    assert!(
        work > 0,
        "an advanced journal frontier must reload instead of reusing stale tail"
    );
}

#[test]
fn optional_tail_never_refuses_an_open_that_fits_without_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("writer-tail-budget.db");
    seed(&path, 256);
    let route = partition();
    let mut fitting = 0;
    for (ordinal, limit) in [2, 4, 8, 16, 32, 64, 128].into_iter().enumerate() {
        let mut small = options();
        small.memory_limit = Some(limit * 1024 * 1024);
        let Ok(baseline) = Database::open_with_options(&path, small.clone()) else {
            continue;
        };
        drop(baseline);
        fitting += 1;
        let session = WriterVectorSession::default();
        let db = Database::open_with_writer_vector_session(&path, options(), &session).unwrap();
        append(&db, 30_000 + ordinal as i64, vec![0.0, 1.0, 1.0]);
        db.detach_writer_vector_session(&session, &route);
        drop(db);
        let reopened = Database::open_with_writer_vector_session(&path, small, &session);
        assert!(
            reopened.is_ok(),
            "optional cache cannot refuse a {limit}MiB open: {:?}",
            reopened.err()
        );
        session.clear();
    }
    assert!(
        fitting > 0,
        "the bounded budgets must include an independently fitting open"
    );
}
