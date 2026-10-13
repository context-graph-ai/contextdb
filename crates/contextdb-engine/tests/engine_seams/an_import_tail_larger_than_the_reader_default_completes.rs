#![cfg(feature = "test-seams")]
//! A real captured session (2.18 MB, 857 events) was refused while cg's import
//! made its new passages searchable: the uncovered vector suffix of one
//! partition is decoded whole, and that decode was charged to a private reader
//! carrying the shipped 16 MiB default instead of the writer budget that had
//! already admitted the suffix. A suffix whose decoded vectors exceed that
//! default must still be absorbed, exactly, inside the declared writer budget.
use contextdb_core::{Value, Wallclock, read_contract::ReadLimits};
use contextdb_engine::vector_observations::{VectorJournalReplayWork, observe_journal_replay};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const DIMENSION: usize = 1536;
const BASE_ROWS: i64 = 16;
const SUFFIX_ROWS: usize = 3000;
const WRITER_LIMIT: usize = 512 * 1024 * 1024;
const TARGET_ID: i64 = 1_000_000;

fn vector_for(id: i64) -> Vec<f32> {
    (0..DIMENSION)
        .map(|lane| (((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.5) + 0.5)
        .collect()
}

fn append(db: &Database, id: i64, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("vector".into(), Value::Vector(vector)),
        ]),
    )
    .unwrap();
}

#[test]
fn an_import_suffix_larger_than_the_shipped_reader_memory_is_absorbed_exactly() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let decoded_suffix_bytes = (SUFFIX_ROWS + 1) * DIMENSION * std::mem::size_of::<f32>();
    assert!(
        decoded_suffix_bytes as u64 > ReadLimits::SHIPPED_MEMORY,
        "the fixture's decoded suffix ({decoded_suffix_bytes} bytes) must exceed the shipped reader memory"
    );
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("large-import-suffix.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(WRITER_LIMIT),
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        &format!(
            "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)"
        ),
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..BASE_ROWS {
        append(&db, id, vector_for(id));
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in BASE_ROWS..BASE_ROWS + SUFFIX_ROWS as i64 {
        append(&db, id, vector_for(id));
    }
    let mut target = vec![0.0; DIMENSION];
    target[0] = 1.0;
    append(&db, TARGET_ID, target.clone());

    let reader_before = db.declared_read_memory_for_test().retained_bytes_for_test();
    let replayed_vectors = Arc::new(AtomicUsize::new(0));
    let observed = replayed_vectors.clone();
    let observation = observe_journal_replay(Arc::new(move |event| {
        if matches!(event.work, VectorJournalReplayWork::Vector { .. }) {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let completed = db.persist_import_vector_tails();
    drop(observation);
    assert!(
        completed.as_ref().is_ok_and(|complete| *complete),
        "the import suffix must be absorbed, not refused: {completed:?}"
    );
    assert_eq!(
        replayed_vectors.load(Ordering::SeqCst),
        SUFFIX_ROWS + 1,
        "exactly the uncovered suffix is read, once"
    );

    let high_water = db.write_memory_high_water_for_test();
    eprintln!(
        "large import suffix: rows={} decoded_bytes={decoded_suffix_bytes} shipped_reader={} writer_high_water={high_water} writer_limit={WRITER_LIMIT}",
        SUFFIX_ROWS + 1,
        ReadLimits::SHIPPED_MEMORY
    );
    assert!(
        high_water <= WRITER_LIMIT,
        "the suffix is charged inside the declared writer budget"
    );
    assert_eq!(
        db.declared_read_memory_for_test().retained_bytes_for_test(),
        reader_before,
        "the import leaves the declared reader exactly as it found it"
    );
    let receipt = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(
        receipt.temporary_workspace, 0,
        "import workspace releases at completion"
    );

    let rows = db
        .execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new(),
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(BASE_ROWS + SUFFIX_ROWS as i64 + 1)]]
    );
    let nearest = db
        .execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(target))]),
        )
        .unwrap()
        .rows;
    assert_eq!(nearest, vec![vec![Value::Int64(TARGET_ID)]]);
}
