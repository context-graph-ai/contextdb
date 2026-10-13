#![cfg(feature = "test-seams")]
//! A real captured session (2.18 MB, 857 events) was refused while cg's import
//! made its new passages searchable: the uncovered vector suffix of one
//! partition is decoded whole, and that decode was charged to a private reader
//! carrying the shipped 16 MiB default instead of the writer budget that had
//! already admitted the suffix. A suffix whose decoded vectors exceed that
//! default must still be absorbed, exactly, with every decode charged to the
//! declared writer budget and no private reader created.
use contextdb_core::{Value, Wallclock, read_contract::ReadLimits};
use contextdb_engine::memory_accounting::{
    BudgetLedgerEntry, take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_observations::{VectorJournalReplayWork, observe_journal_replay};
use contextdb_engine::vector_read_memory::{
    reader_parents_created_for_test, take_reader_budget_ledger_for_test,
    watch_reader_budget_ledger_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const DIMENSION: usize = 1536;
const BASE_ROWS: i64 = 16;
const SUFFIX_ROWS: usize = 3000;
const EXTRA_ROWS: usize = 64;
const WRITER_LIMIT: usize = 512 * 1024 * 1024;
const TARGET_ID: i64 = 1_000_000;
const DECODE: &str = "writer_source/decode_stored_value";
// Exact writer decode charge of one import-tail call on this fixture, per
// absorbed vector and fixed. Per vector, the stored f32 body alone is charged
// 48 + 16 * (DIMENSION - 1) = 24,608 bytes while it decodes (the decoder's
// geometric-growth admission) plus DIMENSION * 4 = 6,144 for its owned copy;
// the remaining 4,107 bytes are the vector's journal record, index name,
// decode control and its share of the native append's page decodes.
const DECODE_BYTES_PER_VECTOR: usize = 34_859;
const DECODE_BYTES_FIXED: usize = 51_993;
const DECODE_ADMISSIONS_PER_VECTOR: usize = 1_554;
const DECODE_ADMISSIONS_FIXED: usize = 362;

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

fn summarize(ledger: &[BudgetLedgerEntry]) -> BTreeMap<String, (usize, usize)> {
    let mut by_name = BTreeMap::new();
    for entry in ledger {
        let slot = by_name.entry(entry.what.clone()).or_insert((0, 0));
        slot.0 += 1;
        slot.1 += entry.bytes;
    }
    by_name
}

/// What one import-tail call charged.
struct Absorbed {
    vectors: usize,
    decode_bytes: usize,
    decode_admissions: usize,
    decode_peak_used: usize,
    reader_parents_created: u64,
    reader_ledger: BTreeMap<String, (usize, usize)>,
}

fn absorb(suffix_rows: usize) -> Absorbed {
    let decoded_suffix_bytes = (suffix_rows + 1) * DIMENSION * std::mem::size_of::<f32>();
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
    for id in BASE_ROWS..BASE_ROWS + suffix_rows as i64 {
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
    let parents_before = reader_parents_created_for_test();
    watch_budget_ledger_for_test();
    watch_reader_budget_ledger_for_test();
    let completed = db.persist_import_vector_tails();
    let reader_ledger = summarize(&take_reader_budget_ledger_for_test());
    let writer_ledger = take_budget_ledger_for_test();
    let reader_parents_created = reader_parents_created_for_test() - parents_before;
    drop(observation);
    assert!(
        completed.as_ref().is_ok_and(|complete| *complete),
        "the import suffix must be absorbed, not refused: {completed:?}"
    );
    assert_eq!(
        replayed_vectors.load(Ordering::SeqCst),
        suffix_rows + 1,
        "exactly the uncovered suffix is read, once"
    );
    let decode: Vec<_> = writer_ledger
        .iter()
        .filter(|entry| entry.what == DECODE)
        .collect();
    let decode_bytes = decode.iter().map(|entry| entry.bytes).sum();
    let decode_peak_used = decode
        .iter()
        .map(|entry| entry.used_after)
        .max()
        .unwrap_or(0);
    let high_water = db.write_memory_high_water_for_test();
    eprintln!(
        "large import suffix: rows={} decoded_bytes={decoded_suffix_bytes} shipped_reader={} writer_decode_bytes={decode_bytes} writer_decode_admissions={} writer_peak_used_at_decode={decode_peak_used} writer_high_water={high_water} writer_limit={WRITER_LIMIT} reader_parents_created={reader_parents_created} reader_ledger={reader_ledger:?} writer_ledger={:?}",
        suffix_rows + 1,
        ReadLimits::SHIPPED_MEMORY,
        decode.len(),
        summarize(&writer_ledger),
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
        vec![vec![Value::Int64(BASE_ROWS + suffix_rows as i64 + 1)]]
    );
    let nearest = db
        .execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(target))]),
        )
        .unwrap()
        .rows;
    assert_eq!(nearest, vec![vec![Value::Int64(TARGET_ID)]]);
    Absorbed {
        vectors: suffix_rows + 1,
        decode_bytes,
        decode_admissions: decode.len(),
        decode_peak_used,
        reader_parents_created,
        reader_ledger,
    }
}

#[test]
fn an_import_suffix_larger_than_the_shipped_reader_memory_is_absorbed_exactly() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let small = absorb(SUFFIX_ROWS);
    let large = absorb(SUFFIX_ROWS + EXTRA_ROWS);
    for absorbed in [&small, &large] {
        assert_eq!(
            absorbed.reader_parents_created, 0,
            "import-tail work creates no private reader"
        );
        assert!(
            !absorbed
                .reader_ledger
                .contains_key("reader/materialize_answer"),
            "no import-tail decode is charged to a reader: {:?}",
            absorbed.reader_ledger
        );
        assert!(
            absorbed.decode_peak_used <= WRITER_LIMIT,
            "every decode admission fits the declared writer budget"
        );
        assert_eq!(
            absorbed.decode_bytes,
            DECODE_BYTES_PER_VECTOR * absorbed.vectors + DECODE_BYTES_FIXED,
            "exact writer decode charge for {} vectors",
            absorbed.vectors
        );
        assert_eq!(
            absorbed.decode_admissions,
            DECODE_ADMISSIONS_PER_VECTOR * absorbed.vectors + DECODE_ADMISSIONS_FIXED,
            "exact writer decode admissions for {} vectors",
            absorbed.vectors
        );
    }
    let extra = large.decode_bytes - small.decode_bytes;
    eprintln!(
        "large import suffix: writer decode bytes {} -> {} for {EXTRA_ROWS} more vectors: {extra} bytes, {} per vector",
        small.decode_bytes,
        large.decode_bytes,
        extra as f64 / EXTRA_ROWS as f64
    );
}
