//! One exact identity search over the 1,024-row, 128-dimension maintained
//! partition does a fixed amount of storage, decode and admission work.
//!
//! The vector maintenance journey issues more than a thousand of these
//! searches, so the work one of them does sets most of that journey's time.
//! Elapsed time is not asserted. The work is counted at the doors it passes:
//! page requests to the writer's page cache, membership records examined,
//! rows and vector bodies decoded, vectors scored, and admissions charged to
//! the declared reader and to the writer budget. Each ceiling is the count the
//! engine does today, asserted as "no more than", so a change that makes one
//! search do more of any kind of work fails here instead of slowing the
//! journey unnoticed. A change that does less lowers the ceiling with it.
use crate::resident_history_support::{exec, insert_vector, open, query_vector};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe as observe_resident;
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::memory_accounting::{
    take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_read_memory::{
    take_native_admissions_for_test, watch_native_admissions_for_test,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const ROWS: usize = 1024;
const DIM: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchWork {
    /// Page requests to the writer's page cache, hits and misses alike.
    page_requests: u64,
    /// Live and superseded membership records examined.
    membership_records: u64,
    decoded_rows: u64,
    decoded_vectors: u64,
    vector_body_accesses: u64,
    vectors_scored: u64,
    reader_admissions: u64,
    writer_budget_charges: u64,
}

/// Measured on the engine as it stands: 1,024 live memberships plus the 512
/// superseded ones the fixture's updates leave behind, each live body decoded
/// and scored once, one projected row, one reader admission, and about
/// thirteen and a half page requests per live vector.
const CEILING: SearchWork = SearchWork {
    page_requests: 14_054,
    membership_records: 1_536,
    decoded_rows: 1,
    decoded_vectors: 1_024,
    vector_body_accesses: 1_024,
    vectors_scored: 1_024,
    reader_admissions: 1,
    writer_budget_charges: 0,
};

struct CountScores(AtomicU64);
impl NativeVectorScoreObserver for CountScores {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn measure(db: &contextdb_engine::Database, id: usize) -> SearchWork {
    let reader = db
        .declared_read_memory_for_test()
        .witness_identity_for_test();
    let scored = Arc::new(CountScores(AtomicU64::new(0)));
    let cache_before = db.__writer_cache_stats_for_test().unwrap();
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    watch_native_admissions_for_test();
    watch_budget_ledger_for_test();
    let (answer, resident) = observe_resident(|| {
        with_native_vector_score_observer_for_test(scored.clone(), || {
            query_vector(db, "building", id, DIM, "EXACT")
        })
    });
    let ledger = take_budget_ledger_for_test();
    let admissions = take_native_admissions_for_test();
    let read = scope.counters();
    drop(scope);
    let cache_after = db.__writer_cache_stats_for_test().unwrap();
    assert_eq!(
        answer,
        vec![vec![Value::Int64(id as i64)]],
        "the exact search returns the identity it was asked for"
    );
    SearchWork {
        page_requests: (cache_after.0 + cache_after.1) - (cache_before.0 + cache_before.1),
        membership_records: resident.membership_keys_examined as u64,
        decoded_rows: read.decoded_rows,
        decoded_vectors: read.decoded_vectors,
        vector_body_accesses: read.vector_body_accesses,
        vectors_scored: scored.0.load(Ordering::SeqCst),
        reader_admissions: admissions
            .iter()
            .filter(|witness| witness.parent == reader && witness.admitted)
            .count() as u64,
        writer_budget_charges: ledger
            .iter()
            .filter(|entry| entry.what != "release")
            .count() as u64,
    }
}

#[test]
fn an_exact_identity_search_over_a_maintained_partition_stays_inside_its_work_ceiling() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("exact-work.db"), Some(64 * 1024 * 1024));
    exec(
        &db,
        "CREATE TABLE building (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(128) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED HNSW (M = 16, EF_CONSTRUCTION = 32)) HISTORY ALL",
    );
    for id in 0..ROWS {
        insert_vector(&db, "building", id, Some(0), DIM);
    }
    // The superseded memberships the maintenance journey leaves behind:
    // retained history the exact route passes over without scoring.
    for _ in 0..4 {
        for id in 0..128 {
            exec(
                &db,
                &format!("UPDATE building SET scope = 0 WHERE id = {id}"),
            );
        }
    }
    // A first search warms whatever a search may legitimately keep; the
    // ceiling is for a search in steady state, as the journey issues them.
    query_vector(&db, "building", ROWS - 1, DIM, "EXACT");
    let first = measure(&db, 0);
    let second = measure(&db, 0);
    eprintln!("exact identity search work: {first:?}");
    assert_eq!(
        first, second,
        "the same search over the same state does the same work"
    );
    assert_eq!(
        first.vectors_scored, ROWS as u64,
        "an exact search scores every live vector in the partition exactly once"
    );
    assert!(
        first.page_requests <= CEILING.page_requests,
        "page requests per exact search grew: {first:?}, ceiling {CEILING:?}"
    );
    assert!(
        first.membership_records <= CEILING.membership_records
            && first.decoded_rows <= CEILING.decoded_rows
            && first.decoded_vectors <= CEILING.decoded_vectors
            && first.vector_body_accesses <= CEILING.vector_body_accesses,
        "records decoded per exact search grew: {first:?}, ceiling {CEILING:?}"
    );
    assert!(
        first.reader_admissions <= CEILING.reader_admissions
            && first.writer_budget_charges <= CEILING.writer_budget_charges,
        "admissions per exact search grew: {first:?}, ceiling {CEILING:?}"
    );
}
