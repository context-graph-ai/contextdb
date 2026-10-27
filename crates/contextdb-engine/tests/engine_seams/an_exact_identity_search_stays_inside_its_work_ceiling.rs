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
use crate::primitive_work_support::{SearchWork, Work, observe_work};
use crate::resident_history_support::{exec, insert_vector, open, query_vector};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::read_work_probe::{BillingWork, observe_billing_work_for_test};

const ROWS: usize = 1024;
const DIM: usize = 128;

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

const BILLING_CEILING: BillingWork = BillingWork {
    native_child_identities: 9_775,
    metadata_reservations: 1_202,
    copy_reservations: 10,
};

// Byte fields exclude the declared cache capacity and its native index;
// they retain the measured fixed controls and operation workspace allowances.
const EXTENDED_CEILING: Work = Work {
    search: CEILING,
    backend_bytes: Some(323_584),
    backend_reads: Some(79),
    reader_peak: Some(1_345),
    stored_vector_decodes: 1_024,
    fresh_dirty_pages: 0,
    fresh_dirty_page_bytes: 0,
    native_pages: None,
    native_page_bytes: 0,
    other_native_pages: 0,
    other_native_page_bytes: 0,
    writer_after: 8_495,
    writer_before: 8_495,
    writer_peak: 8_495 + 584,
};

fn measure(db: &contextdb_engine::Database, id: usize) -> (Work, BillingWork) {
    // Observe only the operation, excluding observer setup and witness inspection.
    let ((answer, billing), work) = observe_work(db, || {
        observe_billing_work_for_test(|| query_vector(db, "building", id, DIM, "EXACT"))
    });
    assert_eq!(answer, vec![vec![Value::Int64(id as i64)]]);
    eprintln!(
        "PRIMITIVE {}",
        serde_json::json!({"fixture":"exact-maintained-1024x128", "work":work, "billing":{"native_child_identities":billing.native_child_identities,"metadata_reservations":billing.metadata_reservations,"copy_reservations":billing.copy_reservations}})
    );
    (work, billing)
}

#[test]
#[expect(
    clippy::absurd_extreme_comparisons,
    reason = "The work-ceiling assertion compares every measured dimension directly, including zero ceilings."
)]
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
    let (first, first_billing) = measure(&db, 0);
    let (second, second_billing) = measure(&db, 0);
    assert_eq!(
        first_billing, second_billing,
        "repeated searches do the same billing work"
    );
    assert!(
        first_billing.native_child_identities <= BILLING_CEILING.native_child_identities
            && first_billing.metadata_reservations <= BILLING_CEILING.metadata_reservations
            && first_billing.copy_reservations <= BILLING_CEILING.copy_reservations,
        "billing calls per exact search grew: {first_billing:?}, ceiling {BILLING_CEILING:?}"
    );
    first.assert_within(
        "maintained exact search",
        &EXTENDED_CEILING.with_declared_writer_cache(&db),
    );
    second.assert_within(
        "maintained exact search",
        &EXTENDED_CEILING.with_declared_writer_cache(&db),
    );
    let first = first.search;
    let second = second.search;
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
