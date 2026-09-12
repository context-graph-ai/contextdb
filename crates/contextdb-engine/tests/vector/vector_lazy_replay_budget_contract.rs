//! Restart-time maintained-vector residency must be admitted before it is
//! allocated.
//!
//! A database can retain a saved graph while that graph is absent from this
//! process. The first bounded `INDEXED` read is therefore not ordinary query
//! scratch work: it may have to decode that graph and replay the committed
//! journal suffix into a fresh tail. These checks use a real file close and
//! reopen, then the bounded production executor. They require both the
//! request's memory ceiling and the database's standing budget to reject that
//! residency before a partial graph becomes visible.
//!
//! The current test seams expose generation residency, fresh-tail entries,
//! bounded typed refusals, and the shared accountant. They intentionally do
//! not expose a separate decode/replay reservation or a "rebuilt from raw
//! vectors" counter. Consequently this file proves no partial resident state,
//! no leaked shared charge, the saved generation identity, and the replayed
//! tail result; it does not claim an unobservable internal allocation order.

use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::store::{VectorGraphGenerationStatus, VectorPartitionRef};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const BASE_ROWS: usize = 32;
const MAX_MAINTENANCE_CYCLES: usize = 64;
const SCOPE: u128 = 0xB0D6_E700;
const FIRST_ID: u128 = 0xB0D6_E701;
const TAIL_ID: u128 = 0xB0D6_E801;

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 128,
        result_bytes: 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 64,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn scope() -> Uuid {
    Uuid::from_u128(SCOPE)
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("lazy_replay_items", "embedding")
}

fn partition() -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope())])
        .expect("the fixture's UUID partition key is canonical")
}

fn partition_ref() -> VectorPartitionRef {
    VectorPartitionRef::new(index(), partition())
}

fn graph_ready(db: &Database) -> bool {
    db.vector_store_for_test()
        .partition_info(&index(), &partition())
        .is_some_and(|info| info.graph_available)
}

fn status(db: &Database) -> VectorGraphGenerationStatus {
    db.vector_store_for_test()
        .partition_graph_generation_status(&partition_ref())
        .expect("the fixture keeps one declared partition state")
}

fn request(limits: ReadLimits) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        "SELECT id FROM lazy_replay_items WHERE scope_id = $scope \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
        params([
            ("scope", Value::Uuid(scope())),
            ("query", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
        limits,
        Arc::new(FrozenClock),
    )
}

fn assert_memory_refusal(error: bounded::TestError, which_budget: &str) {
    let bounded::TestError::Refused(refusal) = error else {
        panic!(
            "a {which_budget} refusal while loading a saved graph must use the bounded read \
             refusal vocabulary, got {error:?}"
        );
    };
    assert_eq!(
        refusal.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "the caller must be able to branch on a crossed memory ceiling ({which_budget})"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = refusal.detail() else {
        panic!("the crossed {which_budget} ceiling must carry typed detail");
    };
    assert_eq!(
        detail.limit,
        ReadFailureLimit::Memory,
        "the typed refusal must name memory rather than hide decode/replay behind route loss"
    );
}

fn assert_dormant_and_uncharged(
    db: &Database,
    before: usize,
    expected_base: contextdb_vector::store::VectorGraphGeneration,
) {
    let after = status(db);
    assert_eq!(
        after.dormant_base,
        Some(expected_base),
        "a refused first read keeps the same saved base available for a later admitted read"
    );
    assert!(
        !after.base_resident,
        "a refused load must not publish a partial base graph"
    );
    assert!(
        !after.change_resident,
        "a refused load must not publish a partial change graph"
    );
    assert_eq!(
        db.accountant().usage().used,
        before,
        "a refused load must return every decode and replay charge before the caller can retry"
    );
}

fn seed_closed_fixture(path: &std::path::Path) {
    let db = Database::open(path).expect("open file-backed vector fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE lazy_replay_items (\
            id UUID PRIMARY KEY, \
            scope_id UUID NOT NULL, \
            embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED\
        )",
        &empty(),
    )
    .expect("create the maintained partitioned vector table");

    let tx = db.begin_or_panic();
    for ordinal in 0..BASE_ROWS {
        db.insert_row(
            tx,
            "lazy_replay_items",
            params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(FIRST_ID + ordinal as u128)),
                ),
                ("scope_id", Value::Uuid(scope())),
                (
                    "embedding",
                    Value::Vector(vec![1.0, ordinal as f32 * 0.001, 0.0]),
                ),
            ]),
        )
        .expect("stage a deterministic base vector");
    }
    db.commit(tx).expect("commit the base vectors");

    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if graph_ready(&db) {
            break;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance batch returns");
    }
    assert!(
        graph_ready(&db),
        "caller-driven maintenance must make and save the route before restart"
    );

    db.execute(
        "INSERT INTO lazy_replay_items (id, scope_id, embedding) \
         VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(TAIL_ID))),
            ("scope", Value::Uuid(scope())),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
    )
    .expect("commit one journal entry after the saved base");
    db.close()
        .expect("close the saved-base plus journal-tail fixture");
}

fn reopen_dormant(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("reopen the saved vector fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let reopened = status(&db);
    assert!(
        reopened.dormant_base.is_some(),
        "restart registers the saved base without loading it"
    );
    assert!(
        !reopened.base_resident,
        "the first bounded query, not open, is where lazy residency is admitted"
    );
    db
}

fn bounded_tail_result(db: &Database, limits: ReadLimits) {
    let result = bounded::execute(db, &request(limits))
        .expect("an adequately admitted bounded indexed read loads the saved route");
    assert_eq!(
        result.result.rows,
        vec![vec![Value::Uuid(Uuid::from_u128(TAIL_ID))]],
        "the first post-restart route must replay the committed journal suffix into its fresh tail"
    );
}

/// A bounded caller's own ceiling must be checked before restart-time decode
/// and replay allocate or publish a graph. A later request with enough room
/// must use that same saved generation and replayed tail.
#[test]
fn first_bounded_restart_query_charges_lazy_decode_and_replay_to_the_request_ceiling() {
    let directory = TempDir::new().expect("temporary vector store directory");
    let path = directory.path().join("request-ceiling.db");
    seed_closed_fixture(&path);

    // Measure the exact resident increase through the real bounded route,
    // then reopen again so the refusal begins from a genuinely dormant graph.
    let calibration = reopen_dormant(&path);
    let calibration_before = calibration.accountant().usage().used;
    bounded_tail_result(&calibration, limits());
    let resident_increase = calibration
        .accountant()
        .usage()
        .used
        .checked_sub(calibration_before)
        .expect("a successful lazy load does not reduce the pre-query resident charge");
    assert!(
        resident_increase > 0,
        "the successful first read must make saved-graph residency observable to the accountant"
    );
    calibration.close().expect("close the calibration reopen");

    let db = reopen_dormant(&path);
    let before = db.accountant().usage().used;
    let expected_base = status(&db)
        .dormant_base
        .expect("the fixture saved one base generation");
    let tight = ReadLimits {
        memory: resident_increase.saturating_sub(1) as u64,
        ..limits()
    };
    assert_memory_refusal(
        bounded::execute(&db, &request(tight)).expect_err(
            "one byte below lazy decode/replay residency must refuse before allocation",
        ),
        "request memory ceiling",
    );
    assert_dormant_and_uncharged(&db, before, expected_base);

    bounded_tail_result(&db, limits());
    let loaded = status(&db);
    assert_eq!(
        loaded.base,
        Some(expected_base),
        "the retry uses the saved generation rather than replacing its durable identity"
    );
    assert!(
        loaded.base_resident,
        "the admitted retry publishes one complete base graph"
    );
    assert!(
        loaded.fresh_tail_entries >= 1,
        "the admitted retry replays the post-base journal entry into its fresh mutable tail"
    );
}

/// The store-wide budget must reject the same lazy work before it changes
/// resident state, even if the individual request declared ample room.
#[test]
fn first_bounded_restart_query_charges_lazy_decode_and_replay_to_the_store_budget() {
    let directory = TempDir::new().expect("temporary vector store directory");
    let path = directory.path().join("store-budget.db");
    seed_closed_fixture(&path);

    let calibration = reopen_dormant(&path);
    let calibration_before = calibration.accountant().usage().used;
    bounded_tail_result(&calibration, limits());
    let resident_increase = calibration
        .accountant()
        .usage()
        .used
        .checked_sub(calibration_before)
        .expect("a successful lazy load does not reduce the pre-query resident charge");
    assert!(resident_increase > 0);
    calibration.close().expect("close the calibration reopen");

    let db = reopen_dormant(&path);
    let before = db.accountant().usage().used;
    let expected_base = status(&db)
        .dormant_base
        .expect("the fixture saved one base generation");
    db.set_memory_limit(Some(before + resident_increase.saturating_sub(1)))
        .expect("tighten only the standing store budget after open");
    let request = request(limits());
    let error = db
        .read_session(limits())
        .unwrap()
        .execute(&request.sql, &request.params)
        .expect_err("one byte below lazy decode/replay residency must refuse before allocation");
    let budget = before + resident_increase.saturating_sub(1);
    assert!(
        matches!(error, contextdb_core::Error::MemoryBudgetExceeded {
        budget_limit_bytes, requested_bytes, available_bytes, ..
    } if budget_limit_bytes == budget && requested_bytes > available_bytes),
        "the STORE refusal keeps the actual exhausted budget: {error:?}"
    );
    assert_dormant_and_uncharged(&db, before, expected_base);

    db.set_memory_limit(None)
        .expect("restore the standing budget for the retry");
    bounded_tail_result(&db, limits());
    let loaded = status(&db);
    assert_eq!(loaded.base, Some(expected_base));
    assert!(loaded.base_resident);
    assert!(loaded.fresh_tail_entries >= 1);
}
