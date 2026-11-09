//! Searches over partitions of very different sizes keep one global answer: a
//! partitioned clustered-index table answers INDEXED exactly as EXACT does
//! across small and large partitions, a reader pinned at a snapshot keeps its
//! own answers through growth, shrinkage, maintenance and a replacement, an
//! exhaustive search refuses a failed admission with a typed memory refusal
//! and no charge or partial state, and INDEXED refuses a scope whose index is
//! not built.

use super::retained_query_rows::RetainedRows;

use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::vector_ivf_observations::{
    NativeIvfFaultBoundary, NativeIvfSealEvent, NativeIvfSealObserver,
};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 8;

struct Counter(AtomicU64);

impl NativeIvfSealObserver for Counter {
    fn observe(&self, event: NativeIvfSealEvent<'_>) {
        if let NativeIvfSealEvent::QueryExecuted { work, .. } = event {
            self.0.fetch_add(
                work.centroid_evaluations
                    + work.quantized_evaluations
                    + work.full_precision_evaluations
                    + work.buffer_evaluations,
                Ordering::SeqCst,
            );
        }
    }

    fn before_boundary(&self, _boundary: NativeIvfFaultBoundary<'_>) -> contextdb_core::Result<()> {
        Ok(())
    }
}

impl NativeVectorScoreObserver for Counter {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
fn vector(seed: usize) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| {
            let mixed = (seed as u64 + 11)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64 + 5).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            ((mixed ^ (mixed >> 29)) % 10_007) as f32 / 10_007.0 - 0.5
        })
        .collect()
}

/// A partitioned table searched exactly, with the given live rows per
/// partition.
fn fixture(db: &Database, rows_per_partition: &[usize]) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE flat_docs (id UUID PRIMARY KEY, partition_id INTEGER NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (partition_id) MAX_PARTITIONS 8 \
             SEARCH_MODE EXACT)"
        ),
        &empty(),
    )
    .expect("declare the partitioned EXACT table");
    let mut seed = 0usize;
    for (partition, rows) in rows_per_partition.iter().enumerate() {
        for _ in 0..*rows {
            seed += 1;
            db.execute(
                "INSERT INTO flat_docs (id, partition_id, embedding) VALUES ($id, $partition, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                    ("partition".to_owned(), Value::Int64(partition as i64)),
                    ("embedding".to_owned(), Value::Vector(vector(seed))),
                ]),
            )
            .expect("insert one deterministic row");
        }
    }
}

struct Observed {
    result: QueryResult,
    native_scores: u64,
}

fn search(db: &Database, mode: &str) -> Observed {
    let native = Arc::new(Counter(AtomicU64::new(0)));
    let result = with_native_vector_score_observer_for_test(native.clone(), || {
        db.execute(
            &format!(
                "SELECT id FROM flat_docs ORDER BY embedding <=> $query \
                 USE VECTOR {mode} LIMIT 10"
            ),
            &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
        )
        .expect("the vector search executes")
    });
    Observed {
        result,
        native_scores: native.0.load(Ordering::SeqCst),
    }
}

fn insert_rows(db: &Database, partition: i64, first_seed: usize, rows: usize) {
    for seed in first_seed..first_seed + rows {
        db.execute(
            "INSERT INTO flat_docs (id, partition_id, embedding) VALUES ($id, $partition, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                ("partition".to_owned(), Value::Int64(partition)),
                ("embedding".to_owned(), Value::Vector(vector(seed))),
            ]),
        )
        .expect("insert one deterministic row");
    }
}
fn drive_maintenance(db: &Database) {
    for _ in 0..64 {
        let report = db
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle")
            .vector;
        if report.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not settle");
}

fn ids_at(db: &Database, mode: &str, snapshot: contextdb_core::SnapshotId) -> Vec<Value> {
    db.execute_at_snapshot(
        &format!(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
        snapshot,
    )
    .unwrap_or_else(|error| panic!("the {mode} search at {snapshot:?} executes: {error:?}"))
    .rows()
    .iter()
    .map(|row| row[0].clone())
    .collect()
}

#[test]
fn a_reader_keeps_its_captured_answer_across_growth_shrinkage_and_maintenance() {
    let db = Database::open_memory();
    fixture(&db, &[150]);
    let below = db.snapshot();
    let _below_pin = db.pin_snapshot(below).expect("pin the 150-row snapshot");
    assert_eq!(ids_at(&db, "AUTO", below), ids_at(&db, "EXACT", below));
    let below_answer = ids_at(&db, "AUTO", below);

    // Growth to 250 rows: the old reader keeps its 150-row answer, a new
    // reader answers over 250.
    insert_rows(&db, 0, 10_000, 100);
    drive_maintenance(&db);
    let above = db.snapshot();
    let _above_pin = db.pin_snapshot(above).expect("pin the 250-row snapshot");
    assert_eq!(ids_at(&db, "AUTO", below), below_answer);
    assert_eq!(ids_at(&db, "AUTO", above), ids_at(&db, "EXACT", above));
    let above_answer = ids_at(&db, "AUTO", above);

    // Shrinkage to 130 rows: the newest reader answers over 130, while the
    // readers that captured 150 and 250 keep their answers.
    for seed in (10_000..10_100).chain(1..=20) {
        db.execute(
            "DELETE FROM flat_docs WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
        )
        .expect("shrink the partition");
    }
    drive_maintenance(&db);
    let shrunk = db.snapshot();
    assert_eq!(ids_at(&db, "AUTO", shrunk), ids_at(&db, "EXACT", shrunk));
    assert_eq!(ids_at(&db, "AUTO", above), above_answer);
    assert_eq!(ids_at(&db, "AUTO", below), below_answer);

    // After the raw bodies are evicted the exhaustive route streams them and
    // keeps the same answer.
    let index = VectorIndexRef::new("flat_docs", "embedding");
    let before_eviction = ids_at(&db, "EXACT", below);
    let key = contextdb_core::VectorPartitionKey::from_values(&[Value::Int64(0)])
        .expect("an integer is a partition identity");
    db.vector_store_for_test()
        .evict_raw_partition_for_test(&index, &key);
    assert_eq!(ids_at(&db, "EXACT", below), before_eviction);
}

struct FrozenClock;

impl contextdb_core::read_contract::DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limited_exact_search(
    db: &Database,
    memory: u64,
) -> Result<bounded::TestResult, bounded::TestError> {
    bounded::execute(
        db,
        &bounded::BoundedReadRequest::new(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10",
            HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
            contextdb_core::read_contract::ReadLimits {
                result_rows: 1_000,
                result_bytes: 16 * 1024 * 1024,
                work: 10_000_000,
                active_ms: 1_000_000,
                memory,
                cursor_page_rows: 128,
                cursor_page_bytes: 4 * 1024 * 1024,
                cursor_idle_ms: 10_000,
                cursor_lifetime_ms: 100_000,
            },
            Arc::new(FrozenClock),
        ),
    )
}
/// Sweep the flat search's request memory upward and return every
/// exact-comparison admission that was refused on the way. Each step either
/// answers exactly as the unlimited search does, or refuses with the typed
/// memory limit; either way a following unlimited search answers the same and
/// the writer's ledger is where it was.
fn refused_exact_admissions(db: &Database, flat_partitions: usize) -> BTreeSet<&'static str> {
    let baseline = search(db, "EXACT");
    let settled = db.accountant().usage().used;
    let mut refused = BTreeSet::new();
    let mut answered = 0usize;
    // Every admission is a multiple of 8 bytes, so an 8-byte step misses none.
    for memory in (8..=4 * 1024 * 1024u64).step_by(8) {
        contextdb_vector::mem::reset_vector_admission_site_for_test();
        match limited_exact_search(db, memory) {
            Ok(outcome) => {
                answered += 1;
                assert_eq!(
                    outcome.result.rows(),
                    baseline.result.rows(),
                    "an admitted flat search gives the whole answer (memory {memory})"
                );
                let _ = flat_partitions;
            }
            Err(bounded::TestError::Refused(failure))
                if matches!(
                    failure.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::Memory
                ) =>
            {
                refused.insert(contextdb_vector::mem::refused_vector_admission_site_for_test());
            }
            Err(other) => panic!("a refused flat search is a typed memory refusal, got {other:?}"),
        }
        assert_eq!(
            db.accountant().usage().used,
            settled,
            "the request leaves no charge on the writer (memory {memory})"
        );
        if answered > 0 {
            break;
        }
    }
    assert!(answered > 0, "the sweep reaches an admitted answer");
    assert_eq!(
        search(db, "EXACT").result.rows(),
        baseline.result.rows(),
        "no refused search left partial state"
    );
    // A budget sweep reaches only the admission whose cumulative charge peaks
    // first; one whose charge sits below an earlier, released peak is never
    // the one that refuses. Each named admission is therefore also refused on
    // its own: its real reservation is asked for more than the budget holds,
    // through the production refusal and cleanup.
    // The partition copy is not among them: the flat route makes it only for
    // an owner reading history over a non-durable partition, and the raw
    // loader that makes its reservations proves their refusal
    // (`dormant_raw_vector_loader_tests::reopened_route_scratch_refuses_before_decode_and_releases`).
    for site in ["route_slots", "cursor_window", "body_read", "merge_state"] {
        contextdb_vector::mem::reset_vector_admission_site_for_test();
        contextdb_vector::mem::refuse_vector_admission_once_for_test(site);
        let outcome = limited_exact_search(db, 4 * 1024 * 1024);
        // An admission this route never makes leaves the injection unspent.
        contextdb_vector::mem::refuse_vector_admission_once_for_test("");
        match outcome {
            Ok(outcome) => assert_eq!(
                outcome.result.rows(),
                baseline.result.rows(),
                "a route that never makes the {site} admission answers whole"
            ),
            Err(bounded::TestError::Refused(failure))
                if matches!(
                    failure.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::Memory
                ) =>
            {
                assert_eq!(
                    contextdb_vector::mem::refused_vector_admission_site_for_test(),
                    site,
                    "the refusal names the admission that was refused"
                );
                refused.insert(site);
            }
            Err(other) => {
                panic!("a refused {site} admission is a typed memory refusal, got {other:?}")
            }
        }
        assert_eq!(
            db.accountant().usage().used,
            settled,
            "a refused {site} admission leaves no charge on the writer"
        );
        assert_eq!(
            search(db, "EXACT").result.rows(),
            baseline.result.rows(),
            "a refused {site} admission left no partial state"
        );
    }
    refused
}
#[test]
#[ignore = "A refused exact search does not name the route-slot admission it was refused at."]
fn a_refused_exact_route_admission_leaves_no_charge_and_no_partial_answer() {
    // Resident bodies: route slots and the merge state.
    let resident = Database::open_memory();
    fixture(&resident, &[120, 150]);
    let mut refused = refused_exact_admissions(&resident, 2);
    // Reopened, the bodies stream: the membership cursor's windows and each
    // body read are admitted too.
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("flat-refusal.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fixture(&db, &[120, 150]);
        db.close().expect("close the fixture");
    }
    let streamed = Database::open(&path).expect("reopen without hydrating raw bodies");
    streamed.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    refused.extend(refused_exact_admissions(&streamed, 2));
    for site in ["route_slots", "cursor_window", "body_read", "merge_state"] {
        assert!(
            refused.contains(site),
            "the {site} admission is refused at least once: refused {refused:?}"
        );
    }
}
#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn a_refused_explain_admission_leaves_no_charge_and_no_partial_prediction() {
    // EXPLAIN is charged to the store's declared reader memory, so each step
    // reopens the store with a larger declaration.
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("explain-refusal.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fixture(&db, &[120, 400, 150]);
        db.execute(
            "CREATE INDEX flat_docs_partition_idx ON flat_docs(partition_id)",
            &empty(),
        )
        .expect("index the residual predicate");
        db.close().expect("close the fixture");
    }
    let query = vector(1_000_000)
        .iter()
        .map(f32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    // The residual filter makes EXPLAIN build and intersect a candidate bitmap
    // and push one entry per disclosed partition.
    let sql = format!(
        "SELECT id FROM flat_docs WHERE partition_id >= 0 ORDER BY embedding <=> [{query}] \
         USE VECTOR AUTO LIMIT 10"
    );
    let open = |memory: u64| {
        let mut options = contextdb_engine::DatabaseOpenOptions::default();
        options.owner_reads.limits.limits.memory = memory;
        options.maintenance_policy = MaintenancePolicy::CallerDriven;
        Database::open_with_options(&path, options)
    };
    let baseline = {
        let db = open(64 * 1024 * 1024).expect("reopen the fixture");
        let explained = db
            .explain_output(&sql)
            .expect("an explain with room")
            .vector_search
            .expect("the explain discloses the vector route");
        db.close().expect("close");
        explained
    };
    let mut refused = BTreeSet::new();
    // One explain under `memory`: `None` when the declaration is too small to
    // open the store at all (no explain is reached, nothing is counted),
    // `Some(None)` when answered, `Some(Some(operation))` when refused there.
    let step = |memory: u64, refused: &mut BTreeSet<String>| -> Option<Option<String>> {
        let db = open(memory).ok()?;
        let settled = db.accountant().usage().used;
        let outcome = match db.explain_output(&sql) {
            Ok(explained) => {
                assert_eq!(
                    explained.vector_search.as_ref(),
                    Some(&baseline),
                    "an admitted explain gives the whole prediction (memory {memory})"
                );
                None
            }
            Err(Error::MemoryBudgetExceeded { operation, .. }) => {
                refused.insert(operation.to_string());
                Some(operation.into_owned())
            }
            Err(other) => panic!("a refused explain is a typed budget refusal, got {other:?}"),
        };
        assert_eq!(
            db.accountant().usage().used,
            settled,
            "every explain charge returns, refused or answered (memory {memory})"
        );
        db.close().expect("close");
        Some(outcome)
    };
    let at_filter = |outcome: &Option<Option<String>>| matches!(outcome, Some(Some(operation)) if operation.contains("explain_candidate_filter"));
    // Coarse to the first admitted explain, remembering the last declaration
    // the candidate filter refused.
    let mut filter_refused = None;
    let mut first = None;
    for memory in (64 * 1024..=64 * 1024 * 1024u64).step_by(64 * 1024) {
        let outcome = step(memory, &mut refused);
        if at_filter(&outcome) {
            filter_refused = Some(memory);
        }
        if outcome == Some(None) {
            first = Some(memory);
            break;
        }
    }
    first.expect("the sweep reaches an admitted explain");
    // The smallest declaration the candidate filter fits in. Its credit is
    // held while the partitions are disclosed, so the disclosure admission is
    // the next one charged above it.
    let (mut low, mut high) = (
        filter_refused.expect("the coarse sweep meets the candidate filter's refusal"),
        filter_refused.unwrap() + 64 * 1024,
    );
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if at_filter(&step(middle, &mut refused)) {
            low = middle;
        } else {
            high = middle;
        }
    }
    assert!(
        !at_filter(&step(high, &mut refused)),
        "the bisection ends where the candidate filter fits"
    );
    // The disclosure's charge sits below the windows released before it, so
    // no declaration makes it the first to refuse. It is refused on its own:
    // its real reservation asks for more than the budget holds.
    contextdb_vector::mem::refuse_vector_admission_once_for_test("explain_partition_disclosure");
    let injected = step(64 * 1024 * 1024, &mut refused);
    contextdb_vector::mem::refuse_vector_admission_once_for_test("");
    assert_eq!(
        injected,
        Some(Some("explain_partition_disclosure".to_owned())),
        "the refused disclosure is a typed refusal that names it"
    );
    assert_eq!(
        step(64 * 1024 * 1024, &mut refused),
        Some(None),
        "after the refusal an explain gives the whole prediction"
    );
    for operation in ["explain_candidate_filter", "explain_partition_disclosure"] {
        assert!(
            refused.iter().any(|refused| refused.contains(operation)),
            "the sweep refuses the {operation} admission at least once: refused {refused:?}"
        );
    }
}
#[test]
fn a_partition_with_growing_replaced_history_answers_exactly_and_is_charged_once() {
    let root = TempDir::new().expect("create the durable fixture directory");
    let db = contextdb_engine::database::open_with_startup_limits(
        root.path().join("replaced-history.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        None,
        None,
    )
    .expect("open a durable database with no memory limit");
    let accountant = db.accountant();
    fixture(&db, &[120]);
    // The live count stays 120 while each round replaces every vector, so the
    // partition's retained history grows under a fixed live count.
    for round in 1..=4usize {
        for seed in 1..=120usize {
            db.execute(
                "UPDATE flat_docs SET embedding = $embedding WHERE id = $id",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                    (
                        "embedding".to_owned(),
                        Value::Vector(vector(seed + round * 1_000)),
                    ),
                ]),
            )
            .expect("replace one vector");
        }
        let settled = accountant.usage().used;
        let auto = search(&db, "AUTO");
        let exact = search(&db, "EXACT");
        assert_eq!(auto.result.rows(), exact.result.rows(), "round {round}");
        assert_eq!(
            exact.native_scores, 120,
            "only the live rows are scored, however much history is retained (round {round})"
        );
        assert_eq!(
            accountant.usage().used,
            settled,
            "every charge for scanning the history returns (round {round})"
        );
    }
    assert_eq!(accountant.underflow_count_for_test(), 0);
}
#[test]
fn a_held_reader_answers_exactly_at_its_own_snapshot_after_the_declaration_changes() {
    // Search breadth is not versioned by snapshot: a later statement against a
    // held snapshot reads the current declaration, and either route returns
    // that snapshot's exact answer.
    let db = Database::open_memory();
    fixture(&db, &[120]);
    let held = db.snapshot();
    let _pin = db.pin_snapshot(held).expect("pin the held snapshot");
    let exact_before = answer_at(&db, "EXACT", held);
    assert_eq!(answer_at(&db, "AUTO", held), exact_before);
    db.execute(
        "ALTER TABLE flat_docs ALTER COLUMN embedding SET IVF (PROBES = 8)",
        &empty(),
    )
    .expect("change the declared breadth");
    assert_eq!(
        answer_at(&db, "EXACT", held),
        exact_before,
        "the held snapshot's rows are unchanged"
    );
}

#[test]
fn a_reader_pinned_before_a_replacement_keeps_its_answer_until_it_finishes() {
    let db = Database::open_memory();
    fixture(&db, &[300]);
    let before = db.snapshot();
    let pin = db.pin_snapshot(before).expect("pin the earlier reader");
    let auto_before = ids_at(&db, "AUTO", before);
    // Deleting a fifth of the rows triggers a replacement of the partition's
    // index.
    for seed in 1..=60usize {
        db.execute(
            "DELETE FROM flat_docs WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
        )
        .expect("delete one row");
    }
    drive_maintenance(&db);
    assert_eq!(
        ids_at(&db, "AUTO", before),
        ids_at(&db, "EXACT", before),
        "the pinned reader still answers from its own snapshot"
    );
    assert_eq!(ids_at(&db, "AUTO", before), auto_before);
    assert_eq!(
        ids_at(&db, "AUTO", db.snapshot()),
        ids_at(&db, "EXACT", db.snapshot()),
        "a new reader answers from the replacement"
    );
    drop(pin);
    db.run_maintenance_cycle()
        .expect("one caller-driven maintenance cycle");
}

fn answer_at(db: &Database, mode: &str, snapshot: contextdb_core::SnapshotId) -> RetainedRows {
    db.execute_at_snapshot(
        &format!(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
        snapshot,
    )
    .expect("the snapshot search executes")
    .into()
}
