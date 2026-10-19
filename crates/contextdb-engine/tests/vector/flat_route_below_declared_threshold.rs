//! A partition whose captured live rows are below the column's declared
//! `FLAT_BELOW` is served by INDEXED from its maintained flat representation:
//! every allowed row of that partition is compared exactly, no graph is
//! walked or loaded for it, and the route receipt says why. At or above the
//! threshold the partition walks its graph as before. A scope may mix both;
//! its answer is the one global top-k either way.

use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
use contextdb_core::{Error, Value, VectorIndexRef, VectorPolicyDeclarationIssue as Issue};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, VectorSearchRoute};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 8;
const FLAT_REASON: &str = "partition_below_flat_threshold";

struct Counter(AtomicU64);

impl HnswCandidateObserver for Counter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
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

/// A partitioned INDEXED table with `FLAT_BELOW` declared (or omitted), the
/// given live rows per partition, and every partition graph maintained.
fn fixture(db: &Database, flat_below: Option<u32>, rows_per_partition: &[usize]) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let flat = flat_below.map_or(String::new(), |value| format!(" FLAT_BELOW {value}"));
    db.execute(
        &format!(
            "CREATE TABLE flat_docs (id UUID PRIMARY KEY, partition_id INTEGER NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (partition_id) MAX_PARTITIONS 8 \
             SEARCH_MODE INDEXED AUTO_INDEX_AT 1{flat})"
        ),
        &empty(),
    )
    .expect("declare the partitioned INDEXED table");
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
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == rows_per_partition.len() && status.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish every partition graph");
}

struct Observed {
    result: QueryResult,
    graph_scores: u64,
    native_scores: u64,
}

fn search(db: &Database, mode: &str) -> Observed {
    let graph = Arc::new(Counter(AtomicU64::new(0)));
    let native = Arc::new(Counter(AtomicU64::new(0)));
    let result = with_hnsw_candidate_observer(graph.clone(), || {
        with_native_vector_score_observer_for_test(native.clone(), || {
            db.execute(
                &format!(
                    "SELECT id FROM flat_docs ORDER BY embedding <=> $query \
                     USE VECTOR {mode} LIMIT 10"
                ),
                &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
            )
            .expect("the vector search executes")
        })
    });
    Observed {
        result,
        graph_scores: graph.0.load(Ordering::SeqCst),
        native_scores: native.0.load(Ordering::SeqCst),
    }
}

/// Each partition's executed route and reason, in partition order.
fn routes(result: &QueryResult) -> Vec<(VectorSearchRoute, Option<String>)> {
    result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route")
        .partition_hnsw
        .iter()
        .map(|partition| {
            (
                partition.route.expect("an executed partition has a route"),
                partition.reason.clone(),
            )
        })
        .collect()
}

fn expected(flat_below: usize, rows_per_partition: &[usize]) -> Vec<bool> {
    rows_per_partition
        .iter()
        .map(|rows| *rows < flat_below)
        .collect()
}

#[test]
fn indexed_below_a_declared_flat_threshold_compares_its_rows_exactly() {
    let rows = [120, 150, 90];
    let db = Database::open_memory();
    fixture(&db, Some(200), &rows);
    assert_eq!(expected(200, &rows), vec![true, true, true]);
    let indexed = search(&db, "INDEXED");
    let exact = search(&db, "EXACT");
    assert_eq!(
        routes(&indexed.result),
        vec![(VectorSearchRoute::Exact, Some(FLAT_REASON.to_owned())); 3],
        "every partition below FLAT_BELOW is served flat, and its receipt says why"
    );
    assert_eq!(indexed.graph_scores, 0, "a flat partition walks no graph");
    assert_eq!(
        indexed.native_scores,
        rows.iter().sum::<usize>() as u64,
        "the flat route compares every allowed row of each flat partition once"
    );
    assert_eq!(
        indexed.result.rows, exact.result.rows,
        "the flat answer is EXACT's"
    );
    let disclosure = indexed
        .result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route");
    assert_eq!(
        (
            disclosure.effective_flat_below,
            disclosure.flat_below_source.as_str()
        ),
        (200, "declared"),
        "the receipt names the effective threshold and its source"
    );
    let passive = db
        .explain_output(&format!(
            "SELECT id FROM flat_docs ORDER BY embedding <=> [{}] USE VECTOR INDEXED LIMIT 10",
            vector(1_000_000)
                .iter()
                .map(f32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .expect("passive explain")
        .vector_search
        .expect("passive explain discloses the vector route");
    assert_eq!(
        (
            passive.effective_flat_below,
            passive.flat_below_source.as_str()
        ),
        (200, "declared"),
        "EXPLAIN names the effective threshold and its source"
    );
}

#[test]
fn a_partition_at_the_flat_threshold_walks_its_graph() {
    let rows = [200, 200];
    let db = Database::open_memory();
    fixture(&db, Some(200), &rows);
    assert_eq!(expected(200, &rows), vec![false, false]);
    let indexed = search(&db, "INDEXED");
    assert!(indexed.graph_scores > 0, "equality walks the graph");
    for (route, reason) in routes(&indexed.result) {
        assert_eq!(route, VectorSearchRoute::Indexed);
        assert_ne!(reason.as_deref(), Some(FLAT_REASON));
    }
}

#[test]
fn an_undeclared_flat_threshold_keeps_every_partition_on_its_graph() {
    let rows = [120, 150];
    let db = Database::open_memory();
    fixture(&db, None, &rows);
    let indexed = search(&db, "INDEXED");
    assert!(indexed.graph_scores > 0, "the default is never flat");
    let disclosure = indexed
        .result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route");
    assert_eq!(
        (
            disclosure.effective_flat_below,
            disclosure.flat_below_source.as_str()
        ),
        (0, "compatibility_profile")
    );
    for (_, reason) in routes(&indexed.result) {
        assert_ne!(reason.as_deref(), Some(FLAT_REASON));
    }
}

#[test]
fn a_mixed_scope_serves_small_partitions_flat_and_large_ones_by_graph() {
    for rows in [[90usize, 400, 150], [400, 90, 400]] {
        let db = Database::open_memory();
        fixture(&db, Some(200), &rows);
        let indexed = search(&db, "INDEXED");
        let exact = search(&db, "EXACT");
        let flat = expected(200, &rows);
        let observed = routes(&indexed.result);
        assert_eq!(observed.len(), rows.len());
        for ((route, reason), flat) in observed.iter().zip(&flat) {
            if *flat {
                assert_eq!(
                    (*route, reason.as_deref()),
                    (VectorSearchRoute::Exact, Some(FLAT_REASON))
                );
            } else {
                assert_eq!(*route, VectorSearchRoute::Indexed);
            }
        }
        assert!(
            indexed.graph_scores > 0,
            "the large partitions walk their graphs"
        );
        assert_eq!(
            indexed.result.rows, exact.result.rows,
            "one global top-10 across flat and graph partitions ({rows:?})"
        );
        assert!(
            indexed.graph_scores + indexed.native_scores <= exact.native_scores,
            "a mixed scope does no more work than EXACT ({rows:?}): {} + {} > {}",
            indexed.graph_scores,
            indexed.native_scores,
            exact.native_scores
        );
    }
}

fn refusal_issue(error: Error) -> Issue {
    match error {
        Error::InvalidVectorPolicyDeclaration { issue, .. } => issue,
        other => panic!("expected a vector policy refusal, got {other:?}"),
    }
}

#[test]
fn flat_below_is_declared_validated_altered_and_reset() {
    let db = Database::open_memory();
    for (clause, issue) in [
        ("FLAT_BELOW -1", Issue::FlatBelowNegative),
        ("FLAT_BELOW 4294967296", Issue::FlatBelowOutOfRange),
    ] {
        let error = db
            .execute(
                &format!(
                    "CREATE TABLE bad_flat (id INTEGER PRIMARY KEY, embedding VECTOR(2) {clause})"
                ),
                &empty(),
            )
            .expect_err("an invalid FLAT_BELOW is refused");
        assert_eq!(refusal_issue(error), issue, "{clause}");
    }
    db.execute(
        "CREATE TABLE flat_policy (id INTEGER PRIMARY KEY, label TEXT, embedding VECTOR(2) FLAT_BELOW 300)",
        &empty(),
    )
    .expect("declare FLAT_BELOW on a vector column");
    let declared = |db: &Database| {
        db.table_meta("flat_policy")
            .expect("the table exists")
            .columns
            .into_iter()
            .find(|column| column.name == "embedding")
            .expect("the vector column exists")
            .flat_below
    };
    assert_eq!(declared(&db), Some(300));
    db.execute(
        "ALTER TABLE flat_policy ALTER COLUMN embedding SET FLAT_BELOW 50",
        &empty(),
    )
    .expect("alter FLAT_BELOW");
    assert_eq!(declared(&db), Some(50));
    db.execute(
        "ALTER TABLE flat_policy ALTER COLUMN embedding SET FLAT_BELOW DEFAULT",
        &empty(),
    )
    .expect("reset FLAT_BELOW");
    assert_eq!(declared(&db), None, "DEFAULT clears the declaration");
    let error = db
        .execute(
            "ALTER TABLE flat_policy ALTER COLUMN label SET FLAT_BELOW 10",
            &empty(),
        )
        .expect_err("FLAT_BELOW is refused on a non-vector column");
    assert_eq!(refusal_issue(error), Issue::RequiresVectorColumn);
}

#[test]
fn a_declared_flat_threshold_survives_reopen_and_is_honoured_after_it() {
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("flat.redb");
    let rows = [120, 150];
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fixture(&db, Some(200), &rows);
        db.close().expect("close the fixture");
    }
    let db = Database::open(&path).expect("reopen the fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let declared = db
        .table_meta("flat_docs")
        .expect("the table exists")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column exists")
        .flat_below;
    assert_eq!(declared, Some(200), "FLAT_BELOW is persisted");
    let indexed = search(&db, "INDEXED");
    assert_eq!(
        routes(&indexed.result),
        vec![(VectorSearchRoute::Exact, Some(FLAT_REASON.to_owned())); 2]
    );
    assert_eq!(indexed.graph_scores, 0);
}

#[test]
fn altering_flat_below_reroutes_new_searches_without_a_rebuild() {
    let rows = [120, 150];
    let db = Database::open_memory();
    fixture(&db, None, &rows);
    let index = VectorIndexRef::new("flat_docs", "embedding");
    let before = db.vector_store_for_test().hnsw_maintenance_status();
    assert!(search(&db, "INDEXED").graph_scores > 0);
    db.execute(
        "ALTER TABLE flat_docs ALTER COLUMN embedding SET FLAT_BELOW 200",
        &empty(),
    )
    .expect("declare FLAT_BELOW");
    let after = db.vector_store_for_test().hnsw_maintenance_status();
    assert_eq!(
        (after.ready_partitions, after.remaining_partitions),
        (before.ready_partitions, before.remaining_partitions),
        "changing FLAT_BELOW asks maintenance for no rebuild of {index}"
    );
    let indexed = search(&db, "INDEXED");
    assert_eq!(
        indexed.graph_scores, 0,
        "the new declaration routes new searches flat"
    );
    db.execute(
        "ALTER TABLE flat_docs ALTER COLUMN embedding SET FLAT_BELOW DEFAULT",
        &empty(),
    )
    .expect("reset FLAT_BELOW");
    assert!(
        search(&db, "INDEXED").graph_scores > 0,
        "DEFAULT walks graphs again"
    );
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
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        if db
            .vector_store_for_test()
            .hnsw_maintenance_status()
            .remaining_partitions
            == 0
        {
            return;
        }
    }
    panic!("maintenance did not settle");
}

fn routes_at(db: &Database, snapshot: contextdb_core::SnapshotId) -> Vec<Option<String>> {
    db.execute_at_snapshot(
        "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
        &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
        snapshot,
    )
    .unwrap_or_else(|error| {
        panic!(
            "the search at snapshot {snapshot:?} executes: {error:?}; graph status {:?}",
            db.vector_store_for_test().hnsw_maintenance_status()
        )
    })
    .trace
    .vector_search
    .expect("the snapshot search discloses its route")
    .partition_hnsw
    .into_iter()
    .map(|partition| partition.reason)
    .collect()
}

#[test]
fn a_reader_keeps_its_captured_count_and_route_across_a_threshold_crossing() {
    let db = Database::open_memory();
    fixture(&db, Some(200), &[150]);
    let flat = vec![Some(FLAT_REASON.to_owned())];
    let below = db.snapshot();
    let _below_pin = db
        .pin_snapshot(below)
        .expect("pin the below-threshold snapshot");
    assert_eq!(routes_at(&db, below), flat);

    // Growth past the threshold: the old reader still sees 150 live rows and
    // stays flat; a new reader sees 250 and walks the graph. No rebuild is
    // needed for either.
    insert_rows(&db, 0, 10_000, 100);
    drive_maintenance(&db);
    let above = db.snapshot();
    let _above_pin = db
        .pin_snapshot(above)
        .expect("pin the above-threshold snapshot");
    assert_eq!(
        routes_at(&db, below),
        flat,
        "the old reader keeps its flat route"
    );
    assert_ne!(
        routes_at(&db, above),
        flat,
        "the grown partition walks its graph"
    );

    // Shrinkage back below it: the newest reader is flat again, while the
    // reader that captured 250 rows keeps its graph route.
    // 250 live rows less the 100 added and 20 originals: 130.
    for seed in (10_000..10_100).chain(1..=20) {
        db.execute(
            "DELETE FROM flat_docs WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
        )
        .expect("shrink the partition below the threshold");
    }
    drive_maintenance(&db);
    let shrunk = db.snapshot();
    assert_eq!(
        routes_at(&db, shrunk),
        flat,
        "the shrunk partition is flat again"
    );
    assert_ne!(
        routes_at(&db, above),
        flat,
        "the reader that captured 250 rows keeps its graph route"
    );
    assert_eq!(
        routes_at(&db, below),
        flat,
        "the first reader is still flat"
    );

    // After the raw bodies are evicted the flat route streams them and keeps
    // the same answer.
    let index = VectorIndexRef::new("flat_docs", "embedding");
    let before_eviction = db
        .execute_at_snapshot(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
            below,
        )
        .expect("search before eviction")
        .rows;
    let key = contextdb_core::VectorPartitionKey::from_values(&[Value::Int64(0)])
        .expect("an integer is a partition identity");
    db.vector_store_for_test()
        .evict_raw_partition_for_test(&index, &key);
    let after_eviction = db
        .execute_at_snapshot(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
            below,
        )
        .expect("search after eviction")
        .rows;
    assert_eq!(after_eviction, before_eviction);
}

#[derive(Clone, Copy)]
struct FrozenClock;

impl contextdb_core::read_contract::DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limited_flat_search(
    db: &Database,
    memory: u64,
) -> Result<bounded::TestResult, bounded::TestError> {
    bounded::execute(
        db,
        &bounded::BoundedReadRequest::new(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
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
fn refused_flat_admissions(db: &Database, flat_partitions: usize) -> BTreeSet<&'static str> {
    let baseline = search(db, "INDEXED");
    assert_eq!(baseline.graph_scores, 0, "the fixture is served flat");
    let settled = db.accountant().usage().used;
    let mut refused = BTreeSet::new();
    let mut answered = 0usize;
    // Every admission is a multiple of 8 bytes, so an 8-byte step misses none.
    for memory in (8..=4 * 1024 * 1024u64).step_by(8) {
        contextdb_vector::mem::reset_vector_admission_site_for_test();
        match limited_flat_search(db, memory) {
            Ok(outcome) => {
                answered += 1;
                assert_eq!(
                    outcome.result.rows, baseline.result.rows,
                    "an admitted flat search gives the whole answer (memory {memory})"
                );
                assert_eq!(
                    routes(&outcome.result),
                    vec![(VectorSearchRoute::Exact, Some(FLAT_REASON.to_owned())); flat_partitions]
                );
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
        search(db, "INDEXED").result.rows,
        baseline.result.rows,
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
        let outcome = limited_flat_search(db, 4 * 1024 * 1024);
        // An admission this route never makes leaves the injection unspent.
        contextdb_vector::mem::refuse_vector_admission_once_for_test("");
        match outcome {
            Ok(outcome) => assert_eq!(
                outcome.result.rows, baseline.result.rows,
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
            search(db, "INDEXED").result.rows,
            baseline.result.rows,
            "a refused {site} admission left no partial state"
        );
    }
    refused
}

#[test]
fn a_refused_flat_route_admission_leaves_no_charge_and_no_partial_answer() {
    // Resident bodies: route slots and the merge state.
    let resident = Database::open_memory();
    fixture(&resident, Some(200), &[120, 150]);
    let mut refused = refused_flat_admissions(&resident, 2);
    // Reopened, the bodies stream: the membership cursor's windows and each
    // body read are admitted too.
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("flat-refusal.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fixture(&db, Some(200), &[120, 150]);
        db.close().expect("close the fixture");
    }
    let streamed = Database::open(&path).expect("reopen without hydrating raw bodies");
    streamed.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    refused.extend(refused_flat_admissions(&streamed, 2));
    for site in ["route_slots", "cursor_window", "body_read", "merge_state"] {
        assert!(
            refused.contains(site),
            "the {site} admission is refused at least once: refused {refused:?}"
        );
    }
}

#[test]
fn an_explicit_zero_flat_threshold_is_declared_and_never_flat() {
    let rows = [120, 150];
    let db = Database::open_memory();
    fixture(&db, Some(0), &rows);
    let declared = db
        .table_meta("flat_docs")
        .expect("the table exists")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column exists")
        .flat_below;
    assert_eq!(
        declared,
        Some(0),
        "an explicit 0 is kept, distinct from omission"
    );
    let indexed = search(&db, "INDEXED");
    assert!(
        indexed.graph_scores > 0,
        "FLAT_BELOW 0 serves no partition flat"
    );
    for (_, reason) in routes(&indexed.result) {
        assert_ne!(reason.as_deref(), Some(FLAT_REASON));
    }
    let disclosure = indexed
        .result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route");
    assert_eq!(
        (
            disclosure.effective_flat_below,
            disclosure.flat_below_source.as_str()
        ),
        (0, "declared")
    );
}

/// Each partition's route, with its reason where the route is exact: a walk's
/// reason names its evidence (predicted or executed), an exact one is the same
/// on both sides.
fn comparable_routes(
    disclosure: &contextdb_engine::VectorSearchDisclosure,
) -> Vec<(Option<VectorSearchRoute>, Option<String>)> {
    disclosure
        .partition_hnsw
        .iter()
        .map(|partition| {
            let reason = (partition.route == Some(VectorSearchRoute::Exact))
                .then(|| partition.reason.clone())
                .flatten();
            (partition.route, reason)
        })
        .collect()
}

fn assert_explain_predicts_execution(db: &Database, filter: &str) {
    let query = vector(1_000_000)
        .iter()
        .map(f32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT id FROM flat_docs {filter}ORDER BY embedding <=> [{query}] USE VECTOR INDEXED LIMIT 10"
    );
    let passive = db
        .explain_output(&sql)
        .expect("passive explain")
        .vector_search
        .expect("passive explain discloses the vector route");
    let executed = db
        .execute(&sql, &empty())
        .expect("the search executes")
        .trace
        .vector_search
        .expect("the executed search discloses its route");
    assert_eq!(
        passive.route, executed.route,
        "statement route ({filter:?})"
    );
    assert_eq!(
        comparable_routes(&passive),
        comparable_routes(&executed),
        "every partition's predicted route is its executed one ({filter:?})"
    );
}

#[test]
fn explain_predicts_the_flat_route_execution_takes() {
    // All flat, mixed in both orders, and a partition exactly at the threshold.
    for rows in [
        vec![120, 150],
        vec![90, 400, 150],
        vec![400, 90],
        vec![200, 199],
    ] {
        let db = Database::open_memory();
        fixture(&db, Some(200), &rows);
        assert_explain_predicts_execution(&db, "");
        // A residual filter does not move the comparator: it reads the
        // captured live count before filtering, as execution does.
        db.execute(
            "CREATE INDEX flat_docs_partition_idx ON flat_docs(partition_id)",
            &empty(),
        )
        .expect("index the residual predicate");
        assert_explain_predicts_execution(&db, "WHERE partition_id >= 0 ");
    }
}

#[test]
fn a_refused_explain_admission_leaves_no_charge_and_no_partial_prediction() {
    // EXPLAIN is charged to the store's declared reader memory, so each step
    // reopens the store with a larger declaration.
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("explain-refusal.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fixture(&db, Some(200), &[120, 400, 150]);
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
         USE VECTOR INDEXED LIMIT 10"
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
fn auto_over_declared_flat_partitions_does_no_more_work_than_exact() {
    let rows = [120, 150, 90];
    let db = Database::open_memory();
    fixture(&db, Some(200), &rows);
    let auto = search(&db, "AUTO");
    let exact = search(&db, "EXACT");
    assert_eq!(
        auto.graph_scores, 0,
        "AUTO walks no graph for flat partitions"
    );
    assert!(
        auto.native_scores <= exact.native_scores,
        "AUTO over flat partitions does no more work than EXACT: {} > {}",
        auto.native_scores,
        exact.native_scores
    );
    assert_eq!(auto.result.rows, exact.result.rows);
}

fn declared_flat_below(db: &Database) -> Option<u32> {
    db.table_meta("flat_docs")
        .expect("the table exists")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column exists")
        .flat_below
}

fn policy_revision(db: &Database) -> u64 {
    db.table_meta("flat_docs")
        .expect("the table exists")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column exists")
        .vector_policy_revision
}

fn replay(source: &Database, receiver: &Database, since: contextdb_core::Lsn) {
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    receiver
        .apply_changes(
            source
                .changes_since(since)
                .expect("read the source's changes"),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("the receiver applies the source's changes");
}

#[test]
fn flat_below_travels_with_create_alter_and_reset_through_replay() {
    let rows = [120, 150];
    let source = Database::open_memory();
    fixture(&source, Some(200), &rows);
    let receiver = Database::open_memory();
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    replay(&source, &receiver, contextdb_core::Lsn(0));
    for _ in 0..64 {
        receiver
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        let status = receiver.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == rows.len() && status.remaining_partitions == 0 {
            break;
        }
    }
    assert_eq!(
        receiver
            .vector_store_for_test()
            .hnsw_maintenance_status()
            .ready_partitions,
        rows.len(),
        "the receiver's maintenance publishes both partition graphs"
    );
    assert_eq!(
        declared_flat_below(&receiver),
        Some(200),
        "CREATE carries FLAT_BELOW"
    );
    let routed = search(&receiver, "INDEXED");
    assert_eq!(
        routes(&routed.result),
        vec![(VectorSearchRoute::Exact, Some(FLAT_REASON.to_owned())); 2],
        "the receiver honours the replayed declaration"
    );

    let revision = policy_revision(&receiver);
    let watermark = source.current_lsn();
    source
        .execute(
            "ALTER TABLE flat_docs ALTER COLUMN embedding SET FLAT_BELOW 100",
            &empty(),
        )
        .expect("alter FLAT_BELOW on the source");
    replay(&source, &receiver, watermark);
    // A replayed ALTER of any vector policy makes the receiver drop its graphs
    // until maintenance rebuilds them (a recorded base defect in schema replay,
    // not in FLAT_BELOW), so the walk below waits for that rebuild.
    drive_maintenance(&receiver);
    assert_eq!(
        declared_flat_below(&receiver),
        Some(100),
        "ALTER SET carries FLAT_BELOW"
    );
    assert_eq!(
        policy_revision(&receiver),
        revision,
        "a FLAT_BELOW change does not revise the graph topology"
    );
    assert!(
        search(&receiver, "INDEXED").graph_scores > 0,
        "partitions at or above the new threshold walk their graphs"
    );

    let watermark = source.current_lsn();
    source
        .execute(
            "ALTER TABLE flat_docs ALTER COLUMN embedding SET FLAT_BELOW DEFAULT",
            &empty(),
        )
        .expect("reset FLAT_BELOW on the source");
    replay(&source, &receiver, watermark);
    assert_eq!(
        declared_flat_below(&receiver),
        None,
        "DEFAULT clears it on the receiver"
    );
}

#[test]
fn reaching_the_flat_threshold_moves_new_readers_to_the_graph_only() {
    let db = Database::open_memory();
    fixture(&db, Some(200), &[199]);
    let flat = vec![Some(FLAT_REASON.to_owned())];
    let below = db.snapshot();
    let _below_pin = db.pin_snapshot(below).expect("pin the 199-row snapshot");
    assert_eq!(routes_at(&db, below), flat);
    insert_rows(&db, 0, 20_000, 1);
    drive_maintenance(&db);
    let at = db.snapshot();
    let _at_pin = db.pin_snapshot(at).expect("pin the 200-row snapshot");
    assert_ne!(
        routes_at(&db, at),
        flat,
        "200 live rows, at the threshold, walk the graph"
    );
    assert_eq!(routes_at(&db, below), flat, "the 199-row reader stays flat");
    // And back: one delete takes new readers below the threshold again.
    db.execute(
        "DELETE FROM flat_docs WHERE id = $id",
        &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(20_000)))]),
    )
    .expect("delete one row");
    drive_maintenance(&db);
    assert_eq!(routes_at(&db, db.snapshot()), flat);
    assert_ne!(
        routes_at(&db, at),
        flat,
        "the 200-row reader keeps its graph route"
    );
}

#[test]
fn indexed_refuses_a_whole_scope_whose_graph_partition_is_unavailable_beside_a_flat_one() {
    let db = Database::open_memory();
    fixture(&db, Some(200), &[120, 400]);
    // A third partition above the threshold whose graph maintenance has not
    // built: INDEXED cannot serve it, and the flat partition beside it does
    // not turn the statement into a partial answer.
    insert_rows(&db, 2, 30_000, 300);
    let outcome = db.execute(
        "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
        &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
    );
    assert!(
        matches!(outcome, Err(Error::VectorIndexedRouteUnavailable { .. })),
        "INDEXED refuses by type rather than answering from the flat and built partitions: {outcome:?}"
    );
}

#[test]
fn a_flat_partition_with_growing_replaced_history_stays_exact_and_charged() {
    use contextdb_engine::memory_accounting::MemoryAccountant;
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(accountant.clone());
    fixture(&db, Some(200), &[120]);
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
        let indexed = search(&db, "INDEXED");
        let exact = search(&db, "EXACT");
        assert_eq!(
            routes(&indexed.result),
            vec![(VectorSearchRoute::Exact, Some(FLAT_REASON.to_owned()))],
            "120 live rows stay flat after round {round}"
        );
        assert_eq!(indexed.result.rows, exact.result.rows, "round {round}");
        assert_eq!(
            indexed.native_scores, 120,
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

fn answer_at(db: &Database, mode: &str, snapshot: contextdb_core::SnapshotId) -> Vec<Vec<Value>> {
    db.execute_at_snapshot(
        &format!(
            "SELECT id FROM flat_docs ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
        snapshot,
    )
    .expect("the snapshot search executes")
    .rows
}

#[test]
fn a_held_reader_reads_the_current_flat_threshold_with_its_own_row_count() {
    // FLAT_BELOW, like AUTO_INDEX_AT and EF_SEARCH, is not versioned by
    // snapshot: a later statement against a held snapshot reads the current
    // declaration, compares it with that snapshot's own live count, and either
    // route returns that snapshot's exact answer.
    let db = Database::open_memory();
    fixture(&db, Some(200), &[120]);
    let held = db.snapshot();
    let _pin = db.pin_snapshot(held).expect("pin the held snapshot");
    let exact_before = answer_at(&db, "EXACT", held);
    assert_eq!(routes_at(&db, held), vec![Some(FLAT_REASON.to_owned())]);
    assert_eq!(answer_at(&db, "INDEXED", held), exact_before);
    db.execute(
        "ALTER TABLE flat_docs ALTER COLUMN embedding SET FLAT_BELOW 100",
        &empty(),
    )
    .expect("lower FLAT_BELOW");
    assert_ne!(
        routes_at(&db, held),
        vec![Some(FLAT_REASON.to_owned())],
        "the held snapshot's 120 rows are at or above the current 100: graph"
    );
    assert_eq!(
        answer_at(&db, "EXACT", held),
        exact_before,
        "the held snapshot's rows are unchanged"
    );
}

/// A read already active when maintenance rebuilds its partition keeps the
/// graph it was answered from: a reader pinned before deletes that trigger a
/// rebuild still walks the graph it started with, and its answer equals
/// EXACT's at its own snapshot. The retired graph is held only while such a
/// reader is: once the reader finishes, the next maintenance cycle releases it.
#[test]
fn a_reader_pinned_before_a_rebuild_keeps_its_graph_until_it_finishes() {
    let db = Database::open_memory();
    fixture(&db, None, &[300]);
    let before = db.snapshot();
    let pin = db.pin_snapshot(before).expect("pin the earlier reader");
    let indexed_before = ids_at(&db, "INDEXED", before);
    // Deleting a fifth of the rows passes the default 10% tombstone share,
    // so maintenance rebuilds the partition's graph.
    for seed in 1..=60usize {
        db.execute(
            "DELETE FROM flat_docs WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
        )
        .expect("delete one row");
    }
    drive_maintenance(&db);
    assert!(
        db.vector_store_for_test().has_retired_graph_generations(),
        "the rebuild retired the graph the pinned reader uses"
    );
    assert_eq!(
        ids_at(&db, "INDEXED", before),
        ids_at(&db, "EXACT", before),
        "the pinned reader still walks its graph"
    );
    assert_eq!(ids_at(&db, "INDEXED", before), indexed_before);
    assert_eq!(
        ids_at(&db, "INDEXED", db.snapshot()),
        ids_at(&db, "EXACT", db.snapshot()),
        "a new reader walks the rebuilt graph"
    );
    drop(pin);
    db.run_maintenance_cycle()
        .expect("one caller-driven maintenance cycle");
    assert!(
        !db.vector_store_for_test().has_retired_graph_generations(),
        "the retired graph is released once its reader finishes"
    );
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
    .rows
    .into_iter()
    .map(|row| row[0].clone())
    .collect()
}
