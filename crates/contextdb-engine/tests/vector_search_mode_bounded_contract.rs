#![cfg(feature = "test-seams")]
//! Contract proofs for whole-query vector modes through the ordinary and
//! memory-bounded execution doors.
//!
//! The tables are deliberately small except for the 1,000-row threshold
//! case. That case is the smallest F32 fixture that can prove a whole-query
//! decision: two selected 500-row partitions must not become two exact scans.

use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Error, RowId, Value, VectorIndexRef, VectorPartitionKey, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const QUERY: &str = "SELECT id FROM vector_docs ORDER BY embedding <=> $query LIMIT 3";

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

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000,
        result_bytes: 16 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 128,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn bounded_request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
}

fn id(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xD00D_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn query_vector() -> Value {
    Value::Vector(vec![1.0, 0.0, 0.0])
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("the projection contains id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(value)) => *value,
            other => panic!("vector result id is UUID, got {other:?}"),
        })
        .collect()
}

fn insert(db: &Database, row_id: Uuid, partition: i64, kind: &str, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO vector_docs (id, partition_id, kind, embedding) \
         VALUES ($id, $partition, $kind, $embedding)",
        &params([
            ("id", Value::Uuid(row_id)),
            ("partition", Value::Int64(partition)),
            ("kind", Value::Text(kind.to_owned())),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("insert a deterministic vector row");
}

fn partitioned_fixture(search_mode: &str) -> Database {
    let db = Database::open_memory();
    declare_partitioned_fixture(&db, search_mode);
    db
}

fn durable_partitioned_fixture(search_mode: &str) -> (TempDir, Database) {
    let root = TempDir::new().expect("create a durable vector-route fixture directory");
    let db = Database::open(root.path().join("vector-route.redb"))
        .expect("open the durable vector-route fixture");
    declare_partitioned_fixture(&db, search_mode);
    (root, db)
}

fn declare_partitioned_fixture(db: &Database, search_mode: &str) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE vector_docs (\
                id UUID PRIMARY KEY, \
                partition_id INTEGER NOT NULL, \
                kind TEXT NOT NULL, \
                embedding VECTOR(3) PARTITION_KEY (partition_id) \
                    MAX_PARTITIONS 8 SEARCH_MODE {search_mode})"
        ),
        &empty(),
    )
    .expect("declare the partitioned vector table");
}

fn drive_maintenance(db: &Database) {
    // A fixed number of finite calls is the public caller-driven operation;
    // this is not a timing wait.
    for _ in 0..32 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance batch succeeds");
    }
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("vector_docs", "embedding")
}

fn partition_key(partition: i64) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Int64(partition)])
        .expect("an integer is a valid partition identity")
}

fn partition_row_ids(db: &Database, partition: i64) -> HashSet<RowId> {
    db.vector_store_for_test()
        .entries_for_partition(&index(), &partition_key(partition))
        .expect("inspect the deterministic partition fixture")
        .into_iter()
        .map(|entry| entry.row_id)
        .collect()
}

fn take_trace(db: &Database) -> contextdb_vector::VectorSearchDebugTrace {
    db.__take_last_query_vector_trace_for_test()
        .expect("the vector query publishes its route trace")
}

fn trace_candidates_in(
    trace: &contextdb_vector::VectorSearchDebugTrace,
    partition_rows: &HashSet<RowId>,
) -> Vec<RowId> {
    trace
        .hnsw_candidate_row_ids
        .iter()
        .copied()
        .filter(|row_id| partition_rows.contains(row_id))
        .collect()
}

#[derive(Default)]
struct HnswCandidateCounter(AtomicU64);

impl bounded::ExecutionProbe for HnswCandidateCounter {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _completed_items: u64) {
        if touch == bounded::TestSourceTouch::HnswCandidate {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

fn assert_bounded_indexed_refusal(result: Result<bounded::TestResult, bounded::TestError>) {
    assert!(
        matches!(&result, Err(bounded::TestError::Engine(message)) if message.contains("no complete maintained route")),
        "bounded INDEXED must refuse the whole request without rows: {result:?}"
    );
}

fn assert_indexed_refusal(error: Error) {
    assert!(
        matches!(&error, Error::VectorIndexedRouteUnavailable { .. }),
        "INDEXED must name an unavailable maintained route rather than silently scanning: {error:?}"
    );
}

/// A filtered `AUTO` search mode is resolved once for all readers.
///
/// A column-default `AUTO` search filtered on a column with NO relational
/// index must resolve to `EXACT` -- named `filtered_route_exact` -- and that
/// resolved mode, reason, and answer must be identical across the ordinary
/// door, the memory-bounded door, and the Rust `SemanticQuery` door, on the
/// SAME snapshot. The aggregate selected scope (1,000 rows across two
/// 500-row partitions) is deliberately AT the count-based crossover, so a
/// route disagreement inside one test on one snapshot is the whole proof: a
/// door that read only the raw statement override would let the store's count
/// crossover admit the graph for a column-default `AUTO`, while a door that
/// resolves the column default itself reports zero HNSW candidates for the
/// identical read.
#[test]
fn a_filtered_search_resolves_one_mode_on_every_door() {
    let db = partitioned_fixture("AUTO");
    // Deliberately NO index on `kind`: the relational route is incomplete,
    // so the route rule -- not the store's count crossover -- must decide.
    for partition in 0..2_i64 {
        for ordinal in 0..500_u128 {
            insert(
                &db,
                id(40_000 + partition as u128 * 1_000 + ordinal),
                partition,
                "eligible",
                vec![1.0, ordinal as f32 * 0.0001, partition as f32 * 0.0001],
            );
        }
    }
    drive_maintenance(&db);
    let sql = "SELECT id FROM vector_docs WHERE kind = $kind \
               ORDER BY embedding <=> $query LIMIT 3";
    let bound = params([
        ("kind", Value::Text("eligible".to_owned())),
        ("query", query_vector()),
    ]);

    // Ordinary door: `requested_auto` must see the column-default
    // AUTO, not only an explicit `USE VECTOR AUTO` override, and force the
    // filtered-exact residual rather than letting the count crossover admit
    // the graph.
    db.__reset_last_query_vector_trace_for_test();
    let ordinary = db
        .execute(sql, &bound)
        .expect("the filtered column-default AUTO search succeeds");
    let ordinary_trace = take_trace(&db);
    assert_eq!(
        ordinary_trace.fallback_reason,
        Some("filtered_route_exact"),
        "a column-default AUTO filtered on an unindexed column must resolve exact, not walk \
         the graph on the count crossover alone: {ordinary_trace:?}"
    );
    assert!(
        !db.__debug_last_query_vector_used_hnsw_for_test(),
        "the ordinary door must not use the maintained route for this filtered read"
    );

    // Bounded disclosure: the bounded door must report the SAME
    // reason, and its disclosed aggregate must stay the true selected-scope
    // count (1,000), never the literal 0 a route-forced-exact read would
    // publish if `declared_mode` and `search_mode` were folded into one
    // field.
    let bounded_result = bounded::execute(&db, &bounded_request(sql, bound.clone()))
        .expect("the bounded filtered column-default AUTO search succeeds");
    let bounded_disclosure = bounded_result
        .result
        .trace
        .vector_search
        .clone()
        .expect("the bounded door publishes a vector disclosure");
    assert_eq!(
        bounded_disclosure.fallback.as_deref(),
        Some("filtered_route_exact"),
        "the bounded door must report the identical reason the ordinary door reported: \
         {bounded_disclosure:?}"
    );
    assert_eq!(
        bounded_disclosure.aggregate_allowed_vectors,
        Some(1_000),
        "a route-forced exact read must still disclose the true selected-scope count, not the \
         literal 0 a folded declared/effective mode would publish: {bounded_disclosure:?}"
    );

    // Rust `SemanticQuery` door: a THIRD independent mode resolution
    // must agree with the other two.
    let mut rust_query = SemanticQuery::new("vector_docs", "embedding", vec![1.0, 0.0, 0.0], 3);
    rust_query.where_clause = Some("kind = 'eligible'".to_owned());
    let rust_results = db
        .semantic_search(rust_query)
        .expect("the Rust filtered column-default AUTO search succeeds");
    let rust_ids: Vec<Uuid> = rust_results
        .iter()
        .map(|result| match result.values.get("id") {
            Some(Value::Uuid(value)) => *value,
            other => panic!("Rust vector result id is UUID, got {other:?}"),
        })
        .collect();

    assert_eq!(
        ids(&ordinary),
        rust_ids,
        "all three doors must resolve the identical mode and therefore return the identical ids"
    );
    assert_eq!(ids(&ordinary), ids(&bounded_result.result));
}

/// `.explain` must report the SAME resolved mode and route the executing
/// doors actually took. Split by inspection path: the non-execution `EXPLAIN`
/// must not report the `Indexed`/`FilteredIndexed` `resolved_mode` and
/// `route` the count crossover alone would give; the executed disclosure must
/// report the forced-exact `resolved_mode` and `fallback` alongside its
/// `Exact` route.
#[test]
fn explain_reports_the_route_forced_exact_mode_the_query_actually_took() {
    let db = partitioned_fixture("AUTO");
    for partition in 0..2_i64 {
        for ordinal in 0..500_u128 {
            insert(
                &db,
                id(41_000 + partition as u128 * 1_000 + ordinal),
                partition,
                "eligible",
                vec![1.0, ordinal as f32 * 0.0001, partition as f32 * 0.0001],
            );
        }
    }
    drive_maintenance(&db);

    // Non-execution EXPLAIN: literal values only, `explain_output`
    // binds nothing.
    let explain_sql = "SELECT id FROM vector_docs WHERE kind = 'eligible' \
                        ORDER BY embedding <=> [1.0, 0.0, 0.0] LIMIT 3";
    let explained = db
        .explain_output(explain_sql)
        .expect("a filtered column-default AUTO explain succeeds passively");
    let disclosure = explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(
        disclosure.requested_mode,
        VectorSearchMode::Auto,
        "the column-default AUTO must still be named as the requested mode"
    );
    assert_eq!(
        disclosure.resolved_mode,
        VectorSearchMode::Exact,
        "the passive explain must report the mode the route rule actually forces, not the \
         mode the count crossover alone would pick: {disclosure:?}"
    );
    assert_eq!(
        disclosure.aggregate_allowed_vectors,
        Some(1_000),
        "the disclosed aggregate must stay the true selected-scope count: {disclosure:?}"
    );

    // Executed disclosure: `resolved_mode` and `fallback` must agree with
    // the route the query took.
    let sql = "SELECT id FROM vector_docs WHERE kind = $kind \
               ORDER BY embedding <=> $query LIMIT 3";
    let bound = params([
        ("kind", Value::Text("eligible".to_owned())),
        ("query", query_vector()),
    ]);
    let runtime = db
        .execute(sql, &bound)
        .expect("the filtered column-default AUTO search executes");
    let runtime_disclosure = runtime
        .trace
        .vector_search
        .expect("the executed query publishes a vector disclosure");
    assert_eq!(
        runtime_disclosure.resolved_mode,
        VectorSearchMode::Exact,
        "the executed disclosure must report the mode the route rule forced: \
         {runtime_disclosure:?}"
    );
    assert_eq!(
        runtime_disclosure.fallback.as_deref(),
        Some("filtered_route_exact"),
        "the executed disclosure must name the route rule as the reason, not claim the \
         maintained index is unavailable when it is fully built: {runtime_disclosure:?}"
    );
}

#[test]
fn auto_uses_the_aggregate_selected_scope_not_each_partition() {
    let db = partitioned_fixture("AUTO");
    // The `kind` predicate is part of candidate selection, so give this route
    // proof the relational index needed to make the maintained vector route
    // eligible. Without it the engine correctly takes the exact residual
    // path, which tests filter support rather than aggregate AUTO sizing.
    db.execute(
        "CREATE INDEX vector_docs_kind_idx ON vector_docs(kind)",
        &empty(),
    )
    .expect("index the candidate-selection predicate");
    // Each state has only 500 F32 vectors, below the automatic graph boundary.
    // Together the selected scope has exactly 1,000, where AUTO must require
    // the indexed route for this one answer.
    for partition in 0..2_i64 {
        for ordinal in 0..500_u128 {
            insert(
                &db,
                id(partition as u128 * 1_000 + ordinal),
                partition,
                "eligible",
                vec![1.0, ordinal as f32 * 0.0001, partition as f32 * 0.0001],
            );
        }
    }
    let sql = "SELECT id FROM vector_docs WHERE kind = $kind \
               ORDER BY embedding <=> $query LIMIT 3";
    let bound = params([
        ("kind", Value::Text("eligible".to_owned())),
        ("query", query_vector()),
    ]);

    // AUTO is allowed to use exact work while no graph is ready and the
    // active budget can pay. The route-choice proof therefore starts only
    // after both partitions have complete maintained graphs: a per-partition
    // threshold implementation would still choose two exact scans here,
    // whereas the approved whole-query count chooses the indexed route.
    drive_maintenance(&db);
    let eager = db
        .execute(sql, &bound)
        .expect("AUTO serves the aggregate boundary after maintenance");
    assert_eq!(eager.rows.len(), 3);
    assert!(
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "two selected 500-row states form one 1,000-row F32 answer and must use the maintained route"
    );

    let bounded = bounded::execute(&db, &bounded_request(sql, bound))
        .expect("the bounded door serves the same maintained aggregate route");
    assert_eq!(bounded.result.rows, eager.rows);
    assert!(
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "the bounded door must not reinterpret the aggregate boundary per partition"
    );
}

#[test]
fn exact_override_is_exhaustive_across_partitions_and_matches_rust_and_bounded_reads() {
    let db = partitioned_fixture("INDEXED");
    let nearest = id(1);
    let middle = id(2);
    let far = id(3);
    insert(&db, nearest, 1, "eligible", vec![1.0, 0.0, 0.0]);
    insert(&db, middle, 2, "eligible", vec![0.8, 0.6, 0.0]);
    insert(&db, far, 1, "eligible", vec![0.0, 1.0, 0.0]);

    let sql = "SELECT id FROM vector_docs WHERE kind = $kind \
               ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 3";
    let bound = params([
        ("kind", Value::Text("eligible".to_owned())),
        ("query", query_vector()),
    ]);
    let eager = db
        .execute(sql, &bound)
        .expect("EXACT overrides the indexed default");
    assert_eq!(ids(&eager), vec![nearest, middle, far]);
    assert!(
        !eager.trace.physical_plan.contains("HNSW"),
        "EXACT never describes a graph route: {:?}",
        eager.trace.physical_plan
    );

    let bounded = bounded::execute(&db, &bounded_request(sql, bound.clone())).expect(
        "the bounded reader performs the same exhaustive request within its declared budget",
    );
    assert_eq!(bounded.result.rows, eager.rows);
    assert!(
        !bounded.result.trace.physical_plan.contains("HNSW"),
        "the bounded reader must not replace EXACT with graph work: {:?}",
        bounded.result.trace.physical_plan
    );

    let mut rust = SemanticQuery::new("vector_docs", "embedding", vec![1.0, 0.0, 0.0], 3);
    rust.search_mode = Some(VectorSearchMode::Exact);
    let rust_ids = db
        .semantic_search(rust)
        .expect("the Rust per-query override is accepted")
        .into_iter()
        .map(|row| match row.values.get("id") {
            Some(Value::Uuid(value)) => *value,
            other => panic!("Rust vector result contains UUID id, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(rust_ids, vec![nearest, middle, far]);
}

#[test]
fn indexed_small_nonempty_scope_refuses_until_maintenance_then_keeps_a_staged_delta_visible() {
    let db = partitioned_fixture("INDEXED");
    let committed = id(20);
    let farther = id(21);
    insert(&db, committed, 1, "eligible", vec![0.9, 0.1, 0.0]);
    insert(&db, farther, 1, "eligible", vec![0.0, 1.0, 0.0]);

    let indexed_sql = "SELECT id FROM vector_docs WHERE partition_id = $partition \
                       ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1";
    let bound = params([("partition", Value::Int64(1)), ("query", query_vector())]);
    assert_indexed_refusal(
        db.execute(indexed_sql, &bound)
            .expect_err("a nonempty small state cannot hide index loss behind an exact scan"),
    );

    drive_maintenance(&db);
    let ready = db
        .execute(indexed_sql, &bound)
        .expect("caller-driven maintenance makes the small nonempty state indexable");
    assert_eq!(ids(&ready), vec![committed]);
    let bounded_ready = bounded::execute(&db, &bounded_request(indexed_sql, bound.clone()))
        .expect("the bounded door serves the same ready indexed state");
    assert_eq!(bounded_ready.result.rows, ready.rows);

    let staged = id(22);
    db.execute("BEGIN", &empty())
        .expect("open the writing transaction");
    insert(&db, staged, 1, "eligible", vec![1.0, 0.0, 0.0]);
    let in_transaction = db
        .execute(indexed_sql, &bound)
        .expect("INDEXED keeps the maintained committed route and scores only the staged delta");
    assert_eq!(ids(&in_transaction), vec![staged]);
    db.execute("ROLLBACK", &empty())
        .expect("leave the fixture at its committed state");
}

#[test]
fn indexed_broad_filter_refuses_before_scan_and_becomes_eligible_with_a_relational_index() {
    let db = partitioned_fixture("INDEXED");
    insert(&db, id(30), 1, "wanted", vec![1.0, 0.0, 0.0]);
    insert(&db, id(31), 1, "other", vec![0.9, 0.1, 0.0]);
    drive_maintenance(&db);

    let sql = "SELECT id FROM vector_docs WHERE kind = $kind \
               ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1";
    let bound = params([
        ("kind", Value::Text("wanted".to_owned())),
        ("query", query_vector()),
    ]);
    let refusal = db
        .execute(sql, &bound)
        .expect_err("INDEXED broad filtering needs a bounded relational candidate route");
    assert!(
        matches!(&refusal, Error::VectorFilteredRouteUnavailable { .. }),
        "a missing supporting index refuses before a table-wide predicate scan: {refusal:?}"
    );

    db.execute(
        "CREATE INDEX vector_docs_kind_idx ON vector_docs(kind)",
        &empty(),
    )
    .expect("declare the relational index the route needs");
    let eager = db
        .execute(sql, &bound)
        .expect("the same query becomes eligible after the declared supporting index exists");
    assert_eq!(ids(&eager), vec![id(30)]);
    let bounded = bounded::execute(&db, &bounded_request(sql, bound))
        .expect("the bounded reader uses the same eligible filtered route");
    assert_eq!(bounded.result.rows, eager.rows);
}

#[test]
fn old_snapshot_keeps_each_partition_graph_after_a_later_write() {
    let (_root, db) = durable_partitioned_fixture("AUTO");
    for partition in 0..2_i64 {
        for ordinal in 0..1_001_u128 {
            insert(
                &db,
                id(10_000 + partition as u128 * 10_000 + ordinal),
                partition,
                "eligible",
                vec![1.0, ordinal as f32 * 0.002 + partition as f32 * 0.001, 0.0],
            );
        }
    }
    drive_maintenance(&db);
    let generation_statuses = [0_i64, 1_i64].map(|partition| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(
                index(),
                partition_key(partition),
            ))
            .expect("each maintained partition has generation status")
    });
    assert!(
        generation_statuses.iter().all(|status| {
            status.base.or(status.dormant_base).is_some()
                || status.change.or(status.dormant_change).is_some()
        }),
        "both fixtures need a durable generation identity: {generation_statuses:?}"
    );
    let unaffected_rows = partition_row_ids(&db, 0);
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    let bound = params([("query", query_vector())]);

    db.__reset_last_query_vector_trace_for_test();
    let ordinary_before = db
        .execute_at_snapshot(QUERY, &bound, old_snapshot)
        .expect("the ordinary old-snapshot route is initially maintained");
    let ordinary_trace_before = take_trace(&db);
    assert!(ordinary_trace_before.used_hnsw);
    assert_eq!(ordinary_trace_before.selected_generations.len(), 2);
    let unaffected_candidates_before =
        trace_candidates_in(&ordinary_trace_before, &unaffected_rows);
    assert!(!unaffected_candidates_before.is_empty());

    let bounded_before_probe = Arc::new(HnswCandidateCounter::default());
    let mut bounded_before_request = bounded_request(QUERY, bound.clone());
    bounded_before_request.probe =
        Some(Arc::clone(&bounded_before_probe) as Arc<dyn bounded::ExecutionProbe>);
    db.__reset_last_query_vector_trace_for_test();
    let bounded_before = db
        .__with_snapshot_override_for_test(old_snapshot, || {
            bounded::execute(&db, &bounded_before_request)
        })
        .expect("the bounded old-snapshot route is initially maintained");
    let bounded_trace_before = take_trace(&db);
    assert_eq!(bounded_before.result.rows, ordinary_before.rows);
    assert!(bounded_trace_before.used_hnsw);
    let bounded_unaffected_candidates_before =
        trace_candidates_in(&bounded_trace_before, &unaffected_rows);
    assert!(!bounded_unaffected_candidates_before.is_empty());

    db.execute(
        "UPDATE vector_docs SET embedding = $embedding WHERE id = $id",
        &params([
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
            ("id", Value::Uuid(id(20_000))),
        ]),
    )
    .expect("commit a later vector only in the second partition");

    db.__reset_last_query_vector_trace_for_test();
    let ordinary_after = db
        .execute_at_snapshot(QUERY, &bound, old_snapshot)
        .expect("the ordinary reader selects both snapshot-compatible graphs");
    let ordinary_trace_after = take_trace(&db);
    assert_eq!(ordinary_after.rows, ordinary_before.rows);
    assert!(ordinary_trace_after.used_hnsw);
    assert_eq!(ordinary_trace_after.supplemented_row_count, 0);
    assert_eq!(
        ordinary_trace_after.selected_generations, ordinary_trace_before.selected_generations,
        "a later write selects no newer generation for the already-open snapshot"
    );
    assert_eq!(
        trace_candidates_in(&ordinary_trace_after, &unaffected_rows),
        unaffected_candidates_before,
        "later work in one partition does not increase graph visits in its unaffected sibling"
    );

    let bounded_after_probe = Arc::new(HnswCandidateCounter::default());
    let mut bounded_after_request = bounded_request(QUERY, bound);
    bounded_after_request.probe =
        Some(Arc::clone(&bounded_after_probe) as Arc<dyn bounded::ExecutionProbe>);
    db.__reset_last_query_vector_trace_for_test();
    let bounded_after = db
        .__with_snapshot_override_for_test(old_snapshot, || {
            bounded::execute(&db, &bounded_after_request)
        })
        .expect("the bounded reader selects both snapshot-compatible graphs");
    let bounded_trace_after = take_trace(&db);
    assert_eq!(bounded_after.result.rows, bounded_before.result.rows);
    assert!(bounded_trace_after.used_hnsw);
    assert_eq!(bounded_trace_after.supplemented_row_count, 0);
    assert_eq!(
        bounded_trace_after.selected_generations,
        bounded_trace_before.selected_generations
    );
    assert_eq!(
        trace_candidates_in(&bounded_trace_after, &unaffected_rows),
        bounded_unaffected_candidates_before,
        "the bounded reader visits the same unaffected-partition graph candidates"
    );
    assert_eq!(
        bounded_after_probe.0.load(Ordering::SeqCst),
        bounded_before_probe.0.load(Ordering::SeqCst),
        "the bounded old-snapshot route does not add graph work after another partition writes"
    );
    drop(old_pin);
}

#[test]
fn filtered_many_partition_search_reuses_partition_local_candidates() {
    const PARTITIONS: u64 = 8;
    const ROWS_PER_PARTITION: u64 = 64;
    const TOTAL_ROWS: u64 = PARTITIONS * ROWS_PER_PARTITION;

    let db = partitioned_fixture("INDEXED");
    db.execute(
        "CREATE INDEX vector_docs_kind_idx ON vector_docs(kind)",
        &empty(),
    )
    .expect("give the filtered route a bounded candidate source");
    for partition in 0..PARTITIONS {
        for ordinal in 0..ROWS_PER_PARTITION {
            insert(
                &db,
                id(40_000 + partition as u128 * 1_000 + ordinal as u128),
                partition as i64,
                "eligible",
                vec![1.0, ordinal as f32 * 0.001, partition as f32 * 0.0001],
            );
        }
    }
    drive_maintenance(&db);

    let sql = "SELECT id FROM vector_docs WHERE kind = $kind ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3";
    let bound = params([
        ("kind", Value::Text("eligible".to_owned())),
        ("query", query_vector()),
    ]);
    let ordinary = db
        .execute(sql, &bound)
        .expect("ordinary filtered INDEXED search succeeds");
    let probe = Arc::new(HnswCandidateCounter::default());
    let mut request = bounded_request(sql, bound);
    request.probe = Some(Arc::clone(&probe) as Arc<dyn bounded::ExecutionProbe>);
    let bounded =
        bounded::execute(&db, &request).expect("bounded filtered INDEXED search succeeds");
    assert_eq!(bounded.result.rows, ordinary.rows);
    let candidate_touches = probe.0.load(Ordering::SeqCst);
    println!(
        "partition_local_candidates rows={TOTAL_ROWS} partitions={PARTITIONS} touches={candidate_touches}"
    );
    assert!(
        candidate_touches < 4 * TOTAL_ROWS,
        "candidate work must be partition-local, not repeated global scans: \
         rows={TOTAL_ROWS} partitions={PARTITIONS} touches={candidate_touches}"
    );
}

#[test]
fn auto_keeps_healthy_partition_results_for_preflight_and_mid_merge_fallbacks() {
    let db = partitioned_fixture("AUTO");
    db.execute(
        "CREATE INDEX vector_docs_kind_idx ON vector_docs(kind)",
        &empty(),
    )
    .expect("give the filtered mixed route its bounded candidate source");
    for partition in 0..2_i64 {
        for ordinal in 0..500_u128 {
            insert(
                &db,
                id(20_000 + partition as u128 * 1_000 + ordinal),
                partition,
                "eligible",
                vec![1.0, ordinal as f32 * 0.002 + partition as f32 * 0.001, 0.0],
            );
        }
    }
    drive_maintenance(&db);
    let first_rows = partition_row_ids(&db, 0);
    let second_rows = partition_row_ids(&db, 1);
    insert(&db, id(22_000), 2, "eligible", vec![1.0, 0.0, 0.0]);
    insert(&db, id(22_001), 2, "excluded", vec![1.0, 0.0, 0.0]);
    let third_rows = partition_row_ids(&db, 2);
    let auto_sql = "SELECT id FROM vector_docs WHERE kind = $kind \
                    ORDER BY embedding <=> $query LIMIT 3";
    let auto_bound = params([
        ("kind", Value::Text("eligible".to_owned())),
        ("query", query_vector()),
    ]);
    let exact_sql = "SELECT id FROM vector_docs WHERE kind = $kind \
                     ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 3";
    let indexed_sql = "SELECT id FROM vector_docs WHERE kind = $kind \
                       ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3";
    let exact = db
        .execute(exact_sql, &auto_bound)
        .expect("the exact control defines global membership and ordering");

    db.__reset_last_query_vector_trace_for_test();
    let ordinary_preflight = db
        .execute(auto_sql, &auto_bound)
        .expect("AUTO exact-compares only the not-yet-built partition");
    let ordinary_preflight_trace = take_trace(&db);
    assert_eq!(ordinary_preflight.rows, exact.rows);
    assert!(ordinary_preflight_trace.used_hnsw);
    assert_eq!(ordinary_preflight_trace.supplemented_row_count, 1);
    assert!(
        !trace_candidates_in(&ordinary_preflight_trace, &first_rows).is_empty()
            && !trace_candidates_in(&ordinary_preflight_trace, &second_rows).is_empty(),
        "both healthy partition graph results survive the unavailable sibling: {ordinary_preflight_trace:?}"
    );
    assert!(
        trace_candidates_in(&ordinary_preflight_trace, &third_rows).is_empty(),
        "the unavailable partition contributes exact rows, not graph rows: trace={ordinary_preflight_trace:?}, rows={third_rows:?}"
    );
    assert_indexed_refusal(
        db.execute(indexed_sql, &auto_bound)
            .expect_err("INDEXED refuses the preflight-unavailable partition without rows"),
    );

    db.__reset_last_query_vector_trace_for_test();
    let bounded_preflight = bounded::execute(&db, &bounded_request(auto_sql, auto_bound.clone()))
        .expect("bounded AUTO charges only the unavailable partition's exact work");
    let bounded_preflight_trace = take_trace(&db);
    assert_eq!(bounded_preflight.result.rows, exact.rows);
    assert!(bounded_preflight_trace.used_hnsw);
    assert_eq!(bounded_preflight_trace.supplemented_row_count, 1);
    assert_bounded_indexed_refusal(bounded::execute(
        &db,
        &bounded_request(indexed_sql, auto_bound.clone()),
    ));

    drive_maintenance(&db);
    db.__reset_last_query_vector_trace_for_test();
    let ready = bounded::execute(&db, &bounded_request(auto_sql, auto_bound.clone()))
        .expect("all three partitions have complete maintained routes");
    let ready_trace = take_trace(&db);
    assert_eq!(ready.result.rows, exact.rows);
    assert_eq!(ready_trace.supplemented_row_count, 0);
    assert!(!trace_candidates_in(&ready_trace, &third_rows).is_empty());
    let _shortfall = db
        .vector_store_for_test()
        .cap_partition_graph_candidates_for_test(&index(), &partition_key(1), 0);

    db.__reset_last_query_vector_trace_for_test();
    let ordinary_shortfall = db
        .execute(auto_sql, &auto_bound)
        .expect("AUTO retains earlier graph results when the last graph falls short");
    let ordinary_shortfall_trace = take_trace(&db);
    assert_eq!(ordinary_shortfall.rows, exact.rows);
    assert!(ordinary_shortfall_trace.used_hnsw);
    assert_eq!(ordinary_shortfall_trace.supplemented_row_count, 500);
    assert!(
        !trace_candidates_in(&ordinary_shortfall_trace, &first_rows).is_empty()
            && !trace_candidates_in(&ordinary_shortfall_trace, &third_rows).is_empty()
    );
    assert!(trace_candidates_in(&ordinary_shortfall_trace, &second_rows).is_empty());
    assert_indexed_refusal(
        db.execute(indexed_sql, &auto_bound)
            .expect_err("INDEXED publishes no rows after a mid-merge shortfall"),
    );

    db.__reset_last_query_vector_trace_for_test();
    let bounded_shortfall = bounded::execute(&db, &bounded_request(auto_sql, auto_bound.clone()))
        .expect("bounded AUTO merges healthy graph and one-partition exact candidates");
    let bounded_shortfall_trace = take_trace(&db);
    assert_eq!(bounded_shortfall.result.rows, exact.rows);
    assert!(bounded_shortfall_trace.used_hnsw);
    assert_eq!(bounded_shortfall_trace.supplemented_row_count, 500);
    assert_bounded_indexed_refusal(bounded::execute(
        &db,
        &bounded_request(indexed_sql, auto_bound.clone()),
    ));

    let mut short_limits = roomy_limits();
    short_limits.work = bounded_shortfall.telemetry.work_units - 1;
    let refused = bounded::execute(
        &db,
        &bounded::BoundedReadRequest::new(
            auto_sql,
            auto_bound,
            short_limits,
            Arc::new(FrozenClock),
        ),
    );
    assert!(
        matches!(
            &refused,
            Err(bounded::TestError::Refused(refusal))
                if matches!(
                    refusal.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::Work
                )
        ),
        "aggregate exact fallback must refuse rather than publish partial graph rows: {refused:?}"
    );
}

#[test]
fn exact_memory_admission_uses_selected_authorized_rows_as_unrelated_vectors_grow() {
    use contextdb_core::ContextId;
    use contextdb_engine::memory_accounting::MemoryAccountant;
    use std::collections::BTreeSet;
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE scoped_scores (id UUID PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, bucket INTEGER NOT NULL, eligible BOOL NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) SEARCH_MODE EXACT, flat_embedding VECTOR(3) SEARCH_MODE EXACT)", &empty()).unwrap();
    db.execute(
        "CREATE INDEX eligible_scores ON scoped_scores(eligible)",
        &empty(),
    )
    .unwrap();
    let context = ContextId::new(id(30_000));
    let scoped = db.scoped_with_contexts(BTreeSet::from([context.clone()]));
    let mut inserted = 0;
    let mut selected_work = HashMap::new();
    for population in [64, 4096] {
        accountant.set_budget(None).unwrap();
        let tx = db.begin_or_panic();
        for ordinal in inserted..population {
            db.execute_in_tx(
                tx,
                "INSERT INTO scoped_scores VALUES ($id, $context, $bucket, $eligible, $vector, $vector)",
                &params([
                    ("id", Value::Uuid(id(ordinal))),
                    (
                        "context",
                        Value::Uuid(if ordinal == 0 { context.0 } else { id(30_001) }),
                    ),
                    ("bucket", Value::Int64(if ordinal < 2 { 1 } else { 2 })),
                    ("eligible", Value::Bool(ordinal < 2)),
                    (
                        "vector",
                        Value::Vector(if ordinal == 0 {
                            vec![0.8, 0.6, 0.0]
                        } else {
                            vec![1.0, 0.0, 0.0]
                        }),
                    ),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        inserted = population;
        let settled = accountant.usage().used;
        accountant.set_budget(Some(settled + 32 * 1024)).unwrap();
        for (column, predicate) in [
            ("embedding", "bucket = 1"),
            ("embedding", "eligible = TRUE"),
            ("flat_embedding", "eligible = TRUE"),
        ] {
            let sql = format!(
                "SELECT id, score FROM scoped_scores WHERE {predicate} ORDER BY {column} <=> $query USE VECTOR EXACT LIMIT 1"
            );
            scoped.__reset_relational_scan_rows_touched();
            let ordinary = scoped
                .execute(&sql, &params([("query", query_vector())]))
                .unwrap();
            assert_eq!(ids(&ordinary), vec![id(0)]);
            let bounded = bounded::execute(
                &scoped,
                &bounded_request(&sql, params([("query", query_vector())])),
            )
            .unwrap();
            assert_eq!(bounded.result.rows, ordinary.rows);
            if let Some(previous) =
                selected_work.insert((column, predicate), bounded.telemetry.work_units)
            {
                assert_eq!(
                    bounded.telemetry.work_units, previous,
                    "unrelated vectors cannot add work to a selected authorized EXACT query"
                );
            }
            let mut query = SemanticQuery::new("scoped_scores", column, vec![1.0, 0.0, 0.0], 1);
            query.where_clause = Some(predicate.to_owned());
            query.search_mode = Some(VectorSearchMode::Exact);
            let rust = scoped.semantic_search(query).unwrap();
            assert_eq!(rust.len(), 1);
            assert_eq!(rust[0].values.get("id"), Some(&Value::Uuid(id(0))));
            assert!((rust[0].vector_score - 0.8).abs() < 1e-6);
            assert_eq!(
                scoped.__relational_scan_rows_touched(),
                0,
                "partition and indexed candidates must never scan unrelated rows"
            );
            assert_eq!(
                accountant.usage().used,
                settled,
                "temporary charges must return after every reader finishes"
            );
        }
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
}

#[test]
fn concurrent_direct_exact_scores_hold_their_charge_through_result_consumption() {
    use contextdb_engine::memory_accounting::MemoryAccountant;
    use contextdb_vector::mem::{ExactScorePhaseForTest, with_exact_score_observer_for_test};
    use std::sync::{Mutex, mpsc};
    use std::time::Duration;
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Arc::new(Database::open_memory_with_accountant(accountant.clone()));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE score_owners (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE EXACT)",
        &empty(),
    )
    .unwrap();
    let tx = db.begin_or_panic();
    for ordinal in 0..64 {
        db.execute_in_tx(
            tx,
            "INSERT INTO score_owners VALUES ($id, $vector)",
            &params([("id", Value::Int64(ordinal)), ("vector", query_vector())]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let settled = accountant.usage().used;
    let bytes = std::mem::size_of::<(RowId, f32)>();
    accountant
        .set_budget(Some(settled + bytes + bytes / 2))
        .unwrap();
    let (arrived, arrivals) = mpsc::channel();
    let (release, releases) = mpsc::channel();
    let releases = Mutex::new(releases);
    let observer_accountant = accountant.clone();
    let reader = db.clone();
    let task = std::thread::spawn(move || {
        with_exact_score_observer_for_test(
            Arc::new(move |phase, capacity, len| {
                arrived
                    .send((phase, capacity, len, observer_accountant.usage().used))
                    .unwrap();
                releases
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }),
            || {
                reader.semantic_search(SemanticQuery::new(
                    "score_owners",
                    "embedding",
                    vec![1.0, 0.0, 0.0],
                    1,
                ))
            },
        )
    });
    for expected_phase in [
        ExactScorePhaseForTest::Scored,
        ExactScorePhaseForTest::Consuming,
    ] {
        let (phase, capacity, len, used) = arrivals.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(phase, expected_phase);
        assert_eq!(
            capacity, 1,
            "the bounded top-one heap owns one score allocation"
        );
        assert_eq!(len, 1);
        assert_eq!(
            used,
            settled + bytes,
            "the actual score allocation remains owned at both boundaries"
        );
        let error = db
            .semantic_search(SemanticQuery::new(
                "score_owners",
                "embedding",
                vec![1.0, 0.0, 0.0],
                1,
            ))
            .unwrap_err();
        assert!(
            matches!(error, Error::VectorExactSearchBudgetExceeded { required_bytes, available_bytes, .. }
            if required_bytes == bytes as u64 && available_bytes == (bytes / 2) as u64),
            "the concurrent direct read must not spend the first read's live charge: {error:?}"
        );
        assert_eq!(accountant.usage().used, settled + bytes);
        release.send(()).unwrap();
    }
    assert_eq!(task.join().unwrap().unwrap().len(), 1);
    assert_eq!(accountant.usage().used, settled);
    assert_eq!(accountant.underflow_count_for_test(), 0);
    assert_eq!(
        db.semantic_search(SemanticQuery::new(
            "score_owners",
            "embedding",
            vec![1.0, 0.0, 0.0],
            1
        ))
        .unwrap()
        .len(),
        1,
        "the released credit must admit the next direct call"
    );
    assert_eq!(accountant.usage().used, settled);
}
