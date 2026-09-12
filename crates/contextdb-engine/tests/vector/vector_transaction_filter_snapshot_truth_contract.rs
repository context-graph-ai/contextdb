//! Transaction, filter, and retained-generation truth for maintained vectors.
//!
//! These are deliberately production-door journeys.  `execute_in_tx`, the
//! existing transaction-aware semantic kernel, and `ReadSession` are the
//! production paths a consumer reaches; generation status, the query trace, and bounded
//! source touches only observe those calls.  No test builds a graph, changes a
//! route, or makes progress through an observation seam.
//!
//! `Database::__semantic_search_in_tx_for_test` and the full-index-enumeration
//! counter are test-only observations of the transaction-aware semantic
//! kernel and the `entries_for_index` slow path, not user-facing APIs.
//! Public SQL reaches the kernel with a transaction, while `semantic_search`
//! deliberately supplies none.
//!
//! The existing global "last vector trace" is deliberately *not* proof here:
//! it can describe a prior SQL call. `__reset_last_query_vector_trace_for_test`
//! plus `__take_last_query_vector_trace_for_test` are the one additional
//! narrow, test-only observation wall this draft needs. They reset/take only
//! debug receipt state around one already-public call; they neither choose a
//! route nor mutate data, graphs, snapshots, or maintenance state.
//! `VectorSearchDebugTrace::selected_generations` binds retained-generation
//! evidence to that same completed call rather than consulting an independent
//! selector after the fact.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use contextdb_vector::VectorSearchDebugTrace;
use contextdb_vector::store::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const F32_INDEX_THRESHOLD: usize = 1_000;
const MAINTENANCE_CYCLES: usize = 64;
const VECTOR_REFUSAL_HEADROOM_BYTES: usize = 8 * 1024;
const SELECTED_SCOPE: u128 = 0xA110_0000_0000_0000_0000_0000_0000_0001;
const OTHER_SCOPE: u128 = 0xA110_0000_0000_0000_0000_0000_0000_0002;

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

#[derive(Default)]
struct VectorTouches {
    table_rows: AtomicU64,
    index_entries: AtomicU64,
    sort_candidates: AtomicU64,
    graph_edges: AtomicU64,
    adjacency_entries: AtomicU64,
    brute_force_vectors: AtomicU64,
    hnsw_vectors: AtomicU64,
    rank_candidates: AtomicU64,
    access_rows: AtomicU64,
}

impl bounded::ExecutionProbe for VectorTouches {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _completed_items: u64) {
        match touch {
            bounded::TestSourceTouch::TableRow => {
                self.table_rows.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::IndexEntry => {
                self.index_entries.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::SortCandidate => {
                self.sort_candidates.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::GraphEdge => {
                self.graph_edges.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::AdjacencyEntry => {
                self.adjacency_entries.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::BruteForceVectorCandidate => {
                self.brute_force_vectors.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::HnswCandidate => {
                self.hnsw_vectors.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::RankCandidate => {
                self.rank_candidates.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::AccessRow => {
                self.access_rows.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

#[derive(Debug, PartialEq, Eq)]
struct VectorTouchSnapshot {
    table_rows: u64,
    index_entries: u64,
    sort_candidates: u64,
    graph_edges: u64,
    adjacency_entries: u64,
    brute_force_vectors: u64,
    hnsw_vectors: u64,
    rank_candidates: u64,
    access_rows: u64,
}

fn vector_touches(counter: &VectorTouches) -> VectorTouchSnapshot {
    VectorTouchSnapshot {
        table_rows: counter.table_rows.load(Ordering::SeqCst),
        index_entries: counter.index_entries.load(Ordering::SeqCst),
        sort_candidates: counter.sort_candidates.load(Ordering::SeqCst),
        graph_edges: counter.graph_edges.load(Ordering::SeqCst),
        adjacency_entries: counter.adjacency_entries.load(Ordering::SeqCst),
        brute_force_vectors: counter.brute_force_vectors.load(Ordering::SeqCst),
        hnsw_vectors: counter.hnsw_vectors.load(Ordering::SeqCst),
        rank_candidates: counter.rank_candidates.load(Ordering::SeqCst),
        access_rows: counter.access_rows.load(Ordering::SeqCst),
    }
}

/// These are every direct-read source counter this draft can observe without
/// adding a route hook: relational table rows, relational index entries, and
/// vector full-index enumeration. Reset before each public refusal so a prior
/// door cannot hide scan-then-refuse work in the next one.
fn reset_refusal_source_touches(db: &Database) {
    db.__reset_relational_scan_rows_touched();
    db.__reset_relational_index_entries_touched();
    db.__reset_vector_full_index_entries_touched_for_test();
}

fn assert_no_refusal_source_touches(db: &Database, what: &str) {
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "{what}: refusal cannot scan table rows before it returns"
    );
    assert_eq!(
        db.__relational_index_entries_touched(),
        0,
        "{what}: refusal cannot walk relational index entries before it returns"
    );
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "{what}: refusal cannot enumerate vector entries before it returns"
    );
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
        result_rows: 2_048,
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
    Uuid::from_u128(0xA111_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn selected_scope() -> Uuid {
    Uuid::from_u128(SELECTED_SCOPE)
}

fn other_scope() -> Uuid {
    Uuid::from_u128(OTHER_SCOPE)
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("vector_truth", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("a UUID partition key has a canonical public identity")
}

fn partition_ref(scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(index(), partition(scope))
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let column = result
        .columns
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("the vector projection includes its UUID id");
    result
        .rows
        .iter()
        .map(|row| match row.get(column) {
            Some(Value::Uuid(id)) => *id,
            other => panic!("vector result id is UUID, got {other:?}"),
        })
        .collect()
}

fn semantic_ids(rows: Vec<contextdb_engine::SearchResult>) -> Vec<Uuid> {
    rows.into_iter()
        .map(|row| match row.values.get("id") {
            Some(Value::Uuid(id)) => *id,
            other => panic!("Rust semantic result id is UUID, got {other:?}"),
        })
        .collect()
}

fn create_vector_truth(db: &Database, mode: &str) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE vector_truth (\
                id UUID PRIMARY KEY, \
                scope_id UUID NOT NULL, \
                kind TEXT NOT NULL, \
                embedding VECTOR(3) PARTITION_KEY (scope_id) \
                    MAX_PARTITIONS 8 SEARCH_MODE {mode}\
             )"
        ),
        &empty(),
    )
    .expect("declare the maintained F32 fixture");
}

fn insert_in_tx(
    db: &Database,
    tx: contextdb_core::TxId,
    row_id: Uuid,
    scope: Uuid,
    kind: &str,
    vector: Vec<f32>,
) {
    db.insert_row(
        tx,
        "vector_truth",
        params([
            ("id", Value::Uuid(row_id)),
            ("scope_id", Value::Uuid(scope)),
            ("kind", Value::Text(kind.to_owned())),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("stage one deterministic vector row");
}

fn seed_indexed_partition(db: &Database, scope: Uuid, first: u128, kind: &str) {
    let tx = db
        .begin()
        .expect("begin the deterministic seed transaction");
    for ordinal in 0..=F32_INDEX_THRESHOLD {
        let score = 1.0 - ordinal as f32 * 0.0001;
        insert_in_tx(
            db,
            tx,
            id(first + ordinal as u128),
            scope,
            kind,
            vec![score, (1.0 - score * score).max(0.0).sqrt(), 0.0],
        );
    }
    db.commit(tx).expect("commit the indexed seed partition");
    drive_until_indexed(db, scope);
}

fn drive_until_indexed(db: &Database, scope: Uuid) {
    for _ in 0..MAINTENANCE_CYCLES {
        if db
            .vector_store_for_test()
            .partition_info(&index(), &partition(scope))
            .is_some_and(|info| info.graph_available)
        {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance batch returns");
    }
    panic!("the deterministic maintained partition never became INDEXED-ready");
}

fn generation(db: &Database, scope: Uuid) -> contextdb_vector::store::VectorGraphGeneration {
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition_ref(scope))
        .expect("the selected partition exists");
    status
        .change
        .or(status.base)
        .or(status.dormant_change)
        .or(status.dormant_base)
        .expect("the maintained partition has a published generation")
}

/// Start one trace receipt for exactly the call that follows. The test-only
/// reset is required because a process-global last trace could otherwise let a
/// Rust or snapshot call inherit an earlier SQL route.
fn reset_hnsw_trace(db: &Database) {
    db.__reset_last_query_vector_trace_for_test();
}

fn assert_hnsw_trace(db: &Database, what: &str) -> VectorSearchDebugTrace {
    let trace = db
        .__take_last_query_vector_trace_for_test()
        .unwrap_or_else(|| panic!("{what}: the public vector query published no debug trace"));
    assert!(
        trace.used_hnsw,
        "{what}: the selected maintained route must use the committed graph, trace={trace:?}"
    );
    assert!(
        trace.hnsw_candidate_count > 0,
        "{what}: the maintained graph must score committed candidates, trace={trace:?}"
    );
    assert_eq!(
        trace.fallback_reason, None,
        "{what}: the maintained graph route must not hide a fallback, trace={trace:?}"
    );
    trace
}

fn assert_exact_trace(db: &Database, what: &str) {
    let trace = db
        .__take_last_query_vector_trace_for_test()
        .unwrap_or_else(|| panic!("{what}: the public vector query published no debug trace"));
    assert!(
        !trace.used_hnsw && trace.hnsw_candidate_count == 0,
        "{what}: AUTO must use exact candidate work without a hidden HNSW route, trace={trace:?}"
    );
}

/// This call is the deliberate transaction-entrance compile wall described in
/// the module documentation. It observes the existing transaction-aware semantic kernel
/// without choosing a new public Rust transaction interface.
fn semantic_in_tx(
    db: &Database,
    tx: contextdb_core::TxId,
    query: SemanticQuery,
) -> contextdb_core::Result<Vec<contextdb_engine::SearchResult>> {
    db.__semantic_search_in_tx_for_test(tx, query)
}

#[test]
fn indexed_transaction_overlay_keeps_the_committed_graph_and_scores_only_staged_delta() {
    let db = Database::open_memory();
    create_vector_truth(&db, "INDEXED");
    seed_indexed_partition(&db, selected_scope(), 10_000, "kept");

    let tx = db.begin().expect("open the writing transaction");
    let replaced = id(10_000);
    let deleted = id(10_001);
    let staged_nearest = id(99_001);
    db.execute_in_tx(
        tx,
        "UPDATE vector_truth SET embedding = $replacement WHERE id = $id",
        &params([
            ("replacement", Value::Vector(vec![0.0, 1.0, 0.0])),
            ("id", Value::Uuid(replaced)),
        ]),
    )
    .expect("stage a replacement away from the query");
    db.execute_in_tx(
        tx,
        "DELETE FROM vector_truth WHERE id = $id",
        &params([("id", Value::Uuid(deleted))]),
    )
    .expect("stage a committed-vector deletion");
    insert_in_tx(
        &db,
        tx,
        staged_nearest,
        selected_scope(),
        "kept",
        vec![1.0, 0.0, 0.0],
    );

    let sql = "SELECT id FROM vector_truth WHERE scope_id = $scope \
               ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 4";
    let bound = params([
        ("scope", Value::Uuid(selected_scope())),
        ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    db.__reset_vector_full_index_entries_touched_for_test();
    reset_hnsw_trace(&db);
    let sql_rows = db
        .execute_in_tx(tx, sql, &bound)
        .expect("SQL INDEXED sees its staged vector delta");
    assert!(
        ids(&sql_rows).contains(&staged_nearest),
        "the staged nearest vector belongs in the transaction's answer"
    );
    assert!(!ids(&sql_rows).contains(&deleted));
    assert!(!ids(&sql_rows).contains(&replaced));
    assert_hnsw_trace(&db, "SQL transaction overlay");
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "SQL transaction search cannot enumerate every committed vector behind a truthful HNSW trace"
    );

    let mut semantic = SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 4);
    semantic.search_mode = Some(VectorSearchMode::Indexed);
    db.__reset_vector_full_index_entries_touched_for_test();
    reset_hnsw_trace(&db);
    let rust_rows = semantic_ids(
        semantic_in_tx(&db, tx, semantic).expect("Rust INDEXED sees the same staged vector delta"),
    );
    assert!(rust_rows.contains(&staged_nearest));
    assert!(!rust_rows.contains(&deleted));
    assert!(!rust_rows.contains(&replaced));
    assert_hnsw_trace(&db, "Rust transaction overlay");
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "Rust transaction search cannot enumerate every committed vector behind a truthful HNSW trace"
    );
    db.rollback(tx)
        .expect("discard the explicit transaction after both public transaction doors");

    // The bounded kernel's source touches are the existing work counter.  Its
    // separate observation transaction has one staged insert, so at most that
    // one row may reach the exact source; none of the 1,001 committed vectors
    // may do so.
    db.execute("BEGIN", &empty())
        .expect("open the bounded transaction observation");
    db.execute(
        "UPDATE vector_truth SET embedding = $replacement WHERE id = $id",
        &params([
            ("replacement", Value::Vector(vec![0.0, 1.0, 0.0])),
            ("id", Value::Uuid(replaced)),
        ]),
    )
    .expect("stage the bounded replacement away from the query");
    db.execute(
        "DELETE FROM vector_truth WHERE id = $id",
        &params([("id", Value::Uuid(deleted))]),
    )
    .expect("stage the bounded committed-vector deletion");
    let bounded_staged_nearest = id(99_002);
    db.execute(
        "INSERT INTO vector_truth (id, scope_id, kind, embedding) \
         VALUES ($id, $scope, 'kept', $embedding)",
        &params([
            ("id", Value::Uuid(bounded_staged_nearest)),
            ("scope", Value::Uuid(selected_scope())),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("stage the bounded observation delta");
    db.__reset_vector_full_index_entries_touched_for_test();
    let counter = Arc::new(VectorTouches::default());
    let mut request = bounded_request(sql, bound);
    request.probe = Some(Arc::clone(&counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_hnsw_trace(&db);
    let bounded_rows = bounded::execute(&db, &request)
        .expect("the bounded production kernel serves the transaction overlay");
    assert_hnsw_trace(&db, "bounded transaction overlay");
    db.execute("ROLLBACK", &empty())
        .expect("discard the bounded observation transaction");
    assert!(ids(&bounded_rows.result).contains(&bounded_staged_nearest));
    assert!(!ids(&bounded_rows.result).contains(&replaced));
    assert!(!ids(&bounded_rows.result).contains(&deleted));
    assert!(
        counter.hnsw_vectors.load(Ordering::SeqCst) > 0,
        "the committed side of a transaction overlay remains graph-driven"
    );
    assert!(
        counter.brute_force_vectors.load(Ordering::SeqCst) <= 2,
        "the staged delta must not make the bounded read exact-scan the 1,001 committed vectors"
    );
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "the bounded transaction route cannot enumerate the committed index"
    );
}

#[test]
fn equal_scores_have_one_row_id_order_for_committed_and_staged_ranked_and_unranked_rows() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE order_scores (id UUID PRIMARY KEY, item_id UUID, weight REAL)",
        &empty(),
    )
    .expect("create rank policy companion rows");
    db.execute(
        "CREATE INDEX order_scores_item_idx ON order_scores(item_id)",
        &empty(),
    )
    .expect("index rank policy companion rows");
    db.execute(
        "CREATE TABLE order_truth (\
            id UUID PRIMARY KEY, \
            embedding VECTOR(3) RANK_POLICY (\
                JOIN order_scores ON item_id, \
                FORMULA '{vector_score} * coalesce({weight}, 1.0)', \
                SORT_KEY weighted\
            )\
         )",
        &empty(),
    )
    .expect("create the ranked vector table");

    // Row ids follow insertion order, while these UUIDs deliberately run in
    // the opposite visual order.  The expected answer therefore catches a
    // comparator that reverses row ids rather than comparing UUID rendering.
    let committed = Uuid::from_u128(0xFFFF);
    let staged = Uuid::from_u128(0x0001);
    db.execute(
        "INSERT INTO order_truth (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Uuid(committed)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("commit the first equal-score row");
    db.execute(
        "INSERT INTO order_scores (id, item_id, weight) VALUES ($id, $item, 1.0)",
        &params([
            ("id", Value::Uuid(id(200_001))),
            ("item", Value::Uuid(committed)),
        ]),
    )
    .expect("commit its rank-policy value");
    let order_index = VectorIndexRef::new("order_truth", "embedding");
    for _ in 0..MAINTENANCE_CYCLES {
        if db
            .vector_store_for_test()
            .partition_info(&order_index, &VectorPartitionKey::unpartitioned())
            .is_some_and(|info| info.graph_available)
        {
            break;
        }
        db.run_maintenance_cycle()
            .expect("publish the committed tie route in one finite batch");
    }
    assert!(
        db.vector_store_for_test()
            .partition_info(&order_index, &VectorPartitionKey::unpartitioned())
            .is_some_and(|info| info.graph_available),
        "the tie fixture has a maintained committed route before staging its delta"
    );

    let tx = db.begin().expect("open the equal-score transaction");
    db.insert_row(
        tx,
        "order_truth",
        params([
            ("id", Value::Uuid(staged)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("stage the second equal-score vector");
    db.insert_row(
        tx,
        "order_scores",
        params([
            ("id", Value::Uuid(id(200_002))),
            ("item_id", Value::Uuid(staged)),
            ("weight", Value::Float64(1.0)),
        ]),
    )
    .expect("stage its equal rank-policy value");

    for sql in [
        "SELECT id FROM order_truth ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 2",
        "SELECT id FROM order_truth ORDER BY embedding <=> $query USE VECTOR INDEXED \
         USE RANK weighted LIMIT 2",
    ] {
        reset_hnsw_trace(&db);
        let result = db
            .execute_in_tx(
                tx,
                sql,
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("equal-score vector query returns an ordered answer");
        assert_eq!(
            ids(&result),
            vec![committed, staged],
            "{sql}: score-descending ties always use ascending internal row id"
        );
        assert_hnsw_trace(&db, "transaction equal-score route");
    }

    for sort_key in [None, Some("weighted")] {
        let mut semantic = SemanticQuery::new("order_truth", "embedding", vec![1.0, 0.0, 0.0], 2);
        semantic.search_mode = Some(VectorSearchMode::Indexed);
        semantic.sort_key = sort_key.map(str::to_owned);
        reset_hnsw_trace(&db);
        assert_eq!(
            semantic_ids(
                semantic_in_tx(&db, tx, semantic)
                    .expect("Rust semantic equal-score query returns an ordered answer"),
            ),
            vec![committed, staged],
            "Rust SemanticQuery ranked and unranked ties use ascending internal row id"
        );
        assert_hnsw_trace(&db, "Rust semantic transaction equal-score route");
    }

    db.commit(tx)
        .expect("commit the equal-score rows for the bounded production door");
    for sql in [
        "SELECT id FROM order_truth ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 2",
        "SELECT id FROM order_truth ORDER BY embedding <=> $query USE VECTOR INDEXED \
         USE RANK weighted LIMIT 2",
    ] {
        let request = bounded_request(sql, params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]));
        reset_hnsw_trace(&db);
        let result = bounded::execute(&db, &request)
            .unwrap_or_else(|error| panic!("bounded equal-score query {sql:?}: {error:?}"));
        assert_eq!(
            ids(&result.result),
            vec![committed, staged],
            "{sql}: bounded ranked and unranked ties use the same ascending internal row id"
        );
        assert_hnsw_trace(&db, "bounded transaction equal-score route");
    }
}

#[test]
fn rust_semantic_filter_merges_staged_matches_and_removals_before_vector_scoring() {
    let db = Database::open_memory();
    create_vector_truth(&db, "INDEXED");
    seed_indexed_partition(&db, selected_scope(), 30_000, "wanted");
    db.execute(
        "CREATE INDEX vector_truth_kind_idx ON vector_truth(kind)",
        &empty(),
    )
    .expect("declare the bounded committed filter source");

    let deleted_match = id(30_000);
    let staged_match = id(39_001);
    let staged_nonmatch = id(39_002);
    let tx = db.begin().expect("open the filter transaction");
    db.execute_in_tx(
        tx,
        "DELETE FROM vector_truth WHERE id = $id",
        &params([("id", Value::Uuid(deleted_match))]),
    )
    .expect("stage deletion of a committed matching row");
    insert_in_tx(
        &db,
        tx,
        staged_match,
        selected_scope(),
        "wanted",
        vec![0.0, 0.0, 1.0],
    );
    insert_in_tx(
        &db,
        tx,
        staged_nonmatch,
        selected_scope(),
        "excluded",
        vec![0.0, 0.0, 1.0],
    );

    let mut query = SemanticQuery::new("vector_truth", "embedding", vec![0.0, 0.0, 1.0], 16);
    query.search_mode = Some(VectorSearchMode::Indexed);
    query.where_clause = Some("kind = 'wanted'".to_owned());
    db.__reset_relational_scan_rows_touched();
    db.__reset_relational_index_entries_touched();
    db.__reset_vector_full_index_entries_touched_for_test();
    reset_hnsw_trace(&db);
    let rows = semantic_ids(
        semantic_in_tx(&db, tx, query).expect("Rust semantic filtering sees its transaction image"),
    );
    assert!(rows.contains(&staged_match));
    assert!(!rows.contains(&staged_nonmatch));
    assert!(!rows.contains(&deleted_match));
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "the transaction filter uses its committed index plus staged overlay, not a table scan"
    );
    let committed_filter_touches = db.__relational_index_entries_touched();
    assert!(
        committed_filter_touches > 0
            && committed_filter_touches <= (F32_INDEX_THRESHOLD + 1) as u64,
        "the committed filter source touches only its bounded postings"
    );
    assert_hnsw_trace(&db, "Rust semantic filter transaction overlay");
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "the Rust transaction filter cannot enumerate the full committed vector index"
    );
    db.rollback(tx)
        .expect("discard the semantic filter transaction after observing its image");
}

#[test]
fn filters_choose_only_allowed_work_and_name_the_same_budget_refusal_on_every_door() {
    let db = Database::open_memory();
    create_vector_truth(&db, "AUTO");
    seed_indexed_partition(&db, selected_scope(), 40_000, "other");
    let wanted = id(49_001);
    let tx = db.begin().expect("open the narrow matching insert");
    insert_in_tx(
        &db,
        tx,
        wanted,
        selected_scope(),
        "wanted",
        vec![1.0, 0.0, 0.0],
    );
    db.commit(tx).expect("commit the one narrow allowed row");
    db.execute(
        "CREATE INDEX vector_truth_kind_idx ON vector_truth(kind)",
        &empty(),
    )
    .expect("declare the supporting relational candidate index");

    let narrow_sql = "SELECT id FROM vector_truth WHERE kind = $kind \
                      ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 1";
    let narrow = params([
        ("kind", Value::Text("wanted".to_owned())),
        ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    db.__reset_relational_scan_rows_touched();
    db.__reset_relational_index_entries_touched();
    db.__reset_vector_full_index_entries_touched_for_test();
    reset_hnsw_trace(&db);
    let sql_narrow = db
        .execute(narrow_sql, &narrow)
        .expect("SQL narrow AUTO search");
    assert_eq!(ids(&sql_narrow), vec![wanted]);
    assert!(
        sql_narrow.trace.rows_examined <= 4,
        "ordinary SQL must not evaluate the narrow predicate across the 1,002-row table: {:?}",
        sql_narrow.trace
    );
    assert_exact_trace(&db, "SQL narrow AUTO route");
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "ordinary SQL uses the complete supporting index rather than scanning the table"
    );
    assert_eq!(
        db.__relational_index_entries_touched(),
        1,
        "ordinary SQL reads only the one allowed equality posting"
    );
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "ordinary SQL resolves the one allowed vector directly instead of enumerating the column"
    );
    let mut semantic = SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 1);
    semantic.search_mode = Some(VectorSearchMode::Auto);
    semantic.where_clause = Some("kind = 'wanted'".to_owned());
    db.__reset_relational_scan_rows_touched();
    db.__reset_relational_index_entries_touched();
    db.__reset_vector_full_index_entries_touched_for_test();
    reset_hnsw_trace(&db);
    assert_eq!(
        semantic_ids(
            db.semantic_search(semantic)
                .expect("Rust narrow AUTO search")
        ),
        vec![wanted]
    );
    assert_exact_trace(&db, "Rust narrow AUTO route");
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "Rust SemanticQuery must not scan the table after a complete supporting index is chosen"
    );
    assert_eq!(
        db.__relational_index_entries_touched(),
        1,
        "Rust SemanticQuery reads only the one allowed posting for this equality"
    );
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "Rust SemanticQuery resolves the one allowed vector directly instead of enumerating the column"
    );
    let narrow_counter = Arc::new(VectorTouches::default());
    let mut narrow_request = bounded_request(narrow_sql, narrow.clone());
    narrow_request.probe = Some(Arc::clone(&narrow_counter) as Arc<dyn bounded::ExecutionProbe>);
    db.__reset_vector_full_index_entries_touched_for_test();
    reset_hnsw_trace(&db);
    let bounded_narrow =
        bounded::execute(&db, &narrow_request).expect("bounded narrow AUTO search");
    assert_eq!(ids(&bounded_narrow.result), vec![wanted]);
    assert_exact_trace(&db, "bounded narrow AUTO route");
    assert_eq!(
        narrow_counter.brute_force_vectors.load(Ordering::SeqCst),
        1,
        "a narrow allowed set scores only its one allowed vector exactly"
    );
    assert_eq!(
        narrow_counter.table_rows.load(Ordering::SeqCst),
        0,
        "the bounded narrow route does not scan table rows"
    );
    assert_eq!(
        narrow_counter.index_entries.load(Ordering::SeqCst),
        1,
        "the bounded narrow route reads one complete relational posting"
    );
    assert_eq!(
        db.__vector_full_index_entries_touched_for_test(),
        0,
        "the bounded narrow route resolves its one allowed vector directly"
    );

    // One more row than the F32 boundary is sufficient to decide INDEXED;
    // threshold discovery must not keep reading the relation after that fact.
    let threshold_db = Database::open_memory();
    create_vector_truth(&threshold_db, "AUTO");
    seed_indexed_partition(&threshold_db, selected_scope(), 50_000, "threshold");
    threshold_db
        .execute(
            "CREATE INDEX vector_truth_kind_idx ON vector_truth(kind)",
            &empty(),
        )
        .expect("index threshold predicate");
    let threshold_sql = "SELECT id FROM vector_truth WHERE kind = 'threshold' \
                         ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 1";
    let threshold_counter = Arc::new(VectorTouches::default());
    let mut threshold_request = bounded_request(
        threshold_sql,
        params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
    );
    threshold_request.probe =
        Some(Arc::clone(&threshold_counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_hnsw_trace(&threshold_db);
    bounded::execute(&threshold_db, &threshold_request)
        .expect("AUTO crosses the maintained F32 threshold");
    assert_hnsw_trace(&threshold_db, "bounded AUTO threshold route");
    assert_eq!(
        threshold_counter.table_rows.load(Ordering::SeqCst),
        0,
        "AUTO threshold discovery uses the complete relational index route, not a table scan"
    );
    assert!(
        threshold_counter.index_entries.load(Ordering::SeqCst) > 0,
        "the threshold proof must observe the supporting relational index"
    );
    assert!(
        threshold_counter.index_entries.load(Ordering::SeqCst) <= (F32_INDEX_THRESHOLD + 1) as u64,
        "AUTO learns that the allowed set crossed the F32 threshold after at most threshold + 1 \
         relational entries, not by draining the relation"
    );
    assert!(
        threshold_counter.hnsw_vectors.load(Ordering::SeqCst) > 0,
        "AUTO at the F32 threshold must use the maintained vector route"
    );
    assert_eq!(
        threshold_counter.brute_force_vectors.load(Ordering::SeqCst),
        0,
        "AUTO at the F32 threshold cannot hide a full exact vector scan"
    );

    let unindexed = Database::open_memory();
    unindexed.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    unindexed
        .execute(
            "CREATE TABLE budget_weights (id UUID PRIMARY KEY, item_id UUID)",
            &empty(),
        )
        .expect("declare the empty rank-policy companion");
    unindexed
        .execute(
            "CREATE INDEX budget_weights_item ON budget_weights(item_id)",
            &empty(),
        )
        .expect("index the rank-policy companion");
    unindexed
        .execute(
            "CREATE TABLE vector_truth (id UUID PRIMARY KEY, scope_id UUID NOT NULL, \
         kind TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) \
         MAX_PARTITIONS 8 SEARCH_MODE INDEXED RANK_POLICY (JOIN budget_weights ON item_id, \
         FORMULA '{vector_score}', SORT_KEY budget_rank))",
            &empty(),
        )
        .expect("declare the maintained fixture with exhaustive rank work");
    seed_indexed_partition(&unindexed, selected_scope(), 60_000, "broad");
    let broad_sql = "SELECT id FROM vector_truth WHERE kind = $kind \
                     ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1";
    let broad = params([
        ("kind", Value::Text("broad".to_owned())),
        ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    reset_refusal_source_touches(&unindexed);
    let sql_error = unindexed
        .execute(broad_sql, &broad)
        .expect_err("SQL INDEXED broad filtering refuses without a relational index");
    assert_no_refusal_source_touches(&unindexed, "SQL INDEXED broad filter");
    reset_refusal_source_touches(&unindexed);
    let rust_error = unindexed
        .semantic_search({
            let mut query = SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 1);
            query.search_mode = Some(VectorSearchMode::Indexed);
            query.where_clause = Some("kind = 'broad'".to_owned());
            query
        })
        .expect_err("Rust INDEXED broad filtering refuses without a relational index");
    assert_no_refusal_source_touches(&unindexed, "Rust INDEXED broad filter");
    reset_refusal_source_touches(&unindexed);
    let read_session_error = unindexed
        .read_session(roomy_limits())
        .expect("open the bounded INDEXED production read session")
        .execute(broad_sql, &broad)
        .expect_err("bounded INDEXED broad filtering refuses without a relational index");
    assert_no_refusal_source_touches(&unindexed, "ReadSession INDEXED broad filter");
    for error in [sql_error, rust_error, read_session_error] {
        assert!(
            matches!(error, Error::VectorFilteredRouteUnavailable { .. }),
            "a broad indexed predicate without its declared candidate index names the filtered-route refusal: {error:?}"
        );
    }
    let broad_counter = Arc::new(VectorTouches::default());
    let mut broad_request = bounded_request(broad_sql, broad.clone());
    broad_request.probe = Some(Arc::clone(&broad_counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_refusal_source_touches(&unindexed);
    let bounded_error = bounded::execute(&unindexed, &broad_request)
        .expect_err("bounded INDEXED broad filtering refuses rather than scanning");
    assert_no_refusal_source_touches(&unindexed, "bounded INDEXED broad filter");
    assert!(
        matches!(
            bounded_error,
            bounded::TestError::Engine(ref message)
                if message.contains("has no bounded relational candidate route")
        ),
        "the test adapter preserves the engine's named filtered-route refusal: {bounded_error:?}"
    );
    assert_eq!(broad_counter.table_rows.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.index_entries.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.sort_candidates.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.graph_edges.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.adjacency_entries.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.brute_force_vectors.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.hnsw_vectors.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.rank_candidates.load(Ordering::SeqCst), 0);
    assert_eq!(broad_counter.access_rows.load(Ordering::SeqCst), 0);

    // Without a supporting relational index, AUTO may evaluate the predicate
    // and compare exactly when its active budget can pay for the complete
    // work. This is the user-visible distinction from INDEXED's unconditional
    // refusal: missing layout does not itself forbid a budgeted exact answer.
    let auto_sql = "SELECT id FROM vector_truth WHERE kind = $kind \
                    ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 1";
    unindexed.__reset_relational_scan_rows_touched();
    unindexed.__reset_relational_index_entries_touched();
    reset_hnsw_trace(&unindexed);
    let sql_auto = unindexed
        .execute(auto_sql, &broad)
        .expect("SQL AUTO may pay for an exact unindexed broad predicate");
    assert_eq!(ids(&sql_auto), vec![id(60_000)]);
    assert_exact_trace(&unindexed, "SQL budgeted broad AUTO exact route");
    assert!(
        unindexed.__relational_scan_rows_touched() > 0
            && unindexed.__relational_scan_rows_touched() <= (F32_INDEX_THRESHOLD + 1) as u64,
        "SQL AUTO's permitted fallback evaluates the finite relation once inside its budget"
    );
    assert_eq!(unindexed.__relational_index_entries_touched(), 0);

    unindexed.__reset_relational_scan_rows_touched();
    unindexed.__reset_relational_index_entries_touched();
    reset_hnsw_trace(&unindexed);
    let rust_auto = unindexed
        .semantic_search({
            let mut query = SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 1);
            query.search_mode = Some(VectorSearchMode::Auto);
            query.where_clause = Some("kind = 'broad'".to_owned());
            query
        })
        .expect("Rust AUTO may pay for the same exact unindexed predicate");
    assert_eq!(semantic_ids(rust_auto), vec![id(60_000)]);
    assert_exact_trace(&unindexed, "Rust budgeted broad AUTO exact route");
    assert!(
        unindexed.__relational_scan_rows_touched() > 0
            && unindexed.__relational_scan_rows_touched() <= (F32_INDEX_THRESHOLD + 1) as u64,
        "Rust AUTO evaluates the same finite relation once inside its budget"
    );
    assert_eq!(unindexed.__relational_index_entries_touched(), 0);

    unindexed.__reset_relational_scan_rows_touched();
    unindexed.__reset_relational_index_entries_touched();
    reset_hnsw_trace(&unindexed);
    let read_session_auto = unindexed
        .read_session(roomy_limits())
        .expect("open a bounded AUTO production read session")
        .execute(auto_sql, &broad)
        .expect("the public bounded reader may pay for the exact broad fallback");
    assert_eq!(ids(&read_session_auto), vec![id(60_000)]);
    assert_exact_trace(&unindexed, "ReadSession budgeted broad AUTO exact route");
    assert!(
        unindexed.__relational_scan_rows_touched() > 0
            && unindexed.__relational_scan_rows_touched() <= (F32_INDEX_THRESHOLD + 1) as u64,
        "ReadSession AUTO evaluates the same finite relation once inside its budget"
    );
    assert_eq!(
        unindexed.__relational_index_entries_touched(),
        0,
        "ReadSession AUTO does not invent an index for the permitted unindexed fallback"
    );

    let auto_counter = Arc::new(VectorTouches::default());
    let mut auto_request = bounded_request(auto_sql, broad.clone());
    auto_request.probe = Some(Arc::clone(&auto_counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_hnsw_trace(&unindexed);
    let bounded_auto = bounded::execute(&unindexed, &auto_request)
        .expect("the bounded kernel completes exact broad work inside roomy limits");
    assert_eq!(ids(&bounded_auto.result), vec![id(60_000)]);
    assert_exact_trace(&unindexed, "bounded budgeted broad AUTO exact route");
    assert_eq!(auto_counter.index_entries.load(Ordering::SeqCst), 0);
    assert_eq!(
        auto_counter.table_rows.load(Ordering::SeqCst),
        (F32_INDEX_THRESHOLD + 1) as u64,
        "the permitted unindexed fallback evaluates each finite row exactly once"
    );
    assert_eq!(
        auto_counter.brute_force_vectors.load(Ordering::SeqCst),
        (F32_INDEX_THRESHOLD + 1) as u64,
        "the permitted fallback compares every allowed stored vector exactly"
    );
    assert_eq!(auto_counter.hnsw_vectors.load(Ordering::SeqCst), 0);

    unindexed
        .execute(
            "CREATE TABLE vector_setup_control (id UUID PRIMARY KEY, embedding VECTOR(3) \
             RANK_POLICY (JOIN budget_weights ON item_id, FORMULA '{vector_score}', \
             SORT_KEY budget_rank))",
            &empty(),
        )
        .expect("declare an empty vector setup control before lowering the memory limit");
    let small_exact_sql = "SELECT id FROM vector_truth ORDER BY embedding <=> $query \
                           USE VECTOR EXACT LIMIT 4";
    let exact_sql = "SELECT id FROM vector_truth ORDER BY embedding <=> $query \
                     USE VECTOR EXACT USE RANK budget_rank LIMIT 4";
    let exact = params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]);
    // Keep the same statement/planning headroom and LIMIT. Unranked exact
    // top-k retains four scores; exhaustive ranking must retain all 1,001
    // candidates before applying the identity formula. That real score pool
    // exceeds 8 KiB without making ordinary statement setup too expensive.
    unindexed
        .set_memory_limit(Some(
            unindexed.accountant().usage().used + VECTOR_REFUSAL_HEADROOM_BYTES,
        ))
        .expect("leave deterministic statement/planning headroom below exact vector work");

    // Prove the fixed headroom admits ordinary parse/plan/session setup on
    // every door. These empty controls differ only in having no vector work,
    // so a later typed refusal cannot be a generic setup-memory failure.
    let setup_sql = "SELECT id FROM vector_setup_control ORDER BY embedding <=> $query \
                     USE VECTOR EXACT USE RANK budget_rank LIMIT 4";
    assert!(
        ids(&unindexed
            .execute(setup_sql, &exact)
            .expect("SQL parse, plan, and result setup fit inside fixed headroom"),)
        .is_empty()
    );
    let mut setup_semantic =
        SemanticQuery::new("vector_setup_control", "embedding", vec![1.0, 0.0, 0.0], 4);
    setup_semantic.search_mode = Some(VectorSearchMode::Exact);
    setup_semantic.sort_key = Some("budget_rank".to_owned());
    assert!(
        semantic_ids(
            unindexed
                .semantic_search(setup_semantic)
                .expect("Rust semantic setup fits inside fixed headroom"),
        )
        .is_empty()
    );
    assert!(
        ids(&unindexed
            .read_session(roomy_limits())
            .expect("open the setup-control read session")
            .execute(setup_sql, &exact)
            .expect("ReadSession parse, plan, and result setup fit inside fixed headroom"),)
        .is_empty()
    );

    // The same unindexed AUTO query must now refuse *before* source work. The
    // empty controls above prove this is the named vector/filter decision,
    // not a generic parse, planning, session, or result-allocation failure.
    reset_refusal_source_touches(&unindexed);
    let sql_auto_error = unindexed.execute(auto_sql, &broad).expect_err(
        "SQL AUTO refuses when its active budget cannot cover the broad exact fallback",
    );
    assert_no_refusal_source_touches(&unindexed, "SQL AUTO broad budget refusal");
    reset_refusal_source_touches(&unindexed);
    let rust_auto_error = unindexed
        .semantic_search({
            let mut query = SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 1);
            query.search_mode = Some(VectorSearchMode::Auto);
            query.where_clause = Some("kind = 'broad'".to_owned());
            query
        })
        .expect_err("Rust AUTO refuses when the broad exact fallback is over budget");
    assert_no_refusal_source_touches(&unindexed, "Rust AUTO broad budget refusal");
    reset_refusal_source_touches(&unindexed);
    let read_session_auto_error = unindexed
        .read_session(roomy_limits())
        .expect("open the bounded AUTO refusal session")
        .execute(auto_sql, &broad)
        .expect_err("ReadSession AUTO returns the same broad budget refusal");
    assert_no_refusal_source_touches(&unindexed, "ReadSession AUTO broad budget refusal");
    for error in [sql_auto_error, rust_auto_error, read_session_auto_error] {
        assert!(
            matches!(error, Error::VectorFilteredRouteUnavailable { .. }),
            "over-budget broad AUTO names the filtered-route refusal on every public door: {error:?}"
        );
    }
    let refused_auto_counter = Arc::new(VectorTouches::default());
    let mut refused_auto_request = bounded_request(auto_sql, broad.clone());
    refused_auto_request.probe =
        Some(Arc::clone(&refused_auto_counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_refusal_source_touches(&unindexed);
    let bounded_auto_error = bounded::execute(&unindexed, &refused_auto_request)
        .expect_err("bounded AUTO refuses before over-budget broad exact work starts");
    assert_no_refusal_source_touches(&unindexed, "bounded AUTO broad budget refusal");
    assert!(
        matches!(
            bounded_auto_error,
            bounded::TestError::Engine(ref message)
                if message.contains("has no bounded relational candidate route")
        ),
        "the bounded adapter preserves the named broad AUTO refusal: {bounded_auto_error:?}"
    );
    assert_eq!(
        vector_touches(&refused_auto_counter),
        VectorTouchSnapshot {
            table_rows: 0,
            index_entries: 0,
            sort_candidates: 0,
            graph_edges: 0,
            adjacency_entries: 0,
            brute_force_vectors: 0,
            hnsw_vectors: 0,
            rank_candidates: 0,
            access_rows: 0,
        }
    );

    let expected_small = (60_000..60_004).map(id).collect::<Vec<_>>();
    assert_eq!(
        ids(&unindexed
            .execute(small_exact_sql, &exact)
            .expect("SQL exact top-k fits")),
        expected_small,
    );
    assert_eq!(
        semantic_ids(
            unindexed
                .semantic_search({
                    let mut query =
                        SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 4);
                    query.search_mode = Some(VectorSearchMode::Exact);
                    query
                })
                .expect("Rust exact top-k fits")
        ),
        expected_small,
    );
    assert_eq!(
        ids(&unindexed
            .read_session(roomy_limits())
            .expect("open small exact session")
            .execute(small_exact_sql, &exact)
            .expect("ReadSession exact top-k fits")),
        expected_small,
    );

    reset_refusal_source_touches(&unindexed);
    let sql_exact_error = unindexed
        .execute(exact_sql, &exact)
        .expect_err("SQL exact route must name its exhausted vector budget");
    assert_no_refusal_source_touches(&unindexed, "SQL EXACT vector budget refusal");
    reset_refusal_source_touches(&unindexed);
    let rust_exact_error = unindexed
        .semantic_search({
            let mut query = SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 4);
            query.search_mode = Some(VectorSearchMode::Exact);
            query.sort_key = Some("budget_rank".to_owned());
            query
        })
        .expect_err("Rust exact route must name its exhausted vector budget");
    assert_no_refusal_source_touches(&unindexed, "Rust EXACT vector budget refusal");
    reset_refusal_source_touches(&unindexed);
    let read_session_exact_error = unindexed
        .read_session(roomy_limits())
        .expect("open a bounded production read session")
        .execute(exact_sql, &exact)
        .expect_err("bounded exact route must name its exhausted vector budget");
    assert_no_refusal_source_touches(&unindexed, "ReadSession EXACT vector budget refusal");
    for error in [sql_exact_error, rust_exact_error, read_session_exact_error] {
        assert!(
            matches!(error, Error::VectorExactSearchBudgetExceeded { .. }),
            "EXACT names VectorExactSearchBudgetExceeded on every public door: {error:?}"
        );
    }
}

#[test]
fn old_snapshot_indexed_uses_its_retained_generation_and_ignores_unselected_growth() {
    let root = tempfile::tempdir().expect("create the durable retained-generation fixture");
    let db = Database::open(root.path().join("retained-generation.redb"))
        .expect("open the durable retained-generation fixture");
    create_vector_truth(&db, "INDEXED");
    db.execute(
        "ALTER TABLE vector_truth ALTER COLUMN embedding \
         SET CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1)",
        &empty(),
    )
    .expect("make this generation-transition fixture explicitly eager");
    seed_indexed_partition(&db, selected_scope(), 70_000, "selected");
    let before = id(70_000);
    let g1 = generation(&db, selected_scope());
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);

    for ordinal in 0..6_u128 {
        db.execute(
            "UPDATE vector_truth SET embedding = $embedding WHERE id = $id",
            &params([
                ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
                ("id", Value::Uuid(id(70_000 + ordinal))),
            ]),
        )
        .expect("commit enough selected-partition work to cross the declared change threshold");
    }
    // First seal the change layer, then commit one more searchable change so
    // the next publication really replaces the base and retires the old chain.
    // Stopping at the first change checkpoint would still search G1 as a base.
    for _ in 0..MAINTENANCE_CYCLES {
        db.run_maintenance_cycle().unwrap();
        let status = db
            .vector_store_for_test()
            .partition_graph_generation_status(&partition_ref(selected_scope()))
            .unwrap();
        if status.change.is_some() || status.dormant_change.is_some() {
            break;
        }
    }
    for ordinal in 0..6_u128 {
        db.execute(
            "UPDATE vector_truth SET embedding = $embedding WHERE id = $id",
            &params([
                ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
                ("id", Value::Uuid(id(70_000 + ordinal))),
            ]),
        )
        .expect("commit the next threshold-crossing wave that requires a replacement base");
    }
    for _ in 0..MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("drive a finite later-generation publication batch");
        let status = db
            .vector_store_for_test()
            .partition_graph_generation_status(&partition_ref(selected_scope()))
            .unwrap();
        if status
            .base
            .or(status.dormant_base)
            .is_some_and(|base| base != g1)
        {
            break;
        }
    }
    let g2 = generation(&db, selected_scope());
    assert_ne!(
        g2, g1,
        "later selected work publishes G2 while G1 is pinned"
    );

    let selected_sql = "SELECT id FROM vector_truth WHERE scope_id = $scope \
                        ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1";
    let selected = params([
        ("scope", Value::Uuid(selected_scope())),
        ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    reset_hnsw_trace(&db);
    let old_rows = db
        .execute_at_snapshot(selected_sql, &selected, old_snapshot)
        .expect("an old snapshot INDEXED query uses its retained compatible generation");
    assert_eq!(
        ids(&old_rows),
        vec![before],
        "the G1-only nearest row proves the old INDEXED query did not select G2"
    );
    let old_trace = assert_hnsw_trace(&db, "old snapshot retained generation");
    assert_eq!(
        old_trace.selected_generations,
        vec![g1],
        "the old INDEXED call used its retained G1 generation, not merely any HNSW graph"
    );
    reset_hnsw_trace(&db);
    let current_rows = db
        .execute(selected_sql, &selected)
        .expect("a current INDEXED query uses G2");
    assert_ne!(
        ids(&current_rows),
        vec![before],
        "the current answer must reflect the replacement rather than reusing pinned G1"
    );
    let current_trace = assert_hnsw_trace(&db, "current snapshot later generation");
    assert_eq!(
        current_trace.selected_generations,
        vec![g2],
        "the current INDEXED call selects G2 while the pinned old call selects G1"
    );

    let selected_status_before = generation(&db, selected_scope());
    let selected_build_serial_before = db
        .vector_store_for_test()
        .raw_hnsw_build_serial_for_partition_for_test(&index(), &partition(selected_scope()));
    let selected_raw_residency_before = db
        .vector_store_for_test()
        .raw_partition_resident_for_test(&index(), &partition(selected_scope()));
    let baseline_counter = Arc::new(VectorTouches::default());
    let mut baseline_request = bounded_request(selected_sql, selected.clone());
    baseline_request.probe =
        Some(Arc::clone(&baseline_counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_hnsw_trace(&db);
    bounded::execute(&db, &baseline_request).expect("baseline selected-partition INDEXED read");
    assert_hnsw_trace(&db, "baseline selected-partition INDEXED route");
    let baseline_touches = vector_touches(&baseline_counter);
    assert!(
        baseline_touches.hnsw_vectors > 0,
        "the baseline selected query records maintained graph work"
    );
    assert_eq!(
        baseline_touches.brute_force_vectors, 0,
        "the baseline selected INDEXED query has no exact fallback"
    );
    let baseline_raw_residency = db
        .vector_store_for_test()
        .raw_partition_resident_for_test(&index(), &partition(selected_scope()));

    let tx = db
        .begin()
        .expect("open unselected-partition growth transaction");
    for ordinal in 0..32_u128 {
        insert_in_tx(
            &db,
            tx,
            id(79_000 + ordinal),
            other_scope(),
            "unselected",
            vec![0.0, 1.0, ordinal as f32 * 0.0001],
        );
    }
    db.commit(tx)
        .expect("commit growth in the unselected partition only");
    db.execute(
        "UPDATE vector_truth SET embedding = $embedding WHERE id = $id",
        &params([
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
            ("id", Value::Uuid(id(79_000))),
        ]),
    )
    .expect("commit an unselected-partition replacement");
    drive_until_indexed(&db, other_scope());

    assert_eq!(
        generation(&db, selected_scope()),
        selected_status_before,
        "unselected growth does not load, build, or publish another selected-partition generation"
    );
    assert_eq!(
        db.vector_store_for_test()
            .raw_hnsw_build_serial_for_partition_for_test(&index(), &partition(selected_scope()),),
        selected_build_serial_before,
        "unselected growth cannot rebuild the selected partition's graph"
    );
    assert_eq!(
        db.vector_store_for_test()
            .raw_partition_resident_for_test(&index(), &partition(selected_scope())),
        selected_raw_residency_before,
        "unselected growth cannot load or evict selected raw vectors before the selected query"
    );
    let after_counter = Arc::new(VectorTouches::default());
    let mut after_request = bounded_request(selected_sql, selected.clone());
    after_request.probe = Some(Arc::clone(&after_counter) as Arc<dyn bounded::ExecutionProbe>);
    reset_hnsw_trace(&db);
    bounded::execute(&db, &after_request)
        .expect("selected INDEXED read remains usable after unselected growth");
    assert_hnsw_trace(&db, "selected INDEXED route after unselected growth");
    assert_eq!(
        vector_touches(&after_counter),
        baseline_touches,
        "unselected growth leaves every available selected-query source counter unchanged: \
         table/index/sort, graph-distance/adjacency, raw-vector/HNSW, rank, and access work"
    );
    assert_eq!(
        after_counter.brute_force_vectors.load(Ordering::SeqCst),
        0,
        "the selected INDEXED query cannot gain an exact raw-vector fallback after other-partition growth"
    );
    assert_eq!(
        db.vector_store_for_test()
            .raw_partition_resident_for_test(&index(), &partition(selected_scope())),
        baseline_raw_residency,
        "unselected growth cannot make the selected query load or evict raw vector bodies"
    );
    reset_hnsw_trace(&db);
    assert_eq!(
        ids(&db
            .execute_at_snapshot(selected_sql, &selected, old_snapshot)
            .expect("the pinned G1 route still serves after unrelated partition growth"),),
        vec![before],
        "unrelated growth cannot change the old snapshot's selected generation or answer"
    );
    let old_trace_after_growth =
        assert_hnsw_trace(&db, "old snapshot after unrelated partition growth");
    assert_eq!(
        old_trace_after_growth.selected_generations,
        vec![g1],
        "the retained old-snapshot query still used G1 after unrelated partition growth"
    );
    reset_hnsw_trace(&db);
    assert_ne!(
        ids(&db
            .execute(selected_sql, &selected)
            .expect("the current G2 route survives unrelated maintained growth"),),
        vec![before],
        "unrelated maintenance cannot move the current selected route back to G1"
    );
    let current_trace_after_growth =
        assert_hnsw_trace(&db, "current snapshot after unrelated partition growth");
    assert_eq!(
        current_trace_after_growth.selected_generations,
        vec![g2],
        "unrelated maintenance cannot change the current selected partition's G2 route"
    );
    drop(old_pin);
}

struct PauseRegisteredVectorRead {
    reached: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl bounded::ExecutionProbe for PauseRegisteredVectorRead {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
    fn before_source_touch(&self, _: bounded::TestSourceTouch, _: u64) {}
    fn cancellation_observed(&self, _: u64) {}
    fn after_snapshot_registration(&self) {
        self.reached.send(()).unwrap();
        self.resume
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    }
}

fn assert_three_current_vectors(db: &Database, expected: &[f32]) {
    let snapshot = db.snapshot();
    let entries = db
        .vector_store_for_test()
        .entries_for_index(&index())
        .unwrap();
    let visible = entries
        .iter()
        .filter(|entry| entry.visible_at(snapshot))
        .collect::<Vec<_>>();
    assert_eq!(visible.len(), 3);
    assert_eq!(
        visible
            .iter()
            .map(|entry| entry.row_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
    for (mode, keyword) in [
        (VectorSearchMode::Exact, "EXACT"),
        (VectorSearchMode::Indexed, "INDEXED"),
    ] {
        let sql = format!(
            "SELECT id, embedding FROM vector_truth ORDER BY embedding <=> $query USE VECTOR {keyword} LIMIT 10"
        );
        let bound = params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]);
        let ordinary = db.execute(&sql, &bound).unwrap();
        let bounded = db
            .read_session(roomy_limits())
            .unwrap()
            .execute(&sql, &bound)
            .unwrap();
        assert_eq!(ordinary.rows, bounded.rows);
        let actual_ids = ids(&ordinary);
        assert_eq!(actual_ids.len(), 3);
        assert_eq!(
            actual_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
        let query = SemanticQuery {
            search_mode: Some(mode),
            ..SemanticQuery::new("vector_truth", "embedding", vec![1.0, 0.0, 0.0], 10)
        };
        let semantic = db.semantic_search(query).unwrap();
        let expected_score = contextdb_vector::cosine_similarity(expected, &[1.0, 0.0, 0.0]);
        let changed = semantic
            .iter()
            .find(|row| row.values.get("id") == Some(&Value::Uuid(id(7))))
            .unwrap();
        assert!((changed.vector_score - expected_score).abs() < 0.00001);
        assert_eq!(semantic_ids(semantic), actual_ids);
        let point = format!(
            "SELECT id, embedding FROM vector_truth WHERE id = $id ORDER BY embedding <=> $query USE VECTOR {keyword} LIMIT 10"
        );
        let point_bound = params([
            ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
            ("id", Value::Uuid(id(7))),
        ]);
        let point_rows = db.execute(&point, &point_bound).unwrap();
        assert_eq!(ids(&point_rows), vec![id(7)]);
        assert_eq!(
            point_rows.rows,
            db.read_session(roomy_limits())
                .unwrap()
                .execute(&point, &point_bound)
                .unwrap()
                .rows
        );
        assert!(ordinary.rows.contains(&point_rows.rows[0]));
    }
}

#[tokio::test]
async fn database_late_lower_commit_keeps_unique_vectors_across_snapshots_restart_and_sync() {
    use contextdb_core::TenantId;
    use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
    use std::sync::atomic::AtomicBool;
    let root = tempfile::tempdir().unwrap();
    let hub_path = root.path().join("snapshot-hub.redb");
    let edge_path = root.path().join("snapshot-edge.redb");
    let db = Arc::new(Database::open(&hub_path).unwrap());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE vector_truth (id UUID PRIMARY KEY, scope_id UUID NOT NULL, kind TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 AUTO_INDEX_AT 1) SYNC TWO WAY SYNC CONFLICT KEEP LATEST", &empty()).unwrap();
    let tx = db.begin().unwrap();
    for (row, vector) in [
        (7, vec![1.0, 0.0, 0.0]),
        (8, vec![0.0, 1.0, 0.0]),
        (9, vec![0.0, 0.0, 1.0]),
    ] {
        insert_in_tx(&db, tx, id(row), selected_scope(), "selected", vector);
    }
    db.commit(tx).unwrap();
    drive_until_indexed(&db, selected_scope());
    assert_three_current_vectors(&db, &[1.0, 0.0, 0.0]);
    let initial_snapshot = db.snapshot();
    let initial_pin = db.pin_snapshot(initial_snapshot);
    let lower = db.begin().unwrap();
    let higher = db.begin().unwrap();
    assert!(lower < higher);
    let update = "UPDATE vector_truth SET embedding = $embedding WHERE id = $id";
    db.execute_in_tx(
        higher,
        update,
        &params([
            ("embedding", Value::Vector(vec![0.6, 0.8, 0.0])),
            ("id", Value::Uuid(id(7))),
        ]),
    )
    .unwrap();
    db.commit(higher).unwrap();
    let intermediate_snapshot = db.snapshot();
    let intermediate_pin = db.pin_snapshot(intermediate_snapshot);
    assert_three_current_vectors(&db, &[0.6, 0.8, 0.0]);
    let mut waiting = Vec::new();
    for keyword in ["EXACT", "INDEXED"] {
        let sql = format!(
            "SELECT id, embedding FROM vector_truth ORDER BY embedding <=> $query USE VECTOR {keyword} LIMIT 10"
        );
        let bound = params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]);
        let expected = db
            .execute_at_snapshot(&sql, &bound, intermediate_snapshot)
            .unwrap();
        let (reached, receiver) = std::sync::mpsc::sync_channel(0);
        let (resume, resume_rx) = std::sync::mpsc::sync_channel(0);
        let mut request = bounded_request(&sql, bound.clone());
        request.probe = Some(Arc::new(PauseRegisteredVectorRead {
            reached,
            resume: std::sync::Mutex::new(resume_rx),
        }));
        let reader = db.clone();
        let task = std::thread::spawn(move || bounded::execute(&reader, &request).unwrap().result);
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        waiting.push((resume, task, expected, sql, bound));
    }
    // The transaction was allocated first but writes the current row later.
    // The production commit path must give its vectors the same new visibility
    // boundary as its row, rather than publishing the earlier allocated ID.
    db.execute_in_tx(
        lower,
        update,
        &params([
            ("embedding", Value::Vector(vec![0.8, 0.6, 0.0])),
            ("id", Value::Uuid(id(7))),
        ]),
    )
    .unwrap();
    db.commit(lower).unwrap();
    assert!(db.snapshot() > intermediate_snapshot);
    for (resume, task, expected, sql, bound) in waiting {
        resume.send(()).unwrap();
        assert_eq!(task.join().unwrap().rows, expected.rows);
        assert_eq!(
            db.execute_at_snapshot(&sql, &bound, intermediate_snapshot)
                .unwrap()
                .rows,
            expected.rows
        );
        let initial = db
            .execute_at_snapshot(&sql, &bound, initial_snapshot)
            .unwrap();
        assert_eq!(ids(&initial).len(), 3);
        assert_ne!(initial.rows, expected.rows);
    }
    assert_three_current_vectors(&db, &[0.8, 0.6, 0.0]);
    let entries = db
        .vector_store_for_test()
        .entries_for_index(&index())
        .unwrap();
    assert!(entries.iter().all(|entry| {
        entry
            .deleted_tx
            .is_none_or(|deleted| deleted >= entry.created_tx)
    }));
    for snapshot in [initial_snapshot, intermediate_snapshot, db.snapshot()] {
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.visible_at(snapshot))
                .count(),
            3
        );
    }
    drop(initial_pin);
    drop(intermediate_pin);
    db.close().unwrap();
    drop(db);

    let hub_identity = Arc::new(FabricIdentity::generate());
    let edge_identity = Arc::new(FabricIdentity::generate());
    for round in 0..2 {
        let hub = Arc::new(Database::open(&hub_path).unwrap());
        let edge = Arc::new(Database::open(&edge_path).unwrap());
        hub.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_three_current_vectors(
            &hub,
            if round == 0 {
                &[0.8, 0.6, 0.0]
            } else {
                &[0.0, 0.6, 0.8]
            },
        );
        let broker = InProcessBroker::new();
        let tenant = "vector-snapshot-versions";
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub.clone(),
                broker.server_as(&hub_identity.node_id()),
                TenantId::from(tenant),
                hub_identity.node_id(),
                hub_identity.clone(),
            ),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move { server.run_until(stop).await }
        });
        broker
            .wait_for_registered_route_for_test(&contextdb_server::subjects::push_subject(tenant))
            .await;
        broker
            .wait_for_registered_route_for_test(&contextdb_server::subjects::pull_subject(tenant))
            .await;
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&edge_identity.node_id()),
            TenantId::from(tenant),
            edge_identity.clone(),
        );
        client.pull_default().await.unwrap();
        drive_until_indexed(&edge, selected_scope());
        assert_three_current_vectors(
            &edge,
            if round == 0 {
                &[0.8, 0.6, 0.0]
            } else {
                &[0.0, 0.6, 0.8]
            },
        );
        if round == 0 {
            let before = hub.snapshot();
            let pin = hub.pin_snapshot(before);
            edge.execute(
                update,
                &params([
                    ("embedding", Value::Vector(vec![0.0, 0.6, 0.8])),
                    ("id", Value::Uuid(id(7))),
                ]),
            )
            .unwrap();
            assert_three_current_vectors(&edge, &[0.0, 0.6, 0.8]);
            let mut waiting = Vec::new();
            for keyword in ["EXACT", "INDEXED"] {
                let sql = format!(
                    "SELECT id, embedding FROM vector_truth ORDER BY embedding <=> $query USE VECTOR {keyword} LIMIT 10"
                );
                let bound = params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]);
                let expected = hub.execute_at_snapshot(&sql, &bound, before).unwrap();
                let (reached, receiver) = std::sync::mpsc::sync_channel(0);
                let (resume, resume_rx) = std::sync::mpsc::sync_channel(0);
                let mut request = bounded_request(&sql, bound.clone());
                request.probe = Some(Arc::new(PauseRegisteredVectorRead {
                    reached,
                    resume: std::sync::Mutex::new(resume_rx),
                }));
                let reader = hub.clone();
                let task =
                    std::thread::spawn(move || bounded::execute(&reader, &request).unwrap().result);
                receiver
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                waiting.push((resume, task, expected, sql, bound));
            }
            client.push().await.unwrap();
            assert_three_current_vectors(&hub, &[0.0, 0.6, 0.8]);
            for (resume, task, expected, sql, bound) in waiting {
                resume.send(()).unwrap();
                assert_eq!(task.join().unwrap().rows, expected.rows);
                let old = hub.execute_at_snapshot(&sql, &bound, before).unwrap();
                assert_eq!(old.rows, expected.rows);
                assert_eq!(ids(&old), vec![id(7), id(8), id(9)]);
                assert_ne!(hub.execute(&sql, &bound).unwrap().rows, expected.rows);
            }
            drop(pin);
        }
        drop(client);
        stop.store(true, Ordering::SeqCst);
        task.await.unwrap();
        drop(server);
        edge.close().unwrap();
        hub.close().unwrap();
    }
}
