//! Adversarial contract tests for the bounded pull kernel.
//!
//! Every call below reaches the crate-private bounded-kernel entrance through
//! the `test-seams` adapter.  Fixture setup deliberately uses the existing
//! uncapped `Database::execute`; only the asserted reads are bounded.  This
//! keeps the assertions on production relational, graph, and vector data paths.

use contextdb_core::read_contract::{
    CursorExpiryKind, DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadFailureDetail,
    ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{ContextId, Direction, Value, VectorIndexRef};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::read_contract::{cursor_page_encoded_size, query_result_encoded_size};
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_graph::mem::MAX_VISITED;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use uuid::Uuid;

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 8_192,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 256,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

#[derive(Clone, Default)]
struct ManualClock {
    now_ms: Arc<AtomicU64>,
}

impl ManualClock {
    fn advance(&self, elapsed_ms: u64) {
        self.now_ms.fetch_add(elapsed_ms, Ordering::SeqCst);
    }
}

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These kernel tests are synchronous.  The immediately-completing
        // future satisfies the shared clock's transport-facing trait.
        Box::pin(async {})
    }
}

fn request(
    sql: impl Into<String>,
    params: HashMap<String, Value>,
    limits: ReadLimits,
    clock: &ManualClock,
) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, params, limits, Arc::new(clock.clone()))
}

fn bounded_success(db: &Database, request: &bounded::BoundedReadRequest) -> bounded::TestResult {
    bounded::execute(db, request).unwrap_or_else(|error| {
        panic!("bounded kernel must execute this production fixture; got {error:?}")
    })
}

fn assert_limit(error: bounded::TestError, limit: ReadFailureLimit, value: u64) {
    let bounded::TestError::Refused(refusal) = error else {
        panic!("expected typed owner_limit_exceeded({limit:?}), got {error:?}");
    };
    assert_eq!(refusal.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = refusal.detail() else {
        panic!("owner_limit_exceeded must preserve canonical typed detail");
    };
    assert_eq!(detail.limit, limit);
    assert_eq!(detail.value, value);
}

fn assert_limit_with_required_bytes(
    error: bounded::TestError,
    limit: ReadFailureLimit,
    value: u64,
    required_bytes: u64,
    statement: &str,
) {
    let bounded::TestError::Refused(refusal) = error else {
        panic!("expected typed owner_limit_exceeded({limit:?}), got {error:?}");
    };
    assert_eq!(refusal.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = refusal.detail() else {
        panic!("owner_limit_exceeded must preserve canonical typed detail");
    };
    assert_eq!(detail.limit, limit);
    assert_eq!(detail.value, value);
    let required = detail
        .required
        .as_ref()
        .expect("an individually oversized row reports its encoded size");
    assert_eq!(required.required_bytes, required_bytes);
    assert_eq!(
        required.required_setting,
        format!("effective cursor_page_bytes >= {required_bytes}"),
        "the required-byte detail names the effective setting and its exact minimum"
    );
    let remedy = detail
        .statement
        .as_ref()
        .expect("an oversized cursor row carries the established statement remedy");
    assert_eq!(remedy.statement, statement);
    assert_eq!(
        remedy.remedy_command, "select fewer columns",
        "the typed remedy tells the caller how to make this one row pageable"
    );
}

fn assert_cursor_expired(error: bounded::TestError, expiry: CursorExpiryKind) {
    let bounded::TestError::Refused(refusal) = error else {
        panic!("expected typed cursor_expired({expiry:?}), got {error:?}");
    };
    assert_eq!(refusal.kind(), ReadFailureKind::CursorExpired);
    assert_eq!(
        refusal.detail(),
        &ReadFailureDetail::CursorExpired { expiry },
        "the expiry cause is stable machine data"
    );
}

fn assert_cursor_not_found(error: bounded::TestError) {
    let bounded::TestError::Refused(refusal) = error else {
        panic!("expected typed cursor_not_found, got {error:?}");
    };
    assert_eq!(refusal.kind(), ReadFailureKind::CursorNotFound);
    assert_eq!(
        refusal.detail(),
        &ReadFailureDetail::None,
        "a released cursor keeps no stale continuation detail"
    );
}

fn dynamic_payload(seed: u64, width: usize) -> String {
    let mut state = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32);
    (0..width)
        .map(|index| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(index as u64 + 1);
            char::from(b'a' + ((state >> 59) % 26) as u8)
        })
        .collect()
}

fn seed_relational(db: &Database, table: &str, rows: usize, payload_width: usize) {
    db.execute(
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)"),
        &HashMap::new(),
    )
    .unwrap_or_else(|error| panic!("create {table}: {error}"));
    for id in 0..rows {
        db.execute(
            &format!("INSERT INTO {table} (id, score, payload) VALUES ($id, $score, $payload)"),
            &params([
                ("id", Value::Int64(id as i64)),
                ("score", Value::Int64(((id * 17 + 3) % rows.max(1)) as i64)),
                (
                    "payload",
                    Value::Text(dynamic_payload(
                        id as u64 + rows as u64,
                        payload_width + id % 7,
                    )),
                ),
            ]),
        )
        .unwrap_or_else(|error| panic!("seed {table}/{id}: {error}"));
    }
}

fn ids(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.ends_with(".id"))
        .expect("query result contains id");
    result
        .rows
        .iter()
        .map(|row| match row[id_column] {
            Value::Int64(value) => value,
            ref other => panic!("id must be INTEGER, got {other:?}"),
        })
        .collect()
}

/// Exact and one-past limits apply during result production, not after an eager
/// `QueryResult` already exists.
#[test]
fn bounded_limits_are_data_derived_exact_and_return_typed_refusals() {
    let db = Database::open_memory();
    seed_relational(&db, "measurements", 37, 19);
    let clock = ManualClock::default();
    let sql = "SELECT id, payload FROM measurements WHERE id < 7 ORDER BY id";

    let profiled = bounded_success(&db, &request(sql, HashMap::new(), roomy_limits(), &clock));
    assert_eq!(ids(&profiled.result), (0..7).collect::<Vec<_>>());
    assert!(profiled.telemetry.work_units >= 7);
    assert!(profiled.telemetry.encoded_bytes > 0);
    assert!(profiled.telemetry.peak_temporary_bytes > 0);
    assert_eq!(
        profiled.telemetry.encoded_bytes as usize,
        query_result_encoded_size(&profiled.result)
            .expect("canonical encoder supplies result byte accounting"),
        "encoded-byte enforcement must use the route-neutral canonical encoder"
    );

    let mut exact_rows = roomy_limits();
    exact_rows.result_rows = 7;
    assert_eq!(
        ids(&bounded_success(&db, &request(sql, HashMap::new(), exact_rows, &clock),).result),
        (0..7).collect::<Vec<_>>()
    );
    let mut one_past_rows = exact_rows;
    one_past_rows.result_rows = 6;
    assert_limit(
        bounded::execute(&db, &request(sql, HashMap::new(), one_past_rows, &clock))
            .expect_err("a complete seven-row result cannot be silently truncated to six"),
        ReadFailureLimit::ResultRows,
        6,
    );

    let mut exact_bytes = roomy_limits();
    exact_bytes.result_bytes = profiled.telemetry.encoded_bytes;
    assert_eq!(
        bounded_success(&db, &request(sql, HashMap::new(), exact_bytes, &clock),)
            .telemetry
            .encoded_bytes,
        profiled.telemetry.encoded_bytes
    );
    let mut one_past_bytes = exact_bytes;
    one_past_bytes.result_bytes -= 1;
    assert_limit(
        bounded::execute(&db, &request(sql, HashMap::new(), one_past_bytes, &clock))
            .expect_err("one encoded byte over the ceiling publishes no partial ordinary result"),
        ReadFailureLimit::ResultBytes,
        one_past_bytes.result_bytes,
    );

    let mut exact_memory = roomy_limits();
    exact_memory.memory = profiled.telemetry.peak_temporary_bytes;
    assert!(
        bounded_success(&db, &request(sql, HashMap::new(), exact_memory, &clock),)
            .telemetry
            .peak_temporary_bytes
            <= exact_memory.memory
    );
    let mut one_past_memory = exact_memory;
    one_past_memory.memory -= 1;
    assert_limit(
        bounded::execute(&db, &request(sql, HashMap::new(), one_past_memory, &clock))
            .expect_err("temporary result state must reserve memory before retaining it"),
        ReadFailureLimit::Memory,
        one_past_memory.memory,
    );

    // A read's local memory ceiling is not a substitute for the database-wide
    // accountant.  The same dynamic result must fit both charges, and a
    // refused second charge must roll back the first rather than leak capacity.
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let dual_charge = Database::open_memory_with_accountant(Arc::clone(&accountant));
    seed_relational(&dual_charge, "dual_charge_rows", 37, 19);
    let dual_sql = "SELECT id, payload FROM dual_charge_rows WHERE id < 7 ORDER BY id";
    let baseline = accountant.usage().used;
    let dual_profile = bounded_success(
        &dual_charge,
        &request(dual_sql, HashMap::new(), roomy_limits(), &clock),
    );
    assert!(dual_profile.telemetry.peak_temporary_bytes > 0);
    let dual_peak_bytes = usize::try_from(dual_profile.telemetry.peak_temporary_bytes)
        .expect("profiled temporary bytes fit the database accountant's native size");
    accountant
        .set_budget(Some(baseline.saturating_add(dual_peak_bytes)))
        .expect("exact database-wide headroom");
    assert!(
        bounded_success(
            &dual_charge,
            &request(dual_sql, HashMap::new(), roomy_limits(), &clock),
        )
        .telemetry
        .peak_temporary_bytes
            <= dual_profile.telemetry.peak_temporary_bytes
    );
    accountant
        .set_budget(Some(
            baseline.saturating_add(dual_peak_bytes).saturating_sub(1),
        ))
        .expect("one-byte-short database-wide headroom");
    assert_limit(
        bounded::execute(
            &dual_charge,
            &request(dual_sql, HashMap::new(), roomy_limits(), &clock),
        )
        .expect_err("a per-read success may not overdraw database-wide memory"),
        ReadFailureLimit::Memory,
        roomy_limits().memory,
    );
}

/// LIMIT stops the table source, while an unindexed sort cannot pretend it can
/// stop early merely because its final output is small.  Sizes are generated at
/// runtime to reject fixture-keyed shortcuts.
#[test]
fn relational_and_index_sources_charge_each_inspection_before_limit_or_sort() {
    for row_count in [19_usize, 113, 509] {
        let db = Database::open_memory();
        seed_relational(&db, "events", row_count, 11 + row_count % 13);
        let clock = ManualClock::default();
        let mut limits = roomy_limits();
        limits.work = 2;
        limits.result_rows = 2;
        db.__reset_relational_scan_rows_touched();
        let outcome = bounded_success(
            &db,
            &request(
                "SELECT id FROM events LIMIT 2",
                HashMap::new(),
                limits,
                &clock,
            ),
        );
        assert_eq!(outcome.result.rows.len(), 2);
        assert_eq!(
            outcome
                .telemetry
                .source_work
                .get(&bounded::TestWorkSource::TableScan),
            Some(&2),
            "LIMIT must stop the table source for runtime dataset size {row_count}"
        );
        assert_eq!(
            db.__relational_scan_rows_touched(),
            2,
            "a post-hoc truncate would touch all {row_count} source rows"
        );

        db.execute(
            "CREATE INDEX events_score_idx ON events(score)",
            &HashMap::new(),
        )
        .expect("index");
        db.__reset_relational_scan_rows_touched();
        let mut index_limits = roomy_limits();
        index_limits.work = 3;
        index_limits.result_rows = 3;
        let range = bounded_success(
            &db,
            &request(
                "SELECT id FROM events WHERE score >= $low AND score < $high LIMIT 3",
                params([
                    ("low", Value::Int64(0)),
                    ("high", Value::Int64(row_count as i64)),
                ]),
                index_limits,
                &clock,
            ),
        );
        assert_eq!(range.result.rows.len(), 3);
        assert_eq!(
            range
                .telemetry
                .source_work
                .get(&bounded::TestWorkSource::IndexRange),
            Some(&3),
            "range work is charged at each index entry before it is inspected"
        );
        assert_eq!(
            db.__relational_scan_rows_touched(),
            0,
            "an indexed range must not hide a table scan behind its small LIMIT"
        );

        let mut sort_limits = roomy_limits();
        sort_limits.work = 5;
        assert_limit(
            bounded::execute(
                &db,
                &request(
                    "SELECT id FROM events ORDER BY payload LIMIT 2",
                    HashMap::new(),
                    sort_limits,
                    &clock,
                ),
            )
            .expect_err("an unindexed sort must charge the source it has to materialize"),
            ReadFailureLimit::Work,
            5,
        );
    }
}

fn graph_uuid(seed: u128) -> Uuid {
    Uuid::from_u128(0xB0D0_ED00_0000_0000_0000_0000_0000_0000_u128.wrapping_add(seed))
}

fn fixture_uuid(namespace: u32, ordinal: u64) -> Uuid {
    Uuid::from_u128(
        0xD37E_1A5E_0000_0000_0000_0000_0000_0000_u128
            | ((namespace as u128) << 64)
            | ordinal as u128,
    )
}

const GRAPH_FIXTURE_FANOUT: usize = 8;
const GRAPH_FIXTURE_INSERT_BATCH: usize = 256;

struct BoundedGraphFixture {
    start: Uuid,
    targets: usize,
    deepest_depth: u8,
}

fn append_graph_child(
    nodes: &mut Vec<Uuid>,
    edges: &mut Vec<(Uuid, Uuid)>,
    parent: Uuid,
    depth: u8,
    first_seed: u128,
    next_seed: &mut u128,
) -> Uuid {
    let child = graph_uuid(first_seed + *next_seed);
    *next_seed += 1;
    nodes.push(child);
    edges.push((parent, child));
    assert!(
        depth <= 10,
        "bounded graph fixture never exceeds SQL's ten-hop cap"
    );
    child
}

/// Seed the real relational node/edge tables in bounded write batches.  Edge
/// INSERTs take the production SQL graph-routing path, so the relational edge
/// metadata and graph adjacency are one fixture rather than two unrelated
/// test-only populations.
fn create_bounded_fanout_graph_fixture(
    db: &Database,
    namespace: u128,
    target_count: usize,
    context: Option<Uuid>,
) -> BoundedGraphFixture {
    assert!(
        target_count >= 10,
        "the fixture reserves a real branch at every depth through ten"
    );
    let context_column = if context.is_some() {
        ", context_id UUID CONTEXT_ID"
    } else {
        ""
    };
    db.execute(
        &format!("CREATE TABLE nodes (id UUID PRIMARY KEY{context_column})"),
        &HashMap::new(),
    )
    .expect("create relational graph nodes");
    db.execute(
        &format!(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT{context_column})"
        ),
        &HashMap::new(),
    )
    .expect("create relational graph edges");

    let first_seed = namespace.saturating_mul(1_000_000);
    let start = graph_uuid(first_seed);
    let mut next_seed = 1_u128;
    let mut nodes = vec![start];
    let mut edges = Vec::with_capacity(target_count);

    // Keep one genuine depth-ten branch while the remaining targets grow from
    // bounded-width breadth-first roots.  This replaces the 100,007-edge star
    // without weakening the ten-hop traversal proof.
    let mut deep_parent = start;
    for depth in 1..=10 {
        deep_parent = append_graph_child(
            &mut nodes,
            &mut edges,
            deep_parent,
            depth,
            first_seed,
            &mut next_seed,
        );
    }
    let mut breadth_frontier = VecDeque::new();
    for _ in 0..GRAPH_FIXTURE_FANOUT - 1 {
        if edges.len() == target_count {
            break;
        }
        let child =
            append_graph_child(&mut nodes, &mut edges, start, 1, first_seed, &mut next_seed);
        breadth_frontier.push_back((child, 1_u8));
    }
    while edges.len() < target_count {
        let (parent, depth) = breadth_frontier
            .pop_front()
            .expect("bounded fan-out leaves work until every target exists");
        assert!(
            depth < 10,
            "breadth expansion remains below the ten-hop cap"
        );
        for _ in 0..GRAPH_FIXTURE_FANOUT {
            if edges.len() == target_count {
                break;
            }
            let child = append_graph_child(
                &mut nodes,
                &mut edges,
                parent,
                depth + 1,
                first_seed,
                &mut next_seed,
            );
            breadth_frontier.push_back((child, depth + 1));
        }
    }

    for batch in nodes.chunks(GRAPH_FIXTURE_INSERT_BATCH) {
        let tx = db.begin_or_panic();
        for id in batch {
            let node_params = match context {
                Some(context) => {
                    params([("id", Value::Uuid(*id)), ("context", Value::Uuid(context))])
                }
                None => params([("id", Value::Uuid(*id))]),
            };
            let node_sql = if context.is_some() {
                "INSERT INTO nodes (id, context_id) VALUES ($id, $context)"
            } else {
                "INSERT INTO nodes (id) VALUES ($id)"
            };
            db.execute_in_tx(tx, node_sql, &node_params)
                .unwrap_or_else(|error| panic!("seed graph node {id}: {error}"));
        }
        db.commit(tx).expect("commit bounded graph node batch");
    }
    for (batch_index, batch) in edges.chunks(GRAPH_FIXTURE_INSERT_BATCH).enumerate() {
        let tx = db.begin_or_panic();
        for (edge_index, (source, target)) in batch.iter().enumerate() {
            let edge_id =
                graph_uuid(first_seed.saturating_add(2_000_000).saturating_add(
                    (batch_index * GRAPH_FIXTURE_INSERT_BATCH + edge_index) as u128,
                ));
            let edge_params = match context {
                Some(context) => params([
                    ("id", Value::Uuid(edge_id)),
                    ("source", Value::Uuid(*source)),
                    ("target", Value::Uuid(*target)),
                    ("context", Value::Uuid(context)),
                ]),
                None => params([
                    ("id", Value::Uuid(edge_id)),
                    ("source", Value::Uuid(*source)),
                    ("target", Value::Uuid(*target)),
                ]),
            };
            let edge_sql = if context.is_some() {
                "INSERT INTO edges (id, source_id, target_id, edge_type, context_id) \
                 VALUES ($id, $source, $target, 'LINKS', $context)"
            } else {
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, 'LINKS')"
            };
            db.execute_in_tx(tx, edge_sql, &edge_params)
                .unwrap_or_else(|error| panic!("seed graph edge {source}->{target}: {error}"));
        }
        db.commit(tx).expect("commit bounded graph edge batch");
    }

    BoundedGraphFixture {
        start,
        targets: edges.len(),
        deepest_depth: 10,
    }
}

const WIDE_GRAPH_SQL: &str = "SELECT COUNT(*) AS candidate_count FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,10}(b) WHERE a.id = $start COLUMNS (b.id AS target))";

fn scalar_i64(result: &contextdb_engine::QueryResult, column: &str) -> i64 {
    let column_index = result
        .columns
        .iter()
        .position(|candidate| candidate == column)
        .unwrap_or_else(|| panic!("result contains {column}"));
    match result.rows.as_slice() {
        [row] => match row[column_index] {
            Value::Int64(value) => value,
            ref other => panic!("{column} must be INTEGER, got {other:?}"),
        },
        rows => panic!("scalar aggregate returned {} rows", rows.len()),
    }
}

fn assert_wide_graph_is_request_bounded(
    db: &Database,
    start: Uuid,
    inserted_targets: usize,
    clock: &ManualClock,
) {
    let mut success_limits = roomy_limits();
    success_limits.result_rows = 1;
    success_limits.cursor_page_rows = 1;
    success_limits.work = (inserted_targets as u64)
        .saturating_mul(32)
        .saturating_add(1_024);
    success_limits.memory = 512 * 1024 * 1024;
    let success = bounded_success(
        db,
        &request(
            WIDE_GRAPH_SQL,
            params([("start", Value::Uuid(start))]),
            success_limits,
            clock,
        ),
    );
    assert_eq!(
        scalar_i64(&success.result, "candidate_count"),
        inserted_targets as i64,
        "the ten-hop query must traverse every runtime-inserted tree target"
    );
    assert!(
        success.telemetry.source_work[&bounded::TestWorkSource::GraphTraversal]
            >= inserted_targets as u64,
        "candidate accounting must include the full graph fan-out"
    );
    assert!(
        success.telemetry.source_peak_temporary_bytes[&bounded::TestWorkSource::GraphTraversal] > 0,
        "graph candidate state is charged to request memory"
    );
    assert!(success.telemetry.peak_temporary_bytes <= success_limits.memory);

    let mut low_work = success_limits;
    low_work.work = 257;
    assert_limit(
        bounded::execute(
            db,
            &request(
                WIDE_GRAPH_SQL,
                params([("start", Value::Uuid(start))]),
                low_work,
                clock,
            ),
        )
        .expect_err("the same graph source must stop at its smaller request work boundary"),
        ReadFailureLimit::Work,
        257,
    );
}

fn assert_legacy_bfs_limit(error: contextdb_core::Error, expected: usize, path: &str) {
    assert!(
        matches!(error, contextdb_core::Error::BfsVisitedExceeded(limit) if limit == expected),
        "{path} must retain BfsVisitedExceeded({expected}), got {error:?}"
    );
}

/// An unrestricted ten-hop graph read succeeds beyond the historical
/// 100,000-visited-node guard when the request explicitly permits the work and
/// memory, while the same source obeys a smaller request budget.
#[test]
fn unrestricted_graph_traversal_obeys_request_budget_beyond_legacy_visited_ceiling() {
    let db = Database::open_memory();
    assert_eq!(
        MAX_VISITED, 100_000,
        "the public legacy BFS ceiling stays frozen"
    );
    let target_count = MAX_VISITED + 7;
    let fixture = create_bounded_fanout_graph_fixture(&db, 11, target_count, None);
    assert_eq!(fixture.targets, target_count);
    assert_eq!(fixture.deepest_depth, 10);
    let edge_types = ["LINKS".to_owned()];
    assert_legacy_bfs_limit(
        db.query_bfs(
            fixture.start,
            Some(&edge_types),
            Direction::Outgoing,
            10,
            db.snapshot(),
        )
        .expect_err("legacy public admin BFS keeps its exact visited ceiling"),
        MAX_VISITED,
        "Database::query_bfs admin path",
    );
    assert_legacy_bfs_limit(
        db.execute(
            WIDE_GRAPH_SQL,
            &params([("start", Value::Uuid(fixture.start))]),
        )
        .expect_err("legacy admin SQL traversal keeps its exact visited ceiling"),
        MAX_VISITED,
        "legacy SQL admin path",
    );
    assert_wide_graph_is_request_bounded(
        &db,
        fixture.start,
        fixture.targets,
        &ManualClock::default(),
    );
}

/// A context-scoped ten-hop graph read succeeds beyond the historical
/// 10,000-visited-node guard when request budgets permit it, and still stops at
/// a smaller request work boundary.
#[test]
fn scoped_graph_traversal_obeys_request_budget_beyond_legacy_visited_ceiling() {
    let target_count = 10_000_usize + 3;
    let context = graph_uuid(23);
    let owner = Database::open_memory();
    let fixture = create_bounded_fanout_graph_fixture(&owner, 23, target_count, Some(context));
    assert_eq!(fixture.targets, target_count);
    assert_eq!(fixture.deepest_depth, 10);
    assert_eq!(
        owner
            .execute(
                "SELECT id FROM nodes WHERE context_id = $context",
                &params([("context", Value::Uuid(context))]),
            )
            .expect("relational node metadata survives fixture seeding")
            .rows
            .len(),
        target_count + 1,
        "the scoped start and every scoped target have context-bearing node rows"
    );
    assert_eq!(
        owner
            .execute(
                "SELECT id FROM edges WHERE context_id = $context",
                &params([("context", Value::Uuid(context))]),
            )
            .expect("relational edge metadata survives fixture seeding")
            .rows
            .len(),
        target_count,
        "every graph target has a context-bearing relational edge row"
    );
    let scoped = owner.scoped_with_contexts(BTreeSet::from([ContextId::new(context)]));
    let edge_types = ["LINKS".to_owned()];
    assert_legacy_bfs_limit(
        scoped
            .query_bfs(
                fixture.start,
                Some(&edge_types),
                Direction::Outgoing,
                10,
                scoped.snapshot(),
            )
            .expect_err("legacy public scoped BFS keeps its exact visited ceiling"),
        10_000,
        "Database::query_bfs scoped/gated path",
    );
    assert_legacy_bfs_limit(
        scoped
            .execute(
                WIDE_GRAPH_SQL,
                &params([("start", Value::Uuid(fixture.start))]),
            )
            .expect_err("legacy scoped SQL traversal keeps its exact visited ceiling"),
        10_000,
        "legacy SQL scoped/gated path",
    );
    assert_wide_graph_is_request_bounded(
        &scoped,
        fixture.start,
        fixture.targets,
        &ManualClock::default(),
    );
}

fn vector_for(seed: u64) -> Vec<f32> {
    let slope = 1.0 / (seed.saturating_add(1) as f32);
    let norm = (1.0 + slope * slope).sqrt();
    vec![1.0 / norm, slope / norm]
}

fn create_vector_table(db: &Database, table: &str, include_context: bool) {
    let columns = if include_context {
        "id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, embedding VECTOR(2), payload TEXT"
    } else {
        "id UUID PRIMARY KEY, embedding VECTOR(2), payload TEXT"
    };
    db.execute(
        &format!("CREATE TABLE {table} ({columns})"),
        &HashMap::new(),
    )
    .expect("create vector table");
}

fn seed_vectors(
    db: &Database,
    table: &str,
    rows: usize,
    context: Option<Uuid>,
    uuid_namespace: u32,
) -> Vec<Uuid> {
    let ids = (0..rows)
        .map(|ordinal| fixture_uuid(uuid_namespace, ordinal as u64))
        .collect::<Vec<_>>();
    let tx = db.begin_or_panic();
    for (index, id) in ids.iter().enumerate() {
        let mut row = HashMap::from([
            ("id".to_owned(), Value::Uuid(*id)),
            (
                "payload".to_owned(),
                Value::Text(dynamic_payload(index as u64 + rows as u64, 13 + index % 5)),
            ),
        ]);
        if let Some(context) = context {
            row.insert("context_id".to_owned(), Value::Uuid(context));
        }
        let row_id = db.insert_row(tx, table, row).expect("insert vector row");
        db.insert_vector(
            tx,
            VectorIndexRef::new(table, "embedding"),
            row_id,
            vector_for(index as u64),
        )
        .expect("insert vector");
    }
    db.commit(tx).expect("commit vectors");
    ids
}

fn uuid_ids(result: &contextdb_engine::QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.ends_with(".id"))
        .expect("result id column");
    result
        .rows
        .iter()
        .map(|row| match row[id_column] {
            Value::Uuid(id) => id,
            ref other => panic!("id must be UUID, got {other:?}"),
        })
        .collect()
}

fn cursor_page_ids(page: &contextdb_core::read_contract::CursorPage) -> Vec<i64> {
    let id_column = page
        .columns
        .iter()
        .position(|column| column == "id" || column.ends_with(".id"))
        .expect("cursor page contains id");
    page.rows
        .iter()
        .map(|row| match row[id_column] {
            Value::Int64(id) => id,
            ref other => panic!("cursor id must be INTEGER, got {other:?}"),
        })
        .collect()
}

#[test]
fn bounded_cursor_type_is_send_sync_static_without_a_database_borrow() {
    fn requires_owned_static_cursor<T: Send + Sync + 'static>() {}
    requires_owned_static_cursor::<bounded::TestCursor>();
}

#[test]
fn cursor_owns_database_and_execution_resources_after_the_caller_drops_its_handle() {
    let database = Arc::new(Database::open_memory());
    seed_relational(&database, "owned_cursor_rows", 7, 19);
    let database_lifetime = Arc::downgrade(&database);
    let clock = ManualClock::default();
    let mut limits = roomy_limits();
    limits.result_rows = 2;
    limits.cursor_page_rows = 2;
    let mut opened = bounded::open_cursor(
        Arc::clone(&database),
        &request(
            "SELECT id FROM owned_cursor_rows ORDER BY id",
            HashMap::new(),
            limits,
            &clock,
        ),
    )
    .expect("open a cursor whose state owns the database Arc");
    assert_eq!(cursor_page_ids(&opened.first_page), vec![0, 1]);
    drop(database);
    assert!(
        database_lifetime.upgrade().is_some(),
        "the cursor-owned execution state keeps the stores and snapshot resources alive"
    );
    let resumed = opened
        .cursor
        .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
        .expect("fetch remains safe after the caller drops its Database handle");
    assert_eq!(cursor_page_ids(&resumed.page), vec![2, 3]);
    opened
        .cursor
        .close()
        .expect("production close releases owned state");
    assert!(
        database_lifetime.upgrade().is_none(),
        "terminal release drops the cursor-owned Database Arc without self-reference tricks"
    );
}

fn create_rank_candidate_fixture(db: &Database) -> Vec<Uuid> {
    db.execute(
        "CREATE TABLE ranked_outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
        &HashMap::new(),
    )
    .expect("create rank outcomes");
    db.execute(
        "CREATE INDEX ranked_outcomes_decision_idx ON ranked_outcomes(decision_id)",
        &HashMap::new(),
    )
    .expect("index rank outcomes");
    db.execute(
        "CREATE TABLE ranked_decisions (
            id UUID PRIMARY KEY,
            confidence REAL,
            embedding VECTOR(2) RANK_POLICY (
                JOIN ranked_outcomes ON decision_id,
                FORMULA 'coalesce({confidence}, 0.0) * coalesce({success}, 0.0)',
                SORT_KEY confidence_weighted
            )
        )",
        &HashMap::new(),
    )
    .expect("create rank decisions");

    let mut ids = Vec::new();
    for index in 0..17_u128 {
        let id = Uuid::from_u128(0xABCD_0000_0000_0000_0000_0000_0000_0000 + index);
        ids.push(id);
        db.execute(
            "INSERT INTO ranked_decisions (id, confidence, embedding) VALUES ($id, $confidence, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("confidence", Value::Float64((index + 1) as f64 / 17.0)),
                ("embedding", Value::Vector(vector_for(index as u64))),
            ]),
        )
        .expect("insert rank decision");
        db.execute(
            "INSERT INTO ranked_outcomes (id, decision_id, success) VALUES ($id, $decision_id, $success)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0xABCE_0000 + index))),
                ("decision_id", Value::Uuid(id)),
                ("success", Value::Bool(index % 3 != 0)),
            ]),
        )
        .expect("insert rank outcome");
    }
    ids
}

fn assert_scoped_candidate_limits(
    db: &Database,
    sql: &str,
    params: HashMap<String, Value>,
    source: bounded::TestWorkSource,
    profile: &bounded::TestResult,
    clock: &ManualClock,
) {
    let source_work = profile
        .telemetry
        .source_work
        .get(&source)
        .copied()
        .unwrap_or_default();
    let access_work = profile
        .telemetry
        .source_work
        .get(&bounded::TestWorkSource::AccessControl)
        .copied()
        .unwrap_or_default();
    assert!(
        source_work > 1,
        "candidate source has multiple charged items"
    );
    assert!(
        access_work > 0,
        "visibility checks use the same work context"
    );

    let shared_work_limit = source_work.max(access_work);
    assert!(
        shared_work_limit < source_work.saturating_add(access_work),
        "one shared budget is stricter than separate per-operator budgets"
    );
    let mut low_work = roomy_limits();
    low_work.work = shared_work_limit;
    assert_limit(
        bounded::execute(db, &request(sql, params.clone(), low_work, clock))
            .expect_err("candidate preparation must stop at the shared work boundary"),
        ReadFailureLimit::Work,
        shared_work_limit,
    );

    let source_peak = profile
        .telemetry
        .source_peak_temporary_bytes
        .get(&source)
        .copied()
        .unwrap_or_default();
    let access_peak = profile
        .telemetry
        .source_peak_temporary_bytes
        .get(&bounded::TestWorkSource::AccessControl)
        .copied()
        .unwrap_or_default();
    assert!(source_peak > 0, "candidate source exposes request memory");
    assert!(access_peak > 0, "visibility checks expose request memory");
    let candidate_peak = source_peak.max(access_peak);
    assert!(
        candidate_peak > 1,
        "candidate and visibility state expose their request-memory peak"
    );
    let mut low_memory = roomy_limits();
    low_memory.memory = candidate_peak - 1;
    assert_limit(
        bounded::execute(db, &request(sql, params, low_memory, clock))
            .expect_err("candidate preparation must stop at the shared memory boundary"),
        ReadFailureLimit::Memory,
        low_memory.memory,
    );
}

/// Brute-force, HNSW, semantic/rank preparation, and access-controlled
/// candidates all spend one request context.  Different dataset sizes and
/// principal-visible rows reject constant-result and fixture-keyed behavior.
#[test]
fn vector_rank_and_access_candidate_work_is_bounded_before_materialization() {
    let clock = ManualClock::default();
    let brute = Database::open_memory();
    create_vector_table(&brute, "brute_vectors", false);
    let brute_ids = seed_vectors(&brute, "brute_vectors", 37, None, 101);
    let brute_sql = "SELECT id FROM brute_vectors ORDER BY embedding <=> $query LIMIT 3";
    let brute_outcome = bounded_success(
        &brute,
        &request(
            brute_sql,
            params([("query", Value::Vector(vector_for(0)))]),
            roomy_limits(),
            &clock,
        ),
    );
    assert_eq!(brute_outcome.result.rows.len(), 3);
    assert!(uuid_ids(&brute_outcome.result).contains(&brute_ids[0]));
    assert!(
        brute_outcome
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::VectorCandidates)
            .copied()
            .unwrap_or_default()
            >= 37,
        "brute-force candidates are charged before the top-k is materialized"
    );
    let mut too_little_vector_work = roomy_limits();
    too_little_vector_work.work = 36;
    assert_limit(
        bounded::execute(
            &brute,
            &request(
                brute_sql,
                params([("query", Value::Vector(vector_for(0)))]),
                too_little_vector_work,
                &clock,
            ),
        )
        .expect_err("brute-force candidates cannot bypass request work through a final LIMIT"),
        ReadFailureLimit::Work,
        36,
    );

    let hnsw = Database::open_memory();
    create_vector_table(&hnsw, "hnsw_vectors", false);
    let hnsw_ids = seed_vectors(&hnsw, "hnsw_vectors", 1_003, None, 102);
    let hnsw_sql = "SELECT id FROM hnsw_vectors ORDER BY embedding <=> $query LIMIT 5";
    hnsw.execute(hnsw_sql, &params([("query", Value::Vector(vector_for(0)))]))
        .expect("prebuild persistent HNSW state before the bounded request");
    assert!(hnsw.__debug_last_query_vector_used_hnsw_for_test());
    let hnsw_outcome = bounded_success(
        &hnsw,
        &request(
            hnsw_sql,
            params([("query", Value::Vector(vector_for(0)))]),
            roomy_limits(),
            &clock,
        ),
    );
    assert_eq!(hnsw_outcome.result.rows.len(), 5);
    assert!(uuid_ids(&hnsw_outcome.result).contains(&hnsw_ids[0]));
    assert!(
        hnsw.__debug_last_query_vector_used_hnsw_for_test(),
        "the bounded production path must exercise HNSW above its real threshold"
    );
    assert!(
        hnsw_outcome
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::VectorCandidates)
            .copied()
            .unwrap_or_default()
            > 0
    );

    let ranked = Database::open_memory();
    let ranked_ids = create_rank_candidate_fixture(&ranked);
    let ranked_outcome = bounded_success(
        &ranked,
        &request(
            "SELECT id FROM ranked_decisions ORDER BY embedding <=> $query USE RANK confidence_weighted LIMIT 4",
            params([("query", Value::Vector(vector_for(0)))]),
            roomy_limits(),
            &clock,
        ),
    );
    assert_eq!(ranked_outcome.result.rows.len(), 4);
    assert!(
        uuid_ids(&ranked_outcome.result)
            .into_iter()
            .all(|id| ranked_ids.contains(&id)),
        "ranked output must come from this runtime-generated vector candidate set"
    );
    assert!(
        ranked_outcome
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::RankCandidates)
            .copied()
            .unwrap_or_default()
            > 0,
        "rank/semantic candidate preparation is charged before rank materialization"
    );

    let temporary = TempDir::new().expect("tempdir");
    let path = temporary.path().join("bounded-access-vectors.db");
    let visible_context = Uuid::from_u128(1);
    let hidden_context = Uuid::from_u128(2);
    let visible_graph_start = fixture_uuid(200, 0);
    let visible_graph_target = fixture_uuid(200, 1);
    let hidden_graph_target = fixture_uuid(200, 2);
    {
        let owner = Database::open(&path).expect("owner open");
        owner
            .execute(
                "CREATE TABLE secured_rows (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, payload TEXT)",
                &HashMap::new(),
            )
            .expect("create access-controlled relational table");
        for (context_namespace, context, count) in [
            (201_u32, visible_context, 11_usize),
            (202_u32, hidden_context, 13),
        ] {
            for index in 0..count {
                owner
                    .execute(
                        "INSERT INTO secured_rows (id, context_id, payload) VALUES ($id, $context, $payload)",
                        &params([
                            ("id", Value::Uuid(fixture_uuid(context_namespace, index as u64))),
                            ("context", Value::Uuid(context)),
                            (
                                "payload",
                                Value::Text(dynamic_payload(index as u64, 21 + count)),
                            ),
                        ]),
                    )
                    .expect("insert access-controlled relational row");
            }
        }
        owner
            .execute(
                "CREATE TABLE secured_nodes (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID)",
                &HashMap::new(),
            )
            .expect("create access-controlled graph nodes");
        owner
            .execute(
                "CREATE TABLE secured_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
                &HashMap::new(),
            )
            .expect("create access-controlled graph edges");
        for (id, context) in [
            (visible_graph_start, visible_context),
            (visible_graph_target, visible_context),
            (hidden_graph_target, hidden_context),
        ] {
            owner
                .execute(
                    "INSERT INTO secured_nodes (id, context_id) VALUES ($id, $context)",
                    &params([("id", Value::Uuid(id)), ("context", Value::Uuid(context))]),
                )
                .expect("insert access-controlled graph node");
        }
        for (edge_ordinal, target) in [visible_graph_target, hidden_graph_target]
            .into_iter()
            .enumerate()
        {
            owner
                .execute(
                    "INSERT INTO secured_edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'LINKS')",
                    &params([
                        ("id", Value::Uuid(fixture_uuid(203, edge_ordinal as u64))),
                        ("source", Value::Uuid(visible_graph_start)),
                        ("target", Value::Uuid(target)),
                    ]),
                )
                .expect("insert access-controlled graph edge");
        }
        create_vector_table(&owner, "secured_vectors", true);
        let visible = seed_vectors(&owner, "secured_vectors", 29, Some(visible_context), 103);
        let hidden = seed_vectors(&owner, "secured_vectors", 31, Some(hidden_context), 104);
        assert_ne!(
            visible[0], hidden[0],
            "runtime-generated contexts have distinct identities"
        );
        owner.close().expect("owner close");
    }
    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(visible_context)]))
            .expect("scoped open");
    let scoped_relational = bounded_success(
        &scoped,
        &request(
            "SELECT id FROM secured_rows LIMIT 5",
            HashMap::new(),
            roomy_limits(),
            &clock,
        ),
    );
    assert_eq!(scoped_relational.result.rows.len(), 5);
    assert!(
        scoped_relational
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::TableScan)
            .copied()
            .unwrap_or_default()
            > 0
    );
    assert!(
        scoped_relational
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::AccessControl)
            .copied()
            .unwrap_or_default()
            > 0,
        "relational access filtering is request work, not a free post-processing step"
    );
    for id in uuid_ids(&scoped_relational.result) {
        let row = scoped
            .point_lookup("secured_rows", "id", &Value::Uuid(id), scoped.snapshot())
            .expect("visible relational result lookup")
            .expect("bounded relational result stays visible");
        assert_eq!(
            row.values.get("context_id"),
            Some(&Value::Uuid(visible_context))
        );
    }
    assert_scoped_candidate_limits(
        &scoped,
        "SELECT id FROM secured_rows LIMIT 5",
        HashMap::new(),
        bounded::TestWorkSource::TableScan,
        &scoped_relational,
        &clock,
    );
    let scoped_graph = bounded_success(
        &scoped,
        &request(
            "SELECT target FROM GRAPH_TABLE(secured_edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $start COLUMNS (b.id AS target))",
            params([("start", Value::Uuid(visible_graph_start))]),
            roomy_limits(),
            &clock,
        ),
    );
    assert_eq!(scoped_graph.result.rows.len(), 1);
    let graph_target_column = scoped_graph
        .result
        .columns
        .iter()
        .position(|column| column == "target")
        .expect("graph projection target column");
    assert_eq!(
        scoped_graph.result.rows[0][graph_target_column],
        Value::Uuid(visible_graph_target),
        "the hidden graph target must never leak through traversal materialization"
    );
    assert!(
        scoped_graph
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::GraphTraversal)
            .copied()
            .unwrap_or_default()
            > 0
    );
    assert!(
        scoped_graph
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::AccessControl)
            .copied()
            .unwrap_or_default()
            > 0,
        "graph edge/node visibility checks spend the same request work context"
    );
    assert_scoped_candidate_limits(
        &scoped,
        "SELECT target FROM GRAPH_TABLE(secured_edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $start COLUMNS (b.id AS target))",
        params([("start", Value::Uuid(visible_graph_start))]),
        bounded::TestWorkSource::GraphTraversal,
        &scoped_graph,
        &clock,
    );
    let secured = bounded_success(
        &scoped,
        &request(
            "SELECT id FROM secured_vectors ORDER BY embedding <=> $query LIMIT 5",
            params([("query", Value::Vector(vector_for(0)))]),
            roomy_limits(),
            &clock,
        ),
    );
    assert_eq!(secured.result.rows.len(), 5);
    assert!(
        secured
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::AccessControl)
            .copied()
            .unwrap_or_default()
            > 0,
        "access filtering is charged to the vector request, not performed after a free candidate build"
    );
    assert!(
        secured
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::VectorCandidates)
            .copied()
            .unwrap_or_default()
            > 0
    );
    for id in uuid_ids(&secured.result) {
        let row = scoped
            .point_lookup("secured_vectors", "id", &Value::Uuid(id), scoped.snapshot())
            .expect("visible result lookup")
            .expect("bounded result must stay visible to this principal");
        assert_eq!(
            row.values.get("context_id"),
            Some(&Value::Uuid(visible_context))
        );
    }
    assert_scoped_candidate_limits(
        &scoped,
        "SELECT id FROM secured_vectors ORDER BY embedding <=> $query LIMIT 5",
        params([("query", Value::Vector(vector_for(0)))]),
        bounded::TestWorkSource::VectorCandidates,
        &secured,
        &clock,
    );
}

struct ExactLimitMatrixCase<'db> {
    name: &'static str,
    db: &'db Database,
    sql: &'static str,
    params: HashMap<String, Value>,
    source: bounded::TestWorkSource,
    touch: bounded::TestSourceTouch,
    uses_hnsw: bool,
}

struct CursorCancellationProbe {
    cancellation: OwnerReadCancellation,
    source_touches: AtomicU64,
    cancellation_calls: AtomicU64,
}

impl CursorCancellationProbe {
    fn new(cancellation: OwnerReadCancellation) -> Self {
        Self {
            cancellation,
            source_touches: AtomicU64::new(0),
            cancellation_calls: AtomicU64::new(0),
        }
    }
}

impl bounded::ExecutionProbe for CursorCancellationProbe {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, completed_items: u64) {
        assert_eq!(touch, bounded::TestSourceTouch::IndexEntry);
        assert_eq!(
            completed_items, 0,
            "production must observe cancellation before a second source touch"
        );
        assert_eq!(
            self.source_touches.fetch_add(1, Ordering::SeqCst),
            0,
            "a cancelled fetch reaches exactly one pre-touch poison point"
        );
        self.cancellation.cancel();
    }

    fn cancellation_observed(&self, _completed_work: u64) {
        self.cancellation_calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct HnswCandidatePathProbe {
    events: AtomicU64,
}

impl contextdb_vector::hnsw::HnswCandidateObserver for HnswCandidatePathProbe {
    fn before_candidate_distance(&self, event: contextdb_vector::hnsw::HnswCandidateDistanceEvent) {
        let expected = self.events.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            event.completed_candidates(),
            expected,
            "the vector source emits contiguous pre-distance candidate ordinals"
        );
    }
}

#[test]
fn hnsw_candidate_observer_is_inside_the_real_prebuilt_distance_loop() {
    let db = Database::open_memory();
    create_vector_table(&db, "observer_hnsw_vectors", false);
    seed_vectors(&db, "observer_hnsw_vectors", 1_003, None, 107);
    let sql = "SELECT id FROM observer_hnsw_vectors ORDER BY embedding <=> $query LIMIT 5";
    db.execute(sql, &params([("query", Value::Vector(vector_for(0)))]))
        .expect("prebuild persistent HNSW state before observing request work");
    assert!(db.__debug_last_query_vector_used_hnsw_for_test());

    let observer = Arc::new(HnswCandidatePathProbe::default());
    let vector_observer: Arc<dyn contextdb_vector::hnsw::HnswCandidateObserver> = observer.clone();
    let observed_result =
        contextdb_vector::hnsw::with_hnsw_candidate_observer(vector_observer, || {
            db.execute(sql, &params([("query", Value::Vector(vector_for(1)))]))
        })
        .expect("sealed source observer wraps a real prebuilt HNSW search");
    let trace = db
        .__debug_last_query_vector_trace_for_test()
        .expect("observed HNSW query publishes its trace");
    let observed_events = observer.events.load(Ordering::SeqCst);
    assert!(trace.used_hnsw);
    assert!(
        observed_events >= trace.hnsw_candidate_count as u64,
        "pre-distance events arise inside search and cover every returned candidate"
    );

    let ordinary_result = db
        .execute(sql, &params([("query", Value::Vector(vector_for(1)))]))
        .expect("ordinary HNSW search remains behaviorally unchanged");
    assert_eq!(ordinary_result.rows, observed_result.rows);
    assert_eq!(
        observer.events.load(Ordering::SeqCst),
        observed_events,
        "the dynamic observer handoff does not leak into a later request"
    );
}

struct SourceLoopPoisonProbe {
    clock: ManualClock,
    accountant: Arc<MemoryAccountant>,
    baseline_accounted_bytes: usize,
    poison_before_work_at: Option<u64>,
    poison_reservation_above: Option<u64>,
    poison_source_after: Option<(bounded::TestSourceTouch, u64)>,
    cancel_on_first_touch: Option<(bounded::TestSourceTouch, OwnerReadCancellation)>,
    work_calls: AtomicU64,
    source_touches: Mutex<BTreeMap<bounded::TestSourceTouch, u64>>,
    peak_temporary_accounted_bytes: AtomicU64,
    cancellation_calls: AtomicU64,
    cancelled: AtomicBool,
}

impl SourceLoopPoisonProbe {
    fn new(
        clock: ManualClock,
        accountant: Arc<MemoryAccountant>,
        baseline_accounted_bytes: usize,
    ) -> Self {
        Self {
            clock,
            accountant,
            baseline_accounted_bytes,
            poison_before_work_at: None,
            poison_reservation_above: None,
            poison_source_after: None,
            cancel_on_first_touch: None,
            work_calls: AtomicU64::new(0),
            source_touches: Mutex::new(BTreeMap::new()),
            peak_temporary_accounted_bytes: AtomicU64::new(0),
            cancellation_calls: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        }
    }

    fn poison_before_work_at(mut self, work: u64) -> Self {
        self.poison_before_work_at = Some(work);
        self
    }

    fn poison_reservation_above(mut self, bytes: u64) -> Self {
        self.poison_reservation_above = Some(bytes);
        self
    }

    fn poison_source_after(mut self, touch: bounded::TestSourceTouch, items: u64) -> Self {
        self.poison_source_after = Some((touch, items));
        self
    }

    fn cancel_on_first_touch(
        mut self,
        touch: bounded::TestSourceTouch,
        cancellation: OwnerReadCancellation,
    ) -> Self {
        self.cancel_on_first_touch = Some((touch, cancellation));
        self
    }

    fn work_units(&self) -> u64 {
        self.work_calls.load(Ordering::SeqCst)
    }

    fn source_items(&self, touch: bounded::TestSourceTouch) -> u64 {
        self.source_touches
            .lock()
            .expect("source touch mutex")
            .get(&touch)
            .copied()
            .unwrap_or_default()
    }

    fn peak_temporary_accounted_bytes(&self) -> u64 {
        self.peak_temporary_accounted_bytes.load(Ordering::SeqCst)
    }
}

impl bounded::ExecutionProbe for SourceLoopPoisonProbe {
    fn before_work(&self, _source: bounded::TestWorkSource, completed_work: u64) {
        if self.poison_before_work_at == Some(completed_work) {
            panic!(
                "bounded source attempted work item {completed_work} after the request budget had to stop it"
            );
        }
        self.clock.advance(1);
        self.work_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, completed_items: u64) {
        if let Some((poisoned_touch, items)) = self.poison_source_after
            && touch == poisoned_touch
            && completed_items == items
        {
            panic!(
                "bounded source touched {touch:?} item {completed_items} after its poison boundary"
            );
        }
        let mut touched = self.source_touches.lock().expect("source touch mutex");
        let observed = touched.entry(touch).or_default();
        assert_eq!(
            *observed, completed_items,
            "the production source loop reports contiguous per-source progress"
        );
        *observed += 1;
        if let Some((cancel_touch, cancellation)) = &self.cancel_on_first_touch
            && touch == *cancel_touch
            && !self.cancelled.swap(true, Ordering::SeqCst)
        {
            cancellation.cancel();
        }
    }

    fn before_temporary_reservation(
        &self,
        _source: bounded::TestWorkSource,
        requested_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        if let Some(maximum) = self.poison_reservation_above
            && held_temporary_bytes.saturating_add(requested_bytes) > maximum
        {
            panic!(
                "bounded read reserved temporary memory before refusing its {maximum}-byte request ceiling"
            );
        }
    }

    fn after_temporary_reservation(
        &self,
        _source: bounded::TestWorkSource,
        _reserved_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        let actually_accounted = self
            .accountant
            .usage()
            .used
            .saturating_sub(self.baseline_accounted_bytes) as u64;
        assert!(
            actually_accounted >= held_temporary_bytes,
            "the bounded request's claimed temporary state is reserved in the database accountant"
        );
        self.peak_temporary_accounted_bytes
            .fetch_max(actually_accounted, Ordering::SeqCst);
    }

    fn cancellation_observed(&self, _completed_work: u64) {
        self.cancellation_calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_exact_n_and_n_minus_one_matrix_case(
    case: ExactLimitMatrixCase<'_>,
    accountant: &Arc<MemoryAccountant>,
) {
    let baseline = accountant.usage().used;
    let profile_clock = ManualClock::default();
    let profile_probe = Arc::new(SourceLoopPoisonProbe::new(
        profile_clock.clone(),
        Arc::clone(accountant),
        baseline,
    ));
    let mut profile_request = request(
        case.sql,
        case.params.clone(),
        roomy_limits(),
        &profile_clock,
    );
    profile_request.probe = Some(profile_probe.clone());
    let profile = bounded_success(case.db, &profile_request);
    let exact_work = profile_probe.work_units();
    let exact_memory = profile_probe.peak_temporary_accounted_bytes();
    let source_items = profile_probe.source_items(case.touch);
    assert!(exact_work > 0, "{} performs real charged work", case.name);
    assert!(
        exact_memory > 0,
        "{} reserves temporary state in the database accountant",
        case.name
    );
    assert!(
        source_items > 0,
        "{} reaches its real source loop",
        case.name
    );
    assert_eq!(
        profile.telemetry.work_units, exact_work,
        "{} telemetry agrees with independently observed source work",
        case.name
    );
    assert!(
        profile
            .telemetry
            .source_work
            .get(&case.source)
            .copied()
            .unwrap_or_default()
            > 0,
        "{} publishes its source classification as well as touching its loop",
        case.name
    );
    assert_eq!(
        accountant.usage().used,
        baseline,
        "{} releases ordinary-read temporary reservations before returning",
        case.name
    );
    if case.uses_hnsw {
        assert!(
            case.db.__debug_last_query_vector_used_hnsw_for_test(),
            "{} reaches the HNSW candidate loop rather than the brute-force fallback",
            case.name
        );
        let trace = case
            .db
            .__debug_last_query_vector_trace_for_test()
            .expect("the HNSW source publishes its ordinary debug trace");
        assert!(
            source_items >= trace.hnsw_candidate_count as u64,
            "the sealed pre-distance observer sees at least every returned HNSW candidate"
        );
    }

    let mut exact_work_limits = roomy_limits();
    exact_work_limits.work = exact_work;
    bounded_success(
        case.db,
        &request(
            case.sql,
            case.params.clone(),
            exact_work_limits,
            &ManualClock::default(),
        ),
    );
    let one_less_work_clock = ManualClock::default();
    let one_less_work_probe = Arc::new(
        SourceLoopPoisonProbe::new(
            one_less_work_clock.clone(),
            Arc::clone(accountant),
            baseline,
        )
        .poison_before_work_at(exact_work - 1)
        .poison_source_after(case.touch, source_items),
    );
    let mut one_less_work_limits = roomy_limits();
    one_less_work_limits.work = exact_work - 1;
    let mut one_less_work_request = request(
        case.sql,
        case.params.clone(),
        one_less_work_limits,
        &one_less_work_clock,
    );
    one_less_work_request.probe = Some(one_less_work_probe.clone());
    assert_limit(
        bounded::execute(case.db, &one_less_work_request)
            .expect_err("N-1 work must stop before the next source touch"),
        ReadFailureLimit::Work,
        exact_work - 1,
    );
    assert_eq!(
        one_less_work_probe.work_units(),
        exact_work - 1,
        "{} refuses before performing work unit N",
        case.name
    );

    let mut exact_memory_limits = roomy_limits();
    exact_memory_limits.memory = exact_memory;
    bounded_success(
        case.db,
        &request(
            case.sql,
            case.params.clone(),
            exact_memory_limits,
            &ManualClock::default(),
        ),
    );
    let one_less_memory_clock = ManualClock::default();
    let one_less_memory_probe = Arc::new(
        SourceLoopPoisonProbe::new(
            one_less_memory_clock.clone(),
            Arc::clone(accountant),
            baseline,
        )
        .poison_reservation_above(exact_memory - 1),
    );
    let mut one_less_memory_limits = roomy_limits();
    one_less_memory_limits.memory = exact_memory - 1;
    let mut one_less_memory_request = request(
        case.sql,
        case.params.clone(),
        one_less_memory_limits,
        &one_less_memory_clock,
    );
    one_less_memory_request.probe = Some(one_less_memory_probe);
    assert_limit(
        bounded::execute(case.db, &one_less_memory_request)
            .expect_err("N-1 memory must refuse before the accountant reservation"),
        ReadFailureLimit::Memory,
        exact_memory - 1,
    );
    assert_eq!(
        accountant.usage().used,
        baseline,
        "{} leaves no database-accounted reservation after a memory refusal",
        case.name
    );

    let exact_active_clock = ManualClock::default();
    let exact_active_probe = Arc::new(SourceLoopPoisonProbe::new(
        exact_active_clock.clone(),
        Arc::clone(accountant),
        baseline,
    ));
    let mut exact_active_limits = roomy_limits();
    exact_active_limits.active_ms = exact_work;
    let mut exact_active_request = request(
        case.sql,
        case.params.clone(),
        exact_active_limits,
        &exact_active_clock,
    );
    exact_active_request.probe = Some(exact_active_probe);
    bounded_success(case.db, &exact_active_request);
    let one_less_active_clock = ManualClock::default();
    let one_less_active_probe = Arc::new(
        SourceLoopPoisonProbe::new(
            one_less_active_clock.clone(),
            Arc::clone(accountant),
            baseline,
        )
        .poison_before_work_at(exact_work - 1),
    );
    let mut one_less_active_limits = roomy_limits();
    one_less_active_limits.active_ms = exact_work - 1;
    let mut one_less_active_request = request(
        case.sql,
        case.params.clone(),
        one_less_active_limits,
        &one_less_active_clock,
    );
    one_less_active_request.probe = Some(one_less_active_probe);
    assert_limit(
        bounded::execute(case.db, &one_less_active_request)
            .expect_err("N-1 active time must stop before the next source touch"),
        ReadFailureLimit::ActiveMs,
        exact_work - 1,
    );

    let cancellation_clock = ManualClock::default();
    let cancellation = OwnerReadCancellation::new();
    let cancellation_probe = Arc::new(
        SourceLoopPoisonProbe::new(cancellation_clock.clone(), Arc::clone(accountant), baseline)
            .cancel_on_first_touch(case.touch, cancellation.clone())
            .poison_source_after(case.touch, 1),
    );
    let mut cancellation_request =
        request(case.sql, case.params, roomy_limits(), &cancellation_clock);
    cancellation_request.cancellation = cancellation;
    cancellation_request.probe = Some(cancellation_probe.clone());
    assert!(matches!(
        bounded::execute(case.db, &cancellation_request),
        Err(bounded::TestError::Cancelled)
    ));
    assert_eq!(
        cancellation_probe.source_items(case.touch),
        1,
        "{} observes cancellation inside its own source loop",
        case.name
    );
    assert_eq!(
        cancellation_probe.cancellation_calls.load(Ordering::SeqCst),
        1,
        "{} cancels exactly once without waiting for an eager collection",
        case.name
    );
}

/// Each distinct source loop has an exact N/N-1 work and memory proof.  The
/// probe's touches, allocation poison, manual clock, and cancellation are
/// independent of kernel telemetry, so final-result counters cannot make an
/// eager implementation appear bounded.
#[test]
fn every_bounded_source_has_exact_work_memory_and_cancellation_matrix() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(Arc::clone(&accountant));
    seed_relational(&db, "matrix_events", 37, 17);
    db.execute(
        "CREATE INDEX matrix_events_score_idx ON matrix_events(score)",
        &HashMap::new(),
    )
    .expect("index matrix events");
    let graph = create_bounded_fanout_graph_fixture(&db, 41, 37, None);

    create_vector_table(&db, "matrix_brute_vectors", false);
    seed_vectors(&db, "matrix_brute_vectors", 37, None, 105);
    create_vector_table(&db, "matrix_hnsw_vectors", false);
    seed_vectors(&db, "matrix_hnsw_vectors", 1_003, None, 106);
    create_rank_candidate_fixture(&db);

    let visible_context = graph_uuid(42);
    let hidden_context = graph_uuid(43);
    db.execute(
        "CREATE TABLE matrix_access_rows (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create access matrix table");
    for (uuid_namespace, context, rows) in [
        (204_u32, visible_context, 19_usize),
        (205_u32, hidden_context, 23),
    ] {
        for index in 0..rows {
            db.execute(
                "INSERT INTO matrix_access_rows (id, context_id, payload) VALUES ($id, $context, $payload)",
                &params([
                    ("id", Value::Uuid(fixture_uuid(uuid_namespace, index as u64))),
                    ("context", Value::Uuid(context)),
                    ("payload", Value::Text(dynamic_payload(index as u64, 23))),
                ]),
            )
            .expect("seed access matrix row");
        }
    }
    let scoped = db.scoped_with_contexts(BTreeSet::from([ContextId::new(visible_context)]));

    // Build and retain the legitimate persistent HNSW cache before any
    // measured request baseline. Request cleanup must release only request
    // state, never erase this index-owned cache to make accounting balance.
    let hnsw_sql = "SELECT id FROM matrix_hnsw_vectors ORDER BY embedding <=> $query LIMIT 5";
    db.execute(hnsw_sql, &params([("query", Value::Vector(vector_for(0)))]))
        .expect("prebuild persistent HNSW state outside the bounded request");
    assert!(db.__debug_last_query_vector_used_hnsw_for_test());
    let hnsw_index = VectorIndexRef::new("matrix_hnsw_vectors", "embedding");
    assert_eq!(db.__debug_vector_hnsw_len(hnsw_index.clone()), Some(1_003));
    let retained_index_baseline = accountant.usage().used;

    let cases = [
        ExactLimitMatrixCase {
            name: "plain table scan",
            db: &db,
            sql: "SELECT id FROM matrix_events LIMIT 3",
            params: HashMap::new(),
            source: bounded::TestWorkSource::TableScan,
            touch: bounded::TestSourceTouch::TableRow,
            uses_hnsw: false,
        },
        ExactLimitMatrixCase {
            name: "index range",
            db: &db,
            sql: "SELECT id FROM matrix_events WHERE score >= $low AND score < $high LIMIT 3",
            params: params([("low", Value::Int64(0)), ("high", Value::Int64(37))]),
            source: bounded::TestWorkSource::IndexRange,
            touch: bounded::TestSourceTouch::IndexEntry,
            uses_hnsw: false,
        },
        ExactLimitMatrixCase {
            name: "unindexed sort",
            db: &db,
            sql: "SELECT id FROM matrix_events ORDER BY payload LIMIT 3",
            params: HashMap::new(),
            source: bounded::TestWorkSource::UnindexedSort,
            touch: bounded::TestSourceTouch::SortCandidate,
            uses_hnsw: false,
        },
        ExactLimitMatrixCase {
            name: "graph traversal",
            db: &db,
            sql: WIDE_GRAPH_SQL,
            params: params([("start", Value::Uuid(graph.start))]),
            source: bounded::TestWorkSource::GraphTraversal,
            touch: bounded::TestSourceTouch::GraphEdge,
            uses_hnsw: false,
        },
        ExactLimitMatrixCase {
            name: "brute-force vector candidates",
            db: &db,
            sql: "SELECT id FROM matrix_brute_vectors ORDER BY embedding <=> $query LIMIT 3",
            params: params([("query", Value::Vector(vector_for(0)))]),
            source: bounded::TestWorkSource::VectorCandidates,
            touch: bounded::TestSourceTouch::BruteForceVectorCandidate,
            uses_hnsw: false,
        },
        ExactLimitMatrixCase {
            name: "HNSW vector candidates",
            db: &db,
            sql: hnsw_sql,
            params: params([("query", Value::Vector(vector_for(0)))]),
            source: bounded::TestWorkSource::VectorCandidates,
            touch: bounded::TestSourceTouch::HnswCandidate,
            uses_hnsw: true,
        },
        ExactLimitMatrixCase {
            name: "rank candidates",
            db: &db,
            sql: "SELECT id FROM ranked_decisions ORDER BY embedding <=> $query USE RANK confidence_weighted LIMIT 4",
            params: params([("query", Value::Vector(vector_for(0)))]),
            source: bounded::TestWorkSource::RankCandidates,
            touch: bounded::TestSourceTouch::RankCandidate,
            uses_hnsw: false,
        },
        ExactLimitMatrixCase {
            name: "access filtering",
            db: &scoped,
            sql: "SELECT id FROM matrix_access_rows LIMIT 3",
            params: HashMap::new(),
            source: bounded::TestWorkSource::AccessControl,
            touch: bounded::TestSourceTouch::AccessRow,
            uses_hnsw: false,
        },
    ];
    for case in cases {
        assert_exact_n_and_n_minus_one_matrix_case(case, &accountant);
    }
    assert_eq!(
        accountant.usage().used,
        retained_index_baseline,
        "bounded request cleanup preserves the prebuilt persistent HNSW cache"
    );
    assert_eq!(
        db.__debug_vector_hnsw_len(hnsw_index),
        Some(1_003),
        "request cleanup must not discard the retained HNSW graph"
    );
}

#[derive(Default)]
struct AdvancingProbe {
    clock: ManualClock,
    cancellation: Option<OwnerReadCancellation>,
    cancel_after_work: Option<u64>,
    before_work_calls: AtomicU64,
    cancellation_calls: AtomicU64,
    sources: Mutex<BTreeMap<bounded::TestWorkSource, u64>>,
}

impl bounded::ExecutionProbe for AdvancingProbe {
    fn before_work(&self, source: bounded::TestWorkSource, completed_work: u64) {
        self.clock.advance(1);
        *self
            .sources
            .lock()
            .expect("probe source mutex")
            .entry(source)
            .or_default() += 1;
        self.before_work_calls.fetch_add(1, Ordering::SeqCst);
        if self.cancel_after_work == Some(completed_work + 1) {
            self.cancellation
                .as_ref()
                .expect("cancel probe receives request cancellation")
                .cancel();
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {
        self.cancellation_calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// Active time is clocked work, never scheduler or wall time; cancellation
/// entered in a source batch wakes the kernel at the next charged unit rather
/// than waiting for eager collection to finish.
#[test]
fn active_time_and_cancellation_are_manual_clock_and_source_wake_driven() {
    let db = Database::open_memory();
    seed_relational(&db, "timed", 23, 7);
    let clock = ManualClock::default();
    let sql = "SELECT id FROM timed LIMIT 7";

    let mut no_wall_time_limit = roomy_limits();
    no_wall_time_limit.work = 7;
    no_wall_time_limit.result_rows = 7;
    no_wall_time_limit.active_ms = 1;
    let no_wall_time = bounded_success(
        &db,
        &request(sql, HashMap::new(), no_wall_time_limit, &clock),
    );
    assert_eq!(no_wall_time.result.rows.len(), 7);
    assert_eq!(
        clock.now_ms(),
        0,
        "scheduler time does not consume active time"
    );

    let active_clock = ManualClock::default();
    let active_probe = Arc::new(AdvancingProbe {
        clock: active_clock.clone(),
        ..Default::default()
    });
    let mut exact_active_limit = no_wall_time_limit;
    exact_active_limit.active_ms = 7;
    let mut exact_active_request = request(sql, HashMap::new(), exact_active_limit, &active_clock);
    exact_active_request.probe = Some(active_probe.clone());
    assert_eq!(
        bounded_success(&db, &exact_active_request)
            .result
            .rows
            .len(),
        7,
        "the exact manual active-time boundary succeeds"
    );
    let failing_active_clock = ManualClock::default();
    let failing_active_probe = Arc::new(AdvancingProbe {
        clock: failing_active_clock.clone(),
        ..Default::default()
    });
    let mut one_past_active_limit = exact_active_limit;
    one_past_active_limit.active_ms = 6;
    let mut one_past_active_request = request(
        sql,
        HashMap::new(),
        one_past_active_limit,
        &failing_active_clock,
    );
    one_past_active_request.probe = Some(failing_active_probe);
    assert_limit(
        bounded::execute(&db, &one_past_active_request)
            .expect_err("manual active time must refuse before a seven-row source completes"),
        ReadFailureLimit::ActiveMs,
        6,
    );

    let cancellation_clock = ManualClock::default();
    let cancellation = OwnerReadCancellation::new();
    let cancellation_probe = Arc::new(AdvancingProbe {
        clock: cancellation_clock.clone(),
        cancellation: Some(cancellation.clone()),
        cancel_after_work: Some(3),
        ..Default::default()
    });
    let mut cancellation_request =
        request(sql, HashMap::new(), roomy_limits(), &cancellation_clock);
    cancellation_request.cancellation = cancellation;
    cancellation_request.probe = Some(cancellation_probe.clone());
    assert!(matches!(
        bounded::execute(&db, &cancellation_request),
        Err(bounded::TestError::Cancelled)
    ));
    assert_eq!(
        cancellation_probe.cancellation_calls.load(Ordering::SeqCst),
        1,
        "cancellation from inside a source batch must wake and unwind exactly once"
    );
    assert_eq!(
        cancellation_probe.before_work_calls.load(Ordering::SeqCst),
        3,
        "polling only between eager batches would inspect additional rows after cancellation"
    );
}

/// Cursor pages are complete, their byte and work budgets reset per fetch, and
/// long scans progress by continuation rather than collecting the whole table.
#[test]
fn cursor_pages_are_complete_byte_bounded_and_per_fetch_budgeted() {
    let db = Arc::new(Database::open_memory());
    seed_relational(&db, "cursor_rows", 43, 23);
    db.execute(
        "CREATE INDEX cursor_rows_id_idx ON cursor_rows(id)",
        &HashMap::new(),
    )
    .expect("declare the ordered index the paged journey reads through");
    let clock = ManualClock::default();
    let sql = "SELECT id, payload FROM cursor_rows ORDER BY id";
    let mut profiling_limits = roomy_limits();
    profiling_limits.result_rows = 4;
    profiling_limits.cursor_page_rows = 4;
    let profiling_request = request(sql, HashMap::new(), profiling_limits, &clock);
    let mut profiled_open = bounded::open_cursor(Arc::clone(&db), &profiling_request)
        .unwrap_or_else(|error| {
            panic!("bounded cursor must open this production fixture; got {error:?}")
        });
    assert_eq!(profiled_open.first_page.rows.len(), 4);
    assert_eq!(cursor_page_ids(&profiled_open.first_page), vec![0, 1, 2, 3]);
    assert!(profiled_open.first_page.has_more);
    let first_page_bytes = cursor_page_encoded_size(&profiled_open.first_page)
        .expect("canonical cursor-page encoding is the byte budget source")
        as u64;
    assert_eq!(profiled_open.telemetry.encoded_bytes, first_page_bytes);
    profiled_open
        .cursor
        .close()
        .expect("close profiling cursor");

    let mut one_row_smaller_page = profiling_limits;
    one_row_smaller_page.cursor_page_rows = 3;
    let smaller_page = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), one_row_smaller_page, &clock),
    )
    .expect("cursor_page_rows shapes the first complete page");
    assert_eq!(smaller_page.first_page.rows.len(), 3);
    assert!(smaller_page.first_page.has_more);
    let three_row_bytes = cursor_page_encoded_size(&smaller_page.first_page)
        .expect("canonical three-row cursor page size") as u64;
    assert!(three_row_bytes < first_page_bytes - 1);

    let mut exact_page_bytes = profiling_limits;
    exact_page_bytes.cursor_page_bytes = first_page_bytes;
    let exact_open = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), exact_page_bytes, &clock),
    )
    .expect("a complete page exactly at its encoded-byte ceiling succeeds");
    assert_eq!(exact_open.first_page.rows.len(), 4);
    assert_eq!(cursor_page_ids(&exact_open.first_page), vec![0, 1, 2, 3]);
    let mut one_byte_below_full_page = exact_page_bytes;
    one_byte_below_full_page.cursor_page_bytes -= 1;
    let partial_open = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), one_byte_below_full_page, &clock),
    )
    .expect("a byte-limited page stops after its last complete row");
    assert_eq!(partial_open.first_page.rows.len(), 3);
    assert_eq!(cursor_page_ids(&partial_open.first_page), vec![0, 1, 2]);
    assert_eq!(
        partial_open.first_page.rows, smaller_page.first_page.rows,
        "byte stopping preserves every value in each complete row"
    );
    assert!(
        partial_open.first_page.has_more,
        "byte stopping reports the unconsumed fourth row truthfully"
    );
    assert_eq!(
        partial_open.telemetry.encoded_bytes as usize,
        cursor_page_encoded_size(&partial_open.first_page)
            .expect("partial page uses canonical encoded bytes")
    );
    assert!(partial_open.telemetry.encoded_bytes <= one_byte_below_full_page.cursor_page_bytes);

    db.execute(
        "CREATE TABLE individually_wide_cursor_row (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create individually wide cursor row");
    let wide_payload = dynamic_payload(0xC0FFEE, 64 * 1024 + first_page_bytes as usize % 257);
    db.execute(
        "INSERT INTO individually_wide_cursor_row (id, payload) VALUES (1, $payload)",
        &params([("payload", Value::Text(wide_payload))]),
    )
    .expect("insert individually wide cursor row");
    let wide_sql = "SELECT id, payload FROM individually_wide_cursor_row";
    let mut wide_profile_limits = roomy_limits();
    wide_profile_limits.result_rows = 1;
    wide_profile_limits.cursor_page_rows = 1;
    let wide_profile = bounded::open_cursor(
        Arc::clone(&db),
        &request(wide_sql, HashMap::new(), wide_profile_limits, &clock),
    )
    .expect("profile the runtime-derived complete single-row page");
    assert_eq!(wide_profile.first_page.rows.len(), 1);
    assert!(!wide_profile.first_page.has_more);
    let required_page_bytes = cursor_page_encoded_size(&wide_profile.first_page)
        .expect("canonical encoded size for the complete single-row page")
        as u64;
    assert_eq!(wide_profile.telemetry.encoded_bytes, required_page_bytes);
    let mut too_narrow_for_one_row = wide_profile_limits;
    too_narrow_for_one_row.cursor_page_bytes = required_page_bytes - 1;
    let wide_error = match bounded::open_cursor(
        Arc::clone(&db),
        &request(wide_sql, HashMap::new(), too_narrow_for_one_row, &clock),
    ) {
        Ok(_) => panic!("an individually oversized row must not produce an empty success page"),
        Err(error) => error,
    };
    assert_limit_with_required_bytes(
        wide_error,
        ReadFailureLimit::CursorPageBytes,
        too_narrow_for_one_row.cursor_page_bytes,
        required_page_bytes,
        wide_sql,
    );

    let expiry_clock = ManualClock::default();
    let mut expiry_limits = profiling_limits;
    expiry_limits.cursor_idle_ms = 10;
    expiry_limits.cursor_lifetime_ms = 20;
    let mut at_idle_boundary = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), expiry_limits, &expiry_clock),
    )
    .expect("cursor opens for deterministic expiry boundary");
    expiry_clock.advance(10);
    assert!(
        at_idle_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .is_ok(),
        "the exact idle boundary remains usable"
    );
    let mut after_idle_boundary = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), expiry_limits, &expiry_clock),
    )
    .expect("second cursor opens at the same manual instant");
    expiry_clock.advance(11);
    assert_cursor_expired(
        after_idle_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .expect_err("one manual millisecond beyond idle must expire"),
        CursorExpiryKind::Idle,
    );

    let lifetime_clock = ManualClock::default();
    let mut at_lifetime_boundary = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), expiry_limits, &lifetime_clock),
    )
    .expect("cursor opens for deterministic lifetime boundary");
    lifetime_clock.advance(9);
    assert!(
        at_lifetime_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .is_ok()
    );
    lifetime_clock.advance(9);
    assert!(
        at_lifetime_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .is_ok()
    );
    lifetime_clock.advance(2);
    assert!(
        at_lifetime_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .is_ok(),
        "the exact lifetime boundary remains usable"
    );
    let mut after_lifetime_boundary = bounded::open_cursor(
        Arc::clone(&db),
        &request(sql, HashMap::new(), expiry_limits, &lifetime_clock),
    )
    .expect("second lifetime cursor opens at the same manual instant");
    lifetime_clock.advance(9);
    assert!(
        after_lifetime_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .is_ok()
    );
    lifetime_clock.advance(9);
    assert!(
        after_lifetime_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .is_ok()
    );
    lifetime_clock.advance(3);
    assert_cursor_expired(
        after_lifetime_boundary
            .cursor
            .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
            .expect_err("one manual millisecond beyond total lifetime must expire"),
        CursorExpiryKind::Lifetime,
    );

    let fetch_clock = ManualClock::default();
    let fetch_cancel = OwnerReadCancellation::new();
    let fetch_probe = Arc::new(AdvancingProbe {
        clock: fetch_clock.clone(),
        ..Default::default()
    });
    let mut per_fetch_limits = profiling_limits;
    per_fetch_limits.work = 4;
    per_fetch_limits.active_ms = 4;
    let refusal_accountant = Arc::new(MemoryAccountant::no_limit());
    let refusal_db = Arc::new(Database::open_memory_with_accountant(Arc::clone(
        &refusal_accountant,
    )));
    refusal_db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    refusal_db
        .execute(
            "CREATE TABLE cursor_refusal_rows (id INTEGER PRIMARY KEY, payload TEXT) HISTORY CURRENT ONLY SYNC OFF",
            &HashMap::new(),
        )
        .expect("create refusal snapshot fixture");
    refusal_db
        .execute(
            "CREATE INDEX cursor_refusal_rows_id_idx ON cursor_refusal_rows(id)",
            &HashMap::new(),
        )
        .expect("declare the ordered index the paged journey reads through");
    for id in 0..9_i64 {
        refusal_db
            .execute(
                "INSERT INTO cursor_refusal_rows (id, payload) VALUES ($id, $payload)",
                &params([
                    ("id", Value::Int64(id)),
                    ("payload", Value::Text(dynamic_payload(id as u64 + 71, 23))),
                ]),
            )
            .expect("insert refusal snapshot row");
    }
    let refusal_baseline_memory = refusal_accountant.usage().used;
    let fetch_ceiling_clock = ManualClock::default();
    let mut fetch_ceiling = bounded::open_cursor(
        Arc::clone(&refusal_db),
        &request(
            "SELECT id, payload FROM cursor_refusal_rows ORDER BY id",
            HashMap::new(),
            per_fetch_limits,
            &fetch_ceiling_clock,
        ),
    )
    .expect("cursor opens at the effective fetch-row ceiling");
    assert!(
        refusal_accountant.usage().used > refusal_baseline_memory,
        "the live fetch cursor holds its retained state in the database accountant"
    );
    refusal_db
        .execute(
            "UPDATE cursor_refusal_rows SET payload = $payload WHERE id = 4",
            &params([("payload", Value::Text(dynamic_payload(0xF37C, 23)))]),
        )
        .expect("supersede a future row while the refusal cursor is pinned");
    assert_eq!(
        refusal_db.__physical_version_count_for_test("cursor_refusal_rows"),
        10
    );
    // An explicit count above `result_rows` is answered before the read is
    // touched: nothing is pulled and no continuation position is consumed, so
    // this refuses the REQUEST, not the read. The cursor therefore stays where
    // the caller left it -- holding its bytes and its snapshot pin, exactly as
    // it does between any two fetches, and bounded by the same idle and
    // lifetime ceilings -- and a smaller page is served from there. That is
    // what makes the refusal's own escape executable rather than advice about
    // a cursor that is already gone. A refusal raised while PULLING is the
    // other case and still releases everything, which
    // `cursor_page_byte_refusal_is_terminal_and_releases_the_cursor_charge`
    // holds.
    assert_limit(
        fetch_ceiling
            .cursor
            .fetch(NonZeroUsize::new(5), OwnerReadCancellation::new())
            .expect_err("an explicit fetch above result_rows must be refused"),
        ReadFailureLimit::ResultRows,
        4,
    );
    assert!(
        refusal_accountant.usage().used > refusal_baseline_memory,
        "a request-shaped refusal keeps the cursor's retained bytes"
    );
    let after_refusal = refusal_db
        .compact_currency_versions()
        .expect("compaction runs while the refusal cursor is still pinned");
    assert_eq!(
        after_refusal.pruned_versions, 0,
        "the cursor keeps its snapshot pin, so the superseded future row survives"
    );
    assert_eq!(
        refusal_db.__physical_version_count_for_test("cursor_refusal_rows"),
        10
    );
    let resumed_after_refusal = fetch_ceiling
        .cursor
        .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
        .expect("the request-shaped refusal is nonterminal and a smaller page is served");
    assert_eq!(
        cursor_page_ids(&resumed_after_refusal.page),
        vec![4, 5, 6, 7],
        "the refused request consumed nothing, so the page resumes where the first one ended"
    );
    fetch_ceiling
        .cursor
        .close()
        .expect("close the still-live cursor after refusal recovery");
    assert_eq!(
        refusal_accountant.usage().used,
        refusal_baseline_memory,
        "explicit close releases what the request-shaped refusal deliberately kept"
    );
    let after_close = refusal_db
        .compact_currency_versions()
        .expect("the close released the future-row snapshot pin");
    assert_eq!(after_close.pruned_versions, 1);
    assert_eq!(
        refusal_db.__physical_version_count_for_test("cursor_refusal_rows"),
        9
    );

    let mut cancellation_cursor = bounded::open_cursor(
        Arc::clone(&refusal_db),
        &request(
            "SELECT id, payload FROM cursor_refusal_rows ORDER BY id",
            HashMap::new(),
            per_fetch_limits,
            &ManualClock::default(),
        ),
    )
    .expect("open cursor before a fetch-local cancellation");
    assert_eq!(
        cursor_page_ids(&cancellation_cursor.first_page),
        vec![0, 1, 2, 3]
    );
    let cancellation = OwnerReadCancellation::new();
    let cancellation_probe = Arc::new(CursorCancellationProbe::new(cancellation.clone()));
    cancellation_cursor
        .cursor
        .set_probe(Some(cancellation_probe.clone()));
    refusal_db.__reset_relational_scan_rows_touched();
    assert!(matches!(
        cancellation_cursor
            .cursor
            .fetch(NonZeroUsize::new(4), cancellation),
        Err(bounded::TestError::Cancelled)
    ));
    assert_eq!(cancellation_probe.source_touches.load(Ordering::SeqCst), 1);
    assert_eq!(
        cancellation_probe.cancellation_calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        refusal_db.__relational_scan_rows_touched(),
        0,
        "fetch cancellation stops before inspecting or advancing the next table row"
    );
    assert!(
        refusal_accountant.usage().used > refusal_baseline_memory,
        "cancellation keeps the production-owned cursor resources live"
    );
    cancellation_cursor.cursor.set_probe(None);
    let resumed_after_cancellation = cancellation_cursor
        .cursor
        .fetch(NonZeroUsize::new(4), OwnerReadCancellation::new())
        .expect("cancellation is nonterminal and the same production cursor resumes");
    assert_eq!(
        cursor_page_ids(&resumed_after_cancellation.page),
        vec![4, 5, 6, 7],
        "a cancelled fetch leaves continuation at the first uninspected row"
    );
    cancellation_cursor
        .cursor
        .close()
        .expect("close the still-live cursor after cancellation recovery");
    assert_eq!(
        refusal_accountant.usage().used,
        refusal_baseline_memory,
        "explicit close releases the resources cancellation deliberately retained"
    );

    let mut per_fetch_request = request(sql, HashMap::new(), per_fetch_limits, &fetch_clock);
    per_fetch_request.probe = Some(fetch_probe);
    db.__reset_relational_scan_rows_touched();
    let mut open = bounded::open_cursor(Arc::clone(&db), &per_fetch_request)
        .expect("first four rows fit one work/active-time budget");
    assert_eq!(open.first_page.rows.len(), 4);
    assert_eq!(cursor_page_ids(&open.first_page), vec![0, 1, 2, 3]);
    assert!(open.first_page.has_more);
    assert_eq!(
        open.telemetry.work_units, 4,
        "opening a four-row page touches only its first four ordered source entries"
    );
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "the ordered journey never falls back to a physical table scan"
    );
    for (page_number, expected_start) in [4_i64, 8, 12, 16, 20, 24, 28, 32, 36]
        .into_iter()
        .enumerate()
    {
        let page = open
            .cursor
            .fetch(NonZeroUsize::new(4), fetch_cancel.clone())
            .expect("each fetch gets a fresh work and active-time budget");
        assert_eq!(
            page.page.rows.len(),
            4,
            "cursor fetch beginning at {expected_start} stays a complete page"
        );
        assert_eq!(
            cursor_page_ids(&page.page),
            (expected_start..expected_start + 4).collect::<Vec<_>>(),
            "cursor continuation resumes the suspended source instead of rerunning SQL"
        );
        assert_eq!(
            page.telemetry.work_units,
            4,
            "page {} must resume from its next source entry instead of rescanning the prefix",
            page_number + 2
        );
        assert!(page.telemetry.peak_temporary_bytes <= per_fetch_limits.memory);
        assert_eq!(
            db.__relational_scan_rows_touched(),
            0,
            "the ordered journey never falls back to a physical table scan"
        );
    }
    let terminal = open
        .cursor
        .fetch(NonZeroUsize::new(4), fetch_cancel)
        .expect("final cursor page");
    assert_eq!(terminal.page.rows.len(), 3);
    assert_eq!(cursor_page_ids(&terminal.page), vec![40, 41, 42]);
    assert!(
        !terminal.page.has_more,
        "only genuine exhaustion ends the cursor"
    );
    assert_eq!(
        terminal.telemetry.work_units, 3,
        "the terminal page touches its remaining three source entries exactly once"
    );
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "the ordered journey never falls back to a physical table scan"
    );
}

/// Every terminal cursor path releases both database-accounted source state
/// and the snapshot registration that protects a future row version.
#[test]
fn cursor_terminal_paths_release_accounted_memory_and_snapshot_pin() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let memory_db = Arc::new(Database::open_memory_with_accountant(Arc::clone(
        &accountant,
    )));
    seed_relational(&memory_db, "cursor_memory_rows", 17, 31);
    let clock = ManualClock::default();
    let mut limits = roomy_limits();
    limits.result_rows = 2;
    limits.cursor_page_rows = 2;
    let baseline_memory = accountant.usage().used;
    let mut memory_cursor = bounded::open_cursor(
        Arc::clone(&memory_db),
        &request(
            "SELECT id, payload FROM cursor_memory_rows ORDER BY id",
            HashMap::new(),
            limits,
            &clock,
        ),
    )
    .expect("open cursor with retained source state");
    assert!(memory_cursor.first_page.has_more);
    let memory_while_open = accountant.usage().used;
    assert!(
        memory_while_open > baseline_memory,
        "retained cursor state is visible in the database accountant"
    );
    memory_cursor
        .cursor
        .close()
        .expect("close memory-accounted cursor");
    assert_eq!(
        accountant.usage().used,
        baseline_memory,
        "close returns every retained byte to the database accountant"
    );
    let mut exhausted_cursor = bounded::open_cursor(
        Arc::clone(&memory_db),
        &request(
            "SELECT id, payload FROM cursor_memory_rows ORDER BY id",
            HashMap::new(),
            limits,
            &clock,
        ),
    )
    .expect("open cursor for exhaustion release");
    let mut has_more = exhausted_cursor.first_page.has_more;
    while has_more {
        let page = exhausted_cursor
            .cursor
            .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
            .expect("drain cursor to genuine exhaustion");
        has_more = page.page.has_more;
    }
    assert_eq!(
        accountant.usage().used,
        baseline_memory,
        "the terminal page auto-releases every retained byte"
    );

    memory_db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    memory_db
        .execute(
            "CREATE TABLE cursor_snapshot_rows (id INTEGER PRIMARY KEY, payload TEXT) HISTORY CURRENT ONLY SYNC OFF",
            &HashMap::new(),
        )
        .expect("create current-version cursor fixture");
    for id in 0..6_i64 {
        memory_db
            .execute(
                "INSERT INTO cursor_snapshot_rows (id, payload) VALUES ($id, $payload)",
                &params([
                    ("id", Value::Int64(id)),
                    ("payload", Value::Text(dynamic_payload(id as u64, 17))),
                ]),
            )
            .expect("insert snapshot cursor row");
    }
    let snapshot_baseline_memory = accountant.usage().used;
    let mut snapshot_cursor = bounded::open_cursor(
        Arc::clone(&memory_db),
        &request(
            "SELECT id, payload FROM cursor_snapshot_rows ORDER BY id",
            HashMap::new(),
            limits,
            &clock,
        ),
    )
    .expect("open snapshot-pinning cursor");
    assert!(snapshot_cursor.first_page.has_more);
    assert!(
        accountant.usage().used > snapshot_baseline_memory,
        "the live snapshot cursor keeps its retained bytes in the database accountant"
    );
    memory_db
        .execute(
            "UPDATE cursor_snapshot_rows SET payload = $payload WHERE id = 4",
            &params([("payload", Value::Text(dynamic_payload(0x51A7E, 17)))]),
        )
        .expect("supersede a row after cursor snapshot capture");
    assert_eq!(
        memory_db.__physical_version_count_for_test("cursor_snapshot_rows"),
        7
    );
    let while_pinned = memory_db
        .compact_currency_versions()
        .expect("compact while cursor snapshot is pinned");
    assert_eq!(
        while_pinned.pruned_versions, 0,
        "the live cursor pin keeps its visible old version"
    );
    assert_eq!(
        memory_db.__physical_version_count_for_test("cursor_snapshot_rows"),
        7
    );
    let mut has_more = snapshot_cursor.first_page.has_more;
    while has_more {
        let page = snapshot_cursor
            .cursor
            .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
            .expect("drain cursor with the superseded row still in its future");
        has_more = page.page.has_more;
    }
    assert_eq!(
        accountant.usage().used,
        snapshot_baseline_memory,
        "has_more=false releases every retained cursor byte without an explicit close"
    );
    let after_exhaustion = memory_db
        .compact_currency_versions()
        .expect("compact immediately after exhaustion releases the snapshot pin");
    assert_eq!(after_exhaustion.pruned_versions, 1);
    assert_eq!(
        memory_db.__physical_version_count_for_test("cursor_snapshot_rows"),
        6
    );

    memory_db
        .execute(
            "CREATE TABLE cursor_expiry_rows (id INTEGER PRIMARY KEY, payload TEXT) HISTORY CURRENT ONLY SYNC OFF",
            &HashMap::new(),
        )
        .expect("create expiry snapshot fixture");
    for id in 0..6_i64 {
        memory_db
            .execute(
                "INSERT INTO cursor_expiry_rows (id, payload) VALUES ($id, $payload)",
                &params([
                    ("id", Value::Int64(id)),
                    ("payload", Value::Text(dynamic_payload(id as u64 + 99, 17))),
                ]),
            )
            .expect("insert expiry snapshot row");
    }
    let expiry_baseline_memory = accountant.usage().used;
    let expiry_clock = ManualClock::default();
    let mut expiry_limits = limits;
    expiry_limits.cursor_idle_ms = 10;
    expiry_limits.cursor_lifetime_ms = 20;
    let mut expiry_cursor = bounded::open_cursor(
        Arc::clone(&memory_db),
        &request(
            "SELECT id, payload FROM cursor_expiry_rows ORDER BY id",
            HashMap::new(),
            expiry_limits,
            &expiry_clock,
        ),
    )
    .expect("open cursor for expiry release");
    assert!(expiry_cursor.first_page.has_more);
    memory_db
        .execute(
            "UPDATE cursor_expiry_rows SET payload = $payload WHERE id = 4",
            &params([("payload", Value::Text(dynamic_payload(0xE771, 17)))]),
        )
        .expect("supersede a future expiry-cursor row");
    assert_eq!(
        memory_db.__physical_version_count_for_test("cursor_expiry_rows"),
        7
    );
    expiry_clock.advance(11);
    assert_cursor_expired(
        expiry_cursor
            .cursor
            .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
            .expect_err("idle expiry releases the retained cursor state"),
        CursorExpiryKind::Idle,
    );
    assert_eq!(
        accountant.usage().used,
        expiry_baseline_memory,
        "expiry returns every retained cursor byte to the database accountant"
    );
    let after_expiry = memory_db
        .compact_currency_versions()
        .expect("compact immediately after expiry releases the snapshot pin");
    assert_eq!(after_expiry.pruned_versions, 1);
    assert_eq!(
        memory_db.__physical_version_count_for_test("cursor_expiry_rows"),
        6
    );
    assert_cursor_not_found(
        expiry_cursor
            .cursor
            .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
            .expect_err("an expired cursor has no continuation to reuse"),
    );
}
