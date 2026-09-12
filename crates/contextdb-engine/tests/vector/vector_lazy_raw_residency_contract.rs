//! Raw vector bodies are a per-partition working set, not startup state.
//! Opening a database may register lightweight identities for every partition,
//! but only the partition an operation actually needs may become resident.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Error, RowId, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_PARTITION: usize = 24;
const MANY_PARTITIONS: usize = 6;
const MANY_ROWS_PER_PARTITION: usize = 200;
const MANY_VECTOR_DIMENSION: usize = 128;
const MAX_MAINTENANCE_CYCLES: usize = 64;

/// Scopes seeded for the two-column lang-filtered candidate-derivation
/// fixture below (`open_lang_residency_fixture`).
const LANG_PARTITIONS: usize = 3;
/// Rows seeded per scope in that fixture. An `SQ8` column's default
/// `AUTO_INDEX_AT` is 5001 (`VectorLayout::effective_auto_index_at`), so the
/// fixture declares its own low `AUTO_INDEX_AT` (below) and still wants
/// enough total rows that the `AUTO`-with-no-index preflight's
/// `exact_score_bytes` reservation clears the fixed per-search overhead a
/// tiny row count would otherwise hide behind (observed ~4.6KiB).
const LANG_ROWS_PER_PARTITION: usize = 150;

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

fn index() -> VectorIndexRef {
    VectorIndexRef::new("raw_residency_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID partition component has one canonical key")
}

fn raw_resident(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .raw_partition_resident_for_test(&index(), &partition(scope))
}

fn seed_partition(db: &Database, scope: Uuid, first: Uuid, axis: usize) {
    for ordinal in 0..ROWS_PER_PARTITION {
        let id = Uuid::from_u128(first.as_u128() + ordinal as u128);
        let mut vector = vec![0.0_f32; 32];
        vector[axis] = 1.0;
        let neighbor = (axis + ordinal + 1) % vector.len();
        vector[neighbor] = ordinal as f32 / 1000.0;
        db.execute(
            "INSERT INTO raw_residency_items (id, scope_id, embedding) \
             VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Uuid(scope)),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("insert one raw-vector fixture row");
    }
}

fn drive_maintenance_until_ready(db: &Database, expected_partitions: usize) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let infos = db
            .vector_store_for_test()
            .partition_infos(&index())
            .unwrap();
        if infos.len() == expected_partitions && infos.iter().all(|info| info.graph_available) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!(
        "maintenance did not publish {expected_partitions} routes within \
         {MAX_MAINTENANCE_CYCLES} cycles"
    );
}

fn bounded_request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        sql,
        bound,
        ReadLimits {
            result_rows: 64,
            result_bytes: 16 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 1_000_000,
            memory: 64 * 1024 * 1024,
            cursor_page_rows: 64,
            cursor_page_bytes: 4 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 100_000,
        },
        Arc::new(FrozenClock),
    )
}

fn result_ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.ends_with(".id"))
        .expect("vector result projects its id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            other => panic!("vector result id is UUID, got {other:?}"),
        })
        .collect()
}

fn row_id_for_uuid(db: &Database, id: Uuid) -> RowId {
    db.scan("raw_residency_items", db.snapshot())
        .expect("scan the deterministic relational fixture")
        .into_iter()
        .find_map(|row| match row.values.get("id") {
            Some(Value::Uuid(found)) if *found == id => Some(row.row_id),
            _ => None,
        })
        .unwrap_or_else(|| panic!("raw-residency row {id} exists"))
}

fn many_scope(ordinal: usize) -> Uuid {
    Uuid::from_u128(0xCA11_0000_0000_0000_0000_0000_0000_0000 + ordinal as u128)
}

fn many_id(partition: usize, ordinal: usize) -> Uuid {
    Uuid::from_u128(
        0xCA12_0000_0000_0000_0000_0000_0000_0000
            + (partition * MANY_ROWS_PER_PARTITION + ordinal) as u128,
    )
}

fn many_vector(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; MANY_VECTOR_DIMENSION];
    vector[axis] = 1.0;
    vector
}

fn seed_many_partition(db: &Database, partition_ordinal: usize) {
    for ordinal in 0..MANY_ROWS_PER_PARTITION {
        let embedding = match (partition_ordinal, ordinal) {
            (0, 0) => many_vector(0),
            (0, 1) => {
                let mut vector = many_vector(0);
                vector[0] = 0.98;
                vector[1] = 0.2;
                vector
            }
            _ => many_vector(1),
        };
        db.execute(
            "INSERT INTO raw_residency_items (id, scope_id, embedding) \
             VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(many_id(partition_ordinal, ordinal))),
                ("scope", Value::Uuid(many_scope(partition_ordinal))),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("insert one many-partition raw-vector fixture row");
    }
}

fn search(db: &Database, scope: Uuid, axis: usize, mode: &str) {
    let mut query = vec![0.0_f32; 32];
    query[axis] = 1.0;
    let sql = format!(
        "SELECT id, embedding FROM raw_residency_items WHERE scope_id = $scope \
         ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 3"
    );
    let result = db
        .execute(
            &sql,
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("{mode} selected-partition search failed: {error}"));
    assert_eq!(result.rows.len(), 3);
    let embedding = result
        .columns
        .iter()
        .position(|column| column == "embedding" || column.ends_with(".embedding"))
        .expect("projected embedding column exists");
    assert!(result.rows.iter().all(|row| {
        matches!(row.get(embedding), Some(Value::Vector(vector)) if vector.len() == 32)
    }));
}

#[test]
#[serial]
fn reopen_and_selected_queries_keep_raw_vector_residency_partition_local() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("lazy-raw-vector-residency.db");
    let alpha = Uuid::from_u128(0xA110);
    let bravo = Uuid::from_u128(0xB220);

    {
        let db = Database::open(&path).expect("open raw-residency fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE raw_residency_items (\
             id UUID PRIMARY KEY, \
             scope_id UUID NOT NULL, \
             embedding VECTOR(32) PARTITION_KEY (scope_id) MAX_PARTITIONS 4\
             )",
            &empty(),
        )
        .expect("create partitioned raw-vector fixture");
        seed_partition(&db, alpha, Uuid::from_u128(0xA100_0000), 0);
        seed_partition(&db, bravo, Uuid::from_u128(0xB200_0000), 1);
        drive_maintenance_until_ready(&db, 2);
        db.close().expect("close maintained fixture");
    }

    let db = Database::open(&path).expect("reopen without hydrating raw vector bodies");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(!raw_resident(&db, alpha));
    assert!(!raw_resident(&db, bravo));
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "open registers directories and saved graphs without loading any raw partition"
    );

    search(&db, alpha, 0, "EXACT");
    assert!(raw_resident(&db, alpha));
    assert!(!raw_resident(&db, bravo));
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        1
    );

    assert!(
        db.vector_store_for_test()
            .evict_raw_partition_for_test(&index(), &partition(alpha)),
        "an idle raw partition returns its resident body and charge"
    );
    assert!(!raw_resident(&db, alpha));
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0
    );

    let activity_before_indexed = db.__vector_passive_activity_counters_for_test();
    search(&db, bravo, 1, "INDEXED");
    let activity_after_indexed = db.__vector_passive_activity_counters_for_test();
    assert!(!raw_resident(&db, alpha));
    assert!(!raw_resident(&db, bravo));
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "INDEXED keeps both selected and sibling raw partitions directory-only"
    );
    assert_eq!(
        activity_after_indexed.raw_partition_loads, activity_before_indexed.raw_partition_loads,
        "INDEXED must not disguise candidate scoring as one complete partition load"
    );
}

#[test]
#[serial]
fn indexed_many_partition_reads_point_load_only_visible_versioned_candidates() {
    let root = TempDir::new().expect("temporary many-partition store directory");
    let path = root.path().join("indexed-candidate-point-load.redb");
    let replaced = many_id(0, 0);
    let stable_nearest = many_id(0, 1);

    {
        let db = Database::open(&path).expect("open many-partition fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            &format!(
                "CREATE TABLE raw_residency_items (\
                 id UUID PRIMARY KEY, \
                 scope_id UUID NOT NULL, \
                 embedding VECTOR({MANY_VECTOR_DIMENSION}) PARTITION_KEY (scope_id) \
                    MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1000\
                 )"
            ),
            &empty(),
        )
        .expect("create the many-partition vector table");
        for partition_ordinal in 0..MANY_PARTITIONS {
            seed_many_partition(&db, partition_ordinal);
        }
        drive_maintenance_until_ready(&db, MANY_PARTITIONS);
        db.close().expect("close the maintained fixture");
    }

    let db = Database::open(&path).expect("reopen with dormant raw bodies and graph generations");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for partition_ordinal in 0..MANY_PARTITIONS {
        let route = contextdb_vector::VectorPartitionRef::new(
            index(),
            partition(many_scope(partition_ordinal)),
        );
        assert!(
            db.vector_store_for_test()
                .preload_dormant_partition_generation(&route, true)
                .expect("preload one saved base graph"),
            "partition {partition_ordinal} starts with one dormant saved base"
        );
    }
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "loading graph generations does not load raw vectors"
    );

    let replaced_row_id = row_id_for_uuid(&db, replaced);
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    db.execute(
        "UPDATE raw_residency_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("embedding", Value::Vector(many_vector(1))),
            ("id", Value::Uuid(replaced)),
        ]),
    )
    .expect("replace the nearest vector after retaining its old snapshot");
    let new_snapshot = db.snapshot();

    let query = params([("query", Value::Vector(many_vector(0)))]);
    let exact_sql = "SELECT id FROM raw_residency_items \
                     ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 2";
    let old_exact = db
        .execute_at_snapshot(exact_sql, &query, old_snapshot)
        .expect("exact old-snapshot control reads the original body");
    let new_exact = db
        .execute_at_snapshot(exact_sql, &query, new_snapshot)
        .expect("exact current-snapshot control reads the replacement body");
    assert_eq!(result_ids(&old_exact).first(), Some(&replaced));
    assert_eq!(result_ids(&new_exact).first(), Some(&stable_nearest));

    let evicted = db.vector_store_for_test().evict_reloadable_raw_partitions();
    assert_eq!(evicted, MANY_PARTITIONS);
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0
    );

    let reloaded_old_exact = db
        .execute_at_snapshot(exact_sql, &query, old_snapshot)
        .expect("bulk exact reload preserves the historical replacement body");
    let reloaded_new_exact = db
        .execute_at_snapshot(exact_sql, &query, new_snapshot)
        .expect("bulk exact reload preserves the current replacement body");
    assert_eq!(reloaded_old_exact.rows, old_exact.rows);
    assert_eq!(reloaded_new_exact.rows, new_exact.rows);
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        MANY_PARTITIONS
    );
    assert_eq!(
        db.vector_store_for_test().evict_reloadable_raw_partitions(),
        MANY_PARTITIONS
    );

    let bytes_per_f32_body = MANY_VECTOR_DIMENSION * std::mem::size_of::<f32>();
    let minimum_bulk_reservation =
        MANY_ROWS_PER_PARTITION * bytes_per_f32_body * 2 + bytes_per_f32_body * 2;
    let point_reservation = bytes_per_f32_body * 4;
    assert!(
        point_reservation < minimum_bulk_reservation,
        "one point body must fit where no complete partition can"
    );
    let settled_usage = db.accountant().usage().used;
    db.set_memory_limit(Some(settled_usage + minimum_bulk_reservation - 1))
        .expect("leave enough database memory for point scoring but not a bulk raw load");
    let settled_usage = db.accountant().usage().used;
    let activity_before = db.__vector_passive_activity_counters_for_test();
    let ownership_before = db.vector_memory_ownership_receipt_for_test();
    let mut observed_graph_candidates = 0_u64;
    let mut observed_unique_candidates = 0_u64;

    // Check the raw-load refusal while the fixture still owns every preloaded
    // graph. Broad indexed reads below release that ownership as they finish,
    // making the same memory cap less restrictive for later ordinary work.
    let bulk_error = db
        .execute_at_snapshot(exact_sql, &query, new_snapshot)
        .expect_err("the same memory ceiling must reject every complete raw-partition load");
    assert!(
        matches!(
            &bulk_error,
            Error::MemoryBudgetExceeded { operation, .. }
                if operation == "load_raw_vector_partition"
        ),
        "the control must fail specifically at bulk raw-vector admission: {bulk_error:?}"
    );

    for (snapshot, exact) in [(old_snapshot, &old_exact), (new_snapshot, &new_exact)] {
        for mode in ["AUTO", "INDEXED"] {
            let sql = format!(
                "SELECT id FROM raw_residency_items \
                 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 2"
            );

            db.__reset_last_query_vector_trace_for_test();
            let ordinary = db
                .execute_at_snapshot(&sql, &query, snapshot)
                .unwrap_or_else(|error| panic!("ordinary {mode} candidate search failed: {error}"));
            let ordinary_trace = db
                .__take_last_query_vector_trace_for_test()
                .expect("ordinary indexed route publishes a trace");
            assert_eq!(ordinary.rows, exact.rows);
            assert!(
                ordinary_trace.used_hnsw,
                "{mode} must use all maintained graphs"
            );
            assert!(
                ordinary_trace
                    .hnsw_candidate_row_ids
                    .contains(&replaced_row_id),
                "the old base must still nominate the replaced row"
            );
            observed_graph_candidates = observed_graph_candidates
                .saturating_add(ordinary_trace.hnsw_candidate_row_ids.len() as u64);
            observed_unique_candidates = observed_unique_candidates.saturating_add(
                ordinary_trace
                    .hnsw_candidate_row_ids
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len() as u64,
            );

            db.__reset_last_query_vector_trace_for_test();
            let bounded = db
                .__with_snapshot_override_for_test(snapshot, || {
                    bounded::execute(&db, &bounded_request(&sql, query.clone()))
                })
                .unwrap_or_else(|error| {
                    panic!("bounded {mode} candidate search failed: {error:?}")
                });
            let bounded_trace = db
                .__take_last_query_vector_trace_for_test()
                .expect("bounded indexed route publishes a trace");
            assert_eq!(bounded.result.rows, exact.rows);
            assert_eq!(bounded.result.rows, ordinary.rows);
            assert!(
                bounded_trace.used_hnsw,
                "bounded {mode} must use maintained graphs"
            );
            observed_graph_candidates = observed_graph_candidates
                .saturating_add(bounded_trace.hnsw_candidate_row_ids.len() as u64);
            observed_unique_candidates = observed_unique_candidates.saturating_add(
                bounded_trace
                    .hnsw_candidate_row_ids
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len() as u64,
            );
        }
    }

    let activity_after = db.__vector_passive_activity_counters_for_test();
    let point_loads = activity_after
        .raw_candidate_point_loads
        .saturating_sub(activity_before.raw_candidate_point_loads);
    let point_body_bytes = activity_after
        .raw_candidate_point_body_bytes
        .saturating_sub(activity_before.raw_candidate_point_body_bytes);
    assert_eq!(
        activity_after.raw_partition_loads,
        activity_before.raw_partition_loads
    );
    assert!(
        point_loads > 0,
        "the versioned graph candidate must use the point loader"
    );
    assert_eq!(
        point_loads, observed_unique_candidates,
        "each distinct visible graph candidate requires exactly one native body score"
    );
    assert!(
        point_loads <= observed_graph_candidates,
        "point reads are bounded by graph candidates: {point_loads} > {observed_graph_candidates}"
    );
    assert_eq!(point_body_bytes, point_loads * bytes_per_f32_body as u64);
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "candidate scoring never installs a complete raw partition"
    );
    let ownership_after = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(ownership_after.raw_vector_bodies, 0);
    assert_eq!(
        ownership_after.temporary_workspace,
        ownership_before.temporary_workspace
    );
    let graph_bytes =
        |ownership: &contextdb_engine::database::VectorMemoryOwnershipReceiptForTest| {
            ownership.base_graph
                + ownership.change_graph
                + ownership.mutable_tail
                + ownership.retired_pinned_graph_bytes
        };
    assert_eq!(
        graph_bytes(&ownership_after),
        graph_bytes(&ownership_before),
        "without a memory limit, broad reads keep their reusable graphs resident"
    );
    assert_eq!(db.accountant().usage().used, settled_usage);
    eprintln!(
        "indexed_candidate_memory_receipt partitions={MANY_PARTITIONS} rows={} \
         graph_candidates={observed_graph_candidates} unique_candidates={observed_unique_candidates} \
         point_loads={point_loads} point_body_bytes={point_body_bytes} raw_partition_load_delta=0 \
         resident_raw_partitions=0 bulk_reservation_bytes={minimum_bulk_reservation}",
        MANY_PARTITIONS * MANY_ROWS_PER_PARTITION,
    );
    drop(old_pin);
}

// ---------------------------------------------------------------------------
// A filtered vector search must learn candidate row ids without loading a
// vector body no statement ever named. The fixture below carries a second
// quantized column (`payload_vec`) that no query in this section ever reads,
// so any residency it accumulates can only have come from candidate
// derivation's own hydration, never from answering the query.
// ---------------------------------------------------------------------------

fn payload_index() -> VectorIndexRef {
    VectorIndexRef::new("raw_residency_items", "payload_vec")
}

fn payload_resident(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .raw_partition_resident_for_test(&payload_index(), &partition(scope))
}

fn lang_scope(partition_ordinal: usize) -> Uuid {
    Uuid::from_u128(0xCA20_0000_0000_0000_0000_0000_0000_0000 + partition_ordinal as u128)
}

/// Encodes its own partition ordinal, exactly as `many_id` does: decoded by
/// `lang_partition_ordinal_of` below.
fn lang_id(partition_ordinal: usize, ordinal: usize) -> Uuid {
    Uuid::from_u128(
        0xCA21_0000_0000_0000_0000_0000_0000_0000
            + (partition_ordinal * LANG_ROWS_PER_PARTITION + ordinal) as u128,
    )
}

fn lang_partition_ordinal_of(id: Uuid) -> Option<usize> {
    let base = 0xCA21_0000_0000_0000_0000_0000_0000_0000_u128;
    let offset = id.as_u128().checked_sub(base)?;
    let offset = usize::try_from(offset).ok()?;
    if offset >= LANG_PARTITIONS * LANG_ROWS_PER_PARTITION {
        return None;
    }
    Some(offset / LANG_ROWS_PER_PARTITION)
}

/// A vector aligned on `axis`, with a small ordinal-scaled jitter on a
/// neighboring axis so ordinal 0 is uniquely closest to the pure-axis query
/// and every later ordinal in the same partition ranks strictly farther --
/// the same closeness-by-ordinal shape `seed_partition` already uses.
fn lang_vector(axis: usize, ordinal: usize) -> Vec<f32> {
    let mut vector = vec![0.0_f32; MANY_VECTOR_DIMENSION];
    vector[axis] = 1.0;
    let neighbor = (axis + ordinal + 1) % vector.len();
    vector[neighbor] = ordinal as f32 / 1000.0;
    vector
}

fn lang_query(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0_f32; MANY_VECTOR_DIMENSION];
    vector[axis] = 1.0;
    vector
}

fn insert_lang_row(db: &Database, id: Uuid, scope: Uuid, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO raw_residency_items (id, scope_id, lang, embedding, payload_vec) \
         VALUES ($id, $scope, 'en', $embedding, $payload)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(embedding)),
            (
                "payload",
                // Never named by any statement in this section -- see the
                // module comment above.
                Value::Vector(vec![0.0_f32; MANY_VECTOR_DIMENSION]),
            ),
        ]),
    )
    .expect("insert one lang-residency fixture row");
}

fn seed_lang_partition(db: &Database, partition_ordinal: usize) {
    let scope = lang_scope(partition_ordinal);
    for ordinal in 0..LANG_ROWS_PER_PARTITION {
        insert_lang_row(
            db,
            lang_id(partition_ordinal, ordinal),
            scope,
            lang_vector(partition_ordinal, ordinal),
        );
    }
}

fn drive_maintenance_until_ready_for_indices(
    db: &Database,
    expected_partitions: usize,
    indices: &[VectorIndexRef],
) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let ready = indices.iter().all(|index| {
            let infos = db.vector_store_for_test().partition_infos(index).unwrap();
            infos.len() == expected_partitions && infos.iter().all(|info| info.graph_available)
        });
        if ready {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!(
        "maintenance did not publish {expected_partitions} routes for every index within \
         {MAX_MAINTENANCE_CYCLES} cycles"
    );
}

/// Opens the two-quantized-column lang-filtered fixture, seeds
/// `LANG_PARTITIONS` scopes of `LANG_ROWS_PER_PARTITION` rows each (every row
/// `lang = 'en'`), optionally declares a relational index on `lang`, drives
/// maintenance until both vector columns have a graph for every partition,
/// closes, and reopens. Confirms that a reopened SQ8 partitioned column
/// starts with zero resident raw partitions, the same as this file's
/// unquantized fixtures.
fn open_lang_residency_fixture(with_lang_index: bool) -> (TempDir, Database) {
    let root = TempDir::new().expect("temporary lang-residency directory");
    let path = root.path().join("lang-residency.db");

    {
        let db = Database::open(&path).expect("open the lang-residency fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            &format!(
                "CREATE TABLE raw_residency_items (\
                 id UUID PRIMARY KEY, \
                 scope_id UUID NOT NULL, \
                 lang TEXT NOT NULL, \
                 embedding VECTOR({MANY_VECTOR_DIMENSION}) WITH (quantization = 'SQ8') \
                    PARTITION_KEY (scope_id) MAX_PARTITIONS 8 AUTO_INDEX_AT 10, \
                 payload_vec VECTOR({MANY_VECTOR_DIMENSION}) WITH (quantization = 'SQ8') \
                    PARTITION_KEY (scope_id) MAX_PARTITIONS 8 AUTO_INDEX_AT 10\
                 )"
            ),
            &empty(),
        )
        .expect("create the two-quantized-column lang-residency table");
        if with_lang_index {
            db.execute(
                "CREATE INDEX lang_idx ON raw_residency_items (lang)",
                &empty(),
            )
            .expect("create the relational lang index");
        }
        for partition_ordinal in 0..LANG_PARTITIONS {
            seed_lang_partition(&db, partition_ordinal);
        }
        drive_maintenance_until_ready_for_indices(
            &db,
            LANG_PARTITIONS,
            &[index(), payload_index()],
        );
        db.close()
            .expect("close the maintained lang-residency fixture");
    }

    let db = Database::open(&path).expect("reopen without hydrating raw vector bodies");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for partition_ordinal in 0..LANG_PARTITIONS {
        let scope = lang_scope(partition_ordinal);
        assert!(
            !raw_resident(&db, scope),
            "reopen must not hydrate the embedding column's raw partitions"
        );
        assert!(
            !payload_resident(&db, scope),
            "reopen must not hydrate the payload_vec column's raw partitions"
        );
    }
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "an SQ8 partitioned column must reopen with zero resident raw partitions, \
         the same as this file's unquantized fixtures"
    );
    // A reopened partition's saved base graph starts dormant; make it
    // searchable the same way the file's many-partition fixture already
    // does, before any query. Loading a graph generation never loads raw
    // vector bodies (asserted again just below).
    for partition_ordinal in 0..LANG_PARTITIONS {
        let scope = lang_scope(partition_ordinal);
        for target_index in [index(), payload_index()] {
            let route = contextdb_vector::VectorPartitionRef::new(target_index, partition(scope));
            assert!(
                db.vector_store_for_test()
                    .preload_dormant_partition_generation(&route, true)
                    .expect("preload one saved base graph"),
                "partition {partition_ordinal} of {:?} starts with one dormant saved base",
                route.index
            );
            // A low AUTO_INDEX_AT plus this many rows can make maintenance
            // publish an incremental change generation on top of the base.
            // Preload it too when present; it is fine when there is none.
            let _ = db
                .vector_store_for_test()
                .preload_dormant_partition_generation(&route, false);
        }
    }
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "loading graph generations must not load raw vectors"
    );
    (root, db)
}

#[test]
#[serial]
fn public_point_lookup_leaves_unrelated_quantized_partitions_dormant() {
    let (_root, db) = open_lang_residency_fixture(false);
    let before = db.__vector_passive_activity_counters_for_test();
    let row = db
        .point_lookup(
            "raw_residency_items",
            "id",
            &Value::Uuid(lang_id(0, 0)),
            db.snapshot(),
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(&row.values["embedding"], Value::Vector(v) if v.len() == MANY_VECTOR_DIMENSION)
    );
    assert!(
        matches!(&row.values["payload_vec"], Value::Vector(v) if v.len() == MANY_VECTOR_DIMENSION)
    );
    let after = db.__vector_passive_activity_counters_for_test();
    assert_eq!(after.raw_partition_loads - before.raw_partition_loads, 2);
    for partition_ordinal in 1..LANG_PARTITIONS {
        assert!(!raw_resident(&db, lang_scope(partition_ordinal)));
        assert!(!payload_resident(&db, lang_scope(partition_ordinal)));
    }
}

/// Ids returned by an `EXACT` run of `sql` on `db`, the reference every mode
/// comparison in this section checks against.
fn exact_reference_ids(db: &Database, sql_auto: &str, query: &HashMap<String, Value>) -> Vec<Uuid> {
    let exact_sql = sql_auto.replace("USE VECTOR AUTO", "USE VECTOR EXACT");
    let result = db
        .execute(&exact_sql, query)
        .expect("EXACT reference query answers");
    result_ids(&result)
}

/// Candidate derivation loads no vector body it was not asked
/// for. Variant (i) (no relational index on `lang`), both readers, modes
/// `AUTO` and `EXACT`.
#[test]
#[serial]
fn candidate_derivation_loads_no_untouched_vector_body() {
    let (_root, db) = open_lang_residency_fixture(false);
    let query = params([("query", Value::Vector(lang_query(0)))]);
    let sql_auto = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
                    ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 3";
    let reference = exact_reference_ids(&db, sql_auto, &query);

    for mode in ["AUTO", "EXACT"] {
        let sql = sql_auto.replace("USE VECTOR AUTO", &format!("USE VECTOR {mode}"));

        let ordinary = db
            .execute(&sql, &query)
            .unwrap_or_else(|error| panic!("ordinary {mode} candidate search failed: {error}"));
        assert_eq!(
            result_ids(&ordinary),
            reference,
            "{mode} ordinary reader must agree with the EXACT reference"
        );
        for partition_ordinal in 0..LANG_PARTITIONS {
            assert!(
                !payload_resident(&db, lang_scope(partition_ordinal)),
                "{mode} ordinary reader: payload_vec is named by no statement and must stay \
                 non-resident for scope {partition_ordinal}"
            );
        }

        let bounded_result = bounded::execute(&db, &bounded_request(&sql, query.clone()))
            .unwrap_or_else(|error| panic!("bounded {mode} candidate search failed: {error:?}"));
        assert_eq!(
            result_ids(&bounded_result.result),
            reference,
            "{mode} bounded reader must agree with the EXACT reference"
        );
        for partition_ordinal in 0..LANG_PARTITIONS {
            assert!(
                !payload_resident(&db, lang_scope(partition_ordinal)),
                "{mode} bounded reader: payload_vec is named by no statement and must stay \
                 non-resident for scope {partition_ordinal}"
            );
        }
    }
}

/// Projecting the searched column loads only the answer's own
/// partitions. Variant (ii) (relational index on `lang`), both readers,
/// modes `AUTO` and `INDEXED`.
#[test]
#[serial]
fn projecting_the_searched_column_loads_only_answer_partitions() {
    let (_root, db) = open_lang_residency_fixture(true);
    let query = params([("query", Value::Vector(lang_query(0)))]);
    let sql_template = "SELECT id, embedding FROM raw_residency_items WHERE lang = 'en' \
                         ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 3";

    for mode in ["AUTO", "INDEXED"] {
        let sql = sql_template.replace("{mode}", mode);

        let ordinary = db
            .execute(&sql, &query)
            .unwrap_or_else(|error| panic!("ordinary {mode} candidate search failed: {error}"));
        let ordinary_ids = result_ids(&ordinary);
        let expected_scopes: std::collections::BTreeSet<usize> = ordinary_ids
            .iter()
            .filter_map(|id| lang_partition_ordinal_of(*id))
            .collect();
        assert!(
            !expected_scopes.is_empty(),
            "{mode}: the fixture's ids must decode to a seeded partition"
        );

        for partition_ordinal in 0..LANG_PARTITIONS {
            let scope = lang_scope(partition_ordinal);
            assert!(
                !payload_resident(&db, scope),
                "{mode} ordinary reader: payload_vec is named by nothing and must stay \
                 non-resident for scope {partition_ordinal}"
            );
            assert_eq!(
                db.vector_store_for_test()
                    .raw_partition_resident_for_test(&index(), &partition(scope)),
                expected_scopes.contains(&partition_ordinal),
                "{mode} ordinary reader: embedding residency for scope {partition_ordinal} \
                 must follow exactly whether the answer returned one of its rows"
            );
        }

        let bounded_result = bounded::execute(&db, &bounded_request(&sql, query.clone()))
            .unwrap_or_else(|error| panic!("bounded {mode} candidate search failed: {error:?}"));
        assert_eq!(
            result_ids(&bounded_result.result),
            ordinary_ids,
            "{mode}: both readers must return identical ids"
        );
        for partition_ordinal in 0..LANG_PARTITIONS {
            let scope = lang_scope(partition_ordinal);
            assert!(
                !payload_resident(&db, scope),
                "{mode} bounded reader: payload_vec is named by nothing and must stay \
                 non-resident for scope {partition_ordinal}"
            );
            assert_eq!(
                db.vector_store_for_test()
                    .raw_partition_resident_for_test(&index(), &partition(scope)),
                expected_scopes.contains(&partition_ordinal),
                "{mode} bounded reader: embedding residency for scope {partition_ordinal} \
                 must follow exactly whether the answer returned one of its rows; the bounded \
                 kernel must not push the answer's own projected column demand into the \
                 candidate scan"
            );
        }
    }
}

/// A predicate that reads the vector column still gets it.
/// Variant (i), `AUTO`, both readers.
#[test]
#[serial]
fn predicate_reading_the_vector_column_still_loads_it() {
    let (_root, db) = open_lang_residency_fixture(false);
    let query = params([("query", Value::Vector(lang_query(0)))]);
    let sql_auto = "SELECT id FROM raw_residency_items \
                    WHERE embedding IS NOT NULL AND lang = 'en' \
                    ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 3";
    let reference = exact_reference_ids(&db, sql_auto, &query);

    let ordinary = db
        .execute(sql_auto, &query)
        .expect("ordinary AUTO candidate search with a vector-column predicate answers");
    assert_eq!(result_ids(&ordinary), reference);

    let bounded_result = bounded::execute(&db, &bounded_request(sql_auto, query.clone()))
        .expect("bounded AUTO candidate search with a vector-column predicate answers");
    assert_eq!(result_ids(&bounded_result.result), reference);
}

/// A subquery under the candidate filter is untouched.
/// Variant (i), `AUTO`, both readers.
#[test]
#[serial]
fn subquery_under_the_candidate_filter_is_untouched() {
    let (_root, db) = open_lang_residency_fixture(false);
    let query = params([("query", Value::Vector(lang_query(0)))]);
    let plain_sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
                     ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 3";
    let reference = exact_reference_ids(&db, plain_sql, &query);

    let sql_auto = "SELECT id FROM raw_residency_items WHERE lang = 'en' AND id IN \
                    (SELECT id FROM raw_residency_items WHERE embedding IS NOT NULL) \
                    ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 3";

    let ordinary = db
        .execute(sql_auto, &query)
        .expect("ordinary AUTO candidate search with a subquery filter answers");
    assert_eq!(result_ids(&ordinary), reference);

    let bounded_result = bounded::execute(&db, &bounded_request(sql_auto, query.clone()))
        .expect("bounded AUTO candidate search with a subquery filter answers");
    assert_eq!(result_ids(&bounded_result.result), reference);
}

/// A covering relational index supplies candidate identities without loading
/// committed row bodies. The residual-filter control proves that the counter
/// observes real materialization; the trace identifies the maintained graph.
#[test]
#[serial]
fn id_only_candidate_route_step_1_counter_and_trace() {
    let (_root, db) = open_lang_residency_fixture(true);
    let query = params([("query", Value::Vector(lang_query(0)))]);

    let sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
               ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3";

    let before = db.__candidate_derivation_counters_for_test();
    let result = db
        .execute(sql, &query)
        .expect("INDEXED candidate search over a relationally indexed filter answers");
    let after = db.__candidate_derivation_counters_for_test();

    assert_eq!(
        after - before,
        0,
        "a covering candidate route must hand off identities without materializing stored rows"
    );

    assert_eq!(result.trace.physical_plan, "IndexScan -> HNSWSearch");
    assert_eq!(result.trace.index_used.as_deref(), Some("lang_idx"));
    assert!(
        !result.trace.predicates_pushed.is_empty(),
        "the id-only route must not construct a trace with predicates_pushed blanked"
    );
    assert!(
        !result.trace.indexes_considered.is_empty(),
        "the id-only route must not construct a trace with indexes_considered blanked"
    );
    assert!(
        result.trace.rows_examined > 0,
        "an INDEXED filtered vector search must never report zero rows examined while real \
         posting work happened"
    );
    let before_residual = db.__candidate_derivation_counters_for_test();
    let residual = db
        .execute(
            "SELECT id FROM raw_residency_items WHERE lang = 'en' AND scope_id IS NOT NULL \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3",
            &query,
        )
        .unwrap();
    assert_eq!(result_ids(&residual), result_ids(&result));
    assert_eq!(
        db.__candidate_derivation_counters_for_test() - before_residual,
        (LANG_PARTITIONS * LANG_ROWS_PER_PARTITION) as u64,
        "the counter must observe every real row read when a residual requires values"
    );
}

/// An index chooses a driving predicate; other predicates on that same key
/// still decide the answer, and SQL NULL never satisfies a comparison.
#[test]
#[serial]
fn candidate_identity_optimization_preserves_repeated_terms_and_null_comparisons() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE candidate_semantics (id UUID PRIMARY KEY, lang TEXT, quality REAL, embedding VECTOR(2))",
        &empty(),
    ).unwrap();
    db.execute(
        "CREATE INDEX lang_idx ON candidate_semantics(lang)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX quality_idx ON candidate_semantics(quality)",
        &empty(),
    )
    .unwrap();
    for (ordinal, (lang, quality)) in [
        (Value::Null, Value::Null),
        (Value::Text("en".into()), Value::Float64(1.0)),
        (Value::Text("fr".into()), Value::Float64(f64::NAN)),
    ]
    .into_iter()
    .enumerate()
    {
        db.execute(
            "INSERT INTO candidate_semantics (id, lang, quality, embedding) VALUES ($id, $lang, $quality, $vector)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(ordinal as u128 + 1))),
                ("lang", lang),
                ("quality", quality),
                ("vector", Value::Vector(vec![1.0, ordinal as f32])),
            ]),
        ).unwrap();
    }
    let query = params([("query", Value::Vector(vec![1.0, 0.0]))]);
    for predicate in [
        "lang = 'en' AND lang = 'fr'",
        "lang < 'fr'",
        "lang IN ('en', 'fr') AND lang = 'fr'",
        "quality > 0.0",
        "quality < 10.0",
    ] {
        let expected = result_ids(
            &db.execute(
                &format!("SELECT id FROM candidate_semantics WHERE {predicate}"),
                &empty(),
            )
            .unwrap(),
        )
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        let sql = format!(
            "SELECT id FROM candidate_semantics WHERE {predicate} ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10"
        );
        let ordinary = db.execute(&sql, &query).unwrap();
        let bounded = bounded::execute(&db, &bounded_request(&sql, query.clone())).unwrap();
        assert_eq!(
            result_ids(&ordinary)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            expected,
            "ordinary: {predicate}"
        );
        assert_eq!(
            result_ids(&bounded.result)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            expected,
            "bounded: {predicate}"
        );
    }
}

/// Assertion F1 -- inside a transaction, the scan route moves nothing extra:
/// the staged row is visible, and the ids match the same statement run
/// outside the transaction plus that row. Variant (i), `AUTO`, both readers.
#[test]
#[serial]
fn scan_route_read_your_writes_inside_a_transaction() {
    let (_root, db) = open_lang_residency_fixture(false);
    let query = params([("query", Value::Vector(lang_query(0)))]);
    let base_sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
                    ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 3";
    // One more slot than the base query, so the staged row has nowhere to
    // evict an original row from: this is "the same statement plus that
    // row," not "the same statement with one row swapped out."
    let tx_sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
                  ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 4";

    let before_tx = db
        .execute(base_sql, &query)
        .expect("pre-transaction AUTO answer");
    let mut expected: std::collections::BTreeSet<Uuid> =
        result_ids(&before_tx).into_iter().collect();

    let staged_id = Uuid::from_u128(0xCA30_0000_0000_0000_0000_0000_0000_0000);
    db.execute("BEGIN", &empty())
        .expect("open the session transaction");
    insert_lang_row(&db, staged_id, lang_scope(0), lang_query(0));
    expected.insert(staged_id);

    let ordinary_in_tx = db
        .execute(tx_sql, &query)
        .expect("ordinary AUTO candidate search inside the transaction answers");
    let ordinary_ids: std::collections::BTreeSet<Uuid> =
        result_ids(&ordinary_in_tx).into_iter().collect();
    assert_eq!(
        ordinary_ids, expected,
        "the ordinary reader must see the pre-transaction top rows plus the staged row"
    );

    let bounded_in_tx = bounded::execute(&db, &bounded_request(tx_sql, query.clone()))
        .expect("bounded AUTO candidate search inside the transaction answers");
    let bounded_ids: std::collections::BTreeSet<Uuid> =
        result_ids(&bounded_in_tx.result).into_iter().collect();
    assert_eq!(
        bounded_ids, expected,
        "the bounded reader must see the pre-transaction top rows plus the staged row"
    );

    db.execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

/// Both readers merge staged inserts and deletes into the identity-only
/// candidate route, materializing only the staged row and preserving rollback.
#[test]
#[serial]
fn indexed_route_read_your_writes_inside_a_transaction() {
    let (_root, db) = open_lang_residency_fixture(true);
    let query = params([(
        "query",
        Value::Vector(lang_query(MANY_VECTOR_DIMENSION - 1)),
    )]);
    let sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
               ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3";

    // The staged vector uses an axis no committed vector is aligned with,
    // so native quantization cannot turn its first-place score into a tie.
    let deleted_existing_id = lang_id(0, 2);
    let staged_id = Uuid::from_u128(0xCA31_0000_0000_0000_0000_0000_0000_0000);

    db.execute("BEGIN", &empty())
        .expect("open the session transaction");
    insert_lang_row(
        &db,
        staged_id,
        lang_scope(0),
        lang_query(MANY_VECTOR_DIMENSION - 1),
    );
    db.execute(
        "DELETE FROM raw_residency_items WHERE id = $id",
        &params([("id", Value::Uuid(deleted_existing_id))]),
    )
    .expect("stage the removal of the third-closest row");

    let before = db.__candidate_derivation_counters_for_test();
    let ordinary = db
        .execute(sql, &query)
        .expect("ordinary INDEXED candidate search inside the transaction answers");
    let after = db.__candidate_derivation_counters_for_test();

    let bounded_result = bounded::execute(&db, &bounded_request(sql, query.clone()))
        .expect("bounded INDEXED candidate search inside the transaction answers");

    let ordinary_ids = result_ids(&ordinary);
    assert!(
        ordinary_ids.contains(&staged_id),
        "the staged insert must be visible: {ordinary_ids:?}"
    );
    assert!(
        !ordinary_ids.contains(&deleted_existing_id),
        "the staged delete must not be visible: {ordinary_ids:?}"
    );
    assert_eq!(
        result_ids(&bounded_result.result),
        ordinary_ids,
        "both readers must resolve the same ids inside the transaction"
    );
    assert_eq!(
        after - before,
        1,
        "only the one staged row needs a body; committed membership stays id-only"
    );

    db.execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

/// Assertion F3 -- the `id`-shaped candidate fallback resolves a staged
/// insert's identity correctly inside a transaction. Variant (i), `AUTO`,
/// ordinary reader. The self-join gives the candidate subtree a `Join`
/// shape, which is exactly the composed shape whose candidate result comes
/// back keyed by `id` rather than `row_id` (see
/// `prv_28b_filtered_indexed_explain_names_specific_or_generic_recovery` in
/// `sql_surface_tests.rs`, which independently confirms this same compound
/// shape has no complete relational route and so is unreachable by the
/// id-only route -- exactly why 2.5's fix, not 2.7's, is what this pins).
#[test]
#[serial]
fn id_shaped_candidate_fallback_resolves_a_staged_insert() {
    let (_root, db) = open_lang_residency_fixture(false);
    let query = params([("query", Value::Vector(lang_query(0)))]);
    let sql = "SELECT d.id FROM raw_residency_items d \
               INNER JOIN raw_residency_items q ON d.id = q.id \
               WHERE q.lang = 'en' \
               ORDER BY d.embedding <=> $query USE VECTOR AUTO LIMIT 4";

    let staged_id = Uuid::from_u128(0xCA32_0000_0000_0000_0000_0000_0000_0000);
    db.execute("BEGIN", &empty())
        .expect("open the session transaction");
    insert_lang_row(&db, staged_id, lang_scope(0), lang_query(0));

    let result = db
        .execute(sql, &query)
        .expect("the self-join id-shaped candidate search answers");
    assert!(
        result_ids(&result).contains(&staged_id),
        "the id-shaped candidate fallback must resolve the staged insert to its row id: {:?}",
        result_ids(&result)
    );

    db.execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

/// `AUTO` refuses typed when its active limits cannot pay for exact
/// scoring. Variant (i), ordinary reader: a `MEMORY_LIMIT` too small for the exact-score
/// array must refuse identically to `INDEXED`'s refusal on the same
/// variant, without materializing any `payload_vec` partition.
#[test]
#[serial]
fn auto_refuses_typed_when_the_exact_score_array_cannot_fit() {
    let (_root, db) = open_lang_residency_fixture(false);
    let query = params([("query", Value::Vector(lang_query(0)))]);

    let en_row_count = LANG_PARTITIONS * LANG_ROWS_PER_PARTITION;
    let exact_score_bytes = en_row_count.saturating_mul(std::mem::size_of::<(RowId, f32)>());
    let settled_usage = db.accountant().usage().used;
    db.set_memory_limit(Some(settled_usage + exact_score_bytes.saturating_sub(1)))
        .expect("install a memory ceiling just under the exact-score reservation");

    let auto_sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
                    ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 3";
    let auto_error = db
        .execute(auto_sql, &query)
        .expect_err("AUTO must refuse rather than silently exceed the memory ceiling");
    assert!(
        matches!(&auto_error, Error::VectorFilteredRouteUnavailable { .. }),
        "AUTO's refusal must be the typed VectorFilteredRouteUnavailable error: {auto_error:?}"
    );

    let indexed_sql = "SELECT id FROM raw_residency_items WHERE lang = 'en' \
                       ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3";
    let indexed_error = db
        .execute(indexed_sql, &query)
        .expect_err("INDEXED must also refuse on the same variant (no supporting index)");
    assert!(
        matches!(&indexed_error, Error::VectorFilteredRouteUnavailable { .. }),
        "INDEXED's refusal must be the same typed error: {indexed_error:?}"
    );
    assert_eq!(
        std::mem::discriminant(&auto_error),
        std::mem::discriminant(&indexed_error),
        "AUTO's refusal must be identical in kind to INDEXED's refusal on the same variant"
    );

    for partition_ordinal in 0..LANG_PARTITIONS {
        assert!(
            !payload_resident(&db, lang_scope(partition_ordinal)),
            "a refused search must not have materialized any payload_vec partition"
        );
    }
}
