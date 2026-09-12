//! Restart and historical-snapshot promises for maintained partitioned vectors.
//!
//! File-backed databases, explicit close/reopen, SQL, and caller-driven
//! maintenance exercise the supported lifecycle. The filtered restart fixture
//! checks the same bound query before close and after reopen, with deterministic
//! expected rows and graph-use controls through all three query surfaces.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const INDEXED_ROWS: usize = 1_000;
const MAX_MAINTENANCE_CYCLES: usize = 64;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

fn ranked_axis_zero(rank: usize) -> Vec<f32> {
    let score = (1.0 - rank as f32 * 0.0005).clamp(0.05, 1.0);
    vec![score, (1.0 - score * score).max(0.0).sqrt(), 0.0]
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn partitioned_search(db: &Database, scope: Uuid, query: Vec<f32>, mode: &str) -> Vec<Uuid> {
    ids(&db
        .execute(
            &format!(
                "SELECT id FROM restart_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
            ),
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("partitioned {mode} vector search: {error}")))
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("restart_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("a UUID partition component has one canonical key")
}

fn graph_available(db: &Database, index: &VectorIndexRef, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(index, &partition(scope))
        .map(|info| info.graph_available)
        .unwrap_or(false)
}

fn graph_serial(db: &Database, index: &VectorIndexRef, scope: Uuid) -> Option<u64> {
    db.vector_store_for_test()
        .raw_hnsw_build_serial_for_partition_for_test(index, &partition(scope))
}

fn drive_maintenance_until_ready(db: &Database, index: &VectorIndexRef, scope: Uuid) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if graph_available(db, index, scope) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle returns");
    }
    panic!(
        "caller-driven maintenance did not publish a complete maintained route within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

fn create_partitioned_table(db: &Database) {
    db.execute(
        "CREATE TABLE restart_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE AUTO\
         )",
        &empty(),
    )
    .expect("create the partitioned restart fixture");
}

fn insert(db: &Database, id: Uuid, scope: Uuid, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO restart_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(embedding)),
        ]),
    )
    .expect("commit one partitioned vector");
}

#[test]
fn an_open_old_snapshot_and_a_later_historical_snapshot_keep_their_complete_vector_views() {
    let directory = TempDir::new().expect("temporary store directory");
    let path = directory.path().join("snapshot-boundary.db");
    let scope_before = Uuid::from_u128(0x501);
    let scope_after = Uuid::from_u128(0x502);
    let row = Uuid::from_u128(0x503);
    let db = Database::open(&path).expect("open file-backed store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_partitioned_table(&db);
    insert(&db, row, scope_before, axis(0));

    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);

    // Nothing is retirable yet: one partition, one live vector, no deletes
    // or moves, and a registered pin -- so the persisted-catalog branch is
    // also inapplicable. A maintenance cycle here must not begin the
    // guarded removal pass that parks every new read for its duration.
    let retirement_passes_before_noop_cycle = db.vector_partition_retirement_pass_count_for_test();
    db.run_maintenance_cycle()
        .expect("a maintenance cycle with nothing retirable still returns");
    assert_eq!(
        db.vector_partition_retirement_pass_count_for_test(),
        retirement_passes_before_noop_cycle,
        "a maintenance cycle with nothing retirable must not begin the guarded removal pass"
    );
    assert_eq!(
        partitioned_search(&db, scope_before, axis(0), "EXACT"),
        vec![row],
        "a read issued during a no-op retirement cycle completes with its snapshot's values"
    );

    db.execute(
        "UPDATE restart_items SET scope_id = $scope, embedding = $embedding WHERE id = $id",
        &params([
            ("scope", Value::Uuid(scope_after)),
            ("embedding", Value::Vector(axis(1))),
            ("id", Value::Uuid(row)),
        ]),
    )
    .expect("commit the vector move after the old read opened");

    let old_rows = db
        .execute_at_snapshot(
            "SELECT id FROM restart_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope_before)),
                ("query", Value::Vector(axis(0))),
            ]),
            old_snapshot,
        )
        .expect("the pinned old snapshot remains complete after the move");
    assert_eq!(
        ids(&old_rows),
        vec![row],
        "the read opened before commit keeps the old partition and vector view"
    );
    assert_eq!(
        partitioned_search(&db, scope_after, axis(1), "EXACT"),
        vec![row],
        "a new read sees only the committed new partition and vector view"
    );

    db.run_maintenance_cycle()
        .expect("maintenance can run while the old snapshot is pinned");
    assert_eq!(
        ids(&db
            .execute_at_snapshot(
                "SELECT id FROM restart_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                &params([
                    ("scope", Value::Uuid(scope_before)),
                    ("query", Value::Vector(axis(0))),
                ]),
                old_snapshot,
            )
            .expect("maintenance cannot make a pinned old vector view partial"),),
        vec![row]
    );
    drop(old_pin);

    // This is deliberately a later call, after the protection has released.
    // The default history policy retains the row version, so the production
    // SQL surface can still prove the historical EXACT answer.  The current
    // engine has no safe test seam that both retires a compatible vector
    // generation and reports that retirement; that stronger reclaimed-
    // generation INDEXED/AUTO refusal proof is intentionally not guessed here.
    db.run_maintenance_cycle()
        .expect("maintenance after the old snapshot releases returns");
    assert_eq!(
        ids(&db
            .execute_at_snapshot(
                "SELECT id FROM restart_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                &params([
                    ("scope", Value::Uuid(scope_before)),
                    ("query", Value::Vector(axis(0))),
                ]),
                old_snapshot,
            )
            .expect("a later historical EXACT read remains exhaustive"),),
        vec![row],
        "historical raw vectors remain usable after their maintained route may be retired"
    );
}

#[test]
fn partitioned_indexed_search_survives_two_clean_restarts_without_a_query_rebuild() {
    let directory = TempDir::new().expect("temporary store directory");
    let path = directory.path().join("sealed-tail-restart.db");
    let scope = Uuid::from_u128(0x601);
    let first = Uuid::from_u128(0x100_000);
    let after_first_restart = Uuid::from_u128(0x200_000);
    let index = index();

    {
        let db = Database::open(&path).expect("open file-backed store");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_partitioned_table(&db);
        let tx = db.begin_or_panic();
        for ordinal in 0..INDEXED_ROWS {
            db.insert_row(
                tx,
                "restart_items",
                params([
                    (
                        "id",
                        Value::Uuid(Uuid::from_u128(first.as_u128() + ordinal as u128)),
                    ),
                    ("scope_id", Value::Uuid(scope)),
                    ("embedding", Value::Vector(ranked_axis_zero(ordinal))),
                ]),
            )
            .expect("stage a deterministic vector row");
        }
        db.commit(tx).expect("commit the seeded indexed partition");
        drive_maintenance_until_ready(&db, &index, scope);
        db.close().expect("close the sealed fixture cleanly");
    }

    {
        let db = Database::open(&path).expect("first clean reopen");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert!(
            graph_available(&db, &index, scope),
            "opening a sealed partition must make its maintained route available before any query"
        );
        let before_query = graph_serial(&db, &index, scope)
            .expect("the reopened maintained route has a stable observed graph identity");
        assert_eq!(
            partitioned_search(&db, scope, axis(0), "INDEXED")
                .first()
                .copied(),
            Some(first),
            "the sealed pre-restart vector remains findable through INDEXED"
        );
        assert_eq!(
            graph_serial(&db, &index, scope),
            Some(before_query),
            "the first post-restart query must use the already-loaded route, not rebuild it"
        );

        insert(&db, after_first_restart, scope, axis(1));
        assert_eq!(
            partitioned_search(&db, scope, axis(1), "INDEXED")
                .first()
                .copied(),
            Some(after_first_restart),
            "a committed post-restart vector joins the fresh tail without replacing the sealed route"
        );
        db.close().expect("close after the fresh-tail commit");
    }

    let db = Database::open(&path).expect("second clean reopen");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(
        graph_available(&db, &index, scope),
        "the second reopen keeps a complete maintained route available before any query"
    );
    let before_second_query = graph_serial(&db, &index, scope)
        .expect("the second reopened route has a stable observed graph identity");
    assert_eq!(
        partitioned_search(&db, scope, axis(0), "INDEXED")
            .first()
            .copied(),
        Some(first),
        "the original sealed vector survives the second restart"
    );
    assert_eq!(
        partitioned_search(&db, scope, axis(1), "INDEXED")
            .first()
            .copied(),
        Some(after_first_restart),
        "the fresh-tail commit survives the second restart"
    );
    assert_eq!(
        graph_serial(&db, &index, scope),
        Some(before_second_query),
        "neither second-restart query may synchronously rebuild the partition"
    );
}

// -- A relationally-indexed filter must survive reopen on the maintained
// route, exactly like the unfiltered query already does. --

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

fn create_lang_partitioned_table(db: &Database) {
    db.execute(
        "CREATE TABLE restart_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         lang TEXT NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE AUTO\
         )",
        &empty(),
    )
    .expect("create the lang-filtered partitioned restart fixture");
    db.execute(
        "CREATE INDEX restart_items_lang_idx ON restart_items(lang)",
        &empty(),
    )
    .expect("index the lang predicate so the filtered route can derive candidates");
}

fn insert_lang(db: &Database, id: Uuid, scope: Uuid, lang: &str, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO restart_items (id, scope_id, lang, embedding) \
         VALUES ($id, $scope, $lang, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("lang", Value::Text(lang.to_owned())),
            ("embedding", Value::Vector(embedding)),
        ]),
    )
    .expect("commit one lang-tagged partitioned vector");
}

/// Deterministic, strictly decreasing similarity to `axis(0)` across the
/// whole 0..2000 global rank space used by this fixture (two 1,000-row
/// scopes). Unlike `ranked_axis_zero`, the step is small enough that no
/// score clamps, so every one of the 2,000 rows keeps a distinct rank.
fn ranked_lang_vector(global_rank: usize) -> Vec<f32> {
    let score = 1.0 - global_rank as f32 * 0.0001;
    vec![score, (1.0 - score * score).max(0.0).sqrt(), 0.0]
}

fn semantic_ids(rows: Vec<contextdb_engine::SearchResult>) -> Vec<Uuid> {
    rows.iter()
        .map(|row| match row.values.get("id") {
            Some(Value::Uuid(value)) => *value,
            other => panic!("semantic search row id is not a UUID: {other:?}"),
        })
        .collect()
}

fn explain_fact(explain: &str, name: &str, expected: &str) -> bool {
    explain.contains(&format!("{name}={expected}"))
}

/// One scope's row count for the lang-filtered fixture: 10 more than
/// `INDEXED_ROWS` so that after 10 rows are excluded as `"fr"`, the
/// remaining `"en"` subset (1,000 rows) still meets the F32
/// `effective_auto_index_at` crossover on its own -- so a single-scope
/// `AUTO` query over the filtered subset must take the indexed route, not
/// fall back to exact because the allowed count merely looks small.
const LANG_ROWS_PER_SCOPE: usize = INDEXED_ROWS + 10;

/// Seed one scope with `LANG_ROWS_PER_SCOPE` rows: the first ten
/// global-rank positions (the nearest possible vectors) are tagged `"fr"`
/// so the unfiltered top-5 excludes the `"en"` subset entirely, and every
/// remaining row is `"en"` -- a strict subset that still crosses the F32
/// indexed threshold on its own within one partition.
fn seed_lang_scope(db: &Database, scope: Uuid, rank_offset: usize) -> HashMap<usize, Uuid> {
    let mut ids_by_rank = HashMap::new();
    for ordinal in 0..LANG_ROWS_PER_SCOPE {
        let global_rank = rank_offset + ordinal;
        let lang = if ordinal < 10 { "fr" } else { "en" };
        let row_id = Uuid::from_u128(0x700_000 + global_rank as u128);
        insert_lang(db, row_id, scope, lang, ranked_lang_vector(global_rank));
        ids_by_rank.insert(global_rank, row_id);
    }
    ids_by_rank
}

#[test]
fn a_filtered_indexed_search_survives_reopen_on_the_maintained_route() {
    let directory = TempDir::new().expect("temporary store directory");
    let path = directory.path().join("lang-filtered-reopen.db");
    let scope_a = Uuid::from_u128(0x801);
    let scope_b = Uuid::from_u128(0x802);
    let idx = index();
    let query = axis(0);

    let db = Database::open(&path).expect("open file-backed store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_lang_partitioned_table(&db);

    let mut ids_by_rank = seed_lang_scope(&db, scope_a, 0);
    ids_by_rank.extend(seed_lang_scope(&db, scope_b, LANG_ROWS_PER_SCOPE));
    drive_maintenance_until_ready(&db, &idx, scope_a);
    drive_maintenance_until_ready(&db, &idx, scope_b);

    // Global rank 0..9 is "fr" (excluded); the nearest "en" rows start at
    // rank 10. Both are within scope_a, so no scope filter is needed to make
    // the expectation exact and deterministic.
    let expected_unfiltered: Vec<Uuid> = (0..5).map(|rank| ids_by_rank[&rank]).collect();
    let expected_filtered: Vec<Uuid> = (10..15).map(|rank| ids_by_rank[&rank]).collect();
    assert_ne!(
        expected_unfiltered, expected_filtered,
        "the fixture must make the lang filter actually change the answer"
    );

    let filtered_sql = "SELECT id FROM restart_items WHERE scope_id = $scope AND lang = 'en' \
                         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";
    let unfiltered_sql = "SELECT id FROM restart_items WHERE scope_id = $scope \
                           ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";
    let auto_filtered_sql = "SELECT id FROM restart_items WHERE scope_id = $scope AND lang = 'en' \
                              ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 5";
    let bound = params([
        ("scope", Value::Uuid(scope_a)),
        ("query", Value::Vector(query.clone())),
    ]);

    // Both routes must already serve the live handle before restart is tested.
    let unfiltered_before = ids(&db
        .execute(unfiltered_sql, &bound)
        .expect("CONTROL: pre-close unfiltered INDEXED search answers on the live handle"));
    assert_eq!(unfiltered_before, expected_unfiltered);
    assert!(
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "CONTROL: pre-close unfiltered INDEXED must use the maintained graph"
    );

    for bounded_reader in [false, true] {
        let result = if bounded_reader {
            bounded::execute(&db, &bounded_request(filtered_sql, bound.clone()))
                .expect("live bounded filtered INDEXED query")
                .result
        } else {
            db.execute(filtered_sql, &bound)
                .expect("live filtered INDEXED query")
        };
        assert_eq!(ids(&result), expected_filtered);
        assert!(db.__debug_last_query_vector_used_hnsw_for_test());
    }
    let mut live_rust_query = SemanticQuery::new("restart_items", "embedding", query.clone(), 5);
    live_rust_query.search_mode = Some(VectorSearchMode::Indexed);
    live_rust_query.where_clause = Some("lang = 'en'".to_owned());
    assert_eq!(
        semantic_ids(db.semantic_search(live_rust_query).unwrap()),
        expected_filtered
    );
    assert!(db.__debug_last_query_vector_used_hnsw_for_test());

    db.close().expect("close the sealed lang-filtered fixture");

    let db = Database::open(&path).expect("reopen the lang-filtered fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(
        graph_available(&db, &idx, scope_a) && graph_available(&db, &idx, scope_b),
        "both partitions must be reported available immediately after a clean reopen"
    );

    // Assertion (2), CONTROL: the unfiltered INDEXED query must still answer
    // through the maintained route after reopen -- this must stay green.
    let unfiltered_after = ids(&db
        .execute(unfiltered_sql, &bound)
        .expect("CONTROL: unfiltered INDEXED search must answer after reopen"));
    assert_eq!(
        unfiltered_after, expected_unfiltered,
        "CONTROL: the unfiltered post-reopen answer must match the pre-close answer"
    );

    // Assertion (1): the filtered INDEXED query, ordinary door.
    let ordinary_filtered_after = db.execute(filtered_sql, &bound);
    match &ordinary_filtered_after {
        Ok(result) => assert_eq!(
            ids(result),
            expected_filtered,
            "the filtered INDEXED query must answer with the same ids as before close"
        ),
        Err(error) => panic!(
            "filtered INDEXED refuses on the ordinary door after reopen even though the \
             partition's graph is available and the unfiltered query on the same store just \
             answered: {error}"
        ),
    }

    // The bounded reader must preserve the same filtered membership.
    let bounded_filtered_after =
        bounded::execute(&db, &bounded_request(filtered_sql, bound.clone()));
    match &bounded_filtered_after {
        Ok(result) => assert_eq!(
            ids(&result.result),
            expected_filtered,
            "the bounded door's filtered INDEXED query must answer with the same ids as before close"
        ),
        Err(error) => {
            panic!("filtered INDEXED refuses on the bounded door after reopen: {error:?}")
        }
    }

    // Assertion (1), Rust `SemanticQuery` door.
    let mut rust_query = SemanticQuery::new("restart_items", "embedding", query.clone(), 5);
    rust_query.search_mode = Some(VectorSearchMode::Indexed);
    rust_query.where_clause = Some("lang = 'en'".to_owned());
    let rust_filtered_after = db.semantic_search(rust_query);
    match rust_filtered_after {
        Ok(rows) => assert_eq!(
            semantic_ids(rows),
            expected_filtered,
            "the Rust SemanticQuery door's filtered INDEXED query must answer with the same ids"
        ),
        Err(error) => {
            assert!(
                matches!(
                    error,
                    Error::VectorIndexedRouteUnavailable { .. }
                        | Error::VectorFilteredRouteUnavailable { .. }
                ),
                "unexpected error shape for the Rust filtered INDEXED door: {error}"
            );
            panic!("filtered INDEXED refuses on the Rust SemanticQuery door after reopen: {error}");
        }
    }

    // Assertion (3): filtered AUTO must report the graph route, not a brute
    // force over every spanned partition.
    db.__reset_last_query_vector_trace_for_test();
    let auto_filtered_after = ids(&db
        .execute(auto_filtered_sql, &bound)
        .expect("filtered AUTO must answer after reopen on its eligible indexed route"));
    assert_eq!(
        auto_filtered_after, expected_filtered,
        "filtered AUTO must return the same ids as the maintained-route answer"
    );
    let auto_trace = db
        .__take_last_query_vector_trace_for_test()
        .expect("the filtered AUTO query must publish a debug trace");
    assert!(
        auto_trace.used_hnsw && auto_trace.fallback_reason.is_none(),
        "filtered AUTO after reopen must take the maintained graph route with no fallback, \
         trace={auto_trace:?}"
    );

    // Assertion (4): `.explain` of the filtered statement after reopen must
    // report an indexed route and no refusal.
    let explain = contextdb_engine::cli_render::render_explain(&db, filtered_sql, &bound)
        .expect(".explain must describe the filtered INDEXED statement without erroring");
    assert!(
        explain_fact(&explain, "route", "indexed")
            || explain_fact(&explain, "route", "filtered-indexed"),
        ".explain of the reopened filtered INDEXED statement must report an indexed route: {explain}"
    );
    assert!(
        explain_fact(&explain, "refusal", "none"),
        ".explain of the reopened filtered INDEXED statement must report no refusal: {explain}"
    );
}
