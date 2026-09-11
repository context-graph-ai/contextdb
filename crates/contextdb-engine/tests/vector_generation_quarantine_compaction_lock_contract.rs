//! Durable quarantine, generation compaction, snapshot retention, and lock-scope proofs.
//!
//! The fixture drives all customer-visible work through `Database`, SQL, and
//! `GRAPH_TABLE`. The `__*for_test` calls below are deliberately narrow test
//! seams: replacing one catalog envelope, observing journal/generation
//! ownership, holding the normal worker clock while deterministically waking
//! one real engine-owned maintenance cycle, and pausing one typed
//! maintenance/search phase. They neither build a route nor select a query
//! route for the test. Authoritative raw vectors are loaded only through a
//! public exact query.
//!
//! `VectorGenerationCatalogRecordFaultForTest`, the typed pause phases,
//! `VectorSearchDebugTrace::selected_generations`, the controlled worker-clock
//! guard and its monotonically increasing worker-cycle receipt, and their
//! corresponding `__*for_test` methods are intentional compile walls until
//! production exposes them under `test-seams`. They are test seams, not
//! behavioral blockers or product APIs: persistence owns the envelope/key
//! mutation, the real maintenance worker owns every woken cycle, maintenance
//! owns the phases, and the lock owner supplies the nonblocking observation.

#![cfg(feature = "test-seams")]

use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, QueryResult, ReadRoute,
    ReadSession, VectorGenerationCatalogRecordFaultForTest, VectorGraphCallbackPhaseForTest,
    VectorJournalTruncationPhaseForTest, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::{VectorPartitionRef, VectorRouteQuarantineReason};
use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_ROUTE: usize = 32;
const MAX_MAINTENANCE_CYCLES: usize = 32;
const SAFETY_TIMEOUT: Duration = Duration::from_secs(5);

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
    VectorIndexRef::new("generation_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID partition component has one canonical key")
}

fn route(scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(index(), partition(scope))
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

fn ranked(axis: usize, ordinal: usize) -> Vec<f32> {
    let score = 1.0 - ordinal as f32 * 0.01;
    let remainder = (1.0 - score * score).max(0.0).sqrt();
    match axis {
        0 => vec![score, remainder, 0.0],
        1 => vec![remainder, score, 0.0],
        _ => panic!("the fixture defines two deterministic vector axes"),
    }
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector query projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn assert_complete_authoritative_route(
    result: &QueryResult,
    first_id: u128,
    axis: usize,
    what: &str,
) {
    assert_eq!(
        ids(result),
        route_ids(first_id),
        "{what}: every authoritative durable row remains present and canonically ranked"
    );
    let embedding_column = result
        .columns
        .iter()
        .position(|column| column == "embedding" || column.rsplit('.').next() == Some("embedding"))
        .expect("the complete authoritative query projects each stored embedding");
    for (ordinal, row) in result.rows.iter().enumerate() {
        assert_eq!(
            row.get(embedding_column),
            Some(&Value::Vector(ranked(axis, ordinal))),
            "{what}: authoritative vector {ordinal} retains its stored value"
        );
    }
}

fn vector_search(
    db: &Database,
    scope: Uuid,
    query: Vec<f32>,
    mode: &str,
) -> Result<Vec<Uuid>, Error> {
    vector_search_limit(db, scope, query, mode, 1)
}

fn vector_search_limit(
    db: &Database,
    scope: Uuid,
    query: Vec<f32>,
    mode: &str,
    limit: usize,
) -> Result<Vec<Uuid>, Error> {
    db.execute(
        &format!(
            "SELECT id FROM generation_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
        ),
        &params([
            ("scope", Value::Uuid(scope)),
            ("query", Value::Vector(query)),
        ]),
    )
    .map(|result| ids(&result))
}

fn route_ids(first_id: u128) -> Vec<Uuid> {
    (0..ROWS_PER_ROUTE)
        .map(|ordinal| Uuid::from_u128(first_id + ordinal as u128))
        .collect()
}

fn create_schema(db: &Database) {
    db.execute(
        "CREATE TABLE generation_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED \
             CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1)\
         )",
        &empty(),
    )
    .expect("create the maintained partitioned vector table");
    db.execute(
        "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL)",
        &empty(),
    )
    .expect("create the unrelated ordinary table");
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .expect("create graph nodes");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .expect("create graph edges");
}

fn insert_route(db: &Database, scope: Uuid, first_id: u128, axis: usize) {
    let tx = db.begin_or_panic();
    for ordinal in 0..ROWS_PER_ROUTE {
        db.insert_row(
            tx,
            "generation_items",
            params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(first_id + ordinal as u128)),
                ),
                ("scope_id", Value::Uuid(scope)),
                ("embedding", Value::Vector(ranked(axis, ordinal))),
            ]),
        )
        .expect("stage one deterministic route vector");
    }
    db.commit(tx).expect("commit one complete vector route");
}

fn graph_ready(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(&index(), &partition(scope))
        .is_some_and(|info| info.graph_available)
}

fn generation(db: &Database, scope: Uuid) -> contextdb_vector::store::VectorGraphGeneration {
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route(scope))
        .unwrap_or_else(|| panic!("route {scope} has generation status"));
    status
        .base
        .or(status.dormant_base)
        .unwrap_or_else(|| panic!("route {scope} has a durable base generation: {status:?}"))
}

fn generation_status(
    db: &Database,
    scope: Uuid,
) -> contextdb_vector::store::VectorGraphGenerationStatus {
    db.vector_store_for_test()
        .partition_graph_generation_status(&route(scope))
        .unwrap_or_else(|| panic!("route {scope} has generation status"))
}

fn reset_vector_trace(db: &Database) {
    db.__reset_last_query_vector_trace_for_test();
}

fn take_vector_trace(db: &Database, what: &str) -> contextdb_vector::VectorSearchDebugTrace {
    db.__take_last_query_vector_trace_for_test()
        .unwrap_or_else(|| panic!("{what}: the completed public query published no vector trace"))
}

fn drive_maintenance_until_ready(db: &Database, scopes: &[Uuid]) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if scopes.iter().copied().all(|scope| graph_ready(db, scope)) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!(
        "caller-driven maintenance did not publish every required route within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

fn drive_maintenance_until_generation_after(db: &Database, scope: Uuid, prior_generation: u64) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
        if generation(db, scope).generation_id > prior_generation {
            return;
        }
    }
    panic!(
        "caller-driven maintenance did not publish a newer generation within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| {
            panic!(
                "SHOW VECTOR_PARTITIONS exposes {name}: {:?}",
                result.columns
            )
        })
}

fn inspected_route(result: &QueryResult, scope: Uuid) -> &[Value] {
    let key_column = column(result, "partition_key");
    let expected_scope = scope.to_string();
    result
        .rows
        .iter()
        .find(|row| {
            matches!(
                row.get(key_column),
                Some(Value::Json(key)) if key.get("scope_id").and_then(|value| value.as_str()) == Some(expected_scope.as_str())
            )
        })
        .unwrap_or_else(|| panic!("inspection includes the {scope} partition: {result:?}"))
}

fn partition_inspection(db: &Database) -> QueryResult {
    db.execute(
        "SHOW VECTOR_PARTITIONS FOR generation_items.embedding",
        &empty(),
    )
    .expect("passively inspect maintained vector partitions")
}

fn text_at<'a>(result: &QueryResult, row: &'a [Value], name: &str) -> &'a str {
    match row.get(column(result, name)) {
        Some(Value::Text(value)) => value,
        value => panic!("inspection {name} is a stable text fact: {value:?}"),
    }
}

fn assert_ordinary_and_graph_reads(db: &Database, graph_start: Uuid, graph_target: Uuid) {
    assert_eq!(
        db.execute("SELECT body FROM ordinary_notes", &empty())
            .expect("ordinary SQL remains available")
            .rows,
        vec![vec![Value::Text("still-readable".to_owned())]]
    );
    assert_eq!(
        db.execute(
            "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             WHERE a.id = $start COLUMNS (b.id AS target))",
            &params([("start", Value::Uuid(graph_start))]),
        )
        .expect("ordinary graph traversal remains available")
        .rows,
        vec![vec![Value::Uuid(graph_target)]]
    );
}

fn seed_durable_two_route_fixture(
    path: &std::path::Path,
    alpha: Uuid,
    bravo: Uuid,
) -> (Uuid, Uuid) {
    let graph_start = Uuid::from_u128(0x701);
    let graph_target = Uuid::from_u128(0x702);
    let db = Database::open(path).expect("open durable generation fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_route(&db, alpha, 0xA000, 0);
    insert_route(&db, bravo, 0xB000, 1);
    db.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'still-readable')",
        &params([("id", Value::Uuid(Uuid::from_u128(0x700)))]),
    )
    .expect("seed the unrelated ordinary row");
    for id in [graph_start, graph_target] {
        db.execute(
            "INSERT INTO nodes (id) VALUES ($id)",
            &params([("id", Value::Uuid(id))]),
        )
        .expect("seed graph node");
    }
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($id, $source, $target, 'LINKS')",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x703))),
            ("source", Value::Uuid(graph_start)),
            ("target", Value::Uuid(graph_target)),
        ]),
    )
    .expect("seed graph edge");
    drive_maintenance_until_ready(&db, &[alpha, bravo]);
    db.close().expect("close the sealed durable fixture");
    (graph_start, graph_target)
}

/// A bad durable catalog record belongs to one route. Reopening must quarantine that
/// route, not convert its raw vectors or its neighbours into a database-open
/// failure.
#[test]
fn malformed_catalog_record_quarantines_only_that_route_without_passive_inspection_side_effects() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("checksum-quarantine.redb");
    let alpha = Uuid::from_u128(0xA101);
    let bravo = Uuid::from_u128(0xB101);
    let (graph_start, graph_target) = seed_durable_two_route_fixture(&path, alpha, bravo);

    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::MalformedEnvelope,
    )
    .expect("make only alpha's checksummed catalog record malformed");

    let db = Database::open(&path).expect("one corrupt vector route cannot refuse database open");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_ordinary_and_graph_reads(&db, graph_start, graph_target);
    assert_eq!(
        vector_search_limit(&db, alpha, axis(0), "EXACT", ROWS_PER_ROUTE)
            .expect("all raw authoritative vectors remain exact-searchable"),
        route_ids(0xA000),
        "quarantine must not make any of alpha's ordinary durable raw vectors disappear"
    );
    assert!(matches!(
        vector_search(&db, alpha, axis(0), "INDEXED"),
        Err(Error::VectorIndexedRouteUnavailable { index: actual_index }) if actual_index == index()
    ));
    assert_eq!(
        vector_search_limit(&db, bravo, axis(1), "INDEXED", ROWS_PER_ROUTE)
            .expect("healthy route stays indexed"),
        route_ids(0xB000),
        "alpha damage must not disturb bravo's maintained route"
    );

    let before_damaged_status = generation_status(&db, alpha);
    let before_healthy_status = generation_status(&db, bravo);
    let before_charge = db.accountant().usage().used;
    let shown = partition_inspection(&db);
    let after_damaged_status = generation_status(&db, alpha);
    let after_healthy_status = generation_status(&db, bravo);
    assert_eq!(
        db.accountant().usage().used,
        before_charge,
        "SHOW must not charge graph/raw residency"
    );
    assert_eq!(
        after_damaged_status, before_damaged_status,
        "SHOW must not change quarantine, residency, generations, tail counters, or pending work"
    );
    assert_eq!(
        after_healthy_status, before_healthy_status,
        "SHOW must not change healthy-route residency, generations, tail counters, or pending work"
    );
    let damaged = inspected_route(&shown, alpha);
    let healthy = inspected_route(&shown, bravo);
    assert_eq!(text_at(&shown, damaged, "query_state"), "unavailable");
    assert_eq!(
        text_at(&shown, damaged, "availability_reason"),
        "corrupt_base"
    );
    assert_eq!(
        text_at(&shown, damaged, "maintenance_reason"),
        "corrupt_base"
    );
    assert_eq!(
        text_at(&shown, damaged, "recovery_action"),
        "run_maintenance_cycle"
    );
    assert_eq!(text_at(&shown, healthy, "query_state"), "ready");
    assert_eq!(text_at(&shown, healthy, "availability_reason"), "none");
}

#[test]
fn corrupt_journal_quarantines_and_repairs_one_route_while_a_cold_neighbour_builds() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("journal-route-repair.redb");
    let alpha = Uuid::from_u128(0xA111);
    let bravo = Uuid::from_u128(0xB111);
    let charlie = Uuid::from_u128(0xC111);
    seed_durable_two_route_fixture(&path, alpha, bravo);

    {
        let db = Database::open(&path).expect("reopen the sealed fixture for later work");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "INSERT INTO generation_items (id, scope_id, embedding) \
             VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0xAFF0))),
                ("scope", Value::Uuid(alpha)),
                ("embedding", Value::Vector(axis(1))),
            ]),
        )
        .expect("leave one uncovered alpha journal record");
        insert_route(&db, charlie, 0xC000, 0);
        db.close()
            .expect("close with alpha journal work and an unbuilt healthy route");
    }
    Database::__corrupt_vector_partition_journal_record_for_test(&path, &route(alpha))
        .expect("damage only alpha's uncovered journal envelope");

    {
        let db = Arc::new(
            Database::open(&path)
                .expect("one corrupt partition journal cannot reject the whole store"),
        );
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let shown = partition_inspection(&db);
        let damaged = inspected_route(&shown, alpha);
        assert_eq!(text_at(&shown, damaged, "query_state"), "unavailable");
        assert_eq!(
            text_at(&shown, damaged, "availability_reason"),
            "corrupt_changes"
        );
        assert_eq!(
            text_at(&shown, damaged, "recovery_action"),
            "run_maintenance_cycle"
        );
        assert!(matches!(
            vector_search(&db, alpha, axis(0), "INDEXED"),
            Err(Error::VectorIndexedRouteUnavailable { index: actual }) if actual == index()
        ));
        assert_eq!(
            vector_search(&db, alpha, axis(0), "AUTO")
                .expect("AUTO compares the quarantined route exactly within budget"),
            vec![Uuid::from_u128(0xA000)]
        );
        assert_eq!(
            vector_search(&db, bravo, axis(1), "INDEXED")
                .expect("the healthy sealed route remains indexed"),
            vec![Uuid::from_u128(0xB000)]
        );
        assert!(matches!(
            vector_search(&db, charlie, axis(0), "INDEXED"),
            Err(Error::VectorIndexedRouteUnavailable { .. })
        ));

        let never_built_pause =
            db.__arm_vector_maintenance_progress_pause_for_test(&route(charlie), 1);
        let worker_db = db.clone();
        let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
        assert!(
            never_built_pause.wait_until_reached_blocking(),
            "the never-built route starts before repair work"
        );
        let while_never_built_runs = partition_inspection(&db);
        assert_eq!(
            text_at(
                &while_never_built_runs,
                inspected_route(&while_never_built_runs, alpha),
                "availability_reason"
            ),
            "corrupt_changes",
            "the previously-built damaged route has not started repair ahead of the never-built route"
        );
        never_built_pause.release();
        let report = worker
            .join()
            .expect("join the caller-driven maintenance cycle")
            .expect("one cycle repairs alpha and advances the unrelated cold route");
        assert_eq!(
            report.vector.built_partitions, 2,
            "the finite wake repairs the damaged route and builds every sampled cold route"
        );
        for (scope, query, expected) in [
            (alpha, axis(0), Uuid::from_u128(0xA000)),
            (bravo, axis(1), Uuid::from_u128(0xB000)),
            (charlie, axis(0), Uuid::from_u128(0xC000)),
        ] {
            assert_eq!(
                vector_search(&db, scope, query, "INDEXED")
                    .expect("every repaired or healthy route is indexed"),
                vec![expected]
            );
        }
        let repaired = partition_inspection(&db);
        assert_eq!(
            text_at(
                &repaired,
                inspected_route(&repaired, alpha),
                "availability_reason"
            ),
            "none"
        );
        db.close().expect("close the durably repaired store");
    }

    let reopened = Database::open(&path).expect("reopen the repaired journal route");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&reopened, alpha, axis(0), "INDEXED")
            .expect("journal repair survives restart"),
        vec![Uuid::from_u128(0xA000)]
    );
    assert_eq!(
        vector_search(&reopened, charlie, axis(0), "INDEXED")
            .expect("the healthy cold route's progress survives restart"),
        vec![Uuid::from_u128(0xC000)]
    );
}

#[test]
fn corrupt_lazy_base_becomes_typed_unavailable_then_repairs_from_raw_vectors() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("lazy-base-route-repair.redb");
    let alpha = Uuid::from_u128(0xA112);
    let bravo = Uuid::from_u128(0xB112);
    seed_durable_two_route_fixture(&path, alpha, bravo);
    let old_generation = {
        let db = Database::open(&path).expect("read the selected generation identity");
        let generation = generation(&db, alpha);
        db.close().expect("close before physical fault injection");
        generation
    };
    Database::__corrupt_vector_partition_base_record_for_test(&path, &route(alpha))
        .expect("damage only alpha's selected base envelope");

    {
        let db = Database::open(&path)
            .expect("a lazy graph-body fault cannot reject the database at open");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert!(matches!(
            vector_search(&db, alpha, axis(0), "INDEXED"),
            Err(Error::VectorIndexedRouteUnavailable { index: actual }) if actual == index()
        ));
        let shown = partition_inspection(&db);
        let damaged = inspected_route(&shown, alpha);
        assert_eq!(text_at(&shown, damaged, "query_state"), "unavailable");
        assert_eq!(
            text_at(&shown, damaged, "availability_reason"),
            "corrupt_base"
        );
        assert_eq!(
            vector_search(&db, alpha, axis(0), "AUTO")
                .expect("AUTO falls back to authoritative exact vectors"),
            vec![Uuid::from_u128(0xA000)]
        );
        assert_eq!(
            vector_search(&db, bravo, axis(1), "INDEXED")
                .expect("the undamaged route remains indexed"),
            vec![Uuid::from_u128(0xB000)]
        );

        let report = db
            .run_maintenance_cycle()
            .expect("maintenance replaces the corrupt body without decoding it again");
        assert_eq!(report.vector.built_partitions, 1);
        let repaired_generation = generation(&db, alpha);
        assert!(
            repaired_generation.generation_id > old_generation.generation_id,
            "repair publishes a new immutable generation identity"
        );
        assert_eq!(
            vector_search(&db, alpha, axis(0), "INDEXED").expect("the repaired base is indexed"),
            vec![Uuid::from_u128(0xA000)]
        );
        db.close().expect("close after durable body repair");
    }

    let reopened = Database::open(&path).expect("reopen the repaired base route");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&reopened, alpha, axis(0), "INDEXED").expect("body repair survives restart"),
        vec![Uuid::from_u128(0xA000)]
    );
}

#[test]
fn repair_preserves_a_dormant_generation_selected_by_a_held_old_snapshot() {
    let root = TempDir::new().expect("temporary durable repair-retention directory");
    let path = root.path().join("repair-retains-old-snapshot.redb");
    let scope = Uuid::from_u128(0xA113);
    let first_id = 0xA600;
    let later_id = Uuid::from_u128(0xA6F0);

    let db = Database::open(&path).expect("open the repair-retention fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_route(&db, scope, first_id, 0);
    drive_maintenance_until_ready(&db, &[scope]);
    let first_generation = generation(&db, scope);
    db.close().expect("close the first durable generation");

    let db = Database::open(&path).expect("reopen with the first graph dormant");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    db.execute(
        "INSERT INTO generation_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(later_id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(axis(1))),
        ]),
    )
    .expect("commit a newer vector after the old snapshot");
    assert!(
        db.vector_store_for_test().evict_idle_graphs() > 0,
        "the old base returns to its durable dormant descriptor before maintenance"
    );
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("publish the first bounded change generation");
        if generation_status(&db, scope).change.is_some() {
            break;
        }
    }
    assert!(generation_status(&db, scope).change.is_some());
    db.execute(
        "UPDATE generation_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(first_id))),
            ("embedding", Value::Vector(axis(0))),
        ]),
    )
    .expect("commit enough newer work to replace the base-plus-change chain");
    assert!(
        db.vector_store_for_test().evict_idle_graphs() > 0,
        "the superseded chain is dormant before its replacement publishes"
    );
    drive_maintenance_until_generation_after(&db, scope, first_generation.generation_id);
    let second_generation = generation(&db, scope);
    assert!(
        db.vector_store_for_test()
            .make_retired_partition_generations_dormant_for_test(&route(scope))
            > 0,
        "the held old-snapshot chain can be selected from durable dormant descriptors"
    );
    let dormant = db.__debug_vector_partition_generation_retention_for_test(&route(scope));
    assert!(
        dormant.durable_superseded_generations > 0,
        "the held snapshot retains its old durable generation: {dormant:?}"
    );
    assert_eq!(
        dormant.resident_superseded_generations, 0,
        "the proof requires the old generation to remain dormant until its old snapshot selects it"
    );

    assert!(
        db.vector_store_for_test()
            .quarantine_partition_route(&route(scope), VectorRouteQuarantineReason::CorruptBase,),
        "mark the newer previously-built route as requiring repair"
    );
    drive_maintenance_until_generation_after(&db, scope, second_generation.generation_id);

    reset_vector_trace(&db);
    let old_result = db
        .execute_at_snapshot(
            "SELECT id FROM generation_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(axis(0))),
            ]),
            old_snapshot,
        )
        .expect("repair keeps the dormant generation selected by the held old snapshot readable");
    assert_eq!(ids(&old_result), vec![Uuid::from_u128(first_id)]);
    assert_eq!(
        take_vector_trace(&db, "old snapshot after repair").selected_generations,
        vec![first_generation],
        "the old snapshot loads its own compatible generation after newer-route repair"
    );

    drop(old_pin);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("released old-snapshot generations reclaim on a later cycle");
        let retained = db.__debug_vector_partition_generation_retention_for_test(&route(scope));
        if retained.durable_superseded_generations == 0
            && retained.resident_superseded_generations == 0
        {
            return;
        }
    }
    panic!("released repair-retained generations did not reclaim");
}

#[test]
fn committed_file_reader_registers_the_same_isolated_catalog_quarantine() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("read-only-checksum-quarantine.redb");
    let alpha = Uuid::from_u128(0xA109);
    let bravo = Uuid::from_u128(0xB109);
    seed_durable_two_route_fixture(&path, alpha, bravo);

    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::MalformedEnvelope,
    )
    .expect("make only alpha's catalog envelope malformed");

    let reader = ReadSession::open(&path)
        .expect("one corrupt vector route cannot refuse the committed-image door");
    assert_eq!(reader.route(), ReadRoute::File);
    let shown = reader
        .execute("SHOW VECTOR_PARTITIONS", &empty())
        .expect("plain read-only inspection reports durable route state");
    let damaged = inspected_route(&shown, alpha);
    let healthy = inspected_route(&shown, bravo);
    assert_eq!(text_at(&shown, damaged, "query_state"), "unavailable");
    assert_eq!(
        text_at(&shown, damaged, "availability_reason"),
        "corrupt_base"
    );
    assert_eq!(text_at(&shown, healthy, "query_state"), "ready");

    let query = |scope, vector, mode| {
        reader.execute(
            &format!(
                "SELECT id FROM generation_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {ROWS_PER_ROUTE}"
            ),
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(vector)),
            ]),
        )
    };
    assert_eq!(
        ids(&query(alpha, axis(0), "EXACT").expect("raw damaged vectors stay exact-readable")),
        route_ids(0xA000)
    );
    assert!(matches!(
        query(alpha, axis(0), "INDEXED"),
        Err(Error::VectorIndexedRouteUnavailable { index: actual_index }) if actual_index == index()
    ));
    assert_eq!(
        ids(&query(bravo, axis(1), "INDEXED").expect("healthy neighbour stays indexed")),
        route_ids(0xB000)
    );
}

#[cfg(unix)]
#[test]
fn embedded_and_bounded_owner_reads_share_quarantine_and_resource_failure_truth() {
    use std::os::unix::fs::PermissionsExt;

    let root = TempDir::new().expect("temporary lifecycle-parity directory");
    let path = root.path().join("lifecycle-parity.redb");
    let runtime = root.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alpha = Uuid::from_u128(0xA10A);
    let bravo = Uuid::from_u128(0xB10A);
    seed_durable_two_route_fixture(&path, alpha, bravo);
    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::MalformedEnvelope,
    )
    .expect("quarantine alpha before opening the serving owner");

    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the quarantined store with bounded owner reads");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    // Keep enough headroom for the bounded metadata read while remaining far
    // below the generation-repair workspace this fixture requires.
    db.set_memory_limit(Some(db.accountant().usage().used + 16 * 1024))
        .unwrap();
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    let reader =
        ReadSession::with_runtime_directory_for_test(&runtime, || ReadSession::open(&path))
            .expect("the live owner is reachable through the bounded reader");
    assert_eq!(reader.route(), ReadRoute::Owner);

    let embedded = partition_inspection(&db);
    let bounded = reader
        .execute(
            "SHOW VECTOR_PARTITIONS FOR generation_items.embedding",
            &empty(),
        )
        .expect("the bounded owner reader inspects the same snapshot truth");
    for shown in [&embedded, &bounded] {
        let damaged = inspected_route(shown, alpha);
        assert_eq!(text_at(shown, damaged, "query_state"), "unavailable");
        assert_eq!(
            text_at(shown, damaged, "availability_reason"),
            "corrupt_base",
            "quarantine identifies why the maintained route is unavailable"
        );
        assert_eq!(text_at(shown, damaged, "maintenance_state"), "stalled");
        assert_eq!(
            text_at(shown, damaged, "maintenance_reason"),
            "memory_limit",
            "the current resource failure remains the maintenance reason"
        );
        assert_eq!(
            text_at(shown, damaged, "recovery_action"),
            "raise_memory_limit"
        );
    }
    assert_eq!(
        embedded.rows, bounded.rows,
        "embedded and bounded owner reads publish one snapshot-derived lifecycle projection"
    );

    db.set_memory_limit(None).unwrap();
    drive_maintenance_until_ready(&db, &[alpha, bravo]);
    let repaired_embedded = partition_inspection(&db);
    let repaired_bounded = reader
        .execute(
            "SHOW VECTOR_PARTITIONS FOR generation_items.embedding",
            &empty(),
        )
        .expect("bounded inspection follows the completed repair");
    for shown in [&repaired_embedded, &repaired_bounded] {
        let repaired = inspected_route(shown, alpha);
        assert_eq!(text_at(shown, repaired, "query_state"), "ready");
        assert_eq!(text_at(shown, repaired, "availability_reason"), "none");
        assert_eq!(text_at(shown, repaired, "maintenance_state"), "idle");
        assert_eq!(text_at(shown, repaired, "maintenance_reason"), "none");
        assert_eq!(text_at(shown, repaired, "recovery_action"), "none");
    }
    assert_eq!(repaired_embedded.rows, repaired_bounded.rows);
}

/// A physical catalog key can be unreadable even when the catalog envelope it
/// points at still identifies a known raw-vector route. That orphan is isolated
/// to the route it describes; it is never a database-open boundary.
#[test]
fn malformed_physical_catalog_key_is_an_isolated_orphan_not_a_database_failure() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("catalog-key-quarantine.redb");
    let alpha = Uuid::from_u128(0xA108);
    let bravo = Uuid::from_u128(0xB108);
    let (graph_start, graph_target) = seed_durable_two_route_fixture(&path, alpha, bravo);

    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::RelocateToMalformedPhysicalKey,
    )
    .expect("move only alpha's catalog envelope under an unreadable physical key");

    let db = Database::open(&path)
        .expect("an unreadable vector-catalog key cannot refuse the whole database");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_ordinary_and_graph_reads(&db, graph_start, graph_target);
    assert_eq!(
        vector_search_limit(&db, alpha, axis(0), "EXACT", ROWS_PER_ROUTE)
            .expect("all of alpha's authoritative raw vectors remain exact-searchable"),
        route_ids(0xA000)
    );
    assert!(matches!(
        vector_search(&db, alpha, axis(0), "INDEXED"),
        Err(Error::VectorIndexedRouteUnavailable { index: actual_index }) if actual_index == index()
    ));
    assert_eq!(
        vector_search_limit(&db, bravo, axis(1), "INDEXED", ROWS_PER_ROUTE)
            .expect("the unrelated valid catalog key remains indexed"),
        route_ids(0xB000)
    );

    let shown = partition_inspection(&db);
    let damaged = inspected_route(&shown, alpha);
    let healthy = inspected_route(&shown, bravo);
    assert_eq!(text_at(&shown, damaged, "query_state"), "unavailable");
    assert_eq!(
        text_at(&shown, damaged, "availability_reason"),
        "corrupt_base",
        "inspection must expose the isolated unreadable catalog identity without printing its bytes"
    );
    assert_eq!(
        text_at(&shown, damaged, "recovery_action"),
        "run_maintenance_cycle"
    );
    assert_eq!(text_at(&shown, healthy, "query_state"), "ready");
    assert_eq!(text_at(&shown, healthy, "availability_reason"), "none");
}

/// A well-formed catalog envelope from a newer format is the same per-route
/// availability boundary as a malformed catalog, but inspection must preserve
/// the distinct safe reason.
#[test]
fn newer_catalog_format_quarantines_only_that_route() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("format-quarantine.redb");
    let alpha = Uuid::from_u128(0xA102);
    let bravo = Uuid::from_u128(0xB102);
    let (graph_start, graph_target) = seed_durable_two_route_fixture(&path, alpha, bravo);

    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::WellFormedNewerEnvelopeVersion {
            version: u16::MAX,
        },
    )
    .expect("make only alpha's catalog record a checksummed newer-format envelope");

    let db = Database::open(&path).expect("one incompatible route cannot refuse database open");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_ordinary_and_graph_reads(&db, graph_start, graph_target);
    assert_eq!(
        vector_search_limit(&db, alpha, axis(0), "EXACT", ROWS_PER_ROUTE)
            .expect("every vector in the incompatible route remains exact-searchable"),
        route_ids(0xA000)
    );
    assert!(matches!(
        vector_search(&db, alpha, axis(0), "INDEXED"),
        Err(Error::VectorIndexedRouteUnavailable { .. })
    ));
    assert_eq!(
        vector_search_limit(&db, bravo, axis(1), "INDEXED", ROWS_PER_ROUTE)
            .expect("healthy route remains indexed"),
        route_ids(0xB000)
    );
    let before_damaged_status = generation_status(&db, alpha);
    let before_healthy_status = generation_status(&db, bravo);
    let before_charge = db.accountant().usage().used;
    let shown = partition_inspection(&db);
    assert_eq!(
        generation_status(&db, alpha),
        before_damaged_status,
        "SHOW must leave the incompatible route quarantined without loading or rebuilding it"
    );
    assert_eq!(
        generation_status(&db, bravo),
        before_healthy_status,
        "SHOW must not change the healthy route while reporting its neighbour"
    );
    assert_eq!(
        db.accountant().usage().used,
        before_charge,
        "SHOW must not charge graph or raw-vector residency"
    );
    let damaged = inspected_route(&shown, alpha);
    let healthy = inspected_route(&shown, bravo);
    assert_eq!(
        text_at(&shown, damaged, "availability_reason"),
        "incompatible_format"
    );
    assert_eq!(
        text_at(&shown, damaged, "maintenance_reason"),
        "incompatible_format"
    );
    assert_eq!(
        text_at(&shown, damaged, "recovery_action"),
        "run_maintenance_cycle"
    );
    assert_eq!(text_at(&shown, healthy, "query_state"), "ready");
}

#[test]
fn caller_driven_repair_replaces_the_quarantined_generation_and_survives_restart() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("repair-quarantine.redb");
    let alpha = Uuid::from_u128(0xA103);
    let bravo = Uuid::from_u128(0xB103);
    seed_durable_two_route_fixture(&path, alpha, bravo);
    let (old_generation, healthy_generation) = {
        let db = Database::open(&path).expect("open the healthy fixture to record its base");
        let damaged_route_generation = generation(&db, alpha);
        let healthy_route_generation = generation(&db, bravo);
        db.close()
            .expect("close the healthy fixture before durable damage");
        (damaged_route_generation, healthy_route_generation)
    };
    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::MalformedEnvelope,
    )
    .expect("make alpha's catalog record malformed before its repair journey");

    let repaired_generation = {
        let db = Arc::new(Database::open(&path).expect("open the quarantined fixture"));
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let authoritative = db
            .execute(
                "SELECT id, embedding FROM generation_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 32",
                &params([
                    ("scope", Value::Uuid(alpha)),
                    ("query", Value::Vector(axis(0))),
                ]),
            )
            .expect("a public exact query loads alpha's authoritative durable raw vectors");
        assert_complete_authoritative_route(
            &authoritative,
            0xA000,
            0,
            "caller-driven repair input",
        );
        let pause = db.__arm_vector_maintenance_progress_pause_for_test(&route(alpha), 7);
        let copy = db.clone();
        let worker = thread::spawn(move || copy.run_maintenance_cycle());
        let reached = pause.wait_until_reached(Duration::from_secs(10));
        let shown = db
            .execute(
                "SHOW VECTOR_PARTITIONS FOR generation_items.embedding",
                &HashMap::new(),
            )
            .unwrap();
        let progress = db
            .vector_store_for_test()
            .partition_info(&route(alpha).index, &route(alpha).partition_key)
            .unwrap()
            .maintenance_progress;
        pause.release();
        worker.join().unwrap().unwrap();
        assert!(reached);
        assert!(
            shown
                .rows
                .iter()
                .any(|row| text_at(&shown, row, "maintenance_state") == "repairing")
        );
        assert_eq!(progress.unwrap().vectors_done, 7);
        drive_maintenance_until_ready(&db, &[alpha]);
        let repaired = generation(&db, alpha);
        assert!(
            repaired.generation_id > old_generation.generation_id,
            "repair publishes a newer complete base rather than restoring the damaged artifact"
        );
        assert_eq!(
            vector_search_limit(&db, alpha, axis(0), "INDEXED", ROWS_PER_ROUTE)
                .expect("repair publishes an indexed route"),
            route_ids(0xA000)
        );
        assert_eq!(
            generation(&db, bravo),
            healthy_generation,
            "repairing alpha must not republish bravo's healthy generation"
        );
        assert_eq!(
            vector_search_limit(&db, bravo, axis(1), "INDEXED", ROWS_PER_ROUTE)
                .expect("the neighbouring healthy route stays indexed during repair"),
            route_ids(0xB000)
        );
        let inspection = partition_inspection(&db);
        assert_eq!(
            text_at(
                &inspection,
                inspected_route(&inspection, alpha),
                "query_state"
            ),
            "ready",
            "repair clears the quarantined availability state"
        );
        db.close().expect("close the repaired durable fixture");
        repaired
    };

    let db = Database::open(&path).expect("restart after caller-driven repair");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        generation(&db, alpha),
        repaired_generation,
        "restart keeps the repaired complete base"
    );
    assert_eq!(
        generation(&db, bravo),
        healthy_generation,
        "repair and restart preserve the neighbouring generation identity"
    );
    assert_eq!(
        vector_search_limit(&db, alpha, axis(0), "INDEXED", ROWS_PER_ROUTE)
            .expect("repaired route remains indexed after restart"),
        route_ids(0xA000)
    );
}

#[test]
fn engine_owned_worker_repairs_the_quarantined_route_without_a_caller_cycle() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("engine-owned-repair-quarantine.redb");
    let alpha = Uuid::from_u128(0xA109);
    let bravo = Uuid::from_u128(0xB109);
    seed_durable_two_route_fixture(&path, alpha, bravo);
    let (old_generation, healthy_generation) = {
        let db = Database::open(&path).expect("open the healthy fixture to record its bases");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let damaged_route_generation = generation(&db, alpha);
        let healthy_route_generation = generation(&db, bravo);
        db.close()
            .expect("close the healthy fixture before durable damage");
        (damaged_route_generation, healthy_route_generation)
    };
    Database::__mutate_vector_generation_catalog_record_for_test(
        &path,
        &route(alpha),
        VectorGenerationCatalogRecordFaultForTest::MalformedEnvelope,
    )
    .expect("make alpha's catalog record malformed before automatic repair");

    let db = Database::open(&path).expect("open the quarantined fixture under default maintenance");
    let worker_clock = db
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("hold the real worker at a deterministic externally-clocked barrier");
    assert_eq!(
        db.maintenance_policy(),
        MaintenancePolicy::EngineOwned,
        "a fresh handle defaults to engine-owned maintenance"
    );
    assert!(
        db.maintenance_status().running,
        "a vector declaration starts the one engine-owned maintenance worker"
    );
    let initial_inspection = partition_inspection(&db);
    let initially_damaged = inspected_route(&initial_inspection, alpha);
    assert_eq!(
        text_at(&initial_inspection, initially_damaged, "query_state"),
        "unavailable"
    );
    assert_eq!(
        text_at(
            &initial_inspection,
            initially_damaged,
            "availability_reason"
        ),
        "corrupt_base"
    );
    assert_eq!(
        text_at(&initial_inspection, initially_damaged, "recovery_action"),
        "wait_for_automatic_work"
    );
    assert!(matches!(
        vector_search(&db, alpha, axis(0), "INDEXED"),
        Err(Error::VectorIndexedRouteUnavailable { .. })
    ));
    let authoritative = db
        .execute(
            "SELECT id, embedding FROM generation_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 32",
            &params([
                ("scope", Value::Uuid(alpha)),
                ("query", Value::Vector(axis(0))),
            ]),
        )
        .expect("a public exact query loads alpha's authoritative durable vectors");
    assert_complete_authoritative_route(&authoritative, 0xA000, 0, "engine-owned repair input");

    let mut completed_worker_cycle = 0;
    let mut worker_wakes = 0;
    let caller_thread = thread::current().id();
    let mut repaired_inspection = None;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let completed = worker_clock
            .wake_once()
            .expect("one deterministic real worker wake completes one finite maintenance cycle");
        assert!(
            completed.cycle_serial > completed_worker_cycle,
            "each requested wake returns the monotonically increasing serial of a newly completed worker cycle"
        );
        assert_ne!(
            completed.worker_thread_id, caller_thread,
            "the controlled wake must execute on the engine-owned worker, never by synchronously substituting the caller-driven cycle"
        );
        completed_worker_cycle = completed.cycle_serial;
        worker_wakes += 1;
        let shown = partition_inspection(&db);
        let damaged = inspected_route(&shown, alpha);
        if text_at(&shown, damaged, "query_state") == "ready" {
            repaired_inspection = Some(shown);
            break;
        }
    }
    assert!(
        worker_wakes > 0,
        "repair must be advanced by at least one real engine-owned worker wake"
    );
    let repaired_inspection = repaired_inspection.unwrap_or_else(|| {
        panic!(
            "engine-owned maintenance did not report alpha ready within {MAX_MAINTENANCE_CYCLES} finite wakes"
        )
    });
    let repaired_row = inspected_route(&repaired_inspection, alpha);
    assert_eq!(
        text_at(&repaired_inspection, repaired_row, "availability_reason"),
        "none"
    );
    assert_eq!(
        text_at(&repaired_inspection, repaired_row, "maintenance_reason"),
        "none"
    );
    assert_eq!(
        text_at(&repaired_inspection, repaired_row, "recovery_action"),
        "none"
    );
    let repaired_generation = generation(&db, alpha);
    assert!(
        repaired_generation.generation_id > old_generation.generation_id,
        "automatic repair publishes a newer complete generation"
    );
    assert_eq!(
        vector_search_limit(&db, alpha, axis(0), "INDEXED", ROWS_PER_ROUTE)
            .expect("automatic repair restores alpha's indexed route"),
        route_ids(0xA000)
    );
    assert_eq!(
        generation(&db, bravo),
        healthy_generation,
        "automatic repair does not republish the healthy neighbouring route"
    );
    assert_eq!(
        vector_search_limit(&db, bravo, axis(1), "INDEXED", ROWS_PER_ROUTE)
            .expect("the healthy neighbouring route remains indexed"),
        route_ids(0xB000)
    );
    drop(worker_clock);
    db.close()
        .expect("close the automatically repaired durable fixture");

    let db = Database::open(&path).expect("restart after engine-owned repair");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        generation(&db, alpha),
        repaired_generation,
        "restart keeps the engine-owned worker's repaired complete generation"
    );
    assert_eq!(
        generation(&db, bravo),
        healthy_generation,
        "restart keeps the neighbouring route's original generation"
    );
    assert_eq!(
        vector_search_limit(&db, alpha, axis(0), "INDEXED", ROWS_PER_ROUTE)
            .expect("the automatically repaired route remains indexed after restart"),
        route_ids(0xA000)
    );
}

#[test]
fn later_mutations_publish_a_new_generation_truncate_only_covered_journal_and_replay_the_suffix() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("generation-suffix.redb");
    let scope = Uuid::from_u128(0xA104);
    let first = Uuid::from_u128(0xA400);
    let inserted = Uuid::from_u128(0xA4F0);
    let tail = Uuid::from_u128(0xA4F1);
    let db = Database::open(&path).expect("open durable generation fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_route(&db, scope, first.as_u128(), 0);
    drive_maintenance_until_ready(&db, &[scope]);
    let before = generation(&db, scope);
    assert!(
        graph_ready(&db, scope),
        "the first base is already a ready graph"
    );

    db.execute(
        "INSERT INTO generation_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(inserted)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(axis(1))),
        ]),
    )
    .expect("commit a later insert");
    db.execute(
        "UPDATE generation_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(first)),
            ("embedding", Value::Vector(axis(1))),
        ]),
    )
    .expect("commit a later replacement");
    db.execute(
        "DELETE FROM generation_items WHERE id = $id",
        &params([("id", Value::Uuid(Uuid::from_u128(first.as_u128() + 1)))]),
    )
    .expect("commit a later delete");

    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
        if generation_status(&db, scope).change.is_some() {
            break;
        }
    }
    let sealed_change = generation_status(&db, scope)
        .change
        .unwrap_or_else(|| panic!("later mutations did not publish one sealed change layer"));
    assert!(
        sealed_change.covered_tx > before.covered_tx
            && sealed_change.covered_lsn > before.covered_lsn,
        "the sealed change advances the covered frontier beyond the ready base"
    );
    assert!(
        graph_ready(&db, scope),
        "the route remains ready while it owns the sealed change layer"
    );

    db.execute(
        "INSERT INTO generation_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0xA4F2))),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(axis(0))),
        ]),
    )
    .expect("commit work after the sealed change so compaction must replace a ready route");
    let crash = db.__arm_vector_journal_truncation_crash_for_test(
        &route(scope),
        VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
    );
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::BuildFailure),
        "the typed crash seam is attached after the catalog is durable and before covered journal deletion"
    );
    drop(crash);
    db.close()
        .expect("close the deliberately interrupted truncation fixture");

    let db = Database::open(&path)
        .expect("a catalog-published/pre-truncation crash leaves a restartable durable route");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&db, scope, axis(1), "INDEXED")
            .expect("restart keeps the sealed-change route searchable"),
        vec![first],
        "equal axis-one scores use the canonical lower-row-id winner after restart"
    );
    assert!(
        graph_ready(&db, scope),
        "later base maintenance starts with an already ready route after crash-safe restart"
    );
    drive_maintenance_until_generation_after(&db, scope, before.generation_id);
    let compacted = generation(&db, scope);
    assert!(
        compacted.generation_id > before.generation_id,
        "later maintenance publishes a newer base despite the existing ready graph"
    );
    assert!(
        compacted.covered_tx > before.covered_tx,
        "the new base advances covered transaction"
    );
    assert!(
        compacted.covered_lsn > before.covered_lsn,
        "the new base advances covered LSN"
    );

    db.execute(
        "INSERT INTO generation_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(tail)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(axis(2))),
        ]),
    )
    .expect("commit suffix work after the compacted base");
    let journal = db.__debug_vector_partition_journal_file_for_test(&route(scope));
    assert_eq!(journal.truncated_through_lsn, compacted.covered_lsn);
    assert!(
        journal
            .physical_record_lsns
            .iter()
            .all(|lsn| *lsn > compacted.covered_lsn),
        "the journal file physically retains no covered record: {journal:?}"
    );
    assert_eq!(
        journal.physical_record_lsns.len(),
        1,
        "only the later tail commit remains in the journal file for restart replay"
    );

    db.close()
        .expect("close after preserving the one-record journal suffix");
    let db = Database::open(&path).expect("reopen the compacted generation");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        generation(&db, scope),
        compacted,
        "restart selects the compacted durable base"
    );
    assert_eq!(
        vector_search(&db, scope, axis(2), "INDEXED")
            .expect("restart replays the suffix into INDEXED"),
        vec![tail],
        "the later journal suffix remains searchable after restart"
    );
    assert_eq!(
        generation_status(&db, scope).fresh_tail_entries,
        1,
        "restart replays only the post-base suffix, not the compacted history"
    );
}

#[test]
fn pinned_old_snapshot_keeps_its_indexed_generation_until_release_then_reclaims_superseded_bytes() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("snapshot-generation-reclaim.redb");
    let scope = Uuid::from_u128(0xA105);
    let old_id = Uuid::from_u128(0xA500);
    let new_id = Uuid::from_u128(0xA5F0);
    let db = Database::open(&path).expect("open snapshot generation fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_route(&db, scope, old_id.as_u128(), 0);
    drive_maintenance_until_ready(&db, &[scope]);
    let prior_generation = generation(&db, scope);
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    db.execute(
        "INSERT INTO generation_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(new_id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(axis(1))),
        ]),
    )
    .expect("commit later work after the old snapshot opened");
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven change-generation cycle returns");
        if generation_status(&db, scope).change.is_some() {
            break;
        }
    }
    assert!(
        generation_status(&db, scope).change.is_some(),
        "the first post-base mutation publishes its bounded change layer"
    );
    db.execute(
        "UPDATE generation_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(old_id)),
            ("embedding", Value::Vector(axis(0))),
        ]),
    )
    .expect("commit later work that compacts the base-plus-change chain");
    drive_maintenance_until_generation_after(&db, scope, prior_generation.generation_id);
    let current_generation = generation(&db, scope);
    assert!(
        current_generation.generation_id > prior_generation.generation_id,
        "the current route selects the newer base"
    );
    reset_vector_trace(&db);
    let old_result = db
        .execute_at_snapshot(
            "SELECT id FROM generation_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(axis(0))),
            ]),
            old_snapshot,
        )
        .expect("pinned old snapshot retains its compatible INDEXED route");
    assert_eq!(ids(&old_result), vec![old_id]);
    let old_trace = take_vector_trace(&db, "old-snapshot INDEXED query");
    assert!(
        old_trace.used_hnsw,
        "the old snapshot actually uses its maintained route"
    );
    assert_eq!(
        old_trace.selected_generations,
        vec![prior_generation],
        "the old query used the retained old generation, not merely a generic HNSW route"
    );

    reset_vector_trace(&db);
    assert_eq!(
        vector_search(&db, scope, axis(1), "INDEXED")
            .expect("a current read uses the newly published INDEXED route"),
        vec![new_id]
    );
    let current_trace = take_vector_trace(&db, "current INDEXED query");
    assert!(
        current_trace.used_hnsw,
        "the current read actually uses HNSW"
    );
    assert_eq!(
        current_trace.selected_generations,
        vec![current_generation],
        "the current query used the new generation while the old one remained pinned"
    );
    let held = db.__debug_vector_partition_generation_retention_for_test(&route(scope));
    assert!(
        held.durable_superseded_bytes > 0,
        "the pinned old generation retains positive durable ownership: {held:?}"
    );
    assert!(
        held.resident_superseded_bytes > 0,
        "the pinned old generation retains positive resident ownership: {held:?}"
    );
    drop(old_pin);
    let mut reclaimed = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite maintenance cycle after snapshot release returns");
        let retained = db.__debug_vector_partition_generation_retention_for_test(&route(scope));
        if retained.durable_superseded_generations == 0
            && retained.resident_superseded_generations == 0
            && retained.durable_superseded_bytes == 0
            && retained.resident_superseded_bytes == 0
        {
            reclaimed = true;
            break;
        }
    }
    assert!(
        reclaimed,
        "released snapshot did not reclaim superseded durable and in-memory generations within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
    assert_eq!(
        vector_search(&db, scope, axis(1), "INDEXED")
            .expect("reclaiming the superseded generation leaves the current route ready"),
        vec![new_id]
    );
}

#[test]
fn preparation_pauses_do_not_block_an_unrelated_commit_or_hot_route_query() {
    for phase in [
        VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
        VectorMaintenancePreparationPhaseForTest::HashStateDigest,
        VectorMaintenancePreparationPhaseForTest::PrepareDurableGeneration,
    ] {
        let root = TempDir::new().expect("temporary durable preparation directory");
        let db = Arc::new(
            Database::open(root.path().join("preparation.redb"))
                .expect("open durable preparation fixture"),
        );
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_schema(&db);
        let hot = Uuid::from_u128(0xA106);
        let cold = Uuid::from_u128(0xB106);
        insert_route(&db, hot, 0xA600, 0);
        drive_maintenance_until_ready(&db, &[hot]);
        insert_route(&db, cold, 0xB600, 1);
        let pause =
            db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(&route(cold), phase);
        let maintenance_db = Arc::clone(&db);
        let (maintenance_tx, maintenance_rx) = mpsc::channel();
        let maintenance_thread = thread::spawn(move || {
            maintenance_tx
                .send(maintenance_db.run_maintenance_cycle())
                .expect("maintenance result receiver remains live");
        });
        if let Err(error) = pause.wait_until_reached_timeout(SAFETY_TIMEOUT) {
            pause.release();
            let _ = maintenance_rx.recv_timeout(SAFETY_TIMEOUT);
            maintenance_thread
                .join()
                .expect("maintenance thread exits after an unreached pause is released");
            panic!("maintenance did not reach its preparation pause in time: {error}");
        }

        let commit_lock_was_free = db.__try_global_commit_lock_for_test();
        let concurrent_id = Uuid::from_u128(0xB6F0);
        let writer_db = Arc::clone(&db);
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer_thread = thread::spawn(move || {
            let result = (|| {
                writer_db.execute(
                    "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'unrelated')",
                    &params([("id", Value::Uuid(Uuid::from_u128(0xC600)))]),
                )?;
                writer_db.execute(
                    "INSERT INTO generation_items (id, scope_id, embedding) \
                     VALUES ($id, $scope, $embedding)",
                    &params([
                        ("id", Value::Uuid(concurrent_id)),
                        ("scope", Value::Uuid(cold)),
                        ("embedding", Value::Vector(axis(0))),
                    ]),
                )?;
                Ok::<(), Error>(())
            })();
            writer_tx
                .send(result)
                .expect("concurrent writer result receiver remains live");
        });
        let hot_db = Arc::clone(&db);
        let (hot_tx, hot_rx) = mpsc::channel();
        let hot_thread = thread::spawn(move || {
            hot_tx
                .send(vector_search(&hot_db, hot, axis(0), "INDEXED"))
                .expect("hot query result receiver remains live");
        });
        let writer_before_release = writer_rx.recv_timeout(SAFETY_TIMEOUT);
        let hot_before_release = hot_rx.recv_timeout(SAFETY_TIMEOUT);
        pause.release();
        let maintenance_result = maintenance_rx.recv_timeout(SAFETY_TIMEOUT);
        let writer_finished =
            writer_before_release.is_ok() || writer_rx.recv_timeout(SAFETY_TIMEOUT).is_ok();
        let hot_finished =
            hot_before_release.is_ok() || hot_rx.recv_timeout(SAFETY_TIMEOUT).is_ok();
        let maintenance_finished =
            maintenance_result.is_ok() || maintenance_rx.recv_timeout(SAFETY_TIMEOUT).is_ok();
        if writer_finished {
            writer_thread
                .join()
                .expect("the concurrent writer exits after maintenance is released");
        }
        if hot_finished {
            hot_thread
                .join()
                .expect("the concurrent hot reader exits after maintenance is released");
        }
        if maintenance_finished {
            maintenance_thread
                .join()
                .expect("the maintenance thread exits after publishing its result");
        }

        assert!(
            commit_lock_was_free,
            "the global commit lock must be free while preparation is paused"
        );
        writer_before_release
            .expect("ordinary and same-route vector commits finish before preparation is released")
            .unwrap_or_else(|error| panic!("concurrent commits during preparation pause: {error}"));
        assert_eq!(
            hot_before_release
                .expect("the unrelated hot query finishes before preparation is released")
                .expect("hot route stays queryable while another route prepares"),
            vec![Uuid::from_u128(0xA600)]
        );
        maintenance_result
            .expect("released maintenance returns before the safety timeout")
            .unwrap_or_else(|error| panic!("maintenance after preparation pause: {error}"));
        assert_eq!(
            vector_search(&db, cold, axis(0), "INDEXED").expect(
                "publication carries the same-route commit that arrived during preparation"
            ),
            vec![concurrent_id],
            "the short publication step cannot lose a journal suffix committed after its frozen input"
        );
    }
}

#[test]
fn graph_search_callback_pause_does_not_hold_the_generation_publication_mutex() {
    let db = Arc::new(Database::open_memory());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    let hot = Uuid::from_u128(0xA107);
    insert_route(&db, hot, 0xA700, 0);
    drive_maintenance_until_ready(&db, &[hot]);
    let pause = db.__arm_one_shot_vector_graph_callback_pause_for_test(
        &route(hot),
        VectorGraphCallbackPhaseForTest::BeforeSearchCallback,
    );
    let query_db = Arc::clone(&db);
    let (query_tx, query_rx) = mpsc::channel();
    let query_thread = thread::spawn(move || {
        query_tx
            .send(vector_search(&query_db, hot, axis(0), "INDEXED"))
            .expect("paused query result receiver remains live");
    });
    if let Err(error) = pause.wait_until_reached_timeout(SAFETY_TIMEOUT) {
        pause.release();
        let _ = query_rx.recv_timeout(SAFETY_TIMEOUT);
        query_thread
            .join()
            .expect("query thread exits after an unreached callback pause is released");
        panic!("indexed search did not reach its callback pause in time: {error}");
    }
    let publication_lock_was_free =
        db.__try_vector_generation_publication_lock_for_test(&route(hot));
    let second_db = Arc::clone(&db);
    let (second_tx, second_rx) = mpsc::channel();
    let second_thread = thread::spawn(move || {
        second_tx
            .send(vector_search(&second_db, hot, axis(0), "EXACT"))
            .expect("second hot query result receiver remains live");
    });
    let second_before_release = second_rx.recv_timeout(SAFETY_TIMEOUT);
    pause.release();
    let first_result = query_rx.recv_timeout(SAFETY_TIMEOUT);
    let first_finished = first_result.is_ok() || query_rx.recv_timeout(SAFETY_TIMEOUT).is_ok();
    let second_finished =
        second_before_release.is_ok() || second_rx.recv_timeout(SAFETY_TIMEOUT).is_ok();
    if first_finished {
        query_thread
            .join()
            .expect("the paused query thread exits after callback release");
    }
    if second_finished {
        second_thread
            .join()
            .expect("the second hot query thread exits after callback release");
    }

    assert!(
        publication_lock_was_free,
        "a graph-search callback must run after releasing the generation-publication mutex"
    );
    assert_eq!(
        second_before_release
            .expect("an exact reader of the same hot route finishes before callback release")
            .expect("the concurrent hot query succeeds"),
        vec![Uuid::from_u128(0xA700)],
        "one graph callback cannot serialize an exact reader of the same route"
    );
    assert_eq!(
        first_result
            .expect("released indexed query returns before the safety timeout")
            .expect("released indexed query succeeds"),
        vec![Uuid::from_u128(0xA700)]
    );
}
