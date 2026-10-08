//! Acceptance: "Durable rows, indexes and their metadata must load on demand rather than remain
//! wholly resident." "Writable open needs measured work/memory evidence, without a new numeric
//! latency threshold."

use crate::resident_history_support::{empty, exec, open, vector};
use contextdb_core::{Lsn, TenantId, Value, Wallclock};
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::writer_query_test_seams::observe;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};
use uuid::Uuid;

#[test]
fn writable_open_never_touches_history_families() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let mut measurements = Vec::new();
    let mut populations = Vec::new();
    for rows in [32_i64, 64] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.redb");
        Database::__create_region_fixture_for_test(&path, 128 * 1024).unwrap();
        prepare_sidecars(&path, rows as usize);
        let database = Database::open(&path).unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        database.execute("CREATE TABLE history_items (id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, payload TEXT, embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE EXACT) HISTORY ALL", &HashMap::new()).unwrap();
        database
            .execute(
                "CREATE INDEX history_scope ON history_items (scope, id)",
                &HashMap::new(),
            )
            .unwrap();
        for id in 1..=rows {
            database
                .execute(
                    "INSERT INTO history_items VALUES ($id, 1, 'original', $vector)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(id)),
                        ("vector".to_owned(), Value::Vector(vec![id as f32, 1.0])),
                    ]),
                )
                .unwrap();
            database
                .execute(
                    "UPDATE history_items SET payload = 'current' WHERE id = $id",
                    &HashMap::from([("id".to_owned(), Value::Int64(id))]),
                )
                .unwrap();
            let tx = database.begin_or_panic();
            database
                .insert_edge(
                    tx,
                    Uuid::from_u128(id as u128 * 2),
                    Uuid::from_u128(id as u128 * 2 + 1),
                    "links".to_owned(),
                    HashMap::new(),
                )
                .unwrap();
            database.commit(tx).unwrap();
        }
        let durable_before = database.__durable_sidecars_for_test().unwrap();
        let ddl_before = database.__durable_ddl_for_test().unwrap();
        assert!(
            ddl_before.len() >= rows as usize / 2,
            "durable DDL occurrence fixture grows with sidecar history"
        );
        let mut counts = [0_usize; 8];
        for (key, _) in &durable_before.1 {
            for (family, prefix) in [
                "sync_ddl_received_arrival.v1.",
                "sync_ddl_superseded.v1.",
                "sync_row_lineage.",
                "sync_graph_arrival.v1.",
                "sync_lineage.v1.record.",
                "sync_terminal_refusal.v1.",
                "retention_expiry.v1.",
                "sync_row_author.",
            ]
            .iter()
            .enumerate()
            {
                if key.starts_with(prefix) {
                    counts[family] += 1;
                }
            }
        }
        assert!(
            counts.iter().all(|count| *count > 0),
            "every required durable sidecar family is populated before open: {counts:?}"
        );
        populations.push((durable_before.0, counts, ddl_before.len()));
        let prior_snapshot = database.snapshot();
        let prior_lsn = database.current_lsn();
        database.close().unwrap();
        drop(database);
        let (opened, work) = observe(|| Database::open(&path));
        let database = opened.unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(
            database.snapshot(),
            prior_snapshot,
            "open restores the exact committed transaction bound"
        );
        assert_eq!(
            database.current_lsn(),
            prior_lsn,
            "open restores the exact committed log bound"
        );
        assert_eq!(
            database.__durable_sidecars_for_test().unwrap(),
            durable_before,
            "every exact durable sidecar key and encoded record survives writable reopen"
        );
        assert_eq!(
            database.__durable_ddl_for_test().unwrap(),
            ddl_before,
            "all durable DDL occurrences survive exact readback"
        );
        // Positive access oracles exclude an empty store masquerading as a cheap open.
        let current = database
            .execute(
                "SELECT id, payload FROM history_items WHERE scope = 1 ORDER BY id",
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            current.rows,
            (1..=rows)
                .map(|id| vec![Value::Int64(id), Value::Text("current".to_owned())])
                .collect::<Vec<_>>()
        );
        let vectors = database.execute("SELECT id FROM history_items WHERE scope = 1 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 64", &HashMap::from([("query".to_owned(), Value::Vector(vec![0.0, 1.0]))])).unwrap();
        assert_eq!(
            vectors.rows,
            (1..=rows)
                .map(|id| vec![Value::Int64(id)])
                .collect::<Vec<_>>(),
            "all saved memberships remain searchable after lazy open"
        );
        assert_eq!(
            database
                .edge_count(Uuid::from_u128(2), "links", database.snapshot())
                .unwrap(),
            1
        );
        let tx = database.begin_or_panic();
        assert!(
            tx.0 > prior_snapshot.0,
            "the next writer never reuses a committed transaction identity"
        );
        database.rollback(tx).unwrap();
        let received_schema_read_memory_1 = database
            .declared_reader_memory()
            .expect("admit the sync observation reader");

        assert!(
            database
                .has_terminal_refusal_markers_for_test(
                    &TenantId::from("resident-fixture"),
                    "fixture-hub",
                    received_schema_read_memory_1
                )
                .unwrap(),
            "point-read terminal refusal evidence remains available after open"
        );
        assert_eq!(
            exec(&database, "SELECT COUNT(*) FROM expired_items"),
            vec![vec![Value::Int64(0)]]
        );
        assert_eq!(
            exec(&database, "SELECT revision FROM items WHERE id = 0"),
            vec![vec![Value::Int64(1)]]
        );
        database.close().unwrap();
        measurements.push(work.history);
    }
    assert!(
        populations[1].0 > populations[0].0 && populations[0].0 > 1,
        "N/2N fixture grows independently observed real allocator region population: {populations:?}"
    );
    assert!(
        populations[1].2 > populations[0].2,
        "DDL occurrence population must grow independently: {populations:?}"
    );
    for family in 0..8 {
        assert!(
            populations[1].1[family] > populations[0].1[family],
            "each durable family grows with N/2N: family={family}; {populations:?}"
        );
    }
    eprintln!(
        "durable family and allocator populations: {populations:?}; open work: {measurements:?}"
    );
    for (arm, work) in measurements.iter().enumerate() {
        assert_eq!(
            (
                work.ddl_occurrences,
                work.sidecar_entries,
                work.sidecar_decodes,
                work.selection_entries
            ),
            (0, [0; 8], [0; 8], 0),
            "writable open enumerates no DDL/received/superseded/row/graph/tombstone/refusal/expiry sidecars: arm={arm}; {work:?}"
        );
    }
    assert_eq!(
        measurements[0].allocator_regions_loaded, measurements[1].allocator_regions_loaded,
        "allocator region loading stays fixed when durable sidecar history doubles"
    );
    assert!(
        measurements[0].allocator_regions_loaded > 0,
        "the open observation must witness the real allocator-state region load"
    );
    for (arm, work) in measurements.iter().enumerate() {
        assert_eq!(
            (
                work.row_versions,
                work.index_rebuild_inputs,
                work.graph_versions,
                work.change_log_entries,
                work.commit_index_entries,
                work.vector_memberships
            ),
            (0, 0, 0, 0, 0, 0),
            "writable open must decode/iterate zero history entries in arm {arm}; observed={work:?}"
        );
    }
    assert_eq!(
        measurements[0].commit_bound_keys, measurements[1].commit_bound_keys,
        "open reads a constant number of commit bounds keys as history doubles"
    );
    assert!(
        measurements[0].commit_bound_keys > 0,
        "the bounds observer must witness the physical committed-index bound lookup"
    );
}

fn prepare_sidecars(path: &Path, n: usize) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let source = Database::open_memory();
    source.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
    exec(
        &source,
        "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(16) PARTITION_KEY (scope) MAX_PARTITIONS 2, revision INT) HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    exec(
        &source,
        "CREATE INDEX items_revision ON items (revision,id)",
    );
    // Large nonresident bodies grow the actual durable page allocator, too.
    exec(
        &source,
        "CREATE TABLE payloads (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
    );
    for id in 0..n {
        source
            .execute(
                "INSERT INTO items VALUES ($id,0,$v,0)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("v".into(), Value::Vector(vector(id, 16))),
                ]),
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO payloads VALUES ($id,$body)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    (
                        "body".into(),
                        Value::Text(format!("{id}:{}", "p".repeat(8192))),
                    ),
                ]),
            )
            .unwrap();
        let tx = source.begin_or_panic();
        source
            .insert_edge(
                tx,
                uuid::Uuid::from_u128(20000 + id as u128),
                uuid::Uuid::from_u128(20001 + id as u128),
                "LINKS".into(),
                empty(),
            )
            .unwrap();
        source.commit(tx).unwrap();
    }
    let db = Arc::new(open(path, Some(64 * 1024 * 1024)));
    db.apply_changes(
        source
            .changes_since(Lsn(0))
            .expect("read the declared changes"),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    for id in 0..n {
        exec(
            &db,
            &format!("UPDATE items SET revision = 1 WHERE id = {id}"),
        );
    }
    let received_source_lsn = source.current_lsn();
    // Authored tombstone and graph-arrival sidecars must participate in growth.
    for id in 0..n / 4 {
        exec(&source, &format!("DELETE FROM payloads WHERE id = {id}"));
    }
    db.apply_changes(
        source
            .changes_since(received_source_lsn)
            .expect("read the declared changes"),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    populate_authenticated_rows(&db, n);
    let ddl_since = source.current_lsn();
    for ordinal in 0..n / 4 {
        let ddl = format!(
            "ALTER TABLE items ALTER COLUMN v SET HNSW (EF_SEARCH = {})",
            32 + ordinal
        );
        exec(&db, &ddl);
        exec(&source, &ddl);
    }
    db.apply_changes(
        source
            .changes_since(ddl_since)
            .expect("read the declared changes"),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();

    // Real authenticated-reply semantics populate terminal refusal evidence;
    // the helper is the existing test-only door to the actual reply commit.
    exec(
        &db,
        "CREATE TABLE refused_items (id INT PRIMARY KEY) HISTORY ALL SYNC TWO WAY",
    );
    for id in 0..n / 4 {
        exec(&db, &format!("INSERT INTO refused_items VALUES ({id})"));
    }
    let refused = db
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .rows
        .into_iter()
        .filter(|row| row.table == "refused_items" && !row.deleted)
        .collect::<Vec<_>>();
    assert_eq!(refused.len(), n / 4);
    let tenant = TenantId::from("resident-fixture");
    db.register_retention_sync_peer("fixture-hub").unwrap();
    db.record_hub_push_reply_effects_for_hub(
        &tenant,
        "fixture-hub",
        &refused,
        &[],
        &[],
        Lsn(900_000),
    )
    .unwrap();
    let received_schema_read_memory_2 = db
        .declared_reader_memory()
        .expect("admit the sync observation reader");

    assert!(
        db.has_terminal_refusal_markers_for_test(
            &tenant,
            "fixture-hub",
            received_schema_read_memory_2
        )
        .unwrap()
    );

    // A retained old frontier keeps real, durable expiry identities after
    // current answers expire; no observer-created mirror is inserted.
    exec(
        &db,
        "CREATE TABLE expired_items (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    for id in 0..n / 4 {
        db.execute(
            "INSERT INTO expired_items VALUES ($id,$expiry)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("expiry".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    let expiry_frontier = db.snapshot();
    let pin = db.pin_snapshot(expiry_frontier);
    db.run_pruning_cycle_checked().unwrap();
    assert_eq!(
        exec(&db, "SELECT COUNT(*) FROM expired_items"),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        db.execute_at_snapshot(
            "SELECT COUNT(*) FROM expired_items",
            &empty(),
            expiry_frontier
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64((n / 4) as i64)]]
    );
    drop(pin);
    db.close().unwrap();
}

// Raw apply deliberately carries no authenticated author/lineage evidence.
// Use an actual signed sync push to populate those production sidecars.
fn populate_authenticated_rows(receiver: &Arc<Database>, n: usize) {
    let sender = Arc::new(Database::open_memory());
    sender.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    exec(
        &sender,
        "CREATE TABLE authored_rows (id INT PRIMARY KEY, payload INT) HISTORY ALL SYNC TWO WAY",
    );
    for id in 0..n {
        exec(
            &sender,
            &format!("INSERT INTO authored_rows VALUES ({id}, 1)"),
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let tenant = TenantId::from("startup-sidecar-fixture");
        let broker = InProcessBroker::new();
        let hub_identity = Arc::new(FabricIdentity::generate());
        let hub_node = hub_identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                receiver.clone(),
                broker.server_as(&hub_node),
                tenant.clone(),
                hub_node.clone(),
                hub_identity,
            )
            .expect("construct the authenticated sync endpoint"),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let server = server.clone();
            async move { server.run_until(stop).await }
        });
        broker
            .wait_for_registered_route_for_test(&contextdb_server::subjects::push_subject(
                "startup-sidecar-fixture",
            ))
            .await;
        let identity = Arc::new(FabricIdentity::generate());
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            sender.clone(),
            broker.client_as(&identity.node_id()),
            tenant,
            identity,
        )
        .expect("construct the authenticated sync endpoint");
        client.push().await.unwrap();
        task.abort();
        let _ = task.await;
    });
    for id in 0..n / 4 {
        exec(
            receiver,
            &format!("DELETE FROM authored_rows WHERE id = {id}"),
        );
    }
}
