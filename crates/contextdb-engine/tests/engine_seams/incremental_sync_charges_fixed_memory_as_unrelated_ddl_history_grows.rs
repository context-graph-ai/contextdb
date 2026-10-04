//! Incremental sync uses bounded declaration summaries and preserves historical policy.
use contextdb_core::{TenantId, Value};
use contextdb_engine::database::resident_history_test_seams::observe as summaries;
use contextdb_engine::sync_types::SchemaSyncCapability;
use contextdb_engine::writer_query_test_seams::observe as history;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            memory_limit: Some(64 << 20),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}
fn sql(db: &Database, statement: &str) {
    db.execute(statement, &HashMap::new()).unwrap();
}
fn ids(db: &Database, table: &str) -> Vec<Vec<Value>> {
    db.execute(
        &format!("SELECT id FROM {table} ORDER BY id"),
        &HashMap::new(),
    )
    .unwrap()
    .rows
}
async fn serve(
    db: Arc<Database>,
    broker: &InProcessBroker,
    identity: Arc<FabricIdentity>,
    tenant: &TenantId,
) -> (
    Arc<AtomicBool>,
    tokio::task::JoinHandle<()>,
    Arc<SyncServer>,
) {
    let node = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node),
            tenant.clone(),
            node,
            identity,
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move {
            server.run_until(stop).await;
        }
    });
    broker
        .wait_for_registered_route_for_test(&contextdb_server::subjects::push_subject(
            tenant.as_str(),
        ))
        .await;
    (stop, task, server)
}
async fn stop_server(
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    server: Arc<SyncServer>,
) {
    stop.store(true, Ordering::SeqCst);
    task.await.unwrap();
    drop(server);
}
#[test]
fn incremental_sync_charges_fixed_memory_as_unrelated_ddl_history_grows() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut peaks = Vec::new();
    let mut payloads = Vec::new();
    for occurrences in [32, 256] {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source.redb");
        let hub_path = directory.path().join("hub.redb");
        let source = Arc::new(open(&source_path));
        let hub = Arc::new(open(&hub_path));
        sql(
            &source,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        );
        for _ in 0..occurrences / 4 {
            sql(
                &source,
                "CREATE TABLE unrelated (id INTEGER PRIMARY KEY) SYNC OFF",
            );
            sql(&source, "ALTER TABLE unrelated ADD COLUMN spare INTEGER");
            sql(&source, "ALTER TABLE unrelated SET SYNC OFF");
            sql(&source, "DROP TABLE unrelated");
        }
        sql(
            &source,
            "CREATE TABLE reincarnated (id INTEGER PRIMARY KEY) SYNC OFF",
        );
        sql(&source, "INSERT INTO reincarnated VALUES (10)");
        sql(&source, "ALTER TABLE reincarnated SET SYNC PUSH ONLY");
        sql(&source, "INSERT INTO reincarnated VALUES (20)");
        sql(&source, "ALTER TABLE reincarnated SET SYNC PULL ONLY");
        sql(&source, "INSERT INTO reincarnated VALUES (30)");
        sql(&source, "DROP TABLE reincarnated");
        sql(
            &source,
            "CREATE TABLE reincarnated (id INTEGER PRIMARY KEY) SYNC TWO WAY",
        );
        sql(&source, "INSERT INTO reincarnated VALUES (40)");
        sql(
            &source,
            "CREATE TABLE held (id INTEGER PRIMARY KEY, owner INTEGER) SYNC TWO WAY",
        );
        sql(&source, "CREATE INDEX held_owner ON held (owner)");
        sql(&source, "ALTER TABLE held SET MAINTAIN COUNT (owner)");
        sql(&source, "ALTER TABLE held SET MAINTAIN COUNT NONE");
        // A final deliverable data commit confirms every preceding declaration.
        sql(&source, "INSERT INTO notes VALUES (1,'bootstrap')");
        let tenant = TenantId::from("declaration-summary-growth");
        let source_identity = Arc::new(FabricIdentity::generate());
        let hub_identity = Arc::new(FabricIdentity::generate());
        let broker = InProcessBroker::new();
        let (stop, task, server) =
            runtime.block_on(serve(hub.clone(), &broker, hub_identity.clone(), &tenant));
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.client_as(&source_identity.node_id()),
            tenant.clone(),
            source_identity.clone(),
        );
        runtime.block_on(client.push()).unwrap();
        runtime.block_on(client.pull_default()).unwrap();
        assert_eq!(
            ids(&hub, "reincarnated"),
            vec![vec![Value::Int64(40)]],
            "dropped generation must not leak into its replacement"
        );
        assert_eq!(ids(&hub, "notes"), vec![vec![Value::Int64(1)]]);
        assert_eq!(
            source.table_meta("held").unwrap().maintained_counts,
            Some(contextdb_core::MaintainedCounts::None)
        );
        drop(client);
        runtime.block_on(stop_server(stop, task, server));
        source.close().unwrap();
        hub.close().unwrap();
        drop(source);
        drop(hub);

        let source = Arc::new(open(&source_path));
        let hub = Arc::new(open(&hub_path));
        let broker = InProcessBroker::new();
        let (stop, task, server) =
            runtime.block_on(serve(hub.clone(), &broker, hub_identity.clone(), &tenant));
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.client_as(&source_identity.node_id()),
            tenant.clone(),
            source_identity.clone(),
        );
        client.set_peer_vector_schema_support_for_test(false);
        sql(&source, "INSERT INTO notes VALUES (2,'fixed-delta')");
        sql(&source, "INSERT INTO held VALUES (99,7)");
        // This phase contains data positions only, so declaration history may
        // not be walked even though an actual earlier schema bootstrap did.
        let ((pending, reads), work) = summaries(|| history(|| client.pending_push_change_count()));
        assert_eq!(pending.unwrap(), 2);
        eprintln!("ddl occurrences={occurrences} reduction={work:?} history={reads:?}");
        assert_eq!(
            work.direction_summary_reads, 2,
            "one predecessor lookup for each distinct table/LSN"
        );
        assert_eq!(work.capability_summary_reads, 0);
        assert_eq!(
            reads.history.ddl_occurrences, 0,
            "direction reduction must not walk unrelated immutable declarations"
        );
        source.accountant().reset_high_water_for_test();
        let baseline = source.accountant().usage().used;
        let ((result, reads), work) = summaries(|| history(|| runtime.block_on(client.push())));
        let result = result.unwrap();
        let peak = source
            .accountant()
            .high_water_for_test()
            .checked_sub(baseline)
            .unwrap();
        eprintln!(
            "ddl occurrences={occurrences} push_peak={peak} baseline={baseline} summary={work:?} history={reads:?}"
        );
        assert_eq!(
            work.capability_summary_reads, 2,
            "one capability point read for each changed table, including sticky held vocabulary"
        );
        assert_eq!(
            reads.history.ddl_occurrences, 0,
            "no DDL delta exists in this data-only phase"
        );
        assert_eq!(
            result.applied_rows, 1,
            "compatible fixed row still reaches the authenticated hub"
        );
        assert_eq!(
            ids(&hub, "notes"),
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
        );
        assert!(
            ids(&hub, "held").is_empty(),
            "clearing a current count declaration must not erase immutable capability holdback"
        );
        assert!(
            client
                .schema_sync_holdbacks()
                .iter()
                .any(|held| held.table == "held"
                    && held.capability == SchemaSyncCapability::MaintainedIndexCounts)
        );
        peaks.push(peak);
        payloads.push((
            ids(&hub, "notes"),
            ids(&hub, "held"),
            ids(&hub, "reincarnated"),
        ));
        drop(client);
        runtime.block_on(stop_server(stop, task, server));
        source.close().unwrap();
        hub.close().unwrap();
        drop(source);
        drop(hub);

        let source = Arc::new(open(&source_path));
        let hub = Arc::new(open(&hub_path));
        let broker = InProcessBroker::new();
        let (stop, task, server) =
            runtime.block_on(serve(hub.clone(), &broker, hub_identity, &tenant));
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.client_as(&source_identity.node_id()),
            tenant.clone(),
            source_identity,
        );
        assert!(
            client
                .schema_sync_holdbacks()
                .iter()
                .any(|held| held.table == "held"
                    && held.capability == SchemaSyncCapability::MaintainedIndexCounts),
            "sticky holdback survives both peers reopening"
        );
        client.set_peer_vector_schema_support_for_test(true);
        runtime.block_on(client.push()).unwrap();
        runtime.block_on(client.pull_default()).unwrap();
        assert_eq!(
            ids(&hub, "held"),
            vec![vec![Value::Int64(99)]],
            "capability catchup resumes actual signed work"
        );
        assert!(client.schema_sync_holdbacks().is_empty());
        assert_eq!(ids(&hub, "reincarnated"), vec![vec![Value::Int64(40)]]);
        drop(client);
        runtime.block_on(stop_server(stop, task, server));
        source.close().unwrap();
        hub.close().unwrap();
    }
    assert_eq!(
        payloads[0], payloads[1],
        "unrelated immutable declarations cannot change the fixed authenticated payload"
    );
    assert_eq!(
        peaks[0], peaks[1],
        "actual writer peak for a fixed sync delta cannot grow with unrelated declaration history"
    );
}
