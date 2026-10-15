//! A real over-frame schema migration uses authenticated resumable Iroh sync.
//! Every later assertion is mandatory; a source staging refusal is reported as
//! that prerequisite failure, never converted into an artificial chunk PASS.

use super::*;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_server::peer_dial_spec;
use contextdb_server::transport::iroh::{IrohServer, LargeRequestTestController};

// The pinned production transport/iroh.rs:73 ceiling. This is used only to
// require an actually oversized workload; neither transport nor database
// limits are overridden by this fixture.
const FRAME_BYTES: usize = 64 * 1024 * 1024;
const PADDING_TABLES: usize = 65;
const DEFAULT_BYTES: usize = 1024 * 1024;
const TENANT: &str = "whole-migration-chunked-iroh";

struct RealHub {
    endpoint: Option<IrohServer>,
    controller: LargeRequestTestController,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl RealHub {
    async fn start(db: Arc<Database>, identity: &Path) -> Self {
        let endpoint = IrohServer::bind(&format!("iroh:?identity={}", identity.display()))
            .await
            .expect("real authenticated Iroh hub");
        let controller = endpoint.large_request_test_controller();
        let ticket = endpoint.ticket();
        let server = Arc::new(SyncServer::new(db, &endpoint, TenantId::from(TENANT)));
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move { server.run_until(stop).await }
        });
        Self {
            endpoint: Some(endpoint),
            controller,
            ticket,
            stop,
            task: Some(task),
        }
    }

    async fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.endpoint.take().unwrap().close().await;
        self.controller.resume_for_test();
        self.task
            .take()
            .unwrap()
            .await
            .expect("the stopped authenticated server is reaped");
    }
}

fn install_oversized_source(db: &Database) -> (Vec<DdlChange>, Lsn) {
    let tx = db.begin().expect("begin ONE oversized migration");
    for (ordinal, ddl) in source_statements().iter().enumerate() {
        db.execute_in_tx(tx, ddl, &empty()).unwrap_or_else(|error| {
            panic!("real full-startup prerequisite ordinal={ordinal}: {error}; DDL={ddl}")
        });
    }
    // Defaults are legal schema content carried by the migration itself.
    // No row payload, lowered frame ceiling, raw apply, or transport facsimile.
    let padding = "🦀".repeat(DEFAULT_BYTES / "🦀".len());
    for ordinal in 0..PADDING_TABLES {
        let ddl = format!(
            "CREATE TABLE migration_page_{ordinal:04} (id INTEGER PRIMARY KEY, payload TEXT NOT NULL DEFAULT '{padding}') SYNC OFF"
        );
        db.execute_in_tx(tx, &ddl, &empty()).unwrap_or_else(|error| panic!("legal over-frame schema prerequisite failed at padding table {ordinal}: {error}"));
    }
    db.commit(tx)
        .expect("the real source must commit the entire oversized migration locally");
    let changes = db.changes_since(Lsn(0));
    assert_eq!(
        changes.ddl.len(),
        source_statements().len() + PADDING_TABLES
    );
    let lsn = *changes.ddl_lsn.first().unwrap();
    assert!(changes.ddl_lsn.iter().all(|value| *value == lsn));
    (changes.ddl, lsn)
}

fn assert_nothing_published(db: &Database, node: &str, incarnation: Incarnation) {
    let visible_tables = db.table_names();
    assert!(
        visible_tables
            .iter()
            .all(|name| name == "work_node_contacts"),
        "the hub must publish no application schema while a fragment is staged: {visible_tables:?}"
    );
    assert!(db.list_triggers().is_empty());
    let events = db.event_bus_status();
    assert!(events.event_types.is_empty());
    assert!(events.sinks.is_empty());
    assert!(events.routes.is_empty());
    assert_eq!(
        db.persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(TENANT),
            node,
            incarnation
        )
        .unwrap(),
        None,
        "partial chunks advance no receiver receipt"
    );
}

fn assert_no_application_ddl(db: &Database) {
    assert!(
        db.changes_since(Lsn(0)).ddl.iter().all(|change| matches!(
            change,
            DdlChange::CreateTable { name, .. } if name == "work_node_contacts"
        )),
        "a stored prefix is not a committed application DDL prefix"
    );
}

fn assert_exact_one_ordered_unit(db: &Database, expected: &[DdlChange], phase: &str) {
    let changes = db.changes_since(Lsn(0));
    assert!(
        changes.ddl == expected,
        "{phase}: exact authored DDL vector; actual_count={} expected_count={} first_difference={:?}",
        changes.ddl.len(),
        expected.len(),
        changes
            .ddl
            .iter()
            .zip(expected)
            .position(|(actual, expected)| actual != expected),
    );
    assert_eq!(
        changes.ddl_lsn.len(),
        expected.len(),
        "{phase}: DDL LSN count"
    );
    let lsn = *changes
        .ddl_lsn
        .first()
        .expect("nonempty full startup migration");
    assert!(
        changes.ddl_lsn.iter().all(|value| *value == lsn),
        "{phase}: one receiver commit boundary"
    );
}

fn assert_complete_migration(
    db: &Database,
    source: &Database,
    ddl: &[DdlChange],
    node: &str,
    incarnation: Incarnation,
    source_lsn: Lsn,
) {
    support::assert_full_schema(db, source);
    support::assert_contains_one_ordered_unit(db, Lsn(0), ddl, "oversized received migration");
    assert_eq!(
        db.persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(TENANT),
            node,
            incarnation
        )
        .unwrap(),
        Some(source_lsn)
    );
    assert_full_cg_subset(db);
}

struct ResumeOnDrop(LargeRequestTestController);
impl Drop for ResumeOnDrop {
    fn drop(&mut self) {
        self.0.resume_for_test();
    }
}

const CHUNK_ROOT_ENV: &str = "CONTEXTDB_SCHEMA_CHUNK_CHILD_ROOT";
const CHUNK_MARKER: &str = "SCHEMA_CHUNK_FIRST_FRAGMENT_DURABLE";

fn stage_receipt(
    stage: &contextdb_server::transport::iroh::LargeRequestStageSnapshot,
) -> serde_json::Value {
    serde_json::json!({
        "subject": stage.subject, "authenticated_node_id": stage.authenticated_node_id,
        "unit_digest": stage.unit_digest, "total_bytes": stage.total_bytes,
        "total_fragments": stage.total_fragments,
        "fragments": stage.fragments.iter().map(|fragment| serde_json::json!({
            "sequence": fragment.sequence, "bytes": fragment.bytes, "digest": fragment.digest,
        })).collect::<Vec<_>>(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_schema_first_fragment_crash_child() {
    let Some(root) = std::env::var_os(CHUNK_ROOT_ENV) else {
        let db = Database::open_memory();
        support::install(
            &db,
            &["CREATE TABLE chunk_child_control (id INTEGER PRIMARY KEY)"],
        );
        assert!(db.table_meta("chunk_child_control").is_some());
        return;
    };
    let root = PathBuf::from(root);
    let hub_path = root.as_path().join("hub.db");
    let source_path = root.as_path().join("source.db");
    let source_identity_path = root.as_path().join("source.identity");
    let hub_identity_path = root.as_path().join("hub.identity");
    let source_identity = FabricIdentity::load_or_generate(&source_identity_path).unwrap();
    let node = source_identity.node_id();
    let receiver = Arc::new(Database::open(&hub_path).unwrap());
    let source = Arc::new(Database::open(&source_path).unwrap());
    source.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
    receiver.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
    let hub = RealHub::start(receiver.clone(), &hub_identity_path).await;
    let client = Arc::new(SyncClient::new(
        source.clone(),
        &peer_dial_spec(&hub.ticket, &source_identity_path),
        TenantId::from(TENANT),
    ));
    client
        .bind_application_table_policy(ApplicationTablePolicyExpectation::new())
        .await
        .expect("persist actual authenticated source-to-hub binding");
    let incarnation = source
        .sync_incarnation_for_test(&TenantId::from(TENANT))
        .unwrap();
    let (ddl, source_lsn) = install_oversized_source(&source);
    let pushed_before = client.push_watermark();
    assert!(client.pending_push_change_count().unwrap() > 0);
    let dispatched_before = hub
        .controller
        .observations_for_test()
        .completed_handler_dispatches;
    hub.controller.pause_after_persisted_fragment_for_test(0);
    let resume = ResumeOnDrop(hub.controller.clone());
    let mut pending_push = tokio::spawn({
        let client = client.clone();
        async move { client.push().await }
    });
    tokio::select! {
        () = hub.controller.wait_until_paused_for_test() => {}
        result = &mut pending_push => {
            panic!(
                "the oversized migration finished before its first durable fragment: {result:?}; hub_task_finished={}; observations={:?}; stages={:?}",
                hub.task.as_ref().is_some_and(tokio::task::JoinHandle::is_finished),
                hub.controller.observations_for_test(),
                hub.controller.stage_snapshots_for_test(),
            );
        }
    }
    let staged = hub.controller.stage_snapshots_for_test().unwrap();
    assert_eq!(staged.len(), 1);
    let manifest = staged[0].clone();
    assert_eq!(manifest.authenticated_node_id, node);
    assert_eq!(
        manifest.subject,
        push_subject(TENANT),
        "stage is bound to this tenant's real sync route"
    );
    assert!(manifest.total_bytes > FRAME_BYTES as u64);
    assert!(manifest.total_fragments > 1);
    assert_eq!(manifest.fragments.len(), 1);
    assert_eq!(manifest.fragments[0].sequence, 0);
    assert_eq!(
        hub.controller
            .observations_for_test()
            .completed_handler_dispatches,
        dispatched_before,
        "no application dispatch while staged"
    );
    assert_eq!(client.push_watermark(), pushed_before);
    assert_eq!(
        source
            .persisted_sync_watermarks(&TenantId::from(TENANT))
            .unwrap()
            .0,
        pushed_before
    );
    assert_nothing_published(&receiver, &node, incarnation);
    assert_no_application_ddl(&receiver);

    // Another real authenticated edge can inspect the hub while the first
    // fragment is staged; it receives no prefix of this transaction.
    let other = Arc::new(Database::open(root.as_path().join("second.db")).unwrap());
    let second_identity = root.as_path().join("second.identity");
    let second_client = SyncClient::new(
        other.clone(),
        &peer_dial_spec(&hub.ticket, &second_identity),
        TenantId::from(TENANT),
    );
    second_client
        .bind_application_table_policy(ApplicationTablePolicyExpectation::new())
        .await
        .unwrap();
    second_client
        .pull_default()
        .await
        .expect("second edge remains usable during staging");
    support::assert_no_schema(&other);
    second_client.shutdown().await;
    drop(second_client);

    other.close().unwrap();
    let receipt = serde_json::json!({
        "stage": stage_receipt(&manifest), "source_lsn": source_lsn.0,
        "source_incarnation": incarnation.0.to_string(), "node": node,
        "pushed_before": pushed_before.0, "source_ddl_entries": ddl.len(),
    });
    let mut file = std::fs::File::create(root.join("fragment-boundary.json")).unwrap();
    file.write_all(serde_json::to_vec_pretty(&receipt).unwrap().as_slice())
        .unwrap();
    file.sync_all().unwrap();
    println!("{CHUNK_MARKER}");
    std::io::stdout().flush().unwrap();
    // Keep the real endpoint, database handles and in-flight authenticated push
    // alive. The parent sends SIGKILL here; no close/shutdown precedes the crash.
    let _keep_live = (hub, client, pending_push, resume, receiver, source);
    loop {
        std::thread::park();
    }
}

fn crash_after_first_fragment(root: &Path) {
    use std::os::unix::process::CommandExt as _;
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            &crate::child_test_filter(
                module_path!(),
                "oversized_schema_first_fragment_crash_child",
            ),
            "--nocapture",
        ])
        .env(CHUNK_ROOT_ENV, root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    command.process_group(0);
    let mut process = command
        .spawn()
        .expect("spawn actual oversized migration crash process");
    let stdout = process.stdout.take().unwrap();
    let (sender, receipts) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line.contains(CHUNK_MARKER) => {
                    let _ = sender.send(Ok(CHUNK_MARKER.to_string()));
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    let _ = sender.send(Err(error.to_string()));
                    return;
                }
            }
        }
        let _ = sender.send(Err("oversized migration source/transport prerequisite failed before its durable fragment; child stderr contains the actual error".to_string()));
    });
    let child = CrashChild {
        child: process,
        receipts,
        terminated: false,
    };
    // Event wait only. The admitted runner owns the process-tree deadlock
    // guard; source fixture construction has no invented performance ceiling.
    assert_eq!(
        child
            .receipts
            .recv()
            .expect("chunk boundary channel")
            .expect("actual first-fragment boundary"),
        CHUNK_MARKER
    );
    child.kill_at_boundary();
    reader.join().unwrap();
}

#[ignore = "CDB-163: bounded-storage-rc2 S3/S4 oversized-pull funding campaign (fix SHA pending); re-enabled when rc2 lands on dev."]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry() {
    let root = tempfile::tempdir().unwrap();
    crash_after_first_fragment(root.path());
    let boundary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("fragment-boundary.json")).unwrap())
            .unwrap();
    let hub_path = root.path().join("hub.db");
    let source_path = root.path().join("source.db");
    let source_identity_path = root.path().join("source.identity");
    let hub_identity_path = root.path().join("hub.identity");
    let node = FabricIdentity::load_or_generate(&source_identity_path)
        .unwrap()
        .node_id();
    assert_eq!(boundary["node"].as_str().unwrap(), node);
    let source_lsn = Lsn(boundary["source_lsn"].as_u64().unwrap());
    let pushed_before = Lsn(boundary["pushed_before"].as_u64().unwrap());
    let incarnation = Incarnation(
        boundary["source_incarnation"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
    );
    // Ordinary production writable open recovers the killed source and hub.
    let source = Arc::new(Database::open(&source_path).unwrap());
    let receiver = Arc::new(Database::open(&hub_path).unwrap());
    source.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
    receiver.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
    support::initialize_fixture_callbacks(&source);
    let ddl = source.changes_since(Lsn(0)).ddl;
    assert_eq!(ddl.len(), source_statements().len() + PADDING_TABLES);
    assert_eq!(
        boundary["source_ddl_entries"].as_u64().unwrap(),
        ddl.len() as u64
    );
    assert_exact_one_ordered_unit(&source, &ddl, "reopened source migration");
    assert_full_cg_subset(&source);
    assert_nothing_published(&receiver, &node, incarnation);
    assert_no_application_ddl(&receiver);
    let other = Arc::new(Database::open(root.path().join("second.db")).unwrap());
    let second_identity = root.path().join("second.identity");
    support::assert_no_schema(&other);
    support::initialize_fixture_callbacks(&other);
    let restarted = RealHub::start(receiver.clone(), &hub_identity_path).await;
    let recovered = restarted.controller.stage_snapshots_for_test().unwrap();
    assert_eq!(recovered.len(), 1);
    let manifest = recovered[0].clone();
    assert_eq!(
        stage_receipt(&manifest),
        boundary["stage"],
        "SIGKILL/reopen retains exact authenticated fragment identity and bytes"
    );
    let client = Arc::new(SyncClient::new(
        source.clone(),
        &peer_dial_spec(&restarted.ticket, &source_identity_path),
        TenantId::from(TENANT),
    ));
    let apply_observer = ReceivedSchemaApplyObserverForTest::new();
    let _observation_scope = receiver.observe_received_schema_applies_for_test(&apply_observer);
    let pause = Arc::new(receiver.pause_before_received_schema_durable_commit_for_test());
    let wait_cancelled = Arc::new(AtomicBool::new(false));
    let mut pause_waiter = tokio::task::spawn_blocking({
        let pause = pause.clone();
        let wait_cancelled = wait_cancelled.clone();
        move || {
            while !wait_cancelled.load(Ordering::SeqCst) {
                if pause.wait_until_reached(Duration::from_secs(1)) {
                    return true;
                }
            }
            false
        }
    });
    let mut push = tokio::spawn({
        let client = client.clone();
        async move { client.push().await }
    });
    tokio::select! {
        biased;
        reached = &mut pause_waiter => {
            assert!(reached.expect("join receiver commit-pause waiter"));
        }
        result = &mut push => {
            wait_cancelled.store(true, Ordering::SeqCst);
            pause_waiter.await.expect("join cancelled commit-pause waiter");
            panic!("the complete staged request finished before its one-commit receiver path: {result:?}");
        }
    }
    // The server holds the commit lock here. This test seam reads published
    // memory directly, without entering the public database operation gate.
    let published = receiver.published_state_snapshot_for_test();
    assert_eq!(
        published.table_names,
        vec!["work_node_contacts".to_string()]
    );
    assert_eq!(published.ddl_changes.len(), 1);
    assert!(matches!(
        &published.ddl_changes[0],
        DdlChange::CreateTable { name, .. } if name == "work_node_contacts"
    ));
    assert!(published.event_type_names.is_empty());
    assert!(published.sink_names.is_empty());
    assert!(published.route_names.is_empty());
    assert!(published.trigger_names.is_empty());
    assert!(published.applied_push_watermarks.is_empty());
    // The storage observer independently proves no durable schema artifact
    // crossed the fence. Public schema and receipt are checked after release.
    let before_commit = apply_observer
        .observation()
        .expect("the authenticated received-schema path entered its staging phase");
    assert_eq!(before_commit.durable_table_rewrites, 0);
    assert_eq!(before_commit.durable_rows_rewritten, 0);
    assert!(before_commit.artifact_writes.is_empty());
    assert_eq!(client.push_watermark(), pushed_before);
    let observations = restarted.controller.observations_for_test();
    assert_eq!(
        observations
            .accepted_fragment_sequences
            .iter()
            .map(|v| v.sequence)
            .collect::<Vec<_>>(),
        (1..manifest.total_fragments).collect::<Vec<_>>(),
        "retry sends only missing authenticated fragments"
    );
    assert!(
        observations
            .accepted_fragment_sequences
            .iter()
            .all(|v| v.unit_digest == manifest.unit_digest
                && v.total_bytes == manifest.total_bytes
                && v.total_fragments == manifest.total_fragments)
    );
    // A lost response must reconcile against the durable receipt, never apply
    // this migration a second time. Existing Iroh support drops that response.
    restarted.controller.drop_next_completed_reply_for_test();
    pause.release();
    if let Err(error) = push.await.expect("join real oversized source push") {
        let transport = restarted.controller.observations_for_test();
        panic!(
            "resume and reconcile one complete migration: {error}; hub_task_finished={}; \
             dispatches={}; injected_reply_resets={}; status_probes={}; pull_requests={}; \
             pull_reply_bytes={:?}; pull_reply_traces={:?}; \
             response_manifests={}; requested_chunks={}; served_chunks={}; \
             completed_responses={}; reply_receipts={}; terminal_reply_failures={}",
            restarted
                .task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished),
            transport.completed_handler_dispatches,
            transport.injected_reply_resets,
            transport.authenticated_status_probes_after_reset,
            transport.authenticated_pull_requests_after_reset,
            transport.authenticated_pull_reply_bytes_after_reset,
            transport.authenticated_pull_reply_traces_after_reset,
            transport.staged_response_manifests.len(),
            transport.requested_response_chunks.len(),
            transport.served_response_chunks.len(),
            transport.completed_response_transfers.len(),
            transport.successful_reply_receipts,
            transport.terminal_reply_receipt_failures,
        );
    }
    assert_complete_migration(&receiver, &source, &ddl, &node, incarnation, source_lsn);
    assert_eq!(client.pending_push_change_count().unwrap(), 0);
    let accepted = receiver.changes_since(Lsn(0));
    let after = restarted.controller.observations_for_test();
    assert_eq!(
        after.completed_handler_dispatches, 1,
        "one oversized handler invocation, including lost-reply reconciliation"
    );
    assert_eq!(after.injected_reply_resets, 1);
    client
        .push()
        .await
        .expect("explicit caller retry after accepted migration");
    assert_eq!(receiver.changes_since(Lsn(0)).ddl, accepted.ddl);
    assert_eq!(receiver.changes_since(Lsn(0)).ddl_lsn, accepted.ddl_lsn);
    let second_client = SyncClient::new(
        other.clone(),
        &peer_dial_spec(&restarted.ticket, &second_identity),
        TenantId::from(TENANT),
    );
    second_client
        .pull_default()
        .await
        .expect("second edge receives the complete oversized migration atomically");
    support::assert_full_schema(&other, &source);
    assert_exact_one_ordered_unit(&other, &ddl, "second edge migration");
    support::seed_cg_artifacts(&receiver, 0x90000);
    support::assert_cg_artifacts(&receiver, 0x90000);
    second_client.shutdown().await;
    client.shutdown().await;
    drop(second_client);
    drop(client);
    restarted.stop().await;
    receiver.close().unwrap();
    let reopened = Database::open(&hub_path).unwrap();
    support::assert_contains_one_ordered_unit(
        &reopened,
        Lsn(0),
        &ddl,
        "oversized received migration after hub reopen",
    );
    support::assert_cg_artifacts(&reopened, 0x90000);
    reopened.close().unwrap();
    let file = contextdb_engine::ReadSession::open(&hub_path).unwrap();
    assert_eq!(file.route(), contextdb_core::read_contract::ReadRoute::File);
    support::assert_cg_artifact_queries(|sql, p| file.execute(sql, p).unwrap().rows, 0x90000);
}

impl Drop for RealHub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.controller.resume_for_test();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
