//! Fail-safe state transitions around an explicit authoritative-hub move.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_server::protocol::{
    MessageType, PushResponse, SyncStatusResponse, WireApplyResult, WireConflict, encode,
};
use contextdb_server::subjects::{push_subject, status_subject};
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "destination-safety";
const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) \
                   SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn declare(db: &Database) {
    db.execute(DDL, &HashMap::new()).expect("declare notes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decoded_old_pull_row_cannot_apply_after_a_destination_move() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = InProcessBroker::new();
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open hub"));
    declare(&hub);
    let declared_through = hub.current_lsn();
    hub.execute(
        "INSERT INTO notes (id, body) VALUES (41, 'old hub row')",
        &HashMap::new(),
    )
    .expect("seed authenticated hub row");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open edge"));
    declare(&edge);
    edge.persist_sync_pull_watermark(&TenantId::from(TENANT), declared_through)
        .expect("skip already-declared schema in focused pull");
    let identity = Arc::new(
        FabricIdentity::load_or_generate(&root.path().join("hub.fabric-identity.key"))
            .expect("hub identity"),
    );
    let hub_node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub,
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&status_subject(TENANT))
        .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = Arc::new(
        SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&edge_node_id),
            TenantId::from(TENANT),
            edge_identity,
        ),
    );
    let pause = client.pause_after_pull_response_for_test();
    let mut pull = tokio::spawn({
        let client = client.clone();
        async move { client.pull_default().await }
    });
    tokio::select! {
        _ = pause.wait_until_reached() => {}
        result = &mut pull => panic!("real authenticated pull failed before the decoded-response fence: {:?}", result),
    }
    client
        .change_destination("new-hub")
        .expect("move after decoded old pull page");
    pause.release();
    let error = pull
        .await
        .expect("old pull task")
        .expect_err("old pull cannot apply its decoded row after the move");
    assert!(
        error.to_string().contains("authoritative destination"),
        "the decoded old pull must fail at its guarded apply boundary: {error}"
    );
    let pulled_key = NaturalKey::single("id".to_string(), Value::Int64(41));
    assert!(
        edge.execute("SELECT * FROM notes WHERE id = 41", &HashMap::new())
            .expect("inspect edge after rejected old pull")
            .rows
            .is_empty(),
        "the decoded old-hub row cannot become visible after the move"
    );
    assert!(
        !edge.row_version_arrived_by_sync("notes", &pulled_key),
        "the decoded old-hub row cannot leave pulled provenance after the move"
    );
    assert_eq!(
        edge.persisted_sync_watermarks(&TenantId::from(TENANT))
            .expect("read post-move watermarks"),
        (Lsn(0), Lsn(0)),
        "the old pull page cannot restore either progress frontier"
    );
    assert_eq!(
        client.pull_watermark(),
        Lsn(0),
        "the decoded old pull cannot restore this client's in-memory pull frontier"
    );
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("hub server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decoded_pull_row_obeys_sync_off_declared_before_apply() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = InProcessBroker::new();
    let hub = Arc::new(Database::open(root.path().join("direction-hub.db")).expect("open hub"));
    declare(&hub);
    let declared_through = hub.current_lsn();
    hub.execute(
        "INSERT INTO notes (id, body) VALUES (42, 'must stay remote')",
        &HashMap::new(),
    )
    .expect("seed authenticated hub row");

    let edge = Arc::new(
        Database::open(root.path().join("direction-edge.db")).expect("open direction edge"),
    );
    declare(&edge);
    edge.persist_sync_pull_watermark(&TenantId::from(TENANT), declared_through)
        .expect("skip already-declared schema in focused pull");

    let identity = Arc::new(
        FabricIdentity::load_or_generate(&root.path().join("direction-hub.fabric-identity.key"))
            .expect("hub identity"),
    );
    let hub_node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub,
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&status_subject(TENANT))
        .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = Arc::new(
        SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&edge_node_id),
            TenantId::from(TENANT),
            edge_identity,
        ),
    );

    let pause = client.pause_after_pull_response_for_test();
    let mut pull = tokio::spawn({
        let client = client.clone();
        async move { client.pull_default().await }
    });
    tokio::select! {
        _ = pause.wait_until_reached() => {}
        result = &mut pull => panic!("authenticated pull failed before the decoded-response fence: {result:?}"),
    }
    edge.execute("ALTER TABLE notes SET SYNC OFF", &HashMap::new())
        .expect("operator turns off pull before the decoded page applies");
    pause.release();

    let result = pull
        .await
        .expect("direction pull task")
        .expect("SYNC OFF filters the decoded page without failing the pull");
    assert_eq!(result.applied_rows, 0, "the decoded row must not apply");
    assert!(
        edge.execute("SELECT * FROM notes WHERE id = 42", &HashMap::new())
            .expect("inspect edge after SYNC OFF")
            .rows
            .is_empty(),
        "a row decoded before SYNC OFF must still obey the declaration current at apply"
    );
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("hub server task");
}

#[test]
fn in_memory_destination_move_selects_held_sync_arrivals_for_one_epoch() {
    let db = Arc::new(Database::open_memory());
    declare(&db);
    db.execute(
        "INSERT INTO notes (id, body) VALUES (1, 'inherited')",
        &HashMap::new(),
    )
    .expect("install one row");
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    db.mark_row_arrived_by_sync_for_test("notes", &key, Lsn(40))
        .expect("mark row as inherited");

    let old_broker = InProcessBroker::new();
    let _old_hub_transport = old_broker.server_as("old-hub");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        old_broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity.clone(),
    );
    let ordinary = client
        .pending_push_change_count()
        .expect("ordinary pending count");
    assert_eq!(
        ordinary, 1,
        "only the locally declared table is pending; the inherited row is self-echo suppressed"
    );

    client
        .change_destination("new-hub")
        .expect("move in-memory destination");
    let new_broker = InProcessBroker::new();
    let _new_hub_transport = new_broker.server_as("new-hub");
    let new_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        new_broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    );
    assert!(
        db.destination_reupload_frontier(&TenantId::from(TENANT), "new-hub")
            .expect("read memory epoch")
            .is_some(),
        "the shared in-memory database retains the move epoch"
    );
    assert_eq!(
        new_client
            .pending_push_change_count()
            .expect("moved pending count"),
        ordinary + 1,
        "the move selects the inherited row without changing ordinary declaration work"
    );

    let epoch_id = db
        .destination_reupload_epoch_identity_for_test(&TenantId::from(TENANT), "new-hub")
        .expect("read memory epoch identity")
        .expect("memory epoch is armed");
    db.complete_destination_reupload(&TenantId::from(TENANT), "new-hub", epoch_id)
        .expect("complete memory epoch");
    assert_eq!(
        new_client
            .pending_push_change_count()
            .expect("completed pending count"),
        ordinary,
        "retiring the epoch restores ordinary self-echo suppression"
    );
}

#[test]
fn old_same_hub_completion_cannot_retire_a_later_destination_epoch() {
    let db = Arc::new(Database::open_memory());
    declare(&db);
    let old_broker = InProcessBroker::new();
    let _old_hub_transport = old_broker.server_as("hub-h");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        old_broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    );

    client
        .change_destination("hub-h")
        .expect("arm first hub-H epoch");
    let first_epoch = db
        .destination_reupload_epoch_identity_for_test(&TenantId::from(TENANT), "hub-h")
        .expect("read first epoch")
        .expect("first epoch is armed");

    client
        .change_destination("hub-x")
        .expect("move away from hub H");
    client.change_destination("hub-h").expect("return to hub H");
    let second_epoch = db
        .destination_reupload_epoch_identity_for_test(&TenantId::from(TENANT), "hub-h")
        .expect("read second epoch")
        .expect("second epoch is armed");
    assert_ne!(
        first_epoch, second_epoch,
        "a later H epoch has a fresh opaque identity even without a data-LSN change"
    );

    db.complete_destination_reupload(&TenantId::from(TENANT), "hub-h", first_epoch)
        .expect("stale completion is harmless");
    assert_eq!(
        db.destination_reupload_epoch_identity_for_test(&TenantId::from(TENANT), "hub-h")
            .expect("read epoch after stale completion"),
        Some(second_epoch),
        "the old H completion cannot retire the later H rebuild"
    );
}

#[test]
fn destination_change_clears_old_hub_lost_ack_state_in_memory_and_on_disk() {
    let root = tempfile::tempdir().expect("tempdir");
    let db_path = root.path().join("edge.db");
    let db = Arc::new(Database::open(&db_path).expect("open edge"));
    declare(&db);
    db.register_retention_sync_peer("old-hub")
        .expect("bind old hub");
    db.persist_sync_pending_push_confirmation(&TenantId::from(TENANT), Some(Lsn(77)))
        .expect("persist old-hub lost acknowledgement");

    let broker = InProcessBroker::new();
    let _new_hub_transport = broker.server_as("new-hub");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    );
    assert_eq!(
        client.pending_push_confirmation_for_test(),
        Lsn(77),
        "fixture: the client loaded the old hub's durable lost-ack marker"
    );

    client
        .change_destination("new-hub")
        .expect("change destination");
    assert_eq!(
        client.pending_push_confirmation_for_test(),
        Lsn(0),
        "the same client forgets the old hub's reconciliation target"
    );
    assert_eq!(
        db.persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read cleared durable marker"),
        None,
        "the old target is removed durably"
    );
    drop(client);
    drop(db);

    let reopened = Database::open(&db_path).expect("reopen edge");
    assert_eq!(
        reopened
            .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
            .expect("read marker after restart"),
        None,
        "restart cannot restore the former hub's lost-ack obligation"
    );
}

struct PauseAfterPushReply {
    inner: Arc<dyn ClientTransport>,
    status_frontier: Lsn,
    accepted_at: Lsn,
    conflicts: Vec<WireConflict>,
    response_ready: Arc<tokio::sync::Notify>,
    release_response: Arc<tokio::sync::Notify>,
}

impl ClientTransport for PauseAfterPushReply {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.reconnect()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            if subject == status_subject(TENANT) {
                return encode(
                    MessageType::StatusResponse,
                    &SyncStatusResponse {
                        applied_push_watermark: Some(self.status_frontier),
                        server_current_lsn: Some(self.status_frontier),
                    },
                )
                .map_err(|error| TransportError::Other(error.to_string()));
            }
            if subject == push_subject(TENANT) {
                let response = encode(
                    MessageType::PushResponse,
                    &PushResponse {
                        result: Some(WireApplyResult {
                            applied_rows: usize::from(self.conflicts.len() < 2),
                            skipped_rows: self.conflicts.len(),
                            conflicts: self.conflicts.clone(),
                            new_lsn: self.accepted_at,
                        }),
                        error: None,
                        application_error: None,
                    },
                )
                .map_err(|error| TransportError::Other(error.to_string()))?;
                self.response_ready.notify_one();
                self.release_response.notified().await;
                return Ok(response);
            }
            Err(TransportError::NoResponder)
        })
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.request(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_hub_reply_finishing_after_move_cannot_restore_its_frontier() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = InProcessBroker::new();
    let _old_hub_transport = broker.server_as("old-hub");

    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let identity =
        Arc::new(FabricIdentity::load_or_generate(&identity_path).expect("edge identity"));
    let edge = Arc::new(Database::open(&edge_path).expect("open edge"));
    declare(&edge);
    edge.execute(
        "INSERT INTO notes (id, body) VALUES (2, 'delete me')",
        &HashMap::new(),
    )
    .expect("insert delete fixture");
    let declaration_lsn = edge.current_lsn();
    let tx = edge.begin().expect("begin same-position source batch");
    edge.execute_in_tx(
        tx,
        "INSERT INTO notes (id, body) VALUES (1, 'held')",
        &HashMap::new(),
    )
    .expect("insert accepted sibling");
    edge.execute_in_tx(tx, "DELETE FROM notes WHERE id = 2", &HashMap::new())
        .expect("delete refused sibling");
    edge.commit(tx).expect("commit same-position source batch");
    let batch = edge.changes_since(declaration_lsn);
    let row_lsn = batch
        .rows
        .iter()
        .find(|row| row.table == "notes" && !row.deleted)
        .map(|row| row.lsn)
        .expect("held row has a local position");
    assert!(
        row_lsn > declaration_lsn,
        "the focused race fixture needs a source frontier between schema and row"
    );
    assert_eq!(
        batch.rows.len(),
        2,
        "the paused reply must address one accepted write and one refused delete"
    );
    assert!(
        batch.rows.iter().all(|row| row.lsn == row_lsn),
        "both semantic reply effects must share the one paused source group"
    );
    edge.persist_sync_push_watermark(&TenantId::from(TENANT), declaration_lsn)
        .expect("skip already-declared schema in this focused row race");
    let live_key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let delete_key = NaturalKey::single("id".to_string(), Value::Int64(2));
    assert!(
        edge.durable_delete_is_pending_for_test("notes", &delete_key)
            .expect("inspect pending delete fixture"),
        "fixture: the delete obligation is pending before the old reply"
    );

    let response_ready = Arc::new(tokio::sync::Notify::new());
    let release_response = Arc::new(tokio::sync::Notify::new());
    let edge_node_id = identity.node_id();
    let transport = Arc::new(PauseAfterPushReply {
        inner: broker.client_as(&edge_node_id),
        status_frontier: declaration_lsn,
        accepted_at: row_lsn,
        conflicts: vec![WireConflict {
            natural_key: delete_key.clone().into(),
            resolution: "InsertIfNotExists".to_string(),
            reason: Some("keep_first".to_string()),
            table: Some("notes".to_string()),
            mutation_kind: Some("delete".to_string()),
            winning_author_node_id: Some("old-hub-winner".to_string()),
            hub_acceptance_position: Some(row_lsn),
        }],
        response_ready: response_ready.clone(),
        release_response: release_response.clone(),
    });
    let scoped_edge = Arc::new(edge.scoped_with_constraints(None, None, None));
    let client = Arc::new(
        SyncClient::with_authenticated_transport_and_identity_for_test(
            scoped_edge,
            transport,
            TenantId::from(TENANT),
            identity.clone(),
        ),
    );
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("inspect focused row batch"),
        2,
        "the paused push must contain only the same-position row mutations, not schema"
    );
    let post_response_pause = client.pause_after_push_response_for_test();
    let old_push = tokio::spawn({
        let client = client.clone();
        async move { client.push().await }
    });
    response_ready.notified().await;
    release_response.notify_one();
    post_response_pause.wait_until_reached().await;
    let mover = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        identity,
    );
    mover
        .change_destination("new-hub")
        .expect("move through the original handle after the scoped reply decoded");
    post_response_pause.release();
    let result = old_push.await.expect("old push task");
    let error = result.expect_err("old hub result must not update moved state");
    assert!(
        error.to_string().contains("authoritative destination"),
        "the completed old response is rejected at the hub-bound progress write: {error}"
    );
    assert_eq!(
        edge.persisted_sync_watermarks(&TenantId::from(TENANT))
            .expect("read post-race watermarks")
            .0,
        Lsn(0),
        "the old hub's high frontier cannot survive the move"
    );
    assert_eq!(
        client.push_watermark(),
        declaration_lsn,
        "the decoded old reply cannot advance the scoped client's in-memory push frontier"
    );
    assert_eq!(
        edge.sync_watermark(),
        Lsn(0),
        "the decoded old reply cannot reopen the engine pruning frontier"
    );
    assert!(
        edge.destination_reupload_frontier(&TenantId::from(TENANT), "new-hub")
            .expect("read new-hub epoch")
            .is_some(),
        "the new-hub rebuild remains armed"
    );
    assert!(
        !edge
            .has_terminal_refusal_markers_for_test(&TenantId::from(TENANT), "old-hub")
            .expect("inspect stale refusal markers"),
        "the late old-hub refusal cannot leave reconciliation state"
    );
    assert!(
        edge.durable_delete_is_pending_for_test("notes", &delete_key)
            .expect("inspect delete obligation after stale reply"),
        "the late old-hub refusal cannot retire the delete obligation"
    );
    let (_, arrivals) = edge.changes_since_with_arrivals(Lsn(0));
    let live = edge
        .changes_since(Lsn(0))
        .rows
        .into_iter()
        .find(|row| !row.deleted && row.natural_key == live_key)
        .expect("accepted sibling remains live");
    assert_eq!(
        arrivals.get(&live.lsn),
        Some(&None),
        "the late old-hub acceptance cannot stamp accepted-order provenance"
    );
}

#[test]
fn post_move_schema_is_refused_until_received_origin_is_available() {
    let root = tempfile::tempdir().expect("tempdir");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open edge"));
    declare(&edge);
    let broker = InProcessBroker::new();
    let _hub_transport = broker.server_as("new-hub");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    );
    client
        .change_destination("new-hub")
        .expect("move destination");
    edge.execute("CREATE INDEX notes_body ON notes(body)", &HashMap::new())
        .expect("publish schema after move");

    let error = client
        .pending_push_change_count()
        .expect_err("post-move schema origin is deliberately fail-closed");
    assert!(
        error
            .to_string()
            .contains("schema published after the move"),
        "the temporary boundary must remain explicit: {error}"
    );
}
