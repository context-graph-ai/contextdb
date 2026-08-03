//! A refused connected unit reports every winner and leaves no partial hub state.

use contextdb_core::{TenantId, Value, VersionedRow};
use contextdb_engine::Database;
use contextdb_server::protocol::{MessageType, decode};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
    client_transport,
};
use contextdb_server::{
    FabricIdentity, SyncClient, SyncServer, TransferDirection, TransferPlane, TransferReceipt,
    peer_dial_spec,
};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "dependency-unit-refusal";
const PARENT_ID: &str = "00000000-0000-4000-8000-0000000000a0";
const FIRST_CHILD_ID: &str = "00000000-0000-4000-8000-0000000000a1";
const SECOND_CHILD_ID: &str = "00000000-0000-4000-8000-0000000000a2";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh exchange")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_tables(db: &Database) {
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare parent table");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare child table");
}

fn insert_parent(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO parents (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert parent");
}

fn update_parent(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "UPDATE parents SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("update parent");
}

fn insert_child(db: &Database, id: Uuid, parent_id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent_id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert child");
}

fn delete_child(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM children WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("delete child");
}

fn row(db: &Database, table: &str, id: Uuid) -> Option<VersionedRow> {
    db.point_lookup(table, "id", &Value::Uuid(id), db.snapshot())
        .expect("point lookup")
}

fn body(row: &VersionedRow) -> Option<&Value> {
    row.values.get("body")
}

fn expected_natural_key(id: Uuid) -> JsonValue {
    json!({
        "column": "id",
        "value": serde_json::to_value(Value::Uuid(id)).expect("serialize UUID key"),
        "rest": [],
    })
}

fn assert_edit_diagnostic(
    diagnostic: &JsonValue,
    table: &str,
    id: Uuid,
    winning_author_node_id: &str,
    hub_acceptance_position: u64,
) {
    assert_eq!(diagnostic.get("table"), Some(&json!(table)));
    assert_eq!(
        diagnostic.get("natural_key"),
        Some(&expected_natural_key(id))
    );
    assert_eq!(diagnostic.get("mutation_kind"), Some(&json!("edit")));
    assert_eq!(
        diagnostic.get("winning_author_node_id"),
        Some(&json!(winning_author_node_id))
    );
    assert_eq!(
        diagnostic.get("hub_acceptance_position"),
        Some(&json!(hub_acceptance_position))
    );
}

fn sync_receipt_items(
    receipts: &[TransferReceipt],
    peer_node_id: &str,
    direction: TransferDirection,
) -> u64 {
    receipts
        .iter()
        .find(|receipt| {
            receipt.peer_node_id == peer_node_id
                && receipt.plane == TransferPlane::Sync
                && receipt.direction == direction
        })
        .map(|receipt| receipt.counters.items)
        .unwrap_or(0)
}

fn assert_hub_winner_unchanged(db: &Database, table: &str, id: Uuid, expected: &VersionedRow) {
    let actual = row(db, table, id).expect("hub winner remains live");
    assert_eq!(
        actual.row_id, expected.row_id,
        "hub winner row identity changes"
    );
    assert_eq!(actual.values, expected.values, "hub winner values change");
    assert_eq!(
        actual.lsn, expected.lsn,
        "hub winner acceptance position changes"
    );
}

struct Hub {
    db: Arc<Database>,
    endpoint: IrohServer,
    server: Arc<SyncServer>,
    ticket: String,
    node_id: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

/// Keeps the real authenticated-Iroh client intact while recording only the
/// sync envelope returned to this test.  The proof must distinguish a
/// dependency-complete pull response from an ordinary response without using
/// an in-process transport or a hand-built wire fixture.
struct ObservingTransport {
    inner: Arc<dyn ClientTransport>,
    pull_response_types: Mutex<Vec<MessageType>>,
}

impl ObservingTransport {
    fn new(inner: Arc<dyn ClientTransport>) -> Self {
        Self {
            inner,
            pull_response_types: Mutex::new(Vec::new()),
        }
    }

    fn pull_response_types(&self) -> Vec<MessageType> {
        self.pull_response_types
            .lock()
            .expect("observe pull response types")
            .clone()
    }
}

impl ClientTransport for ObservingTransport {
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
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let reply = self.inner.request(subject, request_bytes, timeout).await?;
            if subject == contextdb_server::subjects::pull_subject(TENANT) {
                let envelope = decode(&reply)
                    .map_err(|err| TransportError::Other(format!("decode observed pull: {err}")))?;
                self.pull_response_types
                    .lock()
                    .expect("record observed pull response")
                    .push(envelope.message_type);
            }
            Ok(reply)
        })
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

async fn start_hub(root: &Path) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let node_id = FabricIdentity::load_or_generate(&identity_path)
        .expect("persist hub identity")
        .node_id();
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    declare_tables(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(TENANT),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        endpoint,
        server,
        ticket,
        node_id,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task)
            .await
            .expect("authenticated Iroh hub stops within the test bound");
        self.endpoint.close().await;
    }
}

#[tokio::test]
async fn refused_dependency_unit_retires_resend_and_converges_only_after_pull() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_identity =
        Arc::new(FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity"));
    let edge_node_id = edge_identity.node_id();
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    declare_tables(&edge);
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT));

    within(client.push())
        .await
        .expect("bootstrap DDL reaches the authenticated hub before the refused unit");
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("count bootstrap work"),
        0,
        "the test starts the conflicting unit with no earlier source work"
    );
    let hub_received_before_refusal = sync_receipt_items(
        &hub.server.transfer_receipts(),
        &edge_node_id,
        TransferDirection::Received,
    );
    let edge_sent_before_refusal = sync_receipt_items(
        &client.transfer_receipts(),
        &hub.node_id,
        TransferDirection::Sent,
    );

    let parent_id = Uuid::parse_str(PARENT_ID).expect("fixed parent UUID");
    let first_child_id = Uuid::parse_str(FIRST_CHILD_ID).expect("fixed first child UUID");
    let second_child_id = Uuid::parse_str(SECOND_CHILD_ID).expect("fixed second child UUID");

    insert_parent(&hub.db, parent_id, "hub-parent");
    insert_child(&hub.db, first_child_id, parent_id, "hub-first-child");
    insert_child(&hub.db, second_child_id, parent_id, "hub-second-child");
    let hub_parent = row(&hub.db, "parents", parent_id).expect("hub parent winner");
    let hub_first_child = row(&hub.db, "children", first_child_id).expect("first hub child winner");
    let hub_second_child =
        row(&hub.db, "children", second_child_id).expect("second hub child winner");
    let hub_before_refusal = hub.db.current_lsn();

    insert_parent(&edge, parent_id, "edge-parent");
    insert_child(&edge, second_child_id, parent_id, "edge-second-child");
    insert_child(&edge, first_child_id, parent_id, "edge-first-child");
    let source_final_lsn = edge.current_lsn();
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("count connected source work"),
        3,
        "the pending source unit is exactly its parent and two dependent children"
    );

    let refusal = within(client.push())
        .await
        .expect("a confirmed semantic refusal returns complete typed diagnostics, not an indeterminate transport error");
    assert_eq!(
        refusal.applied_rows, 0,
        "a refused unit cannot report a partially applied member"
    );
    assert_eq!(
        refusal.skipped_rows, 3,
        "every parent-or-child conflict is reported as a terminal refusal"
    );
    let diagnostics = serde_json::to_value(&refusal.conflicts)
        .expect("serialize typed diagnostics")
        .as_array()
        .cloned()
        .expect("diagnostics serialize as an ordered array");
    assert_eq!(
        diagnostics.len(),
        3,
        "one complete diagnostic is returned for each refused dependency member"
    );
    assert_edit_diagnostic(
        &diagnostics[0],
        "parents",
        parent_id,
        &hub.node_id,
        hub_parent.lsn.0,
    );
    assert_edit_diagnostic(
        &diagnostics[1],
        "children",
        first_child_id,
        &hub.node_id,
        hub_first_child.lsn.0,
    );
    assert_edit_diagnostic(
        &diagnostics[2],
        "children",
        second_child_id,
        &hub.node_id,
        hub_second_child.lsn.0,
    );
    assert_eq!(
        hub.db
            .changes_since(hub_before_refusal)
            .rows
            .iter()
            .filter(|change| change.table == "parents" || change.table == "children")
            .count(),
        0,
        "receipt persistence may advance the hub LSN, but no refused member mutates the hub"
    );
    assert_hub_winner_unchanged(&hub.db, "parents", parent_id, &hub_parent);
    assert_hub_winner_unchanged(&hub.db, "children", first_child_id, &hub_first_child);
    assert_hub_winner_unchanged(&hub.db, "children", second_child_id, &hub_second_child);
    assert_eq!(
        sync_receipt_items(
            &hub.server.transfer_receipts(),
            &edge_node_id,
            TransferDirection::Received,
        ),
        hub_received_before_refusal + 3,
        "the hub receipt attributes all three refused rows to the persisted edge identity"
    );
    assert_eq!(
        sync_receipt_items(
            &client.transfer_receipts(),
            &hub.node_id,
            TransferDirection::Sent,
        ),
        edge_sent_before_refusal + 3,
        "the edge receipt attributes the refused unit to the authenticated hub identity"
    );
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("count terminally refused work"),
        0,
        "terminal refusal retires resend work before the edge client closes"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &edge_node_id,)
            .expect("read hub edge receipt watermark"),
        Some(source_final_lsn),
        "the hub durably confirms the entire terminally refused source unit"
    );
    assert_eq!(
        body(&row(&edge, "parents", parent_id).expect("edge parent before pull")),
        Some(&Value::Text("edge-parent".to_string())),
        "push reports adjudication but does not implicitly pull the hub winner"
    );

    client.shutdown().await;
    drop(client);
    drop(edge);
    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT));
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("count reopened source work"),
        0,
        "terminal refusal durably retires every member rather than leaving resend work"
    );
    assert!(
        !client
            .has_pending_push_changes()
            .expect("inspect reopened source work"),
        "the reopened edge has no hidden pending resend state"
    );

    within(client.pull_default())
        .await
        .expect("ordinary pull converges the terminally refused edge");
    assert_eq!(
        body(&row(&edge, "parents", parent_id).expect("converged parent")),
        Some(&Value::Text("hub-parent".to_string())),
        "ordinary pull adopts the hub parent after refusal"
    );
    assert_eq!(
        body(&row(&edge, "children", first_child_id).expect("converged first child")),
        Some(&Value::Text("hub-first-child".to_string())),
        "ordinary pull adopts the first hub child after refusal"
    );
    assert_eq!(
        body(&row(&edge, "children", second_child_id).expect("converged second child")),
        Some(&Value::Text("hub-second-child".to_string())),
        "ordinary pull adopts the second hub child after refusal"
    );
    assert_eq!(
        body(
            &edge
                .point_lookup(
                    "parents",
                    "id",
                    &Value::Uuid(parent_id),
                    edge.snapshot_at(source_final_lsn),
                )
                .expect("read retained rejected parent history")
                .expect("rejected parent remains readable for repair"),
        ),
        Some(&Value::Text("edge-parent".to_string())),
        "retained history keeps the rejected parent version available for repair or re-keying"
    );
    assert_eq!(
        body(
            &edge
                .point_lookup(
                    "children",
                    "id",
                    &Value::Uuid(first_child_id),
                    edge.snapshot_at(source_final_lsn),
                )
                .expect("read retained rejected first-child history")
                .expect("rejected first child remains readable for repair"),
        ),
        Some(&Value::Text("edge-first-child".to_string())),
        "retained history keeps the first rejected child version available for repair or re-keying"
    );
    assert_eq!(
        body(
            &edge
                .point_lookup(
                    "children",
                    "id",
                    &Value::Uuid(second_child_id),
                    edge.snapshot_at(source_final_lsn),
                )
                .expect("read retained rejected second-child history")
                .expect("rejected second child remains readable for repair"),
        ),
        Some(&Value::Text("edge-second-child".to_string())),
        "retained history keeps the second rejected child version available for repair or re-keying"
    );
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("count converged terminal-refusal work"),
        0,
        "a terminal-refusal reconciliation pull must not manufacture new outbound work"
    );

    let hub_receipts_before_repeat = hub.server.transfer_receipts();
    let repeated = within(client.push())
        .await
        .expect("a terminally refused unit is not sent again");
    assert_eq!(
        (
            repeated.applied_rows,
            repeated.skipped_rows,
            repeated.conflicts.len()
        ),
        (0, 0, 0),
        "repeat push has neither transmitted work nor repeated diagnostics"
    );
    assert_eq!(
        hub.server.transfer_receipts(),
        hub_receipts_before_repeat,
        "repeat push adds no received sync receipt at the hub"
    );
    assert!(
        client.transfer_receipts().iter().all(|receipt| {
            receipt.plane != TransferPlane::Sync || receipt.direction != TransferDirection::Sent
        }),
        "the reopened client sends no sync payload after terminal refusal"
    );

    client.shutdown().await;
    hub.stop().await;
}

#[tokio::test]
async fn authenticated_iroh_rejects_stale_connected_replay_as_a_whole_before_receipt_progress() {
    let root = tempfile::tempdir().expect("tempdir");
    let stale_root = tempfile::tempdir().expect("stale hub tempdir");
    let authoritative = start_hub(root.path()).await;
    let stale = start_hub(stale_root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_identity =
        Arc::new(FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity"));
    let edge_node_id = edge_identity.node_id();
    let authoritative_dial = peer_dial_spec(&authoritative.ticket, &identity_path);
    let stale_dial = peer_dial_spec(&stale.ticket, &identity_path);
    let edge = Arc::new(Database::open(&edge_path).expect("open receiving edge database"));
    declare_tables(&edge);
    let primary_client = SyncClient::new(edge.clone(), &authoritative_dial, TenantId::from(TENANT));

    within(primary_client.push())
        .await
        .expect("bootstrap declarations reach the authoritative hub");
    let parent_id = Uuid::parse_str(PARENT_ID).expect("fixed parent UUID");
    let child_id = Uuid::parse_str(FIRST_CHILD_ID).expect("fixed child UUID");
    insert_parent(&edge, parent_id, "authoritative-parent");
    insert_child(&edge, child_id, parent_id, "authoritative-child");
    within(primary_client.push())
        .await
        .expect("authoritative parent and child reach the first hub");
    assert!(
        row(&authoritative.db, "parents", parent_id).is_some()
            && row(&authoritative.db, "children", child_id).is_some(),
        "fixture: the authoritative hub starts with the connected parent and child"
    );

    // This deliberately relies on the established destination-change
    // re-upload contract: moving to the second hub resets the confirmation
    // frontier so the edge seeds it with the pre-delete connected state.
    primary_client
        .change_destination(&stale.node_id)
        .expect("move the edge destination to seed the stale authenticated hub");
    primary_client.shutdown().await;
    drop(primary_client);
    let stale_seed = SyncClient::new(edge.clone(), &stale_dial, TenantId::from(TENANT));
    within(stale_seed.push())
        .await
        .expect("stale hub receives the pre-delete connected copy");
    assert!(
        row(&stale.db, "parents", parent_id).is_some()
            && row(&stale.db, "children", child_id).is_some(),
        "fixture: the second real hub retains the connected pre-delete copy"
    );
    update_parent(&stale.db, parent_id, "stale-parent-must-not-land");
    assert_eq!(
        body(&row(&stale.db, "parents", parent_id).expect("stale parent mutation")),
        Some(&Value::Text("stale-parent-must-not-land".to_string())),
        "fixture: the stale connected unit carries a parent value the receiver must never apply"
    );
    stale_seed
        .change_destination(&authoritative.node_id)
        .expect("restore the authoritative hub as the edge destination");
    stale_seed.shutdown().await;
    drop(stale_seed);

    let authoritative_client =
        SyncClient::new(edge.clone(), &authoritative_dial, TenantId::from(TENANT));
    delete_child(&edge, child_id);
    let accepted_delete = within(authoritative_client.push())
        .await
        .expect("authoritative hub accepts the edge child delete");
    assert!(
        accepted_delete.conflicts.is_empty(),
        "fixture: the authoritative delete is accepted rather than merely adjudicated: {:?}",
        accepted_delete.conflicts
    );
    assert!(
        row(&authoritative.db, "children", child_id).is_none(),
        "fixture: the authoritative hub records the accepted child deletion"
    );
    let accepted_child_state_before =
        edge.durable_deletion_state_for_test("children", &Value::Uuid(child_id));
    assert_eq!(
        accepted_child_state_before
            .accepted_delete_marker
            .as_deref(),
        Some("accepted"),
        "fixture: the receiving edge durably records the authoritative child acceptance"
    );
    authoritative_client
        .change_destination(&stale.node_id)
        .expect("point the receiving edge at the stale authenticated hub");
    authoritative_client.shutdown().await;
    drop(authoritative_client);

    let observing_transport = Arc::new(ObservingTransport::new(client_transport(&stale_dial)));
    let stale_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        observing_transport.clone(),
        TenantId::from(TENANT),
        edge_identity,
    );
    let receiver_parent_before = row(&edge, "parents", parent_id).expect("receiver parent");
    assert_eq!(
        body(&receiver_parent_before),
        Some(&Value::Text("authoritative-parent".to_string())),
        "fixture: the receiver still holds the authoritative parent, not the stale mutation"
    );
    assert!(
        row(&edge, "children", child_id).is_none(),
        "receiver keeps the locally accepted child deletion before stale pull"
    );
    let receiver_lsn_before = edge.current_lsn();
    let pull_cursor_before = stale_client.pull_watermark();
    let push_watermark_before = stale_client.push_watermark();
    let persisted_watermarks_before = edge
        .persisted_sync_watermarks(&TenantId::from(TENANT))
        .expect("read receiving edge watermarks before stale pull");
    let persisted_pull_cursor_before = edge
        .persisted_sync_pull_cursor(&TenantId::from(TENANT))
        .expect("read receiving edge pull cursor before stale pull");
    let received_before = sync_receipt_items(
        &stale_client.transfer_receipts(),
        &stale.node_id,
        TransferDirection::Received,
    );
    let stale_sent_before = sync_receipt_items(
        &stale.server.transfer_receipts(),
        &edge_node_id,
        TransferDirection::Sent,
    );

    let error = within(stale_client.pull_default())
        .await
        .expect_err("terminated child lineage refuses the whole stale connected pull");
    assert!(
        error
            .to_string()
            .contains("replays a lineage terminated by an accepted delete"),
        "the stale pull must fail at the accepted-lineage boundary: {error}"
    );
    assert_eq!(
        observing_transport.pull_response_types(),
        vec![MessageType::DependencyCompletePullResponse],
        "the stale hub sends the parent and child as one dependency-complete response"
    );
    assert_eq!(
        sync_receipt_items(
            &stale.server.transfer_receipts(),
            &edge_node_id,
            TransferDirection::Sent,
        ),
        stale_sent_before + 2,
        "the stale hub served both members of the connected replay"
    );
    assert_hub_winner_unchanged(&edge, "parents", parent_id, &receiver_parent_before);
    assert_eq!(
        body(&row(&edge, "parents", parent_id).expect("receiver parent after stale pull")),
        Some(&Value::Text("authoritative-parent".to_string())),
        "the stale parent mutation cannot land when its connected child lineage is terminated"
    );
    assert!(
        row(&edge, "children", child_id).is_none(),
        "the stale child cannot resurrect after its accepted deletion"
    );
    assert_eq!(
        edge.durable_deletion_state_for_test("children", &Value::Uuid(child_id)),
        accepted_child_state_before,
        "the refused replay cannot weaken or rewrite the durable accepted-delete boundary"
    );
    assert_eq!(
        edge.current_lsn(),
        receiver_lsn_before,
        "the terminated dependency unit cannot commit its parent before failing"
    );
    assert_eq!(
        stale_client.pull_watermark(),
        pull_cursor_before,
        "the failed dependency-complete pull cannot advance its cursor"
    );
    assert_eq!(
        stale_client.push_watermark(),
        push_watermark_before,
        "a failed pull cannot advance the edge push watermark"
    );
    assert_eq!(
        edge.persisted_sync_watermarks(&TenantId::from(TENANT))
            .expect("read receiving edge watermarks after stale pull"),
        persisted_watermarks_before,
        "the failed pull cannot persist either sync watermark"
    );
    assert_eq!(
        edge.persisted_sync_pull_cursor(&TenantId::from(TENANT))
            .expect("read receiving edge pull cursor after stale pull"),
        persisted_pull_cursor_before,
        "the failed pull cannot persist a new source-bound cursor"
    );
    assert_eq!(
        sync_receipt_items(
            &stale_client.transfer_receipts(),
            &stale.node_id,
            TransferDirection::Received,
        ),
        received_before,
        "the receiving edge records no received receipt before rejecting the whole unit"
    );

    stale_client.shutdown().await;
    stale.stop().await;
    authoritative.stop().await;
}
