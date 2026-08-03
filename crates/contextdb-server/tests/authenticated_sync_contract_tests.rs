//! Real Iroh journeys for durable declarations and authenticated provenance.

use contextdb_core::{Incarnation, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_server::protocol::{
    MessageType, PushRequest, PushResponse, WireChangeSet, decode, encode,
};
use contextdb_server::subjects::push_subject;
use contextdb_server::transfer_receipts::{TransferDirection, TransferPlane};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
    client_transport,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded real-Iroh operation")
}

// Every journey in this binary creates a real Iroh endpoint.  A restarted
// hub must reclaim its sticky UDP ports, which is incompatible with a sibling
// journey concurrently choosing port zero.  One async permit makes that OS
// resource ownership explicit without weakening any journey or adding time
// based coordination.
static REAL_IROH_JOURNEY_PERMIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn bind_spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn table(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("create declared keep-first table");
}

fn table_if_absent(db: &Database) {
    if db.table_meta("notes").is_none() {
        table(db);
    }
}

fn insert(db: &Database, id: Uuid, body: &str) {
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("body".to_string(), Value::Text(body.to_string())),
    ]);
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &values)
        .expect("insert note");
}

fn update(db: &Database, id: Uuid, body: &str) {
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("body".to_string(), Value::Text(body.to_string())),
    ]);
    db.execute("UPDATE notes SET body = $body WHERE id = $id", &values)
        .expect("update note");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    let values = HashMap::from([("id".to_string(), Value::Uuid(id))]);
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &values)
        .expect("select note");
    result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    })
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    server: Arc<SyncServer>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn hub(root: &Path, tenant: &str) -> Hub {
    let db_path = root.join("hub.db");
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind hub");
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(db_path).expect("open hub database"));
    table_if_absent(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = stop.clone();
        let server = server.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        node_id,
        server,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task)
            .await
            .expect("hub server task stops within the test bound");
    }
}

async fn restart_hub(old_hub: Hub, root: &Path, tenant: &str) -> Hub {
    let expected_node_id = old_hub.node_id.clone();
    old_hub.stop().await;
    let restarted = hub(root, tenant).await;
    assert_eq!(
        restarted.node_id, expected_node_id,
        "reopening the hub database retains its adjacent persisted identity",
    );
    restarted
}

fn edge(root: &Path, name: &str, ticket: &str, tenant: &str) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open(root.join(format!("{name}.db"))).expect("open edge"));
    table_if_absent(&db);
    // The documented constructor must derive the adjacent persisted identity.
    let client = SyncClient::new(db.clone(), ticket, TenantId::from(tenant));
    (db, client)
}

fn received_sync_receipts(hub: &Hub) -> Vec<contextdb_server::TransferReceipt> {
    hub.server
        .transfer_receipts()
        .into_iter()
        .filter(|receipt| {
            receipt.plane == TransferPlane::Sync && receipt.direction == TransferDirection::Received
        })
        .collect()
}

#[derive(Clone, Copy)]
enum LineageFault {
    Missing,
    Forged,
}

impl LineageFault {
    fn name(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Forged => "forged",
        }
    }
}

/// Preserves the actual Iroh peer identities and connection while corrupting
/// one row lineage at the serialized sync boundary.
struct TamperPushLineageTransport {
    inner: Arc<dyn ClientTransport>,
    subject: String,
    fault: LineageFault,
    mutated: Arc<AtomicBool>,
}

impl TamperPushLineageTransport {
    fn rewrite(&self, subject: &str, bytes: Vec<u8>) -> TransportResult<Vec<u8>> {
        if subject != self.subject {
            return Ok(bytes);
        }
        let envelope = decode(&bytes).map_err(|err| TransportError::Other(err.to_string()))?;
        if envelope.message_type != MessageType::PushRequest {
            return Ok(bytes);
        }
        let mut request: PushRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|err| TransportError::Other(err.to_string()))?;
        let Some(row) = request.changeset.rows.first_mut() else {
            return Ok(bytes);
        };
        match self.fault {
            LineageFault::Missing => row.lineage = None,
            LineageFault::Forged => {
                let lineage = row
                    .lineage
                    .as_mut()
                    .expect("authenticated source stamps each pushed row with lineage");
                let first = lineage
                    .attestation
                    .first_mut()
                    .expect("authenticated source signs each pushed row lineage");
                *first ^= 0x80;
            }
        }
        self.mutated.store(true, Ordering::SeqCst);
        encode(MessageType::PushRequest, &request)
            .map_err(|err| TransportError::Other(err.to_string()))
    }
}

impl ClientTransport for TamperPushLineageTransport {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.reconnect()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let request_bytes = match self.rewrite(subject, request_bytes) {
            Ok(bytes) => bytes,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let request_bytes = match self.rewrite(subject, request_bytes) {
            Ok(bytes) => bytes,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
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

async fn assert_authenticated_iroh_lineage_fault_is_refused(fault: LineageFault) {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = format!("lineage-{}", fault.name());
    let hub = hub(root.path(), &tenant).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let identity =
        Arc::new(FabricIdentity::load_or_generate(&identity_path).expect("persist edge identity"));
    let edge_node_id = identity.node_id();
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    table(&edge);
    let bootstrap = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(tenant.as_str()));
    within(bootstrap.push())
        .await
        .expect("bootstrap declarations reach the authenticated hub");
    bootstrap.shutdown().await;

    let id = Uuid::new_v4();
    insert(&edge, id, "must not survive lineage tampering");
    let incarnation = edge
        .sync_incarnation(&TenantId::from(tenant.as_str()))
        .expect("read durable edge incarnation");
    let hub_receipts_before = received_sync_receipts(&hub);
    let hub_watermark_before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(tenant.as_str()),
            &edge_node_id,
            incarnation,
        )
        .expect("read hub edge watermark before tampering");
    let mutated = Arc::new(AtomicBool::new(false));
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        Arc::new(TamperPushLineageTransport {
            inner: client_transport(&dial_spec),
            subject: push_subject(&tenant),
            fault,
            mutated: mutated.clone(),
        }),
        TenantId::from(tenant.as_str()),
        identity,
    );
    let source_push_before = client.push_watermark();
    let source_progress_before = edge
        .persisted_sync_watermarks(&TenantId::from(tenant.as_str()))
        .expect("read edge progress before malformed push");
    let source_receipts_before = client.transfer_receipts();
    let source_pending_before = client
        .pending_push_change_count()
        .expect("count pending malformed source row");

    let refusal = within(client.push())
        .await
        .expect_err("the hub refuses a malformed authenticated row lineage");
    assert!(
        refusal.to_string().contains("lineage"),
        "the refusal identifies the malformed immutable provenance: {refusal}"
    );
    assert!(
        mutated.load(Ordering::SeqCst),
        "the real Iroh transport carried a row-bearing push altered by this test"
    );
    assert_eq!(
        body(&hub.db, id),
        None,
        "the hub cannot expose a row whose v6 lineage failed validation"
    );
    assert_eq!(
        received_sync_receipts(&hub),
        hub_receipts_before,
        "lineage refusal records no received-sync receipt"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(tenant.as_str()),
                &edge_node_id,
                incarnation,
            )
            .expect("read hub edge watermark after tampering"),
        hub_watermark_before,
        "lineage refusal cannot advance the authenticated edge receipt"
    );
    assert_eq!(
        client.push_watermark(),
        source_push_before,
        "lineage refusal cannot retire the source push watermark"
    );
    assert_eq!(
        edge.persisted_sync_watermarks(&TenantId::from(tenant.as_str()))
            .expect("read edge progress after malformed push"),
        source_progress_before,
        "lineage refusal cannot persist source sync progress"
    );
    assert_eq!(
        client.transfer_receipts(),
        source_receipts_before,
        "lineage refusal cannot record a completed sent receipt"
    );
    assert_eq!(
        client
            .pending_push_change_count()
            .expect("count pending malformed source row after refusal"),
        source_pending_before,
        "the malformed source row remains pending for operator repair"
    );

    client.shutdown().await;
    hub.stop().await;
}

#[tokio::test]
async fn authenticated_iroh_refuses_missing_or_forged_v6_lineage_before_effects() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    assert_authenticated_iroh_lineage_fault_is_refused(LineageFault::Missing).await;
    assert_authenticated_iroh_lineage_fault_is_refused(LineageFault::Forged).await;
}

fn exact_conflict(rendered: &serde_json::Value, id: Uuid, author: &str) -> serde_json::Value {
    let conflicts = rendered["conflicts"]
        .as_array()
        .expect("typed storage conflicts");
    assert_eq!(conflicts.len(), 1, "one refused row has one typed conflict");
    let conflict = &conflicts[0];
    assert_eq!(conflict["table"], "notes", "diagnostic names table");
    assert_eq!(
        conflict["natural_key"],
        serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(id)))
            .expect("serialize natural key"),
        "diagnostic keeps the exact typed natural-key shape",
    );
    assert_eq!(conflict["mutation_kind"], "edit", "diagnostic names kind");
    assert_eq!(
        conflict["winning_author_node_id"], author,
        "diagnostic names the authenticated author",
    );
    let position = conflict["hub_acceptance_position"]
        .as_u64()
        .expect("diagnostic carries a numeric hub acceptance position");
    assert!(position > 0, "hub acceptance position starts after zero");
    conflict.clone()
}

#[tokio::test]
async fn declared_policy_survives_restart_and_governs_an_authenticated_exchange() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "declared-policy";
    let hub = hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let (edge_a, client_a) = edge(root.path(), "edge-a", &hub.ticket, tenant);
    let (edge_b, client_b) = edge(root.path(), "edge-b", &hub.ticket, tenant);
    // Both writes exist before either peer contacts the hub; order is the sole
    // arbitrator input and the test seam deliberately defaults to EdgeWins.
    insert(&edge_a, id, "hub-first");
    insert(&edge_b, id, "losing-local");
    within(client_a.push())
        .await
        .expect("first controlled push");
    // Rebinding the restarted hub's remembered port requires both dialing
    // endpoints to finish their asynchronous transport shutdown first.
    client_a.shutdown().await;
    client_b.shutdown().await;
    let hub = restart_hub(hub, root.path(), tenant).await;
    let client_a = SyncClient::new(edge_a.clone(), &hub.ticket, TenantId::from(tenant));
    let client_b = SyncClient::new(edge_b.clone(), &hub.ticket, TenantId::from(tenant));
    let refused = within(client_b.push()).await.expect("second push replies");
    within(client_a.pull_default())
        .await
        .expect("first edge pull");
    within(client_b.pull_default())
        .await
        .expect("second edge pull");
    assert_eq!(body(&hub.db, id).as_deref(), Some("hub-first"));
    assert_eq!(body(&edge_a, id).as_deref(), Some("hub-first"));
    assert_eq!(body(&edge_b, id).as_deref(), Some("hub-first"));
    assert_eq!(
        refused.skipped_rows, 1,
        "KEEP FIRST refuses the second write"
    );
    hub.stop().await;
}

#[tokio::test]
async fn keep_first_refusal_names_authenticated_winner_and_hub_position() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "winner-diagnostic";
    let hub = hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let (winner_db, winner) = edge(root.path(), "winner", &hub.ticket, tenant);
    let (loser_db, loser) = edge(root.path(), "loser", &hub.ticket, tenant);
    insert(&winner_db, id, "winner");
    insert(&loser_db, id, "loser");
    within(winner.push())
        .await
        .expect("controlled winning push");
    let winner_id =
        FabricIdentity::load_or_generate(&root.path().join("winner.db.fabric-identity.key"))
            .expect("winner identity")
            .node_id();
    assert_eq!(
        received_sync_receipts(&hub)
            .into_iter()
            .map(|receipt| receipt.peer_node_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([winner_id.clone()]),
        "Iroh authenticated the edge identity that the diagnostic must name",
    );
    let first = within(loser.push()).await.expect("first losing push");
    let first_rendered = serde_json::to_value(&first).expect("serialize first refusal");
    let first_conflict = exact_conflict(&first_rendered, id, &winner_id);
    let first_position = first_conflict["hub_acceptance_position"].clone();
    // The restarted hub must reclaim its remembered UDP ports.  Close both
    // dialing endpoints before stopping the hub, rather than relying on
    // drop timing while sibling real-Iroh journeys are active.
    winner.shutdown().await;
    loser.shutdown().await;
    let hub = restart_hub(hub, root.path(), tenant).await;
    let winner = SyncClient::new(winner_db.clone(), &hub.ticket, TenantId::from(tenant));
    let loser = SyncClient::new(loser_db.clone(), &hub.ticket, TenantId::from(tenant));
    within(winner.pull_default())
        .await
        .expect("winner pull after hub restart");
    within(loser.pull_default())
        .await
        .expect("loser convergence after hub restart");
    update(&loser_db, id, "loser-again");
    let second = within(loser.push()).await.expect("second losing push");
    let second_rendered = serde_json::to_value(&second).expect("serialize second refusal");
    let second_conflict = exact_conflict(&second_rendered, id, &winner_id);
    assert_eq!(
        second_conflict["hub_acceptance_position"], first_position,
        "winner provenance and its original hub position survive restart",
    );
    assert_eq!(body(&hub.db, id).as_deref(), Some("winner"));
    let hub_key = Uuid::new_v4();
    insert(&hub.db, hub_key, "hub-local-winner");
    let (third_db, third) = edge(root.path(), "third", &hub.ticket, tenant);
    insert(&third_db, hub_key, "edge-loser");
    let hub_local = within(third.push()).await.expect("hub-local losing push");
    let hub_local_rendered = serde_json::to_value(&hub_local).expect("serialize hub-local refusal");
    let _ = exact_conflict(&hub_local_rendered, hub_key, &hub.node_id);
    assert_eq!(body(&hub.db, hub_key).as_deref(), Some("hub-local-winner"));
    hub.stop().await;
}

#[tokio::test]
async fn file_backed_identity_survives_restart_and_database_recreation() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "identity-life";
    let hub = hub(root.path(), tenant).await;
    let db_path = root.path().join("edge.db");
    let first = Arc::new(Database::open(&db_path).expect("open first life"));
    table(&first);
    let first_incarnation: Incarnation = first
        .sync_incarnation(&TenantId::from(tenant))
        .expect("first incarnation");
    insert(&first, Uuid::new_v4(), "first life");
    let first_client = SyncClient::new(first.clone(), &hub.ticket, TenantId::from(tenant));
    within(first_client.push()).await.expect("first push");
    drop(first_client);
    drop(first);
    let reopened = Arc::new(Database::open(&db_path).expect("reopen same database"));
    assert_eq!(
        reopened
            .sync_incarnation(&TenantId::from(tenant))
            .expect("reopened incarnation"),
        first_incarnation,
    );
    insert(&reopened, Uuid::new_v4(), "restarted process");
    let reopened_client = SyncClient::new(reopened.clone(), &hub.ticket, TenantId::from(tenant));
    within(reopened_client.push()).await.expect("reopened push");
    drop(reopened_client);
    drop(reopened);
    std::fs::remove_file(&db_path).expect("remove only database, retain identity");
    let recreated = Arc::new(Database::open(&db_path).expect("recreate database"));
    table(&recreated);
    assert_ne!(
        recreated
            .sync_incarnation(&TenantId::from(tenant))
            .expect("recreated incarnation"),
        first_incarnation,
    );
    insert(&recreated, Uuid::new_v4(), "recreated database");
    let recreated_client = SyncClient::new(recreated, &hub.ticket, TenantId::from(tenant));
    within(recreated_client.push())
        .await
        .expect("recreated push");
    let expected =
        FabricIdentity::load_or_generate(&root.path().join("edge.db.fabric-identity.key"))
            .expect("persisted edge identity")
            .node_id();
    let receipts = received_sync_receipts(&hub);
    assert_eq!(
        receipts.len(),
        1,
        "one peer/direction receipt accumulates three pushes"
    );
    assert_eq!(receipts[0].direction, TransferDirection::Received);
    assert_eq!(receipts[0].plane, TransferPlane::Sync);
    assert_eq!(receipts[0].peer_node_id, expected);
    assert_eq!(
        receipts[0].counters.items, 3,
        "three exact pushed rows arrived"
    );
    hub.stop().await;
}

async fn request_after_server_registers(
    client: Arc<dyn ClientTransport>,
    subject: &str,
    bytes: Vec<u8>,
) -> Vec<u8> {
    loop {
        match client
            .request(subject, bytes.clone(), Duration::from_secs(5))
            .await
        {
            Err(TransportError::NoResponder) => tokio::task::yield_now().await,
            Ok(response) => return response,
            Err(error) => panic!("identityless direct request failed before a response: {error}"),
        }
    }
}

#[tokio::test]
async fn production_sync_refuses_identityless_transport() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "identityless";
    let hub = hub(root.path(), tenant).await;
    let (positive_db, positive) = edge(root.path(), "positive", &hub.ticket, tenant);
    insert(&positive_db, Uuid::new_v4(), "file-backed positive control");
    within(positive.push())
        .await
        .expect("file-backed sync succeeds");
    let before_rows = hub
        .db
        .scan("notes", hub.db.snapshot())
        .expect("snapshot hub rows");
    let before_receipts = received_sync_receipts(&hub);
    let before_lsn = hub.db.current_lsn();
    let anonymous = Arc::new(Database::open_memory());
    table(&anonymous);
    let anonymous_id = Uuid::new_v4();
    insert(&anonymous, anonymous_id, "must not be accepted anonymously");
    let anonymous_client = SyncClient::new(anonymous, &hub.ticket, TenantId::from(tenant));
    assert!(
        within(anonymous_client.push()).await.is_err(),
        "bare in-memory dialing is refused"
    );
    assert_eq!(
        hub.db.scan("notes", hub.db.snapshot()).expect("hub rows"),
        before_rows
    );
    assert_eq!(received_sync_receipts(&hub), before_receipts);
    assert_eq!(hub.db.current_lsn(), before_lsn);
    let explicit_key = root.path().join("in-memory.fabric-identity.key");
    let explicit_spec = format!(
        "iroh:?to={}&identity={}",
        hub.ticket,
        explicit_key.display()
    );
    let explicit_db = Arc::new(Database::open_memory());
    table(&explicit_db);
    insert(&explicit_db, Uuid::new_v4(), "explicit persisted identity");
    let explicit = SyncClient::new(explicit_db.clone(), &explicit_spec, TenantId::from(tenant));
    within(explicit.push())
        .await
        .expect("explicit in-memory identity syncs");
    drop(explicit);
    insert(
        &explicit_db,
        Uuid::new_v4(),
        "explicit identity after reconstruction",
    );
    let explicit_again = SyncClient::new(explicit_db, &explicit_spec, TenantId::from(tenant));
    within(explicit_again.push())
        .await
        .expect("reconstructed explicit client syncs");
    let explicit_node = FabricIdentity::load_or_generate(&explicit_key)
        .expect("explicit persisted identity")
        .node_id();
    let explicit_receipt = received_sync_receipts(&hub)
        .into_iter()
        .find(|receipt| receipt.peer_node_id == explicit_node)
        .expect("explicit identity has a received real-Iroh receipt");
    assert_eq!(explicit_receipt.direction, TransferDirection::Received);
    assert_eq!(explicit_receipt.plane, TransferPlane::Sync);
    assert_eq!(
        explicit_receipt.counters.items, 2,
        "the reconstructed client authenticated as the same persisted identity",
    );
    let broker = InProcessBroker::new();
    let direct_db = Arc::new(Database::open_memory());
    table(&direct_db);
    let direct_server = Arc::new(SyncServer::with_authenticated_transport_for_test(
        direct_db.clone(),
        broker.server(),
        TenantId::from("identityless-direct"),
    ));
    let direct_stop = Arc::new(AtomicBool::new(false));
    let direct_task = tokio::spawn({
        let direct_server = direct_server.clone();
        let direct_stop = direct_stop.clone();
        async move { direct_server.run_until(direct_stop).await }
    });
    let direct_rows_before = direct_db
        .scan("notes", direct_db.snapshot())
        .expect("direct initial rows");
    let direct_receipts_before = direct_server.transfer_receipts();
    let direct_lsn_before = direct_db.current_lsn();
    let direct_source = Database::open_memory();
    table(&direct_source);
    let after_table = direct_source.current_lsn();
    insert(&direct_source, Uuid::new_v4(), "identityless wire mutation");
    let changeset: WireChangeSet = direct_source.changes_since(after_table).into();
    assert!(
        changeset.ddl.is_empty(),
        "direct request carries no duplicate schema DDL"
    );
    assert_eq!(
        changeset.rows.len(),
        1,
        "direct request carries one real row change"
    );
    let request = PushRequest {
        changeset,
        incarnation: Incarnation::mint(),
    };
    let response_bytes = within(request_after_server_registers(
        broker.client(),
        &push_subject("identityless-direct"),
        encode(MessageType::PushRequest, &request).expect("encode direct request"),
    ))
    .await;
    let response_envelope = decode(&response_bytes).expect("decode direct response");
    assert_eq!(response_envelope.message_type, MessageType::PushResponse);
    let response: PushResponse =
        rmp_serde::from_slice(&response_envelope.payload).expect("decode explicit refusal");
    let error = response
        .error
        .expect("identityless request is visibly refused");
    let error_lower = error.to_ascii_lowercase();
    assert!(response.result.is_none(), "refusal has no apply result");
    assert!(
        error_lower.contains("authenticated") && error_lower.contains("identity"),
        "refusal names the missing authenticated identity: {error}"
    );
    assert_eq!(
        direct_db
            .scan("notes", direct_db.snapshot())
            .expect("direct rows"),
        direct_rows_before,
    );
    assert_eq!(direct_server.transfer_receipts(), direct_receipts_before);
    assert_eq!(direct_db.current_lsn(), direct_lsn_before);
    direct_stop.store(true, Ordering::SeqCst);
    within(direct_task)
        .await
        .expect("direct server task stops within the test bound");
    hub.stop().await;
}
