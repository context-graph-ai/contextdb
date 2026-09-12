//! Mixed-version table holdback is durable progress, not process-local memory.
//!
//! These journeys restart each sender while one table is waiting, force a held
//! image across more than one pull page, and lose the acknowledgement for a
//! recovered push whose source LSN is older than healthy work already accepted
//! by the hub. Compatible tables must still move once and the held table must
//! resume automatically after upgrade.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::sync_types::{SchemaSyncCapability, SchemaSyncHoldback};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::protocol::{
    DependencyCompletePullResponse, MessageType, PullResponse, PushRequest, decode,
};
use contextdb_server::subjects::{pull_subject, push_subject};
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{
    FabricIdentity, InProcessBroker, SyncClient, SyncServer, TransferDirection, TransferPlane,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;

const PULL_TENANT: &str = "vector-schema-restart-pull";
const PUSH_TENANT: &str = "vector-schema-restart-push";
const LOST_ACK_TENANT: &str = "vector-schema-recovery-lost-ack";
const RECOVERY_ROWS: usize = 501;
const PULL_PAGE_SIZE: usize = 500;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn text_column(result: QueryResult) -> Vec<String> {
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Text(value)) => value,
            value => panic!("expected one text value, got {value:?}"),
        })
        .collect()
}

fn declare_tables(db: &Database) {
    db.execute(
        "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare ordinary table");
    db.execute(
        "CREATE TABLE partitioned_notes (id UUID PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id)) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare table that needs the newer schema capability");
}

fn insert_ordinary(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(id)),
            ("body", Value::Text(body.to_string())),
        ]),
    )
    .expect("insert ordinary row");
}

fn insert_partitioned(db: &Database, id: Uuid, scope_id: Uuid) {
    db.execute(
        "INSERT INTO partitioned_notes (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope_id)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert partitioned row");
}

fn expected_holdback(table: &str, node: &str) -> Vec<SchemaSyncHoldback> {
    vec![SchemaSyncHoldback {
        table: table.to_string(),
        capability: SchemaSyncCapability::VectorPartitioning,
        node_to_upgrade: node.to_string(),
    }]
}

fn sent_sync_rows(client: &SyncClient, peer: &str) -> u64 {
    client
        .transfer_receipts()
        .into_iter()
        .find(|receipt| {
            receipt.peer_node_id == peer
                && receipt.plane == TransferPlane::Sync
                && receipt.direction == TransferDirection::Sent
        })
        .map(|receipt| receipt.counters.items)
        .unwrap_or_default()
}

struct RunningServer {
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("server task stops");
    }
}

async fn start_server(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningServer {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&pull_subject(tenant))
        .await;
    RunningServer {
        server,
        shutdown,
        task,
    }
}

#[derive(Default)]
struct PullPageStats {
    content_pages: AtomicUsize,
    maximum_rows: AtomicUsize,
}

impl PullPageStats {
    fn reset(&self) {
        self.content_pages.store(0, Ordering::SeqCst);
        self.maximum_rows.store(0, Ordering::SeqCst);
    }

    fn record(&self, rows: usize) {
        if rows > 0 {
            self.content_pages.fetch_add(1, Ordering::SeqCst);
        }
        self.maximum_rows.fetch_max(rows, Ordering::SeqCst);
    }
}

struct InspectPullPages {
    inner: Arc<dyn ClientTransport>,
    subject: String,
    stats: Arc<PullPageStats>,
}

impl ClientTransport for InspectPullPages {
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

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let inner = self.inner.clone();
        let inspect = subject == self.subject;
        let stats = self.stats.clone();
        Box::pin(async move {
            let reply = inner.request(subject, request_bytes, timeout).await?;
            if inspect {
                let envelope = decode(&reply).map_err(|error| {
                    TransportError::Other(format!("decode inspected pull response: {error}"))
                })?;
                let rows = match envelope.message_type {
                    MessageType::PullResponse => {
                        let response: PullResponse = rmp_serde::from_slice(&envelope.payload)
                            .map_err(|error| TransportError::Other(error.to_string()))?;
                        response.changeset.rows.len()
                    }
                    MessageType::DependencyCompletePullResponse => {
                        let response: DependencyCompletePullResponse =
                            rmp_serde::from_slice(&envelope.payload)
                                .map_err(|error| TransportError::Other(error.to_string()))?;
                        response.ordinary.changeset.rows.len()
                            + response
                                .units
                                .iter()
                                .map(|unit| unit.rows.len())
                                .sum::<usize>()
                    }
                    _ => 0,
                };
                stats.record(rows);
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

struct LoseRecoveredRowAck {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
    armed: AtomicBool,
    dropped: AtomicBool,
}

impl LoseRecoveredRowAck {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl ClientTransport for LoseRecoveredRowAck {
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

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let should_drop = subject == self.push_subject
            && self.armed.load(Ordering::SeqCst)
            && !self.dropped.load(Ordering::SeqCst)
            && decode(&request_bytes)
                .ok()
                .and_then(|envelope| {
                    matches!(
                        envelope.message_type,
                        MessageType::PushRequest | MessageType::DependencyCompletePushRequest
                    )
                    .then(|| rmp_serde::from_slice::<PushRequest>(&envelope.payload).ok())
                    .flatten()
                })
                .is_some_and(|request| {
                    request
                        .changeset
                        .rows
                        .iter()
                        .any(|row| row.table == "partitioned_notes")
                });
        let inner = self.inner.clone();
        Box::pin(async move {
            let reply = inner
                .request_single_reply(subject, request_bytes, timeout)
                .await?;
            if should_drop
                && self
                    .dropped
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(TransportError::IncompleteReply(
                    "the recovered table committed but its acknowledgement was lost".to_string(),
                ));
            }
            Ok(reply)
        })
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

#[tokio::test]
async fn newer_hub_restart_keeps_the_holdback_and_recovers_it_in_bounded_pages() {
    let temp = tempfile::TempDir::new().expect("temporary hub directory");
    let path = temp.path().join("hub.redb");
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open(&path).expect("open file-backed hub"));
    let receiver = Arc::new(Database::open_memory());
    declare_tables(&source);
    insert_ordinary(&source, Uuid::from_u128(0xE001), "first");
    for offset in 0..RECOVERY_ROWS {
        insert_partitioned(
            &source,
            Uuid::from_u128(0xE100 + offset as u128),
            Uuid::from_u128(0xE900),
        );
    }

    let hub_identity = Arc::new(FabricIdentity::generate());
    let first_server =
        start_server(&broker, PULL_TENANT, source.clone(), hub_identity.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node_id = receiver_identity.node_id();
    let page_stats = Arc::new(PullPageStats::default());
    let transport = Arc::new(InspectPullPages {
        inner: broker.client_as(&receiver_node_id),
        subject: pull_subject(PULL_TENANT),
        stats: page_stats.clone(),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport,
        TenantId::from(PULL_TENANT),
        receiver_identity,
    );
    first_server
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node_id, false);
    client
        .pull_default()
        .await
        .expect("ordinary table crosses while the partitioned table waits");
    assert_eq!(
        first_server.server.schema_sync_holdbacks(),
        expected_holdback("partitioned_notes", &receiver_node_id)
    );
    first_server.stop().await;
    source.close().expect("close first hub process");
    drop(source);

    let reopened_source = Arc::new(Database::open(&path).expect("reopen hub after restart"));
    let restarted_server =
        start_server(&broker, PULL_TENANT, reopened_source.clone(), hub_identity).await;
    restarted_server
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node_id, false);
    assert_eq!(
        restarted_server.server.schema_sync_holdbacks(),
        expected_holdback("partitioned_notes", &receiver_node_id),
        "a sender reboot must not erase the table and node waiting for upgrade"
    );

    insert_ordinary(&reopened_source, Uuid::from_u128(0xE002), "second");
    client
        .pull_default()
        .await
        .expect("healthy work keeps crossing after the sender restart");
    assert_eq!(
        text_column(
            receiver
                .execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("ordinary rows remain readable")
        ),
        vec!["first".to_string(), "second".to_string()]
    );

    page_stats.reset();
    restarted_server
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node_id, true);
    client
        .pull_default()
        .await
        .expect("upgrading after a sender restart resumes the held table");
    let recovered = receiver
        .execute("SELECT id FROM partitioned_notes", &empty())
        .expect("held table and rows arrive");
    assert_eq!(recovered.rows.len(), RECOVERY_ROWS);
    assert!(
        page_stats.content_pages.load(Ordering::SeqCst) >= 2,
        "a 501-row held image must be paged rather than buffered into one response"
    );
    assert!(
        page_stats.maximum_rows.load(Ordering::SeqCst) <= PULL_PAGE_SIZE,
        "no held-table recovery response may exceed the ordinary 500-row page bound"
    );
    assert!(restarted_server.server.schema_sync_holdbacks().is_empty());

    restarted_server.stop().await;
    reopened_source.close().expect("close restarted hub");
}

#[tokio::test]
async fn newer_edge_restart_keeps_private_progress_and_does_not_resend_healthy_history() {
    let temp = tempfile::TempDir::new().expect("temporary edge directory");
    let path = temp.path().join("edge.redb");
    let broker = InProcessBroker::new();
    let hub = Arc::new(Database::open_memory());
    let edge = Arc::new(Database::open(&path).expect("open file-backed edge"));
    declare_tables(&edge);
    insert_ordinary(&edge, Uuid::from_u128(0xF001), "first");
    insert_partitioned(&edge, Uuid::from_u128(0xF003), Uuid::from_u128(0xF004));

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let running = start_server(&broker, PUSH_TENANT, hub.clone(), hub_identity).await;
    broker
        .wait_for_registered_route_for_test(&push_subject(PUSH_TENANT))
        .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let first_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(PUSH_TENANT),
        edge_identity.clone(),
    );
    first_client.set_peer_vector_schema_support_for_test(false);
    first_client
        .push()
        .await
        .expect("ordinary data crosses while one table waits");
    assert_eq!(
        first_client.schema_sync_holdbacks(),
        expected_holdback("partitioned_notes", &hub_node_id)
    );
    drop(first_client);
    edge.close().expect("close first edge process");
    drop(edge);

    let reopened_edge = Arc::new(Database::open(&path).expect("reopen edge"));
    let restarted_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        reopened_edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(PUSH_TENANT),
        edge_identity,
    );
    restarted_client.set_peer_vector_schema_support_for_test(false);
    assert_eq!(
        restarted_client.schema_sync_holdbacks(),
        expected_holdback("partitioned_notes", &hub_node_id),
        "an edge reboot must retain both the held table and its private compatible frontier"
    );
    restarted_client
        .push()
        .await
        .expect("a no-op old-protocol push remains a no-op after restart");
    assert_eq!(
        sent_sync_rows(&restarted_client, &hub_node_id),
        0,
        "healthy history already accepted before restart must not cross again"
    );

    insert_ordinary(&reopened_edge, Uuid::from_u128(0xF002), "second");
    restarted_client
        .push()
        .await
        .expect("only new healthy work crosses after restart");
    assert_eq!(sent_sync_rows(&restarted_client, &hub_node_id), 1);
    assert_eq!(
        text_column(
            hub.execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("hub ordinary rows")
        ),
        vec!["first".to_string(), "second".to_string()]
    );

    restarted_client.set_peer_vector_schema_support_for_test(true);
    restarted_client
        .push()
        .await
        .expect("hub upgrade resumes the held edge table after edge restart");
    assert_eq!(
        hub.execute("SELECT id FROM partitioned_notes", &empty())
            .expect("held row arrives")
            .rows
            .len(),
        1
    );
    assert!(restarted_client.schema_sync_holdbacks().is_empty());
    assert!(restarted_client.push_watermark() > Lsn(0));

    running.stop().await;
    reopened_edge.close().expect("close restarted edge");
}

#[tokio::test]
async fn a_lost_held_table_reply_requires_its_authenticated_echo_beside_the_older_high_watermark() {
    let broker = InProcessBroker::new();
    let edge = Arc::new(Database::open_memory());
    let hub = Arc::new(Database::open_memory());
    declare_tables(&edge);
    insert_partitioned(&edge, Uuid::from_u128(0xA003), Uuid::from_u128(0xA004));
    insert_ordinary(&edge, Uuid::from_u128(0xA005), "newer-compatible");

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let running = start_server(&broker, LOST_ACK_TENANT, hub.clone(), hub_identity).await;
    broker
        .wait_for_registered_route_for_test(&push_subject(LOST_ACK_TENANT))
        .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let transport = Arc::new(LoseRecoveredRowAck {
        inner: broker.client_as(&edge_node_id),
        push_subject: push_subject(LOST_ACK_TENANT),
        armed: AtomicBool::new(false),
        dropped: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        transport.clone(),
        TenantId::from(LOST_ACK_TENANT),
        edge_identity,
    );
    client.set_peer_vector_schema_support_for_test(false);
    client
        .push()
        .await
        .expect("newer compatible work crosses the older held-table frontier");
    assert_eq!(
        client.schema_sync_holdbacks(),
        expected_holdback("partitioned_notes", &hub_node_id)
    );
    let healthy_frontier = hub
        .persisted_sync_applied_push_watermark_for_node(
            &TenantId::from(LOST_ACK_TENANT),
            &edge_node_id,
        )
        .expect("read hub edge receipt")
        .expect("healthy push receipt exists");

    client.set_peer_vector_schema_support_for_test(true);
    transport.arm();
    client
        .push()
        .await
        .expect("the held row's authenticated echo confirms its outcome");
    let received = running.server.transfer_receipts();
    client
        .push()
        .await
        .expect("confirmed held work stays retired");
    assert_eq!(
        running.server.transfer_receipts(),
        received,
        "the row confirmed through its echo must never be resent as a fresh mutation"
    );
    assert!(transport.dropped.load(Ordering::SeqCst));
    assert_eq!(
        hub.execute("SELECT id FROM partitioned_notes", &empty())
            .expect("recovered row committed before its acknowledgement was lost")
            .rows
            .len(),
        1
    );
    assert!(client.schema_sync_holdbacks().is_empty());
    assert!(
        client.push_watermark() >= healthy_frontier,
        "lost-ack recovery cannot regress or skip the already-confirmed healthy frontier"
    );

    running.stop().await;
}
