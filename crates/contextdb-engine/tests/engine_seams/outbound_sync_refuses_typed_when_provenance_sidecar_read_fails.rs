//! Provenance read failures refuse real outbound exchanges before sending data.
use contextdb_core::{Error, Lsn, TenantId, Value};
use contextdb_engine::transport::{ClientTransport, TransportFuture, TransportStatusFuture};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use redb::{ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

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
struct CountingClient {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
    sends: Arc<AtomicUsize>,
}
impl ClientTransport for CountingClient {
    fn ensure_connected(&self) -> TransportFuture<'_, ()> {
        self.inner.ensure_connected()
    }
    fn reconnect(&self) -> TransportFuture<'_, ()> {
        self.inner.reconnect()
    }
    fn is_connected(&self) -> TransportStatusFuture<'_> {
        self.inner.is_connected()
    }
    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }
    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }
    fn request<'a>(
        &'a self,
        subject: &'a str,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            self.sends.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.request(subject, bytes, timeout)
    }
}
fn set_kind(path: &Path, key: &[u8], kind: u8) -> u8 {
    let raw = redb::Database::open(path).unwrap();
    let tx = raw.begin_write().unwrap();
    let old;
    {
        let sources = tx
            .open_table(TableDefinition::<&[u8], u64>::new("sync_row_source_lsn"))
            .unwrap();
        assert!(
            sources.get(key).unwrap().is_some(),
            "fault must target a real source-LSN sidecar"
        );
        let mut kinds = tx
            .open_table(TableDefinition::<&[u8], u8>::new("sync_row_source_kind"))
            .unwrap();
        old = kinds.get(key).unwrap().unwrap().value();
        kinds.insert(key, kind).unwrap();
    }
    tx.commit().unwrap();
    old
}
fn typed_fault(error: Error) {
    assert!(
        matches!(error, Error::StoreCorrupted { ref path, ref reason }
        if path == "sync provenance" && reason == "unknown source kind"),
        "actual checked read refusal: {error:?}"
    );
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_sync_refuses_typed_when_provenance_sidecar_read_fails() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.redb");
    let edge_path = directory.path().join("edge.redb");
    let sink_path = directory.path().join("sink.redb");
    let source = Arc::new(open(&source_path));
    let edge = Arc::new(open(&edge_path));
    source.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST", &HashMap::new()).unwrap();
    source
        .execute(
            "INSERT INTO notes VALUES (1,'pulled-a','[1,0,0]'),(2,'pulled-b','[0,1,0]')",
            &HashMap::new(),
        )
        .unwrap();
    let tenant = TenantId::from("provenance-read-refusal");
    let source_identity = Arc::new(FabricIdentity::generate());
    let edge_identity = Arc::new(FabricIdentity::generate());
    let broker = InProcessBroker::new();
    let (stop, task, server) =
        serve(source.clone(), &broker, source_identity.clone(), &tenant).await;
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_identity.node_id()),
        tenant.clone(),
        edge_identity.clone(),
    );
    let result = client.pull_default().await.unwrap();
    assert_eq!(result.applied_rows, 2);
    let changes = edge.changes_since(Lsn(0));
    let rows: Vec<_> = changes
        .rows
        .iter()
        .filter(|row| row.table == "notes" && !row.deleted)
        .cloned()
        .collect();
    let vectors: Vec<_> = changes
        .vectors
        .iter()
        .filter(|vector| vector.index.table == "notes" && !vector.vector.is_empty())
        .cloned()
        .collect();
    assert_eq!((rows.len(), vectors.len()), (2, 2));
    assert!(rows.iter().all(|row| row.lsn == rows[0].lsn));
    assert!(vectors.iter().all(|vector| vector.lsn == rows[0].lsn));
    assert!(rows.iter().all(|row| edge.row_change_arrived_by_sync(row)));
    assert!(
        vectors
            .iter()
            .all(|vector| edge.vector_change_arrived_by_sync(vector))
    );
    let a = rows
        .iter()
        .find(|row| row.natural_key.value == Value::Int64(1))
        .unwrap();
    let owner = vectors
        .iter()
        .find(|vector| vector.vector == vec![1., 0., 0.])
        .unwrap()
        .row_id;
    let mut key = b"notes".to_vec();
    key.extend_from_slice(&owner.0.to_be_bytes());
    let persisted = edge.persisted_sync_watermarks(&tenant).unwrap();
    let cursor = edge.persisted_sync_pull_cursor(&tenant).unwrap();
    drop(client);
    stop_server(stop, task, server).await;
    edge.close().unwrap();
    drop(edge);
    let old_kind = set_kind(&edge_path, &key, 255);
    assert_eq!(old_kind, 0, "fault replaces real Pulled provenance");

    let edge = Arc::new(open(&edge_path));
    let broker = InProcessBroker::new();
    let (stop, task, server) =
        serve(source.clone(), &broker, source_identity.clone(), &tenant).await;
    let sends = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(CountingClient {
        inner: broker.client_as(&edge_identity.node_id()),
        push_subject: contextdb_server::subjects::push_subject(tenant.as_str()),
        sends: sends.clone(),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        transport,
        tenant.clone(),
        edge_identity.clone(),
    );
    let before = (client.push_watermark(), client.pull_watermark());
    typed_fault(client.pending_push_change_count().unwrap_err());
    typed_fault(client.__ordinary_push_request_for_test().unwrap_err());
    typed_fault(client.push().await.unwrap_err());
    assert_eq!(
        sends.load(Ordering::SeqCst),
        0,
        "no failed provenance is offered as authored data"
    );
    assert_eq!((client.push_watermark(), client.pull_watermark()), before);
    assert_eq!(edge.persisted_sync_watermarks(&tenant).unwrap(), persisted);
    assert_eq!(edge.persisted_sync_pull_cursor(&tenant).unwrap(), cursor);
    drop(client);
    stop_server(stop, task, server).await;

    // Serve the same corrupted durable owner to a third peer. The real server
    // propagates its checked extraction failure through the transport boundary.
    let sink = Arc::new(open(&sink_path));
    let sink_identity = Arc::new(FabricIdentity::generate());
    let broker = InProcessBroker::new();
    let (stop, task, server) = serve(edge.clone(), &broker, edge_identity.clone(), &tenant).await;
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        sink.clone(),
        broker.client_as(&sink_identity.node_id()),
        tenant.clone(),
        sink_identity,
    );
    let sink_progress = sink.persisted_sync_watermarks(&tenant).unwrap();
    let sink_cursor = sink.persisted_sync_pull_cursor(&tenant).unwrap();
    let error = client.pull_default().await.unwrap_err();
    assert!(
        matches!(error, Error::SyncError(ref message) if message.contains("sync provenance") && message.contains("unknown source kind")),
        "pull's typed transport refusal: {error:?}"
    );
    assert_eq!(
        sink.persisted_sync_watermarks(&tenant).unwrap(),
        sink_progress
    );
    assert_eq!(
        sink.persisted_sync_pull_cursor(&tenant).unwrap(),
        sink_cursor
    );
    assert!(
        broker
            .recorded_exchanges()
            .iter()
            .all(|exchange| exchange.subject
                != contextdb_server::subjects::pull_subject(tenant.as_str())),
        "refused extraction sends no successful pull page"
    );
    assert_eq!(edge.persisted_sync_watermarks(&tenant).unwrap(), persisted);
    assert_eq!(edge.persisted_sync_pull_cursor(&tenant).unwrap(), cursor);
    drop(client);
    stop_server(stop, task, server).await;
    sink.close().unwrap();
    drop(sink);
    edge.close().unwrap();
    drop(edge);
    assert_eq!(set_kind(&edge_path, &key, old_kind), 255);

    let edge = Arc::new(open(&edge_path));
    let broker = InProcessBroker::new();
    let (stop, task, server) =
        serve(source.clone(), &broker, source_identity.clone(), &tenant).await;
    let sends = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(CountingClient {
        inner: broker.client_as(&edge_identity.node_id()),
        push_subject: contextdb_server::subjects::push_subject(tenant.as_str()),
        sends: sends.clone(),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        transport,
        tenant.clone(),
        edge_identity.clone(),
    );
    assert_eq!(
        client.pending_push_change_count().unwrap(),
        0,
        "cleared fault suppresses both real pulled rows and vectors"
    );
    client.push().await.unwrap();
    assert_eq!(sends.load(Ordering::SeqCst), 0);
    // A destination restored before prior acknowledgement must start its
    // outbound reduction at the regression frontier, not an empty-push watermark.
    drop(client);
    edge.persist_sync_push_watermark(&tenant, Lsn(0)).unwrap();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_identity.node_id()),
        tenant.clone(),
        edge_identity,
    );
    // One owner in the same local commit can be pending a fresh hub ordering;
    // its sibling's Pulled marker must not suppress that local owner.
    let b = rows
        .iter()
        .find(|row| row.natural_key.value == Value::Int64(2))
        .unwrap();
    edge.record_hub_accepted_rows(
        std::slice::from_ref(b),
        source.current_lsn(),
        Some(&source_identity.node_id()),
    )
    .unwrap();
    edge.invalidate_accepted_local_ordering_after_hub_regression(Lsn(0))
        .unwrap();
    assert_eq!(a.lsn, b.lsn);
    assert_eq!(
        client.pending_push_change_count().unwrap(),
        2,
        "only pending-local B row and its vector remain outbound at the shared LSN"
    );
    assert!(edge.row_change_arrived_by_sync(a));
    assert!(!edge.row_change_arrived_by_sync(b));
    eprintln!(
        "provenance unknown-kind refusals: push payloads=0, pull pages=0; restored row/vector pending=0, same-LSN local owner pending=2"
    );
    drop(client);
    stop_server(stop, task, server).await;
    edge.close().unwrap();
    source.close().unwrap();
}
