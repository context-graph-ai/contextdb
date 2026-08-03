//! An explicit hub move carries the rows and deletes this edge inherited from
//! its former hub exactly once, without changing their creator identity.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::protocol::{MessageType, PushRequest, WireRowChange, decode};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
    client_transport,
};
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "destination-ancestry";
const LIVE_ID: &str = "00000000-0000-4000-8000-0000000000d1";
const DELETED_ID: &str = "00000000-0000-4000-8000-0000000000d2";
const NEW_HUB_ID: &str = "00000000-0000-4000-8000-0000000000d3";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh exchange")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_table(db: &Database) {
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("declare memories table");
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO memories (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert memory");
}

fn delete(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM memories WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("delete memory");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    let result = db
        .execute(
            "SELECT body FROM memories WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("select memory");
    result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(body)) => Some(body.clone()),
        _ => None,
    })
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

async fn start_hub(root: &Path) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    declare_table(&db);
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
            .expect("hub server task stops within the test bound");
        drop(self.server);
        drop(self.endpoint);
    }
}

struct RecordingPushTransport {
    inner: Arc<dyn ClientTransport>,
    rows: Mutex<Vec<WireRowChange>>,
}

impl RecordingPushTransport {
    fn new(inner: Arc<dyn ClientTransport>) -> Self {
        Self {
            inner,
            rows: Mutex::new(Vec::new()),
        }
    }

    fn rows(&self) -> Vec<WireRowChange> {
        self.rows.lock().expect("read recorded rows").clone()
    }

    fn record(&self, bytes: &[u8]) -> TransportResult<()> {
        let envelope = decode(bytes).map_err(|err| TransportError::Other(err.to_string()))?;
        if !matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) {
            return Ok(());
        }
        let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|err| TransportError::Other(err.to_string()))?;
        self.rows
            .lock()
            .expect("record pushed rows")
            .extend(request.changeset.rows);
        Ok(())
    }
}

impl ClientTransport for RecordingPushTransport {
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
        if let Err(error) = self.record(&request_bytes) {
            return Box::pin(async move { Err(error) });
        }
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if let Err(error) = self.record(&request_bytes) {
            return Box::pin(async move { Err(error) });
        }
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

fn recorded_row(rows: &[WireRowChange], id: Uuid, deleted: bool) -> &WireRowChange {
    rows.iter()
        .find(|row| {
            row.table == "memories"
                && row.natural_key.column == "id"
                && row.natural_key.value == Value::Uuid(id)
                && row.deleted == deleted
        })
        .unwrap_or_else(|| panic!("recorded push lacks memories/{id} deleted={deleted}: {rows:?}"))
}

#[tokio::test]
async fn explicit_destination_change_reuploads_inherited_live_and_delete_lineage_once() {
    let root = tempfile::tempdir().expect("tempdir");
    let old_hub_root = root.path().join("old-hub");
    let new_hub_root = root.path().join("new-hub");
    std::fs::create_dir_all(&old_hub_root).expect("old hub directory");
    std::fs::create_dir_all(&new_hub_root).expect("new hub directory");
    let old_hub = start_hub(&old_hub_root).await;
    let new_hub = start_hub(&new_hub_root).await;

    let creator_path = root.path().join("creator.db");
    let creator_identity_path = root.path().join("creator.db.fabric-identity.key");
    let creator_identity =
        FabricIdentity::load_or_generate(&creator_identity_path).expect("creator identity");
    let creator_node_id = creator_identity.node_id();
    let creator_db = Arc::new(Database::open(&creator_path).expect("open creator database"));
    declare_table(&creator_db);
    let creator_dial = peer_dial_spec(&old_hub.ticket, &creator_identity_path);
    let creator = SyncClient::new(creator_db.clone(), &creator_dial, TenantId::from(TENANT));
    let live_id = Uuid::parse_str(LIVE_ID).expect("live UUID");
    let deleted_id = Uuid::parse_str(DELETED_ID).expect("deleted UUID");
    insert(&creator_db, live_id, "inherited-live");
    insert(&creator_db, deleted_id, "inherited-then-deleted");
    within(creator.push())
        .await
        .expect("creator rows reach old hub");

    let relay_path = root.path().join("relay.db");
    let relay_identity_path = root.path().join("relay.db.fabric-identity.key");
    let relay_db = Arc::new(Database::open(&relay_path).expect("open relay database"));
    declare_table(&relay_db);
    let old_hub_dial = peer_dial_spec(&old_hub.ticket, &relay_identity_path);
    let relay = SyncClient::new(relay_db.clone(), &old_hub_dial, TenantId::from(TENANT));
    within(relay.push())
        .await
        .expect("relay declaration reaches old hub");
    within(relay.pull_default())
        .await
        .expect("relay inherits creator rows from old hub");
    assert_eq!(body(&relay_db, live_id).as_deref(), Some("inherited-live"));
    assert_eq!(
        body(&relay_db, deleted_id).as_deref(),
        Some("inherited-then-deleted")
    );

    delete(&creator_db, deleted_id);
    within(creator.push())
        .await
        .expect("creator delete reaches old hub");
    within(relay.pull_default())
        .await
        .expect("relay inherits creator delete from old hub");
    assert_eq!(body(&relay_db, deleted_id), None);
    assert_eq!(
        relay
            .pending_push_change_count()
            .expect("ordinary pending count"),
        0,
        "ordinary self-echo suppression excludes both inherited versions"
    );

    creator.shutdown().await;
    relay.shutdown().await;
    drop(creator);
    drop(relay);

    let new_hub_dial = peer_dial_spec(&new_hub.ticket, &relay_identity_path);
    let moving = SyncClient::new(relay_db.clone(), &new_hub_dial, TenantId::from(TENANT));
    moving
        .change_destination(&new_hub.node_id)
        .expect("explicitly move to new authenticated hub");
    let new_hub_id = Uuid::parse_str(NEW_HUB_ID).expect("new-hub UUID");
    insert(&new_hub.db, new_hub_id, "new-hub-authored");
    within(moving.pull_default())
        .await
        .expect("relay receives a new-hub-authored row before rebuilding the hub");
    assert_eq!(
        body(&relay_db, new_hub_id).as_deref(),
        Some("new-hub-authored"),
        "the new hub's row is locally visible before the re-upload"
    );
    assert!(
        moving
            .pending_push_change_count()
            .expect("moved pending count")
            >= 2,
        "the explicit move selects the inherited live row and delete"
    );
    moving.shutdown().await;
    drop(moving);
    drop(relay_db);

    let reopened_relay =
        Arc::new(Database::open(&relay_path).expect("reopen relay after destination change"));
    assert_eq!(
        reopened_relay.retention_sync_peer().as_deref(),
        Some(new_hub.node_id.as_str()),
        "the explicit destination survives the restart"
    );
    assert!(
        reopened_relay
            .destination_reupload_frontier(&TenantId::from(TENANT), &new_hub.node_id)
            .expect("read durable destination re-upload")
            .is_some(),
        "the inherited re-upload remains pending across the restart"
    );

    let relay_identity =
        Arc::new(FabricIdentity::load_or_generate(&relay_identity_path).expect("relay identity"));
    let recording = Arc::new(RecordingPushTransport::new(client_transport(&new_hub_dial)));
    let moved = SyncClient::with_authenticated_transport_and_identity_for_test(
        reopened_relay.clone(),
        recording.clone(),
        TenantId::from(TENANT),
        relay_identity,
    );
    within(moved.push())
        .await
        .expect("moved relay rebuilds the new empty hub");

    assert_eq!(
        body(&new_hub.db, live_id).as_deref(),
        Some("inherited-live"),
        "the new hub receives the inherited live row"
    );
    assert_eq!(
        body(&new_hub.db, deleted_id),
        None,
        "the inherited delete remains absent on the new hub"
    );
    let first_push_rows = recording.rows();
    for row in [
        recorded_row(&first_push_rows, live_id, false),
        recorded_row(&first_push_rows, deleted_id, true),
    ] {
        let lineage = row
            .lineage
            .as_ref()
            .expect("every re-uploaded row retains authenticated lineage");
        assert_eq!(
            lineage.author_node_id, creator_node_id,
            "the forwarding edge must not replace the original creator identity"
        );
        assert!(
            !lineage.attestation.is_empty(),
            "portable creator proof accompanies inherited ancestry"
        );
    }
    assert!(
        !first_push_rows.iter().any(|row| {
            row.table == "memories"
                && row.natural_key.column == "id"
                && row.natural_key.value == Value::Uuid(new_hub_id)
        }),
        "a row pulled from the new hub after the move remains an ordinary self-echo"
    );
    assert!(
        reopened_relay
            .destination_reupload_frontier(&TenantId::from(TENANT), &new_hub.node_id)
            .expect("read completed destination re-upload")
            .is_none(),
        "the confirmed re-upload retires its durable one-time marker"
    );

    assert_eq!(
        moved
            .pending_push_change_count()
            .expect("post-move pending count"),
        0,
        "ordinary self-echo suppression resumes after the one-time move"
    );
    let recorded_before_quiet_push = recording.rows().len();
    within(moved.push())
        .await
        .expect("quiet push after destination re-upload");
    assert_eq!(
        recording.rows().len(),
        recorded_before_quiet_push,
        "a later ordinary push does not echo the new hub's row back"
    );

    moved.shutdown().await;
    old_hub.stop().await;
    new_hub.stop().await;
}
