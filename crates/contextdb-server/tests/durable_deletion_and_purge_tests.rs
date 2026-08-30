//! File-backed, authenticated-Iroh journeys for durable deletes and PURGE.
//!
//! These tests deliberately use the public SQL and sync paths. They fail on
//! the missing durable deletion/PURGE implementation, never on a synthetic
//! changeset or a test-only apply shortcut.

use contextdb_core::{Error, Lsn, TenantId, Value, VectorIndexRef};
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_engine::database::{
    AuthoritativePurgeRootClassification, DurableDeletionStateSnapshot,
};
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSpec, MovementPolicy, any_node_holds_claim_for_blob,
    insert_claim, install_work_ledger_schema, node_claim_permits_movement,
    node_holds_claim_for_blob, submit_job,
};
use contextdb_engine::{Database, SinkEvent};
use contextdb_server::blob_resolver::{BlobStore, ResolveError};
use contextdb_server::protocol::{
    DependencyCompletePullResponse, MessageType, PullRequest, PushRequest, WireChangeSet,
    WireDdlChange, WirePurgeChange, WireRowLineage, decode, encode,
};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
    client_transport,
};
use contextdb_server::work_ledger::WORK_NODE_CONTACTS_TABLE;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded ticketed-Iroh operation")
}

async fn within_copy_class_replication<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(120), future)
        .await
        .expect("bounded high-cardinality copy-class replication")
}

fn spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn create_notes(db: &Database) {
    if db.table_meta("notes").is_none() {
        db.execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("create notes table");
    }
}

fn create_keep_first_notes(db: &Database) {
    if db.table_meta("first_notes").is_none() {
        db.execute(
            "CREATE TABLE first_notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .expect("create declared keep-first notes table");
    }
}

fn put(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, '[1,0,0]')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact note");
}

fn edit(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("edit exact note");
}

fn delete(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("commit local delete");
}

fn put_keep_first(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO first_notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert keep-first note");
}

fn delete_keep_first(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM first_notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("commit keep-first delete");
}

fn purge(db: &Database, id: Uuid) {
    db.execute(
        "PURGE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("authoritative SQL PURGE must remove the selected lineage");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    let result = db
        .execute(
            "SELECT body FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("read exact note");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("notes.body must be TEXT, got {other:?}"),
    })
}

fn keep_first_body(db: &Database, id: Uuid) -> Option<String> {
    let result = db
        .execute(
            "SELECT body FROM first_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("read keep-first note");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("first_notes.body must be TEXT, got {other:?}"),
    })
}

fn assert_absent(db: &Database, id: Uuid, place: &str) {
    assert_eq!(body(db, id), None, "{place} must not serve the removed row");
    let visible = db.scan("notes", db.snapshot()).expect("scan notes");
    assert!(
        visible
            .iter()
            .all(|row| row.values.get("id") != Some(&Value::Uuid(id))),
        "{place} must not retain the removed row in the engine scan path"
    );
}

fn note_keys(db: &Database) -> BTreeSet<Uuid> {
    db.execute("SELECT id FROM notes", &HashMap::new())
        .expect("select exact note keys")
        .rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Uuid(id) => id,
            ref other => panic!("notes.id must be UUID, got {other:?}"),
        })
        .collect()
}

fn deletion_state(db: &Database, id: Uuid) -> DurableDeletionStateSnapshot {
    db.durable_deletion_state_for_test("notes", &Value::Uuid(id))
}

/// Leaves the authenticated Iroh request untouched while observing the
/// immutable creation lineage carried for one row.
struct RecordingRowLineageTransport {
    inner: Arc<dyn ClientTransport>,
    id: Uuid,
    captured: Mutex<Vec<WireRowLineage>>,
}

impl RecordingRowLineageTransport {
    fn new(inner: Arc<dyn ClientTransport>, id: Uuid) -> Self {
        Self {
            inner,
            id,
            captured: Mutex::new(Vec::new()),
        }
    }

    fn inspect(&self, bytes: &[u8]) -> TransportResult<()> {
        let envelope = decode(bytes).map_err(|err| TransportError::Other(err.to_string()))?;
        if !matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) {
            return Ok(());
        }
        let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|err| TransportError::Other(err.to_string()))?;
        for row in request.changeset.rows {
            if row.table == "notes" && row.natural_key.value == Value::Uuid(self.id) && !row.deleted
            {
                self.captured
                    .lock()
                    .expect("record pushed row lineage")
                    .push(
                        row.lineage
                            .expect("production v6 push carries immutable row lineage"),
                    );
            }
        }
        Ok(())
    }

    fn captured(&self) -> Vec<WireRowLineage> {
        self.captured
            .lock()
            .expect("read recorded row lineages")
            .clone()
    }
}

impl ClientTransport for RecordingRowLineageTransport {
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
        if let Err(error) = self.inspect(&request_bytes) {
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
        if let Err(error) = self.inspect(&request_bytes) {
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

/// Sends one otherwise-authenticated push with a distinct purge instruction
/// attached. The Iroh transport itself remains the caller, so the hub still
/// sees the configured edge's production fabric identity.
struct PurgeInjectingPushTransport {
    inner: Arc<dyn ClientTransport>,
    injected: AtomicBool,
}

impl PurgeInjectingPushTransport {
    fn new(inner: Arc<dyn ClientTransport>) -> Self {
        Self {
            inner,
            injected: AtomicBool::new(false),
        }
    }

    fn inject_purge(&self, bytes: Vec<u8>) -> TransportResult<Vec<u8>> {
        let envelope = decode(&bytes).map_err(|error| TransportError::Other(error.to_string()))?;
        if !matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) || self.injected.swap(true, Ordering::SeqCst)
        {
            return Ok(bytes);
        }
        let mut request: PushRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|error| TransportError::Other(error.to_string()))?;
        request.changeset.purges.push(WirePurgeChange::default());
        encode(envelope.message_type, &request)
            .map_err(|error| TransportError::Other(error.to_string()))
    }
}

impl ClientTransport for PurgeInjectingPushTransport {
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
        match self.inject_purge(request_bytes) {
            Ok(request_bytes) => self.inner.request(subject, request_bytes, timeout),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        match self.inject_purge(request_bytes) {
            Ok(request_bytes) => self
                .inner
                .request_single_reply(subject, request_bytes, timeout),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

/// Sends otherwise-authenticated data pushes with an invalid immutable root.
/// The server must reply to each retry; leaving an admitted malformed request
/// in the in-flight fanout would make the second request wait forever.
struct MalformedLineagePushTransport {
    inner: Arc<dyn ClientTransport>,
}

impl MalformedLineagePushTransport {
    fn corrupt_lineage(&self, bytes: Vec<u8>) -> TransportResult<Vec<u8>> {
        let envelope = decode(&bytes).map_err(|error| TransportError::Other(error.to_string()))?;
        if !matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) {
            return Ok(bytes);
        }
        let mut request: PushRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|error| TransportError::Other(error.to_string()))?;
        let Some(lineage) = request
            .changeset
            .rows
            .iter_mut()
            .find_map(|row| row.lineage.as_mut())
        else {
            return Ok(bytes);
        };
        lineage.lineage_root = "forged-lineage-root".to_string();
        encode(envelope.message_type, &request)
            .map_err(|error| TransportError::Other(error.to_string()))
    }
}

impl ClientTransport for MalformedLineagePushTransport {
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
        match self.corrupt_lineage(request_bytes) {
            Ok(request_bytes) => self.inner.request(subject, request_bytes, timeout),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        match self.corrupt_lineage(request_bytes) {
            Ok(request_bytes) => self
                .inner
                .request_single_reply(subject, request_bytes, timeout),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

/// Records the hub pages an ordinary sync client applies while making the
/// server enforce a single source frontier per pull reply. This keeps the
/// production Iroh identities and apply path intact; only the wire request's
/// explicit pagination limit is narrowed for this regression.
#[derive(Debug, Clone)]
struct RecordedPullPage {
    request: PullRequest,
    ordinary: contextdb_server::protocol::PullResponse,
    dependency_units: Vec<WireChangeSet>,
}

struct OneFrontierPullTransport {
    inner: Arc<dyn ClientTransport>,
    pages: Mutex<Vec<RecordedPullPage>>,
    pull_requests: AtomicUsize,
    pause_before_second_request: Mutex<Option<(Arc<Notify>, tokio::sync::oneshot::Receiver<()>)>>,
}

struct SecondPullRequestPause {
    reached: Arc<Notify>,
    release: tokio::sync::oneshot::Sender<()>,
}

impl OneFrontierPullTransport {
    fn new(inner: Arc<dyn ClientTransport>) -> Self {
        Self {
            inner,
            pages: Mutex::new(Vec::new()),
            pull_requests: AtomicUsize::new(0),
            pause_before_second_request: Mutex::new(None),
        }
    }

    fn pause_before_second_request_for_test(&self) -> SecondPullRequestPause {
        let reached = Arc::new(Notify::new());
        let (release, receiver) = tokio::sync::oneshot::channel();
        *self
            .pause_before_second_request
            .lock()
            .expect("arm second pull request pause") = Some((reached.clone(), receiver));
        SecondPullRequestPause { reached, release }
    }

    fn pages(&self) -> Vec<RecordedPullPage> {
        self.pages.lock().expect("read recorded pull pages").clone()
    }

    fn force_one_frontier(&self, bytes: Vec<u8>) -> TransportResult<(Vec<u8>, PullRequest)> {
        let envelope = decode(&bytes).map_err(|error| TransportError::Other(error.to_string()))?;
        if !matches!(envelope.message_type, MessageType::PullRequest) {
            return Err(TransportError::Other(
                "one-frontier transport received a non-pull request".to_string(),
            ));
        }
        let mut request: PullRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|error| TransportError::Other(error.to_string()))?;
        request.max_entries = Some(1);
        let bytes = encode(MessageType::PullRequest, &request)
            .map_err(|error| TransportError::Other(error.to_string()))?;
        Ok((bytes, request))
    }

    fn record_response(&self, request: PullRequest, bytes: &[u8]) -> TransportResult<()> {
        let envelope = decode(bytes).map_err(|error| TransportError::Other(error.to_string()))?;
        let (ordinary, dependency_units) = match envelope.message_type {
            MessageType::PullResponse => (
                rmp_serde::from_slice(&envelope.payload)
                    .map_err(|error| TransportError::Other(error.to_string()))?,
                Vec::new(),
            ),
            MessageType::DependencyCompletePullResponse => {
                let response: DependencyCompletePullResponse =
                    rmp_serde::from_slice(&envelope.payload)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                (response.ordinary, response.units)
            }
            other => {
                return Err(TransportError::Other(format!(
                    "one-frontier pull received {other:?}, not a pull response"
                )));
            }
        };
        self.pages
            .lock()
            .expect("record pull page")
            .push(RecordedPullPage {
                request,
                ordinary,
                dependency_units,
            });
        Ok(())
    }

    fn send<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
        single_reply: bool,
    ) -> TransportFuture<'a, Vec<u8>> {
        let (request_bytes, request) = match self.force_one_frontier(request_bytes) {
            Ok(request) => request,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let request_number = self.pull_requests.fetch_add(1, Ordering::SeqCst) + 1;
        let inner = self.inner.clone();
        let subject = subject.to_string();
        Box::pin(async move {
            let pause = if request_number == 2 {
                self.pause_before_second_request
                    .lock()
                    .expect("read second pull request pause")
                    .take()
            } else {
                None
            };
            if let Some((reached, release)) = pause {
                reached.notify_one();
                let _ = release.await;
            }
            let response = if single_reply {
                inner
                    .request_single_reply(&subject, request_bytes, timeout)
                    .await?
            } else {
                inner.request(&subject, request_bytes, timeout).await?
            };
            self.record_response(request, &response)?;
            Ok(response)
        })
    }
}

impl ClientTransport for OneFrontierPullTransport {
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
        self.send(subject, request_bytes, timeout, false)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.send(subject, request_bytes, timeout, true)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

fn keep_first_deletion_state(db: &Database, id: Uuid) -> DurableDeletionStateSnapshot {
    db.durable_deletion_state_for_test("first_notes", &Value::Uuid(id))
}

fn exact_delete_refusal(
    result: &impl serde::Serialize,
    id: Uuid,
    winner_node_id: &str,
    winner_position: u64,
) -> serde_json::Value {
    let rendered = serde_json::to_value(result).expect("typed sync result serializes");
    let conflicts = rendered["conflicts"]
        .as_array()
        .expect("refused delete has typed conflicts");
    assert_eq!(conflicts.len(), 1, "one refused delete has one receipt");
    let conflict = &conflicts[0];
    assert_eq!(conflict["table"], "first_notes", "receipt names table");
    assert_eq!(
        conflict["natural_key"],
        serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(id)))
            .expect("serialize exact natural key"),
        "receipt names exact refused key"
    );
    assert_eq!(conflict["mutation_kind"], "delete", "receipt names delete");
    assert_eq!(
        conflict["winning_author_node_id"], winner_node_id,
        "receipt names the winner's persisted authenticated node identity"
    );
    assert_eq!(
        conflict["hub_acceptance_position"].as_u64(),
        Some(winner_position),
        "receipt carries the winner's exact hub acceptance position"
    );
    conflict.clone()
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&spec(&identity)).await.expect("bind hub");
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open file-backed hub"));
    create_notes(&db);
    create_keep_first_notes(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        node_id,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task).await.expect("hub must stop cleanly");
    }
}

fn edge(root: &Path, name: &str, ticket: &str, tenant: &str) -> (Arc<Database>, SyncClient) {
    let db =
        Arc::new(Database::open(root.join(format!("{name}.db"))).expect("open file-backed edge"));
    create_notes(&db);
    create_keep_first_notes(&db);
    let client = SyncClient::new(db.clone(), ticket, TenantId::from(tenant));
    (db, client)
}

fn reopen_edge(root: &Path, name: &str, ticket: &str, tenant: &str) -> (Arc<Database>, SyncClient) {
    let db =
        Arc::new(Database::open(root.join(format!("{name}.db"))).expect("reopen edge database"));
    create_notes(&db);
    create_keep_first_notes(&db);
    let client = SyncClient::new(db.clone(), ticket, TenantId::from(tenant));
    (db, client)
}

fn copy_test_edge(
    root: &Path,
    name: &str,
    ticket: &str,
    tenant: &str,
) -> (Arc<Database>, SyncClient, PathBuf) {
    let participant_directory = root.join(name);
    std::fs::create_dir_all(&participant_directory)
        .expect("create isolated copy-test participant directory");
    let database_path = participant_directory.join(format!("{name}.db"));
    let identity_path = participant_directory.join(format!("{name}.db.fabric-identity.key"));
    let db = Arc::new(Database::open(database_path).expect("open file-backed copy-test edge"));
    create_notes(&db);
    create_keep_first_notes(&db);
    let client = SyncClient::new(
        db.clone(),
        &peer_dial_spec(ticket, &identity_path),
        TenantId::from(tenant),
    );
    (db, client, identity_path)
}

fn create_purge_copy_tables(db: &Database) {
    for ddl in [
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT) SYNC CONFLICT KEEP LATEST",
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC CONFLICT KEEP LATEST",
        "CREATE INDEX notes_body_idx ON notes (body)",
        "CREATE TABLE purge_notifications (id UUID PRIMARY KEY, payload TEXT) SYNC CONFLICT KEEP LATEST",
        "CREATE EVENT TYPE purge_notification_insert WHEN INSERT ON purge_notifications",
        "CREATE SINK purge_notification_sink TYPE callback",
        "CREATE ROUTE purge_notification_route EVENT purge_notification_insert TO purge_notification_sink",
    ] {
        db.execute(ddl, &HashMap::new())
            .unwrap_or_else(|error| panic!("create copy-class fixture with `{ddl}`: {error}"));
    }
    install_work_ledger_schema(db).expect("install work-ledger schema");
}

fn purge_exact(db: &Database, table: &str, column: &str, value: Value) {
    db.execute(
        &format!("PURGE FROM {table} WHERE {column} = $selected"),
        &HashMap::from([("selected".to_string(), value)]),
    )
    .unwrap_or_else(|error| panic!("authoritative purge of {table}.{column} failed: {error}"));
}

fn insert_graph_edge(db: &Database, id: Uuid, source: Uuid, target: Uuid) {
    for (node_id, name) in [(source, "source"), (target, "target")] {
        db.execute(
            "INSERT INTO nodes (id, name) VALUES ($id, $name) ON CONFLICT (id) DO UPDATE SET name = $name",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(node_id)),
                ("name".to_string(), Value::Text(name.to_string())),
            ]),
        )
        .expect("insert graph node");
    }
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'LINKS')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("source".to_string(), Value::Uuid(source)),
            ("target".to_string(), Value::Uuid(target)),
        ]),
    )
    .expect("insert graph edge");
}

fn graph_targets(db: &Database, source: Uuid) -> BTreeSet<Uuid> {
    let result = db
        .execute(
            "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS target))",
            &HashMap::from([("source".to_string(), Value::Uuid(source))]),
        )
        .expect("traverse exact graph source");
    assert_eq!(
        result.trace.physical_plan, "AdjacencyProbe",
        "graph proof must read the production adjacency index"
    );
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some("forward_adj"),
        "graph proof must read the forward adjacency index"
    );
    result
        .rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Uuid(id) => id,
            ref other => panic!("graph target must be UUID, got {other:?}"),
        })
        .collect()
}

fn row_id(db: &Database, id: Uuid) -> contextdb_core::RowId {
    db.scan("notes", db.snapshot())
        .expect("scan selected vector source")
        .into_iter()
        .find(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .expect("selected row exists")
        .row_id
}

fn insert_notification(db: &Database, id: Uuid, payload: &str) {
    db.execute(
        "INSERT INTO purge_notifications (id, payload) VALUES ($id, $payload)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("payload".to_string(), Value::Text(payload.to_string())),
        ]),
    )
    .expect("queue durable sink payload");
}

fn submit_blob_job(db: &Database, job_id: &str, submitter: &str, claimant: &str, hash: &BlobHash) {
    let spec = JobSpec::builder(job_id, "media.purge", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job(db, &spec, &[] as &[&[u8]]).expect("submit blob-reference job");
    match insert_claim(
        db,
        job_id,
        1,
        claimant,
        1_700_000_300_000,
        1_700_000_000_000,
    )
    .expect("insert blob claim")
    {
        ClaimInsert::Inserted => {}
        other => panic!("blob claim must be newly inserted, got {other:?}"),
    }
}

fn change_set_contains(
    db: &Database,
    selected: Uuid,
    selected_row_id: contextdb_core::RowId,
    secret: &str,
) -> bool {
    let changes = db.changes_since(Lsn(0));
    changes.rows.iter().any(|row| {
        row.values
            .values()
            .any(|value| value == &Value::Text(secret.to_string()))
    }) || changes
        .edges
        .iter()
        .any(|edge| edge.source == selected || edge.target == selected)
        || changes
            .vectors
            .iter()
            .any(|vector| vector.row_id == selected_row_id)
}

fn work_job_ids(db: &Database, job_id: &str) -> Vec<String> {
    db.execute(
        "SELECT job_id FROM work_jobs WHERE job_id = $job_id",
        &HashMap::from([("job_id".to_string(), Value::Text(job_id.to_string()))]),
    )
    .expect("query exact work-ledger referent")
    .rows
    .into_iter()
    .map(|row| match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("work_jobs.job_id must be TEXT, got {other:?}"),
    })
    .collect()
}

fn assert_production_sink_delivers_only_survivor(
    db: &Database,
    selected: Uuid,
    survivor: Uuid,
    place: &str,
) {
    let sentinel = Uuid::new_v4();
    insert_notification(db, sentinel, "sink-drain-sentinel");
    let (delivered_tx, delivered_rx) = mpsc::channel::<SinkEvent>();
    db.register_sink("purge_notification_sink", None, move |event| {
        delivered_tx
            .send(event.clone())
            .map_err(|error| contextdb_engine::SinkError::Permanent(error.to_string()))
    })
    .unwrap_or_else(|error| panic!("register {place} production sink callback: {error}"));

    let mut captured = Vec::new();
    for _ in 0..3 {
        let event = delivered_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|error| panic!("{place} sink drain marker must deliver: {error}"));
        let reached_sentinel = event.row_values.get("id") == Some(&Value::Uuid(sentinel));
        captured.push(event);
        if reached_sentinel {
            break;
        }
    }
    assert_eq!(
        captured.last().and_then(|event| event.row_values.get("id")),
        Some(&Value::Uuid(sentinel)),
        "{place} FIFO callback stream must reach the production drain sentinel"
    );
    let pre_purge = captured
        .iter()
        .filter(|event| event.row_values.get("id") != Some(&Value::Uuid(sentinel)))
        .collect::<Vec<_>>();
    assert_eq!(
        pre_purge.len(),
        1,
        "{place} drain sentinel is excluded and exactly one pre-purge event survives"
    );
    assert_eq!(
        pre_purge[0].row_values.get("id"),
        Some(&Value::Uuid(survivor)),
        "{place} surviving pre-purge event names the unrelated row"
    );
    assert_eq!(
        pre_purge[0].row_values.get("payload"),
        Some(&Value::Text("decoy-durable-payload".to_string())),
        "{place} callback captures the exact survivor payload"
    );
    assert!(
        pre_purge.iter().all(|event| {
            event.row_values.get("id") != Some(&Value::Uuid(selected))
                && event.row_values.values().all(|value| match value {
                    Value::Text(text) => !text.contains("selected-durable-payload"),
                    _ => true,
                })
        }),
        "{place} pre-purge callback set must never contain the selected id or payload"
    );
}

fn assert_no_export_attempt_files(directory: &Path, artifact: &Path) {
    let artifact_name = artifact
        .file_name()
        .expect("artifact has file name")
        .to_string_lossy();
    let attempt_prefix = format!("{artifact_name}.");
    let leftovers = std::fs::read_dir(directory)
        .expect("read export directory")
        .map(|entry| {
            entry
                .expect("read export directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| {
            // Only the attempt's own residue (`{artifact}.{uuid}.tmpexport` and
            // its `.lock`) is leftover; the artifact's persistent
            // `{artifact}.lock` companion is not an attempt file and must
            // never be flagged here.
            name.starts_with(&attempt_prefix)
                && (name.ends_with(".tmpexport") || name.ends_with(".tmpexport.lock"))
        })
        .collect::<Vec<_>>();
    assert!(
        leftovers.is_empty(),
        "export attempt must clean unpublished temporary artifacts and locks, found {leftovers:?}"
    );
}

fn assert_restored_purge(
    restored: &Database,
    selected: Uuid,
    survivor: Uuid,
    selected_row_id_before_purge: contextdb_core::RowId,
    survivor_row_id_before_purge: contextdb_core::RowId,
    removed_secret: &str,
    place: &str,
) {
    assert_absent(restored, selected, place);
    assert!(
        deletion_state(restored, selected).purge_frontier.is_some(),
        "{place} must retain the permanent purge frontier"
    );
    let changes = restored.changes_since(Lsn(0));
    assert!(
        !changes.rows.iter().any(|row| {
            row.values
                .values()
                .any(|value| value == &Value::Text(removed_secret.to_string()))
        }) && !changes
            .vectors
            .iter()
            .any(|vector| vector.row_id == selected_row_id_before_purge),
        "{place} must not restore selected row payload or vector history"
    );
    let vector_plan = restored
        .execute(
            "SELECT id FROM notes ORDER BY embedding <=> [1,0,0] LIMIT 10",
            &HashMap::new(),
        )
        .unwrap_or_else(|error| panic!("materialize {place} ANN/HNSW state: {error}"));
    assert!(
        vector_plan.trace.physical_plan.contains("HNSWSearch"),
        "{place} vector proof must exercise ANN/HNSW"
    );
    let hnsw_len = restored
        .__debug_vector_hnsw_len(VectorIndexRef::new("notes", "embedding"))
        .unwrap_or_else(|| panic!("{place} must materialize HNSW state"));
    assert_eq!(
        hnsw_len, 1000,
        "{place} HNSW graph must contain every survivor and no selected node"
    );
    let vectors = restored
        .query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[1.0, 0.0, 0.0],
            hnsw_len + 1,
            None,
            restored.snapshot(),
        )
        .unwrap_or_else(|error| panic!("inspect {place} vector state: {error}"));
    assert!(
        vectors
            .iter()
            .all(|(row_id, _)| *row_id != selected_row_id_before_purge),
        "{place} vector/ANN state must exclude the pre-purge selected RowId"
    );
    assert!(
        vectors
            .iter()
            .any(|(row_id, _)| *row_id == survivor_row_id_before_purge),
        "{place} vector/ANN state keeps the survivor RowId"
    );
    let selected_lookup = restored
        .execute(
            "SELECT id FROM notes WHERE body = $body",
            &HashMap::from([("body".to_string(), Value::Text(removed_secret.to_string()))]),
        )
        .unwrap_or_else(|error| panic!("inspect {place} selected index entry: {error}"));
    assert_eq!(
        selected_lookup.trace.index_used.as_deref(),
        Some("notes_body_idx"),
        "{place} selected absence proof must use the relational index"
    );
    assert!(
        selected_lookup.rows.is_empty(),
        "{place} relational index must not serve the selected payload"
    );
    let survivor_lookup = restored
        .execute(
            "SELECT id FROM notes WHERE body = 'backup-decoy-survives'",
            &HashMap::new(),
        )
        .unwrap_or_else(|error| panic!("inspect {place} survivor index entry: {error}"));
    assert_eq!(
        survivor_lookup.trace.index_used.as_deref(),
        Some("notes_body_idx"),
        "{place} survivor proof must use the relational index"
    );
    assert_eq!(
        survivor_lookup.rows,
        vec![vec![Value::Uuid(survivor)]],
        "{place} relational index keeps the survivor"
    );
}

#[tokio::test]
async fn offline_delete_survives_restart_reconnect_and_later_pulls() {
    const CHILD_DATABASE: &str = "CONTEXTDB_OFFLINE_DELETE_CHILD_DATABASE";
    const CHILD_ID: &str = "CONTEXTDB_OFFLINE_DELETE_CHILD_ID";

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE) {
        let id = std::env::var(CHILD_ID)
            .expect("process A receives the exact delete key")
            .parse::<Uuid>()
            .expect("process A delete key is a UUID");
        let edge = Database::open(PathBuf::from(database_path))
            .expect("process A opens the shared edge database");
        delete(&edge, id);
        return;
    }

    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "durable-offline-delete";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let edge_a_path = root.path().join("edge-a.db");
    let edge_a_identity = root.path().join("edge-a.db.fabric-identity.key");
    let edge_a = Arc::new(Database::open(&edge_a_path).expect("open file-backed edge A"));
    create_notes(&edge_a);
    create_keep_first_notes(&edge_a);
    let edge_a_node_id = FabricIdentity::load_or_generate(&edge_a_identity)
        .expect("persist edge A's explicit fabric identity")
        .node_id();
    let a = SyncClient::new(
        edge_a.clone(),
        &peer_dial_spec(&hub.ticket, &edge_a_identity),
        TenantId::from(tenant),
    );
    let (edge_b, b) = edge(root.path(), "edge-b", &hub.ticket, tenant);
    put(&edge_a, id, "forget-me");
    let seed_lsn = edge_a.current_lsn();
    within(a.push()).await.expect("seed hub");
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(
                &TenantId::from(tenant),
                &edge_a_node_id,
            )
            .expect("read seeded edge-A authenticated watermark"),
        Some(seed_lsn),
        "the seed push authenticates edge A as its explicit persisted identity"
    );
    within(b.pull_default()).await.expect("seed second edge");
    drop(a);
    drop(edge_a);
    let child = std::process::Command::new(
        std::env::current_exe().expect("locate durable-deletion integration-test binary"),
    )
    .arg("--exact")
    .arg("offline_delete_survives_restart_reconnect_and_later_pulls")
    .arg("--nocapture")
    .env(CHILD_DATABASE, &edge_a_path)
    .env(CHILD_ID, id.to_string())
    .status()
    .expect("start process A that commits the offline delete");
    assert!(
        child.success(),
        "process A must commit the offline delete and exit before process B reconnects: {child}"
    );
    let edge_a = Arc::new(Database::open(&edge_a_path).expect("process B opens edge database"));
    create_notes(&edge_a);
    create_keep_first_notes(&edge_a);
    assert_eq!(
        FabricIdentity::load_or_generate(&edge_a_identity)
            .expect("process B reloads edge A's identity")
            .node_id(),
        edge_a_node_id,
        "process B uses the exact persisted identity process A inherited"
    );
    let a = SyncClient::new(
        edge_a.clone(),
        &peer_dial_spec(&hub.ticket, &edge_a_identity),
        TenantId::from(tenant),
    );
    assert_eq!(
        deletion_state(&edge_a, id).delete_obligation.as_deref(),
        Some("pending"),
        "process B sees process A's committed offline delete as durably owed before reconnect"
    );
    let process_b_lsn = edge_a.current_lsn();
    within(a.push())
        .await
        .expect("restart must offer owed delete");
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(
                &TenantId::from(tenant),
                &edge_a_node_id,
            )
            .expect("read process-B authenticated edge watermark"),
        Some(process_b_lsn),
        "the hub authenticates process B as the same persisted edge identity"
    );
    assert_eq!(
        deletion_state(&edge_a, id)
            .accepted_delete_marker
            .as_deref(),
        Some("accepted"),
        "hub acceptance becomes a durable replay marker after restart"
    );
    assert_eq!(
        deletion_state(&edge_a, id).delete_obligation,
        None,
        "accepted delete is no longer pending for resend"
    );
    within(b.pull_default())
        .await
        .expect("second edge receives delete");
    within(a.pull_default())
        .await
        .expect("later pull cannot resurrect delete");
    for (place, db) in [
        ("hub", &hub.db),
        ("reopened deleting edge", &edge_a),
        ("second edge", &edge_b),
    ] {
        assert_absent(db, id, place);
    }
    drop(a);
    drop(edge_a);
    let edge_a = Arc::new(Database::open(&edge_a_path).expect("second process-B reopen"));
    create_notes(&edge_a);
    create_keep_first_notes(&edge_a);
    let a = SyncClient::new(
        edge_a.clone(),
        &peer_dial_spec(&hub.ticket, &edge_a_identity),
        TenantId::from(tenant),
    );
    assert_eq!(
        deletion_state(&edge_a, id)
            .accepted_delete_marker
            .as_deref(),
        Some("accepted"),
        "accepted-delete replay evidence survives a second reopen"
    );
    within(a.pull_default())
        .await
        .expect("second reopen performs a real authenticated pull");
    assert_absent(&edge_a, id, "second reopened deleting edge");
    hub.stop().await;
}

#[tokio::test]
async fn delete_obligation_transitions_through_hub_outcomes() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "delete-obligation-outcomes";
    let hub = start_hub(root.path(), tenant).await;
    let accepted = Uuid::new_v4();
    let refused = Uuid::new_v4();
    let (accepted_edge, client) = edge(root.path(), "edge", &hub.ticket, tenant);
    put(&accepted_edge, accepted, "accepted-delete");
    within(client.push()).await.expect("seed accepted case");
    let alternate_root = tempfile::tempdir().expect("alternate hub root");
    let alternate = start_hub(alternate_root.path(), tenant).await;
    client
        .change_destination(&alternate.node_id)
        .expect("operator configures the authenticated alternate upstream");
    let alternate_seed = SyncClient::new(
        accepted_edge.clone(),
        &alternate.ticket,
        TenantId::from(tenant),
    );
    within(alternate_seed.push())
        .await
        .expect("alternate hub receives the same original lineage before deletion");
    assert_eq!(
        body(&alternate.db, accepted).as_deref(),
        Some("accepted-delete"),
        "stale alternate has the original lineage before the authoritative delete"
    );
    alternate_seed
        .change_destination(&hub.node_id)
        .expect("operator returns the edge to the authoritative hub before deleting");
    drop(alternate_seed);
    drop(client);
    let client = SyncClient::new(accepted_edge.clone(), &hub.ticket, TenantId::from(tenant));
    delete(&accepted_edge, accepted);
    assert_absent(
        &accepted_edge,
        accepted,
        "accepted edge hides delete before restart",
    );
    assert_eq!(
        deletion_state(&accepted_edge, accepted)
            .delete_obligation
            .as_deref(),
        Some("pending"),
        "accepted case records a pending delete before restart"
    );
    drop(client);
    drop(accepted_edge);
    let (accepted_edge, client) = reopen_edge(root.path(), "edge", &hub.ticket, tenant);
    assert_absent(
        &accepted_edge,
        accepted,
        "reopened accepted edge hides durable pending delete before replay",
    );
    assert_eq!(
        deletion_state(&accepted_edge, accepted)
            .delete_obligation
            .as_deref(),
        Some("pending"),
        "accepted case pending obligation is durable across reopen"
    );
    within(client.push())
        .await
        .expect("offer durable accepted delete");
    assert_eq!(
        deletion_state(&accepted_edge, accepted)
            .accepted_delete_marker
            .as_deref(),
        Some("accepted"),
        "accepted branch persists its marker after reopen"
    );
    assert_absent(&hub.db, accepted, "hub after accepted delete");
    assert_absent(&accepted_edge, accepted, "edge after accepted delete");

    client
        .change_destination(&alternate.node_id)
        .expect("operator changes upstream destination after accepted delete");
    assert_eq!(
        accepted_edge.retention_sync_peer().as_deref(),
        Some(alternate.node_id.as_str()),
        "destination change is durable, rather than an in-memory client preference"
    );
    drop(client);
    drop(accepted_edge);
    let (accepted_edge, alternate_client) =
        reopen_edge(root.path(), "edge", &alternate.ticket, tenant);
    assert_eq!(
        accepted_edge.retention_sync_peer().as_deref(),
        Some(alternate.node_id.as_str()),
        "changed upstream survives reopen"
    );
    assert_eq!(
        deletion_state(&accepted_edge, accepted)
            .accepted_delete_marker
            .as_deref(),
        Some("accepted"),
        "changing upstream does not erase accepted-delete replay evidence"
    );
    within(alternate_client.pull_default())
        .await
        .expect("stale alternate hub serves its pre-delete lineage over real Iroh");
    assert_absent(
        &accepted_edge,
        accepted,
        "accepted marker refuses stale alternate-hub replay",
    );
    alternate_client
        .change_destination(&hub.node_id)
        .expect("operator restores the original upstream destination");
    drop(alternate_client);
    drop(accepted_edge);
    let (accepted_edge, client) = reopen_edge(root.path(), "edge", &hub.ticket, tenant);
    assert_eq!(
        accepted_edge.retention_sync_peer().as_deref(),
        Some(hub.node_id.as_str()),
        "restored upstream identity survives its own reopen"
    );
    put(&hub.db, accepted, "later-authoritative-same-key-write");
    within(client.pull_default())
        .await
        .expect("pull later authoritative same-key write");
    assert_eq!(
        body(&accepted_edge, accepted).as_deref(),
        Some("later-authoritative-same-key-write"),
        "an accepted delete marker rejects old replay but does not ban a later authoritative write"
    );

    let (winner, winner_client) = edge(root.path(), "winner", &hub.ticket, tenant);
    put_keep_first(&winner, refused, "keep-first-winner");
    let winner_push = within(winner_client.push())
        .await
        .expect("hub accepts keep-first winner");
    let winner_position = winner_push.new_lsn.0;
    assert!(winner_position > 0, "winner acceptance position is nonzero");
    let winner_node_id =
        FabricIdentity::load_or_generate(&root.path().join("winner.db.fabric-identity.key"))
            .expect("load persisted winner identity")
            .node_id();
    let (deleting, deleting_client) = edge(root.path(), "deleting", &hub.ticket, tenant);
    within(deleting_client.pull_default())
        .await
        .expect("deleting edge receives current keep-first winner");
    delete_keep_first(&deleting, refused);
    assert_eq!(
        keep_first_deletion_state(&deleting, refused)
            .delete_obligation
            .as_deref(),
        Some("pending"),
        "refused case also persists the delete before adjudication"
    );
    drop(deleting_client);
    drop(deleting);
    let (deleting, deleting_client) = reopen_edge(root.path(), "deleting", &hub.ticket, tenant);
    assert_eq!(
        keep_first_deletion_state(&deleting, refused)
            .delete_obligation
            .as_deref(),
        Some("pending"),
        "refused case remains pending after restart until the hub adjudicates it"
    );
    let refusal = within(deleting_client.push())
        .await
        .expect("hub returns a typed keep-first refusal");
    let receipt = exact_delete_refusal(&refusal, refused, &winner_node_id, winner_position);
    assert_eq!(
        keep_first_deletion_state(&deleting, refused).delete_obligation,
        None,
        "refusal retires resend instead of keeping the delete pending forever"
    );
    assert_eq!(
        keep_first_deletion_state(&deleting, refused).accepted_delete_marker,
        None,
        "a refused delete is not mislabeled as an accepted marker"
    );
    let receipts_before_no_resend = deleting_client.transfer_receipts();
    let no_resend = within(deleting_client.push())
        .await
        .expect("retired refusal is a bounded no-op rather than another transmission");
    assert_eq!(
        no_resend.applied_rows, 0,
        "retired delete reapplies no rows"
    );
    assert_eq!(
        no_resend.skipped_rows, 0,
        "retired delete has no second refusal"
    );
    assert!(
        no_resend.conflicts.is_empty(),
        "retired delete emits no duplicate conflict"
    );
    assert_eq!(
        deleting_client.transfer_receipts(),
        receipts_before_no_resend,
        "retired delete sends no second authenticated sync payload"
    );
    assert_eq!(
        keep_first_body(&hub.db, refused).as_deref(),
        Some("keep-first-winner"),
        "no-resend drive cannot alter the hub winner"
    );
    within(deleting_client.pull_default())
        .await
        .expect("refused deleting edge converges on hub winner");
    assert_eq!(
        keep_first_body(&deleting, refused).as_deref(),
        Some("keep-first-winner"),
        "visible refusal converges rather than silently resurrecting"
    );
    assert_eq!(receipt["mutation_kind"], "delete");
    drop(client);
    drop(accepted_edge);
    drop(deleting_client);
    drop(deleting);
    let (deleting, _) = reopen_edge(root.path(), "deleting", &hub.ticket, tenant);
    assert_eq!(
        keep_first_deletion_state(&deleting, refused).delete_obligation,
        None,
        "refusal remains terminal after another reopen"
    );
    assert_eq!(
        keep_first_body(&deleting, refused).as_deref(),
        Some("keep-first-winner")
    );
    alternate.stop().await;
    hub.stop().await;
}

#[tokio::test]
async fn hub_restart_preserves_winner_provenance_and_position() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "delete-winner-restart";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let hub_node_id = hub.node_id.clone();
    let (winner, winner_client) = edge(root.path(), "winner", &hub.ticket, tenant);
    put_keep_first(&winner, id, "hub-winner");
    let winner_push = within(winner_client.push()).await.expect("write winner");
    let winner_position = winner_push.new_lsn.0;
    assert!(winner_position > 0, "winner acceptance position is nonzero");
    let winner_node_id =
        FabricIdentity::load_or_generate(&root.path().join("winner.db.fabric-identity.key"))
            .expect("load winner identity")
            .node_id();
    let (loser, loser_client) = edge(root.path(), "loser", &hub.ticket, tenant);
    within(loser_client.pull_default())
        .await
        .expect("loser receives hub winner before delete");
    delete_keep_first(&loser, id);
    let before_restart = within(loser_client.push())
        .await
        .expect("first delete receives refusal");
    let before_receipt =
        exact_delete_refusal(&before_restart, id, &winner_node_id, winner_position);
    within(loser_client.pull_default())
        .await
        .expect("first refusal converges loser");
    drop(loser_client);
    drop(loser);
    hub.stop().await;
    let restarted = start_hub(root.path(), tenant).await;
    assert_eq!(
        restarted.node_id, hub_node_id,
        "the same file-backed hub keeps the exact authenticated node identity across restart"
    );
    assert_eq!(
        keep_first_body(&restarted.db, id).as_deref(),
        Some("hub-winner")
    );
    let (loser, loser_client) = edge(root.path(), "loser", &restarted.ticket, tenant);
    delete_keep_first(&loser, id);
    let after_restart = within(loser_client.push())
        .await
        .expect("second delete receives persisted refusal");
    let after_receipt = exact_delete_refusal(&after_restart, id, &winner_node_id, winner_position);
    assert_eq!(
        after_receipt["winning_author_node_id"], before_receipt["winning_author_node_id"],
        "restart preserves the stored winner author, not the current sender or hub label"
    );
    assert_eq!(
        after_receipt["hub_acceptance_position"], before_receipt["hub_acceptance_position"],
        "restart preserves the exact original hub acceptance position"
    );
    restarted.stop().await;
}

#[tokio::test]
async fn authoritative_purge_removes_every_engine_held_copy() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "purge-copy-classes";
    let hub = start_hub(root.path(), tenant).await;
    let selected = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    let (edge_a, a, edge_a_identity) = copy_test_edge(root.path(), "edge-a", &hub.ticket, tenant);
    let (edge_b, b, edge_b_identity) = copy_test_edge(root.path(), "edge-b", &hub.ticket, tenant);
    for db in [&hub.db, &edge_a, &edge_b] {
        create_purge_copy_tables(db);
    }

    put(&edge_a, selected, "selected-secret-before-edit");
    put(&edge_a, survivor, "decoy-survives");
    for ordinal in 2..=1000_u128 {
        put(
            &edge_a,
            Uuid::from_u128(0x7100_0000_0000_0000_0000_0000_0000_0000 + ordinal),
            &format!("vector-decoy-{ordinal}"),
        );
    }
    let held_snapshot = edge_a.snapshot();
    let _open_read = edge_a.pin_snapshot(held_snapshot);
    edit(&edge_a, selected, "selected-secret-after-edit");
    assert_eq!(
        edge_a
            .execute_at_snapshot(
                "SELECT body FROM notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
                held_snapshot,
            )
            .expect("read superseded selected version through open snapshot")
            .rows,
        vec![vec![Value::Text("selected-secret-before-edit".to_string())]],
        "fixture must hold and inspect the selected superseded version"
    );

    let selected_row_id = row_id(&edge_a, selected);
    let vector_query = edge_a
        .execute(
            "SELECT id FROM notes ORDER BY embedding <=> [1,0,0] LIMIT 10",
            &HashMap::new(),
        )
        .expect("materialize production vector search");
    assert!(
        vector_query.trace.physical_plan.contains("HNSWSearch")
            && edge_a
                .__debug_vector_hnsw_len(VectorIndexRef::new("notes", "embedding"))
                .is_some(),
        "fixture must hold the selected vector in a materialized ANN/HNSW graph"
    );
    assert_eq!(
        edge_a
            .execute(
                "SELECT id FROM notes WHERE body = 'selected-secret-after-edit'",
                &HashMap::new(),
            )
            .expect("probe secondary relational index")
            .rows,
        vec![vec![Value::Uuid(selected)]],
        "secondary-index lookup must expose the selected row before purge"
    );

    let selected_target = Uuid::new_v4();
    let survivor_source = Uuid::new_v4();
    let survivor_target = Uuid::new_v4();
    insert_graph_edge(&edge_a, selected, selected, selected_target);
    insert_graph_edge(&edge_a, survivor, survivor_source, survivor_target);
    assert_eq!(
        graph_targets(&edge_a, selected),
        BTreeSet::from([selected_target]),
        "graph adjacency must expose the selected edge before purge"
    );

    // These are copies of one selected lineage, not three independently
    // authored same-key records. Seed them at edge A and let the authenticated
    // baseline push/pull below materialize the hub and edge-B copies.
    insert_notification(&edge_a, selected, "selected-durable-payload");
    insert_notification(&edge_a, survivor, "decoy-durable-payload");

    let hub_identity = root.path().join("hub.db.fabric-identity.key");
    let holder_node = FabricIdentity::load_or_generate(&edge_a_identity)
        .expect("edge A identity")
        .node_id();
    let consumer_node = FabricIdentity::load_or_generate(&edge_b_identity)
        .expect("edge B identity")
        .node_id();
    let blob_bytes = vec![0x5a; 8 * 1024 * 1024];
    let blob_hash = BlobHash::of(&blob_bytes);
    let blob_job_id = selected.to_string();
    submit_blob_job(
        &edge_a,
        &blob_job_id,
        &holder_node,
        &consumer_node,
        &blob_hash,
    );
    let survivor_job_id = format!("survivor-{survivor}");
    let survivor_job = JobSpec::builder(&survivor_job_id, "media.purge", "batch", &holder_node)
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job(&edge_a, &survivor_job, &[] as &[&[u8]]).expect("submit unrelated work-ledger job");

    within_copy_class_replication(a.push())
        .await
        .expect("seed selected and decoy");
    within_copy_class_replication(b.pull_default())
        .await
        .expect("replicate copy classes");
    match insert_claim(
        &edge_b,
        &blob_job_id,
        1,
        &consumer_node,
        1_700_000_300_000,
        1_700_000_000_000,
    )
    .expect("seed edge B live local blob claim")
    {
        ClaimInsert::Inserted => {}
        ClaimInsert::AlreadyHeld { holder } => assert_eq!(
            holder, consumer_node,
            "an existing edge B attempt-1 claim must name the exact consumer identity"
        ),
    }
    let mut copy_holders = Vec::new();
    for (place, db) in [("hub", &hub.db), ("edge a", &edge_a), ("edge b", &edge_b)] {
        let local_selected_row_id = row_id(db, selected);
        let local_survivor_row_id = row_id(db, survivor);
        let search = db
            .execute(
                "SELECT id FROM notes ORDER BY embedding <=> [1,0,0] LIMIT 10",
                &HashMap::new(),
            )
            .unwrap_or_else(|error| panic!("materialize {place} ANN graph: {error}"));
        let hnsw_len = db
            .__debug_vector_hnsw_len(VectorIndexRef::new("notes", "embedding"))
            .unwrap_or_else(|| panic!("{place} must materialize an ANN/HNSW graph"));
        assert!(
            search.trace.physical_plan.contains("HNSWSearch") && hnsw_len > 0,
            "{place} must hold a materialized ANN/HNSW copy before purge"
        );
        let vectors = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                1000,
                None,
                db.snapshot(),
            )
            .unwrap_or_else(|error| panic!("inspect {place} vector index before purge: {error}"));
        assert!(
            vectors
                .iter()
                .any(|(row_id, _)| *row_id == local_selected_row_id),
            "{place} vector index must contain its exact local selected RowId before purge"
        );
        let indexed = db
            .execute(
                "SELECT id FROM notes WHERE body = 'selected-secret-after-edit'",
                &HashMap::new(),
            )
            .unwrap_or_else(|error| panic!("read {place} secondary index: {error}"));
        assert_eq!(
            indexed.rows,
            vec![vec![Value::Uuid(selected)]],
            "{place} secondary index must serve the selected row before purge"
        );
        assert_eq!(
            indexed.trace.index_used.as_deref(),
            Some("notes_body_idx"),
            "{place} proof must read the production secondary index"
        );
        assert_eq!(
            graph_targets(db, selected),
            BTreeSet::from([selected_target]),
            "{place} graph index must serve selected adjacency before purge"
        );
        assert_eq!(
            work_job_ids(db, &selected.to_string()),
            vec![selected.to_string()],
            "{place} work ledger must hold the selected blob referent before purge"
        );
        assert_eq!(
            work_job_ids(db, &survivor_job_id),
            vec![survivor_job_id.clone()],
            "{place} work ledger must hold the unrelated job before purge"
        );
        let queued_rows = db
            .execute(
                "SELECT id FROM purge_notifications WHERE id IN ($selected, $survivor) ORDER BY id",
                &HashMap::from([
                    ("selected".to_string(), Value::Uuid(selected)),
                    ("survivor".to_string(), Value::Uuid(survivor)),
                ]),
            )
            .unwrap_or_else(|error| panic!("inspect {place} sink source rows: {error}"));
        assert_eq!(
            queued_rows.rows.len(),
            2,
            "{place} must enqueue selected and survivor events before callback registration"
        );
        copy_holders.push((
            place,
            db,
            local_selected_row_id,
            local_survivor_row_id,
            hnsw_len,
        ));
    }
    within(a.shutdown()).await;
    within(b.shutdown()).await;
    assert!(
        !a.is_connected().await && !b.is_connected().await,
        "copy-test sync endpoints must release their participant identities before media binding"
    );

    let holder = BlobStore::new(
        hub.db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        hub_identity,
    );
    holder.set_test_clock(1_700_000_000_000);
    assert_eq!(
        holder
            .ingest_bytes(&blob_bytes)
            .expect("ingest unshared blob"),
        blob_hash
    );
    assert!(
        holder
            .exact_hash_state_for_test()
            .contains(&blob_hash.as_bytes()),
        "holder must contain the exact unshared blob before purge"
    );
    let edge_a_blob = BlobStore::new(
        edge_a.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        edge_a_identity.clone(),
    );
    edge_a_blob.set_test_clock(1_700_000_000_000);
    edge_a_blob
        .ingest_bytes(&blob_bytes)
        .expect("materialize edge A held blob through production ingest");
    let edge_b_blob = BlobStore::new(
        edge_b.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        edge_b_identity.clone(),
    );
    for (place, store) in [("hub", &holder), ("edge a", &edge_a_blob)] {
        assert_eq!(
            store.exact_hash_state_for_test(),
            BTreeSet::from([blob_hash.as_bytes()]),
            "{place} must hold the exact selected blob before purge"
        );
    }
    assert!(
        any_node_holds_claim_for_blob(&edge_b, &blob_hash, 1_700_000_000_000)
            .expect("inspect edge B local blob entitlement"),
        "edge B must hold a live local claim for the exact blob before fetch"
    );
    assert!(
        node_holds_claim_for_blob(&edge_a, &consumer_node, &blob_hash, 1_700_000_000_000,)
            .expect("inspect edge A holder-side blob entitlement"),
        "edge A must recognize the exact consumer identity's live blob claim"
    );
    assert!(
        node_claim_permits_movement(
            &edge_a,
            &consumer_node,
            &blob_hash,
            MovementPolicy {
                auto_propagate: true,
            },
            1_700_000_000_000,
        )
        .expect("inspect edge A holder-side movement permission"),
        "edge A must permit the claimed blob to move to the exact consumer identity"
    );
    edge_a_blob.drop_after_bytes_for_test(5 * 1024 * 1024);
    let blob_endpoint = within(IrohServer::bind(&spec(&edge_a_identity)))
        .await
        .expect("bind blob holder");
    assert_eq!(
        blob_endpoint.node_id(),
        holder_node,
        "the serving endpoint must use edge A's one persisted participant identity"
    );
    edge_a_blob.serve_on(&blob_endpoint);
    edge_b_blob.set_test_clock(1_700_000_000_000);
    let mut partial = Vec::new();
    let interrupted =
        within(edge_b_blob.resolve_blob_ref(&blob_hash, &blob_endpoint.ticket(), &mut partial))
            .await;
    assert!(
        matches!(interrupted, Err(ResolveError::TransferAborted)),
        "fixture must create a typed interrupted transfer, got {interrupted:?}"
    );
    assert!(
        !partial.is_empty() && partial.len() < blob_bytes.len(),
        "fixture must create a genuine partial fetch"
    );
    assert_eq!(
        edge_b_blob.fetch_tag_count_for_test(),
        1,
        "interrupted fetch must be engine-protected before purge"
    );
    edge_b_blob
        .ingest_bytes(&blob_bytes)
        .expect("materialize edge B held blob through production ingest");
    assert_eq!(
        edge_b_blob.exact_hash_state_for_test(),
        BTreeSet::from([blob_hash.as_bytes()]),
        "edge B must hold the exact selected blob after the genuine partial fetch"
    );
    assert_eq!(
        edge_b_blob.fetch_tag_count_for_test(),
        1,
        "materializing the complete edge B copy must not erase proof of the interrupted fetch"
    );
    blob_endpoint.close().await;

    // The media endpoint and the original sync clients are explicitly closed
    // above. Reopen the same databases with the same persisted identities so
    // the purge has to reach a restarted, already-holding copy class.
    let a = SyncClient::new(
        edge_a.clone(),
        &peer_dial_spec(&hub.ticket, &edge_a_identity),
        TenantId::from(tenant),
    );
    let b = SyncClient::new(
        edge_b.clone(),
        &peer_dial_spec(&hub.ticket, &edge_b_identity),
        TenantId::from(tenant),
    );

    assert!(
        change_set_contains(
            &edge_a,
            selected,
            selected_row_id,
            "selected-secret-before-edit"
        ) && change_set_contains(
            &edge_a,
            selected,
            selected_row_id,
            "selected-secret-after-edit"
        ),
        "change serving must expose both selected versions before purge"
    );
    let edge_a_delta =
        serde_json::to_string(&edge_a.changes_since(Lsn(0))).expect("serialize pre-purge delta");
    assert!(
        edge_a_delta.contains(&blob_hash.to_hex())
            && edge_a_delta.contains("selected-durable-payload"),
        "sync-serving history must hold the selected blob and durable-sink payload before purge"
    );

    purge_exact(&hub.db, "notes", "id", Value::Uuid(selected));
    purge_exact(&hub.db, "edges", "id", Value::Uuid(selected));
    purge_exact(&hub.db, "nodes", "id", Value::Uuid(selected));
    purge_exact(&hub.db, "purge_notifications", "id", Value::Uuid(selected));
    purge_exact(&hub.db, "work_jobs", "job_id", Value::Text(blob_job_id));
    within_copy_class_replication(a.pull_default())
        .await
        .expect("edge a applies authoritative purge");
    within_copy_class_replication(b.pull_default())
        .await
        .expect("edge b applies authoritative purge");
    for (place, db, local_selected_row_id, local_survivor_row_id, hnsw_len_before) in &copy_holders
    {
        assert_absent(db, selected, place);
        assert_eq!(
            body(db, survivor).as_deref(),
            Some("decoy-survives"),
            "{place} keeps unrelated data"
        );
        assert!(
            !change_set_contains(
                db,
                selected,
                *local_selected_row_id,
                "selected-secret-before-edit"
            ) && !change_set_contains(
                db,
                selected,
                *local_selected_row_id,
                "selected-secret-after-edit"
            ),
            "{place} must not serve the selected lineage as a sync delta"
        );
        let delta =
            serde_json::to_string(&db.changes_since(Lsn(0))).expect("serialize post-purge delta");
        assert!(
            !delta.contains("selected-secret-before-edit")
                && !delta.contains("selected-secret-after-edit")
                && !delta.contains("selected-durable-payload")
                && !delta.contains(&blob_hash.to_hex()),
            "{place} sync history must retain the purge frontier while dropping row, sink, and blob payload"
        );
        assert!(
            deletion_state(db, selected).purge_frontier.is_some(),
            "{place} keeps the permanent opaque purge frontier needed to refuse stale replay"
        );
        let vector_rows = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                1000,
                None,
                db.snapshot(),
            )
            .unwrap_or_else(|error| panic!("inspect {place} vector index after purge: {error}"));
        assert!(
            vector_rows
                .iter()
                .all(|(row_id, _)| row_id != local_selected_row_id),
            "{place} vector and ANN/HNSW state must not return the selected entry"
        );
        assert!(
            vector_rows
                .iter()
                .any(|(row_id, _)| row_id == local_survivor_row_id),
            "{place} vector index keeps the unrelated decoy entry"
        );
        let rebuilt_search = db
            .execute(
                "SELECT id FROM notes ORDER BY embedding <=> [1,0,0] LIMIT 10",
                &HashMap::new(),
            )
            .unwrap_or_else(|error| panic!("rebuild {place} ANN graph after purge: {error}"));
        assert!(
            rebuilt_search.trace.physical_plan.contains("HNSWSearch"),
            "{place} must rematerialize the production HNSW path after purge"
        );
        assert_eq!(
            db.__debug_vector_hnsw_len(VectorIndexRef::new("notes", "embedding")),
            Some(*hnsw_len_before - 1),
            "{place} HNSW graph must lose exactly the selected node"
        );
        let indexed_absence = db
            .execute(
                "SELECT id FROM notes WHERE body = 'selected-secret-after-edit'",
                &HashMap::new(),
            )
            .unwrap_or_else(|error| panic!("inspect {place} secondary index after purge: {error}"));
        assert_eq!(
            indexed_absence.rows,
            Vec::<Vec<Value>>::new(),
            "{place} secondary relational index must not serve the selected row"
        );
        assert_eq!(
            indexed_absence.trace.index_used.as_deref(),
            Some("notes_body_idx"),
            "{place} absence proof must read the production secondary index"
        );
        let indexed_decoy = db
            .execute(
                "SELECT id FROM notes WHERE body = 'decoy-survives'",
                &HashMap::new(),
            )
            .unwrap_or_else(|error| panic!("read {place} indexed decoy after purge: {error}"));
        assert_eq!(
            indexed_decoy.rows,
            vec![vec![Value::Uuid(survivor)]],
            "{place} secondary index keeps the unrelated decoy"
        );
        assert_eq!(
            indexed_decoy.trace.index_used.as_deref(),
            Some("notes_body_idx"),
            "{place} decoy proof must read the production secondary index"
        );
        assert_eq!(
            graph_targets(db, selected),
            BTreeSet::new(),
            "{place} graph adjacency must exclude the selected edge"
        );
        assert_eq!(
            graph_targets(db, survivor_source),
            BTreeSet::from([survivor_target]),
            "{place} graph adjacency keeps the unrelated edge"
        );
        assert!(
            work_job_ids(db, &selected.to_string()).is_empty(),
            "{place} work ledger must not retain the selected blob referent"
        );
        assert_eq!(
            work_job_ids(db, &survivor_job_id),
            vec![survivor_job_id.clone()],
            "{place} work ledger keeps the unrelated job"
        );
    }
    if let Ok(held_read) = edge_a.execute_at_snapshot(
        "SELECT body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        held_snapshot,
    ) {
        assert_eq!(
            held_read.rows,
            Vec::<Vec<Value>>::new(),
            "a still-readable pre-purge snapshot must not expose the removed lineage"
        );
    }
    for (place, store) in [
        ("hub", &holder),
        ("edge a", &edge_a_blob),
        ("edge b", &edge_b_blob),
    ] {
        assert_eq!(
            store.exact_hash_state_for_test(),
            BTreeSet::new(),
            "{place} selected blob bytes must be destroyed by purge"
        );
    }
    assert_eq!(
        edge_b_blob.fetch_tag_count_for_test(),
        0,
        "purge must release protection for the selected partial fetch"
    );

    for (place, db) in [("hub", &hub.db), ("edge a", &edge_a), ("edge b", &edge_b)] {
        assert_production_sink_delivers_only_survivor(db, selected, survivor, place);
    }
    within(a.shutdown()).await;
    within(b.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn concurrent_pre_purge_transaction_cannot_commit_removed_lineage() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "purge-open-transaction";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    put(&hub.db, id, "lineage-before-fence");
    put(&hub.db, survivor, "control-before-fence");
    let tx = hub.db.begin().expect("start pre-purge transaction");
    let control_tx = hub.db.begin().expect("start unpurged control transaction");
    hub.db
        .execute_in_tx(
            tx,
            "UPDATE notes SET body = 'staged-before-purge' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("stage descendant edit");
    hub.db
        .execute_in_tx(
            control_tx,
            "UPDATE notes SET body = 'control-committed' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(survivor))]),
        )
        .expect("stage unpurged control edit");
    purge(&hub.db, id);
    let _typed_refusal: Error = hub
        .db
        .commit(tx)
        .expect_err("pre-purge descendant commit must be refused");
    hub.db
        .commit(control_tx)
        .unwrap_or_else(|error| panic!("unpurged control transaction must commit: {error}"));
    let _ = hub.db.rollback(tx);
    assert_absent(&hub.db, id, "hub after fenced transaction");
    assert_eq!(
        body(&hub.db, survivor).as_deref(),
        Some("control-committed"),
        "the same purge boundary refuses only the transaction carrying the removed lineage"
    );
    hub.stop().await;
}

#[tokio::test]
async fn configured_edge_refuses_purge_before_first_connection_and_after_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "edge-purge-authority";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let (edge, configured_client, edge_identity) =
        copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the edge fabric identity before configuration");
    put(&edge, id, "edge-owned-before-connect");
    let before = note_keys(&edge);
    let before_state = deletion_state(&edge, id);
    let before_lsn = edge.current_lsn();
    let tenant_id = TenantId::from(tenant);
    let before_progress = (
        edge.persisted_sync_watermarks(&tenant_id)
            .expect("read edge watermarks before first refusal"),
        edge.persisted_sync_pending_push_confirmation(&tenant_id)
            .expect("read edge pending confirmation before first refusal"),
        edge.persisted_sync_pull_cursor(&tenant_id)
            .expect("read edge pull cursor before first refusal"),
        edge.persisted_sync_applied_push_watermark(&tenant_id)
            .expect("read edge applied-push watermark before first refusal"),
    );
    let refused = edge
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect_err("configured edge refuses local purge before first connection");
    assert!(
        !matches!(
            &refused,
            Error::ParseError(_) | Error::PlanError(_) | Error::SyncError(_) | Error::Other(_)
        ),
        "configured-edge refusal must be a typed authority error: {refused}"
    );
    let diagnostic = refused.to_string();
    let meaning = diagnostic.to_ascii_lowercase();
    assert!(
        meaning.contains("purge") && meaning.contains("authorit"),
        "configured-edge refusal must identify PURGE authority: {diagnostic}"
    );
    assert!(
        diagnostic.contains(&hub.node_id),
        "configured-edge refusal must identify the durably bound authoritative hub: {refused}"
    );
    assert_eq!(
        note_keys(&edge),
        before,
        "refusal mutates no local row before connection"
    );
    assert_eq!(
        deletion_state(&edge, id),
        before_state,
        "refusal changes no durable deletion state"
    );
    assert_eq!(
        edge.current_lsn(),
        before_lsn,
        "refusal writes no local row commit before connection"
    );
    assert_eq!(
        (
            edge.persisted_sync_watermarks(&tenant_id)
                .expect("read edge watermarks after first refusal"),
            edge.persisted_sync_pending_push_confirmation(&tenant_id)
                .expect("read edge pending confirmation after first refusal"),
            edge.persisted_sync_pull_cursor(&tenant_id)
                .expect("read edge pull cursor after first refusal"),
            edge.persisted_sync_applied_push_watermark(&tenant_id)
                .expect("read edge applied-push watermark after first refusal"),
        ),
        before_progress,
        "refusal writes no cursor, receipt, or watermark state before connection"
    );
    drop(configured_client);
    drop(edge);
    let (edge, configured_client, restarted_identity) =
        copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    assert_eq!(
        restarted_identity, edge_identity,
        "restart must reuse the edge's existing adjacent fabric identity"
    );
    let restart_progress = (
        edge.persisted_sync_watermarks(&tenant_id)
            .expect("read edge watermarks after restart"),
        edge.persisted_sync_pending_push_confirmation(&tenant_id)
            .expect("read edge pending confirmation after restart"),
        edge.persisted_sync_pull_cursor(&tenant_id)
            .expect("read edge pull cursor after restart"),
        edge.persisted_sync_applied_push_watermark(&tenant_id)
            .expect("read edge applied-push watermark after restart"),
    );
    assert_eq!(
        restart_progress, before_progress,
        "restart preserves the configured edge's untouched progress state"
    );
    let refused = edge
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect_err("configured edge still refuses purge after restart");
    assert!(
        !matches!(
            &refused,
            Error::ParseError(_) | Error::PlanError(_) | Error::SyncError(_) | Error::Other(_)
        ),
        "restarted configured-edge refusal must be a typed authority error: {refused}"
    );
    let diagnostic = refused.to_string();
    let meaning = diagnostic.to_ascii_lowercase();
    assert!(
        meaning.contains("purge") && meaning.contains("authorit"),
        "restarted configured-edge refusal must identify PURGE authority: {diagnostic}"
    );
    assert!(
        diagnostic.contains(&hub.node_id),
        "restarted configured-edge refusal must identify the durably bound authoritative hub: {refused}"
    );
    assert_eq!(
        note_keys(&edge),
        before,
        "restart refusal mutates no local row"
    );
    assert_eq!(
        deletion_state(&edge, id),
        before_state,
        "restart refusal changes no durable state"
    );
    assert_eq!(
        edge.current_lsn(),
        before_lsn,
        "restart refusal writes no local row commit"
    );
    assert_eq!(
        (
            edge.persisted_sync_watermarks(&tenant_id)
                .expect("read edge watermarks after restart refusal"),
            edge.persisted_sync_pending_push_confirmation(&tenant_id)
                .expect("read edge pending confirmation after restart refusal"),
            edge.persisted_sync_pull_cursor(&tenant_id)
                .expect("read edge pull cursor after restart refusal"),
            edge.persisted_sync_applied_push_watermark(&tenant_id)
                .expect("read edge applied-push watermark after restart refusal"),
        ),
        restart_progress,
        "restart refusal writes no cursor, receipt, or watermark state"
    );
    drop(configured_client);
    drop(edge);
    hub.stop().await;
}

#[tokio::test]
async fn wrong_bound_hub_pull_is_refused_before_mutation() {
    let root = tempfile::tempdir().expect("tempdir");
    let good = start_hub(root.path(), "bound-hub").await;
    let wrong_root = tempfile::tempdir().expect("wrong hub root");
    let wrong = start_hub(wrong_root.path(), "bound-hub").await;
    let id = Uuid::new_v4();
    let (edge, good_client, edge_identity) =
        copy_test_edge(root.path(), "edge", &good.ticket, "bound-hub");
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the edge fabric identity before binding its hub");
    put(&edge, id, "bound-hub-data");
    within(good_client.push())
        .await
        .expect("establish bound hub");
    within(good_client.shutdown()).await;
    drop(good_client);
    let wrong_client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&wrong.ticket, &edge_identity),
        TenantId::from("bound-hub"),
    );
    within(wrong_client.ensure_connected())
        .await
        .expect("authenticate the non-bound hub before refusal");
    let before = note_keys(&edge);
    let before_lsn = edge.current_lsn();
    let before_state = deletion_state(&edge, id);
    let tenant_id = TenantId::from("bound-hub");
    let before_progress = (
        edge.persisted_sync_watermarks(&tenant_id)
            .expect("read edge watermarks before wrong-hub pull"),
        edge.persisted_sync_pending_push_confirmation(&tenant_id)
            .expect("read edge pending confirmation before wrong-hub pull"),
        edge.persisted_sync_pull_cursor(&tenant_id)
            .expect("read edge pull cursor before wrong-hub pull"),
        edge.persisted_sync_applied_push_watermark(&tenant_id)
            .expect("read edge applied-push watermark before wrong-hub pull"),
    );
    let pull = within(wrong_client.pull_default())
        .await
        .expect_err("an authenticated non-bound hub is refused before pull mutation");
    assert!(
        !matches!(
            &pull,
            Error::ParseError(_) | Error::PlanError(_) | Error::SyncError(_) | Error::Other(_)
        ),
        "wrong-bound-hub refusal must be a typed authority error: {pull}"
    );
    let diagnostic = pull.to_string();
    let meaning = diagnostic.to_ascii_lowercase();
    assert!(
        meaning.contains("pull")
            && meaning.contains("push")
            && meaning.contains("purge")
            && meaning.contains("authorit"),
        "wrong-bound-hub refusal must name protection of pull, push, and PURGE authority: {diagnostic}"
    );
    assert!(
        diagnostic.contains(&good.node_id),
        "wrong-bound-hub refusal must identify the durably bound authoritative hub: {diagnostic}"
    );
    assert_eq!(
        note_keys(&edge),
        before,
        "wrong hub cannot mutate edge rows"
    );
    assert_eq!(
        edge.current_lsn(),
        before_lsn,
        "wrong hub cannot write a local row commit"
    );
    assert_eq!(
        deletion_state(&edge, id),
        before_state,
        "wrong hub cannot install a purge frontier"
    );
    assert_eq!(
        (
            edge.persisted_sync_watermarks(&tenant_id)
                .expect("read edge watermarks after wrong-hub pull"),
            edge.persisted_sync_pending_push_confirmation(&tenant_id)
                .expect("read edge pending confirmation after wrong-hub pull"),
            edge.persisted_sync_pull_cursor(&tenant_id)
                .expect("read edge pull cursor after wrong-hub pull"),
            edge.persisted_sync_applied_push_watermark(&tenant_id)
                .expect("read edge applied-push watermark after wrong-hub pull"),
        ),
        before_progress,
        "wrong hub cannot advance cursor, receipt, or watermark state"
    );
    within(wrong_client.shutdown()).await;
    drop(wrong_client);
    drop(edge);
    good.stop().await;
    wrong.stop().await;
}

#[tokio::test]
async fn edge_and_push_borne_purges_are_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "purge-origin-authority";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let (edge, client, edge_identity) = copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the edge fabric identity before binding its hub");
    put(&edge, id, "edge-cannot-origin-purge");
    within(client.push()).await.expect("seed hub");
    let before = note_keys(&edge);
    let before_state = deletion_state(&edge, id);
    let before_lsn = edge.current_lsn();
    let tenant_id = TenantId::from(tenant);
    let before_progress = (
        edge.persisted_sync_watermarks(&tenant_id)
            .expect("read edge watermarks before edge-origin refusal"),
        edge.persisted_sync_pending_push_confirmation(&tenant_id)
            .expect("read edge pending confirmation before edge-origin refusal"),
        edge.persisted_sync_pull_cursor(&tenant_id)
            .expect("read edge pull cursor before edge-origin refusal"),
        edge.persisted_sync_applied_push_watermark(&tenant_id)
            .expect("read edge applied-push watermark before edge-origin refusal"),
    );
    let local = edge
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect_err("edge SQL PURGE is visibly refused");
    assert!(
        !matches!(
            &local,
            Error::ParseError(_) | Error::PlanError(_) | Error::SyncError(_) | Error::Other(_)
        ),
        "edge SQL refusal must be a typed authority error: {local}"
    );
    let diagnostic = local.to_string();
    let meaning = diagnostic.to_ascii_lowercase();
    assert!(
        meaning.contains("purge") && meaning.contains("authorit"),
        "edge SQL refusal must identify PURGE authority: {diagnostic}"
    );
    assert!(
        diagnostic.contains(&hub.node_id),
        "edge SQL refusal must identify the authoritative hub: {local}"
    );
    assert_eq!(note_keys(&edge), before, "edge SQL refusal mutates no row");
    assert_eq!(
        deletion_state(&edge, id),
        before_state,
        "edge SQL refusal installs no purge frontier"
    );
    assert_eq!(
        edge.current_lsn(),
        before_lsn,
        "edge SQL refusal writes no local row commit"
    );
    assert_eq!(
        (
            edge.persisted_sync_watermarks(&tenant_id)
                .expect("read edge watermarks after edge-origin refusal"),
            edge.persisted_sync_pending_push_confirmation(&tenant_id)
                .expect("read edge pending confirmation after edge-origin refusal"),
            edge.persisted_sync_pull_cursor(&tenant_id)
                .expect("read edge pull cursor after edge-origin refusal"),
            edge.persisted_sync_applied_push_watermark(&tenant_id)
                .expect("read edge applied-push watermark after edge-origin refusal"),
        ),
        before_progress,
        "edge SQL refusal writes no cursor, receipt, or watermark state"
    );
    // This ordinary push proves the local refusal did not turn the row into a
    // DELETE. It is not the required push-borne PURGE proof.
    within(client.push())
        .await
        .expect("refused edge origin does not poison push");
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("edge-cannot-origin-purge")
    );
    within(client.shutdown()).await;
    drop(client);
    drop(edge);
    hub.stop().await;
}

#[tokio::test]
async fn authenticated_edge_push_borne_purge_is_refused_before_hub_mutation() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "purge-push-authority";
    let hub = start_hub(root.path(), tenant).await;
    let selected_id = Uuid::new_v4();
    let ordinary_id = Uuid::new_v4();
    let (edge, client, edge_identity) = copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    let edge_fabric_identity = Arc::new(
        FabricIdentity::load_or_generate(&edge_identity)
            .expect("persist the edge fabric identity before its authenticated push"),
    );
    let hub_dial = peer_dial_spec(&hub.ticket, &edge_identity);

    put(&edge, selected_id, "selected-lineage");
    within(client.push()).await.expect("seed the hub row");
    assert_eq!(
        body(&hub.db, selected_id).as_deref(),
        Some("selected-lineage"),
        "fixture: the hub owns the selected lineage before the forged push"
    );
    within(client.shutdown()).await;
    drop(client);

    let tenant_id = TenantId::from(tenant);
    let before_rows = note_keys(&hub.db);
    let before_selected_body = body(&hub.db, selected_id);
    let before_selected_deletion = deletion_state(&hub.db, selected_id);
    let before_lsn = hub.db.current_lsn();
    let before_progress = (
        hub.db
            .persisted_sync_watermarks(&tenant_id)
            .expect("read hub watermarks before forged purge"),
        hub.db
            .persisted_sync_pending_push_confirmation(&tenant_id)
            .expect("read hub pending confirmation before forged purge"),
        hub.db
            .persisted_sync_pull_cursor(&tenant_id)
            .expect("read hub pull cursor before forged purge"),
        hub.db
            .persisted_sync_applied_push_watermark(&tenant_id)
            .expect("read hub applied-push watermark before forged purge"),
    );

    put(&edge, ordinary_id, "ordinary-row-must-not-partially-apply");
    let injected_transport = Arc::new(PurgeInjectingPushTransport::new(client_transport(
        &hub_dial,
    )));
    let forged_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge,
        injected_transport,
        tenant_id.clone(),
        edge_fabric_identity,
    );
    let refusal = within(forged_client.push())
        .await
        .expect_err("authenticated edge push carrying PURGE must be refused");
    match refusal {
        Error::PurgeRequiresAuthoritativeHub { hub_node_id } => {
            assert_eq!(hub_node_id, hub.node_id, "refusal names the exact hub");
        }
        other => panic!(
            "push-borne PURGE must return a typed authority refusal without conflict-winner fields, got {other:?}"
        ),
    }

    assert_eq!(
        note_keys(&hub.db),
        before_rows,
        "the rejected push cannot add its ordinary sibling or remove the selected lineage"
    );
    assert_eq!(
        body(&hub.db, selected_id),
        before_selected_body,
        "the rejected push cannot change the selected row"
    );
    assert_eq!(
        deletion_state(&hub.db, selected_id),
        before_selected_deletion,
        "the rejected push cannot install deletion or purge state"
    );
    let post_request_changes = hub.db.change_log_since(before_lsn);
    // A contact timestamp can match the already-recorded millisecond and be
    // idempotent, so this exchange may leave no change-log entry. Any entry
    // that does exist must remain hub-local peer bookkeeping.
    assert!(
        post_request_changes.iter().all(|entry| matches!(
            entry,
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. }
                if table == WORK_NODE_CONTACTS_TABLE
        )),
        "the rejected push may record only work_node_contacts peer-contact changes: {post_request_changes:?}"
    );
    assert_eq!(
        (
            hub.db
                .persisted_sync_watermarks(&tenant_id)
                .expect("read hub watermarks after forged purge"),
            hub.db
                .persisted_sync_pending_push_confirmation(&tenant_id)
                .expect("read hub pending confirmation after forged purge"),
            hub.db
                .persisted_sync_pull_cursor(&tenant_id)
                .expect("read hub pull cursor after forged purge"),
            hub.db
                .persisted_sync_applied_push_watermark(&tenant_id)
                .expect("read hub applied-push watermark after forged purge"),
        ),
        before_progress,
        "the rejected push cannot advance hub sync progress"
    );

    within(forged_client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_delete_then_explicit_fresh_same_key_mints_new_lineage_and_syncs() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "ordinary-delete-fresh-lineage";
    let hub = start_hub(root.path(), tenant).await;
    let alternate_root = tempfile::tempdir().expect("alternate hub root");
    let alternate = start_hub(alternate_root.path(), tenant).await;
    let id = Uuid::new_v4();
    let (edge, client, edge_identity) = copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    let identity = Arc::new(
        FabricIdentity::load_or_generate(&edge_identity)
            .expect("persist the edge's existing adjacent fabric identity"),
    );
    let hub_dial = peer_dial_spec(&hub.ticket, &edge_identity);
    let alternate_dial = peer_dial_spec(&alternate.ticket, &edge_identity);

    put(&edge, id, "old-lineage");
    within(client.push()).await.expect("seed the old lineage");
    client
        .change_destination(&alternate.node_id)
        .expect("move the old lineage to a stale alternate hub");
    within(client.shutdown()).await;
    drop(client);
    let alternate_seed = SyncClient::new(edge.clone(), &alternate_dial, TenantId::from(tenant));
    within(alternate_seed.push())
        .await
        .expect("stale alternate receives the exact old lineage");
    assert_eq!(
        body(&alternate.db, id).as_deref(),
        Some("old-lineage"),
        "fixture: alternate hub retains the old lineage for later replay"
    );
    alternate_seed
        .change_destination(&hub.node_id)
        .expect("restore the authoritative hub before deleting");
    within(alternate_seed.shutdown()).await;
    drop(alternate_seed);

    let client = Arc::new(SyncClient::new(
        edge.clone(),
        &hub_dial,
        TenantId::from(tenant),
    ));
    delete(&edge, id);
    assert_absent(
        &edge,
        id,
        "old-lineage edge hides local delete before accepted sync",
    );
    let pause = client.pause_after_push_response_for_test();
    let old_delete_push = tokio::spawn({
        let client = client.clone();
        async move { client.push().await }
    });
    within(pause.wait_until_reached()).await;
    let edge_before_reply_effects = body(&edge, id);
    let hub_before_reply_effects = body(&hub.db, id);
    pause.release();
    let old_delete_result = within(old_delete_push)
        .await
        .expect("ordinary delete push task joins")
        .expect("ordinary delete reaches the authenticated hub");
    assert_eq!(
        edge_before_reply_effects, None,
        "decoded push response leaves the edge row absent before local reply effects"
    );
    assert_eq!(
        hub_before_reply_effects, None,
        "hub has deleted the row when its response is decoded before local reply effects"
    );
    assert!(
        old_delete_result.conflicts.is_empty(),
        "the ordinary delete is accepted"
    );
    assert_absent(&edge, id, "edge after accepted ordinary delete");
    assert_absent(&hub.db, id, "hub after accepted ordinary delete");
    let old_delete = deletion_state(&edge, id);
    assert_eq!(
        old_delete.accepted_delete_marker.as_deref(),
        Some("accepted"),
        "the edge durably records the accepted ordinary delete"
    );
    assert_eq!(
        old_delete.purge_frontier, None,
        "an ordinary delete does not invent a purge frontier"
    );
    let old_root = old_delete
        .lineage_root
        .clone()
        .expect("the accepted ordinary delete retains its lineage root");
    within(client.shutdown()).await;
    drop(client);

    put(&edge, id, "explicit-fresh-same-key");
    let recording_transport = Arc::new(RecordingRowLineageTransport::new(
        client_transport(&hub_dial),
        id,
    ));
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        recording_transport.clone(),
        TenantId::from(tenant),
        identity.clone(),
    );
    let fresh = within(client.push())
        .await
        .expect("explicit fresh same-key creation syncs");
    let captured = recording_transport.captured();
    assert_eq!(
        captured.len(),
        1,
        "the first fresh push carries exactly one lineage for the fresh row"
    );
    let fresh_root = captured[0].lineage_root.clone();
    assert_ne!(
        fresh_root, old_root,
        "the fresh row's actual outbound v6 lineage differs from the accepted old lineage"
    );
    assert!(
        fresh.conflicts.is_empty(),
        "the accepted old tombstone is not inherited by the fresh creation"
    );
    assert_eq!(body(&edge, id).as_deref(), Some("explicit-fresh-same-key"));
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("explicit-fresh-same-key")
    );
    assert_eq!(
        deletion_state(&edge, id),
        old_delete,
        "fresh creation and push retain the accepted old-lineage replay boundary separately"
    );

    client
        .change_destination(&alternate.node_id)
        .expect("point the fresh edge at the stale alternate hub");
    within(client.shutdown()).await;
    drop(client);
    let alternate_client = SyncClient::new(edge.clone(), &alternate_dial, TenantId::from(tenant));
    let stale_replay = within(alternate_client.pull_default())
        .await
        .expect_err("accepted old lineage is visibly refused after fresh creation");
    assert!(
        stale_replay
            .to_string()
            .contains("replays a lineage terminated by an accepted delete"),
        "the stale pull must fail at the retained accepted-lineage boundary: {stale_replay}"
    );
    assert_eq!(
        body(&edge, id).as_deref(),
        Some("explicit-fresh-same-key"),
        "the retained accepted marker refuses old-lineage replay without removing the fresh row"
    );
    assert_eq!(
        deletion_state(&edge, id),
        old_delete,
        "refusing the old replay preserves its accepted-lineage evidence"
    );
    alternate_client
        .change_destination(&hub.node_id)
        .expect("restore the authoritative hub for the fresh delete");
    within(alternate_client.shutdown()).await;
    drop(alternate_client);

    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        client_transport(&hub_dial),
        TenantId::from(tenant),
        identity,
    );
    delete(&edge, id);
    let fresh_delete_result = within(client.push())
        .await
        .expect("delete of the fresh lineage reaches the authenticated hub");
    assert!(
        fresh_delete_result.conflicts.is_empty(),
        "the fresh lineage delete is accepted"
    );
    let fresh_delete = deletion_state(&edge, id);
    assert_eq!(
        fresh_delete.accepted_delete_marker.as_deref(),
        Some("accepted"),
        "the fresh lineage has its own accepted delete marker"
    );
    assert_eq!(
        fresh_delete.lineage_root.as_deref(),
        Some(fresh_root.as_str()),
        "later delete staging preserves rather than repairs the creation-time lineage"
    );

    within(client.shutdown()).await;
    alternate.stop().await;
    hub.stop().await;
}

#[tokio::test]
async fn pre_purge_lineage_is_refused_and_new_same_key_data_syncs() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "purged-lineage-and-fresh-key";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let independent = Uuid::new_v4();
    let (edge, client, edge_identity) = copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the edge's existing adjacent fabric identity");
    put(&edge, id, "old-lineage");
    put(&edge, independent, "independent-record");
    within(client.push()).await.expect("seed old lineage");
    assert_eq!(
        body(&hub.db, independent).as_deref(),
        Some("independent-record"),
        "independently identified data reaches the hub before purge"
    );
    purge(&hub.db, id);
    edit(&edge, id, "offline-descendant-must-not-return");
    let stale = within(client.push())
        .await
        .expect("hub visibly refuses the offline descendant lineage");
    let rendered = serde_json::to_value(&stale).expect("typed lineage refusal serializes");
    let conflicts = rendered["conflicts"]
        .as_array()
        .expect("offline descendant refusal has typed conflicts");
    assert_eq!(
        conflicts.len(),
        1,
        "one offline descendant produces one visible refusal"
    );
    let conflict = &conflicts[0];
    assert_eq!(conflict["table"], "notes", "refusal names the table");
    assert_eq!(
        conflict["natural_key"],
        serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(id)))
            .expect("serialize exact purged key"),
        "refusal names the exact purged key"
    );
    assert_eq!(
        conflict["mutation_kind"], "edit",
        "refusal identifies the descendant edit"
    );
    let diagnostic = conflict.to_string().to_ascii_lowercase();
    assert!(
        diagnostic.contains("purge") && diagnostic.contains("lineage"),
        "typed refusal must identify the permanent purged-lineage boundary: {conflict}"
    );
    assert_absent(&hub.db, id, "hub refuses the offline descendant");
    within(client.pull_default())
        .await
        .expect("edge applies the authoritative purge after visible refusal");
    assert_absent(&edge, id, "edge removes offline descendant after refusal");
    assert_absent(&hub.db, id, "hub keeps purged lineage absent");
    assert!(
        deletion_state(&edge, id).purge_frontier.is_some(),
        "edge durably applies the purge frontier before fresh creation"
    );
    assert_eq!(
        body(&edge, independent).as_deref(),
        Some("independent-record"),
        "independently identified data remains on the edge"
    );
    assert_eq!(
        body(&hub.db, independent).as_deref(),
        Some("independent-record"),
        "independently identified data remains on the hub"
    );
    put(&edge, id, "explicit-fresh-same-key");
    let fresh = within(client.push())
        .await
        .expect("fresh same-key lineage syncs after purge application");
    assert!(
        fresh.conflicts.is_empty(),
        "explicit fresh creation is not part of the refused lineage"
    );
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("explicit-fresh-same-key")
    );
    assert_eq!(body(&edge, id).as_deref(), Some("explicit-fresh-same-key"));
    within(client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn recreated_table_refuses_removed_generation_and_syncs_new_rows() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "recreated-table-generation";
    let hub = start_hub(root.path(), tenant).await;
    let old_id = Uuid::new_v4();
    let never_purged_old_id = Uuid::new_v4();
    let new_id = Uuid::new_v4();
    let outbound_new_id = Uuid::new_v4();
    let (old_source, old_client, old_identity) =
        copy_test_edge(root.path(), "old-source", &hub.ticket, tenant);
    let source_identity = FabricIdentity::load_or_generate(&old_identity)
        .expect("persist the old source's existing adjacent fabric identity");
    put(&old_source, old_id, "removed-generation-row");
    put(
        &old_source,
        never_purged_old_id,
        "never-purged-removed-generation-row",
    );
    let artifact = root.path().join("before-generation-purge.cdb");
    within(old_client.push())
        .await
        .expect("seed the old generation over authenticated sync");
    assert_eq!(
        body(&hub.db, old_id).as_deref(),
        Some("removed-generation-row"),
        "hub receives the old generation before purge"
    );
    assert_eq!(
        body(&hub.db, never_purged_old_id).as_deref(),
        Some("never-purged-removed-generation-row"),
        "hub receives the never-purged old-generation control row"
    );
    old_source
        .export_snapshot(&artifact)
        .expect("capture the authenticated old-generation source before removal");
    purge(&hub.db, old_id);
    hub.db
        .execute("DROP TABLE notes", &HashMap::new())
        .expect("recreate table boundary");
    create_notes(&hub.db);
    put(&hub.db, new_id, "new-generation-row");
    within(old_client.shutdown()).await;
    drop(old_client);
    drop(old_source);

    let restored_old =
        Arc::new(Database::open(&artifact).expect("open restored old-generation source"));
    assert_eq!(
        body(&restored_old, old_id).as_deref(),
        Some("removed-generation-row")
    );
    assert_eq!(
        body(&restored_old, never_purged_old_id).as_deref(),
        Some("never-purged-removed-generation-row")
    );
    edit(
        &restored_old,
        old_id,
        "stale-purged-old-generation-descendant",
    );
    edit(
        &restored_old,
        never_purged_old_id,
        "stale-never-purged-old-generation-descendant",
    );
    let old_key = NaturalKey::single("id".to_string(), Value::Uuid(old_id));
    let never_purged_old_key =
        NaturalKey::single("id".to_string(), Value::Uuid(never_purged_old_id));
    let old_lineage = restored_old
        .authoritative_purge_current_live_row_sidecar_for_test("notes", &old_key)
        .expect("restored old-generation row keeps its authenticated lineage sidecar");
    let restored_client = SyncClient::new(
        restored_old.clone(),
        &peer_dial_spec(&hub.ticket, &old_identity),
        TenantId::from(tenant),
    );
    let replay_source_ceiling = restored_old
        .changes_since(restored_client.push_watermark())
        .max_lsn()
        .expect("the stale old-generation descendants are outbound work");
    let replay = within(restored_client.push())
        .await
        .expect("removed table generation returns visible typed refusals");
    assert_eq!(
        replay.applied_rows, 0,
        "old-generation rows never mutate the hub"
    );
    assert_eq!(replay.skipped_rows, 2, "each old-generation row is refused");
    assert_eq!(
        replay.conflicts.len(),
        2,
        "both old-generation rows stay visible"
    );
    assert!(
        replay.conflicts.iter().any(|conflict| {
            conflict.natural_key == old_key && conflict.reason.as_deref() == Some("purged_lineage")
        }),
        "the purged old-generation row keeps permanent-lineage precedence"
    );
    assert!(
        replay.conflicts.iter().any(|conflict| {
            conflict.natural_key == never_purged_old_key
                && conflict.reason.as_deref() == Some("removed_generation")
        }),
        "the unpurged old-generation row reports its removed table generation"
    );
    let source_incarnation = restored_old
        .sync_incarnation(&TenantId::from(tenant))
        .expect("restored source keeps its authenticated database incarnation");
    let source_node_id = source_identity.node_id();
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(tenant),
                &source_node_id,
                source_incarnation,
            )
            .expect("read durable terminal-refusal receipt"),
        Some(replay_source_ceiling),
        "a fully refused old-generation replay still commits the edge receipt for lost-ack recovery"
    );
    assert_absent(
        &hub.db,
        old_id,
        "recreated table does not accept removed generation",
    );
    assert_absent(
        &hub.db,
        never_purged_old_id,
        "recreated table refuses a never-purged removed-generation row",
    );
    within(restored_client.pull_default())
        .await
        .expect("restored source adopts the recreated table generation");
    assert_absent(
        &restored_old,
        old_id,
        "restored source removes the refused old generation",
    );
    assert_absent(
        &restored_old,
        never_purged_old_id,
        "restored source removes the never-purged old generation",
    );
    assert!(
        matches!(
            restored_old.classify_authoritative_purge_root_for_test(
                "notes",
                old_lineage.table_generation,
                &old_key,
                &old_lineage.lineage_root,
            ),
            AuthoritativePurgeRootClassification::Purged { .. }
        ),
        "restored source retains the old-generation purge frontier after adopting generation two"
    );
    assert_eq!(
        body(&hub.db, new_id).as_deref(),
        Some("new-generation-row"),
        "new-generation row remains on the hub"
    );
    assert_eq!(
        body(&restored_old, new_id).as_deref(),
        Some("new-generation-row"),
        "new-generation row crosses authenticated sync normally"
    );
    put(
        &restored_old,
        outbound_new_id,
        "restored-source-new-generation-row",
    );
    let outbound_new = within(restored_client.push())
        .await
        .expect("a row authored after generation adoption syncs outbound");
    assert!(
        outbound_new.conflicts.is_empty(),
        "the replacement generation accepts newly authored outbound data"
    );
    assert_eq!(
        body(&restored_old, outbound_new_id).as_deref(),
        Some("restored-source-new-generation-row"),
        "the restored source retains its replacement-generation row"
    );
    assert_eq!(
        body(&hub.db, outbound_new_id).as_deref(),
        Some("restored-source-new-generation-row"),
        "the hub accepts a row authored under the replacement generation"
    );
    within(restored_client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn malformed_authenticated_push_replies_to_an_exact_retry() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "malformed-push-retry";
    let hub = start_hub(root.path(), tenant).await;
    let edge_identity = root.path().join("malformed-edge.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("malformed-edge.db")).expect("open edge"));
    create_notes(&edge);
    let identity = Arc::new(
        FabricIdentity::load_or_generate(&edge_identity)
            .expect("persist the malformed edge's authenticated identity"),
    );
    put(&edge, Uuid::new_v4(), "malformed immutable lineage");
    let transport = Arc::new(MalformedLineagePushTransport {
        inner: client_transport(&peer_dial_spec(&hub.ticket, &edge_identity)),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge,
        transport,
        TenantId::from(tenant),
        identity,
    );

    for attempt in 1..=2 {
        let result = within(client.push()).await;
        assert!(
            matches!(result, Err(Error::SyncError(ref error)) if error.contains("lineage root")),
            "retry {attempt} must receive the same preflight rejection, got {result:?}"
        );
    }

    within(client.shutdown()).await;
    hub.stop().await;
}

/// A forced post-restart log gap makes a stale edge receive the hub's persisted
/// current-state snapshot. Its older authoritative purge must still cross
/// first: otherwise the replacement table generation can make the permanent
/// old-generation barrier look foreign and the snapshot can vanish.
#[tokio::test]
async fn reopened_log_gap_pages_old_generation_purge_before_current_schema_snapshot() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "reopened-log-gap-paged-purge";
    let old_id = Uuid::from_u128(0x6a11_0000_0000_0000_0000_0000_0000_0001);
    let current_id = Uuid::from_u128(0x6a11_0000_0000_0000_0000_0000_0000_0002);
    let hub = start_hub(root.path(), tenant).await;
    let edge_identity = root.path().join("paged-edge.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("paged-edge.db")).expect("open edge"));
    create_notes(&edge);
    create_keep_first_notes(&edge);

    put(&hub.db, old_id, "old-generation-row");
    let baseline = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    within(baseline.pull_default())
        .await
        .expect("edge receives the old-generation row before purge");
    assert_eq!(
        body(&edge, old_id).as_deref(),
        Some("old-generation-row"),
        "fixture: the edge holds the lineage that the old-generation purge must remove"
    );
    let old_key = NaturalKey::single("id".to_string(), Value::Uuid(old_id));
    let old_lineage = edge
        .authoritative_purge_current_live_row_sidecar_for_test("notes", &old_key)
        .expect("fixture retains the authenticated old-generation creator root");
    within(baseline.shutdown()).await;
    drop(baseline);

    purge(&hub.db, old_id);
    hub.db
        .execute("DROP TABLE notes", &HashMap::new())
        .expect("close the purged table generation");
    create_notes(&hub.db);
    put(&hub.db, current_id, "current-generation-row");

    // Reopen proves the durable image is complete; then force the exact
    // unavailable-log condition while retaining that store image. The next
    // pull must use persisted-state recovery, not convenient live history.
    let old_hub_handle = hub.db.clone();
    hub.stop().await;
    drop(old_hub_handle);
    let hub = start_hub(root.path(), tenant).await;
    hub.db.__force_sync_log_gap_for_test();
    assert!(
        hub.db.change_log_since(Lsn(0)).is_empty(),
        "fixture: reopening forces the durable persisted-state path instead of change-log replay"
    );
    assert!(
        hub.db.ddl_log_since(Lsn(0)).is_empty(),
        "fixture: reopening forces synthetic current schema rather than a live DDL log"
    );

    let edge_identity_value = Arc::new(
        FabricIdentity::load_or_generate(&edge_identity)
            .expect("reuse the edge's authenticated fabric identity"),
    );
    let transport = Arc::new(OneFrontierPullTransport::new(client_transport(
        &peer_dial_spec(&hub.ticket, &edge_identity),
    )));
    let pause = transport.pause_before_second_request_for_test();
    let client = Arc::new(
        SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            transport.clone(),
            TenantId::from(tenant),
            edge_identity_value,
        ),
    );
    let pulling_client = client.clone();
    let pull = tokio::spawn(async move { pulling_client.pull_default().await });

    // The second request cannot leave until page one has been applied. This
    // proves the durable old-generation purge crosses the edge before the
    // replacement schema is even requested, with no scheduling sleep.
    within(pause.reached.notified()).await;
    assert_eq!(
        body(&edge, old_id),
        None,
        "the first, purge-only page removes the old-generation row before current schema delivery"
    );
    assert!(
        deletion_state(&edge, old_id).purge_frontier.is_some(),
        "the first page durably applies the permanent old-generation purge frontier"
    );
    pause
        .release
        .send(())
        .expect("release the second deterministic pull page");
    within(pull)
        .await
        .expect("paged pull task joins")
        .expect("current schema snapshot applies after the purge page");

    let pages = transport.pages();
    assert!(
        pages.len() >= 2,
        "the purge page is followed by at least one current-snapshot page"
    );
    assert!(
        pages.iter().all(|page| page.request.max_entries == Some(1)),
        "the server received an explicit one-frontier pagination limit on both requests"
    );
    for window in pages.windows(2) {
        assert_eq!(
            window[1].request.since_lsn,
            window[0]
                .ordinary
                .cursor
                .expect("every non-final page carries its consumed frontier cursor"),
            "each page resumes immediately after its predecessor"
        );
    }
    assert!(
        pages[0].ordinary.has_more,
        "the old purge ends the first page"
    );
    assert_eq!(pages[0].ordinary.changeset.purges.len(), 1);
    assert!(
        pages[0].ordinary.changeset.ddl.is_empty()
            && pages[0].ordinary.changeset.rows.is_empty()
            && pages[0].dependency_units.is_empty(),
        "the first page contains only the old-generation purge, never later schema or data"
    );
    assert!(
        !pages.last().expect("at least two pages").ordinary.has_more,
        "the paged current snapshot reaches a finite final page"
    );
    assert!(
        pages[1..]
            .iter()
            .all(|page| page.ordinary.changeset.purges.is_empty()),
        "no current-snapshot page repeats the consumed old purge"
    );
    let later_changesets = pages[1..].iter().flat_map(|page| {
        std::iter::once(&page.ordinary.changeset).chain(page.dependency_units.iter())
    });
    assert!(
        later_changesets.clone().any(|changes| {
            changes.ddl.iter().any(
                |ddl| matches!(ddl, WireDdlChange::CreateTable { name, .. } if name == "notes"),
            )
        }),
        "the second page re-serves the current notes declaration after the old-generation barrier"
    );
    assert!(
        later_changesets.clone().any(|changes| {
            changes.rows.iter().any(|row| {
                row.table == "notes"
                    && row.natural_key.value == Value::Uuid(current_id)
                    && !row.deleted
            })
        }),
        "the second page re-serves current-generation data rather than losing it behind the purge"
    );
    assert_eq!(
        body(&edge, current_id).as_deref(),
        Some("current-generation-row"),
        "the edge applies the re-served current schema and data after its old purge"
    );
    assert_eq!(
        body(&edge, old_id),
        None,
        "the current snapshot never resurrects the purged old-generation row"
    );

    // A wiped worker has neither generation one nor the old row. It still
    // adopts the authenticated permanent frontier before receiving the
    // current snapshot, so later stale generation-one replay remains fenced.
    let blank_directory = root.path().join("blank-paged-edge");
    std::fs::create_dir_all(&blank_directory).expect("create blank edge directory");
    let blank_path = blank_directory.join("blank-paged-edge.db");
    let blank_identity = blank_directory.join("blank-paged-edge.db.fabric-identity.key");
    let blank = Arc::new(Database::open(&blank_path).expect("open genuinely blank edge"));
    let blank_client = SyncClient::new(
        blank.clone(),
        &peer_dial_spec(&hub.ticket, &blank_identity),
        TenantId::from(tenant),
    );
    within(blank_client.pull_default())
        .await
        .expect("a blank edge adopts the old frontier and current snapshot");
    assert_eq!(body(&blank, old_id), None);
    assert_eq!(
        body(&blank, current_id).as_deref(),
        Some("current-generation-row")
    );
    assert!(matches!(
        blank.classify_authoritative_purge_root_for_test(
            "notes",
            old_lineage.table_generation,
            &old_key,
            &old_lineage.lineage_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    within(blank_client.shutdown()).await;
    drop(blank_client);
    drop(blank);
    let reopened_blank =
        Database::open(&blank_path).expect("reopen the wiped edge after frontier adoption");
    assert!(matches!(
        reopened_blank.classify_authoritative_purge_root_for_test(
            "notes",
            old_lineage.table_generation,
            &old_key,
            &old_lineage.lineage_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));

    within(client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn post_purge_backup_preserves_absence() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "post-purge-backup";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    put(&hub.db, id, "must-not-enter-post-purge-backup");
    put(&hub.db, survivor, "backup-decoy-survives");
    for ordinal in 2..1001_u128 {
        put(
            &hub.db,
            Uuid::from_u128(0x7300_0000_0000_0000_0000_0000_0000_0000 + ordinal),
            &format!("backup-vector-decoy-{ordinal}"),
        );
    }
    hub.db
        .execute(
            "CREATE INDEX notes_body_idx ON notes (body)",
            &HashMap::new(),
        )
        .expect("create backup relational index");
    let selected_row_id_before_purge = row_id(&hub.db, id);
    let survivor_row_id_before_purge = row_id(&hub.db, survivor);
    let before_vector_plan = hub
        .db
        .execute(
            "SELECT id FROM notes ORDER BY embedding <=> [1,0,0] LIMIT 10",
            &HashMap::new(),
        )
        .expect("materialize pre-purge backup HNSW graph");
    assert!(
        before_vector_plan
            .trace
            .physical_plan
            .contains("HNSWSearch"),
        "backup fixture must capture real ANN/HNSW state"
    );

    let captured_artifact = root.path().join("captured-before-purge.cdb");
    let export_pause = hub.db.pause_after_export_snapshot_capture_for_test();
    let exporting_db = hub.db.clone();
    let exporting_path = captured_artifact.clone();
    let export_thread = std::thread::spawn(move || exporting_db.export_snapshot(&exporting_path));
    assert!(
        export_pause.wait_until_reached(Duration::from_secs(10)),
        "export must reach the deterministic post-capture, pre-publication fence"
    );

    let purging_db = hub.db.clone();
    let (purge_done_tx, purge_done_rx) = mpsc::channel();
    let purge_thread = std::thread::spawn(move || {
        let result = purging_db.execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        );
        purge_done_tx
            .send(result)
            .expect("report concurrent purge result");
    });
    let purge_result = match purge_done_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(result) => result,
        Err(error) => {
            export_pause.release();
            purge_thread.join().expect("join timed-out purge thread");
            export_thread
                .join()
                .expect("join export after releasing deterministic fence")
                .ok();
            panic!(
                "authoritative purge must fence an engine-held export without waiting for publication: {error}"
            );
        }
    };
    export_pause.release();
    purge_thread.join().expect("join purge thread");
    let captured_export = export_thread.join().expect("join captured export thread");
    purge_result.expect("concurrent authoritative purge");

    match captured_export {
        Ok(_) => {
            let restored =
                Database::open(&captured_artifact).expect("open safely published race artifact");
            assert_restored_purge(
                &restored,
                id,
                survivor,
                selected_row_id_before_purge,
                survivor_row_id_before_purge,
                "must-not-enter-post-purge-backup",
                "artifact whose immutable capture preceded purge",
            );
        }
        Err(error) => {
            let diagnostic = error.to_string();
            let meaning = diagnostic.to_ascii_lowercase();
            assert!(
                meaning.contains("purge")
                    && (meaning.contains("export") || meaning.contains("snapshot"))
                    && (meaning.contains("fence") || meaning.contains("concurrent")),
                "a refused race export must report the specific purge fence, not an unrelated failure: {diagnostic}"
            );
            assert!(
                !captured_artifact.exists(),
                "a purge-fenced captured-before-purge export must not publish an artifact"
            );
        }
    }
    assert_no_export_attempt_files(root.path(), &captured_artifact);

    let artifact = root.path().join("post-purge.cdb");
    hub.db
        .export_snapshot(&artifact)
        .expect("export post-purge artifact");
    let restored = Database::open(&artifact).expect("open post-purge artifact");
    assert_restored_purge(
        &restored,
        id,
        survivor,
        selected_row_id_before_purge,
        survivor_row_id_before_purge,
        "must-not-enter-post-purge-backup",
        "post-purge restored database",
    );
    assert_no_export_attempt_files(root.path(), &artifact);
    hub.stop().await;
}

#[tokio::test]
async fn pre_purge_restore_is_refused_by_peers_then_reissued() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "pre-purge-restore-reissue";
    let hub = start_hub(root.path(), tenant).await;
    let id = Uuid::new_v4();
    let hub_identity = root.path().join("hub.db.fabric-identity.key");
    let hub_node_id = hub.node_id.clone();
    let (edge, client, edge_identity) = copy_test_edge(root.path(), "edge", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the edge's existing adjacent fabric identity");
    put(&edge, id, "old-backup-row");
    within(client.push()).await.expect("seed hub before backup");
    let pre_purge = root.path().join("pre-purge.cdb");
    hub.db
        .export_snapshot(&pre_purge)
        .expect("export pre-purge backup");
    purge(&hub.db, id);
    within(client.pull_default())
        .await
        .expect("edge applies permanent tombstone");
    assert!(
        deletion_state(&edge, id).purge_frontier.is_some(),
        "peer durably retains the purge before hub restore"
    );
    within(client.shutdown()).await;
    drop(client);
    hub.stop().await;

    let restored = Arc::new(Database::open(&pre_purge).expect("open old hub backup"));
    assert_eq!(body(&restored, id).as_deref(), Some("old-backup-row"));
    let (restored_ticket, restored_transport) = {
        let endpoint = IrohServer::bind(&spec(&hub_identity))
            .await
            .expect("restore the hub with its existing persisted fabric identity");
        assert_eq!(
            endpoint.node_id(),
            hub_node_id,
            "restored hub must reuse the authoritative hub identity"
        );
        (endpoint.ticket(), endpoint.transport())
    };
    let restored_identity = Arc::new(
        FabricIdentity::load_or_generate(&hub_identity)
            .expect("load the restored hub's persisted fabric identity"),
    );
    let restored_node_id = restored_identity.node_id();
    let restored_server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            restored.clone(),
            restored_transport,
            TenantId::from(tenant),
            restored_node_id,
            restored_identity,
        ),
    );
    let restored_stop = Arc::new(AtomicBool::new(false));
    let restored_task = tokio::spawn({
        let server = restored_server.clone();
        let stop = restored_stop.clone();
        async move { server.run_until(stop).await }
    });
    let restored_client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&restored_ticket, &edge_identity),
        TenantId::from(tenant),
    );
    let retained_tombstone = deletion_state(&edge, id);
    let expected_lineage_root = retained_tombstone
        .lineage_root
        .clone()
        .expect("peer persists the canonical purged lineage root");
    assert!(
        !expected_lineage_root.is_empty(),
        "the persisted purge lineage root is never an empty placeholder"
    );
    let expected_frontier = Lsn(retained_tombstone
        .purge_frontier
        .as_deref()
        .expect("peer persists the purge frontier")
        .parse::<u64>()
        .expect("persisted purge frontier is a canonical LSN"));
    let refusal = within(restored_client.pull_default())
        .await
        .expect_err("tombstoned peer refuses pre-purge restored lineage with a typed fence");
    match refusal {
        Error::PurgeCausalityFence {
            table,
            key,
            lineage_root,
            frontier,
        } => {
            assert_eq!(table, "notes", "refusal names the table");
            assert_eq!(
                key,
                NaturalKey::single("id".to_string(), Value::Uuid(id)).pairs(),
                "refusal names the exact restored key"
            );
            assert_eq!(
                lineage_root, expected_lineage_root,
                "refusal reports the persisted canonical lineage root"
            );
            assert_eq!(
                frontier, expected_frontier,
                "refusal reports the exact persisted purge frontier"
            );
        }
        other => panic!("pre-purge restore must return PurgeCausalityFence, got {other:?}"),
    }
    assert_absent(
        &edge,
        id,
        "peer keeps tombstone while old backup serves row",
    );
    purge(&restored, id);
    within(restored_client.pull_default())
        .await
        .expect("peer performs a real pull after operator reissues purge");
    assert_absent(
        &restored,
        id,
        "operator reissues purge after old backup restore",
    );
    assert_absent(
        &edge,
        id,
        "peer remains absent after reissued purge crosses sync",
    );
    assert!(
        deletion_state(&restored, id).purge_frontier.is_some(),
        "restored authoritative hub persists the reissued purge"
    );
    assert!(
        deletion_state(&edge, id).purge_frontier.is_some(),
        "peer keeps its permanent purge after the recovery pull"
    );
    within(restored_client.shutdown()).await;
    restored_stop.store(true, Ordering::SeqCst);
    within(restored_task)
        .await
        .expect("restored hub must stop cleanly");
}

#[tokio::test]
async fn purge_reaches_push_only_table_and_refuses_stale_lineage() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "push-only-purge-plane";
    let id = Uuid::from_u128(0x8e1d_041f_81f5_4c39_8a22_49dc_8a70_0444);
    let sentinel_id = Uuid::from_u128(0x0d7a_5a8b_6fce_43ce_b4f4_94d4_3d90_0445);
    let hub = start_hub(root.path(), tenant).await;
    let push_only_ddl = "CREATE TABLE push_only_notes (id UUID PRIMARY KEY, body TEXT) \
         SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST";
    hub.db
        .execute(push_only_ddl, &HashMap::new())
        .expect("hub declares the push-only table");
    hub.db
        .execute(
            "INSERT INTO push_only_notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(sentinel_id)),
                (
                    "body".to_string(),
                    Value::Text("hub-only-unpurged-sentinel".to_string()),
                ),
            ]),
        )
        .expect("hub writes an unpurged push-only sentinel");
    let (edge, client, edge_identity) =
        copy_test_edge(root.path(), "push-only-edge", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the push-only edge fabric identity");
    edge.execute(push_only_ddl, &HashMap::new())
        .expect("edge declares the same push-only table");
    let selected = HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        (
            "body".to_string(),
            Value::Text("must-be-purged-from-one-way-lane".to_string()),
        ),
    ]);
    edge.execute(
        "INSERT INTO push_only_notes (id, body) VALUES ($id, $body)",
        &selected,
    )
    .expect("edge writes the one-way row");
    within(client.push())
        .await
        .expect("push-only row reaches the authoritative hub before purge");
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(id))]),
            )
            .expect("read hub one-way row")
            .rows,
        vec![vec![Value::Text(
            "must-be-purged-from-one-way-lane".to_string()
        )]],
        "fixture proves ordinary push-only row flow reached the hub"
    );
    assert!(
        edge.execute(
            "SELECT body FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(sentinel_id))]),
        )
        .expect("read edge before purge pull")
        .rows
        .is_empty(),
        "ordinary hub-only push-only rows stay absent before purge delivery"
    );
    let stale_edge_artifact = root.path().join("push-only-edge-before-purge.cdb");
    edge.export_snapshot(&stale_edge_artifact)
        .expect("export stale edge after its authenticated seed");
    hub.db
        .execute(
            "PURGE FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("authoritative SQL purge enters its direction-independent delivery plane");
    within(client.pull_default())
        .await
        .expect("push-only edge receives the authoritative purge instruction");
    assert!(
        edge.execute(
            "SELECT body FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(sentinel_id))]),
        )
        .expect("read edge after purge pull")
        .rows
        .is_empty(),
        "the purge plane crosses without pulling the unrelated hub-only row downward"
    );
    for (place, db) in [("hub", &hub.db), ("push-only edge", &edge)] {
        assert!(
            db.execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(id))]),
            )
            .unwrap_or_else(|error| panic!("read {place} after purge: {error}"))
            .rows
            .is_empty(),
            "{place} must keep the purged one-way row absent"
        );
        assert!(
            db.durable_deletion_state_for_test("push_only_notes", &Value::Uuid(id))
                .purge_frontier
                .is_some(),
            "{place} must retain the permanent lineage tombstone"
        );
    }
    within(client.shutdown()).await;
    drop(client);
    drop(edge);

    // The live hub keeps its authoritative tombstone. A stale edge artifact
    // makes a descendant edit and reconnects as the same persisted node.
    let stale_edge =
        Arc::new(Database::open(&stale_edge_artifact).expect("open stale push-only edge artifact"));
    stale_edge
        .execute(
            "UPDATE push_only_notes SET body = 'stale-descendant-after-purge' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("stale edge stages a descendant of the purged one-way row");
    let stale_client = SyncClient::new(
        stale_edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    let refusal = within(stale_client.push())
        .await
        .expect("live tombstoned hub visibly refuses the stale push-only descendant");
    let rendered = serde_json::to_value(&refusal).expect("stale refusal serializes");
    let conflicts = rendered["conflicts"]
        .as_array()
        .expect("stale push-only descendant has typed conflict diagnostics");
    assert_eq!(
        conflicts.len(),
        1,
        "one stale descendant yields exactly one typed refusal"
    );
    assert_eq!(
        conflicts[0]["table"], "push_only_notes",
        "typed refusal names the purged table"
    );
    assert_eq!(
        conflicts[0]["natural_key"],
        serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(id)))
            .expect("serialize stale purged key"),
        "typed refusal names the exact purged key"
    );
    assert_eq!(
        conflicts[0]["mutation_kind"], "edit",
        "typed refusal identifies the stale descendant edit"
    );
    let diagnostic = conflicts[0].to_string().to_ascii_lowercase();
    assert!(
        diagnostic.contains("purge") && diagnostic.contains("lineage"),
        "stale refusal identifies the permanent purged-lineage boundary: {}",
        conflicts[0]
    );
    assert!(
        hub.db
            .execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(id))]),
            )
            .expect("read live hub after stale descendant refusal")
            .rows
            .is_empty(),
        "the live hub keeps the purged one-way row absent after refusing the stale descendant"
    );
    within(stale_client.pull_default())
        .await
        .expect("stale push-only edge receives the authoritative purge after refusal");
    for (place, db) in [("live hub", &hub.db), ("stale push-only edge", &stale_edge)] {
        assert!(
            db.execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(id))]),
            )
            .unwrap_or_else(|error| panic!("read {place} after authoritative purge: {error}"))
            .rows
            .is_empty(),
            "{place} keeps the purged one-way row absent"
        );
        assert!(
            db.durable_deletion_state_for_test("push_only_notes", &Value::Uuid(id))
                .purge_frontier
                .is_some(),
            "{place} retains the permanent tombstone"
        );
    }
    assert!(
        stale_edge
            .execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(sentinel_id))]),
            )
            .expect("read stale edge after authoritative purge")
            .rows
            .is_empty(),
        "the purge plane does not pull the unrelated hub-only push-only sentinel downward"
    );
    within(stale_client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn shared_blob_purge_names_the_remaining_referent() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "shared-blob-survivor";
    let hub = start_hub(root.path(), tenant).await;
    install_work_ledger_schema(&hub.db).expect("install real work-ledger tables");
    // The selected and remaining jobs deliberately share the same exact
    // content address. PURGE must name the remaining job and retain its
    // bytes; deleting an unrelated ordinary row is not a blob proof.
    let shared_bytes = b"content-addressed-shared-input";
    let shared_hash = BlobHash::of(shared_bytes);
    let store = BlobStore::new(
        hub.db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        root.path().join("hub.db.fabric-identity.key"),
    );
    assert_eq!(
        store
            .ingest_bytes(shared_bytes)
            .expect("ingest shared bytes"),
        shared_hash
    );
    for job_id in ["selected-job", "remaining-job"] {
        let job = JobSpec::builder(job_id, "purge-proof", "input", "hub-node")
            .input_refs(vec![InputRef::blob_ref(shared_hash.clone())])
            .submitted_at_ms(1)
            .build();
        submit_job(&hub.db, &job, &[] as &[&[u8]]).expect("submit real blob-referencing job");
    }
    assert_eq!(
        store.exact_hash_state_for_test(),
        BTreeSet::from([shared_hash.as_bytes()]),
        "the production adapter serves exactly the shared content hash before purge"
    );
    let purge_result = hub
        .db
        .execute(
            "PURGE FROM work_jobs WHERE job_id = 'selected-job'",
            &HashMap::new(),
        )
        .expect("authoritative purge selects the work-ledger referent");
    assert_eq!(
        purge_result.rows_affected, 1,
        "purge reports the one selected work-ledger referent"
    );
    let report =
        serde_json::to_string(&purge_result.rows).expect("purge survivor report serializes");
    assert!(
        report.contains("remaining-job"),
        "purge result must name the surviving job that still references the shared blob: {report}"
    );
    let jobs = hub
        .db
        .execute("SELECT job_id FROM work_jobs", &HashMap::new())
        .expect("read surviving work-ledger referent");
    assert_eq!(
        jobs.rows,
        vec![vec![Value::Text("remaining-job".to_string())]],
        "only the named remaining job keeps the shared input live"
    );
    assert_eq!(
        store.exact_hash_state_for_test(),
        BTreeSet::from([shared_hash.as_bytes()]),
        "the exact shared blob remains for the named live referent"
    );
    hub.stop().await;
}
