use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::plugin::DatabasePlugin;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
};
use contextdb_server::identity::FabricIdentity;
use contextdb_server::protocol::{
    DependencyCompletePullResponse, MessageType, PullRequest, PullResponse, PushRequest,
    PushResponse, decode, encode,
};
use contextdb_server::subjects::{pull_subject, push_subject};
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use uuid::Uuid;

const LIVENESS_TIMEOUT: Duration = Duration::from_secs(5);
const BARRIER_TIMEOUT: Duration = Duration::from_secs(5);
const NO_EARLY_REPLY_TIMEOUT: Duration = Duration::from_millis(250);
const TABLE: &str = "items";

#[derive(Clone)]
struct ApplyBlocker {
    min_rows: usize,
    state: Arc<(Mutex<ApplyBlockerState>, Condvar)>,
}

struct ApplyBlockerState {
    hit: bool,
    hits: usize,
    released: bool,
}

impl ApplyBlocker {
    fn new(min_rows: usize) -> Self {
        Self {
            min_rows,
            state: Arc::new((
                Mutex::new(ApplyBlockerState {
                    hit: false,
                    hits: 0,
                    released: false,
                }),
                Condvar::new(),
            )),
        }
    }

    async fn wait_until_hit(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.is_hit() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn is_hit(&self) -> bool {
        let (lock, _) = &*self.state;
        lock.lock().unwrap_or_else(|err| err.into_inner()).hit
    }

    fn hit_count(&self) -> usize {
        let (lock, _) = &*self.state;
        lock.lock().unwrap_or_else(|err| err.into_inner()).hits
    }

    fn release(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(|err| err.into_inner());
        state.released = true;
        cvar.notify_all();
    }
}

impl DatabasePlugin for ApplyBlocker {
    fn on_sync_pull(&self, changeset: &mut ChangeSet) -> contextdb_core::Result<()> {
        if changeset.rows.len() < self.min_rows {
            return Ok(());
        }

        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(|err| err.into_inner());
        state.hit = true;
        state.hits += 1;
        cvar.notify_all();
        while !state.released {
            state = cvar.wait(state).unwrap_or_else(|err| err.into_inner());
        }
        Ok(())
    }
}

fn tenant(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn setup_items_db() -> Database {
    setup_items_db_with_conflict_declaration("KEEP FIRST")
}

fn setup_items_db_with_conflict_declaration(declaration: &str) -> Database {
    let db = Database::open_memory();
    db.execute(
        &format!(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, payload TEXT) SYNC CONFLICT {declaration}"
        ),
        &HashMap::new(),
    )
    .expect("create items table");
    db
}

fn setup_items_db_with_blocker(blocker: ApplyBlocker) -> Database {
    setup_items_db_with_blocker_and_conflict_declaration(blocker, "KEEP FIRST")
}

fn setup_items_db_with_blocker_and_conflict_declaration(
    blocker: ApplyBlocker,
    declaration: &str,
) -> Database {
    let db = Database::open_memory_with_plugin(Arc::new(blocker)).expect("open memory db");
    db.execute(
        &format!(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, payload TEXT) SYNC CONFLICT {declaration}"
        ),
        &HashMap::new(),
    )
    .expect("create items table");
    db
}

fn insert_items_tx(db: &Database, rows: &[(Uuid, &str, &str)]) {
    let tx = db.begin_or_panic();
    for (id, name, payload) in rows {
        db.insert_row(
            tx,
            TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(*id)),
                ("name".to_string(), Value::Text((*name).to_string())),
                ("payload".to_string(), Value::Text((*payload).to_string())),
            ]),
        )
        .expect("insert seed item");
    }
    db.commit(tx).expect("commit seed items");
}

fn item_changeset(ids: &[Uuid], name_prefix: &str, payload_len: usize, lsn: Lsn) -> ChangeSet {
    let payload = "x".repeat(payload_len);
    let rows = ids
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            let name = format!("{name_prefix}-{idx}");
            RowChange {
                table: TABLE.to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(*id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(*id)),
                    ("name".to_string(), Value::Text(name)),
                    ("payload".to_string(), Value::Text(payload.clone())),
                ]),
                deleted: false,
                lsn,
                created_at: None,
            }
        })
        .collect();
    ChangeSet {
        rows,
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    }
}

fn two_row_conflict_changeset(existing_id: Uuid, fresh_id: Uuid, lsn: Lsn) -> ChangeSet {
    ChangeSet {
        rows: vec![
            RowChange {
                table: TABLE.to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(existing_id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(existing_id)),
                    (
                        "name".to_string(),
                        Value::Text("client-conflict".to_string()),
                    ),
                    ("payload".to_string(), Value::Text("client".to_string())),
                ]),
                deleted: false,
                lsn,
                created_at: None,
            },
            RowChange {
                table: TABLE.to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(fresh_id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(fresh_id)),
                    ("name".to_string(), Value::Text("client-fresh".to_string())),
                    ("payload".to_string(), Value::Text("client".to_string())),
                ]),
                deleted: false,
                lsn,
                created_at: None,
            },
        ],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    }
}

fn missing_table_changeset(id: Uuid) -> ChangeSet {
    ChangeSet {
        rows: vec![RowChange {
            table: "missing_items".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("bad".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(9000),
            created_at: None,
        }],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    }
}

fn valid_then_missing_table_changeset(valid_id: Uuid, missing_id: Uuid, lsn: Lsn) -> ChangeSet {
    ChangeSet {
        rows: vec![
            RowChange {
                table: TABLE.to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(valid_id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(valid_id)),
                    (
                        "name".to_string(),
                        Value::Text("valid-before-error".to_string()),
                    ),
                    ("payload".to_string(), Value::Text("valid".to_string())),
                ]),
                deleted: false,
                lsn,
                created_at: None,
            },
            RowChange {
                table: "missing_items".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(missing_id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(missing_id)),
                    (
                        "name".to_string(),
                        Value::Text("invalid-after-valid".to_string()),
                    ),
                ]),
                deleted: false,
                lsn,
                created_at: None,
            },
        ],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    }
}

type RawClient = Arc<dyn ClientTransport>;
type ReplyTask = tokio::task::JoinHandle<contextdb_server::transport::TransportResult<Vec<u8>>>;

/// The server liveness cases need to hold and replay an exact wire request,
/// but v6 refuses a hand-built row because its lineage must be signed by the
/// edge's real fabric identity. This wrapper lets a real `SyncClient` create
/// that request, records it, and returns a local acknowledgement so the test
/// can send the immutable bytes later at the chosen interleaving point.
struct CapturingSignedPushTransport {
    inner: RawClient,
    push_subject: String,
    captured: Mutex<Vec<Vec<u8>>>,
}

impl CapturingSignedPushTransport {
    fn new(inner: RawClient, tenant_id: &str) -> Self {
        Self {
            inner,
            push_subject: push_subject(tenant_id),
            captured: Mutex::new(Vec::new()),
        }
    }

    fn clear(&self) {
        self.captured
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clear();
    }

    fn take_push_for_rows(&self, ids: &HashSet<Uuid>) -> Vec<u8> {
        let mut captured = self.captured.lock().unwrap_or_else(|err| err.into_inner());
        let index = captured.iter().position(|bytes| {
            let envelope = decode(bytes).expect("captured SyncClient request envelope");
            let request: PushRequest =
                rmp_serde::from_slice(&envelope.payload).expect("captured SyncClient push payload");
            let actual_ids = request
                .changeset
                .rows
                .iter()
                .filter_map(|row| row.values.get("id").and_then(Value::as_uuid).copied())
                .collect::<HashSet<_>>();
            actual_ids == *ids && !request.changeset.rows.is_empty()
        });
        let bytes = captured.remove(index.unwrap_or_else(|| {
            panic!("SyncClient did not produce a signed push for rows {ids:?}")
        }));
        let envelope = decode(&bytes).expect("captured SyncClient request envelope");
        let mut request: PushRequest =
            rmp_serde::from_slice(&envelope.payload).expect("captured SyncClient push payload");
        assert!(
            request
                .changeset
                .rows
                .iter()
                .all(|row| row.lineage.is_some()),
            "captured v6 push must carry a signed lineage for every row"
        );
        // An edge-only `missing_items` declaration exists solely to make the
        // malformed-row cases signable. That raw probe must not smuggle the
        // declaration/provenance into a hub that intentionally lacks the
        // table. Every ordinary captured push stays byte-for-byte faithful to
        // the real client's schema-bearing request.
        if request
            .changeset
            .rows
            .iter()
            .any(|row| row.table == "missing_items")
        {
            request.changeset.ddl.clear();
            request.changeset.ddl_lsn.clear();
            request.changeset.ddl_provenance.clear();
            return encode(envelope.message_type, &request)
                .expect("re-encode captured signed push without edge-only DDL");
        }
        bytes
    }

    fn capture_or_forward<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject != self.push_subject {
            return self.inner.request(subject, request_bytes, timeout);
        }
        let result = (|| -> TransportResult<Vec<u8>> {
            let envelope = decode(&request_bytes)
                .map_err(|err| TransportError::Other(format!("decode captured push: {err}")))?;
            let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
                .map_err(|err| TransportError::Other(format!("decode captured push: {err}")))?;
            self.captured
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .push(request_bytes);
            encode(
                MessageType::PushResponse,
                &PushResponse {
                    result: Some(
                        ApplyResult {
                            applied_rows: request.changeset.rows.len(),
                            skipped_rows: 0,
                            conflicts: Vec::new(),
                            new_lsn: Lsn(1),
                        }
                        .into(),
                    ),
                    error: None,
                    application_error: None,
                },
            )
            .map_err(|err| TransportError::Other(format!("encode captured push reply: {err}")))
        })();
        Box::pin(async move { result })
    }
}

impl ClientTransport for CapturingSignedPushTransport {
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
        self.capture_or_forward(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

struct SignedRawClient {
    db: Arc<Database>,
    transport: RawClient,
    client: SyncClient,
    capture: Arc<CapturingSignedPushTransport>,
}

impl SignedRawClient {
    async fn new(fabric: &InProcessBroker, tenant_id: &str) -> Self {
        let db = Arc::new(Database::open_memory());
        let identity = Arc::new(FabricIdentity::generate());
        let node_id = identity.node_id();
        let transport = fabric.client_as(&node_id);
        let capture = Arc::new(CapturingSignedPushTransport::new(
            transport.clone(),
            tenant_id,
        ));
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            capture.clone(),
            contextdb_core::TenantId::from(tenant_id),
            identity,
        );
        client
            .pull_default()
            .await
            .expect("edge learns hub schema before signing a push");
        Self {
            db,
            transport,
            client,
            capture,
        }
    }

    async fn signed_push_bytes(&self, changeset: ChangeSet) -> Vec<u8> {
        self.stage_rows(&changeset);
        let ids = changeset
            .rows
            .iter()
            .filter_map(|row| row.values.get("id").and_then(Value::as_uuid).copied())
            .collect::<HashSet<_>>();
        assert!(!ids.is_empty(), "fixture signs at least one row");
        self.capture.clear();
        self.client
            .push()
            .await
            .expect("capturing transport acknowledges the signed fixture push");
        self.capture.take_push_for_rows(&ids)
    }

    fn stage_rows(&self, changeset: &ChangeSet) {
        if changeset
            .rows
            .iter()
            .any(|row| row.table == "missing_items")
        {
            self.db
                .execute(
                    "CREATE TABLE missing_items (id UUID PRIMARY KEY, name TEXT) SYNC CONFLICT KEEP FIRST",
                    &HashMap::new(),
                )
                .expect("create edge-only missing_items fixture table");
        }
        let tx = self.db.begin_or_panic();
        for row in &changeset.rows {
            assert!(!row.deleted, "this fixture materializes only row inserts");
            self.db
                .upsert_row(tx, &row.table, "id", row.values.clone())
                .expect("stage signed edge fixture row");
        }
        self.db.commit(tx).expect("commit signed edge fixture rows");
    }
}

async fn raw_push_with_reply(
    client: &SignedRawClient,
    tenant_id: &str,
    changeset: ChangeSet,
) -> ReplyTask {
    let encoded = client.signed_push_bytes(changeset).await;
    raw_encoded_push_with_reply(&client.transport, tenant_id, encoded).await
}

async fn raw_encoded_push_with_reply(
    client: &RawClient,
    tenant_id: &str,
    encoded: Vec<u8>,
) -> ReplyTask {
    let client = client.clone();
    let subject = push_subject(tenant_id);
    tokio::spawn(async move {
        client
            .request(&subject, encoded, Duration::from_secs(30))
            .await
    })
}

async fn next_push_response(
    reply_task: &mut ReplyTask,
    timeout: Duration,
) -> Result<PushResponse, tokio::time::error::Elapsed> {
    tokio::time::timeout(timeout, async {
        let payload = (&mut *reply_task)
            .await
            .expect("push reply task must not panic")
            .expect("push transport request must succeed");
        let envelope = decode(&payload).expect("decode push response envelope");
        assert_eq!(
            envelope.message_type,
            MessageType::PushResponse,
            "push reply envelope type"
        );
        rmp_serde::from_slice(&envelope.payload).expect("decode push response payload")
    })
    .await
}

async fn assert_no_push_response(
    reply_task: &mut ReplyTask,
    timeout: Duration,
    blocker: &ApplyBlocker,
    context: &str,
) {
    let response = next_push_response(reply_task, timeout).await;
    if response.is_ok() {
        blocker.release();
    }
    assert!(
        response.is_err(),
        "{context}: parked apply must not send an early/fake push response before release"
    );
}

fn successful_apply(response: PushResponse) -> ApplyResult {
    if let Some(err) = response.error {
        panic!("push returned error: {err}");
    }
    response
        .result
        .expect("push response must include result")
        .into()
}

async fn try_raw_pull(
    client: &RawClient,
    tenant_id: &str,
    since_lsn: Lsn,
) -> Result<PullResponse, String> {
    let request = PullRequest {
        since_lsn,
        max_entries: None,
    };
    let encoded = encode(MessageType::PullRequest, &request)
        .map_err(|err| format!("encode pull request: {err}"))?;
    let payload = client
        .request(&pull_subject(tenant_id), encoded, Duration::from_secs(30))
        .await
        .map_err(|err| format!("pull request: {err}"))?;
    let envelope =
        decode(&payload).map_err(|err| format!("decode pull response envelope: {err}"))?;
    match envelope.message_type {
        MessageType::PullResponse => rmp_serde::from_slice(&envelope.payload)
            .map_err(|err| format!("decode pull response payload: {err}")),
        MessageType::DependencyCompletePullResponse => {
            let response: DependencyCompletePullResponse = rmp_serde::from_slice(&envelope.payload)
                .map_err(|err| format!("decode dependency-complete pull payload: {err}"))?;
            Ok(response.ordinary)
        }
        other => Err(format!("unexpected pull response type: {other:?}")),
    }
}

async fn raw_pull(client: &RawClient, tenant_id: &str, since_lsn: Lsn) -> PullResponse {
    try_raw_pull(client, tenant_id, since_lsn)
        .await
        .expect("raw pull must succeed")
}

async fn wait_for_server_ready(fabric: &InProcessBroker, tenant_id: &str) {
    tokio::time::timeout(
        Duration::from_secs(5),
        fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
            tenant_id,
        )),
    )
    .await
    .unwrap_or_else(|_| panic!("sync server did not become ready for tenant {tenant_id}"));
}

struct ServerTask {
    shutdown: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ServerTask {
    fn abort(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    async fn join(mut self) -> std::thread::Result<()> {
        let thread = self.thread.take().expect("server thread must exist");
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .expect("server join task must not panic")
    }
}

fn run_server(db: Arc<Database>, fabric: Arc<InProcessBroker>, tenant_id: &str) -> ServerTask {
    run_server_until(db, fabric, tenant_id, Arc::new(AtomicBool::new(false)))
}

fn run_server_until(
    db: Arc<Database>,
    fabric: Arc<InProcessBroker>,
    tenant_id: &str,
    shutdown: Arc<AtomicBool>,
) -> ServerTask {
    let tenant_id = tenant_id.to_string();
    let thread_shutdown = shutdown.clone();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("server test runtime");
        runtime.block_on(async move {
            let server = SyncServer::with_authenticated_transport_and_identity_for_test(
                db,
                fabric.server_as(&hub_node_id),
                contextdb_core::TenantId::from(&tenant_id),
                hub_node_id,
                hub_identity,
            );
            server.run_until(thread_shutdown).await;
        });
    });
    ServerTask {
        shutdown,
        thread: Some(thread),
    }
}

fn visible_item_ids(db: &Database, ids: &[Uuid]) -> HashSet<Uuid> {
    let snapshot = db.snapshot();
    visible_item_ids_at_snapshot(db, ids, snapshot)
}

fn visible_item_ids_at_snapshot(
    db: &Database,
    ids: &[Uuid],
    snapshot: contextdb_core::SnapshotId,
) -> HashSet<Uuid> {
    ids.iter()
        .copied()
        .filter(|id| {
            db.point_lookup(TABLE, "id", &Value::Uuid(*id), snapshot)
                .expect("point lookup")
                .is_some()
        })
        .collect()
}

fn visible_item_count(db: &Database, ids: &[Uuid]) -> usize {
    visible_item_ids(db, ids).len()
}

fn item_name(db: &Database, id: Uuid) -> String {
    let row = db
        .point_lookup(TABLE, "id", &Value::Uuid(id), db.snapshot())
        .expect("point lookup")
        .unwrap_or_else(|| panic!("expected row {id}"));
    row.values
        .get("name")
        .and_then(Value::as_text)
        .expect("row name text")
        .to_string()
}

fn pull_row_ids(response: &PullResponse) -> HashSet<Uuid> {
    response
        .changeset
        .rows
        .iter()
        .filter_map(|row| row.values.get("id").and_then(Value::as_uuid).copied())
        .collect()
}

fn new_ids(n: usize) -> Vec<Uuid> {
    (0..n).map(|_| Uuid::new_v4()).collect()
}

fn insert_large_edge_rows(db: &Database, count: usize, payload_len: usize) -> Vec<Uuid> {
    let ids = new_ids(count);
    let payload = "p".repeat(payload_len);
    let tx = db.begin_or_panic();
    for (idx, id) in ids.iter().enumerate() {
        db.insert_row(
            tx,
            TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(*id)),
                ("name".to_string(), Value::Text(format!("large-{idx}"))),
                ("payload".to_string(), Value::Text(payload.clone())),
            ]),
        )
        .expect("insert large edge row");
    }
    db.commit(tx).expect("commit large edge rows");
    ids
}

fn assert_client_push_fixture_uses_chunking(db: &Database) {
    let changeset = db.changes_since(Lsn(0));
    let batches = contextdb_server::split_changeset_for_test(changeset);
    assert!(
        batches.iter().any(|batch| {
            let request = PushRequest {
                changeset: batch.clone().into(),
                incarnation: contextdb_core::Incarnation::default(),
            };
            let encoded =
                encode(MessageType::PushRequest, &request).expect("encode candidate batch");
            contextdb_server::chunking::needs_chunking(&encoded)
        }),
        "large push fixture must exercise SyncClient's chunked push path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss01_pull_answered_during_parked_push_apply() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss01");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let seed_id = Uuid::new_v4();
    insert_items_tx(&server_db, &[(seed_id, "seed", "seed-payload")]);
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&parked_ids, "parked", 0, Lsn(1000)),
    )
    .await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "oversized push must enter the engine apply blocker"
    );
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-01 parked push",
    )
    .await;

    let pull = tokio::time::timeout(
        LIVENESS_TIMEOUT,
        raw_pull(&signed_client.transport, &tenant_id, Lsn(0)),
    )
    .await;
    if pull.is_err() {
        blocker.release();
        let _ = next_push_response(&mut parked_reply, LIVENESS_TIMEOUT).await;
        server.abort();
    }
    assert!(
        pull.is_ok(),
        "pull must be answered while the oversized push apply is parked"
    );
    let pulled_ids = pull_row_ids(&pull.expect("checked ok"));
    let expected_pulled_ids = HashSet::from([seed_id]);
    if pulled_ids != expected_pulled_ids || parked_ids.iter().any(|id| pulled_ids.contains(id)) {
        blocker.release();
    }
    assert_eq!(
        pulled_ids, expected_pulled_ids,
        "pull must expose only the already committed seed row while parked"
    );
    assert!(
        parked_ids.iter().all(|id| !pulled_ids.contains(id)),
        "parked rows must not be visible before their apply completes"
    );

    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-01 parked push before release",
    )
    .await;
    blocker.release();
    let result = successful_apply(
        next_push_response(&mut parked_reply, LIVENESS_TIMEOUT)
            .await
            .expect("parked push reply after release"),
    );
    assert_eq!(result.applied_rows, 50);
    assert_eq!(result.skipped_rows, 0);
    assert!(result.conflicts.is_empty());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss02_second_push_answered_during_parked_push_apply() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss02");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&parked_ids, "parked", 0, Lsn(2000)),
    )
    .await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "first push must enter the engine apply blocker"
    );
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-02 parked first push",
    )
    .await;

    let second_ids = new_ids(3);
    let mut second_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&second_ids, "second", 0, Lsn(2001)),
    )
    .await;
    let second = next_push_response(&mut second_reply, LIVENESS_TIMEOUT).await;
    if second.is_err() {
        blocker.release();
        let _ = next_push_response(&mut parked_reply, LIVENESS_TIMEOUT).await;
        server.abort();
    }
    assert!(
        second.is_ok(),
        "second client's push must receive a reply while first apply is parked"
    );
    let second_result = successful_apply(second.expect("checked ok"));
    assert_eq!(second_result.applied_rows, 3);
    assert_eq!(second_result.skipped_rows, 0);
    assert!(second_result.conflicts.is_empty());
    assert_eq!(
        visible_item_ids(&server_db, &second_ids),
        HashSet::from_iter(second_ids.clone())
    );
    let parked_visible = visible_item_ids(&server_db, &parked_ids);
    if !parked_visible.is_empty() {
        blocker.release();
    }
    assert!(
        parked_visible.is_empty(),
        "first push rows must remain invisible before release"
    );

    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-02 parked first push before release",
    )
    .await;
    blocker.release();
    let parked_result = successful_apply(
        next_push_response(&mut parked_reply, LIVENESS_TIMEOUT)
            .await
            .expect("parked push reply after release"),
    );
    assert_eq!(parked_result.applied_rows, 50);
    assert_eq!(visible_item_count(&server_db, &parked_ids), 50);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss09_duplicate_retry_joins_parked_apply_without_reapplying() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss09_retry");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let ids = new_ids(50);
    let encoded = signed_client
        .signed_push_bytes(item_changeset(&ids, "retry", 0, Lsn(2500)))
        .await;

    let mut first_reply =
        raw_encoded_push_with_reply(&signed_client.transport, &tenant_id, encoded.clone()).await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "first duplicate push must enter the engine apply blocker"
    );
    assert_no_push_response(
        &mut first_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-09 duplicate first push",
    )
    .await;

    let mut retry_reply =
        raw_encoded_push_with_reply(&signed_client.transport, &tenant_id, encoded).await;
    assert_no_push_response(
        &mut retry_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-09 duplicate retry before release",
    )
    .await;
    assert_eq!(
        blocker.hit_count(),
        1,
        "duplicate retry must attach to the in-flight apply instead of entering engine apply again"
    );

    blocker.release();
    let first_result = successful_apply(
        next_push_response(&mut first_reply, LIVENESS_TIMEOUT)
            .await
            .expect("first duplicate reply after release"),
    );
    let retry_result = successful_apply(
        next_push_response(&mut retry_reply, LIVENESS_TIMEOUT)
            .await
            .expect("retry duplicate reply after release"),
    );
    assert_eq!(first_result.applied_rows, 50);
    assert_eq!(
        retry_result.applied_rows, 50,
        "duplicate retry must receive the original apply result, not a second idempotent reapply result"
    );
    assert_eq!(
        blocker.hit_count(),
        1,
        "duplicate retry must not apply again after the original completes"
    );
    assert_eq!(visible_item_count(&server_db, &ids), 50);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss03_replies_not_crossed_and_carry_own_apply_result() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss03");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker_and_conflict_declaration(
        blocker.clone(),
        "KEEP FIRST",
    ));
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let existing_id = Uuid::new_v4();
    let existing_peer = Uuid::new_v4();
    let mut seed_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&[existing_id, existing_peer], "server-seed", 0, Lsn(3000)),
    )
    .await;
    let seed_result = successful_apply(
        next_push_response(&mut seed_reply, LIVENESS_TIMEOUT)
            .await
            .expect("seed push reply"),
    );
    assert_eq!(seed_result.applied_rows, 2);
    assert_eq!(item_name(&server_db, existing_id), "server-seed-0");

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&parked_ids, "parked", 0, Lsn(3001)),
    )
    .await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "parked push must enter the engine apply blocker"
    );
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-03 parked first push",
    )
    .await;

    let fresh_id = Uuid::new_v4();
    let mut second_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        two_row_conflict_changeset(existing_id, fresh_id, Lsn(3002)),
    )
    .await;
    let second = next_push_response(&mut second_reply, LIVENESS_TIMEOUT).await;
    if second.is_err() {
        blocker.release();
        let _ = next_push_response(&mut parked_reply, LIVENESS_TIMEOUT).await;
        server.abort();
    }
    assert!(
        second.is_ok(),
        "second push reply must not be blocked behind the parked push"
    );
    let second_result = successful_apply(second.expect("checked ok"));
    assert_eq!(second_result.applied_rows, 1);
    assert_eq!(second_result.skipped_rows, 1);
    assert_eq!(
        second_result.conflicts.len(),
        1,
        "KEEP FIRST must report the reused row as the second push's one arbitration conflict"
    );
    assert_eq!(
        second_result.conflicts[0].natural_key.value,
        Value::Uuid(existing_id),
        "the second reply must describe its own reused row, never the parked request"
    );
    assert_eq!(
        item_name(&server_db, existing_id),
        "server-seed-0",
        "KEEP FIRST must leave the reused row's server value intact"
    );
    assert_eq!(item_name(&server_db, fresh_id), "client-fresh");

    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-03 parked first push before release",
    )
    .await;
    blocker.release();
    let parked_result = successful_apply(
        next_push_response(&mut parked_reply, LIVENESS_TIMEOUT)
            .await
            .expect("parked push reply after release"),
    );
    assert_eq!(parked_result.applied_rows, 50);
    assert_eq!(parked_result.skipped_rows, 0);
    assert!(parked_result.conflicts.is_empty());
    assert_ne!(
        parked_result.new_lsn, second_result.new_lsn,
        "distinct pushes must report their own commit LSNs"
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss04_parked_apply_visibility_is_atomic() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss04");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let valid_id = Uuid::new_v4();
    let mut failing_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        valid_then_missing_table_changeset(valid_id, Uuid::new_v4(), Lsn(3999)),
    )
    .await;
    let failing_response = next_push_response(&mut failing_reply, LIVENESS_TIMEOUT)
        .await
        .expect("failing mixed push reply");
    assert!(
        failing_response.result.is_none(),
        "mixed valid/invalid push must not report success"
    );
    assert!(
        failing_response
            .error
            .as_deref()
            .is_some_and(|err| err.contains("missing_items")),
        "mixed push error must identify the missing table: {failing_response:?}"
    );
    assert_eq!(
        visible_item_count(&server_db, &[valid_id]),
        0,
        "a failed batch must roll back the valid row that preceded the error"
    );

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&parked_ids, "atomic", 0, Lsn(4000)),
    )
    .await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "parked push must enter the engine apply blocker"
    );
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-04 parked push",
    )
    .await;
    let parked_visible_count = visible_item_count(&server_db, &parked_ids);
    if parked_visible_count != 0 {
        blocker.release();
    }
    assert_eq!(
        parked_visible_count, 0,
        "the blocker is before mutation, so none of the parked rows may be visible"
    );

    let reader_db = server_db.clone();
    let reader_ids = parked_ids.clone();
    let reader = tokio::spawn(async move {
        let mut counts = Vec::new();
        for _ in 0..200 {
            let count = visible_item_count(&reader_db, &reader_ids);
            counts.push(count);
            if count == reader_ids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        counts
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-04 parked push before release",
    )
    .await;
    blocker.release();
    let result = successful_apply(
        next_push_response(&mut parked_reply, LIVENESS_TIMEOUT)
            .await
            .expect("parked push reply after release"),
    );
    assert_eq!(result.applied_rows, 50);
    let counts = reader.await.expect("reader task");
    assert!(
        counts.iter().all(|count| *count == 0 || *count == 50),
        "reader must observe only none-or-all visibility counts, got {counts:?}"
    );
    assert_eq!(visible_item_count(&server_db, &parked_ids), 50);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss05_conflict_policy_parity_matches_inline_apply() {
    let fabric = Arc::new(InProcessBroker::new());
    let existing_id = Uuid::from_u128(1);
    let fresh_id = Uuid::from_u128(2);
    for (declaration, policy) in [
        ("KEEP FIRST", ConflictPolicy::InsertIfNotExists),
        ("KEEP LATEST", ConflictPolicy::LatestWins),
    ] {
        let inline_db = setup_items_db_with_conflict_declaration(declaration);
        insert_items_tx(&inline_db, &[(existing_id, "server-value", "server")]);
        let expected = inline_db
            .apply_changes(
                two_row_conflict_changeset(existing_id, fresh_id, Lsn(5000)),
                &ConflictPolicies::uniform(policy),
            )
            .unwrap_or_else(|err| panic!("apply_changes under {policy:?}: {err}"));

        let tenant_id = tenant(&format!("ss05_{}", declaration.replace(' ', "_")));
        let server_db = Arc::new(setup_items_db_with_conflict_declaration(declaration));
        insert_items_tx(&server_db, &[(existing_id, "server-value", "server")]);
        let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
        wait_for_server_ready(&fabric, &tenant_id).await;
        let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;
        let mut reply = raw_push_with_reply(
            &signed_client,
            &tenant_id,
            two_row_conflict_changeset(existing_id, fresh_id, Lsn(5000)),
        )
        .await;
        let actual = successful_apply(
            next_push_response(&mut reply, LIVENESS_TIMEOUT)
                .await
                .expect("server conflict push reply"),
        );

        // The fresh row never collides locally on either surface (the
        // `(None, _)` apply arm), so it inserts identically regardless of
        // policy or which leg (push vs. receipt-less inline) applied it.
        assert_eq!(
            item_name(&server_db, fresh_id),
            item_name(&inline_db, fresh_id)
        );

        match policy {
            ConflictPolicy::InsertIfNotExists => {
                // Narrowed per the declared-policy pull-adoption contract
                // (Settled Policy 9 / §3.2): full push/inline parity no
                // longer holds for KEEP FIRST. The hub's one-decision
                // contract governs the PUSH leg (this server exchange,
                // carrying an authenticated receipt) -- it still refuses the
                // conflicting row exactly as before. The receipt-less
                // inline `apply_changes` call above now stands in for the
                // PULL leg, which ADOPTS the already-established value
                // instead of silently keeping the caller's own losing one.
                // The two surfaces are asserted explicitly below rather than
                // compared for equality.

                // Receipt-less side (pull leg): adopts the existing row and
                // reports the typed hub-adoption diagnostic.
                assert_eq!(
                    expected.applied_rows, 2,
                    "the receipt-less apply adopts the existing row and inserts \
                     the fresh one: {expected:?}"
                );
                assert_eq!(
                    expected.skipped_rows, 0,
                    "adoption is not a refusal: {expected:?}"
                );
                assert_eq!(
                    expected.conflicts.len(),
                    1,
                    "exactly one hub-adoption diagnostic: {expected:?}"
                );
                let inline_conflict = &expected.conflicts[0];
                assert_eq!(inline_conflict.natural_key.value, Value::Uuid(existing_id));
                assert_eq!(
                    inline_conflict.resolution,
                    ConflictPolicy::InsertIfNotExists
                );
                assert_eq!(
                    inline_conflict.reason.as_deref(),
                    Some("keep_first_hub_adopted"),
                    "the reason must name hub adoption, not a bare refusal: {inline_conflict:?}"
                );
                assert_eq!(
                    inline_conflict.table.as_deref(),
                    Some(TABLE),
                    "{inline_conflict:?}"
                );
                assert_eq!(
                    inline_conflict.mutation_kind.as_deref(),
                    Some("edit"),
                    "{inline_conflict:?}"
                );
                assert_eq!(
                    item_name(&inline_db, existing_id),
                    "client-conflict",
                    "the receipt-less apply adopts the incoming value over its own"
                );

                // Push side: the hub's one-decision refusal contract, now
                // carrying the same complete typed diagnostic shape as the
                // pull leg's adoption (observed at HEAD: reason
                // "dependency_complete_refused", not the old bare
                // "keep_first" -- an authenticated push is a dependency-
                // complete unit, so its refusal reports the established hub
                // winner instead of a bare policy tag).
                assert_eq!(actual.applied_rows, 1);
                assert_eq!(actual.skipped_rows, 1);
                assert_eq!(actual.conflicts.len(), 1);
                let push_conflict = &actual.conflicts[0];
                assert_eq!(push_conflict.natural_key.value, Value::Uuid(existing_id));
                assert_eq!(push_conflict.resolution, ConflictPolicy::InsertIfNotExists);
                assert_eq!(
                    push_conflict.reason.as_deref(),
                    Some("dependency_complete_refused"),
                    "the push leg keeps the hub's one-decision refusal contract: {actual:?}"
                );
                assert_eq!(push_conflict.table.as_deref(), Some(TABLE), "{actual:?}");
                assert_eq!(
                    push_conflict.mutation_kind.as_deref(),
                    Some("edit"),
                    "{actual:?}"
                );
                assert!(
                    push_conflict.winning_author_node_id.is_some(),
                    "the refused push names the hub's established winner: {actual:?}"
                );
                assert!(
                    push_conflict.hub_acceptance_position.is_some(),
                    "the refused push names the hub's acceptance position: {actual:?}"
                );
                assert_eq!(item_name(&server_db, existing_id), "server-value");
            }
            ConflictPolicy::LatestWins => {
                // Unaffected by the pull-adoption contract: full parity
                // still holds for this policy.
                assert_eq!(actual.applied_rows, expected.applied_rows);
                assert_eq!(actual.skipped_rows, expected.skipped_rows);
                assert_eq!(actual.applied_rows, 2);
                assert_eq!(actual.skipped_rows, 0);
                assert!(actual.conflicts.is_empty());
                assert_eq!(item_name(&server_db, existing_id), "client-conflict");
                assert_eq!(
                    item_name(&server_db, existing_id),
                    item_name(&inline_db, existing_id)
                );
            }
            ConflictPolicy::ServerWins | ConflictPolicy::EdgeWins => {
                unreachable!("only schema-declarable conflict policies are under test")
            }
        }
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss06a_chunked_apply_correctness_preserved() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss06a");
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(setup_items_db());
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        fabric.client_as(&edge_node_id),
        contextdb_core::TenantId::from(&tenant_id),
        edge_identity,
    );
    client.pull_default().await.expect("edge learns hub schema");
    let ids = insert_large_edge_rows(&edge_db, 2, 600_000);
    assert_client_push_fixture_uses_chunking(&edge_db);

    let result = client.push().await.expect("chunked push must succeed");
    assert_eq!(result.applied_rows, 2);
    assert_eq!(result.skipped_rows, 0);
    assert!(result.conflicts.is_empty());
    assert_eq!(
        visible_item_ids(&server_db, &ids),
        HashSet::from_iter(ids.clone())
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss06b_chunked_apply_does_not_freeze_event_loop() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss06b");
    let edge_db = Arc::new(Database::open_memory());
    let blocker = ApplyBlocker::new(2);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let transport_client = fabric.client_as(&edge_node_id);
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        transport_client.clone(),
        contextdb_core::TenantId::from(&tenant_id),
        edge_identity,
    );
    client.pull_default().await.expect("edge learns hub schema");
    let ids = insert_large_edge_rows(&edge_db, 2, 600_000);
    assert_client_push_fixture_uses_chunking(&edge_db);
    let push_task = tokio::spawn(async move { client.push().await });

    let hit = blocker.wait_until_hit(BARRIER_TIMEOUT).await;
    if !hit {
        server.abort();
    }
    assert!(
        hit,
        "chunked push must enter the same engine apply blocker as single-message push"
    );
    let chunked_still_parked = !push_task.is_finished();
    if !chunked_still_parked {
        blocker.release();
    }
    assert!(
        chunked_still_parked,
        "chunked push must not send an early/fake reply before parked apply release"
    );
    let visible_while_parked = visible_item_count(&server_db, &ids);
    if visible_while_parked != 0 {
        blocker.release();
    }
    assert_eq!(
        visible_while_parked, 0,
        "chunked rows must not be visible while their apply is parked"
    );

    let pull = tokio::time::timeout(
        LIVENESS_TIMEOUT,
        raw_pull(&transport_client, &tenant_id, Lsn(0)),
    )
    .await;
    if pull.is_err() {
        blocker.release();
        server.abort();
    }
    assert!(
        pull.is_ok(),
        "pull must be answered while a chunked apply is parked"
    );
    let chunked_still_parked_before_release = !push_task.is_finished();
    if !chunked_still_parked_before_release {
        blocker.release();
    }
    assert!(
        chunked_still_parked_before_release,
        "chunked push must not send an early/fake reply before parked apply release"
    );
    blocker.release();
    let result = push_task
        .await
        .expect("chunked push task")
        .expect("chunked push result");
    assert_eq!(result.applied_rows, 2);
    assert_eq!(visible_item_ids(&server_db, &ids), HashSet::from_iter(ids));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss07_error_reply_delivered_without_freezing_peers() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss07");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(server_db.clone(), fabric.clone(), &tenant_id);
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&parked_ids, "parked", 0, Lsn(7000)),
    )
    .await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "parked push must enter the engine apply blocker"
    );
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-07 parked push",
    )
    .await;

    let mut error_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        missing_table_changeset(Uuid::new_v4()),
    )
    .await;
    let error_response = next_push_response(&mut error_reply, LIVENESS_TIMEOUT).await;
    if error_response.is_err() {
        blocker.release();
        let _ = next_push_response(&mut parked_reply, LIVENESS_TIMEOUT).await;
        server.abort();
    }
    assert!(
        error_response.is_ok(),
        "apply errors from other pushes must still receive replies while a peer apply is parked"
    );
    let response = error_response.expect("checked ok");
    assert!(
        response.result.is_none(),
        "error push must not include a successful result"
    );
    let err = response.error.expect("error push must carry error text");
    assert!(
        err.contains("missing_items"),
        "error text must identify the failing table, got {err:?}"
    );

    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-07 parked push before release",
    )
    .await;
    blocker.release();
    let parked_result = successful_apply(
        next_push_response(&mut parked_reply, LIVENESS_TIMEOUT)
            .await
            .expect("parked push reply after release"),
    );
    assert_eq!(parked_result.applied_rows, 50);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss08_accepted_push_reply_is_delivered_during_shutdown_drain() {
    let fabric = Arc::new(InProcessBroker::new());
    let tenant_id = tenant("ss08");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let server = run_server_until(
        server_db.clone(),
        fabric.clone(),
        &tenant_id,
        shutdown.clone(),
    );
    wait_for_server_ready(&fabric, &tenant_id).await;
    let signed_client = SignedRawClient::new(&fabric, &tenant_id).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &signed_client,
        &tenant_id,
        item_changeset(&parked_ids, "shutdown", 0, Lsn(8000)),
    )
    .await;
    assert!(
        blocker.wait_until_hit(BARRIER_TIMEOUT).await,
        "accepted push must enter the engine apply blocker before shutdown"
    );
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-08 parked push",
    )
    .await;
    let shutdown_visible_count = visible_item_count(&server_db, &parked_ids);
    if shutdown_visible_count != 0 {
        blocker.release();
    }
    assert_eq!(
        shutdown_visible_count, 0,
        "accepted push rows must remain invisible until release"
    );
    shutdown.store(true, Ordering::SeqCst);
    assert_no_push_response(
        &mut parked_reply,
        NO_EARLY_REPLY_TIMEOUT,
        &blocker,
        "SS-08 parked push before release",
    )
    .await;
    blocker.release();
    let response = next_push_response(&mut parked_reply, LIVENESS_TIMEOUT).await;
    assert!(
        response.is_ok(),
        "accepted push must deliver its reply during shutdown drain"
    );
    let result = successful_apply(response.expect("checked ok"));
    assert_eq!(result.applied_rows, 50);
    assert_eq!(visible_item_count(&server_db, &parked_ids), 50);
    tokio::time::timeout(LIVENESS_TIMEOUT, server.join())
        .await
        .expect("server run_until should exit after shutdown")
        .expect("server thread should not panic");
}
