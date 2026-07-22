use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::plugin::DatabasePlugin;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
};
use contextdb_server::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, decode, encode,
};
use contextdb_server::subjects::{pull_subject, push_subject};
use contextdb_server::{SyncClient, SyncServer};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

const LIVENESS_TIMEOUT: Duration = Duration::from_secs(5);
const BARRIER_TIMEOUT: Duration = Duration::from_secs(5);
const NO_EARLY_REPLY_TIMEOUT: Duration = Duration::from_millis(250);
const TABLE: &str = "items";

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
}

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

async fn start_nats() -> NatsFixture {
    let nats_conf = format!(
        "{}/../contextdb-server/tests/nats.conf",
        env!("CARGO_MANIFEST_DIR")
    );
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(&nats_conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container = request.start().await.expect("start NATS container");
    let nats_port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS port");
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

fn tenant(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn setup_items_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create items table");
    db
}

fn setup_items_db_with_blocker(blocker: ApplyBlocker) -> Database {
    let db = Database::open_memory_with_plugin(Arc::new(blocker)).expect("open memory db");
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, payload TEXT)",
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

async fn nats_client(nats_url: &str) -> async_nats::Client {
    async_nats::connect(nats_url)
        .await
        .expect("connect to NATS")
}

async fn raw_push_with_reply(
    client: &async_nats::Client,
    tenant_id: &str,
    changeset: ChangeSet,
) -> async_nats::Subscriber {
    let request = PushRequest {
        changeset: changeset.into(),
        incarnation: contextdb_core::Incarnation::default(),
    };
    let encoded = encode(MessageType::PushRequest, &request).expect("encode push request");
    raw_encoded_push_with_reply(client, tenant_id, encoded).await
}

async fn raw_encoded_push_with_reply(
    client: &async_nats::Client,
    tenant_id: &str,
    encoded: Vec<u8>,
) -> async_nats::Subscriber {
    let inbox = client.new_inbox();
    let inbox_sub = client
        .subscribe(inbox.clone())
        .await
        .expect("subscribe inbox");
    client
        .publish_with_reply(push_subject(tenant_id), inbox, encoded.into())
        .await
        .expect("publish push request");
    client.flush().await.expect("flush push request");
    inbox_sub
}

async fn next_push_response(
    inbox_sub: &mut async_nats::Subscriber,
    timeout: Duration,
) -> Result<PushResponse, tokio::time::error::Elapsed> {
    tokio::time::timeout(timeout, async {
        let msg = inbox_sub
            .next()
            .await
            .expect("push response inbox must stay open");
        assert!(
            msg.status.is_none(),
            "push response must not be a NATS status message: {:?}",
            msg.status
        );
        let envelope = decode(&msg.payload).expect("decode push response envelope");
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
    inbox_sub: &mut async_nats::Subscriber,
    timeout: Duration,
    blocker: &ApplyBlocker,
    context: &str,
) {
    let response = next_push_response(inbox_sub, timeout).await;
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
    client: &async_nats::Client,
    tenant_id: &str,
    since_lsn: Lsn,
) -> Result<PullResponse, String> {
    let inbox = client.new_inbox();
    let mut inbox_sub = client
        .subscribe(inbox.clone())
        .await
        .map_err(|err| format!("subscribe pull inbox: {err}"))?;
    let request = PullRequest {
        since_lsn,
        max_entries: None,
    };
    let encoded = encode(MessageType::PullRequest, &request)
        .map_err(|err| format!("encode pull request: {err}"))?;
    client
        .publish_with_reply(pull_subject(tenant_id), inbox, encoded.into())
        .await
        .map_err(|err| format!("publish pull request: {err}"))?;
    client
        .flush()
        .await
        .map_err(|err| format!("flush pull request: {err}"))?;
    let msg = inbox_sub
        .next()
        .await
        .ok_or_else(|| "pull inbox closed".to_string())?;
    if let Some(status) = msg.status {
        return Err(format!("pull status reply: {status:?}"));
    }
    let envelope =
        decode(&msg.payload).map_err(|err| format!("decode pull response envelope: {err}"))?;
    if envelope.message_type != MessageType::PullResponse {
        return Err(format!(
            "unexpected pull response type: {:?}",
            envelope.message_type
        ));
    }
    rmp_serde::from_slice(&envelope.payload)
        .map_err(|err| format!("decode pull response payload: {err}"))
}

async fn raw_pull(client: &async_nats::Client, tenant_id: &str, since_lsn: Lsn) -> PullResponse {
    try_raw_pull(client, tenant_id, since_lsn)
        .await
        .expect("raw pull must succeed")
}

async fn wait_for_server_ready(client: &async_nats::Client, tenant_id: &str, since_lsn: Lsn) {
    for _ in 0..80 {
        if tokio::time::timeout(
            Duration::from_millis(250),
            try_raw_pull(client, tenant_id, since_lsn),
        )
        .await
        .is_ok_and(|result| result.is_ok())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("sync server did not become ready for tenant {tenant_id}");
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

fn run_server(
    db: Arc<Database>,
    nats_url: &str,
    tenant_id: &str,
    policies: ConflictPolicies,
) -> ServerTask {
    run_server_until(
        db,
        nats_url,
        tenant_id,
        policies,
        Arc::new(AtomicBool::new(false)),
    )
}

fn run_server_until(
    db: Arc<Database>,
    nats_url: &str,
    tenant_id: &str,
    policies: ConflictPolicies,
    shutdown: Arc<AtomicBool>,
) -> ServerTask {
    let nats_url = nats_url.to_string();
    let tenant_id = tenant_id.to_string();
    let thread_shutdown = shutdown.clone();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("server test runtime");
        runtime.block_on(async move {
            let server = SyncServer::new(
                db,
                &nats_url,
                contextdb_core::TenantId::from(&tenant_id),
                policies,
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss01");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let seed_id = Uuid::new_v4();
    insert_items_tx(&server_db, &[(seed_id, "seed", "seed-payload")]);
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &nats_client,
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

    let pull =
        tokio::time::timeout(LIVENESS_TIMEOUT, raw_pull(&nats_client, &tenant_id, Lsn(0))).await;
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss02");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &nats_client,
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
        &nats_client,
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss09_retry");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let ids = new_ids(50);
    let request = PushRequest {
        changeset: item_changeset(&ids, "retry", 0, Lsn(2500)).into(),
        incarnation: contextdb_core::Incarnation::default(),
    };
    let encoded = encode(MessageType::PushRequest, &request).expect("encode duplicate push");

    let mut first_reply =
        raw_encoded_push_with_reply(&nats_client, &tenant_id, encoded.clone()).await;
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

    let mut retry_reply = raw_encoded_push_with_reply(&nats_client, &tenant_id, encoded).await;
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss03");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let existing_id = Uuid::new_v4();
    let existing_peer = Uuid::new_v4();
    let mut seed_reply = raw_push_with_reply(
        &nats_client,
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
        &nats_client,
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
        &nats_client,
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
    assert_eq!(second_result.conflicts.len(), 1);
    assert_eq!(
        second_result.conflicts[0].resolution,
        ConflictPolicy::ServerWins
    );
    assert_eq!(
        second_result.conflicts[0].natural_key.value,
        Value::Uuid(existing_id)
    );
    assert_eq!(
        item_name(&server_db, existing_id),
        "server-seed-0",
        "ServerWins must leave the reused row's server value intact"
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss04");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let valid_id = Uuid::new_v4();
    let mut failing_reply = raw_push_with_reply(
        &nats_client,
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
        &nats_client,
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
    let nats = start_nats().await;
    let nats_client = nats_client(&nats.nats_url).await;
    let existing_id = Uuid::from_u128(1);
    let fresh_id = Uuid::from_u128(2);
    for policy in [
        ConflictPolicy::InsertIfNotExists,
        ConflictPolicy::ServerWins,
        ConflictPolicy::EdgeWins,
        ConflictPolicy::LatestWins,
    ] {
        let inline_db = setup_items_db();
        insert_items_tx(&inline_db, &[(existing_id, "server-value", "server")]);
        let expected = inline_db
            .apply_changes(
                two_row_conflict_changeset(existing_id, fresh_id, Lsn(5000)),
                &ConflictPolicies::uniform(policy),
            )
            .unwrap_or_else(|err| panic!("apply_changes under {policy:?}: {err}"));

        let tenant_id = tenant(&format!("ss05_{policy:?}"));
        let server_db = Arc::new(setup_items_db());
        insert_items_tx(&server_db, &[(existing_id, "server-value", "server")]);
        let server = run_server(
            server_db.clone(),
            &nats.nats_url,
            &tenant_id,
            ConflictPolicies::uniform(policy),
        );
        wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;
        let mut reply = raw_push_with_reply(
            &nats_client,
            &tenant_id,
            two_row_conflict_changeset(existing_id, fresh_id, Lsn(5000)),
        )
        .await;
        let actual = successful_apply(
            next_push_response(&mut reply, LIVENESS_TIMEOUT)
                .await
                .expect("server conflict push reply"),
        );

        assert_eq!(actual.applied_rows, expected.applied_rows);
        assert_eq!(actual.skipped_rows, expected.skipped_rows);
        assert_eq!(actual.new_lsn, expected.new_lsn);
        assert_eq!(actual.conflicts.len(), expected.conflicts.len());
        for (actual_conflict, expected_conflict) in
            actual.conflicts.iter().zip(expected.conflicts.iter())
        {
            assert_eq!(
                actual_conflict.natural_key.column,
                expected_conflict.natural_key.column
            );
            assert_eq!(
                actual_conflict.natural_key.value,
                expected_conflict.natural_key.value
            );
            assert_eq!(actual_conflict.resolution, expected_conflict.resolution);
        }

        match policy {
            ConflictPolicy::InsertIfNotExists => {
                assert_eq!(actual.applied_rows, 1);
                assert_eq!(actual.skipped_rows, 1);
                assert!(actual.conflicts.is_empty());
                assert_eq!(item_name(&server_db, existing_id), "server-value");
            }
            ConflictPolicy::ServerWins => {
                assert_eq!(actual.applied_rows, 1);
                assert_eq!(actual.skipped_rows, 1);
                assert_eq!(actual.conflicts.len(), 1);
                assert_eq!(actual.conflicts[0].resolution, ConflictPolicy::ServerWins);
                assert_eq!(
                    actual.conflicts[0].natural_key.value,
                    Value::Uuid(existing_id)
                );
                assert_eq!(item_name(&server_db, existing_id), "server-value");
            }
            ConflictPolicy::EdgeWins => {
                assert_eq!(actual.applied_rows, 2);
                assert_eq!(actual.skipped_rows, 0);
                assert_eq!(actual.conflicts.len(), 1);
                assert_eq!(actual.conflicts[0].resolution, ConflictPolicy::EdgeWins);
                assert_eq!(
                    actual.conflicts[0].natural_key.value,
                    Value::Uuid(existing_id)
                );
                assert_eq!(item_name(&server_db, existing_id), "client-conflict");
            }
            ConflictPolicy::LatestWins => {
                assert_eq!(actual.applied_rows, 2);
                assert_eq!(actual.skipped_rows, 0);
                assert!(actual.conflicts.is_empty());
                assert_eq!(item_name(&server_db, existing_id), "client-conflict");
            }
        }
        assert_eq!(
            item_name(&server_db, existing_id),
            item_name(&inline_db, existing_id)
        );
        assert_eq!(
            item_name(&server_db, fresh_id),
            item_name(&inline_db, fresh_id)
        );
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ss06a_chunked_apply_correctness_preserved() {
    let nats = start_nats().await;
    let tenant_id = tenant("ss06a");
    let edge_db = Arc::new(setup_items_db());
    let ids = insert_large_edge_rows(&edge_db, 2, 600_000);
    assert_client_push_fixture_uses_chunking(&edge_db);
    let server_db = Arc::new(Database::open_memory());
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, Lsn(0)).await;
    let client = SyncClient::new(
        edge_db,
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );

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
    let nats = start_nats().await;
    let tenant_id = tenant("ss06b");
    let edge_db = Arc::new(setup_items_db());
    let ids = insert_large_edge_rows(&edge_db, 2, 600_000);
    assert_client_push_fixture_uses_chunking(&edge_db);
    let blocker = ApplyBlocker::new(2);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, Lsn(0)).await;
    let client = SyncClient::new(
        edge_db,
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
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

    let pull =
        tokio::time::timeout(LIVENESS_TIMEOUT, raw_pull(&nats_client, &tenant_id, Lsn(0))).await;
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss07");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let server = run_server(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &nats_client,
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
        &nats_client,
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
    let nats = start_nats().await;
    let tenant_id = tenant("ss08");
    let blocker = ApplyBlocker::new(50);
    let server_db = Arc::new(setup_items_db_with_blocker(blocker.clone()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let server = run_server_until(
        server_db.clone(),
        &nats.nats_url,
        &tenant_id,
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        shutdown.clone(),
    );
    let nats_client = nats_client(&nats.nats_url).await;
    wait_for_server_ready(&nats_client, &tenant_id, server_db.current_lsn()).await;

    let parked_ids = new_ids(50);
    let mut parked_reply = raw_push_with_reply(
        &nats_client,
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
