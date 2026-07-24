use contextdb_core::{Error, Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
use contextdb_server::chunking::CHUNKING_THRESHOLD;
use contextdb_server::protocol::{
    Envelope, MessageType, PullRequest, PullResponse, PushRequest, PushResponse, SyncStatusRequest,
    SyncStatusResponse, WireApplyResult, WireChangeSet, decode, encode,
};
use contextdb_server::subjects::{pull_subject, push_subject, status_subject};
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer, split_changeset_for_test};
use serde::de::DeserializeOwned;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), fut)
        .await
        .expect("bounded transport operation exceeded 20s")
}

/// Construct an in-process fake hub over `broker` and immediately spawn its
/// serving loop, returning the hub's store, its shutdown flag, and the server
/// task. (`ip_00` deliberately spawns late to probe the pre-run no-responder
/// state, so it keeps its own bespoke bring-up.)
fn start_fake_hub(
    broker: &InProcessBroker,
    tenant: &str,
    policies: ConflictPolicies,
) -> (Arc<Database>, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let server_db = Arc::new(Database::open_memory());
    let server = Arc::new(SyncServer::with_transport(
        server_db.clone(),
        broker.server(),
        contextdb_core::TenantId::from(tenant),
        policies,
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    (server_db, shutdown, server_task)
}

fn column_index(columns: &[String], name: &str) -> usize {
    columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("result must project column {name}: {columns:?}"))
}

fn expect_text_value(db: &Database, table: &str, column: &str, id: Uuid, expected: &str) {
    let mut key = HashMap::new();
    key.insert("id".to_string(), Value::Uuid(id));
    let sql = format!("SELECT {column} FROM {table} WHERE id = $id");
    let result = db
        .execute(&sql, &key)
        .unwrap_or_else(|err| panic!("{table}.{column} lookup must succeed: {err}"));
    assert_eq!(
        result.rows.len(),
        1,
        "{table} must hold exactly the row {id}"
    );
    let idx = column_index(&result.columns, column);
    assert_eq!(
        result.rows[0][idx],
        Value::Text(expected.to_string()),
        "{table}.{column} for {id} must match"
    );
}

fn uuid_set(db: &Database, table: &str) -> BTreeSet<Uuid> {
    let result = db
        .execute(&format!("SELECT id FROM {table}"), &HashMap::new())
        .unwrap_or_else(|err| panic!("{table} id scan must succeed: {err}"));
    let id_idx = column_index(&result.columns, "id");
    result
        .rows
        .iter()
        .map(|row| match &row[id_idx] {
            Value::Uuid(id) => *id,
            other => panic!("{table}.id must be UUID, got {other:?}"),
        })
        .collect()
}

fn exchanges_on(broker: &InProcessBroker, subject: &str) -> Vec<(Envelope, Envelope)> {
    broker
        .recorded_exchanges()
        .into_iter()
        .filter(|exchange| exchange.subject == subject)
        .map(|exchange| {
            let request = decode(&exchange.request_bytes)
                .unwrap_or_else(|err| panic!("decode request bytes on {subject}: {err}"));
            let response = decode(&exchange.response_bytes)
                .unwrap_or_else(|err| panic!("decode response bytes on {subject}: {err}"));
            (request, response)
        })
        .collect()
}

fn payload<T: DeserializeOwned>(envelope: &Envelope, expected: MessageType, label: &str) -> T {
    assert_eq!(
        envelope.message_type, expected,
        "{label} envelope type must match"
    );
    rmp_serde::from_slice(&envelope.payload)
        .unwrap_or_else(|err| panic!("decode {label} payload: {err}"))
}

fn assert_status_exchange(
    exchange: &(Envelope, Envelope),
    expected_response: SyncStatusResponse,
    label: &str,
) {
    let request: SyncStatusRequest = payload(
        &exchange.0,
        MessageType::StatusRequest,
        &format!("{label} request"),
    );
    // The status request now carries the asking edge's per-life incarnation (its
    // value is minted per open, so it is not pinned here); the asking node id
    // still rides the authenticated connection, never the payload. Re-encoding
    // the decoded request must reproduce the exact wire bytes, proving the
    // payload is a faithful SyncStatusRequest and nothing else.
    let reencoded = rmp_serde::to_vec(&request).expect("re-encode status request payload");
    assert_eq!(
        reencoded, exchange.0.payload,
        "{label} request payload must round-trip exactly"
    );
    let response: SyncStatusResponse = payload(
        &exchange.1,
        MessageType::StatusResponse,
        &format!("{label} response"),
    );
    assert_eq!(response, expected_response, "{label} response payload");
}

fn assert_push_exchange(
    exchange: &(Envelope, Envelope),
    expected_changeset: WireChangeSet,
    expected_response: PushResponse,
    label: &str,
) {
    let request: PushRequest = payload(
        &exchange.0,
        MessageType::PushRequest,
        &format!("{label} request"),
    );
    assert_eq!(
        request.changeset, expected_changeset,
        "{label} request payload must be the expected sync batch"
    );
    let response: PushResponse = payload(
        &exchange.1,
        MessageType::PushResponse,
        &format!("{label} response"),
    );
    assert_eq!(
        response, expected_response,
        "{label} response payload must be the server apply result"
    );
}

fn assert_pull_exchange(
    exchange: &(Envelope, Envelope),
    expected_request: PullRequest,
    expected_response: PullResponse,
    label: &str,
) {
    let request: PullRequest = payload(
        &exchange.0,
        MessageType::PullRequest,
        &format!("{label} request"),
    );
    assert_eq!(request, expected_request, "{label} request payload");
    let response: PullResponse = payload(
        &exchange.1,
        MessageType::PullResponse,
        &format!("{label} response"),
    );
    assert_eq!(
        response, expected_response,
        "{label} response payload must be the server change history"
    );
}

fn expected_apply_results_for_batches(
    batches: &[ChangeSet],
    policies: &ConflictPolicies,
) -> Vec<WireApplyResult> {
    let mirror = Database::open_memory();
    batches
        .iter()
        .enumerate()
        .map(|(idx, batch)| {
            mirror
                .apply_changes(batch.clone(), policies)
                .unwrap_or_else(|err| panic!("mirror apply for expected batch {idx}: {err}"))
                .into()
        })
        .collect()
}

/// The expected served changeset for a page whose every row was pushed
/// fresh, with no established fleet-lineage ordering — the shape both
/// `ip_01` and `ip_03` push. Computed from first principles, independent of
/// the server's own arrival-stamping helpers (`changes_since_with_arrivals`
/// / `wire_changeset_with_arrivals`) so this assertion cannot pass merely
/// because the test calls the same machinery it is meant to check: a row
/// with no incoming arrival is stamped with the accepting server's own
/// commit position -- exactly its own resulting `.lsn`, never a value
/// sampled before that commit (see `database.rs`'s `SYNC_SOURCE_LSN_OWN_
/// COMMIT` sentinel; a sampled `current_lsn() == lsn - 1` value was the
/// pre-fix defect this row would otherwise still pin).
fn expected_wire_changeset_for_freshly_pushed_rows(changes: ChangeSet) -> WireChangeSet {
    let mut wire: WireChangeSet = changes.into();
    for row in &mut wire.rows {
        row.arrival = Some(row.lsn);
    }
    wire
}

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|err| panic!("read {dir:?}: {err}")) {
        let entry = entry.unwrap_or_else(|err| panic!("read entry under {dir:?}: {err}"));
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[tokio::test]
async fn ip_00_fake_has_no_responder_until_server_run_loop_serves() {
    let broker = InProcessBroker::new();
    let tenant = "ip-00";
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let server_db = Arc::new(Database::open_memory());
    let server = Arc::new(SyncServer::with_transport(
        server_db.clone(),
        broker.server(),
        contextdb_core::TenantId::from(tenant),
        policies,
    ));

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create notes on edge");
    let id = Uuid::new_v4();
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert(
        "body".to_string(),
        Value::Text("requires-run-loop".to_string()),
    );
    edge_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert on edge");

    let before_run = SyncClient::with_transport(
        edge_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let before_run_result = within(before_run.push()).await;
    assert!(
        before_run_result.is_err(),
        "constructing a server must not register fake responders; server.run_until must own serving"
    );
    let rendered_before_run_error = before_run_result
        .expect_err("checked err above")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        rendered_before_run_error.contains("no responder"),
        "pre-run fake push must fail as a no-responder transport miss, got {rendered_before_run_error}"
    );
    assert!(
        broker.recorded_exchanges().is_empty(),
        "the fake must record only completed request/reply exchanges, not no-responder attempts"
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });

    let after_run = SyncClient::with_transport(
        edge_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let after_run_result = within(after_run.push()).await;
    assert!(
        after_run_result.is_ok(),
        "after server.run_until starts, the fake must route through the real server hub"
    );
    expect_text_value(&server_db, "notes", "body", id, "requires-run-loop");

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;

    let completed_before_shutdown_probe = broker.recorded_exchanges().len();
    let after_id = Uuid::new_v4();
    let mut after_row = HashMap::new();
    after_row.insert("id".to_string(), Value::Uuid(after_id));
    after_row.insert(
        "body".to_string(),
        Value::Text("after-server-loop".to_string()),
    );
    edge_db
        .execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &after_row,
        )
        .expect("insert after server shutdown");
    let after_shutdown_result = within(after_run.push()).await;
    assert!(
        after_shutdown_result.is_err(),
        "after server.run_until exits, the fake must remove responders"
    );
    let rendered_error = after_shutdown_result
        .expect_err("checked err above")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        rendered_error.contains("no responder"),
        "post-shutdown fake push must fail as a no-responder transport miss, got {rendered_error}"
    );
    assert_eq!(
        broker.recorded_exchanges().len(),
        completed_before_shutdown_probe,
        "no-responder attempts after shutdown must not be recorded as completed exchanges"
    );
}

#[tokio::test]
async fn ip_01_push_pull_converges_over_fake() {
    let broker = InProcessBroker::new();
    let tenant = "ip-01";
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant, policies.clone());

    let edge_a_db = Arc::new(Database::open_memory());
    edge_a_db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create notes on edge A");
    let id = Uuid::new_v4();
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert("body".to_string(), Value::Text("hello-fabric".to_string()));
    edge_a_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert on edge A");
    let expected_push_batches = split_changeset_for_test(edge_a_db.changes_since(Lsn(0)));
    let edge_a = SyncClient::with_transport(
        edge_a_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let edge_a_push = within(edge_a.push()).await;
    assert!(
        edge_a_push.is_ok(),
        "push over the in-process fake must succeed with no broker"
    );
    let edge_a_push = edge_a_push.expect("checked ok above");
    expect_text_value(&server_db, "notes", "body", id, "hello-fabric");
    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        1,
        "push must use one broker-free status exchange before sending data"
    );
    assert_status_exchange(
        &status_exchanges[0],
        SyncStatusResponse {
            applied_push_watermark: Some(Lsn(0)),
            server_current_lsn: Some(Lsn(0)),
        },
        "push status",
    );
    let push_exchanges = exchanges_on(&broker, &push_subject(tenant));
    assert_eq!(
        push_exchanges.len(),
        1,
        "push must send one complete PushRequest envelope through the fake"
    );
    assert_push_exchange(
        &push_exchanges[0],
        expected_push_batches[0].clone().into(),
        PushResponse {
            result: Some(edge_a_push.clone().into()),
            error: None,
        },
        "single-batch push",
    );

    let edge_b_db = Arc::new(Database::open_memory());
    let edge_b = SyncClient::with_transport(
        edge_b_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let expected_pull_changes = server_db.changes_since(Lsn(0));
    let expected_pull_response = PullResponse {
        changeset: expected_wire_changeset_for_freshly_pushed_rows(expected_pull_changes.clone()),
        has_more: false,
        cursor: expected_pull_changes.max_lsn(),
        source: server_db
            .sync_incarnation(&contextdb_core::TenantId::from(tenant))
            .ok(),
    };
    assert!(
        within(edge_b.pull(&policies)).await.is_ok(),
        "pull over the in-process fake must succeed with no broker"
    );

    expect_text_value(&edge_b_db, "notes", "body", id, "hello-fabric");
    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        2,
        "push and pull must each make the normal status probe through the fake"
    );
    assert_status_exchange(
        &status_exchanges[1],
        SyncStatusResponse {
            applied_push_watermark: Some(
                expected_push_batches
                    .last()
                    .and_then(|batch| batch.max_lsn())
                    .expect("push batch has max lsn"),
            ),
            server_current_lsn: Some(server_db.current_lsn()),
        },
        "pull status",
    );
    let pull_exchanges = exchanges_on(&broker, &pull_subject(tenant));
    assert_eq!(
        pull_exchanges.len(),
        1,
        "pull must send one complete PullRequest envelope through the fake"
    );
    assert_pull_exchange(
        &pull_exchanges[0],
        PullRequest {
            since_lsn: Lsn(0),
            max_entries: Some(500),
        },
        expected_pull_response,
        "single-page pull",
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

/// A client transport that forwards the push batch to the real hub (so the
/// server applies + commits it) and then *drops the acknowledgement*, exactly
/// as a hub that is killed a few milliseconds into applying the batch looks to
/// the edge: the write has landed durably, but the edge never sees the ack. All
/// other subjects (status, pull, connect) forward untouched, so the edge can
/// still reach the hub to reconcile.
struct DropPushAckAfterApply {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
}

impl ClientTransport for DropPushAckAfterApply {
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
        if subject == self.push_subject {
            let inner = self.inner.clone();
            let subject = subject.to_string();
            Box::pin(async move {
                // Let the hub apply + commit the batch first ...
                let _ = inner
                    .request_single_reply(&subject, request_bytes, timeout)
                    .await;
                // ... then lose the acknowledgement on the way back.
                Err(TransportError::Unreachable(
                    "dial timed out (fault-injected after the hub committed the batch)".to_string(),
                ))
            })
        } else {
            self.inner
                .request_single_reply(subject, request_bytes, timeout)
        }
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

/// The false-negative from usability job USR-19: a push whose batch reached the
/// hub and committed durably, but whose acknowledgement was lost in transit,
/// must NOT be reported as a plain failure. With the hub still reachable, the
/// edge reconciles against the server's applied-push watermark and reports the
/// push as the success it actually was (watermark advanced), never an
/// undifferentiated `SyncError` that drives a needless re-push-as-if-lost.
#[tokio::test]
async fn ip_interrupted_push_after_commit_must_not_report_false_failure() {
    let broker = InProcessBroker::new();
    let tenant = "ip-interrupted-push";
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant, policies.clone());

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create notes on edge");
    let id = Uuid::new_v4();
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert(
        "body".to_string(),
        Value::Text("landed-despite-lost-ack".to_string()),
    );
    edge_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert on edge");

    let faulty = Arc::new(DropPushAckAfterApply {
        inner: broker.client(),
        push_subject: push_subject(tenant),
    });
    let edge = SyncClient::with_transport(
        edge_db.clone(),
        faulty,
        contextdb_core::TenantId::from(tenant),
    );

    let push = within(edge.push()).await;

    // The batch reached the hub and committed durably ...
    expect_text_value(&server_db, "notes", "body", id, "landed-despite-lost-ack");

    // ... so the edge, which can still reach the hub, must reconcile and report
    // the push as the success it actually was — never a plain `SyncError`, and
    // with the watermark advanced so the next push is a no-op, not a re-push.
    assert!(
        push.is_ok(),
        "an interrupted-after-commit push whose data landed must reconcile to success, got {push:?}"
    );
    assert!(
        !matches!(push, Err(Error::SyncError(_))),
        "the false failure this test guards against is a bare SyncError, got {push:?}"
    );
    assert_eq!(
        edge.push_watermark(),
        server_db.current_lsn(),
        "a confirmed push must advance the edge watermark to the committed hub LSN"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

#[tokio::test]
async fn ip_02_conflicting_push_reports_real_apply_result_over_fake() {
    let broker = InProcessBroker::new();
    let tenant = "ip-02";
    let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);

    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant, policies.clone());

    let key = Uuid::new_v4();
    let ddl = "CREATE TABLE claims (id UUID PRIMARY KEY, owner TEXT)";
    server_db.execute(ddl, &HashMap::new()).expect("ddl on hub");
    let mut first_row = HashMap::new();
    first_row.insert("id".to_string(), Value::Uuid(key));
    first_row.insert("owner".to_string(), Value::Text("worker-a".to_string()));
    server_db
        .execute(
            "INSERT INTO claims (id, owner) VALUES ($id, $owner)",
            &first_row,
        )
        .expect("insert hub winner");
    let first_lsn = server_db.current_lsn();

    let second_db = Arc::new(Database::open_memory());
    second_db
        .execute(ddl, &HashMap::new())
        .expect("ddl on second writer");
    second_db
        .persist_sync_push_watermark(
            &contextdb_core::TenantId::from(tenant),
            second_db.current_lsn(),
        )
        .expect("mark local DDL as already synced");
    let mut second_row = HashMap::new();
    second_row.insert("id".to_string(), Value::Uuid(key));
    second_row.insert("owner".to_string(), Value::Text("worker-b".to_string()));
    second_db
        .execute(
            "INSERT INTO claims (id, owner) VALUES ($id, $owner)",
            &second_row,
        )
        .expect("insert second");
    let losing_lsn = second_db.current_lsn();
    let expected_push_batches = split_changeset_for_test(second_db.changes_since(Lsn(0)));
    let second = SyncClient::with_transport(
        second_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let second_push = within(second.push()).await;
    assert!(
        second_push.is_ok(),
        "second push over the in-process fake must succeed with no broker"
    );
    let second_result = second_push.expect("checked ok above");

    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        1,
        "the losing push must still probe hub status through the fake"
    );
    assert_status_exchange(
        &status_exchanges[0],
        SyncStatusResponse {
            applied_push_watermark: Some(Lsn(0)),
            server_current_lsn: Some(first_lsn),
        },
        "conflict push status",
    );
    let push_exchanges = exchanges_on(&broker, &push_subject(tenant));
    assert_eq!(
        push_exchanges.len(),
        1,
        "the losing write must still travel to the hub as push request bytes"
    );
    assert_push_exchange(
        &push_exchanges[0],
        expected_push_batches[0].clone().into(),
        PushResponse {
            result: Some(second_result.clone().into()),
            error: None,
        },
        "conflicting push",
    );
    assert_eq!(
        second_result.applied_rows, 0,
        "the colliding push must not apply over the ServerWins hub"
    );
    assert_eq!(
        second_result.skipped_rows, 1,
        "the ServerWins collision must report one skipped row"
    );
    assert_eq!(
        second_result.conflicts.len(),
        1,
        "the ServerWins collision must report the losing natural key"
    );
    let conflict = &second_result.conflicts[0];
    assert_eq!(
        conflict.resolution,
        ConflictPolicy::ServerWins,
        "the reply must identify the ServerWins resolution"
    );
    assert_eq!(
        conflict.reason.as_deref(),
        Some("server_wins"),
        "the reply must carry the engine's ServerWins reason"
    );
    assert_eq!(
        conflict.natural_key.column, "id",
        "the reply must identify the natural-key column"
    );
    assert_eq!(
        conflict.natural_key.value,
        Value::Uuid(key),
        "the reply must identify the losing key"
    );
    assert_eq!(
        server_db
            .persisted_sync_applied_push_watermark(&contextdb_core::TenantId::from(tenant))
            .expect("read applied-push watermark"),
        Some(losing_lsn),
        "the server must record that it processed the losing push LSN"
    );

    expect_text_value(&server_db, "claims", "owner", key, "worker-a");

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

#[tokio::test]
async fn ip_03_large_changeset_batch_split_converges_over_fake() {
    let broker = InProcessBroker::new();
    let tenant = "ip-03";
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant, policies.clone());

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(
            "CREATE TABLE blobs (id UUID PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create blobs");
    let oversized = "x".repeat(900 * 1024);
    let regular = "x".repeat(100 * 1024);
    let mut expected_ids = BTreeSet::new();
    for idx in 0..10 {
        let id = Uuid::new_v4();
        assert!(expected_ids.insert(id), "generated duplicate UUID");
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Uuid(id));
        let payload = if idx == 0 {
            oversized.clone()
        } else {
            regular.clone()
        };
        row.insert("payload".to_string(), Value::Text(payload));
        edge_db
            .execute(
                "INSERT INTO blobs (id, payload) VALUES ($id, $payload)",
                &row,
            )
            .expect("insert blob");
    }
    let expected_batches = split_changeset_for_test(edge_db.changes_since(Lsn(0)));
    assert!(
        expected_batches.len() > 1,
        "fixture must force several sync batches, got {}",
        expected_batches.len()
    );
    let oversized_push_batches = expected_batches
        .iter()
        .filter(|batch| {
            let request = PushRequest {
                changeset: (*batch).clone().into(),
                incarnation: contextdb_core::Incarnation::default(),
            };
            encode(MessageType::PushRequest, &request)
                .expect("encode expected push batch")
                .len()
                > CHUNKING_THRESHOLD
        })
        .count();
    assert!(
        oversized_push_batches >= 1,
        "fixture must include a complete PushRequest larger than the transport chunk threshold"
    );
    let expected_batch_results = expected_apply_results_for_batches(&expected_batches, &policies);
    let edge = SyncClient::with_transport(
        edge_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let push_result = within(edge.push()).await;
    assert!(
        push_result.is_ok(),
        "multi-batch push over the fake must succeed"
    );
    let push_result = push_result.expect("checked ok above");
    assert_eq!(
        push_result.applied_rows, 10,
        "the hub must apply every row across all split batches"
    );
    let push_exchanges = exchanges_on(&broker, &push_subject(tenant));
    assert_eq!(
        push_exchanges.len(),
        expected_batches.len(),
        "each split batch must be its own complete push request over the fake"
    );
    let mut response_applied_rows = 0usize;
    let mut response_skipped_rows = 0usize;
    let mut response_conflicts = 0usize;
    for (idx, (exchange, expected_batch)) in push_exchanges
        .iter()
        .zip(expected_batches.iter())
        .enumerate()
    {
        let request: PushRequest = payload(
            &exchange.0,
            MessageType::PushRequest,
            &format!("batch {idx} push request"),
        );
        assert_eq!(
            request.changeset,
            WireChangeSet::from(expected_batch.clone()),
            "batch {idx} request payload must equal the splitter output"
        );
        let response: PushResponse = payload(
            &exchange.1,
            MessageType::PushResponse,
            &format!("batch {idx} push response"),
        );
        assert_eq!(response.error, None, "batch {idx} must not error");
        let result = response
            .result
            .unwrap_or_else(|| panic!("batch {idx} response must carry an apply result"));
        assert_eq!(
            result, expected_batch_results[idx],
            "batch {idx} response payload must equal the exact engine apply result"
        );
        response_applied_rows += result.applied_rows;
        response_skipped_rows += result.skipped_rows;
        response_conflicts += result.conflicts.len();
        if idx + 1 == expected_batches.len() {
            assert_eq!(
                result.new_lsn, push_result.new_lsn,
                "last batch response must match the caller-visible final LSN"
            );
        }
    }
    assert_eq!(
        response_applied_rows, push_result.applied_rows,
        "per-batch response row counts must sum to the caller-visible result"
    );
    assert_eq!(
        response_skipped_rows, push_result.skipped_rows,
        "per-batch skipped rows must sum to the caller-visible result"
    );
    assert_eq!(
        response_conflicts,
        push_result.conflicts.len(),
        "per-batch conflicts must sum to the caller-visible result"
    );
    assert_eq!(
        uuid_set(&server_db, "blobs"),
        expected_ids,
        "the hub must hold the exact pushed row identities"
    );

    let reader_db = Arc::new(Database::open_memory());
    let reader = SyncClient::with_transport(
        reader_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let expected_pull_changes = server_db.changes_since(Lsn(0));
    let expected_pull_response = PullResponse {
        changeset: expected_wire_changeset_for_freshly_pushed_rows(expected_pull_changes.clone()),
        has_more: false,
        cursor: expected_pull_changes.max_lsn(),
        source: server_db
            .sync_incarnation(&contextdb_core::TenantId::from(tenant))
            .ok(),
    };
    assert!(
        within(reader.pull(&policies)).await.is_ok(),
        "pull of a multi-batch history over the fake must succeed"
    );

    assert_eq!(
        uuid_set(&reader_db, "blobs"),
        expected_ids,
        "every exact row of a batch-split push must converge over the fake"
    );
    let pull_exchanges = exchanges_on(&broker, &pull_subject(tenant));
    assert_eq!(
        pull_exchanges.len(),
        1,
        "the reader pull must travel as a complete pull request over the fake"
    );
    assert_pull_exchange(
        &pull_exchanges[0],
        PullRequest {
            since_lsn: Lsn(0),
            max_entries: Some(500),
        },
        expected_pull_response,
        "multi-batch history pull",
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

#[test]
fn seam_names_no_concrete_transport_outside_adapter() {
    let root = env!("CARGO_MANIFEST_DIR");
    let adapter_rel = "src/transport/nats.rs";
    let concrete_nats_needles = [
        "async_nats",
        "publish_with_reply",
        "new_inbox",
        "StatusCode::NO_RESPONDERS",
        ".subscribe(",
        "requires_fragmented_send",
        "fragmented_send",
        "transport::nats",
        "::transport::nats",
        "::{nats",
        "nats::",
        "NatsClient",
        "NatsServer",
        "NatsTransport",
        "NatsClientTransport",
        "NatsServerTransport",
    ];
    let mut sources = Vec::new();
    collect_rust_sources(&Path::new(root).join("src"), &mut sources);
    for path in &sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        if rel == adapter_rel {
            continue;
        }
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for needle in concrete_nats_needles {
            assert!(
                !src.contains(needle),
                "{rel} must not name `{needle}` after the transport cut - it belongs only in the NATS adapter"
            );
        }
    }

    let error_src = std::fs::read_to_string(format!("{root}/src/error.rs"))
        .expect("read shared sync error file");
    assert!(
        !error_src.contains("Nats"),
        "the shared sync error type must not name the concrete NATS transport"
    );

    let client_src = std::fs::read_to_string(format!("{root}/src/sync_client.rs"))
        .expect("read sync client source");
    let server_src = std::fs::read_to_string(format!("{root}/src/sync_server.rs"))
        .expect("read sync server source");
    assert!(
        client_src.contains("ClientTransport") && client_src.contains(".request("),
        "sync client must send complete request bytes through ClientTransport::request"
    );
    assert!(
        server_src.contains("ServerTransport") && server_src.contains(".serve("),
        "sync server must register complete byte handlers through ServerTransport::serve"
    );

    for (rel, needle) in [
        ("src/sync_client.rs", "InProcess"),
        ("src/sync_server.rs", "InProcess"),
        ("src/sync_client.rs", "transport::in_process"),
        ("src/sync_server.rs", "transport::in_process"),
        ("src/sync_client.rs", "downcast"),
        ("src/sync_server.rs", "downcast"),
        ("src/sync_client.rs", "std::any::Any"),
        ("src/sync_server.rs", "std::any::Any"),
        ("src/sync_server.rs", "subscribe"),
    ] {
        let path = format!("{root}/{rel}");
        let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        assert!(
            !src.contains(needle),
            "{rel} must not special-case the in-process fake; sync logic must use only the transport traits"
        );
    }

    let chunking_allowed = ["src/chunking.rs", "src/protocol.rs", adapter_rel];
    for path in &sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        if chunking_allowed.contains(&rel.as_str()) {
            continue;
        }
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for needle in [
            "crate::chunking",
            "chunking::",
            "use crate::chunking",
            "use crate::{chunking",
            "MessageType::Chunk",
            "MessageType::ChunkAck",
            "ChunkMessage",
            "ChunkAck",
            "CHUNK_COLLECT_TIMEOUT",
            "MAX_CHUNK",
            "chunk_buffer",
            "needs_chunking",
        ] {
            assert!(
                !src.contains(needle),
                "{rel} must not own transport chunking after the cut; chunk framing belongs below the transport seam"
            );
        }
    }

    for path in &sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));

        let sync_engine_allowed = [
            "src/sync_client.rs",
            "src/sync_server.rs",
            "src/protocol.rs",
            "src/main.rs",
            "src/sync_plugin.rs",
            // The work ledger's distributed half is a sync CONSUMER by
            // design (claim-by-push over SyncClient + the engine ledger
            // API), so this exemption lets it name engine/sync types. Its
            // class- and transport-blindness is enforced by its own source
            // guard (wl_g02 in the engine integration suite); the protocol
            // needles below still apply to it.
            "src/work_ledger.rs",
            // The media resolver owns ledger-backed authorization. Its own
            // containment guard enforces that only the sanctioned server
            // handle crosses the transport-adapter boundary.
            "src/blob_resolver.rs",
        ];
        if !sync_engine_allowed.contains(&rel.as_str()) {
            for needle in [
                "contextdb_engine::",
                "Arc<Database>",
                "Database::",
                "apply_changes",
                "changes_since",
                "ChangeSet",
                "ApplyResult",
                "ConflictPolicies",
                "ConflictPolicy",
            ] {
                assert!(
                    !src.contains(needle),
                    "{rel} must not hide sync-domain engine/apply logic outside the sync/protocol surface"
                );
            }
        }

        let sync_protocol_allowed = [
            "src/sync_client.rs",
            "src/sync_server.rs",
            "src/protocol.rs",
            adapter_rel,
        ];
        if !sync_protocol_allowed.contains(&rel.as_str()) {
            for needle in [
                "PushRequest",
                "PushResponse",
                "PullRequest",
                "PullResponse",
                "SyncStatusRequest",
                "SyncStatusResponse",
                "MessageType::PushRequest",
                "MessageType::PushResponse",
                "MessageType::PullRequest",
                "MessageType::PullResponse",
                "MessageType::StatusRequest",
                "MessageType::StatusResponse",
                "Envelope",
                "decode(",
                "encode(",
                "rmp_serde",
            ] {
                assert!(
                    !src.contains(needle),
                    "{rel} must not hide sync protocol parsing outside the sync/protocol surface"
                );
            }
        }
    }

    let adapter = std::fs::read_to_string(format!("{root}/src/transport/nats.rs"))
        .expect("the NATS adapter module must exist");
    assert!(
        adapter.contains("async_nats::"),
        "the NATS adapter must own the real async_nats usage"
    );

    let mut transport_sources = Vec::new();
    collect_rust_sources(
        &Path::new(root).join("src/transport"),
        &mut transport_sources,
    );
    for path in &transport_sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip transport source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for needle in [
            "contextdb_engine",
            "Database",
            "apply_changes",
            "changes_since",
            "ChangeSet",
            "ApplyResult",
            "ConflictPolicies",
            "ConflictPolicy",
            "PushRequest",
            "PushResponse",
            "PullRequest",
            "PullResponse",
            "SyncStatusRequest",
            "SyncStatusResponse",
            "MessageType::PushRequest",
            "MessageType::PushResponse",
            "MessageType::PullRequest",
            "MessageType::PullResponse",
            "MessageType::StatusRequest",
            "MessageType::StatusResponse",
        ] {
            assert!(
                !src.contains(needle),
                "{rel} must only move request/reply bytes; `{needle}` belongs in sync logic"
            );
        }
    }

    for path in &transport_sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip transport source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        if rel == adapter_rel {
            continue;
        }
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for needle in ["Envelope", "MessageType", "decode", "encode", "rmp_serde"] {
            assert!(
                !src.contains(needle),
                "{rel} must not parse protocol envelopes; the fake moves opaque bytes"
            );
        }
    }
}
