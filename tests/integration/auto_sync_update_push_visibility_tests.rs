use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, Conflict, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
    SyncDirection,
};
use contextdb_server::protocol::{MessageType, PushRequest, PushResponse, decode, encode};
use contextdb_server::subjects::push_subject;
use contextdb_server::{SyncClient, SyncServer};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

const TABLE_SQL: &str = "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)";

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
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

    let container = request.start().await.unwrap();
    let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();

    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql, &empty_params()).unwrap();
}

fn create_t(db: &Database) {
    exec(db, TABLE_SQL);
}

fn pad_tables(db: &Database, count: usize) {
    for index in 0..count {
        exec(
            db,
            &format!("CREATE TABLE pad_{index} (id UUID PRIMARY KEY)"),
        );
    }
}

fn latest_wins() -> ConflictPolicies {
    ConflictPolicies::uniform(ConflictPolicy::LatestWins)
}

fn row_values(key: Uuid, text: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(key)),
        ("v".to_string(), Value::Text(text.to_string())),
    ])
}

fn row_change(key: Uuid, text: &str, lsn: Lsn) -> RowChange {
    RowChange {
        table: "t".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(key),
        },
        values: row_values(key, text),
        deleted: false,
        lsn,
        created_at: None,
    }
}

fn changeset(rows: Vec<RowChange>) -> ChangeSet {
    ChangeSet {
        rows,
        ..ChangeSet::default()
    }
}

fn apply_latest(db: &Database, rows: Vec<RowChange>) -> ApplyResult {
    db.apply_changes(changeset(rows), &latest_wins()).unwrap()
}

fn text_at(db: &Database, key: Uuid) -> Option<String> {
    db.point_lookup("t", "id", &Value::Uuid(key), db.snapshot())
        .unwrap()
        .and_then(|row| {
            row.values
                .get("v")
                .and_then(Value::as_text)
                .map(str::to_string)
        })
}

fn selected_text_at(db: &Database, key: Uuid) -> Option<String> {
    let result = db
        .execute(
            "SELECT v FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(key))]),
        )
        .unwrap();
    assert!(
        result.rows.len() <= 1,
        "primary-key SELECT must return at most one row"
    );
    result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_text)
        .map(str::to_string)
}

fn assert_text(db: &Database, key: Uuid, expected: &str) {
    assert_eq!(text_at(db, key).as_deref(), Some(expected));
}

fn assert_single_row_applied_without_conflict(result: &ApplyResult, context: &str) {
    assert_eq!(result.applied_rows, 1, "{context}: exactly one row applies");
    assert_eq!(result.skipped_rows, 0, "{context}: no row is skipped");
    assert!(
        result.conflicts.is_empty(),
        "{context}: single-writer convergence must not report a conflict"
    );
}

fn insert_sql(db: &Database, key: Uuid, text: &str) {
    db.execute(
        "INSERT INTO t (id, v) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(key)),
            ("v", Value::Text(text.to_string())),
        ]),
    )
    .unwrap();
}

fn update_sql(db: &Database, key: Uuid, text: &str) {
    db.execute(
        "UPDATE t SET v = $v WHERE id = $id",
        &params(vec![
            ("id", Value::Uuid(key)),
            ("v", Value::Text(text.to_string())),
        ]),
    )
    .unwrap();
}

fn delete_sql(db: &Database, key: Uuid) {
    db.execute(
        "DELETE FROM t WHERE id = $id",
        &params(vec![("id", Value::Uuid(key))]),
    )
    .unwrap();
}

fn tenant() -> String {
    format!("tenant_{}", Uuid::new_v4().simple())
}

async fn spawn_latest_wins_server(
    db: Arc<Database>,
    nats_url: &str,
    tenant_id: &str,
) -> tokio::task::JoinHandle<()> {
    let server = Arc::new(SyncServer::new(
        db,
        nats_url,
        contextdb_core::TenantId::from(tenant_id),
        latest_wins(),
    ));
    let server_task = Arc::clone(&server);
    let handle = tokio::spawn(async move { server_task.run().await });
    tokio::time::sleep(Duration::from_millis(250)).await;
    handle
}

async fn spawn_reject_once_push_responder(
    nats_url: &str,
    tenant_id: &str,
    expected_key: Uuid,
) -> tokio::task::JoinHandle<()> {
    let nats = async_nats::connect(nats_url).await.unwrap();
    let mut sub = nats.subscribe(push_subject(tenant_id)).await.unwrap();
    nats.flush().await.unwrap();

    tokio::spawn(async move {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .expect("fake push responder must receive one push");
        let envelope = decode(&msg.payload).unwrap();
        assert_eq!(envelope.message_type, MessageType::PushRequest);
        let request: PushRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
        let changes = ChangeSet::try_from(request.changeset).unwrap();
        let update = changes
            .rows
            .iter()
            .find(|row| row.natural_key.value == Value::Uuid(expected_key))
            .expect("push must include the pending updated row");
        assert_eq!(
            update.values.get("v").and_then(Value::as_text),
            Some("updated")
        );
        let response = PushResponse {
            result: Some(
                ApplyResult {
                    applied_rows: 0,
                    skipped_rows: 1,
                    conflicts: vec![Conflict {
                        natural_key: update.natural_key.clone(),
                        resolution: ConflictPolicy::LatestWins,
                        reason: Some("local_lsn_newer_or_equal".to_string()),
                    }],
                    new_lsn: Lsn(0),
                }
                .into(),
            ),
            error: None,
        };
        let reply = msg.reply.expect("push request must carry a reply inbox");
        let payload = encode(MessageType::PushResponse, &response).unwrap();
        nats.publish(reply, payload.into()).await.unwrap();
        nats.flush().await.unwrap();
    })
}

#[test]
fn red_apply_single_writer_update_converges_under_clock_collision() {
    let receiver = Database::open_memory();
    create_t(&receiver);
    pad_tables(&receiver, 2);
    assert!(receiver.current_lsn() >= Lsn(3));

    let key = uuid(1);
    let insert_result = apply_latest(&receiver, vec![row_change(key, "original", Lsn(2))]);
    assert_single_row_applied_without_conflict(&insert_result, "initial single-writer insert");
    assert_text(&receiver, key, "original");
    assert!(receiver.current_lsn() >= Lsn(3));

    let result = apply_latest(&receiver, vec![row_change(key, "updated", Lsn(3))]);
    assert_single_row_applied_without_conflict(&result, "colliding single-writer update");

    assert_text(&receiver, key, "updated");
}

#[test]
fn red_apply_collision_after_prior_convergence() {
    let receiver = Database::open_memory();
    create_t(&receiver);

    let key = uuid(2);
    let insert_result = apply_latest(&receiver, vec![row_change(key, "original", Lsn(2))]);
    assert_single_row_applied_without_conflict(&insert_result, "initial rolling insert");
    let u3_result = apply_latest(&receiver, vec![row_change(key, "u3", Lsn(3))]);
    assert_single_row_applied_without_conflict(&u3_result, "first rolling update");
    assert_text(&receiver, key, "u3");

    pad_tables(&receiver, 1);
    assert!(receiver.current_lsn() >= Lsn(4));

    let u4_result = apply_latest(&receiver, vec![row_change(key, "u4", Lsn(4))]);
    assert_single_row_applied_without_conflict(&u4_result, "second rolling update");
    assert_text(&receiver, key, "u4");

    let result = apply_latest(&receiver, vec![row_change(key, "u5", Lsn(5))]);
    assert_single_row_applied_without_conflict(&result, "post-convergence colliding update");

    assert_text(&receiver, key, "u5");
}

#[test]
fn red_apply_mixed_changeset_partial_application() {
    let receiver = Database::open_memory();
    create_t(&receiver);
    pad_tables(&receiver, 6);

    let colliding_key = uuid(3);
    let server_newer_key = uuid(4);

    let insert_result = apply_latest(
        &receiver,
        vec![row_change(colliding_key, "original", Lsn(2))],
    );
    assert_single_row_applied_without_conflict(&insert_result, "mixed changeset initial insert");
    assert_text(&receiver, colliding_key, "original");

    insert_sql(&receiver, server_newer_key, "server-newer");
    assert!(receiver.current_lsn() > Lsn(3));

    let result = apply_latest(
        &receiver,
        vec![
            row_change(colliding_key, "updated", Lsn(3)),
            row_change(server_newer_key, "edge-old", Lsn(3)),
        ],
    );
    assert_eq!(
        result.applied_rows, 1,
        "colliding single-author row must apply in the mixed changeset"
    );
    assert_eq!(
        result.skipped_rows, 1,
        "only the genuine two-writer loser may be skipped"
    );
    assert!(
        result
            .conflicts
            .iter()
            .any(|conflict| conflict.natural_key.value == Value::Uuid(server_newer_key)),
        "the genuine two-writer loser must be reported as a conflict"
    );
    assert!(
        !result
            .conflicts
            .iter()
            .any(|conflict| conflict.natural_key.value == Value::Uuid(colliding_key)),
        "the colliding single-author row must not be reported as a conflict"
    );

    assert_text(&receiver, colliding_key, "updated");
    assert_text(&receiver, server_newer_key, "server-newer");
}

#[tokio::test]
async fn red_e2e_single_writer_update_converges_over_nats() {
    let nats = start_nats().await;
    let tenant_id = tenant();

    let server_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    pad_tables(&server_db, 2);
    let _server =
        spawn_latest_wins_server(Arc::clone(&server_db), &nats.nats_url, &tenant_id).await;

    let writer_db = Arc::new(Database::open_memory());
    create_t(&writer_db);
    let client = SyncClient::new(
        Arc::clone(&writer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    client
        .set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");

    let peer_db = Arc::new(Database::open_memory());
    create_t(&peer_db);
    let peer = SyncClient::new(
        Arc::clone(&peer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    peer.set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");

    let key = uuid(5);
    insert_sql(&writer_db, key, "original");
    client.push().await.unwrap();
    assert_text(&server_db, key, "original");
    peer.pull(&latest_wins()).await.unwrap();
    assert_eq!(selected_text_at(&peer_db, key).as_deref(), Some("original"));

    update_sql(&writer_db, key, "updated");
    let update_sender_lsn = writer_db.current_lsn();
    assert!(server_db.current_lsn() >= update_sender_lsn);

    let update_result = client.push().await.unwrap();
    assert_single_row_applied_without_conflict(&update_result, "server collision update push");

    assert_text(&server_db, key, "updated");

    let pull_result = peer.pull(&latest_wins()).await.unwrap();
    assert_single_row_applied_without_conflict(&pull_result, "delta pull after server collision");
    assert_eq!(selected_text_at(&peer_db, key).as_deref(), Some("updated"));
}

#[tokio::test]
async fn red_e2e_dirty_peer_pull_update_converges() {
    let nats = start_nats().await;
    let tenant_id = tenant();

    let server_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    let _server =
        spawn_latest_wins_server(Arc::clone(&server_db), &nats.nats_url, &tenant_id).await;

    let writer_db = Arc::new(Database::open_memory());
    create_t(&writer_db);
    let writer = SyncClient::new(
        Arc::clone(&writer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    writer
        .set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");

    let peer_db = Arc::new(Database::open_memory());
    create_t(&peer_db);
    pad_tables(&peer_db, 3);
    let peer = SyncClient::new(
        Arc::clone(&peer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    peer.set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");

    let key = uuid(6);
    insert_sql(&writer_db, key, "original");
    writer.push().await.unwrap();

    peer.pull(&latest_wins()).await.unwrap();
    assert_text(&peer_db, key, "original");

    update_sql(&writer_db, key, "updated");
    let update_result = writer.push().await.unwrap();
    assert_single_row_applied_without_conflict(
        &update_result,
        "server update before dirty peer pull",
    );
    assert_text(&server_db, key, "updated");
    assert!(peer_db.current_lsn() >= server_db.current_lsn());

    let pull_result = peer.pull(&latest_wins()).await.unwrap();
    assert_single_row_applied_without_conflict(&pull_result, "dirty peer update pull");

    assert_eq!(selected_text_at(&peer_db, key).as_deref(), Some("updated"));
}

#[tokio::test]
async fn red_e2e_writer_restart_pending_update_converges() {
    let nats = start_nats().await;
    let tenant_id = tenant();
    let writer_tmp = tempfile::TempDir::new().unwrap();
    let writer_path = writer_tmp.path().join("writer.redb");

    let server_db = Arc::new(Database::open_memory());
    create_t(&server_db);
    pad_tables(&server_db, 2);
    let shutdown = Arc::new(AtomicBool::new(false));
    let server = Arc::new(SyncServer::new(
        Arc::clone(&server_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
        latest_wins(),
    ));
    let server_task = {
        let server = Arc::clone(&server);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move { server.run_until(shutdown).await })
    };
    tokio::time::sleep(Duration::from_millis(250)).await;

    let writer_db = Arc::new(Database::open(&writer_path).unwrap());
    create_t(&writer_db);
    let client = SyncClient::new(
        Arc::clone(&writer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    client
        .set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");

    let key = uuid(7);
    let sibling = uuid(8);
    insert_sql(&writer_db, sibling, "synced");
    insert_sql(&writer_db, key, "original");
    client.push().await.unwrap();
    assert_text(&server_db, sibling, "synced");
    assert_text(&server_db, key, "original");

    shutdown.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .unwrap()
        .unwrap();
    drop(server);

    let reject_once = spawn_reject_once_push_responder(&nats.nats_url, &tenant_id, key).await;
    update_sql(&writer_db, key, "updated");
    let update_sender_lsn = writer_db.current_lsn();
    let rejected = client.push().await.unwrap();
    reject_once.await.unwrap();
    assert!(
        rejected
            .conflicts
            .iter()
            .any(|conflict| conflict.reason.as_deref() == Some("local_lsn_newer_or_equal")),
        "first update push must observe the nonterminal LSN-clock conflict"
    );
    assert!(
        client.push_watermark() < update_sender_lsn,
        "push watermark must not be persisted past an unapplied updated row"
    );
    drop(client);

    let retry_server =
        spawn_latest_wins_server(Arc::clone(&server_db), &nats.nats_url, &tenant_id).await;
    let restarted = SyncClient::new(
        Arc::clone(&writer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    restarted
        .set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");
    let retry_result = restarted.push().await.unwrap();
    assert_single_row_applied_without_conflict(&retry_result, "restart retry push");

    assert_text(&server_db, key, "updated");
    assert_text(&server_db, sibling, "synced");
    retry_server.abort();
}

#[tokio::test]
async fn red_e2e_single_writer_update_durable_after_server_restart() {
    let nats = start_nats().await;
    let tenant_id = tenant();
    let server_tmp = tempfile::TempDir::new().unwrap();
    let server_path = server_tmp.path().join("server.redb");

    let server_db = Arc::new(Database::open(&server_path).unwrap());
    create_t(&server_db);
    pad_tables(&server_db, 2);
    let shutdown = Arc::new(AtomicBool::new(false));
    let server = Arc::new(SyncServer::new(
        Arc::clone(&server_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
        latest_wins(),
    ));
    let server_task = {
        let server = Arc::clone(&server);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move { server.run_until(shutdown).await })
    };
    tokio::time::sleep(Duration::from_millis(250)).await;

    let writer_db = Arc::new(Database::open_memory());
    create_t(&writer_db);
    let client = SyncClient::new(
        Arc::clone(&writer_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(&tenant_id),
    );
    client
        .set_table_direction("t", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");

    let key = uuid(9);
    insert_sql(&writer_db, key, "original");
    client.push().await.unwrap();
    assert_text(&server_db, key, "original");

    update_sql(&writer_db, key, "updated");
    let update_sender_lsn = writer_db.current_lsn();
    assert!(server_db.current_lsn() >= update_sender_lsn);
    let update_result = client.push().await.unwrap();
    assert_single_row_applied_without_conflict(&update_result, "durable server update push");

    shutdown.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .unwrap()
        .unwrap();
    drop(server);
    server_db.close().unwrap();
    drop(server_db);

    let reopened = Database::open(&server_path).unwrap();
    assert_text(&reopened, key, "updated");
}

#[test]
fn guard_clean_path_single_writer_update_converges() {
    let server = Database::open_memory();
    create_t(&server);

    let key = uuid(10);
    apply_latest(&server, vec![row_change(key, "original", Lsn(2))]);
    apply_latest(&server, vec![row_change(key, "updated", Lsn(3))]);

    assert_text(&server, key, "updated");
}

#[test]
fn guard_single_writer_delete_converges() {
    let writer = Database::open_memory();
    create_t(&writer);
    let after_schema = writer.current_lsn();

    let receiver = Database::open_memory();
    create_t(&receiver);
    pad_tables(&receiver, 2);

    let keep = uuid(11);
    let delete_me = uuid(12);
    insert_sql(&writer, keep, "keep");
    insert_sql(&writer, delete_me, "delete_me");
    let after_inserts = writer.current_lsn();

    receiver
        .apply_changes(writer.changes_since(after_schema), &latest_wins())
        .unwrap();
    assert_text(&receiver, keep, "keep");
    assert_text(&receiver, delete_me, "delete_me");

    delete_sql(&writer, delete_me);
    let delete_changes = writer.changes_since(after_inserts);
    assert!(
        delete_changes
            .rows
            .iter()
            .any(|row| row.deleted && !row.values.is_empty()),
        "delete changeset must carry the wire-accurate __deleted marker"
    );

    receiver
        .apply_changes(delete_changes, &latest_wins())
        .unwrap();

    assert_eq!(text_at(&receiver, delete_me), None);
    assert_text(&receiver, keep, "keep");
}

#[test]
fn guard_two_writer_latest_wins_server_value_survives() {
    let edge = Database::open_memory();
    create_t(&edge);
    let receiver = Database::open_memory();
    create_t(&receiver);

    let key = uuid(13);
    insert_sql(&edge, key, "edge-old");

    insert_sql(&receiver, uuid(14), "padding");
    insert_sql(&receiver, key, "server-newer");
    assert!(receiver.current_lsn() > edge.current_lsn());

    let result = receiver
        .apply_changes(edge.changes_since(Lsn(1)), &latest_wins())
        .unwrap();

    assert_text(&receiver, key, "server-newer");
    assert!(
        result.skipped_rows >= 1,
        "older incoming writer value must be skipped"
    );
}

#[test]
fn guard_two_writer_latest_wins_edge_value_wins() {
    let edge = Database::open_memory();
    create_t(&edge);
    let receiver = Database::open_memory();
    create_t(&receiver);

    let key = uuid(15);
    insert_sql(&receiver, key, "server-old");

    insert_sql(&edge, uuid(16), "padding");
    insert_sql(&edge, key, "edge-newer");
    assert!(edge.current_lsn() > receiver.current_lsn());

    let result = receiver
        .apply_changes(edge.changes_since(Lsn(1)), &latest_wins())
        .unwrap();

    assert_text(&receiver, key, "edge-newer");
    assert!(
        result.applied_rows >= 1,
        "higher-LSN incoming writer value must apply"
    );
}

// Sequential single-writer updates must keep converging after the receiver's
// local commit clock has been driven far past the sender's LSN range by
// unrelated traffic. This is the residual shape the acceptance gate probed:
// one insert applies, the receiver clock inflates, and then several more
// same-provenance updates arrive with sender LSNs that all sit at/below the
// inflated receiver clock. Every update carries the next monotonic sender LSN
// and must win against the prior applied value — the final value must equal the
// LAST update, never strand at an earlier one.
#[test]
fn red_apply_sequential_updates_converge_under_inflated_receiver_clock() {
    let receiver = Database::open_memory();
    create_t(&receiver);

    let key = uuid(17);
    let insert_result = apply_latest(&receiver, vec![row_change(key, "s1", Lsn(2))]);
    assert_single_row_applied_without_conflict(&insert_result, "drift-strand initial insert");
    assert_text(&receiver, key, "s1");

    // Inflate the receiver's local commit clock far past the sender's LSN range
    // with commits to an unrelated table.
    pad_tables(&receiver, 1);
    for n in 0..60u128 {
        insert_sql(&receiver, uuid(10_000 + n), "filler");
    }
    assert!(
        receiver.current_lsn() > Lsn(7),
        "receiver clock must be inflated well past the sender LSN range"
    );

    // Sequential same-provenance updates with monotonically increasing sender
    // LSNs that all fall at/below the inflated receiver clock.
    let updates = [("s2", 3u64), ("s3", 4), ("s4", 5), ("s5", 6), ("s6", 7)];
    for (text, sender_lsn) in updates {
        let result = apply_latest(&receiver, vec![row_change(key, text, Lsn(sender_lsn))]);
        assert_single_row_applied_without_conflict(
            &result,
            &format!("post-drift sequential update {text}"),
        );
        assert_text(&receiver, key, text);
    }

    // The strand defect surfaces as the row frozen at an early update; correct
    // behavior is convergence on the final update.
    assert_text(&receiver, key, "s6");
}

// Re-delivering a row that is already converged — byte-identical values and the
// identical sender LSN — must be a storage no-op AND a ledger no-op. The stored
// row LSN must not move and the re-delivery must not be counted as an applied
// row. At HEAD the equal-LSN fall-through (`row.values == committed.values`)
// routes the re-delivery into the apply branch; `upsert_row_for_sync` returns
// `NoOp` so storage is untouched, but `applied_rows` is still incremented,
// reporting phantom work that never happened.
#[test]
fn red_identical_redelivery_is_a_ledger_no_op() {
    let receiver = Database::open_memory();
    create_t(&receiver);

    let key = uuid(18);
    apply_latest(&receiver, vec![row_change(key, "v1", Lsn(2))]);
    let converge = apply_latest(&receiver, vec![row_change(key, "v2", Lsn(3))]);
    assert_single_row_applied_without_conflict(&converge, "redelivery convergence update");
    assert_text(&receiver, key, "v2");

    // Byte-identical re-delivery of the already-converged row.
    let redeliver = apply_latest(&receiver, vec![row_change(key, "v2", Lsn(3))]);

    assert_eq!(
        redeliver.applied_rows, 0,
        "identical re-delivery of a converged row must not be counted as applied"
    );
    assert_text(&receiver, key, "v2");
    assert!(
        redeliver
            .conflicts
            .iter()
            .all(|conflict| conflict.reason.as_deref()
                != Some("latest_wins_local_lsn_newer_or_equal")),
        "an identical re-delivery must not record a retryable conflict reason"
    );
}

// A concurrent bidirectional reader poisons a row's sync provenance and strands
// the genuine writer.
//
// Scenario: a single writer streams sequential updates to a row while its sender
// clock stays low. A second client (a `.sync pull` session with Both direction)
// pulls those rows and applies them to its OWN database, which restamps them onto
// the reader's local commit clock — a clock that has been inflated far past the
// writer's LSN range by unrelated traffic (e.g. the 60-row pull). When that reader
// echoes a now-stale value back, the echo carries the reader's inflated LSN, not
// the writer's. The server's LatestWins apply lets the inflated echo win and
// records the reader's inflated LSN as the row's source provenance. Every
// subsequent genuine update from the writer then carries a sender LSN below that
// poisoned provenance and is rejected `latest_wins_local_lsn_newer_or_equal`,
// freezing the server on the stale value while the writer believes it is caught
// up. The fix must keep a row's provenance anchored to the originating writer's
// LSN so a downstream reader's echo cannot strand the source.
//
// The echo is reproduced here through the engine apply path: a same-table
// LatestWins re-delivery of a stale value carrying an inflated sender LSN, which
// is the wire-accurate shape of the reader's echo push.
#[test]
fn red_apply_concurrent_reader_echo_must_not_strand_writer() {
    let receiver = Database::open_memory();
    create_t(&receiver);

    let key = uuid(19);

    // Writer streams s1 (insert) and s2 (update) with low sender LSNs.
    let insert_result = apply_latest(&receiver, vec![row_change(key, "s1", Lsn(2))]);
    assert_single_row_applied_without_conflict(&insert_result, "echo-poison initial insert");
    let s2_result = apply_latest(&receiver, vec![row_change(key, "s2", Lsn(3))]);
    assert_single_row_applied_without_conflict(&s2_result, "echo-poison second update");
    assert_text(&receiver, key, "s2");

    // Unrelated traffic inflates the receiver's local commit clock far past the
    // writer's sender LSN range.
    pad_tables(&receiver, 1);
    for n in 0..60u128 {
        insert_sql(&receiver, uuid(20_000 + n), "filler");
    }
    let inflated_clock = receiver.current_lsn();
    assert!(
        inflated_clock > Lsn(8),
        "receiver clock must be inflated past the writer sender LSN range"
    );

    // Writer streams s3.
    let s3_result = apply_latest(&receiver, vec![row_change(key, "s3", Lsn(4))]);
    assert_single_row_applied_without_conflict(&s3_result, "echo-poison third update");
    assert_text(&receiver, key, "s3");

    // A concurrent bidirectional reader echoes a now-stale value ("s2") back,
    // carrying its inflated local LSN rather than the writer's. This must NOT be
    // allowed to overwrite the genuinely newer "s3" nor to poison the row's
    // provenance with the inflated LSN.
    let echo_lsn = inflated_clock;
    apply_latest(&receiver, vec![row_change(key, "s2", echo_lsn)]);

    // The writer keeps streaming with its own low sender LSNs. Every update must
    // continue to apply and the row must converge on the final value.
    let updates = [("s4", 5u64), ("s5", 6), ("s6", 7)];
    for (text, sender_lsn) in updates {
        let result = apply_latest(&receiver, vec![row_change(key, text, Lsn(sender_lsn))]);
        assert!(
            !result
                .conflicts
                .iter()
                .any(|conflict| conflict.reason.as_deref()
                    == Some("latest_wins_local_lsn_newer_or_equal")),
            "writer update {text} must not be rejected as lsn-newer-or-equal after a reader echo"
        );
        assert_eq!(
            result.applied_rows, 1,
            "writer update {text} must apply after a concurrent reader echo"
        );
        assert_text(&receiver, key, text);
    }

    assert_text(&receiver, key, "s6");
}
