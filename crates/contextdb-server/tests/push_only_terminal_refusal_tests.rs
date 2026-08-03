//! Authenticated-Iroh proofs for terminal refusals on one-way outbound tables.

use contextdb_core::{TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

const DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
     RETAIN 1 HOURS SYNC SAFE SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const T0: u64 = 1_700_000_000_000;
const DELETE_CHILD_DATABASE: &str = "CONTEXTDB_PUSH_ONLY_DELETE_CHILD_DATABASE";
const DELETE_CHILD_IDENTITY: &str = "CONTEXTDB_PUSH_ONLY_DELETE_CHILD_IDENTITY";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded ticketed-Iroh operation")
}

struct MockClock(Arc<AtomicU64>);

impl MockClock {
    fn install(start_millis: u64) -> (Self, contextdb_core::WallclockTestClockGuard) {
        let cell = Arc::new(AtomicU64::new(start_millis));
        let guard = {
            let cell = cell.clone();
            Wallclock::test_clock_guard(move || cell.load(Ordering::SeqCst))
        };
        (Self(cell), guard)
    }

    fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

fn bind_spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn declare(db: &Database) {
    if db.table_meta("notes").is_none() {
        db.execute(DDL, &HashMap::new())
            .expect("declare retained push-only keep-first table");
    }
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("write exact local note");
}

fn delete(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("commit exact local delete");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact note")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("notes.body must be TEXT, got {other:?}"),
    })
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let (ticket, transport) = {
        let endpoint = IrohServer::bind(&bind_spec(&identity_path))
            .await
            .expect("bind hub");
        (endpoint.ticket(), endpoint.transport())
    };
    let identity = Arc::new(
        FabricIdentity::load_or_generate(&identity_path).expect("load hub fabric identity"),
    );
    let node_id = identity.node_id();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open file-backed hub"));
    declare(&db);
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            transport,
            TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task).await.expect("hub stops cleanly");
    }
}

fn edge(
    root: &Path,
    name: &str,
    ticket: &str,
    tenant: &str,
) -> (Arc<Database>, SyncClient, PathBuf) {
    let directory = root.join(name);
    std::fs::create_dir_all(&directory).expect("create participant directory");
    let db_path = directory.join(format!("{name}.db"));
    let identity = directory.join(format!("{name}.db.fabric-identity.key"));
    let db = Arc::new(Database::open(db_path).expect("open file-backed edge"));
    declare(&db);
    let client = SyncClient::new(
        db.clone(),
        &peer_dial_spec(ticket, &identity),
        TenantId::from(tenant),
    );
    (db, client, identity)
}

fn reopen_edge(
    root: &Path,
    name: &str,
    ticket: &str,
    tenant: &str,
    identity: &Path,
) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(
        Database::open(root.join(name).join(format!("{name}.db")))
            .expect("reopen file-backed edge"),
    );
    declare(&db);
    let client = SyncClient::new(
        db.clone(),
        &peer_dial_spec(ticket, identity),
        TenantId::from(tenant),
    );
    (db, client)
}

fn exact_conflict(
    result: &impl serde::Serialize,
    id: Uuid,
    mutation_kind: &str,
    winner_node_id: &str,
    winner_position: u64,
) -> serde_json::Value {
    let rendered = serde_json::to_value(result).expect("typed result serializes");
    let conflicts = rendered["conflicts"]
        .as_array()
        .expect("typed conflict list");
    assert_eq!(conflicts.len(), 1, "one refused row yields one diagnostic");
    let conflict = &conflicts[0];
    assert_eq!(conflict["table"], "notes", "diagnostic names table");
    assert_eq!(
        conflict["natural_key"],
        serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(id)))
            .expect("serialize exact natural key"),
        "diagnostic names exact key"
    );
    assert_eq!(
        conflict["mutation_kind"], mutation_kind,
        "diagnostic names mutation"
    );
    assert_eq!(
        conflict["winning_author_node_id"], winner_node_id,
        "diagnostic names authenticated winner"
    );
    assert_eq!(
        conflict["hub_acceptance_position"].as_u64(),
        Some(winner_position),
        "diagnostic keeps exact acceptance position"
    );
    conflict.clone()
}

#[tokio::test]
async fn refused_push_only_write_retires_without_downward_row_flow() {
    let (clock, _clock_guard) = MockClock::install(T0);
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "push-only-terminal-write";
    let id = Uuid::from_u128(0x9d85_0d5d_4391_42be_97cf_68df_2fc5_2111);
    let hub = start_hub(root.path(), tenant).await;
    let (winner, winner_client, winner_identity) = edge(root.path(), "winner", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&winner_identity).expect("persist winner identity");
    insert(&winner, id, "hub-accepted-winner");
    let winner_result = within(winner_client.push())
        .await
        .expect("seed winner at hub");
    let winner_position = winner_result.new_lsn.0;
    let winner_node_id = FabricIdentity::load_or_generate(&winner_identity)
        .expect("reload winner identity")
        .node_id();

    let (loser, loser_client, loser_identity) = edge(root.path(), "loser", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&loser_identity).expect("persist loser identity");
    insert(&loser, id, "one-way-losing-value");
    let source_lsn = loser.current_lsn();
    let refusal = within(loser_client.push())
        .await
        .expect("terminal one-way refusal returns its complete synchronous diagnostic");
    let diagnostic = exact_conflict(&refusal, id, "edit", &winner_node_id, winner_position);
    assert_eq!(
        refusal.applied_rows, 0,
        "a refused write does not mutate the hub"
    );
    assert_eq!(refusal.skipped_rows, 1, "the one refused write is reported");
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("hub-accepted-winner"),
        "the refused write leaves the hub's accepted winner intact"
    );
    assert!(
        !loser_client
            .has_pending_push_changes()
            .expect("inspect terminal resend state"),
        "terminal refusal leaves clean resend state"
    );
    assert_eq!(
        body(&loser, id).as_deref(),
        Some("one-way-losing-value"),
        "a push-only refusal never pulls the hub winner downward"
    );
    let table = diagnostic["table"]
        .as_str()
        .expect("diagnostic table is text");
    let retained = loser
        .point_lookup(table, "id", &Value::Uuid(id), loser.snapshot_at(source_lsn))
        .expect("read exact refused version from retained history")
        .expect("retained refused version");
    assert_eq!(
        retained.values.get("body"),
        Some(&Value::Text("one-way-losing-value".to_string())),
        "the diagnostic-addressed history contains the exact local value"
    );
    let repeat = within(loser_client.push())
        .await
        .expect("retired refusal is a no-op");
    assert_eq!(
        (
            repeat.applied_rows,
            repeat.skipped_rows,
            repeat.conflicts.len()
        ),
        (0, 0, 0),
        "clean terminal state does not resend or re-diagnose the refused write"
    );
    clock.advance(2 * 60 * 60 * 1000);
    let pruning = loser
        .run_pruning_cycle_checked()
        .expect("SYNC SAFE pruning cycle runs after terminal adjudication");
    assert_eq!(
        pruning.pruned_rows, 1,
        "terminal refusal must not leave SYNC SAFE retention blocked: {pruning:?}"
    );
    within(winner_client.shutdown()).await;
    within(loser_client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn refused_push_only_delete_survives_restart_and_retires() {
    let id = Uuid::from_u128(0x5d3d_6b53_14cc_47a9_ae23_995d_5af3_0222);
    if let Some(database_path) = std::env::var_os(DELETE_CHILD_DATABASE) {
        let identity_path = PathBuf::from(
            std::env::var_os(DELETE_CHILD_IDENTITY)
                .expect("child receives the persisted fabric identity path"),
        );
        let deleting = Database::open(PathBuf::from(database_path))
            .expect("process A opens the shared edge database");
        declare(&deleting);
        FabricIdentity::load_or_generate(&identity_path)
            .expect("process A persists the edge identity beside its database");
        insert(&deleting, id, "hub-accepted-winner");
        delete(&deleting, id);
        return;
    }

    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "push-only-terminal-delete";
    let hub = start_hub(root.path(), tenant).await;
    let (winner, winner_client, winner_identity) = edge(root.path(), "winner", &hub.ticket, tenant);
    FabricIdentity::load_or_generate(&winner_identity).expect("persist winner identity");
    insert(&winner, id, "hub-accepted-winner");
    let winner_result = within(winner_client.push())
        .await
        .expect("seed winner at hub");
    let winner_position = winner_result.new_lsn.0;
    let winner_node_id = FabricIdentity::load_or_generate(&winner_identity)
        .expect("reload winner identity")
        .node_id();

    let deleting_directory = root.path().join("deleting");
    std::fs::create_dir_all(&deleting_directory).expect("create process-A edge directory");
    let deleting_database = deleting_directory.join("deleting.db");
    let deleting_identity = deleting_directory.join("deleting.db.fabric-identity.key");
    let child = Command::new(std::env::current_exe().expect("locate integration-test binary"))
        .arg("--exact")
        .arg("refused_push_only_delete_survives_restart_and_retires")
        .arg("--nocapture")
        .env(DELETE_CHILD_DATABASE, &deleting_database)
        .env(DELETE_CHILD_IDENTITY, &deleting_identity)
        .status()
        .expect("start process A that commits the offline delete");
    assert!(
        child.success(),
        "process A must commit the offline delete and exit before process B reconnects: {child}"
    );

    let (deleting, deleting_client) = reopen_edge(
        root.path(),
        "deleting",
        &hub.ticket,
        tenant,
        &deleting_identity,
    );
    assert!(
        deleting_client
            .has_pending_push_changes()
            .expect("reopened delete obligation"),
        "process B sees process A's offline delete as pending until the hub adjudicates it"
    );
    let refusal = within(deleting_client.push())
        .await
        .expect("reconnected one-way delete returns complete diagnostic");
    exact_conflict(&refusal, id, "delete", &winner_node_id, winner_position);
    assert!(
        !deleting_client
            .has_pending_push_changes()
            .expect("terminal delete resend state"),
        "refused delete retires resend work"
    );
    assert_eq!(
        body(&deleting, id),
        None,
        "push-only refusal keeps the deleting edge absent"
    );
    assert_eq!(
        body(&hub.db, id).as_deref(),
        Some("hub-accepted-winner"),
        "hub keeps its accepted winner while the one-way edge stays absent"
    );
    let repeat = within(deleting_client.push())
        .await
        .expect("retired delete is a no-op");
    assert_eq!(
        (
            repeat.applied_rows,
            repeat.skipped_rows,
            repeat.conflicts.len()
        ),
        (0, 0, 0),
        "retired delete is never resent"
    );
    within(winner_client.shutdown()).await;
    within(deleting_client.shutdown()).await;
    hub.stop().await;
}
