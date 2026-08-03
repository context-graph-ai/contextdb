//! A file-backed authoritative removal must reach a bound edge after the hub restarts.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh operation")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_notes(db: &Database) {
    if db.table_meta("notes").is_none() {
        db.execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("declare an explicit two-way notes table");
    }
}

fn insert_note(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact edge-authored note");
}

fn note_body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact note")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(body) => body.clone(),
        value => panic!("notes.body must be TEXT, got {value:?}"),
    })
}

fn note_ids(db: &Database) -> BTreeSet<Uuid> {
    db.execute("SELECT id FROM notes", &HashMap::new())
        .expect("read note keys")
        .rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Uuid(id) => id,
            ref value => panic!("notes.id must be UUID, got {value:?}"),
        })
        .collect()
}

fn deletion_state(
    db: &Database,
    id: Uuid,
) -> contextdb_engine::database::DurableDeletionStateSnapshot {
    db.durable_deletion_state_for_test("notes", &Value::Uuid(id))
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind file-backed authoritative hub");
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open file-backed hub"));
    declare_notes(&db);
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
        within(self.task).await.expect("hub stops cleanly");
    }
}

#[tokio::test]
async fn authoritative_hub_purge_reaches_bound_edge_and_survives_reopen() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "authoritative-purge-delivery";
    let hub = start_hub(root.path(), tenant).await;
    let hub_node_id = hub.node_id.clone();

    let edge_path = root.path().join("edge.db");
    let edge_identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_node_id = FabricIdentity::load_or_generate(&edge_identity_path)
        .expect("persist the edge's adjacent fabric identity")
        .node_id();
    let edge = Arc::new(Database::open(&edge_path).expect("open file-backed edge"));
    declare_notes(&edge);
    let selected = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    insert_note(&edge, selected, "selected edge record");
    insert_note(&edge, survivor, "unrelated edge survivor");
    let edge_lsn = edge.current_lsn();

    let edge_dial = peer_dial_spec(&hub.ticket, &edge_identity_path);
    let client = SyncClient::new(edge.clone(), &edge_dial, TenantId::from(tenant));
    let seed = within(client.push())
        .await
        .expect("edge-originated selected row and survivor reach the hub");
    assert!(
        seed.conflicts.is_empty(),
        "initial bound-edge push is accepted"
    );
    assert_eq!(
        note_body(&hub.db, selected).as_deref(),
        Some("selected edge record"),
        "the selected row originates at the edge and reaches the hub"
    );
    assert_eq!(
        note_body(&hub.db, survivor).as_deref(),
        Some("unrelated edge survivor"),
        "the unrelated edge-authored row proves the bound path carries normal data"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(tenant), &edge_node_id)
            .expect("read hub watermark for the persisted edge identity"),
        Some(edge_lsn),
        "the hub records the edge's adjacent durable identity for this accepted push"
    );

    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .expect("authoritative public SQL removes the selected row");
    assert_eq!(
        note_body(&hub.db, selected),
        None,
        "hub no longer serves the selected row"
    );
    assert_eq!(
        note_body(&hub.db, survivor).as_deref(),
        Some("unrelated edge survivor"),
        "the hub retains the unrelated survivor"
    );
    let hub_state = deletion_state(&hub.db, selected);
    let lineage_root = hub_state
        .lineage_root
        .filter(|root| !root.is_empty())
        .expect("hub records a permanent lineage root for the removal");
    let purge_frontier = hub_state
        .purge_frontier
        .filter(|frontier| !frontier.is_empty())
        .expect("hub records a permanent removal frontier");
    assert_eq!(
        note_body(&edge, selected).as_deref(),
        Some("selected edge record"),
        "the offline edge still has the selected row before authoritative delivery"
    );
    assert_eq!(
        deletion_state(&edge, selected).purge_frontier,
        None,
        "the offline edge has not yet received a removal frontier"
    );

    within(client.shutdown()).await;
    drop(client);
    hub.stop().await;

    let restarted = start_hub(root.path(), tenant).await;
    assert_eq!(
        restarted.node_id, hub_node_id,
        "hub restart retains the same adjacent fabric identity"
    );
    assert_eq!(
        note_body(&restarted.db, selected),
        None,
        "restarted hub keeps removal absence"
    );
    assert_eq!(
        deletion_state(&restarted.db, selected)
            .lineage_root
            .as_deref(),
        Some(lineage_root.as_str()),
        "restarted hub keeps the permanent lineage root"
    );
    assert_eq!(
        deletion_state(&restarted.db, selected)
            .purge_frontier
            .as_deref(),
        Some(purge_frontier.as_str()),
        "restarted hub keeps the permanent removal frontier"
    );

    assert_eq!(
        FabricIdentity::load_or_generate(&edge_identity_path)
            .expect("reload adjacent edge identity")
            .node_id(),
        edge_node_id,
        "the same edge reconnects with its original durable identity"
    );
    let restarted_dial = peer_dial_spec(&restarted.ticket, &edge_identity_path);
    let client = SyncClient::new(edge.clone(), &restarted_dial, TenantId::from(tenant));
    within(client.pull_default())
        .await
        .expect("bound edge pulls from the restarted hub");
    assert_eq!(
        note_body(&edge, selected),
        None,
        "the selected row is removed from the edge by authoritative delivery"
    );
    assert_eq!(
        note_body(&edge, survivor).as_deref(),
        Some("unrelated edge survivor"),
        "authoritative delivery leaves the unrelated survivor exact"
    );
    assert_eq!(
        note_ids(&edge),
        BTreeSet::from([survivor]),
        "authoritative delivery removes only the selected row"
    );
    let delivered_state = deletion_state(&edge, selected);
    assert_eq!(
        delivered_state.lineage_root.as_deref(),
        Some(lineage_root.as_str()),
        "edge records the hub's permanent lineage root"
    );
    let delivered_frontier = delivered_state
        .purge_frontier
        .filter(|frontier| !frontier.is_empty())
        .expect("edge records a permanent removal frontier");

    within(client.pull_default())
        .await
        .expect("repeat pull is idempotent");
    assert_eq!(
        note_body(&edge, selected),
        None,
        "repeat pull cannot restore the selected row"
    );
    assert_eq!(
        note_body(&edge, survivor).as_deref(),
        Some("unrelated edge survivor"),
        "repeat pull leaves the survivor exact"
    );
    assert_eq!(
        deletion_state(&edge, selected).lineage_root.as_deref(),
        Some(lineage_root.as_str()),
        "repeat pull preserves the delivered lineage root"
    );
    assert_eq!(
        deletion_state(&edge, selected).purge_frontier.as_deref(),
        Some(delivered_frontier.as_str()),
        "repeat pull preserves the delivered removal frontier"
    );

    within(client.shutdown()).await;
    drop(client);
    drop(edge);
    let reopened = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    declare_notes(&reopened);
    assert_eq!(
        note_body(&reopened, selected),
        None,
        "reopened edge keeps selected row absent"
    );
    assert_eq!(
        note_body(&reopened, survivor).as_deref(),
        Some("unrelated edge survivor"),
        "reopened edge keeps the survivor exact"
    );
    assert_eq!(
        deletion_state(&reopened, selected).lineage_root.as_deref(),
        Some(lineage_root.as_str()),
        "reopened edge keeps the delivered lineage root"
    );
    assert_eq!(
        deletion_state(&reopened, selected)
            .purge_frontier
            .as_deref(),
        Some(delivered_frontier.as_str()),
        "reopened edge keeps the delivered removal frontier"
    );
    restarted.stop().await;
}
