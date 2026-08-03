//! Connected relational state crosses authenticated sync as one hub acceptance.

use contextdb_core::{Lsn, TenantId, Value, VersionedRow};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "dependency-complete-sync";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh exchange")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_tables(db: &Database) {
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare parent table");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare child table");
}

fn insert_parent(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO parents (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert parent");
}

fn update_parent(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "UPDATE parents SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("update parent");
}

fn insert_child(db: &Database, id: Uuid, parent_id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent_id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert child");
}

fn row(db: &Database, table: &str, id: Uuid) -> Option<VersionedRow> {
    db.point_lookup(table, "id", &Value::Uuid(id), db.snapshot())
        .expect("point lookup")
}

struct Hub {
    db: Arc<Database>,
    endpoint: IrohServer,
    server: Arc<SyncServer>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    declare_tables(&db);
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
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task)
            .await
            .expect("authenticated Iroh hub stops within the test bound");
        self.endpoint.close().await;
    }
}

async fn reopen_edge_before_sync(
    db: Arc<Database>,
    client: SyncClient,
    edge_path: &Path,
    dial_spec: &str,
) -> (Arc<Database>, SyncClient) {
    client.shutdown().await;
    drop(client);
    drop(db);
    let reopened = Arc::new(Database::open(edge_path).expect("reopen edge database"));
    let client = SyncClient::new(reopened.clone(), dial_spec, TenantId::from(TENANT));
    (reopened, client)
}

async fn assert_final_dependency_state(reopen_before_sync: bool) {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_identity = FabricIdentity::load_or_generate(&identity_path)
        .expect("persist edge identity")
        .node_id();
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let mut edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    declare_tables(&edge);

    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    insert_parent(&edge, parent_id, "parent-v1");
    insert_child(&edge, child_id, parent_id, "child");
    update_parent(&edge, parent_id, "parent-v2");

    let mut client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT));
    if reopen_before_sync {
        (edge, client) = reopen_edge_before_sync(edge, client, &edge_path, &dial_spec).await;
    }

    let source_parent = row(&edge, "parents", parent_id).expect("final source parent");
    let source_child = row(&edge, "children", child_id).expect("source child");
    let hub_before = hub.db.current_lsn();
    let pushed = within(client.push()).await;

    let hub_parent = row(&hub.db, "parents", parent_id)
        .expect("the final parent must reach the hub with the connected child, not by itself");
    let hub_child = row(&hub.db, "children", child_id)
        .expect("the child must not be refused before its final parent reaches the hub");
    assert_eq!(
        hub_parent.values.get("body"),
        Some(&Value::Text("parent-v2".to_string())),
        "the hub keeps the final parent value rather than replaying its stale predecessor"
    );
    assert_eq!(
        hub_child.values.get("parent_id"),
        Some(&Value::Uuid(parent_id)),
        "the hub child keeps its declared parent reference"
    );
    assert_eq!(
        hub_parent.created_at, source_parent.created_at,
        "sync preserves the final parent's original timestamp"
    );
    assert_eq!(
        hub_child.created_at, source_child.created_at,
        "sync preserves the child's original timestamp"
    );

    let landed = hub.db.changes_since(hub_before);
    let dependency_lsns = landed
        .rows
        .iter()
        .filter(|change| {
            (change.table == "parents"
                && change
                    .natural_key
                    .key_values()
                    .contains(&Value::Uuid(parent_id)))
                || (change.table == "children"
                    && change
                        .natural_key
                        .key_values()
                        .contains(&Value::Uuid(child_id)))
        })
        .map(|change| change.lsn)
        .collect::<BTreeSet<Lsn>>();
    assert_eq!(
        dependency_lsns.len(),
        1,
        "the connected final state receives one hub acceptance position"
    );

    let received = hub.server.transfer_receipts();
    assert!(
        received
            .iter()
            .any(|receipt| receipt.peer_node_id == edge_identity),
        "the hub records the persisted edge identity that authenticated this state"
    );
    assert!(
        pushed.is_ok(),
        "the connected final state must be accepted: {pushed:?}"
    );

    client.shutdown().await;
    hub.stop().await;
}

async fn assert_invalid_child_refuses_connected_parent(reopen_before_sync: bool) {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let existing_parent = Uuid::new_v4();
    let conflicting_child = Uuid::new_v4();
    insert_parent(&hub.db, existing_parent, "hub-parent");
    insert_child(&hub.db, conflicting_child, existing_parent, "hub-child");

    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    declare_tables(&edge);
    let parent_id = Uuid::new_v4();
    insert_parent(&edge, parent_id, "parent-v1");
    insert_child(&edge, conflicting_child, parent_id, "conflicting-child");
    update_parent(&edge, parent_id, "parent-v2");

    let mut client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT));
    if reopen_before_sync {
        let (_reopened, reopened_client) =
            reopen_edge_before_sync(edge, client, &edge_path, &dial_spec).await;
        client = reopened_client;
    }

    let _ = within(client.push()).await;
    assert!(
        row(&hub.db, "parents", parent_id).is_none(),
        "when the child is invalid at the hub, its connected parent must not land alone"
    );
    assert_eq!(
        row(&hub.db, "children", conflicting_child)
            .expect("existing hub child")
            .values
            .get("body"),
        Some(&Value::Text("hub-child".to_string())),
        "the rejected child does not replace the hub's existing row"
    );

    client.shutdown().await;
    hub.stop().await;
}

#[tokio::test]
async fn dependency_complete_sync_preserves_final_state_and_refuses_partial_units_after_reopen() {
    assert_final_dependency_state(false).await;
    assert_final_dependency_state(true).await;
    assert_invalid_child_refuses_connected_parent(false).await;
    assert_invalid_child_refuses_connected_parent(true).await;
}
