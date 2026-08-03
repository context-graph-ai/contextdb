//! An authoritative purge reaches a sync-off edge while ordinary rows stay local.

use contextdb_core::{SyncDirection, TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TABLE: &str = "sync_off_notes";
const DDL: &str = "CREATE TABLE sync_off_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh operation")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare(db: &Database) {
    db.execute(DDL, &HashMap::new())
        .expect("declare the explicit two-way table");
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO sync_off_notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact local row");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM sync_off_notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact row")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(body) => body.clone(),
        value => panic!("sync_off_notes.body must be TEXT, got {value:?}"),
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
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind file-backed authoritative hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open file-backed hub"));
    declare(&db);
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
async fn authoritative_purge_reaches_sync_off_edge_while_ordinary_rows_stay_local() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "authoritative-purge-sync-off-delivery";
    let selected_id = Uuid::from_u128(0x2b44_1986_52a0_4ea7_a7f5_0e11_7d42_6c11);
    let survivor_id = Uuid::from_u128(0x3745_918f_5166_44b7_b50f_2b44_9a0d_6c12);
    let edge_control_id = Uuid::from_u128(0x6a8d_331b_7e52_4c2f_8064_9d8c_4a21_6c13);
    let hub_control_id = Uuid::from_u128(0x1f36_527a_94c9_42ca_9b8e_7261_3a56_6c14);
    let hub = start_hub(root.path(), tenant).await;
    insert(&hub.db, selected_id, "selected hub row");
    insert(&hub.db, survivor_id, "unrelated hub survivor");

    let edge_path = root.path().join("edge.db");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the stable authenticated edge identity");
    let edge = Arc::new(Database::open(&edge_path).expect("open file-backed edge"));
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    assert!(
        edge.table_meta(TABLE).is_none(),
        "the blank edge has no local table before the first authoritative pull"
    );
    within(client.pull_default())
        .await
        .expect("baseline pull installs the hub table and receives its rows");
    assert_eq!(
        edge.table_meta(TABLE)
            .expect("baseline pull installs table metadata")
            .sync_direction,
        Some(SyncDirection::Both),
        "baseline pull installs the hub's explicit two-way direction"
    );
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub row"),
        "baseline pull gives the edge the exact selected row"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "baseline pull gives the edge the exact unrelated survivor"
    );
    assert_eq!(
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id))
            .purge_frontier,
        None,
        "baseline pull does not create a selected purge frontier"
    );

    hub.db
        .execute(
            &format!("ALTER TABLE {TABLE} SET SYNC OFF"),
            &HashMap::new(),
        )
        .expect("hub sets the table sync direction off");
    assert_eq!(
        hub.db
            .table_meta(TABLE)
            .expect("hub table metadata after alter")
            .sync_direction,
        Some(SyncDirection::None),
        "the hub records its authoritative sync-off direction"
    );
    assert_eq!(
        edge.table_meta(TABLE)
            .expect("edge table metadata before transition pull")
            .sync_direction,
        Some(SyncDirection::Both),
        "before the transition pull the edge retains the former two-way direction"
    );
    within(client.pull_default())
        .await
        .expect("transition pull receives the authoritative sync-off alteration");
    for (place, db) in [("hub", &hub.db), ("edge", &edge)] {
        assert_eq!(
            db.table_meta(TABLE)
                .expect("table metadata after transition pull")
                .sync_direction,
            Some(SyncDirection::None),
            "{place} records the authoritative sync-off direction"
        );
    }
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub row"),
        "the transition pull retains the selected row before purge"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "the transition pull retains the unrelated survivor"
    );

    insert(&edge, edge_control_id, "edge-local control row");
    insert(&hub.db, hub_control_id, "hub-local control row");
    let suppressed_push = within(client.push())
        .await
        .expect("sync-off push completes without ordinary row delivery");
    assert!(
        suppressed_push.conflicts.is_empty(),
        "the suppressed sync-off push has no conflicts"
    );
    assert_eq!(
        body(&hub.db, edge_control_id),
        None,
        "sync-off push leaves the edge-local control row absent at the hub"
    );
    assert_eq!(
        body(&edge, edge_control_id).as_deref(),
        Some("edge-local control row"),
        "sync-off push retains the edge-local control row"
    );

    hub.db
        .execute(
            "PURGE FROM sync_off_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("public standalone PURGE removes the selected lineage at the hub");
    assert_eq!(
        body(&hub.db, selected_id),
        None,
        "the hub no longer serves the selected row"
    );
    assert_eq!(
        body(&hub.db, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "the hub retains the unrelated survivor"
    );
    assert_eq!(
        body(&hub.db, hub_control_id).as_deref(),
        Some("hub-local control row"),
        "the hub retains its local control row"
    );
    let hub_state = hub
        .db
        .durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id));
    let hub_root = hub_state
        .lineage_root
        .filter(|root| !root.is_empty())
        .expect("the hub exposes the selected lineage root");
    assert!(
        hub_state
            .purge_frontier
            .is_some_and(|frontier| !frontier.is_empty()),
        "the hub exposes a nonempty selected purge frontier"
    );

    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub row"),
        "before purge delivery the edge still serves the selected row"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "before purge delivery the edge retains the survivor"
    );
    assert_eq!(
        body(&edge, edge_control_id).as_deref(),
        Some("edge-local control row"),
        "before purge delivery the edge retains its local control row"
    );
    assert_eq!(
        body(&edge, hub_control_id),
        None,
        "before purge delivery the edge lacks the hub-local control row"
    );
    assert_eq!(
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id))
            .purge_frontier,
        None,
        "before purge delivery the edge has no selected purge frontier"
    );

    within(client.pull_default())
        .await
        .expect("sync-off edge receives the authoritative purge");
    assert_eq!(
        body(&edge, selected_id),
        None,
        "authoritative delivery removes the selected lineage from the edge"
    );
    let edge_state = edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id));
    assert_eq!(
        edge_state.lineage_root.as_deref(),
        Some(hub_root.as_str()),
        "the edge records the exact authoritative lineage root"
    );
    assert!(
        edge_state
            .purge_frontier
            .is_some_and(|frontier| !frontier.is_empty()),
        "the edge stores a nonempty local permanent purge frontier"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "authoritative delivery retains the unrelated survivor"
    );
    assert_eq!(
        body(&edge, edge_control_id).as_deref(),
        Some("edge-local control row"),
        "authoritative delivery retains the edge-local control row"
    );
    assert_eq!(
        body(&edge, hub_control_id),
        None,
        "ordinary hub rows remain absent at the sync-off edge"
    );
    assert_eq!(
        body(&hub.db, edge_control_id),
        None,
        "the edge-local control row remains absent at the hub"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}
