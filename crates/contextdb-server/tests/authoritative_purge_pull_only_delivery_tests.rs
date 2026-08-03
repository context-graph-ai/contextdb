//! An authoritative purge reaches a pull-only edge with ordinary row delivery.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TABLE: &str = "pull_only_notes";
const DDL: &str = "CREATE TABLE pull_only_notes (id UUID PRIMARY KEY, body TEXT) \
    SYNC PULL ONLY SYNC CONFLICT KEEP LATEST";

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
        .expect("declare the explicit pull-only table");
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO pull_only_notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact hub-authored row");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM pull_only_notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact row")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(body) => body.clone(),
        value => panic!("pull_only_notes.body must be TEXT, got {value:?}"),
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
async fn authoritative_purge_reaches_pull_only_edge_alongside_ordinary_rows() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "authoritative-purge-pull-only-delivery";
    let selected_id = Uuid::from_u128(0x53ac_5af8_d5b1_45d3_a67d_76ca_4ee4_5991);
    let survivor_id = Uuid::from_u128(0x6415_d69f_9964_46ce_9d42_59d2_d5f9_5992);
    let control_id = Uuid::from_u128(0x2d9d_f4f2_5bbc_4a1b_8554_9f4c_5b7b_5993);
    let hub = start_hub(root.path(), tenant).await;
    insert(&hub.db, selected_id, "selected hub-authored row");
    insert(&hub.db, survivor_id, "unrelated hub-authored survivor");

    let edge_path = root.path().join("edge.db");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the stable authenticated edge identity");
    let edge = Arc::new(Database::open(&edge_path).expect("open file-backed edge"));
    declare(&edge);
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );

    within(client.pull_default())
        .await
        .expect("baseline pull receives the hub-authored rows");
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub-authored row"),
        "baseline pull gives the edge the exact selected hub row"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub-authored survivor"),
        "baseline pull gives the edge the exact unrelated survivor"
    );
    let baseline_edge_state =
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id));
    assert_eq!(
        baseline_edge_state.purge_frontier, None,
        "the baseline edge has not already recorded a purge for the selected row"
    );

    hub.db
        .execute(
            "PURGE FROM pull_only_notes WHERE id = $id",
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
        Some("unrelated hub-authored survivor"),
        "the hub retains the unrelated survivor"
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
    insert(&hub.db, control_id, "ordinary hub control row");

    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub-authored row"),
        "before the second pull the edge still serves the selected row"
    );
    assert_eq!(
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id))
            .purge_frontier,
        None,
        "before the second pull the edge has no selected purge frontier"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub-authored survivor"),
        "before the second pull the edge retains the survivor"
    );
    assert_eq!(
        body(&edge, control_id),
        None,
        "before the second pull the edge lacks the new ordinary control row"
    );

    within(client.pull_default())
        .await
        .expect("second pull delivers the purge and the ordinary control row");
    assert_eq!(
        body(&edge, selected_id),
        None,
        "the second pull removes the selected lineage from the edge"
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
        Some("unrelated hub-authored survivor"),
        "the second pull retains the unrelated survivor"
    );
    assert_eq!(
        body(&edge, control_id).as_deref(),
        Some("ordinary hub control row"),
        "the second pull also delivers the new ordinary control row"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}
