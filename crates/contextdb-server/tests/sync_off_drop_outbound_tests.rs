//! A dropped local-only table must never become outbound sync work.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::DdlChange;
use contextdb_server::transfer_receipts::{TransferDirection, TransferPlane};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "sync-off-drop";
const LOCAL_TABLE: &str = "local_only";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh exchange")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn is_local_table_create_or_drop(change: &DdlChange) -> bool {
    matches!(
        change,
        DdlChange::CreateTable { name, .. } | DdlChange::DropTable { name, .. }
            if name == LOCAL_TABLE
    )
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
    let db_path = root.join("hub.db");
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(db_path).expect("open hub database"));
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

#[tokio::test]
async fn dropping_a_sync_off_table_never_creates_outbound_work_after_full_edge_database_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let edge_identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_identity = FabricIdentity::load_or_generate(&edge_identity_path)
        .expect("persist edge identity")
        .node_id();
    let edge_dial_spec = peer_dial_spec(&hub.ticket, &edge_identity_path);

    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    edge.execute(
        "CREATE TABLE local_only (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &HashMap::new(),
    )
    .expect("declare local-only table");
    edge.execute("DROP TABLE local_only", &HashMap::new())
        .expect("drop local-only table");

    let first_client = SyncClient::new(edge.clone(), &edge_dial_spec, TenantId::from(TENANT));
    assert!(
        !first_client
            .has_pending_push_changes()
            .expect("inspect local-only create and drop"),
        "creating then dropping a SYNC OFF table leaves exactly no outbound work"
    );
    first_client.shutdown().await;
    drop(first_client);
    drop(edge);

    let reopened = Arc::new(Database::open(&edge_path).expect("reopen edge database"));
    assert!(
        reopened.table_meta(LOCAL_TABLE).is_none(),
        "the dropped local-only table stays absent after the edge reopens"
    );
    let client = SyncClient::new(reopened.clone(), &edge_dial_spec, TenantId::from(TENANT));
    assert!(
        !client
            .has_pending_push_changes()
            .expect("inspect reopened edge"),
        "the reopened edge keeps exactly no outbound work for the local-only create and drop"
    );

    reopened
        .execute(
            "CREATE TABLE transit_control (id INTEGER PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .expect("create ordinary control table");
    let control_row = HashMap::from([
        ("id".to_string(), Value::Int64(1)),
        (
            "body".to_string(),
            Value::Text("real Iroh exchange".to_string()),
        ),
    ]);
    reopened
        .execute(
            "INSERT INTO transit_control (id, body) VALUES ($id, $body)",
            &control_row,
        )
        .expect("write ordinary control row");

    let pushed = within(client.push())
        .await
        .expect("push ordinary control row through authenticated Iroh");
    assert_eq!(pushed.applied_rows, 1, "the control row reaches the hub");
    assert_eq!(pushed.skipped_rows, 0, "the hub refuses no control row");
    assert!(
        pushed.conflicts.is_empty(),
        "the control row has no arbitration conflict: {pushed:?}"
    );

    let receipts = hub.server.transfer_receipts();
    let received = receipts
        .iter()
        .filter(|receipt| {
            receipt.plane == TransferPlane::Sync && receipt.direction == TransferDirection::Received
        })
        .collect::<Vec<_>>();
    assert_eq!(
        received.len(),
        1,
        "one authenticated edge sent the control row"
    );
    assert_eq!(
        received[0].peer_node_id, edge_identity,
        "the hub authenticated the edge's database-adjacent persisted identity"
    );
    assert_eq!(
        received[0].counters.items, 1,
        "the real Iroh receipt counts only the ordinary control row"
    );
    assert!(
        hub.db.table_meta(LOCAL_TABLE).is_none(),
        "the local-only table was never created on the hub"
    );
    assert!(
        hub.db
            .changes_since(Lsn(0))
            .ddl
            .iter()
            .all(|change| !is_local_table_create_or_drop(change)),
        "the hub received neither a create nor a drop for the local-only table"
    );
    let hub_control = hub
        .db
        .execute(
            "SELECT body FROM transit_control WHERE id = 1",
            &HashMap::new(),
        )
        .expect("read hub control row");
    assert_eq!(
        hub_control.rows,
        vec![vec![Value::Text("real Iroh exchange".to_string())]],
        "hub state confirms the authenticated control exchange completed"
    );

    client.shutdown().await;
    drop(client);
    hub.stop().await;
}
