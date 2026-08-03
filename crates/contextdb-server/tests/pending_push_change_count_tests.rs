//! A public pending-work count must exactly describe what this edge can push.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transfer_receipts::{TransferDirection, TransferPlane};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "pending-push-change-count";
const LOCAL_TABLE: &str = "local_only";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh exchange")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
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
async fn pending_push_change_count_matches_the_authenticated_iroh_push_set_across_edge_restart() {
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
    let first_count = first_client
        .pending_push_change_count()
        .expect("count local-only create and drop");
    assert_eq!(
        first_count, 2,
        "the SYNC OFF declaration and drop are real pending schema work even though no rows travel"
    );
    assert_eq!(
        first_client
            .has_pending_push_changes()
            .expect("inspect local-only create and drop"),
        first_count > 0,
        "the boolean pending probe agrees with the public exact count"
    );
    let schema_push = within(first_client.push())
        .await
        .expect("deliver the complete authenticated local-only schema history");
    assert_eq!(
        schema_push.applied_rows, 0,
        "schema delivery does not invent a data-row count"
    );
    assert!(
        hub.db.table_meta(LOCAL_TABLE).is_none(),
        "the hub applies the declaration and later drop in order"
    );
    assert_eq!(
        first_client
            .pending_push_change_count()
            .expect("count after local-only schema confirmation"),
        0,
        "confirmed schema delivery clears the pending frontier instead of leaving a phantom"
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
    let reopened_count = client
        .pending_push_change_count()
        .expect("count reopened local-only edge");
    assert_eq!(
        reopened_count, 0,
        "a full edge database reopen keeps exactly no local-only outbound entries"
    );
    assert_eq!(
        client
            .has_pending_push_changes()
            .expect("inspect reopened local-only edge"),
        reopened_count > 0,
        "the boolean pending probe remains equivalent after restart"
    );

    reopened
        .execute(
            "CREATE TABLE transit_control (id INTEGER PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .expect("create ordinary control table");
    reopened
        .execute(
            "INSERT INTO transit_control (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(1)),
                (
                    "body".to_string(),
                    Value::Text("real Iroh exchange".to_string()),
                ),
            ]),
        )
        .expect("write ordinary control row");

    let pending_count = client
        .pending_push_change_count()
        .expect("count ordinary pending control entries");
    assert_eq!(
        pending_count, 2,
        "the actual push set contains exactly the control table declaration and its control row"
    );
    assert_eq!(
        client
            .has_pending_push_changes()
            .expect("inspect ordinary control entries"),
        pending_count > 0,
        "the boolean pending probe agrees with the public exact count before push"
    );

    let pushed = within(client.push())
        .await
        .expect("push ordinary control row through authenticated Iroh");
    assert_eq!(pushed.applied_rows, 1, "the control row reaches the hub");
    assert_eq!(pushed.skipped_rows, 0, "the hub refuses no control row");
    assert!(
        pushed.conflicts.is_empty(),
        "the control row has no arbitration conflict: {pushed:?}"
    );

    let count_after_push = client
        .pending_push_change_count()
        .expect("count after confirmed push");
    assert_eq!(
        count_after_push, 0,
        "a successful push advances the durable edge watermark past every eligible entry"
    );
    assert_eq!(
        client
            .has_pending_push_changes()
            .expect("inspect after confirmed push"),
        count_after_push > 0,
        "the boolean pending probe agrees with the public exact count after push"
    );

    let received = hub
        .server
        .transfer_receipts()
        .into_iter()
        .filter(|receipt| {
            receipt.plane == TransferPlane::Sync
                && receipt.direction == TransferDirection::Received
                && receipt.counters.items > 0
        })
        .collect::<Vec<_>>();
    assert_eq!(
        received.len(),
        1,
        "one authenticated edge sent the control row"
    );
    assert_eq!(
        received[0].peer_node_id, edge_identity,
        "the hub records the edge's exact persisted database-adjacent identity"
    );
    assert_eq!(
        received[0].counters.items, 1,
        "the real Iroh receipt counts the one ordinary control row"
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
