//! An authoritative removal crosses a one-way table's direction boundary.

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

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh operation")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
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
async fn authoritative_purge_reaches_push_only_edge_without_pulling_ordinary_rows() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "authoritative-purge-direction-delivery";
    let selected_id = Uuid::from_u128(0x8e1d_041f_81f5_4c39_8a22_49dc_8a70_0444);
    let sentinel_id = Uuid::from_u128(0x0d7a_5a8b_6fce_43ce_b4f4_94d4_3d90_0445);
    let table = "push_only_notes";
    let push_only_ddl = "CREATE TABLE push_only_notes (id UUID PRIMARY KEY, body TEXT) \
         SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST";
    let hub = start_hub(root.path(), tenant).await;
    hub.db
        .execute(push_only_ddl, &HashMap::new())
        .expect("hub declares the explicit push-only table");

    let edge_directory = root.path().join("push-only-edge");
    std::fs::create_dir_all(&edge_directory).expect("create edge directory");
    let edge_identity = edge_directory.join("push-only-edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the stable authenticated edge identity");
    let edge = Arc::new(
        Database::open(edge_directory.join("push-only-edge.db"))
            .expect("open file-backed push-only edge"),
    );
    edge.execute(push_only_ddl, &HashMap::new())
        .expect("edge declares the same explicit push-only table");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );

    edge.execute(
        "INSERT INTO push_only_notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(selected_id)),
            (
                "body".to_string(),
                Value::Text("selected edge-authored row".to_string()),
            ),
        ]),
    )
    .expect("edge authors the selected row");
    within(client.push())
        .await
        .expect("selected row reaches the authoritative hub over the one-way lane");
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
            )
            .expect("read selected row at hub")
            .rows,
        vec![vec![Value::Text("selected edge-authored row".to_string())]],
        "the ordinary push-only path carries the edge-authored selected row to the hub"
    );

    hub.db
        .execute(
            "INSERT INTO push_only_notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(sentinel_id)),
                (
                    "body".to_string(),
                    Value::Text("hub-only unpurged sentinel".to_string()),
                ),
            ]),
        )
        .expect("hub independently authors the unpurged sentinel row");
    hub.db
        .execute(
            "PURGE FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("public standalone PURGE removes the selected lineage at the hub");
    assert!(
        hub.db
            .execute(
                "SELECT id FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
            )
            .expect("read selected row at hub after purge")
            .rows
            .is_empty(),
        "public standalone PURGE removes the selected lineage at the hub"
    );
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM push_only_notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(sentinel_id))]),
            )
            .expect("read unpurged sentinel at hub")
            .rows,
        vec![vec![Value::Text("hub-only unpurged sentinel".to_string())]],
        "the public purge leaves the unrelated hub-authored sentinel unpurged"
    );
    let hub_state = hub
        .db
        .durable_deletion_state_for_test(table, &Value::Uuid(selected_id));
    let hub_root = hub_state
        .lineage_root
        .filter(|root| !root.is_empty())
        .expect("hub records the selected lineage root");
    assert!(
        hub_state
            .purge_frontier
            .is_some_and(|frontier| !frontier.is_empty()),
        "hub records a nonempty permanent purge frontier"
    );
    assert_eq!(
        edge.execute(
            "SELECT body FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("read selected row at edge before purge delivery")
        .rows,
        vec![vec![Value::Text("selected edge-authored row".to_string())]],
        "ordinary push leaves the selected row present until the authoritative purge is pulled"
    );

    within(client.pull_default())
        .await
        .expect("push-only edge receives the authoritative purge delivery");
    assert!(
        edge.execute(
            "SELECT id FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("read selected row at edge after purge delivery")
        .rows
        .is_empty(),
        "authoritative delivery removes exactly the selected lineage from the edge"
    );
    let edge_state = edge.durable_deletion_state_for_test(table, &Value::Uuid(selected_id));
    assert_eq!(
        edge_state.lineage_root.as_deref(),
        Some(hub_root.as_str()),
        "edge records the authoritative hub lineage root"
    );
    assert!(
        edge_state
            .purge_frontier
            .is_some_and(|frontier| !frontier.is_empty()),
        "edge stores a nonempty local permanent purge frontier"
    );
    assert!(
        edge.execute(
            "SELECT id FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(sentinel_id))]),
        )
        .expect("read hub-only sentinel at edge after purge delivery")
        .rows
        .is_empty(),
        "the authoritative purge arrives without pulling the ordinary hub-only row downward"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}
