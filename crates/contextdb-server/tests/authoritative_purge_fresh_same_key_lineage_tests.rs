use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::database::AuthoritativePurgeRootClassification;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TABLE: &str = "notes";
const DDL: &str =
    "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh operation")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact note");
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
        Value::Text(body) => body.clone(),
        value => panic!("notes.body must be TEXT, got {value:?}"),
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
    db.execute(DDL, &HashMap::new())
        .expect("hub solely declares the two-way notes table");
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
async fn fresh_same_key_insert_after_authoritative_purge_starts_new_lineage_and_syncs() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "fresh-same-key-after-authoritative-purge";
    let selected_id = Uuid::from_u128(0x6f5b_8af6_c469_49a8_b1c0_5135_c77d_1001);
    let survivor_id = Uuid::from_u128(0x79d2_aeb9_1bc3_4e0f_8a5d_863d_c77d_1002);
    let selected_key = NaturalKey::single("id".to_string(), Value::Uuid(selected_id));
    let hub = start_hub(root.path(), tenant).await;
    insert(&hub.db, selected_id, "selected hub row");
    insert(&hub.db, survivor_id, "unrelated hub survivor");

    let edge_path = root.path().join("edge.db");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge_node_id = FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist stable authenticated edge identity")
        .node_id();
    let edge = Arc::new(Database::open(&edge_path).expect("open blank file-backed edge"));
    assert!(
        edge.table_meta(TABLE).is_none(),
        "the edge starts blank so the hub is the sole schema author"
    );
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );

    within(client.pull_default())
        .await
        .expect("baseline pull installs the hub schema and exact rows");
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub row")
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "baseline pull preserves the unrelated survivor"
    );
    let old_hub_sidecar = hub
        .db
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("hub exposes the selected live lineage before purge");
    let old_root = old_hub_sidecar.lineage_root.clone();
    assert!(
        !old_root.is_empty(),
        "the selected live lineage root is nonempty"
    );

    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("public authoritative purge removes the selected row");
    within(client.pull_default())
        .await
        .expect("edge receives the authoritative purge before any new write");
    assert_eq!(
        body(&edge, selected_id),
        None,
        "purge removes the selected row"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "purge leaves the unrelated survivor unchanged"
    );
    let edge_purge_state = edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id));
    let edge_purge_frontier = edge_purge_state
        .purge_frontier
        .filter(|frontier| !frontier.is_empty())
        .expect("the edge persists a nonempty permanent purge frontier");
    assert!(matches!(
        edge.classify_authoritative_purge_root_for_test(
            TABLE,
            old_hub_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));

    insert(&edge, selected_id, "fresh edge same-key row");
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("fresh edge same-key row"),
        "explicit INSERT makes the fresh same-key body visible before sync"
    );
    assert!(matches!(
        edge.classify_authoritative_purge_root_for_test(
            TABLE,
            old_hub_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id))
            .purge_frontier
            .as_deref(),
        Some(edge_purge_frontier.as_str()),
        "fresh insertion leaves the permanent old-lineage frontier intact"
    );

    let pushed = within(client.push())
        .await
        .expect("fresh same-key lineage pushes to the hub");
    assert!(
        pushed
            .conflicts
            .iter()
            .all(|conflict| conflict.reason.as_deref() != Some("purged_lineage")),
        "the fresh root is never reported as a purged-lineage conflict"
    );
    let fresh_edge_sidecar = edge
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("fresh INSERT has a live lineage sidecar after its accepted push");
    let fresh_root = fresh_edge_sidecar.lineage_root.clone();
    assert!(fresh_edge_sidecar.locally_created);
    assert_eq!(fresh_edge_sidecar.author_node_id, edge_node_id);
    assert!(!fresh_root.is_empty());
    assert_ne!(
        fresh_root, old_root,
        "same-key INSERT starts a fresh lineage"
    );
    assert_eq!(
        edge.classify_authoritative_purge_root_for_test(
            TABLE,
            fresh_edge_sidecar.table_generation,
            &selected_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );
    assert!(matches!(
        edge.classify_authoritative_purge_root_for_test(
            TABLE,
            old_hub_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        body(&hub.db, selected_id).as_deref(),
        Some("fresh edge same-key row"),
        "hub accepts the fresh same-key row"
    );
    assert_eq!(
        body(&hub.db, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "hub retains the unrelated survivor"
    );
    let fresh_hub_sidecar = hub
        .db
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("hub stores the accepted fresh live lineage");
    assert!(!fresh_hub_sidecar.locally_created);
    assert_eq!(fresh_hub_sidecar.author_node_id, edge_node_id);
    assert_eq!(fresh_hub_sidecar.lineage_root, fresh_root);
    assert!(matches!(
        hub.db.classify_authoritative_purge_root_for_test(
            TABLE,
            old_hub_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        hub.db.classify_authoritative_purge_root_for_test(
            TABLE,
            fresh_hub_sidecar.table_generation,
            &selected_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );

    within(client.pull_default())
        .await
        .expect("ordinary pull retains the fresh same-key row");
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("fresh edge same-key row")
    );
    assert_eq!(
        edge.authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
            .expect("ordinary pull retains the fresh live lineage")
            .lineage_root,
        fresh_root
    );
    assert!(matches!(
        edge.classify_authoritative_purge_root_for_test(
            TABLE,
            old_hub_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        edge.classify_authoritative_purge_root_for_test(
            TABLE,
            fresh_edge_sidecar.table_generation,
            &selected_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor")
    );

    within(client.shutdown()).await;
    hub.stop().await;
}
