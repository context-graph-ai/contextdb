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
use uuid::Uuid;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded real-Iroh operation")
}

fn bind_spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}

fn notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("declare notes");
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert note");
}

fn update(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("update note");
}

fn delete(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("delete note");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    let result = db
        .execute(
            "SELECT body FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("select note");
    result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(body)) => Some(body.clone()),
        _ => None,
    })
}

#[tokio::test]
async fn explicit_memory_identity_keeps_lineage_across_insert_reconstruction_update_and_delete() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-iroh-lineage");
    let hub_identity = root.path().join("hub.key");
    let endpoint = IrohServer::bind(&bind_spec(&hub_identity))
        .await
        .expect("bind hub");
    let ticket = endpoint.ticket();
    let hub_db = Arc::new(Database::open_memory());
    notes(&hub_db);
    let hub = Arc::new(SyncServer::new(hub_db.clone(), &endpoint, tenant.clone()));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let hub = hub.clone();
        let stop = stop.clone();
        async move { hub.run_until(stop).await }
    });

    let edge = Arc::new(Database::open_memory());
    notes(&edge);
    let edge_identity = root.path().join("edge.key");
    let edge_node = FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist explicit edge identity")
        .node_id();
    let dial = peer_dial_spec(&ticket, &edge_identity);
    let id = Uuid::new_v4();
    insert(&edge, id, "created");
    let first = SyncClient::new(edge.clone(), &dial, tenant.clone());
    let inserted = within(first.push()).await.expect("push insert");
    assert!(inserted.conflicts.is_empty(), "insert has no hub conflict");
    let first_pull = within(first.pull_default())
        .await
        .expect("pull insert receipt");
    assert!(
        first_pull.conflicts.is_empty(),
        "insert pull has no hub conflict"
    );
    assert_eq!(body(&hub_db, id).as_deref(), Some("created"));
    first.shutdown().await;
    drop(first);

    update(&edge, id, "updated");
    let reconstructed = SyncClient::new(edge.clone(), &dial, tenant.clone());
    let updated = within(reconstructed.push()).await.expect("push update");
    assert!(updated.conflicts.is_empty(), "update has no hub conflict");
    assert_eq!(
        updated.applied_rows, 1,
        "update applies the reconstructed row"
    );
    assert_eq!(
        updated.skipped_rows, 0,
        "update does not skip the reconstructed row"
    );
    let update_pull = within(reconstructed.pull_default())
        .await
        .expect("pull update receipt");
    assert!(
        update_pull.conflicts.is_empty(),
        "update pull has no hub conflict"
    );
    assert_eq!(body(&hub_db, id).as_deref(), Some("updated"));
    delete(&edge, id);
    let deleted = within(reconstructed.pull_default())
        .await
        .expect("pull sends an owed delete before receiving hub rows");
    assert!(deleted.conflicts.is_empty(), "delete has no hub conflict");
    assert_eq!(body(&edge, id), None, "edge retains its local deletion");
    reconstructed.shutdown().await;

    let receipts = hub
        .transfer_receipts()
        .into_iter()
        .filter(|receipt| {
            receipt.direction == TransferDirection::Received && receipt.plane == TransferPlane::Sync
        })
        .collect::<Vec<_>>();
    assert!(
        !receipts.is_empty(),
        "hub records the authenticated memory-edge exchange"
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.peer_node_id == edge_node),
        "every received sync receipt belongs to the exact explicit edge identity"
    );
    assert!(
        hub_db
            .scan("notes", hub_db.snapshot())
            .expect("scan hub")
            .is_empty(),
        "delete reaches the hub after reconstructed-client update"
    );
    stop.store(true, Ordering::SeqCst);
    within(task).await.expect("hub stops");
}
