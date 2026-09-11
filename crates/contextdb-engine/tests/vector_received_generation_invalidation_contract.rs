//! A received complete image must retire a receiver's old maintained route.
//!
//! This uses the authenticated pull path, rather than raw `apply_changes`, so
//! the receiver enters the shipped received-schema replacement transaction.

#![cfg(feature = "test-seams")]

use contextdb_core::{Error, TenantId, Value};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 32;
const TENANT: &str = "received-vector-generation-invalidation";
const SCHEMA: &str = "CREATE TABLE received_vector_items (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn exact_search(db: &Database, scope: Uuid, query: Vec<f32>) -> Vec<Uuid> {
    ids(&db
        .execute(
            "SELECT id FROM received_vector_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("exact vector search: {error}")))
}

fn indexed_search(db: &Database, scope: Uuid, query: Vec<f32>) -> Result<Vec<Uuid>, Error> {
    db.execute(
        "SELECT id FROM received_vector_items WHERE scope_id = $scope \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
        &params([
            ("scope", Value::Uuid(scope)),
            ("query", Value::Vector(query)),
        ]),
    )
    .map(|result| ids(&result))
}

fn insert(db: &Database, id: Uuid, scope: Uuid, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO received_vector_items (id, scope_id, embedding) \
         VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(embedding)),
        ]),
    )
    .expect("write one deterministic source vector");
}

fn drive_maintenance(db: &Database) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle succeeds");
    }
}

/// A complete received image replaces both raw vectors and their maintained
/// routes.  Until finite maintenance creates a route for the new image,
/// INDEXED must refuse rather than consult the receiver's old saved route.
#[tokio::test]
async fn complete_received_image_invalidates_stale_maintained_vector_generations() {
    let root = TempDir::new().expect("temporary durable edge directory");
    let edge_path = root.path().join("received-generation-invalidation.redb");
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let edge = Arc::new(Database::open(&edge_path).expect("open durable receiver"));
    let scope = Uuid::from_u128(0xE101);
    let old_id = Uuid::from_u128(0xE102);
    let replacement_id = Uuid::from_u128(0xE103);

    source
        .execute(SCHEMA, &empty())
        .expect("declare the source vector table");
    insert(&source, old_id, scope, vec![1.0, 0.0, 0.0]);

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id,
            hub_identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    );

    client
        .pull_default()
        .await
        .expect("first authenticated pull installs the initial complete image");
    edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    drive_maintenance(&edge);
    assert_eq!(
        indexed_search(&edge, scope, vec![1.0, 0.0, 0.0])
            .expect("the receiver has a maintained route before replacement"),
        vec![old_id]
    );

    // The schema entry makes the next authenticated pull use the received
    // complete-image transaction. Its accepted rows now contain only the
    // replacement owner/vector; the original owner is gone at the source.
    source
        .execute(
            "ALTER TABLE received_vector_items ADD COLUMN source_note TEXT",
            &empty(),
        )
        .expect("make the next pull a received-schema replacement");
    source
        .execute(
            "DELETE FROM received_vector_items WHERE id = $id",
            &params([("id", Value::Uuid(old_id))]),
        )
        .expect("remove the old source vector before the replacement image");
    insert(&source, replacement_id, scope, vec![0.0, 1.0, 0.0]);

    client
        .pull_default()
        .await
        .expect("authenticated received-schema pull replaces the receiver image");

    assert_eq!(
        exact_search(&edge, scope, vec![0.0, 1.0, 0.0]),
        vec![replacement_id],
        "EXACT reads the replacement image rather than a stale maintained route"
    );
    assert_ne!(
        exact_search(&edge, scope, vec![1.0, 0.0, 0.0]),
        vec![old_id],
        "the old vector owner is absent from the received replacement image"
    );
    assert!(
        matches!(
            indexed_search(&edge, scope, vec![0.0, 1.0, 0.0]),
            Err(Error::VectorIndexedRouteUnavailable { .. })
        ),
        "INDEXED must refuse until maintenance creates a route for the replacement image, \
         rather than use the old base/change/catalog route"
    );

    drive_maintenance(&edge);
    assert_eq!(
        indexed_search(&edge, scope, vec![0.0, 1.0, 0.0])
            .expect("finite maintenance creates the replacement route"),
        vec![replacement_id]
    );

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
    drop(server);
    edge.close().expect("close replacement receiver cleanly");
    drop(edge);

    let reopened = Database::open(&edge_path).expect("reopen replacement receiver");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        exact_search(&reopened, scope, vec![0.0, 1.0, 0.0]),
        vec![replacement_id],
        "reopen retains the replacement vector"
    );
    assert_ne!(
        exact_search(&reopened, scope, vec![1.0, 0.0, 0.0]),
        vec![old_id],
        "reopen never resurrects the original vector identity"
    );
    assert_eq!(
        indexed_search(&reopened, scope, vec![0.0, 1.0, 0.0])
            .expect("reopen retains only the route built for the replacement image"),
        vec![replacement_id],
        "the saved route after reopen answers from the replacement image, never the old identity"
    );
}
