//! A vector column's search policy can change over sync without costing the
//! receiver its search graphs.
//!
//! Changing `SEARCH_MODE` rebuilds no vector data or graph, and changing
//! `AUTO_INDEX_AT` changes when a graph is built, not the graph already built.
//! So a receiver that pulls only such an ALTER keeps answering `USE VECTOR
//! INDEXED` from the graphs it maintained before, with no maintenance run in
//! between, and adopts the new policy. Driven through the authenticated pull
//! path, the same received-schema transaction a real edge uses.

use contextdb_core::{TenantId, Value, VectorSearchMode};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 32;
const TENANT: &str = "synced-vector-policy-change";
const SCHEMA: &str = "CREATE TABLE policy_items (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

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

fn indexed_search(
    db: &Database,
    scope: Uuid,
    query: Vec<f32>,
) -> contextdb_core::Result<Vec<Uuid>> {
    db.execute(
        "SELECT id FROM policy_items WHERE scope_id = $scope \
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
        "INSERT INTO policy_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(embedding)),
        ]),
    )
    .expect("write one source vector");
}

fn embedding_column(db: &Database) -> contextdb_core::ColumnDef {
    db.table_meta("policy_items")
        .expect("the receiver holds the table")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column")
}

/// Pull the initial image, build the receiver's graphs, then pull `alter`
/// alone and return the receiver for inspection (no maintenance after the
/// second pull).
async fn receiver_after_synced(alter: &str) -> (Arc<Database>, [(Uuid, Uuid, Vec<f32>); 2]) {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let edge = Arc::new(Database::open_memory());
    let rows = [
        (
            Uuid::from_u128(0xA101),
            Uuid::from_u128(0xA1),
            vec![1.0, 0.0, 0.0],
        ),
        (
            Uuid::from_u128(0xA102),
            Uuid::from_u128(0xA2),
            vec![0.0, 0.0, 1.0],
        ),
    ];
    source
        .execute(SCHEMA, &HashMap::new())
        .expect("declare the source vector table");
    for (id, scope, embedding) in &rows {
        insert(&source, *id, *scope, embedding.clone());
    }
    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id,
            hub_identity,
        )
        .expect("construct the authenticated sync endpoint"),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    let edge_identity = Arc::new(FabricIdentity::generate());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_identity.node_id()),
        TenantId::from(TENANT),
        edge_identity,
    )
    .expect("construct the authenticated sync client");

    client
        .pull_default()
        .await
        .expect("first pull installs the table");
    edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        edge.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle succeeds");
    }
    for (id, scope, embedding) in &rows {
        assert_eq!(
            indexed_search(&edge, *scope, embedding.clone())
                .expect("the receiver has its graphs before the policy change"),
            vec![*id]
        );
    }

    source
        .execute(alter, &HashMap::new())
        .expect("change only the vector column's search policy at the source");
    client
        .pull_default()
        .await
        .expect("pull the policy-only ALTER");

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
    drop(server);
    (edge, rows)
}

#[tokio::test]
async fn a_synced_search_mode_change_keeps_indexed_answering_without_maintenance() {
    let (edge, rows) = receiver_after_synced(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH_MODE AUTO",
    )
    .await;
    assert_eq!(
        embedding_column(&edge).search_mode,
        VectorSearchMode::Auto,
        "the receiver adopts the new search mode"
    );
    for (id, scope, embedding) in &rows {
        assert_eq!(
            indexed_search(&edge, *scope, embedding.clone()).unwrap_or_else(|error| panic!(
                "a search-mode change rebuilds no graph, so INDEXED still answers \
                 without maintenance: {error}"
            )),
            vec![*id]
        );
    }
}

#[tokio::test]
async fn a_synced_auto_index_at_change_keeps_indexed_answering_without_maintenance() {
    let (edge, rows) = receiver_after_synced(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT 7",
    )
    .await;
    assert_eq!(
        embedding_column(&edge).auto_index_at,
        Some(7),
        "the receiver adopts the new AUTO_INDEX_AT"
    );
    for (id, scope, embedding) in &rows {
        assert_eq!(
            indexed_search(&edge, *scope, embedding.clone()).unwrap_or_else(|error| panic!(
                "an AUTO_INDEX_AT change keeps the built graph, so INDEXED still answers \
                 without maintenance: {error}"
            )),
            vec![*id]
        );
    }
}
