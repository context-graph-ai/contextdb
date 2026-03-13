use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::{SyncClient, SyncServer};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires nats-server"]
async fn sync_round_trip_smoke() {
    let edge = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db,
        "nats://localhost:4222",
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });

    let client = SyncClient::new(edge, "nats://localhost:4222", "test_tenant");
    let _ = client.pull(&policies).await;
}
