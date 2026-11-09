//! TRAINING_SEED is declared through the column's WITH carrier, persisted,
//! inspected and transported with synced DDL like every policy member.
//! Inspection shows the declared seed (none when undeclared), the effective
//! seed the next build uses (the declared one, or a seed derived from the
//! partition's own identity when undeclared, the same after reopen), and
//! the seed the serving generation was trained with; a changed seed takes
//! effect when a rebuild publishes, with a different codebook. Zero and the
//! largest u64 are valid seeds; a negative, fractional or unrepresentable
//! seed is the typed policy refusal naming the member and changes nothing.

use super::support::{
    InspectedTraining, drive_until_serving, empty, inspected_training, params, unit_vector,
};
use contextdb_core::{TenantId, Value};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const DIMENSIONS: usize = 8;
const ROWS: i64 = 600;

fn schema(db: &Database) -> String {
    let meta = db
        .table_meta("seeded_items")
        .expect("table metadata exists");
    render_table_meta("seeded_items", &meta)
}

fn training(db: &Database) -> InspectedTraining {
    inspected_training(db, "seeded_items", "embedding", &[])
}

fn populate(db: &Database) {
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO seeded_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

fn alter(db: &Database, sql: &str) {
    db.execute(sql, &empty())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

const TENANT: &str = "training-seed-transport";

#[tokio::test]
#[ignore = "A training seed does not travel with synced table DDL."]
async fn a_training_seed_travels_with_synced_ddl() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let edge = Arc::new(Database::open_memory());
    for db in [&source, &edge] {
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    }
    alter(
        &source,
        &format!(
            "CREATE TABLE seeded_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) WITH (training_seed = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST"
        ),
    );
    populate(&source);

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

    client.pull_default().await.expect("the first pull");
    assert_eq!(
        schema(&edge),
        schema(&source),
        "the receiver holds the sender's declaration"
    );
    drive_until_serving(&edge, ROWS as usize, 1, 10);
    let received = training(&edge);
    assert_eq!(
        (
            received.declared_seed,
            received.effective_seed,
            received.serving_seed
        ),
        (Some(7), 7, Some(7)),
        "the receiver declares, uses and serves the transported seed: {received:?}"
    );

    alter(
        &source,
        "ALTER TABLE seeded_items ALTER COLUMN embedding SET WITH (training_seed = 0)",
    );
    client.pull_default().await.expect("pull the change");
    assert_eq!(schema(&edge), schema(&source));
    assert_eq!(
        (
            training(&edge).declared_seed,
            training(&edge).effective_seed
        ),
        (Some(0), 0),
        "the transported zero seed is declared on the receiver"
    );
    drive_until_serving(&edge, ROWS as usize, 1, 10);
    assert_eq!(
        training(&edge).serving_seed,
        Some(0),
        "the receiver rebuilds with the new seed"
    );

    alter(
        &source,
        "ALTER TABLE seeded_items ALTER COLUMN embedding RESET WITH (training_seed)",
    );
    client.pull_default().await.expect("pull the reset");
    assert_eq!(schema(&edge), schema(&source));
    assert_eq!(
        training(&edge).declared_seed,
        None,
        "the transported reset clears the receiver's seed"
    );

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
}
