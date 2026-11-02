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
    InspectedTraining, drive_until_serving, empty, inspected_training, open_caller_driven, params,
    serving, unit_vector,
};
use contextdb_core::{Error, TenantId, Value, VectorIndexRef};
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

fn codebook(db: &Database) -> u64 {
    serving(db, "seeded_items", "embedding", &[])
        .expect("a generation serves")
        .codebook
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

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn a_training_seed_is_declared_persisted_and_inspected() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("seeded.db");
    let db = open_caller_driven(&path);
    alter(
        &db,
        &format!(
            "CREATE TABLE seeded_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) WITH (training_seed = 0))"
        ),
    );
    populate(&db);
    drive_until_serving(&db, ROWS as usize, 1, 10);
    let at_zero = training(&db);
    assert_eq!(
        (
            at_zero.declared_seed,
            at_zero.effective_seed,
            at_zero.serving_seed
        ),
        (Some(0), 0, Some(0)),
        "zero is a declared seed, used and inspected: {at_zero:?}"
    );
    assert!(
        schema(&db).contains("WITH (training_seed = 0)"),
        "{}",
        schema(&db)
    );
    let zero_codebook = codebook(&db);

    alter(
        &db,
        "ALTER TABLE seeded_items ALTER COLUMN embedding SET WITH (training_seed = 7)",
    );
    let declared = training(&db);
    assert_eq!(
        (
            declared.declared_seed,
            declared.effective_seed,
            declared.serving_seed
        ),
        (Some(7), 7, Some(0)),
        "the new seed is declared and effective; the serving generation keeps the seed it was trained with: {declared:?}"
    );
    drive_until_serving(&db, ROWS as usize, 1, 10);
    let rebuilt = training(&db);
    assert_eq!(
        rebuilt.serving_seed,
        Some(7),
        "the rebuild trains with the new seed: {rebuilt:?}"
    );
    let seven_codebook = codebook(&db);
    assert_ne!(
        seven_codebook, zero_codebook,
        "a different seed trains a different codebook"
    );

    db.close().expect("close the store");
    drop(db);
    let db = open_caller_driven(&path);
    assert_eq!(
        training(&db),
        rebuilt,
        "the declared, effective and serving seeds persist across reopen"
    );
    assert_eq!(codebook(&db), seven_codebook);
    assert!(
        schema(&db).contains("WITH (training_seed = 7)"),
        "{}",
        schema(&db)
    );

    alter(
        &db,
        "ALTER TABLE seeded_items ALTER COLUMN embedding SET WITH (training_seed = 18446744073709551615)",
    );
    assert_eq!(
        training(&db).declared_seed,
        Some(u64::MAX),
        "the largest u64 is a valid seed"
    );

    alter(
        &db,
        "ALTER TABLE seeded_items ALTER COLUMN embedding RESET WITH (training_seed)",
    );
    let derived = training(&db);
    assert_eq!(
        derived.declared_seed, None,
        "RESET WITH clears the declaration: {derived:?}"
    );
    assert!(!schema(&db).contains("training_seed"), "{}", schema(&db));
    db.close().expect("close the store");
    drop(db);
    let db = open_caller_driven(&path);
    assert_eq!(
        training(&db).effective_seed,
        derived.effective_seed,
        "an undeclared seed is derived the same way after reopen"
    );

    let before = (schema(&db), training(&db));
    for value in ["-1", "1.5", "18446744073709551616", "'7'"] {
        let sql = format!(
            "ALTER TABLE seeded_items ALTER COLUMN embedding SET WITH (training_seed = {value})"
        );
        let error = db
            .execute(&sql, &empty())
            .expect_err("an invalid seed is refused");
        assert!(
            matches!(
                &error,
                Error::InvalidVectorPolicyDeclaration { index, issue }
                    if index == &VectorIndexRef::new("seeded_items", "embedding")
                        && format!("{issue:?} {issue}").to_lowercase().contains("training_seed")
            ),
            "{sql} is the typed policy refusal naming training_seed, got {error:?}"
        );
        assert_eq!(
            (schema(&db), training(&db)),
            before,
            "{sql} changes nothing"
        );
    }
}

const TENANT: &str = "training-seed-transport";

#[tokio::test]
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
