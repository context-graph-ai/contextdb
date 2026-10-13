//! A file-backed edge bound to an in-process hub, with delivery-manifest
//! tables, shared by the custody journal checks.
#![allow(dead_code)]
use contextdb_core::{TenantId, Value};
use contextdb_engine::database::DeliveryManifest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::subjects::push_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

pub const TENANT: &str = "custody-fixture";
pub const ROOT_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER parts";
pub const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";

pub fn p() -> HashMap<String, Value> {
    HashMap::new()
}
pub fn key(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".into(), Value::Uuid(id))
}
pub fn root_id(unit: usize) -> Uuid {
    Uuid::from_u128(0x7100_0000_0000_0000_0000_0000_0000_0000 + unit as u128)
}
pub fn member_id(unit: usize) -> Uuid {
    Uuid::from_u128(0x7200_0000_0000_0000_0000_0000_0000_0000 + unit as u128)
}
pub fn expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table("records", ROOT_CLAUSES)
        .unwrap()
        .expect_table("parts", MEMBER_CLAUSES)
        .unwrap()
}
pub fn declare(db: &Database) {
    db.execute(
        &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"),
        &p(),
    )
    .unwrap();
    db.execute(
        &format!("CREATE TABLE parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) {MEMBER_CLAUSES}"),
        &p(),
    )
    .unwrap();
}
pub fn open(path: &Path, limit: Option<usize>) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: limit,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the task-scoped writer")
}
/// One unit: a root row, a member on even units, and its registration.
pub fn commit_unit(db: &Database, unit: usize) {
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("unit {unit}"))),
        ]),
    )
    .unwrap();
    let mut members = Vec::new();
    if unit.is_multiple_of(2) {
        db.insert_row(
            tx,
            "parts",
            HashMap::from([
                ("id".into(), Value::Uuid(member_id(unit))),
                ("record_id".into(), Value::Uuid(root_id(unit))),
                ("body".into(), Value::Text(format!("member {unit}"))),
            ]),
        )
        .unwrap();
        members.push(("parts", key(member_id(unit))));
    }
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: key(root_id(unit)),
            members,
        },
    )
    .unwrap();
    db.commit(tx).unwrap();
}
/// UPDATE of a registered root: retires that root's ownership.
pub fn update_root(db: &Database, unit: usize) {
    db.execute(
        "UPDATE records SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("updated {unit}"))),
        ]),
    )
    .unwrap();
}

pub struct Hub {
    pub db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Hub {
    pub async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}
pub async fn start_hub(broker: &InProcessBroker, hub_db: Arc<Database>) -> Hub {
    declare(&hub_db);
    hub_db
        .__seed_tenant_table_policies_for_test(TenantId::from(TENANT), expectation())
        .unwrap();
    let identity = Arc::new(FabricIdentity::generate());
    let node = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub_db.clone(),
            broker.server_as(&node),
            TenantId::from(TENANT),
            node,
            identity,
        )
        .unwrap(),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&push_subject(TENANT))
        .await;
    Hub {
        db: hub_db,
        shutdown,
        task,
    }
}
/// The edge's sync component, with the fabric key persisted at `key_path`.
pub fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, key_path: &Path) -> SyncClient {
    let identity = Arc::new(FabricIdentity::load_or_generate(key_path).unwrap());
    let node = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node),
        TenantId::from(TENANT),
        identity,
    )
    .unwrap()
}
/// A bound, declared file-backed edge.
pub async fn bound_edge(
    broker: &InProcessBroker,
    dir: &Path,
    limit: Option<usize>,
) -> (Arc<Database>, SyncClient) {
    let edge = Arc::new(open(&dir.join("edge.db"), limit));
    let client = edge_client(&edge, broker, &dir.join("edge.key"));
    client
        .__seed_application_table_policy_binding_for_test(
            expectation(),
            edge.declared_reader_memory().unwrap(),
        )
        .await
        .unwrap();
    declare(&edge);
    (edge, client)
}
