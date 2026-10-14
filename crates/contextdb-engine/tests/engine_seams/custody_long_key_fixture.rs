//! A delivery-manifest root table with a very long name, beside the custody
//! fixture's own tables. A root's ownership key and its life render the
//! table's name, so every root key of this table is kilobytes long.
#![allow(dead_code)]
use crate::custody_fixture::*;
use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::database::DeliveryManifest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_server::{InProcessBroker, SyncClient};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// The long root table's name: 2,048 bytes.
pub fn long_table() -> String {
    format!("r{}", "x".repeat(2047))
}
const LONG_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER parts";

pub fn long_expectation() -> ApplicationTablePolicyExpectation {
    expectation()
        .expect_table(&long_table(), LONG_CLAUSES)
        .unwrap()
}
fn declare_long(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE {} (id UUID PRIMARY KEY, body TEXT) {LONG_CLAUSES}",
            long_table()
        ),
        &p(),
    )
    .unwrap();
}

/// A hub that also declares the long root table.
pub async fn start_long_hub(broker: &InProcessBroker) -> Hub {
    let hub = start_hub(broker, Arc::new(Database::open_memory())).await;
    declare_long(&hub.db);
    hub.db
        .__seed_tenant_table_policies_for_test(TenantId::from(TENANT), long_expectation())
        .unwrap();
    hub
}

/// A bound, declared file-backed edge with the long root table.
pub async fn bound_long_edge(broker: &InProcessBroker, dir: &Path) -> (Arc<Database>, SyncClient) {
    let edge = Arc::new(open(&dir.join("edge.db"), None));
    let client = edge_client(&edge, broker, &dir.join("edge.key"));
    client
        .__seed_application_table_policy_binding_for_test(
            long_expectation(),
            edge.declared_reader_memory().unwrap(),
        )
        .await
        .unwrap();
    declare(&edge);
    declare_long(&edge);
    (edge, client)
}

/// One registered unit rooted in the long table.
pub fn commit_long_unit(db: &Database, unit: usize) {
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        &long_table(),
        HashMap::from([
            ("id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("unit {unit}"))),
        ]),
    )
    .unwrap();
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: &long_table(),
            root_key: key(root_id(unit)),
            members: Vec::new(),
        },
    )
    .unwrap();
    db.commit(tx).unwrap();
}
