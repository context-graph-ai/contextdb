//! Shared scaffolding for the work-ledger / distributed-job test binaries.
//!
//! `start_hub`, `edge`, `tags`, `count_rows` and the logical-time constants are
//! byte-identical across `work_ledger_tests.rs`,
//! `distributed_blob_job_worker_tests.rs`, and
//! `worker_defers_own_submission_tests.rs`, so they are hoisted here. Consumers
//! include this module with
//! `#[path = "work_ledger_support/mod.rs"] mod work_ledger_support;` and
//! `use work_ledger_support::*;`.
//!
//! Deliberately NOT hoisted (each keeps its own): the per-file `within` wrapper
//! (distinct timeout budget + message per suite) and each suite's `WorkExecutor`
//! double (`DemoExecutor` diverges between suites; `ReceivedBytesExecutor` is a
//! distinct variant).
#![allow(dead_code)]

use contextdb_engine::Database;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub const T0: i64 = 1_700_000_000_000;
pub const MINUTE: i64 = 60_000;
pub const LEASE: i64 = 5 * MINUTE;

/// A hub constructed the way the shipped binary constructs one: uniform
/// LatestWins. The ledger's own policy contract must survive this.
pub fn start_hub(
    broker: &InProcessBroker,
    tenant: &str,
) -> (Arc<Database>, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let hub_db = Arc::new(Database::open_memory());
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub_db.clone(),
            broker.server_as(&node_id),
            contextdb_core::TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    (hub_db, shutdown, task)
}

pub fn edge(broker: &InProcessBroker, tenant: &str) -> (Arc<Database>, SyncClient) {
    edge_with_identity(broker, tenant, Arc::new(FabricIdentity::generate()))
}

pub fn edge_with_identity(
    broker: &InProcessBroker,
    tenant: &str,
    identity: Arc<FabricIdentity>,
) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("install ledger schema on edge");
    let node_id = identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node_id),
        contextdb_core::TenantId::from(tenant),
        identity,
    );
    (db, client)
}

pub fn tags(list: &[&str]) -> Vec<String> {
    list.iter().map(|t| t.to_string()).collect()
}

pub fn count_rows(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &HashMap::new())
        .unwrap_or_else(|err| panic!("scan {table}: {err}"))
        .rows
        .len()
}
