//! Two in-process nodes over the work ledger: submit on node A, claim and
//! record a result on node B, then print the claim and transfer lines.
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    JobSpec, install_work_ledger_schema, job_result, record_result, submit_job,
};
use contextdb_server::work_ledger::claim_job;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const T0: i64 = 1_700_000_000_000;
const LEASE: i64 = 5 * 60_000;
const TENANT: &str = "work-fabric-two-nodes";

fn start_hub(
    broker: &InProcessBroker,
) -> (Arc<Database>, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let hub_db = Arc::new(Database::open_memory());
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub_db.clone(),
            broker.server_as(&node_id),
            contextdb_core::TenantId::from(TENANT),
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

fn edge(broker: &InProcessBroker) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("install ledger schema");
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node_id),
        contextdb_core::TenantId::from(TENANT),
        identity,
    );
    (db, client)
}

#[tokio::main]
async fn main() {
    let broker = InProcessBroker::new();
    let (_hub_db, shutdown, task) = start_hub(&broker);
    let (a_db, a_client) = edge(&broker);
    let (b_db, b_client) = edge(&broker);

    let spec = JobSpec::builder("job-1", "describe-image", "once", "node-a")
        .submitted_at_ms(T0)
        .build();
    submit_job(&a_db, &spec, &[b"frame-bytes"]).expect("submit");
    a_client.push().await.expect("A push");

    b_client.pull_default().await.expect("B pull");
    let claim = claim_job(&b_client, "job-1", 1, "node-b", T0 + LEASE, T0)
        .await
        .expect("B claim");
    println!("claim: {claim:?}");

    record_result(
        &b_db,
        "job-1",
        1,
        "node-b",
        b"described",
        serde_json::json!({"backend": "work-fabric-two-nodes"}),
        T0 + 1,
    )
    .expect("record result");
    b_client.push().await.expect("B push result");
    a_client.pull_default().await.expect("A pull result");

    let transferred = job_result(&a_db, "job-1")
        .expect("A result")
        .expect("result row on A");
    println!(
        "transfer: node-b recorded result for {} (executor={})",
        transferred.job_id, transferred.executor_node_id
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}
