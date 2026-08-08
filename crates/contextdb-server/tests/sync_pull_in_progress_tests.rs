//! Library-level half of the CLI pull-liveness correction (owner-folded,
//! 2026-08-06): `SyncClient` gains a `pull_in_progress()` flag so a caller
//! holding the SAME client handle across threads — an embedding consumer,
//! not the CLI's own single-blocking-session process — can observe a pull
//! genuinely in flight, not just before/after it.
//!
//! Uses the existing `pause_after_pull_response_for_test` seam (already
//! proven by the transport adapter's own test suite and by
//! `destination_reupload_safety_tests.rs`) to pause a real pull
//! deterministically right after a page response lands,
//! so the mid-pull check below is a genuine synchronization point, never a
//! sleep-and-hope race.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DDL: &str =
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), fut)
        .await
        .expect("bounded sync exchange exceeded 20s")
}

struct RunningHub {
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn start_hub(broker: &InProcessBroker, tenant: &str, hub_db: Arc<Database>) -> RunningHub {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub_db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
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
    RunningHub { shutdown, task }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_in_progress_is_true_only_while_a_pull_is_running() {
    let broker = InProcessBroker::new();
    let tenant = "pull-in-progress";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    hub_db
        .execute("INSERT INTO notes (id, body) VALUES (1, 'x')", &p())
        .expect("hub seed row, so a real pull response is served");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    let edge_db = Arc::new(Database::open_memory());
    edge_db.execute(DDL, &p()).expect("edge ddl");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let node_id = edge_identity.node_id();
    let transport = broker.client_as(&node_id);
    let edge_client = Arc::new(
        SyncClient::with_authenticated_transport_and_identity_for_test(
            edge_db.clone(),
            transport,
            TenantId::from(tenant),
            edge_identity,
        ),
    );

    assert!(
        !edge_client.pull_in_progress(),
        "no pull has started yet, so pull_in_progress must read false"
    );

    let pause = edge_client.pause_after_pull_response_for_test();
    let pull_task = tokio::spawn({
        let client = edge_client.clone();
        async move { client.pull_default().await }
    });

    within(pause.wait_until_reached()).await;
    assert!(
        edge_client.pull_in_progress(),
        "a page response is being processed right now (the deterministic pause \
         has been reached) — pull_in_progress must read true at this point, \
         observable from a caller sharing this SyncClient handle on another task"
    );

    pause.release();
    let result = within(pull_task)
        .await
        .expect("pull task must not panic")
        .expect("pull must succeed");
    assert_eq!(result.applied_rows, 1, "the seeded row must have applied");

    assert!(
        !edge_client.pull_in_progress(),
        "the pull has fully completed, so pull_in_progress must read false again"
    );

    hub.stop().await;
}
