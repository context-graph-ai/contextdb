//! A hub whose custody group index lost its completion marker refuses native
//! delivery. Transaction repair rebuilds the index complete in its own
//! transaction, so after a reopen one native delivery request is accepted and
//! answers every recorded outcome exactly.
use crate::custody_fixture::*;
use contextdb_core::TenantId;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const UNITS: usize = 3;

fn reopen_hub(path: &std::path::Path) -> Arc<Database> {
    Arc::new(open(path, None))
}

#[tokio::test(flavor = "multi_thread")]
async fn native_delivery_is_served_after_transaction_repair_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let hub_path = dir.path().join("hub.db");
    let hub_key = dir.path().join("hub.key");
    let broker = InProcessBroker::new();

    let hub_db = reopen_hub(&hub_path);
    declare(&hub_db);
    hub_db
        .__seed_tenant_table_policies_for_test(TenantId::from(TENANT), expectation())
        .unwrap();
    let hub = serve_native_hub(&broker, hub_db.clone(), &hub_key).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    for unit in 0..UNITS {
        commit_unit(&edge, unit);
    }
    // The hub receives every unit and records one terminal outcome each.
    client.push().await.unwrap();
    let fetched = client.fetch_delivery_outcomes(None).await.unwrap();
    assert_eq!(
        fetched.len(),
        UNITS,
        "a complete index serves every outcome"
    );
    let counts = hub.server.delivery_route_counts_for_test();
    println!(
        "S3_MARKER complete outcomes={} counts={counts:?}",
        fetched.len()
    );
    assert!(
        counts.0 >= 1 && counts.1 == 0,
        "served natively: {counts:?}"
    );
    hub.stop();

    // The marker is lost; a reopened hub refuses native delivery.
    hub_db.__invalidate_custody_groups_for_test().unwrap();
    Arc::into_inner(hub_db).unwrap().close().unwrap();
    let hub_db = reopen_hub(&hub_path);
    assert!(!hub_db.__custody_groups_complete_for_test().unwrap());
    let hub = serve_native_hub(&broker, hub_db.clone(), &hub_key).await;
    let refused = client.fetch_delivery_outcomes(None).await;
    let counts = hub.server.delivery_route_counts_for_test();
    println!("S3_MARKER refused={refused:?} counts={counts:?}");
    assert!(
        refused.is_err(),
        "an incomplete index is never served: {refused:?}"
    );
    assert_eq!(counts.1, 0, "no Legacy fetch answered in its place");
    hub.stop();

    // Transaction repair, then a fresh process view of the hub.
    hub_db.__repair_transaction_order_for_test().unwrap();
    assert!(hub_db.__custody_groups_complete_for_test().unwrap());
    Arc::into_inner(hub_db).unwrap().close().unwrap();
    let hub_db = reopen_hub(&hub_path);
    assert!(hub_db.__custody_groups_complete_for_test().unwrap());
    let hub = serve_native_hub(&broker, hub_db.clone(), &hub_key).await;
    let fetched = client.fetch_delivery_outcomes(None).await.unwrap();
    let counts = hub.server.delivery_route_counts_for_test();
    println!(
        "S3_MARKER repaired outcomes={} counts={counts:?}",
        fetched.len()
    );
    assert_eq!(fetched.len(), UNITS, "every recorded outcome, exactly once");
    assert_eq!(counts, (1, 0), "one request, served by the native route");
    hub.stop();
    drop(client);
    drop(edge);
    drop(hub_db);
}
