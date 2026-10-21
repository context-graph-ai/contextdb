#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
//! Every schema replay cost is measured at actual storage writes, independent of its planner.

include!("schema_replay_support.rs");
use std::collections::BTreeSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_replay_is_proportional_to_affected_tables() {
    let root = tempfile::tempdir().expect("different-table replay fixture");
    let receiver = receiver(&root.path().join("hub.db"));
    let before = resident_rows(&receiver);
    let hub = Hub::start(receiver.clone(), "schema-distinct-artifacts").await;
    let source = Arc::new(Database::open(root.path().join("source.db")).unwrap());
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let peer = Arc::new(Database::open(root.path().join("peer.db")).unwrap());
    peer.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let client = hub.client(source.clone());
    let peer_client = hub.client(peer.clone());
    hub.bind(&client).await;
    hub.bind(&peer_client).await;
    for db in [&receiver, &source, &peer] {
        declare_one_hundred_tables(db);
        initialize_fixture_callbacks(db);
    }
    pull_phase(&client, "setup-distinct-source-pull").await;
    pull_phase(&peer_client, "setup-distinct-peer-pull").await;
    let since = source.current_lsn();
    let affected = (0..100)
        .map(|ordinal| format!("different_{ordinal:03}"))
        .collect::<BTreeSet<_>>();
    for table in &affected {
        source
            .execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY) SYNC OFF"),
                &empty(),
            )
            .unwrap();
    }
    let entries = source.changes_since(since).ddl.len();
    assert_eq!(
        entries, 100,
        "one hundred distinct declarations, one per commit"
    );
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    let exchange_start = hub.broker.recorded_exchanges().len();
    push_phase(&client, "distinct-replay-push").await;
    drop(scope);
    let observed = trace.observation().expect("actual received schema apply");
    cost_receipt("different-push", &observed);
    let contact_exchanges = sent_push_request_count_since(&hub, exchange_start);
    assert_eq!(contact_exchanges, entries as u64);
    assert_affected_only(&observed, &affected, contact_exchanges);
    assert_eq!(observed.staged_rows_copied, 0);
    assert_eq!(observed.staged_vectors_copied, 0);
    assert_table_work_bound(&observed, entries);
    let pull_trace = ReceivedSchemaApplyObserverForTest::new();
    let pull_scope = peer.observe_received_schema_applies_for_test(&pull_trace);
    pull_phase(&peer_client, "distinct-replay-peer-pull").await;
    drop(pull_scope);
    let pulled = pull_trace.observation().expect("real pull schema apply");
    assert_affected_only(&pulled, &affected, 0);
    cost_receipt("different-pull", &pulled);
    assert_table_work_bound(&pulled, entries);
    pull_phase(&client, "distinct-reconcile-pull").await;
    assert_eq!(schema(&source), schema(&receiver));
    assert_eq!(schema(&peer), schema(&receiver));
    assert_eq!(resident_rows(&receiver), before);
    drop(client);
    drop(peer_client);
    hub.stop().await;
}
