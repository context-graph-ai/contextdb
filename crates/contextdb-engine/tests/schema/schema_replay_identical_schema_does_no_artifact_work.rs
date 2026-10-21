#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
//! Every schema replay cost is measured at actual storage writes, independent of its planner.

include!("schema_replay_support.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_schema_replay_does_zero_actual_artifact_work() {
    let root = tempfile::tempdir().expect("independent identical-replay fixture");
    let receiver = receiver(&root.path().join("hub.db"));
    let hub = Hub::start(receiver.clone(), "schema-identical-artifacts").await;
    let first = Arc::new(Database::open(root.path().join("first.db")).unwrap());
    first.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let second = Arc::new(Database::open(root.path().join("second.db")).unwrap());
    second.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let a = hub.client(first.clone());
    let b = hub.client(second.clone());
    hub.bind(&a).await;
    hub.bind(&b).await;
    // Capture each peer's resident-table schema/data frontier before authoring
    // matching declarations. No affected-schema replay is a prerequisite here.
    pull_phase(&a, "setup-first-resident-pull").await;
    pull_phase(&b, "setup-second-resident-pull").await;
    let first_before = first.current_lsn();
    let second_before = second.current_lsn();
    for db in [&receiver, &first, &second] {
        declare_one_hundred_tables(db);
        initialize_fixture_callbacks(db);
    }
    let resident_before = resident_rows(&receiver);
    assert_eq!(schema(&first), schema(&receiver));
    assert_eq!(schema(&second), schema(&receiver));
    for (label, edge, client, since) in [
        ("first", &first, &a, first_before),
        ("second", &second, &b, second_before),
    ] {
        let expected = edge.changes_since(since).ddl;
        assert_eq!(expected.len(), 102);
        let exchange_start = hub.broker.recorded_exchanges().len();
        let pushed = ReceivedSchemaApplyObserverForTest::new();
        let pulled = ReceivedSchemaApplyObserverForTest::new();
        let push_scope = receiver.observe_received_schema_applies_for_test(&pushed);
        let pull_scope = edge.observe_received_schema_applies_for_test(&pulled);
        // Frozen design :122 and :426-436 bind one complete push + pull round.
        // Markers keep setup, each operation, and the whole round distinguishable.
        // The external admitted debug-host runner enforces <1s; local elapsed
        // time is recorded as evidence, not an uncalibrated machine threshold.
        phase(&format!("identical-{label}-round-begin"));
        let round = Instant::now();
        push_phase(client, &format!("identical-{label}-push")).await;
        pull_phase(client, &format!("identical-{label}-pull")).await;
        let round_ns = round.elapsed().as_nanos();
        println!("SCHEMA_REPLAY_ROUND_NS {label} {round_ns}");
        phase(&format!("identical-{label}-round-end"));
        drop(push_scope);
        drop(pull_scope);
        let push = pushed.storage_observation();
        let pull = pulled.storage_observation();
        assert_eq!(
            sent_ddl_since(&hub, exchange_start),
            expected,
            "SYNC OFF declarations still travel in author order; zero work cannot mean silently skipping source entries"
        );
        cost_receipt(&format!("identical-{label}-push"), &push);
        cost_receipt(&format!("identical-{label}-pull"), &pull);
        assert_zero_images(&push);
        assert_zero_images(&pull);
        assert_eq!(resident_rows(&receiver), resident_before);
        assert_eq!(schema(edge), schema(&receiver));
    }
    drop(a);
    drop(b);
    hub.stop().await;
}
