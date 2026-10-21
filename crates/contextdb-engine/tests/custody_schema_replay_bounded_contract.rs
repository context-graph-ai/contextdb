//! Canonical replay cases with one unrelated row, plus one real-replication witness.
#![allow(clippy::duplicate_mod)]

const UNRELATED_RESIDENT_ROWS: i64 = 1;

#[allow(dead_code)]
#[path = "schema/schema_replay_is_proportional_to_affected_tables.rs"]
mod schema_replay_is_proportional_to_affected_tables;

#[allow(dead_code)]
#[path = "schema/schema_replay_work_limited_to_affected_tables.rs"]
mod schema_replay_work_limited_to_affected_tables;

#[allow(dead_code)]
#[path = "schema/schema_replay_identical_schema_does_no_artifact_work.rs"]
mod schema_replay_identical_schema_does_no_artifact_work;

#[allow(dead_code)]
mod replay {
    include!("schema/schema_replay_support.rs");

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn identical_authored_schema_credits_receipts_and_contacts_without_image_work() {
        use contextdb_core::TenantId;

        let root = tempfile::tempdir().unwrap();
        let receiver = receiver_with_unrelated_rows(&root.path().join("hub.db"), 1);
        let tenant = "bounded-identical-schema";
        let hub = Hub::start(receiver.clone(), tenant).await;
        let first = Arc::new(Database::open(root.path().join("first.db")).unwrap());
        let second = Arc::new(Database::open(root.path().join("second.db")).unwrap());
        let a = hub.client(first.clone());
        let b = hub.client(second.clone());
        for (edge, client) in [(&first, &a), (&second, &b)] {
            edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            hub.bind(client).await;
            pull_phase(client, "bounded-identical-resident-pull").await;
        }
        let first_before = first.current_lsn();
        let second_before = second.current_lsn();
        for db in [&receiver, &first, &second] {
            declare(
                db,
                &[
                    "CREATE TABLE bounded_identical (id INTEGER PRIMARY KEY) SYNC OFF",
                    "CREATE EVENT TYPE bounded_insert WHEN INSERT ON bounded_identical",
                    "CREATE TRIGGER bounded_insert_trigger ON bounded_identical WHEN INSERT INCLUDING SYNC",
                ],
            );
            initialize_fixture_callbacks(db);
        }
        let resident_before = rmp_serde::to_vec(&replay_resident_rows(&receiver, 1)).unwrap();
        for (label, edge, client, since) in [
            ("first", &first, &a, first_before),
            ("second", &second, &b, second_before),
        ] {
            assert_eq!(schema(edge), schema(&receiver));
            let expected = edge.changes_since(since).ddl;
            assert_eq!(
                expected.len(),
                3,
                "all locally authored declarations travel"
            );
            let authored_frontier = edge.current_lsn();
            let exchange_start = hub.broker.recorded_exchanges().len();
            let pushed = ReceivedSchemaApplyObserverForTest::new();
            let pulled = ReceivedSchemaApplyObserverForTest::new();
            let push_scope = receiver.observe_received_schema_applies_for_test(&pushed);
            push_phase(client, &format!("bounded-identical-{label}-push")).await;
            drop(push_scope);
            let pull_scope = edge.observe_received_schema_applies_for_test(&pulled);
            pull_phase(client, &format!("bounded-identical-{label}-pull")).await;
            drop(pull_scope);
            assert_eq!(sent_ddl_since(&hub, exchange_start), expected);
            assert!(client.push_watermark() >= authored_frontier);
            assert_eq!(client.pending_push_change_count().unwrap(), 0);
            assert_eq!(
                edge.persisted_sync_watermarks(&TenantId::from(tenant))
                    .unwrap(),
                (client.push_watermark(), client.pull_watermark()),
                "successful no-op replay durably credits both progress receipts"
            );
            let push = pushed.storage_observation();
            let pull = pulled.storage_observation();
            cost_receipt(label, &push);
            cost_receipt(label, &pull);
            assert_zero_images(&push);
            assert_zero_images(&pull);
            let exchanges = sent_push_request_count_since(&hub, exchange_start);
            assert!(exchanges > 0, "zero work must still send authored schema");
            assert_eq!(
                push.profile_work
                    .get("noop-operational-row-writes")
                    .copied(),
                Some(2 * exchanges),
                "each push commits its old and new contact versions on the receipt boundary"
            );
            let contact_versions: u64 = push
                .artifact_writes
                .iter()
                .filter(|(event, _)| {
                    event.table == "rel_work_node_contacts"
                        && event.operation == TableWriteOperationForTest::Insert
                })
                .map(|(_, count)| *count)
                .sum();
            assert_eq!(
                contact_versions,
                2 * exchanges,
                "actual durable contact versions"
            );
            for db in [&receiver, edge] {
                assert_eq!(
                    rmp_serde::to_vec(&replay_resident_rows(db, 1)).unwrap(),
                    resident_before,
                    "real unrelated row and 768D vector bytes remain intact"
                );
            }
            assert_eq!(schema(edge), schema(&receiver));
        }
        let contacts = receiver
            .execute(
                "SELECT node_id, last_contact_ms FROM work_node_contacts",
                &empty(),
            )
            .unwrap();
        assert_eq!(
            contacts.rows.len(),
            2,
            "both authenticated authors retain contact"
        );
        assert!(
            contacts
                .rows
                .iter()
                .all(|row| row.iter().all(|v| *v != Value::Null))
        );
        drop(a);
        drop(b);
        hub.stop().await;
    }
}
