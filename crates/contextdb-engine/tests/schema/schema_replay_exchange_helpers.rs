// Shared replay observations preserve the same wire and storage-work assertions.

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by custody, custody_schema_replay_identical_schema_does_no_artifact_work, custody_s"
)]
fn sent_ddl_since(hub: &Hub, start: usize) -> Vec<contextdb_engine::sync_types::DdlChange> {
    use contextdb_server::protocol::{MessageType, PushRequest, decode};
    let mut ddl = Vec::new();
    for exchange in hub.broker.recorded_exchanges().into_iter().skip(start) {
        let envelope = decode(&exchange.request_bytes).unwrap();
        if matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) {
            let request: PushRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
            ddl.extend(
                request
                    .changeset
                    .ddl
                    .into_iter()
                    .map(contextdb_engine::sync_types::DdlChange::from),
            );
        }
    }
    ddl
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by custody, custody_schema_replay_is_proportional_to_affected_tables, custody_schem"
)]
fn sent_push_request_count_since(hub: &Hub, start: usize) -> u64 {
    use contextdb_server::protocol::{MessageType, decode};
    hub.broker
        .recorded_exchanges()
        .into_iter()
        .skip(start)
        .filter(|exchange| {
            matches!(
                decode(&exchange.request_bytes).unwrap().message_type,
                MessageType::PushRequest | MessageType::DependencyCompletePushRequest
            )
        })
        .count() as u64
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by custody, custody_schema_replay_is_proportional_to_affected_tables, custody_schem"
)]
fn assert_table_work_bound(observation: &ReceivedSchemaApplyObservationForTest, entries: usize) {
    // Retain the exact frozen <=2*entries ceiling. The independent redb
    // directory/delete stream must obey it too, not only engine loop counters.
    assert!(
        observation.staged_table_images_copied + observation.durable_table_rewrites <= 2 * entries,
        "{observation:?}"
    );
    let actual_relational_rewrites: u64 = observation
        .artifact_writes
        .iter()
        .filter(|(event, _)| {
            (event.operation == TableWriteOperationForTest::DirectoryInsert
                && event.table == "__redb_user_table_directory"
                && event.key.starts_with(b"rel_"))
                || (event.operation == TableWriteOperationForTest::DeleteTable
                    && event.table.starts_with("rel_"))
        })
        .map(|(_, count)| count)
        .sum();
    assert!(
        observation.staged_table_images_copied as u64 + actual_relational_rewrites
            <= 2 * entries as u64,
        "actual table rewrite count {actual_relational_rewrites}"
    );
    assert!(
        actual_relational_rewrites > 0,
        "new empty relations still create actual catalog entries"
    );
}

