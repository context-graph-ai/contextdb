// Canonical test-only replay helpers; this file declares no tests.

#[path = "../fixtures/schema_wall/mod.rs"]
mod support;
use contextdb_core::Value;
use contextdb_engine::database::{
    ReceivedSchemaApplyObservationForTest, ReceivedSchemaApplyObserverForTest,
};
use contextdb_engine::{Database, MaintenancePolicy};
use redb::TableWriteOperationForTest;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use support::*;

fn phase(name: &str) {
    println!("SCHEMA_REPLAY_PHASE {name}");
    std::io::stdout()
        .flush()
        .expect("flush external measurement marker");
    if name.starts_with("identical-")
        && name.ends_with("-round-begin")
        && std::env::var_os("CONTEXTDB_SCHEMA_TIMING_HANDSHAKE").is_some()
    {
        // Only the external timing runner opts into this handshake. It starts
        // its clock before acknowledging, so pipe buffering cannot undercount.
        let mut acknowledgement = String::new();
        std::io::stdin().read_line(&mut acknowledgement).unwrap();
        assert_eq!(acknowledgement, format!("START {name}\n"));
    }
}

// Every public sync operation has a flushed boundary and its original typed error.
// Setup timings diagnose setup only; none can stand in for a replay-round receipt.
async fn pull_phase(client: &contextdb_server::SyncClient, label: &str) {
    phase(&format!("{label}-begin"));
    let pages_before = client.pull_pages_read();
    let started = Instant::now();
    let result = client.pull_default().await;
    println!(
        "SCHEMA_SYNC_RESULT {label} elapsed_ns={} result={result:?}",
        started.elapsed().as_nanos()
    );
    result.unwrap_or_else(|error| {
        panic!("SCHEMA_FIRST_FAILURE phase={label} operation=pull error={error:?}")
    });
    println!(
        "SCHEMA_PULL_PAGES {label} before={pages_before} after={}",
        client.pull_pages_read()
    );
    phase(&format!("{label}-end"));
}

async fn push_phase(client: &contextdb_server::SyncClient, label: &str) {
    phase(&format!("{label}-begin"));
    let started = Instant::now();
    let result = client.push().await;
    println!(
        "SCHEMA_SYNC_RESULT {label} elapsed_ns={} result={result:?}",
        started.elapsed().as_nanos()
    );
    // A provenance refusal is an engine defect in this ordinary flow, never expected success.
    result.unwrap_or_else(|error| {
        panic!("SCHEMA_FIRST_FAILURE phase={label} operation=push error={error:?}")
    });
    phase(&format!("{label}-end"));
}

fn declare_one_hundred_tables(edge: &Database) {
    for ordinal in 0usize..100 {
        let name = format!("schema_{ordinal:03}");
        let parent = ordinal
            .checked_sub(1)
            .map(|previous| format!("schema_{previous:03}"));
        let ddl = match parent {
            Some(parent) => format!(
                "CREATE TABLE {name} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES {parent}(id), body TEXT, FOREIGN KEY (parent_id) REFERENCES {parent}(id)) SYNC OFF"
            ),
            None => format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF"),
        };
        edge.execute(&ddl, &empty())
            .expect("author one generated source schema declaration per commit");
    }
    edge.execute(
        "CREATE EVENT TYPE schema_099_insert WHEN INSERT ON schema_099",
        &empty(),
    )
    .expect("author the final event type");
    edge.execute(
        "CREATE TRIGGER schema_099_insert_trigger ON schema_099 WHEN INSERT INCLUDING SYNC",
        &empty(),
    )
    .expect("author the final including-sync trigger");
}

fn seed_unrelated_resident_vectors(receiver: &Database) {
    seed_unrelated_resident_vectors_with_count(receiver, 10_000);
}

fn seed_unrelated_resident_vectors_with_count(receiver: &Database, row_count: i64) {
    receiver
        .execute(
            "CREATE TABLE resident_vectors (id INTEGER PRIMARY KEY, embedding VECTOR(768)) SYNC TWO WAY",
            &empty(),
    )
    .expect("create the unrelated resident vector table");
    // The replay criterion concerns schema staging, not background graph construction.
    // The full fixture still supplies exactly 10,000 rows; bounded twins supply one.
    // Keep the original dimensions, values and single committed transaction.
    let tx = receiver
        .begin()
        .expect("begin the resident-vector fixture transaction");
    for id in (10_000 - row_count)..10_000 {
        let mut values = vec![0.0; 768];
        values[0] = id as f32;
        values[1] = id as f32 * 0.25;
        let vector = Value::Vector(values);
        receiver
            .execute_in_tx(
                tx,
                "INSERT INTO resident_vectors VALUES ($id, $embedding)",
                &HashMap::from([
                    ("id".to_string(), Value::Int64(id)),
                    ("embedding".to_string(), vector.clone()),
                ]),
            )
            .expect("seed one unrelated resident vector row");
    }
    receiver
        .commit(tx)
        .expect("commit the resident-vector fixture transaction");
}

fn receiver(path: &std::path::Path) -> Arc<Database> {
    receiver_with_unrelated_rows(path, crate::UNRELATED_RESIDENT_ROWS)
}

fn receiver_with_unrelated_rows(path: &std::path::Path, row_count: i64) -> Arc<Database> {
    let db = Arc::new(Database::open(path).expect("file-backed resident receiver"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    // Only the unrelated row count varies; full targets retain the frozen 10,000.
    phase("setup-resident-seed-begin");
    if row_count == 10_000 {
        seed_unrelated_resident_vectors(&db);
    } else {
        seed_unrelated_resident_vectors_with_count(&db, row_count);
    }
    phase("setup-resident-seed-end");
    db
}

fn resident_rows(db: &Database) -> Vec<Vec<Value>> {
    let rows = db
        .execute(
            "SELECT id, embedding FROM resident_vectors ORDER BY id",
            &empty(),
        )
        .expect("resident data remains readable")
        .rows;
    assert_eq!(
        rows.len(),
        usize::try_from(crate::UNRELATED_RESIDENT_ROWS).unwrap(),
        "never shrink the frozen resident dataset"
    );
    rows
}

fn cost_receipt(label: &str, observation: &ReceivedSchemaApplyObservationForTest) {
    let mut totals = std::collections::BTreeMap::<String, (u64, u64)>::new();
    for (event, count) in &observation.artifact_writes {
        let total = totals
            .entry(format!("{}:{:?}", event.table, event.operation))
            .or_default();
        total.0 += count;
        total.1 += serialized_bytes(event, *count);
    }
    println!("SCHEMA_REPLAY_ACTUAL_ARTIFACTS {label} {totals:?}");
    println!(
        "SCHEMA_REPLAY_PROFILE {label} nanos={:?} work={:?}",
        observation.profile_nanos, observation.profile_work
    );
}

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

// The full targets retain the frozen row-count assertion through their root constant.
fn replay_resident_rows(db: &Database, row_count: i64) -> Vec<Vec<Value>> {
    if row_count == 10_000 {
        return resident_rows(db);
    }
    let rows = db
        .execute(
            "SELECT id, embedding FROM resident_vectors ORDER BY id",
            &empty(),
        )
        .expect("bounded resident data remains readable")
        .rows;
    assert_eq!(rows.len(), usize::try_from(row_count).unwrap());
    if row_count == 1 {
        assert_eq!(
            rows[0][0],
            Value::Int64(9999),
            "nonzero bounded row witness"
        );
        let Value::Vector(values) = &rows[0][1] else {
            panic!("bounded witness must retain its vector");
        };
        assert!(
            values.iter().any(|component| *component != 0.0),
            "bounded vector witness must contain a nonzero component"
        );
    }
    rows
}

// Shared real authenticated replay and behavioral oracles; only unrelated size varies.
async fn affected_schema_closure_contract(row_count: i64) {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    let root = tempfile::tempdir().unwrap();
    let receiver = receiver_with_unrelated_rows(&root.path().join("hub.db"), row_count);
    // Valid production declarations: the child's FK and propagation are both
    // dependent on the parent; its INCLUDING SYNC trigger must survive replay.
    let declarations = [
        "CREATE TABLE closure_parent (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [archived]) SYNC TWO WAY",
        "CREATE TABLE closure_child (id UUID PRIMARY KEY, parent_id UUID REFERENCES closure_parent(id) ON STATE archived PROPAGATE SET invalidated, status TEXT) STATE MACHINE (status: active -> [invalidated]) SYNC TWO WAY",
        "CREATE TRIGGER closure_child_insert ON closure_child WHEN INSERT INCLUDING SYNC",
    ];
    declare(&receiver, &declarations);
    let fired_rows = Arc::new(Mutex::new(Vec::new()));
    receiver
        .register_trigger_callback("closure_child_insert", {
            let fired_rows = fired_rows.clone();
            move |_db, context| {
                fired_rows
                    .lock()
                    .unwrap()
                    .push(context.row_values["id"].clone());
                Ok(())
            }
        })
        .unwrap();
    initialize_fixture_callbacks(&receiver);
    let hub = Hub::start(receiver.clone(), "schema-fk-trigger-closure").await;
    let source = Arc::new(Database::open(root.path().join("source.db")).unwrap());
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let client = hub.client(source.clone());
    hub.bind(&client).await;
    // cg declares and registers callbacks before its first pull. Preserve that
    // lifecycle so incoming writes can execute INCLUDING SYNC callbacks.
    declare(&source, &declarations);
    initialize_fixture_callbacks(&source);
    pull_phase(&client, "setup-closure-source-pull").await;
    let before = replay_resident_rows(&receiver, row_count);
    source
        .execute("ALTER TABLE closure_parent ADD COLUMN note TEXT", &empty())
        .expect("author affected parent alteration");
    let observer = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&observer);
    push_phase(&client, "closure-replay-push").await;
    drop(scope);
    let observed = observer
        .observation()
        .expect("real dependency-closure apply");
    let affected = BTreeSet::from(["closure_parent".to_string(), "closure_child".to_string()]);
    cost_receipt("dependency-closure", &observed);
    assert_dependency_complete(&observed, &affected, 1);
    assert!(
        receiver
            .list_triggers()
            .iter()
            .any(|v| v.name == "closure_child_insert" && v.including_sync)
    );
    assert_eq!(replay_resident_rows(&receiver, row_count), before);
    pull_phase(&client, "closure-reconcile-pull").await;
    assert_eq!(schema(&source), schema(&receiver));
    // A metadata-only equality check cannot certify FK/propagation behavior.
    receiver.execute("INSERT INTO closure_parent VALUES ('00000000-0000-0000-0000-000000000001', 'active', NULL)", &empty()).unwrap();
    receiver.execute("INSERT INTO closure_child VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'active')", &empty()).unwrap();
    assert_eq!(
        *fired_rows.lock().unwrap(),
        vec![Value::Uuid(uuid::Uuid::from_u128(2))],
        "the replayed trigger fires for a valid local insert"
    );
    assert!(receiver.execute("INSERT INTO closure_child VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000099', 'active')", &empty()).is_err(), "replayed FK still refuses missing parent");
    fired_rows.lock().unwrap().clear();
    receiver.execute("UPDATE closure_parent SET status = 'archived' WHERE id = '00000000-0000-0000-0000-000000000001'", &empty()).unwrap();
    assert_eq!(
        receiver
            .execute("SELECT status FROM closure_child", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Text("invalidated".into())]]
    );
    source.execute("INSERT INTO closure_parent (id, status) VALUES ('00000000-0000-0000-0000-000000000010', 'active')", &empty()).unwrap();
    source.execute("INSERT INTO closure_child VALUES ('00000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000010', 'active')", &empty()).unwrap();
    push_phase(&client, "closure-including-sync-row-push").await;
    assert_eq!(
        *fired_rows.lock().unwrap(),
        vec![Value::Uuid(uuid::Uuid::from_u128(0x11))],
        "affected schema publication preserves the registered local and INCLUDING SYNC callback"
    );
    if row_count == 1 {
        assert_eq!(observed.staged_rows_copied, 0);
        assert_eq!(observed.staged_vectors_copied, 0);
        assert_eq!(
            rmp_serde::to_vec(&replay_resident_rows(&receiver, row_count)).unwrap(),
            rmp_serde::to_vec(&before).unwrap(),
            "unrelated row and all vector component bytes survive schema and row replay"
        );
    }
    drop(client);
    hub.stop().await;
}
