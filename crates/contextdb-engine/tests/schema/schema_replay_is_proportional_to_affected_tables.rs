#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
//! Every schema replay cost is measured at actual storage writes, independent of its planner.

#[path = "../fixtures/schema_wall/mod.rs"]
mod support;
use contextdb_core::{Lsn, Value};
use contextdb_engine::database::{
    ReceivedSchemaApplyObservationForTest, ReceivedSchemaApplyObserverForTest,
};
use contextdb_engine::{Database, MaintenancePolicy};
use redb::{TableDefinition, TableWriteOperationForTest};
use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::sync::{Arc, Mutex};
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
    receiver
        .execute(
            "CREATE TABLE resident_vectors (id INTEGER PRIMARY KEY, embedding VECTOR(768)) SYNC TWO WAY",
            &empty(),
    )
    .expect("create the unrelated resident vector table");
    // The replay criterion concerns schema staging, not background graph construction.
    // One committed transaction preserves the same 10,000 resident rows and vectors while
    // avoiding 10,000 independent durable checkpoints during fixture setup.
    let tx = receiver
        .begin()
        .expect("begin the resident-vector fixture transaction");
    for id in 0..10_000 {
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
    let db = Arc::new(Database::open(path).expect("file-backed resident receiver"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    // This function is copied byte-for-byte from the frozen fixture: exactly
    // 10,000 rows, each with the original 768-dimensional values, in one commit.
    phase("setup-resident-seed-begin");
    seed_unrelated_resident_vectors(&db);
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
        10_000,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_schema_push_commits_contact_on_the_receipt_boundary() {
    let root = tempfile::tempdir().expect("focused contact-fusion fixture");
    let receiver = Arc::new(Database::open(root.path().join("hub.db")).unwrap());
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let hub = Hub::start(receiver.clone(), "schema-contact-fusion").await;
    let edge = Arc::new(Database::open(root.path().join("edge.db")).unwrap());
    edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let client = hub.client(edge.clone());
    hub.bind(&client).await;
    for db in [&receiver, &edge] {
        db.execute(
            "CREATE TABLE contact_fusion (id INTEGER PRIMARY KEY) SYNC OFF",
            &empty(),
        )
        .unwrap();
    }
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    push_phase(&client, "focused-contact-fusion").await;
    drop(scope);
    let observation = trace.storage_observation();
    cost_receipt("focused-contact-fusion", &observation);
    assert_eq!(observation.staged_table_images_copied, 0);
    assert_eq!(observation.durable_table_rewrites, 0);
    assert_eq!(
        observation
            .profile_work
            .get("noop-operational-row-writes")
            .copied(),
        Some(2),
        "one old and one new liveness version share the schema receipt"
    );
    let contacts = receiver
        .execute(
            "SELECT node_id, last_contact_ms FROM work_node_contacts",
            &empty(),
        )
        .unwrap();
    assert_eq!(contacts.rows.len(), 1);
    drop(client);
    hub.stop().await;
}

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
        let expected = edge
            .changes_since(since)
            .expect("read the declared changes")
            .ddl;
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
    let entries = source
        .changes_since(since)
        .expect("read the declared changes")
        .ddl
        .len();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn affected_schema_closure_keeps_foreign_keys_propagation_and_triggers() {
    let root = tempfile::tempdir().unwrap();
    let receiver = receiver(&root.path().join("hub.db"));
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
    let before = resident_rows(&receiver);
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
    assert_eq!(resident_rows(&receiver), before);
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
    drop(client);
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_pull_then_author_rows_then_first_push_succeeds() {
    ordinary_flow("rows").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_pull_then_author_schema_then_first_push_succeeds() {
    ordinary_flow("schema").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_pull_then_author_pending_count_is_readable() {
    ordinary_flow("status").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_first_push_without_received_schema_is_a_positive_control() {
    ordinary_flow("first-push-control").await;
}

async fn ordinary_flow(kind: &str) {
    let root = tempfile::tempdir().unwrap();
    let receiver = Arc::new(Database::open(root.path().join("hub.db")).unwrap());
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let source = Arc::new(Database::open(root.path().join("edge.db")).unwrap());
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let ddl = "CREATE TABLE ordinary_items (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY";
    let control = kind == "first-push-control";
    if control {
        source.execute(ddl, &empty()).unwrap();
    } else {
        receiver.execute(ddl, &empty()).unwrap();
        receiver
            .execute("INSERT INTO ordinary_items VALUES (1, 'pulled')", &empty())
            .unwrap();
    }
    let hub = Hub::start(receiver.clone(), "ordinary-pull-author-push").await;
    let client = hub.client(source.clone());
    hub.bind(&client).await;
    if !control {
        assert_eq!(client.push_watermark(), Lsn(0));
        pull_phase(&client, "ordinary-setup-pull").await;
        assert_eq!(
            source
                .execute("SELECT id, body FROM ordinary_items", &empty())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(1), Value::Text("pulled".into())]]
        );
        assert_eq!(
            client.push_watermark(),
            Lsn(0),
            "a pull is not a credited local push"
        );
    }
    let since = source.current_lsn();
    if kind == "schema" {
        source
            .execute("ALTER TABLE ordinary_items ADD COLUMN note TEXT", &empty())
            .unwrap();
    } else {
        source
            .execute(
                "INSERT INTO ordinary_items VALUES (2, 'authored')",
                &empty(),
            )
            .unwrap();
    }
    phase("ordinary-authored");
    let expected_ddl = source
        .changes_since(since)
        .expect("read the declared changes")
        .ddl;
    if kind == "status" {
        phase("ordinary-pending-count-begin");
        let count = client.pending_push_change_count();
        println!("SCHEMA_SYNC_PENDING {count:?}");
        assert_eq!(
            count.expect(
                "ordinary pending-count inspection must not refuse received DDL provenance"
            ),
            1
        );
        phase("ordinary-pending-count-end");
    }
    let exchanges = hub.broker.recorded_exchanges().len();
    // Always expect the public successful flow, never assert an implementation's refusal.
    push_phase(&client, "ordinary-first-push").await;
    if kind == "schema" {
        assert_eq!(
            sent_ddl_since(&hub, exchanges),
            expected_ddl,
            "only the locally authored migration travels, with its original order"
        );
        assert_eq!(schema(&source), schema(&receiver));
    } else {
        assert_eq!(
            receiver
                .execute("SELECT body FROM ordinary_items WHERE id = 2", &empty())
                .unwrap()
                .rows,
            vec![vec![Value::Text("authored".into())]]
        );
        use contextdb_server::protocol::{MessageType, PushRequest, decode};
        let mut sent_rows = Vec::new();
        for exchange in hub.broker.recorded_exchanges().into_iter().skip(exchanges) {
            let envelope = decode(&exchange.request_bytes).unwrap();
            if envelope.message_type == MessageType::PushRequest {
                let request: PushRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
                sent_rows.extend(request.changeset.rows);
            }
        }
        assert_eq!(
            sent_rows.len(),
            1,
            "received rows never echo as newly authored work"
        );
        assert_eq!(sent_rows[0].values.get("id"), Some(&Value::Int64(2)));
    }
    pull_phase(&client, "ordinary-reconcile-pull").await;
    assert_eq!(
        source
            .execute("SELECT * FROM ordinary_items ORDER BY id", &empty())
            .unwrap()
            .rows,
        receiver
            .execute("SELECT * FROM ordinary_items ORDER BY id", &empty())
            .unwrap()
            .rows
    );
    assert_eq!(client.pending_push_change_count().unwrap(), 0);
    drop(client);
    hub.stop().await;
}

#[test]
fn largest_affected_replay_preserves_existing_rows_vectors_and_trigger() {
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    let root = tempfile::tempdir().unwrap();
    let receiver = receiver(&root.path().join("hub.db"));
    let source = Database::open_memory();
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    declare_one_hundred_tables(&receiver);
    let calls = largest::prepare_existing_rows_and_vectors(&receiver);
    declare_one_hundred_tables(&source);
    let _source_calls = largest::prepare_existing_rows_and_vectors(&source);
    let resident_before = resident_rows(&receiver);
    let before = largest::rows_and_vectors(&receiver);
    let migration = largest::author_migration(&source);
    assert_eq!(
        migration.ddl.len(),
        1,
        "one root alteration must include 99 FK dependents"
    );
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    phase("largest-receiver-apply-begin");
    // Supplemental receiver-unit isolation: public push/pull tests remain mandatory.
    // No outbound provenance path or atomic local-startup prerequisite hides this apply.
    receiver
        .apply_changes(
            migration,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("dependency-complete root alteration applies");
    phase("largest-receiver-apply-end");
    drop(scope);
    let observation = trace.observation().expect("actual receiver stage reached");
    cost_receipt("largest", &observation);
    assert_dependency_complete(&observation, &largest::affected_tables(), 0);
    assert!(
        observation.staged_rows_copied <= 100,
        "unrelated resident rows copied: {observation:?}"
    );
    assert!(
        observation.staged_vectors_copied <= 100,
        "unrelated resident vectors copied: {observation:?}"
    );
    assert!(
        observation.durable_rows_rewritten <= 100,
        "unrelated durable rows rewritten: {observation:?}"
    );
    assert_eq!(observation.staged_graph_edges_copied, 0);
    assert_eq!(
        receiver
            .execute("SELECT revised FROM schema_000", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Null]],
        "the authored root alteration is actually installed"
    );
    assert_eq!(largest::rows_and_vectors(&receiver), before);
    assert_eq!(resident_rows(&receiver), resident_before);
    largest::assert_constraints_and_callback(&receiver, &calls);
}

/// A received including-sync unit stages ONLY its affected tables, even when
/// a transactional callback writes to a table the unit's own declared DDL
/// never names: staging discovers that table lazily (from the `TableNotFound`
/// the detached apply hits) and stages exactly it, never the store's many
/// unrelated tables.
#[test]
fn including_sync_stage_discovers_a_callback_written_table_beyond_its_declared_affected_set() {
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
    };

    let root = tempfile::tempdir().unwrap();
    let receiver = receiver(&root.path().join("hub.db"));
    declare_one_hundred_tables(&receiver);
    let resident_before = resident_rows(&receiver);

    receiver
        .execute(
            "CREATE TABLE lazy_source (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "CREATE TABLE lazy_derived (id INTEGER PRIMARY KEY, source_id INTEGER) SYNC TWO WAY",
            &empty(),
        )
        .unwrap();
    receiver
        .execute(
            "CREATE TRIGGER lazy_source_insert ON lazy_source WHEN INSERT INCLUDING SYNC",
            &empty(),
        )
        .unwrap();
    receiver
        .register_trigger_callback("lazy_source_insert", |handle, context| {
            let id = context.row_values["id"].clone();
            handle.execute_in_tx(
                context.tx,
                "INSERT INTO lazy_derived (id, source_id) VALUES ($id, $source_id)",
                &HashMap::from([
                    ("id".to_string(), id.clone()),
                    ("source_id".to_string(), id),
                ]),
            )?;
            Ok(())
        })
        .unwrap();
    initialize_fixture_callbacks(&receiver);

    // One received unit: a harmless column addition on the trigger's own
    // table (so the unit carries schema and reaches the staged door, exactly
    // as `including_sync_fires_when_received_schema_and_data_commit_together`
    // does), plus one row for that table. `lazy_derived` is not named
    // anywhere in this unit's DDL -- the callback is the only reason it is
    // ever staged.
    let migration = ChangeSet {
        ddl: vec![DdlChange::AlterTable {
            name: "lazy_source".to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                ("note".to_string(), "TEXT".to_string()),
            ],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }],
        ddl_lsn: vec![Lsn(9001)],
        rows: vec![RowChange {
            table: "lazy_source".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(9001),
            created_at: None,
        }],
        ..ChangeSet::default()
    };

    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    phase("lazy-discovery-apply-begin");
    receiver
        .apply_changes(
            migration,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("the received unit installs");
    phase("lazy-discovery-apply-end");
    drop(scope);
    let observation = trace.observation().expect("actual receiver stage reached");
    cost_receipt("lazy-discovery", &observation);

    let affected = BTreeSet::from(["lazy_source".to_string(), "lazy_derived".to_string()]);
    assert_eq!(
        observation.staged_table_names, affected,
        "the callback's write to lazy_derived is discovered and staged; none of the 100 \
         unrelated tables is: {observation:?}"
    );
    // `assert_affected_only` assumes a DDL-only migration with no row/sync-row
    // writes (see its "no graph/vector/sync-row mutations" comment), which
    // does not hold here: this unit also carries the row that fires the
    // trigger. Its own subset check is the one still exactly relevant.
    assert!(
        observation.durable_table_names.is_subset(&affected),
        "no unrelated table is durably rewritten either: {observation:?}"
    );
    assert_eq!(
        receiver
            .execute("SELECT source_id FROM lazy_derived", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)]],
        "the including-sync callback's write actually committed with the received row"
    );
    assert_eq!(resident_rows(&receiver), resident_before);
}

#[test]
fn trigger_fixture_initialization_is_a_positive_control() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    declare_one_hundred_tables(&db);
    let calls = largest::prepare_existing_rows_and_vectors(&db);
    assert_eq!(largest::rows_and_vectors(&db).len(), 100);
    largest::assert_constraints_and_callback(&db, &calls);
}

#[test]
fn affected_set_oracle_rejects_missing_dependents_and_unrelated_tables() {
    let affected = BTreeSet::from(["parent".to_string(), "child".to_string()]);
    let mut observation = ReceivedSchemaApplyObservationForTest::default();
    observation.durable_observer_attached = true;
    observation.staged_table_names = affected.clone();
    assert_dependency_complete(&observation, &affected, 0);
    observation.staged_table_names.remove("child");
    assert!(
        std::panic::catch_unwind(|| assert_dependency_complete(&observation, &affected, 0))
            .is_err()
    );
    observation.staged_table_names = affected.clone();
    observation.staged_table_names.insert("unrelated".into());
    assert!(
        std::panic::catch_unwind(|| assert_dependency_complete(&observation, &affected, 0))
            .is_err()
    );
}

#[test]
fn actual_artifact_observer_records_unknown_tables_bytes_and_scopes() {
    let root = tempfile::tempdir().unwrap();
    let a = redb::Database::create(root.path().join("a.redb")).unwrap();
    let b = redb::Database::create(root.path().join("b.redb")).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let guard = a.observe_table_writes_for_test(Arc::new({
        let events = events.clone();
        move |event| events.lock().unwrap().push(event.clone())
    }));
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new("unanticipated_artifact");
    for db in [&a, &b] {
        let tx = db.begin_write().unwrap();
        tx.open_table(definition)
            .unwrap()
            .insert(b"key".as_slice(), b"payload".as_slice())
            .unwrap();
        tx.commit().unwrap();
    }
    let captured = events.lock().unwrap().clone();
    let inserts = captured
        .iter()
        .filter(|event| {
            event.table == "unanticipated_artifact"
                && event.operation == TableWriteOperationForTest::Insert
        })
        .collect::<Vec<_>>();
    assert_eq!(
        inserts.len(),
        1,
        "a different database instance is never charged"
    );
    assert_eq!(
        serialized_bytes(inserts[0], 1),
        10,
        "actual serialized key and value lengths"
    );
    assert!(captured.iter().any(|event| event.operation
        == TableWriteOperationForTest::DirectoryInsert
        && event.key == b"unanticipated_artifact"));
    let inner_events = Arc::new(Mutex::new(Vec::new()));
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _inner = a.observe_table_writes_for_test(Arc::new({
            let inner_events = inner_events.clone();
            move |event| inner_events.lock().unwrap().push(event.clone())
        }));
        let tx = a.begin_write().unwrap();
        tx.open_table(definition)
            .unwrap()
            .remove(b"key".as_slice())
            .unwrap();
        tx.commit().unwrap();
        panic!("deliberate observer scope unwind after a real committed removal");
    }));
    assert!(unwound.is_err());
    assert_eq!(
        *events.lock().unwrap(),
        captured,
        "nested observer owns its own operations"
    );
    assert!(
        inner_events
            .lock()
            .unwrap()
            .iter()
            .any(
                |event| event.operation == TableWriteOperationForTest::Remove
                    && event.key == b"key"
                    && event.value_bytes == 7
            )
    );
    let tx = a.begin_write().unwrap();
    assert!(tx.delete_table(definition).unwrap());
    tx.commit().unwrap();
    let captured = events.lock().unwrap().clone();
    assert!(
        captured
            .iter()
            .any(|event| event.operation == TableWriteOperationForTest::DeleteTable),
        "unwind restores the outer observer, including actual table deletion"
    );
    assert!(
        captured
            .iter()
            .any(|event| event.operation == TableWriteOperationForTest::DirectoryRemove)
    );
    drop(guard);
    let tx = a.begin_write().unwrap();
    tx.open_table(definition)
        .unwrap()
        .insert(b"next".as_slice(), b"unobserved".as_slice())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        captured,
        "scope exit stops actual observation"
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
