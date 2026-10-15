#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
//! Every schema replay cost is measured at actual storage writes, independent of its planner.

include!("schema_replay_support.rs");
use contextdb_core::Lsn;
use redb::TableDefinition;
use std::collections::BTreeSet;
use std::sync::Mutex;

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
async fn affected_schema_closure_keeps_foreign_keys_propagation_and_triggers() {
    affected_schema_closure_contract(crate::UNRELATED_RESIDENT_ROWS).await;
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
    let expected_ddl = source.changes_since(since).ddl;
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
