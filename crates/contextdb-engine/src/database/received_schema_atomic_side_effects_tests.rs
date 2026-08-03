use super::*;

type ReceivedSchemaFixture = (
    ChangeSet,
    crate::protocol::ReceivedDdlContext,
    Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
);

fn migration() -> ReceivedSchemaFixture {
    let ddl = vec![
        DdlChange::CreateTable {
            name: "notes".to_string(),
            columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
        DdlChange::CreateEventType {
            name: "note_inserted".to_string(),
            trigger: "INSERT".to_string(),
            table: "notes".to_string(),
        },
        DdlChange::CreateSink {
            name: "archive".to_string(),
            sink_type: "CALLBACK".to_string(),
            url: None,
        },
        DdlChange::CreateRoute {
            name: "archive_notes".to_string(),
            event_type: "note_inserted".to_string(),
            sink: "archive".to_string(),
            table: "notes".to_string(),
            where_in: None,
        },
        DdlChange::CreateTrigger {
            name: "note_audit".to_string(),
            table: "notes".to_string(),
            on_events: vec!["INSERT".to_string()],
        },
    ];
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "laptop".to_string(),
        source_incarnation: Incarnation(4),
        entries: ddl
            .iter()
            .enumerate()
            .map(|(ordinal, change)| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: ordinal as u32,
                table: Database::ddl_affected_table(change).map(str::to_string),
                table_generation: Some(3),
                digest: vec![ordinal as u8],
            })
            .collect(),
    };
    let lineages = vec![(
        "notes".to_string(),
        key.clone(),
        Lsn(42),
        crate::protocol::WireRowLineage {
            author_node_id: "laptop".to_string(),
            author_database_incarnation: Incarnation(4),
            author_local_mutation_position: Lsn(42),
            table_generation: 3,
            lineage_root: format!("author:laptop:{}:42", Incarnation(4).to_hex()),
            attestation: vec![],
        },
    )];
    (
        ChangeSet {
            ddl,
            ddl_lsn: vec![Lsn(41); 5],
            rows: vec![RowChange {
                table: "notes".to_string(),
                natural_key: key,
                values: HashMap::from([("id".to_string(), Value::Int64(1))]),
                deleted: false,
                lsn: Lsn(42),
                created_at: None,
            }],
            ..ChangeSet::default()
        },
        received,
        lineages,
    )
}

fn migration_routed_to_retained_archive() -> ReceivedSchemaFixture {
    let (mut changes, mut received, lineages) = migration();
    for change in &mut changes.ddl {
        match change {
            DdlChange::CreateSink { name, .. } if name == "archive" => {
                *name = "retained_archive".to_string();
            }
            DdlChange::CreateRoute { sink, .. } => {
                *sink = "retained_archive".to_string();
            }
            _ => {}
        }
    }
    let sink_entry = received
        .entries
        .get_mut(2)
        .expect("received sink provenance must match the source DDL vector");
    sink_entry.table = None;
    (changes, received, lineages)
}

fn acl_ids() -> (uuid::Uuid, uuid::Uuid, uuid::Uuid, uuid::Uuid, uuid::Uuid) {
    (
        uuid::Uuid::from_u128(0xA),
        uuid::Uuid::from_u128(0xB),
        uuid::Uuid::from_u128(0xCA),
        uuid::Uuid::from_u128(0x1),
        uuid::Uuid::from_u128(0x2),
    )
}

fn acl_received_migration() -> ReceivedSchemaFixture {
    let (allowed_acl, denied_acl, context, allowed_id, denied_id) = acl_ids();
    let ddl = vec![
        DdlChange::CreateTable {
            name: "acl_grants".to_string(),
            columns: vec![
                ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                ("principal_kind".to_string(), "TEXT".to_string()),
                ("principal_id".to_string(), "TEXT".to_string()),
                ("acl_id".to_string(), "UUID UNIQUE".to_string()),
            ],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
        DdlChange::CreateTable {
            name: "secrets".to_string(),
            columns: vec![
                ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                ("body".to_string(), "TEXT".to_string()),
                ("context_id".to_string(), "UUID CONTEXT_ID".to_string()),
                (
                    "acl_id".to_string(),
                    "UUID ACL REFERENCES acl_grants(acl_id)".to_string(),
                ),
            ],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
        DdlChange::CreateEventType {
            name: "secret_inserted".to_string(),
            trigger: "INSERT".to_string(),
            table: "secrets".to_string(),
        },
        DdlChange::CreateSink {
            name: "acl_archive".to_string(),
            sink_type: "CALLBACK".to_string(),
            url: None,
        },
        DdlChange::CreateRoute {
            name: "archive_secrets".to_string(),
            event_type: "secret_inserted".to_string(),
            sink: "acl_archive".to_string(),
            table: "secrets".to_string(),
            where_in: None,
        },
    ];
    let rows = vec![
        RowChange {
            table: "acl_grants".to_string(),
            natural_key: NaturalKey::single(
                "id".to_string(),
                Value::Uuid(uuid::Uuid::from_u128(0x100)),
            ),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::from_u128(0x100))),
                (
                    "principal_kind".to_string(),
                    Value::Text("Agent".to_string()),
                ),
                ("principal_id".to_string(), Value::Text("alice".to_string())),
                ("acl_id".to_string(), Value::Uuid(allowed_acl)),
            ]),
            deleted: false,
            lsn: Lsn(52),
            created_at: None,
        },
        RowChange {
            table: "acl_grants".to_string(),
            natural_key: NaturalKey::single(
                "id".to_string(),
                Value::Uuid(uuid::Uuid::from_u128(0x101)),
            ),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::from_u128(0x101))),
                (
                    "principal_kind".to_string(),
                    Value::Text("Agent".to_string()),
                ),
                ("principal_id".to_string(), Value::Text("bob".to_string())),
                ("acl_id".to_string(), Value::Uuid(denied_acl)),
            ]),
            deleted: false,
            lsn: Lsn(53),
            created_at: None,
        },
        RowChange {
            table: "secrets".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Uuid(allowed_id)),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(allowed_id)),
                ("body".to_string(), Value::Text("allowed".to_string())),
                ("context_id".to_string(), Value::Uuid(context)),
                ("acl_id".to_string(), Value::Uuid(allowed_acl)),
            ]),
            deleted: false,
            lsn: Lsn(54),
            created_at: None,
        },
        RowChange {
            table: "secrets".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Uuid(denied_id)),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(denied_id)),
                ("body".to_string(), Value::Text("denied".to_string())),
                ("context_id".to_string(), Value::Uuid(context)),
                ("acl_id".to_string(), Value::Uuid(denied_acl)),
            ]),
            deleted: false,
            lsn: Lsn(55),
            created_at: None,
        },
    ];
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-acl"),
        source_node_id: "acl-source".to_string(),
        source_incarnation: Incarnation(9),
        entries: ddl
            .iter()
            .enumerate()
            .map(|(ordinal, change)| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(51),
                ordinal: ordinal as u32,
                table: Database::ddl_affected_table(change).map(str::to_string),
                table_generation: Some(7),
                digest: vec![ordinal as u8],
            })
            .collect(),
    };
    let lineages = rows
        .iter()
        .map(|row| {
            (
                row.table.clone(),
                row.natural_key.clone(),
                row.lsn,
                crate::protocol::WireRowLineage {
                    author_node_id: "acl-source".to_string(),
                    author_database_incarnation: Incarnation(9),
                    author_local_mutation_position: row.lsn,
                    table_generation: 7,
                    lineage_root: format!(
                        "author:acl-source:{}:{}",
                        Incarnation(9).to_hex(),
                        row.lsn.0
                    ),
                    attestation: vec![],
                },
            )
        })
        .collect();
    (
        ChangeSet {
            ddl,
            ddl_lsn: vec![Lsn(51); 5],
            rows,
            ..ChangeSet::default()
        },
        received,
        lineages,
    )
}

fn acl_received_migration_with_incomplete_acl_refs() -> ReceivedSchemaFixture {
    let (mut changes, received, mut lineages) = acl_received_migration();
    let (_, _, context, _, _) = acl_ids();
    let omitted_id = uuid::Uuid::from_u128(0x3);
    let null_id = uuid::Uuid::from_u128(0x4);
    for (id, body, lsn, acl_id) in [
        (omitted_id, "omitted-acl-ref", Lsn(56), None),
        (null_id, "null-acl-ref", Lsn(57), Some(Value::Null)),
    ] {
        let mut values = HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
            ("context_id".to_string(), Value::Uuid(context)),
        ]);
        if let Some(acl_id) = acl_id {
            values.insert("acl_id".to_string(), acl_id);
        }
        let natural_key = NaturalKey::single("id".to_string(), Value::Uuid(id));
        changes.rows.push(RowChange {
            table: "secrets".to_string(),
            natural_key: natural_key.clone(),
            values,
            deleted: false,
            lsn,
            created_at: None,
        });
        lineages.push((
            "secrets".to_string(),
            natural_key,
            lsn,
            crate::protocol::WireRowLineage {
                author_node_id: "acl-source".to_string(),
                author_database_incarnation: Incarnation(9),
                author_local_mutation_position: lsn,
                table_generation: 7,
                lineage_root: format!("author:acl-source:{}:{}", Incarnation(9).to_hex(), lsn.0),
                attestation: vec![],
            },
        ));
    }
    (changes, received, lineages)
}

fn seed_local_acl_route(db: &Database) {
    let (allowed_acl, denied_acl, context, allowed_id, denied_id) = acl_ids();
    for statement in [
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID UNIQUE)",
        "CREATE TABLE secrets (id UUID PRIMARY KEY, body TEXT, context_id UUID CONTEXT_ID, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
        "CREATE EVENT TYPE secret_inserted WHEN INSERT ON secrets",
        "CREATE SINK acl_archive TYPE callback",
        "CREATE ROUTE archive_secrets EVENT secret_inserted TO acl_archive",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    for (id, principal_kind, principal_id, acl_id) in [
        (uuid::Uuid::from_u128(0x100), "Agent", "alice", allowed_acl),
        (uuid::Uuid::from_u128(0x101), "Agent", "bob", denied_acl),
    ] {
        db.execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, $kind, $principal, $acl)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("kind".to_string(), Value::Text(principal_kind.to_string())),
                ("principal".to_string(), Value::Text(principal_id.to_string())),
                ("acl".to_string(), Value::Uuid(acl_id)),
            ]),
        )
        .unwrap();
    }
    for (id, body, acl_id) in [
        (allowed_id, "allowed", allowed_acl),
        (denied_id, "denied", denied_acl),
    ] {
        db.execute(
            "INSERT INTO secrets (id, body, context_id, acl_id) VALUES ($id, $body, $context, $acl)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("body".to_string(), Value::Text(body.to_string())),
                ("context".to_string(), Value::Uuid(context)),
                ("acl".to_string(), Value::Uuid(acl_id)),
            ]),
        )
        .unwrap();
    }
}

fn wait_for_sink_state(label: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {label}"
        );
        std::thread::yield_now();
    }
}

fn assert_acl_route_delivery(db: &Database, phase: &str) {
    use contextdb_core::types::{ContextId, Principal};
    let (_, denied_acl, context, _, _) = acl_ids();
    let scoped =
        db.scoped_with_contexts(std::collections::BTreeSet::from([ContextId::new(context)]));
    let (alice_sent, alice_received) = std::sync::mpsc::sync_channel(2);
    scoped
        .register_sink(
            "acl_archive",
            Some(Principal::Agent("alice".to_string())),
            move |event| {
                alice_sent.send(event.clone()).unwrap();
                Ok(())
            },
        )
        .unwrap();
    let event = alice_received
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the ACL-granted event must reach alice's scoped sink");
    assert_eq!(event.row_values["body"], Value::Text("allowed".to_string()));
    wait_for_sink_state("ACL denied event full sweep", || {
        db.sink_metrics_for_test("acl_archive").examined >= 2
    });
    let metrics = db.sink_metrics_for_test("acl_archive");
    assert_eq!(metrics.delivered, 1, "{phase}");
    assert_eq!(metrics.queued, 1, "{phase}");
    assert!(matches!(
        alice_received.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    let queue = db
        .persistence
        .as_ref()
        .unwrap()
        .load_sink_queue::<event_bus::SinkQueueEntry>("acl_archive")
        .unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].event.row_values["acl_id"], Value::Uuid(denied_acl));
    assert_eq!(
        queue[0].event.row_values["body"],
        Value::Text("denied".to_string())
    );
    let (bob_sent, bob_received) = std::sync::mpsc::sync_channel(2);
    scoped
        .register_sink(
            "acl_archive",
            Some(Principal::Agent("bob".to_string())),
            move |event| {
                bob_sent.send(event.clone()).unwrap();
                Ok(())
            },
        )
        .unwrap();
    let event = bob_received
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the denied event must drain when its granted principal registers");
    assert_eq!(event.row_values["body"], Value::Text("denied".to_string()));
    wait_for_sink_state("Bob acknowledgement of ACL-denied queue entry", || {
        let metrics = db.sink_metrics_for_test("acl_archive");
        metrics.delivered == 2 && metrics.queued == 0
    });
    assert!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_sink_queue::<event_bus::SinkQueueEntry>("acl_archive")
            .unwrap()
            .is_empty()
    );
}

fn assert_received_incomplete_acl_refs_are_denied(db: &Database, phase: &str) {
    use contextdb_core::types::{ContextId, Principal};

    let (_, _, context, _, _) = acl_ids();
    let scoped =
        db.scoped_with_contexts(std::collections::BTreeSet::from([ContextId::new(context)]));
    let (alice_sent, alice_received) = std::sync::mpsc::sync_channel(4);
    scoped
        .register_sink(
            "acl_archive",
            Some(Principal::Agent("alice".to_string())),
            move |event| {
                alice_sent.send(event.clone()).unwrap();
                Ok(())
            },
        )
        .unwrap();

    let granted = alice_received
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the ACL-granted received row must reach alice's scoped sink");
    assert_eq!(
        granted.row_values["body"],
        Value::Text("allowed".to_string()),
        "{phase}: the positive control must deliver before denial is evaluated"
    );
    wait_for_sink_state("received ACL-ref route sweep", || {
        db.sink_metrics_for_test("acl_archive").examined >= 4
    });
    let unexpected = alice_received
        .try_iter()
        .map(|event| event.row_values["body"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        unexpected,
        Vec::<Value>::new(),
        "{phase}: a missing or NULL ACL_REF payload must fail closed for alice"
    );
    let queued = durable_queue_identity(db, "acl_archive");
    assert_eq!(queued.len(), 3, "{phase}");
    assert!(
        queued
            .iter()
            .any(|(_, event)| { event.row_values["body"] == Value::Text("denied".to_string()) })
    );
    assert!(queued.iter().any(|(_, event)| {
        event.row_values["body"] == Value::Text("omitted-acl-ref".to_string())
    }));
    assert!(
        queued.iter().any(|(_, event)| {
            event.row_values["body"] == Value::Text("null-acl-ref".to_string())
        })
    );
}

fn seed_preexisting_side_projection(db: &Database) {
    for statement in [
        "CREATE TABLE retained_notes (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE retained_note_inserted WHEN INSERT ON retained_notes",
        "CREATE SINK retained_archive TYPE callback",
        "CREATE ROUTE retained_archive_notes EVENT retained_note_inserted TO retained_archive",
        "CREATE TRIGGER retained_note_audit ON retained_notes WHEN INSERT",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    db.register_trigger_callback("retained_note_audit", |_db, _ctx| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
    db.execute(
        "INSERT INTO retained_notes (id) VALUES (99)",
        &HashMap::new(),
    )
    .unwrap();
}

fn assert_durable_side_projection(
    db: &Database,
    table: &str,
    event_type: &str,
    sink: &str,
    trigger: &str,
    expected_ids: &[i64],
) {
    let mut rows = db.relational_store.tables.read()[table].clone();
    rows.sort_by_key(|row| match row.values["id"] {
        Value::Int64(id) => id,
        ref value => panic!("{table} id must be Int64, got {value:?}"),
    });
    assert_eq!(
        rows.iter()
            .map(|row| match row.values["id"] {
                Value::Int64(id) => id,
                ref value => panic!("{table} id must be Int64, got {value:?}"),
            })
            .collect::<Vec<_>>(),
        expected_ids
    );
    let queue = db
        .persistence
        .as_ref()
        .unwrap()
        .load_sink_queue::<event_bus::SinkQueueEntry>(sink)
        .unwrap();
    assert_eq!(queue.len(), expected_ids.len());
    for row in &rows {
        let queued = queue
            .iter()
            .find(|queued| queued.row_id == row.row_id)
            .expect("each table row must have exactly one durable sink event");
        assert_eq!(queued.event.event_type, event_type);
        assert_eq!(queued.event.table, table);
        assert_eq!(queued.event.row_values, row.values);
        assert_eq!(queued.row_id, row.row_id);
    }
    assert_eq!(
        db.sink_metrics_for_test(sink).queued,
        expected_ids.len() as u64
    );
    assert!(
        db.list_triggers()
            .iter()
            .any(|declaration| declaration.name == trigger && declaration.table == table)
    );
    let audits = db
        .persistence
        .as_ref()
        .unwrap()
        .load_trigger_audit_history()
        .unwrap()
        .into_iter()
        .filter(|audit| audit.trigger_name == trigger)
        .collect::<Vec<_>>();
    assert_eq!(audits.len(), expected_ids.len());
    for row in &rows {
        assert!(audits.iter().any(|audit| {
            audit.status == TriggerAuditStatus::Fired
                && audit.firing_tx == row.created_tx
                && audit.firing_lsn == row.lsn
        }));
    }
    let ring = db
        .trigger_audit_log()
        .into_iter()
        .filter(|audit| audit.trigger_name == trigger)
        .collect::<Vec<_>>();
    assert_eq!(ring.len(), expected_ids.len());
    for row in &rows {
        assert!(ring.iter().any(|audit| {
            audit.status == TriggerAuditStatus::Fired
                && audit.firing_tx == row.created_tx
                && audit.firing_lsn == row.lsn
        }));
    }
}

fn assert_received_ddl_is_appended(db: &Database, changes: &ChangeSet, receiver_lsn: Lsn) {
    let actual = db.changes_since(Lsn(0));
    assert!(actual.ddl.len() >= changes.ddl.len());
    assert_eq!(
        &actual.ddl[actual.ddl.len() - changes.ddl.len()..],
        changes.ddl
    );
    assert_eq!(
        &actual.ddl_lsn[actual.ddl_lsn.len() - changes.ddl.len()..],
        vec![receiver_lsn; changes.ddl.len()]
    );
    assert_eq!(
        db.relational_store.tables.read()["notes"][0].lsn,
        receiver_lsn
    );
}

fn config_snapshot(db: &Database) -> Vec<(String, Vec<u8>)> {
    let mut config = db.persistence.as_ref().unwrap().dump_config_raw().unwrap();
    config.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    config
}

fn assert_exact_change_log(actual: &ChangeSet, expected: &ChangeSet) {
    assert_eq!(actual.rows, expected.rows);
    assert_eq!(actual.edges, expected.edges);
    assert_eq!(actual.vectors, expected.vectors);
    assert_eq!(actual.ddl, expected.ddl);
    assert_eq!(actual.ddl_lsn, expected.ddl_lsn);
}

fn assert_exact_trigger_audit_count(db: &Database, expected: usize) {
    let durable = db
        .persistence
        .as_ref()
        .unwrap()
        .load_trigger_audit_history()
        .unwrap();
    let ring = db.trigger_audit_log();
    assert_eq!(ring, durable);
    assert_eq!(durable.len(), expected);
    assert_eq!(ring.len(), expected);
}

fn assert_no_received_side_projection(db: &Database) {
    assert!(db.table_meta("notes").is_none());
    assert_eq!(db.sink_metrics_for_test("archive").queued, 0);
    assert!(
        !db.list_triggers()
            .iter()
            .any(|declaration| declaration.name == "note_audit")
    );
    assert!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_sink_queue::<event_bus::SinkQueueEntry>("archive")
            .unwrap()
            .is_empty()
    );
    assert!(
        !db.persistence
            .as_ref()
            .unwrap()
            .load_trigger_audit_history()
            .unwrap()
            .iter()
            .any(|audit| audit.trigger_name == "note_audit")
    );
    assert!(
        !db.trigger_audit_log()
            .iter()
            .any(|audit| audit.trigger_name == "note_audit")
    );
}

fn assert_no_acl_received_artifacts(db: &Database) {
    assert!(db.table_meta("acl_grants").is_none());
    assert!(db.table_meta("secrets").is_none());
    assert!(
        db.register_sink("acl_archive", None, |_| Ok(())).is_err(),
        "a faulted ACL received stage must not publish a sink definition"
    );
    assert!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_sink_queue::<event_bus::SinkQueueEntry>("acl_archive")
            .unwrap()
            .is_empty(),
        "a faulted ACL received stage must not leak route-time grant snapshots into a queue"
    );
}

fn seed_bystander_queue(db: &Database) {
    for statement in [
        "CREATE TABLE bystander_rows (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE bystander_inserted WHEN INSERT ON bystander_rows",
        "CREATE SINK bystander_archive TYPE callback",
        "CREATE ROUTE archive_bystander EVENT bystander_inserted TO bystander_archive",
        "INSERT INTO bystander_rows (id) VALUES (7)",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
}

fn durable_queue_identity(db: &Database, sink: &str) -> Vec<(u64, SinkEvent)> {
    db.persistence
        .as_ref()
        .unwrap()
        .load_sink_queue::<event_bus::SinkQueueEntry>(sink)
        .unwrap()
        .into_iter()
        .map(|entry| (entry.id, entry.event))
        .collect()
}

#[test]
fn received_schema_publish_does_not_resurrect_a_worker_acknowledged_queue_entry() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("received-schema-queue-ack-race.redb");
    let db = std::sync::Arc::new(Database::open(&path).unwrap());
    seed_preexisting_side_projection(&db);
    seed_bystander_queue(&db);
    let old = durable_queue_identity(&db, "retained_archive");
    let bystander = durable_queue_identity(&db, "bystander_archive");
    assert_eq!(old.len(), 1);
    assert_eq!(bystander.len(), 1);
    let old_id = old[0].0;

    let (first_entered_sender, first_entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (first_release_sender, first_release_receiver) = std::sync::mpsc::sync_channel(1);
    let (second_entered_sender, second_entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (second_release_sender, second_release_receiver) = std::sync::mpsc::sync_channel(1);
    let first_release_receiver = std::sync::Arc::new(std::sync::Mutex::new(first_release_receiver));
    let second_release_receiver =
        std::sync::Arc::new(std::sync::Mutex::new(second_release_receiver));
    let callback_invocations = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_first_release_receiver = std::sync::Arc::clone(&first_release_receiver);
    let callback_second_release_receiver = std::sync::Arc::clone(&second_release_receiver);
    let callback_invocations = std::sync::Arc::clone(&callback_invocations);
    db.register_sink(
        "retained_archive",
        None,
        move |_| match callback_invocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => {
                first_entered_sender.send(()).unwrap();
                let _ = callback_first_release_receiver.lock().unwrap().recv();
                Ok(())
            }
            1 => {
                second_entered_sender.send(()).unwrap();
                let _ = callback_second_release_receiver.lock().unwrap().recv();
                Err(SinkError::Transient("hold staged notes event".to_string()))
            }
            _ => Err(SinkError::Transient(
                "keep the staged notes event queued".to_string(),
            )),
        },
    )
    .unwrap();
    first_entered_receiver
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the real worker must enter the old queued callback before the received stage");

    let pause = db.pause_before_received_schema_publish_for_test();
    let (changes, received, lineages) = migration_routed_to_retained_archive();

    std::thread::scope(|scope| {
        let applying = std::sync::Arc::clone(&db);
        let applying_changes = changes.clone();
        let applying_received = received.clone();
        let applying_lineages = lineages.clone();
        let stage = scope.spawn(move || {
            applying.mark_this_thread_for_received_schema_pre_publish_pause_for_test();
            applying.commit_received_schema_stage_for_test(
                &applying_changes,
                &applying_received,
                &applying_lineages,
                &HashMap::new(),
                false,
            )
        });
        assert!(
            pause.wait_until_reached(std::time::Duration::from_secs(2)),
            "received stage must pause after durability and before memory publication"
        );

        first_release_sender.send(()).unwrap();
        wait_for_sink_state(
            "durable worker acknowledgement against the staged same-sink event",
            || {
                let queue = durable_queue_identity(&db, "retained_archive");
                db.sink_metrics_for_test("retained_archive").delivered == 1
                    && queue.len() == 1
                    && queue[0].1.table == "notes"
            },
        );
        let paused_queue = durable_queue_identity(&db, "retained_archive");
        assert_eq!(paused_queue.len(), 1);
        assert_eq!(paused_queue[0].1.table, "notes");
        assert_ne!(paused_queue[0].0, old_id);
        pause.release();
        stage.join().unwrap().unwrap();
    });

    second_entered_receiver
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the staged notes callback must be held after memory publication");

    let notes_queue = durable_queue_identity(&db, "retained_archive");
    assert_eq!(notes_queue.len(), 1);
    assert_eq!(notes_queue[0].1.table, "notes");
    assert_ne!(notes_queue[0].0, old_id);
    assert_eq!(
        db.sink_queue_entries_for_test("retained_archive"),
        notes_queue
    );
    assert_eq!(durable_queue_identity(&db, "bystander_archive"), bystander);
    assert_eq!(
        db.sink_queue_entries_for_test("bystander_archive"),
        bystander
    );

    second_release_sender.send(()).unwrap();
    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        durable_queue_identity(&reopened, "retained_archive"),
        notes_queue
    );
    assert_eq!(
        reopened.sink_queue_entries_for_test("retained_archive"),
        notes_queue
    );
    assert_eq!(
        durable_queue_identity(&reopened, "bystander_archive"),
        bystander
    );
    assert_eq!(
        reopened.sink_queue_entries_for_test("bystander_archive"),
        bystander
    );
}

#[test]
fn received_schema_acl_route_fails_closed_for_null_or_omitted_acl_refs() {
    let (changes, context, lineages) = acl_received_migration_with_incomplete_acl_refs();

    let immediate_temp = tempfile::TempDir::new().unwrap();
    let immediate_path = immediate_temp
        .path()
        .join("received-acl-ref-immediate.redb");
    let immediate = Database::open(&immediate_path).unwrap();
    immediate
        .commit_received_schema_stage_for_test(
            &changes,
            &context,
            &lineages,
            &HashMap::new(),
            false,
        )
        .unwrap();
    assert_received_incomplete_acl_refs_are_denied(&immediate, "received schema immediately");

    let reopened_temp = tempfile::TempDir::new().unwrap();
    let reopened_path = reopened_temp.path().join("received-acl-ref-reopen.redb");
    let reopened_seed = Database::open(&reopened_path).unwrap();
    reopened_seed
        .commit_received_schema_stage_for_test(
            &changes,
            &context,
            &lineages,
            &HashMap::new(),
            false,
        )
        .unwrap();
    reopened_seed.close().unwrap();
    drop(reopened_seed);
    let reopened = Database::open(&reopened_path).unwrap();
    assert_received_incomplete_acl_refs_are_denied(&reopened, "received schema after reopen");
}

#[test]
fn received_schema_acl_route_keeps_the_local_route_time_grant_snapshot() {
    let local_temp = tempfile::TempDir::new().unwrap();
    let local_path = local_temp.path().join("local-acl-route.redb");
    let local = Database::open(&local_path).unwrap();
    seed_local_acl_route(&local);
    assert_acl_route_delivery(&local, "ordinary local commit");

    let local_reopen_temp = tempfile::TempDir::new().unwrap();
    let local_reopen_path = local_reopen_temp.path().join("local-acl-route-reopen.redb");
    let local_reopen_seed = Database::open(&local_reopen_path).unwrap();
    seed_local_acl_route(&local_reopen_seed);
    local_reopen_seed.close().unwrap();
    drop(local_reopen_seed);
    let local_reopened = Database::open(&local_reopen_path).unwrap();
    local_reopened
        .execute("DELETE FROM acl_grants", &HashMap::new())
        .unwrap();
    assert_acl_route_delivery(&local_reopened, "ordinary local commit after reopen");

    let received_temp = tempfile::TempDir::new().unwrap();
    let received_path = received_temp.path().join("received-acl-route.redb");
    let received = Database::open(&received_path).unwrap();
    let (changes, context, lineages) = acl_received_migration();
    received
        .commit_received_schema_stage_for_test(
            &changes,
            &context,
            &lineages,
            &HashMap::new(),
            false,
        )
        .unwrap();
    assert_acl_route_delivery(&received, "received schema immediately");

    let received_reopen_temp = tempfile::TempDir::new().unwrap();
    let received_reopen_path = received_reopen_temp
        .path()
        .join("received-acl-route-reopen.redb");
    let received_reopen_seed = Database::open(&received_reopen_path).unwrap();
    received_reopen_seed
        .commit_received_schema_stage_for_test(
            &changes,
            &context,
            &lineages,
            &HashMap::new(),
            false,
        )
        .unwrap();
    received_reopen_seed.close().unwrap();
    drop(received_reopen_seed);
    let received_reopened = Database::open(&received_reopen_path).unwrap();
    received_reopened
        .execute("DELETE FROM acl_grants", &HashMap::new())
        .unwrap();
    assert_acl_route_delivery(&received_reopened, "received schema after reopen");

    let fault_temp = tempfile::TempDir::new().unwrap();
    let fault_path = fault_temp.path().join("faulted-received-acl-route.redb");
    let faulted = Database::open(&fault_path).unwrap();
    crate::persistence::arm_received_schema_side_effect_persistence_fault_for_test();
    assert!(
        faulted
            .commit_received_schema_stage_for_test(
                &changes,
                &context,
                &lineages,
                &HashMap::new(),
                false,
            )
            .is_err()
    );
    assert_no_acl_received_artifacts(&faulted);
    faulted.close().unwrap();
    drop(faulted);
    let faulted_reopened = Database::open(&fault_path).unwrap();
    assert_no_acl_received_artifacts(&faulted_reopened);
}

#[test]
fn received_schema_migration_publishes_sink_and_trigger_side_effects_atomically() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("received-schema-side-effects.redb");
    let db = Database::open(&path).unwrap();
    seed_preexisting_side_projection(&db);
    let (changes, received, lineages) = migration();
    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &lineages,
        &HashMap::new(),
        false,
    )
    .unwrap();
    let receiver_lsn = db.current_lsn();
    db.execute(
        "INSERT INTO retained_notes (id) VALUES (100)",
        &HashMap::new(),
    )
    .unwrap();

    assert_durable_side_projection(
        &db,
        "retained_notes",
        "retained_note_inserted",
        "retained_archive",
        "retained_note_audit",
        &[99, 100],
    );
    assert_durable_side_projection(&db, "notes", "note_inserted", "archive", "note_audit", &[1]);
    assert_exact_trigger_audit_count(&db, 3);
    assert_received_ddl_is_appended(&db, &changes, receiver_lsn);
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert_durable_side_projection(
        &reopened,
        "retained_notes",
        "retained_note_inserted",
        "retained_archive",
        "retained_note_audit",
        &[99, 100],
    );
    assert_durable_side_projection(
        &reopened,
        "notes",
        "note_inserted",
        "archive",
        "note_audit",
        &[1],
    );
    assert_exact_trigger_audit_count(&reopened, 3);
    assert_received_ddl_is_appended(&reopened, &changes, receiver_lsn);
}

#[test]
fn failed_received_schema_migration_publishes_no_side_effects() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp
        .path()
        .join("received-schema-side-effects-failure.redb");
    let db = Database::open(&path).unwrap();
    seed_preexisting_side_projection(&db);
    let (changes, received, lineages) = migration();
    let baseline_changes = db.changes_since(Lsn(0));
    let baseline_config = config_snapshot(&db);
    let baseline_event_bus = db.received_event_bus_projection();
    let baseline_triggers = db.list_triggers();
    let baseline_audit_ring = db.trigger_audit_log();
    crate::persistence::arm_received_schema_side_effect_persistence_fault_for_test();
    assert!(
        db.commit_received_schema_stage_for_test(
            &changes,
            &received,
            &lineages,
            &HashMap::new(),
            false,
        )
        .is_err()
    );
    assert_exact_change_log(&db.changes_since(Lsn(0)), &baseline_changes);
    assert_eq!(config_snapshot(&db), baseline_config);
    assert_eq!(db.received_event_bus_projection(), baseline_event_bus);
    assert_eq!(db.list_triggers(), baseline_triggers);
    assert_eq!(db.trigger_audit_log(), baseline_audit_ring);
    assert_durable_side_projection(
        &db,
        "retained_notes",
        "retained_note_inserted",
        "retained_archive",
        "retained_note_audit",
        &[99],
    );
    assert_exact_trigger_audit_count(&db, 1);
    assert_no_received_side_projection(&db);
    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert_exact_change_log(&reopened.changes_since(Lsn(0)), &baseline_changes);
    assert_eq!(config_snapshot(&reopened), baseline_config);
    assert_eq!(reopened.received_event_bus_projection(), baseline_event_bus);
    assert_eq!(reopened.list_triggers(), baseline_triggers);
    assert_eq!(reopened.trigger_audit_log(), baseline_audit_ring);
    assert_durable_side_projection(
        &reopened,
        "retained_notes",
        "retained_note_inserted",
        "retained_archive",
        "retained_note_audit",
        &[99],
    );
    assert_exact_trigger_audit_count(&reopened, 1);
    assert_no_received_side_projection(&reopened);
}
