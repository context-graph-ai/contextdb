use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetrySnapshot {
    schema: String,
    rows: String,
    changes: String,
    current_lsn: Lsn,
    config: Vec<(String, Vec<u8>)>,
    durable_queue: String,
    triggers: String,
    trigger_audits: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemoryRetrySnapshot {
    schema: String,
    rows: String,
    changes: String,
    current_lsn: Lsn,
    live_queue: String,
    triggers: String,
    trigger_audits: String,
    receipt_and_applied_watermarks: String,
}

fn ddl_vector() -> Vec<DdlChange> {
    vec![
        DdlChange::CreateTable {
            name: "retry_notes".to_string(),
            columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
        DdlChange::CreateEventType {
            name: "retry_note_inserted".to_string(),
            trigger: "INSERT".to_string(),
            table: "retry_notes".to_string(),
        },
        DdlChange::CreateSink {
            name: "retry_archive".to_string(),
            sink_type: "CALLBACK".to_string(),
            url: None,
        },
        DdlChange::CreateRoute {
            name: "retry_archive_notes".to_string(),
            event_type: "retry_note_inserted".to_string(),
            sink: "retry_archive".to_string(),
            table: "retry_notes".to_string(),
            where_in: None,
        },
        DdlChange::CreateTrigger {
            name: "retry_note_audit".to_string(),
            table: "retry_notes".to_string(),
            on_events: vec!["INSERT".to_string()],
        },
    ]
}

type ReceivedSchemaFixture = (
    ChangeSet,
    crate::protocol::ReceivedDdlContext,
    Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
);

fn received_migration(ddl: Vec<DdlChange>, include_row: bool) -> ReceivedSchemaFixture {
    let source_lsn = Lsn(90);
    let entries = ddl
        .iter()
        .enumerate()
        .map(|(ordinal, change)| {
            let table = Database::ddl_affected_table(change).map(str::to_string);
            let table_generation = table.as_ref().map(|_| 1);
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: source_lsn,
                ordinal: ordinal as u32,
                table: table.clone(),
                table_generation,
                digest: crate::protocol::canonical_ddl_provenance_digest(
                    &crate::protocol::WireDdlChange::from(change.clone()),
                    source_lsn,
                    ordinal as u32,
                    table.as_deref(),
                    table_generation,
                )
                .unwrap(),
            }
        })
        .collect();
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let rows = if include_row {
        vec![RowChange {
            table: "retry_notes".to_string(),
            natural_key: key.clone(),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(91),
            created_at: None,
        }]
    } else {
        Vec::new()
    };
    let lineages = if include_row {
        vec![(
            "retry_notes".to_string(),
            key,
            Lsn(91),
            crate::protocol::WireRowLineage {
                author_node_id: "retry-source".to_string(),
                author_database_incarnation: Incarnation(11),
                author_local_mutation_position: Lsn(91),
                table_generation: 1,
                lineage_root: format!("author:retry-source:{}:91", Incarnation(11).to_hex()),
                attestation: vec![],
            },
        )]
    } else {
        Vec::new()
    };
    (
        ChangeSet {
            ddl_lsn: vec![source_lsn; ddl.len()],
            ddl,
            rows,
            ..ChangeSet::default()
        },
        crate::protocol::ReceivedDdlContext {
            tenant_id: TenantId::from("retry-tenant"),
            source_node_id: "retry-source".to_string(),
            source_incarnation: Incarnation(11),
            entries,
        },
        lineages,
    )
}

fn receipt() -> SyncApplyReceipt {
    SyncApplyReceipt {
        tenant_id: TenantId::from("retry-tenant"),
        node_id: "retry-source".to_string(),
        incarnation: Incarnation(11),
        source_lsn: Lsn(90),
        dependency_complete: true,
    }
}

fn apply_received(
    db: &Database,
    changes: &ChangeSet,
    received: &crate::protocol::ReceivedDdlContext,
    lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
) -> Result<crate::protocol::WireApplyResult> {
    db.commit_received_schema_stage_with_receipt_for_test(
        changes,
        received,
        lineages,
        &HashMap::new(),
        Some(receipt()),
        false,
    )
}

fn snapshot(db: &Database) -> RetrySnapshot {
    let mut config = db.persistence.as_ref().unwrap().dump_config_raw().unwrap();
    config.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    RetrySnapshot {
        schema: format!("{:?}", db.table_meta("retry_notes")),
        rows: format!("{:?}", db.relational_store.tables.read().get("retry_notes")),
        changes: format!("{:?}", db.changes_since(Lsn(0))),
        current_lsn: db.current_lsn(),
        config,
        durable_queue: format!(
            "{:?}",
            db.persistence
                .as_ref()
                .unwrap()
                .load_sink_queue::<event_bus::SinkQueueEntry>("retry_archive")
                .unwrap()
        ),
        triggers: format!("{:?}", db.list_triggers()),
        trigger_audits: format!(
            "{:?}",
            db.persistence
                .as_ref()
                .unwrap()
                .load_trigger_audit_history()
                .unwrap()
        ),
    }
}

fn memory_snapshot(db: &Database) -> MemoryRetrySnapshot {
    MemoryRetrySnapshot {
        schema: format!("{:?}", db.table_meta("retry_notes")),
        rows: format!("{:?}", db.relational_store.tables.read().get("retry_notes")),
        changes: format!("{:?}", db.changes_since(Lsn(0))),
        current_lsn: db.current_lsn(),
        live_queue: format!("{:?}", db.sink_queue_entries_for_test("retry_archive")),
        triggers: format!("{:?}", db.list_triggers()),
        trigger_audits: format!("{:?}", db.trigger_audit_log()),
        receipt_and_applied_watermarks: format!(
            "{:?}",
            db.in_memory_applied_push_watermarks.lock()
        ),
    }
}

fn reopen_if_requested(db: Database, path: &std::path::Path, reopen: bool) -> Database {
    if !reopen {
        return db;
    }
    db.close().unwrap();
    drop(db);
    Database::open(path).unwrap()
}

#[test]
fn identical_received_marker_vector_retry_returns_its_original_observation_without_mutation() {
    let (changes, received, lineages) = received_migration(ddl_vector(), true);
    for reopen in [false, true] {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("identical-retry.redb");
        let db = Database::open(&path).unwrap();
        let original = apply_received(&db, &changes, &received, &lineages).unwrap();
        let db = reopen_if_requested(db, &path, reopen);
        let before = snapshot(&db);
        let retry = apply_received(&db, &changes, &received, &lineages)
            .expect("an identical ordered marker vector must return its original observation");
        assert_eq!(retry, original, "identical retry after reopen={reopen}");
        assert_eq!(
            snapshot(&db),
            before,
            "identical retry after reopen={reopen}"
        );
    }
}

#[test]
fn mixed_applied_and_absent_received_markers_refuse_the_whole_migration_without_mutation() {
    let (prefix_changes, prefix_received, prefix_lineages) =
        received_migration(ddl_vector().into_iter().take(1).collect(), false);
    let (full_changes, full_received, full_lineages) = received_migration(ddl_vector(), true);
    for reopen in [false, true] {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("mixed-marker-retry.redb");
        let db = Database::open(&path).unwrap();
        apply_received(&db, &prefix_changes, &prefix_received, &prefix_lineages).unwrap();
        let db = reopen_if_requested(db, &path, reopen);
        let before = snapshot(&db);
        let error = apply_received(&db, &full_changes, &full_received, &full_lineages)
            .expect_err("one applied and one absent marker must refuse the whole migration");
        assert!(
            matches!(error, Error::SyncError(_)),
            "mixed marker refusal must be typed, got {error:?}"
        );
        assert_eq!(
            snapshot(&db),
            before,
            "mixed marker retry after reopen={reopen}"
        );
    }
}

#[test]
fn changed_ddl_with_the_same_marker_identity_refuses_and_preserves_the_original_marker() {
    let (original_changes, original_received, original_lineages) =
        received_migration(ddl_vector(), true);
    let mut changed_ddl = ddl_vector();
    changed_ddl[0] = DdlChange::CreateTable {
        name: "retry_notes".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
            ("changed".to_string(), "TEXT".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let (changed_changes, changed_received, changed_lineages) =
        received_migration(changed_ddl, true);
    for reopen in [false, true] {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("changed-digest-retry.redb");
        let db = Database::open(&path).unwrap();
        apply_received(
            &db,
            &original_changes,
            &original_received,
            &original_lineages,
        )
        .unwrap();
        let db = reopen_if_requested(db, &path, reopen);
        let before = snapshot(&db);
        let error = apply_received(&db, &changed_changes, &changed_received, &changed_lineages)
            .expect_err(
                "changed DDL under an existing marker identity must refuse the whole migration",
            );
        assert!(
            matches!(error, Error::SyncError(_)),
            "changed marker digest refusal must be typed, got {error:?}"
        );
        assert_eq!(
            snapshot(&db),
            before,
            "changed marker retry after reopen={reopen} must preserve the original digest and state"
        );
    }
}

#[test]
fn memory_identical_received_marker_vector_replays_the_complete_original_result_without_mutation() {
    let (changes, received, lineages) = received_migration(ddl_vector(), true);
    let db = Database::open_memory();
    let original = apply_received(&db, &changes, &received, &lineages).unwrap();
    let before = memory_snapshot(&db);
    let retry = apply_received(&db, &changes, &received, &lineages)
        .expect("an identical memory retry must replay its original WireApplyResult");
    assert_eq!(retry, original, "identical memory retry");
    assert_eq!(memory_snapshot(&db), before, "identical memory retry");
}

#[test]
fn memory_mixed_applied_and_absent_markers_refuse_the_whole_migration_without_mutation() {
    let (prefix_changes, prefix_received, prefix_lineages) =
        received_migration(ddl_vector().into_iter().take(1).collect(), false);
    let (full_changes, full_received, full_lineages) = received_migration(ddl_vector(), true);
    let db = Database::open_memory();
    apply_received(&db, &prefix_changes, &prefix_received, &prefix_lineages).unwrap();
    let before = memory_snapshot(&db);
    let error = apply_received(&db, &full_changes, &full_received, &full_lineages)
        .expect_err("mixed memory markers must refuse the whole migration");
    assert!(
        matches!(error, Error::SyncError(_)),
        "mixed memory marker refusal must be typed, got {error:?}"
    );
    assert_eq!(memory_snapshot(&db), before, "mixed memory marker retry");
}

#[test]
fn memory_changed_ddl_with_the_same_marker_identity_refuses_without_mutation() {
    let (original_changes, original_received, original_lineages) =
        received_migration(ddl_vector(), true);
    let mut changed_ddl = ddl_vector();
    changed_ddl[0] = DdlChange::CreateTable {
        name: "retry_notes".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
            ("changed".to_string(), "TEXT".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let (changed_changes, changed_received, changed_lineages) =
        received_migration(changed_ddl, true);
    let db = Database::open_memory();
    apply_received(
        &db,
        &original_changes,
        &original_received,
        &original_lineages,
    )
    .unwrap();
    let before = memory_snapshot(&db);
    let error = apply_received(&db, &changed_changes, &changed_received, &changed_lineages)
        .expect_err("changed memory marker digest must refuse the whole migration");
    assert!(
        matches!(error, Error::SyncError(_)),
        "changed memory marker digest refusal must be typed, got {error:?}"
    );
    assert_eq!(memory_snapshot(&db), before, "changed memory marker retry");
}
