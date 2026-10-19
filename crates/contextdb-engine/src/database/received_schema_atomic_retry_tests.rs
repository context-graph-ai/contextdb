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

fn structured_table_ddl_with_including_sync_trigger() -> Vec<DdlChange> {
    let owner_columns = vec![
        ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
        ("tenant".to_string(), "TEXT".to_string()),
    ];
    let note_columns = vec![
        ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
        ("tenant".to_string(), "TEXT".to_string()),
        ("owner_id".to_string(), "INTEGER".to_string()),
        ("slug".to_string(), "TEXT".to_string()),
    ];
    let note_foreign_keys = vec![contextdb_core::SingleColumnForeignKey {
        child_column: "owner_id".to_string(),
        parent_table: "retry_owners".to_string(),
        parent_column: "id".to_string(),
    }];
    let note_composite_foreign_keys = vec![contextdb_core::CompositeForeignKey {
        child_columns: vec!["tenant".to_string(), "owner_id".to_string()],
        parent_table: "retry_owners".to_string(),
        parent_columns: vec!["tenant".to_string(), "id".to_string()],
    }];
    let note_composite_unique = vec![vec!["tenant".to_string(), "slug".to_string()]];
    let mut altered_note_columns = note_columns.clone();
    altered_note_columns.push(("content".to_string(), "TEXT".to_string()));
    vec![
        DdlChange::CreateTable {
            name: "retry_owners".to_string(),
            columns: owner_columns,
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: vec![vec!["tenant".to_string(), "id".to_string()]],
        },
        DdlChange::CreateTable {
            name: "retry_notes".to_string(),
            columns: note_columns,
            constraints: Vec::new(),
            foreign_keys: note_foreign_keys.clone(),
            composite_foreign_keys: note_composite_foreign_keys.clone(),
            composite_unique: note_composite_unique.clone(),
        },
        DdlChange::AlterTable {
            name: "retry_notes".to_string(),
            columns: altered_note_columns,
            constraints: Vec::new(),
            foreign_keys: note_foreign_keys,
            composite_foreign_keys: note_composite_foreign_keys,
            composite_unique: note_composite_unique,
        },
        DdlChange::CreateTriggerIncludingSync {
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
                author: None,
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
        hub_contact_ms: None,
    }
}

fn apply_received(
    db: &Database,
    changes: &ChangeSet,
    received: &crate::protocol::ReceivedDdlContext,
    lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
) -> Result<crate::protocol::WireApplyResult> {
    let received_schema_read_memory_1 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_1 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_1.clone());

    db.commit_received_schema_stage_with_receipt_for_test(
        changes,
        received,
        lineages,
        &HashMap::new(),
        Some(receipt()),
        false,
        &mut received_schema_copy_owner_1,
        received_schema_read_memory_1,
    )
}

fn snapshot(db: &Database) -> RetrySnapshot {
    let mut config = db.persistence.as_ref().unwrap().dump_config_raw().unwrap();
    config.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    RetrySnapshot {
        schema: format!("{:?}", db.table_meta("retry_notes")),
        rows: format!("{:?}", db.relational_store.tables.read().get("retry_notes")),
        changes: format!(
            "{:?}",
            db.changes_since(Lsn(0)).expect("read the declared changes")
        ),
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
        changes: format!(
            "{:?}",
            db.changes_since(Lsn(0)).expect("read the declared changes")
        ),
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

fn assert_structured_received_schema_has_no_pending_ddl(db: &Database) {
    let mut trigger_arrivals = Vec::new();
    db.visit_received_ddl_arrivals(
        |occurrence, _| {
            if occurrence.1 == 3 {
                trigger_arrivals.push(occurrence);
            }
            Ok(())
        },
        db.declared_reader_memory()
            .expect("admit the DDL arrival reader"),
    )
    .expect("visit durable received occurrences");
    assert_eq!(
        trigger_arrivals.len(),
        1,
        "the INCLUDING SYNC trigger keeps one source-order ordinal three"
    );
    let trigger_arrival = trigger_arrivals[0];
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(matches!(
        db.raw_ddl_log_occurrence(trigger_arrival.0, trigger_arrival.1, fixture_operation_reader_1.clone())
            .unwrap(),
        DdlChange::CreateTriggerIncludingSync { ref name, ref table, ref on_events }
            if name == "retry_note_audit"
                && table == "retry_notes"
                && on_events == &vec!["INSERT".to_string()]
    ));
    let fixture_operation_reader_2 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    let (pending, provenance) = db
        .changes_since_base(
            Lsn(0),
            &mut fixture_operation_copy_2,
            fixture_operation_reader_2.clone(),
        )
        .expect("received-schema outbound extraction must read its durable vector directory");
    let fixture_operation_reader_3 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let outbound = db
        .filter_outbound_received_ddl(
            pending,
            &provenance,
            None,
            fixture_operation_reader_3.clone(),
        )
        .expect("received structured DDL must bind its local durable occurrence");
    assert!(outbound.ddl.is_empty());

    let meta = db.table_meta("retry_notes").unwrap();
    assert_eq!(meta.composite_foreign_keys.len(), 1);
    assert!(
        meta.unique_constraints
            .iter()
            .any(|columns| { columns == &vec!["tenant".to_string(), "slug".to_string()] })
    );
    assert!(
        meta.columns
            .iter()
            .any(|column| { column.name == "owner_id" && column.references.is_some() })
    );
    assert!(meta.columns.iter().any(|column| column.name == "content"));

    let visible = db.changes_since(Lsn(0)).expect("read the declared changes");
    for ddl in visible.ddl.iter().filter(|ddl| {
        matches!(
            ddl,
            DdlChange::CreateTable { name, .. } | DdlChange::AlterTable { name, .. }
                if name == "retry_notes"
        )
    }) {
        match ddl {
            DdlChange::CreateTable {
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
                ..
            }
            | DdlChange::AlterTable {
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
                ..
            } => {
                assert_eq!(foreign_keys.len(), 1);
                assert_eq!(composite_foreign_keys.len(), 1);
                assert_eq!(composite_unique.len(), 1);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn structured_received_table_ddl_and_including_sync_trigger_stay_non_pending_across_reopen() {
    let ddl = structured_table_ddl_with_including_sync_trigger();
    let (changes, received, lineages) = received_migration(ddl.clone(), false);
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("structured-received-ddl.redb");
    let db = Database::open(&path).unwrap();
    apply_received(&db, &changes, &received, &lineages).unwrap();
    assert_structured_received_schema_has_no_pending_ddl(&db);

    let before_retry = snapshot(&db);
    let mut changed_ddl = ddl;
    let DdlChange::AlterTable {
        composite_unique, ..
    } = &mut changed_ddl[2]
    else {
        unreachable!()
    };
    composite_unique.push(vec!["owner_id".to_string(), "slug".to_string()]);
    let (changed, changed_received, changed_lineages) = received_migration(changed_ddl, false);
    let error = apply_received(&db, &changed, &changed_received, &changed_lineages)
        .expect_err("authenticated structured-field mutation must not reuse the source manifest");
    assert!(
        matches!(&error, Error::SyncError(reason) if reason.contains("retry changes the digest")),
        "the full authenticated source digest remains authoritative: {error:?}"
    );
    assert_eq!(snapshot(&db), before_retry);

    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert_structured_received_schema_has_no_pending_ddl(&reopened);
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

/// Policies 14 and 16: a received dependency-complete unit interleaves data
/// and ALTER by source position, validates the final FK overlay, and publishes
/// one commit. This trusted kernel fixture deliberately bypasses transport
/// authentication; the public authenticated-sync suite owns that separate bar.
/// Existing retry cases have no row on both sides of a shape-changing ALTER.
#[test]
fn received_mixed_data_alter_data_keeps_authored_values_order_and_one_commit() {
    let parent = "mixed_parents";
    let child = "mixed_children";
    let foreign_keys = vec![contextdb_core::SingleColumnForeignKey {
        child_column: "parent_id".to_string(),
        parent_table: parent.to_string(),
        parent_column: "id".to_string(),
    }];
    let initial_columns = vec![
        ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
        ("parent_id".to_string(), "INTEGER".to_string()),
        ("body".to_string(), "TEXT".to_string()),
    ];
    let (seed, seed_context, seed_lineages) = received_migration(
        vec![
            DdlChange::CreateTable {
                name: parent.to_string(),
                columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
            DdlChange::CreateTable {
                name: child.to_string(),
                columns: initial_columns,
                constraints: Vec::new(),
                foreign_keys: foreign_keys.clone(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        ],
        false,
    );
    // These are source TRANSPORT positions; immutable author coordinates below
    // deliberately differ. Neither coordinate is the receiver's public END.
    let source_a = Lsn(101);
    let source_b = Lsn(102);
    let source_c = Lsn(103);
    let text_uuid = uuid::Uuid::from_u128(0x1122).to_string();
    let later_uuid = uuid::Uuid::from_u128(0x3344);
    let key = |id| NaturalKey::single("id".to_string(), Value::Int64(id));
    let pre_row = RowChange {
        table: child.to_string(),
        natural_key: key(1),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(1)),
            ("parent_id".to_string(), Value::Int64(7)),
            ("body".to_string(), Value::Text(text_uuid.clone())),
        ]),
        deleted: false,
        lsn: source_a,
        created_at: None,
    };
    let parent_row = RowChange {
        table: parent.to_string(),
        natural_key: key(7),
        values: HashMap::from([("id".to_string(), Value::Int64(7))]),
        deleted: false,
        lsn: source_c,
        created_at: None,
    };
    let post_row = RowChange {
        table: child.to_string(),
        natural_key: key(2),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(2)),
            ("parent_id".to_string(), Value::Int64(7)),
            ("previous_body".to_string(), Value::Null),
            ("body".to_string(), Value::Uuid(later_uuid)),
        ]),
        deleted: false,
        lsn: source_c,
        created_at: None,
    };
    let renamed_columns = vec![
        ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
        ("parent_id".to_string(), "INTEGER".to_string()),
        ("previous_body".to_string(), "TEXT".to_string()),
    ];
    let mut final_columns = renamed_columns.clone();
    final_columns.push(("body".to_string(), "UUID".to_string()));
    // Two source-ordered ALTER statements at B first rename the old TEXT
    // column, then reuse its old name for UUID. Thus the exact same UUID-shaped
    // text at A must remain Text under previous_body, never be decoded as UUID.
    let changes = ChangeSet {
        ddl: vec![
            DdlChange::AlterTable {
                name: child.to_string(),
                columns: renamed_columns,
                constraints: vec![sync_column_rename_constraint("body", "previous_body")],
                foreign_keys: foreign_keys.clone(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
            DdlChange::AlterTable {
                name: child.to_string(),
                columns: final_columns,
                constraints: Vec::new(),
                foreign_keys,
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        ],
        ddl_lsn: vec![source_b, source_b],
        rows: vec![pre_row, parent_row, post_row],
        ..ChangeSet::default()
    };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: seed_context.tenant_id.clone(),
        source_node_id: seed_context.source_node_id.clone(),
        source_incarnation: seed_context.source_incarnation,
        entries: changes
            .ddl
            .iter()
            .enumerate()
            .map(|(ordinal, ddl)| {
                let table = Some(child.to_string());
                let table_generation = Some(1);
                let digest = crate::protocol::canonical_ddl_provenance_digest(
                    &crate::protocol::WireDdlChange::from(ddl.clone()),
                    source_b,
                    ordinal as u32,
                    table.as_deref(),
                    table_generation,
                )
                .unwrap();
                crate::protocol::ReceivedDdlEntry {
                    source_ddl_lsn: source_b,
                    ordinal: ordinal as u32,
                    table: table.clone(),
                    table_generation,
                    digest: digest.clone(),
                    author: Some(crate::protocol::WireDdlAuthor {
                        node_id: "retry-source".to_string(),
                        incarnation: Incarnation(11),
                        source_ddl_lsn: source_b,
                        ordinal: ordinal as u32,
                        table,
                        table_generation,
                        digest,
                        attestation: Vec::new(),
                        unit_statements: 2,
                    }),
                }
            })
            .collect(),
    };
    let lineages = changes
        .rows
        .iter()
        .zip([Lsn(501), Lsn(503), Lsn(504)])
        .map(|(row, author_position)| {
            (
                row.table.clone(),
                row.natural_key.clone(),
                row.lsn,
                crate::protocol::WireRowLineage {
                    author_node_id: "retry-source".to_string(),
                    author_database_incarnation: Incarnation(11),
                    author_local_mutation_position: author_position,
                    table_generation: 1,
                    lineage_root: format!(
                        "author:retry-source:{}:{}",
                        Incarnation(11).to_hex(),
                        author_position.0
                    ),
                    attestation: Vec::new(),
                },
            )
        })
        .collect::<Vec<_>>();
    let apply_complete =
        |db: &Database,
         unit: &ChangeSet,
         context: &crate::protocol::ReceivedDdlContext,
         lineage: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
         end: Lsn| {
            let memory = db.declared_reader_memory().unwrap();
            let mut copies = contextdb_engine::vector_read_memory::admission(memory.clone());
            db.commit_received_schema_stage_with_receipt_for_test(
                unit,
                context,
                lineage,
                &HashMap::new(),
                Some(SyncApplyReceipt {
                    tenant_id: context.tenant_id.clone(),
                    node_id: context.source_node_id.clone(),
                    incarnation: context.source_incarnation,
                    source_lsn: end,
                    dependency_complete: true,
                    hub_contact_ms: None,
                }),
                false,
                &mut copies,
                memory,
            )
        };
    let assert_visible = |db: &Database| {
        let actual = db
            .execute(
                "SELECT id, parent_id, previous_body, body FROM mixed_children ORDER BY id",
                &HashMap::new(),
            )
            .unwrap()
            .rows;
        assert_eq!(
            actual,
            vec![
                vec![
                    Value::Int64(1),
                    Value::Int64(7),
                    Value::Text(text_uuid.clone()),
                    Value::Null
                ],
                vec![
                    Value::Int64(2),
                    Value::Int64(7),
                    Value::Null,
                    Value::Uuid(later_uuid)
                ],
            ],
            "the pre-ALTER text is renamed; the post-ALTER value is UUID"
        );
        assert_eq!(
            db.execute("SELECT id FROM mixed_parents", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(7)]]
        );
    };
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("mixed-received.redb");
    let db = Database::open(&path).unwrap();
    apply_received(&db, &seed, &seed_context, &seed_lineages).unwrap();
    let before = db.current_lsn();
    let commits_before = db.tx_mgr.commit_index_len();
    assert!(
        db.execute("SELECT id FROM mixed_parents", &HashMap::new())
            .unwrap()
            .rows
            .is_empty(),
        "the parent exists only later in this unit"
    );
    assert!(source_a < source_b && source_b < source_c);
    let outcome = apply_complete(&db, &changes, &received, &lineages, source_c)
        .expect("one complete overlay must accept data A / ALTER B / parent data C");
    assert_eq!(outcome.applied_rows, 3);
    assert_eq!(outcome.skipped_rows, 0);
    assert!(outcome.conflicts.is_empty());
    assert_eq!(
        db.tx_mgr.commit_index_len(),
        commits_before + 1,
        "multiple source positions publish exactly one receiver commit"
    );
    assert_eq!(db.current_lsn(), outcome.new_lsn, "one public END");
    assert!(outcome.new_lsn > before);
    assert_visible(&db);

    let memory = db.declared_reader_memory().unwrap();
    let mut copies = contextdb_engine::vector_read_memory::admission(memory.clone());
    let (outbound, provenance_source) = db
        .changes_since_base(before, &mut copies, memory.clone())
        .expect("read the exact accepted history for onward relay");
    assert_eq!(outbound.rows.len(), 3);
    assert_eq!(outbound.ddl, changes.ddl);
    let forwarded_pre = outbound
        .rows
        .iter()
        .find(|row| row.table == child && row.natural_key == key(1))
        .unwrap();
    let forwarded_parent = outbound
        .rows
        .iter()
        .find(|row| row.table == parent)
        .unwrap();
    for original in &changes.rows {
        let forwarded = outbound
            .rows
            .iter()
            .find(|row| row.table == original.table && row.natural_key == original.natural_key)
            .unwrap();
        assert_eq!(
            forwarded.values, original.values,
            "relay keeps the authored body"
        );
    }
    assert_eq!(
        serde_json::to_value(forwarded_pre.values.get("body").unwrap()).unwrap(),
        serde_json::json!({"Text": text_uuid}),
        "history retains the serialized Value::Text tag"
    );
    assert_eq!(outbound.ddl_lsn.len(), 2);
    assert!(forwarded_pre.lsn < outbound.ddl_lsn[0]);
    assert_eq!(outbound.ddl_lsn[0], outbound.ddl_lsn[1]);
    assert!(outbound.ddl_lsn[1] < forwarded_parent.lsn);
    assert!(
        forwarded_parent.lsn <= outcome.new_lsn,
        "interior replay positions lie inside the one published END"
    );
    let commit_positions = db.tx_mgr.commit_index_snapshot();
    assert_eq!(
        commit_positions
            .range((
                std::ops::Bound::Excluded(before),
                std::ops::Bound::Included(outcome.new_lsn)
            ))
            .map(|(lsn, _)| *lsn)
            .collect::<Vec<_>>(),
        vec![outcome.new_lsn]
    );
    let outgoing_lineages = db
        .outbound_row_lineages(
            &outbound,
            &received.tenant_id,
            "mixed-relay",
            Incarnation(22),
            &|_| Ok(Vec::new()),
            memory.clone(),
        )
        .unwrap();
    let forwarded_lineages = outbound
        .rows
        .iter()
        .map(|row| {
            let lineage = outgoing_lineages
                .get(&(
                    row.table.clone(),
                    rmp_serde::to_vec(&row.natural_key).unwrap(),
                    row.lsn,
                ))
                .unwrap();
            let original = lineages
                .iter()
                .find(|(table, natural_key, _, _)| {
                    table == &row.table && natural_key == &row.natural_key
                })
                .unwrap();
            assert_eq!(
                lineage, &original.3,
                "relay never substitutes its own author coordinate"
            );
            assert_ne!(lineage.author_local_mutation_position, row.lsn);
            assert_ne!(lineage.author_local_mutation_position, outcome.new_lsn);
            (
                row.table.clone(),
                row.natural_key.clone(),
                row.lsn,
                lineage.clone(),
            )
        })
        .collect::<Vec<_>>();
    let outgoing_ddl = db
        .outbound_ddl_provenance(
            &outbound,
            &provenance_source,
            &received.tenant_id,
            "mixed-relay",
            Incarnation(22),
            &|_| Ok(Vec::new()),
            memory,
        )
        .unwrap();
    let relay_context = crate::protocol::ReceivedDdlContext {
        tenant_id: received.tenant_id.clone(),
        source_node_id: "mixed-relay".to_string(),
        source_incarnation: Incarnation(22),
        entries: outgoing_ddl
            .into_iter()
            .enumerate()
            .map(|(ordinal, entry)| {
                let author = entry
                    .author
                    .as_ref()
                    .expect("the original DDL author survives");
                assert_eq!(author.node_id, "retry-source");
                assert_eq!(author.incarnation, Incarnation(11));
                assert_eq!(author.source_ddl_lsn, source_b);
                assert_eq!(author.ordinal, ordinal as u32);
                assert_eq!(author.digest, received.entries[ordinal].digest);
                crate::protocol::ReceivedDdlEntry {
                    source_ddl_lsn: entry.source_ddl_lsn,
                    ordinal: entry.ordinal,
                    table: entry.table,
                    table_generation: entry.table_generation,
                    digest: entry.digest,
                    author: entry.author,
                }
            })
            .collect(),
    };
    let second = Database::open(&temp.path().join("mixed-second.redb")).unwrap();
    apply_received(&second, &seed, &seed_context, &seed_lineages).unwrap();
    let second_commits = second.tx_mgr.commit_index_len();
    let second_outcome = apply_complete(
        &second,
        &outbound,
        &relay_context,
        &forwarded_lineages,
        outcome.new_lsn,
    )
    .expect("the second receiver replays the same unit");
    assert_eq!(second_outcome.applied_rows, 3);
    assert!(second_outcome.conflicts.is_empty());
    assert_eq!(second.tx_mgr.commit_index_len(), second_commits + 1);
    assert_eq!(second.current_lsn(), second_outcome.new_lsn);
    assert_visible(&second);

    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert_eq!(reopened.current_lsn(), outcome.new_lsn);
    assert_visible(&reopened);
}

// Native memory rows of the mapped mixed source-order contract. The original
// test above and every earlier body remain byte-identical. These wrappers are
// separate because the frozen bodies cannot be parameterized by this packet.
#[derive(Clone, Copy, Debug)]
enum NativeMemoryReplayCase { Clean, CallbackRefusal, MemoryRefusal, GraphGuard }

fn open_native_received_memory() -> Database {
    let db = Database::open_owned_for_test(
        std::path::Path::new(":memory:"), DatabaseOpenOptions {
            memory_limit: Some(1_610_612_736),
            ..DatabaseOpenOptions::default()
        },
    ).expect("construct Native memory Database from its actual open-time writer");
    assert!(matches!(&db.tx_mgr, DatabaseTransactionHost::Owned(_)),
        "Native memory replay requires DatabaseTransactionHost::Owned after every construction");
    assert_eq!(db.memory_limit(), Some(1_610_612_736), "keep the invariant writer budget");
    db
}

/// Policies 14/16/31: Native received rows preserve authored types and source
/// prefixes, publish once, and leave no rows/schema/receipt/audits on refusal.
/// The force-Legacy constructor mutant is not caught by the frozen Legacy row.
#[test]
fn native_memory_received_source_prefix_rows() {
    for case in [NativeMemoryReplayCase::Clean, NativeMemoryReplayCase::CallbackRefusal,
        NativeMemoryReplayCase::MemoryRefusal] {
        assert_native_memory_received_source_prefix(case);
    }
}

/// Separate runnable RED obligation: the immutable graph exclusion still refuses
/// private graph prefix selection, including after packet525. No ignore or skip.
#[test]
fn native_memory_received_graph_prefix_requires_complete_path() {
    assert_native_memory_received_source_prefix(NativeMemoryReplayCase::GraphGuard);
}

fn assert_native_memory_received_source_prefix(case: NativeMemoryReplayCase) {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let parent = "mixed_parents";
    let child = "mixed_children";
    let foreign_keys = vec![contextdb_core::SingleColumnForeignKey {
        child_column: "parent_id".to_string(),
        parent_table: parent.to_string(),
        parent_column: "id".to_string(),
    }];
    let initial_columns = vec![
        ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
        ("parent_id".to_string(), "INTEGER".to_string()),
        ("body".to_string(), "TEXT".to_string()),
    ];
    let (seed, seed_context, seed_lineages) = received_migration(
        vec![
            DdlChange::CreateTable {
                name: parent.to_string(),
                columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
            DdlChange::CreateTable {
                name: child.to_string(),
                columns: initial_columns,
                constraints: Vec::new(),
                foreign_keys: foreign_keys.clone(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        ],
        false,
    );
    // These are source TRANSPORT positions; immutable author coordinates below
    // deliberately differ. Neither coordinate is the receiver's public END.
    let source_a = Lsn(101);
    let source_b = Lsn(102);
    let source_c = Lsn(103);
    let text_uuid = uuid::Uuid::from_u128(0x1122).to_string();
    let later_uuid = uuid::Uuid::from_u128(0x3344);
    let key = |id| NaturalKey::single("id".to_string(), Value::Int64(id));
    let pre_row = RowChange {
        table: child.to_string(),
        natural_key: key(1),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(1)),
            ("parent_id".to_string(), Value::Int64(7)),
            ("body".to_string(), Value::Text(text_uuid.clone())),
        ]),
        deleted: false,
        lsn: source_a,
        created_at: None,
    };
    let parent_row = RowChange {
        table: parent.to_string(),
        natural_key: key(7),
        values: HashMap::from([("id".to_string(), Value::Int64(7))]),
        deleted: false,
        lsn: source_c,
        created_at: None,
    };
    let post_row = RowChange {
        table: child.to_string(),
        natural_key: key(2),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(2)),
            ("parent_id".to_string(), Value::Int64(7)),
            ("previous_body".to_string(), Value::Null),
            ("body".to_string(), Value::Uuid(later_uuid)),
        ]),
        deleted: false,
        lsn: source_c,
        created_at: None,
    };
    let renamed_columns = vec![
        ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
        ("parent_id".to_string(), "INTEGER".to_string()),
        ("previous_body".to_string(), "TEXT".to_string()),
    ];
    let mut final_columns = renamed_columns.clone();
    final_columns.push(("body".to_string(), "UUID".to_string()));
    // Two source-ordered ALTER statements at B first rename the old TEXT
    // column, then reuse its old name for UUID. Thus the exact same UUID-shaped
    // text at A must remain Text under previous_body, never be decoded as UUID.
    let mut changes = ChangeSet {
        ddl: vec![
            DdlChange::AlterTable {
                name: child.to_string(),
                columns: renamed_columns,
                constraints: vec![sync_column_rename_constraint("body", "previous_body")],
                foreign_keys: foreign_keys.clone(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
            DdlChange::AlterTable {
                name: child.to_string(),
                columns: final_columns,
                constraints: Vec::new(),
                foreign_keys,
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        ],
        ddl_lsn: vec![source_b, source_b],
        rows: vec![pre_row, parent_row, post_row],
        ..ChangeSet::default()
    };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: seed_context.tenant_id.clone(),
        source_node_id: seed_context.source_node_id.clone(),
        source_incarnation: seed_context.source_incarnation,
        entries: changes
            .ddl
            .iter()
            .enumerate()
            .map(|(ordinal, ddl)| {
                let table = Some(child.to_string());
                let table_generation = Some(1);
                let digest = crate::protocol::canonical_ddl_provenance_digest(
                    &crate::protocol::WireDdlChange::from(ddl.clone()),
                    source_b,
                    ordinal as u32,
                    table.as_deref(),
                    table_generation,
                )
                .unwrap();
                crate::protocol::ReceivedDdlEntry {
                    source_ddl_lsn: source_b,
                    ordinal: ordinal as u32,
                    table: table.clone(),
                    table_generation,
                    digest: digest.clone(),
                    author: Some(crate::protocol::WireDdlAuthor {
                        node_id: "retry-source".to_string(),
                        incarnation: Incarnation(11),
                        source_ddl_lsn: source_b,
                        ordinal: ordinal as u32,
                        table,
                        table_generation,
                        digest,
                        attestation: Vec::new(),
                        unit_statements: 2,
                    }),
                }
            })
            .collect(),
    };
    let lineages = changes
        .rows
        .iter()
        .zip([Lsn(501), Lsn(503), Lsn(504)])
        .map(|(row, author_position)| {
            (
                row.table.clone(),
                row.natural_key.clone(),
                row.lsn,
                crate::protocol::WireRowLineage {
                    author_node_id: "retry-source".to_string(),
                    author_database_incarnation: Incarnation(11),
                    author_local_mutation_position: author_position,
                    table_generation: 1,
                    lineage_root: format!(
                        "author:retry-source:{}:{}",
                        Incarnation(11).to_hex(),
                        author_position.0
                    ),
                    attestation: Vec::new(),
                },
            )
        })
        .collect::<Vec<_>>();
    let apply_complete =
        |db: &Database,
         unit: &ChangeSet,
         context: &crate::protocol::ReceivedDdlContext,
         lineage: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
         end: Lsn| {
            let memory = db.declared_reader_memory().unwrap();
            let mut copies = contextdb_engine::vector_read_memory::admission(memory.clone());
            db.commit_received_schema_stage_with_receipt_for_test(
                unit,
                context,
                lineage,
                &HashMap::new(),
                Some(SyncApplyReceipt {
                    tenant_id: context.tenant_id.clone(),
                    node_id: context.source_node_id.clone(),
                    incarnation: context.source_incarnation,
                    source_lsn: end,
                    dependency_complete: true,
                    hub_contact_ms: None,
                }),
                false,
                &mut copies,
                memory,
            )
        };
    let assert_visible = |db: &Database| {
        let actual = db
            .execute(
                "SELECT id, parent_id, previous_body, body FROM mixed_children ORDER BY id",
                &HashMap::new(),
            )
            .unwrap()
            .rows;
        assert_eq!(
            actual,
            vec![
                vec![
                    Value::Int64(1),
                    Value::Int64(7),
                    Value::Text(text_uuid.clone()),
                    Value::Null
                ],
                vec![
                    Value::Int64(2),
                    Value::Int64(7),
                    Value::Null,
                    Value::Uuid(later_uuid)
                ],
            ],
            "the pre-ALTER text is renamed; the post-ALTER value is UUID"
        );
        assert_eq!(
            db.execute("SELECT id FROM mixed_parents", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(7)]]
        );
    };
    let db = open_native_received_memory();
    apply_received(&db, &seed, &seed_context, &seed_lineages).unwrap();
    let original_limit = db.memory_limit();
    db.execute("CREATE TABLE mixed_unrelated (id INTEGER PRIMARY KEY, body TEXT)", &HashMap::new()).unwrap();
    db.execute("INSERT INTO mixed_unrelated VALUES (99, 'preserved')", &HashMap::new()).unwrap();
    db.execute("CREATE TRIGGER native_prefix_read ON mixed_children WHEN INSERT INCLUDING SYNC", &HashMap::new()).unwrap();
    let visits = Arc::new(Mutex::new(Vec::new()));
    let reject = Arc::new(std::sync::atomic::AtomicBool::new(
        matches!(case, NativeMemoryReplayCase::CallbackRefusal),
    ));
    db.register_transactional_trigger_callback("native_prefix_read", {
        let visits = visits.clone();
        let reject = reject.clone();
        move |handle, context| {
            let id = context.row_values["id"].clone();
            let body = context.row_values["body"].clone();
            // Read the ACTUAL current transaction, not just the supplied context.
            // A final-schema replay decodes A incorrectly or cannot read its old body.
            let selected = handle.execute_in_tx(
                context.tx, "SELECT body FROM mixed_children WHERE id = $id",
                &HashMap::from([("id".to_string(), id.clone())]),
            )?.rows;
            assert_eq!(selected, vec![vec![body.clone()]]);
            let parents = handle.execute_in_tx(
                context.tx, "SELECT id FROM mixed_parents ORDER BY id", &HashMap::new(),
            )?.rows;
            let expected = if id == Value::Int64(1) { Vec::new() }
                else { vec![vec![Value::Int64(7)]] };
            assert_eq!(parents, expected, "a callback sees its source prefix, not the final parent overlay");
            visits.lock().push((id.clone(), body, parents));
            if id == Value::Int64(2) && reject.load(Ordering::SeqCst) {
                return Err(Error::Other("native prefix callback refusal".to_string().into()));
            }
            Ok(())
        }
    }).unwrap();
    db.complete_initialization().unwrap();

    // Public transaction rollback must not add a commit or expose its staged row.
    let rollback_root = db.tx_mgr.published_read_snapshot();
    let rollback_commits = db.tx_mgr.commit_index_snapshot();
    let tx = db.begin().unwrap();
    db.execute_in_tx(tx, "INSERT INTO mixed_parents VALUES (999)", &HashMap::new()).unwrap();
    assert_eq!(db.execute_in_tx(tx, "SELECT id FROM mixed_parents", &HashMap::new()).unwrap().rows,
        vec![vec![Value::Int64(999)]]);
    db.rollback(tx).unwrap();
    assert_eq!(db.tx_mgr.published_read_snapshot(), rollback_root);
    assert_eq!(db.tx_mgr.commit_index_snapshot(), rollback_commits);
    assert!(db.execute("SELECT id FROM mixed_parents", &HashMap::new()).unwrap().rows.is_empty());

    if matches!(case, NativeMemoryReplayCase::GraphGuard) {
        changes.edges.push(EdgeChange {
            source: uuid::Uuid::from_u128(1), target: uuid::Uuid::from_u128(2),
            edge_type: "native_prefix_edge".to_string(), properties: HashMap::new(), lsn: source_a,
        });
        let live = db.relational_store.table_meta.read().clone();
        let mut projected = live.clone();
        for ddl in &changes.ddl { Database::apply_sync_table_ddl_to_projected_meta(&mut projected, ddl); }
        assert!(Database::private_source_order_supported(
            &changes, &live, &projected, db.declared_reader_memory().unwrap(),
        ).unwrap(), "Native graph prefix remains RED until the complete path supports graph changes");
    }
    let before = db.current_lsn();
    let commits_before = db.tx_mgr.commit_index_len();
    assert!(
        db.execute("SELECT id FROM mixed_parents", &HashMap::new())
            .unwrap()
            .rows
            .is_empty(),
        "the parent exists only later in this unit"
    );
    assert!(source_a < source_b && source_b < source_c);
    let root_before = db.tx_mgr.published_read_snapshot();
    let index_before = db.tx_mgr.commit_index_snapshot();
    let schema_before = format!("{:?}", db.table_meta(child));
    let publication_before = format!("{:?}", db.published_state_snapshot_for_test());
    let audits_before = format!("{:?}", db.trigger_audit_log());
    if matches!(case, NativeMemoryReplayCase::CallbackRefusal | NativeMemoryReplayCase::MemoryRefusal) {
        if matches!(case, NativeMemoryReplayCase::MemoryRefusal) { db.set_memory_limit(Some(1)).unwrap(); }
        let failed = apply_complete(&db, &changes, &received, &lineages, source_c);
        // Restore the original declaration before making any observation reads.
        db.set_memory_limit(original_limit).unwrap();
        let error = failed.expect_err("the whole Native received unit must refuse");
        match case {
            NativeMemoryReplayCase::CallbackRefusal => {
                assert!(error.to_string().contains("native prefix callback refusal"), "{error:?}");
                assert_eq!(visits.lock().len(), 2, "refuse only after both genuine source-prefix callbacks");
            }
            NativeMemoryReplayCase::MemoryRefusal => {
                assert!(matches!(error, Error::MemoryBudgetExceeded { .. }), "typed admission refusal: {error:?}");
                assert!(visits.lock().is_empty(), "no callback runs after failed admission");
            }
            _ => unreachable!(),
        }
        assert_eq!(db.current_lsn(), before);
        assert_eq!(db.tx_mgr.published_read_snapshot(), root_before);
        assert_eq!(db.tx_mgr.commit_index_snapshot(), index_before);
        assert_eq!(format!("{:?}", db.table_meta(child)), schema_before);
        assert_eq!(format!("{:?}", db.published_state_snapshot_for_test()), publication_before);
        assert_eq!(format!("{:?}", db.trigger_audit_log()), audits_before);
        assert!(db.execute("SELECT id FROM mixed_children", &HashMap::new()).unwrap().rows.is_empty());
        assert!(db.execute("SELECT id FROM mixed_parents", &HashMap::new()).unwrap().rows.is_empty());
        let refused_history = db.changes_since(before).unwrap();
        assert!(refused_history.rows.is_empty());
        assert!(refused_history.ddl.is_empty());
        assert!(refused_history.edges.is_empty());
        reject.store(false, Ordering::SeqCst);
        visits.lock().clear();
    }
    let observer = ReceivedSchemaApplyObserverForTest::new();
    let observation_scope = db.observe_received_schema_applies_for_test(&observer);
    let outcome = apply_complete(&db, &changes, &received, &lineages, source_c)
        .expect("one complete Native overlay accepts data A / ALTER B / parent data C");
    drop(observation_scope);
    let observed = observer.observation().expect("the actual received stage must attach its existing observer");
    assert_eq!(observed.staged_table_names,
        BTreeSet::from([parent.to_string(), child.to_string()]));
    assert_eq!(*visits.lock(), vec![
        (Value::Int64(1), Value::Text(text_uuid.clone()), Vec::new()),
        (Value::Int64(2), Value::Uuid(later_uuid), vec![vec![Value::Int64(7)]]),
    ], "one callback per source row, with its authored type and current prefix");
    assert_eq!(db.execute("SELECT id, body FROM mixed_unrelated", &HashMap::new()).unwrap().rows,
        vec![vec![Value::Int64(99), Value::Text("preserved".to_string())]]);
    assert_eq!(outcome.applied_rows, 3);
    assert_eq!(outcome.skipped_rows, 0);
    assert!(outcome.conflicts.is_empty());
    assert_eq!(
        db.tx_mgr.commit_index_len(),
        commits_before + 1,
        "multiple source positions publish exactly one receiver commit"
    );
    assert_eq!(db.current_lsn(), outcome.new_lsn, "one public END");
    assert!(outcome.new_lsn > before);
    assert_visible(&db);

    let memory = db.declared_reader_memory().unwrap();
    let mut copies = contextdb_engine::vector_read_memory::admission(memory.clone());
    let (outbound, provenance_source) = db
        .changes_since_base(before, &mut copies, memory.clone())
        .expect("read the exact accepted history for onward relay");
    assert_eq!(outbound.edges.len(), changes.edges.len());
    for (actual, expected) in outbound.edges.iter().zip(&changes.edges) {
        assert_eq!(actual.source, expected.source);
        assert_eq!(actual.target, expected.target);
        assert_eq!(actual.edge_type, expected.edge_type);
        assert_eq!(actual.properties, expected.properties);
    }
    assert_eq!(outbound.rows.len(), 3);
    assert_eq!(outbound.ddl, changes.ddl);
    let forwarded_pre = outbound
        .rows
        .iter()
        .find(|row| row.table == child && row.natural_key == key(1))
        .unwrap();
    let forwarded_parent = outbound
        .rows
        .iter()
        .find(|row| row.table == parent)
        .unwrap();
    for original in &changes.rows {
        let forwarded = outbound
            .rows
            .iter()
            .find(|row| row.table == original.table && row.natural_key == original.natural_key)
            .unwrap();
        assert_eq!(
            forwarded.values, original.values,
            "relay keeps the authored body"
        );
    }
    assert_eq!(
        serde_json::to_value(forwarded_pre.values.get("body").unwrap()).unwrap(),
        serde_json::json!({"Text": text_uuid}),
        "history retains the serialized Value::Text tag"
    );
    assert_eq!(outbound.ddl_lsn.len(), 2);
    assert!(forwarded_pre.lsn < outbound.ddl_lsn[0]);
    assert_eq!(outbound.ddl_lsn[0], outbound.ddl_lsn[1]);
    assert!(outbound.ddl_lsn[1] < forwarded_parent.lsn);
    assert!(
        forwarded_parent.lsn <= outcome.new_lsn,
        "interior replay positions lie inside the one published END"
    );
    let commit_positions = db.tx_mgr.commit_index_snapshot();
    assert_eq!(
        commit_positions
            .range((
                std::ops::Bound::Excluded(before),
                std::ops::Bound::Included(outcome.new_lsn)
            ))
            .map(|(lsn, _)| *lsn)
            .collect::<Vec<_>>(),
        vec![outcome.new_lsn]
    );
    let outgoing_lineages = db
        .outbound_row_lineages(
            &outbound,
            &received.tenant_id,
            "mixed-relay",
            Incarnation(22),
            &|_| Ok(Vec::new()),
            memory.clone(),
        )
        .unwrap();
    let forwarded_lineages = outbound
        .rows
        .iter()
        .map(|row| {
            let lineage = outgoing_lineages
                .get(&(
                    row.table.clone(),
                    rmp_serde::to_vec(&row.natural_key).unwrap(),
                    row.lsn,
                ))
                .unwrap();
            let original = lineages
                .iter()
                .find(|(table, natural_key, _, _)| {
                    table == &row.table && natural_key == &row.natural_key
                })
                .unwrap();
            assert_eq!(
                lineage, &original.3,
                "relay never substitutes its own author coordinate"
            );
            assert_ne!(lineage.author_local_mutation_position, row.lsn);
            assert_ne!(lineage.author_local_mutation_position, outcome.new_lsn);
            (
                row.table.clone(),
                row.natural_key.clone(),
                row.lsn,
                lineage.clone(),
            )
        })
        .collect::<Vec<_>>();
    let outgoing_ddl = db
        .outbound_ddl_provenance(
            &outbound,
            &provenance_source,
            &received.tenant_id,
            "mixed-relay",
            Incarnation(22),
            &|_| Ok(Vec::new()),
            memory,
        )
        .unwrap();
    let relay_context = crate::protocol::ReceivedDdlContext {
        tenant_id: received.tenant_id.clone(),
        source_node_id: "mixed-relay".to_string(),
        source_incarnation: Incarnation(22),
        entries: outgoing_ddl
            .into_iter()
            .enumerate()
            .map(|(ordinal, entry)| {
                let author = entry
                    .author
                    .as_ref()
                    .expect("the original DDL author survives");
                assert_eq!(author.node_id, "retry-source");
                assert_eq!(author.incarnation, Incarnation(11));
                assert_eq!(author.source_ddl_lsn, source_b);
                assert_eq!(author.ordinal, ordinal as u32);
                assert_eq!(author.digest, received.entries[ordinal].digest);
                crate::protocol::ReceivedDdlEntry {
                    source_ddl_lsn: entry.source_ddl_lsn,
                    ordinal: entry.ordinal,
                    table: entry.table,
                    table_generation: entry.table_generation,
                    digest: entry.digest,
                    author: entry.author,
                }
            })
            .collect(),
    };
    let second = open_native_received_memory();
    apply_received(&second, &seed, &seed_context, &seed_lineages).unwrap();
    let second_commits = second.tx_mgr.commit_index_len();
    let second_outcome = apply_complete(
        &second,
        &outbound,
        &relay_context,
        &forwarded_lineages,
        outcome.new_lsn,
    )
    .expect("the second receiver replays the same unit");
    assert_eq!(second_outcome.applied_rows, 3);
    assert!(second_outcome.conflicts.is_empty());
    assert_eq!(second.tx_mgr.commit_index_len(), second_commits + 1);
    assert_eq!(second.current_lsn(), second_outcome.new_lsn);
    assert_visible(&second);

    assert_eq!(db.memory_limit(), original_limit, "restore exactly the declared budget");
    assert_eq!(second.memory_limit(), original_limit);
    second.close().unwrap();
    db.close().unwrap();
}

// Owner contract: contextdb-intent, Policy 16, clarification 2026-10-18.
// Rows below extend the received-schema fixture; existing bodies stay untouched.
#[cfg(test)]
mod edge_column_owner_contract {
    use super::*;

    const EDGE_COLUMNS: &str =
        "id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, note TEXT";
    const SOURCE: uuid::Uuid = uuid::Uuid::from_u128(0x77501);
    const TARGET: uuid::Uuid = uuid::Uuid::from_u128(0x77502);

    fn sql(db: &Database, query: &str) -> Vec<Vec<Value>> {
        db.execute(query, &HashMap::new())
            .unwrap_or_else(|e| panic!("{query}: {e:?}"))
            .rows
    }

    fn open(path: &std::path::Path) -> Database {
        let db = Database::open_owned_for_test(
            path,
            DatabaseOpenOptions {
                memory_limit: Some(1_610_612_736),
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap();
        assert!(
            matches!(&db.tx_mgr, DatabaseTransactionHost::Owned(_)),
            "exercise the real Native host"
        );
        db
    }

    fn insert(db: &Database, table: &str, id: i64) {
        db.execute(&format!("INSERT INTO {table} (id, source_id, target_id, edge_type, note) VALUES ($id, $s, $t, 'LINK', 'kept')"),
            &HashMap::from([("id".into(), Value::Int64(id)), ("s".into(), Value::Uuid(SOURCE)),
                ("t".into(), Value::Uuid(TARGET))])).unwrap();
    }

    fn seed(db: &Database, dag: bool) {
        sql(
            db,
            &format!(
                "CREATE TABLE links ({EDGE_COLUMNS}) {}",
                if dag { "DAG('LINK')" } else { "" }
            ),
        );
        sql(db, &format!("CREATE TABLE witnesses ({EDGE_COLUMNS})"));
        insert(db, "links", 1);
        insert(db, "witnesses", 2);
        let tx = db.begin().unwrap();
        assert!(
            db.insert_edge(tx, SOURCE, TARGET, "LINK".into(), HashMap::new())
                .unwrap()
        );
        db.commit(tx).unwrap();
        assert_links(db, 3);
    }

    // Use a surviving edge-shaped table to ask the global graph after links
    // loses its shape or is dropped. A missing-table error cannot mask a ghost.
    fn assert_links(db: &Database, count: usize) {
        let actual = db.execute("SELECT t FROM GRAPH_TABLE(witnesses MATCH (a)-[:LINK]->(b) WHERE a.id = $s COLUMNS (b.id AS t))",
            &HashMap::from([("s".into(), Value::Uuid(SOURCE))])).unwrap().rows;
        assert_eq!(
            actual,
            vec![vec![Value::Uuid(TARGET)]; count],
            "exact forward bindings including independent owners"
        );
        let reverse = db.execute("SELECT s FROM GRAPH_TABLE(witnesses MATCH (a)-[:LINK]->(b) WHERE b.id = $t COLUMNS (a.id AS s))",
            &HashMap::from([("t".into(), Value::Uuid(TARGET))])).unwrap().rows;
        assert_eq!(
            reverse,
            vec![vec![Value::Uuid(SOURCE)]; count],
            "both adjacency directions agree"
        );
        assert_eq!(
            sql(db, "SELECT id, note FROM witnesses"),
            vec![vec![Value::Int64(2), Value::Text("kept".into())]]
        );
    }

    fn state(
        db: &Database,
    ) -> (
        String,
        String,
        String,
        String,
        Lsn,
        String,
        String,
        String,
        String,
    ) {
        let mut config = db.persistence.as_ref().unwrap().dump_config_raw().unwrap();
        config.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        (
            format!("{:?}", db.table_meta("links")),
            format!("{:?}", sql(db, "SELECT * FROM links")),
            format!("{:?}", db.changes_since(Lsn(0)).unwrap()),
            format!("{config:?}"),
            db.current_lsn(),
            format!("{:?}", db.tx_mgr.commit_index_snapshot()),
            format!("{:?}", db.tx_mgr.published_read_snapshot()),
            format!("{:?}", db.received_ddl_arrivals.read()),
            format!("{:?}", db.in_memory_applied_push_watermarks.lock()),
        )
    }

    fn alter(column: &str, rename: bool, cascade: bool) -> String {
        let query = if rename {
            format!("ALTER TABLE links RENAME COLUMN {column} TO retired_{column}")
        } else {
            format!(
                "ALTER TABLE links DROP COLUMN {column}{}",
                if cascade { " CASCADE" } else { "" }
            )
        };
        contextdb_parser::parse(&query).expect("a refusal must exercise valid public DDL syntax");
        query
    }

    fn reopen(db: Database, path: &std::path::Path) -> Database {
        db.close().unwrap();
        drop(db);
        open(path)
    }

    // A DAG promises its defining shape remains usable; every syntax row shares
    // one atomicity oracle. Mutant: allow defining DROP only when CASCADE is set.
    #[test]
    fn dag_defining_column_changes_refuse_without_any_publication() {
        let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dag.db");
        let db = open(&path);
        seed(&db, true);
        let before = state(&db);
        for column in ["source_id", "target_id", "edge_type"] {
            for (rename, cascade) in [(false, false), (false, true), (true, false)] {
                let query = alter(column, rename, cascade);
                db.execute(&query, &HashMap::new())
                    .expect_err("dependent DAG columns must refuse, including CASCADE");
                assert_eq!(
                    state(&db),
                    before,
                    "no schema, row, marker, receipt, root or commit prefix: {query}"
                );
                assert_links(&db, 3);
            }
        }
        // A multi-statement authored transaction must roll back its earlier DDL too.
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE links RENAME COLUMN note TO annotation",
            &HashMap::new(),
        )
        .unwrap();
        let refused = db.execute_in_tx(
            tx,
            "ALTER TABLE links DROP COLUMN source_id CASCADE",
            &HashMap::new(),
        );
        assert!(
            refused.is_err(),
            "DAG dependency also holds inside a schema transaction"
        );
        db.rollback(tx).unwrap();
        assert_eq!(state(&db), before);
        let db = reopen(db, &path);
        assert_eq!(state(&db), before);
        assert_links(&db, 3);
    }

    // Non-defining changes keep every owner; losing the ordinary shape retires
    // only this table's row owner. Mutants: retire by type, or retain all owners.
    #[test]
    fn edge_column_changes_keep_or_retire_exact_owners_through_cleanup_and_restore() {
        let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
        for dag in [false, true] {
            for column in ["note", "source_id", "target_id", "edge_type"] {
                if dag && column != "note" {
                    continue;
                }
                for rename in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("owners.db");
                    let db = open(&path);
                    seed(&db, dag);
                    let query = alter(column, rename, false);
                    // Staged shape loss is invisible outside, and rollback restores
                    // the defining columns and all links without a marker.
                    let before = state(&db);
                    let tx = db.begin().unwrap();
                    db.execute_in_tx(tx, &query, &HashMap::new()).unwrap();
                    assert_links(&db, 3);
                    db.rollback(tx).unwrap();
                    assert_eq!(state(&db), before);
                    sql(&db, &query);
                    let expected = if column == "note" { 3 } else { 2 };
                    assert_links(&db, expected);
                    assert_eq!(
                        sql(&db, "SELECT id FROM links"),
                        vec![vec![Value::Int64(1)]]
                    );
                    let visible_column = if rename {
                        format!("retired_{column}")
                    } else {
                        "id".to_string()
                    };
                    let value = if !rename {
                        Value::Int64(1)
                    } else {
                        match column {
                            "source_id" => Value::Uuid(SOURCE),
                            "target_id" => Value::Uuid(TARGET),
                            "edge_type" => Value::Text("LINK".into()),
                            _ => Value::Text("kept".into()),
                        }
                    };
                    assert_eq!(
                        sql(&db, &format!("SELECT {visible_column} FROM links")),
                        vec![vec![value]]
                    );
                    if !rename {
                        assert!(
                            db.execute(&format!("SELECT {column} FROM links"), &HashMap::new())
                                .is_err()
                        );
                    }
                    let exported = dir.path().join("restored.db");
                    db.export_snapshot(&exported).unwrap();
                    let restored = open(&exported);
                    assert_links(&restored, expected);
                    restored.close().unwrap();
                    drop(restored);
                    let db = reopen(db, &path);
                    assert_links(&db, expected);
                    // All three cleanup paths run against independently restored
                    // copies of the post-migration state, including shape loss.
                    for cleanup in [
                        "DELETE FROM links WHERE id = 1",
                        "DROP TABLE links",
                        "PURGE FROM links WHERE id = 1",
                    ] {
                        let cleanup_path = dir.path().join(format!(
                            "cleanup-{}.db",
                            cleanup.split_whitespace().next().unwrap()
                        ));
                        db.export_snapshot(&cleanup_path).unwrap();
                        let cleaned = open(&cleanup_path);
                        sql(&cleaned, cleanup);
                        assert_links(&cleaned, 2);
                        let cleaned = reopen(cleaned, &cleanup_path);
                        assert_links(&cleaned, 2);
                        cleaned.close().unwrap();
                    }
                    // Remove the two independent owners: zero proves the retired
                    // row owner was not merely hidden by their overlapping triad.
                    sql(&db, "DELETE FROM witnesses WHERE id = 2");
                    let tx = db.begin().unwrap();
                    db.delete_edge(tx, SOURCE, TARGET, "LINK").unwrap();
                    db.commit(tx).unwrap();
                    let actual = db.execute("SELECT t FROM GRAPH_TABLE(witnesses MATCH (a)-[:LINK]->(b) WHERE a.id = $s COLUMNS (b.id AS t))",
                        &HashMap::from([("s".into(), Value::Uuid(SOURCE))])).unwrap().rows;
                    assert_eq!(
                        actual,
                        if column == "note" {
                            vec![vec![Value::Uuid(TARGET)]]
                        } else {
                            Vec::new()
                        }
                    );
                    db.close().unwrap();
                }
            }
        }
    }

    fn edge_ddl(column: &str, rename: bool, dag: bool) -> DdlChange {
        let mut columns: Vec<(String, String)> = vec![
            ("id".into(), "INTEGER PRIMARY KEY".into()),
            ("source_id".into(), "UUID".into()),
            ("target_id".into(), "UUID".into()),
            ("edge_type".into(), "TEXT".into()),
            ("note".into(), "TEXT".into()),
        ];
        if rename {
            columns
                .iter_mut()
                .find(|(name, _)| name == column)
                .unwrap()
                .0 = format!("retired_{column}");
        } else {
            columns.retain(|(name, _)| name != column);
        }
        let mut constraints = vec![if rename {
            sync_column_rename_constraint(column, &format!("retired_{column}"))
        } else {
            sync_column_drop_constraint(column)
        }];
        if dag {
            constraints.push("DAG('LINK')".into());
        }
        DdlChange::AlterTable {
            name: "links".into(),
            columns,
            constraints,
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }
    }

    // The existing canonical-digest fixture represents transport-validated input;
    // this exercises the real received stage, not signature validation or a policy predicate.
    #[test]
    fn received_edge_column_changes_are_atomic_and_keep_source_identity() {
        let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
        for dag in [false, true] {
            for column in ["note", "source_id", "target_id", "edge_type"] {
                for rename in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("received.db");
                    let db = open(&path);
                    seed(&db, dag);
                    let (mut changes, received, mut lineages) =
                        received_migration(vec![edge_ddl(column, rename, dag)], false);
                    // Same exact UUID endpoints on either side of the authored
                    // DDL position. A final-schema-only decoder cannot silently
                    // discard A's old column or resurrect its retired graph owner.
                    for (id, position) in [(3, Lsn(89)), (4, Lsn(91))] {
                        let mut values = HashMap::from([
                            ("id".into(), Value::Int64(id)),
                            ("source_id".into(), Value::Uuid(SOURCE)),
                            ("target_id".into(), Value::Uuid(TARGET)),
                            ("edge_type".into(), Value::Text("LINK".into())),
                            ("note".into(), Value::Text("kept".into())),
                        ]);
                        if id == 4 {
                            let value = values.remove(column).unwrap();
                            if rename {
                                values.insert(format!("retired_{column}"), value);
                            }
                        }
                        let key = NaturalKey::single("id".into(), Value::Int64(id));
                        changes.rows.push(RowChange {
                            table: "links".into(),
                            natural_key: key.clone(),
                            values,
                            deleted: false,
                            lsn: position,
                            created_at: None,
                        });
                        let author_position = Lsn(800 + id as u64);
                        lineages.push((
                            "links".into(),
                            key,
                            position,
                            crate::protocol::WireRowLineage {
                                author_node_id: "retry-source".into(),
                                author_database_incarnation: Incarnation(11),
                                author_local_mutation_position: author_position,
                                table_generation: 1,
                                lineage_root: format!(
                                    "author:retry-source:{}:{}",
                                    Incarnation(11).to_hex(),
                                    author_position.0
                                ),
                                attestation: Vec::new(),
                            },
                        ));
                    }
                    let apply_unit = || {
                        let memory = db.declared_reader_memory().unwrap();
                        let mut copies =
                            contextdb_engine::vector_read_memory::admission(memory.clone());
                        let mut end = receipt();
                        end.source_lsn = Lsn(91);
                        db.commit_received_schema_stage_with_receipt_for_test(
                            &changes,
                            &received,
                            &lineages,
                            &HashMap::new(),
                            Some(end),
                            false,
                            &mut copies,
                            memory,
                        )
                    };
                    let before = state(&db);
                    if !dag || column == "note" {
                        // Existing persistence fault fires after preparing the whole
                        // received image, before any durable publication.
                        crate::persistence::arm_received_schema_pre_commit_fault_for_test();
                        let failure = apply_unit().expect_err(
                            "pre-commit storage refusal publishes no retirement or marker",
                        );
                        assert!(
                            failure
                                .to_string()
                                .contains("injected received-schema Redb pre-commit failure"),
                            "{failure:?}"
                        );
                        assert_eq!(state(&db), before);
                        assert_links(&db, 3);
                    }
                    let result = apply_unit();
                    if dag && column != "note" {
                        result.expect_err("received DAG dependency refuses the whole migration");
                        assert_eq!(state(&db), before);
                        assert_links(&db, 3);
                    } else {
                        let outcome = result.expect("authenticated column migration");
                        assert_eq!(outcome.applied_rows, 2);
                        assert_links(&db, if column == "note" { 5 } else { 2 });
                        assert_eq!(
                            sql(&db, "SELECT id FROM links ORDER BY id"),
                            vec![
                                vec![Value::Int64(1)],
                                vec![Value::Int64(3)],
                                vec![Value::Int64(4)]
                            ]
                        );
                        let history = db.changes_since(before.4).unwrap();
                        assert_eq!(
                            history.ddl, changes.ddl,
                            "exact signed DROP/RENAME marker survives"
                        );
                        let endpoint = if rename && column == "source_id" {
                            "retired_source_id"
                        } else {
                            "source_id"
                        };
                        if rename || column != "source_id" {
                            assert_eq!(
                                sql(&db, &format!("SELECT {endpoint} FROM links ORDER BY id")),
                                vec![vec![Value::Uuid(SOURCE)]; 3],
                                "source UUID survives the authored prefix transform"
                            );
                        }
                        let identities = db
                            .outbound_row_lineages(
                                &history,
                                &received.tenant_id,
                                "receiver",
                                Incarnation(22),
                                &|_| Ok(Vec::new()),
                                db.declared_reader_memory().unwrap(),
                            )
                            .unwrap();
                        for row in &history.rows {
                            let original = lineages
                                .iter()
                                .find(|(_, key, _, _)| key == &row.natural_key)
                                .unwrap();
                            assert_eq!(
                                identities
                                    .get(&(
                                        row.table.clone(),
                                        rmp_serde::to_vec(&row.natural_key).unwrap(),
                                        row.lsn
                                    ))
                                    .unwrap(),
                                &original.3
                            );
                        }
                        let accepted = state(&db);
                        apply_unit().unwrap();
                        assert_eq!(
                            state(&db),
                            accepted,
                            "same source identity retries without new ownership or markers"
                        );
                    }
                    let expected = if column == "note" {
                        5
                    } else if dag {
                        3
                    } else {
                        2
                    };
                    let exported = dir.path().join("received-restored.db");
                    db.export_snapshot(&exported).unwrap();
                    let restored = open(&exported);
                    assert_links(&restored, expected);
                    restored.close().unwrap();
                    drop(restored);
                    let db = reopen(db, &path);
                    assert_links(&db, expected);
                    db.close().unwrap();
                }
            }
        }
    }

    // Source A reads the old name; source C reads the renamed name, while the
    // unrelated edge table remains untouched. Final-result equality cannot prove
    // route selection: the callback inspects its actual active replay owner.
    #[test]
    fn unrelated_received_migration_uses_private_prefix_route_with_untouched_edges() {
        let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let db = open_native_received_memory();
        seed(&db, false);
        sql(
            &db,
            "CREATE TABLE prefix_notes (id INTEGER PRIMARY KEY, body TEXT)",
        );
        sql(
            &db,
            "CREATE TRIGGER prefix_probe ON prefix_notes WHEN INSERT INCLUDING SYNC",
        );
        let visits = Arc::new(Mutex::new(Vec::new()));
        let reject = Arc::new(std::sync::atomic::AtomicBool::new(true));
        db.register_transactional_trigger_callback("prefix_probe", {
            let visits = visits.clone();
            let reject = reject.clone();
            move |handle, context| {
                let id = context.row_values["id"].clone();
                let name = if id == Value::Int64(1) {
                    "body"
                } else {
                    "previous_body"
                };
                let actual = handle
                    .execute_in_tx(
                        context.tx,
                        &format!("SELECT {name} FROM prefix_notes WHERE id = $id"),
                        &HashMap::from([("id".into(), id.clone())]),
                    )?
                    .rows;
                assert_eq!(
                    actual,
                    vec![vec![Value::Text("prefix".into())]],
                    "callback reads the actual source schema"
                );
                assert!(
                    handle.private_replay_owner(context.tx).is_some(),
                    "an untouched edge table must not force the complete-image fallback route"
                );
                visits.lock().push(id.clone());
                if id == Value::Int64(2) && reject.load(Ordering::SeqCst) {
                    return Err(Error::Other("edge prefix refusal".to_string().into()));
                }
                Ok(())
            }
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let ddl = DdlChange::AlterTable {
            name: "prefix_notes".into(),
            columns: vec![
                ("id".into(), "INTEGER PRIMARY KEY".into()),
                ("previous_body".into(), "TEXT".into()),
            ],
            constraints: vec![sync_column_rename_constraint("body", "previous_body")],
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        };
        let (mut changes, received, _) = received_migration(vec![ddl], false);
        for (id, lsn, name) in [(1, Lsn(89), "body"), (2, Lsn(91), "previous_body")] {
            changes.rows.push(RowChange {
                table: "prefix_notes".into(),
                natural_key: NaturalKey::single("id".into(), Value::Int64(id)),
                values: HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    (name.into(), Value::Text("prefix".into())),
                ]),
                deleted: false,
                lsn,
                created_at: None,
            });
        }
        let lineages = changes
            .rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let position = Lsn(701 + i as u64);
                (
                    row.table.clone(),
                    row.natural_key.clone(),
                    row.lsn,
                    crate::protocol::WireRowLineage {
                        author_node_id: "retry-source".into(),
                        author_database_incarnation: Incarnation(11),
                        author_local_mutation_position: position,
                        table_generation: 1,
                        lineage_root: format!(
                            "author:retry-source:{}:{}",
                            Incarnation(11).to_hex(),
                            position.0
                        ),
                        attestation: Vec::new(),
                    },
                )
            })
            .collect::<Vec<_>>();
        let before = (
            format!("{:?}", db.table_meta("prefix_notes")),
            db.current_lsn(),
            db.tx_mgr.commit_index_snapshot(),
            format!("{:?}", db.in_memory_applied_push_watermarks.lock()),
            format!("{:?}", db.trigger_audit_log()),
        );
        let apply = || {
            let memory = db.declared_reader_memory().unwrap();
            let mut copies = contextdb_engine::vector_read_memory::admission(memory.clone());
            let mut end = receipt();
            end.source_lsn = Lsn(91);
            db.commit_received_schema_stage_with_receipt_for_test(
                &changes,
                &received,
                &lineages,
                &HashMap::new(),
                Some(end),
                false,
                &mut copies,
                memory,
            )
        };
        let error = apply().expect_err("refuse after source C has read its real prefix");
        assert!(
            error.to_string().contains("edge prefix refusal"),
            "{error:?}"
        );
        assert_eq!(*visits.lock(), vec![Value::Int64(1), Value::Int64(2)]);
        assert_eq!(
            (
                format!("{:?}", db.table_meta("prefix_notes")),
                db.current_lsn(),
                db.tx_mgr.commit_index_snapshot(),
                format!("{:?}", db.in_memory_applied_push_watermarks.lock()),
                format!("{:?}", db.trigger_audit_log())
            ),
            before
        );
        assert!(sql(&db, "SELECT id FROM prefix_notes").is_empty());
        assert_links(&db, 3);
        let refused = db.changes_since(before.1).unwrap();
        assert!(refused.ddl.is_empty() && refused.rows.is_empty() && refused.edges.is_empty());
        reject.store(false, Ordering::SeqCst);
        visits.lock().clear();
        let observer = ReceivedSchemaApplyObserverForTest::new();
        let scope = db.observe_received_schema_applies_for_test(&observer);
        let outcome = apply().unwrap();
        drop(scope);
        let observed = observer
            .observation()
            .expect("actual received-stage observation");
        assert_eq!(
            observed.staged_table_names,
            BTreeSet::from(["prefix_notes".to_string()])
        );
        assert_eq!(
            observed.staged_graph_edges_copied, 0,
            "untouched graph is not copied"
        );
        assert_eq!(*visits.lock(), vec![Value::Int64(1), Value::Int64(2)]);
        assert_eq!(
            sql(
                &db,
                "SELECT id, previous_body FROM prefix_notes ORDER BY id"
            ),
            vec![
                vec![Value::Int64(1), Value::Text("prefix".into())],
                vec![Value::Int64(2), Value::Text("prefix".into())]
            ]
        );
        assert_eq!(db.tx_mgr.commit_index_len(), before.2.len() + 1);
        assert_eq!(db.current_lsn(), outcome.new_lsn);
        assert_links(&db, 3);
        let memory = db.declared_reader_memory().unwrap();
        let history = db.changes_since(before.1).unwrap();
        assert_eq!(history.ddl, changes.ddl);
        assert!(
            history.rows[0].lsn < history.ddl_lsn[0] && history.ddl_lsn[0] < history.rows[1].lsn
        );
        let identities = db
            .outbound_row_lineages(
                &history,
                &received.tenant_id,
                "receiver",
                Incarnation(22),
                &|_| Ok(Vec::new()),
                memory,
            )
            .unwrap();
        for row in &history.rows {
            let original = lineages
                .iter()
                .find(|(_, key, _, _)| key == &row.natural_key)
                .unwrap();
            assert_eq!(
                identities
                    .get(&(
                        row.table.clone(),
                        rmp_serde::to_vec(&row.natural_key).unwrap(),
                        row.lsn
                    ))
                    .unwrap(),
                &original.3
            );
        }
    }
}
