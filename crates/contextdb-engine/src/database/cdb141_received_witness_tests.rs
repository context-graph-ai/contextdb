use super::*;
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

#[test]
fn received_publication_abort_retry_and_reopen_preserve_history_witnesses() {
    let (changes, received, lineages) = received_migration(ddl_vector(), true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("received-witness.db");
    let db = Database::open(&path).unwrap();
    crate::persistence::arm_received_schema_pre_commit_fault_for_test();
    assert!(apply_received(&db, &changes, &received, &lineages).is_err());
    db.close().unwrap();
    crate::persistence::cdb141_integrity_revision_tests::assert_all_history_witnesses(&path);
    let db = Database::open(&path).unwrap();
    let original = apply_received(&db, &changes, &received, &lineages).unwrap();
    db.close().unwrap();
    crate::persistence::cdb141_integrity_revision_tests::assert_all_history_witnesses(&path);
    let db = Database::open(&path).unwrap();
    assert_eq!(
        apply_received(&db, &changes, &received, &lineages).unwrap(),
        original
    );
    db.close().unwrap();
    crate::persistence::cdb141_integrity_revision_tests::assert_all_history_witnesses(&path);
}
