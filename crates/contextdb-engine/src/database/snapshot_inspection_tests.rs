use super::*;

fn params(values: &[(&str, Value)]) -> HashMap<String, Value> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect()
}

#[test]
fn snapshot_inspection_reads_physical_history_without_touching_the_artifact() {
    let root = tempfile::tempdir().expect("tempdir");
    let database_path = root.path().join("source.redb");
    let artifact_path = root.path().join("source.snapshot");
    let db = Database::open(&database_path).expect("open source");
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
        &HashMap::new(),
    )
    .expect("declare retained-history table");
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params(&[
            ("id", Value::Int64(7)),
            ("body", Value::Text("first".to_string())),
        ]),
    )
    .expect("insert first version");
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &params(&[
            ("id", Value::Int64(7)),
            ("body", Value::Text("second".to_string())),
        ]),
    )
    .expect("insert second version");
    db.export_snapshot(&artifact_path).expect("export snapshot");
    db.close().expect("close source");

    let artifact_before = std::fs::read(&artifact_path).expect("read artifact before inspection");
    let inspector = SnapshotInspector::open(&artifact_path).expect("open private copy");
    let report = inspector
        .inspect_key(
            "notes",
            &NaturalKey::single("id".to_string(), Value::Int64(7)),
            &["body".to_string()],
        )
        .expect("inspect key");
    assert_eq!(report.total_retained_versions, 2);
    assert!(!report.versions_truncated);
    let bodies = report
        .retained_versions
        .iter()
        .map(|version| {
            version
                .values
                .get("body")
                .and_then(|inspection| inspection.value.as_ref())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        bodies,
        vec![
            Some(&Value::Text("first".to_string())),
            Some(&Value::Text("second".to_string())),
        ]
    );
    inspector.close().expect("close inspection copy");
    assert_eq!(
        std::fs::read(&artifact_path).expect("read artifact after inspection"),
        artifact_before,
        "inspection must never alter the supplied snapshot artifact"
    );
}

#[test]
fn snapshot_inspection_omits_large_values_and_reports_blob_state_without_bytes() {
    let root = tempfile::tempdir().expect("tempdir");
    let database_path = root.path().join("source.redb");
    let artifact_path = root.path().join("source.snapshot");
    let db = Database::open(&database_path).expect("open source");
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("declare table");
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params(&[
            ("id", Value::Int64(9)),
            ("body", Value::Text("x".repeat(40 * 1024))),
        ]),
    )
    .expect("insert oversized value");
    let payload = b"engine-held-distributed-media";
    let hash: [u8; 32] = blake3::hash(payload).into();
    db.blob_repository()
        .install_complete_fixture_for_test(hash, payload, db.current_lsn().0)
        .expect("install complete blob through repository fixture");
    db.export_snapshot(&artifact_path).expect("export snapshot");
    db.close().expect("close source");

    let inspector = SnapshotInspector::open(&artifact_path).expect("open private copy");
    let key = inspector
        .inspect_key(
            "notes",
            &NaturalKey::single("id".to_string(), Value::Int64(9)),
            &["body".to_string()],
        )
        .expect("inspect bounded key");
    let body = key.retained_versions[0]
        .values
        .get("body")
        .expect("body inspection");
    assert!(body.omitted);
    assert_eq!(body.value, None);
    assert_eq!(
        body.omission_reason.as_deref(),
        Some("value_exceeds_per_value_bound")
    );
    assert_eq!(body.source_units, Some(40 * 1024));

    let blob = inspector.inspect_blob(hash).expect("inspect blob");
    assert_eq!(blob.active_generation, Some(1));
    assert_eq!(blob.last_purge_lsn, 0);
    assert_eq!(
        blob.manifest.as_ref().map(|manifest| manifest.state),
        Some(BlobManifestStateInspection::Complete)
    );
    assert_eq!(
        blob.manifest.as_ref().map(|manifest| manifest.total_size),
        Some(payload.len() as u64)
    );
    assert!(blob.tag_roles.servable);
    assert!(!blob.tag_roles.fetch_protection);
    inspector.close().expect("close inspection copy");
}

#[test]
fn sync_apply_state_digest_is_deterministic_and_changes_with_durable_state() {
    let root = tempfile::tempdir().expect("tempdir");
    let database_path = root.path().join("source.redb");
    let before_path = root.path().join("before.snapshot");
    let after_path = root.path().join("after.snapshot");
    let db = Database::open(&database_path).expect("open source");
    db.export_snapshot(&before_path)
        .expect("export empty state");
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
        &HashMap::new(),
    )
    .expect("declare table");
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params(&[
            ("id", Value::Int64(1)),
            ("body", Value::Text("durable".to_string())),
        ]),
    )
    .expect("insert row");
    db.export_snapshot(&after_path)
        .expect("export populated state");
    db.close().expect("close source");

    let before = SnapshotInspector::open(&before_path)
        .expect("open before")
        .inspect_sync_apply_state()
        .expect("inspect before");
    let first = SnapshotInspector::open(&after_path)
        .expect("open first copy")
        .inspect_sync_apply_state()
        .expect("inspect first copy");
    let second = SnapshotInspector::open(&after_path)
        .expect("open second copy")
        .inspect_sync_apply_state()
        .expect("inspect second copy");

    assert_ne!(before.digest, first.digest);
    assert_eq!(
        first, second,
        "separate copies must canonicalize identically"
    );
    assert_eq!(first.tables, 1);
    assert_eq!(first.retained_row_versions, 1);
    assert_eq!(first.ddl_log_entries, 1);
    assert_eq!(first.change_log_entries, 1);
}

#[test]
fn snapshot_export_preserves_trigger_audit_retention_stamps_exactly() {
    let root = tempfile::tempdir().expect("tempdir");
    let database_path = root.path().join("source.redb");
    let artifact_path = root.path().join("source.snapshot");
    let db = Database::open(&database_path).expect("open source");
    db.execute(
        "CREATE TABLE observations (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("declare trigger table");
    db.execute(
        "CREATE TRIGGER observation_audit ON observations WHEN INSERT",
        &HashMap::new(),
    )
    .expect("declare trigger");
    db.register_trigger_callback("observation_audit", |_db, _context| Ok(()))
        .expect("register trigger callback");
    db.execute(
        "INSERT INTO observations (id, body) VALUES (1, 'captured')",
        &HashMap::new(),
    )
    .expect("fire trigger");

    let source_stamps = db
        .persistence
        .as_ref()
        .expect("file-backed source persistence")
        .dump_trigger_audit_stamps_raw()
        .expect("read source trigger-audit stamps");
    assert_eq!(source_stamps.len(), 1, "fixture writes one retention stamp");
    db.export_snapshot(&artifact_path).expect("export snapshot");

    let inspector = SnapshotInspector::open(&artifact_path).expect("open snapshot copy");
    let artifact_stamps = inspector
        .database
        .persistence
        .as_ref()
        .expect("file-backed artifact persistence")
        .dump_trigger_audit_stamps_raw()
        .expect("read artifact trigger-audit stamps");
    assert_eq!(
        artifact_stamps, source_stamps,
        "snapshot export must preserve every trigger-audit retention stamp exactly"
    );
    inspector.close().expect("close snapshot copy");
    db.close().expect("close source");
}
