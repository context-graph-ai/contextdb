#![cfg(feature = "test-seams")]

use super::*;
use crate::work_ledger::{BlobHash, InputRef, JobSpec, install_work_ledger_schema, submit_job};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

const COMPLETE_BLOB_ROLES: [&str; 5] = [
    "canonical_manifest",
    "payload_chunks",
    "outboard_chunks",
    "servable_tag",
    "hash_generation_fence",
];

const PARTIAL_BLOB_ROLES: [&str; 6] = [
    "canonical_manifest",
    "payload_chunks",
    "outboard_chunks",
    "partial_bitfield",
    "fetch_protection",
    "hash_generation_fence",
];

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_ledger() -> (tempfile::TempDir, Database) {
    let root = tempfile::tempdir().expect("create engine-held blob purge fixture directory");
    let db = Database::open(root.path().join("engine-held-blobs.redb"))
        .expect("open file-backed engine-held blob fixture");
    install_work_ledger_schema(&db).expect("install work-ledger schema");
    (root, db)
}

fn reopen_ledger(db: Database, path: &Path) -> Database {
    db.close()
        .expect("flush file-backed engine-held blob database");
    drop(db);
    Database::open(path).expect("reopen file-backed engine-held blob database")
}

fn submit_blob_job(db: &Database, job_id: &str, hash: &BlobHash) {
    let job = JobSpec::builder(job_id, "media.purge", "input", "hub-node")
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(1)
        .build();
    submit_job(db, &job, &[] as &[&[u8]]).expect("submit blob-referencing work job");
}

fn work_job_ids(db: &Database) -> Vec<Vec<Value>> {
    db.execute("SELECT job_id FROM work_jobs ORDER BY job_id", &params())
        .expect("read surviving work jobs")
        .rows
}

fn exact_blob_roles(db: &Database, hash: &BlobHash) -> BTreeSet<String> {
    db.authoritative_purge_blob_roles_for_test(&hash.as_bytes())
}

fn expected_roles(roles: &[&str]) -> BTreeSet<String> {
    roles.iter().map(|role| (*role).to_string()).collect()
}

#[test]
fn incoming_purge_atomically_destroys_present_rows_and_adopts_absent_roots() {
    let root = tempfile::tempdir().expect("create sparse purge edge directory");
    let path = root.path().join("sparse-purge-edge.redb");
    let db = Database::open(&path).expect("open sparse purge edge");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        &params(),
    )
    .expect("create sparse edge table");
    let present_id = uuid::Uuid::new_v4();
    let absent_id = uuid::Uuid::new_v4();
    let mut insert = HashMap::new();
    insert.insert("id".to_string(), Value::Uuid(present_id));
    insert.insert("body".to_string(), Value::Text("present-copy".to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &insert)
        .expect("seed the one lineage this edge actually holds");
    let present_key = NaturalKey::single("id".to_string(), Value::Uuid(present_id));
    let absent_key = NaturalKey::single("id".to_string(), Value::Uuid(absent_id));
    let present = db
        .resolve_authoritative_purge_selection("notes", &present_key)
        .expect("resolve present immutable lineage");
    let absent_root = format!("author:{}:{}:7", "11".repeat(32), Incarnation(7).to_hex());
    let noncanonical_absent_roots = [
        format!("author:{}:7:7", "11".repeat(32)),
        format!("author:{}:{}:7", "AA".repeat(32), Incarnation(7).to_hex()),
        format!(
            "author:{}:{}:0007",
            "11".repeat(32),
            Incarnation(7).to_hex()
        ),
    ];
    for noncanonical_root in noncanonical_absent_roots {
        let invalid_items = vec![
            AuthoritativePurgeDeliveryItem {
                frontier: Lsn(20),
                ordinal: 0,
                table: "notes".to_string(),
                table_generation: present.table_generation,
                natural_key: present_key.clone(),
                purged_lineage_roots: vec![present.lineage_root.clone()],
            },
            AuthoritativePurgeDeliveryItem {
                frontier: Lsn(20),
                ordinal: 1,
                table: "notes".to_string(),
                table_generation: present.table_generation,
                natural_key: absent_key.clone(),
                purged_lineage_roots: vec![noncanonical_root],
            },
        ];
        assert!(
            db.apply_incoming_authoritative_purge_batch_while_authoritative(&invalid_items)
                .is_err(),
            "a noncanonical opaque creator root must fail the whole mixed purge batch"
        );
        assert!(
            db.row_id_for_natural_key_full("notes", &present_key, db.snapshot_for_read())
                .is_some(),
            "failed opaque-root validation must not destroy the present row"
        );
    }
    let items = vec![
        AuthoritativePurgeDeliveryItem {
            frontier: Lsn(20),
            ordinal: 0,
            table: "notes".to_string(),
            table_generation: present.table_generation,
            natural_key: present_key.clone(),
            purged_lineage_roots: vec![present.lineage_root.clone()],
        },
        AuthoritativePurgeDeliveryItem {
            frontier: Lsn(20),
            ordinal: 1,
            table: "notes".to_string(),
            table_generation: present.table_generation,
            natural_key: absent_key.clone(),
            purged_lineage_roots: vec![absent_root.clone()],
        },
    ];

    db.preflight_incoming_authoritative_purge_batch_while_authoritative(&items)
        .expect("sparse edge preflights present destruction plus absent frontier");
    db.apply_incoming_authoritative_purge_batch_while_authoritative(&items)
        .expect("one sparse-edge transaction destroys and adopts");
    assert!(
        db.row_id_for_natural_key_full("notes", &present_key, db.snapshot_for_read())
            .is_none(),
        "the copy this edge held is physically removed"
    );
    for (key, root) in [
        (&present_key, present.lineage_root.as_str()),
        (&absent_key, absent_root.as_str()),
    ] {
        assert!(matches!(
            db.classify_authoritative_purge_root_for_test(
                "notes",
                present.table_generation,
                key,
                root,
            ),
            AuthoritativePurgeRootClassification::Purged { .. }
        ));
    }
    let fresh_root = format!("author:{}:{}:8", "11".repeat(32), Incarnation(7).to_hex());
    assert_eq!(
        db.classify_authoritative_purge_root_for_test(
            "notes",
            present.table_generation,
            &absent_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged,
        "the permanent frontier refuses only the exact destroyed root, not a fresh lineage"
    );
    db.close().expect("close sparse edge after purge");
    drop(db);
    let reopened = Database::open(&path).expect("reopen sparse purge edge");
    for (key, root) in [
        (&present_key, present.lineage_root.as_str()),
        (&absent_key, absent_root.as_str()),
    ] {
        assert!(matches!(
            reopened.classify_authoritative_purge_root_for_test(
                "notes",
                present.table_generation,
                key,
                root,
            ),
            AuthoritativePurgeRootClassification::Purged { .. }
        ));
    }
}

#[test]
fn purge_commits_row_blob_roles_and_delivery_together_or_not_at_all() {
    let (root, db) = open_ledger();
    let path = root.path().join("engine-held-blobs.redb");
    let complete_bytes = b"complete-engine-held-detection-media";
    let partial_bytes = b"interrupted-engine-held-media-with-a-distinct-final-hash";
    let partial_prefix = b"interrupted-engine-held-prefix";
    let complete = BlobHash::of(complete_bytes);
    let partial = BlobHash::of(partial_bytes);
    submit_blob_job(&db, "selected-complete", &complete);
    submit_blob_job(&db, "selected-partial", &partial);
    db.install_authoritative_purge_complete_blob_fixture_for_test(
        &complete.as_bytes(),
        complete_bytes,
        9,
    )
    .expect("seed canonical manifest, complete payload, outboard, serve, protection, and generation fence");
    db.install_authoritative_purge_interrupted_blob_fixture_for_test(
        &partial.as_bytes(),
        partial_prefix,
        partial_bytes.len() as u64,
        99,
    )
    .expect("seed canonical manifest, verified partial, fetch state, and generation fence");
    let complete_roles = expected_roles(&COMPLETE_BLOB_ROLES);
    let partial_roles = expected_roles(&PARTIAL_BLOB_ROLES);
    assert_eq!(exact_blob_roles(&db, &complete), complete_roles);
    assert_eq!(exact_blob_roles(&db, &partial), partial_roles);
    let before_lsn = db.current_lsn();

    db.arm_authoritative_purge_blob_commit_failure_for_test();
    let failure = db
        .execute(
            "PURGE FROM work_jobs WHERE job_id IN ('selected-complete', 'selected-partial')",
            &params(),
        )
        .expect_err("a blob-side durable failure must deny public PURGE success");
    assert!(
        failure.to_string().contains("blob"),
        "the injected failure must identify the engine-held blob commit boundary: {failure}"
    );
    let db = reopen_ledger(db, &path);
    assert_eq!(
        work_job_ids(&db),
        vec![
            vec![Value::Text("selected-complete".to_string())],
            vec![Value::Text("selected-partial".to_string())],
        ]
    );
    assert_eq!(exact_blob_roles(&db, &complete), complete_roles);
    assert_eq!(exact_blob_roles(&db, &partial), partial_roles);
    assert_eq!(db.current_lsn(), before_lsn);
    assert_eq!(
        db.durable_deletion_state_for_test(
            "work_jobs",
            &Value::Text("selected-complete".to_string())
        )
        .purge_frontier,
        None,
        "a rolled-back complete blob removal must not publish a durable purge frontier"
    );
    assert_eq!(
        db.durable_deletion_state_for_test(
            "work_jobs",
            &Value::Text("selected-partial".to_string())
        )
        .purge_frontier,
        None,
        "a rolled-back interrupted blob removal must not publish a durable purge frontier"
    );
    assert!(
        db.authoritative_purge_delivery_items_since(Lsn(0))
            .expect("read durable purge delivery journal after rollback")
            .is_empty(),
        "a failed one-Redb commit must not expose downstream purge delivery"
    );

    db.execute(
        "PURGE FROM work_jobs WHERE job_id IN ('selected-complete', 'selected-partial')",
        &params(),
    )
    .expect("the complete and interrupted selected blob purge commits");
    let db = reopen_ledger(db, &path);
    assert!(work_job_ids(&db).is_empty());
    assert!(exact_blob_roles(&db, &complete).is_empty());
    assert!(exact_blob_roles(&db, &partial).is_empty());
    let delivery = db
        .authoritative_purge_delivery_items_since(Lsn(0))
        .expect("read committed purge delivery journal");
    assert_eq!(
        delivery.len(),
        2,
        "both committed selected lives yield delivery items"
    );
    assert!(delivery.iter().all(|item| item.table == "work_jobs"));
    assert!(delivery.iter().any(|item| {
        item.natural_key
            == NaturalKey::single(
                "job_id".to_string(),
                Value::Text("selected-complete".to_string()),
            )
    }));
    assert!(delivery.iter().any(|item| {
        item.natural_key
            == NaturalKey::single(
                "job_id".to_string(),
                Value::Text("selected-partial".to_string()),
            )
    }));
    let complete_frontier = db
        .durable_deletion_state_for_test("work_jobs", &Value::Text("selected-complete".to_string()))
        .purge_frontier
        .expect("the complete blob's removed work job has a durable frontier after reopen");
    let partial_frontier = db
        .durable_deletion_state_for_test("work_jobs", &Value::Text("selected-partial".to_string()))
        .purge_frontier
        .expect("the interrupted blob's removed work job has a durable frontier after reopen");
    assert_eq!(complete_frontier, partial_frontier);
    assert_eq!(complete_frontier, db.current_lsn().0.to_string());
    assert!(
        delivery
            .iter()
            .all(|item| item.frontier.0.to_string() == complete_frontier),
        "the row frontiers and every committed delivery item share the durable commit LSN"
    );
}

#[test]
fn purge_refuses_pre_frontier_backup_publication_and_post_purge_backup_restores_absence() {
    let (root, db) = open_ledger();
    let complete_bytes = b"backup-fenced-complete-engine-held-media";
    let partial_bytes = b"backup-fenced-interrupted-media-with-a-distinct-final-hash";
    let partial_prefix = b"backup-fenced-interrupted-prefix";
    let complete = BlobHash::of(complete_bytes);
    let partial = BlobHash::of(partial_bytes);
    submit_blob_job(&db, "selected-complete", &complete);
    submit_blob_job(&db, "selected-partial", &partial);
    db.install_authoritative_purge_complete_blob_fixture_for_test(
        &complete.as_bytes(),
        complete_bytes,
        7,
    )
    .expect("seed complete engine-held blob before backup checks");
    db.install_authoritative_purge_interrupted_blob_fixture_for_test(
        &partial.as_bytes(),
        partial_prefix,
        partial_bytes.len() as u64,
        70,
    )
    .expect("seed interrupted engine-held blob before backup checks");
    let complete_roles = expected_roles(&COMPLETE_BLOB_ROLES);
    let partial_roles = expected_roles(&PARTIAL_BLOB_ROLES);

    let control_backup = root.path().join("control-before-purge.redb");
    db.export_snapshot(&control_backup)
        .expect("a backup taken before any purge remains a valid control artifact");
    let control = Database::open(&control_backup).expect("open pre-purge control artifact");
    assert_eq!(
        work_job_ids(&control),
        vec![
            vec![Value::Text("selected-complete".to_string())],
            vec![Value::Text("selected-partial".to_string())],
        ]
    );
    assert_eq!(exact_blob_roles(&control, &complete), complete_roles);
    assert_eq!(exact_blob_roles(&control, &partial), partial_roles);
    assert_eq!(
        control.authoritative_purge_blob_bytes_for_test(&complete.as_bytes(), complete_bytes.len()),
        Some(complete_bytes.to_vec()),
        "the control restore can still serve bounded complete payload bytes"
    );
    assert_eq!(
        control.authoritative_purge_next_missing_range_for_test(&partial.as_bytes()),
        Some(partial_prefix.len() as u64..partial_bytes.len() as u64),
        "the control restore retains its durable next missing range for direct resume"
    );
    control.close().expect("close pre-purge control artifact");

    let refused_backup = root.path().join("captured-before-purge.redb");
    let pause = db.pause_after_export_snapshot_capture_for_test();
    std::thread::scope(|scope| {
        let export = scope.spawn(|| db.export_snapshot(&refused_backup));
        assert!(
            pause.wait_until_reached(std::time::Duration::from_secs(5)),
            "the export must capture its pre-purge snapshot before PURGE commits"
        );
        db.execute(
            "PURGE FROM work_jobs WHERE job_id IN ('selected-complete', 'selected-partial')",
            &params(),
        )
        .expect("purge commits while the obsolete backup is held before publication");
        pause.release();
        let error = export
            .join()
            .expect("paused export thread did not panic")
            .expect_err("a pre-frontier artifact must not publish after PURGE");
        assert!(matches!(error, Error::PurgeExportSnapshotFence { .. }));
    });
    assert!(
        !refused_backup.exists(),
        "the fenced pre-purge artifact is never published for a later restore"
    );

    let post_purge_backup = root.path().join("post-purge.redb");
    db.export_snapshot(&post_purge_backup)
        .expect("post-purge backup publishes after the permanent frontier");
    let restored = Database::open(&post_purge_backup).expect("open post-purge backup artifact");
    assert!(work_job_ids(&restored).is_empty());
    assert!(exact_blob_roles(&restored, &complete).is_empty());
    assert!(exact_blob_roles(&restored, &partial).is_empty());
    assert_eq!(
        restored
            .authoritative_purge_blob_bytes_for_test(&complete.as_bytes(), complete_bytes.len()),
        None,
        "a post-purge restore cannot serve bounded raw bytes for the complete hash"
    );
    assert_eq!(
        restored.authoritative_purge_next_missing_range_for_test(&partial.as_bytes()),
        None,
        "a post-purge restore cannot address a durable resume range for the partial hash"
    );
}
