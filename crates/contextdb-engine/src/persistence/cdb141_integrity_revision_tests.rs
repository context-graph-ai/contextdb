use super::*;
use crate::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, QueryResult, ReadSession,
    ReadSessionOptions,
};
use contextdb_core::read_contract::ReadRoute;
use redb::{ReadableDatabase, ReadableTable};
use std::path::{Path, PathBuf};

fn runtime(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("runtime");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}
fn owner(path: &Path, runtime: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.into()),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap()
}
fn sql(db: &Database, text: &str) {
    db.execute(text, &HashMap::new()).unwrap();
}
fn seed(path: &Path) {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    sql(
        &db,
        "CREATE TABLE integrity_rows (id INT PRIMARY KEY, bucket INT, body TEXT) HISTORY ALL",
    );
    sql(&db, "CREATE INDEX by_bucket ON integrity_rows(bucket, id)");
    sql(
        &db,
        "ALTER TABLE integrity_rows SET MAINTAIN COUNT ALL PREFIXES",
    );
    sql(&db, "CREATE TABLE integrity_controls (id INT PRIMARY KEY)");
    sql(&db, "INSERT INTO integrity_controls VALUES (1)");
    sql(&db, "INSERT INTO integrity_rows VALUES (1, 1, 'old')");
    sql(&db, "UPDATE integrity_rows SET body = 'head' WHERE id = 1");
    db.close().unwrap();
}
fn refusal(result: Result<QueryResult>, label: &str) {
    assert!(
        matches!(result, Err(Error::StoreCorrupted { ref path, ref reason }) if !path.is_empty() && !reason.is_empty()),
        "{label}: {result:?}"
    );
}
fn bypass(path: &Path, index: bool) {
    let db = redb::Database::open(path).unwrap();
    let write = db.begin_write().unwrap();
    if index {
        let name = RedbPersistence::relational_index_table_name("integrity_rows", "by_bucket");
        let mut table = write
            .open_table(redb::TableDefinition::<
                crate::persistence::DurableRelationalIndexKey,
                &[u8],
            >::new(&name))
            .unwrap();
        let key = table.first().unwrap().unwrap().0.value();
        table.remove(key).unwrap();
    } else {
        let mut table = write
            .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(
                "rel_integrity_rows",
            ))
            .unwrap();
        let key = table.first().unwrap().unwrap().0.value().to_vec();
        table.remove(key.as_slice()).unwrap();
    }
    write.commit().unwrap();
}
#[test]
fn empty_index_and_maintained_groups_refuse_both_roots_on_both_public_routes() {
    let queries = [
        "SELECT id, body FROM integrity_rows WHERE bucket = 99",
        "SELECT id, body FROM integrity_rows WHERE bucket IN (98, 99) ORDER BY bucket",
        "SELECT v.id, v.body FROM integrity_rows v INNER JOIN integrity_controls c ON v.id = c.id WHERE v.bucket = 99",
        "SELECT bucket, COUNT(*) FROM integrity_rows GROUP BY bucket",
        "SELECT id, COUNT(*) FROM integrity_rows WHERE bucket = 99 GROUP BY id",
    ];
    for index in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("roots.db");
        let rt = runtime(dir.path());
        seed(&path);
        let before = {
            let file = ReadSession::open(&path).unwrap();
            assert_eq!(file.route(), ReadRoute::File);
            queries.map(|query| file.execute(query, &HashMap::new()).unwrap().rows)
        };
        assert!(
            before[0].is_empty()
                && before[1].is_empty()
                && before[2].is_empty()
                && before[4].is_empty()
        );
        assert_eq!(before[3].len(), 1);
        {
            let db = owner(&path, &rt);
            let session = ReadSession::open_owner_only_in_runtime_dir(
                &path,
                ReadSessionOptions::default(),
                Some(rt.clone()),
            )
            .unwrap();
            assert_eq!(session.route(), ReadRoute::Owner);
            for (query, expected) in queries.into_iter().zip(&before) {
                assert_eq!(
                    session.execute(query, &HashMap::new()).unwrap().rows,
                    *expected
                );
            }
            drop(session);
            db.close().unwrap();
        }
        bypass(&path, index);
        {
            let file = ReadSession::open(&path).unwrap();
            for (position, query) in queries.into_iter().enumerate() {
                let result = file.execute(query, &HashMap::new());
                // This JOIN uses Scan and never selects by_bucket. Its relevant
                // protected root is the canonical body, not an unused index.
                if index && position == 2 {
                    assert_eq!(result.unwrap().rows, before[position]);
                } else {
                    refusal(result, &format!("file index_damage={index}: {query}"));
                }
            }
        }
        let db = owner(&path, &rt);
        let session = ReadSession::open_owner_only_in_runtime_dir(
            &path,
            ReadSessionOptions::default(),
            Some(rt.clone()),
        )
        .unwrap();
        for (position, query) in queries.into_iter().enumerate() {
            let result = session.execute(query, &HashMap::new());
            if index && position == 2 {
                assert_eq!(result.unwrap().rows, before[position]);
            } else {
                refusal(result, &format!("owner index_damage={index}: {query}"));
            }
        }
        drop(session);
        drop(db);
    }
}

// The unique, same-length text remains valid input to the row decoder. Only
// authentication of the consumed native page can reject this physical mutation.
fn physical_flip(path: &Path, marker: &[u8]) {
    use std::os::unix::fs::FileExt;
    let bytes = std::fs::read(path).unwrap();
    let positions = bytes
        .windows(marker.len())
        .enumerate()
        .filter_map(|(i, v)| (v == marker).then_some(i))
        .collect::<Vec<_>>();
    assert!(!positions.is_empty());
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    for position in positions {
        file.write_all_at(b"X", (position + marker.len() - 1) as u64)
            .unwrap();
    }
    file.sync_all().unwrap();
}
#[test]
fn owner_consumed_cold_body_checksums_are_typed_and_unvisited_damage_is_irrelevant() {
    for owner_route in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("physical.db");
        let rt = runtime(dir.path());
        let db = owner(&path, &rt);
        sql(
            &db,
            "CREATE TABLE integrity_rows (id INT PRIMARY KEY, bucket INT, body TEXT) HISTORY ALL",
        );
        const MARKER: &str = "CDB141-COLD-LAST-LEAF-VALID-TEXT";
        for id in 0..64 {
            db.execute(
                "INSERT INTO integrity_rows VALUES ($id, 1, $body)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    (
                        "body".into(),
                        Value::Text(if id == 63 {
                            format!("{MARKER}{}", "a".repeat(8192))
                        } else {
                            "b".repeat(8192)
                        }),
                    ),
                ]),
            )
            .unwrap();
        }
        db.close().unwrap();
        physical_flip(&path, MARKER.as_bytes());
        if !owner_route {
            let file = ReadSession::open(&path).unwrap();
            assert_eq!(file.route(), ReadRoute::File);
            assert_eq!(
                file.execute(
                    "SELECT id, body FROM integrity_rows WHERE id = 0",
                    &HashMap::new()
                )
                .unwrap()
                .rows
                .len(),
                1
            );
            refusal(
                file.execute(
                    "SELECT id, body FROM integrity_rows WHERE id = 63",
                    &HashMap::new(),
                ),
                "file consumed cold body",
            );
            continue;
        }
        let db = owner(&path, &rt); // Capture after damage; no valid pre-damage cache copy exists.
        let session = ReadSession::open_owner_only_in_runtime_dir(
            &path,
            ReadSessionOptions::default(),
            Some(rt.clone()),
        )
        .unwrap();
        assert_eq!(session.route(), ReadRoute::Owner);
        assert_eq!(
            session
                .execute(
                    "SELECT id, body FROM integrity_rows WHERE id = 0",
                    &HashMap::new()
                )
                .unwrap()
                .rows
                .len(),
            1
        );
        refusal(
            session.execute(
                "SELECT id, body FROM integrity_rows WHERE id = 63",
                &HashMap::new(),
            ),
            "consumed cold body",
        );
        drop(session);
        drop(db);
    }
}

#[test]
fn storage_checksum_mapping_preserves_native_corruption_and_distinct_io() {
    let error = RedbPersistence::storage_error(redb::Error::Corrupted(
        "checksum mismatch in directory page".into(),
    ));
    assert!(
        matches!(error, Error::StoreCorrupted { path, reason } if !path.is_empty() && reason.contains("checksum"))
    );
    let io = RedbPersistence::storage_error(redb::StorageError::Io(std::io::Error::from(
        std::io::ErrorKind::PermissionDenied,
    )));
    assert!(!matches!(io, Error::StoreCorrupted { .. }));
}

#[derive(Default)]
struct Touches(std::sync::atomic::AtomicU64);
impl crate::read_session::ReadKernelTestObserver for Touches {
    fn before_source_touch(
        &self,
        _: crate::read_session::ReadKernelSourceEvent,
        _: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn cancellation_observed(
        &self,
        _: crate::read_session::ReadKernelCancellationEvent,
        _: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
    }
}
#[test]
fn actual_current_point_list_and_index_work_stays_bounded_at_n_2n_4n_on_both_routes() {
    use std::sync::atomic::Ordering;
    let queries = [
        "SELECT id, body FROM integrity_rows WHERE id = 1",
        "SELECT id, body FROM integrity_rows WHERE id IN (1, 2) ORDER BY id",
        "SELECT id, body FROM integrity_rows WHERE bucket = 1 ORDER BY bucket",
    ];
    // Manager-authorized composed fixed work: file [1, 2, 2] and owner
    // [87, 131, 97] at each of N, 2N and 4N. The promise is invariant work,
    // with a conservative fixed ceiling for the composed source controls.
    let mut file_work = HashMap::new();
    let mut owner_work = HashMap::new();
    for versions in [64, 128, 256] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("growth.db");
        let rt = runtime(dir.path());
        seed(&path);
        {
            let db = owner(&path, &rt);
            for version in 0..versions {
                db.execute(
                    "UPDATE integrity_rows SET body = $body WHERE id = 1",
                    &HashMap::from([("body".into(), Value::Text(format!("history-{version:04}")))]),
                )
                .unwrap();
            }
            sql(&db, "UPDATE integrity_rows SET body = 'head' WHERE id = 1");
            db.close().unwrap();
        }
        let observation = Arc::new(Touches::default());
        {
            let file = ReadSession::open_with_kernel_observer_for_test(
                &path,
                ReadSessionOptions::default(),
                observation.clone(),
            )
            .unwrap();
            assert_eq!(file.route(), ReadRoute::File);
            for query in queries {
                observation.0.store(0, Ordering::Relaxed);
                let result = file.execute(query, &HashMap::new()).unwrap();
                assert_eq!(result.rows.len(), 1);
                let touches = observation.0.load(Ordering::Relaxed);
                assert!(
                    touches > 0 && touches <= 160,
                    "file {versions}/{query}: {touches}"
                );
                let expected = *file_work.entry(query).or_insert(touches);
                assert_eq!(touches, expected, "file {versions}/{query}: N/2N/4N work is invariant");
            }
        }
        let db = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                owner_reads: OwnerReadConfig {
                    runtime_dir: Some(rt.clone()),
                    ..Default::default()
                },
                test_kernel_observer: Some(observation.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        let session = ReadSession::open_owner_only_in_runtime_dir(
            &path,
            ReadSessionOptions::default(),
            Some(rt.clone()),
        )
        .unwrap();
        for query in queries {
            observation.0.store(0, Ordering::Relaxed);
            assert_eq!(
                session.execute(query, &HashMap::new()).unwrap().rows.len(),
                1
            );
            let touches = observation.0.load(Ordering::Relaxed);
            assert!(
                touches > 0 && touches <= 160,
                "owner {versions}/{query}: {touches}"
            );
            let expected = *owner_work.entry(query).or_insert(touches);
            assert_eq!(touches, expected, "owner {versions}/{query}: N/2N/4N work is invariant");
        }
        drop(session);
        db.close().unwrap();
    }
}

pub(crate) fn assert_all_history_witnesses(path: &Path) {
    let db = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = db.begin_read().unwrap();
    for table in read.list_tables().unwrap() {
        use redb::TableHandle;
        let name = table.name();
        if name.starts_with("rel_")
            || name.starts_with("__rel_index_")
                && name.strip_prefix("__rel_index_").unwrap().len() == 64
        {
            RedbPersistence::verify_history_root(&read, name).unwrap();
        }
    }
}
#[test]
fn export_rebuild_prune_and_drop_recreate_keep_producer_witnesses() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lifecycle.db");
    let exported = dir.path().join("export.db");
    seed(&path);
    assert_all_history_witnesses(&path);
    {
        let db = Database::open(&path).unwrap();
        db.export_snapshot(&exported).unwrap();
        db.close().unwrap();
    }
    assert_all_history_witnesses(&exported);
    {
        let file = ReadSession::open(&exported).unwrap();
        assert_eq!(
            file.execute(
                "SELECT id, body FROM integrity_rows WHERE bucket = 1",
                &HashMap::new()
            )
            .unwrap()
            .rows
            .len(),
            1
        );
    }
    {
        let fixture_open_accountant_415 = std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_415 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_415)).unwrap();
        let store = RedbPersistence::open(&path).unwrap();
        store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_415));
        let metas = store.load_all_table_meta().unwrap();
        store
            .rebuild_relational_indexes_for_current_table(
                "integrity_rows",
                &metas["integrity_rows"],
            )
            .unwrap();
    }
    assert_all_history_witnesses(&path);
    {
        let fixture_open_accountant_426 = std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_426 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_426)).unwrap();
        let store = RedbPersistence::open(&path).unwrap();
        store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_426));
        let rows = store.load_relational_table("integrity_rows").unwrap();
        let old = rows
            .iter()
            .find(|row| row.deleted_tx.is_some())
            .expect("a superseded body exists");
        let stats = store
            .prune_versions_scoped(
                &[("integrity_rows".into(), old.row_id, old.created_tx, old.lsn)],
                &[],
                &[],
            )
            .unwrap();
        assert!(stats.row_keys_removed > 0);
    }
    assert_all_history_witnesses(&path);
    {
        let db = Database::open(&path).unwrap();
        sql(&db, "DROP TABLE integrity_rows");
        sql(
            &db,
            "CREATE TABLE integrity_rows (id INT PRIMARY KEY, bucket INT, body TEXT) HISTORY ALL",
        );
        sql(&db, "CREATE INDEX by_bucket ON integrity_rows(bucket, id)");
        sql(&db, "INSERT INTO integrity_rows VALUES (2, 2, 'recreated')");
        db.close().unwrap();
    }
    assert_all_history_witnesses(&path);
    let file = ReadSession::open(&path).unwrap();
    assert_eq!(
        file.execute(
            "SELECT id, body FROM integrity_rows WHERE bucket = 2",
            &HashMap::new()
        )
        .unwrap()
        .rows
        .len(),
        1
    );
}

#[test]
fn consumed_index_and_witness_pages_refuse_with_public_typed_corruption() {
    for target in [0, 1, 2] {
        for owner_route in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("physical-metadata.db");
            let rt = runtime(dir.path());
            seed(&path);
            let marker = {
                let raw = redb::ReadOnlyDatabase::open(&path).unwrap();
                let read = raw.begin_read().unwrap();
                if target == 2 {
                    b"rel_integrity_rows".to_vec()
                } else if target == 1 {
                    let table = read
                        .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                            "__history_root_witness_v1",
                        ))
                        .unwrap();
                    table
                        .get("rel_integrity_rows")
                        .unwrap()
                        .unwrap()
                        .value()
                        .to_vec()
                } else {
                    let name = format!(
                        "{}_current",
                        RedbPersistence::relational_index_table_name("integrity_rows", "by_bucket")
                    );
                    let table = read
                        .open_table(
                            redb::TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&name),
                        )
                        .unwrap();
                    table.first().unwrap().unwrap().0.value().0
                }
            };
            physical_flip(&path, &marker);
            let query = "SELECT id, body FROM integrity_rows WHERE bucket = 1 ORDER BY bucket";
            if owner_route {
                let db = Database::open_with_options(
                    &path,
                    DatabaseOpenOptions {
                        maintenance_policy: MaintenancePolicy::CallerDriven,
                        owner_reads: OwnerReadConfig {
                            runtime_dir: Some(rt.clone()),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                );
                match db {
                    Err(error) => assert!(
                        matches!(error, Error::StoreCorrupted { .. }),
                        "owner open: {error:?}"
                    ),
                    Ok(db) => {
                        let session = ReadSession::open_owner_only_in_runtime_dir(
                            &path,
                            ReadSessionOptions::default(),
                            Some(rt.clone()),
                        )
                        .unwrap();
                        refusal(
                            session.execute(query, &HashMap::new()),
                            "owner consumed metadata/index",
                        );
                        drop(session);
                        drop(db);
                    }
                }
            } else {
                match ReadSession::open(&path) {
                    Err(error) => assert!(
                        matches!(error, Error::StoreCorrupted { .. }),
                        "file open: {error:?}"
                    ),
                    Ok(file) => refusal(
                        file.execute(query, &HashMap::new()),
                        "file consumed metadata/index",
                    ),
                }
            }
        }
    }
}
