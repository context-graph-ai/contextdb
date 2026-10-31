//! A currency cleanup crash cannot orphan vectors or commit-index entries.

use super::*;
include!("durable_test_observations.rs");
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use tempfile::TempDir;

const VERSIONS: u64 = 4;

fn open(path: &std::path::Path) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn seed(db: &Database) {
    for (name, body, policy) in [
        (
            "currency_vectors",
            "embedding VECTOR(3) PARTITION_KEY (bucket)",
            "HISTORY CURRENT ONLY SYNC OFF",
        ),
        (
            "currency_scalar",
            "value INT",
            "HISTORY CURRENT ONLY SYNC OFF",
        ),
        (
            "untouched",
            "embedding VECTOR(3) PARTITION_KEY (bucket)",
            "SYNC OFF",
        ),
    ] {
        db.execute(
            &format!(
                "CREATE TABLE {name} (id INT PRIMARY KEY, bucket INT NOT NULL, {body}) {policy}"
            ),
            &HashMap::new(),
        )
        .unwrap();
    }
    for table in ["currency_vectors", "untouched"] {
        db.execute(
            &format!("INSERT INTO {table} VALUES (1, 0, [1.0, 0.0, 0.0])"),
            &HashMap::new(),
        )
        .unwrap();
        for version in 1..VERSIONS {
            db.execute(
                &format!("UPDATE {table} SET embedding = $vector WHERE id = 1"),
                &HashMap::from([(
                    "vector".into(),
                    Value::Vector(vec![1.0, version as f32, 0.0]),
                )]),
            )
            .unwrap();
        }
    }
    // Scalar-only commits have no surviving vector-log references. Their old
    // LSNs exercise the second former crash gap independently of vector removal.
    db.execute(
        "INSERT INTO currency_scalar VALUES (1, 0, 0)",
        &HashMap::new(),
    )
    .unwrap();
    for version in 1..VERSIONS {
        db.execute(
            "UPDATE currency_scalar SET value = $value WHERE id = 1",
            &HashMap::from([("value".into(), Value::Int64(version as i64))]),
        )
        .unwrap();
    }
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn raw_vector_records(db: &Database, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.persistence
        .as_ref()
        .unwrap()
        .with_db(|redb| {
            let read = redb.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(TableDefinition::<&[u8], &[u8]>::new(name)) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            table
                .iter()
                .map_err(RedbPersistence::storage_error)?
                .map(|entry| {
                    let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                    Ok((key.value().to_vec(), value.value().to_vec()))
                })
                .collect()
        })
        .unwrap()
}

fn durable_state(db: &Database) -> serde_json::Value {
    let persistence = db.persistence.as_ref().unwrap();
    serde_json::json!({
        "rows": persistence.load_all_tables().unwrap(),
        "changes": persistence.load_change_log().unwrap(),
        "vectors": persistence.load_vectors().unwrap(),
        "memberships": persistence.load_vector_partition_memberships().unwrap(),
        "commit_index": persistence.load_commit_index().unwrap(),
        "tombstones": raw_vector_records(db, "vector_partition_tombstones"),
        "journal": raw_vector_records(db, "vector_partition_journal"),
        "base_generations": raw_vector_records(db, "vector_partition_base_generations"),
        "change_generations": raw_vector_records(db, "vector_partition_change_generations"),
        "catalog": raw_vector_records(db, "vector_partition_generation_catalog"),
    })
}

#[test]
fn currency_cleanup_storage_errors_keep_memory_and_disk_available_for_retry() {
    // This raw codec fixture declares its own writer operation; it has no
    // database request whose ambient owner it may borrow.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    for boundary in [1, 2] {
        let directory = TempDir::new().unwrap();
        let db = open(&directory.path().join("currency-error.redb"));
        seed(&db);
        let before = durable_state(&db);
        let memory_rows = serde_json::to_value(&*db.relational_store.tables.read()).unwrap();
        let memory_log = serde_json::to_value(&*db.change_log.read()).unwrap();
        let vector_fixture_reader_1 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_1 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

        let memory_directory = db
            .vector_store
            .raw_directory_entries(&mut vector_fixture_copy_1)
            .expect("read or publish the admitted vector fixture");
        let charged = db.accountant().usage().used;
        crate::persistence::arm_currency_reclaim_fault_for_test(boundary, false);
        let error = db.compact_currency_versions().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected currency cleanup failure")
        );
        assert_eq!(durable_state(&db), before);
        assert_eq!(
            serde_json::to_value(&*db.relational_store.tables.read()).unwrap(),
            memory_rows
        );
        assert_eq!(
            serde_json::to_value(&*db.change_log.read()).unwrap(),
            memory_log
        );
        let vector_fixture_reader_2 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_2 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_2.clone());

        assert_eq!(
            db.vector_store
                .raw_directory_entries(&mut vector_fixture_copy_2)
                .expect("read or publish the admitted vector fixture"),
            memory_directory
        );
        assert_eq!(db.accountant().usage().used, charged);
        let report = db.compact_currency_versions().unwrap();
        assert_eq!(report.pruned_versions, 2 * (VERSIONS - 1));
        assert_eq!(report.vector_keys_rewritten, VERSIONS - 1);
        assert_eq!(report.commit_index_keys_removed, VERSIONS - 1);
        db.close().unwrap();
    }
}

#[test]
fn currency_cleanup_reclaims_eligible_generations_at_its_durable_boundary() {
    const CHILD_PATH: &str = "CONTEXTDB_GENERATION_CLEANUP_TEST_PATH";
    const CHILD_BOUNDARY: &str = "CONTEXTDB_GENERATION_CLEANUP_TEST_BOUNDARY";
    if let Ok(path) = std::env::var(CHILD_PATH) {
        let boundary = std::env::var(CHILD_BOUNDARY)
            .unwrap()
            .parse::<u8>()
            .unwrap();
        let db = open(std::path::Path::new(&path));
        crate::persistence::arm_currency_reclaim_fault_for_test(boundary, true);
        db.compact_currency_versions().unwrap();
        panic!("generation cleanup did not reach its durable checkpoint");
    }
    for boundary in [1, 2, 3] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("generations.db");
        let db = open(&path);
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED) HISTORY CURRENT ONLY SYNC OFF", &HashMap::new()).unwrap();
        for id in 0..3 {
            db.execute(
                "INSERT INTO items VALUES ($id, 0, [1.0, 0.0, 0.0])",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .unwrap();
        }
        db.run_maintenance_cycle().unwrap();
        let snapshot = db.snapshot();
        let pin = db.pin_snapshot(snapshot).unwrap();
        for v in [0.25, 0.5] {
            db.execute(
                "UPDATE items SET v = $v WHERE id = 0",
                &HashMap::from([("v".into(), Value::Vector(vec![1.0, v, 0.0]))]),
            )
            .unwrap();
            db.run_maintenance_cycle().unwrap();
        }
        let generations = raw_vector_records(&db, "vector_partition_base_generations").len();
        assert!(
            generations >= 2,
            "the registered old snapshot owns a replaced base"
        );
        db.compact_currency_versions().unwrap();
        assert_eq!(
            raw_vector_records(&db, "vector_partition_base_generations").len(),
            generations
        );
        let before = durable_state(&db);
        drop(pin);
        db.close().unwrap();
        drop(db);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "database::currency_cleanup_atomicity_tests::currency_cleanup_reclaims_eligible_generations_at_its_durable_boundary", "--nocapture"])
            .env(CHILD_PATH, &path).env(CHILD_BOUNDARY, boundary.to_string()).output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("CURRENCY_RECLAIM_CRASH={boundary}")),
            "{output:?}"
        );
        let db = open(&path);
        let after = raw_vector_records(&db, "vector_partition_base_generations").len();
        eprintln!(
            "cleanup boundary={boundary} durable base generations before={generations} after={after}"
        );
        if boundary < 3 {
            assert_eq!(durable_state(&db), before);
            db.compact_currency_versions().unwrap();
        }
        assert_eq!(
            raw_vector_records(&db, "vector_partition_base_generations").len(),
            1,
            "the cleanup commit itself removes the unpinned base"
        );
        assert_eq!(
            db.execute(
                "SELECT id FROM items ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 3",
                &HashMap::new()
            )
            .unwrap()
            .rows()
            .len(),
            3
        );
    }
}
