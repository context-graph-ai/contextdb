//! A currency cleanup crash cannot orphan vectors or commit-index entries.

use super::*;
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

fn obsolete_scalar_lsns(db: &Database) -> Vec<Lsn> {
    let rows = db
        .persistence
        .as_ref()
        .unwrap()
        .load_relational_table("currency_scalar")
        .unwrap();
    let newest = rows.iter().map(|row| row.lsn).max().unwrap();
    rows.iter()
        .filter_map(|row| (row.lsn != newest).then_some(row.lsn))
        .collect()
}

fn journal_for_table(db: &Database, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut prefix = (table.len() as u64).to_be_bytes().to_vec();
    prefix.extend_from_slice(table.as_bytes());
    raw_vector_records(db, "vector_partition_journal")
        .into_iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .collect()
}

fn untouched_state(db: &Database) -> serde_json::Value {
    let persistence = db.persistence.as_ref().unwrap();
    serde_json::json!({
        "rows": persistence.load_relational_table("untouched").unwrap(),
        "journal": journal_for_table(db, "untouched"),
        "vectors": persistence.load_vectors().unwrap().into_iter()
            .filter(|entry| entry.index.table == "untouched").collect::<Vec<_>>(),
        "memberships": persistence.load_vector_partition_memberships().unwrap().into_iter()
            .filter(|entry| entry.index.table == "untouched").collect::<Vec<_>>(),
    })
}

fn assert_clean(db: &Database, obsolete_lsns: &[Lsn], untouched: &serde_json::Value) {
    let persistence = db.persistence.as_ref().unwrap();
    for table in ["currency_vectors", "currency_scalar"] {
        assert_eq!(persistence.load_relational_table(table).unwrap().len(), 1);
        assert_eq!(db.relational_store.tables.read()[table].len(), 1);
    }
    let vectors = persistence.load_vectors().unwrap();
    let memberships = persistence.load_vector_partition_memberships().unwrap();
    let selected = vectors
        .iter()
        .filter(|entry| entry.index.table == "currency_vectors")
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 1);
    assert!(selected[0].deleted_tx.is_none());
    let selected_memberships = memberships
        .iter()
        .filter(|entry| entry.index.table == "currency_vectors")
        .collect::<Vec<_>>();
    assert_eq!(selected_memberships.len(), 1);
    assert_eq!(
        selected_memberships[0].vector_created_tx,
        selected[0].created_tx
    );
    assert_eq!(selected_memberships[0].vector_lsn, selected[0].lsn);
    assert!(selected_memberships[0].deleted_tx.is_none());
    let (tail, _) = persistence
        .load_vector_partition_tail_entries(
            &selected_memberships[0].index,
            &selected_memberships[0].partition_key,
            Lsn(0),
        )
        .expect("every retained journal dependency resolves after cleanup");
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].row_id, selected[0].row_id);
    assert_eq!(tail[0].created_tx, selected[0].created_tx);
    assert_eq!(tail[0].lsn, selected[0].lsn);

    assert_eq!(untouched_state(db), *untouched);
    assert_eq!(
        raw_vector_records(db, "vector_partition_tombstones").len(),
        (VERSIONS - 1) as usize,
        "only the unrelated table's tombstones remain"
    );
    assert_eq!(
        vectors
            .iter()
            .filter(|entry| entry.index.table == "untouched")
            .count(),
        VERSIONS as usize
    );
    assert_eq!(
        memberships
            .iter()
            .filter(|entry| entry.index.table == "untouched")
            .count(),
        VERSIONS as usize
    );
    let commits = persistence.load_commit_index().unwrap();
    assert!(
        !obsolete_lsns.is_empty(),
        "the fixture must have removable commit-index identities"
    );
    for lsn in obsolete_lsns {
        assert!(
            !commits.contains_key(lsn),
            "obsolete commit {lsn:?} must be reclaimed"
        );
    }
    assert!(
        !commits.is_empty(),
        "cleanup retains the commit-index anchor and live references"
    );
    assert_eq!(
        db.execute("SELECT value FROM currency_scalar", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Int64((VERSIONS - 1) as i64)]]
    );
}

#[test]
fn currency_cleanup_crashes_keep_both_removal_gaps_atomic_and_retryable() {
    const CHILD_PATH: &str = "CONTEXTDB_CURRENCY_CLEANUP_TEST_PATH";
    const CHILD_BOUNDARY: &str = "CONTEXTDB_CURRENCY_CLEANUP_TEST_BOUNDARY";
    if let Ok(path) = std::env::var(CHILD_PATH) {
        let boundary = std::env::var(CHILD_BOUNDARY)
            .unwrap()
            .parse::<u8>()
            .unwrap();
        let db = open(std::path::Path::new(&path));
        crate::persistence::arm_currency_reclaim_fault_for_test(boundary, true);
        db.compact_currency_versions().unwrap();
        panic!("cleanup did not reach its durable crash checkpoint");
    }
    // The first two stop after relational removal and after vector removal,
    // still inside the same write transaction. The third stops after commit,
    // before memory publication, proving recovery of the complete new state.
    for boundary in [1, 2, 3] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("currency-crash.redb");
        let db = open(&path);
        seed(&db);
        let before = durable_state(&db);
        let obsolete_lsns = obsolete_scalar_lsns(&db);
        assert_eq!(obsolete_lsns.len(), (VERSIONS - 1) as usize);
        let untouched = untouched_state(&db);
        assert_eq!(
            raw_vector_records(&db, "vector_partition_tombstones").len(),
            2 * (VERSIONS - 1) as usize,
            "both tables start with old memberships to reclaim"
        );
        db.close().unwrap();
        drop(db);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "database::currency_cleanup_atomicity_tests::currency_cleanup_crashes_keep_both_removal_gaps_atomic_and_retryable", "--nocapture"])
            .env(CHILD_PATH, &path)
            .env(CHILD_BOUNDARY, boundary.to_string())
            .output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("CURRENCY_RECLAIM_CRASH={boundary}")),
            "child must reach the real transaction boundary: {output:?}"
        );

        let db = open(&path);
        if boundary < 3 {
            assert_eq!(
                durable_state(&db),
                before,
                "no selected removal can commit on its own"
            );
        } else {
            assert_clean(&db, &obsolete_lsns, &untouched);
        }
        let report = db.compact_currency_versions().unwrap();
        assert_eq!(
            report.pruned_versions,
            if boundary < 3 { 2 * (VERSIONS - 1) } else { 0 }
        );
        assert_eq!(
            report.vector_keys_rewritten,
            if boundary < 3 { VERSIONS - 1 } else { 0 }
        );
        assert_eq!(
            report.commit_index_keys_removed,
            if boundary < 3 { VERSIONS - 1 } else { 0 }
        );
        assert_eq!(
            report.tables_in_memory_snapshot, 2,
            "unrelated histories stay outside cleanup selection"
        );
        assert_eq!(report.keys_rewritten, 0);
        assert_eq!(report.edge_keys_rewritten, 0);
        assert_clean(&db, &obsolete_lsns, &untouched);
        let clean = durable_state(&db);
        let prior_journal = before["journal"].as_array().unwrap();
        let live_journal = clean["journal"].as_array().unwrap();
        assert_eq!(
            prior_journal.len() - live_journal.len(),
            2 * (VERSIONS - 1) as usize,
            "only reclaimed versions' upsert/tombstone references are removed"
        );
        for record in live_journal {
            assert!(
                prior_journal.contains(record),
                "surviving journal bytes are unchanged"
            );
        }
        let charged = db.accountant().usage().used;
        let again = db.compact_currency_versions().unwrap();
        assert_eq!(again.pruned_versions, 0);
        assert_eq!(again.vector_keys_rewritten, 0);
        assert_eq!(again.commit_index_keys_removed, 0);
        assert_eq!(
            db.accountant().usage().used,
            charged,
            "a retry cannot release bytes twice"
        );
        assert_eq!(durable_state(&db), clean);
        db.close().unwrap();
        drop(db);
        let reopened = open(&path);
        assert_eq!(
            durable_state(&reopened),
            clean,
            "repeated restart cannot resurrect a removed identity"
        );
        assert_clean(&reopened, &obsolete_lsns, &untouched);
        reopened.close().unwrap();
    }
}

#[test]
fn currency_cleanup_storage_errors_keep_memory_and_disk_available_for_retry() {
    for boundary in [1, 2] {
        let directory = TempDir::new().unwrap();
        let db = open(&directory.path().join("currency-error.redb"));
        seed(&db);
        let before = durable_state(&db);
        let memory_rows = serde_json::to_value(&*db.relational_store.tables.read()).unwrap();
        let memory_log = serde_json::to_value(&*db.change_log.read()).unwrap();
        let memory_directory = db.vector_store.raw_directory_entries();
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
        assert_eq!(db.vector_store.raw_directory_entries(), memory_directory);
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
        let pin = db.pin_snapshot(snapshot);
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
            .rows
            .len(),
            3
        );
    }
}
