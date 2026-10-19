//! Maintenance cleans a covered vector journal backlog in bounded steps and
//! releases the commit lock between them, so a write that arrives while the
//! backlog remains commits instead of waiting for the whole cleanup.
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, VectorJournalTruncationPhaseForTest,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const DIMENSION: usize = 16;

fn route() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("points", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap(),
    )
}

fn vector(id: i64) -> Vec<f32> {
    (0..DIMENSION as u64)
        .map(|lane| {
            let mut z = (id as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(lane.wrapping_mul(0xBF58_476D_1CE4_E5B9));
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z >> 40) as f32 / (1u64 << 24) as f32 - 0.5
        })
        .collect()
}

fn insert_batch(db: &Database, first: i64, rows: i64) {
    let mut sql = String::from("INSERT INTO points VALUES ");
    let mut params = HashMap::new();
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("($i{row}, 1, $v{row})"));
        params.insert(format!("i{row}"), Value::Int64(first + row));
        params.insert(format!("v{row}"), Value::Vector(vector(first + row)));
    }
    db.execute(&sql, &params).unwrap();
}

fn journal_records(db: &Database) -> usize {
    db.__debug_vector_partition_journal_file_for_test(&route())
        .physical_record_lsns
        .len()
}

#[test]
fn a_writer_commits_between_two_steps_of_a_covered_journal_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(256 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    let db = Arc::new(Database::open_with_options(root.path().join("lock.db"), options).unwrap());
    db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 64, EF_SEARCH = 256))"), &HashMap::new()).unwrap();
    db.execute(
        "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    insert_batch(&db, 0, 16);
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    insert_batch(&db, 1_000, 200);
    // The build that absorbs the 200 rows publishes its catalog and stops
    // before deleting the journal records it covered.
    let crash = db.__arm_vector_journal_truncation_crash_for_test(
        &route(),
        VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
    );
    let _ = db.run_maintenance_cycle().unwrap();
    drop(crash);
    let backlog = journal_records(&db);
    assert!(
        backlog > 64,
        "a backlog of more than one cleanup step: {backlog}"
    );

    let pause = db.__arm_one_shot_vector_journal_cleanup_step_gap_pause_for_test();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached(Duration::from_secs(60)),
        "maintenance reaches the gap between two cleanup steps"
    );
    // Checked first: with the lock held across the whole cleanup, anything
    // below that needs it would wait instead of failing.
    assert!(
        db.__try_global_commit_lock_for_test(),
        "the commit lock is free between cleanup steps"
    );
    let remaining = journal_records(&db);
    assert!(
        remaining > 0 && remaining < backlog,
        "one step committed, the rest remains: {remaining}"
    );
    db.execute(
        "INSERT INTO notes VALUES (1, 'written while the backlog remains')",
        &HashMap::new(),
    )
    .unwrap();
    pause.release();
    worker.join().unwrap().unwrap();
    assert_eq!(
        journal_records(&db),
        0,
        "the cleanup still finishes the backlog"
    );
    assert_eq!(
        db.execute("SELECT body FROM notes WHERE id = 1", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Text(
            "written while the backlog remains".into()
        )]]
    );
}
