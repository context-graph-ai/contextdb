//! Opening a store takes one bounded step of the vector journal cleanup.
//! However long a route's unindexed suffix or its covered backlog is, the
//! open reads the same few journal records; maintenance finishes the rest.
//! Damage in an uncovered record the open does not reach is reported by the
//! first indexed read that touches it, which quarantines the route; an exact
//! search still answers and a maintenance cycle repairs it.
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, VectorJournalTruncationPhaseForTest,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;

const DIMENSION: usize = 16;
/// What one open's cleanup step reads at most.
const OPEN_STEP_RECORDS: u64 = 64;

fn open(path: &std::path::Path) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(256 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    // A roomy reader: the first indexed read over a long suffix is a
    // separate bound from the open this file measures.
    options.owner_reads.limits.limits.memory = 1 << 30;
    Database::open_with_options(path, options).unwrap()
}

/// Open and report how many journal records the open's cleanup read.
fn open_counting(path: &std::path::Path) -> (Database, u64) {
    Database::__take_vector_journal_cleanup_records_read_for_test();
    let db = open(path);
    (
        db,
        Database::__take_vector_journal_cleanup_records_read_for_test(),
    )
}

fn route() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("points", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap(),
    )
}

fn vector(id: i64) -> Vec<f32> {
    // Distinct pseudo-random lanes per id (splitmix64), so every stored
    // vector is its own unique nearest neighbour.
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

fn nearest(db: &Database, target: i64, mode: &str) -> Result<Vec<Vec<Value>>, Error> {
    db.execute(
        &format!(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR {mode} LIMIT 1"
        ),
        &HashMap::from([("q".into(), Value::Vector(vector(target)))]),
    )
    .map(|result| result.rows)
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

/// A route built over 16 rows. With `absorbing`, maintenance rebuilds the
/// route over rows committed after the build (the default consolidation);
/// without it the later rows stay an unindexed suffix.
fn built_with(path: &std::path::Path, absorbing: bool) -> Database {
    let db = open(path);
    let consolidation = if absorbing { "" } else { " CONSOLIDATION NONE" };
    db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 64, EF_SEARCH = 256){consolidation})"), &HashMap::new()).unwrap();
    insert_batch(&db, 0, 16);
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    db
}

fn built(path: &std::path::Path) -> Database {
    built_with(path, false)
}

fn journal_records(db: &Database) -> usize {
    db.__debug_vector_partition_journal_file_for_test(&route())
        .physical_record_lsns
        .len()
}

#[test]
fn the_open_reads_the_same_journal_records_however_long_the_suffix_is() {
    let root = tempfile::tempdir().unwrap();
    let mut reads = Vec::new();
    for suffix in [1024i64, 4096] {
        let path = root.path().join(format!("suffix-{suffix}.db"));
        let db = built(&path);
        for batch in 0..suffix / 512 {
            insert_batch(&db, 1_000_000 + batch * 512, 512);
        }
        db.close().unwrap();
        for _ in 0..2 {
            let (db, read) = open_counting(&path);
            assert_eq!(
                journal_records(&db),
                suffix as usize,
                "opening keeps the suffix"
            );
            let last = 1_000_000 + suffix - 3;
            assert_eq!(
                nearest(&db, last, "INDEXED").unwrap(),
                vec![vec![Value::Int64(last)]]
            );
            db.close().unwrap();
            reads.push(read);
        }
    }
    eprintln!("journal records read per open: {reads:?}");
    assert!(
        reads
            .iter()
            .all(|read| *read == reads[0] && *read <= OPEN_STEP_RECORDS),
        "{reads:?}"
    );
}

#[test]
fn a_covered_backlog_drains_one_bounded_step_per_open_and_fully_in_maintenance() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("backlog.db");
    let db = built_with(&path, true);
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
        backlog > 2 * OPEN_STEP_RECORDS as usize,
        "covered backlog {backlog}"
    );
    db.close().unwrap();

    let mut expected = backlog;
    for _ in 0..2 {
        let (db, read) = open_counting(&path);
        expected -= OPEN_STEP_RECORDS as usize;
        eprintln!("backlog open: read {read} journal records");
        assert!(read <= OPEN_STEP_RECORDS, "{read}");
        assert_eq!(
            journal_records(&db),
            expected,
            "one open removes one bounded step"
        );
        assert_eq!(
            nearest(&db, 1_150, "INDEXED").unwrap(),
            vec![vec![Value::Int64(1_150)]]
        );
        db.close().unwrap();
    }
    let db = open(&path);
    db.run_maintenance_cycle().unwrap();
    assert_eq!(
        journal_records(&db),
        0,
        "one maintenance cycle drains the rest"
    );
    assert_eq!(
        nearest(&db, 1_150, "INDEXED").unwrap(),
        vec![vec![Value::Int64(1_150)]]
    );
    db.close().unwrap();
}

#[test]
fn damage_past_the_first_uncovered_record_is_reported_by_the_first_indexed_read() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("late-damage.db");
    let db = built(&path);
    for id in 100..108i64 {
        insert_batch(&db, id, 1);
    }
    db.close().unwrap();
    Database::__corrupt_last_vector_partition_journal_record_for_test(&path, &route()).unwrap();

    let (db, read) = open_counting(&path);
    assert_eq!(
        read, 1,
        "the open validates only the first uncovered record"
    );
    // The first indexed read finds the damage, quarantines the route and
    // refuses as the route being unavailable, as a quarantine found at open
    // does; later indexed reads refuse the same way.
    for read in ["first", "second"] {
        let refused = nearest(&db, 5, "INDEXED");
        assert!(
            matches!(refused, Err(Error::VectorIndexedRouteUnavailable { .. })),
            "{read} indexed read after the damage: {refused:?}"
        );
    }
    let shown = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR points.embedding",
            &HashMap::new(),
        )
        .unwrap();
    let column = |name: &str| shown.columns.iter().position(|c| c == name).unwrap();
    assert_eq!(
        shown.rows[0][column("availability_reason")],
        Value::Text("corrupt_changes".into())
    );
    assert_eq!(
        nearest(&db, 103, "AUTO").unwrap(),
        vec![vec![Value::Int64(103)]],
        "an exact search still answers from the stored vectors"
    );
    db.run_maintenance_cycle().unwrap();
    assert_eq!(
        nearest(&db, 103, "INDEXED").unwrap(),
        vec![vec![Value::Int64(103)]],
        "maintenance repairs the route"
    );
    db.close().unwrap();
}
