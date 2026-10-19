//! Opening a store charges the declared reader the same amount however long
//! its history is. A store whose vector column holds an unindexed suffix, one
//! whose trigger audit has fired far past its ring, and one with a long row
//! history each open at the same reader peak at two history sizes, and the
//! reopened store still answers exactly from its saved index and suffix.
use contextdb_core::Value;
use contextdb_engine::vector_read_memory::{
    take_reader_budget_ledger_for_test, watch_reader_budget_ledger_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const DIMENSION: usize = 16;

fn open(path: &std::path::Path) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(256 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    Database::open_with_options(path, options).unwrap()
}

/// The declared reader's highest held figure while the store opens.
fn open_peak(path: &std::path::Path) -> (Database, usize) {
    watch_reader_budget_ledger_for_test();
    let db = open(path);
    let peak = take_reader_budget_ledger_for_test()
        .iter()
        .map(|entry| entry.used_after)
        .max()
        .unwrap_or(0);
    (db, peak)
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

fn nearest(db: &Database, target: i64) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vector(target)))]),
    )
    .unwrap()
    .rows
}

/// A built route over 16 rows, then `suffix` rows committed after the build
/// and never absorbed into the saved index.
fn suffix_store(path: &std::path::Path, suffix: i64) {
    let db = open(path);
    db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 64, EF_SEARCH = 256) CONSOLIDATION NONE)"), &HashMap::new()).unwrap();
    for id in 0..16i64 {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("v".into(), Value::Vector(vector(id))),
            ]),
        )
        .unwrap();
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for batch in 0..suffix / 512 {
        let first = 1_000_000 + batch * 512;
        let mut sql = String::from("INSERT INTO points VALUES ");
        let mut params = HashMap::new();
        for row in 0..512 {
            if row > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("($i{row}, 1, $v{row})"));
            params.insert(format!("i{row}"), Value::Int64(first + row));
            params.insert(format!("v{row}"), Value::Vector(vector(first + row)));
        }
        db.execute(&sql, &params).unwrap();
    }
    db.close().unwrap();
}

fn trigger_store(path: &std::path::Path, fired: i64) {
    let db = open(path);
    db.execute(
        "CREATE TABLE fire (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER fire_guard ON fire WHEN INSERT",
        &HashMap::new(),
    )
    .unwrap();
    db.register_trigger_callback("fire_guard", |_, _| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
    for id in 0..fired {
        db.execute(
            "INSERT INTO fire (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

fn row_store(path: &std::path::Path, commits: i64) {
    let db = open(path);
    db.execute(
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..commits {
        db.execute(
            "INSERT INTO plain VALUES ($id, 'a modest row body')",
            &HashMap::from([("id".to_string(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

#[test]
fn an_unindexed_vector_suffix_does_not_grow_the_open_charge() {
    let root = tempfile::tempdir().unwrap();
    let mut peaks = Vec::new();
    for suffix in [1024i64, 4096] {
        let path = root.path().join(format!("suffix-{suffix}.db"));
        suffix_store(&path, suffix);
        let (db, peak) = open_peak(&path);
        // The reopened store answers exactly from its saved index and from
        // the suffix committed after it.
        let last = 1_000_000 + suffix - 7;
        assert_eq!(nearest(&db, last), vec![vec![Value::Int64(last)]]);
        assert_eq!(nearest(&db, 5), vec![vec![Value::Int64(5)]]);
        db.close().unwrap();
        eprintln!("suffix {suffix}: open peak {peak} B");
        peaks.push(peak);
    }
    assert_eq!(
        peaks[0], peaks[1],
        "a 4x longer unindexed suffix must not change the reader's open peak"
    );
}

#[test]
fn a_trigger_history_past_its_ring_does_not_grow_the_open_charge() {
    let root = tempfile::tempdir().unwrap();
    let mut peaks = Vec::new();
    for fired in [1100i64, 4400] {
        let path = root.path().join(format!("fired-{fired}.db"));
        trigger_store(&path, fired);
        let (db, peak) = open_peak(&path);
        db.close().unwrap();
        eprintln!("trigger history {fired}: open peak {peak} B");
        peaks.push(peak);
    }
    assert_eq!(peaks[0], peaks[1]);
}

#[test]
fn a_long_row_history_does_not_grow_the_open_charge() {
    let root = tempfile::tempdir().unwrap();
    let mut peaks = Vec::new();
    for commits in [1008i64, 4032] {
        let path = root.path().join(format!("rows-{commits}.db"));
        row_store(&path, commits);
        let (db, peak) = open_peak(&path);
        db.close().unwrap();
        eprintln!("row history {commits}: open peak {peak} B");
        peaks.push(peak);
    }
    assert_eq!(peaks[0], peaks[1]);
}
