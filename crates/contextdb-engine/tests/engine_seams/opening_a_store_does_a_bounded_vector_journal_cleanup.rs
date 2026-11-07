//! Opening a store does the same bounded work however long a route's unindexed
//! suffix is: the open decodes and walks the same few records, and
//! maintenance finishes the rest.
use contextdb_core::Value;
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const DIMENSION: usize = 16;

fn open(path: &std::path::Path) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(256 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    // A roomy reader: reading a long suffix is a separate bound from the open
    // this file measures.
    options.owner_reads.limits.limits.memory = 1 << 30;
    Database::open_with_options(path, options).unwrap()
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
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vector(target)))]),
    )
    .unwrap()
    .rows()
    .to_vec()
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

/// A route built over 16 rows; the rows committed after the build stay an
/// unindexed suffix because consolidation is off.
fn built(path: &std::path::Path) -> Database {
    let db = open(path);
    db.execute(
        &format!(
            "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 CONSOLIDATION NONE)"
        ),
        &HashMap::new(),
    )
    .unwrap();
    insert_batch(&db, 0, 16);
    for _ in 0..512 {
        if db
            .run_maintenance_cycle()
            .unwrap()
            .vector
            .remaining_partitions
            == 0
        {
            break;
        }
    }
    db
}

#[test]
fn the_open_reads_the_same_journal_records_however_long_the_suffix_is() {
    let root = tempfile::tempdir().unwrap();
    let mut observed = Vec::new();
    for suffix in [1024i64, 4096] {
        let path = root.path().join(format!("suffix-{suffix}.db"));
        let db = built(&path);
        for batch in 0..suffix / 512 {
            insert_batch(&db, 1_000_000 + batch * 512, 512);
        }
        db.close().unwrap();
        for _ in 0..2 {
            let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
            let db = open(&path);
            let counters = scope.counters();
            drop(scope);
            let last = 1_000_000 + suffix - 3;
            assert_eq!(
                nearest(&db, last),
                vec![vec![Value::Int64(last)]],
                "opening keeps the suffix"
            );
            db.close().unwrap();
            observed.push(counters);
        }
    }
    eprintln!("open work per open: {observed:?}");
    assert!(
        observed.iter().all(|counters| *counters == observed[0]),
        "{observed:?}"
    );
}
