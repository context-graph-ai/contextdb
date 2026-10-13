#![cfg(feature = "test-seams")]
//! After a partition's first graph is published, maintenance removes the
//! journal records that graph now covers. Each record it decodes is already
//! funded by that cleanup's writer workspace. Before this repair the same
//! decode was charged again, and kept, by a private reader at the shipped
//! 16 MiB default, so cleaning the journal of a partition with a few tens of
//! thousands of new rows was refused on a budget nobody declared.
use contextdb_core::{Value, Wallclock, read_contract::ReadLimits};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const ROWS: i64 = 32_768;

fn partition_status(db: &Database) -> HashMap<String, Value> {
    let result = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR points.embedding",
            &HashMap::new(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    result
        .columns
        .into_iter()
        .zip(result.rows.into_iter().next().unwrap())
        .collect()
}

#[test]
fn a_journal_larger_than_the_shipped_reader_memory_is_cleaned_under_the_writer() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    assert_eq!(ReadLimits::SHIPPED_MEMORY, 16 * 1024 * 1024);
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("journal-cleanup.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(512 * 1024 * 1024),
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..ROWS {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "vector".into(),
                    Value::Vector(vec![0.0, 1.0, id as f32 + 1.0]),
                ),
            ]),
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(ROWS)),
            ("vector".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();
    let mut cycles = 0;
    loop {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(
            report.first_failure.is_none(),
            "cycle {cycles}: {:?}",
            report.first_failure
        );
        cycles += 1;
        if report.remaining_partitions == 0
            && partition_status(&db)["maintenance_state"] == Value::Text("idle".into())
        {
            break;
        }
        assert!(cycles < 4096, "maintenance must finish in finite cycles");
    }
    let status = partition_status(&db);
    eprintln!(
        "journal cleanup: rows={} cycles={cycles} status={status:?}",
        ROWS + 1
    );
    assert_eq!(status["query_state"], Value::Text("ready".into()));
    assert_eq!(
        db.execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new()
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(ROWS + 1)]]
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(ROWS)]]
    );
}
