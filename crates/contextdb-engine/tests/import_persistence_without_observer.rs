//! Ordinary import persistence does not require a physical proof observer.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const WRITER_LIMIT: usize = 512 * 1024 * 1024;

fn options() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(WRITER_LIMIT),
        ..DatabaseOpenOptions::default()
    }
}

fn nearest(db: &Database, id: i64) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vec![0.0, 1.0, id as f32 + 1.0]))]),
    )
    .unwrap()
    .rows()
    .to_vec()
}

fn append(db: &Database, id: i64) {
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

#[test]
#[ignore = "the vector index route is switched off in this release"]
fn ordinary_import_persistence_advances_durable_coverage_without_an_observer() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("unobserved-import.db");
    let db = Database::open_with_options(&path, options()).unwrap();
    db.execute(
 "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 CONSOLIDATION NONE)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        append(&db, id);
    }
    let mut built = false;
    for _ in 0..128 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
        if report.remaining_partitions == 0 {
            built = true;
            break;
        }
    }
    assert!(
        built,
        "the initial native graph finishes in bounded maintenance steps"
    );
    assert!(
        db.persist_import_vector_tails().unwrap(),
        "the initial native index covers its rows"
    );
    append(&db, 16);

    let mut covered = db.persist_import_vector_tails().unwrap();
    for _ in 0..128 {
        if covered {
            break;
        }
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
        covered = db.persist_import_vector_tails().unwrap();
    }
    assert!(
        covered,
        "bounded maintenance reaches full coverage of the committed suffix"
    );
    assert_eq!(nearest(&db, 16), vec![vec![Value::Int64(16)]]);
    assert!(db.write_memory_high_water_for_test() <= WRITER_LIMIT);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    drop(db);
    let reopened = Database::open_with_options(&path, options()).unwrap();
    assert!(
        reopened.persist_import_vector_tails().unwrap(),
        "coverage is durable"
    );
    assert_eq!(nearest(&reopened, 16), vec![vec![Value::Int64(16)]]);
}
