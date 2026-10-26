//! Ordinary import persistence does not require a physical proof observer.
use contextdb_core::{Lsn, TxId, Value, Wallclock};
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

fn serving(db: &Database) -> (Lsn, TxId, Option<TxId>, usize) {
    let routes = db
        .vector_serving_pending_inserts_for_test("points", "embedding")
        .unwrap();
    assert_eq!(routes.len(), 1, "one native route: {routes:?}");
    routes[0]
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
fn ordinary_import_persistence_advances_durable_coverage_without_an_observer() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("unobserved-import.db");
    let db = Database::open_with_options(&path, options()).unwrap();
    db.execute(
        "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)",
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
    let base = serving(&db);
    assert_eq!(base.3, 0, "the initial graph covers its rows");
    append(&db, 16);
    let before = serving(&db);
    assert_eq!(
        before.0, base.0,
        "the append has not advanced serving coverage"
    );
    assert_eq!(before.3, 1, "one real committed vector awaits persistence");
    eprintln!(
        "native import before persistence: {before:?}; store={}",
        path.display()
    );

    assert!(db.persist_import_vector_tails().unwrap());

    let after = serving(&db);
    assert!(
        after.0 > before.0,
        "the committed suffix advances serving coverage"
    );
    assert_eq!(
        after.2,
        Some(after.1),
        "coverage names its committed transaction"
    );
    assert_eq!(after.3, 0, "the suffix is fully covered");
    assert!(db.write_memory_high_water_for_test() <= WRITER_LIMIT);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    drop(db);
    let reopened = Database::open_with_options(&path, options()).unwrap();
    assert_eq!(
        serving(&reopened),
        after,
        "coverage and pending counters are durable"
    );
}
