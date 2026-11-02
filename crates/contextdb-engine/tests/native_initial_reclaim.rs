//! Retiring the unclustered family releases vectors while keeping its journal.
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::ivf::storage::PageKind;
use std::collections::HashMap;

fn initial_kinds(db: &Database) -> (u64, u64) {
    let (mut payloads, mut journals) = (0, 0);
    db.visit_native_ivf_physical_pages_for_test(&VectorIndexRef::new("items", "v"), &mut |page| {
        if page.key.generation == 0 {
            match page.key.kind {
                PageKind::RowVersions | PageKind::RowCopies => journals += 1,
                _ => payloads += 1,
            }
        }
    })
    .unwrap();
    (payloads, journals)
}
#[test]
#[ignore = "index deferred to the follow-on plan"]
fn initial_unclustered_payloads_retire_after_remap_without_recreating_old_pages() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("initial.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 32.00) WITH (training_samples = 32, training_iterations = 1))",&HashMap::new()).unwrap();
    for id in 0..32 {
        db.execute(
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("v".to_string(), Value::Vector(vec![1.0, id as f32 / 32.0])),
            ]),
        )
        .unwrap();
    }
    let before = initial_kinds(&db);
    assert!(
        before.0 > 0 && before.1 > 0,
        "unclustered vector payloads and real row journals exist before maintenance"
    );
    let mut complete = false;
    for _ in 0..32 {
        if db
            .run_maintenance_cycle()
            .unwrap()
            .vector
            .remaining_partitions
            == 0
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    let after = initial_kinds(&db);
    assert_eq!(
        after.0, 0,
        "after state C, the old unclustered canonical/list/tail payloads are gone"
    );
    assert!(after.1 > 0, "the current row-version/copy journal remains");
    db.execute(
        "UPDATE items SET v = $v WHERE id = 0",
        &HashMap::from([("v".to_string(), Value::Vector(vec![0.0, 1.0]))]),
    )
    .unwrap();
    db.execute("DELETE FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    assert_eq!(
        initial_kinds(&db).0,
        0,
        "mutation only follows live copies and never recreates old generation payloads"
    );
    let found = db
        .execute(
            "SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 32",
            &HashMap::from([("q".to_string(), Value::Vector(vec![0.0, 1.0]))]),
        )
        .unwrap();
    assert_eq!(found.rows().len(), 31);
    assert_eq!(found.rows()[0][0], Value::Int64(0));
    db.close().unwrap();
}
