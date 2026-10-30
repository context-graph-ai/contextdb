//! A statement's breadth override controls the real native search only for that statement.
use contextdb_core::Value;
use contextdb_engine::vector_ivf_observations::{
    self, NativeIvfFaultBoundary, NativeIvfSealEvent, NativeIvfSealObserver,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::ivf::Work;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Default)]
struct Observed(Mutex<Work>);
impl NativeIvfSealObserver for Observed {
    fn observe(&self, event: NativeIvfSealEvent<'_>) {
        if let NativeIvfSealEvent::QueryExecuted { work, .. } = event {
            *self.0.lock().unwrap() = *work;
        }
    }
    fn before_boundary(&self, _: NativeIvfFaultBoundary<'_>) -> contextdb_core::Result<()> {
        Ok(())
    }
}
fn work(db: &Database, suffix: &str) -> Work {
    let seen = Arc::new(Observed::default());
    vector_ivf_observations::with_native_ivf_seal_observer_for_test(seen.clone(), || {
        let result = db
            .execute(
                &format!(
                    "SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED {suffix} LIMIT 4"
                ),
                &HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))]),
            )
            .unwrap();
        assert_eq!(result.rows().len(), 4);
    });
    let value = *seen.0.lock().unwrap();
    value
}
#[test]
fn a_query_breadth_override_changes_actual_probes_and_rescores_without_changing_the_column() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("overrides.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED IVF (LISTS = 4, PROBES = 1, REFINE_FACTOR = 1.00) WITH (training_samples = 64, training_iterations = 1))",&HashMap::new()).unwrap();
    let tx = db.begin_or_panic();
    for id in 0..64 {
        let angle = (id as f64) * std::f64::consts::TAU / 64.0;
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                (
                    "v".to_string(),
                    Value::Vector(vec![angle.cos() as f32, angle.sin() as f32]),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
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
    let base = work(&db, "");
    assert_eq!((base.lists_probed, base.full_precision_evaluations), (1, 4));
    let overridden = work(&db, "(PROBES = 4, REFINE_FACTOR = 4.00)");
    assert_eq!(
        (
            overridden.lists_probed,
            overridden.full_precision_evaluations
        ),
        (4, 16),
        "the query uses its own literal breadth"
    );
    let restored = work(&db, "");
    assert_eq!(
        (restored.lists_probed, restored.full_precision_evaluations),
        (1, 4),
        "the column's breadth remains unchanged"
    );
    db.close().unwrap();
}
