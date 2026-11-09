//! One immutable private candidate verifies once while its ordinary serving view stays separate.
use contextdb_core::{VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

#[test]
fn default_training_seed_tracks_policy_revision_but_explicit_zero_stays_zero() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("seed-revision.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, v VECTOR(2) SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    let index = VectorIndexRef::new("items", "v");
    let tuple = VectorPartitionKey::unpartitioned();
    let resolve = || {
        db.native_ivf_resolved_policy_for_test(&index, &tuple)
            .unwrap()
            .training
            .seed
    };
    let first = resolve();
    assert_eq!(
        resolve(),
        first,
        "an unchanged partition/source/revision repeats its default seed"
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 99)",
        &HashMap::new(),
    )
    .unwrap();
    assert_ne!(
        resolve(),
        first,
        "the declared DEFAULT seed includes the selected partition's policy revision"
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET WITH (training_seed = 0)",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(resolve(), 0, "explicit zero is the consumer's exact seed");
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 95)",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(
        resolve(),
        0,
        "a policy revision never changes an explicit seed"
    );
    db.close().unwrap();
}
