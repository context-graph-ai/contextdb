// Independent shared-cache capacity guard. Mandatory-pressure coverage remains
// in the two required original guards once the producer path exists.
#![cfg(feature = "test-seams")]
use contextdb_core::{Value, read_contract::ReadLimits};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

#[test]
fn shared_native_cache_stays_inside_its_publicly_reported_capacity() {
    let root = tempfile::tempdir().unwrap();
    let budget = usize::try_from(ReadLimits::default().memory).unwrap();
    let db = Database::open_with_options(
        root.path().join("capacity.db"),
        DatabaseOpenOptions {
            memory_limit: Some(budget),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = db.native_ivf_cache_observation_for_test().unwrap();
    let reader = db.read_session(ReadLimits::default()).unwrap();
    let shown = reader.read_cache_share_status().unwrap();
    assert!(shown.shared_serving_cache);
    assert_eq!(shown.budget_bytes, budget);
    // Seven is the declared native default policy divisor, not a byte allowance.
    let expected = budget.saturating_sub(shown.fixed_reservation_bytes) / 7;
    assert_eq!(shown.effective_bytes, expected);
    assert_eq!(cache.installed_capacity(), expected);

    let dimensions = contextdb_vector::ivf::storage::PAGE_BYTES / size_of::<f32>();
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR({dimensions}) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95) IVF (LISTS = 1, PROBES = 1)) HISTORY ALL"), &HashMap::new()).unwrap();
    let body_bytes = dimensions.checked_mul(size_of::<f32>()).unwrap();
    let rows = expected
        .checked_div(body_bytes)
        .unwrap()
        .checked_add(1)
        .unwrap();
    assert!(rows.checked_mul(body_bytes).unwrap() > expected);
    let vector = vec![1_f32; dimensions];
    for id in 0..rows {
        db.execute(
            "INSERT INTO items VALUES ($id, $v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("v".into(), Value::Vector(vector.clone())),
            ]),
        )
        .unwrap();
    }
    // EXACT exercises persisted native bodies independently of qualification.
    // This guard makes no claim about index maintenance or mandatory reclaim.
    for _ in ["cold", "warm"] {
        let answer = db
            .execute(
                &format!("SELECT id FROM items ORDER BY v <=> $q USE VECTOR EXACT LIMIT {rows}"),
                &HashMap::from([("q".into(), Value::Vector(vector.clone()))]),
            )
            .unwrap();
        let mut ids = answer
            .rows()
            .iter()
            .map(|row| {
                let [Value::Int64(id)] = row.as_slice() else {
                    panic!("exact native answer contains identity")
                };
                *id
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (0..rows as i64).collect::<Vec<_>>());
        let actual = cache.native_cache_occupancy_for_test();
        assert_eq!(actual.limit, expected);
        assert!(
            actual
                .read_held_bytes
                .checked_add(actual.write_fixed_bytes)
                .unwrap()
                <= expected,
            "actual optional cache pages stay inside the original accounted capacity"
        );
        assert_eq!(db.memory_limit(), Some(budget));
    }
    drop(reader);
    drop(cache);
    db.close().unwrap();
}
