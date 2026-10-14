#![cfg(feature = "test-seams")]
//! Writer-side work that decodes stored values without an operation owner --
//! a local ALTER rewriting a table, journal cleanup, a received schema, a
//! saved topology export -- is charged to the store's declared writer memory
//! limit. Before this repair each such decode opened a private reader at the
//! shipped 16 MiB default, so an ALTER on a table whose vectors decode past
//! 16 MiB was refused whatever MEMORY_LIMIT the consumer declared.
use contextdb_core::{Value, Wallclock, read_contract::ReadLimits};
use contextdb_engine::vector_read_memory::reader_parents_created_for_test;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const DIMENSION: usize = 1536;
const ROWS: i64 = 3000;

#[test]
fn an_alter_on_a_table_larger_than_the_shipped_reader_memory_is_charged_to_the_writer() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    assert_eq!(ReadLimits::SHIPPED_MEMORY, 16 * 1024 * 1024);
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(root.path().join("ownerless-alter.db"), {
        // The consumer declares a reader far larger than the table, so
        // only a budget nobody declared could refuse this ALTER.
        let mut options = DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(512 * 1024 * 1024),
            ..DatabaseOpenOptions::default()
        };
        options.owner_reads.limits.limits.memory = 1 << 30;
        options
    })
    .unwrap();
    db.execute(
        &format!("CREATE TABLE points (id INT PRIMARY KEY, embedding VECTOR({DIMENSION}))"),
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..ROWS {
        let vector: Vec<f32> = (0..DIMENSION)
            .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin())
            .collect();
        db.execute(
            "INSERT INTO points VALUES ($id, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("vector".into(), Value::Vector(vector)),
            ]),
        )
        .unwrap();
    }
    assert!(
        ROWS as usize * DIMENSION * 4 > ReadLimits::SHIPPED_MEMORY as usize,
        "the table's decoded vectors exceed the shipped reader memory"
    );
    let parents = reader_parents_created_for_test();
    let altered = db.execute("ALTER TABLE points ADD COLUMN note TEXT", &HashMap::new());
    let created = reader_parents_created_for_test() - parents;
    assert!(
        altered.is_ok(),
        "a local ALTER is writer work under the declared writer limit: {altered:?}"
    );
    assert_eq!(created, 0, "the ALTER opens no private reader");
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM points", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(ROWS)]]
    );
}
