//! Acceptance: "Compare two runs with increasing amounts of completed work, the identical pending
//! tail and a fixed limit. Seeking strictly after the committed position returns ordered next rows
//! without examining completed entries or neighbouring streams. Record examined-entry counts;
//! returned-row counts alone cannot prove this."

use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::collections::HashMap;

#[test]
fn stream_resume_seeks_after_prefix_and_exclusive_ordinal() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let mut measurements = Vec::new();
    for completed in [10, 2_010] {
        let database = Database::open_memory();
        database.execute(
            "CREATE TABLE resume_events (id INTEGER PRIMARY KEY, context_id INTEGER NOT NULL, stream_id INTEGER NOT NULL, ordinal INTEGER NOT NULL)",
            &HashMap::new(),
        ).unwrap();
        database.execute(
            "CREATE INDEX resume_stream_position ON resume_events (context_id, stream_id, ordinal, id)",
            &HashMap::new(),
        ).unwrap();
        let transaction = database.begin_or_panic();
        // The completed ordinal grows toward the same exclusive resume boundary. Neighbours
        // share each successive leading key, on both sides of the selected stream.
        let mut id = 1;
        for (context, stream, count) in [
            (7, 8, completed),
            (7, 6, completed),
            (7, 9, completed),
            (6, 8, completed),
            (8, 8, completed),
        ] {
            for offset in 0..count {
                database
                    .execute_in_tx(
                        transaction,
                        &format!(
                            "INSERT INTO resume_events VALUES ({id}, {context}, {stream}, {})",
                            100_000 - count + offset
                        ),
                        &HashMap::new(),
                    )
                    .unwrap();
                id += 1;
            }
        }
        for (id, ordinal) in [
            (1_000_000, 100_000),
            (1_000_001, 100_000),
            (1_000_002, 100_001),
            (1_000_003, 100_002),
            (1_000_004, 100_003),
        ] {
            database
                .execute_in_tx(
                    transaction,
                    &format!("INSERT INTO resume_events VALUES ({id}, 7, 8, {ordinal})"),
                    &HashMap::new(),
                )
                .unwrap();
        }
        database.commit(transaction).unwrap();
        let mut arm = Vec::new();
        for (direction, expected) in [
            ("ASC", vec![(1_000_002, 100_001), (1_000_003, 100_002)]),
            ("DESC", vec![(1_000_003, 100_002), (1_000_002, 100_001)]),
        ] {
            let result = database.execute(
                &format!("SELECT id, ordinal FROM resume_events WHERE context_id = 7 AND stream_id = 8 AND ordinal > 100000 AND ordinal < 100003 ORDER BY ordinal {direction}, id {direction} LIMIT 2"),
                &HashMap::new(),
            ).unwrap();
            assert_eq!(
                result.rows,
                expected
                    .into_iter()
                    .map(|(id, ordinal)| vec![Value::Int64(id), Value::Int64(ordinal)])
                    .collect::<Vec<_>>(),
                "{completed}/{direction}: the exclusive ordinal excludes both boundary duplicates"
            );
            assert_eq!(
                result.trace.index_used.as_deref(),
                Some("resume_stream_position")
            );
            assert!(
                result.trace.sort_elided,
                "resume must use declared suffix order: {:?}",
                result.trace
            );
            arm.push(result.trace.rows_examined);
        }
        measurements.push(arm);
    }
    assert_eq!(
        measurements[0], measurements[1],
        "seeking after the committed ordinal must examine equal entries as the completed prefix grows; ASC and DESC must exclude completed entries and neighbours"
    );
    assert_eq!(
        measurements[0],
        vec![2, 2],
        "only the fixed two-row pending window may be examined"
    );
}
