//! Acceptance: "Compare two runs with increasing amounts of completed work, the identical pending
//! tail and a fixed limit. Seeking strictly after the committed position returns ordered next rows
//! without examining completed entries or neighbouring streams. Record examined-entry counts;
//! returned-row counts alone cannot prove this."

use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

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
    consumer_declarations_seek_to_the_pending_batch(false);
    consumer_declarations_seek_to_the_pending_batch(true);
}

/// Completed-history rows committed per fixture transaction.
const HISTORY_BATCH_ROWS: usize = 500;

// Exercise the consumer's complete key declarations and its batch-claim SQL,
// matching the saved consumer declarations and claim/discovery SQL.
fn consumer_declarations_seek_to_the_pending_batch(durable: bool) {
    let mut measurements = Vec::new();
    for completed in [10_i64, 2_010] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("consumer-resume.redb");
        let database = if durable {
            contextdb_engine::Database::open_with_options(
                &path,
                contextdb_engine::DatabaseOpenOptions {
                    maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
                    memory_limit: Some(64 << 20),
                    ..Default::default()
                },
            )
            .unwrap()
        } else {
            Database::open_memory()
        };
        for ddl in [
            "CREATE TABLE contexts (id UUID PRIMARY KEY)",
            "CREATE TABLE event_search_generations (id UUID PRIMARY KEY, context_id UUID NOT NULL, UNIQUE (context_id, id))",
            "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID NOT NULL, UNIQUE (context_id, id))",
            "CREATE TABLE event_interpretation_values (context_id UUID NOT NULL, generation_id UUID NOT NULL, event_id UUID NOT NULL, PRIMARY KEY (context_id, generation_id, event_id), UNIQUE (context_id, generation_id, event_id))",
            "CREATE TABLE event_search_source_events (membership_id UUID NOT NULL UNIQUE, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), generation_id UUID NOT NULL REFERENCES event_search_generations(id), ordinal INTEGER NOT NULL, event_id UUID NOT NULL REFERENCES events(id), connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, session_ref TEXT NOT NULL, source_sequence INTEGER NOT NULL, effective_interpretation_generation_id UUID, captured_source_sha256 TEXT NOT NULL, effective_source_sha256 TEXT NOT NULL, PRIMARY KEY (context_id, generation_id, ordinal), UNIQUE (context_id, generation_id, event_id), FOREIGN KEY (context_id, generation_id) REFERENCES event_search_generations(context_id, id), FOREIGN KEY (context_id, event_id) REFERENCES events(context_id, id), FOREIGN KEY (context_id, effective_interpretation_generation_id, event_id) REFERENCES event_interpretation_values(context_id, generation_id, event_id)) IMMUTABLE SYNC OFF",
            "CREATE INDEX idx_event_search_source_events_generation ON event_search_source_events (generation_id, context_id, ordinal)",
            "CREATE INDEX idx_event_search_source_events_cursor_range ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, source_sequence, ordinal)",
            "CREATE INDEX idx_event_search_source_events_session_claim ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, session_ref, ordinal)",
            "CREATE INDEX idx_event_search_source_events_session_sequence ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, session_ref, source_sequence)",
        ] {
            database.execute(ddl, &HashMap::new()).unwrap();
        }
        let context = Uuid::from_u128(7);
        let generation = Uuid::from_u128(11);
        let neighbour_context = Uuid::from_u128(8);
        let neighbour_generation = Uuid::from_u128(12);
        // The completed history commits in bounded batches: the fixture's
        // 64 MiB writer funds one batch at a time, never the whole prefix in
        // one transaction. Every row is committed before the seek is measured.
        let transaction = std::cell::Cell::new(database.begin_or_panic());
        let staged = std::cell::Cell::new(0_usize);
        let settle = || {
            staged.set(staged.get() + 1);
            if staged.get().is_multiple_of(HISTORY_BATCH_ROWS) {
                database.commit(transaction.get()).unwrap();
                transaction.set(database.begin_or_panic());
            }
        };
        for (context, generation) in [
            (context, generation),
            (neighbour_context, neighbour_generation),
        ] {
            database
                .execute_in_tx(
                    transaction.get(),
                    "INSERT INTO contexts VALUES ($context)",
                    &HashMap::from([("context".into(), Value::Uuid(context))]),
                )
                .unwrap();
            database
                .execute_in_tx(
                    transaction.get(),
                    "INSERT INTO event_search_generations VALUES ($generation,$context)",
                    &HashMap::from([
                        ("context".into(), Value::Uuid(context)),
                        ("generation".into(), Value::Uuid(generation)),
                    ]),
                )
                .unwrap();
        }
        let insert = |id: u128,
                      context: Uuid,
                      generation: Uuid,
                      connector: &str,
                      source: &str,
                      session: &str,
                      ordinal: i64,
                      sequence: i64| {
            let event = Uuid::from_u128(id);
            database
                .execute_in_tx(
                    transaction.get(),
                    "INSERT INTO events VALUES ($event,$context)",
                    &HashMap::from([
                        ("event".into(), Value::Uuid(event)),
                        ("context".into(), Value::Uuid(context)),
                    ]),
                )
                .unwrap();
            database.execute_in_tx(transaction.get(),
                "INSERT INTO event_search_source_events VALUES ($membership,$context,$generation,$ordinal,$event,$connector,$source,$session,$sequence,NULL,'captured','effective')",
                &HashMap::from([
                    ("membership".into(), Value::Uuid(Uuid::from_u128(id + 10_000_000))),
                    ("context".into(), Value::Uuid(context)),
                    ("generation".into(), Value::Uuid(generation)),
                    ("ordinal".into(), Value::Int64(ordinal)),
                    ("event".into(), Value::Uuid(event)),
                    ("connector".into(), Value::Text(connector.into())),
                    ("source".into(), Value::Text(source.into())),
                    ("session".into(), Value::Text(session.into())),
                    ("sequence".into(), Value::Int64(sequence)),
                ])).unwrap();
            settle();
        };
        let mut id = 100_u128;
        for (stream, (row_context, row_generation, connector, source, session)) in [
            (context, generation, "connector", "source", "selected"),
            (context, generation, "connector", "source", "neighbour"),
            (context, generation, "connector", "other-source", "selected"),
            (context, generation, "other-connector", "source", "selected"),
            (
                neighbour_context,
                neighbour_generation,
                "connector",
                "source",
                "selected",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            for offset in 0..completed {
                let ordinal = if stream == 0 {
                    100_000 - completed + offset
                } else {
                    stream as i64 * 1_000_000 + offset
                };
                insert(
                    id,
                    row_context,
                    row_generation,
                    connector,
                    source,
                    session,
                    ordinal,
                    if stream == 1 {
                        completed + offset + 1
                    } else {
                        offset + 1
                    },
                );
                id += 1;
            }
        }
        for offset in 0..4_i64 {
            insert(
                1_000_000 + offset as u128,
                context,
                generation,
                "connector",
                "source",
                "selected",
                100_000 + offset,
                100_000 + offset + 1,
            );
        }
        database.commit(transaction.get()).unwrap();
        let measured = if durable {
            database
                .execute(
                    "CREATE TABLE durable_resume_pressure (id INT PRIMARY KEY, body TEXT) SYNC OFF",
                    &HashMap::new(),
                )
                .unwrap();
            let pressure = database.begin_or_panic();
            for id in 0..128 {
                database
                    .execute_in_tx(
                        pressure,
                        "INSERT INTO durable_resume_pressure VALUES ($id, $body)",
                        &HashMap::from([
                            ("id".into(), Value::Int64(id)),
                            ("body".into(), Value::Text("p".repeat(65536))),
                        ]),
                    )
                    .unwrap();
            }
            database.commit(pressure).unwrap();
            database.close().unwrap();
            drop(database);
            let filter = format!(
                "{}::durable_consumer_reader_child",
                module_path!().split_once("::").unwrap().1
            );
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &filter, "--nocapture"])
                .env("CONTEXTDB_DURABLE_RESUME_FIXTURE", &path)
                .env("CONTEXTDB_DURABLE_RESUME_COMPLETED", completed.to_string())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "fresh durable resume child: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            let record = stdout
                .lines()
                .find_map(|line| line.strip_prefix("DURABLE_RESUME "))
                .expect("fresh child work record");
            serde_json::from_str::<(u64, u64)>(record).unwrap()
        } else {
            measure_consumer_tail(&database, completed)
        };
        measurements.push(measured);
    }
    assert_eq!(
        measurements[0], measurements[1],
        "the declared consumer batch claim must not examine completed entries or neighbouring sessions as the completed prefix grows: {measurements:?}"
    );
    assert_eq!(
        measurements[0],
        (2, 1),
        "only the fixed pending batch may be examined"
    );
}

fn measure_consumer_tail(database: &Database, completed: i64) -> (u64, u64) {
    let context = Uuid::from_u128(7);
    let generation = Uuid::from_u128(11);
    let result = database.execute(
            "SELECT membership_id, ordinal, event_id, connector_id, source_instance_ref, session_ref, source_sequence, effective_interpretation_generation_id, captured_source_sha256, effective_source_sha256 FROM event_search_source_events WHERE context_id = $context_id AND generation_id = $generation_id AND connector_id = $connector_id AND source_instance_ref = $source AND session_ref = $session AND ordinal >= $start_ordinal ORDER BY ordinal ASC LIMIT 2",
            &HashMap::from([
                ("context_id".into(),Value::Uuid(context)),
                ("generation_id".into(),Value::Uuid(generation)),
                ("connector_id".into(),Value::Text("connector".into())),
                ("source".into(),Value::Text("source".into())),
                ("session".into(),Value::Text("selected".into())),
                ("start_ordinal".into(),Value::Int64(100_000)),
            ])).unwrap();
    let expected = (0..2_i64)
        .map(|offset| {
            vec![
                Value::Uuid(Uuid::from_u128(11_000_000 + offset as u128)),
                Value::Int64(100_000 + offset),
                Value::Uuid(Uuid::from_u128(1_000_000 + offset as u128)),
                Value::Text("connector".into()),
                Value::Text("source".into()),
                Value::Text("selected".into()),
                Value::Int64(100_000 + offset + 1),
                Value::Null,
                Value::Text("captured".into()),
                Value::Text("effective".into()),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        result.rows, expected,
        "the batch claim returns only its ordered pending session tail"
    );
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some("idx_event_search_source_events_session_claim")
    );
    assert!(result.trace.sort_elided);
    let discovery = database.execute(
            "SELECT ordinal FROM event_search_source_events WHERE context_id = $context_id AND generation_id = $generation_id AND connector_id = $connector_id AND source_instance_ref = $source AND session_ref = $session AND source_sequence > $committed ORDER BY source_sequence ASC LIMIT 1",
            &HashMap::from([
                ("context_id".into(), Value::Uuid(context)),
                ("generation_id".into(), Value::Uuid(generation)),
                ("connector_id".into(), Value::Text("connector".into())),
                ("source".into(), Value::Text("source".into())),
                ("session".into(), Value::Text("selected".into())),
                ("committed".into(), Value::Int64(completed)),
            ]),
        ).unwrap();
    assert_eq!(discovery.rows, vec![vec![Value::Int64(100_000)]]);
    assert_eq!(
        discovery.trace.index_used.as_deref(),
        Some("idx_event_search_source_events_session_sequence")
    );
    assert!(discovery.trace.sort_elided);
    (result.trace.rows_examined, discovery.trace.rows_examined)
}
#[test]
fn durable_consumer_reader_child() {
    let Some(path) = std::env::var_os("CONTEXTDB_DURABLE_RESUME_FIXTURE") else {
        return;
    };
    let completed = std::env::var("CONTEXTDB_DURABLE_RESUME_COMPLETED")
        .unwrap()
        .parse::<i64>()
        .unwrap();
    let database = Database::open_with_options(
        &path,
        contextdb_engine::DatabaseOpenOptions {
            memory_limit: Some(64 << 20),
            maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    let before = database.__writer_cache_stats_for_test().unwrap();
    for id in 0..128 {
        assert_eq!(
            database
                .execute(
                    &format!("SELECT body FROM durable_resume_pressure WHERE id = {id}"),
                    &HashMap::new()
                )
                .unwrap()
                .rows,
            vec![vec![Value::Text("p".repeat(65536))]]
        );
    }
    let displaced = database.__writer_cache_stats_for_test().unwrap();
    assert!(displaced.1 > before.1 && displaced.2 > before.2);
    let measured = measure_consumer_tail(&database, completed);
    assert!(
        database.__writer_cache_stats_for_test().unwrap().1 > displaced.1,
        "saved consumer index is read after real cache displacement"
    );
    println!(
        "DURABLE_RESUME {}",
        serde_json::to_string(&measured).unwrap()
    );
    database.close().unwrap();
}
