//! Consumers receive exact stream counts and seek pending streams through declared keys.
#![cfg(feature = "test-seams")]

use contextdb_core::{SortDirection, Value};
use contextdb_engine::maintained_counts::MaintainedCountDisclosure;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).into(), value.clone()))
        .collect()
}

fn execute(db: &Database, sql: &str, bindings: &HashMap<String, Value>) -> QueryResult {
    db.execute(sql, bindings)
        .unwrap_or_else(|error| panic!("fixture statement {sql}: {error}"))
}

fn context(n: u128) -> Value {
    Value::Uuid(Uuid::from_u128(n))
}

fn open(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the consumer fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn contexts(db: &Database) {
    execute(
        db,
        "CREATE TABLE contexts (id UUID PRIMARY KEY) SYNC OFF",
        &HashMap::new(),
    );
    for id in [1, 2, 3] {
        execute(
            db,
            "INSERT INTO contexts VALUES ($id)",
            &params(&[("id", context(id))]),
        );
    }
}

const GROUP_COUNT: &str = "SELECT connector_id, source_instance_ref, COUNT(*) AS event_count FROM events WHERE context_id = $context_id GROUP BY connector_id, source_instance_ref";

fn stream_counts(db: &Database, id: u128, expected: &[(&str, &str, i64)]) -> u64 {
    let (answer, work) = contextdb_engine::writer_query_test_seams::observe(|| {
        execute(db, GROUP_COUNT, &params(&[("context_id", context(id))]))
    });
    assert_eq!(
        answer.columns,
        ["connector_id", "source_instance_ref", "event_count"]
    );
    let mut actual = answer
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [
                Value::Text(connector),
                Value::Text(source),
                Value::Int64(count),
            ] => (connector.clone(), source.clone(), *count),
            other => panic!("each stream has its exact integer count: {other:?}"),
        })
        .collect::<Vec<_>>();
    actual.sort();
    let expected = expected
        .iter()
        .map(|(connector, source, count)| (connector.to_string(), source.to_string(), *count))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(
        answer.trace.maintained_count.as_deref(),
        Some(&MaintainedCountDisclosure::Used {
            table: "events".into(),
            columns: vec![
                ("context_id".into(), SortDirection::Asc),
                ("connector_id".into(), SortDirection::Asc),
                ("source_instance_ref".into(), SortDirection::Asc),
            ],
        }),
        "the declared count answers the grouped read"
    );
    assert_eq!(
        work.history.row_versions, 0,
        "grouped maintained counts do not decode event bodies"
    );
    println!(
        "grouped stream count rows_examined={} history={:?}",
        answer.trace.rows_examined, work.history
    );
    answer.trace.rows_examined
}

#[test]
#[allow(clippy::erasing_op, clippy::identity_op)] // Preserve the existing test expression and its assertion contract.
fn grouped_event_counts_use_the_declared_context_stream_count_and_survive_reopen() {
    let count_columns = vec![
        ("context_id".into(), SortDirection::Asc),
        ("generation_id".into(), SortDirection::Asc),
        ("connector_id".into(), SortDirection::Asc),
        ("source_instance_ref".into(), SortDirection::Asc),
        ("session_ref".into(), SortDirection::Asc),
    ];
    let count_sql = "SELECT COUNT(*) AS event_count FROM event_search_source_events WHERE context_id = $context_id AND generation_id = $generation_id AND connector_id = $connector AND source_instance_ref = $source AND session_ref = $session";
    let count_bindings = params(&[
        ("context_id", context(1)),
        ("generation_id", context(10)),
        ("connector", Value::Text("claim-fixture".into())),
        ("source", Value::Text("selected-source".into())),
        ("session", Value::Text("selected-session".into())),
    ]);
    let count_schema = "CREATE TABLE event_search_source_events (membership_id UUID NOT NULL, context_id UUID NOT NULL, generation_id UUID NOT NULL, ordinal INTEGER NOT NULL, event_id UUID NOT NULL, connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, session_ref TEXT NOT NULL, source_sequence INTEGER NOT NULL, effective_interpretation_generation_id UUID, captured_source_sha256 TEXT NOT NULL, effective_source_sha256 TEXT NOT NULL, PRIMARY KEY (context_id, generation_id, ordinal)) IMMUTABLE SYNC OFF";
    let count_indexes = [
        "CREATE INDEX idx_event_search_source_events_generation ON event_search_source_events (generation_id, context_id, ordinal)",
        "CREATE INDEX idx_event_search_source_events_cursor_range ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, source_sequence, ordinal)",
        "CREATE INDEX idx_event_search_source_events_session_claim ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, session_ref, ordinal)",
        "CREATE INDEX idx_event_search_source_events_session_sequence ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, session_ref, source_sequence)",
    ];
    let count_declaration = "ALTER TABLE event_search_source_events SET MAINTAIN COUNT (context_id, generation_id, connector_id, source_instance_ref, session_ref)";
    let count_insert = "INSERT INTO event_search_source_events (membership_id, context_id, generation_id, ordinal, event_id, connector_id, source_instance_ref, session_ref, source_sequence, effective_interpretation_generation_id, captured_source_sha256, effective_source_sha256) VALUES ($membership, $context_id, $generation_id, $ordinal, $event, $connector, $source, $session, $sequence, NULL, 'captured', 'effective')";

    // A historical caller cannot use a current maintained total after appends.
    let historical_directory = tempfile::tempdir().unwrap();
    let historical_db = open(
        &historical_directory
            .path()
            .join("historical-stream-count.redb"),
    );
    execute(&historical_db, count_schema, &HashMap::new());
    for count_index in count_indexes {
        execute(&historical_db, count_index, &HashMap::new());
    }
    execute(&historical_db, count_declaration, &HashMap::new());
    for ordinal in 0..3_i64 {
        let mut row = count_bindings.clone();
        row.extend(params(&[
            ("membership", context(100 + ordinal as u128)),
            ("event", context(200 + ordinal as u128)),
            ("ordinal", Value::Int64(ordinal)),
            ("sequence", Value::Int64(ordinal)),
        ]));
        execute(&historical_db, count_insert, &row);
    }
    let historical_snapshot = historical_db.snapshot();
    let historical_count = execute(&historical_db, count_sql, &count_bindings).rows;
    assert_eq!(historical_count, vec![vec![Value::Int64(3)]]);
    for ordinal in 3..5_i64 {
        let mut row = count_bindings.clone();
        row.extend(params(&[
            ("membership", context(100 + ordinal as u128)),
            ("event", context(200 + ordinal as u128)),
            ("ordinal", Value::Int64(ordinal)),
            ("sequence", Value::Int64(ordinal)),
        ]));
        execute(&historical_db, count_insert, &row);
    }
    let current_count = execute(&historical_db, count_sql, &count_bindings).rows;
    assert_ne!(
        current_count, historical_count,
        "the historical fixture appends matching rows after its snapshot"
    );
    assert_eq!(
        historical_db
            .execute_at_snapshot(count_sql, &count_bindings, historical_snapshot)
            .unwrap()
            .rows,
        historical_count,
        "a historical exact-key count returns its snapshot's total, never the current root's total"
    );
    historical_db.close().unwrap();

    // Keep the selected five-event tail fixed while independently growing the
    // selected completed prefix and the four neighbouring streams.
    let mut count_observations = Vec::new();
    for (completed, neighbours) in [(10_i64, 16_i64), (10, 1_016), (2_010, 16), (2_010, 1_016)] {
        let count_directory = tempfile::tempdir().unwrap();
        let count_db = open(&count_directory.path().join("live-stream-count.redb"));
        execute(&count_db, count_schema, &HashMap::new());
        for count_index in count_indexes {
            execute(&count_db, count_index, &HashMap::new());
        }
        execute(&count_db, count_declaration, &HashMap::new());
        let mut ordinal = 0_i64;
        for (source, session, population) in [
            ("selected-source", "selected-session", completed),
            ("selected-source", "a-neighbour-session", neighbours),
            ("selected-source", "z-neighbour-session", neighbours),
            ("a-neighbour-source", "selected-session", neighbours),
            ("z-neighbour-source", "selected-session", neighbours),
            ("selected-source", "selected-session", 5),
        ] {
            for start in (0..population).step_by(64) {
                let tx = count_db.begin().unwrap();
                for sequence in start..(start + 64).min(population) {
                    let mut row = count_bindings.clone();
                    row.extend(params(&[
                        ("membership", context(100 + ordinal as u128)),
                        ("event", context(10_000 + ordinal as u128)),
                        ("ordinal", Value::Int64(ordinal)),
                        ("sequence", Value::Int64(sequence)),
                        ("source", Value::Text(source.into())),
                        ("session", Value::Text(session.into())),
                    ]));
                    count_db.execute_in_tx(tx, count_insert, &row).unwrap();
                    ordinal += 1;
                }
                count_db.commit(tx).unwrap();
            }
        }
        let answer = execute(&count_db, count_sql, &count_bindings);
        count_observations.push((
            completed,
            neighbours,
            answer.rows,
            answer.trace.maintained_count.as_deref().cloned(),
            answer.trace.rows_examined,
        ));
        count_db.close().unwrap();
    }
    // The maintained record is a real touch. Calibrate no absolute work value:
    // require the same actual work as the fresh first population arm.
    let reference_work = count_observations[0].4;
    let count_expected = [(10_i64, 16_i64), (10, 1_016), (2_010, 16), (2_010, 1_016)]
        .into_iter()
        .map(|(completed, neighbours)| {
            (
                completed,
                neighbours,
                vec![vec![Value::Int64(completed + 5)]],
                Some(MaintainedCountDisclosure::Used {
                    table: "event_search_source_events".into(),
                    columns: count_columns.clone(),
                }),
                reference_work,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        count_observations, count_expected,
        "the live five-key count is served exactly from its declaration with equal examined work across all four history populations"
    );

    let mut examined = Vec::new();
    let mut file_examined = Vec::new();
    let mut file_count_used = Vec::new();
    let mut unpinned_examined = Vec::new();
    let unpinned_query = "SELECT context_id, connector_id, source_instance_ref, COUNT(*) AS event_count FROM events GROUP BY context_id, connector_id, source_instance_ref";
    for unrelated in [32, 64] {
        let directory = tempfile::tempdir().expect("file-backed grouped count fixture");
        let path = directory.path().join("counts.db");
        let db = open(&path);
        contexts(&db);
        execute(
            &db,
            "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, seq INTEGER NOT NULL, UNIQUE (context_id, connector_id, source_instance_ref, seq)) SYNC OFF MAINTAIN COUNT (context_id), (context_id, connector_id, source_instance_ref)",
            &HashMap::new(),
        );
        for (ordinal, (connector, source)) in [
            ("alpha", "source-a"),
            ("alpha", "source-a"),
            ("alpha", "source-a"),
            ("alpha", "source-b"),
            ("alpha", "source-b"),
            ("beta", "source-a"),
        ]
        .into_iter()
        .enumerate()
        {
            execute(
                &db,
                "INSERT INTO events VALUES ($id, $context_id, $connector, $source, $seq)",
                &params(&[
                    ("id", context(100 + ordinal as u128)),
                    ("context_id", context(1)),
                    ("connector", Value::Text(connector.into())),
                    ("source", Value::Text(source.into())),
                    ("seq", Value::Int64(ordinal as i64 + 1)),
                ]),
            );
        }
        for ordinal in 0..unrelated {
            execute(
                &db,
                "INSERT INTO events VALUES ($id, $context_id, 'alpha', 'source-a', $seq)",
                &params(&[
                    ("id", context(1_000 + ordinal as u128)),
                    ("context_id", context(2)),
                    ("seq", Value::Int64(ordinal)),
                ]),
            );
        }
        let expected = [
            ("alpha", "source-a", 3),
            ("alpha", "source-b", 2),
            ("beta", "source-a", 1),
        ];
        examined.push(stream_counts(&db, 1, &expected));
        stream_counts(&db, 2, &[("alpha", "source-a", unrelated)]);
        stream_counts(&db, 3, &[]);
        execute(
            &db,
            "DELETE FROM events WHERE id = $id",
            &params(&[("id", context(100))]),
        );
        let after_delete = [
            ("alpha", "source-a", 2),
            ("alpha", "source-b", 2),
            ("beta", "source-a", 1),
        ];
        stream_counts(&db, 1, &after_delete);
        db.close().expect("close the counted publication");
        let restored = open(&path);
        stream_counts(&restored, 1, &after_delete);
        stream_counts(&restored, 2, &[("alpha", "source-a", unrelated)]);
        // Grow selected history too, keeping the same three stream groups.
        // A scan cannot pass this arm by ignoring growth in another Context.
        for ordinal in 0..unrelated {
            execute(
                &restored,
                "INSERT INTO events VALUES ($id, $context_id, 'alpha', 'source-a', $seq)",
                &params(&[
                    ("id", context(10_000 + ordinal as u128)),
                    ("context_id", context(1)),
                    ("seq", Value::Int64(10_000 + ordinal)),
                ]),
            );
        }
        restored
            .close()
            .expect("close the reopened counted publication");
        drop(restored);
        drop(db);
        let reader = contextdb_engine::ReadSession::open(&path)
            .expect("read-only open of the committed counted image");
        let cursor = reader
            .open_cursor(GROUP_COUNT, &params(&[("context_id", context(1))]))
            .expect("read grouped counts through the file route");
        assert_eq!(
            cursor.route(),
            contextdb_core::read_contract::ReadRoute::File,
            "closed writer forces an independent read-only committed-image route"
        );
        let page = cursor.first_page();
        assert!(
            !page.has_more,
            "all three groups fit the default first page"
        );
        assert_eq!(
            page.columns,
            ["connector_id", "source_instance_ref", "event_count"]
        );
        let mut rows = page.rows.clone();
        rows.sort_by(|left, right| {
            let key = |row: &[Value]| match row {
                [Value::Text(connector), Value::Text(source), Value::Int64(_)] => {
                    (connector.clone(), source.clone())
                }
                other => panic!("exact grouped output values: {other:?}"),
            };
            key(left).cmp(&key(right))
        });
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("alpha".into()),
                    Value::Text("source-a".into()),
                    Value::Int64(2 + unrelated)
                ],
                vec![
                    Value::Text("alpha".into()),
                    Value::Text("source-b".into()),
                    Value::Int64(2)
                ],
                vec![
                    Value::Text("beta".into()),
                    Value::Text("source-a".into()),
                    Value::Int64(1)
                ],
            ],
            "read-only image returns exact grown counts without changing its group set"
        );
        let trace = cursor.trace();
        println!(
            "read-only grouped count selected_history={unrelated} groups=3 rows_examined={} maintained_count={:?}",
            trace.rows_examined, trace.maintained_count
        );
        file_examined.push(trace.rows_examined);
        file_count_used.push(
            trace.maintained_count.as_deref()
                == Some(&MaintainedCountDisclosure::Used {
                    table: "events".into(),
                    columns: vec![
                        ("context_id".into(), SortDirection::Asc),
                        ("connector_id".into(), SortDirection::Asc),
                        ("source_instance_ref".into(), SortDirection::Asc),
                    ],
                }),
        );
        cursor.close().expect("close read-only count cursor");
        // With no WHERE prefix, enumerate the full declared counted list.
        let cursor = reader
            .open_cursor(unpinned_query, &HashMap::new())
            .expect("read every maintained group without a pinned prefix");
        assert_eq!(
            cursor.route(),
            contextdb_core::read_contract::ReadRoute::File
        );
        let page = cursor.first_page();
        assert!(!page.has_more);
        assert_eq!(
            page.columns,
            [
                "context_id",
                "connector_id",
                "source_instance_ref",
                "event_count"
            ]
        );
        let mut groups = page
            .rows
            .iter()
            .map(|row| match row.as_slice() {
                [
                    Value::Uuid(context),
                    Value::Text(connector),
                    Value::Text(source),
                    Value::Int64(count),
                ] => (context.as_u128(), connector.clone(), source.clone(), *count),
                other => panic!("exact unpinned grouped values: {other:?}"),
            })
            .collect::<Vec<_>>();
        groups.sort();
        assert_eq!(
            groups,
            vec![
                (1, "alpha".into(), "source-a".into(), 2 + unrelated),
                (1, "alpha".into(), "source-b".into(), 2),
                (1, "beta".into(), "source-a".into(), 1),
                (2, "alpha".into(), "source-a".into(), unrelated),
            ],
            "an empty prefix includes every group in both Contexts exactly once"
        );
        let trace = cursor.trace();
        println!(
            "read-only unpinned selected_history={unrelated} groups=4 rows_examined={} maintained_count={:?}",
            trace.rows_examined, trace.maintained_count
        );
        assert_eq!(trace.rows_examined, 2 * 4 + 1);
        assert_eq!(
            trace.maintained_count.as_deref(),
            Some(&MaintainedCountDisclosure::Used {
                table: "events".into(),
                columns: vec![
                    ("context_id".into(), SortDirection::Asc),
                    ("connector_id".into(), SortDirection::Asc),
                    ("source_instance_ref".into(), SortDirection::Asc)
                ],
            })
        );
        unpinned_examined.push(trace.rows_examined);
        cursor.close().expect("close unpinned grouped cursor");
    }
    // Adjacent groups carry interleaved trailing index values.
    {
        let directory = tempfile::tempdir().expect("trailing-column counted index fixture");
        let path = directory.path().join("trailing-counts.db");
        let db = open(&path);
        contexts(&db);
        execute(
            &db,
            "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, seq INTEGER NOT NULL, UNIQUE (context_id, connector_id, source_instance_ref, seq)) SYNC OFF MAINTAIN COUNT (context_id), (context_id, connector_id, source_instance_ref)",
            &HashMap::new(),
        );
        execute(
            &db,
            "CREATE INDEX idx_event_stream_with_sequence ON events (context_id, connector_id, source_instance_ref, seq)",
            &HashMap::new(),
        );
        db.close().expect("publish the empty counted table");
        drop(db);
        let empty_reader =
            contextdb_engine::ReadSession::open(&path).expect("read-only empty counted image");
        let empty_cursor = empty_reader
            .open_cursor(unpinned_query, &HashMap::new())
            .expect("unpinned GROUP BY on an empty table");
        assert_eq!(
            empty_cursor.route(),
            contextdb_core::read_contract::ReadRoute::File
        );
        let empty_page = empty_cursor.first_page();
        assert_eq!(
            empty_page.columns,
            [
                "context_id",
                "connector_id",
                "source_instance_ref",
                "event_count"
            ]
        );
        assert!(
            empty_page.rows.is_empty(),
            "an empty table creates no counted group"
        );
        assert!(!empty_page.has_more);
        let empty_trace = empty_cursor.trace();
        println!(
            "read-only unpinned empty groups=0 rows_examined={} maintained_count={:?}",
            empty_trace.rows_examined, empty_trace.maintained_count
        );
        assert_eq!(empty_trace.rows_examined, 2 * 0 + 1);
        assert_eq!(
            empty_trace.maintained_count.as_deref(),
            Some(&MaintainedCountDisclosure::Used {
                table: "events".into(),
                columns: vec![
                    ("context_id".into(), SortDirection::Asc),
                    ("connector_id".into(), SortDirection::Asc),
                    ("source_instance_ref".into(), SortDirection::Asc)
                ],
            })
        );
        empty_cursor.close().expect("close empty grouped cursor");
        drop(empty_reader);
        let db = open(&path);
        for (source, seq) in [
            ("source-a", 1),
            ("source-b", 2),
            ("source-a", 3),
            ("source-b", 4),
            ("source-a", 5),
        ] {
            execute(
                &db,
                "INSERT INTO events VALUES ($id, $context_id, 'alpha', $source, $seq)",
                &params(&[
                    ("id", context(20_000 + seq as u128)),
                    ("context_id", context(1)),
                    ("source", Value::Text(source.into())),
                    ("seq", Value::Int64(seq)),
                ]),
            );
        }
        db.close().expect("publish trailing-column counted image");
        drop(db);
        let reader = contextdb_engine::ReadSession::open(&path)
            .expect("read-only trailing-column counted image");
        let cursor = reader
            .open_cursor(GROUP_COUNT, &params(&[("context_id", context(1))]))
            .expect("count adjacent groups through trailing-column index");
        assert_eq!(
            cursor.route(),
            contextdb_core::read_contract::ReadRoute::File
        );
        let page = cursor.first_page();
        assert!(!page.has_more);
        assert_eq!(
            page.columns,
            ["connector_id", "source_instance_ref", "event_count"]
        );
        let mut rows = page.rows.clone();
        rows.sort_by(|left, right| match (&left[1], &right[1]) {
            (Value::Text(left), Value::Text(right)) => left.cmp(right),
            other => panic!("exact grouped source names: {other:?}"),
        });
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("alpha".into()),
                    Value::Text("source-a".into()),
                    Value::Int64(3)
                ],
                vec![
                    Value::Text("alpha".into()),
                    Value::Text("source-b".into()),
                    Value::Int64(2)
                ],
            ],
            "interleaved trailing sequences neither merge nor skip adjacent groups"
        );
        let trace = cursor.trace();
        println!(
            "read-only trailing-column groups=2 rows_examined={} maintained_count={:?}",
            trace.rows_examined, trace.maintained_count
        );
        assert_eq!(
            trace.rows_examined,
            2 * 2 + 1,
            "each group seek and count lookup plus the terminal empty seek is charged"
        );
        assert_eq!(
            trace.maintained_count.as_deref(),
            Some(&MaintainedCountDisclosure::Used {
                table: "events".into(),
                columns: vec![
                    ("context_id".into(), SortDirection::Asc),
                    ("connector_id".into(), SortDirection::Asc),
                    ("source_instance_ref".into(), SortDirection::Asc)
                ],
            })
        );
        cursor.close().expect("close trailing-column count cursor");
    }
    assert_eq!(
        examined[0], examined[1],
        "doubling another Context does not change selected grouped-count work"
    );
    assert_eq!(
        file_examined,
        vec![2 * 3 + 1, 2 * 3 + 1],
        "each committed image charges every group seek, count lookup and terminal empty seek"
    );
    assert_eq!(
        file_examined[0], file_examined[1],
        "doubling selected event history with the same groups does not change read-only grouped-count work"
    );
    assert_eq!(unpinned_examined, vec![2 * 4 + 1, 2 * 4 + 1]);
    assert_eq!(
        unpinned_examined[0], unpinned_examined[1],
        "unpinned grouped work stays fixed as selected and other Context history grow"
    );
    assert_eq!(
        file_count_used,
        vec![true, true],
        "both committed images serve the declared maintained grouped count"
    );
}

const PENDING_INDEX: &str = "idx_event_search_pending_streams_selection";
const PENDING_QUERY: &str = "SELECT connector_id, source_instance_ref, session_ref FROM event_search_pending_streams WHERE context_id = $context_id AND generation_id = $generation_id AND pending = $pending ORDER BY connector_id ASC, session_ref ASC, source_instance_ref ASC";

fn insert_pending(
    db: &Database,
    context_id: u128,
    generation_id: u128,
    pending: bool,
    connector: &str,
    source: &str,
    session: &str,
) {
    execute(
        db,
        "INSERT INTO event_search_pending_streams VALUES ($context_id, $generation_id, $connector, $source, $session, $pending)",
        &params(&[
            ("context_id", context(context_id)),
            ("generation_id", context(generation_id)),
            ("connector", Value::Text(connector.into())),
            ("source", Value::Text(source.into())),
            ("session", Value::Text(session.into())),
            ("pending", Value::Bool(pending)),
        ]),
    );
}

fn pending_rows(db: &Database, pending: bool, expected: &[(&str, &str, &str)]) -> (u64, u64) {
    let (answer, work) = contextdb_engine::writer_query_test_seams::observe(|| {
        execute(
            db,
            PENDING_QUERY,
            &params(&[
                ("context_id", context(1)),
                ("generation_id", context(10)),
                ("pending", Value::Bool(pending)),
            ]),
        )
    });
    assert_eq!(
        answer.columns,
        ["connector_id", "source_instance_ref", "session_ref"]
    );
    let expected = expected
        .iter()
        .map(|(connector, source, session)| {
            vec![
                Value::Text(connector.to_string()),
                Value::Text(source.to_string()),
                Value::Text(session.to_string()),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        answer.rows, expected,
        "boolean equality selects exactly its declared Context and generation in index suffix order"
    );
    assert_eq!(answer.trace.index_used.as_deref(), Some(PENDING_INDEX));
    assert_eq!(answer.trace.physical_plan, "IndexScan");
    assert!(
        answer.trace.sort_elided,
        "the declared index already supplies the requested suffix order"
    );
    println!(
        "pending seek rows_examined={} history={:?}",
        answer.trace.rows_examined, work.history
    );
    (answer.trace.rows_examined, work.history.row_versions)
}

#[test]
fn boolean_pending_seek_uses_the_declared_context_generation_prefix() {
    let mut examined = Vec::new();
    for unrelated in [32, 64] {
        let directory = tempfile::tempdir().expect("file-backed boolean seek fixture");
        let path = directory.path().join("pending.db");
        let db = open(&path);
        contexts(&db);
        execute(
            &db,
            "CREATE TABLE event_search_pending_streams (context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), generation_id UUID NOT NULL, connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, session_ref TEXT NOT NULL, pending BOOLEAN NOT NULL, PRIMARY KEY (context_id, generation_id, connector_id, source_instance_ref, session_ref)) SYNC OFF",
            &HashMap::new(),
        );
        execute(
            &db,
            "CREATE INDEX idx_event_search_pending_streams_selection ON event_search_pending_streams (context_id, generation_id, pending, connector_id, session_ref, source_instance_ref)",
            &HashMap::new(),
        );
        for (connector, source, session) in [
            ("beta", "source-c", "session-a"),
            ("alpha", "source-a", "session-b"),
            ("alpha", "source-b", "session-a"),
        ] {
            insert_pending(&db, 1, 10, true, connector, source, session);
        }
        insert_pending(&db, 1, 10, false, "false", "source-f", "session-f");
        for ordinal in 0..unrelated {
            let suffix = format!("other-{ordinal:04}");
            insert_pending(&db, 2, 10, true, &suffix, "source-x", "session-x");
            insert_pending(&db, 1, 11, true, &suffix, "source-x", "session-x");
        }
        let expected = [
            ("alpha", "source-b", "session-a"),
            ("alpha", "source-a", "session-b"),
            ("beta", "source-c", "session-a"),
        ];
        let first = pending_rows(&db, true, &expected);
        assert!(first.0 > 0, "the proof records real selected seek work");
        examined.push(first);
        pending_rows(&db, false, &[("false", "source-f", "session-f")]);
        execute(
            &db,
            "UPDATE event_search_pending_streams SET pending = $pending WHERE context_id = $context_id AND generation_id = $generation_id AND connector_id = 'alpha' AND source_instance_ref = 'source-b' AND session_ref = 'session-a'",
            &params(&[
                ("pending", Value::Bool(false)),
                ("context_id", context(1)),
                ("generation_id", context(10)),
            ]),
        );
        let after_update = [
            ("alpha", "source-a", "session-b"),
            ("beta", "source-c", "session-a"),
        ];
        pending_rows(&db, true, &after_update);
        pending_rows(
            &db,
            false,
            &[
                ("alpha", "source-b", "session-a"),
                ("false", "source-f", "session-f"),
            ],
        );
        db.close().expect("close the indexed pending publication");
        let reopened = open(&path);
        pending_rows(&reopened, true, &after_update);
        reopened
            .close()
            .expect("close the reopened indexed publication");
    }
    assert_eq!(
        examined[0], examined[1],
        "doubling rows in other Contexts and generations does not change equality-seek work"
    );
}
