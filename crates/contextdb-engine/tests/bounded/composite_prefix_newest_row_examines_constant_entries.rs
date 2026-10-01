//! A newest-event lookup must cost the stream, not the surrounding history.
//!
//! Context Graph appends an event by asking for the newest event on a
//! `(context_id, stream_id)` prefix.  The composite index already orders the
//! suffix it needs, so this lookup must reverse-walk that one prefix.  A
//! table scan is not made acceptable by returning the right event: at 20,010
//! rows it turns one append into work proportional to the whole stream.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const SMALL_STREAM_ROWS: i64 = 10;
const LARGE_STREAM_ROWS: i64 = 20_010;
const NEIGHBOUR_ROWS: i64 = 20_000;
const CONNECTOR_ID: &str = "test.connector";

#[path = "newest_event_merges_staged_order_without_sort.rs"]
mod newest_event_merges_staged_order_without_sort;

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn stream_id(value: u128) -> Uuid {
    Uuid::from_u128(0xC0DE_0000_0000_0000_0000_0000_0000_0000 + value)
}

fn event_id(stream: Uuid, sequence: i64) -> Uuid {
    Uuid::from_u128(stream.as_u128().wrapping_add((sequence as u128 + 1) << 64))
}

fn new_database() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID NOT NULL, connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, seq INTEGER NOT NULL, payload TEXT, UNIQUE (context_id, connector_id, source_instance_ref, seq))",
            &HashMap::new(),
        )
        .expect("create the Context Graph append fixture with its stream uniqueness constraint");
    database
        .execute(
            "CREATE INDEX idx_events_source_stream ON events (context_id, connector_id, source_instance_ref, seq)",
            &HashMap::new(),
        )
        .expect("create Context Graph's declared source-stream index");
    database
}

fn insert_event(database: &Database, context_id: Uuid, stream: Uuid, sequence: i64, payload: &str) {
    database
        .execute(
            "INSERT INTO events (id, context_id, connector_id, source_instance_ref, seq, payload) \
             VALUES ($id, $context_id, $connector_id, $source_instance_ref, $seq, $payload)",
            &params([
                ("id", Value::Uuid(event_id(stream, sequence))),
                ("context_id", Value::Uuid(context_id)),
                ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                ("source_instance_ref", Value::Text(stream.to_string())),
                ("seq", Value::Int64(sequence)),
                ("payload", Value::Text(payload.to_owned())),
            ]),
        )
        .unwrap_or_else(|error| panic!("insert event {stream}/{sequence}: {error:?}"));
}

fn seed_stream(database: &Database, context_id: Uuid, stream: Uuid, rows: i64) {
    for sequence in 0..rows {
        insert_event(database, context_id, stream, sequence, "committed");
    }
}

fn newest_sql() -> &'static str {
    "SELECT seq, payload FROM events \
     WHERE context_id = $context_id AND connector_id = $connector_id \
       AND source_instance_ref = $source_instance_ref \
     ORDER BY seq DESC LIMIT 1"
}

fn newest(database: &Database, context_id: Uuid, stream: Uuid) -> contextdb_engine::QueryResult {
    database
        .execute(
            newest_sql(),
            &params([
                ("context_id", Value::Uuid(context_id)),
                ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                ("source_instance_ref", Value::Text(stream.to_string())),
            ]),
        )
        .expect("read the newest committed event")
}

fn asserted_newest(
    database: &Database,
    result: &contextdb_engine::QueryResult,
    expected_sequence: i64,
    arm: &str,
) {
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int64(expected_sequence),
            Value::Text("committed".to_owned())
        ]],
        "{arm} returns the stream's newest committed event"
    );
    assert!(
        result.trace.sort_elided,
        "{arm} lets the automatic unique source-stream index supply descending sequence order; trace={:?}",
        result.trace
    );
    let automatic_stream_index = database
        .table_meta("events")
        .expect("read the append fixture metadata")
        .indexes
        .into_iter()
        .find(|index| {
            index.kind == contextdb_core::IndexKind::Auto
                && index.columns.iter().map(|(column, _)| column.as_str()).eq([
                    "context_id",
                    "connector_id",
                    "source_instance_ref",
                    "seq",
                ])
        })
        .expect("the UNIQUE source stream creates its automatic composite index");
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some(automatic_stream_index.name.as_str()),
        "{arm} chooses the automatic UNIQUE index before the equivalent declared index"
    );
}

fn staged_newest(
    database: &Database,
    context_id: Uuid,
    stream: Uuid,
    committed_rows: i64,
    arm: &str,
) -> contextdb_engine::QueryResult {
    let transaction = database.begin_or_panic();
    let staged_sequence = committed_rows;
    database
        .execute_in_tx(
            transaction,
            "INSERT INTO events (id, context_id, connector_id, source_instance_ref, seq, payload) \
             VALUES ($id, $context_id, $connector_id, $source_instance_ref, $seq, $payload)",
            &params([
                ("id", Value::Uuid(event_id(stream, staged_sequence))),
                ("context_id", Value::Uuid(context_id)),
                ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                ("source_instance_ref", Value::Text(stream.to_string())),
                ("seq", Value::Int64(staged_sequence)),
                ("payload", Value::Text("staged".to_owned())),
            ]),
        )
        .unwrap_or_else(|error| panic!("stage {arm} newer event: {error:?}"));
    let staged = database
        .execute_in_tx(
            transaction,
            newest_sql(),
            &params([
                ("context_id", Value::Uuid(context_id)),
                ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                ("source_instance_ref", Value::Text(stream.to_string())),
            ]),
        )
        .unwrap_or_else(|error| {
            panic!("read {arm} newest event through its transaction overlay: {error:?}")
        });
    assert_eq!(
        staged.rows,
        vec![vec![
            Value::Int64(staged_sequence),
            Value::Text("staged".to_owned())
        ]],
        "the {arm} transaction-open reverse walk merges its staged newer event"
    );
    assert!(
        staged.trace.sort_elided,
        "the {arm} transaction keeps declared-index sort elision while merging its staged row; trace={:?}",
        staged.trace
    );
    database
        .rollback(transaction)
        .unwrap_or_else(|error| panic!("discard the {arm} staged fixture row: {error:?}"));
    staged
}

#[test]
fn composite_prefix_newest_row_examines_constant_entries() {
    let context_id = stream_id(1);
    let target_stream = stream_id(2);

    let small = new_database();
    seed_stream(&small, context_id, target_stream, SMALL_STREAM_ROWS);
    let small_result = newest(&small, context_id, target_stream);
    asserted_newest(
        &small,
        &small_result,
        SMALL_STREAM_ROWS - 1,
        "the ten-event stream",
    );

    let large = new_database();
    seed_stream(&large, context_id, target_stream, LARGE_STREAM_ROWS);
    let large_result = newest(&large, context_id, target_stream);
    asserted_newest(
        &large,
        &large_result,
        LARGE_STREAM_ROWS - 1,
        "the 20,010-event stream",
    );

    let neighbours = new_database();
    let neighbour_stream = stream_id(3);
    seed_stream(&neighbours, context_id, neighbour_stream, NEIGHBOUR_ROWS);
    insert_event(&neighbours, context_id, target_stream, 0, "committed");
    let neighbour_result = newest(&neighbours, context_id, target_stream);
    asserted_newest(
        &neighbours,
        &neighbour_result,
        0,
        "a fresh stream beside 20,000 same-context events",
    );

    assert_eq!(
        small_result.trace.rows_examined, large_result.trace.rows_examined,
        "the 10- and 20,010-event streams examine the same number of index entries"
    );
    assert_eq!(
        small_result.trace.rows_examined, neighbour_result.trace.rows_examined,
        "a stream's lookup ignores the 20,000 same-context events in another stream"
    );

    let small_staged = staged_newest(
        &small,
        context_id,
        target_stream,
        SMALL_STREAM_ROWS,
        "ten-event stream",
    );
    let large_staged = staged_newest(
        &large,
        context_id,
        target_stream,
        LARGE_STREAM_ROWS,
        "20,010-event stream",
    );
    let neighbour_staged = staged_newest(
        &neighbours,
        context_id,
        target_stream,
        1,
        "fresh stream beside 20,000 same-context events",
    );

    assert_eq!(
        small_staged.trace.rows_examined, large_staged.trace.rows_examined,
        "transactional lookup examines the same number of entries for 10 and 20,010 committed stream rows"
    );
    assert_eq!(
        small_staged.trace.rows_examined, neighbour_staged.trace.rows_examined,
        "transactional lookup ignores 20,000 same-context rows in another stream"
    );
    for (outside, staged, arm) in [
        (&small_result, &small_staged, "ten-event stream"),
        (&large_result, &large_staged, "20,010-event stream"),
        (
            &neighbour_result,
            &neighbour_staged,
            "fresh stream beside 20,000 same-context events",
        ),
    ] {
        assert!(
            staged.trace.rows_examined >= outside.trace.rows_examined
                && staged.trace.rows_examined <= outside.trace.rows_examined + 1,
            "the {arm} transactional lookup examines its committed reverse-walk entries plus at most its one staged row; outside={:?}, staged={:?}",
            outside.trace,
            staged.trace
        );
    }
}

#[test]
fn historical_in_list_rebuild_seeks_each_declared_run_from_its_own_edge() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE historical_members (id INTEGER PRIMARY KEY, key_value INTEGER NOT NULL, tie INTEGER NOT NULL) HISTORY ALL",
            &HashMap::new(),
        )
        .expect("create the historical membership fixture");
    database
        .execute(
            "CREATE INDEX historical_members_key ON historical_members (key_value, tie)",
            &HashMap::new(),
        )
        .expect("index the historical membership fixture");
    for (id, key_value, tie) in [(1_i64, 10_i64, 1_i64), (2, 90, 2)] {
        database
            .execute(
                "INSERT INTO historical_members (id, key_value, tie) VALUES ($id, $key_value, $tie)",
                &params([
                    ("id", Value::Int64(id)),
                    ("key_value", Value::Int64(key_value)),
                    ("tie", Value::Int64(tie)),
                ]),
            )
            .expect("insert a member visible to the historical snapshot");
    }
    let historical = database.snapshot();

    // Publish a newer membership root before opening the old read. With no
    // reader holding the predecessor root, the bounded cursor must rebuild
    // its historical image from the versioned postings.
    database
        .execute(
            "INSERT INTO historical_members (id, key_value, tie) VALUES (3, 50, 3)",
            &HashMap::new(),
        )
        .expect("supersede the current membership root");

    let result = database
        .execute_at_snapshot(
            "SELECT id, key_value FROM historical_members WHERE key_value IN ($high, $low)",
            &params([("high", Value::Int64(90)), ("low", Value::Int64(10))]),
            historical,
        )
        .expect("rebuild and read both historical IN-list runs");
    let mut ids = result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id), Value::Int64(_)] => *id,
            other => panic!("expected historical id and key, got {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 2],
        "an old-snapshot IN(high, low) rebuild keeps both independently sought runs"
    );
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some("historical_members_key")
    );
    assert_eq!(
        result.trace.rows_examined, 4,
        "the rebuild and answer each touch exactly the two listed postings, never the gap or the newer root"
    );
}

#[test]
fn transaction_ordered_merge_uses_every_directed_sort_key() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE staged_order (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, major INTEGER NOT NULL, minor INTEGER, label TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("create the staged ordering fixture");
    database
        .execute(
            "CREATE INDEX staged_order_full ON staged_order (scope DESC, major, minor DESC)",
            &HashMap::new(),
        )
        .expect("create the mixed-direction ordering index");
    database
        .execute(
            "INSERT INTO staged_order VALUES (1, 'fixed', 7, 5, 'committed-five')",
            &HashMap::new(),
        )
        .expect("insert the committed middle row");

    let transaction = database.begin_or_panic();
    for sql in [
        "INSERT INTO staged_order VALUES (2, 'fixed', 7, 1, 'staged-one')",
        "INSERT INTO staged_order VALUES (3, 'fixed', 7, NULL, 'staged-null')",
    ] {
        database
            .execute_in_tx(transaction, sql, &HashMap::new())
            .expect("stage one tied ordering row");
    }
    let result = database
        .execute_in_tx(
            transaction,
            "SELECT label, minor FROM staged_order WHERE scope = $scope \
             ORDER BY scope DESC, major ASC, minor DESC",
            &params([("scope", Value::Text("fixed".to_owned()))]),
        )
        .expect("merge staged and committed rows in the index order");
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Text("staged-null".to_owned()), Value::Null],
            vec![Value::Text("committed-five".to_owned()), Value::Int64(5),],
            vec![Value::Text("staged-one".to_owned()), Value::Int64(1)],
        ],
        "the pinned leading key and tied middle key defer to the final DESC NULL-first key for both staged sorting and committed merging"
    );
    assert!(
        result.trace.sort_elided,
        "the full mixed-direction declared order remains index supplied; trace={:?}",
        result.trace
    );
    database
        .rollback(transaction)
        .expect("discard the staged ordering fixture");
}

fn assert_cutoff_answer(
    result: &contextdb_engine::QueryResult,
    expected_index: &str,
    key_column: &str,
) {
    assert_eq!(
        result.rows,
        vec![vec![Value::Int64(3), Value::Int64(10)]],
        "the {key_column} reverse walk returns the row at the inclusive cutoff"
    );
    assert_eq!(result.trace.index_used.as_deref(), Some(expected_index));
    assert!(
        result.trace.sort_elided,
        "the {key_column} reverse walk remains index supplied; trace={:?}",
        result.trace
    );
}

#[test]
fn reverse_range_seek_ignores_twenty_thousand_keys_above_the_cutoff() {
    const CUTOFF: i64 = 10;
    const KEYS_ABOVE: i64 = 20_000;

    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE cutoff_rows (id INTEGER PRIMARY KEY, recorded_at INTEGER NOT NULL, composite_key INTEGER NOT NULL, tie INTEGER NOT NULL)",
            &HashMap::new(),
        )
        .expect("create the reverse-range fixture");
    database
        .execute(
            "CREATE INDEX cutoff_rows_recorded ON cutoff_rows (recorded_at)",
            &HashMap::new(),
        )
        .expect("create the single-column cutoff index");
    database
        .execute(
            "CREATE INDEX cutoff_rows_composite ON cutoff_rows (composite_key, tie)",
            &HashMap::new(),
        )
        .expect("create the composite cutoff index");
    database
        .execute(
            "INSERT INTO cutoff_rows VALUES (1, 8, 8, 1), (2, 9, 9, 2), (3, 10, 10, 3)",
            &HashMap::new(),
        )
        .expect("insert rows at and below the cutoff");

    let cutoff = params([("cutoff", Value::Int64(CUTOFF))]);
    let single_sql = "SELECT id, recorded_at FROM cutoff_rows \
                      WHERE recorded_at <= $cutoff \
                      ORDER BY recorded_at DESC LIMIT 1";
    let composite_sql = "SELECT id, composite_key FROM cutoff_rows \
                         WHERE composite_key <= $cutoff \
                         ORDER BY composite_key DESC LIMIT 1";
    let small_single = database
        .execute(single_sql, &cutoff)
        .expect("read the small single-column cutoff");
    let small_composite = database
        .execute(composite_sql, &cutoff)
        .expect("read the small composite cutoff");
    assert_cutoff_answer(&small_single, "cutoff_rows_recorded", "recorded_at");
    assert_cutoff_answer(&small_composite, "cutoff_rows_composite", "composite_key");

    let transaction = database.begin_or_panic();
    for first in (CUTOFF + 1..=CUTOFF + KEYS_ABOVE).step_by(500) {
        let last = (first + 499).min(CUTOFF + KEYS_ABOVE);
        let values = (first..=last)
            .map(|key| {
                let id = 100_000 + key;
                format!("({id}, {key}, {key}, {key})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        database
            .execute_in_tx(
                transaction,
                &format!("INSERT INTO cutoff_rows VALUES {values}"),
                &HashMap::new(),
            )
            .expect("stage a chunk of keys above the cutoff");
    }
    database
        .commit(transaction)
        .expect("publish all keys above the cutoff");

    let large_single = database
        .execute(single_sql, &cutoff)
        .expect("read the large single-column cutoff");
    let large_composite = database
        .execute(composite_sql, &cutoff)
        .expect("read the large composite cutoff");
    assert_cutoff_answer(&large_single, "cutoff_rows_recorded", "recorded_at");
    assert_cutoff_answer(&large_composite, "cutoff_rows_composite", "composite_key");

    assert_eq!(
        (
            large_single.trace.rows_examined,
            large_composite.trace.rows_examined,
        ),
        (
            small_single.trace.rows_examined,
            small_composite.trace.rows_examined,
        ),
        "neither the full-width nor padded composite reverse seek examines any of the 20,000 keys above its upper edge; small=({:?}, {:?}), large=({:?}, {:?})",
        small_single.trace,
        small_composite.trace,
        large_single.trace,
        large_composite.trace,
    );
    assert_eq!(
        (
            large_single.trace.rows_examined,
            large_composite.trace.rows_examined,
        ),
        (1, 1),
        "each bounded reverse seek touches only its answer"
    );
}
