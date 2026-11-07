use contextdb_core::{Value, read_contract::ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use std::collections::HashMap;

#[test]
fn ordered_writer_read_fetches_each_version_by_its_complete_key() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("ordered-rows.db");
    let database = Database::open(&path).expect("open writer");
    database
        .execute(
            "CREATE TABLE ordered_rows (id INTEGER PRIMARY KEY, sequence INTEGER NOT NULL, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("create table");
    database
        .execute(
            "CREATE INDEX ordered_rows_sequence ON ordered_rows (sequence)",
            &HashMap::new(),
        )
        .expect("create ordered index");
    let transaction = database.begin().expect("begin rows");
    for id in 0_i64..64 {
        database
            .execute_in_tx(
                transaction,
                "INSERT INTO ordered_rows (id, sequence, payload) VALUES ($id, $sequence, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("sequence".to_owned(), Value::Int64(63 - id)),
                    ("payload".to_owned(), Value::Text(id.to_string())),
                ]),
            )
            .expect("insert row");
    }
    database.commit(transaction).expect("commit rows");

    let reader = database
        .read_session(ReadLimits::default())
        .expect("open in-process bounded reader");
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let answer = reader
        .execute(
            "SELECT id, payload FROM ordered_rows ORDER BY sequence",
            &HashMap::new(),
        )
        .expect("read ordered rows");
    let counters = work.counters();
    assert_eq!(
        answer.trace().index_used.as_deref(),
        Some("ordered_rows_sequence")
    );
    assert_eq!(answer.rows().len(), 64);
    for (sequence, row) in answer.rows().iter().enumerate() {
        let expected_id = 63 - sequence as i64;
        assert_eq!(
            row,
            &vec![
                Value::Int64(expected_id),
                Value::Text(expected_id.to_string())
            ]
        );
    }
    assert_eq!(counters.row_version_range_reads, 0, "{counters:?}");
    assert_eq!(counters.row_version_point_reads, 64, "{counters:?}");
    drop(work);
    drop(reader);
    database.close().expect("close writer");
}

#[test]
fn ordered_anchor_lookup_seeks_the_filtered_index_run() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("anchor-rows.db");
    let database = Database::open(&path).expect("open writer");
    database
        .execute(
            "CREATE TABLE anchor_rows (id UUID PRIMARY KEY, context_id UUID NOT NULL, event_id UUID NOT NULL, declaration_version TEXT NOT NULL, key TEXT NOT NULL, identity_value TEXT NOT NULL, UNIQUE (context_id, event_id, declaration_version, key, identity_value))",
            &HashMap::new(),
        )
        .expect("create anchor table");
    database
        .execute(
            "CREATE INDEX anchor_rows_event ON anchor_rows (event_id, context_id, declaration_version, key, identity_value)",
            &HashMap::new(),
        )
        .expect("create event-first index");
    let context_id = uuid::Uuid::from_u128(1);
    let transaction = database.begin().expect("begin anchor rows");
    for ordinal in 1_u128..=64 {
        database
            .execute_in_tx(
                transaction,
                "INSERT INTO anchor_rows VALUES ($id, $context_id, $event_id, 'v1', 'key', 'identity')",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(ordinal + 100))),
                    ("context_id".to_owned(), Value::Uuid(context_id)),
                    ("event_id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(ordinal))),
                ]),
            )
            .expect("insert anchor row");
    }
    database.commit(transaction).expect("commit anchor rows");
    let reader = database
        .read_session(ReadLimits::default())
        .expect("open bounded reader");
    let answer = reader
        .execute(
            "SELECT id FROM anchor_rows WHERE event_id = $event_id AND context_id = $context_id ORDER BY declaration_version ASC, key ASC, identity_value ASC",
            &HashMap::from([
                ("event_id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(31))),
                ("context_id".to_owned(), Value::Uuid(context_id)),
            ]),
        )
        .expect("read one event's anchors");
    assert_eq!(
        answer.rows(),
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(131))]]
    );
    assert!(answer.trace().sort_elided);
    assert!(answer.trace().rows_examined <= 2, "{:?}", answer.trace());
    assert!(
        !answer
            .trace()
            .indexes_considered
            .iter()
            .any(|candidate| Some(candidate.name.as_str()) == answer.trace().index_used.as_deref()),
        "the used index must not be reported as rejected: {:?}",
        answer.trace()
    );
    drop(reader);
    database.close().expect("close writer");
}
