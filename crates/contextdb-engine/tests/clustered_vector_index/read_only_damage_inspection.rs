//! A read-only session that finds live damage in a Context's derived index
//! pages tells the operator in plain words and never writes the store: its
//! inspection row shows the rebuild recovery action and the note "quarantined
//! in memory for this reader (AUTO routed to EXACT, verified once per
//! admitted manifest window); persisted and rebuilt on the next writer open",
//! AUTO answers exactly, and INDEXED is refused as store corruption. The next
//! writer open persists the verdict: the row still shows the rebuild action,
//! with the note "damage recorded; rebuild scheduled for the next maintenance
//! cycle", and that verdict is in the store: a fresh read-only session opened
//! after the writer closes, before any maintenance, shows the same action and
//! note without verifying the damage again. Maintenance then rebuilds, after
//! which no recovery action or note remains. No
//! inspection value anywhere says [unrecorded] or [recorded].

use super::support::{
    IndexDamage, IndexTestOptions, SearchRoute, damage_serving_index, damage_verifications,
    drive_until_serving, empty, int_ids, ivf, observe_search, open_reader,
    open_with_index_test_options, params, partition_inspection, unit_vector, verdict_recorded,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::path::Path;

const DIMENSIONS: usize = 16;
const ROWS: std::ops::Range<i64> = 0..240;
const OPTIONS: IndexTestOptions = IndexTestOptions {
    write_buffer_cap_bytes: Some(1024),
    build_batch_rows: None,
};
const WRITER_NOTE: &str = "damage recorded; rebuild scheduled for the next maintenance cycle";
const READER_NOTE: &str = "quarantined in memory for this reader (AUTO routed to EXACT, verified once per admitted manifest window); persisted and rebuilt on the next writer open";

fn query(db: &Database, id: i64, mode: &str) -> contextdb_core::Result<Vec<i64>> {
    db.execute(
        &format!(
            "SELECT id FROM damaged_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        &params([("query", Value::Vector(unit_vector(id as u64, DIMENSIONS)))]),
    )
    .map(|result| int_ids(&result))
}

fn assert_no_persistence_labels(db: &Database, when: &str) {
    let row = partition_inspection(db, "damaged_items", "embedding", &[]);
    for text in &row.texts {
        assert!(
            !text.contains("[unrecorded]") && !text.contains("[recorded]"),
            "{when}: no inspection value names an internal persistence label: {text:?}"
        );
    }
    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("SHOW VECTOR_INDEXES");
    let rendered = format!("{:?}", indexes.rows());
    assert!(
        !rendered.contains("[unrecorded]") && !rendered.contains("[recorded]"),
        "{when}: SHOW VECTOR_INDEXES names no internal persistence label"
    );
}

fn store_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).expect("read the store file")
}

#[test]
fn a_read_only_session_reports_live_damage_in_plain_words_and_the_next_writer_persists_and_rebuilds()
 {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("damaged.db");
    let writer = open_with_index_test_options(&path, OPTIONS);
    writer
        .execute(
            &format!(
                "CREATE TABLE damaged_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE AUTO {})",
                ivf(4, 4, 64)
            ),
            &empty(),
        )
        .expect("declare the clustered index");
    for id in ROWS {
        writer
            .execute(
                "INSERT INTO damaged_items (id, embedding) VALUES ($id, $embedding)",
                &params([
                    ("id", Value::Int64(id)),
                    (
                        "embedding",
                        Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                    ),
                ]),
            )
            .expect("commit one row");
    }
    drive_until_serving(&writer);
    assert_eq!(
        partition_inspection(&writer, "damaged_items", "embedding", &[]).recovery_action,
        "none",
        "fixture: a healthy partition needs no recovery"
    );
    // Damage planted below the engine, with no write or maintenance after it,
    // so no writer has met it.
    damage_serving_index(
        &writer,
        "damaged_items",
        "embedding",
        &[],
        IndexDamage::CodebookDigest,
    );
    writer.close().expect("close the writer");
    drop(writer);

    let before_reader = store_bytes(&path);
    {
        let reader = open_reader(&path);
        for call in 0..2 {
            let row = partition_inspection(&reader, "damaged_items", "embedding", &[]);
            assert_eq!(
                (row.recovery_action.as_str(), row.note.as_deref()),
                ("rebuild", Some(READER_NOTE)),
                "read-only inspection {call} shows the rebuild action and the in-memory quarantine note: {row:?}"
            );
        }
        assert_eq!(
            (
                damage_verifications(&reader),
                verdict_recorded(&reader, "damaged_items", "embedding", &[])
            ),
            (1, false),
            "the reader verified the damage once in its window and recorded nothing"
        );
        assert_no_persistence_labels(&reader, "in the read-only session");
        let (auto_ids, auto) = observe_search(&reader, || query(&reader, 7, "AUTO").unwrap());
        assert_eq!(
            auto.route,
            Some(SearchRoute::Exact),
            "AUTO is routed to EXACT: {auto:?}"
        );
        assert_eq!(
            auto_ids,
            query(&reader, 7, "EXACT").unwrap(),
            "AUTO answers exactly"
        );
        let error = query(&reader, 7, "INDEXED").expect_err("INDEXED is refused");
        assert!(
            format!("{error:?}").starts_with("StoreCorrupted"),
            "INDEXED over the damaged generation is store corruption: {error:?}"
        );
    }
    assert!(
        store_bytes(&path) == before_reader,
        "the read-only session quarantines in memory and writes nothing to the store"
    );

    let writer = open_with_index_test_options(&path, OPTIONS);
    let persisted = partition_inspection(&writer, "damaged_items", "embedding", &[]);
    assert_eq!(
        (
            persisted.recovery_action.as_str(),
            persisted.note.as_deref()
        ),
        ("rebuild", Some(WRITER_NOTE)),
        "the writer open records the damage and schedules the rebuild: {persisted:?}"
    );
    assert_no_persistence_labels(&writer, "after the writer opens");
    // Closed with no maintenance: what the writer open wrote is all a later
    // reader can see.
    writer
        .close()
        .expect("close the writer before any maintenance");
    drop(writer);
    let after_writer = store_bytes(&path);
    assert!(
        after_writer != before_reader,
        "the writer open wrote the verdict to the store"
    );
    {
        let reader = open_reader(&path);
        let row = partition_inspection(&reader, "damaged_items", "embedding", &[]);
        assert_eq!(
            (row.recovery_action.as_str(), row.note.as_deref()),
            ("rebuild", Some(WRITER_NOTE)),
            "a fresh read-only session reads the recorded verdict: {row:?}"
        );
        assert_eq!(
            (
                damage_verifications(&reader),
                verdict_recorded(&reader, "damaged_items", "embedding", &[])
            ),
            (0, true),
            "the recorded verdict is read, not re-verified"
        );
        assert_no_persistence_labels(
            &reader,
            "in a read-only session after the writer recorded it",
        );
    }
    assert!(
        store_bytes(&path) == after_writer,
        "the second read-only session writes nothing either"
    );

    let writer = open_with_index_test_options(&path, OPTIONS);
    drive_until_serving(&writer);
    assert_eq!(
        partition_inspection(&writer, "damaged_items", "embedding", &[]).recovery_action,
        "none",
        "fixture: a healthy partition needs no recovery"
    );
    // Damage planted below the engine, with no write or maintenance after it,
    // so no writer has met it.
    damage_serving_index(
        &writer,
        "damaged_items",
        "embedding",
        &[],
        IndexDamage::CodebookDigest,
    );
    writer.close().expect("close the writer");
    drop(writer);

    let before_reader = store_bytes(&path);
    {
        let reader = open_reader(&path);
        for call in 0..2 {
            let row = partition_inspection(&reader, "damaged_items", "embedding", &[]);
            assert_eq!(
                (row.recovery_action.as_str(), row.note.as_deref()),
                ("rebuild", Some(READER_NOTE)),
                "read-only inspection {call} shows the rebuild action and the in-memory quarantine note: {row:?}"
            );
        }
        assert_eq!(
            (
                damage_verifications(&reader),
                verdict_recorded(&reader, "damaged_items", "embedding", &[])
            ),
            (1, false),
            "the reader verified the damage once in its window and recorded nothing"
        );
        assert_no_persistence_labels(&reader, "in the read-only session");
        let (auto_ids, auto) = observe_search(&reader, || query(&reader, 7, "AUTO").unwrap());
        assert_eq!(
            auto.route,
            Some(SearchRoute::Exact),
            "AUTO is routed to EXACT: {auto:?}"
        );
        assert_eq!(
            auto_ids,
            query(&reader, 7, "EXACT").unwrap(),
            "AUTO answers exactly"
        );
        let error = query(&reader, 7, "INDEXED").expect_err("INDEXED is refused");
        assert!(
            format!("{error:?}").starts_with("StoreCorrupted"),
            "INDEXED over the damaged generation is store corruption: {error:?}"
        );
    }
    assert!(
        store_bytes(&path) == before_reader,
        "the read-only session quarantines in memory and writes nothing to the store"
    );

    let writer = open_with_index_test_options(&path, OPTIONS);
    let persisted = partition_inspection(&writer, "damaged_items", "embedding", &[]);
    assert_eq!(
        persisted.recovery_action, "rebuild",
        "after the writer opens, the persisted verdict still shows the rebuild action: {persisted:?}"
    );
    assert_eq!(
        persisted.note.as_deref(),
        Some(WRITER_NOTE),
        "the note now says the damage is recorded and the rebuild scheduled: {persisted:?}"
    );
    assert_no_persistence_labels(&writer, "after the writer opens");

    drive_until_serving(&writer);
    let rebuilt = partition_inspection(&writer, "damaged_items", "embedding", &[]);
    assert_eq!(
        (rebuilt.recovery_action.as_str(), rebuilt.note.as_deref()),
        ("none", None),
        "after the rebuild no recovery action or damage note remains: {rebuilt:?}"
    );
    assert_no_persistence_labels(&writer, "after the rebuild");
    for id in ROWS.step_by(30) {
        assert_eq!(
            query(&writer, id, "INDEXED").unwrap().first(),
            Some(&id),
            "after the rebuild INDEXED finds row {id}"
        );
    }
}
