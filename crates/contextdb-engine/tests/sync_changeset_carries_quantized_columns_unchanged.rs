//! A peer receives a quantized column exactly as it always did.
//!
//! A quantized vector column is stored only in its vector index; the
//! relational row's own slot holds nothing. That is a storage decision, and a
//! storage decision must not reach the wire. A peer that has been receiving
//! the column and suddenly receives a row without it does not see an error --
//! it sees a row whose vector is absent, writes that absence down, and every
//! search it serves from then on is missing rows nobody can tell are missing.
//!
//! So the changeset carries the column with the vector the sender would have
//! answered with: the quantizer's own round trip of what was inserted. Three
//! paths assemble changesets -- the incremental log, the full snapshot, and
//! the persisted delta after a reopen -- and a peer cannot tell which one its
//! sender used, so all three carry the same payload.
//!
//! The expected values here are computed with the vector crate's public
//! quantizer, never read back from the engine under test, and the wire bytes
//! are compared against a row assembled by hand from those values.

#![cfg(feature = "test-seams")]

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::table_meta::VectorQuantization;
use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::protocol::{WireRowChange, wire_changeset_with_arrivals};
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
};
use contextdb_vector::stored_vector_value;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// Width of both quantized columns.
const DIMENSIONS: usize = 8;

/// The rows every path below must carry, with the vectors they were given.
fn fixture_rows() -> Vec<(Uuid, &'static str, Vec<f32>)> {
    vec![
        (
            Uuid::from_u128(0x00A1_0000_0000_0000_0000_0000_0000_0001),
            "first",
            vec![0.10, 0.25, 0.40, 0.55, 0.70, 0.85, 0.95, 1.00],
        ),
        (
            Uuid::from_u128(0x00A1_0000_0000_0000_0000_0000_0000_0002),
            "second",
            vec![-1.00, -0.60, -0.20, 0.00, 0.20, 0.60, 0.80, 1.00],
        ),
    ]
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// What the quantizer makes of a vector: what it stores, read back as f32.
/// This is the sender's own answer for the column, computed here rather than
/// taken from the engine this file is about.
fn quantized_round_trip(vector: &[f32], quantization: VectorQuantization) -> Vec<f32> {
    stored_vector_value(vector, quantization)
}

fn create_store(path: &std::path::Path) -> Database {
    let database = Database::open(path).expect("open the sending store");
    database
        .execute(
            &format!(
                "CREATE TABLE readings (\
                 id UUID PRIMARY KEY, \
                 name TEXT, \
                 compact VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8'), \
                 tiny VECTOR({DIMENSIONS}) WITH (quantization = 'SQ4'))"
            ),
            &empty(),
        )
        .expect("declare a table with one SQ8 and one SQ4 column");
    for (id, name, vector) in fixture_rows() {
        database
            .execute(
                "INSERT INTO readings (id, name, compact, tiny) \
                 VALUES ($id, $name, $compact, $tiny)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("name", Value::Text(name.to_owned())),
                    ("compact", Value::Vector(vector.clone())),
                    ("tiny", Value::Vector(vector)),
                ]),
            )
            .expect("store a row carrying both quantized columns");
    }
    database
}

/// The row a changeset carries for `id`, or a panic naming what it carried
/// instead.
fn row_for(changes: &ChangeSet, id: Uuid, path: &str) -> RowChange {
    changes
        .rows
        .iter()
        .find(|row| row.natural_key.value == Value::Uuid(id))
        .unwrap_or_else(|| {
            panic!(
                "the {path} changeset carries a row for {id}; it carried {} rows: {:?}",
                changes.rows.len(),
                changes
                    .rows
                    .iter()
                    .map(|row| &row.natural_key.value)
                    .collect::<Vec<_>>()
            )
        })
        .clone()
}

/// Both quantized columns are present and hold the quantizer's own answer.
fn assert_carries_the_quantized_vectors(row: &RowChange, vector: &[f32], path: &str) {
    for (column, quantization) in [
        ("compact", VectorQuantization::SQ8),
        ("tiny", VectorQuantization::SQ4),
    ] {
        let expected = quantized_round_trip(vector, quantization);
        match row.values.get(column) {
            Some(Value::Vector(carried)) => assert_eq!(
                carried, &expected,
                "the {path} changeset carries {column} as the quantizer's own answer for the \
                 vector that was inserted"
            ),
            other => panic!(
                "the {path} changeset carries {column}; it carried {other:?}, so a peer applying \
                 this row records the column as absent and every search it later serves is \
                 missing rows nobody can tell are missing"
            ),
        }
    }
}

/// A row assembled here from the fixture's own inputs, carrying nothing the
/// engine produced except the identity and ordering fields, which are its to
/// assign and are not what this file is about.
fn golden_row(observed: &RowChange, id: Uuid, name: &str, vector: &[f32]) -> RowChange {
    let mut values = HashMap::new();
    values.insert("id".to_owned(), Value::Uuid(id));
    values.insert("name".to_owned(), Value::Text(name.to_owned()));
    values.insert(
        "compact".to_owned(),
        Value::Vector(quantized_round_trip(vector, VectorQuantization::SQ8)),
    );
    values.insert(
        "tiny".to_owned(),
        Value::Vector(quantized_round_trip(vector, VectorQuantization::SQ4)),
    );
    RowChange {
        table: "readings".to_owned(),
        natural_key: NaturalKey::single("id".to_owned(), Value::Uuid(id)),
        values,
        deleted: false,
        lsn: observed.lsn,
        created_at: observed.created_at,
    }
}

/// The wire row a changeset row becomes.
fn wire_row(row: RowChange) -> WireRowChange {
    let wire = wire_changeset_with_arrivals(
        ChangeSet {
            rows: vec![row],
            ..ChangeSet::default()
        },
        &HashMap::new(),
    );
    wire.rows
        .into_iter()
        .next()
        .expect("one row in, one row out of the wire conversion")
}

/// Every field of a wire row, encoded with its values in a fixed key order.
///
/// A wire row's values are a hash map, and two maps holding the same entries
/// encode in whatever order each one happens to iterate -- an artifact of the
/// map, not something a peer can observe, since a peer decodes back to a map.
/// Ordering the keys is what makes a byte comparison a statement about the
/// payload; every other field is encoded exactly as it stands.
fn canonical_wire_bytes(row: RowChange) -> Vec<u8> {
    let wire = wire_row(row);
    let ordered: BTreeMap<&String, &Value> = wire.values.iter().collect();
    rmp_serde::to_vec(&(
        &wire.table,
        &wire.natural_key,
        &ordered,
        wire.deleted,
        wire.lsn,
        &wire.created_at,
        &wire.arrival,
        &wire.lineage,
    ))
    .expect("a wire row encodes")
}

/// Everything a peer is entitled to, checked against one changeset.
fn assert_changeset_is_unchanged(changes: &ChangeSet, path: &str) {
    for (id, name, vector) in fixture_rows() {
        let observed = row_for(changes, id, path);
        assert_carries_the_quantized_vectors(&observed, &vector, path);
        let golden = golden_row(&observed, id, name, &vector);
        assert_eq!(
            canonical_wire_bytes(observed.clone()),
            canonical_wire_bytes(golden.clone()),
            "the {path} changeset's wire bytes for {id} are the bytes of a row carrying the \
             quantizer's own answer for both columns"
        );
        // And the bytes really sent decode to that row, so the comparison
        // above is not passing on a rendering the wire never carries.
        let sent = rmp_serde::to_vec(&wire_row(observed)).expect("a wire row encodes");
        let decoded: WireRowChange =
            rmp_serde::from_slice(&sent).expect("what the wire carries decodes as a wire row");
        assert_eq!(
            decoded,
            wire_row(golden),
            "the {path} row a peer decodes for {id} is the row assembled here from the inputs"
        );
    }
}

#[test]
fn the_incremental_changeset_carries_both_quantized_columns() {
    let directory = tempfile::TempDir::new().expect("task-scoped quantized-sync directory");
    let path = directory.path().join("incremental.db");
    let database = create_store(&path);

    // Straight after the inserts the change log still covers the watermark,
    // so this is the incremental path.
    let changes = database.changes_since(Lsn(0));
    assert_changeset_is_unchanged(&changes, "incremental");
    database.close().expect("the sender closes cleanly");
}

#[test]
fn the_full_snapshot_changeset_carries_both_quantized_columns() {
    let directory = tempfile::TempDir::new().expect("task-scoped quantized-sync directory");
    let path = directory.path().join("snapshot.db");
    create_store(&path).close().expect("close the sender");

    // A reopened store has no change log, and a watermark of zero asks for
    // everything, which is the full-snapshot path.
    let reopened = Database::open(&path).expect("reopen the sending store");
    let changes = reopened.changes_since(Lsn(0));
    assert_changeset_is_unchanged(&changes, "full snapshot");
    reopened.close().expect("the sender closes cleanly");
}

#[test]
fn the_persisted_delta_changeset_carries_both_quantized_columns() {
    let directory = tempfile::TempDir::new().expect("task-scoped quantized-sync directory");
    let path = directory.path().join("delta.db");
    create_store(&path).close().expect("close the sender");

    // A reopened store asked for changes since a watermark above zero takes
    // the persisted-delta path rather than the snapshot one.
    let reopened = Database::open(&path).expect("reopen the sending store");
    let changes = reopened.changes_since(Lsn(1));
    assert_changeset_is_unchanged(&changes, "persisted delta");
    reopened.close().expect("the sender closes cleanly");
}

#[test]
fn a_peer_applying_the_changeset_answers_the_same_vectors() {
    let directory = tempfile::TempDir::new().expect("task-scoped quantized-sync directory");
    let sender_path = directory.path().join("sender.db");
    let sender = create_store(&sender_path);
    let changes = sender.changes_since(Lsn(0));
    let sender_indexes = sender
        .read_session(ReadLimits::default())
        .expect("open a reading view of the sender")
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the sender describes its vector indexes");
    sender.close().expect("the sender closes cleanly");

    let receiver_path = directory.path().join("receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiving store");
    receiver
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("the receiver applies what the sender sent");

    // The receiver was told the column, so it can answer for it -- with the
    // same vectors, through a table it never had declared to it by hand.
    for (id, _, vector) in fixture_rows() {
        for (column, quantization) in [
            ("compact", VectorQuantization::SQ8),
            ("tiny", VectorQuantization::SQ4),
        ] {
            let answered = receiver
                .execute(
                    &format!("SELECT {column} FROM readings WHERE id = $id"),
                    &params(vec![("id", Value::Uuid(id))]),
                )
                .expect("the receiver answers for a replicated row");
            let carried = match answered.rows.first().and_then(|row| row.first()) {
                Some(Value::Vector(carried)) => carried.clone(),
                other => panic!("the receiver answers {column} for {id} with a vector: {other:?}"),
            };
            assert_eq!(
                carried,
                quantized_round_trip(&vector, quantization),
                "the receiver answers {column} with the same vector the sender would have"
            );
        }
    }

    let receiver_indexes = receiver
        .read_session(ReadLimits::default())
        .expect("open a reading view of the receiver")
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the receiver describes its vector indexes");
    assert_eq!(
        rmp_serde::to_vec(&receiver_indexes.rows).expect("index rows encode"),
        rmp_serde::to_vec(&sender_indexes.rows).expect("index rows encode"),
        "sender and receiver describe the same vector indexes, so the quantization travelled \
         with the rows rather than being re-derived"
    );
    receiver.close().expect("the receiver closes cleanly");
}
