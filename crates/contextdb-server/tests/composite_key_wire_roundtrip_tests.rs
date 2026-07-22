//! Multi-column PRIMARY KEY — the wire shape.
//!
//! What a reader should take from this file: the row identity that crosses the
//! msgpack wire between two machines carries every column of the declared key,
//! in the order the key declares them. Two ways of proving it, both against
//! observable encode/decode behavior and neither naming a not-yet-existing
//! struct field. The APPLY-LEVEL way encodes, decodes, and applies a changeset
//! to a second database and compares the rows that come out by value. The
//! PAYLOAD-LEVEL way (the decisive one) encodes the row-identity payload IN
//! ISOLATION from the row values, decodes it generically, and asserts it
//! carries all four ordered column/value components — this is what stops an
//! implementation from leaving `WireNaturalKey` single-column and
//! reconstructing the composite identity from `row.values` on the far side,
//! which the apply-level check alone cannot see because a delete's values map
//! is empty but an insert's is not.
//! Today `WireNaturalKey` is `{ column: String, value: Value }`
//! (`protocol.rs:236-249`) and cannot express more than one column; the
//! payload-level tests read the identity generically (encode the isolated
//! `natural_key`, decode it into a shape-agnostic `serde_json::Value`, and read
//! its leaves as the ordered `(column, value)` pair sequence) so they compile
//! against that type today and stay valid whatever shape the implementer gives
//! it.
//!
//! Before this change: the table-level `PRIMARY KEY (col, col, ...)` form was
//! not in the grammar, so the `CREATE TABLE` failed first; behind it sits the
//! single-column identity these assertions are written for.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.
//! Assertions are by value, with a distinct payload per row.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
use contextdb_server::protocol::WireChangeSet;
use std::collections::HashMap;

const WINDOWS_DDL: &str = "CREATE TABLE metric_windows (\
     machine_id TEXT NOT NULL, \
     sensor_id TEXT NOT NULL, \
     metric TEXT NOT NULL, \
     window_start INTEGER NOT NULL, \
     value INTEGER NOT NULL, \
     PRIMARY KEY (machine_id, sensor_id, metric, window_start))";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn insert_window(
    db: &Database,
    machine: &str,
    sensor: &str,
    metric: &str,
    window_start: i64,
    value: i64,
) {
    let mut row = p();
    row.insert("machine_id".to_string(), Value::Text(machine.to_string()));
    row.insert("sensor_id".to_string(), Value::Text(sensor.to_string()));
    row.insert("metric".to_string(), Value::Text(metric.to_string()));
    row.insert("window_start".to_string(), Value::Int64(window_start));
    row.insert("value".to_string(), Value::Int64(value));
    db.execute(
        "INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) \
         VALUES ($machine_id, $sensor_id, $metric, $window_start, $value)",
        &row,
    )
    .unwrap_or_else(|err| panic!("insert {machine}/{sensor}/{metric}/{window_start}: {err}"));
}

/// Every `metric_windows` row as `(machine, sensor, metric, window_start,
/// value)`, sorted — the by-value view every assertion below compares against.
fn windows(db: &Database) -> Vec<(String, String, String, i64, i64)> {
    let result = db
        .execute("SELECT * FROM metric_windows", &p())
        .expect("metric_windows scan");
    let idx = |name: &str| {
        result
            .columns
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("column {name} must be selected, got {:?}", result.columns))
    };
    let (machine, sensor, metric, window_start, value) = (
        idx("machine_id"),
        idx("sensor_id"),
        idx("metric"),
        idx("window_start"),
        idx("value"),
    );
    let text = |row: &Vec<Value>, i: usize| match &row[i] {
        Value::Text(t) => t.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    let int = |row: &Vec<Value>, i: usize| match &row[i] {
        Value::Int64(v) => *v,
        other => panic!("expected an integer, got {other:?}"),
    };
    let mut rows: Vec<(String, String, String, i64, i64)> = result
        .rows
        .iter()
        .map(|row| {
            (
                text(row, machine),
                text(row, sensor),
                text(row, metric),
                int(row, window_start),
                int(row, value),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// The `notes` bodies, sorted — the single-column-identity guard's by-value view.
fn note_bodies(db: &Database) -> Vec<String> {
    let result = db.execute("SELECT * FROM notes", &p()).expect("notes scan");
    let body = result
        .columns
        .iter()
        .position(|column| column == "body")
        .expect("body column must be selected");
    let mut bodies: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[body] {
            Value::Text(text) => text.clone(),
            other => panic!("body must be text, got {other:?}"),
        })
        .collect();
    bodies.sort();
    bodies
}

/// The row half of a changeset. The subject is row IDENTITY and the receiver
/// already holds the declaration, so the DDL entries are dropped.
fn strip_ddl(mut changes: ChangeSet) -> ChangeSet {
    changes.ddl.clear();
    changes.ddl_lsn.clear();
    changes
}

/// Push a changeset through exactly what the transport does to it: engine
/// `ChangeSet` → `WireChangeSet` → msgpack bytes → `WireChangeSet` →
/// `ChangeSet`.
fn across_the_wire(changes: ChangeSet) -> ChangeSet {
    // Row identity is the subject; the target already holds the declaration.
    let changes = strip_ddl(changes);
    let wire: WireChangeSet = changes.into();
    let bytes = rmp_serde::to_vec(&wire).expect("WireChangeSet must encode under rmp_serde");
    let decoded: WireChangeSet =
        rmp_serde::from_slice(&bytes).expect("WireChangeSet must decode under rmp_serde");
    ChangeSet::try_from(decoded).expect("the decoded changeset must convert back")
}

/// The whole declared identity survives the msgpack round trip: four rows that
/// pairwise share three of their four key columns arrive as four rows with
/// their own values. If the encoded identity carried only the leading column,
/// the rows collapse on apply.
#[test]
fn the_encoded_row_identity_distinguishes_rows_sharing_all_but_one_key_column() {
    let source = Database::open_memory();
    source.execute(WINDOWS_DDL, &p()).expect("source table");
    insert_window(&source, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&source, "machine-a", "sensor-1", "motion", 2_000, 22);
    insert_window(&source, "machine-a", "sensor-2", "motion", 1_000, 33);
    insert_window(&source, "machine-b", "sensor-1", "motion", 1_000, 44);

    let target = Database::open_memory();
    target.execute(WINDOWS_DDL, &p()).expect("target table");
    target
        .apply_changes(
            across_the_wire(source.changes_since(Lsn(0))),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("apply the decoded changeset");

    assert_eq!(
        windows(&target),
        windows(&source),
        "every row must arrive with its own value after the msgpack round trip"
    );
    assert_eq!(
        windows(&target).len(),
        4,
        "four distinct rows crossed the wire"
    );
}

/// A delete crossing the wire names the whole key too: the sibling row that
/// shares three of four key columns is untouched.
#[test]
fn an_encoded_delete_names_the_whole_key() {
    let source = Database::open_memory();
    source.execute(WINDOWS_DDL, &p()).expect("source table");
    insert_window(&source, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&source, "machine-a", "sensor-1", "motion", 2_000, 22);

    let target = Database::open_memory();
    target.execute(WINDOWS_DDL, &p()).expect("target table");
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    target
        .apply_changes(across_the_wire(source.changes_since(Lsn(0))), &policies)
        .expect("apply the inserts");

    let before_delete = source.current_lsn();
    source
        .execute("DELETE FROM metric_windows WHERE window_start = 1000", &p())
        .expect("delete the first window");
    target
        .apply_changes(
            across_the_wire(source.changes_since(before_delete)),
            &policies,
        )
        .expect("apply the delete");

    assert_eq!(
        windows(&target),
        vec![(
            "machine-a".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            2_000,
            22
        )],
        "only the deleted window disappears across the wire"
    );
}

/// Guard, green today and after: a single-column identity still round-trips
/// through the same encode/decode path unchanged.
#[test]
fn guard_a_single_column_identity_still_round_trips() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &p(),
        )
        .expect("source table");
    for (id, body) in [(1_i64, "first"), (2, "second")] {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(body.to_string()));
        source
            .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
            .expect("insert note");
    }

    let target = Database::open_memory();
    target
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &p(),
        )
        .expect("target table");
    target
        .apply_changes(
            across_the_wire(source.changes_since(Lsn(0))),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("apply the decoded changeset");

    assert_eq!(
        note_bodies(&target),
        vec!["first".to_string(), "second".to_string()],
        "single-column identity keeps replicating both rows by value"
    );
}

// ---------------------------------------------------------------------------
// The identity payload itself carries every ordered key component
// ---------------------------------------------------------------------------

/// All-text key columns so every component is a distinctive, greppable marker;
/// the leading key column comes first so leaf order proves declaration order.
/// `note` is a NON-key column carrying its own marker, so a payload that
/// smuggled the row values into the identity would be caught too.
const MARKED_DDL: &str = "CREATE TABLE marked (\
     machine_id TEXT NOT NULL, \
     sensor_id TEXT NOT NULL, \
     metric TEXT NOT NULL, \
     window_key TEXT NOT NULL, \
     note TEXT NOT NULL, \
     PRIMARY KEY (machine_id, sensor_id, metric, window_key))";

const KEY_MARKERS: [&str; 4] = ["kmark-alpha", "kmark-bravo", "kmark-charlie", "kmark-delta"];
const KEY_COLUMNS: [&str; 4] = ["machine_id", "sensor_id", "metric", "window_key"];
const NON_KEY_MARKER: &str = "nonkey-echo";

fn insert_marked(db: &Database) {
    let mut row = p();
    row.insert("machine_id".to_string(), Value::Text(KEY_MARKERS[0].into()));
    row.insert("sensor_id".to_string(), Value::Text(KEY_MARKERS[1].into()));
    row.insert("metric".to_string(), Value::Text(KEY_MARKERS[2].into()));
    row.insert("window_key".to_string(), Value::Text(KEY_MARKERS[3].into()));
    row.insert("note".to_string(), Value::Text(NON_KEY_MARKER.into()));
    db.execute(
        "INSERT INTO marked (machine_id, sensor_id, metric, window_key, note) \
         VALUES ($machine_id, $sensor_id, $metric, $window_key, $note)",
        &row,
    )
    .expect("insert the marked row");
}

/// Every string leaf of a shape-agnostic decode of `bytes`, so the assertion
/// does not depend on whether the identity is a struct, a map, or a list of
/// pairs.
fn string_leaves(bytes: &[u8]) -> Vec<String> {
    let value: serde_json::Value =
        rmp_serde::from_slice(bytes).expect("the identity payload must decode generically");
    let mut leaves = Vec::new();
    collect_strings(&value, &mut leaves);
    leaves
}

fn collect_strings(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::String(s) => out.push(s.clone()),
        serde_json::Value::Array(items) => items.iter().for_each(|v| collect_strings(v, out)),
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                out.push(k.clone());
                collect_strings(v, out);
            }
        }
        _ => {}
    }
}

/// The identity payload's string leaves in wire order, keeping ONLY the ones
/// that are a declared key column name or a key value marker (type tags such as
/// the `Value` enum's variant name are dropped). For any encoding that lays the
/// identity out as ordered `(column, value)` components — a list of pairs, a
/// struct per column, a `Vec<(String, Value)>` — this yields the exact
/// alternating sequence `[col0, val0, col1, val1, ...]`, which pins BOTH the
/// association of each value with its own column AND the declared column order.
/// A payload that carried the columns in one order and the values in another
/// (the gap a presence-plus-value-offset check missed) produces a different
/// sequence and fails.
fn ordered_key_components(bytes: &[u8]) -> Vec<String> {
    string_leaves(bytes)
        .into_iter()
        .filter(|leaf| KEY_COLUMNS.contains(&leaf.as_str()) || KEY_MARKERS.contains(&leaf.as_str()))
        .collect()
}

/// The alternating `[machine_id, kmark-alpha, sensor_id, kmark-bravo, ...]` an
/// ordered composite identity must serialize to.
fn expected_key_components() -> Vec<String> {
    let mut expected = Vec::with_capacity(KEY_COLUMNS.len() * 2);
    for (column, marker) in KEY_COLUMNS.iter().zip(KEY_MARKERS.iter()) {
        expected.push((*column).to_string());
        expected.push((*marker).to_string());
    }
    expected
}

/// The isolated identity payload of a live composite-key row is the four
/// `(column, value)` components in declared order — each value paired with its
/// own column, columns in the order the key declares them — and it does NOT
/// carry the non-key value. Asserting the ORDERED PAIR sequence (not just that
/// every column and every value is present somewhere) is what rejects a payload
/// that lists columns in one order and values in another, and what makes
/// "single column, reconstruct the rest from the row values on the far side"
/// impossible to pass with, since the identity is encoded apart from the row
/// values.
#[test]
fn the_encoded_natural_key_carries_every_ordered_key_component() {
    let source = Database::open_memory();
    source.execute(MARKED_DDL, &p()).expect("source table");
    insert_marked(&source);

    let wire: WireChangeSet = strip_ddl(source.changes_since(Lsn(0))).into();
    assert_eq!(
        wire.rows.len(),
        1,
        "exactly the one inserted row is on the wire"
    );
    let identity_bytes =
        rmp_serde::to_vec(&wire.rows[0].natural_key).expect("the identity payload must encode");

    assert_eq!(
        ordered_key_components(&identity_bytes),
        expected_key_components(),
        "the identity payload must carry the four (column, value) components paired \
         and in declared order — presence alone would let columns and values drift apart"
    );
    assert!(
        !string_leaves(&identity_bytes)
            .iter()
            .any(|leaf| leaf == NON_KEY_MARKER),
        "the identity payload must NOT carry the non-key value {NON_KEY_MARKER}"
    );
}

/// Tombstone case. A delete carries no row values at all, so its identity is the
/// ONLY thing that can name the row. The isolated tombstone identity must carry
/// the same four ordered `(column, value)` pairs.
#[test]
fn the_encoded_delete_identity_carries_every_ordered_key_component() {
    let source = Database::open_memory();
    source.execute(MARKED_DDL, &p()).expect("source table");
    insert_marked(&source);
    let before_delete = source.current_lsn();
    source
        .execute("DELETE FROM marked WHERE window_key = 'kmark-delta'", &p())
        .expect("delete the marked row");

    let wire: WireChangeSet = strip_ddl(source.changes_since(before_delete)).into();
    let tombstone = wire
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("the delete must appear as a tombstone on the wire");
    let identity_bytes =
        rmp_serde::to_vec(&tombstone.natural_key).expect("the tombstone identity must encode");

    assert_eq!(
        ordered_key_components(&identity_bytes),
        expected_key_components(),
        "the tombstone identity must carry the four (column, value) components paired \
         and in declared order"
    );
}

// ---------------------------------------------------------------------------
// The keyless `id`-column fallback round-trips unchanged (green guard)
// ---------------------------------------------------------------------------

/// Covering-index guard, APPLY path — pins the covering-index requirement over
/// the wire, not the literal-`id` resolution (that is proven fixture-free at
/// resolution level in the engine test
/// `c5b_the_literal_id_fallback_is_the_emitted_sync_identity`). The `id` column
/// is `UNIQUE` so it carries a covering index — sync apply probes identity
/// through an index and errors on a bare unindexed column. Insert, update, and
/// delete must all survive the wire round trip by value — a resolver rewrite for
/// composite keys must not disturb this fallback. Green today and after.
#[test]
fn guard_a_keyless_id_table_round_trips_unchanged() {
    let ddl = "CREATE TABLE keyless (id INTEGER UNIQUE, body TEXT)";
    let source = Database::open_memory();
    source.execute(ddl, &p()).expect("source table");
    let target = Database::open_memory();
    target.execute(ddl, &p()).expect("target table");
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    for (id, body) in [(1_i64, "first"), (2, "second")] {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(body.to_string()));
        source
            .execute("INSERT INTO keyless (id, body) VALUES ($id, $body)", &row)
            .expect("insert");
    }
    let after_insert = source.current_lsn();
    target
        .apply_changes(across_the_wire(source.changes_since(Lsn(0))), &policies)
        .expect("apply inserts");

    source
        .execute("UPDATE keyless SET body = 'edited' WHERE id = 2", &p())
        .expect("update row 2");
    source
        .execute("DELETE FROM keyless WHERE id = 1", &p())
        .expect("delete row 1");
    target
        .apply_changes(
            across_the_wire(source.changes_since(after_insert)),
            &policies,
        )
        .expect("apply update and delete");

    let mut rows: Vec<(i64, String)> = target
        .execute("SELECT id, body FROM keyless", &p())
        .expect("scan")
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Int64(id), Value::Text(body)) => (*id, body.clone()),
            other => panic!("id and body expected, got {other:?}"),
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![(2, "edited".to_string())],
        "the id-column fallback matched update and delete by id across the wire"
    );
}
