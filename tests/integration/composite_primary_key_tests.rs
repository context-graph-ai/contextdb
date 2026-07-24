//! Multi-column PRIMARY KEY — engine-side acceptance for declaring a table's
//! identity as a tuple of columns and preserving it across sync.
//!
//! What a reader should take from this file: a table may declare the identity
//! it actually has — one row per machine, per sensor, per metric, per time
//! window — and that whole identity is what the sync path uses to tell one row
//! from another. Today identity is one column end to end
//! (`NaturalKey { column, value }`, `natural_key_column_for_meta` resolving to
//! the FIRST primary-key column), so two rows that differ only in a later key
//! column are the same row to sync and one of them is lost.
//!
//! Before this change, the table-level `PRIMARY KEY (a, b, ...)` form was not
//! in the grammar, so every test that declares one failed at the `CREATE TABLE`
//! with a parse error.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads —
//! nothing here is time-dependent, so no clock seam is needed. Every row
//! assertion is by VALUE: a count-only check passes under a silent overwrite.
//!
//! The keyless + `SYNC SAFE` refusal is pinned at all three declaration doors
//! (`CREATE`, `ALTER`, synced-DDL arrival) in `bounded_tables_tests.rs`; the
//! "still refuses a genuinely keyless table" half lives there, not duplicated
//! here.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use std::collections::HashMap;
use uuid::Uuid;

/// The shape a consumer writes: one row per machine,
/// per sensor, per metric, per window. Declared bare here — retention and
/// direction clauses are declared elsewhere and are exercised over the
/// real transport, so the identity mechanics do not depend on that grammar.
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

/// The row half of a changeset. The target already holds the table
/// declaration, so the DDL entries are dropped: this file is about row
/// IDENTITY, and the declaration's own travel over sync is item 1's subject.
fn rows_only(
    mut changes: contextdb_engine::sync_types::ChangeSet,
) -> contextdb_engine::sync_types::ChangeSet {
    changes.ddl.clear();
    changes.ddl_lsn.clear();
    changes
}

/// The `(key column, key value)` pairs the OUTGOING changeset carries for
/// `table` — i.e. what `natural_key_column_for_meta` actually resolved each
/// row's identity to, read straight off `changes_since` WITHOUT driving apply.
/// Reading the emitted identity (not the applied rows) is what lets these tests
/// inspect the resolver's choice for a bare unindexed key, where apply itself
/// hard-errors (`no covering index`). An EMPTY result means the
/// resolver produced no identity and the table's rows were silently dropped
/// from sync (the general keyless-skip).
fn emitted_identities(db: &Database, table: &str) -> Vec<(String, Value)> {
    db.changes_since(contextdb_core::Lsn(0))
        .rows
        .into_iter()
        .filter(|row| row.table == table)
        .map(|row| (row.natural_key.column, row.natural_key.value))
        .collect()
}

/// Replicate `source` onto a fresh database through the real changeset path —
/// the same `changes_since` → `apply_changes` pair the sync client and server
/// use. `InsertIfNotExists` per C5e: the first value for a key wins, so a
/// silent overwrite shows up as a changed VALUE and a silent collapse shows up
/// as a missing ROW.
fn replicate(source: &Database) -> Database {
    let target = Database::open_memory();
    target.execute(WINDOWS_DDL, &p()).expect("target table");
    target
        .apply_changes(
            rows_only(source.changes_since(contextdb_core::Lsn(0))),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("apply the source changeset");
    target
}

// ---------------------------------------------------------------------------
// C5a — the declaration parses, persists, and renders
// ---------------------------------------------------------------------------

/// C5a. The four-column identity is declared once at `CREATE`, is still the
/// table's identity after the process that declared it is gone, and is visible
/// in `.schema` — a key you cannot read back is a key an operator cannot
/// verify.
#[test]
fn c5a_a_table_level_composite_primary_key_parses_persists_and_renders() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("composite.db");

    let rendered_at_declaration = {
        let db = Database::open(&path).expect("open edge db");
        db.execute(WINDOWS_DDL, &p())
            .expect("a table-level PRIMARY KEY (col, col, ...) must parse");
        let meta = db.table_meta("metric_windows").expect("table meta");
        contextdb_engine::cli_render::render_table_meta("metric_windows", &meta)
    };

    assert!(
        rendered_at_declaration
            .contains("PRIMARY KEY (machine_id, sensor_id, metric, window_start)"),
        ".schema must render the declared multi-column identity, got:\n{rendered_at_declaration}"
    );

    // Survives close and reopen: the key is part of the stored table
    // definition, not a parse-time artifact.
    let reopened = Database::open(&path).expect("reopen edge db");
    let meta = reopened
        .table_meta("metric_windows")
        .expect("table meta after reopen");
    let rendered_after_reopen =
        contextdb_engine::cli_render::render_table_meta("metric_windows", &meta);
    assert_eq!(
        rendered_after_reopen, rendered_at_declaration,
        "the declared key must survive close/reopen unchanged"
    );

    // Round-trip: what `.schema` prints is what the engine accepts, and
    // re-declaring from it reproduces the same table.
    let fresh = Database::open_memory();
    fresh
        .execute(&rendered_after_reopen, &p())
        .unwrap_or_else(|err| {
            panic!("rendered .schema must re-parse and execute: {err}\n{rendered_after_reopen}")
        });
    let fresh_meta = fresh.table_meta("metric_windows").expect("fresh meta");
    assert_eq!(
        contextdb_engine::cli_render::render_table_meta("metric_windows", &fresh_meta),
        rendered_after_reopen,
        "re-rendering the re-parsed definition must be identical"
    );
}

/// C5a. The declared key is enforced as a key: the same four values twice is a
/// duplicate row, and a table that let it through would be handing sync an
/// ambiguous identity.
#[test]
fn c5a_the_declared_composite_key_is_unique_across_all_its_columns() {
    let db = Database::open_memory();
    db.execute(WINDOWS_DDL, &p()).expect("create table");
    insert_window(&db, "machine-a", "sensor-1", "motion", 1_000, 11);

    let mut row = p();
    row.insert("machine_id".to_string(), Value::Text("machine-a".into()));
    row.insert("sensor_id".to_string(), Value::Text("sensor-1".into()));
    row.insert("metric".to_string(), Value::Text("motion".into()));
    row.insert("window_start".to_string(), Value::Int64(1_000));
    row.insert("value".to_string(), Value::Int64(99));
    let err = db
        .execute(
            "INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) \
             VALUES ($machine_id, $sensor_id, $metric, $window_start, $value)",
            &row,
        )
        .expect_err("re-declaring the identical four-column key must be refused");

    assert_eq!(
        windows(&db),
        vec![(
            "machine-a".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            1_000,
            11
        )],
        "the refused duplicate must not have overwritten the first value: {err}"
    );

    // A row differing in ONLY the last key column is a different row.
    insert_window(&db, "machine-a", "sensor-1", "motion", 2_000, 22);
    assert_eq!(windows(&db).len(), 2, "a new window is a new row");
}

// ---------------------------------------------------------------------------
// C5b — identity carries every key column end to end
// ---------------------------------------------------------------------------

/// The decisive shape. Two rows from the SAME machine that differ only in
/// a key column the current single-column resolution never looks at
/// (`window_start`). Under today's identity both rows are "machine-a", so the
/// changeset carries one identity for two rows and the replica keeps one of
/// them. Asserted by value, because a count-only check would pass under a
/// last-writer-wins overwrite.
#[test]
fn c5b_rows_differing_only_in_a_later_key_column_stay_distinct_through_a_changeset() {
    let source = Database::open_memory();
    source.execute(WINDOWS_DDL, &p()).expect("source table");
    insert_window(&source, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&source, "machine-a", "sensor-1", "motion", 2_000, 22);
    insert_window(&source, "machine-a", "sensor-2", "motion", 1_000, 33);
    insert_window(&source, "machine-a", "sensor-1", "occupancy", 1_000, 44);

    let replica = replicate(&source);

    assert_eq!(
        windows(&replica),
        windows(&source),
        "each distinct (machine, sensor, metric, window) must survive replication \
         with its own value — a collapse to one row means the identity on the wire \
         carried only the first key column"
    );
    assert_eq!(
        windows(&replica).len(),
        4,
        "all four rows are distinct rows"
    );
}

/// C5b. The seed's own scenario at engine level: two machines writing the same
/// sensor, metric, and window are two rows, not one — and each keeps its own
/// value.
#[test]
fn c5b_two_machines_writing_the_same_sensor_metric_and_window_stay_two_rows() {
    let source = Database::open_memory();
    source.execute(WINDOWS_DDL, &p()).expect("source table");
    insert_window(&source, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&source, "machine-b", "sensor-1", "motion", 1_000, 22);

    let replica = replicate(&source);

    assert_eq!(
        windows(&replica),
        vec![
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                11
            ),
            (
                "machine-b".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                22
            ),
        ],
        "one surviving row is the failure the seed names"
    );
}

/// C5b. An update to an existing composite-key row replicates as an update to
/// THAT row — the identity is matched on all four columns on the arriving side,
/// not merely on the leading one.
#[test]
fn c5b_an_update_lands_on_the_row_whose_whole_key_matches() {
    let source = Database::open_memory();
    source.execute(WINDOWS_DDL, &p()).expect("source table");
    insert_window(&source, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&source, "machine-a", "sensor-1", "motion", 2_000, 22);

    let mut params = p();
    params.insert("window_start".to_string(), Value::Int64(2_000));
    source
        .execute(
            "UPDATE metric_windows SET value = 77 WHERE window_start = $window_start",
            &params,
        )
        .expect("update the second window");

    let replica = replicate(&source);
    assert_eq!(
        windows(&replica),
        vec![
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                1_000,
                11
            ),
            (
                "machine-a".to_string(),
                "sensor-1".to_string(),
                "motion".to_string(),
                2_000,
                77
            ),
        ],
        "the update must land on the 2000 window and leave the 1000 window alone"
    );
}

/// C5b. A delete replicates against the whole key too: deleting one window
/// must not remove its sibling.
#[test]
fn c5b_a_delete_removes_only_the_row_whose_whole_key_matches() {
    let source = Database::open_memory();
    source.execute(WINDOWS_DDL, &p()).expect("source table");
    insert_window(&source, "machine-a", "sensor-1", "motion", 1_000, 11);
    insert_window(&source, "machine-a", "sensor-1", "motion", 2_000, 22);
    let replica = replicate(&source);
    assert_eq!(windows(&replica).len(), 2, "both windows replicated first");

    let before_delete = source.current_lsn();
    let mut params = p();
    params.insert("window_start".to_string(), Value::Int64(1_000));
    source
        .execute(
            "DELETE FROM metric_windows WHERE window_start = $window_start",
            &params,
        )
        .expect("delete the first window");

    replica
        .apply_changes(
            rows_only(source.changes_since(before_delete)),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("apply the delete");

    assert_eq!(
        windows(&replica),
        vec![(
            "machine-a".to_string(),
            "sensor-1".to_string(),
            "motion".to_string(),
            2_000,
            22
        )],
        "only the deleted window disappears — a single-column tombstone would take both"
    );
}

// ---------------------------------------------------------------------------
// C5c — the SYNC SAFE key requirement is satisfied by a multi-column key
// ---------------------------------------------------------------------------

/// C5c. `SYNC SAFE` refuses a table it cannot identify rows in, because the
/// delete-after-delivery gate would open on rows that never reached the hub. A
/// four-column identity IS an identity, so the promise is accepted. The
/// refusal for a genuinely keyless table stays in force and is pinned in
/// `bounded_tables_tests.rs` (see the file header) — this test is the other
/// half of that pair.
#[test]
fn c5c_sync_safe_accepts_a_table_whose_only_key_is_multi_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE safe_windows (\
             machine_id TEXT NOT NULL, \
             sensor_id TEXT NOT NULL, \
             metric TEXT NOT NULL, \
             window_start INTEGER NOT NULL, \
             value INTEGER NOT NULL, \
             PRIMARY KEY (machine_id, sensor_id, metric, window_start)) \
         RETAIN 1 HOURS SYNC SAFE",
        &p(),
    )
    .expect("SYNC SAFE must accept a multi-column key: it identifies a row across nodes");

    assert!(
        db.table_meta("safe_windows").is_some(),
        "the SYNC SAFE table must exist after acceptance"
    );
}

/// C5c. The same promise added later by `ALTER` is accepted for the same
/// reason — the key requirement is about the table, not about which door the
/// promise arrived through.
#[test]
fn c5c_alter_adding_sync_safe_accepts_a_multi_column_key() {
    let db = Database::open_memory();
    db.execute(WINDOWS_DDL, &p()).expect("create table");
    db.execute(
        "ALTER TABLE metric_windows SET RETAIN 1 HOURS SYNC SAFE",
        &p(),
    )
    .expect("ALTER adding SYNC SAFE must accept a multi-column key");
}

// ---------------------------------------------------------------------------
// C5d — does-not-change guards (green today, must stay green)
// ---------------------------------------------------------------------------

/// C5d guard. A single-column `PRIMARY KEY` table replicates exactly as it does
/// today: same key means same row, different keys mean different rows. Green
/// before and after the change.
#[test]
fn c5d_guard_a_single_column_primary_key_replicates_unchanged() {
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
            rows_only(source.changes_since(contextdb_core::Lsn(0))),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("replicate notes");

    assert_eq!(
        note_bodies(&target),
        vec!["first".to_string(), "second".to_string()],
        "single-column identity keeps replicating both rows by value"
    );
}

/// Guard. A multi-column `UNIQUE` constraint stays exactly what it is —
/// a uniqueness rule, not a sync identity — but the silent-skip contract is
/// replaced by a loud constraint error: standard SQL errors on a UNIQUE
/// violation. A row whose `(sensor_id, metric)` pair collides with an
/// already-written row must be REFUSED with a typed constraint error, never
/// accepted as `Ok` with `rows_affected: 0` — a caller must be able to tell
/// "written" from "silently dropped" without re-reading the table. Nothing
/// here extends multi-column keys to unique constraints, indexes, foreign
/// keys, or vector keys; composite FOREIGN KEY behavior is separately
/// pinned in `tests/integration/composite_foreign_key_tests.rs`.
#[test]
fn c5d_guard_a_multi_column_unique_constraint_is_not_an_identity() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE readings (id INTEGER PRIMARY KEY, sensor_id TEXT NOT NULL, \
         metric TEXT NOT NULL, value INTEGER, UNIQUE (sensor_id, metric))",
        &p(),
    )
    .expect("create table with a multi-column UNIQUE");

    let insert = |id: i64, sensor: &str, metric: &str, value: i64| {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("sensor_id".to_string(), Value::Text(sensor.to_string()));
        row.insert("metric".to_string(), Value::Text(metric.to_string()));
        row.insert("value".to_string(), Value::Int64(value));
        db.execute(
            "INSERT INTO readings (id, sensor_id, metric, value) \
             VALUES ($id, $sensor_id, $metric, $value)",
            &row,
        )
    };

    insert(1, "sensor-1", "motion", 11).expect("first reading");
    insert(2, "sensor-1", "occupancy", 22).expect("different metric is a different pair");

    let duplicate_pair = insert(3, "sensor-1", "motion", 33);
    let err = duplicate_pair.expect_err(
        "a duplicate (sensor_id, metric) pair must be refused with a loud \
         constraint error, never accepted as Ok with rows_affected: 0",
    );
    match err {
        Error::UniqueViolation { table, column } => {
            assert_eq!(table, "readings", "UniqueViolation.table mismatch");
            assert!(
                column.contains("sensor_id") && column.contains("metric"),
                "UniqueViolation.column must name both columns of the \
                 composite constraint, got {column:?}"
            );
        }
        other => panic!(
            "expected a standard-SQL constraint error (Error::UniqueViolation) \
             naming table `readings` and both of `sensor_id`/`metric`, got \
             {other:?}"
        ),
    }

    let result = db
        .execute("SELECT id, value FROM readings", &p())
        .expect("readings scan");
    let mut kept: Vec<(i64, i64)> = result
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Int64(id), Value::Int64(value)) => (*id, *value),
            other => panic!("id and value must be integers, got {other:?}"),
        })
        .collect();
    kept.sort();
    assert_eq!(
        kept,
        vec![(1, 11), (2, 22)],
        "the uniqueness rule keeps the first row for the pair and admits the other metric"
    );

    let rendered = contextdb_engine::cli_render::render_table_meta(
        "readings",
        &db.table_meta("readings").expect("readings meta"),
    );
    assert!(
        !rendered.contains("PRIMARY KEY (sensor_id, metric)"),
        "a UNIQUE constraint must not be rendered as the table's key, got:\n{rendered}"
    );
}

/// C5d guard (F9). A multi-column INDEX is a lookup structure, not a sync
/// identity. Two rows sharing the SAME indexed `(sensor_id, metric)` pair but
/// different primary keys both replicate — the identity stays the `id` column —
/// and the index is not rendered as the table's key. This change does not extend
/// multi-column keys to indexes. Green today and after.
#[test]
fn c5d_guard_a_multi_column_index_is_not_an_identity() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE indexed_readings (id INTEGER PRIMARY KEY, sensor_id TEXT, \
             metric TEXT, value INTEGER)",
            &p(),
        )
        .expect("source table");
    source
        .execute(
            "CREATE INDEX idx_sensor_metric ON indexed_readings (sensor_id, metric)",
            &p(),
        )
        .expect("create the composite index");

    let insert = |db: &Database, id: i64, value: i64| {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("sensor_id".to_string(), Value::Text("sensor-1".into()));
        row.insert("metric".to_string(), Value::Text("motion".into()));
        row.insert("value".to_string(), Value::Int64(value));
        db.execute(
            "INSERT INTO indexed_readings (id, sensor_id, metric, value) \
             VALUES ($id, $sensor_id, $metric, $value)",
            &row,
        )
        .expect("insert");
    };
    // Same indexed pair, different primary key — two distinct rows.
    insert(&source, 1, 11);
    insert(&source, 2, 22);

    let target = Database::open_memory();
    target
        .execute(
            "CREATE TABLE indexed_readings (id INTEGER PRIMARY KEY, sensor_id TEXT, \
             metric TEXT, value INTEGER)",
            &p(),
        )
        .expect("target table");
    target
        .apply_changes(
            rows_only(source.changes_since(contextdb_core::Lsn(0))),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("replicate indexed rows");

    let mut ids: Vec<i64> = target
        .execute("SELECT id FROM indexed_readings", &p())
        .expect("scan")
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(id) => *id,
            other => panic!("id must be an integer, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2],
        "rows sharing an indexed pair stay distinct — the index is not the identity"
    );

    let rendered = contextdb_engine::cli_render::render_table_meta(
        "indexed_readings",
        &source
            .table_meta("indexed_readings")
            .expect("indexed_readings meta"),
    );
    assert!(
        !rendered.contains("PRIMARY KEY (sensor_id, metric)"),
        "an INDEX must not be rendered as the table's key, got:\n{rendered}"
    );
}

/// C5d (F9). A VECTOR column is a SEARCH surface addressed BY the row's natural
/// key — the `ROW_VECTOR('table','col',key)` form resolves a row's stored vector
/// through its natural key — and is never a sync identity itself. Proven two
/// ways, neither leaning on a shadowing single-column primary key to carry the
/// result: the EMITTED sync identity is the `id` column and never the vector
/// column; and a real `ROW_VECTOR` query keyed by `id` resolves and returns
/// identities, not vectors. This change does not extend keys to vector keys. Green
/// today and after.
#[test]
fn c5d_a_vector_column_is_addressed_by_the_key_and_is_never_the_identity() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE embeddings (id UUID PRIMARY KEY, label TEXT, vec VECTOR(2))",
        &p(),
    )
    .expect("vector table");
    let ids = [
        Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00a1),
        Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00b2),
        Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00c3),
    ];
    for (i, id) in ids.iter().enumerate() {
        let mut row = p();
        row.insert("id".to_string(), Value::Uuid(*id));
        row.insert("label".to_string(), Value::Text(format!("row-{i}")));
        row.insert(
            "vec".to_string(),
            Value::Vector(vec![i as f32, (i as f32) + 1.0]),
        );
        db.execute(
            "INSERT INTO embeddings (id, label, vec) VALUES ($id, $label, $vec)",
            &row,
        )
        .expect("insert embedding");
    }

    // The emitted sync identity is the id column, never the vector column.
    let identities = emitted_identities(&db, "embeddings");
    assert_eq!(identities.len(), 3, "one identity per row: {identities:?}");
    for (column, value) in &identities {
        assert_eq!(
            column, "id",
            "the vector must never be the sync identity: {identities:?}"
        );
        assert!(
            matches!(value, Value::Uuid(_)),
            "the identity value is the uuid id: {value:?}"
        );
    }

    // The vector-key surface: ROW_VECTOR resolves a row's vector BY its natural
    // key, and the ranked query returns identities, not vectors.
    let mut params = p();
    params.insert("key".to_string(), Value::Uuid(ids[0]));
    let result = db
        .execute(
            "SELECT id FROM embeddings ORDER BY vec <=> ROW_VECTOR('embeddings','vec',$key) LIMIT 3",
            &params,
        )
        .expect("a ROW_VECTOR query keyed by the natural key must resolve");
    assert_eq!(result.rows.len(), 3, "the ranked query returns every row");
    for row in &result.rows {
        assert!(
            matches!(row[0], Value::Uuid(_)),
            "ROW_VECTOR results are identities, not vectors: {row:?}"
        );
    }
}

/// Guard, APPLY path. This pins the covering-index
/// requirement, not the literal-`id` resolution (that is proven separately and
/// fixture-free in `c5b_the_literal_id_fallback_is_the_emitted_sync_identity`).
/// The `id` column is `UNIQUE` so it carries a covering index — sync apply
/// probes identity through an index and errors (`exact sync key probe ... has no
/// covering index`) on a bare unindexed column, so the fallback is only USABLE
/// through apply when `id` is indexed; that is the shape pinned here. Insert,
/// update, and delete all replicate by `id`. Green today and after.
#[test]
fn c5b_guard_a_keyless_id_table_replicates_unchanged() {
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
        .apply_changes(
            rows_only(source.changes_since(contextdb_core::Lsn(0))),
            &policies,
        )
        .expect("apply inserts");

    source
        .execute("UPDATE keyless SET body = 'edited' WHERE id = 2", &p())
        .expect("update row 2");
    source
        .execute("DELETE FROM keyless WHERE id = 1", &p())
        .expect("delete row 1");
    target
        .apply_changes(rows_only(source.changes_since(after_insert)), &policies)
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
        "the id-column fallback matched update and delete by id"
    );
}

/// C5c (F8). Positive synced-DDL arrival: a composite-key `SYNC SAFE` table's
/// definition, emitted by a source as a changeset and applied to a
/// schema-empty receiver, is accepted — the `SYNC SAFE` key requirement is
/// satisfied by the arriving multi-column key — the key is reconstructed on the
/// receiver, and it is then used for row identity so two rows sharing all but a
/// later key column stay distinct. This exercises the arrival door
/// (`rough_sync_table_meta` reconstruction), not just the local CREATE/ALTER.
#[test]
fn c5c_a_composite_key_sync_safe_table_arrives_by_synced_ddl() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE arrived_windows (\
                 machine_id TEXT NOT NULL, \
                 sensor_id TEXT NOT NULL, \
                 metric TEXT NOT NULL, \
                 window_start INTEGER NOT NULL, \
                 value INTEGER NOT NULL, \
                 PRIMARY KEY (machine_id, sensor_id, metric, window_start)) \
             RETAIN 48 HOURS SYNC SAFE",
            &p(),
        )
        .expect("source declares the composite-key SYNC SAFE table");
    let insert = |machine: &str, window: i64, value: i64| {
        let mut row = p();
        row.insert("machine_id".to_string(), Value::Text(machine.into()));
        row.insert("sensor_id".to_string(), Value::Text("sensor-1".into()));
        row.insert("metric".to_string(), Value::Text("motion".into()));
        row.insert("window_start".to_string(), Value::Int64(window));
        row.insert("value".to_string(), Value::Int64(value));
        source
            .execute(
                "INSERT INTO arrived_windows \
                 (machine_id, sensor_id, metric, window_start, value) \
                 VALUES ($machine_id, $sensor_id, $metric, $window_start, $value)",
                &row,
            )
            .expect("insert");
    };
    // Same machine, two windows: distinct only in the last key column.
    insert("machine-a", 1_000, 11);
    insert("machine-a", 2_000, 22);

    // A schema-empty receiver takes the DDL AND the rows from the wire — the
    // full changeset, DDL included (this is the arrival door).
    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(contextdb_core::Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("the composite-key SYNC SAFE definition must be accepted on arrival");

    let meta = receiver
        .table_meta("arrived_windows")
        .expect("the table must exist on the receiver after synced DDL");
    let rendered = contextdb_engine::cli_render::render_table_meta("arrived_windows", &meta);
    assert!(
        rendered.contains("PRIMARY KEY (machine_id, sensor_id, metric, window_start)"),
        "the receiver must reconstruct the full composite key, got:\n{rendered}"
    );

    let mut windows: Vec<i64> = receiver
        .execute("SELECT window_start FROM arrived_windows", &p())
        .expect("scan")
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(w) => *w,
            other => panic!("window_start must be an integer, got {other:?}"),
        })
        .collect();
    windows.sort();
    assert_eq!(
        windows,
        vec![1_000, 2_000],
        "the reconstructed key is used for identity — both windows survive as distinct rows"
    );
}

/// C5b guard (F7), RESOLUTION path — the fixture-free proof of the literal-`id`
/// fallback. A table with NO declared key AT ALL (no `PRIMARY KEY`, no `UNIQUE`,
/// no index) but an `id` column resolves its emitted sync identity to that `id`
/// column, by value, read straight off the changeset. Because there is no
/// `UNIQUE` here, a resolver that dropped the literal-`id` fallback and instead
/// (wrongly, per C5d) adopted a single-column `UNIQUE` could NOT satisfy this —
/// which the covering-index apply guard alone could, since it contaminates `id`
/// with `UNIQUE`. Green today and after.
#[test]
fn c5b_the_literal_id_fallback_is_the_emitted_sync_identity() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE keyless_bare (id INTEGER, body TEXT)", &p())
        .expect("a bare id table — no PRIMARY KEY, no UNIQUE, no index");
    for (id, body) in [(1_i64, "first"), (2, "second")] {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(body.to_string()));
        db.execute(
            "INSERT INTO keyless_bare (id, body) VALUES ($id, $body)",
            &row,
        )
        .expect("insert");
    }

    let mut identities = emitted_identities(&db, "keyless_bare");
    identities.sort_by_key(|(_, value)| match value {
        Value::Int64(n) => *n,
        _ => i64::MAX,
    });
    assert_eq!(
        identities,
        vec![
            ("id".to_string(), Value::Int64(1)),
            ("id".to_string(), Value::Int64(2)),
        ],
        "the emitted sync identity is the literal id column, by value"
    );
}

/// Negative case, no PK shadow. A composite `UNIQUE` and a composite INDEX
/// on a table with NO primary key and no `id` column are NOT adopted as the sync
/// identity: the resolver produces nothing, so the rows carry no identity onto
/// the wire (the general keyless-skip) rather than being keyed by the
/// index/unique columns. Read at resolution level, so no primary key is present
/// to mask a resolver that would treat the composite index as identity only when
/// no PK exists. If it ever did, an identity naming those columns would appear
/// here. Green today and after.
#[test]
fn c5d_a_composite_index_without_a_primary_key_is_not_adopted_as_identity() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE idx_no_pk (sensor_id TEXT NOT NULL, metric TEXT NOT NULL, \
         value INTEGER, UNIQUE (sensor_id, metric))",
        &p(),
    )
    .expect("a keyless table with a composite UNIQUE, no primary key, no id");
    db.execute("CREATE INDEX idx_np ON idx_no_pk (sensor_id, metric)", &p())
        .expect("a composite index too");
    let mut row = p();
    row.insert("sensor_id".to_string(), Value::Text("sensor-1".into()));
    row.insert("metric".to_string(), Value::Text("motion".into()));
    row.insert("value".to_string(), Value::Int64(11));
    db.execute(
        "INSERT INTO idx_no_pk (sensor_id, metric, value) \
         VALUES ($sensor_id, $metric, $value)",
        &row,
    )
    .expect("insert");

    let identities = emitted_identities(&db, "idx_no_pk");
    assert!(
        identities.is_empty(),
        "a composite UNIQUE/INDEX must not become the sync identity when there is no \
         key — the row is keyless-skipped, not keyed by the index columns: {identities:?}"
    );
}
