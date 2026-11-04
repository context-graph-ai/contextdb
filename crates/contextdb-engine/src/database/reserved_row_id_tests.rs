//! Reserved declarations and engine identifiers on an ordinary durable store.
use super::*;
use crate::read_session::DatabaseOpenOptions;

type Shape = (Vec<String>, Vec<Vec<Value>>);

struct Store {
    db: Database,
    path: std::path::PathBuf,
    _root: tempfile::TempDir,
}
impl Store {
    fn options() -> DatabaseOpenOptions {
        let mut options = DatabaseOpenOptions::default();
        options.owner_reads.enabled = false;
        options.maintenance_policy = MaintenancePolicy::CallerDriven;
        options
    }
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("reserved-name.db");
        let db = Database::open_with_options(&path, Self::options()).unwrap();
        Self {
            db,
            path,
            _root: root,
        }
    }
    fn reopen(self) -> Self {
        let Self { db, path, _root } = self;
        db.close().unwrap();
        drop(db);
        let db = Database::open_with_options(&path, Self::options()).unwrap();
        Self { db, path, _root }
    }
    fn close(self) {
        self.db.close().unwrap();
    }
}
fn sql(db: &Database, statement: &str) {
    db.execute(statement, &HashMap::new()).unwrap();
}
fn shape(db: &Database, statement: &str) -> std::result::Result<Shape, String> {
    db.execute(statement, &HashMap::new())
        .map(|answer| (answer.columns().to_vec(), answer.rows().to_vec()))
        .map_err(|error| format!("{:?}", error.engine_diagnostic()))
}
fn reserved_column(result: Result<()>, table: &str) {
    let error = result.expect_err("DDL accepted a reserved row_id column instead of refusing it");
    assert!(
        matches!(error.engine_diagnostic(), Error::ReservedColumnName { table: found, column } if found == table && column == "row_id"),
        "expected typed ReservedColumnName for {table}.row_id, got {error:?}"
    );
}

fn create_refusal(table: &str, declaration: &str) {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE allowed_create (id INTEGER PRIMARY KEY, graph_row_id INTEGER)",
    );
    sql(
        &store.db,
        "CREATE TABLE allowed_key (graph_row_id INTEGER PRIMARY KEY, payload TEXT)",
    );
    let controls = (
        store.db.table_meta("allowed_create"),
        store.db.table_meta("allowed_key"),
    );
    let before = store.db.current_lsn();
    let result = store.db.execute(declaration, &HashMap::new()).map(|_| ());
    let live_absent = store.db.table_meta(table).is_none();
    let live_lsn = store.db.current_lsn();
    let store = store.reopen();
    let durable_absent = store.db.table_meta(table).is_none();
    let durable_lsn = store.db.current_lsn();
    let durable_controls = (
        store.db.table_meta("allowed_create"),
        store.db.table_meta("allowed_key"),
    );
    store.close();
    eprintln!(
        "CDB240_CREATE table={table} result={result:?} live_absent={live_absent} durable_absent={durable_absent} before={before:?} live_lsn={live_lsn:?} durable_lsn={durable_lsn:?}"
    );
    reserved_column(result, table);
    assert!(
        live_absent && durable_absent,
        "refused CREATE installed a live or durable catalog entry"
    );
    assert_eq!(
        (live_lsn, durable_lsn),
        (before, before),
        "refused CREATE advanced the published LSN"
    );
    assert_eq!(
        durable_controls, controls,
        "ordinary-name controls changed after reopen"
    );
}
#[test]
fn cdb_240_create_refuses_reserved_non_key_column_durably() {
    create_refusal(
        "reserved_create",
        "CREATE TABLE reserved_create (id INTEGER PRIMARY KEY, row_id INTEGER)",
    );
}
#[test]
fn cdb_240_create_refuses_reserved_primary_key_durably() {
    create_refusal(
        "reserved_key",
        "CREATE TABLE reserved_key (row_id INTEGER PRIMARY KEY, payload TEXT)",
    );
}

fn alter_refusal(populated: bool) {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE reserved_alter (id INTEGER PRIMARY KEY, payload TEXT)",
    );
    if populated {
        sql(
            &store.db,
            "INSERT INTO reserved_alter VALUES (9001, 'kept-high')",
        );
        sql(
            &store.db,
            "INSERT INTO reserved_alter VALUES (4001, 'kept-low')",
        );
    }
    // The ordinary-name ALTER uses the same empty/populated path as the refusal.
    sql(
        &store.db,
        "ALTER TABLE reserved_alter ADD COLUMN graph_row_id INTEGER",
    );
    if populated {
        sql(
            &store.db,
            "UPDATE reserved_alter SET graph_row_id = 999 WHERE id = 9001",
        );
        sql(
            &store.db,
            "UPDATE reserved_alter SET graph_row_id = 444 WHERE id = 4001",
        );
    }
    let query = "SELECT id, payload, graph_row_id FROM reserved_alter ORDER BY id";
    let expected = if populated {
        vec![
            vec![
                Value::Int64(4001),
                Value::Text("kept-low".into()),
                Value::Int64(444),
            ],
            vec![
                Value::Int64(9001),
                Value::Text("kept-high".into()),
                Value::Int64(999),
            ],
        ]
    } else {
        vec![]
    };
    let before_rows = shape(&store.db, query).unwrap();
    assert_eq!(
        before_rows.1, expected,
        "sentinel fixture did not preserve its ordinary values"
    );
    let schema = store.db.table_meta("reserved_alter");
    let before = store.db.current_lsn();
    let result = store
        .db
        .execute(
            "ALTER TABLE reserved_alter ADD COLUMN row_id INTEGER",
            &HashMap::new(),
        )
        .map(|_| ());
    let live_schema = store.db.table_meta("reserved_alter");
    let live_lsn = store.db.current_lsn();
    let live_rows = shape(&store.db, query);
    let store = store.reopen();
    let durable_schema = store.db.table_meta("reserved_alter");
    let durable_lsn = store.db.current_lsn();
    let durable_rows = shape(&store.db, query);
    store.close();
    eprintln!(
        "CDB240_ALTER populated={populated} sentinel_rows={} result={result:?} live_schema_unchanged={} durable_schema_unchanged={} live_values_unchanged={} durable_values_unchanged={}",
        expected.len(),
        live_schema == schema,
        durable_schema == schema,
        live_rows.as_ref().ok() == Some(&before_rows),
        durable_rows.as_ref().ok() == Some(&before_rows)
    );
    reserved_column(result, "reserved_alter");
    assert_eq!(
        live_schema, schema,
        "refused ALTER changed the live catalog"
    );
    assert_eq!(
        durable_schema, schema,
        "refused ALTER changed the durable catalog"
    );
    assert_eq!(
        (live_lsn, durable_lsn),
        (before, before),
        "refused ALTER advanced the published LSN"
    );
    assert_eq!(
        live_rows.unwrap(),
        before_rows,
        "refused ALTER changed existing values"
    );
    assert_eq!(
        durable_rows.unwrap(),
        before_rows,
        "existing values changed after reopen"
    );
}
#[test]
fn cdb_240_alter_refuses_reserved_column_on_empty_table_durably() {
    alter_refusal(false);
}
#[test]
fn cdb_240_alter_refuses_reserved_column_on_populated_table_durably() {
    alter_refusal(true);
}

#[test]
fn cdb_228_select_returns_allocated_ids_after_filter_sort_and_reopen() {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE ordinary (id INTEGER PRIMARY KEY, included INTEGER, payload TEXT)",
    );
    // A retired allocation prevents visible-row enumeration from matching
    // the allocated IDs even before the filter and sort are applied.
    let tx = store.db.begin().unwrap();
    let retired = store
        .db
        .insert_row(
            tx,
            "ordinary",
            HashMap::from([
                ("id".into(), Value::Int64(11001)),
                ("included".into(), Value::Int64(0)),
                ("payload".into(), Value::Text("retired-predecessor".into())),
            ]),
        )
        .unwrap();
    store.db.commit(tx).unwrap();
    sql(&store.db, "DELETE FROM ordinary WHERE id = 11001");
    // Reverse natural-key order, with one middle allocation excluded later.
    let inputs = [
        (9001, 1, "high"),
        (7001, 1, "middle"),
        (5001, 0, "excluded"),
        (3001, 1, "low"),
    ];
    let tx = store.db.begin().unwrap();
    let mut allocated = Vec::new();
    for (key, included, payload) in inputs {
        let row_id = store
            .db
            .insert_row(
                tx,
                "ordinary",
                HashMap::from([
                    ("id".into(), Value::Int64(key)),
                    ("included".into(), Value::Int64(included)),
                    ("payload".into(), Value::Text(payload.into())),
                ]),
            )
            .unwrap();
        let engine_id = i64::try_from(row_id.0).unwrap();
        assert_ne!(engine_id, key, "natural key must differ from allocated ID");
        allocated.push((key, engine_id, included, payload));
    }
    store.db.commit(tx).unwrap();
    assert!(
        allocated
            .iter()
            .all(|row| row.1 != i64::try_from(retired.0).unwrap()),
        "retired identity must not be reused by this fixture"
    );
    let mut all = allocated.clone();
    all.sort_by_key(|row| row.0);
    let ordinary_rows = shape(
        &store.db,
        "SELECT id, included, payload FROM ordinary ORDER BY id",
    )
    .unwrap();
    assert_eq!(
        ordinary_rows.1,
        all.iter()
            .map(|row| vec![
                Value::Int64(row.0),
                Value::Int64(row.2),
                Value::Text(row.3.into())
            ])
            .collect::<Vec<_>>(),
        "ordinary projection must contain four live rows and no retired predecessor"
    );
    let mut selected: Vec<_> = allocated.iter().copied().filter(|row| row.2 == 1).collect();
    selected.sort_by_key(|row| row.0);
    assert_eq!(selected.len(), 3);
    assert!(
        selected
            .iter()
            .enumerate()
            .any(|(i, row)| row.1 != (i + 1) as i64),
        "fixture cannot distinguish allocated IDs from result numbering"
    );
    let bare_expected: Shape = (
        vec!["row_id".into()],
        selected
            .iter()
            .map(|row| vec![Value::Int64(row.1)])
            .collect(),
    );
    let mixed_expected: Shape = (
        vec!["row_id".into(), "id".into(), "payload".into()],
        selected
            .iter()
            .map(|row| {
                vec![
                    Value::Int64(row.1),
                    Value::Int64(row.0),
                    Value::Text(row.3.into()),
                ]
            })
            .collect(),
    );
    let later = allocated.iter().find(|row| row.0 == 7001).unwrap();
    let later_expected: Shape = (
        vec!["row_id".into(), "id".into()],
        vec![vec![Value::Int64(later.1), Value::Int64(later.0)]],
    );
    let queries = [
        (
            "bare",
            "SELECT row_id FROM ordinary WHERE included = 1 ORDER BY id",
        ),
        (
            "mixed",
            "SELECT row_id, id, payload FROM ordinary WHERE included = 1 ORDER BY id",
        ),
        ("later", "SELECT row_id, id FROM ordinary WHERE id = 7001"),
    ];
    let live: Vec<_> = queries
        .iter()
        .map(|(_, query)| shape(&store.db, query))
        .collect();
    let store = store.reopen();
    let durable: Vec<_> = queries
        .iter()
        .map(|(_, query)| shape(&store.db, query))
        .collect();
    store.close();
    // Capture all six results before any assertion so the missing bare
    // projection cannot hide the mixed, filtered-later or reopened answers.
    for (phase, answers) in [("live", &live), ("reopened", &durable)] {
        for ((label, _), result) in queries.iter().zip(answers) {
            eprintln!(
                "CDB228_SELECT phase={phase} query={label} allocated={allocated:?} result={result:?}"
            );
        }
    }
    for (phase, answers) in [("live", live), ("reopened", durable)] {
        for (((label, _), result), expected) in
            queries
                .iter()
                .zip(answers)
                .zip([&bare_expected, &mixed_expected, &later_expected])
        {
            assert_eq!(
                &result.unwrap(),
                expected,
                "{phase} {label} did not return the stored engine IDs with the requested columns/order"
            );
        }
    }
}

// Challenges 202/203: every schema door refuses the reserved name before
// publication, and wildcard joins hide identities without losing explicit ids.
#[test]
fn cdb_240_r2_rename_refuses_reserved_destination_without_changing_rows() {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE renamed (id INTEGER PRIMARY KEY, payload INTEGER)",
    );
    sql(&store.db, "INSERT INTO renamed VALUES (9001, 999)");
    let before = shape(&store.db, "SELECT id, payload FROM renamed").unwrap();
    let meta = store.db.table_meta("renamed");
    let lsn = store.db.current_lsn();
    let result = store
        .db
        .execute(
            "ALTER TABLE renamed RENAME COLUMN payload TO row_id",
            &HashMap::new(),
        )
        .map(|_| ());
    let live_meta = store.db.table_meta("renamed");
    let live_rows = shape(&store.db, "SELECT id, payload FROM renamed");
    let live_lsn = store.db.current_lsn();
    let store = store.reopen();
    let durable_meta = store.db.table_meta("renamed");
    let durable_rows = shape(&store.db, "SELECT id, payload FROM renamed");
    let durable_lsn = store.db.current_lsn();
    store.close();
    reserved_column(result, "renamed");
    assert_eq!((live_meta, durable_meta), (meta.clone(), meta));
    assert_eq!(
        (live_rows.unwrap(), durable_rows.unwrap()),
        (before.clone(), before)
    );
    assert_eq!((live_lsn, durable_lsn), (lsn, lsn));
}

fn received_vector_alter(column: &str) -> DdlChange {
    DdlChange::AlterTable {
        name: "received_reserved".into(),
        columns: vec![
            ("id".into(), "INTEGER PRIMARY KEY".into()),
            (column.into(), "VECTOR(3)".into()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

#[test]
fn cdb_240_r2_sync_vector_alter_refuses_reserved_column_atomically() {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE received_reserved (id INTEGER PRIMARY KEY)",
    );
    sql(&store.db, "INSERT INTO received_reserved VALUES (9001)");
    let before = shape(&store.db, "SELECT id FROM received_reserved").unwrap();
    let meta = store.db.table_meta("received_reserved");
    let lsn = store.db.current_lsn();
    let result = store
        .db
        .apply_changes(
            ChangeSet {
                ddl: vec![received_vector_alter("row_id")],
                ddl_lsn: vec![Lsn(90)],
                ..ChangeSet::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .map(|_| ());
    let live_meta = store.db.table_meta("received_reserved");
    let live_rows = shape(&store.db, "SELECT id FROM received_reserved");
    let live_lsn = store.db.current_lsn();
    let store = store.reopen();
    let durable_meta = store.db.table_meta("received_reserved");
    let durable_rows = shape(&store.db, "SELECT id FROM received_reserved");
    let durable_lsn = store.db.current_lsn();
    store.close();
    reserved_column(result, "received_reserved");
    assert_eq!((live_meta, durable_meta), (meta.clone(), meta));
    assert_eq!(
        (live_rows.unwrap(), durable_rows.unwrap()),
        (before.clone(), before)
    );
    assert_eq!((live_lsn, durable_lsn), (lsn, lsn));
}

#[test]
fn cdb_240_r2_typed_received_metadata_refuses_reserved_column() {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE received_reserved (id INTEGER PRIMARY KEY)",
    );
    // The ordinary-name full vector shape must be accepted by this exact path.
    store
        .db
        .project_received_schema_source_order(&[received_vector_alter("embedding")])
        .unwrap();
    let result = store
        .db
        .project_received_schema_source_order(&[received_vector_alter("row_id")])
        .map(|_| ());
    store.close();
    reserved_column(result, "received_reserved");
}

fn joined_store() -> (Store, i64) {
    let store = Store::new();
    sql(
        &store.db,
        "CREATE TABLE a (id INTEGER PRIMARY KEY, payload TEXT)",
    );
    sql(
        &store.db,
        "CREATE TABLE b (id INTEGER PRIMARY KEY, other TEXT)",
    );
    let tx = store.db.begin().unwrap();
    let id = store
        .db
        .insert_row(
            tx,
            "a",
            HashMap::from([
                ("id".into(), Value::Int64(9001)),
                ("payload".into(), Value::Text("left".into())),
            ]),
        )
        .unwrap();
    store.db.commit(tx).unwrap();
    sql(&store.db, "INSERT INTO b VALUES (9001, 'right')");
    (store, i64::try_from(id.0).unwrap())
}

#[test]
fn cdb_240_r2_wildcard_join_hides_both_qualified_engine_ids() {
    let (store, _) = joined_store();
    let answer = shape(&store.db, "SELECT * FROM a JOIN b ON a.id = b.id").unwrap();
    store.close();
    assert_eq!(
        answer.0,
        vec!["a.id", "a.payload", "b.id", "b.other"],
        "wildcard JOIN exposed hidden engine identities"
    );
    assert_eq!(
        answer.1,
        vec![vec![
            Value::Int64(9001),
            Value::Text("left".into()),
            Value::Int64(9001),
            Value::Text("right".into())
        ]]
    );
}

#[test]
fn cdb_228_r2_explicit_qualified_join_id_is_the_stored_identity() {
    let (store, stored) = joined_store();
    assert_ne!(stored, 9001);
    let live = shape(&store.db, "SELECT a.row_id FROM a JOIN b ON a.id = b.id").unwrap();
    let store = store.reopen();
    let durable = shape(&store.db, "SELECT a.row_id FROM a JOIN b ON a.id = b.id").unwrap();
    store.close();
    assert_eq!(live.0.len(), 1);
    assert!(matches!(live.0[0].as_str(), "row_id" | "a.row_id"));
    assert_eq!(live.1, vec![vec![Value::Int64(stored)]]);
    assert_eq!(durable, live);
}
