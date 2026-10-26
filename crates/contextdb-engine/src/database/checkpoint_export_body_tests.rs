include!("durable_test_observations.rs");
use super::*;
use crate::Database;
use contextdb_core::Value;
use std::collections::HashMap;
use std::path::Path;

/// Big enough that the disk-backed committed directory sheds it, which is
/// the whole reason the export has to go back to the store for it.
const WIDE_BODY: usize = 8 * 1024;

fn seeded(path: &Path) -> Database {
    let database = Database::open(path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the exported table");
    database
        .execute(
            "INSERT INTO notes (id, body) VALUES (1, $body)",
            &HashMap::from([("body".to_owned(), Value::Text("n".repeat(WIDE_BODY)))]),
        )
        .expect("commit one wide body");
    database
}

/// Select exact physical identities from the published durable root before
/// narrowing the export budget; the checkpoint loader fills their bodies.
fn exportable_rows(database: &Database, table: &str) -> Vec<VersionedRow> {
    durable_observation::rows(database, table)
}

/// An artifact claims one commit boundary and every body in it. A body the
/// captured snapshot cannot produce is not something to paper over with
/// whatever the directory happened to keep: the row would restore with a
/// payload it never had. So the export stops instead, and it stops the
/// same way whether the row is gone or the whole table is.
#[test]
fn an_exported_body_the_captured_snapshot_cannot_produce_stops_the_export() {
    let directory = tempfile::TempDir::new().expect("task-scoped export directory");
    let path = directory.path().join("export-body.db");
    let database = seeded(&path);
    let meta = database.table_meta("notes").expect("the declared table");
    let source = database
        .persistence
        .as_ref()
        .expect("a stored database has a checkpoint source")
        .checkpoint_read_source()
        .expect("capture the pinned durable source");

    let mut rows = exportable_rows(&database, "notes");
    assert_eq!(rows.len(), 1, "one committed row to export");
    rows[0]
        .values
        .insert("body".to_owned(), Value::Text(String::new()));
    let (filled, admitted) = source
        .load_export_row_values("notes", &meta, &mut rows)
        .expect("the pinned source restores the committed body");
    assert_eq!(filled, 1, "the whole run fits the default budget");
    drop(admitted);
    assert_eq!(
        rows[0].values.get("body"),
        Some(&Value::Text("n".repeat(WIDE_BODY))),
        "the healthy body comes back whole from the snapshot the artifact claims"
    );

    let mut absent_row = rows.clone();
    absent_row[0].lsn = Lsn(absent_row[0].lsn.0.saturating_add(1_000));
    let refused = source
        .load_export_row_values("notes", &meta, &mut absent_row)
        .map(|(filled, _)| filled)
        .expect_err("a row the captured snapshot does not hold stops the export");
    assert!(
        matches!(refused, Error::StoreCorrupted { .. }),
        "a body the snapshot cannot produce is a corruption refusal, not a silent gap: \
             {refused}"
    );

    let mut orphan_table = rows.clone();
    let missing = source
        .load_export_row_values("never_created", &meta, &mut orphan_table)
        .map(|(filled, _)| filled)
        .expect_err("a table the captured snapshot does not hold stops the export");
    assert!(
        matches!(missing, Error::StoreCorrupted { .. }),
        "a table the snapshot cannot produce is a corruption refusal too: {missing}"
    );

    drop(source);
    database.close().expect("release the store");
}

/// An export of bodies the directory has shed is a copy out of the store,
/// and a copy is memory. It is admitted through the same budget the rest
/// of the database answers to, before the body is read out, and an export
/// whose bodies do not all fit at once comes back in runs that do rather
/// than in one copy nothing admitted. A budget that cannot hold even one
/// body is the typed refusal, because there is no smaller run to fall
/// back to.
#[test]
fn exported_bodies_come_back_in_runs_the_memory_budget_admits() {
    const BODIES: usize = 4;
    let directory = tempfile::TempDir::new().expect("task-scoped export directory");
    let path = directory.path().join("export-admission.db");
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the exported table");
    for id in 0..BODIES {
        database
            .execute(
                "INSERT INTO notes (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id as i64)),
                    ("body".to_owned(), Value::Text("n".repeat(WIDE_BODY))),
                ]),
            )
            .expect("commit one wide body");
    }
    let meta = database.table_meta("notes").expect("the declared table");
    let source = database
        .persistence
        .as_ref()
        .expect("a stored database has a checkpoint source")
        .checkpoint_read_source()
        .expect("capture the pinned durable source");
    let mut rows = exportable_rows(&database, "notes");
    assert_eq!(rows.len(), BODIES, "one committed row per seeded body");

    // Room for one body's copy and not for all of them.
    let one_body = WIDE_BODY.saturating_mul(2).saturating_add(4096);
    database
        .set_memory_limit(Some(
            database.accountant().usage().used.saturating_add(one_body),
        ))
        .expect("declare a budget one body fits in");
    let (filled, admitted) = source
        .load_export_row_values("notes", &meta, &mut rows)
        .expect("a run the budget admits is not a refusal");
    assert!(
        (1..BODIES).contains(&filled),
        "the run stops at the last body the budget admitted, not at the batch: {filled}"
    );
    assert!(
        database.accountant().usage().used > 0,
        "the copied bodies are charged while the caller still holds them"
    );
    for row in &mut rows[..filled] {
        row.values = HashMap::new();
    }
    drop(admitted);

    let (next, admitted) = source
        .load_export_row_values("notes", &meta, &mut rows[filled..])
        .expect("the next run picks up where the budget stopped the last one");
    assert!(next >= 1, "every run makes progress");
    drop(admitted);

    // A budget with no room for one body at all.
    database
        .set_memory_limit(Some(database.accountant().usage().used.saturating_add(8)))
        .expect("declare a budget no body fits in");
    let refused = source
        .load_export_row_values("notes", &meta, &mut rows)
        .map(|(filled, _)| filled)
        .expect_err("a body that does not fit on its own is refused");
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "a body the budget cannot hold is the typed memory refusal: {refused}"
    );

    drop(source);
    database.set_memory_limit(None).expect("lift the budget");
    database.close().expect("release the store");
}
