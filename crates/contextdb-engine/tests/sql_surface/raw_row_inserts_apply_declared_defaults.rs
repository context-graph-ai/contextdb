//! Raw row inserts honor the same declared defaults and supplied values as SQL.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn raw_insert(
    db: &Database,
    id: i64,
    cells: &[(&str, Value)],
    label: &str,
    checks: &mut Vec<(String, bool)>,
) {
    let mut values = HashMap::from([("id".to_owned(), Value::Int64(id))]);
    values.extend(
        cells
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone())),
    );
    let tx = db.begin().unwrap();
    match db.insert_row(tx, "items", values) {
        Ok(_) => {
            db.commit(tx).unwrap();
        }
        Err(error) => {
            checks.push((
                format!("{label}: raw insert {id} refused: {error:?}"),
                false,
            ));
            db.rollback(tx).unwrap();
        }
    }
}

fn sql_insert(db: &Database, id: i64, cells: &[(&str, Value)]) {
    let mut values = HashMap::from([("id".to_owned(), Value::Int64(id))]);
    values.extend(
        cells
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone())),
    );
    let mut columns = vec!["id".to_owned()];
    columns.extend(cells.iter().map(|(name, _)| (*name).to_owned()));
    let placeholders: Vec<_> = columns
        .iter()
        .map(|name| format!("{}{name}", '$'))
        .collect();
    let sql = format!(
        "INSERT INTO items ({}) VALUES ({})",
        columns.join(", "),
        placeholders.join(", ")
    );
    assert_eq!(db.execute(&sql, &values).unwrap().rows_affected(), 1);
}

fn record_row(
    db: &Database,
    id: i64,
    expected: &[(&str, Value)],
    label: &str,
    checks: &mut Vec<(String, bool)>,
) {
    let row = db
        .point_lookup("items", "id", &Value::Int64(id), db.snapshot())
        .unwrap();
    // A DEFAULT NULL is a stored cell too: a projection alone would make an
    // absent raw cell look identical to an explicitly defaulted NULL cell.
    let matches = row.as_ref().is_some_and(|row| {
        expected
            .iter()
            .all(|(name, value)| row.values.get(*name) == Some(value))
    });
    checks.push((
        format!(
            "{label}: id={id}, stored={:?}, expected={expected:?}",
            row.map(|row| row.values)
        ),
        matches,
    ));
}

fn check_defaults(ddl: &str, defaults: &[(&str, Value)], explicit: &[(&str, Value)]) {
    let clock = Arc::new(AtomicU64::new(1_700_000_000_000));
    let _clock = {
        let clock = Arc::clone(&clock);
        Wallclock::test_clock_guard(move || clock.load(Ordering::SeqCst))
    };
    let mut checks = Vec::new();
    for on_disk in [false, true] {
        clock.store(1_700_000_000_000, Ordering::SeqCst);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("row-defaults.db");
        let db = if on_disk {
            Database::open(&path).unwrap()
        } else {
            Database::open_memory()
        };
        db.execute(ddl, &HashMap::new()).unwrap();
        let label = format!("disk={on_disk}");
        sql_insert(&db, 1, &[]);
        raw_insert(&db, 2, &[], &label, &mut checks);
        sql_insert(&db, 3, explicit);
        raw_insert(&db, 4, explicit, &label, &mut checks);
        for (id, expected) in [(1, defaults), (2, defaults), (3, explicit), (4, explicit)] {
            record_row(
                &db,
                id,
                expected,
                &format!("{label}: first read"),
                &mut checks,
            );
        }
        // Change the clock through the canonical seam, never by waiting.
        // Reading a stored default must not re-evaluate its expression.
        clock.store(1_700_000_060_000, Ordering::SeqCst);
        for (id, expected) in [(1, defaults), (2, defaults), (3, explicit), (4, explicit)] {
            record_row(
                &db,
                id,
                expected,
                &format!("{label}: later read"),
                &mut checks,
            );
        }
        db.close().unwrap();
        if on_disk {
            let session = ReadSession::open(&path).unwrap();
            assert_eq!(
                session.route(),
                contextdb_core::read_contract::ReadRoute::File
            );
            let columns = defaults
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT {columns} FROM items WHERE id = $id");
            for (id, expected) in [(1, defaults), (2, defaults), (3, explicit), (4, explicit)] {
                let result = session
                    .execute(&sql, &HashMap::from([("id".to_owned(), Value::Int64(id))]))
                    .unwrap();
                let wanted = vec![
                    expected
                        .iter()
                        .map(|(_, value)| value.clone())
                        .collect::<Vec<_>>(),
                ];
                checks.push((
                    format!(
                        "{label}: file read id={id}, rows={:?}, expected={wanted:?}",
                        result.rows()
                    ),
                    result.rows() == wanted,
                ));
            }
            drop(session);
            let reopened = Database::open(&path).unwrap();
            for (id, expected) in [(1, defaults), (2, defaults), (3, explicit), (4, explicit)] {
                record_row(
                    &reopened,
                    id,
                    expected,
                    &format!("{label}: reopened"),
                    &mut checks,
                );
            }
            reopened.close().unwrap();
        }
    }
    assert!(
        checks.iter().all(|(_, passed)| *passed),
        "raw and SQL defaults must be stored alike and supplied cells must survive: {checks:?}"
    );
}

#[test]
fn raw_row_inserts_apply_typed_literal_defaults_and_preserve_explicit_values() {
    use uuid::Uuid;
    check_defaults(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL DEFAULT 7, score REAL NOT NULL DEFAULT 0.0, marker UUID NOT NULL DEFAULT '00000000-0000-0000-0000-00000000ed01', label TEXT NOT NULL DEFAULT 'fallback')",
        &[
            ("qty", Value::Int64(7)),
            ("score", Value::Float64(0.0)),
            ("marker", Value::Uuid(Uuid::from_u128(0xed01))),
            ("label", Value::Text("fallback".into())),
        ],
        &[
            ("qty", Value::Int64(-3)),
            ("score", Value::Float64(0.25)),
            ("marker", Value::Uuid(Uuid::from_u128(0xed02))),
            ("label", Value::Text("supplied".into())),
        ],
    );
}

#[test]
fn raw_row_inserts_store_null_defaults_and_preserve_explicit_values() {
    check_defaults(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT DEFAULT NULL)",
        &[("note", Value::Null)],
        &[("note", Value::Text("supplied".into()))],
    );
}

#[test]
fn raw_row_inserts_evaluate_now_once_and_preserve_explicit_timestamps() {
    check_defaults(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, inserted_at TIMESTAMP NOT NULL DEFAULT NOW())",
        &[("inserted_at", Value::Timestamp(1_700_000_000))],
        &[("inserted_at", Value::Timestamp(123))],
    );
}
