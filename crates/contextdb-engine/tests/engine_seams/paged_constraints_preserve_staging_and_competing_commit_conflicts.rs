//! Acceptance: "Exercise eviction and reopen with valid and missing parents, duplicate keys,
//! staged inserts/deletes and competing commits. Prove bounded indexed probes or bounded-memory
//! scans preserve the transaction's staged changes, committed view and conflict semantics."

use contextdb_core::{Error, Value, Wallclock};
use contextdb_engine::writer_query_test_seams::observe;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

#[test]
fn paged_constraints_preserve_staging_and_competing_commit_conflicts() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let params = HashMap::new();
    let mut measurements = Vec::new();
    for history in [16, 32] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("constraints.redb");
        let database = Database::open(&path).unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        for sql in [
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, payload INTEGER) HISTORY ALL",
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id)) HISTORY ALL",
            "CREATE TABLE tuples (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b)) HISTORY ALL",
            "CREATE TABLE unrelated (id INTEGER PRIMARY KEY) HISTORY ALL",
            "INSERT INTO parents VALUES (1, 0)",
            "INSERT INTO tuples VALUES (1, 7, 8), (2, NULL, 8), (3, NULL, 8), (4, 9, 10)",
        ] {
            database.execute(sql, &params).unwrap();
        }
        for ordinal in 0..history {
            database
                .execute(
                    &format!("UPDATE parents SET payload = {ordinal} WHERE id = 1"),
                    &params,
                )
                .unwrap();
            database
                .execute(
                    &format!("INSERT INTO unrelated VALUES ({ordinal})"),
                    &params,
                )
                .unwrap();
        }
        database.close().unwrap();
        drop(database);
        let database = Database::open(&path).unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let duplicate = database
            .execute("INSERT INTO parents VALUES (1, 99)", &params)
            .unwrap_err();
        assert!(
            matches!(duplicate, Error::UniqueViolation { ref table, ref column } if table == "parents" && column == "id"),
            "duplicate primary key retains its exact refusal: {duplicate:?}"
        );
        let null = database
            .execute("INSERT INTO parents VALUES (NULL, 99)", &params)
            .unwrap_err();
        assert!(
            matches!(null, Error::Other(ref message) if message == "NOT NULL constraint violated: parents.id"),
            "null primary key retains its exact refusal: {null:?}"
        );
        let tuple = database
            .execute("UPDATE tuples SET a = 7, b = 8 WHERE id = 4", &params)
            .unwrap_err();
        assert!(
            matches!(tuple, Error::UniqueViolation { ref table, ref column } if table == "tuples" && column == "a, b"),
            "composite uniqueness names the complete key: {tuple:?}"
        );
        assert_eq!(
            database
                .execute("SELECT id FROM tuples WHERE a IS NULL ORDER BY id", &params)
                .unwrap()
                .rows,
            vec![vec![Value::Int64(2)], vec![Value::Int64(3)]],
            "nullable composite unique values do not collide"
        );
        database
            .execute("INSERT INTO children VALUES (1, 1)", &params)
            .unwrap();
        let missing = database
            .execute("INSERT INTO children VALUES (2, 999)", &params)
            .unwrap_err();
        assert!(
            matches!(missing, Error::ForeignKeyViolation { ref child_table, ref child_columns, ref parent_table, ref parent_columns } if child_table == "children" && child_columns == &vec!["parent_id".to_owned()] && parent_table == "parents" && parent_columns == &vec!["id".to_owned()]),
            "missing parent retains its exact foreign-key refusal: {missing:?}"
        );
        let staged = database.begin_or_panic();
        database
            .execute_in_tx(staged, "INSERT INTO parents VALUES (2, 1)", &params)
            .unwrap();
        database
            .execute_in_tx(staged, "INSERT INTO children VALUES (3, 2)", &params)
            .unwrap();
        database.commit(staged).unwrap();
        let deleted = database
            .execute("DELETE FROM parents WHERE id = 2", &params)
            .unwrap_err();
        assert!(
            matches!(deleted, Error::ForeignKeyViolation { .. }),
            "deleting a referenced parent is refused: {deleted:?}"
        );
        let tx = database.begin_or_panic();
        database
            .execute_in_tx(tx, "DELETE FROM children WHERE id = 1", &params)
            .unwrap();
        database
            .execute_in_tx(tx, "DELETE FROM parents WHERE id = 1", &params)
            .unwrap();
        for ordinal in 0..history {
            database
                .execute_in_tx(
                    tx,
                    &format!("INSERT INTO unrelated VALUES ({})", 1000 + ordinal),
                    &params,
                )
                .unwrap();
        }
        let (replacement, probe) =
            observe(|| database.execute_in_tx(tx, "INSERT INTO parents VALUES (1, 777)", &params));
        replacement.unwrap();
        database
            .execute_in_tx(tx, "INSERT INTO children VALUES (1, 1)", &params)
            .unwrap();
        assert_eq!(
            database
                .execute_in_tx(tx, "SELECT payload FROM parents WHERE id = 1", &params)
                .unwrap()
                .rows,
            vec![vec![Value::Int64(777)]],
            "staged replacement is visible to its constraint checks and reads"
        );
        assert_eq!(
            database
                .execute("SELECT payload FROM parents WHERE id = 1", &params)
                .unwrap()
                .rows,
            vec![vec![Value::Int64(history - 1)]],
            "replacement remains private before commit"
        );
        database.commit(tx).unwrap();
        let first = database.begin_or_panic();
        let second = database.begin_or_panic();
        database
            .execute_in_tx(first, "INSERT INTO parents VALUES (3, 1)", &params)
            .unwrap();
        database
            .execute_in_tx(second, "INSERT INTO parents VALUES (3, 2)", &params)
            .unwrap();
        database.commit(first).unwrap();
        let conflict = database.commit(second).unwrap_err();
        assert!(
            matches!(conflict, Error::UniqueViolation { ref table, ref column } if table == "parents" && column == "id"),
            "competing commit preserves the existing duplicate refusal: {conflict:?}"
        );
        assert_eq!(
            database
                .execute("SELECT id, payload FROM parents ORDER BY id", &params)
                .unwrap()
                .rows,
            vec![
                vec![Value::Int64(1), Value::Int64(777)],
                vec![Value::Int64(2), Value::Int64(1)],
                vec![Value::Int64(3), Value::Int64(1)]
            ],
            "the winner and atomic replacement survive the refused competing commit"
        );
        database.close().unwrap();
        measurements.push((
            probe.constraint_staged_rows_examined,
            probe.constraint_postings_examined,
        ));
    }
    assert_eq!(
        measurements[0], measurements[1],
        "one replacement constraint probe must examine equal staged rows and postings when unrelated staging and superseded key versions double"
    );
    assert!(
        measurements[0].0 <= 2 && measurements[0].1 <= 2,
        "one replacement probe is bounded independently of the staged set and key history: {measurements:?}"
    );
}
