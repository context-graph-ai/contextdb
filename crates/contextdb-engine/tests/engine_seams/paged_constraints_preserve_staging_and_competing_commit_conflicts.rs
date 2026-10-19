//! Acceptance: "Exercise eviction and reopen with valid and missing parents, duplicate keys,
//! staged inserts/deletes and competing commits. Prove bounded indexed probes or bounded-memory
//! scans preserve the transaction's staged changes, committed view and conflict semantics."

use contextdb_core::{Error, Value, Wallclock};
use contextdb_engine::writer_query_test_seams::observe_target_key;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

#[test]
fn paged_constraints_preserve_staging_and_competing_commit_conflicts() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let params = HashMap::new();
    let mut measurements = Vec::new();
    let mut cold_measurements = Vec::new();
    for history in [16, 32] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("constraints.redb");
        // A 64 MiB writer limit derives a 4 MiB cache, below the 128 MiB displacement set.
        let database = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                memory_limit: Some(64 * 1024 * 1024),
                ..Default::default()
            },
        )
        .unwrap();
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
        let historical = database.snapshot();
        for ordinal in 0..history {
            database
                .execute(
                    &format!("UPDATE parents SET payload = {ordinal} WHERE id = 1"),
                    &params,
                )
                .unwrap();
            database
                .execute("UPDATE tuples SET a = 7, b = 8 WHERE id = 1", &params)
                .unwrap();
            database
                .execute(
                    &format!("INSERT INTO unrelated VALUES ({ordinal})"),
                    &params,
                )
                .unwrap();
        }
        database
            .execute(
                "CREATE TABLE displacement (id INTEGER PRIMARY KEY, body TEXT)",
                &params,
            )
            .unwrap();
        for batch in 0..16 {
            let mut params =
                HashMap::from([("body".to_owned(), Value::Text("p".repeat(128 * 1024)))]);
            let values = (0..64)
                .map(|offset| {
                    let name = format!("id{offset}");
                    params.insert(name.clone(), Value::Int64(batch * 64 + offset));
                    format!("(${name},$body)")
                })
                .collect::<Vec<_>>()
                .join(",");
            database
                .execute(
                    &format!("INSERT INTO displacement VALUES {values}"),
                    &params,
                )
                .unwrap();
        }
        database.close().unwrap();
        drop(database);
        // A 64 MiB writer limit derives a 4 MiB cache, below the 128 MiB displacement set.
        let database = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                memory_limit: Some(64 * 1024 * 1024),
                ..Default::default()
            },
        )
        .unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let pin = database.pin_snapshot(historical).unwrap();
        let displace = || {
            let before = database.__writer_cache_stats_for_test().unwrap();
            // The ordinal seam walks every prior wide leaf; visit the range once.
            drop(
                database
                    .__hold_row_page_for_test("displacement", 1023)
                    .unwrap(),
            );
            let after = database.__writer_cache_stats_for_test().unwrap();
            assert!(
                after.2 > before.2 && after.1 > before.1,
                "real row-page reads displace the cache: before={before:?}; after={after:?}"
            );
        };
        displace();
        let (duplicate, primary_work) = observe_target_key("parents", &[Value::Int64(1)], || {
            database.execute("INSERT INTO parents VALUES (1, 99)", &params)
        });
        let duplicate = duplicate.unwrap_err();
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
        displace();
        let (tuple, unique_work) =
            observe_target_key("tuples", &[Value::Int64(7), Value::Int64(8)], || {
                database.execute("UPDATE tuples SET a = 7, b = 8 WHERE id = 4", &params)
            });
        let tuple = tuple.unwrap_err();
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
        displace();
        let (valid, parent_work) = observe_target_key("parents", &[Value::Int64(1)], || {
            database.execute("INSERT INTO children VALUES (1, 1)", &params)
        });
        valid.unwrap();
        displace();
        let (missing, missing_work) = observe_target_key("parents", &[Value::Int64(999)], || {
            database.execute("INSERT INTO children VALUES (2, 999)", &params)
        });
        let missing = missing.unwrap_err();
        assert!(
            matches!(missing, Error::ForeignKeyViolation { ref child_table, ref child_columns, ref parent_table, ref parent_columns } if child_table == "children" && child_columns == &vec!["parent_id".to_owned()] && parent_table == "parents" && parent_columns == &vec!["id".to_owned()]),
            "missing parent retains its exact foreign-key refusal: {missing:?}"
        );
        // A child staged against a parent that a competing commit deletes must
        // be rejected at commit using the newly committed parent state.
        database
            .execute("INSERT INTO parents VALUES (50, 0)", &params)
            .unwrap();
        let child = database.begin_or_panic();
        database
            .execute_in_tx(child, "INSERT INTO children VALUES (50, 50)", &params)
            .unwrap();
        database
            .execute("DELETE FROM parents WHERE id = 50", &params)
            .unwrap();
        displace();
        let (removed, removed_work) =
            observe_target_key("parents", &[Value::Int64(50)], || database.commit(child));
        let removed = removed.unwrap_err();
        assert!(
            matches!(removed, Error::ForeignKeyViolation { .. }),
            "competing parent delete invalidates the staged child: {removed:?}"
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
        displace();
        let (replacement, probe) = observe_target_key("parents", &[Value::Int64(1)], || {
            database.execute_in_tx(tx, "INSERT INTO parents VALUES (1, 777)", &params)
        });
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
        displace();
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
        displace();
        let (conflict, competing_work) =
            observe_target_key("parents", &[Value::Int64(3)], || database.commit(second));
        let conflict = conflict.unwrap_err();
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
        assert_eq!(
            database
                .execute_at_snapshot(
                    "SELECT payload FROM parents WHERE id = 1",
                    &params,
                    historical
                )
                .unwrap()
                .rows,
            vec![vec![Value::Int64(0)]],
            "pinned historical parent survives displaced replacement and competing commits"
        );
        assert_eq!(
            database
                .execute_at_snapshot("SELECT a,b FROM tuples WHERE id = 1", &params, historical)
                .unwrap()
                .rows,
            vec![vec![Value::Int64(7), Value::Int64(8)]],
            "pinned composite key remains visible independently of current postings"
        );
        drop(pin);
        cold_measurements.push([
            primary_work,
            unique_work,
            parent_work,
            missing_work,
            probe,
            removed_work,
            competing_work,
        ]);
        database.close().unwrap();
        measurements.push((
            probe.constraint_staged_rows_examined,
            probe.constraint_postings_examined,
        ));
    }
    eprintln!("target-specific N/2N cold constraint observations: {cold_measurements:?}");
    for (arm, probes) in cold_measurements.iter().enumerate() {
        for kind in [0, 1, 2] {
            assert!(
                probes[kind].target_entries_examined > 0,
                "primary, composite unique and valid parent controls must examine actual inputs: arm={arm}; kind={kind}; {:?}",
                probes[kind]
            );
        }
        for (kind, work) in probes.iter().enumerate() {
            assert!(
                work.target_source_calls[1] > 0 && work.target_backend_bytes[1] > 0,
                "a displaced target key must read its own durable posting pages (unrelated UPDATE/child work cannot satisfy this): arm={arm}; kind={kind}; {work:?}"
            );
        }
    }
    eprintln!("target-specific cold constraint observations: {cold_measurements:?}");
    for (kind, (small, large)) in cold_measurements[0]
        .iter()
        .zip(&cold_measurements[1])
        .enumerate()
    {
        assert_eq!(
            small.target_entries_examined, large.target_entries_examined,
            "primary/composite/valid-parent/missing-parent actual examined entries ignore doubled superseded primary/composite/parent history: kind={kind}"
        );
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
