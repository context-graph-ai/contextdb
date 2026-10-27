use super::*;
use redb::ReadableDatabase;
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn test_b_public_current_point_list_and_join_refuse_committed_history_damage() {
    // Public historical point, finite-list and join reads keep complete
    // SQL answers after reopen and refuse corrupted consumed groups.
    {
        let mut failed_public_rows = Vec::new();
        for (shape, sql) in [
            ("point", "SELECT id, body FROM version_rows WHERE id = 1"),
            (
                "finite_list",
                "SELECT id, body FROM version_rows WHERE id IN (1, 2) ORDER BY id",
            ),
            (
                "join",
                "SELECT v.id, v.body FROM version_rows v INNER JOIN control_rows c ON v.id = c.id WHERE v.id IN (1, 2) ORDER BY v.id",
            ),
        ] {
            for damage in [
                "injected_extra_version",
                "missing_interior_version",
                "entirely_missing_deleted_group",
                "reordered_version_bodies",
                "false_terminal_absence",
            ] {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    use redb::ReadableTable;
                    let directory = tempfile::tempdir().unwrap();
                    let path = directory.path().join("public-historical-sql.db");
                    let db = Database::open(&path).unwrap();
                    db.execute(
                        "CREATE TABLE version_rows (id INT PRIMARY KEY, body TEXT)",
                        &HashMap::new(),
                    )
                    .unwrap();
                    db.execute(
                        "CREATE TABLE control_rows (id INT PRIMARY KEY, body TEXT)",
                        &HashMap::new(),
                    )
                    .unwrap();
                    db.execute(
                        "INSERT INTO control_rows (id, body) VALUES (1, 'unaffected')",
                        &HashMap::new(),
                    )
                    .unwrap();
                    db.execute(
                        "INSERT INTO version_rows (id, body) VALUES (1, 'first')",
                        &HashMap::new(),
                    )
                    .unwrap();
                    db.execute(
                        "UPDATE version_rows SET body = 'interior' WHERE id = 1",
                        &HashMap::new(),
                    )
                    .unwrap();
                    let historical = db.writer_read_root().unwrap().unwrap().snapshot;
                    let interior = db
                        .scan("version_rows", historical)
                        .unwrap()
                        .into_iter()
                        .next()
                        .unwrap();
                    db.execute(
                        "UPDATE version_rows SET body = 'head' WHERE id = 1",
                        &HashMap::new(),
                    )
                    .unwrap();
                    if damage == "entirely_missing_deleted_group" {
                        db.execute("DELETE FROM version_rows WHERE id = 1", &HashMap::new())
                            .unwrap();
                    }
                    let current = db.writer_read_root().unwrap().unwrap().snapshot;
                    let control_before = db
                        .scan("control_rows", current)
                        .unwrap()
                        .into_iter()
                        .map(|row| (row.row_id, row.values))
                        .collect::<Vec<_>>();
                    let queries = [sql];
                    let controls = [historical, current].map(|snapshot| {
                        queries.map(|sql| {
                            let result = db
                                .execute_at_snapshot(sql, &HashMap::new(), snapshot)
                                .unwrap();
                            (
                                result.rows().to_vec(),
                                result.trace().physical_plan,
                                result.trace().index_used.clone(),
                            )
                        })
                    });
                    drop(db);
                    {
                        let reopened = Database::open(&path).unwrap();
                        for (snapshot, expected) in [historical, current].into_iter().zip(&controls)
                        {
                            for (sql, rows) in queries.into_iter().zip(expected) {
                                #[cfg(any(test, feature = "test-seams"))]
                                let (result, work) = crate::writer_query_test_seams::observe_target(
                                    "version_rows",
                                    || reopened.execute_at_snapshot(sql, &HashMap::new(), snapshot),
                                );
                                #[cfg(not(any(test, feature = "test-seams")))]
                                let result =
                                    reopened.execute_at_snapshot(sql, &HashMap::new(), snapshot);
                                let result = result.unwrap();
                                #[cfg(any(test, feature = "test-seams"))]
                                {
                                    assert_eq!(
                                        result.rows().to_vec(),
                                        rows.0,
                                        "{shape}/{damage}: the public read returns the fresh control for the exact requested snapshot"
                                    );
                                    if !rows.0.is_empty() {
                                        assert!(
                                            work.target_source_calls
                                                [crate::writer_query_test_seams::PhysicalFamily::Row
                                                    as usize]
                                                > 0,
                                            "{shape}/{damage}: actual source work reads the target table's row family"
                                        );
                                    }
                                }
                                assert_eq!(
                                    (
                                        result.rows().to_vec(),
                                        result.trace().physical_plan,
                                        result.trace().index_used.clone()
                                    ),
                                    *rows,
                                    "{shape}/{damage}: public historical/current SQL after reopen equals fresh same-snapshot complete values and actual plan"
                                );
                            }
                        }
                        drop(reopened);
                    }

                    // The production decoder identifies the interior member. The raw
                    // mutation below copies production-encoded bytes without a codec.
                    let fixture_open_accountant_87263 =
                        std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
                    let _fixture_decode_scope_87263 =
                        crate::read_image_memory::ImageMemory::enter_writer_operation(
                            std::sync::Arc::clone(&fixture_open_accountant_87263).into(),
                        )
                        .unwrap();
                    let store = RedbPersistence::open(&path).unwrap();
                    store.attach_memory_accountant(
                        std::sync::Arc::clone(&fixture_open_accountant_87263).into(),
                    );
                    let versions = store.load_relational_table("version_rows").unwrap();
                    let interior_position = versions
                        .iter()
                        .position(|row| {
                            row.row_id == interior.row_id
                                && row.created_tx == interior.created_tx
                                && row.lsn == interior.lsn
                        })
                        .unwrap();
                    drop(store);
                    drop(_fixture_decode_scope_87263);
                    let table_name = RedbPersistence::rel_table_name("version_rows");
                    let definition = redb::TableDefinition::<&[u8], &[u8]>::new(&table_name);
                    let raw = redb::Database::create(&path).unwrap();
                    let saved = {
                        let read = raw.begin_read().unwrap();
                        let table = read.open_table(definition).unwrap();
                        table
                            .iter()
                            .unwrap()
                            .map(|entry| {
                                let (key, value) = entry.unwrap();
                                (key.value().to_vec(), value.value().to_vec())
                            })
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(
                        saved.len(),
                        versions.len(),
                        "the corruption fixture maps every saved key to the production-decoded sequence"
                    );
                    drop(raw);
                    if damage == "injected_extra_version" {
                        // Lawful public replacement retires this exact member first;
                        // injecting its original bytes leaves an extra physical member.
                        let fixture_open_accountant_87298 = std::sync::Arc::new(
                            crate::memory_accounting::MemoryAccountant::no_limit(),
                        );
                        let _fixture_decode_scope_87298 =
                            crate::read_image_memory::ImageMemory::enter_writer_operation(
                                std::sync::Arc::clone(&fixture_open_accountant_87298).into(),
                            )
                            .unwrap();
                        let store = RedbPersistence::open(&path).unwrap();
                        store.attach_memory_accountant(
                            std::sync::Arc::clone(&fixture_open_accountant_87298).into(),
                        );
                        let retained = versions
                            .iter()
                            .enumerate()
                            .filter(|(position, _)| *position != interior_position)
                            .map(|(_, row)| row.clone())
                            .collect::<Vec<_>>();
                        store.rewrite_table_rows("version_rows", &retained).unwrap();
                        drop(store);
                        drop(_fixture_decode_scope_87298);
                    }
                    let raw = redb::Database::create(&path).unwrap();
                    let write = raw.begin_write().unwrap();
                    {
                        let mut table = write.open_table(definition).unwrap();
                        match damage {
                            "missing_interior_version" => {
                                table.remove(saved[interior_position].0.as_slice()).unwrap();
                            }
                            "entirely_missing_deleted_group" | "false_terminal_absence" => {
                                for (key, _) in &saved {
                                    table.remove(key.as_slice()).unwrap();
                                }
                            }
                            "injected_extra_version" => {
                                let (key, value) = &saved[interior_position];
                                table.insert(key.as_slice(), value.as_slice()).unwrap();
                            }
                            "reordered_version_bodies" => {
                                // Redb orders keys. Exchanging two intact encoded bodies
                                // makes their version identities disagree with that order.
                                table
                                    .insert(
                                        saved[0].0.as_slice(),
                                        saved[interior_position].1.as_slice(),
                                    )
                                    .unwrap();
                                table
                                    .insert(
                                        saved[interior_position].0.as_slice(),
                                        saved[0].1.as_slice(),
                                    )
                                    .unwrap();
                            }
                            _ => unreachable!(),
                        }
                    }
                    write.commit().unwrap();
                    drop(raw);
                    let reopened = Database::open(&path).unwrap();
                    for (snapshot, expected) in [historical, current].into_iter().zip(&controls) {
                        for (sql, _owed_rows) in queries.into_iter().zip(expected) {
                            match reopened.execute_at_snapshot(sql, &HashMap::new(), snapshot) {
                                Err(Error::StoreCorrupted {
                                    path: actual,
                                    reason,
                                }) => {
                                    // rooted preflight reports the logical table for every damage kind.
                                    assert_eq!(
                                        actual, "version_rows",
                                        "{damage}: public SQL names the corrupted logical table or version"
                                    );
                                    assert!(
                                        !(reason == ""),
                                        "{damage}: public SQL corruption has an explanation"
                                    );
                                }
                                other => panic!(
                                    "{damage}: public SQL refuses before affected answer or successful absence: {sql}: {other:?}"
                                ),
                            }
                        }
                    }
                    assert_eq!(
                        reopened
                            .scan("control_rows", current)
                            .unwrap()
                            .into_iter()
                            .map(|row| (row.row_id, row.values))
                            .collect::<Vec<_>>(),
                        control_before,
                        "{damage}: public unaffected control retains every original identity and value"
                    );
                    drop(reopened);
                }));
                if outcome.is_err() {
                    failed_public_rows.push((shape, damage));
                }
            }
        }
        assert_eq!(
            failed_public_rows,
            Vec::<(&str, &str)>::new(),
            "every named public historical SQL row executes"
        );
    }
}
