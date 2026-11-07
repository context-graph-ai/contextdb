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

#[derive(Clone, Copy)]
enum UniqueRootDoor {
    BatchCommit,
    OneShotLookup,
}

/// The same raw-redb technique as the historical-root rows: alter the physical
/// store without the producer hook that would publish a matching root witness.
/// Only the historical primary-key index is removed; current postings and rows
/// stay intact, so consuming an absent key cannot discover this by row reciprocity.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn damaged_unique_root_case(door: UniqueRootDoor) {
    use redb::TableHandle;

    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unique-root.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let none = HashMap::new();
    db.execute(
        "CREATE TABLE unique_root_rows (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC OFF",
        &none,
    )
    .unwrap();
    db.execute(
        "INSERT INTO unique_root_rows VALUES (1, 'committed')",
        &none,
    )
    .unwrap();
    let rows_before = db
        .execute("SELECT id, body FROM unique_root_rows", &none)
        .unwrap()
        .rows()
        .to_vec();
    let lsn_before = db.current_lsn();
    let old_root = db.writer_read_root().unwrap().unwrap();
    let old_identity = old_root.storage_identity();
    let columns = ["id".to_string()];
    let missing = [Value::Int64(2)];
    let mut stats = CommitStageStats::default();
    assert_eq!(
        db.committed_unique_conflict_row_id(
            "unique_root_rows",
            &columns,
            &missing,
            old_root.snapshot,
            &HashSet::new(),
            Some(&mut stats),
            db.declared_reader_memory().unwrap(),
        )
        .unwrap(),
        None
    );
    assert_eq!(
        stats.indexed_probes, 1,
        "the lawful control consumes the unique index"
    );
    drop(old_root);

    // Keep genuine SQL-produced pending rows, so the damaged-root row reaches
    // commit-time revalidation rather than failing the earlier statement probe.
    let staged_tx = db.begin().unwrap();
    for id in [2, 3] {
        db.execute_in_tx(
            staged_tx,
            "INSERT INTO unique_root_rows VALUES ($id, 'pending')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let staged = db
        .tx_mgr
        .with_write_set(staged_tx, |ws| ws.clone())
        .unwrap();
    db.rollback(staged_tx).unwrap();
    db.close().unwrap();
    drop(db);

    let raw = redb::Database::open(&path).unwrap();
    let historical_indexes = raw
        .begin_read()
        .unwrap()
        .list_tables()
        .unwrap()
        .filter_map(|table| {
            table
                .name()
                .strip_prefix("__rel_index_")
                .is_some_and(|hash| {
                    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                .then_some(table)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        historical_indexes.len(),
        1,
        "only the declared primary-key index exists"
    );
    let index = historical_indexes.into_iter().next().unwrap();
    let index_name = index.name().to_string();
    let write = raw.begin_write().unwrap();
    assert!(write.delete_table(index).unwrap());
    write.commit().unwrap();
    drop(raw);

    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let damaged_root = db.writer_read_root().unwrap().unwrap();
    assert_ne!(
        damaged_root.storage_identity(),
        old_identity,
        "damage advances the captured root"
    );
    let snapshot = damaged_root.snapshot;
    drop(damaged_root);
    let outcome = match door {
        UniqueRootDoor::OneShotLookup => {
            let memory = db.declared_reader_memory().unwrap();
            let image =
                crate::read_image_memory::ImageMemory::new_direct_reader(None, memory.clone())
                    .unwrap();
            image.scope(|| {
                db.committed_unique_conflict_row_id(
                    "unique_root_rows",
                    &columns,
                    &missing,
                    snapshot,
                    &HashSet::new(),
                    None,
                    memory,
                )
                .map(|_| ())
            })
        }
        UniqueRootDoor::BatchCommit => {
            let tx = db.begin().unwrap();
            assert_eq!(
                tx, staged_tx,
                "reopen preserves the pending SQL rows' transaction identity"
            );
            let bytes = staged
                .relational_insert_admitted_bytes
                .values()
                .flat_map(|rows| rows.values())
                .copied()
                .sum();
            // Re-admit the original recorded row invoices before installing the
            // test-owned SQL write set; commit/rollback keeps the normal refund.
            db.finalization.accountant.try_allocate(bytes).unwrap();
            db.tx_mgr.with_write_set(tx, |ws| *ws = staged).unwrap();
            db.commit(tx)
        }
    };
    assert!(
        matches!(&outcome, Err(Error::StoreCorrupted { .. })),
        "a uniqueness consumer must refuse the damaged index root, got {outcome:?}"
    );
    assert_eq!(db.current_lsn(), lsn_before, "refusal publishes no commit");
    assert_eq!(
        db.execute("SELECT id, body FROM unique_root_rows", &none)
            .unwrap()
            .rows(),
        rows_before.as_slice(),
        "refusal preserves every committed row"
    );
    db.close().unwrap();
    drop(db);
    let raw = redb::Database::open(&path).unwrap();
    assert!(
        !raw.begin_read()
            .unwrap()
            .list_tables()
            .unwrap()
            .any(|table| table.name() == index_name),
        "a refused consumer must not repair or replace the damaged index"
    );
}

#[test]
fn a_batch_unique_check_refuses_a_damaged_index_root() {
    damaged_unique_root_case(UniqueRootDoor::BatchCommit);
}

#[test]
fn a_one_shot_unique_lookup_refuses_a_damaged_index_root() {
    damaged_unique_root_case(UniqueRootDoor::OneShotLookup);
}
