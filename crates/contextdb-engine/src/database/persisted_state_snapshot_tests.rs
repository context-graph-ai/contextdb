use super::*;
use redb::ReadableDatabase;
include!("durable_test_observations.rs");

fn create_and_insert(db: &Database) -> RowId {
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, body TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "INSERT INTO events (id, body) VALUES (1, 'retired')",
        &params,
    )
    .unwrap();
    let fixture_operation_reader_6 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    db.row_id_for_natural_key_full(
        "events",
        &NaturalKey::single("id".to_string(), Value::Int64(1)),
        db.snapshot_for_read(),
        fixture_operation_reader_6.clone(),
    )
    .unwrap()
    .unwrap()
}

#[test]
fn fallback_snapshots_do_not_emit_an_old_raw_version_after_its_latest_position_is_tombstoned() {
    let db = Database::open_memory();
    let old_row_id = create_and_insert(&db);
    let old = db
        .relational_store
        .live_row_by_id("events", old_row_id)
        .unwrap();
    let meta = db.relational_store.table_meta.read()["events"].clone();
    let mut tombstone = old.clone();
    tombstone.lsn = Lsn(old.lsn.0 + 1);
    tombstone.deleted_tx = Some(TxId(old.created_tx.0 + 1));
    let projection = RelationalStore::table_projection("events", meta, vec![old, tombstone]);
    let publication = db
        .relational_store
        .prepare_received_schema_publication(vec![projection], std::iter::empty::<String>());
    db.relational_store
        .publish_prepared_received_schema(publication);

    assert!(
        db.relational_store.tables.read()["events"]
            .iter()
            .any(|row| row.row_id == old_row_id && row.deleted_tx.is_none())
    );
    assert!(
        db.relational_store
            .live_row_by_id("events", old_row_id)
            .is_none()
    );
    let fixture_operation_reader_41 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_41 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_41.clone());

    assert!(
        db.full_state_snapshot(
            &mut fixture_operation_copy_41,
            fixture_operation_reader_41.clone()
        )
        .expect("materialize full state")
        .rows
        .is_empty()
    );
    let fixture_operation_reader_42 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_42 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_42.clone());

    assert!(
        db.persisted_state_since_with_ddl_provenance(
            Lsn(1),
            &mut fixture_operation_copy_42,
            fixture_operation_reader_42.clone()
        )
        .expect("materialize persisted state")
        .0
        .rows
        .is_empty()
    );
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn fallback_snapshots_keep_a_fresh_same_key_row_after_the_old_row_id_is_tombstoned() {
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
                    let damaged_table_meta = db.table_meta("version_rows").unwrap();
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
                    let fixture_open_accountant_7598 =
                        std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
                    let _fixture_decode_scope_7598 =
                        crate::read_image_memory::ImageMemory::enter_writer_operation(
                            std::sync::Arc::clone(&fixture_open_accountant_7598).into(),
                        )
                        .unwrap();
                    let store = RedbPersistence::open(&path).unwrap();
                    store.attach_memory_accountant(
                        std::sync::Arc::clone(&fixture_open_accountant_7598).into(),
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
                    drop(_fixture_decode_scope_7598);
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
                        let fixture_open_accountant_7620 = std::sync::Arc::new(
                            crate::memory_accounting::MemoryAccountant::no_limit(),
                        );
                        let _fixture_decode_scope_7620 =
                            crate::read_image_memory::ImageMemory::enter_writer_operation(
                                std::sync::Arc::clone(&fixture_open_accountant_7620).into(),
                            )
                            .unwrap();
                        let store = RedbPersistence::open(&path).unwrap();
                        store.attach_memory_accountant(
                            std::sync::Arc::clone(&fixture_open_accountant_7620).into(),
                        );
                        let retained = versions
                            .iter()
                            .enumerate()
                            .filter(|(position, _)| *position != interior_position)
                            .map(|(_, row)| row.clone())
                            .collect::<Vec<_>>();
                        store.rewrite_table_rows("version_rows", &retained).unwrap();
                        drop(store);
                        drop(_fixture_decode_scope_7620);
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
                        for (sql, owed_rows) in queries.into_iter().zip(expected) {
                            // An empty current index range does not consume the retired
                            // historical group. Corruption is owed before data in each
                            // range whose lawful same-snapshot control is nonempty.
                            if owed_rows.0.is_empty() {
                                continue;
                            }
                            match reopened.execute_at_snapshot(sql, &HashMap::new(), snapshot) {
                                Err(Error::StoreCorrupted { path: _, reason }) => {
                                    // Error provenance labels are internal; the public
                                    // table binding identifies the damaged logical table.
                                    assert_eq!(
                                        reopened.table_meta("version_rows").unwrap(),
                                        damaged_table_meta,
                                        "{damage}: the fixture selects the damaged logical table"
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
    let mut failed_version_rows = Vec::new();
    // A historical group refuses missing, extra or reordered durable versions
    // before exposing a value, identity, observation or false absence.
    for damage in [
        "missing_interior_version",
        "entirely_missing_deleted_group",
        "injected_extra_version",
        "reordered_version_bodies",
        "false_terminal_absence",
    ] {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            use redb::ReadableTable;
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("version-corruption.db");
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
            let meta = db.table_meta("version_rows").unwrap();
            let control_meta = db.table_meta("control_rows").unwrap();
            let control_before = db
                .scan("control_rows", current)
                .unwrap()
                .into_iter()
                .map(|row| (row.row_id, row.values))
                .collect::<Vec<_>>();
            let read_memory = db.declared_reader_memory().unwrap();
            drop(db);

            // The production decoder identifies the interior member. The raw
            // mutation below copies production-encoded bytes without a codec.
            let fixture_open_accountant_7716 =
                std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_7716 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_7716).into(),
                )
                .unwrap();
            let store = RedbPersistence::open(&path).unwrap();
            store.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_7716).into(),
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
            drop(_fixture_decode_scope_7716);
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
                let fixture_open_accountant_7738 =
                    std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
                let _fixture_decode_scope_7738 =
                    crate::read_image_memory::ImageMemory::enter_writer_operation(
                        std::sync::Arc::clone(&fixture_open_accountant_7738).into(),
                    )
                    .unwrap();
                let store = RedbPersistence::open(&path).unwrap();
                store.attach_memory_accountant(
                    std::sync::Arc::clone(&fixture_open_accountant_7738).into(),
                );
                let retained = versions
                    .iter()
                    .enumerate()
                    .filter(|(position, _)| *position != interior_position)
                    .map(|(_, row)| row.clone())
                    .collect::<Vec<_>>();
                store.rewrite_table_rows("version_rows", &retained).unwrap();
                drop(store);
                drop(_fixture_decode_scope_7738);
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
                            .insert(saved[0].0.as_slice(), saved[interior_position].1.as_slice())
                            .unwrap();
                        table
                            .insert(saved[interior_position].0.as_slice(), saved[0].1.as_slice())
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            write.commit().unwrap();
            drop(raw);
            let (image, receipt, source) = crate::persistence::load_read_image(&path)
                .unwrap()
                .into_parts();
            receipt.validate_released().unwrap();

            let mut control_after = Vec::new();
            source
                .bounded_visit_relational_rows(
                    "control_rows",
                    &control_meta,
                    current,
                    |row| {
                        control_after.push((row.row_id, row.values.clone()));
                        Ok::<(), Error>(())
                    },
                    read_memory.clone(),
                )
                .unwrap();
            assert_eq!(
                control_after, control_before,
                "{damage}: an unaffected table keeps every original row identity and payload"
            );

            for snapshot in [historical, current] {
                let mut clones = Vec::new();
                let identity: Result<Option<VersionedRow>> = source
                    .bounded_relational_row_by_identity_admitted(
                        "version_rows",
                        &meta,
                        interior.row_id,
                        snapshot,
                        &mut || Ok::<(), Error>(()),
                        &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| {
                            match admission {
                                crate::read_image_memory::RowReadAdmission::Copy(bytes) => {
                                    clones.push(bytes)
                                }
                                crate::read_image_memory::RowReadAdmission::Move(memory) => {
                                    clones.push(memory.retained_payload_bytes())
                                }
                            };
                            Ok(())
                        },
                        read_memory.clone(),
                    );
                match identity {
                    Err(Error::StoreCorrupted {
                        path: actual,
                        reason,
                    }) => {
                        assert_eq!(
                            actual,
                            path.display().to_string().as_str(),
                            "{damage}: identity refusal identifies the affected store"
                        );
                        assert!(
                            !(reason == ""),
                            "{damage}: identity refusal explains corruption"
                        );
                    }
                    other => panic!(
                        "{damage}: identity must refuse before a value or false absence, got {other:?}"
                    ),
                }
                assert!(
                    clones.is_empty(),
                    "{damage}: identity exposes no affected allocation callback"
                );

                let mut observed = Vec::new();
                let mut clones = Vec::new();
                let mut cursor = contextdb_relational::mem::BoundedPhysicalCursor::default();
                let physical: Result<Option<VersionedRow>> = source
                    .bounded_relational_physical_next(
                        "version_rows",
                        &meta,
                        snapshot,
                        &mut cursor,
                        &mut || Ok::<(), Error>(()),
                        &mut |row| {
                            observed.push((row.row_id, row.created_tx, row.lsn));
                            Ok(())
                        },
                        &mut |bytes| {
                            clones.push(bytes);
                            Ok(())
                        },
                    );
                match physical {
                    Err(Error::StoreCorrupted {
                        path: actual,
                        reason,
                    }) => {
                        assert_eq!(
                            actual,
                            path.display().to_string().as_str(),
                            "{damage}: physical refusal identifies the affected store"
                        );
                        assert!(
                            !(reason == ""),
                            "{damage}: physical refusal explains corruption"
                        );
                    }
                    other => panic!(
                        "{damage}: physical next must refuse before a value or terminal absence, got {other:?}"
                    ),
                }
                assert!(
                    observed.is_empty(),
                    "{damage}: reconciliation precedes every affected version observation"
                );
                assert!(
                    clones.is_empty(),
                    "{damage}: physical next exposes no affected allocation callback"
                );

                let mut visited = Vec::new();
                let scan: Result<()> = source.bounded_visit_relational_rows(
                    "version_rows",
                    &meta,
                    snapshot,
                    |row| {
                        visited.push((row.row_id, row.values.clone()));
                        Ok::<(), Error>(())
                    },
                    read_memory.clone(),
                );
                match scan {
                    Err(Error::StoreCorrupted {
                        path: actual,
                        reason,
                    }) => {
                        assert_eq!(
                            actual,
                            path.display().to_string().as_str(),
                            "{damage}: scan refusal identifies the affected store"
                        );
                        assert!(
                            !(reason == ""),
                            "{damage}: scan refusal explains corruption"
                        );
                    }
                    other => panic!(
                        "{damage}: scan must refuse before affected values or successful completion, got {other:?}"
                    ),
                }
                assert!(
                    visited.is_empty(),
                    "{damage}: scan exposes no affected row callback"
                );
            }
            match source.relational_row_by_id_lsn(
                "version_rows",
                &meta,
                interior.row_id,
                interior.lsn,
            ) {
                Err(Error::StoreCorrupted {
                    path: actual,
                    reason,
                }) => {
                    assert_eq!(
                        actual,
                        path.display().to_string().as_str(),
                        "{damage}: exact-LSN refusal identifies the affected store"
                    );
                    assert!(
                        !(reason == ""),
                        "{damage}: exact-LSN refusal explains corruption"
                    );
                }
                other => panic!(
                    "{damage}: exact-LSN must refuse before a closed version or false absence, got {other:?}"
                ),
            }
            drop((source, image));
        }));
        if outcome.is_err() {
            failed_version_rows.push(damage);
        }
    }

    // Public no-log flush and table replacement preserve the same historical
    // answers as fresh reads from the ordinary DML producer after reopen.
    for producer in ["no_log_flush_data", "rewrite_table_rows"] {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let directory = tempfile::tempdir().unwrap();
            let control_path = directory.path().join("producer-control.db");
            let path = directory.path().join("producer-under-test.db");
            let db = Database::open(&control_path).unwrap();
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
            let historical = db.writer_read_root().unwrap().unwrap().snapshot;
            let historical_before = db
                .scan("version_rows", historical)
                .unwrap()
                .into_iter()
                .map(|row| (row.row_id, row.values))
                .collect::<Vec<_>>();
            drop(db);
            std::fs::copy(&control_path, &path).unwrap();
            let db = Database::open(&control_path).unwrap();
            db.execute(
                "UPDATE version_rows SET body = 'replacement' WHERE id = 1",
                &HashMap::new(),
            )
            .unwrap();
            let current = db.writer_read_root().unwrap().unwrap().snapshot;
            let current_rows = db.scan("version_rows", current).unwrap();
            let current_before = current_rows
                .iter()
                .map(|row| (row.row_id, row.values.clone()))
                .collect::<Vec<_>>();
            let meta = db.table_meta("version_rows").unwrap();
            let control_meta = db.table_meta("control_rows").unwrap();
            let control_before = db
                .scan("control_rows", current)
                .unwrap()
                .into_iter()
                .map(|row| (row.row_id, row.values))
                .collect::<Vec<_>>();
            let read_memory = db.declared_reader_memory().unwrap();
            drop(db);
            let fixture_open_accountant_7864 =
                std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_7864 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_7864).into(),
                )
                .unwrap();
            let control_store = RedbPersistence::open(&control_path).unwrap();
            control_store.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_7864).into(),
            );
            let all_versions = control_store.load_relational_table("version_rows").unwrap();
            drop(control_store);
            drop(_fixture_decode_scope_7864);
            let fixture_open_accountant_7867 =
                std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_7867 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_7867).into(),
                )
                .unwrap();
            let store = RedbPersistence::open(&path).unwrap();
            store.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_7867).into(),
            );
            match producer {
                "no_log_flush_data" => {
                    let successor = current_rows.first().unwrap();
                    let mut ws = contextdb_tx::WriteSet::new();
                    ws.relational_deletes.push((
                        "version_rows".into(),
                        successor.row_id,
                        successor.created_tx,
                    ));
                    ws.relational_inserts
                        .push(("version_rows".into(), successor.clone()));
                    ws.commit_lsn = Some(successor.lsn);
                    ws.finalized_tx = Some(successor.created_tx);
                    store.flush_data(&ws).unwrap();
                }
                "rewrite_table_rows" => store
                    .rewrite_table_rows("version_rows", &all_versions)
                    .unwrap(),
                _ => unreachable!(),
            }
            drop(store);
            drop(_fixture_decode_scope_7867);
            let (image, receipt, source) = crate::persistence::load_read_image(&path)
                .unwrap()
                .into_parts();
            receipt.validate_released().unwrap();
            for (snapshot, before) in [(historical, &historical_before), (current, &current_before)]
            {
                let mut cursor = contextdb_relational::mem::BoundedPhysicalCursor::default();
                let mut after = Vec::new();
                loop {
                    let next: Option<VersionedRow> = source
                        .bounded_relational_physical_next(
                            "version_rows",
                            &meta,
                            snapshot,
                            &mut cursor,
                            &mut || Ok::<(), Error>(()),
                            &mut |_| Ok(()),
                            &mut |_| Ok(()),
                        )
                        .unwrap();
                    let Some(row) = next else { break };
                    cursor.note_emitted();
                    after.push((row.row_id, row.values));
                }
                assert_eq!(
                    after, *before,
                    "{producer}: reopened historical/current traversal equals the fresh DML control in identity, payload and order"
                );
                assert!(
                    cursor.is_exhausted(),
                    "{producer}: equality includes the terminal pull"
                );
                let mut scanned = Vec::new();
                source
                    .bounded_visit_relational_rows(
                        "version_rows",
                        &meta,
                        snapshot,
                        |row| {
                            scanned.push((row.row_id, row.values.clone()));
                            Ok::<(), Error>(())
                        },
                        read_memory.clone(),
                    )
                    .unwrap();
                assert_eq!(
                    scanned, *before,
                    "{producer}: full scan equals the fresh DML control"
                );
                for (row_id, values) in before {
                    let row: Option<VersionedRow> = source
                        .bounded_relational_row_by_identity_admitted(
                            "version_rows",
                            &meta,
                            *row_id,
                            snapshot,
                            &mut || Ok::<(), Error>(()),
                            &mut |_| Ok(()),
                            read_memory.clone(),
                        )
                        .unwrap();
                    assert_eq!(
                        row.map(|row| (row.row_id, row.values)),
                        Some((*row_id, values.clone())),
                        "{producer}: historical/current identity lookup equals the fresh DML control"
                    );
                }
            }
            for version in &all_versions {
                let exact = source
                    .relational_row_by_id_lsn("version_rows", &meta, version.row_id, version.lsn)
                    .unwrap();
                assert_eq!(
                    exact.map(|row| (
                        row.row_id,
                        row.created_tx,
                        row.deleted_tx,
                        row.lsn,
                        row.created_at,
                        row.values
                    )),
                    Some((
                        version.row_id,
                        version.created_tx,
                        version.deleted_tx,
                        version.lsn,
                        version.created_at,
                        version.values.clone()
                    )),
                    "{producer}: exact-LSN returns the complete production-authored closed/live version"
                );
            }
            let mut control_after = Vec::new();
            source
                .bounded_visit_relational_rows(
                    "control_rows",
                    &control_meta,
                    current,
                    |row| {
                        control_after.push((row.row_id, row.values.clone()));
                        Ok::<(), Error>(())
                    },
                    read_memory.clone(),
                )
                .unwrap();
            assert_eq!(
                control_after, control_before,
                "{producer}: another table keeps its complete original answer"
            );
            drop((source, image));
        }));
        if outcome.is_err() {
            failed_version_rows.push(producer);
        }
    }
    assert_eq!(
        failed_version_rows,
        Vec::<&str>::new(),
        "every named version-corruption and durable-producer row must satisfy its unchanged oracle"
    );
    // A released durable historical snapshot keeps its complete relational answer.
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("physical-policy.db");
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE joined_rows (id INT PRIMARY KEY, payload JSON)",
            &HashMap::new(),
        )
        .unwrap();
        for (id, len) in [0, 1, 4, 5, 8, 9].into_iter().enumerate() {
            db.execute(
                "INSERT INTO joined_rows (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    (
                        "payload".into(),
                        Value::Json(serde_json::Value::Array(
                            (0..len).map(|_| serde_json::Value::Null).collect(),
                        )),
                    ),
                ]),
            )
            .unwrap();
        }
        let historical = db.writer_read_root().unwrap().unwrap().snapshot;
        // Preserve the complete historical answer before later mutations.
        // Row identity and values are the snapshot answer; later publication
        // may change retained lifecycle metadata without changing that answer.
        let historical_before_close = db
            .scan("joined_rows", historical)
            .unwrap()
            .into_iter()
            .map(|row| (row.row_id, row.values))
            .collect::<Vec<_>>();
        db.execute(
            "UPDATE joined_rows SET payload = $payload WHERE id = 1",
            &HashMap::from([(
                "payload".into(),
                Value::Json(
                    serde_json::json!({"日本語": ["é😀", [], [null, null, null, null, null]]}),
                ),
            )]),
        )
        .unwrap();
        db.execute("DELETE FROM joined_rows WHERE id = 5", &HashMap::new())
            .unwrap();
        let current = db.writer_read_root().unwrap().unwrap().snapshot;
        let meta = db.table_meta("joined_rows").unwrap();
        let caller = db.declared_reader_memory().unwrap();
        let current_before_close = db
            .scan("joined_rows", current)
            .unwrap()
            .into_iter()
            .map(|row| (row.row_id, row.values))
            .collect::<Vec<_>>();
        drop(caller);
        drop(db);
        let (image, receipt, source) = crate::persistence::load_read_image(&path)
            .unwrap()
            .into_parts();
        receipt.validate_released().unwrap();
        let identity = source.storage_identity();
        for (snapshot, before_close) in [
            (historical, &historical_before_close),
            (current, &current_before_close),
        ] {
            let mut legacy_cursor = contextdb_relational::mem::BoundedPhysicalCursor::default();
            let mut legacy_observed = Vec::new();
            let mut legacy_retention = Vec::new();
            let mut legacy_rows = Vec::new();
            let mut legacy_touches = Vec::new();
            loop {
                let row: Option<VersionedRow> = source
                    .bounded_relational_physical_next(
                        "joined_rows",
                        &meta,
                        snapshot,
                        &mut legacy_cursor,
                        &mut || {
                            legacy_touches.push(());
                            Ok::<(), Error>(())
                        },
                        &mut |row| {
                            legacy_observed.push((
                                (
                                    row.row_id,
                                    row.created_tx,
                                    row.deleted_tx,
                                    row.lsn,
                                    row.created_at,
                                ),
                                row.estimated_bytes(),
                            ));
                            Ok(())
                        },
                        &mut |amount| {
                            legacy_retention.push(amount);
                            Ok(())
                        },
                    )
                    .unwrap();
                let Some(row) = row else { break };
                legacy_cursor.note_emitted();
                legacy_rows.push(row);
            }
            assert_eq!(
                legacy_rows.len(),
                before_close.len(),
                "closing every writer handle must preserve the complete captured snapshot"
            );
            let after_reopen = legacy_rows
                .into_iter()
                .map(|row| (row.row_id, row.values))
                .collect::<Vec<_>>();
            assert_eq!(
                after_reopen, *before_close,
                "reopening must preserve snapshot row identity, payload and physical ordering"
            );
            assert!(
                legacy_cursor.is_exhausted(),
                "the relational comparison covers the terminal physical pull"
            );
            let observed_identity = source.storage_identity();
            assert!(observed_identity.transaction_id >= identity.transaction_id);
            assert_eq!(
                observed_identity.user_root, identity.user_root,
                "all physical pulls stay on the reopened immutable read source"
            );
        }
        drop((source, image));
    }

    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("snapshot-fresh-row.redb");
    let db = Database::open(&path).expect("open snapshot fresh-row database");
    let old_row_id = create_and_insert(&db);
    db.execute("DELETE FROM events WHERE id = 1", &HashMap::new())
        .unwrap();
    db.execute(
        "INSERT INTO events (id, body) VALUES (1, 'fresh')",
        &HashMap::new(),
    )
    .unwrap();
    let fixture_operation_reader_7 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let fresh_row_id = db
        .row_id_for_natural_key_full(
            "events",
            &NaturalKey::single("id".to_string(), Value::Int64(1)),
            db.snapshot_for_read(),
            fixture_operation_reader_7.clone(),
        )
        .unwrap()
        .unwrap();
    drop(db);
    let db = Database::open(&path).expect("reopen snapshot fresh-row database");

    assert_ne!(old_row_id, fresh_row_id);
    assert!(
        durable_observation::rows(&db, "events")
            .iter()
            .all(|row| row.row_id != old_row_id || !row.visible_at(db.snapshot_for_read()))
    );
    assert!(
        durable_observation::rows(&db, "events")
            .iter()
            .any(|row| row.row_id == fresh_row_id && row.visible_at(db.snapshot_for_read()))
    );
    let fixture_operation_reader_43 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_43 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_43.clone());

    let fixture_operation_reader_44 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_44 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_44.clone());

    for rows in [
        db.full_state_snapshot(
            &mut fixture_operation_copy_43,
            fixture_operation_reader_43.clone(),
        )
        .expect("materialize full state")
        .rows,
        db.persisted_state_since_with_ddl_provenance(
            Lsn(1),
            &mut fixture_operation_copy_44,
            fixture_operation_reader_44.clone(),
        )
        .expect("materialize persisted state")
        .0
        .rows,
    ] {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values.get("body"),
            Some(&Value::Text("fresh".to_string()))
        );
    }
}

#[test]
fn persisted_delta_replays_synthetic_schema_after_its_requested_frontier() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("snapshot-schema-pagination.redb");
    let db = Database::open(&path).expect("open schema pagination database");
    create_and_insert(&db);
    db.execute(
        "CREATE TABLE later_events (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX later_events_body ON later_events (body)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO later_events (id, body) VALUES (2, 'paged')",
        &HashMap::new(),
    )
    .unwrap();
    drop(db);

    let reopened = Database::open(&path).expect("reopen schema pagination database");
    let fixture_operation_reader_45 = reopened
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_45 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_45.clone());

    let (full, full_source) = reopened
        .persisted_state_since_with_ddl_provenance(
            Lsn(0),
            &mut fixture_operation_copy_45,
            fixture_operation_reader_45.clone(),
        )
        .expect("materialize complete persisted state");
    assert_eq!(full_source.kind, DdlProvenanceKind::Historical);
    assert_eq!(full.ddl.len(), full.ddl_lsn.len());
    let events_lsn = full
        .ddl
        .iter()
        .zip(full.ddl_lsn.iter())
        .find_map(|(ddl, lsn)| {
            matches!(ddl, DdlChange::CreateTable { name, .. } if name == "events").then_some(*lsn)
        })
        .expect("complete history records the first table in author order");
    let later_lsn = full
        .ddl
        .iter()
        .zip(full.ddl_lsn.iter())
        .find_map(|(ddl, lsn)| {
            matches!(ddl, DdlChange::CreateTable { name, .. } if name == "later_events")
                .then_some(*lsn)
        })
        .expect("complete history records the second table in author order");
    assert!(later_lsn > events_lsn);
    let fixture_operation_reader_46 = reopened
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_46 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_46.clone());

    let (changes, source) = reopened
        .persisted_state_since_with_ddl_provenance(
            events_lsn,
            &mut fixture_operation_copy_46,
            fixture_operation_reader_46.clone(),
        )
        .expect("materialize paged persisted state");

    assert_eq!(source.kind, DdlProvenanceKind::Historical);
    assert_eq!(changes.ddl.len(), changes.ddl_lsn.len());
    assert!(
        changes.ddl.iter().any(
            |ddl| matches!(ddl, DdlChange::CreateTable { name, .. } if name == "later_events")
        ),
        "a paged delta after the requested frontier replays the tables it touches"
    );
    assert!(
        changes.ddl.iter().any(
            |ddl| matches!(ddl, DdlChange::CreateIndex { table, .. } if table == "later_events")
        ),
        "dependent declarations of the affected table travel with it"
    );
    assert!(
        changes
            .ddl
            .iter()
            .all(|ddl| !matches!(ddl, DdlChange::CreateTable { name, .. } if name == "events")),
        "a paged delta must not replay an unaffected table that landed at or before the frontier"
    );
    assert!(
        changes.ddl_lsn.iter().all(|lsn| *lsn > events_lsn),
        "each replayed declaration keeps the LSN it was authored at"
    );
    let later_position = changes
        .ddl
        .iter()
        .position(
            |ddl| matches!(ddl, DdlChange::CreateTable { name, .. } if name == "later_events"),
        )
        .expect("affected create is present");
    let index_position = changes
        .ddl
        .iter()
        .position(
            |ddl| matches!(ddl, DdlChange::CreateIndex { table, .. } if table == "later_events"),
        )
        .expect("affected index is present");
    assert!(
        later_position < index_position,
        "paged schema replay preserves author order"
    );
    assert_eq!(changes.ddl_lsn[later_position], later_lsn);
    assert_ne!(
        changes.ddl_lsn[later_position], changes.ddl_lsn[index_position],
        "each historical declaration carries its own LSN"
    );
}

#[test]
fn fallback_snapshots_omit_a_position_live_row_that_is_not_visible_by_its_natural_key() {
    let db = Database::open_memory();
    let row_id = create_and_insert(&db);
    let mut candidate = db
        .relational_store
        .live_row_by_id("events", row_id)
        .unwrap();
    let meta = db.relational_store.table_meta.read()["events"].clone();
    let snapshot = db.snapshot_for_read();
    candidate.created_tx = TxId(snapshot.0 + 1);
    candidate.lsn = Lsn(candidate.lsn.0 + 1);
    let projection = RelationalStore::table_projection("events", meta, vec![candidate]);
    let publication = db
        .relational_store
        .prepare_received_schema_publication(vec![projection], std::iter::empty::<String>());
    db.relational_store
        .publish_prepared_received_schema(publication);

    let natural_key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let empty_skip_deleted = HashSet::new();
    assert!(
        db.relational_store
            .live_row_by_id("events", row_id)
            .is_some()
    );
    let fixture_operation_reader_8 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.visible_row_by_natural_key(
            "events",
            &natural_key,
            snapshot,
            &empty_skip_deleted,
            fixture_operation_reader_8.clone()
        )
        .unwrap()
        .is_none()
    );
    let fixture_operation_reader_47 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_47 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_47.clone());

    assert!(
        db.full_state_snapshot(
            &mut fixture_operation_copy_47,
            fixture_operation_reader_47.clone()
        )
        .expect("materialize full state")
        .rows
        .is_empty()
    );
    let fixture_operation_reader_48 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_48 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_48.clone());

    assert!(
        db.persisted_state_since_with_ddl_provenance(
            Lsn(1),
            &mut fixture_operation_copy_48,
            fixture_operation_reader_48.clone()
        )
        .expect("materialize persisted state")
        .0
        .rows
        .is_empty()
    );
}
