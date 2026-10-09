use super::*;
use crate::Database;
use contextdb_core::Value;
use contextdb_relational::mem::BoundedOrderedRowCursor;

fn fixture(path: &Path) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE postings (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX by_bucket ON postings(bucket)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..5 {
        db.execute(
            "INSERT INTO postings VALUES ($id, 1)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

fn cursor(reverse: bool) -> BoundedOrderedRowCursor {
    let mut cursor = BoundedOrderedRowCursor::for_index("by_bucket".into(), reverse);
    cursor.select_identities_only();
    cursor.source_configure(1);
    cursor
}

fn pull(load: &ReadPersistenceLoad, cursor: &mut BoundedOrderedRowCursor) -> Result<Option<RowId>> {
    let meta = &load.image.table_meta["postings"];
    let declaration = RedbPersistence::relational_index_declarations(meta)
        .into_iter()
        .find(|d| d.name == "by_bucket")
        .unwrap();
    let snapshot = SnapshotId(
        load.image
            .newest_committed_tx_for_test()
            .map(|tx| tx.0)
            .unwrap(),
    );
    let persistence_fixture_parent_1 =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            load.source
                .verified_ordered_runs
                .memory
                .accountant
                .usage()
                .limit
                .map(|bytes| bytes as u64)
                .unwrap_or(u64::MAX),
        );
    let persistence_fixture_reader_1 = persistence_fixture_parent_1
        .child()
        .expect("admit the persistence fixture reader child");

    load.source
        .bounded_relational_ordered_next(
            "postings",
            meta,
            &declaration,
            snapshot,
            cursor,
            &mut || Ok(()),
            &mut || Ok(()),
            &mut |_| Ok(()),
            persistence_fixture_reader_1,
        )
        .map(|row| row.map(|row| row.row_id))
}

/// The same table and index as `fixture`, with keys wide enough that the
/// authenticated windows over this range are several times the run cache's
/// whole share of the declared session. That is what makes the eviction below
/// real capacity pressure: a build no longer reserves a fixed multiple of what
/// it needs, so the cache has to be genuinely full for one to displace another.
fn wide_pressure_fixture(path: &Path, rows: i64) {
    const KEY_BYTES: usize = 4 * 1024;
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE postings (id INTEGER PRIMARY KEY, bucket TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX by_bucket ON postings(bucket)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..rows {
        db.execute(
            "INSERT INTO postings VALUES ($id, $bucket)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "bucket".into(),
                    Value::Text(format!("{id:012}{}", "w".repeat(KEY_BYTES))),
                ),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

#[test]
fn capacity_eviction_resumes_both_cached_delivery_paths_in_both_directions() {
    const ROWS: i64 = 160;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pressure.db");
    wide_pressure_fixture(&path, ROWS);
    for reverse in [false, true] {
        for batch in [false, true] {
            let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
            let mut baseline = cursor(reverse);
            let mut expected = Vec::new();
            while let Some(id) = pull(&load, &mut baseline).unwrap() {
                expected.push(id);
            }
            let mut partial = cursor(reverse);
            let mut actual = Vec::new();
            if batch {
                // The batch path only delivers windows the cache already
                // holds; opening one is the pull path's work. Over a range
                // whose windows are several times the cache, the baseline walk
                // above has already displaced its own first window, so a
                // throwaway cursor opens it again before the batch drain.
                let mut prime = cursor(reverse);
                assert!(pull(&load, &mut prime).unwrap().is_some());
                let snapshot = SnapshotId(
                    load.image
                        .newest_committed_tx_for_test()
                        .map(|tx| tx.0)
                        .unwrap(),
                );
                let persistence_fixture_parent_2 =
                    crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                        load.source
                            .verified_ordered_runs
                            .memory
                            .accountant
                            .usage()
                            .limit
                            .map(|bytes| bytes as u64)
                            .unwrap_or(u64::MAX),
                    );
                let persistence_fixture_reader_2 = persistence_fixture_parent_2
                    .child()
                    .expect("admit the persistence fixture reader child");

                let error = load.source.bounded_visit_verified_ordered_identities(
                    "postings",
                    "by_bucket",
                    snapshot,
                    &mut partial,
                    &mut || Ok(()),
                    &mut |id| {
                        actual.push(id);
                        Err(Error::Other("suspend after delivery".into()))
                    },
                    persistence_fixture_reader_2,
                );
                assert!(error.is_err());
            } else {
                actual.push(pull(&load, &mut partial).unwrap().unwrap());
            }
            let cached_key = partial.source_cache_key().unwrap();
            assert!(load.source.verified_ordered_runs.get(&cached_key).is_some());
            // Ordinary range builds the other way down the same index consume
            // the cache's capacity until the suspended window is given back.
            // No test seam: the windows over this range are several times the
            // cache's whole share, so this is eviction through capacity alone.
            let mut competing = cursor(!reverse);
            let mut displaced = false;
            for _ in 0..64 {
                if load.source.verified_ordered_runs.get(&cached_key).is_none() {
                    displaced = true;
                    break;
                }
                if pull(&load, &mut competing).unwrap().is_none() {
                    competing = cursor(!reverse);
                }
            }
            assert!(
                displaced,
                "the suspended window was never displaced by capacity: \
                 direction={reverse}, batch={batch}"
            );
            while let Some(id) = pull(&load, &mut partial).unwrap() {
                actual.push(id);
            }
            assert_eq!(actual, expected, "direction={reverse}, batch={batch}");
        }
    }
}

#[test]
fn canonical_run_rejects_missing_extra_changed_and_hidden_physical_postings() {
    // Historical point and identity batches prove the complete addressed
    // version group before exposing values or absence. The private historical
    // variants have no public older-snapshot entry.
    {
        let mut failed_doors = Vec::new();
        for door in ["point", "identity", "authentication_batch"] {
            for damage in ["missing_interior_version", "entirely_missing_deleted_group", "injected_extra_version", "reordered_version_bodies", "false_terminal_absence"] {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            use redb::ReadableTable;
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("historical-door.db");
            let db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE version_rows (id INT PRIMARY KEY, body TEXT)", &HashMap::new()).unwrap();
            db.execute("CREATE TABLE control_rows (id INT PRIMARY KEY, body TEXT)", &HashMap::new()).unwrap();
            db.execute("INSERT INTO control_rows (id, body) VALUES (1, 'unaffected')", &HashMap::new()).unwrap();
            db.execute("INSERT INTO version_rows (id, body) VALUES (1, 'first')", &HashMap::new()).unwrap();
            db.execute("UPDATE version_rows SET body = 'interior' WHERE id = 1", &HashMap::new()).unwrap();
            let historical = db.writer_read_root().unwrap().unwrap().snapshot;
            let interior = db.scan("version_rows", historical).unwrap().into_iter().next().unwrap();
            db.execute("UPDATE version_rows SET body = 'head' WHERE id = 1", &HashMap::new()).unwrap();
            if damage == "entirely_missing_deleted_group" {
                db.execute("DELETE FROM version_rows WHERE id = 1", &HashMap::new()).unwrap();
            }
            let current = db.writer_read_root().unwrap().unwrap().snapshot;
            let meta = db.table_meta("version_rows").unwrap();
            let control_meta = db.table_meta("control_rows").unwrap();
            let control_row = db.scan("control_rows", current).unwrap().into_iter().next().unwrap();
            let control_before = db.scan("control_rows", current).unwrap().into_iter()
                .map(|row| (row.row_id, row.values)).collect::<Vec<_>>();
            let read_memory = db.declared_reader_memory().unwrap();
            let fixture_writer_accountant = db.bounded_read_accountant();
            let before = [historical, current].map(|snapshot| db.scan("version_rows", snapshot).unwrap().into_iter()
                .map(|row| (row.row_id, row.values)).collect::<Vec<_>>());
            drop(db);
            {
                let (image, receipt, source) = load_read_image(&path).unwrap().into_parts();
                receipt.validate_released().unwrap();
                let mut authentication = source.content_identity.batch(RETAINED_CONTENT_PROOF_BYTES);
                let (control, _control_memory) = source.with_read(|read| source.row_identity_by_id_in_read(
                    read, "control_rows", &control_meta, control_row.row_id, current, Some(&mut authentication))).unwrap().unwrap();
                assert_eq!((control.row_id, control.values), (control_row.row_id, control_row.values.clone()),
                    "{door}/{damage}: the shared real authentication batch first proves the unaffected identity and value");
                for (snapshot, expected) in [historical, current].into_iter().zip(&before) {
                    let actual = source.with_read(|read| match door {
                        "point" => source.row_by_id_in_read(read, "version_rows", &meta, interior.row_id, snapshot),
                        "identity" => source.row_identity_by_id_in_read(read, "version_rows", &meta, interior.row_id, snapshot, None),
                        "authentication_batch" => source.row_identity_by_id_in_read(read, "version_rows", &meta, interior.row_id, snapshot, Some(&mut authentication)),
                        _ => unreachable!(),
                    }).unwrap().into_iter().map(|(row, _memory)| (row.row_id, row.values)).collect::<Vec<_>>();
                    assert_eq!(actual, *expected, "{door}/{damage}: lawful complete historical/current answer equals fresh public scan at the same snapshot");
                }
                drop(authentication);
                drop((source, image));
            }

            // The production decoder identifies the interior member. The raw
            // mutation below copies production-encoded bytes without a codec.
            let fixture_open_accountant_261 = std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_261 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_261)).unwrap();
            let store = RedbPersistence::open(&path).unwrap();
            store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_261));
            let versions = store.load_relational_table("version_rows").unwrap();
            let interior_position = versions.iter().position(|row|
                row.row_id == interior.row_id && row.created_tx == interior.created_tx && row.lsn == interior.lsn
            ).unwrap();
            drop(store);
            drop(_fixture_decode_scope_261);
            let table_name = RedbPersistence::rel_table_name("version_rows");
            let definition = redb::TableDefinition::<&[u8], &[u8]>::new(&table_name);
            let raw = redb::Database::create(&path).unwrap();
            let saved = {
                let read = raw.begin_read().unwrap();
                let table = read.open_table(definition).unwrap();
                table.iter().unwrap().map(|entry| {
                    let (key, value) = entry.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                }).collect::<Vec<_>>()
            };
            assert_eq!(saved.len(), versions.len(), "the corruption fixture maps every saved key to the production-decoded sequence");
            drop(raw);
            if damage == "injected_extra_version" {
                // Lawful public replacement retires this exact member first;
                // injecting its original bytes leaves an extra physical member.
                let fixture_open_accountant_283 = Arc::clone(&fixture_writer_accountant);
                let _fixture_decode_scope_283 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_283)).unwrap();
                let store = RedbPersistence::open(&path).unwrap();
                store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_283));
                let retained = versions.iter().enumerate().filter(|(position, _)| *position != interior_position)
                    .map(|(_, row)| row.clone()).collect::<Vec<_>>();
                store.rewrite_table_rows("version_rows", &retained).unwrap();
                drop(store);
                drop(_fixture_decode_scope_283);
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
                        for (key, _) in &saved { table.remove(key.as_slice()).unwrap(); }
                    }
                    "injected_extra_version" => {
                        let (key, value) = &saved[interior_position];
                        table.insert(key.as_slice(), value.as_slice()).unwrap();
                    }
                    "reordered_version_bodies" => {
                        // Redb orders keys. Exchanging two intact encoded bodies
                        // makes their version identities disagree with that order.
                        table.insert(saved[0].0.as_slice(), saved[interior_position].1.as_slice()).unwrap();
                        table.insert(saved[interior_position].0.as_slice(), saved[0].1.as_slice()).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            write.commit().unwrap();
            drop(raw);
            let (image, receipt, source) = crate::persistence::load_read_image(&path).unwrap().into_parts();
            receipt.validate_released().unwrap();

            let mut authentication = source.content_identity.batch(RETAINED_CONTENT_PROOF_BYTES);
            let (control, _control_memory) = source.with_read(|read| source.row_identity_by_id_in_read(
                read, "control_rows", &control_meta, control_row.row_id, current, Some(&mut authentication))).unwrap().unwrap();
            assert_eq!((control.row_id, control.values), (control_row.row_id, control_row.values.clone()),
                "{door}/{damage}: a batch proves the unaffected identity before the affected group refuses");
            for snapshot in [historical, current] {
                let answer = source.with_read(|read| match door {
                    "point" => source.row_by_id_in_read(read, "version_rows", &meta, interior.row_id, snapshot),
                    "identity" => source.row_identity_by_id_in_read(read, "version_rows", &meta, interior.row_id, snapshot, None),
                    "authentication_batch" => source.row_identity_by_id_in_read(read, "version_rows", &meta, interior.row_id, snapshot, Some(&mut authentication)),
                    _ => unreachable!(),
                });
                match answer {
                    Err(Error::StoreCorrupted { path: actual, reason }) => {
                        assert_eq!(actual, path.display().to_string(), "{door}/{damage}: corruption identifies the affected store");
                        assert!(!reason.is_empty(), "{door}/{damage}: corruption has an explanation");
                    }
                    other => panic!("{door}/{damage}: historical/current point refuses before an affected value or false absence, got {other:?}"),
                }
            }
            let mut unaffected = Vec::new();
            source.bounded_visit_relational_rows("control_rows", &control_meta, current, |row| {
                unaffected.push((row.row_id, row.values.clone())); Ok::<(), Error>(())
            }, read_memory.clone()).unwrap();
            assert_eq!(unaffected, control_before, "{door}/{damage}: unaffected table retains the complete fresh control answer");
            drop(authentication);
            drop((source, image));
                }));
                if outcome.is_err() { failed_doors.push((door, damage)); }
            }
        }
        assert_eq!(failed_doors, Vec::<(&str, &str)>::new(), "every named historical private door executes its own lawful and corrupt oracle");
    }
    for damage in ["missing", "extra", "changed", "hidden"] {
        for reverse in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("physical.db");
            fixture(&path);
            let db = redb::Database::open(&path).unwrap();
            let write = db.begin_write().unwrap();
            let name = RedbPersistence::relational_index_table_name("postings", "by_bucket");
            {
                let mut table = write
                    .open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(
                        &name,
                    ))
                    .unwrap();
                let (key, value) = {
                    let (key, value) = table.first().unwrap().unwrap();
                    (key.value(), value.value().to_vec())
                };
                let position = key.decode().unwrap();
                match damage {
                    "missing" => {
                        table.remove(key).unwrap();
                    }
                    "extra" | "changed" => {
                        if damage == "changed" {
                            table.remove(key).unwrap();
                        }
                        let other = DurableRelationalIndexKey::new(
                            position.key,
                            RowId(u64::MAX - 1),
                            position.created_tx,
                        )
                        .unwrap();
                        table.insert(other, value.as_slice()).unwrap();
                    }
                    "hidden" => {
                        let tombstone =
                            RedbPersistence::encode(&Some(position.created_tx)).unwrap();
                        table.insert(key, tombstone.as_slice()).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            write.commit().unwrap();
            drop(db);
            let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
            assert!(
                matches!(
                    pull(&load, &mut cursor(reverse)),
                    Err(Error::StoreCorrupted { .. })
                ),
                "{damage}, reverse={reverse}"
            );
        }
    }
}

#[test]
fn canonical_range_reconciles_retired_postings_without_decoding_row_bodies() {
    use crate::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retired.db");
    fixture(&path);
    let db = Database::open(&path).unwrap();
    db.execute(
        "UPDATE postings SET bucket = 2 WHERE id = 2",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("DELETE FROM postings WHERE id = 3", &HashMap::new())
        .unwrap();
    db.close().unwrap();

    // Retired postings really remain in the physical index. The canonical
    // current range must exclude them without loading their old row bodies.
    let raw = redb::Database::open(&path).unwrap();
    let read = raw.begin_read().unwrap();
    let name = RedbPersistence::relational_index_table_name("postings", "by_bucket");
    let index = read
        .open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(
            &name,
        ))
        .unwrap();
    assert_eq!(index.len().unwrap(), 6);
    let retired = index
        .iter()
        .unwrap()
        .filter(|entry| {
            let (_, value) = entry.as_ref().unwrap();
            RedbPersistence::decode::<Option<TxId>>(value.value())
                .unwrap()
                .is_some()
        })
        .count();
    assert_eq!(retired, 2);
    drop(index);
    drop(read);
    drop(raw);

    let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let mut forward = cursor(false);
    let mut expected = Vec::new();
    while let Some(id) = pull(&load, &mut forward).unwrap() {
        expected.push(id);
    }
    assert_eq!(expected.len(), 4);
    assert_eq!(
        expected
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4
    );
    let mut backward = cursor(true);
    let mut actual = Vec::new();
    while let Some(id) = pull(&load, &mut backward).unwrap() {
        actual.push(id);
    }
    expected.reverse();
    assert_eq!(actual, expected);
    assert_eq!(scope.counters().decoded_rows, 0);
}

/// Memory-peak control for the bounded working window.
///
/// A reader walking an authenticated range holds one window of proven postings,
/// and what that window costs is a property of the window, not of how long the
/// range is. Measured against the database accountant the reader actually
/// charges: the bytes pinned while the cursor is mid-range are the same for a
/// range four times the size, and both ranges are delivered whole.
#[test]
fn the_pinned_working_set_does_not_grow_with_the_range() {
    const KEY_BYTES: usize = 4 * 1024;
    const DECLARED_MEMORY: usize = 8 * 1024 * 1024;

    fn wide_fixture(path: &Path, rows: i64) {
        let db = Database::open(path).unwrap();
        db.execute(
            "CREATE TABLE wide (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute("CREATE INDEX by_label ON wide(label)", &HashMap::new())
            .unwrap();
        let label = "w".repeat(KEY_BYTES);
        for id in 0..rows {
            db.execute(
                "INSERT INTO wide VALUES ($id, $label)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("label".into(), Value::Text(label.clone())),
                ]),
            )
            .unwrap();
        }
        db.close().unwrap();
    }

    fn pull_wide(
        load: &ReadPersistenceLoad,
        cursor: &mut BoundedOrderedRowCursor,
    ) -> Result<Option<RowId>> {
        let meta = &load.image.table_meta["wide"];
        let declaration = RedbPersistence::relational_index_declarations(meta)
            .into_iter()
            .find(|d| d.name == "by_label")
            .unwrap();
        let snapshot = SnapshotId(
            load.image
                .newest_committed_tx_for_test()
                .map(|tx| tx.0)
                .unwrap(),
        );
        let persistence_fixture_parent_3 =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                load.source
                    .verified_ordered_runs
                    .memory
                    .accountant
                    .usage()
                    .limit
                    .map(|bytes| bytes as u64)
                    .unwrap_or(u64::MAX),
            );
        let persistence_fixture_reader_3 = persistence_fixture_parent_3
            .child()
            .expect("admit the persistence fixture reader child");

        load.source
            .bounded_relational_ordered_next(
                "wide",
                meta,
                &declaration,
                snapshot,
                cursor,
                &mut || Ok(()),
                &mut || Ok(()),
                &mut |_| Ok(()),
                persistence_fixture_reader_3,
            )
            .map(|row| row.map(|row| row.row_id))
    }

    let mut measured = Vec::new();
    for rows in [128_i64, 512] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wide-window.db");
        wide_fixture(&path, rows);
        let load = load_read_image_with_memory(&path, Some(DECLARED_MEMORY)).unwrap();
        let accountant = &load.source.verified_ordered_runs.memory.accountant;
        let baseline = accountant.usage().used;

        let mut cursor = BoundedOrderedRowCursor::for_index("by_label".into(), false);
        cursor.select_identities_only();
        cursor.source_configure(1);
        let mut delivered = Vec::new();
        delivered.push(
            pull_wide(&load, &mut cursor)
                .expect("a result-limited read over a range larger than the budget is answered")
                .expect("the first identity"),
        );
        // What the reader is holding while it is part way through the range.
        let pinned = accountant.usage().used.saturating_sub(baseline);
        while let Some(id) = pull_wide(&load, &mut cursor).unwrap() {
            delivered.push(id);
        }
        assert_eq!(
            delivered.len(),
            rows as usize,
            "the whole range is delivered from bounded windows"
        );
        assert_eq!(
            delivered,
            (1..=rows as u64).map(RowId).collect::<Vec<_>>(),
            "in order, exactly once, across every window boundary"
        );
        let payload = rows as usize * KEY_BYTES;
        eprintln!("bounded window peak: rows={rows} whole_range_payload={payload} pinned={pinned}");
        measured.push((rows, payload, pinned));
    }

    let (small_rows, small_payload, small_pinned) = measured[0];
    let (large_rows, large_payload, large_pinned) = measured[1];
    assert!(
        large_payload > small_payload * 3,
        "the second range really is several times the first: {small_payload} -> {large_payload}"
    );
    assert!(
        large_pinned <= small_pinned,
        "the pinned working set is the window's, not the range's: \
         rows {small_rows}->{large_rows} pinned {small_pinned}->{large_pinned}"
    );
    assert!(
        large_pinned < large_payload,
        "a range larger than the window is never held whole: \
         pinned={large_pinned} whole_range_payload={large_payload}"
    );
}

/// A bounded authenticated window partitions valid work inside the caller's
/// EXISTING budget. Its implementation cap is a bound to grow into, never an
/// entry fee: a small result whose proof path and payload need tens of
/// kilobytes must be answered under a session that has far less than the
/// build's ceiling free, and the window must reserve what it needs rather than
/// a fixed minimum.
#[test]
fn a_small_window_reserves_what_it_needs_and_is_answered_with_little_left_free() {
    // Large enough that the build's own ceiling (the cache share, capped by
    // the build maximum) is the full 8 MiB implementation cap. This is the
    // shape the review names: a small result refused because that cap was
    // taken as a minimum reservation.
    const DECLARED_MEMORY: usize = 256 * 1024 * 1024;
    // What is deliberately left free for the whole authenticated read. It is a
    // fraction of the build ceiling and far more than this window needs.
    const LEFT_FREE: usize = 512 * 1024;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("small-window.db");
    fixture(&path);

    // 1. The reservation a cold window actually takes for a small range.
    {
        let load = load_read_image_with_memory(&path, Some(DECLARED_MEMORY)).unwrap();
        let accountant = &load.source.verified_ordered_runs.memory.accountant;
        let baseline = accountant.usage().used;
        let before = accountant.high_water_for_test();
        let mut walk = cursor(false);
        let mut delivered = Vec::new();
        while let Some(id) = pull(&load, &mut walk).unwrap() {
            delivered.push(id);
        }
        assert_eq!(delivered.len(), 5, "the whole small range is delivered");
        let peak = accountant
            .high_water_for_test()
            .max(before)
            .saturating_sub(baseline);
        eprintln!("small window reservation: peak_above_baseline={peak}");
        assert!(
            peak < 1024 * 1024,
            "a five-posting window reserved {peak} bytes: the build's ceiling is a \
             bound to grow into, not a minimum reservation"
        );
    }

    // 2. The same read, with almost all of the declared session budget already
    //    held by the session itself. Nothing about this result needs the
    //    build's ceiling, so nothing about it may be refused.
    {
        let load = load_read_image_with_memory(&path, Some(DECLARED_MEMORY)).unwrap();
        let memory = Arc::clone(&load.source.verified_ordered_runs.memory);
        let free = DECLARED_MEMORY.saturating_sub(memory.accountant.usage().used);
        memory
            .reserve(free.saturating_sub(LEFT_FREE))
            .expect("the session may hold what it has already admitted");
        assert!(
            memory.accountant.usage().available.unwrap() <= LEFT_FREE,
            "the session really is down to its last {LEFT_FREE} bytes"
        );

        let mut walk = cursor(false);
        let mut delivered = Vec::new();
        loop {
            match pull(&load, &mut walk) {
                Ok(Some(id)) => delivered.push(id),
                Ok(None) => break,
                Err(error) => panic!(
                    "a small authenticated result was refused because the build \
                     reserved a fixed minimum rather than what it needs: {error}"
                ),
            }
        }
        assert_eq!(
            delivered,
            (1..=5u64).map(RowId).collect::<Vec<_>>(),
            "the whole range is delivered, in order, exactly once"
        );

        // And a range that needs more than one window is still delivered under
        // the same remaining budget: the windows partition the work, each
        // admitted as it is built.
        let mut reverse_walk = cursor(true);
        let mut reversed = Vec::new();
        while let Some(id) = pull(&load, &mut reverse_walk).unwrap() {
            reversed.push(id);
        }
        assert_eq!(
            reversed,
            (1..=5u64).rev().map(RowId).collect::<Vec<_>>(),
            "the reverse walk is answered under the same remaining budget"
        );
    }

    // 3. Available memory between this window's actual demand and the 64 KiB
    //    partition size: a result that fits must be answered. 40 KiB is more
    //    than this five-posting window needs and less than the old 64 KiB
    //    admission floor, so a floor-first reservation refuses it.
    {
        const BETWEEN_DEMAND_AND_PARTITION: usize = 40 * 1024;
        let load = load_read_image_with_memory(&path, Some(DECLARED_MEMORY)).unwrap();
        let memory = Arc::clone(&load.source.verified_ordered_runs.memory);
        let free = DECLARED_MEMORY.saturating_sub(memory.accountant.usage().used);
        memory
            .reserve(free.saturating_sub(BETWEEN_DEMAND_AND_PARTITION))
            .expect("the session may hold what it has already admitted");
        assert!(
            memory.accountant.usage().available.unwrap() <= BETWEEN_DEMAND_AND_PARTITION,
            "the session really is down to its last {BETWEEN_DEMAND_AND_PARTITION} bytes"
        );

        let mut walk = cursor(false);
        let mut delivered = Vec::new();
        loop {
            match pull(&load, &mut walk) {
                Ok(Some(id)) => delivered.push(id),
                Ok(None) => break,
                Err(error) => panic!(
                    "a window whose actual demand fits in {BETWEEN_DEMAND_AND_PARTITION} bytes \
                     was refused because admission took a 64 KiB floor before reading: {error}"
                ),
            }
        }
        assert_eq!(
            delivered,
            (1..=5u64).map(RowId).collect::<Vec<_>>(),
            "available memory between actual demand and 64 KiB still answers the range"
        );
    }

    // 4. Available memory that covers the exact shortfall at a growth boundary
    //    but not another 64 KiB: after the first 64 KiB is spoken for, a later
    //    small increment must grow by what it needs, not round the shortfall
    //    up to a full partition.
    {
        const FITS_SHORTFALL_NOT_PARTITION: usize = 80 * 1024;
        let load = load_read_image_with_memory(&path, Some(DECLARED_MEMORY)).unwrap();
        let memory = Arc::clone(&load.source.verified_ordered_runs.memory);
        let free = DECLARED_MEMORY.saturating_sub(memory.accountant.usage().used);
        memory
            .reserve(free.saturating_sub(FITS_SHORTFALL_NOT_PARTITION))
            .expect("the session may hold what it has already admitted");
        assert!(
            memory.accountant.usage().available.unwrap() <= FITS_SHORTFALL_NOT_PARTITION,
            "the session really is down to its last {FITS_SHORTFALL_NOT_PARTITION} bytes"
        );

        let mut walk = cursor(false);
        let mut delivered = Vec::new();
        loop {
            match pull(&load, &mut walk) {
                Ok(Some(id)) => delivered.push(id),
                Ok(None) => break,
                Err(error) => panic!(
                    "a window whose remaining shortfall fits in \
                     {FITS_SHORTFALL_NOT_PARTITION} bytes was refused because growth \
                     rounded the shortfall up to 64 KiB: {error}"
                ),
            }
        }
        assert_eq!(
            delivered,
            (1..=5u64).map(RowId).collect::<Vec<_>>(),
            "growth at a partition boundary admits the exact shortfall"
        );
    }
}

/// The only remaining refusal is a genuine one: a budget that cannot hold a
/// working page and the path to it at all. It stays the existing typed error.
#[test]
fn a_budget_too_small_for_one_working_page_is_still_the_typed_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("no-room.db");
    fixture(&path);
    let load = load_read_image_with_memory(&path, Some(256 * 1024 * 1024)).unwrap();
    let memory = Arc::clone(&load.source.verified_ordered_runs.memory);
    let free = (256 * 1024 * 1024usize).saturating_sub(memory.accountant.usage().used);
    // Leave a few hundred bytes: not a page, not a descent, not one posting.
    memory
        .reserve(free.saturating_sub(256))
        .expect("the session may hold what it has already admitted");

    let mut walk = cursor(false);
    let refused = pull(&load, &mut walk);
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "a budget that cannot hold one working page is the existing typed \
         refusal, not an answer from the physical index: {refused:?}"
    );
}
