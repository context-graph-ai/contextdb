//! A durable bounded read keeps its captured physical root across publication
//! and cleanup, including the pause before logical registration.
use super::*;
use crate::local_transport::ManualDeadlineClock;

fn captured_read_survives_compaction(cursor: bool) {
    let dir = tempfile::tempdir().expect("temporary durable store");
    let db = Arc::new(Database::open(dir.path().join("atomic-read.db")).expect("open store"));
    db.__set_currency_maintenance_interval(Duration::from_secs(3600));
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("create history-limited table");
    for (id, body) in [(1, "original"), (2, "stable")] {
        db.execute(
            "INSERT INTO items (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("body".to_owned(), Value::Text(body.to_owned())),
            ]),
        )
        .expect("seed row");
    }

    let pause = ApplyPhasePauseGuard::arm_for_test(
        db.snapshot_registry.durable_admission_test_pause.clone(),
    );
    let reader_db = Arc::clone(&db);
    let reader = std::thread::spawn(move || {
        let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::at(0));
        let budget = ReadRegistrationBudget::new(Arc::clone(&clock), 0, 10_000);
        let withdrawn = OwnerReadCancellation::new();
        if cursor {
            let fixture_operation_reader_1 = reader_db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            let (snapshot, _, _, storage, registration, root) = reader_db
                .bounded_cursor_snapshot_registration_with_root(
                    clock,
                    10_000,
                    &budget,
                    &withdrawn,
                    fixture_operation_reader_1.clone(),
                )
                .expect("cursor admission succeeds")
                .expect("cursor remains active");
            assert!(storage.is_some(), "cursor captured a durable root");
            (
                snapshot,
                Box::new((storage, registration)) as Box<dyn Send + Sync>,
                root.expect("cursor retains its captured physical root"),
            )
        } else {
            let fixture_operation_reader_2 = reader_db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            let (snapshot, _, registration, root) = reader_db
                .bounded_read_snapshot_registration_with_root(
                    &budget,
                    &withdrawn,
                    fixture_operation_reader_2.clone(),
                )
                .expect("read admission succeeds")
                .expect("read remains active");
            (
                snapshot,
                registration,
                root.expect("pull retains its captured physical root"),
            )
        }
    });

    assert!(
        pause.wait_until_reached_blocking(),
        "capture reached registration seam"
    );
    assert!(
        db.tx_mgr
            .try_with_commit_lock_for(Duration::ZERO, || ())
            .is_some(),
        "a captured physical root permits foreground commit progress"
    );
    let writer_db = Arc::clone(&db);
    let (attempted, attempt_seen) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        attempted.send(()).expect("report writer attempt");
        writer_db
            .execute(
                "UPDATE items SET body = 'replacement' WHERE id = 1",
                &HashMap::new(),
            )
            .expect("replace row");
        writer_db
            .compact_currency_versions()
            .expect("drive cleanup")
    });
    attempt_seen
        .recv()
        .expect("writer starts while reader is paused");
    // Join before releasing capture: writer and latest-root cleanup must finish
    // while the reader is still paused, protected solely by its physical root.
    let report = writer
        .join()
        .expect("writer completes while capture is paused");
    pause.release();
    let (snapshot, pin, root) = reader.join().expect("reader thread completes");
    assert_eq!(
        report.pruned_versions, 1,
        "latest-root cleanup reclaims the superseded version"
    );
    assert_eq!(db.__physical_version_count_for_test("items"), 2);
    let identity = root.storage_identity();
    assert_ne!(
        identity,
        db.writer_read_root().unwrap().unwrap().storage_identity(),
        "the registered reader retains the old physical identity"
    );
    assert!(
        db.__native_read_transaction_count_for_test(identity.transaction_id)
            .unwrap()
            > 0
    );
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let scope = db.enter_writer_read(Some(root.clone()), fixture_operation_reader_1.clone());
    let result = db
        .execute_at_snapshot(
            "SELECT id, body FROM items ORDER BY id",
            &HashMap::new(),
            snapshot,
        )
        .expect("read the selected complete snapshot");
    assert_eq!(
        result.rows(),
        vec![
            vec![Value::Int64(1), Value::Text("original".to_owned())],
            vec![Value::Int64(2), Value::Text("stable".to_owned())],
        ]
    );
    drop(scope);
    drop(pin);
    drop(root);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
    assert_eq!(
        db.__native_read_transaction_count_for_test(identity.transaction_id)
            .unwrap(),
        0,
        "final unpin releases the retained physical transaction"
    );
    db.compact_currency_versions()
        .expect("cleanup after reader release");
    assert_eq!(
        db.execute("SELECT id, body FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        vec![
            vec![Value::Int64(1), Value::Text("replacement".to_owned())],
            vec![Value::Int64(2), Value::Text("stable".to_owned())]
        ]
    );
}

#[test]
fn durable_pull_registers_before_releasing_its_captured_root() {
    captured_read_survives_compaction(false);
}

#[test]
fn durable_cursor_registers_before_releasing_its_captured_root() {
    captured_read_survives_compaction(true);
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn durable_admission_rechecks_cancellation_and_deadline_before_registering() {
    for cursor in [false, true] {
        for cancelled in [false, true] {
            let dir = tempfile::tempdir().expect("temporary durable store");
            let db = Arc::new(Database::open(dir.path().join("withdrawn.db")).expect("open store"));
            let physical = db.writer_read_root().unwrap().unwrap().storage_identity();
            let native_before = db
                .__native_read_transaction_count_for_test(physical.transaction_id)
                .unwrap();
            let pause = ApplyPhasePauseGuard::arm_for_test(
                db.snapshot_registry.durable_admission_test_pause.clone(),
            );
            let clock = Arc::new(ManualDeadlineClock::at(0));
            let withdrawn = OwnerReadCancellation::new();
            let reader_db = Arc::clone(&db);
            let reader_clock = Arc::clone(&clock);
            let reader_withdrawn = withdrawn.clone();
            let reader = std::thread::spawn(move || {
                let clock: Arc<dyn DeadlineClock> = reader_clock;
                let budget = ReadRegistrationBudget::new(Arc::clone(&clock), 0, 5);
                if cursor {
                    let fixture_operation_reader_3 = reader_db
                        .declared_reader_memory()
                        .expect("admit the actual fixture reader");

                    reader_db
                        .bounded_cursor_snapshot_registration(
                            clock,
                            5,
                            &budget,
                            &reader_withdrawn,
                            fixture_operation_reader_3.clone(),
                        )
                        .map(|admitted| admitted.is_some())
                } else {
                    let fixture_operation_reader_4 = reader_db
                        .declared_reader_memory()
                        .expect("admit the actual fixture reader");

                    reader_db
                        .bounded_read_snapshot_registration(
                            &budget,
                            &reader_withdrawn,
                            fixture_operation_reader_4.clone(),
                        )
                        .map(|admitted| admitted.is_some())
                }
            });
            assert!(pause.wait_until_reached_blocking());
            if cancelled {
                withdrawn.cancel();
            } else {
                clock.advance_to(6);
            }
            pause.release();
            let admitted = reader.join().expect("reader exits");
            if cancelled {
                assert!(!admitted.expect("cancellation is not an error"));
            } else {
                assert!(matches!(
                    admitted,
                    Err(Error::ReadFailure(failure))
                        if failure.kind()
                            == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
                ));
            }
            assert!(db.snapshot_registry.state.lock().active.is_empty());
            assert_eq!(
                db.__native_read_transaction_count_for_test(physical.transaction_id)
                    .unwrap(),
                native_before,
                "cancelled or expired registration retains no extra physical root"
            );
        }
    }
}

// Acceptance: new readers capture the current physical publication while
// historical visibility remains at the requested logical snapshot. An older
// reader with that same logical snapshot must not supply the new reader's root.
#[test]
fn fresh_snapshot_pin_does_not_borrow_an_older_readers_root() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().expect("temporary durable store");
    let db = Database::open_with_options(
        dir.path().join("fresh-capture.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .expect("open store");
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC OFF",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'original')", &HashMap::new())
        .unwrap();
    let snapshot = db.snapshot();
    let older_pin = db
        .pin_snapshot(snapshot)
        .expect("first reader captures root");
    let older_root = older_pin._root.as_ref().expect("durable first reader");
    let older_identity = older_root.storage_identity();

    db.execute(
        "UPDATE items SET body = 'replacement' WHERE id = 1",
        &HashMap::new(),
    )
    .unwrap();
    let current_identity = db
        .writer_read_root()
        .unwrap()
        .expect("current publication")
        .storage_identity();
    assert_ne!(
        older_identity, current_identity,
        "fixture publishes a new root"
    );

    let fresh_pin = db.pin_snapshot(snapshot).expect("fresh pin");
    let selected_root = fresh_pin
        ._root
        .clone()
        .expect("fresh reader has a durable root");
    assert_eq!(
        selected_root.storage_identity(),
        current_identity,
        "a fresh reader must not borrow the older reader's physical root"
    );
    let scope = db.enter_writer_read(
        Some(selected_root),
        db.declared_reader_memory().expect("admit fixture reader"),
    );
    assert_eq!(
        db.execute_at_snapshot(
            "SELECT id, body FROM items ORDER BY id",
            &HashMap::new(),
            snapshot,
        )
        .unwrap()
        .rows(),
        vec![vec![Value::Int64(1), Value::Text("original".to_owned())]],
        "current physical storage still filters out the later logical version"
    );
    drop(scope);
    assert_eq!(
        db.execute("SELECT body FROM items", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Text("replacement".to_owned())]]
    );
    drop(fresh_pin);
    drop(older_pin);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
}

// Historical vector authenticity is a different criterion from relational pin
// registration: an internally consistent physical fingerprint cannot substitute
// for authenticating selected placement companions and their global controls.
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn historical_vector_queries_refuse_damaged_companions_in_the_selected_root() {
    use redb::ReadableTable as _;

    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let companions = [
        (
            "vector_partition_membership",
            "vector_membership_authority_pages_root_v1",
        ),
        (
            "vector_membership_source_pages",
            "vector_membership_source_pages_root_v1",
        ),
        (
            "vector_membership_reverse_pages",
            "vector_membership_reverse_pages_root_v1",
        ),
        (
            "vector_open_directory",
            "vector_open_directory_pages_root_v1",
        ),
        (
            "vector_membership_summary_pages",
            "vector_membership_summary_pages_root_v1",
        ),
    ];
    let mut cases = Vec::new();
    for (table, fingerprint) in companions {
        for damage in ["missing", "extra", "contradictory"] {
            for rebind in [false, true] {
                cases.push((table, Some(fingerprint), damage, rebind));
            }
        }
    }
    for damage in ["count", "digest"] {
        cases.push((
            "vector_open_directory",
            Some("vector_open_directory_pages_root_v1"),
            damage,
            true,
        ));
    }

    for damage in ["missing", "digest"] {
        cases.push(("__content_identity_metadata", None, damage, false));
    }

    assert_eq!(
        cases.len(),
        34,
        "every original companion-damage row remains covered"
    );
    for (table_name, fingerprint_key, damage, rebind) in cases {
        let case = format!("{table_name}/{damage}/rebind={rebind}");
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            dir.path().join("historical-vectors.db"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute(
            "CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(2) SEARCH_MODE EXACT) HISTORY ALL SYNC OFF",
            &HashMap::new(),
        ).unwrap();
        db.execute("INSERT INTO vectors VALUES (1, '[1,0]')", &HashMap::new())
            .unwrap();
        let historical = db.snapshot();
        let retained = db.writer_read_root().unwrap().unwrap();
        db.execute(
            "UPDATE vectors SET embedding = '[0,1]' WHERE id = 1",
            &HashMap::new(),
        )
        .unwrap();
        let successor = db.writer_read_root().unwrap().unwrap();
        assert_ne!(
            retained.storage_identity(),
            successor.storage_identity(),
            "{case}"
        );
        // A raw projection uses __pk_id and never consumes vector membership.
        // Exact vector search enters the real historical vector preflight on
        // the selected physical root before candidate discovery and scoring.
        let sql = "SELECT id, embedding FROM vectors ORDER BY embedding <=> [1,0] USE VECTOR EXACT LIMIT 1";
        let expected = vec![vec![Value::Int64(1), Value::Vector(vec![1.0, 0.0])]];
        // A newer physical root with an old logical snapshot is valid. Neither
        // a different physical identity nor an old logical time is corruption.
        {
            let _scope = db.enter_writer_read(
                Some(successor.clone()),
                db.declared_reader_memory().unwrap(),
            );
            let answer = db
                .execute_at_snapshot(sql, &HashMap::new(), historical)
                .unwrap();
            assert_eq!(answer.rows(), expected, "healthy successor: {case}");
            let route = answer
                .trace()
                .vector_search
                .as_ref()
                .expect("the healthy control must execute the vector consumer");
            assert_eq!(
                route.route_evidence,
                VectorSearchRouteEvidence::Executed,
                "{case}"
            );
            assert_eq!(route.route, Some(VectorSearchRoute::Exact), "{case}");
        }
        let persistence = db.persistence.as_ref().unwrap();
        persistence
            .with_db(|native| {
                let write = native.begin_write().unwrap();
                if table_name == "__content_identity_metadata" {
                    let mut table = write
                        .open_table(redb::TableDefinition::<&str, &[u8]>::new(table_name))
                        .unwrap();
                    let key = "vector_open_directory_root_v1";
                    let mut bytes = table
                        .get(key)
                        .unwrap()
                        .expect("producer directory commitment")
                        .value()
                        .to_vec();
                    assert_eq!(bytes.len(), 32);
                    if damage == "missing" {
                        table.remove(key).unwrap();
                    } else {
                        bytes[31] ^= 1;
                        table.insert(key, bytes.as_slice()).unwrap();
                    }
                } else {
                    let definition = redb::TableDefinition::<&[u8], &[u8]>::new(table_name);
                    {
                        let mut table = write.open_table(definition).unwrap();
                        let records: Vec<_> = table
                            .iter()
                            .unwrap()
                            .map(|row| {
                                let (key, value) = row.unwrap();
                                (key.value().to_vec(), value.value().to_vec())
                            })
                            .collect();
                        let (key, value) = records
                            .iter()
                            .find(|(key, _)| !key.is_empty())
                            .expect("real producer companion");
                        match damage {
                            "missing" => {
                                table.remove(key.as_slice()).unwrap();
                            }
                            "extra" => {
                                let mut extra = key.clone();
                                extra.push(0);
                                assert!(table.get(extra.as_slice()).unwrap().is_none());
                                table.insert(extra.as_slice(), value.as_slice()).unwrap();
                            }
                            "contradictory" => {
                                // Reuse a real successor's valid envelope when one
                                // exists: this is a key/identity contradiction,
                                // rather than an invented serialized membership.
                                let replacement = records
                                    .iter()
                                    .find(|(other, _)| other != key && !other.is_empty());
                                let mut bytes = replacement
                                    .map_or_else(|| value.clone(), |(_, bytes)| bytes.clone());
                                if replacement.is_none() {
                                    bytes[0] ^= 1;
                                }
                                table.insert(key.as_slice(), bytes.as_slice()).unwrap();
                            }
                            "count" | "digest" => {
                                let mut root = table
                                    .get(&[][..])
                                    .unwrap()
                                    .expect("producer global directory root")
                                    .value()
                                    .to_vec();
                                assert_eq!(root.len(), 40);
                                root[if damage == "count" { 7 } else { 39 }] ^= 1;
                                table.insert(&[][..], root.as_slice()).unwrap();
                            }
                            _ => unreachable!(),
                        }
                    }
                    if rebind {
                        // Use the storage engine's actual fingerprint. Only this
                        // physical binding is updated; semantic companions and
                        // the producer's global commitments remain untouched.
                        let fingerprint =
                            write.table_root_fingerprint(definition).unwrap().unwrap();
                        write
                            .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                                "__content_identity_metadata",
                            ))
                            .unwrap()
                            .insert(fingerprint_key.unwrap(), fingerprint.as_slice())
                            .unwrap();
                    }
                }
                write.commit().unwrap();
                Ok(())
            })
            .unwrap();
        let damaged = persistence
            .initialize_writer_snapshot(db.snapshot(), db.current_lsn())
            .unwrap();
        assert_ne!(
            successor.storage_identity(),
            damaged.storage_identity(),
            "damage committed: {case}"
        );
        {
            let _scope =
                db.enter_writer_read(Some(damaged.into()), db.declared_reader_memory().unwrap());
            let answer = db.execute_at_snapshot(sql, &HashMap::new(), historical);
            assert!(
                matches!(&answer, Err(Error::StoreCorrupted { .. })),
                "selected damaged root must refuse typed corruption: {case}: {answer:?}"
            );
        }
        // A reader already holding the earlier physical root must still return
        // the exact vector after companion damage in a later transaction.
        {
            let _scope = db.enter_writer_read(Some(retained), db.declared_reader_memory().unwrap());
            assert_eq!(
                db.execute_at_snapshot(sql, &HashMap::new(), historical)
                    .unwrap()
                    .rows(),
                expected,
                "retained root: {case}"
            );
        }
    }
}

#[cfg(feature = "test-seams")]
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn prepared_metadata_publication_keeps_bounded_vector_reads_and_cursors_available() {
    use crate::executor::bounded_read_test_support as bounded;
    use contextdb_core::read_contract::ReadLimits;

    #[derive(Default)]
    struct Registrations(std::sync::Mutex<Vec<SnapshotId>>);
    impl bounded::ExecutionProbe for Registrations {
        fn registered_snapshot_for_test(&self, snapshot: SnapshotId) {
            self.0.lock().expect("registrations").push(snapshot);
        }
        fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
        fn cancellation_observed(&self, _: u64) {}
    }
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_owned_for_test(
            &directory.path().join("metadata.db"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    db.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(2) SEARCH_MODE EXACT) SYNC OFF", &HashMap::new()).unwrap();
    db.execute("INSERT INTO vectors VALUES (1, '[1,0]')", &HashMap::new())
        .unwrap();
    let published = db.tx_mgr.published_read_snapshot();
    let registrations = Arc::new(Registrations::default());
    db.tx_mgr.with_maintenance_publication(|publication| {
        // This is the actual guard acquired by native sink attempt/ack writes
        // before they enter storage, held until their joint settlement ends.
        let prepared = publication.prepare_exact_root()?;
        assert_eq!((prepared.snapshot(), prepared.lsn()), published);
        let reader = Arc::clone(&db);
        let probe = Arc::clone(&registrations);
        std::thread::spawn(move || {
            let mut request = bounded::BoundedReadRequest::new(
                "SELECT id FROM vectors ORDER BY embedding <=> $probe LIMIT 1",
                HashMap::from([("probe".to_owned(), Value::Vector(vec![1.0, 0.0]))]),
                ReadLimits::default(), Arc::new(ManualDeadlineClock::at(0)),
            );
            request.probe = Some(probe as Arc<dyn bounded::ExecutionProbe>);
            let result = bounded::execute(&reader, &request).expect("bounded read during metadata preparation");
            assert_eq!(result.result.rows(), vec![vec![Value::Int64(1)]]);
            let cursor = bounded::open_cursor(reader, &request).expect("bounded cursor during metadata preparation");
            assert_eq!(cursor.first_page.rows(), vec![vec![Value::Int64(1)]]);
            assert!(!cursor.first_page.has_more());
        }).join().expect("both reads finish while the metadata guard is still live");
        assert_eq!((prepared.snapshot(), prepared.lsn()), published);
        assert_eq!(*registrations.0.lock().unwrap(), vec![published.0, published.0]);
        eprintln!("prepared metadata read receipt: snapshot {:?}, LSN {:?}; both bounded forms returned the committed row while the guard was live", published.0, published.1);
        Ok(())
    }).unwrap();
    db.close()
        .expect("close the native writer after both reads finish");
}
