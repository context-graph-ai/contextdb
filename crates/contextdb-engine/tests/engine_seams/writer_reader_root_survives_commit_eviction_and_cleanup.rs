//! Acceptance: "Existing readers must finish on their original view through writer acquisition,
//! commits and maintenance, without retry, false corruption, newer-state substitution or blocking
//! the writer for the session lifetime. New readers see complete committed state."

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey, Wallclock};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commit};
use contextdb_engine::writer_query_test_seams::{
    PhysicalFamily, observe as observe_query, observe_target,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{
    collections::HashMap,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
};

struct StatementPause {
    reached: mpsc::Sender<()>,
    release: Arc<Barrier>,
    entered: AtomicBool,
}
impl bounded::ExecutionProbe for StatementPause {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
    fn after_source_capture(&self) {
        if !self.entered.swap(true, Ordering::SeqCst) {
            self.reached.send(()).unwrap();
            self.release.wait();
        }
    }
    fn cancellation_observed(&self, _: u64) {}
}

// Release a captured statement even if cache displacement or writer checks panic.
struct StatementRelease(Arc<Barrier>);
impl Drop for StatementRelease {
    fn drop(&mut self) {
        self.0.wait();
    }
}

#[test]
fn writer_reader_root_survives_commit_eviction_and_cleanup() {
    for boundary in [
        CommitBoundary::BeforeRedb,
        CommitBoundary::BeforeMemoryPublication,
    ] {
        reader_at_boundary(boundary);
    }
}

fn reader_at_boundary(boundary: CommitBoundary) {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reader-roots.redb");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            memory_limit: Some(64 * 1024 * 1024),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    database.execute("CREATE TABLE reader_items (id INTEGER PRIMARY KEY, unique_key TEXT UNIQUE, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 8, EF_CONSTRUCTION = 32)) HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST", &HashMap::new()).unwrap();
    let old_vectors = [
        vec![1.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, 0.0, 1.0],
    ];
    for (offset, vector) in old_vectors.iter().enumerate() {
        database
            .execute(
                "INSERT INTO reader_items VALUES ($id, $key, $body, $vector)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(offset as i64 + 1)),
                    ("key".to_owned(), Value::Text(format!("old-{}", offset + 1))),
                    ("body".to_owned(), Value::Text("b".repeat(64 * 1024))),
                    ("vector".to_owned(), Value::Vector(vector.clone())),
                ]),
            )
            .unwrap();
        // Spread the three answer identities across the physical row tree,
        // beyond the cursor's first/pending leaf. The same 128 scalar filler
        // rows have NULL embeddings and keep the vector answer set unchanged.
        if offset < 2 {
            for id in 1000 + offset as i64 * 64..1064 + offset as i64 * 64 {
                database
                    .execute(
                        "INSERT INTO reader_items VALUES ($id, $key, NULL, NULL)",
                        &HashMap::from([
                            ("id".to_owned(), Value::Int64(id)),
                            (
                                "key".to_owned(),
                                Value::Text(format!("filler-{id}-{}", "x".repeat(1024))),
                            ),
                        ]),
                    )
                    .unwrap();
            }
        }
    }
    for _ in 0..8 {
        database.run_maintenance_cycle().unwrap();
    }
    database
        .execute(
            "CREATE TABLE displacement (id INT PRIMARY KEY, body TEXT) SYNC OFF",
            &HashMap::new(),
        )
        .unwrap();
    // Pressure many times the entire cache share, across all second-chance stripes.
    for batch in 0..16 {
        let mut params = HashMap::from([("body".to_owned(), Value::Text("p".repeat(128 * 1024)))]);
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
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                memory_limit: Some(64 * 1024 * 1024),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let relation_height = database
        .__durable_relation_tree_height_for_test("reader_items")
        .unwrap();
    assert!(
        relation_height > 1,
        "cold-row fixture requires actual child leaves below the eagerly opened relation root: height={relation_height}"
    );
    eprintln!(
        "reader_items fixture: 3 vector rows + 128 scalar filler rows; actual tree height={relation_height}"
    );
    let old_snapshot = database.snapshot();

    let pinned = database.pin_snapshot(old_snapshot);
    let transaction = database.begin_or_panic();
    let sql = "SELECT id, unique_key, embedding FROM reader_items WHERE unique_key >= 'old-' ORDER BY unique_key";
    let successor_sql =
        "SELECT id, unique_key, embedding FROM reader_items WHERE id <= 3 ORDER BY id";
    let cache_share = contextdb_engine::persistence::redb_writer_cache_bytes_for_limit_for_test(
        Some(64 * 1024 * 1024),
    );
    assert_eq!(
        database
            .writer_memory_ownership_receipt_for_test()
            .persistence_cache,
        cache_share,
        "the reopened writer owns the cache share derived from its declared memory limit"
    );
    let displace = || {
        let before = database.__writer_cache_stats_for_test().unwrap();
        let backend_bytes = Arc::new(AtomicU64::new(0));
        let counted = backend_bytes.clone();
        let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
            counted.fetch_add(bytes as u64, Ordering::Relaxed);
        }));
        // Read actual wide values, not just the keys on the path to the last
        // ordinal. These 64 bodies alone exceed the declared page-cache share.
        for ordinal in 0..64 {
            let page = database
                .__hold_row_page_for_test("displacement", ordinal)
                .unwrap();
            assert!(page.value().len() >= 128 * 1024);
            drop(page);
        }
        drop(observation);
        let pressure_bytes = backend_bytes.load(Ordering::Relaxed);
        let after = database.__writer_cache_stats_for_test().unwrap();
        eprintln!(
            "reader displacement: memory_limit=67108864 cache_share={cache_share} actual_backend_bytes={pressure_bytes} before={before:?} after={after:?}"
        );
        assert!(
            pressure_bytes > cache_share as u64,
            "actual backend pressure must exceed this writer's declared cache share: bytes={pressure_bytes}; share={cache_share}"
        );
        assert!(
            after.2 > before.2 && after.1 > before.1,
            "reader displacement must cause actual cache misses and evictions: before={before:?}; after={after:?}"
        );
    };
    let expected_old = old_vectors
        .iter()
        .enumerate()
        .map(|(offset, vector)| {
            vec![
                Value::Int64(offset as i64 + 1),
                Value::Text(format!("old-{}", offset + 1)),
                Value::Vector(vector.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let (initial_transaction, initial_transaction_work) = observe_target("reader_items", || {
        database.execute_in_tx(transaction, sql, &HashMap::new())
    });
    assert_eq!(initial_transaction.unwrap().rows, expected_old);
    let clock = Arc::new(ManualDeadlineClock::at(0));
    let limits = ReadLimits {
        cursor_page_rows: 1,
        result_rows: 32,
        work: 100_000,
        active_ms: 1000,
        memory: 64 * 1024 * 1024,
        ..Default::default()
    };
    let request = bounded::BoundedReadRequest::new(sql, HashMap::new(), limits, clock.clone());
    let (opened, admission_sources) = observe_target("reader_items", || {
        bounded::open_cursor(database.clone(), &request)
    });
    let mut opened = opened.unwrap();
    let original_physical = admission_sources
        .physical_sources
        .iter()
        .flatten()
        .find_map(|(family, identity)| {
            (*family == PhysicalFamily::Row || *family == PhysicalFamily::Posting)
                .then_some(*identity)
        });
    assert!(
        original_physical.is_some(),
        "reader admission must expose its actual row/posting storage transaction, without opening an independent root"
    );
    let original_physical = original_physical.unwrap();
    assert!(
        database
            .__native_read_transaction_count_for_test(original_physical.transaction_id)
            .unwrap()
            > 0,
        "cursor admission retains an actual native read transaction at its captured root"
    );
    assert!(
        opened.cursor.continuation_bytes().unwrap().total > 0,
        "open cursor owns real continuation scratch"
    );
    assert_eq!(opened.first_page.rows, expected_old[..1]);
    let (statement_reached, statement_started) = mpsc::channel();
    let statement_release = Arc::new(Barrier::new(2));
    let pause = Arc::new(StatementPause {
        reached: statement_reached,
        release: statement_release.clone(),
        entered: AtomicBool::new(false),
    });
    let (commit_reached, commit_started) = mpsc::channel();
    let (commit_release, commit_resume) = mpsc::channel();
    let commit_resume = Arc::new(Mutex::new(commit_resume));
    let waits = Arc::new(AtomicU64::new(0));
    let during_clock = Arc::new(ManualDeadlineClock::at(0));
    let (statement, during, during_work, evicted, cleanup) = std::thread::scope(|scope| {
        let statement = scope.spawn(|| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let mut request =
                bounded::BoundedReadRequest::new(sql, HashMap::new(), limits, clock.clone());
            request.probe = Some(pause);
            observe_query(|| bounded::execute(&database, &request))
        });
        statement_started.recv().unwrap();
        let release_statement = StatementRelease(statement_release.clone());
        let writer = scope.spawn(|| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let tx = database.begin_or_panic();
            for id in 1..=3 {
                database.execute_in_tx(tx, "UPDATE reader_items SET unique_key = $key, embedding = $vector WHERE id = $id", &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)), ("key".to_owned(), Value::Text(format!("new-{id}"))),
                    ("vector".to_owned(), Value::Vector(vec![0.1, 0.2, 0.3])),
                ])).unwrap();
            }
            observe_commit(Arc::new(move |point| {
                if point == boundary {
                    commit_reached.send(()).unwrap();
                    commit_resume.lock().unwrap().recv().unwrap();
                }
            }), || database.commit(tx)).unwrap();
        });
        commit_started.recv().unwrap();
        let captured_before_read = database.snapshot();
        let wait_clock = during_clock.clone();
        let observed_waits = waits.clone();
        let (during, during_work) = contextdb_tx::commit_wait_test_seams::observe(
            Arc::new(move || {
                observed_waits.fetch_add(1, Ordering::SeqCst);
                wait_clock.advance_to(1000);
            }),
            || {
                observe_query(|| {
                    bounded::execute(
                        &database,
                        &bounded::BoundedReadRequest::new(
                            sql,
                            HashMap::new(),
                            limits,
                            during_clock.clone(),
                        ),
                    )
                })
            },
        );
        // Release the writer before any assertion so even a refused reader cannot
        // strand the scoped writer/statement threads during assertion unwinding.
        commit_release.send(()).unwrap();
        writer.join().unwrap();
        displace();
        let evicted = database
            .vector_store_for_test()
            .evict_raw_partition_for_test(
                &VectorIndexRef::new("reader_items", "embedding"),
                &VectorPartitionKey::default(),
            );
        let cleanup = database.compact_currency_versions();
        drop(release_statement);
        let statement = statement.join().unwrap();
        assert_eq!(
            captured_before_read, old_snapshot,
            "a paused commit has not replaced the published transaction root"
        );
        (statement, during, during_work, evicted, cleanup)
    });
    cleanup.unwrap();
    assert_eq!(
        statement.0.unwrap().result.rows,
        expected_old,
        "an active statement retains its original rows, keys and vectors across commit and cleanup"
    );
    assert_eq!(
        statement.1.captured_read_snapshot,
        Some(old_snapshot.0),
        "the active statement actually captured the original committed transaction"
    );
    let expected_new = (1..=3)
        .map(|id| {
            vec![
                Value::Int64(id),
                Value::Text(format!("new-{id}")),
                Value::Vector(vec![0.1, 0.2, 0.3]),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        database
            .execute(successor_sql, &HashMap::new())
            .unwrap()
            .rows,
        expected_new,
        "a newer reader sees the complete successor"
    );
    displace();
    let (historical_result, historical_work) = observe_target("reader_items", || {
        database.execute_at_snapshot(sql, &HashMap::new(), old_snapshot)
    });
    assert_eq!(historical_result.unwrap().rows, expected_old);
    displace();
    let (transaction_result, transaction_work) = observe_target("reader_items", || {
        database.execute_in_tx(transaction, sql, &HashMap::new())
    });
    assert_eq!(transaction_result.unwrap().rows, expected_old);
    displace();
    let (successor_result, successor_work) = observe_target("reader_items", || {
        bounded::execute(
            &database,
            &bounded::BoundedReadRequest::new(successor_sql, HashMap::new(), limits, clock.clone()),
        )
    });
    assert_eq!(successor_result.unwrap().result.rows, expected_new);
    let successor_physical = successor_work
        .physical_sources
        .iter()
        .flatten()
        .find_map(|(family, identity)| {
            (*family == PhysicalFamily::Row || *family == PhysicalFamily::Posting)
                .then_some(*identity)
        })
        .expect("successor statement must observe its own actual physical page root");
    // Only the three answer rows have vectors; the maintained vector route excludes NULLs.
    let (_, indexed_work) = observe_target("reader_items", || {
        for (offset, vector) in old_vectors.iter().enumerate() {
            assert_eq!(database.execute_at_snapshot("SELECT id FROM reader_items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
                &HashMap::from([("q".into(),Value::Vector(vector.clone()))]),old_snapshot).unwrap().rows,
                vec![vec![Value::Int64(offset as i64 + 1)]], "old reader traverses its maintained vector generation");
        }
    });
    let (_, indexed_transaction_work) = observe_target("reader_items", || {
        for (offset, vector) in old_vectors.iter().enumerate() {
            assert_eq!(database.execute_in_tx(transaction,
                "SELECT id FROM reader_items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
                &HashMap::from([("q".into(), Value::Vector(vector.clone()))])).unwrap().rows,
                vec![vec![Value::Int64(offset as i64 + 1)]]);
        }
    });
    assert_ne!(
        original_physical, successor_physical,
        "the successor has a different real storage transaction/root"
    );
    // Leave the cursor as the final holder of the retired original root.
    database.rollback(transaction).unwrap();
    drop(pinned);
    assert!(
        database
            .__native_read_transaction_count_for_test(original_physical.transaction_id)
            .unwrap()
            > 0,
        "the continuation itself keeps its retired native root alive"
    );
    // The intervening vector checks project these same old rows and warm
    // their pages again. Displace after those checks, immediately before
    // testing continuation reloads from the last holder of the old root.
    displace();
    let mut has_more = opened.first_page.has_more;
    let mut continued = opened.first_page.rows;
    let mut continuation_sources = Vec::new();
    for _ in 0..expected_old.len() {
        if !has_more {
            break;
        }
        displace();
        let cache_before_pull = database.__writer_cache_stats_for_test().unwrap();
        // Family operations install nested observers. This outer scope counts
        // backend reads that happen outside those attributed operations.
        let pull_backend_bytes = Arc::new(AtomicU64::new(0));
        let counted = pull_backend_bytes.clone();
        let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
            counted.fetch_add(bytes as u64, Ordering::Relaxed);
        }));
        let (page, sources) = observe_target("reader_items", || {
            opened.cursor.fetch(None, Default::default())
        });
        drop(observation);
        eprintln!(
            "retained cursor pull: backend_bytes_outside_family_observers={} cache_before={cache_before_pull:?} cache_after={:?} sources={sources:?}",
            pull_backend_bytes.load(Ordering::Relaxed),
            database.__writer_cache_stats_for_test().unwrap(),
        );
        let page = page.unwrap().page;
        has_more = page.has_more;
        continued.extend(page.rows);
        if sources.physical_source_calls > 0 {
            continuation_sources.push(sources);
        }
    }
    assert!(
        !has_more,
        "the finite fixture must reach actual cursor completion"
    );
    assert_eq!(
        continued, expected_old,
        "the bounded continuation reloads its original row/key/vector view"
    );
    assert!(
        continuation_sources
            .iter()
            .any(|work| work.target_backend_bytes[PhysicalFamily::Row as usize] > 0)
            && continuation_sources
                .iter()
                .any(|work| work.target_backend_bytes[PhysicalFamily::Posting as usize] > 0),
        "the retained cursor must reload displaced target row and posting data pages: {continuation_sources:?}"
    );
    // Observe completion before explicit close; the cursor object remains alive.
    // The current writer retains its successor root throughout these checks.
    assert_eq!(
        database
            .__native_read_transaction_count_for_test(original_physical.transaction_id)
            .unwrap(),
        0,
        "completion releases the actual captured native transaction after its last old-root holder"
    );
    assert_eq!(
        opened.cursor.continuation_bytes().unwrap().total,
        0,
        "completion releases continuation scratch while the cursor object survives"
    );
    let cancellation_request =
        bounded::BoundedReadRequest::new(successor_sql, HashMap::new(), limits, clock.clone());
    let (cancelled_cursor, cancelled_work) = observe_target("reader_items", || {
        bounded::open_cursor(database.clone(), &cancellation_request)
    });
    let mut cancelled_cursor = cancelled_cursor.unwrap();
    assert!(
        cancelled_work
            .physical_sources
            .iter()
            .flatten()
            .all(|(_, root)| *root == successor_physical)
    );
    assert!(
        cancelled_cursor.cursor.continuation_bytes().unwrap().total > 0,
        "cancellable cursor owns real continuation scratch"
    );
    // Retire the successor so terminal cursor close can release its last
    // native owner without asserting database-wide absence of live readers.
    database
        .execute(
            "UPDATE displacement SET body = 'successor root' WHERE id = 0",
            &HashMap::new(),
        )
        .unwrap();
    assert!(
        database
            .__native_read_transaction_count_for_test(successor_physical.transaction_id)
            .unwrap()
            > 0,
        "cancellable cursor retains the retired successor's actual native transaction"
    );
    let cancellation = contextdb_engine::OwnerReadCancellation::new();
    cancellation.cancel();
    let retained_bytes = cancelled_cursor.cursor.continuation_bytes().unwrap().total;
    let retained_native = database
        .__native_read_transaction_count_for_test(successor_physical.transaction_id)
        .unwrap();
    let (cancelled, cancelled_fetch_work) = observe_target("reader_items", || {
        cancelled_cursor.cursor.fetch(None, cancellation)
    });
    assert!(matches!(cancelled, Err(bounded::TestError::Cancelled)));
    assert_eq!(cancelled_fetch_work.physical_source_calls, 0);
    assert_eq!(
        cancelled_cursor.cursor.continuation_bytes().unwrap().total,
        retained_bytes
    );
    assert_eq!(
        database
            .__native_read_transaction_count_for_test(successor_physical.transaction_id)
            .unwrap(),
        retained_native,
        "request cancellation retains the cursor's actual storage root"
    );
    let resumed = cancelled_cursor
        .cursor
        .fetch(None, Default::default())
        .unwrap()
        .page;
    assert_eq!(resumed.rows, vec![expected_new[1].clone()]);
    assert!(
        resumed.has_more,
        "the same cursor remains suspended after resuming its cancelled fetch"
    );
    assert!(cancelled_cursor.cursor.continuation_bytes().unwrap().total > 0);
    cancelled_cursor.cursor.close().unwrap();
    assert_eq!(
        database
            .__native_read_transaction_count_for_test(successor_physical.transaction_id)
            .unwrap(),
        0,
        "terminal close releases the actual retired native transaction without dropping the cursor object"
    );
    assert_eq!(
        cancelled_cursor.cursor.continuation_bytes().unwrap().total,
        0,
        "terminal close releases continuation scratch while the cursor object survives"
    );
    let refusal_clock = Arc::new(ManualDeadlineClock::at(0));
    let refusal_request = bounded::BoundedReadRequest::new(
        successor_sql,
        HashMap::new(),
        ReadLimits {
            cursor_idle_ms: 10,
            cursor_lifetime_ms: 100,
            ..limits
        },
        refusal_clock.clone(),
    );
    let (refused_cursor, refusal_work) = observe_target("reader_items", || {
        bounded::open_cursor(database.clone(), &refusal_request)
    });
    let refusal_physical = refusal_work
        .physical_sources
        .iter()
        .flatten()
        .next()
        .unwrap()
        .1;
    let mut refused_cursor = refused_cursor.unwrap();
    assert!(refused_cursor.first_page.has_more);
    assert!(refused_cursor.cursor.continuation_bytes().unwrap().total > 0);
    database
        .execute(
            "UPDATE displacement SET body = 'refusal root' WHERE id = 1",
            &HashMap::new(),
        )
        .unwrap();
    assert!(
        database
            .__native_read_transaction_count_for_test(refusal_physical.transaction_id)
            .unwrap()
            > 0
    );
    refusal_clock.advance_to(11);
    let error = refused_cursor
        .cursor
        .fetch(None, Default::default())
        .err()
        .expect("idle expiry is a terminal typed refusal");
    let bounded::TestError::Refused(refusal) = error else {
        panic!("expected typed idle expiry, got {error:?}")
    };
    assert_eq!(
        refusal.kind(),
        contextdb_core::read_contract::ReadFailureKind::CursorExpired
    );
    assert_eq!(
        refusal.detail(),
        &contextdb_core::read_contract::ReadFailureDetail::CursorExpired {
            expiry: contextdb_core::read_contract::CursorExpiryKind::Idle,
        }
    );
    assert_eq!(
        database
            .__native_read_transaction_count_for_test(refusal_physical.transaction_id)
            .unwrap(),
        0,
        "typed terminal refusal releases the retired root while the cursor object survives"
    );
    assert_eq!(refused_cursor.cursor.continuation_bytes().unwrap().total, 0);
    assert_eq!(
        database
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0,
        "terminal reads release vector scratch owners"
    );
    let released_cleanup = database.compact_currency_versions().unwrap();
    assert!(
        released_cleanup.pruned_versions > 0,
        "cleanup actually reclaims eligible superseded versions after the final reader releases"
    );
    assert!(
        evicted,
        "the fixture must exercise a real raw-vector eviction and reload"
    );
    assert_eq!(
        waits.load(Ordering::SeqCst),
        0,
        "a statement starting while a commit is paused must capture the published root without waiting for that commit"
    );
    assert_eq!(during_work.captured_read_snapshot, Some(old_snapshot.0));
    assert_eq!(during.unwrap().result.rows, expected_old);
    let mut observed_families = [false; 4];
    for (label, work) in [
        ("admission", admission_sources),
        ("active statement", statement.1),
        ("read during durability window", during_work),
        ("captured transaction", transaction_work),
        ("initial transaction", initial_transaction_work),
        (
            "captured transaction maintained generation",
            indexed_transaction_work,
        ),
    ]
    .into_iter()
    .chain(
        continuation_sources
            .into_iter()
            .map(|work| ("continuation", work)),
    ) {
        assert!(
            work.physical_source_calls > 0,
            "{label} must observe actual row/posting/vector page-source transactions after eviction: {work:?}"
        );
        assert!(
            work.physical_source_calls <= work.physical_sources.len(),
            "source provenance observation must not overflow"
        );
        for (family, identity) in work.physical_sources.into_iter().flatten() {
            observed_families[family as usize] = true;
            assert_eq!(
                identity, original_physical,
                "{label} page access must use the captured original physical root; fresh-root/old-logical-filter substitution is forbidden"
            );
        }
    }
    assert_eq!(
        observed_families, [true; 4],
        "reader must use row, posting, raw-vector and maintained-graph sources, not one arbitrary family"
    );
    for (label, work) in [
        ("later historical rows", historical_work),
        ("later historical maintained generation", indexed_work),
    ] {
        assert!(
            work.physical_source_calls > 0
                && work.physical_source_calls <= work.physical_sources.len(),
            "{label} must expose its current physical source: {work:?}"
        );
        assert!(
            work.physical_sources
                .iter()
                .flatten()
                .all(|(_, identity)| *identity == successor_physical),
            "{label} pins the current root and filters old MVCC intervals"
        );
    }
    assert!(
        continuation_sources_cold(&successor_work, &transaction_work, &historical_work),
        "displaced target rows and postings must really reload data pages from their attributed roots"
    );
    assert!(
        successor_work
            .physical_sources
            .iter()
            .flatten()
            .all(|(_, identity)| *identity == successor_physical),
        "every actual successor page access uses the successor root"
    );
    database.close().unwrap();
}

fn continuation_sources_cold(
    successor: &contextdb_engine::writer_query_test_seams::WriterQueryWork,
    transaction: &contextdb_engine::writer_query_test_seams::WriterQueryWork,
    historical: &contextdb_engine::writer_query_test_seams::WriterQueryWork,
) -> bool {
    [successor, transaction, historical].iter().all(|work| {
        work.target_backend_bytes[PhysicalFamily::Row as usize] > 0
            && work.target_backend_bytes[PhysicalFamily::Posting as usize] > 0
    })
}

struct PagedStatementPause {
    pause: StatementPause,
    registered: Arc<AtomicU64>,
}
impl bounded::ExecutionProbe for PagedStatementPause {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
    fn after_source_capture(&self) {
        self.pause.after_source_capture();
    }
    fn registered_snapshot_for_test(&self, snapshot: contextdb_core::SnapshotId) {
        self.registered.store(snapshot.0, Ordering::SeqCst);
    }
    fn cancellation_observed(&self, _: u64) {}
}

// Decode the one published BASE while the writer is closed. This checks the
// actual saved graph format, rather than a transient in-memory paging flag.
fn assert_native_paged_base(path: &std::path::Path) {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    #[derive(serde::Deserialize)]
    struct Base {
        _index: VectorIndexRef,
        _partition: VectorPartitionKey,
        _generation: u64,
        _base_tx: contextdb_core::TxId,
        _base_lsn: contextdb_core::Lsn,
        _highest_tx: contextdb_core::TxId,
        _highest_lsn: contextdb_core::Lsn,
        dimension: u32,
        _quantization: contextdb_core::VectorQuantization,
        _algorithm: u16,
        format: u16,
        graph: (Vec<u8>, [u8; 32]),
        _digest: [u8; 32],
    }
    let store = redb::Database::open(path).unwrap();
    let read = store.begin_read().unwrap();
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(
            "vector_partition_base_generations",
        ))
        .unwrap();
    let mut records = table.iter().unwrap();
    let (_, value) = records.next().unwrap().unwrap();
    assert!(
        records.next().is_none(),
        "this fixture publishes exactly one BASE"
    );
    let encoded = value.value();
    let magic = b"contextdb-vector-partition";
    assert_eq!(&encoded[..magic.len()], magic);
    let length_offset = magic.len() + 3;
    let payload_offset = length_offset + 8;
    let length =
        u64::from_be_bytes(encoded[length_offset..payload_offset].try_into().unwrap()) as usize;
    assert_eq!(encoded.len(), payload_offset + length + 32);
    let (base, consumed): (Base, usize) = bincode::serde::decode_from_slice(
        &encoded[payload_offset..payload_offset + length],
        bincode::config::standard(),
    )
    .unwrap();
    assert_eq!(consumed, length);
    assert_eq!(base.dimension, 3);
    assert_eq!(
        base.format, 0x0100,
        "published BASE uses VECTOR_GRAPH_PAGED_FORMAT_VERSION"
    );
    assert_eq!(
        &base.graph.0[..8],
        b"CDBVPG01",
        "BASE contains native paged metadata"
    );
    assert_eq!(*blake3::hash(&base.graph.0).as_bytes(), base.graph.1);
    eprintln!(
        "saved native BASE format={}, metadata_bytes={}",
        base.format,
        base.graph.0.len()
    );
}

// Pause after the kernel captures its sources, before its first paged pull.
// The successor must publish while this reader still owns the old native root.
#[test]
fn paged_vector_reader_keeps_the_root_captured_before_successor_publication() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paged-vector-root.redb");
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                memory_limit: Some(64 * 1024 * 1024),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    const DIMENSION: usize = 3;
    const ROWS: i64 = 1024;
    let vector = |axis: usize| {
        let mut value = vec![0.0; DIMENSION];
        value[axis] = 1.0;
        value
    };
    database.execute("CREATE TABLE paged_items (id INT PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 8, EF_CONSTRUCTION = 32)) HISTORY CURRENT ONLY SYNC OFF", &HashMap::new()).unwrap();
    for (id, vector) in [(1, vector(0)), (2, vector(1)), (3, vector(2))] {
        database
            .execute(
                "INSERT INTO paged_items VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vector)),
                ]),
            )
            .unwrap();
    }
    let tx = database.begin_or_panic();
    for id in 4..=ROWS {
        database
            .execute_in_tx(
                tx,
                "INSERT INTO paged_items VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vec![1.0, 1.0, 1.0])),
                ]),
            )
            .unwrap();
    }
    database.commit(tx).unwrap();
    let route = contextdb_vector::VectorPartitionRef::new(
        VectorIndexRef::new("paged_items", "embedding"),
        VectorPartitionKey::default(),
    );
    // Price the existing owners before the FIRST build. The complete 1024 x
    // 3 graph must exceed this finite construction allowance; the first
    // committed cursor below verifies that admission in the actual cache state.
    let owners = database
        .writer_memory_ownership_receipt_for_test()
        .named_owner_total;
    database
        .set_memory_limit(Some(owners + 256 * 1024))
        .unwrap();
    database.run_maintenance_cycle().unwrap();
    let first = database
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap();
    eprintln!("native initial owners={owners}, first={first:?}");
    assert!(
        first
            .maintenance_progress
            .is_some_and(|p| p.vectors_done > 0 && p.vectors_done < ROWS as usize),
        "native base construction must commit unfinished finite progress: {first:?}"
    );
    // The recorded unfinished build fixes the native paged route. Restore the
    // original writer budget so storage relocation can accompany completion;
    // this fixture proves serving-root identity, not builder peak residency.
    database.set_memory_limit(Some(64 * 1024 * 1024)).unwrap();
    for _ in 0..256 {
        let info = database
            .vector_store_for_test()
            .partition_info(&route.index, &route.partition_key)
            .unwrap();
        if info.graph_available && info.maintenance_progress.is_none() {
            break;
        }
        database.run_maintenance_cycle().unwrap();
    }
    let published = database
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap();
    assert!(
        published.graph_available && published.maintenance_progress.is_none(),
        "the native paged BASE must be finished and published before reopen: {published:?}"
    );
    assert!(
        database
            .vector_store_for_test()
            .partition_has_paged_storage(&route),
        "the finite native BASE must publish paged storage"
    );
    database.set_memory_limit(Some(64 * 1024 * 1024)).unwrap();
    database
        .execute(
            "CREATE TABLE displacement (id INT PRIMARY KEY, body TEXT) SYNC OFF",
            &HashMap::new(),
        )
        .unwrap();
    for batch in 0..16 {
        let mut params = HashMap::from([("body".into(), Value::Text("p".repeat(128 * 1024)))]);
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
    assert_native_paged_base(&path);
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                memory_limit: Some(64 * 1024 * 1024),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    eprintln!(
        "native reopened info={:?}, generations={:?}, paged_storage={}",
        database
            .vector_store_for_test()
            .partition_info(&route.index, &route.partition_key),
        database
            .vector_store_for_test()
            .partition_graph_generation_status(&route),
        database
            .vector_store_for_test()
            .partition_has_paged_storage(&route)
    );
    let snapshot = database.snapshot();
    let (_, old_work) = observe_target("paged_items", || {
        database
            .execute("SELECT id FROM paged_items WHERE id = 2", &HashMap::new())
            .unwrap()
    });
    let old_root = old_work
        .physical_sources
        .iter()
        .flatten()
        .find_map(|(family, identity)| (*family == PhysicalFamily::Row).then_some(*identity))
        .unwrap();
    let (reached, captured) = mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let registered = Arc::new(AtomicU64::new(0));
    let pause = Arc::new(PagedStatementPause {
        pause: StatementPause {
            reached,
            release: release.clone(),
            entered: AtomicBool::new(false),
        },
        registered: registered.clone(),
    });
    let (opened, work) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let mut request = bounded::BoundedReadRequest::new(
                "SELECT id, embedding FROM paged_items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
                HashMap::from([("q".into(),Value::Vector(vector(1)))]),
                ReadLimits { cursor_page_rows: 1, result_rows: 3, work: 100_000, active_ms: 1000,
                    memory: 64 * 1024 * 1024, ..Default::default() },
                Arc::new(ManualDeadlineClock::at(0)));
            request.probe = Some(pause);
            observe_target("paged_items", || bounded::open_cursor(database.clone(), &request))
        });
        captured.recv().unwrap();
        let release_reader = StatementRelease(release);
        let tx = database.begin_or_panic();
        for id in 1..=3 {
            database
                .execute_in_tx(
                    tx,
                    "UPDATE paged_items SET embedding = $v WHERE id = $id",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vec![0.1, 0.2, 0.3])),
                    ]),
                )
                .unwrap();
        }
        database.commit(tx).unwrap();
        assert!(
            database.snapshot() > snapshot,
            "successor publishes before the first page read"
        );
        database.compact_currency_versions().unwrap();
        let before = database.__writer_cache_stats_for_test().unwrap();
        drop(
            database
                .__hold_row_page_for_test("displacement", 1023)
                .unwrap(),
        );
        let after = database.__writer_cache_stats_for_test().unwrap();
        assert!(
            after.1 > before.1 && after.2 > before.2,
            "paused reader must survive actual displacement: before={before:?}, after={after:?}"
        );
        assert!(
            database
                .__native_read_transaction_count_for_test(old_root.transaction_id)
                .unwrap()
                > 0,
            "the paused paged reader retains its original physical transaction"
        );
        drop(release_reader);
        reader.join().unwrap()
    });
    let mut opened = opened.unwrap();
    assert_eq!(
        opened.first_page.rows,
        vec![vec![Value::Int64(2), Value::Vector(vec![0.0, 1.0, 0.0])]],
        "the first paged answer retains its captured row and vector after successor publication"
    );
    eprintln!("paged captured-root work old={old_root:?} actual={work:?}");
    assert_eq!(registered.load(Ordering::SeqCst), snapshot.0);
    for (_, root) in work.physical_sources.iter().flatten() {
        assert_eq!(
            root.transaction_id, old_root.transaction_id,
            "row and vector pages use the same captured physical transaction, never a fresh successor root"
        );
    }
    for family in [
        PhysicalFamily::Row,
        PhysicalFamily::Vector,
        PhysicalFamily::Graph,
    ] {
        assert!(
            work.physical_sources
                .iter()
                .flatten()
                .any(|(observed, _)| *observed == family),
            "the paged reader exercises its actual {family:?} source: {work:?}"
        );
    }
    opened.cursor.close().unwrap();
    assert_eq!(
        database
            .__native_read_transaction_count_for_test(old_root.transaction_id)
            .unwrap(),
        0,
        "release drops the original native root"
    );
    database.compact_currency_versions().unwrap();
    database.close().unwrap();
}
