//! Acceptance: "Existing readers must finish on their original view through writer acquisition,
//! commits and maintenance, without retry, false corruption, newer-state substitution or blocking
//! the writer for the session lifetime. New readers see complete committed state."

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey, Wallclock};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commit};
use contextdb_engine::writer_query_test_seams::observe as observe_query;
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

#[test]
fn writer_reader_root_survives_commit_eviction_and_cleanup() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reader-roots.redb");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    database.execute("CREATE TABLE reader_items (id INTEGER PRIMARY KEY, unique_key TEXT UNIQUE, embedding VECTOR(3) SEARCH_MODE EXACT) HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST", &HashMap::new()).unwrap();
    let old_vectors = [
        vec![1.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, 0.0, 1.0],
    ];
    for (offset, vector) in old_vectors.iter().enumerate() {
        database
            .execute(
                "INSERT INTO reader_items VALUES ($id, $key, $vector)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(offset as i64 + 1)),
                    ("key".to_owned(), Value::Text(format!("old-{}", offset + 1))),
                    ("vector".to_owned(), Value::Vector(vector.clone())),
                ]),
            )
            .unwrap();
    }
    database.close().unwrap();
    drop(database);
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let old_snapshot = database.snapshot();
    let pinned = database.pin_snapshot(old_snapshot);
    let transaction = database.begin_or_panic();
    let sql = "SELECT id, unique_key, embedding FROM reader_items ORDER BY id";
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
    assert_eq!(
        database
            .execute_in_tx(transaction, sql, &HashMap::new())
            .unwrap()
            .rows,
        expected_old
    );
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
    let mut opened = bounded::open_cursor(database.clone(), &request).unwrap();
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
                if point == CommitBoundary::BeforeMemoryPublication {
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
        let evicted = database
            .vector_store_for_test()
            .evict_raw_partition_for_test(
                &VectorIndexRef::new("reader_items", "embedding"),
                &VectorPartitionKey::default(),
            );
        let cleanup = database.compact_currency_versions();
        statement_release.wait();
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
        database.execute(sql, &HashMap::new()).unwrap().rows,
        expected_new,
        "a newer reader sees the complete successor"
    );
    assert_eq!(
        database
            .execute_at_snapshot(sql, &HashMap::new(), old_snapshot)
            .unwrap()
            .rows,
        expected_old
    );
    assert_eq!(
        database
            .execute_in_tx(transaction, sql, &HashMap::new())
            .unwrap()
            .rows,
        expected_old
    );
    let mut continued = opened.first_page.rows;
    for _ in 1..expected_old.len() {
        continued.extend(
            opened
                .cursor
                .fetch(None, Default::default())
                .unwrap()
                .page
                .rows,
        );
    }
    assert_eq!(
        continued, expected_old,
        "the bounded continuation reloads its original row/key/vector view"
    );
    opened.cursor.close().unwrap();
    database.rollback(transaction).unwrap();
    drop(pinned);
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
    database.close().unwrap();
}
