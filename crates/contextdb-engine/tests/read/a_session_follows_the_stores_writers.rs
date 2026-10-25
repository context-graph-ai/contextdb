//! A read session follows the store's writers: after its writer closes it
//! reads the committed file, and once a new writer opens and commits, its
//! next statement sees those commits, through the new owner or a fresh file
//! image, instead of staying on the snapshot it read while no writer was
//! open. A statement that arrives while a new writer is still claiming the
//! store waits for that writer's decision and is answered, never sent down
//! the closed writer's channel. A cursor continued into the file after its
//! owner exits reads under the session's own declared memory, keeping every
//! other limit it negotiated with the owner.

use crate::continuation_support::continued_cursor_file_limits;
use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadLimits, ReadLimits, ReadRoute};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::persistence::read_persistence_test_scaffold::observe_writer_reader_snapshots_for_test;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};

const SELECT: &str = "SELECT id FROM items ORDER BY id";

fn ids(ids: &[i64]) -> Vec<Vec<Value>> {
    ids.iter().map(|id| vec![Value::Int64(*id)]).collect()
}

fn insert(database: &Database, id: i64) {
    database
        .execute(
            "INSERT INTO items (id) VALUES ($id)",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .expect("commit a row");
}

/// The first writer with rows 1..=3 and a session opened beside it.
fn session_beside_a_writer(path: &Path) -> (Database, ReadSession) {
    let writer = Database::open(path).expect("open the first writer");
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 1..=3 {
        insert(&writer, id);
    }
    let session = ReadSession::open(path).expect("open a session beside the writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    (writer, session)
}

fn rows(session: &ReadSession, when: &str) -> Vec<Vec<Value>> {
    session
        .execute(SELECT, &HashMap::new())
        .unwrap_or_else(|refusal| panic!("{when}, the session must be answered: {refusal}"))
        .rows()
        .to_vec()
}

#[test]
fn a_session_reading_the_file_after_its_writer_closed_sees_a_new_writers_commits() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let (first, session) = session_beside_a_writer(&path);
    assert_eq!(rows(&session, "beside the first writer"), ids(&[1, 2, 3]));

    first.close().expect("close the first writer");
    assert_eq!(
        rows(&session, "with no writer open"),
        ids(&[1, 2, 3]),
        "with no writer open the session reads the committed file"
    );

    let second = Database::open(&path).expect("open a new writer");
    insert(&second, 4);
    assert_eq!(
        rows(&session, "after a new writer committed a row"),
        ids(&[1, 2, 3, 4]),
        "a session that read the file while no writer was open must see a later writer's commits"
    );
    insert(&second, 5);
    assert_eq!(
        rows(&session, "after that writer committed another row"),
        ids(&[1, 2, 3, 4, 5])
    );
    drop(session);
    second.close().expect("close the new writer");
}

enum Progress {
    /// The statement is waiting for the claiming writer's decision.
    WaitingForWriter,
    /// The statement finished, answered or refused.
    Finished(Result<Vec<Vec<Value>>, String>),
}

/// Reports the first claim-window wait.
struct ClaimWaitReport(Mutex<Option<mpsc::Sender<Progress>>>);

impl ReadSessionTestObserver for ClaimWaitReport {
    fn observe_event(&self, event: ReadSessionEvent) {
        if matches!(event, ReadSessionEvent::ClaimWindowWait { .. })
            && let Some(waiting) = self.0.lock().expect("claim wait sender").take()
        {
            let _ = waiting.send(Progress::WaitingForWriter);
        }
    }
}

#[test]
fn a_statement_arriving_while_a_new_writer_is_still_claiming_the_store_waits_for_it_and_is_answered()
 {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let first = Database::open(&path).expect("open the first writer");
    first
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 1..=3 {
        insert(&first, id);
    }
    let (progress, progress_rx) = mpsc::channel();
    // The session's clock never moves, so its routing allowance cannot run
    // out: only the claiming writer's decision can end the wait.
    let session = ReadSession::open_with_observations_for_test(
        &path,
        ReadSessionOptions::default(),
        Arc::new(ManualDeadlineClock::at(0)),
        Some(Arc::new(ClaimWaitReport(Mutex::new(Some(
            progress.clone(),
        ))))),
        None,
    )
    .expect("open a session beside the first writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    assert_eq!(rows(&session, "beside the first writer"), ids(&[1, 2, 3]));
    first.close().expect("close the first writer");

    let answer = std::thread::scope(|scope| {
        // The new writer is held inside its open: it holds the store and its
        // claim window, and has published nothing a reader can dial. Dropping
        // `resume` (including by a panic below) lets it finish.
        let (reached, reached_rx) = mpsc::channel::<()>();
        let (resume, resume_rx) = mpsc::channel::<()>();
        let reached = Mutex::new(reached);
        let resume_rx = Mutex::new(resume_rx);
        let opening_path = path.clone();
        let opening = scope.spawn(move || {
            observe_writer_reader_snapshots_for_test(
                Arc::new(move |_registry| {
                    let _ = reached.lock().expect("writer reached sender").send(());
                    let _ = resume_rx.lock().expect("writer resume receiver").recv();
                }),
                || Database::open(&opening_path),
            )
        });
        reached_rx
            .recv()
            .expect("the new writer is claiming the store");

        let finished = progress;
        let reading = &session;
        scope.spawn(move || {
            let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reading
                    .execute(SELECT, &HashMap::new())
                    .map(|result| result.rows().to_vec())
                    .map_err(|refusal| refusal.to_string())
            }))
            .unwrap_or_else(|_| Err("the statement panicked".to_owned()));
            let _ = finished.send(Progress::Finished(answer));
        });
        match progress_rx.recv().expect("the reader reports its progress") {
            Progress::WaitingForWriter => {}
            Progress::Finished(answer) => panic!(
                "a statement arriving while a new writer claims the store must wait for that \
                 writer's decision; it answered first: {answer:?}"
            ),
        }
        drop(resume);
        let answer = match progress_rx.recv().expect("the reader reports its answer") {
            Progress::Finished(answer) => answer,
            Progress::WaitingForWriter => unreachable!("the wait is reported once"),
        };
        let second = opening
            .join()
            .expect("writer thread")
            .expect("the new writer opens once released");
        drop(second);
        answer
    });
    let rows = answer.unwrap_or_else(|refusal| {
        panic!(
            "a statement that waited for a new writer's claim must be answered, never sent down \
             the closed writer's channel: {refusal}"
        )
    });
    assert_eq!(rows, ids(&[1, 2, 3]));
}

#[test]
fn a_cursor_continued_after_its_owner_exits_reads_under_the_sessions_declared_memory() {
    const MIB: u64 = 1024 * 1024;
    let owner_limits = ReadLimits {
        memory: 8 * MIB,
        result_rows: 1_000,
        cursor_page_rows: 2,
        ..ReadLimits::default()
    };
    let session_limits = ReadLimits {
        memory: 64 * MIB,
        result_rows: 500,
        cursor_page_rows: 2,
        ..ReadLimits::default()
    };
    let negotiated = ReadLimits::stricter_of(session_limits, owner_limits)
        .expect("the session's request and the owner's maximum intersect");
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let writer = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                limits: OwnerReadLimits {
                    limits: owner_limits,
                    ..OwnerReadLimits::default()
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the writer");
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 1..=6 {
        insert(&writer, id);
    }
    let session = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            memory_limit: Some((64 * MIB) as usize),
            limits: session_limits,
            ..ReadSessionOptions::default()
        },
    )
    .expect("open a session beside the writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    let mut cursor = session
        .open_cursor(SELECT, &HashMap::new())
        .expect("open a cursor through the owner");
    assert_eq!(cursor.first_page().rows(), ids(&[1, 2]).as_slice());
    assert!(cursor.first_page().has_more());

    writer.close().expect("the owner exits");
    let page = cursor
        .fetch(None)
        .expect("the cursor continues into the file after its owner exits");
    assert_eq!(page.rows(), ids(&[3, 4]).as_slice());

    let (memory_limit, limits) = continued_cursor_file_limits(&cursor);
    let declared = 64 * MIB;
    assert!(
        limits.memory > owner_limits.memory && limits.memory <= declared,
        "once the owner has exited, the continued cursor's memory comes from the session's \
         declared {declared} bytes, not the departed owner's {} bytes: it reads under {} bytes",
        owner_limits.memory,
        limits.memory
    );
    let memory_limit = memory_limit.expect("the continued file read carries a memory ceiling");
    assert!(
        memory_limit as u64 > owner_limits.memory && memory_limit as u64 <= declared,
        "the continued file read's memory ceiling comes from the session's declaration: \
         {memory_limit} bytes"
    );
    assert_eq!(
        (
            limits.result_rows,
            limits.result_bytes,
            limits.work,
            limits.active_ms,
            limits.cursor_page_bytes,
            limits.cursor_idle_ms,
            limits.cursor_lifetime_ms,
        ),
        (
            negotiated.result_rows,
            negotiated.result_bytes,
            negotiated.work,
            negotiated.active_ms,
            negotiated.cursor_page_bytes,
            negotiated.cursor_idle_ms,
            negotiated.cursor_lifetime_ms,
        ),
        "every limit other than memory stays as negotiated with the owner"
    );
    let mut seen = page.rows().to_vec();
    let mut more = page.has_more();
    while more {
        let page = cursor.fetch(None).expect("the continued cursor reads on");
        seen.extend(page.rows().iter().cloned());
        more = page.has_more();
    }
    assert_eq!(seen, ids(&[3, 4, 5, 6]));
}
