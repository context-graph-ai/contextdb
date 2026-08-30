//! Fairness between open inspection cursors and a schema change waiting on them.
//!
//! A schema change on a table waits for the inspection cursors already open
//! over it and then runs; inspection work that begins after the schema change
//! is waiting queues behind it.  So a session that already holds one open
//! cursor over a table cannot keep the change waiting forever by opening
//! further cursors over the same table: once the change is waiting, the next
//! cursor either queues behind it or is refused with an answer naming the
//! cursor the session must close.  Admitting that next cursor ahead of the
//! waiting change is what turns "waits and then runs" into "never runs".
//!
//! Every cursor below is opened through the production bounded-cursor entrance
//! and every schema change is issued as real SQL through `Database::execute`.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

/// How long a schema change is watched before it counts as waiting on the
/// cursors already open over its table.  A change that is not gated at all
/// returns far sooner than this, so the window decides only how long the
/// passing direction costs.
const HELD_OBSERVATION: Duration = Duration::from_millis(1_500);

/// How long a cursor opened while a schema change is waiting is watched before
/// it counts as queued behind that change.  A cursor admitted ahead of the
/// change answers far sooner than this.
const ADMISSION_OBSERVATION: Duration = Duration::from_millis(1_500);

/// How long anything is watched once the last cursor standing in the way is
/// released.  This bound only separates "released" from "never released", and
/// is deadlock detection rather than synchronization: a released gate admits
/// its waiters immediately.
const RELEASED_OBSERVATION: Duration = Duration::from_secs(30);

/// Cursor expiry is driven by the injected clock, never by wall time, so a
/// cursor held open across another session's schema change cannot expire and
/// let that change through by accident.
#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These cursor journeys are synchronous; the immediately-completing
        // future satisfies the shared transport-facing clock trait.
        Box::pin(async {})
    }
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        // One row per page keeps a cursor open past its first page, which is
        // what makes it a live inspection holding its schema read guard.
        cursor_page_rows: 1,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn unit_vector(seed: u64) -> Vec<f32> {
    let slope = 1.0 / (seed.saturating_add(1) as f32);
    let norm = (1.0 + slope * slope).sqrt();
    vec![1.0 / norm, slope / norm]
}

fn vector_fixture(table: &str) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute(
        &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, embedding VECTOR(2), payload TEXT)"),
        &HashMap::new(),
    )
    .expect("create the vector table");
    for ordinal in 0..12_u128 {
        db.execute(
            &format!(
                "INSERT INTO {table} (id, embedding, payload) VALUES ($id, $embedding, $payload)"
            ),
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::from_u128(ordinal + 1))),
                (
                    "embedding".to_owned(),
                    Value::Vector(unit_vector(ordinal as u64)),
                ),
                ("payload".to_owned(), Value::Text(format!("row-{ordinal}"))),
            ]),
        )
        .expect("seed a vector row");
    }
    db
}

fn vector_request(table: &str) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        format!("SELECT id FROM {table} ORDER BY embedding <=> $query LIMIT 4"),
        HashMap::from([("query".to_owned(), Value::Vector(unit_vector(0)))]),
        roomy_limits(),
        Arc::new(FrozenClock),
    )
}

/// Open a cursor over the table's vector index and page it once past its
/// opening page, so it is a live inspection in progress rather than a handle
/// that merely exists.
fn live_vector_cursor(db: Arc<Database>, table: &str) -> bounded::TestCursor {
    let opened = bounded::open_cursor(db, &vector_request(table)).expect("open a vector cursor");
    assert!(
        opened.first_page.has_more,
        "an inspection cursor must stay open past its first page so it is still \
         reading when a schema change arrives"
    );
    let mut cursor = opened.cursor;
    let paged = cursor
        .fetch(None, OwnerReadCancellation::new())
        .expect("page the open cursor once");
    assert!(
        !paged.page.rows.is_empty(),
        "paging an open inspection cursor must return rows, so the cursor is \
         demonstrably mid-inspection"
    );
    cursor
}

/// What a cursor opened while a schema change is waiting reported back, and
/// whether that change had already completed at the moment it reported.
struct AdmissionReport {
    opened_rows: Option<usize>,
    refusal: Option<String>,
    engine_error: Option<String>,
    schema_change_had_completed: bool,
}

fn open_and_report(
    db: Arc<Database>,
    table: &str,
    schema_change_completed: &AtomicBool,
) -> AdmissionReport {
    let outcome = bounded::open_cursor(db, &vector_request(table));
    // Stamped the instant the open returns, before anything is released, so
    // the stamp answers "was the schema change still waiting when this cursor
    // was admitted?".
    let schema_change_had_completed = schema_change_completed.load(Ordering::SeqCst);
    match outcome {
        Ok(opened) => AdmissionReport {
            opened_rows: Some(opened.first_page.rows.len()),
            refusal: None,
            engine_error: None,
            schema_change_had_completed,
        },
        Err(bounded::TestError::Refused(failure)) => AdmissionReport {
            opened_rows: None,
            refusal: Some(format!("{failure:?}")),
            engine_error: None,
            schema_change_had_completed,
        },
        Err(other) => AdmissionReport {
            opened_rows: None,
            refusal: None,
            engine_error: Some(format!("{other:?}")),
            schema_change_had_completed,
        },
    }
}

struct SchemaChange {
    entered: mpsc::Receiver<()>,
    finished: mpsc::Receiver<Result<(), String>>,
    worker: thread::JoinHandle<()>,
}

/// Issue a vector-column rename on its own thread.  Renaming the column is a
/// real schema change to the index the open cursors are reading.
fn rename_vector_column(
    db: Arc<Database>,
    table: &str,
    completed: Arc<AtomicBool>,
) -> SchemaChange {
    let sql = format!("ALTER TABLE {table} RENAME COLUMN embedding TO embedding_v2");
    let (entered_tx, entered) = mpsc::channel();
    let (finished_tx, finished) = mpsc::channel();
    let worker = thread::spawn(move || {
        entered_tx
            .send(())
            .expect("the schema-change thread announces itself before its statement");
        let outcome = db
            .execute(&sql, &HashMap::new())
            .map(|_| ())
            .map_err(|error| error.to_string());
        completed.store(true, Ordering::SeqCst);
        let _ = finished_tx.send(outcome);
    });
    SchemaChange {
        entered,
        finished,
        worker,
    }
}

impl SchemaChange {
    fn wait_until_issued(&self) {
        self.entered
            .recv()
            .expect("the schema-change thread reaches its statement");
    }

    /// Prove the change is genuinely waiting on the open cursor rather than
    /// merely started: it produces no answer while that cursor is open.
    fn assert_waiting(&self, promise: &str) {
        match self.finished.recv_timeout(HELD_OBSERVATION) {
            Ok(outcome) => panic!(
                "{promise}: the schema change completed ({outcome:?}) while an inspection \
                 cursor over its table was still open, so it never waited and this journey \
                 cannot say anything about fairness"
            ),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                panic!("{promise}: the schema-change thread ended without reporting an outcome")
            }
        }
    }

    fn assert_admitted(self, promise: &str) {
        match self.finished.recv_timeout(RELEASED_OBSERVATION) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!(
                "{promise}: the schema change failed after the cursors in its way were \
                 released: {error}"
            ),
            Err(error) => panic!(
                "{promise}: the schema change never completed after the cursors in its way \
                 were released ({error:?})"
            ),
        }
        self.worker
            .join()
            .expect("the schema-change thread finishes without panicking");
    }
}

/// A thread already holding one open cursor over a table cannot open a second
/// cursor over that same table ahead of a schema change that is already
/// waiting: the second open either queues behind the change or is refused with
/// an answer naming the cursor to close.
#[test]
fn a_second_cursor_from_a_holding_thread_never_opens_ahead_of_a_waiting_schema_change() {
    const TABLE: &str = "fair_admission_docs";
    let db = vector_fixture(TABLE);
    let completed = Arc::new(AtomicBool::new(false));
    // The reading thread parks its first cursor where this thread can reach
    // it.  Nothing in the contract needs that; it is how a second open that
    // correctly queues behind the change is freed afterwards, so the queueing
    // direction finishes instead of leaving a stuck process behind.
    let parked: Arc<Mutex<Option<bounded::TestCursor>>> = Arc::new(Mutex::new(None));

    let (opened_tx, opened) = mpsc::channel::<()>();
    let (proceed_tx, proceed) = mpsc::channel::<()>();
    let (report_tx, reported) = mpsc::channel::<AdmissionReport>();

    let reader_db = Arc::clone(&db);
    let reader_parked = Arc::clone(&parked);
    let reader_completed = Arc::clone(&completed);
    let reader = thread::spawn(move || {
        let first = live_vector_cursor(Arc::clone(&reader_db), TABLE);
        *reader_parked.lock().expect("park the first cursor") = Some(first);
        opened_tx
            .send(())
            .expect("the reading thread announces its first open cursor");
        proceed
            .recv()
            .expect("the reading thread waits until the schema change is proven waiting");

        let report = open_and_report(Arc::clone(&reader_db), TABLE, &reader_completed);
        let _ = report_tx.send(report);
        drop(
            reader_parked
                .lock()
                .expect("reclaim the first cursor")
                .take(),
        );
    });

    opened
        .recv()
        .expect("the reading thread reports its first open cursor");
    let change = rename_vector_column(Arc::clone(&db), TABLE, Arc::clone(&completed));
    change.wait_until_issued();
    change.assert_waiting(
        "a schema change waits for the inspection cursors already open over its table",
    );
    proceed_tx
        .send(())
        .expect("the reading thread is released to attempt its second cursor");

    let report = match reported.recv_timeout(ADMISSION_OBSERVATION) {
        Ok(report) => report,
        Err(RecvTimeoutError::Timeout) => {
            // The second open is queued behind the waiting change.  Release
            // the first cursor so the change can run, then require the change
            // to have completed before the second open returns.
            drop(parked.lock().expect("release the first cursor").take());
            change.assert_admitted(
                "releasing the cursor the change was waiting for lets that change run",
            );
            let report = reported
                .recv_timeout(RELEASED_OBSERVATION)
                .expect("a queued second open returns once the schema change has run");
            reader
                .join()
                .expect("the reading thread finishes without panicking");
            assert!(
                report.schema_change_had_completed,
                "a second cursor that queued behind the waiting schema change must not \
                 return until that change has run"
            );
            return;
        }
        Err(RecvTimeoutError::Disconnected) => {
            panic!("the reading thread ended without reporting what its second open did")
        }
    };

    if let Some(refusal) = report.refusal.as_ref() {
        assert!(
            refusal.to_lowercase().contains("cursor"),
            "a second open refused while a schema change is waiting must name the open \
             cursor the session has to close; got {refusal}"
        );
        drop(parked.lock().expect("release the first cursor").take());
        change.assert_admitted("releasing the refused session's cursor lets the change run");
        reader
            .join()
            .expect("the reading thread finishes without panicking");
        return;
    }

    if let Some(engine_error) = report.engine_error.as_ref() {
        drop(parked.lock().expect("release the first cursor").take());
        change.assert_admitted("releasing the first cursor lets the change run");
        let _ = reader.join();
        panic!(
            "a second open attempted while a schema change is waiting must either queue \
             behind that change or be refused with an answer naming the open cursor; it \
             failed with an unrelated error instead: {engine_error}"
        );
    }

    let rows = report
        .opened_rows
        .expect("a second open that neither queued nor was refused reports its rows");
    let admitted_ahead = !report.schema_change_had_completed;
    drop(parked.lock().expect("release the first cursor").take());
    change.assert_admitted("releasing the first cursor lets the change run");
    reader
        .join()
        .expect("the reading thread finishes without panicking");
    assert!(
        !admitted_ahead,
        "a schema change already waiting on {TABLE} must not be postponed by inspection \
         that begins after it: the thread holding an open cursor over {TABLE} opened a \
         second cursor over {TABLE} and was handed {rows} row(s) from it while that schema \
         change was still waiting, so a session that keeps opening cursors postpones the \
         change indefinitely"
    );
}

/// The comparison: a cursor opened by a thread holding nothing queues behind
/// the waiting schema change, so the postponement above is about how a holding
/// thread is admitted and not about how cursors are opened in general.
#[test]
fn a_cursor_from_a_thread_holding_nothing_queues_behind_a_waiting_schema_change() {
    const TABLE: &str = "queued_admission_docs";
    let db = vector_fixture(TABLE);
    let completed = Arc::new(AtomicBool::new(false));
    // Held on this thread, so the late opener below is genuinely a thread with
    // no claim of its own on the index.
    let mut held = live_vector_cursor(Arc::clone(&db), TABLE);

    let change = rename_vector_column(Arc::clone(&db), TABLE, Arc::clone(&completed));
    change.wait_until_issued();
    change.assert_waiting(
        "a schema change waits for the inspection cursors already open over its table",
    );

    let (report_tx, reported) = mpsc::channel::<AdmissionReport>();
    let latecomer_db = Arc::clone(&db);
    let latecomer_completed = Arc::clone(&completed);
    let latecomer = thread::spawn(move || {
        let report = open_and_report(latecomer_db, TABLE, &latecomer_completed);
        let _ = report_tx.send(report);
    });

    match reported.recv_timeout(ADMISSION_OBSERVATION) {
        Ok(report) => {
            held.close().expect("close the held cursor");
            change.assert_admitted("releasing the held cursor lets the change run");
            let _ = latecomer.join();
            assert!(
                report.schema_change_had_completed,
                "inspection that begins after a schema change is waiting must queue behind \
                 that change; a thread with no claim of its own was admitted ahead of it"
            );
            return;
        }
        Err(RecvTimeoutError::Timeout) => {}
        Err(RecvTimeoutError::Disconnected) => {
            panic!("the late thread ended without reporting what its open did")
        }
    }

    held.close()
        .expect("closing the held cursor releases what the change is waiting for");
    change.assert_admitted("releasing the held cursor lets the queued change run");
    let report = reported
        .recv_timeout(RELEASED_OBSERVATION)
        .expect("a queued open returns once the schema change has run");
    latecomer
        .join()
        .expect("the late thread finishes without panicking");
    assert!(
        report.schema_change_had_completed,
        "a queued open must not return until the schema change it queued behind has run"
    );
}
