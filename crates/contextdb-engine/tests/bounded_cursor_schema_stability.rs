//! Schema stability of a retained bounded read cursor.
//!
//! A bounded cursor that survives between requests keeps a vector schema read
//! guard for the whole time it is retained, so the index it is reading cannot
//! change shape underneath it: vector DDL naming that index must wait until
//! every live read guard is released.  A retained cursor is owned state that
//! moves between service threads, so releasing one on a thread other than the
//! thread that opened it must leave the protection intact for both threads.
//!
//! Every read below is opened through the production bounded-cursor entrance
//! and every schema change is issued as real SQL through `Database::execute`.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

/// How long a schema change is watched before it counts as waiting on a live
/// read guard.  A change that is not actually gated returns far sooner than
/// this, so the window decides only how long the passing direction costs.
const HELD_OBSERVATION: Duration = Duration::from_millis(1_500);

/// How long a schema change may take once the last read guard is gone.  This
/// bound only distinguishes "released" from "never released"; a released gate
/// admits the waiting writer immediately.
const RELEASED_OBSERVATION: Duration = Duration::from_secs(30);

/// Cursor expiry is driven by the injected clock, never by wall time, so a
/// cursor retained across a cross-thread handoff cannot expire mid-journey.
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
        // One row per page keeps the cursor open past its first page, which is
        // what makes it retain its schema read guard.
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

/// Open a cursor over the table's vector index and leave it retained, holding
/// its schema read guard.
fn retained_vector_cursor(db: Arc<Database>, table: &str) -> bounded::TestCursor {
    let request = bounded::BoundedReadRequest::new(
        format!("SELECT id FROM {table} ORDER BY embedding <=> $query LIMIT 4"),
        HashMap::from([("query".to_owned(), Value::Vector(unit_vector(0)))]),
        roomy_limits(),
        Arc::new(FrozenClock),
    );
    let opened = bounded::open_cursor(db, &request).expect("open a vector cursor");
    assert!(
        opened.first_page.has_more,
        "a vector cursor must stay open past its first page so it keeps holding \
         its schema read guard"
    );
    opened.cursor
}

/// Open a cursor over the same table that reads no vector column, so it
/// retains a snapshot and execution state but no vector schema read guard.
fn retained_relational_cursor(db: Arc<Database>, table: &str) -> bounded::TestCursor {
    let request = bounded::BoundedReadRequest::new(
        format!("SELECT id FROM {table} ORDER BY id"),
        HashMap::new(),
        roomy_limits(),
        Arc::new(FrozenClock),
    );
    let opened = bounded::open_cursor(db, &request).expect("open a relational cursor");
    assert!(
        opened.first_page.has_more,
        "the comparison cursor must stay open so it retains its execution state"
    );
    opened.cursor
}

struct SchemaChange {
    entered: mpsc::Receiver<()>,
    finished: mpsc::Receiver<Result<(), String>>,
    worker: thread::JoinHandle<()>,
}

/// Issue a vector-column rename on its own thread.  Renaming the column is a
/// real schema change to the index a reader is holding.
fn rename_vector_column(db: Arc<Database>, table: &str) -> SchemaChange {
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

    fn assert_waiting(&self, promise: &str) {
        match self.finished.recv_timeout(HELD_OBSERVATION) {
            Ok(outcome) => panic!(
                "{promise}: vector DDL completed ({outcome:?}) while a vector schema read \
                 guard was held, so the retained cursor's index can change shape underneath it"
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
            Ok(Err(error)) => {
                panic!(
                    "{promise}: vector DDL failed after the last read guard was released: {error}"
                )
            }
            Err(error) => panic!(
                "{promise}: vector DDL never completed after the last read guard was \
                 released ({error:?})"
            ),
        }
        self.worker
            .join()
            .expect("the schema-change thread finishes without panicking");
    }
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_owned()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "panic with a non-text payload".to_owned()
    }
}

/// Releasing a cursor reports whether the release itself panicked, so a
/// release that corrupts the releasing thread's bookkeeping is observable
/// without ending the journey it belongs to.
fn release_reporting_panics(cursor: bounded::TestCursor) -> Option<String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(cursor)))
        .err()
        .map(|payload| panic_text(&*payload))
}

/// The instrument: a retained vector cursor makes a vector schema change wait,
/// and releasing the cursor admits it.
#[test]
fn a_retained_vector_cursor_holds_vector_ddl_until_it_is_released() {
    let db = vector_fixture("gated_docs");
    let mut cursor = retained_vector_cursor(Arc::clone(&db), "gated_docs");

    let change = rename_vector_column(Arc::clone(&db), "gated_docs");
    change.wait_until_issued();
    change.assert_waiting("a retained vector cursor keeps its index schema-stable");

    cursor
        .close()
        .expect("closing a cursor releases its retained schema read guard");
    change.assert_admitted("releasing the last read guard admits the schema change");
}

/// The instrument discriminates: retaining cursor state alone does not hold
/// vector DDL, so the wait above comes from the vector schema read guard and
/// not from cursor retention in general.
#[test]
fn a_retained_cursor_without_a_vector_read_does_not_hold_vector_ddl() {
    let db = vector_fixture("ungated_docs");
    let mut cursor = retained_relational_cursor(Arc::clone(&db), "ungated_docs");

    let change = rename_vector_column(Arc::clone(&db), "ungated_docs");
    change.wait_until_issued();
    change.assert_admitted(
        "a retained cursor that reads no vector column leaves the vector index unclaimed",
    );

    cursor
        .close()
        .expect("closing the comparison cursor releases its retained state");
}

/// A retained cursor released on a thread that did not open it must leave the
/// opening thread's protection intact: a guard taken there afterwards still
/// holds the index against vector DDL.
#[test]
fn vector_ddl_waits_for_a_guard_taken_after_a_cursor_was_released_on_another_thread() {
    let db = vector_fixture("moved_docs");
    // Opened here, so the claim on the index is recorded against this thread.
    let moved = retained_vector_cursor(Arc::clone(&db), "moved_docs");

    let (cursor_tx, cursor_rx) = mpsc::channel::<bounded::TestCursor>();
    let (report_tx, report_rx) = mpsc::channel::<Option<String>>();
    let receiver = thread::spawn(move || {
        let cursor = cursor_rx
            .recv()
            .expect("the receiving thread takes ownership of the retained cursor");
        let report = release_reporting_panics(cursor);
        let _ = report_tx.send(report);
    });
    cursor_tx
        .send(moved)
        .expect("a retained cursor moves between service threads");
    let release_report = report_rx
        .recv()
        .expect("the receiving thread reports how the release went");
    receiver
        .join()
        .expect("the receiving thread survives releasing a cursor it did not open");

    // The moved cursor is gone; a cursor opened here now must genuinely own
    // the index again.
    let mut reacquired = retained_vector_cursor(Arc::clone(&db), "moved_docs");
    let change = rename_vector_column(Arc::clone(&db), "moved_docs");
    change.wait_until_issued();
    change.assert_waiting(
        "a cursor opened after an earlier cursor was released on another thread still \
         keeps its index schema-stable",
    );

    reacquired
        .close()
        .expect("closing the reacquired cursor releases its schema read guard");
    change.assert_admitted("releasing the reacquired guard admits the schema change");

    assert!(
        release_report.is_none(),
        "releasing a retained cursor on a thread that did not open it must not disturb \
         that thread's record of which indexes it holds; the release panicked: {release_report:?}"
    );
}

/// The thread that received and released a foreign cursor still holds its own
/// index claims for real afterwards.
#[test]
fn a_thread_that_released_a_foreign_cursor_still_holds_its_own_guard() {
    let db = vector_fixture("handoff_docs");
    let moved = retained_vector_cursor(Arc::clone(&db), "handoff_docs");

    let (cursor_tx, cursor_rx) = mpsc::channel::<bounded::TestCursor>();
    let (ready_tx, ready_rx) = mpsc::channel::<Option<String>>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let worker_db = Arc::clone(&db);
    let worker = thread::spawn(move || {
        let foreign = cursor_rx
            .recv()
            .expect("the receiving thread takes ownership of the retained cursor");
        let report = release_reporting_panics(foreign);
        let mut own = retained_vector_cursor(worker_db, "handoff_docs");
        ready_tx
            .send(report)
            .expect("the receiving thread announces its own retained cursor");
        release_rx
            .recv()
            .expect("the receiving thread is told when to release its own cursor");
        own.close()
            .expect("closing releases the receiving thread's own schema read guard");
    });

    cursor_tx
        .send(moved)
        .expect("a retained cursor moves between service threads");
    let release_report = ready_rx
        .recv()
        .expect("the receiving thread reports before holding its own cursor");

    let change = rename_vector_column(Arc::clone(&db), "handoff_docs");
    change.wait_until_issued();
    change.assert_waiting(
        "a cursor opened by the thread that released a foreign cursor keeps its index \
         schema-stable",
    );

    release_tx
        .send(())
        .expect("the receiving thread is still waiting to release");
    change.assert_admitted("releasing the receiving thread's own guard admits the schema change");
    worker
        .join()
        .expect("the receiving thread finishes without panicking");

    assert!(
        release_report.is_none(),
        "releasing a foreign cursor must not disturb the receiving thread's record of \
         which indexes it holds; the release panicked: {release_report:?}"
    );
}
