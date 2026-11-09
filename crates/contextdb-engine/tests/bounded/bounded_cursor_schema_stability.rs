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

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

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
        opened.first_page.has_more(),
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
