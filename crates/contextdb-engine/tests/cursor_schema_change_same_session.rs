//! A session that changes vector schema while its own cursor is open.
//!
//! A retained bounded cursor holds a vector schema read guard so the index it
//! reads cannot change shape underneath it.  That protection is aimed at other
//! sessions.  The session that opened the cursor is a different case: its own
//! schema change can never be admitted while it is still holding the cursor,
//! so the only honest answer is a refusal that names the open cursor.  Waiting
//! is not an answer — the session is waiting for itself, and the wait can only
//! end when the session runs a statement it can no longer reach.
//!
//! The cursor is opened through the production bounded-cursor entrance and the
//! schema change is issued as real SQL through `Database::execute`, both on one
//! session thread.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

/// How long the session thread is watched before its schema change counts as
/// never returning.  This is deadlock detection, not synchronization: a
/// session that answers — with a refusal or with success — answers far sooner,
/// and the window only decides how long the failing direction costs.
const SELF_BLOCK_WATCH: Duration = Duration::from_secs(30);

/// Cursor expiry is driven by the injected clock, never by wall time, so a
/// retained cursor cannot expire while its own session is mid-statement.
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

fn rename_statement(table: &str) -> String {
    format!("ALTER TABLE {table} RENAME COLUMN embedding TO embedding_v2")
}

/// A session holding its own open cursor is refused when it changes the vector
/// schema that cursor reads, and the refusal says the open cursor is why.
#[test]
fn a_session_changing_vector_schema_under_its_own_open_cursor_is_refused_not_blocked() {
    let db = vector_fixture("own_cursor_docs");
    let (answered_tx, answered) = mpsc::channel::<Result<(), String>>();
    // The session parks its own cursor where this thread can reach it.  Nothing
    // in the contract needs that; it is how a session left waiting on itself is
    // freed afterwards, so the failing direction reports an unanswered
    // statement instead of leaving a stuck process behind.
    let parked: Arc<Mutex<Option<bounded::TestCursor>>> = Arc::new(Mutex::new(None));
    let session_db = Arc::clone(&db);
    let session_parked = Arc::clone(&parked);
    let session = thread::spawn(move || {
        let own_cursor = retained_vector_cursor(Arc::clone(&session_db), "own_cursor_docs");
        *session_parked
            .lock()
            .expect("park the session's own cursor") = Some(own_cursor);
        let outcome = session_db
            .execute(&rename_statement("own_cursor_docs"), &HashMap::new())
            .map(|_| ())
            .map_err(|error| error.to_string());
        let _ = answered_tx.send(outcome);
        drop(
            session_parked
                .lock()
                .expect("reclaim the session's own cursor")
                .take(),
        );
    });

    let outcome = match answered.recv_timeout(SELF_BLOCK_WATCH) {
        Ok(outcome) => outcome,
        Err(RecvTimeoutError::Timeout) => {
            drop(parked.lock().expect("free the waiting session").take());
            let _ = session.join();
            panic!(
                "a session that changes vector schema while its own cursor is open must be \
                 answered, not made to wait for itself: the statement produced no answer \
                 within {SELF_BLOCK_WATCH:?}, and only closing the cursor from outside the \
                 session let it finish"
            );
        }
        Err(RecvTimeoutError::Disconnected) => {
            panic!("the session thread ended without reporting what its own schema change did")
        }
    };
    session
        .join()
        .expect("the session thread finishes without panicking");

    let refusal = outcome.expect_err(
        "a schema change issued under the session's own open cursor must be refused; \
         admitting it would change the index shape the live cursor is reading",
    );
    let refusal_text = refusal.to_lowercase();
    assert!(
        refusal_text.contains("cursor"),
        "the refusal must name the open cursor that stands in the way, so the session \
         knows to close it; got {refusal:?}"
    );
}

/// The refusal is about the session's own live cursor, not a standing ban: the
/// same session runs the same schema change once its cursor is closed.
#[test]
fn a_session_may_change_vector_schema_once_its_own_cursor_is_closed() {
    let db = vector_fixture("closed_cursor_docs");
    let mut own_cursor = retained_vector_cursor(Arc::clone(&db), "closed_cursor_docs");
    own_cursor
        .close()
        .expect("closing releases the session's retained schema read guard");

    db.execute(&rename_statement("closed_cursor_docs"), &HashMap::new())
        .expect("a session with no open cursor changes its own vector schema");
}
