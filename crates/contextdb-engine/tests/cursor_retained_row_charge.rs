//! A cursor is charged for the rows it is still holding.
//!
//! A page that does not publish everything it pulled hands the remainder to
//! the next page, and those rows stay alive in the cursor until some later
//! page takes them. What the cursor is reported to cost has to include them:
//! a reader holding a cursor with a large deferred row uses that memory, and a
//! ceiling or an operator that is told otherwise is being told something
//! untrue. The scaffolding — a page's columns, its row capacity, the replay
//! container — is the flat part and is the only flat part.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;

/// Every assertion here is decided by bytes and rows, never by elapsed time.
#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

/// Rows wide enough that one of them held back is a visible charge rather than
/// a rounding difference against the page scaffolding.
const PAYLOAD_BYTES: usize = 96 * 1024;
const FIXTURE_ROWS: i64 = 12;

fn seeded_store() -> Arc<Database> {
    let db = Database::open_memory();
    db.set_memory_limit(Some(64 * 1024 * 1024))
        .expect("declare a database ceiling the fixture fits inside");
    db.execute(
        "CREATE TABLE wide_rows (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the wide-row fixture table");
    for id in 0..FIXTURE_ROWS {
        db.execute(
            "INSERT INTO wide_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("payload".to_owned(), Value::Text("w".repeat(PAYLOAD_BYTES))),
            ]),
        )
        .expect("insert a wide fixture row");
    }
    Arc::new(db)
}

/// A page ceiling that admits one wide row and refuses a second, so every page
/// but the last leaves a pulled row waiting in the cursor.
fn one_row_per_page_limits() -> ReadLimits {
    let payload = u64::try_from(PAYLOAD_BYTES).expect("the payload width fits this platform");
    ReadLimits {
        result_rows: 4_096,
        result_bytes: payload.saturating_mul(64),
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        // Ask for more rows than a page can carry, so the byte ceiling — not
        // the row count — is what stops the page and defers a pulled row.
        cursor_page_rows: 4,
        cursor_page_bytes: payload.saturating_add(payload / 2),
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn request(limits: ReadLimits) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        "SELECT id, payload FROM wide_rows ORDER BY id",
        HashMap::new(),
        limits,
        Arc::new(FrozenClock),
    )
}

fn used(db: &Database) -> usize {
    db.accountant().usage().used
}

#[test]
fn a_row_the_cursor_defers_is_charged_for_until_a_page_publishes_it() {
    let db = seeded_store();
    let baseline = used(&db);
    let request = request(one_row_per_page_limits());
    let opened = bounded::open_cursor(Arc::clone(&db), &request).expect("open a wide-row cursor");
    assert_eq!(
        opened.first_page.rows.len(),
        1,
        "the byte ceiling admits one wide row"
    );
    assert!(opened.first_page.has_more);

    // The page stopped at the byte ceiling, so the row it had already pulled
    // is waiting inside the cursor. A cursor that only carried scaffolding
    // could not account for a whole payload.
    let holding_a_deferred_row = used(&db);
    assert!(
        holding_a_deferred_row.saturating_sub(baseline) >= PAYLOAD_BYTES,
        "a deferred row must be charged: baseline {baseline}, holding {holding_a_deferred_row}"
    );

    let mut cursor = opened.cursor;
    let mut answered = opened.first_page.rows.len();
    let mut has_more = opened.first_page.has_more;
    while has_more {
        let fetched = cursor
            .fetch(None, OwnerReadCancellation::new())
            .expect("the wide-row cursor keeps answering");
        answered = answered.saturating_add(fetched.page.rows.len());
        has_more = fetched.page.has_more;
    }
    assert_eq!(
        i64::try_from(answered).expect("the fixture row count fits this platform"),
        FIXTURE_ROWS
    );

    cursor.close().expect("close the wide-row cursor");
    assert_eq!(
        used(&db),
        baseline,
        "a drained and closed cursor gives back exactly what it took"
    );
}

#[test]
fn an_interrupted_fetch_keeps_the_rows_the_cursor_is_still_holding_charged() {
    let db = seeded_store();
    let baseline = used(&db);
    let opened = bounded::open_cursor(Arc::clone(&db), &request(one_row_per_page_limits()))
        .expect("open a wide-row cursor");
    let mut cursor = opened.cursor;
    let mut answered = opened.first_page.rows.len();
    assert!(opened.first_page.has_more);

    // A fetch that is already withdrawn when it starts publishes nothing and
    // leaves the cursor exactly as it was: still alive, still holding the row
    // the previous page deferred.
    let withdrawn = OwnerReadCancellation::new();
    withdrawn.cancel();
    let interrupted = cursor.fetch(None, withdrawn);
    assert!(interrupted.is_err(), "a withdrawn fetch publishes no page");
    let holding_after_interruption = used(&db);
    assert!(
        holding_after_interruption.saturating_sub(baseline) >= PAYLOAD_BYTES,
        "an interrupted fetch keeps the deferred row charged: baseline {baseline}, \
         holding {holding_after_interruption}"
    );

    // Resuming loses nothing: the row the interrupted fetch did not publish is
    // still there to be published.
    let mut has_more = true;
    while has_more {
        let fetched = cursor
            .fetch(None, OwnerReadCancellation::new())
            .expect("the resumed cursor keeps answering");
        answered = answered.saturating_add(fetched.page.rows.len());
        has_more = fetched.page.has_more;
    }
    assert_eq!(
        i64::try_from(answered).expect("the fixture row count fits this platform"),
        FIXTURE_ROWS,
        "an interrupted fetch loses no row"
    );

    cursor.close().expect("close the resumed cursor");
    assert_eq!(
        used(&db),
        baseline,
        "a resumed, drained and closed cursor returns exactly to where it started"
    );
}
