//! What a cursor's replay costs is the shape of the page it is building.
//!
//! A cursor carries the rows a page pulled but did not publish into the next
//! page. That carrier belongs to the page: it is sized by the rows this fetch
//! asked for, taken and given back with the rest of the page's scaffolding.
//! Sizing it by the largest result the limits would ever admit charges every
//! cursor for a result nobody asked for, so raising the result-row ceiling on
//! a store holding a handful of rows would refuse the read for memory.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Every refusal here is decided by memory, never by elapsed time.
#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These reads are synchronous; the immediately-completing future
        // satisfies the shared transport-facing clock trait.
        Box::pin(async {})
    }
}

/// A store small enough that no declared ceiling is anywhere near it, so a
/// refusal can only come from what the engine charged for on its own account.
const FIXTURE_ROWS: i64 = 7;

fn seeded_store() -> Arc<Database> {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE replay_rows (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the replay fixture table");
    for id in 0..FIXTURE_ROWS {
        db.execute(
            "INSERT INTO replay_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("payload".to_owned(), Value::Text(format!("replay-{id}"))),
            ]),
        )
        .expect("insert a replay fixture row");
    }
    Arc::new(db)
}

/// The shipped memory ceiling, with a result-row ceiling raised far past
/// anything this store holds. A caller is entitled to raise the row ceiling
/// without also raising the memory ceiling.
fn wide_row_ceiling_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 4_000_000,
        result_bytes: ReadLimits::SHIPPED_RESULT_BYTES,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: ReadLimits::SHIPPED_MEMORY,
        cursor_page_rows: 100,
        cursor_page_bytes: ReadLimits::SHIPPED_CURSOR_PAGE_BYTES,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn request(limits: ReadLimits) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        "SELECT id, payload FROM replay_rows ORDER BY id",
        HashMap::new(),
        limits,
        Arc::new(FrozenClock),
    )
}

fn ids(page: &contextdb_core::read_contract::CursorPage) -> Vec<i64> {
    page.rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id), Value::Text(_)] => *id,
            other => panic!("replay page projection returned {other:?}"),
        })
        .collect()
}

#[test]
fn a_cursor_under_a_wide_row_ceiling_answers_the_rows_the_store_holds() {
    let db = seeded_store();
    let request = request(wide_row_ceiling_limits());
    let opened = bounded::open_cursor(Arc::clone(&db), &request)
        .expect("a wide result-row ceiling does not charge the cursor for rows nobody asked for");
    let mut answered = opened.first_page.rows.len();
    let mut cursor = opened.cursor;
    let mut has_more = opened.first_page.has_more;
    while has_more {
        let fetched = cursor
            .fetch(None, OwnerReadCancellation::new())
            .expect("the cursor keeps answering under the wide row ceiling");
        answered = answered.saturating_add(fetched.page.rows.len());
        has_more = fetched.page.has_more;
    }
    assert_eq!(
        i64::try_from(answered).expect("the fixture row count fits this platform"),
        FIXTURE_ROWS
    );
    cursor.close().expect("close the wide-ceiling cursor");
}

#[test]
fn an_explicit_fetch_equal_to_the_result_row_ceiling_still_succeeds() {
    let db = seeded_store();
    let limits = ReadLimits::default();
    let request = request(limits);
    let opened =
        bounded::open_cursor(Arc::clone(&db), &request).expect("open a cursor under the defaults");
    let mut cursor = opened.cursor;
    let exact = usize::try_from(limits.result_rows).expect("the declared row ceiling fits usize");
    let fetched = cursor
        .fetch(
            Some(NonZeroUsize::new(exact).expect("the declared row ceiling is non-zero")),
            OwnerReadCancellation::new(),
        )
        .expect("an explicit fetch equal to result_rows succeeds");
    assert!(!fetched.page.has_more);
    cursor.close().expect("close the exact-fetch cursor");
}

#[test]
fn a_single_row_page_cursor_still_carries_its_unpublished_rows_between_pages() {
    let db = seeded_store();
    let mut limits = wide_row_ceiling_limits();
    // One row per page, so the fixture is drained over several equally-shaped
    // pages and the replay carries the unpublished rows between them.
    limits.cursor_page_rows = 1;
    let request = request(limits);
    let opened = bounded::open_cursor(Arc::clone(&db), &request)
        .expect("open a single-row-page cursor under the wide row ceiling");
    let mut cursor = opened.cursor;
    let mut answered = ids(&opened.first_page);
    let mut has_more = opened.first_page.has_more;
    while has_more {
        let fetched = cursor
            .fetch(None, OwnerReadCancellation::new())
            .expect("the single-row-page cursor keeps answering");
        assert!(fetched.page.rows.len() <= 1);
        answered.extend(ids(&fetched.page));
        has_more = fetched.page.has_more;
    }
    assert_eq!(answered, (0..FIXTURE_ROWS).collect::<Vec<_>>());
    cursor.close().expect("close the single-row-page cursor");
}
