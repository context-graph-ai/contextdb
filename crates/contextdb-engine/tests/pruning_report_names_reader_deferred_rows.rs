//! An operator must be able to tell "nothing to reclaim" from "held back for
//! a reader".
//!
//! Retention leaves an expired row physically in place while a registered
//! reader -- an in-flight statement, a held snapshot pin, or a suspended
//! bounded cursor -- can still resolve to it. That is correct, and it is
//! invisible: the cycle reports `pruned_rows: 0`, exactly as a cycle with
//! nothing expired at all reports. An operator watching a store whose disk is
//! not coming back has no way to tell which of the two is happening, and no
//! way to know that closing a reader is what would move it. The pruning
//! report has to say how many rows it deferred for readers.
//!
//! Everything below runs on this thread with no sleeping: the only retention
//! cycles are the ones each journey asks for through
//! `Database::run_pruning_cycle_checked`, and the reader is released by
//! closing the cursor, never by waiting for one.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// The promise this file protects, quoted in every failure so the reader of a
/// failure does not have to come back here for it.
const DEFERRAL_PROMISE: &str = "a retention cycle reports how many expired rows it left in place \
                                because a registered reader can still see them, so an operator \
                                can tell a store with nothing to reclaim from a store waiting on \
                                a reader";

const NEVER: i64 = i64::MAX;
const ALREADY: i64 = -1;

#[derive(Clone, Default)]
struct ManualClock {
    now_ms: Arc<AtomicU64>,
}

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// Budgets wide enough that nothing here is refused for size, with a small
/// page so the cursor stays suspended -- and so stays registered -- across the
/// retention cycle.
fn paged_limits(page_rows: u64) -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: page_rows,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

/// A retained table holding exactly ONE expired row, so the deferred count
/// under test is unambiguous, plus enough live rows to keep a cursor open.
fn seeded_database() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    // The caller drives retention, so the only cycles are the ones asked for.
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 1 HOURS",
        &HashMap::new(),
    )
    .expect("create the retained table");
    let item = |ordinal: u128| Uuid::from_u128(0x17E4_0000_0000_0000_0000_0000_0000_0000 + ordinal);
    for ordinal in 0..4_u128 {
        db.execute(
            "INSERT INTO items (id, expires_at) VALUES ($id, $expires)",
            &params([
                ("id", Value::Uuid(item(ordinal))),
                ("expires", Value::Timestamp(NEVER)),
            ]),
        )
        .expect("seed a live row");
    }
    db.execute(
        "INSERT INTO items (id, expires_at) VALUES ($id, $expires)",
        &params([
            ("id", Value::Uuid(item(9))),
            ("expires", Value::Timestamp(ALREADY)),
        ]),
    )
    .expect("seed the one expired row");
    db
}

fn request(db_limits: ReadLimits, clock: &ManualClock) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        "SELECT id FROM items",
        HashMap::new(),
        db_limits,
        Arc::new(clock.clone()),
    )
}

/// A cycle run while a suspended cursor still holds its snapshot must both
/// leave the expired row alone and SAY that it did, with the count.
#[test]
fn a_pass_that_defers_rows_for_a_registered_reader_reports_the_deferred_count() {
    let journey = "one expired row, one suspended bounded cursor holding a snapshot that sees it";
    let db = seeded_database();
    let clock = ManualClock::default();

    let mut opened = bounded::open_cursor(Arc::clone(&db), &request(paged_limits(2), &clock))
        .expect("open a bounded cursor over the retained table");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor suspended, so its snapshot stays registered"
    );

    let deferred = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle while the cursor is registered");
    assert_eq!(
        deferred.pruned_rows, 0,
        "{journey}: {DEFERRAL_PROMISE}. The expired row must really be held back before the \
         report is judged; a reclaimed row would make this journey prove nothing"
    );
    assert_eq!(
        deferred.rows_deferred_for_readers, 1,
        "{journey}: {DEFERRAL_PROMISE}. The cycle held one expired row back for the registered \
         reader and reported {deferred:?}, which reads exactly like a store with nothing left to \
         reclaim"
    );

    opened.cursor.close().expect("close the bounded cursor");
    drop(opened);

    let reclaimed = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle once no reader is registered");
    assert_eq!(
        reclaimed.pruned_rows, 1,
        "{journey}: {DEFERRAL_PROMISE}. The row the earlier cycle deferred must be reclaimed \
         once its reader is gone, got {reclaimed:?}"
    );
    assert_eq!(
        reclaimed.rows_deferred_for_readers, 0,
        "{journey}: {DEFERRAL_PROMISE}. Nothing was held back this cycle, so nothing may be \
         reported as held back, got {reclaimed:?}"
    );
}

/// The same store with no reader registered at all reclaims its expired row
/// and reports nothing deferred -- the other half of the distinction an
/// operator reads.
#[test]
fn a_pass_with_no_registered_reader_reports_nothing_deferred() {
    let journey = "one expired row, no registered reader";
    let db = seeded_database();

    let report = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle with nothing registered");
    assert_eq!(
        report.pruned_rows, 1,
        "{journey}: {DEFERRAL_PROMISE}. With no reader registered the expired row is reclaimed, \
         got {report:?}"
    );
    assert_eq!(
        report.rows_deferred_for_readers, 0,
        "{journey}: {DEFERRAL_PROMISE}. A cycle that held nothing back must report zero, not a \
         count an operator would read as a waiting reader, got {report:?}"
    );
}
