//! What a cancelled ranked read gives back.
//!
//! A ranked vector read pulls each candidate row out of the table, charges the
//! bytes it is holding, and then runs the rank join for it. Cancellation can
//! land in the middle of that join, with a row already pulled and already
//! charged. Whatever the read was holding at that moment belongs back on the
//! database's accountant before the read is over, so a caller who cancels and
//! asks again starts from the budget they actually have and not from a smaller
//! one.
//!
//! The rank fixture, the cancellation, and every read below go through the
//! production bounded-kernel entrance.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::memory_accounting::MemoryAccountant;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Cancellation here is driven by the source loop reaching a rank candidate,
/// never by elapsed time.
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

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        // One row per page keeps the cursor open past its first page.
        cursor_page_rows: 1,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn unit_vector(seed: u64) -> Vec<f32> {
    let slope = 1.0 / (seed.saturating_add(1) as f32);
    let norm = (1.0 + slope * slope).sqrt();
    vec![1.0 / norm, slope / norm]
}

/// Cancel the read the moment it reaches its Nth rank candidate, which is
/// inside the join the ranked materializer runs for a row it has already pulled
/// and charged.
struct CancelAtRankCandidate {
    cancellation: OwnerReadCancellation,
    cancel_at: u64,
    rank_candidates: AtomicU64,
}

impl CancelAtRankCandidate {
    fn new(cancellation: OwnerReadCancellation, cancel_at: u64) -> Self {
        Self {
            cancellation,
            cancel_at,
            rank_candidates: AtomicU64::new(0),
        }
    }

    fn rank_candidates(&self) -> u64 {
        self.rank_candidates.load(Ordering::SeqCst)
    }
}

impl bounded::ExecutionProbe for CancelAtRankCandidate {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _completed_items: u64) {
        if touch != bounded::TestSourceTouch::RankCandidate {
            return;
        }
        let reached = self.rank_candidates.fetch_add(1, Ordering::SeqCst) + 1;
        if reached == self.cancel_at {
            self.cancellation.cancel();
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

const RANKED_ROWS: u128 = 24;

/// Rows wide enough that an abandoned one is a visible charge rather than a
/// rounding difference.
fn ranked_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE ranked_outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
        &HashMap::new(),
    )
    .expect("create the joined outcome table");
    db.execute(
        "CREATE INDEX ranked_outcomes_decision_idx ON ranked_outcomes(decision_id)",
        &HashMap::new(),
    )
    .expect("index the joined outcome table");
    db.execute(
        "CREATE TABLE ranked_decisions (
            id UUID PRIMARY KEY,
            confidence REAL,
            note TEXT,
            embedding VECTOR(2) RANK_POLICY (
                JOIN ranked_outcomes ON decision_id,
                FORMULA 'coalesce({confidence}, 0.0) * coalesce({success}, 0.0)',
                SORT_KEY confidence_weighted
            )
        )",
        &HashMap::new(),
    )
    .expect("create the ranked table");

    for ordinal in 0..RANKED_ROWS {
        let id = Uuid::from_u128(0xABCD_0000_0000_0000_0000_0000_0000_0000_u128 + ordinal);
        db.execute(
            "INSERT INTO ranked_decisions (id, confidence, note, embedding)
             VALUES ($id, $confidence, $note, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("confidence", Value::Float64((ordinal + 1) as f64 / 32.0)),
                ("note", Value::Text("x".repeat(200))),
                ("embedding", Value::Vector(unit_vector(ordinal as u64))),
            ]),
        )
        .expect("store a ranked row");
        db.execute(
            "INSERT INTO ranked_outcomes (id, decision_id, success) VALUES ($id, $decision_id, $success)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0xBEEF_0000 + ordinal))),
                ("decision_id", Value::Uuid(id)),
                ("success", Value::Bool(ordinal % 3 != 0)),
            ]),
        )
        .expect("store the joined outcome row");
    }
}

const RANKED_SQL: &str = "SELECT id, note FROM ranked_decisions \
                          ORDER BY embedding <=> $query USE RANK confidence_weighted LIMIT 8";

/// Cancelling a ranked read again and again must cost nothing: the database's
/// accountant returns to where it stood before the read, and the reopened
/// attempt still has its whole budget.
#[test]
fn repeatedly_cancelled_ranked_reads_return_every_byte_they_abandon() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Arc::new(Database::open_memory_with_accountant(Arc::clone(
        &accountant,
    )));
    ranked_fixture(&db);
    let settled = accountant.usage().used;

    let mut reached_rank = 0_u64;
    for attempt in 1..=16_u64 {
        let cancellation = OwnerReadCancellation::new();
        let probe = Arc::new(CancelAtRankCandidate::new(cancellation.clone(), attempt));
        let mut request = bounded::BoundedReadRequest::new(
            RANKED_SQL,
            params([("query", Value::Vector(unit_vector(0)))]),
            roomy_limits(),
            Arc::new(FrozenClock),
        );
        request.cancellation = cancellation;
        request.probe = Some(probe.clone());

        match bounded::open_cursor(Arc::clone(&db), &request) {
            Err(bounded::TestError::Cancelled) => {}
            Ok(_) => panic!(
                "attempt {attempt}: cancelling at rank candidate {attempt} must stop the \
                 read; it completed instead"
            ),
            Err(other) => panic!(
                "attempt {attempt}: cancelling at a rank candidate must report \
                 cancellation, got {other:?}"
            ),
        }
        reached_rank = reached_rank.max(probe.rank_candidates());
        assert_eq!(
            accountant.usage().used,
            settled,
            "attempt {attempt}: a cancelled ranked read must give the database back \
             every byte it charged; the row it abandoned mid-rank is still charged"
        );
    }

    assert!(
        reached_rank > 0,
        "the ranked read must actually reach a rank candidate for this to say anything"
    );
}
