//! A user table named `dual` is a table, not a constant.
//!
//! The bounded read path intercepts the name `dual` and answers with a
//! one-row, zero-column constant source.  Nothing reserves that name, so an
//! operator may create a real table called `dual`, insert into it, and index a
//! vector column on it.  Every read below is issued through the production
//! bounded-kernel entrance against a table the operator actually created, and
//! asks for what the operator stored.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Reads here are decided by ceilings and stored rows, never by elapsed time.
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
        result_rows: 256,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 64,
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

fn request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
}

/// The row count the operator stores in the table they named `dual`.
const STORED_ROWS: u128 = 6;

/// A table the operator named `dual`, carrying a vector column with a rank
/// policy — exactly the shape that reaches the ranked materializer.
fn dual_named_table_with_rank_policy() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE dual_outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
        &HashMap::new(),
    )
    .expect("create the joined outcome table");
    db.execute(
        "CREATE INDEX dual_outcomes_decision_idx ON dual_outcomes(decision_id)",
        &HashMap::new(),
    )
    .expect("index the joined outcome table");
    db.execute(
        "CREATE TABLE dual (
            id UUID PRIMARY KEY,
            confidence REAL,
            embedding VECTOR(2) RANK_POLICY (
                JOIN dual_outcomes ON decision_id,
                FORMULA 'coalesce({confidence}, 0.0) * coalesce({success}, 0.0)',
                SORT_KEY confidence_weighted
            )
        )",
        &HashMap::new(),
    )
    .expect("an operator may create a table named dual");

    for ordinal in 0..STORED_ROWS {
        let id = Uuid::from_u128(0xD0A1_0000_0000_0000_0000_0000_0000_0000 + ordinal);
        db.execute(
            "INSERT INTO dual (id, confidence, embedding) VALUES ($id, $confidence, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("confidence", Value::Float64((ordinal + 1) as f64 / 8.0)),
                ("embedding", Value::Vector(unit_vector(ordinal as u64))),
            ]),
        )
        .expect("store a row in the table named dual");
        db.execute(
            "INSERT INTO dual_outcomes (id, decision_id, success) VALUES ($id, $decision_id, $success)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0xD0A2_0000 + ordinal))),
                ("decision_id", Value::Uuid(id)),
                ("success", Value::Bool(true)),
            ]),
        )
        .expect("store the joined outcome row");
    }
    db
}

/// The rows the operator stored are readable by an ordinary bounded read.
#[test]
fn a_bounded_read_of_a_user_table_named_dual_returns_its_stored_rows() {
    let db = dual_named_table_with_rank_policy();
    let outcome = bounded::execute(&db, &request("SELECT id FROM dual", HashMap::new()))
        .expect("reading a table the operator created must succeed");
    assert_eq!(
        outcome.result.rows.len(),
        STORED_ROWS as usize,
        "a bounded read of the table named dual must return the rows the operator \
         stored in it, not the zero-column constant source the name is intercepted \
         into; got {:?} with columns {:?}",
        outcome.result.rows,
        outcome.result.columns
    );
}

/// The ranked vector read over the same table completes and answers with the
/// operator's rows rather than ending the read in a slice panic.
#[test]
fn a_ranked_vector_read_of_a_user_table_named_dual_answers_with_its_rows() {
    let db = dual_named_table_with_rank_policy();
    let outcome = bounded::execute(
        &db,
        &request(
            "SELECT id FROM dual ORDER BY embedding <=> $query USE RANK confidence_weighted LIMIT 3",
            params([("query", Value::Vector(unit_vector(0)))]),
        ),
    )
    .expect("a ranked vector read of a table the operator created must succeed");
    assert_eq!(
        outcome.result.rows.len(),
        3,
        "a ranked vector read of the table named dual must rank the operator's own \
         rows; got {:?} with columns {:?}",
        outcome.result.rows,
        outcome.result.columns
    );
}

/// A cursor over the same ranked read pages the operator's rows.
#[test]
fn a_cursor_over_a_ranked_read_of_a_user_table_named_dual_pages_its_rows() {
    let db = dual_named_table_with_rank_policy();
    let mut limits = roomy_limits();
    limits.cursor_page_rows = 1;
    let mut paged = bounded::BoundedReadRequest::new(
        "SELECT id FROM dual ORDER BY embedding <=> $query USE RANK confidence_weighted LIMIT 3",
        params([("query", Value::Vector(unit_vector(0)))]),
        limits,
        Arc::new(FrozenClock),
    );
    paged.probe = None;
    let opened = bounded::open_cursor(Arc::clone(&db), &paged)
        .expect("a cursor over a ranked read of an operator's table must open");
    assert_eq!(
        opened.first_page.rows.len(),
        1,
        "the first page of a cursor over the table named dual must carry one of the \
         operator's own rows; got {:?}",
        opened.first_page.rows
    );
    let mut cursor = opened.cursor;
    cursor
        .close()
        .expect("closing releases the cursor's retained state");
}
