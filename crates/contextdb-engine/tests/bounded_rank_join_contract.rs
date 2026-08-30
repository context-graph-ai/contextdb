#![cfg(feature = "test-seams")]
//! A ranked vector read resolves its joined row, it does not read the joined
//! table.
//!
//! A rank policy names a joined table, a join column, and the declared index
//! that orders that column. Resolving the joined row for one ranked candidate
//! is therefore a lookup: the index names the row identity, and the row is
//! fetched by that identity. A resolution that instead reads every row of the
//! joined table charges the operator's declared work ceiling once per stored
//! row per candidate, so the same ranked read that is served over a small
//! joined table is refused over a large one holding the same answers.
//!
//! Every read below is issued through the production bounded-kernel entrance.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Anchor rows carrying the ranked vector column.
const ANCHOR_ROWS: u64 = 8;
/// Rows stored in the joined table. Only one of them joins to each anchor.
const JOINED_ROWS: u64 = 2_000;
/// Candidates the ranked read asks for.
const RANKED_LIMIT: u64 = 3;
/// Joined-table inspections a resolution may make per ranked candidate and
/// still be a lookup: the joined run, plus room for the boundary entry.
const JOIN_LOOKUP_BUDGET: u64 = 16;

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
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
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

/// A ranked vector table whose rank policy joins a table far larger than the
/// set of rows that actually join to any anchor.
fn ranked_table_over_a_large_joined_table() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE ranked_outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
        &HashMap::new(),
    )
    .expect("create the joined outcome table");
    db.execute(
        "CREATE INDEX ranked_outcomes_decision_idx ON ranked_outcomes(decision_id)",
        &HashMap::new(),
    )
    .expect("declare the joined outcome index the rank policy relies on");
    db.execute(
        "CREATE TABLE ranked_decisions (
            id UUID PRIMARY KEY,
            confidence REAL,
            embedding VECTOR(2) RANK_POLICY (
                JOIN ranked_outcomes ON decision_id,
                FORMULA 'coalesce({confidence}, 0.0) * coalesce({success}, 0.0)',
                SORT_KEY confidence_weighted
            )
        )",
        &HashMap::new(),
    )
    .expect("create the ranked decision table");

    for ordinal in 0..ANCHOR_ROWS {
        let id = Uuid::from_u128(0xDEC1_0000u128 + ordinal as u128);
        db.execute(
            "INSERT INTO ranked_decisions (id, confidence, embedding) VALUES ($id, $confidence, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("confidence", Value::Float64((ordinal + 1) as f64 / 16.0)),
                ("embedding", Value::Vector(unit_vector(ordinal))),
            ]),
        )
        .expect("store a ranked decision row");
        db.execute(
            "INSERT INTO ranked_outcomes (id, decision_id, success) VALUES ($id, $decision_id, $success)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0xDEC2_0000u128 + ordinal as u128))),
                ("decision_id", Value::Uuid(id)),
                ("success", Value::Bool(true)),
            ]),
        )
        .expect("store the joined outcome row for a ranked decision");
    }

    // Joined rows that belong to decisions this read never ranks. They are the
    // rows a lookup never has to look at.
    for ordinal in 0..JOINED_ROWS {
        db.execute(
            "INSERT INTO ranked_outcomes (id, decision_id, success) VALUES ($id, $decision_id, $success)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0xDEC3_0000u128 + ordinal as u128)),
                ),
                (
                    "decision_id",
                    Value::Uuid(Uuid::from_u128(0xDEC4_0000u128 + ordinal as u128)),
                ),
                ("success", Value::Bool(false)),
            ]),
        )
        .expect("store an unrelated joined outcome row");
    }
    db
}

const RANKED_SQL: &str = "SELECT id FROM ranked_decisions ORDER BY embedding <=> $query \
                          USE RANK confidence_weighted LIMIT 3";

fn ranked_request(limits: ReadLimits) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        RANKED_SQL,
        params([("query", Value::Vector(unit_vector(0)))]),
        limits,
        Arc::new(FrozenClock),
    )
}

/// The joined row for a ranked candidate is named by the policy's declared
/// index, so resolving it must not read rows the join excludes.
#[test]
fn a_ranked_read_resolves_its_joined_row_without_reading_the_joined_table() {
    let db = ranked_table_over_a_large_joined_table();
    let outcome = bounded::execute(&db, &ranked_request(roomy_limits()))
        .expect("a ranked vector read must be served");

    assert_eq!(
        outcome.result.rows.len(),
        RANKED_LIMIT as usize,
        "the ranked read answers with the candidates it asked for"
    );
    let inspected = outcome
        .telemetry
        .source_work
        .get(&bounded::TestWorkSource::RankCandidates)
        .copied()
        .unwrap_or_default();
    let ceiling = RANKED_LIMIT * JOIN_LOOKUP_BUDGET;
    assert!(
        inspected <= ceiling,
        "resolving the joined row for each of {RANKED_LIMIT} ranked candidates must be a \
         lookup through the policy's declared index; the read inspected {inspected} joined \
         candidates against a joined table of {} rows, so it reads the whole joined table \
         once per candidate",
        JOINED_ROWS + ANCHOR_ROWS
    );
}

/// The declared work ceiling belongs to the answer, not to the size of a table
/// the join excludes.
#[test]
fn a_ranked_read_is_served_under_a_work_ceiling_sized_for_its_candidates() {
    let db = ranked_table_over_a_large_joined_table();
    let mut limits = roomy_limits();
    limits.work = (ANCHOR_ROWS + RANKED_LIMIT * JOIN_LOOKUP_BUDGET) * 8;
    let outcome = bounded::execute(&db, &ranked_request(limits));

    let served = match outcome {
        Ok(served) => served,
        Err(error) => panic!(
            "a ranked read over {ANCHOR_ROWS} anchor rows must be served under a work ceiling \
             of {} units; it was answered with {error:?}, so the ceiling is being spent on \
             joined rows that never join to a ranked candidate",
            limits.work
        ),
    };
    assert_eq!(served.result.rows.len(), RANKED_LIMIT as usize);
}
