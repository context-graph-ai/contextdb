//! How a read that runs out of memory says so.
//!
//! A bounded read is refused in one vocabulary: a typed refusal carrying the
//! ceiling that was crossed, so a caller can branch on it and an operator can
//! be told which setting to raise.  Running out of memory is a refusal like any
//! other, and it stays a refusal whether the memory the read wanted was denied
//! by its own ceiling or by the database's standing budget.  An internal engine
//! string in that position tells the caller nothing it can act on.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::memory_accounting::MemoryAccountant;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Refusals here are decided by memory, never by elapsed time.
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
        result_rows: 4_096,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 256,
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

fn graph_uuid(seed: u128) -> Uuid {
    Uuid::from_u128(0x5EED_0000_0000_0000_0000_0000_0000_0000 + seed)
}

const GRAPH_SQL: &str = "SELECT COUNT(*) AS candidate_count FROM GRAPH_TABLE(edges \
                         MATCH (a)-[:LINKS]->{1,4}(b) WHERE a.id = $start \
                         COLUMNS (b.id AS target))";

const FANOUT: u128 = 2_000;

/// A start node with a wide outgoing fan-out, so a traversal has real
/// continuation state to retain.
fn graph_fixture(db: &Database) -> Uuid {
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &HashMap::new())
        .expect("create the node table");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .expect("create the edge table");

    let start = graph_uuid(0);
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "INSERT INTO nodes (id) VALUES ($id)",
        &params([("id", Value::Uuid(start))]),
    )
    .expect("seed the start node");
    for ordinal in 1..=FANOUT {
        let child = graph_uuid(ordinal);
        db.execute_in_tx(
            tx,
            "INSERT INTO nodes (id) VALUES ($id)",
            &params([("id", Value::Uuid(child))]),
        )
        .expect("seed a child node");
        db.execute_in_tx(
            tx,
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'LINKS')",
            &params([
                ("id", Value::Uuid(graph_uuid(1_000_000 + ordinal))),
                ("source", Value::Uuid(start)),
                ("target", Value::Uuid(child)),
            ]),
        )
        .expect("seed an edge");
    }
    db.commit(tx).expect("commit the graph fixture");
    start
}

fn assert_memory_refusal(error: bounded::TestError, budget: &str) {
    let bounded::TestError::Refused(refusal) = error else {
        panic!(
            "a read refused because it ran out of memory ({budget}) must say so in the \
             refusal vocabulary a caller can branch on, got {error:?}"
        );
    };
    assert_eq!(
        refusal.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "running out of memory is a crossed ceiling ({budget})"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = refusal.detail() else {
        panic!("a crossed ceiling carries its typed detail ({budget})");
    };
    assert_eq!(
        detail.limit,
        ReadFailureLimit::Memory,
        "the refusal names memory as the ceiling that was crossed ({budget})"
    );
}

/// The read's own memory ceiling.
#[test]
fn a_read_that_exhausts_its_own_memory_ceiling_is_refused_in_the_read_vocabulary() {
    let db = Database::open_memory();
    let start = graph_fixture(&db);
    let mut limits = roomy_limits();
    limits.memory = 4 * 1024;
    let request = bounded::BoundedReadRequest::new(
        GRAPH_SQL,
        params([("start", Value::Uuid(start))]),
        limits,
        Arc::new(FrozenClock),
    );
    assert_memory_refusal(
        bounded::execute(&db, &request)
            .expect_err("a 4 KiB read ceiling cannot hold this traversal"),
        "the read's own ceiling",
    );
}

/// The database's standing budget.
#[test]
fn a_read_that_exhausts_the_database_memory_budget_is_refused_in_the_read_vocabulary() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(Arc::clone(&accountant));
    let start = graph_fixture(&db);
    let settled = accountant.usage().used;
    accountant
        .set_budget(Some(settled + 4 * 1024))
        .expect("tighten the database budget to just above what is already held");

    let request = bounded::BoundedReadRequest::new(
        GRAPH_SQL,
        params([("start", Value::Uuid(start))]),
        roomy_limits(),
        Arc::new(FrozenClock),
    );
    let outcome = bounded::execute(&db, &request);
    let error = match outcome {
        Ok(_) => {
            accountant
                .set_budget(None)
                .expect("restore the database budget");
            panic!(
                "a 4 KiB headroom on the database budget cannot hold this traversal; the \
                 read completed instead"
            );
        }
        Err(error) => error,
    };
    accountant
        .set_budget(None)
        .expect("restore the database budget");
    assert_memory_refusal(error, "the database budget");
}
