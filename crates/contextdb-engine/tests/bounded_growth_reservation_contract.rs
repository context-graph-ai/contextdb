#![cfg(feature = "test-seams")]
//! Growing a charged collection is charged in amortized steps.
//!
//! A bounded read charges memory before it retains anything, and a collection
//! that accumulates items has to grow. Growing it by exactly one slot each
//! time turns every append into a fresh allocation and a copy of everything
//! already accumulated, so the cost of a read grows with the square of the
//! items it examines while the declared work ceiling counts only the items.
//! An operator who declares a work ceiling gets a read whose real cost is no
//! longer described by it. Amortized growth — asking for the larger capacity
//! before growing into it — keeps charge-before-retain intact and keeps the
//! declared ceiling meaningful.
//!
//! The witness is the number of growth reservations the request context is
//! asked for, observed through the production kernel's own reservation seam.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Edges the traversal accumulates. Large enough that one-slot-at-a-time
/// growth is unmistakable against amortized growth.
const ACCUMULATED_EDGES: u64 = 1_024;

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

/// Counts the growth reservations the traversal source asks the request
/// context for. It changes nothing about the read.
#[derive(Default)]
struct GrowthReservationCounter {
    traversal_reservations: AtomicU64,
}

impl bounded::ExecutionProbe for GrowthReservationCounter {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn before_temporary_reservation(
        &self,
        source: bounded::TestWorkSource,
        _requested_bytes: u64,
        _held_temporary_bytes: u64,
    ) {
        if source == bounded::TestWorkSource::GraphTraversal {
            self.traversal_reservations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 4_096,
        result_bytes: 64 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 4_096,
        cursor_page_bytes: 16 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

/// One node per edge, so the traversal accumulates a distinct edge each step.
fn wide_edge_fixture() -> Database {
    let db = Database::open_memory();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &HashMap::new())
        .expect("create the node table");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .expect("create the edge table");
    let start = Uuid::from_u128(0x6E0D_0000_0000_0000_0000_0000_0000_0000);
    db.execute(
        "INSERT INTO nodes (id) VALUES ($id)",
        &params([("id", Value::Uuid(start))]),
    )
    .expect("store the start node");
    let tx = db.begin_or_panic();
    for ordinal in 0..ACCUMULATED_EDGES {
        let target = Uuid::from_u128(0x6E1D_0000u128 + ordinal as u128);
        db.execute_in_tx(
            tx,
            "INSERT INTO nodes (id) VALUES ($id)",
            &params([("id", Value::Uuid(target))]),
        )
        .expect("store a neighbour node");
        db.execute_in_tx(
            tx,
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'LINKS')",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0x6E2D_0000u128 + ordinal as u128)),
                ),
                ("source", Value::Uuid(start)),
                ("target", Value::Uuid(target)),
            ]),
        )
        .expect("store an edge");
    }
    db.commit(tx).expect("commit the edge batch");
    db
}

const OPEN_TRAVERSAL_SQL: &str =
    "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) COLUMNS (b.id AS target))";

/// Accumulating a thousand edges must not cost a thousand separate growth
/// reservations, each one a reallocation that copies everything before it.
#[test]
fn a_traversal_that_accumulates_edges_grows_its_state_in_amortized_steps() {
    let db = wide_edge_fixture();
    let counter = Arc::new(GrowthReservationCounter::default());
    let mut request = bounded::BoundedReadRequest::new(
        OPEN_TRAVERSAL_SQL,
        HashMap::new(),
        roomy_limits(),
        Arc::new(FrozenClock),
    );
    request.probe = Some(Arc::clone(&counter) as Arc<dyn bounded::ExecutionProbe>);

    let outcome = bounded::execute(&db, &request).expect("an open traversal must be served");
    assert_eq!(
        outcome.result.rows.len(),
        ACCUMULATED_EDGES as usize,
        "the traversal reports every stored edge"
    );

    let reservations = counter.traversal_reservations.load(Ordering::SeqCst);
    let amortized_ceiling = ACCUMULATED_EDGES / 8 + 64;
    assert!(
        reservations <= amortized_ceiling,
        "accumulating {ACCUMULATED_EDGES} edges asked the request context for {reservations} \
         growth reservations; growth taken one slot at a time reallocates and copies the whole \
         accumulated state on every append, so the read's real cost grows with the square of \
         the edges it examines while the declared work ceiling counts only the edges"
    );
}
