// Every test here arms the graph-candidate cap seam, so the whole binary is
// gated on the feature that compiles the seam in.
#![cfg(feature = "test-seams")]
//! A nearest-neighbour read answers with the rows it asked for, whichever
//! route serves it, even when the index graph hands the search back fewer
//! candidates than the caller asked for.
//!
//! The graph is an accelerator, not the answer. A live graph routinely reaches
//! only part of itself, and rows retired since it was built drop out of what it
//! returns, so a search can come back holding fewer usable neighbours than the
//! caller asked for. The eager route notices that shortfall and reads the
//! stored vectors exactly, so the caller still gets its neighbours. A caller on
//! the bounded route asked the same question of the same committed state and is
//! entitled to the same rows: the bounded route may refuse a read whose
//! declared budget cannot pay for the work, but it may not quietly hand back a
//! shorter answer.

use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Rows enrolled into the vector index. Above the count at which a graph
/// search still reaches every stored point, so a candidate shortfall is a
/// regime this index can really be in rather than an artefact of the fixture.
const ENROLLED_VECTORS: u64 = 6_000;
/// Neighbours the read asks for.
const VECTOR_TOP_K: usize = 10;
/// Usable candidates the graph hands back once the cap is armed. Fewer than
/// the caller asked for, which is the whole point.
const SHORT_GRAPH_CANDIDATES: usize = 3;

const VECTOR_SQL: &str = "SELECT id FROM neighbourhoods ORDER BY embedding <=> $query LIMIT 10";

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
        result_rows: 1_024,
        result_bytes: 16 * 1024 * 1024,
        work: 100_000_000,
        active_ms: 1_000_000,
        memory: 256 * 1024 * 1024,
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

fn request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
}

/// Counts which candidate source the read actually used. It changes nothing
/// about the read; it only keeps the fixture honest about what it exercises.
#[derive(Default)]
struct VectorSourceCounter {
    graph_candidates: AtomicU64,
    exhaustive_candidates: AtomicU64,
}

impl bounded::ExecutionProbe for VectorSourceCounter {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _completed_items: u64) {
        match touch {
            bounded::TestSourceTouch::HnswCandidate => {
                self.graph_candidates.fetch_add(1, Ordering::SeqCst);
            }
            bounded::TestSourceTouch::BruteForceVectorCandidate => {
                self.exhaustive_candidates.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

/// Distinct points along one arc, so the nearest neighbours of the first point
/// are unambiguous and the two routes have one ranking to agree on.
fn vector_for(seed: u64) -> Vec<f32> {
    let angle = seed as f32 * 0.0007;
    vec![angle.cos(), angle.sin(), 0.25]
}

fn query_params() -> HashMap<String, Value> {
    params([("query", Value::Vector(vector_for(0)))])
}

/// A vector table large enough for its index to be served by the graph, with
/// every enrolled row visible at the read's snapshot.
fn enrolled_vector_index() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE neighbourhoods (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .expect("create the vector table");
    for ordinal in 0..ENROLLED_VECTORS {
        db.execute(
            "INSERT INTO neighbourhoods (id, embedding) VALUES ($id, $embedding)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0x5EED_0000u128 + ordinal as u128)),
                ),
                ("embedding", Value::Vector(vector_for(ordinal))),
            ]),
        )
        .expect("enroll a vector row");
    }
    // The first read builds the graph. A shortfall needs a graph to fall short
    // of, so the fixture is not in the regime it claims until this has run.
    db.execute(VECTOR_SQL, &query_params())
        .expect("the first nearest-neighbour read builds the index graph");
    db
}

/// The eager route answers a graph shortfall by reading the stored vectors
/// exactly. The bounded route asks the same question of the same committed
/// state, so it owes the caller the same rows.
#[test]
fn a_nearest_neighbour_read_answers_the_same_rows_when_the_index_graph_falls_short() {
    let db = enrolled_vector_index();
    let index = VectorIndexRef::new("neighbourhoods", "embedding");
    let store = db.vector_store_for_test();
    assert!(
        store.has_hnsw_index_for(&index),
        "this fixture asserts what happens when an index graph falls short, so the index must \
         have a graph"
    );

    let _short_graph = store.cap_graph_candidates_for_test(&index, SHORT_GRAPH_CANDIDATES);
    let eager = db
        .execute(VECTOR_SQL, &query_params())
        .expect("the eager route serves the nearest-neighbour read");
    assert_eq!(
        eager.rows.len(),
        VECTOR_TOP_K,
        "the eager route reads the stored vectors exactly when the graph hands back \
         {SHORT_GRAPH_CANDIDATES} usable candidates for {VECTOR_TOP_K} requested neighbours; \
         this fixture must reach that regime for the parity it asserts to be the reachable one"
    );

    let counter = Arc::new(VectorSourceCounter::default());
    let mut probed = request(VECTOR_SQL, query_params());
    probed.probe = Some(Arc::clone(&counter) as Arc<dyn bounded::ExecutionProbe>);
    let outcome = bounded::execute(&db, &probed).expect("a nearest-neighbour read must be served");
    assert!(
        counter.graph_candidates.load(Ordering::SeqCst) > 0,
        "the bounded read must reach the graph source for this fixture to be about a graph \
         shortfall at all"
    );
    assert_eq!(
        outcome.result.rows,
        eager.rows,
        "the graph handed back {SHORT_GRAPH_CANDIDATES} usable candidates for {VECTOR_TOP_K} \
         requested neighbours; the bounded route returned {} rows where the same statement on \
         the eager route returns {}, so a caller on the bounded route is quietly served a \
         shorter answer from the same committed state",
        outcome.result.rows.len(),
        eager.rows.len()
    );
}

/// A budget that cannot pay for the exact read the shortfall requires is a
/// refusal the caller can see and act on, not a short answer that looks like
/// the whole one.
#[test]
fn a_budget_too_small_for_the_read_a_graph_shortfall_requires_is_refused_not_shortened() {
    let db = enrolled_vector_index();
    let index = VectorIndexRef::new("neighbourhoods", "embedding");
    let store = db.vector_store_for_test();
    let _short_graph = store.cap_graph_candidates_for_test(&index, SHORT_GRAPH_CANDIDATES);

    let served = bounded::execute(&db, &request(VECTOR_SQL, query_params()))
        .expect("the roomy budget must serve the read");
    let observed_work = served.telemetry.work_units;

    // A budget sized to what the read has already been observed to spend, plus
    // a margin. Whatever the read does with a shortfall, this budget is the one
    // it just fitted inside, so a refusal here is a refusal about the extra
    // work the shortfall requires and not about the read as a whole.
    let mut limits = roomy_limits();
    limits.work = observed_work.saturating_add(64);
    let request =
        bounded::BoundedReadRequest::new(VECTOR_SQL, query_params(), limits, Arc::new(FrozenClock));

    match bounded::execute(&db, &request) {
        Ok(outcome) => assert_eq!(
            outcome.result.rows.len(),
            VECTOR_TOP_K,
            "a read served inside a {} unit budget answered with {} of {VECTOR_TOP_K} requested \
             neighbours; a caller told its read succeeded cannot tell that the graph shortfall \
             cost it rows, so the short answer must instead be a refusal naming the ceiling it \
             could not pay",
            limits.work,
            outcome.result.rows.len()
        ),
        Err(bounded::TestError::Refused(refusal)) => {
            assert_eq!(
                refusal.kind(),
                ReadFailureKind::OwnerLimitExceeded,
                "a read that cannot pay for the work a graph shortfall requires is refused \
                 against its declared ceiling, observed {refusal:?}"
            );
            let ReadFailureDetail::OwnerLimitExceeded(detail) = refusal.detail() else {
                panic!("a crossed ceiling carries the typed detail a caller branches on");
            };
            assert_eq!(
                detail.limit,
                ReadFailureLimit::Work,
                "the work a graph shortfall requires is charged as work, so the refusal names \
                 the work ceiling, observed {detail:?}"
            );
        }
        Err(other) => panic!(
            "a graph shortfall under a tight budget is answered in full or refused against the \
             declared ceiling, observed {other:?}"
        ),
    }
}
