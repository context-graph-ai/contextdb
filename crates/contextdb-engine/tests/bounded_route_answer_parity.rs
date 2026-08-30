#![cfg(feature = "test-seams")]
//! One statement, one snapshot, one answer.
//!
//! The bounded read route exists to bound what a read may spend, not to change
//! what it returns. A caller who asks the same question of the same committed
//! state must get the same rows and the same refusal identity whichever route
//! serves it: an edge that leaves and re-enters the same node is still an edge,
//! a vector index whose graph candidates are mostly invisible at the read's
//! snapshot must still answer with the rows that are visible, and a row the
//! reader is not entitled to read is an access refusal, not a missing row.
//!
//! Every bounded read below is issued through the production bounded-kernel
//! entrance and compared against the same statement on the eager route.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Principal, Value};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

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
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
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

// --- an edge from a node to itself -----------------------------------------

const SELF_LOOP_PINNED_SQL: &str = "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:DEPENDS_ON]->(b) \
     WHERE a.id = $start COLUMNS (b.id AS target))";
const SELF_LOOP_OPEN_SQL: &str = "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:DEPENDS_ON]->(b) \
     COLUMNS (b.id AS target))";

/// A module that depends on itself. One node, one stored edge.
fn self_referencing_edge() -> (Database, Uuid) {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .expect("create the node table");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .expect("create the edge table");
    let node = Uuid::from_u128(0x5E1F_0000_0000_0000_0000_0000_0000_0001);
    db.execute(
        "INSERT INTO nodes (id, name) VALUES ($id, $name)",
        &params([
            ("id", Value::Uuid(node)),
            ("name", Value::Text("self-referencing".to_owned())),
        ]),
    )
    .expect("store the node");
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($id, $source, $target, 'DEPENDS_ON')",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x5E1F_0002))),
            ("source", Value::Uuid(node)),
            ("target", Value::Uuid(node)),
        ]),
    )
    .expect("store the self-referencing edge");
    (db, node)
}

/// A traversal that starts at the node the edge leaves must still report the
/// node the edge arrives at, even when they are the same node.
#[test]
fn a_traversal_pinned_to_a_start_node_reports_that_node_s_edge_to_itself() {
    let (db, node) = self_referencing_edge();
    let eager = db
        .execute(
            SELF_LOOP_PINNED_SQL,
            &params([("start", Value::Uuid(node))]),
        )
        .expect("the eager route serves the pinned traversal");
    assert_eq!(
        eager.rows.len(),
        1,
        "the stored edge is the answer on the eager route"
    );

    let outcome = bounded::execute(
        &db,
        &request(SELF_LOOP_PINNED_SQL, params([("start", Value::Uuid(node))])),
    )
    .expect("a pinned traversal must be served");
    assert_eq!(
        outcome.result.rows.len(),
        1,
        "an edge from a node to itself is an edge; the pinned traversal returned {:?} where \
         the same statement on the eager route returns the stored edge",
        outcome.result.rows
    );
}

/// The same stored edge, reached without pinning a start node. The two bounded
/// graph sources must agree with each other.
#[test]
fn an_unpinned_traversal_and_a_pinned_traversal_agree_about_an_edge_to_itself() {
    let (db, node) = self_referencing_edge();
    let open = bounded::execute(&db, &request(SELF_LOOP_OPEN_SQL, HashMap::new()))
        .expect("an unpinned traversal must be served");
    let pinned = bounded::execute(
        &db,
        &request(SELF_LOOP_PINNED_SQL, params([("start", Value::Uuid(node))])),
    )
    .expect("a pinned traversal must be served");

    assert_eq!(
        pinned.result.rows.len(),
        open.result.rows.len(),
        "pinning the start node narrows which edges are traversed, not whether an edge from \
         a node to itself exists; unpinned returned {:?} and pinned returned {:?}",
        open.result.rows,
        pinned.result.rows
    );
}

// --- a vector index whose graph candidates are mostly invisible -------------

/// Rows enrolled into the vector index.
const ENROLLED_VECTORS: u64 = 1_000;
/// Rows left visible after the retirement pass.
const VISIBLE_VECTORS: u64 = 12;
/// Neighbours the read asks for.
const VECTOR_TOP_K: usize = 10;

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

fn vector_for(seed: u64) -> Vec<f32> {
    let angle = seed as f32 * 0.0037;
    vec![angle.cos(), angle.sin(), 0.25]
}

/// A vector table whose enrolled rows are mostly retired, so the index graph
/// is far larger than the set of rows visible at the read's snapshot.
fn mostly_retired_vector_index() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE archives (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .expect("create the vector table");
    for ordinal in 0..ENROLLED_VECTORS {
        db.execute(
            "INSERT INTO archives (id, embedding) VALUES ($id, $embedding)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0x7EC0_0000u128 + ordinal as u128)),
                ),
                ("embedding", Value::Vector(vector_for(ordinal))),
            ]),
        )
        .expect("enroll a vector row");
    }
    for ordinal in VISIBLE_VECTORS..ENROLLED_VECTORS {
        db.execute(
            "DELETE FROM archives WHERE id = $id",
            &params([(
                "id",
                Value::Uuid(Uuid::from_u128(0x7EC0_0000u128 + ordinal as u128)),
            )]),
        )
        .expect("retire a vector row");
    }
    db
}

const VECTOR_SQL: &str = "SELECT id FROM archives ORDER BY embedding <=> $query LIMIT 10";

/// A nearest-neighbour read answers with the rows visible at its snapshot. An
/// index graph full of retired rows changes how the neighbours are found, not
/// how many the caller is entitled to. Both routes reach this answer through
/// their exhaustive source: an index whose live entries have fallen below the
/// graph-eligibility floor is searched exactly, on either route. This is the
/// regime a caller can reach deterministically, and it is asserted here as a
/// standing guard on route parity.
#[test]
fn a_nearest_neighbour_read_answers_with_the_rows_visible_at_its_snapshot() {
    let db = mostly_retired_vector_index();
    let query = params([("query", Value::Vector(vector_for(0)))]);
    let eager = db
        .execute(VECTOR_SQL, &query)
        .expect("the eager route serves the nearest-neighbour read");
    assert_eq!(
        eager.rows.len(),
        VECTOR_TOP_K,
        "the eager route answers with the neighbours the caller asked for"
    );

    let counter = Arc::new(VectorSourceCounter::default());
    let mut probed = request(
        VECTOR_SQL,
        params([("query", Value::Vector(vector_for(0)))]),
    );
    probed.probe = Some(Arc::clone(&counter) as Arc<dyn bounded::ExecutionProbe>);
    let outcome = bounded::execute(&db, &probed).expect("a nearest-neighbour read must be served");
    assert!(
        counter.exhaustive_candidates.load(Ordering::SeqCst) > 0,
        "a retired index is served exhaustively on the bounded route; this fixture must reach \
         that source for the parity it asserts to be the reachable one"
    );
    assert_eq!(
        outcome.result.rows.len(),
        eager.rows.len(),
        "{VISIBLE_VECTORS} rows are visible and {VECTOR_TOP_K} neighbours were asked for; the \
         bounded route returned {} where the same statement on the eager route returns {}",
        outcome.result.rows.len(),
        eager.rows.len()
    );
}

// --- a row the reader is not entitled to read ------------------------------

const DENIED_PRINCIPAL: &str = "unentitled-reader";
const ROW_VECTOR_SQL: &str = "SELECT id FROM docs \
                              ORDER BY embedding <=> ROW_VECTOR('docs','embedding',$anchor) LIMIT 2";

/// A document table gated by entitlement, holding one anchor document the
/// reading principal is not entitled to read.
fn gated_documents() -> (Database, Uuid) {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &HashMap::new(),
    )
    .expect("create the grant table");
    let held = Uuid::from_u128(0xD0C0_0001);
    let withheld = Uuid::from_u128(0xD0C0_0002);
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
         VALUES ($id, 'Agent', $principal, $acl)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0xD0C0_0003))),
            ("principal", Value::Text(DENIED_PRINCIPAL.to_owned())),
            ("acl", Value::Uuid(held)),
        ]),
    )
    .expect("grant the principal the entitlement it holds");
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), embedding VECTOR(3))",
        &HashMap::new(),
    )
    .expect("create the gated document table");
    let anchor = Uuid::from_u128(0xD0C1_0000);
    db.execute(
        "INSERT INTO docs (id, acl_id, embedding) VALUES ($id, $acl, $embedding)",
        &params([
            ("id", Value::Uuid(anchor)),
            ("acl", Value::Uuid(withheld)),
            ("embedding", Value::Vector(vector_for(1))),
        ]),
    )
    .expect("store the anchor document the principal may not read");
    for ordinal in 1..4u64 {
        db.execute(
            "INSERT INTO docs (id, acl_id, embedding) VALUES ($id, $acl, $embedding)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0xD0C1_0000u128 + ordinal as u128)),
                ),
                ("acl", Value::Uuid(held)),
                ("embedding", Value::Vector(vector_for(ordinal + 1))),
            ]),
        )
        .expect("store a readable document");
    }
    (db, anchor)
}

/// Reading a row the principal is not entitled to read is refused as an access
/// decision. Reporting it as a missing row tells the caller the store does not
/// hold what it holds, and hides the refusal a caller branches on.
#[test]
fn a_row_the_reader_is_not_entitled_to_read_keeps_its_access_refusal_identity() {
    let (db, anchor) = gated_documents();
    let scoped = db.scoped_with_constraints(
        None,
        None,
        Some(Principal::Agent(DENIED_PRINCIPAL.to_owned())),
    );
    let eager = scoped
        .execute(ROW_VECTOR_SQL, &params([("anchor", Value::Uuid(anchor))]))
        .expect_err("the eager route refuses the unentitled anchor");
    let eager_text = eager.to_string();
    assert!(
        !eager_text.contains("not found"),
        "the eager route must name an access refusal for an unentitled anchor, observed \
         {eager_text}"
    );

    let refusal = bounded::execute(
        &scoped,
        &request(ROW_VECTOR_SQL, params([("anchor", Value::Uuid(anchor))])),
    )
    .expect_err("the bounded route refuses the unentitled anchor");
    let bounded::TestError::Engine(bounded_text) = &refusal else {
        panic!(
            "the bounded route refuses the unentitled anchor with an engine error, got {refusal:?}"
        );
    };
    assert!(
        !bounded_text.contains("not found"),
        "an unentitled anchor is an access refusal on both routes; the eager route reports \
         `{eager_text}` and the bounded route reports `{bounded_text}`, so a caller branching \
         on the access refusal sees a missing row instead"
    );
}
