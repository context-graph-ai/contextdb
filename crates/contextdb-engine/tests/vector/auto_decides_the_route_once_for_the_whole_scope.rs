//! The automatic mode decides between the graph walk and exhaustive comparison
//! once for the whole statement, so splitting the same rows across more
//! Contexts never changes which route answers or what it costs.
//!
//! A find is one search producing one top k. The walk it would run spends at
//! most one budget `B = cap(EF_SEARCH, k) x M_max0`, `M_max0` being the widest
//! admitted graph's layer-0 maximum degree (`2 x M`), however many Contexts
//! the rows sit in; the exhaustive route compares every admitted row once. So
//! `AUTO` weighs the walk's `B` evaluations against the admitted rows'
//! comparisons, each at its measured unit cost, and picks the cheaper for the
//! statement. A decision made per Context instead flips as the same rows are
//! split more finely: a Context small enough to compare outright is compared,
//! and the one statement runs the exhaustive route over some Contexts and the
//! walk over others.
//!
//! Every expected figure here is computed from the declared numbers and the
//! measured unit costs the approved crossover carries, never read back from
//! the implementation.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, VectorSearchRoute};
use contextdb_vector::hnsw::with_union_search_receipts;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;
/// The breadth and degree the column declares; every graph is built at `M`.
const DECLARED_EF_SEARCH: usize = 400;
const DECLARED_M: usize = 16;
const SPLITS: [usize; 3] = [1, 8, 64];
/// Both corpora are cheaper to compare in memory at this breadth. The graph
/// side of the crossover is exercised by the larger route-cost fixtures.
const CORPORA: [usize; 2] = [512, 2_048];

/// The measured walk cost and in-memory exact comparison cost, in nanoseconds.
/// The latter is 3,400 ns on the release 2,000-row calibration used by AUTO.
const WALK_EVALUATION_NANOS: u128 = 5_300;
const EXHAUSTIVE_COMPARISON_NANOS: u128 = 3_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Walk,
    Exhaustive,
}

/// HNSW builds its bottom layer with twice the declared degree.
fn layer0_max_degree(declared_m: usize) -> usize {
    2 * declared_m
}

/// The one budget a walk over the statement may spend.
fn walk_budget() -> usize {
    DECLARED_EF_SEARCH.max(LIMIT) * layer0_max_degree(DECLARED_M)
}

/// The statement-level decision, from the aggregate admitted rows alone.
fn expected_route(aggregate_allowed: usize) -> Route {
    let walk = walk_budget() as u128 * WALK_EVALUATION_NANOS;
    let exhaustive = aggregate_allowed as u128 * EXHAUSTIVE_COMPARISON_NANOS;
    if exhaustive <= walk {
        Route::Exhaustive
    } else {
        Route::Walk
    }
}

struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

#[derive(Default)]
struct TouchCounter {
    touches: Mutex<BTreeMap<bounded::TestSourceTouch, u64>>,
}

impl bounded::ExecutionProbe for TouchCounter {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _: u64) {
        *self
            .touches
            .lock()
            .expect("the touch counter is never poisoned")
            .entry(touch)
            .or_default() += 1;
    }

    fn cancellation_observed(&self, _: u64) {}
}

impl TouchCounter {
    fn count(&self, touch: bounded::TestSourceTouch) -> u64 {
        self.touches
            .lock()
            .expect("the touch counter is never poisoned")
            .get(&touch)
            .copied()
            .unwrap_or(0)
    }
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xA07C_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xA07C_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

/// The same rows at every split: row `ordinal` has the same vector however
/// the corpus is divided.
fn embedding(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (ordinal
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn query_vector() -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = axis.wrapping_mul(0x27d4_eb2f) % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

fn fixture(rows: usize, partitions: usize) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE split_rows (id INT PRIMARY KEY, context_id UUID NOT NULL, \
             generation_id UUID NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 64 SEARCH_MODE AUTO AUTO_INDEX_AT 1 \
             HNSW (M = {DECLARED_M}, EF_SEARCH = {DECLARED_EF_SEARCH}))"
        ),
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    let per_partition = rows / partitions;
    for ordinal in 0..rows {
        let partition = ordinal / per_partition;
        db.execute(
            "INSERT INTO split_rows VALUES ($id, $context, $generation, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(ordinal as i64 + 1)),
                ("context".into(), Value::Uuid(context_id(partition))),
                ("generation".into(), Value::Uuid(generation_id(partition))),
                ("embedding".into(), Value::Vector(embedding(ordinal))),
            ]),
        )
        .expect("seed one row");
    }
    let index = VectorIndexRef::new("split_rows", "embedding");
    for _ in 0..4096 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = db
            .vector_store_for_test()
            .partition_infos(&index, &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if ready.len() == partitions && ready.iter().all(|partition| partition.graph_available) {
            return db;
        }
    }
    panic!("every partition must publish a maintained graph before the route is compared");
}

fn statement(partitions: usize) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([("query".into(), Value::Vector(query_vector()))]);
    let mut contexts = Vec::with_capacity(partitions);
    let mut generations = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    (
        format!(
            "SELECT id FROM split_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             ORDER BY embedding <=> $query LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

/// What one split of one corpus observed.
#[derive(Debug)]
struct Observed {
    /// Every admitted Context's executed route, in scope order.
    routes: Vec<Route>,
    rows: usize,
    exhaustive_comparisons: u64,
    walk_evaluations: u64,
    searches: usize,
    walk_admission: usize,
    walk_expansion: usize,
    walk_budget: usize,
    walk_terminal: Option<&'static str>,
}

fn observe(rows: usize, partitions: usize) -> Observed {
    let db = fixture(rows, partitions);
    let (sql, params) = statement(partitions);
    let counter = Arc::new(TouchCounter::default());
    let mut request =
        bounded::BoundedReadRequest::new(sql, params, limits(), Arc::new(FrozenClock));
    request.probe = Some(counter.clone());
    let (executed, receipts) = with_union_search_receipts(|| bounded::execute(&db, &request));
    let executed = executed.expect("the automatic find answers");
    let disclosure = executed
        .result
        .trace
        .vector_search
        .as_ref()
        .expect("a vector find discloses its route");
    let routes = disclosure
        .partition_hnsw
        .iter()
        .map(|partition| match partition.route {
            Some(VectorSearchRoute::Exact) => Route::Exhaustive,
            Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed) => Route::Walk,
            other => panic!("every admitted Context reports its executed route, got {other:?}"),
        })
        .collect();
    let receipt = receipts.first();
    Observed {
        routes,
        rows: executed.result.rows.len(),
        exhaustive_comparisons: counter.count(bounded::TestSourceTouch::BruteForceVectorCandidate),
        walk_evaluations: counter.count(bounded::TestSourceTouch::HnswCandidate),
        searches: receipts.len(),
        walk_admission: receipt.map_or(0, |receipt| receipt.admission_evaluations),
        walk_expansion: receipt.map_or(0, |receipt| receipt.expansion_evaluations),
        walk_budget: receipt.map_or(0, |receipt| receipt.budget),
        walk_terminal: receipt.map(|receipt| receipt.terminal),
    }
}

/// The same rows split across one, eight and sixty-four Contexts, for a corpus
/// the in-memory crossover sends to exhaustive comparison:
/// `AUTO` takes the route the aggregate decision names at every split, the
/// same route for every Context, spends that route's work, and answers a full
/// page.
#[test]
fn auto_takes_the_aggregate_route_however_the_rows_are_split() {
    let mut failures = Vec::new();
    for corpus in CORPORA {
        let expected = expected_route(corpus);
        let mut decisions = Vec::new();
        for partitions in SPLITS {
            let cell = format!("{corpus} rows over {partitions} Contexts");
            let observed = observe(corpus, partitions);
            println!(
                "auto_split_decision {cell} expected={expected:?} B={} observed={observed:?}",
                walk_budget()
            );
            decisions.push((partitions, observed.routes.clone()));
            if observed.rows != LIMIT {
                failures.push(format!("{cell}: {} rows, not a full page", observed.rows));
            }
            if observed.routes.len() != partitions {
                failures.push(format!(
                    "{cell}: {} Contexts reported a route, {partitions} were admitted",
                    observed.routes.len()
                ));
            }
            if let Some(other) = observed.routes.iter().find(|route| **route != expected) {
                failures.push(format!(
                    "{cell}: the aggregate decision is {expected:?}, but a Context took {other:?} \
                     (routes {:?})",
                    observed.routes
                ));
            }
            match expected {
                Route::Exhaustive => {
                    if observed.exhaustive_comparisons != corpus as u64 {
                        failures.push(format!(
                            "{cell}: the exhaustive route compares each of the {corpus} admitted \
                             rows once, it compared {}",
                            observed.exhaustive_comparisons
                        ));
                    }
                    if observed.searches != 0 || observed.walk_evaluations != 0 {
                        failures.push(format!(
                            "{cell}: the exhaustive route walks no graph, but {} searches spent \
                             {} graph evaluations",
                            observed.searches, observed.walk_evaluations
                        ));
                    }
                }
                Route::Walk => {
                    if observed.searches != 1 {
                        failures.push(format!(
                            "{cell}: the walk is one search over the scope, saw {}",
                            observed.searches
                        ));
                    }
                    if observed.walk_budget != walk_budget() {
                        failures.push(format!(
                            "{cell}: the walk was given {} evaluations, the declared numbers \
                             allow {}",
                            observed.walk_budget,
                            walk_budget()
                        ));
                    }
                    if observed.walk_expansion > walk_budget() {
                        failures.push(format!(
                            "{cell}: the walk spent {} evaluations past opening its layers, \
                             more than its budget {}",
                            observed.walk_expansion,
                            walk_budget()
                        ));
                    }
                    if observed.walk_terminal == Some("refused") {
                        failures.push(format!(
                            "{cell}: the walk was refused, so another route answered"
                        ));
                    }
                    if observed.exhaustive_comparisons != 0 {
                        failures.push(format!(
                            "{cell}: the walk compared {} rows exhaustively as well",
                            observed.exhaustive_comparisons
                        ));
                    }
                    if observed.walk_admission + observed.walk_expansion == 0 {
                        failures.push(format!("{cell}: the walk evaluated nothing"));
                    }
                }
            }
        }
        let first = decisions
            .first()
            .map(|(_, routes)| routes.first().copied())
            .unwrap_or(None);
        if decisions
            .iter()
            .any(|(_, routes)| routes.iter().any(|route| Some(*route) != first))
        {
            failures.push(format!(
                "{corpus} rows: the route changed with the split: {decisions:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
