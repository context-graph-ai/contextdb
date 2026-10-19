//! An `AUTO` search that resolves to the maintained graph must cost exactly
//! what an explicit `INDEXED` search of the same statement costs.
//!
//! `AUTO` is how a consumer asks for the best available route, not a second and
//! more expensive way to reach the route `INDEXED` names outright. The two arms
//! of one filtered partitioned statement, over one store and one snapshot, are
//! compared on the deterministic counters the bounded kernel already publishes:
//! total charged work, examined rows, the per-source work split, the source
//! items actually reached, and the answer itself.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::{BTreeMap, HashMap};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use uuid::Uuid;

const PARTITIONS: usize = 8;
/// The frozen consumer shape: 10,000 passages over eight Contexts, every one of
/// them current, with `is_current` served as a residual from the declared
/// index.
const ROWS_PER_PARTITION: usize = 1_250;
const DIMENSIONS: usize = 16;

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

/// Counts every source item the bounded kernel actually reaches, by kind.
#[derive(Default)]
struct TouchCounter {
    touches: Mutex<BTreeMap<bounded::TestSourceTouch, u64>>,
    work_events: AtomicU64,
}

impl bounded::ExecutionProbe for TouchCounter {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {
        self.work_events.fetch_add(1, Ordering::SeqCst);
    }

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
    fn snapshot(&self) -> BTreeMap<bounded::TestSourceTouch, u64> {
        self.touches
            .lock()
            .expect("the touch counter is never poisoned")
            .clone()
    }
}

struct Executed {
    rows: Vec<Vec<Value>>,
    work_units: u64,
    rows_examined: u64,
    source_work: BTreeMap<bounded::TestWorkSource, u64>,
    touches: BTreeMap<bounded::TestSourceTouch, u64>,
    resolved_mode: Option<contextdb_core::VectorSearchMode>,
    route: Option<contextdb_engine::VectorSearchRoute>,
    aggregate_allowed_vectors: Option<usize>,
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC047_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC047_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

/// A deterministic spread over the unit cube; no randomness, no clock.
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

fn query_vector(probe: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (probe
                .wrapping_mul(0xc2b2_ae35)
                .wrapping_add(axis.wrapping_mul(0x27d4_eb2f)))
                % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

fn fixture() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE route_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
         embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the partitioned route fixture");
    db.execute(
        "CREATE INDEX route_vectors_active_idx ON route_vectors \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the relational index the residual filter is served from");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            db.execute(
                "INSERT INTO route_vectors VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one partitioned vector row");
        }
    }
    for _ in 0..512 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = db
            .vector_store_for_test()
            .partition_infos(
                &VectorIndexRef::new("route_vectors", "embedding"),
                &mut partition_observation_owner_1,
            )
            .expect("inspect partition readiness");
        if ready.len() == PARTITIONS && ready.iter().all(|partition| partition.graph_available) {
            return db;
        }
    }
    panic!("every partition must publish a maintained route before the route arms are compared");
}

fn statement(partitions: usize, mode: Option<&str>) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector(0))),
    ]);
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
    // The declared arm carries no `USE VECTOR` clause at all: it is the
    // statement a consumer whose column declares `SEARCH_MODE AUTO` writes, and
    // it is the shape the measured regression was seen on.
    let clause = match mode {
        Some(mode) => format!(" USE VECTOR {mode}"),
        None => String::new(),
    };
    (
        format!(
            "SELECT id FROM route_vectors WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query{clause} LIMIT 10",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

fn execute(db: &Database, partitions: usize, mode: Option<&str>) -> Executed {
    let counter = Arc::new(TouchCounter::default());
    let (sql, params) = statement(partitions, mode);
    let mut request =
        bounded::BoundedReadRequest::new(sql, params, limits(), Arc::new(FrozenClock));
    request.probe = Some(counter.clone());
    let executed = bounded::execute(db, &request).expect("the bounded route arm succeeds");
    let disclosure = executed.result.trace.vector_search.as_ref();
    Executed {
        rows: executed.result.rows.clone(),
        work_units: executed.telemetry.work_units,
        rows_examined: executed.result.trace.rows_examined,
        source_work: executed.telemetry.source_work.clone(),
        touches: counter.snapshot(),
        resolved_mode: disclosure.map(|disclosure| disclosure.resolved_mode),
        route: disclosure.and_then(|disclosure| disclosure.route),
        aggregate_allowed_vectors: disclosure.and_then(|d| d.aggregate_allowed_vectors),
    }
}

fn report(scope: usize, label: &str, executed: &Executed) {
    println!(
        "route_arm scope={scope} mode={label} resolved={:?} route={:?} allowed={:?} \
         work_units={} rows_examined={} source_work={:?} touches={:?}",
        executed.resolved_mode,
        executed.route,
        executed.aggregate_allowed_vectors,
        executed.work_units,
        executed.rows_examined,
        executed.source_work,
        executed.touches,
    );
}

fn assert_same_cost(scope: usize, label: &str, arm: &Executed, indexed: &Executed) {
    assert_eq!(
        arm.resolved_mode,
        Some(contextdb_core::VectorSearchMode::Indexed),
        "{label} at scope {scope} must resolve to the maintained graph on this fixture"
    );
    assert_eq!(
        arm.route, indexed.route,
        "{label} at scope {scope} takes the route INDEXED takes"
    );
    assert_eq!(
        arm.rows, indexed.rows,
        "{label} at scope {scope} returns the answer INDEXED returns"
    );
    assert_eq!(
        arm.rows_examined, indexed.rows_examined,
        "{label} at scope {scope} examines the rows INDEXED examines: {} vs {}",
        arm.rows_examined, indexed.rows_examined
    );
    assert_eq!(
        arm.source_work, indexed.source_work,
        "{label} at scope {scope} charges the same work to the same sources as INDEXED"
    );
    assert_eq!(
        arm.touches, indexed.touches,
        "{label} at scope {scope} reaches the same source items as INDEXED, kind for kind"
    );
    assert_eq!(
        arm.work_units, indexed.work_units,
        "{label} at scope {scope} charges the total INDEXED charges: {} vs {}",
        arm.work_units, indexed.work_units
    );
}

#[test]
fn auto_that_resolves_to_the_graph_costs_what_indexed_costs() {
    let db = fixture();
    for scope in [1_usize, 3, PARTITIONS] {
        let declared = execute(&db, scope, None);
        let auto = execute(&db, scope, Some("AUTO"));
        let indexed = execute(&db, scope, Some("INDEXED"));
        report(scope, "DECLARED", &declared);
        report(scope, "AUTO", &auto);
        report(scope, "INDEXED", &indexed);
        if scope == PARTITIONS {
            let exact = execute(&db, scope, Some("EXACT"));
            report(scope, "EXACT", &exact);
            for (label, arm) in [("declared", &declared), ("explicit", &auto)] {
                assert_eq!(
                    arm.route, exact.route,
                    "{label} AUTO chooses cheaper exact work at scope {scope}"
                );
                assert_eq!(
                    arm.rows, exact.rows,
                    "{label} AUTO returns the exact answer"
                );
                assert_eq!(arm.rows_examined, exact.rows_examined);
                assert_eq!(arm.source_work, exact.source_work);
                assert_eq!(arm.touches, exact.touches);
                assert_eq!(arm.work_units, exact.work_units);
            }
            continue;
        }
        assert_eq!(
            indexed.resolved_mode,
            Some(contextdb_core::VectorSearchMode::Indexed),
            "an explicit INDEXED request resolves to the graph"
        );
        assert_eq!(
            indexed.rows.len(),
            10,
            "the fixture is wide enough for a full page of hits"
        );
        // The declared arm is the statement the consumer writes -- no `USE
        // VECTOR` clause, the column's own `SEARCH_MODE AUTO`.
        assert_same_cost(scope, "a declared AUTO", &declared, &indexed);
        assert_same_cost(scope, "an explicit AUTO", &auto, &indexed);
    }
}
