#![allow(dead_code)] // Existing compatibility and test seam shape.
use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::database::VectorSearchTraceObservation;
use contextdb_engine::{
    Database, MaintenancePolicy, QueryResult, VectorSearchResidual, VectorSearchRoute,
};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use uuid::Uuid;

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 1_250;
const LIVE_ROWS_PER_PARTITION: usize = ROWS_PER_PARTITION / 2;

struct Counter(AtomicU64);

impl HnswCandidateObserver for Counter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeVectorScoreObserver for Counter {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ScoredQuery {
    result: QueryResult,
    graph_scores: u64,
    native_scores: u64,
    debug: VectorSearchTraceObservation,
}

impl ScoredQuery {
    fn total_scores(&self) -> u64 {
        self.graph_scores.saturating_add(self.native_scores)
    }
}

fn execute_and_count_scoring(
    db: &Database,
    sql: &str,
    query: &HashMap<String, Value>,
) -> ScoredQuery {
    let graph_scores = Arc::new(Counter(AtomicU64::new(0)));
    let native_scores = Arc::new(Counter(AtomicU64::new(0)));
    db.__reset_last_query_vector_trace_for_test();
    let result = with_hnsw_candidate_observer(graph_scores.clone(), || {
        with_native_vector_score_observer_for_test(native_scores.clone(), || {
            db.execute(sql, query).unwrap()
        })
    });
    let debug = {
        let seam_read_memory = db
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        db.__take_last_query_vector_trace_for_test(seam_read_memory)
            .expect("read the admitted trace observation")
    }
    .expect("every vector query publishes its real route receipt");
    let graph_scores = graph_scores.0.load(Ordering::SeqCst);
    let native_scores = native_scores.0.load(Ordering::SeqCst);
    assert!(
        !debug.used_hnsw || graph_scores > 0,
        "a graph route must expose its actual graph distance work"
    );
    assert!(
        debug.used_hnsw || native_scores > 0,
        "an exact route must expose its actual native-vector score work"
    );
    ScoredQuery {
        result,
        graph_scores,
        native_scores,
        debug,
    }
}

#[test]
fn native_score_observer_counts_each_exact_comparison_once() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observer_control (id INT PRIMARY KEY, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .expect("create the exact-observer positive-control table");
    for (id, embedding) in [
        (1_i64, vec![1.0, 0.0]),
        (2, vec![0.5, 0.5]),
        (3, vec![0.0, 1.0]),
    ] {
        db.execute(
            "INSERT INTO observer_control VALUES ($id, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("embedding".into(), Value::Vector(embedding)),
            ]),
        )
        .expect("seed the exact-observer positive-control row");
    }

    let observed = execute_and_count_scoring(
        &db,
        "SELECT id FROM observer_control ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 3",
        &HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]),
    );
    assert!(!observed.debug.used_hnsw);
    assert_eq!(observed.result.rows.len(), 3);
    assert_eq!(observed.graph_scores, 0);
    assert_eq!(
        observed.native_scores, 3,
        "the native observer records one score for each exact comparison without double-counting"
    );
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC046_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC046_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn bind_uuid_values(
    params: &mut HashMap<String, Value>,
    column: &str,
    prefix: &str,
    values: impl IntoIterator<Item = Uuid>,
) -> String {
    let values = values.into_iter().collect::<BTreeSet<_>>();
    assert!(!values.is_empty());
    let mut placeholders = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let name = format!("{prefix}_{index}");
        params.insert(name.clone(), Value::Uuid(value));
        placeholders.push(format!("${name}"));
    }
    if placeholders.len() == 1 {
        format!("{column} = {}", placeholders[0])
    } else {
        format!("{column} IN ({})", placeholders.join(", "))
    }
}

fn scoped_statement(partitions: usize, mode: &str) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([("is_current".into(), Value::Bool(true))]);
    let contexts = bind_uuid_values(
        &mut params,
        "context_id",
        "search_context",
        (0..partitions).map(context_id),
    );
    let generations = bind_uuid_values(
        &mut params,
        "generation_id",
        "search_generation",
        (0..partitions).map(generation_id),
    );
    params.insert(
        "query".into(),
        Value::Vector(maintained_search::held_out_vector(0)),
    );
    (
        format!(
            "SELECT id FROM indexed_vectors WHERE {contexts} AND {generations} AND \
             is_current = $is_current ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        params,
    )
}

fn set_probe(params: &mut HashMap<String, Value>, probe: usize) {
    params.insert(
        "query".into(),
        Value::Vector(maintained_search::held_out_vector(probe)),
    );
}

fn overlap_at_ten(indexed: &QueryResult, exact: &QueryResult) -> u64 {
    u64::try_from(
        indexed
            .rows
            .iter()
            .filter(|row| exact.rows.contains(row))
            .count(),
    )
    .unwrap()
}

fn assert_only_current_rows(result: &QueryResult) {
    for row in &result.rows {
        let [Value::Int64(id)] = row.as_slice() else {
            panic!("expected one integer corpus id, got {row:?}");
        };
        let ordinal = usize::try_from(id - 1).expect("frozen ids are positive");
        assert!(
            ordinal % ROWS_PER_PARTITION < LIVE_ROWS_PER_PARTITION,
            "the residual is_current filter must exclude corpus row {id}"
        );
    }
}

fn fixture() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE indexed_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
         embedding VECTOR(768) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX indexed_vectors_active_idx ON indexed_vectors \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .unwrap();
    for ordinal in 0..maintained_search::CORPUS_ROWS {
        let partition = maintained_search::corpus_partition(ordinal) as usize;
        db.execute(
            "INSERT INTO indexed_vectors VALUES \
             ($id, $context, $generation, $current, $embedding)",
            &HashMap::from([
                (
                    "id".into(),
                    Value::Int64(maintained_search::corpus_id(ordinal)),
                ),
                ("context".into(), Value::Uuid(context_id(partition))),
                ("generation".into(), Value::Uuid(generation_id(partition))),
                (
                    "current".into(),
                    Value::Bool(ordinal % ROWS_PER_PARTITION < LIVE_ROWS_PER_PARTITION),
                ),
                (
                    "embedding".into(),
                    Value::Vector(maintained_search::corpus_vector(ordinal)),
                ),
            ]),
        )
        .unwrap();
    }
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == PARTITIONS && status.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("maintenance did not publish all eight frozen pair partitions")
}

fn single_context_fixture() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE single_context_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
         embedding VECTOR(768) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX single_context_vectors_active_idx ON single_context_vectors \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .unwrap();
    for ordinal in 0..maintained_search::CORPUS_ROWS {
        db.execute(
            "INSERT INTO single_context_vectors VALUES \
             ($id, $context, $generation, TRUE, $embedding)",
            &HashMap::from([
                (
                    "id".into(),
                    Value::Int64(maintained_search::corpus_id(ordinal)),
                ),
                ("context".into(), Value::Uuid(context_id(0))),
                ("generation".into(), Value::Uuid(generation_id(0))),
                (
                    "embedding".into(),
                    Value::Vector(maintained_search::corpus_vector(ordinal)),
                ),
            ]),
        )
        .unwrap();
    }
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 1 && status.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("maintenance did not publish the frozen single-context pair partition")
}

fn assert_scoped_trace(result: &QueryResult, partitions: usize) -> bool {
    let disclosure = result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search reports its resolved route");
    assert_eq!(
        disclosure.partition_key_columns,
        vec!["context_id".to_string(), "generation_id".to_string()]
    );
    match disclosure.route {
        Some(VectorSearchRoute::Exact) => {
            assert_eq!(disclosure.residual, VectorSearchResidual::None)
        }
        Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed) => {
            assert_eq!(disclosure.residual, VectorSearchResidual::Bounded)
        }
        None => panic!("an executed query cannot retain an unresolved vector route"),
    }
    assert_eq!(
        disclosure.aggregate_allowed_vectors,
        Some(partitions * LIVE_ROWS_PER_PARTITION),
        "the disclosed allowed set excludes the non-current half"
    );
    assert_eq!(
        disclosure.partition_hnsw.len(),
        partitions,
        "the two IN lists name only the real Context/generation pairs; invalid cross-pairs are empty"
    );
    disclosure
        .partition_hnsw
        .iter()
        .any(|partition| ROWS_PER_PARTITION > partition.hnsw_ef_search)
}

fn assert_recall(cell: &str, matched: u64) {
    let possible = u64::try_from(maintained_search::HELD_OUT_ROWS * 10).unwrap();
    assert!(
        matched * 100 >= possible * 95,
        "{cell} recall@10 over all 100 held-out queries must be at least .95: {matched}/{possible}"
    );
}

/// Timing rounds per query and statement. Each statement's latency for a
/// query is its fastest round.
#[cfg(feature = "nightly-tests")]
const TIMING_ROUNDS: usize = 3;

/// Wall time of one `execute`, parsing and planning included, without any
/// scoring observer installed.
#[cfg(feature = "nightly-tests")]
fn timed_execute(db: &Database, sql: &str, params: &HashMap<String, Value>) -> u128 {
    let started = std::time::Instant::now();
    let result = db.execute(sql, params).unwrap();
    let micros = started.elapsed().as_micros();
    drop(result);
    micros
}

#[cfg(feature = "nightly-tests")]
fn percentile<T: Copy + Ord>(values: &[T], percent: usize) -> T {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * percent).div_ceil(100).max(1) - 1]
}

#[cfg(feature = "nightly-tests")]
fn p95<T: Copy + Ord>(values: &[T]) -> T {
    percentile(values, 95)
}

#[cfg(feature = "nightly-tests")]
fn spread<T: Copy + Ord + std::fmt::Debug>(values: &[T]) -> String {
    if values.is_empty() {
        return "none".to_string();
    }
    format!(
        "p50={:?} p95={:?} max={:?}",
        percentile(values, 50),
        p95(values),
        percentile(values, 100)
    )
}

/// Same-process, interleaved, observer-free latency of several statements
/// over the frozen held-out queries. Per query, every round runs each
/// statement once, in an order rotated by query and round. `raw[s][q][r]` is
/// statement `s`'s round `r` on query `q`; `best[s][q]` is its fastest round,
/// the per-query estimator every acceptance comparison below uses.
#[cfg(feature = "nightly-tests")]
struct Latencies {
    raw: Vec<Vec<[u128; TIMING_ROUNDS]>>,
    best: Vec<Vec<u128>>,
}

#[cfg(feature = "nightly-tests")]
impl Latencies {
    fn measure(
        db: &Database,
        statements: &[&str],
        params: &mut HashMap<String, Value>,
        set_query: impl Fn(&mut HashMap<String, Value>, usize),
    ) -> Self {
        let count = statements.len();
        let mut raw = vec![Vec::new(); count];
        for probe in 0..maintained_search::HELD_OUT_ROWS {
            set_query(params, probe);
            let mut rounds = vec![[0_u128; TIMING_ROUNDS]; count];
            // The round also rotates the order, so it is an index, not an item.
            #[allow(clippy::needless_range_loop)]
            for round in 0..TIMING_ROUNDS {
                for slot in 0..count {
                    let which = (slot + probe + round) % count;
                    rounds[which][round] = timed_execute(db, statements[which], params);
                }
            }
            for (statement, samples) in rounds.into_iter().enumerate() {
                raw[statement].push(samples);
            }
        }
        let best = raw
            .iter()
            .map(|per_query| {
                per_query
                    .iter()
                    .map(|samples| *samples.iter().min().unwrap())
                    .collect()
            })
            .collect();
        Self { raw, best }
    }

    /// The measured noise bound: the p95, over queries, of the gap between
    /// the two EXACT runs of the same query, under the same estimator.
    fn noise_bound(&self, exact: usize, control: usize) -> u128 {
        let gaps: Vec<u128> = self.best[exact]
            .iter()
            .zip(&self.best[control])
            .map(|(left, right)| left.abs_diff(*right))
            .collect();
        p95(&gaps)
    }

    /// Round-paired differences `left - right`, every query and round, for
    /// the report.
    fn paired_differences(&self, left: usize, right: usize) -> Vec<i128> {
        self.raw[left]
            .iter()
            .zip(&self.raw[right])
            .flat_map(|(left, right)| {
                left.iter()
                    .zip(right)
                    .map(|(left, right)| *left as i128 - *right as i128)
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

/// One frozen multi-Context cell searched in `mode` and in EXACT: work,
/// recall and paired latency, all measured before anything is asserted.
#[cfg(feature = "nightly-tests")]
struct CellRun {
    partitions: usize,
    mode: &'static str,
    mode_work: Vec<u64>,
    exact_work: Vec<u64>,
    graph_queries: u64,
    matched: u64,
    low_recall: Vec<(usize, u64)>,
    not_cheaper: Vec<usize>,
    latencies: Latencies,
}

#[cfg(feature = "nightly-tests")]
impl CellRun {
    const MODE: usize = 0;
    const EXACT: usize = 1;
    const CONTROL: usize = 2;

    fn mode_total(&self) -> u64 {
        self.mode_work.iter().sum()
    }

    fn exact_total(&self) -> u64 {
        self.exact_work.iter().sum()
    }

    fn mode_p95(&self) -> u128 {
        p95(&self.latencies.best[Self::MODE])
    }

    fn exact_p95(&self) -> u128 {
        p95(&self.latencies.best[Self::EXACT])
    }

    fn noise_bound(&self) -> u128 {
        self.latencies.noise_bound(Self::EXACT, Self::CONTROL)
    }

    fn report(&self) {
        let not_cheaper_latency: Vec<u128> = self
            .not_cheaper
            .iter()
            .map(|probe| self.latencies.best[Self::MODE][*probe])
            .collect();
        println!(
            "wall_search_indexed partitions={} mode={} probes={} matched={} mode_scores={} exact_scores={} mode_mean={:.1} exact_mean={:.1} graph_queries={} mode_latency_us {} exact_latency_us {} noise_bound_us={} round_paired_mode_minus_exact_us {} round_paired_exact_minus_exact_us {} equal_or_greater_work_queries={} their_mode_latency_us {} low_recall={:?}",
            self.partitions,
            self.mode,
            maintained_search::HELD_OUT_ROWS,
            self.matched,
            self.mode_total(),
            self.exact_total(),
            self.mode_total() as f64 / self.mode_work.len() as f64,
            self.exact_total() as f64 / self.exact_work.len() as f64,
            self.graph_queries,
            spread(&self.latencies.best[Self::MODE]),
            spread(&self.latencies.best[Self::EXACT]),
            self.noise_bound(),
            spread(&self.latencies.paired_differences(Self::MODE, Self::EXACT)),
            spread(
                &self
                    .latencies
                    .paired_differences(Self::EXACT, Self::CONTROL)
            ),
            self.not_cheaper.len(),
            spread(&not_cheaper_latency),
            self.low_recall,
        );
        for probe in &self.not_cheaper {
            println!(
                "wall_search_indexed partitions={} mode={} equal_or_greater_work probe={probe} mode_scores={} exact_scores={} mode_latency_us={} exact_latency_us={}",
                self.partitions,
                self.mode,
                self.mode_work[*probe],
                self.exact_work[*probe],
                self.latencies.best[Self::MODE][*probe],
                self.latencies.best[Self::EXACT][*probe],
            );
        }
    }

    /// The graph-using bar: strictly less mean work than EXACT and a p95
    /// latency no greater than EXACT's. Returns every way the cell misses it.
    fn graph_bar_misses(&self) -> Vec<String> {
        let mut misses = Vec::new();
        if self.mode_total() >= self.exact_total() {
            misses.push(format!(
                "{}-pair {}: mean work is not strictly below EXACT ({} >= {})",
                self.partitions,
                self.mode,
                self.mode_total(),
                self.exact_total()
            ));
        }
        if self.mode_p95() > self.exact_p95() {
            misses.push(format!(
                "{}-pair {}: p95 latency above EXACT ({}us > {}us)",
                self.partitions,
                self.mode,
                self.mode_p95(),
                self.exact_p95()
            ));
        }
        misses
    }
}

/// Run one frozen multi-Context cell in `mode` and in EXACT. Route-shape
/// facts every query must hold (current rows only, EXACT scoring each current
/// row once, the disclosed scope) are asserted as they are observed; the
/// acceptance bars are left to the caller.
#[cfg(feature = "nightly-tests")]
fn run_cell(db: &Database, partitions: usize, mode: &'static str) -> CellRun {
    let (mode_sql, mut params) = scoped_statement(partitions, mode);
    let exact_sql = mode_sql.replace(&format!("USE VECTOR {mode}"), "USE VECTOR EXACT");
    let mut run = CellRun {
        partitions,
        mode,
        mode_work: Vec::new(),
        exact_work: Vec::new(),
        graph_queries: 0,
        matched: 0,
        low_recall: Vec::new(),
        not_cheaper: Vec::new(),
        latencies: Latencies {
            raw: Vec::new(),
            best: Vec::new(),
        },
    };
    let mut strict_required_queries = 0_u64;
    for probe in 0..maintained_search::HELD_OUT_ROWS {
        set_probe(&mut params, probe);
        let searched = execute_and_count_scoring(db, &mode_sql, &params);
        let exact = execute_and_count_scoring(db, &exact_sql, &params);
        assert_only_current_rows(&searched.result);
        assert_only_current_rows(&exact.result);
        assert_eq!(
            exact.native_scores,
            u64::try_from(partitions * LIVE_ROWS_PER_PARTITION).unwrap(),
            "EXACT scores every current vector in this search cell exactly once"
        );
        assert_eq!(exact.graph_scores, 0);
        if assert_scoped_trace(&searched.result, partitions) {
            strict_required_queries += 1;
        }
        let disclosure = searched
            .result
            .trace
            .vector_search
            .as_ref()
            .expect("the executed search reports its resolved route");
        if mode == "AUTO"
            && partitions * LIVE_ROWS_PER_PARTITION < disclosure.effective_auto_index_at
        {
            assert_eq!(
                disclosure.route,
                Some(VectorSearchRoute::Exact),
                "a scope below AUTO_INDEX_AT is compared exactly"
            );
            assert_eq!(
                searched.graph_scores, 0,
                "a scope below AUTO_INDEX_AT does no graph work"
            );
        }
        if searched.debug.used_hnsw {
            run.graph_queries += 1;
            assert!(
                matches!(
                    disclosure.route,
                    Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed)
                ),
                "a graph walk must be visible in the public route receipt"
            );
        }
        if searched.total_scores() >= exact.total_scores() {
            run.not_cheaper.push(probe);
        }
        let query_matched = overlap_at_ten(&searched.result, &exact.result);
        if query_matched * 100 < 10 * 95 {
            run.low_recall.push((probe, query_matched));
        }
        run.matched += query_matched;
        run.mode_work.push(searched.total_scores());
        run.exact_work.push(exact.total_scores());
    }
    assert!(
        strict_required_queries == 0 || strict_required_queries == 100,
        "the fixed cell's per-partition breadth cannot vary by query"
    );
    run.latencies = Latencies::measure(
        db,
        &[&mode_sql, &exact_sql, &exact_sql],
        &mut params,
        set_probe,
    );
    run.report();
    run
}

#[cfg(feature = "nightly-tests")]
#[test]
fn indexed_route_does_less_work_than_exact() {
    // The intent's multi-Context bar for AUTO. A scope whose current rows are
    // below the declared AUTO_INDEX_AT is compared exactly, with no graph
    // work. Where AUTO uses the graph it must do strictly less MEAN distance
    // work than EXACT, and its p95 LATENCY is at most EXACT's. Where AUTO
    // compares every row it IS the exact route: its work equals EXACT's, and
    // its p95 latency is within the measured noise bound of EXACT's.
    //
    // Latency: same process, interleaved and rotated per query and round,
    // parsing and planning included, no scoring observer; a query's latency
    // is its fastest of TIMING_ROUNDS rounds, and p95 is taken over the 100
    // held-out queries. The noise bound is the p95, over the same queries and
    // under the same estimator, of the gap between two EXACT runs of each
    // query. Queries with equal or greater work are reported with their
    // latency, not forbidden. Every query keeps recall@10 of at least .95
    // against EXACT. The bar is wall-clock by nature; a failure under host
    // load is re-run alone before it is called a defect.
    let db = fixture();
    for partitions in [1_usize, 3, 8] {
        let run = run_cell(&db, partitions, "AUTO");
        assert_recall(&format!("AUTO {partitions}-pair cell"), run.matched);
        assert!(
            run.low_recall.is_empty(),
            "every query in the AUTO {partitions}-pair cell keeps recall@10 of at least .95; (probe, matched/10): {:?}",
            run.low_recall
        );
        if run.graph_queries > 0 {
            let misses = run.graph_bar_misses();
            assert!(misses.is_empty(), "{misses:#?}");
        } else {
            assert_eq!(
                run.mode_work, run.exact_work,
                "an all-exact AUTO cell does exactly EXACT's distance work, query by query"
            );
            let gap = run.mode_p95().abs_diff(run.exact_p95());
            assert!(
                gap <= run.noise_bound(),
                "an all-exact AUTO {partitions}-pair cell's p95 latency is within the measured noise of EXACT's: |{} - {}| = {gap}us > {}us",
                run.mode_p95(),
                run.exact_p95(),
                run.noise_bound()
            );
        }
    }
}

#[cfg(feature = "nightly-tests")]
#[test]
fn forced_indexed_multi_context_cells_meet_the_graph_bar() {
    // The same frozen scopes and held-out queries, forced onto the graph with
    // INDEXED, under the graph-using bar: strictly less MEAN distance work
    // than EXACT, p95 latency at most EXACT's (measured as in the AUTO test
    // above), and recall@10 of at least .95 on every query. Every cell is
    // measured and reported before any miss is asserted, so each failing cell
    // is named separately.
    //
    // The bar is the intent's (bounded-storage intent, acceptance row "Index
    // acceleration"): "graph-using multi-Context scopes have p95 <= EXACT",
    // while the single-Context bar is the mandatory 10,000-row cell, asserted
    // by `single_context_saved_graph_auto_and_indexed_are_faster_than_exact`.
    // The 1-Context cell here is not a multi-Context scope, so it is reported
    // (work, p95 latency, recall) and not asserted.
    let db = fixture();
    let mut misses = Vec::new();
    for partitions in [1_usize, 3, 8] {
        let run = run_cell(&db, partitions, "INDEXED");
        assert_eq!(
            run.graph_queries,
            u64::try_from(maintained_search::HELD_OUT_ROWS).unwrap(),
            "forced INDEXED walks the graph for every query"
        );
        if partitions == 1 {
            println!(
                "wall_search_indexed partitions=1 mode=INDEXED reported_not_asserted mean_work_below_exact={} p95_at_most_exact={} low_recall={:?}",
                run.mode_total() < run.exact_total(),
                run.mode_p95() <= run.exact_p95(),
                run.low_recall,
            );
            continue;
        }
        if !run.low_recall.is_empty() {
            misses.push(format!(
                "{partitions}-pair INDEXED: recall@10 below .95 for (probe, matched/10) {:?}",
                run.low_recall
            ));
        }
        misses.extend(run.graph_bar_misses());
    }
    assert!(misses.is_empty(), "{misses:#?}");
}

#[cfg(feature = "nightly-tests")]
#[test]
fn declared_flat_multi_context_cells_are_never_slower_than_exact() {
    // INDEXED's maintained flat route (BEHAVIOR CHANGE "INDEXED maintained
    // flat route"; the user promise: the index is never slower than checking
    // every passage). The frozen scopes and held-out queries, with FLAT_BELOW
    // declared on the column after the frozen build, so the fixture data are
    // untouched. Each partition's route is expected from that declaration and
    // the frozen live count, not from what the search reports: 1,250 live
    // rows per partition < 1,251, so every partition is flat. An all-flat
    // INDEXED scope does EXACT's work, keeps every query at 10/10, and its
    // p95 latency is at most EXACT's, paired and interleaved, planning
    // included -- the AUTO all-exact noise allowance does not apply to it.
    const FLAT_BELOW: usize = ROWS_PER_PARTITION + 1;
    let db = fixture();
    db.execute(
        &format!("ALTER TABLE indexed_vectors ALTER COLUMN embedding SET FLAT_BELOW {FLAT_BELOW}"),
        &HashMap::new(),
    )
    .expect("declare FLAT_BELOW on the frozen column");
    let mut misses = Vec::new();
    for partitions in [3_usize, 8] {
        let expected_flat = ROWS_PER_PARTITION < FLAT_BELOW;
        assert!(
            expected_flat,
            "the declared threshold covers every frozen partition"
        );
        let (sql, mut params) = scoped_statement(partitions, "INDEXED");
        set_probe(&mut params, 0);
        let routes: Vec<_> = db
            .execute(&sql, &params)
            .expect("the declared flat INDEXED search executes")
            .trace
            .vector_search
            .expect("the executed search reports its route")
            .partition_hnsw
            .into_iter()
            .map(|partition| (partition.route, partition.reason))
            .collect();
        assert_eq!(
            routes,
            vec![
                (
                    Some(VectorSearchRoute::Exact),
                    Some(contextdb_vector::mem::PARTITION_BELOW_FLAT_THRESHOLD_REASON.to_owned())
                );
                partitions
            ],
            "every frozen partition is served by its maintained flat route"
        );
        let run = run_cell(&db, partitions, "INDEXED");
        if run.graph_queries != 0 {
            misses.push(format!("{partitions}-pair flat INDEXED walked a graph"));
        }
        if run.mode_work != run.exact_work {
            misses.push(format!(
                "{partitions}-pair flat INDEXED work differs from EXACT's ({} vs {})",
                run.mode_total(),
                run.exact_total()
            ));
        }
        if !run.low_recall.is_empty() {
            misses.push(format!(
                "{partitions}-pair flat INDEXED recall below 10/10: {:?}",
                run.low_recall
            ));
        }
        if run.mode_p95() > run.exact_p95() {
            misses.push(format!(
                "{partitions}-pair flat INDEXED p95 latency above EXACT ({}us > {}us)",
                run.mode_p95(),
                run.exact_p95()
            ));
        }
    }
    assert!(misses.is_empty(), "{misses:#?}");
}

#[cfg(feature = "nightly-tests")]
#[test]
fn single_context_saved_graph_auto_and_indexed_are_faster_than_exact() {
    // The mandatory 10,000-row single-Context bar, per graph mode: strictly
    // less MEAN distance work than EXACT, a strictly lower p95 latency than
    // EXACT's (measured as in the multi-Context test, with a second EXACT run
    // as the reported noise control), recall@10 of at least .95 on every
    // query, and every query with equal or greater work reported with its
    // latency.
    let single = single_context_fixture();
    let mut params = HashMap::from([
        ("context".into(), Value::Uuid(context_id(0))),
        ("generation".into(), Value::Uuid(generation_id(0))),
        ("is_current".into(), Value::Bool(true)),
        (
            "query".into(),
            Value::Vector(maintained_search::held_out_vector(0)),
        ),
    ]);
    let prefix = "SELECT id FROM single_context_vectors WHERE context_id = $context AND \
                  generation_id = $generation AND is_current = $is_current \
                  ORDER BY embedding <=> $query USE VECTOR";
    let auto_sql = format!("{prefix} AUTO LIMIT 10");
    let indexed_sql = format!("{prefix} INDEXED LIMIT 10");
    let exact_sql = format!("{prefix} EXACT LIMIT 10");
    let mut auto_work = Vec::new();
    let mut indexed_work = Vec::new();
    let mut exact_work = Vec::new();
    let mut auto_matched = 0_u64;
    let mut indexed_matched = 0_u64;
    let mut low_recall = Vec::new();
    let mut observed_graph_scores = 0_u64;
    for probe in 0..maintained_search::HELD_OUT_ROWS {
        set_probe(&mut params, probe);
        let auto = execute_and_count_scoring(&single, &auto_sql, &params);
        let indexed = execute_and_count_scoring(&single, &indexed_sql, &params);
        let exact = execute_and_count_scoring(&single, &exact_sql, &params);
        assert_eq!(exact.native_scores, maintained_search::CORPUS_ROWS as u64);
        assert_eq!(exact.graph_scores, 0);
        for (label, observed) in [("AUTO", &auto), ("INDEXED", &indexed)] {
            assert!(
                observed.debug.used_hnsw,
                "{label} must use the 10,000-row graph"
            );
            assert!(
                observed.graph_scores > 0,
                "{label}'s graph observer is a positive control"
            );
            let query_matched = overlap_at_ten(&observed.result, &exact.result);
            if query_matched * 100 < 10 * 95 {
                low_recall.push((label, probe, query_matched));
            }
        }
        auto_matched += overlap_at_ten(&auto.result, &exact.result);
        indexed_matched += overlap_at_ten(&indexed.result, &exact.result);
        auto_work.push(auto.total_scores());
        indexed_work.push(indexed.total_scores());
        exact_work.push(exact.total_scores());
        observed_graph_scores = observed_graph_scores
            .saturating_add(auto.graph_scores)
            .saturating_add(indexed.graph_scores);
    }
    const AUTO: usize = 0;
    const INDEXED: usize = 1;
    const EXACT: usize = 2;
    const CONTROL: usize = 3;
    let latencies = Latencies::measure(
        &single,
        &[&auto_sql, &indexed_sql, &exact_sql, &exact_sql],
        &mut params,
        set_probe,
    );
    let exact_total = exact_work.iter().sum::<u64>();
    let exact_p95 = p95(&latencies.best[EXACT]);
    let mut misses = Vec::new();
    for (label, work, column) in [
        ("AUTO", &auto_work, AUTO),
        ("INDEXED", &indexed_work, INDEXED),
    ] {
        let total = work.iter().sum::<u64>();
        let mode_p95 = p95(&latencies.best[column]);
        let not_cheaper: Vec<usize> = (0..work.len())
            .filter(|probe| work[*probe] >= exact_work[*probe])
            .collect();
        let not_cheaper_latency: Vec<u128> = not_cheaper
            .iter()
            .map(|probe| latencies.best[column][*probe])
            .collect();
        println!(
            "wall_search_indexed single_rows={} mode={label} probes={} mode_scores={total} exact_scores={exact_total} mode_mean={:.1} exact_mean={:.1} mode_latency_us {} exact_latency_us {} noise_bound_us={} round_paired_mode_minus_exact_us {} round_paired_exact_minus_exact_us {} equal_or_greater_work_queries={} probes={not_cheaper:?} their_mode_latency_us {}",
            maintained_search::CORPUS_ROWS,
            maintained_search::HELD_OUT_ROWS,
            total as f64 / work.len() as f64,
            exact_total as f64 / exact_work.len() as f64,
            spread(&latencies.best[column]),
            spread(&latencies.best[EXACT]),
            latencies.noise_bound(EXACT, CONTROL),
            spread(&latencies.paired_differences(column, EXACT)),
            spread(&latencies.paired_differences(EXACT, CONTROL)),
            not_cheaper.len(),
            spread(&not_cheaper_latency),
        );
        if total >= exact_total {
            misses.push(format!(
                "{label}: mean work is not strictly below EXACT ({total} >= {exact_total})"
            ));
        }
        if mode_p95 >= exact_p95 {
            misses.push(format!(
                "{label}: p95 latency is not strictly below EXACT ({mode_p95}us >= {exact_p95}us)"
            ));
        }
    }
    println!(
        "wall_search_indexed single_rows={} probes={} auto_matched={auto_matched} indexed_matched={indexed_matched} graph_scores={observed_graph_scores} low_recall={low_recall:?}",
        maintained_search::CORPUS_ROWS,
        maintained_search::HELD_OUT_ROWS,
    );
    assert_recall("AUTO mandatory single-10,000 cell", auto_matched);
    assert_recall("INDEXED mandatory single-10,000 cell", indexed_matched);
    assert!(
        low_recall.is_empty(),
        "every query keeps recall@10 of at least .95 in the mandatory 10,000-row cell; (mode, probe, matched/10): {low_recall:?}"
    );
    assert!(observed_graph_scores > 0);
    assert!(misses.is_empty(), "{misses:#?}");
}
