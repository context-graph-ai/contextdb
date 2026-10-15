#![allow(dead_code)] // Existing compatibility and test seam shape.
use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{
    Database, MaintenancePolicy, QueryResult, VectorSearchResidual, VectorSearchRoute,
};
use contextdb_vector::VectorSearchDebugTrace;
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
    debug: VectorSearchDebugTrace,
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
    let debug = db
        .__take_last_query_vector_trace_for_test()
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

#[cfg(feature = "nightly-tests")]
#[test]
#[ignore = "CDB-161: bounded-storage-rc2 S5 AUTO calibration campaign (fix SHA pending); re-enabled when rc2 lands on dev."]
fn indexed_route_does_less_work_than_exact() {
    let db = fixture();
    for partitions in [1_usize, 3, 8] {
        let (auto_sql, mut auto_params) = scoped_statement(partitions, "AUTO");
        let exact_sql = auto_sql.replace("AUTO", "EXACT");
        let mut auto_evaluations = 0_u64;
        let mut exact_evaluations = 0_u64;
        let mut matched = 0_u64;
        let mut strict_required_queries = 0_u64;
        for probe in 0..maintained_search::HELD_OUT_ROWS {
            set_probe(&mut auto_params, probe);
            let auto = execute_and_count_scoring(&db, &auto_sql, &auto_params);
            let exact = execute_and_count_scoring(&db, &exact_sql, &auto_params);
            assert_only_current_rows(&auto.result);
            assert_only_current_rows(&exact.result);
            assert_eq!(
                exact.native_scores,
                u64::try_from(partitions * LIVE_ROWS_PER_PARTITION).unwrap(),
                "EXACT scores every current vector in this search cell exactly once"
            );
            assert_eq!(exact.graph_scores, 0);
            assert!(
                auto.total_scores() <= exact.total_scores(),
                "AUTO cannot score more candidates than EXACT in any frozen scope"
            );
            let strict_required = assert_scoped_trace(&auto.result, partitions);
            if strict_required {
                strict_required_queries += 1;
                assert!(
                    auto.debug.used_hnsw,
                    "a live partition wider than its disclosed EF_SEARCH must traverse its graph"
                );
                assert!(
                    matches!(
                        auto.result
                            .trace
                            .vector_search
                            .as_ref()
                            .and_then(|d| d.route),
                        Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed)
                    ),
                    "a required graph walk must be visible in the public route receipt"
                );
                assert!(
                    auto.total_scores() < exact.total_scores(),
                    "graph work is strictly smaller only when live rows exceed disclosed EF_SEARCH"
                );
            }
            matched += overlap_at_ten(&auto.result, &exact.result);
            auto_evaluations += auto.total_scores();
            exact_evaluations += exact.total_scores();
        }
        assert_recall(&format!("AUTO {partitions}-pair cell"), matched);
        assert!(auto_evaluations <= exact_evaluations);
        assert!(
            strict_required_queries == 0 || strict_required_queries == 100,
            "the fixed cell's per-partition breadth cannot vary by query"
        );
        println!(
            "wall_search_indexed partitions={} probes={} matched={} auto_scores={} exact_scores={} strict_queries={}",
            partitions,
            maintained_search::HELD_OUT_ROWS,
            matched,
            auto_evaluations,
            exact_evaluations,
            strict_required_queries,
        );
    }
}

#[cfg(feature = "nightly-tests")]
#[test]
#[ignore = "CDB-162: bounded-storage-rc2 S5 per-candidate walk-cost campaign (fix SHA pending); re-enabled when rc2 lands on dev."]
fn single_context_saved_graph_auto_and_indexed_are_faster_than_exact() {
    let single = single_context_fixture();
    let mut auto_evaluations = 0_u64;
    let mut indexed_evaluations = 0_u64;
    let mut exact_evaluations = 0_u64;
    let mut auto_matched = 0_u64;
    let mut indexed_matched = 0_u64;
    let mut observed_graph_scores = 0_u64;
    for probe in 0..maintained_search::HELD_OUT_ROWS {
        let params = HashMap::from([
            ("context".into(), Value::Uuid(context_id(0))),
            ("generation".into(), Value::Uuid(generation_id(0))),
            ("is_current".into(), Value::Bool(true)),
            (
                "query".into(),
                Value::Vector(maintained_search::held_out_vector(probe)),
            ),
        ]);
        let prefix = "SELECT id FROM single_context_vectors WHERE context_id = $context AND \
                      generation_id = $generation AND is_current = $is_current \
                      ORDER BY embedding <=> $query USE VECTOR";
        let auto = execute_and_count_scoring(&single, &format!("{prefix} AUTO LIMIT 10"), &params);
        let indexed =
            execute_and_count_scoring(&single, &format!("{prefix} INDEXED LIMIT 10"), &params);
        let exact =
            execute_and_count_scoring(&single, &format!("{prefix} EXACT LIMIT 10"), &params);
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
            assert!(
                observed.total_scores() < exact.total_scores(),
                "{label} must score strictly fewer vectors than exact in the mandatory 10,000-row cell"
            );
        }
        auto_matched += overlap_at_ten(&auto.result, &exact.result);
        indexed_matched += overlap_at_ten(&indexed.result, &exact.result);
        auto_evaluations += auto.total_scores();
        indexed_evaluations += indexed.total_scores();
        exact_evaluations += exact.total_scores();
        observed_graph_scores = observed_graph_scores
            .saturating_add(auto.graph_scores)
            .saturating_add(indexed.graph_scores);
    }
    assert_recall("AUTO mandatory single-10,000 cell", auto_matched);
    assert_recall("INDEXED mandatory single-10,000 cell", indexed_matched);
    assert!(observed_graph_scores > 0);
    assert!(auto_evaluations < exact_evaluations);
    assert!(indexed_evaluations < exact_evaluations);
    println!(
        "wall_search_indexed single_rows={} probes={} auto={{matched:{},scores:{}}} indexed={{matched:{},scores:{}}} exact_scores={} graph_scores={}",
        maintained_search::CORPUS_ROWS,
        maintained_search::HELD_OUT_ROWS,
        auto_matched,
        auto_evaluations,
        indexed_matched,
        indexed_evaluations,
        exact_evaluations,
        observed_graph_scores,
    );
}
