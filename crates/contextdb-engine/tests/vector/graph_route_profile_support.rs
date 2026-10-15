use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use uuid::Uuid;

const PARTITIONS: usize = 3;
const PROBES: usize = 100;
const K: usize = 10;
// Include the query floor and the larger silent profile in the calibration.
const BREADTH_CANDIDATES: [usize; 9] = [10, 12, 16, 20, 24, 28, 32, 100, 400];

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

#[derive(Clone)]
struct QueryMeasurement {
    ids: Vec<i64>,
    work: u64,
    micros: u64,
    ef_search: usize,
    used_hnsw: bool,
}

#[derive(Clone)]
struct Baseline {
    queries: Vec<QueryMeasurement>,
    work: u64,
    p95_micros: u64,
}

#[derive(Debug, Clone, Copy)]
enum Scope {
    One,
    Several,
    All,
}

impl Scope {
    const ALL: [Self; 3] = [Self::One, Self::Several, Self::All];

    fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Several => "several",
            Self::All => "all",
        }
    }

    fn count(self) -> usize {
        match self {
            Self::One => 1,
            Self::Several => 2,
            Self::All => 3,
        }
    }
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xF320_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn result_ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("profile query expected one integer id, got {other:?}"),
        })
        .collect()
}

fn statement(scope: Scope, filter: Option<&str>, mode: &str) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::new();
    let scope_predicate = match scope {
        Scope::One => {
            params.insert("context_0".to_owned(), Value::Uuid(context_id(0)));
            Some("context_id = $context_0".to_owned())
        }
        Scope::Several => {
            params.insert("context_0".to_owned(), Value::Uuid(context_id(0)));
            params.insert("context_1".to_owned(), Value::Uuid(context_id(1)));
            Some("context_id IN ($context_0, $context_1)".to_owned())
        }
        Scope::All => None,
    };
    let predicate = scope_predicate
        .into_iter()
        .chain(filter.map(str::to_owned))
        .collect::<Vec<_>>()
        .join(" AND ");
    let where_clause = if predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {predicate}")
    };
    (
        format!(
            "SELECT id FROM profile_items{where_clause} ORDER BY embedding <=> $query \
             USE VECTOR {mode} LIMIT {K}"
        ),
        params,
    )
}

fn execute_and_measure(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,
) -> QueryMeasurement {
    let graph = Arc::new(Counter(AtomicU64::new(0)));
    let native = Arc::new(Counter(AtomicU64::new(0)));
    let started = Instant::now();
    let result = with_hnsw_candidate_observer(graph.clone(), || {
        with_native_vector_score_observer_for_test(native.clone(), || {
            db.execute(sql, params)
                .expect("execute one frozen profile query")
        })
    });
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let disclosure = result
        .trace
        .vector_search
        .as_ref()
        .expect("profile query publishes vector disclosure");
    let ef_search = disclosure
        .partition_hnsw
        .first()
        .map_or(0, |partition| partition.hnsw_ef_search);
    QueryMeasurement {
        ids: result_ids(&result),
        work: graph
            .0
            .load(Ordering::SeqCst)
            .saturating_add(native.0.load(Ordering::SeqCst)),
        micros,
        ef_search,
        used_hnsw: db.__debug_last_query_vector_used_hnsw_for_test(),
    }
}

fn percentile_95(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    values[(values.len() * 95).div_ceil(100).saturating_sub(1)]
}

fn exact_baseline(
    db: &Database,
    shape: &str,
    seed: usize,
    scope: Scope,
    filter: Option<&str>,
) -> Baseline {
    let (sql, mut params) = statement(scope, filter, "EXACT");
    let mut queries = Vec::with_capacity(PROBES);
    let mut work = 0_u64;
    let mut times = Vec::with_capacity(PROBES);
    for probe in 0..PROBES {
        params.insert(
            "query".to_owned(),
            Value::Vector(maintained_search::profile_vector(
                shape, seed, "held-out", probe,
            )),
        );
        let measured = execute_and_measure(db, &sql, &params);
        assert!(!measured.used_hnsw);
        work = work.saturating_add(measured.work);
        times.push(measured.micros);
        queries.push(measured);
    }
    Baseline {
        queries,
        work,
        p95_micros: percentile_95(&mut times),
    }
}

fn indexed_cell(
    db: &Database,
    shape: &str,
    seed: usize,
    scope: Scope,
    filter: Option<&str>,
    exact: &Baseline,
) -> (u64, u64, u64, usize) {
    let (sql, mut params) = statement(scope, filter, "INDEXED");
    let mut matched = 0_u64;
    let mut work = 0_u64;
    let mut times = Vec::with_capacity(PROBES);
    let mut observed_ef = None;
    for probe in 0..PROBES {
        params.insert(
            "query".to_owned(),
            Value::Vector(maintained_search::profile_vector(
                shape, seed, "held-out", probe,
            )),
        );
        let measured = execute_and_measure(db, &sql, &params);
        assert!(
            measured.used_hnsw,
            "INDEXED must execute the maintained graph"
        );
        observed_ef.get_or_insert(measured.ef_search);
        assert_eq!(observed_ef, Some(measured.ef_search));
        matched = matched.saturating_add(
            measured
                .ids
                .iter()
                .filter(|id| exact.queries[probe].ids.contains(id))
                .count() as u64,
        );
        work = work.saturating_add(measured.work);
        times.push(measured.micros);
    }
    (
        matched,
        work,
        percentile_95(&mut times),
        observed_ef.unwrap_or(0),
    )
}

fn fixture(n: usize, shape: &str, seed: usize) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE profile_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, \
         bucket INTEGER NOT NULL, embedding VECTOR(64) PARTITION_KEY (context_id) \
         MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1000)",
        &HashMap::new(),
    )
    .expect("declare the frozen F32 profile table");
    db.execute(
        "CREATE INDEX profile_scope_bucket ON profile_items(context_id, bucket)",
        &HashMap::new(),
    )
    .expect("index scoped profile residuals");
    db.execute(
        "CREATE INDEX profile_bucket ON profile_items(bucket)",
        &HashMap::new(),
    )
    .expect("index all-Context profile residuals");
    let tx = db.begin().expect("begin frozen profile ingestion");
    for partition in 0..PARTITIONS {
        for ordinal in 0..n {
            let global = partition * 1_000_000_000 + ordinal;
            db.insert_row(
                tx,
                "profile_items",
                HashMap::from([
                    ("id".to_owned(), Value::Int64((global + 1) as i64)),
                    ("context_id".to_owned(), Value::Uuid(context_id(partition))),
                    ("bucket".to_owned(), Value::Int64((ordinal % 10) as i64)),
                    (
                        "embedding".to_owned(),
                        Value::Vector(maintained_search::profile_vector(
                            shape, seed, "corpus", global,
                        )),
                    ),
                ]),
            )
            .expect("stage one frozen profile row");
        }
    }
    db.commit(tx).expect("commit frozen profile ingestion");
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("run frozen profile maintenance");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == PARTITIONS && status.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("maintenance did not publish all {PARTITIONS} profile partitions at n={n}")
}

fn quality_passes(matched: u64, indexed_work: u64, exact_work: u64) -> bool {
    matched * 100 >= (PROBES * K * 95) as u64 && indexed_work <= exact_work
}

const BUILD_QUALITY_PROFILES: [(usize, &str, usize); 4] = [
    (5_000_usize, "anisotropic", 17_usize),
    (5_001, "topic_clusters", 17),
    (50_000, "isotropic", 29),
    (50_001, "anisotropic", 17),
];

fn run_build_quality_profile(profile: usize) {
    assert_eq!(
        contextdb_vector::hnsw::f32_hnsw_build_options_for_test(),
        (false, false),
        "the frozen global F32 build choice must remain explicit"
    );
    let (n, shape, seed) = BUILD_QUALITY_PROFILES[profile];
    let filters = [("broad", "bucket < 5"), ("narrow", "bucket = 0")];
    assert_eq!(
        BUILD_QUALITY_PROFILES.len() * filters.len() * Scope::ALL.len(),
        24,
        "the affected build-quality matrix has 24 cells across four required corpora"
    );
    let mut cells = 0usize;
    let db = fixture(n, shape, seed);
    for (filter_name, predicate) in filters {
        for scope in Scope::ALL {
            let exact = exact_baseline(&db, shape, seed, scope, Some(predicate));
            let (matched, work, p95_micros, breadth) =
                indexed_cell(&db, shape, seed, scope, Some(predicate), &exact);
            cells += 1;
            println!(
                "{}",
                json!({
                    "kind": "build_quality_cell",
                    "n": n,
                    "shape": shape,
                    "seed": seed,
                    "scope": scope.as_str(),
                    "contexts": scope.count(),
                    "filter": filter_name,
                    "queries": PROBES,
                    "breadth": breadth,
                    "extend_candidates": false,
                    "keeping_pruned": false,
                    "matched_at_10": matched,
                    "possible_at_10": PROBES * K,
                    "recall": matched as f64 / (PROBES * K) as f64,
                    "indexed_work": work,
                    "exact_work": exact.work,
                    "indexed_p95_micros": p95_micros,
                    "exact_p95_micros": exact.p95_micros,
                })
            );
            assert!(
                matched * 100 >= (PROBES * K * 95) as u64,
                "F32 build quality failed n={n} scope={} filter={filter_name}: {matched}/{}",
                scope.as_str(),
                PROBES * K,
            );
        }
    }
    assert_eq!(cells, 6, "each build-quality corpus has six cells");
}
