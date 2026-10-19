//! Diagnostic sibling of `single_context_saved_graph_auto_and_indexed_are_faster_than_exact`:
//! the same 10,000-row single-Context fixture and held-out queries, measuring
//! for each graph search radius the distance work INDEXED and AUTO spend (mean,
//! p95, max) against EXACT, each query's recall@10 and the route AUTO takes.
//! Run explicitly with `--ignored --nocapture`; `UNION_RECALL_RADII` (comma
//! separated, default: the index's own) chooses the radii.

use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
    with_union_search_radius,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

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

fn scored(db: &Database, sql: &str, params: &HashMap<String, Value>) -> (QueryResult, u64, u64) {
    let graph = Arc::new(Counter(AtomicU64::new(0)));
    let native = Arc::new(Counter(AtomicU64::new(0)));
    let result = with_hnsw_candidate_observer(graph.clone(), || {
        with_native_vector_score_observer_for_test(native.clone(), || {
            db.execute(sql, params).expect("vector query")
        })
    });
    (
        result,
        graph.0.load(Ordering::SeqCst),
        native.0.load(Ordering::SeqCst),
    )
}

fn context_id() -> Uuid {
    Uuid::from_u128(0xC046_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC046_1000_0000_0000_0000_0000_0000_0000)
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
                ("context".into(), Value::Uuid(context_id())),
                ("generation".into(), Value::Uuid(generation_id())),
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
    panic!("maintenance did not publish the single-context partition")
}

fn continue_exact_only(work: &mut HashMap<&str, Vec<u64>>, exact_native: u64) {
    work.entry("EXACT").or_default().push(exact_native);
}

fn percentile(values: &[u64], fraction: f64) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64) * fraction).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn single_context_search_radius_work_diagnostic() {
    let radii: Vec<Option<f32>> = std::env::var("UNION_RECALL_RADII")
        .ok()
        .map(|list| {
            list.split(',')
                .map(|radius| Some(radius.trim().parse().expect("radius")))
                .collect()
        })
        .unwrap_or_else(|| vec![None]);
    let db = single_context_fixture();
    let prefix = "SELECT id FROM single_context_vectors WHERE context_id = $context AND \
                  generation_id = $generation AND is_current = $is_current \
                  ORDER BY embedding <=> $query USE VECTOR";
    // The same scope without the residual `is_current` predicate, to separate
    // what the filter costs a search from what the walk costs.
    let unfiltered_prefix = "SELECT id FROM single_context_vectors WHERE context_id = $context AND \
                             generation_id = $generation \
                             ORDER BY embedding <=> $query USE VECTOR";
    for radius in radii {
        let mut work = HashMap::<&str, Vec<u64>>::new();
        let mut matched = HashMap::<&str, u64>::new();
        let mut low = HashMap::<&str, Vec<(usize, usize)>>::new();
        let mut auto_graph = 0usize;
        let mut nanos = HashMap::<&str, Vec<u128>>::new();
        // The first pass warms caches; only the second is measured.
        for pass in 0..2 {
            for probe in 0..maintained_search::HELD_OUT_ROWS {
                let params = HashMap::from([
                    ("context".into(), Value::Uuid(context_id())),
                    ("generation".into(), Value::Uuid(generation_id())),
                    ("is_current".into(), Value::Bool(true)),
                    (
                        "query".into(),
                        Value::Vector(maintained_search::held_out_vector(probe)),
                    ),
                ]);
                let started = std::time::Instant::now();
                let (exact, _, exact_native) =
                    scored(&db, &format!("{prefix} EXACT LIMIT 10"), &params);
                let elapsed = started.elapsed().as_nanos();
                if pass == 0 {
                    continue_exact_only(&mut work, exact_native);
                } else {
                    nanos.entry("EXACT").or_default().push(elapsed);
                }
                for mode in ["INDEXED", "AUTO", "INDEXED_UNFILTERED", "EXACT_UNFILTERED"] {
                    let sql = match mode {
                        "INDEXED_UNFILTERED" | "EXACT_UNFILTERED" => format!(
                            "{} {} LIMIT 10",
                            unfiltered_prefix,
                            mode.trim_end_matches("_UNFILTERED")
                        ),
                        _ => format!("{prefix} {mode} LIMIT 10"),
                    };
                    let run = || scored(&db, &sql, &params);
                    let started = std::time::Instant::now();
                    let (result, graph, native) = match radius {
                        Some(radius) => with_union_search_radius(radius, run),
                        None => run(),
                    };
                    let elapsed = started.elapsed().as_nanos();
                    if pass == 1 {
                        nanos.entry(mode).or_default().push(elapsed);
                        continue;
                    }
                    if mode == "AUTO" && graph > 0 {
                        auto_graph += 1;
                    }
                    work.entry(mode).or_default().push(graph + native);
                    let overlap = result
                        .rows
                        .iter()
                        .filter(|row| exact.rows.contains(row))
                        .count();
                    *matched.entry(mode).or_default() += overlap as u64;
                    if overlap < 10 {
                        low.entry(mode).or_default().push((probe, overlap));
                    }
                }
            }
        }
        for mode in [
            "EXACT",
            "INDEXED",
            "AUTO",
            "INDEXED_UNFILTERED",
            "EXACT_UNFILTERED",
        ] {
            let mut values = nanos[mode].clone();
            values.sort_unstable();
            let total: u128 = values.iter().sum();
            let scores: u64 = work[mode].iter().sum();
            eprintln!(
                "TIMING radius={radius:?} mode={mode} warm_mean_us={:.1} warm_p95_us={:.1} ns_per_score={:.0}",
                total as f64 / values.len() as f64 / 1_000.0,
                values[(values.len() * 95).div_ceil(100) - 1] as f64 / 1_000.0,
                total as f64 / scores.max(1) as f64,
            );
        }
        for mode in [
            "EXACT",
            "INDEXED",
            "AUTO",
            "INDEXED_UNFILTERED",
            "EXACT_UNFILTERED",
        ] {
            let values = &work[mode];
            eprintln!(
                "SINGLE radius={radius:?} mode={mode} probes={} work_mean={:.1} work_p95={} work_max={} matched={} below_ten={:?}",
                values.len(),
                values.iter().sum::<u64>() as f64 / values.len() as f64,
                percentile(values, 0.95),
                values.iter().max().unwrap(),
                matched.get(mode).copied().unwrap_or(0),
                low.get(mode).cloned().unwrap_or_default(),
            );
        }
        eprintln!("SINGLE radius={radius:?} auto_used_graph={auto_graph}");
    }
}
