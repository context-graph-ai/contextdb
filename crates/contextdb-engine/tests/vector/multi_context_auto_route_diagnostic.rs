//! Diagnostic sibling of `indexed_route_does_less_work_than_exact`: the same
//! eight-Context fixture (half of each Context's rows current), reporting for
//! each frozen cell what AUTO saw and chose -- the aggregate allowed count,
//! the effective AUTO_INDEX_AT, the route and its reason, and each
//! partition's disclosed breadth. Run with `--ignored --nocapture`.

use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::hnsw::with_union_search_receipts;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 1_250;
const LIVE_ROWS_PER_PARTITION: usize = ROWS_PER_PARTITION / 2;

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC046_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC046_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn bind(
    params: &mut HashMap<String, Value>,
    column: &str,
    prefix: &str,
    values: impl IntoIterator<Item = Uuid>,
) -> String {
    let values = values.into_iter().collect::<BTreeSet<_>>();
    let mut names = Vec::new();
    for (index, value) in values.into_iter().enumerate() {
        let name = format!("{prefix}_{index}");
        params.insert(name.clone(), Value::Uuid(value));
        names.push(format!("${name}"));
    }
    if names.len() == 1 {
        format!("{column} = {}", names[0])
    } else {
        format!("{column} IN ({})", names.join(", "))
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
            "INSERT INTO indexed_vectors VALUES ($id, $context, $generation, $current, $embedding)",
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
    panic!("maintenance did not publish all eight partitions")
}

#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn multi_context_auto_route_diagnostic() {
    let db = fixture();
    for partitions in [1_usize, 3, 8] {
        let mut params = HashMap::from([("is_current".into(), Value::Bool(true))]);
        let contexts = bind(
            &mut params,
            "context_id",
            "c",
            (0..partitions).map(context_id),
        );
        let generations = bind(
            &mut params,
            "generation_id",
            "g",
            (0..partitions).map(generation_id),
        );
        params.insert(
            "query".into(),
            Value::Vector(maintained_search::held_out_vector(0)),
        );
        let sql = format!(
            "SELECT id FROM indexed_vectors WHERE {contexts} AND {generations} AND \
             is_current = $is_current ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10"
        );
        let executed = db.execute(&sql, &params).expect("AUTO executes");
        let disclosure = executed
            .trace
            .vector_search
            .as_ref()
            .expect("route receipt");
        let explained = db.explain_output(&sql.replace(
            "$query",
            &format!("{:?}", maintained_search::held_out_vector(0)),
        ));
        let breadths: Vec<_> = disclosure
            .partition_hnsw
            .iter()
            .map(|partition| {
                (
                    partition.hnsw_ef_search,
                    partition.route.map(|route| route.as_str()),
                )
            })
            .collect();
        eprintln!(
            "CELL partitions={partitions} aggregate_allowed={:?} effective_auto_index_at={} route={:?} fallback={:?} partitions={breadths:?}",
            disclosure.aggregate_allowed_vectors,
            disclosure.effective_auto_index_at,
            disclosure.route.map(|route| route.as_str()),
            disclosure.fallback,
        );
        let mut walk = Vec::new();
        for probe in 0..maintained_search::HELD_OUT_ROWS {
            params.insert(
                "query".into(),
                Value::Vector(maintained_search::held_out_vector(probe)),
            );
            let (_, receipts) = with_union_search_receipts(|| {
                db.execute(&sql.replace("AUTO", "INDEXED"), &params)
                    .expect("INDEXED executes")
            });
            walk.push(
                receipts
                    .iter()
                    .map(|receipt| receipt.admission_evaluations + receipt.expansion_evaluations)
                    .sum::<usize>(),
            );
        }
        walk.sort_unstable();
        eprintln!(
            "WALK partitions={partitions} indexed_walk_mean={:.1} p95={} max={} exact_rows={}",
            walk.iter().sum::<usize>() as f64 / walk.len() as f64,
            walk[94],
            walk[99],
            partitions * LIVE_ROWS_PER_PARTITION,
        );
        match explained {
            Ok(explained) => {
                let passive = explained.vector_search.expect("passive receipt");
                eprintln!(
                    "EXPLAIN partitions={partitions} route={:?} fallback={:?}",
                    passive.route.map(|route| route.as_str()),
                    passive.fallback,
                );
            }
            Err(error) => eprintln!("EXPLAIN partitions={partitions} error={error}"),
        }
    }
}

struct WorkCounter(std::sync::atomic::AtomicU64);

impl contextdb_vector::hnsw::HnswCandidateObserver for WorkCounter {
    fn before_candidate_distance(&self, _: contextdb_vector::hnsw::HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl contextdb_vector::mem::NativeVectorScoreObserver for WorkCounter {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

/// Where a forced-INDEXED search over the frozen filtered cells spends its
/// work: graph distances against native rescoring, walk expansions and how
/// each partition walk stopped, and the wall time beside EXACT's.
#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn filtered_walk_work_diagnostic() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    let db = fixture();
    for partitions in [1_usize, 3, 8] {
        let mut params = HashMap::from([("is_current".into(), Value::Bool(true))]);
        let contexts = bind(
            &mut params,
            "context_id",
            "c",
            (0..partitions).map(context_id),
        );
        let generations = bind(
            &mut params,
            "generation_id",
            "g",
            (0..partitions).map(generation_id),
        );
        let sql = format!(
            "SELECT id FROM indexed_vectors WHERE {contexts} AND {generations} AND \
             is_current = $is_current ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
        );
        let exact_sql = sql.replace("INDEXED", "EXACT");
        let (mut graph, mut native, mut expansions, mut evaluated) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut indexed_us, mut exact_us) = (Vec::new(), Vec::new());
        let mut stops = HashMap::<String, usize>::new();
        for probe in 0..maintained_search::HELD_OUT_ROWS {
            params.insert(
                "query".into(),
                Value::Vector(maintained_search::held_out_vector(probe)),
            );
            let graph_count = Arc::new(WorkCounter(AtomicU64::new(0)));
            let native_count = Arc::new(WorkCounter(AtomicU64::new(0)));
            let (_, traces) = contextdb_vector::hnsw::with_union_walk_traces(0, || {
                contextdb_vector::hnsw::with_hnsw_candidate_observer(graph_count.clone(), || {
                    contextdb_vector::mem::with_native_vector_score_observer_for_test(
                        native_count.clone(),
                        || db.execute(&sql, &params).expect("INDEXED executes"),
                    )
                })
            });
            graph.push(graph_count.0.load(Ordering::SeqCst) as f64);
            native.push(native_count.0.load(Ordering::SeqCst) as f64);
            expansions.push(traces.iter().map(|trace| trace.expansions).sum::<usize>() as f64);
            evaluated.push(
                traces
                    .iter()
                    .map(|trace| trace.evaluated.len())
                    .sum::<usize>() as f64,
            );
            for trace in &traces {
                let key = match trace.stop {
                    Some(stop) if stop.head_is_farther => "head_farther".to_string(),
                    Some(_) => "stop_other".to_string(),
                    None => "no_stop(frontier_exhausted_or_limit)".to_string(),
                };
                *stops.entry(key).or_default() += 1;
            }
            for round in 0..3 {
                let order: [&str; 2] = if (probe + round) % 2 == 0 {
                    [&sql, &exact_sql]
                } else {
                    [&exact_sql, &sql]
                };
                for statement in order {
                    let started = std::time::Instant::now();
                    db.execute(statement, &params).expect("search executes");
                    let micros = started.elapsed().as_micros() as f64;
                    if statement == sql {
                        indexed_us.push(micros);
                    } else {
                        exact_us.push(micros);
                    }
                }
            }
        }
        eprintln!(
            "FILTERED_WALK partitions={partitions} allowed={} graph_scores_mean={:.1} native_scores_mean={:.1} walk_expansions_mean={:.1} walk_evaluated_mean={:.1} stops={stops:?} indexed_us_mean={:.0} exact_us_mean={:.0}",
            partitions * LIVE_ROWS_PER_PARTITION,
            mean(&graph),
            mean(&native),
            mean(&expansions),
            mean(&evaluated),
            mean(&indexed_us),
            mean(&exact_us),
        );
    }
}

/// Controls for `filtered_walk_work_diagnostic`: the same frozen cells walked
/// without the `is_current` filter (every live row allowed), and walked with
/// the filter at search radius 0, so the share the filter and the radius each
/// add to the walk is measured rather than assumed.
#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn filtered_walk_controls_diagnostic() {
    let db = fixture();
    for partitions in [1_usize, 3, 8] {
        let mut params = HashMap::new();
        let contexts = bind(
            &mut params,
            "context_id",
            "c",
            (0..partitions).map(context_id),
        );
        let generations = bind(
            &mut params,
            "generation_id",
            "g",
            (0..partitions).map(generation_id),
        );
        let unfiltered = format!(
            "SELECT id FROM indexed_vectors WHERE {contexts} AND {generations} \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
        );
        let filtered = format!(
            "SELECT id FROM indexed_vectors WHERE {contexts} AND {generations} AND \
             is_current = TRUE ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
        );
        let (mut unfiltered_walk, mut radius0_walk) = (Vec::new(), Vec::new());
        let (mut unfiltered_us, mut radius0_us) = (Vec::new(), Vec::new());
        for probe in 0..maintained_search::HELD_OUT_ROWS {
            params.insert(
                "query".into(),
                Value::Vector(maintained_search::held_out_vector(probe)),
            );
            let started = std::time::Instant::now();
            let (_, receipts) = with_union_search_receipts(|| {
                db.execute(&unfiltered, &params)
                    .expect("unfiltered INDEXED executes")
            });
            unfiltered_us.push(started.elapsed().as_micros() as f64);
            unfiltered_walk.push(
                receipts
                    .iter()
                    .map(|receipt| receipt.admission_evaluations + receipt.expansion_evaluations)
                    .sum::<usize>() as f64,
            );
            let started = std::time::Instant::now();
            let (_, receipts) = contextdb_vector::hnsw::with_union_search_radius(0.0, || {
                with_union_search_receipts(|| {
                    db.execute(&filtered, &params)
                        .expect("filtered INDEXED executes")
                })
            });
            radius0_us.push(started.elapsed().as_micros() as f64);
            radius0_walk.push(
                receipts
                    .iter()
                    .map(|receipt| receipt.admission_evaluations + receipt.expansion_evaluations)
                    .sum::<usize>() as f64,
            );
        }
        eprintln!(
            "WALK_CONTROLS partitions={partitions} live={} unfiltered_walk_mean={:.1} unfiltered_us_mean={:.0} filtered_allowed={} filtered_radius0_walk_mean={:.1} filtered_radius0_us_mean={:.0}",
            partitions * ROWS_PER_PARTITION,
            mean(&unfiltered_walk),
            mean(&unfiltered_us),
            partitions * LIVE_ROWS_PER_PARTITION,
            mean(&radius0_walk),
            mean(&radius0_us),
        );
    }
}
