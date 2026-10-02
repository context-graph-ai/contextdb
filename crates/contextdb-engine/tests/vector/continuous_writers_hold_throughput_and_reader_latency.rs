//! Continuous writers preserve throughput and reader latency: under six closed-loop writers
//! running the fixed ten-commit mix (6 inserts, 3 vector replacements, 1 delete) the written
//! partition stays on its maintained route, replacement base generations keep publishing, no
//! writer starves, and the unrelated prebuilt partition keeps its generation.
//!
//! This test asserts only exact structural facts: counts of successful commits, per-writer
//! change counts, the number of replacement base publications, the route of every query, and the
//! unrelated partition's generation identity. It never asserts elapsed time. Every query's
//! duration, its visited work, the change-layer sizes around every publication, and the achieved
//! commits/second are written to the JSON receipt at `CONTEXTDB_CW_RECEIPT`; the 200 ms warm p95
//! and 5 commits/second bars, and the "change layer shrinks across the last two publications"
//! comparison, are evaluated from that receipt by the campaign cell.
//!
//! Full-size parameters are the defaults; the CONTEXTDB_CW_* variables shrink them for a smoke run:
//! SEED_VECTORS (100000), UNRELATED_VECTORS (20000), DIMENSIONS (768), COMMITS (10000),
//! PUBLICATIONS (3), MEMORY_LIMIT (2147483648), RECEIPT (path; default is a temp file whose path
//! is printed).

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, VectorSearchRoute};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

const WRITERS: usize = 6;
const MIN_CHANGES_PER_WRITER: u64 = 50;
const HARD_CAP_FACTOR: u64 = 20;

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer, got {value:?}")),
        Err(_) => default,
    }
}

fn hot_context() -> Uuid {
    Uuid::from_u128(0xC0DE_0000_0000_0000_0000_0000_0000_0001)
}

fn quiet_context() -> Uuid {
    Uuid::from_u128(0xC0DE_0000_0000_0000_0000_0000_0000_0002)
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("continuous_vectors", "embedding")
}

fn partition_ref(context: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(
        index(),
        VectorPartitionKey::from_values(&[Value::Uuid(context)])
            .expect("one UUID component is one canonical partition key"),
    )
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic vector for (role, ordinal): every call with the same arguments returns the same vector.
fn vector(role: u64, ordinal: u64, dimensions: usize) -> Vec<f32> {
    let mut state = role.wrapping_mul(0x1_0000_0001_B3).wrapping_add(ordinal);
    (0..dimensions)
        .map(|_| (splitmix(&mut state) >> 40) as f32 / (1u64 << 23) as f32 - 1.0)
        .collect()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn seed(db: &Database, context: Uuid, first_id: i64, count: u64, role: u64, dimensions: usize) {
    let tx = db.begin().expect("begin the seed transaction");
    for offset in 0..count {
        let id = first_id + offset as i64;
        db.execute_in_tx(
            tx,
            "INSERT INTO continuous_vectors VALUES ($id, $context, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("context", Value::Uuid(context)),
                (
                    "embedding",
                    Value::Vector(vector(role, id as u64, dimensions)),
                ),
            ]),
        )
        .expect("seed one searchable vector");
    }
    db.commit(tx).expect("commit the seed transaction");
}

fn search_sql() -> &'static str {
    "SELECT id FROM continuous_vectors WHERE context_id = $context \
     ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
}

fn is_maintained_route(result: &QueryResult) -> bool {
    matches!(
        result
            .trace
            .vector_search
            .as_ref()
            .and_then(|disclosure| disclosure.route),
        Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed)
    )
}

fn visited_work(result: &QueryResult) -> u64 {
    let disclosure = result
        .trace
        .vector_search
        .as_ref()
        .expect("every vector query reports its route disclosure");
    (disclosure.admission_evaluations.unwrap_or(0) + disclosure.expansion_evaluations.unwrap_or(0))
        as u64
}

#[derive(Clone, Copy, Default)]
struct LayerSample {
    base_generation: u64,
    change_durable_bytes: u64,
    fresh_tail_entries: u64,
    pending_generations: u64,
}

fn layer_sample(db: &Database, context: Uuid) -> LayerSample {
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition_ref(context))
        .expect("the partition has a published generation status");
    let base = status.base.or(status.dormant_base);
    let change = status.change.or(status.dormant_change);
    LayerSample {
        base_generation: base.map_or(0, |g| g.generation_id),
        change_durable_bytes: change.map_or(0, |g| g.durable_bytes),
        fresh_tail_entries: status.fresh_tail_entries as u64,
        pending_generations: status.pending_generation_count as u64,
    }
}

#[derive(Default)]
struct ReaderLog {
    durations_ns: Vec<u64>,
    visited: Vec<u64>,
    off_route_queries: u64,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64) * p).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn distribution(log: &ReaderLog) -> serde_json::Value {
    let mut ns = log.durations_ns.clone();
    ns.sort_unstable();
    let mut visited = log.visited.clone();
    visited.sort_unstable();
    serde_json::json!({
        "queries": ns.len(),
        "off_route_queries": log.off_route_queries,
        "p50_ns": percentile(&ns, 0.50),
        "p95_ns": percentile(&ns, 0.95),
        "p99_ns": percentile(&ns, 0.99),
        "max_ns": ns.last().copied().unwrap_or(0),
        "visited_work_p95": percentile(&visited, 0.95),
        "visited_work_max": visited.last().copied().unwrap_or(0),
        "durations_ns": log.durations_ns,
        "visited_work": log.visited,
    })
}

#[cfg(feature = "nightly-tests")]
#[test]
fn continuous_writers_hold_throughput_and_reader_latency() {
    let seed_vectors = env_u64("CONTEXTDB_CW_SEED_VECTORS", 100_000);
    let unrelated_vectors = env_u64("CONTEXTDB_CW_UNRELATED_VECTORS", 20_000);
    let dimensions = env_u64("CONTEXTDB_CW_DIMENSIONS", 768) as usize;
    let target_commits = env_u64("CONTEXTDB_CW_COMMITS", 10_000);
    let target_publications = env_u64("CONTEXTDB_CW_PUBLICATIONS", 3);
    let memory_limit = env_u64("CONTEXTDB_CW_MEMORY_LIMIT", 2 * 1024 * 1024 * 1024);
    let directory = tempfile::tempdir().expect("fixture directory");
    let receipt_path = std::env::var("CONTEXTDB_CW_RECEIPT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| directory.path().join("continuous-writers-receipt.json"));

    let db = Arc::new(Database::open(directory.path().join("store.db")).expect("open the store"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.set_memory_limit(Some(memory_limit as usize))
        .expect("declare the memory ceiling");
    db.execute(
        &format!(
            "CREATE TABLE continuous_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
             embedding VECTOR({dimensions}) PARTITION_KEY (context_id) MAX_PARTITIONS 4 \
             SEARCH_MODE AUTO CONSOLIDATION (CHANGE_PERCENT = 10))"
        ),
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    seed(&db, hot_context(), 1, seed_vectors, 1, dimensions);
    let quiet_first_id = 1_000_000_000_i64;
    seed(
        &db,
        quiet_context(),
        quiet_first_id,
        unrelated_vectors,
        2,
        dimensions,
    );

    // Both partitions publish a saved graph before any writer starts, so the unrelated
    // partition is a real indexed-route control and the hot partition starts on its maintained route.
    let mut ready = false;
    for _ in 0..4096 {
        db.run_maintenance_cycle().expect("advance maintenance");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 2 && status.remaining_partitions == 0 {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "both partitions publish a saved graph before the run"
    );
    let quiet_before = layer_sample(&db, quiet_context());
    assert_ne!(
        quiet_before.base_generation, 0,
        "the unrelated partition has a published base"
    );

    let done = AtomicBool::new(false);
    let commits = AtomicU64::new(0);
    let hard_cap = target_commits * HARD_CAP_FACTOR;
    let writer_changes: Vec<AtomicU64> = (0..WRITERS).map(|_| AtomicU64::new(0)).collect();
    let hot_log = Mutex::new(ReaderLog::default());
    let quiet_log = Mutex::new(ReaderLog::default());
    let publications = Mutex::new(Vec::<serde_json::Value>::new());
    let started = Instant::now();

    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let (db, done, commits, mine) = (&db, &done, &commits, &writer_changes[writer]);
            scope.spawn(move || {
                // Each writer owns the seed ids congruent to its index plus its own inserts.
                let mut owned: Vec<i64> = (1..=seed_vectors as i64)
                    .filter(|id| (*id as usize) % WRITERS == writer)
                    .collect();
                let mut next_insert = 2_000_000_000_i64 + (writer as i64) * 100_000_000;
                let mut step = 0_u64;
                let mut replace_cursor = 0_usize;
                while !done.load(Ordering::Acquire) {
                    let slot = step % 10;
                    let outcome = match slot {
                        0..=5 => {
                            next_insert += 1;
                            owned.push(next_insert);
                            db.execute(
                                "INSERT INTO continuous_vectors VALUES ($id, $context, $embedding)",
                                &params([
                                    ("id", Value::Int64(next_insert)),
                                    ("context", Value::Uuid(hot_context())),
                                    (
                                        "embedding",
                                        Value::Vector(vector(10 + writer as u64, next_insert as u64, dimensions)),
                                    ),
                                ]),
                            )
                        }
                        6..=8 => {
                            replace_cursor = (replace_cursor + 7) % owned.len();
                            let id = owned[replace_cursor];
                            db.execute(
                                "UPDATE continuous_vectors SET embedding = $embedding WHERE id = $id",
                                &params([
                                    ("id", Value::Int64(id)),
                                    (
                                        "embedding",
                                        Value::Vector(vector(100 + writer as u64, step ^ (id as u64), dimensions)),
                                    ),
                                ]),
                            )
                        }
                        _ => {
                            let id = owned.swap_remove(owned.len() / 2);
                            db.execute(
                                "DELETE FROM continuous_vectors WHERE id = $id",
                                &params([("id", Value::Int64(id))]),
                            )
                        }
                    };
                    outcome.unwrap_or_else(|error| {
                        panic!("writer {writer} step {step}: a refused or failed commit fails the run: {error:?}")
                    });
                    step += 1;
                    mine.fetch_add(1, Ordering::AcqRel);
                    if commits.fetch_add(1, Ordering::AcqRel) + 1 >= hard_cap {
                        done.store(true, Ordering::Release);
                    }
                }
            });
        }

        // Maintenance is driven closed-loop for the whole run: publication is the real engine path.
        {
            let (db, done) = (&db, &done);
            scope.spawn(move || {
                while !done.load(Ordering::Acquire) {
                    db.run_maintenance_cycle()
                        .expect("maintenance cycle during the run");
                }
            });
        }

        // Reader 1: the written partition. It also observes every base publication.
        {
            let (db, done, commits, hot_log, publications) =
                (&db, &done, &commits, &hot_log, &publications);
            scope.spawn(move || {
                let mut seen = layer_sample(db, hot_context());
                let mut interval_peak = seen;
                let mut query_ordinal = 0_u64;
                while !done.load(Ordering::Acquire) {
                    let query = vector(7, query_ordinal % 997, dimensions);
                    query_ordinal += 1;
                    let begun = Instant::now();
                    let result = db
                        .execute(
                            search_sql(),
                            &params([
                                ("context", Value::Uuid(hot_context())),
                                ("query", Value::Vector(query)),
                            ]),
                        )
                        .expect("the hot-partition search succeeds");
                    let elapsed = begun.elapsed().as_nanos() as u64;
                    {
                        let mut log = hot_log.lock().unwrap();
                        log.durations_ns.push(elapsed);
                        log.visited.push(visited_work(&result));
                        if !is_maintained_route(&result) {
                            log.off_route_queries += 1;
                        }
                    }
                    let now = layer_sample(db, hot_context());
                    interval_peak.change_durable_bytes =
                        interval_peak.change_durable_bytes.max(now.change_durable_bytes);
                    interval_peak.fresh_tail_entries =
                        interval_peak.fresh_tail_entries.max(now.fresh_tail_entries);
                    interval_peak.pending_generations =
                        interval_peak.pending_generations.max(now.pending_generations);
                    if now.base_generation != seen.base_generation {
                        let mut list = publications.lock().unwrap();
                        let ordinal = list.len() + 1;
                        list.push(serde_json::json!({
                            "ordinal": ordinal,
                            "base_generation_before": seen.base_generation,
                            "base_generation_after": now.base_generation,
                            "commits_at_publication": commits.load(Ordering::Acquire),
                            "change_layer_high_water_before_bytes": interval_peak.change_durable_bytes,
                            "fresh_tail_high_water_before": interval_peak.fresh_tail_entries,
                            "pending_generations_high_water_before": interval_peak.pending_generations,
                            "change_layer_after_bytes": now.change_durable_bytes,
                            "fresh_tail_after": now.fresh_tail_entries,
                            "pending_generations_after": now.pending_generations,
                        }));
                        seen = now;
                        interval_peak = now;
                        if list.len() as u64 >= target_publications
                            && commits.load(Ordering::Acquire) >= target_commits
                        {
                            done.store(true, Ordering::Release);
                        }
                    } else if commits.load(Ordering::Acquire) >= target_commits
                        && publications.lock().unwrap().len() as u64 >= target_publications
                    {
                        done.store(true, Ordering::Release);
                    }
                }
            });
        }

        // Reader 2: the unrelated prebuilt partition.
        {
            let (db, done, quiet_log) = (&db, &done, &quiet_log);
            scope.spawn(move || {
                let mut query_ordinal = 0_u64;
                while !done.load(Ordering::Acquire) {
                    let query = vector(8, query_ordinal % 997, dimensions);
                    query_ordinal += 1;
                    let begun = Instant::now();
                    let result = db
                        .execute(
                            search_sql(),
                            &params([
                                ("context", Value::Uuid(quiet_context())),
                                ("query", Value::Vector(query)),
                            ]),
                        )
                        .expect("the unrelated-partition search succeeds");
                    let elapsed = begun.elapsed().as_nanos() as u64;
                    let mut log = quiet_log.lock().unwrap();
                    log.durations_ns.push(elapsed);
                    log.visited.push(visited_work(&result));
                    if !is_maintained_route(&result) {
                        log.off_route_queries += 1;
                    }
                }
            });
        }
    });

    let elapsed_ns = started.elapsed().as_nanos() as u64;
    let total_commits = commits.load(Ordering::Acquire);
    let per_writer: Vec<u64> = writer_changes
        .iter()
        .map(|c| c.load(Ordering::Acquire))
        .collect();
    let quiet_after = layer_sample(&db, quiet_context());
    let publications = publications.into_inner().unwrap();
    let hot = hot_log.into_inner().unwrap();
    let quiet = quiet_log.into_inner().unwrap();

    let receipt = serde_json::json!({
        "schema": "continuous-writers-receipt/1",
        "parameters": {
            "seed_vectors": seed_vectors, "unrelated_vectors": unrelated_vectors,
            "dimensions": dimensions, "target_commits": target_commits,
            "target_publications": target_publications, "memory_limit_bytes": memory_limit,
            "writers": WRITERS, "mix": "6 insert, 3 replace, 1 delete",
        },
        "successful_commits": total_commits,
        "per_writer_changes": per_writer,
        "elapsed_ns": elapsed_ns,
        "commits_per_second": total_commits as f64 / (elapsed_ns as f64 / 1e9),
        "publications": publications,
        "hot_reader": distribution(&hot),
        "unrelated_reader": distribution(&quiet),
        "unrelated_partition": {
            "base_generation_before": quiet_before.base_generation,
            "base_generation_after": quiet_after.base_generation,
        },
        "writer_memory_high_water_bytes": db.write_memory_high_water_for_test(),
        "durable_writer_charged_bytes": db.__durable_writer_charged_bytes_for_test(),
    });
    std::fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt).unwrap())
        .expect("write the receipt");
    println!("CONTINUOUS_WRITERS_RECEIPT {}", receipt_path.display());

    assert!(
        total_commits >= target_commits,
        "successful vector-changing commits: {total_commits} < {target_commits}"
    );
    assert!(
        publications.len() as u64 >= target_publications,
        "replacement base publications: {} < {target_publications}",
        publications.len()
    );
    for (writer, changes) in per_writer.iter().enumerate() {
        assert!(
            *changes >= MIN_CHANGES_PER_WRITER,
            "writer {writer} committed only {changes} changes"
        );
    }
    assert_eq!(
        per_writer.iter().sum::<u64>(),
        total_commits,
        "every acknowledged commit is attributed to exactly one writer"
    );
    assert_eq!(
        hot.off_route_queries, 0,
        "the written partition stays on the maintained route throughout"
    );
    assert_eq!(
        quiet.off_route_queries, 0,
        "the unrelated partition stays on the maintained route throughout"
    );
    assert!(
        !hot.durations_ns.is_empty() && !quiet.durations_ns.is_empty(),
        "both readers ran"
    );
    assert_eq!(
        quiet_after.base_generation, quiet_before.base_generation,
        "the unrelated partition keeps its generation"
    );
}
