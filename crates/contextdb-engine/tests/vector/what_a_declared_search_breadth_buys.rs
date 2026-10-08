//! What each declared search breadth buys, measured rather than chosen.
//!
//! A consumer declares `HNSW (EF_SEARCH = n)` on its vector column and gets a
//! recall and a cost. This records both, over one Context holding 10,000 (and
//! 12,500) vectors of 768 dimensions, against the exhaustive answer on the
//! same stored representation: recall@10, distance evaluations per find, and
//! warm engine milliseconds. It declares no default -- it is the table the
//! column's owner reads before declaring one.
//!
//! Run with `--run-ignored all --no-capture`. It reuses the seeded fixture
//! `where_one_partition_spends_its_find` builds, on a working copy, because
//! it alters the column's declared breadth between arms.

use contextdb_core::read_contract::ReadRoute;
use contextdb_core::types::ContextId;
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use uuid::Uuid;

const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;
const HELD_OUT_QUERIES: usize = 100;
/// The breadths this table reports. Measurement points, not candidates.
const DECLARED_BREADTHS: [usize; 5] = [40, 64, 100, 200, 400];

fn rows() -> usize {
    std::env::var("CONTEXTDB_F3_BREADTH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000)
}

/// The fixture this reuses is the attribution harness's, so its Context,
/// generation and embeddings are that harness's, exactly.
fn context_id() -> Uuid {
    Uuid::from_u128(0xC04B_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC04B_1000_0000_0000_0000_0000_0000_0000)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSIONS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

/// A query no stored row is. The corpus occupies ordinals `1..=rows`, so the
/// held-out block starts past it and is the same block at every breadth.
fn held_out_query(ordinal: usize) -> Vec<f32> {
    embedding(10_000_000 + ordinal)
}

#[derive(Default)]
struct Evaluations {
    decoded: AtomicU64,
    locks: AtomicU64,
    /// What the walk compares outside its distance arithmetic, which is where
    /// a graph-walk evaluation costs more than an exhaustive comparison of the
    /// same vector.
    reached_entries: AtomicU64,
    frontier_entries: AtomicU64,
    result_entries: AtomicU64,
    edges: AtomicU64,
}

impl contextdb_vector::hnsw::PagedVectorObserver for Evaluations {
    fn vector_decoded(&self) {
        self.decoded.fetch_add(1, Ordering::Relaxed);
    }
    fn page_cache_locked(&self) {
        self.locks.fetch_add(1, Ordering::Relaxed);
    }
    fn walk_compared(&self, work: contextdb_vector::hnsw::PagedWalkWork) {
        self.reached_entries
            .fetch_add(work.reached_entries, Ordering::Relaxed);
        self.frontier_entries
            .fetch_add(work.frontier_entries, Ordering::Relaxed);
        self.result_entries
            .fetch_add(work.result_entries, Ordering::Relaxed);
        self.edges.fetch_add(work.edges, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct WalkTotals {
    locks: u64,
    reached_entries: u64,
    frontier_entries: u64,
    result_entries: u64,
    edges: u64,
}

impl WalkTotals {
    fn add(&mut self, seen: &Evaluations) {
        self.locks += seen.locks.load(Ordering::Relaxed);
        self.reached_entries += seen.reached_entries.load(Ordering::Relaxed);
        self.frontier_entries += seen.frontier_entries.load(Ordering::Relaxed);
        self.result_entries += seen.result_entries.load(Ordering::Relaxed);
        self.edges += seen.edges.load(Ordering::Relaxed);
    }

    fn line(&self, finds: u64) -> String {
        format!(
            "locks_per_find={:.0} reached_entries_per_find={:.0} \
             frontier_entries_per_find={:.0} result_entries_per_find={:.0} edges_per_find={:.0}",
            self.locks as f64 / finds as f64,
            self.reached_entries as f64 / finds as f64,
            self.frontier_entries as f64 / finds as f64,
            self.result_entries as f64 / finds as f64,
            self.edges as f64 / finds as f64,
        )
    }
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let position = (fraction * (sorted.len() - 1) as f64).round() as usize;
    sorted[position.min(sorted.len() - 1)]
}

fn answer_ids(answer: &contextdb_engine::QueryResult) -> Vec<i64> {
    answer
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the projection returns one integer id, got {other:?}"),
        })
        .collect()
}

fn find_sql(mode: &str) -> String {
    format!(
        "SELECT id FROM passages WHERE context_id IN ($context) AND \
         generation_id IN ($generation) AND is_current = $is_current \
         ORDER BY embedding <=> $query {mode} LIMIT {LIMIT}"
    )
}

fn params(query: &[f32]) -> HashMap<String, Value> {
    HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query.to_vec())),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ])
}

#[cfg(unix)]
fn open_reader(path: &std::path::Path, runtime: &std::path::Path) -> ReadSession {
    let reader = ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

/// Declare the breadth on the column. It applies to queries opened after it
/// without replacing the built graph, so the same stored representation
/// answers every arm of the table.
#[cfg(unix)]
fn declare_breadth(path: &std::path::Path, runtime: &std::path::Path, breadth: usize) {
    let admin = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the working copy to declare its breadth");
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    admin
        .execute(
            &format!(
                "ALTER TABLE passages ALTER COLUMN embedding SET HNSW (EF_SEARCH = {breadth})"
            ),
            &HashMap::new(),
        )
        .expect("declare the search breadth");
    let index = VectorIndexRef::new("passages", "embedding");
    let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
        admin
            .declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let infos = admin
        .vector_store_for_test()
        .partition_infos(&index, &mut partition_observation_owner_1)
        .expect("inspect partition readiness");
    assert!(
        !infos.is_empty() && infos.iter().all(|partition| partition.graph_available),
        "declaring a breadth does not withdraw the built graph"
    );
    admin.close().expect("seal the working copy");
}

#[cfg(unix)]
#[test]
#[ignore = "measurement for the owner, not a bar, and it cannot run in the default suite: it \
            copies a multi-hundred-megabyte seeded store, ALTERS the column's declared breadth \
            between arms, and runs 100 held-out queries at each of six breadths on two \
            populations -- about a minute per population and mutating by design. There is no \
            property here to twin: every number it prints is an observation, and nothing about \
            it passes or fails."]
fn what_a_declared_search_breadth_buys() {
    use std::os::unix::fs::PermissionsExt;

    let source = std::path::PathBuf::from(
        std::env::var("CONTEXTDB_P1_ATTRIBUTION_DIR")
            .unwrap_or_else(|_| format!("{}/attribution", env!("CARGO_TARGET_TMPDIR"))),
    )
    .join(format!("rows-{}", rows()));
    assert!(
        source.join("one-partition.db").exists(),
        "seed {} with where_one_partition_spends_its_find before measuring breadth",
        source.display()
    );

    // A working copy, because this alters the column between arms and the
    // seeded fixture answers other harnesses.
    let root = std::path::PathBuf::from(
        std::env::var("CONTEXTDB_F3_BREADTH_DIR")
            .unwrap_or_else(|_| format!("{}/breadth", env!("CARGO_TARGET_TMPDIR"))),
    )
    .join(format!("rows-{}", rows()));
    let path = root.join("one-partition.db");
    let runtime = root.join("runtime");
    if !path.exists() {
        std::fs::create_dir_all(&runtime).expect("the reader runtime directory");
        // Every file the writer left beside the store, not just the store: a
        // reader opens the committed image through its sidecars, and a copy
        // missing them is refused as an unimplemented route.
        for entry in std::fs::read_dir(&source).expect("read the seeded fixture directory") {
            let entry = entry.expect("one seeded fixture entry");
            if entry.file_type().expect("the entry's kind").is_file() {
                std::fs::copy(entry.path(), root.join(entry.file_name()))
                    .expect("copy one seeded fixture file");
            }
        }
    }
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");

    let queries: Vec<Vec<f32>> = (0..HELD_OUT_QUERIES).map(held_out_query).collect();

    // Ground truth from the engine's own exhaustive route over the same stored
    // representation, so recall is measured against what this database holds
    // rather than against the test's idea of the corpus.
    let exact_sql = find_sql("USE VECTOR EXACT");
    let mut truth: Vec<HashSet<i64>> = Vec::with_capacity(queries.len());
    let mut exact_ms: Vec<f64> = Vec::with_capacity(queries.len());
    let mut exact_evaluations = 0u64;
    let mut exact_totals = WalkTotals::default();
    {
        let reader = open_reader(&path, &runtime);
        let _ = reader.execute(&exact_sql, &params(&queries[0]));
        for query in &queries {
            let evaluations = Arc::new(Evaluations::default());
            let started = Instant::now();
            let answer =
                contextdb_vector::hnsw::with_paged_vector_observer(evaluations.clone(), || {
                    reader
                        .execute(&exact_sql, &params(query))
                        .expect("the exhaustive route answers")
                });
            exact_ms.push(started.elapsed().as_secs_f64() * 1e3);
            exact_evaluations += evaluations.decoded.load(Ordering::Relaxed);
            exact_totals.add(&evaluations);
            let ids = answer_ids(&answer);
            assert_eq!(ids.len(), LIMIT, "the exhaustive route answers a full page");
            truth.push(ids.into_iter().collect());
        }
    }
    exact_ms.sort_by(|left, right| left.partial_cmp(right).expect("finite timings"));
    println!(
        "breadth rows={} arm=EXACT recall=1.000 evaluations_per_find={:.0} p50_ms={:.1} \
         p95_ms={:.1} queries={HELD_OUT_QUERIES}",
        rows(),
        exact_evaluations as f64 / queries.len() as f64,
        percentile(&exact_ms, 0.50),
        percentile(&exact_ms, 0.95),
    );
    println!(
        "walk_work rows={} arm=EXACT {}",
        rows(),
        exact_totals.line(queries.len() as u64)
    );

    let indexed_sql = find_sql("USE VECTOR INDEXED");

    // The undeclared arm first, before anything is declared on the column:
    // what a consumer gets today at default settings, on a reader that has
    // just opened. Open and the first find are recorded separately from warm
    // because they are separate promises.
    {
        let opening = Instant::now();
        let reader = open_reader(&path, &runtime);
        let open_ms = opening.elapsed().as_secs_f64() * 1e3;
        let cold = Instant::now();
        let first = reader.execute(&indexed_sql, &params(&queries[0]));
        let cold_ms = cold.elapsed().as_secs_f64() * 1e3;
        let cold_note = match &first {
            Ok(answer) => format!("rows={}", answer.rows.len()),
            Err(refusal) => format!("REFUSED {refusal:?}"),
        };

        let mut hits = 0usize;
        let mut evaluations_total = 0u64;
        let mut totals = WalkTotals::default();
        let mut warm_ms: Vec<f64> = Vec::with_capacity(queries.len());
        for (position, query) in queries.iter().enumerate() {
            let evaluations = Arc::new(Evaluations::default());
            let started = Instant::now();
            let answer =
                contextdb_vector::hnsw::with_paged_vector_observer(evaluations.clone(), || {
                    reader
                        .execute(&indexed_sql, &params(query))
                        .expect("the graph route answers")
                });
            warm_ms.push(started.elapsed().as_secs_f64() * 1e3);
            evaluations_total += evaluations.decoded.load(Ordering::Relaxed);
            totals.add(&evaluations);
            hits += answer_ids(&answer)
                .into_iter()
                .filter(|id| truth[position].contains(id))
                .count();
        }
        warm_ms.sort_by(|left, right| left.partial_cmp(right).expect("finite timings"));
        let evaluations_per_find = evaluations_total as f64 / queries.len() as f64;
        println!(
            "breadth rows={} arm=DEFAULT recall={:.3} evaluations_per_find={evaluations_per_find:.0} \
             p50_ms={:.1} p95_ms={:.1} us_per_evaluation={:.2} open_ms={open_ms:.1} \
             cold_ms={cold_ms:.1} cold={cold_note} queries={HELD_OUT_QUERIES}",
            rows(),
            hits as f64 / (queries.len() * LIMIT) as f64,
            percentile(&warm_ms, 0.50),
            percentile(&warm_ms, 0.95),
            percentile(&warm_ms, 0.50) * 1e3 / evaluations_per_find.max(1.0),
        );
        println!(
            "walk_work rows={} arm=DEFAULT {}",
            rows(),
            totals.line(queries.len() as u64)
        );

        // What a consumer who names no route gets. AUTO is the route the
        // engine's own costing picks for this partition, so this row is the
        // verdict of that costing, measured rather than read off the model.
        let auto_sql = find_sql("USE VECTOR AUTO");
        let mut auto_hits = 0usize;
        let mut auto_evaluations = 0u64;
        let mut auto_totals = WalkTotals::default();
        let mut auto_ms: Vec<f64> = Vec::with_capacity(queries.len());
        let _ = reader.execute(&auto_sql, &params(&queries[0]));
        for (position, query) in queries.iter().enumerate() {
            let evaluations = Arc::new(Evaluations::default());
            let started = Instant::now();
            let answer =
                contextdb_vector::hnsw::with_paged_vector_observer(evaluations.clone(), || {
                    reader
                        .execute(&auto_sql, &params(query))
                        .expect("the automatic route answers")
                });
            auto_ms.push(started.elapsed().as_secs_f64() * 1e3);
            auto_evaluations += evaluations.decoded.load(Ordering::Relaxed);
            auto_totals.add(&evaluations);
            auto_hits += answer_ids(&answer)
                .into_iter()
                .filter(|id| truth[position].contains(id))
                .count();
        }
        auto_ms.sort_by(|left, right| left.partial_cmp(right).expect("finite timings"));
        println!(
            "breadth rows={} arm=AUTO recall={:.3} evaluations_per_find={:.0} p50_ms={:.1} \
             p95_ms={:.1} queries={HELD_OUT_QUERIES}",
            rows(),
            auto_hits as f64 / (queries.len() * LIMIT) as f64,
            auto_evaluations as f64 / queries.len() as f64,
            percentile(&auto_ms, 0.50),
            percentile(&auto_ms, 0.95),
        );
        println!(
            "walk_work rows={} arm=AUTO {}",
            rows(),
            auto_totals.line(queries.len() as u64)
        );
    }

    for breadth in DECLARED_BREADTHS {
        declare_breadth(&path, &runtime, breadth);
        let reader = open_reader(&path, &runtime);
        let _ = reader.execute(&indexed_sql, &params(&queries[0]));

        let mut hits = 0usize;
        let mut refused = 0usize;
        let mut evaluations_total = 0u64;
        let mut totals = WalkTotals::default();
        let mut warm_ms: Vec<f64> = Vec::with_capacity(queries.len());
        for (position, query) in queries.iter().enumerate() {
            let evaluations = Arc::new(Evaluations::default());
            let started = Instant::now();
            let answer =
                contextdb_vector::hnsw::with_paged_vector_observer(evaluations.clone(), || {
                    reader.execute(&indexed_sql, &params(query))
                });
            evaluations_total += evaluations.decoded.load(Ordering::Relaxed);
            totals.add(&evaluations);
            match answer {
                Ok(answer) => {
                    warm_ms.push(started.elapsed().as_secs_f64() * 1e3);
                    hits += answer_ids(&answer)
                        .into_iter()
                        .filter(|id| truth[position].contains(id))
                        .count();
                }
                Err(Error::VectorIndexedRouteUnavailable { .. }) => refused += 1,
                Err(error) => {
                    panic!("declared breadth {breadth} returned an unexpected error: {error:?}")
                }
            }
        }
        warm_ms.sort_by(|left, right| left.partial_cmp(right).expect("finite timings"));
        println!(
            "breadth rows={} arm=EF_SEARCH={breadth} recall_on_answers={:.3} \
             refused={refused} evaluations_per_attempt={:.0} p50_ms={:.1} p95_ms={:.1} \
             queries={HELD_OUT_QUERIES}",
            rows(),
            hits as f64 / ((queries.len() - refused).max(1) * LIMIT) as f64,
            evaluations_total as f64 / queries.len() as f64,
            percentile(&warm_ms, 0.50),
            percentile(&warm_ms, 0.95),
        );
        println!(
            "walk_work rows={} arm=EF_SEARCH={breadth} {}",
            rows(),
            totals.line(queries.len() as u64)
        );
    }
}
