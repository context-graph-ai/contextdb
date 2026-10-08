//! Attribution for a single-partition find at scale: where the milliseconds go.
//!
//! One Context holding 10,000 vectors of 768 dimensions, `LIMIT 10`, read
//! through a real `ReadSession` on the file route. Not a bar — a breakdown.
//! Run with `--ignored --nocapture`.

use contextdb_core::read_contract::ReadRoute;
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use uuid::Uuid;

fn rows() -> usize {
    std::env::var("CONTEXTDB_P1_ATTRIBUTION_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000)
}
const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;

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

#[derive(Default)]
struct PageCounts {
    filled: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    decoded: AtomicU64,
    query_norms: AtomicU64,
    locks: AtomicU64,
}

impl contextdb_vector::hnsw::PagedVectorObserver for PageCounts {
    fn page_cache_locked(&self) {
        self.locks.fetch_add(1, Ordering::Relaxed);
    }
    fn page_filled(&self) {
        self.filled.fetch_add(1, Ordering::Relaxed);
    }
    fn page_looked_up(&self, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn vector_decoded(&self) {
        self.decoded.fetch_add(1, Ordering::Relaxed);
    }
    fn query_norm_computed(&self) {
        self.query_norms.fetch_add(1, Ordering::Relaxed);
    }
}

/// What one 768-dimension comparison costs, on this CPU, through the kernel the
/// exact route actually calls.
#[test]
#[ignore = "attribution, not a bar: it seeds a 10,000 x 768 store (about ten minutes) and \
            prints where a find's milliseconds go. Nothing here asserts a contract, so there is \
            no property to twin -- the contracts it informed are proven by their own tests."]
fn one_cosine_evaluation_costs() {
    let query = embedding(0);
    let stored: Vec<Vec<f32>> = (1..=1_000usize).map(embedding).collect();
    // Warm the caches so this measures arithmetic, not first touch.
    let mut sink = 0.0f32;
    for vector in &stored {
        sink += contextdb_vector::cosine_similarity(&query, vector);
    }
    let started = Instant::now();
    for _ in 0..10 {
        for vector in &stored {
            sink += contextdb_vector::cosine_similarity(&query, vector);
        }
    }
    let elapsed = started.elapsed();
    let evaluations = 10 * stored.len();
    println!(
        "cosine dims={DIMENSIONS} evaluations={evaluations} total_ms={:.3} per_eval_ns={:.1} \
         sink={sink}",
        elapsed.as_secs_f64() * 1e3,
        elapsed.as_nanos() as f64 / evaluations as f64,
    );
    println!(
        "cosine projected_ms_for_10000_vectors={:.2}",
        elapsed.as_nanos() as f64 / evaluations as f64 * 10_000.0 / 1e6
    );
}

#[cfg(unix)]
#[test]
#[ignore = "attribution, not a bar: it seeds a 10,000 x 768 store (about ten minutes) and \
            prints where a find's milliseconds go. Nothing here asserts a contract, so there is \
            no property to twin -- the contracts it informed are proven by their own tests."]
fn where_one_partition_spends_its_find() {
    use std::os::unix::fs::PermissionsExt;

    // The fixture is expensive to build (10,000 rows x 768 dimensions) and this
    // is an attribution run that wants re-running as the numbers lead
    // somewhere. Build it once at a stable path and reuse it.
    let root = std::path::PathBuf::from(
        std::env::var("CONTEXTDB_P1_ATTRIBUTION_DIR").unwrap_or_else(|_| {
            // Keep this fixture in the current build's scratch directory.
            format!("{}/attribution", env!("CARGO_TARGET_TMPDIR"))
        }),
    )
    .join(format!("rows-{}", rows()));
    let path = root.join("one-partition.db");
    let runtime = root.join("runtime");
    let already_seeded = path.exists();
    if !already_seeded {
        std::fs::create_dir_all(&runtime).expect("the reader runtime directory");
    }
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");

    let seeding = Instant::now();
    if already_seeded {
        println!("fixture reused at {}", path.display());
    } else {
        seed_one_partition(&path, &runtime);
        println!(
            "fixture seeded rows={} dims={DIMENSIONS} seed_ms={:.0}",
            rows(),
            seeding.elapsed().as_secs_f64() * 1e3
        );
    }
    measure_one_partition(&path, &runtime);
}

#[cfg(unix)]
fn seed_one_partition(path: &std::path::Path, runtime: &std::path::Path) {
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
    .expect("open the fixture store");
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    admin
        .execute(
            "CREATE TABLE passages (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR(768) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX passages_active_idx ON passages (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for ordinal in 0..rows() {
        admin
            .execute(
                "INSERT INTO passages VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id())),
                    ("generation".into(), Value::Uuid(generation_id())),
                    ("embedding".into(), Value::Vector(embedding(ordinal + 1))),
                ]),
            )
            .expect("seed one passage");
    }
    let index = VectorIndexRef::new("passages", "embedding");
    let mut cycles = 0;
    for _ in 0..4096 {
        admin
            .run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        cycles += 1;
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            admin
                .declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = admin
            .vector_store_for_test()
            .partition_infos(&index, &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if !ready.is_empty() && ready.iter().all(|partition| partition.graph_available) {
            break;
        }
    }
    let mut partition_observation_owner_2 = contextdb_engine::vector_read_memory::admission(
        admin
            .declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let infos = admin
        .vector_store_for_test()
        .partition_infos(&index, &mut partition_observation_owner_2)
        .expect("inspect partition readiness");
    println!(
        "fixture partitions={} graph_available={} maintenance_cycles={cycles}",
        infos.len(),
        infos.iter().all(|partition| partition.graph_available),
    );
    admin.close().expect("seal the fixture store");
}

#[cfg(unix)]
fn measure_one_partition(path: &std::path::Path, runtime: &std::path::Path) {
    let open = Instant::now();
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
    let open_ms = open.elapsed().as_secs_f64() * 1e3;
    assert_eq!(reader.route(), ReadRoute::File);

    let params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(embedding(0))),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ]);
    let statement = |mode: &str| {
        format!(
            "SELECT id FROM passages WHERE context_id IN ($context) AND \
             generation_id IN ($generation) AND is_current = $is_current \
             ORDER BY embedding <=> $query {mode} LIMIT {LIMIT}"
        )
    };

    for (label, mode) in [("auto", ""), ("exact", "USE VECTOR EXACT")] {
        let sql = statement(mode);
        // Cold first find in this reader, then a warm one. Both are reported:
        // the cold number is the "first query uses saved state" cell.
        let cold = Instant::now();
        let first = reader.execute(&sql, &params);
        let cold_ms = cold.elapsed().as_secs_f64() * 1e3;
        let first = match first {
            Ok(answer) => answer,
            Err(refusal) => {
                // Recorded, not fatal: the exact route refuses this size for a
                // reason of its own (it hashes about 9,919 bytes per vector to
                // score one), and that is a separate defect from the serving
                // cache this cell is about.
                println!("find[{label}] cold_ms={cold_ms:.1} REFUSED {refusal:?}");
                continue;
            }
        };
        assert_eq!(first.rows.len(), LIMIT, "{label} answers a full page");
        if label == "auto" {
            // The contract this cell exists for: a fresh reader's FIRST find
            // over one 10,000-vector partition, at the default settings,
            // answers inside its unchanged active budget. At a fixed 16 MiB
            // serving cache it did not answer at all.
            assert!(
                cold_ms < 5_000.0,
                "the first find in a fresh reader answers inside its active budget, took \
                 {cold_ms:.0} ms"
            );
        }

        let pages = Arc::new(PageCounts::default());
        let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        let warm = Instant::now();
        let answer = contextdb_vector::hnsw::with_paged_vector_observer(pages.clone(), || {
            reader.execute(&sql, &params).expect("the find answers")
        });
        let warm_ms = warm.elapsed().as_secs_f64() * 1e3;
        let counters = work.counters();
        drop(work);

        println!(
            "find[{label}] open_ms={open_ms:.1} cold_ms={cold_ms:.1} warm_ms={warm_ms:.1} \
             rows={} plan={} index={:?} route={:?} rows_examined={}",
            answer.rows.len(),
            answer.trace.physical_plan,
            answer.trace.index_used,
            answer.trace.vector_search,
            answer.trace.rows_examined,
        );
        println!(
            "find[{label}] decoded_rows={} stored_row_bytes={} decoded_vectors={} \
             vector_body_accesses={} hashed_bytes={} hashed_rows={} hashed_vectors={} \
             runs_built={} postings_walked={}",
            counters.decoded_rows,
            counters.stored_row_bytes,
            counters.decoded_vectors,
            counters.vector_body_accesses,
            counters.hashed_bytes,
            counters.hashed_rows,
            counters.hashed_vectors,
            counters.verified_runs_built,
            counters.verified_postings_walked,
        );
        println!(
            "find[{label}] page_locks={} page_hits={} page_misses={} pages_filled={} \
             vectors_decoded_from_pages={} query_norms={} rows={}",
            pages.locks.load(Ordering::Relaxed),
            pages.hits.load(Ordering::Relaxed),
            pages.misses.load(Ordering::Relaxed),
            pages.filled.load(Ordering::Relaxed),
            pages.decoded.load(Ordering::Relaxed),
            pages.query_norms.load(Ordering::Relaxed),
            rows(),
        );
    }
}
