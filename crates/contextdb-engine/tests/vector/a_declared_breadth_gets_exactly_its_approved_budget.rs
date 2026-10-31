//! A request's declared work limit bounds a clustered-index find over many
//! Contexts: whatever the limit, the distance evaluations the find spends
//! never exceed it, an answered find does strictly less work than EXACT and
//! returns a full page, and a find that cannot finish inside a tight limit
//! refuses with the typed work refusal instead of answering from part of the
//! scope.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::vector_ivf_observations::{
    NativeIvfFaultBoundary, NativeIvfSealEvent, NativeIvfSealObserver,
    with_native_ivf_seal_observer_for_test,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const PARTITIONS: usize = 8;
/// More rows per partition than the widest declared breadth, so the index is
/// what every scope searches.
const ROWS_PER_PARTITION: usize = 500;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xB0D6_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xB0D6_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

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

/// The work and memory the finds are approved for. What this module proves
/// is the work budget a declared breadth buys, counted in evaluations, never
/// how long a find takes; the active-time limit is set past any debug-build
/// find of this fixture so that a slow or loaded machine cannot stand in for
/// a budget refusal.
fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        active_ms: 120_000,
        ..ReadLimits::default()
    }
}

#[cfg(unix)]
fn open_admin(path: &std::path::Path, runtime: &std::path::Path) -> Database {
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
}

/// Eight Contexts of 500 rows, every index built.
#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path) {
    let admin = open_admin(path, runtime);
    admin
        .execute(
            &format!(
                "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
                 generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
                 embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
                 MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1 )"
            ),
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX scoped_rows_active_idx ON scoped_rows \
             (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            admin
                .execute(
                    "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(ordinal as i64 + 1)),
                        ("context".into(), Value::Uuid(context_id(partition))),
                        ("generation".into(), Value::Uuid(generation_id(partition))),
                        ("embedding".into(), Value::Vector(embedding(ordinal))),
                    ]),
                )
                .expect("seed one partitioned row");
        }
    }
    let index = VectorIndexRef::new("scoped_rows", "embedding");
    for _ in 0..4096 {
        admin
            .run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            admin
                .declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = admin
            .vector_store_for_test()
            .partition_infos(&index, &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if ready.len() == PARTITIONS
            && ready.iter().all(|partition| {
                admin.__debug_vector_partition_indexed_for_test(
                    partition.partition.index.clone(),
                    partition.partition.partition_key.clone(),
                )
            })
        {
            admin.close().expect("seal the fixture store");
            return;
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

/// Declare the breadth on the column. It applies to queries opened after it
/// without rebuilding the index, so every breadth is measured over the same
/// index.
#[cfg(unix)]
fn declare_breadth(path: &std::path::Path, runtime: &std::path::Path, breadth: usize) {
    let admin = open_admin(path, runtime);
    admin
        .execute(
            &format!("ALTER TABLE scoped_rows ALTER COLUMN embedding SET IVF (PROBES = {breadth})"),
            &HashMap::new(),
        )
        .expect("declare the search breadth");
    admin.close().expect("seal the fixture store");
}

fn statement(selected: usize) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector())),
    ]);
    let mut contexts = Vec::with_capacity(selected);
    let mut generations = Vec::with_capacity(selected);
    for partition in 0..selected {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    (
        format!(
            "SELECT id FROM scoped_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query USE VECTOR INDEXED \
             LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

#[cfg(unix)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn open_reader(
    path: &std::path::Path,
    runtime: &std::path::Path,
    limits: ReadLimits,
) -> ReadSession {
    let reader = ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                contexts: Some(
                    (0..PARTITIONS)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits,
                memory_limit: Some(limits.memory as usize),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

#[derive(Default)]
struct SearchEvaluations {
    evaluations: AtomicU64,
}

impl NativeIvfSealObserver for SearchEvaluations {
    fn observe(&self, event: NativeIvfSealEvent<'_>) {
        if let NativeIvfSealEvent::QueryExecuted { work, .. } = event {
            self.evaluations.fetch_add(
                work.centroid_evaluations
                    + work.quantized_evaluations
                    + work.full_precision_evaluations
                    + work.buffer_evaluations,
                Ordering::SeqCst,
            );
        }
    }

    fn before_boundary(&self, _boundary: NativeIvfFaultBoundary<'_>) -> contextdb_core::Result<()> {
        Ok(())
    }
}

/// Opening the index and searching it are both work the request declared a
/// limit for: whatever that limit, the two together never exceed it, and the
/// answer is a full page or a typed refusal.
#[cfg(unix)]
#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn opening_and_searching_are_charged_against_the_declared_work() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let path = directory.path().join("declared-work.db");
    seed(&path, &runtime);
    declare_breadth(&path, &runtime, 400);

    let (sql, params) = statement(PARTITIONS);
    for work in [300_u64, 600, 1_200, 2_400, 4_800, 9_600, 20_000] {
        let reader = open_reader(&path, &runtime, ReadLimits { work, ..limits() });
        let observer = Arc::new(SearchEvaluations::default());
        let answer = with_native_ivf_seal_observer_for_test(observer.clone(), || {
            reader.execute(&sql, &params)
        });
        let spent = observer.evaluations.load(Ordering::SeqCst);
        println!("declared_work work={work} spent={spent}");
        assert!(
            spent <= work,
            "work={work}: opening and searching spent {spent} evaluations, more than the \
             request declared"
        );
        match answer {
            Ok(answer) => {
                assert!(
                    spent < (PARTITIONS * ROWS_PER_PARTITION) as u64,
                    "work={work}: an answered search does strictly less work than EXACT"
                );
                assert_eq!(
                    answer.rows().len(),
                    LIMIT,
                    "work={work}: never a short page"
                );
            }
            Err(error) => assert!(
                format!("{error:?}").contains("Work"),
                "work={work}: a find that cannot finish inside the limit is a typed work \
                 refusal, got {error:?}"
            ),
        }
    }
}
