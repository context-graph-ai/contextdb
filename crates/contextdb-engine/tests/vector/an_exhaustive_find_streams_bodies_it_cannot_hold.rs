//! A find that has to compare rows over every authorized Context must answer
//! inside the memory its reader declared, by streaming the stored vectors one
//! at a time instead of loading whole Contexts of them.
//!
//! The automatic route compares rows exactly wherever a partition's maintained
//! graph cannot answer -- it does not fit the declared memory, its chain is
//! incomplete, or it came back short. That exhaustive work must score each
//! row's own stored vector, never the graph's copy, and it must not need room
//! for a whole partition of those vectors at once: a reader declared for a few
//! megabytes can hold one vector at a time, and that is all an exhaustive
//! comparison needs. Loading each partition whole would refuse a broad find
//! for memory the answer never needs.
//!
//! Measured on the file route through a real `ReadSession`. Every partition's
//! graph is made to come back short, which sends the whole scope down the
//! exhaustive route, and the reader's budget is set from what it holds before
//! the find so that it cannot hold even one partition's vectors.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions, VectorSearchRoute,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 600;
const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;

/// What loading one partition's stored vectors whole reserves: the resident F32
/// bodies plus the same again as decode workspace, before the one-body scratch.
const ONE_RAW_PARTITION_BYTES: u64 =
    (ROWS_PER_PARTITION * DIMENSIONS * std::mem::size_of::<f32>() * 2) as u64;

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xE4AC_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xE4AC_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
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

/// The independent oracle: the inserted vectors themselves, compared in full.
fn oracle_top_ids() -> Vec<i64> {
    let query = query_vector();
    let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
    let mut scored = (0..PARTITIONS * ROWS_PER_PARTITION)
        .map(|ordinal| {
            let vector = embedding(ordinal);
            let dot = vector.iter().zip(&query).map(|(a, b)| a * b).sum::<f32>();
            let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
            (ordinal as i64 + 1, dot / (norm * query_norm))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    scored.truncate(LIMIT);
    scored.into_iter().map(|(id, _)| id).collect()
}

#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path) {
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
            &format!(
                "CREATE TABLE streamed_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
                 generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
                 embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id, generation_id) \
                 MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1)"
            ),
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX streamed_rows_active_idx ON streamed_rows \
             (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            admin
                .execute(
                    "INSERT INTO streamed_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
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
    let index = VectorIndexRef::new("streamed_rows", "embedding");
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
        if ready.len() == PARTITIONS && ready.iter().all(|partition| partition.graph_available) {
            admin.close().expect("seal the fixture store");
            return;
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

fn statement() -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector())),
    ]);
    let mut contexts = Vec::with_capacity(PARTITIONS);
    let mut generations = Vec::with_capacity(PARTITIONS);
    for partition in 0..PARTITIONS {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    (
        format!(
            "SELECT id FROM streamed_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

#[cfg(unix)]
fn open_reader(path: &std::path::Path, runtime: &std::path::Path, memory: u64) -> ReadSession {
    // This proof is about memory. The time allowance is wide so an unoptimized
    // test build on a loaded machine is not what decides it.
    let limits = ReadLimits {
        work: 50_000_000,
        active_ms: 600_000,
        memory,
        ..ReadLimits::default()
    };
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
                memory_limit: Some(memory as usize),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the find returns ids, got {other:?}"),
        })
        .collect()
}

#[cfg(unix)]
#[test]
fn an_exhaustive_find_over_every_context_streams_what_it_cannot_hold() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("streamed-exhaustive.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    seed(&path, &runtime);
    let index = VectorIndexRef::new("streamed_rows", "embedding");
    let (sql, params) = statement();
    let oracle = oracle_top_ids();

    // What the reader holds before it searches, read off a reader that has room
    // for everything, so the tight budget below is set from the reader's own
    // footprint rather than guessed.
    let probe = open_reader(&path, &runtime, 512 * 1024 * 1024);
    let opened_bytes = probe
        .file_memory_for_test()
        .expect("a file-route reader reports its memory")
        .0
        .used as u64;
    drop(probe);

    // Room for everything the find needs except one partition's vectors whole.
    let budget = opened_bytes + ONE_RAW_PARTITION_BYTES / 2;
    let reader = open_reader(&path, &runtime, budget);
    let store = reader
        .file_vector_store_for_test()
        .expect("a file-route reader searches its own vector store");
    // Every graph comes back empty, so every partition is answered by comparing
    // its rows -- the exhaustive route this proof is about.
    let _short = store.cap_graph_candidates_for_test(&index, 0);
    let before = reader
        .file_vector_passive_activity_for_test()
        .expect("a file-route reader reports its vector activity");

    let answer = reader.execute(&sql, &params);
    let after = reader
        .file_vector_passive_activity_for_test()
        .expect("a file-route reader reports its vector activity");
    let peak = reader
        .file_memory_for_test()
        .expect("a file-route reader reports its memory")
        .2 as u64;
    println!(
        "streamed exhaustive opened={opened_bytes} budget={budget} peak={peak} \
         one_raw_partition={ONE_RAW_PARTITION_BYTES} raw_partition_loads={} \
         raw_candidate_point_loads={} answer={:?}",
        after.raw_partition_loads - before.raw_partition_loads,
        after.raw_candidate_point_loads - before.raw_candidate_point_loads,
        answer.as_ref().map(|answer| ids(&answer.rows)),
    );
    let answer = answer.expect(
        "a find over every Context answers inside a budget that cannot hold one partition's vectors",
    );
    assert_eq!(
        ids(&answer.rows),
        oracle,
        "the exhaustive route returns exactly the oracle's nearest rows from the stored vectors",
    );
    assert_eq!(
        after.raw_partition_loads, before.raw_partition_loads,
        "the exhaustive route never loads a partition's vectors whole",
    );
    assert!(
        peak <= budget,
        "the reader never holds more than it declared: peak {peak} against {budget}",
    );

    drop(_short);
    drop(reader);
    let damaged = VectorPartitionRef::new(
        index,
        VectorPartitionKey::from_values(&[
            Value::Uuid(context_id(0)),
            Value::Uuid(generation_id(0)),
        ])
        .expect("the damaged partition has one canonical key"),
    );
    Database::__corrupt_vector_partition_saved_graph_for_test(&path, &damaged)
        .expect("flip one byte in a saved graph without changing any row body");
    let damaged_reader = open_reader(&path, &runtime, budget);
    let exact_sql = sql.replace(" LIMIT ", " USE VECTOR EXACT LIMIT ");
    let exact = damaged_reader
        .execute(&exact_sql, &params)
        .expect("EXACT reads authoritative bodies when a graph is damaged");
    assert_eq!(ids(&exact.rows), oracle);
    let damaged_store = damaged_reader
        .file_vector_store_for_test()
        .expect("the damaged reader still has a vector store");
    let _short = damaged_store.cap_graph_candidates_for_test(&damaged.index, 0);
    let auto = damaged_reader
        .execute(&sql, &params)
        .expect("AUTO can compare authoritative bodies after graph damage");
    assert_eq!(ids(&auto.rows), oracle);
    assert_eq!(
        auto.trace
            .vector_search
            .as_ref()
            .and_then(|search| search.route),
        Some(VectorSearchRoute::Exact),
        "the damaged graph cannot provide an exact fallback answer"
    );
}
