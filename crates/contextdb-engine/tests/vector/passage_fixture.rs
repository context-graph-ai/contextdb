//! The passage fixture the serving-page harnesses read.
//!
//! One Context, one generation, `rows` passages of 768 dimensions, maintained
//! until every partition has a graph. It lives under this build's own scratch
//! directory and each harness asks for it by size, so a harness seeds what it
//! needs instead of depending on a sibling having run first.

#![allow(dead_code)]

use contextdb_core::read_contract::ReadRoute;
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

pub const DIMENSIONS: usize = 768;

pub fn context_id() -> Uuid {
    Uuid::from_u128(0xC04C_0000_0000_0000_0000_0000_0000_0000)
}

pub fn generation_id() -> Uuid {
    Uuid::from_u128(0xC04C_1000_0000_0000_0000_0000_0000_0000)
}

pub fn embedding(ordinal: usize) -> Vec<f32> {
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

pub fn params(query_ordinal: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(embedding(query_ordinal))),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ])
}

pub fn find_sql(mode: &str, limit: usize) -> String {
    format!(
        "SELECT id FROM passages WHERE context_id IN ($context) AND \
         generation_id IN ($generation) AND is_current = $is_current \
         ORDER BY embedding <=> $query {mode} LIMIT {limit}"
    )
}

pub fn answer_ids(answer: &contextdb_engine::QueryResult) -> Vec<i64> {
    answer
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the projection returns one integer id, got {other:?}"),
        })
        .collect()
}

/// The fixture directory for `rows` passages, seeded if this build has not
/// seeded it yet. `CONTEXTDB_P1_EXACT_DIR` overrides where it lives.
#[cfg(unix)]
pub fn seeded(rows: usize) -> (std::path::PathBuf, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let root =
        std::path::PathBuf::from(std::env::var("CONTEXTDB_P1_EXACT_DIR").unwrap_or_else(|_| {
            // Keep this fixture in the current build's scratch directory.
            format!("{}/exact-auth", env!("CARGO_TARGET_TMPDIR"))
        }))
        .join(format!("rows-{rows}"));
    let path = root.join("passages.db");
    let runtime = root.join("runtime");
    if !path.exists() {
        std::fs::create_dir_all(&runtime).expect("the reader runtime directory");
        seed(&path, &runtime, rows);
    }
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    (path, runtime)
}

#[cfg(unix)]
pub fn open_reader(
    path: &std::path::Path,
    runtime: &std::path::Path,
    memory_limit: Option<usize>,
) -> contextdb_core::Result<ReadSession> {
    ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                memory_limit,
                ..ReadSessionOptions::default()
            },
        )
    })
}

/// Asserts the reader took the committed-file route, which is the one these
/// harnesses are about.
#[cfg(unix)]
pub fn open_file_reader(
    path: &std::path::Path,
    runtime: &std::path::Path,
    memory_limit: Option<usize>,
) -> ReadSession {
    let reader = open_reader(path, runtime, memory_limit)
        .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path, rows: usize) {
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
    for ordinal in 0..rows {
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
        if !ready.is_empty() && ready.iter().all(|partition| partition.graph_available) {
            break;
        }
    }
    admin.close().expect("seal the fixture store");
}
