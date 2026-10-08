//! An exhaustive search authenticates every authoritative body while sharing
//! the upper proof nodes it reads across candidates.
//!
//! Body verification and proof-node work are separate counters. A small proof
//! count alone cannot certify an exhaustive answer: every selected body must
//! still be verified against the committed root.
//!
//! Two shortcuts could make this number small while breaking the route.
//!
//! Scoring from maintained generation serving pages would make exhaustive
//! search depend on the index. EXACT compares every allowed vector; for F32,
//! it compares the inserted F32 vector even when the graph is reclaimed or corrupt.
//!
//! Walking one authenticated RANGE over the column's bodies does not exist to be
//! walked: `mutation_tree_key` (`content_identity.rs:1033-1053`) keeps key order
//! only for ordered postings and HASHES every other family's key, and the
//! physical vector table has no partition key, so a column scan would read other
//! Contexts and history, breaking the fixed-scope query bound.
//!
//! The directory's selected version identities are read through one pinned
//! transaction with a bounded authentication batch. The assertions below
//! separately grade bodies verified and the distinct proof paths loaded.
//!
//! The answer may not change by one row: this is an overhead repair, and the
//! proof asserts the published ids are identical to the route's own baseline.

use contextdb_core::read_contract::ReadRoute;
use contextdb_core::types::ContextId;
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ExactVectorBodyDamage, prepare_exact_vector_body_damage_for_test,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;
use uuid::Uuid;

const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;
/// A 64 KiB serving page holds about twenty-one 768-dimension vectors. Two
/// authenticated vector identities per body and a fixed allowance for the
/// roots and candidate-index paths bound distinct proof nodes.
fn proof_node_ceiling(rows: usize) -> u64 {
    (2 * rows.div_ceil(21) + 64) as u64
}

fn context_id() -> Uuid {
    Uuid::from_u128(0xC04C_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC04C_1000_0000_0000_0000_0000_0000_0000)
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

fn rows() -> usize {
    std::env::var("CONTEXTDB_P1_EXACT_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_250)
}

#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path) {
    seed_with_rows(path, runtime, rows());
}

#[cfg(unix)]
fn seed_with_rows(path: &std::path::Path, runtime: &std::path::Path, row_count: usize) {
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
    for ordinal in 0..row_count {
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

#[cfg(unix)]
#[test]
#[ignore = "large exact-search diagnostic; run explicitly when measuring body and proof work"]
fn an_exact_search_authenticates_pages_not_every_vector() {
    use std::os::unix::fs::PermissionsExt;

    let root =
        std::path::PathBuf::from(std::env::var("CONTEXTDB_P1_EXACT_DIR").unwrap_or_else(|_| {
            // Keep this fixture in the current build's scratch directory.
            format!("{}/exact-auth", env!("CARGO_TARGET_TMPDIR"))
        }))
        .join(format!("rows-{}", rows()));
    let path = root.join("passages.db");
    let runtime = root.join("runtime");
    if !path.exists() {
        std::fs::create_dir_all(&runtime).expect("the reader runtime directory");
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
            .expect("the runtime directory is private to its owner");
        seed(&path, &runtime);
    }

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);

    let params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(embedding(0))),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ]);
    let sql = |mode: &str| {
        format!(
            "SELECT id FROM passages WHERE context_id IN ($context) AND \
             generation_id IN ($generation) AND is_current = $is_current \
             ORDER BY embedding <=> $query {mode} LIMIT {LIMIT}"
        )
    };
    let ids = |answer: &contextdb_engine::QueryResult| {
        answer
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("the projection returns one integer id, got {other:?}"),
            })
            .collect::<Vec<_>>()
    };

    // Ground truth, computed here rather than borrowed from another route. An
    // exhaustive search IS the true top-k by definition, so the contract is
    // that it returns exactly that -- not that it agrees with the graph route,
    // which is approximate on purpose and legitimately differs.
    let query = embedding(0);
    let mut ranked: Vec<(i64, f32)> = (0..rows())
        .map(|ordinal| {
            (
                ordinal as i64 + 1,
                contextdb_vector::cosine_similarity(&query, &embedding(ordinal + 1)),
            )
        })
        .collect();
    ranked.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.0.cmp(&right.0))
    });
    let true_top: Vec<i64> = ranked.iter().take(LIMIT).map(|(id, _)| *id).collect();

    let exact_sql = sql("USE VECTOR EXACT");
    let observed = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let started = Instant::now();
    let answer = reader.execute(&exact_sql, &params);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1e3;
    let counters = observed.counters();
    drop(observed);

    let answer = match answer {
        Ok(answer) => answer,
        Err(refusal) => panic!(
            "an exhaustive search over {} vectors must answer inside its active budget, got \
             {refusal:?} after {elapsed_ms:.0} ms",
            rows()
        ),
    };
    println!(
        "exact_auth rows={} warm_ms={elapsed_ms:.1} bodies_verified={} proof_nodes_read={} \
         proof_nodes_hashed={} hashed_vectors={} hashed_bytes={} decoded_vectors={} ids={:?}",
        rows(),
        counters.authoritative_bodies_verified,
        counters.proof_nodes_read,
        counters.proof_nodes_hashed,
        counters.hashed_vectors,
        counters.hashed_bytes,
        counters.decoded_vectors,
        ids(&answer),
    );

    assert_eq!(
        ids(&answer),
        true_top,
        "an exhaustive search returns the true top ten, whatever unit its vectors are \
         authenticated in"
    );
    assert_eq!(
        counters.authoritative_bodies_verified,
        rows() as u64,
        "the exhaustive route authenticates every candidate body"
    );
    assert_eq!(counters.proof_nodes_read, counters.proof_nodes_hashed);
    assert!(
        counters.proof_nodes_hashed <= proof_node_ceiling(rows()),
        "an exhaustive search over {} vectors hashed {} proof nodes; distinct proof paths \
         have a ceiling of {} for this partition",
        rows(),
        counters.proof_nodes_hashed,
        proof_node_ceiling(rows()),
    );
}

#[cfg(unix)]
#[test]
fn exact_search_refuses_damaged_missing_and_mismatched_authoritative_bodies() {
    use std::os::unix::fs::PermissionsExt;

    let sql = "SELECT id FROM passages WHERE context_id IN ($context) AND \
               generation_id IN ($generation) AND is_current = $is_current \
               ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10";
    let params = HashMap::from([
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(embedding(1))),
    ]);
    for damage in [
        ExactVectorBodyDamage::MalformedBody,
        ExactVectorBodyDamage::MissingBody,
        ExactVectorBodyDamage::MismatchedLeaf,
    ] {
        for expected_route in [ReadRoute::Owner, ReadRoute::File] {
            let directory = tempfile::TempDir::new().expect("task-scoped damaged-body fixture");
            let path = directory.path().join("passages.db");
            let runtime = directory.path().join("runtime");
            std::fs::create_dir(&runtime).expect("reader runtime directory");
            std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
                .expect("private reader runtime");
            seed_with_rows(&path, &runtime, 32);
            prepare_exact_vector_body_damage_for_test(&path, damage)
                .expect("plant one physical body defect without changing its committed root");
            let owner = if expected_route == ReadRoute::Owner {
                Some(
                    Database::open_with_options(
                        &path,
                        DatabaseOpenOptions {
                            owner_reads: OwnerReadConfig {
                                runtime_dir: Some(runtime.clone()),
                                ..OwnerReadConfig::default()
                            },
                            ..DatabaseOpenOptions::default()
                        },
                    )
                    .expect("the owner opens before reading the damaged body"),
                )
            } else {
                None
            };
            let opened = ReadSession::with_runtime_directory_for_test(&runtime, || {
                ReadSession::open_with_options(
                    &path,
                    ReadSessionOptions {
                        contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                        ..ReadSessionOptions::default()
                    },
                )
            });
            let answer = match opened {
                Ok(reader) => {
                    assert_eq!(reader.route(), expected_route);
                    reader.execute(sql, &params)
                }
                Err(error) => Err(error),
            };
            assert!(
                matches!(answer, Err(Error::StoreCorrupted { .. })),
                "{expected_route:?} {damage:?}: exact read must refuse corrupt authoritative bytes, got {answer:?}"
            );
            if let Some(owner) = owner {
                owner.close().expect("close the damaged owner");
            }
        }
    }
}
