//! Damage to a store is reported, never served.
//!
//! A filtered vector search takes its candidate identities from a declared
//! relational index, and the fast path serves them from a canonical posting run
//! it has already proved and cached. If that cached run is treated as
//! authoritative on its own, a store that has lost exactly one physical posting
//! for a live row answers one row short and says nothing: the row is simply
//! missing from every answer, which is the shape of damage a reader must never
//! hide. Each newly built window is therefore reconciled against the physical
//! index tree before it is delivered.

use contextdb_core::Value;
use contextdb_core::VectorIndexRef;
use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    DurableStoreDamage, busiest_relational_index_for_test, prepare_durable_store_damage_for_test,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use uuid::Uuid;

const PARTITIONS: usize = 4;
const ROWS_PER_PARTITION: usize = 200;
const DIMENSIONS: usize = 8;

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC049_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC049_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
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

fn open_options(runtime: &std::path::Path) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            runtime_dir: Some(runtime.to_path_buf()),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

fn seed(path: &std::path::Path, runtime: &std::path::Path) {
    let db = Database::open_with_options(path, open_options(runtime))
        .expect("open the fixture store for writing");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE posted_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
         embedding VECTOR(8) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 4 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    db.execute(
        "CREATE INDEX posted_vectors_active_idx ON posted_vectors \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the index the candidate identities come from");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            db.execute(
                "INSERT INTO posted_vectors VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one row and its posting");
        }
    }
    for _ in 0..256 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let ready = db
            .vector_store_for_test()
            .partition_infos(&VectorIndexRef::new("posted_vectors", "embedding"))
            .expect("inspect partition readiness");
        if ready.len() == PARTITIONS && ready.iter().all(|partition| db.__debug_vector_partition_indexed_for_test(partition.partition.index.clone(), partition.partition.partition_key.clone())) {
            db.close().expect("close the seeded store");
            return;
        }
    }
    panic!("maintenance did not publish every partition's graph");
}

fn statement() -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(embedding(7))),
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
            "SELECT id FROM posted_vectors WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query LIMIT 10",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

/// The route a consumer reads through. It is the only route that takes its
/// candidate identities from the authenticated posting run, so it is the only
/// route on which the physical index is consulted at all.
fn read(path: &std::path::Path, runtime: &std::path::Path) -> Result<usize, String> {
    let (sql, params) = statement();
    let reader = ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                limits: ReadLimits {
                    work: 5_000_000,
                    memory: 512 * 1024 * 1024,
                    ..ReadLimits::default()
                },
                ..ReadSessionOptions::default()
            },
        )
    })
    .map_err(|error| format!("{error:?}"))?;
    assert_eq!(
        reader.route(),
        ReadRoute::File,
        "this proof measures the file route"
    );
    // Read twice: the first read builds and caches the authenticated posting
    // run, the second is served from that cache. Damage must be reported on
    // both, because a cached run that is trusted on its own is exactly the
    // hole this proof exists to close.
    let first = reader
        .execute(&sql, &params)
        .map_err(|error| format!("{error:?}"))?;
    println!(
        "posting_read first_rows={} plan={} index={:?} pushed={:?} examined={}",
        first.rows.len(),
        first.trace.physical_plan,
        first.trace.index_used,
        first.trace.predicates_pushed,
        first.trace.rows_examined,
    );
    let second = reader
        .execute(&sql, &params)
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(
        first.rows.len(),
        second.rows.len(),
        "the cached run answers what the built run answered"
    );
    Ok(second.rows.len())
}

#[cfg(unix)]
#[test]
fn a_removed_index_posting_is_reported_as_damage() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let private = |path: &std::path::Path| {
        std::fs::create_dir(path).expect("a reader runtime directory");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .expect("the runtime directory is private to its owner");
    };
    let healthy_path = directory.path().join("healthy.db");
    let healthy_runtime = directory.path().join("healthy-runtime");
    let damaged_path = directory.path().join("damaged.db");
    let damaged_runtime = directory.path().join("damaged-runtime");
    private(&healthy_runtime);
    private(&damaged_runtime);
    // Seeded twice rather than copied: the seed is deterministic, and a copy of
    // the store file alone would leave the reader's companion state behind.
    seed(&healthy_path, &healthy_runtime);
    seed(&damaged_path, &damaged_runtime);

    let answered = read(&healthy_path, &healthy_runtime).expect("the undamaged store answers");
    assert_eq!(
        answered, 10,
        "the undamaged store answers a full page, so a short answer afterwards is damage"
    );

    let (damaged_table, damaged_index) = busiest_relational_index_for_test(&damaged_path)
        .expect("name the index about to lose a posting");
    println!("posting_damage table={damaged_table} index={damaged_index}");
    assert_eq!(
        (damaged_table.as_str(), damaged_index.as_str()),
        ("posted_vectors", "posted_vectors_active_idx"),
        "the proof must damage the index the read is served from"
    );
    prepare_durable_store_damage_for_test(
        &damaged_path,
        DurableStoreDamage::MissingRelationalIndexPosting,
    )
    .expect("remove exactly one physical posting for a live row");

    match read(&damaged_path, &damaged_runtime) {
        Err(refusal) => assert!(
            refusal.contains("StoreCorrupted"),
            "a missing physical posting must be reported as damage, not as {refusal}"
        ),
        Ok(served) => panic!(
            "a missing physical posting was served as an answer of {served} rows instead of \
             being reported as damage"
        ),
    }
}
