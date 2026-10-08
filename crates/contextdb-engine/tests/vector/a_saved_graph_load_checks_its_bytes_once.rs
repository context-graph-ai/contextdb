//! A fresh reader's first find loads the saved graph of every partition it
//! searches, and checks each one's bytes ONCE.
//!
//! A saved graph is immutable and its record carries a digest over every byte
//! of it, checked when the record is read. Sizing the load and decoding it
//! read those same bytes again; hashing them again proves nothing the record's
//! digest has not already proved, and on a 12,500-passage partition each extra
//! pass is a full walk over about 174 MB. At eight Contexts that was the whole
//! of a first find's 5,000 ms ceiling.
//!
//! The promise is measured on the file route through a real `ReadSession`,
//! the door the consumer reads through, with a fresh reader per scope so each
//! scope's first find pays for exactly its own loads:
//!
//! - every load makes one full hash pass, over its own saved bytes and the few
//!   bytes of record around them, at one, two and five partitions;
//! - a byte flipped inside one partition's saved graph is still refused: the
//!   route is quarantined and an INDEXED find over it gets the typed refusal,
//!   while the undamaged partition beside it still answers.

use contextdb_core::find_attribution::{LoadMeters, load_meters};
use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

const PARTITIONS: usize = 5;
const ROWS_PER_PARTITION: usize = 400;
const DIMENSIONS: usize = 16;
pub(crate) const LIMIT: usize = 10;

/// How many Contexts a fixture splits its rows over, how many rows each holds,
/// and how wide their vectors are.
#[derive(Clone, Copy)]
pub(crate) struct Shape {
    pub(crate) partitions: usize,
    pub(crate) rows_per_partition: usize,
    pub(crate) dimensions: usize,
}

const SHAPE: Shape = Shape {
    partitions: PARTITIONS,
    rows_per_partition: ROWS_PER_PARTITION,
    dimensions: DIMENSIONS,
};

/// What one load may hash besides its saved graph: the rest of the record
/// that carries it (identity, boundaries, the digests themselves) and its
/// framing. Read off the record's shape, a few hundred bytes; rounded up.
const RECORD_BYTES_AROUND_A_GRAPH: u64 = 4 * 1024;

pub(crate) fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC4EC_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

pub(crate) fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC4EC_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn embedding(ordinal: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|axis| {
            let seed = (ordinal
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn query_vector(dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|axis| {
            let seed = axis.wrapping_mul(0x27d4_eb2f) % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

/// A reader's limits with `memory` bytes of memory.
pub(crate) fn limits(memory: usize) -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: memory as u64,
        ..ReadLimits::default()
    }
}

/// Room enough that nothing in these fixtures is ever refused for memory.
pub(crate) const ROOMY: usize = 512 * 1024 * 1024;

pub(crate) fn index() -> VectorIndexRef {
    VectorIndexRef::new("scoped_rows", "embedding")
}

#[cfg(unix)]
pub(crate) struct Fixture {
    pub(crate) _directory: tempfile::TempDir,
    pub(crate) path: std::path::PathBuf,
    pub(crate) runtime: std::path::PathBuf,
    pub(crate) shape: Shape,
}

#[cfg(unix)]
fn seed() -> Fixture {
    seed_shape(SHAPE)
}

#[cfg(unix)]
pub(crate) fn seed_shape(shape: Shape) -> Fixture {
    seed_partition_rows(shape, &vec![shape.rows_per_partition; shape.partitions])
}

#[cfg(unix)]
pub(crate) fn seed_partition_rows(shape: Shape, rows: &[usize]) -> Fixture {
    use std::os::unix::fs::PermissionsExt;

    assert_eq!(rows.len(), shape.partitions);
    assert!(rows.iter().all(|&count| count <= shape.rows_per_partition));

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("saved-graph-checks.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
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
                "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR({}) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1)",
                shape.dimensions
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
    for (partition, &row_count) in rows.iter().enumerate().take(shape.partitions) {
        for local in 0..row_count {
            let ordinal = partition * shape.rows_per_partition + local;
            admin
                .execute(
                    "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(ordinal as i64 + 1)),
                        ("context".into(), Value::Uuid(context_id(partition))),
                        ("generation".into(), Value::Uuid(generation_id(partition))),
                        (
                            "embedding".into(),
                            Value::Vector(embedding(ordinal, shape.dimensions)),
                        ),
                    ]),
                )
                .expect("seed one partitioned row");
        }
    }
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
            .partition_infos(&index(), &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if ready.len() == shape.partitions
            && ready.iter().all(|partition| partition.graph_available)
        {
            admin.close().expect("seal the fixture store");
            return Fixture {
                _directory: directory,
                path,
                runtime,
                shape,
            };
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

pub(crate) fn statement(
    shape: Shape,
    partitions: &[usize],
    route: &str,
) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        (
            "query".into(),
            Value::Vector(query_vector(shape.dimensions)),
        ),
    ]);
    let mut contexts = Vec::with_capacity(partitions.len());
    let mut generations = Vec::with_capacity(partitions.len());
    for &partition in partitions {
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
             AND is_current = $is_current ORDER BY embedding <=> $query {route} LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

#[cfg(unix)]
fn fresh_reader(fixture: &Fixture) -> ReadSession {
    fresh_reader_within(fixture, ROOMY)
}

/// A fresh reader over the fixture's committed file whose memory limit is
/// `memory` bytes.
#[cfg(unix)]
pub(crate) fn fresh_reader_within(fixture: &Fixture, memory: usize) -> ReadSession {
    let reader = ReadSession::with_runtime_directory_for_test(&fixture.runtime, || {
        ReadSession::open_with_options(
            &fixture.path,
            ReadSessionOptions {
                contexts: Some(
                    (0..fixture.shape.partitions)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits: limits(memory),
                memory_limit: Some(memory),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

pub(crate) fn ids(result: &contextdb_engine::QueryResult) -> Vec<Value> {
    result.rows.iter().map(|row| row[0].clone()).collect()
}

pub(crate) fn since(before: LoadMeters, after: LoadMeters) -> LoadMeters {
    LoadMeters {
        loads: after.loads - before.loads,
        saved_bytes: after.saved_bytes - before.saved_bytes,
        hash_passes: after.hash_passes - before.hash_passes,
        hashed_bytes: after.hashed_bytes - before.hashed_bytes,
        priced_bytes: after.priced_bytes - before.priced_bytes,
        minimum_bytes: after.minimum_bytes - before.minimum_bytes,
        admitted_bytes: after.admitted_bytes - before.admitted_bytes,
        held_bytes: after.held_bytes - before.held_bytes,
        refused: after.refused - before.refused,
        cache_bytes: after.cache_bytes - before.cache_bytes,
        given_back_bytes: after.given_back_bytes - before.given_back_bytes,
        least_cache_loads: after.least_cache_loads - before.least_cache_loads,
    }
}

#[cfg(unix)]
#[test]
fn a_fresh_readers_first_find_hashes_each_saved_graph_it_loads_once() {
    let fixture = seed();
    let mut failures = Vec::new();
    for selected in [1_usize, 2, PARTITIONS] {
        let partitions = (0..selected).collect::<Vec<_>>();
        let (sql, params) = statement(SHAPE, &partitions, "USE VECTOR INDEXED");
        let reader = fresh_reader(&fixture);
        let before = load_meters();
        let answer = reader
            .execute(&sql, &params)
            .expect("the first indexed find answers");
        let spent = since(before, load_meters());
        println!(
            "saved graph checks: partitions={selected} loads={} saved_bytes={} \
             hash_passes={} hashed_bytes={}",
            spent.loads, spent.saved_bytes, spent.hash_passes, spent.hashed_bytes,
        );

        assert_eq!(
            answer.rows.len(),
            LIMIT,
            "at {selected} partitions a full page"
        );
        assert!(
            spent.loads >= selected as u64,
            "at {selected} partitions the first find loads every searched partition's saved \
             graph ({} loads)",
            spent.loads
        );
        if spent.hash_passes != spent.loads {
            failures.push(format!(
                "at {selected} partitions {} loads made {} full hash passes; one each is the \
                 promise",
                spent.loads, spent.hash_passes
            ));
        }
        if spent.hashed_bytes < spent.saved_bytes
            || spent.hashed_bytes > spent.saved_bytes + spent.loads * RECORD_BYTES_AROUND_A_GRAPH
        {
            failures.push(format!(
                "at {selected} partitions the loads hashed {} bytes for {} saved bytes; each \
                 saved byte once, plus at most {RECORD_BYTES_AROUND_A_GRAPH} bytes of record \
                 per load, is the promise",
                spent.hashed_bytes, spent.saved_bytes
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[cfg(unix)]
#[test]
fn a_byte_flipped_inside_a_saved_graph_is_refused_as_damage() {
    for filtered_window in [false, true] {
        let fixture = seed();
        if filtered_window {
            let admin = Database::open_with_options(
                &fixture.path,
                DatabaseOpenOptions {
                    owner_reads: OwnerReadConfig {
                        runtime_dir: Some(fixture.runtime.clone()),
                        ..OwnerReadConfig::default()
                    },
                    ..DatabaseOpenOptions::default()
                },
            )
            .expect("open the fixture to make each pair require a filtered window");
            for id in [1_i64, ROWS_PER_PARTITION as i64 + 1] {
                admin
                    .execute(
                        "UPDATE scoped_rows SET is_current = FALSE WHERE id = $id",
                        &HashMap::from([("id".into(), Value::Int64(id))]),
                    )
                    .expect("one rejected row prevents the whole-pair shortcut");
            }
            admin.close().expect("seal the filtered fixture");
        }
        let damaged = VectorPartitionRef::new(
            index(),
            VectorPartitionKey::from_values(&[
                Value::Uuid(context_id(0)),
                Value::Uuid(generation_id(0)),
            ])
            .expect("two UUID partition components have one canonical key"),
        );
        Database::__corrupt_vector_partition_saved_graph_for_test(&fixture.path, &damaged)
            .expect("flip one byte inside partition zero's saved graph");

        let reader = fresh_reader(&fixture);
        let (sql, params) = statement(SHAPE, &[0], "USE VECTOR INDEXED");
        let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        let refused = reader.execute(&sql, &params);
        let counters = work.counters();
        drop(work);
        assert!(
            matches!(
                &refused,
                Err(Error::VectorIndexedRouteUnavailable { index: refused }) if *refused == index()
            ),
            "filtered_window={filtered_window}: damage refuses INDEXED, got {refused:?}"
        );
        if filtered_window {
            assert!(
                counters.verified_runs_built > 0,
                "the damaged find used authenticated filtered windows: {counters:?}"
            );
        } else {
            assert_eq!(
                counters.verified_runs_built, 0,
                "the whole-pair path enumerates no posting window: {counters:?}"
            );
            assert!(
                counters.verified_prefix_probes > 0,
                "the whole-pair path proves its pair: {counters:?}"
            );
        }

        let (sql, params) = statement(SHAPE, &[1], "USE VECTOR INDEXED");
        let (exact_sql, exact_params) = statement(SHAPE, &[1], "USE VECTOR EXACT");
        let oracle = fresh_reader(&fixture)
            .execute(&exact_sql, &exact_params)
            .expect("the exhaustive route answers the undamaged partition");
        let healthy = fresh_reader(&fixture);
        let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        let answer = healthy
            .execute(&sql, &params)
            .expect("the healthy neighbour still answers through its saved graph");
        let counters = work.counters();
        drop(work);
        assert_eq!(ids(&answer), ids(&oracle));
        assert_eq!(
            counters.verified_runs_built > 0,
            filtered_window,
            "the healthy neighbour witnesses the same candidate route: {counters:?}"
        );
    }
}
