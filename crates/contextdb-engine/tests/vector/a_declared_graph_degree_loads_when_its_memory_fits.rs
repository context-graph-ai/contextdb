//! A graph degree the schema owner declares stays usable whenever the declared
//! memory can hold the graph it builds.
//!
//! A larger declared degree gives every point a longer neighbour list, so the
//! saved graph needs more memory once a fresh reader loads it. The reader must
//! price that load from the saved graph itself before it allocates anything:
//! with enough memory, a fresh `INDEXED` find uses the saved graph, rebuilds
//! nothing and answers exactly what the same graph answered before it was
//! saved; with too little, the find is refused through the ordinary memory
//! budget error before the graph is allocated, never by the reader tripping
//! over its own estimate afterwards. This runs at a high declared degree and
//! at the default, writing through the production file-backed store and
//! reading through a fresh file reader.

use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, ReadSession, ReadSessionOptions};
use contextdb_vector::hnsw::{
    HnswBuildObserver, PagedVectorObserver, with_hnsw_build_observer, with_paged_vector_observer,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const ROWS: usize = 1_250;
const DIMENSION: usize = 64;
const QUERIES: usize = 10;
const MAX_MAINTENANCE_CYCLES: usize = 64;

struct BuildCounter(AtomicU64);

impl HnswBuildObserver for BuildCounter {
    fn before_hnsw_build(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Vectors scored out of the saved graph's pages: nonzero only when the find
/// walked the saved graph through the file reader's paged route.
struct PagedScores(AtomicU64);

impl PagedVectorObserver for PagedScores {
    fn vector_decoded(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("degree_items", "embedding")
}

fn scope() -> Uuid {
    Uuid::from_u128(0xdec)
}

fn partition() -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope())]).expect("a UUID partition key")
}

fn vector(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

const FIND: &str = "SELECT id FROM degree_items WHERE scope_id = $scope \
                    ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";

fn query(query: usize) -> HashMap<String, Value> {
    params([
        ("scope", Value::Uuid(scope())),
        ("query", Value::Vector(vector(1_000_000 + query))),
    ])
}

fn find(db: &Database, ordinal: usize) -> Result<Vec<Uuid>, Error> {
    db.execute(FIND, &query(ordinal)).map(|result| ids(&result))
}

fn seed(path: &std::path::Path, declared: &str) -> Vec<Vec<Uuid>> {
    let db = Database::open(path).expect("open the file-backed seed store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE degree_items (\
             id UUID PRIMARY KEY, \
             scope_id UUID NOT NULL, \
             embedding VECTOR({DIMENSION}) \
             PARTITION_KEY (scope_id) MAX_PARTITIONS 4 \
             SEARCH_MODE INDEXED{declared}\
             )"
        ),
        &HashMap::new(),
    )
    .expect("create the declared-degree fixture");
    let tx = db.begin_or_panic();
    for ordinal in 0..ROWS {
        db.insert_row(
            tx,
            "degree_items",
            params([
                ("id", Value::Uuid(Uuid::from_u128(ordinal as u128 + 1))),
                ("scope_id", Value::Uuid(scope())),
                ("embedding", Value::Vector(vector(ordinal))),
            ]),
        )
        .expect("stage one deterministic vector row");
    }
    db.commit(tx).expect("commit the partition");
    let ready = || {
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        db.vector_store_for_test()
            .partition_info(&index(), &partition(), &mut partition_observation_owner_1)
            .unwrap()
            .is_some_and(|info| info.graph_available)
    };
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if ready() {
            break;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle returns");
    }
    assert!(ready(), "maintenance publishes the declared graph");
    let answers = (0..QUERIES)
        .map(|query| find(&db, query).expect("the maintained graph answers before saving"))
        .collect();
    db.close().expect("close the sealed fixture");
    answers
}

/// A fresh file reader over the saved store, under the session memory limit
/// given, and what it holds charged once open.
fn reader(path: &std::path::Path, memory_limit: Option<usize>) -> (ReadSession, usize) {
    let runtime = path.with_extension("runtime");
    std::fs::create_dir_all(&runtime).expect("create the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let session = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                memory_limit,
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open a fresh file reader over the saved store");
    assert_eq!(
        session.route(),
        contextdb_core::read_contract::ReadRoute::File
    );
    let held = charged(&session);
    (session, held)
}

fn charged(session: &ReadSession) -> usize {
    session
        .file_memory_for_test()
        .expect("the file route reports its memory")
        .0
        .used
}

#[test]
fn a_declared_graph_degree_loads_when_its_memory_fits_and_is_refused_when_it_does_not() {
    let mut broken = Vec::new();
    for (label, declared) in [
        ("default degree", ""),
        (
            "declared degree 128",
            " HNSW (M = 128, EF_CONSTRUCTION = 128)",
        ),
    ] {
        let directory = TempDir::new().expect("temporary store directory");
        let path = directory.path().join("declared-degree.db");
        let before = seed(&path, declared);

        // Enough memory: the fresh reader loads the saved graph and answers
        // from it, exactly as the same graph answered before it was saved.
        let (session, opened) = reader(&path, None);
        let builds = Arc::new(BuildCounter(AtomicU64::new(0)));
        let scored = Arc::new(PagedScores(AtomicU64::new(0)));
        let mut after = Vec::new();
        let loaded = with_hnsw_build_observer(builds.clone(), || {
            with_paged_vector_observer(scored.clone(), || {
                for ordinal in 0..QUERIES {
                    after.push(ids(&session.execute(FIND, &query(ordinal))?));
                }
                Ok::<_, Error>(())
            })
        });
        let rebuilt = builds.0.load(Ordering::SeqCst);
        let from_pages = scored.0.load(Ordering::SeqCst);
        let charge = charged(&session).saturating_sub(opened);
        println!(
            "degree_load case=\"{label}\" result={loaded:?} builds={rebuilt} \
             paged_scores={from_pages} charge={charge} opened={opened}"
        );
        match loaded {
            Err(error) => broken.push(format!(
                "{label}: a fresh reader with no memory limit could not answer from the saved \
                 graph: {error}"
            )),
            Ok(()) => {
                if rebuilt != 0 || from_pages == 0 {
                    broken.push(format!(
                        "{label}: the fresh reader must answer from the saved graph and build \
                         nothing; it built {rebuilt} and scored {from_pages} vectors from \
                         saved pages"
                    ));
                }
                if after != before {
                    broken.push(format!(
                        "{label}: the saved graph answered differently in a fresh reader"
                    ));
                }
            }
        }
        drop(session);
        assert!(
            charge > 0,
            "{label}: a fresh reader over a saved graph must retain a positive charge, not 0"
        );

        // Too little memory. Under a limit the reader shrinks its serving
        // cache to fit, down to one page, so the part of the load that cannot
        // shrink is the graph's own topology -- its points, neighbour lists
        // and row mappings, about half of what the unlimited load kept at both
        // degrees. A quarter of that charge cannot hold the topology. What a
        // reader holds once open also follows its limit, so the limit is
        // settled on the headroom a limited reader actually has left. The
        // find must then be refused through the memory budget before the
        // graph is allocated, and leave the reader's charge where it was.
        let headroom = charge / 4;
        let mut limit = opened + headroom;
        let (mut session, mut opened) = reader(&path, Some(limit));
        for _ in 0..8 {
            // Within an eighth: a reader's open cost moves by a few bytes
            // with its limit, and a third of the charge is still below the
            // topology.
            if limit - opened <= headroom + headroom / 8 {
                break;
            }
            limit = opened + headroom;
            (session, opened) = reader(&path, Some(limit));
        }
        assert!(
            limit - opened <= headroom + headroom / 8,
            "{label}: a limited reader settles on {} bytes of headroom",
            limit - opened
        );
        let refused = session.execute(FIND, &query(0));
        let left = charged(&session);
        println!("degree_refusal case=\"{label}\" result={refused:?} left={left} opened={opened}");
        match &refused {
            Err(Error::MemoryBudgetExceeded { operation, .. })
                if operation == "load_retained_vector_generation" => {}
            Err(Error::MemoryBudgetExceeded { operation, .. }) => {
                broken.push(format!(
                    "{label}: below the memory the load needs, the find must be refused by the \
                     memory budget for operation load_retained_vector_generation, not \
                     {operation}"
                ));
            }
            _ => {
                broken.push(format!(
                    "{label}: below the memory the load needs, the find must be refused by the \
                     memory budget; it returned {refused:?}"
                ));
            }
        }
        if left != opened {
            broken.push(format!(
                "{label}: a refused load leaves {left} bytes charged against {opened} before it"
            ));
        }
    }
    assert!(broken.is_empty(), "{}", broken.join("\n"));
}
