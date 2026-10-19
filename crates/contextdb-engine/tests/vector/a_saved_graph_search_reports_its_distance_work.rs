//! A search over a saved graph must report every distance it evaluates.
//!
//! The work a graph search saves over EXACT is measured by counting distance
//! evaluations through the candidate-distance observer. A fresh file reader
//! walks the saved graph out of its serving pages rather than out of a
//! resident graph, and that walk evaluates real distances too. If those
//! evaluations never reach the observer, the file route reports no graph work
//! at all and a "strictly fewer evaluations than EXACT" check reads work the
//! counters did not see.
//!
//! Measured through a real `ReadSession` on the file route, on the first
//! search of a fresh reader, over one Context whose saved graph is paged.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, QueryResult, ReadSession,
    ReadSessionOptions, VectorSearchRoute,
};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, PagedVectorObserver, UnionSearchReceipt,
    with_hnsw_candidate_observer, with_paged_vector_observer, with_union_search_receipts,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const ROWS: usize = 2_000;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;

#[derive(Default)]
struct Counts {
    graph: AtomicU64,
    native: AtomicU64,
    decoded: AtomicU64,
}

impl HnswCandidateObserver for Counts {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.graph.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeVectorScoreObserver for Counts {
    fn before_native_vector_score(&self) {
        self.native.fetch_add(1, Ordering::SeqCst);
    }
}

impl PagedVectorObserver for Counts {
    /// One stored vector read out of the saved graph's pages, whether for a
    /// walk distance or for a returned candidate's published score. Counted
    /// apart from the two scoring observers, as the independent tally of what
    /// the file route really scored.
    fn vector_decoded(&self) {
        self.decoded.fetch_add(1, Ordering::SeqCst);
    }
}

fn context_id() -> Uuid {
    Uuid::from_u128(0xC05D_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC05D_1000_0000_0000_0000_0000_0000_0000)
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

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

/// Seed the store, and measure the same INDEXED find on the writer's own
/// in-memory route before sealing it, so the proof can compare the two routes.
fn seed(path: &std::path::Path, runtime: &std::path::Path) -> Observed {
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
            "CREATE TABLE saved_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1)",
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX saved_rows_active_idx ON saved_rows \
             (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for ordinal in 0..ROWS {
        admin
            .execute(
                "INSERT INTO saved_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id())),
                    ("generation".into(), Value::Uuid(generation_id())),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one partitioned row");
    }
    let index = VectorIndexRef::new("saved_rows", "embedding");
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
        if ready.len() == 1 && ready.iter().all(|partition| partition.graph_available) {
            let (sql, params) = statement("INDEXED");
            // Warm the resident graph first, as the file route's measured find
            // is measured after nothing but opening.
            admin
                .execute(&sql, &params)
                .expect("the in-memory find answers");
            let in_memory = observe_with("in-memory INDEXED", || {
                admin
                    .execute(&sql, &params)
                    .map_err(|error| format!("{error:?}"))
            });
            admin.close().expect("seal the fixture store");
            return in_memory;
        }
    }
    panic!("the partition must publish a saved graph before the read is measured");
}

fn statement(mode: &str) -> (String, HashMap<String, Value>) {
    (
        format!(
            "SELECT id FROM saved_rows WHERE context_id = $context AND \
             generation_id = $generation AND is_current = $is_current \
             ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {LIMIT}"
        ),
        HashMap::from([
            ("context".into(), Value::Uuid(context_id())),
            ("generation".into(), Value::Uuid(generation_id())),
            ("is_current".into(), Value::Bool(true)),
            ("query".into(), Value::Vector(query_vector())),
        ]),
    )
}

struct Observed {
    answer: QueryResult,
    graph: u64,
    native: u64,
    decoded: u64,
    receipts: Vec<UnionSearchReceipt>,
}

impl Observed {
    fn route(&self) -> Option<VectorSearchRoute> {
        self.answer
            .trace
            .vector_search
            .as_ref()
            .and_then(|disclosure| disclosure.route)
    }
}

fn observe_with(label: &str, execute: impl FnOnce() -> Result<QueryResult, String>) -> Observed {
    let counts = Arc::new(Counts::default());
    let (answer, receipts) = with_union_search_receipts(|| {
        with_paged_vector_observer(counts.clone(), || {
            with_hnsw_candidate_observer(counts.clone(), || {
                with_native_vector_score_observer_for_test(counts.clone(), execute)
            })
        })
    });
    Observed {
        answer: answer.unwrap_or_else(|error| panic!("the {label} find answers: {error}")),
        graph: counts.graph.load(Ordering::SeqCst),
        native: counts.native.load(Ordering::SeqCst),
        decoded: counts.decoded.load(Ordering::SeqCst),
        receipts,
    }
}

fn observe(reader: &ReadSession, mode: &str) -> Observed {
    let (sql, params) = statement(mode);
    observe_with(mode, || {
        reader
            .execute(&sql, &params)
            .map_err(|error| format!("{error:?}"))
    })
}

#[test]
fn a_saved_graph_search_reports_its_distance_work() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("saved-graph.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let in_memory = seed(&path, &runtime);

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                limits: limits(),
                memory_limit: Some(limits().memory as usize),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);

    // The fresh reader's first find: it loads the saved graph and walks it.
    let indexed = observe(&reader, "INDEXED");
    let exact = observe(&reader, "EXACT");
    let walk_evaluations = indexed
        .receipts
        .iter()
        .map(|receipt| receipt.admission_evaluations + receipt.expansion_evaluations)
        .sum::<usize>() as u64;
    // Native scoring reports returned candidates on every saved-page codec.
    // The old paged decoder observer does not cover native topology pages.
    let rescored = indexed.native;
    println!(
        "saved_graph_distance_work rows={ROWS} indexed_graph={} indexed_native={} \
         indexed_decoded={} walk_evaluations={walk_evaluations} rescored={rescored} \
         receipts={:?} exact_graph={} exact_native={} indexed_route={:?} exact_route={:?} \
         in_memory_graph={} in_memory_native={} in_memory_decoded={} in_memory_route={:?}",
        indexed.graph,
        indexed.native,
        indexed.decoded,
        indexed.receipts,
        exact.graph,
        exact.native,
        indexed.route(),
        exact.route(),
        in_memory.graph,
        in_memory.native,
        in_memory.decoded,
        in_memory.route(),
    );

    assert_eq!(
        indexed.answer.rows.len(),
        LIMIT,
        "INDEXED answers a full page"
    );
    assert_eq!(exact.answer.rows.len(), LIMIT, "EXACT answers a full page");
    assert!(
        matches!(
            indexed.route(),
            Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed)
        ),
        "the INDEXED find walks the saved graph"
    );
    assert_eq!(
        indexed.receipts.len(),
        1,
        "the INDEXED find runs exactly one walk over the saved graph: {:?}",
        indexed.receipts
    );
    assert!(
        walk_evaluations > 0,
        "the walk over the saved graph evaluates distances"
    );
    assert!(
        indexed.graph > 0,
        "a walk over a saved graph must report its distance evaluations: observed {} \
         against the walk's own {walk_evaluations}",
        indexed.graph
    );
    assert!(
        indexed.graph <= walk_evaluations,
        "each evaluation is observed once: observed {} against the walk's own {walk_evaluations}",
        indexed.graph
    );
    assert_eq!(exact.graph, 0, "EXACT walks no graph");
    assert_eq!(
        exact.native, ROWS as u64,
        "EXACT compares every vector in the partition once"
    );
    assert!(
        indexed.graph < exact.native,
        "the saved-graph walk evaluates strictly fewer vectors than EXACT compares: \
         {} against {}",
        indexed.graph,
        exact.native
    );

    // The returned candidates' published scores out of the saved graph's
    // pages are the same step the stored-body route reports as native scores,
    // and must be visible the same way: graph plus native is the walk plus
    // the re-scored candidates, each counted once.
    assert_eq!(
        rescored, LIMIT as u64,
        "the file route scores each returned candidate once out of the saved graph's pages"
    );
    assert_eq!(
        indexed.graph, walk_evaluations,
        "the graph observer sees exactly the walk's evaluations"
    );
    assert_eq!(
        indexed.native, in_memory.native,
        "both routes report the same returned-candidate scoring work"
    );
    assert_eq!(
        indexed.graph + indexed.native,
        walk_evaluations + rescored,
        "the file route's graph-plus-native total is the walk plus the re-scored candidates"
    );

    // The writer's in-memory route reads no saved pages. It answers through
    // the same one search as the file route: the same walk over the same
    // graph, observed as the same graph evaluations, and one published score
    // out of the stored bodies per returned candidate. It used to walk its
    // partition on its own and re-score every row that walk kept.
    assert_eq!(
        in_memory.route(),
        Some(VectorSearchRoute::FilteredIndexed),
        "the in-memory find walks its resident graph"
    );
    assert_eq!(
        in_memory.decoded, 0,
        "the in-memory route reads no saved pages"
    );
    assert!(
        in_memory.graph > 0,
        "the in-memory walk reports its distances"
    );
    assert_eq!(
        in_memory.graph, indexed.graph,
        "the in-memory route runs the same one search as the file route"
    );
    assert_eq!(
        in_memory.native, LIMIT as u64,
        "the in-memory route publishes one score out of stored bodies per returned candidate"
    );
}
