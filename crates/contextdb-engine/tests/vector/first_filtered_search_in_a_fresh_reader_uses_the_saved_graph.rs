use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{
    Database, MaintenancePolicy, QueryResult, ReadPhase, ReadProgress, ReadProgressObserver,
    ReadSession, ReadSessionOptions,
};
use contextdb_vector::hnsw::{
    HnswBuildObserver, HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_build_observer,
    with_hnsw_candidate_observer, with_union_search_receipts,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::{
    collections::{BTreeSet, HashMap},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use uuid::Uuid;

const CHILD_ROLE: &str = "CONTEXTDB_SAVED_GRAPH_FRESH_READER_CHILD";
const CHILD_PATH: &str = "CONTEXTDB_SAVED_GRAPH_FRESH_READER_PATH";
const CHILD_RECEIPT: &str = "saved-graph-fresh-reader-child-executed";
const PARTITIONS: usize = 8;

struct BuildCounter(AtomicU64);

impl HnswBuildObserver for BuildCounter {
    fn before_hnsw_build(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ScoreCounter(AtomicU64);

impl HnswCandidateObserver for ScoreCounter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeVectorScoreObserver for ScoreCounter {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct CompletedReads(Mutex<Vec<ReadProgress>>);

impl ReadProgressObserver for CompletedReads {
    fn progress(&self, _progress: ReadProgress) {}

    fn completed(&self, totals: ReadProgress) {
        if totals.phase == ReadPhase::Executing {
            self.0.lock().unwrap().push(totals);
        }
    }
}

struct ObservedRead {
    result: QueryResult,
    work: u64,
    graph_scores: u64,
    native_scores: u64,
}

fn observed_read(
    session: &ReadSession,
    progress: &CompletedReads,
    builds: &Arc<BuildCounter>,
    sql: &str,
    params: &HashMap<String, Value>,
) -> ObservedRead {
    let completed_before = progress.0.lock().unwrap().len();
    let graph = Arc::new(ScoreCounter(AtomicU64::new(0)));
    let native = Arc::new(ScoreCounter(AtomicU64::new(0)));
    let result = with_hnsw_build_observer(builds.clone(), || {
        with_hnsw_candidate_observer(graph.clone(), || {
            with_native_vector_score_observer_for_test(native.clone(), || {
                session.execute(sql, params)
            })
        })
    })
    .expect("the fresh file-route query succeeds");
    let completed = progress.0.lock().unwrap();
    assert_eq!(
        completed.len(),
        completed_before + 1,
        "the public progress observer reports one completed production read"
    );
    ObservedRead {
        result,
        work: completed.last().unwrap().work,
        graph_scores: graph.0.load(Ordering::SeqCst),
        native_scores: native.0.load(Ordering::SeqCst),
    }
}

fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_owned(),
    }
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC046_2000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC046_3000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn bind_uuid_values(
    params: &mut HashMap<String, Value>,
    column: &str,
    prefix: &str,
    values: impl IntoIterator<Item = Uuid>,
) -> String {
    let values = values.into_iter().collect::<BTreeSet<_>>();
    assert!(!values.is_empty());
    let mut placeholders = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let name = format!("{prefix}_{index}");
        params.insert(name.clone(), Value::Uuid(value));
        placeholders.push(format!("${name}"));
    }
    if placeholders.len() == 1 {
        format!("{column} = {}", placeholders[0])
    } else {
        format!("{column} IN ({})", placeholders.join(", "))
    }
}

fn statement(table: &str, partitions: usize, mode: &str) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        (
            "query".into(),
            Value::Vector(maintained_search::held_out_vector(0)),
        ),
        ("is_current".into(), Value::Bool(true)),
    ]);
    let contexts = bind_uuid_values(
        &mut params,
        "context_id",
        "search_context",
        (0..partitions).map(context_id),
    );
    let generations = bind_uuid_values(
        &mut params,
        "generation_id",
        "search_generation",
        (0..partitions).map(generation_id),
    );
    (
        format!(
            "SELECT id FROM {table} WHERE {contexts} AND {generations} AND \
             is_current = $is_current ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        params,
    )
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE saved_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
         embedding VECTOR(768) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX saved_vectors_active_idx ON saved_vectors \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .unwrap();
    for ordinal in 0..maintained_search::CORPUS_ROWS {
        let partition = maintained_search::corpus_partition(ordinal) as usize;
        let common = [
            (
                "id".into(),
                Value::Int64(maintained_search::corpus_id(ordinal)),
            ),
            (
                "embedding".into(),
                Value::Vector(maintained_search::corpus_vector(ordinal)),
            ),
        ];
        db.execute(
            "INSERT INTO saved_vectors VALUES ($id, $context, $generation, TRUE, $embedding)",
            &common
                .clone()
                .into_iter()
                .chain([
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                ])
                .collect(),
        )
        .unwrap();
    }
    let builds = Arc::new(BuildCounter(AtomicU64::new(0)));
    for _ in 0..128 {
        with_hnsw_build_observer(builds.clone(), || db.run_maintenance_cycle()).unwrap();
        let maintenance = db.vector_store_for_test().hnsw_maintenance_status();
        if maintenance.ready_indexes == 1
            && maintenance.ready_partitions == PARTITIONS
            && maintenance.remaining_partitions == 0
        {
            assert!(
                builds.0.load(Ordering::SeqCst) > 0,
                "the build observer must first prove it sees real maintenance construction"
            );
            db.close().unwrap();
            return;
        }
    }
    panic!("maintenance did not publish the eight-pair saved graphs");
}

fn fresh_reader_child(path: &std::path::Path) {
    let builds = Arc::new(BuildCounter(AtomicU64::new(0)));
    let progress = Arc::new(CompletedReads::default());
    let open_started = Instant::now();
    let fresh = with_hnsw_build_observer(builds.clone(), || {
        ReadSession::open_with_progress(
            path,
            ReadSessionOptions {
                limits: contextdb_core::read_contract::ReadLimits {
                    work: 50_000,
                    memory: 1024 * 1024 * 1024,
                    ..contextdb_core::read_contract::ReadLimits::default()
                },
                ..ReadSessionOptions::default()
            },
            progress.clone(),
        )
    })
    .expect("the fresh process opens the file route");
    let open_ms = open_started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(
        builds.0.load(Ordering::SeqCst),
        0,
        "opening may decode durable graphs but must not construct one"
    );

    // This proof is only the accepted fresh file-reader eight-pair scope.
    // The 10,000-vector single-context acceleration cell is independent.
    let (eight_sql, eight_params) = statement("saved_vectors", PARTITIONS, "AUTO");
    let first_started = Instant::now();
    let (eight_first, searches) = with_union_search_receipts(|| {
        observed_read(&fresh, &progress, &builds, &eight_sql, &eight_params)
    });
    let first_ms = first_started.elapsed().as_secs_f64() * 1_000.0;
    let warm_started = Instant::now();
    let eight_warm = observed_read(&fresh, &progress, &builds, &eight_sql, &eight_params);
    let warm_ms = warm_started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(
        searches.len(),
        1,
        "the first read searches the saved graph once"
    );
    let search = searches[0];
    assert_eq!(search.terminal, "answer_complete");
    assert_eq!(search.layers_admitted, PARTITIONS);
    assert_eq!(
        search.budget,
        search.resolved_ef_search.max(10) * search.layer_zero_degree
    );
    assert!(
        search.admission_evaluations + search.expansion_evaluations <= search.budget,
        "the first graph walk fits its existing one-search candidate allowance"
    );
    assert!(
        eight_first.work <= 50_000,
        "the first read fits its declared work budget"
    );
    let (eight_exact_sql, _) = statement("saved_vectors", PARTITIONS, "EXACT");
    // The exact oracle scores every stored vector. Its separate reader must
    // allow that work to finish without changing the bounded graph read.
    let oracle = with_hnsw_build_observer(builds.clone(), || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                limits: contextdb_core::read_contract::ReadLimits {
                    work: 50_000,
                    active_ms: 30_000,
                    memory: 1024 * 1024 * 1024,
                    ..contextdb_core::read_contract::ReadLimits::default()
                },
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open a separate exact oracle reader");
    let eight_exact = with_hnsw_build_observer(builds.clone(), || {
        oracle.execute(&eight_exact_sql, &eight_params)
    })
    .expect("the full exact oracle completes");
    assert_eq!(eight_first.result.rows(), eight_warm.result.rows());
    assert_eq!(
        eight_first.result.rows(),
        eight_exact.rows(),
        "the fresh eight-Context answer preserves exact result identities"
    );
    assert!(eight_first.work <= eight_warm.work);
    assert!(
        eight_first.graph_scores > 0 || eight_first.native_scores > 0,
        "the first eight-pair read must reach a real scoring path"
    );
    assert_eq!(
        builds.0.load(Ordering::SeqCst),
        0,
        "open, first reads, warm reads, and exact comparison must build no replacement graph"
    );
    println!(
        "wall_search_fresh open_ms={open_ms:.3} first_ms={first_ms:.3} warm_ms={warm_ms:.3} first={{work:{},graph:{},native:{},trace:{:?},admission:{},expansion:{},budget:{},layer_zero_degree:{}}} warm={{work:{},graph:{},native:{},trace:{:?}}} builds={}",
        eight_first.work,
        eight_first.graph_scores,
        eight_first.native_scores,
        eight_first.result.trace(),
        search.admission_evaluations,
        search.expansion_evaluations,
        search.budget,
        search.layer_zero_degree,
        eight_warm.work,
        eight_warm.graph_scores,
        eight_warm.native_scores,
        eight_warm.result.trace(),
        builds.0.load(Ordering::SeqCst),
    );
    println!("{CHILD_RECEIPT}");
}

#[test]
fn first_filtered_search_in_a_fresh_reader_uses_the_saved_graph_child() {
    let Ok(path) = std::env::var(CHILD_PATH) else {
        return;
    };
    fresh_reader_child(std::path::Path::new(&path));
}

#[test]
fn first_filtered_search_in_a_fresh_reader_uses_the_saved_graph() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("saved-graph.redb");
    seed(&path);
    let output = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg(child_test_filter(
            module_path!(),
            "first_filtered_search_in_a_fresh_reader_uses_the_saved_graph_child",
        ))
        .arg("--nocapture")
        .env(CHILD_ROLE, "1")
        .env(CHILD_PATH, &path)
        .output()
        .expect("spawn a distinct fresh-reader process");
    assert!(
        output.status.success(),
        "fresh reader child failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(CHILD_RECEIPT),
        "fresh reader child ran no assertion receipt; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
}
