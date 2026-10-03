use super::maintained_search;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey, VectorQuantization};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, HnswQuerySelfScoreEvent,
    HnswQuerySelfScoreObserver, with_hnsw_candidate_observer, with_hnsw_query_self_score_observer,
};
use contextdb_vector::mem::{
    BoundedCandidateGroupingEventForTest, BoundedCandidateGroupingFaultForTest,
    BoundedCandidateGroupingObserverForTest, NativeVectorScoreObserver,
    with_bounded_candidate_grouping_fault_for_test,
    with_bounded_candidate_grouping_observer_for_test, with_native_vector_score_observer_for_test,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use uuid::Uuid;

const SELECTED_ROWS: usize = 1_000;
const CURRENT_ROWS_PER_CONTEXT: usize = 125;

struct DistanceCounter(AtomicU64);

impl HnswCandidateObserver for DistanceCounter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeVectorScoreObserver for DistanceCounter {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct GroupingCounter {
    row_directory_lookups: AtomicU64,
    snapshot_visibility_probes: AtomicU64,
    placements: AtomicU64,
}

impl BoundedCandidateGroupingObserverForTest for GroupingCounter {
    fn observe(&self, event: BoundedCandidateGroupingEventForTest) {
        match event {
            BoundedCandidateGroupingEventForTest::RowDirectoryLookup => {
                self.row_directory_lookups.fetch_add(1, Ordering::SeqCst);
            }
            BoundedCandidateGroupingEventForTest::SnapshotVisibilityProbe => {
                self.snapshot_visibility_probes
                    .fetch_add(1, Ordering::SeqCst);
            }
            BoundedCandidateGroupingEventForTest::Placement => {
                self.placements.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Default)]
struct QuerySelfScoreCounter {
    f32: AtomicU64,
    sq8: AtomicU64,
    sq4: AtomicU64,
}

impl QuerySelfScoreCounter {
    fn count(&self, quantization: VectorQuantization) -> u64 {
        match quantization {
            VectorQuantization::F32 => self.f32.load(Ordering::SeqCst),
            VectorQuantization::SQ8 => self.sq8.load(Ordering::SeqCst),
            VectorQuantization::SQ4 => self.sq4.load(Ordering::SeqCst),
        }
    }

    fn total(&self) -> u64 {
        self.f32.load(Ordering::SeqCst)
            + self.sq8.load(Ordering::SeqCst)
            + self.sq4.load(Ordering::SeqCst)
    }

    fn by_representation(&self) -> [u64; 3] {
        [
            self.count(VectorQuantization::F32),
            self.count(VectorQuantization::SQ8),
            self.count(VectorQuantization::SQ4),
        ]
    }
}

impl HnswQuerySelfScoreObserver for QuerySelfScoreCounter {
    fn after_query_self_score_charge(&self, event: HnswQuerySelfScoreEvent) {
        assert!(
            event.dimension() > 0,
            "a graph query representation has dimensions"
        );
        match event.quantization() {
            VectorQuantization::F32 => &self.f32,
            VectorQuantization::SQ8 => &self.sq8,
            VectorQuantization::SQ4 => &self.sq4,
        }
        .fetch_add(1, Ordering::SeqCst);
    }
}

struct ChargedQuery {
    result: bounded::TestResult,
    distance_evaluations: u64,
    query_self_score_computations: u64,
    query_self_scores_by_representation: [u64; 3],
    row_directory_lookups: u64,
    snapshot_visibility_probes: u64,
    placements: u64,
    used_hnsw: bool,
}

struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 50_000,
        memory: 256 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC045_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC045_2000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn insert(db: &Database, ordinal: usize, partition: usize, current: bool) {
    db.execute(
        "INSERT INTO scoped_vectors VALUES ($id, $context, $generation, $current, $embedding)",
        &HashMap::from([
            (
                "id".into(),
                Value::Int64(maintained_search::corpus_id(ordinal)),
            ),
            ("context".into(), Value::Uuid(context_id(partition))),
            ("generation".into(), Value::Uuid(generation_id(partition))),
            ("current".into(), Value::Bool(current)),
            (
                "embedding".into(),
                Value::Vector(maintained_search::corpus_vector(ordinal)),
            ),
        ]),
    )
    .unwrap();
}

fn seed(include_unrelated_contexts: bool) -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE scoped_vectors (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
         embedding VECTOR(768) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX scoped_vectors_current_idx ON scoped_vectors \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .unwrap();

    // Both fixtures contain byte-for-byte the same selected Context: the
    // accepted 1,000 frozen vectors with exactly 125 current rows.
    for ordinal in 0..SELECTED_ROWS {
        insert(&db, ordinal, 0, ordinal < CURRENT_ROWS_PER_CONTEXT);
    }
    if include_unrelated_contexts {
        // Grow only the other seven Contexts to the frozen 10,000-row total.
        // Each has exactly 125 current rows; corpus ids and vector bytes stay
        // in their frozen order.
        for ordinal in SELECTED_ROWS..maintained_search::CORPUS_ROWS {
            let offset = ordinal - SELECTED_ROWS;
            let context = 1 + offset % 7;
            let local_ordinal = offset / 7;
            insert(
                &db,
                ordinal,
                context,
                local_ordinal < CURRENT_ROWS_PER_CONTEXT,
            );
        }
    }
    db
}

fn selected_context_snapshot(db: &Database) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id, context_id, generation_id, is_current, embedding FROM scoped_vectors \
         WHERE context_id = $context AND generation_id = $generation ORDER BY id ASC",
        &HashMap::from([
            ("context".into(), Value::Uuid(context_id(0))),
            ("generation".into(), Value::Uuid(generation_id(0))),
        ]),
    )
    .expect("read every selected-Context field that can affect search")
    .rows
}

fn query(db: &Database, partitions: usize) -> ChargedQuery {
    let graph = Arc::new(DistanceCounter(AtomicU64::new(0)));
    let native = Arc::new(DistanceCounter(AtomicU64::new(0)));
    let grouping = Arc::new(GroupingCounter::default());
    let query_self_scores = Arc::new(QuerySelfScoreCounter::default());
    let mut params = HashMap::from([
        (
            "query".into(),
            Value::Vector(maintained_search::held_out_vector(0)),
        ),
        ("is_current".into(), Value::Bool(true)),
    ]);
    let mut contexts = Vec::with_capacity(partitions);
    let mut generations = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    let result = with_bounded_candidate_grouping_observer_for_test(grouping.clone(), || {
        with_hnsw_query_self_score_observer(query_self_scores.clone(), || {
            with_hnsw_candidate_observer(graph.clone(), || {
                with_native_vector_score_observer_for_test(native.clone(), || {
                    bounded::execute(
                        db,
                        &bounded::BoundedReadRequest::new(
                            format!(
                                "SELECT id FROM scoped_vectors WHERE context_id IN ({}) AND \
                         generation_id IN ({}) AND is_current = $is_current \
                         ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10",
                                contexts.join(", "),
                                generations.join(", "),
                            ),
                            params,
                            limits(),
                            Arc::new(FrozenClock),
                        ),
                    )
                })
            })
        })
    })
    .expect("the bounded filtered query succeeds");
    ChargedQuery {
        used_hnsw: db.__debug_last_query_vector_used_hnsw_for_test(),
        result,
        distance_evaluations: graph.0.load(Ordering::SeqCst) + native.0.load(Ordering::SeqCst),
        query_self_score_computations: query_self_scores.total(),
        query_self_scores_by_representation: query_self_scores.by_representation(),
        row_directory_lookups: grouping.row_directory_lookups.load(Ordering::SeqCst),
        snapshot_visibility_probes: grouping.snapshot_visibility_probes.load(Ordering::SeqCst),
        placements: grouping.placements.load(Ordering::SeqCst),
    }
}

fn honest_charge_range(query: &ChargedQuery, selected_candidates: usize) -> (u64, u64) {
    let lower = query
        .result
        .result
        .trace
        .rows_examined
        .saturating_add(selected_candidates as u64)
        .saturating_add(query.distance_evaluations)
        .saturating_add(query.query_self_score_computations);
    // Publishing each returned row performs one separately charged point read.
    let upper = lower.saturating_add(query.result.result.rows.len() as u64);
    (lower, upper)
}

fn assert_independent_charge_bound(label: &str, query: &ChargedQuery, selected_candidates: usize) {
    assert!(
        query.distance_evaluations > 0,
        "{label} must reach a native or HNSW distance observation before its charge is bounded"
    );
    assert_eq!(
        query.row_directory_lookups, selected_candidates as u64,
        "{label} must use the physical row-to-partition directory once per candidate"
    );
    assert_eq!(
        query.snapshot_visibility_probes, selected_candidates as u64,
        "{label} must probe one physical partition directory for snapshot visibility per current candidate"
    );
    assert_eq!(
        query.placements, selected_candidates as u64,
        "{label} must place and charge each selected candidate exactly once"
    );
    let (lower, upper) = honest_charge_range(query, selected_candidates);
    assert!(
        (lower..=upper).contains(&query.result.telemetry.work_units),
        "{label} charged work must include examined rows + candidate placements + independently observed distances + actual query self scores, and may add only one point read per returned row: work={}, range={lower}..={upper}, telemetry={:?}",
        query.result.telemetry.work_units,
        query.result.telemetry,
    );
}

fn grouping_fixture(partitions: usize) -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE grouping_vectors (id INTEGER PRIMARY KEY, partition_id INTEGER NOT NULL, \
         kind TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (partition_id) \
         MAX_PARTITIONS 64 SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .expect("declare the candidate-grouping fixture");
    db.execute(
        "CREATE INDEX grouping_vectors_kind_idx ON grouping_vectors(kind)",
        &HashMap::new(),
    )
    .expect("give candidate grouping an indexed relational source");
    for ordinal in 0..SELECTED_ROWS {
        db.execute(
            "INSERT INTO grouping_vectors VALUES ($id, $partition, $kind, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(ordinal as i64)),
                (
                    "partition".into(),
                    Value::Int64((ordinal % partitions) as i64),
                ),
                ("kind".into(), Value::Text("eligible".to_owned())),
                (
                    "embedding".into(),
                    Value::Vector(vec![1.0, ordinal as f32 * 0.0001, 0.0]),
                ),
            ]),
        )
        .expect("seed one grouping candidate");
    }
    for _ in 0..256 {
        db.run_maintenance_cycle()
            .expect("advance deterministic grouping-fixture maintenance");
        let ready = db
            .vector_store_for_test()
            .partition_infos(&contextdb_core::VectorIndexRef::new(
                "grouping_vectors",
                "embedding",
            ))
            .expect("inspect grouping partition readiness");
        if ready.len() == partitions && ready.iter().all(|partition| partition.graph_available) {
            return db;
        }
    }
    panic!("all {partitions} grouping partitions must publish maintained routes");
}

fn grouped_query(db: &Database, partitions: usize, mode: &str) -> ChargedQuery {
    grouped_query_with_fault(db, partitions, mode, None)
}

fn grouped_query_with_fault(
    db: &Database,
    partitions: usize,
    mode: &str,
    fault: Option<BoundedCandidateGroupingFaultForTest>,
) -> ChargedQuery {
    let graph = Arc::new(DistanceCounter(AtomicU64::new(0)));
    let native = Arc::new(DistanceCounter(AtomicU64::new(0)));
    let grouping = Arc::new(GroupingCounter::default());
    let query_self_scores = Arc::new(QuerySelfScoreCounter::default());
    let mut params = HashMap::from([
        ("query".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
        ("kind".into(), Value::Text("eligible".to_owned())),
    ]);
    let mut selected = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let name = format!("partition_{partition}");
        params.insert(name.clone(), Value::Int64(partition as i64));
        selected.push(format!("${name}"));
    }
    let sql = format!(
        "SELECT id FROM grouping_vectors WHERE kind = $kind AND partition_id IN ({}) \
         ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10",
        selected.join(", ")
    );
    let work_limit = if fault
        == Some(BoundedCandidateGroupingFaultForTest::RepeatGlobalCandidatePassPerPartition)
    {
        ReadLimits {
            work: 250_000,
            ..limits()
        }
    } else {
        limits()
    };
    let execute = || {
        with_bounded_candidate_grouping_observer_for_test(grouping.clone(), || {
            with_hnsw_query_self_score_observer(query_self_scores.clone(), || {
                with_hnsw_candidate_observer(graph.clone(), || {
                    with_native_vector_score_observer_for_test(native.clone(), || {
                        bounded::execute(
                            db,
                            &bounded::BoundedReadRequest::new(
                                sql,
                                params,
                                work_limit,
                                Arc::new(FrozenClock),
                            ),
                        )
                    })
                })
            })
        })
    };
    let result = match fault {
        Some(fault) => with_bounded_candidate_grouping_fault_for_test(fault, execute),
        None => execute(),
    }
    .expect("the fixed candidate scope remains affordable");
    ChargedQuery {
        used_hnsw: db.__debug_last_query_vector_used_hnsw_for_test(),
        result,
        distance_evaluations: graph.0.load(Ordering::SeqCst) + native.0.load(Ordering::SeqCst),
        query_self_score_computations: query_self_scores.total(),
        query_self_scores_by_representation: query_self_scores.by_representation(),
        row_directory_lookups: grouping.row_directory_lookups.load(Ordering::SeqCst),
        snapshot_visibility_probes: grouping.snapshot_visibility_probes.load(Ordering::SeqCst),
        placements: grouping.placements.load(Ordering::SeqCst),
    }
}

const SCORE_PARTITIONS: usize = 8;
const SCORE_BASE_ROWS: usize = 128;
const SCORE_DUPLICATE_ROWS: usize = 24;

fn score_fixture() -> (tempfile::TempDir, Database) {
    let directory = tempfile::tempdir().expect("temporary score directory");
    let db = Database::open(directory.path().join("query-score.redb"))
        .expect("open the durable score fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE score_vectors (\
         id INTEGER PRIMARY KEY, partition_id INTEGER NOT NULL, kind TEXT NOT NULL, \
         f32_embedding VECTOR(3) WITH (quantization = 'F32') \
             PARTITION_KEY (partition_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1, \
         sq8_embedding VECTOR(3) WITH (quantization = 'SQ8') \
             PARTITION_KEY (partition_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1, \
         sq4_embedding VECTOR(3) WITH (quantization = 'SQ4') \
             PARTITION_KEY (partition_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1\
         )",
        &HashMap::new(),
    )
    .expect("declare all compatible graph query representations");
    db.execute(
        "CREATE INDEX score_vectors_kind_idx ON score_vectors(kind)",
        &HashMap::new(),
    )
    .expect("give the score fixture an indexed candidate source");

    for ordinal in 0..SCORE_BASE_ROWS {
        let embedding = if ordinal < SCORE_DUPLICATE_ROWS - SCORE_PARTITIONS {
            vec![1.0, 0.0, 0.0]
        } else {
            vec![0.0, 1.0, 0.0]
        };
        db.execute(
            "INSERT INTO score_vectors VALUES \
             ($id, $partition, 'eligible', $f32, $sq8, $sq4)",
            &HashMap::from([
                ("id".into(), Value::Int64(ordinal as i64)),
                (
                    "partition".into(),
                    Value::Int64((ordinal % SCORE_PARTITIONS) as i64),
                ),
                ("f32".into(), Value::Vector(embedding.clone())),
                ("sq8".into(), Value::Vector(embedding.clone())),
                ("sq4".into(), Value::Vector(embedding)),
            ]),
        )
        .expect("seed one base-graph score row");
    }

    let columns = ["f32_embedding", "sq8_embedding", "sq4_embedding"];
    for _ in 0..256 {
        db.run_maintenance_cycle()
            .expect("advance score-fixture graph maintenance");
        if columns.iter().all(|column| {
            let index = VectorIndexRef::new("score_vectors", *column);
            db.vector_store_for_test()
                .partition_infos(&index)
                .is_ok_and(|partitions| {
                    partitions.len() == SCORE_PARTITIONS
                        && partitions.iter().all(|partition| partition.graph_available)
                })
        }) {
            break;
        }
    }

    for column in columns {
        let index = VectorIndexRef::new("score_vectors", column);
        let partitions = db
            .vector_store_for_test()
            .partition_infos(&index)
            .expect("inspect score graph partitions");
        assert_eq!(partitions.len(), SCORE_PARTITIONS);
        assert!(
            partitions.iter().all(|partition| partition.graph_available),
            "every {column} base partition must be graph-ready before adding a fresh tail"
        );
    }

    for partition in 0..SCORE_PARTITIONS {
        let id = SCORE_BASE_ROWS + partition;
        db.execute(
            "INSERT INTO score_vectors VALUES \
             ($id, $partition, 'eligible', [1.0, 0.0, 0.0], \
              [1.0, 0.0, 0.0], [1.0, 0.0, 0.0])",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("partition".into(), Value::Int64(partition as i64)),
            ]),
        )
        .expect("append one duplicate vector to every fresh graph tail");
    }

    for column in columns {
        let index = VectorIndexRef::new("score_vectors", column);
        for partition in 0..SCORE_PARTITIONS {
            let key = VectorPartitionKey::from_values(&[Value::Int64(partition as i64)])
                .expect("integer partition key");
            let status = db
                .vector_store_for_test()
                .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(
                    index.clone(),
                    key,
                ))
                .expect("the score route remains present");
            assert!(status.base.is_some(), "the score query spans a sealed base");
            assert_eq!(
                status.fresh_tail_entries, 1,
                "the score query also spans one fresh layer in each partition"
            );
        }
    }
    (directory, db)
}

fn representation_query(db: &Database, column: &str, mode: &str) -> ChargedQuery {
    let graph = Arc::new(DistanceCounter(AtomicU64::new(0)));
    let native = Arc::new(DistanceCounter(AtomicU64::new(0)));
    let grouping = Arc::new(GroupingCounter::default());
    let query_self_scores = Arc::new(QuerySelfScoreCounter::default());
    let result = with_bounded_candidate_grouping_observer_for_test(grouping.clone(), || {
        with_hnsw_query_self_score_observer(query_self_scores.clone(), || {
            with_hnsw_candidate_observer(graph.clone(), || {
                with_native_vector_score_observer_for_test(native.clone(), || {
                    bounded::execute(
                        db,
                        &bounded::BoundedReadRequest::new(
                            format!(
                                "SELECT id FROM score_vectors WHERE kind = 'eligible' \
                                 ORDER BY {column} <=> $query USE VECTOR {mode} LIMIT {SCORE_DUPLICATE_ROWS}"
                            ),
                            HashMap::from([(
                                "query".into(),
                                Value::Vector(vec![1.0, 0.0, 0.0]),
                            )]),
                            limits(),
                            Arc::new(FrozenClock),
                        ),
                    )
                })
            })
        })
    })
    .unwrap_or_else(|error| panic!("bounded {column} {mode} score query: {error:?}"));
    ChargedQuery {
        used_hnsw: db.__debug_last_query_vector_used_hnsw_for_test(),
        result,
        distance_evaluations: graph.0.load(Ordering::SeqCst) + native.0.load(Ordering::SeqCst),
        query_self_score_computations: query_self_scores.total(),
        query_self_scores_by_representation: query_self_scores.by_representation(),
        row_directory_lookups: grouping.row_directory_lookups.load(Ordering::SeqCst),
        snapshot_visibility_probes: grouping.snapshot_visibility_probes.load(Ordering::SeqCst),
        placements: grouping.placements.load(Ordering::SeqCst),
    }
}

#[test]
fn a_filtered_scope_is_charged_for_its_candidates() {
    let small_db = seed(false);
    let grown_db = seed(true);
    let small_selected = selected_context_snapshot(&small_db);
    let grown_selected = selected_context_snapshot(&grown_db);
    assert_eq!(small_selected.len(), SELECTED_ROWS);
    assert_eq!(
        small_selected
            .iter()
            .filter(|row| row.get(3) == Some(&Value::Bool(true)))
            .count(),
        CURRENT_ROWS_PER_CONTEXT
    );
    assert_eq!(
        small_selected, grown_selected,
        "all selected rows, vectors, and current-state values stay byte-identical while only unrelated Contexts grow"
    );
    assert_eq!(
        grown_db
            .execute("SELECT id FROM scoped_vectors", &HashMap::new())
            .unwrap()
            .rows
            .len(),
        maintained_search::CORPUS_ROWS,
        "the grown arm retains the frozen 10,000-vector corpus"
    );

    let small = query(&small_db, 1);
    let grown = query(&grown_db, 1);
    assert_eq!(
        small.result.result.rows, grown.result.result.rows,
        "unrelated partitions cannot alter the selected answer"
    );
    assert_eq!(
        small.result.telemetry.work_units, grown.result.telemetry.work_units,
        "the same 125-current-row scope keeps the same charged work as only unrelated partitions grow"
    );
    assert_independent_charge_bound("small selected Context", &small, CURRENT_ROWS_PER_CONTEXT);
    assert_independent_charge_bound("grown selected Context", &grown, CURRENT_ROWS_PER_CONTEXT);

    let all_contexts = query(&grown_db, 8);
    assert_eq!(all_contexts.result.result.rows.len(), 10);
    assert_eq!(
        all_contexts
            .result
            .result
            .trace
            .vector_search
            .as_ref()
            .and_then(|detail| detail.aggregate_allowed_vectors),
        Some(8 * CURRENT_ROWS_PER_CONTEXT),
        "the eight-Context arm admits exactly 125 current vectors per Context"
    );
    assert!(
        all_contexts.result.telemetry.work_units <= limits().work,
        "the eight-Context current scope retains cg's unchanged 50,000-work budget: {:?}",
        all_contexts.result.telemetry
    );
    assert_independent_charge_bound(
        "eight-Context scope",
        &all_contexts,
        8 * CURRENT_ROWS_PER_CONTEXT,
    );
    println!(
        "wall_search_charge selected_rows={} current_per_context={} small={{work:{},examined:{},trace:{:?}}} grown={{work:{},examined:{},trace:{:?},source_work:{:?}}} eight={{work:{},examined:{},trace:{:?}}}",
        small_selected.len(),
        CURRENT_ROWS_PER_CONTEXT,
        small.result.telemetry.work_units,
        small.result.result.trace.rows_examined,
        small.result.result.trace,
        grown.result.telemetry.work_units,
        grown.result.result.trace.rows_examined,
        grown.result.result.trace,
        grown.distance_evaluations,
        all_contexts.result.telemetry.work_units,
        all_contexts.result.result.trace.rows_examined,
        all_contexts.result.result.trace,
    );
}

#[test]
fn one_thousand_candidates_are_grouped_once_across_eight_or_sixty_four_partitions() {
    let eight = grouping_fixture(8);
    let sixty_four = grouping_fixture(64);
    for mode in ["EXACT", "INDEXED"] {
        let eight_query = grouped_query(&eight, 8, mode);
        let sixty_four_query = grouped_query(&sixty_four, 64, mode);
        assert_eq!(
            eight_query.result.result.rows, sixty_four_query.result.result.rows,
            "partition fanout cannot alter the fixed 1,000-candidate answer in {mode} mode"
        );
        assert_eq!(
            eight_query.used_hnsw,
            mode == "INDEXED",
            "the eight-partition {mode} arm must exercise its declared route"
        );
        assert_eq!(
            sixty_four_query.used_hnsw,
            mode == "INDEXED",
            "the sixty-four-partition {mode} arm must exercise its declared route"
        );
        assert_independent_charge_bound(
            &format!("eight-partition {mode} grouping"),
            &eight_query,
            SELECTED_ROWS,
        );
        assert_independent_charge_bound(
            &format!("sixty-four-partition {mode} grouping"),
            &sixty_four_query,
            SELECTED_ROWS,
        );
        assert_eq!(
            eight_query.row_directory_lookups, sixty_four_query.row_directory_lookups,
            "8 -> 64 selected partitions must not multiply membership work in {mode} mode"
        );
        assert_eq!(
            eight_query.placements, sixty_four_query.placements,
            "8 -> 64 selected partitions must retain one placement per candidate in {mode} mode"
        );
        println!(
            "candidate_grouping mode={mode} eight={{work:{},membership:{},placement:{},distance:{}}} sixty_four={{work:{},membership:{},placement:{},distance:{}}}",
            eight_query.result.telemetry.work_units,
            eight_query.row_directory_lookups,
            eight_query.placements,
            eight_query.distance_evaluations,
            sixty_four_query.result.telemetry.work_units,
            sixty_four_query.row_directory_lookups,
            sixty_four_query.placements,
            sixty_four_query.distance_evaluations,
        );
    }
}

#[test]
fn graph_query_self_score_is_charged_once_per_compatible_representation() {
    let (_directory, db) = score_fixture();
    let expected_ids = (0..SCORE_DUPLICATE_ROWS - SCORE_PARTITIONS)
        .chain(SCORE_BASE_ROWS..SCORE_BASE_ROWS + SCORE_PARTITIONS)
        .map(|id| vec![Value::Int64(id as i64)])
        .collect::<Vec<_>>();
    for (column, quantization, representation_slot) in [
        ("f32_embedding", VectorQuantization::F32, 0),
        ("sq8_embedding", VectorQuantization::SQ8, 1),
        ("sq4_embedding", VectorQuantization::SQ4, 2),
    ] {
        let exact = representation_query(&db, column, "EXACT");
        let indexed = representation_query(&db, column, "INDEXED");
        assert!(
            !exact.used_hnsw,
            "{column} EXACT must stay on the exact route"
        );
        assert!(
            indexed.used_hnsw,
            "{column} INDEXED must traverse the sealed and fresh graph layers"
        );
        assert_eq!(
            exact.result.result.rows, expected_ids,
            "{column} exact duplicates"
        );
        assert_eq!(
            indexed.result.result.rows, exact.result.result.rows,
            "{quantization:?} graph duplicate correction must preserve complete recall and canonical order"
        );
        assert_eq!(
            exact.query_self_score_computations, 0,
            "the exact route does not run HNSW duplicate correction"
        );
        let mut expected_counts = [0_u64; 3];
        expected_counts[representation_slot] = 1;
        assert_eq!(
            indexed.query_self_scores_by_representation, expected_counts,
            "{quantization:?} computes and charges one correctly tagged query self score across all partitions and layers"
        );
        assert_independent_charge_bound(
            &format!("{quantization:?} exact score route"),
            &exact,
            SCORE_BASE_ROWS + SCORE_PARTITIONS,
        );
        assert_independent_charge_bound(
            &format!("{quantization:?} indexed score route"),
            &indexed,
            SCORE_BASE_ROWS + SCORE_PARTITIONS,
        );
    }
}

#[test]
fn grouping_charge_bounds_reject_uncharged_and_repeated_candidate_work() {
    for partitions in [8, 64] {
        let db = grouping_fixture(partitions);
        let control = grouped_query(&db, partitions, "INDEXED");
        for fault in [
            BoundedCandidateGroupingFaultForTest::RepeatGlobalCandidatePassPerPartition,
            BoundedCandidateGroupingFaultForTest::SuppressPlacementCharge,
        ] {
            let faulty = grouped_query_with_fault(&db, partitions, "INDEXED", Some(fault));
            assert_eq!(
                faulty.result.result.rows, control.result.result.rows,
                "the {fault:?} control changes work observation, not the answer"
            );
            let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert_independent_charge_bound(
                    &format!("{partitions}-partition {fault:?} control"),
                    &faulty,
                    SELECTED_ROWS,
                );
            }));
            assert!(
                rejected.is_err(),
                "the fixed 1,000-candidate assertion must reject {fault:?} across {partitions} partitions"
            );
        }
    }
}

fn historical_partition_rows(
    db: &Database,
    snapshot: Option<contextdb_core::SnapshotId>,
    partition: i64,
) -> Vec<Vec<Value>> {
    let sql = "SELECT id FROM historical_grouping \
               WHERE kind = 'eligible' AND partition_id IN ($partition) \
               ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10";
    let params = HashMap::from([
        ("partition".into(), Value::Int64(partition)),
        ("query".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    snapshot
        .map_or_else(
            || db.execute(sql, &params),
            |snapshot| db.execute_at_snapshot(sql, &params, snapshot),
        )
        .expect("historical partition query")
        .rows
}

fn historical_deleted_rows(
    db: &Database,
    snapshot: Option<contextdb_core::SnapshotId>,
) -> Vec<Vec<Value>> {
    let sql = "SELECT id FROM historical_deleted WHERE kind = 'eligible' \
               ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10";
    let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    snapshot
        .map_or_else(
            || db.execute(sql, &params),
            |snapshot| db.execute_at_snapshot(sql, &params, snapshot),
        )
        .expect("historical deleted-row query")
        .rows
}

fn bounded_historical_grouping(
    db: &Database,
    snapshot: contextdb_core::SnapshotId,
) -> (bounded::TestResult, Arc<GroupingCounter>) {
    let grouping = Arc::new(GroupingCounter::default());
    let result = db
        .__with_snapshot_override_for_test(snapshot, || {
            with_bounded_candidate_grouping_observer_for_test(grouping.clone(), || {
                bounded::execute(
                    db,
                    &bounded::BoundedReadRequest::new(
                        "SELECT id FROM historical_grouping \
                         WHERE kind = 'eligible' AND partition_id IN (1) \
                         ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                        HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
                        limits(),
                        Arc::new(FrozenClock),
                    ),
                )
            })
        })
        .expect("the old snapshot groups its visible physical vector");
    (result, grouping)
}

fn historical_vector_directory_count(db: &Database) -> usize {
    db.vector_store_for_test()
        .raw_directory_entries()
        .into_iter()
        .filter(|(index, _)| {
            index.table == "historical_grouping" || index.table == "historical_deleted"
        })
        .count()
}

#[test]
fn historical_vector_placement_survives_pruning_and_reconstruction_until_versions_reclaim() {
    let directory = tempfile::tempdir().expect("temporary history directory");
    let path = directory.path().join("vector-history.redb");
    let accountant = Arc::new(contextdb_engine::memory_accounting::MemoryAccountant::no_limit());
    let baseline = accountant.usage().used;
    let db = Database::open_with_config(
        &path,
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the history store with the caller accountant");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE historical_grouping (id INT PRIMARY KEY, kind TEXT NOT NULL, \
         partition_id INT NOT NULL, embedding VECTOR(3) \
         PARTITION_KEY (partition_id) MAX_PARTITIONS 4 SEARCH_MODE EXACT) \
         HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("declare partitioned current-only history");
    db.execute(
        "CREATE INDEX historical_grouping_kind_idx ON historical_grouping(kind)",
        &HashMap::new(),
    )
    .expect("index the partitioned history filter");
    db.execute(
        "CREATE TABLE historical_deleted (id INT PRIMARY KEY, kind TEXT NOT NULL, \
         embedding VECTOR(3) SEARCH_MODE EXACT) HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("declare unpartitioned current-only history");
    db.execute(
        "CREATE INDEX historical_deleted_kind_idx ON historical_deleted(kind)",
        &HashMap::new(),
    )
    .expect("index the deleted-row history filter");

    db.execute(
        "INSERT INTO historical_grouping VALUES \
         (1, 'eligible', 1, [1.0, 0.0, 0.0]), \
         (2, 'unrelated', 3, [0.0, 1.0, 0.0])",
        &HashMap::new(),
    )
    .expect("insert the moved row and unrelated cleanup row");
    db.execute(
        "UPDATE historical_grouping SET embedding = [0.0, 0.0, 1.0] WHERE id = 2",
        &HashMap::new(),
    )
    .expect("create history that predates the held snapshot");
    db.execute(
        "INSERT INTO historical_deleted VALUES (7, 'eligible', [1.0, 0.0, 0.0])",
        &HashMap::new(),
    )
    .expect("insert the unpartitioned row that will be deleted");

    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    db.execute(
        "UPDATE historical_grouping SET partition_id = 2, embedding = [0.0, 1.0, 0.0] \
         WHERE id = 1",
        &HashMap::new(),
    )
    .expect("move the selected vector in process");
    db.execute(
        "UPDATE historical_deleted SET embedding = [0.0, 1.0, 0.0] WHERE id = 7",
        &HashMap::new(),
    )
    .expect("create a post-snapshot unpartitioned version before deletion");
    db.execute(
        "DELETE FROM historical_deleted WHERE id = 7",
        &HashMap::new(),
    )
    .expect("delete the unpartitioned vector after the snapshot");

    assert_eq!(
        db.__physical_version_count_for_test("historical_grouping"),
        4
    );
    assert_eq!(
        db.__physical_version_count_for_test("historical_deleted"),
        2
    );
    assert_eq!(historical_vector_directory_count(&db), 6);
    assert_eq!(
        historical_partition_rows(&db, Some(old_snapshot), 1),
        vec![vec![Value::Int64(1)]],
        "the old-only scope sees the pre-move row"
    );
    assert!(
        historical_partition_rows(&db, Some(old_snapshot), 2).is_empty(),
        "the old snapshot excludes the new-only scope"
    );
    assert!(
        historical_partition_rows(&db, None, 1).is_empty(),
        "the current snapshot excludes the old-only scope"
    );
    assert_eq!(
        historical_partition_rows(&db, None, 2),
        vec![vec![Value::Int64(1)]],
        "the current snapshot sees only the moved row"
    );
    assert_eq!(
        historical_deleted_rows(&db, Some(old_snapshot)),
        vec![vec![Value::Int64(7)]],
        "the old snapshot keeps the unpartitioned deleted row"
    );
    assert!(historical_deleted_rows(&db, None).is_empty());

    let receipt_before_prune = db.vector_memory_ownership_receipt_for_test();
    let deferred = db
        .compact_currency_versions()
        .expect("prune only versions unrelated to the held view");
    assert_eq!(
        deferred.pruned_versions, 1,
        "the unrelated pre-snapshot version is reclaimed immediately"
    );
    assert_eq!(
        deferred.versions_deferred_for_readers, 2,
        "the moved row and deleted row versions visible to the snapshot are deferred"
    );
    assert_eq!(
        deferred.vector_keys_rewritten, 2,
        "cleanup removes the unrelated vector version and the post-snapshot deleted vector while retaining both snapshot-visible bodies"
    );
    assert_eq!(
        db.__physical_version_count_for_test("historical_grouping"),
        3
    );
    assert_eq!(
        db.__physical_version_count_for_test("historical_deleted"),
        2
    );
    assert_eq!(historical_vector_directory_count(&db), 4);
    let receipt_after_partial_prune = db.vector_memory_ownership_receipt_for_test();
    assert!(
        receipt_after_partial_prune.raw_vector_bodies < receipt_before_prune.raw_vector_bodies,
        "pruning the unrelated version returns its raw vector body"
    );
    assert_eq!(receipt_after_partial_prune.temporary_workspace, 0);

    let (in_process, grouping) = bounded_historical_grouping(&db, old_snapshot);
    assert_eq!(in_process.result.rows, vec![vec![Value::Int64(1)]]);
    assert_eq!(grouping.row_directory_lookups.load(Ordering::SeqCst), 1);
    assert_eq!(
        grouping.snapshot_visibility_probes.load(Ordering::SeqCst),
        1,
        "the durable membership route checks only the selected snapshot-visible placement"
    );
    assert_eq!(grouping.placements.load(Ordering::SeqCst), 1);
    assert_eq!(
        in_process
            .result
            .trace
            .vector_search
            .as_ref()
            .and_then(|detail| detail.aggregate_allowed_vectors),
        Some(1)
    );
    assert_eq!(
        historical_deleted_rows(&db, Some(old_snapshot)),
        vec![vec![Value::Int64(7)]]
    );

    drop(old_pin);
    db.close()
        .expect("close with the deferred historical versions still durable");
    drop(db);

    assert_eq!(
        accountant.usage().used,
        baseline,
        "dropping the first history image releases every body-cache owner"
    );
    let db = Database::open_with_config(
        &path,
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("reopen deferred vector history with the same caller accountant");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        db.__physical_version_count_for_test("historical_grouping"),
        3
    );
    assert_eq!(
        db.__physical_version_count_for_test("historical_deleted"),
        2
    );
    assert_eq!(
        historical_partition_rows(&db, Some(old_snapshot), 1),
        vec![vec![Value::Int64(1)]],
        "a later public historical read uses reconstructed partition history"
    );
    assert!(historical_partition_rows(&db, Some(old_snapshot), 2).is_empty());
    assert_eq!(
        historical_deleted_rows(&db, Some(old_snapshot)),
        vec![vec![Value::Int64(7)]],
        "the supported file reload retains the old unpartitioned body"
    );
    let (reconstructed, reconstructed_grouping) = bounded_historical_grouping(&db, old_snapshot);
    assert_eq!(reconstructed.result.rows, vec![vec![Value::Int64(1)]]);
    assert_eq!(
        reconstructed_grouping
            .row_directory_lookups
            .load(Ordering::SeqCst),
        1,
        "reopen rebuilds one row-to-partition history lookup"
    );
    assert_eq!(
        reconstructed_grouping
            .snapshot_visibility_probes
            .load(Ordering::SeqCst),
        1,
        "the reconstructed durable route selects the old snapshot-visible placement directly"
    );
    assert_eq!(reconstructed_grouping.placements.load(Ordering::SeqCst), 1);

    let index = VectorIndexRef::new("historical_grouping", "embedding");
    let old_partition = VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap();
    let new_partition = VectorPartitionKey::from_values(&[Value::Int64(2)]).unwrap();
    let retained_keys = db
        .vector_store_for_test()
        .partition_keys_including_historical(&index)
        .expect("inspect reconstructed typed partition history");
    assert!(retained_keys.contains(&old_partition));
    assert!(retained_keys.contains(&new_partition));

    assert_eq!(
        historical_vector_directory_count(&db),
        4,
        "the supported reload exposes durable identities for both current rows and both snapshot-visible historical bodies before reclaim"
    );
    let before_release_prune = db.vector_memory_ownership_receipt_for_test();
    let reclaimed = db
        .compact_currency_versions()
        .expect("reclaim versions after the reader protection is gone");
    assert_eq!(reclaimed.pruned_versions, 2);
    assert_eq!(reclaimed.versions_deferred_for_readers, 0);
    assert_eq!(
        reclaimed.vector_keys_rewritten, 2,
        "reclaim removes the moved history and the snapshot-visible vector copy of the deleted row"
    );
    assert_eq!(
        db.__physical_version_count_for_test("historical_grouping"),
        2
    );
    assert_eq!(
        db.__physical_version_count_for_test("historical_deleted"),
        1,
        "the relational delete anchor remains while its obsolete vector history is gone"
    );
    assert_eq!(
        historical_vector_directory_count(&db),
        2,
        "current-only cleanup leaves only the two current partitioned vector identities"
    );
    let after_release_prune = db.vector_memory_ownership_receipt_for_test();
    eprintln!(
        "HISTORICAL_CACHE_OWNERS before={before_release_prune:?} after={after_release_prune:?}"
    );
    assert_eq!(before_release_prune.raw_vector_bodies, 0);
    assert_eq!(after_release_prune.raw_vector_bodies, 0);
    assert_eq!(before_release_prune.raw_reverse_directory, 0);
    assert_eq!(after_release_prune.raw_reverse_directory, 0);
    assert_eq!(
        before_release_prune.partition_vectors - after_release_prune.partition_vectors,
        2560,
        "reclaim releases the two obsolete durable partition registry owners"
    );
    assert_eq!(
        before_release_prune.typed_partition_keys - after_release_prune.typed_partition_keys,
        48,
        "reclaim releases the two obsolete typed-key owners"
    );
    assert_eq!(
        before_release_prune.retained_total - after_release_prune.retained_total,
        2608,
        "the retained-owner release is exactly registry plus typed keys"
    );
    assert_eq!(
        before_release_prune.accountant_total - after_release_prune.accountant_total,
        2608,
        "the caller accountant releases the same owners exactly once"
    );
    assert!(
        after_release_prune.retained_total <= before_release_prune.retained_total,
        "reclaim cannot increase retained vector ownership"
    );
    assert_eq!(after_release_prune.temporary_workspace, 0);
    let final_keys = db
        .vector_store_for_test()
        .partition_keys_including_historical(&index)
        .expect("inspect final typed partitions");
    assert!(!final_keys.contains(&old_partition));
    assert!(final_keys.contains(&new_partition));
    assert!(
        historical_partition_rows(&db, Some(old_snapshot), 1).is_empty(),
        "a reclaimed current-only version cannot be reconstructed later"
    );
    assert!(historical_deleted_rows(&db, Some(old_snapshot)).is_empty());
    assert!(historical_partition_rows(&db, None, 1).is_empty());
    assert_eq!(
        historical_partition_rows(&db, None, 2),
        vec![vec![Value::Int64(1)]]
    );
    db.close().expect("close the reclaimed historical store");
    drop(db);
    assert_eq!(
        accountant.usage().used,
        baseline,
        "final history-image drop releases the retained body and header owners exactly once"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

#[test]
fn exhaustive_exact_scan_charges_retained_dead_and_future_versions_before_visibility() {
    const FUTURE_VERSIONS: u64 = 64;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE exhaustive_history (id INT PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE EXACT)",
        &HashMap::new(),
    )
    .expect("declare the exhaustive-history fixture");
    db.execute(
        "INSERT INTO exhaustive_history VALUES (1, [1.0, 0.0, 0.0])",
        &HashMap::new(),
    )
    .expect("insert the snapshot-visible vector");

    let snapshot = db.snapshot();
    let _pin = db.pin_snapshot(snapshot);
    let sql = "SELECT id FROM exhaustive_history \
               ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1";
    let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let run = |limits| {
        db.__with_snapshot_override_for_test(snapshot, || {
            bounded::execute(
                &db,
                &bounded::BoundedReadRequest::new(
                    sql,
                    params.clone(),
                    limits,
                    Arc::new(FrozenClock),
                ),
            )
        })
    };

    let baseline = run(limits()).expect("the one-entry exhaustive control succeeds");
    assert_eq!(baseline.result.rows, vec![vec![Value::Int64(1)]]);
    let baseline_vector_work = baseline
        .telemetry
        .source_work
        .get(&bounded::TestWorkSource::VectorCandidates)
        .copied()
        .unwrap_or_default();

    for ordinal in 0..FUTURE_VERSIONS {
        db.execute(
            "UPDATE exhaustive_history SET embedding = $embedding WHERE id = 1",
            &HashMap::from([(
                "embedding".into(),
                Value::Vector(vec![0.0, 1.0, ordinal as f32 / FUTURE_VERSIONS as f32]),
            )]),
        )
        .expect("append one future vector version");
    }
    assert_eq!(
        db.vector_store_for_test()
            .entries_for_index(&VectorIndexRef::new("exhaustive_history", "embedding"))
            .expect("inspect the physical exhaustive directory")
            .len(),
        FUTURE_VERSIONS as usize + 1,
        "the old snapshot is backed by one visible entry plus retained dead/future entries"
    );

    let grown = run(limits()).expect("roomy work still completes the historical scan");
    assert_eq!(grown.result.rows, baseline.result.rows);
    let grown_vector_work = grown
        .telemetry
        .source_work
        .get(&bounded::TestWorkSource::VectorCandidates)
        .copied()
        .unwrap_or_default();
    assert_eq!(
        grown_vector_work - baseline_vector_work,
        FUTURE_VERSIONS,
        "each additional invisible physical entry consumes exactly one exhaustive candidate charge"
    );

    let mut tight = limits();
    tight.work = baseline.telemetry.work_units + 4;
    let refused = run(tight);
    assert!(
        matches!(
            &refused,
            Err(bounded::TestError::Refused(refusal))
                if matches!(
                    refusal.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::Work
                )
        ),
        "retained dead/future growth must hit the declared work ceiling before visibility can skip it: {refused:?}"
    );
}
