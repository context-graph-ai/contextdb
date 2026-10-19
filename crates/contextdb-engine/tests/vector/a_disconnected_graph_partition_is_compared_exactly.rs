//! An unfiltered walk that runs out of frontier has reached every row its
//! maintained graph connects. A partition whose snapshot holds more live rows
//! than the walk reached distinct rows has rows its graph does not connect,
//! and which rows those are depends on how the graph was built. INDEXED never
//! hides that behind comparing rows: the whole statement is refused, naming
//! the partition. AUTO compares that partition row by row, through the same
//! charged path EXACT uses, and its route receipt says why. Distinct rows
//! decide it, not graph points: a reached row's superseded versions are that
//! row again, so they cannot hide an unreached one, and deleted history the
//! graph still holds does not move a connected partition off its graph.

use contextdb_core::{Error, Value};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, VectorSearchRoute};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, UnionSearchReceipt,
    with_hnsw_candidate_observer, with_hnsw_unlinked_rows_for_test, with_union_search_receipts,
};
use contextdb_vector::mem::{
    GRAPH_FRONTIER_DISCONNECTED_REASON, NativeVectorScoreObserver,
    with_native_vector_score_observer_for_test,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 8;
/// No more live rows than the walk's breadth (`EF_SEARCH = 300`), so an
/// unfiltered walk explores its graph to the end and the frontier runs out.
const LIVE_ROWS: usize = 300;
/// A 1% expansion ceiling makes AUTO price the walk below comparing every
/// row, so AUTO walks these partitions too. M = 14 gives a level-zero bound
/// of 28 selected links plus the 4 reachability owes, the 32 this pricing
/// was set at. Every graph is connected until a row is cut off on purpose.
const POLICY: &str = "SEARCH_MODE INDEXED AUTO_INDEX_AT 1 AUTO_EXPANSION_CEILING 1 \
                      HNSW (M = 14, EF_SEARCH = 300)";

struct Counter(AtomicU64);

impl HnswCandidateObserver for Counter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeVectorScoreObserver for Counter {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A deterministic vector near `direction` (1 or -1 on every axis).
fn vector(seed: usize, direction: f32) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| {
            let mixed = (seed as u64 + 11)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64 + 5).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            direction + ((mixed ^ (mixed >> 29)) % 10_007) as f32 / 10_007.0 * 0.4 - 0.2
        })
        .collect()
}

fn query() -> Vec<f32> {
    vector(1_000_000, 1.0)
}

fn create_with(db: &Database, clauses: &str) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE gaps (id UUID PRIMARY KEY, \
             embedding VECTOR({DIMENSION}) {POLICY}{clauses})"
        ),
        &HashMap::new(),
    )
    .expect("declare the INDEXED table");
}

fn insert(db: &Database, seed: usize, direction: f32) {
    db.execute(
        "INSERT INTO gaps (id, embedding) VALUES ($id, $embedding)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            (
                "embedding".to_owned(),
                Value::Vector(vector(seed, direction)),
            ),
        ]),
    )
    .expect("insert one deterministic row");
}

fn settle(db: &Database) {
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 1 && status.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish the graph");
}

fn live_fixture(db: &Database) {
    live_fixture_with(db, "");
}

fn live_fixture_with(db: &Database, clauses: &str) {
    create_with(db, clauses);
    for seed in 1..=LIVE_ROWS {
        insert(db, seed, 1.0);
    }
    settle(db);
}

fn row_id(db: &Database, seed: usize) -> u64 {
    db.scan("gaps", db.snapshot())
        .expect("scan the table")
        .into_iter()
        .find_map(|row| match row.values.get("id") {
            Some(Value::Uuid(id)) if *id == Uuid::from_u128(seed as u128) => Some(row.row_id.0),
            _ => None,
        })
        .unwrap_or_else(|| panic!("row {seed} is visible"))
}

struct Run {
    result: QueryResult,
    receipts: Vec<UnionSearchReceipt>,
    graph_scores: u64,
    native_scores: u64,
}

/// The INDEXED search, which must refuse: its typed refusal.
fn refused(db: &Database, unlinked: &[u64]) -> Error {
    with_hnsw_unlinked_rows_for_test(unlinked, || {
        db.execute(
            "SELECT id FROM gaps ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
        )
        .expect_err("INDEXED refuses a partition its graph does not connect")
    })
}

fn run(db: &Database, mode: &str, unlinked: &[u64]) -> Run {
    let graph = Arc::new(Counter(AtomicU64::new(0)));
    let native = Arc::new(Counter(AtomicU64::new(0)));
    let (result, receipts) = with_hnsw_unlinked_rows_for_test(unlinked, || {
        with_union_search_receipts(|| {
            with_hnsw_candidate_observer(graph.clone(), || {
                with_native_vector_score_observer_for_test(native.clone(), || {
                    db.execute(
                        &format!(
                            "SELECT id FROM gaps ORDER BY embedding <=> $query \
                             USE VECTOR {mode} LIMIT 10"
                        ),
                        &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
                    )
                    .unwrap_or_else(|error| panic!("the {mode} search executes: {error:?}"))
                })
            })
        })
    });
    Run {
        result,
        receipts,
        graph_scores: graph.0.load(Ordering::SeqCst),
        native_scores: native.0.load(Ordering::SeqCst),
    }
}

fn ids(result: &QueryResult) -> Vec<Value> {
    result.rows.iter().map(|row| row[0].clone()).collect()
}

fn reasons(result: &QueryResult) -> Vec<(VectorSearchRoute, Option<String>)> {
    result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route")
        .partition_hnsw
        .iter()
        .map(|partition| {
            (
                partition.route.expect("an executed partition has a route"),
                partition.reason.clone(),
            )
        })
        .collect()
}

fn disconnected() -> Vec<(VectorSearchRoute, Option<String>)> {
    vec![(
        VectorSearchRoute::Exact,
        Some(GRAPH_FRONTIER_DISCONNECTED_REASON.to_owned()),
    )]
}

/// The nearest row of the exact answer, by its internal row id.
fn nearest_row(db: &Database, exact: &QueryResult) -> u64 {
    let Value::Uuid(nearest) = exact.rows[0][0].clone() else {
        panic!("the answer carries UUID ids");
    };
    row_id(db, nearest.as_u128() as usize)
}

fn assert_disconnected_row_is_refused_or_compared(db: &Database, label: &str) {
    let exact = run(db, "EXACT", &[]);
    let connected = run(db, "INDEXED", &[]);
    assert!(
        connected.graph_scores > 0,
        "{label}: INDEXED walks the graph"
    );
    assert_eq!(
        ids(&connected.result),
        ids(&exact.result),
        "{label}: control"
    );
    assert_ne!(
        reasons(&connected.result),
        disconnected(),
        "{label}: a connected graph answers from the graph"
    );
    let nearest = nearest_row(db, &exact.result);
    let auto = run(db, "AUTO", &[nearest]);
    assert_eq!(
        ids(&auto.result),
        ids(&exact.result),
        "{label}: AUTO still finds the row the graph does not connect"
    );
    assert_eq!(
        reasons(&auto.result),
        disconnected(),
        "{label}: AUTO's receipt says the partition was compared because its graph is disconnected"
    );
    assert_eq!(
        auto.receipts
            .iter()
            .map(|receipt| receipt.disconnected_partitions)
            .sum::<usize>(),
        1,
        "{label}: the walk handed back one disconnected partition"
    );
    assert_eq!(
        auto.native_scores, exact.native_scores,
        "{label}: the disconnected partition costs AUTO what EXACT costs it"
    );
    // Each detection sets the graph aside; maintenance rebuilds it before
    // the next detection is observed.
    db.run_maintenance_cycle()
        .expect("rebuild the set-aside graph");
    assert!(
        matches!(
            refused(db, &[nearest]),
            Error::VectorIndexedPartitionDisconnected { partition: 0, .. }
        ),
        "{label}: INDEXED refuses the whole statement, naming the partition"
    );
}

#[test]
fn a_live_row_the_graph_does_not_connect_refuses_indexed_and_is_compared_by_auto() {
    let db = Database::open_memory();
    live_fixture(&db);
    assert_disconnected_row_is_refused_or_compared(&db, "resident");

    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("disconnected.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        live_fixture(&db);
        db.close().expect("close the fixture");
    }
    let reopened = Database::open(&path).expect("reopen the saved graph");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_disconnected_row_is_refused_or_compared(&reopened, "reopened");
}

#[test]
fn deleted_history_does_not_move_a_connected_partition_off_its_graph() {
    let db = Database::open_memory();
    live_fixture(&db);
    let answer = ids(&run(&db, "INDEXED", &[]).result);
    // Once the graph holds deleted entries every kept row is re-scored from
    // its stored vector, a cost the kept set bounds; the first round with
    // history is the baseline the later, longer histories must not exceed.
    let mut first = None;
    let mut next_seed = 10_000;
    for round in 1..=3 {
        // Rows far from the query, inserted, maintained into the graph and
        // deleted: the live population stays at LIVE_ROWS.
        let seeds = next_seed..next_seed + LIVE_ROWS;
        next_seed += LIVE_ROWS;
        for seed in seeds.clone() {
            insert(&db, seed, -1.0);
        }
        settle(&db);
        for seed in seeds {
            db.execute(
                "DELETE FROM gaps WHERE id = $id",
                &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
            )
            .expect("delete one history row");
        }
        settle(&db);
        let observed = run(&db, "INDEXED", &[]);
        assert_eq!(ids(&observed.result), answer, "round {round}: same answer");
        assert_ne!(
            reasons(&observed.result),
            disconnected(),
            "round {round}: deleted history leaves the partition on its graph"
        );
        assert_eq!(
            observed
                .receipts
                .iter()
                .map(|receipt| receipt.disconnected_partitions)
                .sum::<usize>(),
            0,
            "round {round}: no disconnected partition"
        );
        let work = (observed.graph_scores, observed.native_scores);
        assert_eq!(
            work,
            *first.get_or_insert(work),
            "round {round}: work does not grow with the deleted history"
        );
    }
}

#[test]
fn reached_rows_superseded_versions_cannot_hide_an_unreached_row() {
    let db = Database::open_memory();
    // No consolidation: a replaced row's old point stays in the base graph
    // beside its new point in the tail, both reached as the same row.
    live_fixture_with(&db, " CONSOLIDATION NONE");
    let before = (1..=LIVE_ROWS)
        .map(|seed| row_id(&db, seed))
        .collect::<Vec<_>>();
    // A new version of every row, each reached beside its superseded one,
    // so the walk admits more graph points than the partition holds rows.
    for seed in 1..=LIVE_ROWS {
        db.execute(
            "UPDATE gaps SET embedding = $embedding WHERE id = $id",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                (
                    "embedding".to_owned(),
                    Value::Vector(vector(seed + 500, 1.0)),
                ),
            ]),
        )
        .expect("replace one row's vector");
    }
    settle(&db);
    let after = (1..=LIVE_ROWS)
        .map(|seed| row_id(&db, seed))
        .collect::<Vec<_>>();
    let exact = run(&db, "EXACT", &[]);
    let nearest = nearest_row(&db, &exact.result);
    let cut = run(&db, "AUTO", &[nearest]);
    let admissions = cut
        .receipts
        .iter()
        .map(|receipt| receipt.allowed_admissions)
        .sum::<usize>();
    // Every connected current row is admitted once; the superseded version
    // beside it is reached but never competes as a row.
    assert_eq!(
        admissions,
        LIVE_ROWS - 1,
        "each connected row's current version is admitted once (row ids kept across \
         replacement: {})",
        before == after
    );
    assert_eq!(ids(&cut.result), ids(&exact.result));
    assert_eq!(reasons(&cut.result), disconnected());
    db.run_maintenance_cycle()
        .expect("rebuild the set-aside graph");
    assert!(matches!(
        refused(&db, &[nearest]),
        Error::VectorIndexedPartitionDisconnected { partition: 0, .. }
    ));
}

/// One text fact of the single partition SHOW VECTOR_PARTITIONS reports.
fn inspected(db: &Database, name: &str) -> String {
    let shown = db
        .execute("SHOW VECTOR_PARTITIONS FOR gaps.embedding", &HashMap::new())
        .expect("inspect the partition");
    let position = shown
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("SHOW VECTOR_PARTITIONS exposes {name}: {:?}", shown.columns));
    assert_eq!(shown.rows.len(), 1, "one partition");
    match &shown.rows[0][position] {
        Value::Text(value) => value.clone(),
        value => panic!("{name} is text: {value:?}"),
    }
}

#[test]
fn a_detected_cut_off_sets_the_partition_aside_until_maintenance_rebuilds_it() {
    let db = Database::open_memory();
    live_fixture(&db);
    let exact = run(&db, "EXACT", &[]);
    let nearest = nearest_row(&db, &exact.result);
    assert_eq!(inspected(&db, "query_state"), "ready");
    assert!(matches!(
        refused(&db, &[nearest]),
        Error::VectorIndexedPartitionDisconnected { partition: 0, .. }
    ));
    // The cut-off graph is set aside, as corrupt change state is: inspection
    // names it and its next action, and INDEXED stays refused even where the
    // walk would no longer meet the cut.
    assert_eq!(inspected(&db, "query_state"), "unavailable");
    assert_eq!(inspected(&db, "availability_reason"), "disconnected_graph");
    assert_eq!(inspected(&db, "recovery_action"), "run_maintenance_cycle");
    assert!(matches!(
        db.execute(
            "SELECT id FROM gaps ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
        ),
        Err(Error::VectorIndexedRouteUnavailable { .. })
    ));
    // One maintenance run rebuilds it, and INDEXED returns the row again.
    db.run_maintenance_cycle()
        .expect("one caller-driven maintenance cycle");
    assert_eq!(inspected(&db, "query_state"), "ready");
    assert_eq!(inspected(&db, "availability_reason"), "none");
    let answered = run(&db, "INDEXED", &[]);
    assert_eq!(ids(&answered.result), ids(&exact.result));
    assert!(answered.graph_scores > 0, "the rebuilt graph answers");
}

/// Maintenance repairs a disconnected graph under the declarations it
/// already has, however often a cut-off recurs: each detected cut sets the
/// partition aside, the next maintenance cycle rebuilds it with the same
/// declared policy and limits, and INDEXED answers again. No retuning is
/// asked for and no repair is withheld.
#[test]
fn a_cut_off_that_recurs_is_repaired_each_time_without_retuning() {
    let db = Database::open_memory();
    live_fixture(&db);
    let exact = run(&db, "EXACT", &[]);
    let nearest = nearest_row(&db, &exact.result);
    for round in 1..=3 {
        assert!(
            matches!(
                refused(&db, &[nearest]),
                Error::VectorIndexedPartitionDisconnected { .. }
            ),
            "round {round}: the cut is detected"
        );
        assert_eq!(
            inspected(&db, "query_state"),
            "unavailable",
            "round {round}"
        );
        assert_eq!(inspected(&db, "availability_reason"), "disconnected_graph");
        assert_eq!(inspected(&db, "recovery_action"), "run_maintenance_cycle");
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        assert_eq!(
            inspected(&db, "query_state"),
            "ready",
            "round {round}: repaired"
        );
        assert_eq!(
            ids(&run(&db, "INDEXED", &[]).result),
            ids(&exact.result),
            "round {round}: INDEXED answers from the repaired graph"
        );
    }
}

/// A disconnection report names the graph the search walked. Once the walk
/// has given its tail back, a publication can replace that graph before the
/// report lands; the report then says nothing about the graph serving now,
/// and nothing is set aside. Deterministically: the search is held right after
/// its walk, maintenance publishes a rebuild of the same partition, and the
/// released search's report is ignored -- the rebuilt graph stays ready. The
/// same report about the graph that still serves sets it aside.
#[test]
fn a_report_landing_after_a_rebuild_sets_nothing_aside() {
    let db = Arc::new(Database::open_memory());
    live_fixture(&db);
    // Deleting a third of the rows passes the declared tombstone share, so
    // the next maintenance cycle publishes a rebuilt graph.
    for seed in 201..=LIVE_ROWS {
        db.execute(
            "DELETE FROM gaps WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
        )
        .expect("delete one row");
    }
    let exact = run(&db, "EXACT", &[]);
    let nearest = nearest_row(&db, &exact.result);
    let rebuilds = Arc::new(AtomicU64::new(0));
    let hook = {
        let db = Arc::clone(&db);
        let rebuilds = Arc::clone(&rebuilds);
        Box::new(move || {
            for _ in 0..8 {
                let report = db
                    .run_maintenance_cycle()
                    .expect("one caller-driven maintenance cycle");
                if report.vector.built_partitions != 0 {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
    };
    let searched = contextdb_vector::mem::with_after_walk_hook_for_test(hook, || {
        with_hnsw_unlinked_rows_for_test(&[nearest], || {
            db.execute(
                "SELECT id FROM gaps ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
                &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
            )
        })
    });
    assert!(
        rebuilds.load(Ordering::SeqCst) > 0,
        "maintenance rebuilt the partition between the walk and its report"
    );
    assert!(
        matches!(
            searched,
            Err(Error::VectorIndexedPartitionDisconnected { .. })
        ),
        "the held search still found the cut in the graph it walked: {searched:?}"
    );
    assert_eq!(
        inspected(&db, "query_state"),
        "ready",
        "the rebuilt graph is not set aside for a report about the one it replaced"
    );
    assert_eq!(ids(&run(&db, "INDEXED", &[]).result), ids(&exact.result));
    // The same report about the graph that serves now does set it aside.
    assert!(matches!(
        refused(&db, &[nearest]),
        Error::VectorIndexedPartitionDisconnected { .. }
    ));
    assert_eq!(inspected(&db, "query_state"), "unavailable");
}

const PARTITIONS: i64 = 3;

/// Three partitions of LIVE_ROWS rows each, every graph maintained.
fn partitioned_fixture(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE gaps_p (id UUID PRIMARY KEY, partition_id INTEGER NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (partition_id) MAX_PARTITIONS 8 \
             {POLICY})"
        ),
        &HashMap::new(),
    )
    .expect("declare the partitioned table");
    for partition in 0..PARTITIONS {
        for row in 1..=LIVE_ROWS {
            let seed = partition as usize * 1_000 + row;
            db.execute(
                "INSERT INTO gaps_p (id, partition_id, embedding) VALUES ($id, $partition, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                    ("partition".to_owned(), Value::Int64(partition)),
                    ("embedding".to_owned(), Value::Vector(vector(seed, 1.0))),
                ]),
            )
            .expect("insert one deterministic row");
        }
    }
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == PARTITIONS as usize && status.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish every partition graph");
}

fn partitioned(db: &Database, mode: &str, unlinked: &[u64]) -> Result<Run, Error> {
    let native = Arc::new(Counter(AtomicU64::new(0)));
    let graph = Arc::new(Counter(AtomicU64::new(0)));
    let (result, receipts) = with_hnsw_unlinked_rows_for_test(unlinked, || {
        with_union_search_receipts(|| {
            with_hnsw_candidate_observer(graph.clone(), || {
                with_native_vector_score_observer_for_test(native.clone(), || {
                    db.execute(
                        &format!(
                            "SELECT id FROM gaps_p ORDER BY embedding <=> $query \
                             USE VECTOR {mode} LIMIT 10"
                        ),
                        &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
                    )
                })
            })
        })
    });
    result.map(|result| Run {
        result,
        receipts,
        graph_scores: graph.0.load(Ordering::SeqCst),
        native_scores: native.0.load(Ordering::SeqCst),
    })
}

/// The row id of each of the first two partitions' nearest rows.
fn nearest_of_first_two_partitions(db: &Database) -> Vec<u64> {
    (0..2)
        .map(|partition| {
            let exact = db
                .execute(
                    "SELECT id FROM gaps_p WHERE partition_id = $partition \
                     ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                    &HashMap::from([
                        ("partition".to_owned(), Value::Int64(partition)),
                        ("query".to_owned(), Value::Vector(query())),
                    ]),
                )
                .expect("exact nearest of one partition");
            let Value::Uuid(id) = exact.rows[0][0].clone() else {
                panic!("UUID ids");
            };
            db.scan("gaps_p", db.snapshot())
                .expect("scan")
                .into_iter()
                .find_map(|row| match row.values.get("id") {
                    Some(Value::Uuid(value)) if *value == id => Some(row.row_id.0),
                    _ => None,
                })
                .expect("the nearest row is visible")
        })
        .collect()
}

fn assert_several_disconnected_partitions(db: &Database, label: &str) {
    let exact = partitioned(db, "EXACT", &[]).expect("EXACT answers");
    let cut = nearest_of_first_two_partitions(db);
    // A reopened store loads its saved graphs on the first walk and keeps
    // them resident; the ledger is read after that warm walk. A parallel
    // build can leave a distinct row cut off on its own; the product sets
    // such a graph aside and maintenance rebuilds it, and the fixture does the
    // same until its graphs are connected before any row is cut on purpose.
    let mut warm = partitioned(db, "AUTO", &[]).expect("a warm AUTO walk");
    for _ in 0..4 {
        let found = warm
            .receipts
            .iter()
            .map(|receipt| receipt.disconnected_partitions)
            .sum::<usize>();
        if found == 0 {
            break;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        warm = partitioned(db, "AUTO", &[]).expect("a warm AUTO walk");
    }
    assert_eq!(
        warm.receipts
            .iter()
            .map(|receipt| receipt.disconnected_partitions)
            .sum::<usize>(),
        0,
        "{label}: the fixture's graphs are connected before any row is cut"
    );
    // The partition flags are admitted before any walk reports: refused
    // there, the statement is refused whole, typed, and charges nothing.
    let settled = db.accountant().usage().used;
    contextdb_vector::mem::reset_vector_admission_site_for_test();
    contextdb_vector::mem::refuse_vector_admission_once_for_test("disconnected_slots");
    let refused = partitioned(db, "AUTO", &cut);
    contextdb_vector::mem::refuse_vector_admission_once_for_test("");
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "{label}: a refused partition-flag admission is a typed memory refusal, got {:?}",
        refused.as_ref().map(|run| ids(&run.result))
    );
    assert_eq!(
        contextdb_vector::mem::refused_vector_admission_site_for_test(),
        "disconnected_slots",
        "{label}: the refusal names the partition-flag admission"
    );
    assert_eq!(
        db.accountant().usage().used,
        settled,
        "{label}: nothing stays charged"
    );
    // Admitted, two partitions are reported disconnected in one statement.
    let auto = partitioned(db, "AUTO", &cut).expect("AUTO answers");
    assert_eq!(
        ids(&auto.result),
        ids(&exact.result),
        "{label}: AUTO finds both cut rows"
    );
    let disconnected_routes = reasons(&auto.result)
        .into_iter()
        .filter(|(_, reason)| reason.as_deref() == Some(GRAPH_FRONTIER_DISCONNECTED_REASON))
        .count();
    assert_eq!(
        disconnected_routes, 2,
        "{label}: both cut partitions are disclosed"
    );
    assert_eq!(
        auto.receipts
            .iter()
            .map(|receipt| receipt.disconnected_partitions)
            .sum::<usize>(),
        2,
        "{label}: the walks flagged two partitions"
    );
    assert_eq!(
        db.accountant().usage().used,
        settled,
        "{label}: the flags go back"
    );
}

#[test]
fn several_disconnected_partitions_are_flagged_in_one_admitted_array() {
    let db = Database::open_memory();
    partitioned_fixture(&db);
    assert_several_disconnected_partitions(&db, "resident");

    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("disconnected-partitions.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        partitioned_fixture(&db);
        db.close().expect("close the fixture");
    }
    let reopened = Database::open(&path).expect("reopen the saved graphs");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_several_disconnected_partitions(&reopened, "reopened");
}
