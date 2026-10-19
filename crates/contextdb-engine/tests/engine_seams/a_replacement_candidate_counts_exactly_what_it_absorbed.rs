#![cfg(feature = "test-seams")]
//! While a replacement graph is built for a route, commits after its build
//! frontier are absorbed into it one committed LSN per catch-up. Each catch-up
//! must cover exactly its own LSN: the candidate's frontier names that LSN's
//! commit, its pending-insert counter loses exactly the absorbed inserts that
//! are still live, every later commit stays counted, and tombstones -- deleted
//! nodes still physically in the candidate graph -- are never retired by a
//! catch-up. A crash on either side of a catch-up commit keeps exactly that.
use contextdb_core::{TxId, Value, Wallclock};
use contextdb_engine::writer_commit_test_seams::{
    BuildSuccessor, CatchUpCohortCap, CommitBoundary, cap_catch_up_cohorts_per_wake,
    last_build_successor, observe,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::Arc;

type Counters = ((TxId, u64, u64), Option<(TxId, u64, u64)>);

/// A maintenance cycle absorbs up to its batch quota of committed cohorts;
/// these tests read the candidate's counters after each absorbed cohort, so
/// they let one cycle absorb one.
fn one_cohort_per_cycle() -> CatchUpCohortCap {
    cap_catch_up_cohorts_per_wake(1)
}

fn options() -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(64 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    options
}

fn insert(db: &Database, id: i64, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("vector".into(), Value::Vector(vector)),
        ]),
    )
    .unwrap();
}

fn delete(db: &Database, id: i64) {
    db.execute(
        "DELETE FROM points WHERE id = $id",
        &HashMap::from([("id".into(), Value::Int64(id))]),
    )
    .unwrap();
}

/// A distinct direction per later row, so each is its own nearest answer.
fn later(id: i64) -> Vec<f32> {
    vec![1.0, (id % 97) as f32 * 0.01, (id % 89) as f32 * 0.03]
}

/// A built route with a 2,048-row native import tail and a replacement build
/// queued over it; nothing committed after the replacement's frontier yet.
fn queued_replacement(path: &std::path::Path) -> Database {
    let db = Database::open_with_options(path, options()).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512))", &HashMap::new()).unwrap();
    for id in 0..16 {
        insert(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..2064 {
        insert(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    assert!(db.persist_import_vector_tails().unwrap());
    let queued = db.run_maintenance_cycle().unwrap().vector;
    assert!(queued.first_failure.is_none(), "{:?}", queued.first_failure);
    db
}

fn counters(db: &Database) -> Counters {
    let routes = db
        .vector_pending_counters_for_test("points", "embedding")
        .unwrap();
    assert_eq!(routes.len(), 1, "{routes:?}");
    routes[0]
}

/// Run maintenance cycles until `catch_ups` more candidate catch-ups have
/// committed; return the successor each one committed.
fn drive_catch_ups(db: &Database, catch_ups: usize) -> Vec<BuildSuccessor> {
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    for _ in 0..512 {
        if seen.lock().len() >= catch_ups {
            break;
        }
        let sink = seen.clone();
        let report = observe(
            Arc::new(move |at| {
                if at == CommitBoundary::CandidateCatchUpAfterRedb {
                    sink.lock()
                        .push(last_build_successor().expect("a catch-up successor"));
                }
            }),
            || db.run_maintenance_cycle(),
        )
        .unwrap()
        .vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
    }
    let seen = std::mem::take(&mut *seen.lock());
    assert!(seen.len() >= catch_ups, "{seen:?}");
    seen
}

fn drive_to_idle(db: &Database) {
    for _ in 0..512 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
        if report.remaining_partitions == 0 {
            let status = db
                .execute(
                    "SHOW VECTOR_PARTITIONS FOR points.embedding",
                    &HashMap::new(),
                )
                .unwrap();
            let state = status
                .columns
                .iter()
                .position(|column| column == "maintenance_state")
                .map(|at| status.rows[0][at].clone());
            if state == Some(Value::Text("idle".into())) {
                return;
            }
        }
    }
    panic!("maintenance must finish in finite cycles");
}

fn nearest(db: &Database, query: Vec<f32>) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(query))]),
    )
    .unwrap()
    .rows
}

#[test]
fn each_catch_up_covers_only_its_own_commit_and_keeps_later_inserts_pending() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let _cohorts = one_cohort_per_cycle();
    let root = tempfile::tempdir().unwrap();
    let db = queued_replacement(&root.path().join("three-later.db"));
    for id in [20_001, 20_002, 20_003] {
        insert(&db, id, later(id));
    }
    let (_, before) = counters(&db);
    let before = before.expect("a replacement candidate is building");
    assert_eq!(before.1, 3, "three later inserts pending at the candidate");
    let first = drive_catch_ups(&db, 1).remove(0);
    let (_, candidate) = counters(&db);
    let candidate = candidate.expect("still building");
    eprintln!("after the first catch-up: successor={first:?} candidate={candidate:?}");
    assert_eq!(
        candidate.0, first.frontier_tx,
        "the candidate's counters move to the absorbed commit's transaction"
    );
    assert_eq!(candidate.1, 2, "the two later inserts stay pending");
    let rest = drive_catch_ups(&db, 2);
    assert_eq!(counters(&db).1.map(|c| c.1), Some(0), "{rest:?}");
    drive_to_idle(&db);
    for id in [20_001, 20_002, 20_003] {
        assert_eq!(nearest(&db, later(id)), vec![vec![Value::Int64(id)]]);
    }
}

#[test]
fn catch_up_never_retires_a_tombstone_of_a_node_still_in_the_candidate_graph() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let _cohorts = one_cohort_per_cycle();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("tombstone-debt.db");
    let db = queued_replacement(&path);
    // Row 5 is in the candidate's build snapshot; deleting it now leaves its
    // node in the candidate graph as debt.
    delete(&db, 5);
    insert(&db, 20_001, later(20_001));
    let (_, before) = counters(&db);
    let before = before.expect("a replacement candidate is building");
    let debt = before.2;
    assert!(
        debt >= 1,
        "the delete is counted against the candidate: {before:?}"
    );
    assert_eq!(before.1, 1);
    // The delete's commit is absorbed first: no insert to retire, debt kept.
    drive_catch_ups(&db, 1);
    let after_delete = counters(&db).1.expect("still building");
    assert_eq!(after_delete.1, 1, "the later insert stays pending");
    assert_eq!(
        after_delete.2, debt,
        "absorbing a delete keeps its graph debt"
    );
    // The unrelated later insert is absorbed: still the same debt.
    drive_catch_ups(&db, 1);
    let after_insert = counters(&db).1.expect("still building");
    assert_eq!(after_insert.1, 0);
    assert_eq!(after_insert.2, debt, "an unrelated insert retires no debt");
    drive_to_idle(&db);
    let published = counters(&db).0;
    db.close().unwrap();
    let db = Database::open_with_options(&path, options()).unwrap();
    assert_eq!(counters(&db).0, published, "the counters survive reopen");
    assert_eq!(
        nearest(&db, later(20_001)),
        vec![vec![Value::Int64(20_001)]]
    );
}

#[test]
fn an_absorbed_insert_deleted_later_is_not_subtracted_twice() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let _cohorts = one_cohort_per_cycle();
    let root = tempfile::tempdir().unwrap();
    let db = queued_replacement(&root.path().join("superseded.db"));
    insert(&db, 20_001, later(20_001));
    delete(&db, 20_001);
    insert(&db, 20_002, later(20_002));
    let before = counters(&db)
        .1
        .expect("a replacement candidate is building");
    assert_eq!(before.1, 1, "only 20_002 is a live pending insert");
    // Absorbing 20_001's insert retires nothing: it is no longer live.
    drive_catch_ups(&db, 1);
    assert_eq!(counters(&db).1.expect("still building").1, 1);
    drive_catch_ups(&db, 2);
    assert_eq!(counters(&db).1.map(|c| c.1), Some(0));
    drive_to_idle(&db);
    assert_eq!(
        nearest(&db, later(20_002)),
        vec![vec![Value::Int64(20_002)]]
    );
}

fn crash_at_the_first_catch_up(test: &str, cut: &str) {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let (Ok(path), Ok(out)) = (
        std::env::var("S7_CATCH_UP_CRASH_PATH"),
        std::env::var("S7_CATCH_UP_CRASH_OUT"),
    ) {
        let boundary = if cut == "before" {
            CommitBoundary::CandidateCatchUpBeforeRedb
        } else {
            CommitBoundary::CandidateCatchUpAfterRedb
        };
        let db = Database::open_with_options(path, options()).unwrap();
        for _ in 0..512 {
            let out = out.clone();
            observe(
                Arc::new(move |at| {
                    if at == boundary {
                        let successor = last_build_successor().expect("a catch-up successor");
                        std::fs::write(&out, format!("{}", successor.frontier_tx.0)).unwrap();
                        std::process::exit(77);
                    }
                }),
                || db.run_maintenance_cycle(),
            )
            .unwrap();
        }
        panic!("a catch-up commit must be reached");
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("catch-up-crash.db");
    let out = root.path().join("absorbed-tx");
    let db = queued_replacement(&path);
    for id in [20_001, 20_002, 20_003] {
        insert(&db, id, later(id));
    }
    let before = counters(&db)
        .1
        .expect("a replacement candidate is building");
    assert_eq!(before.1, 3);
    db.close().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg(test)
        .args(["--exact", "--nocapture"])
        .env("S7_CATCH_UP_CRASH_PATH", &path)
        .env("S7_CATCH_UP_CRASH_OUT", &out)
        .status()
        .unwrap();
    assert_eq!(child.code(), Some(77), "the {cut} cut is reached");
    let absorbed_tx = TxId(std::fs::read_to_string(&out).unwrap().parse().unwrap());
    let db = Database::open_with_options(&path, options()).unwrap();
    let after = counters(&db).1.expect("the candidate survives reopen");
    eprintln!(
        "{cut} the first catch-up commit: before={before:?} after={after:?} absorbed_tx={absorbed_tx:?}"
    );
    if cut == "before" {
        assert_eq!(after, before, "nothing of the catch-up is durable");
    } else {
        assert_eq!(
            after.0, absorbed_tx,
            "the absorbed commit is the candidate's frontier"
        );
        assert_eq!(after.1, 2, "exactly the absorbed insert is retired");
        assert_eq!(after.2, before.2);
    }
    drive_to_idle(&db);
    assert_eq!(counters(&db).0.1, 0);
    for id in [20_001, 20_002, 20_003] {
        assert_eq!(nearest(&db, later(id)), vec![vec![Value::Int64(id)]]);
    }
}

#[test]
fn a_crash_after_a_catch_up_commit_keeps_exactly_its_coverage() {
    crash_at_the_first_catch_up(
        "a_replacement_candidate_counts_exactly_what_it_absorbed::a_crash_after_a_catch_up_commit_keeps_exactly_its_coverage",
        "after",
    );
}

#[test]
fn a_crash_before_a_catch_up_commit_keeps_the_previous_coverage() {
    crash_at_the_first_catch_up(
        "a_replacement_candidate_counts_exactly_what_it_absorbed::a_crash_before_a_catch_up_commit_keeps_the_previous_coverage",
        "before",
    );
}
