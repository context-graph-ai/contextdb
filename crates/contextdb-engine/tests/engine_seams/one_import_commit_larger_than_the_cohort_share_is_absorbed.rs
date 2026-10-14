#![cfg(feature = "test-seams")]
//! A cg file is one committed LSN. When that one commit's new vectors exceed
//! the share of the writer limit an import cohort may retain, making them
//! searchable must still complete under the unchanged limit: the route is
//! rebuilt by a replacement build in fixed batches, durable and unpublished
//! until it covers the commit, then published atomically. The call covers
//! exactly the commits up to its fixed target; a crash at any batch keeps the
//! old serving coverage and resumes after the last durable batch.
use contextdb_core::{Lsn, TxId, Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams;
use contextdb_engine::vector_observations::observe_import_rebuild_steps_for_test;
use contextdb_engine::vector_read_memory::unowned_decodes_for_test;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, last_build_successor, observe};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::hnsw::paged_builder::PagedVectorPhase;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

const DIMENSION: usize = 1536;
const BASE_ROWS: i64 = 16;
const ONE_COMMIT: usize = 4096;
const ROOMY: usize = 512 * 1024 * 1024;
const TIGHT: usize = 16 * 1024 * 1024;

fn options(writer: usize) -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(writer),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    options
}

/// A unique direction per row: no two rows share a nearest neighbour.
fn vector_for(id: i64) -> Vec<f32> {
    let mut values: Vec<f32> = (0..DIMENSION)
        .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.25)
        .collect();
    values[(id as usize) % DIMENSION] += 4.0;
    values[(id as usize * 7 + 3) % DIMENSION] += 2.0;
    values
}

fn insert_one(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("vector".into(), Value::Vector(vector_for(id))),
        ]),
    )
    .unwrap();
}

/// One INSERT statement, one committed LSN, `rows` new vectors from `first`.
fn one_commit(db: &Database, first: i64, rows: usize) {
    let mut sql = String::from("INSERT INTO points VALUES ");
    let mut params = HashMap::new();
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("($i{row}, 1, $v{row})"));
        let id = first + row as i64;
        params.insert(format!("i{row}"), Value::Int64(id));
        params.insert(format!("v{row}"), Value::Vector(vector_for(id)));
    }
    db.execute(&sql, &params).unwrap();
}

/// A built 16-row route, then one commit of 4,096 vectors, closed.
fn store_with_one_large_commit(path: &std::path::Path) {
    store_with_one_commit_of(path, ONE_COMMIT);
}

/// A built 16-row route, then one commit of `rows` vectors, closed.
fn store_with_one_commit_of(path: &std::path::Path, rows: usize) {
    let db = Database::open_with_options(path, options(ROOMY)).unwrap();
    db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)"), &HashMap::new()).unwrap();
    for id in 0..BASE_ROWS {
        insert_one(&db, id);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    one_commit(&db, 1_000_000, rows);
    db.close().unwrap();
}

fn serving_pending(db: &Database) -> usize {
    let routes = db
        .vector_serving_pending_inserts_for_test("points", "embedding")
        .unwrap();
    assert_eq!(routes.len(), 1, "{routes:?}");
    routes[0].3
}

fn nearest(db: &Database, id: i64) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vector_for(id)))]),
    )
    .unwrap()
    .rows
}

fn count(db: &Database) -> i64 {
    match db
        .execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new(),
        )
        .unwrap()
        .rows[0][0]
    {
        Value::Int64(count) => count,
        ref other => panic!("{other:?}"),
    }
}

/// Successors committed by replacement-build batches during `operation`.
fn with_batches<T>(
    operation: impl FnOnce() -> T,
) -> (T, Vec<(CommitBoundary, usize, PagedVectorPhase)>) {
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let result = observe(
        Arc::new(move |at| {
            if matches!(
                at,
                CommitBoundary::BuildBatchBeforeRedb | CommitBoundary::BuildBatchAfterRedb
            ) {
                let successor = last_build_successor().expect("a build successor");
                sink.lock().push((at, successor.points, successor.phase));
            }
        }),
        operation,
    );
    let seen = std::mem::take(&mut *seen.lock());
    (result, seen)
}

#[test]
fn one_committed_lsn_larger_than_the_cohort_share_is_absorbed_without_raising_the_limit() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("one-large-commit.db");
    store_with_one_large_commit(&path);
    let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
    assert_eq!(serving_pending(&db), ONE_COMMIT);
    let unowned = unowned_decodes_for_test();
    let ((completed, work), batches) =
        with_batches(|| resident_history_test_seams::observe(|| db.persist_import_vector_tails()));
    let inserting = batches
        .iter()
        .filter(|(at, _, phase)| {
            *at == CommitBoundary::BuildBatchAfterRedb && *phase == PagedVectorPhase::Inserting
        })
        .count();
    eprintln!(
        "one large commit: completed={completed:?} inserting_batches={inserting} source_visits={} high_water={}",
        work.maintenance_source_visits,
        db.write_memory_high_water_for_test()
    );
    assert_eq!(completed.as_ref().ok(), Some(&true), "{completed:?}");
    assert_eq!(db.memory_limit(), Some(TIGHT), "the limit is never raised");
    assert!(
        inserting > 1,
        "the commit is absorbed in more than one batch"
    );
    assert!(db.write_memory_high_water_for_test() <= TIGHT);
    assert_eq!(unowned_decodes_for_test(), unowned, "no ownerless decode");
    assert_eq!(serving_pending(&db), 0, "the whole commit is covered");
    assert!(
        db.vector_build_progress_for_test("points", "embedding")
            .unwrap()
            .is_empty()
    );
    assert_eq!(count(&db), BASE_ROWS + ONE_COMMIT as i64);
    for id in [1_000_000, 1_002_047, 1_004_095, 3] {
        assert_eq!(nearest(&db, id), vec![vec![Value::Int64(id)]]);
    }
}

#[test]
fn an_older_build_publishes_at_its_own_frontier_before_a_fresh_build_absorbs_the_large_commit() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("older-build.db");
    {
        let db = Database::open_with_options(&path, options(ROOMY)).unwrap();
        db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512))"), &HashMap::new()).unwrap();
        for id in 0..BASE_ROWS {
            insert_one(&db, id);
        }
        while db
            .run_maintenance_cycle()
            .unwrap()
            .vector
            .remaining_partitions
            != 0
        {}
        for id in BASE_ROWS..BASE_ROWS + 2048 {
            insert_one(&db, id);
        }
        db.close().unwrap();
    }
    {
        // Under the tight limit one cycle starts a replacement and leaves it
        // unfinished: four batches of a 2,064-point build.
        let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
        assert!(db.persist_import_vector_tails().unwrap());
        let cycle = db.run_maintenance_cycle().unwrap().vector;
        assert!(cycle.first_failure.is_none(), "{:?}", cycle.first_failure);
        let builds = db
            .vector_build_progress_for_test("points", "embedding")
            .unwrap();
        eprintln!("older build after one cycle: {builds:?}");
        assert_eq!(builds.len(), 1, "a replacement build is in progress");
        assert_eq!(builds[0].0, PagedVectorPhase::Inserting);
        db.close().unwrap();
    }
    {
        let db = Database::open_with_options(&path, options(ROOMY)).unwrap();
        one_commit(&db, 1_000_000, ONE_COMMIT);
        db.close().unwrap();
    }
    let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
    let older_frontier = db
        .vector_build_progress_for_test("points", "embedding")
        .unwrap()[0]
        .2;
    let ((completed, _), batches) =
        with_batches(|| resident_history_test_seams::observe(|| db.persist_import_vector_tails()));
    assert_eq!(completed.as_ref().ok(), Some(&true), "{completed:?}");
    assert_eq!(db.memory_limit(), Some(TIGHT));
    eprintln!(
        "older frontier {older_frontier:?}, {} build transactions",
        batches.len()
    );
    assert_eq!(serving_pending(&db), 0);
    assert_eq!(count(&db), BASE_ROWS + 2048 + ONE_COMMIT as i64);
    for id in [1_000_000, 1_004_095, 1000] {
        assert_eq!(nearest(&db, id), vec![vec![Value::Int64(id)]]);
    }
}

#[test]
fn a_large_commit_after_the_import_target_stays_pending_for_the_next_call() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("after-target.db");
    store_with_one_large_commit(&path);
    let db = Arc::new(Database::open_with_options(&path, options(TIGHT)).unwrap());
    // Between two build steps of this call, another writer commits 256
    // vectors in one commit: larger than the cohort share, after the target.
    let committed = Arc::new(AtomicBool::new(false));
    let late = (Arc::clone(&db), Arc::clone(&committed));
    let _steps = observe_import_rebuild_steps_for_test(Arc::new(move |_| {
        if !late.1.swap(true, Ordering::SeqCst) {
            let db = Arc::clone(&late.0);
            std::thread::spawn(move || one_commit(&db, 2_000_000, 256))
                .join()
                .unwrap();
        }
    }));
    assert!(db.persist_import_vector_tails().unwrap());
    drop(_steps);
    assert!(committed.load(Ordering::SeqCst), "the late commit happened");
    assert_eq!(
        serving_pending(&db),
        256,
        "the commit after this call's target stays pending, uncovered"
    );
    assert_eq!(nearest(&db, 1_000_000), vec![vec![Value::Int64(1_000_000)]]);
    // It is answered from the pending suffix meanwhile, and the next call
    // covers it.
    assert_eq!(nearest(&db, 2_000_100), vec![vec![Value::Int64(2_000_100)]]);
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(serving_pending(&db), 0);
    assert_eq!(count(&db), BASE_ROWS + ONE_COMMIT as i64 + 256);
}

#[test]
fn a_pinned_reader_keeps_its_answer_while_the_large_commit_is_rebuilt_and_published() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("pinned-reader.db");
    store_with_one_large_commit(&path);
    let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
    let query = "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1";
    let params = HashMap::from([("q".into(), Value::Vector(vector_for(1_000_123)))]);
    let pinned = db.begin().unwrap();
    let before = db.execute_in_tx(pinned, query, &params).unwrap().rows;
    assert_eq!(before, vec![vec![Value::Int64(1_000_123)]]);
    assert!(db.persist_import_vector_tails().unwrap());
    db.execute(
        "DELETE FROM points WHERE id = $id",
        &HashMap::from([("id".into(), Value::Int64(1_000_123))]),
    )
    .unwrap();
    insert_one(&db, 3_000_000);
    let cycle = db.run_maintenance_cycle().unwrap().vector;
    assert!(cycle.first_failure.is_none(), "{:?}", cycle.first_failure);
    assert_eq!(
        db.execute_in_tx(pinned, query, &params).unwrap().rows,
        before,
        "the pinned reader keeps its original answer"
    );
    let fresh = db.execute(query, &params).unwrap().rows;
    assert_ne!(fresh, before, "a fresh reader sees the delete");
    assert_eq!(nearest(&db, 3_000_000), vec![vec![Value::Int64(3_000_000)]]);
    db.rollback(pinned).unwrap();
}

#[test]
fn a_rebuild_holds_the_same_decode_charge_at_every_step_whatever_its_size() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let mut envelopes = Vec::new();
    for rows in [1024, ONE_COMMIT] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("step-envelope.db");
        store_with_one_commit_of(&path, rows);
        let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
        // At each step boundary, the bytes the import's own decode owner
        // still holds: a step's build records and inspection decodes are
        // returned with the step, never accumulated across steps.
        let held = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let sink = held.clone();
        let steps = observe_import_rebuild_steps_for_test(Arc::new(move |import_held| {
            sink.lock().push(import_held);
        }));
        assert!(db.persist_import_vector_tails().unwrap());
        drop(steps);
        assert_eq!(db.memory_limit(), Some(TIGHT), "the limit is never raised");
        assert_eq!(serving_pending(&db), 0);
        let held = std::mem::take(&mut *held.lock());
        eprintln!(
            "rebuild of {rows}: {} step boundaries, import decode held {:?}..{:?}",
            held.len(),
            held.first(),
            held.last()
        );
        assert!(held.len() > 1, "the rebuild takes more than one step");
        assert!(
            held.iter().all(|bytes| *bytes == held[0]),
            "the import's decode owner holds the same at every step: {held:?}"
        );
        envelopes.push((rows, held.len(), held[0]));
    }
    assert!(envelopes[1].1 > envelopes[0].1, "{envelopes:?}");
    assert_eq!(
        envelopes[0].2, envelopes[1].2,
        "the same envelope at both build sizes: {envelopes:?}"
    );
}

/// Run the import in a child that dies at `cut`, then prove reopen and resume.
fn crash_inside_the_large_commit(test: &str, cut: &str) {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    const K: usize = 3;
    if let Ok(path) = std::env::var("S7_REBUILD_CRASH_PATH") {
        let db = Database::open_with_options(path, options(TIGHT)).unwrap();
        let inserted = Arc::new(AtomicUsize::new(0));
        let cut_at = cut.to_string();
        observe(
            Arc::new(move |at| {
                let Some(successor) = last_build_successor() else {
                    return;
                };
                let inserting = successor.phase == PagedVectorPhase::Inserting;
                match (cut_at.as_str(), at) {
                    ("after", CommitBoundary::BuildBatchAfterRedb) if inserting => {
                        if inserted.fetch_add(1, Ordering::SeqCst) + 1 == K {
                            std::process::exit(77);
                        }
                    }
                    ("before", CommitBoundary::BuildBatchAfterRedb) if inserting => {
                        inserted.fetch_add(1, Ordering::SeqCst);
                    }
                    ("before", CommitBoundary::BuildBatchBeforeRedb)
                        if inserting && inserted.load(Ordering::SeqCst) == K =>
                    {
                        std::process::exit(77);
                    }
                    ("complete", CommitBoundary::BuildBatchAfterRedb)
                        if successor.phase == PagedVectorPhase::Complete =>
                    {
                        std::process::exit(77);
                    }
                    _ => {}
                }
            }),
            || db.persist_import_vector_tails(),
        )
        .unwrap();
        panic!("the {cut} cut must be reached");
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("rebuild-crash.db");
    store_with_one_large_commit(&path);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg(test)
        .args(["--exact", "--nocapture"])
        .env("S7_REBUILD_CRASH_PATH", &path)
        .status()
        .unwrap();
    assert_eq!(child.code(), Some(77), "the {cut} cut is reached");
    let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
    assert_eq!(
        serving_pending(&db),
        ONE_COMMIT,
        "the unpublished build leaves the old serving coverage"
    );
    let builds = db
        .vector_build_progress_for_test("points", "embedding")
        .unwrap();
    eprintln!("{cut}: durable build after the crash: {builds:?}");
    assert_eq!(builds.len(), 1, "the build record survives");
    let (phase, points, _) = builds[0];
    match cut {
        "complete" => assert_eq!(phase, PagedVectorPhase::Complete),
        _ => {
            assert_eq!(phase, PagedVectorPhase::Inserting);
            assert_eq!(points, 16 * K, "exactly {K} batches are durable");
        }
    }
    let ((completed, work), batches) =
        with_batches(|| resident_history_test_seams::observe(|| db.persist_import_vector_tails()));
    assert_eq!(completed.as_ref().ok(), Some(&true), "{completed:?}");
    let resumed: Vec<usize> = batches
        .iter()
        .filter(|(at, _, phase)| {
            *at == CommitBoundary::BuildBatchAfterRedb && *phase == PagedVectorPhase::Inserting
        })
        .map(|(_, points, _)| *points)
        .collect();
    eprintln!(
        "{cut}: resumed inserting batches end at {resumed:?}, source visits {}",
        work.maintenance_source_visits
    );
    if cut == "complete" {
        assert!(
            resumed.is_empty(),
            "a complete build only publishes on resume"
        );
    } else {
        assert_eq!(
            resumed.first().copied(),
            Some(16 * (K + 1)),
            "resume continues after the last durable batch"
        );
    }
    assert_eq!(serving_pending(&db), 0);
    assert_eq!(count(&db), BASE_ROWS + ONE_COMMIT as i64);
    for id in [1_000_000, 1_004_095] {
        assert_eq!(nearest(&db, id), vec![vec![Value::Int64(id)]]);
    }
}

#[test]
fn a_crash_after_a_rebuild_batch_resumes_after_it() {
    crash_inside_the_large_commit(
        "one_import_commit_larger_than_the_cohort_share_is_absorbed::a_crash_after_a_rebuild_batch_resumes_after_it",
        "after",
    );
}

#[test]
fn a_crash_before_a_rebuild_batch_is_durable_resumes_at_the_previous_one() {
    crash_inside_the_large_commit(
        "one_import_commit_larger_than_the_cohort_share_is_absorbed::a_crash_before_a_rebuild_batch_is_durable_resumes_at_the_previous_one",
        "before",
    );
}

#[test]
fn a_crash_after_the_rebuild_completes_publishes_it_on_resume() {
    crash_inside_the_large_commit(
        "one_import_commit_larger_than_the_cohort_share_is_absorbed::a_crash_after_the_rebuild_completes_publishes_it_on_resume",
        "complete",
    );
}

#[allow(dead_code)]
fn _types(_: Lsn, _: TxId) {}
