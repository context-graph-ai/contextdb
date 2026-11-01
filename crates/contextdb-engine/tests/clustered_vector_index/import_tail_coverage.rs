//! `Database::persist_import_vector_tails` answers one question for a bulk
//! import: is every committed passage vector covered by the published
//! clustered index (its committed write buffer included)? It builds nothing
//! and chases no later commit; the existing maintenance cycle advances the
//! index and the caller alternates the two. Every transition here is driven by
//! an explicit call and read from a returned value or a report counter.

use super::support::{empty, ivf, params, unit_vector};
use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::{VectorBuildObserver, with_vector_build_observer};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const DIMENSIONS: usize = 16;
const IMPORTED: i64 = 40;
/// A finite cap on maintenance cycles, so a defect fails instead of hanging.
const MAX_CYCLES: usize = 256;

#[derive(Default)]
struct Builds(AtomicU64);

impl VectorBuildObserver for Builds {
    fn before_vector_build(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn open(path: &std::path::Path) -> Database {
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the file-backed import store");
    db.execute(
        &format!(
            "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    db
}

/// `open` for a table whose cleanup reclaims a replaced vector, as the
/// context-graph consumer declares its passages.
fn open_current_only(path: &std::path::Path) -> Database {
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the file-backed import store");
    db.execute(
        &format!(
            "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {}) HISTORY CURRENT ONLY SYNC OFF",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    db
}

/// Give `ids` new vectors, leaving their previous vectors dead. Row `id` is
/// then nearest to `unit_vector(id + salt)`.
fn replace(db: &Database, ids: std::ops::Range<i64>, salt: u64) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "UPDATE passages SET embedding = $embedding WHERE id = $id",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64 + salt, DIMENSIONS)),
                ),
            ]),
        )
        .expect("replace one passage vector");
    }
    db.commit(tx).expect("commit the replaced vectors");
}

fn commit(db: &Database, ids: std::ops::Range<i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO passages (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one passage vector");
    }
    db.commit(tx).expect("commit the passage vectors");
}

/// One maintenance cycle, failing the test if the report shows a failure;
/// returns the partitions the report still lists as remaining.
fn cycle(db: &Database) -> usize {
    let report = db
        .run_maintenance_cycle()
        .expect("one caller-driven maintenance cycle")
        .vector;
    assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
    report.remaining_partitions
}

/// Maintenance cycles until the report shows nothing remaining; the count of
/// cycles it took, read from the reports and never from time.
fn maintain_to_completion(db: &Database) -> usize {
    for cycles in 1..=MAX_CYCLES {
        if cycle(db) == 0 {
            return cycles;
        }
    }
    panic!("maintenance did not finish within {MAX_CYCLES} cycles");
}

/// The call sequence of the context-graph consumer's `maintain_passage_vectors`:
/// ask, run maintenance, ask again, and return when covered or when nothing
/// remains. Returns the maintenance cycles it ran.
fn maintain_passage_vectors(db: &Database) -> usize {
    let mut cycles = 0;
    if db
        .persist_import_vector_tails()
        .expect("first coverage ask")
    {
        return cycles;
    }
    loop {
        let remaining = cycle(db);
        cycles += 1;
        if db.persist_import_vector_tails().expect("coverage ask") {
            return cycles;
        }
        if remaining == 0 {
            return cycles;
        }
        assert!(cycles < MAX_CYCLES, "the consumer loop must be finite");
    }
}

#[test]
fn committed_vectors_are_not_covered_until_the_index_is_published() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open(&root.path().join("incomplete.db"));
    commit(&db, 0..IMPORTED);
    assert!(
        !db.persist_import_vector_tails().expect("coverage ask"),
        "no maintenance cycle has run, so the published index does not cover the committed vectors"
    );
}

#[test]
fn maintenance_to_completion_covers_every_committed_vector() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open(&root.path().join("complete.db"));
    commit(&db, 0..IMPORTED);
    assert!(!db.persist_import_vector_tails().expect("coverage ask"));
    let cycles = maintain_to_completion(&db);
    assert!(cycles >= 1);
    assert!(
        db.persist_import_vector_tails().expect("coverage ask"),
        "the published index covers every committed vector"
    );
}

#[test]
fn new_committed_work_is_covered_again_after_one_more_maintenance_run_and_the_call_builds_nothing()
{
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open(&root.path().join("regrowth.db"));
    commit(&db, 0..IMPORTED);
    maintain_to_completion(&db);
    assert!(db.persist_import_vector_tails().expect("coverage ask"));

    commit(&db, IMPORTED..IMPORTED + 1);
    // The append may already be covered by the committed write buffer; the call
    // reports the verified coverage either way and builds nothing.
    let builds = Arc::new(Builds::default());
    let after_append = with_vector_build_observer(builds.clone(), || {
        db.persist_import_vector_tails().expect("coverage ask")
    });
    assert_eq!(
        builds.0.load(Ordering::SeqCst),
        0,
        "the coverage call builds nothing (reported {after_append})"
    );
    maintain_to_completion(&db);
    assert!(
        db.persist_import_vector_tails().expect("coverage ask"),
        "one more maintenance run covers the new commit"
    );
}

#[test]
fn the_consumer_loop_finishes_in_finitely_many_cycles_and_is_idle_once_covered() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open(&root.path().join("consumer.db"));
    commit(&db, 0..IMPORTED);
    let cycles = maintain_passage_vectors(&db);
    assert!(
        cycles >= 1,
        "an import with nothing published runs maintenance"
    );
    assert!(db.persist_import_vector_tails().expect("coverage ask"));
    assert_eq!(
        maintain_passage_vectors(&db),
        0,
        "once covered, the consumer loop runs no maintenance"
    );
    commit(&db, IMPORTED..IMPORTED + 8);
    let again = maintain_passage_vectors(&db);
    assert!(again <= MAX_CYCLES);
    assert!(db.persist_import_vector_tails().expect("coverage ask"));
}

/// Cleanup that reclaims a replaced vector must leave a healthy store: the
/// consumer's import stopped here with a corrupt-store refusal at its first
/// sealed file.
#[test]
fn retention_of_a_replaced_passage_keeps_maintenance_search_and_the_next_commit() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("replaced.db");
    let db = open_current_only(&path);
    commit(&db, 0..IMPORTED);
    maintain_to_completion(&db);
    replace(&db, 0..10, 100);
    assert_eq!(
        db.compact_currency_versions()
            .expect("cleanup reclaims the replaced vectors")
            .pruned_versions,
        10
    );
    maintain_to_completion(&db);
    let nearest = |db: &Database| {
        db.execute(
            "SELECT id FROM passages ORDER BY embedding <=> $query LIMIT 1",
            &params([(
                "query",
                Value::Vector(unit_vector(3 + 100, DIMENSIONS)),
            )]),
        )
        .expect("search after cleanup")
        .rows()[0][0]
            .clone()
    };
    assert_eq!(nearest(&db), Value::Int64(3));
    commit(&db, IMPORTED..IMPORTED + 1);
    db.close().expect("close the store");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("reopen after cleanup");
    assert_eq!(nearest(&db), Value::Int64(3));
}

#[test]
fn the_consumer_loop_covers_a_replaced_passage_after_retention() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_current_only(&root.path().join("consumer-replaced.db"));
    commit(&db, 0..IMPORTED);
    maintain_to_completion(&db);
    replace(&db, 0..10, 100);
    assert_eq!(
        db.compact_currency_versions()
            .expect("cleanup reclaims the replaced vectors")
            .pruned_versions,
        10
    );
    maintain_passage_vectors(&db);
    assert!(db.persist_import_vector_tails().expect("coverage ask"));
}

/// A version born and superseded after a build began can be reclaimed while
/// that build is unfinished. Publication then checks the build's change count
/// against the source, so it fails if cleanup left the count behind.
#[test]
fn retention_during_an_unfinished_build_keeps_its_change_count() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_current_only(&root.path().join("unfinished.db"));
    commit(&db, 0..IMPORTED);
    let index = contextdb_core::VectorIndexRef::new("passages", "embedding");
    let tuple = contextdb_core::VectorPartitionKey::from_values(&[]).unwrap();
    let unfinished = |db: &Database| {
        db.native_ivf_maintenance_state_for_test(&index, &tuple)
            .expect("read the persisted build")
            .build
            .is_some_and(|build| build.phase != contextdb_vector::ivf::storage::BuildPhase::Ready)
    };
    assert!(cycle(&db) > 0, "one cycle does not finish this build");
    assert!(unfinished(&db), "the first cycle leaves a build unfinished");
    replace(&db, 0..10, 100);
    replace(&db, 0..10, 200);
    // One finite pass may only advance its cursor; a few passes visit the row.
    let pruned: u64 = (0..8)
        .map(|_| {
            db.compact_currency_versions()
                .expect("cleanup during the build")
                .pruned_versions
        })
        .sum();
    assert_eq!(pruned, 10, "cleanup reclaims the versions the build never saw");
    assert!(unfinished(&db), "cleanup ran while the build was unfinished");
    maintain_to_completion(&db);
    let rows = db
        .execute(
            "SELECT id FROM passages ORDER BY embedding <=> $query LIMIT 1",
            &params([(
                "query",
                Value::Vector(unit_vector(3 + 200, DIMENSIONS)),
            )]),
        )
        .expect("search after the build");
    assert_eq!(rows.rows()[0][0], Value::Int64(3));
    assert!(db.persist_import_vector_tails().expect("coverage ask"));
}

/// An empty declared index still prices its fixed roots and controls. Without
/// them its build workspace takes the whole remaining budget and those required
/// roots are refused, which stopped the consumer's import at its first file.
#[test]
fn maintaining_an_empty_declared_index_is_not_refused_under_a_memory_limit() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = Database::open_with_options(
        &root.path().join("empty.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(256 * 1024 * 1024),
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the limited store");
    db.execute(
        &format!(
            "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    maintain_to_completion(&db);
}
