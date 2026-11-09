//! `Database::persist_import_vector_tails` answers one question for a bulk
//! import: is every committed passage vector covered by the published
//! clustered index (its committed write buffer included)? It builds nothing
//! and chases no later commit; the existing maintenance cycle advances the
//! index and the caller alternates the two. Every transition here is driven by
//! an explicit call and read from a returned value or a report counter.

use super::support::{empty, ivf, params, unit_vector};
use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};

const DIMENSIONS: usize = 16;
const IMPORTED: i64 = 40;
/// A finite cap on maintenance cycles, so a defect fails instead of hanging.
const MAX_CYCLES: usize = 256;

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

/// `open_current_only` with the column's declared search mode.
fn open_current_only_as(path: &std::path::Path, search_mode: &str) -> Database {
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
            "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE {search_mode} {}) HISTORY CURRENT ONLY SYNC OFF",
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

/// Cleanup that reclaims a replaced vector must leave a healthy store: the
/// consumer's import stopped here with a corrupt-store refusal at its first
/// sealed file.
#[test]
fn retention_of_a_replaced_passage_keeps_maintenance_search_and_the_next_commit() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("replaced.db");
    let db = open_current_only_as(&path, "AUTO");
    commit(&db, 0..IMPORTED);
    maintain_to_completion(&db);
    replace(&db, 0..10, 100);
    // A cleanup pass prunes only versions at or below the LSN frontier its
    // cursor recorded when it last wrapped, so versions written after the
    // maintenance cycle's own pass wait for the next wrap. One wrap visits each
    // row group (3 work units: the group, its head record and body probe) and
    // each stored version (1 unit), within the pass budget of 128 units
    // (compaction_and_ddl_rendering.rs), and a pass that resumes a group pays
    // its head again. Passes run until a whole wrap prunes nothing more.
    let wrap_units = 3 * IMPORTED as usize + (IMPORTED as usize + 10);
    let passes_per_wrap = wrap_units.div_ceil(128 - 2) + 1;
    let (mut pruned, mut quiet, mut passes) = (0_u64, 0_usize, 0_usize);
    while pruned == 0 || quiet < passes_per_wrap {
        assert!(
            passes < 3 * passes_per_wrap,
            "cleanup settles within three cursor wraps: {passes} passes, {pruned} pruned"
        );
        let now = db
            .compact_currency_versions()
            .expect("cleanup reclaims the replaced vectors")
            .pruned_versions;
        passes += 1;
        pruned += now;
        quiet = if now == 0 { quiet + 1 } else { 0 };
    }
    assert_eq!(pruned, 10, "cleanup reclaims the replaced vectors");
    maintain_to_completion(&db);
    let nearest = |db: &Database| {
        db.execute(
            "SELECT id FROM passages ORDER BY embedding <=> $query LIMIT 1",
            &params([("query", Value::Vector(unit_vector(3 + 100, DIMENSIONS)))]),
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

/// An empty declared index still prices its fixed roots and controls. Without
/// them its build workspace takes the whole remaining budget and those required
/// roots are refused, which stopped the consumer's import at its first file.
#[test]
fn maintaining_an_empty_declared_index_is_not_refused_under_a_memory_limit() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = Database::open_with_options(
        root.path().join("empty.db"),
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
