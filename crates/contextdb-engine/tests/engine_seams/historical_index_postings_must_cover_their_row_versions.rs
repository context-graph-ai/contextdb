//! A historical index answers a read at an older snapshot from its postings
//! alone, so its postings must cover exactly what the row versions cover.
//!
//! Every writer that changes postings or row versions folds both into one
//! commitment per index and refuses to commit when they disagree. A writer
//! that skips one posting change, in any family that maintains postings,
//! fails its commit as corruption and leaves the store as it was. A reader
//! refuses a commitment that does not authenticate.
use crate::resident_history_support::{empty, exec, open};
use contextdb_core::{Error, Lsn, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    damage_historical_index_commitment_for_test, historical_index_inventory_for_test,
    refuse_next_commitment_admission_for_test, skip_next_index_retirement_for_test,
    skip_next_postings_fold_for_test, take_commitment_admitted_bytes_for_test,
};
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use std::path::Path;

fn refused_as_corruption(result: contextdb_core::Result<impl std::fmt::Debug>, family: &str) {
    match result {
        Err(Error::StoreCorrupted { .. }) => {}
        other => panic!("{family}: a skipped posting change must refuse the commit, got {other:?}"),
    }
}

fn tags(db: &Database) -> Vec<Vec<Value>> {
    exec(db, "SELECT id, tag FROM tagged ORDER BY id")
}

fn reopened(path: &Path, db: Database) -> Database {
    drop(db.close());
    drop(db);
    open(path, None)
}

fn indexed(path: &Path, history: &str) -> Database {
    let db = open(path, None);
    exec(
        &db,
        &format!(
            "CREATE TABLE tagged (id INT PRIMARY KEY, tag TEXT, revision INT) {history} SYNC OFF"
        ),
    );
    exec(&db, "CREATE INDEX tagged_by_tag ON tagged (tag)");
    exec(&db, "INSERT INTO tagged VALUES (1, 'a', 0), (2, 'b', 0)");
    db
}

#[test]
fn a_commit_that_skips_a_posting_change_is_refused_and_leaves_the_store_as_it_was() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commit.db");
    let db = indexed(&path, "HISTORY ALL");
    let before = tags(&db);
    skip_next_postings_fold_for_test();
    refused_as_corruption(
        db.execute("UPDATE tagged SET tag = 'z' WHERE id = 1", &empty()),
        "commit",
    );
    let db = reopened(&path, db);
    assert_eq!(tags(&db), before, "the refused commit wrote nothing");
    exec(&db, "UPDATE tagged SET tag = 'z' WHERE id = 1");
    assert_eq!(
        exec(&db, "SELECT id FROM tagged WHERE tag = 'z'"),
        vec![vec![Value::Int64(1)]]
    );
    db.close().unwrap();
}

#[test]
fn an_index_build_that_skips_a_posting_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rebuild.db");
    let db = open(&path, None);
    exec(
        &db,
        "CREATE TABLE tagged (id INT PRIMARY KEY, tag TEXT, revision INT) HISTORY ALL SYNC OFF",
    );
    exec(&db, "INSERT INTO tagged VALUES (1, 'a', 0), (2, 'b', 0)");
    exec(&db, "UPDATE tagged SET tag = 'c' WHERE id = 2");
    skip_next_postings_fold_for_test();
    refused_as_corruption(
        db.execute("CREATE INDEX tagged_by_tag ON tagged (tag)", &empty()),
        "rebuild",
    );
    let db = reopened(&path, db);
    exec(&db, "CREATE INDEX tagged_by_tag ON tagged (tag)");
    assert_eq!(
        exec(&db, "SELECT id FROM tagged WHERE tag = 'c'"),
        vec![vec![Value::Int64(2)]]
    );
    db.close().unwrap();
}

#[test]
fn a_purge_that_skips_a_posting_is_refused_and_erases_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge.db");
    let db = indexed(&path, "HISTORY ALL");
    exec(&db, "UPDATE tagged SET tag = 'c' WHERE id = 2");
    let before = tags(&db);
    skip_next_postings_fold_for_test();
    refused_as_corruption(
        db.execute("PURGE FROM tagged WHERE id = 1", &empty()),
        "purge",
    );
    let db = reopened(&path, db);
    assert_eq!(tags(&db), before, "the refused purge erased nothing");
    db.close().unwrap();
}

#[test]
fn a_retention_split_that_skips_a_posting_is_refused() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retention.db");
    let db = indexed(&path, "HISTORY CURRENT ONLY");
    // Revisions that keep the key extend one posting interval; reclaiming the
    // superseded versions splits or trims it.
    for revision in 1..=3 {
        exec(
            &db,
            &format!("UPDATE tagged SET revision = {revision} WHERE id = 1"),
        );
    }
    exec(&db, "UPDATE tagged SET tag = 'y' WHERE id = 1");
    skip_next_postings_fold_for_test();
    let mut refused = false;
    for _ in 0..4 {
        match db.run_maintenance_cycle() {
            Err(Error::StoreCorrupted { .. }) => {
                refused = true;
                break;
            }
            Err(other) => panic!("retention: unexpected refusal {other:?}"),
            Ok(_) => {}
        }
    }
    assert!(
        refused,
        "retention: reclaiming versions past a skipped posting change must refuse"
    );
    let db = reopened(&path, db);
    assert_eq!(
        exec(&db, "SELECT id FROM tagged WHERE tag = 'y'"),
        vec![vec![Value::Int64(1)]]
    );
    db.close().unwrap();
}

#[test]
fn a_dropped_index_takes_its_commitment_and_a_recreated_one_starts_fresh() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop.db");
    let db = indexed(&path, "HISTORY ALL");
    let early = db.snapshot();
    exec(&db, "UPDATE tagged SET tag = 'c' WHERE id = 2");
    exec(&db, "DROP INDEX tagged_by_tag ON tagged");
    exec(&db, "UPDATE tagged SET tag = 'd' WHERE id = 1");
    exec(&db, "CREATE INDEX tagged_by_tag ON tagged (tag)");
    let db = reopened(&path, db);
    assert_eq!(
        db.execute_at_snapshot("SELECT id FROM tagged WHERE tag = 'b'", &empty(), early)
            .unwrap()
            .rows,
        vec![vec![Value::Int64(2)]],
        "the recreated index answers the older snapshot from fresh postings"
    );
    assert_eq!(
        exec(&db, "SELECT id FROM tagged WHERE tag = 'd'"),
        vec![vec![Value::Int64(1)]]
    );
    db.close().unwrap();
}

#[test]
fn a_reader_refuses_a_commitment_that_does_not_authenticate() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reader.db");
    let db = indexed(&path, "HISTORY ALL");
    let early = db.snapshot();
    exec(&db, "UPDATE tagged SET tag = 'c' WHERE id = 2");
    assert_eq!(
        db.execute_at_snapshot("SELECT id FROM tagged WHERE tag = 'b'", &empty(), early)
            .unwrap()
            .rows,
        vec![vec![Value::Int64(2)]]
    );
    db.close().unwrap();
    drop(db);
    damage_historical_index_commitment_for_test(&path, "tagged", "tagged_by_tag").unwrap();
    let db = open(&path, None);
    refused_as_corruption(
        db.execute_at_snapshot("SELECT id FROM tagged WHERE tag = 'b'", &empty(), early),
        "reader",
    );
    db.close().unwrap();
}

#[test]
fn a_received_change_that_skips_a_posting_is_refused_and_applies_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let source = indexed(&directory.path().join("source.db"), "HISTORY ALL");
    let path = directory.path().join("receiver.db");
    let receiver = open(&path, None);
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    receiver
        .apply_changes(source.changes_since(Lsn(0)).unwrap(), &policies)
        .unwrap();
    let before = tags(&receiver);
    let since = source.current_lsn();
    exec(&source, "UPDATE tagged SET tag = 'z' WHERE id = 1");
    skip_next_postings_fold_for_test();
    refused_as_corruption(
        receiver.apply_changes(source.changes_since(since).unwrap(), &policies),
        "sync replay",
    );
    let receiver = reopened(&path, receiver);
    assert_eq!(
        tags(&receiver),
        before,
        "the refused replay applied nothing"
    );
    receiver.close().unwrap();
    source.close().unwrap();
}

/// Bytes the commitment of one row insert admitted into a table carrying
/// `indexes` declared indexes.
fn admitted_for_an_insert_with(indexes: usize) -> usize {
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory.path().join("admitted.db"), None);
    let columns = (0..indexes)
        .map(|column| format!(", c{column} INT"))
        .collect::<String>();
    exec(
        &db,
        &format!("CREATE TABLE wide (id INT PRIMARY KEY{columns}) HISTORY ALL SYNC OFF"),
    );
    for column in 0..indexes {
        exec(
            &db,
            &format!("CREATE INDEX wide_c{column} ON wide (c{column})"),
        );
    }
    let values = (0..indexes).map(|_| ", 1").collect::<String>();
    take_commitment_admitted_bytes_for_test();
    exec(&db, &format!("INSERT INTO wide VALUES (1{values})"));
    let admitted = take_commitment_admitted_bytes_for_test();
    db.close().unwrap();
    admitted
}

#[test]
fn a_commit_admits_its_commitment_memory_for_every_index_it_touches() {
    let one = admitted_for_an_insert_with(1);
    let eight = admitted_for_an_insert_with(8);
    assert!(
        eight >= one + 7 * 2 * 2048,
        "each touched index's two 2 KiB states are admitted to the writer budget: \
         1 index {one} B, 8 indexes {eight} B"
    );
}

#[test]
fn a_refused_commitment_admission_rolls_the_write_back() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("refused.db");
    let db = indexed(&path, "HISTORY ALL");
    let before = tags(&db);
    refuse_next_commitment_admission_for_test();
    match db.execute("UPDATE tagged SET tag = 'z' WHERE id = 1", &empty()) {
        Err(Error::MemoryBudgetExceeded { subsystem, .. }) => {
            assert_eq!(subsystem, "historical_index_commitment")
        }
        other => panic!("the refused admission is a typed memory refusal, got {other:?}"),
    }
    let db = reopened(&path, db);
    assert_eq!(tags(&db), before, "the refused write published nothing");
    exec(&db, "UPDATE tagged SET tag = 'z' WHERE id = 1");
    assert_eq!(
        exec(&db, "SELECT id FROM tagged WHERE tag = 'z'"),
        vec![vec![Value::Int64(1)]]
    );
    db.close().unwrap();
}

#[test]
fn a_dropped_index_leaves_no_postings_or_commitment_behind() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dropped.db");
    let db = indexed(&path, "HISTORY ALL");
    exec(&db, "UPDATE tagged SET tag = 'c' WHERE id = 2");
    db.close().unwrap();
    drop(db);
    assert_eq!(
        historical_index_inventory_for_test(&path, "tagged", "tagged_by_tag").unwrap(),
        (true, true, true),
        "a declared index holds its postings and its commitment"
    );
    let db = open(&path, None);
    exec(&db, "DROP INDEX tagged_by_tag ON tagged");
    db.close().unwrap();
    drop(db);
    assert_eq!(
        historical_index_inventory_for_test(&path, "tagged", "tagged_by_tag").unwrap(),
        (false, false, false),
        "dropping the index retires its historical and current postings and its states"
    );
}

#[test]
fn purging_rows_after_dropping_their_index_leaves_no_indexed_copy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-then-purge.db");
    let db = indexed(&path, "HISTORY ALL");
    exec(&db, "UPDATE tagged SET tag = 'secret' WHERE id = 1");
    exec(&db, "DROP INDEX tagged_by_tag ON tagged");
    exec(&db, "PURGE FROM tagged WHERE id = 1");
    assert_eq!(
        tags(&db),
        vec![vec![Value::Int64(2), Value::Text("b".into())]]
    );
    db.close().unwrap();
    drop(db);
    assert_eq!(
        historical_index_inventory_for_test(&path, "tagged", "tagged_by_tag").unwrap(),
        (false, false, false),
        "no posting of the dropped index survives to carry the purged row's key"
    );
}

#[test]
fn a_retirement_that_leaves_postings_behind_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("skipped-retirement.db");
    let db = indexed(&path, "HISTORY ALL");
    skip_next_index_retirement_for_test();
    refused_as_corruption(
        db.execute("DROP INDEX tagged_by_tag ON tagged", &empty()),
        "drop",
    );
    let db = reopened(&path, db);
    assert_eq!(
        exec(&db, "SELECT id FROM tagged WHERE tag = 'a'"),
        vec![vec![Value::Int64(1)]],
        "the refused drop left the index declared and serving"
    );
    exec(&db, "DROP INDEX tagged_by_tag ON tagged");
    db.close().unwrap();
    drop(db);
    assert_eq!(
        historical_index_inventory_for_test(&path, "tagged", "tagged_by_tag").unwrap(),
        (false, false, false)
    );
}

const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;
const START_MILLIS: u64 = 1_700_000_000_000;

fn retained(path: &Path) -> Database {
    let db = open(path, None);
    exec(
        &db,
        "CREATE TABLE expiring (id INT PRIMARY KEY, tag TEXT) RETAIN 1 DAYS SYNC OFF",
    );
    exec(&db, "CREATE INDEX expiring_by_tag ON expiring (tag)");
    exec(&db, "INSERT INTO expiring VALUES (1, 'a'), (2, 'b')");
    db
}

#[test]
fn retention_erases_an_expired_live_row_with_its_postings_and_current_index_entry() {
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(START_MILLIS));
    let reader = clock.clone();
    let _clock =
        Wallclock::test_clock_guard(move || reader.load(std::sync::atomic::Ordering::SeqCst));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("expiring.db");
    // Retention reclaims these expired rows through the cycle's ordinary
    // path, which closes their postings before it removes the versions. The
    // erase of a version that is still live when it is reclaimed is proven by
    // a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics_as_the_store_grows
    // and by the posting-prune unit tests; this guards the outcome either way.
    let db = reopened(&path, retained(&path));
    clock.store(
        START_MILLIS + 2 * DAY_MILLIS,
        std::sync::atomic::Ordering::SeqCst,
    );
    exec(&db, "INSERT INTO expiring VALUES (3, 'a')");
    let report = db
        .run_maintenance_cycle()
        .expect("retention reclaims expired live rows");
    assert_eq!(
        report.pruning.pruned_rows, 2,
        "rows 1 and 2 expired: {report:?}"
    );
    let db = reopened(&path, db);
    assert_eq!(
        exec(&db, "SELECT id FROM expiring WHERE tag = 'a'"),
        vec![vec![Value::Int64(3)]],
        "the index answers only the surviving row"
    );
    assert!(exec(&db, "SELECT id FROM expiring WHERE tag = 'b'").is_empty());
    // A current-index entry left behind would refuse the reused key.
    exec(&db, "INSERT INTO expiring VALUES (1, 'b')");
    assert_eq!(
        exec(&db, "SELECT id FROM expiring WHERE tag = 'b'"),
        vec![vec![Value::Int64(1)]]
    );
    db.close().unwrap();
}

#[test]
fn retention_never_hides_a_version_a_pinned_snapshot_still_sees() {
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(START_MILLIS));
    let reader = clock.clone();
    let _clock =
        Wallclock::test_clock_guard(move || reader.load(std::sync::atomic::Ordering::SeqCst));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pinned.db");
    let db = reopened(&path, retained(&path));
    let early = db.snapshot();
    let pin = db.pin_snapshot(early).unwrap();
    clock.store(
        START_MILLIS + 2 * DAY_MILLIS,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deferred = db
        .run_maintenance_cycle()
        .expect("retention defers what a pinned reader sees");
    assert_eq!(deferred.pruning.pruned_rows, 0, "{deferred:?}");
    assert_eq!(
        db.execute_at_snapshot("SELECT id FROM expiring WHERE tag = 'a'", &empty(), early)
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)]],
        "the pinned snapshot still finds the expired row through its index"
    );
    drop(pin);
    let pruned = db
        .run_maintenance_cycle()
        .expect("retention reclaims once the reader is gone");
    assert_eq!(pruned.pruning.pruned_rows, 2, "{pruned:?}");
    assert!(exec(&db, "SELECT id FROM expiring WHERE tag = 'a'").is_empty());
    db.close().unwrap();
}
