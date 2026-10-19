//! A historical index read costs the rows it returns plus one step per dead
//! (key, row) group it passes, never a row's history length.
//!
//! At an older snapshot, a row selects the one version visible there by an
//! authenticated seek, so how many versions that row has before or after the
//! snapshot adds no work, before and after a reopen. A (key, row) group that
//! held no version visible at the snapshot is stepped over once, whatever its
//! history.
use crate::resident_history_support::{empty, exec, open};
use contextdb_core::Value;
use contextdb_engine::direct_file_reader::test_seams::{
    HistoricalReadWorkCounters, ReadWorkCounters, ReadWorkObservationScope, ReadWorkPhase,
};

/// What one read did: its rows, the row decode and proof counters, and the
/// historical storage visits.
fn observed_read(
    db: &contextdb_engine::Database,
    sql: &str,
    snapshot: contextdb_core::SnapshotId,
) -> (
    Vec<Vec<Value>>,
    ReadWorkCounters,
    HistoricalReadWorkCounters,
) {
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let rows = db
        .execute_at_snapshot(sql, &empty(), snapshot)
        .unwrap()
        .rows;
    (rows, work.counters(), work.historical_counters())
}

/// One row with `versions` versions before the snapshot and as many after it.
/// Returns the work of `LIMIT 1` at the snapshot, read in the writing
/// lifetime or after a reopen.
fn long_row_read(
    versions: i64,
    reopen_before_read: bool,
) -> (ReadWorkCounters, HistoricalReadWorkCounters) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("long-row.db");
    let mut db = open(&path, None);
    exec(
        &db,
        "CREATE TABLE history (id INT PRIMARY KEY, revision INT, body TEXT) HISTORY ALL",
    );
    exec(&db, "INSERT INTO history VALUES (0, 0, 'body')");
    for revision in 1..=versions {
        exec(
            &db,
            &format!("UPDATE history SET revision = {revision} WHERE id = 0"),
        );
    }
    let snapshot = db.snapshot();
    for revision in versions + 1..=2 * versions {
        exec(
            &db,
            &format!("UPDATE history SET revision = {revision} WHERE id = 0"),
        );
    }
    if reopen_before_read {
        db.close().unwrap();
        drop(db);
        db = open(&path, None);
    }
    let (rows, work, historical) = observed_read(
        &db,
        "SELECT id, revision FROM history ORDER BY id LIMIT 1",
        snapshot,
    );
    assert_eq!(rows, vec![vec![Value::Int64(0), Value::Int64(versions)]]);
    db.close().unwrap();
    (work, historical)
}

#[test]
fn a_long_row_history_adds_no_work_to_a_historical_read() {
    for reopen_before_read in [false, true] {
        let (short_work, short) = long_row_read(16, reopen_before_read);
        let (long_work, long) = long_row_read(128, reopen_before_read);
        assert_eq!(
            (short_work.decoded_rows, long_work.decoded_rows),
            (1, 1),
            "reopened={reopen_before_read}: the read decodes the one version it returns"
        );
        assert_eq!(
            (
                short.manifest_members_visited,
                short.historical_postings_fetched
            ),
            (
                long.manifest_members_visited,
                long.historical_postings_fetched
            ),
            "reopened={reopen_before_read}: manifest members and postings visited do not grow \
             with the row's history: 16 versions {short:?}, 128 versions {long:?}"
        );
        assert!(
            long_work.proof_nodes_read <= short_work.proof_nodes_read + 32,
            "reopened={reopen_before_read}: proof work grows at most with the tree depth: \
             16 versions {short_work:?}, 128 versions {long_work:?}"
        );
    }
}

const DEAD_ROWS: i64 = 16;

/// `DEAD_ROWS` rows each held key 'a' for `versions` versions and then moved
/// to 'z' before the snapshot; one more row holds 'a' at the snapshot. Returns
/// the work of the equality read at the snapshot after a reopen.
fn dead_prefix_read(versions: i64) -> (ReadWorkCounters, HistoricalReadWorkCounters) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dead-prefix.db");
    let db = open(&path, None);
    exec(
        &db,
        "CREATE TABLE tagged (id INT PRIMARY KEY, tag TEXT, revision INT) HISTORY ALL",
    );
    exec(&db, "CREATE INDEX tagged_by_tag ON tagged (tag)");
    for id in 0..DEAD_ROWS {
        exec(&db, &format!("INSERT INTO tagged VALUES ({id}, 'a', 0)"));
        for revision in 1..versions {
            exec(
                &db,
                &format!("UPDATE tagged SET revision = {revision} WHERE id = {id}"),
            );
        }
        exec(&db, &format!("UPDATE tagged SET tag = 'z' WHERE id = {id}"));
    }
    exec(
        &db,
        &format!("INSERT INTO tagged VALUES ({DEAD_ROWS}, 'a', 0)"),
    );
    let snapshot = db.snapshot();
    exec(
        &db,
        &format!("UPDATE tagged SET revision = 9 WHERE id = {DEAD_ROWS}"),
    );
    db.close().unwrap();
    drop(db);
    let db = open(&path, None);
    let (rows, work, historical) =
        observed_read(&db, "SELECT id FROM tagged WHERE tag = 'a'", snapshot);
    assert_eq!(rows, vec![vec![Value::Int64(DEAD_ROWS)]]);
    db.close().unwrap();
    (work, historical)
}

#[test]
fn dead_groups_are_stepped_once_each_whatever_their_history() {
    let (one_work, one) = dead_prefix_read(1);
    let (six_work, six) = dead_prefix_read(6);
    assert_eq!(
        (one_work.decoded_rows, six_work.decoded_rows),
        (1, 1),
        "only the returned row is decoded; a dead group is decided from its posting"
    );
    assert_eq!(
        (one.dead_groups_stepped, six.dead_groups_stepped),
        (DEAD_ROWS as u64, DEAD_ROWS as u64),
        "each dead (key, row) group is stepped over exactly once"
    );
    assert_eq!(
        (
            one.historical_postings_fetched,
            one.manifest_members_visited
        ),
        (
            six.historical_postings_fetched,
            six.manifest_members_visited
        ),
        "a dead group costs the same however many versions it held: 1 version {one:?}, \
         6 versions {six:?}"
    );
}
