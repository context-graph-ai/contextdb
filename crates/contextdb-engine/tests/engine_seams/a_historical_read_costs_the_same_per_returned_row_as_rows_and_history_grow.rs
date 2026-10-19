//! A read at an older snapshot costs the rows it returns, whether or not the
//! store was reopened since that snapshot was taken.
//!
//! A historical read of a `HISTORY ALL` table decodes the same number of row
//! versions per row it returns at any table size and history length, before
//! and after a reopen. Re-walking the table for every returned row made each
//! such read cost rows × table size.
use crate::resident_history_support::{empty, exec, open};
use contextdb_core::Value;
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};

/// Rows written at revision 0 and snapshotted, then each row rewritten
/// `later_versions` times in a later process lifetime. Returns the versions
/// the read at the snapshot decoded, read either in that same lifetime or
/// after one more reopen.
fn versions_decoded_by_a_snapshot_read(
    rows: i64,
    later_versions: i64,
    reopen_before_read: bool,
) -> u64 {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("history.db");
    let db = open(&path, None);
    exec(
        &db,
        "CREATE TABLE versions (id INT PRIMARY KEY, revision INT, body TEXT) HISTORY ALL",
    );
    for id in 0..rows {
        db.execute(
            "INSERT INTO versions VALUES ($id, 0, $body)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("body".into(), Value::Text("v".repeat(1024))),
            ]),
        )
        .unwrap();
    }
    let old = db.snapshot();
    db.close().unwrap();
    drop(db);

    let mut db = open(&path, None);
    for revision in 1..=later_versions {
        for id in 0..rows {
            exec(
                &db,
                &format!("UPDATE versions SET revision = {revision} WHERE id = {id}"),
            );
        }
    }
    if reopen_before_read {
        db.close().unwrap();
        drop(db);
        db = open(&path, None);
    }
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let read = db
        .execute_at_snapshot(
            "SELECT id, revision FROM versions ORDER BY id",
            &empty(),
            old,
        )
        .unwrap();
    let decoded = work.counters().decoded_rows;
    drop(work);
    assert_eq!(
        read.rows,
        (0..rows)
            .map(|id| vec![Value::Int64(id), Value::Int64(0)])
            .collect::<Vec<_>>(),
        "the snapshot answers what it held"
    );
    db.close().unwrap();
    decoded
}

#[test]
fn a_historical_read_costs_the_same_per_returned_row_as_rows_and_history_grow() {
    let mut per_returned_row = Vec::new();
    for rows in [16, 64] {
        for later_versions in [1, 8] {
            for reopen_before_read in [false, true] {
                let decoded =
                    versions_decoded_by_a_snapshot_read(rows, later_versions, reopen_before_read);
                per_returned_row.push((rows, later_versions, reopen_before_read, decoded));
            }
        }
    }
    let ratio = |(rows, _, _, decoded): &(i64, i64, bool, u64)| *decoded as f64 / *rows as f64;
    let first = ratio(&per_returned_row[0]);
    assert!(
        per_returned_row.iter().all(|case| ratio(case) == first),
        "(rows, later versions per row, reopened before the read, versions decoded): versions \
         decoded per returned row stay flat as rows and history grow, before and after reopen: \
         {per_returned_row:?}"
    );
}
