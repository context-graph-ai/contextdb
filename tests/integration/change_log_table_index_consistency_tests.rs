//! `change_log_table_index` (the per-table shadow a scoped version-cleanup
//! / retention pass reads instead of scanning the whole change-log `Vec` --
//! see `composite_store::ChangeLogTableIndex`) must never drift from
//! `change_log` itself: for any table, the index's own `(lsn, row_id)`
//! coordinates for that table's `RowInsert`/`RowDelete` entries must equal
//! what a fresh scan of the real change log (filtered to that table) would
//! find, at every point the two are maintained -- after ordinary appends,
//! after a reopen from disk (the index is REBUILT from the loaded log, not
//! carried over), and after a scoped cleanup pass has physically removed
//! entries (the index's own removal must track the Vec's own removal
//! exactly, not merely approximately).
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use std::collections::HashMap;

fn advertise(db: &Database, key: &str, capability_id: &str, advertised_at: i64) {
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text(key.to_string()));
    params.insert("node_id".to_string(), Value::Text(key.to_string()));
    params.insert(
        "capability_id".to_string(),
        Value::Text(capability_id.to_string()),
    );
    params.insert("tags".to_string(), Value::Json(serde_json::json!([])));
    params.insert("advertised_at".to_string(), Value::Timestamp(advertised_at));
    db.execute(
        "INSERT INTO work_capabilities (capability_key, node_id, capability_id, tags, detail, \
         advertised_at) VALUES ($k, $node_id, $capability_id, $tags, NULL, $advertised_at) \
         ON CONFLICT (capability_key) DO UPDATE SET \
         advertised_at = $advertised_at, node_id = $node_id, capability_id = $capability_id",
        &params,
    )
    .expect("advertise/re-advertise a capability");
}

/// The ground truth this test checks the index against: every
/// `RowInsert`/`RowDelete` entry in the REAL change log naming `table`,
/// reduced to the same `(lsn, row_id)` shape and sort order
/// `__change_log_table_index_for_test` uses -- built from the public
/// `change_log_since` surface, never from the index itself.
fn change_log_coords_for_table(
    db: &Database,
    table: &str,
) -> Vec<(contextdb_core::Lsn, contextdb_core::RowId)> {
    let mut coords: Vec<(contextdb_core::Lsn, contextdb_core::RowId)> = db
        .change_log_since(contextdb_core::Lsn(0))
        .into_iter()
        .filter_map(|entry| match entry {
            ChangeLogEntry::RowInsert {
                table: entry_table,
                row_id,
                lsn,
            }
            | ChangeLogEntry::RowDelete {
                table: entry_table,
                row_id,
                lsn,
                ..
            } if entry_table == table => Some((lsn, row_id)),
            _ => None,
        })
        .collect();
    coords.sort();
    coords
}

fn assert_index_matches_change_log(db: &Database, table: &str, when: &str) {
    assert_eq!(
        db.__change_log_table_index_for_test(table),
        change_log_coords_for_table(db, table),
        "change_log_table_index[{table}] must equal the real change log's own {table} entries \
         {when} -- any difference is the index drifting from its ground truth"
    );
}

/// Baseline: ordinary appends (fresh inserts, then superseding upserts) keep
/// the index and the real log in lockstep the whole time, on an in-memory
/// database (no reopen involved yet).
#[test]
fn the_index_matches_the_change_log_after_ordinary_appends() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    assert_index_matches_change_log(&db, "work_capabilities", "before any writes");

    for i in 0..3 {
        advertise(&db, "node-a", &format!("cap-a{i}"), 1_700_000_000_000 + i);
        assert_index_matches_change_log(&db, "work_capabilities", "after each incremental append");
    }
    advertise(&db, "node-b", "cap-b0", 1_700_000_000_100);
    assert_index_matches_change_log(&db, "work_capabilities", "after a second logical row");
}

/// Reopen: the index is REBUILT from the persisted change log at
/// `Database::open` (`build_change_log_aux_indexes`), not carried over from
/// the closed handle -- so this proves the rebuild reconstructs exactly
/// what a live handle would have shown, not merely something plausible.
#[test]
fn the_index_survives_a_reopen_from_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("reopen.db");
    let before_close = {
        let db = Database::open(&path).expect("open");
        install_work_ledger_schema(&db).expect("install");
        for i in 0..4 {
            advertise(&db, "node-a", &format!("cap-{i}"), 1_700_000_000_000 + i);
        }
        advertise(&db, "node-b", "cap-b0", 1_700_000_000_200);
        assert_index_matches_change_log(&db, "work_capabilities", "before close");
        db.__change_log_table_index_for_test("work_capabilities")
    };

    let db2 = Database::open(&path).expect("reopen");
    assert_eq!(
        db2.__change_log_table_index_for_test("work_capabilities"),
        before_close,
        "a reopen must rebuild the SAME per-table coordinates the closed handle held"
    );
    assert_index_matches_change_log(
        &db2,
        "work_capabilities",
        "immediately after reopen, before any new write",
    );

    // One more write after reopen, to prove the rebuilt index keeps
    // tracking correctly going forward, not just at the instant of load.
    advertise(&db2, "node-a", "cap-post-reopen", 1_700_000_000_300);
    assert_index_matches_change_log(&db2, "work_capabilities", "after a write following reopen");
}

/// A scoped cleanup pass physically removes entries from the change log
/// (`remove_pruned_change_log_entries`) -- the index's own removal
/// (`remove_pruned_from_table_index`) must track that exactly, not merely
/// shrink by roughly the right amount. File-backed, deliberately: an
/// in-memory database has no persistence layer, so the scoped removal path
/// this test exercises would never run its persisted half at all.
#[test]
fn the_index_matches_the_change_log_after_a_scoped_cleanup_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("scoped_removal.db");
    let db = Database::open(&path).expect("open");
    install_work_ledger_schema(&db).expect("install");
    for i in 0..6 {
        advertise(&db, "node-a", &format!("cap-{i}"), 1_700_000_000_000 + i);
    }
    advertise(&db, "node-b", "cap-b0", 1_700_000_000_500);
    assert_index_matches_change_log(&db, "work_capabilities", "before cleanup");

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup must produce prunable versions: {report:?}"
    );
    assert_index_matches_change_log(&db, "work_capabilities", "immediately after cleanup");

    // A further write, then a second cleanup cycle -- proves the index
    // does not merely happen to be correct once, but stays correct across
    // repeated append/removal cycles on the SAME table.
    advertise(&db, "node-a", "cap-after-cleanup", 1_700_000_000_600);
    advertise(&db, "node-a", "cap-after-cleanup-2", 1_700_000_000_700);
    let report2 = db
        .compact_currency_versions()
        .expect("second compaction must succeed");
    assert!(
        report2.pruned_versions > 0,
        "the second round of churn must also produce prunable versions: {report2:?}"
    );
    assert_index_matches_change_log(&db, "work_capabilities", "after a second cleanup cycle");
}
