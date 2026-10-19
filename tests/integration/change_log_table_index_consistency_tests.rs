//! Row history remains selectable by table after scoped cleanup and reopen.
//! In-memory append controls also keep their resident lookup consistent.

use contextdb_core::Value;
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use contextdb_engine::{Database, MaintenancePolicy};
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
        .unwrap()
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

type Coordinate = (contextdb_core::Lsn, contextdb_core::RowId);

fn advertise_coordinate(
    db: &Database,
    key: &str,
    capability: &str,
    at: i64,
    replaces: bool,
) -> Vec<Coordinate> {
    let before = db.current_lsn();
    advertise(db, key, capability, at);
    let entries = db
        .change_log_since(before)
        .expect("read this committed advertisement");
    let coordinates = entries
        .into_iter()
        .filter_map(|entry| match entry {
            ChangeLogEntry::RowInsert { table, row_id, lsn }
            | ChangeLogEntry::RowDelete {
                table, row_id, lsn, ..
            } if table == "work_capabilities" => Some((lsn, row_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        coordinates.len(),
        1 + usize::from(replaces),
        "an advertisement publishes its insertion and, when replacing, its prior-version deletion"
    );
    coordinates
}

fn assert_durable_history(db: &Database, table: &str, expected: &[Coordinate], when: &str) {
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(
        change_log_coords_for_table(db, table),
        expected,
        "public durable history returns every exact {table} coordinate {when}"
    );
    assert!(
        change_log_coords_for_table(db, "missing_history_table").is_empty(),
        "selecting a missing table must not leak another table's history"
    );
}

fn unrelated_history(db: &Database) -> Vec<Coordinate> {
    db.execute(
        "CREATE TABLE unrelated_history (id INTEGER PRIMARY KEY) HISTORY ALL",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO unrelated_history VALUES (999)",
        &HashMap::new(),
    )
    .unwrap();
    let expected = change_log_coords_for_table(db, "unrelated_history");
    assert_eq!(
        expected.len(),
        1,
        "the unrelated table has a positive history control"
    );
    expected
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

/// Reopening preserves exact table-selected history and accepts a later append.
#[test]
fn the_index_survives_a_reopen_from_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("reopen.db");
    let (mut expected, unrelated) = {
        let db = Database::open(&path).expect("open");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        install_work_ledger_schema(&db).expect("install");
        let unrelated = unrelated_history(&db);
        let mut expected = Vec::new();
        for i in 0..4 {
            expected.extend(advertise_coordinate(
                &db,
                "node-a",
                &format!("cap-{i}"),
                1_700_000_000_000 + i,
                i > 0,
            ));
        }
        expected.extend(advertise_coordinate(
            &db,
            "node-b",
            "cap-b0",
            1_700_000_000_200,
            false,
        ));
        assert_durable_history(&db, "work_capabilities", &expected, "before close");
        db.close().unwrap();
        (expected, unrelated)
    };
    let db = Database::open(&path).expect("reopen");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_durable_history(
        &db,
        "work_capabilities",
        &expected,
        "immediately after reopen before another write",
    );
    assert_durable_history(
        &db,
        "unrelated_history",
        &unrelated,
        "after reopening the sibling table",
    );
    expected.extend(advertise_coordinate(
        &db,
        "node-a",
        "cap-post-reopen",
        1_700_000_000_300,
        true,
    ));
    assert_durable_history(
        &db,
        "work_capabilities",
        &expected,
        "after a write following reopen",
    );
    assert_durable_history(
        &db,
        "unrelated_history",
        &unrelated,
        "after the other table's append",
    );
}

/// Scoped cleanup removes only obsolete table coordinates across repeated churn.
#[test]
fn the_index_matches_the_change_log_after_a_scoped_cleanup_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("scoped_removal.db");
    let db = Database::open(&path).expect("open");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    install_work_ledger_schema(&db).expect("install");
    let unrelated = unrelated_history(&db);
    let mut expected = Vec::new();
    let mut latest_a = Vec::new();
    for i in 0..6 {
        latest_a = advertise_coordinate(
            &db,
            "node-a",
            &format!("cap-{i}"),
            1_700_000_000_000 + i,
            i > 0,
        );
        expected.extend(latest_a.iter().copied());
    }
    let latest_b = advertise_coordinate(&db, "node-b", "cap-b0", 1_700_000_000_500, false);
    expected.extend(latest_b.iter().copied());
    assert_durable_history(&db, "work_capabilities", &expected, "before cleanup");
    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup must produce prunable versions: {report:?}"
    );
    // The newest replacement shares its LSN and stable row id between the
    // delete and insert. Both durable occurrences name the surviving version.
    assert_durable_history(
        &db,
        "work_capabilities",
        &[latest_a.clone(), latest_b.clone()].concat(),
        "immediately after cleanup",
    );
    assert_durable_history(
        &db,
        "unrelated_history",
        &unrelated,
        "after scoped cleanup of another table",
    );
    let next_a = advertise_coordinate(&db, "node-a", "cap-after-cleanup", 1_700_000_000_600, true);
    let final_a = advertise_coordinate(
        &db,
        "node-a",
        "cap-after-cleanup-2",
        1_700_000_000_700,
        true,
    );
    assert_durable_history(
        &db,
        "work_capabilities",
        &[latest_a, latest_b.clone(), next_a, final_a.clone()].concat(),
        "after further appends before the second cleanup",
    );
    let report2 = db
        .compact_currency_versions()
        .expect("second compaction must succeed");
    assert!(
        report2.pruned_versions > 0,
        "the second round of churn must also produce prunable versions: {report2:?}"
    );
    // Each call performs one finite safe batch; finish this fixture's churn
    // before asserting the final history coordinates.
    let mut drained = false;
    for _ in 0..16 {
        let batch = db
            .compact_currency_versions()
            .expect("drain finite cleanup batch");
        if batch.pruned_versions == 0 {
            drained = true;
            break;
        }
    }
    assert!(
        drained,
        "the finite fixture must finish cleanup within sixteen batches"
    );
    assert_durable_history(
        &db,
        "work_capabilities",
        &[final_a, latest_b].concat(),
        "after a second cleanup cycle",
    );
    assert_durable_history(
        &db,
        "unrelated_history",
        &unrelated,
        "after a second scoped cleanup",
    );
}
