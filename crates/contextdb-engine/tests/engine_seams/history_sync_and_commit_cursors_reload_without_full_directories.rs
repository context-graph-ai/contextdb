//! "History bookkeeping": bounded durable access on reopen, append,
//! retained-history readback and sync preserves progress and snapshot boundaries.
use crate::resident_history_support as support;
use contextdb_core::{Lsn, Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_engine::{
    Database,
    sync_types::{ConflictPolicies, ConflictPolicy},
};
use support::*;

#[test]
fn history_sync_and_commit_cursors_reload_without_full_directories() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut owners = Vec::new();
    for n in [64, 128] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let db = open(&path, None);
        exec(
            &db,
            "CREATE TABLE history (id INT PRIMARY KEY, revision INT) HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        );
        exec(
            &db,
            "CREATE TABLE cache_pressure (id INT PRIMARY KEY, body TEXT) HISTORY CURRENT ONLY SYNC OFF",
        );
        for id in 0..64 {
            db.execute(
                "INSERT INTO cache_pressure VALUES ($id,$body)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("body".into(), Value::Text("p".repeat(65536))),
                ]),
            )
            .unwrap();
        }
        let mut checkpoints = Vec::new();
        for id in 0..n {
            exec(&db, "BEGIN");
            exec(&db, &format!("INSERT INTO history VALUES ({id}, 0)"));
            exec(&db, &format!("INSERT INTO history VALUES ({}, 0)", id + n));
            exec(&db, "COMMIT");
            checkpoints.push((db.current_lsn(), db.snapshot()));
            exec(
                &db,
                &format!("UPDATE history SET revision = 1 WHERE id = {id}"),
            );
        }
        let log_entries = db.change_log_since(Lsn(0));
        let log = canonical(&log_entries);
        let commits = db.commit_index_for_test();
        let changes = canonical(&db.changes_since(Lsn(0)));
        let suffix = checkpoints[n / 2].0;
        let suffix_oracle = canonical(&db.changes_since(suffix));
        let artifact = dir.path().join("snapshot.db");
        db.export_snapshot(&artifact).unwrap();
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(16 * 1024 * 1024));
        let before_pressure = db.__writer_cache_stats_for_test().unwrap();
        for id in 0..64 {
            assert_eq!(
                exec(
                    &db,
                    &format!("SELECT body FROM cache_pressure WHERE id = {id}")
                ),
                vec![vec![Value::Text("p".repeat(65536))]]
            );
        }
        let after_pressure = db.__writer_cache_stats_for_test().unwrap();
        assert!(
            after_pressure.2 > before_pressure.2,
            "durable cache pressure actually evicts pages"
        );
        assert_eq!(
            canonical(&db.change_log_since(Lsn(0))),
            log,
            "exact log occurrence order including equal-LSN siblings"
        );
        assert_eq!(
            db.commit_index_for_test(),
            commits,
            "complete ordered commit oracle"
        );
        assert_eq!(canonical(&db.changes_since(Lsn(0))), changes);
        assert_eq!(
            canonical(&db.changes_since(suffix)),
            suffix_oracle,
            "resumed export suffix"
        );
        for (ordinal, (lsn, snapshot)) in checkpoints.iter().enumerate().step_by(7) {
            assert_eq!(db.snapshot_at(*lsn), *snapshot);
            assert_eq!(
                db.execute_at_snapshot(
                    &format!("SELECT revision FROM history WHERE id = {ordinal}"),
                    &empty(),
                    *snapshot
                )
                .unwrap()
                .rows,
                vec![vec![Value::Int64(0)]]
            );
        }
        let receiver = Database::open_memory();
        receiver.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
        receiver
            .apply_changes(
                db.changes_since(Lsn(0)),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .unwrap();
        assert_eq!(
            exec(&receiver, "SELECT id, revision FROM history ORDER BY id"),
            exec(&db, "SELECT id, revision FROM history ORDER BY id")
        );
        let snapshot = open(&artifact, None);
        assert_eq!(
            exec(&snapshot, "SELECT id, revision FROM history ORDER BY id"),
            exec(&db, "SELECT id, revision FROM history ORDER BY id")
        );
        snapshot.close().unwrap();
        let last = checkpoints[n - 1].0;
        let before_tail = db.__writer_cache_stats_for_test().unwrap();
        let (page, page_work) = observe(|| db.change_log_since(last));
        let after_tail = db.__writer_cache_stats_for_test().unwrap();
        assert_eq!(
            canonical(&page),
            canonical(
                &log_entries
                    .iter()
                    .filter(|entry| entry.lsn() > last)
                    .cloned()
                    .collect::<Vec<_>>()
            ),
            "exact fixed resumed tail"
        );
        assert!(
            page_work.log_keys_examined > 0 && page_work.log_keys_examined <= 16,
            "fixed trailing window never examines the completed history: {page_work:?}"
        );
        assert!(
            after_tail.0 + after_tail.1 > before_tail.0 + before_tail.1,
            "resumed history must access the actual durable page source, not a resident log"
        );
        let r = db.writer_memory_ownership_receipt_for_test();
        assert_eq!(r.reconciliation_gap, 0);
        owners.push((
            r.change_log_charges.iter().sum::<usize>(),
            db.__resident_commit_directory_bytes_for_test(),
            r.ddl_log_estimated_bytes,
            db.__history_sidecar_owners_for_test().total(),
        ));
        db.close().unwrap();
    }
    eprintln!("history retained owners N/2N: {owners:?}");
    assert!(
        owners.iter().all(|(log, commit, ddl, sidecars)| *log == 0
            && *commit == 0
            && *ddl == 0
            && *sidecars == 0),
        "durable history cursors must retain no full log or commit directory: {owners:?}"
    );
}
