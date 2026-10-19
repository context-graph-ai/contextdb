//! "History bookkeeping": bounded durable access on reopen, append,
//! retained-history readback and sync preserves progress and snapshot boundaries.
use crate::resident_history_support as support;
use contextdb_core::{Lsn, Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_engine::writer_query_test_seams::observe as writer_work;
use contextdb_engine::{
    Database,
    sync_types::{ConflictPolicies, ConflictPolicy},
};
use support::*;

#[test]
fn history_sync_and_commit_cursors_reload_without_full_directories() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut owners = Vec::new();
    let mut sync_work = Vec::new();
    let mut append_work = Vec::new();
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
        let log_entries = db.change_log_since(Lsn(0)).unwrap();
        let log = canonical(&log_entries);
        let commits = db.commit_index_for_test();
        let changes = canonical(&db.changes_since(Lsn(0)).expect("read the declared changes"));
        let suffix = checkpoints[n / 2].0;
        let suffix_oracle =
            canonical(&db.changes_since(suffix).expect("read the declared changes"));
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
            canonical(&db.change_log_since(Lsn(0)).unwrap()),
            log,
            "exact log occurrence order including equal-LSN siblings"
        );
        assert_eq!(
            db.commit_index_for_test(),
            commits,
            "complete ordered commit oracle"
        );
        assert_eq!(
            canonical(&db.changes_since(Lsn(0)).expect("read the declared changes")),
            changes
        );
        assert_eq!(
            canonical(&db.changes_since(suffix).expect("read the declared changes")),
            suffix_oracle,
            "resumed export suffix"
        );
        let fixed_suffix = checkpoints[n - 1].0;
        let fixed_oracle = canonical(
            &log_entries
                .iter()
                .filter(|entry| entry.lsn() > fixed_suffix)
                .cloned()
                .collect::<Vec<_>>(),
        );
        let (exported_suffix, export_work) = observe(|| {
            db.changes_since(fixed_suffix)
                .expect("read the declared changes")
        });
        assert_eq!(
            canonical(&exported_suffix),
            fixed_oracle,
            "resumed export suffix"
        );
        sync_work.push(export_work.log_keys_examined);
        assert!(
            export_work.log_keys_examined > 0 && export_work.log_keys_examined <= 16,
            "fixed export suffix must seek the saved position: {export_work:?}"
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
                db.changes_since(Lsn(0)).expect("read the declared changes"),
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
        let (page, page_work) = observe(|| db.change_log_since(last).unwrap());
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
        let before_append = db.current_lsn();
        let ((_, append), directory_append) =
            writer_work(|| observe(|| exec(&db, "INSERT INTO history VALUES (1000000, 2)")));
        let (tail, tail_work) = observe(|| db.change_log_since(before_append).unwrap());
        assert_eq!(tail.len(), 1);
        assert_eq!(
            exec(&db, "SELECT revision FROM history WHERE id = 1000000"),
            vec![vec![Value::Int64(2)]]
        );
        let (current_snapshot, directory_lookup) = writer_work(|| db.snapshot_at(db.current_lsn()));
        assert_eq!(current_snapshot, db.snapshot());
        assert!(
            directory_lookup.history.commit_index_entries > 0
                && directory_lookup.history.commit_index_entries <= 2,
            "actual saved-directory lookup examines only the requested committed position: {directory_lookup:?}"
        );
        append_work.push((
            append.log_keys_examined,
            tail_work.log_keys_examined,
            directory_append.history.commit_index_entries,
            directory_lookup.history.commit_index_entries,
        ));
        assert!(tail_work.log_keys_examined > 0 && tail_work.log_keys_examined <= 16);
        assert_eq!(db.__resident_commit_directory_bytes_for_test(), 0);
        db.close().unwrap();
    }
    assert_eq!(
        sync_work[0], sync_work[1],
        "fixed resumed export work ignores doubled completed log"
    );
    assert_eq!(
        append_work[0], append_work[1],
        "post-reopen append and fixed log tail ignore doubled commit history"
    );
    eprintln!("history retained owners N/2N: {owners:?}");
    assert!(
        owners.iter().all(|(log, commit, ddl, sidecars)| *log == 0
            && *commit == 0
            && *ddl == 0
            && *sidecars == 0),
        "durable history cursors must retain no full log or commit directory: {owners:?}"
    );
}
