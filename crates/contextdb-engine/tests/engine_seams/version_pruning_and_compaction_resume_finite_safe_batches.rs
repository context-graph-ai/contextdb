//! "Maintenance must prepare replacement indexes and reclaim eligible storage
//! in finite batches while admitted writes and ordinary reads continue."
use crate::resident_history_support as support;
use contextdb_core::{Value, Wallclock};
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSourceEvent, ReadKernelTestObserver,
};
use contextdb_engine::{OwnerReadCancellation, ReadSession, ReadSessionOptions};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Default)]
struct ExternalSource(AtomicUsize);
impl ReadKernelTestObserver for ExternalSource {
    fn before_source_touch(&self, _: ReadKernelSourceEvent, _: OwnerReadCancellation) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn cancellation_observed(&self, _: ReadKernelCancellationEvent, _: OwnerReadCancellation) {}
}
use contextdb_engine::database::resident_history_test_seams::observe;
use support::*;

fn survivors(db: &contextdb_engine::Database, old: contextdb_core::SnapshotId, inserted: usize) {
    assert_eq!(
        exec(db, "SELECT id,revision FROM current_versions ORDER BY id"),
        (0..256)
            .map(|id| vec![Value::Int64(id), Value::Int64(1)])
            .chain((256..256 + inserted as i64).map(|id| vec![Value::Int64(id), Value::Int64(2)]))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        db.execute_at_snapshot(
            "SELECT id,revision FROM all_versions ORDER BY id",
            &empty(),
            old
        )
        .unwrap()
        .rows,
        (0..256)
            .map(|id| vec![Value::Int64(id), Value::Int64(0)])
            .collect::<Vec<_>>(),
        "HISTORY ALL exact frontier survives every cleanup batch"
    );
    assert_eq!(
        exec(db, "SELECT id FROM parents"),
        vec![vec![Value::Int64(1)]],
        "expired FK-protected row survives"
    );
}
#[test]
fn version_pruning_and_compaction_resume_finite_safe_batches() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reclamation.db");
    let db = open(&path, None);
    exec(
        &db,
        "CREATE TABLE all_versions (id INT PRIMARY KEY,revision INT,body TEXT) HISTORY ALL",
    );
    exec(
        &db,
        "CREATE TABLE current_versions (id INT PRIMARY KEY,revision INT,body TEXT) HISTORY CURRENT ONLY SYNC OFF",
    );
    exec(
        &db,
        "CREATE TABLE parents (id INT PRIMARY KEY,expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    exec(
        &db,
        "CREATE TABLE children (id INT PRIMARY KEY,parent INT REFERENCES parents(id))",
    );
    db.execute(
        "INSERT INTO parents VALUES (1,$expiry)",
        &std::collections::HashMap::from([("expiry".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    exec(&db, "INSERT INTO children VALUES (1,1)");
    for id in 0..256 {
        for table in ["all_versions", "current_versions"] {
            db.execute(
                &format!("INSERT INTO {table} VALUES ($id,0,$body)"),
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("body".into(), Value::Text("r".repeat(8192))),
                ]),
            )
            .unwrap();
        }
    }
    for id in 2..=258 {
        db.execute(
            "INSERT INTO parents VALUES ($id,$expiry)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("expiry".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    exec(
        &db,
        "CREATE TABLE reader_pressure (id INT PRIMARY KEY, body TEXT)",
    );
    for id in 0..128 {
        db.execute(
            "INSERT INTO reader_pressure VALUES ($id,$body)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("body".into(), Value::Text("d".repeat(65536))),
            ]),
        )
        .unwrap();
    }
    let old = db.snapshot();
    db.close().unwrap();
    drop(db);
    let external_source = Arc::new(ExternalSource::default());
    let external = ReadSession::open_with_observations_for_test(
        &path,
        ReadSessionOptions::default(),
        Arc::new(contextdb_engine::local_transport::ManualDeadlineClock::default()),
        None,
        Some(external_source.clone()),
    )
    .unwrap();
    let external_root = external
        .file_snapshot_for_test()
        .expect("a real external file root");
    let pinned_oracle = (0..256)
        .map(|id| vec![Value::Int64(id), Value::Int64(0)])
        .collect::<Vec<_>>();
    assert_eq!(
        external
            .execute(
                "SELECT id,revision FROM current_versions ORDER BY id",
                &empty()
            )
            .unwrap()
            .rows,
        pinned_oracle
    );
    let db = open(&path, None);
    // The external file reader is the ONLY root protecting current-only history.
    for id in 0..256 {
        for table in ["all_versions", "current_versions"] {
            exec(
                &db,
                &format!("UPDATE {table} SET revision = 1 WHERE id = {id}"),
            );
        }
    }
    let (held, held_work) = observe(|| db.compact_currency_versions());
    let held = held.unwrap();
    // An external physical root can safely retain old pages even when the
    // current tree drops an old version. Its late read below is the safety
    // oracle; current-tree logical pruning must still be a finite batch.
    assert!(
        held.pruned_versions <= 64 && held_work.cleanup_versions_examined <= 128,
        "cleanup beside an external root examines and removes only one finite batch: {held:?}; {held_work:?}"
    );
    assert_eq!(
        external
            .execute(
                "SELECT id,revision FROM current_versions ORDER BY id",
                &empty()
            )
            .unwrap()
            .rows,
        pinned_oracle
    );
    let (retention, first_retention_work) = observe(|| db.run_pruning_cycle_checked());
    let retention = retention.unwrap();
    let unfinished_retention = exec(&db, "SELECT COUNT(*) FROM parents")[0][0] != Value::Int64(1);
    db.close().unwrap();
    drop(db);
    let db = open(&path, None);
    let mut retention_work = vec![first_retention_work];
    let mut retention_batches = vec![retention.pruned_rows];
    for _ in 0..32 {
        let (report, work) = observe(|| db.run_pruning_cycle_checked());
        retention_batches.push(report.unwrap().pruned_rows);
        retention_work.push(work);
    }
    assert_eq!(
        exec(&db, "SELECT id FROM parents ORDER BY id"),
        vec![vec![Value::Int64(1)]],
        "eligible expired controls disappear while the FK-protected parent survives"
    );
    eprintln!("retention batches after restart: {retention_batches:?}; work={retention_work:?}");
    // Old reader still sees both rows, current root loses the eligible control.
    assert_eq!(
        external
            .execute("SELECT id FROM parents ORDER BY id", &empty())
            .unwrap()
            .rows,
        (1..=258)
            .map(|id| vec![Value::Int64(id)])
            .collect::<Vec<_>>()
    );
    // Exercise physical batches WHILE only the external reader protects old pages.
    for generation in 0..4 {
        db.__storage_batch_for_test().unwrap();
        exec(
            &db,
            &format!("UPDATE current_versions SET body = 'successor-{generation}' WHERE id = 255"),
        );
    }
    // Displace reader-side pages with more live bodies than its cache share;
    // the old target body has not been selected by any preceding statement.
    for id in 0..128 {
        assert_eq!(
            external
                .execute(
                    &format!("SELECT body FROM reader_pressure WHERE id = {id}"),
                    &empty()
                )
                .unwrap()
                .rows,
            vec![vec![Value::Text("d".repeat(65536))]]
        );
    }
    let before_late_source = external_source.0.load(Ordering::Relaxed);
    assert_eq!(
        external
            .execute("SELECT body FROM current_versions WHERE id = 255", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Text("r".repeat(8192))]],
        "late old body read remains intact after physical displacement without a local pin"
    );
    assert!(
        external_source.0.load(Ordering::Relaxed) > before_late_source,
        "late old body answer comes through the real bounded source"
    );
    assert_eq!(
        external.file_snapshot_for_test(),
        Some(external_root),
        "physical batches cannot replace the external reader's captured root"
    );
    assert_eq!(
        exec(&db, "SELECT body FROM current_versions WHERE id = 255"),
        vec![vec![Value::Text("successor-3".into())]]
    );
    survivors(&db, old, 0);
    let backlog_held = db.__deferred_free_backlog_for_test().unwrap();
    assert!(
        backlog_held.iter().any(|(_, pages)| *pages > 0),
        "reader roots create real deferred storage ownership"
    );
    drop(external);
    for _ in 0..32 {
        let (report, work) = observe(|| db.run_pruning_cycle_checked());
        retention_batches.push(report.unwrap().pruned_rows);
        retention_work.push(work);
    }
    assert_eq!(
        retention_batches.iter().sum::<u64>(),
        257,
        "eligible expiry versions must be physically reclaimed after their external root leaves"
    );
    assert_eq!(
        db.__durable_version_count_for_test("parents").unwrap(),
        1,
        "physical durable retention oracle excludes a no-op after logical expiry"
    );
    let (first, first_work) = observe(|| db.compact_currency_versions());
    let first = first.unwrap();
    let unfinished = db
        .__durable_version_count_for_test("current_versions")
        .unwrap()
        > 256;
    // Restart immediately after ONE batch; a whole-population operation cannot
    // masquerade as resumable maintenance by closing after it already finished.
    db.close().unwrap();
    drop(db);
    let db = open(&path, None);
    let remaining_at_restart = db
        .__durable_version_count_for_test("current_versions")
        .unwrap();
    let mut reclaimed = held.pruned_versions + first.pruned_versions;
    let mut work = vec![held_work, first_work];
    let mut batches = vec![held.pruned_versions, first.pruned_versions];
    for cycle in 0..32 {
        let (result, observed) = observe(|| db.compact_currency_versions());
        let result = result.unwrap();
        reclaimed += result.pruned_versions;
        batches.push(result.pruned_versions);
        work.push(observed);
        if cycle < 4 {
            exec(
                &db,
                &format!(
                    "INSERT INTO current_versions VALUES ({},2,'new')",
                    256 + cycle
                ),
            );
        }
        survivors(&db, old, (cycle + 1).min(4));
    }
    assert_eq!(reclaimed, 260, "eligible reclamation really finishes");
    assert_eq!(
        db.__durable_version_count_for_test("current_versions")
            .unwrap(),
        260
    );
    assert_eq!(
        db.__durable_version_count_for_test("all_versions").unwrap(),
        512
    );
    // Physical maintenance uses its actual single-batch production path.
    let first_storage = db.__storage_batch_for_test().unwrap();
    let cursor = db.__storage_cursor_for_test();
    db.close().unwrap();
    drop(db);
    let db = open(&path, None);
    let restored_cursor = db.__storage_cursor_for_test();
    let mut storage_batches = vec![first_storage];
    let mut completed = false;
    let mut moved = first_storage.2;
    for _ in 0..512 {
        let progress = db.__storage_batch_for_test().unwrap();
        moved += progress.2;
        storage_batches.push(progress);
        survivors(&db, old, 4);
        if progress.0 {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "physical compaction finishes finite batches after restart"
    );
    // Drive ordinary bounded commits; deferred frees must drain rather than
    // remain forever merely because logical cleanup returned success.
    for _ in 0..512 {
        if db
            .__deferred_free_backlog_for_test()
            .unwrap()
            .iter()
            .all(|(entries, pages)| *entries == 0 && *pages == 0)
        {
            break;
        }
        db.__commit_storage_transaction_for_test().unwrap();
    }
    let drained = db.__deferred_free_backlog_for_test().unwrap();
    eprintln!(
        "currency first={first:?}, unfinished={unfinished}, restart remaining={remaining_at_restart}, batches={batches:?}, work={work:?}; storage cursor {cursor} -> {restored_cursor}, batches={storage_batches:?}; frees {backlog_held:?} -> {drained:?}, relocated={moved}"
    );
    assert!(
        unfinished_retention,
        "retention commits an unfinished batch before restart; whole-population expiry is not finite progress"
    );
    assert!(
        retention_work
            .iter()
            .any(|work| work.retention_keys_examined > 0)
            && retention_work
                .iter()
                .all(|work| work.retention_keys_examined <= 512),
        "retention candidate source windows are positively observed and bounded: {retention_work:?}"
    );
    assert!(
        retention_batches.iter().all(|count| *count <= 64),
        "retention physical cleanup performs finite durable deletions"
    );
    assert!(
        unfinished && remaining_at_restart > 256,
        "restart must resume unfinished cleanup, not reopen after whole-population reclamation"
    );
    assert!(
        work.iter()
            .any(|observed| observed.cleanup_versions_examined > 0),
        "real cleanup source work must be observed; zero observers cannot certify batches"
    );
    assert!(
        work.iter()
            .all(|observed| observed.cleanup_versions_examined <= 128),
        "all examined keeper/protection keys, including nonremoved rows, must fit a frozen batch: {work:?}"
    );
    assert!(
        batches.iter().all(|count| *count <= 64),
        "successful deletion work is also finite"
    );
    assert!(
        !first_storage.0 && restored_cursor == cursor,
        "unfinished physical cursor survives restart without discarding progress"
    );
    assert!(
        storage_batches
            .iter()
            .all(|(_, examined, _)| *examined <= 80),
        "each real physical step bounds all discovered pages"
    );
    assert!(
        moved > 0,
        "a no-op compactor cannot pass eligible physical reclamation"
    );
    assert_eq!(
        drained,
        [(0, 0), (0, 0)],
        "eligible deferred-free entries and pages actually drain"
    );
    db.close().unwrap();
}
