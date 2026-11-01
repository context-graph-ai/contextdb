include!("durable_test_observations.rs");
use super::*;
use tempfile::TempDir;

#[test]
fn retention_reclaim_crash_preserves_atomic_copies_and_deferred_retry_identity() {
    const CHILD_PATH: &str = "CONTEXTDB_RETENTION_RECLAIM_TEST_PATH";
    const CHILD_CASE: &str = "CONTEXTDB_RETENTION_RECLAIM_TEST_CASE";
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let expired = uuid::Uuid::from_u128(80_001);
    let survivor = uuid::Uuid::from_u128(80_002);
    let endpoint = uuid::Uuid::from_u128(80_003);
    let insert = |db: &Database, id: uuid::Uuid, expires: i64| {
        db.execute(
            "INSERT INTO retained VALUES ($id, $expires, '[1,0]')",
            &HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("expires".into(), Value::Timestamp(expires)),
            ]),
        )
        .unwrap();
    };
    if let Ok(path) = std::env::var(CHILD_PATH) {
        let case = std::env::var(CHILD_CASE).unwrap();
        let db = Database::open(&path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let pin = (case == "deferred").then(|| db.pin_snapshot(db.snapshot()).unwrap());
        if pin.is_some() {
            insert(&db, expired, -1);
        }
        assert_eq!(
            db.persistence
                .as_ref()
                .unwrap()
                .load_vectors()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            db.persistence
                .as_ref()
                .unwrap()
                .load_row_owned_edges_for_test(false)
                .unwrap()
                .len(),
            2
        );
        db.__arm_retention_reclaim_crash_for_test(case != "before");
        db.run_pruning_cycle_checked().unwrap();
        panic!("the production reclaim checkpoint was not reached");
    }
    for case in ["before", "after", "deferred"] {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("retention-crash.redb");
        let mut expired_lsn = None;
        {
            let db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            db.execute("CREATE TABLE retained (id UUID PRIMARY KEY, expires_at TIMESTAMP EXPIRES, embedding VECTOR(2)) RETAIN 1 HOURS", &params()).unwrap();
            db.execute("CREATE TABLE retained_scalar (id INT PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &params()).unwrap();
            db.execute("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)", &params()).unwrap();
            if case != "deferred" {
                db.execute("INSERT INTO retained_scalar VALUES (1, -1)", &params())
                    .unwrap();
                expired_lsn = Some(db.tx_mgr.current_lsn());
                // The scalar commit becomes reclaimable before any vector row
                // exists, so the crash below is the only thing under test.
                db.run_maintenance_cycle().unwrap();
            }
            insert(&db, survivor, i64::MAX);
            if case != "deferred" {
                insert(&db, expired, -1);
            }
            for (ordinal, source) in [expired, survivor].into_iter().enumerate() {
                db.execute(
                    "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS')",
                    &HashMap::from([
                        (
                            "id".into(),
                            Value::Uuid(uuid::Uuid::from_u128(90_000 + ordinal as u128)),
                        ),
                        ("source".into(), Value::Uuid(source)),
                        ("target".into(), Value::Uuid(endpoint)),
                    ]),
                )
                .unwrap();
            }
            db.close().unwrap();
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "database::retention_prune_persistence_tests::retention_reclaim_crash_preserves_atomic_copies_and_deferred_retry_identity", "--nocapture"])
            .env(CHILD_PATH, &path).env(CHILD_CASE, case).output().unwrap();
        assert!(!output.status.success());
        let marker = format!(
            "RETENTION_RECLAIM_CRASH={}",
            if case == "before" { 1 } else { 2 }
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(&marker),
            "child must reach the actual durable boundary: {:?}",
            output
        );
        {
            let db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            let persistence = db.persistence.as_ref().unwrap();
            assert_eq!(
                visible_rows(&db, "retained"),
                1,
                "expiry stays durable across the crash"
            );
            assert_eq!(
                persistence.load_vectors().unwrap().len(),
                if case == "before" { 2 } else { 1 },
                "a row commit must never strand its vector"
            );
            if let Some(lsn) = expired_lsn {
                assert_eq!(
                    persistence.load_commit_index().unwrap().contains_key(&lsn),
                    case == "before",
                    "the eligible commit-index identity follows the same durable commit as the row"
                );
            }
            assert_eq!(
                persistence
                    .load_row_owned_edges_for_test(false)
                    .unwrap()
                    .len(),
                if case == "after" { 1 } else { 2 }
            );
            if case == "deferred" {
                let nodes = persistence
                    .load_config_value::<BTreeSet<NodeId>>(
                        crate::persistence::RETENTION_DEFERRED_EDGE_NODES,
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    nodes,
                    BTreeSet::from([expired]),
                    "the reader-deferred edge has a durable retry identity after its row is gone"
                );
            }
            db.run_pruning_cycle_checked().unwrap();
            if let Some(lsn) = expired_lsn {
                assert!(
                    !persistence.load_commit_index().unwrap().contains_key(&lsn),
                    "a retry reclaims the eligible commit-index identity"
                );
            }
            assert_eq!(persistence.load_vectors().unwrap().len(), 1);
            let edges = persistence.load_row_owned_edges_for_test(false).unwrap();
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0].source, survivor);
            assert_eq!(
                persistence
                    .load_row_owned_edges_for_test(true)
                    .unwrap()
                    .len(),
                1
            );
            assert!(
                persistence
                    .load_config_value::<BTreeSet<NodeId>>(
                        crate::persistence::RETENTION_DEFERRED_EDGE_NODES
                    )
                    .unwrap()
                    .is_none()
            );
            db.close().unwrap();
        }
        let reopened = Database::open(&path).unwrap();
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(visible_rows(&reopened, "retained"), 1);
        assert_eq!(
            reopened
                .persistence
                .as_ref()
                .unwrap()
                .load_vectors()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            reopened
                .persistence
                .as_ref()
                .unwrap()
                .load_row_owned_edges_for_test(false)
                .unwrap()
                .len(),
            1
        );
    }
}

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn visible_rows(db: &Database, table: &str) -> usize {
    let snapshot = db.snapshot();
    durable_observation::rows(db, table)
        .iter()
        .filter(|row| row.visible_at(snapshot))
        .count()
}

/// Returns the DB plus the mock-clock guard, which the caller must hold:
/// the expiry read (`Wallclock::now()` inside `run_pruning_cycle*`) runs
/// on the test thread later in the test body, and dropping the guard
/// early would hand it the real clock again.
fn db_with_expired_row() -> (Database, contextdb_core::WallclockTestClockGuard, PathBuf) {
    let mock_now = Arc::new(AtomicU64::new(1_000_000));
    let clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(Ordering::SeqCst))
    };
    let tmp = TempDir::new().unwrap();
    let path = tmp.keep().join("retention-persist-failure.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, note TEXT) RETAIN 1 SECONDS",
        &params(),
    )
    .unwrap();
    db.execute("INSERT INTO obs (id, note) VALUES (1, 'old')", &params())
        .unwrap();
    // Advance 2 s past the row's stamped time — beyond the 1 s TTL. Both
    // the insert stamp and the expiry read happen on this thread, so the
    // mock governs both; no real sleep needed.
    mock_now.fetch_add(2_000, Ordering::SeqCst);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    (db, clock, path)
}

#[test]
fn checked_prune_reports_persistence_failure_without_mutating_memory() {
    let (db, _clock, path) = db_with_expired_row();
    let before = durable_observation::rows(&db, "obs");
    assert_eq!(before.len(), 1);
    let snapshot = db.snapshot();

    db.persistence.as_ref().unwrap().close();
    let err = db.run_pruning_cycle_checked().unwrap_err();

    assert!(
        err.to_string().contains("database persistence is closed"),
        "checked pruning must surface persistence rewrite failure; got {err:?}"
    );
    drop(db);
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(reopened.snapshot(), snapshot);
    assert_eq!(
        durable_observation::rows(&reopened, "obs"),
        before,
        "failed persistence preserves exact durable row versions and expiry stamps for retry"
    );
    assert_eq!(visible_rows(&reopened, "obs"), 1);
}

#[test]
fn pruning_reports_failure_and_keeps_memory() {
    let (db, _clock, path) = db_with_expired_row();
    let before = durable_observation::rows(&db, "obs");
    assert_eq!(before.len(), 1);
    let snapshot = db.snapshot();

    db.persistence.as_ref().unwrap().close();
    let err = db.run_pruning_cycle().unwrap_err();

    assert!(
        matches!(&err, Error::Other(detail) if detail == "database persistence is closed"),
        "pruning must surface the closed persistence error; got {err:?}"
    );
    drop(db);
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(reopened.snapshot(), snapshot);
    assert_eq!(
        durable_observation::rows(&reopened, "obs"),
        before,
        "failed persistence preserves exact durable row versions and expiry stamps for retry"
    );
    assert_eq!(visible_rows(&reopened, "obs"), 1);
}
