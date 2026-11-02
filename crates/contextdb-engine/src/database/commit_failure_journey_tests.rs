//! An engine commit that failed past the storage boundary: no later write on this
//! Database, committed reads exact, the first error kept, reopen restores service.
//!
//! The failure here is a real declared disk limit, with no injected fault. A disk
//! refusal past the boundary needing a reopen is the temporary state named as
//! deferred to the late-admission work. These rows pin containment. They do not
//! bless that refusal as policy.

use super::*;

const ROWS: u128 = 48;

/// Production opens the Legacy host (database.rs:2990). The Owned host exists only under
/// cfg(test), and is the only caller of the prepared commit at writer_source.rs:1145 and
/// of the failure branch at native_commit_paths.rs:3733. Each row runs on both.
#[derive(Clone, Copy, Debug)]
enum Host {
    Production,
    OwnedForTest,
}

const HOSTS: [Host; 2] = [Host::Production, Host::OwnedForTest];

fn open_on(host: Host, path: &std::path::Path, options: DatabaseOpenOptions) -> Database {
    match host {
        Host::Production => Database::open_with_options(path, options),
        Host::OwnedForTest => Database::open_owned_for_test(path, options),
    }
    .unwrap()
}

fn open_plain(host: Host, path: &std::path::Path) -> Database {
    open_on(host, path, DatabaseOpenOptions::default())
}

fn ids() -> Vec<uuid::Uuid> {
    (0..ROWS).map(|n| uuid::Uuid::from_u128(5000 + n)).collect()
}

fn text(round: u64) -> String {
    format!("{round:04}-{}", "x".repeat(400))
}

fn storage_latched(db: &Database) -> bool {
    persistence_latched(db.persistence.as_ref().unwrap())
}

fn persistence_latched(persistence: &RedbPersistence) -> bool {
    persistence
        .with_db(|storage| Ok(storage.__commit_failure_latched_for_test()))
        .unwrap()
}

/// Takes the disk limit out of memory only, because recording it would be a write.
/// What refuses after this is the latch, and not the limit.
fn lift_limit_in_memory(db: &Database) {
    db.persistence
        .as_ref()
        .unwrap()
        .set_disk_limit(None)
        .unwrap();
    db.disk_limit.store(0, Ordering::SeqCst);
}

fn assert_refused_by_the_latch(refused: Result<()>, context: &str) {
    assert!(refused.is_err(), "{context}: a later write was admitted");
    assert!(
        !matches!(
            refused,
            Err(Error::DiskBudgetExceeded { .. } | Error::PublicationInterrupted)
        ),
        "{context}: the refusal must come from the failed commit, got {refused:?}"
    );
}

fn later_update(db: &Database, table: &str, round: u64) -> Result<()> {
    let tx = db.begin()?;
    for id in ids() {
        let updated = db.execute_in_tx(
            tx,
            &format!("UPDATE {table} SET v = $v WHERE id = $id"),
            &HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("v".into(), Value::Text(text(round))),
            ]),
        );
        if let Err(error) = updated {
            let _ = db.rollback(tx);
            return Err(error);
        }
    }
    db.commit(tx)
}

fn staged(db: &Database, table: &str, round: u64) -> TxId {
    let tx = db.begin().unwrap();
    for id in ids() {
        db.execute_in_tx(
            tx,
            &format!("UPDATE {table} SET v = $v WHERE id = $id"),
            &HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("v".into(), Value::Text(text(round))),
            ]),
        )
        .unwrap();
    }
    tx
}

fn count(db: &Database, table: &str, round: u64) -> i64 {
    let rows = db
        .execute(
            &format!("SELECT COUNT(*) FROM {table} WHERE v = $v"),
            &HashMap::from([("v".into(), Value::Text(text(round)))]),
        )
        .unwrap();
    match rows.rows().first().and_then(|row| row.first()) {
        Some(Value::Int64(found)) => *found,
        other => panic!("count row: {other:?}"),
    }
}

/// Rewrites leave freed pages, `stable` is never rewritten, and `heavy` holds expired
/// rows so a maintenance pass has real work it must be refused from doing.
fn seeded(host: Host, path: &std::path::Path) -> Database {
    let db = open_plain(host, path);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let none = HashMap::new();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &none)
        .unwrap();
    db.execute("CREATE TABLE stable (id UUID PRIMARY KEY, v TEXT)", &none)
        .unwrap();
    db.execute(
        "CREATE TABLE heavy (id UUID PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
        &none,
    )
    .unwrap();
    let tx = db.begin().unwrap();
    for id in ids() {
        for (table, round) in [("t", 0), ("stable", 99)] {
            db.execute_in_tx(
                tx,
                &format!("INSERT INTO {table} (id, v) VALUES ($id, $v)"),
                &HashMap::from([
                    ("id".into(), Value::Uuid(id)),
                    ("v".into(), Value::Text(text(round))),
                ]),
            )
            .unwrap();
        }
        db.execute_in_tx(
            tx,
            "INSERT INTO heavy (id, expires) VALUES ($id, $expires)",
            &HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("expires".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    for round in 1..=3 {
        let tx = staged(&db, "t", round);
        db.commit(tx).unwrap();
    }
    db.maintenance_context()
        .unwrap()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    db
}

fn with_fixture_scope(run: impl FnOnce()) {
    let _decode = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    run();
}

fn raw_integrity_is_clean(path: &std::path::Path) {
    let mut raw = redb::Database::builder().create(path).unwrap();
    let integrity = raw.check_integrity();
    assert!(matches!(integrity, Ok(true)), "{integrity:?}");
}

const DISK_STEPS: std::ops::RangeInclusive<u64> = 0..=32;
const DISK_STEP: u64 = 4096;

#[test]
fn a_commit_past_the_boundary_refuses_later_writes_until_reopen_and_reads_stay_exact() {
    with_fixture_scope(|| {
        for host in HOSTS {
            let (mut crossed, mut before) = (0, 0);
            for step in DISK_STEPS {
                let delta = step * DISK_STEP;
                let context = format!("{host:?} disk headroom {delta}");
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("journey.db");
                let db = seeded(host, &path);
                let tx = staged(&db, "t", 200);
                let len = std::fs::metadata(&path).unwrap().len();
                db.set_disk_limit(Some(len + delta)).unwrap();
                let target = db.commit(tx);
                // No trigger here, so no audit write ran: this is the commit's own outcome.
                let target_crossed = storage_latched(&db);
                let lifted = db.set_disk_limit(None);
                let Err(first) = target else {
                    lifted.unwrap();
                    continue;
                };
                assert!(
                    matches!(first, Error::DiskBudgetExceeded { .. }),
                    "{context}: the first error must stay the refusal, got {first:?}"
                );
                if !target_crossed {
                    lifted.unwrap();
                    before += 1;
                    // Refused before the boundary: this Database keeps serving.
                    let tx = staged(&db, "t", 4);
                    db.commit(tx).unwrap();
                    assert_eq!(count(&db, "t", 4), 48, "{context}");
                    db.close().unwrap();
                    continue;
                }
                crossed += 1;
                // Temporary, deferred to the late-admission work: a limit cannot be
                // raised on this Database, because recording it is a write.
                assert!(lifted.is_err(), "{context}: recording a limit wrote");
                lift_limit_in_memory(&db);
                assert_refused_by_the_latch(later_update(&db, "t", 300), &context);
                assert_refused_by_the_latch(
                    db.execute(
                        "INSERT INTO stable (id, v) VALUES ($id, $v)",
                        &HashMap::from([
                            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(1))),
                            ("v".into(), Value::Text(text(5))),
                        ]),
                    )
                    .map(|_| ()),
                    &context,
                );
                assert!(
                    db.run_pruning_cycle_checked().is_err(),
                    "{context}: maintenance must not write on this Database"
                );
                assert_eq!(count(&db, "t", 3), 48, "{context}: last committed rows");
                assert_eq!(
                    count(&db, "t", 200),
                    0,
                    "{context}: the failed commit leaked"
                );
                assert_eq!(count(&db, "stable", 99), 48, "{context}: untouched rows");
                assert_refused_by_the_latch(db.close(), &context);
                drop(db);
                raw_integrity_is_clean(&path);
                let reopened = open_plain(host, &path);
                reopened.set_disk_limit(None).unwrap();
                assert_eq!(count(&reopened, "t", 3), 48, "{context}: after reopen");
                assert_eq!(
                    count(&reopened, "stable", 99),
                    48,
                    "{context}: after reopen"
                );
                let tx = staged(&reopened, "t", 400);
                reopened.commit(tx).unwrap();
                assert_eq!(
                    count(&reopened, "t", 400),
                    48,
                    "{context}: service restored"
                );
                reopened.close().unwrap();
            }
            assert!(
                crossed > 0,
                "{host:?}: no disk limit reached the commit boundary"
            );
            eprintln!("JOURNEY {host:?} crossed={crossed} refused_before={before}");
        }
    });
}

#[test]
fn a_memory_refusal_before_the_boundary_keeps_the_database_serving() {
    with_fixture_scope(|| {
        for host in HOSTS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("journey.db");
            let db = seeded(host, &path);
            let tx = staged(&db, "t", 200);
            let used = db.accountant.usage().used;
            db.accountant.set_budget(Some(used + 8192)).unwrap();
            let refused = db.commit(tx);
            db.accountant.set_budget(None).unwrap();
            assert!(
                matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
                "{refused:?}"
            );
            assert!(
                !storage_latched(&db),
                "a refused preparation must not latch"
            );
            let tx = staged(&db, "t", 4);
            db.commit(tx).unwrap();
            assert_eq!(count(&db, "t", 4), 48);
            db.close().unwrap();
        }
    });
}

/// Reads the latch at the moment the commit reports failure, before any rollback
/// audit runs. An audit write can itself cross the boundary afterwards, which reads
/// the same as this commit having crossed it if the latch is read any later.
#[derive(Default)]
struct LatchAtCommitFailure {
    persistence: std::sync::OnceLock<Arc<RedbPersistence>>,
    reported: AtomicBool,
    latched: AtomicBool,
}

impl crate::plugin::DatabasePlugin for LatchAtCommitFailure {
    fn commit_failed(&self, _: &contextdb_tx::WriteSet, _: crate::plugin::CommitSource, _: &Error) {
        if let Some(persistence) = self.persistence.get() {
            self.latched
                .store(persistence_latched(persistence), Ordering::SeqCst);
            self.reported.store(true, Ordering::SeqCst);
        }
    }
}

fn rolled_back(entries: impl IntoIterator<Item = TriggerAuditEntry>) -> usize {
    entries
        .into_iter()
        .filter(|entry| matches!(entry.status, TriggerAuditStatus::RolledBack { .. }))
        .count()
}

/// The rollback audit is a fresh history write straight after the failed commit. It
/// must be refused, with no audit row, and the original error kept.
#[test]
fn the_rollback_audit_of_a_commit_past_the_boundary_writes_nothing() {
    with_fixture_scope(|| {
        for host in HOSTS {
            let (mut crossed, mut audit_latched_after) = (0, 0u64);
            for step in DISK_STEPS {
                let delta = step * DISK_STEP;
                let context = format!("{host:?} audit disk headroom {delta}");
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("audit.db");
                let plugin = Arc::new(LatchAtCommitFailure::default());
                let db = open_on(
                    host,
                    &path,
                    DatabaseOpenOptions {
                        plugin: plugin.clone(),
                        ..DatabaseOpenOptions::default()
                    },
                );
                plugin
                    .persistence
                    .set(Arc::clone(db.persistence.as_ref().unwrap()))
                    .ok()
                    .unwrap();
                db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
                let none = HashMap::new();
                db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, v TEXT)", &none)
                    .unwrap();
                db.execute("CREATE TRIGGER note_audit ON notes WHEN INSERT", &none)
                    .unwrap();
                let fired = Arc::new(AtomicU64::new(0));
                let callback_fired = Arc::clone(&fired);
                db.register_trigger_callback("note_audit", move |_db, _ctx| {
                    callback_fired.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap();
                db.complete_initialization().unwrap();
                let insert_batch = |base: u128| {
                    let tx = db.begin().unwrap();
                    for n in 0..ROWS {
                        db.execute_in_tx(
                            tx,
                            "INSERT INTO notes (id, v) VALUES ($id, $v)",
                            &HashMap::from([
                                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(base + n))),
                                ("v".into(), Value::Text(text(1))),
                            ]),
                        )
                        .unwrap();
                    }
                    tx
                };
                for base in [1_000, 2_000, 3_000] {
                    let tx = insert_batch(base);
                    db.commit(tx).unwrap();
                }
                let audits_before = db.trigger_audit_log().len();
                fired.store(0, Ordering::SeqCst);
                let tx = insert_batch(9_000);
                let len = std::fs::metadata(&path).unwrap().len();
                db.set_disk_limit(Some(len + delta)).unwrap();
                let target = db.commit(tx);
                let lifted = db.set_disk_limit(None);
                let Err(first) = target else {
                    lifted.unwrap();
                    continue;
                };
                assert!(
                    plugin.reported.load(Ordering::SeqCst),
                    "{context}: the failed commit never reported, so nothing here reads its boundary: {first:?}"
                );
                if !plugin.latched.load(Ordering::SeqCst) {
                    // Refused before the boundary, so its audits were ordinary writes that
                    // may themselves cross it. What they showed in memory must be durable.
                    let in_memory =
                        rolled_back(db.trigger_audit_log().into_iter().skip(audits_before));
                    let handle_failed = lifted.is_err();
                    let closed = db.close();
                    assert_eq!(closed.is_err(), handle_failed, "{context}: {closed:?}");
                    drop(db);
                    drop(plugin);
                    raw_integrity_is_clean(&path);
                    let reopened = open_plain(host, &path);
                    let durable = rolled_back(
                        reopened
                            .trigger_audit_history(TriggerAuditFilter::default())
                            .unwrap(),
                    );
                    assert!(
                        durable >= in_memory,
                        "{context}: {in_memory} audits in memory, {durable} durable"
                    );
                    audit_latched_after += u64::from(handle_failed);
                    reopened.close().unwrap();
                    continue;
                }
                crossed += 1;
                assert!(lifted.is_err(), "{context}: recording a limit wrote");
                assert!(
                    fired.load(Ordering::SeqCst) > 0,
                    "{context}: no trigger ran, so the audit list is empty and this proves nothing"
                );
                assert!(
                    matches!(first, Error::DiskBudgetExceeded { .. }),
                    "{context}: the audit must not replace the first error, got {first:?}"
                );
                let new_audits: Vec<_> = db
                    .trigger_audit_log()
                    .into_iter()
                    .skip(audits_before)
                    .collect();
                assert!(
                    !new_audits
                        .iter()
                        .any(|entry| matches!(entry.status, TriggerAuditStatus::RolledBack { .. })),
                    "{context}: an audit reached memory on a failed handle: {new_audits:?}"
                );
                assert_refused_by_the_latch(db.close(), &context);
                drop(db);
                drop(plugin);
                raw_integrity_is_clean(&path);
                let reopened = open_plain(host, &path);
                let durable = reopened
                    .trigger_audit_history(TriggerAuditFilter::default())
                    .unwrap();
                assert!(
                    !durable
                        .iter()
                        .any(|entry| matches!(entry.status, TriggerAuditStatus::RolledBack { .. })),
                    "{context}: a rollback audit reached storage: {durable:?}"
                );
                reopened.close().unwrap();
            }
            assert!(
                crossed > 0,
                "{host:?}: no disk limit reached the audit's commit boundary"
            );
            eprintln!(
                "JOURNEY {host:?} audit crossed={crossed} latched_by_its_own_audit={audit_latched_after}"
            );
        }
    });
}

/// Sink delivery keeps its queue in ordinary commits of its own (update_sink_queue_entry,
/// redb_flush.rs). The dispatch drive pauses the dispatcher and runs each delivery on this
/// thread, returning its outcome, so nothing here waits on another thread or a clock.
///
/// Production host only. On the test-only Owned host, whose sink queue alone settles through
/// the prepared commit at writer_source.rs:1145, the dispatcher panics at its first attempt
/// ("paid owning sink metrics", event_bus.rs:7838; filed as CDB-205), so that route has no
/// engine row; the prepared route is proven by the redb rows.
#[test]
fn a_sink_queue_commit_past_the_boundary_refuses_later_writes_until_reopen() {
    const SINK: &str = "note_sink";
    with_fixture_scope(|| {
        let (mut crossed, mut settled, mut refused_before) = (0, 0, 0);
        for step in DISK_STEPS {
            let delta = step * DISK_STEP;
            let context = format!("sink disk headroom {delta}");
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("sink.db");
            let db = open_plain(Host::Production, &path);
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            let none = HashMap::new();
            for statement in [
                "CREATE TABLE notes (id UUID PRIMARY KEY, v TEXT)",
                "CREATE EVENT TYPE note_inserted WHEN INSERT ON notes",
                "CREATE SINK note_sink TYPE callback",
                "CREATE ROUTE note_route EVENT note_inserted TO note_sink",
            ] {
                db.execute(statement, &none).unwrap();
            }
            let delivered = Arc::new(AtomicU64::new(0));
            let drive = db.sink_dispatch_drive_for_test().unwrap();
            db.register_sink(SINK, None, {
                let delivered = Arc::clone(&delivered);
                move |_| {
                    delivered.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();
            let insert_batch = |base: u128| -> Result<()> {
                let tx = db.begin()?;
                for n in 0..ROWS {
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO notes (id, v) VALUES ($id, $v)",
                        &HashMap::from([
                            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(base + n))),
                            ("v".into(), Value::Text(text(1))),
                        ]),
                    )?;
                }
                db.commit(tx)
            };
            for (round, base) in [1_000, 2_000, 3_000].into_iter().enumerate() {
                insert_batch(base).unwrap();
                let receipt = drive
                    .drive_through_for_test(drive.committed_prefix_for_test())
                    .unwrap();
                assert!(
                    receipt.is_complete_for_test(),
                    "{context}: seeding {receipt:?}"
                );
                assert_eq!(
                    delivered.load(Ordering::SeqCst),
                    (round as u64 + 1) * ROWS as u64,
                    "{context}: seeding deliveries"
                );
            }
            insert_batch(9_000).unwrap();
            let prefix = drive.committed_prefix_for_test();
            assert_eq!(
                prefix.entries_for_test().len(),
                ROWS as usize,
                "{context}: queued"
            );
            let len = std::fs::metadata(&path).unwrap().len();
            db.set_disk_limit(Some(len + delta)).unwrap();
            let driven = drive.drive_through_for_test(prefix);
            let target_crossed = storage_latched(&db);
            let lifted = db.set_disk_limit(None);
            if !target_crossed {
                lifted.unwrap();
                match &driven {
                    Ok(receipt) if receipt.is_complete_for_test() => settled += 1,
                    _ => refused_before += 1,
                }
                drop(drive);
                db.close().unwrap();
                continue;
            }
            crossed += 1;
            assert!(lifted.is_err(), "{context}: recording a limit wrote");
            lift_limit_in_memory(&db);
            assert_refused_by_the_latch(insert_batch(20_000), &context);
            let committed = 4 * ROWS as i64;
            let rows = db.execute("SELECT COUNT(*) FROM notes", &none).unwrap();
            assert_eq!(
                rows.rows()[0][0],
                Value::Int64(committed),
                "{context}: committed rows"
            );
            drop(drive);
            assert_refused_by_the_latch(db.close(), &context);
            drop(db);
            raw_integrity_is_clean(&path);
            let reopened = open_plain(Host::Production, &path);
            reopened.set_disk_limit(None).unwrap();
            let rows = reopened
                .execute("SELECT COUNT(*) FROM notes", &none)
                .unwrap();
            assert_eq!(
                rows.rows()[0][0],
                Value::Int64(committed),
                "{context}: after reopen"
            );
            reopened
                .execute(
                    "INSERT INTO notes (id, v) VALUES ($id, $v)",
                    &HashMap::from([
                        ("id".into(), Value::Uuid(uuid::Uuid::from_u128(30_000))),
                        ("v".into(), Value::Text(text(2))),
                    ]),
                )
                .unwrap();
            reopened.close().unwrap();
        }
        eprintln!(
            "JOURNEY sink crossed={crossed} settled={settled} refused_before={refused_before}"
        );
        assert!(
            crossed > 0,
            "no disk limit reached a sink queue commit's boundary"
        );
    });
}
