//! §30 — Same-DB cross-thread progress under a registered trigger callback.
//! Acceptance tests for the wait-and-proceed contract, cross-DB Class B
//! independence, cross-DB-from-callback Class A, source-order priority,
//! deadlock-guard timeout, schema-agnosticity across cascade types,
//! Drop/close while parked, and §29 regression guards.

use crate::common_tracing::{
    T30_COUNT_ENABLED, T30_LAST_WARN_FIELDS, T30_WARN_COUNT,
    install_global_subscriber as t30_install_global_subscriber, t30_reset_warn_counters,
};
use contextdb_core::{
    CallbackKind, Error, MemoryAccountant, Result, Value, VectorIndexRef,
    types::{EdgeType, NodeId},
};
use contextdb_engine::plugin::DatabasePlugin;
use contextdb_engine::{CronAuditKind, Database, QueryResult};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

type LabeledQueryResults = Arc<Mutex<Vec<(&'static str, Result<QueryResult>)>>>;
type LabeledUnitResults = Arc<Mutex<Vec<(&'static str, Result<()>)>>>;

// RAII guard that removes a process-wide env var on drop, ensuring cleanup on
// panic or normal exit. Used by t30_17 + the surface-named siblings (t30_17_commit
// / _rollback / _apply_changes / _close) that override
// `CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS` for their bounded-budget assertion.
pub(crate) struct EnvGuard(pub &'static str);
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: 2024 edition requires unsafe for env mutation.
        unsafe {
            std::env::remove_var(self.0);
        }
    }
}

// Releases an atomic callback gate on all exits. This prevents expected-RED
// assertion panics from leaving a spawned callback thread parked and poisoning
// the process-global callback-active counters for later tests.
struct AtomicReleaseGuard(Arc<AtomicBool>);
impl Drop for AtomicReleaseGuard {
    fn drop(&mut self) {
        self.0.store(true, AtomicOrdering::SeqCst);
    }
}

// Releases a two-party callback barrier on all exits after the callback has
// reached its `entered` barrier. Tests disarm it after performing the normal
// explicit release. Use only after the parked callback is known to be waiting.
struct BarrierReleaseGuard {
    barrier: Option<Arc<Barrier>>,
}
impl BarrierReleaseGuard {
    fn armed(barrier: Arc<Barrier>) -> Self {
        Self {
            barrier: Some(barrier),
        }
    }
    fn disarm(&mut self) {
        self.barrier = None;
    }
}
impl Drop for BarrierReleaseGuard {
    fn drop(&mut self) {
        if let Some(barrier) = self.barrier.take() {
            barrier.wait();
        }
    }
}

#[derive(Default)]
struct ParkAutocommitOnQueryPlugin {
    armed: AtomicBool,
    entered: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
}

impl ParkAutocommitOnQueryPlugin {
    fn arm(&self) {
        *self.entered.0.lock().unwrap() = false;
        *self.release.0.lock().unwrap() = false;
        self.armed.store(true, AtomicOrdering::SeqCst);
    }

    fn wait_until_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut entered = self.entered.0.lock().unwrap();
        while !*entered {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, wait) = self.entered.1.wait_timeout(entered, remaining).unwrap();
            entered = next;
            if wait.timed_out() && !*entered {
                return false;
            }
        }
        true
    }

    fn release(&self) {
        let mut release = self.release.0.lock().unwrap();
        *release = true;
        self.release.1.notify_all();
    }
}

impl DatabasePlugin for ParkAutocommitOnQueryPlugin {
    fn on_query(&self, sql: &str) -> Result<()> {
        if self.armed.swap(false, AtomicOrdering::SeqCst) && sql.contains("INSERT INTO other") {
            {
                let mut entered = self.entered.0.lock().unwrap();
                *entered = true;
                self.entered.1.notify_all();
            }
            let mut release = self.release.0.lock().unwrap();
            while !*release {
                release = self.release.1.wait(release).unwrap();
            }
        }
        Ok(())
    }
}

// ---------- Shared helpers ----------

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn wait_until_trigger_wait_observed(db: &Database, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if db
            .trigger_progress_telemetry_snapshot_for_test()
            .wait_observed
            >= 1
        {
            return true;
        }
        thread::sleep(Duration::from_millis(2));
    }
    false
}

fn row_id_for_uuid(db: &Database, table: &str, key: u128) -> contextdb_core::RowId {
    let expected = Value::Uuid(uuid(key));
    db.scan(table, db.snapshot())
        .unwrap_or_else(|err| panic!("scan {table} for row_id failed: {err:?}"))
        .into_iter()
        .find(|row| row.values.get("id") == Some(&expected))
        .unwrap_or_else(|| panic!("row {key:x} not found in {table}"))
        .row_id
}

/// Forward-progress watchdog. Watches the engine's process-wide commit-tick
/// counter instead of wall-clock: under heavy parallel-test load the body
/// thread can run wall-clock-slowly while the engine is making real progress
/// on other tests in the same process. Fires only when no commit has been
/// observed anywhere in this process for `CONTEXTDB_TEST_STALL_SECS` (default
/// 60), or when an absolute backstop `CONTEXTDB_TEST_TIMEOUT_SECS` is reached
/// (default 3600). Both thresholds are env-overridable. Individual
/// deadlock-guard tests still assert their own 2-second engine timeout
/// behavior independently.
///
/// Spawns the body in a thread; assertion panics rethrow as panics, not
/// timeouts.
fn run_with_timeout<F, T>(body: F) -> std::result::Result<T, &'static str>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let stall_secs = std::env::var("CONTEXTDB_TEST_STALL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);
    let backstop_secs = std::env::var("CONTEXTDB_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(3600);
    let handle = thread::spawn(body);
    let start = Instant::now();
    let mut last_tick = Database::global_commit_ticks_for_test();
    let mut last_tick_observed_at = Instant::now();
    while !handle.is_finished() {
        let now_tick = Database::global_commit_ticks_for_test();
        let now = Instant::now();
        if now_tick != last_tick {
            last_tick = now_tick;
            last_tick_observed_at = now;
        } else if now.duration_since(last_tick_observed_at) >= Duration::from_secs(stall_secs) {
            return Err("engine stalled (no commit ticks)");
        }
        if now.duration_since(start) >= Duration::from_secs(backstop_secs) {
            return Err("hard backstop reached");
        }
        thread::sleep(Duration::from_millis(50));
    }
    match handle.join() {
        Ok(value) => Ok(value),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Build an in-memory `Database` with a trigger declared on table `t` and
/// a callback parked on a barrier pair. The callback waits on `entered.wait()`
/// to signal it is active, then waits on `done.wait()` to release.
fn build_trigger_parked_db(entered: Arc<Barrier>, done: Arc<Barrier>) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
        .unwrap();
    let entered_cb = entered.clone();
    let done_cb = done.clone();
    db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
        entered_cb.wait();
        done_cb.wait();
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();
    db
}

/// Build an in-memory `Database` whose trigger callback parks on a barrier
/// pair AND open a tx on Thread B (the would-be commit/rollback/apply_changes
/// caller) BEFORE the callback fires. Returns the database and the pre-opened
/// tx-id for Thread B. The `entered`/`done` barriers behave the same as
/// `build_trigger_parked_db`.
///
/// Use this helper when the test needs to hit the same-DB B2 contention on
/// `commit()`/`rollback()`/`apply_changes()` (as opposed to `begin()`):
/// Thread B already holds an open tx, so its commit-time path is the one
/// that races against Thread A's parked callback.
fn build_trigger_parked_with_open_tx(
    entered: Arc<Barrier>,
    done: Arc<Barrier>,
) -> (Arc<Database>, contextdb_core::TxId) {
    let db = build_trigger_parked_db(entered, done);
    let tx_b = db.begin().expect("Thread B opens tx before callback fires");
    (db, tx_b)
}

/// Build an in-memory `Database` with a paused cron tickler and a cron callback
/// parked on a barrier pair. Tests explicitly drive the callback with
/// `cron_run_due_now_for_test()`.
fn build_cron_parked_db(
    entered: Arc<Barrier>,
    done: Arc<Barrier>,
) -> (Arc<Database>, contextdb_engine::CronPauseGuard) {
    let db = Arc::new(Database::open_memory());
    let pause = db.pause_cron_tickler_for_test();
    db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
        .unwrap();
    let entered_cb = entered.clone();
    let done_cb = done.clone();
    db.register_cron_callback("cb", move |_db_handle| {
        entered_cb.wait();
        done_cb.wait();
        Ok(())
    })
    .unwrap();
    (db, pause)
}

/// Spawn a thread that fires the parked trigger by INSERTing into table `t`.
/// Returns the join handle so callers can confirm the firing tx committed.
fn fire_trigger_in_thread(db: Arc<Database>, key: u128) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let tx = db.begin()?;
        db.execute_in_tx(
            tx,
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
        )?;
        db.commit(tx)
    })
}

// ---------- Tracing subscriber ----------
//
// `T30_WARN_COUNT`, `T30_COUNT_ENABLED`, `T30_LAST_WARN_FIELDS`,
// `t30_install_global_subscriber`, and `t30_reset_warn_counters` are imported
// from `crate::common_tracing` (see Stub 2 above). The subscriber is shared
// with §29's `t37_*` siblings; both suites' counters dispatch from one
// `set_global_default`.

// `EnvGuard` (above) is exported via `pub(crate)` so the §29 t37
// deadlock-guard sibling and any future env-mutating tests can reuse it.
// Any test that intentionally parks a callback must either:
// - use `AtomicReleaseGuard` for an atomic release flag, or
// - arm `BarrierReleaseGuard` immediately after the callback reaches its
//   `entered` barrier and before any RED assertion can panic.
// Assertions that are expected to fail at stub time are evaluated only after
// the callback has been released and all spawned callback/probe threads have
// been joined.

// ---------- Test bodies follow in the per-test code section ----------

#[test]
#[serial]
fn t30_01_sustained_writer_flood_50_threads_commits_under_lens7_baseline() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE observation (id UUID PRIMARY KEY, writer INTEGER, seq INTEGER)", &empty()).unwrap();
        db.execute("CREATE TRIGGER tr ON observation WHEN INSERT", &empty()).unwrap();
        let first_callback_seen = Arc::new(AtomicBool::new(false));
        let first_callback_entered = Arc::new(AtomicBool::new(false));
        let release_first_callback = Arc::new(AtomicBool::new(false));
        let _release_guard = AtomicReleaseGuard(release_first_callback.clone());
        let first_callback_seen_cb = first_callback_seen.clone();
        let first_callback_entered_cb = first_callback_entered.clone();
        let release_first_callback_cb = release_first_callback.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            if !first_callback_seen_cb.swap(true, AtomicOrdering::SeqCst) {
                first_callback_entered_cb.store(true, AtomicOrdering::SeqCst);
                while !release_first_callback_cb.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        }).unwrap();
        db.complete_initialization().unwrap();

        let barrier = Arc::new(Barrier::new(51));
        let stop = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_typed_err_same_db = Arc::new(AtomicUsize::new(0));

        let writers = (0..50)
            .map(|writer| {
                let db = db.clone();
                let barrier = barrier.clone();
                let stop = stop.clone();
                let committed = committed.clone();
                let attempts = attempts.clone();
                let observed_typed_err_same_db = observed_typed_err_same_db.clone();
                thread::spawn(move || {
                    barrier.wait();
                    while !stop.load(AtomicOrdering::SeqCst) {
                        let seq = attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        let tx = match db.begin() {
                            Ok(tx) => tx,
                            Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                                observed_typed_err_same_db.fetch_add(1, AtomicOrdering::SeqCst);
                                continue;
                            }
                            Err(other) => panic!("writer {writer} got unexpected: {other:?}"),
                        };
                        let key = (writer as u128) * 0x100000 + seq as u128;
                        if db.execute_in_tx(
                            tx,
                            "INSERT INTO observation (id, writer, seq) VALUES ($id, $w, $s)",
                            &HashMap::from([
                                ("id".to_string(), Value::Uuid(uuid(key))),
                                ("w".to_string(), Value::Int64(writer as i64)),
                                ("s".to_string(), Value::Int64(seq as i64)),
                            ]),
                        ).is_ok() && db.commit(tx).is_ok() {
                            committed.fetch_add(1, AtomicOrdering::SeqCst);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        // 51st party: observer just synchronises the barrier (mirrors cg-evidence's reader thread shape).
        let observer_barrier = barrier.clone();
        let observer_committed = committed.clone();
        let observer_stop = stop.clone();
        let observer = thread::spawn(move || {
            observer_barrier.wait();
            while !observer_stop.load(AtomicOrdering::SeqCst)
                && observer_committed.load(AtomicOrdering::SeqCst) < 50
            {
                thread::yield_now();
            }
        });

        let entered_deadline = Instant::now() + Duration::from_secs(2);
        while !first_callback_entered.load(AtomicOrdering::SeqCst) {
            if Instant::now() >= entered_deadline {
                stop.store(true, AtomicOrdering::SeqCst);
                release_first_callback.store(true, AtomicOrdering::SeqCst);
                panic!("first trigger callback did not enter; contention setup failed");
            }
            thread::sleep(Duration::from_millis(1));
        }
        let overlap_deadline = Instant::now() + Duration::from_secs(1);
        while observed_typed_err_same_db.load(AtomicOrdering::SeqCst) == 0
            && db.trigger_progress_telemetry_snapshot_for_test().wait_observed == 0
            && Instant::now() < overlap_deadline
        {
            thread::sleep(Duration::from_millis(2));
        }
        let _overlap_observed = observed_typed_err_same_db.load(AtomicOrdering::SeqCst) > 0
            || db.trigger_progress_telemetry_snapshot_for_test().wait_observed > 0;
        release_first_callback.store(true, AtomicOrdering::SeqCst);

        // Generous acceptance deadline: this is a deadlock/progress guard, not
        // a throughput budget. The throughput ratio is enforced by the bench.
        let warmup_deadline = Instant::now() + Duration::from_secs(60);
        while committed.load(AtomicOrdering::SeqCst) < 50 {
            if Instant::now() >= warmup_deadline {
                stop.store(true, AtomicOrdering::SeqCst);
                for w in writers { let _ = w.join(); }
                let _ = observer.join();
                let observed = committed.load(AtomicOrdering::SeqCst);
                let typed = observed_typed_err_same_db.load(AtomicOrdering::SeqCst);
                panic!(
                    "writer flood failed to reach 50 commits within 60s; committed={observed}, typed_err_same_db={typed}"
                );
            }
            thread::yield_now();
        }
        observer.join().unwrap();
        stop.store(true, AtomicOrdering::SeqCst);
        for w in writers { w.join().unwrap(); }

        let typed = observed_typed_err_same_db.load(AtomicOrdering::SeqCst);
        assert_eq!(
            typed, 0,
            "no writer must observe Err(CallbackActiveCrossThread{{Trigger}}) for same-DB contention; saw {typed}"
        );

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        // The held first callback makes contention deterministic. This
        // sustained-flood test still treats `wait_observed` as informational;
        // the stricter load-bearing wait assertions live in t30_02/t30_25.
        // `wait_observed` is u64 — any value satisfies the informational
        // contract; bind it explicitly so the snapshot field is read (and
        // future field-renames get a compile error here).
        let _ = snap.wait_observed;
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "engine must report 0 same-DB typed-Err observations; saw {snap:?}"
        );
    })
    .expect("t30_01 deadlocked");
}

#[test]
#[serial]
fn t30_02_two_writer_interleave_same_db_thread_b_waits_then_proceeds() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3002);
        entered.wait(); // callback parked
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        // Thread B: begin() must wait and proceed (no typed-Err).
        let db_b = db.clone();
        let probe = thread::spawn(move || db_b.begin());

        // Wait until Thread B has actually parked before releasing the
        // callback. This makes `wait_observed` load-bearing instead of timing
        // based.
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut park_observed = false;
        while Instant::now() < deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 1 {
                park_observed = true;
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        // Release callback.
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "Thread B did not park before callback release; saw {snap_after_release:?}"
        );

        let result = probe.join().unwrap();
        match result {
            Ok(tx) => {
                let _ = db.rollback(tx);
            }
            Err(other) => panic!("Thread B expected Ok, got {other:?}"),
        }

        // Deterministic load-bearing assertion: the barrier guarantees Thread B
        // parked while Thread A's callback was held; the wait_observed counter
        // MUST register that park.
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "barrier-forced park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "barrier-forced park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_02 deadlocked");
}

#[test]
#[serial]
fn t30_02_commit_same_db_b2_waits_then_proceeds() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, tx_b) = build_trigger_parked_with_open_tx(entered.clone(), done.clone());

        // Thread A: fire the trigger so its callback parks at entered.wait().
        let fire = fire_trigger_in_thread(db.clone(), 0x3002C);
        entered.wait(); // callback parked
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        // Thread B: commit() must wait and proceed (no typed-Err).
        let db_b = db.clone();
        let probe = thread::spawn(move || db_b.commit(tx_b));

        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(1));
        // Release callback.
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        assert!(
            park_observed,
            "commit probe did not park before callback release"
        );

        let result = probe.join().unwrap();
        result.expect("Thread B commit() must wait-and-proceed Ok under same-DB B2");

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "commit-path park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "commit-path park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_02_commit deadlocked");
}

#[test]
#[serial]
fn t30_02_rollback_same_db_b2_waits_then_proceeds() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, tx_b) = build_trigger_parked_with_open_tx(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3002B);
        entered.wait(); // callback parked
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        // Thread B: rollback() must wait and proceed (no typed-Err).
        let db_b = db.clone();
        let probe = thread::spawn(move || db_b.rollback(tx_b));

        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(1));
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        assert!(
            park_observed,
            "rollback probe did not park before callback release"
        );

        let result = probe.join().unwrap();
        result.expect("Thread B rollback() must wait-and-proceed Ok under same-DB B2");

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "rollback-path park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "rollback-path park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_02_rollback deadlocked");
}

#[test]
#[serial]
fn t30_02_apply_changes_same_db_b2_waits_then_proceeds() {
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        // apply_changes does not consume a tx-id; it opens its own internal
        // tx. Use the standard parked-callback DB (no pre-opened tx needed).
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3002A);
        entered.wait(); // callback parked
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        // Thread B: apply_changes() must wait and proceed under same-DB B2.
        // The same-DB B2 gate (assert_public_tx_control_callbacks_allowed) is
        // walked regardless of changeset payload — an empty ChangeSet
        // exercises the gate the same as a populated one (mirrors t30_09's
        // Class A reentry probe shape).
        let changes = ChangeSet::default();
        let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
        let db_b = db.clone();
        let probe = thread::spawn(move || db_b.apply_changes(changes, &policies));

        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(1));
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        assert!(
            park_observed,
            "apply_changes probe did not park before callback release"
        );

        let result = probe.join().unwrap();
        result.expect("Thread B apply_changes() must wait-and-proceed Ok under same-DB B2");

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "apply_changes-path park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "apply_changes-path park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_02_apply_changes deadlocked");
}

#[test]
#[serial]
fn t30_03_two_writer_interleave_cross_db_dby_no_callback_proceeds_without_engagement() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db_x = build_trigger_parked_db(entered.clone(), done.clone());

        // db_y has no callback; it must remain unaffected by db_x's parked callback.
        let db_y = Arc::new(Database::open_memory());

        let fire = fire_trigger_in_thread(db_x.clone(), 0x3003);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        let tx = db_y
            .begin()
            .expect("db_y must remain independent of db_x's parked callback");
        let _ = db_y.rollback(tx);

        // Cross-DB independence positive-control: db_y has no callback and
        // is in a separate concurrency domain from db_x. A regression that
        // ties the wait primitive to process-wide state would engage db_y's
        // wait counter even though db_y is unrelated. Pin zero engagement.
        assert_eq!(
            db_y.trigger_progress_telemetry_snapshot_for_test()
                .wait_observed,
            0,
            "DB-Y must not engage wait when DB-X has the active callback"
        );

        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
    })
    .expect("t30_03 deadlocked");
}

#[test]
#[serial]
fn t30_03b_two_writer_interleave_cross_db_non_owner_probe_proceeds_without_engagement() {
    run_with_timeout(|| {
        let db_y = Arc::new(Database::open_memory());

        let entered_x = Arc::new(Barrier::new(2));
        let done_x = Arc::new(Barrier::new(2));
        let db_x = build_trigger_parked_db(entered_x.clone(), done_x.clone());

        let fire_x = fire_trigger_in_thread(db_x.clone(), 0x3003B1);
        entered_x.wait();
        let mut done_x_guard = BarrierReleaseGuard::armed(done_x.clone());

        // Thread C: begin() on db_y from a thread that is not db_y's owner.
        // DB-Y is an unrelated concurrency domain, so DB-X's parked trigger
        // must not leak a process-wide typed-Err into ordinary worker use.
        let db_y_probe = db_y.clone();
        let result = thread::spawn(move || db_y_probe.begin()).join().unwrap();
        let tx = result.expect("cross-DB non-owner DB-Y begin must proceed");
        db_y.rollback(tx).expect("rollback DB-Y tx");

        done_x.wait();
        done_x_guard.disarm();
        fire_x.join().unwrap().unwrap();
        let snap = db_y.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(
            snap.deadlock_guard_timeout_observed, 0,
            "unrelated DB-Y work must not increment deadlock-guard counter; saw {snap:?}"
        );
    })
    .expect("t30_03b deadlocked");
}

#[test]
#[serial]
fn t30_04_no_trigger_callback_baseline_throughput_n32_writers() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, writer INTEGER, seq INTEGER)",
            &empty(),
        )
        .unwrap();
        // No trigger registered. This is the baseline.

        let n_writers = 32;
        let commits_per_writer = 16;
        let committed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n_writers);
        for w in 0..n_writers {
            let db = db.clone();
            let committed = committed.clone();
            handles.push(thread::spawn(move || {
                for s in 0..commits_per_writer {
                    let tx = db.begin().expect("baseline begin");
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO t (id, writer, seq) VALUES ($id, $w, $s)",
                        &HashMap::from([
                            (
                                "id".to_string(),
                                Value::Uuid(uuid((w as u128) * 0x10000 + s as u128)),
                            ),
                            ("w".to_string(), Value::Int64(w as i64)),
                            ("s".to_string(), Value::Int64(s as i64)),
                        ]),
                    )
                    .expect("baseline insert");
                    db.commit(tx).expect("baseline commit");
                    committed.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            committed.load(AtomicOrdering::SeqCst),
            n_writers * commits_per_writer
        );

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(
            snap.wait_observed, 0,
            "no callback registered → no wait engagement; snap={snap:?}"
        );
    })
    .expect("t30_04 deadlocked");
}

#[test]
#[serial]
fn t30_05_sustained_writer_flood_n32_with_no_op_trigger_all_commit() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, writer INTEGER, seq INTEGER)",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let first_callback_seen = Arc::new(AtomicBool::new(false));
        let first_callback_entered = Arc::new(AtomicBool::new(false));
        let release_first_callback = Arc::new(AtomicBool::new(false));
        let _release_guard = AtomicReleaseGuard(release_first_callback.clone());
        let first_callback_seen_cb = first_callback_seen.clone();
        let first_callback_entered_cb = first_callback_entered.clone();
        let release_first_callback_cb = release_first_callback.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            if !first_callback_seen_cb.swap(true, AtomicOrdering::SeqCst) {
                first_callback_entered_cb.store(true, AtomicOrdering::SeqCst);
                while !release_first_callback_cb.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let n_writers = 32;
        let commits_per_writer = 16;
        let writer_barrier = Arc::new(Barrier::new(n_writers + 1));
        let committed = Arc::new(AtomicUsize::new(0));
        let typed_err_same_db = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n_writers);
        for w in 0..n_writers {
            let db = db.clone();
            let writer_barrier = writer_barrier.clone();
            let committed = committed.clone();
            let typed_err_same_db = typed_err_same_db.clone();
            handles.push(thread::spawn(move || {
                writer_barrier.wait();
                for s in 0..commits_per_writer {
                    let tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(Error::CallbackActiveCrossThread {
                            kind: CallbackKind::Trigger,
                        }) => {
                            typed_err_same_db.fetch_add(1, AtomicOrdering::SeqCst);
                            continue;
                        }
                        Err(other) => panic!("unexpected: {other:?}"),
                    };
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO t (id, writer, seq) VALUES ($id, $w, $s)",
                        &HashMap::from([
                            (
                                "id".to_string(),
                                Value::Uuid(uuid((w as u128) * 0x10000 + s as u128)),
                            ),
                            ("w".to_string(), Value::Int64(w as i64)),
                            ("s".to_string(), Value::Int64(s as i64)),
                        ]),
                    )
                    .expect("insert");
                    db.commit(tx).expect("commit");
                    committed.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }));
        }
        writer_barrier.wait();
        let entered_deadline = Instant::now() + Duration::from_secs(2);
        while !first_callback_entered.load(AtomicOrdering::SeqCst) {
            if Instant::now() >= entered_deadline {
                release_first_callback.store(true, AtomicOrdering::SeqCst);
                panic!("first trigger callback did not enter; contention setup failed");
            }
            thread::sleep(Duration::from_millis(1));
        }
        let overlap_deadline = Instant::now() + Duration::from_secs(1);
        while typed_err_same_db.load(AtomicOrdering::SeqCst) == 0
            && db
                .trigger_progress_telemetry_snapshot_for_test()
                .wait_observed
                == 0
            && Instant::now() < overlap_deadline
        {
            thread::sleep(Duration::from_millis(2));
        }
        let overlap_observed = typed_err_same_db.load(AtomicOrdering::SeqCst) > 0
            || db
                .trigger_progress_telemetry_snapshot_for_test()
                .wait_observed
                > 0;
        release_first_callback.store(true, AtomicOrdering::SeqCst);
        for h in handles {
            h.join().unwrap();
        }
        assert!(
            overlap_observed,
            "first callback was held but no writer reached the contention gate before release"
        );

        let observed_typed_err = typed_err_same_db.load(AtomicOrdering::SeqCst);
        assert_eq!(
            observed_typed_err, 0,
            "no writer may observe same-DB B2 typed-Err under §30; saw {observed_typed_err}"
        );
        assert_eq!(
            committed.load(AtomicOrdering::SeqCst),
            n_writers * commits_per_writer
        );

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        let _ = snap.wait_observed; // informational for no-op callbacks; deterministic parks are asserted elsewhere.
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "engine must report 0 same-DB typed-Err observations; snap={snap:?}"
        );
        assert_eq!(
            snap.deadlock_guard_timeout_observed, 0,
            "no healthy contention may trip deadlock guard; snap={snap:?}"
        );
    })
    .expect("t30_05 deadlocked");
}

#[test]
#[serial]
fn t30_06_class_a_reentry_begin_inside_trigger_callback_returns_callback_reentry_trigger() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_outer.begin());
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3006)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected CallbackReentry{{Trigger}}, got {observed:?}"
        );
        // Firing-tx commit-survival: the callback returned Err for the inner
        // tx-control surface, but the outer firing INSERT must still commit.
        let snap = db.snapshot();
        assert_eq!(
            db.scan("t", snap).unwrap().len(),
            1,
            "firing INSERT must commit even though the callback's inner tx-control returned Err"
        );
    })
    .expect("t30_06 deadlocked");
}

#[test]
#[serial]
fn t30_07_class_a_reentry_commit_inside_trigger_callback_returns_callback_reentry_trigger() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let setup_tx = db.begin().expect("setup tx");
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_outer.commit(setup_tx));
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3007)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected CallbackReentry{{Trigger}} on commit, got {observed:?}"
        );
        // Firing-tx commit-survival: the callback's inner commit() returned
        // Err but the outer firing INSERT must still commit.
        let snap = db.snapshot();
        assert_eq!(
            db.scan("t", snap).unwrap().len(),
            1,
            "firing INSERT must commit despite callback's inner commit() Err"
        );
        let _ = db.rollback(setup_tx);
    })
    .expect("t30_07 deadlocked");
}

#[test]
#[serial]
fn t30_08_class_a_reentry_rollback_inside_trigger_callback_returns_callback_reentry_trigger() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let setup_tx = db.begin().expect("setup tx");
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_outer.rollback(setup_tx));
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3008)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected CallbackReentry{{Trigger}} on rollback, got {observed:?}"
        );
        // Firing-tx commit-survival.
        let snap = db.snapshot();
        assert_eq!(
            db.scan("t", snap).unwrap().len(),
            1,
            "firing INSERT must commit despite callback's inner rollback() Err"
        );
        let _ = db.rollback(setup_tx);
    })
    .expect("t30_08 deadlocked");
}

#[test]
#[serial]
fn t30_09_class_a_reentry_apply_changes_inside_trigger_callback_returns_callback_reentry_trigger() {
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<contextdb_engine::ApplyResult>>>> =
            Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_outer.apply_changes(
                ChangeSet::default(),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            ));
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3009)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected CallbackReentry{{Trigger}} on apply_changes, got {observed:?}"
        );
        // Firing-tx commit-survival.
        let snap = db.snapshot();
        assert_eq!(
            db.scan("t", snap).unwrap().len(),
            1,
            "firing INSERT must commit despite callback's inner apply_changes() Err"
        );
    })
    .expect("t30_09 deadlocked");
}

#[test]
#[serial]
fn t30_10_class_a_reentry_close_inside_trigger_callback_returns_callback_reentry_trigger() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_outer.close());
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3010)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected CallbackReentry{{Trigger}} on close, got {observed:?}"
        );
        // Firing-tx commit-survival: callback's close() Err must not invalidate
        // the firing INSERT.
        let snap = db.snapshot();
        assert_eq!(
            db.scan("t", snap).unwrap().len(),
            1,
            "firing INSERT must commit despite callback's inner close() Err"
        );
        // db remains open (close was rejected).
        let tx = db.begin().expect("db still open");
        let _ = db.rollback(tx);
    })
    .expect("t30_10 deadlocked");
}

#[test]
#[serial]
fn t30_11_cross_db_write_from_inside_callback_returns_callback_reentry_trigger() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        // db_y is a completely separate concurrency domain.
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();

        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_y_for_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            // Synchronous tx-control on db_y from inside db_x's callback. Cross-DB-from-callback Class A.
            *observed_cb.lock().unwrap() = Some(db_y_for_cb.begin());
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();

        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011)))]),
        )
        .unwrap();

        let observed = observed.lock().unwrap().take().expect("callback ran");
        match observed {
            Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!(
                "expected cross-DB-from-callback Class A CallbackReentry{{Trigger}}, got {other:?}"
            ),
        }
        // Firing-tx commit-survival on db_x: the callback's cross-DB call to
        // db_y returned Err (cross-DB-from-callback Class A); the firing
        // INSERT on db_x must still commit.
        let snap_x = db_x.snapshot();
        assert_eq!(
            db_x.scan("t", snap_x).unwrap().len(),
            1,
            "firing INSERT on db_x must commit despite cross-DB-from-callback Class A Err"
        );
    })
    .expect("t30_11 deadlocked");
}

#[test]
#[serial]
fn t30_11_execute_from_trigger_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE y (id UUID PRIMARY KEY, label TEXT)", &empty())
            .unwrap();
        db_y.execute(
            "INSERT INTO y (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0x3011E8))),
                (
                    "label".to_string(),
                    Value::Text("update-target".to_string()),
                ),
            ]),
        )
        .unwrap();
        db_y.execute(
            "INSERT INTO y (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0x3011E9))),
                (
                    "label".to_string(),
                    Value::Text("delete-target".to_string()),
                ),
            ]),
        )
        .unwrap();
        let observed: LabeledQueryResults = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_y_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            let mut out = observed_cb.lock().unwrap();
            out.push((
                "insert",
                db_y_cb.execute(
                    "INSERT INTO y (id, label) VALUES ($id, $label)",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(uuid(0x3011E))),
                        ("label".to_string(), Value::Text("insert".to_string())),
                    ]),
                ),
            ));
            out.push((
                "update",
                db_y_cb.execute(
                    "UPDATE y SET label = $label WHERE id = $id",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(uuid(0x3011E8))),
                        ("label".to_string(), Value::Text("updated".to_string())),
                    ]),
                ),
            ));
            out.push((
                "delete",
                db_y_cb.execute(
                    "DELETE FROM y WHERE id = $id",
                    &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E9)))]),
                ),
            ));
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();
        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E0)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 3, "all execute DML probes ran");
        for (name, result) in observed.iter() {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Trigger
                    })
                ),
                "expected typed trigger CallbackReentry for cross-DB execute {name}, got {result:?}"
            );
        }
        assert_eq!(
            db_y.scan("y", db_y.snapshot()).unwrap().len(),
            2,
            "db_y writes must not stage"
        );
    })
    .expect("t30_11 trigger execute deadlocked");
}

#[test]
#[serial]
fn t30_11_execute_in_tx_from_trigger_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty()).unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE y (id UUID PRIMARY KEY, label TEXT)", &empty()).unwrap();
        db_y.execute("INSERT INTO y (id, label) VALUES ($id, $label)", &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0x3011EA))),
            ("label".to_string(), Value::Text("update-target".to_string())),
        ])).unwrap();
        db_y.execute("INSERT INTO y (id, label) VALUES ($id, $label)", &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0x3011EB))),
            ("label".to_string(), Value::Text("delete-target".to_string())),
        ])).unwrap();
        let tx_y = db_y.begin().expect("setup db_y tx");
        let observed: LabeledQueryResults = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_y_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            let mut out = observed_cb.lock().unwrap();
            out.push(("insert", db_y_cb.execute_in_tx(
                tx_y,
                "INSERT INTO y (id, label) VALUES ($id, $label)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x3011E1))),
                    ("label".to_string(), Value::Text("insert".to_string())),
                ]),
            )));
            out.push(("update", db_y_cb.execute_in_tx(
                tx_y,
                "UPDATE y SET label = $label WHERE id = $id",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x3011EA))),
                    ("label".to_string(), Value::Text("updated".to_string())),
                ]),
            )));
            out.push(("delete", db_y_cb.execute_in_tx(
                tx_y,
                "DELETE FROM y WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011EB)))]),
            )));
            Ok(())
        }).unwrap();
        db_x.complete_initialization().unwrap();
        db_x.execute("INSERT INTO t (id) VALUES ($id)", &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E2)))])).unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 3, "all execute_in_tx DML probes ran");
        for (name, result) in observed.iter() {
            assert!(matches!(result, Err(Error::CallbackReentry { kind: CallbackKind::Trigger })),
                "expected typed trigger CallbackReentry for cross-DB execute_in_tx {name}, got {result:?}");
        }
        assert_eq!(db_y.scan("y", db_y.snapshot()).unwrap().len(), 2, "db_y rows must remain unchanged");
        db_y.rollback(tx_y).expect("setup tx remains rollback-able");
    }).expect("t30_11 trigger execute_in_tx deadlocked");
}

#[test]
#[serial]
fn t30_11_direct_helpers_from_trigger_callback_against_other_db_return_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty()).unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE y (id UUID PRIMARY KEY)", &empty()).unwrap();
        db_y.execute("CREATE TABLE v (id UUID PRIMARY KEY, embedding VECTOR(3))", &empty()).unwrap();
        db_y.execute("INSERT INTO y (id) VALUES ($id)", &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E8)))])).unwrap();
        db_y.execute("INSERT INTO v (id) VALUES ($id)", &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E3)))])).unwrap();
        let delete_row = row_id_for_uuid(&db_y, "y", 0x3011E8);
        let vector_row = row_id_for_uuid(&db_y, "v", 0x3011E3);
        let tx_y = db_y.begin().expect("setup db_y tx");
        let observed: LabeledUnitResults = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_y_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            let mut out = observed_cb.lock().unwrap();
            out.push(("insert_row", db_y_cb.insert_row(tx_y, "y", HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E4)))])).map(|_| ())));
            out.push(("upsert_row", db_y_cb.upsert_row(tx_y, "y", "id", HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E5)))])).map(|_| ())));
            out.push(("delete_row", db_y_cb.delete_row(tx_y, "y", delete_row)));
            out.push(("insert_edge", db_y_cb.insert_edge(tx_y, uuid(0x3011E5), uuid(0x3011E6), "x".to_string(), HashMap::new()).map(|_| ())));
            out.push(("delete_edge", db_y_cb.delete_edge(tx_y, uuid(0x3011E5), uuid(0x3011E6), "x")));
            out.push(("insert_vector", db_y_cb.insert_vector(tx_y, VectorIndexRef::new("v", "embedding"), vector_row, vec![0.1, 0.2, 0.3])));
            out.push(("delete_vector", db_y_cb.delete_vector(tx_y, VectorIndexRef::new("v", "embedding"), vector_row)));
            Ok(())
        }).unwrap();
        db_x.complete_initialization().unwrap();
        db_x.execute("INSERT INTO t (id) VALUES ($id)", &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011E7)))])).unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 7, "all direct helper probes ran");
        for (name, result) in observed.iter() {
            assert!(matches!(result, Err(Error::CallbackReentry { kind: CallbackKind::Trigger })),
                "expected typed trigger CallbackReentry for cross-DB direct helper {name}, got {result:?}");
        }
        db_y.rollback(tx_y).expect("setup tx remains rollback-able");
    }).expect("t30_11 trigger direct helpers deadlocked");
}

#[test]
#[serial]
fn t30_11_execute_from_cron_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        let _pause = db_x.pause_cron_tickler_for_test();
        db_x.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE y (id UUID PRIMARY KEY, label TEXT)", &empty())
            .unwrap();
        db_y.execute(
            "INSERT INTO y (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0x3011C8))),
                (
                    "label".to_string(),
                    Value::Text("update-target".to_string()),
                ),
            ]),
        )
        .unwrap();
        db_y.execute(
            "INSERT INTO y (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0x3011C9))),
                (
                    "label".to_string(),
                    Value::Text("delete-target".to_string()),
                ),
            ]),
        )
        .unwrap();
        let observed: LabeledQueryResults = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_y_cb = db_y.clone();
        db_x.register_cron_callback("cb", move |_db_x_handle| {
            let mut out = observed_cb.lock().unwrap();
            out.push((
                "insert",
                db_y_cb.execute(
                    "INSERT INTO y (id) VALUES ($id)",
                    &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C0)))]),
                ),
            ));
            out.push((
                "update",
                db_y_cb.execute(
                    "UPDATE y SET label = $label WHERE id = $id",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(uuid(0x3011C8))),
                        ("label".to_string(), Value::Text("updated".to_string())),
                    ]),
                ),
            ));
            out.push((
                "delete",
                db_y_cb.execute(
                    "DELETE FROM y WHERE id = $id",
                    &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C9)))]),
                ),
            ));
            Ok(())
        })
        .unwrap();
        db_x.cron_run_due_now_for_test().expect("cron tick");
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 3, "all cron execute DML probes ran");
        for (name, result) in observed.iter() {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Cron
                    })
                ),
                "expected typed cron CallbackReentry for cross-DB execute {name}, got {result:?}"
            );
        }
        assert_eq!(
            db_y.scan("y", db_y.snapshot()).unwrap().len(),
            2,
            "db_y writes must not stage"
        );
    })
    .expect("t30_11 cron execute deadlocked");
}

#[test]
#[serial]
fn t30_11_execute_in_tx_from_cron_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        let _pause = db_x.pause_cron_tickler_for_test();
        db_x.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty()).unwrap();
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE y (id UUID PRIMARY KEY, label TEXT)", &empty()).unwrap();
        db_y.execute("INSERT INTO y (id, label) VALUES ($id, $label)", &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0x3011CA))),
            ("label".to_string(), Value::Text("update-target".to_string())),
        ])).unwrap();
        db_y.execute("INSERT INTO y (id, label) VALUES ($id, $label)", &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0x3011CB))),
            ("label".to_string(), Value::Text("delete-target".to_string())),
        ])).unwrap();
        let tx_y = db_y.begin().expect("setup db_y tx");
        let observed: LabeledQueryResults = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_y_cb = db_y.clone();
        db_x.register_cron_callback("cb", move |_db_x_handle| {
            let mut out = observed_cb.lock().unwrap();
            out.push(("insert", db_y_cb.execute_in_tx(
                tx_y,
                "INSERT INTO y (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C1)))])
            )));
            out.push(("update", db_y_cb.execute_in_tx(
                tx_y,
                "UPDATE y SET label = $label WHERE id = $id",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x3011CA))),
                    ("label".to_string(), Value::Text("updated".to_string())),
                ]),
            )));
            out.push(("delete", db_y_cb.execute_in_tx(
                tx_y,
                "DELETE FROM y WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011CB)))]),
            )));
            Ok(())
        }).unwrap();
        db_x.cron_run_due_now_for_test().expect("cron tick");
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 3, "all cron execute_in_tx DML probes ran");
        for (name, result) in observed.iter() {
            assert!(matches!(result, Err(Error::CallbackReentry { kind: CallbackKind::Cron })),
                "expected typed cron CallbackReentry for cross-DB execute_in_tx {name}, got {result:?}");
        }
        assert_eq!(db_y.scan("y", db_y.snapshot()).unwrap().len(), 2, "db_y rows must remain unchanged");
        db_y.rollback(tx_y).expect("setup tx remains rollback-able");
    }).expect("t30_11 cron execute_in_tx deadlocked");
}

#[test]
#[serial]
fn t30_11_direct_helpers_from_cron_callback_against_other_db_return_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        let _pause = db_x.pause_cron_tickler_for_test();
        db_x.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty()).unwrap();
        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE y (id UUID PRIMARY KEY)", &empty()).unwrap();
        db_y.execute("CREATE TABLE v (id UUID PRIMARY KEY, embedding VECTOR(3))", &empty()).unwrap();
        db_y.execute("INSERT INTO y (id) VALUES ($id)", &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011CC)))])).unwrap();
        db_y.execute("INSERT INTO v (id) VALUES ($id)", &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C2)))])).unwrap();
        let delete_row = row_id_for_uuid(&db_y, "y", 0x3011CC);
        let vector_row = row_id_for_uuid(&db_y, "v", 0x3011C2);
        let tx_y = db_y.begin().expect("setup db_y tx");
        let observed: LabeledUnitResults = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_y_cb = db_y.clone();
        db_x.register_cron_callback("cb", move |_db_x_handle| {
            let mut out = observed_cb.lock().unwrap();
            out.push(("insert_row", db_y_cb.insert_row(tx_y, "y", HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C3)))])).map(|_| ())));
            out.push(("upsert_row", db_y_cb.upsert_row(tx_y, "y", "id", HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C4)))])).map(|_| ())));
            out.push(("delete_row", db_y_cb.delete_row(tx_y, "y", delete_row)));
            out.push(("insert_edge", db_y_cb.insert_edge(tx_y, uuid(0x3011C4), uuid(0x3011C5), "x".to_string(), HashMap::new()).map(|_| ())));
            out.push(("delete_edge", db_y_cb.delete_edge(tx_y, uuid(0x3011C4), uuid(0x3011C5), "x")));
            out.push(("insert_vector", db_y_cb.insert_vector(tx_y, VectorIndexRef::new("v", "embedding"), vector_row, vec![0.1, 0.2, 0.3])));
            out.push(("delete_vector", db_y_cb.delete_vector(tx_y, VectorIndexRef::new("v", "embedding"), vector_row)));
            Ok(())
        }).unwrap();
        db_x.cron_run_due_now_for_test().expect("cron tick");
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 7, "all direct helper probes ran");
        for (name, result) in observed.iter() {
            assert!(matches!(result, Err(Error::CallbackReentry { kind: CallbackKind::Cron })),
                "expected typed cron CallbackReentry for cross-DB direct helper {name}, got {result:?}");
        }
        db_y.rollback(tx_y).expect("setup tx remains rollback-able");
    }).expect("t30_11 cron direct helpers deadlocked");
}

#[test]
#[serial]
fn t30_12_spawned_detached_thread_from_inside_callback_writes_other_db_succeeds() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();

        // The spawn happens INSIDE the callback body, but the spawned thread
        // waits until the callback has returned before touching db_y. This
        // isolates the contract under test: thread-local callback identity
        // must not propagate through `thread::spawn`. If the spawned thread
        // wrote db_y while db_x's callback was still active, it would be
        // ordinary unrelated DB-Y work; synchronous same-thread cross-DB calls
        // remain Class A and are covered above.
        let spawned_handle: Arc<Mutex<Option<thread::JoinHandle<Result<()>>>>> =
            Arc::new(Mutex::new(None));
        let start_spawned = Arc::new(AtomicBool::new(false));
        let _start_guard = AtomicReleaseGuard(start_spawned.clone());
        let spawned_handle_cb = spawned_handle.clone();
        let db_y_for_cb = db_y.clone();
        let start_spawned_cb = start_spawned.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            let db_y_inner = db_y_for_cb.clone();
            let start_spawned_inner = start_spawned_cb.clone();
            let h = thread::spawn(move || {
                while !start_spawned_inner.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
                // Spawned thread does NOT inherit thread-locals → normal writer
                // on db_y after db_x's callback-active scope has exited.
                let tx = db_y_inner.begin()?;
                db_y_inner.execute_in_tx(
                    tx,
                    "INSERT INTO t (id) VALUES ($id)",
                    &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3012)))]),
                )?;
                db_y_inner.commit(tx)
            });
            *spawned_handle_cb.lock().unwrap() = Some(h);
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();

        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3012A)))]),
        )
        .unwrap();

        start_spawned.store(true, AtomicOrdering::SeqCst);
        let h = spawned_handle
            .lock()
            .unwrap()
            .take()
            .expect("spawned thread");
        h.join()
            .unwrap()
            .expect("spawned thread's commit on db_y must succeed");

        // Verify the spawned write landed on db_y.
        let snap = db_y.snapshot();
        let rows = db_y.scan("t", snap).expect("scan db_y");
        assert!(
            rows.iter()
                .any(|r| r.values.get("id") == Some(&Value::Uuid(uuid(0x3012)))),
            "spawned thread's INSERT must be visible on db_y"
        );
        // Firing-tx commit-survival on db_x: the callback spawned a detached
        // thread (which is permitted) and returned Ok; the firing INSERT on
        // db_x must commit.
        let snap_x = db_x.snapshot();
        assert_eq!(
            db_x.scan("t", snap_x).unwrap().len(),
            1,
            "firing tx must commit despite spawned-thread cross-DB write"
        );
    })
    .expect("t30_12 deadlocked");
}

#[test]
#[serial]
fn t30_12b_captured_trigger_tx_execute_in_tx_read_from_wrong_thread_returns_typed_err() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let (tx_sender, tx_receiver) = mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let release_cb = release.clone();
        db.register_trigger_callback("tr", move |_db, ctx| {
            tx_sender.send(ctx.tx).unwrap();
            release_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let fire = fire_trigger_in_thread(db.clone(), 0x3012B);
        let captured_tx = tx_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("trigger callback did not expose tx");
        let mut release_guard = BarrierReleaseGuard::armed(release.clone());

        let db_probe = db.clone();
        let result = thread::spawn(move || db_probe.execute_in_tx(captured_tx, "SELECT * FROM t", &empty()))
            .join()
            .unwrap();
        match result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!(
                "captured trigger tx-bound handle used from another thread must return CallbackActiveCrossThread{{Trigger}}, got {other:?}"
            ),
        }

        release.wait();
        release_guard.disarm();
        fire.join().unwrap().unwrap();
    })
    .expect("t30_12b deadlocked");
}

#[test]
#[serial]
fn t30_13_trigger_inside_cron_callback_tx_nested_same_thread_same_db_atomic() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE host (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute(
            "CREATE TABLE cascade_target (id UUID PRIMARY KEY)",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE TRIGGER tr ON host WHEN INSERT", &empty())
            .unwrap();
        db.register_trigger_callback("tr", |db_handle, ctx| {
            // Trigger callback writes to a different table via its tx-bound handle.
            db_handle.execute_in_tx(
                ctx.tx,
                "INSERT INTO cascade_target (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3013A)))]),
            )?;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        db.execute("CREATE SCHEDULE s EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |db_handle| {
            db_handle.execute(
                "INSERT INTO host (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3013B)))]),
            )?;
            Ok(())
        })
        .unwrap();

        thread::sleep(Duration::from_millis(5));
        db.cron_run_due_now_for_test().expect("cron tick");

        let snap = db.snapshot();
        let host_rows = db.scan("host", snap).expect("scan host");
        let cascade_rows = db
            .scan("cascade_target", snap)
            .expect("scan cascade_target");
        assert_eq!(host_rows.len(), 1, "host row must commit");
        assert_eq!(
            cascade_rows.len(),
            1,
            "trigger-cascade row must commit atomically with cron's tx"
        );
    })
    .expect("t30_13 deadlocked");
}

#[test]
#[serial]
fn t30_14_b1_cron_active_same_db_returns_callback_active_cron_immediately() {
    run_with_timeout(|| {
        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered_cron.clone(), done_cron.clone());

        // Park cron callback on Thread A.
        let db_cron = db.clone();
        let cron_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            db_cron.cron_run_due_now_for_test().expect("cron tick");
        });
        entered_cron.wait();
        let mut done_cron_guard = BarrierReleaseGuard::armed(done_cron.clone());

        // Thread C: must observe B1 (Cron) immediately, NOT wait on any
        // trigger primitive. Public same-DB cron-active + trigger-active
        // simultaneity is unreachable because this same gate prevents the
        // public writer that would fire the trigger.
        let started = Instant::now();
        let result = db.begin();
        let elapsed = started.elapsed();
        match result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            }) => {}
            other => {
                panic!("expected B1 (Cron) typed-Err immediately, got {other:?} after {elapsed:?}")
            }
        }
        // Pin the "immediately" property — must return well within 1 second
        // (no wait engagement).
        assert!(
            elapsed < Duration::from_millis(1000),
            "B1 must return immediately, not engage a wait primitive; elapsed={elapsed:?}"
        );

        done_cron.wait();
        done_cron_guard.disarm();
        cron_thread.join().unwrap();
    })
    .expect("t30_14 deadlocked");
}

#[test]
#[serial]
fn t30_14b_cron_trigger_dispatch_failure_rolls_back_active_tx_while_guard_retained() {
    run_with_timeout(|| {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        let db = Arc::new(Database::open_memory_with_accountant(accountant.clone()));
        let _pause = db.pause_cron_tickler_for_test();
        db.execute(
            "CREATE TABLE host (id UUID PRIMARY KEY, payload TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE TRIGGER tr ON host WHEN INSERT", &empty())
            .unwrap();
        db.register_trigger_callback("tr", |_db_handle, _ctx| {
            Err(Error::Other("trigger veto from cron cleanup regression".to_string()))
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute("CREATE SCHEDULE s EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |db_handle| {
            db_handle.execute(
                "INSERT INTO host (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x3014B))),
                    ("payload".to_string(), Value::Text("x".repeat(4096))),
                ]),
            )?;
            Ok(())
        })
        .unwrap();

        let baseline_used = accountant.usage().used;
        let successful = db.cron_run_due_now_for_test().expect("cron tick should audit failure");
        assert_eq!(successful, 0, "failed cron callback must not count as fired");

        let audits = db.cron_audit_log_for_test();
        assert!(
            audits.iter().any(|entry| {
                matches!(
                    &entry.kind,
                    CronAuditKind::Failed(message)
                        if message.contains("trigger veto from cron cleanup regression")
                )
            }),
            "cron failure audit must retain trigger callback error; audits={audits:?}"
        );
        assert_eq!(
            db.scan("host", db.snapshot()).expect("scan host").len(),
            0,
            "failed cron-trigger transaction must not leave committed rows"
        );
        assert_eq!(
            accountant.usage().used,
            baseline_used,
            "failed cron-trigger transaction must release pending row allocations while trigger guard is retained"
        );
    })
    .expect("t30_14b cron trigger failure cleanup test deadlocked");
}

#[test]
#[serial]
fn t30_15_schema_agnosticity_relational_graph_vector_cascade_in_callback_n_writers_all_commit() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        // Relational table (the firing table) + cascade target relational table.
        db.execute("CREATE TABLE host (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute(
            "CREATE TABLE cascade_rel (id UUID PRIMARY KEY, host_id UUID)",
            &empty(),
        )
        .unwrap();
        // Vector index on cascade_rel.embedding (declared via a separate column).
        db.execute(
            "CREATE TABLE vec_target (id UUID PRIMARY KEY, embedding VECTOR(4))",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE TRIGGER tr ON host WHEN INSERT", &empty())
            .unwrap();

        // Set up a static node for the graph cascade (engine-managed graph nodes are
        // constructed implicitly via insert_edge once both endpoints are set).
        let anchor_node: NodeId = uuid(0x301FA);

        db.register_trigger_callback("tr", move |db_handle, ctx| {
            // 1. Relational cascade.
            db_handle.execute_in_tx(
                ctx.tx,
                "INSERT INTO cascade_rel (id, host_id) VALUES ($id, $host)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    (
                        "host".to_string(),
                        ctx.row_values.get("id").cloned().unwrap_or(Value::Null),
                    ),
                ]),
            )?;
            // 2. Graph edge cascade.
            let target: NodeId = Uuid::new_v4();
            let edge_type: EdgeType = "host_to_anchor".to_string();
            let _ =
                db_handle.insert_edge(ctx.tx, anchor_node, target, edge_type, HashMap::new())?;
            // 3. Vector cascade.
            let row_id_uuid = Uuid::new_v4();
            db_handle.execute_in_tx(
                ctx.tx,
                "INSERT INTO vec_target (id, embedding) VALUES ($id, $emb)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(row_id_uuid)),
                    ("emb".to_string(), Value::Vector(vec![0.1, 0.2, 0.3, 0.4])),
                ]),
            )?;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let n_writers = 16;
        let committed = Arc::new(AtomicUsize::new(0));
        for w in 0..n_writers {
            let db = db.clone();
            let committed = committed.clone();
            let tx = db.begin().expect("begin");
            db.execute_in_tx(
                tx,
                "INSERT INTO host (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3015_0000 + w as u128)))]),
            )
            .expect("host insert");
            db.commit(tx).expect("commit");
            committed.fetch_add(1, AtomicOrdering::SeqCst);
        }
        assert_eq!(committed.load(AtomicOrdering::SeqCst), n_writers);

        let snap = db.snapshot();
        let host_rows = db.scan("host", snap).expect("scan host");
        let cascade_rows = db.scan("cascade_rel", snap).expect("scan cascade_rel");
        let vec_rows = db.scan("vec_target", snap).expect("scan vec_target");
        assert_eq!(
            host_rows.len(),
            n_writers,
            "host: every writer's INSERT must commit"
        );
        assert_eq!(
            cascade_rows.len(),
            n_writers,
            "cascade_rel: trigger cascade fires once per host commit"
        );
        assert_eq!(
            vec_rows.len(),
            n_writers,
            "vec_target: vector cascade fires once per host commit"
        );
        // Graph-edge query: a regression that no-ops insert_edge would pass
        // the relational/vector scans silently. Pin the cascade-graph result
        // explicitly: 16 writers × one edge per callback = 16 edges from the
        // anchor node along the `host_to_anchor` edge type.
        let edge_type: EdgeType = "host_to_anchor".to_string();
        let graph_rows = db
            .edge_count(anchor_node, &edge_type, snap)
            .expect("count graph edges from anchor");
        assert_eq!(
            graph_rows, n_writers,
            "graph cascade: trigger cascade must insert one edge per host commit"
        );
    })
    .expect("t30_15 deadlocked");
}

#[test]
#[serial]
fn t30_15b_schema_agnosticity_wait_holds_mid_cascade() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE host (id UUID PRIMARY KEY)", &empty()).unwrap();
        db.execute("CREATE TABLE cascade_rel (id UUID PRIMARY KEY, host_id UUID)", &empty()).unwrap();
        db.execute("CREATE TABLE vec_target (id UUID PRIMARY KEY, embedding VECTOR(4))", &empty()).unwrap();
        db.execute("CREATE TRIGGER tr ON host WHEN INSERT", &empty()).unwrap();

        let anchor_node: NodeId = uuid(0x3015BA);
        let mid_cascade_entered = Arc::new(Barrier::new(2));
        let mid_cascade_done = Arc::new(Barrier::new(2));
        let mid_cascade_entered_cb = mid_cascade_entered.clone();
        let mid_cascade_done_cb = mid_cascade_done.clone();

        db.register_trigger_callback("tr", move |db_handle, ctx| {
            // Phase 1: relational write.
            db_handle.execute_in_tx(
                ctx.tx,
                "INSERT INTO cascade_rel (id, host_id) VALUES ($id, $host)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("host".to_string(), ctx.row_values.get("id").cloned().unwrap_or(Value::Null)),
                ]),
            )?;
            // Park mid-cascade — Thread C's begin() must wait-and-proceed
            // while Thread A is suspended between cascade phases.
            mid_cascade_entered_cb.wait();
            mid_cascade_done_cb.wait();
            // Phase 2: graph edge cascade.
            let target: NodeId = Uuid::new_v4();
            let edge_type: EdgeType = "host_to_anchor".to_string();
            let _ = db_handle.insert_edge(ctx.tx, anchor_node, target, edge_type, HashMap::new())?;
            // Phase 3: vector cascade.
            db_handle.execute_in_tx(
                ctx.tx,
                "INSERT INTO vec_target (id, embedding) VALUES ($id, $emb)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("emb".to_string(), Value::Vector(vec![0.1, 0.2, 0.3, 0.4])),
                ]),
            )?;
            Ok(())
        }).unwrap();
        db.complete_initialization().unwrap();

        // Thread A: fires the trigger; its callback parks mid-cascade
        // between Phase 1 (relational write) and Phase 2 (graph) /
        // Phase 3 (vector).
        let fire = fire_trigger_in_thread_into_host(db.clone(), 0x3015BC);
        mid_cascade_entered.wait();  // callback parked between cascade phases
        let mut mid_cascade_guard = BarrierReleaseGuard::armed(mid_cascade_done.clone());

        // Snapshot-isolation check: while Thread A's callback is parked
        // mid-cascade, the test thread reads via db.snapshot() (no open tx
        // required — snapshot() is a free-standing read primitive). Thread
        // A's tx is uncommitted; all cascade subsystems must show ZERO
        // rows from this firing.
        let snap_b = db.snapshot();
        let host_mid = db.scan("host", snap_b).expect("scan host mid");
        let cascade_mid = db.scan("cascade_rel", snap_b).expect("scan cascade_rel mid");
        let vec_mid = db.scan("vec_target", snap_b).expect("scan vec_target mid");
        let edge_type_observed: EdgeType = "host_to_anchor".to_string();
        let graph_mid = db.edge_count(anchor_node, &edge_type_observed, snap_b)
            .expect("count graph edges mid");
        assert_eq!(host_mid.len(), 0, "snapshot must not observe Thread A's uncommitted host row");
        assert_eq!(cascade_mid.len(), 0, "snapshot must not observe Thread A's uncommitted relational cascade row");
        assert_eq!(vec_mid.len(), 0, "snapshot must not observe Thread A's uncommitted vector cascade row");
        assert_eq!(graph_mid, 0, "snapshot must not observe Thread A's uncommitted graph cascade edge");

        // Spawn Thread C — a distinct thread (NOT the test thread), so the
        // engine's cross-thread arm of the same-DB B2 wait primitive is
        // exercised. Thread C calls db.begin(); the wait primitive parks
        // it because Thread A's callback is active mid-cascade. Thread C's
        // tx commits after Thread A finishes.
        let db_c = db.clone();
        let probe = thread::spawn(move || -> Result<()> {
            let tx_c = db_c.begin()?;
            db_c.commit(tx_c)
        });

        // Wait until Thread C is observed parked on the wait primitive.
        // Bounded poll with 1 s timeout — under correct §30 the counter
        // increments before Thread C blocks.
        let park_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let mut park_observed = false;
        while std::time::Instant::now() < park_deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 1 {
                park_observed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        // Release the mid-cascade barrier so Thread A continues through
        // graph + vector phases and commits.
        mid_cascade_done.wait();
        mid_cascade_guard.disarm();
        fire.join().unwrap().unwrap();
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "Thread C did not park on same-DB B2 wait primitive within 1 s while Thread A was mid-cascade; saw {snap_after_release:?}"
        );
        // Thread C wakes when Thread A's callback exits; its begin()
        // returns Ok and its commit returns Ok.
        probe.join().unwrap().expect("Thread C must complete after waking");

        // Verify each cascade subsystem received its row exactly once.
        let snap = db.snapshot();
        let host_rows = db.scan("host", snap).expect("scan host");
        let cascade_rows = db.scan("cascade_rel", snap).expect("scan cascade_rel");
        let vec_rows = db.scan("vec_target", snap).expect("scan vec_target");
        let graph_rows = db.edge_count(anchor_node, &edge_type_observed, snap)
            .expect("count graph edges");
        assert_eq!(host_rows.len(), 1, "host row from Thread A");
        assert_eq!(cascade_rows.len(), 1, "relational cascade row");
        assert_eq!(vec_rows.len(), 1, "vector cascade row");
        assert_eq!(graph_rows, 1, "graph cascade edge");

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "Thread C's mid-cascade-forced park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "mid-cascade park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_15b deadlocked");
}

/// Variant of `fire_trigger_in_thread` that fires a trigger declared on
/// table `host` (not `t`). Used by t30_15b where the firing table is the
/// cascade host.
fn fire_trigger_in_thread_into_host(
    db: Arc<Database>,
    key: u128,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let tx = db.begin()?;
        db.execute_in_tx(
            tx,
            "INSERT INTO host (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
        )?;
        db.commit(tx)
    })
}

#[test]
#[serial]
fn t30_16_drop_while_callback_parked_no_panic_close_releases_after_callback_exits() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3016);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        // Thread B: holds an Arc<Database> clone, calls close(). Must block
        // until the callback releases.
        let db_b = db.clone();
        let close_started = Instant::now();
        let close_handle = thread::spawn(move || (db_b.close(), close_started.elapsed()));

        // Let the close-thread reach the wait point.
        thread::sleep(Duration::from_millis(50));
        let close_pre_release = close_handle.is_finished();

        // Release the callback.
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();

        let (close_result, close_elapsed) = close_handle.join().unwrap();
        assert!(
            !close_pre_release,
            "close() must block while the trigger callback is parked"
        );
        close_result.expect("close() must succeed after callback exits");
        // Close took at least the small delay we slept — the wake path engaged.
        assert!(close_elapsed >= Duration::from_millis(50));

        // Wake-side closed-flag re-check: a fresh begin() observes the closed flag.
        // The canonical closed-handle error is `Error::Other("database handle is closed")`
        // (constructed by `closed_database_error()` in `crates/contextdb-engine/src/database.rs`).
        // Pin to that exact shape; reject any other Err variant to catch a regression
        // that surfaces a typed trigger Err on the post-close path.
        match db.begin() {
            Err(Error::Other(msg)) if msg == "database handle is closed" => {}
            other => panic!(
                "begin() after close must return Err(Error::Other(\"database handle is closed\")); got {other:?}"
            ),
        }
    }).expect("t30_16 deadlocked");
}

#[test]
#[serial]
fn t30_16_close_requested_while_begin_waiter_parked_wakes_begin_to_closed() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3016C);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        let db_begin = db.clone();
        let begin_probe = thread::spawn(move || db_begin.begin());
        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(2));

        let db_close = db.clone();
        let close_probe = thread::spawn(move || db_close.close());
        let close_park_deadline = Instant::now() + Duration::from_secs(2);
        let mut close_park_observed = false;
        while Instant::now() < close_park_deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 2 {
                close_park_observed = true;
                break;
            }
            if close_probe.is_finished() {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        let close_finished_before_release = close_probe.is_finished();

        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        let close_result = close_probe.join().unwrap();
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "begin waiter must park before close is requested; snap={snap_after_release:?}"
        );
        assert!(
            close_park_observed,
            "close() must become the second parked waiter before release; snap={snap_after_release:?}"
        );
        assert!(
            !close_finished_before_release,
            "close() must wait for callback release, but must not be starved by parked begin waiter"
        );
        close_result.expect("close should succeed after callback exits");

        match begin_probe.join().unwrap() {
            Err(Error::Other(msg)) if msg == "database handle is closed" => {}
            other => panic!("parked begin must wake to closed-handle error after close wins, got {other:?}"),
        }
    }).expect("t30_16 close-vs-begin-waiter deadlocked");
}

#[test]
#[serial]
fn t30_16b_close_requested_while_sql_begin_waiter_parked_wakes_begin_to_closed() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3016B);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        let db_begin = db.clone();
        let begin_probe = thread::spawn(move || db_begin.execute("BEGIN", &empty()));
        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(2));

        let db_close = db.clone();
        let close_probe = thread::spawn(move || db_close.close());
        let close_park_deadline = Instant::now() + Duration::from_secs(2);
        let mut close_park_observed = false;
        while Instant::now() < close_park_deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 2 {
                close_park_observed = true;
                break;
            }
            if close_probe.is_finished() {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        let close_finished_before_release = close_probe.is_finished();

        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        let close_result = close_probe.join().unwrap();
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "SQL BEGIN waiter must park before close is requested; snap={snap_after_release:?}"
        );
        assert!(
            close_park_observed,
            "close() must become the second parked waiter before release; snap={snap_after_release:?}"
        );
        assert!(
            !close_finished_before_release,
            "close() must wait for callback release, but must not be starved by parked SQL BEGIN"
        );
        close_result.expect("close should succeed after callback exits");

        match begin_probe.join().unwrap() {
            Err(Error::Other(msg)) if msg == "database handle is closed" => {}
            other => panic!(
                "parked SQL BEGIN must wake to closed-handle error after close wins, got {other:?}"
            ),
        }
    })
    .expect("t30_16b SQL BEGIN close-priority test deadlocked");
}

#[test]
#[serial]
fn t30_16c_close_requested_while_sql_rollback_waiter_parked_wakes_rollback_to_closed() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        db.execute("BEGIN", &empty())
            .expect("pre-open SQL session tx");

        let fire = fire_trigger_in_thread(db.clone(), 0x3016C2);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        let db_rollback = db.clone();
        let rollback_probe = thread::spawn(move || db_rollback.execute("ROLLBACK", &empty()));
        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(2));

        let db_close = db.clone();
        let close_probe = thread::spawn(move || db_close.close());
        let close_park_deadline = Instant::now() + Duration::from_secs(2);
        let mut close_park_observed = false;
        while Instant::now() < close_park_deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 2 {
                close_park_observed = true;
                break;
            }
            if close_probe.is_finished() {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        let close_finished_before_release = close_probe.is_finished();

        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        let close_result = close_probe.join().unwrap();
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "SQL ROLLBACK waiter must park before close is requested; snap={snap_after_release:?}"
        );
        assert!(
            close_park_observed,
            "close() must become the second parked waiter before release; snap={snap_after_release:?}"
        );
        assert!(
            !close_finished_before_release,
            "close() must wait for callback release, but must not be starved by parked SQL ROLLBACK"
        );
        close_result.expect("close should succeed after callback exits");

        match rollback_probe.join().unwrap() {
            Err(Error::Other(msg)) if msg == "database handle is closed" => {}
            other => panic!(
                "parked SQL ROLLBACK must wake to closed-handle error after close wins, got {other:?}"
            ),
        }
    })
    .expect("t30_16c SQL ROLLBACK close-priority test deadlocked");
}

#[test]
#[serial]
fn t30_16d_sql_autocommit_dml_does_not_rewait_inside_outer_execute_operation() {
    run_with_timeout(|| {
        let plugin = Arc::new(ParkAutocommitOnQueryPlugin::default());
        let db = Arc::new(Database::open_memory_with_plugin(plugin.clone()).unwrap());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TABLE other (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let entered_cb = entered.clone();
        let done_cb = done.clone();
        db.register_trigger_callback("tr", move |_db_handle, _ctx| {
            entered_cb.wait();
            done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        plugin.arm();
        let db_writer = db.clone();
        let writer = thread::spawn(move || {
            db_writer.execute(
                "INSERT INTO other (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3016D0)))]),
            )
        });
        assert!(
            plugin.wait_until_entered(Duration::from_secs(2)),
            "autocommit writer must reach plugin after execute() opens its outer operation"
        );

        let fire = fire_trigger_in_thread(db.clone(), 0x3016D1);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        plugin.release();
        let db_close = db.clone();
        let close_probe = thread::spawn(move || db_close.close());
        thread::sleep(Duration::from_millis(50));
        assert!(
            !close_probe.is_finished(),
            "close should wait while the independent trigger callback remains parked"
        );
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        writer.join().unwrap().expect("autocommit insert succeeds");
        close_probe.join().unwrap().expect("close succeeds");
    })
    .expect("t30_16d SQL autocommit helper wait regression test deadlocked");
}

#[test]
#[serial]
fn t30_17_deadlock_guard_timeout_unhealthy_park_returns_typed_err_plus_one_warn_event() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    // SAFETY: 2024 edition requires unsafe for env mutation. EnvGuard ensures
    // cleanup even if the test panics (see Stub 2 helper).
    unsafe {
        std::env::set_var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS", "2000");
    }
    let _env_guard = EnvGuard("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS");
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let parked_forever_done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let parked_forever_done_cb = parked_forever_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            entered_cb.wait();
            parked_forever_done_cb.wait(); // never released by this test
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let fire = fire_trigger_in_thread(db.clone(), 0x3017);
        entered.wait();

        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);

        let started = Instant::now();
        let probe_result = db.begin();
        let elapsed = started.elapsed();

        let warn_count = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);

        // Release the parked-forever callback so the firing thread can exit.
        parked_forever_done.wait();
        fire.join().unwrap().unwrap();

        match probe_result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("deadlock-guard must return typed-Err, got {other:?}"),
        }
        assert!(
            elapsed >= Duration::from_millis(2000),
            "deadlock-guard must wait at least 2 s before firing; elapsed={elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(10_000),
            "deadlock-guard exceeded a generous 10 s test budget; elapsed={elapsed:?}"
        );
        assert_eq!(
            warn_count, 1,
            "deadlock-guard must emit exactly one tracing::warn!; saw {warn_count}"
        );
        // Structured-field shape — pin to the canonical field names per the
        // Behavior Contract item 9. Implementation must emit `trigger`,
        // `trigger_name`, `waited_ms`, and `surface` exactly. Disjunctions like
        // `surface || begin` are rejected — the test pins the canonical
        // field-name vocabulary, not the value, so a regression that renames
        // the field surfaces here.
        let last = T30_LAST_WARN_FIELDS
            .get()
            .and_then(|m| m.lock().unwrap().clone());
        let last = last.expect("warn fields captured");
        assert!(
            last.contains("probe_trigger"),
            "warn must name the trigger value; got {last:?}"
        );
        assert!(
            last.contains("trigger_name"),
            "warn must carry the canonical `trigger_name` field; got {last:?}"
        );
        assert!(
            last.contains("waited_ms"),
            "warn must carry the canonical `waited_ms` field; got {last:?}"
        );
        assert!(
            last.contains("surface"),
            "warn must carry the canonical `surface` field; got {last:?}"
        );
        // For begin(), `surface=begin` (or the engine's canonical begin name).
        // The cross-surface siblings (t30_17_commit / _rollback / _apply_changes /
        // _close) pin the per-surface value.
        assert!(
            last.contains("begin"),
            "begin-surface deadlock-guard warn must name `begin` as surface; got {last:?}"
        );

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(
            snap.deadlock_guard_timeout_observed, 1,
            "telemetry must report 1 timeout; snap={snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 1,
            "telemetry must report exactly 1 same-DB typed Err on timeout; snap={snap:?}"
        );
    })
    .expect("t30_17 deadlocked");
}

// ============================================================================
// Cross-surface deadlock-guard siblings: the `tracing::warn!` and structured-field
// shape must engage on every public tx-control surface, not just begin(). FIX 14:
// pin the canonical surface-name vocabulary per surface so a regression that
// emits the warn only on begin() while the wait engages on commit/rollback/
// apply_changes/close fails here.
// ============================================================================

#[test]
#[serial]
fn t30_17_deadlock_guard_commit() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    unsafe {
        std::env::set_var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS", "2000");
    }
    let _env_guard = EnvGuard("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS");
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let parked_forever_done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let parked_forever_done_cb = parked_forever_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            entered_cb.wait();
            parked_forever_done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        // Thread B opens its tx BEFORE the callback fires so its commit is
        // the contended surface.
        let tx_b = db.begin().expect("Thread B opens tx");
        let fire = fire_trigger_in_thread(db.clone(), 0x3017C);
        entered.wait();
        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);
        let started = Instant::now();
        let probe_result = db.commit(tx_b);
        let elapsed = started.elapsed();
        let warn_count = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
        parked_forever_done.wait();
        fire.join().unwrap().unwrap();
        match probe_result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("deadlock-guard on commit must return typed-Err, got {other:?}"),
        }
        assert!(elapsed >= Duration::from_millis(2000));
        assert!(elapsed < Duration::from_millis(10_000));
        assert_eq!(
            warn_count, 1,
            "deadlock-guard must emit exactly one warn on commit"
        );
        let last = T30_LAST_WARN_FIELDS
            .get()
            .and_then(|m| m.lock().unwrap().clone())
            .expect("warn fields captured");
        assert!(
            last.contains("probe_trigger"),
            "warn must name the trigger; got {last:?}"
        );
        assert!(
            last.contains("trigger_name"),
            "warn must carry `trigger_name`; got {last:?}"
        );
        assert!(
            last.contains("waited_ms"),
            "warn must carry `waited_ms`; got {last:?}"
        );
        assert!(
            last.contains("surface"),
            "warn must carry `surface`; got {last:?}"
        );
        assert!(
            last.contains("commit"),
            "commit-surface warn must name `commit`; got {last:?}"
        );
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(snap.deadlock_guard_timeout_observed, 1);
        assert_eq!(snap.typed_err_observed_same_db, 1);
    })
    .expect("t30_17_commit deadlocked");
}

#[test]
#[serial]
fn t30_17_deadlock_guard_rollback() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    unsafe {
        std::env::set_var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS", "2000");
    }
    let _env_guard = EnvGuard("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS");
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let parked_forever_done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let parked_forever_done_cb = parked_forever_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            entered_cb.wait();
            parked_forever_done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let tx_b = db.begin().expect("Thread B opens tx");
        let fire = fire_trigger_in_thread(db.clone(), 0x3017B);
        entered.wait();
        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);
        let started = Instant::now();
        let probe_result = db.rollback(tx_b);
        let elapsed = started.elapsed();
        let warn_count = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
        parked_forever_done.wait();
        fire.join().unwrap().unwrap();
        match probe_result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("deadlock-guard on rollback must return typed-Err, got {other:?}"),
        }
        assert!(elapsed >= Duration::from_millis(2000));
        assert!(elapsed < Duration::from_millis(10_000));
        assert_eq!(warn_count, 1);
        let last = T30_LAST_WARN_FIELDS
            .get()
            .and_then(|m| m.lock().unwrap().clone())
            .expect("warn fields captured");
        assert!(last.contains("probe_trigger"));
        assert!(last.contains("trigger_name"));
        assert!(last.contains("waited_ms"));
        assert!(last.contains("surface"));
        assert!(
            last.contains("rollback"),
            "rollback-surface warn must name `rollback`; got {last:?}"
        );
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(snap.deadlock_guard_timeout_observed, 1);
        assert_eq!(snap.typed_err_observed_same_db, 1);
    })
    .expect("t30_17_rollback deadlocked");
}

#[test]
#[serial]
fn t30_17_deadlock_guard_apply_changes() {
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    unsafe {
        std::env::set_var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS", "2000");
    }
    let _env_guard = EnvGuard("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS");
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let parked_forever_done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let parked_forever_done_cb = parked_forever_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            entered_cb.wait();
            parked_forever_done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let fire = fire_trigger_in_thread(db.clone(), 0x3017A);
        entered.wait();
        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);
        let started = Instant::now();
        let probe_result = db.apply_changes(
            ChangeSet::default(),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        );
        let elapsed = started.elapsed();
        let warn_count = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
        parked_forever_done.wait();
        fire.join().unwrap().unwrap();
        match probe_result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("deadlock-guard on apply_changes must return typed-Err, got {other:?}"),
        }
        assert!(elapsed >= Duration::from_millis(2000));
        assert!(elapsed < Duration::from_millis(10_000));
        assert_eq!(warn_count, 1);
        let last = T30_LAST_WARN_FIELDS
            .get()
            .and_then(|m| m.lock().unwrap().clone())
            .expect("warn fields captured");
        assert!(last.contains("probe_trigger"));
        assert!(last.contains("trigger_name"));
        assert!(last.contains("waited_ms"));
        assert!(last.contains("surface"));
        assert!(
            last.contains("apply_changes"),
            "apply_changes warn must name `apply_changes`; got {last:?}"
        );
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(snap.deadlock_guard_timeout_observed, 1);
        assert_eq!(snap.typed_err_observed_same_db, 1);
    })
    .expect("t30_17_apply_changes deadlocked");
}

#[test]
#[serial]
fn t30_17_deadlock_guard_close() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    unsafe {
        std::env::set_var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS", "2000");
    }
    let _env_guard = EnvGuard("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS");
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let parked_forever_done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let parked_forever_done_cb = parked_forever_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            entered_cb.wait();
            parked_forever_done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let fire = fire_trigger_in_thread(db.clone(), 0x3017C);
        entered.wait();
        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);
        // Thread B's close() must engage the deadlock-guard wait. Use a clone
        // so the test thread retains its handle for the post-deadline assertions.
        let db_b = db.clone();
        let started = Instant::now();
        let probe_result = thread::spawn(move || db_b.close()).join().unwrap();
        let elapsed = started.elapsed();
        let warn_count = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
        parked_forever_done.wait();
        fire.join().unwrap().unwrap();
        match probe_result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("deadlock-guard on close must return typed-Err, got {other:?}"),
        }
        assert!(elapsed >= Duration::from_millis(2000));
        assert!(elapsed < Duration::from_millis(10_000));
        assert_eq!(warn_count, 1);
        let last = T30_LAST_WARN_FIELDS
            .get()
            .and_then(|m| m.lock().unwrap().clone())
            .expect("warn fields captured");
        assert!(last.contains("probe_trigger"));
        assert!(last.contains("trigger_name"));
        assert!(last.contains("waited_ms"));
        assert!(last.contains("surface"));
        assert!(
            last.contains("close"),
            "close-surface warn must name `close`; got {last:?}"
        );
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(snap.deadlock_guard_timeout_observed, 1);
        assert_eq!(snap.typed_err_observed_same_db, 1);
        let tx = db.begin().expect("close timeout must not close the handle");
        db.rollback(tx)
            .expect("post-timeout handle must remain rollback-able");
    })
    .expect("t30_17_close deadlocked");
}

#[test]
#[serial]
fn t30_18_deadlock_guard_healthy_contention_no_tracing_warn_emitted() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY, writer INTEGER, seq INTEGER)", &empty()).unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
        let first_callback_seen = Arc::new(AtomicBool::new(false));
        let first_callback_entered = Arc::new(AtomicBool::new(false));
        let release_first_callback = Arc::new(AtomicBool::new(false));
        let _release_guard = AtomicReleaseGuard(release_first_callback.clone());
        let first_callback_seen_cb = first_callback_seen.clone();
        let first_callback_entered_cb = first_callback_entered.clone();
        let release_first_callback_cb = release_first_callback.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            if !first_callback_seen_cb.swap(true, AtomicOrdering::SeqCst) {
                first_callback_entered_cb.store(true, AtomicOrdering::SeqCst);
                while !release_first_callback_cb.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        }).unwrap();
        db.complete_initialization().unwrap();

        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);

        let n_writers = 16;
        let commits_per_writer = 8;
        let writer_barrier = Arc::new(Barrier::new(n_writers + 1));
        let committed = Arc::new(AtomicUsize::new(0));
        let typed_err_observed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n_writers);
        for w in 0..n_writers {
            let db = db.clone();
            let writer_barrier = writer_barrier.clone();
            let committed = committed.clone();
            let typed_err_observed = typed_err_observed.clone();
            handles.push(thread::spawn(move || {
                writer_barrier.wait();
                for s in 0..commits_per_writer {
                    let tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                            typed_err_observed.fetch_add(1, AtomicOrdering::SeqCst);
                            continue;
                        }
                        Err(other) => panic!("unexpected: {other:?}"),
                    };
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO t (id, writer, seq) VALUES ($id, $w, $s)",
                        &HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid((w as u128) * 0x10000 + s as u128))),
                            ("w".to_string(), Value::Int64(w as i64)),
                            ("s".to_string(), Value::Int64(s as i64)),
                        ]),
                    ).expect("insert");
                    db.commit(tx).expect("commit");
                    committed.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }));
        }
        writer_barrier.wait();
        let entered_deadline = Instant::now() + Duration::from_secs(2);
        while !first_callback_entered.load(AtomicOrdering::SeqCst) {
            if Instant::now() >= entered_deadline {
                release_first_callback.store(true, AtomicOrdering::SeqCst);
                panic!("first trigger callback did not enter; contention setup failed");
            }
            thread::sleep(Duration::from_millis(1));
        }
        let overlap_deadline = Instant::now() + Duration::from_secs(1);
        while typed_err_observed.load(AtomicOrdering::SeqCst) == 0
            && db.trigger_progress_telemetry_snapshot_for_test().wait_observed == 0
            && Instant::now() < overlap_deadline
        {
            thread::sleep(Duration::from_millis(2));
        }
        let overlap_observed = typed_err_observed.load(AtomicOrdering::SeqCst) > 0
            || db.trigger_progress_telemetry_snapshot_for_test().wait_observed > 0;
        release_first_callback.store(true, AtomicOrdering::SeqCst);
        for h in handles { h.join().unwrap(); }
        assert!(
            overlap_observed,
            "first callback was held but no writer reached the contention gate before release"
        );

        let warn_count_before_control = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        // Positive control.
        tracing::warn!(
            target: "contextdb_engine::acceptance_positive_control",
            "trigger callback wait exceeded deadlock guard"
        );
        let warn_count_after_control = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);

        // Post-§30 the wait-and-proceed contract guarantees every commit lands
        // (no typed-Err observed). Under §29 baseline this assertion fails
        // because typed-Err is observed and committed < n_writers * commits_per_writer.
        assert_eq!(
            committed.load(AtomicOrdering::SeqCst),
            n_writers * commits_per_writer,
            "every writer must commit under §30; committed={}, typed_err_observed={}",
            committed.load(AtomicOrdering::SeqCst),
            typed_err_observed.load(AtomicOrdering::SeqCst),
        );
        assert!(
            warn_count_after_control > warn_count_before_control,
            "positive-control event was not captured; subscriber wiring is broken"
        );
        assert_eq!(
            warn_count_before_control, 0,
            "engine must not emit any tracing::warn! on healthy same-DB B2 wait paths; saw {warn_count_before_control}"
        );
    }).expect("t30_18 deadlocked");
}

#[test]
#[serial]
fn t30_19_cron_then_trigger_sequential_phases_typed_err_then_wait() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let entered_cron_cb = entered_cron.clone();
        let done_cron_cb = done_cron.clone();

        db.execute("CREATE SCHEDULE s EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            entered_cron_cb.wait();
            done_cron_cb.wait();
            Ok(())
        })
        .unwrap();

        let entered_trigger = Arc::new(Barrier::new(2));
        let done_trigger = Arc::new(Barrier::new(2));
        let entered_trigger_cb = entered_trigger.clone();
        let done_trigger_cb = done_trigger.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            entered_trigger_cb.wait();
            done_trigger_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        // Phase 1: cron callback parked → Thread B's begin() returns typed-Err Cron.
        let db_cron = db.clone();
        let cron_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            db_cron.cron_run_due_now_for_test().expect("cron tick");
        });
        entered_cron.wait();
        let mut done_cron_guard = BarrierReleaseGuard::armed(done_cron.clone());

        let result = db.begin();
        match result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            }) => {}
            other => panic!("Phase 1: expected B1 typed-Err Cron, got {other:?}"),
        }

        done_cron.wait();
        done_cron_guard.disarm();
        cron_thread.join().unwrap();

        // Phase 2: trigger callback parked → Thread C's begin() waits and proceeds.
        let trigger_thread = fire_trigger_in_thread(db.clone(), 0x3019);
        entered_trigger.wait();
        let mut done_trigger_guard = BarrierReleaseGuard::armed(done_trigger.clone());

        let db_c = db.clone();
        let probe = thread::spawn(move || db_c.begin());
        thread::sleep(Duration::from_millis(50));
        done_trigger.wait();
        done_trigger_guard.disarm();
        trigger_thread.join().unwrap().unwrap();

        let tx = probe
            .join()
            .unwrap()
            .expect("Phase 2: same-DB B2 must wait and proceed");
        let _ = db.rollback(tx);

        // Phase 2 telemetry: the trigger-callback park forced a wait; the
        // wait_observed counter must register at least once. (Phase 1's cron
        // typed-Err is bookkept independently; this assertion targets the
        // §30 wait primitive engagement only.)
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "Phase 2: same-DB B2 trigger park must register on wait_observed; saw {snap:?}"
        );
    })
    .expect("t30_19 deadlocked");
}

#[test]
#[serial]
fn t30_20_panic_freedom_1000_iterations_debug() {
    run_with_timeout(|| run_panic_freedom_begin_iterations(0x3020_0000, "debug"))
        .expect("t30_20 deadlocked");
}

#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t30_20_release_panic_freedom_1000_iterations_release() {
    // Same body as t30_20; release-only.
    run_with_timeout(|| run_panic_freedom_begin_iterations(0x3020_2000, "release"))
        .expect("t30_20_release deadlocked");
}

fn run_panic_freedom_begin_iterations(key_base: u128, label: &'static str) {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
        .unwrap();
    let entered_generation = Arc::new((Mutex::new(0usize), Condvar::new()));
    let release_generation = Arc::new((Mutex::new(0usize), Condvar::new()));
    let entered_generation_cb = entered_generation.clone();
    let release_generation_cb = release_generation.clone();
    db.register_trigger_callback("tr", move |_db, _ctx| {
        let generation = {
            let (entered_lock, entered_cvar) = &*entered_generation_cb;
            let mut entered = entered_lock.lock().unwrap();
            *entered += 1;
            let generation = *entered;
            entered_cvar.notify_all();
            generation
        };
        let (release_lock, release_cvar) = &*release_generation_cb;
        let mut released = release_lock.lock().unwrap();
        while *released < generation {
            released = release_cvar.wait(released).unwrap();
        }
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();

    for iteration in 0..1_000 {
        let before = *entered_generation.0.lock().unwrap();
        let fire = fire_trigger_in_thread(db.clone(), key_base + iteration as u128);
        let deadline = Instant::now() + Duration::from_secs(2);
        let generation = {
            let (entered_lock, entered_cvar) = &*entered_generation;
            let mut entered = entered_lock.lock().unwrap();
            while *entered == before {
                let now = Instant::now();
                if now >= deadline {
                    let (release_lock, release_cvar) = &*release_generation;
                    *release_lock.lock().unwrap() = usize::MAX;
                    release_cvar.notify_all();
                    let _ = fire.join();
                    panic!("{label} iteration {iteration}: trigger callback did not enter");
                }
                let remaining = deadline.saturating_duration_since(now);
                let (next, timeout) = entered_cvar.wait_timeout(entered, remaining).unwrap();
                entered = next;
                if timeout.timed_out() && *entered == before {
                    let (release_lock, release_cvar) = &*release_generation;
                    *release_lock.lock().unwrap() = usize::MAX;
                    release_cvar.notify_all();
                    let _ = fire.join();
                    panic!("{label} iteration {iteration}: trigger callback did not enter");
                }
            }
            *entered
        };

        // begin() under §30 waits and proceeds; under §29 returns typed-Err.
        // Either way, no panic. The probe runs on a separate thread so this
        // harness can release the callback under the wait-and-proceed contract.
        let db_probe = db.clone();
        let outcome_handle = thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db_probe.begin()))
        });
        {
            let (release_lock, release_cvar) = &*release_generation;
            *release_lock.lock().unwrap() = generation;
            release_cvar.notify_all();
        }
        fire.join().unwrap().unwrap();
        let outcome = outcome_handle.join().unwrap();
        assert!(
            outcome.is_ok(),
            "{label} iteration {iteration}: begin panicked"
        );
        if let Ok(Ok(tx)) = outcome {
            let _ = db.rollback(tx);
        }
    }
}

#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t30_02_release_two_writer_interleave_same_db_b2_waits_then_proceeds() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3002A);
        entered.wait();

        let db_b = db.clone();
        let probe = thread::spawn(move || db_b.begin());

        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(1));
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            park_observed,
            "release begin probe did not park before callback release"
        );

        let result = probe.join().unwrap();
        match result {
            Ok(tx) => {
                let _ = db.rollback(tx);
            }
            Err(other) => panic!("release Thread B expected Ok, got {other:?}"),
        }

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "release barrier-forced park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "release barrier-forced park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_02_release deadlocked");
}

#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t30_05_release_sustained_writer_flood_n32_with_no_op_trigger_all_commit() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY, writer INTEGER, seq INTEGER)", &empty()).unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
        let first_callback_seen = Arc::new(AtomicBool::new(false));
        let first_callback_entered = Arc::new(AtomicBool::new(false));
        let release_first_callback = Arc::new(AtomicBool::new(false));
        let _release_guard = AtomicReleaseGuard(release_first_callback.clone());
        let first_callback_seen_cb = first_callback_seen.clone();
        let first_callback_entered_cb = first_callback_entered.clone();
        let release_first_callback_cb = release_first_callback.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            if !first_callback_seen_cb.swap(true, AtomicOrdering::SeqCst) {
                first_callback_entered_cb.store(true, AtomicOrdering::SeqCst);
                while !release_first_callback_cb.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        }).unwrap();
        db.complete_initialization().unwrap();

        let n_writers = 32;
        let commits_per_writer = 16;
        let writer_barrier = Arc::new(Barrier::new(n_writers + 1));
        let committed = Arc::new(AtomicUsize::new(0));
        let typed_err_same_db = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n_writers);
        for w in 0..n_writers {
            let db = db.clone();
            let writer_barrier = writer_barrier.clone();
            let committed = committed.clone();
            let typed_err_same_db = typed_err_same_db.clone();
            handles.push(thread::spawn(move || {
                writer_barrier.wait();
                for s in 0..commits_per_writer {
                    let tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                            typed_err_same_db.fetch_add(1, AtomicOrdering::SeqCst);
                            continue;
                        }
                        Err(other) => panic!("release unexpected: {other:?}"),
                    };
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO t (id, writer, seq) VALUES ($id, $w, $s)",
                        &HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid((w as u128) * 0x10000 + s as u128))),
                            ("w".to_string(), Value::Int64(w as i64)),
                            ("s".to_string(), Value::Int64(s as i64)),
                        ]),
                    ).expect("release insert");
                    db.commit(tx).expect("release commit");
                    committed.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }));
        }
        writer_barrier.wait();
        let entered_deadline = Instant::now() + Duration::from_secs(2);
        while !first_callback_entered.load(AtomicOrdering::SeqCst) {
            if Instant::now() >= entered_deadline {
                release_first_callback.store(true, AtomicOrdering::SeqCst);
                panic!("release: first trigger callback did not enter; contention setup failed");
            }
            thread::sleep(Duration::from_millis(1));
        }
        let overlap_deadline = Instant::now() + Duration::from_secs(1);
        while typed_err_same_db.load(AtomicOrdering::SeqCst) == 0
            && db.trigger_progress_telemetry_snapshot_for_test().wait_observed == 0
            && Instant::now() < overlap_deadline
        {
            thread::sleep(Duration::from_millis(2));
        }
        let overlap_observed = typed_err_same_db.load(AtomicOrdering::SeqCst) > 0
            || db.trigger_progress_telemetry_snapshot_for_test().wait_observed > 0;
        release_first_callback.store(true, AtomicOrdering::SeqCst);
        for h in handles { h.join().unwrap(); }
        assert!(
            overlap_observed,
            "release: first callback was held but no writer reached the contention gate before release"
        );

        assert_eq!(typed_err_same_db.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(committed.load(AtomicOrdering::SeqCst), n_writers * commits_per_writer);

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        let _ = snap.wait_observed; // informational for no-op callbacks; deterministic parks are asserted elsewhere.
        assert_eq!(snap.typed_err_observed_same_db, 0, "release typed-Err; snap={snap:?}");
        assert_eq!(snap.deadlock_guard_timeout_observed, 0, "release deadlock-guard; snap={snap:?}");
    })
    .expect("t30_05_release deadlocked");
}

#[test]
fn t30_21_display_strings_byte_pinned_post_30() {
    let cross_thread_trigger = Error::CallbackActiveCrossThread {
        kind: CallbackKind::Trigger,
    }
    .to_string();
    assert_eq!(
        cross_thread_trigger,
        "trigger callback active on another thread; retry once the callback completes",
        "CallbackActiveCrossThread{{Trigger}} Display drift"
    );
    let cross_thread_cron = Error::CallbackActiveCrossThread {
        kind: CallbackKind::Cron,
    }
    .to_string();
    assert_eq!(
        cross_thread_cron,
        "cron callback active on another thread; retry once the callback completes"
    );
    let reentry_trigger = Error::CallbackReentry {
        kind: CallbackKind::Trigger,
    }
    .to_string();
    assert_eq!(
        reentry_trigger,
        "operation not allowed inside a trigger callback (API misuse — fix the call site)"
    );
    let reentry_cron = Error::CallbackReentry {
        kind: CallbackKind::Cron,
    }
    .to_string();
    assert_eq!(
        reentry_cron,
        "operation not allowed inside a cron callback (API misuse — fix the call site)"
    );
}

#[test]
#[serial]
fn t30_22_source_order_priority_b1_unrelated_trigger_unchanged() {
    run_with_timeout(|| {
        // db: cron parks; db_y: trigger parks. Thread C's begin() on db must
        // observe B1 (Cron). The unrelated trigger must not mask or delay the
        // local cron-active error.
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty()).unwrap();
        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        db.execute("CREATE SCHEDULE s EVERY '1 MILLISECONDS' TX (cb)", &empty()).unwrap();
        let entered_cron_cb = entered_cron.clone();
        let done_cron_cb = done_cron.clone();
        db.register_cron_callback("cb", move |_db_handle| {
            entered_cron_cb.wait();
            done_cron_cb.wait();
            Ok(())
        }).unwrap();

        let entered_trigger = Arc::new(Barrier::new(2));
        let done_trigger = Arc::new(Barrier::new(2));
        let db_y = build_trigger_parked_db(entered_trigger.clone(), done_trigger.clone());

        let db_cron = db.clone();
        let cron_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            db_cron.cron_run_due_now_for_test().expect("cron tick");
        });
        entered_cron.wait();
        let mut done_cron_guard = BarrierReleaseGuard::armed(done_cron.clone());

        let trigger_thread = fire_trigger_in_thread(db_y.clone(), 0x3022);
        entered_trigger.wait();
        let mut done_trigger_guard = BarrierReleaseGuard::armed(done_trigger.clone());

        let started = Instant::now();
        let result = db.begin();
        let elapsed = started.elapsed();
        match result {
            Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Cron }) => {}
            other => panic!("expected same-DB B1 (Cron) despite unrelated trigger; got {other:?}"),
        }
        // Mirrors t30_14: B1 cron typed-Err must return immediately, not
        // engage the wait primitive. A regression that routes B1 through the
        // §30 wait-and-proceed gate would make this elapsed close to the
        // callback hold duration.
        assert!(
            elapsed < Duration::from_millis(1000),
            "B1 cron typed-Err must return immediately, not engage the trigger wait primitive; elapsed={elapsed:?}"
        );

        done_cron.wait();
        done_cron_guard.disarm();
        cron_thread.join().unwrap();
        done_trigger.wait();
        done_trigger_guard.disarm();
        trigger_thread.join().unwrap().unwrap();
    }).expect("t30_22 deadlocked");
}

#[test]
#[serial]
fn t30_23_source_order_priority_a1_over_a2_priority_unchanged() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_outer.begin());
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |db_handle| {
            db_handle.execute(
                "INSERT INTO t (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3023)))]),
            )?;
            Ok(())
        })
        .unwrap();

        thread::sleep(Duration::from_millis(5));
        db.cron_run_due_now_for_test().expect("cron tick");

        let observed = observed.lock().unwrap().take().expect("trigger ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "expected A1 (Cron) priority over A2 (Trigger), got {observed:?}"
        );
    })
    .expect("t30_23 deadlocked");
}

#[test]
#[serial]
fn t30_25_background_worker_handle_same_db_b2_waits_and_proceeds() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let worker = Arc::new(db.__trigger_progress_worker_handle_for_test());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty()).unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty()).unwrap();

        let trigger_entered = Arc::new(Barrier::new(2));
        let trigger_done = Arc::new(Barrier::new(2));
        let trigger_entered_cb = trigger_entered.clone();
        let trigger_done_cb = trigger_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            trigger_entered_cb.wait();
            trigger_done_cb.wait();
            Ok(())
        }).unwrap();
        db.complete_initialization().unwrap();

        // Thread A: an internal worker Database struct fires the trigger and
        // parks inside the trigger callback. No cron callback is active; this
        // isolates the shared-TriggerState discriminator from the B1 cron
        // typed-Err path.
        let fire = fire_trigger_in_thread(worker.clone(), 0x3025);
        trigger_entered.wait();  // worker-fired trigger callback parked
        let mut trigger_done_guard = BarrierReleaseGuard::armed(trigger_done.clone());

        // Thread B: owner handle, distinct from the worker struct, must
        // still classify as same-DB because both handles share TriggerState.
        let db_probe = db.clone();
        let probe = thread::spawn(move || db_probe.begin());

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut park_observed = false;
        while Instant::now() < deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 1 {
                park_observed = true;
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }

        trigger_done.wait();
        trigger_done_guard.disarm();
        fire.join().unwrap().expect("worker trigger fire");
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "owner-handle writer did not park on worker-fired same-DB trigger wait; saw {snap_after_release:?}"
        );

        let result = probe.join().unwrap();
        match result {
            Ok(tx) => {
                let _ = db.rollback(tx);
            }
            Err(other) => panic!(
                "owner-handle Thread B expected wait-and-proceed Ok while \
                 internal-worker trigger callback was active, got {other:?}"
            ),
        }

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "internal-worker same-DB park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "internal-worker same-DB B2 must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_25 deadlocked");
}

#[test]
#[serial]
fn t30_02_execute_same_db_b2_waits_then_proceeds() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        db.execute(
            "CREATE TABLE execute_target (id UUID PRIMARY KEY)",
            &empty(),
        )
        .unwrap();

        let fire = fire_trigger_in_thread(db.clone(), 0x3002E);
        entered.wait(); // callback parked
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        // Thread B: db.execute("INSERT ...") must wait and proceed under §30.
        // Target a non-trigger table so the probe itself does not fire a
        // second parked callback after Thread A releases.
        let db_b = db.clone();
        let probe = thread::spawn(move || {
            db_b.execute(
                "INSERT INTO execute_target (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3002EB)))]),
            )
        });

        let park_observed = wait_until_trigger_wait_observed(&db, Duration::from_secs(1));
        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();
        assert!(
            park_observed,
            "execute probe did not park before callback release"
        );

        let result = probe.join().unwrap();
        result.expect("Thread B execute() must wait-and-proceed Ok under same-DB B2");
        assert_eq!(
            db.scan("execute_target", db.snapshot()).unwrap().len(),
            1,
            "execute() probe row must commit after wait"
        );

        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "execute-helper park must register on wait_observed; saw {snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 0,
            "execute-helper park must NOT register typed-Err; saw {snap:?}"
        );
    })
    .expect("t30_02_execute deadlocked");
}

#[test]
#[serial]
fn t30_panicking_callback_wakes_parked_waiter() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        // Two-stage barrier: callback signals "active" (entered), then waits
        // on `panic_barrier` BEFORE panicking. The test thread polls Thread B's
        // park status, then releases `panic_barrier` only after Thread B is
        // confirmed parked. Without this, the panic can race ahead of Thread
        // B's begin() and the test would assert wake-on-panic without ever
        // having parked anything.
        let panic_barrier = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty()).unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
        let entered_cb = entered.clone();
        let panic_barrier_cb = panic_barrier.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            entered_cb.wait();
            // Hold the active-callback state until the test confirms Thread B
            // has parked.
            panic_barrier_cb.wait();
            // Callback panics AFTER Thread B is parked. catch_unwind in the
            // engine must rewind and the wake-on-completion path must still
            // fire so that Thread B's parked begin() proceeds.
            panic!("intentional callback panic for wake-correctness test");
        }).unwrap();
        db.complete_initialization().unwrap();

        // Thread A fires the trigger; the firing thread observes the panic
        // through the engine's typed-Err return (panic-freedom contract).
        let db_a = db.clone();
        let fire = thread::spawn(move || {
            // The firing tx's outcome is implementation-defined under panic
            // (the engine surfaces the panic as Err); the test only cares
            // that Thread B wakes.
            let tx = db_a.begin().expect("a begin");
            let _ = db_a.execute_in_tx(
                tx,
                "INSERT INTO t (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC0DE1)))]),
            );
            let _ = db_a.commit(tx);
        });
        entered.wait();  // callback active, holding before panic
        let mut panic_guard = BarrierReleaseGuard::armed(panic_barrier.clone());

        // Thread B: begin() parks while the callback is active.
        let db_b = db.clone();
        let started = Instant::now();
        let probe = thread::spawn(move || db_b.begin());

        // Poll until Thread B is observed parked on the wait primitive.
        // Bounded 1 s timeout — under correct §30 the counter increments
        // promptly when Thread B's begin() blocks.
        let park_deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut park_observed = false;
        while std::time::Instant::now() < park_deadline {
            let snap = db.trigger_progress_telemetry_snapshot_for_test();
            if snap.wait_observed >= 1 {
                park_observed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }

        // Release the panic barrier — callback panics; engine's catch_unwind
        // runs the wake path on the unwind edge.
        panic_barrier.wait();
        panic_guard.disarm();

        // Wait for the firing thread to finish (the engine surfaces the
        // panic as Err to the firing surface).
        fire.join().unwrap();
        let snap_after_release = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            park_observed,
            "Thread B did not park on same-DB B2 wait primitive within 1 s before callback panic; saw {snap_after_release:?}"
        );

        let result = probe.join().unwrap();
        let elapsed = started.elapsed();
        match result {
            Ok(tx) => { let _ = db.rollback(tx); }
            other => panic!("Thread B must wake after callback panic, got {other:?}"),
        }
        // Thread B must wake well before the deadlock-guard's 60-second
        // default fires. A regression that fires the wake only on the
        // happy-path return would hit the 60-second bound.
        assert!(
            elapsed < Duration::from_secs(5),
            "Thread B must wake on callback panic without engaging the deadlock guard; elapsed={elapsed:?}"
        );
        // Load-bearing counter assertions: Thread B must have parked AND
        // woken via the panic-edge wake path (not via deadlock-guard timeout).
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert!(
            snap.wait_observed >= 1,
            "Thread B must park before callback panic; saw {snap:?}"
        );
        assert_eq!(
            snap.deadlock_guard_timeout_observed, 0,
            "Thread B must wake on panic-edge notify, not on deadlock-guard timeout; saw {snap:?}"
        );
    }).expect("t30_panicking_callback_wakes deadlocked");
}

#[test]
#[serial]
fn t30_cron_b1_immediate_typed_err_remains_unparked() {
    run_with_timeout(|| {
        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered_cron.clone(), done_cron.clone());

        // Thread A: cron callback parks.
        let db_cron = db.clone();
        let cron_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            db_cron.cron_run_due_now_for_test().expect("cron tick");
        });
        entered_cron.wait();
        let mut done_cron_guard = BarrierReleaseGuard::armed(done_cron.clone());

        // Thread E: with cron active (B1), begin() must return typed Cron
        // immediately. It must not wait on any callback primitive.
        let started = Instant::now();
        let result = db.begin();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "cron B1 must return immediately, not park; elapsed={elapsed:?}"
        );
        match result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            }) => {}
            other => panic!("expected immediate CallbackActiveCrossThread{{Cron}}, got {other:?}"),
        }

        done_cron.wait();
        done_cron_guard.disarm();
        cron_thread.join().unwrap();
    })
    .expect("t30_cron_b1_immediate deadlocked");
}

#[test]
#[serial]
fn t30_11_commit_from_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let setup_tx_y = db_y.begin().expect("setup tx on db_y");

        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_y_for_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_y_for_cb.commit(setup_tx_y));
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();

        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C)))]),
        )
        .unwrap();

        let observed = observed.lock().unwrap().take().expect("callback ran");
        match observed {
            Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("expected cross-DB-from-callback Class A on commit, got {other:?}"),
        }
    })
    .expect("t30_11_commit deadlocked");
}

#[test]
#[serial]
fn t30_11_rollback_from_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let db_y = Arc::new(Database::open_memory());
        db_y.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let setup_tx_y = db_y.begin().expect("setup tx on db_y");

        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_y_for_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_y_for_cb.rollback(setup_tx_y));
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();

        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011B)))]),
        )
        .unwrap();

        let observed = observed.lock().unwrap().take().expect("callback ran");
        match observed {
            Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("expected cross-DB-from-callback Class A on rollback, got {other:?}"),
        }
    })
    .expect("t30_11_rollback deadlocked");
}

#[test]
#[serial]
fn t30_11_apply_changes_from_callback_against_other_db_returns_callback_reentry() {
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let db_y = Arc::new(Database::open_memory());

        let observed: Arc<Mutex<Option<Result<contextdb_engine::ApplyResult>>>> =
            Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_y_for_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_y_for_cb.apply_changes(
                ChangeSet::default(),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            ));
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();

        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011A)))]),
        )
        .unwrap();

        let observed = observed.lock().unwrap().take().expect("callback ran");
        match observed {
            Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            }) => {}
            other => {
                panic!("expected cross-DB-from-callback Class A on apply_changes, got {other:?}")
            }
        }
    })
    .expect("t30_11_apply_changes deadlocked");
}

#[test]
#[serial]
fn t30_11_close_from_callback_against_other_db_returns_callback_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let db_y = Arc::new(Database::open_memory());

        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_y_for_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            *observed_cb.lock().unwrap() = Some(db_y_for_cb.close());
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();

        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0x3011C)))]),
        )
        .unwrap();

        let observed = observed.lock().unwrap().take().expect("callback ran");
        match observed {
            Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("expected cross-DB-from-callback Class A on close, got {other:?}"),
        }
        // db_y must remain open (close was rejected).
        let tx = db_y.begin().expect("db_y still open");
        let _ = db_y.rollback(tx);
    })
    .expect("t30_11_close deadlocked");
}

#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t30_16_release() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let fire = fire_trigger_in_thread(db.clone(), 0x3016A);
        entered.wait();
        let mut done_guard = BarrierReleaseGuard::armed(done.clone());

        let db_b = db.clone();
        let close_started = Instant::now();
        let close_handle = thread::spawn(move || (db_b.close(), close_started.elapsed()));

        thread::sleep(Duration::from_millis(50));
        let close_pre_release = close_handle.is_finished();

        done.wait();
        done_guard.disarm();
        fire.join().unwrap().unwrap();

        let (close_result, close_elapsed) = close_handle.join().unwrap();
        assert!(
            !close_pre_release,
            "release: close() must block while callback parked"
        );
        close_result.expect("release: close() must succeed after callback exits");
        assert!(close_elapsed >= Duration::from_millis(50));

        match db.begin() {
            Err(Error::Other(msg)) if msg == "database handle is closed" => {}
            other => panic!("release: begin() after close must return Err(closed); got {other:?}"),
        }
    })
    .expect("t30_16_release deadlocked");
}

#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t30_17_release() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    unsafe {
        std::env::set_var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS", "2000");
    }
    let _env_guard = EnvGuard("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS");
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let parked_forever_done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let parked_forever_done_cb = parked_forever_done.clone();
        db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
            entered_cb.wait();
            parked_forever_done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let fire = fire_trigger_in_thread(db.clone(), 0x3017A);
        entered.wait();
        let mut parked_guard = BarrierReleaseGuard::armed(parked_forever_done.clone());
        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);
        let started = Instant::now();
        let probe_result = db.begin();
        let elapsed = started.elapsed();
        let warn_count = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
        parked_forever_done.wait();
        parked_guard.disarm();
        fire.join().unwrap().unwrap();
        match probe_result {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => {}
            other => panic!("release deadlock-guard typed-Err, got {other:?}"),
        }
        assert!(elapsed >= Duration::from_millis(2000));
        assert!(elapsed < Duration::from_millis(10_000));
        assert_eq!(warn_count, 1);
        // Structured-field shape — replicated from t30_17 (debug). Release
        // builds inline `tracing::warn!` differently than debug; the
        // structured-field identity must hold across both. A regression that
        // emits the warn under debug but degrades to a positional-only
        // event under release fails here.
        let last = T30_LAST_WARN_FIELDS
            .get()
            .and_then(|m| m.lock().unwrap().clone());
        let last = last.expect("release: warn fields captured");
        assert!(
            last.contains("probe_trigger"),
            "release: warn must name the trigger value; got {last:?}"
        );
        assert!(
            last.contains("trigger_name"),
            "release: warn must carry the canonical `trigger_name` field; got {last:?}"
        );
        assert!(
            last.contains("waited_ms"),
            "release: warn must carry the canonical `waited_ms` field; got {last:?}"
        );
        assert!(
            last.contains("surface"),
            "release: warn must carry the canonical `surface` field; got {last:?}"
        );
        assert!(
            last.contains("begin"),
            "release: begin-surface deadlock-guard warn must name `begin` as surface; got {last:?}"
        );
        let snap = db.trigger_progress_telemetry_snapshot_for_test();
        assert_eq!(
            snap.deadlock_guard_timeout_observed, 1,
            "release telemetry must report 1 timeout; snap={snap:?}"
        );
        assert_eq!(
            snap.typed_err_observed_same_db, 1,
            "release telemetry must report exactly 1 same-DB typed Err on timeout; snap={snap:?}"
        );
    })
    .expect("t30_17_release deadlocked");
}

#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t30_18_release() {
    t30_install_global_subscriber();
    t30_reset_warn_counters();
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY, writer INTEGER, seq INTEGER)", &empty()).unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
        let first_callback_seen = Arc::new(AtomicBool::new(false));
        let first_callback_entered = Arc::new(AtomicBool::new(false));
        let release_first_callback = Arc::new(AtomicBool::new(false));
        let _release_guard = AtomicReleaseGuard(release_first_callback.clone());
        let first_callback_seen_cb = first_callback_seen.clone();
        let first_callback_entered_cb = first_callback_entered.clone();
        let release_first_callback_cb = release_first_callback.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            if !first_callback_seen_cb.swap(true, AtomicOrdering::SeqCst) {
                first_callback_entered_cb.store(true, AtomicOrdering::SeqCst);
                while !release_first_callback_cb.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        }).unwrap();
        db.complete_initialization().unwrap();

        T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);

        let n_writers = 16;
        let commits_per_writer = 8;
        let writer_barrier = Arc::new(Barrier::new(n_writers + 1));
        let committed = Arc::new(AtomicUsize::new(0));
        let typed_err_observed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n_writers);
        for w in 0..n_writers {
            let db = db.clone();
            let writer_barrier = writer_barrier.clone();
            let committed = committed.clone();
            let typed_err_observed = typed_err_observed.clone();
            handles.push(thread::spawn(move || {
                writer_barrier.wait();
                for s in 0..commits_per_writer {
                    let tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                            typed_err_observed.fetch_add(1, AtomicOrdering::SeqCst);
                            continue;
                        }
                        Err(other) => panic!("release unexpected: {other:?}"),
                    };
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO t (id, writer, seq) VALUES ($id, $w, $s)",
                        &HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid((w as u128) * 0x10000 + s as u128))),
                            ("w".to_string(), Value::Int64(w as i64)),
                            ("s".to_string(), Value::Int64(s as i64)),
                        ]),
                    ).expect("release insert");
                    db.commit(tx).expect("release commit");
                    committed.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }));
        }
        writer_barrier.wait();
        let entered_deadline = Instant::now() + Duration::from_secs(2);
        while !first_callback_entered.load(AtomicOrdering::SeqCst) {
            if Instant::now() >= entered_deadline {
                release_first_callback.store(true, AtomicOrdering::SeqCst);
                panic!("release: first trigger callback did not enter; contention setup failed");
            }
            thread::sleep(Duration::from_millis(1));
        }
        let overlap_deadline = Instant::now() + Duration::from_secs(1);
        while typed_err_observed.load(AtomicOrdering::SeqCst) == 0
            && db.trigger_progress_telemetry_snapshot_for_test().wait_observed == 0
            && Instant::now() < overlap_deadline
        {
            thread::sleep(Duration::from_millis(2));
        }
        let overlap_observed = typed_err_observed.load(AtomicOrdering::SeqCst) > 0
            || db.trigger_progress_telemetry_snapshot_for_test().wait_observed > 0;
        release_first_callback.store(true, AtomicOrdering::SeqCst);
        for h in handles { h.join().unwrap(); }
        assert!(
            overlap_observed,
            "release: first callback was held but no writer reached the contention gate before release"
        );

        let warn_count_before_control = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        tracing::warn!(
            target: "contextdb_engine::acceptance_positive_control",
            "trigger callback wait exceeded deadlock guard"
        );
        let warn_count_after_control = T30_WARN_COUNT.load(AtomicOrdering::SeqCst);
        T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);

        assert_eq!(committed.load(AtomicOrdering::SeqCst), n_writers * commits_per_writer);
        assert!(warn_count_after_control > warn_count_before_control);
        assert_eq!(warn_count_before_control, 0, "release: no engine warn on healthy paths");
    }).expect("t30_18_release deadlocked");
}

/// Verifies the forward-progress watchdog's signal source: every successful
/// commit advances `Database::global_commit_ticks_for_test`. Without this gate
/// a refactor that moves the bump away from the commit success path would
/// silently weaken every `run_with_timeout` in the trigger concurrency suite
/// to a 60-second false-deadlock without anyone noticing.
#[test]
fn global_commit_ticks_advance_on_successful_commit() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE p (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    let tick_after_ddl = Database::global_commit_ticks_for_test();

    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
    db.execute("INSERT INTO p (id) VALUES ($id)", &params)
        .unwrap();
    let tick_after_insert = Database::global_commit_ticks_for_test();
    assert!(
        tick_after_insert > tick_after_ddl,
        "successful commit must advance the global tick counter; \
         before={tick_after_ddl}, after={tick_after_insert}"
    );

    let tx = db.begin().unwrap();
    db.commit(tx).unwrap();
    let tick_after_empty_commit = Database::global_commit_ticks_for_test();
    assert!(
        tick_after_empty_commit > tick_after_insert,
        "empty-write-set commit must still advance the tick counter; \
         before={tick_after_insert}, after={tick_after_empty_commit}"
    );

    let tx = db.begin().unwrap();
    db.rollback(tx).unwrap();
    let tick_after_rollback = Database::global_commit_ticks_for_test();
    assert_eq!(
        tick_after_rollback, tick_after_empty_commit,
        "rollback must not advance the commit tick counter; \
         before={tick_after_empty_commit}, after={tick_after_rollback}"
    );
}
