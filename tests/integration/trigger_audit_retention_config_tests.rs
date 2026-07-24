//! Trigger-audit retention as engine open-config: the engine's OWN durable
//! trigger-firing history used to be bounded only by a hardcoded 7-day
//! constant with a getter that ignored any input and no setter at all. It is
//! now a declarable window (`Database::set_trigger_audit_retention`) -- but
//! deliberately ENGINE OPEN-CONFIG ONLY, not a stored declaration: it lives on
//! the in-process handle for that handle's lifetime, the same way a disk
//! limit or memory ceiling does, and is never written to the store, never a
//! DDL axis (there is no user table to attach `RETAIN` to), and never
//! transported. A fresh handle opened on the same path with no call to the
//! setter reports the shipped default, never a value a PRIOR handle declared.
//!
//! `Database::trigger_audit_retention()` reports the declared override, and
//! a manually-driven `run_maintenance_cycle()` honors it (pinned below). But
//! `MaintenanceContext` used to freeze the window as a plain `Duration` at
//! `spawn_maintenance` time: a declared override never reached an
//! ALREADY-RUNNING engine-owned loop, because the loop reads its window from
//! that frozen snapshot, not from the handle. The last test below drives the
//! real background loop (not `run_maintenance_cycle()`) and fails against
//! that gap until the window is shared (`Arc<AtomicU64>`) and re-read per
//! tick.
//!
//! Discipline: no sleeps, no elapsed-time assertions -- `Wallclock::
//! test_clock_guard` only -- EXCEPT the running-loop honor test below, which
//! by construction cannot use the mocked clock: `Wallclock::test_clock_guard`
//! is thread-local and only affects the calling (test) thread, never the
//! engine-owned maintenance thread it does not control. That test declares a
//! real, tiny window and drives the loop's TICK CADENCE (not the window)
//! through the existing `__set_currency_maintenance_interval` test knob,
//! then bounds its wait with a state-polled ceiling -- the same shape the
//! engine crate's own background-loop tests already use.

use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_audited_db(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open file-backed db");
    db.execute("CREATE TABLE fire (id INTEGER PRIMARY KEY)", &p())
        .expect("trigger source table");
    db.execute("CREATE TRIGGER fire_guard ON fire WHEN INSERT", &p())
        .expect("trigger");
    db.register_trigger_callback("fire_guard", |_, _| Ok(()))
        .expect("trigger callback");
    db.complete_initialization().expect("initialization");
    db
}

fn fire_trigger(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        db.execute("INSERT INTO fire (id) VALUES ($id)", &row)
            .expect("trigger firing insert");
    }
}

fn audit_history_len(db: &Database) -> usize {
    db.trigger_audit_history(Default::default())
        .expect("durable audit history")
        .len()
}

/// The value passed at open-config time (via the setter, on this handle) is
/// honored by the getter on that SAME handle -- real, wired plumbing.
#[test]
fn a_declared_trigger_audit_retention_is_honored_on_the_same_handle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("config.db")).expect("open");
    assert_eq!(
        db.trigger_audit_retention(),
        Some(Duration::from_secs(7 * 24 * 60 * 60)),
        "the shipped default applies until an operator declares one"
    );
    db.set_trigger_audit_retention(Duration::from_secs(60))
        .expect("declare a 1-minute window");
    assert_eq!(
        db.trigger_audit_retention(),
        Some(Duration::from_secs(60)),
        "the getter must report the declared override on this handle"
    );
}

/// The pin for the open-config contract: a declared window is engine
/// open-config, not a stored declaration. A handle that declares a 1-minute
/// window, then is dropped WITHOUT ever pushing that value to the store, must
/// leave a fresh handle opened on the identical path reading the shipped
/// 7-day default -- never the 60 seconds the prior handle declared.
#[test]
fn a_declared_trigger_audit_retention_does_not_survive_a_fresh_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.db");

    {
        let db = Database::open(&path).expect("open");
        db.set_trigger_audit_retention(Duration::from_secs(60))
            .expect("declare a 1-minute window");
        assert_eq!(
            db.trigger_audit_retention(),
            Some(Duration::from_secs(60)),
            "the declaration is honored on the handle that made it"
        );
        // Handle drops here with no further action -- the setter must not
        // have written anything to the store for this to matter.
    }

    let reopened = Database::open(&path).expect("reopen the same path");
    assert_eq!(
        reopened.trigger_audit_retention(),
        Some(Duration::from_secs(7 * 24 * 60 * 60)),
        "a fresh handle on the SAME path with no setter call must see the shipped default, \
         proving the prior handle's declaration was never persisted to the store"
    );
}

/// The declared window is honored by the ACTUAL maintenance pass, not just
/// echoed back by the getter: retention set through engine open-config must
/// prune on the next cycle. A 1-minute declared window must age out rows a
/// 2-minute clock advance leaves behind; the shipped 7-day default would not.
#[test]
fn a_declared_short_trigger_audit_retention_prunes_on_the_next_cycle() {
    let mock_now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_audited_db(&dir.path().join("audited.db"));
    db.set_trigger_audit_retention(Duration::from_secs(60))
        .expect("declare a 1-minute window");

    fire_trigger(&db, 0..10);
    assert_eq!(audit_history_len(&db), 10, "setup: 10 firings recorded");

    // Two minutes past the DECLARED window, but nowhere near the shipped
    // 7-day default -- the honor leg must read the declaration, not the
    // constant.
    mock_now.fetch_add(2 * 60 * 1000, AtomicOrdering::SeqCst);
    let report = db.run_maintenance_cycle().expect("maintenance cycle");
    assert_eq!(
        report.pruned_trigger_audit_rows, 10,
        "the cycle must prune according to the DECLARED 1-minute window, not the shipped \
         7-day default: {report:?}"
    );
    assert_eq!(
        audit_history_len(&db),
        0,
        "every firing predates the declared window's cutoff"
    );
}

/// The gap the two tests above cannot see: they both drive
/// `run_maintenance_cycle()` directly on the test thread, which reads the
/// handle's open-config fresh on every call by construction. The real
/// production path is the engine-owned background loop, whose
/// `MaintenanceContext` used to be built ONCE at `spawn_maintenance` time and
/// never rebuilt. This test spawns that real loop FIRST (via an audited
/// open, which is eligible and starts it immediately), only THEN declares a
/// new retention window on the already-running database, and proves the
/// loop's own next real tick -- not a manual cycle call -- honors it.
///
/// The window itself is real wall-clock time (a mocked `Wallclock` cannot
/// reach the background thread -- see the module doc). Only the loop's TICK
/// CADENCE is sped up, via the existing `__set_currency_maintenance_interval`
/// test-only knob, so the test does not wait a real 7-day (or even
/// 1-minute) window to observe a prune.
#[test]
fn a_declared_trigger_audit_retention_reaches_an_already_running_engine_owned_loop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("audited-running-loop.db");
    {
        // Declare the trigger on a throwaway handle, then close it. Locally
        // executed DDL does not itself spawn the maintenance loop -- only
        // `Database::open` and arriving sync DDL reconcile it -- so the
        // production shape the review finding names ("on reopen of an
        // audited root the thread spawns... before a consumer can call the
        // setter") needs a real reopen to produce a loop that is already
        // running before any setter call.
        let setup = open_audited_db(&path);
        setup.close().expect("close the setup handle");
    }
    let db = Database::open(&path).expect("reopen the audited root");
    // Trigger DECLARATIONS persist across a reopen; the Rust callback
    // closure cannot, so a fresh handle must re-register it and re-signal
    // readiness before it can accept writes on the trigger's table -- same
    // as any real consumer reopening an audited root.
    db.register_trigger_callback("fire_guard", |_, _| Ok(()))
        .expect("re-register the trigger callback after reopen");
    db.complete_initialization()
        .expect("re-signal initialization complete after reopen");

    // The reopen above makes this database maintenance-eligible
    // (`has_durable_trigger_audit` reads the persisted trigger declaration):
    // the engine-owned loop is already running at this point, at the
    // shipped default cadence and the shipped default 7-day retention --
    // neither of which this test has touched yet.
    assert!(
        db.__maintenance_thread_running(),
        "reopening an audited root must self-start the engine-owned maintenance loop \
         before any consumer can call the setter"
    );

    // Speed up the ALREADY-RUNNING loop's cadence (this respawns the thread,
    // but still with the shipped default retention -- the setter below has
    // not run yet). This is the test-only knob named in the review finding;
    // the WINDOW stays real.
    db.__set_currency_maintenance_interval(Duration::from_millis(40));
    assert!(
        db.__maintenance_thread_running(),
        "the sped-up loop must still be running"
    );

    fire_trigger(&db, 0..10);
    assert_eq!(audit_history_len(&db), 10, "setup: 10 firings recorded");

    // Declare a tiny real window on the handle -- AFTER the loop above is
    // already running. Before the fix, the running loop's frozen
    // `MaintenanceContext` never saw this: it would keep pruning against the
    // stale 7-day snapshot taken at spawn, and the 10 rows above would never
    // age out within this test's bounded wait.
    db.set_trigger_audit_retention(Duration::from_millis(50))
        .expect("declare a 50ms window on the running database");

    // State-polled wait, bounded by a generous ceiling: the test's promise
    // IS that a real background thread, already running before the setter
    // call, does real work against the NEW window -- so a ceiling remains by
    // necessity, but the verdict is decided by observed STATE (the audit
    // history draining to zero), never by how long the test happened to
    // sleep. Mirrors the engine crate's own background-loop test shape
    // (`installing_the_ledger_starts_maintenance_and_it_auto_heals`).
    let start = std::time::Instant::now();
    let mut last_seen = audit_history_len(&db);
    while start.elapsed() < Duration::from_secs(10) {
        last_seen = audit_history_len(&db);
        if last_seen == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        last_seen, 0,
        "the ALREADY-RUNNING engine-owned loop must prune according to the window declared \
         on it after it started, not the stale snapshot taken at spawn"
    );
}
