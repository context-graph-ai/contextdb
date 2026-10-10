//! Deterministic stops, foreground writes and a crash between reclaim steps, at the maintenance
//! cycle's own cancellation checkpoint (`build_checkpoint`, run before every reclaim step).
//!
//! The synchronous driver stays on the test's thread, so the thread-local checkpoint observer and
//! the storage layer's per-thread commit counters see exactly this cycle's work.
use super::*;
use std::sync::atomic::AtomicU64;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

struct Store {
    _dir: Option<tempfile::TempDir>,
    path: PathBuf,
    db: Arc<Database>,
}

fn seed(db: &Database) {
    db.execute("CREATE TABLE churn (id INTEGER PRIMARY KEY, payload TEXT)", &p()).unwrap();
    db.execute("CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT)", &p()).unwrap();
    db.execute("INSERT INTO churn (id, payload) VALUES (0, 'seed')", &p()).unwrap();
    for chunk in 0..2 {
        let mut sql = String::from("INSERT INTO bulk (id, payload) VALUES ");
        for i in 0..200 {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({}, '{}')", chunk * 200 + i, "x".repeat(200)));
        }
        db.execute(&sql, &p()).unwrap();
    }
}

fn churn(db: &Database, commits: usize) {
    for generation in 0..commits {
        db.execute(
            &format!("UPDATE churn SET payload = 'g{generation}{}' WHERE id = 0", "y".repeat(8192)),
            &p(),
        )
        .unwrap();
    }
}

fn backlog(db: &Database) -> (usize, usize) {
    let counts = db.__deferred_free_backlog_for_test().unwrap();
    (counts[0].0 + counts[1].0, counts[0].1 + counts[1].1)
}

/// A store whose released snapshot left more extents than one 4,096-extent step can free.
fn store_with_backlog_in(path: PathBuf, dir: Option<tempfile::TempDir>) -> Store {
    let db = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap(),
    );
    seed(&db);
    let session = db.read_session(ReadLimits::default()).unwrap();
    let cursor = session.open_cursor("SELECT id FROM bulk ORDER BY id", &p()).unwrap();
    assert!(cursor.first_page().has_more);
    churn(&db, 140);
    drop(cursor);
    drop(session);
    Store { _dir: dir, path, db }
}

fn store_with_backlog() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reclaim-cancel.db");
    store_with_backlog_in(path, Some(dir))
}

const STEP_EXTENTS: usize = 4096;

/// Observer state shared with the checkpoint closure.
#[derive(Default)]
struct Observed {
    commits: AtomicU64,
    reclaimed_pages: AtomicU64,
    checkpoints: AtomicU64,
}

fn account(observed: &Observed) {
    let work = redb::commit_work::take_commit_pages_for_test();
    observed.commits.fetch_add(work.commits, Ordering::SeqCst);
    observed.reclaimed_pages.fetch_add(work.reclaimed_pages, Ordering::SeqCst);
    observed.checkpoints.fetch_add(1, Ordering::SeqCst);
}

fn cycle(db: &Database, context: &MaintenanceContext) -> Result<MaintenanceReport> {
    let read_memory = context.read_request_provider.request_budget().unwrap();
    let _ = db;
    context.run_cycle(CurrencyGate::Always, read_memory)
}

fn assert_one_committed_step(store: &Store, before: (usize, usize), observed: &Observed) {
    let reclaimed = observed.reclaimed_pages.load(Ordering::SeqCst) as usize;
    assert!(
        reclaimed >= STEP_EXTENTS / 2 && reclaimed <= STEP_EXTENTS,
        "exactly one full bounded step (not the ordinary 64-extent commit): {reclaimed}"
    );
    let after = backlog(&store.db);
    assert!(before.1 > STEP_EXTENTS, "fixture must exceed one step: {before:?}");
    assert!(after.1 < before.1, "the committed step stays durable: {before:?} -> {after:?}");
    assert!(
        after.1 + reclaimed >= before.1,
        "the remainder is untouched beyond that one step: {before:?} -> {after:?} (reclaimed {reclaimed})"
    );
    assert!(after.1 > 0, "a remainder is left to resume: {after:?}");
}

/// Row 2 (worker_stop): a stop requested after exactly one committed step returns the existing
/// typed error; the step stays durable, the rest is untouched, and a later cycle resumes it.
#[test]
fn worker_stop_after_one_committed_step_leaves_a_resumable_remainder() {
    let store = store_with_backlog();
    let before = backlog(&store.db);
    let stop = Arc::new(AtomicBool::new(false));
    let mut context = store.db.maintenance_context();
    context.worker_stop = Some(Arc::clone(&stop));
    let observed = Arc::new(Observed::default());
    redb::commit_work::reset_commit_pages_for_test();
    let (observer_state, observer_stop) = (Arc::clone(&observed), Arc::clone(&stop));
    let result = resident_history_test_seams::with_checkpoint_observer(
        Arc::new(move || {
            account(&observer_state);
            if observer_state.reclaimed_pages.load(Ordering::SeqCst) > 0 {
                observer_stop.store(true, Ordering::SeqCst);
            }
        }),
        || cycle(&store.db, &context),
    );
    account(&observed);
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert_eq!(observed.commits.load(Ordering::SeqCst), 1, "exactly one commit before the stop");
    assert_one_committed_step(&store, before, &observed);
    drop(context);
    // A later ordinary cycle resumes and finishes it.
    for _ in 0..4 {
        store.db.run_maintenance_cycle().unwrap();
    }
    assert!(backlog(&store.db).1 <= 128, "{:?}", backlog(&store.db));
}

/// Row 2 (resource_closed): the same stop through the closed-resource flag.
#[test]
fn resource_closed_after_one_committed_step_leaves_a_resumable_remainder() {
    let store = store_with_backlog();
    let before = backlog(&store.db);
    let context = store.db.maintenance_context();
    let observed = Arc::new(Observed::default());
    redb::commit_work::reset_commit_pages_for_test();
    let (observer_state, flag) = (Arc::clone(&observed), Arc::clone(&store.db.resource_closed));
    let result = resident_history_test_seams::with_checkpoint_observer(
        Arc::new(move || {
            account(&observer_state);
            if observer_state.reclaimed_pages.load(Ordering::SeqCst) > 0 {
                flag.store(true, Ordering::SeqCst);
            }
        }),
        || cycle(&store.db, &context),
    );
    account(&observed);
    store.db.resource_closed.store(false, Ordering::SeqCst);
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert_eq!(observed.commits.load(Ordering::SeqCst), 1, "exactly one commit before the stop");
    assert_one_committed_step(&store, before, &observed);
    drop(context);
    for _ in 0..4 {
        store.db.run_maintenance_cycle().unwrap();
    }
    assert!(backlog(&store.db).1 <= 128, "{:?}", backlog(&store.db));
}

/// Row 3: a foreground writer commits between the cycle's steps. The cycle still completes within
/// its bound, nothing is lost, and later cycles drain what the writer added.
#[test]
fn foreground_writes_between_steps_do_not_extend_the_cycle_or_lose_data() {
    let store = store_with_backlog();
    let context = store.db.maintenance_context();
    let observed = Arc::new(Observed::default());
    let added = Arc::new(AtomicU64::new(0));
    redb::commit_work::reset_commit_pages_for_test();
    let (observer_state, observer_added, db) =
        (Arc::clone(&observed), Arc::clone(&added), Arc::clone(&store.db));
    let result = resident_history_test_seams::with_checkpoint_observer(
        Arc::new(move || {
            account(&observer_state);
            if observer_state.reclaimed_pages.load(Ordering::SeqCst) > 0
                && observer_added.load(Ordering::SeqCst) < 12
            {
                observer_added.fetch_add(1, Ordering::SeqCst);
                churn(&db, 1);
                // The foreground commit's own work is not the cycle's.
                account(&observer_state);
            }
        }),
        || cycle(&store.db, &context),
    );
    account(&observed);
    result.unwrap();
    assert!(added.load(Ordering::SeqCst) >= 1, "the writer must have run between steps");
    // At most 64 reclaim steps plus the cycle's other checkpoints.
    assert!(observed.checkpoints.load(Ordering::SeqCst) <= 64 + 16, "{}", observed.checkpoints.load(Ordering::SeqCst));
    drop(context);
    for _ in 0..6 {
        store.db.run_maintenance_cycle().unwrap();
    }
    assert!(backlog(&store.db).1 <= 400, "{:?}", backlog(&store.db));
    let rows = store.db.execute("SELECT COUNT(*) FROM bulk", &p()).unwrap();
    assert_eq!(rows.rows, vec![vec![Value::Int64(400)]]);
}

/// Row 10: a crash between steps. A child process aborts at the checkpoint after its first
/// committed step; the parent reopens the durable remainder and finishes it.
#[test]
fn a_crash_between_steps_leaves_the_remainder_and_reopen_resumes_it() {
    const ROLE: &str = "WRITECOST_ENGINE_CRASH_CHILD";
    if let Ok(path) = std::env::var(ROLE) {
        let store = store_with_backlog_in(PathBuf::from(path), None);
        let context = store.db.maintenance_context();
        let observed = Arc::new(Observed::default());
        redb::commit_work::reset_commit_pages_for_test();
        let observer_state = Arc::clone(&observed);
        let _ = resident_history_test_seams::with_checkpoint_observer(
            Arc::new(move || {
                account(&observer_state);
                if observer_state.reclaimed_pages.load(Ordering::SeqCst) > 0 {
                    std::process::abort();
                }
            }),
            || cycle(&store.db, &context),
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "database::writecost_reclaim_cancellation_tests::a_crash_between_steps_leaves_the_remainder_and_reopen_resumes_it",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ROLE, &path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must die by abort");
    let db = Database::open(&path).unwrap();
    let rows = db.execute("SELECT COUNT(*) FROM bulk", &p()).unwrap();
    assert_eq!(rows.rows, vec![vec![Value::Int64(400)]], "committed data survives the crash");
    let remainder = backlog(&db);
    assert!(remainder.1 > 0, "the durable remainder survives: {remainder:?}");
    for _ in 0..4 {
        db.run_maintenance_cycle().unwrap();
    }
    assert!(backlog(&db).1 <= 128, "{:?}", backlog(&db));
}
