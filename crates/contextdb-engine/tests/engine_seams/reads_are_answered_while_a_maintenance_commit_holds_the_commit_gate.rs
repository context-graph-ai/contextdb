//! An ordinary read is answered from its captured committed view while a
//! maintenance commit of vector maintenance holds the writer commit gate.
//!
//! Promise (contextdb-intent, Policy 22): ordinary relational and graph reads
//! stay usable while maintenance runs, and bookkeeping never forces a healthy
//! read to be retried. A read's answer is built from a published immutable root,
//! so it cannot depend on a later maintenance commit finishing; the only
//! synchronization its publication needs is the erasure gate, which keeps a
//! purge from passing a result that is being published.
//!
//! A real file-backed maintenance cycle is held inside one of its commits (a
//! construction batch or a generation publication), under
//! the commit gate, by an observer installed on the maintenance thread. While it
//! is held, reads run on the test thread. Every read is observed for a recorded
//! wait on the commit gate: the observer releases the commit the moment a wait is
//! recorded, so an implementation that waits fails an assertion on the recorded
//! wait instead of hanging behind the holder. Timeouts are deadlock guards only.
//!
//! Reason (2026-10-16): scheduled maintenance cycles made one-row reads wait
//! behind each maintenance commit's commit gate, so a read crossing the active-time
//! allowance was refused. No earlier test admits a read while a real maintenance commit is held
//! at its gate. The rows differ in what the read is (point, graph, continuation,
//! after a purge, beside an ordinary writer's publication) and share the harness.

use contextdb_core::read_contract::{ReadFailureKind, ReadLimits};
use contextdb_core::{Error, Value};
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe as observe_commits};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, MetadataRequest, OwnerReadConfig, ReadSession,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// Deadlock guard only: nothing here asserts how long anything took.
const GUARD: Duration = Duration::from_secs(120);
const PLAIN_ROWS: i64 = 10;

fn no_params() -> HashMap<String, Value> {
    HashMap::new()
}

struct Fixture {
    directory: tempfile::TempDir,
    owner_root: Option<std::path::PathBuf>,
    database: Arc<Database>,
    source: Uuid,
    target: Uuid,
}

fn fixture() -> Fixture {
    fixture_with(false)
}

/// The same store, opened as an owner that serves reads on its own channel.
fn owner_fixture() -> Fixture {
    fixture_with(true)
}

fn fixture_with(serving: bool) -> Fixture {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().expect("task-scoped directory");
    let owner_root = serving.then(|| {
        let root = directory.path().join("owner-runtime");
        std::fs::create_dir(&root).expect("runtime root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure the runtime root");
        root
    });
    let database = Database::open_with_options(
        directory.path().join("maintenance-reads.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            owner_reads: OwnerReadConfig {
                enabled: serving,
                runtime_dir: owner_root.clone(),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the file-backed store");
    for ddl in [
        "CREATE TABLE plain (id INT PRIMARY KEY, body TEXT)",
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL)",
        "CREATE TABLE erasable (id INT PRIMARY KEY, payload TEXT)",
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
    ] {
        database
            .execute(ddl, &no_params())
            .expect("declare a table");
    }
    for id in 0..PLAIN_ROWS {
        database
            .execute(
                &format!("INSERT INTO plain (id, body) VALUES ({id}, 'row-{id}')"),
                &no_params(),
            )
            .expect("seed a plain row");
        database
            .execute(
                &format!("INSERT INTO erasable (id, payload) VALUES ({id}, 'secret-{id}')"),
                &no_params(),
            )
            .expect("seed an erasable row");
    }
    for id in 0..200 {
        database
            .execute(
                &format!(
                    "INSERT INTO items (id, v) VALUES ({id}, '[{}, 0.5, 1]')",
                    f64::from(id) / 100.0
                ),
                &no_params(),
            )
            .expect("seed a vector row");
    }
    // Build the index once, then make most of it stale: the cycle the tests
    // observe has real maintenance commites to run.
    database
        .run_maintenance_cycle()
        .expect("the first maintenance cycle builds the index");
    for id in 0..160 {
        database
            .execute(
                &format!("UPDATE items SET v = '[0, 1, {}]' WHERE id = {id}", id % 7),
                &no_params(),
            )
            .expect("make a vector stale");
    }
    let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
    database
        .execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $s, $t, 'LINKS')",
            &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::new_v4())),
                ("s".into(), Value::Uuid(source)),
                ("t".into(), Value::Uuid(target)),
            ]),
        )
        .expect("seed an edge");
    Fixture {
        directory,
        owner_root,
        database: Arc::new(database),
        source,
        target,
    }
}

/// A session that is answered by the serving owner over its channel.
fn owner_session(fixture: &Fixture) -> ReadSession {
    let root = fixture.owner_root.as_ref().expect("an owner fixture");
    let session = ReadSession::with_runtime_directory_for_test(root, || {
        ReadSession::open(fixture.directory.path().join("maintenance-reads.db"))
    })
    .expect("the serving owner answers a reader");
    assert_eq!(
        session.route(),
        contextdb_core::read_contract::ReadRoute::Owner,
        "the reads below go to the owner's own read service"
    );
    session
}

/// What the body sees while a real maintenance commit is held at its gate.
struct Held<'scope> {
    /// Releases the held commit. Safe to call more than once.
    release: SyncSender<()>,
    still_held: &'scope dyn Fn() -> bool,
}

impl Held<'_> {
    /// A read answered on another thread (the owner's service), where the
    /// commit-wait observation cannot see it: the answer and whether the commit
    /// was still held when it arrived are the evidence.
    fn answered<T>(&self, read: impl FnOnce() -> T) -> (T, bool) {
        let answer = read();
        (answer, (self.still_held)())
    }

    /// A read run under the commit-wait observation. Returns the read's result
    /// and how many times it recorded a wait on the commit gate. The first
    /// recorded wait releases the held commit so the old behaviour ends at an
    /// assertion on the count rather than hanging.
    fn observed<T>(&self, read: impl FnOnce() -> T) -> (T, usize) {
        let waits = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&waits);
        let release = self.release.clone();
        let result = contextdb_tx::commit_wait_test_seams::observe(
            Arc::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
                let _ = release.try_send(());
            }),
            read,
        );
        (result, waits.load(Ordering::SeqCst))
    }
}

/// Hold one maintenance commit of a real maintenance cycle at its commit gate,
/// run `body` against it, then let the commit go.
fn while_a_maintenance_commit_is_held<T>(
    database: &Arc<Database>,
    body: impl FnOnce(&Held<'_>) -> T,
) -> (T, bool) {
    let (reached, reached_rx) = sync_channel::<()>(1);
    let (release, release_rx) = sync_channel::<()>(1);
    let release_rx = Mutex::new(release_rx);
    let paused = Arc::new(AtomicBool::new(false));
    let observer_paused = Arc::clone(&paused);
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let observer_seen = Arc::clone(&seen);
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            observe_commits(
                Arc::new(move |at| {
                    observer_seen.lock().expect("seen").push(format!("{at:?}"));
                    if matches!(
                        at,
                        CommitBoundary::BuildBatchBeforeRedb | CommitBoundary::GenerationBeforeRedb
                    ) && !observer_paused.swap(true, Ordering::SeqCst)
                    {
                        reached.send(()).expect("announce the held commit");
                        release_rx
                            .lock()
                            .expect("release gate")
                            .recv_timeout(GUARD)
                            .expect("the held commit is released");
                    }
                }),
                || database.run_maintenance_cycle(),
            )
        });
        let held = reached_rx.recv_timeout(GUARD).is_ok();
        let outcome = if held {
            let still_held = || !worker.is_finished();
            let held = Held {
                release: release.clone(),
                still_held: &still_held,
            };
            Some(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || body(&held),
            )))
        } else {
            None
        };
        let _ = release.try_send(());
        worker
            .join()
            .expect("the maintenance thread ends")
            .expect("the cycle completes after the commit is released");
        match outcome {
            Some(Ok(value)) => (value, true),
            Some(Err(panic)) => std::panic::resume_unwind(panic),
            None => panic!(
                "the maintenance cycle never reached a maintenance commit; boundaries seen: {:?}",
                seen.lock().expect("seen")
            ),
        }
    })
}

fn rows_of(result: contextdb_core::Result<contextdb_engine::QueryResult>) -> Vec<Vec<Value>> {
    result.expect("the read is answered").rows
}

/// A relational point read is answered with the exact captured row, with no
/// recorded commit-gate wait, while the commit is still held.
#[test]
fn a_point_read_is_answered_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = fixture();
    let session = fixture
        .database
        .read_session(ReadLimits::default())
        .expect("a read session");
    let ((answer, waits, held_when_answered), _) =
        while_a_maintenance_commit_is_held(&fixture.database, |held| {
            let (answer, waits) = held
                .observed(|| session.execute("SELECT body FROM plain WHERE id = 7", &no_params()));
            (answer, waits, (held.still_held)())
        });
    assert_eq!(waits, 0, "the read recorded no wait on the commit gate");
    assert!(
        held_when_answered,
        "the answer arrived while the commit was still held"
    );
    assert_eq!(rows_of(answer), vec![vec![Value::Text("row-7".into())]]);
}

/// A graph read is answered with the exact captured edge under the same
/// conditions.
#[test]
fn a_graph_read_is_answered_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = fixture();
    let session = fixture
        .database
        .read_session(ReadLimits::default())
        .expect("a read session");
    let target = fixture.target;
    let params = HashMap::from([("s".into(), Value::Uuid(fixture.source))]);
    let ((answer, waits, held_when_answered), _) = while_a_maintenance_commit_is_held(
        &fixture.database,
        |held| {
            let (answer, waits) = held.observed(|| {
            session.execute(
                "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $s COLUMNS (b.id AS t))",
                &params,
            )
        });
            (answer, waits, (held.still_held)())
        },
    );
    assert_eq!(waits, 0, "the read recorded no wait on the commit gate");
    assert!(
        held_when_answered,
        "the answer arrived while the commit was still held"
    );
    assert_eq!(rows_of(answer), vec![vec![Value::Uuid(target)]]);
}

/// A cursor opened before the commit is held continues, page by page, from its
/// own captured view while the commit is held.
#[test]
fn a_cursor_continues_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = fixture();
    let session = fixture
        .database
        .read_session(ReadLimits {
            cursor_page_rows: 1,
            ..ReadLimits::default()
        })
        .expect("a read session");
    let mut cursor = session
        .open_cursor("SELECT id FROM plain ORDER BY id", &no_params())
        .expect("open a cursor");
    assert_eq!(cursor.first_page().rows, vec![vec![Value::Int64(0)]]);
    let ((page, waits, held_when_answered), _) =
        while_a_maintenance_commit_is_held(&fixture.database, |held| {
            let (page, waits) = held.observed(|| cursor.fetch(NonZeroUsize::new(1)));
            (page, waits, (held.still_held)())
        });
    assert_eq!(
        waits, 0,
        "the continuation recorded no wait on the commit gate"
    );
    assert!(
        held_when_answered,
        "the page arrived while the commit was still held"
    );
    assert_eq!(
        page.expect("the next page").rows,
        vec![vec![Value::Int64(1)]]
    );
}

/// A result captured before a purge committed is refused with the
/// authoritative-erasure failure, never served, and its refusal needs no wait on
/// the commit gate while the commit is held.
#[test]
fn a_result_captured_before_a_purge_is_refused_as_erased_while_a_maintenance_commit_is_held() {
    let fixture = fixture();
    let session = fixture
        .database
        .read_session(ReadLimits::default())
        .expect("a read session");
    let cursor_session_limits = ReadLimits {
        cursor_page_rows: 1,
        ..ReadLimits::default()
    };
    let capturing = fixture
        .database
        .read_session(cursor_session_limits)
        .expect("a second read session");
    let mut cursor = capturing
        .open_cursor("SELECT payload FROM erasable ORDER BY id", &no_params())
        .expect("open a cursor over the erasable table");
    assert_eq!(cursor.first_page().rows.len(), 1);
    fixture
        .database
        .execute("PURGE FROM erasable WHERE id = 3", &no_params())
        .expect("the authoritative purge commits");
    let ((answer, waits, cursor_page), _) =
        while_a_maintenance_commit_is_held(&fixture.database, |held| {
            let (answer, waits) = held.observed(|| {
                session.execute("SELECT payload FROM erasable ORDER BY id", &no_params())
            });
            let (page, cursor_waits) = held.observed(|| cursor.fetch(NonZeroUsize::new(1)));
            (answer, waits + cursor_waits, page)
        });
    assert_eq!(waits, 0, "neither refusal waited on the commit gate");
    // A read that began after the purge sees the post-purge table.
    assert_eq!(rows_of(answer).len(), PLAIN_ROWS as usize - 1);
    match cursor_page {
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::AuthoritativeErasure,
            "a cursor captured before the purge is refused as erased"
        ),
        other => panic!("a cursor captured before the purge must not be served: {other:?}"),
    }
}

/// A read admitted while an ordinary writer's commit is held after its durable
/// write and before its memory publication sees exactly the state before that
/// commit, and records no wait on the commit gate.
#[test]
fn a_read_beside_a_writer_held_before_its_memory_publication_sees_the_old_state() {
    let fixture = fixture();
    let session = fixture
        .database
        .read_session(ReadLimits::default())
        .expect("a read session");
    let (reached, reached_rx) = sync_channel::<()>(1);
    let (release, release_rx) = sync_channel::<()>(1);
    let release_rx = Mutex::new(release_rx);
    let paused = Arc::new(AtomicBool::new(false));
    let observer_paused = Arc::clone(&paused);
    let database = Arc::clone(&fixture.database);
    let (answer, waits, count) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            observe_commits(
                Arc::new(move |at| {
                    if at == CommitBoundary::BeforeMemoryPublication
                        && !observer_paused.swap(true, Ordering::SeqCst)
                    {
                        reached.send(()).expect("announce the held commit");
                        release_rx
                            .lock()
                            .expect("release gate")
                            .recv_timeout(GUARD)
                            .expect("the held commit is released");
                    }
                }),
                || {
                    database.execute(
                        "INSERT INTO plain (id, body) VALUES (100, 'new')",
                        &no_params(),
                    )
                },
            )
        });
        reached_rx
            .recv_timeout(GUARD)
            .expect("the writer reached its memory publication boundary");
        let waits = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&waits);
        let release_on_wait = release.clone();
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            contextdb_tx::commit_wait_test_seams::observe(
                Arc::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    let _ = release_on_wait.try_send(());
                }),
                || {
                    let answer =
                        session.execute("SELECT body FROM plain WHERE id = 100", &no_params());
                    let count = session.execute("SELECT id FROM plain", &no_params());
                    (answer, count)
                },
            )
        }));
        let _ = release.try_send(());
        writer
            .join()
            .expect("the writer thread ends")
            .expect("the held commit completes");
        let (answer, count) = observed.expect("the observed reads");
        (answer, waits.load(Ordering::SeqCst), count)
    });
    assert_eq!(waits, 0, "the reads recorded no wait on the commit gate");
    assert_eq!(
        rows_of(answer),
        Vec::<Vec<Value>>::new(),
        "the held commit is not visible before it is published"
    );
    assert_eq!(rows_of(count).len(), PLAIN_ROWS as usize);
}

/// A cursor opened before later commits keeps returning exactly the rows of its
/// own view: nothing newer is substituted into a later page.
#[test]
fn a_cursor_never_substitutes_a_newer_state_into_a_later_page() {
    let fixture = fixture();
    let session = fixture
        .database
        .read_session(ReadLimits {
            cursor_page_rows: 1,
            ..ReadLimits::default()
        })
        .expect("a read session");
    let mut cursor = session
        .open_cursor("SELECT id FROM plain ORDER BY id", &no_params())
        .expect("open a cursor");
    fixture
        .database
        .execute(
            "INSERT INTO plain (id, body) VALUES (100, 'later')",
            &no_params(),
        )
        .expect("a later commit");
    let mut seen = cursor.first_page().rows.clone();
    let mut has_more = cursor.first_page().has_more;
    while has_more {
        let page = cursor.fetch(NonZeroUsize::new(1)).expect("a later page");
        seen.extend(page.rows.clone());
        has_more = page.has_more;
    }
    assert_eq!(
        seen,
        (0..PLAIN_ROWS)
            .map(|id| vec![Value::Int64(id)])
            .collect::<Vec<_>>(),
        "exactly the rows of the cursor's own view, in order"
    );
}

/// The same point read, answered by the serving owner over its channel.
#[test]
fn an_owner_answers_a_point_read_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = owner_fixture();
    let session = owner_session(&fixture);
    let ((answer, held_when_answered), _) =
        while_a_maintenance_commit_is_held(&fixture.database, |held| {
            held.answered(|| session.execute("SELECT body FROM plain WHERE id = 7", &no_params()))
        });
    assert!(
        held_when_answered,
        "the owner answered while the commit was still held"
    );
    assert_eq!(rows_of(answer), vec![vec![Value::Text("row-7".into())]]);
}

/// The same graph read, answered by the serving owner.
#[test]
fn an_owner_answers_a_graph_read_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = owner_fixture();
    let session = owner_session(&fixture);
    let target = fixture.target;
    let params = HashMap::from([("s".into(), Value::Uuid(fixture.source))]);
    let ((answer, held_when_answered), _) = while_a_maintenance_commit_is_held(
        &fixture.database,
        |held| {
            held.answered(|| {
            session.execute(
                "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $s COLUMNS (b.id AS t))",
                &params,
            )
        })
        },
    );
    assert!(
        held_when_answered,
        "the owner answered while the commit was still held"
    );
    assert_eq!(rows_of(answer), vec![vec![Value::Uuid(target)]]);
}

/// A metadata request (the table inventory) is answered by the owner while
/// the commit is held: it samples the committed log position without waiting
/// for the gate.
#[test]
fn an_owner_answers_a_metadata_request_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = owner_fixture();
    let session = owner_session(&fixture);
    let ((answer, held_when_answered), _) =
        while_a_maintenance_commit_is_held(&fixture.database, |held| {
            held.answered(|| session.metadata(MetadataRequest::Tables, None))
        });
    assert!(
        held_when_answered,
        "the owner answered while the commit was still held"
    );
    answer.expect("the table inventory is answered");
}

/// An EXPLAIN is answered by the owner while the commit is held, naming the plan
/// of the statement without running it.
#[test]
fn an_owner_answers_an_explain_while_a_maintenance_commit_holds_the_commit_gate() {
    let fixture = owner_fixture();
    let session = owner_session(&fixture);
    let ((answer, held_when_answered), _) =
        while_a_maintenance_commit_is_held(&fixture.database, |held| {
            held.answered(|| {
                session.metadata(
                    MetadataRequest::Explain {
                        sql: "SELECT body FROM plain WHERE id = 7".to_owned(),
                        params: HashMap::new(),
                    },
                    None,
                )
            })
        });
    assert!(
        held_when_answered,
        "the owner answered while the commit was still held"
    );
    answer.expect("the plan is answered without running the statement");
}
