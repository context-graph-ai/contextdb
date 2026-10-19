use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use contextdb_core::Value;
use contextdb_core::read_contract::{ReadFailureKind, ReadLimits, ReadRoute};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};

const ROLE: &str = "CONTEXTDB_CURSOR_HANDOVER_ROLE";
const PATH: &str = "CONTEXTDB_CURSOR_HANDOVER_PATH";
const RUNTIME_ROOT: &str = "CONTEXTDB_CURSOR_HANDOVER_RUNTIME_ROOT";

#[test]
fn writer_child() {
    let Ok(role) = std::env::var(ROLE) else {
        return;
    };
    let path = std::env::var(PATH).expect("child store path");
    let db = match std::env::var_os(RUNTIME_ROOT) {
        Some(root) => Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    runtime_dir: Some(root.into()),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        ),
        None => Database::open(&path),
    }
    .expect("child writer opens store");
    if role == "arriving" {
        db.execute(
            "INSERT INTO handover_rows (id) VALUES (1000)",
            &HashMap::new(),
        )
        .expect("arriving writer commits a visible change");
        drop(db);
        println!("COMMITTED");
    } else {
        println!("HOLDING");
        std::io::stdout().flush().expect("flush holding marker");
        let mut input = String::new();
        loop {
            input.clear();
            std::io::stdin()
                .lock()
                .read_line(&mut input)
                .expect("writer command");
            match input.trim() {
                "purge" => {
                    db.execute("PURGE FROM handover_rows WHERE id = 110", &HashMap::new())
                        .expect("purge a row in the affected cursor");
                    println!("PURGED");
                    std::io::stdout().flush().expect("flush purge marker");
                }
                "release" => break,
                other => panic!("unknown writer command: {other}"),
            }
        }
        drop(db);
        println!("RELEASED");
    }
    std::io::stdout().flush().expect("flush child marker");
}

struct WriterChild {
    child: Child,
    output: BufReader<ChildStdout>,
}

impl WriterChild {
    fn spawn(role: &str, path: &Path) -> Self {
        Self::spawn_in_runtime(role, path, None)
    }

    fn spawn_in_runtime(role: &str, path: &Path, runtime_root: Option<&Path>) -> Self {
        let mut command = Command::new(std::env::current_exe().expect("test binary"));
        command
            .arg("--exact")
            .arg(super::child_test_filter(module_path!(), "writer_child"))
            .arg("--nocapture")
            .env(ROLE, role)
            .env(PATH, path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some(runtime_root) = runtime_root {
            command.env(RUNTIME_ROOT, runtime_root);
        }
        let mut child = command.spawn().expect("spawn writer child");
        let output = BufReader::new(child.stdout.take().expect("child stdout"));
        Self { child, output }
    }

    fn wait_for(&mut self, marker: &str) {
        let mut line = String::new();
        while self.output.read_line(&mut line).expect("read child output") != 0 {
            if line.contains(marker) {
                return;
            }
            line.clear();
        }
        panic!("child exited before {marker}");
    }

    fn finish(mut self, marker: &str) {
        self.wait_for(marker);
        assert!(self.child.wait().expect("wait for child").success());
    }

    fn release(mut self) {
        writeln!(self.child.stdin.as_mut().expect("child stdin"), "release")
            .expect("release writer");
        self.finish("RELEASED");
    }

    fn purge(&mut self) {
        writeln!(self.child.stdin.as_mut().expect("child stdin"), "purge").expect("request purge");
        self.wait_for("PURGED");
    }
}

#[cfg(unix)]
#[test]
fn owner_cursor_handover_uses_the_supplied_runtime_root() {
    use std::os::unix::fs::PermissionsExt as _;

    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("supplied-runtime.db");
    let runtime_root = dir.path().join("state");
    std::fs::create_dir(&runtime_root).expect("create supplied runtime root");
    std::fs::set_permissions(&runtime_root, std::fs::Permissions::from_mode(0o700))
        .expect("secure supplied runtime root");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create rows");
    for id in 0..120 {
        db.execute(
            &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed row");
    }
    drop(db);

    let mut holder = WriterChild::spawn_in_runtime("holding", &path, Some(&runtime_root));
    holder.wait_for("HOLDING");
    let session = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions {
            limits: ReadLimits {
                result_rows: 200,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
        Some(runtime_root.clone()),
    )
    .expect("open on supplied runtime root");
    assert_eq!(session.route(), ReadRoute::Owner);
    let mut cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("open owner cursor");
    assert_eq!(ids(cursor.first_page()), (0..100).collect::<Vec<_>>());
    assert!(
        std::fs::read_dir(&runtime_root)
            .expect("inspect supplied runtime root")
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with("storage-")),
        "the owner cursor publishes its retained root under the supplied root"
    );
    holder.release();
    WriterChild::spawn_in_runtime("arriving", &path, Some(&runtime_root)).finish("COMMITTED");
    let mut seen = ids(cursor.first_page());
    while seen.len() < 120 {
        let page = cursor.fetch(None).expect("continue retained root");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen, (0..120).collect::<Vec<_>>());
}

fn ids(page: &contextdb_core::read_contract::CursorPage) -> Vec<i64> {
    page.rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

struct PauseBeforeRegistration {
    reached: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

struct ExpiryReleased(mpsc::Sender<()>);

impl ReadSessionTestObserver for ExpiryReleased {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event == ReadSessionEvent::OwnerCursorExpiryReleased {
            self.0.send(()).expect("report expiry release");
        }
    }
}

fn seed_rows(path: &Path, count: i64) {
    let db = Database::open(path).expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create rows");
    for id in 0..count {
        db.execute(
            &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed row");
    }
}

#[test]
fn small_cursor_admission_work_and_charged_memory_ignore_unrelated_store_size() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let mut measurements = Vec::new();
    for unrelated_rows in [0, 64, 512] {
        let dir = tempfile::tempdir().expect("fixture directory");
        let path = dir.path().join("unrelated-size.db");
        let db = Database::open(&path).expect("open writer");
        db.execute(
            "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create selected table");
        db.execute(
            "CREATE TABLE unrelated_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create unrelated table");
        for id in 0..120 {
            db.execute(
                &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
                &HashMap::new(),
            )
            .expect("seed selected row");
        }
        for id in 0..unrelated_rows {
            db.execute(
                "INSERT INTO unrelated_rows (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), Value::Text("x".repeat(8192))),
                ]),
            )
            .expect("grow unrelated table");
        }
        let file_bytes = std::fs::metadata(&path).expect("store metadata").len();
        let session = db
            .read_session(ReadLimits {
                result_rows: 200,
                ..ReadLimits::default()
            })
            .expect("live owner read session");
        let before = session.charged_bytes().expect("baseline charged memory");
        let backend_bytes = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&backend_bytes);
        let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
            counter.fetch_add(bytes as u64, Ordering::SeqCst);
        }));
        let cursor = session
            .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
            .expect("open small cursor");
        drop(observation);
        assert!(cursor.first_page().has_more);
        let charged_delta = session
            .charged_bytes()
            .expect("cursor charged memory")
            .saturating_sub(before);
        measurements.push((
            file_bytes,
            backend_bytes.load(Ordering::SeqCst),
            charged_delta,
        ));
        drop(cursor);
        drop(session);
        db.close().expect("close store");
    }
    assert!(measurements.windows(2).all(|pair| pair[0].0 < pair[1].0));
    eprintln!("cursor admission (store bytes, backend bytes, charged bytes): {measurements:?}");
    let baseline = measurements[0];
    for (file_bytes, work_bytes, charged_bytes) in measurements {
        assert!(
            work_bytes <= baseline.1.saturating_add(64 * 1024),
            "cursor admission read {work_bytes} bytes at store size {file_bytes}, baseline {}",
            baseline.1
        );
        assert!(
            charged_bytes <= baseline.2.saturating_add(64 * 1024),
            "cursor admission charged {charged_bytes} bytes at store size {file_bytes}, baseline {}",
            baseline.2
        );
    }
}

#[test]
fn complete_first_page_releases_the_owner_cursor_hold() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("complete-first-page.db");
    seed_rows(&path, 1);
    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let session = ReadSession::open(&path).expect("open owner route");
    let cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("open complete cursor");
    assert!(!cursor.first_page().has_more);
    assert!(!cursor.is_live());
    holder.release();
    let writer = Database::open(&path).expect("replacement writer");
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("observe released root")
            .snapshots,
        0,
        "the cursor object remains alive but its root is released"
    );
    let _ = &cursor;
}

#[test]
fn final_fetch_releases_pages_before_the_cursor_object_is_dropped() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("complete-last-page.db");
    seed_rows(&path, 120);
    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let session = ReadSession::open(&path).expect("open owner route");
    let mut cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("open paged cursor");
    assert!(cursor.first_page().has_more);
    holder.release();
    let writer = Database::open(&path).expect("replacement writer");
    writer
        .execute(
            "INSERT INTO handover_rows (id) VALUES (1000)",
            &HashMap::new(),
        )
        .expect("rewrite after handover");
    let retained = writer
        .__external_snapshot_retention_for_test()
        .expect("observe held old pages");
    assert!(retained.retained_pages > 0);
    let last = cursor.fetch(None).expect("finish old snapshot");
    assert_eq!(ids(&last), (100..120).collect::<Vec<_>>());
    assert!(!last.has_more);
    assert!(!cursor.is_live());
    for id in 1001..1004 {
        writer
            .execute(
                &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
                &HashMap::new(),
            )
            .expect("advance reclamation");
    }
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("observe released root")
            .snapshots,
        0
    );
    assert!(
        retained.retained_page_allocations.iter().any(|page| {
            !writer
                .__external_snapshot_page_allocated_for_test(*page)
                .expect("inspect physical page allocation")
        }),
        "the replacement writer must reclaim at least one formerly held page"
    );
    let _ = &cursor;
}

#[cfg(target_os = "linux")]
#[test]
fn memory_refusal_on_file_continuation_releases_registration_and_presence() {
    use contextdb_core::Error;
    use std::os::fd::AsRawFd;

    fn reader_presence(path: &Path) -> bool {
        const WIDTH: i64 = 1 << 60;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open file to inspect reader presence");
        for base in [WIDTH, 2 * WIDTH] {
            let mut lock = libc::flock {
                l_type: libc::F_WRLCK as libc::c_short,
                l_whence: libc::SEEK_SET as libc::c_short,
                l_start: base,
                l_len: WIDTH,
                l_pid: 0,
            };
            // SAFETY: lock is initialized and the file descriptor stays open.
            assert_ne!(
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) },
                -1
            );
            if lock.l_type != libc::F_UNLCK as libc::c_short {
                return true;
            }
        }
        false
    }

    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("continuation-memory.db");
    let db = Database::open(&path).expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create rows");
    for id in 0..3 {
        db.execute(
            "INSERT INTO handover_rows (id, payload) VALUES ($id, 'small')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .expect("seed page");
    }
    db.close().expect("close seed writer");
    // The file continuation's source reserves at least 64 KiB for source
    // state plus its fixed verifier reservation. The owner's small first page
    // fits this limit, while loading the committed file cannot.
    let budget = 64 * 1024;

    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let session = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            memory_limit: Some(budget),
            limits: ReadLimits {
                memory: budget as u64,
                result_bytes: 1024 * 1024,
                cursor_page_rows: 1,
                cursor_page_bytes: 1024 * 1024,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
    )
    .expect("open bounded owner session");
    assert_eq!(session.route(), ReadRoute::Owner);
    let mut cursor = session
        .open_cursor("SELECT id, payload FROM handover_rows", &HashMap::new())
        .expect("open first small page");
    assert_eq!(cursor.first_page().rows.len(), 1);
    assert!(cursor.first_page().has_more);
    holder.release();
    let writer = Database::open(&path).expect("open replacement writer");
    assert!(
        reader_presence(&path),
        "live cursor holds presence before fetch"
    );
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("observe registered root")
            .snapshots,
        1
    );

    let error = cursor
        .fetch(None)
        .expect_err("continuation source reservation exceeds its memory budget");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "{error:?}"
    );
    assert!(!cursor.is_live(), "memory refusal terminates the cursor");
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("observe released root")
            .snapshots,
        0
    );
    assert!(
        !reader_presence(&path),
        "presence releases while cursor stays alive"
    );
    let _ = &cursor;
}

#[test]
fn idle_expiry_reclaims_pages_without_another_client_request() {
    use contextdb_core::read_contract::{CursorExpiryKind, ReadFailureKind};

    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("idle-expiry.db");
    seed_rows(&path, 120);
    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let clock = Arc::new(ManualDeadlineClock::at(1000));
    let (expired_tx, expired_rx) = mpsc::channel();
    let session = ReadSession::open_with_clock_and_observer_for_test(
        &path,
        ReadSessionOptions {
            limits: ReadLimits {
                cursor_idle_ms: 5000,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
        clock.clone(),
        Arc::new(ExpiryReleased(expired_tx)),
    )
    .expect("open owner route");
    let mut cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("open paged cursor");
    assert!(cursor.first_page().has_more);
    holder.release();
    let writer = Database::open(&path).expect("replacement writer");
    writer
        .execute(
            "INSERT INTO handover_rows (id) VALUES (1000)",
            &HashMap::new(),
        )
        .expect("rewrite after handover");
    let retained = writer
        .__external_snapshot_retention_for_test()
        .expect("observe held old pages");
    assert!(retained.retained_pages > 0);
    clock.advance_to(6001);
    expired_rx
        .recv()
        .expect("deadline watcher releases the root");
    for id in 1001..1004 {
        writer
            .execute(
                &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
                &HashMap::new(),
            )
            .expect("advance reclamation");
    }
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("observe released root")
            .snapshots,
        0
    );
    assert!(retained.retained_page_allocations.iter().any(|page| {
        !writer
            .__external_snapshot_page_allocated_for_test(*page)
            .expect("inspect physical page allocation")
    }));
    match cursor.fetch(None) {
        Err(contextdb_core::Error::ReadFailure(failure)) => {
            assert_eq!(failure.kind(), ReadFailureKind::CursorExpired);
            assert!(matches!(
                failure.detail(),
                contextdb_core::read_contract::ReadFailureDetail::CursorExpired {
                    expiry: CursorExpiryKind::Idle
                }
            ));
        }
        other => panic!("expired cursor returned {other:?}"),
    }
}

impl ReadSessionTestObserver for PauseBeforeRegistration {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event == ReadSessionEvent::BeforeOwnerCursorHandoffRegistration {
            self.reached.send(()).expect("report response receipt");
            self.resume
                .lock()
                .expect("handoff pause")
                .recv()
                .expect("resume registration");
        }
    }
}

#[test]
fn owner_exit_after_open_response_does_not_break_client_registration() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("paused-handoff.db");
    let db = Database::open(&path).expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create rows");
    for id in 0..120 {
        db.execute(
            &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed row");
    }
    drop(db);

    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let observer = Arc::new(PauseBeforeRegistration {
        reached: reached_tx,
        resume: Mutex::new(resume_rx),
    });
    let session = ReadSession::open_with_observer_for_test(
        &path,
        ReadSessionOptions {
            limits: ReadLimits {
                result_rows: 200,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
        observer,
    )
    .expect("open owner route");
    assert_eq!(session.route(), ReadRoute::Owner);
    let opened = std::thread::spawn(move || {
        session.open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
    });
    reached_rx
        .recv()
        .expect("first page has reached the client");
    holder.release();
    WriterChild::spawn("arriving", &path).finish("COMMITTED");
    resume_tx.send(()).expect("finish exact registration");
    let mut cursor = opened
        .join()
        .expect("join opener")
        .expect("client registers old root");
    let mut seen = ids(cursor.first_page());
    while seen.len() < 120 {
        let page = cursor.fetch(None).expect("continue old root");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen, (0..120).collect::<Vec<_>>());
}

#[test]
fn purge_during_handoff_invalidates_affected_continuation_after_owner_exit() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("erased-handoff.db");
    let db = Database::open(&path).expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create affected table");
    db.execute(
        "CREATE TABLE other_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create unaffected table");
    for id in 0..120 {
        db.execute(
            &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed affected row");
        db.execute(
            &format!("INSERT INTO other_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed unaffected row");
    }
    drop(db);

    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let options = ReadSessionOptions {
        limits: ReadLimits {
            result_rows: 200,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    };
    let unaffected_session =
        ReadSession::open_with_options(&path, options.clone()).expect("unaffected owner route");
    let mut unaffected = unaffected_session
        .open_cursor("SELECT id FROM other_rows ORDER BY id", &HashMap::new())
        .expect("open unaffected cursor before purge");
    assert!(unaffected.first_page().has_more);

    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let observer = Arc::new(PauseBeforeRegistration {
        reached: reached_tx,
        resume: Mutex::new(resume_rx),
    });
    let affected_session = ReadSession::open_with_observer_for_test(&path, options, observer)
        .expect("affected owner route");
    let opening = std::thread::spawn(move || {
        affected_session.open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
    });
    reached_rx.recv().expect("affected first page received");
    holder.purge();
    holder.release();
    resume_tx.send(()).expect("finish exact registration");
    let mut affected = opening
        .join()
        .expect("join affected opener")
        .expect("register affected old root");
    assert!(affected.first_page().has_more);
    match affected.fetch(None) {
        Err(contextdb_core::Error::ReadFailure(failure)) => {
            assert_eq!(failure.kind(), ReadFailureKind::AuthoritativeErasure);
        }
        other => panic!("old erased rows escaped handoff: {other:?}"),
    }
    assert!(
        !affected.is_live(),
        "erasure terminates the affected cursor"
    );

    let mut seen = ids(unaffected.first_page());
    while seen.len() < 120 {
        let page = unaffected
            .fetch(None)
            .expect("continue unaffected old root");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen, (0..120).collect::<Vec<_>>());
    assert!(!unaffected.is_live());

    let replacement = Database::open(&path).expect("inspect released reader holds");
    let retained = replacement
        .__external_snapshot_retention_for_test()
        .expect("observe snapshot holds after both terminal cursors");
    assert_eq!(retained.snapshots, 0);
    assert_eq!(retained.retained_pages, 0);
}

#[test]
fn cursor_opened_before_writer_handover_finishes_its_snapshot() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("handover.db");
    let db = Database::open(&path).expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create rows");
    for id in 0..250 {
        db.execute(
            &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed row");
    }
    drop(db);

    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let limits = ReadLimits {
        result_rows: 500,
        ..ReadLimits::default()
    };
    let session = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            limits,
            ..ReadSessionOptions::default()
        },
    )
    .expect("open alongside writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    let mut cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("open owner cursor");
    let mut seen = ids(cursor.first_page());
    assert_eq!(seen.len(), 100);
    assert!(cursor.first_page().has_more);

    holder.release();
    WriterChild::spawn("arriving", &path).finish("COMMITTED");

    loop {
        let page = cursor
            .fetch(None)
            .expect("finish opened snapshot after writer handover");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen.len(), 250);
    assert_eq!(seen.iter().copied().collect::<HashSet<_>>().len(), 250);
    assert_eq!(seen, (0..250).collect::<Vec<_>>());
}

#[test]
fn multiple_owner_pages_continue_while_the_replacement_writer_is_live() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("live-handover.db");
    let db = Database::open(&path).expect("seed writer");
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create rows");
    for id in 0..12 {
        db.execute(
            &format!("INSERT INTO handover_rows (id) VALUES ({id})"),
            &HashMap::new(),
        )
        .expect("seed row");
    }
    drop(db);
    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let session = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            limits: ReadLimits {
                cursor_page_rows: 2,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
    )
    .expect("open owner session");
    let mut cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("open owner cursor");
    let mut seen = ids(cursor.first_page());
    seen.extend(ids(&cursor.fetch(None).expect("second owner page")));
    seen.extend(ids(&cursor
        .fetch(std::num::NonZeroUsize::new(1))
        .expect("short owner page")));
    assert_eq!(seen, (0..5).collect::<Vec<_>>());
    holder.release();
    let writer = Database::open(&path).expect("replacement writer");
    writer
        .execute(
            "INSERT INTO handover_rows (id) VALUES (1000)",
            &HashMap::new(),
        )
        .expect("replacement commit");
    loop {
        let page = cursor.fetch(None).expect("continue while writer is live");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen, (0..12).collect::<Vec<_>>());
    cursor.close().expect("close local continuation");
    drop(writer);
}

#[test]
fn an_abrupt_owner_exit_preserves_old_rows_through_replacement_rewrites() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("abrupt-handover.db");
    let database = Database::open(&path).expect("seed writer");
    database
        .execute(
            "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create rows");
    let expected: Vec<_> = (0..32_i64)
        .map(|id| {
            let payload = format!("original-{id}-{}", "x".repeat(256));
            database
                .execute(
                    "INSERT INTO handover_rows (id, payload) VALUES ($id, $payload)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(id)),
                        ("payload".to_owned(), Value::Text(payload.clone())),
                    ]),
                )
                .expect("seed original row");
            vec![Value::Int64(id), Value::Text(payload)]
        })
        .collect();
    drop(database);
    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let session = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            limits: ReadLimits {
                cursor_page_rows: 4,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
    )
    .expect("open owner session");
    let mut cursor = session
        .open_cursor(
            "SELECT id, payload FROM handover_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("open retained owner cursor");
    let mut seen = cursor.first_page().rows.clone();
    assert_eq!(seen, expected[..4]);
    assert!(cursor.first_page().has_more);

    // kill + wait is the synchronization: no graceful database shutdown or
    // owner cursor cleanup can protect the remaining pages for this client.
    holder.child.kill().expect("kill the owner process");
    let exit = holder.child.wait().expect("reap the killed owner");
    assert!(!exit.success());
    let writer = Database::open(&path).expect("recover and acquire replacement writer");
    for generation in 0..4 {
        for id in 0..32_i64 {
            writer
                .execute(
                    "UPDATE handover_rows SET payload = $payload WHERE id = $id",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(id)),
                        (
                            "payload".to_owned(),
                            Value::Text(format!(
                                "replacement-{generation}-{id}-{}",
                                "y".repeat(256)
                            )),
                        ),
                    ]),
                )
                .expect("rewrite rows through repeated copy-on-write commits");
        }
    }
    for id in (5..32_i64).step_by(2) {
        writer
            .execute(
                "DELETE FROM handover_rows WHERE id = $id",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("delete rows still visible to the original cursor");
    }
    writer
        .execute(
            "INSERT INTO handover_rows VALUES (1000, 'new row')",
            &HashMap::new(),
        )
        .expect("commit a new row outside the cursor snapshot");
    loop {
        let page = cursor
            .fetch(None)
            .expect("read old pages retained across abrupt owner exit");
        seen.extend(page.rows);
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen, expected);
    cursor.close().expect("release retained pages");
    drop(writer);
}

fn small_budget_handover(deliver_owner_rows: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bounded-replay.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE handover_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..50 {
        db.execute(
            &format!("INSERT INTO handover_rows VALUES ({id})"),
            &HashMap::new(),
        )
        .unwrap();
    }
    drop(db);
    let mut holder = WriterChild::spawn("holding", &path);
    holder.wait_for("HOLDING");
    let statement = if deliver_owner_rows == 0 {
        "SELECT SUM(id) FROM handover_rows"
    } else {
        "SELECT id FROM handover_rows ORDER BY id"
    };
    let open = |work| -> contextdb_core::Result<_> {
        let session = ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                limits: ReadLimits {
                    work,
                    cursor_page_rows: 2,
                    ..ReadLimits::default()
                },
                ..ReadSessionOptions::default()
            },
        )?;
        let cursor = session.open_cursor(statement, &HashMap::new())?;
        Ok((session, cursor))
    };
    // Find the actual minimum admission cost; do not bake kernel work counts
    // into this fixture. The opened statement must remain resumable there.
    let is_work_refusal = |error: &contextdb_core::Error| {
        matches!(error,
        contextdb_core::Error::ReadFailure(failure) if matches!(failure.detail(),
        contextdb_core::read_contract::ReadFailureDetail::OwnerLimitExceeded(detail)
        if detail.limit == contextdb_core::read_contract::ReadFailureLimit::Work))
    };
    let mut upper = 1;
    loop {
        match open(upper) {
            Ok(_) => break,
            Err(error) if is_work_refusal(&error) => {
                upper *= 2;
                assert!(upper <= 1_000_000);
            }
            Err(error) => panic!("calibrate cursor admission: {error:?}"),
        }
    }
    let mut lower = 1;
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        match open(middle) {
            Ok(_) => upper = middle,
            Err(error) if is_work_refusal(&error) => lower = middle + 1,
            Err(error) => panic!("calibrate cursor admission: {error:?}"),
        }
    }
    let (_session, mut cursor) = open(upper).unwrap();
    let mut seen = ids(cursor.first_page());
    if deliver_owner_rows == 0 {
        assert!(
            seen.is_empty(),
            "an unfinished aggregate starts with an empty page"
        );
    }
    assert!(cursor.first_page().has_more);
    for _ in 0..100 {
        if seen.len() >= deliver_owner_rows {
            break;
        }
        seen.extend(ids(&cursor.fetch(None).unwrap()));
    }
    assert!(seen.len() >= deliver_owner_rows);
    holder.release();
    let too_many = std::num::NonZeroUsize::new(ReadLimits::default().result_rows as usize + 1);
    assert!(
        matches!(cursor.fetch(too_many), Err(contextdb_core::Error::ReadFailure(failure))
        if matches!(failure.detail(), contextdb_core::read_contract::ReadFailureDetail::OwnerLimitExceeded(detail)
            if detail.limit == contextdb_core::read_contract::ReadFailureLimit::ResultRows))
    );
    let replay = cursor.fetch(None).unwrap();
    assert!(
        replay.rows.is_empty(),
        "replay yields after spending one caller allowance of {upper} work units"
    );
    assert!(replay.has_more);
    let mut finished = false;
    for _ in 0..200 {
        let page = cursor.fetch(None).unwrap();
        seen.extend(ids(&page));
        if !page.has_more {
            finished = true;
            break;
        }
    }
    assert!(finished, "each yielded request must retain progress");
    let expected = if deliver_owner_rows == 0 {
        vec![(0..50).sum()]
    } else {
        (0..50).collect::<Vec<_>>()
    };
    assert_eq!(seen, expected);
}

#[test]
fn replay_yields_under_one_public_fetch_budget_and_keeps_its_progress() {
    small_budget_handover(40);
}

#[test]
fn an_empty_owner_first_page_does_not_skip_the_first_replayed_row() {
    small_budget_handover(0);
}

// This integration target has one allocator oracle. Only allocations made on
// the thread inside measure() participate; a header retains the measurement so
// deallocation on another thread, or after the next sample starts, is correct.
// The oracle includes transient vectors and COW buffers, unlike reader charges.
pub(crate) mod append_heap_oracle {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Counters {
        live: AtomicUsize,
        peak: AtomicUsize,
        funding: Option<Arc<AtomicUsize>>,
        unfunded: AtomicUsize,
        allocations: AtomicUsize,
    }
    thread_local! {
        static ACTIVE: Cell<*const Counters> = const { Cell::new(std::ptr::null()) };
    }
    struct Header {
        counters: *const Counters,
    }
    fn expanded(layout: Layout) -> (Layout, usize) {
        let alignment = layout.align().max(std::mem::align_of::<Header>());
        let offset = std::mem::size_of::<Header>().next_multiple_of(alignment);
        (
            Layout::from_size_align(layout.size() + offset, alignment).unwrap(),
            offset,
        )
    }
    struct Allocator;
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let (allocation, offset) = expanded(layout);
            let base = unsafe { System.alloc(allocation) };
            if base.is_null() {
                return base;
            }
            let pointer = unsafe { base.add(offset) };
            let counters = ACTIVE.try_with(Cell::get).unwrap_or(std::ptr::null());
            unsafe {
                pointer
                    .sub(std::mem::size_of::<Header>())
                    .cast::<Header>()
                    .write(Header { counters });
            }
            if !counters.is_null() {
                // Retain the counters without allocating in the allocator hook.
                unsafe { Arc::increment_strong_count(counters) };
                let counters = unsafe { &*counters };
                let live = counters.live.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
                counters.peak.fetch_max(live, Ordering::SeqCst);
                if let Some(funding) = &counters.funding {
                    // No caller code runs between System.alloc and this sample.
                    // Observe the funding at this allocation boundary, including
                    // both buffers of a realloc; never infer order from a peak.
                    counters.allocations.fetch_add(1, Ordering::SeqCst);
                    counters.unfunded.fetch_max(
                        live.saturating_sub(funding.load(Ordering::SeqCst)),
                        Ordering::SeqCst,
                    );
                }
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            let (allocation, offset) = expanded(layout);
            let header = unsafe {
                pointer
                    .sub(std::mem::size_of::<Header>())
                    .cast::<Header>()
                    .read()
            };
            if !header.counters.is_null() {
                let counters = unsafe { Arc::from_raw(header.counters) };
                counters.live.fetch_sub(layout.size(), Ordering::SeqCst);
            }
            unsafe { System.dealloc(pointer.sub(offset), allocation) };
        }
    }
    #[global_allocator]
    static ALLOCATOR: Allocator = Allocator;

    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(std::ptr::null()));
        }
    }
    pub(super) fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
        let counters = Arc::new(Counters::default());
        ACTIVE.with(|active| {
            assert!(active.get().is_null());
            active.set(Arc::as_ptr(&counters));
        });
        let reset = Reset;
        let value = operation();
        drop(reset);
        (value, counters.peak.load(Ordering::SeqCst))
    }

    pub(crate) fn measure_funding<T>(
        funding: Arc<AtomicUsize>,
        operation: impl FnOnce() -> T,
    ) -> (T, usize, usize, usize) {
        let counters = Arc::new(Counters {
            funding: Some(funding),
            ..Counters::default()
        });
        ACTIVE.with(|active| {
            assert!(active.get().is_null());
            active.set(Arc::as_ptr(&counters));
        });
        let reset = Reset;
        let value = operation();
        drop(reset);
        (
            value,
            counters.peak.load(Ordering::SeqCst),
            counters.unfunded.load(Ordering::SeqCst),
            counters.allocations.load(Ordering::SeqCst),
        )
    }
}

#[test]
fn closing_a_cursor_keeps_the_next_small_append_cleanup_bounded() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    struct PageCharge {
        current: Arc<AtomicU64>,
        bytes: u64,
    }
    impl Drop for PageCharge {
        fn drop(&mut self) {
            self.current.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }
    let mut measurements = Vec::new();
    for churn_commits in [32, 256] {
        let dir = tempfile::tempdir().expect("fixture directory");
        let path = dir.path().join("released-cursor-cleanup.db");
        seed_rows(&path, 120);
        let db = Database::open(&path).expect("open writer");
        db.execute(
            "CREATE TABLE unrelated_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create unrelated churn table");
        db.execute(
            "INSERT INTO unrelated_rows (id, payload) VALUES (0, 'seed')",
            &HashMap::new(),
        )
        .expect("seed unrelated churn");
        let session = db
            .read_session(ReadLimits::default())
            .expect("open live read session");
        let cursor = session
            .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
            .expect("hold selected snapshot");
        assert!(cursor.first_page().has_more);
        for generation in 0..churn_commits {
            db.execute(
                "UPDATE unrelated_rows SET payload = $payload WHERE id = 0",
                &HashMap::from([(
                    "payload".to_owned(),
                    Value::Text(format!("{generation}:{}", "x".repeat(8192))),
                )]),
            )
            .expect("replace unrelated payload under held snapshot");
        }
        let held_backlog = db
            .__deferred_free_backlog_for_test()
            .expect("observe both deferred-free trees while cursor is held");
        assert!(
            held_backlog.iter().map(|(_, pages)| pages).sum::<usize>() > 64,
            "churn must leave a material aggregate deferred-free backlog: {held_backlog:?}"
        );
        drop(cursor);
        let current = Arc::new(AtomicU64::new(0));
        let peak = Arc::new(AtomicU64::new(0));
        let total = Arc::new(AtomicU64::new(0));
        let page_current = Arc::clone(&current);
        let page_peak = Arc::clone(&peak);
        let page_total = Arc::clone(&total);
        let admit = Arc::new(
            move |bytes: usize| -> std::io::Result<redb::ReadPageCharge> {
                let bytes = bytes as u64;
                let live = page_current.fetch_add(bytes, Ordering::SeqCst) + bytes;
                page_peak.fetch_max(live, Ordering::SeqCst);
                page_total.fetch_add(bytes, Ordering::SeqCst);
                Ok(Box::new(PageCharge {
                    current: Arc::clone(&page_current),
                    bytes,
                }))
            },
        );
        redb::commit_work::reset_commit_pages_for_test();
        redb::commit_work::reset_page_tags_and_spans_for_test();
        let (result, heap_peak) = append_heap_oracle::measure(|| {
            redb::with_read_page_charge(admit, || {
                db.execute(
                    "INSERT INTO handover_rows (id) VALUES (1000)",
                    &HashMap::new(),
                )
            })
        });
        result.expect("small append after cursor release");
        let bookkeeping_copies = redb::commit_work::take_freed_pages_by_tag()
            .into_iter()
            .filter(|(tag, _)| *tag == redb::commit_work::TAG_COMMIT_BOOKKEEPING)
            .map(|(_, copies)| copies)
            .sum::<u64>();
        let work = redb::commit_work::take_commit_pages_for_test();
        assert!(work.commits > 0);
        assert!(
            work.reclaimed_pages > 0,
            "release must enable actual cleanup"
        );
        assert!(work.reclaimed_pages <= 64 * work.commits, "{work:?}");
        assert!(work.reclaimed_entries <= 64 * work.commits, "{work:?}");
        measurements.push((
            churn_commits,
            work,
            peak.load(Ordering::SeqCst),
            total.load(Ordering::SeqCst),
            heap_peak,
            bookkeeping_copies,
        ));
        let after_append = db
            .__deferred_free_backlog_for_test()
            .expect("observe durable remainder after bounded append");
        assert!(
            after_append.iter().map(|(_, pages)| pages).sum::<usize>() > 64,
            "the measured append must leave a backlog for restart: {after_append:?}"
        );
        // Create another protected backlog after the measured small append.
        // A snapshot held over close leaves this work for the next writer,
        // preserving the restart-continuation proof.
        let retained_cursor = session
            .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
            .expect("retain a snapshot across writer close");
        assert!(retained_cursor.first_page().has_more);
        for generation in 0..96 {
            db.execute(
                "UPDATE unrelated_rows SET payload = $payload WHERE id = 0",
                &HashMap::from([(
                    "payload".to_owned(),
                    Value::Text(format!("retained-{generation}:{}", "x".repeat(8192))),
                )]),
            )
            .expect("create deferred frees under retained snapshot");
        }
        assert!(
            db.__deferred_free_backlog_for_test()
                .unwrap()
                .iter()
                .map(|(_, pages)| pages)
                .sum::<usize>()
                > 64
        );
        db.close().expect("close writer");
        drop(retained_cursor);
        drop(session);
        assert_eq!(
            current.load(Ordering::SeqCst),
            0,
            "all observed source-page charges release"
        );
        let reopened = Database::open(&path).expect("reopen partially reclaimed engine store");
        let after_reopen = reopened
            .__deferred_free_backlog_for_test()
            .expect("observe persisted cleanup continuation after reopen");
        assert!(
            after_reopen.iter().map(|(_, pages)| pages).sum::<usize>() > 0,
            "bounded close/open housekeeping must preserve later work: {after_reopen:?}"
        );
        // Each empty durable storage transaction uses the engine-configured
        // ordinary commit path and budget. Stop adding SQL mutations here so
        // the assertion covers both entire trees, including cleanup's own new
        // bookkeeping entries, rather than only the original churn's keys.
        let drive_limit = after_reopen
            .iter()
            .map(|(entries, pages)| entries + pages)
            .sum::<usize>()
            * 2
            + 16;
        let mut drained = false;
        for _ in 0..drive_limit {
            let remaining = reopened
                .__deferred_free_backlog_for_test()
                .expect("read pending entries and pages without draining");
            if remaining == [(0, 0); 2] {
                drained = true;
                break;
            }
            redb::commit_work::reset_commit_pages_for_test();
            reopened
                .__commit_storage_transaction_for_test()
                .expect("drive one later small durable commit");
            let work = redb::commit_work::take_commit_pages_for_test();
            assert_eq!(work.commits, 1, "one deterministic cleanup drive: {work:?}");
            assert!(work.reclaimed_pages <= 64, "{work:?}");
            assert!(work.reclaimed_entries <= 64, "{work:?}");
        }
        assert!(
            drained,
            "both entire durable free trees must become empty: {:?}",
            reopened.__deferred_free_backlog_for_test().unwrap()
        );
        assert_eq!(
            reopened.__deferred_free_backlog_for_test().unwrap(),
            [(0, 0); 2],
            "observation must not create or reclaim any pending work"
        );
        let rows = reopened
            .execute(
                "SELECT id FROM handover_rows WHERE id = 1000",
                &HashMap::new(),
            )
            .expect("first small append survives reopen and complete cleanup");
        assert_eq!(rows.rows, vec![vec![Value::Int64(1000)]]);
        reopened.close().expect("close fully drained engine store");
    }
    eprintln!(
        "released cursor cleanup (churn commits, work, peak source charge, total source charge, peak new live heap bytes, bookkeeping COW copies): {measurements:?}"
    );
    let baseline = &measurements[0];
    for (_, _, peak, total, heap_peak, bookkeeping_copies) in &measurements {
        assert!(
            *bookkeeping_copies <= baseline.5 + 16,
            "post-cleanup bookkeeping copies stay bounded"
        );
        assert!(
            *peak <= baseline.2 + 64 * 1024,
            "uncached source-page charge peak stays bounded (cache and COW buffers excluded)"
        );
        assert!(
            *total <= baseline.3 + 256 * 1024,
            "cleanup page admission work remains bounded"
        );
        assert!(
            *heap_peak <= baseline.4 + 64 * 1024,
            "actual transient append allocations stay bounded"
        );
    }
}

#[test]
fn explicit_close_drains_released_snapshot_frees_before_handoff() {
    use contextdb_engine::{OwnerReadTestHooks, QuiescentStopPhase};

    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("close-drain.db");
    seed_rows(&path, 120);
    let phases = Arc::new(Mutex::new(Vec::new()));
    let observed_phases = Arc::clone(&phases);
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                test_hooks: Some(OwnerReadTestHooks {
                    clock: Arc::new(ManualDeadlineClock::default()),
                    drain_started: mpsc::channel().0,
                    active_request_registered: None,
                    accept_thread_exited: None,
                    accept_thread_joined: None,
                    service_dropped: None,
                    quiescent_stop: Some(Arc::new(move |phase| {
                        observed_phases
                            .lock()
                            .unwrap()
                            .push((phase, redb::commit_work::take_commit_pages_for_test()));
                    })),
                }),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open writer");
    db.execute(
        "CREATE TABLE churn (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create churn table");
    db.execute("INSERT INTO churn VALUES (0, 'seed')", &HashMap::new())
        .expect("seed churn row");
    let session = db
        .read_session(ReadLimits::default())
        .expect("read session");
    let cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("pin old snapshot");
    assert!(cursor.first_page().has_more);
    for generation in 0..96 {
        db.execute(
            "UPDATE churn SET payload = $payload WHERE id = 0",
            &HashMap::from([(
                "payload".to_owned(),
                Value::Text(format!("{generation}:{}", "x".repeat(8192))),
            )]),
        )
        .expect("churn under held snapshot");
    }
    drop(cursor);
    drop(session);
    let pending = db
        .__deferred_free_backlog_for_test()
        .expect("pending frees");
    assert!(
        pending.iter().map(|(_, pages)| pages).sum::<usize>() > 64,
        "fixture must need multiple bounded commits: {pending:?}"
    );

    redb::commit_work::reset_commit_pages_for_test();
    db.close().expect("close drains eligible frees");
    let observed = phases.lock().unwrap();
    let work = observed
        .iter()
        .find(|(phase, _)| *phase == QuiescentStopPhase::Published)
        .expect("close publishes handoff")
        .1;
    let after_handoff = observed
        .iter()
        .find(|(phase, _)| *phase == QuiescentStopPhase::AcceptedDrained)
        .expect("close finishes accepted reads")
        .1;
    assert_eq!(
        after_handoff.commits, 0,
        "published handoff must perform no further storage commits"
    );
    assert_eq!(
        redb::commit_work::take_commit_pages_for_test().commits,
        0,
        "resource release after handoff must not commit"
    );
    assert!(
        work.commits > 1,
        "close must perform bounded cleanup commits: {work:?}"
    );
    // This legacy observation field counts allocated extents, not physical
    // pages. One extent may contain several pages.
    let reclaimed_extents = work.reclaimed_pages;
    assert!(
        reclaimed_extents > 0,
        "close must reclaim eligible extents: {work:?}"
    );
    assert!(reclaimed_extents <= 64 * work.commits, "{work:?}");
    assert!(work.reclaimed_entries <= 64 * work.commits, "{work:?}");

    let reopened = Database::open(&path).expect("reopen drained store");
    let after_reopen = reopened
        .__deferred_free_backlog_for_test()
        .expect("inspect durable free lists after close");
    assert_eq!(
        after_reopen,
        [(0, 0), (1, 1)],
        "the earlier backlog is drained; the final handoff housekeeping commit leaves one system entry"
    );
    reopened.close().expect("close reopened store");
}

#[test]
fn explicit_close_reports_drain_io_failure_and_releases_the_store() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("close-drain-failure.db");
    seed_rows(&path, 120);
    let db = Database::open(&path).expect("open writer");
    db.execute(
        "CREATE TABLE churn (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create churn table");
    db.execute("INSERT INTO churn VALUES (0, 'seed')", &HashMap::new())
        .expect("seed churn row");
    let session = db
        .read_session(ReadLimits::default())
        .expect("read session");
    let cursor = session
        .open_cursor("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("hold old snapshot");
    assert!(cursor.first_page().has_more);
    for generation in 0..70 {
        db.execute(
            "UPDATE churn SET payload = $payload WHERE id = 0",
            &HashMap::from([(
                "payload".to_owned(),
                Value::Text(format!("{generation}:{}", "x".repeat(8192))),
            )]),
        )
        .expect("create cleanup backlog");
    }
    drop(cursor);
    drop(session);
    assert!(
        db.__deferred_free_backlog_for_test()
            .unwrap()
            .iter()
            .map(|(_, pages)| pages)
            .sum::<usize>()
            > 64
    );
    db.__fail_next_close_drain_for_test()
        .expect("arm one cleanup commit I/O fault");
    let error = db
        .close()
        .expect_err("storage drain failure must reach caller");
    assert!(
        error
            .to_string()
            .contains("injected deferred-free commit I/O failure"),
        "preserve cleanup cause: {error}"
    );
    let reopened = Database::open(&path).expect("failed close must release writer resources");
    assert!(
        reopened
            .__deferred_free_backlog_for_test()
            .unwrap()
            .iter()
            .map(|(_, pages)| pages)
            .sum::<usize>()
            > 0,
        "the aborted cleanup commit leaves durable pending work"
    );
    let rows = reopened
        .execute("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
        .expect("committed data remains readable");
    assert_eq!(rows.rows.len(), 120);
    reopened.close().expect("close reopened store");
}

#[test]
fn handoff_preparation_failure_still_releases_explicit_close_resources() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("close-handoff-failure.db");
    seed_rows(&path, 2);
    let db = Database::open(&path).expect("open writer");
    db.__fail_next_close_handoff_for_test()
        .expect("arm handoff preparation fault");
    let error = db.close().expect_err("handoff failure reaches caller");
    assert!(
        error
            .to_string()
            .contains("injected handoff preparation I/O failure"),
        "preserve handoff cause: {error}"
    );
    let reopened = Database::open(&path).expect("failed handoff releases owner resources");
    assert_eq!(
        reopened
            .execute("SELECT id FROM handover_rows ORDER BY id", &HashMap::new())
            .expect("committed data remains readable")
            .rows
            .len(),
        2
    );
    reopened.close().expect("close reopened store");
}
