#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadRoute,
};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::owner_read::{OwnerServicePublicationObserver, OwnerServiceSpec};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerReadTestHooks, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

const CHILD_PATH: &str = "CONTEXTDB_PURGE_PUBLICATION_CHILD_PATH";
const CHILD_READY: &str = "CONTEXTDB_PURGE_PUBLICATION_CHILD_READY";
const CHILD_GO: &str = "CONTEXTDB_PURGE_PUBLICATION_CHILD_GO";
const CHILD_OUTCOME: &str = "CONTEXTDB_PURGE_PUBLICATION_CHILD_OUTCOME";
const CRASH_PATH: &str = "CONTEXTDB_PURGE_PUBLICATION_CRASH_PATH";
const PAUSE_ARMED: &str = "CONTEXTDB_PURGE_PUBLICATION_PAUSE_ARMED";
const CONTROL_SOCKET: &str = "CONTEXTDB_PURGE_PUBLICATION_CONTROL_SOCKET";
const MIXED_ROOT_CHILD: &str = "CONTEXTDB_PURGE_MIXED_ROOT_CHILD";
#[cfg(target_os = "linux")]
const NO_PLATFORM_STAGE: &str = "CONTEXTDB_PURGE_NO_PLATFORM_STAGE";

#[cfg(target_os = "linux")]
#[test]
fn mixed_known_and_unknown_presence_refuses_purge_but_allows_known_reader_work() {
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;

    if run_with_platform_runtime(
        "mixed_known_and_unknown_presence_refuses_purge_but_allows_known_reader_work",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed-presence.db");
    seed(&path);
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::File);
    let identity = reader
        .file_storage_registration_for_test()
        .unwrap()
        .handle_identity;
    let unknown_file = OpenOptions::new().read(true).open(&path).unwrap();
    const WIDTH: i64 = 1 << 60;
    for (range, half) in [
        identity[..8].try_into().unwrap(),
        identity[8..].try_into().unwrap(),
    ]
    .into_iter()
    .enumerate()
    {
        let known_offset = (u64::from_le_bytes(half) as i64) & (WIDTH - 1);
        let unknown_offset = if known_offset == 0 {
            1
        } else {
            known_offset - 1
        };
        let mut lock = libc::flock {
            l_type: libc::F_RDLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: ((range as i64) + 1) * WIDTH + unknown_offset,
            l_len: 1,
            l_pid: 0,
        };
        // SAFETY: the lock is initialized and unknown_file remains open.
        assert_ne!(
            unsafe { libc::fcntl(unknown_file.as_raw_fd(), libc::F_OFD_SETLK, &mut lock) },
            -1
        );
    }
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'new')",
            &HashMap::new(),
        )
        .expect("ordinary work with the known reader remains possible");
    let refusal = writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap_err();
    assert!(
        matches!(
            refusal,
            contextdb_core::Error::ReaderCoordinationUnavailable { .. }
        ),
        "{refusal:?}"
    );
    drop(unknown_file);
    writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .expect("the authenticated known reader can receive erasure publication");
    writer.close().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn different_roots_without_platform_root_refuse_unsafe_reclamation_and_purge() {
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::process::{Command, Stdio};

    let stage = std::env::var(NO_PLATFORM_STAGE).ok();
    let test_name = crate::child_test_filter(
        module_path!(),
        "different_roots_without_platform_root_refuse_unsafe_reclamation_and_purge",
    );
    if stage.as_deref() == Some("reader") {
        let path = std::path::PathBuf::from(std::env::var("CONTEXTDB_CROSS_STORE").unwrap());
        let root = std::path::PathBuf::from(std::env::var("CONTEXTDB_CROSS_A").unwrap());
        let session =
            ReadSession::open_in_runtime_dir(&path, ReadSessionOptions::default(), Some(root))
                .unwrap();
        assert_eq!(session.route(), ReadRoute::File);
        println!("READER_READY");
        std::io::stdout().flush().unwrap();
        let mut signal = [0_u8; 1];
        std::io::stdin().read_exact(&mut signal).unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT payload FROM erased_rows WHERE id = 1",
                    &HashMap::new()
                )
                .unwrap()
                .rows,
            vec![vec![Value::Text("secret".to_owned())]],
        );
        return;
    }
    if stage.as_deref() == Some("writer") {
        let path = std::path::PathBuf::from(std::env::var("CONTEXTDB_CROSS_STORE").unwrap());
        let root_a = std::path::PathBuf::from(std::env::var("CONTEXTDB_CROSS_A").unwrap());
        let root_b = std::path::PathBuf::from(std::env::var("CONTEXTDB_CROSS_B").unwrap());
        let mut reader = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--nocapture"])
            .env(NO_PLATFORM_STAGE, "reader")
            .env("CONTEXTDB_CROSS_STORE", &path)
            .env("CONTEXTDB_CROSS_A", &root_a)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(reader.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                output.read_line(&mut line).unwrap() > 0,
                "reader exited before readiness"
            );
            if line.contains("READER_READY") {
                break;
            }
        }
        let writer = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    runtime_dir: Some(root_b),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("writer open remains available with an unknown reader root");
        writer
            .execute(
                "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'new')",
                &HashMap::new(),
            )
            .expect("ordinary commit remains available under a conservative floor");
        let compact = writer.compact_now().unwrap_err();
        assert!(
            matches!(
                &compact,
                contextdb_core::Error::ReaderCoordinationUnavailable { .. }
            ),
            "{compact:?}"
        );
        assert!(compact.to_string().contains("presence"), "{compact:?}");
        let purge = writer
            .execute(
                "PURGE FROM erased_rows WHERE payload = 'secret'",
                &HashMap::new(),
            )
            .unwrap_err();
        assert!(
            matches!(
                &purge,
                contextdb_core::Error::ReaderCoordinationUnavailable { .. }
            ),
            "{purge:?}"
        );
        assert!(purge.to_string().contains("presence"), "{purge:?}");
        reader.stdin.take().unwrap().write_all(&[1]).unwrap();
        assert!(
            reader.wait().unwrap().success(),
            "old reader snapshot stayed readable"
        );
        writer
            .execute(
                "PURGE FROM erased_rows WHERE payload = 'secret'",
                &HashMap::new(),
            )
            .expect("purge resumes after the unknown reader releases its hold");
        writer.close().unwrap();
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cross-root.db");
    let root_a = directory.path().join("runtime-a");
    let root_b = directory.path().join("runtime-b");
    for root in [&root_a, &root_b] {
        std::fs::create_dir(root).unwrap();
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    seed(&path);
    let unavailable = directory.path().join("no-platform-runtime");
    assert!(!unavailable.exists());
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test_name, "--nocapture"])
        .env(NO_PLATFORM_STAGE, "writer")
        .env("CONTEXTDB_CROSS_STORE", &path)
        .env("CONTEXTDB_CROSS_A", &root_a)
        .env("CONTEXTDB_CROSS_B", &root_b)
        .env("XDG_RUNTIME_DIR", unavailable)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_with_platform_runtime(test: &str) -> bool {
    if std::env::var_os(MIXED_ROOT_CHILD).is_some() {
        return false;
    }
    let directory = tempfile::tempdir().unwrap();
    let platform = directory.path().join("platform-runtime");
    std::fs::create_dir(&platform).unwrap();
    std::fs::set_permissions(&platform, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(module_path!(), test))
        .arg("--nocapture")
        .env("XDG_RUNTIME_DIR", &platform)
        .env(MIXED_ROOT_CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "mixed-root child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).expect("create purge publication fixture");
    db.execute(
        "CREATE TABLE erased_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE unrelated_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL",
        &HashMap::new(),
    )
    .unwrap();
    for id in 1..=3_i64 {
        db.execute(
            "INSERT INTO erased_rows (id, payload) VALUES ($id, 'secret')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO unrelated_rows (id, payload) VALUES (1, 'kept')",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
}

fn assert_erasure(error: contextdb_core::Error) {
    assert!(matches!(
        error,
        contextdb_core::Error::ReadFailure(failure)
            if failure.kind() == ReadFailureKind::AuthoritativeErasure
    ));
}

#[test]
fn supplied_reader_and_default_writer_share_erasure_publication() {
    if run_with_platform_runtime("supplied_reader_and_default_writer_share_erasure_publication") {
        return;
    }
    mixed_root_purge(true);
}

#[test]
fn default_reader_and_supplied_writer_share_erasure_publication() {
    if run_with_platform_runtime("default_reader_and_supplied_writer_share_erasure_publication") {
        return;
    }
    mixed_root_purge(false);
}

fn mixed_root_purge(reader_supplied: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge-reader.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);

    {
        let mut options = ReadSessionOptions::default();
        options.limits.cursor_page_rows = 1;
        let session = ReadSession::open_in_runtime_dir(
            &path,
            options,
            reader_supplied.then(|| runtime.clone()),
        )
        .unwrap();
        assert_eq!(session.route(), ReadRoute::File);
        assert_eq!(
            session
                .execute(
                    "SELECT payload FROM erased_rows WHERE id = 1",
                    &HashMap::new()
                )
                .unwrap()
                .rows,
            vec![vec![Value::Text("secret".to_owned())]]
        );
        let mut cursor = session
            .open_cursor(
                "SELECT id, payload FROM erased_rows ORDER BY id",
                &HashMap::new(),
            )
            .unwrap();
        assert!(cursor.first_page().has_more);

        let writer = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    runtime_dir: (!reader_supplied).then(|| runtime.clone()),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("writer reopens after the direct reader released its initial load lock");
        writer
            .execute(
                "PURGE FROM erased_rows WHERE payload = 'secret'",
                &HashMap::new(),
            )
            .expect("durable authoritative purge");

        assert_erasure(
            session
                .execute("SELECT payload FROM erased_rows", &HashMap::new())
                .unwrap_err(),
        );
        assert_erasure(cursor.fetch(NonZeroUsize::new(1)).unwrap_err());
        assert_eq!(
            session
                .execute("SELECT payload FROM unrelated_rows", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Text("kept".to_owned())]]
        );
        writer.close().unwrap();
        let fresh = ReadSession::open(&path).unwrap();
        assert!(
            fresh
                .execute("SELECT id FROM erased_rows", &HashMap::new())
                .unwrap()
                .rows
                .is_empty()
        );
    }
}

#[test]
fn an_unreadable_configured_root_refuses_writer_open_with_its_path() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unreadable-open.db");
    let runtime = directory.path().join("candidate-runtime");
    std::fs::create_dir(&runtime).unwrap();
    seed(&path);
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    );
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    match result {
        Err(contextdb_core::Error::ReaderCoordinationUnavailable {
            operation, path, ..
        }) => {
            assert_eq!(operation, "storage reclamation");
            assert_eq!(path, runtime.display().to_string());
        }
        Err(other) => panic!("writer open needs a typed reader-coordination refusal: {other:?}"),
        Ok(writer) => {
            writer.close().unwrap();
            panic!("writer open cannot reclaim without authenticating a configured root");
        }
    }
}

#[test]
fn an_unreadable_candidate_refuses_purge_without_blocking_database_open() {
    struct RestoreRuntime(std::path::PathBuf);
    impl Drop for RestoreRuntime {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unreadable-candidate.db");
    let runtime = directory.path().join("candidate-runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);
    let writer = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("the writer opens before the candidate root becomes unreadable");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o000)).unwrap();
    let _restore = RestoreRuntime(runtime.clone());
    let refusal = writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap_err();
    assert!(
        matches!(refusal, contextdb_core::Error::ReaderCoordinationUnavailable { ref operation, ref path, .. }
            if operation == "authoritative purge" && path == &runtime.display().to_string()),
        "purge must name its candidate root in a typed refusal: {refusal:?}"
    );
    assert_eq!(
        writer
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .len(),
        3,
    );
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .expect("the same writer can purge after coordination is restored");
    writer.close().unwrap();
}

#[test]
fn an_unreadable_candidate_refuses_writer_reclamation_until_it_is_restored() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reclamation-candidate.db");
    let runtime = directory.path().join("candidate-runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);
    let writer = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = writer.execute(
        "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'new')",
        &HashMap::new(),
    );
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let refusal = result.unwrap_err();
    assert!(
        matches!(refusal, contextdb_core::Error::ReaderCoordinationUnavailable { ref operation, ref path, .. }
            if operation == "storage reclamation" && path == &runtime.display().to_string()),
        "writer reclamation must name its candidate root in a typed refusal: {refusal:?}"
    );
    writer
        .execute(
            "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'new')",
            &HashMap::new(),
        )
        .expect("the same writer progresses after coordination is restored");
    writer.close().unwrap();
}

#[derive(Default)]
struct PublicationPause {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl PublicationPause {
    fn wait_until_paused(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }
}

impl OwnerServicePublicationObserver for PublicationPause {
    fn before_cursor_page_publication(&self, _request_ordinal: u64) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn cancellation_received(&self, _request_ordinal: u64) {}

    fn cursor_page_published(&self, _request_ordinal: u64) {}
}

#[derive(Default)]
struct AuthorizedPublicationPause {
    armed: AtomicBool,
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl AuthorizedPublicationPause {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn wait_until_paused(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }
}

impl OwnerServicePublicationObserver for AuthorizedPublicationPause {
    fn before_cursor_page_publication(&self, _request_ordinal: u64) {}

    fn publication_authorized(&self, _request_ordinal: u64) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn cancellation_received(&self, _request_ordinal: u64) {}

    fn cursor_page_published(&self, _request_ordinal: u64) {}
}

struct ExpireBeforePublication {
    clock: Arc<ManualDeadlineClock>,
}

impl OwnerServicePublicationObserver for ExpireBeforePublication {
    fn before_cursor_page_publication(&self, _request_ordinal: u64) {
        self.clock.advance_to(self.clock.now_ms() + 21);
    }

    fn cancellation_received(&self, _request_ordinal: u64) {}

    fn cursor_page_published(&self, _request_ordinal: u64) {}
}

#[test]
fn owner_publication_honors_active_deadline_before_any_purge_durability() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner-publication-deadline.db");
    let runtime = directory.path().join("owner-runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);

    let clock = Arc::new(ManualDeadlineClock::default());
    let observer = Arc::new(ExpireBeforePublication {
        clock: Arc::clone(&clock),
    });
    let (drain_started, _drain) = std::sync::mpsc::channel();
    let database =
        OwnerServiceSpec::with_publication_observer_during_start_for_test(observer, || {
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    owner_reads: OwnerReadConfig {
                        runtime_dir: Some(runtime.clone()),
                        test_hooks: Some(OwnerReadTestHooks {
                            clock: clock.clone(),
                            drain_started,
                            active_request_registered: None,
                            accept_thread_exited: None,
                            accept_thread_joined: None,
                            service_dropped: None,
                            quiescent_stop: None,
                        }),
                        ..OwnerReadConfig::default()
                    },
                    ..DatabaseOpenOptions::default()
                },
            )
        })
        .unwrap();
    let mut options = ReadSessionOptions::default();
    options.limits.active_ms = 20;
    options.limits.cursor_page_rows = 1;
    let session = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(&path, options)
    })
    .unwrap();
    let mut cursor = session
        .open_cursor(
            "SELECT id, payload FROM erased_rows ORDER BY id",
            &HashMap::new(),
        )
        .unwrap();
    let error = cursor.fetch(NonZeroUsize::new(1)).unwrap_err();
    let contextdb_core::Error::ReadFailure(failure) = error else {
        panic!("publication deadline must remain a typed read refusal: {error:?}")
    };
    assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("publication deadline must name its active-time limit")
    };
    assert_eq!(detail.limit, ReadFailureLimit::ActiveMs);
    assert_eq!(detail.value, 20);
    assert_eq!(
        session
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .len(),
        3,
        "deadline refusal precedes durable erasure and changes no row"
    );
    database
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap();
    assert!(
        session
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
    database.close().unwrap();
}

#[test]
fn held_owner_publication_does_not_hold_the_commit_mutex_and_purge_waits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner-publication-progress.db");
    let runtime = directory.path().join("owner-runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);

    let pause = Arc::new(AuthorizedPublicationPause::default());
    let database = Arc::new(
        OwnerServiceSpec::with_publication_observer_during_start_for_test(pause.clone(), || {
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    owner_reads: OwnerReadConfig {
                        runtime_dir: Some(runtime.clone()),
                        ..OwnerReadConfig::default()
                    },
                    ..DatabaseOpenOptions::default()
                },
            )
        })
        .unwrap(),
    );
    let mut options = ReadSessionOptions::default();
    options.limits.cursor_page_rows = 1;
    let session = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(&path, options)
    })
    .unwrap();
    let cursor = session
        .open_cursor(
            "SELECT id, payload FROM erased_rows ORDER BY id",
            &HashMap::new(),
        )
        .unwrap();
    pause.arm();
    let fetch = std::thread::spawn(move || {
        let mut cursor = cursor;
        cursor.fetch(NonZeroUsize::new(1))
    });
    pause.wait_until_paused();

    database
        .execute(
            "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'also kept')",
            &HashMap::new(),
        )
        .expect("ordinary commit progresses during held owner publication");
    let (purge_done, purge_result) = std::sync::mpsc::channel();
    let purge_database = Arc::clone(&database);
    std::thread::spawn(move || {
        purge_done
            .send(purge_database.execute(
                "PURGE FROM erased_rows WHERE payload = 'secret'",
                &HashMap::new(),
            ))
            .unwrap();
    });
    assert!(matches!(
        purge_result.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    pause.release();
    fetch.join().unwrap().unwrap();
    purge_result
        .recv_timeout(Duration::from_secs(10))
        .expect("purge completes after publication")
        .unwrap();
    assert!(
        session
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
    database.close().unwrap();
}

#[test]
fn owner_cursor_paused_before_carrier_publication_cannot_emit_after_purge() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner-purge-reader.db");
    let runtime = directory.path().join("owner-runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);

    let pause = Arc::new(PublicationPause::default());
    let mut open = DatabaseOpenOptions::default();
    open.owner_reads = OwnerReadConfig {
        runtime_dir: Some(runtime.clone()),
        ..OwnerReadConfig::default()
    };
    let database =
        OwnerServiceSpec::with_publication_observer_during_start_for_test(pause.clone(), || {
            Database::open_with_options(&path, open)
        })
        .unwrap();
    let mut session_options = ReadSessionOptions::default();
    session_options.limits.cursor_page_rows = 1;
    let clock = Arc::new(ManualDeadlineClock::at(1000));
    let session = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_clock_for_test(&path, session_options, clock.clone())
    })
    .unwrap();
    assert_eq!(session.route(), ReadRoute::Owner);
    let cursor = session
        .open_cursor(
            "SELECT id, payload FROM erased_rows ORDER BY id",
            &HashMap::new(),
        )
        .unwrap();
    assert!(cursor.first_page().has_more);

    let fetch = std::thread::spawn(move || {
        let mut cursor = cursor;
        let result = cursor.fetch(NonZeroUsize::new(1));
        (cursor, result)
    });
    pause.wait_until_paused();
    database
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap();
    let held = database
        .__external_snapshot_retention_for_test()
        .expect("observe cursor hold before erasure refusal");
    assert!(held.snapshots > 0, "client still holds the published root");
    assert!(held.retained_pages > 0, "purge retains old cursor pages");
    assert_eq!(
        session.route_resources_for_test().unwrap().active_cursors,
        1
    );
    pause.release();
    let (cursor, result) = fetch.join().unwrap();
    assert_erasure(result.unwrap_err());
    assert!(!cursor.is_live(), "authoritative erasure ends the cursor");
    assert_eq!(clock.now_ms(), 1000, "no deadline advanced");
    let released = database
        .__external_snapshot_retention_for_test()
        .expect("observe release while failed cursor stays alive");
    assert_eq!(released.snapshots, 0, "client released its exact root");
    assert_eq!(
        released.retained_pages, 0,
        "no cursor pages remain retained"
    );
    assert_eq!(
        session.route_resources_for_test().unwrap().active_cursors,
        0
    );
    let _ = &cursor;

    assert!(
        session
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        session
            .execute("SELECT payload FROM unrelated_rows", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Text("kept".to_owned())]]
    );
    database.close().unwrap();
}

#[test]
fn cross_process_reader_child() {
    let Ok(path) = std::env::var(CHILD_PATH) else {
        return;
    };
    let outcome = std::path::PathBuf::from(std::env::var(CHILD_OUTCOME).unwrap());
    let session = ReadSession::open(path).expect("child opens direct physical reader");
    assert_eq!(session.route(), ReadRoute::File);
    assert_eq!(
        session
            .execute("SELECT payload FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .len(),
        3
    );
    // Every caller of this child sets CONTROL_SOCKET and drives it through
    // the same ready/go/answer handshake below; there is no remaining
    // caller that relies on the CHILD_READY/CHILD_GO marker-file handshake.
    let control_path = std::env::var_os(CONTROL_SOCKET).expect("caller sets CONTROL_SOCKET");
    let mut control = std::os::unix::net::UnixStream::connect(control_path).unwrap();
    control.write_all(b"r").unwrap();
    let mut go = [0_u8; 1];
    control.read_exact(&mut go).unwrap();
    let answer = match session.execute("SELECT payload FROM erased_rows", &HashMap::new()) {
        Err(contextdb_core::Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::AuthoritativeErasure =>
        {
            b"erasure".as_slice()
        }
        Ok(_) => b"stale".as_slice(),
        Err(_) => b"wrong-error".as_slice(),
    };
    std::fs::write(outcome, answer).unwrap();
    control.write_all(answer).unwrap();
}

#[test]
fn durable_purge_invalidates_a_reader_in_another_process() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cross-process.db");
    // The child still needs valid CHILD_READY/CHILD_GO/CHILD_OUTCOME paths
    // (it unconditionally reads and, for the outcome file, writes them), but
    // with CONTROL_SOCKET set it never touches CHILD_READY/CHILD_GO: the
    // ready/go handshake and the outcome answer travel over the socket
    // instead.
    let unused_ready = directory.path().join("unused-ready");
    let unused_go = directory.path().join("unused-go");
    let unused_outcome = directory.path().join("unused-outcome");
    let control_path = directory.path().join("control.sock");
    let listener = std::os::unix::net::UnixListener::bind(&control_path).unwrap();
    seed(&path);

    let filter = crate::child_test_filter(module_path!(), "cross_process_reader_child");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(CHILD_PATH, &path)
        .env(CHILD_READY, &unused_ready)
        .env(CHILD_GO, &unused_go)
        .env(CHILD_OUTCOME, &unused_outcome)
        .env(CONTROL_SOCKET, &control_path)
        .spawn()
        .expect("spawn separate direct reader process");
    let (mut control, _) = listener.accept().unwrap();
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    let writer = Database::open(&path).expect("writer opens beside separate reader process");
    writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap();
    control.write_all(b"g").unwrap();
    // The child writes its answer to the socket and then exits, closing its
    // end; a blocking read to EOF gets the whole answer without polling.
    let mut answer = Vec::new();
    control.read_to_end(&mut answer).unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "reader child failed: {status}");
    assert_eq!(answer, b"erasure");
    writer.close().unwrap();
}

#[test]
fn direct_result_publication_holds_only_the_final_fence_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-boundary.db");
    let unused_ready = directory.path().join("unused-reader-ready");
    let unused_go = directory.path().join("unused-reader-go");
    let unused_outcome = directory.path().join("unused-reader-outcome");
    let control_path = directory.path().join("control.sock");
    // Existence is the temporal gate the publication-pause seam checks: it
    // must not exist while the child's own startup read runs (that read must
    // pass through unpaused), only from here on, once the ready/go handshake
    // below has completed and the child is about to run the read this test
    // actually means to pause.
    let armed_marker = directory.path().join("publication-armed");
    let listener = std::os::unix::net::UnixListener::bind(&control_path).unwrap();
    seed(&path);

    let filter = crate::child_test_filter(module_path!(), "cross_process_reader_child");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(CHILD_PATH, &path)
        .env(CHILD_READY, &unused_ready)
        .env(CHILD_GO, &unused_go)
        .env(CHILD_OUTCOME, &unused_outcome)
        .env(CONTROL_SOCKET, &control_path)
        .env(PAUSE_ARMED, &armed_marker)
        .spawn()
        .expect("spawn paused direct reader process");
    // First connection: the reader's own ready/go handshake and, later, its
    // answer (read to EOF once the child exits).
    let (mut control, _) = listener.accept().unwrap();
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    let writer = Arc::new(Database::open(&path).expect("writer opens beside paused direct reader"));
    std::fs::write(&armed_marker, b"armed")
        .expect("arm the publication pause before the go signal");
    control.write_all(b"g").unwrap();

    // Second connection: opened independently, deep inside the read, by the
    // direct-result publication seam once it reaches the final fence
    // boundary it pauses at.
    let (mut pause_control, _) = listener.accept().unwrap();
    let mut paused = [0_u8; 1];
    pause_control.read_exact(&mut paused).unwrap();

    let (finished, completion) = std::sync::mpsc::channel();
    let purge_writer = Arc::clone(&writer);
    std::thread::spawn(move || {
        let result = purge_writer.execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        );
        finished.send(result).unwrap();
    });
    assert!(
        matches!(
            completion.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "purge cannot cross a direct result's held final publication boundary"
    );
    writer
        .execute(
            "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'also kept')",
            &HashMap::new(),
        )
        .expect("an ordinary writer progresses while purge waits outside the commit mutex");
    pause_control.write_all(b"g").unwrap();
    let mut answer = Vec::new();
    control.read_to_end(&mut answer).unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "paused reader child failed: {status}");
    assert_eq!(
        answer, b"stale",
        "the read linearized before the purge and may publish its completed result"
    );
    let purge = completion
        .recv_timeout(Duration::from_secs(10))
        .expect("purge progresses immediately after the final publication boundary");
    purge.unwrap();
    writer.close().unwrap();
}

#[test]
fn stopped_direct_publication_releases_on_process_death_without_holding_commit_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-process-death.db");
    let control_path = directory.path().join("control.sock");
    let stop_armed = directory.path().join("stop-armed");
    let outcome = directory.path().join("unused-outcome");
    let listener = std::os::unix::net::UnixListener::bind(&control_path).unwrap();
    seed(&path);

    let filter = crate::child_test_filter(module_path!(), "cross_process_reader_child");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(CHILD_PATH, &path)
        .env(CHILD_READY, directory.path().join("unused-ready"))
        .env(CHILD_GO, directory.path().join("unused-go"))
        .env(CHILD_OUTCOME, &outcome)
        .env(CONTROL_SOCKET, &control_path)
        .env("CONTEXTDB_PURGE_PUBLICATION_STOP_AT_FINAL", &stop_armed)
        .spawn()
        .expect("spawn stopped direct publication");
    let (mut control, _) = listener.accept().unwrap();
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    let writer = Arc::new(Database::open(&path).unwrap());
    std::fs::write(&stop_armed, b"armed").unwrap();
    control.write_all(b"g").unwrap();
    let mut status = 0_i32;
    let waited = unsafe { libc::waitpid(child.id() as libc::pid_t, &mut status, libc::WUNTRACED) };
    assert_eq!(waited, child.id() as libc::pid_t);
    assert!(libc::WIFSTOPPED(status));

    let purge_writer = Arc::clone(&writer);
    let (purge_done, purge_result) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        purge_done
            .send(purge_writer.execute(
                "PURGE FROM erased_rows WHERE payload = 'secret'",
                &HashMap::new(),
            ))
            .unwrap();
    });
    writer
        .execute(
            "INSERT INTO unrelated_rows (id, payload) VALUES (2, 'also kept')",
            &HashMap::new(),
        )
        .expect("ordinary write progresses while purge waits on stopped process");
    assert!(matches!(
        purge_result.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    child.kill().unwrap();
    let killed = child.wait().unwrap();
    assert!(!killed.success());
    purge_result
        .recv_timeout(Duration::from_secs(10))
        .expect("process death releases both publication locks")
        .unwrap();
    writer.close().unwrap();
    let fresh = ReadSession::open(&path).unwrap();
    assert!(
        fresh
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn crashing_purge_writer_child() {
    let Ok(path) = std::env::var(CRASH_PATH) else {
        return;
    };
    let writer = Database::open(path).unwrap();
    let _ = writer.execute(
        "PURGE FROM erased_rows WHERE payload = 'secret'",
        &HashMap::new(),
    );
    panic!("purge writer did not stop at its armed crash boundary");
}

fn crash_writer(path: &std::path::Path, boundary: &str) {
    let filter = crate::child_test_filter(module_path!(), "crashing_purge_writer_child");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(CRASH_PATH, path)
        .env("CONTEXTDB_PURGE_PUBLICATION_CRASH_AT", boundary)
        .status()
        .expect("run crashing purge writer");
    assert_eq!(status.code(), Some(86), "writer stopped outside crash seam");
}

#[test]
fn crash_before_durable_commit_recovers_as_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("precommit-crash.db");
    seed(&path);
    let session = ReadSession::open(&path).unwrap();
    crash_writer(&path, "before-durable-commit");
    assert_erasure(
        session
            .execute("SELECT payload FROM erased_rows", &HashMap::new())
            .unwrap_err(),
    );

    let recovered = Database::open(&path).expect("recover unresolved aborted publication");
    assert_eq!(
        session
            .execute("SELECT payload FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .len(),
        3
    );
    recovered.close().unwrap();
}

#[test]
fn crash_after_durable_commit_recovers_as_committed_erasure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("postcommit-crash.db");
    seed(&path);
    let session = ReadSession::open(&path).unwrap();
    crash_writer(&path, "after-durable-commit");
    assert_erasure(
        session
            .execute("SELECT payload FROM erased_rows", &HashMap::new())
            .unwrap_err(),
    );

    let recovered = Database::open(&path).expect("recover committed purge publication");
    assert_erasure(
        session
            .execute("SELECT payload FROM erased_rows", &HashMap::new())
            .unwrap_err(),
    );
    recovered.close().unwrap();
    let fresh = ReadSession::open(&path).unwrap();
    assert!(
        fresh
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn companion_inspection_failure_refuses_purge_with_database_path() {
    struct RestoreCompanion {
        parked: std::path::PathBuf,
        companion: std::path::PathBuf,
    }
    impl Drop for RestoreCompanion {
        fn drop(&mut self) {
            if self.parked.exists() {
                std::fs::rename(&self.parked, &self.companion)
                    .expect("restore the writer's companion pathname");
            }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("companion-inspection.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    seed(&path);
    let writer = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the writer before hiding its companion pathname");
    let companion = contextdb_core::store_companion_path(&path);
    let parked = directory.path().join("parked-companion");
    std::fs::rename(&companion, &parked).expect("make companion inspection fail");
    let restore = RestoreCompanion { parked, companion };

    let refusal = writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap_err();
    assert!(
        matches!(&refusal, contextdb_core::Error::ReaderCoordinationUnavailable { operation, path: error_path, reason }
            if operation == "authoritative purge"
                && error_path == &path.display().to_string()
                && reason.contains("open companion")),
        "companion inspection must be a typed refusal with its database path: {refusal:?}"
    );
    assert_eq!(
        writer
            .execute("SELECT id FROM erased_rows", &HashMap::new())
            .unwrap()
            .rows
            .len(),
        3,
        "a refused purge leaves its rows intact"
    );

    drop(restore);
    writer
        .execute(
            "PURGE FROM erased_rows WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .expect("the same writer can purge after companion inspection recovers");
    writer.close().unwrap();
}
