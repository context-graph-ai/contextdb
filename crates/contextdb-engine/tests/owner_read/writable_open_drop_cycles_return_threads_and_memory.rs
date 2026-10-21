#![cfg(all(target_os = "linux", feature = "nightly-tests"))]

//! A writable open must release the owner-read service on both close paths.
//!
//! The socket disappearing is not enough: the accept worker can retain the
//! service after its pathname has gone.  This Linux-only proof uses the
//! process's actual thread names and resident set, then waits for this
//! fixture's actual accept-thread and final-service events before measuring.

use contextdb_engine::local_transport::MonotonicDeadlineClock;
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerReadTestHooks, ReadRoute, ReadSession,
};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::Duration;

const CYCLES: usize = 40;
const MAX_RSS_GROWTH_KIB: u64 = 64 * 1024;
const EXIT_WAIT: Duration = Duration::from_secs(10);

fn secure_runtime_root(directory: &Path, arm: &str) -> PathBuf {
    let root = directory.join(format!("{arm}-owner-runtime"));
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime directory");
    root
}

fn owner_options(runtime_dir: PathBuf, test_hooks: OwnerReadTestHooks) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            runtime_dir: Some(runtime_dir),
            test_hooks: Some(test_hooks),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

fn task_file(path: &Path) -> Option<String> {
    task_file_result(std::fs::read_to_string(path), path)
}

fn task_file_result(result: std::io::Result<String>, path: &Path) -> Option<String> {
    match result {
        Ok(contents) => Some(contents),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            None
        }
        Err(error) => panic!("read Linux task file {}: {error}", path.display()),
    }
}

fn task_is_exiting(stat: &str) -> bool {
    // proc_pid_stat(5) field 9 is the kernel flags, after the parenthesized comm.
    // Linux v6.17 include/linux/sched.h defines PF_EXITING as 0x00000004.
    // A joined thread can remain visible during kernel exit; it is no longer live work.
    let (_, fields) = stat
        .rsplit_once(')')
        .expect("Linux task stat has a closing comm parenthesis");
    let flags = fields
        .split_whitespace()
        .nth(6)
        .expect("Linux task stat has field 9 flags")
        .parse::<u64>()
        .expect("Linux task stat flags are an unsigned decimal integer");
    flags & 0x00000004 != 0
}

fn owner_thread_count_in(tasks: &Path) -> usize {
    std::fs::read_dir(tasks)
        .expect("read this process's Linux thread directory")
        .map(|entry| entry.expect("read a Linux task directory entry"))
        .filter(|entry| {
            let path = entry.path();
            task_file(&path.join("comm"))
                .is_some_and(|comm| comm.trim().starts_with("contextdb-owner"))
                && task_file(&path.join("stat")).is_some_and(|stat| !task_is_exiting(&stat))
        })
        .count()
}

fn owner_thread_count() -> usize {
    owner_thread_count_in(Path::new("/proc/self/task"))
}

#[test]
fn census_counts_a_live_named_owner_thread() {
    let baseline = owner_thread_count();
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("contextdb-owner-census".to_owned())
        .spawn(move || {
            ready.send(()).expect("announce live owner thread");
            let _ = wait.recv();
        })
        .expect("spawn a task-scoped live owner thread");
    started
        .recv_timeout(EXIT_WAIT)
        .expect("owner thread is live");
    let live_count = owner_thread_count();
    drop(release);
    thread.join().expect("join the task-scoped owner thread");
    assert_eq!(live_count, baseline + 1);
}

#[test]
fn census_distinguishes_kernel_exit_and_rejects_malformed_flags() {
    let tasks = tempfile::tempdir().expect("task-scoped synthetic proc directory");
    let task = tasks.path().join("1");
    std::fs::create_dir(&task).unwrap();
    std::fs::write(task.join("comm"), "contextdb-owner\n").unwrap();
    for (state, flags, expected) in [("R", "4194380", 0), ("R", "4194376", 1), ("Z", "0", 1)] {
        std::fs::write(
            task.join("stat"),
            format!("1 (contextdb-owner (with spaces)) {state} 2 3 4 0 -1 {flags} 0\n"),
        )
        .unwrap();
        assert_eq!(owner_thread_count_in(tasks.path()), expected);
    }
    for malformed in [
        "1 (contextdb-owner) R 2 3 4 0 -1 invalid 0",
        "1 (contextdb-owner) R 2 3 4 0 -1 -4 0",
        "1 (contextdb-owner) R 2 3 4 0 -1 18446744073709551616 0",
        "1 (contextdb-owner) R 2 3",
        "1 contextdb-owner R 2 3 4 0 -1 4",
    ] {
        std::fs::write(task.join("stat"), malformed).unwrap();
        assert!(std::panic::catch_unwind(|| owner_thread_count_in(tasks.path())).is_err());
    }
}

#[test]
fn census_allows_disappeared_tasks_but_rejects_other_io_errors() {
    let tasks = tempfile::tempdir().expect("task-scoped synthetic proc directory");
    let task = tasks.path().join("1");
    std::fs::create_dir(&task).unwrap();
    assert_eq!(owner_thread_count_in(tasks.path()), 0);
    std::fs::write(task.join("comm"), "contextdb-owner\n").unwrap();
    assert_eq!(owner_thread_count_in(tasks.path()), 0);
    std::fs::create_dir(task.join("stat")).unwrap();
    assert!(std::panic::catch_unwind(|| owner_thread_count_in(tasks.path())).is_err());
    std::fs::remove_file(task.join("comm")).unwrap();
    std::fs::create_dir(task.join("comm")).unwrap();
    assert!(std::panic::catch_unwind(|| owner_thread_count_in(tasks.path())).is_err());
}

#[test]
fn census_accepts_enoent_and_esrch_but_panics_on_other_read_errors() {
    let path = Path::new("/proc/self/task/vanished/comm");
    assert_eq!(
        task_file_result(Ok("contextdb-owner\n".to_owned()), path),
        Some("contextdb-owner\n".to_owned())
    );
    for code in [libc::ENOENT, libc::ESRCH] {
        assert_eq!(
            task_file_result(Err(std::io::Error::from_raw_os_error(code)), path),
            None
        );
    }
    for code in [libc::EISDIR, libc::EACCES, libc::EIO, libc::EINVAL] {
        let panic = std::panic::catch_unwind(|| {
            task_file_result(Err(std::io::Error::from_raw_os_error(code)), path)
        })
        .expect_err("non-vanished task read errors must panic");
        let message = panic
            .downcast_ref::<String>()
            .expect("task read panic has path context");
        assert!(message.contains(&path.display().to_string()));
    }
    assert!(
        std::panic::catch_unwind(|| {
            task_file_result(
                Err(std::io::Error::other("unexpected task read error")),
                path,
            )
        })
        .is_err()
    );
}

fn resident_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read this process status");
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .expect("Linux reports VmRSS in /proc/self/status")
}

fn lifecycle_hooks() -> (OwnerReadTestHooks, Receiver<()>, Receiver<()>, Receiver<()>) {
    let (accept_thread_exited, accept_thread_exit) = mpsc::channel();
    let (accept_thread_joined, accept_thread_join) = mpsc::channel();
    let (service_dropped, service_drop) = mpsc::channel();
    (
        OwnerReadTestHooks {
            clock: Arc::new(MonotonicDeadlineClock::new()),
            drain_started: mpsc::channel().0,
            active_request_registered: None,
            accept_thread_exited: Some(accept_thread_exited),
            accept_thread_joined: Some(accept_thread_joined),
            service_dropped: Some(service_dropped),
            quiescent_stop: None,
        },
        accept_thread_exit,
        accept_thread_join,
        service_drop,
    )
}

fn run_cycles(arm: &str, close_before_drop: bool, retain_authenticated_idle_reader: bool) {
    let directory = tempfile::tempdir().expect("task-scoped owner-cycle directory");
    let runtime_root = secure_runtime_root(directory.path(), arm);
    let path = directory.path().join(format!("{arm}.db"));
    let before_rss = resident_kib();
    assert_eq!(
        Database::live_owner_read_service_count_for_test(),
        0,
        "{arm} begins with no service retained from another cycle"
    );

    for cycle in 1..=CYCLES {
        let thread_baseline = owner_thread_count();
        let (hooks, accept_thread_exit, accept_thread_join, service_drop) = lifecycle_hooks();
        let database =
            Database::open_with_options(&path, owner_options(runtime_root.clone(), hooks))
                .unwrap_or_else(|error| panic!("open {arm} cycle {cycle}: {error:?}"));
        let idle_reader = retain_authenticated_idle_reader.then(|| {
            let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
                ReadSession::open(&path)
            })
            .unwrap_or_else(|error| {
                panic!("open authenticated idle reader for {arm} cycle {cycle}: {error:?}")
            });
            assert_eq!(
                session.route(),
                ReadRoute::Owner,
                "{arm} cycle {cycle} retains an authenticated owner-route reader"
            );
            session
        });
        assert!(
            matches!(accept_thread_exit.try_recv(), Err(TryRecvError::Empty)),
            "{arm} cycle {cycle} does not report an accept-thread exit while its writable handle is still live"
        );
        assert!(
            matches!(accept_thread_join.try_recv(), Err(TryRecvError::Empty)),
            "{arm} cycle {cycle} does not report a service-owned accept-thread join while its writable handle is still live"
        );
        assert!(
            matches!(service_drop.try_recv(), Err(TryRecvError::Empty)),
            "{arm} cycle {cycle} does not report a service drop while its writable handle is still live"
        );
        if close_before_drop {
            database
                .close()
                .unwrap_or_else(|error| panic!("close {arm} cycle {cycle}: {error:?}"));
        }
        drop(database);

        accept_thread_exit
            .recv_timeout(EXIT_WAIT)
            .unwrap_or_else(|error| {
                panic!(
                    "{arm} cycle {cycle} did not observe its accept thread completion: {error:?}"
                )
            });
        if close_before_drop {
            accept_thread_join
                .recv_timeout(EXIT_WAIT)
                .unwrap_or_else(|error| {
                    panic!(
                        "{arm} cycle {cycle} did not observe the service-owned accept-thread join: {error:?}"
                    )
            });
            assert!(
                !matches!(accept_thread_join.try_recv(), Ok(())),
                "{arm} cycle {cycle} reports its service-owned accept-thread join exactly once"
            );
        } else {
            assert!(
                matches!(
                    accept_thread_join.try_recv(),
                    Err(TryRecvError::Disconnected)
                ),
                "{arm} cycle {cycle} drop releases the service without waiting to join its accept thread"
            );
        }
        service_drop
            .recv_timeout(EXIT_WAIT)
            .unwrap_or_else(|error| {
                panic!("{arm} cycle {cycle} did not observe its owner service drop: {error:?}")
            });
        assert_eq!(
            owner_thread_count(),
            thread_baseline,
            "{arm} cycle {cycle} releases its actual contextdb-owner threads after service-owned completion and join"
        );
        assert_eq!(
            Database::live_owner_read_service_count_for_test(),
            0,
            "{arm} cycle {cycle} returns the live-service census to zero after its final service drop"
        );
        drop(idle_reader);
    }

    let growth = resident_kib().saturating_sub(before_rss);
    assert!(
        growth < MAX_RSS_GROWTH_KIB,
        "{arm} grows VmRSS by {growth} KiB after {CYCLES} cycles, below the 64 MiB limit"
    );
}

#[test]
fn writable_open_drop_cycles_return_threads_and_memory() {
    run_cycles("drop", false, false);
    run_cycles("close", true, false);
}

#[test]
fn writable_drop_reaps_an_authenticated_idle_owner_reader() {
    run_cycles("connected-idle-drop", false, true);
}

#[test]
fn writable_close_reaps_an_authenticated_idle_owner_reader() {
    run_cycles("connected-idle-close", true, true);
}
