//! Shared real-reader contention fixture for both default and seam-enabled tests.
use super::*;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::mpsc;

const WATCHDOG: std::time::Duration = std::time::Duration::from_secs(60);

pub(super) fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("handoff.db");
    let seed = crate::Database::open(&path).expect("create a durable store");
    seed.execute(
        "CREATE TABLE erased_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL",
        &HashMap::new(),
    )
    .expect("create the table");
    for id in 1..=3_i64 {
        seed.execute(
            "INSERT INTO erased_rows (id, payload) VALUES ($id, 'secret')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .expect("seed a row");
    }
    seed.close().expect("release the seed writer");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("secure the runtime directory");
    (directory, path, runtime)
}

// Run B while a real unrelated A leave owns its store exclusion and root lifetime hold.
// The wait hook reports a blocking root acquisition directly; no timeout is an oracle.
#[cfg(target_os = "linux")]
pub(super) fn during_unrelated_reader_leave<R: Send>(
    operation: impl FnOnce() -> R + Send,
) -> (R, bool) {
    use read_persistence_test_scaffold::{
        ReaderPausePoint, install_reader_join_wait_hook_for_test, pause_readers_at_for_test,
    };
    let private_base = platform_reader_runtime_base().unwrap();
    std::fs::set_permissions(&private_base, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (_directory, path, supplied) = fixture();
    let earlier = crate::ReadSession::open_in_runtime_dir_with_clock_for_test(
        &path,
        crate::ReadSessionOptions::default(),
        Some(supplied),
        std::sync::Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )
    .unwrap();
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::LeavingWithRegistryExclusion);
    std::thread::scope(|scope| {
        let leaving = scope.spawn(move || drop(earlier));
        let ticket = pause
            .wait_for_paused_reader(WATCHDOG)
            .expect("A's real leave holds its registry exclusion");
        let (edge_tx, edge_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let working = scope.spawn(move || {
            let waiting = edge_tx.clone();
            install_reader_join_wait_hook_for_test(Box::new(move || {
                let _ = waiting.send(true);
            }));
            let result = operation();
            let _ = result_tx.send(result);
            let _ = edge_tx.send(false);
        });
        let blocked = edge_rx
            .recv_timeout(WATCHDOG)
            .expect("B either finishes or reports its actual root wait");
        pause.release(ticket);
        drop(pause);
        leaving.join().unwrap();
        let result = result_rx
            .recv_timeout(WATCHDOG)
            .expect("B completes after A's finite leave");
        working.join().unwrap();
        (result, blocked)
    })
}
