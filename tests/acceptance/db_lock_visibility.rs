use crate::common::{cli_bin, ensure_release_binaries, run_cli_script};
use contextdb_core::{Error, Lsn, SnapshotId, Value};
use contextdb_engine::Database;
use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_tx::WriteSet;
use std::collections::HashMap;
use std::env;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

fn current_test_exe_for_child() -> PathBuf {
    let path = env::current_exe().expect("current test executable path");
    if path.exists() {
        return path;
    }

    // Other acceptance tests invoke Cargo, which can unlink the on-disk test
    // binary while this process is still running. Linux keeps /proc/self/exe
    // executable in that case.
    #[cfg(target_os = "linux")]
    {
        PathBuf::from("/proc/self/exe")
    }

    #[cfg(not(target_os = "linux"))]
    {
        path
    }
}

fn assert_error<T>(result: contextdb_core::Result<T>, message: &str) -> Error {
    let err = result.err();
    assert!(err.is_some(), "{message}: expected Err, got Ok");
    err.unwrap()
}

/// RED — t19_01
#[test]
fn t19_01_same_process_second_open_returns_typed_database_locked() {
    let tmp = TempDir::new().unwrap();
    let path: PathBuf = tmp.path().join("db.redb");

    let _h1 = Database::open(&path).expect("first open succeeds");
    // A same-process registry must be authoritative even if the advisory lock
    // file disappears while the first handle is still alive.
    let lock_path = path.with_extension("lock");
    std::fs::remove_file(&lock_path).expect("test removes advisory lock file");
    let result = Database::open(&path);
    let err = assert_error(result, "second open in same process must be rejected");
    match err {
        Error::DatabaseLocked {
            holder_pid,
            path: p,
        } => {
            assert_eq!(holder_pid, std::process::id(), "holder_pid must match self");
            assert_eq!(p, path, "error path must match opened path");
        }
        other => panic!("expected DatabaseLocked, got: {other:?}"),
    }
}

/// RED — t19_02
#[test]
fn t19_02_cross_process_concurrent_open_returns_typed_database_locked() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db.redb");
    ensure_release_binaries();

    // Spawn the CLI binary which holds the path open.
    let mut child = Command::new(cli_bin())
        .arg(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Push something so the child has the path created and locked.
    let stdin = child.stdin.as_mut().unwrap();
    use std::io::Write;
    stdin
        .write_all(b"CREATE TABLE t (id UUID PRIMARY KEY)\n")
        .unwrap();
    stdin.flush().unwrap();
    // Give the child a moment to commit and hold the lock.
    std::thread::sleep(Duration::from_millis(500));

    let result = Database::open(&path);
    let err = assert_error(result, "concurrent cross-process open must be rejected");
    assert!(
        matches!(err, Error::DatabaseLocked { .. }),
        "expected DatabaseLocked, got: {err}"
    );
    if let Error::DatabaseLocked { holder_pid, .. } = &err {
        assert_eq!(*holder_pid, child.id(), "holder_pid must match child PID");
        assert_ne!(*holder_pid, 0, "holder_pid must be non-zero");
    }
    {
        use contextdb_core::types::{ContextId, Principal, ScopeLabel};
        use std::collections::BTreeSet;
        let result = Database::open_with_constraints(
            &path,
            Some(BTreeSet::from([ContextId::new(Uuid::from_u128(1))])),
            Some(BTreeSet::from([ScopeLabel::new("edge")])),
            Some(Principal::Agent("a1".into())),
        );
        let err = assert_error(
            result,
            "cross-process composed constraints open must be rejected",
        );
        match err {
            Error::DatabaseLocked {
                holder_pid,
                path: p,
            } => {
                assert_eq!(
                    holder_pid,
                    child.id(),
                    "holder_pid for composed open must match child PID"
                );
                assert_eq!(p, path, "composed open lock error path must match");
            }
            other => {
                panic!("expected DatabaseLocked for cross-process composed open, got: {other:?}")
            }
        }
    }

    let _ = child.kill();
    let _ = child.wait();
}

/// REGRESSION GUARD — t19_03
#[test]
fn t19_03_sequential_cross_process_open_sees_prior_commits() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("seq.redb");

    let script = "\
CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)
INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'row-0')
INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'row-1')
INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'row-2')
INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000004', 'row-3')
INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000005', 'row-4')
";
    let out = run_cli_script(&path, &[], script);
    assert!(
        out.status.success(),
        "child process should seed committed rows; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let db2 = Database::open(&path).expect("sequential reopen must succeed");
    let scan = db2.scan("t", db2.snapshot()).unwrap();
    assert_eq!(scan.len(), 5, "all 5 prior commits must be visible");
}

/// RED — t19_04
#[test]
fn t19_04_same_process_overlap_then_serial_succeeds() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("overlap.redb");

    let h1 = Database::open(&path).unwrap();
    h1.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("v".into(), Value::Text("a".into()));
    h1.execute("INSERT INTO t (id, v) VALUES ($id, $v)", &p)
        .unwrap();

    // While h1 is alive, second open must fail.
    let result = Database::open(&path);
    assert!(
        matches!(result, Err(Error::DatabaseLocked { .. })),
        "expected DatabaseLocked while h1 is alive"
    );

    // Drop h1. Then second open must succeed and see the row.
    h1.close().unwrap();
    drop(h1);
    let h2 = Database::open(&path).expect("second open after drop must succeed");
    let scan = h2.scan("t", h2.snapshot()).unwrap();
    assert_eq!(scan.len(), 1, "row inserted by h1 must be visible in h2");
    let v = scan[0].values.get("v").cloned();
    assert_eq!(v, Some(Value::Text("a".into())));
}

/// RED — t19_05
#[test]
fn t19_05_database_locked_carries_path_and_pid() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("typed.redb");
    let _h1 = Database::open(&path).unwrap();
    let err = assert_error(Database::open(&path), "second open must be rejected");
    match err {
        Error::DatabaseLocked {
            holder_pid,
            path: p,
        } => {
            assert_ne!(holder_pid, 0, "holder_pid must be non-zero");
            assert_eq!(p, path, "path must match");
        }
        other => panic!("expected DatabaseLocked, got: {other:?}"),
    }
}

/// REGRESSION GUARD — t19_06: stale PID lock from a crashed prior process is reclaimable.
#[test]
fn t19_06_crashed_holder_pid_lock_reclaimable() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("stale.redb");

    // Write a .lock file pointing to a known-dead PID. We pick u32::MAX because
    // /proc/4294967295 will not exist on Linux. (Test is skipped on non-Linux
    // since acquire_pid_lock's /proc check is Linux-specific; cfg-gate
    // accordingly.)
    #[cfg(target_os = "linux")]
    {
        let lock_path = path.with_extension("lock");
        std::fs::write(&lock_path, format!("{}", u32::MAX)).unwrap();

        let h = Database::open(&path).expect("stale PID lock must be reclaimed");
        // Sanity: write a row, read it back.
        h.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY)",
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let scan = h.scan("t", h.snapshot()).unwrap();
        assert_eq!(
            scan.len(),
            0,
            "fresh DB after stale-lock reclaim should be empty"
        );
    }
}

/// RED — t19_07: cross-variant open rejection — all open APIs share the lock registry.
#[test]
fn t19_07_cross_variant_open_rejected() {
    use contextdb_core::types::{ContextId, Principal, ScopeLabel};
    use std::collections::BTreeSet;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("xvar.redb");

    // Variant A: open_as_principal first.
    let h1 = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    std::fs::remove_file(path.with_extension("lock"))
        .expect("test removes advisory lock file to force canonical registry path");

    #[cfg(unix)]
    {
        let alias_dir = tmp.path().join("alias");
        std::os::unix::fs::symlink(tmp.path(), &alias_dir).unwrap();
        let alias_path = alias_dir.join("xvar.redb");
        let err = assert_error(
            Database::open(&alias_path),
            "symlink-alias open must be rejected by canonical lock registry",
        );
        match err {
            Error::DatabaseLocked {
                holder_pid,
                path: p,
            } => {
                assert_eq!(
                    holder_pid,
                    std::process::id(),
                    "holder_pid is the same process holding h1"
                );
                assert_eq!(
                    p, path,
                    "lock error path must resolve to the original canonical DB path"
                );
            }
            other => panic!("expected DatabaseLocked for symlink alias, got: {other:?}"),
        }
    }

    // Variant B: open_with_contexts on the same path must be rejected.
    let err = assert_error(
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(Uuid::from_u128(1))])),
        "cross-variant second open must be rejected by the same registry",
    );
    match err {
        Error::DatabaseLocked {
            holder_pid,
            path: p,
        } => {
            assert_eq!(
                holder_pid,
                std::process::id(),
                "holder_pid is the same process holding h1"
            );
            assert_eq!(p, path, "error path must match");
        }
        other => panic!("expected DatabaseLocked, got: {other:?}"),
    }

    // Variant C: also rejected.
    let err2 = assert_error(
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])),
        "cross-variant scope-labels open must also be rejected",
    );
    assert!(matches!(err2, Error::DatabaseLocked { .. }), "got: {err2}");

    // Variant D: composed constraints open is the downstream product surface and
    // must share the same registry.
    let err_constraints = assert_error(
        Database::open_with_constraints(
            &path,
            Some(BTreeSet::from([ContextId::new(Uuid::from_u128(1))])),
            Some(BTreeSet::from([ScopeLabel::new("edge")])),
            Some(Principal::Agent("a1".into())),
        ),
        "cross-variant composed constraints open must also be rejected",
    );
    assert!(
        matches!(err_constraints, Error::DatabaseLocked { .. }),
        "got: {err_constraints}"
    );

    // Variant E: plain open also rejected.
    let err3 = assert_error(Database::open(&path), "plain open must also be rejected");
    assert!(matches!(err3, Error::DatabaseLocked { .. }), "got: {err3}");

    // Drop h1; a different open variant now succeeds.
    drop(h1);
    let _h2 = Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")]))
        .expect("after dropping the principal handle, scope-labels open must succeed");
}

/// REGRESSION GUARD — a final symlink to a not-yet-created DB must resolve to
/// the target path, not become an independent registry/lock key.
#[cfg(unix)]
#[test]
fn t19_08_final_symlink_to_missing_db_uses_target_lock() {
    let tmp = TempDir::new().unwrap();
    let real_path = tmp.path().join("real.redb");
    let alias_path = tmp.path().join("alias.redb");
    std::os::unix::fs::symlink(&real_path, &alias_path).unwrap();

    let _h1 = Database::open(&alias_path).expect("open through final symlink succeeds");
    assert!(real_path.exists(), "symlink open must create the target DB");

    let err = assert_error(
        Database::open(&real_path),
        "target open must share lock with final symlink opener",
    );
    match err {
        Error::DatabaseLocked {
            holder_pid,
            path: p,
        } => {
            assert_eq!(holder_pid, std::process::id());
            assert_eq!(p, real_path);
        }
        other => panic!("expected DatabaseLocked for final symlink alias, got: {other:?}"),
    }
}

/// REGRESSION GUARD — close is a real lifecycle boundary. Once it releases the
/// path lock, the old handle must not continue serving normal reads.
#[test]
fn t19_09_closed_handle_rejects_normal_reads() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("closed.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let snapshot = db.snapshot();
    db.close().unwrap();

    let err = db.scan("t", snapshot).unwrap_err();
    assert!(
        matches!(&err, Error::Other(msg) if msg.contains("closed")),
        "expected closed-handle error, got: {err:?}"
    );
    let changes = std::panic::catch_unwind(AssertUnwindSafe(|| db.changes_since(Lsn(0))));
    assert!(
        changes.is_err(),
        "closed changes_since must not serve stale in-memory state"
    );

    let _reopened = Database::open(&path).expect("released path can be reopened");
}

struct CrossHandleClosedReadPlugin {
    target: Arc<Database>,
    snapshot: SnapshotId,
    observed_closed: Arc<Mutex<bool>>,
}

impl DatabasePlugin for CrossHandleClosedReadPlugin {
    fn pre_commit(&self, _ws: &WriteSet, _source: CommitSource) -> contextdb_core::Result<()> {
        match self.target.scan("t", self.snapshot) {
            Err(Error::Other(message)) if message.contains("closed") => {
                *self.observed_closed.lock().unwrap() = true;
                Ok(())
            }
            Ok(rows) => Err(Error::Other(format!(
                "closed cross-handle scan unexpectedly returned {} rows",
                rows.len()
            ))),
            Err(err) => Err(err),
        }
    }
}

/// REGRESSION GUARD — nested public calls on a different Database handle must
/// take that handle's lifecycle gate rather than reusing the caller's.
#[test]
fn t19_10_cross_handle_nested_operation_checks_target_closed_state() {
    let tmp = TempDir::new().unwrap();
    let target_path = tmp.path().join("target.redb");
    let caller_path = tmp.path().join("caller.redb");

    let target = Arc::new(Database::open(&target_path).unwrap());
    target
        .execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    let snapshot = target.snapshot();
    target.close().unwrap();

    let observed_closed = Arc::new(Mutex::new(false));
    let plugin = Arc::new(CrossHandleClosedReadPlugin {
        target: target.clone(),
        snapshot,
        observed_closed: observed_closed.clone(),
    });
    let caller = Database::open_with_plugin(&caller_path, plugin).unwrap();
    caller
        .execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    caller
        .execute(
            "INSERT INTO t (id) VALUES ('00000000-0000-0000-0000-000000000001')",
            &HashMap::new(),
        )
        .unwrap();

    assert!(
        *observed_closed.lock().unwrap(),
        "cross-handle nested operation must observe the target handle is closed"
    );
}

/// REGRESSION GUARD — if the sidecar lock file disappears while a handle is
/// alive, a rejected same-process open must not leave a fresh lock authority
/// that lets another process open concurrently.
#[test]
fn t19_11_lockfile_unlink_does_not_split_cross_process_authority() {
    if let Ok(path) = env::var("CONTEXTDB_T19_CHILD_OPEN_PATH") {
        let path = PathBuf::from(path);
        match Database::open(&path) {
            Err(Error::DatabaseLocked { .. }) => std::process::exit(0),
            Ok(_) => std::process::exit(2),
            Err(_) => std::process::exit(3),
        }
    }

    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("split.redb");

    let _h1 = Database::open(&path).expect("first open succeeds");
    std::fs::remove_file(path.with_extension("lock")).expect("test removes advisory lock file");

    let err = assert_error(
        Database::open(&path),
        "same-process rejected open must not recreate the lock authority",
    );
    assert!(
        matches!(err, Error::DatabaseLocked { .. }),
        "expected same-process DatabaseLocked, got: {err:?}"
    );

    let child = Command::new(current_test_exe_for_child())
        .arg("--exact")
        .arg("db_lock_visibility::t19_11_lockfile_unlink_does_not_split_cross_process_authority")
        .arg("--nocapture")
        .env("CONTEXTDB_T19_CHILD_OPEN_PATH", &path)
        .status()
        .unwrap();
    assert!(
        child.success(),
        "cross-process open must remain locked after sidecar removal and rejected same-process open"
    );
}
