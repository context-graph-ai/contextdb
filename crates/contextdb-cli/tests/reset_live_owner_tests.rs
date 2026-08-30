//! A reset must never replace a store held by a live CLI process.
//!
//! This drives two real `contextdb` processes.  The child owns the database
//! through its open stdin session; the parent invokes the installed CLI's
//! `reset --force` command and proves the original file and binary companion remain
//! intact before asking the child to make a second durable write.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

fn lock_path_for(path: &Path) -> PathBuf {
    let mut companion = path.as_os_str().to_os_string();
    companion.push(".lock");
    PathBuf::from(companion)
}

fn wait_for_owner_lock(path: &Path) {
    let lock = lock_path_for(path);
    for _ in 0..100_000 {
        if lock.is_file() {
            return;
        }
        std::thread::yield_now();
    }
    panic!(
        "child never published its ownership companion at {}",
        lock.display()
    );
}

#[test]
fn reset_force_refuses_a_live_owner_without_unlinking_its_store_or_lock() {
    let dir = tempfile::tempdir().expect("scratch directory");
    let path = dir.path().join("owned.db");
    let mut child = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .arg(&path)
        // The owning process creates the store and writes to it, which is what
        // `--write` authorizes; a bare path would only read.
        .arg("--write")
        .arg("--json")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start owning contextdb process");
    let child_pid = child.id();
    let stdin = child.stdin.as_mut().expect("child stdin");
    stdin
        .write_all(b"CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);\nINSERT INTO notes (id, body) VALUES (1, 'before');\n")
        .expect("write initial row to owning process");
    stdin.flush().expect("flush initial row");
    let mut child_stdout = BufReader::new(child.stdout.take().expect("child stdout"));
    let mut initial = String::new();
    child_stdout
        .read_line(&mut initial)
        .expect("read create acknowledgement");
    initial.clear();
    child_stdout
        .read_line(&mut initial)
        .expect("read initial write acknowledgement");
    assert_eq!(
        initial.trim(),
        r#"{"rows_affected":1}"#,
        "child wrote before reset"
    );
    wait_for_owner_lock(&path);

    let before_db = fs::read(&path).expect("owner-created database bytes");
    let lock_path = lock_path_for(&path);
    let before_lock = fs::read(&lock_path).expect("owner lock bytes");
    #[cfg(unix)]
    let before_db_identity = {
        let metadata = fs::metadata(&path).expect("database metadata");
        (metadata.dev(), metadata.ino())
    };
    #[cfg(unix)]
    let before_lock_identity = {
        let metadata = fs::metadata(&lock_path).expect("lock metadata");
        (metadata.dev(), metadata.ino())
    };

    let reset = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .arg("reset")
        .arg(&path)
        .arg("--force")
        .output()
        .expect("run installed reset command");
    let reset_output = format!(
        "{}{}",
        String::from_utf8_lossy(&reset.stdout),
        String::from_utf8_lossy(&reset.stderr)
    );
    assert!(
        !reset.status.success(),
        "reset must refuse a live owner, not replace its file. output:\n{reset_output}"
    );
    assert!(
        reset_output.contains(&child_pid.to_string())
            && reset_output.contains(&path.display().to_string()),
        "refusal must name the exact owner PID and database path. output:\n{reset_output}"
    );
    assert_eq!(fs::read(&path).expect("database after refusal"), before_db);
    assert_eq!(
        fs::read(&lock_path).expect("lock after refusal"),
        before_lock
    );
    #[cfg(unix)]
    {
        let database = fs::metadata(&path).expect("database metadata after refusal");
        let lock = fs::metadata(&lock_path).expect("lock metadata after refusal");
        assert_eq!(
            (database.dev(), database.ino()),
            before_db_identity,
            "a refused reset must retain the exact database file, not replace it with equal bytes"
        );
        assert_eq!(
            (lock.dev(), lock.ino()),
            before_lock_identity,
            "a refused reset must retain the exact coordination lock, not replace it with equal bytes"
        );
    }

    let stdin = child
        .stdin
        .as_mut()
        .expect("child stdin stays open after refusal");
    stdin
        .write_all(b"INSERT INTO notes (id, body) VALUES (2, 'after');\n")
        .expect("child remains able to write after refused reset");
    stdin.flush().expect("flush continued write");
    let mut after = String::new();
    child_stdout
        .read_line(&mut after)
        .expect("read continued write acknowledgement");
    assert_eq!(
        after.trim(),
        r#"{"rows_affected":1}"#,
        "child wrote after reset refusal"
    );
    drop(child.stdin.take());
    let child_status = child.wait().expect("wait for owner exit");
    assert!(
        child_status.success(),
        "owning CLI must exit cleanly: {child_status}"
    );

    let mut probe = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .arg(&path)
        .arg("--json")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start readback CLI");
    probe
        .stdin
        .as_mut()
        .expect("readback stdin")
        .write_all(b"SELECT id, body FROM notes ORDER BY id;\n")
        .expect("readback SQL");
    let readback = probe.wait_with_output().expect("readback output");
    assert!(readback.status.success(), "readback failed: {readback:?}");
    let document: serde_json::Value =
        serde_json::from_slice(&readback.stdout).expect("readback must be a JSON result");
    // A successful ordinary SELECT is one namespaced, column-carrying
    // document; the bare row array it used to publish is gone.
    let actual = document
        .get("result")
        .and_then(|result| result.get("rows"))
        .cloned()
        .unwrap_or_else(|| panic!("readback must publish a result document, got: {document}"));
    let expected: serde_json::Value =
        serde_json::from_str(r#"[{"id":1,"body":"before"},{"id":2,"body":"after"}]"#)
            .expect("expected readback JSON");
    assert_eq!(
        actual, expected,
        "the original store must retain both child writes after reset refused ownership"
    );

    let final_reset = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .arg("reset")
        .arg(&path)
        .arg("--force")
        .output()
        .expect("reset after owner exit");
    assert!(
        final_reset.status.success(),
        "reset must succeed once the owner has exited: {}",
        String::from_utf8_lossy(&final_reset.stderr)
    );
    let fresh = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .arg(&path)
        .arg("--write")
        .arg("--json")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("open freshly reset database");
    let mut fresh = fresh;
    fresh
        .stdin
        .as_mut()
        .expect("fresh database stdin")
        .write_all(b"CREATE TABLE fresh (id INTEGER PRIMARY KEY);\n")
        .expect("create table after reset");
    let fresh_output = fresh.wait_with_output().expect("fresh database output");
    assert!(
        fresh_output.status.success(),
        "a successful reset must leave a usable empty store: {fresh_output:?}"
    );
}
