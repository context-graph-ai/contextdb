//! Exit `2` means the invocation itself was wrong and nothing ran — no work
//! attempted, per the exit-code table. The CLI must never open (and, for a
//! fresh path, create) the database file before it validates that
//! `--tenant-id` was paired with a sync endpoint — a caller that branches on
//! exit `2` to mean "safe to retry with a fixed command line, nothing
//! changed on disk" must never find a brand-new database file waiting for
//! it.

use std::io::Write;
use std::process::{Command, Stdio};

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "cdb-usage-side-effects-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create unique temp dir");
    dir
}

fn run_cli(args: &[&str], stdin_sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    for a in args {
        cmd.arg(a);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb-cli");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin_sql.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

// A `--tenant-id` with no sync endpoint is a usage error: the
// invocation is wrong, so the database must never be opened (and, since the
// path is fresh, created on disk) before that check runs.
#[test]
fn usage_error_before_sync_endpoint_check_does_not_create_the_database() {
    let dir = unique_temp_dir("cli-fresh");
    let db_path = dir.join("fresh.db");
    assert!(!db_path.exists(), "the path must start out absent");

    let (code, stdout, stderr) = run_cli(
        &["--tenant-id", "acme", db_path.to_str().expect("utf8 path")],
        "",
    );
    assert_eq!(code, Some(2), "stdout:\n{stdout}\nstderr:\n{stderr}");

    assert!(
        !db_path.exists(),
        "exit 2 must mean no work was attempted — the database file must never \
         be created before the --tenant-id/--sync-endpoint check runs. Directory \
         contents: {:?}",
        std::fs::read_dir(&dir)
            .map(|entries| entries
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect::<Vec<_>>())
            .unwrap_or_default()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// (c) Positive control — a VALID invocation still creates the
// database. Pins that the contract in the test above must not stop a normal run
// from creating a fresh file.
#[test]
fn valid_invocation_still_creates_the_database() {
    let dir = unique_temp_dir("cli-valid");
    let db_path = dir.join("valid.db");
    assert!(!db_path.exists(), "the path must start out absent");

    let (code, stdout, stderr) = run_cli(&[db_path.to_str().expect("utf8 path")], ".quit\n");
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        db_path.exists(),
        "a valid invocation must still create the database file"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
