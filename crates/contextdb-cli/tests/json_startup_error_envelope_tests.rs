//! Under `--json`, every error is supposed to be one `{"error":{...}}` JSON
//! document on stderr — the whole point being that a consumer never has to
//! fall back to parsing human prose. That contract holds for errors raised
//! inside the REPL (SQL errors, meta-command errors), but `main.rs`'s own
//! error sites — argv validation, opening the database, the final shutdown
//! sequence — still `eprintln!` plain human text regardless of `--json`.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};

fn run(args: &[&str]) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb-cli"));
    for a in args {
        cmd.arg(a);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb-cli");
    // Nothing reads stdin in these scenarios (the process exits during
    // startup or shutdown before the REPL loop reads a line); dropping the
    // handle closes it so the process never blocks waiting on input.
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn run_with_stdin(args: &[&str], stdin_sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb-cli"));
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

/// The first non-empty stderr line, parsed as JSON — panics naming the whole
/// stream when no line parses, so a plain-text failure is easy to diagnose.
fn first_stderr_envelope(stderr: &str) -> serde_json::Value {
    stderr
        .lines()
        .find(|l| !l.trim().is_empty())
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .unwrap_or_else(|| {
            panic!("stderr's first line must be a JSON error envelope, got stderr:\n{stderr}")
        })
}

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("cdb-json-startup-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create unique temp dir");
    dir
}

// An invalid --memory-limit value is a usage error (argv is wrong,
// nothing ran); under --json it must still be one envelope on stderr.
#[test]
fn json_invalid_memory_limit_emits_usage_envelope() {
    let (code, stdout, stderr) = run(&["--json", "--memory-limit", "12Q", ":memory:"]);
    assert_eq!(code, Some(2), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got:\n{stdout}"
    );
    let envelope = first_stderr_envelope(&stderr);
    assert_eq!(
        envelope["error"]["class"],
        serde_json::json!("usage"),
        "main.rs must never eprintln! plain text here regardless of --json, got: {envelope}\nfull stderr:\n{stderr}"
    );
    assert!(
        envelope["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("--memory-limit")),
        "got: {envelope}"
    );
}

// --tenant-id with no sync endpoint is an incomplete flag combination
// (a usage error); under --json it must be one envelope on stderr.
#[test]
fn json_tenant_id_without_endpoint_emits_usage_envelope() {
    let (code, stdout, stderr) = run(&["--json", "--tenant-id", "acme", ":memory:"]);
    assert_eq!(code, Some(2), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got:\n{stdout}"
    );
    let envelope = first_stderr_envelope(&stderr);
    assert_eq!(
        envelope["error"]["class"],
        serde_json::json!("usage"),
        "main.rs must never eprintln! plain text here regardless of --json, got: {envelope}\nfull stderr:\n{stderr}"
    );
    assert!(
        envelope["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("--sync-endpoint")),
        "got: {envelope}"
    );
}

// A database that cannot be opened (here: permission denied) is a
// definitive failure of a valid invocation, not a usage error; under --json
// it must be one envelope on stderr, exit 1.
#[test]
fn json_database_open_failure_emits_error_envelope() {
    let dir = unique_temp_dir("open-failure");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
        .expect("chmod denied dir");
    let db_path = dir.join("db.sqlite");
    let (code, stdout, stderr) = run(&["--json", db_path.to_str().expect("utf8 path")]);
    let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755));
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(code, Some(1), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got:\n{stdout}"
    );
    let envelope = first_stderr_envelope(&stderr);
    // The exact class (io vs. the sql catch-all) isn't pinned by the docs —
    // what's pinned is that it's a real envelope, not the usage class (the
    // invocation itself was fine; the failure happened after argv parsing),
    // and that the message identifies the failure.
    assert_ne!(
        envelope["error"]["class"],
        serde_json::json!("usage"),
        "a database-open failure is not a usage error, got: {envelope}"
    );
    assert!(
        envelope["error"]["class"].is_string(),
        "main.rs must never eprintln! plain text here regardless of --json, got: {envelope}\nfull stderr:\n{stderr}"
    );
    assert!(
        envelope["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("open")),
        "got: {envelope}"
    );
}

// A final sync push that fails at shutdown (here: an endpoint the
// transport cannot dial) is a definitive failure; under --json it must be one
// envelope on stderr, exit 1. Cheaply constructible: a syntactically-accepted
// but undialable endpoint fails fast, no live hub needed.
#[test]
fn json_final_sync_push_failure_emits_error_envelope() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY);\n\
               INSERT INTO t (id) VALUES ('00000000-0000-0000-0000-000000000001');\n\
               .quit\n";
    let (code, stdout, stderr) = run_with_stdin(
        &[
            "--json",
            "--tenant-id",
            "acme",
            "--sync-endpoint",
            "not-a-real-endpoint",
            ":memory:",
        ],
        sql,
    );
    assert_eq!(
        code,
        Some(1),
        "a failed final sync push is a definitive failure. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // Find the document that IS an error envelope, not just the first
    // parseable line: a startup connection probe warning can legitimately
    // precede it on stderr as its own JSON document (a `{"notice":{...}}`,
    // distinct from `{"error":{...}}` — the run hasn't failed yet at that
    // point, and conflating the two would misreport the exit-3
    // interrupted-push case as a hard error). The assertion's job is only to
    // confirm the real error envelope exists somewhere in the stream.
    let envelope = stderr
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .find(|d| d.get("error").is_some())
        .unwrap_or_else(|| {
            panic!(
                "at least one stderr line must be a JSON error envelope, never \
                 plain text like \"Final sync push failed: ...\". stderr:\n{stderr}"
            )
        });
    assert!(
        envelope["error"]["class"].is_string(),
        "got: {envelope}\nfull stderr:\n{stderr}"
    );
}
