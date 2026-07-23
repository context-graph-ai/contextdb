//! Under `--json`, stdout is JSON Lines and stderr is JSON Lines too — every
//! error and every notice, from any source inside the process, is one
//! complete JSON document per line. A consumer parsing stderr should never
//! have to fall back to text scanning partway through the stream.
//!
//! Scope: this covers everything the process controls once argument parsing
//! has succeeded (SQL/meta-command errors, startup validation errors, and
//! informational notices). It deliberately does NOT cover `clap`'s own
//! native failures (an unrecognized flag, a missing required argument) —
//! those are raised by `clap` itself before this process's own error-reporting
//! ever runs, and are out of scope for this contract by design.

use std::io::Write;
use std::process::{Command, Stdio};

fn run(args: &[&str], stdin_sql: &str) -> (Option<i32>, String, String) {
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

fn assert_every_line_is_json(stderr: &str, context: &str) {
    for line in stderr.lines().filter(|l| !l.trim().is_empty()) {
        serde_json::from_str::<serde_json::Value>(line.trim()).unwrap_or_else(|e| {
            panic!("{context}: every stderr line must be JSON, but {line:?} failed: {e}\nfull stderr:\n{stderr}")
        });
    }
}

// (a) — a plain in-REPL SQL error under --json.
#[test]
fn json_stderr_pure_for_in_repl_sql_error() {
    let (code, _stdout, stderr) = run(&["--json", ":memory:"], "SELET bad;\n.quit\n");
    assert_eq!(code, Some(1), "stderr:\n{stderr}");
    assert!(!stderr.trim().is_empty(), "expected an error on stderr");
    assert_every_line_is_json(&stderr, "in-REPL SQL error");
}

// (a) — a startup usage error from the custom `--memory-limit`/`--disk-limit`
// value parser, which runs before the REPL loop starts at all.
#[test]
fn json_stderr_pure_for_startup_usage_error() {
    let (code, _stdout, stderr) = run(&["--json", "--memory-limit", "12Q", ":memory:"], "");
    assert_eq!(code, Some(2), "stderr:\n{stderr}");
    assert!(
        !stderr.trim().is_empty(),
        "expected a usage error on stderr"
    );
    assert_every_line_is_json(&stderr, "startup usage error");
}

// (a) — a session that produces BOTH notices (the `--nats-url` deprecation
// warning and the resulting unreachable-endpoint warning) and a real error,
// all in the same stderr stream.
#[test]
fn json_stderr_pure_across_notices_and_an_error() {
    let (code, _stdout, stderr) = run(
        &[
            "--json",
            "--tenant-id",
            "acme",
            "--nats-url",
            "nats://localhost:1",
            ":memory:",
        ],
        "SELET bad;\n.quit\n",
    );
    assert_eq!(code, Some(1), "stderr:\n{stderr}");
    let lines: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "expected at least one notice and one error on stderr, got:\n{stderr}"
    );
    assert_every_line_is_json(&stderr, "notices plus an error");
}

// (b) — `.help` under --json is the one meta-command that does not emit a
// result document on stdout (it is prose that changes with every feature);
// pin its ACTUAL wire shape: a single `{"help":[...]}` document on stderr,
// stdout empty, exit 0. (The docs disagree with themselves on where `.help`
// goes under `--json` — this test pins the binary's real, chosen behavior,
// not either copy of the doc text.)
#[test]
fn json_help_emits_single_document_on_stderr() {
    let (code, stdout, stderr) = run(&["--json", ":memory:"], ".help\n.quit\n");
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.trim().is_empty(),
        "stdout must stay empty, got:\n{stdout}"
    );

    let stderr_lines: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        stderr_lines.len(),
        1,
        "`.help` under --json must emit exactly one stderr document, got:\n{stderr}"
    );
    let doc: serde_json::Value = serde_json::from_str(stderr_lines[0].trim()).unwrap_or_else(|e| {
        panic!(
            "the one stderr line must be JSON: {e}\nline: {}",
            stderr_lines[0]
        )
    });
    let help = doc["help"]
        .as_array()
        .unwrap_or_else(|| panic!("expected {{\"help\": [...]}}, got: {doc}"));
    assert!(
        !help.is_empty(),
        "the help array must not be empty, got: {doc}"
    );
    assert!(
        help.iter().all(|line| line.is_string()),
        "every help entry must be a string, got: {doc}"
    );
}
