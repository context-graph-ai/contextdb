//! CLI routing for `Error::ImmutableColumn`.
//!
//! REWRITTEN 2026-07-22: `cl02_immutable_column_is_not_fatal` asserted the OLD
//! contract — `ImmutableColumn` classified non-fatal via the CLI's
//! fatal/non-fatal split, so it printed `Error: ...` to STDOUT and exited 0.
//! This is reversed: every runtime error, fatal or not, now renders to
//! STDERR and fails the run — the fatal/non-fatal split
//! (`is_fatal_cli_error` / `classify_cli_error`) has no remaining consumer
//! and is deleted with it. Rewritten in place rather than deleted (AGENTS.md
//! requires mutation-testing evidence to delete or merge a test; this is a
//! premise reversal, not a removal, and keeps the file's product concern:
//! how the CLI routes ImmutableColumn).

use std::io::Write;
use std::process::{Command, Stdio};

fn run_cli(stdin_sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(":memory:");
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

// ============================================================================
// An UPDATE on an IMMUTABLE column must route to stderr and
// fail the run — ImmutableColumn must never be classified non-fatal in a way
// that prints "Error: ..." to STDOUT and exits 0, indistinguishable from
// success to a script branching on `$?`.
// ============================================================================
#[test]
fn cl02_immutable_column_routes_to_stderr_and_exits_one() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, decision_type TEXT NOT NULL IMMUTABLE);\n\
               INSERT INTO t (id, decision_type) VALUES ('00000000-0000-0000-0000-000000000001', 'a');\n\
               UPDATE t SET decision_type = 'b' WHERE id = '00000000-0000-0000-0000-000000000001';\n";
    let (code, stdout, stderr) = run_cli(sql);
    assert_eq!(
        code,
        Some(1),
        "an UPDATE on an IMMUTABLE column must fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("Error:"),
        "no error text may reach stdout — only the echoed INSERT line is expected there. stdout:\n{stdout}"
    );
    assert!(
        stderr.contains("decision_type"),
        "stderr must name the immutable column. stderr:\n{stderr}"
    );
}
