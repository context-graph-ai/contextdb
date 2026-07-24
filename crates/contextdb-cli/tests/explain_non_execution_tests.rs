//! `.explain <sql>` must show the engine's chosen plan without ever running
//! the statement — the same contract for every output mode, including
//! `--json`. The JSON branch must never call the engine's real execute path
//! to get a trace: `.explain DELETE FROM t` must never delete rows,
//! `.explain UPDATE ...` must never mutate them, and `.explain INSERT ...`
//! must never insert a row. A caller asking "what would this do" must never
//! get it done to them instead.

use std::io::Write;
use std::process::{Command, Stdio};

fn run_cli(extra_args: &[&str], stdin_sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(":memory:");
    for a in extra_args {
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

fn stdout_docs(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .collect()
}

// (a) `.explain DELETE FROM t` under --json must not delete anything.
#[test]
fn explain_json_delete_leaves_rows_intact() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a');\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b');\n\
               .explain DELETE FROM t\n\
               SELECT id FROM t;\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let docs = stdout_docs(&stdout);
    assert!(
        docs.iter().any(|d| d.get("explain").is_some()),
        "an explain document must be emitted, got docs:\n{docs:?}"
    );
    let rows = docs
        .last()
        .and_then(|d| d.as_array().cloned())
        .unwrap_or_else(|| panic!("the follow-up SELECT must emit a JSON array, got:\n{stdout}"));
    assert_eq!(
        rows.len(),
        2,
        "`.explain DELETE FROM t` under --json must NOT delete the rows — the \
         JSON branch must never execute the statement to get a trace. Rows after explain:\n{rows:?}"
    );
}

// (b) `.explain UPDATE ...` under --json must not mutate anything.
#[test]
fn explain_json_update_leaves_rows_intact() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'original');\n\
               .explain UPDATE t SET name = 'mutated' WHERE id = '00000000-0000-0000-0000-000000000001'\n\
               SELECT name FROM t WHERE id = '00000000-0000-0000-0000-000000000001';\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let docs = stdout_docs(&stdout);
    assert!(
        docs.iter().any(|d| d.get("explain").is_some()),
        "an explain document must be emitted, got docs:\n{docs:?}"
    );
    let rows = docs
        .last()
        .and_then(|d| d.as_array().cloned())
        .unwrap_or_else(|| panic!("the follow-up SELECT must emit a JSON array, got:\n{stdout}"));
    assert_eq!(
        rows.len(),
        1,
        "the row must still exist, got:\n{rows:?}\nfull stdout:\n{stdout}"
    );
    assert_eq!(
        rows[0]["name"],
        serde_json::json!("original"),
        "`.explain UPDATE ...` under --json must NOT change the row, got:\n{rows:?}"
    );
}

// (c) `.explain INSERT ...` under --json must not add a row. Cheapest
// possible observation of non-execution: presence, not content, changes.
#[test]
fn explain_json_insert_does_not_add_a_row() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               .explain INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000009', 'ghost')\n\
               SELECT id FROM t;\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let docs = stdout_docs(&stdout);
    assert!(
        docs.iter().any(|d| d.get("explain").is_some()),
        "an explain document must be emitted, got docs:\n{docs:?}"
    );
    let rows = docs
        .last()
        .and_then(|d| d.as_array().cloned())
        .unwrap_or_else(|| panic!("the follow-up SELECT must emit a JSON array, got:\n{stdout}"));
    assert_eq!(
        rows.len(),
        0,
        "`.explain INSERT ...` under --json must NOT add a row, got:\n{rows:?}"
    );
}

// (c) baseline — a read-only statement's explain document still has the
// expected shape under --json (guards against a fix that breaks the SELECT
// path while correcting the mutating one).
#[test]
fn explain_json_select_emits_plan_document() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY);\n.explain SELECT * FROM t\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = stdout_docs(&stdout);
    let explain = docs
        .iter()
        .find(|d| d.get("explain").is_some())
        .unwrap_or_else(|| {
            panic!(".explain SELECT under --json must emit an explain document, got:\n{stdout}")
        });
    assert!(
        explain["explain"]["physical_plan"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "explain.physical_plan must be a non-empty string, got: {explain}"
    );
}

// (d) baseline pin, positive control — human mode routes `.explain`
// through the non-executing plan path (`explain_output`/`db.explain`), never
// `db.execute`. This isolates the contract above to the --json branch
// specifically.
#[test]
fn explain_human_mode_still_does_not_execute() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a');\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b');\n\
               .explain DELETE FROM t\n\
               SELECT id FROM t;\n";
    let (code, stdout, stderr) = run_cli(&[], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    // Each id appears twice if the row survives: once in the scripted INSERT
    // echo, once in the final SELECT's rendered table row. Once (the echo
    // only) would mean the row was actually deleted.
    for id in [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    ] {
        assert_eq!(
            stdout.matches(id).count(),
            2,
            "human-mode `.explain DELETE FROM t` must leave {id} intact (echo + SELECT row), got stdout:\n{stdout}"
        );
    }
}
