//! Machine-readable output (`--json`) and complete human output for the CLI.
//!
//! An agent or script needs query results as structured JSON, and a human
//! result is COMPLETE or it is refused — no renderer truncates, and the row-cap
//! flag that used to disable a truncating renderer no longer exists. These
//! drive the binary end-to-end so the behavior is proven through the argument
//! parser, the REPL, and the formatter together.

use std::io::Write;
use std::process::{Command, Stdio};

/// Run the CLI over `:memory:` with the given extra args, feeding `stdin_sql` on
/// stdin (scripted, non-interactive). Returns (success, stdout, stderr).
fn run_cli(extra_args: &[&str], stdin_sql: &str) -> (bool, String, String) {
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
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The rows of the last result document on stdout.
///
/// A successful ordinary SELECT is ONE namespaced, column-carrying document —
/// `{"result":{"columns":[…],"rows":[…]}}` — not a bare array. A consumer that
/// could not tell a result apart from a cursor page or an error document had no
/// contract to parse, which is why the bare array is gone with no deprecation
/// layer.
fn last_result_rows(stdout: &str) -> Option<Vec<serde_json::Value>> {
    stdout
        .lines()
        .rev()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .find_map(|document| document.get("result")?.get("rows")?.as_array().cloned())
}

#[test]
fn json_select_emits_array_of_objects() {
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES (1, 'alice');\n\
               INSERT INTO t (id, name) VALUES (2, 'bob');\n\
               SELECT id, name FROM t ORDER BY id;\n";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(ok, "cli must exit 0. stderr:\n{stderr}\nstdout:\n{stdout}");

    let arr = last_result_rows(&stdout)
        .unwrap_or_else(|| panic!("no result document on stdout. stdout:\n{stdout}"));

    assert_eq!(arr.len(), 2, "two rows expected, got:\n{stdout}");
    assert_eq!(arr[0]["id"], serde_json::json!(1));
    assert_eq!(arr[0]["name"], serde_json::json!("alice"));
    assert_eq!(arr[1]["id"], serde_json::json!(2));
    assert_eq!(arr[1]["name"], serde_json::json!("bob"));
}

/// Build SQL that creates a table and inserts `n` rows, then a `SELECT *`.
fn insert_n_then_select(n: usize) -> String {
    let mut s = String::from("CREATE TABLE big (id INTEGER PRIMARY KEY, name TEXT);\n");
    for i in 0..n {
        s.push_str(&format!(
            "INSERT INTO big (id, name) VALUES ({i}, 'row{i}');\n"
        ));
    }
    s.push_str("SELECT * FROM big ORDER BY id;\n");
    s
}

/// Count the table's DATA rows in ASCII output: lines starting with '|' are the
/// header plus each data row; subtract the single header row.
fn data_row_count(stdout: &str) -> usize {
    let bar_lines = stdout.lines().filter(|l| l.starts_with('|')).count();
    bar_lines.saturating_sub(1)
}

/// A human result publishes every row it has. The old default truncated at 100
/// and printed a footer naming a flag to turn the truncation off; both are
/// gone, because a result the renderer silently shortened is an answer nobody
/// can trust.
#[test]
fn human_output_publishes_every_row_of_a_complete_result() {
    let (ok, stdout, stderr) = run_cli(&[], &insert_n_then_select(150));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    assert_eq!(
        data_row_count(&stdout),
        150,
        "every row of a complete result is printed. stdout tail:\n{}",
        stdout.lines().rev().take(6).collect::<Vec<_>>().join("\n")
    );
    assert!(
        !stdout.contains("showing"),
        "no truncation footer exists to print. stdout:\n{stdout}"
    );
}

/// The flag that disabled the removed truncation is removed with it, and with
/// no deprecation layer: it is an invalid invocation, not a no-op.
#[test]
fn the_removed_row_cap_flag_is_not_an_accepted_spelling() {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(":memory:").arg("--all");
    let out = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn contextdb-cli");
    assert_eq!(
        out.status.code(),
        Some(2),
        "a removed flag is rejected by the argument parser. stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn json_output_publishes_every_row() {
    let (ok, stdout, stderr) = run_cli(&["--json"], &insert_n_then_select(150));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    let arr = last_result_rows(&stdout)
        .unwrap_or_else(|| panic!("no result document on stdout. stdout:\n{stdout}"));
    assert_eq!(arr.len(), 150, "machine rendering publishes every row too");
}

// A UNIQUE-violation runtime error used to leave `ok` true (exit 0), because
// that class was classified non-fatal. Every runtime error now fails the run
// (exit 1), so `ok` is false here; the error still goes to stderr, stdout
// stays pure JSON, and the surviving statements still produce their JSON.
#[test]
fn json_stdout_is_pure_and_run_exits_one_when_a_runtime_error_occurs() {
    // A UNIQUE/PRIMARY-KEY violation is a runtime error: the SESSION
    // continues (the surviving statements still run), but the RUN now fails.
    // Under --json its "Error:" line must go to stderr, never stdout — stdout
    // must remain pure JSON so a machine consumer can parse the stream.
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES (1, 'a');\n\
               INSERT INTO t (id, name) VALUES (1, 'dup');\n\
               SELECT id, name FROM t ORDER BY id;\n";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        !ok,
        "a run that hit a runtime error must exit non-zero. stderr:\n{stderr}"
    );

    // (a) the error is a JSON envelope on STDERR — the general --json error
    // contract (json_meta_commands.rs's json_sql_error_emits_error_envelope_on_stderr
    // pins the general case). Parsing the envelope and checking its fields is
    // strictly stronger than a bare `stderr.contains("Error:")` prefix check.
    let error_doc: serde_json::Value = stderr
        .lines()
        .next()
        .and_then(|l| serde_json::from_str(l.trim()).ok())
        .unwrap_or_else(|| {
            panic!("stderr's first line must be a JSON error envelope, got stderr:\n{stderr}")
        });
    assert_eq!(
        error_doc["error"]["class"],
        serde_json::json!("sql"),
        "got: {error_doc}"
    );
    assert!(
        error_doc["error"]["message"]
            .as_str()
            .is_some_and(|m| m.to_lowercase().contains("constraint")),
        "error.message must mention the constraint violation, got: {error_doc}"
    );

    // (b) STDOUT is PURE JSON: every non-empty line parses as JSON, and no
    // "Error:" text appears anywhere. A stray non-JSON line fails the test.
    assert!(
        !stdout.contains("Error:"),
        "no error text may reach stdout under --json. stdout:\n{stdout}"
    );
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        serde_json::from_str::<serde_json::Value>(line.trim()).unwrap_or_else(|e| {
            panic!(
                "every stdout line must be JSON, but {line:?} failed: {e}\nfull stdout:\n{stdout}"
            )
        });
    }

    // The surviving statements still produced their JSON (the first insert and
    // the final one-row select).
    let arr = last_result_rows(&stdout)
        .unwrap_or_else(|| panic!("no result document on stdout. stdout:\n{stdout}"));
    assert_eq!(arr.len(), 1, "only the first row committed");
    assert_eq!(arr[0]["name"], serde_json::json!("a"));
}

#[test]
fn a_small_result_prints_its_rows_and_nothing_else() {
    let (ok, stdout, stderr) = run_cli(&[], &insert_n_then_select(3));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    assert_eq!(data_row_count(&stdout), 3);
    assert!(
        !stdout.contains("showing"),
        "no truncation footer exists to print. stdout:\n{stdout}"
    );
}
