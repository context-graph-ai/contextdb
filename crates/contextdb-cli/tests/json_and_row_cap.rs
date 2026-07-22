//! Machine-readable output (`--json`) and bounded default output (`--all`) for
//! the CLI (usability jobs USR-8 / AGT-8 / AGT-15).
//!
//! An agent or script needs query results as structured JSON, and the default
//! human table must be bounded (a `SELECT *` on a large table dumped hundreds of
//! rows with no cap or narrowing hint). These drive the binary end-to-end so the
//! flags are proven through clap, the REPL, and the formatter together.

use std::io::Write;
use std::process::{Command, Stdio};

/// Run the CLI over `:memory:` with the given extra args, feeding `stdin_sql` on
/// stdin (scripted, non-interactive). Returns (success, stdout, stderr).
fn run_cli(extra_args: &[&str], stdin_sql: &str) -> (bool, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb-cli"));
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

/// The last stdout line that parses as JSON of the requested shape.
fn last_json(stdout: &str) -> Option<serde_json::Value> {
    stdout
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
}

#[test]
fn json_select_emits_array_of_objects() {
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)\n\
               INSERT INTO t (id, name) VALUES (1, 'alice')\n\
               INSERT INTO t (id, name) VALUES (2, 'bob')\n\
               SELECT id, name FROM t ORDER BY id\n";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(ok, "cli must exit 0. stderr:\n{stderr}\nstdout:\n{stdout}");

    let arr = stdout
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_else(|| panic!("no JSON array on stdout. stdout:\n{stdout}"));

    assert_eq!(arr.len(), 2, "two rows expected, got:\n{stdout}");
    assert_eq!(arr[0]["id"], serde_json::json!(1));
    assert_eq!(arr[0]["name"], serde_json::json!("alice"));
    assert_eq!(arr[1]["id"], serde_json::json!(2));
    assert_eq!(arr[1]["name"], serde_json::json!("bob"));
}

/// Build SQL that creates a table and inserts `n` rows, then a `SELECT *`.
fn insert_n_then_select(n: usize) -> String {
    let mut s = String::from("CREATE TABLE big (id INTEGER PRIMARY KEY, name TEXT)\n");
    for i in 0..n {
        s.push_str(&format!(
            "INSERT INTO big (id, name) VALUES ({i}, 'row{i}')\n"
        ));
    }
    s.push_str("SELECT * FROM big ORDER BY id\n");
    s
}

/// Count the table's DATA rows in ASCII output: lines starting with '|' are the
/// header plus each data row; subtract the single header row.
fn data_row_count(stdout: &str) -> usize {
    let bar_lines = stdout.lines().filter(|l| l.starts_with('|')).count();
    bar_lines.saturating_sub(1)
}

#[test]
fn default_output_caps_at_100_rows_with_footer() {
    let (ok, stdout, stderr) = run_cli(&[], &insert_n_then_select(150));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    assert_eq!(
        data_row_count(&stdout),
        100,
        "default output must cap at 100 data rows. stdout tail:\n{}",
        stdout.lines().rev().take(6).collect::<Vec<_>>().join("\n")
    );
    assert!(
        stdout.contains("showing 100 of 150 rows") && stdout.contains("--all"),
        "footer must name the total and --all. stdout:\n{stdout}"
    );
}

#[test]
fn all_flag_disables_the_cap_and_footer() {
    let (ok, stdout, stderr) = run_cli(&["--all"], &insert_n_then_select(150));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    assert_eq!(
        data_row_count(&stdout),
        150,
        "--all must print every row. stdout tail:\n{}",
        stdout.lines().rev().take(4).collect::<Vec<_>>().join("\n")
    );
    assert!(
        !stdout.contains("showing 100 of"),
        "--all must not print the cap footer. stdout:\n{stdout}"
    );
}

#[test]
fn json_output_is_uncapped() {
    let (ok, stdout, stderr) = run_cli(&["--json"], &insert_n_then_select(150));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    let arr = last_json(&stdout)
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_else(|| panic!("no JSON array on stdout. stdout:\n{stdout}"));
    assert_eq!(arr.len(), 150, "JSON output must not be capped");
}

#[test]
fn json_stdout_stays_pure_when_a_runtime_error_occurs() {
    // A UNIQUE/PRIMARY-KEY violation is a NON-fatal runtime error: the session
    // continues. Under --json its "Error:" line must go to stderr, never stdout
    // — stdout must remain pure JSON so a machine consumer can parse the stream.
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)\n\
               INSERT INTO t (id, name) VALUES (1, 'a')\n\
               INSERT INTO t (id, name) VALUES (1, 'dup')\n\
               SELECT id, name FROM t ORDER BY id\n";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "session must survive a non-fatal error. stderr:\n{stderr}"
    );

    // (a) the error text is on STDERR.
    assert!(
        stderr.contains("Error:") && stderr.to_lowercase().contains("constraint"),
        "the runtime error must be reported on stderr, got stderr:\n{stderr}"
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
    let arr = last_json(&stdout)
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_else(|| panic!("no JSON array on stdout. stdout:\n{stdout}"));
    assert_eq!(arr.len(), 1, "only the first row committed");
    assert_eq!(arr[0]["name"], serde_json::json!("a"));
}

#[test]
fn rows_under_the_cap_have_no_footer() {
    let (ok, stdout, stderr) = run_cli(&[], &insert_n_then_select(3));
    assert!(ok, "cli must exit 0. stderr:\n{stderr}");
    assert_eq!(data_row_count(&stdout), 3);
    assert!(
        !stdout.contains("showing"),
        "a result under the cap must not print a footer. stdout:\n{stdout}"
    );
}
