//! The CLI must run real multi-line SQL statements, exactly like psql or
//! sqlite3 — a statement is terminated by a `;` that is NOT inside a quoted
//! string, no matter how many lines it spans. `docs/getting-started.md:137`
//! ships a real worked example (`contextdb-cli :memory: <<'SQL' ... SQL`)
//! with a multi-line `CREATE TABLE`, so this documented usage must work.
//!
//! These tests drive the real `contextdb-cli` binary over stdin (piped /
//! scripted mode, matching how `<<'SQL' ... SQL` and `< schema.sql` invoke
//! it), following the CLI crate's existing black-box test convention (see
//! `tests/json_and_row_cap.rs`).
//!
//! The cases below are chosen so that "standard `;` termination" is the only
//! algorithm that can pass all of them: two/three statements on ONE
//! physical line (kills any line-oriented accumulator); a multi-line
//! single-quoted string whose FIRST physical line ends in an embedded `;`
//! (kills a `trim_end().ends_with(';')` shortcut specifically); escaped
//! quotes (`''`) adjacent to semicolons; a `;` inside a double-quoted
//! identifier; and a `;` inside both `--` and `/* */` comments.

use std::io::Write;
use std::process::{Command, Stdio};

/// Run the CLI over `:memory:` with the given extra args, feeding `stdin_sql`
/// on stdin (scripted, non-interactive — stdin is piped, not a terminal).
/// Returns (success, stdout, stderr).
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

/// The last stdout line that parses as JSON of the requested shape.
/// The rows of the last result document on stdout. A successful ordinary
/// SELECT is one namespaced, column-carrying document — never a bare array.
fn last_result_rows(stdout: &str) -> Option<Vec<serde_json::Value>> {
    stdout
        .lines()
        .rev()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .find_map(|document| document.get("result")?.get("rows")?.as_array().cloned())
}

// ============================================================================
// (a) A piped multi-line CREATE TABLE + INSERT + SELECT round-trip succeeds.
// ============================================================================
#[test]
fn piped_multiline_create_insert_select_round_trips() {
    // Mirrors the shape of the docs/getting-started.md:137 worked example:
    // a CREATE TABLE whose column list spans several lines, terminated by a
    // `;` on its own closing line, followed by a multi-line INSERT.
    let sql = "\
CREATE TABLE t (
  id UUID PRIMARY KEY,
  name TEXT
);
INSERT INTO t (id, name) VALUES
  ('11111111-1111-1111-1111-111111111111', 'alice');
SELECT id, name FROM t;
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a real multi-line schema file (docs/getting-started.md:137 shape) must run end to end. stderr:\n{stderr}\nstdout:\n{stdout}"
    );

    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(
        arr.len(),
        1,
        "one row expected from the multi-line INSERT, got stdout:\n{stdout}"
    );
    assert_eq!(
        arr[0]["id"],
        serde_json::json!("11111111-1111-1111-1111-111111111111")
    );
    assert_eq!(arr[0]["name"], serde_json::json!("alice"));
}

// ============================================================================
// (b) A `;` inside a quoted string must never be treated as a statement
// terminator — neither on a single line nor when the statement itself spans
// multiple lines (the multi-line scan must track quote state, not just
// search for the first `;` byte).
// ============================================================================
#[test]
fn semicolon_inside_quoted_string_on_one_line_is_not_a_statement_separator() {
    let sql = "\
CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT);
INSERT INTO notes (id, body) VALUES ('22222222-2222-2222-2222-222222222222', 'a;b;c');
SELECT body FROM notes;
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a `;` inside a quoted string must not split the statement. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(
        arr[0]["body"],
        serde_json::json!("a;b;c"),
        "the quoted semicolons must survive verbatim, got stdout:\n{stdout}"
    );
}

#[test]
fn quoted_semicolon_survives_when_the_statement_itself_spans_multiple_lines() {
    // The INSERT's own `;` terminator is on a later line than the opening
    // `(`, AND the string value contains embedded semicolons. A naive
    // multi-line accumulator that scans for the first `;` byte (ignoring
    // quote state) would terminate the statement early, inside the string.
    let sql = "\
CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT);
INSERT INTO notes (id, body)
VALUES (
  '33333333-3333-3333-3333-333333333333',
  'semi;colon;inside'
);
SELECT body FROM notes WHERE id = '33333333-3333-3333-3333-333333333333';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a multi-line statement with an embedded quoted `;` must run end to end. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(
        arr[0]["body"],
        serde_json::json!("semi;colon;inside"),
        "the embedded quoted semicolons must survive verbatim across the multi-line scan, got stdout:\n{stdout}"
    );
}

// ============================================================================
// (c) Meta-commands (dot-commands) remain single-line: a `.tables`-style
// command must keep executing on its own line, with no trailing `;` and
// without being folded into the new multi-line SQL accumulation. This must
// hold even right after a multi-line SQL statement.
// ============================================================================
#[test]
fn meta_command_after_a_multiline_statement_still_executes_single_line() {
    let sql = "\
CREATE TABLE alpha (
  id UUID PRIMARY KEY
);
.tables
";
    let (ok, stdout, stderr) = run_cli(&[], sql);
    assert!(
        ok,
        "the multi-line CREATE TABLE plus a following meta-command must both succeed. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(
        stdout.lines().any(|l| l.trim() == "alpha"),
        "`.tables` must list the table created by the multi-line CREATE TABLE, and must run as its own single-line command (no trailing `;`). stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

// ============================================================================
// (d) Two or three statements on ONE physical line must all run. Standard
// `;` framing does not care about line breaks at all; a line-oriented
// accumulator (append lines until the buffer ends in `;`) would treat this
// whole line as a single malformed statement instead of three.
// ============================================================================
#[test]
fn multiple_statements_on_a_single_physical_line_all_run() {
    let sql = "CREATE TABLE same_line (id UUID PRIMARY KEY, v TEXT); INSERT INTO same_line (id, v) VALUES ('44444444-4444-4444-4444-444444444444', 'same-line'); SELECT v FROM same_line WHERE id = '44444444-4444-4444-4444-444444444444';\n";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "three `;`-separated statements on one physical line must all run. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(arr[0]["v"], serde_json::json!("same-line"));
}

// ============================================================================
// (e) A multi-line single-quoted string whose FIRST physical line ends with
// an embedded `;` (still inside the open quote), with the closing quote and
// the real statement terminator on a LATER line. This specifically kills a
// `trim_end().ends_with(';')`-style cheat: that first line, taken alone,
// really does end in `;`, even though the string (and the statement) is not
// closed yet.
// ============================================================================
#[test]
fn multiline_string_ending_its_first_physical_line_in_an_embedded_semicolon_is_not_mistaken_for_the_terminator()
 {
    let sql = "\
CREATE TABLE notes2 (id UUID PRIMARY KEY, body TEXT);
INSERT INTO notes2 (id, body) VALUES ('55555555-5555-5555-5555-555555555555', 'first line;
second line');
SELECT body FROM notes2 WHERE id = '55555555-5555-5555-5555-555555555555';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a string whose first physical line ends in `;` must not be treated as a complete statement. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(
        arr[0]["body"],
        serde_json::json!("first line;\nsecond line"),
        "the embedded newline and semicolon must survive verbatim, got stdout:\n{stdout}"
    );
}

// ============================================================================
// (f) Escaped quotes (`''`) adjacent to semicolons.
// ============================================================================
#[test]
fn escaped_quote_immediately_before_the_statement_terminator_is_handled() {
    // 'ends in quote''' -> open quote, "ends in quote", an escaped-quote pair
    // (a literal `'`), then the real closing quote, then `;`.
    let sql = "\
CREATE TABLE notes3 (id UUID PRIMARY KEY, body TEXT);
INSERT INTO notes3 (id, body) VALUES ('66666666-6666-6666-6666-666666666666', 'ends in quote''');
SELECT body FROM notes3 WHERE id = '66666666-6666-6666-6666-666666666666';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "an escaped quote immediately before the terminator must not confuse the scan. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(arr[0]["body"], serde_json::json!("ends in quote'"));
}

#[test]
fn escaped_quote_pair_next_to_an_embedded_semicolon_inside_a_string() {
    let sql = "\
CREATE TABLE notes4 (id UUID PRIMARY KEY, body TEXT);
INSERT INTO notes4 (id, body) VALUES ('77777777-7777-7777-7777-777777777777', 'it''s a semi;test');
SELECT body FROM notes4 WHERE id = '77777777-7777-7777-7777-777777777777';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "an escaped quote next to an embedded `;` must not confuse the scan. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(arr[0]["body"], serde_json::json!("it's a semi;test"));
}

// ============================================================================
// (g) A `;` inside a double-quoted identifier is another quoted region and
// must not terminate the statement.
// ============================================================================
#[test]
fn semicolon_inside_a_double_quoted_identifier_is_not_a_statement_separator() {
    let sql = "\
CREATE TABLE quoted_col (id UUID PRIMARY KEY, \"a;b\" TEXT);
INSERT INTO quoted_col (id, \"a;b\") VALUES ('88888888-8888-8888-8888-888888888888', 'value1');
SELECT \"a;b\" FROM quoted_col WHERE id = '88888888-8888-8888-8888-888888888888';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a `;` inside a double-quoted identifier must not split the statement. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(arr[0]["a;b"], serde_json::json!("value1"));
}

// ============================================================================
// (h) A `;` inside a `--` line comment or a `/* */` block comment must not
// terminate the statement, even when the comment falls on an earlier
// physical line than the real terminator.
// ============================================================================
#[test]
fn semicolon_inside_a_line_comment_does_not_terminate_the_statement() {
    let sql = "\
CREATE TABLE t2 (
  id UUID PRIMARY KEY, -- keep this; column stable
  v TEXT
);
INSERT INTO t2 (id, v) VALUES ('99999999-9999-9999-9999-999999999999', 'ok');
SELECT v FROM t2 WHERE id = '99999999-9999-9999-9999-999999999999';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a `;` inside a `--` comment on an earlier line must not end the statement. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(arr[0]["v"], serde_json::json!("ok"));
}

#[test]
fn semicolon_inside_a_block_comment_does_not_terminate_the_statement() {
    let sql = "\
CREATE TABLE t3 (
  id UUID PRIMARY KEY,
  /* description column; keep nullable */
  v TEXT
);
INSERT INTO t3 (id, v) VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'ok2');
SELECT v FROM t3 WHERE id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
";
    let (ok, stdout, stderr) = run_cli(&["--json"], sql);
    assert!(
        ok,
        "a `;` inside a `/* */` comment on an earlier line must not end the statement. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    let arr = last_result_rows(&stdout).unwrap_or_else(|| {
        panic!("no result document on stdout. stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(arr.len(), 1, "stdout:\n{stdout}");
    assert_eq!(arr[0]["v"], serde_json::json!("ok2"));
}
