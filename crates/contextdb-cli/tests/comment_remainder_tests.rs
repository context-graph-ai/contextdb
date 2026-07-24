//! Comment-only trivia left over after a statement's real terminator must
//! never make the CLI think a statement is still open.
//!
//! `take_terminated()` (`crates/contextdb-cli/src/repl.rs`) splits a
//! `;`-terminated statement off the front of the gathered buffer and leaves
//! whatever followed the `;` on the same physical line for the next round —
//! by design, so `SELECT 1; SELECT 2;` on one line works. When what's
//! left is PURE COMMENT TRIVIA (`-- ...` or a closed `/* ... */`) and nothing
//! else, `StatementBuffer::is_empty()` must report `true`, even though the
//! comment text itself is non-whitespace — `statement_terminator` has
//! already scanned straight through it and found no further `;`. A buffer
//! that stayed "open" in that case would have two observable consequences,
//! both of which these tests rule out:
//!
//! (a) In scripted/piped mode, the end-of-input tail flush
//!     (`take_remaining`) would treat the leftover comment as a final
//!     statement and run it as SQL — a bare comment is not a valid
//!     statement, so that would be a spurious fatal `ParseError` and the
//!     whole run would exit non-zero, even though every real statement in
//!     the script succeeded.
//! (b) Any meta-command immediately following (`.tables`, `\dt`, ...) would
//!     be routed through `route_line` with `statement_open: true` (because
//!     the buffer still isn't empty), so it would be swallowed into the
//!     "open statement" as more SQL text instead of executing as a command
//!     — same spurious `ParseError`, and the command would never run.
//!
//! Contract: comment-only trivia after a statement's real terminator is NOT
//! an open statement. A script ending in `SELECT 1; -- done` must exit 0,
//! and a meta-command right after such a trailing comment must actually run.
//!
//! The deliberate exception, also pinned here: an UNCLOSED `/* ...` is not
//! trivia — the distinction is CLOSED-vs-OPEN, not comment-vs-not-comment.
//! A following line genuinely continues an unterminated block comment, so
//! the statement stays open, a meta-command right after it is comment text
//! (not a command), and the unterminated remainder reaching the parser at
//! end-of-input must fail loudly (non-zero exit), never be silently
//! discarded.
//!
//! These drive the real `contextdb-cli` binary over stdin (piped/scripted
//! mode), following the existing convention in `tests/multiline_statement_tests.rs`.

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

// ============================================================================
// (a) A trailing comment after the last statement's terminator, with nothing
// else following, must not be treated as an open (and then spuriously
// executed) statement — the run must exit 0.
// ============================================================================

#[test]
fn trailing_line_comment_at_eof_exits_cleanly() {
    let (ok, stdout, stderr) = run_cli(&[], "SELECT 1; -- comment\n");
    assert!(
        ok,
        "a `--` comment left over after the real terminator, with nothing else \
         following, must not fail the run. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(
        !stderr.contains("Error:"),
        "no spurious error may be printed for pure trailing trivia. stderr:\n{stderr}"
    );
}

#[test]
fn trailing_closed_block_comment_at_eof_exits_cleanly() {
    let (ok, stdout, stderr) = run_cli(&[], "SELECT 1; /* comment */\n");
    assert!(
        ok,
        "a closed `/* */` comment left over after the real terminator, with \
         nothing else following, must not fail the run. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(
        !stderr.contains("Error:"),
        "no spurious error may be printed for pure trailing trivia. stderr:\n{stderr}"
    );
}

// ============================================================================
// (b) A meta-command immediately after a trailing comment must actually run
// as a meta-command, not be swallowed into the (wrongly still-open)
// statement buffer as more SQL text.
// ============================================================================

#[test]
fn meta_command_after_a_trailing_line_comment_still_runs() {
    let sql = "CREATE TABLE after_line_comment (id UUID PRIMARY KEY); -- note\n.tables\n";
    let (ok, stdout, stderr) = run_cli(&[], sql);
    assert!(
        ok,
        "the CREATE TABLE plus the following `.tables` must both succeed. \
         stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(
        stdout.lines().any(|l| l.trim() == "after_line_comment"),
        "`.tables` must actually run and list the table created just before \
         the trailing `--` comment, not be swallowed into pending SQL as text. \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn meta_command_after_a_trailing_block_comment_still_runs() {
    let sql = "CREATE TABLE after_block_comment (id UUID PRIMARY KEY); /* note */\n.tables\n";
    let (ok, stdout, stderr) = run_cli(&[], sql);
    assert!(
        ok,
        "the CREATE TABLE plus the following `.tables` must both succeed. \
         stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(
        stdout.lines().any(|l| l.trim() == "after_block_comment"),
        "`.tables` must actually run and list the table created just before \
         the trailing `/* */` comment, not be swallowed into pending SQL as \
         text. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

// ============================================================================
// The deliberate exception: an UNCLOSED `/* ...` is not trivia — the next
// physical line genuinely continues it, so the statement must stay open, a
// following meta-command must be treated as comment text (not a command),
// and the unterminated remainder reaching the parser at end-of-input must
// be a loud, non-zero failure. This is a regression lock on the
// CLOSED-vs-OPEN distinction the contract above depends on — not
// comment-vs-not-comment — so a change to `is_empty()`/`is_only_trivia`
// cannot quietly widen into silently discarding unterminated input the user
// actually wrote.
// ============================================================================

#[test]
fn unterminated_block_comment_keeps_the_statement_open_and_errors_loudly_at_eof() {
    let sql = "CREATE TABLE marker (id UUID PRIMARY KEY); /* unterminated\n.tables\n";
    let (ok, stdout, stderr) = run_cli(&[], sql);
    assert!(
        !ok,
        "an unterminated block comment must fail the run loudly at EOF, not \
         silently discard the dangling input. stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(
        stderr.contains("Error:"),
        "the unterminated remainder reaching the parser must produce a \
         reported error, not a silent exit. stderr:\n{stderr}"
    );
    assert!(
        !stdout.lines().any(|l| l.trim() == "marker"),
        "`.tables` must NOT have run as a meta-command here — it is comment \
         text inside the still-open statement, so the table it would have \
         listed must not appear. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}
