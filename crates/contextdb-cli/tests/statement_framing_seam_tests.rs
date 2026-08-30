//! Interactive-mode coverage of the shared statement framing component,
//! WITHOUT a PTY dependency.
//!
//! `run_interactive` and `run_scripted` (`crates/contextdb-cli/src/repl.rs`)
//! both drive the exact same per-line path — `feed_line`, built on
//! `StatementBuffer` + `route_line` + `statement_terminator` — so asserting
//! that path directly pins the multi-line contract for BOTH adapters without
//! spawning a terminal. `contextdb_cli::testing` exposes the framing
//! primitives read-only, alongside `feed_line` itself plus
//! `InputContext`/`SessionState`/`session_exit_code`, so a parity test can
//! drive the real per-line path rather than a test-side reimplementation of
//! it. This is a test-only visibility surface with no production behavior
//! change.
//!
//! What each test pins:
//! - `StatementBuffer::is_empty()` IS the continuation state: `false` means a
//!   statement is open. `run_interactive` shows the `...>` prompt while it is
//!   `false`, and checks it again at loop exit to decide whether to warn
//!   about a discarded incomplete statement. The print itself is interactive
//!   UI and is intentionally NOT tested here — only the component-level
//!   signal it reads.
//! - `route_line` is where "a meta-command is only a meta-command when no
//!   statement is open" is decided, including the asymmetry: once a
//!   statement is open, EVERY line — even one that looks like a
//!   `.meta-command` or a `--` comment — routes as `Sql`, because it is that
//!   statement's text now, not a command and not something to drop.
//! - `statement_terminator` is the raw quote/comment-aware `;` scan; it must
//!   always return a valid `char` boundary, including with multi-byte
//!   content.
//! - One parity test drives the SAME input through the REAL `feed_line`
//!   twice — once shaped like `run_scripted` (a line number per line) and
//!   once shaped like `run_interactive` (no line numbers) — against two
//!   separate in-memory databases, then asserts the two databases end up in
//!   an IDENTICAL state. Since `feed_line` is the actual production function
//!   both adapters call, this is not a reimplementation of the framing
//!   contract; it drives it.
//!
//! Correction: this file's `drive` helper used to ALSO replicate
//! `run_interactive`'s own line preprocessing (trim every line, skip it
//! before `feed_line` is even called if the trimmed line is empty) inside
//! its "interactive" branch, on the theory that this made the parity test
//! more "realistic." That was wrong: it silently baked a REAL bug (that
//! preprocessing corrupts a multi-line quoted value's leading whitespace and
//! drops its interior blank lines) into what was supposed to be a clean
//! statement about `feed_line`'s own adapter-agnostic contract — the parity
//! test only kept passing because its input happened to contain no leading
//! whitespace or blank lines to lose. `drive` now feeds RAW lines on both
//! branches, differing only in what `feed_line` itself documents as the real
//! difference (`interactive`/`script_line`). The preprocessing bug is
//! exercised separately, explicitly, by
//! `interactive_readline_preprocessing_corrupts_a_multiline_quoted_value`
//! below.
//!
//! `interactive_readline_filter(line: &str, statement_open: bool) ->
//! Option<&str>` (`repl.rs`) is the exact per-line decision
//! `run_interactive` makes: `None` drops the line (only when no
//! statement is open), `Some` is the RAW text to feed, never trimmed. The
//! test below calls that real function directly instead of reproducing it
//! inline, so it exercises the actual adapter decision.

use contextdb_cli::formatter::format_query_result_json;
use contextdb_cli::testing::{
    InputContext, LineRouting, OutputOptions, Session, SessionState, StatementBuffer, feed_line,
    interactive_readline_filter, route_line, session_exit_code, statement_terminator,
};
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::Database;
use std::collections::HashMap;
use std::sync::Arc;

/// A writing session over a throwaway in-memory database — the mode
/// `:memory:` always has, and the one these framing fixtures need.
fn writing_session(database: &Arc<Database>) -> Session {
    Session::writing(Arc::clone(database), ReadLimits::default())
        .expect("a live database opens its own bounded read view")
}

// ============================================================================
// Continuation state across fed lines.
// ============================================================================

#[test]
fn statement_buffer_tracks_continuation_state_across_fed_lines() {
    let mut buf = StatementBuffer::default();
    assert!(buf.is_empty(), "a fresh buffer has no open statement");

    buf.push_line("CREATE TABLE t (", Some(1));
    assert!(
        !buf.is_empty(),
        "a statement is now open — this is what drives the `...>` continuation prompt"
    );
    assert!(
        buf.take_terminated().is_none(),
        "no `;` yet, nothing to take"
    );

    buf.push_line("  id UUID PRIMARY KEY", Some(2));
    assert!(!buf.is_empty());
    assert!(buf.take_terminated().is_none());

    buf.push_line(");", Some(3));
    let stmt = buf
        .take_terminated()
        .expect("the `;` on line 3 closes the statement");
    assert_eq!(
        stmt.text.trim(),
        "CREATE TABLE t (\n  id UUID PRIMARY KEY\n);"
    );
    assert_eq!(
        stmt.first_line,
        Some(1),
        "the statement is attributed to the line it STARTED on, not where it closed"
    );
    assert!(
        buf.is_empty(),
        "closing the statement clears the open/continuation state"
    );
}

#[test]
fn take_terminated_leaves_the_remainder_of_the_line_for_the_next_call() {
    let mut buf = StatementBuffer::default();
    buf.push_line("CREATE TABLE t (id UUID PRIMARY KEY); SELECT 1;", Some(1));

    let first = buf.take_terminated().expect("first statement");
    assert_eq!(first.text.trim(), "CREATE TABLE t (id UUID PRIMARY KEY);");

    let second = buf
        .take_terminated()
        .expect("second statement, same physical line");
    assert_eq!(second.text.trim(), "SELECT 1;");

    assert!(buf.is_empty());
    assert!(buf.take_terminated().is_none());
}

#[test]
fn take_remaining_flushes_an_unterminated_statement_at_end_of_input() {
    let mut buf = StatementBuffer::default();
    buf.push_line("SELECT 1", Some(1));
    assert!(!buf.is_empty());

    let stmt = buf
        .take_remaining()
        .expect("end-of-input flush must return the gathered text even with no `;`");
    assert_eq!(stmt.text.trim(), "SELECT 1");
    assert!(buf.is_empty());
    assert!(
        buf.take_remaining().is_none(),
        "a second flush with nothing gathered must be None"
    );
}

// ============================================================================
// Meta-command dispatch only when no statement is open.
// ============================================================================

#[test]
fn route_line_treats_meta_commands_as_meta_only_when_no_statement_is_open() {
    // No open statement: a leading `.`/`\` is a meta-command, a whole-line
    // `--` comment and a blank line are skipped (not input), ordinary SQL
    // text starts a statement.
    assert_eq!(route_line(".tables", false), LineRouting::Meta);
    assert_eq!(route_line("\\dt", false), LineRouting::Meta);
    assert_eq!(route_line("-- just a comment", false), LineRouting::Skip);
    assert_eq!(route_line("", false), LineRouting::Skip);
    assert_eq!(route_line("   ", false), LineRouting::Skip);
    assert_eq!(route_line("SELECT 1", false), LineRouting::Sql);

    // A statement is ALREADY open: every line continues it, even one that
    // would otherwise look like a meta-command, a comment, or be blank — a
    // `.tables`-shaped or `--`-shaped line inside an unfinished statement is
    // that statement's TEXT, not a command and not a dropped comment.
    assert_eq!(route_line(".tables", true), LineRouting::Sql);
    assert_eq!(route_line("\\dt", true), LineRouting::Sql);
    assert_eq!(route_line("-- just a comment", true), LineRouting::Sql);
    assert_eq!(route_line("", true), LineRouting::Sql);
    assert_eq!(route_line("SELECT 1", true), LineRouting::Sql);
}

// ============================================================================
// Unfinished-statement-at-exit detection — the component-level state
// `run_interactive` checks after its readline loop exits. The warning PRINT
// itself is interactive-only UI and is deliberately not exercised here.
// ============================================================================

#[test]
fn statement_buffer_signals_an_unfinished_statement_at_exit() {
    let mut buf = StatementBuffer::default();
    buf.push_line("SELECT 1", Some(1));
    // No `;` was ever fed. This is exactly the state `run_interactive` reads
    // at loop exit to decide whether to warn
    // ("Discarding incomplete statement (no closing `;`).").
    assert!(
        !buf.is_empty(),
        "an unterminated statement must still read as open at end of input"
    );
}

#[test]
fn statement_buffer_signals_nothing_open_when_everything_was_terminated() {
    let mut buf = StatementBuffer::default();
    buf.push_line("SELECT 1;", Some(1));
    buf.take_terminated();
    assert!(
        buf.is_empty(),
        "a fully terminated session has nothing left open — no exit warning is warranted"
    );
}

// ============================================================================
// The raw `;` scan must always return a valid char boundary, including with
// multi-byte content ahead of the terminator.
// ============================================================================

#[test]
fn statement_terminator_returns_a_char_boundary_with_multibyte_content() {
    let text = "SELECT '\u{1F600}' FROM t; -- trailing\n";
    let end = statement_terminator(text).expect("a terminator must be found");
    // `str::split_at` panics on a non-char-boundary index; reaching the
    // assertions below already proves `end` is a valid boundary.
    let (before, after) = text.split_at(end);
    assert_eq!(before, "SELECT '\u{1F600}' FROM t;");
    assert_eq!(after, " -- trailing\n");
}

#[test]
fn statement_terminator_ignores_semicolons_in_every_quoted_or_commented_region() {
    let text = "'a;b' \"c;d\" -- e;f\n /* g;h */ ;";
    let end = statement_terminator(text).expect("the only real terminator is the trailing `;`");
    assert_eq!(
        end,
        text.len(),
        "must land on the final `;`, not any earlier one"
    );
}

// ============================================================================
// Root cause: comment-only trivia left in the buffer after
// `take_terminated` splits off a statement must not read as an open
// statement. `is_empty()` is `self.text.trim().is_empty()`, and a comment's
// text is not whitespace, so it used to report `false` — the component-level
// bug behind both black-box symptoms in `tests/comment_remainder_tests.rs`.
// ============================================================================

#[test]
fn buffer_is_empty_after_a_terminated_statement_leaves_only_a_trailing_comment() {
    let mut buf = StatementBuffer::default();
    buf.push_line("SELECT 1; -- trailing", Some(1));
    let stmt = buf
        .take_terminated()
        .expect("the `;` closes the SELECT immediately");
    assert_eq!(stmt.text.trim(), "SELECT 1;");

    assert!(
        buf.is_empty(),
        "pure comment trivia left over after the real terminator must not \
         read as an open statement — a false `is_empty()` here would make \
         the end-of-input flush re-run the comment as SQL and make a \
         following meta-command route as more SQL text instead of a \
         command"
    );
}

// ============================================================================
// Parity: driving the REAL `feed_line` scripted-style and interactive-style
// over the SAME input, against two separate in-memory databases, must leave
// both databases in an IDENTICAL final state.
// ============================================================================

/// Drive `lines` through the real `feed_line`, shaped like one adapter or the
/// other. Both branches feed the RAW line unchanged — the only difference is
/// what `feed_line` itself documents as adapter-specific: `interactive` and
/// whether a 1-based `script_line` is attached. This is deliberately NOT a
/// faithful reproduction of `run_interactive`'s own line preprocessing (see
/// the module doc comment) — that preprocessing is a separate, currently
/// buggy step ABOVE `feed_line`, not part of `feed_line`'s own contract.
/// Returns the resulting database and the session exit code.
fn drive(lines: &[&str], interactive: bool) -> (Arc<Database>, i32) {
    let db = Arc::new(Database::open_memory());
    let session_handle = writing_session(&db);
    let mut session = SessionState::default();
    let mut pending = StatementBuffer::default();

    for (idx, line) in lines.iter().enumerate() {
        let input = InputContext {
            interactive,
            script_line: if interactive { None } else { Some(idx + 1) },
            output: OutputOptions::default(),
            // These fixtures frame statements against an in-memory database,
            // which is writable with no flag at all.
            store_writes_permitted: true,
        };
        let keep_going = feed_line(
            &session_handle,
            None,
            None,
            line,
            input,
            None,
            &mut session,
            &mut pending,
        );
        assert!(keep_going, "no `.quit`/`.exit` in this driven script");
    }

    (db, session_exit_code(&session))
}

#[test]
fn scripted_and_interactive_drives_of_feed_line_leave_identical_database_state() {
    // Every statement here is properly `;`-terminated, so neither adapter's
    // own end-of-input tail-flush policy (which `feed_line` alone does not
    // perform — both real adapters do it themselves after their loop) comes
    // into play; this isolates the test to `feed_line` itself. Includes a
    // multi-line statement, a same-line pair, a comment, and a meta-command,
    // so a regression in any one of them would show up as a state mismatch.
    let lines = [
        "CREATE TABLE t (",
        "  id UUID PRIMARY KEY,",
        "  v TEXT",
        ");",
        "-- a comment between statements",
        "INSERT INTO t (id, v) VALUES ('11111111-1111-1111-1111-111111111111', 'a;b');",
        ".tables",
        "INSERT INTO t (id, v) VALUES ('22222222-2222-2222-2222-222222222222', 'c'); INSERT INTO t (id, v) VALUES ('33333333-3333-3333-3333-333333333333', 'd');",
    ];

    let (scripted_db, scripted_exit) = drive(&lines, false);
    let (interactive_db, interactive_exit) = drive(&lines, true);

    assert_eq!(scripted_exit, 0, "the scripted drive must not error");
    assert_eq!(interactive_exit, 0, "the interactive drive must not error");

    let query = "SELECT id, v FROM t ORDER BY id";
    let scripted_result = scripted_db
        .execute(query, &HashMap::new())
        .expect("scripted drive: final query");
    let interactive_result = interactive_db
        .execute(query, &HashMap::new())
        .expect("interactive drive: final query");

    assert_eq!(
        scripted_result.rows.len(),
        3,
        "all three INSERTs (one multi-line, two on one physical line) must have run"
    );
    assert_eq!(
        format_query_result_json(&scripted_result),
        format_query_result_json(&interactive_result),
        "the two adapter shapes must leave the database in an identical final state"
    );
}

// ============================================================================
// `run_interactive` must never trim every line and drop a
// line entirely (before `feed_line` is even called) when the trimmed line
// is empty. Inside an OPEN multi-line quoted string, both would be
// corruption: a continuation line's leading whitespace can be part of the
// stored value, and a blank line inside the quotes must contribute an empty
// line to it, not vanish. This drives the REAL per-line decision through
// `interactive_readline_filter` (`repl.rs`, exposed via
// `contextdb_cli::testing`) — not a reproduction of it.
// ============================================================================

/// Drives `lines` through the REAL interactive per-line path:
/// `interactive_readline_filter` decides whether to drop each line (and, if
/// not, hands back the RAW text — never trimmed), then that text goes
/// straight to `feed_line`. This is the actual production decision
/// `run_interactive` makes, not a test-side reimplementation of it.
fn drive_interactive_via_seam(lines: &[&str]) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    let session_handle = writing_session(&db);
    let mut session = SessionState::default();
    let mut pending = StatementBuffer::default();

    for raw_line in lines {
        let Some(fed) = interactive_readline_filter(raw_line, !pending.is_empty()) else {
            continue;
        };
        let input = InputContext {
            interactive: true,
            script_line: None,
            output: OutputOptions::default(),
            store_writes_permitted: true,
        };
        let keep_going = feed_line(
            &session_handle,
            None,
            None,
            fed,
            input,
            None,
            &mut session,
            &mut pending,
        );
        assert!(keep_going, "no `.quit`/`.exit` in this driven script");
    }

    db
}

#[test]
fn interactive_readline_preprocessing_corrupts_a_multiline_quoted_value() {
    // A user pasting/typing a multi-line value at the `contextdb>` /
    // `      ...>` prompt: the continuation line has meaningful leading
    // spaces (indentation the user intends to be part of the stored text),
    // and there is a blank line in the middle of the quoted value.
    let lines = [
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT);",
        "INSERT INTO notes (id, body) VALUES ('44444444-4444-4444-4444-444444444444', 'first",
        "   indented second line",
        "",
        "end');",
    ];

    let db = drive_interactive_via_seam(&lines);
    let result = db
        .execute(
            "SELECT body FROM notes WHERE id = '44444444-4444-4444-4444-444444444444'",
            &HashMap::new(),
        )
        .expect("query the stored value");
    assert_eq!(result.rows.len(), 1, "the INSERT must have run");

    let stored = match &result.rows[0][0] {
        contextdb_core::Value::Text(t) => t.clone(),
        other => panic!("expected a text value, got {other:?}"),
    };

    // What a correct interactive adapter must store: every line's content
    // verbatim (leading whitespace intact), joined by the newlines the user
    // actually typed, INCLUDING the blank line's own newline.
    assert_eq!(
        stored, "first\n   indented second line\n\nend",
        "leading whitespace on a continuation line and a blank line inside an \
         open quoted value must survive interactive input byte-for-byte; got \
         {stored:?}"
    );
}
