use crate::formatter::{DEFAULT_ROW_CAP, format_query_result_capped, format_query_result_json};
use crate::json_output::{self, ErrorClass};
use contextdb_core::{Error, TableMeta};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};
use contextdb_server::exit_codes::{EXIT_ERROR, EXIT_INTERRUPTED_PUSH_UNCONFIRMED, EXIT_OK};
use contextdb_server::{SyncClient, SyncPlugin};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::io::{BufRead, IsTerminal};
use std::sync::Arc;

/// Where a line came from: an interactive terminal or a script. The only
/// framing difference between the two is the line number a script can cite and
/// what an unfinished statement does at exit.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct InputContext {
    pub interactive: bool,
    pub script_line: Option<usize>,
    pub output: OutputOptions,
}

/// How query results are rendered. `--json` emits a JSON array of row objects
/// (uncapped); otherwise the human table is capped at [`DEFAULT_ROW_CAP`] rows
/// unless `all` (the `--all` flag) is set.
#[derive(Clone, Copy, Debug, Default)]
pub struct OutputOptions {
    pub json: bool,
    pub all: bool,
}

impl OutputOptions {
    /// Report a failure on stderr in whichever form this session speaks: the
    /// `{"error":{...}}` document under `--json`, the human `Error: ...` line
    /// otherwise.
    ///
    /// The CLI binary reports its own startup and shutdown failures through
    /// this, so a `--json` consumer parses one stderr format for the whole
    /// process, not just for the statements inside the REPL.
    pub fn report_error(&self, class: ErrorClass, message: &str) {
        if self.json {
            json_output::print_error(class, message, None);
        } else {
            eprintln!("Error: {message}");
        }
    }

    /// Report something the operator should see that is NOT a failure — a
    /// deprecation, an endpoint the CLI will keep retrying, a push whose
    /// outcome is merely unknown. Reporting these as errors would tell a
    /// consumer the run failed when it did not.
    pub fn report_notice(&self, class: ErrorClass, message: &str) {
        if self.json {
            json_output::print_notice(class, message);
        } else {
            eprintln!("{message}");
        }
    }
}

/// What a session has accumulated so far, and what its exit code will be.
#[derive(Default)]
#[doc(hidden)]
pub struct SessionState {
    pub had_error: bool,
    /// Set when a `.sync push` was interrupted after sending and the hub could
    /// not confirm the batch landed. Drives [`EXIT_INTERRUPTED_PUSH_UNCONFIRMED`]
    /// in non-interactive use; the interactive REPL prints the warning and
    /// continues.
    pub had_unconfirmed_push: bool,
    pub trace_enabled: bool,
    /// A terminal session showed the human every error as it happened, so the
    /// process code reports the SESSION, not the statements inside it — the
    /// same reason `psql` and `sqlite3` leave an interactive session at 0. A
    /// scripted run has nobody watching, so there every error fails the run.
    pub interactive: bool,
}

/// Collapse a finished session's state to a process exit code: a definitive
/// error dominates ([`EXIT_ERROR`]), then an unconfirmed interrupted push
/// ([`EXIT_INTERRUPTED_PUSH_UNCONFIRMED`]), else success ([`EXIT_OK`]).
///
/// An unconfirmed push still reports its own code from an interactive session:
/// the human cannot act on it once the process is gone, and a wrapper script
/// around an interactive invocation must still learn that the data's fate is
/// unknown.
#[doc(hidden)]
pub fn session_exit_code(session: &SessionState) -> i32 {
    if session.had_error && !session.interactive {
        EXIT_ERROR
    } else if session.had_unconfirmed_push {
        EXIT_INTERRUPTED_PUSH_UNCONFIRMED
    } else {
        EXIT_OK
    }
}

/// Run the REPL loop. Returns the process exit code: [`EXIT_OK`] if all commands
/// succeeded, [`EXIT_ERROR`] if any error occurred, and
/// [`EXIT_INTERRUPTED_PUSH_UNCONFIRMED`] if a `.sync push` was interrupted after
/// sending with its outcome unconfirmed (and no harder error occurred).
///
/// One exception: an INTERACTIVE session
/// returns [`EXIT_OK`] even after errors. The person at the terminal saw each
/// one as it happened and chose to keep going, so the exit code reports the
/// session rather than the statements inside it — the same thing `psql` and
/// `sqlite3` do. A scripted session has nobody watching, so there every error
/// is reported. An unconfirmed push is reported in both, because nobody can act
/// on it once the process is gone.
pub fn run(
    db: Arc<Database>,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        eprintln!("ContextDB v{}", env!("CARGO_PKG_VERSION"));
        eprintln!("Enter .help for usage hints.");
        return run_interactive(&db, sync_client, rt, sync_plugin, output);
    }

    run_scripted(&db, sync_client, rt, sync_plugin, output)
}

/// One whole SQL statement gathered from the input, with the input line it
/// started on so a parse error can still be placed in a script.
#[doc(hidden)]
pub struct PendingStatement {
    pub text: String,
    pub first_line: Option<usize>,
}

/// Gathers input lines into whole SQL statements, exactly as `psql` and
/// `sqlite3` do: a statement ends at the first `;` that is not inside a quoted
/// string or a comment, however many lines it spans, and a line that ends
/// without such a `;` continues on the next one. Whatever is left when the
/// input ends is run as a final statement.
///
/// The interactive and the scripted adapter both frame their input through
/// this, so it carries the whole multi-line contract for both; it is re-exported
/// through [`crate::testing`] to be asserted directly, without a terminal.
#[derive(Default)]
#[doc(hidden)]
pub struct StatementBuffer {
    text: String,
    first_line: Option<usize>,
}

impl StatementBuffer {
    /// Whether no statement is open. What a terminator leaves behind is often
    /// pure trivia — `SELECT 1; -- done` leaves ` -- done` — and trivia is not
    /// a statement waiting to be continued: nothing follows it, so the next
    /// line starts fresh and end of input has nothing to run.
    pub fn is_empty(&self) -> bool {
        is_only_trivia(&self.text)
    }

    pub fn push_line(&mut self, line: &str, line_no: Option<usize>) {
        if self.is_empty() {
            self.text.clear();
            self.first_line = line_no;
        }
        self.text.push_str(line);
        self.text.push('\n');
    }

    /// Split off the next `;`-terminated statement, leaving anything that
    /// followed the `;` on the same line for the next round.
    pub fn take_terminated(&mut self) -> Option<PendingStatement> {
        let end = statement_terminator(&self.text)?;
        let text: String = self.text.drain(..end).collect();
        let first_line = self.first_line;
        if self.is_empty() {
            self.text.clear();
            self.first_line = None;
        }
        Some(PendingStatement { text, first_line })
    }

    /// Take whatever is left, terminated or not (end of input).
    pub fn take_remaining(&mut self) -> Option<PendingStatement> {
        if self.is_empty() {
            self.text.clear();
            self.first_line = None;
            return None;
        }
        Some(PendingStatement {
            text: std::mem::take(&mut self.text),
            first_line: self.first_line.take(),
        })
    }
}

/// Whether `text` is nothing but whitespace and CLOSED comments — nothing a
/// statement could be continued from.
///
/// An UNCLOSED `/*` is deliberately NOT trivia. The next physical line genuinely
/// continues that comment, so the statement stays open: a `.tables` typed after
/// it is comment text, not a command, and at end of input the unterminated
/// remainder reaches the parser and fails loudly rather than being silently
/// discarded.
fn is_only_trivia(text: &str) -> bool {
    let mut chars = text.char_indices().peekable();

    while let Some((_, ch)) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '-' && chars.peek().is_some_and(|(_, next)| *next == '-') {
            for (_, c) in chars.by_ref() {
                if c == '\n' {
                    break;
                }
            }
            continue;
        }
        if ch == '/' && chars.peek().is_some_and(|(_, next)| *next == '*') {
            chars.next();
            let mut star = false;
            let mut closed = false;
            for (_, c) in chars.by_ref() {
                if star && c == '/' {
                    closed = true;
                    break;
                }
                star = c == '*';
            }
            if !closed {
                return false;
            }
            continue;
        }
        return false;
    }

    true
}

/// Byte offset just past the first statement-terminating `;` in `text`, or
/// `None` when there is none. A `;` inside a single-quoted string, a
/// double-quoted identifier, a `--` line comment, or a `/* */` block comment is
/// not a terminator. Scanning is by character, so multi-byte text is safe.
#[doc(hidden)]
pub fn statement_terminator(text: &str) -> Option<usize> {
    let mut chars = text.char_indices().peekable();
    let mut in_string = false;
    let mut in_identifier = false;

    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' if !in_identifier => {
                if in_string {
                    // A doubled '' is an escaped quote, not the end of the string.
                    if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                        chars.next();
                    } else {
                        in_string = false;
                    }
                } else {
                    in_string = true;
                }
            }
            '"' if !in_string => in_identifier = !in_identifier,
            '-' if !in_string
                && !in_identifier
                && chars.peek().is_some_and(|(_, next)| *next == '-') =>
            {
                for (_, c) in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '/' if !in_string
                && !in_identifier
                && chars.peek().is_some_and(|(_, next)| *next == '*') =>
            {
                chars.next();
                let mut star = false;
                for (_, c) in chars.by_ref() {
                    if star && c == '/' {
                        break;
                    }
                    star = c == '*';
                }
            }
            ';' if !in_string && !in_identifier => return Some(idx + 1),
            _ => {}
        }
    }

    None
}

/// What an input line is. Both adapters route every line through
/// [`route_line`], so this is where "a meta-command is only a meta-command when
/// no statement is open" is decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum LineRouting {
    /// A blank line or a whole-line comment between statements: not input.
    Skip,
    /// A meta-command. Stays single-line and needs no `;`.
    Meta,
    /// Text belonging to a SQL statement, gathered until a `;` closes it.
    Sql,
}

/// Route one input line, given whether a statement is already open. While a
/// statement is open every line continues it — a leading `.` or `--` inside an
/// unfinished statement is that statement's text, not a command and not a
/// comment to drop.
#[doc(hidden)]
pub fn route_line(line: &str, statement_open: bool) -> LineRouting {
    if statement_open {
        return LineRouting::Sql;
    }
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with("--") {
        return LineRouting::Skip;
    }
    if trimmed.starts_with('.') || trimmed.starts_with('\\') {
        return LineRouting::Meta;
    }
    LineRouting::Sql
}

/// The interactive adapter's ONE per-line decision, made before the line
/// reaches [`feed_line`]: `None` drops the line, `Some` is the text to feed.
///
/// The text is returned RAW. Inside an open statement a line's leading and
/// trailing spaces are part of the SQL — a continuation line of a quoted value
/// carries the user's indentation, and a blank line carries its own newline —
/// so trimming here would silently corrupt stored data. Only a blank line typed
/// when NO statement is open is dropped, because there is nothing for it to
/// belong to.
#[doc(hidden)]
pub fn interactive_readline_filter(line: &str, statement_open: bool) -> Option<&str> {
    if !statement_open && line.trim().is_empty() {
        return None;
    }
    Some(line)
}

fn run_interactive(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
    let mut session = SessionState {
        interactive: true,
        ..SessionState::default()
    };
    let mut pending = StatementBuffer::default();

    loop {
        let prompt = if pending.is_empty() {
            "contextdb> "
        } else {
            "      ...> "
        };
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                let Some(fed) = interactive_readline_filter(&line, !pending.is_empty()) else {
                    continue;
                };
                // History gets a trimmed copy for recall; the SQL text does not.
                let trimmed = fed.trim();
                if !trimmed.is_empty() {
                    let _ = rl.add_history_entry(trimmed);
                }
                if !feed_line(
                    db,
                    sync_client,
                    rt,
                    fed,
                    InputContext {
                        interactive: true,
                        script_line: None,
                        output,
                    },
                    sync_plugin,
                    &mut session,
                    &mut pending,
                ) {
                    break;
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(_) => break,
        }
    }

    if !pending.is_empty() {
        eprintln!("Discarding incomplete statement (no closing `;`).");
    }

    session_exit_code(&session)
}

fn run_scripted(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let mut session = SessionState::default();
    let mut pending = StatementBuffer::default();
    let stdin = std::io::stdin();
    let mut stopped = false;
    for (line_idx, line) in stdin.lock().lines().enumerate() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        if !feed_line(
            db,
            sync_client,
            rt,
            &line,
            InputContext {
                interactive: false,
                script_line: Some(line_idx + 1),
                output,
            },
            sync_plugin,
            &mut session,
            &mut pending,
        ) {
            stopped = true;
            break;
        }
    }

    // End of input closes the last statement even without a trailing `;`.
    if !stopped && let Some(statement) = pending.take_remaining() {
        run_statement(
            db,
            statement,
            InputContext {
                interactive: false,
                script_line: None,
                output,
            },
            &mut session,
        );
    }

    session_exit_code(&session)
}

/// Feed one input line to the session. Returns `false` when the session should
/// stop (`.quit`).
///
/// This is the whole per-line path, and BOTH adapters call it: `run_interactive`
/// and `run_scripted` differ only in where the line comes from, the line number
/// they can cite, and what an unfinished statement does at exit. Driving this
/// directly is therefore how the interactive path is covered without a terminal.
#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub fn feed_line(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    input: InputContext,
    sync_plugin: Option<&SyncPlugin>,
    session: &mut SessionState,
    pending: &mut StatementBuffer,
) -> bool {
    match route_line(line, !pending.is_empty()) {
        LineRouting::Skip => return true,
        LineRouting::Meta => {
            return process_meta_line(
                db,
                sync_client,
                rt,
                line.trim(),
                input,
                sync_plugin,
                session,
            );
        }
        LineRouting::Sql => {}
    }

    pending.push_line(line, input.script_line);
    while let Some(statement) = pending.take_terminated() {
        run_statement(db, statement, input, session);
    }
    true
}

/// Run one gathered statement, echoing scripted INSERTs as before. A parse
/// error is placed at the line the statement started on.
fn run_statement(
    db: &Database,
    statement: PendingStatement,
    input: InputContext,
    session: &mut SessionState,
) {
    let sql = statement.text.trim();
    if sql.is_empty() {
        return;
    }
    let input = InputContext {
        script_line: statement.first_line,
        ..input
    };
    let upper = sql.to_uppercase();
    // The scripted INSERT echo is human affordance; under --json stdout is
    // the machine data channel, so suppress it to keep the stream clean.
    if !input.interactive && !input.output.json && upper.starts_with("INSERT") {
        println!("{sql}");
    }
    if !execute_sql(db, sql, input, session.trace_enabled) {
        session.had_error = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn process_meta_line(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    input: InputContext,
    sync_plugin: Option<&SyncPlugin>,
    session: &mut SessionState,
) -> bool {
    if line.starts_with(".sync") || line.starts_with("\\sync") {
        let mut parts = line.splitn(2, ' ');
        let _cmd = parts.next();
        let rest = parts.next().unwrap_or("").trim();
        let outcome = run_sync_command(
            sync_client,
            rt,
            rest,
            sync_plugin,
            &mut session.had_unconfirmed_push,
        );
        if outcome.ok {
            report_success(&outcome.message, &outcome.json, input.output);
        } else {
            report_failure(outcome.class, &outcome.message, input);
            session.had_error = true;
        }
    } else if is_trace_command(line) {
        if !handle_trace_command(line, input, &mut session.trace_enabled) {
            session.had_error = true;
        }
    } else if is_explain_command(line) {
        if !handle_explain_command(db, line, input) {
            session.had_error = true;
        }
    } else {
        let outcome = handle_meta_command(db, sync_client, rt, line, input, sync_plugin);
        if !outcome.ok {
            session.had_error = true;
        }
        if !outcome.keep_going {
            return false;
        }
    }
    true
}

/// A meta-command's result: the JSON document on stdout under `--json`, the
/// human line otherwise.
fn report_success(message: &str, document: &serde_json::Value, output: OutputOptions) {
    if output.json {
        json_output::print_document(document);
    } else {
        println!("{message}");
    }
}

/// A failure, always on stderr: the `{"error":{...}}` envelope under `--json`,
/// the human line otherwise. stdout carries results and nothing else.
fn report_failure(class: ErrorClass, message: &str, input: InputContext) {
    if input.output.json {
        json_output::print_error(class, message, input.script_line);
    } else {
        eprintln!("{message}");
    }
}

struct SyncCommandOutcome {
    message: String,
    /// The same outcome as a machine document, printed instead of `message`
    /// under `--json`. Unused when the command failed — a failure is reported
    /// through the error envelope on stderr.
    json: serde_json::Value,
    ok: bool,
    /// Which error family a failure belongs to, for the `--json` envelope.
    class: ErrorClass,
}

impl SyncCommandOutcome {
    fn succeeded(message: String, json: serde_json::Value) -> Self {
        Self {
            message,
            json,
            ok: true,
            class: ErrorClass::Sync,
        }
    }

    fn failed(class: ErrorClass, message: String) -> Self {
        Self {
            message,
            json: serde_json::Value::Null,
            ok: false,
            class,
        }
    }
}

/// What a meta-command did: whether the session continues (`.quit` ends it) and
/// whether the command succeeded. A failed meta-command fails the run, exactly
/// like a failed SQL statement — a script cannot tell the two apart from `$?`
/// and should not have to.
pub(crate) struct MetaCommandOutcome {
    keep_going: bool,
    ok: bool,
}

impl MetaCommandOutcome {
    fn done() -> Self {
        Self {
            keep_going: true,
            ok: true,
        }
    }

    fn failed() -> Self {
        Self {
            keep_going: true,
            ok: false,
        }
    }

    fn quit() -> Self {
        Self {
            keep_going: false,
            ok: true,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_meta_command(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    input: InputContext,
    sync_plugin: Option<&SyncPlugin>,
) -> MetaCommandOutcome {
    let mut parts = line.splitn(2, ' ');
    let cmd = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();

    match cmd {
        ".quit" | ".exit" | "\\q" => return MetaCommandOutcome::quit(),
        ".help" | "\\?" => {
            // Help is prose for a human, and there is no machine contract worth
            // freezing in it: an agent discovers the surface from docs/cli.md
            // and `--help`, not from the REPL banner. Under --json it is
            // written to stderr as one document, so stdout stays pure JSON
            // Lines and stderr stays parseable.
            let lines = help_lines(rest);
            if input.output.json {
                eprintln!("{}", serde_json::json!({ "help": lines }));
            } else {
                for line in lines {
                    println!("{line}");
                }
            }
        }
        ".tables" | "\\dt" => {
            let names = db.table_names();
            if input.output.json {
                json_output::print_document(&json_output::tables_document(names));
            } else {
                for t in names {
                    println!("{t}");
                }
            }
        }
        ".schema" | "\\d" => {
            if rest.is_empty() {
                report_failure(
                    ErrorClass::Usage,
                    "Usage: .schema <table> or \\d <table>",
                    input,
                );
                return MetaCommandOutcome::failed();
            } else if let Some(meta) = db.table_meta(rest) {
                if input.output.json {
                    json_output::print_document(&json_output::table_meta_document(rest, &meta));
                } else {
                    print_table_meta(rest, &meta);
                }
            } else {
                report_failure(ErrorClass::Sql, &format!("Table not found: {rest}"), input);
                return MetaCommandOutcome::failed();
            }
        }
        ".maintenance" => match rest {
            "status" => {
                let status = db.maintenance_status();
                if input.output.json {
                    json_output::print_document(&json_output::maintenance_status_document(&status));
                } else {
                    println!(
                        "running={} retention_enabled={} currency_compaction_enabled={} \
                             active_maintenance_loops={} policy={}",
                        status.running,
                        status.retention_enabled,
                        status.currency_compaction_enabled,
                        status.active_maintenance_loops,
                        json_output::maintenance_policy_wire_word(status.policy),
                    );
                }
            }
            "run" => match db.run_maintenance_cycle() {
                Ok(report) => {
                    if input.output.json {
                        json_output::print_document(&json_output::maintenance_report_document(
                            &report,
                        ));
                    } else {
                        println!(
                            "pruned_rows={} currency_pruned_versions={} \
                                 pruned_trigger_audit_rows={} auto_compact_ran={}",
                            report.pruning.pruned_rows,
                            report.currency.pruned_versions,
                            report.pruned_trigger_audit_rows,
                            report.compaction.ran,
                        );
                    }
                }
                Err(err) => {
                    report_failure(ErrorClass::of(&err), &err.to_string(), input);
                    return MetaCommandOutcome::failed();
                }
            },
            "compact" => match db.compact_now() {
                Ok(report) => {
                    if input.output.json {
                        json_output::print_document(&json_output::compaction_report_document(
                            &report,
                        ));
                    } else {
                        println!(
                            "ran={} duration_micros={} bytes_before={:?} bytes_after={:?} \
                                 file_shrank={} fragmentation_before={}",
                            report.ran,
                            report.duration_micros,
                            report.bytes_before,
                            report.bytes_after,
                            report.file_shrank,
                            report.fragmentation_before,
                        );
                    }
                }
                Err(err) => {
                    report_failure(ErrorClass::of(&err), &err.to_string(), input);
                    return MetaCommandOutcome::failed();
                }
            },
            _ => {
                report_failure(
                    ErrorClass::Usage,
                    "Usage: .maintenance status | .maintenance run | .maintenance compact",
                    input,
                );
                return MetaCommandOutcome::failed();
            }
        },
        ".sync" | "\\sync" => {
            let outcome = handle_sync_command(sync_client, rt, rest, sync_plugin);
            if !outcome.ok {
                report_failure(outcome.class, &outcome.message, input);
                return MetaCommandOutcome::failed();
            }
            report_success(&outcome.message, &outcome.json, input.output);
        }
        _ => {
            report_failure(
                ErrorClass::Usage,
                &format!("Unknown command: {cmd}. Type \\? for help."),
                input,
            );
            return MetaCommandOutcome::failed();
        }
    }

    MetaCommandOutcome::done()
}

/// The help text for a topic, one line per entry. Rendering is the caller's
/// business — stdout for a human, one JSON document on stderr under `--json`.
fn help_lines(topic: &str) -> Vec<&'static str> {
    if topic.eq_ignore_ascii_case("vector") {
        return vec![
            "VECTOR(N) WITH (quantization = 'F32'|'SQ8'|'SQ4')",
            "SHOW VECTOR_INDEXES",
            "ORDER BY embedding <=> [0.1, 0.2, 0.3] LIMIT 5",
            "ORDER BY embedding <=> ROW_VECTOR('table', 'embedding', $key) LIMIT 5",
            "Vector errors include LegacyVectorStoreDetected, StoreCorrupted, VectorIndexDimensionMismatch, UnknownVectorIndex, PersistedRowVectorRowMissing, and PersistedRowVectorCellNull",
        ];
    }
    if topic.eq_ignore_ascii_case("propagate") {
        return vec![
            "PROPAGATE ON EDGE <edge_type> INCOMING|OUTGOING|BOTH STATE <state> SET <state>",
            "  [MAX DEPTH n] [ABORT ON FAILURE]",
            "PROPAGATE ON STATE <state> EXCLUDE VECTOR",
            "<column> REFERENCES <table>(<col>) ON STATE <state> PROPAGATE SET <state>",
            "",
            "Worked example:",
            "  CREATE TABLE decisions (",
            "    id UUID PRIMARY KEY,",
            "    status TEXT NOT NULL,",
            "    intention_id UUID REFERENCES intentions(id)",
            "      ON STATE archived PROPAGATE SET invalidated,",
            "    embedding VECTOR(384)",
            "  ) STATE MACHINE (status: active -> [invalidated, superseded])",
            "    PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated",
            "    PROPAGATE ON STATE invalidated EXCLUDE VECTOR",
            "    PROPAGATE ON STATE superseded EXCLUDE VECTOR",
            "See docs/query-language.md for the full grammar.",
        ];
    }
    vec![
        ".help / \\?          Show this message",
        ".help vector        Show vector index syntax and errors",
        ".help propagate     Show PROPAGATE DDL grammar and a worked example",
        ".quit/.exit / \\q    Exit REPL",
        ".tables / \\dt       List tables",
        ".schema / \\d <tbl>  Show table schema and constraints",
        ".explain <sql>      Show execution plan",
        ".trace on|off       Toggle one-line execution traces",
        ".sync status              Show sync connection info",
        ".sync push                Push local changes to server",
        "                          exit codes: 0 = pushed; 1 = failed; 3 = interrupted after sending, outcome unconfirmed, re-push is safe",
        ".sync pull                Pull remote changes from server",
        ".sync reconnect           Reconnect to the sync endpoint",
        ".sync destination         Move retained-data delivery to the connected hub",
        ".sync direction <t> <d>   Set table sync direction (Push|Pull|Both|None)",
        ".sync policy <t> <p>      Set table conflict policy (InsertIfNotExists|ServerWins|EdgeWins|LatestWins)",
        ".sync policy default <p>  Set default conflict policy",
        ".sync auto [on|off]       Toggle auto-sync after DML",
    ]
}

fn is_trace_command(line: &str) -> bool {
    matches!(
        line.split_whitespace().next(),
        Some(".trace") | Some("\\trace")
    )
}

fn is_explain_command(line: &str) -> bool {
    matches!(line.split_whitespace().next(), Some(".explain"))
}

fn handle_trace_command(line: &str, input: InputContext, trace_enabled: &mut bool) -> bool {
    let output = input.output;
    let mut parts = line.split_whitespace();
    let _cmd = parts.next();
    match parts.next() {
        Some(state) if state.eq_ignore_ascii_case("on") => {
            *trace_enabled = true;
            report_success(
                "Trace enabled",
                &json_output::trace_state_document(true),
                output,
            );
        }
        Some(state) if state.eq_ignore_ascii_case("off") => {
            *trace_enabled = false;
            report_success(
                "Trace disabled",
                &json_output::trace_state_document(false),
                output,
            );
        }
        None => {
            let state = if *trace_enabled { "on" } else { "off" };
            report_success(
                &format!("Trace: {state}"),
                &json_output::trace_state_document(*trace_enabled),
                output,
            );
        }
        Some(_) => {
            report_failure(ErrorClass::Usage, "Usage: .trace on|off", input);
            return false;
        }
    }
    true
}

fn handle_explain_command(db: &Database, line: &str, input: InputContext) -> bool {
    let rest = line.strip_prefix(".explain").unwrap_or("").trim();
    if rest.is_empty() {
        report_failure(ErrorClass::Usage, "Usage: .explain <sql>", input);
        return false;
    }

    if input.output.json {
        // The same rule the human path applies through `explain_output`: only a
        // read-only statement may be RUN to collect a runtime trace. Anything
        // else is planned statically — `.explain DELETE FROM t` answers what it
        // WOULD do, and must never do it.
        if !explain_can_use_runtime_trace(rest) {
            return match db.explain(rest) {
                Ok(plan) => {
                    json_output::print_document(&json_output::static_plan_document(&plan));
                    true
                }
                Err(e) => {
                    report_error(&e, input);
                    false
                }
            };
        }
        return match db.execute(rest, &HashMap::new()) {
            Ok(result) => {
                json_output::print_document(&json_output::explain_document(&result));
                true
            }
            Err(e) => {
                report_error(&e, input);
                false
            }
        };
    }

    match explain_output(db, rest) {
        Ok(plan) => {
            print!("{}", plan);
            true
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            false
        }
    }
}

/// Report an engine error: the `{"error":{...}}` envelope on stderr under
/// `--json`, the human `Error: ...` line on stderr otherwise. Never stdout.
fn report_error(error: &Error, input: InputContext) {
    let flattened = error.to_string().replace('\n', " ");
    if input.output.json {
        json_output::print_error(ErrorClass::of(error), &flattened, input.script_line);
    } else if matches!(error, Error::ParseError(_))
        && let Some(line) = input.script_line
    {
        eprintln!("Error: line {line}: {flattened}");
    } else {
        eprintln!("Error: {flattened}");
    }
}

fn explain_output(db: &Database, sql: &str) -> contextdb_core::Result<String> {
    if explain_can_use_runtime_trace(sql) {
        contextdb_engine::cli_render::render_explain(db, sql, &HashMap::new())
    } else {
        db.explain(sql).map(|mut plan| {
            if !plan.ends_with('\n') {
                plan.push('\n');
            }
            plan
        })
    }
}

fn explain_can_use_runtime_trace(sql: &str) -> bool {
    sql.split_whitespace().next().is_some_and(|token| {
        token.eq_ignore_ascii_case("SELECT") || token.eq_ignore_ascii_case("WITH")
    })
}

fn handle_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
) -> SyncCommandOutcome {
    let mut unconfirmed_push = false;
    run_sync_command(sync_client, rt, args, sync_plugin, &mut unconfirmed_push)
}

fn run_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
    unconfirmed_push: &mut bool,
) -> SyncCommandOutcome {
    let parts: Vec<&str> = args.split_whitespace().collect();
    let sub = parts.first().copied().unwrap_or("status");

    let (Some(client), Some(rt)) = (sync_client, rt) else {
        // Without a sync endpoint a QUERY still answers the question it was
        // asked ("is sync set up?" — no), so it succeeds. An ACTION did not
        // happen: reporting it as success would let a scripted
        // `push && shutdown` on a misconfigured machine abandon data it
        // believes reached the hub, which is the same failure the distinct
        // unconfirmed-push exit code exists to prevent.
        let message = "Sync not configured. Start with --tenant-id to enable.".to_string();
        return match sub {
            "status" => SyncCommandOutcome::succeeded(
                message,
                serde_json::json!({ "sync": { "configured": false } }),
            ),
            "auto" => SyncCommandOutcome::succeeded(
                message,
                serde_json::json!({ "sync_auto": { "configured": false, "enabled": false } }),
            ),
            _ => SyncCommandOutcome::failed(ErrorClass::Usage, message),
        };
    };

    match sub {
        "status" => {
            let connected = rt.block_on(client.ensure_connected()).is_ok();
            let view = crate::sync_status::SyncEndpointStatusView {
                tenant_id: client.tenant_id().to_string(),
                endpoint: client.endpoint().to_string(),
                transport_connected: connected,
                database_lsn: client.db().current_lsn().to_string(),
                push_watermark: client.push_watermark().to_string(),
                pull_watermark: client.pull_watermark().to_string(),
            };
            let base = crate::sync_status::render_sync_endpoint_status(&view);
            let render = contextdb_engine::cli_render::render_sync_status(client.db());
            let mut document = crate::sync_status::sync_endpoint_status_json(&view);
            if let Some(object) = document.as_object_mut() {
                object.insert(
                    "committed_txid".to_string(),
                    serde_json::json!(client.db().committed_watermark().0),
                );
            }
            SyncCommandOutcome::succeeded(
                format!("{base}\n{render}"),
                serde_json::json!({ "sync": document }),
            )
        }
        "push" => match rt.block_on(client.push()) {
            Ok(result) => {
                let mut msg = format!(
                    "Pushed: {} applied, {} skipped, {} conflicts",
                    result.applied_rows,
                    result.skipped_rows,
                    result.conflicts.len()
                );
                for conflict in &result.conflicts {
                    if let Some(reason) = &conflict.reason {
                        msg.push_str(&format!("\n  conflict: {}", reason));
                    }
                }
                SyncCommandOutcome::succeeded(
                    msg,
                    serde_json::json!({ "sync_push": apply_result_json(&result, "applied") }),
                )
            }
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. }) => {
                // The batch reached the hub but the acknowledgement was lost, so
                // whether the data landed is genuinely unknown. Keep `ok` true so
                // this is NOT reported as a plain failure (usability job USR-19)
                // and the interactive REPL prints the warning and continues, but
                // flag it so non-interactive use exits with the distinct
                // EXIT_INTERRUPTED_PUSH_UNCONFIRMED code — a scripted
                // `push && shutdown` must not read this unknown outcome as a
                // clean success. Re-running `.sync push` reconciles idempotently
                // whether or not the batch committed.
                *unconfirmed_push = true;
                SyncCommandOutcome::succeeded(
                    format!(
                        "Push interrupted after sending to {} (tenant {}); the hub did not acknowledge, so the data may or may not have landed. Run `.sync push` again to reconcile — it is safe whether or not the batch committed.",
                        client.endpoint(),
                        client.tenant_id()
                    ),
                    serde_json::json!({ "sync_push": { "outcome": "unconfirmed" } }),
                )
            }
            Err(e) => SyncCommandOutcome::failed(
                ErrorClass::of(&e),
                format!(
                    "Push to sync endpoint {} (tenant {}) failed: {e}. Check the endpoint is reachable, then retry with `.sync reconnect` followed by `.sync push`.",
                    client.endpoint(),
                    client.tenant_id()
                ),
            ),
        },
        "pull" => match rt.block_on(client.pull_default()) {
            Ok(result) => {
                let mut msg = format!(
                    "Pulled: {} applied, {} skipped, {} conflicts",
                    result.applied_rows,
                    result.skipped_rows,
                    result.conflicts.len()
                );
                for conflict in &result.conflicts {
                    if let Some(reason) = &conflict.reason {
                        msg.push_str(&format!("\n  conflict: {}", reason));
                    }
                }
                SyncCommandOutcome::succeeded(
                    msg,
                    serde_json::json!({ "sync_pull": apply_result_json(&result, "applied") }),
                )
            }
            Err(e) => SyncCommandOutcome::failed(
                ErrorClass::of(&e),
                format!(
                    "Pull from sync endpoint {} (tenant {}) failed: {e}. Check the endpoint is reachable, then retry with `.sync reconnect` followed by `.sync pull`.",
                    client.endpoint(),
                    client.tenant_id()
                ),
            ),
        },
        "reconnect" => {
            rt.block_on(client.reconnect());
            let connected = rt.block_on(client.is_connected());
            let message = crate::sync_status::render_reconnect_outcome(connected);
            if connected {
                SyncCommandOutcome::succeeded(
                    message,
                    serde_json::json!({ "sync_reconnect": { "connected": true } }),
                )
            } else {
                SyncCommandOutcome::failed(ErrorClass::Sync, message)
            }
        }
        "destination" => {
            // Move this edge's retained-data destination to the hub it is
            // currently, provably connected to. The operator points the process
            // at the new server (`--sync-endpoint`) and restarts, then runs
            // this; it authorises the move at the sync layer — clearing what the
            // PREVIOUS destination confirmed — so the next push rebuilds the new
            // destination from everything the edge holds, and nothing local is
            // deleted until the new destination confirms receipt.
            if rt.block_on(client.ensure_connected()).is_err() {
                return SyncCommandOutcome::failed(
                    ErrorClass::Sync,
                    format!(
                        "Cannot change destination: the sync endpoint {} is unreachable. Start the CLI pointed at the new server with --sync-endpoint, then run `.sync destination`.",
                        client.endpoint()
                    ),
                );
            }
            let Some(node_id) = client.connected_hub_node_id() else {
                return SyncCommandOutcome::failed(
                    ErrorClass::Sync,
                    "Cannot change destination: the transport does not authenticate a hub identity, so there is nothing to bind to. Use the dial-by-key (iroh) transport."
                        .to_string(),
                );
            };
            match client.change_destination(&node_id) {
                Ok(()) => SyncCommandOutcome::succeeded(
                    format!(
                        "Retained-data destination is now hub {node_id}. Run `.sync push` to rebuild it from everything this edge holds; nothing local is deleted until it confirms receipt."
                    ),
                    serde_json::json!({ "sync_destination": { "hub_node_id": node_id } }),
                ),
                Err(err) => SyncCommandOutcome::failed(
                    ErrorClass::of(&err),
                    format!("Changing destination failed: {err}"),
                ),
            }
        }
        "direction" => {
            if parts.len() != 3 {
                return SyncCommandOutcome::failed(
                    ErrorClass::Usage,
                    "Usage: .sync direction <table> <Push|Pull|Both|None>".to_string(),
                );
            }
            let table = parts[1];
            let dir = match parts[2] {
                "Push" | "push" => SyncDirection::Push,
                "Pull" | "pull" => SyncDirection::Pull,
                "Both" | "both" => SyncDirection::Both,
                "None" | "none" => SyncDirection::None,
                other => {
                    return SyncCommandOutcome::failed(
                        ErrorClass::Usage,
                        format!("Unknown direction: {other}. Use: Push, Pull, Both, None"),
                    );
                }
            };
            if let Err(err) = client.set_table_direction(table, dir) {
                // A delivery-promising table refuses a direction that would
                // stop delivering it; surface the engine's reason verbatim.
                return SyncCommandOutcome::failed(ErrorClass::of(&err), err.to_string());
            }
            SyncCommandOutcome::succeeded(
                format!("{table} -> {dir:?}"),
                serde_json::json!({
                    "sync_direction": {
                        "table": table,
                        "direction": json_output::sync_direction_wire_word(dir),
                    }
                }),
            )
        }
        "policy" => {
            if parts.len() != 3 {
                return SyncCommandOutcome::failed(
                    ErrorClass::Usage,
                    "Usage: .sync policy <table> <InsertIfNotExists|ServerWins|EdgeWins|LatestWins>\n       .sync policy default <policy>".to_string(),
                );
            }
            let policy = match parts[2] {
                "InsertIfNotExists" => ConflictPolicy::InsertIfNotExists,
                "ServerWins" => ConflictPolicy::ServerWins,
                "EdgeWins" => ConflictPolicy::EdgeWins,
                "LatestWins" => ConflictPolicy::LatestWins,
                other => {
                    return SyncCommandOutcome::failed(
                        ErrorClass::Usage,
                        format!(
                            "Unknown policy: {other}. Use: InsertIfNotExists, ServerWins, EdgeWins, LatestWins"
                        ),
                    );
                }
            };
            if parts[1] == "default" {
                client.set_default_conflict_policy(policy);
                SyncCommandOutcome::succeeded(
                    format!("Default conflict policy -> {policy:?}"),
                    serde_json::json!({
                        "sync_policy": {
                            "scope": "default",
                            "policy": json_output::conflict_policy_wire_word(policy),
                        }
                    }),
                )
            } else {
                client.set_conflict_policy(parts[1], policy);
                SyncCommandOutcome::succeeded(
                    format!("{} -> {policy:?}", parts[1]),
                    serde_json::json!({
                        "sync_policy": {
                            "table": parts[1],
                            "policy": json_output::conflict_policy_wire_word(policy),
                        }
                    }),
                )
            }
        }
        "auto" => {
            let Some(plugin) = sync_plugin else {
                return SyncCommandOutcome::succeeded(
                    "Auto-sync is not enabled for this session. Restart the CLI with --tenant-id (and a sync endpoint) to turn it on.".to_string(),
                    serde_json::json!({ "sync_auto": { "configured": false, "enabled": false } }),
                );
            };
            let toggle = parts.get(1).copied().unwrap_or("");
            match toggle {
                "on" => {
                    plugin.set_auto(true);
                    SyncCommandOutcome::succeeded(
                        "Auto-sync enabled".to_string(),
                        auto_sync_json(true),
                    )
                }
                "off" => {
                    plugin.set_auto(false);
                    SyncCommandOutcome::succeeded(
                        "Auto-sync disabled".to_string(),
                        auto_sync_json(false),
                    )
                }
                "" => {
                    let enabled = plugin.is_auto();
                    let state = if enabled { "on" } else { "off" };
                    SyncCommandOutcome::succeeded(
                        format!("Auto-sync: {state}"),
                        auto_sync_json(enabled),
                    )
                }
                other => SyncCommandOutcome::failed(
                    ErrorClass::Usage,
                    format!("Unknown auto-sync option: {other}. Use: on, off"),
                ),
            }
        }
        _ => SyncCommandOutcome::failed(
            ErrorClass::Usage,
            format!(
                "Unknown sync command: {sub}. Try: status, push, pull, reconnect, destination, direction, policy, auto"
            ),
        ),
    }
}

/// A push or pull result as a document: what moved, what was skipped, and each
/// conflict's reason — the same three facts the human line reports.
fn apply_result_json(
    result: &contextdb_engine::sync_types::ApplyResult,
    outcome: &str,
) -> serde_json::Value {
    serde_json::json!({
        "applied_rows": result.applied_rows,
        "skipped_rows": result.skipped_rows,
        "conflicts": result
            .conflicts
            .iter()
            .filter_map(|conflict| conflict.reason.as_ref())
            .map(|reason| serde_json::json!({ "reason": reason }))
            .collect::<Vec<_>>(),
        "outcome": outcome,
    })
}

fn auto_sync_json(enabled: bool) -> serde_json::Value {
    serde_json::json!({ "sync_auto": { "configured": true, "enabled": enabled } })
}

fn print_table_meta(table: &str, meta: &TableMeta) {
    print!(
        "{}",
        contextdb_engine::cli_render::render_table_meta(table, meta)
    );
}

/// Execute a SQL statement and print the result. Returns `true` on success, `false` on error.
fn execute_sql(db: &Database, sql: &str, input: InputContext, trace_enabled: bool) -> bool {
    let output = input.output;
    match db.execute(sql, &HashMap::new()) {
        Ok(result) => {
            let rows_examined = db.__rows_examined();
            if result.columns.is_empty() {
                if output.json {
                    // A non-query statement: a small JSON status object so the
                    // machine channel stays JSON end to end.
                    println!(
                        "{}",
                        serde_json::json!({ "rows_affected": result.rows_affected })
                    );
                } else {
                    println!("ok (rows_affected={})", result.rows_affected);
                }
            } else if output.json {
                // Query result as a JSON array of row objects — uncapped.
                println!("{}", format_query_result_json(&result));
            } else {
                let show_empty_headers = sql
                    .trim()
                    .trim_end_matches(';')
                    .eq_ignore_ascii_case("SHOW VECTOR_INDEXES");
                let cap = if output.all {
                    None
                } else {
                    Some(DEFAULT_ROW_CAP)
                };
                let formatted = format_query_result_capped(&result, cap, show_empty_headers);
                println!("{formatted}");
            }
            if trace_enabled {
                if output.json {
                    // The trace is diagnostics about a result, not the result:
                    // on stdout it would corrupt the JSON Lines stream a
                    // consumer is parsing.
                    json_output::print_trace(&result.trace, rows_examined);
                } else {
                    println!(
                        "{}",
                        contextdb_engine::cli_render::render_query_trace(
                            &result.trace,
                            rows_examined
                        )
                    );
                }
            }
            true
        }
        Err(e) => {
            // Every error goes to stderr and fails the run. The session still
            // continues to the next statement (a script gets to see all of its
            // errors, as in `psql` and `sqlite3`), but the process reports the
            // failure — an error a caller cannot see on `$?` is an error it
            // acts on as though it were success.
            report_error(&e, input);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};
    use contextdb_parser::{Statement, parse};

    /// A scripted, human-output context — what the meta-command tests below
    /// drive, so they exercise the same path a piped session takes.
    fn scripted_input() -> InputContext {
        InputContext {
            interactive: false,
            script_line: None,
            output: OutputOptions::default(),
        }
    }

    #[test]
    fn maintenance_status_reports_a_database_with_nothing_declared() {
        let db = Database::open_memory();
        let outcome = handle_meta_command(
            &db,
            None,
            None,
            ".maintenance status",
            scripted_input(),
            None,
        );
        assert!(outcome.keep_going && outcome.ok);
    }

    #[test]
    fn maintenance_run_drives_one_cycle_on_demand() {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY) RETAIN 1 HOURS",
            &HashMap::new(),
        )
        .unwrap();
        let outcome =
            handle_meta_command(&db, None, None, ".maintenance run", scripted_input(), None);
        assert!(outcome.keep_going && outcome.ok);
    }

    #[test]
    fn maintenance_with_no_subcommand_is_a_usage_error() {
        let db = Database::open_memory();
        let outcome = handle_meta_command(&db, None, None, ".maintenance", scripted_input(), None);
        assert!(outcome.keep_going && !outcome.ok);
    }

    #[test]
    fn test_backslash_dt() {
        let db = Database::open_memory();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        assert!(handle_meta_command(&db, None, None, "\\dt", scripted_input(), None).keep_going);
    }

    // B1: Existing \dt works with new handle_meta_command signature
    #[test]
    fn b1_existing_dt_works_with_new_signature() {
        let db = Database::open_memory();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        // Pass None for sync_client and rt — existing commands must work without sync
        let dt = handle_meta_command(&db, None, None, "\\dt", scripted_input(), None);
        assert!(dt.keep_going && dt.ok);
        // Also verify .tables works
        let tables = handle_meta_command(&db, None, None, ".tables", scripted_input(), None);
        assert!(tables.keep_going && tables.ok);
        // .quit ends the session, successfully
        let quit = handle_meta_command(&db, None, None, ".quit", scripted_input(), None);
        assert!(!quit.keep_going && quit.ok);
    }

    // .sync subcommands handle missing sync configuration — every one of
    // them SAYS so, and the QUERY/ACTION split decides whether that answer is a
    // success. A query was answered; an action did not happen, and reporting it
    // as success is what would let a scripted `push && shutdown` abandon data
    // it believes reached the hub.
    #[test]
    fn b2_sync_not_configured_message() {
        for (subcmd, expected_ok) in [
            ("status", true),
            ("auto", true),
            ("push", false),
            ("pull", false),
            ("reconnect", false),
            ("destination", false),
            ("direction t Push", false),
            ("policy t ServerWins", false),
        ] {
            let result = handle_sync_command(None, None, subcmd, None);
            assert!(
                result.message.contains("Sync not configured"),
                "subcmd '{}' should return 'Sync not configured', got: {}",
                subcmd,
                result.message
            );
            assert_eq!(
                result.ok, expected_ok,
                "subcmd '{}' must report ok={} with no sync configured",
                subcmd, expected_ok
            );
        }
    }

    // B3: .sync direction parses all four direction values
    #[test]
    fn b3_sync_direction_parsing() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "nats://localhost:19999",
                contextdb_core::TenantId::from("b3-test"),
            )
        });

        // Suppress unused import warning — SyncDirection is used to verify the API exists
        let _directions = [
            SyncDirection::Push,
            SyncDirection::Pull,
            SyncDirection::Both,
            SyncDirection::None,
        ];

        for (table, dir) in [
            ("observations", "Push"),
            ("patterns", "pull"),
            ("decisions", "Both"),
            ("scratch", "None"),
        ] {
            let args = format!("direction {} {}", table, dir);
            let result = handle_sync_command(Some(&client), Some(&rt), &args, None).message;
            assert!(
                result.contains(table),
                "direction command for '{}' should contain table name, got: {}",
                table,
                result
            );
        }
    }

    // B4: .sync policy parses all four policies + default
    #[test]
    fn b4_sync_policy_parsing() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "nats://localhost:19999",
                contextdb_core::TenantId::from("b4-test"),
            )
        });

        // Suppress unused import warning — ConflictPolicy is used to verify the API exists
        let _policies = [
            ConflictPolicy::InsertIfNotExists,
            ConflictPolicy::ServerWins,
            ConflictPolicy::EdgeWins,
            ConflictPolicy::LatestWins,
        ];

        let result = handle_sync_command(
            Some(&client),
            Some(&rt),
            "policy obs InsertIfNotExists",
            None,
        )
        .message;
        assert!(
            result.contains("InsertIfNotExists"),
            "policy command should contain 'InsertIfNotExists', got: {}",
            result
        );

        let result =
            handle_sync_command(Some(&client), Some(&rt), "policy default ServerWins", None)
                .message;
        assert!(
            result.contains("Default") || result.contains("default"),
            "default policy command should reference 'default', got: {}",
            result
        );
    }

    // B5: .sync invalid arguments handled gracefully
    #[test]
    fn b5_sync_invalid_args() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "nats://localhost:19999",
                contextdb_core::TenantId::from("b5-test"),
            )
        });

        for bad_input in [
            "bogus",
            "direction",
            "direction table_only",
            "direction t InvalidDir",
            "policy",
            "policy table_only",
            "policy t InvalidPolicy",
        ] {
            let result = handle_sync_command(Some(&client), Some(&rt), bad_input, None).message;
            assert!(
                !result.contains("not implemented"),
                "bad input '{}' should not return 'not implemented', got: {}",
                bad_input,
                result
            );
        }
    }

    #[test]
    fn rt2_repl_schema_display_round_trip_parse() {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE repl_rt_sm (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [done])",
            &HashMap::new(),
        )
        .unwrap();

        let meta = db.table_meta("repl_rt_sm").expect("table meta");
        let rendered = contextdb_engine::cli_render::render_table_meta("repl_rt_sm", &meta);
        assert!(matches!(parse(&rendered), Ok(Statement::CreateTable(_))));
    }

    // USR-14: `.schema` must render the declared RETAIN and PROPAGATE policy —
    // both the table-level RETAIN / PROPAGATE ON EDGE / PROPAGATE ON STATE
    // EXCLUDE VECTOR clauses and the column-level foreign-key ON STATE PROPAGATE
    // SET form — and the rendered DDL must round-trip to an equivalent table.
    #[test]
    fn schema_render_includes_retain_and_propagate_round_trip() {
        let make_db = || {
            let db = Database::open_memory();
            db.execute(
                "CREATE TABLE intentions (id UUID PRIMARY KEY, status TEXT)",
                &HashMap::new(),
            )
            .expect("parent table must create");
            db
        };
        let decisions_ddl = "CREATE TABLE decisions (\
                id UUID PRIMARY KEY, \
                description TEXT NOT NULL, \
                status TEXT NOT NULL, \
                intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, \
                embedding VECTOR(384)\
            ) STATE MACHINE (status: active -> [invalidated, superseded]) \
            RETAIN 30 DAYS SYNC SAFE \
            PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated \
            PROPAGATE ON STATE invalidated EXCLUDE VECTOR \
            PROPAGATE ON STATE superseded EXCLUDE VECTOR";

        let db = make_db();
        db.execute(decisions_ddl, &HashMap::new())
            .expect("decisions table must create");
        let meta = db.table_meta("decisions").expect("table meta");
        let rendered = contextdb_engine::cli_render::render_table_meta("decisions", &meta);

        // The declared policy must be visible in `.schema` output.
        assert!(
            rendered.contains("RETAIN 30 DAYS SYNC SAFE"),
            "rendered .schema must include the RETAIN clause, got:\n{rendered}"
        );
        assert!(
            rendered.contains("PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated"),
            "rendered .schema must include the edge PROPAGATE clause, got:\n{rendered}"
        );
        assert!(
            rendered.contains("PROPAGATE ON STATE invalidated EXCLUDE VECTOR"),
            "rendered .schema must include the vector-exclusion PROPAGATE clause, got:\n{rendered}"
        );
        assert!(
            rendered.contains("ON STATE archived PROPAGATE SET invalidated"),
            "rendered .schema must include the column foreign-key PROPAGATE form, got:\n{rendered}"
        );

        // Round-trip: the rendered DDL must re-parse and reconstruct an
        // equivalent table (re-rendering the reconstructed meta is identical).
        assert!(
            matches!(parse(&rendered), Ok(Statement::CreateTable(_))),
            "rendered .schema must re-parse as CREATE TABLE, got:\n{rendered}"
        );
        let db2 = make_db();
        db2.execute(&rendered, &HashMap::new()).unwrap_or_else(|e| {
            panic!("rendered DDL must execute on a fresh db: {e:?}\n{rendered}")
        });
        let meta2 = db2
            .table_meta("decisions")
            .expect("round-tripped table meta");
        let rerendered = contextdb_engine::cli_render::render_table_meta("decisions", &meta2);
        assert_eq!(
            rendered, rerendered,
            "re-rendering the round-tripped schema must be identical (equivalent TableMeta)"
        );
        assert_eq!(
            meta2.default_ttl_seconds,
            Some(30 * 24 * 60 * 60),
            "round-tripped RETAIN duration must survive"
        );
        assert!(meta2.sync_safe, "round-tripped SYNC SAFE flag must survive");
    }

    /// USR-19 exit contract. A `.sync push` interrupted after its batch was sent,
    /// where the hub cannot be reached to confirm the batch landed, must exit
    /// with the DISTINCT unconfirmed code (3) — never a plain success (0, which
    /// would let a `push && shutdown` caller close the machine on maybe-lost
    /// data) and never the definitive failure (1) that USR-19 removed. It still
    /// renders `ok` (the interactive REPL prints the warning and continues).
    #[test]
    fn interrupted_push_unconfirmed_maps_to_distinct_exit_code() {
        use contextdb_server::transport::{
            ClientTransport, TransportError, TransportFuture, TransportResult,
            TransportStatusFuture,
        };
        use std::time::Duration;

        // Connects, but every request after connect is unreachable — so the push
        // batch cannot be sent AND the reconcile status probe cannot confirm it.
        struct UnreachableAfterConnect;
        impl ClientTransport for UnreachableAfterConnect {
            fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
            fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
                Box::pin(async { true })
            }
            fn request<'a>(
                &'a self,
                _subject: &'a str,
                _bytes: Vec<u8>,
                _timeout: Duration,
            ) -> TransportFuture<'a, Vec<u8>> {
                Box::pin(async {
                    Err(TransportError::Unreachable(
                        "dial timed out (test)".to_string(),
                    ))
                })
            }
            fn request_single_reply<'a>(
                &'a self,
                _subject: &'a str,
                _bytes: Vec<u8>,
                _timeout: Duration,
            ) -> TransportFuture<'a, Vec<u8>> {
                Box::pin(async {
                    Err(TransportError::Unreachable(
                        "dial timed out (test)".to_string(),
                    ))
                })
            }
            fn ensure_single_reply_retry_safe(&self, _bytes: &[u8]) -> TransportResult<()> {
                Ok(())
            }
        }

        let db = Arc::new(Database::open_memory());
        db.execute(
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO notes (id, body) VALUES ('n1', 'pending')",
            &HashMap::new(),
        )
        .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = SyncClient::with_transport(
            db,
            Arc::new(UnreachableAfterConnect),
            contextdb_core::TenantId::from("usr19-exit"),
        );

        // The library surfaces the distinct indeterminate error — never a bare
        // SyncError and never a false Ok.
        let push = rt.block_on(client.push());
        assert!(
            matches!(push, Err(Error::SyncPushUnconfirmed { .. })),
            "an unconfirmable interrupted push must be SyncPushUnconfirmed, got {push:?}"
        );

        // The CLI render keeps `ok` true (not a plain failure) but raises the
        // unconfirmed flag, so a non-interactive session exits with the distinct
        // code 3.
        let mut unconfirmed_push = false;
        let outcome = run_sync_command(
            Some(&client),
            Some(&rt),
            "push",
            None,
            &mut unconfirmed_push,
        );
        assert!(
            outcome.ok,
            "an unconfirmed push must not render as a plain failure"
        );
        assert!(
            unconfirmed_push,
            "an unconfirmed push must raise the distinct-exit flag"
        );
        assert!(
            outcome.message.contains("interrupted"),
            "the warning must name the interruption, got: {}",
            outcome.message
        );

        let session = SessionState {
            had_error: false,
            had_unconfirmed_push: unconfirmed_push,
            trace_enabled: false,
            interactive: false,
        };
        let code = session_exit_code(&session);
        assert_eq!(
            code, EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
            "an unconfirmed push must map to the distinct exit code"
        );
        assert_ne!(code, 0, "must not be a clean-success exit");
        assert_ne!(code, 1, "must not be the definitive-failure exit");
        assert_eq!(EXIT_INTERRUPTED_PUSH_UNCONFIRMED, 3);
    }

    /// A terminal session showed the human every error as it happened, so
    /// the process code reports the session, not the statements inside it.
    /// A scripted run has nobody watching, so the same input fails there.
    #[test]
    fn interactive_session_errors_do_not_set_the_process_exit_code() {
        for (interactive, expected) in [(true, EXIT_OK), (false, EXIT_ERROR)] {
            let db = Database::open_memory();
            let mut session = SessionState {
                interactive,
                ..SessionState::default()
            };
            let mut pending = StatementBuffer::default();
            feed_line(
                &db,
                None,
                None,
                "SELET * FROM nothing;",
                InputContext {
                    interactive,
                    script_line: Some(1),
                    output: OutputOptions::default(),
                },
                None,
                &mut session,
                &mut pending,
            );
            assert!(session.had_error, "the parse error must be recorded");
            assert_eq!(
                session_exit_code(&session),
                expected,
                "an interactive session reports the session; a scripted one fails the run"
            );
        }
    }

    /// The unconfirmed push is the one signal a terminal session still
    /// reports, because nobody can act on it once the process is gone.
    #[test]
    fn unconfirmed_push_still_exits_three_in_an_interactive_session() {
        let session = SessionState {
            had_error: false,
            had_unconfirmed_push: true,
            trace_enabled: false,
            interactive: true,
        };
        assert_eq!(
            session_exit_code(&session),
            EXIT_INTERRUPTED_PUSH_UNCONFIRMED
        );
    }

    /// The exit-code ordering: a definitive error dominates (1), then an
    /// unconfirmed interrupted push (3), then a confirmed/clean run (0). The
    /// confirmed-landed push reports success at the library level in
    /// contextdb-server's `ip_interrupted_push_after_commit_*` test; here we pin
    /// how a clean session maps to exit 0.
    #[test]
    fn session_exit_code_orders_error_over_unconfirmed_over_success() {
        let clean = SessionState::default();
        assert_eq!(session_exit_code(&clean), 0, "a clean run exits 0");

        let unconfirmed = SessionState {
            had_error: false,
            had_unconfirmed_push: true,
            trace_enabled: false,
            interactive: false,
        };
        assert_eq!(
            session_exit_code(&unconfirmed),
            EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
            "an unconfirmed push alone exits 3"
        );

        let errored = SessionState {
            had_error: true,
            had_unconfirmed_push: true,
            trace_enabled: false,
            interactive: false,
        };
        assert_eq!(
            session_exit_code(&errored),
            1,
            "a definitive error dominates an unconfirmed push"
        );
    }
}
