use crate::command_registry::{
    MetaCommandAuthorization, authorize_meta_command_before_dispatch, canonical_help_signatures,
};
use crate::formatter::format_query_result_with_empty_headers;
use crate::json_output::{self, ErrorClass};
use contextdb_core::Error;
use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerServingState, ReadFailure, ReadFailureDetail, ReadFailureKind,
    ReadFailureLimit, ReadLimits,
};
use contextdb_engine::{Database, MetadataBody, MetadataRequest, ReadCursor, ReadSession};
use contextdb_server::exit_codes::{EXIT_ERROR, EXIT_INTERRUPTED_PUSH_UNCONFIRMED, EXIT_OK};
use contextdb_server::{SyncClient, SyncPlugin};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::io::{BufRead, IsTerminal};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// What this session may do to the store, and what it reads through.
///
/// A reading session holds ONLY the bounded read view. There is no database
/// handle behind it to reach around, which is what makes "never creates,
/// never changes a byte" a property of the session's SHAPE rather than a rule
/// every handler has to remember. A writing session holds both, and its own
/// SELECTs still go through the bounded view, so a writer's reads are bounded
/// exactly like anyone else's.
pub enum Session {
    Reading {
        reader: ReadSession,
        /// The store this session was pointed at. `.owner status` asks the
        /// path about its owner rather than the session, because it resolves
        /// no route and must answer even when there is no owner to route to.
        path: PathBuf,
    },
    Writing {
        database: Arc<Database>,
        reader: ReadSession,
    },
}

impl Session {
    /// Build a writing session over an already-open database, with the bounded
    /// read view its own reads will go through.
    pub fn writing(database: Arc<Database>, limits: ReadLimits) -> contextdb_core::Result<Self> {
        let reader = database.read_session(limits)?;
        Ok(Self::Writing { database, reader })
    }

    /// Adopt an already-selected read route as a reading session.
    pub fn reading(reader: ReadSession, path: PathBuf) -> Self {
        Self::Reading { reader, path }
    }

    /// The bounded view every read in this session goes through, whichever
    /// mode it is in.
    pub(crate) fn reader(&self) -> &ReadSession {
        match self {
            Self::Reading { reader, .. } => reader,
            Self::Writing { reader, .. } => reader,
        }
    }

    /// The store path a reading session was pointed at. A writing session is
    /// its own owner and answers about itself, so it needs none.
    fn read_path(&self) -> Option<&Path> {
        match self {
            Self::Reading { path, .. } => Some(path.as_path()),
            Self::Writing { .. } => None,
        }
    }

    /// The live database, when this session has one. A reading session has
    /// none, and that is the point.
    pub(crate) fn database(&self) -> Option<&Arc<Database>> {
        match self {
            Self::Reading { .. } => None,
            Self::Writing { database, .. } => Some(database),
        }
    }

    pub(crate) fn writes_permitted(&self) -> bool {
        self.database().is_some()
    }
}

/// The cancellation token of the statement this session is running, if any.
///
/// Ctrl-C at a terminal cancels THAT statement and nothing else: the session,
/// its route, and any open cursor all survive. A session with no terminal has
/// no holder at all, so an interrupt there keeps its ordinary meaning and ends
/// the process.
#[derive(Default)]
pub(crate) struct RunningStatement {
    token: Mutex<Option<OwnerReadCancellation>>,
}

impl RunningStatement {
    /// Announce the statement about to run, and take it back when it is done.
    /// The guard clears the slot even if the statement panics, so a later
    /// interrupt can never cancel a statement that already finished.
    fn running(&self, token: &OwnerReadCancellation) -> RunningStatementGuard<'_> {
        if let Ok(mut slot) = self.token.lock() {
            *slot = Some(token.clone());
        }
        RunningStatementGuard { holder: self }
    }

    fn cancel(&self) {
        if let Ok(slot) = self.token.lock()
            && let Some(token) = slot.as_ref()
        {
            token.cancel();
        }
    }
}

struct RunningStatementGuard<'a> {
    holder: &'a RunningStatement,
}

impl Drop for RunningStatementGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.holder.token.lock() {
            *slot = None;
        }
    }
}

/// Turn the interrupt a terminal sends into a cancellation of whatever
/// statement is running, instead of the end of the process.
///
/// Installed ONLY for an interactive session. While the prompt is reading, the
/// line editor holds the terminal and answers Ctrl-C itself by discarding the
/// typed line; while a statement is running the terminal is back in its
/// ordinary mode, so the same keystroke arrives here as a signal and cancels
/// the statement through the read-cancellation seam. A session with no
/// terminal never installs this, so an interrupt there still ends the process.
#[cfg(unix)]
fn install_statement_interrupt(running: Arc<RunningStatement>) {
    std::thread::spawn(move || {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        runtime.block_on(async move {
            let Ok(mut interrupts) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            else {
                return;
            };
            while interrupts.recv().await.is_some() {
                running.cancel();
            }
        });
    });
}

#[cfg(not(unix))]
fn install_statement_interrupt(_running: Arc<RunningStatement>) {}

/// The one cursor a session may hold, and what a later request to it means.
///
/// Draining a cursor frees the slot for the next `.cursor open` but does NOT
/// throw the cursor away: a fetch-until-empty loop asks where it left off, and
/// it left off at the end, so the drained cursor keeps answering one empty
/// success page. Closing it explicitly is a different thing entirely — that
/// cursor is gone, and asking for it again is a refusal.
#[derive(Default)]
enum CursorSlot {
    /// Never opened in this session.
    #[default]
    Absent,
    /// Open with more to give.
    Open(ReadCursor),
    /// Exhausted; still answers empty success pages, and no longer occupies
    /// the session's one cursor slot.
    Drained(ReadCursor),
    /// Explicitly closed. Asking for it again is `cursor_not_found`.
    Closed,
}

/// Where a line came from: an interactive terminal or a script. The only
/// framing difference between the two is the line number a script can cite and
/// what an unfinished statement does at exit.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct InputContext {
    pub interactive: bool,
    pub script_line: Option<usize>,
    pub output: OutputOptions,
    /// Whether this session was opened with `--write`. A session that was not
    /// may read the store but never change it, and that answer is given before
    /// dispatch rather than by whichever handler happens to notice.
    pub store_writes_permitted: bool,
}

/// How results are rendered. `--json` publishes machine documents; otherwise
/// the human table is printed. Rendering is all this decides: a result is
/// complete or it is refused, and neither rendering has a ceiling of its own.
#[derive(Clone, Copy, Debug, Default)]
pub struct OutputOptions {
    pub json: bool,
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

    /// Report a read refusal on stderr: the `{"error":{…,"detail":{…}}}`
    /// document under `--json`, the human line otherwise.
    ///
    /// The stable `class` and `detail.kind` come from the refusal itself, so
    /// the CLI can never classify a refusal differently from the engine that
    /// produced it.
    pub fn report_read_failure(&self, failure: &ReadFailure, line: Option<usize>) {
        self.report_read_failure_on_route(failure, line, None);
    }

    /// The same report, told by a session that knows which route it read on —
    /// so a crossed ceiling can name the setting that would actually move it.
    pub(crate) fn report_read_failure_on_route(
        &self,
        failure: &ReadFailure,
        line: Option<usize>,
        route: Option<contextdb_core::read_contract::ReadRoute>,
    ) {
        let message = read_failure_message(failure, route);
        if self.json {
            json_output::print_error_with_detail(
                ErrorClass::from(failure.class()),
                &message,
                line,
                Some(&json_output::read_failure_detail_document(failure)),
            );
        } else {
            eprintln!("Error: {message}");
        }
    }

    /// Report a notice whose typed detail must remain machine-addressable.
    pub fn report_notice_document(
        &self,
        class: ErrorClass,
        message: &str,
        detail: &serde_json::Value,
    ) {
        if self.json {
            json_output::print_notice_document(class, message, detail);
        } else {
            eprintln!("{message}: {detail}");
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
    /// The session's one cursor.
    cursor: CursorSlot,
    /// The SELECT that cursor is paging. A refusal the engine raises against
    /// the cursor knows the ceiling but never saw the statement, so the
    /// session that did keeps it here to complete the refusal's remedy.
    cursor_statement: Option<String>,
    /// Where a running statement announces itself so an interrupt can reach
    /// it. `None` when nobody is at a terminal to press Ctrl-C.
    interrupts: Option<Arc<RunningStatement>>,
    /// Whether this session has already said how it reads. The route is chosen
    /// once and never changes, so saying it twice would suggest it might have.
    route_reported: bool,
    /// Whether this session has an open write transaction. The CLI issues
    /// every `BEGIN`/`COMMIT`/`ROLLBACK` itself, so it knows; a cursor may not
    /// open across one, because cursor state must never mix committed and
    /// uncommitted rows.
    transaction_open: bool,
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
/// Whether this process is running under `--json`, recorded once at the top
/// of [`run`] (the single per-process entry point). Read by background
/// helpers — the pull-progress streamer chief among them — that have no
/// path back to the caller's [`OutputOptions`] without adding a parameter to
/// `run_sync_command`/`handle_sync_command`, both of which several
/// test-module call sites already fix at their current arities. A
/// process-global is safe here specifically because `run` is called exactly
/// once per real process (the compiled `contextdb` binary); no in-process
/// test calls `run` itself (tests exercise `feed_line`/`run_sync_command`
/// directly), so this never observes a stale value left by an unrelated
/// test in the same test binary.
static JSON_OUTPUT_MODE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

fn json_output_mode() -> bool {
    *JSON_OUTPUT_MODE.get().unwrap_or(&false)
}

/// The refusal in words, plus the next action it teaches.
///
/// The engine states WHAT stopped the read; naming the command or flag that
/// resolves it is the CLI's job, because the answer is spelled in command-line
/// vocabulary the engine does not own. A refusal whose own detail already
/// carries its remedy — a crossed ceiling names both escapes, route-aware —
/// says it once and is not given a second ending here.
fn read_failure_message(
    failure: &ReadFailure,
    route: Option<contextdb_core::read_contract::ReadRoute>,
) -> String {
    // A crossed ceiling has two ways out, and which one is HONEST depends on
    // the route. Reading a file directly, the caller owns its own ceilings and
    // may raise them for a deliberate one-shot export. Reading through an
    // owner, those same flags cannot raise owner policy — offering them would
    // be a lie — so the refusal names the writer-side setting instead. The
    // cursor is the in-session escape either way, and the refusal's own detail
    // already carries it copy-ready.
    if failure.kind() == ReadFailureKind::OwnerLimitExceeded {
        let raise = match route {
            Some(contextdb_core::read_contract::ReadRoute::File) => Some(
                "raise --read-result-rows / --read-result-bytes for a deliberate one-shot export",
            ),
            Some(contextdb_core::read_contract::ReadRoute::Owner) => Some(
                "the ceiling is the owner's, and a reader cannot raise it: change \
                 --owner-read-result-rows / --owner-read-result-bytes on the writer",
            ),
            None => None,
        };
        return match raise {
            Some(raise) => format!("{failure}; {raise}"),
            None => failure.to_string(),
        };
    }
    let recovery = match failure.kind() {
        ReadFailureKind::WriteRequiresFlag => Some("rerun with --write"),
        ReadFailureKind::StoreNotFound => Some("--write creates it"),
        ReadFailureKind::HeldByWriter => {
            Some("drop --write: a read session reaches the live owner's channel")
        }
        ReadFailureKind::HeldByReaders => Some("retry once they finish hydrating"),
        ReadFailureKind::OwnerNotRunning => Some("this session reads the committed file directly"),
        ReadFailureKind::OwnerNotServing => {
            Some("close the writer and rerun, or start it without --no-owner-reads")
        }
        ReadFailureKind::OwnerUserMismatch => Some("run as the user whose process owns the store"),
        ReadFailureKind::OwnerMismatch | ReadFailureKind::OwnerDisconnected => {
            Some("start a new invocation to route afresh")
        }
        ReadFailureKind::OwnerAtCapacity => Some(
            "retry, close another inspecting session, or raise the writer's \
             --owner-read-concurrency",
        ),
        ReadFailureKind::OwnerTimeout => Some("retry, or raise --read-owner-response-ms"),
        ReadFailureKind::InvalidChannelData | ReadFailureKind::LocalProtocolMismatch => {
            Some("the owner process and this client must be the same release")
        }
        ReadFailureKind::CursorExpired => Some("reopen it with `.cursor open <SELECT>`"),
        ReadFailureKind::CursorNotFound => Some("open one with `.cursor open <SELECT>`"),
        ReadFailureKind::CursorAlreadyOpen => Some("close it with `.cursor close` first"),
        ReadFailureKind::CursorTransactionActive => {
            Some("commit or roll back the transaction first")
        }
        ReadFailureKind::CursorInvalidStatement => {
            Some("`.cursor open` takes exactly one read-only SELECT")
        }
        ReadFailureKind::DirectReadRequiresWriter => {
            Some("close every holder and rerun with --write")
        }
        ReadFailureKind::InvalidContinuation => Some("start the listing again without --continue"),
        ReadFailureKind::OperationAlreadyCompleted | ReadFailureKind::OwnerLimitExceeded => None,
        // The refusal already names the inspection it could not answer and the
        // route that answers it, so a second sentence here would only repeat it.
        ReadFailureKind::OwnerRouteUnsupported => None,
        // Nothing on this command line declares an identity to read as, so a
        // session refused for declaring one was opened by an embedder. The
        // refusal already names the identity it declared and the one the
        // writer reads as, and there is no flag here to point at.
        ReadFailureKind::DeclaredPrincipalRefused => None,
    };
    match recovery {
        Some(recovery) => format!("{failure}; {recovery}"),
        None => failure.to_string(),
    }
}

/// Add to a crossed-ceiling refusal the two things only the CLI knows: the
/// statement that was refused, and the copy-ready command that pages it.
///
/// An agent that is handed back its own statement can re-issue it; one that is
/// handed a printed instruction can run it exactly as printed. Neither is
/// something the engine can supply — it never saw the command line, and the
/// cursor is CLI vocabulary — so the session that does know fills them in. A
/// refusal that already carries them is left alone.
fn refusal_naming_the_statement(failure: &ReadFailure, sql: &str) -> ReadFailure {
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        return failure.clone();
    };
    if detail.statement.is_some() {
        return failure.clone();
    }
    let statement = sql.trim().trim_end_matches(';').trim().to_owned();
    ReadFailure::owner_limit_exceeded(contextdb_core::read_contract::OwnerLimitExceededDetail {
        limit: detail.limit,
        value: detail.value,
        required: detail.required.clone(),
        statement: Some(contextdb_core::read_contract::StatementRemedy {
            remedy_command: format!(".cursor open {statement}"),
            statement,
        }),
    })
}

/// The refusal a session that may not write answers a writing statement or
/// command with. It carries no further detail: the kind IS the whole answer.
fn write_requires_flag() -> ReadFailure {
    ReadFailure::new(ReadFailureKind::WriteRequiresFlag, ReadFailureDetail::None)
        .expect("a write refusal carries no specialized detail")
}

/// Whether this statement would change the store, decided by the parser's own
/// classification rather than by a keyword list kept here — the same
/// classification a live owner applies when it answers a read, so the two
/// surfaces can never disagree about what a write is.
///
/// Input that parses as nothing is NOT a write. It is a parse error, reported
/// identically in read and write sessions, so a typo never arrives dressed as
/// a permission problem.
#[allow(dead_code)]
fn statement_writes(sql: &str) -> bool {
    match contextdb_parser::parse(sql) {
        Ok(statement) => {
            contextdb_parser::statement_effect(&statement)
                != contextdb_parser::StatementEffect::Read
        }
        Err(_) => false,
    }
}

/// What an interactive session announces about itself at startup, and the
/// prompt it then wears. Neither exists when nobody is watching a terminal.
const READ_SESSION_BANNER: &str = "read-only session — pass --write to mutate";
const WRITE_SESSION_BANNER: &str = "write session — mutations are enabled";
const READ_SESSION_PROMPT: &str = "contextdb(ro)> ";
const WRITE_SESSION_PROMPT: &str = "contextdb> ";

/// What `.sync status` answers in a session that cannot write. It reports THIS
/// process's sync state, and says so, so nobody reads it as the store's.
const READ_SESSION_SYNC_STATUS: &str = "no sync in this session — this reports the CLI session \
     only, not the store; a live owner's sync state belongs to that owner process.";

pub fn run(
    session: &Session,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let _ = JSON_OUTPUT_MODE.set(output.json);
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        eprintln!(
            "{}",
            if session.writes_permitted() {
                WRITE_SESSION_BANNER
            } else {
                READ_SESSION_BANNER
            }
        );
        return run_interactive(session, sync_client, rt, sync_plugin, output);
    }

    run_scripted(session, sync_client, rt, sync_plugin, output)
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
    session_handle: &Session,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let store_writes_permitted = session_handle.writes_permitted();
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
    let running = Arc::new(RunningStatement::default());
    install_statement_interrupt(Arc::clone(&running));
    let mut session = SessionState {
        interactive: true,
        interrupts: Some(running),
        ..SessionState::default()
    };
    let mut pending = StatementBuffer::default();

    loop {
        let prompt = if !pending.is_empty() {
            "      ...> "
        } else if store_writes_permitted {
            WRITE_SESSION_PROMPT
        } else {
            READ_SESSION_PROMPT
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
                    session_handle,
                    sync_client,
                    rt,
                    fed,
                    InputContext {
                        interactive: true,
                        script_line: None,
                        output,
                        store_writes_permitted,
                    },
                    sync_plugin,
                    &mut session,
                    &mut pending,
                ) {
                    break;
                }
            }
            // Ctrl-C at an idle prompt discards what was typed and ends
            // nothing — the statement in progress, if any, was already
            // cancelled by the signal handler. Ctrl-D ends the session.
            Err(ReadlineError::Interrupted) => {
                pending = StatementBuffer::default();
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(_) => break,
        }
    }

    if !pending.is_empty() {
        eprintln!("Discarding incomplete statement (no closing `;`).");
    }

    session_exit_code(&session)
}

fn run_scripted(
    session_handle: &Session,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let store_writes_permitted = session_handle.writes_permitted();
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
            session_handle,
            sync_client,
            rt,
            &line,
            InputContext {
                interactive: false,
                script_line: Some(line_idx + 1),
                output,
                store_writes_permitted,
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
            session_handle,
            statement,
            InputContext {
                interactive: false,
                script_line: None,
                output,
                store_writes_permitted,
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
    session_handle: &Session,
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
                session_handle,
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
        run_statement(session_handle, statement, input, session);
    }
    true
}

/// Run one gathered statement, echoing scripted INSERTs as before. A parse
/// error is placed at the line the statement started on.
fn run_statement(
    session_handle: &Session,
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
    if !execute_sql(session_handle, sql, input, session) {
        session.had_error = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn process_meta_line(
    session_handle: &Session,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    input: InputContext,
    sync_plugin: Option<&SyncPlugin>,
    session: &mut SessionState,
) -> bool {
    // The authorization answer comes FIRST, before any command-specific
    // dispatch. A command that writes to the store is refused here even when
    // its handler would have been a no-op, so the refusal is what a session
    // without `--write` observes rather than whatever the handler happened to
    // do.
    if let Some(MetaCommandAuthorization::RefuseStoreWrite(_)) =
        authorize_meta_command_before_dispatch(line, input.store_writes_permitted)
    {
        input
            .output
            .report_read_failure(&write_requires_flag(), input.script_line);
        session.had_error = true;
        return true;
    }
    // A session that cannot write has no sync of its own to report, and the
    // store's sync state is not this session's to speak for. Saying both is
    // what keeps the answer from being read as the store's health.
    if !input.store_writes_permitted
        && line.split_whitespace().collect::<Vec<_>>() == [".sync", "status"]
    {
        report_success(
            READ_SESSION_SYNC_STATUS,
            &json_output::read_session_sync_status_document(READ_SESSION_SYNC_STATUS),
            input.output,
        );
        return true;
    }
    if line.starts_with(".sync") {
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
        if !handle_explain_command(session_handle, line, input) {
            session.had_error = true;
        }
    } else {
        let outcome = handle_meta_command(
            session_handle,
            sync_client,
            rt,
            line,
            input,
            sync_plugin,
            session,
        );
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
    session_handle: &Session,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    input: InputContext,
    sync_plugin: Option<&SyncPlugin>,
    session: &mut SessionState,
) -> MetaCommandOutcome {
    let mut parts = line.splitn(2, ' ');
    let cmd = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();

    match cmd {
        ".cursor" => return handle_cursor_command(session_handle, rest, input, session),
        ".owner" => return handle_owner_command(session_handle, rest, input),
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
            let Ok(continuation) = paged_metadata_continuation(rest) else {
                return refuse_continuation(".tables", input);
            };
            return answer_metadata(
                session_handle,
                MetadataRequest::Tables,
                continuation,
                input,
                session,
            );
        }
        ".schema" | "\\d" => {
            if rest.is_empty() {
                report_failure(
                    ErrorClass::Usage,
                    "Usage: .schema <table> or \\d <table>",
                    input,
                );
                return MetaCommandOutcome::failed();
            }
            return answer_metadata(
                session_handle,
                MetadataRequest::Schema {
                    table: rest.to_owned(),
                },
                None,
                input,
                session,
            );
        }
        ".maintenance" => {
            if rest == "status" {
                return answer_metadata(
                    session_handle,
                    MetadataRequest::MaintenanceStatus,
                    None,
                    input,
                    session,
                );
            }
            // Running a cycle and compacting CHANGE the store, so they are the
            // live database's to do; only a writing session ever reaches here,
            // because the authorization seam refused the others already.
            let Some(db) = session_handle.database() else {
                input
                    .output
                    .report_read_failure(&write_requires_flag(), input.script_line);
                return MetaCommandOutcome::failed();
            };
            match rest {
                "run" => match db.run_maintenance_cycle() {
                    Ok(report) => {
                        if input.output.json {
                            json_output::print_document(&json_output::maintenance_report_document(
                                &report,
                            ));
                        } else {
                            println!(
                                "pruned_rows={} rows_deferred_for_readers={} \
                                 currency_pruned_versions={} \
                                 pruned_trigger_audit_rows={} auto_compact_ran={}",
                                report.pruning.pruned_rows,
                                report.pruning.rows_deferred_for_readers,
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
            }
        }
        ".events" => {
            let Some(tail) = rest.strip_prefix("status") else {
                report_failure(ErrorClass::Usage, "Usage: .events status", input);
                return MetaCommandOutcome::failed();
            };
            let Ok(continuation) = paged_metadata_continuation(tail.trim()) else {
                return refuse_continuation(".events status", input);
            };
            return answer_metadata(
                session_handle,
                MetadataRequest::EventsStatus,
                continuation,
                input,
                session,
            );
        }
        ".sync" => {
            let outcome = handle_sync_command(sync_client, rt, rest, sync_plugin);
            if !outcome.ok {
                report_failure(outcome.class, &outcome.message, input);
                return MetaCommandOutcome::failed();
            }
            report_success(&outcome.message, &outcome.json, input.output);
        }
        _ => {
            // The whole line, not just the first word: someone who typed a
            // removed spelling with its argument reads back exactly what they
            // typed, and a proof that a spelling is gone can look for it.
            report_failure(
                ErrorClass::Usage,
                &format!("Unknown command: {}. Type \\? for help.", line.trim()),
                input,
            );
            return MetaCommandOutcome::failed();
        }
    }

    MetaCommandOutcome::done()
}

/// Tells the person what a still-running read has done, and no more often
/// than they asked to hear it.
///
/// The engine reports as it advances; WHETHER that is worth saying is this
/// side's decision, because the threshold is a flag the caller set. So the
/// first report is withheld until the read has been going longer than the
/// threshold, and one line is printed per threshold after that — a read that
/// finishes quickly says nothing at all, which is the point.
pub struct ReadProgressReporter {
    output: OutputOptions,
    threshold: std::time::Duration,
    started: std::time::Instant,
    last_said: Mutex<Option<std::time::Instant>>,
}

impl ReadProgressReporter {
    pub fn new(output: OutputOptions, threshold_ms: u64) -> Self {
        Self {
            output,
            threshold: std::time::Duration::from_millis(threshold_ms),
            started: std::time::Instant::now(),
            last_said: Mutex::new(None),
        }
    }

    /// Whether enough time has passed since the last thing said to say another.
    fn due(&self, now: std::time::Instant) -> bool {
        let Ok(mut last) = self.last_said.lock() else {
            return false;
        };
        let since = match *last {
            Some(previous) => now.duration_since(previous),
            None => now.duration_since(self.started),
        };
        if since < self.threshold {
            return false;
        }
        *last = Some(now);
        true
    }
}

impl contextdb_engine::ReadProgressObserver for ReadProgressReporter {
    fn progress(&self, progress: contextdb_engine::ReadProgress) {
        let now = std::time::Instant::now();
        if !self.due(now) {
            return;
        }
        let document = match progress.phase {
            contextdb_engine::ReadPhase::Hydrating => {
                json_output::hydration_notice_document(progress.loaded_bytes, progress.total_bytes)
            }
            contextdb_engine::ReadPhase::Executing => {
                json_output::statement_progress_notice_document(
                    progress.active_ms,
                    progress.rows,
                    progress.bytes,
                )
            }
        };
        if self.output.json {
            json_output::print_stderr_document(&document);
        } else if let Some(message) = document
            .get("notice")
            .and_then(|notice| notice.get("message"))
            .and_then(|message| message.as_str())
        {
            eprintln!("{message}");
        }
    }
}

/// Say once, at the first store-reading command, HOW this session reads.
///
/// Only a session that CHOSE a route has one to report. A writing session is
/// the owner: it reads its own live state in its own process, with no file-or-
/// owner decision behind it and therefore nothing to announce. `.help`,
/// `.trace`, `.quit`, `.exit`, `.sync status` and `.owner status` read no rows,
/// so none of them reaches here.
fn note_read_route(session_handle: &Session, input: InputContext, session: &mut SessionState) {
    if session.route_reported {
        return;
    }
    let Session::Reading { reader, .. } = session_handle else {
        return;
    };
    session.route_reported = true;
    let document = json_output::read_route_notice_document(reader.route(), reader.snapshot_at());
    if input.output.json {
        json_output::print_stderr_document(&document);
    } else if let Some(message) = document
        .get("notice")
        .and_then(|notice| notice.get("message"))
        .and_then(|message| message.as_str())
    {
        eprintln!("{message}");
    }
}

/// Ask the store about itself through the one metadata door, and publish what
/// it answers.
///
/// Every route goes through here. The door answers the same body whether it
/// was projected from a committed file, from the writer's own live state in
/// this process, or from an owner over a channel — so `.tables` and `.schema`
/// cannot say different things about the same store depending on how the
/// session happened to reach it.
fn answer_metadata(
    session_handle: &Session,
    request: MetadataRequest,
    continuation: Option<&str>,
    input: InputContext,
    session: &mut SessionState,
) -> MetaCommandOutcome {
    note_read_route(session_handle, input, session);
    match session_handle.reader().metadata(request, continuation) {
        Ok(answer) => {
            publish_metadata(&answer, input);
            MetaCommandOutcome::done()
        }
        Err(error) => {
            report_error(&error, input);
            MetaCommandOutcome::failed()
        }
    }
}

/// One metadata answer, rendered for whoever is reading.
fn publish_metadata(answer: &contextdb_engine::MetadataAnswer, input: InputContext) {
    let json = input.output.json;
    match &answer.body {
        MetadataBody::Tables { items, has_more } => {
            let continuation = answer.continuation.as_deref();
            if json {
                json_output::print_document(&json_output::tables_document(
                    items.clone(),
                    *has_more,
                    continuation,
                ));
            } else {
                for name in items {
                    println!("{name}");
                }
                println!("has_more: {has_more}");
                // The follow-up is printed as a command, not described as one,
                // so the instruction is executable exactly as it appears.
                if let Some(token) = continuation {
                    println!(".tables --continue {token}");
                }
            }
        }
        MetadataBody::EventsStatus {
            status,
            has_more,
            continuation,
        } => {
            // The continuation rides beside the answer on the route that pages;
            // where it rides inside the body instead, that is still the same
            // continuation, and a caller must never see two different answers.
            let resume = answer.continuation.as_deref().or(continuation.as_deref());
            if json {
                json_output::print_document(&json_output::events_status_body_document(
                    status, *has_more, resume,
                ));
            } else {
                print_direct_events_status(status);
                println!("has_more: {has_more}");
                if let Some(token) = resume {
                    println!(".events status --continue {token}");
                }
            }
        }
        MetadataBody::Schema { schema } => {
            if json {
                json_output::print_document(&json_output::schema_document(schema));
            } else {
                print!("{}", schema.ddl);
                if !schema.ddl.ends_with('\n') {
                    println!();
                }
            }
        }
        MetadataBody::MaintenanceStatus { status } => {
            if json {
                json_output::print_document(&json_output::maintenance_status_body_document(status));
            } else {
                println!(
                    "running={} retention_enabled={} currency_compaction_enabled={} \
                     active_maintenance_loops={} policy={}",
                    status.running,
                    status.retention_enabled,
                    status.currency_compaction_enabled,
                    status.active_maintenance_loops,
                    status.policy,
                );
            }
        }
        MetadataBody::Explain {
            physical_plan,
            index,
            ..
        } => {
            if json {
                json_output::print_document(&json_output::static_plan_document(physical_plan));
            } else {
                println!("{physical_plan}");
                if let Some(index) = index {
                    println!("index: {index}");
                }
            }
        }
        // The committed image state is not a command anyone can type; the CLI
        // never asks for it, so there is nothing here to render.
        MetadataBody::ImageState { .. } => {}
    }
}

/// The argument tail of a paged metadata command: the continuation it was
/// given, if any.
///
/// A malformed tail is refused as the misuse it is rather than being read as a
/// fresh listing, because a caller that believes it is resuming and is in fact
/// starting over reads the inventory twice and never knows.
fn paged_metadata_continuation(rest: &str) -> std::result::Result<Option<&str>, ()> {
    if rest.is_empty() {
        return Ok(None);
    }
    let token = rest.strip_prefix("--continue ").ok_or(())?.trim();
    if token.is_empty() {
        Err(())
    } else {
        Ok(Some(token))
    }
}

/// Refuse a continuation the command cannot use.
///
/// A malformed `--continue` is caller error, not an empty listing: a caller
/// that believes it is resuming and is in fact starting over reads the whole
/// inventory twice and never learns it did.
fn refuse_continuation(command: &str, input: InputContext) -> MetaCommandOutcome {
    let failure = ReadFailure::invalid_continuation(format!(
        "{command} resumes only with the continuation it issued"
    ));
    input
        .output
        .report_read_failure(&failure, input.script_line);
    MetaCommandOutcome::failed()
}

/// `.owner status` — the file-backed process owner's state, as control data.
///
/// It resolves no row-reading route: it describes the owner, it does not
/// establish a way to read rows, and it stays answerable when every reader
/// slot is occupied. A writing session is its own owner and answers about
/// itself; an inspecting session asks the store's path, which answers whether
/// or not anyone owns it.
fn handle_owner_command(
    session_handle: &Session,
    rest: &str,
    input: InputContext,
) -> MetaCommandOutcome {
    if rest != "status" {
        report_failure(ErrorClass::Usage, "Usage: .owner status", input);
        return MetaCommandOutcome::failed();
    }
    let owned = match session_handle.database() {
        Some(database) => Some(contextdb_engine::OwnerReport {
            status: database.owner_read_status(),
            // A writer answering about itself reports its state; the serving
            // detail is what the CHANNEL computes, and this session is not
            // talking to one.
            serving: None,
        }),
        None => {
            let path = session_handle
                .read_path()
                .expect("a session with no database was pointed at a path");
            match ReadSession::owner_report_in_runtime_dir(
                path,
                session_handle.reader().options(),
                session_handle.reader().runtime_dir(),
            ) {
                Ok(report) => Some(report),
                // Nobody owning the store is the ANSWER to `.owner status`, not
                // a failure to get one: an idle store reports that it has no
                // process owner, and that report succeeds.
                Err(Error::ReadFailure(failure))
                    if failure.kind() == ReadFailureKind::OwnerNotRunning =>
                {
                    None
                }
                Err(error) => {
                    report_error(&error, input);
                    return MetaCommandOutcome::failed();
                }
            }
        }
    };
    let document = match &owned {
        Some(report) => json_output::owner_report_document(report),
        None => json_output::owner_not_running_document(),
    };
    if input.output.json {
        json_output::print_document(&document);
    } else {
        let state = match &owned {
            Some(report) => json_output::owner_serving_state_wire_word(report.status.state),
            None => "not_running",
        };
        println!("owner: {state}");
    }
    // An owner that was expected to serve and cannot is a failed report, not a
    // successful one: a script that reads the state must also be able to read
    // it off the exit code.
    if owned
        .as_ref()
        .is_some_and(|report| report.status.state == OwnerServingState::NotServing)
    {
        return MetaCommandOutcome::failed();
    }
    MetaCommandOutcome::done()
}

/// `.cursor open` / `.cursor fetch` / `.cursor close` — the one cursor a
/// session may hold.
fn handle_cursor_command(
    session_handle: &Session,
    rest: &str,
    input: InputContext,
    session: &mut SessionState,
) -> MetaCommandOutcome {
    let mut parts = rest.splitn(2, ' ');
    let verb = parts.next().unwrap_or("");
    let argument = parts.next().unwrap_or("").trim();
    match verb {
        "open" => cursor_open(session_handle, argument, input, session),
        "fetch" => cursor_fetch(session_handle, argument, input, session),
        "close" => cursor_close(input, session),
        _ => {
            report_failure(
                ErrorClass::Usage,
                "Usage: .cursor open <SELECT> | .cursor fetch [rows] | .cursor close",
                input,
            );
            MetaCommandOutcome::failed()
        }
    }
}

/// Refuse a cursor request, and say so in the words the refusal itself carries.
fn refuse_cursor(kind: ReadFailureKind, input: InputContext) -> MetaCommandOutcome {
    let failure = ReadFailure::new(kind, ReadFailureDetail::None)
        .expect("every cursor refusal used here carries no specialized detail");
    input
        .output
        .report_read_failure(&failure, input.script_line);
    MetaCommandOutcome::failed()
}

/// Publish one page and record what the cursor became. A page that exhausted
/// the read frees the session's cursor slot without throwing the cursor away,
/// so the next fetch still answers where it left off — at the end.
fn publish_cursor_page(
    page: &contextdb_core::read_contract::CursorPage,
    cursor: ReadCursor,
    input: InputContext,
    session: &mut SessionState,
) -> MetaCommandOutcome {
    session.cursor = if page.has_more {
        CursorSlot::Open(cursor)
    } else {
        CursorSlot::Drained(cursor)
    };
    if input.output.json {
        json_output::print_document(&json_output::cursor_page_document(page));
    } else {
        for row in &page.rows {
            let cells: Vec<String> = row.iter().map(crate::formatter::render_value).collect();
            println!("{}", cells.join(" | "));
        }
        println!("({} rows, has_more: {})", page.rows.len(), page.has_more);
    }
    MetaCommandOutcome::done()
}

fn cursor_open(
    session_handle: &Session,
    argument: &str,
    input: InputContext,
    session: &mut SessionState,
) -> MetaCommandOutcome {
    if matches!(session.cursor, CursorSlot::Open(_)) {
        return refuse_cursor(ReadFailureKind::CursorAlreadyOpen, input);
    }
    if argument.is_empty() {
        report_failure(ErrorClass::Usage, "Usage: .cursor open <SELECT>", input);
        return MetaCommandOutcome::failed();
    }
    // Cursor state must never mix committed and uncommitted rows, so it may
    // not open across a transaction — and refusing must leave that
    // transaction exactly as it found it.
    if session.transaction_open {
        return refuse_cursor(ReadFailureKind::CursorTransactionActive, input);
    }
    // Exactly one read-only SELECT. A write handed to the cursor is refused AS
    // a write, so the user is told the one thing that would let it through;
    // anything else is command misuse and never executes.
    match contextdb_parser::parse(argument) {
        Ok(statement) => {
            if contextdb_parser::statement_effect(&statement)
                != contextdb_parser::StatementEffect::Read
            {
                return refuse_cursor(ReadFailureKind::WriteRequiresFlag, input);
            }
            if !matches!(statement, contextdb_parser::Statement::Select(_)) {
                return refuse_cursor(ReadFailureKind::CursorInvalidStatement, input);
            }
        }
        Err(_) => return refuse_cursor(ReadFailureKind::CursorInvalidStatement, input),
    }
    note_read_route(session_handle, input, session);
    match session_handle
        .reader()
        .open_cursor(argument, &HashMap::new())
    {
        Ok(cursor) => {
            let page = cursor.first_page().clone();
            session.cursor_statement =
                Some(argument.trim().trim_end_matches(';').trim().to_owned());
            publish_cursor_page(&page, cursor, input, session)
        }
        Err(error) => {
            report_error(&error, input);
            MetaCommandOutcome::failed()
        }
    }
}

fn cursor_fetch(
    session_handle: &Session,
    argument: &str,
    input: InputContext,
    session: &mut SessionState,
) -> MetaCommandOutcome {
    let rows = if argument.is_empty() {
        None
    } else {
        match argument.parse::<usize>().ok().and_then(NonZeroUsize::new) {
            Some(rows) => Some(rows),
            None => {
                report_failure(
                    ErrorClass::Usage,
                    "Usage: .cursor fetch [rows], where rows is a positive whole number",
                    input,
                );
                return MetaCommandOutcome::failed();
            }
        }
    };
    let mut cursor = match std::mem::take(&mut session.cursor) {
        CursorSlot::Open(cursor) | CursorSlot::Drained(cursor) => cursor,
        // A cursor that never existed and one this session explicitly closed
        // are the same answer: there is nothing here to fetch from. That is
        // distinct from a DRAINED cursor, which answers an empty success page.
        CursorSlot::Absent | CursorSlot::Closed => {
            session.cursor = CursorSlot::Closed;
            return refuse_cursor(ReadFailureKind::CursorNotFound, input);
        }
    };
    let cancellation = OwnerReadCancellation::new();
    let fetched = {
        let _running = session
            .interrupts
            .as_ref()
            .map(|holder| holder.running(&cancellation));
        cursor.fetch_with_cancellation(rows, &cancellation)
    };
    match fetched {
        Ok(page) => publish_cursor_page(&page, cursor, input, session),
        // A cancelled fetch leaves the cursor exactly where it was, so the
        // next fetch resumes with nothing repeated or skipped.
        Err(Error::ReadCancelled) => {
            session.cursor = CursorSlot::Open(cursor);
            input
                .output
                .report_notice(ErrorClass::Io, "cursor fetch cancelled");
            MetaCommandOutcome::done()
        }
        Err(error) => {
            // The slot must say what is actually there. A refusal the cursor
            // survived leaves it open for the next fetch; a refusal that ENDED
            // the read -- a crossed ceiling gives its retained bytes back and
            // unpins its snapshot rather than waiting for a caller to remember
            // to close it -- leaves nothing, and a slot that went on claiming
            // it would answer `cursor_already_open` to the next `.cursor open`
            // while every fetch and close said the cursor was gone. That is a
            // session with no cursor and no way to get one, so the slot is
            // freed instead: exactly the state an explicit close leaves, from
            // which `.cursor open` works.
            session.cursor = if cursor.is_live() {
                CursorSlot::Open(cursor)
            } else {
                CursorSlot::Closed
            };
            report_cursor_error(session_handle, &error, input, session);
            MetaCommandOutcome::failed()
        }
    }
}

/// Report a refused fetch the way the ordinary read surface reports one.
///
/// A crossed ceiling names a different way out on each route -- a direct reader
/// owns its own ceilings, a reader through an owner does not -- so the refusal
/// is told by a session that knows which route it read on. Reporting it
/// route-blind here would hand a cursor user the bare ceiling and no step to
/// take, while the same refusal on an ordinary SELECT names one.
fn report_cursor_error(
    session_handle: &Session,
    error: &Error,
    input: InputContext,
    session: &SessionState,
) {
    if let Error::ReadFailure(failure) = error
        && failure.kind() == ReadFailureKind::OwnerLimitExceeded
    {
        // A count above the row ceiling refused the REQUEST, and the ceiling
        // it names IS the count this cursor will serve. So the remedy is that
        // exact fetch, not a flag change: the cursor is still open and the
        // read has not moved. Raising `--read-*` is the escape for the
        // TERMINAL ceilings, and offering it here would send the caller to
        // restart a session they never lost.
        if let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail()
            && detail.limit == ReadFailureLimit::ResultRows
        {
            let remedied = ReadFailure::owner_limit_exceeded(
                contextdb_core::read_contract::OwnerLimitExceededDetail {
                    limit: detail.limit,
                    value: detail.value,
                    required: detail.required.clone(),
                    statement: Some(contextdb_core::read_contract::StatementRemedy {
                        statement: session.cursor_statement.clone().unwrap_or_default(),
                        remedy_command: format!(".cursor fetch {}", detail.value),
                    }),
                },
            );
            input
                .output
                .report_read_failure(&remedied, input.script_line);
            return;
        }
        input.output.report_read_failure_on_route(
            failure,
            input.script_line,
            Some(session_handle.reader().route()),
        );
        return;
    }
    report_error(error, input);
}

fn cursor_close(input: InputContext, session: &mut SessionState) -> MetaCommandOutcome {
    let slot = std::mem::take(&mut session.cursor);
    session.cursor = CursorSlot::Closed;
    let outcome = match slot {
        // Closing past the end is a no-op success, so a fetch-until-empty loop
        // that ends with a close always exits clean.
        CursorSlot::Absent | CursorSlot::Closed | CursorSlot::Drained(_) => Ok(()),
        CursorSlot::Open(cursor) => cursor.close(),
    };
    match outcome {
        Ok(()) => {
            if input.output.json {
                json_output::print_document(&json_output::cursor_closed_document());
            } else {
                println!("cursor closed");
            }
            MetaCommandOutcome::done()
        }
        Err(error) => {
            report_error(&error, input);
            MetaCommandOutcome::failed()
        }
    }
}

/// The help text for a topic, one line per entry. Rendering is the caller's
/// business — stdout for a human, one JSON document on stderr under `--json`.
///
/// The general list is GENERATED from the command registry, so `.help` can
/// neither offer a command the CLI does not have nor omit one it does. The
/// gloss beside each spelling is prose; the spelling itself is never written
/// down twice.
fn help_lines(topic: &str) -> Vec<String> {
    if topic.eq_ignore_ascii_case("vector") {
        return [
            "VECTOR(N) WITH (quantization = 'F32'|'SQ8'|'SQ4')",
            "SHOW VECTOR_INDEXES",
            "ORDER BY embedding <=> [0.1, 0.2, 0.3] LIMIT 5",
            "ORDER BY embedding <=> ROW_VECTOR('table', 'embedding', $key) LIMIT 5",
            "Vector errors include LegacyVectorStoreDetected, StoreCorrupted, VectorIndexDimensionMismatch, UnknownVectorIndex, PersistedRowVectorRowMissing, and PersistedRowVectorCellNull",
        ]
        .iter()
        .map(|line| (*line).to_owned())
        .collect();
    }
    if topic.eq_ignore_ascii_case("propagate") {
        return [
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
        ]
        .iter()
        .map(|line| (*line).to_owned())
        .collect();
    }
    canonical_help_signatures()
        .into_iter()
        .map(|spelling| {
            let gloss = command_gloss(spelling);
            // A toggle is shown the way it is typed; the registry keeps the
            // bare spelling it dispatches on.
            let shown = crate::command_registry::help_signature(spelling);
            if gloss.is_empty() {
                shown.to_owned()
            } else {
                format!("{shown:<22}{gloss}")
            }
        })
        .collect()
}

/// What each published spelling is for, in one line a person can act on.
///
/// A spelling with no gloss still appears in `.help` — it is a command the CLI
/// has, and hiding it because nobody wrote a sentence would make the discovery
/// surface lie.
fn command_gloss(spelling: &str) -> &'static str {
    match spelling {
        ".tables" => "List table names; a large listing resumes with --continue.",
        ".schema" => "Show a table's full declared contract.",
        ".explain" => "Show a statement's execution plan; never applies a write.",
        ".events status" => "Bounded, resumable event/sink/route/schedule health.",
        ".maintenance status" => "One complete bounded maintenance-state response.",
        ".cursor open" => "Page a result larger than an ordinary ceiling.",
        ".cursor fetch" => "Next page; omit the count to use the declared page size.",
        ".cursor close" => "Release this session's cursor.",
        ".maintenance run" => "Run one cleanup cycle. Needs --write.",
        ".maintenance compact" => "Reclaim file space. Needs --write.",
        ".sync push" => "Push local changes to the server. Needs --write.",
        ".sync pull" => "Pull remote changes from the server. Needs --write.",
        ".sync reconnect" => "Reconnect to the sync endpoint. Needs --write.",
        ".sync destination" => "Move retained-data delivery to the connected hub. Needs --write.",
        ".sync auto" => "Toggle auto-sync after DML. Needs --write.",
        ".owner status" => "The file-backed owner's policy and state; works at capacity.",
        ".help" => "Show this message. `.help vector` / `.help propagate` show grammar.",
        ".trace" => "Toggle one-line execution traces.",
        ".quit" | ".exit" => "End the session.",
        ".sync status" => "This CLI session's own sync state, not the store's.",
        "\\dt" => "Alias for .tables.",
        "\\d" => "Alias for .schema.",
        "\\q" => "Alias for .quit.",
        "\\?" => "Alias for .help.",
        "migrate" => "Bring a legacy-format store forward in place.",
        "reset" => "Recreate a wedged or corrupt store. Needs --force.",
        "diagnose" => "Report a store's format and layout, read-only.",
        "snapshot" => "Publish a purge-fenced backup artifact.",
        "inspect" => "Read durable key and media state from an artifact or file.",
        "purge" => "Force-gated whole-table authoritative erasure.",
        _ => "",
    }
}

fn is_trace_command(line: &str) -> bool {
    // `\trace` is not a spelling. The conventional aliases the contract keeps
    // are the psql-shaped ones the registry publishes, and this is not one.
    matches!(line.split_whitespace().next(), Some(".trace"))
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

fn handle_explain_command(session_handle: &Session, line: &str, input: InputContext) -> bool {
    let rest = line.strip_prefix(".explain").unwrap_or("").trim();
    if rest.is_empty() {
        report_failure(ErrorClass::Usage, "Usage: .explain <sql>", input);
        return false;
    }

    // Only a read-only statement may be RUN to collect a real runtime route.
    // Anything that would write is planned instead — `.explain DELETE FROM t`
    // answers what it WOULD do and must never do it — and the plan says
    // plainly that no statement was run to produce it.
    if !explain_can_use_runtime_trace(rest) {
        // A writing session plans a write against its own live database. The
        // bounded read view will not plan one — it accepts read-only plans by
        // design — so asking it would turn "here is what this WOULD do" into a
        // refusal, which is not what `.explain` promises.
        if let Some(database) = session_handle.database() {
            return match database.explain(rest) {
                Ok(plan) => {
                    if input.output.json {
                        json_output::print_document(&json_output::static_plan_document(&plan));
                    } else {
                        print!("{plan}");
                        if !plan.ends_with('\n') {
                            println!();
                        }
                    }
                    true
                }
                Err(error) => {
                    report_error(&error, input);
                    false
                }
            };
        }
        return match session_handle.reader().metadata(
            MetadataRequest::Explain {
                sql: rest.to_owned(),
            },
            None,
        ) {
            Ok(answer) => {
                let MetadataBody::Explain { physical_plan, .. } = &answer.body else {
                    report_error(&Error::ReadSessionNotImplemented, input);
                    return false;
                };
                if input.output.json {
                    json_output::print_document(&json_output::static_plan_document(physical_plan));
                } else {
                    println!("{physical_plan}");
                }
                true
            }
            Err(error) => {
                report_error(&error, input);
                false
            }
        };
    }

    match session_handle.reader().execute(rest, &HashMap::new()) {
        Ok(result) => {
            if input.output.json {
                json_output::print_document(&json_output::explain_document(&result));
            } else {
                // The same shape `.explain` gives for a statement it could
                // only plan: an operator asking about a route gets one answer,
                // whether or not the statement was run to produce it.
                print!(
                    "{}",
                    contextdb_engine::cli_render::render_explain_trace(&result.trace)
                );
            }
            true
        }
        Err(error) => {
            report_error(&error, input);
            false
        }
    }
}

/// Report an engine error: the `{"error":{...}}` envelope on stderr under
/// `--json`, the human `Error: ...` line on stderr otherwise. Never stdout.
fn report_error(error: &Error, input: InputContext) {
    // A read refusal carries its own class and typed detail; rendering it as a
    // bare message would throw away the fields a script branches on.
    if let Error::ReadFailure(failure) = error {
        input.output.report_read_failure(failure, input.script_line);
        return;
    }
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

/// The pull-progress rendering DECISION, pulled out of the sleep-driven
/// ticker loop so it is directly testable: pure, no thread, no sleep, no
/// terminal. `None` when `pages_read` has not moved since the last report
/// (nothing new to say); `Some(line)` naming the current page count when it
/// has.
fn interactive_pull_progress_line(pages_read: u64, last_reported: u64) -> Option<String> {
    if pages_read == last_reported {
        None
    } else {
        Some(format!("Pulling... {pages_read} page(s) read so far"))
    }
}

/// Runs `.sync pull`, streaming periodic liveness signals from the SAME
/// per-call page counter (`SyncClient::pull_pages_read_this_pull`) while it
/// runs, so a caller can tell a long multi-page pull is alive, not stuck.
///
/// A single CLI session blocks for the whole duration of its own pull (one
/// process, one store lock) — polling `.sync status --json` from elsewhere
/// mid-pull is not the story here (see the doc comment on `.sync status`'s
/// `pull_in_progress` field for the actual cross-thread signal a library
/// consumer sharing one `SyncClient` handle gets). This is the in-session
/// liveness story instead: at an interactive terminal with human output,
/// periodic text lines via the pure `interactive_pull_progress_line` seam;
/// under `--json`, periodic `{"sync_pull_progress":{"pages_read":N}}` JSON
/// Lines notices on stderr — which stream regardless of whether stdout is a
/// terminal, since a script or agent piping `--json` needs the same
/// liveness signal a human at a prompt gets. A non-interactive, non-`--json`
/// session (plain text piped through a script) gets neither: nobody is
/// watching stdout as text and nothing parses stderr as JSON there, so
/// there's nothing to usefully stream.
fn pull_with_progress(
    client: &SyncClient,
    rt: &tokio::runtime::Runtime,
) -> Result<contextdb_engine::sync_types::ApplyResult, contextdb_core::Error> {
    let json = json_output_mode();
    let interactive_terminal = std::io::stdin().is_terminal();
    let stream_progress = json || interactive_terminal;
    let progress_stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        if stream_progress {
            scope.spawn(|| {
                let mut last_reported = client.pull_pages_read_this_pull();
                let mut waited = std::time::Duration::ZERO;
                let tick = std::time::Duration::from_millis(200);
                let report_every = std::time::Duration::from_secs(2);
                while !progress_stop.load(std::sync::atomic::Ordering::SeqCst) {
                    std::thread::sleep(tick);
                    waited += tick;
                    if waited < report_every {
                        continue;
                    }
                    waited = std::time::Duration::ZERO;
                    let now = client.pull_pages_read_this_pull();
                    if json {
                        if now != last_reported {
                            json_output::print_stderr_document(
                                &json_output::sync_pull_progress_document(now),
                            );
                            last_reported = now;
                        }
                    } else if let Some(line) = interactive_pull_progress_line(now, last_reported) {
                        eprintln!("{line}");
                        last_reported = now;
                    }
                }
            });
        }
        let result = rt.block_on(client.pull_default());
        progress_stop.store(true, std::sync::atomic::Ordering::SeqCst);
        result
    })
}

fn run_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
    unconfirmed_push: &mut bool,
) -> SyncCommandOutcome {
    let parts: Vec<&str> = args.split_whitespace().collect();
    // `.sync` alone names no operation, and the command registry classifies the
    // bare spelling Invalid exactly like `.events`, `.maintenance`, `.cursor`,
    // and `.owner`. Reading it as one of the operations -- silently, as a
    // status query -- would make the registry and the session disagree about
    // what the word means, so it is answered as a usage error that lists the
    // operations that do exist.
    let Some(sub) = parts.first().copied() else {
        return SyncCommandOutcome::failed(
            ErrorClass::Usage,
            "Usage: .sync status | .sync push | .sync pull | .sync reconnect | \
             .sync destination <hub> | .sync auto on|off"
                .to_string(),
        );
    };

    if !matches!(
        sub,
        "status" | "auto" | "push" | "pull" | "reconnect" | "destination"
    ) {
        return SyncCommandOutcome::failed(
            ErrorClass::Usage,
            format!("Unknown .sync command: {sub}"),
        );
    }

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
            let pull_pages_read = client.pull_pages_read();
            let pull_in_progress = client.pull_in_progress();
            let base = crate::sync_status::render_sync_endpoint_status(&view);
            let render = contextdb_engine::cli_render::render_sync_status(client.db());
            let mut document = crate::sync_status::sync_endpoint_status_json(&view);
            if let Some(object) = document.as_object_mut() {
                object.insert(
                    "committed_txid".to_string(),
                    serde_json::json!(client.db().committed_watermark().0),
                );
                // Liveness signals (not part of the stable watermark
                // vocabulary `sync_status.rs` owns). `pull_pages_read` is
                // the cumulative page count across every pull this client
                // has issued. NOTE the real liveness story here: a single
                // CLI session BLOCKS for the whole duration of its own
                // `.sync pull` (one statement at a time, one process, one
                // store lock), so a script running `.sync status` before
                // and after a pull in the SAME session only ever observes
                // before/after, never during -- it cannot poll this twice
                // mid-pull the way a second, concurrent process might hope
                // to. The actual mid-pull liveness signals are: (1) the
                // periodic `sync_pull_progress` stderr notices a long pull
                // streams while it runs (see `pull_with_progress`), and (2)
                // `pull_in_progress` below, which DOES turn `true` mid-pull
                // -- but only for a caller sharing this exact `SyncClient`
                // handle across threads/tasks (an embedding consumer, not
                // the CLI's own single-blocking-session process).
                object.insert(
                    "pull_pages_read".to_string(),
                    serde_json::json!(pull_pages_read),
                );
                object.insert(
                    "pull_in_progress".to_string(),
                    serde_json::json!(pull_in_progress),
                );
            }
            SyncCommandOutcome::succeeded(
                format!("{base}\nPull pages read: {pull_pages_read}\n{render}"),
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
                    msg.push_str(&format!(
                        "\n  conflict: {}",
                        contextdb_engine::cli_render::render_sync_conflict(conflict)
                    ));
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
            Err(contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key }) => {
                // The hub's typed refusal proves both sides already agree
                // the row is deleted -- benign convergence, not a failure.
                // Same contract as the CLI's own unconditional exit-push.
                SyncCommandOutcome::succeeded(
                    format!(
                        "Push re-offered {table} {key:?}, which the hub already converged on \
                         as deleted; nothing to do."
                    ),
                    serde_json::json!({ "sync_push": { "outcome": "converged" } }),
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
        "pull" => match pull_with_progress(client, rt) {
            Ok(result) => {
                let mut msg = format!(
                    "Pulled: {} applied, {} skipped, {} conflicts",
                    result.applied_rows,
                    result.skipped_rows,
                    result.conflicts.len()
                );
                for conflict in &result.conflicts {
                    msg.push_str(&format!(
                        "\n  conflict: {}",
                        contextdb_engine::cli_render::render_sync_conflict(conflict)
                    ));
                }
                let mut pull_doc = apply_result_json(&result, "applied");
                if let Some(object) = pull_doc.as_object_mut() {
                    // Cumulative across every pull this client has issued.
                    object.insert(
                        "pull_pages_read".to_string(),
                        serde_json::json!(client.pull_pages_read()),
                    );
                    // THIS pull's own page count, distinct from the
                    // cumulative counter above -- a no-op second pull reads
                    // 0 here while the cumulative counter holds steady, so
                    // a caller can tell "this pull did nothing" from "this
                    // pull did a lot" without diffing two cumulative reads.
                    object.insert(
                        "pull_pages_read_this_pull".to_string(),
                        serde_json::json!(client.pull_pages_read_this_pull()),
                    );
                }
                SyncCommandOutcome::succeeded(msg, serde_json::json!({ "sync_pull": pull_doc }))
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
/// complete typed conflict receipt — the same document the human line reports.
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
            .map(contextdb_engine::cli_render::sync_conflict_document)
            .collect::<Vec<_>>(),
        "outcome": outcome,
    })
}

fn auto_sync_json(enabled: bool) -> serde_json::Value {
    serde_json::json!({ "sync_auto": { "configured": true, "enabled": enabled } })
}

/// Human rendering of `.events status`: every declared event type, sink (with
/// its delivery counters), route, and schedule (with its fire count).
fn print_direct_events_status(status: &contextdb_engine::DirectEventsStatus) {
    println!("Event types:");
    if status.event_types.is_empty() {
        println!("  (none)");
    }
    for event_type in &status.event_types {
        println!(
            "  {} WHEN {} ON {}",
            event_type.name, event_type.trigger, event_type.table
        );
    }
    println!("Sinks:");
    if status.sinks.is_empty() {
        println!("  (none)");
    }
    for sink in &status.sinks {
        println!(
            "  {} TYPE {} registered={} delivered={} queued={} retried={} \
             permanent_failures={} examined={}",
            sink.name,
            sink.sink_type,
            sink.callback_registered,
            sink.delivered,
            sink.queued,
            sink.retried,
            sink.permanent_failures,
            sink.examined,
        );
    }
    println!("Routes:");
    if status.routes.is_empty() {
        println!("  (none)");
    }
    for route in &status.routes {
        println!(
            "  {} EVENT {} TO {}",
            route.name, route.event_type, route.sink
        );
    }
    println!("Schedules:");
    if status.schedules.is_empty() {
        println!("  (none)");
    }
    for schedule in &status.schedules {
        println!(
            "  {} EVERY {} TX ({}) registered={} fired={} next_fire_at_ms={} \
             last_fire_at_ms={:?}",
            schedule.name,
            schedule.every,
            schedule.callback,
            schedule.callback_registered,
            schedule.fire_count,
            schedule.next_fire_at_ms,
            schedule.last_fire_at_ms,
        );
    }
}

/// Execute a SQL statement and print the result. Returns `true` on success, `false` on error.
/// After a statement the CLI's own statement-execution path just ran
/// successfully, register the CLI's own zero-config default for whatever
/// needs an external callback to do anything observable: `CREATE SINK` gets
/// a delivery callback so a routed event actually delivers (the engine's
/// queue-until-registered contract — acceptance `t5_26` — is untouched,
/// since this registers only AFTER the sink already exists, same as any
/// other consumer would); `CREATE SCHEDULE` gets a cron callback so a
/// declared schedule fires. `.events status` already renders the resulting
/// delivered/queued/fire_count counters, so the default is deliberately a
/// quiet no-op rather than printing anything itself -- a background
/// dispatcher thread writing ad hoc lines to stderr at an unpredictable time
/// would violate the `--json` stderr-is-JSON-Lines-only contract
/// (`json_stderr_purity_tests.rs`).
///
/// This is a CLI-layer convenience, not an engine default: an embedding
/// consumer (context-graph, vigil) that opens a `Database` directly and
/// never calls through `feed_line`/`execute_sql` never sees it — only a
/// session driven through the CLI's real per-line path does. Best-effort:
/// a parse failure or a registration error must never turn an
/// already-successful DDL statement into a failure the operator sees.
/// Ephemeral (`:memory:`) sessions only -- see `Database::is_memory_backed`'s
/// doc comment. A file-backed store is durable-until-explicitly-registered
/// on purpose: a CLI script is routinely used to seed schema/data for a
/// SEPARATE, later process (its own Rust program, possibly principal- or
/// context-scoped) to open and register the real callback against; eagerly
/// registering and draining here would consume those durably-queued events
/// out from under that later, legitimate registration before it ever gets a
/// chance to run (acceptance `t5_16`/`t5_06`/`t5_13`/`t5_15`/`t5_31` all
/// depend on this). An interactive/piped `:memory:` session has no later
/// process to hand off to -- it IS the whole session -- so it gets the
/// zero-config convenience instead.
fn register_cli_defaults_for_statement(db: &Database, sql: &str) {
    if !db.is_memory_backed() {
        return;
    }
    let Ok(statement) = contextdb_parser::parse(sql) else {
        return;
    };
    match statement {
        contextdb_parser::Statement::CreateSink { name, .. } => {
            let _ = db.register_sink(&name, None, |_event| Ok(()));
        }
        contextdb_parser::Statement::CreateSchedule { callback, .. } => {
            let _ = db.register_cron_callback(&callback, |_db| Ok(()));
        }
        _ => {}
    }
}

/// Briefly wait for a sink the CLI's own statement path registered a
/// callback for to drain to zero queued after a statement, so the operator
/// sees delivery settle before the next prompt/statement rather than racing
/// an unpredictable background dispatcher-thread wakeup. Bounded: a sink
/// that will never drain (an ACL-denied item, a slow external callback that
/// isn't ours) gives up after the bound instead of hanging the session.
/// Costs nothing when nothing is queued -- the common case. Ephemeral
/// (`:memory:`) sessions only, for the same reason
/// `register_cli_defaults_for_statement` is scoped that way.
fn settle_cli_registered_sinks(db: &Database) {
    if !db.is_memory_backed() {
        return;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        let still_settling = db
            .event_bus_status()
            .sinks
            .iter()
            .any(|sink| sink.callback_registered && sink.metrics.queued > 0);
        if !still_settling || std::time::Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

fn execute_sql(
    session_handle: &Session,
    sql: &str,
    input: InputContext,
    session: &mut SessionState,
) -> bool {
    let output = input.output;
    let statement = contextdb_parser::parse(sql).ok();
    let writes = statement.as_ref().is_some_and(|statement| {
        contextdb_parser::statement_effect(statement) != contextdb_parser::StatementEffect::Read
    });
    // The answer comes BEFORE execution. A session that may not write must not
    // reach the store with a mutating statement at all, so nothing partial can
    // be left behind by a statement that was never allowed to run.
    if writes && !session_handle.writes_permitted() {
        output.report_read_failure(&write_requires_flag(), input.script_line);
        return false;
    }

    note_read_route(session_handle, input, session);
    let cancellation = OwnerReadCancellation::new();
    let executed = match session_handle.database() {
        // A write has one door, and it is the live database.
        Some(database) if writes => database.execute(sql, &HashMap::new()),
        // Every read takes the bounded door, including a writer's own read
        // inside the transaction it opened itself — reading your own
        // uncommitted rows and staying under a ceiling are not in tension, so
        // a writer's SELECTs are bounded exactly like anyone else's. Input
        // that parses as nothing takes this door too, so a parse error reads
        // identically in both kinds of session.
        _ => {
            let _running = session
                .interrupts
                .as_ref()
                .map(|holder| holder.running(&cancellation));
            session_handle
                .reader()
                .execute_with_cancellation(sql, &HashMap::new(), &cancellation)
        }
    };

    match executed {
        Ok(result) => {
            if let Some(database) = session_handle.database() {
                register_cli_defaults_for_statement(database, sql);
                settle_cli_registered_sinks(database);
            }
            if let Some(statement) = &statement {
                session.transaction_open = match statement {
                    contextdb_parser::Statement::Begin => true,
                    contextdb_parser::Statement::Commit | contextdb_parser::Statement::Rollback => {
                        false
                    }
                    _ => session.transaction_open,
                };
            }
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
                json_output::print_document(&json_output::result_document(&result));
            } else {
                let show_empty_headers = sql
                    .trim()
                    .trim_end_matches(';')
                    .eq_ignore_ascii_case("SHOW VECTOR_INDEXES");
                let formatted = format_query_result_with_empty_headers(&result, show_empty_headers);
                if !formatted.is_empty() {
                    println!("{formatted}");
                }
                // One footer, and it is the last thing a successful ordinary
                // result prints. There is no other decoration: a result is
                // complete or it is refused.
                println!("({} rows)", result.rows.len());
            }
            if session.trace_enabled {
                if output.json {
                    // The trace is diagnostics about a result, not the result:
                    // on stdout it would corrupt the JSON Lines stream a
                    // consumer is parsing.
                    json_output::print_trace(&result.trace, result.trace.rows_examined);
                } else {
                    println!(
                        "{}",
                        contextdb_engine::cli_render::render_query_trace(
                            &result.trace,
                            result.trace.rows_examined
                        )
                    );
                }
            }
            true
        }
        // Cancelling is what the person asked for, not a failure of the run:
        // the statement publishes nothing, the session carries on, and the
        // exit code says nothing went wrong.
        Err(Error::ReadCancelled) => {
            output.report_notice(ErrorClass::Io, "statement cancelled");
            true
        }
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::OwnerLimitExceeded =>
        {
            output.report_read_failure_on_route(
                &refusal_naming_the_statement(&failure, sql),
                input.script_line,
                Some(session_handle.reader().route()),
            );
            false
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
    use contextdb_parser::{Statement, parse};

    /// A writing session over a throwaway in-memory database — what these
    /// fixtures drive, and the mode `:memory:` always has.
    fn writing_session(database: &Arc<Database>) -> Session {
        Session::writing(Arc::clone(database), ReadLimits::default())
            .expect("a live database opens its own bounded read view")
    }

    /// A scripted, human-output context — what the meta-command tests below
    /// drive, so they exercise the same path a piped session takes.
    fn scripted_input() -> InputContext {
        InputContext {
            interactive: false,
            script_line: None,
            output: OutputOptions::default(),
            store_writes_permitted: true,
        }
    }

    #[test]
    fn maintenance_status_reports_a_database_with_nothing_declared() {
        let db = Arc::new(Database::open_memory());
        let outcome = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            ".maintenance status",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
        assert!(outcome.keep_going && outcome.ok);
    }

    #[test]
    fn maintenance_run_drives_one_cycle_on_demand() {
        let db = Arc::new(Database::open_memory());
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY) RETAIN 1 HOURS",
            &HashMap::new(),
        )
        .unwrap();
        let outcome = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            ".maintenance run",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
        assert!(outcome.keep_going && outcome.ok);
    }

    #[test]
    fn maintenance_with_no_subcommand_is_a_usage_error() {
        let db = Arc::new(Database::open_memory());
        let outcome = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            ".maintenance",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
        assert!(outcome.keep_going && !outcome.ok);
    }

    // -----------------------------------------------------------------
    // D3 — trigger/event/schedule CLI door (owner-ruled 2026-08-06).
    //
    // `CREATE EVENT TYPE`/`SINK`/`ROUTE`/`SCHEDULE` parse and the engine
    // executes them, but nothing in the CLI's statement path ever calls
    // `register_sink`/`register_cron_callback`/`complete_initialization`.
    // These three tests drive the exact statements an operator can type —
    // `db.execute` is what the REPL's main loop calls for every non-meta
    // line — and prove there is today no CLI-only way to observe a
    // route/sink deliver or a schedule fire.
    // -----------------------------------------------------------------

    #[test]
    fn events_status_meta_command_is_unrecognized_today() {
        // An operator who has declared a route/sink/schedule needs a way to
        // see what's registered and whether it has ever delivered/fired. No
        // such introspection meta-command exists yet, so `.events status`
        // falls through to the generic "Unknown command" usage error.
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &HashMap::new())
            .unwrap();
        db.execute("CREATE EVENT TYPE t_ins WHEN INSERT ON t", &HashMap::new())
            .unwrap();
        db.execute("CREATE SINK s TYPE callback", &HashMap::new())
            .unwrap();
        db.execute("CREATE ROUTE r EVENT t_ins TO s", &HashMap::new())
            .unwrap();

        let outcome = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            ".events status",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
        assert!(
            outcome.ok,
            "an operator must be able to list registered event types/sinks/routes/\
             schedules and their delivery/fire counters via a `.events status`-style \
             meta-command; got a usage error instead, meaning no such introspection \
             command exists yet"
        );
    }

    // -----------------------------------------------------------------
    // CLI pull liveness (owner-folded correction, 2026-08-06), interactive
    // printer half. `pull_with_interactive_progress`'s own doc comment
    // admits it is "deliberately untested (a wall-clock-timed background
    // printer is not worth pinning in a test)" — that stance is now
    // folded/overridden: the printer's RENDERING decision (does pages_read
    // having moved since the last report produce a new line, and what does
    // that line say) is pure and must be pulled out from under the
    // sleep-driven ticker loop into its own deterministic, directly testable
    // function. Proposed shape (implementer may rename/restructure —
    // e.g. as an injectable `Write` sink plus tick source instead — the
    // load-bearing requirement is just that the RENDER DECISION itself is
    // reachable without a real thread, a real sleep, or a real terminal):
    //
    //   fn interactive_pull_progress_line(pages_read: u64, last_reported: u64) -> Option<String>
    //
    // returning `None` when nothing has changed since the last report, and
    // `Some(line)` when it has — so `pull_with_interactive_progress`'s
    // ticker loop becomes a thin driver over this pure function instead of
    // an inline, unpinnable `if now != last { eprintln!(...) }`.
    // -----------------------------------------------------------------

    #[test]
    fn interactive_pull_progress_line_is_silent_when_pages_read_has_not_moved() {
        assert_eq!(
            interactive_pull_progress_line(3, 3),
            None,
            "no new page has been read since the last report, so there is nothing new to say"
        );
    }

    #[test]
    fn interactive_pull_progress_line_reports_when_pages_read_advances() {
        let line = interactive_pull_progress_line(5, 3).unwrap_or_else(|| {
            panic!(
                "pages_read advanced from 3 to 5 since the last report; the deterministic \
                 rendering seam must produce a line an operator can read, not silence"
            )
        });
        assert!(
            line.contains('5'),
            "the reported line must name the current page count: {line:?}"
        );
    }

    /// Drive `lines` through the REPL's REAL per-line statement-execution
    /// path (`feed_line` — what both the interactive and scripted adapters
    /// call for every line), not raw `db.execute`. Panics if any line makes
    /// the session quit or if any statement reports an error, since none of
    /// these fixtures are expected to fail.
    fn drive_lines(db: &Arc<Database>, lines: &[&str]) {
        let session_handle = writing_session(db);
        let mut session = SessionState::default();
        let mut pending = StatementBuffer::default();
        for line in lines {
            let keep_going = feed_line(
                &session_handle,
                None,
                None,
                line,
                scripted_input(),
                None,
                &mut session,
                &mut pending,
            );
            assert!(keep_going, "line must not end the session: {line}");
        }
        assert!(
            !session.had_error,
            "every fixture statement must succeed through the real CLI statement path"
        );
    }

    #[test]
    fn cli_statement_flow_never_registers_a_sink_so_a_routed_event_queues_undelivered() {
        // Driven through `feed_line`, the REAL per-line path both the
        // interactive and scripted adapters call — not raw `db.execute` —
        // exactly what running these same lines at the `contextdb` prompt
        // does today. The event fires into the sink's durable queue and
        // sits there forever; nothing an operator typed makes it observably
        // deliver. RULED contract (owner, 2026-08-06): when the CLI's own
        // statement-execution path processes CREATE SINK, the CLI registers
        // its own default callback — the engine's queue-until-registered
        // contract (acceptance t5_26) stays intact, and delivery becomes
        // observable through the CLI alone.
        let db = Arc::new(Database::open_memory());
        drive_lines(
            &db,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY);",
                "CREATE EVENT TYPE t_ins WHEN INSERT ON t;",
                "CREATE SINK s TYPE callback;",
                "CREATE ROUTE r EVENT t_ins TO s;",
                "INSERT INTO t (id) VALUES (1);",
            ],
        );

        let metrics = db.sink_metrics("s");
        assert_eq!(
            metrics.delivered, 1,
            "the CLI's real statement-execution path must register a default \
             delivery callback when it processes CREATE SINK, so a routed event \
             observably delivers when it fires; today nothing in that path does \
             this (queued={}, delivered={})",
            metrics.queued, metrics.delivered
        );
    }

    #[test]
    fn cli_statement_flow_never_registers_a_cron_callback_so_a_schedule_never_fires() {
        // Same story for CREATE SCHEDULE, driven through the real `feed_line`
        // path. `cron_run_due_now_for_test` synchronously drives any due
        // schedule (a bounded, sleep-free engine test seam) rather than this
        // test sleeping to wait for wall-clock time to pass — but it only
        // bound-waits up to 75ms for an imminently-due schedule
        // (`wait_for_imminent_cron_due_for_test`), so the interval below is
        // kept under that bound (a schedule due further out than the wait
        // window is silently skipped by that one call, independent of
        // whatever the ALSO-running background tickler thread does). And
        // because that background tickler can fire the schedule on its own
        // wall-clock cadence at any time, the assertion reads the schedule's
        // PERSISTED fire_count via `cron_status()` rather than trusting
        // `cron_run_due_now_for_test`'s own return value, which only counts
        // fires from this one call and misses a tickler fire that already
        // landed first.
        let db = Arc::new(Database::open_memory());
        drive_lines(
            &db,
            &[
                "CREATE TABLE cron_log (id INTEGER PRIMARY KEY);",
                "CREATE SCHEDULE hb EVERY '30 MILLISECONDS' TX (hb_cb);",
            ],
        );

        // Each call re-evaluates due state against a FRESH 75ms budget, so a
        // few attempts absorb ordinary scheduler jitter on a loaded box
        // without this test ever sleeping itself — every wait is inside the
        // engine's own bounded seam.
        let mut fire_count = 0;
        for _ in 0..5 {
            let _ = db.cron_run_due_now_for_test();
            fire_count = db
                .cron_status()
                .iter()
                .find(|schedule| schedule.name == "hb")
                .expect("schedule hb must be registered after CREATE SCHEDULE")
                .fire_count;
            if fire_count > 0 {
                break;
            }
        }
        assert!(
            fire_count > 0,
            "a SCHEDULE declared purely through CLI SQL must be able to fire \
             observably — the CLI must supply a default callback so an operator \
             who never writes Rust still sees it run; got persisted fire_count=0 \
             after 5 attempts"
        );
    }

    #[test]
    fn test_backslash_dt() {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        assert!(
            handle_meta_command(
                &writing_session(&db),
                None,
                None,
                "\\dt",
                scripted_input(),
                None,
                &mut SessionState::default(),
            )
            .keep_going
        );
    }

    // B1: Existing \dt works with new handle_meta_command signature
    #[test]
    fn b1_existing_dt_works_with_new_signature() {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        // Pass None for sync_client and rt — existing commands must work without sync
        let dt = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            "\\dt",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
        assert!(dt.keep_going && dt.ok);
        // Also verify .tables works
        let tables = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            ".tables",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
        assert!(tables.keep_going && tables.ok);
        // .quit ends the session, successfully
        let quit = handle_meta_command(
            &writing_session(&db),
            None,
            None,
            ".quit",
            scripted_input(),
            None,
            &mut SessionState::default(),
        );
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

    // B5: .sync invalid arguments handled gracefully
    #[test]
    fn b5_sync_invalid_args() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "iroh:invalid-test-ticket",
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
        let db = Arc::new(Database::open_memory());
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
            let db = Arc::new(Database::open_memory());
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
        struct UnreachableAfterConnect {
            local_node_id: String,
        }
        impl ClientTransport for UnreachableAfterConnect {
            fn peer_node_id(&self) -> Option<String> {
                Some("unreachable-test-hub".to_string())
            }

            fn local_node_id(&self) -> Option<String> {
                Some(self.local_node_id.clone())
            }

            fn has_stable_edge_identity(&self) -> bool {
                true
            }

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
        let edge_identity = Arc::new(contextdb_server::FabricIdentity::generate());
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            db,
            Arc::new(UnreachableAfterConnect {
                local_node_id: edge_identity.node_id(),
            }),
            contextdb_core::TenantId::from("usr19-exit"),
            edge_identity,
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
            ..SessionState::default()
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
            let db = Arc::new(Database::open_memory());
            let mut session = SessionState {
                interactive,
                ..SessionState::default()
            };
            let mut pending = StatementBuffer::default();
            feed_line(
                &writing_session(&db),
                None,
                None,
                "SELET * FROM nothing;",
                InputContext {
                    interactive,
                    script_line: Some(1),
                    output: OutputOptions::default(),
                    store_writes_permitted: true,
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
            ..SessionState::default()
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
            ..SessionState::default()
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
            ..SessionState::default()
        };
        assert_eq!(
            session_exit_code(&errored),
            1,
            "a definitive error dominates an unconfirmed push"
        );
    }
}
