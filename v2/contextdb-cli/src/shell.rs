//! Statement collection and dispatch; the library owns reads, cursors and transactions.
use crate::input::{Scan, scan};
use crate::interrupt;
use crate::output::{Json, Output, json};
use contextdb::{
    Cursor, Database, Error, Maintenance, QueryResult, ReadSession, Result, Transaction,
};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::io::{self, Write};
use std::ops::ControlFlow;
use std::sync::{Arc, atomic::Ordering::SeqCst};
pub struct Store {
    pub reader: Arc<ReadSession>,
    pub writer: Option<Database>,
    pub mode: Maintenance,
}
const HELP: &[&str] = &[
    "<statement>; Run SQL; BEGIN; COMMIT; ROLLBACK; need --write.",
    ".tables \\dt   .schema <table> \\d <table>",
    ".explain <sql>      Show the path a statement would take, without running it.",
    ".cursor open <sql> | fetch [rows] | close",
    ".events status   .maintenance run | status (needs --write)",
    "SHOW VECTOR_INDEXES; SHOW VECTOR_PARTITIONS; Describe vectors.",
    "SHOW STATEMENT_STATISTICS; RESET STATEMENT_STATISTICS; Count the work of statements.",
    ".verify   .trace on | off   .help [vector | propagate] \\?   .quit \\q",
];
const VECTOR_HELP: &[&str] = &[
    "<column> VECTOR(<n>) [WITH (quantization = 'f32'|'sq8'|'sq4')] [PARTITION_KEY (<column>, ...)]",
    "  [MAX_PARTITIONS <n>] [SEARCH_MODE AUTO|EXACT|INDEXED] [INDEXING_THRESHOLD <n>]",
    "SELECT ... ORDER BY <column> <=> [0.1, 0.2] LIMIT <n> [USE VECTOR AUTO|EXACT|INDEXED]",
    "SHOW VECTOR_INDEXES; SHOW VECTOR_PARTITIONS;",
];
const PROPAGATE_HELP: &[&str] = &[
    "<column> REFERENCES <table>(<column>) ON STATE <state> PROPAGATE SET <state> [MAX DEPTH <n>] [ABORT ON FAILURE]",
    "PROPAGATE ON EDGE <type> INCOMING|OUTGOING|BOTH STATE <state> SET <state> [MAX DEPTH <n>] [ABORT ON FAILURE]",
    "PROPAGATE ON STATE <state> EXCLUDE VECTOR",
];
pub struct Shell<'a> {
    store: &'a Store,
    terminal: bool,
    pub interrupted: bool,
    output: Output,
    transaction: Option<Transaction<'a>>,
    pending: String,
    refused: bool,
    failed: bool,
    stop: bool,
    line: u64,
    start: u64,
    cursor: Option<Cursor>,
    trace: bool,
    writer: Option<bool>,
}
pub fn succeeded(terminal: bool, refused: bool, failed: bool) -> bool {
    !failed && (terminal || !refused)
}
pub fn banner(writer: bool) -> &'static str {
    if writer {
        "write session — mutations are enabled"
    } else {
        "read-only session — pass --write to mutate"
    }
}
pub fn prompt(writer: bool, pending: bool) -> &'static str {
    match (writer, pending) {
        (_, true) => "      ...> ",
        (true, false) => "contextdb> ",
        (false, false) => "contextdb(ro)> ",
    }
}
/// Whether the run succeeded, and whether an interrupt ended it.
pub fn run(store: &Store, output: Output, stop: bool, terminal: bool) -> (bool, bool) {
    let mut s = Shell::new(store, output, stop, terminal);
    if terminal {
        s.at_a_terminal();
        return (succeeded(true, s.refused, s.failed), false);
    }
    let (lines, input) = std::sync::mpsc::channel();
    let (end, reading) = (Err(io::ErrorKind::UnexpectedEof.into()), lines.clone());
    #[allow(
        clippy::disallowed_methods,
        reason = "input read beside the statements, so an interrupt wakes a waiting run"
    )]
    std::thread::spawn(move || {
        io::stdin()
            .lines()
            .chain([end])
            .try_for_each(|l| reading.send(l))
    });
    let ok = interrupt::running(interrupt::Active::Input(lines), || {
        for line in input {
            match line {
                Ok(line) => {
                    if s.line(&line).is_break() {
                        return succeeded(false, s.refused, s.failed);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    s.interrupted = true;
                    return false;
                }
                Err(e) => {
                    s.output.refusal(
                        "io",
                        &format!("line {} of the input is not text: {e}", s.line + 1),
                    );
                    return false;
                }
            }
        }
        let rest = std::mem::take(&mut s.pending);
        let _ = s.statement(&rest);
        succeeded(false, s.refused, s.failed)
    });
    (ok, s.interrupted)
}
impl<'a> Shell<'a> {
    pub fn new(store: &'a Store, output: Output, stop: bool, terminal: bool) -> Self {
        Shell {
            store,
            terminal,
            interrupted: false,
            output,
            transaction: None,
            pending: String::new(),
            refused: false,
            failed: false,
            stop: stop && !terminal,
            line: 0,
            start: 1,
            cursor: None,
            trace: false,
            writer: None,
        }
    }
    fn at_a_terminal(&mut self) {
        let writer = self.store.writer.is_some();
        let mut editor = match DefaultEditor::new() {
            Ok(e) => e,
            Err(e) => {
                self.output.refusal("io", &e.to_string());
                self.failed = true;
                return;
            }
        };
        eprintln!("{}", banner(writer));
        loop {
            match editor.readline(prompt(writer, !self.pending.is_empty())) {
                Ok(line) => {
                    let _ = editor.add_history_entry(line.trim());
                    if self.line(&line).is_break() {
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) => self.pending.clear(),
                Err(ReadlineError::Eof) => break,
                Err(e) => {
                    self.output.refusal("io", &e.to_string());
                    self.failed = true;
                    break;
                }
            }
        }
        if !self.pending.is_empty() {
            eprintln!("Discarding incomplete statement (no closing `;`).");
        }
    }
    pub fn line(&mut self, line: &str) -> ControlFlow<()> {
        self.line += 1;
        if self.pending.is_empty() {
            self.start = self.line;
            if line.trim().starts_with(['.', '\\']) {
                return self.command(line.trim());
            }
        }
        self.pending.push_str(line);
        self.pending.push('\n');
        loop {
            match scan(&self.pending) {
                Scan::Ends(end) => {
                    let text: String = self.pending.drain(..end).collect();
                    let lines = text.bytes().filter(|b| *b == b'\n').count() as u64;
                    if self.statement(&text).is_break() {
                        return ControlFlow::Break(());
                    }
                    self.start += lines;
                }
                Scan::Open => break,
                Scan::Blank => {
                    self.pending.clear();
                    break;
                }
            }
        }
        ControlFlow::Continue(())
    }
    fn command(&mut self, line: &str) -> ControlFlow<()> {
        let mut words = line.split_whitespace();
        let answer = match (words.next(), words.next(), words.next()) {
            (Some(".tables" | "\\dt"), None, _) => self.measured(
                "tables",
                self.store.reader.table_names().map(|v| Output::tables(&v)),
            ),
            (Some(".schema" | "\\d"), Some(t), None) => {
                let reader = &self.store.reader;
                let meta = reader.table_meta(t);
                let doc = reader
                    .declaration(t)
                    .and_then(|v| meta.map(|m| Output::schema(t, &v, &m)));
                self.measured("schema", doc)
            }
            (Some(".explain"), Some(_), _) => {
                let lines = self
                    .store
                    .reader
                    .explain(line[8..].trim().trim_end_matches(';'));
                self.measured("explain", lines.map(|v| Output::explain(&v)))
            }
            (Some(".help" | "\\?"), None, _) => Ok(self.output.help(HELP)),
            (Some(".help" | "\\?"), Some("vector"), None) => Ok(self.output.help(VECTOR_HELP)),
            (Some(".help" | "\\?"), Some("propagate"), None) => {
                Ok(self.output.help(PROPAGATE_HELP))
            }
            (Some(".quit" | ".exit" | "\\q"), None, _) => return ControlFlow::Break(()),
            (Some(".verify"), None, _) => return self.statement("SHOW CONTENT_SIZE"),
            (Some(".events"), Some("status"), None) => self.measured("events", self.store.reader.event_bus_status()
            .map(|v|json!(&*v))),
            (Some(".maintenance"),Some("run"),None)=>self.store.writer.as_ref().ok_or(Error::ReadOnlySession).and_then(Database::run_maintenance_cycle).map(|r|self.output.status("maintenance_cycle",r)),
            (Some(".maintenance"), Some("compact"), steps) => self.store.writer.as_ref().ok_or(Error::ReadOnlySession).and_then(|db| {
                let steps = steps.map_or(Ok(u32::MAX), str::parse::<u32>).map_err(|_| Error::ParseError("steps is a whole number".into()))?;
                let (mut report, mut moved) = (Default::default(), 0);
                for _ in 0..steps {
                    report = db.compact_now()?;
                    moved += report.compacted_pages.unwrap_or(0);
                    if report.work_remaining != Some(true) || report.compaction_waiting_on.is_some() {
                        break;
                    }
                }
                report.compacted_pages = Some(moved);
                report.page_slack_ratio = db.page_slack_ratio();
                Ok(report)
            }).map(|r| self.output.status("maintenance_cycle", r)),
            (Some(".maintenance"),Some("status"),None)=>self.measured("maintenance",self.store.writer.as_ref().ok_or(Error::ReadOnlySession).and_then(Database::last_maintenance_cycle).map(|r|json!({"mode":if self.store.mode==Maintenance::Engine {"engine"}else{"caller"},"last_cycle":r}))),
            (Some(".trace"), Some(state @ ("on" | "off")), None) => {
                self.trace = state == "on";
                Ok(self.output.trace_state(state))
            }
            (Some(".cursor"), Some(action), _) => self.cursor(action, line),
            _ => {
                self.refused = true;
                let first = line.split_whitespace().next().unwrap_or_default();
                let usage = HELP
                    .iter()
                    .find(|h| h.split_whitespace().any(|w| w == first));
                self.output.unknown_command(line, usage.copied());
                Ok(Ok(()))
            }
        };
        self.answered(answer, line)
    }
    fn cursor(&mut self, action: &str, line: &str) -> Result<io::Result<()>> {
        let reader = &self.store.reader;
        match action {
            "open" => {
                if self.transaction.is_some() {
                    return self.refuse(
                        "cursor_transaction_active",
                        "close the transaction before opening a cursor",
                    );
                }
                if self.cursor.is_some() {
                    return self.refuse("cursor_already_open", "close the open cursor first");
                }
                let sql = line[".cursor open".len()..].trim().trim_end_matches(';');
                let mut c =
                    interrupt::reading(reader, || reader.open_cursor(sql, &HashMap::new()))?;
                let page = interrupt::reading(reader, || c.fetch(None))?;
                let answer = self.output.cursor(&page, c.has_more());
                self.cursor = Some(c);
                Ok(answer)
            }
            "fetch" if line.split_whitespace().count() <= 3 => {
                let n = line
                    .split_whitespace()
                    .nth(2)
                    .map(str::parse)
                    .transpose()
                    .map_err(|_| Error::ParseError("fetch rows must be positive".into()))?;
                let Some(c) = self.cursor.as_mut() else {
                    return self.refuse("cursor_not_found", "open a cursor first");
                };
                let result = interrupt::reading(reader, || c.fetch(n))?;
                Ok(self.output.cursor(&result, c.has_more()))
            }
            "close" if line.split_whitespace().count() == 2 => {
                if let Some(mut c) = self.cursor.take() {
                    c.close()?;
                }
                Ok(self.output.cursor_close())
            }
            _ => self.refuse("usage", ".cursor open <sql> | fetch [rows] | close"),
        }
    }
    fn statement(&mut self, text: &str) -> ControlFlow<()> {
        let sql = text.trim().trim_end_matches(';').trim_end();
        if sql.is_empty() {
            return ControlFlow::Continue(());
        }
        let result = (|| {
            let current = self.store.reader.writer_running()?;
            if self.writer.is_some_and(|old| old != current) {
                let n = self.store.reader.committed_watermark()?.0;
                let what = ["the writer stopped", "the writer is back"][usize::from(current)];
                self.output
                    .notice("writer", &format!("{what}; reading commit {n}"));
            }
            self.writer = Some(current);
            if sql.eq_ignore_ascii_case("BEGIN") {
                self.begin()
            } else if sql.eq_ignore_ascii_case("COMMIT") {
                self.end(Transaction::commit)
            } else if sql.eq_ignore_ascii_case("ROLLBACK") {
                self.end(|t| {
                    t.rollback();
                    Ok(())
                })
            } else {
                let result = self.execute(sql)?;
                if self.trace {
                    self.output.trace(&result);
                }
                Ok(if result.is_report() {
                    self.output.fields(&result)
                } else {
                    self.output.result(&result)
                })
            }
        })();
        self.answered(result, sql)
    }
    fn answered(&mut self, answer: Result<io::Result<()>>, sql: &str) -> ControlFlow<()> {
        match answer {
            Ok(Ok(())) => {}
            Err(e) => {
                self.refused = true;
                self.output.error(&e, Some((self.start, sql)));
            }
            Ok(Err(e)) => {
                if e.kind() != io::ErrorKind::BrokenPipe {
                    self.failed = true;
                    self.output.refusal("io", &e.to_string());
                }
                return ControlFlow::Break(());
            }
        }
        self.interrupted |= interrupt::FIRED.swap(false, SeqCst) && !self.terminal;
        if self.interrupted || self.stop && self.refused {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
    fn measured(&mut self, kind: &str, value: Result<Json>) -> Result<io::Result<()>> {
        let mut text = Vec::new();
        let drawn = self.output.draw(kind, value?, &mut text);
        if drawn.is_ok() && text.len() as u64 > self.output.result {
            self.output.too_large(text.len() as u64, self.start);
            self.refused = true;
            return Ok(Ok(()));
        }
        Ok(drawn.and_then(|()| io::stdout().write_all(&text)))
    }
    fn refuse(&mut self, kind: &str, message: &str) -> Result<io::Result<()>> {
        self.refused = true;
        self.output.shell_refusal(kind, message, self.start);
        Ok(Ok(()))
    }
    /// The open transaction answers all; else the read session reads, and the writer the rest.
    fn execute(&self, sql: &str) -> Result<QueryResult> {
        let (params, reader) = (HashMap::new(), &self.store.reader);
        let write = |db: &Database| interrupt::writing(db, || db.execute(sql, &params));
        match (&self.transaction, &self.store.writer) {
            (Some(tx), Some(db)) => interrupt::writing(db, || tx.execute(sql, &params)),
            (_, Some(db)) if !contextdb::read_session_answers(sql) => write(db),
            (_, writer) => match interrupt::reading(reader, || reader.execute(sql, &params)) {
                Err(Error::ReadOnlySession) => {
                    write(writer.as_ref().ok_or(Error::ReadOnlySession)?)
                }
                answer => answer,
            },
        }
    }
    fn begin(&mut self) -> Result<io::Result<()>> {
        let db = self.store.writer.as_ref().ok_or(Error::ReadOnlySession)?;
        if self.transaction.is_none() {
            self.transaction = Some(db.transaction()?);
        } else {
            self.output
                .warning("there is already a transaction in progress");
        }
        Ok(self.output.rows_affected(0))
    }
    fn end(&mut self, how: impl FnOnce(Transaction<'a>) -> Result<()>) -> Result<io::Result<()>> {
        if self.store.writer.is_none() {
            return Err(Error::ReadOnlySession);
        }
        if let Some(tx) = self.transaction.take() {
            how(tx)?;
        } else {
            self.output.warning("there is no transaction in progress");
        }
        Ok(self.output.rows_affected(0))
    }
}
