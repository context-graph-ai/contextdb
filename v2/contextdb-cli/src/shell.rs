//! The shell: reads lines, gathers them into statements, runs each against the store and
//! prints what it gave.

use std::collections::HashMap;
use std::io::{self, BufRead, IsTerminal};
use std::ops::ControlFlow;

use contextdb::{Database, Error, QueryResult, ReadSession, Result, Transaction};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::input::{Scan, scan};
use crate::output::Output;

/// The store as this run opened it.
pub enum Store {
    Read(ReadSession),
    Write(Database),
}

const HELP: [&str; 8] = [
    "<statement>;        Run a SQL statement. It may span lines and ends at its `;`.",
    "BEGIN; ... COMMIT;  Run statements as one transaction; ROLLBACK; undoes them, except SET MEMORY_LIMIT. Needs --write.",
    ".tables   \\dt       List the tables.",
    ".schema <table>   \\d <table>   Show a table's declaration as SQL.",
    ".explain <sql>      Show how a similarity search would be answered, without running it.",
    "SHOW VECTOR_INDEXES; SHOW VECTOR_PARTITIONS;   Describe the VECTOR columns and their partitions.",
    ".help     \\?        Show this list.",
    ".quit     \\q        End the session.",
];

struct Shell<'a> {
    store: &'a Store,
    output: Output,
    /// The transaction `BEGIN` opened, until `COMMIT` or `ROLLBACK`.
    transaction: Option<Transaction<'a>>,
    /// Lines of a statement whose `;` has not come yet.
    pending: String,
    /// Whether anything was refused.
    refused: bool,
}

/// Runs the shell over the standard input until it ends or `.quit`.
///
/// Returns whether the run succeeded. A piped run succeeds only when every statement and every
/// shell command did; a session at a terminal showed each refusal as it happened and always
/// succeeds.
pub fn run(store: &Store, output: Output) -> bool {
    let mut shell = Shell {
        store,
        output,
        transaction: None,
        pending: String::new(),
        refused: false,
    };
    if std::io::stdin().is_terminal() {
        shell.at_a_terminal();
        return true;
    }
    for (at, line) in std::io::stdin().lock().lines().enumerate() {
        // What follows a line that cannot be read may rest on it: nothing after it runs, and
        // a transaction left open is not committed.
        let Ok(line) = line else {
            let message = format!(
                "line {} of the input is not text, so the run ends there",
                at + 1
            );
            shell.output.refusal("io", &message);
            return false;
        };
        if shell.line(&line).is_break() {
            return !shell.refused;
        }
    }
    // Piped input ends its last statement without a `;`.
    let rest = std::mem::take(&mut shell.pending);
    let _ = shell.statement(&rest);
    !shell.refused
}

impl<'a> Shell<'a> {
    /// Reads lines a person types, with line editing and recall of the session's earlier
    /// lines. Nothing of the session is kept after it ends.
    fn at_a_terminal(&mut self) {
        let (banner, prompt) = match self.store {
            Store::Read(_) => (
                "read-only session — pass --write to mutate",
                "contextdb(ro)> ",
            ),
            Store::Write(_) => ("write session — mutations are enabled", "contextdb> "),
        };
        let mut editor = match DefaultEditor::new() {
            Ok(editor) => editor,
            Err(error) => {
                self.output
                    .refusal("io", &format!("the terminal cannot be read: {error}"));
                return;
            }
        };
        eprintln!("{banner}");
        loop {
            let open = !self.pending.is_empty();
            match editor.readline(if open { "      ...> " } else { prompt }) {
                Ok(line) => {
                    if line.trim().is_empty() && !open {
                        continue;
                    }
                    // A line that cannot be kept for recall is still run.
                    let _ = editor.add_history_entry(line.trim());
                    if self.line(&line).is_break() {
                        return;
                    }
                }
                // Ctrl-C drops what was typed and the session goes on.
                Err(ReadlineError::Interrupted) => self.pending.clear(),
                // Ctrl-D, or a terminal that went away.
                Err(_) => break,
            }
        }
        if !self.pending.is_empty() {
            eprintln!("Discarding incomplete statement (no closing `;`).");
        }
    }

    /// Takes one line of input. Breaks when the line ends the session.
    fn line(&mut self, line: &str) -> ControlFlow<()> {
        // A shell command is one line, and only where no statement is open: inside a statement
        // a line that starts with a dot is that statement's text.
        let command = line.trim();
        if self.pending.is_empty() && command.starts_with(['.', '\\']) {
            return self.command(command);
        }
        self.pending.push_str(line);
        self.pending.push('\n');
        loop {
            match scan(&self.pending) {
                Scan::Ends(end) => {
                    let statement: String = self.pending.drain(..end).collect();
                    if self.statement(&statement).is_break() {
                        return ControlFlow::Break(());
                    }
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

    /// Runs one shell command.
    fn command(&mut self, line: &str) -> ControlFlow<()> {
        let mut words = line.split_whitespace();
        let answered = match (words.next(), words.next(), words.next()) {
            (Some(".tables" | "\\dt"), None, _) => match self.store {
                Store::Read(reader) => reader.table_names(),
                Store::Write(db) => Ok(db.table_names()),
            }
            .map(|names| self.output.tables(&names)),
            (Some(".schema" | "\\d"), Some(table), None) => match self.store {
                Store::Read(reader) => reader.declaration(table),
                Store::Write(db) => db.declaration(table),
            }
            .map(|declaration| self.output.schema(table, &declaration)),
            (Some(".explain"), Some(_), _) => {
                let sql = line[".explain".len()..].trim();
                let sql = sql.strip_suffix(';').unwrap_or(sql);
                match self.store {
                    Store::Read(reader) => reader.explain(sql),
                    Store::Write(db) => db.explain(sql),
                }
                .map(|lines| self.output.explain(&lines))
            }
            (Some(".help" | "\\?"), None, _) => Ok(self.output.help(&HELP)),
            (Some(".quit" | ".exit" | "\\q"), None, _) => return ControlFlow::Break(()),
            _ => {
                self.refused = true;
                let first = line.split_whitespace().next().unwrap_or_default();
                let usage = HELP
                    .into_iter()
                    .find(|h| h.split_whitespace().any(|w| w == first));
                self.output.unknown_command(line, usage);
                Ok(Ok(()))
            }
        };
        self.answered(answered)
    }

    /// Runs one statement and prints its result or its refusal. Breaks when the result
    /// cannot be written.
    fn statement(&mut self, text: &str) -> ControlFlow<()> {
        let sql = text.trim();
        let sql = sql.strip_suffix(';').unwrap_or(sql).trim_end();
        if sql.is_empty() {
            return ControlFlow::Continue(());
        }
        let ran = if sql.eq_ignore_ascii_case("BEGIN") {
            self.begin()
        } else if sql.eq_ignore_ascii_case("COMMIT") {
            self.end(Transaction::commit)
        } else if sql.eq_ignore_ascii_case("ROLLBACK") {
            self.end(|transaction| {
                transaction.rollback();
                Ok(())
            })
        } else {
            // The rows of the two inspection statements are too wide for a table.
            let mut words = sql.split_whitespace().map(str::to_ascii_uppercase);
            let inspection = words.next().as_deref() == Some("SHOW")
                && words.next().is_some_and(|word| word.starts_with("VECTOR_"));
            self.execute(sql).map(|result| match inspection {
                true => self.output.fields(&result),
                false => self.output.result(&result),
            })
        };
        self.answered(ran)
    }

    /// Prints a refusal, or ends the session when the answer could not be written: as `.quit`
    /// does when nobody reads the output any more, and as a refusal for any other failure.
    fn answered(&mut self, answered: Result<io::Result<()>>) -> ControlFlow<()> {
        match answered {
            Ok(Ok(())) => {}
            Err(error) => {
                self.refused = true;
                self.output.error(&error);
            }
            Ok(Err(error)) => {
                if error.kind() != io::ErrorKind::BrokenPipe {
                    self.refused = true;
                    let message = format!("the output cannot be written: {error}");
                    self.output.refusal("io", &message);
                }
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    fn execute(&self, sql: &str) -> Result<QueryResult> {
        let params = HashMap::new();
        match (&self.transaction, self.store) {
            (Some(transaction), _) => transaction.execute(sql, &params),
            (None, Store::Write(db)) => db.execute(sql, &params),
            (None, Store::Read(reader)) => reader.execute(sql, &params),
        }
    }

    /// `BEGIN`. With a transaction open already it changes nothing, warns, and answers the same.
    fn begin(&mut self) -> Result<io::Result<()>> {
        let Store::Write(db) = self.store else {
            return Err(Error::ReadOnlySession);
        };
        match self.transaction {
            None => self.transaction = Some(db.transaction()?),
            Some(_) => self
                .output
                .warning("there is already a transaction in progress"),
        }
        Ok(self.output.rows_affected(0))
    }

    /// `COMMIT` or `ROLLBACK`. With no transaction open it changes nothing, warns, and answers
    /// the same.
    fn end(&mut self, how: impl FnOnce(Transaction<'a>) -> Result<()>) -> Result<io::Result<()>> {
        if let Store::Read(_) = self.store {
            return Err(Error::ReadOnlySession);
        }
        match self.transaction.take() {
            Some(transaction) => how(transaction)?,
            None => self.output.warning("there is no transaction in progress"),
        }
        Ok(self.output.rows_affected(0))
    }
}
