//! The shell: reads lines, gathers them into statements, runs each against the store and
//! prints what it gave.

use std::collections::HashMap;
use std::io::{BufRead, IsTerminal};
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

const HELP: [&str; 6] = [
    "<statement>;        Run a SQL statement. It may span lines and ends at its `;`.",
    "BEGIN; ... COMMIT;  Run statements as one transaction; ROLLBACK; undoes them, except SET MEMORY_LIMIT. Needs --write.",
    ".tables   \\dt       List the tables.",
    ".schema <table>   \\d <table>   Show a table's declaration as SQL.",
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
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if shell.line(&line).is_break() {
            return !shell.refused;
        }
    }
    // Piped input ends its last statement without a `;`.
    let rest = std::mem::take(&mut shell.pending);
    shell.statement(&rest);
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
                    self.statement(&statement);
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
            (Some(".help" | "\\?"), None, _) => {
                self.output.help(&HELP);
                Ok(())
            }
            (Some(".quit" | ".exit" | "\\q"), None, _) => return ControlFlow::Break(()),
            _ => {
                self.refused = true;
                self.output.unknown_command(line);
                Ok(())
            }
        };
        if let Err(error) = answered {
            self.refused = true;
            self.output.error(&error);
        }
        ControlFlow::Continue(())
    }

    /// Runs one statement and prints its result or its refusal.
    fn statement(&mut self, text: &str) {
        let sql = text.trim();
        let sql = sql.strip_suffix(';').unwrap_or(sql).trim_end();
        if sql.is_empty() {
            return;
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
            self.execute(sql).map(|result| self.output.result(&result))
        };
        if let Err(error) = ran {
            self.refused = true;
            self.output.error(&error);
        }
    }

    fn execute(&self, sql: &str) -> Result<QueryResult> {
        let params = HashMap::new();
        match (&self.transaction, self.store) {
            (Some(transaction), _) => transaction.execute(sql, &params),
            (None, Store::Write(db)) => db.execute(sql, &params),
            (None, Store::Read(reader)) => reader.execute(sql, &params),
        }
    }

    /// `BEGIN`. With a transaction open already it changes nothing and answers the same.
    fn begin(&mut self) -> Result<()> {
        let Store::Write(db) = self.store else {
            return Err(Error::ReadOnlySession);
        };
        if self.transaction.is_none() {
            self.transaction = Some(db.transaction()?);
        }
        self.output.rows_affected(0);
        Ok(())
    }

    /// `COMMIT` or `ROLLBACK`. With no transaction open it changes nothing and answers the
    /// same.
    fn end(&mut self, how: impl FnOnce(Transaction<'a>) -> Result<()>) -> Result<()> {
        if let Store::Read(_) = self.store {
            return Err(Error::ReadOnlySession);
        }
        if let Some(transaction) = self.transaction.take() {
            how(transaction)?;
        }
        self.output.rows_affected(0);
        Ok(())
    }
}
