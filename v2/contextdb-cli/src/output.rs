//! Everything the shell prints: results on the output, refusals on the error stream, each as
//! text for a person or, under `--json`, as one JSON document a line. A result that cannot be
//! written is handed back to the shell as the failure it is.

use std::io::{Result, Write, stdout};

use contextdb::serde_json::{Map, Value as Json, json};
use contextdb::{Error, QueryResult, Value};

/// How this run prints.
#[derive(Clone, Copy)]
pub struct Output {
    pub json: bool,
}

impl Output {
    /// The result of one statement: the rows of a SELECT, or how many rows a write changed.
    pub fn result(self, result: &QueryResult) -> Result<()> {
        if result.columns().is_empty() {
            self.rows_affected(result.rows_affected())
        } else if self.json {
            // A row is written key by key so that its keys come in the order of `columns`.
            let mut rows = String::new();
            for row in result.rows() {
                if !rows.is_empty() {
                    rows.push(',');
                }
                rows.push('{');
                for (at, (column, value)) in result.columns().iter().zip(row).enumerate() {
                    if at > 0 {
                        rows.push(',');
                    }
                    rows.push_str(&format!("{}:{}", json!(column), cell(value)));
                }
                rows.push('}');
            }
            writeln!(
                stdout(),
                "{{\"result\":{{\"columns\":{},\"rows\":[{rows}]}}}}",
                json!(result.columns())
            )
        } else {
            write!(stdout(), "{}", table(result.columns(), result.rows()))
        }
    }

    /// The rows of an inspection statement: one `name: value` to a line, with an empty line
    /// between rows.
    pub fn fields(self, result: &QueryResult) -> Result<()> {
        if self.json {
            return self.result(result);
        }
        for (at, row) in result.rows().iter().enumerate() {
            if at > 0 {
                writeln!(stdout())?;
            }
            for (name, value) in result.columns().iter().zip(row) {
                writeln!(stdout(), "{name}: {}", text(value))?;
            }
        }
        Ok(())
    }

    /// What `.explain` answers: its lines.
    pub fn explain(self, lines: &str) -> Result<()> {
        if self.json {
            let lines: Vec<&str> = lines.lines().collect();
            writeln!(stdout(), "{}", json!({ "explain": { "lines": lines } }))
        } else {
            writeln!(stdout(), "{lines}")
        }
    }

    /// A statement that returns no rows succeeded and changed this many.
    pub fn rows_affected(self, rows: u64) -> Result<()> {
        if self.json {
            writeln!(stdout(), "{}", json!({ "rows_affected": rows }))
        } else {
            writeln!(stdout(), "ok (rows_affected={rows})")
        }
    }

    /// What `.tables` answers: the table names, one a line.
    pub fn tables(self, names: &[String]) -> Result<()> {
        if self.json {
            writeln!(stdout(), "{}", json!({ "tables": { "items": names } }))
        } else {
            names
                .iter()
                .try_for_each(|name| writeln!(stdout(), "{name}"))
        }
    }

    /// What `.schema` answers: the table's declaration as SQL.
    pub fn schema(self, table: &str, declaration: &str) -> Result<()> {
        if self.json {
            let schema = json!({ "schema": { "table": table, "ddl": declaration } });
            writeln!(stdout(), "{schema}")
        } else {
            writeln!(stdout(), "{}", declaration.trim_end())
        }
    }

    /// What `.help` answers.
    pub fn help(self, lines: &[&str]) -> Result<()> {
        if self.json {
            writeln!(stdout(), "{}", json!({ "help": lines }))
        } else {
            lines
                .iter()
                .try_for_each(|line| writeln!(stdout(), "{line}"))
        }
    }

    /// A refusal from the library. A program reads `class` and `detail.kind`; the message is
    /// for a person.
    pub fn error(self, error: &Error) {
        let class = match error {
            Error::StoreMissing { .. }
            | Error::DatabaseLocked { .. }
            | Error::StoreCorrupted { .. }
            | Error::StoreIo { .. }
            | Error::MemoryBudgetExceeded { .. } => "io",
            _ => "sql",
        };
        let mut detail = Map::new();
        detail.insert("kind".to_owned(), json!(kind(error)));
        if let Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        } = error
        {
            detail.insert("requested_bytes".to_owned(), json!(requested_bytes));
            detail.insert("available_bytes".to_owned(), json!(available_bytes));
            detail.insert("budget_limit_bytes".to_owned(), json!(budget_limit_bytes));
        }
        // Where an option of this program is the way on, the sentence is the program's own:
        // the library's does not know the options.
        let message = match error {
            Error::StoreMissing { path } => format!(
                "there is no store at this path: {}; --write creates it",
                path.display()
            ),
            Error::DatabaseLocked { holder_pid, path } => format!(
                "process {holder_pid} holds the store at {} for writing; wait for it to \
                 finish, or open without --write to read",
                path.display()
            ),
            Error::ReadOnlySession => {
                "this statement writes and this session only reads; rerun with --write".to_owned()
            }
            Error::MemoryBudgetExceeded { .. } => format!(
                "{}; raise it with --memory-limit",
                error.to_string().trim_end_matches('.')
            ),
            other => other.to_string(),
        };
        self.refused(class, &message, Some(detail));
    }

    /// A warning: the statement still answers and the run goes on, as in Postgres.
    pub fn warning(self, message: &str) {
        eprintln!("warning: {message}");
    }

    /// A refusal of the shell's own: `class` is `usage` for a command line or a shell command
    /// it cannot act on, `sql` for a statement.
    pub fn refusal(self, class: &str, message: &str) {
        self.refused(class, message, None);
    }

    /// A line that starts like a shell command and is not one, or a command used wrongly, with
    /// its `usage`. For a person it is a plain line with no `Error:` before it.
    pub fn unknown_command(self, line: &str, usage: Option<&str>) {
        let message = match usage {
            Some(usage) => format!("Usage: {usage}"),
            None => format!("Unknown command: {line}. Type \\? for help."),
        };
        if self.json {
            self.refused("usage", &message, None);
        } else {
            eprintln!("{message}");
        }
    }

    fn refused(self, class: &str, message: &str, detail: Option<Map<String, Json>>) {
        if self.json {
            let mut error = Map::new();
            error.insert("class".to_owned(), json!(class));
            error.insert("message".to_owned(), json!(message));
            if let Some(detail) = detail {
                error.insert("detail".to_owned(), Json::Object(detail));
            }
            eprintln!("{}", json!({ "error": error }));
        } else {
            eprintln!("Error: {message}");
        }
    }
}

/// The word a program reads for a refusal's kind: the three the earlier program published
/// under another word keep it, and every other kind is its own name in lower snake case.
fn kind(error: &Error) -> String {
    match error {
        Error::StoreMissing { .. } => "store_not_found".to_owned(),
        Error::DatabaseLocked { .. } => "held_by_writer".to_owned(),
        Error::ReadOnlySession => "write_requires_flag".to_owned(),
        other => {
            let mut word = String::new();
            for c in format!("{other:?}")
                .chars()
                .take_while(char::is_ascii_alphanumeric)
            {
                if c.is_ascii_uppercase() && !word.is_empty() {
                    word.push('_');
                }
                word.push(c.to_ascii_lowercase());
            }
            word
        }
    }
}

/// One value as JSON.
fn cell(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(value) => json!(value),
        Value::Int64(value) | Value::Timestamp(value) => json!(value),
        Value::Float64(value) => json!(value),
        Value::Text(value) => json!(value),
        Value::Uuid(value) => json!(value.to_string()),
        Value::Json(value) => value.clone(),
        Value::Vector(value) => json!(value),
        Value::TxId(value) => json!(value.0),
    }
}

/// One value as a person reads it in a table.
fn text(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) | Value::Timestamp(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Uuid(value) => value.to_string(),
        Value::Json(value) => value.to_string(),
        Value::Vector(value) => format!("{value:?}"),
        Value::TxId(value) => value.0.to_string(),
    }
}

/// The rows as a bordered table with a `(N rows)` line under it, `(1 row)` for one. With no rows only that line
/// is printed.
pub fn table(columns: &[String], rows: &[Vec<Value>]) -> String {
    let footer = match rows.len() {
        1 => "(1 row)\n".to_owned(),
        count => format!("({count} rows)\n"),
    };
    if rows.is_empty() {
        return footer;
    }
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    let mut widths: Vec<usize> = columns.iter().map(|name| name.chars().count()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }

    let mut border = String::new();
    for width in &widths {
        border.push('+');
        border.push_str(&"-".repeat(width + 2));
    }
    border.push_str("+\n");
    let line = |cells: &[String]| {
        let mut line = String::from("|");
        for (cell, width) in cells.iter().zip(&widths) {
            line.push_str(&format!(" {cell:<width$} |"));
        }
        line.push('\n');
        line
    };

    let mut out = border.clone() + &line(columns) + &border;
    for row in &rows {
        out.push_str(&line(row));
    }
    out + &border + &footer
}
