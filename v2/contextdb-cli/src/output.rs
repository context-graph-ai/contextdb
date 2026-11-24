//! Answers are values; only render chooses JSON or text and the destination stream.
pub(crate) use contextdb::serde_json::{Map, Value as Json, json};
use contextdb::{ColumnDefault, Error, QueryResult, TableMeta, Value};
use std::io::{Result, Write, stderr, stdout};
#[derive(Clone, Copy)]
pub struct Output {
    pub json: bool,
    /// The limit a read runs under and the option or options that raise it; none for a writer.
    pub read: Option<(u64, &'static str)>,
    pub result: u64,
}
impl Output {
    pub fn result(self, r: &QueryResult) -> Result<()> {
        if r.columns().is_empty() {
            self.rows_affected(r.rows_affected())
        } else {
            self.report("result", r, None)
        }
    }
    pub fn report(self, key: &str, result: &QueryResult, more: Option<bool>) -> Result<()> {
        if self.json {
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
                "{{{}:{{\"columns\":{},\"rows\":[{rows}]{}}}}}",
                json!(key),
                json!(result.columns()),
                more.map(|m| format!(",\"has_more\":{m}"))
                    .unwrap_or_default()
            )
        } else {
            write!(stdout(), "{}", table(result.columns(), result.rows()))?;
            if let Some(more) = more {
                writeln!(stdout(), "has_more: {more}")?;
            }
            Ok(())
        }
    }

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

    pub fn rows_affected(self, n: u64) -> Result<()> {
        self.render("rows_affected", json!(n))
    }
    pub fn tables(names: &[String]) -> Json {
        json!({"items":names})
    }
    pub fn schema(table: &str, ddl: &str, m: &TableMeta) -> Json {
        let columns = m.columns.iter().map(|c| {
            let default = c.default.as_ref().map(|d| match d {
                ColumnDefault::Literal(value) => cell(value),
                _ => json!("NOW()"),
            });
            let (immutable, expires) = (m.immutable_columns.contains(&c.name), m.expires_column.as_ref() == Some(&c.name));
            json!({"name":c.name,"type":c.column_type.to_string(),"nullable":c.nullable,"primary_key":c.primary_key,"unique":c.unique,"default":default,"immutable":immutable,"expires":expires})
        });
        let indexes = m.indexes.iter().map(
            |i| json!({"name":i.name,"columns":i.columns.iter().map(|c| &c.0).collect::<Vec<_>>()}),
        );
        let edges = m.propagate_edges.iter().map(|e| json!({"edge_type":e.edge_type,"direction":e.direction,"on_state":e.rule.on_state,"set":e.rule.set,"max_depth":e.rule.max_depth,"abort_on_failure":e.rule.abort_on_failure}));
        let mut doc = json!({"table":table,"columns":columns.collect::<Vec<_>>(),"primary_key":m.columns.iter().filter(|c| c.primary_key).map(|c| &c.name).collect::<Vec<_>>(),"indexes":indexes.collect::<Vec<_>>(),"immutable":m.immutable,"dag_edge_types":m.dag,"propagate":{"edges":edges.collect::<Vec<_>>(),"exclude_vector_states":m.exclude_vector_states},"ddl":ddl});
        let named = |v: Option<String>| v.map(|v| json!(v));
        let optional = [
            ("state_machine", m.state_machine.as_ref().map(|s| json!({"column":s.column,"transitions":s.transitions.iter().map(|(from, to)| json!({"state":from,"next":to})).collect::<Vec<_>>()}))),
            ("retain", m.retain.as_ref().map(|r| json!({"count":r.count,"unit":format!("{:?}", r.unit),"sync_safe":r.sync_safe}))),
            ("history", named(m.history.map(|h| format!("{h:?}")))),
            ("sync_direction", named(m.sync.map(|d| format!("{d:?}")))),
            ("conflict_policy", named(m.conflict.map(|p| format!("{p:?}")))),
        ];
        for (key, value) in optional {
            if let Some(value) = value {
                doc[key] = value;
            }
        }
        doc
    }
    pub fn explain(lines: &str) -> Json {
        json!({"lines":lines.lines().collect::<Vec<_>>()})
    }
    pub fn help(self, lines: &[&str]) -> Result<()> {
        self.render("help", json!(lines))
    }
    pub fn notice(self, kind: &str, message: &str) {
        let note = json!({"class":"io","message":message,"detail":{"kind":kind}});
        let _ = self.render("notice", note);
    }
    pub fn warning(self, message: &str) {
        self.notice("warning", &format!("warning: {message}"));
    }
    pub fn cursor(self, r: &QueryResult, more: bool) -> Result<()> {
        self.report("cursor", r, Some(more))
    }
    pub fn cursor_close(self) -> Result<()> {
        self.render("cursor", json!({"closed":true}))
    }
    pub fn trace_state(self, state: &str) -> Result<()> {
        self.render("trace", json!(state))
    }
    pub fn trace(self, r: &QueryResult) {
        let _=self.render("trace",json!({"rows_examined":r.rows_examined(),"distance_evaluations":r.distance_evaluations(),"cache_hits":r.pages().0,"cache_reads":r.pages().1,"peak_bytes":r.peak_bytes()}));
    }
    pub fn status(self, kind: &str, value: impl serde::Serialize) -> Result<()> {
        self.render(kind, json!(value))
    }
    pub fn error(self, error: &Error, at: Option<(u64, &str)>) {
        let line = at.map(|a| a.0);
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
        if let Error::DiskLimitExceeded {
            current_bytes,
            budget_limit_bytes,
            ..
        } = error
        {
            detail.insert("current_bytes".to_owned(), json!(current_bytes));
            detail.insert("budget_limit_bytes".to_owned(), json!(budget_limit_bytes));
        }
        if let (Error::ReadLimitExceeded { field, limit }, Some((_, sql))) = (error, at) {
            let page = *field == "result_rows" && sql.starts_with(".cursor fetch");
            let rows = field.starts_with("result_") && !sql.starts_with(['.', '\\']);
            detail.extend(rows.then(|| ("statement".into(), json!(sql))));
            let remedy = if page {
                format!(".cursor fetch {limit}")
            } else {
                format!(".cursor open {sql}")
            };
            detail.extend((page || rows).then(|| ("remedy_command".into(), json!(remedy))));
        }
        // Where an option of this program is the way on, the sentence is the program's own:
        // the library's does not know the options.
        let message = match error {
            Error::StoreMissing { path } => format!(
                "there is no store at this path: {}; --write creates it",
                path.display()
            ),
            Error::ReadOnlySession => {
                "this statement writes and this session only reads; rerun with --write".to_owned()
            }
            Error::MemoryBudgetExceeded { hint, .. } => {
                let s = error.to_string();
                let s = s.trim_end_matches(hint).trim_end_matches([' ', '.']);
                let f = self.read.filter(|r| detail["budget_limit_bytes"] == r.0);
                format!("{s}; raise it with {}", f.map_or("--memory-limit", |r| r.1))
            }
            Error::StoreCorrupted { path, .. } => {
                let p = path.display();
                format!("{error} (contextdb diagnose {p}, then contextdb reset {p} --force)")
            }
            Error::ReadLimitExceeded { field, limit } => format!(
                "read limit {field} exceeded ({limit}); raise it with --read-{}",
                field.replace('_', "-")
            ),
            // The library's way on is a call; here it is a flag.
            other => other
                .to_string()
                .replace("open a read session", "open it without --write")
                .replace("ReadLimits::active_ms", "--read-active-ms")
                .replace("ReadLimits::result_bytes", "--read-result-bytes")
                .replace("ReadLimits::cache_share", "--read-cache-share"),
        };
        self.refused(class(error), &message, detail, line);
    }

    /// A metadata answer longer than `result_bytes`, refused at its line with nothing printed.
    pub fn too_large(self, required: u64, line: u64) {
        let (limit, setting) = (self.result, format!("effective result_bytes >= {required}"));
        let message = format!("read limit result_bytes exceeded ({limit}); {setting}");
        let detail = json!({"kind": "read_limit_exceeded", "limit": "result_bytes", "value": limit,
            "required_bytes": required, "required_setting": setting});
        let detail = detail.as_object().cloned().unwrap_or_default();
        self.refused("io", &message, detail, Some(line));
    }
    pub fn refusal(self, class: &str, message: &str) {
        self.refused(class, message, Map::new(), None);
    }
    /// The shell's own refusal: its kind is in `detail.kind`.
    pub fn shell_refusal(self, kind: &str, message: &str, line: impl Into<Option<u64>>) {
        let class = match kind {
            "usage" | "unknown_command" => "usage",
            "cursor_not_found" => "io",
            _ => "sql",
        };
        let detail = Map::from_iter([("kind".into(), json!(kind))]);
        self.refused(class, message, detail, line.into());
    }
    pub fn unknown_command(self, line: &str, usage: Option<&str>) {
        let message = match usage {
            Some(usage) => format!("Usage: {usage}"),
            None => format!("Unknown command: {line}. Type \\? for help."),
        };
        if self.json {
            self.shell_refusal("unknown_command", &message, None);
        } else {
            eprintln!("{message}");
        }
    }

    fn refused(self, class: &str, message: &str, detail: Map<String, Json>, line: Option<u64>) {
        let mut error = json!({"class":class,"message":message});
        match line {
            Some(n) if self.json => error["line"] = json!(n),
            Some(n) => error["message"] = json!(format!("line {n}: {message}")),
            None => {}
        }
        if !detail.is_empty() {
            error["detail"] = json!(detail);
        }
        let _ = self.render("error", error);
    }
    fn render(self, kind: &str, value: Json) -> Result<()> {
        let mut out: Box<dyn Write> =
            if matches!(kind, "notice" | "error") || (kind == "trace" && value.is_object()) {
                Box::new(stderr())
            } else {
                Box::new(stdout())
            };
        self.draw(kind, value, &mut out)
    }
    /// Writes one answer, as JSON or text, to `out`.
    pub fn draw(self, kind: &str, mut value: Json, mut out: impl Write) -> Result<()> {
        if value["live"] == false {
            for at in value["schedules"].as_array_mut().into_iter().flatten() {
                at["callback_registered"] = Json::Null;
            }
            for sink in value["sinks"].as_array_mut().into_iter().flatten() {
                sink["callback_registered"] = Json::Null;
                for (key, v) in sink["metrics"].as_object_mut().into_iter().flatten() {
                    if key != "queued" && key != "uncertain" {
                        *v = Json::Null;
                    }
                }
            }
        }
        if self.json {
            return writeln!(out, "{}", json!({kind:value}));
        }
        match kind {
            "notice" => return writeln!(out, "{}", value["message"].as_str().unwrap_or("")),
            "error" => return writeln!(out, "Error: {}", value["message"].as_str().unwrap_or("")),
            "rows_affected" => return writeln!(out, "ok (rows_affected={value})"),
            "schema" => return writeln!(out, "{}", value["ddl"].as_str().unwrap_or("").trim_end()),
            "trace" if value.is_string() => {
                return writeln!(out, "Trace is {}.", value.as_str().unwrap_or(""));
            }
            "cursor" if value["closed"] == true => return writeln!(out, "cursor closed"),
            _ => {}
        }
        let lines = match kind {
            "tables" => value["items"].as_array(),
            "explain" => value["lines"].as_array(),
            "help" => value.as_array(),
            _ => None,
        };
        if let Some(lines) = lines {
            for line in lines {
                writeln!(out, "{}", line.as_str().unwrap_or(""))?;
            }
            return Ok(());
        }
        let items = match &value {
            Json::Array(a) => a.clone(),
            Json::Object(o) if o.values().any(Json::is_array) => o
                .iter()
                .flat_map(|(k, v)| v.as_array().into_iter().flatten().map(|v| json!({k:v})))
                .collect(),
            _ => vec![value],
        };
        let mut rows = Vec::new();
        let mut columns = std::collections::BTreeSet::new();
        for item in items {
            let mut row = std::collections::BTreeMap::new();
            flatten("", &item, &mut row);
            columns.extend(row.keys().cloned());
            rows.push(row);
        }
        let columns = columns.into_iter().collect::<Vec<_>>();
        let unknown = Value::Text("unknown".into());
        let rows = rows
            .iter()
            .map(|r| {
                columns
                    .iter()
                    .map(|c| match r.get(c) {
                        Some(Value::Null) if c == "page_slack_ratio" => unknown.clone(),
                        cell => cell.cloned().unwrap_or(Value::Null),
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        write!(out, "{}", table(&columns, &rows))
    }
}
/// The class of a refusal, by what the caller can do: io for the store and the writer it reaches,
/// usage for misuse of the shell, sql for the statement.
pub(crate) fn class(error: &Error) -> &'static str {
    match error {
        Error::StoreMissing { .. }
        | Error::DatabaseLocked { .. }
        | Error::HeldByReaders { .. }
        | Error::StoreCorrupted { .. }
        | Error::StoreIo { .. }
        | Error::MemoryBudgetExceeded { .. }
        | Error::DiskLimitExceeded { .. }
        | Error::ReadLimitExceeded { .. }
        | Error::OwnerNotRunning
        | Error::OwnerNotServing
        | Error::OwnerRefused { .. }
        | Error::OwnerLimitExceeded
        | Error::OwnerDisconnected => "io",
        Error::CursorInvalidStatement => "usage",
        _ => "sql",
    }
}
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
            // Padded by hand: a formatter refuses a width past 65,535.
            let pad = " ".repeat(width - cell.chars().count());
            line.push_str(&format!(" {cell}{pad} |"));
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

fn display(v: &Json) -> Value {
    match v {
        Json::Null => Value::Null,
        Json::String(s) => Value::Text(s.clone()),
        v => Value::Json(v.clone()),
    }
}
fn flatten(prefix: &str, v: &Json, row: &mut std::collections::BTreeMap<String, Value>) {
    match v {
        Json::Object(o) => {
            for (k, v) in o {
                flatten(
                    &if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    },
                    v,
                    row,
                );
            }
        }
        Json::Array(a) => {
            for (i, v) in a.iter().enumerate() {
                flatten(&format!("{prefix}.{i}"), v, row);
            }
        }
        v => {
            row.insert(prefix.into(), display(v));
        }
    }
}
