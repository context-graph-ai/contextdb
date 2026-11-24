//! Runs every script in `tests/sql/`, in name order, as one test.
//!
//! A script is statements, each ending with `;` at the end of a line. A line of `----` after a
//! statement begins its expected output, which ends at the next blank line; a statement without
//! one must succeed. A SELECT prints its column names and then one row per line, a write prints
//! `rows affected: N`, and a refusal prints `error: ` and the kind's name. An expected refusal
//! can append ` | ` and its full wording to check both; a kind alone checks only the kind.
//! An answer's first line ends with its counted work, `  (read R, compared C)`: the rows the
//! statement read and the stored vectors it compared, so that a change of plan shows as a diff.
//! An expected cell written `*` stands for any value. A line starting with `#` between
//! statements is a comment of the script.
//!
//! `SQL_SCRIPT=keys.sql cargo test --test sql` runs one file, and
//! `UPDATE_EXPECTED=1 cargo test --test sql` rewrites in place the expected lines that differ,
//! keeping every line that still matches as written, `*` included.

mod support;

use std::path::Path;

use contextdb::{Error, QueryResult, Value, params};

#[test]
fn every_script_gives_its_expected_output() {
    let folder = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let only = std::env::var("SQL_SCRIPT").ok();
    let mut names: Vec<String> = std::fs::read_dir(&folder)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".sql") && only.as_ref().is_none_or(|only| only == name))
        .collect();
    names.sort();
    assert!(!names.is_empty(), "no script matches SQL_SCRIPT={only:?}");
    let source = support::folder("sql-source");
    support::close(support::open(&source));
    let mut failures = Vec::new();
    for name in &names {
        failures.extend(failures_of(&folder, name, &source));
    }
    let report = failures.join("\n\n");
    assert!(report.is_empty(), "statements that failed:\n\n{report}");
    // Both are tools for a developer: a run with either set never passes, so no environment
    // gives a pass that a clean one would not.
    for tool in ["SQL_SCRIPT", "UPDATE_EXPECTED"] {
        assert!(
            std::env::var_os(tool).is_none(),
            "{tool} is set: this run is a tool, not a check"
        );
    }
}

/// Runs one script in a fresh store and gives back what went wrong, each with its file and line.
fn failures_of(folder: &Path, name: &str, source: &Path) -> Vec<String> {
    let path = folder.join(name);
    let text = std::fs::read_to_string(&path).unwrap();
    let store = support::folder(&format!("sql-{name}"));
    std::fs::copy(source.join("data"), store.join("data")).expect("copy the empty store");
    let db = support::open(&store);
    let mut lines = text.lines().peekable();
    let (mut line_number, mut begins) = (0, 0);
    let mut rewritten = String::new();
    let mut statement = String::new();
    let mut failures = Vec::new();
    while let Some(line) = lines.next() {
        line_number += 1;
        rewritten.push_str(line);
        rewritten.push('\n');
        if statement.is_empty() {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            begins = line_number;
        }
        statement.push_str(line);
        statement.push('\n');
        let Some(sql) = statement.trim_end().strip_suffix(';') else {
            continue;
        };
        let actual = printed(db.execute(sql, &params! {}));
        let at = format!("{name}:{begins}:\n{statement}");
        statement.clear();
        if lines.next_if_eq(&"----").is_none() {
            if actual[0].starts_with("error: ") {
                failures.push((false, format!("{at}must succeed, and gave {}", actual[0])));
            }
            continue;
        }
        let mut expected = Vec::new();
        while let Some(line) = lines.next_if(|line| !line.trim().is_empty()) {
            expected.push(line);
        }
        line_number += 1 + expected.len();
        let kept: Vec<&str> = (actual.iter().enumerate())
            .map(|(i, a)| match expected.get(i) {
                Some(e) if same_line(e, a) => *e,
                _ => a.as_str(),
            })
            .collect();
        rewritten.push_str("----\n");
        rewritten.push_str(&kept.join("\n"));
        rewritten.push('\n');
        if kept != expected {
            let (expected, actual) = (expected.join("\n"), actual.join("\n"));
            failures.push((
                true,
                format!("{at}expected:\n{expected}\nactual:\n{actual}"),
            ));
        }
    }
    if !statement.trim().is_empty() {
        failures.push((false, format!("{name}:{begins}:\n{statement}has no `;`")));
    }
    let update = std::env::var_os("UPDATE_EXPECTED").is_some();
    if update && rewritten != text {
        std::fs::write(&path, rewritten).unwrap();
    }
    failures.retain(|(differs, _)| !(update && *differs));
    failures.into_iter().map(|(_, failure)| failure).collect()
}

/// An expected line matches when every cell is the actual cell or is written `*`.
fn same_line(expected: &str, actual: &str) -> bool {
    if expected.starts_with("error: ") && !expected.contains(" | ") {
        return actual.split(" | ").next() == Some(expected);
    }
    let wild = |(e, a): (&str, &str)| e == "*" || e == a;
    let (expected, actual) = (expected.split(" | "), actual.split(" | "));
    expected.clone().count() == actual.clone().count() && expected.zip(actual).all(wild)
}

/// What a statement gave, as the lines a script expects.
fn printed(result: Result<QueryResult, Error>) -> Vec<String> {
    let result = match result {
        Err(error) => {
            let debug = format!("{error:?}");
            let kind = debug.split(|c: char| !c.is_ascii_alphanumeric()).next();
            return vec![format!("error: {} | {error}", kind.unwrap_or_default())];
        }
        Ok(result) => result,
    };
    let (read, compared) = (result.rows_examined(), result.distance_evaluations());
    let work = format!("  (read {read}, compared {compared})");
    if result.columns().is_empty() {
        return vec![format!("rows affected: {}{work}", result.rows_affected())];
    }
    let mut lines = vec![format!("{}{work}", result.columns().join(" | "))];
    for row in result.rows() {
        lines.push(row.iter().map(cell).collect::<Vec<_>>().join(" | "));
    }
    lines
}

/// The one way a value is printed in a script. A REAL keeps its decimal point, a TIMESTAMP and
/// a transaction id say what they are, and JSON is printed compactly, so that a value stored as
/// the wrong type shows in the expected output.
fn cell(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => format!("{value:?}"),
        Value::Text(value) => value.clone(),
        Value::Uuid(value) => value.to_string(),
        Value::Timestamp(value) => format!("Timestamp({value})"),
        Value::Json(value) => value.to_string(),
        Value::Vector(value) => format!("{value:?}"),
        Value::TxId(value) => format!("TxId({})", value.0),
    }
}
