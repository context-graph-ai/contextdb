//! Runs the SQL examples of the documentation: every fenced `sql` block of `README.md` and
//! `docs/*.md` at the repository's root, each page top to bottom in a fresh store.
//!
//! A statement ends at a `;` that ends a line, and a new one begins at an unindented line that
//! starts with a statement's first word (the unindented `SELECT` that closes a `WITH` belongs to
//! it). `BEGIN` opens one transaction and `COMMIT` or `ROLLBACK` ends it; `.explain` runs the
//! statement it is given. A `CREATE TABLE` of a name the page has already declared starts that
//! table again: the earlier one is dropped first. A `$name` is bound from `bound`.
//!
//! A statement must do what its page shows in the comments that follow it: `-- Err(...)` is the
//! refusal, as `Debug` prints it; `-- Error: ...` is the refusal's sentence; `-- refused` is any
//! refusal; lines of cells two spaces apart that begin with the result's first column name are
//! the result; anything else means the statement runs. `tests/docs/expected.tsv` lists the
//! blocks that do not, with what the named statements do now (`ok`, `refused`, or the beginning
//! of the refusal's `Debug` text; `*` is every statement not named). A block that is not listed
//! and differs from its page fails, and so does a listed block that does what its page shows.

use std::collections::HashMap;
use std::path::Path;

use contextdb::serde_json::json;
use contextdb::{Database, QueryResult, Transaction, Uuid, Value};

const FIRST_WORDS: [&str; 16] = [
    "ALTER", "BEGIN", "COMMIT", "CREATE", "DECLARE", "DELETE", "DISCARD", "DROP", "INSERT",
    "PURGE", "ROLLBACK", "SELECT", "SET", "SHOW", "UPDATE", "WITH",
];

/// The value every `$name` of the pages is bound to. A name that is not listed is an id.
fn bound(name: &str) -> Value {
    match name {
        "query" | "query_vector" => Value::Vector(unit_vector()),
        name if name.ends_with("embedding") => Value::Vector(unit_vector()),
        "data" => Value::Json(json!({"region": "us-east-1"})),
        "name" | "name1" | "name2" | "session" => Value::Text(name.to_string()),
        "edge_type" => Value::Text("CITES".to_string()),
        "cutoff" => Value::Timestamp(0),
        "n" | "m" => Value::Int64(1),
        "include_drafts" => Value::Bool(true),
        id => Value::Uuid(Uuid::from_u128(id.bytes().fold(0, |sum: u128, byte| {
            sum.wrapping_mul(257).wrapping_add(byte.into())
        }))),
    }
}

/// The pages' vector columns that take a bound value all have 384 numbers.
fn unit_vector() -> Vec<f32> {
    (0..384).map(|at| if at == 0 { 1.0 } else { 0.0 }).collect()
}

#[derive(Default)]
struct Statement {
    text: String,
    /// The comments on and after the statement, up to the next one.
    notes: Vec<String>,
}

enum Got {
    Done(Option<QueryResult>),
    Refused { kind: String, sentence: String },
}

#[derive(Debug)]
enum Want {
    Done,
    Refused,
    Kind(String),
    Sentence(String),
}

#[test]
fn every_sql_example_of_the_documentation_does_what_its_page_shows() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut pages: Vec<String> = std::fs::read_dir(root.join("docs"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".md"))
        .map(|name| format!("docs/{name}"))
        .collect();
    pages.push("README.md".to_string());
    pages.sort();
    let mut expected = expected();
    let (mut blocks, mut failures) = (0, Vec::new());
    for page in &pages {
        let text = std::fs::read_to_string(root.join(page)).unwrap();
        let store = Path::new(env!("CARGO_TARGET_TMPDIR")).join(page.replace('/', "-"));
        if store.exists() {
            std::fs::remove_dir_all(&store).unwrap();
        }
        let db = Database::open(&store).expect("a fresh store opens");
        let mut transaction = None;
        let (mut number, mut begins, mut block) = (0, 0, None::<String>);
        for (at, line) in text.lines().enumerate() {
            let Some(body) = &mut block else {
                if line.trim() == "```sql" {
                    (begins, block) = (at + 1, Some(String::new()));
                }
                continue;
            };
            if line.trim() != "```" {
                body.push_str(line);
                body.push('\n');
                continue;
            }
            number += 1;
            blocks += 1;
            let listed = expected.remove(&(page.clone(), number));
            let mut as_the_page_shows = true;
            for (at, statement) in statements(body).iter().enumerate() {
                let got = run(&db, &mut transaction, &statement.text);
                let differs = wrong(&shown(statement), &got, &statement.notes);
                as_the_page_shows &= differs.is_none();
                let now = listed.as_ref().and_then(|listed| {
                    let named = |which: &str| listed.iter().find(|(named, _)| named == which);
                    named(&(at + 1).to_string()).or_else(|| named("*"))
                });
                let wrong = match now {
                    Some((_, now)) => wrong(&listed_as(now), &got, &[]),
                    None => differs,
                };
                if let Some(wrong) = wrong {
                    failures.push(format!(
                        "{page}:{begins} block {number} statement {}: {wrong}\n    {}",
                        at + 1,
                        statement.text.lines().next().unwrap_or_default()
                    ));
                }
            }
            if listed.is_some() && as_the_page_shows {
                failures.push(format!(
                    "{page}:{begins} block {number} is listed in expected.tsv and does what its \
                     page shows: take its row out"
                ));
            }
            (transaction, block) = (None, None);
        }
    }
    let mut unused: Vec<String> = expected
        .keys()
        .map(|(page, number)| format!("expected.tsv lists block {number} of {page}: no such block"))
        .collect();
    unused.sort();
    failures.extend(unused);
    assert!(blocks > 0, "the pages have no sql block");
    let report = failures.join("\n");
    assert!(report.is_empty(), "of {blocks} blocks:\n{report}");
}

/// The rows of `expected.tsv`: for a page and a block of it, what the named statements do now.
fn expected() -> HashMap<(String, usize), Vec<(String, String)>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/docs/expected.tsv");
    let text = std::fs::read_to_string(path).unwrap();
    let mut rows = HashMap::new();
    for line in text.lines().skip(1) {
        let cells: Vec<&str> = line.split('\t').collect();
        assert!(
            cells.len() == 5,
            "expected.tsv: a row is five cells: {line}"
        );
        assert!(
            ["later", "changed", "page"].contains(&cells[2]),
            "expected.tsv: list: {line}"
        );
        let now = cells[3]
            .split("; ")
            .map(|entry| {
                entry
                    .split_once('=')
                    .expect("expected.tsv: an entry is which=what")
            })
            .map(|(which, what)| (which.to_string(), what.to_string()))
            .collect();
        let block = cells[1].parse().expect("expected.tsv: a block is a number");
        assert!(
            rows.insert((cells[0].to_string(), block), now).is_none(),
            "listed twice: {line}"
        );
    }
    rows
}

/// The code of a line and the comment after it.
fn split_comment(line: &str) -> (&str, &str) {
    let mut quoted = false;
    for (at, character) in line.char_indices() {
        quoted ^= character == '\'';
        if !quoted && line[at..].starts_with("--") {
            return (&line[..at], line[at + 2..].trim());
        }
    }
    (line, "")
}

fn statements(block: &str) -> Vec<Statement> {
    let mut block = block.to_string();
    while let Some(from) = block.find("/*") {
        let Some(len) = block[from..].find("*/") else {
            break;
        };
        block.replace_range(from..from + len + 2, "");
    }
    let (mut done, mut open, mut waits_for_select) = (Vec::new(), None::<Statement>, false);
    for line in block.lines() {
        let (code, note) = split_comment(line);
        if code.trim().is_empty() {
            if let Some(statement) = open
                .as_mut()
                .or(done.last_mut())
                .filter(|_| !note.is_empty())
            {
                statement.notes.push(note.to_string());
            }
            continue;
        }
        let first = code
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or_default();
        let first = first.to_ascii_uppercase();
        let command = code.starts_with('.');
        let closes_with = first == "SELECT" && waits_for_select;
        if command || (FIRST_WORDS.contains(&first.as_str()) && !closes_with) {
            done.extend(open.take());
        }
        if open.is_none() || closes_with {
            waits_for_select = open.is_none() && first == "WITH";
        }
        let statement = open.get_or_insert_with(Statement::default);
        let code = code.trim_end();
        statement.text.push_str(code.trim_end_matches(';'));
        statement.text.push('\n');
        if !note.is_empty() {
            statement.notes.push(note.to_string());
        }
        if command || code.ends_with(';') {
            done.extend(open.take());
        }
    }
    done.extend(open);
    done
}

fn run<'db>(db: &'db Database, open: &mut Option<Transaction<'db>>, text: &str) -> Got {
    let text = text.trim().strip_prefix(".explain").unwrap_or(text).trim();
    let upper = text.to_ascii_uppercase();
    let result = match (upper.as_str(), open.take()) {
        ("BEGIN", None) => db.transaction().map(|transaction| {
            *open = Some(transaction);
            None
        }),
        ("COMMIT", Some(transaction)) => transaction.commit().map(|()| None),
        ("ROLLBACK", Some(transaction)) => {
            transaction.rollback();
            Ok(None)
        }
        (_, transaction) => {
            *open = transaction;
            let again = upper.strip_prefix("CREATE TABLE ").map(|rest| {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                name.to_ascii_lowercase()
            });
            let params: HashMap<String, Value> = text
                .split('$')
                .skip(1)
                .map(|rest| {
                    rest.chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect()
                })
                .map(|name: String| (name.clone(), bound(&name)))
                .collect();
            let dropped = match again.filter(|name| db.table_names().contains(name)) {
                Some(name) => drop_table(db, &name),
                None => Ok(()),
            };
            dropped
                .and_then(|()| match open {
                    Some(transaction) => transaction.execute(text, &params),
                    None => db.execute(text, &params),
                })
                .map(Some)
        }
    };
    match result {
        Ok(result) => Got::Done(result),
        Err(error) => Got::Refused {
            kind: format!("{error:?}"),
            sentence: error.to_string(),
        },
    }
}

/// What the page shows a statement doing.
fn shown(statement: &Statement) -> Want {
    for note in &statement.notes {
        if let Some(kind) = note
            .strip_prefix("Err(")
            .and_then(|rest| rest.strip_suffix(')'))
        {
            return Want::Kind(kind.to_string());
        }
        if let Some(sentence) = note.strip_prefix("Error: ") {
            return Want::Sentence(sentence.to_string());
        }
        if note.starts_with("refused") {
            return Want::Refused;
        }
    }
    Want::Done
}

fn listed_as(now: &str) -> Want {
    match now {
        "ok" => Want::Done,
        "refused" => Want::Refused,
        kind => Want::Kind(kind.to_string()),
    }
}

/// How what a statement did differs from what was wanted, if it does.
fn wrong(want: &Want, got: &Got, notes: &[String]) -> Option<String> {
    match (want, got) {
        (Want::Done, Got::Done(result)) => {
            result.as_ref().and_then(|result| wrong_rows(result, notes))
        }
        (Want::Refused, Got::Refused { .. }) => None,
        (Want::Kind(kind), Got::Refused { kind: got, .. }) if got.starts_with(kind) => None,
        (Want::Sentence(sentence), Got::Refused { sentence: got, .. }) if got == sentence => None,
        (want, Got::Done(_)) => Some(format!("wanted {want:?}, and it ran")),
        (want, Got::Refused { kind, sentence }) => {
            Some(format!("wanted {want:?}, got {kind}: {sentence}"))
        }
    }
}

/// How a result differs from the one the comments after its statement show, if they show one.
fn wrong_rows(result: &QueryResult, notes: &[String]) -> Option<String> {
    let cells = |line: &String| -> Vec<String> {
        line.split("  ")
            .map(str::trim)
            .filter(|cell| !cell.is_empty())
            .map(String::from)
            .collect()
    };
    let at = notes.iter().position(|note| {
        let cells = cells(note);
        cells.len() > 1 && cells.first() == result.columns().first()
    })?;
    let shown: Vec<Vec<String>> = notes[at..].iter().map(cells).collect();
    let mut got = vec![result.columns().to_vec()];
    got.extend(result.rows().iter().map(|row| {
        row.iter()
            .map(|value| match value {
                Value::Null => "NULL".to_string(),
                Value::Text(text) => text.clone(),
                Value::Int64(number) => number.to_string(),
                Value::Float64(number) => format!("{number:?}"),
                Value::Uuid(id) => id.to_string(),
                other => format!("{other:?}"),
            })
            .collect()
    }));
    (shown != got).then(|| format!("the page shows the rows {shown:?}, got {got:?}"))
}

/// Drops a table the page declares again, and first every table that refers to it: a page's
/// later example may declare a table an earlier one of the page's tables refers to.
fn drop_table(db: &Database, name: &str) -> contextdb::Result<()> {
    let reference = format!("references {name}(");
    for other in db.table_names() {
        let refers = other != name
            && db
                .declaration(&other)
                .is_ok_and(|text| text.to_ascii_lowercase().contains(&reference));
        if refers {
            drop_table(db, &other)?;
        }
    }
    db.execute(&format!("DROP TABLE {name}"), &HashMap::new())
        .map(drop)
}
