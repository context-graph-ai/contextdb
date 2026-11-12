//! Runs the built `contextdb` program the way a script does: statements piped to its standard
//! input, and its output, its error stream and its exit status read back.
//!
//! Each test has its own folder under the build's temporary folder, left behind for inspection.

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use contextdb::{Database, params};
use serde_json::{Value as Json, json};

/// What one run of the program gave back.
struct Run {
    stdout: String,
    stderr: String,
    code: Option<i32>,
}

impl Run {
    /// The lines of the error stream that report a refused statement to a person.
    fn errors(&self) -> Vec<&str> {
        self.stderr
            .lines()
            .filter(|line| line.starts_with("Error:"))
            .collect()
    }
}

/// An empty folder of this test's own.
fn folder(test: &str) -> PathBuf {
    let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("shell-{test}"));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).unwrap();
    }
    std::fs::create_dir_all(&folder).unwrap();
    folder
}

/// Runs `contextdb` in `dir` with these arguments and this text piped to its standard input.
///
/// The input is written before anything is read back. Every input here is far smaller than a
/// pipe holds, so the write cannot wait on the program.
fn shell(dir: &Path, args: &[&str], input: impl AsRef<[u8]>) -> Run {
    shell_to(dir, args, input, false)
}

/// [`shell`], with the program's output closed before it writes when `closed` is set.
fn shell_to(dir: &Path, args: &[&str], input: impl AsRef<[u8]>, closed: bool) -> Run {
    let mut child = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if closed {
        drop(child.stdout.take());
    }
    // A program that refuses its command line exits without reading its input.
    match child.stdin.take().unwrap().write_all(input.as_ref()) {
        Err(error) if error.kind() != ErrorKind::BrokenPipe => panic!("{error}"),
        _ => {}
    }
    let output = child.wait_with_output().unwrap();
    let run = Run {
        stdout: String::from_utf8(output.stdout).unwrap(),
        stderr: String::from_utf8(output.stderr).unwrap(),
        code: output.status.code(),
    };
    assert!(
        !run.stderr.contains("panicked at"),
        "the program panicked:\n{}",
        run.stderr
    );
    run
}

/// A path as a command-line argument.
fn arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// Every line of a stream as a JSON document.
fn documents(stream: &str) -> Vec<Json> {
    stream
        .lines()
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|_| panic!("not a JSON document: {line}"))
        })
        .collect()
}

/// The names in a folder, sorted.
fn listing(folder: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(folder)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// The tables a run printed for a person, in order: each is its header and then its rows, cell
/// by cell. A result with no rows prints no table and is an empty list here.
fn tables(stdout: &str) -> Vec<Vec<Vec<String>>> {
    let mut tables = Vec::new();
    let mut table = Vec::new();
    for line in stdout.lines() {
        let cells = line
            .strip_prefix('|')
            .and_then(|rest| rest.strip_suffix('|'));
        if let Some(cells) = cells {
            let cells = cells.split('|').map(|cell| cell.trim().to_owned());
            table.push(cells.collect());
        } else if line.starts_with('(') {
            tables.push(std::mem::take(&mut table));
        }
    }
    tables
}

/// The rows of a printed search whose columns are `id` and `distance`: each row's key and its
/// distance as a number, however the program writes a whole REAL.
fn nearest(table: &[Vec<String>]) -> Vec<(i64, f64)> {
    assert_eq!(table[0], ["id", "distance"], "{table:?}");
    table[1..]
        .iter()
        .map(|row| (row[0].parse().unwrap(), row[1].parse().unwrap()))
        .collect()
}

/// A vector in a JSON document: an array of numbers.
fn numbers(vector: &Json) -> Vec<f32> {
    vector
        .as_array()
        .unwrap_or_else(|| panic!("not a JSON array: {vector}"))
        .iter()
        .map(|number| {
            number
                .as_f64()
                .unwrap_or_else(|| panic!("not a number: {number}")) as f32
        })
        .collect()
}

const CREATE_NOTES: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);\n";
const SELECT_NOTES: &str = "SELECT id, body FROM notes ORDER BY id;\n";

const ONE_NOTE: &str = "\
+----+-------+
| id | body  |
+----+-------+
| 1  | first |
+----+-------+
(1 row)
";

const TWO_NOTES: &str = "\
+----+--------+
| id | body   |
+----+--------+
| 1  | first  |
| 2  | second |
+----+--------+
(2 rows)
";

#[test]
fn reading_a_missing_store_is_refused_and_creates_nothing() {
    let dir = folder("reading-a-missing-store");
    let store = dir.join("store");

    let run = shell(&dir, &[arg(&store)], SELECT_NOTES);
    assert_eq!(run.code, Some(1), "{}", run.stderr);
    assert_eq!(run.stdout, "");
    assert!(
        run.stderr.contains("there is no store at this path")
            && run.stderr.contains("--write creates it"),
        "{}",
        run.stderr
    );
    assert_eq!(
        listing(&dir),
        Vec::<String>::new(),
        "a refused read creates nothing"
    );

    // The same refusal for a program that reads it: one document on the error stream, class io.
    // A path whose `data` is a folder cannot be read as a store either: kind store_io.
    let broken = dir.join("broken");
    std::fs::create_dir_all(broken.join("data")).unwrap();
    for (path, kind) in [(&store, "store_not_found"), (&broken, "store_io")] {
        let run = shell(&dir, &[arg(path), "--json"], SELECT_NOTES);
        assert_eq!(run.code, Some(1), "{}", run.stderr);
        assert_eq!(run.stdout, "");
        let refusals = documents(&run.stderr);
        assert_eq!(refusals.len(), 1, "{}", run.stderr);
        assert_eq!(
            refusals[0]["error"]["detail"]["kind"], kind,
            "{}",
            run.stderr
        );
        assert_eq!(
            refusals[0]["error"]["class"], "io",
            "a store that cannot be read is a failure of the storage, not of the statement: {}",
            run.stderr
        );
    }
    assert_eq!(listing(&dir), ["broken"], "a refused read creates nothing");
}

#[test]
fn write_creates_a_missing_store() {
    let dir = folder("write-creates-a-missing-store");
    let store = dir.join("store");

    let insert = "INSERT INTO notes (id, body) VALUES (1, 'first');\n";
    let run = shell(
        &dir,
        &[arg(&store), "--write"],
        format!("{CREATE_NOTES}{insert}"),
    );
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.errors(), Vec::<&str>::new());
    assert!(
        store.is_dir(),
        "the store is one folder at the path that was given"
    );
    assert_eq!(
        listing(&dir),
        ["store"],
        "nothing is written beside the store's folder"
    );

    // It was a whole read-write session: the next shell reads what it wrote.
    let run = shell(&dir, &[arg(&store)], SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, ONE_NOTE);

    // On a store that is there, --write opens it and keeps what it holds.
    let insert = "INSERT INTO notes (id, body) VALUES (2, 'second');\n";
    let run = shell(
        &dir,
        &[arg(&store), "--write"],
        format!("{insert}{SELECT_NOTES}"),
    );
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert!(run.stdout.contains(TWO_NOTES), "{}", run.stdout);
}

#[test]
fn a_removed_option_is_refused_by_name_with_exit_2_before_the_store_opens() {
    let dir = folder("a-removed-option-is-refused");
    let store = dir.join("store");
    let removed = [
        ["--read-owner-connect-ms", "1000"],
        ["--read-owner-routing-retry-ms", "1000"],
        ["--read-owner-response-ms", "11000"],
        ["--owner-read-concurrency", "4"],
        ["--owner-read-request-ms", "10000"],
        ["--owner-read-result-rows", "500"],
    ];
    for [option, value] in removed {
        // With --write a store would be created here if the program got as far as opening one.
        for extra in [&[][..], &["--json"][..]] {
            let mut args = vec![arg(&store), "--write", option, value];
            args.extend_from_slice(extra);
            let run = shell(&dir, &args, CREATE_NOTES);
            assert_eq!(run.code, Some(2), "{option}: {}", run.stderr);
            assert!(
                run.stderr.contains(option),
                "{option} is not named in: {}",
                run.stderr
            );
            assert_eq!(run.stdout, "", "{option}");
            // The command line is refused in the argument parser's own words, with or
            // without --json.
            assert!(
                !run.stderr.trim_start().starts_with('{'),
                "{option}: {}",
                run.stderr
            );
            assert_eq!(
                listing(&dir),
                Vec::<String>::new(),
                "{option}: the store was opened"
            );
        }
    }

    // An option the program never had, and a command line with no store on it, are refused the
    // same way: exit 2 in the argument parser's words, with nothing opened or created.
    let unknown = shell(
        &dir,
        &[arg(&store), "--write", "--frobnicate"],
        CREATE_NOTES,
    );
    assert_eq!(unknown.code, Some(2), "{}", unknown.stderr);
    assert!(
        unknown.stderr.contains("--frobnicate"),
        "{}",
        unknown.stderr
    );
    assert_eq!(unknown.stdout, "");
    for no_path in [&[][..], &["--write"][..], &["--write", "--json"][..]] {
        let run = shell(&dir, no_path, CREATE_NOTES);
        assert_eq!(run.code, Some(2), "{no_path:?}: {}", run.stderr);
        assert_eq!(run.stdout, "", "{no_path:?}");
        assert!(
            !run.stderr.trim_start().starts_with('{'),
            "{no_path:?}: {}",
            run.stderr
        );
    }
    assert_eq!(
        listing(&dir),
        Vec::<String>::new(),
        "a refused command line created something"
    );
}

#[test]
fn a_folder_named_like_a_command_opens_when_written_as_a_path() {
    let dir = folder("a-folder-named-like-a-command");
    let made = shell(
        &dir,
        &["./server", "--write"],
        "CREATE TABLE notes (id INTEGER PRIMARY KEY);\nINSERT INTO notes (id) VALUES (7);\n",
    );
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    assert!(dir.join("server").is_dir());

    // Written as a path, the folder is the store.
    let as_path = shell(&dir, &["./server"], "SELECT id FROM notes;\n");
    assert_eq!(as_path.code, Some(0), "{}", as_path.stderr);
    assert_eq!(
        as_path.stdout,
        "+----+\n| id |\n+----+\n| 7  |\n+----+\n(1 row)\n"
    );

    // Written bare, the word is the command, which this version refuses by name; the store
    // beside it is not opened as a shell.
    let as_command = shell(&dir, &["server"], "SELECT id FROM notes;\n");
    assert_ne!(as_command.code, Some(0));
    assert!(
        as_command.stderr.contains("server"),
        "{}",
        as_command.stderr
    );
    assert!(!as_command.stdout.contains("| 7"), "{}", as_command.stdout);
}

#[test]
fn piped_statements_all_run_and_a_refused_one_fails_the_run() {
    let dir = folder("piped-statements-all-run");
    let store = dir.join("store");
    let input = "\
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
.tables
INSERT INTO notes (id, body) VALUES (1, 'first');
INSERT INTO notes (id, body) VALUES (1, 'again');
SELECT id,
       body
  FROM notes
 ORDER BY id;
SELECT nope FROM notes;
INSERT INTO notes (id, body) VALUES (2, 'second');
SELECT COUNT(*) AS n FROM notes;
";
    let run = shell(&dir, &[arg(&store), "--write"], input);

    // A statement may span lines, a command is one line, and every result is on the output.
    let first = run
        .stdout
        .find(ONE_NOTE)
        .expect("the statement written over four lines ran");
    assert!(
        run.stdout.lines().any(|line| line == "notes"),
        "{}",
        run.stdout
    );

    // Each refused statement is reported on the error stream, never as a result and never as
    // a success: three statements changed something and three say so.
    assert_eq!(run.errors().len(), 2, "{}", run.stderr);
    assert!(!run.stdout.contains("Error"), "{}", run.stdout);
    let ok: Vec<&str> = run
        .stdout
        .lines()
        .filter(|line| line.starts_with("ok ("))
        .collect();
    assert_eq!(
        ok,
        [
            "ok (rows_affected=0)",
            "ok (rows_affected=1)",
            "ok (rows_affected=1)"
        ]
    );

    // The statements after a refused one still ran.
    let count = "+---+\n| n |\n+---+\n| 2 |\n+---+\n(1 row)\n";
    let last = run
        .stdout
        .find(count)
        .expect("the statements after a refused one ran");
    assert!(first < last);

    // No prompt and no banner on either stream, and the run failed because a statement did.
    for stream in [&run.stdout, &run.stderr] {
        assert!(
            !stream.contains("contextdb>") && !stream.contains("contextdb(ro)>"),
            "{stream}"
        );
        assert!(!stream.contains("session"), "{stream}");
    }
    assert_eq!(run.code, Some(1));

    // A run in which every statement succeeded exits 0 and prints its results and nothing else.
    let run = shell(&dir, &[arg(&store)], SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, TWO_NOTES);

    // Output that nobody reads any more ends the session quietly: no crash report, and the
    // exit status 0, as no statement was refused.
    let run = shell_to(
        &dir,
        &[arg(&store)],
        "SELECT id FROM notes ORDER BY id;\nSELECT id FROM notes ORDER BY id;\n",
        true,
    );
    assert_eq!(
        run.code,
        Some(0),
        "a closed output must end the run quietly with status 0: {}",
        run.stderr
    );

    // A line that is not valid text ENDS the run there: the refusal names the line, the status
    // is 1, and no statement after it runs, inside a transaction or not; one left open by the
    // end is not committed.
    for (script, line, kept) in [
        (
            "INSERT INTO notes (id, body) VALUES (10, 'before');\n",
            2,
            3,
        ),
        (
            "BEGIN;\nINSERT INTO notes (id, body) VALUES (12, 'inside');\n",
            3,
            3,
        ),
    ] {
        let mut input = script.as_bytes().to_vec();
        input.extend_from_slice(b"\xff\xfe\n");
        input.extend_from_slice(b"INSERT INTO notes (id, body) VALUES (11, 'after');\nCOMMIT;\n");
        let run = shell(&dir, &[arg(&store), "--write"], input);
        assert_eq!(
            run.code,
            Some(1),
            "an undecodable line must fail the run: {}",
            run.stderr
        );
        assert!(
            run.errors()
                .iter()
                .any(|error| error.contains(&format!("line {line}"))),
            "the refusal must name the line that cannot be decoded, line {line}: {}",
            run.stderr
        );
        let run = shell(&dir, &[arg(&store)], "SELECT COUNT(*) AS n FROM notes;\n");
        assert!(
            run.stdout.contains(&format!("| {kept} |")),
            "only the statements before the undecodable line stand ({kept} notes): {}",
            run.stdout
        );
    }
}

#[test]
fn json_prints_one_document_per_statement_with_columns_in_declared_order() {
    let dir = folder("json-prints-one-document-per-statement");
    let store = dir.join("store");
    let input = "\
CREATE TABLE notes (id INTEGER PRIMARY KEY, zeta TEXT, alpha TEXT);
INSERT INTO notes (id, zeta, alpha) VALUES (1, 'z1', 'a1'), (2, 'z2', NULL);
SELECT zeta, id, alpha FROM notes ORDER BY id;
SELECT * FROM notes WHERE id = 404;
SELECT nope FROM notes;
.tables
";
    let run = shell(&dir, &[arg(&store), "--write", "--json"], input);

    // One document per statement that gave a result, in the order they were given. `columns`
    // is the order the statement or the table declared, which the keys of a row cannot carry.
    let results = documents(&run.stdout);
    assert_eq!(results.len(), 5, "{}", run.stdout);
    assert_eq!(results[0], json!({"rows_affected": 0}));
    assert_eq!(results[1], json!({"rows_affected": 2}));
    assert_eq!(
        results[2],
        json!({"result": {
            "columns": ["zeta", "id", "alpha"],
            "rows": [
                {"zeta": "z1", "id": 1, "alpha": "a1"},
                {"zeta": "z2", "id": 2, "alpha": null},
            ],
        }})
    );
    assert_eq!(
        results[3],
        json!({"result": {"columns": ["id", "zeta", "alpha"], "rows": []}})
    );
    assert_eq!(results[4]["tables"]["items"], json!(["notes"]));

    // The keys of a row come in the order of `columns` too, so a consumer that reads the row
    // as written sees zeta, id, alpha and not the alphabet.
    let line = run.stdout.lines().nth(2).unwrap();
    let rows = &line[line.find("\"rows\"").expect("the document has rows")..];
    let at = |key: &str| {
        rows.find(&format!("\"{key}\""))
            .expect("the row has the key")
    };
    assert!(at("zeta") < at("id") && at("id") < at("alpha"), "{rows}");

    // The refused statement is a document too, on the error stream.
    let refusals: Vec<Json> = documents(&run.stderr)
        .into_iter()
        .filter(|document| document.get("error").is_some())
        .collect();
    assert_eq!(refusals.len(), 1, "{}", run.stderr);
    assert_eq!(refusals[0]["error"]["class"], "sql");
    assert!(refusals[0]["error"]["message"].is_string());
    assert_eq!(run.code, Some(1));
}

#[test]
fn a_read_shell_refuses_a_write_by_name() {
    let dir = folder("a-read-shell-refuses-a-write");
    let store = dir.join("store");
    let insert = "INSERT INTO notes (id, body) VALUES (1, 'first');\n";
    let made = shell(
        &dir,
        &[arg(&store), "--write"],
        format!("{CREATE_NOTES}{insert}"),
    );
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    let writes = "\
INSERT INTO notes (id, body) VALUES (2, 'second');
UPDATE notes SET body = 'changed';
DELETE FROM notes;
CREATE TABLE other (id INTEGER PRIMARY KEY);
BEGIN;
";
    // Each of the five is refused, telling the person what to add, and the read after them
    // answers with the row as it was.
    let run = shell(&dir, &[arg(&store)], format!("{writes}{SELECT_NOTES}"));
    assert_eq!(run.code, Some(1));
    assert_eq!(run.errors().len(), 5, "{}", run.stderr);
    assert!(
        run.errors().iter().all(|line| line.contains("--write")),
        "{}",
        run.stderr
    );
    assert!(run.stdout.contains(ONE_NOTE), "{}", run.stdout);
    assert!(!run.stdout.contains("ok ("), "{}", run.stdout);

    // A program reads the refusal's kind.
    let run = shell(&dir, &[arg(&store), "--json"], writes);
    assert_eq!(run.code, Some(1));
    let refusals: Vec<Json> = documents(&run.stderr)
        .into_iter()
        .filter(|document| document.get("error").is_some())
        .collect();
    assert_eq!(refusals.len(), 5, "{}", run.stderr);
    for refusal in &refusals {
        assert_eq!(
            refusal["error"]["detail"]["kind"], "write_requires_flag",
            "{refusal}"
        );
    }

    // Nothing was written: one row, one table.
    let run = shell(
        &dir,
        &[arg(&store), "--json"],
        format!("{SELECT_NOTES}.tables\n"),
    );
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    let results = documents(&run.stdout);
    assert_eq!(
        results[0]["result"]["rows"],
        json!([{"id": 1, "body": "first"}])
    );
    assert_eq!(results[1]["tables"]["items"], json!(["notes"]));
}

#[test]
fn a_read_shell_answers_while_a_writer_holds_the_store() {
    let dir = folder("a-read-shell-answers-while-a-writer-holds");
    let store = dir.join("store");

    // This test is the writer, and holds the store until the end.
    let writer = Database::open(&store).unwrap();
    writer
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &params! {},
        )
        .unwrap();
    writer
        .execute(
            "INSERT INTO notes (id, body) VALUES (1, 'first')",
            &params! {},
        )
        .unwrap();

    // The store really is held: a second writer is refused, naming this process.
    let second = shell(&dir, &[arg(&store), "--write"], SELECT_NOTES);
    assert_eq!(second.code, Some(1), "{}", second.stderr);
    assert!(
        second.stderr.contains(&std::process::id().to_string()),
        "{}",
        second.stderr
    );

    // A read shell needs nothing from the writer and answers.
    let run = shell(&dir, &[arg(&store)], SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, ONE_NOTE);

    // The writer was not held up by it, and the next read shell reads its next commit.
    writer
        .execute(
            "INSERT INTO notes (id, body) VALUES (2, 'second')",
            &params! {},
        )
        .unwrap();
    let run = shell(&dir, &[arg(&store)], SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, TWO_NOTES);

    writer.close().unwrap();
}

#[test]
fn begin_commit_and_rollback_run_a_transaction() {
    let dir = folder("begin-commit-and-rollback");
    let store = dir.join("store");
    let input = "\
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
BEGIN;
INSERT INTO notes (id, body) VALUES (1, 'first');
INSERT INTO notes (id, body) VALUES (2, 'second');
COMMIT;
BEGIN;
INSERT INTO notes (id, body) VALUES (3, 'third');
DELETE FROM notes WHERE id = 1;
SELECT id FROM notes ORDER BY id;
ROLLBACK;
SELECT id, body FROM notes ORDER BY id;
";
    let run = shell(&dir, &[arg(&store), "--write"], input);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.errors(), Vec::<&str>::new());

    // Inside the second transaction its own changes are read; after ROLLBACK they are gone and
    // what the first one committed is there.
    let inside = "+----+\n| id |\n+----+\n| 2  |\n| 3  |\n+----+\n(2 rows)\n";
    let seen_inside = run
        .stdout
        .find(inside)
        .expect("a transaction reads its own changes");
    let seen_after = run
        .stdout
        .find(TWO_NOTES)
        .expect("ROLLBACK left what COMMIT stored");
    assert!(seen_inside < seen_after);

    // The committed transaction reached the store, and the rolled-back one never did.
    let run = shell(&dir, &[arg(&store)], SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, TWO_NOTES);

    // As in Postgres, BEGIN inside a transaction and COMMIT or ROLLBACK outside one each warn on
    // the error stream and the run still ends well, so a script that lost its BEGIN is told.
    let run = shell(
        &dir,
        &[arg(&store), "--write"],
        "BEGIN;\nBEGIN;\nCOMMIT;\nCOMMIT;\nROLLBACK;\n",
    );
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    let warnings: Vec<&str> = run
        .stderr
        .lines()
        .filter(|line| line.starts_with("warning:"))
        .collect();
    assert_eq!(
        warnings,
        [
            "warning: there is already a transaction in progress",
            "warning: there is no transaction in progress",
            "warning: there is no transaction in progress",
        ],
        "{}",
        run.stderr
    );
}

#[test]
fn tables_and_schema_print_what_was_declared() {
    let dir = folder("tables-and-schema");
    let store = dir.join("store");
    // What `.schema notes` prints: the declaration as SQL. A column with no rule prints its name
    // and type alone, and nothing the author did not write is printed: no index behind the key
    // or the unique rules, no table option.
    let notes = "\
CREATE TABLE notes (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT,
  stars INTEGER DEFAULT 3,
  slug TEXT UNIQUE,
  UNIQUE (title, stars)
);
";
    let authors = "CREATE TABLE authors (id UUID PRIMARY KEY, name TEXT);\n";
    let made = shell(&dir, &[arg(&store), "--write"], format!("{notes}{authors}"));
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // `.tables` lists the names in order, one a line.
    let tables = shell(&dir, &[arg(&store)], ".tables\n");
    assert_eq!(tables.code, Some(0), "{}", tables.stderr);
    let names: Vec<&str> = tables
        .stdout
        .lines()
        .filter(|line| ["authors", "notes"].contains(line))
        .collect();
    assert_eq!(names, ["authors", "notes"], "{}", tables.stdout);

    let schema = shell(&dir, &[arg(&store)], ".schema notes\n");
    assert_eq!(schema.code, Some(0), "{}", schema.stderr);
    assert_eq!(schema.stdout, notes);

    // Each command has a short spelling that prints the same.
    assert_eq!(shell(&dir, &[arg(&store)], "\\dt\n").stdout, tables.stdout);
    assert_eq!(
        shell(&dir, &[arg(&store)], "\\d notes\n").stdout,
        schema.stdout
    );

    // What `.schema` printed declares the same table again in another store.
    let copy = dir.join("copy");
    let made = shell(&dir, &[arg(&copy), "--write"], &schema.stdout);
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    assert_eq!(shell(&dir, &[arg(&copy)], ".schema notes\n").stdout, notes);

    // A command used wrongly prints how it is used, never that it is unknown.
    for (input, command) in [(".schema\n", ".schema"), (".tables extra\n", ".tables")] {
        let run = shell(&dir, &[arg(&store)], input);
        let usage = run.stderr.lines().find(|line| line.starts_with("Usage:"));
        assert!(
            usage.is_some_and(|line| line.contains(command)),
            "{command} used wrongly must print a Usage: line that names it:\n{}",
            run.stderr
        );
        assert!(
            !run.stderr.contains("Unknown command"),
            "{command} is a command; its wrong use was called unknown:\n{}",
            run.stderr
        );
    }
}

#[test]
fn the_memory_limit_option_is_the_sessions_ceiling() {
    let dir = folder("the-memory-limit-option");
    let store = dir.join("store");

    // The option is the ceiling the session reports, in bytes, with K, M and G as powers of
    // 1024; without it there is none.
    let limit = |args: &[&str]| {
        let run = shell(&dir, args, "SHOW MEMORY_LIMIT;\n");
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        documents(&run.stdout)[0]["result"]["rows"][0]["limit"].clone()
    };
    assert_eq!(limit(&[arg(&store), "--write", "--json"]), json!("none"));
    // With no limit a session counts 9 MiB as used before any statement: 1 MiB and a page
    // cache of 8 MiB. A read shell with no limit counts the same.
    for args in [
        &[arg(&store), "--write", "--json"][..],
        &[arg(&store), "--json"],
    ] {
        let run = shell(&dir, args, "SHOW MEMORY_LIMIT;\n");
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        assert_eq!(
            documents(&run.stdout)[0]["result"]["rows"][0]["used"],
            json!(9_437_184_u64),
            "used with no limit"
        );
    }
    for (size, bytes) in [
        ("65536K", 67_108_864_u64),
        ("64M", 67_108_864),
        ("1G", 1_073_741_824),
    ] {
        let args = [arg(&store), "--write", "--json", "--memory-limit", size];
        assert_eq!(limit(&args), json!(bytes), "--memory-limit {size}");
    }

    // A read shell answers the same statement without --write, with all five columns and with
    // the ceiling of its own session.
    let columns = [
        "limit",
        "used",
        "available",
        "startup_ceiling",
        "ceiling_refresh_failures",
    ];
    let run = shell(&dir, &[arg(&store), "--json"], "SHOW MEMORY_LIMIT;\n");
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(
        documents(&run.stdout)[0]["result"]["columns"],
        json!(columns)
    );
    let args = [arg(&store), "--json", "--memory-limit", "64M"];
    assert_eq!(
        limit(&args),
        json!(67_108_864_u64),
        "a read shell's --memory-limit"
    );

    // A size that cannot be read is a mistake in the command line: exit 2, nothing opened.
    let unread = dir.join("unread");
    let run = shell(
        &dir,
        &[arg(&unread), "--write", "--memory-limit", "12Q"],
        CREATE_NOTES,
    );
    assert_eq!(run.code, Some(2), "{}", run.stderr);
    assert!(!unread.exists());

    // 64 rows of one mebibyte each, written with no ceiling.
    let big = dir.join("big");
    let writer = Database::open(&big).unwrap();
    writer
        .execute(
            "CREATE TABLE blobs (id INTEGER PRIMARY KEY, body TEXT)",
            &params! {},
        )
        .unwrap();
    for id in 1..=64_i64 {
        let body = format!("{id:08}").repeat(131_072);
        writer
            .execute(
                "INSERT INTO blobs (id, body) VALUES ($id, $body)",
                &params! { "id" => id, "body" => body },
            )
            .unwrap();
    }
    writer.close().unwrap();

    // Under a 16 MiB ceiling, reading or writing, the statement that would bring all 64 MiB
    // into memory is refused by name with the numbers of the refusal, and gives no result; the
    // session goes on, and a statement that fits is answered.
    let statements = "SELECT id, body FROM blobs ORDER BY id;\nSELECT COUNT(*) AS n FROM blobs;\n";
    for mode in [&[][..], &["--write"][..]] {
        let mut args = vec![arg(&big), "--json", "--memory-limit", "16M"];
        args.extend_from_slice(mode);
        let run = shell(&dir, &args, statements);
        assert_eq!(run.code, Some(1), "{mode:?}: {}", run.stderr);
        let refusals: Vec<Json> = documents(&run.stderr)
            .into_iter()
            .filter(|document| document.get("error").is_some())
            .collect();
        assert_eq!(refusals.len(), 1, "{mode:?}: {}", run.stderr);
        let error = &refusals[0]["error"];
        assert_eq!(error["class"], "io", "{mode:?}: {error}");
        assert_eq!(
            error["detail"]["kind"], "memory_budget_exceeded",
            "{mode:?}: {error}"
        );
        for number in ["requested_bytes", "available_bytes", "budget_limit_bytes"] {
            assert!(
                error["detail"][number].is_u64(),
                "{mode:?}: no {number} in {error}"
            );
        }
        assert_eq!(
            documents(&run.stdout),
            [json!({"result": {"columns": ["n"], "rows": [{"n": 64}]}})],
            "{mode:?}"
        );
    }
}

#[test]
fn a_similarity_search_runs_through_the_program() {
    let dir = folder("a-similarity-search-runs");
    let store = dir.join("store");
    // From [1, 0, 0] row 1 is at distance 0, row 5 between 0 and 1, row 2 at 1 and row 3 at 2.
    // Row 4 has no vector.
    let declare = "\
CREATE TABLE t (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  v VECTOR(3) PARTITION_KEY (kind) SEARCH_MODE EXACT
);
INSERT INTO t (id, kind, v)
VALUES (1, 'a', [1.0, 0.0, 0.0]), (2, 'a', [0.0, 1.0, 0.0]), (3, 'b', [-1.0, 0.0, 0.0]),
       (4, 'b', NULL), (5, 'b', [0.1, 0.2, 0.3]);
";
    let search =
        "SELECT id, v <=> [1.0, 0.0, 0.0] AS distance FROM t ORDER BY distance LIMIT 10;\n";
    let rows = "SELECT id, v FROM t ORDER BY id;\n";

    // Declared, written and searched in one piped write session.
    let run = shell(
        &dir,
        &[arg(&store), "--write"],
        format!("{declare}{search}{rows}"),
    );
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.errors(), Vec::<&str>::new());
    let printed = tables(&run.stdout);
    assert_eq!(printed.len(), 2, "{}", run.stdout);

    // The rows that have a vector, nearest first, each with its distance under the name the
    // statement gave it.
    let found = nearest(&printed[0]);
    let ids: Vec<i64> = found.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, [1, 5, 2, 3], "{}", run.stdout);
    assert_eq!(
        (found[0].1, found[2].1, found[3].1),
        (0.0, 1.0, 2.0),
        "{}",
        run.stdout
    );
    assert!(0.0 < found[1].1 && found[1].1 < 1.0, "{}", run.stdout);

    // A vector prints as its numbers in brackets, and no vector as NULL.
    assert_eq!(printed[1][0], ["id", "v"], "{}", run.stdout);
    assert_eq!(printed[1][1], ["1", "[1.0, 0.0, 0.0]"], "{}", run.stdout);
    assert_eq!(printed[1][4], ["4", "NULL"], "{}", run.stdout);
    assert_eq!(printed[1][5], ["5", "[0.1, 0.2, 0.3]"], "{}", run.stdout);

    // For a program the distance is a number, a vector an array of numbers and no vector null.
    let run = shell(&dir, &[arg(&store), "--json"], format!("{search}{rows}"));
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    let results = documents(&run.stdout);
    assert_eq!(results.len(), 2, "{}", run.stdout);
    assert_eq!(results[0]["result"]["columns"], json!(["id", "distance"]));
    let found = results[0]["result"]["rows"].as_array().unwrap();
    let ids: Vec<i64> = found
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [1, 5, 2, 3], "{}", run.stdout);
    for row in found {
        assert!(row["distance"].is_number(), "{row}");
    }
    let distance = |row: usize| found[row]["distance"].as_f64().unwrap();
    assert_eq!(
        (distance(0), distance(2), distance(3)),
        (0.0, 1.0, 2.0),
        "{}",
        run.stdout
    );
    assert!(0.0 < distance(1) && distance(1) < 1.0, "{}", run.stdout);
    assert_eq!(results[1]["result"]["columns"], json!(["id", "v"]));
    let stored = results[1]["result"]["rows"].as_array().unwrap();
    assert_eq!(stored.len(), 5, "{}", run.stdout);
    assert_eq!(numbers(&stored[0]["v"]), [1.0, 0.0, 0.0]);
    assert!(stored[3]["v"].is_null(), "{}", stored[3]);
    assert_eq!(numbers(&stored[4]["v"]), [0.1, 0.2, 0.3]);

    // `.schema` prints the VECTOR column with its clauses in the order they are declared in,
    // and with the limit of 256 partitions that a key declared without one has.
    let schema = shell(&dir, &[arg(&store)], ".schema t\n");
    assert_eq!(schema.code, Some(0), "{}", schema.stderr);
    let words = schema
        .stdout
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        words.contains("v VECTOR(3) PARTITION_KEY (kind) MAX_PARTITIONS 256 SEARCH_MODE EXACT"),
        "{}",
        schema.stdout
    );

    // What it printed declares the same table again in another store.
    let copy = dir.join("copy");
    let made = shell(&dir, &[arg(&copy), "--write"], &schema.stdout);
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    assert_eq!(
        shell(&dir, &[arg(&copy)], ".schema t\n").stdout,
        schema.stdout
    );
}

#[test]
fn an_indexed_search_runs_through_the_program() {
    let dir = folder("an-indexed-search-runs");
    let store = dir.join("store");
    let made = shell(
        &dir,
        &[arg(&store), "--write"],
        "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR(3));\n\
         INSERT INTO passages (id, embedding) VALUES (1, [0.0, 1.0, 0.0]), (2, [1.0, 0.0, 0.0]);\n",
    );
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // The same search asked for INDEXED and then for EXACT: no maintenance cycle has grouped
    // these rows, so INDEXED searches every one of them and both give the same table.
    let search = |mode: &str| {
        format!(
            "SELECT id FROM passages ORDER BY embedding <=> [1.0, 0.0, 0.0] USE VECTOR {mode} \
             LIMIT 2;\n"
        )
    };
    let input = format!("{}{}", search("INDEXED"), search("EXACT"));
    let table = "+----+\n| id |\n+----+\n| 2  |\n| 1  |\n+----+\n(2 rows)\n";
    let document = json!({"result": {"columns": ["id"], "rows": [{"id": 2}, {"id": 1}]}});

    // Reading or writing, for a person and for a program.
    for mode in [&[][..], &["--write"][..]] {
        let mut args = vec![arg(&store)];
        args.extend_from_slice(mode);
        let run = shell(&dir, &args, &input);
        assert_eq!(run.code, Some(0), "{mode:?}: {}", run.stderr);
        assert_eq!(run.errors(), Vec::<&str>::new(), "{mode:?}");
        assert_eq!(run.stdout, format!("{table}{table}"), "{mode:?}");

        args.push("--json");
        let run = shell(&dir, &args, &input);
        assert_eq!(run.code, Some(0), "{mode:?}: {}", run.stderr);
        assert_eq!(
            documents(&run.stdout),
            [document.clone(), document.clone()],
            "{mode:?}"
        );
    }

    // Two vectors outside any grouping under AUTO_INDEX_AT 1: the index is behind its writes,
    // and INDEXED is refused by name rather than answered as an exact scan.
    let behind = dir.join("behind");
    let made = shell(
        &dir,
        &[arg(&behind), "--write"],
        "CREATE TABLE behind (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT 1);\n\
         INSERT INTO behind (id, v) VALUES (1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0]);\n",
    );
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    let run = shell(
        &dir,
        &[arg(&behind), "--json"],
        search("INDEXED")
            .replace("passages", "behind")
            .replace("embedding", "v"),
    );
    assert_eq!(run.code, Some(1), "{}", run.stderr);
    let refusals = documents(&run.stderr);
    assert_eq!(refusals.len(), 1, "{}", run.stderr);
    assert_eq!(
        refusals[0]["error"]["detail"]["kind"], "vector_indexed_route_unavailable",
        "{}",
        run.stderr
    );
}

/// What the two tests below search and inspect: a column with a partition for each `kind`, two
/// vectors in each, and no maintenance cycle run.
const KINDS: &str = "\
CREATE TABLE notes (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (kind));
INSERT INTO notes (id, kind, v)
VALUES (1, 'a', [1.0, 0.0, 0.0]), (2, 'a', [0.0, 1.0, 0.0]), (3, 'b', [-1.0, 0.0, 0.0]),
       (4, 'b', NULL), (5, 'b', [0.1, 0.2, 0.3]);
";

/// The fields of `SHOW VECTOR_INDEXES`, in the order they are printed.
const INDEX_FIELDS: [&str; 31] = [
    "table",
    "column",
    "dimension",
    "quantization",
    "vector_count",
    "bytes",
    "partition_key_columns",
    "max_partitions",
    "live_partitions",
    "search_mode",
    "declared_auto_index_at",
    "effective_auto_index_at",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "durable_index_bytes",
    "indexed_available",
    "maintenance_state",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "unavailable_partitions",
    "stalled_partitions",
    "broad_route",
    "recovery_action",
];

/// The fields of `SHOW VECTOR_PARTITIONS`, in the order they are printed.
const PARTITION_FIELDS: [&str; 18] = [
    "table",
    "column",
    "partition_key",
    "live_rows",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "durable_index_bytes",
    "indexed_available",
    "availability_reason",
    "maintenance_state",
    "maintenance_reason",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "lists",
    "probes",
    "recovery_action",
];

#[test]
fn vector_inspection_prints_one_field_to_a_line() {
    let dir = folder("vector-inspection-prints");
    let store = dir.join("store");
    let more = "CREATE TABLE other (id INTEGER PRIMARY KEY, w VECTOR(2));\n";
    let made = shell(&dir, &[arg(&store), "--write"], format!("{KINDS}{more}"));
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // Each statement with the fields it prints and, for each row, the values of three of them.
    let indexes = [["notes", "v", "3"], ["other", "w", "2"]];
    let partitions = [
        ["notes", "v", r#"{"kind":"a"}"#],
        ["notes", "v", r#"{"kind":"b"}"#],
        ["other", "w", "{}"],
    ];
    let cases = [
        ("SHOW VECTOR_INDEXES;\n", &INDEX_FIELDS[..], &indexes[..]),
        (
            "SHOW VECTOR_PARTITIONS;\n",
            &PARTITION_FIELDS[..],
            &partitions[..],
        ),
    ];
    for (statement, fields, rows) in cases {
        // For a person: rows with an empty line between them, each `name: value` lines in the
        // statement's order, and no line too long for a terminal.
        let run = shell(&dir, &[arg(&store)], statement);
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        assert!(
            run.stdout.lines().all(|line| line.chars().count() <= 120),
            "{}",
            run.stdout
        );
        let printed: Vec<Vec<(&str, &str)>> = run
            .stdout
            .split("\n\n")
            .map(|row| {
                row.lines()
                    .filter(|line| !line.starts_with('('))
                    .map(|line| {
                        line.split_once(": ")
                            .unwrap_or_else(|| panic!("not `name: value`: {line:?}"))
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|row| !row.is_empty())
            .collect();
        assert_eq!(printed.len(), rows.len(), "{}", run.stdout);
        for (row, first) in printed.iter().zip(rows) {
            let names: Vec<&str> = row.iter().map(|(name, _)| *name).collect();
            assert_eq!(names, fields, "{}", run.stdout);
            let values: Vec<&str> = row[..3].iter().map(|(_, value)| *value).collect();
            assert_eq!(values, first, "{}", run.stdout);
        }

        // For a program: the usual result document, its columns the same names.
        let run = shell(&dir, &[arg(&store), "--json"], statement);
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        let results = documents(&run.stdout);
        assert_eq!(results.len(), 1, "{}", run.stdout);
        assert_eq!(results[0]["result"]["columns"], json!(fields));
        let found = results[0]["result"]["rows"].as_array().unwrap();
        assert_eq!(found.len(), rows.len(), "{}", run.stdout);
        assert_eq!(found[0]["table"], "notes", "{}", run.stdout);
    }
}

#[test]
fn explain_prints_how_each_partition_is_searched() {
    let dir = folder("explain-prints");
    let store = dir.join("store");
    let made = shell(&dir, &[arg(&store), "--write"], KINDS);
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // Four vectors in two partitions, far below the 100,000 from which AUTO uses a grouping,
    // so the search asked for under AUTO is the exact one, and none grouped. A line ending in
    // `*` is checked by its name alone: with no grouping the partition has no count of groups
    // to print.
    let lines = [
        "requested_mode: AUTO",
        "resolved_mode: EXACT",
        "effective_auto_index_at: 100000",
        "aggregate_allowed_vectors: 4",
        r#"partition_key_columns: ["kind"]"#,
        "partitions: 2",
        r#"partition: {"kind":"a"}"#,
        "  route: exact",
        "  lists: *",
        "  probes: *",
        "  live_rows: 2",
        "  outside_grouping: 2",
        r#"partition: {"kind":"b"}"#,
        "  route: exact",
        "  lists: *",
        "  probes: *",
        "  live_rows: 2",
        "  outside_grouping: 2",
        "refusal: none",
    ];
    let same = |printed: &[&str]| {
        printed.len() == lines.len()
            && lines.iter().zip(printed).all(|(expected, line)| {
                expected
                    .strip_suffix('*')
                    .map_or(expected == line, |name| line.starts_with(name))
            })
    };
    let search = ".explain SELECT id FROM notes ORDER BY v <=> '[1,0,0]' LIMIT 3\n";

    // The lines and nothing else: the search is not run, so no table of rows is printed.
    let run = shell(&dir, &[arg(&store)], search);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert!(
        same(&run.stdout.lines().collect::<Vec<_>>()),
        "{}",
        run.stdout
    );

    // Any other statement is refused in one line, and the session goes on to the next.
    let other = ".explain SELECT id FROM notes ORDER BY id\n";
    let next = "SELECT id FROM notes WHERE id = 5;\n";
    let run = shell(&dir, &[arg(&store)], format!("{other}{next}"));
    assert_eq!(run.code, Some(1), "{}", run.stderr);
    let refusals: Vec<&str> = run.stderr.lines().collect();
    assert_eq!(refusals.len(), 1, "{}", run.stderr);
    assert!(
        refusals[0].starts_with("Error: ")
            && refusals[0].contains(
                "explaining this statement is not supported in this version of the library"
            ),
        "{}",
        run.stderr
    );
    assert_eq!(tables(&run.stdout), [[["id"], ["5"]]], "{}", run.stdout);

    // For a program: one document that holds the same lines.
    let run = shell(&dir, &[arg(&store), "--json"], search);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    let results = documents(&run.stdout);
    assert_eq!(results.len(), 1, "{}", run.stdout);
    let printed: Vec<&str> = results[0]["explain"]["lines"]
        .as_array()
        .unwrap_or_else(|| panic!("no explain.lines: {}", results[0]))
        .iter()
        .map(|line| line.as_str().unwrap())
        .collect();
    assert!(same(&printed), "{}", run.stdout);
}

#[test]
fn help_lists_explain() {
    let dir = folder("help-lists-explain");
    let store = dir.join("store");
    let made = shell(&dir, &[arg(&store), "--write"], CREATE_NOTES);
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    let run = shell(&dir, &[arg(&store)], ".help\n");
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert!(
        run.stdout.lines().any(|line| line.starts_with(".explain")),
        "{}",
        run.stdout
    );
}

#[test]
fn a_read_shell_searches_while_a_writer_holds_the_store() {
    let dir = folder("a-read-shell-searches-while-a-writer-holds");
    let store = dir.join("store");

    // This test is the writer, and holds the store until the end. From [1, 0, 0] row 1 is at
    // distance 1 and row 2 at 2.
    let writer = Database::open(&store).unwrap();
    for sql in [
        "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        "INSERT INTO passages (id, embedding) VALUES (1, [0.0, 1.0, 0.0]), (2, [-1.0, 0.0, 0.0])",
    ] {
        writer.execute(sql, &params! {}).unwrap();
    }
    let search = "SELECT id, embedding <=> [1.0, 0.0, 0.0] AS distance FROM passages \
                  ORDER BY distance LIMIT 2;\n";

    // The store really is held: a second writer is refused, naming this process.
    let second = shell(&dir, &[arg(&store), "--write"], search);
    assert_eq!(second.code, Some(1), "{}", second.stderr);
    assert!(
        second.stderr.contains(&std::process::id().to_string()),
        "{}",
        second.stderr
    );

    // A read shell needs nothing from the writer: it searches and gets the rows.
    let run = shell(&dir, &[arg(&store)], search);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    let printed = tables(&run.stdout);
    assert_eq!(printed.len(), 1, "{}", run.stdout);
    assert_eq!(nearest(&printed[0]), [(1, 1.0), (2, 2.0)], "{}", run.stdout);

    // The writer was not held up by it, and the next read shell searches its next commit, in
    // which row 3 is the nearest.
    writer
        .execute(
            "INSERT INTO passages (id, embedding) VALUES (3, [1.0, 0.0, 0.0])",
            &params! {},
        )
        .unwrap();
    let run = shell(&dir, &[arg(&store)], search);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    let printed = tables(&run.stdout);
    assert_eq!(printed.len(), 1, "{}", run.stdout);
    assert_eq!(nearest(&printed[0]), [(3, 0.0), (1, 1.0)], "{}", run.stdout);

    writer.close().unwrap();
}
