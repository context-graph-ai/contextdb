//! Runs the built `contextdb` program the way a script does: statements piped to its standard
//! input, and its output, its error stream and its exit status read back.
//!
//! Each test has its own folder under the build's temporary folder, left behind for inspection.

#[path = "../../contextdb/tests/support/opening.rs"]
pub mod opening;

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::os::unix::fs::PermissionsExt as Mode;
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
    let mut child = piped(dir, args);
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

/// Starts `contextdb` in `dir` with these arguments, its three streams piped to this test.
fn piped(dir: &Path, args: &[&str]) -> std::process::Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    let command = command.args(args).current_dir(dir).stdin(Stdio::piped());
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Sends `child` the signal Ctrl-C sends, through the shell's own kill, which every Unix has; a
/// slim container has no kill program. Whether the signal was sent.
fn interrupt(child: &std::process::Child) -> bool {
    let kill = format!("kill -INT {}", child.id());
    Command::new("sh")
        .args(["-c", &kill])
        .status()
        .unwrap()
        .success()
}

/// A path as a command-line argument.
fn arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// A read shell over `store`.
fn run_read(dir: &Path, store: &Path, input: impl AsRef<[u8]>) -> Run {
    shell(dir, &[arg(store)], input)
}

/// A read-write shell over `store`.
fn run_write(dir: &Path, store: &Path, input: impl AsRef<[u8]>) -> Run {
    shell(dir, &[arg(store), "--write"], input)
}

/// A read shell over `store` that prints JSON documents.
fn run_json(dir: &Path, store: &Path, input: impl AsRef<[u8]>) -> Run {
    shell(dir, &[arg(store), "--json"], input)
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

    let run = run_read(&dir, &store, SELECT_NOTES);
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
        let run = run_json(&dir, path, SELECT_NOTES);
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
    let key = r#"inspect key store --table notes --key-json {"column":"id","value":{"Int64":1}}"#;
    for (args, code) in [
        ("diagnose store", 1),
        ("snapshot export store copy", 1),
        (key, 1),
        ("purge store --table notes --force", 1),
        ("reset store --force", 1),
        ("purge store --table notes", 2),
        ("reset store", 2),
        ("store --write --read-result-rows 0", 2),
        ("store --write --read-result-bytes 0", 2),
        ("store --write --read-work 0", 2),
        ("store --write --read-active-ms 0", 2),
        ("store --write --read-memory 0", 2),
        ("store --write --read-cursor-page-rows 0", 2),
        ("store --write --read-cursor-page-bytes 0", 2),
        ("store --write --read-cursor-idle-ms 0", 2),
        ("store --write --read-cursor-lifetime-ms 0", 2),
        ("store --disk-limit 1G", 2),
        ("store --maintenance caller", 2),
        ("store --maintenance-poll-ms 1", 2),
    ] {
        let args: Vec<_> = args.split_whitespace().collect();
        assert_eq!(shell(&dir, &args, "").code, Some(code));
        assert_eq!(listing(&dir), ["broken"]);
    }
}

#[test]
fn write_creates_a_missing_store() {
    let dir = folder("write-creates-a-missing-store");
    let store = dir.join("store");

    let insert = "INSERT INTO notes (id, body) VALUES (1, 'first');\n";
    let run = run_write(&dir, &store, format!("{CREATE_NOTES}{insert}"));
    assert_eq!(run.stderr.matches("created a new store at").count(), 1);
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
    let run = run_read(&dir, &store, SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, ONE_NOTE);

    // On a store that is there, --write opens it and keeps what it holds.
    let insert = "INSERT INTO notes (id, body) VALUES (2, 'second');\n";
    let run = run_write(&dir, &store, format!("{insert}{SELECT_NOTES}"));
    assert!(!run.stderr.contains("created a new store at"));
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert!(run.stdout.contains(TWO_NOTES), "{}", run.stdout);
    let events = "CREATE EVENT TYPE changed WHEN UPDATE ON notes;\nCREATE SINK audit TYPE callback;\nCREATE ROUTE r EVENT changed TO audit;\nUPDATE notes SET body = 'changed' WHERE id = 2;\nUPDATE notes SET body = 'second' WHERE id = 2;\n";
    assert_eq!(run_write(&dir, &store, events).code, Some(0));
    let mut writer = Some(Database::open(&store).unwrap());
    let inspect = r#"inspect key store --table notes --key-json {"column":"id","value":{"Int64":1}} --column body --json"#;
    let read = "store --json";
    let write = "store --write --json --maintenance caller";
    let engine = "store --write --json --maintenance engine";
    let cursor = "store --json --read-cursor-page-rows 1";
    let fetch = ".cursor open SELECT id FROM notes ORDER BY id\n.cursor fetch 2\n.cursor close extra\n.cursor fetch 1\n.cursor close\n";
    let close = ".cursor open SELECT id FROM notes\n.cursor close\n";
    let text = "store --read-cursor-page-rows 1";
    let open = ".cursor open SELECT id, body FROM notes ORDER BY id\n";
    let pages = format!("{open}.cursor fetch\n.cursor fetch\n");
    let trace = ".trace on\nSELECT 1;\n.trace off\n";
    let cycle = ".maintenance run\n.maintenance status\n";
    let transaction = "BEGIN;\n.cursor open SELECT id FROM notes\nROLLBACK;\n";
    let callback = "\"callback_registered\":null";
    let count = "SELECT COUNT(*) AS n FROM notes;\n";
    let row_limit = "store --json --read-result-rows 1 --read-cursor-page-rows 1";
    let statement = "\"statement\":\"SELECT id, body FROM notes ORDER BY id\"";
    let remedy = "\"remedy_command\":\".cursor open SELECT id, body FROM notes ORDER BY id\"";
    let limited = "store --json --read-result-rows 2 --read-cursor-page-rows 1";
    let on = ".cursor open SELECT id FROM notes ORDER BY id\n.cursor fetch 3\n.cursor fetch 2\n";
    let memory = "store --json --memory-limit 64M --read-memory 16M";
    let alter = |c: &str| format!("ALTER TABLE d ALTER COLUMN v SET {c};\n");
    let make = format!(
        "CREATE TABLE d (id INTEGER PRIMARY KEY, v VECTOR(2));\n{}",
        alter("IVF (LISTS = 1)")
    );
    let probed =
        alter("PROBES 2") + "EXPLAIN SELECT id FROM d ORDER BY v <=> [1.0, 0.0] LIMIT 1;\n";
    let purged = "rows_affected=2";
    let bytes = "store --json --read-result-bytes 1 --read-cursor-page-bytes 1";
    let shown = "SHOW READ_CACHE_SHARE;";
    let [share0, share50] = [0, 50].map(|n| format!("store --json --read-cache-share {n}"));
    for (args, input, fragment, code) in [
        ("diagnose store", "", "notes", 0),
        (inspect, "", "\"body\":\"first\"", 0),
        ("snapshot export store copy", "", "snapshot", 0),
        ("copy --json", SELECT_NOTES, "\"body\":\"second\"", 0),
        (read, ".events status\n", callback, 0),
        (read, ".events status\n", "\"queued\":2", 0),
        (read, ".verify\n", "notes", 0),
        (read, ".help vector\n.help propagate\n", "VECTOR(<n>)", 0),
        (read, "SELECT 1\n.tables\n;\n", "error", 1),
        (read, "\\trace on\n\\sync status\n", "error", 1),
        (read, "\\? \n\\dt\n\\d notes\n\\q\n", "notes", 0),
        (write, "\\? \n\\dt\n\\d notes\n\\q\n", "notes", 0),
        (read, "SELEC 1;\n", "error", 1),
        (read, ".nope\n.tables;\n", "error", 1),
        (
            read,
            ".nope\n",
            "\"class\":\"usage\",\"detail\":{\"kind\":\"unknown_command\"",
            1,
        ),
        (read, "INSERT INTO notes (id) VALUES (9);\n", "error", 1),
        ("missing --json", "", "error", 1),
        ("store --nope", "", "--nope", 2),
        (read, trace, "rows_examined", 0),
        ("store", trace, "Trace is on.", 0),
        ("store", trace, "Trace is off.", 0),
        (cursor, fetch, "\"id\":2", 1),
        (cursor, close, "\"closed\":true", 0),
        (text, pages.as_str(), "has_more: true", 0),
        (text, pages.as_str(), "has_more: false", 0),
        (text, open, "first", 0),
        (row_limit, SELECT_NOTES, "--read-result-rows", 1),
        (row_limit, SELECT_NOTES, "\"class\":\"io\"", 1),
        (row_limit, SELECT_NOTES, "\"line\":1", 1),
        (
            row_limit,
            ".cursor fetch\n",
            "\"class\":\"io\",\"detail\":{\"kind\":\"cursor_not_found\"",
            1,
        ),
        (
            row_limit,
            SELECT_NOTES,
            "\"kind\":\"read_limit_exceeded\"",
            1,
        ),
        (row_limit, SELECT_NOTES, statement, 1),
        (row_limit, SELECT_NOTES, remedy, 1),
        (limited, on, "\"remedy_command\":\".cursor fetch 2\"", 1),
        (limited, on, "\"has_more\":false", 1),
        ("store --json --read-work 1", SELECT_NOTES, "--read-work", 1),
        (bytes, ".tables\n", "\"required_bytes\":", 1),
        (bytes, ".events status\n", "\"required_bytes\":", 1),
        (memory, "SHOW MEMORY_LIMIT;\n", "16777216", 0),
        (&share0, shown, "\"effective_bytes\":0", 0),
        (&share50, shown, "\"effective_percent\":50", 0),
        ("store --read-cache-share 101", "", "100", 2),
        ("store", "SET READ_CACHE_SHARE 10;", "READ_CACHE_SHARE", 1),
        (write, ".maintenance status\n", "caller", 0),
        (write, cycle, "work_remaining", 0),
        (write, cycle, "versions_removed", 0),
        (write, make.as_str(), "write PROBES n", 1),
        (
            write,
            alter("SEARCH (TARGET_ACCURACY = 95)").as_str(),
            "write PROBES n",
            1,
        ),
        (
            write,
            alter("PROBES 0").as_str(),
            "PROBES takes a whole number of 1 or more, or DEFAULT",
            1,
        ),
        (write, probed.as_str(), "(declared)", 0),
        (write, ".maintenance compact\n", "compaction_free_ratio", 0),
        (
            write,
            ".maintenance compact 1\n",
            "compaction_free_ratio",
            0,
        ),
        (write, ".maintenance run\n", "\"page_slack_ratio\":null", 0),
        (write, ".maintenance run\n", "versions_removed", 0),
        (
            "store --write --maintenance caller",
            ".maintenance run\n",
            "unknown",
            0,
        ),
        (
            write,
            "SET STORAGE_COMPACTION MAX_SWEEPS 1;\n",
            "MAX_SWEEPS",
            1,
        ),
        (engine, ".maintenance status\n", "engine", 0),
        (write, transaction, "transaction", 1),
        ("purge store --table notes --force", "", purged, 0),
        (read, count, "\"n\":0", 0),
        ("reset store --force", "", "reset", 0),
        ("store", ".tables\n", "", 0),
    ] {
        if args == write
            && let Some(writer) = writer.take()
        {
            writer.close().unwrap();
        }
        let args: Vec<_> = args.split_whitespace().collect();
        let run = shell(&dir, &args, input);
        assert_eq!(run.code, Some(code), "{}", run.stderr);
        // A refusal is a sentence for a person, never a debug dump.
        for dump in [
            "Some(",
            "ParseError",
            "ReadOnlySession",
            "StoreMissing",
            "::",
        ] {
            assert!(!run.stderr.contains(dump), "{dump}: {}", run.stderr);
        }
        let printed = format!("{}{}", run.stdout, run.stderr);
        assert!(printed.contains(fragment), "{printed}");
        assert!(!fragment.is_empty() || run.stdout.is_empty());
    }
    let named = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .env("CONTEXTDB_DB_PATH", "store")
        .current_dir(&dir)
        .output()
        .unwrap();
    assert_eq!(named.status.code(), Some(0), "the variable names the store");
}

#[test]
fn purge_table_names_one_table_and_nothing_else() {
    let dir = folder("purge-table-names-one-table");
    let store = dir.join("store");
    let setup = "CREATE TABLE other (id INTEGER PRIMARY KEY);\nINSERT INTO other (id) VALUES (1);\nINSERT INTO notes (id, body) VALUES (1, 'a'), (2, 'b');\n";
    run_write(&dir, &store, format!("{CREATE_NOTES}{setup}"));
    // --table is one table's name: a list or a WHERE is refused and purges nothing.
    for table in ["notes, other", "notes WHERE id = 1"] {
        let run = shell(
            &dir,
            &["purge", arg(&store), "--table", table, "--force"],
            "",
        );
        assert_ne!(
            run.code,
            Some(0),
            "--table {table:?} is not one table name: {}",
            run.stdout
        );
        let count =
            "SELECT (SELECT COUNT(*) FROM notes) AS n, (SELECT COUNT(*) FROM other) AS o;\n";
        let run = run_read(&dir, &store, count);
        assert!(
            run.stdout.contains('2') && run.stdout.contains('1'),
            "a refused purge removes no row: {}",
            run.stdout
        );
    }
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
    let run = run_write(&dir, &store, input);

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
    let args = [arg(&store), "--write", "--stop-at-first-refusal"];
    let stopped = shell(&dir, &args, input);
    assert_eq!(stopped.code, Some(1));
    assert!(stopped.stdout.is_empty());
    assert!(stopped.stderr.contains("line 1"));

    // A run in which every statement succeeded exits 0 and prints its results and nothing else.
    let run = run_read(&dir, &store, SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, TWO_NOTES);
    // At the end of piped input a last statement with no `;` is run, as psql runs it.
    let run = run_read(&dir, &store, SELECT_NOTES.trim_end().trim_end_matches(';'));
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
        let run = run_write(&dir, &store, input);
        assert_eq!(
            run.code,
            Some(1),
            "an undecodable line must fail the run: {}",
            run.stderr
        );
        assert!(
            run.errors()
                .iter()
                .any(|error| error.contains(&format!("line {line} of the input is not text"))),
            "the refusal must name the line that cannot be decoded, line {line}: {}",
            run.stderr
        );
        let run = run_read(&dir, &store, "SELECT COUNT(*) AS n FROM notes;\n");
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
CREATE TABLE jobs (id INTEGER PRIMARY KEY, st TEXT NOT NULL) STATE MACHINE (st: active -> [done]);
INSERT INTO jobs (id, st) VALUES (1, 'active');
UPDATE jobs SET st = 'draft' WHERE id = 1;
";
    let run = shell(&dir, &[arg(&store), "--write", "--json"], input);

    // One document per statement that gave a result, in the order they were given. `columns`
    // is the order the statement or the table declared, which the keys of a row cannot carry.
    let results = documents(&run.stdout);
    assert_eq!(results.len(), 7, "{}", run.stdout);
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
    assert_eq!(refusals.len(), 2, "{}", run.stderr);
    assert_eq!(refusals[0]["error"]["class"], "sql");
    assert_eq!(refusals[1]["error"]["class"], "sql");
    let said = refusals[1]["error"]["message"].as_str().unwrap();
    assert!(said.ends_with(": active -> draft"), "{said}");
    assert!(refusals[0]["error"]["message"].is_string());
    assert_eq!(run.code, Some(1));
}

#[test]
fn a_read_shell_refuses_a_write_by_name() {
    let dir = folder("a-read-shell-refuses-a-write");
    let store = dir.join("store");
    let insert = "INSERT INTO notes (id, body) VALUES (1, 'first');\n";
    let made = run_write(&dir, &store, format!("{CREATE_NOTES}{insert}"));
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
    let run = run_read(&dir, &store, format!("{writes}{SELECT_NOTES}"));
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
    let run = run_json(&dir, &store, writes);
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
    let run = run_json(&dir, &store, format!("{SELECT_NOTES}.tables\n"));
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

    // The same for a similarity search: from [1, 0, 0] row 1 is at distance 1 and row 2 at 2.
    for sql in [
        "CREATE TABLE passages (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        "INSERT INTO passages (id, embedding) VALUES (1, [0.0, 1.0, 0.0]), (2, [-1.0, 0.0, 0.0])",
    ] {
        writer.execute(sql, &params! {}).unwrap();
    }
    let search = "SELECT id, embedding <=> [1.0, 0.0, 0.0] AS distance FROM passages \
                  ORDER BY distance LIMIT 2;\n";

    // A read shell needs nothing from the writer and answers.
    let run = run_read(&dir, &store, SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, ONE_NOTE);
    let run = run_read(&dir, &store, search);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(nearest(&tables(&run.stdout)[0]), [(1, 1.0), (2, 2.0)]);

    // The writer was not held up by it, and the next read shell reads its next commit, in which
    // row 3 is the nearest.
    writer
        .execute(
            "INSERT INTO passages (id, embedding) VALUES (3, [1.0, 0.0, 0.0])",
            &params! {},
        )
        .unwrap();
    writer
        .execute(
            "INSERT INTO notes (id, body) VALUES (2, 'second')",
            &params! {},
        )
        .unwrap();
    let run = run_read(&dir, &store, SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, TWO_NOTES);
    let run = run_read(&dir, &store, search);
    assert_eq!(
        nearest(&tables(&run.stdout)[0]),
        [(3, 0.0), (1, 1.0)],
        "{}",
        run.stdout
    );

    // A write shell started beside this writer says what it waits for, then waits: when this
    // writer lets go, its write goes in and it exits cleanly.
    let mut waiting = piped(&dir, &[arg(&store), "--write", "--json"]);
    let insert = b"INSERT INTO notes (id, body) VALUES (3, 'third');\n";
    waiting.stdin.take().unwrap().write_all(insert).unwrap();
    let mut stderr = BufReader::new(waiting.stderr.take().unwrap());
    let mut said = String::new();
    stderr.read_line(&mut said).unwrap();
    let notice: Json = serde_json::from_str(&said).unwrap_or_default();
    let waits = format!("waiting for the writer that holds {}", store.display());
    assert_eq!(notice["notice"]["class"], "io", "a notice has a class");
    assert_eq!(
        notice["notice"]["message"], waits,
        "under --json the waiting line is a notice document"
    );
    writer.close().unwrap();
    let output = waiting.wait_with_output().unwrap();
    stderr.read_to_string(&mut said).unwrap();
    assert!(output.status.success(), "{said}");
    let run = run_json(&dir, &store, "SELECT id FROM notes ORDER BY id;\n");
    assert_eq!(
        documents(&run.stdout)[0]["result"]["rows"],
        json!([{"id": 1}, {"id": 2}, {"id": 3}]),
        "the waiting shell's write went in once the writer let go"
    );
    let mut reader = piped(&dir, &[arg(&store), "--json"]);
    let mut input = reader.stdin.take().unwrap();
    let mut output = BufReader::new(reader.stdout.take().unwrap());
    let mut owner = Some(Database::open(&store).unwrap());
    for online in [true, false, true] {
        if !online {
            owner.take().unwrap().close().unwrap();
        }
        if online && owner.is_none() {
            owner = Some(Database::open(&store).unwrap());
        }
        input
            .write_all(b"SELECT COUNT(*) AS n FROM notes;\n")
            .unwrap();
        let mut line = String::new();
        assert!(output.read_line(&mut line).unwrap() > 0);
        assert_eq!(documents(&line)[0]["result"]["rows"], json!([{"n":3}]));
    }
    assert!(interrupt(&reader), "the signal is sent");
    let done = reader.wait_with_output().unwrap();
    assert_eq!(done.status.code(), Some(130));
    let notices = String::from_utf8(done.stderr).unwrap();
    assert_eq!(
        notices
            .lines()
            .filter(|line| line.contains("writer"))
            .count(),
        2
    );
    assert!(notices.contains("commit"));
    let run = run_json(&dir, &store, "SELECT COUNT(*) AS n FROM notes;\n");
    let results = documents(&run.stdout);
    assert_eq!(results[0]["result"]["rows"], json!([{"n":3}]));
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
WITH n AS (SELECT id FROM notes) SELECT id FROM n ORDER BY id;
ROLLBACK;
SELECT id, body FROM notes ORDER BY id;
";
    let run = run_write(&dir, &store, input);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.errors(), Vec::<&str>::new());

    // Inside the second transaction its own changes are read, by a WITH as by a SELECT (M6);
    // after ROLLBACK they are gone and what the first one committed is there.
    let inside = "+----+\n| id |\n+----+\n| 2  |\n| 3  |\n+----+\n(2 rows)\n";
    assert_eq!(run.stdout.matches(inside).count(), 2, "{}", run.stdout);
    let seen_inside = run
        .stdout
        .rfind(inside)
        .expect("a transaction reads its own changes");
    let seen_after = run
        .stdout
        .find(TWO_NOTES)
        .expect("ROLLBACK left what COMMIT stored");
    assert!(seen_inside < seen_after);

    // The committed transaction reached the store, and the rolled-back one never did.
    let run = run_read(&dir, &store, SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.stdout, TWO_NOTES);

    // As in Postgres, BEGIN inside a transaction and COMMIT or ROLLBACK outside one each warn on
    // the error stream and the run still ends well, so a script that lost its BEGIN is told.
    let run = run_write(
        &dir,
        &store,
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
    let made = run_write(&dir, &store, format!("{notes}{authors}"));
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // `.tables` lists the names in order, one a line.
    let tables = run_read(&dir, &store, ".tables\n");
    assert_eq!(tables.code, Some(0), "{}", tables.stderr);
    let names: Vec<&str> = tables
        .stdout
        .lines()
        .filter(|line| ["authors", "notes"].contains(line))
        .collect();
    assert_eq!(names, ["authors", "notes"], "{}", tables.stdout);

    let schema = run_read(&dir, &store, ".schema notes\n");
    assert_eq!(schema.code, Some(0), "{}", schema.stderr);
    assert_eq!(schema.stdout, notes);

    // Each command has a short spelling that prints the same.
    assert_eq!(run_read(&dir, &store, "\\dt\n").stdout, tables.stdout);
    assert_eq!(run_read(&dir, &store, "\\d notes\n").stdout, schema.stdout);

    // What `.schema` printed declares the same table again in another store.
    let copy = dir.join("copy");
    let made = run_write(&dir, &copy, &schema.stdout);
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    assert_eq!(run_read(&dir, &copy, ".schema notes\n").stdout, notes);

    // A command used wrongly prints how it is used, never that it is unknown.
    for (input, command) in [(".schema\n", ".schema"), (".tables extra\n", ".tables")] {
        let run = run_read(&dir, &store, input);
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
    // A program reads the declared contract as fields, beside the ddl.
    let json = documents(&run_json(&dir, &store, ".schema notes\n").stdout);
    let (schema, columns) = (&json[0]["schema"], &json[0]["schema"]["columns"]);
    let all = "table columns primary_key indexes immutable dag_edge_types propagate ddl";
    let no = "state_machine retain history sync_direction conflict_policy";
    let cols = "name type nullable primary_key unique default immutable expires";
    let has = |v: &Json, ks: &str| ks.split(' ').filter(|k| v.get(k).is_some()).count();
    let seen = (has(schema, all), has(schema, no), has(&columns[0], cols));
    assert_eq!(seen, (8, 0, 8), "{schema}");
    assert_eq!(columns[0]["primary_key"], true, "{schema}");
    assert_eq!(columns[1]["nullable"], false, "{schema}");
    assert_eq!(columns[4]["unique"], true, "{schema}");
    // A table that follows its parent's PURGE prints it back.
    let ddl = "CREATE TABLE kids (id INTEGER PRIMARY KEY, n INTEGER REFERENCES notes(id) FOLLOWS PARENT PURGE);\n";
    run_write(&dir, &store, ddl);
    let kids = run_read(&dir, &store, ".schema kids\n").stdout;
    assert!(kids.contains("FOLLOWS PARENT PURGE"), "{kids}");
    let s = arg(&store);
    let at = |n: u64, flags: &str, input: &str| {
        let args = format!("{s} --read-result-bytes {n} --read-cursor-page-bytes 1 {flags}");
        shell(&dir, &args.split_whitespace().collect::<Vec<_>>(), input)
    };
    for (flags, input) in [
        ("", ".schema notes\n"),
        ("--json", ".schema notes\n"),
        ("--json --write", ".maintenance status\n"),
        ("", ".tables\n"),
    ] {
        let whole = at(u64::MAX, flags, input).stdout;
        let n = whole.len() as u64;
        assert_eq!(at(n, flags, input).stdout, whole, "{input}: whole");
        let short = at(n - 1, flags, input);
        let io = flags.is_empty() || short.stderr.contains("\"class\":\"io\"");
        let named = short.stderr.contains(&format!("result_bytes >= {n}"));
        assert!(io && named && short.stdout.is_empty(), "{}", short.stderr);
        if flags == "--json" {
            let setting = format!("effective result_bytes >= {n}");
            let detail = json!({"kind": "read_limit_exceeded", "limit": "result_bytes",
                "value": n - 1, "required_bytes": n, "required_setting": setting});
            let error = documents(&short.stderr).remove(0);
            assert_eq!(error["error"]["detail"], detail, "cli.md:281-284");
        }
    }
    let tail = "SELEC 1;\n.tables\nINSERT INTO notes (id, title) VALUES (9, 'x');\n";
    let run = at(1, "--write", tail);
    let error = run_write(&dir, &store, "SELEC 1;\n").stderr;
    let ack = run.stdout == "ok (rows_affected=1)\n";
    assert!(ack && run.stderr.starts_with(&error), "{}", run.stderr);
}

#[test]
fn the_memory_limit_option_is_the_sessions_ceiling() {
    let dir = folder("the-memory-limit-option");
    let store = dir.join("store");

    // The option is the ceiling the session reports, in bytes, with K, M and G as powers of
    // 1024; without it a writer is under the default, a quarter of the machine's memory.
    let limit = |args: &[&str]| {
        let run = shell(&dir, args, "SHOW MEMORY_LIMIT;\n");
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        documents(&run.stdout)[0]["result"]["rows"][0]["limit"].clone()
    };
    let default = limit(&[arg(&store), "--write", "--json"]);
    assert!(
        default.is_u64(),
        "a writer with no limit declared shows the default, not {default}"
    );
    // A writer with no limit counts the opening charge. A read shell with no limit counts the
    // reader's floor.
    for (args, writer) in [
        (&[arg(&store), "--write", "--json"][..], true),
        (&[arg(&store), "--json"], false),
    ] {
        let run = shell(&dir, args, "SHOW MEMORY_LIMIT;\n");
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        let expected = if writer {
            opening::opening_charge(&store, None)
        } else {
            11_018_240
        };
        assert_eq!(
            documents(&run.stdout)[0]["result"]["rows"][0]["used"],
            json!(expected),
            "used with no limit"
        );
    }
    for (size, bytes) in [
        ("65536K", 67_108_864_u64),
        ("64M", 67_108_864),
        ("1G", 1_073_741_824),
        ("2G", 2_147_483_648),
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
    let run = run_json(&dir, &store, "SHOW MEMORY_LIMIT;\n");
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
        args.extend(["--read-result-bytes", "1073741824"]);
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
        let said = error["message"].as_str().unwrap();
        assert!(
            said.ends_with("; raise it with --memory-limit"),
            "{mode:?}: {said}"
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
    let run = shell(&dir, &[arg(&big), "--json", "--write"], statements);
    assert!(
        run.code == Some(1)
            && run.stderr.contains("memory_budget_exceeded")
            && run.stderr.contains("16777216"),
        "a write shell has the default limits: stdout {} stderr {}",
        run.stdout,
        run.stderr
    );
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
    let run = run_write(&dir, &store, format!("{declare}{search}{rows}"));
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
    let run = run_json(&dir, &store, format!("{search}{rows}"));
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
    let schema = run_read(&dir, &store, ".schema t\n");
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
    let made = run_write(&dir, &copy, &schema.stdout);
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    assert_eq!(run_read(&dir, &copy, ".schema t\n").stdout, schema.stdout);
}

#[test]
fn an_indexed_search_runs_through_the_program() {
    let dir = folder("an-indexed-search-runs");
    let store = dir.join("store");
    let made = run_write(
        &dir,
        &store,
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

    // Two vectors outside any IVF under INDEXING_THRESHOLD 1: the index is behind its writes,
    // and INDEXED is refused by name rather than answered as an exact scan.
    let behind = dir.join("behind");
    let made = run_write(
        &dir,
        &behind,
        "CREATE TABLE behind (id INTEGER PRIMARY KEY, v VECTOR(3) INDEXING_THRESHOLD 1);\n\
         INSERT INTO behind (id, v) VALUES (1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0]);\n",
    );
    assert_eq!(made.code, Some(0), "{}", made.stderr);
    let run = run_json(
        &dir,
        &behind,
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
const INDEX_FIELDS: &str = "\
table column dimension quantization vector_count bytes partition_key_columns \
max_partitions live_partitions search_mode declared_indexing_threshold \
effective_indexing_threshold declared_probes declared_compaction_mode \
declared_compaction_change_percent declared_compaction_tombstone_percent \
effective_compaction_mode effective_compaction_change_percent \
effective_compaction_tombstone_percent pending_tuples tombstones \
durable_vector_bytes durable_index_bytes indexed_available maintenance_state \
maintenance_vectors_total maintenance_vectors_done maintenance_vectors_remaining \
unavailable_partitions stalled_partitions broad_route recovery_action";

/// The fields of `SHOW VECTOR_PARTITIONS`, in the order they are printed.
const PARTITION_FIELDS: &str = "\
table column partition_key live_rows pending_tuples tombstones durable_vector_bytes \
durable_index_bytes indexed_available availability_reason maintenance_state \
maintenance_reason maintenance_vectors_total maintenance_vectors_done \
maintenance_vectors_remaining lists probes recovery_action";

#[test]
fn vector_inspection_prints_one_field_to_a_line() {
    let dir = folder("vector-inspection-prints");
    let store = dir.join("store");
    let more = "CREATE TABLE other (id INTEGER PRIMARY KEY, w VECTOR(2));\n";
    let made = run_write(&dir, &store, format!("{KINDS}{more}"));
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // Each statement with the fields it prints and, for each row, the values of three of them.
    let indexes = [["notes", "v", "3"], ["other", "w", "2"]];
    let partitions = [
        ["notes", "v", r#"{"kind":"a"}"#],
        ["notes", "v", r#"{"kind":"b"}"#],
        ["other", "w", "{}"],
    ];
    let cases = [
        ("SHOW VECTOR_INDEXES;\n", INDEX_FIELDS, &indexes[..]),
        (
            "SHOW VECTOR_PARTITIONS;\n",
            PARTITION_FIELDS,
            &partitions[..],
        ),
    ];
    for (statement, fields, rows) in cases {
        let fields: Vec<&str> = fields.split(' ').collect();
        // For a person: rows with an empty line between them, each `name: value` lines in the
        // statement's order, and no line too long for a terminal.
        let run = run_read(&dir, &store, statement);
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
        let run = run_json(&dir, &store, statement);
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
    let made = run_write(&dir, &store, KINDS);
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // Four vectors in two partitions, far below the 100,000 from which AUTO uses an IVF,
    // so the search asked for under AUTO is the exact one, and none grouped. A line ending in
    // `*` is checked by its name alone: with no IVF the partition has no count of groups
    // to print.
    let lines = [
        "requested_mode: AUTO",
        "resolved_mode: EXACT",
        "effective_indexing_threshold: 100000",
        "aggregate_allowed_vectors: 4",
        r#"partition_key_columns: ["kind"]"#,
        "partitions: 2",
        r#"partition: {"kind":"a"}"#,
        "  route: exact",
        "  lists: *",
        "  probes: *",
        "  live_rows: 2",
        "  unindexed_rows: 2",
        r#"partition: {"kind":"b"}"#,
        "  route: exact",
        "  lists: *",
        "  probes: *",
        "  live_rows: 2",
        "  unindexed_rows: 2",
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
    let run = run_read(&dir, &store, search);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert!(
        same(&run.stdout.lines().collect::<Vec<_>>()),
        "{}",
        run.stdout
    );

    // A statement that reads no stored table is refused in one line; the session goes on.
    let other = ".explain SELECT 1\n";
    let next = "SELECT id FROM notes WHERE id = 5;\n";
    let run = run_read(&dir, &store, format!("{other}{next}"));
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
    let run = run_json(&dir, &store, search);
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
fn explain_prints_the_path_of_a_statement_and_runs_nothing() {
    let dir = folder("explain-prints-the-path");
    let store = dir.join("store");
    let declare = "\
CREATE TABLE notes (id INTEGER PRIMARY KEY, tag TEXT);
CREATE INDEX notes_tag ON notes (tag);
INSERT INTO notes (id, tag) VALUES (1, 'a'), (2, 'b');
";
    let made = run_write(&dir, &store, declare);
    assert_eq!(made.code, Some(0), "{}", made.stderr);

    // The path of a read, in the words the library documents.
    let read = run_read(
        &dir,
        &store,
        ".explain SELECT id FROM notes WHERE tag = 'a'\n",
    );
    assert_eq!(read.code, Some(0), "{}", read.stderr);
    assert_eq!(read.errors(), Vec::<&str>::new());
    for words in ["IndexScan { index: notes_tag }", "predicates_pushed: [tag]"] {
        assert!(
            read.stdout.contains(words),
            "no `{words}` in:\n{}",
            read.stdout
        );
    }

    // The path of a write is printed and the write is not made, in a session that may write.
    let asked = run_write(
        &dir,
        &store,
        ".explain DELETE FROM notes WHERE id = 1\nSELECT COUNT(*) AS n FROM notes;\n",
    );
    assert_eq!(asked.code, Some(0), "{}", asked.stderr);
    assert_eq!(asked.errors(), Vec::<&str>::new());
    assert!(
        asked.stdout.contains("IndexScan { index: __pk_id }"),
        "{}",
        asked.stdout
    );
    let counted = tables(&asked.stdout);
    let count = counted.last().expect("the count is printed");
    assert_eq!(count.last().unwrap(), &["2"], "{}", asked.stdout);

    // The command is listed in the help.
    let help = run_read(&dir, &store, ".help\n");
    assert!(
        help.stdout.lines().any(|line| line.starts_with(".explain")),
        "{}",
        help.stdout
    );
}

#[test]
fn a_cell_70000_characters_wide_is_printed_without_a_panic() {
    let dir = folder("a-cell-70000-characters-wide");
    let store = dir.join("store");

    // This test is the writer, and holds the store until the end; the shell reads beside it.
    let writer = Database::open(&store).unwrap();
    writer
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &params! {},
        )
        .unwrap();
    writer
        .execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &params! { "id" => 4_242_421_i64, "body" => "x".repeat(70_000) },
        )
        .unwrap();

    // `shell` itself fails the test on a panic; a panic also ends the program with 101.
    let run = run_read(&dir, &store, SELECT_NOTES);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.errors(), Vec::<&str>::new());
    // The row is printed: its other cell is there, however the wide one is shown.
    assert!(
        run.stdout.contains("4242421"),
        "the row's id is not in the {} bytes printed",
        run.stdout.len()
    );
    assert!(
        run.stdout.contains("(1 row)"),
        "no count of rows is printed"
    );
}

/// A closed store of `rows` notes at `dir/name`.
fn notes_store(dir: &Path, name: &str, rows: u32) -> PathBuf {
    let store = dir.join(name);
    let mut sql = format!("{CREATE_NOTES}BEGIN;\n");
    for id in 1..=rows {
        sql += &format!("INSERT INTO notes (id, body) VALUES ({id}, 'row {id}');\n");
    }
    assert_eq!(run_write(dir, &store, sql + "COMMIT;\n").code, Some(0));
    store
}

/// Every file of a store folder with its bytes.
fn files(store: &Path) -> Vec<(String, Vec<u8>)> {
    let read = |name: String| (std::fs::read(store.join(&name)).unwrap_or_default(), name);
    listing(store)
        .into_iter()
        .map(read)
        .map(|(b, n)| (n, b))
        .collect()
}

#[test]
fn diagnose_changes_nothing_and_a_reset_is_advised_only_for_a_damaged_store() {
    let dir = folder("diagnose-and-damage");
    let store = notes_store(&dir, "store", 300);
    let kept = files(&store);
    let key = r#"{"column":"id","value":{"Int64":1}}"#;
    let healthy = [
        vec!["diagnose", "store"],
        vec![
            "inspect",
            "key",
            "store",
            "--table",
            "nope",
            "--key-json",
            key,
        ],
        vec!["store"],
    ];
    for args in healthy {
        let run = shell(&dir, &args, "SELECT * FROM nope;\n");
        let said = run.stdout + &run.stderr;
        assert!(
            !said.contains("reset"),
            "{args:?} told a healthy store to reset: {said}"
        );
    }
    assert!(files(&store) == kept, "diagnose and reads change no byte");

    // One byte of one stored row is damaged: the problem is named, with the way on.
    let mut data = std::fs::read(store.join("data")).unwrap();
    let row = data.windows(7).position(|w| w == b"row 150").unwrap();
    data[row] ^= 0xFF;
    std::fs::write(store.join("data"), &data).unwrap();
    let kept = files(&store);
    for args in [vec!["diagnose", arg(&store)], vec![arg(&store)]] {
        let run = shell(&dir, &args, SELECT_NOTES);
        let said = run.stderr;
        assert_eq!(run.code, Some(1), "{args:?}: {said}");
        assert!(
            said.contains("corrupt") && !said.contains("reads cleanly"),
            "{said}"
        );
        let diagnose = said.find("contextdb diagnose");
        assert!(
            diagnose.is_some() && diagnose < said.find("contextdb reset"),
            "damage names diagnose, then reset: {said}"
        );
    }
    assert!(
        files(&store) == kept,
        "a read of a damaged store changes no byte"
    );
}

#[test]
fn every_command_help_lists_its_arguments_and_matches_the_usage_a_wrong_call_prints() {
    let dir = folder("help-matches-usage");
    let (path, force) = ("<PATH>", "--force");
    for (command, listed) in [
        (vec!["purge"], vec![path, "--table", force]),
        (vec!["reset"], vec![path, force]),
        (vec!["diagnose"], vec![path]),
        (vec!["snapshot", "export"], vec![path, "<ARTIFACT>"]),
        (
            vec!["inspect", "key"],
            vec![path, "--table", "--key-json", "--column"],
        ),
        (vec!["inspect", "blob"], vec![path, "--hash"]),
        (vec!["inspect", "sync-apply-state"], vec![path]),
    ] {
        let help = shell(&dir, &[&command[..], &["--help"]].concat(), "").stdout;
        assert!(
            listed
                .iter()
                .all(|w| help.lines().any(|l| l.trim().starts_with(w))),
            "{command:?} describes {listed:?} (a usage line is not that):\n{help}"
        );
        let usage = help.lines().find(|l| l.starts_with("Usage:")).unwrap();
        let usage = usage.replace(" [OPTIONS]", "");
        let wrong = shell(&dir, &command, "");
        assert!(
            wrong.code == Some(2) && wrong.stderr.contains(&usage),
            "{usage}:\n{}",
            wrong.stderr
        );
    }
    let top = shell(&dir, &["--help"], "").stdout;
    for word in [
        "purge",
        "snapshot",
        "inspect",
        "diagnose",
        "reset",
        "--write",
        "--read-work",
    ] {
        assert!(
            top.contains(word),
            "the program's --help lists {word}:\n{top}"
        );
    }
}

#[test]
fn compact_runs_to_the_end_and_stops_for_a_reader() {
    let dir = folder("compact-to-the-end");
    let store = dir.join("store");
    let row = |i| format!("INSERT INTO notes VALUES ({i}, '{}');\n", "x".repeat(6000));
    let sql = (1..=1000).map(row).collect::<String>();
    let free = format!("{CREATE_NOTES}BEGIN;\n{sql}COMMIT;\nDELETE FROM notes WHERE id < 501;\n");
    run_write(&dir, &store, free);
    let cycle = |input: &str| {
        let run = shell(&dir, &[arg(&store), "--write", "--json"], input);
        documents(&run.stdout).pop().unwrap()["maintenance_cycle"].clone()
    };
    let held = cycle(".cursor open SELECT id FROM notes\n.maintenance compact\n");
    assert_eq!(
        held["compaction_waiting_on"], "Readers",
        "an open cursor keeps its view: stop"
    );
    let one = cycle(".maintenance compact 1\n");
    assert!(one["compacted_pages"].as_u64() <= Some(256) && one["work_remaining"] == true);
    let all = cycle(".maintenance compact\n");
    // The one step already moved what was pending. A later compact has nothing left.
    assert!(
        all["compacted_pages"].as_u64() == Some(0) && all["work_remaining"] == false,
        "compact report {all}"
    );
}

#[test]
fn inspect_key_values_may_come_in_any_order_and_the_versions_say_if_more_exist() {
    let dir = folder("inspect-key-order");
    let store = dir.join("store");
    let make = "CREATE TABLE p (a INT, b INT, body TEXT, PRIMARY KEY (a, b));\n";
    run_write(
        &dir,
        &store,
        format!(
            "{make}INSERT INTO p VALUES (1, 2, 'first');\nUPDATE p SET body = 'second' WHERE a = 1;\n"
        ),
    );
    let key = |(c, v): (&str, i32), (r, w): (&str, i32)| {
        let json = format!(
            r#"{{"column":"{c}","value":{{"Int64":{v}}},"rest":[["{r}",{{"Int64":{w}}}]]}}"#
        );
        shell(
            &dir,
            &[
                "inspect",
                "key",
                arg(&store),
                "--table",
                "p",
                "--key-json",
                &json,
            ],
            "",
        )
        .stdout
    };
    let (in_order, swapped) = (key(("a", 1), ("b", 2)), key(("b", 2), ("a", 1)));
    assert_eq!(
        in_order, swapped,
        "values named out of key order read the same row"
    );
    let upper = key(("A", 1), ("B", 2));
    assert_eq!(
        in_order, upper,
        "a key named in upper case reads the row and its versions"
    );
    for want in ["replaced_by_commit", "first", "second", "has_more: false"] {
        assert!(
            in_order.contains(want),
            "the row and its versions are printed with {want}: {in_order}"
        );
    }
}

#[test]
fn inspect_key_reads_one_row_whatever_the_stores_size() {
    let dir = folder("inspect-reads-one-row");
    let key = r#"{"column":"id","value":{"Int64":5}}"#;
    for rows in [10, 2000] {
        let store = notes_store(&dir, &format!("rows-{rows}"), rows);
        // One row read is all the work the session may do; reading the table would exceed it.
        let read = ["--read-work", "1", "inspect", "key", arg(&store)];
        let inspect = &[&read[..], &["--table", "notes", "--key-json", key]].concat();
        let mode = |m| std::fs::set_permissions(store.join("readers"), Mode::from_mode(m)).unwrap();
        mode(0o555);
        let run = shell(&dir, inspect, "");
        assert!(
            run.code == Some(0) && run.stdout.contains("row 5"),
            "a store folder that cannot be written is read: {rows}: {}",
            run.stderr
        );
        mode(0o755);
        let _writer = Database::open(&store).unwrap();
        mode(0o555);
        let held = format!("a writer holds the store at {}", store.display());
        let refused = shell(&dir, inspect, "").stderr;
        assert!(refused.contains(&held), "{refused}");
        mode(0o755);
        let scan = shell(
            &dir,
            &["--read-work", "1", arg(&store)],
            "SELECT * FROM notes;\n",
        );
        assert_eq!(
            scan.code,
            Some(1),
            "reading the table exceeds the limit: {}",
            scan.stderr
        );
    }
}

#[test]
fn a_cursor_session_prints_and_refuses_by_its_documented_contract() {
    let dir = folder("a-cursor-session");
    let store = notes_store(&dir, "store", 2);
    let args = [arg(&store), "--json", "--read-cursor-page-rows", "1"];
    let open = ".cursor open SELECT id FROM notes ORDER BY id\n";
    let after = ".cursor fetch\n.cursor close\n.cursor fetch\n.cursor open INSERT INTO notes VALUES (3, 'x')\n.cursor open EXPLAIN SELECT 1\n";
    let run = shell(&dir, &args, format!("{open}{open}{after}"));
    let first = run.stdout.matches("\"id\":1").count();
    assert_eq!(
        first, 1,
        "a second open is refused, so the cursor keeps its place"
    );
    let kinds = "cursor_not_found write_requires_flag cursor_invalid_statement cursor_already_open";
    for kind in kinds.split_whitespace() {
        let said = run.stderr.contains(&format!("\"kind\":\"{kind}\""));
        assert!(said, "{kind} is the kind of a refusal: {}", run.stderr);
    }
    let usage = "\"class\":\"usage\",\"detail\":{\"kind\":\"cursor_invalid_statement\"";
    assert!(
        run.stderr.contains(usage),
        "a statement no cursor reads is misuse: {}",
        run.stderr
    );
}

/// A piped writing shell idle on its input closes the store on Ctrl-C and exits 130 (section 7).
/// What tells a closed store from one left open: the next run's first write lands at once, where
/// after an exit with the store open it is refused while free space is being found.
#[test]
fn an_interrupt_while_a_piped_writing_shell_waits_closes_the_store() {
    let dir = folder("interrupt-idle-writer");
    let store = dir.join("store");
    assert_eq!(run_write(&dir, &store, CREATE_NOTES).code, Some(0));
    let mut writer = piped(&dir, &[arg(&store), "--write", "--json"]);
    let (mut input, mut line) = (writer.stdin.take().unwrap(), String::new());
    let mut output = BufReader::new(writer.stdout.take().unwrap());
    input
        .write_all(b"INSERT INTO notes (id, body) VALUES (1, 'a');\n")
        .unwrap();
    let answered = output.read_line(&mut line).unwrap() > 0;
    let exit = (answered, interrupt(&writer), writer.wait().unwrap().code());
    let said = "the shell answered, waited on its input, and exits 130 on the interrupt";
    assert_eq!(exit, (true, true, Some(130)), "{said}");
    let args = [arg(&store), "--write", "--disk-limit", "32M"];
    let next = shell(
        &dir,
        &args,
        "INSERT INTO notes (id, body) VALUES (2, 'b');\n",
    );
    let said = "the store was closed: the next run's first write lands at once";
    assert_eq!(next.code, Some(0), "{said}: {}", next.stderr);
}

/// M6: with no transaction open a writing shell sends SELECT, WITH and a SHOW that is not a
/// setting to its read session and everything else to the writer, before either charges it.
/// Its reads have no row ceiling.
#[test]
fn a_writing_shell_sends_each_statement_to_the_session_that_answers_it() {
    let dir = folder("which-session");
    let store = dir.join("store");
    let rows: Vec<String> = (1000..=1500).map(|n| format!("({n}, 'n')")).collect();
    let input = format!(
        "{CREATE_NOTES}SET MEMORY_LIMIT 64M;\nSHOW MEMORY_LIMIT;\n\
         WITH n AS (SELECT id FROM notes) SELECT id FROM n;\nSHOW MEMORY_LIMIT;\n\
         INSERT INTO notes (id, body) VALUES (1, '{}');\n\
         INSERT INTO notes (id, body) VALUES {};\n\
         SELECT id FROM notes WHERE id >= 1000;\n",
        "x".repeat(6 << 20),
        rows.join(", ")
    );
    let run = shell(&dir, &[arg(&store), "--write", "--json"], input);
    assert!(run.code == Some(0), "6 MiB is the writer's: {}", run.stderr);
    let results: Vec<Json> = documents(&run.stdout)
        .iter()
        .filter_map(|document| Some(document.get("result")?["rows"].clone()))
        .collect();
    assert_eq!(
        results[0][0]["limit"],
        json!(64 << 20),
        "SHOW reads what SET set"
    );
    let (before, after) = (&results[0][0]["used"], &results[2][0]["used"]);
    assert_eq!(
        after, before,
        "a WITH is the reader's, not charged to the writer"
    );
    assert_eq!(
        results[3].as_array().map(Vec::len),
        Some(501),
        "all 501 rows print"
    );
}
