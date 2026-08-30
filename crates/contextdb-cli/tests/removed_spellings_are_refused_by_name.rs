//! A spelling the contract removed is answered BY NAME.
//!
//! Two surfaces remove spellings, and both used to answer in a way that left
//! the person guessing. `repair` was replaced by `diagnose`, and typing it
//! produced a parser complaint about the PATH that followed it — never the
//! word that was wrong. The backslash aliases `\trace` and `\sync` do not
//! exist, and typing one with its argument echoed back only the first word,
//! so `\trace on` was reported as `\trace`.
//!
//! What is held here: the refusal names what the person actually typed, and
//! the removed word never returns to a discovery surface to do it.

use std::io::Write;
use std::process::{Command, Stdio};

const EXIT_USAGE: i32 = 2;
const EXIT_ERROR: i32 = 1;

struct Answer {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Answer {
    fn describe(&self) -> String {
        format!(
            "exit {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.code, self.stdout, self.stderr
        )
    }
}

fn run(args: &[&str], stdin: &str) -> Answer {
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    command.args(args);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let output = child.wait_with_output().expect("wait for contextdb");
    Answer {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// A real store. A reading session resolves its route at start, so a session
/// command still needs a store to be IN -- what these tests hold is that the
/// session reads no data from it, not that it opened nothing.
fn a_store(tag: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "cdb-removed-spellings-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("store.db").to_string_lossy().into_owned();
    let seeded = run(
        &[&path, "--write"],
        "CREATE TABLE rows (id INTEGER PRIMARY KEY);\n",
    );
    assert_eq!(
        seeded.code,
        Some(0),
        "the fixture store must be created before it is read.\n{}",
        seeded.describe()
    );
    path
}

#[test]
fn the_removed_repair_command_is_refused_by_name_and_points_at_diagnose() {
    let answer = run(&["repair", &a_store("repair")], "");

    assert_eq!(
        answer.code,
        Some(EXIT_USAGE),
        "a removed spelling is wrong about the invocation, so nothing is attempted.\n{}",
        answer.describe()
    );
    let message = format!("{}{}", answer.stdout, answer.stderr);
    assert!(
        message.contains("repair"),
        "the refusal names the word that was wrong, not the path that followed it.\n{}",
        answer.describe()
    );
    assert!(
        message.contains("diagnose"),
        "and names the word that replaced it, so the person can rerun.\n{}",
        answer.describe()
    );
}

#[test]
fn the_removed_repair_command_stays_off_every_discovery_surface() {
    for discovery in [vec!["--help"], vec!["help"]] {
        let answer = run(&discovery, "");
        let published = format!("{}{}", answer.stdout, answer.stderr);
        assert!(
            !published.contains("repair"),
            "`repair` is answered when typed but never offered: {discovery:?} must not \
             publish it.\n{}",
            answer.describe()
        );
        assert!(
            published.contains("diagnose"),
            "the replacement IS offered.\n{}",
            answer.describe()
        );
    }
}

#[test]
fn a_removed_meta_alias_is_refused_carrying_the_whole_typed_line() {
    for typed in ["\\trace on", "\\sync status"] {
        let answer = run(&[&a_store("alias"), "--json"], &format!("{typed}\n"));

        assert_eq!(
            answer.code,
            Some(EXIT_ERROR),
            "a refused meta-command makes a piped session exit 1.\n{}",
            answer.describe()
        );
        assert!(
            answer.stdout.trim().is_empty(),
            "a refusal publishes no result.\n{}",
            answer.describe()
        );
        assert!(
            answer.stderr.contains(typed),
            "the refusal carries the WHOLE spelling that was typed ({typed}), not just its \
             first word.\n{}",
            answer.describe()
        );
    }
}

/// The dotted spellings these aliases used to abbreviate are still real, so
/// the refusal above is about the alias and not about the command.
#[test]
fn the_dotted_spellings_the_removed_aliases_abbreviated_still_answer() {
    let answer = run(&[&a_store("dotted"), "--json"], ".trace on\n.sync status\n");

    assert_eq!(
        answer.code,
        Some(0),
        "`.trace` and `.sync status` are session commands and answer without reading \
         data.\n{}",
        answer.describe()
    );
    assert!(
        answer.stdout.contains("\"trace\""),
        "`.trace on` publishes its trace state.\n{}",
        answer.describe()
    );
    assert!(
        answer.stdout.contains("\"sync_status\""),
        "`.sync status` publishes this session's sync state.\n{}",
        answer.describe()
    );
}

/// A family head with no operation after it names the operations that exist.
/// `.sync` is declared `Invalid` by the command registry exactly like
/// `.events`, `.maintenance`, `.cursor`, and `.owner`; answering it as a
/// silent `.sync status` would make the registry and the session disagree
/// about what the bare word means.
#[test]
fn a_family_head_with_no_operation_after_it_is_a_usage_error_in_either_session() {
    for writes in [false, true] {
        let store = a_store(if writes { "bare-write" } else { "bare-read" });
        let mut args = vec![store.as_str(), "--json"];
        if writes {
            args.insert(1, "--write");
        }
        let answer = run(&args, ".sync\n");

        assert_ne!(
            answer.code,
            Some(0),
            "`.sync` alone is not an operation, so the session does not succeed \
             (writes_permitted={writes}).\n{}",
            answer.describe()
        );
        assert!(
            answer.stdout.trim().is_empty(),
            "a refused meta-command publishes no result (writes_permitted={writes}).\n{}",
            answer.describe()
        );
        assert!(
            answer.stderr.contains("\"class\":\"usage\""),
            "the invocation was wrong, not the data (writes_permitted={writes}).\n{}",
            answer.describe()
        );
        assert!(
            answer.stderr.contains(".sync status") && answer.stderr.contains(".sync push"),
            "the refusal names the operations that do exist \
             (writes_permitted={writes}).\n{}",
            answer.describe()
        );
    }
}
