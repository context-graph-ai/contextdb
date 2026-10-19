//! A writer process that dies releases everything it held on the store, with
//! no cleanup by anyone else. (The store itself needs the next writer's
//! recovery before it can be read directly: that is the existing contract and
//! this proof does not change it.)
//!
//! Promise (contextdb-intent, Policies 21-23): the store's coordination is
//! made of kernel locks that die with their holder. A writer killed mid-life
//! leaves the store immediately openable for the next writer, with no stale claim to wait out and no waiting writer's gate byte
//! left taken. Every lock on the data file is a byte-range lock tied to its
//! open file description, so process death releases it exactly as the
//! whole-file lock it replaced did.
//!
//! Test-estate reason (2026-10-15, reader/writer handoff, lock-kind
//! conversion): the conversion of the data-file locks changes what dies with
//! a process; no earlier test kills a writer process and then opens the store
//! from a fresh process view with the admission counters in hand. A
//! subprocess is the only way to kill a holder. One function: it needs one
//! child process and one kill.

use crate::writer_acquisition_support::{
    SEEDED_ROWS, count_rows, counts, open_writer, seeded_store,
};
use contextdb_engine::{Database, DatabaseOpenOptions, ReadSession};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

const ROLE: &str = "CONTEXTDB_DEAD_WRITER_ROLE";
const STORE: &str = "CONTEXTDB_DEAD_WRITER_STORE";

/// The child half of the killed-writer test: a writer that holds the store until it is killed.
#[test]
fn holding_writer_child() {
    if std::env::var_os(ROLE).is_none() {
        return;
    }
    let path = std::env::var(STORE).expect("child store path");
    let database = Database::open(&path).expect("the child writer opens");
    println!("HOLDING");
    use std::io::Write as _;
    std::io::stdout().flush().expect("flush the marker");
    // Wait to be killed; never exit on our own.
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    drop(database);
}

/// Test-estate reason (2026-10-15): every lock on the data file dies with its holder: after a
/// writer process is killed the next writer opens without waiting. A subprocess is the only way to
/// kill a holder.
#[test]
fn a_killed_writer_process_leaves_the_store_open_to_the_next_writer() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "killed-writer.db");

    let filter = module_path!()
        .split_once("::")
        .map(|(_, rest)| format!("{rest}::holding_writer_child"))
        .expect("a module path inside the test binary");
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(ROLE, "holding")
        .env(STORE, &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the holding writer process");
    let mut output = BufReader::new(child.stdout.take().expect("child stdout"));
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(
            output
                .read_line(&mut line)
                .expect("read the child's output"),
            0,
            "the child exited before it held the store"
        );
        if line.trim() == "HOLDING" {
            break;
        }
    }

    child.kill().expect("kill the holding writer");
    child.wait().expect("reap the killed writer");

    let before = counts(&path);
    // The next writer's open recovers the store the dead writer left; it
    // must answer without waiting on anything the dead process held.
    let next = open_writer(&path, DatabaseOpenOptions::default());
    next.finish()
        .expect("the next writer opens")
        .close()
        .expect("close");

    // The store is whole again: a reader opens it and sees the seeded rows.
    let session = ReadSession::open(&path).expect("a reader opens the recovered store");
    assert_eq!(count_rows(&session), SEEDED_ROWS);
    assert_eq!(
        counts(&path).writer_hold_waits,
        before.writer_hold_waits,
        "the next writer made no blocking acquisition"
    );
}
