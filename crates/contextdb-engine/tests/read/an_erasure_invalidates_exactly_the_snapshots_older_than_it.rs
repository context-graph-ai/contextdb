//! A completed erasure invalidates exactly the reads that captured their view
//! before it, and nothing else.
//!
//! Promise (contextdb-intent, Policy 12 and Policy 22): a read that captured
//! its snapshot before an authoritative erasure of a table it reads is refused
//! with a typed erasure failure and emits no further rows; a read that
//! captured its snapshot after the erasure, a read of an unrelated table, and a
//! read after a rolled-back erasure are all served in full; a fresh row with a
//! purged row's key, created after the erasure, is readable. This holds for
//! PURGE and for local DISCARD alike. DISCARD leaves no purge record behind, so
//! its effect on older snapshots is the case a lineage-only check misses.
//!
//! Each journey keeps the writer in a child process, so the reader is served by
//! the owner route and its page authorization runs at the writer, after the
//! erasure.
//!
//! Reason (2026-10-16): final read authorization is moving from enumerating
//! every current and displaced purge record to a durable per-table and global
//! erasure frontier. Only PURGE writes the records the old check read; the
//! rows below pin the contract the frontier must keep, in particular that a
//! committed DISCARD still refuses an older snapshot, that a rollback raises no
//! frontier, that the frontier survives many later ordinary displacements and
//! does not refuse a snapshot captured after the erasure.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};

use contextdb_core::Value;
use contextdb_core::read_contract::{CursorPage, ReadFailureKind, ReadLimits, ReadRoute};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, ReadCursor, ReadSession, ReadSessionOptions};

const ROLE: &str = "CONTEXTDB_ERASURE_SNAPSHOT_ROLE";
const PATH: &str = "CONTEXTDB_ERASURE_SNAPSHOT_PATH";
const ROWS: i64 = 120;

fn none() -> HashMap<String, Value> {
    HashMap::new()
}

/// The writer process: holds the store and runs the statements it is told to
/// run, one per line. `rollback <sql>` stages the statement in a transaction
/// and rolls it back.
#[test]
fn writer_child() {
    let Ok(role) = std::env::var(ROLE) else {
        return;
    };
    assert_eq!(role, "writer");
    let path = std::env::var(PATH).expect("child store path");
    let db = Database::open(&path).expect("child writer opens the store");
    println!("HOLDING");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    loop {
        line.clear();
        let read = std::io::stdin().lock().read_line(&mut line).unwrap();
        let command = line.trim();
        if read == 0 || command == "release" {
            break;
        }
        if let Some(sql) = command.strip_prefix("rollback ") {
            let tx = db.begin().unwrap();
            db.execute_in_tx(tx, sql, &none())
                .expect("stage the statement");
            db.rollback(tx).unwrap();
        } else {
            db.execute(command, &none()).expect("run the statement");
        }
        println!("DONE");
        std::io::stdout().flush().unwrap();
    }
    drop(db);
    println!("RELEASED");
    std::io::stdout().flush().unwrap();
}

struct Writer {
    child: Child,
    output: BufReader<ChildStdout>,
}

impl Writer {
    fn spawn(path: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg(super::child_test_filter(module_path!(), "writer_child"))
            .arg("--nocapture")
            .env(ROLE, "writer")
            .env(PATH, path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn the writer process");
        let output = BufReader::new(child.stdout.take().expect("child stdout"));
        let mut writer = Self { child, output };
        writer.wait_for("HOLDING");
        writer
    }

    fn wait_for(&mut self, marker: &str) {
        let mut line = String::new();
        while self.output.read_line(&mut line).expect("child output") != 0 {
            if line.contains(marker) {
                return;
            }
            line.clear();
        }
        panic!("the writer process exited before {marker}");
    }

    fn run(&mut self, command: &str) {
        writeln!(self.child.stdin.as_mut().unwrap(), "{command}").unwrap();
        self.wait_for("DONE");
    }

    fn release(mut self) {
        writeln!(self.child.stdin.as_mut().unwrap(), "release").unwrap();
        self.wait_for("RELEASED");
        assert!(self.child.wait().unwrap().success());
    }
}

fn options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            result_rows: 200,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    }
}

fn ids(page: &CursorPage) -> Vec<i64> {
    page.rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

fn seed(path: &Path) {
    let db = Database::open(path).expect("create the store");
    for table in ["notes", "other_notes"] {
        db.execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT NOT NULL) HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST"),
            &none(),
        )
        .expect("declare a table");
        for id in 0..ROWS {
            db.execute(
                &format!("INSERT INTO {table} (id, body) VALUES ({id}, 'secret')"),
                &none(),
            )
            .expect("seed a row");
        }
    }
    db.close().unwrap();
}

fn session(path: &Path) -> ReadSession {
    let session = ReadSession::open_with_options(path, options()).expect("open a reader");
    assert_eq!(
        session.route(),
        ReadRoute::Owner,
        "the writer process owns the store"
    );
    session
}

/// A cursor over `table` whose first page is delivered and which has more.
fn open(session: &ReadSession, table: &str) -> ReadCursor {
    let cursor = session
        .open_cursor(&format!("SELECT id FROM {table} ORDER BY id"), &none())
        .expect("open a cursor");
    assert!(cursor.first_page().has_more, "the cursor has further pages");
    cursor
}

fn assert_erased(result: contextdb_core::Result<CursorPage>) {
    match result {
        Err(contextdb_core::Error::ReadFailure(failure)) => {
            assert_eq!(failure.kind(), ReadFailureKind::AuthoritativeErasure);
        }
        other => panic!("an older snapshot must be refused as erased: {other:?}"),
    }
}

fn drain(mut cursor: ReadCursor) -> Vec<i64> {
    let mut seen = ids(cursor.first_page());
    while cursor.first_page().has_more {
        let page = cursor.fetch(None).expect("a served continuation page");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    seen
}

struct PauseBeforeRegistration {
    reached: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl ReadSessionTestObserver for PauseBeforeRegistration {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event == ReadSessionEvent::BeforeOwnerCursorHandoffRegistration {
            self.reached.send(()).expect("report the first page");
            self.resume
                .lock()
                .expect("registration pause")
                .recv()
                .expect("resume registration");
        }
    }
}

/// Opens a cursor over `table` that has captured its snapshot and received its
/// first page, runs `during` while the cursor is not yet registered, then lets
/// the registration finish. The cursor's later pages are authorized after
/// `during` has completed.
fn open_across(path: &Path, table: &str, during: impl FnOnce()) -> ReadCursor {
    // The reader of `session()` is already registered; this one is the paused
    // opener.
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let observer = Arc::new(PauseBeforeRegistration {
        reached: reached_tx,
        resume: Mutex::new(resume_rx),
    });
    let session = ReadSession::open_with_observer_for_test(path, options(), observer)
        .expect("open a reader with a pause");
    let sql = format!("SELECT id FROM {table} ORDER BY id");
    let opening = std::thread::spawn(move || {
        let cursor = session.open_cursor(&sql, &none());
        (session, cursor)
    });
    reached_rx.recv().expect("the first page was received");
    during();
    resume_tx.send(()).expect("finish registration");
    let (_session, cursor) = opening.join().expect("join the opener");
    let cursor = cursor.expect("register the older snapshot");
    assert!(cursor.first_page().has_more, "the cursor has further pages");
    cursor
}

fn without(skipped: &[i64]) -> Vec<i64> {
    (0..ROWS).filter(|id| !skipped.contains(id)).collect()
}

#[test]
fn a_committed_discard_refuses_a_cursor_opened_before_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("discard.db");
    seed(&path);
    let mut writer = Writer::spawn(&path);
    let session = session(&path);
    // A registered, idle cursor keeps this process a fully acquired reader, so a
    // purge is not refused as racing a reader that is still acquiring.
    let _hold = open(&session, "other_notes");
    let mut unrelated = open(&session, "other_notes");
    let mut older = open_across(&path, "notes", || {
        writer.run("DISCARD FROM notes WHERE id = 7")
    });
    assert_erased(older.fetch(None));
    assert!(!older.is_live(), "erasure ends the cursor");
    let mut seen = ids(unrelated.first_page());
    loop {
        let page = unrelated
            .fetch(None)
            .expect("the unrelated table is served");
        seen.extend(ids(&page));
        if !page.has_more {
            break;
        }
    }
    assert_eq!(seen, without(&[]));
    writer.release();
}

#[test]
fn a_cursor_opened_after_a_discard_is_served_in_full() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("discard-after.db");
    seed(&path);
    let mut writer = Writer::spawn(&path);
    let session = session(&path);
    // A registered, idle cursor keeps this process a fully acquired reader, so a
    // purge is not refused as racing a reader that is still acquiring.
    let _hold = open(&session, "other_notes");
    writer.run("DISCARD FROM notes WHERE id = 7");
    assert_eq!(drain(open(&session, "notes")), without(&[7]));
    writer.release();
}

#[test]
fn a_rolled_back_discard_leaves_an_older_cursor_valid() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("discard-rollback.db");
    seed(&path);
    let mut writer = Writer::spawn(&path);
    let session = session(&path);
    // A registered, idle cursor keeps this process a fully acquired reader, so a
    // purge is not refused as racing a reader that is still acquiring.
    let _hold = open(&session, "other_notes");
    let older = open_across(&path, "notes", || {
        writer.run("rollback DISCARD FROM notes WHERE id = 7")
    });
    assert_eq!(
        drain(older),
        without(&[]),
        "a rollback erased nothing and raised no frontier"
    );
    writer.release();
}

#[test]
fn a_fresh_row_with_a_purged_rows_key_is_read_by_a_later_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge-fresh.db");
    seed(&path);
    let mut writer = Writer::spawn(&path);
    let session = session(&path);
    writer.run("PURGE FROM notes WHERE id = 7");
    writer.run("INSERT INTO notes (id, body) VALUES (7, 'fresh')");
    assert_eq!(
        drain(open(&session, "notes")),
        without(&[]),
        "the fresh row is read"
    );
    let fresh = session
        .execute("SELECT body FROM notes WHERE id = 7", &none())
        .expect("point read of the fresh row");
    assert_eq!(fresh.rows, vec![vec![Value::Text("fresh".into())]]);
    writer.release();
}

#[test]
fn the_erasure_frontier_works_after_the_store_changes_hands() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reopen.db");
    seed(&path);
    let mut first = Writer::spawn(&path);
    first.run("DISCARD FROM notes WHERE id = 7");
    first.run("PURGE FROM other_notes WHERE id = 9");
    first.release();
    let mut second = Writer::spawn(&path);
    let session = session(&path);
    // A registered, idle cursor keeps this process a fully acquired reader, so a
    // purge is not refused as racing a reader that is still acquiring.
    let _hold = open(&session, "other_notes");
    // A snapshot captured now is newer than both earlier erasures.
    assert_eq!(drain(open(&session, "notes")), without(&[7]));
    assert_eq!(drain(open(&session, "other_notes")), without(&[9]));
    // An erasure by the new owner still refuses what predates it.
    let mut older = open_across(&path, "notes", || {
        second.run("DISCARD FROM notes WHERE id = 8")
    });
    assert_erased(older.fetch(None));
    second.release();
}

// Policy 12: the final export fence must include DISCARD, which intentionally
// creates no Purged lifecycle record. The existing post-capture pause cannot
// distinguish a successful copy from a refusal during copying; the existing
// commit observer's export-publication event drives this exact race instead.
#[test]
fn a_discard_after_the_export_copy_refuses_publication_of_the_older_artifact() {
    use contextdb_engine::writer_commit_test_seams::{self, CommitBoundary};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("export-source.db");
    let destination = directory.path().join("export.db");
    seed(&path);
    let db = Arc::new(Database::open(&path).unwrap());
    let (reached, copied) = mpsc::channel();
    let (resume, proceed) = mpsc::channel();
    let proceed = Mutex::new(proceed);
    let exporting = db.clone();
    let target = destination.clone();
    let worker = std::thread::spawn(move || {
        writer_commit_test_seams::observe(
            Arc::new(move |boundary| {
                if boundary == CommitBoundary::ExportBeforePublication {
                    reached.send(()).unwrap();
                    proceed.lock().unwrap().recv().unwrap();
                }
            }),
            || exporting.export_snapshot(&target),
        )
    });
    copied.recv().unwrap();
    let discard = db.execute("DISCARD FROM notes WHERE id = 7", &none());
    resume.send(()).unwrap();
    let result = worker.join().unwrap();
    discard.expect("the DISCARD commits after copying finishes");
    assert!(
        matches!(result, Err(contextdb_core::Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::AuthoritativeErasure)
    );
    assert!(
        !destination.exists(),
        "an older erased image is never published"
    );
    let partials = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("tmpexport"))
        .collect::<Vec<_>>();
    assert!(
        partials.is_empty(),
        "the refused export releases its temporary artifact and lock"
    );
    db.close().unwrap();
}

// Unlike the acquisition-window fixture, these cursors have completed their
// reader registration before PURGE starts. No acquiring-reader refusal can
// stand in for a committed erasure or hide its required typed continuation.
fn purge_refuses_a_registered_older_cursor(after_handover: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("registered-purge.db");
    seed(&path);
    let mut writer = Some(Writer::spawn(&path));
    let session = session(&path);
    let mut older = open(&session, "notes");
    let unrelated = open(&session, "other_notes");
    writer
        .as_mut()
        .unwrap()
        .run("PURGE FROM notes WHERE id = 7");
    if after_handover {
        writer.take().unwrap().release();
    }
    assert_erased(older.fetch(None));
    assert!(!older.is_live());
    assert_eq!(drain(unrelated), without(&[]));
    if let Some(writer) = writer {
        writer.release();
    }
}
#[test]
fn a_committed_purge_refuses_a_fully_registered_older_cursor() {
    purge_refuses_a_registered_older_cursor(false);
}
#[test]
fn a_committed_purge_still_refuses_its_registered_older_cursor_after_handover() {
    purge_refuses_a_registered_older_cursor(true);
}
