//! A reader of a store whose writer died holds a starting writer back only for
//! a fixed amount of work, however large the store is.
//!
//! Promise (contextdb-intent, Policies 21-23): a writer waiting on a starting
//! reader waits for work bounded by the storage format, never by the size of
//! the store. For a crashed store the reader must still choose, and check, the
//! commit a repair would keep -- but that walk grows with the store, so it
//! happens after the reader has released the data lock and the hold, on a root
//! its provisional registration already protects. Under admission the reader
//! reads the header and nothing else.
//!
//! The writer runs in a child process and dies (a seam on its own store file,
//! never a sleep):
//! - between two commits (every commit that returned is whole);
//! - at the sync of a torn commit, whose header reached the file and whose
//!   pages did not, so the primary fails its checksums and the commit before
//!   it, the secondary, is the one a repair keeps.
//!
//! The reader is stopped just after it released the data lock and the hold.
//! A writer opening at that moment is granted, and repairs the store.

use crate::writer_acquisition_support::{WATCHDOG, next_event, open_writer, watch};
use contextdb_core::Value;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderPausePoint, WriterAdmissionEvent, arm_writer_torn_commit_crash_for_test,
    pause_readers_at_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, ReadSession};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const ROLE: &str = "CONTEXTDB_CRASHED_STORE_WALK_ROLE";
const STORE: &str = "CONTEXTDB_CRASHED_STORE_WALK_STORE";
const ROWS: &str = "CONTEXTDB_CRASHED_STORE_WALK_ROWS";
/// The format's admission budget in base pages.
const ADMISSION_PAGE_BUDGET: u64 = 110;

fn insert(database: &Database, id: i64) {
    database
        .execute(
            &format!("INSERT INTO crash_rows (id, payload) VALUES ({id}, 'row-{id}')"),
            &HashMap::new(),
        )
        .expect("commit one row");
}

/// The child half: commit `ROWS` rows, one commit each, then die as `ROLE`
/// says.
#[test]
fn crashing_writer_child() {
    let Ok(role) = std::env::var(ROLE) else {
        return;
    };
    let store = PathBuf::from(std::env::var(STORE).expect("child store path"));
    let rows: i64 = std::env::var(ROWS)
        .expect("child row count")
        .parse()
        .expect("a row count");
    let database = Database::open(&store).expect("open the writer");
    database
        .execute(
            "CREATE TABLE crash_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the table");
    for id in 1..=rows {
        insert(&database, id);
    }
    eprintln!("WRITER_COMMITTED={rows}");
    match role.as_str() {
        "between-commits" => std::process::abort(),
        "at-a-torn-commits-sync" => {
            arm_writer_torn_commit_crash_for_test();
            insert(&database, rows + 1);
            panic!("the armed sync never came: the commit was not durable");
        }
        other => panic!("unknown writer role {other}"),
    }
}

fn crashed_store(directory: &Path, rows: i64, role: &str) -> PathBuf {
    let store = directory.join("crashed.db");
    let filter = module_path!()
        .split_once("::")
        .map(|(_, rest)| format!("{rest}::crashing_writer_child"))
        .expect("a module path inside the test binary");
    let writer = Command::new(std::env::current_exe().expect("test binary"))
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(ROLE, role)
        .env(STORE, &store)
        .env(ROWS, rows.to_string())
        .output()
        .expect("run the writer child");
    let stderr = String::from_utf8_lossy(&writer.stderr);
    assert!(
        !writer.status.success() && stderr.contains(&format!("WRITER_COMMITTED={rows}")),
        "the writer must die after its commits: {stderr}"
    );
    store
}

fn visible_ids(rows: Vec<Vec<Value>>) -> Vec<i64> {
    rows.into_iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref other => panic!("an id is an integer, got {other:?}"),
        })
        .collect()
}

fn assert_the_walk_of_a_crashed_store_is_outside_admission(rows: i64, role: &str) {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let store = crashed_store(directory.path(), rows, role);
    let expected: Vec<i64> = (1..=rows).collect();

    let pause = pause_readers_at_for_test(&store, ReaderPausePoint::AfterDRelease);
    let (tell, heard) = std::sync::mpsc::channel();
    let reader_path = store.clone();
    let reader = std::thread::spawn(move || {
        let session = ReadSession::open(&reader_path);
        let receipt = redb::last_admission_read_receipt();
        let _ = tell.send((session, receipt));
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never released the data lock and the hold");

    // The reader holds nothing a writer waits for: the writer is granted and
    // repairs the store while the reader's walk has not yet happened.
    let watch = watch(&store);
    let writer = open_writer(&store, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(
            &watch,
            "while the reader of a crashed store is outside admission"
        ),
        WriterAdmissionEvent::Acquired,
        "{role} at {rows} rows: the writer is granted at once"
    );
    let database = writer
        .finish()
        .expect("the writer opens and repairs the store");

    pause.release(ticket);
    let (session, receipt) = heard
        .recv_timeout(WATCHDOG)
        .expect("the reader never finished its walk");
    reader.join().expect("the reader thread finishes");

    let receipt = receipt.expect("the reader's root capture left a receipt");
    assert!(
        receipt.charged_pages <= ADMISSION_PAGE_BUDGET,
        "{role} at {rows} rows: admission charged {} base pages, over the format's \
         {ADMISSION_PAGE_BUDGET}",
        receipt.charged_pages
    );
    let session = session.expect("the reader reads the commit the repair kept");
    let reader_rows = visible_ids(
        session
            .execute("SELECT id FROM crash_rows ORDER BY id", &HashMap::new())
            .expect("the reader reads the commit the repair kept")
            .rows,
    );
    assert_eq!(
        reader_rows, expected,
        "{role} at {rows} rows: the reader reads the commit the repair kept"
    );
    let writer_rows = visible_ids(
        database
            .execute("SELECT id FROM crash_rows ORDER BY id", &HashMap::new())
            .expect("the repaired writer reads")
            .rows,
    );
    assert_eq!(
        writer_rows, expected,
        "{role} at {rows} rows: the writer that repaired the store sees the same rows"
    );
    drop(session);
    database.close().expect("close the writer");
}

/// Test-estate reason (2026-10-15): admission of a reader of a store whose
/// writer died between commits reads a fixed number of pages at every store
/// size, and the writer is granted before the reader's walk. Today the walk of
/// the whole tree runs under the data lock and the receipt grows with the store.
#[test]
fn a_store_whose_writer_died_between_commits_is_walked_outside_admission() {
    for rows in [3, 64, 256] {
        assert_the_walk_of_a_crashed_store_is_outside_admission(rows, "between-commits");
    }
}

/// Test-estate reason (2026-10-15): the same when the primary commit is torn,
/// so the reader's walk fails the primary and falls back to the secondary: the
/// fallback walk is outside admission too and picks the commit the repair keeps.
#[test]
fn a_store_with_a_torn_primary_is_walked_outside_admission_and_falls_back_to_the_secondary() {
    for rows in [3, 64, 256] {
        assert_the_walk_of_a_crashed_store_is_outside_admission(rows, "at-a-torn-commits-sync");
    }
}

/// Test-estate reason (2026-10-15): without a boot identity a crashed store's reader
/// cannot know whether the dead writer's last commit is whole, so it is not
/// served the store: it gets the typed needs-writer refusal and the next writer
/// repairs the store. macOS only; the Linux rows above hold where the boot
/// identity exists.
#[cfg(target_os = "macos")]
#[test]
fn on_macos_a_crashed_stores_reader_is_told_a_writer_must_repair_it_first() {
    use contextdb_core::read_contract::ReadFailureKind;

    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let store = crashed_store(directory.path(), 3, "between-commits");
    match ReadSession::open(&store) {
        Err(contextdb_core::Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::DirectReadRequiresWriter,
            "a crashed store's reader is told a writer is needed"
        ),
        Err(other) => panic!("the refusal is the typed needs-writer failure: {other:?}"),
        Ok(_) => panic!("a crashed store is not served without a boot identity"),
    }
}
