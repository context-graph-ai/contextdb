//! A session that reads a file can say how old its view of the store is.
//!
//! Reading a committed file means reading ONE image, taken at one moment;
//! every answer that session gives describes the store as it was then, and
//! nothing it does afterwards changes that. A caller looking at such an answer
//! needs to know how old it is, so the moment the image was read is part of
//! what the session can be asked.
//!
//! A session talking to a live owner has no such moment. The owner's state is
//! still moving and every answer is as current as the instant it was asked, so
//! the honest answer is that there is no snapshot instant -- not an invented
//! one that a caller would render and believe.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::Value;
use contextdb_core::Wallclock;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn seed(database: &Database) {
    database
        .execute(
            "CREATE TABLE snapshot_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the snapshot fixture table");
    database
        .execute(
            "INSERT INTO snapshot_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("payload".to_owned(), Value::Text("kept".to_owned())),
            ]),
        )
        .expect("insert the snapshot fixture row");
}

#[test]
fn a_file_session_reports_the_moment_it_read_the_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped snapshot directory");
    let path = directory.path().join("snapshot.db");
    let database = Database::open(&path).expect("open the snapshot fixture writer");
    seed(&database);
    database.close().expect("the fixture writer closes cleanly");

    // A clock the test owns, so the reported instant is a fact about when the
    // store was read rather than about how fast this machine is.
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let session = ReadSession::open(&path).expect("open the idle store");
    assert_eq!(session.route(), ReadRoute::File);
    assert_eq!(
        session.snapshot_at(),
        Some(Wallclock(1_700_000_000_000)),
        "the reported instant is when the image was read"
    );

    // The view does not move underneath the caller: reading again answers
    // from the same image, taken at the same moment.
    session
        .execute("SELECT payload FROM snapshot_rows", &HashMap::new())
        .expect("the idle file answers");
    assert_eq!(
        session.snapshot_at(),
        Some(Wallclock(1_700_000_000_000)),
        "one session reads one image, so its instant never moves"
    );
}

#[test]
fn two_file_sessions_opened_at_different_moments_say_so() {
    let directory = tempfile::TempDir::new().expect("task-scoped snapshot directory");
    let path = directory.path().join("two-reads.db");
    let database = Database::open(&path).expect("open the snapshot fixture writer");
    seed(&database);
    database.close().expect("the fixture writer closes cleanly");

    let earlier = {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        ReadSession::open(&path).expect("open the idle store early")
    };
    let later = {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_060_000);
        ReadSession::open(&path).expect("open the idle store later")
    };

    assert_eq!(earlier.snapshot_at(), Some(Wallclock(1_700_000_000_000)));
    assert_eq!(later.snapshot_at(), Some(Wallclock(1_700_000_060_000)));
    assert!(
        later.snapshot_at() > earlier.snapshot_at(),
        "the session opened later read a newer view of the store"
    );
}

#[test]
fn a_session_reading_a_live_owner_has_no_snapshot_instant_to_report() {
    let directory = tempfile::TempDir::new().expect("task-scoped snapshot directory");
    let runtime_root = secure_runtime_root(directory.path(), "live-snapshot-runtime");
    let path = directory.path().join("live-snapshot.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads");
    seed(&database);

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);
    assert_eq!(
        session.snapshot_at(),
        None,
        "an owner's state is still moving, so there is no one moment to report"
    );

    drop(session);
    database.close().expect("the writer closes cleanly");

    // The same store, now idle, does have one.
    let idle =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("the store is still readable once its owner is gone");
    assert_eq!(idle.route(), ReadRoute::File);
    assert!(
        idle.snapshot_at().is_some(),
        "a committed file was read at a moment, and says which"
    );
}
