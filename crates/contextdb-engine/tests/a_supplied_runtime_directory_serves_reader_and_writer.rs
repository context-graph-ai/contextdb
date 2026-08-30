//! One operator-supplied runtime directory serves both sides of a deployment.
//!
//! A container, a packaged service, or a Home Assistant add-on has no
//! `XDG_RUNTIME_DIR`, so it states where the local read channel lives. The
//! writer has always honored that statement. These journeys hold the other
//! half: an embedding caller states the SAME directory to its reading session
//! and reaches the live owner through it.
//!
//! Every opener here is the production one an embedder actually calls. No test
//! seam is used anywhere in this file on purpose -- a directory that only works
//! through a `_for_test` door is a directory a packaged deployment cannot use.

#![cfg(unix)]

use contextdb_core::read_contract::{OwnerServingState, ReadFailureKind, ReadRoute};
use contextdb_core::{Error, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadProgress, ReadProgressObserver,
    ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// An owner-only directory an operator supplies, of the exact shape a service
/// manager creates for a packaged service.
fn supplied_runtime_directory(inside: &Path, name: &str) -> PathBuf {
    let root = inside.join(name);
    std::fs::create_dir(&root).expect("create the operator-supplied runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the operator-supplied runtime directory");
    std::fs::canonicalize(&root).expect("resolve the supplied directory the way the store will")
}

fn seed(database: &Database) {
    database
        .execute(
            "CREATE TABLE shared (id INTEGER PRIMARY KEY, label TEXT)",
            &HashMap::new(),
        )
        .expect("create the shared table");
    database
        .execute(
            "INSERT INTO shared (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("label".to_owned(), Value::Text("kept".to_owned())),
            ]),
        )
        .expect("seed one row");
}

/// A writer that publishes its channel in the directory it was handed.
fn writer_in(directory: &Path, runtime: &Path) -> (Database, PathBuf) {
    let path = directory.join("supplied.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("a writer publishes its channel where the operator said");
    assert_eq!(
        database.owner_read_status().state,
        OwnerServingState::Serving,
        "the supplied directory is one the writer can actually serve from"
    );
    seed(&database);
    (database, path)
}

#[derive(Default)]
struct CountingObserver {
    reports: AtomicUsize,
}

impl ReadProgressObserver for CountingObserver {
    fn progress(&self, _progress: ReadProgress) {
        self.reports.fetch_add(1, Ordering::SeqCst);
    }
}

/// Every production opener that takes the directory reaches the owner through
/// it, and the session it hands back really reads rows.
#[test]
fn every_reading_opener_that_takes_the_directory_reaches_the_owner_through_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped supplied-runtime directory");
    let runtime = supplied_runtime_directory(directory.path(), "supplied-runtime");
    let (database, path) = writer_in(directory.path(), &runtime);

    let session = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("a reader told where the channel lives finds the owner");
    assert_eq!(session.route(), ReadRoute::Owner);
    let answer = session
        .execute("SELECT label FROM shared ORDER BY id", &HashMap::new())
        .expect("the owner answers over its channel");
    assert_eq!(answer.rows.len(), 1, "the seeded row travels the channel");
    drop(session);

    let owner_only = ReadSession::open_owner_only_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("an owner-only open finds the same owner");
    assert_eq!(owner_only.route(), ReadRoute::Owner);
    drop(owner_only);

    let observer = Arc::new(CountingObserver::default());
    let watched = ReadSession::open_with_progress_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
        observer.clone(),
    )
    .expect("a watched open finds the same owner");
    assert_eq!(watched.route(), ReadRoute::Owner);
    drop(watched);

    let report = ReadSession::owner_report_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.as_path()),
    )
    .expect("the owner reports itself through the same directory");
    assert_eq!(
        report.status.state,
        OwnerServingState::Serving,
        "the owner reached through the supplied directory is the serving one"
    );
    assert!(
        report.serving.is_some(),
        "a serving owner reports the ceilings it is serving under"
    );

    database.close().expect("the writer closes cleanly");
}

/// The control: the same directory, and nobody owning the store. There is no
/// channel in it, so the committed file answers -- the directory is where a
/// reader LOOKS, not a claim that an owner is there.
#[test]
fn the_same_directory_reads_the_committed_file_when_nobody_owns_the_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped supplied-runtime directory");
    let runtime = supplied_runtime_directory(directory.path(), "supplied-runtime");
    let (database, path) = writer_in(directory.path(), &runtime);
    database.close().expect("the writer releases the store");

    let session = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("an unowned store is readable");
    assert_eq!(session.route(), ReadRoute::File);
    let answer = session
        .execute("SELECT label FROM shared ORDER BY id", &HashMap::new())
        .expect("the committed file answers");
    assert_eq!(answer.rows.len(), 1);
    drop(session);

    let refusal = ReadSession::open_owner_only_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime),
    )
    .err()
    .expect("an owner-only open of an unowned store has no owner to ask");
    let Error::ReadFailure(failure) = &refusal else {
        panic!("an absent owner is a typed refusal: {refusal:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::OwnerNotRunning);
}
