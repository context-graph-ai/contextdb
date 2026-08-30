//! A store held by a writer that will not serve reads says so, by name.
//!
//! The companion beside every file-backed store records what its writer
//! decided about inspection: serving, deliberately disabled, or wanted but
//! failed to start. That record exists so a later reader can be TOLD why it
//! cannot have rows, instead of being dropped onto the committed file, meeting
//! the writer's own lock there, and reporting whatever the storage layer
//! happened to say.
//!
//! So a reading session that finds a recorded owner which is not serving is
//! refused `owner_not_serving`, carrying the reason the writer recorded --
//! before the file is opened at all. Only a store nobody owns falls through to
//! the file, and that is what the control below holds in place.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{
    OwnerServingReason, OwnerServingState, ReadFailureDetail, ReadFailureKind, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::persistence::read_persistence_test_scaffold;
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
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
            "CREATE TABLE held (id INTEGER PRIMARY KEY, label TEXT)",
            &HashMap::new(),
        )
        .expect("create the held table");
    database
        .execute(
            "INSERT INTO held (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("label".to_owned(), Value::Text("kept".to_owned())),
            ]),
        )
        .expect("seed one row");
}

/// A writer that holds the store and was told not to serve inspection at all.
fn writer_that_will_not_serve(directory: &Path, runtime_root: PathBuf) -> (Database, PathBuf) {
    let path = directory.join("unserved.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                enabled: false,
                runtime_dir: Some(runtime_root),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("a writer opens even when it will not serve inspection");
    seed(&database);
    (database, path)
}

/// The reader is refused by name, with the reason its writer recorded, and
/// never reaches the committed file.
#[test]
fn a_reader_is_told_the_recorded_owner_state_instead_of_the_file_s_complaint() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-state directory");
    let runtime_root = secure_runtime_root(directory.path(), "unserved-runtime");
    let (database, path) = writer_that_will_not_serve(directory.path(), runtime_root.clone());

    // What the writer actually put on disk, so the refusal below is measured
    // against a record that exists rather than an assumed one.
    let recorded = read_persistence_test_scaffold::inspect_companion_record_for_test(&path)
        .expect("the writer published its companion record");
    assert_eq!(
        recorded.fields.owner_read_status,
        database.owner_read_status(),
        "the companion records the state the writer itself reports, because that record is the \
         only thing a later reader has to go on"
    );
    assert_eq!(
        recorded.fields.owner_read_status.state,
        OwnerServingState::ServingDisabled,
        "a writer told not to serve inspection recorded that decision"
    );
    assert_eq!(
        recorded.fields.owner_read_status.reason,
        Some(OwnerServingReason::DisabledByConfiguration),
        "the recorded state carries the reason a reader is owed"
    );

    let refusal =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .err()
            .expect("a store whose owner will not serve cannot be inspected");

    let Error::ReadFailure(failure) = &refusal else {
        panic!(
            "the recorded owner state is a typed read refusal, not an untyped error: {refusal:?}"
        );
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerNotServing,
        "a writer holding the store with no usable inspection channel is owner_not_serving: \
         {failure:?}"
    );
    assert!(
        matches!(failure.detail(), ReadFailureDetail::Reason { reason } if !reason.is_empty()),
        "the refusal carries the reason the writer recorded: {failure:?}"
    );

    database.close().expect("the writer closes cleanly");
}

#[test]
fn owner_status_returns_a_live_disabled_writer_s_recorded_state_instead_of_absence() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-state directory");
    let runtime_root = secure_runtime_root(directory.path(), "disabled-status-runtime");
    let (database, path) = writer_that_will_not_serve(directory.path(), runtime_root.clone());

    let report = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, ReadSessionOptions::default())
    })
    .expect("a live disabled owner has a recorded status to report");
    assert_eq!(report.status, database.owner_read_status());
    assert_eq!(report.status.state, OwnerServingState::ServingDisabled);
    assert!(
        report.serving.is_none(),
        "a disabled owner has no active serving limits to report"
    );

    database.close().expect("the writer closes cleanly");
}

/// The control: a store nobody owns is not an unserved owner. It reads from
/// the committed file exactly as it does today.
#[test]
fn a_store_nobody_owns_still_reads_from_the_committed_file() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-state directory");
    let runtime_root = secure_runtime_root(directory.path(), "unowned-runtime");
    let (database, path) = writer_that_will_not_serve(directory.path(), runtime_root.clone());
    database.close().expect("the writer releases the store");

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("an unowned store is readable");
    assert_eq!(session.route(), ReadRoute::File);
    let answer = session
        .execute("SELECT label FROM held ORDER BY id", &HashMap::new())
        .expect("the committed file answers");
    assert_eq!(answer.rows.len(), 1, "the seeded row is still there");
}
