//! What an owner says about how it is serving, in full.
//!
//! "Is anyone there" is the smallest useful answer and rarely the one an
//! operator needs. The next questions are always the same: which ceilings are
//! actually in force, and are they the shipped defaults or something this
//! deployment chose; how long will it wait; how many readers can it take and
//! how many has it got; what memory is it holding. The owner computes all of
//! that to answer a status request at all, so a caller gets it whole.
//!
//! Where there is no owner serving, there is nothing below the word to
//! report -- and reporting zeroes would read as an owner that allows nothing,
//! which is a different and false statement.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{OwnerReadLimits, OwnerServingState, ReadLimits, ReadRoute};
use contextdb_core::{Error, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerConfigurationSource, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// A ceiling this deployment deliberately narrowed, so the report has
/// something to call an override rather than a default. It stays above the
/// standing page ceiling, because a whole answer that cannot hold one page is
/// an incoherent declaration and the owner refuses to start on it.
const CHOSEN_RESULT_ROWS: u64 = 137;
const CHOSEN_CONCURRENCY: u64 = 3;

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
            "CREATE TABLE report_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the report fixture table");
    database
        .execute(
            "INSERT INTO report_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("payload".to_owned(), Value::Text("kept".to_owned())),
            ]),
        )
        .expect("insert the report fixture row");
}

fn served_store(directory: &Path, runtime_root: PathBuf) -> (Database, PathBuf) {
    let path = directory.join("reported.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root),
                limits: OwnerReadLimits {
                    limits: ReadLimits {
                        result_rows: CHOSEN_RESULT_ROWS,
                        ..ReadLimits::default()
                    },
                    concurrency: CHOSEN_CONCURRENCY,
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads");
    seed(&database);
    (database, path)
}

#[test]
fn a_serving_owner_reports_the_limits_and_capacity_it_is_actually_running_with() {
    let directory = tempfile::TempDir::new().expect("task-scoped report directory");
    let runtime_root = secure_runtime_root(directory.path(), "serving-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let report = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, ReadSessionOptions::default())
    })
    .expect("the owner answers a status request");

    assert_eq!(report.status.state, OwnerServingState::Serving);
    let serving = report
        .serving
        .as_ref()
        .expect("a serving owner has something to report below the word");

    // The ceiling this deployment chose comes back as chosen, with the value
    // it was given -- an operator reading a surprising limit needs to know
    // whether it is theirs to change.
    assert_eq!(
        serving.effective_limits.result_rows.value,
        CHOSEN_RESULT_ROWS
    );
    assert_eq!(
        serving.effective_limits.result_rows.source,
        OwnerConfigurationSource::Override,
        "a limit this deployment set is reported as set, not as a default"
    );
    assert_eq!(
        serving.effective_limits.concurrency.value,
        CHOSEN_CONCURRENCY
    );

    // A ceiling nobody touched comes back as the shipped default, with the
    // shipped value -- even though this deployment did choose a different
    // ceiling beside it. A source describes its own setting, so an operator
    // reading a surprising limit can tell which one is theirs to change.
    assert_eq!(
        serving.effective_limits.result_bytes.value,
        ReadLimits::default().result_bytes
    );
    assert_eq!(
        serving.effective_limits.result_bytes.source,
        OwnerConfigurationSource::Default,
        "a limit nobody set is reported as a default, even next to one that was set"
    );
    for (name, reported) in [
        ("memory", serving.effective_limits.memory),
        ("work", serving.effective_limits.work),
        ("active_ms", serving.effective_limits.active_ms),
        (
            "cursor_page_rows",
            serving.effective_limits.cursor_page_rows,
        ),
        ("cursor_idle_ms", serving.effective_limits.cursor_idle_ms),
        (
            "cursor_lifetime_ms",
            serving.effective_limits.cursor_lifetime_ms,
        ),
    ] {
        assert_eq!(
            reported.source,
            OwnerConfigurationSource::Default,
            "{name} was never set by this deployment, so it is a default: {reported:?}"
        );
    }
    assert_eq!(
        serving.effective_limits.memory.value,
        ReadLimits::default().memory
    );
    assert_eq!(
        serving.effective_limits.cursor_lifetime_ms.value,
        ReadLimits::default().cursor_lifetime_ms
    );

    // How long it waits, and how many readers it can take against how many it
    // has -- the two numbers that answer "will it serve me".
    assert!(
        serving.timeouts.request_ms.value > 0,
        "an owner that waits no time at all could serve nobody: {:?}",
        serving.timeouts
    );
    assert_eq!(serving.admission.capacity, CHOSEN_CONCURRENCY);
    assert!(
        serving.admission.active_readers <= serving.admission.capacity,
        "an owner never reports serving more readers than it can take: {:?}",
        serving.admission
    );

    // Memory is what the owner holds; a ceiling it does not declare is
    // reported as absent rather than as nothing left.
    if let Some(available) = serving.memory.available_bytes {
        assert!(
            available <= serving.effective_limits.memory.value,
            "available memory is measured against the ceiling in force: {:?}",
            serving.memory
        );
    }

    // The short answer is still the short answer.
    let status = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_status(&path, ReadSessionOptions::default())
    })
    .expect("the owner answers a status request");
    assert_eq!(status, report.status, "both doors report the same state");

    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_repointed_alias_does_not_report_the_old_store_owner() {
    let directory = tempfile::TempDir::new().expect("task-scoped report-alias directory");
    let runtime_root = secure_runtime_root(directory.path(), "report-alias-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    std::fs::remove_file(&path).expect("unlink the old store pathname");
    let missing_target = directory
        .path()
        .join("parent-that-does-not-exist")
        .join("missing-target.db");
    std::os::unix::fs::symlink(&missing_target, &path)
        .expect("repoint the old name to a missing target");

    let report = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, ReadSessionOptions::default())
    });
    assert!(
        matches!(
            report,
            Err(Error::ReadFailure(ref failure))
                if failure.kind()
                    == contextdb_core::read_contract::ReadFailureKind::OwnerNotRunning
        ),
        "the old owner no longer owns the missing target named by the repointed alias: {report:?}"
    );
    assert!(
        !missing_target
            .parent()
            .expect("missing target has a parent")
            .exists(),
        "owner report must not create the missing target parent"
    );

    database
        .close()
        .expect("close the owner of the unlinked old store");
}

#[test]
fn an_owner_serving_a_reader_says_so_in_the_count_it_reports() {
    let directory = tempfile::TempDir::new().expect("task-scoped report directory");
    let runtime_root = secure_runtime_root(directory.path(), "active-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let idle = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, ReadSessionOptions::default())
    })
    .expect("the owner answers")
    .serving
    .expect("a serving owner reports its admission")
    .admission;

    // A reader holding a cursor is a reader the owner is serving.
    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);
    let cursor = session
        .open_cursor("SELECT payload FROM report_rows", &HashMap::new())
        .expect("open a cursor against the owner");

    let busy = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::owner_report(&path, ReadSessionOptions::default())
    })
    .expect("the owner answers while it is busy")
    .serving
    .expect("a serving owner reports its admission")
    .admission;

    assert_eq!(
        busy.capacity, idle.capacity,
        "capacity is what the owner was configured with, not what it is using"
    );
    assert!(
        busy.active_readers >= idle.active_readers,
        "an owner serving a reader does not report fewer than when it served none: \
         idle {idle:?}, busy {busy:?}"
    );

    cursor.close().expect("close the cursor");
    drop(session);
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_store_nobody_owns_has_no_owner_report_to_give() {
    let directory = tempfile::TempDir::new().expect("task-scoped report directory");
    let path = directory.path().join("unowned.db");
    let database = Database::open(&path).expect("open the report fixture writer");
    seed(&database);
    database.close().expect("the fixture writer closes cleanly");

    // Nobody is serving, so there are no limits in force to describe. The
    // answer says that rather than describing an owner that is not there.
    let refused = ReadSession::owner_report(&path, ReadSessionOptions::default());
    assert!(
        matches!(refused, Err(Error::ReadFailure(_))),
        "a store with no owner has no owner to report on: {refused:?}"
    );

    // And the store is still perfectly readable, which is the point of
    // knowing there is no owner.
    let session = ReadSession::open(&path).expect("the idle store is readable");
    assert_eq!(session.route(), ReadRoute::File);
    assert_eq!(
        session
            .execute("SELECT payload FROM report_rows", &HashMap::new())
            .expect("the idle file answers")
            .rows
            .len(),
        1
    );
}
