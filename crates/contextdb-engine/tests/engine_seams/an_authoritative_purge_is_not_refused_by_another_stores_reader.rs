//! An authoritative purge on one store is not refused because a reader of
//! another store, owned by the same user, is joining or leaving the shared
//! runtime directory at that moment. That reader holds the runtime
//! directory's lock briefly and says nothing about this store's readers; the
//! purge proves its own readers through its own store's admission, as the
//! reader audit already does under the same contention.
//!
//! The lock is held by a planted holder on its own descriptor for the whole
//! purge, exactly as that other reader would hold it; the purge neither waits
//! for it nor refuses. A reader of the purged store itself is different: while
//! it is inside its own join, holding this store's reader admission, the purge
//! still refuses, so only another store's contention is set aside.

use contextdb_core::read_contract::{OwnerReadCancellation, ReadRoute};
use contextdb_core::{Error, Value};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderPausePoint, pause_readers_at_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The paused-reader wait takes a bound; this one is never what ends it: a
/// reader that never arrives fails the test at the runner's terminate limit.
const NEVER_REACHED: Duration = Duration::from_secs(60 * 60);

fn secure_runtime_directory(parent: &Path) -> PathBuf {
    let directory = parent.join("runtime");
    std::fs::create_dir(&directory).expect("create the shared runtime directory");
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
        .expect("secure the shared runtime directory");
    std::fs::canonicalize(&directory).expect("resolve the shared runtime directory")
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn an_authoritative_purge_succeeds_while_another_stores_reader_holds_the_shared_runtime_directory()
{
    let directory = tempfile::tempdir().expect("scratch directory");
    let runtime = secure_runtime_directory(directory.path());
    let path = directory.path().join("a.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open store A");
    database
        .execute(
            "CREATE TABLE erasable (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 1..=2i64 {
        database
            .execute(
                "INSERT INTO erasable (id, body) VALUES ($id, 'personal data')",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("commit a row");
    }

    // A reader of another store joins or leaves the shared runtime directory:
    // it holds the directory's lock on its own descriptor.
    let other_store_reader = std::fs::File::open(&runtime).expect("open the runtime directory");
    fs2::FileExt::try_lock_exclusive(&other_store_reader)
        .expect("the other store's reader holds the runtime directory's lock");

    let purged = database.execute("PURGE FROM erasable WHERE id = 1", &HashMap::new());
    let remaining = database
        .execute("SELECT id FROM erasable ORDER BY id", &HashMap::new())
        .expect("read the surviving rows")
        .rows()
        .to_vec();
    drop(other_store_reader);

    purged.unwrap_or_else(|refusal| {
        panic!(
            "an authoritative purge on this store must not be refused because another store's \
             reader holds the shared runtime directory: {refusal}"
        )
    });
    assert_eq!(remaining, vec![vec![Value::Int64(2)]]);

    // Control: a reader of this store is inside its own join, holding this
    // store's reader admission. The purge still refuses.
    let session =
        ReadSession::with_runtime_directory_for_test(&runtime, || ReadSession::open(&path))
            .expect("open a session beside the writer, through the store's runtime directory");
    assert_eq!(session.route(), ReadRoute::Owner);
    let joining = pause_readers_at_for_test(&path, ReaderPausePoint::BeforeRegistration);
    // The joining reader is ended by its own cancellation once it is let go,
    // so nothing here waits for a timeout.
    let cancellation = OwnerReadCancellation::new();
    let refused = std::thread::scope(|scope| {
        let reading = &session;
        let cancelling = &cancellation;
        let cursor = scope.spawn(move || {
            reading
                .open_cursor_with_cancellation(
                    "SELECT id FROM erasable ORDER BY id",
                    &HashMap::new(),
                    cancelling,
                )
                .map(|cursor| cursor.first_page().rows().to_vec())
                .map_err(|refusal| refusal.to_string())
        });
        let ticket = joining
            .wait_for_paused_reader(NEVER_REACHED)
            .expect("a reader of this store is inside its join");
        let refused = database.execute("PURGE FROM erasable WHERE id = 2", &HashMap::new());
        cancellation.cancel();
        joining.release(ticket);
        let _ = cursor.join().expect("reader thread");
        refused
    });
    assert!(
        matches!(refused, Err(Error::ReaderCoordinationUnavailable { .. })),
        "while a reader of this store is inside its own join the purge still refuses: {refused:?}"
    );
    drop(joining);
    drop(session);
    database.close().expect("close store A");
}
