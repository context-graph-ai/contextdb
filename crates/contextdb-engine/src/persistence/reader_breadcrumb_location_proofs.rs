use super::*;

/// A runtime directory an operator supplied, owner-only, outside any
/// platform default.
fn supplied_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the operator-supplied runtime root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure the operator-supplied runtime root");
    }
    root
}

/// The supplied root is the state directory for the channel and the
/// reader registry alike.
#[test]
fn a_supplied_runtime_root_moves_the_breadcrumb_location() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");

    let where_readers_go = reader_breadcrumb_directory(&store, &root)
        .expect("a supplied root derives the reader registry directory");

    assert!(
        where_readers_go.starts_with(&root),
        "readers are written down under the supplied runtime root"
    );
    if let Some(default_runtime) = reader_breadcrumb_runtime_directory() {
        assert_ne!(
            where_readers_go,
            reader_breadcrumb_directory(&store, default_runtime.path()).unwrap()
        );
    }
}

/// The writer scans the same root its direct readers publish into.
#[test]
fn a_writer_reads_the_breadcrumbs_of_a_supplied_runtime_root() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");
    let breadcrumb = current_reader_breadcrumb().expect("identify this reader process");
    let guard = ReaderBreadcrumbGuard::create(&store, &root, Some(&breadcrumb))
        .expect("publish a locked reader breadcrumb inside the supplied root");
    let (observed, verified) = live_reader_holders(&store, Some(&root));
    assert_eq!(observed, 1);
    assert_eq!(verified.len(), 1);
    drop(guard);
    assert!(root.is_dir(), "reader teardown preserves the supplied root");
}

/// A real store nobody is holding, released at once.
fn idle_store(directory: &Path, name: &str) -> PathBuf {
    let store = directory.join(name);
    drop(RedbPersistence::create(&store).expect("claim and release a real store"));
    store
}

/// The wait answers immediately when nobody holds the store. The verdict
/// comes from the readers themselves, so a deployment's runtime directory
/// neither supplies it nor withholds it.
#[test]
fn a_store_nobody_holds_is_released_at_once() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = idle_store(directory.path(), "packaged.db");
    let resolved = RuntimeDirectory::supplied(&root);
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release(&store, Some(&resolved), &stop),
            ReaderReleaseWait::Released
        ),
        "no reader is holding this store, so there is nothing to wait for"
    );
}

/// A pathname that is not a store has no companion beside it, so there is
/// nowhere for a reader of it to record a hold and nothing this wait can
/// observe. A caller about to take it is told that, never `Released`.
#[test]
fn a_pathname_with_no_companion_is_unobservable_not_released() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("not-a-store.db");
    std::fs::write(&store, b"").expect("plant a pathname that is not a store");
    let resolved = RuntimeDirectory::supplied(&root);
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release(&store, Some(&resolved), &stop),
            ReaderReleaseWait::Unobservable(_)
        ),
        "a pathname with no coordination artifact beside it is not a store with no readers"
    );
}

/// The door a packaged consumer can actually reach: it holds the runtime
/// ROOT its deployment named, not a directory this crate resolved, and it
/// still reaches the wait below it and answers for the store.
#[test]
fn a_supplied_root_still_reaches_the_wait_below_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = idle_store(directory.path(), "packaged.db");
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release_in_runtime_dir(&store, Some(&root), &stop),
            ReaderReleaseWait::Released
        ),
        "the root-taking door resolves what it was given and answers for the store"
    );
}

/// A root this process cannot read says nothing about this store's
/// readers. Where the owner's channel lives is a different question, and a
/// caller waiting on a store nobody is reading is answered from the store.
#[test]
fn a_root_that_cannot_be_read_does_not_decide_the_answer() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let store = idle_store(directory.path(), "packaged.db");
    let absent = directory.path().join("no-such-runtime-directory");
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release_in_runtime_dir(&store, Some(&absent), &stop),
            ReaderReleaseWait::Released
        ),
        "nobody is holding this store, and an unreadable channel root is not a reason to \
             refuse to say so"
    );
}

/// Without a supplied root the new door is the old one: the platform's own
/// runtime location, same answer.
#[test]
fn no_supplied_root_is_the_ordinary_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");
    let stop = OwnerReadCancellation::default();
    stop.cancel();

    assert!(
        matches!(
            wait_for_reader_release_in_runtime_dir(&store, None, &stop),
            ReaderReleaseWait::Stopped
        ),
        "the root-taking door carries the same stop contract as the one it delegates to"
    );
}

/// A caller that has already stopped is answered at once, with no wait.
#[test]
fn a_stopped_wait_answers_without_waiting() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");
    let resolved = RuntimeDirectory::supplied(&root);
    let stop = OwnerReadCancellation::default();
    stop.cancel();

    assert!(
        matches!(
            wait_for_reader_release(&store, Some(&resolved), &stop),
            ReaderReleaseWait::Stopped
        ),
        "an interrupt is as prompt as a release"
    );
}
