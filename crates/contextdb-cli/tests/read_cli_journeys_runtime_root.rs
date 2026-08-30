//! Journey: writer and reader share one operator-supplied runtime root.
//!
//! Contract held here: a container, a packaged service, or a Home Assistant
//! add-on has no `XDG_RUNTIME_DIR`, so it states where the local read channel
//! lives with `--owner-read-runtime-dir`. That one validated root has to serve
//! BOTH sides — the writer that creates the channel and the reading session
//! that dials it. A root the writer honors and the reader ignores means the
//! packaged deployment can start a writer nobody can ever inspect.
//!
//! The owner here is a genuinely separate process, because a runtime root is
//! only shared when two processes actually resolve it.

mod read_cli_support;

use read_cli_support::*;

/// A directory an operator supplies, owner-only, outside any runtime default.
fn supplied_runtime_root(store: &Store) -> std::path::PathBuf {
    let root = store.folder().join("supplied-runtime");
    std::fs::create_dir(&root).expect("create the operator-supplied runtime root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure the operator-supplied runtime root");
    }
    root
}

#[test]
fn a_reader_given_the_writer_s_runtime_root_reaches_the_owner() {
    let store = store_with(&create_seeded_table("shared", 4));
    let root = supplied_runtime_root(&store);
    let root_str = root.to_str().expect("utf-8 runtime root");
    let _owner = live_owner_with(&store.path, &["--owner-read-runtime-dir", root_str], "");

    let outcome = run(
        &[
            store.path_str(),
            "--owner-read-runtime-dir",
            root_str,
            "--json",
        ],
        "SELECT id FROM shared ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "a reader told where the channel lives reads through it.\n{}",
        outcome.describe()
    );
    let notices = outcome.route_notices();
    let route = notices
        .first()
        .and_then(|notice| notice.get("detail"))
        .and_then(|detail| detail.get("route"))
        .and_then(|route| route.as_str());
    assert_eq!(
        route,
        Some("owner"),
        "the supplied root is where the reader finds the live owner.\n{}",
        outcome.describe()
    );
}

/// The control: the same store, the same supplied root, and no writer. The
/// session reads the committed file, which is what a root with no channel in
/// it should mean.
#[test]
fn the_supplied_root_still_reads_the_file_when_nobody_owns_the_store() {
    let store = store_with(&create_seeded_table("shared", 4));
    let root = supplied_runtime_root(&store);
    let root_str = root.to_str().expect("utf-8 runtime root");

    let outcome = run(
        &[
            store.path_str(),
            "--owner-read-runtime-dir",
            root_str,
            "--json",
        ],
        "SELECT id FROM shared ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "an unowned store reads from its committed file.\n{}",
        outcome.describe()
    );
    let notices = outcome.route_notices();
    let route = notices
        .first()
        .and_then(|notice| notice.get("detail"))
        .and_then(|detail| detail.get("route"))
        .and_then(|route| route.as_str());
    assert_eq!(
        route,
        Some("file"),
        "no channel in the supplied root means the file answers.\n{}",
        outcome.describe()
    );
}
