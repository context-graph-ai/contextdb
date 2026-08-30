//! A read that dies inside the owner still answers the reader.
//!
//! A reader on the owner route is blocked on one thing: the reply slot for
//! the request it sent. A worker that unwinds without filling that slot
//! leaves the reader waiting for a reply nobody will ever write, and the
//! connection waiting for a wake that will never come. Nothing is logged for
//! the reader to see and nothing fails; the read simply stops answering until
//! its deadline expires, and every later statement on that connection is
//! stuck behind it.
//!
//! One panic anywhere inside a read does this -- in a plugin, a trigger
//! callback, an observer. So the unwind is caught where the reply is owed and
//! answered as the engine failure it is, carrying what the panic said, and
//! the connection is left able to serve the next statement.
//!
//! Nothing here waits on a clock: the pin is the ANSWER, not how long it took.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{
    OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState, ReadClientTimeouts, ReadLimits,
    ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::plugin::DatabasePlugin;
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

/// A statement carrying this sentinel is the one the plugin dies on. It is a
/// literal rather than a table name, so seeding the fixture -- which names
/// the same table -- does not trip it.
const DOOMED_SENTINEL: &str = "424242";
/// The table both statements below read from.
const TABLE: &str = "rows_under_test";
/// What the panic says, so the answer can be checked for carrying it.
const PANIC_REASON: &str = "the plugin refused to survive this statement";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// A plugin that unwinds while the owner is serving one particular read.
/// `on_query` is the engine's own example of somewhere a panic can reach a
/// read from, so this is the ordinary door rather than a special one.
#[derive(Default)]
struct PanicOnDoomedQuery;

impl DatabasePlugin for PanicOnDoomedQuery {
    fn on_query(&self, sql: &str) -> contextdb_core::Result<()> {
        assert!(!sql.contains(DOOMED_SENTINEL), "{PANIC_REASON}");
        Ok(())
    }
}

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime root");
    root
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 10_000,
        result_bytes: 64 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 600_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// A live writable owner serving readers, with a plugin that will unwind on
/// one named statement.
fn live_owner(path: &Path, runtime_dir: std::path::PathBuf) -> Database {
    let options = DatabaseOpenOptions {
        plugin: Arc::new(PanicOnDoomedQuery),
        owner_reads: OwnerReadConfig {
            limits: OwnerReadLimits {
                limits: roomy_limits(),
                concurrency: 4,
            },
            timeouts: OwnerServiceTimeouts {
                request_ms: 60_000,
                shutdown_drain_ms: 10_000,
            },
            runtime_dir: Some(runtime_dir),
            handler: None,
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    };
    let owner = Database::open_with_options(path, options).expect("start the writable owner");
    assert_eq!(
        owner.owner_read_status().state,
        OwnerServingState::Serving,
        "the owner must be serving before a reader dials it"
    );
    owner
}

fn seed(owner: &Database) {
    owner
        .execute(
            &format!("CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY)"),
            &empty(),
        )
        .expect("create the table both statements read from");
    owner
        .execute(&format!("INSERT INTO {TABLE} (id) VALUES (7)"), &empty())
        .expect("give the surviving statement something to answer with");
}

/// How many requests the owner has taken off its connections so far.
fn received(owner: &Database) -> u64 {
    owner
        .owner_received_request_count_for_test()
        .expect("a live owner counts the requests it takes off its connections")
}

#[test]
fn a_read_that_panics_in_the_owner_is_answered_and_leaves_the_connection_usable() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-panic directory");
    let path = directory.path().join("panicking.db");
    let runtime_root = secure_runtime_root(&directory, "panic-runtime");
    let owner = live_owner(&path, runtime_root.clone());
    seed(&owner);

    let reader = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                limits: roomy_limits(),
                timeouts: ReadClientTimeouts::default(),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("a live owner selects the owner route");
    assert_eq!(reader.route(), ReadRoute::Owner);

    // The panic happens inside the owner while it is serving this statement.
    // The reader is not told to wait: it is told what went wrong.
    let before = received(&owner);
    let answered = reader.execute(
        &format!("SELECT id FROM {TABLE} WHERE id = {DOOMED_SENTINEL}"),
        &empty(),
    );
    let failure = match answered {
        Err(error) => error,
        Ok(result) => panic!(
            "a read whose owner died mid-flight is answered with a failure, not with {} rows",
            result.rows.len()
        ),
    };
    let rendered = failure.to_string();
    assert!(
        rendered.contains(PANIC_REASON),
        "the answer carries what the panic said, so the operator can find it: {rendered}"
    );
    assert!(
        matches!(failure, Error::Other(_)),
        "a read that ended unexpectedly is an engine failure, not a refusal a caller could have \
         avoided: {failure:?}"
    );

    // The connection is not spoiled: the very next statement on the SAME
    // session is served.
    let next = reader
        .execute(&format!("SELECT id FROM {TABLE}"), &empty())
        .expect("the same session's next statement is served on the same connection");
    assert_eq!(
        next.rows.len(),
        1,
        "and it answers with the row it was asked for"
    );

    // Both statements reached the owner: the failed one was served and
    // answered, not dropped before it arrived.
    assert_eq!(
        received(&owner) - before,
        2,
        "the owner took both statements off its connections -- the one that died and the one \
         after it"
    );

    owner.close().expect("the owner closes cleanly");
}
