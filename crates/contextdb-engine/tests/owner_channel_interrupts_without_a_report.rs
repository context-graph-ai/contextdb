//! Stopping a read that is running inside another process, whether or not it
//! has said anything.
//!
//! Cancelling over the channel used to be able to travel only alongside
//! something the owner sent back, which made "can I stop this read" depend on
//! whether that read happened to be the reporting kind. A read that reports
//! nothing is exactly the read a caller most wants to be able to stop, so the
//! interrupt travels on the cancellation itself: the token tells a listener,
//! and the listener writes the interrupt on a second handle onto the same
//! carrier while the reading thread is still blocked on the first.

#![cfg(feature = "test-seams")]

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadProgress, ReadProgressObserver,
    ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const FIXTURE_ROWS: i64 = 4_000;
const NEEDLES: [i64; 2] = [200, 2_500];

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create task-scoped owner runtime root");
    #[cfg(unix)]
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

fn live_owner(path: &Path, runtime_dir: std::path::PathBuf) -> Database {
    let options = DatabaseOpenOptions {
        owner_reads: OwnerReadConfig {
            limits: OwnerReadLimits {
                limits: roomy_limits(),
                concurrency: 8,
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

fn seed(database: &Database) {
    database
        .execute(
            "CREATE TABLE interrupt_rows (id INTEGER PRIMARY KEY, marker TEXT, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    let mut next = 0;
    while next < FIXTURE_ROWS {
        let tx = database.begin().expect("begin a fixture batch");
        let last = (next + 500).min(FIXTURE_ROWS);
        while next < last {
            let marker = if NEEDLES.contains(&next) {
                "needle"
            } else {
                "hay"
            };
            database
                .execute_in_tx(
                    tx,
                    "INSERT INTO interrupt_rows (id, marker, payload) VALUES ($id, $marker, $payload)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(next)),
                        ("marker".to_owned(), Value::Text(marker.to_owned())),
                        ("payload".to_owned(), Value::Text(format!("row-{next}"))),
                    ]),
                )
                .unwrap_or_else(|error| panic!("insert fixture row {next}: {error}"));
            next += 1;
        }
        database.commit(tx).expect("commit a fixture batch");
    }
}

fn one_row_page_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            cursor_page_rows: 1,
            ..roomy_limits()
        },
        timeouts: ReadClientTimeouts::default(),
        ..ReadSessionOptions::default()
    }
}

fn owner_active_readers(path: &Path, runtime_root: &Path) -> u64 {
    let report = ReadSession::with_runtime_directory_for_test(runtime_root, || {
        ReadSession::owner_report(path, ReadSessionOptions::default())
    })
    .expect("the owner answers a status report over its channel");
    report
        .serving
        .expect("a serving owner's report carries its serving section")
        .admission
        .active_readers
}

/// An observer that records what it heard and cancels once armed.
struct CancelWhenArmed {
    armed: Mutex<Option<OwnerReadCancellation>>,
    heard: Mutex<Vec<ReadProgress>>,
}

impl CancelWhenArmed {
    fn new() -> Self {
        Self {
            armed: Mutex::new(None),
            heard: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self, cancellation: OwnerReadCancellation) {
        *self
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(cancellation);
    }

    fn heard_while_armed(&self) -> usize {
        self.heard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

impl ReadProgressObserver for CancelWhenArmed {
    fn progress(&self, progress: ReadProgress) {
        let armed = self
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cancellation) = armed.as_ref() {
            self.heard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(progress);
            cancellation.cancel();
        }
    }
}

#[test]
fn a_statement_cancelled_before_it_is_sent_is_refused_without_reaching_the_owner() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = directory.path().join("pre-cancelled.db");
    let runtime_root = secure_runtime_root(&directory, "pre-cancelled-runtime");
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

    // Nobody is watching this read, so nothing it does can carry the news
    // that it should stop. The cancellation carries it itself, and it is
    // already carrying it before the reader would send anything.
    let cancellation = OwnerReadCancellation::new();
    cancellation.cancel();
    let received_before = owner
        .owner_received_request_count_for_test()
        .expect("a live owner counts the requests it takes off its connections");
    let refused = reader.execute_with_cancellation(
        "SELECT id FROM interrupt_rows ORDER BY id",
        &HashMap::new(),
        &cancellation,
    );
    let received_after = owner
        .owner_received_request_count_for_test()
        .expect("a live owner counts the requests it takes off its connections");
    assert!(
        matches!(refused, Err(Error::ReadCancelled)),
        "a read cancelled before it was sent is refused as cancelled: {refused:?}"
    );

    // The other half of the same promise: the owner never saw it. The count
    // is taken immediately either side of the cancelled statement and nothing
    // else touches the channel in between, because every accepted frame is
    // counted -- a status probe would move it just as a query would.
    assert_eq!(
        received_after, received_before,
        "a statement cancelled before it is sent never reaches the owner, so the owner's count \
         of the requests it has taken off its connections does not move"
    );

    assert_eq!(
        owner_active_readers(&path, &runtime_root),
        0,
        "the owner's in-flight reader count returns to zero once the cancelled read unwinds"
    );

    // The control for the count above: the same statement, not cancelled,
    // does move it. Without this, a count that never moves at all would read
    // as proof that the cancelled statement stayed home.
    let before_served = owner
        .owner_received_request_count_for_test()
        .expect("a live owner counts the requests it takes off its connections");
    let served = reader
        .execute("SELECT id FROM interrupt_rows ORDER BY id", &HashMap::new())
        .expect("the same statement, uncancelled, is served");
    assert_eq!(served.rows.len(), FIXTURE_ROWS as usize);
    assert!(
        owner
            .owner_received_request_count_for_test()
            .expect("a live owner counts the requests it takes off its connections")
            > before_served,
        "the owner's received-request count moves for a statement that does reach it, so a count \
         that did not move above is a statement that never arrived"
    );

    let next = reader
        .execute(
            "SELECT id FROM interrupt_rows WHERE id = 0",
            &HashMap::new(),
        )
        .expect("the same session's next statement succeeds on the same connection");
    assert_eq!(next.rows.len(), 1);
}

#[test]
fn a_cursor_fetch_that_walks_the_store_reports_and_can_be_stopped_from_inside_the_observer() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = directory.path().join("fetch-cancel.db");
    let runtime_root = secure_runtime_root(&directory, "fetch-cancel-runtime");
    let owner = live_owner(&path, runtime_root.clone());
    seed(&owner);

    let observer = Arc::new(CancelWhenArmed::new());
    let reader = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_progress(
            &path,
            one_row_page_options(),
            Arc::clone(&observer) as Arc<dyn ReadProgressObserver>,
        )
    })
    .expect("a live owner selects the owner route");

    // No ordering, so the read streams: the first page stops at the first
    // needle and the walk to the second is done by the FETCH, which is the
    // call under test. (Ordering the same statement makes the plan collect
    // and sort every matching row while the cursor is being opened, leaving
    // the fetch nothing to do and nothing to report.)
    let mut cursor = reader
        .open_cursor(
            "SELECT id FROM interrupt_rows WHERE marker = $marker",
            &HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))]),
        )
        .expect("open a cursor over the owner channel");

    // An open cursor is work the owner is holding for this reader, so it
    // occupies a reader slot for as long as it stays open. Recorded here so
    // the count after the cancelled fetch below is read against what the
    // cursor itself accounts for, not against zero.
    let held_by_the_open_cursor = owner_active_readers(&path, &runtime_root);

    let cancellation = OwnerReadCancellation::new();
    observer.arm(cancellation.clone());
    let refused = cursor.fetch_with_cancellation(None, &cancellation);
    assert!(
        matches!(refused, Err(Error::ReadCancelled)),
        "cancelling a cursor fetch from inside the observer, over the owner channel, yields \
         ReadCancelled: {refused:?}"
    );
    assert!(
        observer.heard_while_armed() >= 1,
        "the fetch told the caller what it was doing before the caller stopped it"
    );

    assert_eq!(
        owner_active_readers(&path, &runtime_root),
        held_by_the_open_cursor,
        "a cancelled fetch leaves the owner holding exactly what the still-open cursor held          before it -- it neither strands the fetch's own work nor releases the cursor"
    );

    let page = cursor
        .fetch(None)
        .expect("the cursor survives the cancelled fetch and answers the next one");
    assert!(
        !page.rows.is_empty() || !page.has_more,
        "the cursor resumes cleanly after the cancelled fetch: {page:?}"
    );
    cursor.close().expect("close the cursor");
    assert_eq!(
        owner_active_readers(&path, &runtime_root),
        0,
        "closing the cursor gives the owner back everything this reader held"
    );
}
