use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerReadStatus, OwnerServingReason, OwnerServingState,
    ReadFailureDetail, ReadFailureKind, ReadLimits,
};
use contextdb_core::{Error, Result};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::read_session::ReadRouteResourceSnapshot;
use contextdb_engine::{
    Database, DatabaseOpenOptions, DatabasePlugin, OwnerReadConfig, OwnerReadTestHooks,
    OwnerRequestHandler, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create task-scoped owner runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime root");
    root
}

fn draining_status() -> OwnerReadStatus {
    OwnerReadStatus {
        state: OwnerServingState::NotServing,
        reason: Some(OwnerServingReason::ShutdownDraining),
    }
}

fn assert_bound_draining_listener(path: &std::path::Path, runtime_root: &std::path::Path) {
    assert_eq!(
        ReadSession::with_runtime_directory_for_test(runtime_root, || {
            ReadSession::probe_owner_status_for_test(path, ReadSessionOptions::default())
        })
        .expect("status request reaches the still-owned listener"),
        draining_status(),
    );
}

fn assert_owner_listener_released(path: &std::path::Path, runtime_root: &std::path::Path) {
    assert!(matches!(
        ReadSession::with_runtime_directory_for_test(runtime_root, || {
            ReadSession::probe_owner_status_for_test(path, ReadSessionOptions::default())
        }),
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::OwnerNotRunning
    ));
}

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
    cancellation_observed: bool,
}

#[derive(Clone, Default)]
struct GatedHandler {
    gate: Arc<(Mutex<GateState>, Condvar)>,
}

impl GatedHandler {
    fn wait_until_entered(&self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().expect("handler gate lock");
        while !state.entered {
            state = changed.wait(state).expect("handler gate wait");
        }
    }

    fn release(&self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().expect("handler gate lock");
        state.released = true;
        changed.notify_all();
    }

    fn cancellation_observed(&self) -> bool {
        self.gate
            .0
            .lock()
            .expect("handler gate lock")
            .cancellation_observed
    }
}

impl OwnerRequestHandler for GatedHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        if request != b"hold" {
            return Ok(request.to_vec());
        }
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().expect("handler gate lock");
        state.entered = true;
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).expect("handler gate wait");
        }
        state.cancellation_observed = cancellation.is_cancelled();
        changed.notify_all();
        Ok(request.to_vec())
    }
}

struct NonceHandler(Vec<u8>);

impl OwnerRequestHandler for NonceHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        let mut response = self.0.clone();
        response.extend_from_slice(request);
        Ok(response)
    }
}

#[derive(Default)]
struct PluginCounts {
    opened: AtomicUsize,
    closed: AtomicUsize,
    changed: Condvar,
    lock: Mutex<()>,
}

#[derive(Clone)]
struct CountingPlugin {
    counts: Arc<PluginCounts>,
}

impl CountingPlugin {
    fn new() -> Self {
        Self {
            counts: Arc::new(PluginCounts::default()),
        }
    }

    fn wait_until_closed(&self) {
        let mut guard = self.counts.lock.lock().expect("plugin count lock");
        while self.counts.closed.load(Ordering::SeqCst) == 0 {
            guard = self.counts.changed.wait(guard).expect("plugin close wait");
        }
    }
}

impl DatabasePlugin for CountingPlugin {
    fn on_open(&self) -> Result<()> {
        self.counts.opened.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_close(&self) -> Result<()> {
        self.counts.closed.fetch_add(1, Ordering::SeqCst);
        self.counts.changed.notify_all();
        Ok(())
    }
}

fn owner_options(
    plugin: CountingPlugin,
    handler: GatedHandler,
    clock: ManualDeadlineClock,
    drain_started: std::sync::mpsc::Sender<()>,
    runtime_dir: std::path::PathBuf,
) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        plugin: Arc::new(plugin),
        owner_reads: OwnerReadConfig {
            limits: OwnerReadLimits {
                limits: ReadLimits::default(),
                concurrency: 1,
            },
            timeouts: contextdb_core::read_contract::OwnerServiceTimeouts {
                request_ms: 60_000,
                shutdown_drain_ms: 10_000,
            },
            runtime_dir: Some(runtime_dir),
            handler: Some(Arc::new(handler)),
            test_hooks: Some(OwnerReadTestHooks {
                clock: Arc::new(clock),
                drain_started,
            }),
            ..OwnerReadConfig::default()
        },
        ..DatabaseOpenOptions::default()
    }
}

fn create_owner_database(
    handler: GatedHandler,
    plugin: CountingPlugin,
    clock: ManualDeadlineClock,
    drain_started: std::sync::mpsc::Sender<()>,
) -> (tempfile::TempDir, std::path::PathBuf, Arc<Database>) {
    let directory = tempfile::TempDir::new().expect("task-scoped owner database directory");
    let path = directory.path().join("owner-read-lifecycle.db");
    let runtime_root = secure_runtime_root(&directory, "owner-read-runtime");
    let database = Database::open_with_options(
        &path,
        owner_options(plugin, handler, clock, drain_started, runtime_root.clone()),
    )
    .expect("configured file owner opens before its read service is exercised");
    assert_eq!(
        database.owner_read_status().state,
        OwnerServingState::Serving
    );
    database
        .execute(
            "CREATE TABLE lifecycle_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the ordinary database fixture");
    database
        .execute(
            "INSERT INTO lifecycle_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), contextdb_core::Value::Int64(1)),
                (
                    "payload".to_owned(),
                    contextdb_core::Value::Text("ordinary operations survive draining".to_owned()),
                ),
            ]),
        )
        .expect("insert the ordinary database fixture");
    (directory, runtime_root, Arc::new(database))
}

const WRITER_PROBE: &str = "CONTEXTDB_OWNER_WRITER_PROBE";
const WRITER_PROBE_PATH: &str = "CONTEXTDB_OWNER_WRITER_PROBE_PATH";

#[test]
fn independent_writer_probe_reports_the_existing_owner() {
    if std::env::var_os(WRITER_PROBE).is_none() {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(WRITER_PROBE_PATH).expect("writer probe path"));
    // The process holding this store is the one that started this probe.
    let holder_pid = std::os::unix::process::parent_id();
    match Database::open(&path) {
        Ok(_) => panic!("a store its parent process holds must not open here"),
        Err(refusal) => assert_refused_as_held_by_writer(refusal, holder_pid, &path),
    }
}

/// A second open from ANOTHER process is refused by name: the store is held by
/// a writer, and the refusal says which one so an operator knows what to stop.
/// A second open from the SAME process is `DatabaseLocked` and is asserted as
/// such where it happens.
fn assert_refused_as_held_by_writer(refusal: Error, holder_pid: u32, path: &std::path::Path) {
    let Error::ReadFailure(failure) = &refusal else {
        panic!("a store another process holds reported {refusal:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::HeldByWriter);
    let ReadFailureDetail::HeldByWriter(detail) = failure.detail() else {
        panic!("a held-by-writer refusal carries the holder as a structured detail: {refusal:?}");
    };
    assert_eq!(detail.process_id, Some(u64::from(holder_pid)));
    assert!(
        failure.to_string().contains(&holder_pid.to_string()),
        "the refusal must name the holding process {holder_pid}: {failure}"
    );
    assert!(
        failure.to_string().contains(&path.display().to_string()),
        "the refusal must name the store it is about: {failure}"
    );
}

fn assert_child_writer_sees_existing_owner(path: &std::path::Path) {
    let output =
        Command::new(std::env::current_exe().expect("current integration-test executable"))
            .arg("--exact")
            .arg("independent_writer_probe_reports_the_existing_owner")
            .arg("--nocapture")
            .env(WRITER_PROBE, "1")
            .env(WRITER_PROBE_PATH, path)
            .output()
            .expect("run the independent writer probe");
    assert!(
        output.status.success(),
        "independent writer probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_database_resources_owned(snapshot: ReadRouteResourceSnapshot) {
    assert!(snapshot.channel_identity.is_some());
    assert!(snapshot.open_registry_owned);
    assert!(snapshot.persistence_owned);
    assert!(snapshot.blob_repository_owned);
    assert!(snapshot.plugin_open);
    assert!(snapshot.snapshot_registry_owned);
    assert!(snapshot.memory_accountant_owned);
}

#[test]
fn in_process_requests_bypass_saturated_owner_admission_without_changing_the_live_route() {
    let handler = GatedHandler::default();
    let directory = tempfile::TempDir::new().expect("task-scoped saturation path directory");
    let owner_path = directory.path().join("saturated-owner.db");
    let runtime_root = secure_runtime_root(&directory, "saturated-owner-runtime");
    let (drain_started, _drain_started_at) = std::sync::mpsc::channel();
    let database = Database::open_with_options(
        &owner_path,
        owner_options(
            CountingPlugin::new(),
            handler.clone(),
            ManualDeadlineClock::default(),
            drain_started,
            runtime_root.clone(),
        ),
    )
    .expect("open the one-slot owner used for admission proof");
    assert_eq!(
        database.owner_read_status().state,
        OwnerServingState::Serving
    );
    let external = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open(&owner_path)
    })
    .expect("select the owner route externally");
    let held = std::thread::spawn(move || external.request_owner("administrative", b"hold"));
    handler.wait_until_entered();
    let before_embedded = database
        .read_route_resources_for_test()
        .expect("capture route resources while the external slot is held");
    assert_eq!(before_embedded.owner_services_started, 1);
    assert_eq!(before_embedded.active_owner_slots, 1);

    let embedded = database
        .read_session(ReadLimits::default())
        .expect("create the in-process live view while owner capacity is full");
    let embedded_request = std::thread::spawn(move || {
        assert_eq!(
            embedded.route(),
            contextdb_core::read_contract::ReadRoute::Owner
        );
        embedded.request_owner("administrative", b"embedded")
    });
    assert_eq!(
        embedded_request
            .join()
            .expect("cross-thread embedded owner request joins")
            .expect("in-process request dispatches directly without an owner slot"),
        b"embedded".to_vec()
    );
    let after_embedded = database
        .read_route_resources_for_test()
        .expect("capture resources after direct in-process dispatch");
    assert_eq!(
        after_embedded.channel_identity,
        before_embedded.channel_identity
    );
    assert_eq!(
        after_embedded.owner_services_started,
        before_embedded.owner_services_started
    );
    assert_eq!(
        after_embedded.local_channel_operations,
        before_embedded.local_channel_operations
    );
    assert_eq!(
        after_embedded.direct_backend_opens,
        before_embedded.direct_backend_opens
    );
    assert_eq!(
        after_embedded.active_owner_slots,
        before_embedded.active_owner_slots
    );
    let refused = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open(&owner_path)
    })
    .expect("a separate external caller reaches the already-selected owner")
    .request_owner("administrative", b"second")
    .expect_err("the owner slot remains occupied after the in-process request");
    assert!(matches!(
        refused,
        Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::OwnerAtCapacity
    ));

    handler.release();
    held.join()
        .expect("held external request joins after release")
        .expect("held external request completes");
    database.close().expect("close the one-slot owner");
}

#[test]
fn drain_timeout_preserves_database_resources_until_a_retry_finishes_close() {
    let handler = GatedHandler::default();
    let plugin = CountingPlugin::new();
    let clock = ManualDeadlineClock::default();
    let (drain_started, drain_started_at) = std::sync::mpsc::channel();
    let (directory, runtime_root, database) = create_owner_database(
        handler.clone(),
        plugin.clone(),
        clock.clone(),
        drain_started,
    );
    let path = directory.path().join("owner-read-lifecycle.db");

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live file owner selects the owner route once");
    let request = std::thread::spawn(move || session.request_owner("administrative", b"hold"));
    handler.wait_until_entered();
    let owned_before_drain = database
        .read_route_resources_for_test()
        .expect("capture the owner channel before draining");
    assert_database_resources_owned(owned_before_drain);

    let closing_database = Arc::clone(&database);
    let close = std::thread::spawn(move || closing_database.close());
    drain_started_at
        .recv()
        .expect("owner close announces draining before its configured deadline");
    assert_bound_draining_listener(&path, &runtime_root);
    assert_child_writer_sees_existing_owner(&path);
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 0);
    clock.advance_to(
        contextdb_core::read_contract::OwnerServiceTimeouts::default().shutdown_drain_ms,
    );
    let error = close
        .join()
        .expect("close returns its typed drain outcome")
        .expect_err("a handler held beyond the drain deadline leaves the database open");
    assert!(matches!(error, Error::OwnerReadDrainTimeout));
    assert_eq!(
        database.owner_read_status(),
        contextdb_core::read_contract::OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::ShutdownDraining),
        }
    );
    assert!(matches!(
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path)),
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::OwnerNotServing
    ));
    assert_bound_draining_listener(&path, &runtime_root);
    database
        .execute("SELECT id, payload FROM lifecycle_rows", &HashMap::new())
        .expect("ordinary database work remains available while owner reads drain");
    assert!(matches!(
        Database::open(&path),
        Err(Error::DatabaseLocked { .. })
    ));
    assert_child_writer_sees_existing_owner(&path);
    let retained_after_timeout = database
        .read_route_resources_for_test()
        .expect("inspect retained resources after the drain timeout");
    assert_eq!(
        retained_after_timeout.channel_identity,
        owned_before_drain.channel_identity
    );
    assert_eq!(
        retained_after_timeout.owner_services_started,
        owned_before_drain.owner_services_started
    );
    assert_database_resources_owned(retained_after_timeout);
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 0);

    let committed_id = i64::try_from(ReadLimits::default().result_rows.saturating_add(1))
        .expect("declared row limit fits the SQL integer fixture");
    let committed_payload = format!("committed-after-drain-timeout-{committed_id}");
    database
        .execute(
            "INSERT INTO lifecycle_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), contextdb_core::Value::Int64(committed_id)),
                (
                    "payload".to_owned(),
                    contextdb_core::Value::Text(committed_payload.clone()),
                ),
            ]),
        )
        .expect("the still-open owner commits a real file-backed write after timeout");

    handler.release();
    request
        .join()
        .expect("held request joins after release")
        .expect("the handler receives its completed request");
    assert!(handler.cancellation_observed());
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 0);
    database
        .close()
        .expect("a later close finalizes exactly once after the handler releases");
    assert_owner_listener_released(&path, &runtime_root);
    assert_eq!(plugin.counts.opened.load(Ordering::SeqCst), 1);
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 1);
    let reopened = Database::open(&path).expect("reopen after successful retry close");
    let persisted = reopened
        .execute(
            "SELECT payload FROM lifecycle_rows WHERE id = $id",
            &HashMap::from([("id".to_owned(), contextdb_core::Value::Int64(committed_id))]),
        )
        .expect("read the write committed after drain timeout");
    assert_eq!(
        persisted.rows,
        vec![vec![contextdb_core::Value::Text(committed_payload)]]
    );
    reopened.close().expect("close final verification reopen");
}

#[test]
fn a_suspended_cursor_is_cancelled_and_released_when_its_owner_closes() {
    let handler = GatedHandler::default();
    let plugin = CountingPlugin::new();
    let clock = ManualDeadlineClock::default();
    let (drain_started, drain_started_at) = std::sync::mpsc::channel();
    let (directory, runtime_root, database) =
        create_owner_database(handler, plugin.clone(), clock.clone(), drain_started);
    let path = directory.path().join("owner-read-lifecycle.db");
    let cursor_row_id = i64::try_from(ReadLimits::default().result_rows.saturating_add(1))
        .expect("declared row limit fits the cursor fixture");
    database
        .execute(
            "INSERT INTO lifecycle_rows (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), contextdb_core::Value::Int64(cursor_row_id)),
                (
                    "payload".to_owned(),
                    contextdb_core::Value::Text("cursor-retained-row".to_owned()),
                ),
            ]),
        )
        .expect("seed a second row so the first cursor page remains suspended");
    let cursor_limits = ReadLimits {
        cursor_page_rows: 1,
        ..ReadLimits::default()
    };
    let opening_session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                limits: cursor_limits,
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("select the live owner before opening a one-row cursor page");
    let mut cursor = opening_session
        .open_cursor(
            "SELECT id, payload FROM lifecycle_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("open a cursor with a retained continuation");
    assert_eq!(
        cursor.route(),
        contextdb_core::read_contract::ReadRoute::Owner
    );
    assert_eq!(cursor.first_page().rows.len(), 1);
    assert!(cursor.first_page().has_more);
    drop(opening_session);
    assert_eq!(
        database
            .read_route_resources_for_test()
            .expect("inspect retained cursor resources")
            .active_cursors,
        1
    );

    // Closing the owner cancels every request, cursor and application handler
    // BEFORE it drains, so a suspended cursor is released rather than waited
    // for: nothing admitted is left for the drain to wait on, the close
    // finishes, and the channel comes down with it.
    let closing = Arc::clone(&database);
    let close = std::thread::spawn(move || closing.close());
    drain_started_at
        .recv()
        .expect("cursor-holding close reaches draining");
    clock.advance_to(
        contextdb_core::read_contract::OwnerServiceTimeouts::default().shutdown_drain_ms,
    );
    close
        .join()
        .expect("cursor-holding close joins")
        .expect("a cancelled cursor leaves the drain nothing to wait for");

    plugin.wait_until_closed();
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 1);
    assert_owner_listener_released(&path, &runtime_root);
    let after_close = database
        .read_route_resources_for_test()
        .expect("inspect what the closed owner still holds");
    assert_eq!(after_close.active_cursors, 0);

    // The reader is told plainly that the owner it was reading through has
    // stopped; it is not left holding a cursor that no longer exists, and it
    // is not quietly moved to some other view of the store.
    assert!(matches!(
        cursor.fetch(None),
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::OwnerNotServing
    ));
    assert!(matches!(
        cursor.close(),
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::OwnerNotServing
    ));

    drop(database);
    Database::open(&path).expect("the file reopens once its closed owner has released it");
}

#[test]
fn drop_defers_final_resource_release_until_a_cancelled_handler_returns() {
    let handler = GatedHandler::default();
    let plugin = CountingPlugin::new();
    let clock = ManualDeadlineClock::default();
    let (drain_started, drain_started_at) = std::sync::mpsc::channel();
    let directory = tempfile::TempDir::new().expect("task-scoped owner database directory");
    let path = directory.path().join("drop-owner-read-lifecycle.db");
    let runtime_root = secure_runtime_root(&directory, "drop-owner-runtime");
    let database = Database::open_with_options(
        &path,
        owner_options(
            plugin.clone(),
            handler.clone(),
            clock.clone(),
            drain_started,
            runtime_root.clone(),
        ),
    )
    .expect("configured file owner opens before drop");
    assert_eq!(
        database.owner_read_status().state,
        OwnerServingState::Serving
    );
    let session = database
        .read_session(ReadLimits::default())
        .expect("in-process request owner uses the live database directly");
    let request = std::thread::spawn(move || session.request_owner("administrative", b"hold"));
    handler.wait_until_entered();
    let owned_before_drop = database
        .read_route_resources_for_test()
        .expect("capture channel identity before dropping its owner handle");
    assert_database_resources_owned(owned_before_drop);

    let (drop_returned, drop_returned_at) = std::sync::mpsc::channel();
    let dropping = std::thread::spawn(move || {
        drop(database);
        drop_returned
            .send(())
            .expect("publish owner-handle drop return");
    });
    drain_started_at
        .recv()
        .expect("drop announces draining through the manual-clock seam");
    assert_bound_draining_listener(&path, &runtime_root);
    clock.advance_to(
        contextdb_core::read_contract::OwnerServiceTimeouts::default().shutdown_drain_ms,
    );
    drop_returned_at
        .recv()
        .expect("owner-handle drop returns after transferring finalization");
    dropping.join().expect("owner-handle drop thread joins");
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 0);
    assert!(matches!(
        Database::open(&path),
        Err(Error::DatabaseLocked { .. })
    ));
    assert_child_writer_sees_existing_owner(&path);
    assert_bound_draining_listener(&path, &runtime_root);
    let retained_by_finalizer = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::route_resources_for_path_for_test(&path)
    })
    .expect("inspect the channel retained by the eventual finalizer");
    assert_eq!(
        retained_by_finalizer.channel_identity,
        owned_before_drop.channel_identity
    );
    assert_database_resources_owned(retained_by_finalizer);

    handler.release();
    request
        .join()
        .expect("held request joins after the eventual finalizer observes release")
        .expect("the cancelled handler returns through the request path");
    assert!(handler.cancellation_observed());
    plugin.wait_until_closed();
    assert_eq!(plugin.counts.closed.load(Ordering::SeqCst), 1);
    assert_owner_listener_released(&path, &runtime_root);
    Database::open(&path).expect("reopen succeeds after the eventual finalizer releases resources");
}

#[test]
fn file_owner_refuses_a_real_second_process_until_that_holder_releases() {
    const HOLDER: &str = "CONTEXTDB_OWNER_READ_TEST_HOLDER";
    const HOLDER_PATH: &str = "CONTEXTDB_OWNER_READ_TEST_PATH";
    const HOLDER_RUNTIME: &str = "CONTEXTDB_OWNER_READ_TEST_RUNTIME";
    const HOLDER_NONCE: &str = "CONTEXTDB_OWNER_READ_TEST_NONCE";
    if std::env::var_os(HOLDER).is_some() {
        let path = std::path::PathBuf::from(std::env::var_os(HOLDER_PATH).expect("holder path"));
        let runtime_root = std::path::PathBuf::from(
            std::env::var_os(HOLDER_RUNTIME).expect("holder runtime root"),
        );
        let nonce = std::env::var(HOLDER_NONCE).expect("holder handler nonce");
        let mut options = DatabaseOpenOptions::default();
        options.owner_reads.runtime_dir = Some(runtime_root);
        options.owner_reads.handler = Some(Arc::new(NonceHandler(nonce.as_bytes().to_vec())));
        let database = Database::open_with_options(&path, options)
            .expect("child owns the task-scoped database");
        assert_eq!(
            database.owner_read_status().state,
            OwnerServingState::Serving
        );
        println!("owner-read-holder-ready:{nonce}");
        std::io::stdout().flush().expect("flush child readiness");
        let mut release = String::new();
        std::io::stdin()
            .read_line(&mut release)
            .expect("parent release signal");
        database.close().expect("child releases database ownership");
        return;
    }

    let directory = tempfile::TempDir::new().expect("task-scoped owner database directory");
    let path = directory.path().join("second-process-owner.db");
    let runtime_root = secure_runtime_root(&directory, "child-holder-runtime");
    let nonce = format!("holder:{}:", uuid::Uuid::new_v4());
    let executable = std::env::current_exe().expect("current integration-test executable");
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("file_owner_refuses_a_real_second_process_until_that_holder_releases")
        .arg("--nocapture")
        .env(HOLDER, "1")
        .env(HOLDER_PATH, &path)
        .env(HOLDER_RUNTIME, &runtime_root)
        .env(HOLDER_NONCE, &nonce)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the independent database holder");
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = lines
            .next()
            .expect("child writes a readiness line")
            .expect("read child readiness line");
        if line == format!("owner-read-holder-ready:{nonce}") {
            break;
        }
    }

    let channel_reader =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("path-opened reader selects the child listener through its owner record");
    let challenge =
        format!("listener-exclusive-{}", ReadLimits::default().result_rows).into_bytes();
    let mut expected = nonce.as_bytes().to_vec();
    expected.extend_from_slice(&challenge);
    assert_eq!(
        channel_reader
            .request_owner("holder-identity", &challenge)
            .expect("child-specific handler answers over the actual local channel"),
        expected,
    );
    drop(channel_reader);
    match Database::open(&path) {
        Ok(_) => panic!("a store the child process holds must not open here"),
        Err(refusal) => assert_refused_as_held_by_writer(refusal, child.id(), &path),
    }
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(b"release\n")
        .expect("release the independent database holder");
    assert!(child.wait().expect("child holder exit").success());
    Database::open(&path).expect("database reopens after the independent holder exits");
}
