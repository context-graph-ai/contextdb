use contextdb_core::read_contract::{
    CursorPage, OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::local_transport::MAX_FRAME_BYTES;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    DurableStoreDamage, prepare_durable_store_damage_for_test,
};
use contextdb_engine::read_contract::encode_query_result;
use contextdb_engine::read_session::{
    OwnerHandshakeMismatchForTest, ReadKernelCancellationEvent, ReadKernelSource,
    ReadKernelSourceEvent, ReadKernelTestObserver, ReadRouteOpenFailure, ReadRouteResourceSnapshot,
    ReadSessionEvent, ReadSessionOperation, ReadSessionTestObserver,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::num::NonZeroUsize;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

struct NonceHandler {
    nonce: Vec<u8>,
}

impl OwnerRequestHandler for NonceHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        let mut response = self.nonce.clone();
        response.extend_from_slice(request);
        Ok(response)
    }
}

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create task-scoped owner runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime root");
    root
}

fn roomy_limits() -> ReadLimits {
    let bytes = u64::try_from(MAX_FRAME_BYTES)
        .expect("local frame ceiling fits the public read-limit vocabulary");
    ReadLimits {
        result_rows: bytes,
        result_bytes: bytes.saturating_mul(3),
        work: bytes.saturating_mul(3),
        active_ms: bytes,
        memory: bytes.saturating_mul(4),
        cursor_page_rows: bytes,
        cursor_page_bytes: bytes.saturating_mul(2),
        cursor_idle_ms: bytes,
        cursor_lifetime_ms: bytes.saturating_mul(2),
    }
}

fn client_timeouts() -> ReadClientTimeouts {
    let quantum = u64::try_from(MAX_FRAME_BYTES).expect("frame ceiling fits timeout vocabulary");
    ReadClientTimeouts {
        connect_ms: quantum,
        routing_retry_ms: quantum,
        response_ms: quantum.saturating_mul(4),
    }
}

fn cancellation_batch_rows() -> u64 {
    u64::try_from([ReadRoute::File, ReadRoute::Owner].len())
        .expect("public route vocabulary size fits the row limit")
}

fn session_options() -> ReadSessionOptions {
    let mut limits = roomy_limits();
    limits.cursor_page_rows = cancellation_batch_rows();
    ReadSessionOptions {
        limits,
        timeouts: client_timeouts(),
        ..ReadSessionOptions::default()
    }
}

fn owner_options(
    handler: Arc<dyn OwnerRequestHandler>,
    runtime_dir: std::path::PathBuf,
    kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
) -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions::default();
    let quantum = u64::try_from(MAX_FRAME_BYTES).expect("frame ceiling fits timeout vocabulary");
    options.owner_reads = OwnerReadConfig {
        limits: OwnerReadLimits {
            limits: roomy_limits(),
            concurrency: u64::try_from(std::mem::size_of::<ReadRoute>())
                .expect("route vocabulary size fits concurrency")
                .saturating_add(1),
        },
        timeouts: OwnerServiceTimeouts {
            request_ms: quantum.saturating_mul(2),
            shutdown_drain_ms: quantum,
        },
        runtime_dir: Some(runtime_dir),
        handler: Some(handler),
        ..OwnerReadConfig::default()
    };
    options.test_kernel_observer = kernel_observer;
    options
}

fn seed_committed_rows(path: &std::path::Path) -> usize {
    let database = Database::open(path).expect("create the file-backed committed fixture");
    database
        .execute(
            "CREATE TABLE route_rows (id INTEGER, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the route fixture table");
    let operation_count = u64::try_from(
        [
            ReadSessionOperation::Execute,
            ReadSessionOperation::CursorOpen,
            ReadSessionOperation::CursorFetch,
        ]
        .len(),
    )
    .expect("bounded operation vocabulary fits the row limit");
    let row_count = usize::try_from(
        cancellation_batch_rows()
            .saturating_mul(operation_count)
            .saturating_add(1),
    )
    .expect("dynamic multi-page fixture fits this platform");
    for id in 0..row_count {
        database
            .execute(
                "INSERT INTO route_rows (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(i64::try_from(id).unwrap())),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("route-{id}-{row_count}")),
                    ),
                ]),
            )
            .expect("insert a dynamic committed route fixture row");
    }
    database
        .close()
        .expect("release the idle file for direct reading");
    row_count
}

fn cursor_page_ids(page: &CursorPage) -> Vec<i64> {
    page.rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("cursor id projection returned {other:?}"),
        })
        .collect()
}

fn direct_session(observer: Arc<dyn ReadKernelTestObserver>) -> (tempfile::TempDir, ReadSession) {
    let directory = tempfile::TempDir::new().expect("task-scoped direct-route directory");
    let path = directory.path().join("direct-cancellation.db");
    let runtime_root = secure_runtime_root(&directory, "direct-cancellation-runtime");
    seed_committed_rows(&path);
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_kernel_observer_for_test(&path, session_options(), observer)
    })
    .expect("idle file selects the direct backend");
    assert_eq!(session.route(), ReadRoute::File);
    (directory, session)
}

fn owner_session(
    observer: Arc<dyn ReadKernelTestObserver>,
) -> (tempfile::TempDir, Database, ReadSession) {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-route directory");
    let path = directory.path().join("owner-cancellation.db");
    seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "owner-cancellation-runtime");
    let owner = Database::open_with_options(
        &path,
        owner_options(
            Arc::new(NonceHandler {
                nonce: b"owner-cancellation:".to_vec(),
            }),
            runtime_root.clone(),
            Some(observer),
        ),
    )
    .expect("start the writable owner");
    assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);
    let session = ReadSession::with_runtime_directory_for_test(
        owner
            .owner_read_config()
            .runtime_dir
            .as_deref()
            .expect("owner runtime"),
        || ReadSession::open_with_options(&path, session_options()),
    )
    .expect("live writable owner selects the owner backend");
    assert_eq!(session.route(), ReadRoute::Owner);
    (directory, owner, session)
}

fn assert_cancelled<T>(result: contextdb_core::Result<T>) {
    assert!(matches!(result, Err(Error::ReadCancelled)));
}

fn assert_one_selected_backend_operation(
    before: ReadRouteResourceSnapshot,
    after: ReadRouteResourceSnapshot,
    route: ReadRoute,
) {
    assert_eq!(after.channel_identity, before.channel_identity);
    match route {
        ReadRoute::File => {
            assert_eq!(
                after.local_channel_operations,
                before.local_channel_operations
            );
            assert_eq!(after.direct_backend_opens, before.direct_backend_opens);
            assert_eq!(after.direct_backend_opens, 1);
        }
        ReadRoute::Owner => {
            assert_eq!(
                after.local_channel_operations,
                before.local_channel_operations.saturating_add(1),
            );
            assert_eq!(after.direct_backend_opens, 0);
        }
    }
}

#[test]
fn file_and_owner_routes_select_once_and_return_canonical_equivalent_results() {
    let directory = tempfile::TempDir::new().expect("task-scoped route fixture directory");
    let path = directory.path().join("route-equivalence.db");
    let row_count = seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "route-equivalence-runtime");
    let owner_nonce = format!("owner:{}:", row_count).into_bytes();

    let file_session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, session_options())
    })
    .expect("an idle file selects the direct route");
    assert_eq!(file_session.route(), ReadRoute::File);
    let file_result = file_session
        .execute(
            "SELECT id, payload FROM route_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("direct route returns the complete bounded result");
    let file_error = file_session
        .execute("SELECT * FROM route_rows_missing", &HashMap::new())
        .expect_err("direct route reports the missing table");
    assert!(matches!(
        file_session.request_owner("route-proof", b"file-has-no-owner"),
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::OwnerNotRunning
    ));
    drop(file_session);

    let owner = Database::open_with_options(
        &path,
        owner_options(
            Arc::new(NonceHandler {
                nonce: owner_nonce.clone(),
            }),
            runtime_root.clone(),
            None,
        ),
    )
    .expect("reopen the file with its configured owner read service");
    assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);
    let owner_session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, session_options())
    })
    .expect("a live owner selects the owner route");
    assert_eq!(owner_session.route(), ReadRoute::Owner);
    let owner_challenge = format!("challenge-{row_count}").into_bytes();
    let mut expected_owner_response = owner_nonce;
    expected_owner_response.extend_from_slice(&owner_challenge);
    assert_eq!(
        owner_session
            .request_owner("route-proof", &owner_challenge)
            .expect("owner route traverses the configured local channel"),
        expected_owner_response,
    );
    let owner_result = owner_session
        .execute(
            "SELECT id, payload FROM route_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("owner route returns the complete bounded result");
    let owner_error = owner_session
        .execute("SELECT * FROM route_rows_missing", &HashMap::new())
        .expect_err("owner route reports the missing table");

    assert_eq!(file_result.columns, owner_result.columns);
    assert_eq!(file_result.rows, owner_result.rows);
    assert_eq!(file_result.rows_affected, owner_result.rows_affected);
    assert_eq!(
        format!("{:?}", file_result.trace),
        format!("{:?}", owner_result.trace)
    );
    assert_eq!(
        format!("{:?}", file_result.cascade),
        format!("{:?}", owner_result.cascade)
    );
    assert_eq!(file_result.rows.len(), row_count);
    assert_eq!(
        encode_query_result(&file_result).expect("encode file result canonically"),
        encode_query_result(&owner_result).expect("encode owner result canonically"),
    );
    assert!(matches!(
        (file_error, owner_error),
        (Error::TableNotFound(file), Error::TableNotFound(owner)) if file == owner
    ));
    assert_eq!(owner_session.route(), ReadRoute::Owner);
    assert_eq!(
        owner_session
            .route_resources_for_test()
            .expect("inspect the selected owner route")
            .direct_backend_opens,
        0
    );
    owner
        .close()
        .expect("close the live owner after route comparison");
}

#[test]
fn route_resolution_test_seam_shapes_are_available_without_a_second_selector() {
    let _: fn(
        std::path::PathBuf,
        ReadSessionOptions,
        OwnerHandshakeMismatchForTest,
        Arc<dyn ReadSessionTestObserver>,
    ) -> contextdb_core::Result<ReadSession> =
        ReadSession::open_with_owner_handshake_mismatch_for_test;
    let _ = ReadSessionEvent::OwnerResolution {
        attempt: 1,
        owner_available: false,
    };
    let _ = ReadSessionEvent::BeforeDirectBackendOpen { attempt: 1 };
    let _ = ReadSessionEvent::RouteOpenFailed {
        attempt: 1,
        failure: ReadRouteOpenFailure::WriterBecameOwner,
    };
    let _ = ReadSessionEvent::OwnershipRaceRetry {
        failed_attempt: 1,
        retry_attempt: 2,
    };
}

#[derive(Default)]
struct StartupRaceState {
    events: Vec<ReadSessionEvent>,
    direct_open_waiting: bool,
    reader_finished: bool,
    released: bool,
}

#[derive(Default)]
struct StartupRaceObserver {
    state: Mutex<StartupRaceState>,
    changed: Condvar,
}

impl StartupRaceObserver {
    fn reader_finished(&self) {
        let mut state = self.state.lock().expect("startup-race observer lock");
        state.reader_finished = true;
        self.changed.notify_all();
    }

    fn wait_for_direct_open_boundary(&self) {
        let mut state = self.state.lock().expect("startup-race observer lock");
        while !state.direct_open_waiting && !state.reader_finished {
            state = self
                .changed
                .wait(state)
                .expect("startup-race observer wait");
        }
        assert!(
            state.direct_open_waiting,
            "route selection returned before reaching the real direct-open boundary",
        );
    }

    fn release_direct_open(&self) {
        let mut state = self.state.lock().expect("startup-race observer lock");
        state.released = true;
        self.changed.notify_all();
    }

    fn events(&self) -> Vec<ReadSessionEvent> {
        self.state
            .lock()
            .expect("startup-race observer lock")
            .events
            .clone()
    }
}

impl ReadSessionTestObserver for StartupRaceObserver {
    fn observe_event(&self, event: ReadSessionEvent) {
        let mut state = self.state.lock().expect("startup-race observer lock");
        state.events.push(event);
        if event == (ReadSessionEvent::BeforeDirectBackendOpen { attempt: 1 }) {
            state.direct_open_waiting = true;
            self.changed.notify_all();
            while !state.released {
                state = self
                    .changed
                    .wait(state)
                    .expect("startup-race observer wait");
            }
        }
    }
}

#[derive(Default)]
struct RouteEventRecorder(Mutex<Vec<ReadSessionEvent>>);

impl RouteEventRecorder {
    fn events(&self) -> Vec<ReadSessionEvent> {
        self.0.lock().expect("route event recorder lock").clone()
    }
}

impl ReadSessionTestObserver for RouteEventRecorder {
    fn observe_event(&self, event: ReadSessionEvent) {
        self.0
            .lock()
            .expect("route event recorder lock")
            .push(event);
    }
}

fn route_resolution_events(events: &[ReadSessionEvent]) -> Vec<ReadSessionEvent> {
    events
        .iter()
        .copied()
        .filter(|event| {
            matches!(
                event,
                ReadSessionEvent::OwnerResolution { .. }
                    | ReadSessionEvent::BeforeDirectBackendOpen { .. }
                    | ReadSessionEvent::RouteOpenFailed { .. }
                    | ReadSessionEvent::OwnershipRaceRetry { .. }
                    | ReadSessionEvent::RouteSelected(_)
            )
        })
        .collect()
}

fn assert_terminal_route_open_failure(
    observer: &RouteEventRecorder,
    expected: ReadRouteOpenFailure,
) {
    let events = route_resolution_events(&observer.events());
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ReadSessionEvent::RouteOpenFailed { .. }))
            .count(),
        1,
    );
    assert!(events.contains(&ReadSessionEvent::RouteOpenFailed {
        attempt: 1,
        failure: expected,
    }));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ReadSessionEvent::OwnershipRaceRetry { .. }))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ReadSessionEvent::RouteSelected(_)))
    );
}

const STARTUP_RACE_OWNER_CHILD: &str = "CONTEXTDB_STARTUP_RACE_OWNER_CHILD";
const STARTUP_RACE_OWNER_PATH: &str = "CONTEXTDB_STARTUP_RACE_OWNER_PATH";
const STARTUP_RACE_OWNER_RUNTIME: &str = "CONTEXTDB_STARTUP_RACE_OWNER_RUNTIME";
const STARTUP_RACE_OWNER_NONCE: &str = "CONTEXTDB_STARTUP_RACE_OWNER_NONCE";

fn spawn_startup_race_owner(
    path: &std::path::Path,
    runtime_root: &std::path::Path,
    nonce: &str,
) -> Child {
    let executable = std::env::current_exe().expect("current integration-test executable");
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("writer_winning_the_direct_open_boundary_retries_once_into_the_real_owner")
        .arg("--nocapture")
        .env(STARTUP_RACE_OWNER_CHILD, "1")
        .env(STARTUP_RACE_OWNER_PATH, path)
        .env(STARTUP_RACE_OWNER_RUNTIME, runtime_root)
        .env(STARTUP_RACE_OWNER_NONCE, nonce)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the independent startup-race writer");
    let stdout = child.stdout.take().expect("startup-race writer stdout");
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = lines
            .next()
            .expect("startup-race writer emits readiness or exits")
            .expect("read startup-race writer output");
        if line == format!("contextdb-startup-race-ready:{nonce}") {
            break;
        }
    }
    child
}

#[test]
fn writer_winning_the_direct_open_boundary_retries_once_into_the_real_owner() {
    if std::env::var_os(STARTUP_RACE_OWNER_CHILD).is_some() {
        let path = std::path::PathBuf::from(
            std::env::var_os(STARTUP_RACE_OWNER_PATH).expect("startup-race writer path"),
        );
        let runtime_root = std::path::PathBuf::from(
            std::env::var_os(STARTUP_RACE_OWNER_RUNTIME).expect("startup-race writer runtime"),
        );
        let nonce =
            std::env::var(STARTUP_RACE_OWNER_NONCE).expect("startup-race writer handler nonce");
        let owner = Database::open_with_options(
            &path,
            owner_options(
                Arc::new(NonceHandler {
                    nonce: nonce.as_bytes().to_vec(),
                }),
                runtime_root,
                None,
            ),
        )
        .expect("child becomes the writer while direct open is held");
        assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);
        println!("contextdb-startup-race-ready:{nonce}");
        std::io::stdout()
            .flush()
            .expect("flush startup-race readiness");
        let mut release = String::new();
        std::io::stdin()
            .read_line(&mut release)
            .expect("parent releases the startup-race writer");
        owner.close().expect("startup-race writer closes");
        return;
    }

    let directory = tempfile::TempDir::new().expect("task-scoped startup-race directory");
    let path = directory.path().join("startup-race.db");
    let row_count = seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "startup-race-runtime");
    let nonce = format!("startup-race:{}:{}:", row_count, uuid::Uuid::new_v4());
    let observer = Arc::new(StartupRaceObserver::default());
    let opening_observer = observer.clone();
    let opening_path = path.clone();
    let opening_runtime = runtime_root.clone();
    let reader = std::thread::spawn(move || {
        let result = ReadSession::with_runtime_directory_for_test(&opening_runtime, || {
            ReadSession::open_with_observer_for_test(
                &opening_path,
                session_options(),
                opening_observer.clone(),
            )
        });
        opening_observer.reader_finished();
        result
    });
    observer.wait_for_direct_open_boundary();

    let mut owner = spawn_startup_race_owner(&path, &runtime_root, &nonce);
    observer.release_direct_open();
    let session = reader
        .join()
        .expect("startup-race reader thread joins")
        .expect("the single ownership-race retry selects the writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    let challenge = format!("race-challenge-{row_count}").into_bytes();
    let mut expected = nonce.as_bytes().to_vec();
    expected.extend_from_slice(&challenge);
    assert_eq!(
        session
            .request_owner("startup-race", &challenge)
            .expect("the retried selection traverses the child owner's real channel"),
        expected,
    );
    assert_eq!(
        route_resolution_events(&observer.events()),
        vec![
            ReadSessionEvent::OwnerResolution {
                attempt: 1,
                owner_available: false,
            },
            ReadSessionEvent::BeforeDirectBackendOpen { attempt: 1 },
            ReadSessionEvent::RouteOpenFailed {
                attempt: 1,
                failure: ReadRouteOpenFailure::WriterBecameOwner,
            },
            ReadSessionEvent::OwnershipRaceRetry {
                failed_attempt: 1,
                retry_attempt: 2,
            },
            ReadSessionEvent::OwnerResolution {
                attempt: 2,
                owner_available: true,
            },
            ReadSessionEvent::RouteSelected(ReadRoute::Owner),
        ],
        "only a writer winning the real direct-open race may trigger the one retry",
    );
    let selected = session
        .route_resources_for_test()
        .expect("inspect the route selected after the startup race");
    assert_eq!(selected.direct_backend_opens, 1);
    assert_eq!(selected.local_channel_operations, 1);

    owner.kill().expect("stop the selected startup-race owner");
    owner.wait().expect("reap the selected startup-race owner");
    assert!(matches!(
        session.request_owner("startup-race", &challenge),
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::OwnerDisconnected
    ));
    let after_disconnect = session
        .route_resources_for_test()
        .expect("inspect the fixed route after selected-owner disconnect");
    assert_eq!(after_disconnect.channel_identity, selected.channel_identity);
    assert_eq!(
        after_disconnect.direct_backend_opens,
        selected.direct_backend_opens
    );
    assert_eq!(
        after_disconnect.local_channel_operations,
        selected.local_channel_operations.saturating_add(1),
    );
    let final_events = route_resolution_events(&observer.events());
    assert_eq!(
        final_events
            .iter()
            .filter(|event| matches!(event, ReadSessionEvent::OwnershipRaceRetry { .. }))
            .count(),
        1,
    );
    assert_eq!(
        final_events
            .iter()
            .filter(|event| {
                **event == (ReadSessionEvent::BeforeDirectBackendOpen { attempt: 1 })
            })
            .count(),
        1,
    );
    assert!(!final_events.contains(&ReadSessionEvent::RouteSelected(ReadRoute::File)));
}

#[test]
fn missing_store_direct_open_failure_is_terminal_without_an_ownership_retry() {
    let directory = tempfile::TempDir::new().expect("task-scoped missing-store directory");
    let runtime_root = secure_runtime_root(&directory, "missing-store-runtime");
    let path = directory
        .path()
        .join(format!("missing-{}.db", uuid::Uuid::new_v4()));
    let observer = Arc::new(RouteEventRecorder::default());
    let result = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_observer_for_test(&path, session_options(), observer.clone())
    });
    assert!(matches!(
        result,
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::StoreNotFound
    ));
    assert_terminal_route_open_failure(&observer, ReadRouteOpenFailure::DirectOpen);
}

#[test]
fn missing_store_beneath_a_missing_parent_keeps_the_typed_refusal() {
    let directory = tempfile::TempDir::new().expect("task-scoped missing-parent directory");
    let runtime_root = secure_runtime_root(&directory, "missing-parent-runtime");
    let path = directory
        .path()
        .join("parent-that-does-not-exist")
        .join("missing-store.db");
    let observer = Arc::new(RouteEventRecorder::default());
    let result = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_observer_for_test(&path, session_options(), observer.clone())
    });
    assert!(matches!(
        result,
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::StoreNotFound
    ));
    assert!(
        !path.parent().expect("missing store has a parent").exists(),
        "a read-only open must not create the missing parent"
    );
    assert_terminal_route_open_failure(&observer, ReadRouteOpenFailure::DirectOpen);
}

#[test]
fn damaged_store_direct_open_failure_is_terminal_without_an_ownership_retry() {
    let directory = tempfile::TempDir::new().expect("task-scoped damaged-store directory");
    let path = directory.path().join("damaged-store.db");
    seed_committed_rows(&path);
    prepare_durable_store_damage_for_test(&path, DurableStoreDamage::MalformedRecord)
        .expect("damage a real durable record through the persistence fixture seam");
    let runtime_root = secure_runtime_root(&directory, "damaged-store-runtime");
    let observer = Arc::new(RouteEventRecorder::default());
    let result = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_observer_for_test(&path, session_options(), observer.clone())
    });
    assert!(matches!(result, Err(Error::StoreCorrupted { .. })));
    assert_terminal_route_open_failure(&observer, ReadRouteOpenFailure::StoreDamage);
}

#[test]
fn direct_read_requiring_writer_is_terminal_without_an_ownership_retry() {
    let directory = tempfile::TempDir::new().expect("task-scoped direct-refusal directory");
    let path = directory.path().join("direct-refusal.db");
    seed_committed_rows(&path);
    prepare_durable_store_damage_for_test(&path, DurableStoreDamage::NonMonotonicCommitIndex)
        .expect("prepare state whose direct diagnosis requires a writer");
    let runtime_root = secure_runtime_root(&directory, "direct-refusal-runtime");
    let observer = Arc::new(RouteEventRecorder::default());
    let result = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_observer_for_test(&path, session_options(), observer.clone())
    });
    assert!(matches!(
        result,
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::DirectReadRequiresWriter
    ));
    assert_terminal_route_open_failure(&observer, ReadRouteOpenFailure::DirectReadRequiresWriter);
}

struct RejectIfInvokedHandler;

impl OwnerRequestHandler for RejectIfInvokedHandler {
    fn handle(
        &self,
        _namespace: &str,
        _request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        panic!("an unauthenticated request must not reach the configured handler")
    }
}

#[test]
fn owner_identity_authentication_failure_is_terminal_without_direct_fallback_or_retry() {
    let directory = tempfile::TempDir::new().expect("task-scoped authentication directory");
    let path = directory.path().join("owner-authentication.db");
    seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "owner-authentication-runtime");
    let owner = Database::open_with_options(
        &path,
        owner_options(Arc::new(RejectIfInvokedHandler), runtime_root.clone(), None),
    )
    .expect("start the authenticated owner endpoint");
    assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);
    let observer = Arc::new(RouteEventRecorder::default());

    let result = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_owner_handshake_mismatch_for_test(
            &path,
            session_options(),
            OwnerHandshakeMismatchForTest::DatabaseIdentity,
            observer.clone(),
        )
    });
    assert!(matches!(
        result,
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::OwnerMismatch
    ));
    assert_terminal_route_open_failure(&observer, ReadRouteOpenFailure::OwnerAuthentication);
    assert!(
        !observer
            .events()
            .contains(&ReadSessionEvent::BeforeDirectBackendOpen { attempt: 1 })
    );
    owner
        .close()
        .expect("close owner after authentication refusal");
}

#[derive(Default)]
struct KernelCancellationState {
    armed: bool,
    entered: Option<ReadKernelSourceEvent>,
    kernel_token: Option<OwnerReadCancellation>,
    released: bool,
    operation_finished: bool,
    cancellation_observed: bool,
}

#[derive(Default)]
struct KernelCancellationGate {
    state: Mutex<KernelCancellationState>,
    changed: Condvar,
}

impl KernelCancellationGate {
    fn arm(&self) {
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        *state = KernelCancellationState {
            armed: true,
            ..KernelCancellationState::default()
        };
    }

    fn operation_finished(&self) {
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        state.operation_finished = true;
        self.changed.notify_all();
    }

    fn wait_for_source(&self) -> (ReadKernelSourceEvent, OwnerReadCancellation) {
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        while state.entered.is_none() && !state.operation_finished {
            state = self
                .changed
                .wait(state)
                .expect("kernel cancellation gate wait");
        }
        let event = state.entered.expect(
            "operation returned before the selected backend entered the bounded source loop",
        );
        let cancellation = state
            .kernel_token
            .as_ref()
            .expect("source event carries the kernel cancellation token")
            .clone();
        (event, cancellation)
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        state.released = true;
        self.changed.notify_all();
    }

    fn disarm(&self) {
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        state.armed = false;
    }

    fn assert_kernel_reported_cancellation(&self) {
        assert!(
            self.state
                .lock()
                .expect("kernel cancellation gate lock")
                .cancellation_observed,
            "typed cancellation must be emitted by the bounded kernel",
        );
    }
}

impl ReadKernelTestObserver for KernelCancellationGate {
    fn before_source_touch(
        &self,
        event: ReadKernelSourceEvent,
        cancellation: OwnerReadCancellation,
    ) {
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        if !state.armed {
            return;
        }
        if event.source == ReadKernelSource::TableRow
            && event.completed_items == cancellation_batch_rows().saturating_sub(1)
            && state.entered.is_none()
        {
            state.entered = Some(event);
            state.kernel_token = Some(cancellation.clone());
            self.changed.notify_all();
        }
        if state.entered.is_none() {
            return;
        }
        while !state.released {
            state = self
                .changed
                .wait(state)
                .expect("kernel cancellation gate wait");
        }
        assert!(
            cancellation.is_cancelled(),
            "the token consumed beyond the selected backend must be the caller's cancelled token",
        );
    }

    fn cancellation_observed(
        &self,
        event: ReadKernelCancellationEvent,
        cancellation: OwnerReadCancellation,
    ) {
        assert!(cancellation.is_cancelled());
        let mut state = self.state.lock().expect("kernel cancellation gate lock");
        let entered = state
            .entered
            .expect("cancellation follows a source-loop entry");
        assert_eq!(event.operation, entered.operation);
        assert_eq!(event.route, entered.route);
        state.cancellation_observed = true;
        self.changed.notify_all();
    }
}

fn cancel_execute_inside_kernel(
    session: ReadSession,
    gate: Arc<KernelCancellationGate>,
    route: ReadRoute,
) {
    gate.arm();
    let session = Arc::new(session);
    let before = session
        .route_resources_for_test()
        .expect("capture selected backend state before execute");
    let before_source_rows = before.bounded_source_items_completed;
    let cancellation = OwnerReadCancellation::new();
    let operation_cancellation = cancellation.clone();
    let finished = gate.clone();
    let executing = Arc::clone(&session);
    let worker = std::thread::spawn(move || {
        let result = executing.execute_with_cancellation(
            "SELECT id FROM route_rows",
            &HashMap::new(),
            &operation_cancellation,
        );
        finished.operation_finished();
        result
    });
    let (event, kernel_token) = gate.wait_for_source();
    let blocked = session.route_resources_for_test();
    let cancelled_before_request = kernel_token.is_cancelled();
    cancellation.cancel();
    let identical_token_observed = kernel_token.is_cancelled();
    gate.release();
    let outcome = worker.join();
    assert_eq!(event.operation, ReadSessionOperation::Execute);
    assert_eq!(event.route, route);
    assert_eq!(event.source, ReadKernelSource::TableRow);
    assert_eq!(
        event.completed_items,
        cancellation_batch_rows().saturating_sub(1)
    );
    assert!(!cancelled_before_request);
    assert_eq!(
        blocked
            .expect("inspect the blocked execute source")
            .bounded_source_items_completed,
        before_source_rows.saturating_add(cancellation_batch_rows().saturating_sub(1)),
        "the cancellation gate must be beyond the backend adapter and after one real row",
    );
    assert_cancelled(outcome.expect("cancelled execute worker joins"));
    let after = session
        .route_resources_for_test()
        .expect("inspect execute source after cancellation");
    assert_eq!(
        after.bounded_source_items_completed,
        before_source_rows.saturating_add(cancellation_batch_rows().saturating_sub(1)),
        "the cancelled kernel must not touch the second row",
    );
    assert_one_selected_backend_operation(before, after, route);
    assert!(
        identical_token_observed,
        "a route-level check or unrelated backend token cannot satisfy kernel cancellation",
    );
    gate.assert_kernel_reported_cancellation();
}

fn cancel_cursor_open_inside_kernel(
    session: ReadSession,
    gate: Arc<KernelCancellationGate>,
    route: ReadRoute,
) {
    gate.arm();
    let session = Arc::new(session);
    let before = session
        .route_resources_for_test()
        .expect("capture selected backend state before cursor open");
    let before_source_rows = before.bounded_source_items_completed;
    let cancellation = OwnerReadCancellation::new();
    let operation_cancellation = cancellation.clone();
    let finished = gate.clone();
    let opening = Arc::clone(&session);
    let worker = std::thread::spawn(move || {
        let result = opening.open_cursor_with_cancellation(
            "SELECT id FROM route_rows",
            &HashMap::new(),
            &operation_cancellation,
        );
        finished.operation_finished();
        result
    });
    let (event, kernel_token) = gate.wait_for_source();
    let blocked = session.route_resources_for_test();
    let cancelled_before_request = kernel_token.is_cancelled();
    cancellation.cancel();
    let identical_token_observed = kernel_token.is_cancelled();
    gate.release();
    let outcome = worker.join();
    assert_eq!(event.operation, ReadSessionOperation::CursorOpen);
    assert_eq!(event.route, route);
    assert_eq!(event.source, ReadKernelSource::TableRow);
    assert_eq!(
        event.completed_items,
        cancellation_batch_rows().saturating_sub(1)
    );
    assert!(!cancelled_before_request);
    assert_eq!(
        blocked
            .expect("inspect the blocked cursor-open source")
            .bounded_source_items_completed,
        before_source_rows.saturating_add(cancellation_batch_rows().saturating_sub(1)),
    );
    assert_cancelled(outcome.expect("cancelled cursor-open worker joins"));
    let after = session
        .route_resources_for_test()
        .expect("inspect cursor-open source after cancellation");
    assert_eq!(
        after.bounded_source_items_completed,
        before_source_rows.saturating_add(cancellation_batch_rows().saturating_sub(1)),
        "cursor open must not touch the second row after cancellation",
    );
    assert_one_selected_backend_operation(before, after, route);
    assert!(identical_token_observed);
    gate.assert_kernel_reported_cancellation();
}

fn cancel_cursor_fetch_inside_kernel(
    session: ReadSession,
    gate: Arc<KernelCancellationGate>,
    route: ReadRoute,
) {
    let cursor = session
        .open_cursor("SELECT id FROM route_rows", &HashMap::new())
        .expect("open a cursor with a suspended continuation before fetch cancellation");
    assert!(cursor.first_page().has_more);
    let first_page_ids = cursor_page_ids(cursor.first_page());
    let requested_rows = usize::try_from(cancellation_batch_rows())
        .expect("cancellation batch size fits this platform");
    assert_eq!(first_page_ids.len(), requested_rows);
    gate.arm();
    let before = session
        .route_resources_for_test()
        .expect("capture selected backend state before cursor fetch");
    let before_source_rows = before.bounded_source_items_completed;
    let cancellation = OwnerReadCancellation::new();
    let operation_cancellation = cancellation.clone();
    let finished = gate.clone();
    let worker = std::thread::spawn(move || {
        let mut cursor = cursor;
        let result = cursor
            .fetch_with_cancellation(NonZeroUsize::new(requested_rows), &operation_cancellation);
        finished.operation_finished();
        (cursor, result)
    });
    let (event, kernel_token) = gate.wait_for_source();
    let blocked = session.route_resources_for_test();
    let cancelled_before_request = kernel_token.is_cancelled();
    cancellation.cancel();
    let identical_token_observed = kernel_token.is_cancelled();
    gate.release();
    let outcome = worker.join();
    assert_eq!(event.operation, ReadSessionOperation::CursorFetch);
    assert_eq!(event.route, route);
    assert_eq!(event.source, ReadKernelSource::TableRow);
    assert_eq!(
        event.completed_items,
        cancellation_batch_rows().saturating_sub(1)
    );
    assert!(!cancelled_before_request);
    assert_eq!(
        blocked
            .expect("inspect the blocked cursor-fetch source")
            .bounded_source_items_completed,
        before_source_rows.saturating_add(cancellation_batch_rows().saturating_sub(1)),
    );
    let (mut cursor, cancelled_fetch) = outcome.expect("cancelled cursor-fetch worker joins");
    assert_cancelled(cancelled_fetch);
    let after = session
        .route_resources_for_test()
        .expect("inspect cursor-fetch source after cancellation");
    assert_eq!(
        after.bounded_source_items_completed,
        before_source_rows.saturating_add(cancellation_batch_rows().saturating_sub(1)),
        "cursor fetch must not touch the second requested row after cancellation",
    );
    assert_one_selected_backend_operation(before, after, route);
    assert!(identical_token_observed);
    gate.assert_kernel_reported_cancellation();
    gate.disarm();

    let expected_start = first_page_ids
        .last()
        .copied()
        .expect("the first cursor page contains a real row")
        .saturating_add(1);
    let next_page = cursor
        .fetch(NonZeroUsize::new(requested_rows))
        .expect("the same cursor resumes after its cancelled fetch");
    assert_eq!(
        cursor_page_ids(&next_page),
        (0..requested_rows)
            .map(|offset| expected_start.saturating_add(i64::try_from(offset).unwrap()))
            .collect::<Vec<_>>(),
        "a cancelled unpublished page must not advance the suspended continuation",
    );
    assert!(next_page.has_more, "the cursor remains genuinely suspended");
    let after_resume = session
        .route_resources_for_test()
        .expect("inspect the selected route after cursor resume");
    assert_one_selected_backend_operation(after, after_resume, route);
    assert_eq!(cursor.route(), route);
    cursor.close().expect("close the resumed cursor");
}

#[test]
fn direct_execute_observes_mid_operation_cancellation_inside_the_kernel() {
    let gate = Arc::new(KernelCancellationGate::default());
    let (_directory, session) = direct_session(gate.clone());
    cancel_execute_inside_kernel(session, gate, ReadRoute::File);
}

#[test]
fn direct_cursor_open_observes_mid_operation_cancellation_inside_the_kernel() {
    let gate = Arc::new(KernelCancellationGate::default());
    let (_directory, session) = direct_session(gate.clone());
    cancel_cursor_open_inside_kernel(session, gate, ReadRoute::File);
}

#[test]
fn direct_cursor_fetch_observes_mid_operation_cancellation_inside_the_kernel() {
    let gate = Arc::new(KernelCancellationGate::default());
    let (_directory, session) = direct_session(gate.clone());
    cancel_cursor_fetch_inside_kernel(session, gate, ReadRoute::File);
}

#[test]
fn live_owner_execute_observes_mid_operation_cancellation_inside_the_kernel() {
    let gate = Arc::new(KernelCancellationGate::default());
    let (_directory, owner, session) = owner_session(gate.clone());
    cancel_execute_inside_kernel(session, gate, ReadRoute::Owner);
    owner.close().expect("close owner after cancellation proof");
}

#[test]
fn live_owner_cursor_open_observes_mid_operation_cancellation_inside_the_kernel() {
    let gate = Arc::new(KernelCancellationGate::default());
    let (_directory, owner, session) = owner_session(gate.clone());
    cancel_cursor_open_inside_kernel(session, gate, ReadRoute::Owner);
    owner.close().expect("close owner after cancellation proof");
}

#[test]
fn live_owner_cursor_fetch_observes_mid_operation_cancellation_inside_the_kernel() {
    let gate = Arc::new(KernelCancellationGate::default());
    let (_directory, owner, session) = owner_session(gate.clone());
    cancel_cursor_fetch_inside_kernel(session, gate, ReadRoute::Owner);
    owner.close().expect("close owner after cancellation proof");
}

#[derive(Default)]
struct PartialFrameState {
    first_nonterminal_frame: bool,
    query_finished: bool,
    released: bool,
    events: Vec<ReadSessionEvent>,
}

#[derive(Default)]
struct PartialFrameObserver {
    state: Mutex<PartialFrameState>,
    changed: Condvar,
}

impl PartialFrameObserver {
    fn events(&self) -> Vec<ReadSessionEvent> {
        self.state
            .lock()
            .expect("partial-frame observer lock")
            .events
            .clone()
    }

    fn wait_for_first_nonterminal_frame(&self) {
        let mut state = self.state.lock().expect("partial-frame observer lock");
        while !state.first_nonterminal_frame && !state.query_finished {
            state = self
                .changed
                .wait(state)
                .expect("partial-frame observer wait");
        }
        assert!(
            state.first_nonterminal_frame,
            "query returned before route assembly exposed its first nonterminal frame"
        );
    }

    fn query_finished(&self) {
        let mut state = self.state.lock().expect("partial-frame observer lock");
        state.query_finished = true;
        self.changed.notify_all();
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("partial-frame observer lock");
        state.released = true;
        self.changed.notify_all();
    }
}

impl ReadSessionTestObserver for PartialFrameObserver {
    fn observe_event(&self, event: ReadSessionEvent) {
        let mut state = self.state.lock().expect("partial-frame observer lock");
        state.events.push(event);
        if event == (ReadSessionEvent::ResponseFrameReceived { terminal: false }) {
            state.first_nonterminal_frame = true;
            self.changed.notify_all();
            while !state.released {
                state = self
                    .changed
                    .wait(state)
                    .expect("partial-frame observer wait");
            }
        }
    }
}

fn seed_multiframe_result(path: &std::path::Path) {
    let database = Database::open(path).expect("create the multi-frame route fixture");
    database
        .execute(
            "CREATE TABLE multiframe_rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the multi-frame table");
    let payload_bytes = MAX_FRAME_BYTES.saturating_add(MAX_FRAME_BYTES / 4);
    database
        .execute(
            "INSERT INTO multiframe_rows (id, payload) VALUES (1, $payload)",
            &HashMap::from([("payload".to_owned(), Value::Text("x".repeat(payload_bytes)))]),
        )
        .expect("insert a result that crosses the local frame ceiling");
    database
        .close()
        .expect("release fixture before child owner starts");
}

const ROUTE_OWNER_CHILD: &str = "CONTEXTDB_ROUTE_OWNER_CHILD";
const ROUTE_OWNER_PATH: &str = "CONTEXTDB_ROUTE_OWNER_PATH";
const ROUTE_OWNER_RUNTIME: &str = "CONTEXTDB_ROUTE_OWNER_RUNTIME";
const ROUTE_OWNER_NONCE: &str = "CONTEXTDB_ROUTE_OWNER_NONCE";

fn spawn_owner_process(
    path: &std::path::Path,
    runtime_root: &std::path::Path,
    nonce: &str,
) -> Child {
    let executable = std::env::current_exe().expect("current integration-test executable");
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("owner_disconnect_after_partial_reply_never_falls_back_within_the_session")
        .arg("--nocapture")
        .env(ROUTE_OWNER_CHILD, "1")
        .env(ROUTE_OWNER_PATH, path)
        .env(ROUTE_OWNER_RUNTIME, runtime_root)
        .env(ROUTE_OWNER_NONCE, nonce)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the independent writable owner");
    let stdout = child.stdout.take().expect("child owner stdout");
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = lines
            .next()
            .expect("child owner writes a readiness line")
            .expect("read child owner output");
        if line == format!("contextdb-route-owner-ready:{nonce}") {
            break;
        }
    }
    child
}

#[test]
fn owner_disconnect_after_partial_reply_never_falls_back_within_the_session() {
    if std::env::var_os(ROUTE_OWNER_CHILD).is_some() {
        let path =
            std::path::PathBuf::from(std::env::var_os(ROUTE_OWNER_PATH).expect("child owner path"));
        let runtime_root = std::path::PathBuf::from(
            std::env::var_os(ROUTE_OWNER_RUNTIME).expect("child owner runtime root"),
        );
        let nonce = std::env::var(ROUTE_OWNER_NONCE).expect("child owner handler nonce");
        let owner = Database::open_with_options(
            &path,
            owner_options(
                Arc::new(NonceHandler {
                    nonce: nonce.as_bytes().to_vec(),
                }),
                runtime_root,
                None,
            ),
        )
        .expect("child process becomes the writable owner");
        assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);
        println!("contextdb-route-owner-ready:{nonce}");
        std::io::stdout().flush().expect("flush child readiness");
        let mut release = String::new();
        std::io::stdin()
            .read_line(&mut release)
            .expect("parent release or process kill");
        owner.close().expect("released child owner closes");
        return;
    }

    let directory = tempfile::TempDir::new().expect("task-scoped route fixture directory");
    let path = directory.path().join("owner-mid-reply.db");
    seed_multiframe_result(&path);
    let owner_runtime = secure_runtime_root(&directory, "owner-route-runtime");
    let original_nonce = format!("original:{}:", uuid::Uuid::new_v4());
    let replacement_nonce = format!("replacement:{}:", uuid::Uuid::new_v4());
    assert_ne!(original_nonce, replacement_nonce);
    let mut original_owner = spawn_owner_process(&path, &owner_runtime, &original_nonce);

    let observer = Arc::new(PartialFrameObserver::default());
    let session = Arc::new(
        ReadSession::with_runtime_directory_for_test(&owner_runtime, || {
            ReadSession::open_with_observer_for_test(&path, session_options(), observer.clone())
        })
        .expect("path-opened reader selects the child owner once"),
    );
    assert_eq!(session.route(), ReadRoute::Owner);
    let challenge = format!("selected-owner-{}", MAX_FRAME_BYTES).into_bytes();
    let mut original_response = original_nonce.as_bytes().to_vec();
    original_response.extend_from_slice(&challenge);
    assert_eq!(
        session
            .request_owner("route-identity", &challenge)
            .expect("the selected child handler answers over the real local channel"),
        original_response,
    );
    let before_query = session
        .route_resources_for_test()
        .expect("capture route evidence after the nonce request");
    assert_eq!(before_query.local_channel_operations, 1);
    assert_eq!(before_query.direct_backend_opens, 0);
    let selected_identity = session
        .route_resources_for_test()
        .expect("capture selected owner identity")
        .channel_identity
        .expect("owner route carries channel identity");
    let querying = Arc::clone(&session);
    let query_observer = Arc::clone(&observer);
    let query = std::thread::spawn(move || {
        let result = querying.execute("SELECT payload FROM multiframe_rows", &HashMap::new());
        query_observer.query_finished();
        result
    });
    observer.wait_for_first_nonterminal_frame();

    original_owner
        .kill()
        .expect("kill original owner between reply frames");
    original_owner.wait().expect("reap original owner");
    let mut replacement_owner = spawn_owner_process(&path, &owner_runtime, &replacement_nonce);
    observer.release();

    let failure = query
        .join()
        .expect("mid-reply query thread joins")
        .expect_err("partial response is discarded after owner death");
    assert!(matches!(
        failure,
        Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::OwnerDisconnected
    ));
    assert_eq!(session.route(), ReadRoute::Owner);
    let after_disconnect = session
        .route_resources_for_test()
        .expect("inspect route after disconnect");
    assert_eq!(after_disconnect.channel_identity, Some(selected_identity));
    assert_eq!(
        after_disconnect.local_channel_operations,
        before_query.local_channel_operations.saturating_add(1),
    );
    assert_eq!(after_disconnect.direct_backend_opens, 0);
    assert_eq!(after_disconnect.active_owner_slots, 0);
    assert_eq!(after_disconnect.active_cursors, 0);
    let events = observer.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == ReadSessionEvent::RouteSelected(ReadRoute::Owner))
            .count(),
        1,
        "the failed invocation keeps its original owner selection",
    );
    assert!(!events.contains(&ReadSessionEvent::RouteSelected(ReadRoute::File)));
    assert!(!events.contains(&ReadSessionEvent::DirectBackendOpen));
    assert!(events.contains(&ReadSessionEvent::ResponseFrameReceived { terminal: false }));

    let fresh = ReadSession::with_runtime_directory_for_test(&owner_runtime, || {
        ReadSession::open_with_options(&path, session_options())
    })
    .expect("only a fresh invocation may select the replacement owner");
    assert_eq!(fresh.route(), ReadRoute::Owner);
    let mut replacement_response = replacement_nonce.as_bytes().to_vec();
    replacement_response.extend_from_slice(&challenge);
    assert_eq!(
        fresh
            .request_owner("route-identity", &challenge)
            .expect("a fresh invocation reaches only the replacement handler"),
        replacement_response,
    );
    let replacement_resources = fresh
        .route_resources_for_test()
        .expect("inspect the replacement owner route");
    assert_eq!(
        replacement_resources.channel_identity,
        Some(selected_identity)
    );
    assert_eq!(replacement_resources.local_channel_operations, 1);
    assert_eq!(replacement_resources.direct_backend_opens, 0);
    replacement_owner
        .kill()
        .expect("stop replacement owner after fresh-route proof");
    replacement_owner.wait().expect("reap replacement owner");
}

#[test]
fn ordinary_owner_disconnect_failure_shape_remains_constructible() {
    let failure = ReadFailure::new(ReadFailureKind::OwnerDisconnected, ReadFailureDetail::None)
        .expect("owner disconnect uses ordinary detail");
    assert!(matches!(
        Error::from(failure),
        Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::OwnerDisconnected
    ));
}
