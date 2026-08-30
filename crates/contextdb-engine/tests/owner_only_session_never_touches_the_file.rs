#![cfg(feature = "test-seams")]
//! A consumer that holds a store path and wants to ask the live owner an
//! administrative question must be able to open a session that either takes
//! the owner route or says plainly that no owner is running — and that must
//! never fall through to reading the committed file.
//!
//! The surface these arms compile against, and the one the engine implements:
//!
//! ```ignore
//! impl ReadSession {
//!     /// Select the owner route only. A store nobody owns answers
//!     /// `OwnerNotRunning` and the committed file is never opened.
//!     pub fn open_owner_only(
//!         path: impl AsRef<Path>,
//!         options: ReadSessionOptions,
//!     ) -> Result<ReadSession>;
//! }
//! ```
//!
//! A `route` field on `ReadSessionOptions` was the alternative and is rejected
//! here: that struct is built by plain literal throughout the estate, with no
//! `..Default::default()` tail, so a new field would break every construction
//! site at once. A second opener adds the door without moving anything.
//!
//! What makes the "never opened the file" claim provable rather than asserted:
//! a store whose committed file refuses direct reading answers a DIFFERENT
//! typed failure when the file is consulted. If the owner-only door still says
//! `OwnerNotRunning` there, the file was not consulted.

use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerServiceTimeouts, OwnerServingState,
    ReadClientTimeouts, ReadFailureKind, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::local_transport::MAX_FRAME_BYTES;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    DurableStoreDamage, prepare_durable_store_damage_for_test,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// How many times a readiness poll may ask before the journey is judged stuck.
///
/// This is a count of asks, not a span of time: the writer either comes up
/// while the poll is asking or it never does, and a bounded count says which
/// without measuring a clock. The bound is far above the handful of asks a
/// starting writer actually needs, so exhausting it is a real defect rather
/// than a slow machine.
const MAX_READINESS_POLLS: usize = 50_000;

/// The typed answers only a door that CONSULTED THE COMMITTED FILE can give.
///
/// This is how the never-touches-the-file promise stays provable now that
/// owner-absent is no longer the one permitted refusal: whatever the owner-only
/// door says about a store it could not reach an owner of, it can never say one
/// of these, because saying one means it hydrated the store it promised to
/// leave alone.
const FILE_ROUTE_DIAGNOSES: [ReadFailureKind; 4] = [
    ReadFailureKind::DirectReadRequiresWriter,
    ReadFailureKind::HeldByReaders,
    ReadFailureKind::HeldByWriter,
    ReadFailureKind::StoreNotFound,
];

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

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> PathBuf {
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

fn session_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: roomy_limits(),
        timeouts: client_timeouts(),
        ..ReadSessionOptions::default()
    }
}

fn owner_options(
    handler: Arc<dyn OwnerRequestHandler>,
    runtime_dir: PathBuf,
) -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions::default();
    let quantum = u64::try_from(MAX_FRAME_BYTES).expect("frame ceiling fits timeout vocabulary");
    options.owner_reads = OwnerReadConfig {
        limits: OwnerReadLimits {
            limits: roomy_limits(),
            concurrency: u64::try_from([ReadRoute::File, ReadRoute::Owner].len())
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
    options
}

fn seed_committed_rows(path: &Path) -> usize {
    let database = Database::open(path).expect("create the file-backed committed fixture");
    database
        .execute(
            "CREATE TABLE owner_only_rows (id INTEGER, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the owner-only fixture table");
    let row_count = [ReadRoute::File, ReadRoute::Owner].len() * 3;
    for id in 0..row_count {
        database
            .execute(
                "INSERT INTO owner_only_rows (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Int64(i64::try_from(id).expect("fixture row identifier")),
                    ),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("owner-only-{id}-{row_count}")),
                    ),
                ]),
            )
            .expect("insert an owner-only fixture row");
    }
    database
        .close()
        .expect("release the idle file so no owner holds it");
    row_count
}

/// Every reader breadcrumb the runtime root currently carries.
///
/// A direct hydration publishes one of these for as long as it holds the
/// committed image, and a writer starting beside it reads exactly this set to
/// decide whether readers are in its way. A door that never opens the file
/// leaves the set empty.
fn reader_breadcrumbs(runtime_root: &Path) -> Vec<PathBuf> {
    fn collect(directory: &Path, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect(&path, found);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "reader")
            {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    collect(runtime_root, &mut found);
    found
}

fn expect_owner_not_running(result: Result<ReadSession>, context: &str) {
    match result {
        Ok(session) => panic!(
            "{context}: the owner-only door opened a {:?}-route session where no owner is running",
            session.route()
        ),
        Err(Error::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerNotRunning,
            "{context}: a store with no owner must be owner-absent, not a file-route diagnosis",
        ),
        Err(other) => panic!("{context}: expected a typed owner-absent refusal, got {other:?}"),
    }
}

#[test]
fn an_owner_only_open_serves_a_live_owner_exactly_as_an_ordinary_open_does() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-only directory");
    let path = directory.path().join("owner-only-live.db");
    let row_count = seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "owner-only-live-runtime");
    let nonce = format!("owner-only:{row_count}:").into_bytes();

    let owner = Database::open_with_options(
        &path,
        owner_options(
            Arc::new(NonceHandler {
                nonce: nonce.clone(),
            }),
            runtime_root.clone(),
        ),
    )
    .expect("start the writable owner beside the committed file");
    assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);

    let statement = "SELECT id, payload FROM owner_only_rows ORDER BY id";
    let challenge = format!("challenge-{row_count}").into_bytes();
    let mut expected_response = nonce;
    expected_response.extend_from_slice(&challenge);

    ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        let ordinary = ReadSession::open_with_options(&path, session_options())
            .expect("a live owner selects the owner route for an ordinary open");
        assert_eq!(ordinary.route(), ReadRoute::Owner);
        let ordinary_result = ordinary
            .execute(statement, &HashMap::new())
            .expect("the ordinary owner-route session answers the statement");
        let ordinary_response = ordinary
            .request_owner("owner-only-proof", &challenge)
            .expect("the ordinary owner-route session reaches the handler");
        drop(ordinary);

        let owner_only = ReadSession::open_owner_only(&path, session_options())
            .expect("a live owner answers the owner-only door");
        let owner_only_result = owner_only
            .execute(statement, &HashMap::new())
            .expect("the owner-only session answers the same statement");
        let owner_only_response = owner_only
            .request_owner("owner-only-proof", &challenge)
            .expect("the owner-only session reaches the same handler");

        assert_eq!(owner_only.route(), ReadRoute::Owner);
        assert_eq!(owner_only_result.columns, ordinary_result.columns);
        assert_eq!(owner_only_result.rows, ordinary_result.rows);
        assert_eq!(owner_only_result.rows.len(), row_count);
        assert_eq!(owner_only_response, expected_response);
        assert_eq!(owner_only_response, ordinary_response);
        assert_eq!(
            ReadSession::owner_status(&path, session_options())
                .expect("the owner reports its own serving state")
                .state,
            OwnerServingState::Serving,
        );
    });

    owner.close().expect("close the owner-only fixture owner");
}

#[test]
fn an_owner_only_open_with_no_owner_leaves_the_committed_file_alone() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner-absent directory");
    let path = directory.path().join("owner-only-absent.db");
    seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "owner-only-absent-runtime");

    ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        expect_owner_not_running(
            ReadSession::open_owner_only(&path, session_options()),
            "an idle store nobody owns",
        );
    });
    assert_eq!(
        reader_breadcrumbs(&runtime_root),
        Vec::<PathBuf>::new(),
        "a door that never opens the file publishes no reader breadcrumb",
    );

    // The writer starting immediately afterwards is the consumer this defect
    // hurt: a door that had hydrated the file would refuse it as reader
    // contention instead of letting it take the store.
    let owner = match Database::open_with_options(
        &path,
        owner_options(
            Arc::new(NonceHandler {
                nonce: b"owner-only-absent:".to_vec(),
            }),
            runtime_root.clone(),
        ),
    ) {
        Ok(owner) => owner,
        Err(Error::ReadFailure(failure)) => panic!(
            "the writer was refused as {:?} after an owner-only ask that must not have touched \
             the file",
            failure.kind()
        ),
        Err(other) => panic!("the writer failed to start: {other:?}"),
    };
    assert_eq!(owner.owner_read_status().state, OwnerServingState::Serving);

    let route = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_owner_only(&path, session_options())
            .expect("the same door reaches the owner once one is there")
            .route()
    });
    assert_eq!(route, ReadRoute::Owner);
    owner.close().expect("close the owner-absent fixture owner");

    // The file was readable the whole time, so the refusal above was this
    // door's route decision and nothing about the store.
    let file_route = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, session_options())
            .expect("an ordinary open still reads the idle committed file")
            .route()
    });
    assert_eq!(file_route, ReadRoute::File);
}

/// A consumer polling for its writer to come up must not cost that writer the
/// store, and must not read the store's file behind its back.
///
/// This journey once also ratified "while the writer is starting, the only
/// answer is owner-absent." That sentence was the defect the owner ruled on
/// (2026-08-27): a writer that has CLAIMED the store but not yet published its
/// serving decision is never reported absent, so owner-absent inside that
/// window is a settled verdict about a store somebody is holding, and it
/// licenses a consumer's absent-store fallback against a live claim. What the
/// pin actually exists to protect -- the poll never touches the committed file
/// and never blocks or delays the writer's startup -- is asserted below
/// directly instead.
#[test]
fn polling_the_owner_only_door_never_stands_in_a_starting_writer_s_way() {
    let directory = tempfile::TempDir::new().expect("task-scoped readiness-poll directory");
    let path = directory.path().join("owner-only-readiness.db");
    seed_committed_rows(&path);
    let runtime_root = secure_runtime_root(&directory, "owner-only-readiness-runtime");

    let (opened_tx, opened_rx) = mpsc::channel::<String>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let writer_path = path.clone();
    let writer_runtime = runtime_root.clone();
    let writer = std::thread::spawn(move || {
        match Database::open_with_options(
            &writer_path,
            owner_options(
                Arc::new(NonceHandler {
                    nonce: b"owner-only-readiness:".to_vec(),
                }),
                writer_runtime,
            ),
        ) {
            Ok(database) => {
                opened_tx
                    .send("opened".to_owned())
                    .expect("report that the writer took the store");
                let _ = release_rx.recv();
                database.close().expect("close the readiness writer");
            }
            Err(error) => {
                opened_tx
                    .send(format!("refused: {error}"))
                    .expect("report that the writer was refused");
            }
        }
    });

    let mut writer_report: Option<String> = None;
    let mut owner_route_answers = 0_usize;
    let mut polls = 0_usize;
    ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        while polls < MAX_READINESS_POLLS {
            polls += 1;
            // The writer's own word is read BEFORE the poll, so what this
            // iteration knows about the writer is true of the poll it is about
            // to make rather than of the one it already made.
            if writer_report.is_none() {
                writer_report = opened_rx.try_recv().ok();
            }
            let writer_is_holding = writer_report.as_deref() == Some("opened");
            match ReadSession::open_owner_only(&path, session_options()) {
                Ok(session) => {
                    assert_eq!(
                        session.route(),
                        ReadRoute::Owner,
                        "a readiness poll must never end up reading the file",
                    );
                    owner_route_answers += 1;
                }
                Err(Error::ReadFailure(failure)) => {
                    // The store this journey seeded was closed before the poll
                    // began, so until the readiness writer claims it there is
                    // genuinely no owner and a refusal is honest. What a poll
                    // may never do is arrive at any answer by consulting the
                    // FILE.
                    assert!(
                        !FILE_ROUTE_DIAGNOSES.contains(&failure.kind()),
                        "a readiness poll answered {:?}, which only a door that opened the \
                         committed file can say",
                        failure.kind(),
                    );
                    // And once the writer has reported that it took the store
                    // and published its decision, ANY refusal is a settled
                    // verdict that is false about a store somebody owns. The
                    // poll owes the writer's real answer here, resolved inside
                    // this caller's own declared connect and routing-retry
                    // deadlines -- one ask, not a longer wait and not a
                    // second chance.
                    assert!(
                        !writer_is_holding,
                        "the writer reported that it holds this store and published its serving \
                         decision, yet the poll answered {:?} -- a settled verdict about a store \
                         that is owned",
                        failure.kind(),
                    );
                }
                Err(other) => panic!("a readiness poll answered {other:?}"),
            }
            if writer_report.is_some() && owner_route_answers > 0 {
                break;
            }
        }
    });
    let _ = release_tx.send(());
    writer.join().expect("join the readiness writer");

    assert_eq!(
        writer_report.as_deref(),
        Some("opened"),
        "polling for readiness must not cost the writer the store",
    );
    assert!(
        owner_route_answers > 0,
        "the poll never reached the owner within {MAX_READINESS_POLLS} asks",
    );
}

#[test]
fn a_store_needing_a_writable_repair_still_answers_owner_absent_from_the_owner_only_door() {
    let directory = tempfile::TempDir::new().expect("task-scoped unreadable-store directory");
    let path = directory.path().join("owner-only-needs-repair.db");
    seed_committed_rows(&path);
    prepare_durable_store_damage_for_test(&path, DurableStoreDamage::NonMonotonicCommitIndex)
        .expect("prepare state whose direct diagnosis requires a writer");
    let runtime_root = secure_runtime_root(&directory, "owner-only-needs-repair-runtime");

    ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        expect_owner_not_running(
            ReadSession::open_owner_only(&path, session_options()),
            "a store whose file needs a writable repair",
        );

        // The contrast is the proof: consulting this file produces a
        // different typed answer, so the owner-only door plainly did not.
        match ReadSession::open_with_options(&path, session_options()) {
            Err(Error::ReadFailure(failure)) => assert_eq!(
                failure.kind(),
                ReadFailureKind::DirectReadRequiresWriter,
                "the fixture must be a store the file route refuses",
            ),
            Ok(session) => panic!(
                "the fixture store was readable on the {:?} route, so it proves nothing",
                session.route()
            ),
            Err(other) => panic!("the fixture store failed in an unexpected way: {other:?}"),
        }
    });
}
