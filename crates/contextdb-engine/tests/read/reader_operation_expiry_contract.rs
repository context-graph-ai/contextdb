//! The clock expires an operation; no declaration expires a whole session.
use super::reader_proof_support as fixture;
use contextdb_core::read_contract::{
    CursorExpiryKind, DeadlineClock, ReadFailure, ReadFailureDetail, ReadFailureKind,
    ReadFailureLimit, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSourceEvent, ReadKernelTestObserver,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadCancellation, OwnerReadConfig, OwnerReadTestHooks,
    ReadSession,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug)]
enum Operation {
    Statement,
    IdleCursor,
    LifetimeCursor,
}
struct AdvanceOnce {
    clock: Arc<ManualDeadlineClock>,
    armed: AtomicBool,
}
impl ReadKernelTestObserver for AdvanceOnce {
    fn before_source_touch(&self, _: ReadKernelSourceEvent, _: OwnerReadCancellation) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.clock.advance_to(self.clock.now_ms() + 21);
        }
    }
    fn cancellation_observed(&self, _: ReadKernelCancellationEvent, _: OwnerReadCancellation) {}
}
fn failure<T>(result: contextdb_core::Result<T>) -> ReadFailure {
    match result {
        Err(Error::ReadFailure(failure)) => failure,
        Err(other) => panic!("operation expiry must keep its typed refusal, got {other:?}"),
        Ok(_) => panic!("an operation beyond its configured limit cannot succeed"),
    }
}
fn exercise(route: ReadRoute, operation: Operation) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("expiry.db");
    let runtime = fixture::runtime(root.path());
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE expiry_rows (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..8 {
        db.execute(
            "INSERT INTO expiry_rows VALUES ($id, 'original')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
    let clock = Arc::new(ManualDeadlineClock::default());
    let advance = Arc::new(AdvanceOnce {
        clock: clock.clone(),
        armed: AtomicBool::new(false),
    });
    let (drain_tx, _drain_rx) = std::sync::mpsc::channel();
    let owner = (route == ReadRoute::Owner).then(|| {
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    runtime_dir: Some(runtime.clone()),
                    test_hooks: Some(OwnerReadTestHooks {
                        clock: clock.clone(),
                        drain_started: drain_tx,
                        active_request_registered: None,
                        accept_thread_exited: None,
                        accept_thread_joined: None,
                        service_dropped: None,
                        quiescent_stop: None,
                    }),
                    ..Default::default()
                },
                test_kernel_observer: Some(advance.clone()),
                ..Default::default()
            },
        )
        .unwrap()
    });
    let mut options = fixture::options();
    options.limits.active_ms = 20;
    options.limits.cursor_idle_ms = 40;
    options.limits.cursor_lifetime_ms = 100;
    options.limits.cursor_page_rows = 1;
    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_observations_for_test(
            &path,
            options,
            clock.clone(),
            None,
            Some(advance.clone()),
        )
    })
    .unwrap();
    assert_eq!(reader.route(), route);
    let snapshot = reader.file_snapshot_for_test();
    let baseline = {
        let fixture_answer_29 = reader
        .execute(
            "SELECT id, payload FROM expiry_rows ORDER BY id",
            &HashMap::new(),
        )
        .unwrap();
        fixture_answer_29.rows().to_vec()
    };
    match operation {
        Operation::Statement => {
            advance.armed.store(true, Ordering::SeqCst);
            let refusal = failure(reader.execute(
                "SELECT id, payload FROM expiry_rows ORDER BY id",
                &HashMap::new(),
            ));
            assert!(
                !advance.armed.load(Ordering::SeqCst),
                "the real kernel reached the clock advance"
            );
            assert_eq!(refusal.kind(), ReadFailureKind::OwnerLimitExceeded);
            let ReadFailureDetail::OwnerLimitExceeded(detail) = refusal.detail() else {
                panic!("typed active-time detail")
            };
            assert_eq!(detail.limit, ReadFailureLimit::ActiveMs);
            assert_eq!(detail.value, 20);
        }
        Operation::IdleCursor | Operation::LifetimeCursor => {
            let mut cursor = reader
                .open_cursor(
                    "SELECT id, payload FROM expiry_rows ORDER BY id",
                    &HashMap::new(),
                )
                .unwrap();
            assert!(cursor.first_page().has_more());
            let file_resources = cursor.file_resource_witness_for_test().unwrap();
            if route == ReadRoute::File {
                let held = file_resources
                    .as_ref()
                    .expect("actual file cursor resources are exposed");
                assert!(held.snapshot_is_retained() && held.memory_is_retained());
            }
            assert_eq!(reader.route_resources_for_test().unwrap().active_cursors, 1);
            let expiry = match operation {
                Operation::IdleCursor => {
                    clock.advance_to(41);
                    CursorExpiryKind::Idle
                }
                Operation::LifetimeCursor => {
                    // Each fetch resets idle age. At 101ms the last fetch was
                    // 11ms ago; only the 100ms lifetime has expired.
                    for ms in [30, 60, 90] {
                        clock.advance_to(ms);
                        assert!(
                            cursor
                                .fetch(std::num::NonZeroUsize::new(1))
                                .unwrap()
                                .has_more()
                        );
                    }
                    clock.advance_to(101);
                    CursorExpiryKind::Lifetime
                }
                Operation::Statement => unreachable!(),
            };
            assert_eq!(
                failure(cursor.fetch(std::num::NonZeroUsize::new(1))),
                ReadFailure::cursor_expired(expiry)
            );
            if let Some(held) = file_resources {
                assert!(
                    !held.snapshot_is_retained(),
                    "expired cursor releases its actual snapshot hold before handle drop"
                );
                assert!(
                    !held.memory_is_retained(),
                    "expired cursor releases its actual memory owner before handle drop"
                );
            }
            assert!(
                !cursor.is_live(),
                "expired operation releases its retained backend continuation"
            );
            assert_eq!(
                reader.route_resources_for_test().unwrap().active_cursors,
                0,
                "expiry returns the operation slot before handle drop"
            );
            drop(cursor);
        }
    }
    assert_eq!(
        reader
            .execute(
                "SELECT id, payload FROM expiry_rows ORDER BY id",
                &HashMap::new()
            )
            .unwrap()
            .rows(),
        baseline,
        "the same live session accepts a later valid statement after {operation:?} on {route:?}"
    );
    assert_eq!(reader.file_snapshot_for_test(), snapshot);
    // No idle/session lifetime exists. This is not an operation in flight.
    clock.advance_to(1_000_000);
    assert_eq!(
        reader
            .execute(
                "SELECT id, payload FROM expiry_rows ORDER BY id",
                &HashMap::new()
            )
            .unwrap()
            .rows(),
        baseline
    );
    assert_eq!(reader.file_snapshot_for_test(), snapshot);
    drop(reader);
    if let Some(owner) = owner {
        owner.close().unwrap();
    }
}
#[test]
fn file_read_limits_expire_the_operation_without_expiring_the_session() {
    exercise(ReadRoute::File, Operation::Statement);
}
#[test]
fn file_cursor_idle_expiry_preserves_the_session() {
    exercise(ReadRoute::File, Operation::IdleCursor);
}
#[test]
fn file_cursor_lifetime_expiry_preserves_the_session() {
    exercise(ReadRoute::File, Operation::LifetimeCursor);
}
#[test]
fn owner_active_statement_expiry_preserves_the_session() {
    exercise(ReadRoute::Owner, Operation::Statement);
}
#[test]
fn owner_cursor_idle_expiry_preserves_the_session() {
    exercise(ReadRoute::Owner, Operation::IdleCursor);
}
#[test]
fn owner_cursor_lifetime_expiry_preserves_the_session() {
    exercise(ReadRoute::Owner, Operation::LifetimeCursor);
}
