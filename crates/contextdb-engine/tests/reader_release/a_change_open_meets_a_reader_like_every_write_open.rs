//! An open that means to CHANGE an existing store meets a reader the way
//! every writable open does: it waits once for the reader to finish its
//! acquisition, and only the caller's own deadline ends that wait early. A
//! caller that wants the answer at once passes a deadline that has already
//! passed, and gets the typed held-by-readers refusal naming the reader, so
//! an operator command can say "somebody is reading this store, here is who"
//! without a retry loop of its own.
//!
//! The reader is a real direct reader stopped at the start of its snapshot
//! acquisition, the state that holds the store. The opens are observed
//! through the engine's own admission record, and the deadline is read from a
//! manual clock, so nothing sleeps and nothing measures how long anything
//! took.

use crate::writer_acquisition_support::{
    ManualClock, Phase, SEEDED_ROWS, count_rows, next_event, open_writer, park_readers,
    seeded_store, watch,
};
use contextdb_core::Error;
use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureKind};
use contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionEvent;
use contextdb_engine::{DatabaseOpenOptions, OpenDisposition, ReaderWaitDeadline};
use std::sync::Arc;

/// Test-estate reason (2026-10-26): an existing-only writable open follows the writer-acquisition
/// contract: with an already-expired caller deadline it refuses at once with held-by-readers naming
/// the reader, and with no deadline it waits once and opens. Both are checked against one parked
/// reader, so they share a function.
#[test]
fn an_existing_only_open_refuses_at_once_on_an_expired_deadline_and_otherwise_waits_once() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "change-open.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let expired = open_writer(
        &path,
        DatabaseOpenOptions {
            open_disposition: OpenDisposition::ExistingOnly,
            reader_wait_deadline: Some(ReaderWaitDeadline {
                clock: Arc::new(ManualClock::at(500)),
                deadline_ms: 100,
            }),
            ..DatabaseOpenOptions::default()
        },
    );
    match expired.finish() {
        Err(Error::ReadFailure(failure)) => {
            assert_eq!(failure.kind(), ReadFailureKind::HeldByReaders);
            match failure.detail() {
                ReadFailureDetail::HeldByReaders(detail) => {
                    assert!(
                        detail.observed_direct_readers >= 1,
                        "the refusal counts the reader holding the store: {detail:?}"
                    );
                    assert!(
                        detail
                            .verified_readers
                            .iter()
                            .any(|reader| reader.process_id == std::process::id()),
                        "the refusal names the process whose reader holds the store: {detail:?}"
                    );
                }
                other => panic!("a held-by-readers refusal carries its readers, got {other:?}"),
            }
        }
        Err(other) => panic!(
            "an existing-only open whose deadline has already passed is told the store is held \
             by readers, got {other:?}"
        ),
        Ok(_) => panic!("an existing-only open opened a store a reader was holding"),
    }
    assert_eq!(
        next_event(
            &watch,
            "for the existing-only open whose deadline has passed"
        ),
        WriterAdmissionEvent::Refused,
        "an already-expired deadline is answered before any wait begins"
    );

    let waiting = open_writer(
        &path,
        DatabaseOpenOptions {
            open_disposition: OpenDisposition::ExistingOnly,
            ..DatabaseOpenOptions::default()
        },
    );
    assert_eq!(
        next_event(&watch, "for the existing-only open with no deadline"),
        WriterAdmissionEvent::Waiting,
        "with no deadline of its own, a change-open waits for the reader like every write open"
    );

    let session = reader.session();
    assert_eq!(
        next_event(&watch, "after the reader finished its acquisition"),
        WriterAdmissionEvent::Acquired,
        "the waiting open's next event is the open itself: one wait, no refusal in between"
    );
    let database = waiting
        .finish()
        .expect("the change-open opens once the reader has finished acquiring");
    assert_eq!(
        count_rows(&session),
        SEEDED_ROWS,
        "the reader that held the store keeps its snapshot"
    );
    drop(session);
    database.close().expect("close the writer");
}
