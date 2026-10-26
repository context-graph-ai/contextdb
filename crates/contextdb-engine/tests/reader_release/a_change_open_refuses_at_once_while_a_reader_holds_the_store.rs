//! An open that means to CHANGE an existing store answers at once that a
//! reader holds it; it never waits for the reader to leave.
//!
//! The ordinary writable open is one caller: a writer that meets a reader
//! inside its snapshot acquisition waits once and then opens
//! (`a_writer_open_waits_once_for_a_starting_reader`). An existing-only open
//! is a different caller. It is how an operator command edits a deployment it
//! already has, and the operator is owed the typed held-by-readers answer
//! naming who holds the store, so they can ask again once that reader has
//! finished, rather than a command that sits blocked behind it.
//!
//! The reader is a real direct reader stopped at the start of its snapshot
//! acquisition, the state that holds the store. The open is observed through
//! the engine's own admission record, so nothing sleeps and nothing measures
//! how long anything took.

use crate::writer_acquisition_support::{
    Phase, SEEDED_ROWS, count_rows, next_event, open_writer, park_readers, seeded_store, watch,
};
use contextdb_core::Error;
use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureKind};
use contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionEvent;
use contextdb_engine::{DatabaseOpenOptions, OpenDisposition};

fn existing_only() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        open_disposition: OpenDisposition::ExistingOnly,
        ..DatabaseOpenOptions::default()
    }
}

/// Test-estate reason (2026-10-26): an existing-only writable open meeting a reader inside its
/// acquisition refuses at once with the typed held-by-readers answer naming that reader, where the
/// ordinary open waits. No earlier test opens an existing-only writer while a reader holds the
/// store.
#[test]
fn an_existing_only_open_refuses_at_once_while_a_reader_holds_the_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "change-open.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let writer = open_writer(&path, existing_only());
    assert_eq!(
        next_event(
            &watch,
            "for an existing-only open while a reader holds the store"
        ),
        WriterAdmissionEvent::Refused,
        "an open that means to change an existing store answers at once that a reader holds \
         it; it never waits for the reader to leave"
    );
    match writer.finish() {
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
            "an existing-only open meeting a reader is told the store is held by readers, got \
             {other:?}"
        ),
        Ok(_) => panic!("an existing-only open opened a store a reader was holding"),
    }

    let session = reader.session();
    assert_eq!(
        count_rows(&session),
        SEEDED_ROWS,
        "the reader that held the store keeps its snapshot"
    );
    let database = open_writer(&path, existing_only())
        .finish()
        .expect("once the reader has finished acquiring, the same open changes the store");
    drop(session);
    database.close().expect("close the writer");
}
