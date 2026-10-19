//! A format check that is in the middle of its table scan holds up nobody else.
//!
//! Promise (contextdb-intent, Policies 21-23): classifying a store's format is a
//! registered read whose table scan happens after the reader hold and the data
//! lock are released. While one check sits in that scan, a reader that starts on
//! the same store goes through admission, a writer that queues behind that
//! reader is granted as soon as the reader leaves its protected section, and the
//! same holds for a reader and writer on a different store. The scan is not
//! serialized with anything outside itself: no process-wide lock is held across
//! it.
//!
//! Test-estate reason, shared by both functions here (2026-10-15): the check's
//! scan used a process-wide hook lock that a second reader's admission also took,
//! so a check paused in its scan blocked every other reader in the process. No
//! earlier test parks a check inside its scan and then drives a second reader and a
//! writer past it. The two functions differ only in whether the reader and writer
//! use the checked store or another one.

use crate::writer_acquisition_support::{
    Phase, WATCHDOG, next_event, open_writer, park_readers, seeded_store, watch,
};
use contextdb_engine::DatabaseOpenOptions;
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    WriterAdmissionEvent, pause_next_classification_scan_for_test,
};
use std::path::Path;
use std::sync::mpsc::TryRecvError;

fn assert_a_paused_classification_blocks_nobody(checked: &Path, other: &Path) {
    // The next format check in this process stops at the start of its table scan,
    // after its admission.
    let pause = pause_next_classification_scan_for_test();
    let checked_path = checked.to_path_buf();
    let (tell, classified) = std::sync::mpsc::channel();
    let classifier = std::thread::spawn(move || {
        let _ = tell.send(RedbPersistence::is_legacy_format_store(&checked_path));
    });
    assert!(
        pause.wait_until_paused(WATCHDOG),
        "the format check never reached its table scan"
    );

    // A reader of the other (or the same) store goes through admission and stops
    // inside it, then a writer queues behind it.
    let mut readers = park_readers(other, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(other);
    let writer = open_writer(other, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(
            &watch,
            "behind a reader while a format check is paused in its scan"
        ),
        WriterAdmissionEvent::Waiting,
        "the writer queues behind the reader"
    );

    // The reader leaves its protected section; the writer is granted while the
    // format check is still paused.
    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader left"),
        WriterAdmissionEvent::Acquired,
        "the writer is granted while the format check is still inside its scan"
    );
    let database = writer.finish().expect("the writer opens");
    assert!(
        matches!(classified.try_recv(), Err(TryRecvError::Empty)),
        "the format check is still paused in its scan"
    );

    pause.let_go();
    let _verdict = classified
        .recv_timeout(WATCHDOG)
        .expect("the format check finishes once let go");
    classifier.join().expect("the classifier thread finishes");
    database.close().expect("close the writer");
}

#[test]
fn a_paused_format_check_does_not_hold_up_a_reader_and_writer_of_the_same_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let store = seeded_store(directory.path(), "classified-same.db");
    assert_a_paused_classification_blocks_nobody(&store, &store);
}

#[test]
fn a_paused_format_check_does_not_hold_up_a_reader_and_writer_of_a_different_store() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let checked = seeded_store(directory.path(), "classified-checked.db");
    let other = seeded_store(directory.path(), "classified-other.db");
    assert_a_paused_classification_blocks_nobody(&checked, &other);
}
