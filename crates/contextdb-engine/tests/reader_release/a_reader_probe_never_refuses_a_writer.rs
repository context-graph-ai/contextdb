//! A reader that is only looking at who owns a store never makes a writer
//! open fail.
//!
//! Promise (contextdb-intent, Policies 21-23): a writer is never refused
//! because a reader is observing the store; observation reads and never
//! locks anything a writer needs. The defect this pins: a reader's probe of
//! the writer's presence takes a shared lock on the companion for the span
//! between its probe and its decision, and a writer opening in that span is
//! refused `held_by_writer`, though only a reader was looking.
//!
//! Test-estate reason (2026-10-15): the found defect. No existing
//! test catches it, because every existing proof lets the reader finish its
//! probe before the writer opens; this one holds the reader exactly between
//! its probe and its decision while the writer opens. It cannot be a row of
//! another journey: no other schedule parks a reader at that point.

use crate::writer_acquisition_support::{
    WATCHDOG, count_rows, next_event, open_writer, seeded_store, watch,
};
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderPausePoint, WriterAdmissionEvent, pause_readers_at_for_test,
};
use contextdb_engine::{DatabaseOpenOptions, ReadSession};
use std::sync::mpsc;

/// Test-estate reason (2026-10-15): a reader that is only observing who owns the store never makes
/// a writer open fail. Today the probe holds a shared lock on the companion between its probe and
/// its decision, and a writer that opens in that span is refused held-by-writer. No earlier test
/// parks a reader at that point.
#[test]
fn a_reader_paused_between_its_probe_and_its_decision_does_not_refuse_a_writer() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "probed.db");
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::AfterProbe);
    let (tell, heard) = mpsc::channel();
    let reader_path = path.clone();
    let reader = std::thread::spawn(move || {
        let _ = tell.send(ReadSession::open(&reader_path));
    });
    let ticket = pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never finished its probe");

    let watch = watch(&path);
    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "while a reader sits between probe and decision"),
        WriterAdmissionEvent::Acquired,
        "a reader that is only observing the store neither makes the writer wait nor \
         refuses it: the writer opens first time"
    );
    let database = writer.finish().expect("the writer opens");

    pause.release(ticket);
    let session = heard
        .recv_timeout(WATCHDOG)
        .expect("the probing reader never finished")
        .expect("the reader decides after the writer opened and is answered by it");
    reader.join().expect("the reader thread finishes");
    assert_eq!(session.route(), ReadRoute::Owner);
    assert_eq!(
        count_rows(&session),
        crate::writer_acquisition_support::SEEDED_ROWS
    );
    drop(session);
    database.close().expect("close the writer");
}
