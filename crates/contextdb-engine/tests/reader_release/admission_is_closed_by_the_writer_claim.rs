//! Once a writer has claimed a store, admission is closed behind its claim:
//! a reader or cleaner that arrives afterwards makes no request on the gate,
//! takes no hold, and never enters the protected section, so a stream of
//! arrivals cannot starve the writer. A reader that was already past its
//! precheck when the claim landed is refused at the gate exactly once.
//!
//! Promise (contextdb-intent, Policies 21-23): a writer's wait for starting
//! readers is bounded by the readers that were already inside when it
//! arrived, never by readers that arrive later. The bound is observed on the
//! store's physical admission counters (keyed by the store's device and
//! inode), so a proof reads exact counts and never a duration.
//!
//! Test-estate reason, shared by every function here (2026-10-15, reader/writer
//! handoff): the schedules below prove that admission is closed behind the
//! writer's claim. No earlier test can state them: before this work there is
//! no gate, no claim byte and no counter. The variants are thin wrappers over
//! the two helpers, so the only separate functions are the schedules that
//! differ in who is paused where.

use crate::writer_acquisition_support::{
    Phase, SEEDED_ROWS, WATCHDOG, assert_no_entrant_reached_the_protected_section, count_rows,
    counts, next_event, open_writer, park_readers, queue_a_writer_behind_a_reader, seeded_store,
    spawn_newcomers, watch,
};
use contextdb_core::read_contract::{OwnerServingReason, ReadRoute};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderPausePoint, WriterAdmissionEvent, inspect_companion_record_for_test,
    pause_readers_at_for_test, wait_for_hold_range_refusals_for_test,
};
use contextdb_engine::{DatabaseOpenOptions, ReadSession};
use std::sync::mpsc;

/// `newcomers` readers arrive after the writer's claim and
/// make no request on the gate, take no hold and enter no data-lock section;
/// the writer then opens after the one reader that was inside, and every
/// newcomer is answered by the writer.
fn assert_newcomers_stop_at_the_claim(newcomers: usize, require_stale_draining_record: bool) {
    let queued = queue_a_writer_behind_a_reader("claimed.db");
    if require_stale_draining_record {
        let record = inspect_companion_record_for_test(&queued.path)
            .expect("the previous owner's closing left its companion record");
        assert_eq!(
            record.fields.owner_read_status.reason,
            Some(OwnerServingReason::ShutdownDraining),
            "the fixture must be the stale shutdown-draining handover, or this row proves nothing"
        );
    }
    let arrived = spawn_newcomers(&queued.path, newcomers);
    let before = queued.before;
    let path = queued.path.clone();
    // The newcomers cannot finish before the writer opens: they are behind
    // its claim. Letting the reader go is what lets the whole line move.
    let opened = queued.release_and_open();
    arrived.all_answered_by_the_writer();
    let after_newcomers = counts(&path);
    assert_no_entrant_reached_the_protected_section(before, after_newcomers);
    opened.close();
}

/// Test-estate reason (2026-10-15): a reader that arrives after a writer's claim makes no request
/// on the gate, takes no hold and enters no data-lock section. Today there is no claim byte or gate
/// and the newcomer takes the hold itself.
#[test]
fn a_reader_arriving_after_the_writer_claim_makes_no_gate_or_hold_request() {
    assert_newcomers_stop_at_the_claim(1, false);
}

/// Test-estate reason (2026-10-15): a stream of overlapping arrivals cannot starve a queued writer:
/// none makes a gate request and the writer completes after the readers already inside. Same
/// journey as the newcomer test with more arrivals.
#[test]
fn overlapping_newcomers_while_the_writer_is_queued_make_no_gate_request() {
    assert_newcomers_stop_at_the_claim(4, false);
}

/// Test-estate reason (2026-10-15): a previous owner's stale shutdown-draining record never lets a
/// newcomer take the quiescent route past a writer that holds the claim. Same journey as the
/// newcomer test, with the fixture checked to be that handover.
#[test]
fn a_stale_shutdown_draining_record_does_not_open_the_quiescent_route_past_a_claiming_writer() {
    assert_newcomers_stop_at_the_claim(1, true);
}

/// Test-estate reason (2026-10-15): closing a session that was open before the writer queued, which
/// runs last-reader cleanup, makes no gate or hold request and enters no data-lock section. Through
/// public actions only; it becomes decisive once cleanup takes the shared admission path.
#[test]
fn a_cleaner_arriving_after_the_writer_claim_makes_no_gate_or_hold_request() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "cleaner.db");
    let earlier = ReadSession::open(&path).expect("a reader that arrived before the writer");
    assert_eq!(earlier.route(), ReadRoute::File);
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "behind the parked reader"),
        WriterAdmissionEvent::Waiting
    );
    let before = counts(&path);

    drop(earlier);
    let after_cleaner = counts(&path);
    assert_no_entrant_reached_the_protected_section(before, after_cleaner);

    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader left"),
        WriterAdmissionEvent::Acquired
    );
    writer
        .finish()
        .expect("the writer opens")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): a reader already past its precheck when the writer claims the
/// store is refused at the gate: its range request fails, it never holds the hold and never enters
/// the data-lock section.
#[test]
fn a_reader_paused_after_its_precheck_is_refused_at_the_gate_and_never_holds() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "precheck.db");
    let mut inside = park_readers(&path, Phase::SourceStart, 1);
    let inside = inside.remove(0);
    let precheck_pause = pause_readers_at_for_test(&path, ReaderPausePoint::AfterPrecheck);
    let (tell, heard) = mpsc::channel();
    let reader_path = path.clone();
    let late = std::thread::spawn(move || {
        let _ = tell.send(ReadSession::open(&reader_path));
    });
    let ticket = precheck_pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never finished its precheck");

    let watch = watch(&path);
    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "behind the reader inside the section"),
        WriterAdmissionEvent::Waiting
    );
    let before = counts(&path);

    precheck_pause.release(ticket);
    assert!(
        wait_for_hold_range_refusals_for_test(&path, before.hold_range_refusals + 1, WATCHDOG),
        "the released reader's range request was never refused at the gate"
    );
    let at_refusal = counts(&path);
    assert_eq!(
        at_refusal.hold_shared_grants, before.hold_shared_grants,
        "the refused reader never held the hold"
    );
    assert_eq!(
        at_refusal.data_shared_grants, before.data_shared_grants,
        "the refused reader never entered the data-lock section"
    );

    inside.let_go();
    drop(inside.session());
    assert_eq!(
        next_event(&watch, "after the reader inside left"),
        WriterAdmissionEvent::Acquired
    );
    let database = writer.finish().expect("the writer opens");
    let answer = heard
        .recv_timeout(WATCHDOG)
        .expect("the refused reader never finished");
    late.join().expect("the reader thread finishes");
    crate::writer_acquisition_support::assert_a_late_reader_was_routed_behind_the_writer(
        answer,
        "the reader refused at the gate",
    );
    database.close().expect("close the writer");
}

/// Test-estate reason (2026-10-15): a reader already inside the gate makes the writer wait for the
/// hold, with at most one blocking acquisition per byte, and the writer opens after the reader's
/// data-lock section.
#[test]
fn a_reader_paused_after_the_gate_makes_the_writer_wait_exactly_once_for_the_hold() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "gate.db");
    let gate_pause = pause_readers_at_for_test(&path, ReaderPausePoint::AfterGate);
    let (tell, heard) = mpsc::channel();
    let reader_path = path.clone();
    let reader = std::thread::spawn(move || {
        let _ = tell.send(ReadSession::open(&reader_path));
    });
    let ticket = gate_pause
        .wait_for_paused_reader(WATCHDOG)
        .expect("the reader never passed the gate");

    let watch = watch(&path);
    let before = counts(&path);
    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "while a reader holds the hold"),
        WriterAdmissionEvent::Waiting
    );

    gate_pause.release(ticket);
    let session = heard
        .recv_timeout(WATCHDOG)
        .expect("the reader never finished")
        .expect("the reader that held the hold finishes its acquisition");
    reader.join().expect("the reader thread finishes");
    assert_eq!(
        next_event(&watch, "after the reader's section"),
        WriterAdmissionEvent::Acquired
    );
    let database = writer.finish().expect("the writer opens");
    let after = counts(&path);
    let waits = after.writer_hold_waits - before.writer_hold_waits;
    assert!(
        (1..=2).contains(&waits),
        "the writer waits for the hold (and, if the reader releases the hold before its data \
         lock, once more for the data lock): at most one blocking acquisition per byte, \
         never a repeat; made {waits}"
    );
    assert_eq!(count_rows(&session), SEEDED_ROWS);
    drop(session);
    database.close().expect("close the writer");
}
