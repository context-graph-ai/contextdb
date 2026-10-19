//! A writer that arrives while a direct reader is still acquiring its snapshot
//! waits for that acquisition to end and then opens, as one coordinated
//! operation. Nobody retries, and a reader that arrives in between cannot
//! take the slot.
//!
//! Promise (contextdb-intent, Policies 21-23, and the read contract): an
//! ordinary writer acquisition cannot force the caller to retry, and a reader
//! that has already begun keeps its snapshot. Until now a writable open that
//! met a reader inside its acquisition was refused `held_by_readers`, and the
//! caller wrote its own sleep-and-retry loop around it.
//!
//! Every journey here stops a REAL reader at a named point of its snapshot
//! acquisition and observes the writer through the engine's own admission
//! event record, so nothing sleeps and nothing measures how long anything
//! took. The bounds are failsafes that turn a hang into a message.
//!
//! The reason line below is the test-estate record for every function in
//! this file: each proves a contract no earlier test states, because no
//! earlier test opens a writer while a reader is inside its acquisition and
//! expects success.

use crate::writer_acquisition_support::{
    Expected, Journey, ManualClock, Phase, SEEDED_ROWS, WATCHDOG, assert_writer_opens_during,
    count_rows, next_event, open_writer, park_readers, seeded_store, watch,
};
use contextdb_core::Error;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadFailureDetail, ReadFailureKind};
use contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionEvent;
use contextdb_engine::{DatabaseOpenOptions, ReaderWaitDeadline};
use std::sync::Arc;

/// Test-estate reason (2026-10-15): the writer-acquisition contract: a writer that meets a reader
/// inside its snapshot acquisition waits and opens, a reader that arrives meanwhile is routed
/// behind it, and the first reader keeps its snapshot. Today the writable open is refused
/// `held_by_readers`; no earlier test opens a writer in that window expecting success.
#[test]
fn a_writer_arriving_during_a_reader_s_acquisition_waits_once_and_a_late_reader_routes_behind_it() {
    assert_writer_opens_during(Journey {
        phase: Phase::SourceStart,
        expected: Expected::WaitsForTheReader,
        late_reader: true,
        strip_companion: false,
    });
}

/// Test-estate reason (2026-10-15): a writer completes only after the last of two readers leaves
/// and is not woken by an unrelated release. A single reader cannot express this: the failure it
/// catches (a wake on the first release followed by a refusal) needs two.
#[test]
fn a_writer_completes_only_after_the_last_of_two_readers_leaves() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "two-readers.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 2);
    let second = readers.remove(1);
    let first = readers.remove(0);
    let watch = watch(&path);

    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "with two readers inside their acquisition"),
        WriterAdmissionEvent::Waiting,
        "a writer that meets readers inside their acquisition waits for them"
    );

    // One reader leaves; the other is still inside its section, holding the
    // store. The writer's record must still be exactly "waiting": a wake that
    // ended in a refusal would have recorded one more event.
    first.let_go();
    let first_session = first.session();
    assert_eq!(count_rows(&first_session), SEEDED_ROWS);

    second.let_go();
    let second_session = second.session();
    assert_eq!(
        next_event(&watch, "after the last reader left"),
        WriterAdmissionEvent::Acquired,
        "the writer's next event, after the readers leave, is the open itself: \
         one wait, no refusal in between"
    );
    let database = writer
        .finish()
        .expect("the writer opens after the last reader");
    assert_eq!(count_rows(&second_session), SEEDED_ROWS);
    drop(first_session);
    drop(second_session);
    database.close().expect("close the writer");
}

/// Test-estate reason (2026-10-15): cancelling a waiting writer returns the typed cancellation
/// promptly and leaves no claim, gate or helper behind, with the reader's snapshot intact. Today no
/// wait exists to cancel.
#[test]
fn cancelling_a_waiting_writer_returns_the_cancellation_and_leaves_nothing_held() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "cancelled.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let stop = OwnerReadCancellation::new();
    let writer = open_writer(
        &path,
        DatabaseOpenOptions {
            reader_wait_stop: Some(stop.clone()),
            ..DatabaseOpenOptions::default()
        },
    );
    assert_eq!(
        next_event(&watch, "while a reader is inside its acquisition"),
        WriterAdmissionEvent::Waiting
    );

    stop.cancel();
    match writer.finish() {
        Err(Error::ReadCancelled) => {}
        Err(other) => panic!("a cancelled wait answers the typed cancellation, got {other:?}"),
        Ok(_) => panic!("a cancelled wait opened the store"),
    }

    // Nothing is held: the reader is untouched and still inside its section,
    // and once it leaves, a fresh writer opens at once -- no wait, no refusal.
    reader.let_go();
    let session = reader.session();
    assert_eq!(
        count_rows(&session),
        SEEDED_ROWS,
        "the reader that was acquiring keeps its snapshot through the writer's cancellation"
    );
    let again = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "for the writer that follows the cancelled one"),
        WriterAdmissionEvent::Acquired,
        "the cancelled writer left no claim, no gate and no helper behind: the next \
         writer opens without waiting"
    );
    again
        .finish()
        .expect("the next writer opens")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): a cancellation that arrives after the readers have released and
/// before the open returns is still the typed cancellation, with every lock released. It needs the
/// engine paused at an internal point no public action reaches, so it is its own function.
#[test]
fn cancelling_at_the_linearization_point_returns_the_cancellation_and_releases_everything() {
    use contextdb_engine::persistence::read_persistence_test_scaffold::hold_writer_wait_before_linearization_for_test;

    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "linearized.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let hold = hold_writer_wait_before_linearization_for_test(&path);

    let stop = OwnerReadCancellation::new();
    let writer = open_writer(
        &path,
        DatabaseOpenOptions {
            reader_wait_stop: Some(stop.clone()),
            ..DatabaseOpenOptions::default()
        },
    );
    assert_eq!(
        next_event(&watch, "while a reader is inside its acquisition"),
        WriterAdmissionEvent::Waiting
    );
    reader.let_go();
    let session = reader.session();
    assert!(
        hold.wait_reached(WATCHDOG),
        "the writer's wait returned and reached the linearization point"
    );
    stop.cancel();
    hold.release();
    match writer.finish() {
        Err(Error::ReadCancelled) => {}
        Err(other) => {
            panic!("cancellation at the final check is the typed cancellation: {other:?}")
        }
        Ok(_) => panic!("a writer cancelled before the open returned must not open"),
    }
    assert_eq!(count_rows(&session), SEEDED_ROWS);
    let again = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "for the writer after a cancelled linearization"),
        WriterAdmissionEvent::Acquired,
        "the data lock and every gate byte were released with the cancellation"
    );
    again
        .finish()
        .expect("the next writer opens")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): a caller-supplied deadline bounds the wait, and its expiry is
/// the typed held-by-readers refusal naming the readers. A deadline is a different input with a
/// different answer from cancellation.
#[test]
fn an_expired_caller_deadline_is_the_held_by_readers_refusal_naming_the_readers() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "deadline.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let clock = ManualClock::at(0);
    let writer = open_writer(
        &path,
        DatabaseOpenOptions {
            reader_wait_deadline: Some(ReaderWaitDeadline {
                clock: Arc::new(clock.clone()),
                deadline_ms: 100,
            }),
            ..DatabaseOpenOptions::default()
        },
    );
    assert_eq!(
        next_event(&watch, "before the deadline"),
        WriterAdmissionEvent::Waiting,
        "a deadline in the future is a wait, not a refusal"
    );

    clock.advance_to(100);
    let refusal = match writer.finish() {
        Err(Error::ReadFailure(refusal)) => refusal,
        Err(other) => panic!("an expired deadline is a typed read failure, got {other:?}"),
        Ok(_) => panic!("the deadline expired while a reader still held the store"),
    };
    assert_eq!(refusal.kind(), ReadFailureKind::HeldByReaders);
    match refusal.detail() {
        ReadFailureDetail::HeldByReaders(detail) => assert_eq!(
            detail.observed_direct_readers, 1,
            "the refusal names the one reader that was still inside its acquisition"
        ),
        other => panic!("held_by_readers carries its own detail, got {other:?}"),
    }
    assert_eq!(
        next_event(&watch, "at the deadline"),
        WriterAdmissionEvent::Refused
    );

    reader.let_go();
    drop(reader.session());
}

/// Test-estate reason (2026-10-15): inputs that are already past are answered before any wait
/// begins: an expired deadline is held-by-readers at once and a cancelled token is the typed
/// cancellation at once. Both are checked against one parked reader, so they share a function.
#[test]
fn an_already_expired_deadline_and_an_already_cancelled_token_answer_before_any_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "already.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let past = ManualClock::at(500);
    let expired = open_writer(
        &path,
        DatabaseOpenOptions {
            reader_wait_deadline: Some(ReaderWaitDeadline {
                clock: Arc::new(past),
                deadline_ms: 100,
            }),
            ..DatabaseOpenOptions::default()
        },
    );
    match expired.finish() {
        Err(Error::ReadFailure(refusal)) => {
            assert_eq!(refusal.kind(), ReadFailureKind::HeldByReaders)
        }
        other => panic!("an already-expired deadline is held_by_readers at once: {other:?}"),
    }
    assert_eq!(
        next_event(&watch, "for the already-expired deadline"),
        WriterAdmissionEvent::Refused,
        "no wait began: the first and only event is the refusal"
    );

    let stop = OwnerReadCancellation::new();
    stop.cancel();
    let cancelled = open_writer(
        &path,
        DatabaseOpenOptions {
            reader_wait_stop: Some(stop),
            ..DatabaseOpenOptions::default()
        },
    );
    match cancelled.finish() {
        Err(Error::ReadCancelled) => {}
        other => panic!("an already-cancelled token is the cancellation at once: {other:?}"),
    }

    reader.let_go();
    drop(reader.session());
}

/// Test-estate reason (2026-10-15): decoding the committed image no longer happens inside the
/// protected acquisition section, so a reader paused before its decode does not hold the writer.
/// Same journey as the writer-waits test through the shared helper, with the pause point moved and
/// the expected outcome changed to opening at once.
#[test]
fn a_reader_paused_before_its_decode_does_not_hold_the_writer() {
    assert_writer_opens_during(Journey {
        phase: Phase::BeforeDecode,
        expected: Expected::OpensAtOnce,
        late_reader: true,
        strip_companion: false,
    });
}

/// Test-estate reason (2026-10-15): a reader whose store has no companion file is protected like
/// any other: the writer waits for it, then opens. Same journey as the writer-waits test through
/// the shared helper.
#[test]
fn a_companionless_reader_in_its_section_makes_the_writer_wait_then_open() {
    assert_writer_opens_during(Journey {
        phase: Phase::SourceStart,
        expected: Expected::WaitsForTheReader,
        late_reader: false,
        strip_companion: true,
    });
}

/// Test-estate reason (2026-10-15): a default open, with no stop and no deadline, waits for the
/// readers and then opens. Same journey as the writer-waits test through the shared helper, whose
/// options are the defaults.
#[test]
fn a_default_open_waits_then_opens() {
    assert_writer_opens_during(Journey {
        phase: Phase::SourceStart,
        expected: Expected::WaitsForTheReader,
        late_reader: false,
        strip_companion: false,
    });
}

/// Test-estate reason (2026-10-15): where the platform cannot give the cancellation guarantee, a
/// default open still waits and only a cancellable open is refused with the typed unsupported
/// refusal. macOS only.
#[cfg(target_os = "macos")]
#[test]
fn on_macos_a_default_open_waits_and_a_cancellable_open_is_refused_unsupported() {
    use crate::writer_acquisition_support::is_unsupported_cancellable_wait;

    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "macos.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let cancellable = open_writer(
        &path,
        DatabaseOpenOptions {
            reader_wait_stop: Some(OwnerReadCancellation::new()),
            ..DatabaseOpenOptions::default()
        },
    );
    match cancellable.finish() {
        Err(error) if is_unsupported_cancellable_wait(&error) => {}
        other => panic!("a cancellable wait is unsupported here: {other:?}"),
    }

    let plain = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "for the default open"),
        WriterAdmissionEvent::Waiting
    );
    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader left"),
        WriterAdmissionEvent::Acquired
    );
    plain
        .finish()
        .expect("the default open waited and opened")
        .close()
        .expect("close");
}
