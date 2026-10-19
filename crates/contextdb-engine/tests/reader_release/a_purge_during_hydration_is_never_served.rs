//! An authoritative erasure that lands while a reader is still building its
//! snapshot is never served to that reader.
//!
//! Promise (contextdb-intent, authoritative erasure): once a PURGE has
//! committed, no reader serves the erased rows -- not even a reader that began
//! before the PURGE and was caught mid-acquisition. The reader is refused with
//! the typed authoritative-erasure failure (at open or on its first read of
//! the erased table); it is never answered with the pre-purge rows. A PURGE
//! that committed BEFORE the reader captured its snapshot is no refusal at
//! all: that reader sees the post-purge store.
//!
//! Test-estate reason, shared by every function here (2026-10-15, reader/writer
//! handoff): the decode no longer runs under the data lock, so a PURGE can now
//! interleave with hydration, which it could not before. While the decode is
//! still under the data lock these journeys stop at the writer's refusal and
//! pin nothing further. The decisive mutant is the removal of the fence
//! re-baseline in the reader's admission: with it removed, the decode journeys
//! serve the erased rows. The cases are rows of one helper: the schedules
//! differ only in the store's companion, the hydration path and the order of
//! capture and PURGE.

use crate::writer_acquisition_support::{
    Phase, SEEDED_ROWS, count_rows, next_event, open_writer, park_readers, seeded_store, watch,
};
use contextdb_core::Error;
use contextdb_core::read_contract::ReadFailureKind;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    WriterAdmissionEvent, force_complete_hydration_for_test, leave_next_purge_pending_for_test,
};
use contextdb_engine::{DatabaseOpenOptions, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

fn is_erasure(error: &Error) -> bool {
    matches!(
        error,
        Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::AuthoritativeErasure
    )
}

fn is_purge_coordination_refusal(error: &Error) -> bool {
    matches!(
        error,
        Error::ReaderCoordinationUnavailable { operation, .. } if operation == "authoritative purge"
    )
}

/// The reader is stopped before it decodes its image; a writer opens and
/// attempts a PURGE; the reader is let go. It must never serve an erased row.
fn assert_a_purge_during_decode_is_never_served(strip_companion: bool, fallback: bool) {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "purged.db");
    if strip_companion {
        let _ = std::fs::remove_file(contextdb_core::store_companion_path(&path));
    }
    // While held, direct readers of this store decode the complete image (the
    // established fallback) instead of loading rows lazily.
    let _complete = fallback.then(|| force_complete_hydration_for_test(&path));
    let mut readers = park_readers(&path, Phase::BeforeDecode, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "while a reader is only about to decode"),
        WriterAdmissionEvent::Acquired,
        "a reader that is about to decode holds nothing a writer waits for"
    );
    let database = writer.finish().expect("the writer opens");
    // A PURGE that meets a reader still hydrating is either refused with a
    // typed refusal (the inventory is not certain yet) or it commits; in
    // neither case may the reader be served the erased rows afterwards.
    let purge = database.execute("PURGE FROM held WHERE id = 1", &HashMap::new());
    let purged = match &purge {
        Ok(_) => true,
        Err(error) => {
            assert!(
                is_erasure(error) || is_purge_coordination_refusal(error),
                "a PURGE that cannot be accounted for is refused with a typed purge refusal, \
                 got {error:?}"
            );
            false
        }
    };

    match reader.outcome() {
        Err(error) => assert!(
            purged && is_erasure(&error),
            "a reader is refused only because an erasure committed, and with the erasure failure; \
             purged={purged}, got {error:?}"
        ),
        Ok(session) => match session.execute("SELECT id FROM held", &HashMap::new()) {
            Err(error) => assert!(
                purged && is_erasure(&error),
                "purged={purged}: typed erasure refusal only after a committed purge, got {error:?}"
            ),
            Ok(answer) => assert!(
                !purged,
                "a reader that began before a committed purge served the erased table: {:?}",
                answer.rows
            ),
        },
    }
    database.close().expect("close the writer");
}

/// Test-estate reason (2026-10-15): a committed erasure is never served to a reader that began
/// before it: the reader is refused with the erasure failure, never answered with the pre-purge
/// rows. A purge refused while the reader is still hydrating is also acceptable. Pins behaviour
/// that becomes reachable once the decode leaves the protected section.
#[test]
fn a_purge_during_the_reader_s_decode_is_never_served() {
    assert_a_purge_during_decode_is_never_served(false, false);
}

/// Test-estate reason (2026-10-15): the same on a store whose companion file does not exist.
#[test]
fn a_purge_during_the_decode_of_a_companionless_reader_is_never_served() {
    assert_a_purge_during_decode_is_never_served(true, false);
}

/// Test-estate reason (2026-10-15): the same on the complete-image fallback.
#[test]
fn a_purge_during_the_fallback_decode_is_never_served() {
    assert_a_purge_during_decode_is_never_served(false, true);
}

/// Test-estate reason (2026-10-15): a reader registered in both the default root and a supplied
/// root is covered by a purge that lands during its decode.
#[test]
fn a_purge_during_the_decode_of_a_reader_publishing_in_two_roots_is_never_served() {
    use crate::writer_acquisition_support::pause_at;
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "two-roots.db");
    let root = directory.path().join("supplied-root");
    std::fs::create_dir(&root).expect("supplied root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the supplied root");
    let pause = pause_at(&path, Phase::BeforeDecode);
    let (tell, heard) = std::sync::mpsc::channel();
    let reader_path = path.clone();
    let reader = std::thread::spawn(move || {
        let _ = tell.send(ReadSession::open_in_runtime_dir(
            &reader_path,
            ReadSessionOptions::default(),
            Some(root),
        ));
    });
    let ticket = pause
        .wait_for_paused_reader(crate::writer_acquisition_support::WATCHDOG)
        .expect("the two-root reader never reached its decode");

    let database = contextdb_engine::Database::open(&path).expect("the writer opens");
    let purge = database.execute("PURGE FROM held WHERE id = 1", &HashMap::new());
    let purged = match &purge {
        Ok(_) => true,
        Err(error) => {
            assert!(
                is_erasure(error) || is_purge_coordination_refusal(error),
                "a PURGE that cannot be accounted for is refused with a typed purge refusal, \
                 got {error:?}"
            );
            false
        }
    };
    pause.release(ticket);
    let outcome = heard
        .recv_timeout(crate::writer_acquisition_support::WATCHDOG)
        .expect("the two-root reader never finished");
    reader.join().expect("the reader thread finishes");
    match outcome {
        Err(error) => assert!(
            purged && is_erasure(&error),
            "refused only because an erasure committed, with the erasure failure; purged={purged}, got {error:?}"
        ),
        Ok(session) => match session.execute("SELECT id FROM held", &HashMap::new()) {
            Err(error) => assert!(purged && is_erasure(&error), "purged={purged}: {error:?}"),
            Ok(answer) => assert!(
                !purged,
                "a two-root reader served the erased table after a committed purge: {:?}",
                answer.rows
            ),
        },
    }
    database.close().expect("close the writer");
}

/// Test-estate reason (2026-10-15): a purge stopped before it decided fails closed: a reader caught
/// by it is refused and never served, and the next writer open decides the purge one way or the
/// other.
#[test]
fn a_purge_left_pending_during_the_decode_fails_closed_and_recovery_decides() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "pending.db");
    let mut readers = park_readers(&path, Phase::BeforeDecode, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let writer = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "while a reader is only about to decode"),
        WriterAdmissionEvent::Acquired
    );
    let database = writer.finish().expect("the writer opens");
    {
        // Thread-local: armed on the thread that runs the PURGE.
        let _pending = leave_next_purge_pending_for_test();
        database
            .execute("PURGE FROM held WHERE id = 1", &HashMap::new())
            .expect_err("a purge stopped with its fence Pending returns an error");
    }

    match reader.outcome() {
        Err(error) => assert!(
            is_erasure(&error)
                || is_purge_coordination_refusal(&error)
                || matches!(&error, Error::ReadFailure(_)),
            "a reader caught by a Pending purge is refused with a typed failure, got {error:?}"
        ),
        Ok(session) => assert!(
            session
                .execute("SELECT id FROM held", &HashMap::new())
                .is_err(),
            "a Pending purge fails closed: the reader must not be served"
        ),
    }
    drop(database);

    // Recovery at the next writer open decides the purge; afterwards the
    // store is consistent: three rows (aborted) or two (committed).
    let recovered = contextdb_engine::Database::open(&path).expect("recovery opens the store");
    recovered.close().expect("close the recovered store");
    let session = ReadSession::open(&path).expect("a reader after recovery");
    let rows = count_rows(&session);
    assert!(
        rows == SEEDED_ROWS || rows == SEEDED_ROWS - 1,
        "recovery decided the purge one way or the other, got {rows} rows"
    );
}

/// Test-estate reason (2026-10-15): a reader answered by a serving writer is never served erased
/// rows after that writer closes and drains and its successor commits a purge.
#[test]
fn a_purge_after_an_owner_drain_is_never_served_to_the_owner_route_reader() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "drain.db");
    let first = contextdb_engine::Database::open(&path).expect("the first writer serves reads");
    let session = ReadSession::open(&path).expect("a reader through the owner route");
    assert_eq!(
        session.route(),
        contextdb_core::read_contract::ReadRoute::Owner,
        "a serving writer answers readers itself"
    );
    assert_eq!(count_rows(&session), SEEDED_ROWS);
    first.close().expect("the first writer closes and drains");

    let second = contextdb_engine::Database::open(&path).expect("the next writer opens");
    second
        .execute("PURGE FROM held WHERE id = 1", &HashMap::new())
        .expect("the authoritative purge commits");
    match session.execute("SELECT id FROM held", &HashMap::new()) {
        Err(error) => assert!(
            is_erasure(&error)
                || matches!(
                    &error,
                    Error::ReadFailure(failure)
                        if failure.kind() == ReadFailureKind::OwnerNotServing
                ),
            "after the purge the drained reader is refused (the erasure failure, or the \
             drained owner no longer serving), got {error:?}"
        ),
        Ok(answer) => panic!(
            "the reader served the erased table after a committed purge across a drain: {:?}",
            answer.rows
        ),
    }
    second.close().expect("close the second writer");
}

/// Test-estate reason (2026-10-15): a purge that committed before the reader captured its snapshot
/// is no refusal: the reader sees the post-purge store.
#[test]
fn a_purge_before_the_capture_is_no_refusal() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "purged-before.db");
    let writer = contextdb_engine::Database::open(&path).expect("the writer opens");
    writer
        .execute("PURGE FROM held WHERE id = 1", &HashMap::new())
        .expect("the authoritative purge commits");
    writer.close().expect("close the writer");

    let session = ReadSession::open(&path).expect("a reader after the purge is not refused");
    assert_eq!(
        count_rows(&session),
        SEEDED_ROWS - 1,
        "a reader that captured after the purge sees the post-purge store"
    );
}
