//! A signal caught by the caller's own handler never turns a default writer
//! wait into an error and never makes the engine retry.
//!
//! Promise (contextdb-intent, Policies 21-23): a default open, which supplies
//! no stop and no deadline, waits until the store is handed over and then
//! opens. A process that installs an ordinary signal handler without
//! restart semantics -- the usual case for a supervisor -- must not see that
//! wait come back as an interrupted-call failure, and the engine must not
//! answer an interruption by looping: the wait makes at most one blocking
//! acquisition per byte it needs. The handler is deferred until the acquisition ends, and then
//! it runs.
//!
//! Test-estate reason (2026-10-15, reader/writer handoff, signal discipline
//! of the default wait): no earlier test interrupts a blocked lock wait, so
//! nothing catches an `EINTR` that escapes as a failure or is retried in a
//! loop. The oracle: the open succeeds, the blocking acquisitions stay within one
//! per byte (`writer_hold_waits` moves by one or two, never more), and the
//! handler ran exactly once. It cannot be a row of the journey tests, whose
//! writers are never signalled. Unix, Linux-and-macOS signal semantics.

use crate::writer_acquisition_support::{
    Phase, counts, next_event, park_readers, seeded_store, watch,
};
use contextdb_engine::DatabaseOpenOptions;
use contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionEvent;
use std::sync::atomic::{AtomicU64, Ordering};

static HANDLER_RUNS: AtomicU64 = AtomicU64::new(0);

extern "C" fn count_the_signal(_: libc::c_int) {
    HANDLER_RUNS.fetch_add(1, Ordering::SeqCst);
}

/// Test-estate reason (2026-10-15): a caught signal does not interrupt the default wait: the open
/// succeeds, the blocking acquisitions stay within one per byte, and the caller's handler runs
/// once. No earlier test interrupts a blocked lock wait.
#[test]
fn a_caught_signal_neither_fails_nor_repeats_a_default_writer_wait() {
    // A handler installed WITHOUT SA_RESTART: a blocked lock wait it
    // interrupts would return EINTR.
    // SAFETY: installs a plain counting handler for SIGUSR1 in this process.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = count_the_signal as *const () as usize;
        action.sa_flags = 0;
        libc::sigemptyset(&mut action.sa_mask);
        assert_eq!(
            libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut()),
            0
        );
    }

    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "signalled.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let before = counts(&path);

    // The writer runs on its own thread; the proof signals exactly that
    // thread once it is waiting.
    let (thread_id_tx, thread_id_rx) = std::sync::mpsc::channel();
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        // SAFETY: reads this thread's own identifier.
        thread_id_tx
            .send(unsafe { libc::pthread_self() })
            .expect("announce the writer thread");
        contextdb_engine::Database::open_with_options(&writer_path, DatabaseOpenOptions::default())
    });
    let writer_thread = thread_id_rx.recv().expect("the writer thread's identifier");
    assert_eq!(
        next_event(&watch, "while a reader is inside its acquisition"),
        WriterAdmissionEvent::Waiting
    );

    // SAFETY: signals a thread of this process that is still running.
    assert_eq!(
        unsafe { libc::pthread_kill(writer_thread, libc::SIGUSR1) },
        0
    );

    reader.let_go();
    drop(reader.session());
    let database = writer
        .join()
        .expect("the writer thread finishes")
        .expect("a caught signal does not turn the default wait into a failure");
    assert_eq!(
        next_event(&watch, "after the reader left"),
        WriterAdmissionEvent::Acquired
    );
    let waits = counts(&path).writer_hold_waits - before.writer_hold_waits;
    assert!(
        (1..=2).contains(&waits),
        "the wait made at most one blocking acquisition per byte (the hold, then the data lock): \
         an interruption is deferred, never retried; made {waits}"
    );
    assert_eq!(
        HANDLER_RUNS.load(Ordering::SeqCst),
        1,
        "the caller's handler runs once, after the acquisition ends"
    );
    database.close().expect("close the writer");
}
