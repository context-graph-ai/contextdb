//! A session whose writer has closed re-selects its route for the next
//! statement. If a replacement writer finishes opening between that
//! re-selection choosing the file and the file open itself, the session is
//! served by the replacement writer within the allowance it started with; it
//! is never refused merely because the new writer won that race.
//!
//! The session's re-selection is held at its direct backend open, the
//! replacement writer opens completely, and the session's clock is moved to
//! the last millisecond of the handover allowance its re-selection started
//! with before the session is let go. Serving it needs no new wait, so a
//! claim-window wait after the release would be a fresh allowance.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

const STARTED_MS: u64 = 10_000;

enum ReaderProgress {
    /// The re-selection reached its direct backend open and is held there.
    AtDirectOpen,
    /// The statement finished, served or refused.
    Finished(Result<Vec<Vec<Value>>, String>),
}

/// Holds the first direct backend open after it is armed; later ones pass.
struct HoldFirstDirectOpen {
    armed: AtomicBool,
    released: AtomicBool,
    claim_waits_after_release: AtomicUsize,
    progress: Mutex<mpsc::Sender<ReaderProgress>>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl ReadSessionTestObserver for HoldFirstDirectOpen {
    fn observe_event(&self, event: ReadSessionEvent) {
        match event {
            ReadSessionEvent::BeforeDirectBackendOpen { .. }
                if self.armed.swap(false, Ordering::SeqCst) =>
            {
                let _ = self
                    .progress
                    .lock()
                    .expect("reader progress sender")
                    .send(ReaderProgress::AtDirectOpen);
                let _ = self.resume.lock().expect("reader resume receiver").recv();
                self.released.store(true, Ordering::SeqCst);
            }
            ReadSessionEvent::ClaimWindowWait { .. } if self.released.load(Ordering::SeqCst) => {
                self.claim_waits_after_release
                    .fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

#[test]
fn a_session_reselecting_after_its_writer_closed_is_served_when_the_replacement_writer_wins_the_direct_open()
 {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let first = Database::open(&path).expect("open the first writer");
    first
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 0..4i64 {
        first
            .execute(
                "INSERT INTO items (id) VALUES ($id)",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("commit row");
    }
    let committed: Vec<_> = (0..4i64).map(|id| vec![Value::Int64(id)]).collect();

    let options = ReadSessionOptions::default();
    let allowance_ms = options.timeouts.routing_retry_ms;
    let clock = ManualDeadlineClock::at(STARTED_MS);
    let (progress, progress_rx) = mpsc::channel();
    let (resume, resume_rx) = mpsc::channel::<()>();
    let observer = Arc::new(HoldFirstDirectOpen {
        armed: AtomicBool::new(false),
        released: AtomicBool::new(false),
        claim_waits_after_release: AtomicUsize::new(0),
        progress: Mutex::new(progress.clone()),
        resume: Mutex::new(resume_rx),
    });
    let session = ReadSession::open_with_observations_for_test(
        &path,
        options,
        Arc::new(clock.clone()),
        Some(Arc::clone(&observer) as Arc<dyn ReadSessionTestObserver>),
        None,
    )
    .expect("open a session beside the first writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .expect("the session reads beside its first writer")
            .rows(),
        committed.as_slice()
    );

    first.close().expect("close the first writer");
    observer.armed.store(true, Ordering::SeqCst);

    let answer = std::thread::scope(|scope| {
        // Dropping `resume` (including by a panic below) lets the reader go.
        let resume = resume;
        let finished = progress;
        let reading = &session;
        // The observer keeps its own progress sender alive, so the reader
        // reports every outcome, a panic included, as its answer; otherwise
        // a panicking statement would leave the receives below waiting.
        scope.spawn(move || {
            let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reading
                    .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
                    .map(|result| result.rows().to_vec())
                    .map_err(|error| error.to_string())
            }))
            .unwrap_or_else(|panic| {
                let text = panic
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "a non-text panic".to_owned());
                Err(format!("the statement panicked: {text}"))
            });
            let _ = finished.send(ReaderProgress::Finished(answer));
        });

        match progress_rx.recv().expect("the reader reports its progress") {
            ReaderProgress::AtDirectOpen => {}
            ReaderProgress::Finished(answer) => panic!(
                "after its writer closed, the session answered without re-selecting its route \
                 through the direct backend open: {answer:?}"
            ),
        }
        let replacement = Database::open(&path).expect("the replacement writer opens completely");
        clock.advance_to(STARTED_MS + allowance_ms - 1);
        resume.send(()).expect("release the reader");
        let answer = match progress_rx.recv().expect("the reader reports its answer") {
            ReaderProgress::Finished(answer) => answer,
            ReaderProgress::AtDirectOpen => unreachable!("only the first direct open is held"),
        };
        (answer, replacement)
    });
    let (answer, replacement) = answer;

    let rows = answer.unwrap_or_else(|refusal| {
        panic!(
            "a session re-selecting after its writer closed must be served when the replacement \
             writer finished opening first, not refused: {refusal}"
        )
    });
    assert_eq!(rows, committed);
    assert_eq!(
        observer.claim_waits_after_release.load(Ordering::SeqCst),
        0,
        "serving through the already-published replacement writer begins no new claim-window wait"
    );
    drop(session);
    replacement.close().expect("close the replacement writer");
}
