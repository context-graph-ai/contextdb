use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::sync::{Arc, mpsc};

struct UnexpiringClock;
impl DeadlineClock for UnexpiringClock {
    fn now_ms(&self) -> u64 {
        1
    }
    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn an_already_serving_owner_is_selected_before_its_writable_open_returns() {
    struct OwnerReady {
        reached: mpsc::Sender<()>,
        resume: std::sync::Mutex<mpsc::Receiver<()>>,
    }
    impl ReadSessionTestObserver for OwnerReady {
        fn observe_event(&self, event: ReadSessionEvent) {
            if event == ReadSessionEvent::OwnerServiceStarted {
                self.reached.send(()).unwrap();
                self.resume.lock().unwrap().recv().unwrap();
            }
        }
    }
    struct ServingRoute;
    impl ReadSessionTestObserver for ServingRoute {
        fn observe_event(&self, event: ReadSessionEvent) {
            assert!(
                !matches!(event, ReadSessionEvent::ClaimWindowWait { .. }),
                "a serving owner must answer before its remaining open bookkeeping finishes"
            );
        }
    }
    struct ResumeOnDrop(Option<mpsc::Sender<()>>);
    impl Drop for ResumeOnDrop {
        fn drop(&mut self) {
            if let Some(resume) = self.0.take() {
                let _ = resume.send(());
            }
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("serving.db");
    let seed = Database::open(&path).unwrap();
    seed.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    seed.execute("INSERT INTO items VALUES (1)", &HashMap::new())
        .unwrap();
    drop(seed);
    let (reached, reached_rx) = mpsc::channel();
    let (resume, resume_rx) = mpsc::channel();
    let options = contextdb_engine::DatabaseOpenOptions {
        test_observer: Some(Arc::new(OwnerReady {
            reached,
            resume: std::sync::Mutex::new(resume_rx),
        })),
        ..contextdb_engine::DatabaseOpenOptions::default()
    };
    let writer_path = path.clone();
    let opening = std::thread::spawn(move || Database::open_with_options(&writer_path, options));
    let resume = ResumeOnDrop(Some(resume));
    reached_rx.recv().unwrap();
    let reader = ReadSession::open_with_observations_for_test(
        &path,
        ReadSessionOptions::default(),
        Arc::new(UnexpiringClock),
        Some(Arc::new(ServingRoute)),
        None,
    )
    .unwrap();
    drop(resume);
    let owner = opening.join().unwrap().unwrap();
    assert_eq!(
        reader
            .execute("SELECT id FROM items", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]]
    );
    drop(reader);
    drop(owner);
}
