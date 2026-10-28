//! A routing allowance cannot turn writer turnover into a refused saved read.
use contextdb_core::read_contract::{DeadlineClock, ReadRoute};
use contextdb_core::{Error, Value};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const START_MS: u64 = 10_000;
const QUERY: &str = "SELECT value FROM items WHERE id = 1";

struct ExhaustAtDirectOpen {
    clock: ManualDeadlineClock,
    expire_at: u64,
    armed: AtomicBool,
    fired: AtomicBool,
}

impl ReadSessionTestObserver for ExhaustAtDirectOpen {
    fn observe_event(&self, event: ReadSessionEvent) {
        if matches!(event, ReadSessionEvent::BeforeDirectBackendOpen { .. })
            && self.armed.swap(false, Ordering::SeqCst)
        {
            assert_eq!(
                self.clock.now_ms(),
                START_MS,
                "turnover must start its routing deadline at the unchanged manual time"
            );
            self.clock.advance_to(self.expire_at);
            self.fired.store(true, Ordering::SeqCst);
        }
    }
}

fn writer(path: &Path) -> Database {
    Database::open(path).expect("open the production Legacy writer through the public constructor")
}

#[test]
fn a_file_reader_keeps_answering_when_turnover_hydration_exhausts_its_routing_allowance() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("saved-read.db");
    {
        let first = writer(&path);
        first
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)",
                &HashMap::new(),
            )
            .unwrap();
        first
            .execute("INSERT INTO items VALUES (1, 'original')", &HashMap::new())
            .unwrap();
        first.close().unwrap();
    }

    let clock = ManualDeadlineClock::at(START_MS);
    let options = ReadSessionOptions::default();
    let expire_at = START_MS + options.timeouts.routing_retry_ms + 1;
    let observer = Arc::new(ExhaustAtDirectOpen {
        clock: clock.clone(),
        expire_at,
        armed: AtomicBool::new(false),
        fired: AtomicBool::new(false),
    });
    let session = ReadSession::open_with_clock_and_observer_for_test(
        &path,
        options,
        Arc::new(clock.clone()),
        observer.clone(),
    )
    .unwrap();
    assert_eq!(session.route(), ReadRoute::File);
    assert_eq!(
        session.execute(QUERY, &HashMap::new()).unwrap().rows(),
        vec![vec![Value::Text("original".into())]]
    );

    {
        let replacement = writer(&path);
        replacement
            .execute(
                "UPDATE items SET value = 'latest saved' WHERE id = 1",
                &HashMap::new(),
            )
            .unwrap();
        replacement.close().unwrap();
    }
    observer.armed.store(true, Ordering::SeqCst);
    let answer = session.execute(QUERY, &HashMap::new());
    assert!(
        observer.fired.load(Ordering::SeqCst),
        "the real turnover direct-open seam was reached"
    );
    assert_eq!(
        clock.now_ms(),
        expire_at,
        "the routing allowance is exhausted before hydration"
    );
    // Drive a second statement even on RED: a failed turnover must not leave
    // this reader permanently unusable. Both outcomes retain their typed errors.
    let later = session.execute(QUERY, &HashMap::new());
    for (label, outcome) in [("turnover read", &answer), ("later read", &later)] {
        if let Err(Error::ReadFailure(failure)) = outcome {
            eprintln!(
                "{label}: typed {:?}, {:?}",
                failure.kind(),
                failure.detail()
            );
        }
    }
    let answer = answer.expect(
        "a writer that came and went must not make the reader refuse its last saved answer",
    );
    assert_eq!(
        answer.rows(),
        vec![vec![Value::Text("latest saved".into())]]
    );
    assert_eq!(
        later
            .expect("the same reader must also answer a later statement")
            .rows(),
        vec![vec![Value::Text("latest saved".into())]]
    );
}
