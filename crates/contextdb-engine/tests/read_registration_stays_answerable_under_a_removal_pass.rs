//! A read that parks behind an in-flight version-cleanup pass must stay
//! answerable to the caller that opened it.
//!
//! Registering a read snapshot at or below a running pass's watermark waits
//! for that pass, and waiting is deliberate: the pass sampled the registered
//! set before this read existed, so admitting the read onto that sample would
//! promise protection the pass can still violate.  What the caller is owed is
//! not an immediate registration — it is a way out.  The wait spans the whole
//! pass, persisted removal included, and a reader that has withdrawn its
//! request is still held there: the caller's cancellation is never consulted,
//! so a declared read has no terminal answer and no escape while the pass
//! runs.
//!
//! Deterministic, no sleeping as synchronisation: the pass is held at a real
//! checkpoint (`pause_after_currency_floor_sample_for_test`), the read is
//! proven to be parked before anything is cancelled, and the cancellation is
//! an event this thread signals — not a duration this thread waits out.  The
//! bounded receives below exist so a parked read fails the journey instead of
//! hanging it; nothing here asserts how long any step took.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// The promise this journey protects, quoted in every failure so a reader of
/// the failure does not have to come back here for it.
const ANSWERABLE_PROMISE: &str = "a declared read always reaches a terminal answer: a read \
                                  parked behind a running version-cleanup pass must observe the \
                                  caller's cancellation and answer, rather than staying held \
                                  until that pass finishes its persisted removal";

/// A clock the journey owns, so no step depends on real time passing.
#[derive(Clone, Default)]
struct ManualClock {
    now_ms: Arc<AtomicU64>,
}

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

/// Budgets wide enough that nothing in this journey is refused for size, so
/// the only thing that can stop the read is the pass it parks behind.
fn wide_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 8,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 100_000,
        cursor_lifetime_ms: 1_000_000,
    }
}

fn advertise(db: &Database, key: &str, capability_id: &str, advertised_at: i64) {
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text(key.to_string()));
    params.insert("node_id".to_string(), Value::Text(key.to_string()));
    params.insert(
        "capability_id".to_string(),
        Value::Text(capability_id.to_string()),
    );
    params.insert("tags".to_string(), Value::Json(serde_json::json!([])));
    params.insert("advertised_at".to_string(), Value::Timestamp(advertised_at));
    db.execute(
        "INSERT INTO work_capabilities (capability_key, node_id, capability_id, tags, detail, \
         advertised_at) VALUES ($k, $node_id, $capability_id, $tags, NULL, $advertised_at) \
         ON CONFLICT (capability_key) DO UPDATE SET \
         advertised_at = $advertised_at, node_id = $node_id, capability_id = $capability_id",
        &params,
    )
    .expect("advertise a capability");
}

/// What the parked read finally answered, in the caller's terms.
enum ReadOutcome {
    Opened,
    Cancelled,
    Refused(String),
    Failed(String),
}

impl ReadOutcome {
    /// Whether the caller got something it can branch on: the read was
    /// withdrawn and said so, or it refused and named what stopped it.
    fn is_terminal_answer(&self) -> bool {
        matches!(self, Self::Cancelled | Self::Refused(_))
    }

    fn describe(&self) -> String {
        match self {
            Self::Opened => "the read opened as if nothing had withdrawn it".to_string(),
            Self::Cancelled => "a cancelled answer".to_string(),
            Self::Refused(detail) => format!("a refusal naming what stopped it: {detail}"),
            Self::Failed(detail) => format!("an engine fault: {detail}"),
        }
    }
}

#[test]
fn a_read_parked_behind_a_running_cleanup_pass_answers_the_cancellation_that_withdraws_it() {
    let journey = "fresh read at the watermark, parked behind a mid-flight cleanup pass";
    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("install the ledger schema");

    // A superseded version for the pass to decide about, so the pass has real
    // work and its persisted removal is a real span of time to be held for.
    advertise(&db, "node-a", "cap-old", 1_700_000_000_000);
    advertise(&db, "node-a", "cap-new", 1_700_000_000_100);

    let floor_pause = db.pause_after_currency_floor_sample_for_test();
    let db_pass = Arc::clone(&db);
    let pass_handle = thread::spawn(move || {
        db_pass
            .compact_currency_versions()
            .expect("the cleanup pass must succeed")
    });
    assert!(
        floor_pause.wait_until_reached(Duration::from_secs(5)),
        "{journey}: the cleanup pass must reach its floor-sample checkpoint, otherwise the read \
         below never parks and this journey proves nothing"
    );

    // The read opens HERE, with no commit in between, so its snapshot is the
    // very watermark the paused pass sampled -- the case the registration
    // holds for the pass's full duration.
    let cancellation = OwnerReadCancellation::new();
    let mut request = bounded::BoundedReadRequest::new(
        "SELECT capability_id FROM work_capabilities",
        HashMap::new(),
        wide_limits(),
        Arc::new(ManualClock::default()),
    );
    request.cancellation = cancellation.clone();

    let (outcome_tx, outcome_rx) = mpsc::channel::<ReadOutcome>();
    let db_read = Arc::clone(&db);
    let read_handle = thread::spawn(move || {
        let outcome = match bounded::open_cursor(db_read, &request) {
            Ok(_) => ReadOutcome::Opened,
            Err(bounded::TestError::Cancelled) => ReadOutcome::Cancelled,
            Err(bounded::TestError::Refused(refusal)) => {
                ReadOutcome::Refused(format!("{refusal:?}"))
            }
            Err(other) => ReadOutcome::Failed(format!("{other:?}")),
        };
        outcome_tx.send(outcome).expect("report the read's outcome");
    });

    // Nothing else in this read's open path blocks, so a read that has not
    // answered yet is parked in the registration. Proving that here is what
    // makes the cancellation below land while it is parked, rather than
    // racing an already-finished read.
    assert!(
        outcome_rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "{journey}: the read must still be parked behind the in-flight pass at this point; a read \
         that already answered was never held, and the cancellation below would prove nothing"
    );

    // The caller withdraws the read. The pass is still held at its
    // checkpoint and is released only after the assertion below, so whatever
    // answers here answered from inside the park.
    cancellation.cancel();
    let outcome = outcome_rx.recv_timeout(Duration::from_secs(5));
    let released_outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            floor_pause.release();
            let _ = pass_handle.join();
            let _ = read_handle.join();
            panic!(
                "{journey}: {ANSWERABLE_PROMISE}. The withdrawn read never answered while the \
                 pass was held at its checkpoint: {error:?}"
            );
        }
    };
    assert!(
        released_outcome.is_terminal_answer(),
        "{journey}: {ANSWERABLE_PROMISE}. The withdrawn read answered, but not with a terminal \
         document the caller can branch on: it returned {}",
        released_outcome.describe()
    );

    floor_pause.release();
    let report = pass_handle.join().expect("the pass thread must not panic");
    assert!(
        report.pruned_versions > 0,
        "{journey}: the held pass must really have had removal work to do, otherwise the read was \
         never parked behind a persisted removal: {report:?}"
    );
    read_handle.join().expect("the read thread must not panic");
}
