//! A cancelled graph response must leave the selected channel reusable.
use contextdb_core::read_contract::{OwnerReadCancellation, ReadFailureKind, ReadRoute};
use contextdb_core::{Error, Value};
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSource, ReadKernelSourceEvent, ReadKernelTestObserver,
    ReadSessionOperation,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, ReadSession, ReadSessionOptions,
};
use contextdb_engine::{ReadPhase, ReadProgress, ReadProgressObserver};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// Exactly the shipped row-reporting interval. ProgressRelay publishes an earned
// report ahead of the terminal even if its worker has already completed.
const EDGES: u128 = 500;
const GRAPH: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t)) ORDER BY t";
const NEXT: &str = "SELECT id FROM nodes WHERE id = $source";

fn params() -> HashMap<String, Value> {
    HashMap::from([("source".into(), Value::Uuid(uuid::Uuid::from_u128(1)))])
}

fn graph_rows() -> Vec<Vec<Value>> {
    (2..=EDGES + 1)
        .map(|id| vec![Value::Uuid(uuid::Uuid::from_u128(id))])
        .collect()
}

fn next_rows() -> Vec<Vec<Value>> {
    vec![vec![Value::Uuid(uuid::Uuid::from_u128(1))]]
}

fn seed(path: &Path) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let nodes = (1..=EDGES + 1)
        .map(|id| format!("('{}')", uuid::Uuid::from_u128(id)))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(
        &format!("INSERT INTO nodes VALUES {nodes}"),
        &HashMap::new(),
    )
    .unwrap();
    let edges = (2..=EDGES + 1)
        .map(|target| {
            format!(
                "('{}', '{}', '{}', 'LINKS')",
                uuid::Uuid::from_u128(10_000 + target),
                uuid::Uuid::from_u128(1),
                uuid::Uuid::from_u128(target)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(
        &format!("INSERT INTO edges VALUES {edges}"),
        &HashMap::new(),
    )
    .unwrap();
    db
}

#[derive(Default)]
struct CancelAtProgress {
    token: Mutex<Option<OwnerReadCancellation>>,
    event: Mutex<Option<ReadProgress>>,
}

impl ReadProgressObserver for CancelAtProgress {
    fn progress(&self, event: ReadProgress) {
        if event.phase == ReadPhase::Executing {
            let token = self.token.lock().unwrap().take();
            if let Some(token) = token {
                *self.event.lock().unwrap() = Some(event);
                // Called by OwnerClient only after decoding a real nonterminal
                // Progress response frame. No owner wait or callback lock.
                token.cancel();
            }
        }
    }
}

#[test]
fn cancelling_after_a_graph_response_starts_leaves_the_same_reader_usable() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("response-cancel.db");
    let db = seed(&path);
    let progress = Arc::new(CancelAtProgress::default());
    let reader =
        ReadSession::open_with_progress(&path, ReadSessionOptions::default(), progress.clone())
            .unwrap();
    assert_eq!(reader.route(), ReadRoute::Owner);
    assert_eq!(
        reader.execute(GRAPH, &params()).unwrap().rows(),
        graph_rows(),
        "healthy graph control precedes cancellation"
    );
    let before = reader.route_resources_for_test().unwrap();
    let token = OwnerReadCancellation::new();
    *progress.token.lock().unwrap() = Some(token.clone());
    let cancelled = reader.execute_with_cancellation(GRAPH, &params(), &token);
    let event = progress
        .event
        .lock()
        .unwrap()
        .take()
        .expect("a real decoded nonterminal execution-progress frame precedes cancellation");
    assert_eq!(event.phase, ReadPhase::Executing);
    assert!(
        event.work >= 1_000 || event.rows >= EDGES as u64,
        "counter thresholds, not time, earn this progress event"
    );
    assert!(token.is_cancelled());
    assert!(
        matches!(cancelled, Err(Error::ReadCancelled)),
        "started-response cancellation keeps its exact public type: {cancelled:?}"
    );
    let next = reader.execute(NEXT, &params());
    if let Err(Error::ReadFailure(failure)) = &next {
        eprintln!(
            "next statement: typed {:?}, {:?}",
            failure.kind(),
            failure.detail()
        );
    }
    let after = reader.route_resources_for_test().unwrap();
    assert_eq!(reader.route(), ReadRoute::Owner);
    assert_eq!(after.channel_identity, before.channel_identity);
    assert_eq!(after.direct_backend_opens, before.direct_backend_opens);
    assert_eq!(
        after.local_channel_operations,
        before.local_channel_operations + 2,
        "one cancelled statement and exactly one subsequent statement"
    );
    assert_eq!(after.active_cursors, before.active_cursors);
    let next =
        next.expect("the next query on the same reader must succeed after graph cancellation");
    assert_eq!(
        next.rows(),
        next_rows(),
        "a fresh query must not receive the old graph answer"
    );
    drop(next);
    drop(reader);
    db.close().unwrap();
}

#[derive(Default)]
struct OwnerStop {
    armed: AtomicBool,
    touches: AtomicUsize,
    first: Mutex<Option<ReadKernelSourceEvent>>,
    cancelled: Mutex<Vec<ReadKernelCancellationEvent>>,
}

impl ReadKernelTestObserver for OwnerStop {
    fn before_source_touch(
        &self,
        event: ReadKernelSourceEvent,
        cancellation: OwnerReadCancellation,
    ) {
        if event.source == ReadKernelSource::GraphEdge {
            self.touches.fetch_add(1, Ordering::SeqCst);
            if self.armed.swap(false, Ordering::SeqCst) {
                *self.first.lock().unwrap() = Some(event);
                cancellation.cancel();
            }
        }
    }
    fn cancellation_observed(&self, event: ReadKernelCancellationEvent, _: OwnerReadCancellation) {
        self.cancelled.lock().unwrap().push(event);
    }
}

fn observed_owner(path: &Path, stop: Arc<OwnerStop>) -> Database {
    let fixture = seed(path);
    fixture.close().unwrap();
    drop(fixture);
    // The observer must be on the STORE: channel work runs on its owner
    // thread. This public constructor selects the same production Legacy host.
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            test_kernel_observer: Some(stop),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn owner_cancellation_stops_before_fetching_the_first_graph_edge() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner-stop.db");
    let stop = Arc::new(OwnerStop::default());
    let db = observed_owner(&path, stop.clone());
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::Owner);
    assert_eq!(
        reader.execute(GRAPH, &params()).unwrap().rows(),
        graph_rows()
    );
    assert!(
        stop.touches.load(Ordering::SeqCst) > 0,
        "the real serving owner carries this observer"
    );
    stop.touches.store(0, Ordering::SeqCst);
    stop.armed.store(true, Ordering::SeqCst);
    let caller = OwnerReadCancellation::new();
    let error = reader
        .execute_with_cancellation(GRAPH, &params(), &caller)
        .unwrap_err();
    // This token belongs to the owner, not to the caller. Clean77 reports
    // its owner-only stop as OwnerTimeout; caller cancellation is checked
    // separately and must retain exact ReadCancelled mapping.
    assert!(
        matches!(&error, Error::ReadCancelled)
            || matches!(&error, Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::OwnerTimeout),
        "owner-only stop retains its typed refusal: {error:?}"
    );
    eprintln!("owner-only stop: {error:?}");
    assert!(
        !caller.is_cancelled(),
        "this control cancels the actual owner kernel token"
    );
    let event = stop
        .first
        .lock()
        .unwrap()
        .take()
        .expect("real GraphEdge source boundary");
    assert_eq!(event.route, ReadRoute::Owner);
    assert_eq!(event.operation, ReadSessionOperation::Execute);
    assert_eq!(event.completed_items, 0);
    assert_eq!(
        stop.touches.load(Ordering::SeqCst),
        1,
        "no later graph edge source is reached"
    );
    let observed = stop.cancelled.lock().unwrap();
    assert_eq!(
        observed.len(),
        1,
        "the real kernel observes cancellation exactly once"
    );
    assert_eq!(observed[0].route, ReadRoute::Owner);
    assert_eq!(observed[0].operation, ReadSessionOperation::Execute);
    drop(observed);
    assert_eq!(reader.execute(NEXT, &params()).unwrap().rows(), next_rows());
    assert_eq!(
        reader.execute(GRAPH, &params()).unwrap().rows(),
        graph_rows()
    );
    drop(reader);
    db.close().unwrap();
}

#[test]
fn a_precancelled_graph_query_does_not_touch_its_source_or_poison_the_reader() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pre-cancel.db");
    let stop = Arc::new(OwnerStop::default());
    let db = observed_owner(&path, stop.clone());
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::Owner);
    assert_eq!(
        reader.execute(GRAPH, &params()).unwrap().rows(),
        graph_rows()
    );
    let touches = stop.touches.load(Ordering::SeqCst);
    assert!(
        touches > 0,
        "healthy graph execution proves the observer is attached to the owner"
    );
    let token = OwnerReadCancellation::new();
    token.cancel();
    let error = reader
        .execute_with_cancellation(GRAPH, &params(), &token)
        .unwrap_err();
    assert!(matches!(error, Error::ReadCancelled));
    assert_eq!(
        stop.touches.load(Ordering::SeqCst),
        touches,
        "pre-cancellation never enters the graph source"
    );
    assert!(
        stop.cancelled.lock().unwrap().is_empty(),
        "no owner execution was cancelled"
    );
    assert_eq!(reader.execute(NEXT, &params()).unwrap().rows(), next_rows());
    assert_eq!(
        reader.execute(GRAPH, &params()).unwrap().rows(),
        graph_rows()
    );
    drop(reader);
    db.close().unwrap();
}
