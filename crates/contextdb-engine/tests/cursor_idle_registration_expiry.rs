//! An idle-expired cursor must stop deferring retention.
//!
//! A suspended bounded cursor registers its pinned snapshot, and retention
//! defers any row that snapshot can still see.  That deferral is bounded by
//! the cursor's own idle window: once `cursor_idle_ms` has passed with no
//! fetch, the engine's own rule says the cursor is expired and its next fetch
//! is refused.  A caller that simply walks away — opens a cursor, reads a
//! page, and never comes back — is the ordinary case, not an exotic one, and
//! the rows it was reading must not stay pinned for the life of the process.
//!
//! Time here moves only through the injected cursor clock; there is no
//! sleeping and no background maintenance, so each journey is deterministic.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    CursorExpiryKind, CursorPage, DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadFailure,
    ReadLimits,
};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// The promise this file protects, quoted in every failure.
const IDLE_PROMISE: &str = "a bounded cursor defers retention only while it is still alive: once \
                            its idle window has passed, the rows its pinned snapshot was holding \
                            back are reclaimable again";

const NEVER: i64 = i64::MAX;
const ALREADY: i64 = -1;
const IDLE_MS: u64 = 10_000;

#[derive(Clone, Default)]
struct ManualClock {
    now_ms: Arc<AtomicU64>,
}

impl ManualClock {
    fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }
}

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn paged_limits(page_rows: usize) -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: page_rows as u64,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: IDLE_MS,
        cursor_lifetime_ms: 100_000,
    }
}

fn request(
    sql: impl Into<String>,
    limits: ReadLimits,
    clock: &ManualClock,
) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, HashMap::new(), limits, Arc::new(clock.clone()))
}

fn page_target_count(page: &CursorPage) -> usize {
    page.rows.len()
}

/// A source with eight outgoing edges, two of whose target rows are already
/// past their expiry stamp and visible at any snapshot opened here.  Those two
/// rows are exactly what a live cursor registration defers.
fn open_graph_fixture() -> (Arc<Database>, Vec<Uuid>) {
    let db = Arc::new(Database::open_memory());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 1 HOURS",
        &HashMap::new(),
    )
    .expect("create the retained node table");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .expect("create the edge table");

    let node = |ordinal: u128| Uuid::from_u128(0x1D1E_0000_0000_0000_0000_0000_0000_0000 + ordinal);
    let source = node(0);
    let targets: Vec<Uuid> = (1..=8_u128).map(node).collect();
    let expiring = [targets[1], targets[2]];

    let insert_node = |id: Uuid, expires: i64| {
        db.execute(
            "INSERT INTO nodes (id, expires_at) VALUES ($id, $expires)",
            &params([
                ("id", Value::Uuid(id)),
                ("expires", Value::Timestamp(expires)),
            ]),
        )
        .expect("seed a graph node");
    };
    insert_node(source, NEVER);
    for target in &targets {
        let expires = if expiring.contains(target) {
            ALREADY
        } else {
            NEVER
        };
        insert_node(*target, expires);
    }
    for (ordinal, target) in targets.iter().enumerate() {
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'LINKS')",
            &params([
                ("id", Value::Uuid(node(1_000 + ordinal as u128))),
                ("source", Value::Uuid(source)),
                ("target", Value::Uuid(*target)),
            ]),
        )
        .expect("seed a graph edge");
    }
    (db, expiring.to_vec())
}

const GRAPH_SQL: &str = "SELECT s, t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                         COLUMNS (a.id AS s, b.id AS t))";

/// The reader opens a cursor, takes a couple of pages, and never comes back.
/// Its idle window passes on the clock the cursor itself reads.  Retention
/// then owes the rows the deferral was holding: the reader that was being
/// protected is, by the engine's own rule, no longer entitled to another page.
#[test]
fn retention_reclaims_rows_a_cursor_stopped_being_entitled_to_when_its_idle_window_passed() {
    let journey = "graph edge cursor abandoned past its idle window, retention cycle";
    let (db, expiring) = open_graph_fixture();

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(GRAPH_SQL, paged_limits(2), &clock),
    )
    .expect("open a bounded cursor over the whole edge set");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor open past its first page"
    );
    assert_eq!(
        page_target_count(&opened.first_page),
        2,
        "{journey}: the first page must carry a full page, so the reader really is mid-adjacency"
    );
    let second = opened
        .cursor
        .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
        .expect("page the graph cursor a second time");
    assert!(
        second.page.has_more,
        "{journey}: the cursor must still be mid-adjacency when it is abandoned"
    );

    // While the cursor is alive its registration rightly defers the expired
    // rows it can see. This proves the deferral is really in force, so the
    // assertion after the idle window is about the deadline and nothing else.
    let while_alive = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle while the cursor is still alive");
    assert_eq!(
        while_alive.pruned_rows, 0,
        "{journey}: retention must defer rows a live registered cursor snapshot still sees"
    );

    // The reader walks away. Only the clock moves — no fetch, no close.
    clock.set(IDLE_MS + 1);

    let after_idle = db
        .run_pruning_cycle_checked()
        .expect("run the retention cycle again once the idle window has passed");
    assert_eq!(
        after_idle.pruned_rows,
        expiring.len() as u64,
        "{journey}: {IDLE_PROMISE}. The cursor's idle window has passed, so its registration no \
         longer protects the {} expired rows it could see; retention reported {} reclaimed",
        expiring.len(),
        after_idle.pruned_rows
    );

    // The reader is genuinely finished: the engine refuses the next page for
    // idle expiry, which is the same deadline retention was asked to honor.
    match opened
        .cursor
        .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
    {
        Err(bounded::TestError::Refused(failure)) => assert_eq!(
            failure,
            ReadFailure::cursor_expired(CursorExpiryKind::Idle),
            "{journey}: {IDLE_PROMISE}. The abandoned cursor must be refused for idle expiry"
        ),
        other => panic!(
            "{journey}: {IDLE_PROMISE}. An abandoned cursor past its idle window must be refused, \
             got {other:?}",
            other = other.map(|fetched| fetched.page.rows.len())
        ),
    }
}

/// The premise the journey above rests on, on its own: the idle deadline this
/// file advances past is the deadline the engine itself enforces.
#[test]
fn a_cursor_left_past_its_idle_window_is_refused_its_next_page() {
    let journey = "cursor idle deadline, next page";
    let (db, _expiring) = open_graph_fixture();

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(GRAPH_SQL, paged_limits(2), &clock),
    )
    .expect("open a bounded cursor over the whole edge set");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor open past its first page"
    );

    clock.set(IDLE_MS + 1);
    match opened
        .cursor
        .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
    {
        Err(bounded::TestError::Refused(failure)) => assert_eq!(
            failure,
            ReadFailure::cursor_expired(CursorExpiryKind::Idle),
            "{journey}: a cursor past its idle window is refused for idle expiry"
        ),
        other => panic!(
            "{journey}: a cursor past its idle window must be refused, got {other:?}",
            other = other.map(|fetched| fetched.page.rows.len())
        ),
    }
}
