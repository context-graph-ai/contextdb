//! Retention must judge an ADJACENCY entry by the edge's own visibility.
//!
//! A retention cycle collects the ids of the rows whose live versions it
//! reclaims and then drops every adjacency entry touching one of those ids.
//! That reach is decided entirely by the node row; the edge entry itself is
//! never asked whether a registered reader can still see it.  A schema-free
//! store lets an edge point at an id that carries no row yet, so a row for
//! that id can be written — and age out — entirely AFTER a cursor pinned its
//! snapshot.  The node row is then rightly reclaimable, but the edge is not:
//! it was committed before the snapshot and the pinned cursor still owes it.
//!
//! Everything below runs on this thread with no sleeping: the writes land
//! between fetch calls, and the only retention cycle is the one the journey
//! asks for through `Database::run_pruning_cycle_checked`.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::{
    CursorPage, DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// The promise this file protects, quoted in every failure so the reader of a
/// failure does not have to come back here for it.
const EDGE_PROMISE: &str = "a bounded graph cursor pages one committed snapshot: an edge visible \
                            at that snapshot must be emitted exactly once, and retention may \
                            physically remove an adjacency entry only when no registered reader \
                            can still see that edge";

const NEVER: i64 = i64::MAX;
const ALREADY: i64 = -1;

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

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// Budgets wide enough that nothing here is refused for size, with a small
/// page so the cursor is still mid-adjacency when retention runs.
fn paged_limits(page_rows: usize) -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: page_rows as u64,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
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

fn page_targets(page: &CursorPage) -> Vec<Uuid> {
    let target = page
        .columns
        .iter()
        .position(|column| column == "t" || column.ends_with(".t"))
        .unwrap_or_else(|| {
            panic!(
                "cursor page carries the target column, got {:?}",
                page.columns
            )
        });
    page.rows
        .iter()
        .map(|row| match row[target] {
            Value::Uuid(value) => value,
            ref other => panic!("a graph target must be a UUID, got {other:?}"),
        })
        .collect()
}

/// Drain a still-open cursor to exhaustion one page at a time.  A fetch that
/// fails is reported against the promise: an edge lost under a pinned reader
/// must not become a read failure either.
fn drain(
    cursor: &mut bounded::TestCursor,
    page_rows: usize,
    mut has_more: bool,
    journey: &str,
) -> Vec<Uuid> {
    let mut collected = Vec::new();
    while has_more {
        let fetched = cursor
            .fetch(NonZeroUsize::new(page_rows), OwnerReadCancellation::new())
            .unwrap_or_else(|error| {
                panic!(
                    "{journey}: {EDGE_PROMISE}. A later page failed instead of continuing the \
                     pinned read: {error:?}"
                )
            });
        collected.extend(page_targets(&fetched.page));
        has_more = fetched.page.has_more;
    }
    collected
}

fn assert_emitted_exactly_once(emitted: &[Uuid], expected: &[Uuid], journey: &str) {
    let mut sorted = emitted.to_vec();
    sorted.sort();
    let mut expected_sorted = expected.to_vec();
    expected_sorted.sort();
    assert_eq!(
        sorted, expected_sorted,
        "{journey}: {EDGE_PROMISE}. Pages emitted {emitted:?} in order; the snapshot's visible \
         edge targets are {expected:?}"
    );
}

/// How many oriented edges an uncapped read sees right now, so a journey can
/// prove its fixture before it judges the cursor.
fn visible_edge_targets(db: &Database) -> Vec<Uuid> {
    let result = db
        .execute(
            "SELECT s, t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             COLUMNS (a.id AS s, b.id AS t))",
            &HashMap::new(),
        )
        .expect("uncapped read of the whole edge set");
    let target = result
        .columns
        .iter()
        .position(|column| column == "t" || column.ends_with(".t"))
        .expect("uncapped read returns the target column");
    let mut targets = result
        .rows
        .iter()
        .map(|row| match row[target] {
            Value::Uuid(value) => value,
            ref other => panic!("a graph target must be a UUID, got {other:?}"),
        })
        .collect::<Vec<_>>();
    targets.sort();
    targets
}

/// An edge committed before the cursor opened, pointing at an id that carried
/// no row at that moment.  A row for that id is written afterwards, already
/// past its expiry, so retention reclaims it — correctly, since no registered
/// snapshot can see that row.  The EDGE is a different question: it was
/// committed before the snapshot, the pinned cursor has not reached it yet,
/// and it must still be emitted.
#[test]
fn retention_keeps_a_snapshot_visible_edge_whose_target_row_arrives_and_expires_after_the_snapshot()
{
    let journey = "graph edge cursor, target row written and expired after the snapshot";
    let db = Arc::new(Database::open_memory());
    // The caller drives retention, so the only cycle in this journey is the
    // one asked for below.
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

    let node = |ordinal: u128| Uuid::from_u128(0x0ED9_0000_0000_0000_0000_0000_0000_0000 + ordinal);
    let source = node(0);
    let targets: Vec<Uuid> = (1..=9_u128).map(node).collect();
    // The LAST target carries no node row when the cursor opens, so a row for
    // it is genuinely newer than the cursor's snapshot when it arrives below.
    let rowless_at_open = targets[8];

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
    for target in targets.iter().take(8) {
        insert_node(*target, NEVER);
    }
    // One statement per edge, so this source's adjacency order is the order
    // written here and the rowless target sits last — past where the cursor
    // is paused when retention runs.
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
    let mut all_targets = targets.clone();
    all_targets.sort();
    assert_eq!(
        visible_edge_targets(&db),
        all_targets,
        "{journey}: an edge to an id with no row of its own must be readable before the cursor \
         is judged"
    );

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(
            "SELECT s, t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             COLUMNS (a.id AS s, b.id AS t))",
            paged_limits(2),
            &clock,
        ),
    )
    .expect("open a bounded cursor over the whole edge set");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor open past its first page"
    );
    let mut emitted = page_targets(&opened.first_page);
    let second = opened
        .cursor
        .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
        .expect("page the graph cursor a second time");
    emitted.extend(page_targets(&second.page));
    assert!(
        second.page.has_more,
        "{journey}: the cursor must still be mid-adjacency when retention runs"
    );
    assert!(
        !emitted.contains(&rowless_at_open),
        "{journey}: the edge under test must NOT have been reached yet, so a lost edge shows up \
         as a silent skip rather than a page the cursor already delivered"
    );

    // Ordinary write traffic: a row finally arrives for the id the edge
    // already points at, already past its expiry stamp.
    db.execute(
        "INSERT INTO nodes (id, expires_at) VALUES ($id, $expires)",
        &params([
            ("id", Value::Uuid(rowless_at_open)),
            ("expires", Value::Timestamp(ALREADY)),
        ]),
    )
    .expect("write the late node row the pinned snapshot cannot see");

    // Ordinary maintenance traffic. The new row is newer than the cursor's
    // snapshot, so reclaiming it is correct and expected; what the cursor is
    // owed is the EDGE, which is older than the snapshot.
    let report = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle while the graph cursor is pinned");
    assert_eq!(
        report.pruned_rows, 1,
        "{journey}: the late node row must really be reclaimed before the cursor is judged; a \
         deferred row would make this journey prove nothing"
    );

    emitted.extend(drain(&mut opened.cursor, 2, second.page.has_more, journey));

    assert_emitted_exactly_once(&emitted, &targets, journey);
}
