//! A graph edge cursor whose adjacency genuinely shrinks under it.
//!
//! Retention leaves alone every row a registered snapshot can still see, so a
//! pinned cursor usually watches nothing move.  The interesting case is the
//! one where the reclaim is entirely legitimate: adjacency entries the
//! cursor's snapshot can NOT see — an edge already deleted before the cursor
//! opened, whose target row is written and aged out afterwards — sit ahead of
//! where the cursor is paused.  Removing them shortens the source's adjacency
//! and every surviving entry slides down, so the integer position the
//! suspended continuation was holding now addresses the wrong entry.  The
//! continuation re-anchors by identity, and the reader must still receive
//! every edge its snapshot can see, exactly once.
//!
//! No sleeping and no background maintenance: the writes and the single
//! retention cycle land between fetch calls on this thread.

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

/// The promise this file protects, quoted in every failure.
const SHRINK_PROMISE: &str = "a bounded graph cursor pages one committed snapshot: when \
                              retention compacts adjacency entries the snapshot cannot see, \
                              every edge it CAN see is still emitted exactly once and no page \
                              fails";

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
                    "{journey}: {SHRINK_PROMISE}. A later page failed instead of continuing the \
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
        "{journey}: {SHRINK_PROMISE}. Pages emitted {emitted:?} in order; the snapshot's visible \
         edge targets are {expected:?}"
    );
}

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

/// Two edges are written first, so they occupy the FRONT of the source's
/// adjacency, and are then deleted before any cursor opens — a deleted entry
/// keeps its place in the adjacency until retention removes it.  Their target
/// ids get rows only after the cursor pins its snapshot, already past expiry,
/// so retention may reclaim those rows and with them the two front entries.
/// The cursor is paused past that point, so the shrink lands strictly below
/// the position it is holding.
///
/// The same cycle also meets the opposite case in the same adjacency: two
/// targets whose rows were written BEFORE the cursor pinned its snapshot and
/// were already past expiry when they were written.  Retention must DEFER
/// those rows — the pinned reader can still see them — so their adjacency
/// entries stay exactly where they are while the entries below them are
/// removed.  Every one of those deferred edges must reach the reader exactly
/// once: neither dropped with the reclaimed entries, nor emitted twice by a
/// continuation that re-reads adjacency it has already passed.
#[test]
fn a_graph_edge_cursor_emits_every_visible_edge_when_retention_compacts_entries_it_cannot_see() {
    let journey = "graph edge cursor, retention compacts entries below the paused position";
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

    let node = |ordinal: u128| Uuid::from_u128(0x5147_0000_0000_0000_0000_0000_0000_0000 + ordinal);
    let source = node(0);
    // Two withdrawn edges first, then six that stay, then two whose target
    // rows retention must hold back for this reader.
    let withdrawn: Vec<Uuid> = (1..=2_u128).map(node).collect();
    let kept: Vec<Uuid> = (11..=16_u128).map(node).collect();
    let deferred: Vec<Uuid> = (21..=22_u128).map(node).collect();
    // Everything the cursor's snapshot can see, in adjacency order.
    let visible: Vec<Uuid> = kept.iter().chain(deferred.iter()).copied().collect();

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
    let insert_edge = |ordinal: u128, target: Uuid| {
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'LINKS')",
            &params([
                ("id", Value::Uuid(node(1_000 + ordinal))),
                ("source", Value::Uuid(source)),
                ("target", Value::Uuid(target)),
            ]),
        )
        .expect("seed a graph edge");
    };

    insert_node(source, NEVER);
    for target in &kept {
        insert_node(*target, NEVER);
    }
    // Written past expiry BEFORE the cursor pins its snapshot, so the reader
    // is entitled to these rows and retention has to hold them back rather
    // than reclaim them with the entries below.
    for target in &deferred {
        insert_node(*target, ALREADY);
    }
    // Order matters: the withdrawn edges are written first so they hold the
    // front of this source's adjacency, and the deferred pair sits last, past
    // where the cursor is paused when retention runs.
    for (ordinal, target) in withdrawn.iter().enumerate() {
        insert_edge(ordinal as u128, *target);
    }
    for (ordinal, target) in kept.iter().enumerate() {
        insert_edge(100 + ordinal as u128, *target);
    }
    for (ordinal, target) in deferred.iter().enumerate() {
        insert_edge(200 + ordinal as u128, *target);
    }

    // The two front edges are withdrawn before any cursor opens, so no
    // snapshot taken below can see them; their adjacency entries stay in
    // place, still charged, until retention removes them.
    let tx = db
        .begin()
        .expect("open a transaction to withdraw two edges");
    for target in &withdrawn {
        db.delete_edge(tx, source, *target, "LINKS")
            .expect("withdraw an edge before any cursor opens");
    }
    db.commit(tx).expect("commit the withdrawn edges");
    let mut visible_sorted = visible.clone();
    visible_sorted.sort();
    assert_eq!(
        visible_edge_targets(&db),
        visible_sorted,
        "{journey}: only the six kept edges and the two whose target rows retention must hold \
         back may be readable once the front two are withdrawn"
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
    assert_eq!(
        emitted.len(),
        4,
        "{journey}: the cursor must be paused past the two front entries, so the compaction \
         below happens strictly beneath the position it is holding"
    );

    // Rows finally arrive for the two withdrawn targets, already past their
    // expiry stamp and newer than the cursor's snapshot — so retention may
    // reclaim them, and with them the two front adjacency entries.
    for target in &withdrawn {
        insert_node(*target, ALREADY);
    }
    let (_, adjacency_before) = db.__vector_and_graph_fingerprint_for_test();
    let report = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle while the graph cursor is pinned");
    assert_eq!(
        report.pruned_rows,
        withdrawn.len() as u64,
        "{journey}: exactly the two late node rows must be reclaimed before the cursor is judged \
         — a deferred row would leave the adjacency unchanged and prove nothing, and reclaiming \
         either expired row the pinned reader can still see would take an edge the reader is \
         owed"
    );
    let (_, adjacency_after) = db.__vector_and_graph_fingerprint_for_test();
    assert_ne!(
        adjacency_before, adjacency_after,
        "{journey}: the adjacency must really have been compacted, otherwise the cursor never \
         has to re-anchor and this journey proves nothing"
    );
    assert_eq!(
        visible_edge_targets(&db),
        visible_sorted,
        "{journey}: the two expired target rows the pinned reader can still see must be held \
         back by this cycle, so their adjacency entries stay in place"
    );

    emitted.extend(drain(&mut opened.cursor, 2, second.page.has_more, journey));

    assert_emitted_exactly_once(&emitted, &visible, journey);

    // The two held-back rows were prune-eligible all along: once the reader
    // that was entitled to them is gone, the next cycle reclaims them. Without
    // this, a fixture whose expired rows were never prune candidates would
    // assert the deferral above against nothing.
    drop(opened);
    let after_reader = db
        .run_pruning_cycle_checked()
        .expect("run one more retention cycle once the graph cursor is gone");
    assert_eq!(
        after_reader.pruned_rows,
        deferred.len() as u64,
        "{journey}: the two expired rows must be reclaimed once no reader is entitled to them, \
         which is what proves the earlier cycle held them back for the reader rather than \
         ignoring them"
    );
}
