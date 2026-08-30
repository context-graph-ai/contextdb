//! Source stability of a bounded read cursor across its whole paging life.
//!
//! A bounded cursor opens ONE committed snapshot.  For as long as it pages,
//! every item visible at that snapshot and unchanged throughout must be
//! emitted exactly once: never skipped, never returned on two pages, and the
//! page stream must not end early or fail.  Ordinary write traffic committed
//! after the snapshot — an INSERT, an UPDATE, a retention cycle reclaiming
//! versions the snapshot cannot see — is exactly the traffic a live database
//! carries, and none of it may change what a pinned cursor returns.
//! PostgreSQL portal semantics are the tie-breaker: a portal cannot re-emit a
//! row it already returned, and cannot lose one it has not.
//!
//! Every read below opens through the production bounded-cursor entrance, and
//! every concurrent write is real SQL through `Database::execute` or the real
//! retention cycle through `Database::run_pruning_cycle_checked`.  The writes
//! land between fetch calls on this thread, so each journey is deterministic
//! with no cross-thread choreography and no sleeping.

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

/// The promise every journey in this file protects, quoted in each failure so
/// a reader of the failure does not have to find it here.
const SNAPSHOT_PROMISE: &str = "a bounded cursor pages one committed snapshot: an item visible at \
                                that snapshot and unchanged throughout must be emitted exactly \
                                once, and the page stream must neither fail nor stop early, \
                                whatever ordinary writes commit after the snapshot";

#[derive(Clone, Default)]
struct ManualClock {
    now_ms: Arc<AtomicU64>,
}

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These journeys are synchronous; the immediately-completing future
        // satisfies the shared transport-facing clock trait.
        Box::pin(async {})
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// Budgets wide enough that nothing here is refused for size, with the page
/// size deliberately small so the cursor is still open when the concurrent
/// write commits.
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
    params: HashMap<String, Value>,
    limits: ReadLimits,
    clock: &ManualClock,
) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, params, limits, Arc::new(clock.clone()))
}

fn column_index(page: &CursorPage, name: &str) -> usize {
    page.columns
        .iter()
        .position(|column| column == name || column.ends_with(&format!(".{name}")))
        .unwrap_or_else(|| panic!("cursor page contains {name}, got {:?}", page.columns))
}

fn page_ids(page: &CursorPage) -> Vec<i64> {
    let id = column_index(page, "id");
    page.rows
        .iter()
        .map(|row| match row[id] {
            Value::Int64(value) => value,
            ref other => panic!("id must be INTEGER, got {other:?}"),
        })
        .collect()
}

fn page_targets(page: &CursorPage) -> Vec<Uuid> {
    let target = column_index(page, "t");
    page.rows
        .iter()
        .map(|row| match row[target] {
            Value::Uuid(value) => value,
            ref other => panic!("a graph target must be a UUID, got {other:?}"),
        })
        .collect()
}

/// What an uncapped read sees right now, so a journey can prove its
/// concurrent write really committed before it blames the cursor.
fn committed_ids(db: &Database, table: &str) -> Vec<i64> {
    let result = db
        .execute(&format!("SELECT id FROM {table}"), &HashMap::new())
        .unwrap_or_else(|error| panic!("uncapped read of {table}: {error}"));
    let id = result
        .columns
        .iter()
        .position(|column| column == "id")
        .expect("uncapped read returns id");
    let mut ids = result
        .rows
        .iter()
        .map(|row| match row[id] {
            Value::Int64(value) => value,
            ref other => panic!("id must be INTEGER, got {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// Every expected item, exactly once — the assertion the promise reduces to.
/// Reports the raw emission order too, so a duplicate and a skip are told
/// apart at a glance.
fn assert_emitted_exactly_once<T>(emitted: &[T], expected: &[T], journey: &str)
where
    T: Clone + Ord + std::fmt::Debug,
{
    let mut sorted = emitted.to_vec();
    sorted.sort();
    let expected_sorted = {
        let mut expected = expected.to_vec();
        expected.sort();
        expected
    };
    assert_eq!(
        sorted, expected_sorted,
        "{journey}: {SNAPSHOT_PROMISE}. Pages emitted {emitted:?} in order; the snapshot's \
         visible items are {expected:?}"
    );
}

/// Drain a still-open cursor to exhaustion, one page at a time, collecting
/// what each page carried.  A fetch that fails is reported against the
/// promise: a pinned cursor whose source moved underneath it must not turn an
/// ordinary concurrent write into a read failure.
fn drain<T>(
    cursor: &mut bounded::TestCursor,
    page_rows: usize,
    mut has_more: bool,
    read_page: impl Fn(&CursorPage) -> Vec<T>,
    journey: &str,
) -> Vec<T> {
    let mut collected = Vec::new();
    while has_more {
        let fetched = cursor
            .fetch(NonZeroUsize::new(page_rows), OwnerReadCancellation::new())
            .unwrap_or_else(|error| {
                panic!(
                    "{journey}: {SNAPSHOT_PROMISE}. A later page failed instead of continuing the \
                     pinned read: {error:?}"
                )
            });
        collected.extend(read_page(&fetched.page));
        has_more = fetched.page.has_more;
    }
    collected
}

/// An ordered cursor paging one index key, reading it high-to-low, addresses
/// its place in that key's posting list by integer offset.  An ordinary
/// INSERT sharing the index key lengthens that list, so the same offset
/// resolves to a row the cursor already returned.
#[test]
fn an_ordered_cursor_paging_downward_survives_an_insert_into_the_key_it_is_reading() {
    let journey = "ordered cursor, reverse, concurrent same-key INSERT";
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE reverse_grades (id INTEGER PRIMARY KEY, grade INTEGER, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the reverse-ordered fixture");
    db.execute(
        "CREATE INDEX reverse_grades_grade_idx ON reverse_grades(grade)",
        &HashMap::new(),
    )
    .expect("declare the index the ordered cursor pages through");
    // Two rows share the high key and two share the low key, so the cursor is
    // still inside the high key's posting list when the INSERT lands.
    for (id, grade) in [(1_i64, 100_i64), (2, 100), (5, 50), (6, 50)] {
        db.execute(
            "INSERT INTO reverse_grades (id, grade, payload) VALUES ($id, $grade, $payload)",
            &params([
                ("id", Value::Int64(id)),
                ("grade", Value::Int64(grade)),
                ("payload", Value::Text(format!("row-{id}"))),
            ]),
        )
        .expect("seed a reverse-ordered row");
    }

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(
            "SELECT id FROM reverse_grades ORDER BY grade DESC",
            HashMap::new(),
            paged_limits(1),
            &clock,
        ),
    )
    .expect("open an ordered cursor over the declared index");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor open past its first page"
    );
    let mut emitted = page_ids(&opened.first_page);

    // Ordinary write traffic: a new row sharing the key the cursor is inside.
    // Its own id is 3, so a wrong answer differs in WHICH ids come back, not
    // only how many.
    db.execute(
        "INSERT INTO reverse_grades (id, grade, payload) VALUES (3, 100, 'arrived-after-open')",
        &HashMap::new(),
    )
    .expect("commit an ordinary insert while the cursor is pinned");
    assert_eq!(
        committed_ids(&db, "reverse_grades"),
        vec![1, 2, 3, 5, 6],
        "{journey}: the concurrent insert must really be committed before the cursor is judged"
    );

    emitted.extend(drain(
        &mut opened.cursor,
        1,
        opened.first_page.has_more,
        page_ids,
        journey,
    ));

    // Row 3 was committed after the snapshot, so the pinned cursor owes
    // exactly the four rows that existed when it opened.
    assert_emitted_exactly_once(&emitted, &[1, 2, 5, 6], journey);
    let grades = HashMap::from([(1_i64, 100_i64), (2, 100), (5, 50), (6, 50)]);
    assert!(
        emitted
            .windows(2)
            .all(|pair| grades[&pair[0]] >= grades[&pair[1]]),
        "{journey}: {SNAPSHOT_PROMISE}. ORDER BY grade DESC must stay non-increasing across \
         pages; emitted {emitted:?}"
    );
}

/// The forward twin.  An UPDATE that leaves the paged index key alone still
/// republishes the row's posting, and the posting list keeps its entries in
/// row-identity order — so the new posting lands BEFORE the cursor's offset
/// and pushes an already-returned row back under it.
#[test]
fn an_ordered_cursor_paging_upward_survives_an_update_that_keeps_the_key_it_is_reading() {
    let journey = "ordered cursor, forward, concurrent same-key UPDATE";
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE forward_grades \
         (id INTEGER PRIMARY KEY, grade INTEGER, bucket INTEGER, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the forward-ordered fixture");
    db.execute(
        "CREATE INDEX forward_grades_grade_idx ON forward_grades(grade)",
        &HashMap::new(),
    )
    .expect("declare the index the ordered cursor pages through");
    // A second declared index gives the update something to change, so the
    // write is an ordinary update rather than a no-op for index maintenance.
    db.execute(
        "CREATE INDEX forward_grades_bucket_idx ON forward_grades(bucket)",
        &HashMap::new(),
    )
    .expect("declare the second index the concurrent update moves");
    for (id, grade, bucket) in [
        (11_i64, 10_i64, 1_i64),
        (12, 10, 2),
        (13, 10, 3),
        (21, 20, 4),
    ] {
        db.execute(
            "INSERT INTO forward_grades (id, grade, bucket, payload) \
             VALUES ($id, $grade, $bucket, $payload)",
            &params([
                ("id", Value::Int64(id)),
                ("grade", Value::Int64(grade)),
                ("bucket", Value::Int64(bucket)),
                ("payload", Value::Text(format!("row-{id}"))),
            ]),
        )
        .expect("seed a forward-ordered row");
    }

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(
            "SELECT id FROM forward_grades ORDER BY grade",
            HashMap::new(),
            paged_limits(1),
            &clock,
        ),
    )
    .expect("open an ordered cursor over the declared index");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor open past its first page"
    );
    let mut emitted = page_ids(&opened.first_page);
    assert_eq!(
        emitted,
        vec![11],
        "{journey}: the first page must carry the lowest-key row, so the update below lands on a \
         row the cursor has already returned"
    );

    // Ordinary write traffic: update a row the cursor already returned,
    // leaving its ORDER BY key untouched.
    db.execute(
        "UPDATE forward_grades SET bucket = 99, payload = 'updated-after-open' WHERE id = 11",
        &HashMap::new(),
    )
    .expect("commit an ordinary update while the cursor is pinned");
    let updated_buckets = db
        .execute(
            "SELECT bucket FROM forward_grades WHERE id = 11",
            &HashMap::new(),
        )
        .expect("uncapped read of the updated row");
    assert_eq!(
        updated_buckets.rows.len(),
        1,
        "{journey}: the concurrent update must really be committed before the cursor is judged"
    );
    assert_eq!(
        updated_buckets.rows[0][0],
        Value::Int64(99),
        "{journey}: the concurrent update must really be committed before the cursor is judged"
    );

    emitted.extend(drain(
        &mut opened.cursor,
        1,
        opened.first_page.has_more,
        page_ids,
        journey,
    ));

    assert_emitted_exactly_once(&emitted, &[11, 12, 13, 21], journey);
    let grades = HashMap::from([(11_i64, 10_i64), (12, 10), (13, 10), (21, 20)]);
    assert!(
        emitted
            .windows(2)
            .all(|pair| grades[&pair[0]] <= grades[&pair[1]]),
        "{journey}: {SNAPSHOT_PROMISE}. ORDER BY grade must stay non-decreasing across pages; \
         emitted {emitted:?}"
    );
}

/// A physical scan cursor addresses rows by their place in the table's
/// version list and freezes that list's length when it opens.  A retention
/// cycle reclaims expired versions the cursor's snapshot cannot see; the
/// versions it CAN see move down to fill the gap, and the frozen position
/// then lands past them.
#[test]
fn a_scanning_cursor_emits_every_visible_row_when_retention_reclaims_older_versions() {
    let journey = "physical scan cursor, concurrent retention cycle";
    let db = Arc::new(Database::open_memory());
    // The caller drives retention, so the only prune in this journey is the
    // one this test asks for, between two fetches.
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE scanned_rows \
         (id INTEGER PRIMARY KEY, expires_at TIMESTAMP EXPIRES, payload TEXT) RETAIN 1 HOURS",
        &HashMap::new(),
    )
    .expect("create the retained scan fixture");

    const NEVER: i64 = i64::MAX;
    const ALREADY: i64 = -1;
    // Rows 1..=3 are seeded already-expired and then updated to never expire.
    // Their superseded versions sit at the FRONT of the version list, are
    // invisible to any later snapshot, and are what retention reclaims.
    for id in 1..=3_i64 {
        db.execute(
            "INSERT INTO scanned_rows (id, expires_at, payload) VALUES ($id, $expires, $payload)",
            &params([
                ("id", Value::Int64(id)),
                ("expires", Value::Timestamp(ALREADY)),
                ("payload", Value::Text(format!("superseded-{id}"))),
            ]),
        )
        .expect("seed a row whose first version will age out");
    }
    for id in 4..=9_i64 {
        db.execute(
            "INSERT INTO scanned_rows (id, expires_at, payload) VALUES ($id, $expires, $payload)",
            &params([
                ("id", Value::Int64(id)),
                ("expires", Value::Timestamp(NEVER)),
                ("payload", Value::Text(format!("kept-{id}"))),
            ]),
        )
        .expect("seed a row that never ages out");
    }
    for id in 1..=3_i64 {
        db.execute(
            "UPDATE scanned_rows SET expires_at = $expires, payload = $payload WHERE id = $id",
            &params([
                ("id", Value::Int64(id)),
                ("expires", Value::Timestamp(NEVER)),
                ("payload", Value::Text(format!("current-{id}"))),
            ]),
        )
        .expect("supersede the expiring version with one that never ages out");
    }
    assert_eq!(
        db.__physical_version_count_for_test("scanned_rows"),
        12,
        "{journey}: the fixture needs three reclaimable versions ahead of nine visible rows"
    );

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(
            "SELECT id, payload FROM scanned_rows",
            HashMap::new(),
            paged_limits(2),
            &clock,
        ),
    )
    .expect("open a scanning cursor over the retained table");
    assert!(
        opened.first_page.has_more,
        "{journey}: the fixture must leave the cursor open past its first page"
    );
    let mut emitted = page_ids(&opened.first_page);
    let second = opened
        .cursor
        .fetch(NonZeroUsize::new(2), OwnerReadCancellation::new())
        .expect("page the scanning cursor a second time");
    emitted.extend(page_ids(&second.page));
    assert!(
        second.page.has_more,
        "{journey}: the cursor must still be mid-table when retention runs"
    );

    // Ordinary maintenance traffic: the real retention cycle, reclaiming only
    // versions no live snapshot can see.
    let report = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle while the cursor is pinned");
    assert_eq!(
        report.pruned_rows, 3,
        "{journey}: the retention cycle must really reclaim the three expired versions before \
         the cursor is judged"
    );
    assert_eq!(
        db.__physical_version_count_for_test("scanned_rows"),
        9,
        "{journey}: only the nine snapshot-visible rows remain physically after the cycle"
    );

    emitted.extend(drain(
        &mut opened.cursor,
        2,
        second.page.has_more,
        page_ids,
        journey,
    ));

    assert_emitted_exactly_once(&emitted, &(1..=9_i64).collect::<Vec<i64>>(), journey);
}

/// A bounded graph edge cursor remembers its place in one source's adjacency
/// as an integer offset and the length that adjacency had when it opened.
/// Retention reclaiming some target nodes shortens that adjacency; the
/// surviving edges move down, and the offset then lands past them.
#[test]
fn a_graph_edge_cursor_emits_every_surviving_edge_when_retention_reclaims_nodes() {
    let journey = "graph edge cursor, concurrent retention cycle";
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

    const NEVER: i64 = i64::MAX;
    const ALREADY: i64 = -1;
    let node = |ordinal: u128| Uuid::from_u128(0x5717_0000_0000_0000_0000_0000_0000_0000 + ordinal);
    let source = node(0);
    let targets: Vec<Uuid> = (1..=8_u128).map(node).collect();
    // Targets 2 and 3 age out; every other node stays. They sit early in the
    // source's adjacency, ahead of where the cursor will be paused.
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
    // One statement per edge, so the source's adjacency order is the order
    // written here.
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

    let clock = ManualClock::default();
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(
            "SELECT s, t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             COLUMNS (a.id AS s, b.id AS t))",
            HashMap::new(),
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

    // Retention runs while the cursor's snapshot registration is live, so the
    // expiring nodes — visible at that snapshot — must be DEFERRED, not
    // reclaimed underneath the cursor. Their physical reclaim is owed only
    // after the last registration releases.
    let report = db
        .run_pruning_cycle_checked()
        .expect("run one real retention cycle while the graph cursor is pinned");
    assert_eq!(
        report.pruned_rows, 0,
        "{journey}: retention must defer rows a registered cursor snapshot still sees"
    );

    emitted.extend(drain(
        &mut opened.cursor,
        2,
        second.page.has_more,
        page_targets,
        journey,
    ));

    opened
        .cursor
        .close()
        .expect("close the graph cursor so its snapshot registration releases");
    let after_close = db
        .run_pruning_cycle_checked()
        .expect("run the retention cycle again after the cursor released");
    assert_eq!(
        after_close.pruned_rows, 2,
        "{journey}: the deferred reclaim must really happen once no snapshot pins the rows"
    );

    // Every edge whose endpoints survived the whole journey was visible at the
    // snapshot and never changed, so each owes exactly one appearance. Edges
    // to the reclaimed nodes were visible at the snapshot too, so pages that
    // already carried them are correct and they are not asserted here.
    let survivors: Vec<Uuid> = targets
        .iter()
        .copied()
        .filter(|target| !expiring.contains(target))
        .collect();
    let surviving_emissions: Vec<Uuid> = emitted
        .iter()
        .copied()
        .filter(|target| !expiring.contains(target))
        .collect();
    assert_emitted_exactly_once(&surviving_emissions, &survivors, journey);
}
