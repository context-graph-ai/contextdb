//! A ceiling covers the work a hop really does, not the size of the hop.
//!
//! A graph hop whose start is named by a property rather than an identifier
//! has to find that start before it can walk anywhere, and with no index over
//! the property it finds it by reading the node table. So a query that
//! returns two edges can read a hundred thousand rows to decide which two. An
//! operator who sets a ceiling is bounding the second number, because the
//! first is not the one that can hurt them. A door that resolves the start to
//! completion and only then notices the ceiling has honoured nothing: the
//! machine has already done the work, the memory has already been held, and
//! the refusal -- if it comes at all -- arrives after the damage rather than
//! instead of it.
//!
//! So each arm has exactly two acceptable outcomes and no third: the read
//! refuses with the typed refusal that names the ceiling that was tightened,
//! or it answers with the PROCESS having stayed inside that ceiling plus a
//! fixed, generous slack. A late answer that cost the whole start resolution
//! is the failure, and so is a refusal that first paid for it.
//!
//! The accountant is checked either side of every arm, because a read that
//! strands bytes shrinks the operator's budget with every query.

use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering};
use uuid::Uuid;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

struct PeakTrackingAllocator;

fn note_growth(delta: isize) {
    let live = LIVE_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for PeakTrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_growth(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakTrackingAllocator = PeakTrackingAllocator;

fn live_bytes() -> i64 {
    LIVE_BYTES.load(Ordering::Relaxed) as i64
}

fn restart_peak() {
    PEAK_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
}

fn peak_growth_since(before: i64) -> i64 {
    PEAK_BYTES.load(Ordering::Relaxed) as i64 - before
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Nodes carrying the property the hop starts from, none of them indexed.
/// Large enough that reading them all is unmistakably past any ceiling here,
/// and small enough that seeding them is not the slowest thing in the run.
const NODE_ROWS: u128 = 12_000;
/// Bytes of payload on each node, so the memory a full resolution would hold
/// is far past the memory ceiling rather than close to it.
const PAYLOAD_BYTES: usize = 256;
/// How many edges the hop actually returns.
const ANSWER_ROWS: u128 = 3;
/// The one node the hop starts from, named by a property no index covers.
const START: u128 = 0;
const START_KIND: &str = "root";

/// What a read may hold beyond its ceiling before the number stops being an
/// answer and starts being a copy of the table it scanned.
const SLACK_BYTES: i64 = 4 * 1024 * 1024;

/// A clock that moves on every time anything looks at it. A read enforcing a
/// deadline has to keep asking what time it is, so under this clock time
/// passes in step with the read's own progress rather than with the machine
/// -- which is what lets a deadline be crossed part-way through the work
/// without the test ever reading a real clock or waiting for one.
#[derive(Clone)]
struct ClockThatMovesWhenLookedAt {
    now_ms: Arc<AtomicU64>,
}

impl DeadlineClock for ClockThatMovesWhenLookedAt {
    fn now_ms(&self) -> u64 {
        self.now_ms.fetch_add(1, Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

/// Short enough that it is crossed while the work is still going on, and long
/// enough that the read gets started first.
const DEADLINE_MS: u64 = 32;

fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn id(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00B2_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn charged(database: &Database) -> u64 {
    database.accountant().usage().used as u64
}

fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, kind TEXT, payload TEXT)",
            &empty(),
        )
        .expect("create the node table, with no index over the kind");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");

    database
        .execute("BEGIN", &empty())
        .expect("open the seeding transaction");
    let payload = "x".repeat(PAYLOAD_BYTES);
    for ordinal in 0..NODE_ROWS {
        let kind = if ordinal == START {
            START_KIND.to_owned()
        } else {
            format!("kind-{ordinal}")
        };
        database
            .execute(
                "INSERT INTO nodes (id, kind, payload) VALUES ($id, $kind, $payload)",
                &params(vec![
                    ("id", Value::Uuid(id(ordinal))),
                    ("kind", Value::Text(kind)),
                    ("payload", Value::Text(payload.clone())),
                ]),
            )
            .expect("seed a node");
    }
    // The edges go in through the same door the nodes did, so the hop is
    // resolved the way a caller's own writes would be resolved.
    for ordinal in 0..ANSWER_ROWS {
        database
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, 'LINKS')",
                &params(vec![
                    ("id", Value::Uuid(id(900_000 + ordinal))),
                    ("source", Value::Uuid(id(START))),
                    ("target", Value::Uuid(id(1 + ordinal))),
                ]),
            )
            .expect("connect the start to a neighbour");
    }
    database
        .execute("COMMIT", &empty())
        .expect("commit the seeding transaction");
    database
}

/// The start is named by a property, not by an identifier, and no index
/// covers it -- so finding where the hop begins means reading the node table.
const OUTER: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                     WHERE a.kind = 'root' COLUMNS (b.id AS t))";

/// What came back, said plainly enough to tell a ceiling refusal from a door
/// that cannot run the statement at all.
enum Outcome {
    Answered(usize),
    CrossedCeiling(ReadFailureLimit),
    RefusedOtherwise(String),
}

fn classify(result: contextdb_core::Result<usize>) -> Outcome {
    match result {
        Ok(rows) => Outcome::Answered(rows),
        Err(Error::ReadFailure(failure)) => {
            if failure.kind() == ReadFailureKind::OwnerLimitExceeded
                && let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail().clone()
            {
                Outcome::CrossedCeiling(detail.limit)
            } else {
                Outcome::RefusedOtherwise(format!("{failure:?}"))
            }
        }
        Err(other) => Outcome::RefusedOtherwise(other.to_string()),
    }
}

fn described(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Answered(rows) => format!("answered {rows} rows"),
        Outcome::CrossedCeiling(limit) => format!("refused, naming {limit:?}"),
        Outcome::RefusedOtherwise(error) => format!("refused otherwise: {error}"),
    }
}

/// Run the outer statement one way under one tightened ceiling and hold the
/// result to the two outcomes a ceiling allows.
#[allow(clippy::too_many_arguments)]
fn arm(
    database: &Database,
    what: &str,
    limits: ReadLimits,
    expected: ReadFailureLimit,
    really_examined: u64,
    peak_allowance: i64,
    clock: Option<ClockThatMovesWhenLookedAt>,
    through_a_cursor: bool,
    failures: &mut Vec<String>,
) {
    let charged_before = charged(database);
    let before = live_bytes();
    restart_peak();

    let open = |limits: ReadLimits| match &clock {
        Some(clock) => database.read_session_with_clock_for_test(limits, Arc::new(clock.clone())),
        None => database.read_session(limits),
    };
    let result = if through_a_cursor {
        run_through_a_cursor(&open, limits)
    } else {
        open(limits)
            .expect("open a bounded read view")
            .execute(OUTER, &start_params())
            .map(|answered| answered.rows.len())
    };
    let outcome = classify(result);
    let peak = peak_growth_since(before);
    let charged_after = charged(database);
    let allowance = peak_allowance;
    println!(
        "OBSERVED {what}: {} | peak {peak} of an allowance of {allowance} | charged \
         {charged_before} then {charged_after}",
        described(&outcome)
    );

    match &outcome {
        Outcome::CrossedCeiling(limit) if *limit == expected => {}
        Outcome::CrossedCeiling(limit) => failures.push(format!(
            "{what}: the refusal names {limit:?} rather than the ceiling that was tightened, \
             {expected:?}"
        )),
        Outcome::Answered(rows) => {
            // An answer under a ceiling smaller than the work the statement
            // really does means the ceiling bounded nothing: the machine did
            // the work and the number the operator set was decoration.
            if expected == ReadFailureLimit::ActiveMs {
                failures.push(format!(
                    "{what}: the read answered {rows} rows although its deadline passed while it \
                     was still working, so the deadline bounded nothing"
                ));
            }
            if expected == ReadFailureLimit::Work && really_examined > limits.work {
                failures.push(format!(
                    "{what}: the read answered {rows} rows under a ceiling of {} units while the \
                     statement really examines {really_examined}, so the ceiling bounded nothing",
                    limits.work
                ));
            }
            if peak > allowance {
                failures.push(format!(
                    "{what}: the read answered {rows} rows but the process took {peak} bytes \
                     against an allowance of {allowance} -- it resolved the start over the whole \
                     table and honoured the ceiling with nothing but a late answer"
                ));
            }
        }
        Outcome::RefusedOtherwise(error) => failures.push(format!(
            "{what}: the door did not reach the ceiling at all -- it cannot run the statement: \
             {error}"
        )),
    }
    if peak > allowance {
        failures.push(format!(
            "{what}: the process took {peak} bytes against an allowance of {allowance}; a \
             refusal that first pays for the whole start resolution has already done the harm \
             the ceiling exists to prevent"
        ));
    }
    if charged_after != charged_before {
        failures.push(format!(
            "{what}: the store was charged {charged_before} before the read and {charged_after} \
             after it"
        ));
    }
}

fn start_params() -> HashMap<String, Value> {
    empty()
}

fn run_through_a_cursor(
    open: &dyn Fn(ReadLimits) -> contextdb_core::Result<contextdb_engine::ReadSession>,
    limits: ReadLimits,
) -> contextdb_core::Result<usize> {
    let session = open(limits).expect("open a bounded read view");
    let mut cursor = session.open_cursor(OUTER, &start_params())?;
    let mut delivered = cursor.first_page().rows.len();
    let mut has_more = cursor.first_page().has_more;
    while has_more {
        let page = cursor.fetch(NonZeroUsize::new(64))?;
        delivered += page.rows.len();
        has_more = page.has_more;
    }
    cursor.close()?;
    Ok(delivered)
}

#[test]
fn a_hop_that_reads_a_whole_table_to_find_its_start_is_bounded_by_the_ceiling_that_covers_it() {
    let database = seeded();
    let whole = database
        .execute(OUTER, &start_params())
        .expect("the executor answers the hop");
    assert_eq!(
        whole.rows.len() as u128,
        ANSWER_ROWS,
        "the answer is small; it is the start resolution behind it that is not"
    );

    // What the statement really costs, taken from the executor rather than
    // restated: it is the figure the work ceiling is supposed to bound.
    let really_examined = whole.trace.rows_examined;
    assert_eq!(
        whole.trace.physical_plan, "AdjacencyProbe",
        "the executor finds the start and then probes from it: {:?}",
        whole.trace
    );
    assert!(
        really_examined >= NODE_ROWS as u64,
        "finding the start means reading the node table, so the work behind this small answer is \
         at least the {NODE_ROWS} nodes: {really_examined}"
    );

    // And the door under test must take that same route before its ceilings
    // are worth measuring -- a door that answers some other way is not the
    // one this file is about, and its ceilings would be measuring nothing.
    let unbounded = database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(OUTER, &start_params())
        .expect("a bounded read answers the hop when nothing is tightened");
    assert_eq!(
        unbounded.trace.physical_plan, whole.trace.physical_plan,
        "a bounded read reaches this answer the same way the executor does, so what its ceilings \
         bound is the start resolution: the executor says {:?} and it says {:?}",
        whole.trace.physical_plan, unbounded.trace.physical_plan
    );
    assert_eq!(
        unbounded.trace.rows_examined, really_examined,
        "and it does the same amount of work getting there: the executor examined \
         {really_examined} and it examined {}",
        unbounded.trace.rows_examined
    );

    let mut failures = Vec::new();
    // Room for a handful of rows, nowhere near a read of every node.
    let tight_work = ReadLimits {
        work: 256,
        ..roomy()
    };
    // Room for the answer, nowhere near the payload every node holds.
    let tight_memory = ReadLimits {
        memory: 256 * 1024,
        ..roomy()
    };

    arm(
        &database,
        "a work ceiling smaller than the start resolution",
        tight_work,
        ReadFailureLimit::Work,
        really_examined,
        tight_work.memory as i64 + SLACK_BYTES,
        None,
        false,
        &mut failures,
    );
    arm(
        &database,
        "a memory ceiling smaller than the node table",
        tight_memory,
        ReadFailureLimit::Memory,
        really_examined,
        tight_memory.memory as i64 + SLACK_BYTES,
        None,
        false,
        &mut failures,
    );
    arm(
        &database,
        "the same work ceiling, read through a cursor",
        tight_work,
        ReadFailureLimit::Work,
        really_examined,
        tight_work.memory as i64 + SLACK_BYTES,
        None,
        true,
        &mut failures,
    );
    arm(
        &database,
        "the same memory ceiling, read through a cursor",
        tight_memory,
        ReadFailureLimit::Memory,
        really_examined,
        tight_memory.memory as i64 + SLACK_BYTES,
        None,
        true,
        &mut failures,
    );

    // A deadline the read crosses while the work is still going on. Memory is
    // left roomy here on purpose -- what is being bounded is time -- so the
    // peak is held to a fixed budget instead: a read stopped by its deadline
    // part-way should not be holding everything it would have read.
    let tight_deadline = ReadLimits {
        active_ms: DEADLINE_MS,
        ..roomy()
    };
    for through_a_cursor in [false, true] {
        arm(
            &database,
            if through_a_cursor {
                "a deadline crossed part-way, read through a cursor"
            } else {
                "a deadline crossed part-way through the start resolution"
            },
            tight_deadline,
            ReadFailureLimit::ActiveMs,
            really_examined,
            SLACK_BYTES,
            Some(ClockThatMovesWhenLookedAt {
                now_ms: Arc::new(AtomicU64::new(0)),
            }),
            through_a_cursor,
            &mut failures,
        );
    }

    assert!(
        failures.is_empty(),
        "an operator's ceiling bounds the work a hop does to find its start, not the size of the \
         answer it produces:\n{}",
        failures.join("\n")
    );
}
