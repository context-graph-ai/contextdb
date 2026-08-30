//! A bounded read of a transaction's own staged graph work is still bounded.
//!
//! A writing session is entitled to read back what it has staged. It is NOT
//! entitled to have the store take a copy of the whole write set to answer
//! that read: a session that has staged thousands of edges carrying real
//! payloads is holding tens of megabytes, and a read that clones them has
//! spent the operator's whole memory budget on one query while reporting that
//! it stayed inside a one-mebibyte ceiling. A ceiling that a read can leave
//! by reading its own transaction is not a ceiling.
//!
//! So each pin here has exactly two acceptable outcomes and no third: the
//! read answers while the PROCESS stays inside the ceiling it was given (plus
//! a fixed, generous slack for the answer and the plan), or it refuses with
//! the typed refusal that names the ceiling it crossed. Silently doing the
//! work and staying quiet about it is the failure.
//!
//! The second pin is about a walk read one page at a time: paging exists so a
//! caller can take a large answer in pieces, which is only true if page ten
//! costs what page two cost. A cursor that restarts its walk on every fetch
//! turns a linear read into a quadratic one, and the caller finds out as a
//! refusal on a ceiling that fits the work the walk really needs. That is
//! measured through the WORK CEILING itself -- the smallest ceiling each
//! shape needs -- never through elapsed time.

use contextdb_core::read_contract::{
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicIsize, Ordering};
use uuid::Uuid;

/// Live heap bytes, and the high-water mark since it was last reset.
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

/// Start the high-water mark again from where the process stands now, so what
/// is measured next is the growth of the call under test alone.
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

fn node(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x005A_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

const HUB: u128 = 1;
/// Enough staged edges, carrying enough payload, that a copy of the write set
/// is unmistakable against any fixed cost.
const STAGED_EDGES: u128 = 2_000;
const COMMITTED_EDGES: u128 = 400;
const NOTE_BYTES: usize = 4_096;
const VECTOR_DIMENSIONS: usize = 256;

/// Roughly ten mebibytes of staged payload.
fn staged_payload_bytes() -> i64 {
    STAGED_EDGES as i64 * (NOTE_BYTES + VECTOR_DIMENSIONS * 4) as i64
}

const MEMORY_CEILING: u64 = 1024 * 1024;
/// What a read may hold beyond its ceiling before the number stops being an
/// answer and starts being a copy of the store. Generous on purpose: the
/// failure this catches is a whole write set, not a rounding difference.
const SLACK_BYTES: i64 = 4 * 1024 * 1024;

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

fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .expect("create the node table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");
    database
        .execute(
            "INSERT INTO nodes (id) VALUES ($id)",
            &params(vec![("id", Value::Uuid(node(HUB)))]),
        )
        .expect("insert the hub node");
    let tx = database.begin().expect("begin the committed batch");
    for ordinal in 0..COMMITTED_EDGES {
        database
            .insert_edge(
                tx,
                node(HUB),
                node(10_000 + ordinal),
                "LINKS".to_owned(),
                HashMap::new(),
            )
            .expect("commit an edge before the transaction under test opens");
    }
    database.commit(tx).expect("commit the committed batch");
    database
}

/// Stage a large graph write set inside the SQL session's transaction: many
/// edges carrying real payload, and many removals of edges that are already
/// committed.
///
/// The bounded view watches the SQL session's transaction, while the graph
/// doors take a transaction id, so the fixture learns the id the session is
/// about to take and stages on it. Nothing trusts that pairing: the pins below
/// first check what the session's OWN read sees, and work staged on any other
/// transaction would not be in it.
fn stage_a_large_write_set(database: &Database) {
    let tx = database.next_tx();
    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    let note = "x".repeat(NOTE_BYTES);
    for ordinal in 0..STAGED_EDGES {
        database
            .insert_edge(
                tx,
                node(HUB),
                node(20_000 + ordinal),
                "LINKS".to_owned(),
                HashMap::from([
                    ("note".to_owned(), Value::Text(note.clone())),
                    (
                        "embedding".to_owned(),
                        Value::Vector((0..VECTOR_DIMENSIONS).map(|index| index as f32).collect()),
                    ),
                ]),
            )
            .expect("stage an edge carrying real payload");
    }
    for ordinal in 0..COMMITTED_EDGES {
        database
            .delete_edge(tx, node(HUB), node(10_000 + ordinal), "LINKS")
            .expect("stage the removal of a committed edge");
    }
}

const ONE_HOP: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                       WHERE a.id = $hub COLUMNS (b.id AS t))";

fn hub_params() -> HashMap<String, Value> {
    params(vec![("hub", Value::Uuid(node(HUB)))])
}

/// The ceiling a typed refusal says it crossed, or a panic naming what came
/// back instead.
fn crossed_ceiling(error: &Error) -> ReadFailureLimit {
    let Error::ReadFailure(failure) = error else {
        panic!(
            "a read that cannot answer inside its ceiling says so in the typed refusal, not as \
             prose: {error:?}"
        );
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "the refusal is reported as a crossed ceiling: {failure:?}"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail().clone() else {
        panic!("a crossed ceiling carries its typed detail: {failure:?}");
    };
    detail.limit
}

/// One read of the staged write set under the given ceilings. Two outcomes are
/// acceptable and no third: an answer taken without the process leaving the
/// ceiling, or the typed refusal that names it.
fn read_under(what: &str, limits: ReadLimits, expected_limit: ReadFailureLimit) {
    let database = seeded();
    stage_a_large_write_set(&database);

    let staged = database
        .execute(ONE_HOP, &hub_params())
        .expect("the session reads its own staged work");
    assert_eq!(
        staged.rows.len(),
        STAGED_EDGES as usize,
        "the fixture is only adversarial if the transaction really staged {STAGED_EDGES} edges \
         and removed every committed one"
    );

    let before = live_bytes();
    restart_peak();
    let outcome = database
        .read_session(limits)
        .expect("open a bounded read view")
        .execute(ONE_HOP, &hub_params());
    let peak = peak_growth_since(before);
    let allowance = limits.memory as i64 + SLACK_BYTES;
    println!(
        "OBSERVED {what}: staged payload {} bytes, read peak {peak} bytes, allowance {allowance}, \
         outcome {}",
        staged_payload_bytes(),
        if outcome.is_ok() {
            "answered"
        } else {
            "refused"
        }
    );

    match outcome {
        Ok(answered) => {
            assert_eq!(
                answered.rows.len(),
                STAGED_EDGES as usize,
                "an answer is the whole answer or it is a refusal, never a quiet truncation"
            );
            assert!(
                peak <= allowance,
                "{what}: the read answered, but the process took {peak} bytes to do it against a \
                 ceiling of {} plus {SLACK_BYTES} slack -- a read that copies the write set has \
                 spent the operator's budget while reporting that it stayed inside it",
                limits.memory
            );
        }
        Err(error) => {
            let crossed = crossed_ceiling(&error);
            assert_eq!(
                crossed, expected_limit,
                "{what}: the refusal names the ceiling that was actually tightened: {error}"
            );
            assert!(
                peak <= allowance,
                "{what}: the read refused, but only after taking {peak} bytes -- a refusal that \
                 first copies the write set has already done the harm the ceiling exists to \
                 prevent"
            );
        }
    }
}

#[test]
fn reading_a_large_staged_write_set_stays_inside_a_small_memory_ceiling() {
    read_under(
        "a small memory ceiling",
        ReadLimits {
            memory: MEMORY_CEILING,
            ..roomy()
        },
        ReadFailureLimit::Memory,
    );
}

#[test]
fn reading_a_large_staged_write_set_stays_inside_a_small_work_ceiling() {
    read_under(
        "a small work ceiling",
        ReadLimits {
            work: 200,
            ..roomy()
        },
        ReadFailureLimit::Work,
    );
}

/// How many rows a page of the paged walk carries, and how many pages that
/// makes. A rescanning cursor costs the page count times what a walking one
/// costs, so the two shapes are far apart at this many pages.
const PAGE_ROWS: u64 = 100;

/// The smallest work ceiling, found by doubling, at which `attempt` succeeds.
/// Doubling is what makes this a measurement rather than a guess: the answer
/// is whatever the door really needs, not a figure the fixture chose.
fn smallest_work_ceiling(
    what: &str,
    reason: &std::cell::RefCell<Option<String>>,
    attempt: impl Fn(ReadLimits) -> bool,
) -> u64 {
    let mut work = 16_u64;
    while work <= 1 << 24 {
        if attempt(ReadLimits { work, ..roomy() }) {
            println!("OBSERVED {what}: smallest work ceiling {work}");
            return work;
        }
        work *= 2;
    }
    panic!(
        "{what} could not be answered under any work ceiling this search reaches, and the last \
         thing that stopped it was: {:?}",
        reason.borrow()
    );
}

/// A committed graph of the same size and payload as the staged one, with no
/// transaction open. Paging is measured here because a cursor is refused while
/// a transaction is in progress -- see the pin directly above -- so a paged
/// walk of STAGED work is not a shape the store offers.
fn committed_payload_graph() -> Database {
    let database = seeded();
    let note = "x".repeat(NOTE_BYTES);
    let tx = database.begin().expect("begin the payload batch");
    for ordinal in 0..STAGED_EDGES {
        database
            .insert_edge(
                tx,
                node(HUB),
                node(20_000 + ordinal),
                "LINKS".to_owned(),
                HashMap::from([
                    ("note".to_owned(), Value::Text(note.clone())),
                    (
                        "embedding".to_owned(),
                        Value::Vector((0..VECTOR_DIMENSIONS).map(|index| index as f32).collect()),
                    ),
                ]),
            )
            .expect("commit an edge carrying real payload");
    }
    database.commit(tx).expect("commit the payload batch");
    database
}

/// A cursor outlives the call that opened it while a transaction need not, so
/// the store refuses to open one inside a transaction rather than hand out a
/// cursor whose ground can vanish. That refusal is already pinned for a
/// relational statement; what is pinned here is that it holds for a graph walk
/// too, and that the walk it refuses is one the store can otherwise answer.
///
/// This is also why the paging measurement below reads a COMMITTED graph: a
/// paged walk of STAGED work is not a shape the store offers.
#[test]
fn a_cursor_over_staged_work_is_refused_while_the_transaction_is_open() {
    let database = seeded();
    stage_a_large_write_set(&database);

    let refusal = database
        .read_session(ReadLimits {
            cursor_page_rows: PAGE_ROWS,
            ..roomy()
        })
        .expect("open a bounded read view")
        .open_cursor(ONE_HOP, &hub_params())
        .err()
        .expect("a cursor cannot open over a session that has a transaction in progress");
    println!("OBSERVED opening a cursor inside an open transaction: {refusal}");
    let Error::ReadFailure(failure) = &refusal else {
        panic!("the refusal is typed, not prose: {refusal:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::CursorTransactionActive,
        "the refusal is the one the reading surface already has for this: {failure:?}"
    );

    // The same walk still answers in one call inside the transaction, so the
    // refusal is about the CURSOR outliving the transaction, not about the
    // read being impossible.
    let answered = database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(ONE_HOP, &hub_params())
        .expect("the same walk answers in one call inside the transaction");
    assert_eq!(
        answered.rows.len(),
        STAGED_EDGES as usize,
        "the walk the cursor was refused is a walk the store can answer"
    );
}

/// Paging exists so a caller can take a large answer in pieces. A cursor that
/// stops part-way has not given the caller a smaller answer, it has given them
/// a WRONG one -- the rows it never delivered are indistinguishable from rows
/// the walk does not have.
#[test]
fn paging_through_a_walk_delivers_every_row_the_walk_has() {
    let database = committed_payload_graph();
    let expected_rows = database
        .execute(ONE_HOP, &hub_params())
        .expect("the executor answers the walk")
        .rows
        .len();

    let session = database
        .read_session(ReadLimits {
            cursor_page_rows: PAGE_ROWS,
            ..roomy()
        })
        .expect("open a bounded read view");
    let mut cursor = session
        .open_cursor(ONE_HOP, &hub_params())
        .expect("open a cursor over the walk");

    // The cursor's FIRST page is produced atomically when the cursor opens --
    // it is part of what opening a cursor gives you, not something a later
    // fetch re-delivers. A reader that starts counting at the first `fetch`
    // discards it and mistakes its own omission for lost rows.
    let mut delivered = cursor.first_page().rows.len();
    let mut pages = 1_usize;
    let mut has_more = cursor.first_page().has_more;
    let mut stopped_by = None;
    while has_more {
        match cursor.fetch(NonZeroUsize::new(PAGE_ROWS as usize)) {
            Ok(page) => {
                delivered += page.rows.len();
                pages += 1;
                has_more = page.has_more;
            }
            Err(error) => {
                stopped_by = Some(error.to_string());
                break;
            }
        }
    }
    let _ = cursor.close();
    println!(
        "OBSERVED paging a walk of {expected_rows} rows: {pages} pages, {delivered} rows, \
         stopped by {stopped_by:?}"
    );

    assert_eq!(
        stopped_by, None,
        "the ceilings here are far above what this walk needs, so nothing should stop the cursor \
         part-way: it stopped at page {pages} after {delivered} of {expected_rows} rows"
    );
    assert_eq!(
        delivered, expected_rows,
        "a cursor hands over every row the walk has, in pieces: it delivered {delivered} of \
         {expected_rows} across {pages} pages"
    );
}

#[test]
fn paging_through_a_walk_costs_what_walking_it_once_costs() {
    let database = committed_payload_graph();
    // How many rows the walk really has, taken from the executor rather than
    // restated: the fixture commits payload edges on top of the plain ones
    // already there, and a hand-written count would silently make every paged
    // attempt look like a failure.
    let expected_rows = database
        .execute(ONE_HOP, &hub_params())
        .expect("the executor answers the walk")
        .rows
        .len();

    let single_refusal = std::cell::RefCell::new(None::<String>);
    let in_one_call =
        smallest_work_ceiling("the whole walk in one call", &single_refusal, |limits| {
            database
                .read_session(limits)
                .expect("open a bounded read view")
                .execute(ONE_HOP, &hub_params())
                .is_ok()
        });

    // Why a paged attempt failed, so a failure that is not about the ceiling
    // cannot be read as one.
    let last_refusal = std::cell::RefCell::new(None::<String>);
    let a_page_at_a_time = smallest_work_ceiling(
        "the same walk one page at a time",
        &last_refusal,
        |limits| {
            let session = database
                .read_session(ReadLimits {
                    cursor_page_rows: PAGE_ROWS,
                    ..limits
                })
                .expect("open a bounded read view");
            let mut cursor = match session.open_cursor(ONE_HOP, &hub_params()) {
                Ok(cursor) => cursor,
                Err(error) => {
                    *last_refusal.borrow_mut() = Some(error.to_string());
                    return false;
                }
            };
            // The page the cursor produced when it opened counts too.
            let mut delivered = cursor.first_page().rows.len();
            let mut has_more = cursor.first_page().has_more;
            while has_more {
                match cursor.fetch(NonZeroUsize::new(PAGE_ROWS as usize)) {
                    Ok(page) => {
                        delivered += page.rows.len();
                        has_more = page.has_more;
                    }
                    Err(error) => {
                        *last_refusal.borrow_mut() = Some(error.to_string());
                        return false;
                    }
                }
            }
            let _ = cursor.close();
            delivered == expected_rows
        },
    );

    let pages = (expected_rows as u128).div_ceil(PAGE_ROWS as u128);
    println!(
        "OBSERVED paging: one call needs {in_one_call}, {pages} pages need {a_page_at_a_time}"
    );
    assert!(
        a_page_at_a_time <= in_one_call * 4,
        "reading a walk one page at a time examines the same items the walk examines, so it needs \
         about the same work ceiling: one call needs {in_one_call} and {pages} pages need \
         {a_page_at_a_time}. A figure that grows with the page count is a cursor restarting its \
         walk on every fetch, which turns a linear read into a quadratic one."
    );
    drop(last_refusal);
}
