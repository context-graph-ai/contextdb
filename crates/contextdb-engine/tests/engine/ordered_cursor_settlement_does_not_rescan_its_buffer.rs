//! What a cursor charges for one page does not depend on how much it still
//! has to hand out.
//!
//! An ordered read over an unindexed column has to sort before it can answer,
//! so a cursor over one holds the sorted rows and hands them out a page at a
//! time. That is the shape paging exists for: a caller takes a hundred-
//! thousand-row answer in pieces without holding it all. It only works if
//! settling a page costs what a page costs. A cursor that walks what it is
//! still holding on every fetch turns a linear read into a quadratic one, and
//! the caller meets it as a read that refuses under a ceiling sized for the
//! work the answer really needs.
//!
//! The work counter is not reachable from a live-database read session, so
//! every claim here is measured through the CEILING that counter bounds: the
//! smallest ceiling each shape needs, found by doubling, which is a
//! measurement rather than a guess because the answer is whatever the door
//! really requires. Nothing here reads a clock.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::Database;
use std::collections::HashMap;
use std::num::NonZeroUsize;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// The buffered answer under test. Large enough that walking it once per
/// fetch is a different order of cost from handing out a page.
const LARGE_ROWS: i64 = 9_000;
/// A second answer of the same shape, two orders of magnitude smaller. What
/// separates "a fetch costs what a fetch costs" from "a fetch costs what the
/// buffer holds" is whether these two need the same headroom per fetch.
const SMALL_ROWS: i64 = 1_000;
const PAGE_ROWS: u64 = 100;

/// The most a shape may need over another before the two are doing different
/// amounts of work rather than the same amount with noise around it.
const HEADROOM_MULTIPLE: u64 = 4;

fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1 << 30,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: PAGE_ROWS,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// Rows whose sort column is deliberately out of insertion order, so the sort
/// really has to move them and cannot be elided.
fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE buffered (id INTEGER PRIMARY KEY, bucket TEXT, sort_key INTEGER)",
            &empty(),
        )
        .expect("create the fixture table");

    database
        .execute("BEGIN", &empty())
        .expect("open the seeding transaction");
    let mut id = 0_i64;
    for (bucket, count) in [("small", SMALL_ROWS), ("large", LARGE_ROWS)] {
        for _ in 0..count {
            // A scatter that no insertion order matches, derived from the id
            // so the fixture is the same on every run.
            let sort_key = (id.wrapping_mul(2_654_435_761)) % 1_000_003;
            database
                .execute(
                    "INSERT INTO buffered (id, bucket, sort_key) VALUES ($id, $bucket, $sort_key)",
                    &params(vec![
                        ("id", Value::Int64(id)),
                        ("bucket", Value::Text(bucket.to_owned())),
                        ("sort_key", Value::Int64(sort_key)),
                    ]),
                )
                .expect("seed a row");
            id += 1;
        }
    }
    database
        .execute("COMMIT", &empty())
        .expect("commit the seeding transaction");
    database
}

const ORDERED: &str = "SELECT id FROM buffered WHERE bucket = $bucket ORDER BY sort_key, id";

fn bucket(name: &str) -> HashMap<String, Value> {
    params(vec![("bucket", Value::Text(name.to_owned()))])
}

/// The smallest work ceiling, found by doubling, at which `attempt` succeeds.
/// Doubling makes this a measurement: the figure is whatever the door really
/// needs, not one the fixture chose.
/// Where the search starts and stops. The floor is above anything these
/// shapes could plausibly need for bookkeeping alone, so the search is a
/// handful of doublings rather than twenty; the ceiling is far above what
/// handing out this answer once really costs, so a shape that cannot be done
/// under it is not being squeezed, it is doing a different amount of work.
const SEARCH_FLOOR: u64 = 1_024;
const SEARCH_CEILING: u64 = 1 << 20;

fn smallest_work_ceiling(what: &str, attempt: impl Fn(ReadLimits) -> bool) -> u64 {
    let mut work = SEARCH_FLOOR;
    while work <= SEARCH_CEILING {
        if attempt(ReadLimits { work, ..roomy() }) {
            println!("MEASURED {what}: smallest work ceiling {work}");
            return work;
        }
        work *= 2;
    }
    panic!("{what} could not be done under any work ceiling this search reaches");
}

/// Open a cursor and take `fetches` pages from it, counting the page the
/// cursor opened with. `None` means take every page.
fn open_and_take(
    database: &Database,
    which: &str,
    limits: ReadLimits,
    fetches: Option<usize>,
) -> Option<usize> {
    let session = database.read_session(limits).ok()?;
    let mut cursor = session.open_cursor(ORDERED, &bucket(which)).ok()?;
    let mut delivered = cursor.first_page().rows.len();
    let mut has_more = cursor.first_page().has_more;
    let mut taken = 0_usize;
    while has_more && fetches.is_none_or(|wanted| taken < wanted) {
        let page = cursor.fetch(NonZeroUsize::new(PAGE_ROWS as usize)).ok()?;
        delivered += page.rows.len();
        has_more = page.has_more;
        taken += 1;
    }
    let _ = cursor.close();
    Some(delivered)
}

#[test]
fn settling_one_page_costs_the_same_whether_much_or_little_is_still_buffered() {
    let database = seeded();

    let mut needs = HashMap::new();
    for (which, rows) in [("large", LARGE_ROWS), ("small", SMALL_ROWS)] {
        let whole = database
            .execute(ORDERED, &bucket(which))
            .unwrap_or_else(|error| panic!("the executor answers the {which} answer: {error}"));
        assert_eq!(
            whole.rows.len() as i64,
            rows,
            "the fixture holds the {which} answer this shape is about"
        );

        let to_open = smallest_work_ceiling(
            &format!("opening a cursor over the {which} answer"),
            |limits| open_and_take(&database, which, limits, Some(0)).is_some(),
        );
        let to_open_and_take_one = smallest_work_ceiling(
            &format!("opening a cursor over the {which} answer and taking one more page"),
            |limits| open_and_take(&database, which, limits, Some(1)).is_some(),
        );
        needs.insert(which, (to_open, to_open_and_take_one));
    }

    let (large_open, large_one) = needs["large"];
    let (small_open, small_one) = needs["small"];
    // What one fetch adds, which is the thing that must not scale with what is
    // still buffered. A doubling search lands on powers of two, so a fetch
    // that costs nothing measurable shows as no increase at all; the floor
    // keeps that case from dividing by zero.
    let large_step = large_one.saturating_sub(large_open).max(1);
    let small_step = small_one.saturating_sub(small_open).max(1);
    println!(
        "MEASURED one more page: the {LARGE_ROWS}-row answer needs {large_open} to open and \
         {large_one} to take one more (step {large_step}); the {SMALL_ROWS}-row answer needs \
         {small_open} then {small_one} (step {small_step})"
    );

    assert!(
        large_step <= small_step.saturating_mul(HEADROOM_MULTIPLE),
        "a page is a page: taking one costs {large_step} more with {LARGE_ROWS} rows still \
         buffered and {small_step} more with {SMALL_ROWS}, so what a fetch charges is decided by \
         what the cursor is still holding rather than by what it hands over -- which is what \
         turns a paged read of a large answer into quadratic work"
    );
}

#[test]
fn taking_a_buffered_answer_a_page_at_a_time_costs_what_taking_it_at_once_costs() {
    let database = seeded();
    let whole = database
        .execute(ORDERED, &bucket("large"))
        .expect("the executor answers the large answer");
    let expected = whole.rows.len();

    let in_one_call = smallest_work_ceiling("the whole ordered answer in one call", |limits| {
        database
            .read_session(limits)
            .expect("open a bounded read view")
            .execute(ORDERED, &bucket("large"))
            .is_ok()
    });
    let a_page_at_a_time = smallest_work_ceiling("the same answer one page at a time", |limits| {
        open_and_take(&database, "large", limits, None) == Some(expected)
    });

    let pages = (expected as u64).div_ceil(PAGE_ROWS);
    println!(
        "MEASURED {expected} rows: one call needs {in_one_call}, {pages} pages need \
         {a_page_at_a_time}"
    );
    assert!(
        a_page_at_a_time <= in_one_call.saturating_mul(HEADROOM_MULTIPLE),
        "taking an answer in pieces examines the rows the answer has, once, so it needs about the \
         ceiling one call needs: one call needs {in_one_call} and {pages} pages need \
         {a_page_at_a_time}. A figure that grows with the page count is a cursor re-walking what \
         it still holds on every fetch"
    );
}
