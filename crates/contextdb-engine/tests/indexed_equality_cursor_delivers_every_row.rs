//! Reading an indexed lookup one page at a time gives the same rows as
//! reading it all at once.
//!
//! This is the plainest thing a cursor does: an equality on an indexed
//! column, a handful of matching rows, no transaction open, nothing rotating
//! underneath. If paging cannot be trusted here it cannot be trusted
//! anywhere, and the way it fails matters as much as that it fails -- a
//! cursor that returns fewer rows and NO error has not told the caller
//! anything is wrong. The rows it withheld are indistinguishable from rows
//! the table does not have, so the caller acts on a table that looks smaller
//! than it is.
//!
//! Page size is the axis under test, because it is the one thing a caller
//! picks freely and has no reason to think changes the ANSWER. A caller who
//! asks for ten rows at a time and one who asks for one at a time are asking
//! the same question.
//!
//! Every shape is asked both ways -- once through `execute`, once through a
//! cursor -- and the executor's answer is the oracle; nothing is restated by
//! hand. Where the statement asked for an order, the sequence is part of the
//! answer; where it did not, the rows are compared as a set, because a
//! statement with no ORDER BY promises which rows match and nothing about
//! their sequence.
//!
//! A cursor also holds store memory while it is open and gives it back when
//! it closes, so each run checks the accountant either side of the cursor: a
//! cursor that strands bytes shrinks the operator's budget with every paged
//! read, and one that gives back more than it took lifts the effective
//! ceiling above the configured one.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::Database;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// The page sizes a caller might pick. One row at a time is the sharpest,
/// because it is the shape where every row costs its own fetch; a page larger
/// than the whole answer is the other end, where paging should be a no-op.
const PAGE_SIZES: [u64; 5] = [1, 2, 3, 10, 100];

/// Ceilings far above anything these lookups need, so a ceiling can never be
/// what stops a cursor part-way. Only the page size varies.
fn roomy(page_rows: u64) -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: page_rows,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn row_id(ordinal: i64) -> Uuid {
    Uuid::from_u128(0x001E_0000_0000_0000_0000_0000_0000_0000 + ordinal as u128)
}

/// How many rows each bucket holds. The small bucket is the size the failure
/// was first seen at; the large one is big enough that a page boundary falls
/// in several different places as the page size changes.
const SMALL_BUCKET: i64 = 3;
const LARGE_BUCKET: i64 = 50;
/// Rows in other buckets, so an index that is doing no narrowing is not
/// mistaken for one that is.
const OTHER_ROWS: i64 = 200;

fn charged(database: &Database) -> u64 {
    database.accountant().usage().used as u64
}

fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, bucket TEXT, ordinal INTEGER)",
            &empty(),
        )
        .expect("create the fixture table");
    database
        .execute("CREATE INDEX items_bucket ON items (bucket)", &empty())
        .expect("index the bucket column");

    let mut next = 0_i64;
    let insert = |bucket: &str, ordinal: i64| {
        database
            .execute(
                "INSERT INTO items (id, bucket, ordinal) VALUES ($id, $bucket, $ordinal)",
                &params(vec![
                    ("id", Value::Uuid(row_id(ordinal))),
                    ("bucket", Value::Text(bucket.to_owned())),
                    ("ordinal", Value::Int64(ordinal)),
                ]),
            )
            .expect("insert a row");
    };
    for _ in 0..SMALL_BUCKET {
        insert("small", next);
        next += 1;
    }
    for _ in 0..LARGE_BUCKET {
        insert("large", next);
        next += 1;
    }
    for index in 0..OTHER_ROWS {
        insert(&format!("other-{}", index % 7), next);
        next += 1;
    }
    database
}

/// The identifier of the row the primary-key shape looks up: one of the small
/// bucket's rows, so the fixture needs no extra row for it.
fn pinned_id() -> Uuid {
    row_id(0)
}

struct Paged {
    rows: Vec<String>,
    pages: usize,
    stopped_by: Option<String>,
    charged_before: u64,
    charged_after: u64,
}

fn rendered(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter().map(|row| format!("{row:?}")).collect()
}

fn page_through(
    database: &Database,
    sql: &str,
    sql_params: &HashMap<String, Value>,
    page_rows: u64,
) -> Paged {
    let charged_before = charged(database);
    let session = database
        .read_session(roomy(page_rows))
        .expect("open a bounded read view");
    let mut cursor = session
        .open_cursor(sql, sql_params)
        .expect("open a cursor over the lookup");

    // The cursor's FIRST page is produced atomically when the cursor opens --
    // it is part of what opening a cursor gives you, not something a later
    // fetch re-delivers. A reader that starts counting at the first `fetch`
    // discards it and mistakes its own omission for lost rows.
    let mut rows = rendered(&cursor.first_page().rows);
    let mut pages = 1_usize;
    let mut has_more = cursor.first_page().has_more;
    let mut stopped_by = None;
    while has_more {
        match cursor.fetch(NonZeroUsize::new(page_rows as usize)) {
            Ok(page) => {
                rows.extend(rendered(&page.rows));
                pages += 1;
                has_more = page.has_more;
                assert!(
                    pages < 10_000,
                    "a cursor that keeps promising more without finishing is not paging"
                );
            }
            Err(error) => {
                stopped_by = Some(error.to_string());
                break;
            }
        }
    }
    cursor.close().expect("close the cursor");
    drop(session);

    Paged {
        rows,
        pages,
        stopped_by,
        charged_before,
        charged_after: charged(database),
    }
}

struct Shape {
    what: &'static str,
    sql: &'static str,
    params: HashMap<String, Value>,
    ordered: bool,
    expected_rows: usize,
}

fn shapes() -> Vec<Shape> {
    let small = params(vec![("bucket", Value::Text("small".to_owned()))]);
    let large = params(vec![("bucket", Value::Text("large".to_owned()))]);
    vec![
        Shape {
            what: "three rows an index points at",
            sql: "SELECT ordinal FROM items WHERE bucket = $bucket",
            params: small.clone(),
            ordered: false,
            expected_rows: SMALL_BUCKET as usize,
        },
        Shape {
            what: "the same three rows in the order they were asked for",
            sql: "SELECT ordinal FROM items WHERE bucket = $bucket ORDER BY ordinal",
            params: small,
            ordered: true,
            expected_rows: SMALL_BUCKET as usize,
        },
        Shape {
            what: "fifty rows an index points at",
            sql: "SELECT ordinal FROM items WHERE bucket = $bucket",
            params: large.clone(),
            ordered: false,
            expected_rows: LARGE_BUCKET as usize,
        },
        Shape {
            what: "the same fifty rows in the order they were asked for",
            sql: "SELECT ordinal FROM items WHERE bucket = $bucket ORDER BY ordinal",
            params: large,
            ordered: true,
            expected_rows: LARGE_BUCKET as usize,
        },
        Shape {
            what: "the one row a primary key points at",
            sql: "SELECT ordinal FROM items WHERE id = $id",
            params: params(vec![("id", Value::Uuid(pinned_id()))]),
            ordered: false,
            expected_rows: 1,
        },
    ]
}

fn compared(rows: &[String], ordered: bool) -> Vec<String> {
    let mut compared = rows.to_vec();
    if !ordered {
        compared.sort();
    }
    compared
}

#[test]
fn an_indexed_lookup_read_one_page_at_a_time_gives_what_one_call_gives() {
    let database = seeded();
    let mut losses = Vec::new();

    for shape in shapes() {
        let whole = database
            .execute(shape.sql, &shape.params)
            .unwrap_or_else(|error| panic!("the executor answers {}: {error}", shape.what));
        assert_eq!(
            whole.rows.len(),
            shape.expected_rows,
            "{}: the fixture holds the rows this shape is about",
            shape.what
        );
        let expected = compared(&rendered(&whole.rows), shape.ordered);

        for page_rows in PAGE_SIZES {
            let paged = page_through(&database, shape.sql, &shape.params, page_rows);
            let delivered = compared(&paged.rows, shape.ordered);
            println!(
                "OBSERVED {} at {page_rows} rows a page: one call {} rows, {} pages delivered {} \
                 rows, charged {} then {}, stopped by {:?}",
                shape.what,
                expected.len(),
                paged.pages,
                delivered.len(),
                paged.charged_before,
                paged.charged_after,
                paged.stopped_by
            );

            if let Some(stopped) = &paged.stopped_by {
                losses.push(format!(
                    "{} at {page_rows} rows a page: the cursor stopped at page {} after {} of {} \
                     rows: {stopped}",
                    shape.what,
                    paged.pages,
                    delivered.len(),
                    expected.len()
                ));
                continue;
            }
            if delivered.len() != expected.len() {
                losses.push(format!(
                    "{} at {page_rows} rows a page: one call gives {} rows and {} pages gave {}, \
                     with no error -- the rows it withheld are indistinguishable from rows the \
                     table does not have",
                    shape.what,
                    expected.len(),
                    paged.pages,
                    delivered.len()
                ));
                continue;
            }
            if delivered != expected {
                losses.push(format!(
                    "{} at {page_rows} rows a page: the pages hold the right number of rows but \
                     not the same ones{}: one call gives {expected:?} and the pages gave \
                     {delivered:?}",
                    shape.what,
                    if shape.ordered { ", in order" } else { "" }
                ));
                continue;
            }
            if paged.charged_after != paged.charged_before {
                losses.push(format!(
                    "{} at {page_rows} rows a page: the cursor was charged {} before it opened \
                     and {} after it closed",
                    shape.what, paged.charged_before, paged.charged_after
                ));
            }
        }
    }

    assert!(
        losses.is_empty(),
        "how many rows a caller asks for at a time cannot change which rows exist, and a cursor \
         that hands back fewer without saying so leaves the caller acting on a table that looks \
         smaller than it is:\n{}",
        losses.join("\n")
    );
}
