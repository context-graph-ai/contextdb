//! Taking a graph walk one page at a time gives the same answer as taking it
//! all at once.
//!
//! Paging exists so a caller can handle an answer larger than it wants to hold
//! at once. That is only useful if the pieces add up to the whole: a cursor
//! that stops part-way has not given a smaller answer, it has given a WRONG
//! one, because the rows it never delivered are indistinguishable from rows
//! the walk does not have. A caller who pages a fan-out and gets twenty-two
//! pages of a twenty-four page walk has no way to know, and the graph they
//! act on is missing edges that are really there.
//!
//! So each shape here is asked BOTH ways -- once through `execute`, once
//! through a cursor with small pages -- and the two answers are compared. The
//! executor's answer is the oracle; nothing is restated by hand. Where the
//! statement asked for an order, the sequence is part of the answer; where it
//! did not, the rows are compared as a set, because a walk with no ORDER BY
//! promises which nodes are reachable and nothing about their sequence.
//!
//! A cursor also holds store memory while it is open, and gives it back when
//! it closes. So each shape checks the accountant before the cursor opens and
//! after it closes: a cursor that leaves bytes charged shrinks the operator's
//! budget with every paged read, and one that gives back more than it took
//! lifts the effective ceiling above the configured one.

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

/// Small enough that a walk of this size really is taken in many pieces.
const PAGE_ROWS: u64 = 100;

/// Ceilings far above anything these walks need, so a ceiling can never be
/// what stops a cursor part-way.
fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: PAGE_ROWS,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn node(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x009A_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

const HUB: u128 = 1;
/// A fan-out large enough to need many pages, and larger than the point at
/// which a paged walk was observed to stop.
const LEAVES: u128 = 2_400;
/// How many of those leaves carry a chain onwards, so a multi-hop walk has
/// depth as well as breadth.
const CHAINED_LEAVES: u128 = 200;

fn charged(database: &Database) -> u64 {
    database.accountant().usage().used as u64
}

/// One hub with a wide fan-out, and a chain of two further hops hanging off
/// some of the leaves.
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

    let tx = database.begin().expect("begin the graph batch");
    for ordinal in 0..LEAVES {
        database
            .insert_edge(
                tx,
                node(HUB),
                node(10_000 + ordinal),
                "LINKS".to_owned(),
                HashMap::new(),
            )
            .expect("connect the hub to a leaf");
    }
    for ordinal in 0..CHAINED_LEAVES {
        database
            .insert_edge(
                tx,
                node(10_000 + ordinal),
                node(50_000 + ordinal),
                "LINKS".to_owned(),
                HashMap::new(),
            )
            .expect("carry the chain one hop further");
        database
            .insert_edge(
                tx,
                node(50_000 + ordinal),
                node(90_000 + ordinal),
                "LINKS".to_owned(),
                HashMap::new(),
            )
            .expect("carry the chain one more hop");
    }
    database.commit(tx).expect("commit the graph batch");
    database
}

/// What a cursor delivered, and what stopped it if anything did.
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

fn page_through(database: &Database, sql: &str, sql_params: &HashMap<String, Value>) -> Paged {
    let charged_before = charged(database);
    let session = database
        .read_session(roomy())
        .expect("open a bounded read view");
    let mut cursor = session
        .open_cursor(sql, sql_params)
        .expect("open a cursor over the walk");

    // The cursor's FIRST page is produced atomically when the cursor opens --
    // it is part of what opening a cursor gives you, not something a later
    // fetch re-delivers. A reader that starts counting at the first `fetch`
    // discards it and mistakes its own omission for lost rows.
    let mut rows = rendered(&cursor.first_page().rows);
    let mut pages = 1_usize;
    let mut has_more = cursor.first_page().has_more;
    let mut stopped_by = None;
    while has_more {
        match cursor.fetch(NonZeroUsize::new(PAGE_ROWS as usize)) {
            Ok(page) => {
                rows.extend(rendered(&page.rows));
                pages += 1;
                has_more = page.has_more;
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

/// Ask one statement both ways and hold the paged answer to the whole one.
fn both_ways(what: &str, sql: &str, sql_params: &HashMap<String, Value>, ordered: bool) {
    let database = seeded();
    let whole = database
        .execute(sql, sql_params)
        .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));
    let expected = {
        let mut expected = rendered(&whole.rows);
        if !ordered {
            expected.sort();
        }
        expected
    };
    assert!(
        expected.len() as u64 > PAGE_ROWS * 20,
        "{what} is only a paging test if the walk needs many pages: {} rows",
        expected.len()
    );

    let paged = page_through(&database, sql, sql_params);
    let delivered = {
        let mut delivered = paged.rows.clone();
        if !ordered {
            delivered.sort();
        }
        delivered
    };
    println!(
        "OBSERVED {what}: one call {} rows, {} pages delivered {} rows, charged {} then {}, \
         stopped by {:?}",
        expected.len(),
        paged.pages,
        delivered.len(),
        paged.charged_before,
        paged.charged_after,
        paged.stopped_by
    );

    assert_eq!(
        paged.stopped_by,
        None,
        "{what}: the ceilings are far above what this walk needs, so nothing should stop the \
         cursor part-way -- it stopped at page {} after {} of {} rows",
        paged.pages,
        delivered.len(),
        expected.len()
    );
    assert_eq!(
        delivered.len(),
        expected.len(),
        "{what}: a cursor hands over every row the walk has, in pieces -- it delivered {} of {} \
         across {} pages, and the rows it never delivered look exactly like rows the walk does \
         not have",
        delivered.len(),
        expected.len(),
        paged.pages
    );
    assert_eq!(
        delivered,
        expected,
        "{what}: the pieces add up to the same answer the whole call gives{}",
        if ordered {
            ", in the order it was asked for"
        } else {
            ""
        }
    );
    assert_eq!(
        paged.charged_after, paged.charged_before,
        "{what}: a cursor gives back exactly what it held when it closes -- charged {} before it \
         opened and {} after it closed",
        paged.charged_before, paged.charged_after
    );
}

const ONE_HOP: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                       WHERE a.id = $hub COLUMNS (b.id AS t))";
const ONE_HOP_ORDERED: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                               WHERE a.id = $hub COLUMNS (b.id AS t)) ORDER BY t";
const MULTI_HOP: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) \
                         WHERE a.id = $hub COLUMNS (b.id AS t))";

fn hub_params() -> HashMap<String, Value> {
    params(vec![("hub", Value::Uuid(node(HUB)))])
}

#[test]
fn paging_a_wide_fan_out_delivers_every_row_one_call_delivers() {
    both_ways("a hub's whole fan-out", ONE_HOP, &hub_params(), false);
}

#[test]
fn paging_an_ordered_fan_out_delivers_the_same_rows_in_the_same_order() {
    both_ways(
        "a hub's fan-out in the order it was asked for",
        ONE_HOP_ORDERED,
        &hub_params(),
        true,
    );
}

#[test]
fn paging_a_walk_several_hops_deep_delivers_every_row_one_call_delivers() {
    both_ways(
        "a walk several hops out of the hub",
        MULTI_HOP,
        &hub_params(),
        false,
    );
}
