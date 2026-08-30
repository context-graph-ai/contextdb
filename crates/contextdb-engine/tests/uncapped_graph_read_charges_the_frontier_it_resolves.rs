//! A traversal pays for the starts it actually resolves.
//!
//! A read that names no ceilings of its own is governed by the store's memory
//! limit, and a traversal's frontier is the biggest thing it asks the store
//! for. Pricing that frontier from one start regardless of the read is wrong
//! in both directions, and the two directions hurt different people: a
//! traversal whose filter matches NOTHING is refused for a frontier it never
//! builds, so an operator sees a memory refusal for a query that reads
//! nothing; and a traversal that fans out across many starts is charged for
//! one, so a limit meant to stop a runaway traversal does not stop it.
//!
//! So the charge follows the resolution -- one start's worth as each start is
//! resolved, nothing at all when none is. Both directions are pinned here
//! against a budget calibrated from the database itself: the smallest headroom
//! that admits a single start's frontier, and the half of it that does not.

#![cfg(feature = "test-seams")]

use contextdb_core::{Error, Value};
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// How many nodes the many-start traversal begins from. Far more than the one
/// start a single-start budget admits, so a frontier priced per resolved start
/// cannot fit and a frontier priced once can.
const MANY_STARTS: usize = 32;

/// Traversal depth, at the deepest the engine admits. The frontier estimate
/// scales with it, so the deepest traversal makes one start's frontier the
/// dominant thing in a tight budget rather than something lost among the row
/// reads.
const DEPTH: u32 = 10;

/// Headroom the calibration starts from, halved until a single start's
/// frontier no longer fits.
const GENEROUS_HEADROOM: usize = 1 << 22;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn traversal(start_filter: &str) -> String {
    format!(
        "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{{1,{DEPTH}}}(b) \
         WHERE a.name = '{start_filter}' COLUMNS (b.id AS t))"
    )
}

fn create_and_seed(database: &Database) {
    database
        .execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
            &empty(),
        )
        .expect("create the node table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");

    // One node the single-start traversal begins from, and many the
    // many-start traversal begins from. Each start has one edge out, so the
    // traversal answers rows in both cases and the only thing that differs is
    // how many frontiers it has to build.
    seed_start(database, "lone");
    for _ in 0..MANY_STARTS {
        seed_start(database, "hub");
    }
}

fn seed_start(database: &Database, name: &str) {
    let start = Uuid::new_v4();
    let target = Uuid::new_v4();
    insert_node(database, start, name);
    insert_node(database, target, "leaf");
    database
        .execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'LINKS')",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::new_v4())),
                ("source".to_owned(), Value::Uuid(start)),
                ("target".to_owned(), Value::Uuid(target)),
            ]),
        )
        .expect("insert the edge out of a start");
}

fn insert_node(database: &Database, id: Uuid, name: &str) {
    database
        .execute(
            "INSERT INTO nodes (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(id)),
                ("name".to_owned(), Value::Text(name.to_owned())),
            ]),
        )
        .expect("insert a node");
}

/// Run `sql` with the store limited to what it is already holding plus
/// `headroom`, then lift the limit again.
fn under_headroom(
    database: &Database,
    accountant: &MemoryAccountant,
    headroom: usize,
    sql: &str,
) -> contextdb_core::Result<QueryResult> {
    accountant
        .set_budget(Some(accountant.usage().used + headroom))
        .expect("tighten the store's memory limit");
    let answered = database.execute(sql, &empty());
    accountant
        .set_budget(None)
        .expect("lift the store's memory limit");
    answered
}

fn frontier_refusal(error: &Error) -> bool {
    matches!(
        error,
        Error::MemoryBudgetExceeded { subsystem, operation, .. }
            if subsystem == "bfs_frontier" && operation == "graph_bfs"
    )
}

fn describe(error: &Error) -> String {
    format!("{error:?}")
}

/// The smallest headroom, by halving, that still admits ONE start's frontier.
/// Half of what this returns is refused, and refused for the frontier -- so
/// the two budgets either side of it say exactly what one start's frontier
/// costs, without the test needing to know the estimate.
fn headroom_that_admits_one_start(database: &Database, accountant: &MemoryAccountant) -> usize {
    let one_start = traversal("lone");
    let mut admits = GENEROUS_HEADROOM;
    under_headroom(database, accountant, admits, &one_start)
        .expect("a generous budget must admit a single start's frontier");
    loop {
        let smaller = admits / 2;
        assert!(
            smaller > 0,
            "a single start's frontier must cost the store something"
        );
        match under_headroom(database, accountant, smaller, &one_start) {
            Ok(_) => admits = smaller,
            Err(error) if frontier_refusal(&error) => return admits,
            Err(error) => panic!(
                "halving the budget must end in the frontier refusal, not {}",
                describe(&error)
            ),
        }
    }
}

#[test]
fn a_traversal_that_resolves_no_start_is_not_refused_for_a_frontier_it_never_builds() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_accountant(Arc::clone(&accountant));
    create_and_seed(&database);

    let admits_one_start = headroom_that_admits_one_start(&database, &accountant);
    // Below what one start's frontier costs, proven by the calibration: a
    // traversal priced before it resolves anything is refused here.
    let below_one_start = admits_one_start / 2;

    let answered = under_headroom(
        &database,
        &accountant,
        below_one_start,
        &traversal("no-such-start"),
    );

    let rows = answered.unwrap_or_else(|error| {
        panic!(
            "a traversal whose filter matches no start builds no frontier and \
             must answer, not be refused: {}",
            describe(&error)
        )
    });
    assert!(
        rows.rows.is_empty(),
        "a traversal whose filter matches no start answers no rows"
    );
}

#[test]
fn a_traversal_that_resolves_many_starts_is_refused_when_only_one_frontier_fits() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_accountant(Arc::clone(&accountant));
    create_and_seed(&database);

    let admits_one_start = headroom_that_admits_one_start(&database, &accountant);

    let answered = under_headroom(&database, &accountant, admits_one_start, &traversal("hub"));

    let error = answered.err().unwrap_or_else(|| {
        panic!(
            "a budget that admits one start's frontier must refuse a traversal \
             resolving {MANY_STARTS} of them"
        )
    });
    assert!(
        frontier_refusal(&error),
        "the refusal must name the frontier the traversal was building, not \
         some other allocation: {}",
        describe(&error)
    );
}
