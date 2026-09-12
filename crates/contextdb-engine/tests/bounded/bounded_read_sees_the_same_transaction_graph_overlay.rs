//! A bounded read inside an open transaction walks the graph that
//! transaction can see.
//!
//! A writing session that has staged an edge, and removed another, is
//! entitled to read its own work back before it commits -- that is what makes
//! a multi-step write reviewable while it is still abandonable. Reading it
//! back through a bounded view is the same question about the same
//! transaction, so it has to give the same walk: the staged edge is walked,
//! the removed one is not, and once the transaction is abandoned neither door
//! remembers any of it.
//!
//! The ORACLE is the executor's own answer inside the same transaction, never
//! a set restated by hand. A walk that answers with a superset would leak
//! another session's uncommitted work; one that answers with a subset would
//! tell a writer its own staged work is not there.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
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

/// Ceilings far above anything this fixture needs, so a ceiling can never be
/// what makes the two doors disagree.
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

fn node(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00E0_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

const START: u128 = 1;
const MIDDLE: u128 = 2;
const COMMITTED_LEAF: u128 = 3;
const STAGED_LEAF: u128 = 4;

/// The walk under test: everything reachable from the start within two hops.
const WALK: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,2}(b) \
                    WHERE a.id = $start COLUMNS (b.id AS t))";

/// One hop out of the middle node, which is where the staged edge and the
/// removed edge both live.
const ONE_HOP: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                       WHERE a.id = $middle COLUMNS (b.id AS t))";

fn walk_params() -> HashMap<String, Value> {
    params(vec![("start", Value::Uuid(node(START)))])
}

fn one_hop_params() -> HashMap<String, Value> {
    params(vec![("middle", Value::Uuid(node(MIDDLE)))])
}

/// The rows a walk answered, as a SET: a walk promises which nodes are
/// reachable and nothing about the sequence they arrive in.
fn reached(result: &QueryResult) -> Vec<String> {
    let mut reached: Vec<String> = result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Uuid(id)) => format!("{id}"),
            other => panic!("a walk answers with node ids, got {other:?}"),
        })
        .collect();
    reached.sort();
    reached
}

fn insert_node(database: &Database, id: Uuid) {
    database
        .execute(
            "INSERT INTO nodes (id) VALUES ($id)",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect("insert a node");
}

fn insert_edge(database: &Database, source: Uuid, target: Uuid) {
    database
        .execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'LINKS')",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("source", Value::Uuid(source)),
                ("target", Value::Uuid(target)),
            ]),
        )
        .expect("insert an edge");
}

/// A committed two-hop chain, plus a node that nothing points at yet.
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
    for ordinal in [START, MIDDLE, COMMITTED_LEAF] {
        insert_node(&database, node(ordinal));
    }
    insert_edge(&database, node(START), node(MIDDLE));
    insert_edge(&database, node(MIDDLE), node(COMMITTED_LEAF));
    database
}

/// Stage, inside the session's open transaction, the node and edge that make
/// the walk answer differently, and remove one committed edge.
///
/// The bounded view watches the SQL session's transaction, while the graph
/// doors take a transaction id, so the fixture learns the id the session is
/// about to take and stages the graph work on it. Nothing here trusts that
/// pairing: every pin below first asserts what the session's OWN read sees,
/// and work staged on any other transaction would not be in it.
fn stage_the_overlay(database: &Database) {
    let tx = database.next_tx();
    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    insert_node(database, node(STAGED_LEAF));
    database
        .insert_edge(
            tx,
            node(MIDDLE),
            node(STAGED_LEAF),
            "LINKS".to_owned(),
            HashMap::new(),
        )
        .expect("stage an edge inside the transaction");
    database
        .delete_edge(tx, node(MIDDLE), node(COMMITTED_LEAF), "LINKS")
        .expect("remove a committed edge inside the transaction");
}

fn eager(database: &Database, sql: &str, sql_params: &HashMap<String, Value>) -> QueryResult {
    database
        .execute(sql, sql_params)
        .expect("the executor answers")
}

fn bounded(database: &Database, sql: &str, sql_params: &HashMap<String, Value>) -> QueryResult {
    database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, sql_params)
        .expect("a bounded read answers")
}

#[test]
fn a_bounded_walk_inside_an_open_transaction_sees_what_that_transaction_staged() {
    let database = seeded();
    let committed = reached(&eager(&database, WALK, &walk_params()));
    assert_eq!(
        committed,
        {
            let mut expected = vec![
                format!("{}", node(MIDDLE)),
                format!("{}", node(COMMITTED_LEAF)),
            ];
            expected.sort();
            expected
        },
        "the fixture starts from a committed two-hop chain"
    );

    stage_the_overlay(&database);

    let eager_in_tx = eager(&database, WALK, &walk_params());
    let bounded_in_tx = bounded(&database, WALK, &walk_params());
    println!(
        "OBSERVED walk in the open transaction: executor {:?} | bounded {:?}",
        reached(&eager_in_tx),
        reached(&bounded_in_tx)
    );

    assert_ne!(
        reached(&eager_in_tx),
        committed,
        "the fixture is adversarial only if the transaction changes the answer"
    );
    assert_eq!(
        reached(&bounded_in_tx),
        reached(&eager_in_tx),
        "a bounded read inside an open transaction walks the graph that transaction sees: the \
         executor reaches {:?} and the bounded read reaches {:?}",
        reached(&eager_in_tx),
        reached(&bounded_in_tx)
    );
    assert_eq!(
        bounded_in_tx.trace.physical_plan, eager_in_tx.trace.physical_plan,
        "and describes the walk in the same words"
    );

    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

#[test]
fn a_bounded_probe_inside_an_open_transaction_hides_the_edge_that_transaction_removed() {
    let database = seeded();
    stage_the_overlay(&database);

    let eager_in_tx = eager(&database, ONE_HOP, &one_hop_params());
    let bounded_in_tx = bounded(&database, ONE_HOP, &one_hop_params());
    println!(
        "OBSERVED one hop in the open transaction: executor {:?} | bounded {:?}",
        reached(&eager_in_tx),
        reached(&bounded_in_tx)
    );

    assert_eq!(
        reached(&eager_in_tx),
        vec![format!("{}", node(STAGED_LEAF))],
        "the fixture stages one edge out of the middle node and removes the other"
    );
    assert_eq!(
        reached(&bounded_in_tx),
        reached(&eager_in_tx),
        "the edge this transaction removed is gone from its own bounded read, and the one it \
         staged is there: the executor reaches {:?} and the bounded read reaches {:?}",
        reached(&eager_in_tx),
        reached(&bounded_in_tx)
    );

    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

#[test]
fn an_abandoned_transaction_leaves_nothing_behind_for_a_bounded_walk() {
    let database = seeded();
    let committed = reached(&eager(&database, WALK, &walk_params()));

    stage_the_overlay(&database);
    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");

    let eager_after = eager(&database, WALK, &walk_params());
    let bounded_after = bounded(&database, WALK, &walk_params());
    println!(
        "OBSERVED walk after the abandoned transaction: executor {:?} | bounded {:?}",
        reached(&eager_after),
        reached(&bounded_after)
    );

    assert_eq!(
        reached(&eager_after),
        committed,
        "abandoning the transaction puts the committed chain back"
    );
    assert_eq!(
        reached(&bounded_after),
        committed,
        "a bounded walk remembers none of an abandoned transaction: it reaches {:?} where the \
         committed graph reaches {committed:?}",
        reached(&bounded_after)
    );
}
