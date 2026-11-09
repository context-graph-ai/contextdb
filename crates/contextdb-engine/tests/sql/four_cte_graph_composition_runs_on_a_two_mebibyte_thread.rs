//! The Context Graph discovery composition runs on an ordinary 2 MiB stack.
//!
//! This is the actual four-CTE `GRAPH_TABLE` statement from Context Graph's
//! discovery acceptance fixture.  It executes at a captured snapshot on a
//! debug thread whose stack is fixed at 2 MiB; the engine does not get a
//! larger worker stack to hide recursive planner/executor frames.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use uuid::Uuid;

const FOUR_CTE_DISCOVERY: &str = "WITH based AS (
    SELECT d_id FROM GRAPH_TABLE (edges MATCH (e)<-[:BASED_ON]-(d) WHERE e.id = $entity COLUMNS (d.id AS d_id))
), about AS (
    SELECT d_id FROM GRAPH_TABLE (edges MATCH (e)<-[:ABOUT]-(d) WHERE e.id = $entity COLUMNS (d.id AS d_id))
), served_by_based AS (
    SELECT i_id FROM GRAPH_TABLE (edges MATCH (d)-[:SERVES]->(i) WHERE d.id IN (SELECT d_id FROM based) COLUMNS (i.id AS i_id))
), served_by_about AS (
    SELECT i_id FROM GRAPH_TABLE (edges MATCH (d)-[:SERVES]->(i) WHERE d.id IN (SELECT d_id FROM about) COLUMNS (i.id AS i_id))
)
SELECT id FROM nodes
WHERE id IN (SELECT i_id FROM served_by_based)
   OR id IN (SELECT i_id FROM served_by_about)";

fn node(value: u128) -> Uuid {
    Uuid::from_u128(0xC7E0_0000_0000_0000_0000_0000_0000_0000 + value)
}

fn insert_node(database: &Database, id: Uuid) {
    database
        .execute(
            "INSERT INTO nodes (id) VALUES ($id)",
            &HashMap::from([("id".to_owned(), Value::Uuid(id))]),
        )
        .unwrap_or_else(|error| panic!("insert node {id}: {error:?}"));
}

fn insert_edge(database: &Database, id: Uuid, source: Uuid, target: Uuid, edge_type: &str) {
    database
        .execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, $edge_type)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(id)),
                ("source".to_owned(), Value::Uuid(source)),
                ("target".to_owned(), Value::Uuid(target)),
                ("edge_type".to_owned(), Value::Text(edge_type.to_owned())),
            ]),
        )
        .unwrap_or_else(|error| panic!("insert {edge_type} edge {source}->{target}: {error:?}"));
}

fn fixture() -> (Arc<Database>, Uuid, BTreeSet<Uuid>) {
    let database = Arc::new(Database::open_memory());
    database
        .execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &HashMap::new())
        .expect("create the Context Graph node table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &HashMap::new(),
        )
        .expect("create the Context Graph edge table");

    let entity = node(1);
    let based = node(2);
    let about = node(3);
    let served_by_based = node(4);
    let served_by_about = node(5);
    for id in [entity, based, about, served_by_based, served_by_about] {
        insert_node(&database, id);
    }
    insert_edge(&database, node(100), based, entity, "BASED_ON");
    insert_edge(&database, node(101), about, entity, "ABOUT");
    insert_edge(&database, node(102), based, served_by_based, "SERVES");
    insert_edge(&database, node(103), about, served_by_about, "SERVES");

    (
        database,
        entity,
        BTreeSet::from([served_by_based, served_by_about]),
    )
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn four_cte_graph_composition_runs_on_a_two_mebibyte_thread() {
    let (database, entity, expected) = fixture();
    let snapshot = database.snapshot();
    let thread = std::thread::Builder::new()
        .name("contextdb-four-cte-2mib".to_owned())
        .stack_size(2 << 20)
        .spawn(move || {
            database.execute_at_snapshot(
                FOUR_CTE_DISCOVERY,
                &HashMap::from([("entity".to_owned(), Value::Uuid(entity))]),
                snapshot,
            )
        })
        .expect("start a 2 MiB debug thread");
    let result = thread
        .join()
        .expect("the engine does not overflow the 2 MiB debug thread")
        .expect("execute the actual four-CTE Context Graph discovery statement");
    let returned = result
        .rows()
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Uuid(id)] => *id,
            other => panic!("the discovery statement returns node UUIDs, got {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(returned, expected);
}
