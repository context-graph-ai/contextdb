//! An edge's identity is the unique key its table declares, and where that key
//! is exactly source, target and type, or no key is declared, the engine
//! de-duplicates that triple automatically
//! (`an_edge_keeps_the_identity_its_table_declares`). That holds whichever door
//! the edge comes through: the graph door (`insert_edge`) and the row door
//! (`insert_row`) write the same edge, so the same triple written through both,
//! in either order, is one edge -- one graph edge, one binding for a graph
//! pattern over it, and a graph pattern joined to its target returns that
//! target once.
//!
//! The SQL door's own duplicate is pinned by `sql_10_edge_dedup_no_relational_leak`
//! and `with_no_declared_key_the_triple_is_still_deduplicated`; this file pins the
//! two doors meeting.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const EDGES: &str =
    "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)";

struct Fixture {
    db: Database,
    source: Uuid,
    target: Uuid,
}

fn fixture() -> Fixture {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(EDGES, &HashMap::new()).unwrap();
    let source = Uuid::from_u128(0xe001);
    let target = Uuid::from_u128(0xe002);
    for (id, name) in [(source, "root"), (target, "child")] {
        db.execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(id)),
                ("name".to_owned(), Value::Text(name.to_owned())),
            ]),
        )
        .unwrap();
    }
    Fixture { db, source, target }
}

fn edge_row(source: Uuid, target: Uuid) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Uuid(Uuid::from_u128(0xe0e0))),
        ("source_id".to_owned(), Value::Uuid(source)),
        ("target_id".to_owned(), Value::Uuid(target)),
        ("edge_type".to_owned(), Value::Text("DEPENDS_ON".into())),
    ])
}

/// One edge: one graph edge from the source, and one binding for a graph
/// pattern joined to its target.
fn assert_one_edge(f: &Fixture, order: &str) {
    assert_eq!(
        f.db.edge_count(f.source, "DEPENDS_ON", f.db.snapshot())
            .unwrap(),
        1,
        "{order}: the same triple through both doors is one graph edge"
    );
    let joined =
        f.db.execute(
            "WITH reachable AS (
                SELECT b_id FROM GRAPH_TABLE(
                    edges MATCH (a)-[:DEPENDS_ON]->(b)
                    WHERE a.id = $root_id
                    COLUMNS (b.id AS b_id)
                )
            )
            SELECT e.name FROM entities e INNER JOIN reachable r ON e.id = r.b_id",
            &HashMap::from([("root_id".to_owned(), Value::Uuid(f.source))]),
        )
        .unwrap();
    assert_eq!(
        joined.rows(),
        vec![vec![Value::Text("child".into())]],
        "{order}: a graph pattern over one edge yields one binding"
    );
}

fn relational_rows(f: &Fixture) -> Value {
    f.db.execute(
        "SELECT COUNT(*) FROM edges WHERE source_id = $source AND target_id = $target AND edge_type = 'DEPENDS_ON'",
        &HashMap::from([
            ("source".to_owned(), Value::Uuid(f.source)),
            ("target".to_owned(), Value::Uuid(f.target)),
        ]),
    )
    .unwrap()
    .rows()[0][0]
        .clone()
}

#[test]
fn an_edge_written_through_the_graph_door_and_then_the_row_door_is_one_edge() {
    let f = fixture();
    let tx = f.db.begin().unwrap();
    assert!(
        f.db.insert_edge(tx, f.source, f.target, "DEPENDS_ON".into(), HashMap::new())
            .unwrap(),
        "the first write of the edge is inserted"
    );
    f.db.insert_row(tx, "edges", edge_row(f.source, f.target))
        .unwrap();
    f.db.commit(tx).unwrap();
    assert_one_edge(&f, "graph door, then row door");
    assert_eq!(
        relational_rows(&f),
        Value::Int64(1),
        "the row door's row is the edge's one relational row"
    );
}

#[test]
fn an_edge_written_through_the_graph_door_keeps_its_properties_when_the_row_door_writes_it_again() {
    let f = fixture();
    let properties = HashMap::from([("weight".to_owned(), Value::Float64(0.5))]);
    let tx = f.db.begin().unwrap();
    assert!(
        f.db.insert_edge(
            tx,
            f.source,
            f.target,
            "DEPENDS_ON".into(),
            properties.clone()
        )
        .unwrap(),
        "the first write of the edge, with its properties, is inserted"
    );
    f.db.insert_row(tx, "edges", edge_row(f.source, f.target))
        .unwrap();
    f.db.commit(tx).unwrap();
    assert_one_edge(&f, "graph door with properties, then row door");
    assert_eq!(
        relational_rows(&f),
        Value::Int64(1),
        "the row door's row is the edge's one relational row"
    );
    assert_eq!(
        f.db.get_edge_properties(f.source, f.target, "DEPENDS_ON", f.db.snapshot())
            .unwrap(),
        Some(properties),
        "the one edge keeps the properties the graph door gave it"
    );
}

#[test]
fn an_edge_written_through_the_row_door_and_then_the_graph_door_is_one_edge() {
    let f = fixture();
    let tx = f.db.begin().unwrap();
    f.db.insert_row(tx, "edges", edge_row(f.source, f.target))
        .unwrap();
    assert!(
        !f.db
            .insert_edge(tx, f.source, f.target, "DEPENDS_ON".into(), HashMap::new())
            .unwrap(),
        "the same triple through the graph door after the row door inserts nothing"
    );
    f.db.commit(tx).unwrap();
    assert_one_edge(&f, "row door, then graph door");
    assert_eq!(
        relational_rows(&f),
        Value::Int64(1),
        "the row door's row is the edge's one relational row"
    );
}
