//! One logical edge written through both doors -- the graph door
//! (`insert_edge`, carrying properties) and the row door (`insert_row`), in
//! either order -- reads as that one edge through every read door: the edge
//! count, a SQL graph pattern, a breadth-first walk, a point lookup of its row,
//! the edge's properties, and, on a read session after reopen, both a
//! statement and a cursor page. A sibling that differs only by a further
//! column of a wider declared key is a different edge and a second binding
//! (`an_edge_keeps_the_identity_its_table_declares`).

use contextdb_core::{Direction, Value};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

const SOURCE: Uuid = Uuid::from_u128(0xd001);
const TARGET: Uuid = Uuid::from_u128(0xd002);
const EDGE_ROW: Uuid = Uuid::from_u128(0xd0e0);

const PLAIN_EDGES: &str =
    "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)";
const WIDE_KEY_EDGES: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, subtype_key TEXT NOT NULL DEFAULT '', UNIQUE (source_id, target_id, edge_type, subtype_key))";

const JOINED: &str = "WITH reachable AS (
        SELECT b_id FROM GRAPH_TABLE(
            edges MATCH (a)-[:DEPENDS_ON]->(b)
            WHERE a.id = $root_id
            COLUMNS (b.id AS b_id)
        )
    )
    SELECT e.name FROM entities e INNER JOIN reachable r ON e.id = r.b_id";

fn root() -> HashMap<String, Value> {
    HashMap::from([("root_id".to_owned(), Value::Uuid(SOURCE))])
}

fn weight() -> HashMap<String, Value> {
    HashMap::from([("weight".to_owned(), Value::Float64(0.5))])
}

fn child(count: usize) -> Vec<Vec<Value>> {
    vec![vec![Value::Text("child".into())]; count]
}

fn store(path: &Path, edges: &str) -> Database {
    let db = Database::open(path).expect("open the store");
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(edges, &HashMap::new()).unwrap();
    for (id, name) in [(SOURCE, "root"), (TARGET, "child")] {
        db.execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(id)),
                ("name".to_owned(), Value::Text(name.to_owned())),
            ]),
        )
        .unwrap();
    }
    db
}

fn edge_row(id: Uuid, subtype: Option<&str>) -> HashMap<String, Value> {
    let mut row = HashMap::from([
        ("id".to_owned(), Value::Uuid(id)),
        ("source_id".to_owned(), Value::Uuid(SOURCE)),
        ("target_id".to_owned(), Value::Uuid(TARGET)),
        ("edge_type".to_owned(), Value::Text("DEPENDS_ON".into())),
    ]);
    if let Some(subtype) = subtype {
        row.insert("subtype_key".to_owned(), Value::Text(subtype.to_owned()));
    }
    row
}

fn write_through_both_doors(db: &Database, graph_first: bool) {
    let tx = db.begin().unwrap();
    if graph_first {
        db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), weight())
            .unwrap();
        db.insert_row(tx, "edges", edge_row(EDGE_ROW, None))
            .unwrap();
    } else {
        db.insert_row(tx, "edges", edge_row(EDGE_ROW, None))
            .unwrap();
        db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), weight())
            .unwrap();
    }
    db.commit(tx).unwrap();
}

/// Every door on the writing store, then a statement and a cursor page on a
/// read session after reopen, report the one edge.
fn assert_every_door_reads_one_edge(graph_first: bool) {
    let order = if graph_first {
        "graph door, then row door"
    } else {
        "row door, then graph door"
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("both-doors.db");
    let db = store(&path, PLAIN_EDGES);
    write_through_both_doors(&db, graph_first);

    assert_eq!(
        db.edge_count(SOURCE, "DEPENDS_ON", db.snapshot()).unwrap(),
        1,
        "{order}: the edge count is one"
    );
    assert_eq!(
        db.execute(JOINED, &root()).unwrap().rows(),
        child(1),
        "{order}: a SQL graph pattern yields one binding"
    );
    let walked = db
        .query_bfs(SOURCE, None, Direction::Outgoing, 1, db.snapshot())
        .unwrap();
    assert_eq!(
        walked.nodes.iter().filter(|node| node.id == TARGET).count(),
        1,
        "{order}: a breadth-first walk reaches the target once: {walked:?}"
    );
    let row = db
        .point_lookup("edges", "id", &Value::Uuid(EDGE_ROW), db.snapshot())
        .unwrap()
        .expect("the edge's row is found by its key");
    assert_eq!(
        (row.values.get("source_id"), row.values.get("target_id")),
        (Some(&Value::Uuid(SOURCE)), Some(&Value::Uuid(TARGET))),
        "{order}: a point lookup returns the edge's row"
    );
    assert_eq!(
        db.get_edge_properties(SOURCE, TARGET, "DEPENDS_ON", db.snapshot())
            .unwrap(),
        Some(weight()),
        "{order}: the one edge carries the properties the graph door gave it"
    );
    db.close().unwrap();

    let session = ReadSession::open(&path).expect("open a read session");
    assert_eq!(
        session.execute(JOINED, &root()).unwrap().rows(),
        child(1),
        "{order}: a read session's statement yields one binding"
    );
    let cursor = session
        .open_cursor(JOINED, &root())
        .expect("open a cursor over the graph pattern");
    assert_eq!(
        cursor.first_page().rows(),
        child(1).as_slice(),
        "{order}: a cursor page yields one binding"
    );
    assert!(
        !cursor.first_page().has_more(),
        "{order}: the cursor has nothing after the one binding"
    );
}

#[test]
fn one_edge_written_graph_door_first_reads_as_one_edge_through_every_door() {
    assert_every_door_reads_one_edge(true);
}

#[test]
fn one_edge_written_row_door_first_reads_as_one_edge_through_every_door() {
    assert_every_door_reads_one_edge(false);
}

#[test]
fn a_wider_key_sibling_of_an_edge_written_through_both_doors_stays_a_second_binding() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("wider-key.db");
    let db = store(&path, WIDE_KEY_EDGES);
    write_through_both_doors(&db, true);
    let tx = db.begin().unwrap();
    db.insert_row(tx, "edges", edge_row(Uuid::from_u128(0xd0e1), Some("b")))
        .expect("a sibling differing by a wider-key column is a new row");
    db.commit(tx).unwrap();
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM edges", &HashMap::new())
            .unwrap()
            .rows()[0][0],
        Value::Int64(2),
        "the sibling is a second row"
    );
    assert_eq!(
        db.execute(JOINED, &root()).unwrap().rows(),
        child(2),
        "a sibling that differs only by a wider-key column is a second binding"
    );
}

#[path = "mixed_edge_updates_duplicates_and_gated_views.rs"]
mod mixed_edge_updates_duplicates_and_gated_views;
