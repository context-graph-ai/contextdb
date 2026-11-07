//! A table declared `DAG(...)` silently keeps one copy of a duplicate
//! `(source_id, target_id, edge_type)` edge (`docs/query-language.md`, DAG):
//! a second write of the same triple is not an error and adds nothing, whether
//! it arrives through the row door (`insert_row`) or the graph door
//! (`insert_edge`). The SQL door's own duplicate on an undeclared table is
//! pinned by `sql_10_edge_dedup_no_relational_leak`; the two doors meeting on
//! an undeclared table by `the_same_edge_through_both_doors_is_one_edge`.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const DAG_EDGES: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('DEPENDS_ON')";

const SOURCE: Uuid = Uuid::from_u128(0xda01);
const TARGET: Uuid = Uuid::from_u128(0xda02);

fn dag_store() -> Database {
    let db = Database::open_memory();
    db.execute(DAG_EDGES, &HashMap::new()).unwrap();
    db
}

fn edge_row(id: u128) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Uuid(Uuid::from_u128(id))),
        ("source_id".to_owned(), Value::Uuid(SOURCE)),
        ("target_id".to_owned(), Value::Uuid(TARGET)),
        ("edge_type".to_owned(), Value::Text("DEPENDS_ON".into())),
    ])
}

fn assert_one_copy(db: &Database, what: &str, relational_rows: i64) {
    assert_eq!(
        db.edge_count(SOURCE, "DEPENDS_ON", db.snapshot()).unwrap(),
        1,
        "{what}: a DAG table keeps one graph edge for the triple"
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM edges", &HashMap::new())
            .unwrap()
            .rows()[0][0],
        Value::Int64(relational_rows),
        "{what}: the duplicate adds no relational row"
    );
}

#[test]
fn a_dag_table_keeps_one_copy_of_an_edge_written_twice_through_the_row_door() {
    let db = dag_store();
    let tx = db.begin().unwrap();
    db.insert_row(tx, "edges", edge_row(0xda10)).unwrap();
    db.insert_row(tx, "edges", edge_row(0xda11))
        .expect("a duplicate triple on a DAG table is not an error");
    db.commit(tx).unwrap();
    assert_one_copy(&db, "row door twice", 1);
}

#[test]
fn a_dag_table_keeps_one_copy_of_an_edge_written_twice_through_the_graph_door() {
    let db = dag_store();
    let tx = db.begin().unwrap();
    assert!(
        db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), HashMap::new())
            .unwrap()
    );
    assert!(
        !db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), HashMap::new())
            .expect("a duplicate triple on a DAG edge type is not an error"),
        "the second graph-door write of the triple inserts nothing"
    );
    db.commit(tx).unwrap();
    assert_one_copy(&db, "graph door twice", 0);
}

#[test]
fn a_dag_table_keeps_one_copy_of_an_edge_written_through_the_row_door_and_then_the_graph_door() {
    let db = dag_store();
    let tx = db.begin().unwrap();
    db.insert_row(tx, "edges", edge_row(0xda20)).unwrap();
    assert!(
        !db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), HashMap::new())
            .expect("a duplicate triple on a DAG table is not an error"),
        "the graph-door write after the row door inserts nothing"
    );
    db.commit(tx).unwrap();
    assert_one_copy(&db, "row door, then graph door", 1);
}
