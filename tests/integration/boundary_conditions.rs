use super::helpers::*;

#[test]
fn bc_01_empty_database() {
    let db = setup_ontology_db();
    assert!(db.scan("entities", db.snapshot()).unwrap().is_empty());
}

#[test]
fn bc_02_depth_bound() {
    let db = setup_ontology_db();
    let nodes: Vec<uuid::Uuid> = (0..6).map(|_| uuid::Uuid::new_v4()).collect();
    let tx = db.begin();
    for i in 0..5 {
        db.insert_edge(tx, nodes[i], nodes[i + 1], "R".to_string(), std::collections::HashMap::new()).unwrap();
    }
    db.commit(tx).unwrap();

    let out = db.query_bfs(nodes[0], None, contextdb_core::Direction::Outgoing, 3, db.snapshot()).unwrap();
    assert_eq!(out.nodes.len(), 3);
}

#[test]
#[ignore = "requires 1M row scale harness"]
fn bc_06_ignored_scale() {}
