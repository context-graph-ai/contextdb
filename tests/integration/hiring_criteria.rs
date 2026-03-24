use super::helpers::*;
use contextdb_core::Value;

#[test]
fn hc_f01_ontology_ops() {
    let db = setup_ontology_db();
    let id = uuid::Uuid::new_v4();
    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("n".into())),
        ]),
    )
    .unwrap();
    assert_eq!(db.scan("entities", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn hc_f03_ignored_hnsw_recall() {}

#[test]
#[ignore = "requires ARM64 cross compile"]
fn hc_f06_ignored_arm64() {}

#[test]
#[ignore = "requires OOM harness"]
fn hc_t01_ignored_oom() {}

#[test]
#[ignore = "requires WAL durability"]
fn hc_t02_ignored_wal() {}

#[test]
fn hc_t04_bfs_mvcc() {
    let db = setup_ontology_db();
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "R".to_string(), std::collections::HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();
    let result = db
        .query_bfs(
            a,
            None,
            contextdb_core::Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(result.nodes.len(), 1);
}
