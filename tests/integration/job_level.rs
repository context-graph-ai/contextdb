use super::helpers::*;
use contextdb_core::{Direction, Value, schema};

#[test]
fn job_01_placeholder_all_ontology_tables_queryable() {
    let db = setup_ontology_db();
    for table in schema::ONTOLOGY_TABLES {
        let rows = db.scan(table, db.snapshot()).unwrap();
        assert!(rows.is_empty());
    }
}

#[test]
fn job_02_impact_analysis_explain_and_result() {
    let db = setup_ontology_db();
    let (entity_id, _d1, _d2) = setup_impact_analysis_scenario(&db);

    let bfs = db
        .query_bfs(
            entity_id,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            2,
            db.snapshot(),
        )
        .unwrap();
    assert!(!bfs.nodes.is_empty());

    let explain = db
        .explain("WITH n AS (MATCH (a)-[:BASED_ON*1..2]->(b) RETURN b.id) SELECT * FROM n")
        .unwrap();
    assert!(explain.contains("GraphBfs"));
}

#[test]
fn job_03_vector_search_with_explain() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let row = db
        .insert_row(
            tx,
            "observations",
            std::collections::HashMap::from([(
                "id".to_string(),
                Value::Uuid(uuid::Uuid::new_v4()),
            )]),
        )
        .unwrap();
    db.insert_vector(tx, row, vec![1.0, 0.0]).unwrap();
    db.commit(tx).unwrap();

    let out = db
        .execute(
            "SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 1",
            &make_params(vec![("q", Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);

    let explain = db
        .explain("SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 1")
        .unwrap();
    assert!(explain.contains("VectorSearch"));
}

#[test]
fn job_04_cross_subsystem_tx() {
    let db = setup_ontology_db();
    let source = uuid::Uuid::new_v4();
    let target = uuid::Uuid::new_v4();

    let tx = db.begin();
    let rid = db
        .insert_row(
            tx,
            "entities",
            std::collections::HashMap::from([("id".to_string(), Value::Uuid(source))]),
        )
        .unwrap();
    db.insert_edge(
        tx,
        source,
        target,
        "RELATES_TO".to_string(),
        std::collections::HashMap::new(),
    )
    .unwrap();
    db.insert_vector(tx, rid, vec![1.0, 0.0]).unwrap();
    db.commit(tx).unwrap();

    assert_eq!(db.scan("entities", db.snapshot()).unwrap().len(), 1);
}

#[test]
#[ignore = "requires disk-backed storage + memory accounting"]
fn job_05_ignored_disk_budget() {}

#[test]
#[ignore = "requires sync implementation"]
fn job_06_ignored_sync() {}

#[test]
fn job_07_embedded_operation() {
    let db = setup_ontology_db();
    assert_eq!(db.scan("entities", db.snapshot()).unwrap().len(), 0);
}
