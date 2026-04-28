use super::helpers::*;
use contextdb_core::{Direction, Value};
use contextdb_engine::Database;
use tempfile::TempDir;

#[test]
fn job_01_placeholder_all_ontology_tables_queryable() {
    let db = setup_ontology_db();
    for table in db.table_names() {
        let rows = db.scan(&table, db.snapshot()).unwrap();
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
        .explain(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,2}(b) COLUMNS (b.id AS b_id))",
        )
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
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("observations", "embedding"),
        row,
        embedding384(&[1.0, 0.0]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let out = db
        .execute(
            "SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 1",
            &make_params(vec![("q", Value::Vector(embedding384(&[1.0, 0.0])))]),
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
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("entities", "embedding"),
        rid,
        vec![1.0, 0.0],
    )
    .unwrap();
    db.commit(tx).unwrap();

    assert_eq!(db.scan("entities", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn job_05_ignored_disk_budget() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("job_05.db");
    let db = Database::open(&db_path).expect("open file-backed db");

    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, content TEXT)",
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let limit_kib = std::fs::metadata(&db_path)
        .expect("metadata")
        .len()
        .div_ceil(1024)
        + 96;
    db.execute(
        &format!("SET DISK_LIMIT '{limit_kib}K'"),
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let payload = "x".repeat(8 * 1024);
    let mut inserted = 0usize;
    let mut failure = None;
    for _ in 0..128 {
        match db.execute(
            "INSERT INTO observations (id, content) VALUES ($id, $content)",
            &std::collections::HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                ("content".to_string(), Value::Text(payload.clone())),
            ]),
        ) {
            Ok(_) => inserted += 1,
            Err(err) => {
                failure = Some(err.to_string());
                break;
            }
        }
    }

    assert!(
        inserted > 0,
        "disk budget should allow at least one ontology row before rejecting writes"
    );
    let err = failure.expect("disk budget must eventually reject oversized ontology growth");
    assert!(
        err.to_lowercase().contains("disk budget"),
        "error must mention disk budget, got: {err}"
    );
}

#[test]
fn job_07_embedded_operation() {
    let db = setup_ontology_db();
    assert_eq!(db.scan("entities", db.snapshot()).unwrap().len(), 0);
}
