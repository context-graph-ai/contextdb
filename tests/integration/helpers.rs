use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

pub fn embedding384(values: &[f32]) -> Vec<f32> {
    assert!(
        values.len() <= 384,
        "test embedding helper accepts at most 384 dimensions"
    );
    let mut vector = vec![0.0; 384];
    for (slot, value) in vector.iter_mut().zip(values.iter()) {
        *slot = *value;
    }
    vector
}

pub fn setup_ontology_db() -> Database {
    let db = Database::open_memory();
    let params = HashMap::new();

    db.execute(
        "CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, confidence REAL, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, entity_type TEXT, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entity_snapshots (id UUID PRIMARY KEY, entity_id UUID, state JSON, valid_from INTEGER, valid_to INTEGER)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, entity_id UUID, data JSON, embedding VECTOR(384)) IMMUTABLE",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOLEAN)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, affected_decision_id UUID, status TEXT, severity TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE approvals (id UUID PRIMARY KEY, decision_id UUID)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE patterns (id UUID PRIMARY KEY, description TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sync_state (id UUID PRIMARY KEY, push_watermark INTEGER, pull_watermark INTEGER)",
        &params,
    )
    .unwrap();

    db
}

pub fn setup_ontology_db_with_dag() -> Database {
    let db = Database::open_memory();
    let params = HashMap::new();

    db.execute(
        "CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, confidence REAL, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, entity_type TEXT, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entity_snapshots (id UUID PRIMARY KEY, entity_id UUID, state JSON, valid_from INTEGER, valid_to INTEGER)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, entity_id UUID, data JSON, embedding VECTOR(384)) IMMUTABLE",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOLEAN)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, affected_decision_id UUID, status TEXT, severity TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE approvals (id UUID PRIMARY KEY, decision_id UUID)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE patterns (id UUID PRIMARY KEY, description TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sync_state (id UUID PRIMARY KEY, push_watermark INTEGER, pull_watermark INTEGER)",
        &params,
    )
    .unwrap();

    db
}

pub fn setup_propagation_ontology_db() -> Database {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT, description TEXT)",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT, confidence REAL, embedding VECTOR(384)) STATE MACHINE (status: active -> [archived, paused, superseded])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, confidence REAL, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, embedding VECTOR(384)) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated PROPAGATE ON STATE invalidated EXCLUDE VECTOR PROPAGATE ON STATE superseded EXCLUDE VECTOR",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, entity_type TEXT)",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entity_snapshots (id UUID PRIMARY KEY, entity_id UUID, state JSON, valid_from INTEGER, valid_to INTEGER)",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, entity_id UUID, data JSON, embedding VECTOR(384)) IMMUTABLE",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOLEAN)",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, affected_decision_id UUID, status TEXT, severity TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE approvals (id UUID PRIMARY KEY, decision_id UUID)",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE patterns (id UUID PRIMARY KEY, description TEXT)",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sync_state (id UUID PRIMARY KEY, push_watermark INTEGER, pull_watermark INTEGER)",
        &p,
    )
    .unwrap();

    db
}

pub fn setup_impact_analysis_scenario(db: &Database) -> (Uuid, Uuid, Uuid) {
    let entity_id = Uuid::new_v4();
    let decision1_id = Uuid::new_v4();
    let decision2_id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(entity_id)),
            ("name".to_string(), Value::Text("entity".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(decision1_id)),
            ("description".to_string(), Value::Text("d1".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(decision2_id)),
            ("description".to_string(), Value::Text("d2".to_string())),
        ]),
    )
    .unwrap();
    db.insert_edge(
        tx,
        decision1_id,
        entity_id,
        "BASED_ON".to_string(),
        HashMap::new(),
    )
    .unwrap();
    db.insert_edge(
        tx,
        decision2_id,
        decision1_id,
        "CITES".to_string(),
        HashMap::new(),
    )
    .unwrap();
    db.commit(tx).unwrap();

    (entity_id, decision1_id, decision2_id)
}

pub fn make_params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<HashMap<_, _>>()
}
