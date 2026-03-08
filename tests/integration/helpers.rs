use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

pub fn setup_ontology_db() -> Database {
    Database::open_memory()
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
