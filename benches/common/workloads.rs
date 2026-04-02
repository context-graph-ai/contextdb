use contextdb_core::{Direction, Value};
use contextdb_engine::Database;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use uuid::Uuid;

pub fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

pub fn setup_graph_relational_vector_db() -> (Database, Uuid) {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, entity_id UUID, context_id TEXT, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .unwrap();

    let root = Uuid::new_v4();
    let neighbors: Vec<Uuid> = (0..32).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(root)),
            ("entity_type".to_string(), Value::Text("ROOT".to_string())),
            ("name".to_string(), Value::Text("root".to_string())),
        ]),
    )
    .unwrap();
    for (idx, neighbor) in neighbors.iter().enumerate() {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(*neighbor)),
                ("entity_type".to_string(), Value::Text("NODE".to_string())),
                ("name".to_string(), Value::Text(format!("n-{idx}"))),
            ]),
        )
        .unwrap();
        db.insert_edge(
            tx,
            root,
            *neighbor,
            "RELATES_TO".to_string(),
            HashMap::new(),
        )
        .unwrap();
        let row_id = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(*neighbor)),
                    ("context_id".to_string(), Value::Text("ctx".to_string())),
                ]),
            )
            .unwrap();
        db.insert_vector(
            tx,
            row_id,
            vec![1.0 - (idx as f32 * 0.02), idx as f32 * 0.02],
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    (db, root)
}

pub fn run_graph_relational_vector_query(db: &Database, root: Uuid) {
    let neighborhood = db
        .query_bfs(
            root,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .unwrap();
    let ids: std::collections::HashSet<Uuid> = neighborhood.nodes.iter().map(|n| n.id).collect();
    let candidate_obs = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values
                .get("entity_id")
                .and_then(Value::as_uuid)
                .map(|id| ids.contains(id))
                .unwrap_or(false)
                && r.values.get("context_id") == Some(&Value::Text("ctx".to_string()))
        })
        .unwrap();
    let mut candidates = RoaringTreemap::new();
    for r in &candidate_obs {
        candidates.insert(r.row_id);
    }
    db.query_vector(&[1.0, 0.0], 5, Some(&candidates), db.snapshot())
        .unwrap();
}

pub fn setup_flagship_recall_db() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());

    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, obs_type TEXT NOT NULL, source TEXT NOT NULL, embedding VECTOR(3))",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT NOT NULL, name TEXT NOT NULL)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT NOT NULL, status TEXT NOT NULL)",
        &empty_params(),
    )
    .unwrap();

    let gate = Uuid::from_u128(1);
    let parking = Uuid::from_u128(2);
    let dec_gate = Uuid::from_u128(10);
    let dec_park = Uuid::from_u128(11);
    let obs_g1 = Uuid::from_u128(20);
    let obs_g2 = Uuid::from_u128(21);
    let obs_p1 = Uuid::from_u128(22);

    let insert = |sql: &str, params: Vec<(&str, Value)>| {
        db.execute(
            sql,
            &params
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap();
    };

    insert(
        "INSERT INTO entities (id, entity_type, name) VALUES ($id, 'LOCATION', 'main-gate')",
        vec![("id", Value::Uuid(gate))],
    );
    insert(
        "INSERT INTO entities (id, entity_type, name) VALUES ($id, 'LOCATION', 'parking-lot')",
        vec![("id", Value::Uuid(parking))],
    );
    insert(
        "INSERT INTO decisions (id, description, status) VALUES ($id, 'Alert on unknown person at gate', 'active')",
        vec![("id", Value::Uuid(dec_gate))],
    );
    insert(
        "INSERT INTO decisions (id, description, status) VALUES ($id, 'Log vehicle plates in parking', 'superseded')",
        vec![("id", Value::Uuid(dec_park))],
    );
    insert(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'BASED_ON')",
        vec![("src", Value::Uuid(dec_gate)), ("tgt", Value::Uuid(gate))],
    );
    insert(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'BASED_ON')",
        vec![
            ("src", Value::Uuid(dec_park)),
            ("tgt", Value::Uuid(parking)),
        ],
    );
    insert(
        "INSERT INTO observations (id, obs_type, source, embedding) VALUES ($id, 'person_detected', 'cam-gate', [0.9, 0.1, 0.0])",
        vec![("id", Value::Uuid(obs_g1))],
    );
    insert(
        "INSERT INTO observations (id, obs_type, source, embedding) VALUES ($id, 'person_detected', 'cam-gate', [0.95, 0.05, 0.0])",
        vec![("id", Value::Uuid(obs_g2))],
    );
    insert(
        "INSERT INTO observations (id, obs_type, source, embedding) VALUES ($id, 'vehicle_detected', 'cam-parking', [0.0, 0.95, 0.05])",
        vec![("id", Value::Uuid(obs_p1))],
    );
    insert(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'OBSERVED_ON')",
        vec![("src", Value::Uuid(obs_g1)), ("tgt", Value::Uuid(gate))],
    );
    insert(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'OBSERVED_ON')",
        vec![("src", Value::Uuid(obs_g2)), ("tgt", Value::Uuid(gate))],
    );
    insert(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'OBSERVED_ON')",
        vec![("src", Value::Uuid(obs_p1)), ("tgt", Value::Uuid(parking))],
    );

    db
}

pub fn run_flagship_recall_query(db: &Database) {
    let result = db
        .execute(
            "WITH similar_obs AS (\
                SELECT id FROM observations \
                ORDER BY embedding <=> $query_vec \
                LIMIT 5\
            ), \
            reached AS (\
                SELECT b_id FROM GRAPH_TABLE(\
                    edges MATCH (a)-[:OBSERVED_ON]->{1,1}(entity)<-[:BASED_ON]-(b) \
                    WHERE a.id IN (SELECT id FROM similar_obs) \
                    COLUMNS (b.id AS b_id)\
                )\
            ) \
            SELECT d.id, d.description \
            FROM decisions d \
            INNER JOIN reached r ON d.id = r.b_id \
            WHERE d.status = 'active'",
            &HashMap::from([("query_vec".to_string(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "expected one active matching decision"
    );
}

pub fn setup_subscription_db() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db
}

pub fn subscription_insert_sql(id: u128) -> String {
    format!("INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-{id:012}', 'row-{id}')")
}

pub fn drain_events(rx: &Receiver<contextdb_engine::plugin::CommitEvent>) -> usize {
    let mut count = 0usize;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    count
}
