mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn seed_fixture() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, obs_type TEXT NOT NULL, source TEXT NOT NULL, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT NOT NULL, name TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT NOT NULL, status TEXT NOT NULL)",
        &empty(),
    )
    .unwrap();

    let gate = Uuid::from_u128(1);
    let parking = Uuid::from_u128(2);
    db.execute(
        "INSERT INTO entities (id, entity_type, name) VALUES ($id, 'LOCATION', 'main-gate')",
        &params(vec![("id", Value::Uuid(gate))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO entities (id, entity_type, name) VALUES ($id, 'LOCATION', 'parking-lot')",
        &params(vec![("id", Value::Uuid(parking))]),
    )
    .unwrap();

    let dec_gate = Uuid::from_u128(10);
    let dec_park = Uuid::from_u128(11);
    db.execute(
        "INSERT INTO decisions (id, description, status) VALUES ($id, 'Alert on unknown person at gate', 'active')",
        &params(vec![("id", Value::Uuid(dec_gate))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO decisions (id, description, status) VALUES ($id, 'Log vehicle plates in parking', 'superseded')",
        &params(vec![("id", Value::Uuid(dec_park))]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'BASED_ON')",
        &params(vec![
            ("src", Value::Uuid(dec_gate)),
            ("tgt", Value::Uuid(gate)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'BASED_ON')",
        &params(vec![
            ("src", Value::Uuid(dec_park)),
            ("tgt", Value::Uuid(parking)),
        ]),
    )
    .unwrap();

    let obs_g1 = Uuid::from_u128(20);
    let obs_g2 = Uuid::from_u128(21);
    let obs_p1 = Uuid::from_u128(22);
    db.execute(
        "INSERT INTO observations (id, obs_type, source, embedding) VALUES ($id, 'person_detected', 'cam-gate', [0.9, 0.1, 0.0])",
        &params(vec![("id", Value::Uuid(obs_g1))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO observations (id, obs_type, source, embedding) VALUES ($id, 'person_detected', 'cam-gate', [0.95, 0.05, 0.0])",
        &params(vec![("id", Value::Uuid(obs_g2))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO observations (id, obs_type, source, embedding) VALUES ($id, 'vehicle_detected', 'cam-parking', [0.0, 0.95, 0.05])",
        &params(vec![("id", Value::Uuid(obs_p1))]),
    )
    .unwrap();

    db.execute(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'OBSERVED_ON')",
        &params(vec![
            ("src", Value::Uuid(obs_g1)),
            ("tgt", Value::Uuid(gate)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'OBSERVED_ON')",
        &params(vec![
            ("src", Value::Uuid(obs_g2)),
            ("tgt", Value::Uuid(gate)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES ($src, $tgt, 'OBSERVED_ON')",
        &params(vec![
            ("src", Value::Uuid(obs_p1)),
            ("tgt", Value::Uuid(parking)),
        ]),
    )
    .unwrap();
    db
}

fn run_query_and_assert(db: &Database) {
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
            &params(vec![("query_vec", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let desc = match &result.rows[0][1] {
        Value::Text(value) => value,
        other => panic!("expected Text for description, got: {other:?}"),
    };
    assert!(desc.contains("gate"), "expected gate decision, got: {desc}");
    assert!(
        !desc.contains("parking"),
        "superseded parking decision should not appear"
    );
}

fn timed_query_and_assert(db: &Database) {
    let started = Instant::now();
    run_query_and_assert(db);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "query took {}ms, expected < 50ms",
        elapsed.as_millis()
    );
}

fn bench_three_paradigm_recall_under_50ms(c: &mut Criterion) {
    let db = seed_fixture();
    run_query_and_assert(&db);
    c.bench_function("three_paradigm_recall_under_50ms", |b| {
        b.iter_batched(
            seed_fixture,
            |db| timed_query_and_assert(&db),
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(benches, bench_three_paradigm_recall_under_50ms);
criterion_main!(benches);
