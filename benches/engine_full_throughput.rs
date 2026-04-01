use contextdb_core::{Direction, Value};
use contextdb_engine::Database;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use roaring::RoaringTreemap;
use std::collections::HashMap;
use uuid::Uuid;

fn batched_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("batched_insert");
    for count in [1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let db = Database::open_memory();
                db.execute(
                    "CREATE TABLE items (id UUID PRIMARY KEY, seq INTEGER)",
                    &HashMap::new(),
                )
                .unwrap();
                let tx = db.begin();
                for i in 0..count {
                    db.insert_row(
                        tx,
                        "items",
                        HashMap::from([
                            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                            ("seq".to_string(), Value::Int64(i as i64)),
                        ]),
                    )
                    .unwrap();
                }
                db.commit(tx).unwrap();
            });
        });
    }
    group.finish();
}

fn persist_and_reopen(c: &mut Criterion) {
    c.bench_function("persist_10k_rows_reopen", |b| {
        b.iter(|| {
            let tmp = tempfile::TempDir::new().unwrap();
            let path = tmp.path().join("bench.db");
            let db = Database::open(&path).unwrap();
            db.execute(
                "CREATE TABLE t (id UUID PRIMARY KEY, seq INTEGER)",
                &HashMap::new(),
            )
            .unwrap();
            for batch in 0..100 {
                let tx = db.begin();
                for i in 0..100 {
                    db.insert_row(
                        tx,
                        "t",
                        HashMap::from([
                            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                            ("seq".to_string(), Value::Int64((batch * 100 + i) as i64)),
                        ]),
                    )
                    .unwrap();
                }
                db.commit(tx).unwrap();
            }
            db.close().unwrap();
            let db2 = Database::open(&path).unwrap();
            assert_eq!(db2.scan("t", db2.snapshot()).unwrap().len(), 10_000);
        });
    });
}

fn mixed_workflow_latency(c: &mut Criterion) {
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

    c.bench_function("mixed_workflow/graph_relational_vector", |b| {
        b.iter(|| {
            let neighborhood = db
                .query_bfs(
                    root,
                    Some(&["RELATES_TO".to_string()]),
                    Direction::Outgoing,
                    2,
                    db.snapshot(),
                )
                .unwrap();
            let ids: std::collections::HashSet<Uuid> =
                neighborhood.nodes.iter().map(|n| n.id).collect();
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
        });
    });
}

criterion_group!(
    benches,
    batched_insert_throughput,
    persist_and_reopen,
    mixed_workflow_latency,
);
criterion_main!(benches);
