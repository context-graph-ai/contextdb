use contextdb_engine::Database;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use uuid::Uuid;

fn sql_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_insert");
    for count in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let db = Database::open_memory();
                db.execute(
                    "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, seq INTEGER)",
                    &HashMap::new(),
                )
                .unwrap();
                for i in 0..count {
                    db.execute(
                        "INSERT INTO items (id, name, seq) VALUES ($id, $name, $seq)",
                        &HashMap::from([
                            (
                                "id".to_string(),
                                contextdb_core::Value::Uuid(Uuid::new_v4()),
                            ),
                            (
                                "name".to_string(),
                                contextdb_core::Value::Text(format!("item-{i}")),
                            ),
                            ("seq".to_string(), contextdb_core::Value::Int64(i as i64)),
                        ]),
                    )
                    .unwrap();
                }
            });
        });
    }
    group.finish();
}

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
                            (
                                "id".to_string(),
                                contextdb_core::Value::Uuid(Uuid::new_v4()),
                            ),
                            ("seq".to_string(), contextdb_core::Value::Int64(i as i64)),
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

fn vector_insert_and_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector");

    group.bench_function("insert_1000_vectors", |b| {
        b.iter(|| {
            let db = Database::open_memory();
            db.execute(
                "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
                &HashMap::new(),
            )
            .unwrap();
            let tx = db.begin();
            for i in 0..1_000 {
                let angle = i as f32 * 0.01;
                let rid = db
                    .insert_row(
                        tx,
                        "obs",
                        HashMap::from([(
                            "id".to_string(),
                            contextdb_core::Value::Uuid(Uuid::new_v4()),
                        )]),
                    )
                    .unwrap();
                db.insert_vector(tx, rid, vec![angle.cos(), angle.sin(), 0.0])
                    .unwrap();
            }
            db.commit(tx).unwrap();
        });
    });

    group.bench_function("search_in_1000_vectors", |b| {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin();
        for i in 0..1_000 {
            let angle = i as f32 * 0.01;
            let rid = db
                .insert_row(
                    tx,
                    "obs",
                    HashMap::from([(
                        "id".to_string(),
                        contextdb_core::Value::Uuid(Uuid::new_v4()),
                    )]),
                )
                .unwrap();
            db.insert_vector(tx, rid, vec![angle.cos(), angle.sin(), 0.0])
                .unwrap();
        }
        db.commit(tx).unwrap();

        b.iter(|| {
            db.query_vector(&[1.0, 0.0, 0.0], 10, None, db.snapshot())
                .unwrap();
        });
    });

    group.finish();
}

fn graph_bfs_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_bfs");

    group.bench_function("bfs_1000_node_chain", |b| {
        let db = Database::open_memory();
        let tx = db.begin();
        let mut nodes = Vec::new();
        for _ in 0..1_000 {
            nodes.push(Uuid::new_v4());
        }
        for i in 1..nodes.len() {
            db.insert_edge(
                tx,
                nodes[i - 1],
                nodes[i],
                "CHAIN".to_string(),
                HashMap::new(),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();

        b.iter(|| {
            db.query_bfs(
                nodes[0],
                None,
                contextdb_core::Direction::Outgoing,
                1000,
                db.snapshot(),
            )
            .unwrap();
        });
    });

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
                            (
                                "id".to_string(),
                                contextdb_core::Value::Uuid(Uuid::new_v4()),
                            ),
                            (
                                "seq".to_string(),
                                contextdb_core::Value::Int64((batch * 100 + i) as i64),
                            ),
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

criterion_group!(
    benches,
    sql_insert_throughput,
    batched_insert_throughput,
    vector_insert_and_search,
    graph_bfs_throughput,
    persist_and_reopen,
);
criterion_main!(benches);
