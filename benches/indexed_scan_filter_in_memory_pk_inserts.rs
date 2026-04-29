mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROWS: i64 = 100_000;
const SAMPLE: i64 = 73_000;

fn seed_and_assert() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, bucket INTEGER)",
        &empty(),
    )
    .unwrap();
    let mut sampled_id = None;
    for i in 0..ROWS {
        let id = Uuid::new_v4();
        if i == SAMPLE {
            sampled_id = Some(id);
        }
        db.execute(
            "INSERT INTO t (id, bucket) VALUES ($id, $b)",
            &params(vec![("id", Value::Uuid(id)), ("b", Value::Int64(i))]),
        )
        .unwrap();
    }
    let count = db.execute("SELECT COUNT(*) FROM t", &empty()).unwrap();
    assert_eq!(count.rows, vec![vec![Value::Int64(ROWS)]]);
    let result = db
        .execute(
            "SELECT bucket FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sampled_id.unwrap()))]),
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int64(SAMPLE)]]);
}

fn timed_seed_and_assert() {
    let started = Instant::now();
    seed_and_assert();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "in-memory 100K PK INSERTs must finish in <10s, took {elapsed:?}"
    );
}

fn bench_in_memory_100k_pk_inserts_sublinear(c: &mut Criterion) {
    seed_and_assert();
    c.bench_function("in_memory_100k_pk_inserts_sublinear", |b| {
        b.iter(timed_seed_and_assert)
    });
}

criterion_group!(benches, bench_in_memory_100k_pk_inserts_sublinear);
criterion_main!(benches);
