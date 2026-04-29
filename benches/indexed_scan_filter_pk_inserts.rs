mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROWS: i64 = 10_000;
const SAMPLE: i64 = 7_300;

fn seed_and_assert() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let mut sampled_id = None;
    for i in 0..ROWS {
        let id = Uuid::new_v4();
        if i == SAMPLE {
            sampled_id = Some(id);
        }
        db.execute(
            "INSERT INTO t (id, a) VALUES ($id, $a)",
            &params(vec![("id", Value::Uuid(id)), ("a", Value::Int64(i))]),
        )
        .unwrap();
    }
    let id = sampled_id.unwrap();
    let probe = db
        .__probe_constraint_check("t", "id", Value::Uuid(id))
        .unwrap();
    assert_eq!(probe.trace.physical_plan, "IndexScan");
    let result = db
        .execute(
            "SELECT a FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int64(SAMPLE)]]);
}

fn timed_seed_and_assert() {
    let started = Instant::now();
    seed_and_assert();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(4),
        "10K PK inserts must finish in <4s, took {elapsed:?}"
    );
}

fn bench_pk01_pk_inserts_routes_through_index(c: &mut Criterion) {
    seed_and_assert();
    c.bench_function("pk01_pk_inserts_routes_through_index", |b| {
        b.iter(timed_seed_and_assert)
    });
}

criterion_group!(benches, bench_pk01_pk_inserts_routes_through_index);
criterion_main!(benches);
