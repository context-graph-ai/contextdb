mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROWS: i64 = 10_000;
const SAMPLE: i64 = 5_000;

fn seed_and_assert() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (user_id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    let mut sample_user = None;
    for i in 0..ROWS {
        let uid = Uuid::new_v4();
        if i == SAMPLE {
            sample_user = Some(uid);
        }
        db.execute(
            "INSERT INTO t (user_id, name) VALUES ($id, $n)",
            &params(vec![
                ("id", Value::Uuid(uid)),
                ("n", Value::Text(format!("u{i}"))),
            ]),
        )
        .unwrap();
    }
    let user = sample_user.unwrap();
    let probe = db
        .__probe_constraint_check("t", "user_id", Value::Uuid(user))
        .unwrap();
    assert_eq!(probe.trace.physical_plan, "IndexScan");
    let result = db
        .execute(
            "SELECT user_id, name FROM t WHERE user_id = $id",
            &params(vec![("id", Value::Uuid(user))]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Uuid(user));
    assert_eq!(result.rows[0][1], Value::Text(format!("u{SAMPLE}")));
}

fn timed_seed_and_assert() {
    let started = Instant::now();
    seed_and_assert();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(4),
        "non-id PK inserts must finish in <4s, took {elapsed:?}"
    );
}

fn bench_non_id_pk_inserts_routes_through_index(c: &mut Criterion) {
    seed_and_assert();
    c.bench_function("non_id_pk_inserts_routes_through_index", |b| {
        b.iter(timed_seed_and_assert)
    });
}

criterion_group!(benches, bench_non_id_pk_inserts_routes_through_index);
criterion_main!(benches);
