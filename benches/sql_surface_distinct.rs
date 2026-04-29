mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROWS: usize = 5_000;

fn seed_items() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, tag TEXT)",
        &empty(),
    )
    .unwrap();
    for i in 0..ROWS {
        let tag = format!("tag-{}", i % 100);
        db.execute(
            "INSERT INTO items (id, tag) VALUES ($id, $tag)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("tag", Value::Text(tag)),
            ]),
        )
        .unwrap();
    }
    db
}

fn assert_distinct_tags(db: &Database) {
    let result = db
        .execute("SELECT DISTINCT tag FROM items", &empty())
        .unwrap();
    assert_eq!(result.rows.len(), 100);
}

fn seed_and_assert() {
    let db = seed_items();
    assert_distinct_tags(&db);
}

fn timed_distinct_and_assert(db: &Database) {
    let started = Instant::now();
    assert_distinct_tags(db);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "SELECT DISTINCT should finish within 5 seconds on 5k rows"
    );
}

fn bench_sql_distinct_not_quadratic(c: &mut Criterion) {
    seed_and_assert();
    c.bench_function("sql_distinct_not_quadratic", |b| {
        b.iter_batched(
            seed_items,
            |db| timed_distinct_and_assert(&db),
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(benches, bench_sql_distinct_not_quadratic);
criterion_main!(benches);
