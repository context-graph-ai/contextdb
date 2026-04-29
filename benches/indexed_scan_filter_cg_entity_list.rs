mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROWS: i64 = 5_000;

fn seed_entities() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT, name TEXT, created_at TIMESTAMP)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_entity_type ON entities (entity_type, created_at DESC, id DESC)",
        &empty(),
    )
    .unwrap();
    for i in 0..ROWS {
        let entity_type = if i % 2 == 0 { "Service" } else { "Database" };
        let name = if i % 5 == 0 {
            format!("pay-entity-{i}")
        } else {
            format!("misc-entity-{i}")
        };
        db.execute(
            "INSERT INTO entities (id, entity_type, name, created_at) VALUES ($id, $et, $n, $ts)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("et", Value::Text(entity_type.into())),
                ("n", Value::Text(name)),
                ("ts", Value::Timestamp(1_700_000_000_000 + i)),
            ]),
        )
        .unwrap();
    }
    db
}

fn run_filtered_query(db: &Database, rows_examined_limit: u64) {
    db.__reset_rows_examined();
    let result = db
        .execute(
            "SELECT id, entity_type, name FROM entities \
             WHERE entity_type = $et AND name LIKE $pat \
             ORDER BY created_at DESC, id DESC",
            &params(vec![
                ("et", Value::Text("Service".into())),
                ("pat", Value::Text("%pay%".into())),
            ]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 500);
    assert_eq!(result.trace.physical_plan, "IndexScan");
    assert!(result.trace.sort_elided);
    assert!(
        db.__rows_examined() <= rows_examined_limit,
        "IndexScan must not examine the full 5K table; got {}",
        db.__rows_examined()
    );
}

fn seed_and_assert() {
    let db = seed_entities();
    run_filtered_query(&db, 3000);
}

fn timed_queries_and_assert(db: &Database) {
    let mut samples = Vec::new();
    for _ in 0..20 {
        let started = Instant::now();
        run_filtered_query(db, 3000);
        samples.push(started.elapsed());
    }
    samples.sort();
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    assert!(
        p95 <= Duration::from_millis(150),
        "p95 {p95:?} exceeds 150ms budget"
    );
}

fn bench_cg02_entity_list_filter_under_budget(c: &mut Criterion) {
    seed_and_assert();
    c.bench_function("cg02_entity_list_filter_under_budget", |b| {
        b.iter_batched(
            seed_entities,
            |db| timed_queries_and_assert(&db),
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(benches, bench_cg02_entity_list_filter_under_budget);
criterion_main!(benches);
