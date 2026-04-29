mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const ROWS: i64 = 100_000;
const SAMPLE: i64 = 42_000;

fn insert_rows(path: &std::path::Path) -> Uuid {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, bucket INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_bucket ON t (bucket)", &empty())
        .unwrap();
    let mut sampled_id = None;
    for i in 0..ROWS {
        if i % 1_000 == 0 {
            db.execute("BEGIN", &empty()).unwrap();
        }
        let id = Uuid::new_v4();
        if i == SAMPLE {
            sampled_id = Some(id);
        }
        db.execute(
            "INSERT INTO t (id, bucket) VALUES ($id, $b)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("b", Value::Int64(i % 10_000)),
            ]),
        )
        .unwrap();
        if i % 1_000 == 999 {
            db.execute("COMMIT", &empty()).unwrap();
        }
    }
    sampled_id.unwrap()
}

fn assert_reopened(path: &std::path::Path, sampled_id: Uuid) {
    let db = Database::open(path).unwrap();
    let count = db.execute("SELECT COUNT(*) FROM t", &empty()).unwrap();
    assert_eq!(count.rows, vec![vec![Value::Int64(ROWS)]]);
    let r = db
        .execute("SELECT id FROM t WHERE bucket = 42", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows.len(), 10);
    assert!(db.__rows_examined() <= 50, "got {}", db.__rows_examined());
    let sampled = db
        .execute(
            "SELECT id, bucket FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(sampled_id))]),
        )
        .unwrap();
    assert_eq!(sampled.rows.len(), 1);
    assert_eq!(sampled.rows[0][0], Value::Uuid(sampled_id));
    assert_eq!(sampled.rows[0][1], Value::Int64(SAMPLE % 10_000));
}

fn seed_reopen_and_assert() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let sampled_id = insert_rows(&path);
    assert_reopened(&path, sampled_id);
}

fn timed_seed_reopen_and_assert() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let insert_started = Instant::now();
    let sampled_id = insert_rows(&path);
    let insert_elapsed = insert_started.elapsed();
    assert!(
        insert_elapsed < Duration::from_secs(900),
        "100K file-backed INSERTs must finish in <900s, took {insert_elapsed:?}"
    );
    let reopen_started = Instant::now();
    assert_reopened(&path, sampled_id);
    let reopen_elapsed = reopen_started.elapsed();
    assert!(
        reopen_elapsed < Duration::from_secs(10),
        "100K-row reopen must complete in <10s; took {reopen_elapsed:?}"
    );
}

fn bench_file_backed_100k_rows_open_upper_bound(c: &mut Criterion) {
    seed_reopen_and_assert();
    c.bench_function("file_backed_100k_rows_open_upper_bound", |b| {
        b.iter(timed_seed_reopen_and_assert)
    });
}

criterion_group!(benches, bench_file_backed_100k_rows_open_upper_bound);
criterion_main!(benches);
