mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::env;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const SECONDARY_INDEX_COLUMNS: usize = 19;
const INSERT_BATCH_ROWS: usize = 1_000;
const WIDE_TOTAL_ROWS: usize = 1_000;
const FILE_AUTOCOMMIT_ROWS: usize = 100;
const REOPEN_INDEX_SLOTS: usize = 5;

fn uuid(n: usize) -> Uuid {
    Uuid::from_u128(0x5A00_0000_0000_0000_0000_0000_0000_0000 + n as u128)
}

fn bench_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn create_indexed_table(db: &Database, table: &str, total_index_slots: usize) {
    assert!((1..=20).contains(&total_index_slots));
    let mut columns = vec![
        "id UUID PRIMARY KEY".to_string(),
        "seq INTEGER".to_string(),
        "payload TEXT".to_string(),
    ];
    for i in 0..SECONDARY_INDEX_COLUMNS {
        columns.push(format!("c{i} INTEGER"));
    }
    db.execute(
        &format!("CREATE TABLE {table} ({})", columns.join(", ")),
        &empty(),
    )
    .unwrap();
    for i in 0..(total_index_slots - 1) {
        db.execute(
            &format!("CREATE INDEX {table}_idx_{i} ON {table} (c{i})"),
            &empty(),
        )
        .unwrap();
    }
}

fn row_values(row: usize, payload: Option<&str>) -> HashMap<String, Value> {
    let mut values = HashMap::with_capacity(SECONDARY_INDEX_COLUMNS + 3);
    values.insert("id".to_string(), Value::Uuid(uuid(row)));
    values.insert("seq".to_string(), Value::Int64(row as i64));
    values.insert(
        "payload".to_string(),
        Value::Text(payload.unwrap_or("").to_string()),
    );
    for i in 0..SECONDARY_INDEX_COLUMNS {
        values.insert(format!("c{i}"), Value::Int64((row + i) as i64));
    }
    values
}

fn insert_direct_batch(
    db: &Database,
    table: &str,
    start_row: usize,
    rows: usize,
    payload: Option<&str>,
) {
    let tx = db.begin_or_panic();
    for row in start_row..(start_row + rows) {
        db.insert_row(tx, table, row_values(row, payload)).unwrap();
    }
    db.commit(tx).unwrap();
}

fn setup_insert_case(total_index_slots: usize, sibling_index_slots: Option<usize>) -> Database {
    let db = Database::open_memory();
    create_indexed_table(&db, "t", total_index_slots);
    if let Some(slots) = sibling_index_slots {
        create_indexed_table(&db, "sib", slots);
    }
    db
}

fn assert_insert_counter_gates() {
    for total_index_slots in [1, 5, 20] {
        let db = setup_insert_case(total_index_slots, None);
        db.__reset_index_maintenance_visits();
        insert_direct_batch(&db, "t", total_index_slots * 10_000, 3, None);
        assert_eq!(
            db.__index_maintenance_visits(),
            (3 * total_index_slots) as u64,
            "insert maintenance visits must match rows times table-local index slots"
        );
    }

    let db = setup_insert_case(1, Some(20));
    db.__reset_index_maintenance_visits();
    insert_direct_batch(&db, "t", 900_000, 3, None);
    assert_eq!(
        db.__index_maintenance_visits(),
        3,
        "sibling indexes must not affect writes into a one-index table"
    );
}

fn bench_insert_cost_by_index_count(c: &mut Criterion) {
    assert_insert_counter_gates();

    let mut group = c.benchmark_group("storage_scale_insert_index_count");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(300));
    for total_index_slots in [1, 5, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(total_index_slots),
            &total_index_slots,
            |b, &slots| {
                b.iter_batched(
                    || setup_insert_case(slots, None),
                    |db| {
                        db.__reset_index_maintenance_visits();
                        insert_direct_batch(&db, "t", slots * 100_000, INSERT_BATCH_ROWS, None);
                        assert_eq!(
                            db.__index_maintenance_visits(),
                            (INSERT_BATCH_ROWS * slots) as u64
                        );
                        black_box(db.__index_maintenance_visits());
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.bench_function("cross_table_sibling_20_indexes", |b| {
        b.iter_batched(
            || setup_insert_case(1, Some(20)),
            |db| {
                db.__reset_index_maintenance_visits();
                insert_direct_batch(&db, "t", 800_000, INSERT_BATCH_ROWS, None);
                assert_eq!(db.__index_maintenance_visits(), INSERT_BATCH_ROWS as u64);
                black_box(db.__index_maintenance_visits());
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn seed_file_backed(rows: usize, total_index_slots: usize) -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let db = Database::open(&path).unwrap();
    create_indexed_table(&db, "t", total_index_slots);
    let mut start = 0;
    while start < rows {
        let batch = (rows - start).min(1_000);
        insert_direct_batch(&db, "t", start, batch, None);
        start += batch;
    }
    db.close().unwrap();
    (tmp, path)
}

fn reopen_and_assert(path: &Path, rows: usize, total_index_slots: usize) {
    let db = Database::open(path).unwrap();
    assert_eq!(
        db.__open_index_maintenance_visits(),
        (rows * total_index_slots) as u64,
        "open maintenance visits must match rows times table-local index slots"
    );
    let count = db.execute("SELECT COUNT(*) FROM t", &empty()).unwrap();
    assert_eq!(count.rows, vec![vec![Value::Int64(rows as i64)]]);
}

fn reopen_row_counts() -> Vec<usize> {
    let mut rows = vec![1_000, 10_000, 100_000];
    if env::var("CONTEXTDB_SSH_REOPEN_1M").ok().as_deref() == Some("1") {
        rows.push(bench_env_usize("CONTEXTDB_SSH_REOPEN_ROWS", 1_000_000));
    }
    rows
}

fn bench_reopen_cost_by_row_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_scale_reopen_rows");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(300));
    for rows in reopen_row_counts() {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter_batched(
                || seed_file_backed(rows, REOPEN_INDEX_SLOTS),
                |(_tmp, path)| reopen_and_assert(&path, rows, REOPEN_INDEX_SLOTS),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn setup_wide_table() -> Database {
    let db = Database::open_memory();
    create_indexed_table(&db, "t", 1);
    db
}

fn insert_wide_rows_by_commit_size(db: &Database, rows_per_commit: usize) {
    let payload = "x".repeat(1024);
    let mut start = 0;
    while start < WIDE_TOTAL_ROWS {
        let batch = (WIDE_TOTAL_ROWS - start).min(rows_per_commit);
        insert_direct_batch(db, "t", 2_000_000 + start, batch, Some(&payload));
        start += batch;
    }
    let count = db.execute("SELECT COUNT(*) FROM t", &empty()).unwrap();
    assert_eq!(count.rows, vec![vec![Value::Int64(WIDE_TOTAL_ROWS as i64)]]);
}

fn bench_wide_batch_size_comparator(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_scale_wide_rows_per_commit");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(300));
    for rows_per_commit in [1, 10, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(rows_per_commit),
            &rows_per_commit,
            |b, &batch| {
                b.iter_batched(
                    setup_wide_table,
                    |db| insert_wide_rows_by_commit_size(&db, batch),
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn setup_file_autocommit_table() -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, seq INTEGER, payload TEXT)",
        &empty(),
    )
    .unwrap();
    db.close().unwrap();
    (tmp, path)
}

fn insert_file_autocommit_rows(path: &Path) {
    let db = Database::open(path).unwrap();
    let payload = "x".repeat(1024);
    for row in 0..FILE_AUTOCOMMIT_ROWS {
        db.execute(
            "INSERT INTO t (id, seq, payload) VALUES ($id, $seq, $payload)",
            &params(vec![
                ("id", Value::Uuid(uuid(3_000_000 + row))),
                ("seq", Value::Int64(row as i64)),
                ("payload", Value::Text(payload.clone())),
            ]),
        )
        .unwrap();
    }
    let count = db.execute("SELECT COUNT(*) FROM t", &empty()).unwrap();
    assert_eq!(
        count.rows,
        vec![vec![Value::Int64(FILE_AUTOCOMMIT_ROWS as i64)]]
    );
}

fn bench_file_backed_single_row_autocommit(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_scale_file_autocommit");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(300));
    group.bench_function("single_row_100_writes", |b| {
        b.iter_batched(
            setup_file_autocommit_table,
            |(_tmp, path)| insert_file_autocommit_rows(&path),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets =
        bench_insert_cost_by_index_count,
        bench_reopen_cost_by_row_count,
        bench_wide_batch_size_comparator,
        bench_file_backed_single_row_autocommit
);
criterion_main!(benches);
