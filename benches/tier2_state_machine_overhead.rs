//! benches/tier2_state_machine_overhead.rs

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use contextdb_core::{Error, TxId, Value};
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use uuid::Uuid;

const FULL_PRE_GATE_ROWS: usize = 100_000;
const QUICK_PRE_GATE_ROWS: usize = 10_000;
const SEED_BATCH_ROWS: usize = 128;
const TIMED_UPDATES: usize = 1_000;
const MIN_STATE_UPDATES_PER_SEC: f64 = 150.0;
const MAX_UPDATE_P95: Duration = Duration::from_millis(15);

#[derive(Debug)]
struct StateReport {
    elapsed: Duration,
    update_p95: Duration,
    rows_per_sec: f64,
}

struct StateCursor {
    accepted_self_edge_cursor: usize,
    accepted_distinct_cursor: usize,
    accepted_state_is_running: Vec<bool>,
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn pre_gate_rows() -> usize {
    if std::env::args().any(|arg| arg == "--test") {
        QUICK_PRE_GATE_ROWS
    } else {
        FULL_PRE_GATE_ROWS
    }
}

fn params(items: Vec<(&str, Value)>) -> HashMap<String, Value> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn p95(mut latencies: Vec<Duration>) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }
    latencies.sort_unstable();
    let idx = (latencies.len() * 95).div_ceil(100).saturating_sub(1);
    latencies[idx]
}

fn declare_schema(db: &Database) {
    db.execute(
        "CREATE TABLE jobs (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'queued') STATE MACHINE (status: queued -> [queued, running], running -> [running, queued, done])",
        &empty(),
    )
    .expect("create state-machine table");
    db.execute(
        "CREATE TABLE strict_jobs (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'queued') STATE MACHINE (status: queued -> [running], running -> [done])",
        &empty(),
    )
    .expect("create strict state-machine table");
}

fn insert_job(db: &Database, tx: TxId, table: &str, id: Uuid) {
    db.insert_row(
        tx,
        table,
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("status".to_string(), Value::Text("queued".to_string())),
        ]),
    )
    .expect("stage job");
}

fn setup_seeded_db() -> (Database, Vec<Uuid>, Uuid) {
    let db = Database::open_memory();
    declare_schema(&db);
    let pre_gate_rows = pre_gate_rows();
    let mut ids = Vec::with_capacity(pre_gate_rows);
    let mut seq = 0_usize;
    while seq < pre_gate_rows {
        let tx = db.begin();
        let end = (seq + SEED_BATCH_ROWS).min(pre_gate_rows);
        while seq < end {
            let id = Uuid::new_v4();
            ids.push(id);
            insert_job(&db, tx, "jobs", id);
            seq += 1;
        }
        db.commit(tx).expect("commit state-machine seed batch");
    }
    let strict_id = Uuid::new_v4();
    let strict_tx = db.begin();
    insert_job(&db, strict_tx, "strict_jobs", strict_id);
    db.commit(strict_tx).expect("commit strict job");
    assert_eq!(
        db.execute("SELECT id FROM jobs", &empty())
            .expect("count seeded jobs")
            .rows
            .len(),
        pre_gate_rows
    );
    (db, ids, strict_id)
}

fn pre_gate_state_machine_semantics(db: &Database, strict_id: Uuid) {
    let err = db
        .execute(
            "UPDATE strict_jobs SET status = 'queued' WHERE id = $id",
            &params(vec![("id", Value::Uuid(strict_id))]),
        )
        .expect_err("undeclared self-edge must reject");
    assert!(
        matches!(err, Error::InvalidStateTransition(_)),
        "undeclared self-edge must return InvalidStateTransition, got {err:?}"
    );
}

fn run_state_updates(
    db: &Database,
    ids: &[Uuid],
    strict_id: Uuid,
    cursor: &mut StateCursor,
) -> StateReport {
    let mut latencies = Vec::with_capacity(TIMED_UPDATES);
    let started = Instant::now();
    for step in 0..TIMED_UPDATES {
        let update_started = Instant::now();
        match step % 4 {
            0 => {
                let idx = cursor.accepted_self_edge_cursor % ids.len();
                cursor.accepted_self_edge_cursor =
                    cursor.accepted_self_edge_cursor.saturating_add(1);
                let status = if cursor.accepted_state_is_running[idx] {
                    "running"
                } else {
                    "queued"
                };
                let result = db
                    .execute(
                        "UPDATE jobs SET status = $status WHERE id = $id AND status = $status",
                        &params(vec![
                            ("id", Value::Uuid(ids[idx])),
                            ("status", Value::Text(status.to_string())),
                        ]),
                    )
                    .expect("declared self-edge update");
                assert_eq!(result.rows_affected, 1);
            }
            1 => {
                let idx = cursor.accepted_distinct_cursor % ids.len();
                cursor.accepted_distinct_cursor = cursor.accepted_distinct_cursor.saturating_add(1);
                let from = if cursor.accepted_state_is_running[idx] {
                    "running"
                } else {
                    "queued"
                };
                let to = if cursor.accepted_state_is_running[idx] {
                    "queued"
                } else {
                    "running"
                };
                let result = db
                    .execute(
                        "UPDATE jobs SET status = $to WHERE id = $id AND status = $from",
                        &params(vec![
                            ("id", Value::Uuid(ids[idx])),
                            ("from", Value::Text(from.to_string())),
                            ("to", Value::Text(to.to_string())),
                        ]),
                    )
                    .expect("declared distinct transition update");
                assert_eq!(result.rows_affected, 1);
                cursor.accepted_state_is_running[idx] = !cursor.accepted_state_is_running[idx];
            }
            2 => {
                let err = db
                    .execute(
                        "UPDATE strict_jobs SET status = 'queued' WHERE id = $id",
                        &params(vec![("id", Value::Uuid(strict_id))]),
                    )
                    .expect_err("undeclared self-edge must reject");
                assert!(
                    matches!(err, Error::InvalidStateTransition(_)),
                    "undeclared self-edge must return InvalidStateTransition, got {err:?}"
                );
            }
            _ => {
                let err = db
                    .execute(
                        "UPDATE strict_jobs SET status = 'done' WHERE id = $id",
                        &params(vec![("id", Value::Uuid(strict_id))]),
                    )
                    .expect_err("undeclared distinct transition must reject");
                assert!(
                    matches!(err, Error::InvalidStateTransition(_)),
                    "undeclared distinct transition must return InvalidStateTransition, got {err:?}"
                );
            }
        }
        latencies.push(update_started.elapsed());
    }
    let elapsed = started.elapsed();
    StateReport {
        elapsed,
        update_p95: p95(latencies),
        rows_per_sec: TIMED_UPDATES as f64 / elapsed.as_secs_f64(),
    }
}

fn assert_report(report: &StateReport) {
    assert!(
        report.rows_per_sec >= MIN_STATE_UPDATES_PER_SEC,
        "state-machine UPDATE throughput too low: {:.0} updates/sec; report={report:?}",
        report.rows_per_sec
    );
    assert!(
        report.update_p95 <= MAX_UPDATE_P95,
        "state-machine UPDATE p95 {:?} exceeded {:?}; report={report:?}",
        report.update_p95,
        MAX_UPDATE_P95
    );
    black_box(report.elapsed);
}

fn bench(c: &mut Criterion) {
    let (db, ids, strict_id) = setup_seeded_db();
    pre_gate_state_machine_semantics(&db, strict_id);
    let mut cursor = StateCursor {
        accepted_self_edge_cursor: 0,
        accepted_distinct_cursor: 0,
        accepted_state_is_running: vec![false; ids.len()],
    };
    let pre_gate = run_state_updates(&db, &ids, strict_id, &mut cursor);
    assert_report(&pre_gate);

    let mut group = c.benchmark_group("tier2_state_machine_overhead");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    group.bench_function("100k_seeded_1k_mixed_state_machine_updates", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let report = run_state_updates(&db, &ids, strict_id, &mut cursor);
                assert_report(&report);
                elapsed += report.elapsed;
            }
            elapsed
        });
    });
    group.finish();

    let post_gate = run_state_updates(&db, &ids, strict_id, &mut cursor);
    assert_report(&post_gate);
}

criterion_group!(benches, bench);
criterion_main!(benches);
