//! benches/tier2_commit_validate_overhead.rs

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use uuid::Uuid;

const FULL_PRE_GATE_ROWS: u64 = 100_000;
const QUICK_PRE_GATE_ROWS: u64 = 10_000;
const SEED_BATCH_ROWS: u64 = 128;
const TIMED_COMMITS: u64 = 1_000;
const MIN_DISTINCT_COMMITS_PER_SEC: f64 = 1_000.0;
const MAX_COMMIT_P95: Duration = Duration::from_millis(10);

#[derive(Debug)]
struct CommitReport {
    elapsed: Duration,
    commit_p95: Duration,
    rows_per_sec: f64,
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn pre_gate_rows() -> u64 {
    if std::env::args().any(|arg| arg == "--test") {
        QUICK_PRE_GATE_ROWS
    } else {
        FULL_PRE_GATE_ROWS
    }
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
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .expect("create unique table");
}

fn insert_entity(db: &Database, tx: contextdb_core::TxId, seq: u64) {
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("name".to_string(), Value::Text(format!("entity-{seq}"))),
        ]),
    )
    .expect("stage entity");
}

fn setup_seeded_db() -> (Database, u64) {
    let db = Database::open_memory();
    declare_schema(&db);
    let pre_gate_rows = pre_gate_rows();
    let mut seq = 0_u64;
    while seq < pre_gate_rows {
        let tx = db.begin();
        let end = (seq + SEED_BATCH_ROWS).min(pre_gate_rows);
        while seq < end {
            insert_entity(&db, tx, seq);
            seq += 1;
        }
        db.commit(tx).expect("commit seed batch");
    }
    let count = db
        .execute("SELECT id FROM entities", &empty())
        .expect("count seeded rows")
        .rows
        .len();
    assert_eq!(count as u64, pre_gate_rows);
    (db, pre_gate_rows)
}

fn run_distinct_unique_commits(db: &Database, next_seq: &mut u64, commits: u64) -> CommitReport {
    let mut commit_latencies = Vec::with_capacity(commits as usize);
    let started = Instant::now();
    for _ in 0..commits {
        let seq = *next_seq;
        *next_seq = (*next_seq).saturating_add(1);
        let tx = db.begin();
        insert_entity(db, tx, seq);
        let commit_started = Instant::now();
        db.commit(tx).expect("commit distinct unique insert");
        commit_latencies.push(commit_started.elapsed());
    }
    let elapsed = started.elapsed();
    CommitReport {
        elapsed,
        commit_p95: p95(commit_latencies),
        rows_per_sec: commits as f64 / elapsed.as_secs_f64(),
    }
}

fn assert_report(report: &CommitReport) {
    assert!(
        report.rows_per_sec >= MIN_DISTINCT_COMMITS_PER_SEC,
        "commit-time UNIQUE re-validation throughput too low: {:.0} commits/sec; report={report:?}",
        report.rows_per_sec
    );
    assert!(
        report.commit_p95 <= MAX_COMMIT_P95,
        "commit-time UNIQUE re-validation p95 {:?} exceeded {:?}; report={report:?}",
        report.commit_p95,
        MAX_COMMIT_P95
    );
    black_box(report.elapsed);
}

fn bench(c: &mut Criterion) {
    let (db, seeded_rows) = setup_seeded_db();
    let mut next_seq = seeded_rows;
    let pre_gate = run_distinct_unique_commits(&db, &mut next_seq, 128);
    assert_report(&pre_gate);

    let mut group = c.benchmark_group("tier2_commit_validate_overhead");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    group.bench_function("100k_seeded_1k_distinct_unique_commits", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let report = run_distinct_unique_commits(&db, &mut next_seq, TIMED_COMMITS);
                assert_report(&report);
                elapsed += report.elapsed;
            }
            elapsed
        });
    });
    group.finish();

    let post_gate = run_distinct_unique_commits(&db, &mut next_seq, TIMED_COMMITS);
    assert_report(&post_gate);
}

criterion_group!(benches, bench);
criterion_main!(benches);
