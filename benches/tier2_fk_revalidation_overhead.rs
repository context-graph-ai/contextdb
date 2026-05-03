//! benches/tier2_fk_revalidation_overhead.rs

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use contextdb_core::{RowId, TxId, Value};
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use uuid::Uuid;

const FULL_PRE_GATE_ROWS: usize = 100_000;
const QUICK_PRE_GATE_ROWS: usize = 10_000;
const SEED_BATCH_ROWS: usize = 128;
const TIMED_COMMITS: usize = 1_000;
const MIN_FK_COMMITS_PER_SEC: f64 = 750.0;
const MAX_COMMIT_P95: Duration = Duration::from_millis(15);

#[derive(Debug)]
struct FkReport {
    elapsed: Duration,
    commit_p95: Duration,
    rows_per_sec: f64,
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

fn p95(mut latencies: Vec<Duration>) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }
    latencies.sort_unstable();
    let idx = (latencies.len() * 95).div_ceil(100).saturating_sub(1);
    latencies[idx]
}

fn declare_schema(db: &Database) {
    db.execute("CREATE TABLE decisions (id UUID PRIMARY KEY)", &empty())
        .expect("create parent");
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, decision_id UUID NOT NULL REFERENCES decisions(id))",
        &empty(),
    )
    .expect("create child");
    db.execute(
        "CREATE INDEX invalidations_decision_id_idx ON invalidations (decision_id)",
        &empty(),
    )
    .expect("index child FK column");
}

fn insert_parent(db: &Database, tx: TxId, id: Uuid) -> RowId {
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("stage parent")
}

fn insert_child(db: &Database, tx: TxId, id: Uuid, parent: Uuid) {
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("decision_id".to_string(), Value::Uuid(parent)),
        ]),
    )
    .expect("stage child");
}

fn setup_seeded_db() -> (Database, Vec<Uuid>) {
    let db = Database::open_memory();
    declare_schema(&db);
    let pre_gate_rows = pre_gate_rows();
    let mut parents = Vec::with_capacity(pre_gate_rows);
    let mut seq = 0_usize;
    while seq < pre_gate_rows {
        let tx = db.begin();
        let end = (seq + SEED_BATCH_ROWS).min(pre_gate_rows);
        while seq < end {
            let parent = Uuid::new_v4();
            parents.push(parent);
            insert_parent(&db, tx, parent);
            insert_child(&db, tx, Uuid::new_v4(), parent);
            seq += 1;
        }
        db.commit(tx).expect("commit FK seed batch");
    }
    assert_eq!(
        db.execute("SELECT id FROM decisions", &empty())
            .expect("count parents")
            .rows
            .len(),
        pre_gate_rows
    );
    assert_eq!(
        db.execute("SELECT id FROM invalidations", &empty())
            .expect("count children")
            .rows
            .len(),
        pre_gate_rows
    );
    (db, parents)
}

fn run_fk_commits(db: &Database, parents: &[Uuid]) -> FkReport {
    let mut commit_latencies = Vec::with_capacity(TIMED_COMMITS);
    let started = Instant::now();
    for offset in 0..TIMED_COMMITS {
        if offset.is_multiple_of(2) {
            let parent = parents[offset % parents.len()];
            let tx = db.begin();
            insert_child(db, tx, Uuid::new_v4(), parent);
            let commit_started = Instant::now();
            db.commit(tx).expect("commit child insert with live parent");
            commit_latencies.push(commit_started.elapsed());
        } else {
            let parent = Uuid::new_v4();
            let setup_tx = db.begin();
            let parent_row_id = insert_parent(db, setup_tx, parent);
            db.commit(setup_tx).expect("commit childless parent setup");

            let tx = db.begin();
            db.delete_row(tx, "decisions", parent_row_id)
                .expect("stage childless parent delete");
            let commit_started = Instant::now();
            db.commit(tx).expect("commit childless parent delete");
            commit_latencies.push(commit_started.elapsed());
        }
    }
    let elapsed = started.elapsed();
    FkReport {
        elapsed,
        commit_p95: p95(commit_latencies),
        rows_per_sec: TIMED_COMMITS as f64 / elapsed.as_secs_f64(),
    }
}

fn assert_report(report: &FkReport) {
    assert!(
        report.rows_per_sec >= MIN_FK_COMMITS_PER_SEC,
        "commit-time FK re-validation throughput too low: {:.0} commits/sec; report={report:?}",
        report.rows_per_sec
    );
    assert!(
        report.commit_p95 <= MAX_COMMIT_P95,
        "commit-time FK re-validation p95 {:?} exceeded {:?}; report={report:?}",
        report.commit_p95,
        MAX_COMMIT_P95
    );
    black_box(report.elapsed);
}

fn bench(c: &mut Criterion) {
    let (db, parents) = setup_seeded_db();
    let pre_gate = run_fk_commits(&db, &parents);
    assert_report(&pre_gate);

    let mut group = c.benchmark_group("tier2_fk_revalidation_overhead");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    group.bench_function("100k_seeded_1k_mixed_fk_commits", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let report = run_fk_commits(&db, &parents);
                assert_report(&report);
                elapsed += report.elapsed;
            }
            elapsed
        });
    });
    group.finish();

    let post_gate = run_fk_commits(&db, &parents);
    assert_report(&post_gate);
}

criterion_group!(benches, bench);
criterion_main!(benches);
