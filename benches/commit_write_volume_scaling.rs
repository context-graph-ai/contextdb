//! benches/commit_write_volume_scaling.rs

use std::array;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use contextdb_core::{TxId, Value};
use contextdb_engine::{CommitStageStats, Database};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use uuid::Uuid;

const SAMPLE_SIZE: usize = 10;
const WRITE_VOLUME_N: [usize; 4] = [10, 30, 100, 1000];
const HISTORY_N: usize = 30;
const TRIGGER_N: usize = 100;
const FILE_BACKED_N: usize = 100;
const HISTORY_SMALL: usize = 1_000;
const HISTORY_LARGE: usize = 10_000;
const HISTORY_SOAK: usize = 100_000;
const SEED_BATCH_ROWS: usize = 250;
const WRITE_FLATNESS_BUDGET: f64 = 1.5;
const FILE_BACKED_PARITY_BUDGET: f64 = 1.75;
const HISTORY_FLATNESS_BUDGET: f64 = 1.5;
const TRIGGER_PARITY_BUDGET: f64 = 1.5;

#[derive(Debug, Clone)]
struct CommitMeasurement {
    elapsed: Duration,
    stats: CommitStageStats,
}

#[derive(Debug)]
struct BudgetReport {
    min_plain_per_n: f64,
    plain_n100: Duration,
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(items: Vec<(&str, Value)>) -> HashMap<String, Value> {
    items.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn uuid(seed: u128) -> Uuid {
    Uuid::from_u128(0xC0DE_0000_0000_0000_0000_0000_0000_0000 + seed)
}

fn declare_mixed_schema(db: &Database) {
    db.execute(
        "CREATE TABLE parent (a UUID NOT NULL, b INTEGER NOT NULL, \
         status TEXT NOT NULL DEFAULT 'active', UNIQUE (a, b)) \
         STATE MACHINE (status: active -> [invalidated, superseded])",
        &empty(),
    )
    .expect("create parent");
    db.execute(
        "CREATE TABLE child (id UUID PRIMARY KEY, c1 UUID NOT NULL, c2 INTEGER NOT NULL, \
         tag TEXT NOT NULL, UNIQUE (c1, c2, tag), FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
        &empty(),
    )
    .expect("create child");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL, \
         UNIQUE (source_id, target_id, edge_type))",
        &empty(),
    )
    .expect("create edges");
}

fn open_case(file_backed: bool) -> (Option<TempDir>, Database) {
    if file_backed {
        let tmp = TempDir::new().unwrap();
        let db = Database::open(tmp.path().join("context.redb")).unwrap();
        (Some(tmp), db)
    } else {
        (None, Database::open_memory())
    }
}

fn insert_parent(db: &Database, tx: TxId, a: Uuid, b: i64) {
    db.insert_row(
        tx,
        "parent",
        HashMap::from([
            ("a".to_string(), Value::Uuid(a)),
            ("b".to_string(), Value::Int64(b)),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("stage parent");
}

fn seed_parents(db: &Database, n: usize, b_offset: i64, uuid_offset: u128) -> Vec<(Uuid, i64)> {
    let mut parents = Vec::with_capacity(n);
    let mut start = 0;
    while start < n {
        let tx = db.begin_or_panic();
        let end = (start + SEED_BATCH_ROWS).min(n);
        for i in start..end {
            let a = uuid(uuid_offset + i as u128);
            let b = b_offset + i as i64;
            insert_parent(db, tx, a, b);
            parents.push((a, b));
        }
        db.commit(tx).expect("commit parent seed batch");
        start = end;
    }
    parents
}

fn seed_history(db: &Database, rows: usize) {
    let mut start = 0;
    while start < rows {
        let tx = db.begin_or_panic();
        let end = (start + SEED_BATCH_ROWS).min(rows);
        for i in start..end {
            let a = uuid(10_000_000 + i as u128);
            let b = 1_000_000 + i as i64;
            insert_parent(db, tx, a, b);
            db.insert_row(
                tx,
                "child",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(20_000_000 + i as u128))),
                    ("c1".to_string(), Value::Uuid(a)),
                    ("c2".to_string(), Value::Int64(b)),
                    ("tag".to_string(), Value::Text(format!("hist-{i}"))),
                ]),
            )
            .expect("stage history child");
            db.insert_row(
                tx,
                "edges",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(30_000_000 + i as u128))),
                    (
                        "source_id".to_string(),
                        Value::Uuid(uuid(40_000_000 + i as u128)),
                    ),
                    (
                        "target_id".to_string(),
                        Value::Uuid(uuid(50_000_000 + i as u128)),
                    ),
                    ("edge_type".to_string(), Value::Text(format!("hist-{i}"))),
                ]),
            )
            .expect("stage history edge");
        }
        db.commit(tx).expect("commit history seed batch");
        start = end;
    }
}

fn stage_mixed_cascade_tx(db: &Database, parents: &[(Uuid, i64)]) -> TxId {
    let tx = db.begin_or_panic();
    for (i, (a, b)) in parents.iter().enumerate() {
        db.execute_in_tx(
            tx,
            "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b",
            &params(vec![("a", Value::Uuid(*a)), ("b", Value::Int64(*b))]),
        )
        .expect("stage parent update");
        db.insert_row(
            tx,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(60_000_000 + i as u128))),
                ("c1".to_string(), Value::Uuid(*a)),
                ("c2".to_string(), Value::Int64(*b)),
                ("tag".to_string(), Value::Text(format!("tag-{b}"))),
            ]),
        )
        .expect("stage child");
        db.insert_row(
            tx,
            "edges",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(70_000_000 + i as u128))),
                ("source_id".to_string(), Value::Uuid(*a)),
                (
                    "target_id".to_string(),
                    Value::Uuid(uuid(80_000_000 + i as u128)),
                ),
                ("edge_type".to_string(), Value::Text("cascades".to_string())),
            ]),
        )
        .expect("stage edge");
    }
    tx
}

fn stage_upsert_cascade_tx(db: &Database, parents: &[(Uuid, i64)], upsert_hits: usize) -> TxId {
    let tx = db.begin_or_panic();
    for (i, (a, b)) in parents.iter().enumerate() {
        db.execute_in_tx(
            tx,
            "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b",
            &params(vec![("a", Value::Uuid(*a)), ("b", Value::Int64(*b))]),
        )
        .expect("stage parent update");
        if i < upsert_hits {
            db.execute_in_tx(
                tx,
                "INSERT INTO child (id, c1, c2, tag) VALUES ($id, $c1, $c2, $tag) \
                 ON CONFLICT (c1, c2, tag) DO UPDATE SET tag = $new_tag",
                &params(vec![
                    ("id", Value::Uuid(uuid(140_000_000 + i as u128))),
                    ("c1", Value::Uuid(*a)),
                    ("c2", Value::Int64(*b)),
                    ("tag", Value::Text(format!("tag-{b}"))),
                    ("new_tag", Value::Text(format!("updated-{b}"))),
                ]),
            )
            .expect("stage upsert hit");
        } else {
            db.insert_row(
                tx,
                "child",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(170_000_000 + i as u128))),
                    ("c1".to_string(), Value::Uuid(*a)),
                    ("c2".to_string(), Value::Int64(*b)),
                    ("tag".to_string(), Value::Text(format!("tag-{b}"))),
                ]),
            )
            .expect("stage child");
        }
        db.insert_row(
            tx,
            "edges",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(150_000_000 + i as u128))),
                ("source_id".to_string(), Value::Uuid(*a)),
                (
                    "target_id".to_string(),
                    Value::Uuid(uuid(160_000_000 + i as u128)),
                ),
                ("edge_type".to_string(), Value::Text("cascades".to_string())),
            ]),
        )
        .expect("stage edge");
    }
    tx
}

fn assert_mixed_landed(db: &Database, n: usize, history_rows: usize) {
    let invalidated = db
        .execute(
            "SELECT a FROM parent WHERE status = 'invalidated'",
            &empty(),
        )
        .expect("select invalidated parents");
    assert_eq!(invalidated.rows.len(), n);
    assert_eq!(
        db.execute("SELECT id FROM child", &empty())
            .expect("select children")
            .rows
            .len(),
        history_rows + n
    );
    assert_eq!(
        db.execute("SELECT id FROM edges", &empty())
            .expect("select edges")
            .rows
            .len(),
        history_rows + n
    );
}

fn assert_upsert_landed(db: &Database, n: usize, upsert_hits: usize) {
    assert_mixed_landed(db, n, 0);
    let rows = db
        .execute("SELECT tag FROM child", &empty())
        .expect("select child tags");
    let updated = rows
        .rows
        .iter()
        .filter(|row| matches!(&row[0], Value::Text(tag) if tag.starts_with("updated-")))
        .count();
    assert_eq!(updated, upsert_hits);
}

fn stats_delta(before: CommitStageStats, after: CommitStageStats) -> CommitStageStats {
    CommitStageStats {
        rows_validated: after.rows_validated.saturating_sub(before.rows_validated),
        indexed_probes: after.indexed_probes.saturating_sub(before.indexed_probes),
        staged_vs_staged_comparisons: after
            .staged_vs_staged_comparisons
            .saturating_sub(before.staged_vs_staged_comparisons),
        scan_rows_touched: after
            .scan_rows_touched
            .saturating_sub(before.scan_rows_touched),
        index_maintenance_visits: after
            .index_maintenance_visits
            .saturating_sub(before.index_maintenance_visits),
        stage_wall_nanos: match (before.stage_wall_nanos, after.stage_wall_nanos) {
            (Some(before), Some(after)) => {
                Some(array::from_fn(|i| after[i].saturating_sub(before[i])))
            }
            _ => None,
        },
    }
}

fn print_measurement(label: &str, measurement: &CommitMeasurement) {
    println!(
        "{label}: elapsed={:?} stats={:?}",
        measurement.elapsed, measurement.stats
    );
    if let Some(stage_wall_nanos) = measurement.stats.stage_wall_nanos {
        println!("{label}: stage_wall_nanos={stage_wall_nanos:?}");
    }
}

fn measure_plain_cascade(n: usize, history_rows: usize, file_backed: bool) -> CommitMeasurement {
    let (_tmp, db) = open_case(file_backed);
    declare_mixed_schema(&db);
    seed_history(&db, history_rows);
    let parents = seed_parents(&db, n, 0, 100_000);
    db.__reset_commit_stage_stats();
    let before = db.__commit_stage_stats();
    let tx = stage_mixed_cascade_tx(&db, &parents);
    let started = Instant::now();
    db.commit(tx).expect("commit measured cascade");
    let elapsed = started.elapsed();
    let after = db.__commit_stage_stats();
    assert_mixed_landed(&db, n, history_rows);
    CommitMeasurement {
        elapsed,
        stats: stats_delta(before, after),
    }
}

fn measure_upsert_cascade(n: usize) -> CommitMeasurement {
    let (_tmp, db) = open_case(false);
    declare_mixed_schema(&db);
    let parents = seed_parents(&db, n, 0, 130_000);
    let upsert_hits = n / 3;
    if upsert_hits > 0 {
        let pre = db.begin_or_panic();
        for (i, (a, b)) in parents.iter().take(upsert_hits).enumerate() {
            db.insert_row(
                pre,
                "child",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(130_000_000 + i as u128))),
                    ("c1".to_string(), Value::Uuid(*a)),
                    ("c2".to_string(), Value::Int64(*b)),
                    ("tag".to_string(), Value::Text(format!("tag-{b}"))),
                ]),
            )
            .expect("stage pre-existing child");
        }
        db.commit(pre).expect("commit pre-existing children");
    }

    db.__reset_commit_stage_stats();
    let before = db.__commit_stage_stats();
    let tx = stage_upsert_cascade_tx(&db, &parents, upsert_hits);
    let started = Instant::now();
    db.commit(tx).expect("commit upsert cascade");
    let elapsed = started.elapsed();
    let after = db.__commit_stage_stats();
    assert_upsert_landed(&db, n, upsert_hits);
    CommitMeasurement {
        elapsed,
        stats: stats_delta(before, after),
    }
}

fn setup_trigger_case(n: usize) -> (Database, Vec<(Uuid, i64)>) {
    let db = Database::open_memory();
    declare_mixed_schema(&db);
    db.execute("CREATE TABLE fire (id UUID PRIMARY KEY)", &empty())
        .expect("create fire table");
    db.execute("CREATE TRIGGER tr ON fire WHEN INSERT", &empty())
        .expect("declare trigger");
    let parents = seed_parents(&db, n, 0, 200_000);
    let callback_parents = parents.clone();
    db.register_trigger_callback("tr", move |db_handle, ctx| {
        for (i, (a, b)) in callback_parents.iter().enumerate() {
            db_handle.execute_in_tx(
                ctx.tx,
                "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b",
                &params(vec![("a", Value::Uuid(*a)), ("b", Value::Int64(*b))]),
            )?;
            db_handle.insert_row(
                ctx.tx,
                "child",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(90_000_000 + i as u128))),
                    ("c1".to_string(), Value::Uuid(*a)),
                    ("c2".to_string(), Value::Int64(*b)),
                    ("tag".to_string(), Value::Text(format!("tag-{b}"))),
                ]),
            )?;
            db_handle.insert_row(
                ctx.tx,
                "edges",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(100_000_000 + i as u128))),
                    ("source_id".to_string(), Value::Uuid(*a)),
                    (
                        "target_id".to_string(),
                        Value::Uuid(uuid(110_000_000 + i as u128)),
                    ),
                    ("edge_type".to_string(), Value::Text("cascades".to_string())),
                ]),
            )?;
        }
        Ok(())
    })
    .expect("register trigger callback");
    db.complete_initialization()
        .expect("complete trigger initialization");
    (db, parents)
}

fn measure_trigger_cascade(n: usize) -> CommitMeasurement {
    let (db, _parents) = setup_trigger_case(n);
    db.__reset_commit_stage_stats();
    let before = db.__commit_stage_stats();
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "INSERT INTO fire (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(uuid(120_000_000)))]),
    )
    .expect("stage trigger fire row");
    let started = Instant::now();
    db.commit(tx).expect("commit trigger cascade");
    let elapsed = started.elapsed();
    let after = db.__commit_stage_stats();
    assert_mixed_landed(&db, n, 0);
    CommitMeasurement {
        elapsed,
        stats: stats_delta(before, after),
    }
}

fn median_duration(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn duration_per_n(duration: Duration, n: usize) -> f64 {
    duration.as_secs_f64() / n as f64
}

fn sample_median_plain(label: &str, n: usize, history_rows: usize, file_backed: bool) -> Duration {
    let mut samples = Vec::with_capacity(SAMPLE_SIZE);
    for sample in 0..SAMPLE_SIZE {
        let measurement = measure_plain_cascade(n, history_rows, file_backed);
        print_measurement(&format!("{label}/sample-{sample}"), &measurement);
        samples.push(measurement.elapsed);
    }
    median_duration(samples)
}

fn sample_median_trigger(label: &str, n: usize) -> Duration {
    let mut samples = Vec::with_capacity(SAMPLE_SIZE);
    for sample in 0..SAMPLE_SIZE {
        let measurement = measure_trigger_cascade(n);
        print_measurement(&format!("{label}/sample-{sample}"), &measurement);
        samples.push(measurement.elapsed);
    }
    median_duration(samples)
}

fn sample_median_upsert(label: &str, n: usize) -> Duration {
    let mut samples = Vec::with_capacity(SAMPLE_SIZE);
    for sample in 0..SAMPLE_SIZE {
        let measurement = measure_upsert_cascade(n);
        print_measurement(&format!("{label}/sample-{sample}"), &measurement);
        samples.push(measurement.elapsed);
    }
    median_duration(samples)
}

fn run_budget_gates() -> BudgetReport {
    let mut plain_medians = Vec::with_capacity(WRITE_VOLUME_N.len());
    for n in WRITE_VOLUME_N {
        let median = sample_median_plain(&format!("plain-n-{n}"), n, 0, false);
        plain_medians.push((n, median));
    }

    let min_plain_per_n = plain_medians
        .iter()
        .map(|(n, median)| duration_per_n(*median, *n))
        .fold(f64::INFINITY, f64::min);
    for (n, median) in &plain_medians {
        let per_n = duration_per_n(*median, *n);
        assert!(
            per_n <= WRITE_FLATNESS_BUDGET * min_plain_per_n,
            "write-volume flatness budget failed at N={n}: per-N {per_n:.9}s > {:.9}s",
            WRITE_FLATNESS_BUDGET * min_plain_per_n
        );
    }

    let mut upsert_medians = Vec::with_capacity(WRITE_VOLUME_N.len());
    for n in WRITE_VOLUME_N {
        let median = sample_median_upsert(&format!("upsert-n-{n}"), n);
        upsert_medians.push((n, median));
    }
    let min_upsert_per_n = upsert_medians
        .iter()
        .map(|(n, median)| duration_per_n(*median, *n))
        .fold(f64::INFINITY, f64::min);
    for (n, median) in &upsert_medians {
        let per_n = duration_per_n(*median, *n);
        assert!(
            per_n <= WRITE_FLATNESS_BUDGET * min_upsert_per_n,
            "upsert flatness budget failed at N={n}: per-N {per_n:.9}s > {:.9}s",
            WRITE_FLATNESS_BUDGET * min_upsert_per_n
        );
    }

    let plain_n100 = plain_medians
        .iter()
        .find(|(n, _)| *n == TRIGGER_N)
        .map(|(_, median)| *median)
        .expect("N=100 median present");
    let trigger_n100 = sample_median_trigger("trigger-n-100", TRIGGER_N);
    assert!(
        trigger_n100.as_secs_f64() <= TRIGGER_PARITY_BUDGET * plain_n100.as_secs_f64(),
        "trigger parity budget failed: trigger {:?} > {:.2}x plain {:?}",
        trigger_n100,
        TRIGGER_PARITY_BUDGET,
        plain_n100
    );

    let history_small = sample_median_plain("history-1k", HISTORY_N, HISTORY_SMALL, false);
    let history_large = sample_median_plain("history-10k", HISTORY_N, HISTORY_LARGE, false);
    assert!(
        history_large.as_secs_f64() <= HISTORY_FLATNESS_BUDGET * history_small.as_secs_f64(),
        "history flatness budget failed: H=10K {:?} > {:.2}x H=1K {:?}",
        history_large,
        HISTORY_FLATNESS_BUDGET,
        history_small
    );

    if std::env::var("CONTEXTDB_CWV_SOAK_100K").ok().as_deref() == Some("1") {
        let history_soak = sample_median_plain("history-100k-soak", HISTORY_N, HISTORY_SOAK, false);
        assert!(
            history_soak.as_secs_f64() <= HISTORY_FLATNESS_BUDGET * history_small.as_secs_f64(),
            "one-time soak arm failed: H=100K {:?} > {:.2}x H=1K {:?}",
            history_soak,
            HISTORY_FLATNESS_BUDGET,
            history_small
        );
    }

    let file_n100 = sample_median_plain("file-backed-n-100", FILE_BACKED_N, 0, true);
    let file_per_n = duration_per_n(file_n100, FILE_BACKED_N);
    let plain_file_n_per_n = duration_per_n(plain_n100, FILE_BACKED_N);
    assert!(
        file_per_n <= FILE_BACKED_PARITY_BUDGET * plain_file_n_per_n,
        "file-backed flatness budget failed: per-N {file_per_n:.9}s > {:.9}s",
        FILE_BACKED_PARITY_BUDGET * plain_file_n_per_n
    );

    BudgetReport {
        min_plain_per_n,
        plain_n100,
    }
}

fn bench(c: &mut Criterion) {
    let budget = run_budget_gates();
    black_box(&budget);

    let mut group = c.benchmark_group("commit_write_volume_scaling/write_volume");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    for n in WRITE_VOLUME_N {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let measurement = measure_plain_cascade(n, 0, false);
                    print_measurement(&format!("criterion-plain-n-{n}"), &measurement);
                    elapsed += measurement.elapsed;
                }
                elapsed
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("commit_write_volume_scaling/upsert");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    for n in WRITE_VOLUME_N {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let measurement = measure_upsert_cascade(n);
                    print_measurement(&format!("criterion-upsert-n-{n}"), &measurement);
                    elapsed += measurement.elapsed;
                }
                elapsed
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("commit_write_volume_scaling/history");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    for history_rows in [HISTORY_SMALL, HISTORY_LARGE] {
        group.bench_with_input(
            BenchmarkId::from_parameter(history_rows),
            &history_rows,
            |b, &history_rows| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let measurement = measure_plain_cascade(HISTORY_N, history_rows, false);
                        print_measurement(
                            &format!("criterion-history-{history_rows}"),
                            &measurement,
                        );
                        elapsed += measurement.elapsed;
                    }
                    elapsed
                });
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("commit_write_volume_scaling/trigger");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    group.bench_function("trigger_staged_n_100", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let measurement = measure_trigger_cascade(TRIGGER_N);
                print_measurement("criterion-trigger-n-100", &measurement);
                elapsed += measurement.elapsed;
            }
            elapsed
        });
    });
    group.finish();

    let mut group = c.benchmark_group("commit_write_volume_scaling/file_backed");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    group.bench_function("file_backed_n_100", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let measurement = measure_plain_cascade(FILE_BACKED_N, 0, true);
                print_measurement("criterion-file-backed-n-100", &measurement);
                elapsed += measurement.elapsed;
            }
            elapsed
        });
    });
    group.finish();

    assert!(budget.min_plain_per_n.is_finite());
    assert!(budget.plain_n100 > Duration::ZERO);
}

criterion_group!(benches, bench);
criterion_main!(benches);
