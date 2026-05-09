//! §30 — Trigger-throughput Criterion bench. Asserts P99 wall-clock budgets
//! and no-op-trigger-vs-no-trigger baseline ratio for committing under a
//! registered no-op trigger callback. Absolute budgets are generous deadlock
//! guards; the ratio is the product signal. Override via env.

use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use contextdb_core::Value;

fn bench_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn bench_env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn percentile_99(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    let idx = ((samples.len() as f64) * 0.99) as usize;
    samples[idx.min(samples.len().saturating_sub(1))]
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn collect_p99(register_trigger: bool, writers: usize, commits_per_writer: usize) -> Duration {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    if register_trigger {
        let first_callback_seen = Arc::new(AtomicBool::new(false));
        let first_callback_entered = Arc::new(AtomicBool::new(false));
        let release_first_callback = Arc::new(AtomicBool::new(false));
        let first_callback_seen_cb = first_callback_seen.clone();
        let first_callback_entered_cb = first_callback_entered.clone();
        let release_first_callback_cb = release_first_callback.clone();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            if !first_callback_seen_cb.swap(true, AtomicOrdering::SeqCst) {
                first_callback_entered_cb.store(true, AtomicOrdering::SeqCst);
                while !release_first_callback_cb.load(AtomicOrdering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        // Deterministic RED warmup excluded from measured samples. §29 returns
        // typed Err to the probes; §30 waits and proceeds, then the normal
        // no-op-trigger benchmark below measures steady-state overhead.
        let warm_db = db.clone();
        let warm_fire = thread::spawn(move || {
            let tx = warm_db.begin().expect("warm begin");
            warm_db
                .execute_in_tx(
                    tx,
                    "INSERT INTO t (id) VALUES ($id)",
                    &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xBEEFFEED)))]),
                )
                .expect("warm insert");
            warm_db.commit(tx).expect("warm commit");
        });
        let entered_deadline = Instant::now() + Duration::from_secs(2);
        while !first_callback_entered.load(AtomicOrdering::SeqCst) {
            assert!(
                Instant::now() < entered_deadline,
                "trigger warmup callback did not enter"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let mut probes = Vec::with_capacity(writers);
        for _ in 0..writers {
            let db = db.clone();
            probes.push(thread::spawn(move || {
                let tx = db.begin().expect("warm probe begin");
                db.rollback(tx).expect("warm probe rollback");
            }));
        }
        thread::sleep(Duration::from_millis(50));
        release_first_callback.store(true, AtomicOrdering::SeqCst);
        warm_fire.join().unwrap();
        for probe in probes {
            probe.join().unwrap();
        }
    }

    let mut handles = Vec::with_capacity(writers);
    let samples = Arc::new(std::sync::Mutex::new(Vec::with_capacity(
        writers * commits_per_writer,
    )));
    for w in 0..writers {
        let db = db.clone();
        let samples = samples.clone();
        handles.push(thread::spawn(move || {
            for i in 0..commits_per_writer {
                let started = Instant::now();
                let tx = db.begin().expect("begin");
                db.execute_in_tx(
                    tx,
                    "INSERT INTO t (id) VALUES ($id)",
                    &HashMap::from([(
                        "id".to_string(),
                        Value::Uuid(uuid((w as u128) * 0x10000 + i as u128)),
                    )]),
                )
                .expect("insert");
                db.commit(tx).expect("commit");
                samples.lock().unwrap().push(started.elapsed());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    percentile_99(std::mem::take(&mut *samples.lock().unwrap()))
}

fn bench_trigger_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigger_throughput");
    group.sample_size(20);

    group.bench_function("trigger_commit_p99_latency_under_no_op_trigger_n_writers", |b| {
        let writers = std::env::var("CONTEXTDB_TRIGGER_BENCH_WRITERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16);
        let commits_per_writer = std::env::var("CONTEXTDB_TRIGGER_BENCH_COMMITS_PER_WRITER")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8);
        let commodity_budget_ms = bench_env_u64("CONTEXTDB_TRIGGER_COMMIT_COMMODITY_P99_MS", 200);
        let jetson_budget_ms = bench_env_u64("CONTEXTDB_TRIGGER_COMMIT_JETSON_P99_MS", 500);
        let max_trigger_to_baseline_ratio =
            bench_env_f64("CONTEXTDB_TRIGGER_COMMIT_P99_BASELINE_RATIO_MAX", 4.0);

        // Collect one baseline and one trigger batch up-front; assert before
        // the Criterion measurement loop so `cargo bench -- --test` is
        // deterministic and actually gates the product ratio.
        let baseline_p99 = collect_p99(false, writers, commits_per_writer);
        let p99 = collect_p99(true, writers, commits_per_writer);

        assert!(
            p99.as_secs_f64() <= baseline_p99.as_secs_f64() * max_trigger_to_baseline_ratio,
            "trigger_commit_p99_latency: trigger p99 {:?} exceeded baseline {:?} by ratio budget {max_trigger_to_baseline_ratio}x",
            p99,
            baseline_p99
        );

        assert!(
            p99 <= Duration::from_millis(jetson_budget_ms),
            "trigger_commit_p99_latency: p99 {:?} exceeded Jetson budget {jetson_budget_ms}ms",
            p99
        );
        assert!(
            p99 <= Duration::from_millis(commodity_budget_ms),
            "trigger_commit_p99_latency: p99 {:?} exceeded commodity budget {commodity_budget_ms}ms",
            p99
        );

        b.iter(|| {
            let db = Arc::new(Database::open_memory());
            db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty()).unwrap();
            db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty()).unwrap();
            db.register_trigger_callback("tr", |_db, _ctx| Ok(())).unwrap();
            db.complete_initialization().unwrap();
            let tx = db.begin().expect("begin");
            db.execute_in_tx(
                tx,
                "INSERT INTO t (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xBEEF)))]),
            )
            .expect("insert");
            db.commit(tx).expect("commit");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_trigger_throughput);
criterion_main!(benches);
