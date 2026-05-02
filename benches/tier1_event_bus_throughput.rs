use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const PRE_GATE_ROWS: usize = 128;
const TIMED_ROWS: usize = 10_000;
const TIMED_BATCH_ROWS: usize = 2_000;
const MIN_COMMIT_ROWS_PER_SEC: f64 = 5_000.0;
const MAX_AMORTIZED_COMMIT_P95: Duration = Duration::from_millis(5);
const MAX_FAST_SINK_P95: Duration = Duration::from_secs(2);

fn bench_event_bus_throughput(c: &mut Criterion) {
    pre_gate_exact_delivery();

    let mut group = c.benchmark_group("tier1_event_bus_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("commit_loop_with_blocked_sink", |b| {
        b.iter_custom(|iters| {
            let mut measured = Duration::ZERO;
            for _ in 0..iters {
                measured += timed_path_and_post_gate();
            }
            measured
        });
    });
    group.finish();
}

fn declare_schema(db: &Database) {
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, seq INT, severity TEXT, reason TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("CREATE SINK fast_sink TYPE callback", &HashMap::new())
        .unwrap();
    db.execute("CREATE SINK slow_sink TYPE callback", &HashMap::new())
        .unwrap();
    db.execute(
        "CREATE ROUTE fast_r EVENT inv_match TO fast_sink WHERE severity IN ('warning')",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE ROUTE slow_r EVENT inv_match TO slow_sink WHERE severity IN ('warning')",
        &HashMap::new(),
    )
    .unwrap();
}

fn pre_gate_exact_delivery() {
    let db = Database::open_memory();
    declare_schema(&db);
    let fast = Arc::new(AtomicU64::new(0));
    let slow = Arc::new(AtomicU64::new(0));
    let fast_cb = fast.clone();
    db.register_sink("fast_sink", None, move |_| {
        fast_cb.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    let slow_cb = slow.clone();
    db.register_sink("slow_sink", None, move |_| {
        slow_cb.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();

    insert_rows(&db, PRE_GATE_ROWS);
    wait_for_count(
        &fast,
        PRE_GATE_ROWS as u64,
        Duration::from_secs(5),
        "fast pre-gate",
    );
    wait_for_count(
        &slow,
        PRE_GATE_ROWS as u64,
        Duration::from_secs(5),
        "slow pre-gate",
    );
    assert_eq!(fast.load(Ordering::SeqCst), PRE_GATE_ROWS as u64);
    assert_eq!(slow.load(Ordering::SeqCst), PRE_GATE_ROWS as u64);
}

fn timed_path_and_post_gate() -> Duration {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("event-bus-throughput.redb");
    let db = Database::open(&path).unwrap();
    declare_schema(&db);

    let fast = Arc::new(AtomicU64::new(0));
    let slow = Arc::new(AtomicU64::new(0));
    let fast_latencies = Arc::new(Mutex::new(Vec::with_capacity(TIMED_ROWS)));
    let sent_at: Arc<Mutex<Vec<Option<Instant>>>> = Arc::new(Mutex::new(vec![None; TIMED_ROWS]));

    let fast_cb = fast.clone();
    let fast_latencies_cb = fast_latencies.clone();
    let sent_at_cb = sent_at.clone();
    db.register_sink("fast_sink", None, move |event| {
        let Some(Value::Int64(seq)) = event.row_values.get("seq") else {
            return Ok(());
        };
        let sent = sent_at_cb.lock().unwrap()[*seq as usize]
            .expect("sent timestamp must exist for event seq");
        fast_latencies_cb.lock().unwrap().push(sent.elapsed());
        fast_cb.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();

    let slow_gate = Arc::new(Mutex::new(()));
    let slow_gate_cb = slow_gate.clone();
    let slow_cb = slow.clone();
    db.register_sink("slow_sink", None, move |_| {
        let _guard = slow_gate_cb.lock().unwrap();
        slow_cb.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();

    let slow_guard = slow_gate.lock().unwrap();
    let mut commit_latencies = Vec::with_capacity(TIMED_ROWS);
    let commit_loop_start = Instant::now();
    for batch_start in (0..TIMED_ROWS).step_by(TIMED_BATCH_ROWS) {
        db.execute("BEGIN", &HashMap::new()).unwrap();
        let batch_end = (batch_start + TIMED_BATCH_ROWS).min(TIMED_ROWS);
        for seq in batch_start..batch_end {
            insert_row(&db, seq);
        }
        let commit_ready = Instant::now();
        {
            let mut sent = sent_at.lock().unwrap();
            for seq in batch_start..batch_end {
                sent[seq] = Some(commit_ready);
            }
        }
        let start = Instant::now();
        db.execute("COMMIT", &HashMap::new()).unwrap();
        commit_latencies.push(start.elapsed() / (batch_end - batch_start) as u32);
    }
    let commit_elapsed = commit_loop_start.elapsed();

    let rows_per_sec = TIMED_ROWS as f64 / commit_elapsed.as_secs_f64();
    assert!(
        rows_per_sec >= MIN_COMMIT_ROWS_PER_SEC,
        "event bus commit loop throughput too low: {rows_per_sec:.0} rows/sec"
    );
    let commit_p95 = percentile(commit_latencies, 95.0);
    assert!(
        commit_p95 <= MAX_AMORTIZED_COMMIT_P95,
        "event bus amortized commit p95 under blocked sink too high: {commit_p95:?}"
    );

    wait_for_count(
        &fast,
        TIMED_ROWS as u64,
        Duration::from_secs(10),
        "fast timed path",
    );
    let fast_p95 = percentile(fast_latencies.lock().unwrap().clone(), 95.0);
    assert!(
        fast_p95 <= MAX_FAST_SINK_P95,
        "fast sink p95 latency under blocked slow sink too high: {fast_p95:?}"
    );
    std::hint::black_box(fast_p95);

    assert_eq!(
        fast.load(Ordering::SeqCst),
        TIMED_ROWS as u64,
        "fast sink must receive every timed-path event exactly once"
    );
    assert_eq!(
        slow.load(Ordering::SeqCst),
        0,
        "blocked slow sink must not make progress before its gate is released"
    );

    drop(slow_guard);
    wait_for_count(
        &slow,
        TIMED_ROWS as u64,
        Duration::from_secs(10),
        "slow post-gate",
    );
    assert_eq!(
        slow.load(Ordering::SeqCst),
        TIMED_ROWS as u64,
        "slow sink must drain every queued timed-path event after gate release"
    );

    commit_elapsed
}

fn insert_rows(db: &Database, rows: usize) {
    for seq in 0..rows {
        insert_row(db, seq);
    }
}

fn insert_row(db: &Database, seq: usize) {
    let mut params = HashMap::new();
    params.insert("id".into(), Value::Uuid(Uuid::new_v4()));
    params.insert("seq".into(), Value::Int64(seq as i64));
    params.insert("sev".into(), Value::Text("warning".into()));
    params.insert("rsn".into(), Value::Text("bench".into()));
    db.execute(
        "INSERT INTO invalidations (id, seq, severity, reason) VALUES ($id, $seq, $sev, $rsn)",
        &params,
    )
    .unwrap();
}

fn wait_for_count(counter: &AtomicU64, expected: u64, timeout: Duration, label: &str) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline && counter.load(Ordering::SeqCst) < expected {
        std::thread::yield_now();
    }
    assert!(
        counter.load(Ordering::SeqCst) >= expected,
        "{label} sink delivered {} events, expected {expected}",
        counter.load(Ordering::SeqCst)
    );
}

fn percentile(mut values: Vec<Duration>, p: f64) -> Duration {
    assert!(
        !values.is_empty(),
        "cannot compute percentile of empty sample"
    );
    values.sort_unstable();
    let idx = ((values.len() - 1) as f64 * (p / 100.0)).round() as usize;
    values[idx]
}

criterion_group!(benches, bench_event_bus_throughput);
criterion_main!(benches);
