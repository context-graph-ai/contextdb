use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROOT_UUID: u128 = 0x2100;
const SNAPSHOT_P95_BUDGET: Duration = Duration::from_millis(5);

#[derive(Debug)]
struct ContentionReport {
    probes: usize,
    torn_reads: usize,
    first_torn: Option<String>,
    snapshot_p95: Duration,
}

#[derive(Debug)]
struct ProbeCounts {
    a: usize,
    b: usize,
    edges: usize,
    vectors: Option<usize>,
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("a", "embedding")
}

fn setup_db() -> Arc<Database> {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE a (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE TABLE b (id UUID PRIMARY KEY, label TEXT)", &empty())
        .unwrap();
    Arc::new(db)
}

fn write_three_paradigm_commit(db: &Database, seq: u128) {
    let id = Uuid::from_u128(seq + 1);
    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "a",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("label".to_string(), Value::Text(format!("a-{seq}"))),
            ]),
        )
        .unwrap();
    db.insert_row(
        tx,
        "b",
        HashMap::from([
            (
                "id".to_string(),
                Value::Uuid(Uuid::from_u128(1_000_000 + seq)),
            ),
            ("label".to_string(), Value::Text(format!("b-{seq}"))),
        ]),
    )
    .unwrap();
    db.insert_edge(
        tx,
        Uuid::from_u128(ROOT_UUID),
        id,
        "LINKS".to_string(),
        HashMap::new(),
    )
    .unwrap();
    db.insert_vector(tx, index(), row_id, vec![1.0, 0.0, 0.0])
        .unwrap();
    db.commit(tx).unwrap();
}

fn probe_once(db: &Database, total_commits: usize, probe_vector: bool) -> (Duration, ProbeCounts) {
    let started = Instant::now();
    let snapshot = db.snapshot();
    let snapshot_latency = started.elapsed();

    let a = db.scan("a", snapshot).unwrap().len();
    let b = db.scan("b", snapshot).unwrap().len();
    let edges = db
        .edge_count(Uuid::from_u128(ROOT_UUID), "LINKS", snapshot)
        .unwrap_or(0);
    let vectors = if probe_vector {
        Some(
            db.query_vector(index(), &[1.0, 0.0, 0.0], total_commits, None, snapshot)
                .unwrap()
                .len(),
        )
    } else {
        None
    };

    (
        snapshot_latency,
        ProbeCounts {
            a,
            b,
            edges,
            vectors,
        },
    )
}

fn torn_detail(counts: &ProbeCounts) -> Option<String> {
    let vector_ok = counts.vectors.is_none_or(|vectors| vectors == counts.a);
    if counts.a == counts.b && counts.b == counts.edges && vector_ok {
        return None;
    }

    Some(format!(
        "a={}, b={}, edges={}, vectors={:?}",
        counts.a, counts.b, counts.edges, counts.vectors
    ))
}

fn p95(mut latencies: Vec<Duration>) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }
    latencies.sort_unstable();
    let idx = (latencies.len() * 95).div_ceil(100).saturating_sub(1);
    latencies[idx]
}

fn run_contention_fixture(
    readers: usize,
    writers: usize,
    commits_per_writer: usize,
    vector_probe_stride: usize,
) -> ContentionReport {
    let db = setup_db();
    let total_commits = writers * commits_per_writer;
    let stop = Arc::new(AtomicBool::new(false));
    let vector_probe_stride = vector_probe_stride.max(1);

    let mut reader_handles = Vec::new();
    for _ in 0..readers {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        reader_handles.push(thread::spawn(move || {
            let mut probes = 0usize;
            let mut torn_reads = 0usize;
            let mut first_torn = None;
            let mut latencies = Vec::new();

            while !stop.load(Ordering::SeqCst) {
                let probe_vector = probes.is_multiple_of(vector_probe_stride);
                let (latency, counts) = probe_once(&db, total_commits, probe_vector);
                latencies.push(latency);
                if let Some(detail) = torn_detail(&counts) {
                    torn_reads += 1;
                    first_torn.get_or_insert(detail);
                }
                probes += 1;
                thread::yield_now();
            }

            (probes, torn_reads, first_torn, latencies)
        }));
    }

    thread::sleep(Duration::from_millis(5));

    let mut writer_handles = Vec::new();
    for writer in 0..writers {
        let db = Arc::clone(&db);
        writer_handles.push(thread::spawn(move || {
            for offset in 0..commits_per_writer {
                let seq = (writer * commits_per_writer + offset) as u128;
                write_three_paradigm_commit(&db, seq);
                thread::yield_now();
            }
        }));
    }

    for handle in writer_handles {
        handle.join().unwrap();
    }
    thread::sleep(Duration::from_millis(5));
    stop.store(true, Ordering::SeqCst);

    let mut probes = 0usize;
    let mut torn_reads = 0usize;
    let mut first_torn = None;
    let mut latencies = Vec::new();
    for handle in reader_handles {
        let (reader_probes, reader_torn, reader_first_torn, reader_latencies) =
            handle.join().unwrap();
        probes += reader_probes;
        torn_reads += reader_torn;
        if first_torn.is_none() {
            first_torn = reader_first_torn;
        }
        latencies.extend(reader_latencies);
    }

    let (latency, final_counts) = probe_once(&db, total_commits, true);
    latencies.push(latency);
    if let Some(detail) = torn_detail(&final_counts) {
        torn_reads += 1;
        first_torn.get_or_insert(detail);
    }
    probes += 1;

    ContentionReport {
        probes,
        torn_reads,
        first_torn,
        snapshot_p95: p95(latencies),
    }
}

fn assert_contention_report(report: &ContentionReport) {
    assert!(report.probes > 0, "contention fixture must take snapshots");
    assert_eq!(
        report.torn_reads, 0,
        "atomic visibility fixture observed torn reads; first={:?}",
        report.first_torn
    );
    assert!(
        report.snapshot_p95 < SNAPSHOT_P95_BUDGET,
        "snapshot acquisition p95 {:?} exceeded {:?}",
        report.snapshot_p95,
        SNAPSHOT_P95_BUDGET
    );
}

fn bench_atomic_visibility_under_contention(c: &mut Criterion) {
    let pre_gate = run_contention_fixture(1, 1, 50, 1);
    assert_contention_report(&pre_gate);

    let mut group = c.benchmark_group("tier1_atomic_visibility_under_contention");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(250));
    group.bench_function("8_readers_2_writers_1000_commits", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let started = Instant::now();
                let report = run_contention_fixture(8, 2, 1000, 16);
                assert_contention_report(&report);
                elapsed += started.elapsed();
            }
            elapsed
        });
    });
    group.finish();

    let post_gate = run_contention_fixture(8, 2, 1000, 1);
    assert_contention_report(&post_gate);
}

criterion_group!(benches, bench_atomic_visibility_under_contention);
criterion_main!(benches);
