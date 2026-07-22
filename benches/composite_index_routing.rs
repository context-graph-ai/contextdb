mod common;

use common::scale::{empty, params};
use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::{BTreeSet, HashMap};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const ROUTE_SOURCE_COUNT: usize = 10;
const INLIST_PARAM_COUNT: usize = 10_000;
const CONCURRENT_WRITES: usize = 128;
const ROUTE_ROWS_EXAMINED: u64 = ROUTE_SOURCE_COUNT as u64;
const INLIST_ROWS_EXAMINED: u64 = INLIST_PARAM_COUNT as u64;
const ROUTE_P95_10K_BUDGET: Duration = Duration::from_millis(25);
const ROUTE_P95_100K_BUDGET: Duration = Duration::from_millis(75);
const ROUTE_P95_1M_BUDGET: Duration = Duration::from_millis(250);
const WRITE_COMMIT_P95_BUDGET: Duration = Duration::from_millis(250);
const INLIST_PEAK_RSS_DELTA_BUDGET_KIB: u64 = 128 * 1024;

#[derive(Clone)]
struct RouteCase {
    db: Arc<Database>,
    sql: String,
    params: HashMap<String, Value>,
    expected_ids: BTreeSet<Uuid>,
    expected_rows_examined: u64,
}

#[derive(Debug, Clone, Copy)]
struct InListProbeStats {
    elapsed: Duration,
    peak_rss_delta_kib: u64,
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(0xD000_0000_0000_0000_0000_0000_0000_0000 + n)
}

fn route_sql(source_count: usize) -> String {
    let placeholders = (0..source_count)
        .map(|i| format!("$s{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT id FROM edges \
         WHERE source_id IN ({placeholders}) \
           AND target_id = $target \
           AND edge_type = $edge_type"
    )
}

fn create_edges_table(db: &Database) {
    db.execute(
        "CREATE TABLE edges (
            id UUID PRIMARY KEY,
            source_id UUID,
            target_id UUID,
            edge_type TEXT
        )",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_edges_source ON edges (source_id)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_edges_route ON edges (source_id, target_id, edge_type)",
        &empty(),
    )
    .unwrap();
}

fn insert_edge(db: &Database, id: Uuid, source_id: Uuid, target_id: Uuid, edge_type: &str) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type)
         VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("source_id", Value::Uuid(source_id)),
            ("target_id", Value::Uuid(target_id)),
            ("edge_type", Value::Text(edge_type.to_string())),
        ]),
    )
    .unwrap();
}

fn ids_from_result(result: &QueryResult) -> BTreeSet<Uuid> {
    let id_idx = result
        .columns
        .iter()
        .position(|c| c == "id" || c.rsplit('.').next() == Some("id"))
        .unwrap_or_else(|| panic!("id column not found in {:?}", result.columns));
    result
        .rows
        .iter()
        .map(|row| match &row[id_idx] {
            Value::Uuid(id) => *id,
            other => panic!("expected UUID id, got {other:?}"),
        })
        .collect()
}

fn source_params(sources: &[Uuid], target: Uuid) -> HashMap<String, Value> {
    let mut values = HashMap::with_capacity(sources.len() + 2);
    for (i, source) in sources.iter().enumerate() {
        values.insert(format!("s{i}"), Value::Uuid(*source));
    }
    values.insert("target".to_string(), Value::Uuid(target));
    values.insert("edge_type".to_string(), Value::Text("BASED_ON".into()));
    values
}

fn seed_context_graph_route_case(row_count: usize) -> RouteCase {
    let db = Arc::new(Database::open_memory());
    create_edges_table(&db);

    let target = uuid(9_000_000);
    let sources = (0..ROUTE_SOURCE_COUNT)
        .map(|i| uuid(1_000 + i as u128))
        .collect::<Vec<_>>();
    let mut expected_ids = BTreeSet::new();
    for (i, source) in sources.iter().enumerate() {
        let id = uuid(10_000 + i as u128);
        expected_ids.insert(id);
        insert_edge(&db, id, *source, target, "BASED_ON");
    }

    for i in expected_ids.len()..row_count {
        let source = if i % 4 == 0 {
            sources[i % sources.len()]
        } else {
            uuid(100_000 + (i % 2_048) as u128)
        };
        let noise_target = uuid(200_000 + (i % 16_384) as u128);
        let edge_type = if i % 3 == 0 { "CITES" } else { "MENTIONS" };
        insert_edge(
            &db,
            uuid(300_000 + i as u128),
            source,
            noise_target,
            edge_type,
        );
    }

    RouteCase {
        db,
        sql: route_sql(sources.len()),
        params: source_params(&sources, target),
        expected_ids,
        expected_rows_examined: ROUTE_ROWS_EXAMINED,
    }
}

fn seed_inlist_10k_param_case() -> RouteCase {
    let db = Arc::new(Database::open_memory());
    create_edges_table(&db);

    let target = uuid(8_000_000);
    let sources = (0..INLIST_PARAM_COUNT)
        .map(|i| uuid(1_000_000 + i as u128))
        .collect::<Vec<_>>();
    let mut expected_ids = BTreeSet::new();
    for (i, source) in sources.iter().enumerate() {
        let id = uuid(2_000_000 + i as u128);
        expected_ids.insert(id);
        insert_edge(&db, id, *source, target, "BASED_ON");
        insert_edge(
            &db,
            uuid(3_000_000 + i as u128),
            *source,
            uuid(4_000_000 + i as u128),
            "CITES",
        );
    }

    RouteCase {
        db,
        sql: route_sql(sources.len()),
        params: source_params(&sources, target),
        expected_ids,
        expected_rows_examined: INLIST_ROWS_EXAMINED,
    }
}

fn run_route_query(case: &RouteCase, assert_rows_examined: bool) {
    if assert_rows_examined {
        case.db.__reset_rows_examined();
    }
    let result = case.db.execute(&case.sql, &case.params).unwrap();
    assert_eq!(ids_from_result(&result), case.expected_ids);
    assert_eq!(result.trace.physical_plan, "IndexScan");
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some("__graph_edge_source_target_type")
    );
    let pushed = result
        .trace
        .predicates_pushed
        .iter()
        .map(|c| c.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(pushed, ["source_id", "target_id", "edge_type"]);
    assert!(result.trace.indexes_considered.iter().any(|candidate| {
        candidate.name == "idx_edges_source"
            && candidate.rejected_reason.as_ref()
                == "fewer predicate columns matched than chosen index"
    }));
    if assert_rows_examined {
        assert_eq!(
            case.db.__rows_examined(),
            case.expected_rows_examined,
            "composite prefix probe must not walk same-source noise rows"
        );
    }
}

fn p95(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    let idx = (samples.len() * 95).div_ceil(100).saturating_sub(1);
    samples[idx]
}

fn assert_route_p95_under_budget(case: &RouteCase, budget: Duration) {
    let mut samples = Vec::new();
    for _ in 0..10 {
        let started = Instant::now();
        run_route_query(case, true);
        samples.push(started.elapsed());
    }
    let got = p95(samples);
    assert!(got < budget, "route p95 {got:?} exceeds budget {budget:?}");
}

fn status_kib(label: &str) -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix(label)?;
            rest.split_whitespace().next()?.parse::<u64>().ok()
        })
        .unwrap_or(0)
}

fn measure_inlist_probe(case: &RouteCase) -> InListProbeStats {
    let rss_before = status_kib("VmRSS:");
    let peak_before = status_kib("VmHWM:");
    let started = Instant::now();
    run_route_query(case, true);
    let elapsed = started.elapsed();
    let rss_after = status_kib("VmRSS:");
    let peak_after = status_kib("VmHWM:");
    InListProbeStats {
        elapsed,
        peak_rss_delta_kib: peak_after
            .saturating_sub(peak_before)
            .max(rss_after.saturating_sub(rss_before)),
    }
}

fn assert_inlist_memory_under_budget(stats: InListProbeStats) {
    assert!(
        stats.peak_rss_delta_kib <= INLIST_PEAK_RSS_DELTA_BUDGET_KIB,
        "10K IN-list probe peak RSS delta {} KiB exceeds {} KiB budget",
        stats.peak_rss_delta_kib,
        INLIST_PEAK_RSS_DELTA_BUDGET_KIB
    );
}

fn insert_writer_noise(db: &Database, batch: usize, i: usize) {
    let offset = (batch * CONCURRENT_WRITES + i) as u128;
    insert_edge(
        db,
        uuid(6_000_000 + offset),
        uuid(6_100_000 + offset),
        uuid(6_200_000 + offset),
        "WRITE_NOISE",
    );
}

fn run_concurrent_write_probe(case: &RouteCase, batch: usize) {
    run_route_query(case, true);
    let writer_db = case.db.clone();
    let writer = thread::spawn(move || {
        let mut latencies = Vec::with_capacity(CONCURRENT_WRITES);
        for i in 0..CONCURRENT_WRITES {
            let started = Instant::now();
            insert_writer_noise(&writer_db, batch, i);
            latencies.push(started.elapsed());
        }
        latencies
    });

    let mut reads = 0usize;
    loop {
        run_route_query(case, false);
        reads += 1;
        if writer.is_finished() {
            break;
        }
        if reads.is_multiple_of(64) {
            thread::yield_now();
        }
    }

    let latencies = writer.join().expect("writer thread must complete");
    assert!(!latencies.is_empty());
    let commit_p95 = p95(latencies);
    assert!(
        commit_p95 < WRITE_COMMIT_P95_BUDGET,
        "concurrent writer commit p95 {commit_p95:?} exceeds budget {WRITE_COMMIT_P95_BUDGET:?}"
    );
    assert!(reads > 0, "reader must run while writer commits");
    run_route_query(case, true);
}

fn route_scales() -> Vec<(usize, Duration)> {
    let mut scales = vec![
        (10_000, ROUTE_P95_10K_BUDGET),
        (100_000, ROUTE_P95_100K_BUDGET),
    ];
    if std::env::var_os("CONTEXTDB_COMPOSITE_ROUTING_1M").is_some() {
        scales.push((1_000_000, ROUTE_P95_1M_BUDGET));
    }
    scales
}

fn bench_context_graph_route(c: &mut Criterion) {
    let mut group = c.benchmark_group("composite_index_context_graph_route");
    for (rows, budget) in route_scales() {
        let case = seed_context_graph_route_case(rows);
        run_route_query(&case, true);
        assert_route_p95_under_budget(&case, budget);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &case, |b, case| {
            b.iter(|| run_route_query(black_box(case), true));
        });
    }
    group.finish();
}

fn bench_inlist_10k_param_peak_rss(c: &mut Criterion) {
    let case = seed_inlist_10k_param_case();
    let stats = measure_inlist_probe(&case);
    assert_inlist_memory_under_budget(stats);
    black_box(stats.peak_rss_delta_kib);
    c.bench_function("composite_index_inlist_10k_params_peak_rss", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            let mut peak_delta_kib = 0;
            for _ in 0..iters {
                let stats = measure_inlist_probe(&case);
                assert_inlist_memory_under_budget(stats);
                elapsed += stats.elapsed;
                peak_delta_kib = peak_delta_kib.max(stats.peak_rss_delta_kib);
            }
            black_box(peak_delta_kib);
            elapsed
        })
    });
}

fn bench_concurrent_write_route(c: &mut Criterion) {
    let case = seed_context_graph_route_case(10_000);
    run_concurrent_write_probe(&case, 0);
    let mut batch = 1usize;
    c.bench_function("composite_index_route_concurrent_writes", |b| {
        b.iter(|| {
            let current = batch;
            batch += 1;
            run_concurrent_write_probe(black_box(&case), current);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_context_graph_route, bench_inlist_10k_param_peak_rss, bench_concurrent_write_route
}
criterion_main!(benches);
