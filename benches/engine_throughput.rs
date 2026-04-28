use contextdb_engine::Database;
use contextdb_engine::plugin::CorePlugin;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use contextdb_core::{MemoryAccountant, Value, VectorIndexRef};

fn bench_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn bench_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn nv_full_bench() -> bool {
    std::env::var_os("CONTEXTDB_NV_BENCH_FULL").is_some()
}

fn nv_default(local: usize, full: usize) -> usize {
    if nv_full_bench() { full } else { local }
}

fn params(values: Vec<(&str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn deterministic_vector(dim: usize, seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let mut values = Vec::with_capacity(dim);
    for _ in 0..dim {
        state = state
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let unit = ((state >> 33) as f64) / ((1u64 << 31) as f64);
        values.push((unit as f32) * 2.0 - 1.0);
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        values.iter_mut().for_each(|value| *value /= norm);
    }
    values
}

fn create_single_vector_table(
    db: &Database,
    table: &str,
    column: &str,
    dim: usize,
    quantization: Option<&str>,
) {
    let quantization = quantization
        .map(|q| format!(" WITH (quantization = '{q}')"))
        .unwrap_or_default();
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, {column} VECTOR({dim}){quantization})"
        ),
        &HashMap::new(),
    )
    .unwrap();
}

fn insert_vectors_direct(
    db: &Database,
    table: &str,
    column: &str,
    dim: usize,
    count: usize,
    seed_offset: u64,
) {
    let tx = db.begin();
    for i in 0..count {
        let row_id = db
            .insert_row(
                tx,
                table,
                HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
            )
            .unwrap();
        db.insert_vector(
            tx,
            VectorIndexRef::new(table, column),
            row_id,
            deterministic_vector(dim, seed_offset + i as u64),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}

fn vector_index_bytes(db: &Database, table: &str, column: &str) -> usize {
    let result = db.execute("SHOW VECTOR_INDEXES", &HashMap::new()).unwrap();
    let table_idx = result
        .columns
        .iter()
        .position(|name| name == "table")
        .unwrap();
    let column_idx = result
        .columns
        .iter()
        .position(|name| name == "column")
        .unwrap();
    let bytes_idx = result
        .columns
        .iter()
        .position(|name| name == "bytes")
        .unwrap();
    result
        .rows
        .iter()
        .find_map(|row| {
            let table_matches = matches!(&row[table_idx], Value::Text(value) if value == table);
            let column_matches = matches!(&row[column_idx], Value::Text(value) if value == column);
            if table_matches
                && column_matches
                && let Value::Int64(bytes) = row[bytes_idx]
            {
                return Some(bytes as usize);
            }
            None
        })
        .unwrap_or_else(|| panic!("SHOW VECTOR_INDEXES did not include {table}.{column}"))
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

fn persist_single_vector_db(
    path: &Path,
    table: &str,
    column: &str,
    dim: usize,
    quantization: Option<&str>,
    count: usize,
) -> u64 {
    let db = Database::open(path).unwrap();
    create_single_vector_table(&db, table, column, dim, quantization);
    insert_vectors_direct(&db, table, column, dim, count, 1);
    db.close().unwrap();
    file_len(path)
}

fn percentile_99(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    let index = ((samples.len().saturating_sub(1)) * 99) / 100;
    samples[index]
}

fn insert_evidence_sql(db: &Database, text_dim: usize, vision_dim: usize, row: usize) {
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $text, $vision)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            (
                "text",
                Value::Vector(deterministic_vector(text_dim, 10_000 + row as u64)),
            ),
            (
                "vision",
                Value::Vector(deterministic_vector(vision_dim, 20_000 + row as u64)),
            ),
        ]),
    )
    .unwrap();
}

fn create_evidence_table(db: &Database, text_dim: usize, vision_dim: usize) {
    db.execute(
        &format!(
            "CREATE TABLE evidence (
                id UUID PRIMARY KEY,
                vector_text VECTOR({text_dim}) WITH (quantization = 'SQ8'),
                vector_vision VECTOR({vision_dim}) WITH (quantization = 'SQ8')
            )"
        ),
        &HashMap::new(),
    )
    .unwrap();
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<usize> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<usize>().ok()?;
    Some(resident_pages.saturating_mul(4096))
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> Option<usize> {
    None
}

fn sql_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_insert");
    for count in [100, 500] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let db = Database::open_memory();
                db.execute(
                    "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, seq INTEGER)",
                    &HashMap::new(),
                )
                .unwrap();
                for i in 0..count {
                    db.execute(
                        "INSERT INTO items (id, name, seq) VALUES ($id, $name, $seq)",
                        &HashMap::from([
                            (
                                "id".to_string(),
                                contextdb_core::Value::Uuid(Uuid::new_v4()),
                            ),
                            (
                                "name".to_string(),
                                contextdb_core::Value::Text(format!("item-{i}")),
                            ),
                            ("seq".to_string(), contextdb_core::Value::Int64(i as i64)),
                        ]),
                    )
                    .unwrap();
                }
            });
        });
    }
    group.finish();
}

fn sql_query_throughput(c: &mut Criterion) {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, category TEXT, seq INTEGER, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    for i in 0..1_000 {
        db.execute(
            "INSERT INTO items (id, category, seq, name) VALUES ($id, $category, $seq, $name)",
            &HashMap::from([
                (
                    "id".to_string(),
                    contextdb_core::Value::Uuid(Uuid::new_v4()),
                ),
                (
                    "category".to_string(),
                    contextdb_core::Value::Text(if i % 2 == 0 { "a" } else { "b" }.to_string()),
                ),
                ("seq".to_string(), contextdb_core::Value::Int64(i)),
                (
                    "name".to_string(),
                    contextdb_core::Value::Text(format!("item-{i}")),
                ),
            ]),
        )
        .unwrap();
    }

    c.bench_function("sql_query/select_after_1k_rows", |b| {
        b.iter(|| {
            db.execute(
                "SELECT id, name FROM items WHERE category = 'a' AND seq >= 500 ORDER BY seq DESC LIMIT 20",
                &HashMap::new(),
            )
            .unwrap();
        });
    });
}

fn vector_insert_and_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector");

    group.bench_function("insert_500_vectors", |b| {
        b.iter(|| {
            let db = Database::open_memory();
            db.execute(
                "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
                &HashMap::new(),
            )
            .unwrap();
            let tx = db.begin();
            for i in 0..500 {
                let angle = i as f32 * 0.01;
                let rid = db
                    .insert_row(
                        tx,
                        "obs",
                        HashMap::from([(
                            "id".to_string(),
                            contextdb_core::Value::Uuid(Uuid::new_v4()),
                        )]),
                    )
                    .unwrap();
                db.insert_vector(
                    tx,
                    contextdb_core::VectorIndexRef::new("obs", "embedding"),
                    rid,
                    vec![angle.cos(), angle.sin(), 0.0],
                )
                .unwrap();
            }
            db.commit(tx).unwrap();
        });
    });

    group.bench_function("search_in_1000_vectors", |b| {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin();
        for i in 0..1_000 {
            let angle = i as f32 * 0.01;
            let rid = db
                .insert_row(
                    tx,
                    "obs",
                    HashMap::from([(
                        "id".to_string(),
                        contextdb_core::Value::Uuid(Uuid::new_v4()),
                    )]),
                )
                .unwrap();
            db.insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("obs", "embedding"),
                rid,
                vec![angle.cos(), angle.sin(), 0.0],
            )
            .unwrap();
        }
        db.commit(tx).unwrap();

        b.iter(|| {
            db.query_vector(
                contextdb_core::VectorIndexRef::new("obs", "embedding"),
                &[1.0, 0.0, 0.0],
                10,
                None,
                db.snapshot(),
            )
            .unwrap();
        });
    });

    group.finish();
}

fn graph_bfs_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_bfs");

    group.bench_function("bfs_1000_node_chain", |b| {
        let db = Database::open_memory();
        let tx = db.begin();
        let mut nodes = Vec::new();
        for _ in 0..1_000 {
            nodes.push(Uuid::new_v4());
        }
        for i in 1..nodes.len() {
            db.insert_edge(
                tx,
                nodes[i - 1],
                nodes[i],
                "CHAIN".to_string(),
                HashMap::new(),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();

        b.iter(|| {
            db.query_bfs(
                nodes[0],
                None,
                contextdb_core::Direction::Outgoing,
                1000,
                db.snapshot(),
            )
            .unwrap();
        });
    });

    group.finish();
}

fn persist_and_reopen_smoke(c: &mut Criterion) {
    c.bench_function("persist_1k_rows_reopen", |b| {
        b.iter(|| {
            let tmp = tempfile::TempDir::new().unwrap();
            let path = tmp.path().join("bench.db");
            let db = Database::open(&path).unwrap();
            db.execute(
                "CREATE TABLE t (id UUID PRIMARY KEY, seq INTEGER)",
                &HashMap::new(),
            )
            .unwrap();
            for batch in 0..10 {
                let tx = db.begin();
                for i in 0..100 {
                    db.insert_row(
                        tx,
                        "t",
                        HashMap::from([
                            (
                                "id".to_string(),
                                contextdb_core::Value::Uuid(Uuid::new_v4()),
                            ),
                            (
                                "seq".to_string(),
                                contextdb_core::Value::Int64((batch * 100 + i) as i64),
                            ),
                        ]),
                    )
                    .unwrap();
                }
                db.commit(tx).unwrap();
            }
            db.close().unwrap();
            let db2 = Database::open(&path).unwrap();
            assert_eq!(db2.scan("t", db2.snapshot()).unwrap().len(), 1_000);
        });
    });
}

fn named_vector_index_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("named_vector_indexes");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(100));
    group.warm_up_time(Duration::from_millis(50));

    group.bench_function("nv_sq8_footprint", |b| {
        let dim = bench_env_usize("CONTEXTDB_NV_FOOTPRINT_DIM", nv_default(256, 768));
        let count = bench_env_usize("CONTEXTDB_NV_FOOTPRINT_ROWS", nv_default(1_000, 10_000));
        let tmp = tempfile::TempDir::new().unwrap();
        let sq8_path = tmp.path().join("sq8.db");
        let f32_path = tmp.path().join("f32.db");

        let sq8_file_bytes =
            persist_single_vector_db(&sq8_path, "footprint", "vec", dim, Some("SQ8"), count);
        let f32_file_bytes =
            persist_single_vector_db(&f32_path, "footprint", "vec", dim, None, count);
        assert!(
            sq8_file_bytes <= f32_file_bytes / 3,
            "nv_sq8_footprint: SQ8 file footprint must be <= 1/3 of F32 baseline; sq8={sq8_file_bytes}, f32={f32_file_bytes}"
        );

        let f32_payload_bytes = count * dim * std::mem::size_of::<f32>();
        let accountant = Arc::new(MemoryAccountant::with_budget(f32_payload_bytes));
        let db = Database::open_memory_with_accountant(accountant.clone());
        create_single_vector_table(&db, "footprint", "vec", dim, Some("SQ8"));
        insert_vectors_direct(&db, "footprint", "vec", dim, count, 1);
        let sq8_index_bytes = vector_index_bytes(&db, "footprint", "vec");
        assert!(
            sq8_index_bytes <= f32_payload_bytes / 3,
            "nv_sq8_footprint: SQ8 reported memory footprint must be <= 1/3 of F32 payload; sq8={sq8_index_bytes}, f32_payload={f32_payload_bytes}"
        );

        b.iter(|| black_box((sq8_file_bytes, f32_file_bytes, sq8_index_bytes)));
    });

    group.bench_function("nv_sq8_search_latency", |b| {
        let dim = bench_env_usize("CONTEXTDB_NV_SEARCH_DIM", nv_default(128, 768));
        let count = bench_env_usize("CONTEXTDB_NV_SEARCH_ROWS", nv_default(2_000, 1_000_000));
        let probes = bench_env_usize("CONTEXTDB_NV_SEARCH_PROBES", nv_default(10, 20));
        let max_p99_ms = bench_env_u64("CONTEXTDB_NV_SEARCH_P99_MS", 100);

        let db = Database::open_memory();
        create_single_vector_table(&db, "searchbench", "vec", dim, Some("SQ8"));
        insert_vectors_direct(&db, "searchbench", "vec", dim, count, 1);
        let query = deterministic_vector(dim, 1);

        let mut samples = Vec::with_capacity(probes);
        for _ in 0..probes {
            let started = Instant::now();
            db.query_vector(
                VectorIndexRef::new("searchbench", "vec"),
                &query,
                10,
                None,
                db.snapshot(),
            )
            .unwrap();
            samples.push(started.elapsed());
        }
        let p99 = percentile_99(samples);
        assert!(
            p99 <= Duration::from_millis(max_p99_ms),
            "nv_sq8_search_latency: p99 {:?} exceeded {max_p99_ms}ms at {count}x{dim}",
            p99
        );

        b.iter(|| {
            db.query_vector(
                VectorIndexRef::new("searchbench", "vec"),
                black_box(&query),
                10,
                None,
                db.snapshot(),
            )
            .unwrap();
        });
    });

    group.bench_function("nv_multi_vector_ingest", |b| {
        let rows = bench_env_usize("CONTEXTDB_NV_INGEST_ROWS", nv_default(20, 100));
        let text_dim = bench_env_usize("CONTEXTDB_NV_TEXT_DIM", nv_default(128, 768));
        let vision_dim = bench_env_usize("CONTEXTDB_NV_VISION_DIM", nv_default(96, 512));
        let total_dim = text_dim + vision_dim;
        let baseline_dim = total_dim / 2;

        let tmp = tempfile::TempDir::new().unwrap();
        let single = Database::open(tmp.path().join("single.db")).unwrap();
        create_single_vector_table(&single, "single", "vec", baseline_dim, Some("SQ8"));
        let single_started = Instant::now();
        for row in 0..(rows * 2) {
            single
                .execute(
                    "INSERT INTO single (id, vec) VALUES ($id, $vec)",
                    &params(vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        (
                            "vec",
                            Value::Vector(deterministic_vector(baseline_dim, 30_000 + row as u64)),
                        ),
                    ]),
                )
                .unwrap();
        }
        let single_elapsed = single_started.elapsed();
        single.close().unwrap();

        let multi = Database::open(tmp.path().join("multi.db")).unwrap();
        create_evidence_table(&multi, text_dim, vision_dim);
        let multi_started = Instant::now();
        for row in 0..rows {
            insert_evidence_sql(&multi, text_dim, vision_dim, row);
        }
        let multi_elapsed = multi_started.elapsed();
        multi.close().unwrap();

        assert!(
            multi_elapsed.as_secs_f64() <= single_elapsed.as_secs_f64() * 1.10,
            "nv_multi_vector_ingest: two-vector ingest exceeded 10% overhead against same-vector-count single-index baseline; multi={multi_elapsed:?}, single={single_elapsed:?}"
        );

        b.iter(|| {
            let db = Database::open_memory();
            create_evidence_table(&db, text_dim, vision_dim);
            for row in 0..rows {
                insert_evidence_sql(&db, text_dim, vision_dim, row);
            }
        });
    });

    group.bench_function("nv_ingest_p99_latency", |b| {
        let rows = bench_env_usize("CONTEXTDB_NV_INGEST_P99_ROWS", nv_default(20, 50));
        let text_dim = bench_env_usize("CONTEXTDB_NV_TEXT_DIM", nv_default(128, 768));
        let vision_dim = bench_env_usize("CONTEXTDB_NV_VISION_DIM", nv_default(96, 512));
        let commodity_budget_ms = bench_env_u64("CONTEXTDB_NV_INGEST_COMMODITY_P99_MS", 50);
        let jetson_budget_ms = bench_env_u64("CONTEXTDB_NV_INGEST_JETSON_P99_MS", 100);

        let db = Database::open_memory();
        create_evidence_table(&db, text_dim, vision_dim);
        let mut samples = Vec::with_capacity(rows);
        for row in 0..rows {
            let started = Instant::now();
            insert_evidence_sql(&db, text_dim, vision_dim, row);
            samples.push(started.elapsed());
        }
        let p99 = percentile_99(samples);
        assert!(
            p99 <= Duration::from_millis(jetson_budget_ms),
            "nv_ingest_p99_latency: p99 {:?} exceeded Jetson budget {jetson_budget_ms}ms",
            p99
        );
        assert!(
            p99 <= Duration::from_millis(commodity_budget_ms),
            "nv_ingest_p99_latency: p99 {:?} exceeded commodity budget {commodity_budget_ms}ms",
            p99
        );

        b.iter(|| {
            let db = Database::open_memory();
            create_evidence_table(&db, text_dim, vision_dim);
            for row in 0..rows {
                insert_evidence_sql(&db, text_dim, vision_dim, row);
            }
        });
    });

    group.bench_function("nv_replay_peak_memory", |b| {
        let rows = bench_env_usize("CONTEXTDB_NV_REPLAY_ROWS", nv_default(200, 100_000));
        let dim = bench_env_usize("CONTEXTDB_NV_REPLAY_DIM", nv_default(64, 128));
        let indexes = bench_env_usize("CONTEXTDB_NV_REPLAY_INDEXES", 5);
        let memory_limit = bench_env_usize(
            "CONTEXTDB_NV_REPLAY_MEMORY_LIMIT",
            rows * indexes * dim * 2,
        );
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("replay.db");
        {
            let db = Database::open(&path).unwrap();
            let columns = (0..indexes)
                .map(|i| format!("v{i} VECTOR({dim}) WITH (quantization = 'SQ8')"))
                .collect::<Vec<_>>()
                .join(", ");
            db.execute(
                &format!("CREATE TABLE replay (id UUID PRIMARY KEY, {columns})"),
                &HashMap::new(),
            )
            .unwrap();
            for row in 0..rows {
                let mut values = vec![("id", Value::Uuid(Uuid::new_v4()))];
                for i in 0..indexes {
                    values.push((
                        match i {
                            0 => "v0",
                            1 => "v1",
                            2 => "v2",
                            3 => "v3",
                            4 => "v4",
                            _ => panic!("CONTEXTDB_NV_REPLAY_INDEXES supports up to 5"),
                        },
                        Value::Vector(deterministic_vector(dim, (i * rows + row) as u64)),
                    ));
                }
                let column_names = (0..indexes)
                    .map(|i| format!("v{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = (0..indexes)
                    .map(|i| format!("$v{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                db.execute(
                    &format!("INSERT INTO replay (id, {column_names}) VALUES ($id, {placeholders})"),
                    &params(values),
                )
                .unwrap();
            }
            db.close().unwrap();
        }

        let accountant = Arc::new(MemoryAccountant::with_budget(memory_limit));
        let before_rss = current_rss_bytes().unwrap_or(0);
        let reopened =
            Database::open_with_config(&path, Arc::new(CorePlugin), accountant.clone()).unwrap();
        let after_rss = current_rss_bytes().unwrap_or(before_rss);
        let accounted = accountant.usage().used;
        assert!(
            accounted <= memory_limit,
            "nv_replay_peak_memory: accounted replay bytes {accounted} exceeded MEMORY_LIMIT {memory_limit}"
        );
        if after_rss > before_rss {
            let rss_delta = after_rss - before_rss;
            assert!(
                rss_delta <= memory_limit.saturating_mul(4),
                "nv_replay_peak_memory: RSS delta {rss_delta} was unexpectedly high for MEMORY_LIMIT {memory_limit}"
            );
        }

        b.iter(|| {
            black_box(reopened.scan("replay", reopened.snapshot()).unwrap().len());
        });
    });

    group.bench_function("nv_post_replay_first_search_latency", |b| {
        let rows = bench_env_usize("CONTEXTDB_NV_FIRST_SEARCH_ROWS", nv_default(500, 2_000));
        let dim = bench_env_usize("CONTEXTDB_NV_FIRST_SEARCH_DIM", nv_default(64, 128));
        let indexes = bench_env_usize("CONTEXTDB_NV_FIRST_SEARCH_INDEXES", 5);
        let max_ms = bench_env_u64("CONTEXTDB_NV_FIRST_SEARCH_MS", 500);
        let db = Database::open_memory();
        let columns = (0..indexes)
            .map(|i| format!("v{i} VECTOR({dim}) WITH (quantization = 'SQ8')"))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(
            &format!("CREATE TABLE firstsearch (id UUID PRIMARY KEY, {columns})"),
            &HashMap::new(),
        )
        .unwrap();
        for i in 0..indexes {
            insert_vectors_direct(
                &db,
                "firstsearch",
                &format!("v{i}"),
                dim,
                rows,
                i as u64 * 10_000,
            );
        }
        let query = deterministic_vector(dim, 1);
        let mut max_elapsed = Duration::ZERO;
        for i in 0..indexes {
            let started = Instant::now();
            db.query_vector(
                VectorIndexRef::new("firstsearch", format!("v{i}")),
                &query,
                10,
                None,
                db.snapshot(),
            )
            .unwrap();
            max_elapsed = max_elapsed.max(started.elapsed());
        }
        assert!(
            max_elapsed <= Duration::from_millis(max_ms),
            "nv_post_replay_first_search_latency: max first search {:?} exceeded {max_ms}ms",
            max_elapsed
        );

        b.iter(|| {
            for i in 0..indexes {
                db.query_vector(
                    VectorIndexRef::new("firstsearch", format!("v{i}")),
                    black_box(&query),
                    10,
                    None,
                    db.snapshot(),
                )
                .unwrap();
            }
        });
    });

    group.bench_function("nv_disjoint_index_read_concurrency", |b| {
        let readers = bench_env_usize("CONTEXTDB_NV_CONCURRENT_READERS", 20);
        let indexes = bench_env_usize("CONTEXTDB_NV_CONCURRENT_INDEXES", 10);
        let rows = bench_env_usize("CONTEXTDB_NV_CONCURRENT_ROWS", 200);
        let dim = bench_env_usize("CONTEXTDB_NV_CONCURRENT_DIM", 64);
        let ddl_budget_ms = bench_env_u64("CONTEXTDB_NV_CONCURRENT_DDL_MS", 5);
        let db = Arc::new(Database::open_memory());
        let columns = (0..indexes)
            .map(|i| format!("v{i} VECTOR({dim}) WITH (quantization = 'SQ8')"))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(
            &format!("CREATE TABLE concurrent (id UUID PRIMARY KEY, {columns})"),
            &HashMap::new(),
        )
        .unwrap();
        for i in 0..indexes {
            insert_vectors_direct(&db, "concurrent", &format!("v{i}"), dim, rows, i as u64 * 10_000);
        }

        let mut handles = Vec::with_capacity(readers);
        for reader in 0..readers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                let column = format!("v{}", reader % indexes);
                let query = deterministic_vector(dim, reader as u64 + 1);
                db.query_vector(
                    VectorIndexRef::new("concurrent", column),
                    &query,
                    5,
                    None,
                    db.snapshot(),
                )
                .unwrap();
            }));
        }
        let started = Instant::now();
        db.execute("ALTER TABLE concurrent ADD COLUMN note TEXT", &HashMap::new())
            .unwrap();
        let ddl_elapsed = started.elapsed();
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(
            ddl_elapsed <= Duration::from_millis(ddl_budget_ms),
            "nv_disjoint_index_read_concurrency: DDL elapsed {:?} exceeded {ddl_budget_ms}ms with {readers} readers",
            ddl_elapsed
        );

        b.iter(|| {
            let query = deterministic_vector(dim, 1);
            db.query_vector(
                VectorIndexRef::new("concurrent", "v0"),
                black_box(&query),
                5,
                None,
                db.snapshot(),
            )
            .unwrap();
        });
    });

    group.bench_function("nv_multi_index_fsync_amplification", |b| {
        let dim = bench_env_usize("CONTEXTDB_NV_FSYNC_DIM", 64);
        let indexes = bench_env_usize("CONTEXTDB_NV_FSYNC_INDEXES", 10);
        let repeats = bench_env_usize("CONTEXTDB_NV_FSYNC_REPEATS", 5);

        fn commit_one(path: &Path, indexes: usize, dim: usize) -> Duration {
            let db = Database::open(path).unwrap();
            let columns = (0..indexes)
                .map(|i| format!("v{i} VECTOR({dim}) WITH (quantization = 'SQ8')"))
                .collect::<Vec<_>>()
                .join(", ");
            db.execute(
                &format!("CREATE TABLE fsyncbench (id UUID PRIMARY KEY, {columns})"),
                &HashMap::new(),
            )
            .unwrap();
            let mut values = vec![("id", Value::Uuid(Uuid::new_v4()))];
            for i in 0..indexes {
                values.push((
                    match i {
                        0 => "v0",
                        1 => "v1",
                        2 => "v2",
                        3 => "v3",
                        4 => "v4",
                        5 => "v5",
                        6 => "v6",
                        7 => "v7",
                        8 => "v8",
                        9 => "v9",
                        _ => panic!("CONTEXTDB_NV_FSYNC_INDEXES supports up to 10"),
                    },
                    Value::Vector(deterministic_vector(dim, i as u64 + 1)),
                ));
            }
            let column_names = (0..indexes)
                .map(|i| format!("v{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = (0..indexes)
                .map(|i| format!("$v{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let started = Instant::now();
            db.execute(
                &format!("INSERT INTO fsyncbench (id, {column_names}) VALUES ($id, {placeholders})"),
                &params(values),
            )
            .unwrap();
            let elapsed = started.elapsed();
            db.close().unwrap();
            elapsed
        }

        let mut one_index = Vec::new();
        let mut multi_index = Vec::new();
        for repeat in 0..repeats {
            let tmp = tempfile::TempDir::new().unwrap();
            one_index.push(commit_one(&tmp.path().join(format!("one-{repeat}.db")), 1, dim * indexes));
            multi_index.push(commit_one(&tmp.path().join(format!("multi-{repeat}.db")), indexes, dim));
        }
        one_index.sort_unstable();
        multi_index.sort_unstable();
        let one_median = one_index[one_index.len() / 2];
        let multi_median = multi_index[multi_index.len() / 2];
        assert!(
            multi_median.as_secs_f64() <= one_median.as_secs_f64() * 2.0,
            "nv_multi_index_fsync_amplification: {indexes}-index commit median exceeded 2x single-index baseline; multi={multi_median:?}, single={one_median:?}"
        );

        b.iter(|| black_box((one_median, multi_median)));
    });

    group.finish();
}

criterion_group!(
    benches,
    sql_insert_throughput,
    sql_query_throughput,
    vector_insert_and_search,
    graph_bfs_throughput,
    persist_and_reopen_smoke,
    named_vector_index_benches,
);
criterion_main!(benches);
