use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};
use uuid::Uuid;

const FORWARD_PROBE_10K_P95_BUDGET: Duration = Duration::from_millis(25);
const REVERSE_PROBE_100K_P95_BUDGET: Duration = Duration::from_millis(50);
const NON_ID_START_100K_P95_BUDGET: Duration = Duration::from_millis(50);

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(0xC000_0000_0000_0000_0000_0000_0000_0000 + n)
}

fn create_graph_tables(db: &Database, node_columns: &str) {
    db.execute(
        &format!("CREATE TABLE nodes (id UUID PRIMARY KEY{node_columns})"),
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
}

fn insert_node(db: &Database, id: Uuid, extras: &[(&str, Value)]) {
    let mut columns = vec!["id".to_string()];
    let mut placeholders = vec!["$id".to_string()];
    let mut values = HashMap::from([("id".to_string(), Value::Uuid(id))]);
    for (column, value) in extras {
        columns.push((*column).to_string());
        placeholders.push(format!("${column}"));
        values.insert((*column).to_string(), value.clone());
    }
    db.execute(
        &format!(
            "INSERT INTO nodes ({}) VALUES ({})",
            columns.join(", "),
            placeholders.join(", ")
        ),
        &values,
    )
    .unwrap();
}

fn insert_edge(db: &Database, source: Uuid, target: Uuid, edge_type: &str) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source", Value::Uuid(source)),
            ("target", Value::Uuid(target)),
            ("edge_type", Value::Text(edge_type.to_string())),
        ]),
    )
    .unwrap();
}

fn uuid_set(result: &contextdb_engine::QueryResult, column: &str) -> BTreeSet<Uuid> {
    let idx = result
        .columns
        .iter()
        .position(|c| c == column || c.rsplit('.').next() == Some(column))
        .unwrap_or_else(|| panic!("column {column} not found in {:?}", result.columns));
    result
        .rows
        .iter()
        .map(|row| match &row[idx] {
            Value::Uuid(id) => *id,
            other => panic!("expected UUID in {column}, got {other:?}"),
        })
        .collect()
}

fn seed_unrelated_edges(db: &Database, base: u128, count: usize, edge_type: &str) {
    for i in 0..count {
        insert_edge(
            db,
            uuid(base + (i as u128 * 2)),
            uuid(base + (i as u128 * 2) + 1),
            edge_type,
        );
    }
}

fn p95(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    let idx = (samples.len() * 95).div_ceil(100).saturating_sub(1);
    samples[idx]
}

fn assert_p95_under_budget<F>(mut probe: F, budget: Duration)
where
    F: FnMut(),
{
    let mut samples = Vec::new();
    for _ in 0..10 {
        let started = Instant::now();
        probe();
        samples.push(started.elapsed());
    }
    let got = p95(samples);
    assert!(got < budget, "p95 {got:?} exceeds budget {budget:?}");
}

fn seed_forward_probe_10k() -> (Database, Uuid, BTreeSet<Uuid>) {
    let db = Database::open_memory();
    create_graph_tables(&db, "");
    let start = uuid(1);
    insert_node(&db, start, &[]);
    let targets: BTreeSet<_> = (0..8).map(|i| uuid(10 + i)).collect();
    for target in &targets {
        insert_node(&db, *target, &[]);
        insert_edge(&db, start, *target, "LINKS");
    }
    seed_unrelated_edges(&db, 10_000, 10_000, "LINKS");
    (db, start, targets)
}

fn run_forward_probe(db: &Database, start: Uuid, expected: &BTreeSet<Uuid>) {
    let result = db
        .execute(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $start COLUMNS (b.id AS t))",
            &params(vec![("start", Value::Uuid(start))]),
        )
        .unwrap();
    assert_eq!(uuid_set(&result, "t"), *expected);
    assert_eq!(result.trace.physical_plan, "AdjacencyProbe");
    assert_eq!(result.trace.index_used.as_deref(), Some("forward_adj"));
    assert_eq!(db.__rows_examined(), expected.len() as u64);
}

fn seed_reverse_probe_100k() -> (Database, Uuid, BTreeSet<Uuid>) {
    let db = Database::open_memory();
    create_graph_tables(&db, "");
    let anchor = uuid(100);
    insert_node(&db, anchor, &[]);
    let sources: BTreeSet<_> = (0..8).map(|i| uuid(110 + i)).collect();
    for source in &sources {
        insert_node(&db, *source, &[]);
        insert_edge(&db, *source, anchor, "SERVES");
    }
    seed_unrelated_edges(&db, 100_000, 100_000, "SERVES");
    (db, anchor, sources)
}

fn run_reverse_probe(db: &Database, anchor: Uuid, expected: &BTreeSet<Uuid>) {
    let result = db
        .execute(
            "SELECT d FROM GRAPH_TABLE(edges MATCH (a)<-[:SERVES]-(b) WHERE a.id = $anchor COLUMNS (b.id AS d))",
            &params(vec![("anchor", Value::Uuid(anchor))]),
        )
        .unwrap();
    assert_eq!(uuid_set(&result, "d"), *expected);
    assert_eq!(result.trace.physical_plan, "AdjacencyProbe");
    assert_eq!(result.trace.index_used.as_deref(), Some("reverse_adj"));
    assert_eq!(db.__rows_examined(), expected.len() as u64);
}

fn seed_non_id_start_resolution_100k() -> (Database, BTreeSet<Uuid>) {
    let db = Database::open_memory();
    create_graph_tables(&db, ", kind TEXT");
    db.execute("CREATE INDEX idx_nodes_kind ON nodes (kind)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, kind TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_docs_kind ON docs (kind)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE tasks (id UUID PRIMARY KEY, kind TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_tasks_kind ON tasks (kind)", &empty())
        .unwrap();

    let start = uuid(1_000);
    insert_node(&db, start, &[("kind", Value::Text("root".into()))]);
    let targets: BTreeSet<_> = (0..12).map(|i| uuid(1_010 + i)).collect();
    for target in &targets {
        insert_node(&db, *target, &[("kind", Value::Text("target".into()))]);
        insert_edge(&db, start, *target, "LINKS");
    }

    for i in 0..34_000 {
        insert_node(
            &db,
            uuid(20_000 + i),
            &[("kind", Value::Text(format!("node-noise-{i}")))],
        );
    }
    for i in 0..33_000 {
        db.execute(
            "INSERT INTO docs (id, kind) VALUES ($id, $kind)",
            &params(vec![
                ("id", Value::Uuid(uuid(80_000 + i))),
                ("kind", Value::Text(format!("doc-noise-{i}"))),
            ]),
        )
        .unwrap();
    }
    for i in 0..33_000 {
        db.execute(
            "INSERT INTO tasks (id, kind) VALUES ($id, $kind)",
            &params(vec![
                ("id", Value::Uuid(uuid(140_000 + i))),
                ("kind", Value::Text(format!("task-noise-{i}"))),
            ]),
        )
        .unwrap();
    }
    (db, targets)
}

fn run_non_id_start_probe(db: &Database, expected: &BTreeSet<Uuid>) {
    let result = db
        .execute(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.kind = 'root' COLUMNS (b.id AS t))",
            &empty(),
        )
        .unwrap();
    assert_eq!(uuid_set(&result, "t"), *expected);
    assert_eq!(result.trace.physical_plan, "AdjacencyProbe");
    assert_eq!(result.trace.index_used.as_deref(), Some("forward_adj"));
    assert_eq!(db.__rows_examined(), 1 + expected.len() as u64);
}

fn bench_forward_probe_10k(c: &mut Criterion) {
    let (db, start, expected) = seed_forward_probe_10k();
    run_forward_probe(&db, start, &expected);
    assert_p95_under_budget(
        || run_forward_probe(&db, start, &expected),
        FORWARD_PROBE_10K_P95_BUDGET,
    );
    c.bench_function("bench_forward_probe_10k", |b| {
        b.iter_custom(|iters| {
            let started = Instant::now();
            for _ in 0..iters {
                run_forward_probe(&db, start, &expected);
            }
            started.elapsed()
        })
    });
}

fn bench_reverse_probe_100k(c: &mut Criterion) {
    let (db, anchor, expected) = seed_reverse_probe_100k();
    run_reverse_probe(&db, anchor, &expected);
    assert_p95_under_budget(
        || run_reverse_probe(&db, anchor, &expected),
        REVERSE_PROBE_100K_P95_BUDGET,
    );
    c.bench_function("bench_reverse_probe_100k", |b| {
        b.iter_custom(|iters| {
            let started = Instant::now();
            for _ in 0..iters {
                run_reverse_probe(&db, anchor, &expected);
            }
            started.elapsed()
        })
    });
}

fn bench_non_id_start_resolution_100k(c: &mut Criterion) {
    let (db, expected) = seed_non_id_start_resolution_100k();
    run_non_id_start_probe(&db, &expected);
    assert_p95_under_budget(
        || run_non_id_start_probe(&db, &expected),
        NON_ID_START_100K_P95_BUDGET,
    );
    c.bench_function("bench_non_id_start_resolution_100k", |b| {
        b.iter_custom(|iters| {
            let started = Instant::now();
            for _ in 0..iters {
                run_non_id_start_probe(&db, &expected);
            }
            started.elapsed()
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_non_id_start_resolution_100k, bench_forward_probe_10k, bench_reverse_probe_100k
}
criterion_main!(benches);
