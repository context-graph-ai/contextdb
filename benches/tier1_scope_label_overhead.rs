use contextdb_core::Value;
use contextdb_core::types::ScopeLabel;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const TIMED_ROWS: u128 = 100_000;
const HOT_SELECTS: usize = 1_000;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn seed(path: &std::path::Path, rows: u128) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE signatures (id UUID, body TEXT, \
         scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server'))",
        &empty(),
    )
    .unwrap();
    let tx = db.begin();
    for i in 0..rows {
        db.insert_row(
            tx,
            "signatures",
            HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(i + 1))),
                ("body".into(), Value::Text(format!("sig-{i}"))),
                (
                    "scope_label".into(),
                    Value::Text(if i % 2 == 0 { "edge" } else { "server" }.into()),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.execute("CREATE INDEX signatures_id_idx ON signatures(id)", &empty())
        .unwrap();
    db.close().unwrap();
}

fn hot_selects(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,
    iterations: usize,
) -> Duration {
    let started = Instant::now();
    for _ in 0..iterations {
        let result = db.execute(sql, params).unwrap();
        std::hint::black_box(result.rows[0][0].clone());
    }
    started.elapsed()
}

fn bench_scope_label_overhead(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let admin_path = tmp.path().join("scope-label-admin.redb");
    let gated_path = tmp.path().join("scope-label-gated.redb");
    let rows = TIMED_ROWS;
    let hot_sql = "SELECT id FROM signatures WHERE id = $id";
    let hot_params = HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(1)))]);
    seed(&admin_path, rows);
    seed(&gated_path, rows);

    let admin = Database::open(&admin_path).unwrap();
    assert_eq!(
        admin
            .execute("SELECT id FROM signatures", &empty())
            .unwrap()
            .rows
            .len(),
        rows as usize
    );
    c.bench_function("tier1_scope_label_overhead/ungated", |b| {
        b.iter(|| {
            let result = admin.execute(hot_sql, &hot_params).unwrap();
            std::hint::black_box(result.rows[0][0].clone());
        });
    });

    let gated =
        Database::open_with_scope_labels(&gated_path, BTreeSet::from([ScopeLabel::new("edge")]))
            .unwrap();
    assert_eq!(
        gated
            .execute("SELECT id, scope_label FROM signatures", &empty())
            .unwrap()
            .rows
            .len(),
        (rows / 2) as usize
    );
    assert!(
        gated
            .execute("SELECT scope_label FROM signatures", &empty())
            .unwrap()
            .rows
            .iter()
            .all(|row| row[0] == Value::Text("edge".into()))
    );

    c.bench_function("tier1_scope_label_overhead/ratio_guard", |b| {
        b.iter_custom(|iters| {
            let _ = iters;
            let iterations = HOT_SELECTS;
            let ungated = hot_selects(&admin, hot_sql, &hot_params, iterations);
            let gated_elapsed = hot_selects(&gated, hot_sql, &hot_params, iterations);
            assert!(
                gated_elapsed.as_nanos() < ungated.as_nanos().saturating_mul(2),
                "scope-label guard overhead exceeded 2x: gated={gated_elapsed:?} ungated={ungated:?}"
            );
            gated_elapsed
        });
    });

    c.bench_function("tier1_scope_label_overhead/gated", |b| {
        b.iter(|| {
            let result = gated.execute(hot_sql, &hot_params).unwrap();
            std::hint::black_box(result.rows[0][0].clone());
        });
    });
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(300))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_scope_label_overhead
}
criterion_main!(benches);
