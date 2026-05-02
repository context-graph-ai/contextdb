use contextdb_core::Value;
use contextdb_core::types::Principal;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const TIMED_ROWS: u128 = 100_000;
const HOT_SELECTS: usize = 1_000;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn row_acl_id(i: u128) -> Uuid {
    Uuid::from_u128(1_000_000 + i)
}

fn seed(path: &std::path::Path, rows: u128) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE acl_grants (id UUID, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE memos (id UUID, body TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
        &empty(),
    )
    .unwrap();
    let tx = db.begin();
    for i in 0..rows {
        let principal = if i % 2 == 0 { "a1" } else { "b1" };
        let acl = row_acl_id(i);
        db.insert_row(
            tx,
            "acl_grants",
            HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(2_000_000 + i))),
                ("principal_kind".into(), Value::Text("Agent".into())),
                ("principal_id".into(), Value::Text(principal.into())),
                ("acl_id".into(), Value::Uuid(acl)),
            ]),
        )
        .unwrap();
    }
    for i in 0..rows {
        let acl = row_acl_id(i);
        db.insert_row(
            tx,
            "memos",
            HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(i + 10))),
                ("body".into(), Value::Text(format!("memo-{i}"))),
                ("acl_id".into(), Value::Uuid(acl)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.execute("CREATE INDEX memos_id_idx ON memos(id)", &empty())
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

fn bench_acl_overhead(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let admin_path = tmp.path().join("acl-admin.redb");
    let gated_path = tmp.path().join("acl-gated.redb");
    let rows = TIMED_ROWS;
    let hot_sql = "SELECT id FROM memos WHERE id = $id";
    let hot_params = HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(10)))]);
    seed(&admin_path, rows);
    seed(&gated_path, rows);

    let admin = Database::open(&admin_path).unwrap();
    assert_eq!(
        admin
            .execute("SELECT id FROM memos", &empty())
            .unwrap()
            .rows
            .len(),
        rows as usize
    );
    assert_eq!(
        admin
            .execute("SELECT id FROM acl_grants", &empty())
            .unwrap()
            .rows
            .len(),
        rows as usize,
        "bench must include one grant per protected row"
    );
    c.bench_function("tier1_acl_overhead/ungated", |b| {
        b.iter(|| {
            let result = admin.execute(hot_sql, &hot_params).unwrap();
            std::hint::black_box(result.rows[0][0].clone());
        });
    });

    let gated = Database::open_as_principal(&gated_path, Principal::Agent("a1".into())).unwrap();
    assert_eq!(
        gated
            .execute("SELECT id, acl_id FROM memos", &empty())
            .unwrap()
            .rows
            .len(),
        (rows / 2) as usize
    );
    assert!(
        gated
            .execute("SELECT id FROM memos", &empty())
            .unwrap()
            .rows
            .iter()
            .all(|row| matches!(&row[0], Value::Uuid(id) if (id.as_u128() - 10) % 2 == 0))
    );

    c.bench_function("tier1_acl_overhead/ratio_guard", |b| {
        b.iter_custom(|iters| {
            let _ = iters;
            let iterations = HOT_SELECTS;
            let ungated = hot_selects(&admin, hot_sql, &hot_params, iterations);
            let gated_elapsed = hot_selects(&gated, hot_sql, &hot_params, iterations);
            assert!(
                gated_elapsed.as_nanos() < ungated.as_nanos().saturating_mul(2),
                "ACL guard overhead exceeded 2x: gated={gated_elapsed:?} ungated={ungated:?}"
            );
            gated_elapsed
        });
    });

    c.bench_function("tier1_acl_overhead/gated", |b| {
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
    targets = bench_acl_overhead
}
criterion_main!(benches);
