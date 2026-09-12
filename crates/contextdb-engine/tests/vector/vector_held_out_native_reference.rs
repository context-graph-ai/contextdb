use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use std::{collections::HashMap, sync::Arc};

struct Clock;
impl DeadlineClock for Clock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

// Independent scalar native F32 reference, including accumulator order.
fn native_score(query: &[f32], vector: &[f32]) -> f32 {
    let (mut dot, mut q2, mut v2) = (0.0_f32, 0.0_f32, 0.0_f32);
    for (&q, &v) in query.iter().zip(vector) {
        dot += q * v;
        q2 += q * q;
        v2 += v * v;
    }
    if q2 == 0.0 || v2 == 0.0 {
        0.0
    } else {
        dot / (q2.sqrt() * v2.sqrt())
    }
}
fn generated(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(17);
    (0..64)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as i32 - (1 << 23)) as f32 / (1 << 23) as f32
        })
        .collect()
}

#[test]
fn held_out_unfiltered_native_reference_recovers_the_required_neighbors() {
    let directory = tempfile::tempdir().unwrap();
    let (db, vectors, queries, table) = if let Some(input) =
        std::env::var_os("CONTEXTDB_VECTOR_REGRESSION_FIXTURE")
    {
        let input = std::path::PathBuf::from(input);
        let db = Database::open(directory.path().join("query.redb")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let bytes = std::fs::read(input.join("native-f32.bin")).unwrap();
        assert_eq!(bytes.len() % (8 + 64 * 4), 0);
        let vectors = bytes
            .chunks_exact(8 + 64 * 4)
            .map(|row| {
                let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                let vector = row[8..]
                    .chunks_exact(4)
                    .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap()))
                    .collect::<Vec<_>>();
                (id, vector)
            })
            .collect::<Vec<_>>();
        let queries: Vec<Vec<f32>> =
            serde_json::from_slice(&std::fs::read(input.join("probes.json")).unwrap()).unwrap();
        db.execute("CREATE TABLE receipt_items (id INT PRIMARY KEY, scope_id INT NOT NULL, embedding VECTOR(64) PARTITION_KEY (scope_id))", &HashMap::new()).unwrap();
        for batch in vectors.chunks(128) {
            db.execute("BEGIN", &HashMap::new()).unwrap();
            for (id, vector) in batch {
                db.execute(
                    "INSERT INTO receipt_items VALUES ($id, 0, $query)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(*id)),
                        ("query".into(), Value::Vector(vector.clone())),
                    ]),
                )
                .unwrap();
            }
            db.execute("COMMIT", &HashMap::new()).unwrap();
        }
        for _ in 0..4 {
            db.run_maintenance_cycle().unwrap();
        }
        (db, vectors, queries, "receipt_items")
    } else {
        let db = Database::open_memory();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(64))",
            &HashMap::new(),
        )
        .unwrap();
        let vectors = (1..=1000)
            .map(|id| (id, generated(id as u64)))
            .collect::<Vec<_>>();
        db.execute("BEGIN", &HashMap::new()).unwrap();
        for (id, vector) in &vectors {
            db.execute(
                "INSERT INTO items VALUES ($id, $query)",
                &HashMap::from([
                    ("id".into(), Value::Int64(*id)),
                    ("query".into(), Value::Vector(vector.clone())),
                ]),
            )
            .unwrap();
        }
        db.execute("COMMIT", &HashMap::new()).unwrap();
        for _ in 0..4 {
            db.run_maintenance_cycle().unwrap();
        }
        (
            db,
            vectors,
            (0..8).map(|id| generated(9000 + id)).collect(),
            "items",
        )
    };
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let mut recovered = 0;
    for query in &queries {
        let mut reference = vectors
            .iter()
            .map(|(id, v)| (*id, native_score(query, v)))
            .collect::<Vec<_>>();
        reference.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let params = HashMap::from([("query".into(), Value::Vector(query.clone()))]);
        let sql = format!(
            "SELECT id, score FROM {table} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
        );
        let ordinary = db.execute(&sql, &params).unwrap();
        let limits = ReadLimits {
            work: 10_000_000,
            memory: 256 * 1024 * 1024,
            ..ReadLimits::default()
        };
        let bounded = bounded::execute(
            &db,
            &bounded::BoundedReadRequest::new(&sql, params, limits, Arc::new(Clock)),
        )
        .unwrap();
        assert_eq!(ordinary.rows, bounded.result.rows);
        assert_eq!(ordinary.rows.len(), 10);
        for row in &ordinary.rows {
            let Value::Int64(id) = row[0] else {
                panic!("integer identity");
            };
            let native = reference.iter().find(|r| r.0 == id).unwrap();
            assert_eq!(row[1], Value::Float64(native.1 as f64), "native score bits");
            recovered += usize::from(reference[..10].iter().any(|r| r.0 == id));
        }
    }
    println!(
        "native_reference_rows={} probes={} recovered={recovered}/{}",
        vectors.len(),
        queries.len(),
        queries.len() * 10
    );
    assert!(
        recovered * 100 >= queries.len() * 10 * 95,
        "held-out native recall {recovered}/{}",
        queries.len() * 10
    );
}
