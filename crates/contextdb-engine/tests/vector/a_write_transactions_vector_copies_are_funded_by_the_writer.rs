//! A write transaction's vector copies are funded by the writer's budget, never by a reader
//! statement's read limit: one transaction that inserts more vector bytes than a statement may
//! read commits, under a writer limit that holds them.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSIONS: usize = 768;
// The commit's per-row bookkeeping for 40,000 vectors (about 556 B each, ~22 MB) is above
// the 16 MiB a statement may read; the vectors themselves are ~123 MB.
const ROWS: i64 = 40_000;

#[test]
fn a_write_transaction_larger_than_a_statement_read_limit_commits_under_the_writer_budget() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("store.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.set_memory_limit(Some(1 << 30)).unwrap();
    db.execute(
        &format!(
            "CREATE TABLE seeded (id INT PRIMARY KEY, context_id UUID NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) MAX_PARTITIONS 4)"
        ),
        &HashMap::new(),
    )
    .unwrap();
    let context = Value::Uuid(Uuid::from_u128(7));
    let tx = db.begin().unwrap();
    for id in 0..ROWS {
        let vector: Vec<f32> = (0..DIMENSIONS)
            .map(|lane| ((id as f32) * 0.37 + lane as f32 * 0.11).sin())
            .collect();
        db.execute_in_tx(
            tx,
            "INSERT INTO seeded VALUES ($id, $context, $embedding)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("context".to_string(), context.clone()),
                ("embedding".to_string(), Value::Vector(vector)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx)
        .expect("a write commit is funded by the writer budget, not a statement read limit");
    let count = db
        .execute("SELECT id FROM seeded", &HashMap::new())
        .unwrap()
        .rows
        .len();
    assert_eq!(count as i64, ROWS);
}
