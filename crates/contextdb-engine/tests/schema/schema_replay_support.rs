// Canonical test-only replay helpers; this file declares no tests.

#[path = "../fixtures/retained_query_rows.rs"]
mod retained_query_rows;
#[path = "../fixtures/schema_wall/mod.rs"]
mod support;
use contextdb_core::Value;
use contextdb_engine::database::{
    ReceivedSchemaApplyObservationForTest, ReceivedSchemaApplyObserverForTest,
};
use contextdb_engine::{Database, MaintenancePolicy};
use redb::TableWriteOperationForTest;
use retained_query_rows::RetainedRows;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use support::*;

fn phase(name: &str) {
    println!("SCHEMA_REPLAY_PHASE {name}");
    std::io::stdout()
        .flush()
        .expect("flush external measurement marker");
    if name.starts_with("identical-")
        && name.ends_with("-round-begin")
        && std::env::var_os("CONTEXTDB_SCHEMA_TIMING_HANDSHAKE").is_some()
    {
        // Only the external timing runner opts into this handshake. It starts
        // its clock before acknowledging, so pipe buffering cannot undercount.
        let mut acknowledgement = String::new();
        std::io::stdin().read_line(&mut acknowledgement).unwrap();
        assert_eq!(acknowledgement, format!("START {name}\n"));
    }
}

// Every public sync operation has a flushed boundary and its original typed error.
// Setup timings diagnose setup only; none can stand in for a replay-round receipt.
async fn pull_phase(client: &contextdb_server::SyncClient, label: &str) {
    phase(&format!("{label}-begin"));
    let pages_before = client.pull_pages_read();
    let started = Instant::now();
    let result = client.pull_default().await;
    println!(
        "SCHEMA_SYNC_RESULT {label} elapsed_ns={} result={result:?}",
        started.elapsed().as_nanos()
    );
    result.unwrap_or_else(|error| {
        panic!("SCHEMA_FIRST_FAILURE phase={label} operation=pull error={error:?}")
    });
    println!(
        "SCHEMA_PULL_PAGES {label} before={pages_before} after={}",
        client.pull_pages_read()
    );
    phase(&format!("{label}-end"));
}

async fn push_phase(client: &contextdb_server::SyncClient, label: &str) {
    phase(&format!("{label}-begin"));
    let started = Instant::now();
    let result = client.push().await;
    println!(
        "SCHEMA_SYNC_RESULT {label} elapsed_ns={} result={result:?}",
        started.elapsed().as_nanos()
    );
    // A provenance refusal is an engine defect in this ordinary flow, never expected success.
    result.unwrap_or_else(|error| {
        panic!("SCHEMA_FIRST_FAILURE phase={label} operation=push error={error:?}")
    });
    phase(&format!("{label}-end"));
}

fn declare_one_hundred_tables(edge: &Database) {
    for ordinal in 0usize..100 {
        let name = format!("schema_{ordinal:03}");
        let parent = ordinal
            .checked_sub(1)
            .map(|previous| format!("schema_{previous:03}"));
        let ddl = match parent {
            Some(parent) => format!(
                "CREATE TABLE {name} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES {parent}(id), body TEXT, FOREIGN KEY (parent_id) REFERENCES {parent}(id)) SYNC OFF"
            ),
            None => format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF"),
        };
        edge.execute(&ddl, &empty())
            .expect("author one generated source schema declaration per commit");
    }
    edge.execute(
        "CREATE EVENT TYPE schema_099_insert WHEN INSERT ON schema_099",
        &empty(),
    )
    .expect("author the final event type");
    edge.execute(
        "CREATE TRIGGER schema_099_insert_trigger ON schema_099 WHEN INSERT INCLUDING SYNC",
        &empty(),
    )
    .expect("author the final including-sync trigger");
}

fn seed_unrelated_resident_vectors(receiver: &Database) {
    seed_unrelated_resident_vectors_with_count(receiver, 10_000);
}

fn seed_unrelated_resident_vectors_with_count(receiver: &Database, row_count: i64) {
    receiver
        .execute(
            "CREATE TABLE resident_vectors (id INTEGER PRIMARY KEY, embedding VECTOR(768)) SYNC TWO WAY",
            &empty(),
    )
    .expect("create the unrelated resident vector table");
    // The replay criterion concerns schema staging, not background graph construction.
    // The full fixture still supplies exactly 10,000 rows; bounded twins supply one.
    // Keep the original dimensions, values and single committed transaction.
    let tx = receiver
        .begin()
        .expect("begin the resident-vector fixture transaction");
    for id in (10_000 - row_count)..10_000 {
        let mut values = vec![0.0; 768];
        values[0] = id as f32;
        values[1] = id as f32 * 0.25;
        let vector = Value::Vector(values);
        receiver
            .execute_in_tx(
                tx,
                "INSERT INTO resident_vectors VALUES ($id, $embedding)",
                &HashMap::from([
                    ("id".to_string(), Value::Int64(id)),
                    ("embedding".to_string(), vector.clone()),
                ]),
            )
            .expect("seed one unrelated resident vector row");
    }
    receiver
        .commit(tx)
        .expect("commit the resident-vector fixture transaction");
}

fn receiver(path: &std::path::Path) -> Arc<Database> {
    receiver_with_unrelated_rows(path, crate::UNRELATED_RESIDENT_ROWS)
}

fn receiver_with_unrelated_rows(path: &std::path::Path, row_count: i64) -> Arc<Database> {
    let db = Arc::new(Database::open(path).expect("file-backed resident receiver"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    // Only the unrelated row count varies; full targets retain the frozen 10,000.
    phase("setup-resident-seed-begin");
    if row_count == 10_000 {
        seed_unrelated_resident_vectors(&db);
    } else {
        seed_unrelated_resident_vectors_with_count(&db, row_count);
    }
    phase("setup-resident-seed-end");
    db
}

fn resident_rows(db: &Database) -> RetainedRows {
    let rows: RetainedRows = db
        .execute(
            "SELECT id, embedding FROM resident_vectors ORDER BY id",
            &empty(),
        )
        .expect("resident data remains readable")
        .into();
    assert_eq!(
        rows.len(),
        usize::try_from(crate::UNRELATED_RESIDENT_ROWS).unwrap(),
        "never shrink the frozen resident dataset"
    );
    rows
}

fn cost_receipt(label: &str, observation: &ReceivedSchemaApplyObservationForTest) {
    let mut totals = std::collections::BTreeMap::<String, (u64, u64)>::new();
    for (event, count) in &observation.artifact_writes {
        let total = totals
            .entry(format!("{}:{:?}", event.table, event.operation))
            .or_default();
        total.0 += count;
        total.1 += serialized_bytes(event, *count);
    }
    println!("SCHEMA_REPLAY_ACTUAL_ARTIFACTS {label} {totals:?}");
    println!(
        "SCHEMA_REPLAY_PROFILE {label} nanos={:?} work={:?}",
        observation.profile_nanos, observation.profile_work
    );
}

include!("schema_replay_exchange_helpers.rs");

// The full targets retain the frozen row-count assertion through their root constant.
fn replay_resident_rows(db: &Database, row_count: i64) -> RetainedRows {
    if row_count == 10_000 {
        return resident_rows(db);
    }
    let rows: RetainedRows = db
        .execute(
            "SELECT id, embedding FROM resident_vectors ORDER BY id",
            &empty(),
        )
        .expect("bounded resident data remains readable")
        .into();
    assert_eq!(rows.len(), usize::try_from(row_count).unwrap());
    if row_count == 1 {
        assert_eq!(
            rows[0][0],
            Value::Int64(9999),
            "nonzero bounded row witness"
        );
        let Value::Vector(values) = &rows[0][1] else {
            panic!("bounded witness must retain its vector");
        };
        assert!(
            values.iter().any(|component| *component != 0.0),
            "bounded vector witness must contain a nonzero component"
        );
    }
    rows
}
