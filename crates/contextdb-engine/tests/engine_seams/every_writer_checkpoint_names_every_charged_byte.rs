//! Every writer memory checkpoint names every byte the writer is charged for.
//!
//! The memory census reads the engine's own checkpoint lines while a real
//! import runs. A line is only evidence when the writer accountant's total is
//! exactly the sum of its named writer owners at that line. That must hold
//! while snapshots are pinned, transactions are open and commits land, not
//! only once the store is quiet. Process-owned residue, such as the open-time
//! snapshot pin capacity, belongs to the process rather than to this writer's
//! headroom. It must be reported beside the writer owners and never folded
//! into their sum.
//!
//! The byte-owner audit takes its field list from the receipt type itself, so
//! a new receipt field fails here until it is classified and emitted.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use serde_json::Value as Json;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use uuid::Uuid;

const RECEIPT_PATH: &str = "CONTEXTDB_TEST_SEAMS_RECEIPT_PATH";
const DIM: usize = 64;
const BATCHES: usize = 24;
const ROWS_PER_BATCH: usize = 8;

/// Writer owners whose sum must equal the writer accountant's total.
const NAMED_WRITER: &[&str] = &[
    "writer_registration",
    "engine_accountant_backing",
    "writer_adapter_controls",
    "pending_lineage_generation_charge",
    "storage_maintenance_control_charge",
    "commit_source_reference_charge",
    "writer_schema_charge",
    "published_root_charge",
    "captured_root_charge",
    "staged_index_postings",
    "current_row_values",
    "current_row_side_maps",
    "current_row_index_postings",
    "current_row_f32_projections",
    "staged_row_admissions",
    "staged_vector_admissions",
    "table_metadata",
    "graph_entries",
    "change_log_charges",
    "commit_index_charge",
    "persistence_cache",
    "custody_companion_memo",
    "custody_tenant_mirror",
    "current_membership_charge",
    "captured_membership_charge",
    "sink_window_charge",
];
/// A named writer owner the checkpoint line carries outside the receipt type.
const NAMED_WRITER_EMITTED_ONLY: &[&str] = &["schema_history_charge"];
const NAMED_WRITER_VECTOR: &[&str] = &["retained_total", "temporary_workspace"];
/// Owned by the process, not by this writer's headroom.
const PROCESS_OWNED: &[&str] = &["snapshot_pin_capacity_charge"];
const PROCESS_OWNED_VECTOR: &[&str] = &[
    "process_native_bytes",
    "process_native_owner_identity",
    "combined_writer_envelope",
];
/// Counts and physical inventories: observations, not accountant charges.
const OBSERVATIONS: &[&str] = &[
    "current_row_versions",
    "historical_row_versions",
    "current_rows_by_table",
    "historical_row_priced_bytes",
    "staged_row_versions",
    "staged_vector_owner_controls",
    "change_log_entries",
    "commit_index_entries",
    "current_membership_by_table",
    "relational_inventory",
    "ddl_log_entries",
    "ddl_log_estimated_bytes",
    "active_transactions",
    "pending_commit_metadata",
    "pending_schema_stages",
];
const TOTALS: &[&str] = &["named_owner_total", "reconciliation_gap"];
/// The parts `retained_total` is the sum of.
const VECTOR_RETAINED_PARTS: &[&str] = &[
    "source_metadata",
    "raw_vector_bodies",
    "raw_reverse_directory",
    "partition_vectors",
    "vector_partition_controls",
    "base_graph",
    "change_graph",
    "mutable_tail",
    "typed_partition_keys",
    "retired_pinned_graph_bytes",
];
/// Charged to the declared reader parent, not to the writer.
const VECTOR_READER: &[&str] = &[
    "reader_source_metadata",
    "reader_last_query_trace",
    "reader_child_controls",
    "reader_root_control",
    "reader_child_payloads",
    "reader_acl_cache",
    "reader_parent_total",
];
const VECTOR_TOTALS: &[&str] = &["non_vector_total", "accountant_total"];

fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(256 * 1024 * 1024),
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the census store")
}

fn context(n: usize) -> Uuid {
    Uuid::from_u128(0xCE05_0000_0000_0000_0000_0000_0000_0000 | n as u128)
}

fn embedding(id: usize) -> Vec<f32> {
    let mut v = vec![0.0; DIM];
    v[0] = 1.0;
    v[1] = (id + 1) as f32 / 4096.0;
    v[2] = ((id * 31) % 257) as f32 / 4096.0;
    v
}

fn passage(id: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".into(), Value::Int64(id as i64)),
        ("context".into(), Value::Uuid(context(id % 3))),
        (
            "body".into(),
            Value::Text(format!("passage {id} {}", "x".repeat(id % 97))),
        ),
        ("v".into(), Value::Vector(embedding(id))),
    ])
}

const INSERT: &str = "INSERT INTO passages VALUES ($id, $context, $body, $v)";

fn search(db: &Database, id: usize) {
    let rows = db
        .execute(
            "SELECT id FROM passages WHERE context_id = $context ORDER BY v <=> $v LIMIT 5",
            &HashMap::from([
                ("context".into(), Value::Uuid(context(id % 3))),
                ("v".into(), Value::Vector(embedding(id))),
            ]),
        )
        .expect("search while the import runs");
    assert!(!rows.rows().is_empty());
}

/// The import shape the census watches: batched commits with searches
/// between them, a caller-held snapshot pin and a long transaction open
/// across those commits, reads at the pinned snapshot, and maintenance.
fn import_workload(path: &Path) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let db = open(path);
    db.execute(
        "CREATE TABLE passages (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         body TEXT NOT NULL, v VECTOR(64) PARTITION_KEY (context_id) MAX_PARTITIONS 8 \
         SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the passages table");
    db.execute(
        "CREATE INDEX passages_context ON passages (context_id)",
        &HashMap::new(),
    )
    .expect("declare the context index");
    for id in 0..ROWS_PER_BATCH {
        db.execute(INSERT, &passage(id)).expect("seed one passage");
    }
    let pinned = db.snapshot();
    let pin = db.pin_snapshot(pinned).expect("pin a live snapshot slot");
    let long = db.begin().expect("open a transaction across the import");
    db.execute_in_tx(long, INSERT, &passage(1_000_000))
        .expect("stage a row in the long transaction");
    let mut next = ROWS_PER_BATCH;
    for batch in 0..BATCHES {
        let tx = db.begin().expect("begin one import batch");
        for _ in 0..ROWS_PER_BATCH {
            db.execute_in_tx(tx, INSERT, &passage(next))
                .expect("stage one passage");
            next += 1;
        }
        db.commit(tx).expect("commit one import batch");
        search(&db, next - 1);
        db.execute_at_snapshot(
            "SELECT id FROM passages ORDER BY v <=> $v LIMIT 3",
            &HashMap::from([("v".into(), Value::Vector(embedding(batch)))]),
            pinned,
        )
        .expect("read at the pinned snapshot while commits land");
        db.execute(
            "UPDATE passages SET body = $body WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Int64(batch as i64)),
                ("body".into(), Value::Text(format!("revised {batch}"))),
            ]),
        )
        .expect("revise one passage");
        if batch % 6 == 5 {
            db.run_maintenance_cycle()
                .expect("drive one maintenance cycle");
        }
    }
    db.commit(long).expect("commit the long transaction");
    drop(pin);
    db.execute(INSERT, &passage(next))
        .expect("commit after the pin is released");
    search(&db, next);
    db.close().expect("close the census store");
}

fn object<'a>(value: &'a Json, what: &str) -> &'a serde_json::Map<String, Json> {
    value
        .as_object()
        .unwrap_or_else(|| panic!("{what} is a JSON object: {value}"))
}

fn bytes(value: &Json) -> u64 {
    match value {
        Json::Array(items) => items.iter().map(bytes).sum(),
        other => other
            .as_u64()
            .unwrap_or_else(|| panic!("charge is a byte count: {other}")),
    }
}

fn keys(map: &serde_json::Map<String, Json>) -> BTreeSet<String> {
    map.keys().cloned().collect()
}

fn set(lists: &[&[&str]]) -> BTreeSet<String> {
    lists
        .iter()
        .flat_map(|list| list.iter().map(|name| name.to_string()))
        .collect()
}

#[test]
fn every_writer_checkpoint_names_every_charged_byte_while_pins_and_commits_are_live() {
    let dir = tempfile::tempdir().unwrap();
    if std::env::var_os(RECEIPT_PATH).is_some() {
        import_workload(&dir.path().join("census.db"));
        return;
    }
    let receipts = dir.path().join("writer.jsonl");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(
                module_path!(),
                "every_writer_checkpoint_names_every_charged_byte_while_pins_and_commits_are_live",
            ),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(RECEIPT_PATH, &receipts)
        .output()
        .expect("launch the census workload with checkpoint lines enabled");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("running 1 test"),
        "the exact filter runs the workload once: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.status.success(),
        "census workload failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // The receipt type's own fields, from a store of the same build.
    let reference = open(&dir.path().join("reference.db"));
    let receipt = reference.writer_memory_ownership_receipt_for_test();
    let pin_capacity = receipt.snapshot_pin_capacity_charge as u64;
    let typed = serde_json::to_value(&receipt).unwrap();
    reference.close().unwrap();
    let typed = object(&typed, "writer receipt");
    let typed_vector = object(&typed["vector"], "vector receipt");
    let mut classified = set(&[NAMED_WRITER, PROCESS_OWNED, OBSERVATIONS, TOTALS]);
    classified.insert("vector".into());
    let classified_vector = set(&[
        NAMED_WRITER_VECTOR,
        PROCESS_OWNED_VECTOR,
        VECTOR_RETAINED_PARTS,
        VECTOR_READER,
        VECTOR_TOTALS,
    ]);
    assert_eq!(
        keys(typed),
        classified,
        "every writer receipt field is classified as a named writer owner, \
         process-owned residue, an observation or a total"
    );
    assert_eq!(
        keys(typed_vector),
        classified_vector,
        "every vector receipt field is classified"
    );
    assert!(
        pin_capacity > 0,
        "the open-time snapshot pin capacity is funded"
    );

    let text = std::fs::read_to_string(&receipts).expect("the workload emitted checkpoint lines");
    let lines = text
        .lines()
        .map(|line| serde_json::from_str::<Json>(line).expect("one JSON checkpoint per line"))
        .filter(|line| line["kind"] == "writer_memory_ownership")
        .collect::<Vec<_>>();
    assert!(
        lines.len() > BATCHES,
        "a checkpoint for every committed batch: {} lines",
        lines.len()
    );
    assert!(
        lines
            .iter()
            .any(|line| line["receipt"]["active_transactions"].as_u64() > Some(0)),
        "some checkpoints are taken while another transaction is open"
    );

    let mut emitted_fields = set(&[NAMED_WRITER_EMITTED_ONLY]);
    emitted_fields.extend(keys(typed));
    let mut failures = Vec::new();
    for line in &lines {
        let at = format!(
            "{} at LSN {}",
            line["checkpoint"].as_str().unwrap_or("?"),
            line["lsn"]
        );
        let emitted = object(&line["receipt"], "checkpoint receipt");
        let vector = object(&emitted["vector"], "checkpoint vector receipt");
        let missing = emitted_fields
            .difference(&keys(emitted))
            .cloned()
            .collect::<Vec<_>>();
        let missing_vector = classified_vector
            .difference(&keys(vector))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() || !missing_vector.is_empty() {
            failures.push(format!(
                "{at}: fields not emitted: {missing:?} vector {missing_vector:?}"
            ));
        }
        if let Some(pin) = emitted.get("snapshot_pin_capacity_charge")
            && bytes(pin) != pin_capacity
        {
            failures.push(format!(
                "{at}: snapshot pin capacity {} is not the funded {pin_capacity}",
                bytes(pin)
            ));
        }
        let named = NAMED_WRITER
            .iter()
            .chain(NAMED_WRITER_EMITTED_ONLY)
            .map(|name| emitted.get(*name).map_or(0, bytes))
            .sum::<u64>()
            + NAMED_WRITER_VECTOR
                .iter()
                .map(|name| vector.get(*name).map_or(0, bytes))
                .sum::<u64>();
        let accountant = bytes(&vector["accountant_total"]);
        let reported = bytes(&emitted["named_owner_total"]);
        if reported != named {
            failures.push(format!(
                "{at}: reported named total {reported} is not the sum of named writer \
                 owners {named} (process residue folded in or an owner left out)"
            ));
        }
        if accountant != named {
            failures.push(format!(
                "{at}: writer accountant total {accountant} != named writer owners {named}, \
                 {} unnamed bytes; active transactions {}, adapter controls {}",
                accountant as i64 - named as i64,
                emitted["active_transactions"],
                emitted["writer_adapter_controls"]
            ));
        }
        let parts = VECTOR_RETAINED_PARTS
            .iter()
            .map(|name| vector.get(*name).map_or(0, bytes))
            .sum::<u64>();
        if parts != bytes(&vector["retained_total"]) {
            failures.push(format!(
                "{at}: vector retained total {} is not the sum of its parts {parts}",
                vector["retained_total"]
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} checkpoints do not account for every writer byte:\n{}",
        failures
            .iter()
            .map(|f| f.split(':').next().unwrap_or_default())
            .collect::<BTreeSet<_>>()
            .len(),
        lines.len(),
        failures.join("\n")
    );
}
