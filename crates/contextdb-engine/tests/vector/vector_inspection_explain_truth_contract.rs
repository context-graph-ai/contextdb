//! Production-door proofs for passive vector inspection and truthful explain.
//!
//! This deliberately uses the SQL inspection/explain surfaces and the existing
//! read-only test counters.  The counters establish that inspection did not
//! construct a graph or change the store charge; they are not another way to
//! obtain lifecycle facts. `QueryTrace::vector_search` is an intentional
//! production compile wall for the accepted typed, per-result vector
//! disclosure; the current label-only `physical_plan` is not treated as that
//! promised surface. `__vector_passive_activity_counters_for_test` is the
//! second narrow observation wall: it counts raw/graph loads, replay, build,
//! repair, compaction, and eviction without performing any of them.

use contextdb_core::{ContextId, Value, VectorIndexRef, VectorPartitionKey, VectorSearchMode};
use contextdb_engine::{
    Database, ExplainOutput, MaintenancePolicy, QueryResult, VectorSearchScopeShape,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::{BTreeSet, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_FINITE_CYCLES: usize = 32;
const PROOF_WAIT: Duration = Duration::from_secs(5);
const SCOPE_ALPHA: &str = "layout-key-alpha-that-must-not-leak";
const SCOPE_BRAVO: &str = "layout-key-bravo-that-must-not-leak";
const SCOPE_HIDDEN: &str = "unauthorized-layout-key-that-must-not-leak";
const HIDDEN_CONTEXT: u128 = 0xA11C_E000_0000_0000_0000_0000_0000_0102;
const HIDDEN_ROW: u128 = 0xA11C_E000_0000_0000_0000_0000_0000_0014;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("vector_inspection_docs", "embedding")
}

fn route(scope: &str) -> VectorPartitionRef {
    VectorPartitionRef::new(
        index(),
        VectorPartitionKey::from_values(&[Value::Text(scope.to_owned())])
            .expect("one TEXT partition component has one canonical key"),
    )
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|candidate| candidate == name)
        .unwrap_or_else(|| panic!("inspection contains {name}: {:?}", result.columns))
}

fn row_for(result: &QueryResult, table: &str, vector_column: &str) -> usize {
    let table_column = column(result, "table");
    let vector_column_index = column(result, "column");
    result
        .rows
        .iter()
        .position(|row| {
            row[table_column] == Value::Text(table.to_owned())
                && row[vector_column_index] == Value::Text(vector_column.to_owned())
        })
        .unwrap_or_else(|| panic!("inspection describes {table}.{vector_column}: {result:?}"))
}

fn partition_row_for_scope(result: &QueryResult, scope: &str) -> usize {
    let key_column = column(result, "partition_key");
    result
        .rows
        .iter()
        .position(|row| {
            matches!(
                row.get(key_column),
                Some(Value::Json(key))
                    if key.get("scope_id").and_then(serde_json::Value::as_str) == Some(scope)
            )
        })
        .unwrap_or_else(|| panic!("inspection contains partition {scope:?}: {result:?}"))
}

fn value<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    result
        .rows
        .get(row)
        .and_then(|values| values.get(column(result, name)))
        .unwrap_or_else(|| panic!("inspection has row {row} and {name}: {result:?}"))
}

fn text<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a str {
    match value(result, row, name) {
        Value::Text(value) => value,
        other => panic!("inspection {name} is text, got {other:?}"),
    }
}

fn int(result: &QueryResult, row: usize, name: &str) -> i64 {
    match value(result, row, name) {
        Value::Int64(value) => *value,
        other => panic!("inspection {name} is an integer, got {other:?}"),
    }
}

fn non_null(result: &QueryResult, row: usize, name: &str) {
    assert_ne!(
        value(result, row, name),
        &Value::Null,
        "{name} must report a real lifecycle fact rather than NULL: {result:?}"
    );
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("vector result projects its UUID id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            other => panic!("vector result id is UUID, got {other:?}"),
        })
        .collect()
}

fn seed_fixture(db: &Database) -> Uuid {
    db.execute(
        "CREATE TABLE vector_inspection_docs (\
            id UUID PRIMARY KEY, \
            context_id UUID CONTEXT_ID, \
            scope_id TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            embedding VECTOR(3) PARTITION_KEY (scope_id) \
                MAX_PARTITIONS 8 SEARCH_MODE INDEXED\
        )",
        &empty(),
    )
    .expect("declare a partitioned vector column");

    let source_id = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0001);
    let context = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0101);
    for (offset, scope, vector) in [
        (0_u128, SCOPE_ALPHA, vec![1.0, 0.0, 0.0]),
        (1_u128, SCOPE_ALPHA, vec![0.9, 0.1, 0.0]),
        (2_u128, SCOPE_BRAVO, vec![0.0, 1.0, 0.0]),
        (3_u128, SCOPE_BRAVO, vec![0.0, 0.9, 0.1]),
    ] {
        let id = if offset == 0 {
            source_id
        } else {
            Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0010 + offset)
        };
        db.execute(
            "INSERT INTO vector_inspection_docs \
             (id, context_id, scope_id, kind, embedding) \
             VALUES ($id, $context, $scope, $kind, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("context", Value::Uuid(context)),
                ("scope", Value::Text(scope.to_owned())),
                (
                    "kind",
                    Value::Text("filter-value-that-must-not-leak".to_owned()),
                ),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("commit one small deterministic partition member");
    }
    source_id
}

fn fixture() -> (Database, Uuid) {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let source_id = seed_fixture(&db);
    (db, source_id)
}

fn show_indexes(db: &Database) -> QueryResult {
    db.execute("SHOW VECTOR_INDEXES", &empty())
        .expect("SHOW VECTOR_INDEXES is an admin inspection surface")
}

fn show_partitions(db: &Database) -> QueryResult {
    db.execute(
        "SHOW VECTOR_PARTITIONS FOR vector_inspection_docs.embedding",
        &empty(),
    )
    .expect("SHOW VECTOR_PARTITIONS is an admin inspection surface")
}

fn drive_to_ready(db: &Database) {
    for _ in 0..MAX_FINITE_CYCLES {
        let partitions = show_partitions(db);
        if (0..partitions.rows.len()).all(|row| text(&partitions, row, "query_state") == "ready") {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance batch returns");
    }
    panic!("finite caller-driven maintenance did not publish every partition route");
}

#[test]
fn failed_maintenance_memory_admission_stays_inspectable_until_limit_raise_recovery() {
    let (db, _) = fixture();
    let used_before_build = db.accountant().usage().used;
    db.set_memory_limit(Some(used_before_build + 4_096))
        .expect("leave inspection headroom below the graph-build admission requirement");

    let refusal = db
        .run_maintenance_cycle()
        .expect("the cycle reports a local build refusal and still closes");
    assert_eq!(
        refusal.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    let reported = refusal
        .vector
        .first_failure_details
        .expect("the cycle keeps the refusing operation and byte counts");
    assert!(reported.requested_bytes.unwrap() > reported.available_bytes.unwrap());
    assert!(reported.recovery_instruction.contains("SET MEMORY_LIMIT"));
    let alpha_route = route(SCOPE_ALPHA);
    let inspected = db
        .vector_store_for_test()
        .partition_info(&alpha_route.index, &alpha_route.partition_key)
        .and_then(|partition| partition.maintenance_failure_details)
        .expect("Rust partition inspection retains the same safe failure detail");
    assert_eq!(inspected, reported);

    for _ in 0..2 {
        let partitions = show_partitions(&db);
        let alpha = partition_row_for_scope(&partitions, SCOPE_ALPHA);
        assert_eq!(text(&partitions, alpha, "maintenance_state"), "stalled");
        assert_eq!(
            text(&partitions, alpha, "maintenance_reason"),
            "memory_limit"
        );
        assert_eq!(
            text(&partitions, alpha, "recovery_action"),
            "raise_memory_limit"
        );
    }

    db.set_memory_limit(None)
        .expect("raise the limit through the existing durable setter");
    drive_to_ready(&db);
    let recovered = show_partitions(&db);
    let alpha = partition_row_for_scope(&recovered, SCOPE_ALPHA);
    assert_eq!(text(&recovered, alpha, "query_state"), "ready");
    assert_eq!(text(&recovered, alpha, "maintenance_reason"), "none");
    assert_eq!(text(&recovered, alpha, "recovery_action"), "none");
}

#[test]
fn policy_changes_clear_a_failure_when_the_healthy_route_no_longer_needs_work() {
    const POLICY_SCOPE: &str = "policy-scope";
    let directory = TempDir::new().expect("create the file-backed policy fixture directory");
    let db = Database::open(directory.path().join("policy-failure.db"))
        .expect("open the file-backed policy fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE policy_failure_docs (\
            id INT PRIMARY KEY, \
            scope_id TEXT NOT NULL, \
            embedding VECTOR(3) PARTITION_KEY (scope_id) SEARCH_MODE INDEXED \
                HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 200) \
                CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 100)\
        )",
        &empty(),
    )
    .expect("declare a low threshold on one file-backed partition");
    for id in 0..32 {
        db.execute(
            "INSERT INTO policy_failure_docs VALUES ($id, 'policy-scope', '[1,0,0]')",
            &params([("id", Value::Int64(id))]),
        )
        .expect("seed the healthy maintained partition");
    }
    assert_eq!(
        db.run_maintenance_cycle()
            .expect("publish the initial healthy route")
            .vector
            .built_partitions,
        1
    );

    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used.saturating_add(96 * 1024)))
        .expect("leave enough headroom for one write but not a replacement build");
    db.execute(
        "UPDATE policy_failure_docs SET embedding = '[0,1,0]' WHERE id = 0",
        &empty(),
    )
    .expect("cross the low consolidation threshold without losing the serving route");

    let show_policy_partitions = || {
        db.execute(
            "SHOW VECTOR_PARTITIONS FOR policy_failure_docs.embedding",
            &empty(),
        )
        .expect("inspect the policy fixture partition")
    };
    let assert_failure_visible = || {
        let partitions = show_policy_partitions();
        let partition = partition_row_for_scope(&partitions, POLICY_SCOPE);
        assert_eq!(text(&partitions, partition, "query_state"), "ready");
        assert_eq!(text(&partitions, partition, "maintenance_state"), "stalled");
        assert_eq!(
            text(&partitions, partition, "maintenance_reason"),
            "memory_limit"
        );
        assert_eq!(
            text(&partitions, partition, "recovery_action"),
            "raise_memory_limit"
        );
        let indexes = show_indexes(&db);
        let index = row_for(&indexes, "policy_failure_docs", "embedding");
        assert_eq!(text(&indexes, index, "query_state"), "ready");
        assert_eq!(text(&indexes, index, "maintenance_state"), "stalled");
        assert_eq!(int(&indexes, index, "stalled_partitions"), 1);
        assert_eq!(text(&indexes, index, "broad_route_reason"), "none");
        assert_eq!(text(&indexes, index, "broad_route_recovery_action"), "none");
    };
    let assert_idle_without_work = || {
        let partitions = show_policy_partitions();
        let partition = partition_row_for_scope(&partitions, POLICY_SCOPE);
        assert_eq!(text(&partitions, partition, "query_state"), "ready");
        assert_eq!(text(&partitions, partition, "maintenance_state"), "idle");
        assert_eq!(text(&partitions, partition, "maintenance_reason"), "none");
        assert_eq!(text(&partitions, partition, "recovery_action"), "none");
        let indexes = show_indexes(&db);
        let index = row_for(&indexes, "policy_failure_docs", "embedding");
        assert_eq!(text(&indexes, index, "query_state"), "ready");
        assert_eq!(text(&indexes, index, "maintenance_state"), "idle");
        assert_eq!(int(&indexes, index, "stalled_partitions"), 0);
        assert_eq!(text(&indexes, index, "broad_route_reason"), "none");
        assert_eq!(text(&indexes, index, "broad_route_recovery_action"), "none");
        let cycle = db
            .run_maintenance_cycle()
            .expect("a policy with no current need returns an idle finite cycle");
        assert_eq!(cycle.vector.built_partitions, 0);
        assert_eq!(cycle.vector.first_failure, None);
    };

    let refused = db
        .run_maintenance_cycle()
        .expect("record the threshold replacement refusal");
    assert_eq!(
        refused.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    assert_failure_visible();

    db.execute(
        "ALTER TABLE policy_failure_docs ALTER COLUMN embedding SET CONSOLIDATION NONE",
        &empty(),
    )
    .expect("disable automatic threshold consolidation");
    assert_idle_without_work();

    db.execute(
        "ALTER TABLE policy_failure_docs ALTER COLUMN embedding SET CONSOLIDATION \
            (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 100)",
        &empty(),
    )
    .expect("make the retained changes eligible again");
    let refused = db
        .run_maintenance_cycle()
        .expect("record a second refusal for the same retained changes");
    assert_eq!(
        refused.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    assert_failure_visible();

    db.execute(
        "ALTER TABLE policy_failure_docs ALTER COLUMN embedding SET CONSOLIDATION \
            (CHANGE_PERCENT = 100, TOMBSTONE_PERCENT = 100)",
        &empty(),
    )
    .expect("raise both thresholds above the retained changes");
    assert_idle_without_work();

    let partition_key = VectorPartitionKey::from_values(&[Value::Text(POLICY_SCOPE.to_owned())])
        .expect("construct the declared TEXT partition key");
    assert_eq!(
        db.vector_store_for_test()
            .partition_info(
                &VectorIndexRef::new("policy_failure_docs", "embedding"),
                &partition_key,
            )
            .and_then(|info| info.maintenance_failure),
        None,
        "an obsolete refusal cannot reappear when later changes become eligible"
    );
}

fn assert_no_secrets(explain: &str, secrets: &[String]) {
    for secret in secrets {
        assert!(
            !explain.contains(secret),
            "explain disclosed caller or authorization data {secret:?}: {explain}"
        );
    }
    for forbidden_count in [
        "authorized_count=",
        "candidate_count=",
        "partition_count=",
        "selected_count=",
        "selected_partitions=",
        "vector_count=",
        "live_rows=",
    ] {
        assert!(
            !explain.contains(forbidden_count),
            "explain disclosed an authorization-sensitive count through {forbidden_count:?}: {explain}"
        );
    }
}

fn fixture_row_ids() -> [Uuid; 5] {
    [
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0001),
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0011),
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0012),
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0013),
        Uuid::from_u128(HIDDEN_ROW),
    ]
}

fn secret_strings(extra: impl IntoIterator<Item = String>) -> Vec<String> {
    fixture_row_ids()
        .into_iter()
        .map(|id| id.to_string())
        .chain([
            SCOPE_HIDDEN.to_owned(),
            Uuid::from_u128(HIDDEN_CONTEXT).to_string(),
        ])
        .chain(extra)
        .collect()
}

fn assert_explain_fact(explain: &str, name: &str, expected: &str) {
    let fact = format!("{name}={expected}");
    assert!(
        explain.contains(&fact),
        "explain must report the structural fact {fact:?}: {explain}"
    );
}

fn runtime_vector_explain(result: &QueryResult) -> String {
    result
        .trace
        .vector_search
        .as_ref()
        .expect("a runtime vector query returns its own typed vector disclosure")
        .to_string()
}

fn assert_ready_layer_facts(explain: &str) {
    assert_explain_fact(explain, "partition_key", "scope_id");
    assert_explain_fact(explain, "base", "present");
    assert_explain_fact(explain, "change", "absent");
    assert_explain_fact(explain, "tail", "empty");
}

#[test]
fn vector_inspection_is_passive_and_reports_real_pre_and_post_maintenance_facts() {
    let (db, source_id) = fixture();
    let before_charge = db.accountant().usage().used;
    let before_serial = db.__debug_vector_hnsw_build_serial_for_test(index());
    let before_activity = db.__vector_passive_activity_counters_for_test();
    assert_eq!(
        before_serial, None,
        "the populated route starts before its first build"
    );

    let indexes = show_indexes(&db);
    let index_row = row_for(&indexes, "vector_inspection_docs", "embedding");
    let partitions = show_partitions(&db);
    assert_eq!(
        partitions.rows.len(),
        2,
        "two real key tuples have two detail rows"
    );

    assert_eq!(text(&indexes, index_row, "query_state"), "unavailable");
    assert_eq!(text(&indexes, index_row, "maintenance_state"), "idle");
    assert_eq!(text(&indexes, index_row, "broad_route"), "fanout");
    assert_eq!(
        text(&indexes, index_row, "broad_route_state"),
        "unavailable"
    );
    assert_eq!(
        text(&indexes, index_row, "broad_route_reason"),
        "initial_build"
    );
    assert_eq!(
        text(&indexes, index_row, "broad_route_recovery_action"),
        "run_maintenance_cycle"
    );
    assert_eq!(
        value(&indexes, index_row, "stalled_partitions"),
        &Value::Int64(0)
    );
    assert_eq!(int(&indexes, index_row, "vector_count"), 4);
    assert_eq!(int(&indexes, index_row, "live_partitions"), 2);
    assert_eq!(
        int(&indexes, index_row, "retained_partitions"),
        0,
        "live partitions are not also reported as snapshot-retained partitions"
    );
    for field in [
        "pending_inserts",
        "tombstones",
        "durable_vector_bytes",
        "charged_vector_bytes",
        "durable_index_bytes",
        "charged_index_bytes",
    ] {
        non_null(&indexes, index_row, field);
    }
    assert_eq!(
        int(&indexes, index_row, "bytes"),
        int(&indexes, index_row, "charged_vector_bytes")
            + int(&indexes, index_row, "charged_index_bytes"),
        "the compatibility byte total is the two charged categories exactly once"
    );
    assert_eq!(
        int(&indexes, index_row, "durable_vector_bytes"),
        0,
        "an in-memory database must not relabel charged vector memory as durable file bytes"
    );
    assert_eq!(
        int(&indexes, index_row, "durable_index_bytes"),
        0,
        "an in-memory database has no durable index artifact"
    );
    assert!(
        int(&indexes, index_row, "charged_vector_bytes")
            + int(&indexes, index_row, "charged_index_bytes")
            > 0,
        "the live in-memory fixture has positive charged ownership even though durable bytes are zero"
    );
    for field in [
        "oldest_base_tx",
        "newest_base_tx",
        "maintenance_vectors_total",
        "maintenance_vectors_done",
        "maintenance_vectors_remaining",
        "broad_route_base_tx",
        "broad_route_vectors_total",
        "broad_route_vectors_done",
        "broad_route_vectors_remaining",
    ] {
        assert_eq!(
            value(&indexes, index_row, field),
            &Value::Null,
            "idle work without a valid base has no invented {field}"
        );
    }

    for partition_row in 0..partitions.rows.len() {
        assert_eq!(int(&partitions, partition_row, "live_rows"), 2);
        assert_eq!(int(&partitions, partition_row, "retained_rows"), 0);
        assert_eq!(
            text(&partitions, partition_row, "query_state"),
            "unavailable"
        );
        assert_eq!(
            text(&partitions, partition_row, "availability_reason"),
            "initial_build"
        );
        assert_eq!(
            text(&partitions, partition_row, "maintenance_state"),
            "idle"
        );
        assert_eq!(
            text(&partitions, partition_row, "maintenance_reason"),
            "initial_build"
        );
        assert_eq!(
            text(&partitions, partition_row, "recovery_action"),
            "run_maintenance_cycle",
            "CallerDriven inspection tells the host to run the finite public maintenance call"
        );
        for field in [
            "pending_inserts",
            "tombstones",
            "durable_vector_bytes",
            "charged_vector_bytes",
            "durable_index_bytes",
            "charged_index_bytes",
        ] {
            non_null(&partitions, partition_row, field);
        }
        for field in [
            "base_generation",
            "base_tx",
            "maintenance_vectors_total",
            "maintenance_vectors_done",
            "maintenance_vectors_remaining",
            "maintenance_checkpoint_tx",
        ] {
            assert_eq!(
                value(&partitions, partition_row, field),
                &Value::Null,
                "an idle first-build partition has no invented {field}"
            );
        }
    }

    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        before_serial,
        "SHOW VECTOR_* must not build or load the maintained route"
    );
    assert_eq!(
        db.accountant().usage().used,
        before_charge,
        "SHOW VECTOR_* must not change charged memory merely to answer lifecycle facts"
    );
    assert_eq!(
        db.__vector_passive_activity_counters_for_test(),
        before_activity,
        "SHOW VECTOR_* cannot hide load, replay, build, repair, compaction, or eviction behind a net-zero receipt"
    );

    drive_to_ready(&db);
    let ready_charge_before = db.accountant().usage().used;
    let ready_serial_before = db.__debug_vector_hnsw_build_serial_for_test(index());
    let ready_activity_before = db.__vector_passive_activity_counters_for_test();
    let ready_generation_before = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&route(scope))
            .expect("ready partition has generation state")
    });
    let ready_indexes = show_indexes(&db);
    let ready_index_row = row_for(&ready_indexes, "vector_inspection_docs", "embedding");
    let ready_partitions = show_partitions(&db);
    assert_eq!(
        text(&ready_indexes, ready_index_row, "query_state"),
        "ready"
    );
    assert_eq!(
        text(&ready_indexes, ready_index_row, "maintenance_state"),
        "idle"
    );
    assert_eq!(
        text(&ready_indexes, ready_index_row, "broad_route"),
        "fanout"
    );
    assert_eq!(
        text(&ready_indexes, ready_index_row, "broad_route_state"),
        "ready"
    );
    assert_eq!(
        text(&ready_indexes, ready_index_row, "broad_route_reason"),
        "none"
    );
    assert_eq!(
        text(
            &ready_indexes,
            ready_index_row,
            "broad_route_recovery_action"
        ),
        "none"
    );
    for field in ["oldest_base_tx", "newest_base_tx"] {
        non_null(&ready_indexes, ready_index_row, field);
    }
    for field in [
        "pending_inserts",
        "tombstones",
        "durable_vector_bytes",
        "charged_vector_bytes",
        "durable_index_bytes",
        "charged_index_bytes",
    ] {
        non_null(&ready_indexes, ready_index_row, field);
    }
    for field in [
        "maintenance_vectors_total",
        "maintenance_vectors_done",
        "maintenance_vectors_remaining",
        "broad_route_base_tx",
        "broad_route_vectors_total",
        "broad_route_vectors_done",
        "broad_route_vectors_remaining",
    ] {
        assert_eq!(value(&ready_indexes, ready_index_row, field), &Value::Null);
    }
    for partition_row in 0..ready_partitions.rows.len() {
        assert_eq!(int(&ready_partitions, partition_row, "live_rows"), 2);
        assert_eq!(int(&ready_partitions, partition_row, "retained_rows"), 0);
        assert_eq!(
            text(&ready_partitions, partition_row, "query_state"),
            "ready"
        );
        assert_eq!(
            text(&ready_partitions, partition_row, "maintenance_state"),
            "idle"
        );
        assert_eq!(
            text(&ready_partitions, partition_row, "availability_reason"),
            "none"
        );
        assert_eq!(
            text(&ready_partitions, partition_row, "maintenance_reason"),
            "none"
        );
        assert_eq!(
            text(&ready_partitions, partition_row, "recovery_action"),
            "none"
        );
        for field in [
            "base_generation",
            "base_tx",
            "pending_inserts",
            "tombstones",
            "durable_vector_bytes",
            "charged_vector_bytes",
            "durable_index_bytes",
            "charged_index_bytes",
            "maintenance_checkpoint_tx",
        ] {
            non_null(&ready_partitions, partition_row, field);
        }
        for field in [
            "maintenance_vectors_total",
            "maintenance_vectors_done",
            "maintenance_vectors_remaining",
        ] {
            assert_eq!(value(&ready_partitions, partition_row, field), &Value::Null);
        }
    }
    assert_eq!(db.accountant().usage().used, ready_charge_before);
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        ready_serial_before
    );
    let ready_generation_after = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&route(scope))
            .expect("ready partition still has generation state")
    });
    assert_eq!(
        ready_generation_after, ready_generation_before,
        "ready inspection cannot load, rebuild, replay, or advance a generation"
    );
    assert_eq!(
        db.__vector_passive_activity_counters_for_test(),
        ready_activity_before,
        "repeated ready inspection performs no transient vector lifecycle activity"
    );

    let pending_id = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0020);
    db.execute(
        "INSERT INTO vector_inspection_docs \
         (id, context_id, scope_id, kind, embedding) \
         VALUES ($id, $context, $scope, 'pending', $embedding)",
        &params([
            ("id", Value::Uuid(pending_id)),
            (
                "context",
                Value::Uuid(Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0101)),
            ),
            ("scope", Value::Text(SCOPE_ALPHA.to_owned())),
            ("embedding", Value::Vector(vec![0.8, 0.2, 0.0])),
        ]),
    )
    .expect("commit one searchable insert after base publication");
    db.execute(
        "DELETE FROM vector_inspection_docs WHERE id = $id",
        &params([("id", Value::Uuid(source_id))]),
    )
    .expect("commit one tombstone after base publication");
    let changed_partitions = show_partitions(&db);
    let alpha = partition_row_for_scope(&changed_partitions, SCOPE_ALPHA);
    assert_eq!(int(&changed_partitions, alpha, "pending_inserts"), 1);
    assert_eq!(int(&changed_partitions, alpha, "tombstones"), 1);
    assert_eq!(text(&changed_partitions, alpha, "query_state"), "ready");
    assert_eq!(
        text(&changed_partitions, alpha, "maintenance_reason"),
        "tombstones"
    );
    let changed_indexes = show_indexes(&db);
    let changed_index_row = row_for(&changed_indexes, "vector_inspection_docs", "embedding");
    assert_eq!(
        int(&changed_indexes, changed_index_row, "pending_inserts"),
        1
    );
    assert_eq!(int(&changed_indexes, changed_index_row, "tombstones"), 1);
}

#[test]
fn file_backed_inspection_separates_positive_durable_bytes_from_charged_residency_once() {
    let root = TempDir::new().expect("temporary durable inspection store");
    let path = root.path().join("vector-inspection-durable.redb");
    {
        let db = Database::open(&path).expect("open the durable inspection fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        seed_fixture(&db);
        drive_to_ready(&db);
        db.close().expect("seal the durable vector generations");
    }

    let db = Database::open(&path).expect("reopen the durable inspection fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let charge_before = db.accountant().usage().used;
    let activity_before = db.__vector_passive_activity_counters_for_test();
    let generations_before = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&route(scope))
            .expect("reopened partition has dormant generation state")
    });

    let indexes = show_indexes(&db);
    let index_row = row_for(&indexes, "vector_inspection_docs", "embedding");
    let partitions = show_partitions(&db);
    let indexes_again = show_indexes(&db);
    let partitions_again = show_partitions(&db);
    assert_eq!(indexes_again.columns, indexes.columns);
    assert_eq!(indexes_again.rows, indexes.rows);
    assert_eq!(partitions_again.columns, partitions.columns);
    assert_eq!(partitions_again.rows, partitions.rows);

    let durable_vector = int(&indexes, index_row, "durable_vector_bytes");
    let durable_index = int(&indexes, index_row, "durable_index_bytes");
    let charged_vector = int(&indexes, index_row, "charged_vector_bytes");
    let charged_index = int(&indexes, index_row, "charged_index_bytes");
    assert!(
        durable_vector > 0 && durable_index > 0,
        "a reopened file reports its saved raw vectors and graph artifacts as positive durable bytes"
    );
    assert!(
        charged_vector + charged_index > 0,
        "the lightweight reopened registry remains positively charged without relabelling it as file bytes"
    );
    assert_eq!(
        int(&indexes, index_row, "bytes"),
        charged_vector + charged_index,
        "the compatibility total remains charged memory only"
    );
    for (summary_field, partition_field) in [
        ("durable_vector_bytes", "durable_vector_bytes"),
        ("durable_index_bytes", "durable_index_bytes"),
        ("charged_vector_bytes", "charged_vector_bytes"),
        ("charged_index_bytes", "charged_index_bytes"),
    ] {
        let detail_sum: i64 = (0..partitions.rows.len())
            .map(|row| int(&partitions, row, partition_field))
            .sum();
        assert_eq!(
            int(&indexes, index_row, summary_field),
            detail_sum,
            "the whole-index {summary_field} is the partition detail sum exactly once"
        );
    }
    assert_eq!(db.accountant().usage().used, charge_before);
    assert_eq!(
        db.__vector_passive_activity_counters_for_test(),
        activity_before,
        "repeated file-backed inspection cannot load dormant raw bodies or graphs and then evict them"
    );
    let generations_after = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&route(scope))
            .expect("inspection leaves reopened generation state present")
    });
    assert_eq!(generations_after, generations_before);
    db.close().expect("close durable inspection verification");
}

#[test]
fn active_inspection_reports_one_atomic_progress_snapshot_without_advancing_it() {
    let (db, _) = fixture();
    let db = Arc::new(db);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&route(SCOPE_ALPHA), 1);
    let worker_db = Arc::clone(&db);
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx
            .send(worker_db.run_maintenance_cycle())
            .expect("maintenance result receiver remains live");
    });
    if !pause.wait_until_reached(PROOF_WAIT) {
        pause.release();
        let _ = done_rx.recv_timeout(PROOF_WAIT);
        let _ = worker.join();
        panic!("the finite maintenance cycle did not reach its deterministic progress point");
    }

    // Release the worker even if one of the metadata-only observations
    // panics. Assertions run only after the paused thread is free.
    let observed = catch_unwind(AssertUnwindSafe(|| {
        let charge_before = db.accountant().usage().used;
        let activity_before = db.__vector_passive_activity_counters_for_test();
        let generation_before = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
            db.vector_store_for_test()
                .partition_graph_generation_status(&route(scope))
                .unwrap_or_else(|| panic!("the partition {scope:?} has generation state"))
        });
        let partitions = show_partitions(&db);
        let indexes = show_indexes(&db);
        let charge_after = db.accountant().usage().used;
        let activity_after = db.__vector_passive_activity_counters_for_test();
        let generation_after = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
            db.vector_store_for_test()
                .partition_graph_generation_status(&route(scope))
                .unwrap_or_else(|| panic!("inspection leaves partition {scope:?} present"))
        });
        (
            charge_before,
            activity_before,
            generation_before,
            partitions,
            indexes,
            charge_after,
            activity_after,
            generation_after,
        )
    }));
    pause.release();
    let maintenance_result = done_rx
        .recv_timeout(PROOF_WAIT)
        .expect("released finite maintenance returns");
    worker.join().expect("maintenance worker does not panic");
    maintenance_result.expect("released finite maintenance succeeds");
    let (
        charge_before,
        activity_before,
        generation_before,
        partitions,
        indexes,
        charge_after,
        activity_after,
        generation_after,
    ) = match observed {
        Ok(observed) => observed,
        Err(payload) => resume_unwind(payload),
    };

    let alpha = partition_row_for_scope(&partitions, SCOPE_ALPHA);
    let index_row = row_for(&indexes, "vector_inspection_docs", "embedding");
    assert_eq!(text(&partitions, alpha, "query_state"), "unavailable");
    assert_eq!(
        text(&partitions, alpha, "availability_reason"),
        "initial_build"
    );
    assert_eq!(text(&partitions, alpha, "maintenance_state"), "building");
    assert_eq!(
        text(&partitions, alpha, "maintenance_reason"),
        "initial_build"
    );
    assert_eq!(int(&partitions, alpha, "maintenance_vectors_total"), 2);
    assert_eq!(int(&partitions, alpha, "maintenance_vectors_done"), 1);
    assert_eq!(int(&partitions, alpha, "maintenance_vectors_remaining"), 1);
    non_null(&partitions, alpha, "maintenance_checkpoint_tx");
    assert_eq!(
        text(&partitions, alpha, "recovery_action"),
        "run_maintenance_cycle"
    );
    assert_eq!(text(&indexes, index_row, "query_state"), "unavailable");
    assert_eq!(text(&indexes, index_row, "maintenance_state"), "building");
    assert_eq!(int(&indexes, index_row, "maintenance_vectors_total"), 2);
    assert_eq!(int(&indexes, index_row, "maintenance_vectors_done"), 1);
    assert_eq!(int(&indexes, index_row, "maintenance_vectors_remaining"), 1);
    assert_eq!(int(&indexes, index_row, "unavailable_partitions"), 2);
    assert_eq!(int(&indexes, index_row, "stalled_partitions"), 0);
    assert_eq!(text(&indexes, index_row, "broad_route"), "fanout");
    assert_eq!(
        text(&indexes, index_row, "broad_route_state"),
        "unavailable"
    );
    assert_eq!(
        text(&indexes, index_row, "broad_route_reason"),
        "initial_build"
    );
    assert_eq!(
        text(&indexes, index_row, "broad_route_recovery_action"),
        "run_maintenance_cycle"
    );
    for field in [
        "broad_route_base_tx",
        "broad_route_vectors_total",
        "broad_route_vectors_done",
        "broad_route_vectors_remaining",
    ] {
        assert_eq!(
            value(&indexes, index_row, field),
            &Value::Null,
            "fan-out inspection cannot invent maintained broad-route progress"
        );
    }
    let bravo = partition_row_for_scope(&partitions, SCOPE_BRAVO);
    assert_eq!(text(&partitions, bravo, "maintenance_state"), "idle");
    for field in [
        "maintenance_vectors_total",
        "maintenance_vectors_done",
        "maintenance_vectors_remaining",
    ] {
        assert_eq!(
            value(&partitions, bravo, field),
            &Value::Null,
            "the untouched partition remains one idle row in the same atomic snapshot"
        );
    }
    assert_eq!(charge_after, charge_before);
    assert_eq!(
        activity_after, activity_before,
        "inspection cannot perform hidden lifecycle work while reporting the paused snapshot"
    );
    assert_eq!(
        generation_after, generation_before,
        "SHOW cannot advance the paused work it is reporting"
    );
}

#[test]
fn engine_owned_ready_inspection_reports_no_recovery_without_inspecting_into_a_build() {
    let (db, _) = fixture();
    drive_to_ready(&db);
    let serial_before = db.__debug_vector_hnsw_build_serial_for_test(index());
    db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
    let charge_before = db.accountant().usage().used;
    let activity_before = db.__vector_passive_activity_counters_for_test();
    let generations_before = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&route(scope))
            .expect("ready engine-owned partition has generation state")
    });

    let partitions = show_partitions(&db);
    for partition_row in 0..partitions.rows.len() {
        assert_eq!(
            text(&partitions, partition_row, "recovery_action"),
            "none",
            "a healthy ready partition does not tell the operator to repair anything"
        );
    }
    let indexes = show_indexes(&db);
    let index_row = row_for(&indexes, "vector_inspection_docs", "embedding");
    assert_eq!(
        text(&indexes, index_row, "broad_route_recovery_action"),
        "none",
        "the healthy whole-index broad route reports no recovery action"
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        serial_before,
        "inspection itself must not be the action that builds an EngineOwned route"
    );
    assert_eq!(
        db.accountant().usage().used,
        charge_before,
        "engine-owned inspection cannot load or allocate route state"
    );
    assert_eq!(
        db.__vector_passive_activity_counters_for_test(),
        activity_before,
        "engine-owned inspection cannot trigger and then undo vector lifecycle work"
    );
    let generations_after = [SCOPE_ALPHA, SCOPE_BRAVO].map(|scope| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&route(scope))
            .expect("engine-owned inspection leaves generation state present")
    });
    assert_eq!(
        generations_after, generations_before,
        "engine-owned inspection cannot replay or advance a ready generation"
    );
}

#[test]
fn the_same_pending_partition_reports_the_owner_of_maintenance_on_both_show_surfaces() {
    let (db, _) = fixture();
    let caller_partitions = show_partitions(&db);
    let caller_indexes = show_indexes(&db);
    let caller_index_row = row_for(&caller_indexes, "vector_inspection_docs", "embedding");
    assert!(
        caller_partitions
            .rows
            .iter()
            .enumerate()
            .all(|(row, _)| text(&caller_partitions, row, "query_state") == "unavailable")
    );
    assert!(caller_partitions.rows.iter().enumerate().all(|(row, _)| {
        text(&caller_partitions, row, "recovery_action") == "run_maintenance_cycle"
    }));
    assert_eq!(
        text(
            &caller_indexes,
            caller_index_row,
            "broad_route_recovery_action"
        ),
        "run_maintenance_cycle"
    );

    db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
    let engine_partitions = show_partitions(&db);
    let engine_indexes = show_indexes(&db);
    let engine_index_row = row_for(&engine_indexes, "vector_inspection_docs", "embedding");
    assert_eq!(
        engine_partitions.rows,
        caller_partitions
            .rows
            .iter()
            .cloned()
            .map(|mut row| {
                row[column(&caller_partitions, "recovery_action")] =
                    Value::Text("wait_for_automatic_work".to_owned());
                row
            })
            .collect::<Vec<_>>(),
        "changing maintenance ownership changes only the pending partition's recovery advice"
    );
    assert_eq!(
        text(
            &engine_indexes,
            engine_index_row,
            "broad_route_recovery_action"
        ),
        "wait_for_automatic_work"
    );
}

#[test]
fn vector_explain_names_mode_scope_route_merge_and_safe_filter_recovery_without_secrets() {
    let (db, source_id) = fixture();
    db.execute(
        "CREATE INDEX inspection_context ON vector_inspection_docs (context_id)",
        &empty(),
    )
    .unwrap();
    let hidden_context = Uuid::from_u128(HIDDEN_CONTEXT);
    db.execute(
        "INSERT INTO vector_inspection_docs \
         (id, context_id, scope_id, kind, embedding) \
         VALUES ($id, $context, $scope, $kind, $embedding)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(HIDDEN_ROW))),
            ("context", Value::Uuid(hidden_context)),
            ("scope", Value::Text(SCOPE_HIDDEN.to_owned())),
            (
                "kind",
                Value::Text("filter-value-that-must-not-leak".to_owned()),
            ),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .expect("seed one partition the scoped caller is not authorized to inspect");
    drive_to_ready(&db);
    let context = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0101);
    let scoped = db.scoped_with_contexts(BTreeSet::from([ContextId::new(context)]));
    let source = source_id.to_string();
    let filter_value = "filter-value-that-must-not-leak".to_owned();
    let literal_vector = "[0.1234567,0.2345678,0.3456789]".to_owned();
    let explain_passively = |sql: &str| {
        let charge_before = db.accountant().usage().used;
        let serial_before = db.__debug_vector_hnsw_build_serial_for_test(index());
        let activity_before = db.__vector_passive_activity_counters_for_test();
        let scan_before = db.__relational_scan_rows_touched();
        let generations_before = [SCOPE_ALPHA, SCOPE_BRAVO, SCOPE_HIDDEN].map(|scope| {
            db.vector_store_for_test()
                .partition_graph_generation_status(&route(scope))
                .expect("ready partition has generation state before explain")
        });
        let explain = scoped
            .explain(sql)
            .expect("the scoped explain is metadata-only");
        // A passive explain on a Context-restricted handle must never
        // derive its candidates by scanning the whole table: neither
        // `vector_policy_facts_for_explain` nor `vector_search_disclosure`
        // may walk `effective_read_candidates` for a restricted `.explain`.
        assert_eq!(
            db.__relational_scan_rows_touched(),
            scan_before,
            "a passive explain must never scan the table to derive a restricted \
             reader's candidates: {sql}"
        );
        assert_eq!(
            db.accountant().usage().used,
            charge_before,
            "explain cannot load a graph or vector body"
        );
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(index()),
            serial_before,
            "explain cannot build a route"
        );
        let generations_after = [SCOPE_ALPHA, SCOPE_BRAVO, SCOPE_HIDDEN].map(|scope| {
            db.vector_store_for_test()
                .partition_graph_generation_status(&route(scope))
                .expect("ready partition has generation state after explain")
        });
        assert_eq!(
            generations_after, generations_before,
            "explain cannot replay, compact, repair, or advance a generation"
        );
        assert_eq!(
            db.__vector_passive_activity_counters_for_test(),
            activity_before,
            "explain cannot load and evict, replay, repair, or compact behind an unchanged net receipt"
        );
        explain
    };

    for (mode_clause, requested_mode, resolved_mode, expected_route) in [
        ("USE VECTOR EXACT", "EXACT", "EXACT", "exact"),
        (
            "USE VECTOR INDEXED",
            "INDEXED",
            "INDEXED",
            "filtered-indexed",
        ),
        ("USE VECTOR AUTO", "AUTO", "EXACT", "exact"),
        ("", "INDEXED", "INDEXED", "filtered-indexed"),
    ] {
        let sql = format!(
            "SELECT id FROM vector_inspection_docs \
             WHERE scope_id = '{SCOPE_ALPHA}' \
             ORDER BY embedding <=> ROW_VECTOR('vector_inspection_docs','embedding','{source}') \
             {mode_clause} LIMIT 2"
        );
        let explain = explain_passively(&sql);
        assert_explain_fact(&explain, "requested_mode", requested_mode);
        assert_explain_fact(&explain, "resolved_mode", resolved_mode);
        assert_explain_fact(&explain, "scope", "one");
        assert_explain_fact(&explain, "route", expected_route);
        assert_explain_fact(&explain, "merge", "global");
        assert_explain_fact(
            &explain,
            "residual",
            if resolved_mode == "EXACT" {
                "none"
            } else {
                "bounded"
            },
        );
        assert_explain_fact(&explain, "refusal", "none");
        assert_ready_layer_facts(&explain);
        if requested_mode == "EXACT" {
            assert!(
                !explain.contains("HNSW"),
                "explicit EXACT must never be described as an HNSW route: {explain}"
            );
        }
        assert!(
            explain.contains("<redacted>"),
            "a literal row-source key and partition value retain only structural redaction markers: {explain}"
        );
        assert_no_secrets(
            &explain,
            &secret_strings([source.clone(), SCOPE_ALPHA.to_owned(), context.to_string()]),
        );

        let runtime = scoped
            .execute(
                &format!(
                    "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope \
                     ORDER BY embedding <=> $query {mode_clause} LIMIT 2"
                ),
                &params([
                    ("scope", Value::Text(SCOPE_ALPHA.to_owned())),
                    ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
                ]),
            )
            .expect("the ready maintained route executes through the ordinary production door");
        let runtime_explain = runtime_vector_explain(&runtime);
        assert_explain_fact(&runtime_explain, "requested_mode", requested_mode);
        assert_explain_fact(&runtime_explain, "resolved_mode", resolved_mode);
        assert_explain_fact(&runtime_explain, "scope", "one");
        assert_explain_fact(&runtime_explain, "route", expected_route);
        assert_explain_fact(&runtime_explain, "merge", "global");
        assert_explain_fact(
            &runtime_explain,
            "residual",
            if resolved_mode == "EXACT" {
                "none"
            } else {
                "bounded"
            },
        );
        assert_explain_fact(&runtime_explain, "refusal", "none");
        assert_ready_layer_facts(&runtime_explain);
        if requested_mode == "EXACT" {
            assert!(
                !runtime_explain.contains("HNSW"),
                "explicit EXACT runtime explain must never describe HNSW: {runtime_explain}"
            );
        }
        assert!(
            runtime_explain.contains("<vector>"),
            "a bound query vector keeps only its structural vector placeholder: {runtime_explain}"
        );
        assert_no_secrets(
            &runtime_explain,
            &secret_strings([
                SCOPE_ALPHA.to_owned(),
                "[1.0, 0.0, 0.0]".to_owned(),
                context.to_string(),
            ]),
        );
    }

    for (predicate, scope_shape) in [
        (format!("scope_id = '{SCOPE_ALPHA}'"), "one"),
        (
            format!("scope_id = '{SCOPE_ALPHA}' OR scope_id = '{SCOPE_BRAVO}'"),
            "several",
        ),
        ("TRUE".to_owned(), "all"),
    ] {
        let explain = explain_passively(&format!(
            "SELECT id FROM vector_inspection_docs WHERE {predicate} \
                 ORDER BY embedding <=> {literal_vector} USE VECTOR EXACT LIMIT 2"
        ));
        assert_explain_fact(&explain, "scope", scope_shape);
        assert_explain_fact(&explain, "requested_mode", "EXACT");
        assert_explain_fact(&explain, "resolved_mode", "EXACT");
        assert_explain_fact(&explain, "route", "exact");
        assert_explain_fact(&explain, "merge", "global");
        assert_explain_fact(&explain, "residual", "none");
        assert_explain_fact(&explain, "refusal", "none");
        assert_ready_layer_facts(&explain);
        assert!(
            !explain.contains("HNSW"),
            "every explicit EXACT scope shape remains an exact route: {explain}"
        );
        assert!(
            explain.contains("<vector>"),
            "a literal query vector keeps only its structural vector placeholder: {explain}"
        );
        assert_no_secrets(
            &explain,
            &secret_strings([
                literal_vector.clone(),
                "0.1234567".to_owned(),
                "0.2345678".to_owned(),
                "0.3456789".to_owned(),
                SCOPE_ALPHA.to_owned(),
                SCOPE_BRAVO.to_owned(),
                context.to_string(),
            ]),
        );
    }

    let bound_row_source = scoped
        .execute(
            "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope \
             ORDER BY embedding <=> ROW_VECTOR('vector_inspection_docs','embedding',$source) \
             USE VECTOR EXACT LIMIT 2",
            &params([
                ("scope", Value::Text(SCOPE_ALPHA.to_owned())),
                ("source", Value::Uuid(source_id)),
            ]),
        )
        .expect("the runtime row-source path resolves a bound key without disclosing it");
    let bound_row_source_explain = runtime_vector_explain(&bound_row_source);
    assert_explain_fact(&bound_row_source_explain, "requested_mode", "EXACT");
    assert_explain_fact(&bound_row_source_explain, "resolved_mode", "EXACT");
    assert_explain_fact(&bound_row_source_explain, "route", "exact");
    assert_explain_fact(&bound_row_source_explain, "scope", "one");
    assert_explain_fact(&bound_row_source_explain, "merge", "global");
    assert_explain_fact(&bound_row_source_explain, "residual", "none");
    assert_explain_fact(&bound_row_source_explain, "refusal", "none");
    assert_ready_layer_facts(&bound_row_source_explain);
    assert!(
        bound_row_source_explain.contains("<redacted>"),
        "the runtime row-source key leaves only a structural redaction marker: {bound_row_source_explain}"
    );
    assert_no_secrets(
        &bound_row_source_explain,
        &secret_strings([source.clone(), SCOPE_ALPHA.to_owned(), context.to_string()]),
    );

    let filtered_sql = format!(
        "SELECT id FROM vector_inspection_docs \
             WHERE kind = '{filter_value}' \
             ORDER BY embedding <=> ROW_VECTOR('vector_inspection_docs','embedding','{source}') \
             USE VECTOR INDEXED LIMIT 2"
    );
    // The access index itself supplies a bounded candidate route even when
    // the SQL predicate has no dedicated index. Remove it to exercise the
    // missing candidate route and its ordinary SQL recovery.
    let access_index_route = explain_passively(&filtered_sql);
    assert_explain_fact(&access_index_route, "residual", "bounded");
    assert_explain_fact(&access_index_route, "refusal", "none");
    db.execute(
        "DROP INDEX inspection_context ON vector_inspection_docs",
        &empty(),
    )
    .unwrap();
    let refusal = explain_passively(&filtered_sql);
    assert_explain_fact(&refusal, "requested_mode", "INDEXED");
    assert_explain_fact(&refusal, "resolved_mode", "INDEXED");
    assert_explain_fact(&refusal, "scope", "all");
    assert_explain_fact(&refusal, "route", "filtered-indexed");
    assert_explain_fact(&refusal, "merge", "global");
    assert_explain_fact(&refusal, "residual", "unsupported");
    assert_explain_fact(&refusal, "refusal", "filtered_route_unavailable");
    assert_ready_layer_facts(&refusal);
    assert_explain_fact(
        &refusal,
        "recovery",
        "CREATE INDEX vector_inspection_docs_kind_idx ON vector_inspection_docs (kind)",
    );
    assert!(
        refusal.contains("<redacted>"),
        "the refused row-source/filter query keeps only structural redaction markers: {refusal}"
    );
    assert_no_secrets(
        &refusal,
        &secret_strings([source.clone(), filter_value.clone(), context.to_string()]),
    );

    db.execute(
        "CREATE INDEX vector_inspection_docs_kind_idx ON vector_inspection_docs (kind)",
        &empty(),
    )
    .expect("install the exact ordinary index recommended by explain");
    let recovered = explain_passively(&filtered_sql);
    assert_explain_fact(&recovered, "requested_mode", "INDEXED");
    assert_explain_fact(&recovered, "resolved_mode", "INDEXED");
    assert_explain_fact(&recovered, "scope", "all");
    assert_explain_fact(&recovered, "route", "filtered-indexed");
    assert_explain_fact(&recovered, "merge", "global");
    assert_explain_fact(&recovered, "residual", "bounded");
    assert_explain_fact(&recovered, "refusal", "none");
    assert_explain_fact(&recovered, "recovery", "none");
    assert_ready_layer_facts(&recovered);
    assert_no_secrets(
        &recovered,
        &secret_strings([source, filter_value, context.to_string()]),
    );

    db.__reset_relational_scan_rows_touched();
    db.__reset_relational_index_entries_touched();
    let recovered_runtime = scoped.execute(&filtered_sql, &empty()).expect(
        "the query that explain marked recovered executes through the bounded filtered route",
    );
    let recovered_ids = ids(&recovered_runtime);
    assert!(
        !recovered_ids.is_empty(),
        "the recovered route returns the authorized matching rows"
    );
    assert!(
        !recovered_ids.contains(&Uuid::from_u128(HIDDEN_ROW)),
        "the recovered broad route cannot return the unauthorized partition"
    );
    let recovered_runtime_explain = runtime_vector_explain(&recovered_runtime);
    assert_explain_fact(&recovered_runtime_explain, "requested_mode", "INDEXED");
    assert_explain_fact(&recovered_runtime_explain, "resolved_mode", "INDEXED");
    assert_explain_fact(&recovered_runtime_explain, "route", "filtered-indexed");
    assert_explain_fact(&recovered_runtime_explain, "scope", "all");
    assert_explain_fact(&recovered_runtime_explain, "residual", "bounded");
    assert_explain_fact(&recovered_runtime_explain, "refusal", "none");
    assert_ready_layer_facts(&recovered_runtime_explain);
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "the recovered filtered route uses the recommended index rather than scanning the table"
    );
    assert!(
        db.__relational_index_entries_touched() > 0,
        "the recovered filtered route actually reads bounded ordinary-index postings"
    );
    assert_no_secrets(
        &recovered_runtime_explain,
        &secret_strings([
            SCOPE_ALPHA.to_owned(),
            SCOPE_BRAVO.to_owned(),
            context.to_string(),
        ]),
    );
}
/// A passive `.explain` runs nothing and binds nothing, so it must accept
/// placeholders in a partition predicate: neither the vector disclosure nor
/// the policy facts of the same explain may fail with
/// `Error::NotFound("missing parameter: scope")` or silently drop that
/// failure. Legs that need no unbound placeholder (the no-predicate "all"
/// shape, and execution of the same statement) are controls.
#[test]
fn explaining_a_prepared_partition_predicate_binds_nothing_and_names_the_shape() {
    const VEC: &str = "[0.1234567,0.2345678,0.3456789]";
    let (db, _source_id) = fixture();
    drive_to_ready(&db);

    let named_sql = format!(
        "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope \
         ORDER BY embedding <=> {VEC} LIMIT 2"
    );
    let second_named_sql = format!(
        "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope_alt \
         ORDER BY embedding <=> {VEC} LIMIT 2"
    );
    let literal_sql = format!(
        "SELECT id FROM vector_inspection_docs WHERE scope_id = '{SCOPE_ALPHA}' \
         ORDER BY embedding <=> {VEC} LIMIT 2"
    );
    let in_sql = "SELECT id FROM vector_inspection_docs WHERE scope_id IN ($a, $b) \
         ORDER BY embedding <=> [0.1234567,0.2345678,0.3456789] LIMIT 2"
        .to_owned();
    let no_predicate_sql =
        format!("SELECT id FROM vector_inspection_docs ORDER BY embedding <=> {VEC} LIMIT 2");

    let explain_passively = |sql: &str| -> String {
        let charge_before = db.accountant().usage().used;
        let serial_before = db.__debug_vector_hnsw_build_serial_for_test(index());
        let activity_before = db.__vector_passive_activity_counters_for_test();
        let explain = db
            .explain(sql)
            .unwrap_or_else(|error| panic!("explain must bind nothing and never error on an unbound partition predicate: {error}\nSQL: {sql}"));
        assert_eq!(
            db.accountant().usage().used,
            charge_before,
            "a prepared-predicate explain cannot load a graph or vector body"
        );
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(index()),
            serial_before,
            "a prepared-predicate explain cannot build a route"
        );
        assert_eq!(
            db.__vector_passive_activity_counters_for_test(),
            activity_before,
            "a prepared-predicate explain cannot load, replay, repair, or compact behind an unchanged net receipt"
        );
        explain
    };

    // Leg 1 -- named placeholder.
    let named_explain = explain_passively(&named_sql);
    assert_explain_fact(&named_explain, "scope", "one");
    assert_explain_fact(&named_explain, "refusal", "none");
    assert_explain_fact(&named_explain, "residual", "none");

    // Leg 2 -- a second named placeholder spelling; identical facts.
    let second_named_explain = explain_passively(&second_named_sql);
    assert_explain_fact(&second_named_explain, "scope", "one");
    assert_explain_fact(&second_named_explain, "refusal", "none");
    assert_explain_fact(&second_named_explain, "residual", "none");

    // Leg 3 -- the structured disclosure through the unscoped admin handle.
    for sql in [&named_sql, &second_named_sql] {
        let explained: ExplainOutput = db.explain_output(sql).unwrap_or_else(|error| {
            panic!("explain_output must bind nothing: {error}\nSQL: {sql}")
        });
        let disclosure = explained
            .vector_search
            .expect("a vector-similarity SELECT reports a vector disclosure");
        assert_eq!(disclosure.scope, VectorSearchScopeShape::One);
        assert_eq!(
            disclosure.partition_key_columns,
            vec!["scope_id".to_owned()]
        );
    }

    // Grammar order is `ORDER BY ... USE VECTOR ... LIMIT`, so the mode
    // clause is inserted before the trailing `LIMIT 2`, not appended after it.
    let insert_mode_before_limit = |sql: &str, mode_clause: &str| -> String {
        sql.replacen(" LIMIT 2", &format!(" {mode_clause} LIMIT 2"), 1)
    };

    // Leg 4 -- route parity under an explicit mode: a placeholder explain and
    // the same statement with the literal inlined must report the same
    // scope, route, residual, and refusal. AUTO and the empty clause are
    // deliberately excluded: for an unbound key their allowed-vector count is
    // reported as unresolved until bind rather than counted (leg 5), so a
    // placeholder explain has no literal-inlined twin to match.
    for mode_clause in ["USE VECTOR EXACT", "USE VECTOR INDEXED"] {
        let prepared = explain_passively(&insert_mode_before_limit(&named_sql, mode_clause));
        let literal = explain_passively(&insert_mode_before_limit(&literal_sql, mode_clause));
        for fact in ["scope", "route", "residual", "refusal"] {
            let prepared_value = extract_fact(&prepared, fact);
            let literal_value = extract_fact(&literal, fact);
            assert_eq!(
                prepared_value, literal_value,
                "{fact} must agree between a prepared and a literal-inlined explain under {mode_clause}"
            );
        }
    }

    // Leg 5 -- resolved-mode correctness for an unbound key. With no `USE VECTOR`
    // clause, the query takes the column's own declared mode (`SEARCH_MODE INDEXED`,
    // fixture above) regardless of the key being unbound -- this bound case is
    // unchanged. Under an explicit `USE VECTOR AUTO` clause, the planner resolves
    // partition scope only after parameters are bound (partition selection
    // produces one global answer), so an unbound key leaves AUTO
    // unresolved-until-bind rather than resolving it from a count taken over a scope
    // the caller never named -- there is no trustworthy count to derive a route from,
    // so the old count-vs-threshold derivation no longer applies here.
    let declared_mode_explained = db.explain_output(&named_sql).unwrap_or_else(|error| {
        panic!("explain_output must bind nothing: {error}\nSQL: {named_sql}")
    });
    let declared_mode_disclosure = declared_mode_explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(
        declared_mode_disclosure.resolved_mode,
        VectorSearchMode::Indexed,
        "with no USE VECTOR clause, resolved mode follows the column's own declared mode even \
         with an unbound key: {declared_mode_disclosure:?}"
    );

    let explicit_auto_sql = insert_mode_before_limit(&named_sql, "USE VECTOR AUTO");
    let explicit_auto_explained = db
        .explain_output(&explicit_auto_sql)
        .unwrap_or_else(|error| {
            panic!("explain_output must bind nothing: {error}\nSQL: {explicit_auto_sql}")
        });
    let explicit_auto_disclosure = explicit_auto_explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(
        explicit_auto_disclosure.resolved_mode,
        VectorSearchMode::Auto,
        "an explicit USE VECTOR AUTO over an unbound key must leave the route \
         unresolved-until-bind rather than resolving it to EXACT or INDEXED from a count taken \
         over a scope the caller never named: {explicit_auto_disclosure:?}"
    );
    let explicit_auto_reason = explicit_auto_disclosure
        .fallback
        .as_deref()
        .or(explicit_auto_disclosure.refusal.as_deref())
        .unwrap_or("");
    assert!(
        ["unresolved", "unbound", "unbind", "bind"]
            .iter()
            .any(|needle| explicit_auto_reason.to_lowercase().contains(needle)),
        "an unresolved AUTO route for an unbound partition key must carry a typed reason \
         explaining why: fallback={:?} refusal={:?}",
        explicit_auto_disclosure.fallback,
        explicit_auto_disclosure.refusal
    );

    // Leg 6 -- layer facts are the whole-column facts, not the empty-prefix
    // facts: an unbound placeholder must not collapse base/change/tail to
    // absent.
    let unpartitioned_explain = explain_passively(&no_predicate_sql);
    assert!(
        unpartitioned_explain.contains("base=present")
            || unpartitioned_explain.contains("base=absent"),
        "control: the unpartitioned explain reports a base fact: {unpartitioned_explain}"
    );
    for fact in ["base", "change", "tail"] {
        let prepared_value = extract_fact(&named_explain, fact);
        let unpartitioned_value = extract_fact(&unpartitioned_explain, fact);
        assert_eq!(
            prepared_value, unpartitioned_value,
            "{fact} must match the whole-column explain, not render as the empty-prefix facts"
        );
    }
    assert!(
        !(extract_fact(&named_explain, "base") == "absent"
            && extract_fact(&named_explain, "change") == "absent"
            && extract_fact(&named_explain, "tail") == "empty"),
        "an unbound partition predicate must not render every layer as absent: {named_explain}"
    );

    // Leg 7 -- shape, not identity.
    let in_explain = explain_passively(&in_sql);
    assert_explain_fact(&in_explain, "scope", "several");
    let all_explain = explain_passively(&no_predicate_sql);
    assert_explain_fact(&all_explain, "scope", "all");

    // Leg 8 -- nothing leaks.
    assert_no_secrets(
        &named_explain,
        &secret_strings([SCOPE_ALPHA.to_owned(), SCOPE_BRAVO.to_owned()]),
    );
    assert_no_secrets(
        &second_named_explain,
        &secret_strings([SCOPE_ALPHA.to_owned(), SCOPE_BRAVO.to_owned()]),
    );

    // Leg 9 -- control: execution of the same unbound statement is unchanged.
    let execution_error = db
        .execute(&named_sql, &empty())
        .expect_err("an unbound partition predicate must still fail to execute");
    assert!(
        execution_error
            .to_string()
            .contains("missing parameter: scope"),
        "execution keeps the ordinary missing-parameter refusal: {execution_error}"
    );
    let second_named_execution_error = db
        .execute(&second_named_sql, &empty())
        .expect_err("an unbound second named partition predicate must still fail to execute");
    assert!(
        second_named_execution_error
            .to_string()
            .contains("missing parameter"),
        "second named execution keeps the ordinary missing-parameter refusal: {second_named_execution_error}"
    );
}

/// Pull one `name=value` structural fact out of a rendered `.explain` block.
fn extract_fact<'a>(explain: &'a str, name: &str) -> &'a str {
    let needle = format!("{name}=");
    let start = explain
        .find(&needle)
        .unwrap_or_else(|| panic!("explain must report the structural fact {name:?}: {explain}"))
        + needle.len();
    explain[start..]
        .split(|c: char| c.is_whitespace())
        .next()
        .unwrap_or("")
}

#[test]
fn restricted_explain_admits_only_visible_identities_at_equal_headroom() {
    for indexed in [false, true] {
        let mut expected = None;
        for hidden in [0, 64, 2048] {
            let db = Database::open_memory();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            db.execute("CREATE TABLE private_vectors (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, embedding VECTOR(3))", &empty()).unwrap();
            if indexed {
                db.execute(
                    "CREATE INDEX by_context ON private_vectors(context_id)",
                    &empty(),
                )
                .unwrap();
            }
            let visible = Uuid::from_u128(1);
            let tx = db.begin().unwrap();
            for id in 0..=hidden {
                db.execute_in_tx(
                    tx,
                    "INSERT INTO private_vectors VALUES ($id, $context, $embedding)",
                    &params([
                        ("id", Value::Int64(id)),
                        (
                            "context",
                            Value::Uuid(if id == 0 { visible } else { Uuid::from_u128(2) }),
                        ),
                        ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
                    ]),
                )
                .unwrap();
            }
            db.commit(tx).unwrap();
            let scoped = db.scoped_with_contexts(BTreeSet::from([ContextId::new(visible)]));
            let resident = db.accountant().usage().used;
            let activity = db.__vector_passive_activity_counters_for_test();
            let scans = db.__relational_scan_rows_touched();
            let mut outcomes = Vec::new();
            for headroom in [0, 543, 544, 1024, 4096] {
                db.set_memory_limit(Some(resident + headroom)).unwrap();
                let result = scoped.explain_output("SELECT id FROM private_vectors ORDER BY embedding <=> [1,0,0] USE VECTOR EXACT LIMIT 1");
                match result {
                    Ok(explain) => {
                        assert!(headroom >= 544);
                        assert_eq!(
                            explain
                                .vector_search
                                .as_ref()
                                .unwrap()
                                .aggregate_allowed_vectors,
                            Some(1)
                        );
                        outcomes.push(format!("{explain:?}"));
                    }
                    Err(error) => {
                        assert!(
                            headroom < 544,
                            "authorized-only allocation must fit: {error:?}"
                        );
                        assert!(
                            matches!(error, contextdb_core::Error::MemoryBudgetExceeded {
                            requested_bytes: 544, available_bytes, budget_limit_bytes, ..
                        } if available_bytes == headroom && budget_limit_bytes == headroom)
                        );
                        outcomes.push(format!("{error:?}"));
                    }
                }
                assert_eq!(db.accountant().usage().used, resident);
            }
            assert_eq!(db.__vector_passive_activity_counters_for_test(), activity);
            assert_eq!(db.__relational_scan_rows_touched(), scans);
            if let Some(expected) = &expected {
                assert_eq!(&outcomes, expected);
            } else {
                expected = Some(outcomes);
            }
        }
    }
}
