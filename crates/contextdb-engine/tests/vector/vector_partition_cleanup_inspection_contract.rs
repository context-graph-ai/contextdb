//! Lifecycle, cleanup, and inspection contracts for partitioned vector state.
//!
//! Public database operations drive held snapshots, DDL, close/reopen, and memory
//! accounting. Deterministic lifecycle inspection checks retained states and the
//! operator's recovery action without substituting guessed counters.

use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;
use uuid::Uuid;

const INDEX_COLUMNS: &[&str] = &[
    "table",
    "column",
    "dimension",
    "quantization",
    "vector_count",
    "bytes",
    "partition_key_columns",
    "max_partitions",
    "live_partitions",
    "retained_partitions",
    "search_mode",
    "declared_auto_index_at",
    "effective_auto_index_at",
    "declared_hnsw_m",
    "declared_hnsw_ef_construction",
    "declared_hnsw_ef_search",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "oldest_base_tx",
    "newest_base_tx",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "charged_vector_bytes",
    "durable_index_bytes",
    "charged_index_bytes",
    "query_state",
    "maintenance_state",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "unavailable_partitions",
    "stalled_partitions",
    "broad_route",
    "broad_route_state",
    "broad_route_base_tx",
    "broad_route_vectors_total",
    "broad_route_vectors_done",
    "broad_route_vectors_remaining",
    "broad_route_reason",
    "broad_route_recovery_action",
];

const PARTITION_COLUMNS: &[&str] = &[
    "table",
    "column",
    "partition_key",
    "live_rows",
    "retained_rows",
    "base_generation",
    "base_tx",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "charged_vector_bytes",
    "durable_index_bytes",
    "charged_index_bytes",
    "query_state",
    "availability_reason",
    "maintenance_state",
    "maintenance_reason",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "maintenance_checkpoint_tx",
    "desired_hnsw_m",
    "desired_hnsw_ef_construction",
    "desired_hnsw_ef_search",
    "serving_hnsw_m",
    "serving_hnsw_ef_construction",
    "serving_hnsw_ef_search",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "desired_policy_revision",
    "serving_policy_revision",
    "recovery_action",
];

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("inspection has {name}: {:?}", result.columns))
}

fn value<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    result
        .rows
        .get(row)
        .and_then(|row| row.get(column(result, name)))
        .unwrap_or_else(|| panic!("inspection has row {row} and {name}: {result:?}"))
}

fn partition_rows(db: &Database, table: &str, column_name: &str) -> QueryResult {
    db.execute(
        &format!("SHOW VECTOR_PARTITIONS FOR {table}.{column_name}"),
        &empty(),
    )
    .unwrap_or_else(|error| panic!("inspect {table}.{column_name}: {error}"))
}

fn index_rows(db: &Database) -> QueryResult {
    db.execute("SHOW VECTOR_INDEXES", &empty())
        .unwrap_or_else(|error| panic!("inspect vector indexes: {error}"))
}

fn scope_key(scope: &str) -> Value {
    Value::Json(serde_json::json!({ "scope": scope }))
}

fn assert_index_presence(result: &QueryResult, table: &str, column_name: &str, expected: bool) {
    let present = result.rows.iter().any(|row| {
        row[column(result, "table")] == Value::Text(table.to_owned())
            && row[column(result, "column")] == Value::Text(column_name.to_owned())
    });
    assert_eq!(
        present,
        expected,
        "SHOW VECTOR_INDEXES must {} {table}.{column_name}: {result:?}",
        if expected { "contain" } else { "not retain" }
    );
}

/// A state that an actual held read can still
/// see counts at the cap; once that hold is released, moving the last vector
/// may retire the old state and admit the destination without an over-limit
/// intermediate state.
#[test]
fn held_snapshot_blocks_at_cap_key_move_then_cleanup_admits_it_and_inspects_stable_order() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE partition_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2
        ) HISTORY CURRENT ONLY SYNC OFF",
        &empty(),
    )
    .expect("create a current-only partitioned vector table");

    let first = Uuid::from_u128(0x101);
    let second = Uuid::from_u128(0x102);
    for (id, scope, embedding) in [
        (first, "alpha", vec![1.0, 0.0, 0.0]),
        (second, "bravo", vec![0.0, 1.0, 0.0]),
    ] {
        db.execute(
            "INSERT INTO partition_items (id, scope, embedding) VALUES ($id, $scope, $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("scope", Value::Text(scope.to_owned())),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("seed one occupied partition");
    }

    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot);
    let refusal = db
        .execute(
            "UPDATE partition_items SET scope = 'charlie' WHERE id = $id",
            &params(vec![("id", Value::Uuid(first))]),
        )
        .expect_err("a snapshot-retained source state must count at the cap");
    assert!(
        matches!(
            refusal,
            Error::VectorPartitionLimitExceeded {
                index,
                max_partitions: 2,
            } if index == VectorIndexRef::new("partition_items", "embedding")
        ),
        "the at-cap move must return the typed partition-limit refusal"
    );
    let unchanged = db
        .execute(
            "SELECT scope FROM partition_items WHERE id = $id",
            &params(vec![("id", Value::Uuid(first))]),
        )
        .expect("read the refused row");
    assert_eq!(unchanged.rows, vec![vec![Value::Text("alpha".to_owned())]]);
    drop(pin);

    db.execute(
        "UPDATE partition_items SET scope = 'charlie' WHERE id = $id",
        &params(vec![("id", Value::Uuid(first))]),
    )
    .expect("after the hold releases, the last-vector move reuses the source capacity");

    let partitions = partition_rows(&db, "partition_items", "embedding");
    assert_eq!(
        partitions.columns,
        PARTITION_COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
        "partition inspection has one fixed public shape"
    );
    assert_eq!(partitions.rows.len(), 2);
    assert_eq!(value(&partitions, 0, "partition_key"), &scope_key("bravo"));
    assert_eq!(
        value(&partitions, 1, "partition_key"),
        &scope_key("charlie")
    );
    for row in 0..partitions.rows.len() {
        assert_eq!(value(&partitions, row, "live_rows"), &Value::Int64(1));
        assert_eq!(value(&partitions, row, "retained_rows"), &Value::Int64(0));
    }

    let summary = index_rows(&db);
    assert_eq!(
        summary.columns,
        INDEX_COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
        "whole-index inspection has one fixed public shape"
    );
    let row = summary
        .rows
        .iter()
        .position(|row| {
            row[column(&summary, "table")] == Value::Text("partition_items".to_owned())
                && row[column(&summary, "column")] == Value::Text("embedding".to_owned())
        })
        .expect("whole-index inspection contains the declared column");
    assert_eq!(value(&summary, row, "live_partitions"), &Value::Int64(2));
    assert_eq!(
        value(&summary, row, "retained_partitions"),
        &Value::Int64(0)
    );
    assert_eq!(value(&summary, row, "vector_count"), &Value::Int64(2));
}

#[test]
fn concurrent_first_partitions_share_one_atomic_cap_admission() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE concurrent_partition_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS 1
        ) HISTORY CURRENT ONLY SYNC OFF",
        &empty(),
    )
    .expect("create the one-slot concurrent admission fixture");

    let start = Arc::new(Barrier::new(3));
    let attempts = [
        (Uuid::from_u128(0x151), "alpha", vec![1.0, 0.0]),
        (Uuid::from_u128(0x152), "bravo", vec![0.0, 1.0]),
    ]
    .into_iter()
    .map(|(id, scope, embedding)| {
        let db = db.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            let result = db.execute(
                "INSERT INTO concurrent_partition_items (id, scope, embedding) \
                 VALUES ($id, $scope, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("scope", Value::Text(scope.to_string())),
                    ("embedding", Value::Vector(embedding)),
                ]),
            );
            (id, scope, result)
        })
    })
    .collect::<Vec<_>>();
    start.wait();
    let results = attempts
        .into_iter()
        .map(|attempt| attempt.join().expect("concurrent writer did not panic"))
        .collect::<Vec<_>>();

    assert_eq!(
        results
            .iter()
            .filter(|(_, _, result)| result.is_ok())
            .count(),
        1,
        "exactly one new partition owns the sole cap slot"
    );
    let refused = results
        .iter()
        .find_map(|(id, scope, result)| result.as_ref().err().map(|error| (*id, *scope, error)))
        .expect("one racing writer is refused");
    assert!(
        matches!(
            refused.2,
            Error::VectorPartitionLimitExceeded {
                index,
                max_partitions: 1,
            } if index == &VectorIndexRef::new("concurrent_partition_items", "embedding")
        ),
        "the losing writer receives the exact typed cap refusal: {:?}",
        refused.2
    );
    let rows = db
        .execute(
            "SELECT id, scope FROM concurrent_partition_items ORDER BY scope",
            &empty(),
        )
        .expect("read the one committed winner");
    assert_eq!(rows.rows.len(), 1, "the losing row never commits");
    assert!(
        rows.rows
            .iter()
            .all(|row| row.first() != Some(&Value::Uuid(refused.0))),
        "the typed refusal rolls back the losing relational row"
    );
    let partitions = partition_rows(&db, "concurrent_partition_items", "embedding");
    assert_eq!(partitions.rows.len(), 1);
    assert_eq!(value(&partitions, 0, "live_rows"), &Value::Int64(1));
    assert_eq!(value(&partitions, 0, "retained_rows"), &Value::Int64(0));
}

#[test]
fn second_vector_column_cap_refusal_rolls_back_the_row_and_both_columns() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE two_column_partition_items (
            id UUID PRIMARY KEY,
            scope_a TEXT NOT NULL,
            scope_b TEXT NOT NULL,
            embedding_a VECTOR(2) PARTITION_KEY (scope_a) MAX_PARTITIONS 2,
            embedding_b VECTOR(2) PARTITION_KEY (scope_b) MAX_PARTITIONS 1
        ) HISTORY CURRENT ONLY SYNC OFF",
        &empty(),
    )
    .expect("declare two independently capped vector columns");
    db.execute(
        "INSERT INTO two_column_partition_items \
         (id, scope_a, scope_b, embedding_a, embedding_b) \
         VALUES ($id, 'a-one', 'b-one', $a, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(0x161))),
            ("a", Value::Vector(vec![1.0, 0.0])),
            ("b", Value::Vector(vec![0.0, 1.0])),
        ]),
    )
    .expect("seed both first-column and second-column partitions");

    let refused_id = Uuid::from_u128(0x162);
    let refusal = db
        .execute(
            "INSERT INTO two_column_partition_items \
             (id, scope_a, scope_b, embedding_a, embedding_b) \
             VALUES ($id, 'a-two', 'b-two', $a, $b)",
            &params(vec![
                ("id", Value::Uuid(refused_id)),
                ("a", Value::Vector(vec![0.9, 0.1])),
                ("b", Value::Vector(vec![0.1, 0.9])),
            ]),
        )
        .expect_err("the second column crosses its own one-partition cap");
    assert!(
        matches!(
            &refusal,
            Error::VectorPartitionLimitExceeded {
                index,
                max_partitions: 1,
            } if index == &VectorIndexRef::new("two_column_partition_items", "embedding_b")
        ),
        "the second independently declared column owns the refusal: {refusal:?}"
    );
    assert!(
        db.execute(
            "SELECT id FROM two_column_partition_items WHERE id = $id",
            &params(vec![("id", Value::Uuid(refused_id))]),
        )
        .unwrap()
        .rows
        .is_empty(),
        "the row is absent after vector admission fails"
    );
    for column_name in ["embedding_a", "embedding_b"] {
        let partitions = partition_rows(&db, "two_column_partition_items", column_name);
        assert_eq!(
            partitions.rows.len(),
            1,
            "{column_name} keeps only its original partition"
        );
        assert_eq!(value(&partitions, 0, "live_rows"), &Value::Int64(1));
        assert_eq!(value(&partitions, 0, "retained_rows"), &Value::Int64(0));
    }
}

#[test]
fn cap_admission_keeps_every_untouched_live_partition_indexed_and_counted() {
    let root = TempDir::new().expect("temporary vector store directory");
    let path = root.path().join("live-cap-admission.db");
    let db = Database::open(&path).expect("open file-backed cap fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE live_cap_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3
                SEARCH_MODE INDEXED AUTO_INDEX_AT 1
        ) SYNC OFF",
        &empty(),
    )
    .expect("create a three-slot partitioned table");

    let seeded = [
        (Uuid::from_u128(0x361), "alpha", vec![1.0, 0.0, 0.0]),
        (Uuid::from_u128(0x362), "bravo", vec![0.0, 1.0, 0.0]),
    ];
    for (id, scope, embedding) in &seeded {
        db.execute(
            "INSERT INTO live_cap_items (id, scope, embedding) VALUES ($id, $scope, $embedding)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("scope", Value::Text((*scope).to_owned())),
                ("embedding", Value::Vector(embedding.clone())),
            ]),
        )
        .expect("seed one live partition");
    }
    assert_eq!(
        db.run_maintenance_cycle()
            .expect("build both initially populated partitions")
            .vector
            .built_partitions,
        2
    );

    let charlie = Uuid::from_u128(0x363);
    db.execute(
        "INSERT INTO live_cap_items (id, scope, embedding) \
         VALUES ($id, 'charlie', $embedding)",
        &params(vec![
            ("id", Value::Uuid(charlie)),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .expect("the third live partition is admitted at the declared boundary");
    assert_eq!(
        db.run_maintenance_cycle()
            .expect("build the admitted boundary partition")
            .vector
            .built_partitions,
        1
    );

    let refusal = db
        .execute(
            "INSERT INTO live_cap_items (id, scope, embedding) \
             VALUES ($id, 'delta', $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(0x364))),
                ("embedding", Value::Vector(vec![0.5, 0.5, 0.0])),
            ]),
        )
        .expect_err("three live partitions still occupy all three declared slots");
    assert!(matches!(
        refusal,
        Error::VectorPartitionLimitExceeded {
            max_partitions: 3,
            ..
        }
    ));

    let index = VectorIndexRef::new("live_cap_items", "embedding");
    let expected = seeded
        .into_iter()
        .chain([(charlie, "charlie", vec![0.0, 0.0, 1.0])]);
    for (id, scope, query) in expected {
        let key = VectorPartitionKey::from_values(&[Value::Text(scope.to_owned())])
            .expect("scope has a canonical partition key");
        let info = db
            .vector_store_for_test()
            .partition_info(&index, &key)
            .expect("the live partition keeps its inspection state");
        assert!(
            info.graph_available,
            "untouched live partition {scope} keeps its indexed route"
        );
        let result = db
            .execute(
                "SELECT id FROM live_cap_items WHERE scope = $scope \
                 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
                &params(vec![
                    ("scope", Value::Text(scope.to_owned())),
                    ("query", Value::Vector(query)),
                ]),
            )
            .expect("the retained indexed route remains searchable");
        assert_eq!(result.rows, vec![vec![Value::Uuid(id)]]);
    }

    let partitions = partition_rows(&db, "live_cap_items", "embedding");
    assert_eq!(partitions.rows.len(), 3);
    assert!(partitions.rows.iter().all(|row| {
        row[column(&partitions, "live_rows")] == Value::Int64(1)
            && row[column(&partitions, "query_state")] == Value::Text("ready".to_owned())
    }));
    let indexes = index_rows(&db);
    let row = indexes
        .rows
        .iter()
        .position(|row| row[column(&indexes, "table")] == Value::Text("live_cap_items".to_owned()))
        .expect("whole-index inspection contains the cap fixture");
    assert_eq!(value(&indexes, row, "live_partitions"), &Value::Int64(3));
    assert_eq!(
        value(&indexes, row, "retained_partitions"),
        &Value::Int64(0)
    );
}

/// A last-vector deletion cannot make a retained generation vanish from the
/// operator view or from MAX_PARTITIONS while a real snapshot is pinned. The
/// next maintenance pass after unpin performs guarded retirement and releases
/// the slot for reuse.
#[test]
fn historical_only_partition_stays_inspectable_and_counted_until_unpin() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE retained_partition_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 1
        ) HISTORY CURRENT ONLY SYNC OFF",
        &empty(),
    )
    .expect("create one-slot retained partition fixture");
    let alpha = Uuid::from_u128(0x181);
    let beta = Uuid::from_u128(0x182);
    db.execute(
        "INSERT INTO retained_partition_items (id, scope, embedding) \
         VALUES ($id, 'alpha', $embedding)",
        &params(vec![
            ("id", Value::Uuid(alpha)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("seed the sole partition");

    let pin = db.pin_snapshot(db.snapshot());
    db.execute(
        "DELETE FROM retained_partition_items WHERE id = $id",
        &params(vec![("id", Value::Uuid(alpha))]),
    )
    .expect("delete the last live vector while its old image is pinned");

    let retained = partition_rows(&db, "retained_partition_items", "embedding");
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(value(&retained, 0, "partition_key"), &scope_key("alpha"));
    assert_eq!(value(&retained, 0, "live_rows"), &Value::Int64(0));
    assert_eq!(value(&retained, 0, "retained_rows"), &Value::Int64(1));
    let refusal = db
        .execute(
            "INSERT INTO retained_partition_items (id, scope, embedding) \
             VALUES ($id, 'beta', $embedding)",
            &params(vec![
                ("id", Value::Uuid(beta)),
                ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
            ]),
        )
        .expect_err("the pinned historical partition still occupies MAX_PARTITIONS");
    assert!(matches!(
        refusal,
        Error::VectorPartitionLimitExceeded {
            max_partitions: 1,
            ..
        }
    ));

    drop(pin);
    db.run_maintenance_cycle()
        .expect("the first post-unpin maintenance pass retires historical-only state");
    let retired = partition_rows(&db, "retained_partition_items", "embedding");
    assert_eq!(retired.rows.len(), 0);
    db.execute(
        "INSERT INTO retained_partition_items (id, scope, embedding) \
         VALUES ($id, 'beta', $embedding)",
        &params(vec![
            ("id", Value::Uuid(beta)),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
    )
    .expect("after unpin the retired partition releases its slot");
    let released = partition_rows(&db, "retained_partition_items", "embedding");
    assert_eq!(released.rows.len(), 1);
    assert_eq!(value(&released, 0, "partition_key"), &scope_key("beta"));
}

#[test]
fn file_backed_retirement_releases_a_snapshot_free_slot_while_a_sibling_tail_stays_active() {
    let root = TempDir::new().expect("temporary vector store directory");
    let path = root.path().join("continuous-sibling-retirement.db");
    let db = Database::open(&path).expect("open file-backed retirement fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE retirement_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2 AUTO_INDEX_AT 1
                CONSOLIDATION (CHANGE_PERCENT = 100, TOMBSTONE_PERCENT = 100)
        ) SYNC OFF",
        &empty(),
    )
    .expect("create a durable two-slot partitioned table");
    let alpha = Uuid::from_u128(0x401);
    let bravo = Uuid::from_u128(0x402);
    for (id, scope, embedding) in [
        (alpha, "alpha", vec![1.0, 0.0, 0.0]),
        (bravo, "bravo", vec![0.0, 1.0, 0.0]),
    ] {
        db.execute(
            "INSERT INTO retirement_items (id, scope, embedding) VALUES ($id, $scope, $embedding)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("scope", Value::Text(scope.to_owned())),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("seed one occupied partition");
    }
    let initial = db
        .run_maintenance_cycle()
        .expect("one wake builds every never-built partition");
    assert_eq!(initial.vector.built_partitions, 2);
    assert_eq!(
        initial.vector.built_indexes, 1,
        "two partitions built for one vector column count as one built index"
    );

    let held = db.pin_snapshot(db.snapshot());
    db.execute(
        "DELETE FROM retirement_items WHERE id = $id",
        &params(vec![("id", Value::Uuid(alpha))]),
    )
    .expect("delete alpha's last vector while its old image is pinned");
    db.execute(
        "INSERT INTO retirement_items (id, scope, embedding) VALUES ($id, 'bravo', $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(0x403))),
            ("embedding", Value::Vector(vec![0.0, 0.9, 0.1])),
        ]),
    )
    .expect("leave bravo with a live unconsolidated tail");
    let retained = partition_rows(&db, "retirement_items", "embedding");
    let alpha_row = retained
        .rows
        .iter()
        .position(|row| row[column(&retained, "partition_key")] == scope_key("alpha"))
        .expect("alpha remains visible while pinned");
    let bravo_row = retained
        .rows
        .iter()
        .position(|row| row[column(&retained, "partition_key")] == scope_key("bravo"))
        .expect("bravo remains visible");
    assert_eq!(
        value(&retained, alpha_row, "retained_rows"),
        &Value::Int64(1)
    );
    assert_eq!(
        value(&retained, alpha_row, "tombstones"),
        &Value::Int64(1),
        "alpha's final delete remains pending while its old snapshot is pinned"
    );
    assert_eq!(
        value(&retained, bravo_row, "pending_inserts"),
        &Value::Int64(1)
    );
    let alpha_route = VectorPartitionRef::new(
        VectorIndexRef::new("retirement_items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Text("alpha".to_owned())]).unwrap(),
    );
    let alpha_journal = db.__debug_vector_partition_journal_file_for_test(&alpha_route);
    assert!(
        alpha_journal
            .physical_record_lsns
            .iter()
            .any(|lsn| *lsn > alpha_journal.truncated_through_lsn),
        "alpha's final delete remains physically uncovered before the release wake: {alpha_journal:?}"
    );
    let before_release = db.vector_memory_ownership_receipt_for_test();
    assert!(
        before_release.base_graph > 0,
        "the retained alpha route owns charged graph bytes before release"
    );
    drop(held);

    let cycle = db
        .run_maintenance_cycle()
        .expect("the next wake represents alpha's delete and retires it despite bravo's tail");
    assert_eq!(
        cycle.vector.built_partitions, 1,
        "bravo stays below its declared consolidation threshold"
    );
    let released = partition_rows(&db, "retirement_items", "embedding");
    assert_eq!(released.rows.len(), 1, "alpha's inspection row is released");
    assert_eq!(value(&released, 0, "partition_key"), &scope_key("bravo"));
    assert_eq!(value(&released, 0, "pending_inserts"), &Value::Int64(1));
    assert_eq!(
        value(&released, 0, "maintenance_reason"),
        &Value::Text("none".to_owned()),
        "a healthy tail below both declared thresholds is not maintenance work"
    );
    assert_eq!(
        value(&released, 0, "recovery_action"),
        &Value::Text("none".to_owned())
    );
    let after_release = db.vector_memory_ownership_receipt_for_test();
    assert!(
        after_release.base_graph <= before_release.base_graph,
        "retirement cannot increase charged base-graph ownership"
    );
    let alpha_status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&alpha_route)
        .expect("raw historical alpha state remains addressable to the lifecycle seam");
    assert!(
        alpha_status.base.is_none()
            && alpha_status.change.is_none()
            && alpha_status.dormant_base.is_none()
            && alpha_status.dormant_change.is_none()
            && !alpha_status.base_resident
            && !alpha_status.change_resident,
        "retirement releases every charged and reloadable alpha graph owner: {alpha_status:?}"
    );

    db.execute(
        "INSERT INTO retirement_items (id, scope, embedding) VALUES ($id, 'charlie', $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(0x404))),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .expect("alpha's released MAX_PARTITIONS slot admits charlie");
    let reused = partition_rows(&db, "retirement_items", "embedding");
    assert_eq!(reused.rows.len(), 2);
    assert!(
        reused
            .rows
            .iter()
            .any(|row| { row[column(&reused, "partition_key")] == scope_key("charlie") })
    );
}

#[test]
fn explicit_storage_compaction_cannot_retire_the_index_needed_by_a_pinned_old_snapshot() {
    let root = TempDir::new().expect("temporary vector store directory");
    let path = root.path().join("compaction-old-snapshot.db");
    let db = Database::open(&path).expect("open file-backed snapshot fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE snapshot_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2
                SEARCH_MODE INDEXED AUTO_INDEX_AT 1
                CONSOLIDATION (CHANGE_PERCENT = 100, TOMBSTONE_PERCENT = 100)
        ) SYNC OFF",
        &empty(),
    )
    .expect("create a durable snapshot fixture");
    let id = Uuid::from_u128(0x501);
    db.execute(
        "INSERT INTO snapshot_items (id, scope, embedding) VALUES ($id, 'alpha', $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("seed alpha");
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    db.execute(
        "DELETE FROM snapshot_items WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("delete the final live vector after the old snapshot");
    db.run_maintenance_cycle()
        .expect("publish the delete-only generation while the old snapshot keeps its route");

    db.compact_now()
        .expect("run explicit storage compaction through its production door");
    let index = VectorIndexRef::new("snapshot_items", "embedding");
    let alpha_key = VectorPartitionKey::from_values(&[Value::Text("alpha".to_owned())])
        .expect("canonical alpha partition key");
    assert!(
        !db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &alpha_key),
        "explicit compaction evicts alpha's reloadable raw bodies"
    );
    db.run_maintenance_cycle()
        .expect("the idle retirement pass consults the eviction-proof directory");
    let retained = partition_rows(&db, "snapshot_items", "embedding");
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(value(&retained, 0, "retained_rows"), &Value::Int64(1));
    assert!(
        !matches!(value(&retained, 0, "base_generation"), Value::Null),
        "the retained old-snapshot route keeps a generation after raw-body eviction"
    );
    let result = db
        .execute_at_snapshot(
            "SELECT id FROM snapshot_items WHERE scope = 'alpha' \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            old_snapshot,
        )
        .expect("the already-open snapshot retains its compatible indexed route");
    assert_eq!(result.rows, vec![vec![Value::Uuid(id)]]);
    drop(old_pin);
    db.run_maintenance_cycle()
        .expect("a later unpinned pass may retire the route");
    assert!(
        partition_rows(&db, "snapshot_items", "embedding")
            .rows
            .is_empty()
    );
}

/// A populated partition with no maintained graph reports initial construction
/// and the operator's next action through ordinary lifecycle inspection.
#[test]
fn inspection_names_the_lifecycle_of_a_populated_unavailable_partition() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE lifecycle_inspection (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 4
        )",
        &empty(),
    )
    .expect("create partitioned inspection fixture");
    db.execute(
        "INSERT INTO lifecycle_inspection (id, scope, embedding) VALUES ($id, 'only', $embedding)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(0x201))),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("create one partition waiting for its first maintained route");

    let partitions = partition_rows(&db, "lifecycle_inspection", "embedding");
    assert_eq!(partitions.rows.len(), 1);
    assert_eq!(
        value(&partitions, 0, "query_state"),
        &Value::Text("unavailable".to_owned())
    );
    assert_eq!(
        value(&partitions, 0, "availability_reason"),
        &Value::Text("initial_build".to_owned()),
        "an unavailable first-build partition must name initial build rather than leave an operator guessing"
    );
    assert!(
        matches!(
            value(&partitions, 0, "maintenance_state"),
            Value::Text(state)
                if matches!(state.as_str(), "idle" | "building" | "replaying" | "compacting" | "repairing" | "stalled")
        ),
        "partition maintenance state must be one fixed lifecycle word"
    );
    assert!(
        matches!(
            value(&partitions, 0, "maintenance_reason"),
            Value::Text(reason)
                if matches!(reason.as_str(), "none" | "initial_build" | "new_changes" | "tombstones" | "corrupt_base" | "corrupt_changes" | "incompatible_format" | "memory_limit" | "disk_limit" | "build_failure")
        ),
        "partition maintenance reason must be a safe stable word"
    );
    assert!(
        matches!(
            value(&partitions, 0, "recovery_action"),
            Value::Text(action)
                if matches!(action.as_str(), "none" | "wait_for_automatic_work" | "run_maintenance_cycle" | "raise_memory_limit" | "raise_disk_limit_or_free_space" | "inspect_build_failure")
        ),
        "partition recovery action must tell the operator how to restore service"
    );
}

/// Rename changes the sole public vector identity,
/// and a later column drop removes that identity across a close/reopen.  The
/// memory-accountant comparison is deliberately relative: it proves the
/// observable release signal without guessing the size of table metadata.
#[test]
fn rename_then_drop_removes_the_only_vector_identity_after_reopen_and_releases_memory() {
    let root = TempDir::new().expect("temporary vector store directory");
    let path = root.path().join("cleanup.db");
    let id = Uuid::from_u128(0x301);

    let db = Database::open(&path).expect("open file-backed cleanup fixture");
    db.execute(
        "CREATE TABLE cleanup_items (
            id UUID PRIMARY KEY,
            scope TEXT NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 4
        )",
        &empty(),
    )
    .expect("create cleanup fixture");
    let charged_before_vector = db.accountant().usage().used;
    db.execute(
        "INSERT INTO cleanup_items (id, scope, embedding) VALUES ($id, 'only', $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert vector whose allocation later must release");
    let charged_with_vector = db.accountant().usage().used;
    assert!(
        charged_with_vector > charged_before_vector,
        "the fixture must observe a charged vector allocation before it can prove release"
    );

    db.execute(
        "ALTER TABLE cleanup_items RENAME COLUMN embedding TO embedding_v2",
        &empty(),
    )
    .expect("rename vector column");
    let renamed = index_rows(&db);
    assert_index_presence(&renamed, "cleanup_items", "embedding", false);
    assert_index_presence(&renamed, "cleanup_items", "embedding_v2", true);
    db.close().expect("close renamed store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen renamed store");
    let renamed = index_rows(&reopened);
    assert_index_presence(&renamed, "cleanup_items", "embedding", false);
    assert_index_presence(&renamed, "cleanup_items", "embedding_v2", true);
    let found = reopened
        .execute(
            "SELECT id FROM cleanup_items ORDER BY embedding_v2 <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .expect("renamed vector remains searchable after reopen");
    assert_eq!(found.rows, vec![vec![Value::Uuid(id)]]);
    assert!(matches!(
        reopened.execute(
            "SELECT id FROM cleanup_items ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        ),
        Err(Error::UnknownVectorIndex { index })
            if index == VectorIndexRef::new("cleanup_items", "embedding")
    ));

    reopened
        .execute(
            "ALTER TABLE cleanup_items DROP COLUMN embedding_v2",
            &empty(),
        )
        .expect("drop the renamed vector column");
    assert_index_presence(
        &index_rows(&reopened),
        "cleanup_items",
        "embedding_v2",
        false,
    );
    let charged_after_drop = reopened.accountant().usage().used;
    assert!(
        charged_after_drop < charged_with_vector,
        "dropping a vector column must release its observable memory charge: before vector \
         {charged_before_vector}, with vector {charged_with_vector}, after drop {charged_after_drop}"
    );
    reopened.close().expect("close dropped-column store");
    drop(reopened);

    let reopened = Database::open(&path).expect("reopen dropped-column store");
    assert_index_presence(
        &index_rows(&reopened),
        "cleanup_items",
        "embedding_v2",
        false,
    );
    assert!(matches!(
        reopened.execute(
            "SELECT id FROM cleanup_items ORDER BY embedding_v2 <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        ),
        Err(Error::UnknownVectorIndex { index })
            if index == VectorIndexRef::new("cleanup_items", "embedding_v2")
    ));
}
