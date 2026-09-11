//! An explicit INDEXED read at a pinned snapshot keeps the eligible fresh tail
//! while newer commits continue and after maintenance seals a crossing layer.

#![cfg(feature = "test-seams")]

use contextdb_core::{TxId, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::store::{
    VectorGraphGeneration, VectorGraphGenerationStatus, VectorPartitionRef,
};
use std::collections::{HashMap, HashSet};
use tempfile::TempDir;
use uuid::Uuid;

const BASE_ROWS: usize = 32;
const MAX_MAINTENANCE_CYCLES: usize = 32;

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
    VectorIndexRef::new("snapshot_frontier_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("the UUID partition key has one canonical identity")
}

fn route(scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(index(), partition(scope))
}

fn ids(result: &QueryResult) -> HashSet<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("the vector query projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn status(db: &Database, scope: Uuid) -> VectorGraphGenerationStatus {
    db.vector_store_for_test()
        .partition_graph_generation_status(&route(scope))
        .unwrap_or_else(|| panic!("route {scope} has durable generation status"))
}

fn base(status: VectorGraphGenerationStatus) -> VectorGraphGeneration {
    status
        .base
        .or(status.dormant_base)
        .unwrap_or_else(|| panic!("the maintained route has a sealed base: {status:?}"))
}

fn drive_until_base(db: &Database, scope: Uuid) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if status(db, scope).base.is_some() {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!("caller-driven maintenance did not seal a base within {MAX_MAINTENANCE_CYCLES} cycles");
}

#[test]
fn indexed_pinned_snapshot_keeps_eligible_tail_across_newer_commits() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("snapshot-frontier-completeness.redb");
    let scope = Uuid::from_u128(0x5A01);
    let base_first_id = 0x5A10;
    let inserted_after_base = Uuid::from_u128(0x5A80);
    let inserted_after_snapshot = Uuid::from_u128(0x5A81);
    let db = Database::open(&path).expect("open the durable vector fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE snapshot_frontier_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED \
             CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1)\
         )",
        &empty(),
    )
    .expect("create the maintained partitioned vector table");

    let tx = db.begin_or_panic();
    for ordinal in 0..BASE_ROWS {
        db.insert_row(
            tx,
            "snapshot_frontier_items",
            params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(base_first_id + ordinal as u128)),
                ),
                ("scope_id", Value::Uuid(scope)),
                (
                    "embedding",
                    Value::Vector(vec![1.0, ordinal as f32 * 0.001, 0.0]),
                ),
            ]),
        )
        .expect("stage one deterministic base vector");
    }
    db.commit(tx).expect("commit the base vectors");
    drive_until_base(&db, scope);
    let sealed_base = base(status(&db, scope));

    db.execute(
        "INSERT INTO snapshot_frontier_items (id, scope_id, embedding) \
         VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(inserted_after_base)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
    )
    .expect("commit the row visible only after the sealed base");
    let snapshot = db.snapshot();
    let snapshot_tx = TxId::from_snapshot(snapshot);
    let pin = db.pin_snapshot(snapshot);

    db.execute(
        "INSERT INTO snapshot_frontier_items (id, scope_id, embedding) \
         VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(inserted_after_snapshot)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .expect("commit the row beyond the pinned snapshot");

    let sql = "SELECT id FROM snapshot_frontier_items WHERE scope_id = $scope \
               ORDER BY embedding <=> $query USE VECTOR";
    let query = params([
        ("scope", Value::Uuid(scope)),
        ("query", Value::Vector(vec![0.0, 1.0, 0.0])),
    ]);
    let tail_result = db
        .execute_at_snapshot(&format!("{sql} INDEXED LIMIT 10"), &query, snapshot)
        .expect("a newer commit does not disable the snapshot-compatible fresh tail");
    let tail_ids = ids(&tail_result);
    assert_eq!(tail_result.rows.len(), 10, "the sealed base fills LIMIT 10");
    assert!(
        tail_ids.contains(&inserted_after_base),
        "the bit-identical eligible tail row wins a slot even though the base can fill LIMIT"
    );
    assert!(
        !tail_ids.contains(&inserted_after_snapshot),
        "visibility filtering excludes the newer tail row"
    );

    let mut sealed_change = None;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
        let observed = status(&db, scope);
        if base(observed) == sealed_base
            && let Some(change) = observed.change.or(observed.dormant_change)
            && change.covered_tx > snapshot_tx
            && change.covered_lsn > sealed_base.covered_lsn
        {
            sealed_change = Some(change);
            break;
        }
    }
    let change = sealed_change.unwrap_or_else(|| {
        panic!(
            "maintenance did not retain B while sealing a change frontier beyond N within \
             {MAX_MAINTENANCE_CYCLES} cycles"
        )
    });
    assert!(
        sealed_base.covered_tx <= snapshot_tx && change.covered_tx > snapshot_tx,
        "B is compatible with S while the observed change frontier includes N"
    );
    assert!(
        change.covered_tx > snapshot_tx,
        "the observed change frontier includes the row committed after the pinned snapshot"
    );

    let expected_at_snapshot = (0..BASE_ROWS)
        .map(|ordinal| Uuid::from_u128(base_first_id + ordinal as u128))
        .chain(std::iter::once(inserted_after_base))
        .collect::<HashSet<_>>();
    let exact_at_snapshot = db
        .execute_at_snapshot(&format!("{sql} EXACT LIMIT 64"), &query, snapshot)
        .expect("EXACT reads the authoritative pinned snapshot");
    assert_eq!(
        exact_at_snapshot.rows.len(),
        expected_at_snapshot.len(),
        "EXACT returns one result for every row visible at S"
    );
    assert_eq!(
        ids(&exact_at_snapshot),
        expected_at_snapshot,
        "EXACT sees every row visible at S, including I and excluding N"
    );

    let current_indexed = db
        .execute(&format!("{sql} INDEXED LIMIT 64"), &query)
        .expect("the current B-plus-change INDEXED route remains usable");
    let expected_current = expected_at_snapshot
        .iter()
        .copied()
        .chain(std::iter::once(inserted_after_snapshot))
        .collect::<HashSet<_>>();
    assert_eq!(
        current_indexed.rows.len(),
        expected_current.len(),
        "the current INDEXED route returns one result for every row through N"
    );
    assert_eq!(
        ids(&current_indexed),
        expected_current,
        "the current INDEXED route is complete through N"
    );

    let crossing_top_ten = db
        .execute_at_snapshot(&format!("{sql} INDEXED LIMIT 10"), &query, snapshot)
        .expect("the change graph crossing the snapshot remains visibility-filterable");
    let crossing_top_ten_ids = ids(&crossing_top_ten);
    assert_eq!(crossing_top_ten.rows.len(), 10);
    assert!(
        crossing_top_ten_ids.contains(&inserted_after_base),
        "the crossing change graph keeps the eligible nearest row"
    );
    assert!(
        !crossing_top_ten_ids.contains(&inserted_after_snapshot),
        "the crossing change graph cannot publish its newer row"
    );
    let crossing_complete = db
        .execute_at_snapshot(&format!("{sql} INDEXED LIMIT 64"), &query, snapshot)
        .expect("the complete snapshot-compatible chain remains indexed");
    assert_eq!(
        ids(&crossing_complete),
        expected_at_snapshot,
        "base plus crossing change returns every visible row and no newer row"
    );
    drop(pin);
}
