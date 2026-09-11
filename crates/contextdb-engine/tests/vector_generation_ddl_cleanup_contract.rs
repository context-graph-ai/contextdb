//! Durable-generation cleanup contracts for legal vector-column DDL.
//!
//! These journeys use only the public database door to create, populate,
//! maintain, rename, drop, close, and reopen a store.  The test-only vector
//! observations merely prove that an already-maintained route is present or
//! absent; they never build, repair, or select one.

#![cfg(feature = "test-seams")]

use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 16;
const SEEDED_ROWS: usize = 8;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID partition component has one canonical key")
}

fn graph_available(db: &Database, index: &VectorIndexRef, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(index, &partition(scope))
        .map(|info| info.graph_available)
        .unwrap_or(false)
}

fn graph_serial(db: &Database, index: &VectorIndexRef, scope: Uuid) -> Option<u64> {
    db.vector_store_for_test()
        .raw_hnsw_build_serial_for_partition_for_test(index, &partition(scope))
}

fn index_is_present(db: &Database, expected: &VectorIndexRef) -> bool {
    db.vector_store_for_test()
        .index_infos()
        .into_iter()
        .any(|info| info.index == *expected)
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn indexed_search(db: &Database, column: &str, scope: Uuid, query: Vec<f32>) -> Vec<Uuid> {
    ids(&db
        .execute(
            &format!(
                "SELECT id FROM generation_ddl_items WHERE scope_id = $scope \
                 ORDER BY {column} <=> $query USE VECTOR INDEXED LIMIT 1"
            ),
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("indexed search through {column}: {error}")))
}

fn create_and_seal_partition(db: &Database, scope: Uuid, first: Uuid) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE generation_ddl_items (
            id UUID PRIMARY KEY,
            scope_id UUID NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED
        )",
        &empty(),
    )
    .expect("create partitioned vector fixture");

    for ordinal in 0..SEEDED_ROWS {
        let id = Uuid::from_u128(first.as_u128() + ordinal as u128);
        let vector = if ordinal == 0 {
            vec![1.0, 0.0, 0.0]
        } else {
            vec![0.0, 1.0, ordinal as f32 / SEEDED_ROWS as f32]
        };
        db.execute(
            "INSERT INTO generation_ddl_items (id, scope_id, embedding) \
             VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Uuid(scope)),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("commit one deterministic partitioned vector");
    }

    let index = VectorIndexRef::new("generation_ddl_items", "embedding");
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if graph_available(db, &index, scope) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle returns");
    }
    panic!(
        "caller-driven maintenance did not durably publish the partition route within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

/// A caller that renames a maintained vector column keeps its one saved route:
/// reopening may use the renamed route, but must neither retain the old
/// identity nor make the first indexed search reconstruct it.
#[test]
fn rename_moves_a_durably_sealed_partition_generation_to_the_new_identity() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("rename-generation.db");
    let scope = Uuid::from_u128(0xD001);
    let first = Uuid::from_u128(0xD100);
    let old_index = VectorIndexRef::new("generation_ddl_items", "embedding");
    let renamed_index = VectorIndexRef::new("generation_ddl_items", "embedding_v2");

    {
        let db = Database::open(&path).expect("open file-backed fixture");
        create_and_seal_partition(&db, scope, first);
        assert!(
            graph_available(&db, &old_index, scope),
            "caller-driven maintenance must publish the original maintained route before DDL"
        );
        assert!(
            graph_serial(&db, &old_index, scope).is_some(),
            "the fixture must observe a built route before proving that DDL moves it"
        );

        db.execute(
            "ALTER TABLE generation_ddl_items RENAME COLUMN embedding TO embedding_v2",
            &empty(),
        )
        .expect("rename the generated vector column");
        assert!(
            !index_is_present(&db, &old_index),
            "rename leaves no old vector identity in the live store"
        );
        assert!(
            index_is_present(&db, &renamed_index),
            "rename moves the maintained identity instead of creating an unmaintained copy"
        );
        db.close().expect("close the renamed store cleanly");
    }

    let db = Database::open(&path).expect("clean reopen after vector-column rename");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(
        !index_is_present(&db, &old_index),
        "clean reopen must not restore the old vector identity"
    );
    assert!(
        index_is_present(&db, &renamed_index),
        "clean reopen must retain the renamed vector identity"
    );
    assert!(
        graph_available(&db, &renamed_index, scope),
        "the renamed saved route must be available before any post-reopen query"
    );
    let before_query = graph_serial(&db, &renamed_index, scope)
        .expect("the reopened renamed route has a stable observed identity");
    assert_eq!(
        indexed_search(&db, "embedding_v2", scope, vec![1.0, 0.0, 0.0]),
        vec![first],
        "the renamed maintained INDEXED route returns the sealed vector after clean reopen"
    );
    assert_eq!(
        graph_serial(&db, &renamed_index, scope),
        Some(before_query),
        "the first query after reopen must use the renamed saved route rather than rebuild it"
    );
    assert!(matches!(
        db.execute(
            "SELECT id FROM generation_ddl_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
        ),
        Err(Error::UnknownVectorIndex { index }) if index == old_index
    ));
}

/// Dropping a renamed generated vector column removes all of the durable
/// generation state.  A later open must succeed without resurrecting either
/// the name that created it or the name that temporarily carried it.
#[test]
fn drop_of_a_renamed_generated_vector_column_leaves_no_identity_after_clean_reopen() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("drop-generation.db");
    let scope = Uuid::from_u128(0xD201);
    let first = Uuid::from_u128(0xD300);
    let old_index = VectorIndexRef::new("generation_ddl_items", "embedding");
    let renamed_index = VectorIndexRef::new("generation_ddl_items", "embedding_v2");

    {
        let db = Database::open(&path).expect("open file-backed fixture");
        create_and_seal_partition(&db, scope, first);
        db.execute(
            "ALTER TABLE generation_ddl_items RENAME COLUMN embedding TO embedding_v2",
            &empty(),
        )
        .expect("rename the generated vector column before dropping it");
        assert!(
            graph_available(&db, &renamed_index, scope),
            "rename must carry the built route up to the legal drop"
        );
        db.execute(
            "ALTER TABLE generation_ddl_items DROP COLUMN embedding_v2",
            &empty(),
        )
        .expect("drop the renamed generated vector column");
        assert!(
            !index_is_present(&db, &old_index) && !index_is_present(&db, &renamed_index),
            "drop removes both possible vector identities before close"
        );
        db.close().expect("close the dropped-column store cleanly");
    }

    let db = Database::open(&path).expect("clean reopen after generated vector-column drop");
    assert!(
        !index_is_present(&db, &old_index) && !index_is_present(&db, &renamed_index),
        "clean reopen must not recreate either dropped generation identity"
    );
    for (column, index) in [("embedding", old_index), ("embedding_v2", renamed_index)] {
        assert!(matches!(
            db.execute(
                &format!(
                    "SELECT id FROM generation_ddl_items \
                     ORDER BY {column} <=> $query USE VECTOR INDEXED LIMIT 1"
                ),
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            ),
            Err(Error::UnknownVectorIndex { index: actual }) if actual == index
        ));
    }
}
