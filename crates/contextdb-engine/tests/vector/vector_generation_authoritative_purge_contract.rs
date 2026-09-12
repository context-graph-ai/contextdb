//! Authoritative-purge cleanup contract for maintained vector generations.
//!
//! The public `PURGE` door permanently removes one selected lineage.  For a
//! partitioned vector index, that removal must retire saved graph state only
//! for the partition whose last vector was purged.  A neighbour partition is
//! still entitled to keep and use its already-maintained route.

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_PARTITION: usize = 8;
const MAX_MAINTENANCE_CYCLES: usize = 16;

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
    VectorIndexRef::new("purge_generation_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID partition component has one canonical key")
}

fn graph_available(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(&index(), &partition(scope))
        .is_some_and(|info| info.graph_available)
}

fn graph_serial(db: &Database, scope: Uuid) -> Option<u64> {
    db.vector_store_for_test()
        .raw_hnsw_build_serial_for_partition_for_test(&index(), &partition(scope))
}

fn saved_generation_status(
    db: &Database,
    scope: Uuid,
) -> Option<contextdb_vector::store::VectorGraphGenerationStatus> {
    db.vector_store_for_test()
        .partition_graph_generation_status(&VectorPartitionRef::new(index(), partition(scope)))
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

fn indexed_search(db: &Database, scope: Uuid, query: Vec<f32>) -> Vec<Uuid> {
    ids(&db
        .execute(
            "SELECT id FROM purge_generation_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("indexed search for partition {scope}: {error}")))
}

fn create_table(db: &Database) {
    db.execute(
        "CREATE TABLE purge_generation_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED\
         )",
        &empty(),
    )
    .expect("create the authoritative-purge vector fixture");
}

fn seed_partition(db: &Database, scope: Uuid, first: Uuid, leading_axis: usize) {
    for ordinal in 0..ROWS_PER_PARTITION {
        let id = Uuid::from_u128(first.as_u128() + ordinal as u128);
        let vector = if ordinal == 0 {
            match leading_axis {
                0 => vec![1.0, 0.0, 0.0],
                1 => vec![0.0, 1.0, 0.0],
                _ => panic!("the fixture has two deterministic axes"),
            }
        } else {
            vec![0.0, 0.0, 1.0]
        };
        db.execute(
            "INSERT INTO purge_generation_items (id, scope_id, embedding) \
             VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Uuid(scope)),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("commit one vector in the selected partition");
    }
}

fn drive_maintenance_until_all_ready(db: &Database, scopes: &[Uuid]) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if scopes
            .iter()
            .copied()
            .all(|scope| graph_available(db, scope))
        {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!(
        "caller-driven maintenance did not publish every partition route within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

/// Purging every lineage in alpha permanently removes alpha's vector and its
/// saved route.  It must not throw away bravo's saved route merely because
/// both partitions share one declared vector column.
#[test]
fn authoritative_purge_retires_only_the_selected_partitions_saved_generation_after_reopen() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("authoritative-purge-generation.db");
    let alpha = Uuid::from_u128(0xA110);
    let bravo = Uuid::from_u128(0xB220);
    let alpha_first = Uuid::from_u128(0xA100_0000);
    let bravo_first = Uuid::from_u128(0xB200_0000);

    {
        let db = Database::open(&path).expect("open file-backed purge fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_table(&db);
        seed_partition(&db, alpha, alpha_first, 0);
        seed_partition(&db, bravo, bravo_first, 1);
        drive_maintenance_until_all_ready(&db, &[alpha, bravo]);

        let bravo_generation = graph_serial(&db, bravo)
            .expect("the unrelated partition has a maintained generation before PURGE");
        assert_eq!(
            indexed_search(&db, bravo, vec![0.0, 1.0, 0.0]),
            vec![bravo_first],
            "the unrelated partition has a usable indexed answer before PURGE"
        );

        let purged = db
            .execute(
                "PURGE FROM purge_generation_items WHERE scope_id = $scope",
                &params([("scope", Value::Uuid(alpha))]),
            )
            .expect("the public authoritative PURGE removes alpha's complete lineage");
        assert_eq!(
            purged.rows_affected, ROWS_PER_PARTITION as u64,
            "the public purge selected every vector lineage in alpha and no bravo lineage"
        );
        assert_eq!(
            indexed_search(&db, alpha, vec![1.0, 0.0, 0.0]),
            Vec::<Uuid>::new(),
            "no indexed query may return a permanently purged vector"
        );
        assert!(
            saved_generation_status(&db, alpha).is_none(),
            "the selected partition retains no live saved-generation address after PURGE"
        );
        assert_eq!(
            graph_serial(&db, bravo),
            Some(bravo_generation),
            "purging alpha must leave bravo's maintained generation identity unchanged"
        );
        assert_eq!(
            indexed_search(&db, bravo, vec![0.0, 1.0, 0.0]),
            vec![bravo_first],
            "bravo remains queryable through its maintained route before close"
        );
        db.close().expect("close the purged store cleanly");
    }

    let db = Database::open(&path).expect("clean reopen after authoritative PURGE");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        indexed_search(&db, alpha, vec![1.0, 0.0, 0.0]),
        Vec::<Uuid>::new(),
        "clean reopen must not restore a query-addressable purged vector"
    );
    assert!(
        saved_generation_status(&db, alpha).is_none(),
        "clean reopen must not restore a saved-generation address for the purged partition"
    );
    let bravo_before_query = graph_serial(&db, bravo)
        .expect("the unrelated partition retains its saved generation after clean reopen");
    assert_eq!(
        indexed_search(&db, bravo, vec![0.0, 1.0, 0.0]),
        vec![bravo_first],
        "the unrelated partition keeps its indexed result after clean reopen"
    );
    assert_eq!(
        graph_serial(&db, bravo),
        Some(bravo_before_query),
        "the first reopened bravo query uses its saved generation rather than rebuilding it"
    );
}
