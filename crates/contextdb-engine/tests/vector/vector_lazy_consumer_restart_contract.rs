//! Restart must not turn the raw-vector cache into the source of truth.
//!
//! These journeys open a file-backed database with two dormant vector
//! partitions, then drive ordinary row reads, outbound change extraction,
//! mutation, DDL, export, and inspection through their public production
//! doors. Test seams observe residency only; they never load or repair data.

use contextdb_core::{Lsn, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::database::SnapshotInspector;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;
use uuid::Uuid;

const ALPHA_SCOPE: Uuid = Uuid::from_u128(0xA110);
const BRAVO_SCOPE: Uuid = Uuid::from_u128(0xB220);
const ALPHA_ID: Uuid = Uuid::from_u128(0xA111);
const BRAVO_ID: Uuid = Uuid::from_u128(0xB221);

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
    VectorIndexRef::new("lazy_consumer_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID component has one canonical partition key")
}

fn raw_resident(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .raw_partition_resident_for_test(&index(), &partition(scope))
}

fn seed(path: &Path) {
    let db = Database::open(path).expect("open lazy-consumer fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE lazy_consumer_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4\
         ) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &empty(),
    )
    .expect("create the partitioned syncable fixture");
    for (id, scope, embedding) in [
        (ALPHA_ID, ALPHA_SCOPE, vec![1.0, 0.0, 0.0]),
        (BRAVO_ID, BRAVO_SCOPE, vec![0.0, 1.0, 0.0]),
    ] {
        db.execute(
            "INSERT INTO lazy_consumer_items (id, scope_id, embedding) \
             VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Uuid(scope)),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("insert one partitioned vector row");
    }
    db.close().expect("close the seeded fixture");
}

fn reopen(path: &Path) -> Database {
    let db = Database::open(path).expect("reopen the lazy-consumer fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(!raw_resident(&db, ALPHA_SCOPE));
    assert!(!raw_resident(&db, BRAVO_SCOPE));
    assert_eq!(
        db.vector_store_for_test()
            .resident_raw_partition_count_for_test(),
        0,
        "open retains vector identities without loading either raw body"
    );
    db
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("query projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn exact_ids(db: &Database, scope: Uuid, query: Vec<f32>, limit: usize) -> Vec<Uuid> {
    ids(&db
        .execute(
            &format!(
                "SELECT id FROM lazy_consumer_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT {limit}"
            ),
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .expect("exact partition search succeeds"))
}

fn assert_row_vector(row: &contextdb_core::VersionedRow, expected: &[f32]) {
    assert_eq!(
        row.values.get("embedding"),
        Some(&Value::Vector(expected.to_vec())),
        "a caller-facing row contains its stored vector rather than a false NULL"
    );
}

#[test]
fn point_lookup_after_reopen_loads_only_the_row_partition_and_returns_its_vector() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("point-lookup.db");
    seed(&path);
    let db = reopen(&path);

    let row = db
        .point_lookup(
            "lazy_consumer_items",
            "id",
            &Value::Uuid(ALPHA_ID),
            db.snapshot(),
        )
        .expect("point lookup succeeds")
        .expect("alpha row exists");
    assert_row_vector(&row, &[1.0, 0.0, 0.0]);
    assert!(raw_resident(&db, ALPHA_SCOPE));
    assert!(
        !raw_resident(&db, BRAVO_SCOPE),
        "reading alpha cannot load an unrelated dormant partition"
    );
}

#[test]
fn outbound_snapshot_after_reopen_contains_every_vector_and_owner_value() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("outbound-snapshot.db");
    seed(&path);
    let db = reopen(&path);

    let changes = db.changes_since(Lsn(0));
    assert_eq!(
        changes.vectors.len(),
        2,
        "a full outbound snapshot cannot omit dormant vector bodies"
    );
    assert!(changes.rows.iter().all(|row| {
        matches!(
            row.values.get("embedding"),
            Some(Value::Vector(vector)) if vector.len() == 3
        )
    }));
    let mut vectors = changes
        .vectors
        .iter()
        .map(|vector| (vector.row_id, vector.vector.clone()))
        .collect::<Vec<_>>();
    vectors.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(vectors.len(), 2);
    assert!(vectors.iter().any(|(_, vector)| vector == &[1.0, 0.0, 0.0]));
    assert!(vectors.iter().any(|(_, vector)| vector == &[0.0, 1.0, 0.0]));
}

#[test]
fn key_only_partition_move_after_reopen_preserves_the_vector_across_another_restart() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("key-only-move.db");
    seed(&path);
    {
        let db = reopen(&path);
        db.execute(
            "UPDATE lazy_consumer_items SET scope_id = $scope WHERE id = $id",
            &params([
                ("scope", Value::Uuid(BRAVO_SCOPE)),
                ("id", Value::Uuid(ALPHA_ID)),
            ]),
        )
        .expect("move alpha by changing only its declared partition key");
        assert_eq!(
            exact_ids(&db, ALPHA_SCOPE, vec![1.0, 0.0, 0.0], 2),
            Vec::<Uuid>::new()
        );
        assert_eq!(
            exact_ids(&db, BRAVO_SCOPE, vec![1.0, 0.0, 0.0], 2)[0],
            ALPHA_ID,
            "the key-only move carries alpha's existing vector into bravo"
        );
        db.close().expect("close after the key-only move");
    }

    let db = Database::open(&path).expect("reopen after the key-only move");
    assert_eq!(
        exact_ids(&db, BRAVO_SCOPE, vec![1.0, 0.0, 0.0], 2)[0],
        ALPHA_ID,
        "the moved vector remains durable after another restart"
    );
}

#[test]
fn delete_after_reopen_tombstones_the_dormant_vector_and_keeps_the_other_partition() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("delete.db");
    seed(&path);
    {
        let db = reopen(&path);
        db.execute(
            "DELETE FROM lazy_consumer_items WHERE id = $id",
            &params([("id", Value::Uuid(ALPHA_ID))]),
        )
        .expect("delete alpha after restart");
        assert!(
            db.vector_store_for_test()
                .partition_info(&index(), &partition(ALPHA_SCOPE))
                .is_none_or(|info| info.live_rows == 0),
            "the accepted delete removes alpha from live vector membership"
        );
        assert_eq!(
            exact_ids(&db, BRAVO_SCOPE, vec![0.0, 1.0, 0.0], 1),
            vec![BRAVO_ID]
        );
        db.close().expect("close after deleting alpha");
    }

    let db = Database::open(&path).expect("reopen after deleting alpha");
    assert_eq!(
        exact_ids(&db, ALPHA_SCOPE, vec![1.0, 0.0, 0.0], 1),
        Vec::<Uuid>::new()
    );
    assert_eq!(
        exact_ids(&db, BRAVO_SCOPE, vec![0.0, 1.0, 0.0], 1),
        vec![BRAVO_ID]
    );
}

#[test]
fn unrelated_relational_ddl_after_reopen_preserves_vectors_without_loading_them() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("relational-ddl.db");
    seed(&path);
    {
        let db = reopen(&path);
        db.execute(
            "CREATE INDEX lazy_scope_idx ON lazy_consumer_items (scope_id)",
            &empty(),
        )
        .expect("create an ordinary relational index");
        assert_eq!(
            db.vector_store_for_test()
                .resident_raw_partition_count_for_test(),
            0,
            "DDL unrelated to vector bodies must not hydrate either partition"
        );
        db.close().expect("close after ordinary DDL");
    }

    let db = Database::open(&path).expect("reopen after ordinary relational DDL");
    assert_eq!(
        exact_ids(&db, ALPHA_SCOPE, vec![1.0, 0.0, 0.0], 1),
        vec![ALPHA_ID]
    );
    assert_eq!(
        exact_ids(&db, BRAVO_SCOPE, vec![0.0, 1.0, 0.0], 1),
        vec![BRAVO_ID]
    );
}

#[test]
fn vector_rename_after_reopen_moves_dormant_bodies_without_losing_them() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("vector-rename.db");
    seed(&path);
    {
        let db = reopen(&path);
        db.execute(
            "ALTER TABLE lazy_consumer_items RENAME COLUMN embedding TO embedding_v2",
            &empty(),
        )
        .expect("rename the dormant vector column");
        db.close().expect("close after vector rename");
    }

    let db = Database::open(&path).expect("reopen after renaming dormant vectors");
    let result = db
        .execute(
            "SELECT id FROM lazy_consumer_items WHERE scope_id = $scope \
             ORDER BY embedding_v2 <=> $query USE VECTOR EXACT LIMIT 1",
            &params([
                ("scope", Value::Uuid(ALPHA_SCOPE)),
                ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("search the renamed dormant vector column");
    assert_eq!(ids(&result), vec![ALPHA_ID]);
}

#[test]
fn snapshot_inspection_counts_vectors_without_hydrating_the_inspection_copy_first() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("inspection-source.db");
    let artifact = root.path().join("inspection.snapshot");
    seed(&path);
    {
        let db = reopen(&path);
        db.export_snapshot(&artifact)
            .expect("export the complete dormant-vector state");
        db.close().expect("close the export source");
    }

    let inspector = SnapshotInspector::open(&artifact).expect("open the exported inspection copy");
    let report = inspector
        .inspect_sync_apply_state()
        .expect("inspect the complete exported state");
    assert_eq!(
        report.vectors, 2,
        "inspection counts durable vectors even before their raw bodies are resident"
    );
    inspector.close().expect("close the inspection copy");
}
