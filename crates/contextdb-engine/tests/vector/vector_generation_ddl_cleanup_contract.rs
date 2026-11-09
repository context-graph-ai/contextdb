//! Vector identity contracts for legal vector-column DDL.
//!
//! These journeys use only the public database door to create, populate,
//! rename, drop, close, and reopen a store.  The test-only vector observation
//! merely proves that a vector identity is present or absent.

use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

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

fn index_is_present(db: &Database, expected: &VectorIndexRef) -> bool {
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

    db.with_vector_store_for_test(|store| {
        store
            .index_infos(&mut vector_fixture_copy_1)
            .expect("read or publish the admitted vector fixture")
    })
    .into_iter()
    .any(|info| info.index == *expected)
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns()
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows()
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn exact_search(db: &Database, column: &str, scope: Uuid, query: Vec<f32>) -> Vec<Uuid> {
    ids(&db
        .execute(
            &format!(
                "SELECT id FROM generation_ddl_items WHERE scope_id = $scope \
                 ORDER BY {column} <=> $query USE VECTOR EXACT LIMIT 1"
            ),
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("exact search through {column}: {error}")))
}

fn create_and_seed_partition(db: &Database, scope: Uuid, first: Uuid) {
    db.execute(
        "CREATE TABLE generation_ddl_items (
            id UUID PRIMARY KEY,
            scope_id UUID NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE EXACT
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
}

/// A caller that renames a vector column keeps its one vector: reopening uses
/// the renamed identity and does not retain the old one.
#[test]
fn rename_moves_a_partitioned_vector_column_to_the_new_identity() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("rename-generation.db");
    let scope = Uuid::from_u128(0xD001);
    let first = Uuid::from_u128(0xD100);
    let old_index = VectorIndexRef::new("generation_ddl_items", "embedding");
    let renamed_index = VectorIndexRef::new("generation_ddl_items", "embedding_v2");

    {
        let db = Database::open(&path).expect("open file-backed fixture");
        create_and_seed_partition(&db, scope, first);
        assert!(
            index_is_present(&db, &old_index),
            "the fixture declares the vector identity before DDL"
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
            "rename moves the vector identity instead of creating a copy"
        );
        db.close().expect("close the renamed store cleanly");
    }

    let db = Database::open(&path).expect("clean reopen after vector-column rename");
    assert!(
        !index_is_present(&db, &old_index),
        "clean reopen must not restore the old vector identity"
    );
    assert!(
        index_is_present(&db, &renamed_index),
        "clean reopen must retain the renamed vector identity"
    );
    assert_eq!(
        exact_search(&db, "embedding_v2", scope, vec![1.0, 0.0, 0.0]),
        vec![first],
        "the renamed vector column returns the stored vector after clean reopen"
    );
    assert!(matches!(
        db.execute(
            "SELECT id FROM generation_ddl_items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
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
        create_and_seed_partition(&db, scope, first);
        db.execute(
            "ALTER TABLE generation_ddl_items RENAME COLUMN embedding TO embedding_v2",
            &empty(),
        )
        .expect("rename the generated vector column before dropping it");
        assert!(
            index_is_present(&db, &renamed_index),
            "rename must carry the vector identity up to the legal drop"
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
                     ORDER BY {column} <=> $query USE VECTOR EXACT LIMIT 1"
                ),
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            ),
            Err(Error::UnknownVectorIndex { index: actual }) if actual == index
        ));
    }
}

/// Catalog and paged bindings share a commit: either name is wholly usable.
#[test]
#[cfg(unix)]
fn rename_crash_keeps_catalog_and_paged_bindings_atomic() {
    use std::os::unix::process::ExitStatusExt;
    const CHILD_PATH: &str = "CONTEXTDB_SEALED_RENAME_CRASH_PATH";
    const CHILD_BOUNDARY: &str = "CONTEXTDB_SEALED_RENAME_CRASH_BOUNDARY";
    if let Ok(path) = std::env::var(CHILD_PATH) {
        let boundary = std::env::var(CHILD_BOUNDARY).unwrap().parse().unwrap();
        let db = Database::open(path).unwrap();
        db.arm_table_meta_rewrite_crash_for_test(boundary);
        let outcome = db.execute(
            "ALTER TABLE generation_ddl_items RENAME COLUMN embedding TO embedding_v2",
            &empty(),
        );
        panic!("armed rename checkpoint did not abort: {outcome:?}");
    }
    for boundary in [1, 2, 3] {
        let root = TempDir::new().unwrap();
        let path = root.path().join("atomic-rename.db");
        let scope = Uuid::from_u128(0xD001);
        let first = Uuid::from_u128(0xD100);
        {
            let db = Database::open(&path).unwrap();
            create_and_seed_partition(&db, scope, first);
            db.close().unwrap();
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "vector_generation_ddl_cleanup_contract::rename_crash_keeps_catalog_and_paged_bindings_atomic", "--nocapture"])
            .env(CHILD_PATH, &path)
            .env(CHILD_BOUNDARY, boundary.to_string())
            .output().unwrap();
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("TABLE_META_REWRITE_CRASH={boundary}"))
        );
        assert_eq!(output.status.signal(), Some(6));
        let db = Database::open(&path).unwrap();
        let (present, absent) = if boundary == 3 {
            ("embedding_v2", "embedding")
        } else {
            ("embedding", "embedding_v2")
        };
        let present_index = VectorIndexRef::new("generation_ddl_items", present);
        assert!(index_is_present(&db, &present_index));
        assert!(!index_is_present(
            &db,
            &VectorIndexRef::new("generation_ddl_items", absent)
        ));
        assert_eq!(
            exact_search(&db, present, scope, vec![1.0, 0.0, 0.0]),
            vec![first]
        );
        db.close().unwrap();
    }
}
