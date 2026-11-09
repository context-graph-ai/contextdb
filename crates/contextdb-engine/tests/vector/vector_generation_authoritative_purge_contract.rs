//! Authoritative-purge contract for partitioned vector columns.
//!
//! The public `PURGE` door permanently removes one selected lineage.  For a
//! partitioned vector column, that removal must remove the vectors of the
//! selected partition only.  A neighbour partition keeps every vector and its
//! answers, before and after a clean reopen.

use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_PARTITION: usize = 8;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
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

fn exact_search(db: &Database, scope: Uuid, query: Vec<f32>) -> Vec<Uuid> {
    ids(&db
        .execute(
            "SELECT id FROM purge_generation_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("exact search for partition {scope}: {error}")))
}

fn create_table(db: &Database) {
    db.execute(
        "CREATE TABLE purge_generation_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE EXACT\
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

/// Purging every lineage in alpha permanently removes alpha's vectors.  It
/// must not touch bravo's merely because both partitions share one declared
/// vector column.
#[test]
fn authoritative_purge_removes_only_the_selected_partitions_vectors_after_reopen() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root.path().join("authoritative-purge-generation.db");
    let alpha = Uuid::from_u128(0xA110);
    let bravo = Uuid::from_u128(0xB220);
    let alpha_first = Uuid::from_u128(0xA100_0000);
    let bravo_first = Uuid::from_u128(0xB200_0000);

    {
        let db = Database::open(&path).expect("open file-backed purge fixture");
        create_table(&db);
        seed_partition(&db, alpha, alpha_first, 0);
        seed_partition(&db, bravo, bravo_first, 1);

        assert_eq!(
            exact_search(&db, alpha, vec![1.0, 0.0, 0.0]),
            vec![alpha_first],
            "the selected partition answers before PURGE"
        );
        assert_eq!(
            exact_search(&db, bravo, vec![0.0, 1.0, 0.0]),
            vec![bravo_first],
            "the unrelated partition has a usable answer before PURGE"
        );

        let purged = db
            .execute(
                "PURGE FROM purge_generation_items WHERE scope_id = $scope",
                &params([("scope", Value::Uuid(alpha))]),
            )
            .expect("the public authoritative PURGE removes alpha's complete lineage");
        assert_eq!(
            purged.rows_affected(),
            ROWS_PER_PARTITION as u64,
            "the public purge selected every vector lineage in alpha and no bravo lineage"
        );
        assert_eq!(
            exact_search(&db, alpha, vec![1.0, 0.0, 0.0]),
            Vec::<Uuid>::new(),
            "no query may return a permanently purged vector"
        );
        assert_eq!(
            exact_search(&db, bravo, vec![0.0, 1.0, 0.0]),
            vec![bravo_first],
            "bravo remains queryable before close"
        );
        db.close().expect("close the purged store cleanly");
    }

    let db = Database::open(&path).expect("clean reopen after authoritative PURGE");
    assert_eq!(
        exact_search(&db, alpha, vec![1.0, 0.0, 0.0]),
        Vec::<Uuid>::new(),
        "clean reopen must not restore a query-addressable purged vector"
    );
    assert_eq!(
        exact_search(&db, bravo, vec![0.0, 1.0, 0.0]),
        vec![bravo_first],
        "the unrelated partition keeps its result after clean reopen"
    );
}
