use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, ReadSession};
use std::collections::HashMap;
use uuid::Uuid;

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn vector_ids(reader: &ReadSession, scope: &str, mode: &str) -> Vec<Uuid> {
    let result = reader
        .execute(
            &format!(
                "SELECT id FROM compact_vectors WHERE scope = $scope \
                 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
            ),
            &params([
                ("scope", Value::Text(scope.to_owned())),
                ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap_or_else(|error| panic!("{mode} search in {scope}: {error}"));
    if mode == "INDEXED" {
        assert_eq!(
            result.trace().vector_search.as_ref().unwrap().resolved_mode,
            VectorSearchMode::Indexed
        );
    }
    ids(&result)
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    result
        .rows()
        .iter()
        .map(|row| match row.first() {
            Some(Value::Uuid(id)) => *id,
            value => panic!("expected UUID id, got {value:?}"),
        })
        .collect()
}

#[test]
fn committed_vector_update_then_compact_publishes_one_generation_to_new_readers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("vector-update-compact.db");
    let [first, moved, rolled_back, deleted] = [1, 2, 3, 4].map(Uuid::from_u128);
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE compact_vectors (id UUID PRIMARY KEY, scope TEXT NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2 \
         SEARCH_MODE INDEXED AUTO_INDEX_AT 1) SYNC OFF",
        &HashMap::new(),
    )
    .unwrap();
    for (id, scope, vector) in [
        (first, "alpha", vec![1.0, 0.0, 0.0]),
        (moved, "alpha", vec![0.8, 0.6, 0.0]),
        (rolled_back, "beta", vec![0.0, 1.0, 0.0]),
        (deleted, "alpha", vec![0.0, 0.0, 1.0]),
    ] {
        db.execute(
            "INSERT INTO compact_vectors VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Text(scope.to_owned())),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .unwrap();
    }
    for _ in 0..8 {
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 2 && status.remaining_partitions == 0 {
            break;
        }
        db.run_maintenance_cycle().unwrap();
    }
    assert_eq!(
        db.vector_store_for_test()
            .hnsw_maintenance_status()
            .ready_partitions,
        2
    );
    db.close().unwrap();

    let old = ReadSession::open(&path).unwrap();
    let old_snapshot = old.file_snapshot_for_test().unwrap();
    let old_alpha = vec![first, moved, deleted];
    assert_eq!(vector_ids(&old, "alpha", "EXACT"), old_alpha);
    assert_eq!(vector_ids(&old, "alpha", "INDEXED"), old_alpha);

    let writer = Database::open(&path).unwrap();
    writer.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let tx = writer.begin().unwrap();
    writer
        .execute_in_tx(
            tx,
            "UPDATE compact_vectors SET embedding = $embedding WHERE id = $id",
            &params([
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
                ("id", Value::Uuid(rolled_back)),
            ]),
        )
        .unwrap();
    writer.rollback(tx).unwrap();
    assert_eq!(
        writer
            .execute(
                "SELECT embedding FROM compact_vectors WHERE id = $id",
                &params([("id", Value::Uuid(rolled_back))]),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Vector(vec![0.0, 1.0, 0.0])]],
        "rollback publishes neither a vector body nor membership",
    );
    writer
        .execute(
            "UPDATE compact_vectors SET scope = 'beta', embedding = $embedding WHERE id = $id",
            &params([
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
                ("id", Value::Uuid(moved)),
            ]),
        )
        .unwrap();
    writer
        .execute(
            "DELETE FROM compact_vectors WHERE id = $id",
            &params([("id", Value::Uuid(deleted))]),
        )
        .unwrap();
    writer
        .execute(
            "UPDATE compact_vectors SET embedding = $embedding WHERE id = $id",
            &params([
                ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
                ("id", Value::Uuid(first)),
            ]),
        )
        .unwrap();
    assert!(writer.compact_now().unwrap().ran);
    writer.close().unwrap();

    assert_eq!(vector_ids(&old, "alpha", "EXACT"), old_alpha);
    assert_eq!(vector_ids(&old, "alpha", "INDEXED"), old_alpha);
    assert_eq!(old.file_snapshot_for_test(), Some(old_snapshot));

    let fresh = ReadSession::open(&path).unwrap();
    assert_ne!(fresh.file_snapshot_for_test(), Some(old_snapshot));
    assert_eq!(vector_ids(&fresh, "alpha", "EXACT"), vec![first]);
    assert_eq!(vector_ids(&fresh, "alpha", "INDEXED"), vec![first]);
    assert_eq!(
        vector_ids(&fresh, "beta", "EXACT"),
        vec![moved, rolled_back]
    );
    assert_eq!(
        vector_ids(&fresh, "beta", "INDEXED"),
        vec![moved, rolled_back]
    );
    drop(fresh);
    drop(old);

    let restarted = ReadSession::open(&path).unwrap();
    assert_eq!(vector_ids(&restarted, "alpha", "INDEXED"), vec![first]);
    assert_eq!(
        vector_ids(&restarted, "beta", "INDEXED"),
        vec![moved, rolled_back]
    );
}
