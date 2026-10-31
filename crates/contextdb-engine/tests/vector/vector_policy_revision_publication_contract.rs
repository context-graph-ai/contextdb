//! Online vector-policy revision publication through caller-driven maintenance
//! and indexed search. A preparation pause holds a real unpublished build while
//! queries and newer declarations proceed; its timeout only guards deadlock.

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, MaintenancePolicy, QueryResult, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

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
    VectorIndexRef::new("revision_docs", "embedding")
}

fn scope() -> Uuid {
    Uuid::from_u128(0x6101)
}

fn partition() -> VectorPartitionRef {
    VectorPartitionRef::new(
        index(),
        VectorPartitionKey::from_values(&[Value::Uuid(scope())])
            .expect("the fixture UUID has one canonical partition key"),
    )
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

fn create_current_control(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE revision_docs (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) SEARCH_MODE INDEXED\
         )",
        &empty(),
    )
    .expect("create current maintained route control");
    for (id, embedding) in [
        (Uuid::from_u128(0x6111), axis(0)),
        (Uuid::from_u128(0x6112), vec![0.9, 0.1, 0.0]),
        (Uuid::from_u128(0x6113), axis(1)),
    ] {
        db.execute(
            "INSERT INTO revision_docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Uuid(scope())),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("seed current maintained route control");
    }
}

#[test]
fn registered_layout_checks_every_authored_policy_member_and_revision_before_insert() {
    use contextdb_core::Error;

    let db = Database::open_memory();
    create_current_control(&db);
    let store = db.vector_store_for_test();
    let vector_fixture_reader_3 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_3 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_3.clone());

    let expected = store
        .index_layout(&index(), &mut vector_fixture_copy_3)
        .unwrap();
    let metadata = db.table_meta("revision_docs").unwrap();
    for field in ["auto_index_at", "policy_revision"] {
        let mut stale = expected.clone();
        let vector_fixture_reader_4 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_4 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_4.clone());
        let vector_fixture_retained_4 = store
            .retained_column_copy_owner(&index(), &vector_fixture_copy_4)
            .expect("borrow the existing retained column owner");

        match field {
            "auto_index_at" => stale.auto_index_at = Some(1_000),
            "policy_revision" => stale.policy_revision += 1,
            _ => unreachable!(),
        }
        store
            .register_index_with_layout(
                index(),
                stale,
                vector_fixture_retained_4.accountant_for_test().clone(),
                &mut vector_fixture_copy_4,
            )
            .unwrap();
        let error = db
            .execute(
                "INSERT INTO revision_docs (id, scope_id, embedding) VALUES ($id, $scope, $vector)",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(0x61FF))),
                    ("scope", Value::Uuid(scope())),
                    ("vector", Value::Vector(axis(0))),
                ]),
            )
            .expect_err("a stale registered layout refuses before row/vector publication");
        assert!(
            matches!(&error, Error::Other(message) if message.starts_with("registered vector layout does not match the durable declaration")),
            "{field}: {error:?}"
        );
        let vector_fixture_reader_5 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_5 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_5.clone());
        let vector_fixture_retained_5 = store
            .retained_column_copy_owner(&index(), &vector_fixture_copy_5)
            .expect("borrow the existing retained column owner");

        store
            .register_index_with_layout(
                index(),
                expected.clone(),
                vector_fixture_retained_5.accountant_for_test().clone(),
                &mut vector_fixture_copy_5,
            )
            .unwrap();
        assert_eq!(db.table_meta("revision_docs").unwrap(), metadata);
        assert_eq!(
            db.execute("SELECT id FROM revision_docs", &empty())
                .unwrap()
                .rows()
                .len(),
            3
        );
        let vector_fixture_reader_6 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_6 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_6.clone());

        assert_eq!(
            store
                .partition_info(
                    &index(),
                    &partition().partition_key,
                    &mut vector_fixture_copy_6
                )
                .unwrap()
                .unwrap()
                .live_rows,
            3
        );
    }
    db.execute(
        "INSERT INTO revision_docs (id, scope_id, embedding) VALUES ($id, $scope, $vector)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x61FF))),
            ("scope", Value::Uuid(scope())),
            ("vector", Value::Vector(axis(0))),
        ]),
    )
    .expect("matching declarations still admit the same row and vector");
    assert_eq!(
        db.execute("SELECT id FROM revision_docs", &empty())
            .unwrap()
            .rows()
            .len(),
        4
    );
}
