//! Damage to one route's clustered-index pages -- a flipped byte in its
//! codebook or a missing list page -- refuses that route and no other, and
//! the reader still opens. Damage to the schema metadata bound beside the
//! pages refuses every route.

use contextdb_core::{Error, Value};
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_ROUTE: u128 = 32;

fn alpha() -> Uuid {
    Uuid::from_u128(0xA218)
}

fn bravo() -> Uuid {
    Uuid::from_u128(0xB218)
}

fn ranked(axis: usize, ordinal: u128) -> Vec<f32> {
    let score = 1.0 - ordinal as f32 * 0.01;
    let remainder = (1.0 - score * score).max(0.0).sqrt();
    let mut vector = vec![0.0; 3];
    vector[axis] = score;
    vector[(axis + 1) % 3] = remainder;
    vector
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

/// The two committed routes under `search_mode`, with no index build.
fn seed_rows(path: &std::path::Path, search_mode: &str) -> Database {
    let db = Database::open(path).expect("open the durable two-route store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE paged_items (\
             id UUID PRIMARY KEY, \
             scope_id UUID NOT NULL, \
             embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE {search_mode}\
             )"
        ),
        &HashMap::new(),
    )
    .expect("declare the partitioned vector table");
    for (scope, first, axis) in [(alpha(), 0xA000, 0), (bravo(), 0xB000, 1)] {
        let tx = db.begin_or_panic();
        for ordinal in 0..ROWS_PER_ROUTE {
            db.insert_row(
                tx,
                "paged_items",
                HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Uuid(Uuid::from_u128(first + ordinal)),
                    ),
                    ("scope_id".to_owned(), Value::Uuid(scope)),
                    ("embedding".to_owned(), Value::Vector(ranked(axis, ordinal))),
                ]),
            )
            .expect("stage one route vector");
        }
        db.commit(tx).expect("commit one route");
    }
    db
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[expect(
    clippy::into_iter_on_ref,
    reason = "The fixture iterates the borrowed query-result rows without consuming the retained answer."
)]
fn find_as(
    reader: &ReadSession,
    scope: Uuid,
    query: Vec<f32>,
    mode: &str,
) -> contextdb_core::Result<Vec<Uuid>> {
    reader
        .execute(
            &format!(
                "SELECT id FROM paged_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 4"
            ),
            &HashMap::from([
                ("scope".to_owned(), Value::Uuid(scope)),
                ("query".to_owned(), Value::Vector(query)),
            ]),
        )
        .map(|result| {
            result
                .rows()
                .into_iter()
                .map(|row| match row.as_slice() {
                    [Value::Uuid(id)] => *id,
                    other => panic!("one id column: {other:?}"),
                })
                .collect()
        })
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn schema_metadata_damage_is_not_treated_as_page_damage() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("paged-schema-damage.redb");
    seed_rows(&path, "AUTO")
        .close()
        .expect("close the committed store");
    {
        let database = redb::Database::create(&path).expect("open the raw store");
        let write = database.begin_write().expect("raw write");
        write
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("meta"))
            .expect("the schema metadata table")
            .insert("zz-unbound-schema-damage", b"x".as_slice())
            .expect("damage the schema metadata without rebinding");
        write.commit().expect("commit the damage");
    }
    let outcome = ReadSession::open_with_options(&path, ReadSessionOptions::default())
        .and_then(|session| find_as(&session, bravo(), axis(1), "EXACT"));
    assert!(
        matches!(outcome, Err(Error::StoreCorrupted { .. })),
        "schema metadata damage refuses even the undamaged route, got {outcome:?}"
    );
}
