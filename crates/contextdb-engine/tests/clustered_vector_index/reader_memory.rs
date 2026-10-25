//! A search charges the index metadata it loads to the reader budget as whole
//! objects: the centroid table once at its full size, and each selected
//! Context directory once at its full size, never piece by piece. Everything
//! is returned when the search ends. A reader declared smaller than one list
//! page is refused with the reader's existing typed memory refusal, never a
//! panic, and keeps nothing.

use super::support::{
    drive_until_serving, empty, int_ids, ivf, list_page_bytes, observe_reader_charges,
    observe_search, open_caller_driven, params, unit_vector,
};
use contextdb_core::Value;
use contextdb_core::read_contract::{
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSIONS: usize = 16;
const ROWS_PER_CONTEXT: i64 = 300;
const SQL: &str = "SELECT id FROM charged_items WHERE context_id = $c0 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";

fn context(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xD0_0000 + ordinal)
}

fn store(path: &std::path::Path) -> Database {
    let db = open_caller_driven(path);
    db.execute(
        &format!(
            "CREATE TABLE charged_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) SEARCH_MODE INDEXED {})",
            ivf(8, 2, 32)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    let tx = db.begin_or_panic();
    for ordinal in 0..2_u128 {
        for row in 0..ROWS_PER_CONTEXT {
            let id = ordinal as i64 * ROWS_PER_CONTEXT + row;
            db.execute_in_tx(
                tx,
                "INSERT INTO charged_items (id, context_id, embedding) VALUES ($id, $context, $embedding)",
                &params([
                    ("id", Value::Int64(id)),
                    ("context", Value::Uuid(context(ordinal))),
                    ("embedding", Value::Vector(unit_vector(id as u64, DIMENSIONS))),
                ]),
            )
            .expect("stage one row");
        }
    }
    db.commit(tx).expect("commit both Contexts");
    drive_until_serving(&db);
    db
}

fn bound() -> HashMap<String, Value> {
    params([
        ("c0", Value::Uuid(context(0))),
        ("query", Value::Vector(unit_vector(11, DIMENSIONS))),
    ])
}

#[test]
fn centroids_and_a_selected_directory_are_charged_whole_and_released_at_operation_end() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = store(&root.path().join("charged.db"));
    let retained_before = db.declared_read_memory_for_test().retained_bytes_for_test();

    let ((result, work), charges) = observe_reader_charges(&db, || {
        observe_search(&db, || db.execute(SQL, &bound()).expect("indexed search"))
    });
    assert!(
        work.clustered_route,
        "the search ran on the clustered index"
    );
    assert_eq!(int_ids(&result)[0], 11, "the query's own row answers first");

    assert!(
        charges.centroid_table_bytes > 0,
        "the search loaded the centroid table: {charges:?}"
    );
    assert_eq!(
        charges.centroid_table_charges,
        vec![charges.centroid_table_bytes],
        "the centroid table is charged once, as one whole object: {charges:?}"
    );
    assert_eq!(
        charges.directory_object_bytes.len(),
        1,
        "a one-Context search selects one directory object: {charges:?}"
    );
    assert!(charges.directory_object_bytes[0] > 0);
    assert_eq!(
        charges.directory_object_charges, charges.directory_object_bytes,
        "each selected directory object is charged once, as one whole object: {charges:?}"
    );
    assert_eq!(
        charges.outstanding_after, 0,
        "nothing the search charged is outstanding after it returns: {charges:?}"
    );
    assert_eq!(
        db.declared_read_memory_for_test().retained_bytes_for_test(),
        retained_before,
        "the declared reader retains what it retained before the search"
    );
}

#[test]
fn a_reader_smaller_than_one_list_page_is_refused_with_the_typed_reader_refusal() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = store(&root.path().join("tight.db"));
    let page = list_page_bytes(&db, "charged_items", "embedding");
    assert!(page > 1, "a list page has a size");

    let roomy = db
        .read_session(ReadLimits::default())
        .expect("a default reader session");
    assert_eq!(
        int_ids(
            &roomy
                .execute(SQL, &bound())
                .expect("a default reader answers")
        )[0],
        11
    );

    let retained_before = db.declared_read_memory_for_test().retained_bytes_for_test();
    let tight = db
        .read_session(ReadLimits {
            memory: page / 2,
            ..ReadLimits::default()
        })
        .expect("a reader session smaller than one list page opens");
    let refused = tight
        .execute(SQL, &bound())
        .expect_err("a reader smaller than one list page cannot search");
    let typed_reader_refusal = match &refused {
        contextdb_core::Error::MemoryBudgetExceeded { subsystem, .. } => subsystem == "reader",
        contextdb_core::Error::ReadFailure(failure) => {
            failure.kind() == ReadFailureKind::OwnerLimitExceeded
                && matches!(
                    failure.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::Memory
                )
        }
        _ => false,
    };
    assert!(
        typed_reader_refusal,
        "the refusal is the reader's typed memory refusal: {refused:?}"
    );
    assert_eq!(
        db.declared_read_memory_for_test().retained_bytes_for_test(),
        retained_before,
        "a refused search returns every charge it took"
    );
}
