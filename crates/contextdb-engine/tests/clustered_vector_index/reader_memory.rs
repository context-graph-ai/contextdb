//! A search charges each index metadata object it loads to the reader budget
//! as one whole object at the object's full size, never piece by piece: each
//! selected Context's centroid codebook, and each directory chunk or manifest
//! it actually loads. Directory chunks stream through a bounded window. Every
//! such object is released when the search returns, even while its answer is
//! still held; the answer keeps its own funding until it is dropped, pages the
//! cache keeps stay with the cache, and repeated searches return the reader to
//! the same retained level. A reader declared smaller than one list page is
//! refused with the reader's existing typed memory refusal, never a panic, and
//! keeps nothing.

use super::support::{
    MetadataObjectKind, ReaderCharges, drive_until_serving, empty, int_ids, ivf, list_page_bytes,
    observe_reader_charges, observe_search, open_caller_driven, params, unit_vector,
};
use contextdb_core::Value;
use contextdb_core::read_contract::{
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSIONS: usize = 16;
const CONTEXTS: u128 = 12;
const ROWS_PER_CONTEXT: i64 = 60;
const SQL: &str = "SELECT id FROM charged_items WHERE context_id = $c0 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";

fn context(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xD0_0000 + ordinal)
}

fn store(path: &std::path::Path) -> Database {
    let db = open_caller_driven(path);
    db.execute(
        &format!(
            "CREATE TABLE charged_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) SEARCH_MODE INDEXED {})",
            ivf(4, 2, 32)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    let tx = db.begin_or_panic();
    for ordinal in 0..CONTEXTS {
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
    db.commit(tx).expect("commit every Context");
    drive_until_serving(&db, CONTEXTS as usize * ROWS_PER_CONTEXT as usize, 1, 10);
    db
}

fn bound() -> HashMap<String, Value> {
    params([
        ("c0", Value::Uuid(context(0))),
        ("query", Value::Vector(unit_vector(11, DIMENSIONS))),
    ])
}

const ALL_CONTEXTS_SQL: &str =
    "SELECT id FROM charged_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";

/// Every loaded object is funded once at its full size and released when the
/// search returned; directory chunks stayed inside the admitted window.
fn assert_whole_objects_released(charges: &ReaderCharges, scope: &str) {
    assert!(
        charges
            .objects
            .iter()
            .any(|object| object.kind == MetadataObjectKind::DirectoryChunk),
        "{scope}: the search loaded directory chunks: {charges:?}"
    );
    for object in &charges.objects {
        assert!(object.object_bytes > 0, "{scope}: {object:?} has a size");
        assert_eq!(
            object.charges,
            vec![object.object_bytes],
            "{scope}: {object:?} is funded by one whole-object charge of its full size"
        );
        assert_eq!(
            object.outstanding_after, 0,
            "{scope}: {object:?} is released when the search returns"
        );
    }
    assert!(
        charges.directory_window >= 1,
        "{scope}: the directory window admits at least one chunk: {charges:?}"
    );
    assert!(
        (1..=charges.directory_window).contains(&charges.peak_resident_directory_chunks),
        "{scope}: resident directory chunks never exceed the window: {charges:?}"
    );
}

fn codebook_contexts(charges: &ReaderCharges) -> Vec<Vec<Value>> {
    charges
        .objects
        .iter()
        .filter(|object| object.kind == MetadataObjectKind::Codebook)
        .map(|object| object.context.clone())
        .collect()
}

fn all_contexts_bound() -> HashMap<String, Value> {
    params([("query", Value::Vector(unit_vector(11, DIMENSIONS)))])
}

#[test]
fn index_metadata_is_charged_whole_and_released_while_the_answer_keeps_its_own_funding() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = store(&root.path().join("charged.db"));
    let retained = || db.declared_read_memory_for_test().retained_bytes_for_test();

    // One Context: that Context's codebook, once.
    let ((answer, work), charges) = observe_reader_charges(&db, || {
        observe_search(&db, || {
            db.execute(SQL, &bound()).expect("one-Context search")
        })
    });
    assert!(
        work.clustered_route,
        "the search ran on the clustered index"
    );
    assert_whole_objects_released(&charges, "one Context");
    assert_eq!(
        codebook_contexts(&charges),
        vec![vec![Value::Uuid(context(0))]],
        "a one-Context search loads that Context's codebook once: {charges:?}"
    );
    // The answer outlives every metadata object and is still readable.
    assert_eq!(int_ids(&answer)[0], 11, "the query's own row answers first");
    let with_answer = retained();
    drop(answer);
    let after_answer = retained();
    assert!(
        with_answer > after_answer,
        "the held answer keeps its own reader funding until it is dropped: \
         {with_answer} while held, {after_answer} after"
    );

    // Every Context: one codebook per Context, directory chunks windowed.
    let ((answer, work), charges) = observe_reader_charges(&db, || {
        observe_search(&db, || {
            db.execute(ALL_CONTEXTS_SQL, &all_contexts_bound())
                .expect("all-Context search")
        })
    });
    assert!(
        work.clustered_route,
        "the search ran on the clustered index"
    );
    assert_whole_objects_released(&charges, "all Contexts");
    let mut loaded = codebook_contexts(&charges)
        .into_iter()
        .map(|tuple| match tuple.as_slice() {
            [Value::Uuid(context)] => *context,
            other => panic!("a Context is one UUID partition key, got {other:?}"),
        })
        .collect::<Vec<_>>();
    loaded.sort();
    assert_eq!(
        loaded,
        (0..CONTEXTS).map(context).collect::<Vec<_>>(),
        "an all-Context search loads each Context's codebook exactly once: {charges:?}"
    );
    assert_eq!(
        answer.rows().len(),
        5,
        "the all-Context answer is a full page"
    );
    drop(answer);

    // Final release: once answers are dropped, repeating the same searches
    // returns the reader to the same retained level; nothing accumulates per
    // search, while pages the cache keeps may stay charged to it.
    let settled = retained();
    for _ in 0..3 {
        drop(db.execute(SQL, &bound()).expect("one-Context search"));
        drop(
            db.execute(ALL_CONTEXTS_SQL, &all_contexts_bound())
                .expect("all-Context search"),
        );
    }
    assert_eq!(
        retained(),
        settled,
        "repeated searches leave the reader exactly where the first ones left it"
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
