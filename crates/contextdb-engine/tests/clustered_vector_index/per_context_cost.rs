//! Searching one Context costs that Context, not the store: a one-Context
//! search reads strictly fewer list pages than an all-Context search on the
//! same store and never reads another Context's list-segment pages; a
//! several-Context search reads exactly the Contexts it names. Each Context
//! has its own published generation with the declared list count, so an
//! unrelated Context growing and publishing a replacement leaves an unchanged
//! Context's codebook, list count and search work exactly as they were.

use super::support::{
    SearchWork, drive_until_serving, empty, ivf, observe_search, open_caller_driven, params,
    serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use uuid::Uuid;

const DIMENSIONS: usize = 16;
const CONTEXTS: u128 = 4;
const ROWS_PER_CONTEXT: i64 = 150;

fn context(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xC0_0000 + ordinal)
}

fn row_vector(id: i64) -> Vec<f32> {
    unit_vector(id as u64, DIMENSIONS)
}

fn observed(db: &Database, filter: &str, bound: &[(String, Value)]) -> SearchWork {
    let mut bound = bound
        .iter()
        .cloned()
        .collect::<std::collections::HashMap<_, _>>();
    bound.insert("query".to_owned(), Value::Vector(row_vector(3)));
    let sql = format!(
        "SELECT id FROM scoped_items {filter} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
    );
    let (result, work) = observe_search(db, || db.execute(&sql, &bound).expect("scoped search"));
    assert_eq!(result.rows().len(), 10, "{sql} answers a full page");
    assert!(work.clustered_route, "{sql} ran on the clustered index");
    work
}

fn uuid_set(work: &SearchWork) -> Vec<Uuid> {
    let mut contexts = work
        .contexts_read
        .iter()
        .map(|tuple| match tuple.as_slice() {
            [Value::Uuid(context)] => *context,
            other => panic!("a Context is one UUID partition key, got {other:?}"),
        })
        .collect::<Vec<_>>();
    contexts.sort();
    contexts
}

#[test]
fn a_one_context_search_reads_only_its_own_segments_and_fewer_pages_than_all() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("scoped.db"));
    db.execute(
        &format!(
            "CREATE TABLE scoped_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) SEARCH_MODE INDEXED {})",
            ivf(8, 2, 32)
        ),
        &empty(),
    )
    .expect("declare the per-Context clustered index");
    let tx = db.begin_or_panic();
    for ordinal in 0..CONTEXTS {
        for row in 0..ROWS_PER_CONTEXT {
            let id = ordinal as i64 * ROWS_PER_CONTEXT + row;
            db.execute_in_tx(
                tx,
                "INSERT INTO scoped_items (id, context_id, embedding) VALUES ($id, $context, $embedding)",
                &params([
                    ("id", Value::Int64(id)),
                    ("context", Value::Uuid(context(ordinal))),
                    ("embedding", Value::Vector(row_vector(id))),
                ]),
            )
            .expect("stage one row");
        }
    }
    db.commit(tx).expect("commit every Context");
    drive_until_serving(&db);
    let generations = (0..CONTEXTS)
        .map(|ordinal| {
            let generation = serving(
                &db,
                "scoped_items",
                "embedding",
                &[Value::Uuid(context(ordinal))],
            )
            .unwrap_or_else(|| panic!("Context {ordinal} has its own serving generation"));
            assert_eq!(
                generation.lists, 8,
                "each Context's generation has the declared lists"
            );
            generation.generation
        })
        .collect::<Vec<_>>();
    assert_eq!(generations.len(), CONTEXTS as usize);

    let one = observed(
        &db,
        "WHERE context_id = $c0",
        &[("c0".to_owned(), Value::Uuid(context(0)))],
    );
    let several = observed(
        &db,
        "WHERE context_id IN ($c0, $c1)",
        &[
            ("c0".to_owned(), Value::Uuid(context(0))),
            ("c1".to_owned(), Value::Uuid(context(1))),
        ],
    );
    let all = observed(&db, "", &[]);

    assert_eq!(
        uuid_set(&one),
        vec![context(0)],
        "a one-Context search reads only that Context's segment pages: {one:?}"
    );
    assert_eq!(
        uuid_set(&several),
        vec![context(0), context(1)],
        "a several-Context search reads exactly the named Contexts: {several:?}"
    );
    assert_eq!(
        uuid_set(&all),
        (0..CONTEXTS).map(context).collect::<Vec<_>>(),
        "an all-Context search reads every Context: {all:?}"
    );
    assert!(
        one.list_pages_read > 0,
        "the one-Context search read list pages: {one:?}"
    );
    assert!(
        one.list_pages_read < all.list_pages_read,
        "one Context reads strictly fewer list pages than all Contexts: one {one:?}, all {all:?}"
    );
}

const TARGET_ROWS: i64 = 150;
/// Each unrelated Context starts with N rows and grows to 2N.
const UNRELATED_N: i64 = 1_000;
const TARGET_QUERIES: [u64; 4] = [3, 40, 77, 120];

fn grown_context(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xC1_0000 + ordinal)
}

fn insert_rows(db: &Database, ordinal: u128, ids: std::ops::Range<i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO grown_items (id, context_id, embedding) VALUES ($id, $context, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("context", Value::Uuid(grown_context(ordinal))),
                ("embedding", Value::Vector(row_vector(id))),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the Context's rows");
}

/// The answer and complete work of each fixed query against the target
/// Context alone.
fn target_searches(db: &Database) -> Vec<(Vec<i64>, SearchWork)> {
    TARGET_QUERIES
        .iter()
        .map(|seed| {
            let bound = params([
                ("target", Value::Uuid(grown_context(0))),
                ("query", Value::Vector(row_vector(*seed as i64))),
            ]);
            let (result, work) = observe_search(db, || {
                db.execute(
                    "SELECT id FROM grown_items WHERE context_id = $target ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
                    &bound,
                )
                .expect("target-Context search")
            });
            assert!(work.clustered_route, "the target search ran on the clustered index");
            assert_eq!(
                uuid_set(&work),
                vec![grown_context(0)],
                "the target search reads only the target Context: {work:?}"
            );
            (super::support::int_ids(&result), work)
        })
        .collect()
}

#[test]
fn an_unrelated_context_growing_and_republishing_leaves_a_fixed_context_s_work_unchanged() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("grown.db"));
    // LISTS and PROBES are left to their population defaults, so a rule that
    // resolved them over the whole column would move with unrelated growth.
    db.execute(
        &format!(
            "CREATE TABLE grown_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) SEARCH_MODE INDEXED IVF (RERANK = 32) CONSOLIDATION (CHANGE_PERCENT = 10))"
        ),
        &empty(),
    )
    .expect("declare the per-Context clustered index");
    insert_rows(&db, 0, 0..TARGET_ROWS);
    insert_rows(&db, 1, 10_000..10_000 + UNRELATED_N);
    insert_rows(&db, 2, 20_000..20_000 + UNRELATED_N);
    drive_until_serving(&db);

    let generation = |ordinal: u128| {
        serving(
            &db,
            "grown_items",
            "embedding",
            &[Value::Uuid(grown_context(ordinal))],
        )
        .unwrap_or_else(|| panic!("Context {ordinal} has a serving generation"))
    };
    let target_before = generation(0);
    let unrelated_before = [generation(1), generation(2)];
    let work_before = target_searches(&db);

    insert_rows(&db, 1, 10_000 + UNRELATED_N..10_000 + 2 * UNRELATED_N);
    insert_rows(&db, 2, 20_000 + UNRELATED_N..20_000 + 2 * UNRELATED_N);
    assert_eq!(
        generation(0),
        target_before,
        "unrelated growth leaves the target's generation, codebook and list count alone"
    );
    assert_eq!(
        target_searches(&db),
        work_before,
        "after unrelated N to 2N growth the target's answers and centroid, candidate, \
         canonical, buffer, list, visibility, re-rank and directory work are exactly equal"
    );

    drive_until_serving(&db);
    for (index, ordinal) in [1_u128, 2].into_iter().enumerate() {
        assert_ne!(
            generation(ordinal).generation,
            unrelated_before[index].generation,
            "unrelated Context {ordinal} published a replacement generation"
        );
    }
    assert_eq!(
        generation(0),
        target_before,
        "the unrelated replacements leave the target's generation, codebook and list count alone"
    );
    assert_eq!(
        target_searches(&db),
        work_before,
        "after the unrelated replacements publish, the target's answers and every work count \
         are exactly equal"
    );
}
