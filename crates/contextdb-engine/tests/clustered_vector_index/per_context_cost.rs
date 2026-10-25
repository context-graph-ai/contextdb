//! Searching one Context costs that Context, not the store: a one-Context
//! search reads strictly fewer list pages than an all-Context search on the
//! same store and never reads another Context's list-segment pages; a
//! several-Context search reads exactly the Contexts it names. Each Context
//! has its own published generation with the declared list count.

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
