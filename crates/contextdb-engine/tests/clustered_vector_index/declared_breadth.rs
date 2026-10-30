//! A declared breadth bounds a search's work exactly. With explicit
//! `IVF (LISTS, PROBES, REFINE_FACTOR)`, every searched Context probes exactly
//! PROBES of its lists, reads exactly those lists' pages, and rescores exactly
//! ceil(LIMIT x REFINE_FACTOR) candidates at full precision, the same per
//! Context over one, three or eight Contexts and never a multiple of it; and
//! inspection lists the declared pair at every depth.

use super::support::{
    SearchRoute, drive_until_serving, empty, inspected_search_settings, list_pages, nearest_lists,
    observe_search, open_caller_driven, params, unit_vector,
};
use contextdb_core::Value;
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSIONS: usize = 16;
const CONTEXTS: u128 = 8;
// The smallest equal-sized groups whose PROBES lists can supply the full
// LIMIT * REFINE_FACTOR shortlist, while every declared list stays nonempty.
const ROWS_PER_CONTEXT: i64 =
    (LISTS as u64 * (LIMIT * REFINE_FACTOR as u64).div_ceil(PROBES)) as i64;
const LISTS: u32 = 8;
const PROBES: u64 = 3;
const REFINE_FACTOR: u32 = 2;
const LIMIT: u64 = 10;

fn context(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xB7_0000 + ordinal)
}

#[test]
fn explicit_probes_and_refine_factor_bound_list_pages_and_rescores_exactly() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("breadth.db"));
    db.execute(
        &format!(
            "CREATE TABLE breadth_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) SEARCH_MODE INDEXED IVF (LISTS = {LISTS}, PROBES = {PROBES}, REFINE_FACTOR = {REFINE_FACTOR}.00))"
        ),
        &empty(),
    )
    .expect("declare an explicit breadth");
    let tx = db.begin_or_panic();
    for ordinal in 0..CONTEXTS {
        for row in 0..ROWS_PER_CONTEXT {
            let id = ordinal as i64 * ROWS_PER_CONTEXT + row;
            db.execute_in_tx(
                tx,
                "INSERT INTO breadth_items (id, context_id, embedding) VALUES ($id, $context, $embedding)",
                &params([
                    ("id", Value::Int64(id)),
                    ("context", Value::Uuid(context(ordinal))),
                    ("embedding", Value::Vector(unit_vector(row as u64 % u64::from(LISTS), DIMENSIONS))),
                ]),
            )
            .expect("stage one row");
        }
    }
    db.commit(tx).expect("commit every Context");
    drive_until_serving(&db, CONTEXTS as usize * ROWS_PER_CONTEXT as usize, 1, 10);

    for ordinal in 0..CONTEXTS {
        let settings = inspected_search_settings(
            &db,
            "breadth_items",
            "embedding",
            &[Value::Uuid(context(ordinal))],
        );
        assert!(
            !settings.is_empty(),
            "Context {ordinal}: inspection lists its search settings"
        );
        for setting in &settings {
            assert_eq!(
                (setting.probes, setting.refine_factor),
                (PROBES, f64::from(REFINE_FACTOR)),
                "Context {ordinal}: the explicit declaration replaces the pair at every depth: \
                 {settings:?}"
            );
        }
    }

    let query = unit_vector(31_337, DIMENSIONS);
    let rescored_per_context = LIMIT * u64::from(REFINE_FACTOR);
    for scope in [1_u128, 3, CONTEXTS] {
        let mut bound = HashMap::from([("query".to_owned(), Value::Vector(query.clone()))]);
        let placeholders = (0..scope)
            .map(|ordinal| {
                let name = format!("c{ordinal}");
                bound.insert(name.clone(), Value::Uuid(context(ordinal)));
                format!("${name}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT id FROM breadth_items WHERE context_id IN ({placeholders}) ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT {LIMIT}"
        );
        let (result, work) =
            observe_search(&db, || db.execute(&sql, &bound).expect("scoped search"));
        assert_eq!(
            result.rows().len() as u64,
            LIMIT,
            "{scope} Contexts: a full page"
        );
        assert_eq!(
            work.route,
            Some(SearchRoute::Indexed),
            "{scope} Contexts: {work:?}"
        );
        let expected_pages = (0..scope)
            .map(|ordinal| {
                let key = [Value::Uuid(context(ordinal))];
                let lists = nearest_lists(&db, "breadth_items", "embedding", &key, &query, PROBES);
                assert_eq!(
                    lists.len() as u64,
                    PROBES,
                    "Context {ordinal} probes {PROBES} lists"
                );
                lists
                    .into_iter()
                    .map(|list| list_pages(&db, "breadth_items", "embedding", &key, list))
                    .sum::<u64>()
            })
            .sum::<u64>();
        let contexts = scope as u64;
        assert_eq!(
            work.lists_probed,
            PROBES * contexts,
            "{scope} Contexts: each Context probes exactly PROBES lists: {work:?}"
        );
        assert_eq!(
            work.list_pages_read, expected_pages,
            "{scope} Contexts: the search reads exactly the probed lists' pages: {work:?}"
        );
        assert_eq!(
            work.full_precision_evaluations,
            rescored_per_context * contexts,
            "{scope} Contexts: each Context rescores exactly ceil(LIMIT x REFINE_FACTOR) \
             candidates: {work:?}"
        );
        assert_eq!(
            work.buffer_evaluations, 0,
            "{scope} Contexts: nothing is buffered"
        );
        assert_eq!(
            work.contexts_read.len() as u64,
            contexts,
            "{scope} Contexts: {work:?}"
        );
    }
}
