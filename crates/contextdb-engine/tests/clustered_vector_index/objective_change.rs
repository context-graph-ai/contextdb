//! Changing a Context's search objective never relabels an old setting as
//! meeting a target it was not qualified for. After TARGET_ACCURACY is
//! raised, AUTO keeps the old qualified setting only for the LIMITs whose
//! stored same-route prefix summaries still meet the new target on every
//! sample query, and takes EXACT, with the exact answer, everywhere else;
//! explicit INDEXED keeps serving but is not reported as meeting the new
//! target; inspection shows requalification pending while the old
//! generation serves. A changed EXPECTED_VECTORS leaves the accuracy target,
//! and so the covered LIMITs, as they were, with requalification pending.
//! Maintenance publishes a requalified generation, after which nothing is
//! pending and AUTO takes the index at the LIMITs the new qualification
//! covers.
//!
//! Here one sample query's depth-20 answer lacks its exact nearest
//! neighbour, so at LIMIT 20 that sample finds 19 of 20 (95%): enough for
//! the default target of 95, not for 99. Every depth-50 answer is exact.

use super::support::{
    ObjectiveStatus, SearchRoute, SearchWork, calibration_receipt, drive_until_serving, empty,
    int_ids, observe_search, open_caller_driven, params, requalification_pending,
    rewrite_next_calibration_answers, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 16;
/// Large enough that the indexed route can do less work than EXACT.
const ROWS: i64 = 4_000;

fn store(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE objective_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE AUTO)"
        ),
        &empty(),
    )
    .expect("declare a vector column with the derived search policy");
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO objective_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

fn search(db: &Database, mode: &str, limit: usize) -> (Vec<i64>, SearchWork) {
    let (result, work) = observe_search(db, || {
        db.execute(
            &format!(
                "SELECT id FROM objective_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
            ),
            &params([("query", Value::Vector(unit_vector(99_999, DIMENSIONS)))]),
        )
        .expect("vector search")
    });
    (int_ids(&result), work)
}

fn assert_auto_takes_the_index(db: &Database, limit: usize, depth: u32, why: &str) {
    let (_, auto) = search(db, "AUTO", limit);
    assert_eq!(
        (auto.route, auto.served_depth, auto.objective_status),
        (
            Some(SearchRoute::Indexed),
            Some(depth),
            Some(ObjectiveStatus::Met)
        ),
        "LIMIT {limit}, {why}: AUTO keeps the depth-{depth} setting: {auto:?}"
    );
}

fn assert_auto_takes_exact(db: &Database, limit: usize, why: &str) {
    let (auto_ids, auto) = search(db, "AUTO", limit);
    assert_eq!(
        auto.route,
        Some(SearchRoute::Exact),
        "LIMIT {limit}, {why}: AUTO takes EXACT: {auto:?}"
    );
    assert_eq!(
        auto_ids,
        search(db, "EXACT", limit).0,
        "AUTO's LIMIT {limit} is exact"
    );
    let (ids, indexed) = search(db, "INDEXED", limit);
    assert_eq!(ids.len(), limit, "explicit INDEXED keeps serving");
    assert_ne!(
        indexed.objective_status,
        Some(ObjectiveStatus::Met),
        "LIMIT {limit}, {why}: explicit INDEXED is never reported as meeting a target its \
         setting was not qualified for: {indexed:?}"
    );
}

fn qualified(root: &std::path::Path) -> Database {
    let db = open_caller_driven(&root.join("objective.db"));
    store(&db);
    let _rewritten = rewrite_next_calibration_answers(
        &db,
        "objective_items",
        "embedding",
        &[],
        Box::new(|query, depth, exact| {
            if query == 0 && depth == 20 {
                exact[1..].to_vec()
            } else {
                exact.to_vec()
            }
        }),
    );
    drive_until_serving(&db, ROWS as usize, 1, 10);
    db
}

#[test]
fn a_raised_target_keeps_only_the_settings_that_meet_it_until_requalification() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = qualified(root.path());
    let generation = serving(&db, "objective_items", "embedding", &[])
        .expect("a generation serves")
        .generation;
    assert!(!requalification_pending(
        &db,
        "objective_items",
        "embedding",
        &[]
    ));
    assert_auto_takes_the_index(&db, 20, 20, "fixture at the default target of 95");
    assert_auto_takes_the_index(&db, 50, 50, "fixture at the default target of 95");

    db.execute(
        "ALTER TABLE objective_items ALTER COLUMN embedding SET SEARCH (TARGET_ACCURACY = 99)",
        &empty(),
    )
    .expect("raise the accuracy target");
    assert!(
        requalification_pending(&db, "objective_items", "embedding", &[]),
        "inspection shows requalification pending after the target is raised"
    );
    assert_eq!(
        serving(&db, "objective_items", "embedding", &[]).map(|serving| serving.generation),
        Some(generation),
        "the old generation keeps serving until a requalified one publishes"
    );
    assert_auto_takes_exact(&db, 20, "whose stored summary found 95% for one sample");
    assert_auto_takes_the_index(
        &db,
        50,
        50,
        "whose stored summaries are exact on every sample",
    );

    drive_until_serving(&db, ROWS as usize, 1, 10);
    assert!(
        !requalification_pending(&db, "objective_items", "embedding", &[]),
        "nothing is pending once a requalified generation publishes"
    );
    let receipt = calibration_receipt(&db, "objective_items", "embedding", &[])
        .expect("the requalified generation has a calibration receipt");
    assert!(
        receipt.generation > generation,
        "a requalified generation publishes: {receipt:?}"
    );
    assert_auto_takes_the_index(&db, 20, 20, "requalified at 99 with exact answers");
    assert_auto_takes_the_index(&db, 50, 50, "requalified at 99 with exact answers");
}

#[test]
fn a_changed_expected_population_keeps_the_covered_settings_with_requalification_pending() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = qualified(root.path());
    let generation = serving(&db, "objective_items", "embedding", &[])
        .expect("a generation serves")
        .generation;
    db.execute(
        "ALTER TABLE objective_items ALTER COLUMN embedding SET SEARCH (EXPECTED_VECTORS = 200000)",
        &empty(),
    )
    .expect("declare a larger expected population");
    assert!(
        requalification_pending(&db, "objective_items", "embedding", &[]),
        "inspection shows requalification pending after the expected population changes"
    );
    assert_auto_takes_the_index(
        &db,
        20,
        20,
        "whose summaries still meet the unchanged target",
    );
    assert_auto_takes_the_index(
        &db,
        50,
        50,
        "whose summaries still meet the unchanged target",
    );

    drive_until_serving(&db, ROWS as usize, 1, 10);
    assert!(!requalification_pending(
        &db,
        "objective_items",
        "embedding",
        &[]
    ));
    assert!(
        serving(&db, "objective_items", "embedding", &[])
            .is_some_and(|serving| serving.generation > generation),
        "a requalified generation publishes"
    );
    assert_auto_takes_the_index(&db, 50, 50, "after requalification");
}
