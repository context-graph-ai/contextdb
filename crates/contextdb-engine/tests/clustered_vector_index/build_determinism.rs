//! Rebuilding a clustered generation from the same rows under one declared
//! TRAINING_SEED gives the same index: the same codec steps, the same build
//! work and the same answers, including under the seed zero. A build's work
//! is deterministic for a fixed population, and bounded by its own declared
//! numbers: training never does more distance work than its iterations times
//! its reservoir times its lists, and assignment never more than its rows
//! times its lists.

use super::support::{
    BuildWork, build_work, declare_training_seed, drive_until_serving, empty, int_ids,
    observe_search, open_caller_driven, params, quantizer_steps, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::path::Path;

const DIMENSIONS: usize = 16;
const QUERIES: u64 = 20;

fn built(path: &Path, rows: i64, seed: u64) -> Database {
    let db = open_caller_driven(path);
    db.execute(
        &format!(
            "CREATE TABLE seeded_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}))"
        ),
        &empty(),
    )
    .expect("declare the table");
    declare_training_seed(&db, "seeded_items", "embedding", seed);
    let tx = db.begin_or_panic();
    for id in 0..rows {
        db.execute_in_tx(
            tx,
            "INSERT INTO seeded_items (id, embedding) VALUES ($id, $embedding)",
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
    drive_until_serving(&db);
    db
}

fn answers(db: &Database) -> Vec<Vec<i64>> {
    (0..QUERIES)
        .map(|probe| {
            let (result, work) = observe_search(db, || {
                db.execute(
                    "SELECT id FROM seeded_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 50",
                    &params([("query", Value::Vector(unit_vector(800_000 + probe, DIMENSIONS)))]),
                )
                .expect("indexed search")
            });
            assert!(work.clustered_route, "the search ran on the clustered index");
            int_ids(&result)
        })
        .collect()
}

fn work(db: &Database) -> BuildWork {
    build_work(db, "seeded_items", "embedding", &[])
}

#[test]
fn rebuilding_from_the_same_rows_under_one_training_seed_gives_the_same_index_and_answers() {
    let root = tempfile::tempdir().expect("temporary store directory");
    for seed in [0_u64, 7] {
        let first = built(&root.path().join(format!("first-{seed}.db")), 1_500, seed);
        let second = built(&root.path().join(format!("second-{seed}.db")), 1_500, seed);
        assert_eq!(
            serving(&first, "seeded_items", "embedding", &[]).map(|serving| serving.lists),
            serving(&second, "seeded_items", "embedding", &[]).map(|serving| serving.lists),
            "seed {seed}: the same list count"
        );
        assert_eq!(
            quantizer_steps(&first, "seeded_items", "embedding", &[]),
            quantizer_steps(&second, "seeded_items", "embedding", &[]),
            "seed {seed}: the same codec"
        );
        assert_eq!(
            work(&first),
            work(&second),
            "seed {seed}: the same build work"
        );
        assert_eq!(
            answers(&first),
            answers(&second),
            "seed {seed}: the same answers to every query"
        );
    }
}

#[test]
fn a_build_does_deterministic_work_bounded_by_its_declared_numbers() {
    let root = tempfile::tempdir().expect("temporary store directory");
    for rows in [2_000_i64, 4_000] {
        let first = built(&root.path().join(format!("work-{rows}-a.db")), rows, 11);
        let second = built(&root.path().join(format!("work-{rows}-b.db")), rows, 11);
        let measured = work(&first);
        assert_eq!(
            measured,
            work(&second),
            "{rows} rows: build work is deterministic"
        );
        assert_eq!(
            measured.assigned_rows, rows as u64,
            "{rows} rows: every row is assigned"
        );
        assert!(
            measured.lists >= 1 && measured.training_iterations >= 1,
            "{rows} rows: {measured:?}"
        );
        assert!(
            measured.training_samples >= 1 && measured.training_samples <= rows as u64,
            "{rows} rows: the reservoir is drawn from the population: {measured:?}"
        );
        assert!(
            measured.training_distance_evaluations > 0
                && measured.training_distance_evaluations
                    <= measured.training_iterations * measured.training_samples * measured.lists,
            "{rows} rows: training does no more distance work than iterations x reservoir x lists: \
             {measured:?}"
        );
        assert!(
            measured.assignment_distance_evaluations > 0
                && measured.assignment_distance_evaluations
                    <= measured.assigned_rows * measured.lists,
            "{rows} rows: assignment does no more distance work than rows x lists: {measured:?}"
        );
    }
}
