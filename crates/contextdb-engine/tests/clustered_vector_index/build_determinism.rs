//! Rebuilding a clustered generation from the same rows under one declared
//! TRAINING_SEED gives the same index: the same codebook, codec steps, build
//! work and answers, including under the seed zero; and the declared seed is
//! honoured, since another seed over the same rows trains a different
//! codebook. A build's work is deterministic for a fixed population, and
//! bounded by the training policy inspection reports, not by numbers the
//! build reports about itself: training never does more distance work than
//! TRAINING_ITERATIONS x min(N, TRAINING_SAMPLES) x lists, and assignment
//! never more than N x lists.

use super::support::{
    BuildWork, build_work, declare_training_seed, drive_until_serving, empty, inspected_training,
    int_ids, observe_search, open_caller_driven, params, quantizer_steps, serving, unit_vector,
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
    drive_until_serving(&db, rows as usize, 1, 10);
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
#[ignore = "index deferred to the follow-on plan"]
fn rebuilding_from_the_same_rows_under_one_training_seed_gives_the_same_index_and_answers() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let mut codebooks = Vec::new();
    for seed in [0_u64, 7] {
        let first = built(&root.path().join(format!("first-{seed}.db")), 1_500, seed);
        let second = built(&root.path().join(format!("second-{seed}.db")), 1_500, seed);
        let (first_serving, second_serving) = (
            serving(&first, "seeded_items", "embedding", &[]).expect("the first build serves"),
            serving(&second, "seeded_items", "embedding", &[]).expect("the second build serves"),
        );
        assert_eq!(
            first_serving.lists, second_serving.lists,
            "seed {seed}: the same list count"
        );
        assert_eq!(
            first_serving.codebook, second_serving.codebook,
            "seed {seed}: the same trained codebook"
        );
        codebooks.push(first_serving.codebook);
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
    assert_ne!(
        codebooks[0], codebooks[1],
        "the declared seed is honoured: seeds 0 and 7 over the same rows train different codebooks"
    );
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
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
        // The bounds come from the policy inspection reports and the serving
        // generation's list count, never from the numbers the build reports.
        let policy = inspected_training(&first, "seeded_items", "embedding", &[]);
        let lists = serving(&first, "seeded_items", "embedding", &[])
            .expect("the build serves")
            .lists;
        let samples = (rows as u64).min(policy.sample_cap);
        assert!(
            policy.iterations >= 1 && samples >= 1 && lists >= 1,
            "{rows} rows: {policy:?}"
        );
        assert_eq!(
            (
                measured.training_iterations,
                measured.training_samples,
                measured.lists
            ),
            (policy.iterations, samples, lists),
            "{rows} rows: the build ran the inspected policy: {measured:?}, {policy:?}"
        );
        assert!(
            measured.training_distance_evaluations > 0
                && measured.training_distance_evaluations <= policy.iterations * samples * lists,
            "{rows} rows: training does no more distance work than TRAINING_ITERATIONS x \
             min(N, TRAINING_SAMPLES) x lists: {measured:?}, {policy:?}"
        );
        assert!(
            measured.assignment_distance_evaluations > 0
                && measured.assignment_distance_evaluations <= rows as u64 * lists,
            "{rows} rows: assignment does no more distance work than N x lists: {measured:?}"
        );
    }
}
