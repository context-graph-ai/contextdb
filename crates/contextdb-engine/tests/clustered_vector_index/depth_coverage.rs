//! Coverage is decided per calibrated depth. Calibration qualifies a fixed
//! set of depths, each by its own production search with its own shortlist,
//! and a LIMIT k query is covered only when k is one of those depths and that
//! depth's own run met the target on every sample query. Here the depth-10
//! run of one sample query misses two exact neighbours while every other run
//! is exact: LIMIT 10 is uncovered even though the depth-20 and depth-50 runs,
//! whose first ten answers are exact, qualify; so is any LIMIT outside the
//! calibrated set. An implementation that infers a shallow depth from a
//! deeper run's prefix, instead of running it, fails. On a partition whose
//! receipt qualifies a depth below EXACT's work, AUTO takes the index there.

use super::support::{
    MAX_MAINTENANCE_CYCLES, ObjectiveStatus, SearchOutcome, SearchRoute, SearchWork,
    calibration_receipt, drive_until_serving, empty, int_ids, observe_search, open_caller_driven,
    params, plant_qualification, rewrite_next_calibration_answers, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const DIMENSIONS: usize = 16;
/// Large enough that the indexed route can do less work than EXACT.
const ROWS: i64 = 4_000;
const DEFAULT_DEPTHS: [u32; 7] = [1, 2, 5, 10, 20, 50, 100];

fn store(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE covered_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}))"
        ),
        &empty(),
    )
    .expect("declare a vector column with the derived search policy");
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO covered_items (id, embedding) VALUES ($id, $embedding)",
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
                "SELECT id FROM covered_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
            ),
            &params([("query", Value::Vector(unit_vector(99_999, DIMENSIONS)))]),
        )
        .expect("vector search")
    });
    (int_ids(&result), work)
}

fn assert_uncovered(db: &Database, limit: usize, why: &str) {
    let (auto_ids, auto) = search(db, "AUTO", limit);
    assert_eq!(
        auto.route,
        Some(SearchRoute::Exact),
        "AUTO takes EXACT for LIMIT {limit}, {why}: {auto:?}"
    );
    assert_eq!(
        auto_ids,
        search(db, "EXACT", limit).0,
        "AUTO's LIMIT {limit} is the exact answer"
    );
    let (indexed_ids, indexed) = search(db, "INDEXED", limit);
    assert_eq!(indexed_ids.len(), limit);
    assert_eq!(
        indexed.route,
        Some(SearchRoute::Indexed),
        "explicit INDEXED keeps its route"
    );
    assert_eq!(indexed.outcome, Some(SearchOutcome::Complete));
    assert_eq!(
        indexed.objective_status,
        Some(ObjectiveStatus::Unqualified),
        "explicit INDEXED serves LIMIT {limit} as unqualified, {why}: {indexed:?}"
    );
}

#[test]
fn only_calibrated_depths_are_covered_each_by_its_own_run() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("coverage.db"));
    store(&db);

    // (sample query, depth) -> length of the exact ranking calibration handed
    // to that depth's own run.
    let runs = Arc::new(Mutex::new(BTreeMap::<(usize, u32), usize>::new()));
    {
        let seen = Arc::clone(&runs);
        let _rewritten = rewrite_next_calibration_answers(
            &db,
            "covered_items",
            "embedding",
            &[],
            Box::new(move |query, depth, exact| {
                seen.lock().unwrap().insert((query, depth), exact.len());
                if query == 0 && depth == 10 {
                    exact[..8].to_vec()
                } else {
                    exact.to_vec()
                }
            }),
        );
        drive_until_serving(&db);
    }

    let receipt = calibration_receipt(&db, "covered_items", "embedding", &[])
        .expect("the serving generation has a calibration receipt");
    assert_eq!(
        receipt.generation,
        serving(&db, "covered_items", "embedding", &[])
            .unwrap()
            .generation
    );
    let depths = receipt
        .depths
        .iter()
        .map(|row| row.depth)
        .collect::<Vec<_>>();
    assert_eq!(
        depths,
        DEFAULT_DEPTHS.to_vec(),
        "inspection lists the calibrated depths"
    );

    let runs = runs.lock().unwrap().clone();
    let queries = runs
        .keys()
        .map(|(query, _)| *query)
        .collect::<BTreeSet<_>>();
    assert!(queries.contains(&0), "calibration ran sample query 0");
    for query in &queries {
        for depth in DEFAULT_DEPTHS {
            assert_eq!(
                runs.get(&(*query, depth)),
                Some(&(depth as usize)),
                "sample query {query} ran its own depth-{depth} search over the exact top {depth}"
            );
        }
    }
    assert_eq!(
        runs.len(),
        queries.len() * DEFAULT_DEPTHS.len(),
        "calibration runs exactly the calibrated depths and no others: {runs:?}"
    );
    for row in &receipt.depths {
        let own = ((f64::from(row.depth) * receipt.refine_factor).ceil() as u64)
            .max(u64::from(row.depth));
        assert_eq!(
            row.shortlist, own,
            "depth {} uses its own shortlist: {receipt:?}",
            row.depth
        );
        assert_eq!(row.sample_queries as usize, queries.len());
    }
    let at = |depth: u32| {
        *receipt
            .depths
            .iter()
            .find(|row| row.depth == depth)
            .unwrap()
    };
    assert_eq!(
        (at(10).queries_below, at(10).qualified),
        (1, false),
        "the depth-10 run of one sample missed two exact neighbours: {receipt:?}"
    );
    for depth in [20, 50] {
        assert_eq!(
            (at(depth).queries_below, at(depth).qualified),
            (0, true),
            "every depth-{depth} run is exact and the depth qualifies: {receipt:?}"
        );
    }

    assert_uncovered(
        &db,
        10,
        "because its own depth-10 run missed, though deeper runs are exact",
    );
    for limit in [11, 30] {
        assert_uncovered(&db, limit, "because it is not a calibrated depth");
    }
    for limit in [20, 50] {
        let (ids, indexed) = search(&db, "INDEXED", limit);
        assert_eq!(ids.len(), limit);
        assert_eq!(indexed.outcome, Some(SearchOutcome::Complete));
        assert_eq!(
            indexed.objective_status,
            Some(ObjectiveStatus::Met),
            "a qualified depth meets its objective: {indexed:?}"
        );
    }
}

#[test]
fn auto_takes_the_index_on_a_qualified_partition() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("qualified.db"));
    store(&db);
    drive_until_serving(&db);
    let generation = serving(&db, "covered_items", "embedding", &[])
        .expect("a generation serves")
        .generation;
    // A receipt stating depth 10 qualified at a fortieth of EXACT's work.
    plant_qualification(
        &db,
        "covered_items",
        "embedding",
        &[],
        &[10],
        100,
        u64::try_from(ROWS).unwrap(),
    );
    for _ in 0..MAX_MAINTENANCE_CYCLES.min(4) {
        assert_eq!(
            serving(&db, "covered_items", "embedding", &[])
                .unwrap()
                .generation,
            generation,
            "the planted receipt belongs to the serving generation"
        );
        let (ids, auto) = search(&db, "AUTO", 10);
        assert_eq!(ids.len(), 10);
        assert_eq!(
            auto.route,
            Some(SearchRoute::Indexed),
            "AUTO takes the index at a depth qualified below EXACT's work: {auto:?}"
        );
        assert_eq!(
            auto.objective_status,
            Some(ObjectiveStatus::Met),
            "{auto:?}"
        );
        assert_eq!(auto.served_generation, Some(generation));
        db.run_maintenance_cycle().expect("one maintenance cycle");
    }
    let (_, auto) = search(&db, "AUTO", 11);
    assert_eq!(
        auto.route,
        Some(SearchRoute::Exact),
        "a LIMIT outside the qualified depths still takes EXACT: {auto:?}"
    );
}

const AXES: usize = 16;
/// Above half of one 8-bit step over a [-1, 1] range: the true nearest's
/// quantized code moves one step away from the query's.
const NEAREST_OFFSET: f32 = 0.0045;
/// Below half a step: each decoy's quantized code equals the query's, yet at
/// full precision two such offsets put it farther than the true nearest.
const DECOY_OFFSET: f32 = 0.0035;
const DECOYS: usize = 99;
const BACKGROUND: i64 = 5_000;
const NEAREST_ID: i64 = 1_000_000;
const FIRST_DECOY_ID: i64 = 2_000_000;

fn axis_query() -> Vec<f32> {
    let mut query = vec![0.0; AXES];
    query[0] = 1.0;
    query
}

#[test]
fn a_depth_one_run_misses_what_a_depth_one_hundred_prefix_would_find() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("distinguishing.db"));
    db.execute(
        &format!("CREATE TABLE near_items (id INTEGER PRIMARY KEY, embedding VECTOR({AXES}))"),
        &empty(),
    )
    .expect("declare the searched column with the derived policy");
    db.execute(
        &format!("CREATE TABLE near_questions (id INTEGER PRIMARY KEY, embedding VECTOR({AXES}))"),
        &empty(),
    )
    .expect("declare the calibration questions");
    db.execute(
        "INSERT INTO near_questions (id, embedding) VALUES (1, $embedding)",
        &params([("embedding", Value::Vector(axis_query()))]),
    )
    .expect("the single calibration question is the axis query");
    super::support::declare_calibration_queries(
        &db,
        "near_items",
        "embedding",
        "near_questions",
        "embedding",
    );

    let mut nearest = axis_query();
    nearest[1] = NEAREST_OFFSET;
    let decoys = (1..AXES)
        .flat_map(|i| (i + 1..AXES).map(move |j| (i, j)))
        .take(DECOYS)
        .map(|(i, j)| {
            let mut decoy = axis_query();
            decoy[i] = DECOY_OFFSET;
            decoy[j] = DECOY_OFFSET;
            decoy
        })
        .collect::<Vec<_>>();
    assert_eq!(decoys.len(), DECOYS);
    let tx = db.begin_or_panic();
    let insert = |id: i64, vector: Vec<f32>| {
        db.execute_in_tx(
            tx,
            "INSERT INTO near_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("stage one row");
    };
    for id in 0..BACKGROUND {
        insert(id, unit_vector(id as u64, AXES));
    }
    insert(NEAREST_ID, nearest.clone());
    for (offset, decoy) in decoys.iter().enumerate() {
        insert(FIRST_DECOY_ID + offset as i64, decoy.clone());
    }
    db.commit(tx).expect("commit the fixture");
    drive_until_serving(&db);

    // The fixture's own premise, at full precision: the true nearest is the
    // single nearest row, and the decoys are the next 99.
    let exact = |limit: usize| {
        int_ids(
            &db.execute(
                &format!(
                    "SELECT id FROM near_items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT {limit}"
                ),
                &params([("query", Value::Vector(axis_query()))]),
            )
            .unwrap(),
        )
    };
    assert_eq!(
        exact(1),
        vec![NEAREST_ID],
        "the true nearest is nearest at full precision"
    );
    let top = exact(DECOYS + 1).into_iter().collect::<BTreeSet<_>>();
    assert!(
        top.contains(&NEAREST_ID)
            && (0..DECOYS as i64).all(|offset| top.contains(&(FIRST_DECOY_ID + offset))),
        "the exact top 100 is the true nearest and the 99 decoys"
    );

    let receipt = calibration_receipt(&db, "near_items", "embedding", &[])
        .expect("the serving generation has a calibration receipt");
    let at = |depth: u32| {
        *receipt
            .depths
            .iter()
            .find(|row| row.depth == depth)
            .unwrap_or_else(|| panic!("depth {depth} is calibrated: {receipt:?}"))
    };
    let (one, hundred) = (at(1), at(100));
    assert!(
        one.effective_candidates >= 1 && one.effective_candidates <= one.shortlist,
        "depth 1 persists the candidates its own run rescored: {one:?}"
    );
    assert!(
        hundred.effective_candidates >= 100,
        "depth 100 persists the candidates its own run rescored: {hundred:?}"
    );
    assert_eq!(
        (hundred.queries_below, hundred.qualified),
        (0, true),
        "the depth-100 run finds the whole exact top 100 and qualifies: {receipt:?}"
    );

    // The fixture separates the depths in the generation's own quantized
    // order: the true nearest is past depth 1's candidates, within depth 100's.
    let order = super::support::quantized_order(&db, "near_items", "embedding", &[], &axis_query());
    let rank = order
        .iter()
        .position(|key| *key == Value::Int64(NEAREST_ID))
        .expect("the true nearest has a quantized score") as u64;
    assert!(
        rank >= one.effective_candidates && rank < hundred.effective_candidates,
        "fixture: the true nearest is quantized-rank {rank}, outside depth 1's {} candidates and \
         inside depth 100's {}",
        one.effective_candidates,
        hundred.effective_candidates
    );

    assert_eq!(
        (one.queries_below, one.qualified),
        (1, false),
        "the depth-1 run misses the true nearest under its own shortlist, though the top of the \
         depth-100 answer is that row: {receipt:?}"
    );
    let (auto_ids, auto) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM near_items ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 1",
            &params([("query", Value::Vector(axis_query()))]),
        )
        .unwrap()
    });
    assert_eq!(
        auto.route,
        Some(SearchRoute::Exact),
        "depth 1 is uncovered: AUTO takes EXACT"
    );
    assert_eq!(int_ids(&auto_ids), vec![NEAREST_ID]);
    let (_, indexed) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM near_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([("query", Value::Vector(axis_query()))]),
        )
        .unwrap()
    });
    assert_eq!(
        indexed.objective_status,
        Some(ObjectiveStatus::Unqualified),
        "explicit INDEXED at the uncovered depth 1 is unqualified: {indexed:?}"
    );
}
