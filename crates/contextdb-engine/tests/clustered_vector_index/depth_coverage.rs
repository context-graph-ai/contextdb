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
const DECOYS: usize = 99;
const BACKGROUND: i64 = 5_000;
const NEAREST_ID: i64 = 1_000_000;
const FIRST_DECOY_ID: i64 = 2_000_000;
/// Every evaluated depth runs at this pinned point: two probed lists, so the
/// indexed route scores a small fraction of the 5,100 vectors EXACT compares;
/// depth 1 rescores ceil(1 x 2) = 2 candidates and depth 100 rescores 200.
const PINNED_PROBES: u64 = 2;
const PINNED_FACTOR: f64 = 2.0;
/// Offsets as fractions of a dimension's quantization step. A decoy's offset
/// stays a margin below half a step, so it can keep the query's code; the true
/// nearest's offset is a margin above half a step, so it moves one code.
const DECOY_FRACTION: f32 = 0.35;
const NEAREST_FRACTION: f32 = 0.8;
/// Axes each decoy is offset on: enough that, at full precision, every decoy
/// is farther from the query than the true nearest.
const DECOY_AXES: usize = 8;

fn axis_query() -> Vec<f32> {
    let mut query = vec![0.0; AXES];
    query[0] = 1.0;
    query
}

fn near_store(path: &std::path::Path, rows: &[(i64, Vec<f32>)]) -> Database {
    let db = open_caller_driven(path);
    for table in ["near_items", "near_questions"] {
        db.execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR({AXES}))"),
            &empty(),
        )
        .expect("declare the table");
    }
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
    let tx = db.begin_or_panic();
    for id in 0..BACKGROUND {
        db.execute_in_tx(
            tx,
            "INSERT INTO near_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(unit_vector(id as u64, AXES))),
            ]),
        )
        .expect("stage one background row");
    }
    for (id, vector) in rows {
        db.execute_in_tx(
            tx,
            "INSERT INTO near_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(*id)),
                ("embedding", Value::Vector(vector.clone())),
            ]),
        )
        .expect("stage one fixture row");
    }
    db.commit(tx).expect("commit the fixture");
    {
        let _pinned = super::support::plant_calibration_setting(
            &db,
            "near_items",
            "embedding",
            &[],
            PINNED_PROBES,
            PINNED_FACTOR,
        );
        drive_until_serving(&db);
    }
    db
}

fn codes(db: &Database, vector: &[f32]) -> Vec<i32> {
    super::support::quantized_codes(db, "near_items", "embedding", &[], vector)
}

#[test]
fn a_depth_one_run_misses_what_a_depth_one_hundred_prefix_would_find() {
    let root = tempfile::tempdir().expect("temporary store directory");

    // Probe store: the same background and the query's own direction, to read
    // the production codec's steps over the fixture's real value range.
    let probe = near_store(&root.path().join("probe.db"), &[(NEAREST_ID, axis_query())]);
    let steps = super::support::quantizer_steps(&probe, "near_items", "embedding", &[]);
    assert_eq!(steps.len(), AXES, "the codec reports a step per dimension");
    assert!(
        steps.iter().all(|step| *step > 0.0),
        "every step is positive: {steps:?}"
    );
    let query_codes = codes(&probe, &axis_query());
    // For each off-query axis, the signed offset that keeps the query's code
    // (decoys) and the one that moves it (the true nearest), checked against
    // the codec itself rather than assumed from the step alone.
    let keeps = |axis: usize| {
        [1.0_f32, -1.0].into_iter().find_map(|sign| {
            let mut vector = axis_query();
            vector[axis] = sign * DECOY_FRACTION * steps[axis];
            (codes(&probe, &vector) == query_codes).then_some(vector[axis])
        })
    };
    let moves = |axis: usize| {
        [1.0_f32, -1.0].into_iter().find_map(|sign| {
            let mut vector = axis_query();
            vector[axis] = sign * NEAREST_FRACTION * steps[axis];
            (codes(&probe, &vector) != query_codes).then_some(vector[axis])
        })
    };
    let nearest_axis = (1..AXES)
        .find(|axis| moves(*axis).is_some())
        .expect("some axis moves the query's code one step at the nearest's offset");
    let decoy_offsets = (1..AXES)
        .filter(|axis| *axis != nearest_axis)
        .filter_map(|axis| keeps(axis).map(|offset| (axis, offset)))
        .collect::<Vec<_>>();
    assert!(
        decoy_offsets.len() >= DECOY_AXES + 3,
        "enough axes keep the query's code at the decoy offset for 99 distinct decoys: \
         {decoy_offsets:?}"
    );
    let nearest_offset =
        moves(nearest_axis).expect("the verified signed offset of the true nearest");
    probe.close().expect("close the probe store");

    let mut nearest = axis_query();
    nearest[nearest_axis] = nearest_offset;
    // Distinct decoys: the first 99 subsets of DECOY_AXES code-keeping axes.
    let mut decoys = Vec::with_capacity(DECOYS);
    let mut chosen = (0..DECOY_AXES).collect::<Vec<_>>();
    while decoys.len() < DECOYS {
        let mut decoy = axis_query();
        for index in &chosen {
            let (axis, offset) = decoy_offsets[*index];
            decoy[axis] = offset;
        }
        decoys.push(decoy);
        // Next combination in lexicographic order.
        let Some(position) = (0..DECOY_AXES)
            .rev()
            .find(|position| chosen[*position] < decoy_offsets.len() - DECOY_AXES + position)
        else {
            break;
        };
        chosen[position] += 1;
        for later in position + 1..DECOY_AXES {
            chosen[later] = chosen[later - 1] + 1;
        }
    }
    assert_eq!(decoys.len(), DECOYS, "enough distinct decoys");
    let mut rows = vec![(NEAREST_ID, nearest.clone())];
    rows.extend(
        decoys
            .iter()
            .enumerate()
            .map(|(i, decoy)| (FIRST_DECOY_ID + i as i64, decoy.clone())),
    );
    let db = near_store(&root.path().join("distinguishing.db"), &rows);

    // Preconditions against the store that is judged.
    assert_eq!(
        super::support::quantizer_steps(&db, "near_items", "embedding", &[]),
        steps,
        "the judged store's codec has the probe store's steps"
    );
    let query_codes = codes(&db, &axis_query());
    assert_ne!(
        codes(&db, &nearest),
        query_codes,
        "the true nearest's code moved one step"
    );
    for decoy in &decoys {
        assert_eq!(
            codes(&db, decoy),
            query_codes,
            "every decoy keeps the query's code"
        );
    }
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
        "at full precision the true nearest is nearest"
    );
    let top = exact(DECOYS + 1).into_iter().collect::<BTreeSet<_>>();
    assert!(
        top.contains(&NEAREST_ID)
            && (0..DECOYS as i64).all(|offset| top.contains(&(FIRST_DECOY_ID + offset))),
        "the exact top 100 is the true nearest and the 99 decoys"
    );
    let receipt = calibration_receipt(&db, "near_items", "embedding", &[])
        .expect("the serving generation has a calibration receipt");
    assert_eq!(
        receipt.refine_factor, PINNED_FACTOR,
        "the pinned factor serves: {receipt:?}"
    );
    let at = |depth: u32| {
        *receipt
            .depths
            .iter()
            .find(|row| row.depth == depth)
            .unwrap_or_else(|| panic!("depth {depth} is calibrated: {receipt:?}"))
    };
    let (one, hundred) = (at(1), at(100));
    assert_eq!(
        one.shortlist, 2,
        "depth 1 rescores its own ceil(1 x 2) candidates"
    );
    assert!(
        (1..=2).contains(&one.effective_candidates),
        "depth 1 persists the candidates it rescored: {one:?}"
    );
    assert!(hundred.effective_candidates >= 100, "{hundred:?}");
    // The true nearest and every decoy sit in lists the pinned two-probe
    // search reads.
    let probed = super::support::nearest_lists(
        &db,
        "near_items",
        "embedding",
        &[],
        &axis_query(),
        PINNED_PROBES,
    );
    assert_eq!(
        probed.len() as u64,
        PINNED_PROBES,
        "the pinned search probes two lists"
    );
    for key in
        std::iter::once(NEAREST_ID).chain((0..DECOYS as i64).map(|offset| FIRST_DECOY_ID + offset))
    {
        let list =
            super::support::assigned_list(&db, "near_items", "embedding", &[], &Value::Int64(key));
        assert!(
            probed.contains(&list),
            "precondition: row {key} is in list {list}, among the probed lists {probed:?}"
        );
    }
    let order = super::support::quantized_order(&db, "near_items", "embedding", &[], &axis_query());
    let rank = order
        .iter()
        .position(|key| *key == Value::Int64(NEAREST_ID))
        .expect("the true nearest has a quantized score") as u64;
    assert!(
        rank >= one.effective_candidates && rank < hundred.effective_candidates,
        "precondition: the true nearest is quantized-rank {rank}, outside depth 1's candidates \
         and inside depth 100's ({one:?}, {hundred:?})"
    );

    // Both outcomes, observed directly at the pinned point. The depth-1 run
    // with its own shortlist misses the true nearest, while the first row of
    // the depth-100 answer is the true nearest: prefix inference would call
    // depth 1 covered.
    let indexed = |limit: usize| {
        let (result, work) = observe_search(&db, || {
            db.execute(
                &format!(
                    "SELECT id FROM near_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT {limit}"
                ),
                &params([("query", Value::Vector(axis_query()))]),
            )
            .unwrap()
        });
        (int_ids(&result), work)
    };
    let (depth_one, depth_one_work) = indexed(1);
    assert_ne!(
        depth_one,
        vec![NEAREST_ID],
        "the depth-1 run misses under its own shortlist"
    );
    let (depth_hundred, depth_hundred_work) = indexed(100);
    let (_, exact_hundred_work) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM near_items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 100",
            &params([("query", Value::Vector(axis_query()))]),
        )
        .unwrap()
    });
    assert!(
        depth_hundred_work.distance_evaluations() < exact_hundred_work.distance_evaluations(),
        "precondition: at the pinned point the depth-100 search does less work than EXACT: \
         indexed {depth_hundred_work:?}, exact {exact_hundred_work:?}"
    );
    assert_eq!(
        depth_hundred.first(),
        Some(&NEAREST_ID),
        "the depth-100 answer's first row is the true nearest, which a prefix reading would credit to depth 1"
    );

    // The per-depth verdict: depth 1 uncovered, depth 100 qualified.
    assert_eq!(
        (hundred.queries_below, hundred.qualified),
        (0, true),
        "{receipt:?}"
    );
    assert_eq!(
        (one.queries_below, one.qualified),
        (1, false),
        "the per-depth calibration reports depth 1 uncovered, not the prefix's covered: {receipt:?}"
    );
    assert_eq!(
        depth_one_work.objective_status,
        Some(ObjectiveStatus::Unqualified),
        "explicit INDEXED at the uncovered depth 1 is unqualified: {depth_one_work:?}"
    );
    let (auto_result, auto) = observe_search(&db, || {
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
    assert_eq!(int_ids(&auto_result), vec![NEAREST_ID]);
}
