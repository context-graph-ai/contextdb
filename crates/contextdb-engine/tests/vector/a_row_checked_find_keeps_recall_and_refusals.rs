//! A filter too broad to list within its row-check allowance is checked on
//! the rows the walk reaches. That changes how the walk starts and when it
//! decides it is starved, so recall and refusals are measured against EXACT
//! across the shapes that move them: how much of the table the filter admits,
//! how the vectors cluster, rows sharing one vector with few of them admitted,
//! one partition or several, a resident graph or one reloaded from pages.
//! At the default breadth no cell refuses and every cell keeps recall@10 at
//! 0.95 or better; at a narrow declared breadth every answer keeps that
//! recall and any refusal is typed. Every find examines at most its walk
//! budget, half that again for its bounded probe, and the rows it outputs.

use contextdb_core::{Error, Value};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSION: usize = 16;
const ROWS_PER_PARTITION: usize = 1_500;
const PARTITIONS: usize = 3;
const LIMIT: usize = 10;
const PROBES: u64 = 6;
const NARROW: usize = 16;

fn context(partition: usize) -> Uuid {
    Uuid::from_u128(0xc000 + partition as u128)
}

fn noise(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(17);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as i32 - (1 << 23)) as f32 / (1 << 23) as f32
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    Uniform,
    Clustered,
    /// Groups of 30 rows sharing one vector, few of each group admitted.
    SharedSparse,
}

fn vector(shape: Shape, row: usize) -> Vec<f32> {
    match shape {
        Shape::Uniform => noise(row as u64 + 1),
        Shape::Clustered => {
            let centre = noise(1_000_000 + (row % 8) as u64);
            let jitter = noise(row as u64 + 1);
            centre
                .iter()
                .zip(jitter)
                .map(|(c, j)| c + 0.05 * j)
                .collect()
        }
        Shape::SharedSparse => noise(2_000_000 + (row / 30) as u64),
    }
}

/// Whether the filter admits `row`, for a filter admitting `percent` of rows.
fn admitted(shape: Shape, percent: usize, row: usize) -> bool {
    match shape {
        Shape::SharedSparse => row % 30 < (30 * percent).div_ceil(100).max(1),
        _ => (row * 37) % 100 < percent,
    }
}

fn fill(db: &Database, shape: Shape, percent: usize, breadth: Option<usize>) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let hnsw = breadth.map_or(String::new(), |breadth| {
        format!(" HNSW (M = 8, EF_SEARCH = {breadth})")
    });
    db.execute(
        &format!(
            "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
             is_searchable BOOLEAN NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (context_id) \
             SEARCH_MODE INDEXED AUTO_INDEX_AT 1{hnsw})"
        ),
        &HashMap::new(),
    )
    .expect("declare the passages");
    db.execute(
        "CREATE INDEX passages_searchable ON passages (context_id, is_searchable)",
        &HashMap::new(),
    )
    .expect("index the filter columns");
    for partition in 0..PARTITIONS {
        for row in 0..ROWS_PER_PARTITION {
            let id = partition * ROWS_PER_PARTITION + row;
            db.execute(
                "INSERT INTO passages VALUES ($id, $context, $searchable, $embedding)",
                &HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Uuid(Uuid::from_u128(id as u128 + 1)),
                    ),
                    ("context".to_owned(), Value::Uuid(context(partition))),
                    (
                        "searchable".to_owned(),
                        Value::Bool(admitted(shape, percent, row)),
                    ),
                    ("embedding".to_owned(), Value::Vector(vector(shape, id))),
                ]),
            )
            .expect("insert one passage");
        }
    }
    for _ in 0..64 {
        if db
            .run_maintenance_cycle()
            .expect("one maintenance cycle")
            .vector
            .remaining_partitions
            == 0
        {
            return;
        }
    }
    panic!("maintenance did not settle");
}

fn find(db: &Database, mode: &str, partitions: usize, query: &[f32]) -> Result<QueryResult, Error> {
    let scope = (0..partitions)
        .map(|partition| format!("'{}'", context(partition)))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(
        &format!(
            "SELECT id FROM passages WHERE context_id IN ({scope}) AND is_searchable = TRUE \
             ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {LIMIT}"
        ),
        &HashMap::from([("query".to_owned(), Value::Vector(query.to_vec()))]),
    )
}

fn ids(result: &QueryResult) -> Vec<Value> {
    result.rows.iter().map(|row| row[0].clone()).collect()
}

/// One cell: (answered, refused, mean recall over answers).
fn cell(db: &Database, shape: Shape, partitions: usize, label: &str) -> (usize, usize, f64) {
    let (mut answered, mut refused, mut recall) = (0usize, 0usize, 0f64);
    for probe in 0..PROBES {
        let query = vector(shape, 7_777 + probe as usize * 131);
        let exact = find(db, "EXACT", partitions, &query).expect("EXACT answers");
        match find(db, "INDEXED", partitions, &query) {
            Ok(indexed) => {
                answered += 1;
                let expected = ids(&exact);
                let hits = ids(&indexed)
                    .iter()
                    .filter(|id| expected.contains(id))
                    .count();
                recall += hits as f64 / expected.len().max(1) as f64;
                let search = indexed
                    .trace
                    .vector_search
                    .as_ref()
                    .expect("the INDEXED find discloses its route");
                if let Some(budget) = search.global_candidate_budget {
                    assert!(
                        indexed.trace.rows_examined <= (budget + budget / 2 + LIMIT) as u64,
                        "{label} probe {probe}: examined {} rows, past {budget} + {budget}/2 + {LIMIT}",
                        indexed.trace.rows_examined
                    );
                }
            }
            Err(Error::VectorIndexedRouteUnavailable { .. }) => refused += 1,
            Err(error) => panic!("{label} probe {probe}: refused untyped: {error:?}"),
        }
    }
    (answered, refused, recall / answered.max(1) as f64)
}

#[test]
fn a_row_checked_find_keeps_recall_and_refusals() {
    let mut failures = Vec::new();
    for shape in [Shape::Uniform, Shape::Clustered, Shape::SharedSparse] {
        for percent in [1, 10, 50, 90, 100] {
            for (breadth_label, breadth) in [("default", None), ("narrow", Some(NARROW))] {
                for reloaded in [false, true] {
                    let root = tempfile::tempdir().expect("fixture directory");
                    let path = root.path().join("passages.db");
                    let db = if reloaded {
                        {
                            let db = Database::open(&path).expect("open the durable fixture");
                            fill(&db, shape, percent, breadth);
                            db.close().expect("close");
                        }
                        Database::open(&path).expect("reopen the saved graphs")
                    } else {
                        let db = Database::open_memory();
                        fill(&db, shape, percent, breadth);
                        db
                    };
                    for partitions in [1, 2, PARTITIONS] {
                        let label = format!(
                            "{shape:?} {percent}% {breadth_label} {} {partitions}p",
                            if reloaded { "reloaded" } else { "resident" }
                        );
                        let (answered, refused, recall) = cell(&db, shape, partitions, &label);
                        println!(
                            "CELL {label}: answered {answered} refused {refused} recall {recall:.3}"
                        );
                        if breadth.is_none() && refused > 0 {
                            failures
                                .push(format!("{label}: {refused} refusals at default breadth"));
                        }
                        if answered > 0 && recall < 0.95 {
                            failures.push(format!("{label}: recall {recall:.3} < 0.95"));
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "cells below the bar:\n{}",
        failures.join("\n")
    );
}

/// The selected partitions each retain their declared allowance. This focused
/// row proves the default three-partition cells that the broad matrix reports
/// only as an aggregate: every probe must answer, and its actual top ten must
/// overlap the exact top ten by at least 95 percent.
fn assert_default_three_partition_cell(shape: Shape, percent: usize, reloaded: bool) {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("passages.db");
    let db = if reloaded {
        {
            let db = Database::open(&path).expect("open the durable fixture");
            fill(&db, shape, percent, None);
            db.close().expect("close");
        }
        Database::open(&path).expect("reopen the saved graphs")
    } else {
        let db = Database::open_memory();
        fill(&db, shape, percent, None);
        db
    };

    let label = format!(
        "{shape:?} {percent}% default {} {PARTITIONS}p",
        if reloaded { "reloaded" } else { "resident" }
    );
    for probe in 0..PROBES {
        let query = vector(shape, 7_777 + probe as usize * 131);
        let exact = find(&db, "EXACT", PARTITIONS, &query).expect("EXACT answers");
        let indexed = find(&db, "INDEXED", PARTITIONS, &query)
            .unwrap_or_else(|error| panic!("{label} probe {probe}: indexed refusal: {error:?}"));
        let expected = ids(&exact);
        let actual = ids(&indexed);
        assert_eq!(
            expected.len(),
            LIMIT,
            "{label} probe {probe}: exact top-{LIMIT} fixture answer"
        );
        assert_eq!(
            actual.len(),
            LIMIT,
            "{label} probe {probe}: indexed top-{LIMIT} answer"
        );
        let overlap = actual.iter().filter(|id| expected.contains(id)).count();
        assert!(
            overlap * 100 >= LIMIT * 95,
            "{label} probe {probe}: exact overlap@{LIMIT} {overlap}/{LIMIT} is below 0.95"
        );
    }
}

#[test]
fn default_three_partition_row_checked_cells_answer_every_exact_probe() {
    for (shape, percent, reloaded) in [
        (Shape::Uniform, 50, false),
        (Shape::Uniform, 50, true),
        (Shape::Uniform, 90, false),
        (Shape::Uniform, 90, true),
        (Shape::SharedSparse, 50, false),
        (Shape::SharedSparse, 50, true),
    ] {
        assert_default_three_partition_cell(shape, percent, reloaded);
    }
}

/// Walk budget the column declares for one top-`k` search:
/// `max(EF_SEARCH, k, 1) * (2 * M + 4)`.
const fn declared_row_check_allowance(ef_search: usize, k: usize, m: usize) -> usize {
    let mut cap = ef_search;
    if k > cap {
        cap = k;
    }
    if cap < 1 {
        cap = 1;
    }
    cap * (m * 2 + 4)
}

const EXHAUSTION_M: usize = 8;
const EXHAUSTION_EF: usize = 1;
const EXHAUSTION_LIMIT: usize = 1;
const EXHAUSTION_ALLOWANCE: usize =
    declared_row_check_allowance(EXHAUSTION_EF, EXHAUSTION_LIMIT, EXHAUSTION_M);
const EXHAUSTION_NONMATCHES: usize = EXHAUSTION_ALLOWANCE;
const EXHAUSTION_MATCHES: usize = EXHAUSTION_ALLOWANCE / 2 + 1;
const EXHAUSTION_FLAT_BELOW: usize = EXHAUSTION_NONMATCHES + EXHAUSTION_MATCHES + 1;

fn declare_flat_passages(db: &Database, flat_below: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
             is_searchable BOOLEAN NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (context_id) \
             SEARCH_MODE INDEXED AUTO_INDEX_AT 1 FLAT_BELOW {flat_below} \
             HNSW (M = {EXHAUSTION_M}, EF_SEARCH = {EXHAUSTION_EF}))"
        ),
        &HashMap::new(),
    )
    .expect("declare the passages");
    db.execute(
        "CREATE INDEX passages_searchable ON passages (context_id, is_searchable)",
        &HashMap::new(),
    )
    .expect("index the filter columns");
}

fn settle_passages(db: &Database) {
    for _ in 0..64 {
        if db
            .run_maintenance_cycle()
            .expect("one maintenance cycle")
            .vector
            .remaining_partitions
            == 0
        {
            return;
        }
    }
    panic!("maintenance did not settle");
}

fn insert_passage(db: &Database, context: Uuid, row: usize, searchable: bool, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO passages VALUES ($id, $context, $searchable, $embedding)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(row as u128 + 1))),
            ("context".to_owned(), Value::Uuid(context)),
            ("searchable".to_owned(), Value::Bool(searchable)),
            ("embedding".to_owned(), Value::Vector(embedding)),
        ]),
    )
    .expect("insert one passage");
}

fn search_one_context(
    db: &Database,
    mode: &str,
    context: Uuid,
    query: &[f32],
    limit: usize,
) -> Result<QueryResult, Error> {
    db.execute(
        &format!(
            "SELECT id FROM passages WHERE context_id = '{context}' AND is_searchable = TRUE \
             ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
        ),
        &HashMap::from([("query".to_owned(), Value::Vector(query.to_vec()))]),
    )
}

/// One executed partition whose route is exact. EXACT mode records
/// `exact_requested` before any flat arm, so that receipt is not proof the
/// partition was served flat. AUTO records `partition_below_flat_threshold`
/// from the flat arm, and that receipt is the flat proof.
fn assert_one_exact_partition(result: &QueryResult, label: &str, reason: &str) {
    let search = result
        .trace
        .vector_search
        .as_ref()
        .unwrap_or_else(|| panic!("{label}: the find discloses its route"));
    let routes: Vec<_> = search
        .partition_hnsw
        .iter()
        .map(|partition| {
            (
                partition
                    .route
                    .unwrap_or_else(|| panic!("{label}: an executed partition has a route")),
                partition.reason.clone(),
            )
        })
        .collect();
    assert_eq!(
        routes,
        vec![(
            contextdb_engine::VectorSearchRoute::Exact,
            Some(reason.to_owned()),
        )],
        "{label}: one exact partition with reason {reason}"
    );
}

/// The recall matrix always keeps hits, and the six-cell probe requires a
/// full top ten. Neither watches an INDEXED flat merge that spends its
/// row-check allowance on non-matches and then publishes no row. The
/// candidate index is `(context_id, is_searchable)`. Eleven matches are
/// more than half the declared allowance, so that drain caps. The matches
/// are inserted only after twenty non-matches, which is the allowance, so
/// this names the empty merge only when the flat scan visits entries in
/// insertion order. EXACT's receipt is `exact_requested`. AUTO's receipt
/// `statement_exhaustive_cheaper_than_walk` records its actual cost decision.
#[test]
fn indexed_flat_row_check_exhaustion_before_later_matches_is_refused() {
    assert!(EXHAUSTION_ALLOWANCE >= 1);
    assert!(EXHAUSTION_NONMATCHES + EXHAUSTION_MATCHES > EXHAUSTION_ALLOWANCE);
    assert!(EXHAUSTION_MATCHES > EXHAUSTION_ALLOWANCE / 2);
    assert!(EXHAUSTION_FLAT_BELOW > EXHAUSTION_NONMATCHES + EXHAUSTION_MATCHES);

    let context = Uuid::from_u128(0xc100);
    let db = Database::open_memory();
    declare_flat_passages(&db, EXHAUSTION_FLAT_BELOW);
    let mut query = noise(9_001);
    query[0] = 1.0;
    for row in 0..EXHAUSTION_NONMATCHES {
        insert_passage(&db, context, row, false, noise(row as u64 + 1));
    }
    let best = EXHAUSTION_NONMATCHES;
    for row in EXHAUSTION_NONMATCHES..EXHAUSTION_NONMATCHES + EXHAUSTION_MATCHES {
        let embedding = if row == best {
            query.clone()
        } else {
            noise(50_000 + row as u64)
        };
        insert_passage(&db, context, row, true, embedding);
    }
    settle_passages(&db);
    let best_id = Value::Uuid(Uuid::from_u128(best as u128 + 1));

    let exact = search_one_context(&db, "EXACT", context, &query, EXHAUSTION_LIMIT)
        .expect("EXACT returns the later match");
    let auto = search_one_context(&db, "AUTO", context, &query, EXHAUSTION_LIMIT)
        .expect("AUTO returns the later match");
    assert_eq!(ids(&exact), vec![best_id.clone()]);
    assert_eq!(ids(&auto), ids(&exact), "AUTO and EXACT name the same later match");
    assert_one_exact_partition(&exact, "EXACT", "exact_requested");
    // The statement-wide cost decision precedes the per-partition flat arm.
    // This healthy, small resident scope is cheaper to compare exhaustively.
    assert_one_exact_partition(&auto, "AUTO", "statement_exhaustive_cheaper_than_walk");
    assert_eq!(auto.trace.vector_search.as_ref().unwrap().partition_hnsw.len(), 1);

    match search_one_context(&db, "INDEXED", context, &query, EXHAUSTION_LIMIT) {
        Ok(indexed) => panic!(
            "INDEXED published {} rows after the row-check allowance; an empty answer is an incomplete check",
            indexed.rows.len()
        ),
        Err(Error::VectorIndexedRouteUnavailable { .. }) => {}
        Err(error) => panic!("INDEXED refused untyped: {error:?}"),
    }
}

/// A covered predicate that names no row drains its index range to the end.
/// INDEXED, EXACT, and AUTO all finish with no row. This is not a ledger
/// that stopped early.
#[test]
fn completed_covered_filter_with_no_matches_returns_empty() {
    let context = Uuid::from_u128(0xc101);
    let db = Database::open_memory();
    declare_flat_passages(&db, 8);
    let query = noise(9_002);
    for row in 0..3 {
        insert_passage(&db, context, row, false, noise(row as u64 + 10));
    }
    settle_passages(&db);
    for mode in ["INDEXED", "EXACT", "AUTO"] {
        let result = search_one_context(&db, mode, context, &query, EXHAUSTION_LIMIT)
            .unwrap_or_else(|error| panic!("{mode} refused a finished empty filter: {error:?}"));
        assert!(
            result.rows.is_empty(),
            "{mode} published rows for a filter that names none"
        );
    }
}
