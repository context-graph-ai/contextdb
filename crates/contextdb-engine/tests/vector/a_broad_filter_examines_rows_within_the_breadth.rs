//! An INDEXED search over a built graph does work its declared breadth
//! bounds, however many rows its filter allows. A filter on columns beside
//! the vector -- the shape a context's current, searchable passages take --
//! is checked on the rows the walk reaches, not answered by listing every
//! allowed row before the walk: the rows the statement examines stay within
//! its walk budget, half that again for a bounded probe, and the rows it
//! outputs, whether the filter allows every row or most of them, with
//! recall@10 against EXACT of at least 0.95.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSION: usize = 16;
const ROWS: usize = 3_000;
const M: usize = 8;
const BREADTH: usize = 32;
const LIMIT: usize = 10;

fn context() -> Uuid {
    Uuid::from_u128(0xc0)
}
fn generation() -> Uuid {
    Uuid::from_u128(0x6e)
}

fn vector(seed: u64) -> Vec<f32> {
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

/// `searchable(row)` decides which rows the filter allows.
fn fixture(searchable: impl Fn(usize) -> bool) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
             generation_id UUID NOT NULL, is_current BOOLEAN NOT NULL, \
             is_searchable BOOLEAN NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (context_id, generation_id) \
             SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = {M}, EF_SEARCH = {BREADTH}))"
        ),
        &HashMap::new(),
    )
    .expect("declare the passages");
    db.execute(
        "CREATE INDEX passages_active ON passages \
         (context_id, generation_id, is_current, is_searchable)",
        &HashMap::new(),
    )
    .expect("index the filter columns");
    for row in 0..ROWS {
        db.execute(
            "INSERT INTO passages VALUES ($id, $context, $generation, TRUE, $searchable, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::from_u128(row as u128 + 1))),
                ("context".to_owned(), Value::Uuid(context())),
                ("generation".to_owned(), Value::Uuid(generation())),
                ("searchable".to_owned(), Value::Bool(searchable(row))),
                ("embedding".to_owned(), Value::Vector(vector(row as u64 + 1))),
            ]),
        )
        .expect("insert one passage");
    }
    for _ in 0..64 {
        if db
            .run_maintenance_cycle()
            .expect("one maintenance cycle")
            .vector
            .remaining_partitions
            == 0
        {
            return db;
        }
    }
    panic!("maintenance did not settle");
}

fn find(db: &Database, mode: &str, probe: u64) -> QueryResult {
    db.execute(
        &format!(
            "SELECT id FROM passages WHERE context_id = $context AND generation_id = $generation \
             AND is_current = TRUE AND is_searchable = TRUE \
             ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {LIMIT}"
        ),
        &HashMap::from([
            ("context".to_owned(), Value::Uuid(context())),
            ("generation".to_owned(), Value::Uuid(generation())),
            ("query".to_owned(), Value::Vector(vector(1_000_000 + probe))),
        ]),
    )
    .unwrap_or_else(|error| panic!("the {mode} find answers: {error:?}"))
}

fn ids(result: &QueryResult) -> Vec<Value> {
    result.rows.iter().map(|row| row[0].clone()).collect()
}

#[test]
fn a_broad_filter_examines_rows_within_the_breadth() {
    for (label, searchable) in [
        ("every row allowed", (|_| true) as fn(usize) -> bool),
        ("nine rows in ten allowed", |row| row % 10 != 3),
    ] {
        let db = fixture(searchable);
        let (mut hits, mut wanted) = (0usize, 0usize);
        for probe in 0..4 {
            let indexed = find(&db, "INDEXED", probe);
            let exact = find(&db, "EXACT", probe);
            let search = indexed
                .trace
                .vector_search
                .as_ref()
                .expect("the INDEXED find discloses its route");
            let budget = search
                .global_candidate_budget
                .expect("the graph walk discloses its budget") as u64;
            // A bounded probe reads at most half the walk budget of index
            // entries; the walk checks at most one row per point its budget
            // lets it evaluate; the rows the answer outputs are read once more.
            assert!(
                indexed.trace.rows_examined <= budget + budget / 2 + LIMIT as u64,
                "{label} probe {probe}: the find examined {} rows, past its walk budget of \
                 {budget}, half that again for its probe, and its {LIMIT} output rows, over {ROWS} rows",
                indexed.trace.rows_examined
            );
            let expected = ids(&exact);
            hits += ids(&indexed)
                .iter()
                .filter(|id| expected.contains(id))
                .count();
            wanted += expected.len();
        }
        let recall = hits as f64 / wanted.max(1) as f64;
        println!("{label}: recall@{LIMIT} {recall:.3}");
        assert!(
            recall >= 0.95,
            "{label}: recall@{LIMIT} {recall:.3} against EXACT is below 0.95"
        );
    }
}
