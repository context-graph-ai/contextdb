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
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSION: usize = 16;
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

const EXHAUSTION_LIMIT: usize = 1;
fn declare_exact_passages(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
             is_searchable BOOLEAN NOT NULL, \
             embedding VECTOR({DIMENSION}) PARTITION_KEY (context_id) SEARCH_MODE EXACT)"
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

fn insert_passage(db: &Database, context: Uuid, row: usize, searchable: bool, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO passages VALUES ($id, $context, $searchable, $embedding)",
        &HashMap::from([
            (
                "id".to_owned(),
                Value::Uuid(Uuid::from_u128(row as u128 + 1)),
            ),
            ("context".to_owned(), Value::Uuid(context)),
            ("searchable".to_owned(), Value::Bool(searchable)),
            ("embedding".to_owned(), Value::Vector(embedding)),
        ]),
    )
    .expect("insert one passage");
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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

/// A filter that names no row finishes with no row. EXACT and AUTO both
/// answer an empty page, and an INDEXED request is refused with its typed
/// error. This is not a ledger that stopped early.
#[test]
fn completed_covered_filter_with_no_matches_returns_empty() {
    let context = Uuid::from_u128(0xc101);
    let db = Database::open_memory();
    declare_exact_passages(&db);
    let query = noise(9_002);
    for row in 0..3 {
        insert_passage(&db, context, row, false, noise(row as u64 + 10));
    }
    for mode in ["EXACT", "AUTO"] {
        let result = search_one_context(&db, mode, context, &query, EXHAUSTION_LIMIT)
            .unwrap_or_else(|error| panic!("{mode} refused a finished empty filter: {error:?}"));
        assert!(
            result.rows().is_empty(),
            "{mode} published rows for a filter that names none"
        );
    }
    assert!(matches!(
        search_one_context(&db, "INDEXED", context, &query, EXHAUSTION_LIMIT),
        Err(Error::VectorIndexedRouteUnavailable { .. })
    ));
}
