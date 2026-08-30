//! A neighbour search over candidates a named query produced answers through
//! a bounded read.
//!
//! "Take the things this node links to, drop the ones marked deprecated, and
//! give me the two nearest" is one question, written the way SQL lets you
//! write it: named intermediate queries feeding a neighbour search. The
//! executor answers it. A door that cannot does not return a rougher answer
//! -- it returns none, and the caller is told the store cannot do something
//! the store does perfectly well through its other door. For a reading
//! surface that is a hole, not a limit: the caller has no smaller question to
//! fall back on, because the filtering is the point.
//!
//! The executor's answer is the oracle, and the order is part of it: the
//! statement asks for the nearest two, so which one comes first is the
//! answer.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn entity(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00C7_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

const ROOT: u128 = 1;
const NEAR: u128 = 2;
const FAR: u128 = 3;
/// Linked to the root but marked deprecated, so the named query in the middle
/// really has something to drop.
const RETIRED: u128 = 4;
/// Nearest of all, but nothing links to it, so the first named query really
/// has something to exclude.
const UNLINKED: u128 = 5;

fn vector_of(ordinal: u128) -> Vec<f32> {
    match ordinal {
        ROOT => vec![0.0, 0.0, 1.0],
        NEAR => vec![1.0, 0.0, 0.0],
        FAR => vec![0.0, 1.0, 0.0],
        RETIRED => vec![0.99, 0.01, 0.0],
        UNLINKED => vec![1.0, 0.0, 0.0],
        other => panic!("no vector for entity {other}"),
    }
}

fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3), \
             is_deprecated BOOLEAN)",
            &empty(),
        )
        .expect("create the entity table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");

    for (ordinal, name, deprecated) in [
        (ROOT, "root", false),
        (NEAR, "near", false),
        (FAR, "far", false),
        (RETIRED, "retired", true),
        (UNLINKED, "unlinked", false),
    ] {
        database
            .execute(
                "INSERT INTO entities (id, name, embedding, is_deprecated) \
                 VALUES ($id, $name, $embedding, $deprecated)",
                &params(vec![
                    ("id", Value::Uuid(entity(ordinal))),
                    ("name", Value::Text(name.to_owned())),
                    ("embedding", Value::Vector(vector_of(ordinal))),
                    ("deprecated", Value::Bool(deprecated)),
                ]),
            )
            .expect("insert an entity");
    }

    let tx = database.begin().expect("begin the edge batch");
    for ordinal in [NEAR, FAR, RETIRED] {
        database
            .insert_edge(
                tx,
                entity(ROOT),
                entity(ordinal),
                "RELATES_TO".to_owned(),
                HashMap::new(),
            )
            .expect("link the root to a neighbour");
        database
            .insert_row(
                tx,
                "edges",
                HashMap::from([
                    ("id".to_owned(), Value::Uuid(Uuid::new_v4())),
                    ("source_id".to_owned(), Value::Uuid(entity(ROOT))),
                    ("target_id".to_owned(), Value::Uuid(entity(ordinal))),
                    ("edge_type".to_owned(), Value::Text("RELATES_TO".to_owned())),
                ]),
            )
            .expect("record the edge row");
    }
    database.commit(tx).expect("commit the edge batch");
    database
}

const CTE_NEIGHBOURS: &str = "WITH neighborhood AS (
        SELECT b_id FROM GRAPH_TABLE(
            edges MATCH (a)-[:RELATES_TO]->(b)
            WHERE a.id = $root COLUMNS (b.id AS b_id)
        )
    ),
    filtered AS (
        SELECT id, name, embedding
        FROM entities e
        INNER JOIN neighborhood n ON e.id = n.b_id
        WHERE e.is_deprecated = FALSE
    )
    SELECT id, name FROM filtered ORDER BY embedding <=> $query LIMIT 2";

fn cte_params() -> HashMap<String, Value> {
    params(vec![
        ("root", Value::Uuid(entity(ROOT))),
        ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
    ])
}

fn answer(result: &QueryResult) -> String {
    format!("columns={:?} rows={:?}", result.columns, result.rows)
}

#[test]
fn a_neighbour_search_over_candidates_a_named_query_produced_answers_through_a_bounded_read() {
    let database = seeded();

    let eager = database
        .execute(CTE_NEIGHBOURS, &cte_params())
        .expect("the executor answers the candidate-filtered neighbour search");
    assert_eq!(
        eager.rows.len(),
        2,
        "the fixture keeps two candidates after the named queries have narrowed them"
    );
    let names: Vec<String> = eager
        .rows
        .iter()
        .map(|row| match row.get(1) {
            Some(Value::Text(name)) => name.clone(),
            other => panic!("expected a name in the second column, got {other:?}"),
        })
        .collect();
    assert_eq!(
        names,
        vec!["near".to_owned(), "far".to_owned()],
        "the deprecated neighbour and the entity nothing links to are both left out, and the \
         nearest kept candidate comes first"
    );

    let bounded = database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(CTE_NEIGHBOURS, &cte_params());
    println!(
        "OBSERVED a candidate-filtered neighbour search: executor {} | bounded {}",
        answer(&eager),
        match &bounded {
            Ok(bounded) => answer(bounded),
            Err(error) => format!("refused: {error}"),
        }
    );

    match bounded {
        Ok(bounded) => assert_eq!(
            answer(&bounded),
            answer(&eager),
            "the same question, asked through a bounded read, has the same answer"
        ),
        Err(error) => panic!(
            "the executor answers {} and a bounded read cannot run the statement at all: {error} \
             -- the filtering is the point of the question, so the caller has no smaller one to \
             fall back on",
            answer(&eager)
        ),
    }
}
