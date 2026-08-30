//! A bounded neighbour search takes the same route to the answer the
//! executor takes.
//!
//! Once a vector column has enough rows, the store answers a neighbour search
//! through its approximate index rather than by scoring every document, and
//! it says so in the trace. That line is how an operator knows a search over
//! a million documents is not quietly reading a million documents. A door
//! that reports a different route is not describing the same work: either it
//! really is scoring everything and the operator cannot see it, or it is not
//! and the trace is fiction. Both are answers the operator cannot act on.
//!
//! The executor's own trace is the oracle, and the oracle is checked first --
//! each shape asserts that the EXECUTOR reports the approximate index before
//! comparing, so a case cannot pass by both doors agreeing on a brute-force
//! scan. Rows are compared in order, because these statements all ask for the
//! nearest few and the order is the answer.

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

/// Enough documents that the approximate index is the route the store picks.
/// A thousand exactly is not enough -- the store still scores every document
/// at that size -- so this sits above the point where the route changes.
const DOCUMENTS: usize = 1_024;

fn doc(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x007E_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn source() -> Uuid {
    doc(1)
}

fn source_vector() -> Vec<f32> {
    vec![0.731_234, 0.212_345, 0.648_901]
}

fn scattered(ordinal: usize) -> Vec<f32> {
    let angle = ordinal as f32 * 0.013;
    vec![
        angle.sin().abs(),
        angle.cos().abs(),
        (angle * 0.5).sin().abs(),
    ]
}

fn plain_documents() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .expect("create the document table");
    insert_documents(&database);
    database
}

/// The same documents under a ranking policy, so the ranked shape is measured
/// on a column whose answer is a weighted score rather than raw distance.
fn ranked_documents() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success_rate REAL)",
            &empty(),
        )
        .expect("create the outcome table");
    database
        .execute(
            "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
            &empty(),
        )
        .expect("index the outcome table");
    database
        .execute(
            "CREATE TABLE docs (
                id UUID PRIMARY KEY,
                label TEXT,
                embedding VECTOR(3) RANK_POLICY (
                    JOIN outcomes ON decision_id,
                    FORMULA '{vector_score} * coalesce({success_rate}, 1.0)',
                    SORT_KEY weighted
                )
            )",
            &empty(),
        )
        .expect("create the ranked document table");
    insert_documents(&database);
    for ordinal in 0..DOCUMENTS {
        database
            .execute(
                "INSERT INTO outcomes (id, decision_id, success_rate) \
                 VALUES ($id, $decision_id, $success_rate)",
                &params(vec![
                    ("id", Value::Uuid(doc(100_000 + ordinal as u128))),
                    ("decision_id", Value::Uuid(doc(1 + ordinal as u128))),
                    (
                        "success_rate",
                        Value::Float64(0.25 + (ordinal % 4) as f64 * 0.5),
                    ),
                ]),
            )
            .expect("record an outcome for a document");
    }
    database
}

fn insert_documents(database: &Database) {
    for ordinal in 0..DOCUMENTS {
        let embedding = if ordinal == 0 {
            source_vector()
        } else {
            scattered(ordinal)
        };
        database
            .execute(
                "INSERT INTO docs (id, label, embedding) VALUES ($id, $label, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(doc(1 + ordinal as u128))),
                    ("label", Value::Text(format!("doc-{ordinal}"))),
                    ("embedding", Value::Vector(embedding)),
                ]),
            )
            .expect("insert a document");
    }
}

fn described(result: &QueryResult) -> String {
    format!(
        "plan={:?} index_used={:?} rows={:?}",
        result.trace.physical_plan, result.trace.index_used, result.rows
    )
}

const FILTERED_PERSISTED: &str = "SELECT id FROM docs WHERE id != $source \
                                  ORDER BY embedding <=> ROW_VECTOR('docs','embedding',$source) \
                                  LIMIT 5";
const FILTERED_LITERAL: &str = "SELECT id FROM docs WHERE id != $source \
                                ORDER BY embedding <=> $query LIMIT 5";
const RANKED_PERSISTED: &str = "SELECT id, score FROM docs \
                                ORDER BY embedding <=> ROW_VECTOR('docs','embedding',$source) \
                                USE RANK weighted LIMIT 5";
const RANKED_LITERAL: &str = "SELECT id, score FROM docs ORDER BY embedding <=> $query \
                              USE RANK weighted LIMIT 5";

fn source_params() -> HashMap<String, Value> {
    params(vec![("source", Value::Uuid(source()))])
}

fn query_params() -> HashMap<String, Value> {
    params(vec![("query", Value::Vector(source_vector()))])
}

fn both_sources() -> HashMap<String, Value> {
    params(vec![
        ("source", Value::Uuid(source())),
        ("query", Value::Vector(source_vector())),
    ])
}

/// Ask one statement through both doors, having first checked that the
/// executor really takes the approximate route.
fn both_doors_take_the_same_route(
    database: &Database,
    what: &str,
    sql: &str,
    sql_params: &HashMap<String, Value>,
    disagreements: &mut Vec<String>,
) {
    let eager = database
        .execute(sql, sql_params)
        .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));
    assert!(
        eager.trace.physical_plan.contains("HNSWSearch"),
        "{what}: the executor answers this through the approximate index, which is what makes \
         the case worth comparing: {:?}",
        eager.trace.physical_plan
    );
    assert!(
        !eager.rows.is_empty(),
        "{what}: the executor finds neighbours to compare"
    );

    let bounded = match database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, sql_params)
    {
        Ok(bounded) => bounded,
        Err(error) => {
            disagreements.push(format!(
                "{what}: the executor answers {} and a bounded read refuses: {error}",
                described(&eager)
            ));
            return;
        }
    };
    println!(
        "OBSERVED {what}: executor {} | bounded {}",
        described(&eager),
        described(&bounded)
    );

    if described(&bounded) != described(&eager) {
        disagreements.push(format!(
            "{what}: the executor answers {} and a bounded read answers {}",
            described(&eager),
            described(&bounded)
        ));
    }
}

#[test]
fn a_bounded_neighbour_search_reports_the_route_the_executor_reports() {
    let plain = plain_documents();
    let ranked = ranked_documents();
    let mut disagreements = Vec::new();

    both_doors_take_the_same_route(
        &plain,
        "a filtered search whose query vector is read from a stored row",
        FILTERED_PERSISTED,
        &source_params(),
        &mut disagreements,
    );
    both_doors_take_the_same_route(
        &plain,
        "the same filtered search written with the vector spelled out",
        FILTERED_LITERAL,
        &both_sources(),
        &mut disagreements,
    );

    // Asking again once the graph exists is the read-only case: the second
    // search must take the same route as the first and change nothing.
    both_doors_take_the_same_route(
        &plain,
        "the same search again, with the graph already built",
        FILTERED_PERSISTED,
        &source_params(),
        &mut disagreements,
    );

    both_doors_take_the_same_route(
        &ranked,
        "a ranked search whose query vector is read from a stored row",
        RANKED_PERSISTED,
        &source_params(),
        &mut disagreements,
    );
    both_doors_take_the_same_route(
        &ranked,
        "the same ranked search written with the vector spelled out",
        RANKED_LITERAL,
        &query_params(),
        &mut disagreements,
    );

    assert!(
        disagreements.is_empty(),
        "the trace is how an operator knows a search over a large collection is not quietly \
         reading all of it, so a door that names a different route describes work nobody can \
         check:\n{}",
        disagreements.join("\n")
    );
}
