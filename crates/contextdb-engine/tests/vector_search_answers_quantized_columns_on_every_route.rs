//! A vector search answers a quantized column, whichever door asked.
//!
//! A quantized column is stored only in its vector index; the relational
//! row's own slot holds nothing. A read that projects such a column has to
//! fetch it from the store that holds it. A read that does not answers
//! `NULL` -- and `NULL` is not an error, so nothing tells the caller their
//! embedding is gone. They write it down, index it, compare against it, and
//! every answer built on it afterwards is silently wrong.
//!
//! The column is the same column on every route, so the answer is the same:
//! the writer's own handle, the bounded read view, a cursor over it, and a
//! reader that opened the committed file with no writer running. And it is
//! the same whether the search ORDERED BY an ordinary vector column or by a
//! quantized one.
//!
//! Expected values come from the vector crate's own quantizer, not from the
//! engine this file is about.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_core::table_meta::VectorQuantization;
use contextdb_engine::{Database, QueryResult, ReadSession};
use contextdb_vector::stored_vector_value;
use std::collections::HashMap;
use uuid::Uuid;

/// Width of every vector column below.
const DIMENSIONS: usize = 8;
/// Candidates each search asks for.
const SEARCH_LIMIT: usize = 2;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000,
        result_bytes: 64 * 1024 * 1024,
        work: 100_000_000,
        active_ms: 600_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// The rows every route must be able to answer for.
fn fixture_rows() -> Vec<(Uuid, Vec<f32>)> {
    vec![
        (
            Uuid::from_u128(0x00B2_0000_0000_0000_0000_0000_0000_0001),
            vec![1.00, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.30],
        ),
        (
            Uuid::from_u128(0x00B2_0000_0000_0000_0000_0000_0000_0002),
            vec![0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00],
        ),
        (
            Uuid::from_u128(0x00B2_0000_0000_0000_0000_0000_0000_0003),
            vec![-1.00, -0.50, 0.00, 0.50, 1.00, 0.50, 0.00, -0.50],
        ),
    ]
}

/// What the quantizer makes of a vector, computed here rather than read back
/// from the engine under test.
fn expected(vector: &[f32], quantization: VectorQuantization) -> Vec<f32> {
    stored_vector_value(vector, quantization)
}

fn query_vector() -> Value {
    Value::Vector(fixture_rows()[0].1.clone())
}

/// A seeded store, returned LIVE. Reopening it would defeat the point: a
/// store that has been closed and loaded again carries the vector in the
/// relational row itself, so nothing has to be fetched from the index and
/// every route answers correctly whether or not it knows how to fetch.
fn seeded_store(path: &std::path::Path) -> Database {
    let database = Database::open(path).expect("create the store");
    database
        .execute(
            &format!(
                "CREATE TABLE embeddings (\
                 id UUID PRIMARY KEY, \
                 plain VECTOR({DIMENSIONS}), \
                 compact VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8'), \
                 tiny VECTOR({DIMENSIONS}) WITH (quantization = 'SQ4'))"
            ),
            &empty(),
        )
        .expect("declare one ordinary and two quantized vector columns");
    for (id, vector) in fixture_rows() {
        database
            .execute(
                "INSERT INTO embeddings (id, plain, compact, tiny) \
                 VALUES ($id, $plain, $compact, $tiny)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("plain", Value::Vector(vector.clone())),
                    ("compact", Value::Vector(vector.clone())),
                    ("tiny", Value::Vector(vector)),
                ]),
            )
            .expect("store a row carrying all three vector columns");
    }
    database
}

/// `SELECT id, compact, tiny ... ORDER BY <column> <=> $query LIMIT k`.
fn search_sql(order_by: &str) -> String {
    format!(
        "SELECT id, compact, tiny FROM embeddings ORDER BY {order_by} <=> $query \
         LIMIT {SEARCH_LIMIT}"
    )
}

/// Every row a search answered carries both quantized columns, holding the
/// quantizer's own answer for the vector that row was given.
fn assert_answers_the_quantized_columns(result: &QueryResult, route: &str, shape: &str) {
    // Prove the read took the route this file is about. A vector search that
    // quietly fell back to a table scan reads the column from the row, which
    // is the one place it is never missing -- so a pass would say nothing.
    assert!(
        result.trace.physical_plan.contains("Search"),
        "the {shape} search on the {route} route is answered by a vector search, not by {}",
        result.trace.physical_plan
    );
    assert_eq!(
        result.rows.len(),
        SEARCH_LIMIT,
        "the {shape} search on the {route} route answers with the candidates it asked for"
    );
    let by_id: HashMap<Uuid, Vec<f32>> = fixture_rows().into_iter().collect();

    for row in &result.rows {
        let id = match row.first() {
            Some(Value::Uuid(id)) => *id,
            other => panic!("the {route} route answers with an id-leading row, got {other:?}"),
        };
        let vector: &Vec<f32> = by_id
            .get(&id)
            .expect("an answered row is one of the stored rows");
        for (position, column, quantization) in [
            (1_usize, "compact", VectorQuantization::SQ8),
            (2, "tiny", VectorQuantization::SQ4),
        ] {
            match row.get(position) {
                Some(Value::Vector(answered)) => assert_eq!(
                    answered,
                    &expected(vector.as_slice(), quantization),
                    "the {shape} search on the {route} route answers {column} for {id} with the \
                     quantizer's own vector"
                ),
                other => panic!(
                    "the {shape} search on the {route} route answers {column} for {id} with a \
                     vector; it answered {other:?}, and a caller told NULL keeps the row, indexes \
                     it, and compares against it without ever learning the embedding is gone"
                ),
            }
        }
    }
}

/// Both search shapes, on one route.
fn assert_route_answers(route: &str, mut run: impl FnMut(&str) -> QueryResult) {
    for (shape, order_by) in [
        ("ordinary-column", "plain"),
        ("quantized-column", "compact"),
    ] {
        let result = run(&search_sql(order_by));
        assert_answers_the_quantized_columns(&result, route, shape);
    }
}

#[test]
fn the_writers_own_handle_answers_the_quantized_columns_a_search_projects() {
    let directory = tempfile::TempDir::new().expect("task-scoped vector-route directory");
    let path = directory.path().join("writer.db");
    let database = seeded_store(&path);

    assert_route_answers("writer handle", |sql| {
        database
            .execute(sql, &params(vec![("query", query_vector())]))
            .expect("the writer's own handle serves a vector search")
    });
    database.close().expect("the writer closes cleanly");
}

#[test]
fn the_bounded_read_view_answers_the_quantized_columns_a_search_projects() {
    let directory = tempfile::TempDir::new().expect("task-scoped vector-route directory");
    let path = directory.path().join("bounded.db");
    let database = seeded_store(&path);

    assert_route_answers("bounded read view", |sql| {
        database
            .read_session(roomy())
            .expect("open a bounded read view")
            .execute(sql, &params(vec![("query", query_vector())]))
            .expect("the bounded view serves a vector search")
    });
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_cursor_answers_the_quantized_columns_a_search_projects() {
    let directory = tempfile::TempDir::new().expect("task-scoped vector-route directory");
    let path = directory.path().join("cursor.db");
    let database = seeded_store(&path);

    assert_route_answers("cursor", |sql| {
        let session = database
            .read_session(roomy())
            .expect("open a bounded read view");
        // A cursor's pages carry no trace of their own, so the statement the
        // cursor is opened for is proven to be a vector search through the
        // same session's one-shot answer for it.
        let traced = session
            .execute(sql, &params(vec![("query", query_vector())]))
            .expect("the same statement is served in one shot");
        assert!(
            traced.trace.physical_plan.contains("Search"),
            "the statement this cursor is opened for is answered by a vector search, not by {}",
            traced.trace.physical_plan
        );
        let mut cursor = session
            .open_cursor(sql, &params(vec![("query", query_vector())]))
            .expect("open a cursor over a vector search");
        let columns = cursor.first_page().columns.clone();
        let mut rows = cursor.first_page().rows.clone();
        let mut more = cursor.first_page().has_more;
        while more {
            let page = cursor.fetch(None).expect("a cursor page arrives");
            rows.extend(page.rows.iter().cloned());
            more = page.has_more;
        }
        QueryResult {
            columns,
            rows,
            rows_affected: 0,
            trace: traced.trace,
            cascade: None,
        }
    });
    database.close().expect("the writer closes cleanly");
}

#[test]
fn a_reader_of_the_committed_file_answers_the_quantized_columns_a_search_projects() {
    let directory = tempfile::TempDir::new().expect("task-scoped vector-route directory");
    let path = directory.path().join("file-route.db");
    seeded_store(&path)
        .close()
        .expect("the writer closes so the file can be read on its own");

    // Nobody is holding the store, so this is the reader that decodes the
    // committed file for itself.
    assert_route_answers("committed file", |sql| {
        ReadSession::open(&path)
            .expect("open the committed file for reading")
            .execute(sql, &params(vec![("query", query_vector())]))
            .expect("a reader of the committed file serves a vector search")
    });
}
