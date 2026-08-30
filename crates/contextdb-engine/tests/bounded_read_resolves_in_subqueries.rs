//! A statement whose filter is another query answers through a bounded read.
//!
//! `WHERE column IN (SELECT ...)` is ordinary SQL, and a caller who writes it
//! is entitled to the same answer whichever door it asks through. A door that
//! cannot resolve the inner query does not return a smaller answer -- it
//! returns no answer at all, and the caller is told the store cannot execute
//! a statement the store executes perfectly well through its other door. That
//! is a hole in the reading surface, not a limit on the query.
//!
//! The executor's answer is the oracle. The statement is asked outside any
//! transaction and again inside an open one, because a bounded read inside a
//! transaction takes a different path to the same rows and either could be
//! the one that cannot resolve the inner query.

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

/// Two departments a filter keeps and one it drops, with people in each, so
/// an answer that ignored the inner query would be visibly wider.
fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE departments (id UUID PRIMARY KEY, name TEXT)",
            &empty(),
        )
        .expect("create the department table");
    database
        .execute(
            "CREATE TABLE employees (id UUID PRIMARY KEY, dept TEXT, name TEXT)",
            &empty(),
        )
        .expect("create the employee table");
    for (ordinal, name) in [(1_u128, "engineering"), (2, "support")] {
        database
            .execute(
                "INSERT INTO departments (id, name) VALUES ($id, $name)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::from_u128(ordinal))),
                    ("name", Value::Text(name.to_owned())),
                ]),
            )
            .expect("insert a department");
    }
    for (ordinal, dept, name) in [
        (10_u128, "engineering", "alice"),
        (11, "support", "bob"),
        (12, "engineering", "carol"),
        (13, "a-department-nobody-created", "dave"),
    ] {
        database
            .execute(
                "INSERT INTO employees (id, dept, name) VALUES ($id, $dept, $name)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::from_u128(ordinal))),
                    ("dept", Value::Text(dept.to_owned())),
                    ("name", Value::Text(name.to_owned())),
                ]),
            )
            .expect("insert an employee");
    }
    database
}

const IN_SUBQUERY: &str =
    "SELECT name FROM employees WHERE dept IN (SELECT name FROM departments) ORDER BY name";

fn answer(result: &QueryResult) -> String {
    format!("columns={:?} rows={:?}", result.columns, result.rows)
}

fn both_doors_agree(database: &Database, what: &str, disagreements: &mut Vec<String>) {
    let eager = database
        .execute(IN_SUBQUERY, &empty())
        .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));
    assert_eq!(
        eager.rows.len(),
        3,
        "{what}: the fixture keeps the three people in a department that exists"
    );

    let bounded = database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(IN_SUBQUERY, &empty());
    println!(
        "OBSERVED {what}: executor {} | bounded {}",
        answer(&eager),
        match &bounded {
            Ok(bounded) => answer(bounded),
            Err(error) => format!("refused: {error}"),
        }
    );

    match bounded {
        Ok(bounded) => {
            if answer(&bounded) != answer(&eager) {
                disagreements.push(format!(
                    "{what}: the executor answers {} and a bounded read answers {}",
                    answer(&eager),
                    answer(&bounded)
                ));
            }
        }
        Err(error) => disagreements.push(format!(
            "{what}: the executor answers {} and a bounded read cannot run the statement at all: \
             {error}",
            answer(&eager)
        )),
    }
}

#[test]
fn a_filter_written_as_another_query_answers_through_a_bounded_read() {
    let database = seeded();
    let mut disagreements = Vec::new();

    both_doors_agree(&database, "outside any transaction", &mut disagreements);

    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    both_doors_agree(&database, "inside an open transaction", &mut disagreements);
    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");

    assert!(
        disagreements.is_empty(),
        "a filter written as another query is ordinary SQL, and a door that cannot run it tells \
         the caller the store cannot do something the store does through its other door:\n{}",
        disagreements.join("\n")
    );
}
