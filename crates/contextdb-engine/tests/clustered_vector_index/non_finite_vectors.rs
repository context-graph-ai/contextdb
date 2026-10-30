//! A vector with a NaN or infinite component is refused where it enters the
//! engine, by INSERT or UPDATE, with the typed InvalidVectorValue cause naming
//! the component's position, and nothing is stored: the row count, the
//! existing row's vector and the exact answer are unchanged, and finite
//! writes keep committing.

use super::support::{count, empty, int_ids, params};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 8;

fn finite(seed: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| if axis == seed % DIMENSIONS { 1.0 } else { 0.0 })
        .collect()
}

fn with(position: usize, component: f32) -> Vec<f32> {
    let mut vector = finite(1);
    vector[position] = component;
    vector
}

fn nearest(db: &Database, mode: &str) -> Vec<i64> {
    int_ids(
        &db.execute(
            &format!(
                "SELECT id FROM finite_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 3"
            ),
            &params([("query", Value::Vector(finite(1)))]),
        )
        .expect("vector search"),
    )
}

fn assert_refused(
    result: contextdb_core::Result<impl std::fmt::Debug>,
    position: usize,
    why: &str,
) {
    let error = result.expect_err(why);
    let debug = format!("{error:?}");
    assert!(
        debug.starts_with("InvalidVectorValue") && debug.contains(&position.to_string()),
        "{why}: the typed InvalidVectorValue cause naming position {position}, got {debug}"
    );
}

#[test]
fn non_finite_components_are_refused_at_the_engine_boundary() {
    let db = Database::open_memory();
    db.execute(
        &format!(
            "CREATE TABLE finite_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}))"
        ),
        &empty(),
    )
    .expect("declare the vector table");
    for id in 0..4 {
        db.execute(
            "INSERT INTO finite_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(finite(id as usize))),
            ]),
        )
        .expect("a finite vector commits");
    }
    let before = (count(&db, "finite_items"), nearest(&db, "EXACT"));

    for (position, component) in [(3, f32::NAN), (5, f32::INFINITY), (6, f32::NEG_INFINITY)] {
        assert_refused(
            db.execute(
                "INSERT INTO finite_items (id, embedding) VALUES (99, $embedding)",
                &params([("embedding", Value::Vector(with(position, component)))]),
            ),
            position,
            &format!("INSERT with {component} at position {position}"),
        );
        assert_refused(
            db.execute(
                "UPDATE finite_items SET embedding = $embedding WHERE id = 1",
                &params([("embedding", Value::Vector(with(position, component)))]),
            ),
            position,
            &format!("UPDATE to {component} at position {position}"),
        );
        assert_eq!(
            (count(&db, "finite_items"), nearest(&db, "EXACT")),
            before,
            "a refused {component} at position {position} stores nothing"
        );
    }

    db.execute(
        "INSERT INTO finite_items (id, embedding) VALUES (4, $embedding)",
        &params([("embedding", Value::Vector(finite(4)))]),
    )
    .expect("finite writes keep committing after the refusals");
    assert_eq!(count(&db, "finite_items"), before.0 + 1);
}
