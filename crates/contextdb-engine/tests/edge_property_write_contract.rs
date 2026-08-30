#![cfg(feature = "test-seams")]
//! Properties a caller hands to `insert_edge` must reach the edge.
//!
//! An edge can come into being two ways — named directly, or derived from a
//! row in a table shaped like an edge — and a caller attaching properties
//! does not know or care which happened first. If the second call quietly
//! discards what it was given while returning `Ok`, the caller is told their
//! properties were stored when nothing was, and no read anywhere will ever
//! show them. That is the defect this pins: not slow, not approximate —
//! absent, and silently so.
//!
//! The boolean answer keeps its own meaning: it says whether a NEW edge was
//! inserted, so an existing edge still answers `false`. What must not happen
//! is `false` doubling as "and I threw your properties away".

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn properties(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn edge_properties(db: &Database, source: Uuid, target: Uuid) -> HashMap<String, Value> {
    db.get_edge_properties(source, target, "links", db.snapshot())
        .expect("read the edge's properties")
        .expect("the edge is there to be read")
}

fn seeded(db: &Database, table: &str) {
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)"
        ),
        &HashMap::new(),
    )
    .expect("declare an edge-shaped table");
}

fn derive_edge(db: &Database, table: &str, source: Uuid, target: Uuid) {
    db.execute(
        &format!(
            "INSERT INTO {table} (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, $edge_type)"
        ),
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(0xED9E))),
            ("source".to_owned(), Value::Uuid(source)),
            ("target".to_owned(), Value::Uuid(target)),
            ("edge_type".to_owned(), Value::Text("links".to_owned())),
        ]),
    )
    .expect("derive the graph edge from a row");
}

fn attach(db: &Database, source: Uuid, target: Uuid, props: HashMap<String, Value>) -> bool {
    let tx = db.begin().expect("begin the attaching transaction");
    let inserted = db
        .insert_edge(tx, source, target, "links".to_owned(), props)
        .expect("attaching properties must not fail");
    db.commit(tx).expect("commit the attaching transaction");
    inserted
}

#[test]
fn a_new_edge_keeps_the_properties_it_was_named_with() {
    let db = Database::open_memory();
    let source = Uuid::from_u128(1);
    let target = Uuid::from_u128(2);

    let inserted = attach(
        &db,
        source,
        target,
        properties([("weight", Value::Float64(0.5))]),
    );

    assert!(inserted, "naming an absent edge inserts it");
    assert_eq!(
        edge_properties(&db, source, target),
        properties([("weight", Value::Float64(0.5))])
    );
}

#[test]
fn an_existing_edge_receives_the_properties_it_is_given() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("edges.db");
    let db = Database::open(&path).expect("open the store");
    let table = "edge_rows";
    let source = Uuid::from_u128(3);
    let target = Uuid::from_u128(4);
    seeded(&db, table);
    derive_edge(&db, table, source, target);
    assert!(
        edge_properties(&db, source, target).is_empty(),
        "a derived edge starts with no properties of its own"
    );

    let inserted = attach(
        &db,
        source,
        target,
        properties([
            ("evidence", Value::Text("witnessed".to_owned())),
            ("weight", Value::Float64(0.25)),
        ]),
    );

    assert!(
        !inserted,
        "the edge was already there, so nothing new was inserted"
    );
    assert_eq!(
        edge_properties(&db, source, target),
        properties([
            ("evidence", Value::Text("witnessed".to_owned())),
            ("weight", Value::Float64(0.25)),
        ]),
        "the properties the caller handed over must be on the edge"
    );
    assert_eq!(
        db.edge_count(source, "links", db.snapshot())
            .expect("count the edges out of the source"),
        1,
        "applying properties must not leave the edge twice over"
    );

    // And they are the edge's, not the session's.
    db.close().expect("close the writer");
    let reopened = Database::open(&path).expect("reopen the store");
    assert_eq!(
        edge_properties(&reopened, source, target),
        properties([
            ("evidence", Value::Text("witnessed".to_owned())),
            ("weight", Value::Float64(0.25)),
        ]),
        "properties applied to an existing edge must survive the writer"
    );
    reopened.close().expect("close the reopened store");
}

#[test]
fn naming_no_properties_asks_only_whether_the_edge_exists() {
    let db = Database::open_memory();
    let table = "probe_rows";
    let source = Uuid::from_u128(5);
    let target = Uuid::from_u128(6);
    seeded(&db, table);
    derive_edge(&db, table, source, target);
    attach(
        &db,
        source,
        target,
        properties([("weight", Value::Float64(1.5))]),
    );

    let inserted = attach(&db, source, target, HashMap::new());

    assert!(!inserted, "the edge exists, so nothing is inserted");
    assert_eq!(
        edge_properties(&db, source, target),
        properties([("weight", Value::Float64(1.5))]),
        "a caller who named no properties made no claim about them, so the edge keeps its own"
    );
}
