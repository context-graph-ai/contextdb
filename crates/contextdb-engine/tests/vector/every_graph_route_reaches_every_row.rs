//! Neighbour selection on near-duplicate vectors leaves whole groups of points
//! with no incoming link: the language fixture's graphs once left 95 of 150
//! points unreachable, and INDEXED either missed those rows or refused the
//! partition. Every graph now holds the links its reachability owes, so each
//! route answers INDEXED exactly as EXACT does: the maintained graph, the
//! tail rows committed after it, and an import tail saved in pages, in each
//! representation, filtered and not.

use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

const DIMENSION: usize = 32;
const REPRESENTATIONS: [&str; 3] = [
    "",
    " WITH (quantization = 'SQ8')",
    " WITH (quantization = 'SQ4')",
];

/// One shared axis plus a small, moving second one.
fn degenerate(ordinal: usize) -> Vec<f32> {
    let mut vector = vec![0.0_f32; DIMENSION];
    vector[0] = 1.0;
    vector[(ordinal + 1) % DIMENSION] = (ordinal % 150 + 1) as f32 / 1000.0;
    vector
}

/// Queries near the fixture, none equal to a stored vector.
fn queries() -> Vec<Vec<f32>> {
    (0..6)
        .map(|probe| {
            let mut query = vec![0.0_f32; DIMENSION];
            query[0] = 1.0;
            query[(probe * 5 + 2) % DIMENSION] = 0.0137 * (probe + 1) as f32;
            query[(probe * 7 + 3) % DIMENSION] += 0.0021;
            query
        })
        .collect()
}

fn options() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    }
}

/// The search breadth exceeds every route's rows, so a walk that stops
/// short of a row it can reach is impossible: an INDEXED answer short of
/// EXACT's would be a row the graph does not reach.
fn declare(db: &Database, representation: &str) {
    db.execute(
        &format!(
            "CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, tag TEXT NOT NULL, \
             embedding VECTOR({DIMENSION}){representation} PARTITION_KEY (bucket) \
             MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
             HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)"
        ),
        &HashMap::new(),
    )
    .expect("declare the table");
    db.execute("CREATE INDEX items_tag ON items (tag)", &HashMap::new())
        .expect("index the filter column");
}

fn put(db: &Database, id: usize) {
    db.execute(
        "INSERT INTO items VALUES ($id, 1, $tag, $embedding)",
        &HashMap::from([
            ("id".into(), Value::Int64(id as i64)),
            (
                "tag".into(),
                Value::Text(
                    if id.is_multiple_of(3) {
                        "kept"
                    } else {
                        "other"
                    }
                    .into(),
                ),
            ),
            ("embedding".into(), Value::Vector(degenerate(id))),
        ]),
    )
    .expect("insert one row");
}

fn settle(db: &Database) {
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

fn ids(db: &Database, mode: &str, filter: &str, query: &[f32], label: &str) -> Vec<Value> {
    let sql = format!(
        "SELECT id FROM items {filter} ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
    );
    db.execute(
        &sql,
        &HashMap::from([("query".into(), Value::Vector(query.to_vec()))]),
    )
    .unwrap_or_else(|error| panic!("{label}: the {mode} search [{filter}] answers: {error}"))
    .rows
    .into_iter()
    .map(|row| row[0].clone())
    .collect()
}

fn assert_indexed_equals_exact(db: &Database, label: &str) {
    for (probe, query) in queries().iter().enumerate() {
        for filter in ["", "WHERE tag = 'kept'"] {
            assert_eq!(
                ids(db, "INDEXED", filter, query, label),
                ids(db, "EXACT", filter, query, label),
                "{label} probe {probe} [{filter}]: INDEXED answers as EXACT does"
            );
        }
    }
}

#[test]
fn every_graph_route_reaches_every_row() {
    for representation in REPRESENTATIONS {
        let root = tempfile::tempdir().expect("temporary directory");
        let path = root.path().join("reach.db");
        let label = |route: &str| format!("{route}{representation}");
        {
            let db = Database::open_with_options(&path, options()).expect("open");
            declare(&db, representation);
            for id in 0..150 {
                put(&db, id);
            }
            settle(&db);
            assert_indexed_equals_exact(&db, &label("maintained graph"));
            // Rows committed after maintenance are answered from the tail
            // graph they are inserted into, one commit at a time.
            for id in 150..300 {
                put(&db, id);
            }
            assert_indexed_equals_exact(&db, &label("tail"));
            db.close().expect("close");
        }
        let db = Database::open_with_options(&path, options()).expect("reopen");
        for id in 300..450 {
            put(&db, id);
        }
        assert!(
            db.persist_import_vector_tails()
                .expect("save the import tail in pages")
        );
        assert_indexed_equals_exact(&db, &label("paged import tail"));
        db.close().expect("close");
        let reopened = Database::open_with_options(&path, options()).expect("reopen the pages");
        assert_indexed_equals_exact(&reopened, &label("reopened paged import tail"));
    }
}
