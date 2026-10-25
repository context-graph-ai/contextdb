//! The small populated stores the routes run against. Each row builds its
//! own, so one route's writes or failure never reach another row.

use contextdb_core::{RowId, Value, VectorIndexRef};
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

pub const TENANT: &str = "route-owner";
pub const EDGE_TYPE: &str = "links";

pub fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

pub fn none() -> HashMap<String, Value> {
    HashMap::new()
}

pub fn run(db: &Database, sql: &str) {
    db.execute(sql, &none())
        .unwrap_or_else(|error| panic!("fixture statement `{sql}` failed: {error:?}"));
}

pub fn notes_index() -> VectorIndexRef {
    VectorIndexRef::new("notes", "embedding")
}

pub fn source_node() -> Uuid {
    Uuid::from_u128(0xA1)
}

pub fn target_node() -> Uuid {
    Uuid::from_u128(0xB2)
}

/// Declare and fill the ordinary tables: rows with an index and a vector
/// column, a graph edge, and a table whose history carries two identical
/// search-breadth declarations followed by a dropped column, with saved
/// serving graphs for both of its vector columns.
pub fn populate(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    run(
        db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT, embedding VECTOR(3)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    run(db, "CREATE INDEX notes_bucket ON notes (bucket)");
    for id in 1..=3i64 {
        db.execute(
            "INSERT INTO notes (id, bucket, body, embedding) VALUES ($id, $bucket, $body, $vector)",
            &params(&[
                ("id", Value::Int64(id)),
                ("bucket", Value::Int64(id % 2)),
                ("body", Value::Text(format!("note-{id}"))),
                ("vector", Value::Vector(vec![id as f32, 1.0, 0.5])),
            ]),
        )
        .unwrap_or_else(|error| panic!("fixture note {id} failed: {error:?}"));
    }
    run(db, "UPDATE notes SET body = 'revised' WHERE id = 2");
    let tx = db.begin().expect("fixture graph transaction");
    db.insert_edge(
        tx,
        source_node(),
        target_node(),
        EDGE_TYPE.to_string(),
        HashMap::from([("weight".to_string(), Value::Int64(1))]),
    )
    .expect("fixture graph edge");
    db.commit(tx).expect("fixture graph commit");
    run(
        db,
        "CREATE TABLE breadth_rows (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED HNSW (EF_SEARCH = 7), compressed VECTOR(3) WITH (quantization = 'SQ8') SEARCH_MODE INDEXED HNSW (EF_SEARCH = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    for id in 1..=4i64 {
        let x = id as f32;
        db.execute(
            "INSERT INTO breadth_rows (id, bucket, body, embedding, compressed) VALUES ($id, $bucket, $body, $vector, $vector)",
            &params(&[
                ("id", Value::Int64(id)),
                ("bucket", Value::Int64(id % 2)),
                ("body", Value::Text(format!("authored-{id}"))),
                ("vector", Value::Vector(vec![x, 5.0 - x, 1.0])),
            ]),
        )
        .unwrap_or_else(|error| panic!("fixture breadth row {id} failed: {error:?}"));
    }
    run(db, "UPDATE breadth_rows SET body = 'replacement' WHERE id = 1");
    for _ in 0..8 {
        if ["embedding", "compressed"].iter().all(|column| {
            db.__debug_vector_hnsw_build_serial_for_test(VectorIndexRef::new(
                "breadth_rows",
                *column,
            ))
            .is_some()
        }) {
            break;
        }
        db.run_maintenance_cycle()
            .expect("fixture serving-graph build");
    }
    run(
        db,
        "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 11)",
    );
    run(
        db,
        "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 11)",
    );
    run(db, "ALTER TABLE breadth_rows DROP COLUMN body");
}

/// A file-backed store, populated, with its writer open.
pub struct Store {
    pub dir: tempfile::TempDir,
    pub path: PathBuf,
    pub db: Arc<Database>,
}

impl Store {
    pub fn open() -> Self {
        let dir = tempfile::tempdir().expect("fixture directory");
        let path = dir.path().join("store.db");
        let db = Database::open(&path).expect("fixture store open");
        populate(&db);
        Self {
            dir,
            path,
            db: Arc::new(db),
        }
    }

    /// The populated store's file with no writer left open, for routes that
    /// open a path themselves.
    pub fn closed() -> (tempfile::TempDir, PathBuf) {
        let store = Self::open();
        store.db.close().expect("fixture store close");
        let Store { dir, path, db } = store;
        drop(db);
        (dir, path)
    }

    pub fn sibling(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    pub fn row_id(&self, id: i64) -> RowId {
        self.db
            .point_lookup("notes", "id", &Value::Int64(id), self.db.snapshot())
            .expect("fixture point lookup")
            .expect("fixture row exists")
            .row_id
    }
}

/// An in-memory store with the same population.
pub fn memory_store() -> Arc<Database> {
    let db = Database::open_memory();
    populate(&db);
    Arc::new(db)
}

pub fn existing_file(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, b"route fixture input").expect("fixture input file");
    path
}
