//! FIX 2 — the F4 whole-key resolution must reach the post-upsert vector
//! re-attach, not just the row upsert.
//!
//! F4 made `upsert_row_for_sync` resolve the existing row on the WHOLE composite
//! key. But the two post-upsert vector re-lookups (the LatestWins and EdgeWins
//! arms of the sync-apply) still resolved the row by the LEADING key column
//! alone. For a composite-PK table whose leading column is auto-indexed (an FK),
//! two live rows sharing that leading column exist; the leading-column re-lookup
//! then selects the WRONG sibling and attaches the incoming embedding to it.
//!
//! What a reader should take from this file: an update that carries a new vector
//! for one row of a shared-leading-key pair must land the vector on THAT row,
//! matched on its whole identity — never on its sibling.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Every
//! assertion pins the vector to a specific row id resolved by value.

use contextdb_core::{Lsn, RowId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::*;
use std::collections::HashMap;
use std::sync::Arc;

const IDX_TABLE: &str = "refs";
const IDX_COLUMN: &str = "embedding";

fn idx() -> contextdb_core::VectorIndexRef {
    contextdb_core::VectorIndexRef::new(IDX_TABLE, IDX_COLUMN)
}

/// Composite PK whose LEADING column carries a foreign key, so the engine
/// auto-indexes it — the exact shape where a leading-column point lookup finds a
/// row and returns the WRONG sibling.
fn build_db() -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE contexts (id TEXT PRIMARY KEY, label TEXT)",
        &empty,
    )
    .expect("contexts table");
    db.execute(
        "CREATE TABLE refs (\
         context_id TEXT NOT NULL REFERENCES contexts(id), \
         entity_id TEXT NOT NULL, \
         note TEXT, \
         embedding VECTOR(3), \
         PRIMARY KEY (context_id, entity_id))",
        &empty,
    )
    .expect("refs table");
    let mut ctx = HashMap::new();
    ctx.insert("id".to_string(), Value::Text("ctx1".to_string()));
    ctx.insert("label".to_string(), Value::Text("shared".to_string()));
    db.execute(
        "INSERT INTO contexts (id, label) VALUES ($id, $label)",
        &ctx,
    )
    .expect("insert context");
    db
}

fn ref_row(entity: &str, note: &str, vector: [f32; 3], lsn: u64) -> RowChange {
    let mut values = HashMap::new();
    values.insert("context_id".to_string(), Value::Text("ctx1".to_string()));
    values.insert("entity_id".to_string(), Value::Text(entity.to_string()));
    values.insert("note".to_string(), Value::Text(note.to_string()));
    values.insert("embedding".to_string(), Value::Vector(vector.to_vec()));
    RowChange {
        table: IDX_TABLE.to_string(),
        natural_key: NaturalKey::from_pairs(vec![
            ("context_id".to_string(), Value::Text("ctx1".to_string())),
            ("entity_id".to_string(), Value::Text(entity.to_string())),
        ])
        .expect("composite natural key"),
        values,
        deleted: false,
        lsn: Lsn(lsn),
        created_at: None,
    }
}

fn vector_change(edge_row_id: u64, vector: [f32; 3], lsn: u64) -> VectorChange {
    VectorChange {
        index: idx(),
        row_id: RowId(edge_row_id),
        vector: vector.to_vec(),
        lsn: Lsn(lsn),
    }
}

/// Insert two rows that share the leading key column but differ in the whole key,
/// each with its own embedding. Returns their resolved LOCAL row ids.
fn seed_pair(db: &Database) -> (RowId, RowId) {
    let changeset = ChangeSet {
        rows: vec![
            ref_row("ent1", "first", [1.0, 0.0, 0.0], 10),
            ref_row("ent2", "second", [0.0, 1.0, 0.0], 11),
        ],
        edges: Vec::new(),
        vectors: vec![
            vector_change(u64::MAX - 2, [1.0, 0.0, 0.0], 10),
            vector_change(u64::MAX - 1, [0.0, 1.0, 0.0], 11),
        ],
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let result = db.apply_changes(changeset, &policies).expect("seed apply");
    assert_eq!(result.applied_rows, 2, "both rows inserted: {result:?}");

    let ent1 = db
        .query_vector(idx(), &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .expect("seed query ent1");
    let ent2 = db
        .query_vector(idx(), &[0.0, 1.0, 0.0], 1, None, db.snapshot())
        .expect("seed query ent2");
    assert!(
        ent1[0].1 > 0.99 && ent2[0].1 > 0.99,
        "seed vectors are findable"
    );
    assert_ne!(ent1[0].0, ent2[0].0, "the two rows are distinct local rows");
    (ent1[0].0, ent2[0].0)
}

/// Update `ent2`'s embedding to `[0,0,1]` under `policy`, then assert the new
/// vector attaches to `ent2`'s whole-key row and `ent1`'s vector is untouched.
fn assert_update_hits_the_whole_key_row(policy: ConflictPolicy) {
    let db = build_db();
    let (ent1_row, ent2_row) = seed_pair(&db);

    let changeset = ChangeSet {
        rows: vec![ref_row("ent2", "updated", [0.0, 0.0, 1.0], 20)],
        edges: Vec::new(),
        vectors: vec![vector_change(u64::MAX, [0.0, 0.0, 1.0], 20)],
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let policies = ConflictPolicies::uniform(policy);
    db.apply_changes(changeset, &policies)
        .expect("update apply");

    let new_vec = db
        .query_vector(idx(), &[0.0, 0.0, 1.0], 1, None, db.snapshot())
        .expect("query new vector");
    assert!(
        new_vec[0].1 > 0.99,
        "the updated embedding must be findable under {policy:?}, got {}",
        new_vec[0].1
    );
    assert_eq!(
        new_vec[0].0, ent2_row,
        "the new embedding must attach to ent2's WHOLE-key row under {policy:?}, \
         not to the leading-key sibling ent1 ({:?})",
        ent1_row
    );

    let ent1_vec = db
        .query_vector(idx(), &[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .expect("query ent1 vector");
    assert!(
        ent1_vec[0].1 > 0.99 && ent1_vec[0].0 == ent1_row,
        "ent1's embedding must be untouched by the update to its sibling under {policy:?}"
    );
}

#[test]
fn fix2_latest_wins_update_attaches_vector_to_the_whole_key_row() {
    assert_update_hits_the_whole_key_row(ConflictPolicy::LatestWins);
}

#[test]
fn fix2_edge_wins_update_attaches_vector_to_the_whole_key_row() {
    assert_update_hits_the_whole_key_row(ConflictPolicy::EdgeWins);
}
