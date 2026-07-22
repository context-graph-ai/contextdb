//! FIX B — the outbound owner-row synthesis dedups on the WHOLE natural key, not
//! the leading key alone.
//!
//! When `changes_since` falls back to persisted state (after a reopen, where the
//! ephemeral change log is empty), a vector whose owner row is not otherwise in
//! the delta gets a synthesized owner `RowChange` so the receiver can map the
//! vector's row id to its natural key. That synthesis deduped by the LEADING
//! natural-key column plus the LSN only. Two composite-PK rows that share the
//! leading key, each carrying a vector at the SAME LSN, then collided: only the
//! first owner row was synthesized, and the second vector reached the receiver
//! with no remote-to-local mapping — attaching to the wrong row or orphaning. The
//! dedup must key on the whole natural key, the same basis the row-upsert dedup
//! already uses.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. The
//! defect is asserted on the outgoing changeset by whole identity, deterministic.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// Composite PK whose leading column carries a foreign key (auto-indexed), so two
/// live rows sharing that leading column exist — the shape where a leading-key
/// dedup folds two distinct identities into one.
fn build_db(path: &std::path::Path) -> Arc<Database> {
    let db = Arc::new(Database::open(path).expect("open db"));
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

#[test]
fn fixb_owner_row_synthesis_dedups_on_the_whole_key_not_the_leading_column() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().join("sender.db");

    {
        let db = build_db(&path);

        // Two rows that share the leading key column, each with a vector, written
        // in ONE statement so both vectors carry the SAME LSN.
        let mut seed = HashMap::new();
        seed.insert("v1".to_string(), Value::Vector(vec![1.0, 0.0, 0.0]));
        seed.insert("v2".to_string(), Value::Vector(vec![0.0, 1.0, 0.0]));
        db.execute(
            "INSERT INTO refs (context_id, entity_id, note, embedding) VALUES \
             ('ctx1','ent1','first',$v1), ('ctx1','ent2','second',$v2)",
            &seed,
        )
        .expect("insert the shared-leading-key pair");

        // A NON-vector update of both rows in ONE statement moves the rows' latest
        // relational version PAST the vectors' LSN without rewriting the vectors —
        // so on read-back the vectors need their owner rows synthesized.
        let mut note = HashMap::new();
        note.insert("n".to_string(), Value::Text("renamed".to_string()));
        db.execute("UPDATE refs SET note = $n WHERE context_id = 'ctx1'", &note)
            .expect("note update moves the rows past the vector lsn");
    }

    // Reopen: the ephemeral change log is empty, so `changes_since` derives the
    // delta from persisted state — the path that synthesizes owner rows.
    let db = Arc::new(Database::open(&path).expect("reopen sender"));
    let changeset = db.changes_since(Lsn(1));

    assert_eq!(
        changeset.vectors.len(),
        2,
        "both vectors are in the delta: {:?}",
        changeset.vectors
    );
    let vec_lsn = changeset.vectors[0].lsn;
    assert!(
        changeset.vectors.iter().all(|v| v.lsn == vec_lsn),
        "the two vectors share one LSN (they were written together): {:?}",
        changeset.vectors
    );

    // The owner rows synthesized for those vectors sit at the vectors' LSN (the
    // real rows moved on to a later LSN via the note update). Each must carry its
    // OWN whole identity.
    let synthesized_owner_keys: BTreeSet<String> = changeset
        .rows
        .iter()
        .filter(|row| row.table == "refs" && row.lsn == vec_lsn && !row.deleted)
        .map(|row| format!("{:?}", row.natural_key.pairs()))
        .collect();

    assert!(
        synthesized_owner_keys
            .iter()
            .any(|key| key.contains("ent1")),
        "the first vector's owner row is synthesized: {synthesized_owner_keys:?}"
    );
    assert!(
        synthesized_owner_keys
            .iter()
            .any(|key| key.contains("ent2")),
        "the SECOND shared-leading-key vector's owner row must also be synthesized; \
         the leading-key dedup drops it, leaving the receiver no mapping for that \
         vector. Owner keys at the vector LSN: {synthesized_owner_keys:?}"
    );
}
