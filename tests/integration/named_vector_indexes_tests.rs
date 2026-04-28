use contextdb_core::{Error, Lsn, RowId, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, VectorChange};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

#[test]
fn nv02_two_vector_columns_on_one_row_round_trip_independently() {
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(4)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let text_wins_id = Uuid::new_v4();
    let vision_wins_id = Uuid::new_v4();
    let text_probe = vec![1.0_f32, 0.0, 0.0, 0.0];
    let vision_probe = text_probe.clone();
    let text_wins_text = vec![1.0_f32, 0.0, 0.0, 0.0];
    let text_wins_vision = vec![0.0_f32, 1.0, 0.0, 0.0];
    let vision_wins_text = vec![0.0_f32, 1.0, 0.0, 0.0];
    let vision_wins_vision = vec![1.0_f32, 0.0, 0.0, 0.0];
    let pre_insert_lsn = db.current_lsn();
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(text_wins_id)),
            ("t", Value::Vector(text_wins_text.clone())),
            ("v", Value::Vector(text_wins_vision.clone())),
        ]),
    )
    .expect("insert text-winning row");
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(vision_wins_id)),
            ("t", Value::Vector(vision_wins_text.clone())),
            ("v", Value::Vector(vision_wins_vision.clone())),
        ]),
    )
    .expect("insert vision-winning row");

    let result = db
        .execute(
            "SELECT vector_text, vector_vision FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(text_wins_id))]),
        )
        .expect("select both vectors");
    assert_eq!(result.rows.len(), 1);
    let text_idx = result
        .columns
        .iter()
        .position(|c| c == "vector_text")
        .expect("text col");
    let vision_idx = result
        .columns
        .iter()
        .position(|c| c == "vector_vision")
        .expect("vision col");
    assert_eq!(
        result.rows[0][text_idx],
        Value::Vector(text_wins_text.clone())
    );
    assert_eq!(
        result.rows[0][vision_idx],
        Value::Vector(text_wins_vision.clone())
    );

    // Relational projection alone could pass against a global vector store (the row tuple holds the
    // values regardless of routing). Probe each index by ANN to prove per-(table, column) routing
    // actually keeps the two vectors disjoint at the index level. Both columns deliberately share
    // dimension 4; keying storage by (table, dimension) instead of (table, column) would return the
    // same top row for both probes below, but the correct answers are different rows.
    let id_idx = {
        let r = db
            .execute(
                "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(text_probe))]),
            )
            .expect("text ANN");
        let i = r.columns.iter().position(|c| c == "id").unwrap();
        assert_eq!(
            r.rows[0][i],
            Value::Uuid(text_wins_id),
            "vector_text ANN must route to the vector_text column even when vector_vision has the same dimension"
        );
        i
    };
    let r = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vision_probe))]),
        )
        .expect("vision ANN");
    assert_eq!(
        r.rows[0][id_idx],
        Value::Uuid(vision_wins_id),
        "vector_vision ANN must route to the vector_vision column, not a shared same-dimension store"
    );

    let cs = db.changes_since(pre_insert_lsn);
    assert_eq!(
        cs.vectors.len(),
        4,
        "two rows × two populated vector columns must emit four VectorChanges; got {}",
        cs.vectors.len()
    );
    let indexes: Vec<VectorIndexRef> = cs.vectors.iter().map(|c| c.index.clone()).collect();
    let text_envelopes = indexes
        .iter()
        .filter(|idx| **idx == VectorIndexRef::new("evidence", "vector_text"))
        .count();
    let vision_envelopes = indexes
        .iter()
        .filter(|idx| **idx == VectorIndexRef::new("evidence", "vector_vision"))
        .count();
    assert_eq!(
        text_envelopes, 2,
        "VectorChanges must identify vector_text once per row; got {:?}",
        indexes
    );
    assert_eq!(
        vision_envelopes, 2,
        "VectorChanges must identify vector_vision once per row; got {:?}",
        indexes
    );
    let text_payloads: Vec<Vec<f32>> = cs
        .vectors
        .iter()
        .filter(|change| change.index == VectorIndexRef::new("evidence", "vector_text"))
        .map(|change| change.vector.clone())
        .collect();
    let vision_payloads: Vec<Vec<f32>> = cs
        .vectors
        .iter()
        .filter(|change| change.index == VectorIndexRef::new("evidence", "vector_vision"))
        .map(|change| change.vector.clone())
        .collect();
    assert!(
        text_payloads.contains(&text_wins_text) && text_payloads.contains(&vision_wins_text),
        "live vector_text changes must carry vector_text payloads for both rows; got {text_payloads:?}"
    );
    assert!(
        vision_payloads.contains(&text_wins_vision)
            && vision_payloads.contains(&vision_wins_vision),
        "live vector_vision changes must carry vector_vision payloads for both rows; got {vision_payloads:?}"
    );

    let mut envelope_only = cs.clone();
    for row in &mut envelope_only.rows {
        row.values.remove("vector_text");
        row.values.remove("vector_vision");
    }
    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (
                id UUID PRIMARY KEY,
                vector_text VECTOR(4),
                vector_vision VECTOR(4)
            )",
            &empty_params(),
        )
        .expect("receiver create evidence");
    receiver
        .apply_changes(
            envelope_only,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("receiver applies generated vector-envelope-only live delta");
    let receiver_text = receiver
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        )
        .expect("receiver text ANN");
    let receiver_id_idx = receiver_text
        .columns
        .iter()
        .position(|c| c == "id")
        .unwrap();
    assert_eq!(
        receiver_text.rows[0][receiver_id_idx],
        Value::Uuid(text_wins_id),
        "receiver must materialize generated vector_text payloads from VectorChange envelopes"
    );
    let receiver_vision = receiver
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        )
        .expect("receiver vision ANN");
    assert_eq!(
        receiver_vision.rows[0][receiver_id_idx],
        Value::Uuid(vision_wins_id),
        "receiver must materialize generated vector_vision payloads from VectorChange envelopes"
    );
}

#[test]
fn nv03_update_one_column_invalidates_only_that_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let id = Uuid::new_v4();
    let original_text = vec![1.0_f32, 0.0, 0.0, 0.0];
    let original_vision = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    // other_text is strictly closer to original_text than new_text=[0,0,1,0] (orthogonal, sim 0).
    // Without HNSW invalidation, probing original_text returns id at top-1 (stale entry); with
    // invalidation, the new vector at [0,0,1,0] is still orthogonal to the probe, so other_id wins
    // deterministically. Avoids equidistant-tiebreak flake on a correct implementation.
    let other_text = vec![0.9_f32, 0.1, 0.0, 0.0];
    let other_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(original_text.clone())),
            ("v", Value::Vector(original_vision.clone())),
        ]),
    )
    .expect("insert primary");
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(other_id)),
            ("t", Value::Vector(other_text.clone())),
            ("v", Value::Vector(vec![0.0_f32; 8])),
        ]),
    )
    .expect("insert other");
    let original_row_id = db
        .scan("evidence", db.snapshot())
        .expect("scan evidence")
        .into_iter()
        .find(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .expect("original row exists")
        .row_id;

    let new_text = vec![0.0_f32, 0.0, 1.0, 0.0];
    let pre_update_lsn = db.current_lsn();
    db.execute(
        "UPDATE evidence SET vector_text = $t WHERE id = $id",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(new_text.clone())),
        ]),
    )
    .expect("update text only");

    // Sync stream must show ONLY the column that was actually rewritten. A wrong impl that re-fires
    // every vector index on UPDATE (re-writing untouched columns with their current values) is
    // observationally a no-op for projection but produces an extra envelope here.
    let cs = db.changes_since(pre_update_lsn);
    let written_columns: Vec<&str> = cs.vectors.iter().map(|c| c.index.column.as_str()).collect();
    assert_eq!(
        written_columns,
        vec!["vector_text"],
        "UPDATE that touched only vector_text must emit one VectorChange for vector_text — \
                rewriting every column with its current values is a wrong impl; got {:?}",
        written_columns
    );

    // The new vector for `id` wins ANN against itself: assert the updated row is top-1.
    let new_text_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(new_text.clone()))]),
        )
        .expect("text search after update");
    let id_idx = new_text_hits
        .columns
        .iter()
        .position(|c| c == "id")
        .unwrap();
    assert_eq!(new_text_hits.rows.len(), 1);
    assert_eq!(new_text_hits.rows[0][id_idx], Value::Uuid(id));

    // The old vector for `id` no longer wins ANN against itself: top-1 must be the OTHER row,
    // which still carries the closer vector to original_text in this layout.
    let stale_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(original_text.clone()))]),
        )
        .expect("text search against stale");
    assert_eq!(stale_hits.rows.len(), 1);
    assert_ne!(
        stale_hits.rows[0][id_idx],
        Value::Uuid(id),
        "stale-vector probe must NOT return the updated row at top-1; HNSW must drop the old entry"
    );
    let raw_stale_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &original_text,
            10,
            None,
            db.snapshot(),
        )
        .expect("raw vector search against stale vector");
    assert!(
        !raw_stale_hits
            .iter()
            .any(|(rid, sim)| *rid == original_row_id && *sim > 0.99),
        "raw vector index still exposes the old exact vector for row {original_row_id:?}; hits={raw_stale_hits:?}"
    );

    // Vision column untouched: ANN against original_vision still returns `id` and the projection still has original_vision.
    let vision_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(original_vision.clone()))]),
        )
        .expect("vision search");
    assert_eq!(vision_hits.rows[0][id_idx], Value::Uuid(id));

    let projection = db
        .execute(
            "SELECT vector_vision FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect("read vision");
    let vision_idx = projection
        .columns
        .iter()
        .position(|c| c == "vector_vision")
        .unwrap();
    assert_eq!(
        projection.rows[0][vision_idx],
        Value::Vector(original_vision)
    );
}

#[test]
fn nv04_delete_drops_row_from_every_vector_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let id = Uuid::new_v4();
    let text_vec = vec![1.0_f32, 0.0, 0.0, 0.0];
    let vision_vec = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(text_vec.clone())),
            ("v", Value::Vector(vision_vec.clone())),
        ]),
    )
    .expect("insert");

    let pre_delete_lsn = db.current_lsn();
    db.execute(
        "DELETE FROM evidence WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("delete");

    // Both indexes must drop the row.
    let text_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(text_vec.clone()))]),
        )
        .expect("text search");
    assert!(text_hits.rows.is_empty());
    let vision_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(vision_vec.clone()))]),
        )
        .expect("vision search");
    assert!(vision_hits.rows.is_empty());
    let sender_snap = db.snapshot();
    let raw_text_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &text_vec,
            5,
            None,
            sender_snap,
        )
        .expect("raw sender text index after delete");
    let raw_vision_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &vision_vec,
            5,
            None,
            sender_snap,
        )
        .expect("raw sender vision index after delete");
    assert!(
        raw_text_hits.is_empty() && raw_vision_hits.is_empty(),
        "local DELETE must tombstone raw sender vector entries, not just hide stale row ids in SQL ANN; got text={raw_text_hits:?}, vision={raw_vision_hits:?}"
    );

    // changes_since(pre_delete_lsn) returns only the DELETE entries by construction (pre_delete_lsn
    // was captured AFTER INSERT). The contract: one VectorChange per (row_id, index) pair, all at the
    // same TxId/lsn. With a single deleted row × two indexes, expect exactly two envelopes.
    let cs: ChangeSet = db.changes_since(pre_delete_lsn);
    assert_eq!(
        cs.vectors.len(),
        2,
        "DELETE of one row must emit one VectorChange per registered (table, column); got {}",
        cs.vectors.len()
    );
    let by_index: std::collections::HashMap<&VectorIndexRef, &VectorChange> =
        cs.vectors.iter().map(|c| (&c.index, c)).collect();
    let text_change = by_index
        .get(&VectorIndexRef::new("evidence", "vector_text"))
        .expect("delete envelope for vector_text");
    let vision_change = by_index
        .get(&VectorIndexRef::new("evidence", "vector_vision"))
        .expect("delete envelope for vector_vision");
    assert_eq!(
        text_change.lsn, vision_change.lsn,
        "both indexes must record the DELETE at the same lsn (same TxId)"
    );
    // Both envelopes must reference the SAME row_id — the deleted row's row_id, not RowId(0)
    // defaults. This proves the per-(row, index) envelope contract, not a per-statement aggregate.
    assert_eq!(
        text_change.row_id, vision_change.row_id,
        "both envelopes for the same deleted row must share its row_id; got {} vs {}",
        text_change.row_id.0, vision_change.row_id.0
    );
    assert_ne!(
        text_change.row_id,
        RowId(0),
        "envelope row_id must be the deleted row's row_id, not a default RowId(0)"
    );

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (
                id UUID PRIMARY KEY,
                vector_text VECTOR(4),
                vector_vision VECTOR(8)
            )",
            &empty_params(),
        )
        .expect("receiver create");
    receiver
        .execute(
            "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("t", Value::Vector(text_vec.clone())),
                ("v", Value::Vector(vision_vec.clone())),
            ]),
        )
        .expect("receiver seed");
    receiver
        .apply_changes(
            cs,
            &contextdb_engine::sync_types::ConflictPolicies::uniform(
                contextdb_engine::sync_types::ConflictPolicy::ServerWins,
            ),
        )
        .expect("receiver applies delete changeset");
    let receiver_snap = receiver.snapshot();
    let receiver_text = receiver
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &text_vec,
            5,
            None,
            receiver_snap,
        )
        .expect("receiver text index after delete");
    let receiver_vision = receiver
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &vision_vec,
            5,
            None,
            receiver_snap,
        )
        .expect("receiver vision index after delete");
    assert!(
        receiver_text.is_empty() && receiver_vision.is_empty(),
        "applying DELETE VectorChanges must remove vectors on the receiver; got text={receiver_text:?}, vision={receiver_vision:?}"
    );
}

#[test]
fn nv04b_multi_row_delete_emits_per_row_per_index_envelopes_under_one_lsn() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            category TEXT,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    let row_ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    for id in &row_ids {
        db.execute(
            "INSERT INTO evidence (id, category, vector_text, vector_vision) VALUES ($id, $c, $t, $v)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("c", Value::Text("X".into())),
                ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
                ("v", Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
            ]),
        )
        .expect("insert");
    }

    let pre = db.current_lsn();
    db.execute(
        "DELETE FROM evidence WHERE category = $c",
        &params(vec![("c", Value::Text("X".into()))]),
    )
    .expect("multi-row delete");
    let sender_snap = db.snapshot();
    let raw_text_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            10,
            None,
            sender_snap,
        )
        .expect("raw sender text index after multi-row delete");
    let raw_vision_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            10,
            None,
            sender_snap,
        )
        .expect("raw sender vision index after multi-row delete");
    assert!(
        raw_text_hits.is_empty() && raw_vision_hits.is_empty(),
        "multi-row DELETE must tombstone raw sender vector entries for every index; got text={raw_text_hits:?}, vision={raw_vision_hits:?}"
    );

    let cs = db.changes_since(pre);
    assert_eq!(
        cs.vectors.len(),
        6,
        "3 rows × 2 indexes must emit 6 VectorChanges; got {}",
        cs.vectors.len()
    );
    let lsns: std::collections::HashSet<_> = cs.vectors.iter().map(|c| c.lsn).collect();
    assert_eq!(
        lsns.len(),
        1,
        "all envelopes from one multi-row DELETE must share one TxId/lsn; got {} distinct",
        lsns.len()
    );
    let text_count = cs
        .vectors
        .iter()
        .filter(|c| c.index.column == "vector_text")
        .count();
    let vision_count = cs
        .vectors
        .iter()
        .filter(|c| c.index.column == "vector_vision")
        .count();
    assert_eq!(text_count, 3, "vector_text must drop all 3 rows");
    assert_eq!(vision_count, 3, "vector_vision must drop all 3 rows");
    let pairs: std::collections::HashSet<_> = cs
        .vectors
        .iter()
        .map(|c| (c.row_id, c.index.column.clone()))
        .collect();
    assert_eq!(
        pairs.len(),
        6,
        "multi-row DELETE must emit exactly one envelope for every (row_id, index); got {pairs:?}"
    );
    let row_ids_by_envelope: std::collections::HashSet<_> =
        cs.vectors.iter().map(|c| c.row_id).collect();
    assert_eq!(
        row_ids_by_envelope.len(),
        3,
        "multi-row DELETE must cover all three deleted row ids, not duplicate one row; got {row_ids_by_envelope:?}"
    );
}

#[test]
fn nv06_same_column_name_two_tables_stays_disjoint() {
    let db = Database::open_memory();
    // DIFFERENT dimensions on the two tables' same-named column. A global vector store cannot hold
    // both — it would dim-mismatch on the second insert. A "global ANN + post-filter by FROM table"
    // wrong impl also fails because the post-filter cannot reconcile cross-dim probes. Only true
    // per-(table, column) routing passes.
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty_params(),
    )
    .expect("create evidence");
    db.execute(
        "CREATE TABLE digests (id UUID PRIMARY KEY, vector_text VECTOR(8))",
        &empty_params(),
    )
    .expect("create digests");

    let evidence_id = Uuid::new_v4();
    let digest_id = Uuid::new_v4();
    let evidence_probe = vec![1.0_f32, 0.0, 0.0, 0.0];
    let digest_probe = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

    db.execute(
        "INSERT INTO evidence (id, vector_text) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(evidence_id)),
            ("v", Value::Vector(evidence_probe.clone())),
        ]),
    )
    .expect("insert evidence");
    let digest_insert = db.execute(
        "INSERT INTO digests (id, vector_text) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(digest_id)),
            ("v", Value::Vector(digest_probe.clone())),
        ]),
    );
    assert!(
        digest_insert.is_ok(),
        "same column name on different tables must accept independent dimensions; got {:?}",
        digest_insert.as_ref().err()
    );

    let evidence_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(evidence_probe))]),
        )
        .expect("evidence search");
    let id_idx = evidence_hits
        .columns
        .iter()
        .position(|c| c == "id")
        .unwrap();
    assert_eq!(
        evidence_hits
            .rows
            .iter()
            .map(|r| r[id_idx].clone())
            .collect::<Vec<_>>(),
        vec![Value::Uuid(evidence_id)]
    );

    let digest_hits = db
        .execute(
            "SELECT id FROM digests ORDER BY vector_text <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(digest_probe))]),
        )
        .expect("digest search");
    assert_eq!(
        digest_hits
            .rows
            .iter()
            .map(|r| r[id_idx].clone())
            .collect::<Vec<_>>(),
        vec![Value::Uuid(digest_id)]
    );
}

#[test]
fn nv08_snapshot_round_trip_rebuilds_independent_indexes_per_table_column() {
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("snapshot.db");
    let id = Uuid::new_v4();
    let text_vec = vec![1.0_f32, 0.0, 0.0, 0.0];
    let vision_vec = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    {
        let db = Database::open(&db_path).expect("open file db");
        db.execute(
            "CREATE TABLE evidence (
                id UUID PRIMARY KEY,
                vector_text VECTOR(4),
                vector_vision VECTOR(8)
            )",
            &empty_params(),
        )
        .expect("create evidence");
        db.execute(
            "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("t", Value::Vector(text_vec.clone())),
                ("v", Value::Vector(vision_vec.clone())),
            ]),
        )
        .expect("insert");
        drop(db);
    }

    let reopened = Database::open(&db_path).expect("reopen");
    let snapshot = reopened.changes_since(Lsn(0));
    let snapshot_indexes: Vec<VectorIndexRef> = snapshot
        .vectors
        .iter()
        .map(|change| change.index.clone())
        .collect();
    assert_eq!(
        snapshot_indexes,
        vec![
            VectorIndexRef::new("evidence", "vector_text"),
            VectorIndexRef::new("evidence", "vector_vision"),
        ],
        "a real post-restart changes_since(0) snapshot must emit one vector change per declared vector column with full identity; got {snapshot_indexes:?}"
    );
    let snapshot_by_index: HashMap<VectorIndexRef, &VectorChange> = snapshot
        .vectors
        .iter()
        .map(|change| (change.index.clone(), change))
        .collect();
    let text_snapshot = snapshot_by_index
        .get(&VectorIndexRef::new("evidence", "vector_text"))
        .copied()
        .expect("snapshot vector_text change");
    let vision_snapshot = snapshot_by_index
        .get(&VectorIndexRef::new("evidence", "vector_vision"))
        .copied()
        .expect("snapshot vector_vision change");
    assert_eq!(
        &text_snapshot.vector, &text_vec,
        "changes_since(0) must preserve the vector_text payload, not reuse another vector column"
    );
    assert_eq!(
        &vision_snapshot.vector, &vision_vec,
        "changes_since(0) must preserve the vector_vision payload, not reuse vector_text or a stale value"
    );
    assert_eq!(
        text_snapshot.row_id, vision_snapshot.row_id,
        "snapshot vector changes for the same row must carry the same source row_id"
    );
    assert_ne!(
        text_snapshot.row_id,
        RowId(0),
        "snapshot vector changes must carry the real source row_id, not a default row id"
    );

    let assert_receiver_searches = |receiver: &Database, label: &str| {
        let text_hits = receiver
            .execute(
                "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(text_vec.clone()))]),
            )
            .expect("receiver text search");
        let id_idx = text_hits
            .columns
            .iter()
            .position(|c| c == "id")
            .expect("receiver id column");
        assert_eq!(text_hits.rows.len(), 1, "{label}: text receiver hits");
        assert_eq!(
            text_hits.rows[0][id_idx],
            Value::Uuid(id),
            "{label}: text search must return the row from the generated snapshot"
        );

        let vision_hits = receiver
            .execute(
                "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(vision_vec.clone()))]),
            )
            .expect("receiver vision search");
        assert_eq!(vision_hits.rows.len(), 1, "{label}: vision receiver hits");
        assert_eq!(
            vision_hits.rows[0][id_idx],
            Value::Uuid(id),
            "{label}: vision search must return the row from the generated snapshot"
        );
    };

    let exact_receiver = Database::open_memory();
    exact_receiver
        .apply_changes(
            snapshot.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("fresh receiver applies generated multi-vector snapshot");
    assert_receiver_searches(&exact_receiver, "exact snapshot apply");

    let mut envelope_snapshot = snapshot.clone();
    for row in &mut envelope_snapshot.rows {
        row.values.remove("vector_text");
        row.values.remove("vector_vision");
    }
    let envelope_receiver = Database::open_memory();
    envelope_receiver
        .apply_changes(
            envelope_snapshot,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("fresh receiver applies generated vector envelopes without row vector values");
    assert_receiver_searches(&envelope_receiver, "vector-envelope-only snapshot apply");

    let text_hits = reopened
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(text_vec.clone()))]),
        )
        .expect("text search after reopen");
    let id_idx = text_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(text_hits.rows.len(), 1);
    assert_eq!(
        text_hits.rows[0][id_idx],
        Value::Uuid(id),
        "text search after reopen must return the same row id the writer inserted"
    );

    let vision_hits = reopened
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vision_vec.clone()))]),
        )
        .expect("vision search after reopen");
    assert_eq!(vision_hits.rows.len(), 1);
    assert_eq!(
        vision_hits.rows[0][id_idx],
        Value::Uuid(id),
        "vision search after reopen must return the same row id the writer inserted"
    );

    let mismatched = reopened.execute(
        "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
        &params(vec![("q", Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]),
    );
    assert!(matches!(
        mismatched,
        Err(contextdb_core::Error::VectorIndexDimensionMismatch { ref index, expected: 8, actual: 4 })
            if *index == VectorIndexRef::new("evidence", "vector_vision")
    ));
}

#[test]
fn nv13_multi_vector_insert_partial_failure_rolls_back_atomically() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let id = Uuid::new_v4();
    let result = db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(vec![0.0_f32; 4])), // correct
            ("v", Value::Vector(vec![0.0_f32; 7])), // wrong: vector_vision is dim 8
        ]),
    );
    assert!(matches!(
        result,
        Err(Error::VectorIndexDimensionMismatch { ref index, expected: 8, actual: 7 })
            if *index == VectorIndexRef::new("evidence", "vector_vision")
    ));

    // Relational row absent.
    let count = db
        .execute("SELECT id FROM evidence", &empty_params())
        .expect("select")
        .rows
        .len();
    assert_eq!(count, 0, "relational row must roll back");

    // The dim-correct vector is also absent: ANN search returns nothing.
    let hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![0.0_f32; 4]))]),
        )
        .expect("text search");
    assert!(
        hits.rows.is_empty(),
        "vector_text must have rolled back too"
    );
    let raw_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[0.0_f32; 4],
            5,
            None,
            db.snapshot(),
        )
        .expect("raw text search");
    assert!(
        raw_hits.is_empty(),
        "raw vector_text entries must also roll back; got {raw_hits:?}"
    );
}

#[test]
fn nv13b_multi_vector_update_partial_failure_rolls_back_atomically() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    let id = Uuid::new_v4();
    let original_text = vec![1.0_f32, 0.0, 0.0, 0.0];
    let original_vision = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(original_text.clone())),
            ("v", Value::Vector(original_vision.clone())),
        ]),
    )
    .expect("insert");

    let original_row_id = db
        .scan("evidence", db.snapshot())
        .expect("scan evidence")
        .into_iter()
        .find(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .expect("inserted row exists")
        .row_id;

    // Wrong-dim UPDATE on vector_vision; vector_text would be valid if processed first.
    let new_text = vec![0.0_f32, 1.0, 0.0, 0.0];
    let pre_update_lsn = db.current_lsn();
    let result = db.execute(
        "UPDATE evidence SET vector_text = $t, vector_vision = $v WHERE id = $id",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(new_text.clone())),
            ("v", Value::Vector(vec![0.0_f32; 7])), // wrong dim
        ]),
    );
    assert!(matches!(
        result,
        Err(Error::VectorIndexDimensionMismatch { ref index, expected: 8, actual: 7 })
            if *index == VectorIndexRef::new("evidence", "vector_vision")
    ));

    // Both columns retain their ORIGINAL values; neither was updated.
    let projection = db
        .execute(
            "SELECT vector_text, vector_vision FROM evidence WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect("read");
    let text_idx = projection
        .columns
        .iter()
        .position(|c| c == "vector_text")
        .unwrap();
    let vision_idx = projection
        .columns
        .iter()
        .position(|c| c == "vector_vision")
        .unwrap();
    assert_eq!(
        projection.rows[0][text_idx],
        Value::Vector(original_text),
        "vector_text must NOT be updated when vector_vision validation fails"
    );
    assert_eq!(
        projection.rows[0][vision_idx],
        Value::Vector(original_vision.clone()),
        "vector_vision must remain at its original value"
    );

    // ANN against original vector_text still returns the row at top-1 — HNSW for vector_text was not invalidated.
    let hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]),
        )
        .expect("text search");
    let id_idx = hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(hits.rows[0][id_idx], Value::Uuid(id));
    let raw_new_text_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &new_text,
            10,
            None,
            db.snapshot(),
        )
        .expect("raw new text search");
    assert!(
        !raw_new_text_hits
            .iter()
            .any(|(rid, similarity)| *rid == original_row_id && *similarity > 0.99),
        "failed UPDATE must not leave raw vector_text entry for the attempted new value; got {raw_new_text_hits:?}"
    );
    let cs = db.changes_since(pre_update_lsn);
    assert!(
        cs.vectors.is_empty(),
        "failed UPDATE must not emit vector changes; got {:?}",
        cs.vectors
    );
}

#[test]
fn nv02b_null_vector_column_produces_no_index_entry() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            kind TEXT,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    // The cg evidence shape: a Text-kind row populates vector_text only; vector_vision is NULL.
    let text_row = Uuid::new_v4();
    db.execute(
        "INSERT INTO evidence (id, kind, vector_text) VALUES ($id, $k, $t)",
        &params(vec![
            ("id", Value::Uuid(text_row)),
            ("k", Value::Text("Text".into())),
            ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert text-only row");

    // A Vision-kind row populates vector_vision only; vector_text is NULL.
    let vision_row = Uuid::new_v4();
    db.execute(
        "INSERT INTO evidence (id, kind, vector_vision) VALUES ($id, $k, $v)",
        &params(vec![
            ("id", Value::Uuid(vision_row)),
            ("k", Value::Text("Vision".into())),
            (
                "v",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
        ]),
    )
    .expect("insert vision-only row");

    // ANN against vector_text returns ONLY the text-only row — the vision-only row's NULL vector_text
    // must NOT appear in the index (no zero-padding, no empty entry).
    let text_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        )
        .expect("text search");
    let id_idx = text_hits.columns.iter().position(|c| c == "id").unwrap();
    let returned: Vec<Value> = text_hits.rows.iter().map(|r| r[id_idx].clone()).collect();
    assert_eq!(
        returned,
        vec![Value::Uuid(text_row)],
        "vector_text index must contain only the row that populated it; NULL columns produce no entry"
    );

    // Symmetric for vector_vision.
    let vision_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 5",
            &params(vec![(
                "q",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            )]),
        )
        .expect("vision search");
    let returned: Vec<Value> = vision_hits.rows.iter().map(|r| r[id_idx].clone()).collect();
    assert_eq!(returned, vec![Value::Uuid(vision_row)]);

    // The emitted changeset for the text row's INSERT carries exactly ONE VectorChange (for vector_text),
    // not two — the NULL column emits no envelope.
    let text_only_lsn = {
        let pre = db.current_lsn();
        let id = Uuid::new_v4();
        db.execute(
            "INSERT INTO evidence (id, kind, vector_text) VALUES ($id, $k, $t)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("k", Value::Text("Text".into())),
                ("t", Value::Vector(vec![0.5_f32, 0.5, 0.0, 0.0])),
            ]),
        )
        .expect("third insert");
        pre
    };
    let cs = db.changes_since(text_only_lsn);
    assert_eq!(
        cs.vectors.len(),
        1,
        "INSERT with one populated vector column emits one envelope, not two; got {}",
        cs.vectors.len()
    );
    assert_eq!(
        cs.vectors[0].index,
        VectorIndexRef::new("evidence", "vector_text")
    );
}

#[test]
fn nv05b_vector_order_by_preserves_full_row_projection() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            category TEXT,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    let id = Uuid::new_v4();
    let text_vec = vec![1.0_f32, 0.0, 0.0, 0.0];
    let vision_vec = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let pre_insert_lsn = db.current_lsn();
    db.execute(
        "INSERT INTO evidence (id, category, vector_text, vector_vision) VALUES ($id, $c, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("c", Value::Text("Text".into())),
            ("t", Value::Vector(text_vec.clone())),
            ("v", Value::Vector(vision_vec.clone())),
        ]),
    )
    .expect("insert");

    let cs = db.changes_since(pre_insert_lsn);
    let indexes: Vec<VectorIndexRef> = cs.vectors.iter().map(|c| c.index.clone()).collect();
    assert!(
        indexes.contains(&VectorIndexRef::new("evidence", "vector_text"))
            && indexes.contains(&VectorIndexRef::new("evidence", "vector_vision")),
        "full-row vector insert must emit envelopes for every populated vector column; got {:?}",
        indexes
    );

    let result = db
        .execute(
            "SELECT * FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(text_vec.clone()))]),
        )
        .expect("select * with vector ORDER BY");

    // All four declared columns must be in the projection.
    for col in &["id", "category", "vector_text", "vector_vision"] {
        assert!(
            result.columns.iter().any(|c| c == col),
            "SELECT * with ORDER BY <vec> must project `{col}`; got columns {:?}",
            result.columns
        );
    }
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    let id_idx = result.columns.iter().position(|c| c == "id").unwrap();
    let cat_idx = result.columns.iter().position(|c| c == "category").unwrap();
    let text_idx = result
        .columns
        .iter()
        .position(|c| c == "vector_text")
        .unwrap();
    let vision_idx = result
        .columns
        .iter()
        .position(|c| c == "vector_vision")
        .unwrap();
    assert_eq!(row[id_idx], Value::Uuid(id));
    assert_eq!(row[cat_idx], Value::Text("Text".into()));
    assert_eq!(row[text_idx], Value::Vector(text_vec));
    assert_eq!(row[vision_idx], Value::Vector(vision_vec));
}

#[test]
fn nv14_snapshot_isolation_across_two_indexes() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    // Reader takes a snapshot before any write.
    let pre_snap = db.snapshot();

    // Writer commits a multi-vector row in another thread, returning the assigned row_id so the test
    // can pin identity in post-snap assertions. Direct engine API (insert_row + insert_vector) gives us
    // the row_id without depending on `SELECT row_id` (which is stripped from user-facing QueryResult).
    let writer_db = Arc::clone(&db);
    let id = Uuid::new_v4();
    let writer = thread::spawn(move || -> Result<RowId, Error> {
        let tx = writer_db.begin();
        let row_id = writer_db.insert_row(tx, "evidence", values(vec![("id", Value::Uuid(id))]))?;
        writer_db.insert_vector(
            tx,
            VectorIndexRef::new("evidence", "vector_text"),
            row_id,
            vec![1.0_f32, 0.0, 0.0, 0.0],
        )?;
        writer_db.insert_vector(
            tx,
            VectorIndexRef::new("evidence", "vector_vision"),
            row_id,
            vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        )?;
        writer_db.commit(tx)?;
        Ok(row_id)
    });
    let writer_result = writer.join().expect("writer thread");
    assert!(
        writer_result.is_ok(),
        "writer must commit a row with both vector indexes atomically; got {:?}",
        writer_result.as_ref().err()
    );
    let written_row_id = writer_result.unwrap();

    // The reader at pre-snap must NOT see the row in either index, even though the writer committed.
    let text_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            pre_snap,
        )
        .expect("text search at pre-snap");
    assert!(
        text_hits.is_empty(),
        "snapshot isolation must hide vector_text from pre-snap reader"
    );

    let vision_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            5,
            None,
            pre_snap,
        )
        .expect("vision search at pre-snap");
    assert!(
        vision_hits.is_empty(),
        "snapshot isolation must hide vector_vision from pre-snap reader"
    );

    // A fresh snapshot sees both vectors AT THE WRITER'S row_id — not RowId(0), not some spurious entry.
    let post_snap = db.snapshot();
    let text_post = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            post_snap,
        )
        .expect("text search post");
    assert_eq!(text_post.len(), 1);
    assert_eq!(
        text_post[0].0, written_row_id,
        "post-snap reader must see the writer's row, not a default/spurious row_id"
    );
    let vision_post = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            5,
            None,
            post_snap,
        )
        .expect("vision search post");
    assert_eq!(vision_post[0].0, written_row_id);
}

fn values(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

#[test]
fn nv14b_three_way_unified_transaction_atomicity() {
    use contextdb_core::Direction;

    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY)",
        &empty_params(),
    )
    .expect("create entities");
    // Pre-snap reader takes a snapshot before any writes.
    let pre_snap = db.snapshot();

    // One transaction: insert entity + evidence row + its vector + an outgoing graph edge.
    // Use direct engine API (insert_row + insert_vector + insert_edge) so we capture row_ids for
    // assertions and so the graph edge participates in the same TxId as the relational + vector
    // writes — that's what makes this a "unified-tx" test.
    let entity_id = Uuid::new_v4();
    let evidence_id = Uuid::new_v4();
    let tx = db.begin();
    let _entity_row = db
        .insert_row(tx, "entities", values(vec![("id", Value::Uuid(entity_id))]))
        .expect("insert entity");
    let evidence_row = db
        .insert_row(
            tx,
            "evidence",
            values(vec![("id", Value::Uuid(evidence_id))]),
        )
        .expect("insert evidence");
    db.insert_vector(
        tx,
        VectorIndexRef::new("evidence", "vector_text"),
        evidence_row,
        vec![1.0_f32, 0.0, 0.0, 0.0],
    )
    .expect("insert text vector");
    let vision_insert = db.insert_vector(
        tx,
        VectorIndexRef::new("evidence", "vector_vision"),
        evidence_row,
        vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    );
    assert!(
        vision_insert.is_ok(),
        "one unified transaction must accept vectors for every registered index; got {:?}",
        vision_insert.as_ref().err()
    );
    // Database::insert_edge signature: (tx: TxId, source: NodeId, target: NodeId, edge_type: String,
    // properties: HashMap<String, Value>) -> Result<bool>.
    db.insert_edge(
        tx,
        entity_id,
        evidence_id,
        "evidences".to_string(),
        HashMap::new(),
    )
    .expect("insert edge");
    db.commit(tx).expect("commit");

    let post_snap = db.snapshot();

    // Pre-snap reader sees neither side: no vector entry, no graph edge. (Relational SQL reads
    // always run at current-time snapshot, so we use the engine's snapshot-scoped APIs directly.)
    let vector_pre = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            pre_snap,
        )
        .expect("vector at pre_snap");
    assert!(
        vector_pre.is_empty(),
        "pre-snap reader must not see the vector"
    );

    let bfs_pre = db
        .query_bfs(entity_id, None, Direction::Outgoing, 2, pre_snap)
        .expect("bfs at pre_snap");
    assert!(
        bfs_pre.nodes.iter().all(|n| n.id != evidence_id),
        "pre-snap reader must not see the graph edge to evidence"
    );

    // Post-snap reader sees all three: row, vector, graph edge.
    let vector_post = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            post_snap,
        )
        .expect("vector at post_snap");
    assert!(
        !vector_post.is_empty(),
        "post-snap reader must see the vector"
    );
    // Identity check: the returned row_id must correspond to the writer's evidence row.
    assert_eq!(
        vector_post[0].0, evidence_row,
        "post-snap reader's top-1 hit must be the writer's evidence row; got {:?}",
        vector_post[0].0
    );
    let vision_post = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            5,
            None,
            post_snap,
        )
        .expect("vision at post_snap");
    assert_eq!(
        vision_post[0].0, evidence_row,
        "post-snap reader's vision hit must be the writer's evidence row"
    );

    let bfs_post = db
        .query_bfs(entity_id, None, Direction::Outgoing, 2, post_snap)
        .expect("bfs at post_snap");
    assert!(
        bfs_post.nodes.iter().any(|n| n.id == evidence_id),
        "post-snap reader must see the graph edge to evidence"
    );

    // Sanity: the SQL-current relational read sees the row (current snapshot is post-write).
    let relational = db
        .execute("SELECT id FROM evidence", &empty_params())
        .expect("current relational read");
    assert_eq!(relational.rows.len(), 1);
}

#[test]
fn nv15_parser_rejects_invalid_quantization_and_banned_features() {
    use contextdb_core::Error;

    let db = Database::open_memory();

    // Invalid quantization values must fail at PARSE time (not later as a planner/executor error).
    let r = db.execute(
        "CREATE TABLE q1 (id UUID PRIMARY KEY, v VECTOR(4) WITH (quantization = 'NOPE'))",
        &empty_params(),
    );
    assert!(
        matches!(r, Err(Error::ParseError(_))),
        "invalid quantization value must reject at parse time; got {r:?}"
    );

    let r = db.execute(
        "CREATE TABLE q2 (id UUID PRIMARY KEY, v VECTOR(4) WITH (foo = 'SQ8'))",
        &empty_params(),
    );
    assert!(
        matches!(r, Err(Error::ParseError(_))),
        "wrong WITH key must reject at parse time; got {r:?}"
    );
    let r = db.execute(
        "CREATE TABLE q3 (id UUID PRIMARY KEY, v VECTOR(4) WITH (quantization = 'SQ8', foo = 'bar'))",
        &empty_params(),
    );
    assert!(
        matches!(r, Err(Error::ParseError(_))),
        "unknown extra WITH options must reject at parse time instead of being ignored; got {r:?}"
    );

    // Banned features must STAY rejected at parse time (not relaxed-to-later-error). Each match arm pins
    // a specific rejection variant the existing parser produces today, so a relaxation that defers the
    // ban to a downstream PlanError fails this test rather than passing it.
    assert!(matches!(
        db.execute(
            "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
            &empty_params()
        ),
        Err(Error::RecursiveCteNotSupported)
    ));
    assert!(matches!(
        db.execute("SELECT id FROM evidence GROUP BY id", &empty_params()),
        Err(Error::ParseError(ref s)) if s.contains("GROUP BY")
    ));
    assert!(matches!(
        db.execute(
            "SELECT ROW_NUMBER() OVER (PARTITION BY id) FROM evidence",
            &empty_params()
        ),
        Err(Error::WindowFunctionNotSupported)
    ));
    assert!(matches!(
        db.execute(
            "SELECT id FROM evidence WHERE name MATCH('x')",
            &empty_params()
        ),
        Err(Error::FullTextSearchNotSupported)
    ));
    assert!(matches!(
        db.execute("CREATE PROCEDURE foo() AS SELECT 1", &empty_params()),
        Err(Error::StoredProcNotSupported)
    ));
    // CASE has no grammar rule; expect a generic ParseError (not a downstream PlanError).
    assert!(matches!(
        db.execute(
            "SELECT CASE WHEN id IS NULL THEN 1 ELSE 2 END FROM evidence",
            &empty_params()
        ),
        Err(Error::ParseError(_))
    ));

    // Valid quantization clauses parse.
    db.execute(
        "CREATE TABLE qok (id UUID PRIMARY KEY, v VECTOR(4) WITH (quantization = 'F32'))",
        &empty_params(),
    )
    .expect("F32 parses");
    db.execute(
        "CREATE TABLE qsq8 (id UUID PRIMARY KEY, v VECTOR(4) WITH (quantization = 'SQ8'))",
        &empty_params(),
    )
    .expect("SQ8 parses");
    db.execute(
        "CREATE TABLE qsq4 (id UUID PRIMARY KEY, v VECTOR(4) WITH (quantization = 'SQ4'))",
        &empty_params(),
    )
    .expect("SQ4 parses");
}

#[test]
fn nv16_empty_index_registers_at_declared_dimension() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8),
            vector_audio VECTOR(16)
        )",
        &empty_params(),
    )
    .expect("create three-vector evidence");

    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show vector indexes");
    let table_idx = indexes.columns.iter().position(|c| c == "table").unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let dimension_idx = indexes
        .columns
        .iter()
        .position(|c| c == "dimension")
        .unwrap();
    let count_idx = indexes
        .columns
        .iter()
        .position(|c| c == "vector_count")
        .unwrap();
    let find_index = |column: &str| {
        indexes.rows.iter().find(|row| {
            matches!(&row[table_idx], Value::Text(table) if table == "evidence")
                && matches!(&row[column_idx], Value::Text(found) if found == column)
        })
    };
    for (column, dimension) in [
        ("vector_text", 4_i64),
        ("vector_vision", 8_i64),
        ("vector_audio", 16_i64),
    ] {
        let row = find_index(column)
            .unwrap_or_else(|| panic!("SHOW VECTOR_INDEXES must list empty evidence.{column}"));
        assert_eq!(
            row[dimension_idx],
            Value::Int64(dimension),
            "SHOW VECTOR_INDEXES must preserve declared dimension for empty evidence.{column}"
        );
        assert_eq!(
            row[count_idx],
            Value::Int64(0),
            "fresh empty evidence.{column} must report vector_count=0"
        );
    }

    // No rows inserted. Searching the empty audio index must NOT return UnknownVectorIndex —
    // the schema declared it. It returns an empty result instead.
    let result = db.execute(
        "SELECT id FROM evidence ORDER BY vector_audio <=> $q LIMIT 1",
        &params(vec![("q", Value::Vector(vec![0.0_f32; 16]))]),
    );
    match result {
        Ok(qr) => assert!(qr.rows.is_empty(), "empty index returns empty result"),
        Err(other) => panic!("empty index search must succeed with empty result, got {other:?}"),
    }

    // A wrong-dim probe must fire VectorIndexDimensionMismatch (dimension is known from schema, not data).
    let mismatched = db.execute(
        "SELECT id FROM evidence ORDER BY vector_audio <=> $q LIMIT 1",
        &params(vec![("q", Value::Vector(vec![0.0_f32; 4]))]),
    );
    assert!(matches!(
        mismatched,
        Err(contextdb_core::Error::VectorIndexDimensionMismatch { ref index, expected: 16, actual: 4 })
            if *index == VectorIndexRef::new("evidence", "vector_audio")
    ));
}

#[test]
fn nv16b_engine_surface_is_embedding_space_id_agnostic() {
    // VectorIndexRef has exactly two fields: table and column. No embedding_space_id leak.
    // Compile-time exhaustive destructure: if a future field is added, this test fails to compile,
    // forcing the boundary discussion.
    let r = VectorIndexRef::new("evidence", "vector_text");
    let VectorIndexRef { table, column } = r.clone();
    assert_eq!(table, "evidence");
    assert_eq!(column, "vector_text");

    // SHOW VECTOR_INDEXES surface columns are exactly the engine-level columns — no space_id, no
    // cg-side metadata. cg's embedding_space_bindings table joins these (table, column) pairs back
    // to embedding_space_id at the cg layer.
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty_params(),
    )
    .expect("create");
    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show");
    let allowed: std::collections::HashSet<&str> = [
        "table",
        "column",
        "dimension",
        "quantization",
        "vector_count",
        "bytes",
    ]
    .iter()
    .copied()
    .collect();
    for col in &indexes.columns {
        assert!(
            allowed.contains(col.as_str()),
            "SHOW VECTOR_INDEXES column `{col}` is not in the engine-only allowlist; embedding_space_id and cg-side metadata must NOT appear in the engine surface"
        );
    }
}

#[test]
fn nv19b_changeset_ddl_applies_before_vector_changes_for_new_index() {
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
    };

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
            &HashMap::new(),
        )
        .expect("seed");

    // ChangeSet that ADDS vector_vision via DDL AND writes a vector for the new column in one apply.
    // The receiver must apply DDL first so the index is registered before the VectorChange lands.
    let row_id = RowId(1);
    let id = Uuid::new_v4();
    let lsn = Lsn(10);
    let mut row_values: HashMap<String, Value> = HashMap::new();
    row_values.insert("id".into(), Value::Uuid(id));
    row_values.insert(
        "vector_text".into(),
        Value::Vector(vec![1.0, 0.0, 0.0, 0.0]),
    );
    // Deliberately do NOT include vector_vision in row values. The ONLY landing path for the new
    // column's vector is the VectorChange envelope. A wrong impl that orders DDL after vectors
    // (or one that relies on row-auto-population) cannot make vector_vision searchable.
    let cs = ChangeSet {
        rows: vec![RowChange {
            table: "evidence".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: Value::Uuid(id),
            },
            values: row_values, // contains only id and vector_text; vector_vision is delivered ONLY via VectorChange
            deleted: false,
            lsn,
        }],
        edges: vec![],
        vectors: vec![
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_text"),
                row_id,
                vector: vec![1.0, 0.0, 0.0, 0.0],
                lsn,
            },
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_vision"),
                row_id,
                vector: vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                lsn,
            },
        ],
        ddl: vec![DdlChange::AlterTable {
            name: "evidence".into(),
            columns: vec![("vector_vision".into(), "VECTOR(8)".into())],
            constraints: vec![],
        }],
    };
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .expect("apply must succeed: DDL precedes vectors for same column");

    // Both indexes are now registered AND populated. Because vector_vision was NOT in the row's values
    // map, the only way the vision search finds the row is if the VectorChange envelope landed —
    // which requires DDL to have applied first.
    let vision_hits = receiver.execute(
        "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
        &params(vec![(
            "q",
            Value::Vector(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        )]),
    );
    assert!(
        vision_hits.is_ok(),
        "vision search after DDL+vector apply must succeed; got {:?}",
        vision_hits.as_ref().err()
    );
    let vision_hits = vision_hits.unwrap();
    assert_eq!(vision_hits.rows.len(), 1);
    let id_idx = vision_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(
        vision_hits.rows[0][id_idx],
        Value::Uuid(id),
        "vision search must return the row whose vector arrived via VectorChange envelope; \
                a spurious row_id indicates the envelope never landed correctly"
    );
}

#[test]
fn nv20_drop_table_deregisters_vector_indexes_and_emits_ddl_change() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            (
                "v",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
        ]),
    )
    .expect("seed");
    db.execute(
        "CREATE TABLE digests (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty_params(),
    )
    .expect("create unrelated vector table");
    let digest_id = Uuid::new_v4();
    let digest_vec = vec![0.0_f32, 1.0, 0.0, 0.0];
    db.execute(
        "INSERT INTO digests (id, vector_text) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(digest_id)),
            ("v", Value::Vector(digest_vec.clone())),
        ]),
    )
    .expect("seed unrelated vector table");

    let pre = db.current_lsn();
    db.execute("DROP TABLE evidence", &empty_params())
        .expect("drop");

    // SHOW VECTOR_INDEXES no longer lists either index.
    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show");
    let table_idx = indexes.columns.iter().position(|c| c == "table").unwrap();
    let tables: Vec<&Value> = indexes.rows.iter().map(|r| &r[table_idx]).collect();
    assert!(
        !tables
            .iter()
            .any(|v| matches!(v, Value::Text(s) if s == "evidence")),
        "DROP TABLE must deregister every (table, column) on the dropped table"
    );
    assert!(
        tables
            .iter()
            .any(|v| matches!(v, Value::Text(s) if s == "digests")),
        "DROP TABLE evidence must not clear unrelated vector indexes; SHOW rows were {:?}",
        indexes.rows
    );
    let digest_hits = db
        .execute(
            "SELECT id FROM digests ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(digest_vec.clone()))]),
        )
        .expect("unrelated digest vector search after drop");
    assert!(
        !digest_hits.rows.is_empty(),
        "DROP TABLE evidence must leave digests.vector_text searchable; got no hits"
    );
    let digest_id_idx = digest_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(
        digest_hits.rows[0][digest_id_idx],
        Value::Uuid(digest_id),
        "DROP TABLE evidence must not clear payload/HNSW state for digests.vector_text"
    );

    // The emitted changeset carries a DropTable DDL change; receivers replicate the deregistration.
    let cs = db.changes_since(pre);
    let dropped = cs.ddl.iter().any(|d| {
        matches!(d,
        contextdb_engine::sync_types::DdlChange::DropTable { name } if name == "evidence")
    });
    assert!(
        dropped,
        "DROP TABLE must emit a DropTable DDL change in the sync stream"
    );

    // Recreating the table with same column names but DIFFERENT dimensions opens cleanly — no stale
    // HNSW state inherited from the dropped table.
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(16),
            vector_vision VECTOR(32)
        )",
        &empty_params(),
    )
    .expect("recreate with different dimensions must succeed");
    let recreated_id = Uuid::new_v4();
    let recreated_text = vec![1.0_f32; 16];
    let recreated_vision = vec![0.5_f32; 32];
    let dim_check = db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(recreated_id)),
            ("t", Value::Vector(recreated_text.clone())),
            ("v", Value::Vector(recreated_vision.clone())),
        ]),
    );
    assert!(
        dim_check.is_ok(),
        "recreated table must accept its newly declared dimensions, not the dropped table's"
    );
    let recreated_indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show after recreate");
    let table_idx = recreated_indexes
        .columns
        .iter()
        .position(|c| c == "table")
        .unwrap();
    let column_idx = recreated_indexes
        .columns
        .iter()
        .position(|c| c == "column")
        .unwrap();
    let dim_idx = recreated_indexes
        .columns
        .iter()
        .position(|c| c == "dimension")
        .unwrap();
    let recreated_text_row = recreated_indexes.rows.iter().find(|row| {
        matches!(&row[table_idx], Value::Text(t) if t == "evidence")
            && matches!(&row[column_idx], Value::Text(c) if c == "vector_text")
    });
    let recreated_vision_row = recreated_indexes.rows.iter().find(|row| {
        matches!(&row[table_idx], Value::Text(t) if t == "evidence")
            && matches!(&row[column_idx], Value::Text(c) if c == "vector_vision")
    });
    assert!(
        matches!(
            recreated_text_row.map(|row| &row[dim_idx]),
            Some(Value::Int64(16))
        ),
        "recreated evidence.vector_text must be registered at dimension 16; got {:?}",
        recreated_indexes.rows
    );
    assert!(
        matches!(
            recreated_vision_row.map(|row| &row[dim_idx]),
            Some(Value::Int64(32))
        ),
        "recreated evidence.vector_vision must be registered at dimension 32; got {:?}",
        recreated_indexes.rows
    );

    let text_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(recreated_text))]),
        )
        .expect("recreated text ANN search");
    assert!(
        !text_hits.rows.is_empty(),
        "recreated vector_text index must be populated and searchable; got no hits"
    );
    let id_idx = text_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(
        text_hits.rows[0][id_idx],
        Value::Uuid(recreated_id),
        "recreated vector_text index must be populated and searchable"
    );
    let vision_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(recreated_vision))]),
        )
        .expect("recreated vision ANN search");
    assert!(
        !vision_hits.rows.is_empty(),
        "recreated vector_vision index must be populated and searchable; got no hits"
    );
    let id_idx = vision_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(
        vision_hits.rows[0][id_idx],
        Value::Uuid(recreated_id),
        "recreated vector_vision index must be populated and searchable"
    );
}

#[test]
fn nv20b_alter_table_drop_column_deregisters_only_that_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    let id = Uuid::new_v4();
    let original_text = vec![1.0_f32, 0.0, 0.0, 0.0];
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(original_text.clone())),
            (
                "v",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
        ]),
    )
    .expect("seed");

    db.execute(
        "ALTER TABLE evidence DROP COLUMN vector_vision",
        &empty_params(),
    )
    .expect("drop column");

    // vector_vision is gone from SHOW VECTOR_INDEXES.
    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show");
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let cols: Vec<&Value> = indexes.rows.iter().map(|r| &r[column_idx]).collect();
    assert!(
        !cols
            .iter()
            .any(|v| matches!(v, Value::Text(s) if s == "vector_vision")),
        "DROP COLUMN must deregister vector_vision"
    );
    assert!(
        cols.iter()
            .any(|v| matches!(v, Value::Text(s) if s == "vector_text")),
        "vector_text must still be registered"
    );

    // vector_text index untouched: ANN against original_text still returns the row.
    let r = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(original_text))]),
        )
        .expect("text search");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(r.rows[0][id_idx], Value::Uuid(id));

    // Querying the now-dropped column returns UnknownVectorIndex.
    let dropped = db.execute(
        "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
        &params(vec![("q", Value::Vector(vec![0.0_f32; 8]))]),
    );
    assert!(matches!(
        dropped,
        Err(contextdb_core::Error::UnknownVectorIndex { ref index })
            if *index == VectorIndexRef::new("evidence", "vector_vision")
    ));
}

#[test]
fn nv16d_show_vector_indexes_on_empty_database_returns_empty_with_header() {
    let db = Database::open_memory();
    let r = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("SHOW VECTOR_INDEXES on empty DB must succeed (not error)");
    assert!(
        r.rows.is_empty(),
        "empty DB must return zero rows; got {} rows",
        r.rows.len()
    );
    let expected: Vec<&str> = vec![
        "table",
        "column",
        "dimension",
        "quantization",
        "vector_count",
        "bytes",
    ];
    assert_eq!(
        r.columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        expected,
        "SHOW VECTOR_INDEXES must return the canonical 6-column header even when empty"
    );
}

#[test]
fn nv16c_alter_table_add_column_registers_new_vector_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty_params(),
    )
    .expect("create");

    // ALTER TABLE ADD COLUMN with a new VECTOR column on the EXECUTING node (not just receiver
    // replay — that's nv19b's territory). The new index must register at its declared dimension.
    db.execute(
        "ALTER TABLE evidence ADD COLUMN vector_audio VECTOR(16)",
        &empty_params(),
    )
    .expect("alter table add vector column");

    // SHOW VECTOR_INDEXES lists the new column.
    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show");
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    let dim_idx = indexes
        .columns
        .iter()
        .position(|c| c == "dimension")
        .unwrap();
    let audio_row = indexes
        .rows
        .iter()
        .find(|r| matches!(&r[column_idx], Value::Text(s) if s == "vector_audio"));
    assert!(
        audio_row.is_some(),
        "ALTER TABLE ADD COLUMN must register vector_audio in SHOW VECTOR_INDEXES; got {:?}",
        indexes.rows
    );
    let audio_row = audio_row.unwrap();
    assert!(
        matches!(&audio_row[dim_idx], Value::Int64(16)),
        "vector_audio must register at declared dimension 16; got {:?}",
        audio_row[dim_idx]
    );

    // INSERT into the new column works.
    let id = Uuid::new_v4();
    let probe = vec![1.0_f32; 16];
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_audio) VALUES ($id, $t, $a)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            ("a", Value::Vector(probe.clone())),
        ]),
    )
    .expect("insert into newly added column");

    // ANN search through the new column returns the row.
    let r = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_audio <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(probe))]),
        )
        .expect("audio search");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(r.rows[0][id_idx], Value::Uuid(id));

    // Wrong-dim probe against the new column fires VectorIndexDimensionMismatch with the right index.
    let mismatched = db.execute(
        "SELECT id FROM evidence ORDER BY vector_audio <=> $q LIMIT 1",
        &params(vec![("q", Value::Vector(vec![0.0_f32; 4]))]),
    );
    assert!(matches!(
        mismatched,
        Err(contextdb_core::Error::VectorIndexDimensionMismatch { ref index, expected: 16, actual: 4 })
            if *index == VectorIndexRef::new("evidence", "vector_audio")
    ));
}

#[test]
fn nv_memory_budget_exceeded_carries_index_tag_in_operation_string() {
    use contextdb_core::{Error, MemoryAccountant};
    use std::sync::Arc;

    // Tiny memory budget forces the failure to fire on a vector insert. The ensuing error must surface
    // the offending (table, column) so an operator hitting OOM on a Jetson can attribute the cost.
    // `MemoryAccountant::with_budget(usize)` exists today; `open_memory_with_accountant` accepts an
    // `Arc<MemoryAccountant>` and returns `Self`.
    let accountant = Arc::new(MemoryAccountant::with_budget(4096));
    let db = Database::open_memory_with_accountant(accountant);
    db.execute(
        "CREATE TABLE audio_clips (id UUID PRIMARY KEY, vector_audio VECTOR(1024))",
        &empty_params(),
    )
    .expect("create");

    let r = db.execute(
        "INSERT INTO audio_clips (id, vector_audio) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("v", Value::Vector(vec![1.0_f32; 1024])), // 4096 bytes payload — at or above the budget
        ]),
    );
    match r {
        Err(Error::MemoryBudgetExceeded { ref operation, .. }) => {
            assert!(
                operation.contains('@'),
                "operation tag must include `@<table>.<column>` so the offending index is visible \
                     in MemoryBudgetExceeded; got operation={operation:?}"
            );
            assert!(
                operation.contains("audio_clips") && operation.contains("vector_audio"),
                "operation tag must name the offending (table, column); got operation={operation:?}"
            );
        }
        other => panic!("expected MemoryBudgetExceeded with a tagged operation, got {other:?}"),
    }
}

#[test]
fn nv_plugin_post_commit_sees_populated_vector_index_ref() {
    use contextdb_engine::plugin::CommitSource;
    use contextdb_engine::plugin::DatabasePlugin;
    use contextdb_tx::WriteSet;
    use std::sync::Mutex;

    // Recorder plugin captures every VectorEntry's index seen in post_commit. A wrong impl that
    // populates `index` only on the persistence write path leaves the WriteSet's `vector_inserts`
    // carrying VectorIndexRef::default() — visible to plugins.
    //
    // The trait signature today is `fn post_commit(&self, _ws: &WriteSet, _source: CommitSource)`
    // (crates/contextdb-engine/src/plugin.rs). The test implements it directly — no WriteSetView
    // wrapper is added. `WriteSet.vector_inserts` is a public Vec<VectorEntry> field.
    let captured: Arc<Mutex<Vec<VectorIndexRef>>> = Arc::new(Mutex::new(Vec::new()));
    struct Recorder {
        captured: Arc<Mutex<Vec<VectorIndexRef>>>,
    }
    impl DatabasePlugin for Recorder {
        fn post_commit(&self, ws: &WriteSet, _source: CommitSource) {
            for entry in &ws.vector_inserts {
                self.captured.lock().unwrap().push(entry.index.clone());
            }
        }
    }
    // open_memory_with_plugin takes Arc<dyn DatabasePlugin>.
    let recorder: Arc<dyn DatabasePlugin> = Arc::new(Recorder {
        captured: Arc::clone(&captured),
    });
    let db = Database::open_memory_with_plugin(recorder).expect("open with plugin");
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4), vector_vision VECTOR(8))",
        &empty_params(),
    ).expect("create");
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            (
                "v",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
        ]),
    )
    .expect("insert");

    let seen = captured.lock().unwrap().clone();
    let seen_set: std::collections::HashSet<VectorIndexRef> = seen.into_iter().collect();
    assert!(
        seen_set.contains(&VectorIndexRef::new("evidence", "vector_text")),
        "plugin must see vector_text index in post_commit"
    );
    assert!(
        seen_set.contains(&VectorIndexRef::new("evidence", "vector_vision")),
        "plugin must see vector_vision index in post_commit"
    );
    assert!(
        seen_set
            .iter()
            .all(|i| !i.table.is_empty() && !i.column.is_empty()),
        "no plugin-visible VectorEntry may carry a default VectorIndexRef"
    );
}

#[test]
fn nv_sync_apply_routes_to_pruned_then_repopulated_index() {
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
    };
    use std::time::Duration;

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4)) RETAIN 1 SECONDS",
            &empty_params(),
        )
        .expect("create with RETAIN");
    let id = Uuid::new_v4();
    receiver
        .execute(
            "INSERT INTO evidence (id, vector_text) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("v", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            ]),
        )
        .expect("seed");

    std::thread::sleep(Duration::from_millis(1500));
    receiver.run_pruning_cycle();

    // The index is REGISTERED (DDL didn't change) but vector_count is 0. A push from a writer with a
    // VectorChange for that index must apply, not return UnknownVectorIndex.
    let before = receiver
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show before repopulate");
    let column_idx = before.columns.iter().position(|c| c == "column").unwrap();
    let count_idx = before
        .columns
        .iter()
        .position(|c| c == "vector_count")
        .unwrap();
    let text_index = before
        .rows
        .iter()
        .find(|row| matches!(&row[column_idx], Value::Text(s) if s == "vector_text"));
    assert!(
        text_index.is_some(),
        "pruning must leave the vector_text index registered even when it has zero entries; got {:?}",
        before.rows
    );
    assert!(
        matches!(&text_index.unwrap()[count_idx], Value::Int64(0)),
        "registered-but-empty vector_text index must report vector_count=0 before repopulate"
    );

    let new_id = Uuid::new_v4();
    let new_row_id = RowId(99);
    let mut row_values: HashMap<String, Value> = HashMap::new();
    row_values.insert("id".into(), Value::Uuid(new_id));
    let cs = ChangeSet {
        rows: vec![RowChange {
            table: "evidence".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: Value::Uuid(new_id),
            },
            values: row_values,
            deleted: false,
            lsn: Lsn(20),
        }],
        edges: vec![],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("evidence", "vector_text"),
            row_id: new_row_id,
            vector: vec![0.0_f32, 1.0, 0.0, 0.0],
            lsn: Lsn(20),
        }],
        ddl: vec![],
    };
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .expect("apply must succeed against a registered-but-empty index");

    let r = receiver
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0]))]),
        )
        .expect("text search");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(r.rows[0][id_idx], Value::Uuid(new_id));
}

#[test]
fn nv_deferred_hnsw_build_per_index() {
    // The "lazy on first search" HNSW build serialization must be PER-INDEX, not a global flag. A
    // global flag means rebuilds the wrong index on first search. Indirect observation: insert
    // beyond the build threshold for ONE index, leave the other small, search both — both must
    // return correct results regardless of which builds first. A global-flag impl gets at most one
    // index built; the other is permanently linear-scan and returns wrong results past threshold.
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4), vector_vision VECTOR(8))",
        &empty_params(),
    ).expect("create");

    fn unique_text_vector(seed: usize) -> Vec<f32> {
        let mut state = (seed as u64 + 1)
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let mut v = Vec::with_capacity(4);
        for _ in 0..4 {
            state = state
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let unit = ((state >> 33) as f64) / ((1u64 << 31) as f64);
            v.push((unit as f32) * 2.0 - 1.0);
        }
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.iter_mut().for_each(|x| *x /= norm);
        v
    }

    // Populate vector_text well past HNSW_THRESHOLD (1000); leave vector_vision small.
    let mut text_target_id = None;
    for i in 0..1500 {
        let id = Uuid::new_v4();
        let t = unique_text_vector(i);
        db.execute(
            "INSERT INTO evidence (id, vector_text) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("v", Value::Vector(t.clone())),
            ]),
        )
        .expect("text insert");
        if i == 500 {
            text_target_id = Some((id, t));
        }
    }
    let vision_id = Uuid::new_v4();
    let vision_vec = vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    db.execute(
        "INSERT INTO evidence (id, vector_vision) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(vision_id)),
            ("v", Value::Vector(vision_vec.clone())),
        ]),
    )
    .expect("vision insert");

    let vector_index_bytes = |column: &str| -> Option<i64> {
        let indexes = db
            .execute("SHOW VECTOR_INDEXES", &empty_params())
            .expect("show vector indexes");
        let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
        let bytes_idx = indexes.columns.iter().position(|c| c == "bytes").unwrap();
        indexes
            .rows
            .iter()
            .find(|row| matches!(&row[column_idx], Value::Text(c) if c == column))
            .and_then(|row| match &row[bytes_idx] {
                Value::Int64(bytes) => Some(*bytes),
                _ => None,
            })
    };

    let text_payload_floor = 1500_i64 * 4_i64 * std::mem::size_of::<f32>() as i64;
    let vision_payload_floor = 8_i64 * std::mem::size_of::<f32>() as i64;
    let text_bytes_before = vector_index_bytes("vector_text");
    let vision_bytes_before = vector_index_bytes("vector_vision");
    assert!(
        text_bytes_before.is_some(),
        "SHOW VECTOR_INDEXES must list vector_text with Int64 bytes before search"
    );
    assert!(
        vision_bytes_before.is_some(),
        "SHOW VECTOR_INDEXES must list vector_vision with Int64 bytes before search"
    );
    let text_bytes_before = text_bytes_before.unwrap();
    let vision_bytes_before = vision_bytes_before.unwrap();
    assert!(
        text_bytes_before >= text_payload_floor,
        "vector_text bytes must account for the stored vector payload before HNSW build; got {text_bytes_before}"
    );
    assert!(
        text_bytes_before < text_payload_floor * 2,
        "vector_text HNSW memory must be lazy and absent before first search; bytes before search={text_bytes_before}"
    );
    assert!(
        vision_bytes_before >= vision_payload_floor
            && vision_bytes_before < vision_payload_floor * 2,
        "small vector_vision bytes must account only for its payload before text search; got {vision_bytes_before}"
    );

    // Search vector_text first — triggers HNSW build for vector_text only. A global-flag impl marks
    // BOTH indexes "built" and never builds vector_vision.
    let (text_id, text_vec) = text_target_id.unwrap();
    let used_before_text_build = db.accountant().usage().used;
    let text_trace = db
        .explain("SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1")
        .expect("text explain");
    assert!(
        text_trace.contains("HNSWSearch") && text_trace.contains("vector_text"),
        "large vector_text index must build/use HNSW independently; got: {text_trace}"
    );
    let r1 = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(text_vec))]),
        )
        .expect("text search");
    let id_idx = r1.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(r1.rows[0][id_idx], Value::Uuid(text_id));
    let used_after_text_build = db.accountant().usage().used;
    assert!(
        used_after_text_build > used_before_text_build,
        "first vector_text explain/search must materialize an accountant-tracked HNSW build; used before={used_before_text_build}, after={used_after_text_build}"
    );
    assert_eq!(
        db.__debug_vector_hnsw_len(VectorIndexRef::new("evidence", "vector_text")),
        Some(1500),
        "vector_text must have a real HNSW graph containing exactly its 1500 vectors, not synthetic bytes or a global graph"
    );
    let text_graph_stats = db
        .__debug_vector_hnsw_stats(VectorIndexRef::new("evidence", "vector_text"))
        .expect("vector_text HNSW graph stats after first search");
    assert_eq!(
        text_graph_stats.point_count, 1500,
        "vector_text HNSW graph must expose real graph point count, not just a built flag"
    );
    assert_eq!(
        text_graph_stats.layer0_points, 1500,
        "the base HNSW layer must contain every vector_text entry"
    );
    assert_eq!(
        text_graph_stats.dimension, 4,
        "vector_text HNSW graph must be built with the vector_text dimension"
    );
    assert!(
        text_graph_stats.layer0_neighbor_edges > 0,
        "vector_text HNSW graph must expose actual layer-0 neighbor links; got {text_graph_stats:?}"
    );
    assert_eq!(
        db.__debug_vector_hnsw_len(VectorIndexRef::new("evidence", "vector_vision")),
        None,
        "below-threshold vector_vision must not inherit vector_text's HNSW graph"
    );
    assert_eq!(
        db.__debug_vector_hnsw_stats(VectorIndexRef::new("evidence", "vector_vision")),
        None,
        "below-threshold vector_vision must not inherit vector_text's HNSW graph stats"
    );
    let text_bytes_after = vector_index_bytes("vector_text")
        .expect("SHOW VECTOR_INDEXES must list vector_text after HNSW build");
    let vision_bytes_after_text_search = vector_index_bytes("vector_vision")
        .expect("SHOW VECTOR_INDEXES must list vector_vision after text HNSW build");
    assert!(
        text_bytes_after > text_bytes_before,
        "vector_text SHOW bytes must increase after HNSW build; before={text_bytes_before}, after={text_bytes_after}"
    );
    assert_eq!(
        vision_bytes_after_text_search, vision_bytes_before,
        "building vector_text HNSW must not mutate the small vector_vision index bytes"
    );

    let vision_trace = db
        .explain("SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1")
        .expect("vision explain");
    assert!(
        vision_trace.contains("VectorSearch")
            && !vision_trace.contains("HNSWSearch")
            && vision_trace.contains("vector_vision"),
        "small vector_vision index must not inherit vector_text's HNSW build state; got: {vision_trace}"
    );

    // Now search vector_vision. A global-flag impl that already considers itself "built" returns the
    // wrong answer; a per-IndexState impl runs linear-scan against vector_vision and returns vision_id.
    let used_before_vision_search = db.accountant().usage().used;
    let r2 = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vision_vec))]),
        )
        .expect("vision search");
    assert_eq!(
        r2.rows[0][id_idx],
        Value::Uuid(vision_id),
        "vision search must succeed independently of vector_text's HNSW build state"
    );
    let used_after_vision_search = db.accountant().usage().used;
    assert_eq!(
        used_after_vision_search, used_before_vision_search,
        "searching the below-threshold vector_vision index must not perform an HNSW allocation"
    );
    assert_eq!(
        vector_index_bytes("vector_vision")
            .expect("SHOW VECTOR_INDEXES must list vector_vision after its own search"),
        vision_bytes_before,
        "below-threshold vector_vision SHOW bytes must stay unchanged after its own search"
    );
}

#[test]
fn nv13c_multi_row_multi_vector_insert_partial_failure_rolls_back_atomically() {
    use contextdb_core::Error;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    // The cg ingest shape: one INSERT statement carries N rows (e.g., a vigil frame with N detected
    // faces ingested as N evidence rows). One bad row anywhere in the batch must roll back the whole
    // statement — not just the offending row.
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4(); // bad row: vector_vision dim 7
    let id_c = Uuid::new_v4();
    let result = db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES \
         ($a, $ta, $va), ($b, $tb, $vb), ($c, $tc, $vc)",
        &params(vec![
            ("a", Value::Uuid(id_a)),
            ("ta", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            ("va", Value::Vector(vec![1.0_f32; 8])),
            ("b", Value::Uuid(id_b)),
            ("tb", Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0])),
            ("vb", Value::Vector(vec![0.0_f32; 7])), // wrong dim
            ("c", Value::Uuid(id_c)),
            ("tc", Value::Vector(vec![0.0_f32, 0.0, 1.0, 0.0])),
            ("vc", Value::Vector(vec![1.0_f32; 8])),
        ]),
    );
    assert!(matches!(
        result,
        Err(Error::VectorIndexDimensionMismatch { ref index, expected: 8, actual: 7 })
            if *index == VectorIndexRef::new("evidence", "vector_vision")
    ));
    // ZERO rows: the statement is atomic across the batch, not just per-row.
    let count = db
        .execute("SELECT id FROM evidence", &empty_params())
        .expect("select")
        .rows
        .len();
    assert_eq!(
        count, 0,
        "multi-row INSERT must roll back ALL rows on any per-row dim mismatch; got {count}"
    );
    // Both indexes are empty — neither id_a's nor id_c's text vector should leak into vector_text.
    let r = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        )
        .expect("text search");
    assert!(
        r.rows.is_empty(),
        "vector_text index must roll back too; got {} hits",
        r.rows.len()
    );
    for leaked_probe in [vec![1.0_f32, 0.0, 0.0, 0.0], vec![0.0_f32, 0.0, 1.0, 0.0]] {
        let raw_hits = db
            .query_vector(
                VectorIndexRef::new("evidence", "vector_text"),
                &leaked_probe,
                5,
                None,
                db.snapshot(),
            )
            .expect("raw text search after failed batch");
        assert!(
            raw_hits.is_empty(),
            "failed multi-row INSERT must not leave raw vector_text entries for {leaked_probe:?}; got {raw_hits:?}"
        );
    }
}

#[test]
fn nv02c_multi_row_multi_vector_insert_succeeds_with_per_row_routing() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    let pre_insert_lsn = db.current_lsn();
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($a, $ta, $va), ($b, $tb, $vb)",
        &params(vec![
            ("a", Value::Uuid(id_a)),
            ("ta", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            ("va", Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
            ("b", Value::Uuid(id_b)),
            ("tb", Value::Vector(vec![0.0_f32, 0.0, 1.0, 0.0])),
            ("vb", Value::Vector(vec![0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0])),
        ]),
    ).expect("multi-row multi-vector insert");
    let cs = db.changes_since(pre_insert_lsn);
    assert_eq!(
        cs.vectors.len(),
        4,
        "2 rows × 2 populated vector indexes must emit 4 VectorChanges; got {}",
        cs.vectors.len()
    );
    let expected_indexes = [
        VectorIndexRef::new("evidence", "vector_text"),
        VectorIndexRef::new("evidence", "vector_vision"),
    ];
    let mut by_row: HashMap<RowId, Vec<VectorIndexRef>> = HashMap::new();
    for change in &cs.vectors {
        by_row
            .entry(change.row_id)
            .or_default()
            .push(change.index.clone());
    }
    assert_eq!(
        by_row.len(),
        2,
        "multi-row insert must emit envelopes for exactly two row ids; got {by_row:?}"
    );
    for (row_id, mut indexes) in by_row {
        indexes.sort_by(|a, b| {
            (a.table.as_str(), a.column.as_str()).cmp(&(b.table.as_str(), b.column.as_str()))
        });
        let mut expected = expected_indexes.to_vec();
        expected.sort_by(|a, b| {
            (a.table.as_str(), a.column.as_str()).cmp(&(b.table.as_str(), b.column.as_str()))
        });
        assert_eq!(
            indexes, expected,
            "row {row_id:?} must have exactly one envelope for each populated vector index"
        );
    }

    // Each row is routed to both indexes correctly; ANN against each row's text vector returns that
    // row at top-1 (proves per-row × per-column routing works in the multi-row path).
    let r = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![0.0_f32, 0.0, 1.0, 0.0]))]),
        )
        .expect("text search for id_b's vector");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(r.rows[0][id_idx], Value::Uuid(id_b));
    let vision = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![(
                "q",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            )]),
        )
        .expect("vision search for id_a's vector");
    let id_idx = vision.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(vision.rows[0][id_idx], Value::Uuid(id_a));
}

#[test]
fn nv14c_record_observation_fanout_unified_tx_atomicity() {
    use contextdb_core::Direction;

    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY)",
        &empty_params(),
    )
    .expect("create entities");
    db.execute(
        "CREATE TABLE entity_snapshots (id UUID PRIMARY KEY, entity_id UUID)",
        &empty_params(),
    )
    .expect("create snapshots");
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            observation_id UUID,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create evidence");

    let pre_snap = db.snapshot();

    // The cg record_observation fanout: 1 entity + 1 snapshot + 3 evidence rows (text, vision, mixed)
    // + a graph edge linking entity → snapshot, all in ONE transaction.
    let entity_id = Uuid::new_v4();
    let snapshot_id = Uuid::new_v4();
    let observation_id = Uuid::new_v4();
    let ev1 = Uuid::new_v4();
    let ev2 = Uuid::new_v4();
    let ev3 = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(tx, "entities", values(vec![("id", Value::Uuid(entity_id))]))
        .expect("insert entity");
    db.insert_row(
        tx,
        "entity_snapshots",
        values(vec![
            ("id", Value::Uuid(snapshot_id)),
            ("entity_id", Value::Uuid(entity_id)),
        ]),
    )
    .expect("insert snapshot");
    let r1 = db
        .insert_row(
            tx,
            "evidence",
            values(vec![
                ("id", Value::Uuid(ev1)),
                ("observation_id", Value::Uuid(observation_id)),
            ]),
        )
        .expect("insert evidence 1");
    db.insert_vector(
        tx,
        VectorIndexRef::new("evidence", "vector_text"),
        r1,
        vec![1.0_f32, 0.0, 0.0, 0.0],
    )
    .expect("text vec 1");
    let r2 = db
        .insert_row(
            tx,
            "evidence",
            values(vec![
                ("id", Value::Uuid(ev2)),
                ("observation_id", Value::Uuid(observation_id)),
            ]),
        )
        .expect("insert evidence 2");
    let vision_insert = db.insert_vector(
        tx,
        VectorIndexRef::new("evidence", "vector_vision"),
        r2,
        vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    );
    assert!(
        vision_insert.is_ok(),
        "record_observation fanout must accept mixed text and vision vector indexes in one tx; got {:?}",
        vision_insert.as_ref().err()
    );
    let r3 = db
        .insert_row(
            tx,
            "evidence",
            values(vec![
                ("id", Value::Uuid(ev3)),
                ("observation_id", Value::Uuid(observation_id)),
            ]),
        )
        .expect("insert evidence 3");
    db.insert_vector(
        tx,
        VectorIndexRef::new("evidence", "vector_text"),
        r3,
        vec![0.0_f32, 0.0, 1.0, 0.0],
    )
    .expect("text vec 3");
    db.insert_edge(
        tx,
        entity_id,
        snapshot_id,
        "snapshot_of".to_string(),
        HashMap::new(),
    )
    .expect("insert edge");
    db.commit(tx).expect("commit");

    let post_snap = db.snapshot();

    // Pre-snap reader sees nothing on any of the four surfaces.
    assert!(
        db.query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            pre_snap
        )
        .expect("pre text")
        .is_empty(),
        "pre-snap reader must not see any evidence vectors"
    );
    assert!(
        db.query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            5,
            None,
            pre_snap
        )
        .expect("pre vision")
        .is_empty()
    );
    let pre_bfs = db
        .query_bfs(entity_id, None, Direction::Outgoing, 2, pre_snap)
        .expect("pre bfs");
    assert!(
        pre_bfs.nodes.iter().all(|n| n.id != snapshot_id),
        "pre-snap reader must not see the entity→snapshot edge"
    );

    // Post-snap reader sees ALL four surfaces consistently. ANN through vector_text returns r1 and r3
    // (both populated this column); ANN through vector_vision returns r2.
    let text_post = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            post_snap,
        )
        .unwrap();
    assert!(
        text_post.iter().any(|(rid, _)| *rid == r1),
        "post-snap text ANN must include evidence 1"
    );
    assert!(
        text_post.iter().any(|(rid, _)| *rid == r3),
        "post-snap text ANN must include evidence 3 from the same fanout transaction"
    );
    let vision_post = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            5,
            None,
            post_snap,
        )
        .unwrap();
    assert!(
        vision_post.iter().any(|(rid, _)| *rid == r2),
        "post-snap vision ANN must include evidence 2"
    );
    let post_bfs = db
        .query_bfs(entity_id, None, Direction::Outgoing, 2, post_snap)
        .unwrap();
    assert!(
        post_bfs.nodes.iter().any(|n| n.id == snapshot_id),
        "post-snap BFS must see the snapshot edge"
    );
}

#[test]
fn nv_subscription_event_includes_table_for_vector_only_change() {
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty_params(),
    )
    .expect("create");
    db.execute(
        "CREATE TABLE digests (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty_params(),
    )
    .expect("create unrelated table");

    // Seed and commit the relational row first. The observed commit below must be vector-only.
    let id = Uuid::new_v4();
    let tx = db.begin();
    let row_id = db
        .insert_row(tx, "evidence", values(vec![("id", Value::Uuid(id))]))
        .expect("seed row");
    db.commit(tx).expect("commit seed row");

    // Subscribe after the seed commit, then apply a ChangeSet that touches ONLY a vector index.
    let events = db.subscribe();
    let cs = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("evidence", "vector_text"),
            row_id,
            vector: vec![1.0, 0.0, 0.0, 0.0],
            lsn: Lsn(10),
        }],
        ddl: vec![],
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .expect("apply vector-only");

    // Drain events until we find the apply commit. The vector-only commit MUST list `evidence` in
    // tables_changed so subscribers can react.
    let event = events
        .recv()
        .expect("subscription delivers vector-only commit event");
    assert_eq!(
        event.tables_changed,
        vec!["evidence".to_string()],
        "CommitEvent.tables_changed for a vector-only mutation must name exactly the touched table, not all known tables; got {:?}",
        event.tables_changed
    );
}

#[test]
fn nv_retain_prunes_every_vector_index_for_expired_row() {
    use std::time::Duration;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        ) RETAIN 1 SECONDS",
        &empty_params(),
    )
    .expect("create with RETAIN");

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("t", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0])),
            (
                "v",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
        ]),
    )
    .expect("insert");

    std::thread::sleep(Duration::from_millis(1500));
    db.run_pruning_cycle(); // returns u64 (count pruned); no Result, so no .expect()

    // Both indexes must drop the expired row — not just the first registered.
    let text_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 5",
            &params(vec![("q", Value::Vector(vec![1.0_f32, 0.0, 0.0, 0.0]))]),
        )
        .expect("text search after prune");
    assert!(
        text_hits.rows.is_empty(),
        "vector_text must drop pruned row"
    );

    let vision_hits = db
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 5",
            &params(vec![(
                "q",
                Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            )]),
        )
        .expect("vision search after prune");
    assert!(
        vision_hits.rows.is_empty(),
        "vector_vision must drop pruned row — TTL prune must walk every registered (table, column)"
    );
    let prune_snap = db.snapshot();
    let raw_text_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &[1.0_f32, 0.0, 0.0, 0.0],
            5,
            None,
            prune_snap,
        )
        .expect("raw text index after prune");
    let raw_vision_hits = db
        .query_vector(
            VectorIndexRef::new("evidence", "vector_vision"),
            &[0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            5,
            None,
            prune_snap,
        )
        .expect("raw vision index after prune");
    assert!(
        raw_text_hits.is_empty() && raw_vision_hits.is_empty(),
        "TTL prune must tombstone raw vector entries, not just hide expired rows in SQL ANN; got text={raw_text_hits:?}, vision={raw_vision_hits:?}"
    );

    // SHOW VECTOR_INDEXES.vector_count is 0 for both — the indexes are still REGISTERED (DDL didn't
    // change), they just hold no entries.
    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty_params())
        .expect("show");
    assert_eq!(
        indexes.rows.len(),
        2,
        "both vector indexes must remain registered after pruning; got {:?}",
        indexes.rows
    );
    let count_idx = indexes
        .columns
        .iter()
        .position(|c| c == "vector_count")
        .unwrap();
    let column_idx = indexes.columns.iter().position(|c| c == "column").unwrap();
    for row in &indexes.rows {
        if matches!(&row[column_idx], Value::Text(s) if s.starts_with("vector_")) {
            assert!(
                matches!(&row[count_idx], Value::Int64(0)),
                "every vector index on `evidence` must report vector_count=0 after prune; got {:?}",
                row[count_idx]
            );
        }
    }
}

#[test]
fn nv_indexed_filter_intersects_per_column_vector_search() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            category TEXT,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &empty_params(),
    )
    .expect("create");
    db.execute(
        "CREATE INDEX idx_category ON evidence (category)",
        &empty_params(),
    )
    .expect("create category index");

    // 3 rows category='A', 3 rows category='B'. B has exact decoys for each query so a wrong
    // "global ANN first, post-filter after LIMIT" implementation returns empty/wrong under LIMIT 1.
    let a_text_vectors = [
        vec![1.0_f32, 0.0, 0.0, 0.0],
        vec![0.0_f32, 1.0, 0.0, 0.0],
        vec![0.0_f32, 0.0, 0.8, 0.2],
    ];
    let a_vision_vectors = [
        vec![1.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        vec![0.0_f32, 0.8, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0],
        vec![0.0_f32, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ];
    let b_text_vectors = [
        vec![0.0_f32, 0.0, 1.0, 0.0],
        vec![0.5_f32, 0.5, 0.0, 0.0],
        vec![0.0_f32, 0.0, 0.0, 1.0],
    ];
    let b_vision_vectors = [
        vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        vec![0.5_f32, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        vec![0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
    ];
    let mut a_ids = Vec::new();
    let mut b_ids = Vec::new();
    for i in 0..3 {
        let id = Uuid::new_v4();
        a_ids.push(id);
        db.execute(
            "INSERT INTO evidence (id, category, vector_text, vector_vision) VALUES ($id, $c, $t, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("c", Value::Text("A".into())),
                ("t", Value::Vector(a_text_vectors[i].clone())),
                ("v", Value::Vector(a_vision_vectors[i].clone())),
            ]),
        ).expect("insert A");
    }
    for i in 0..3 {
        let id = Uuid::new_v4();
        b_ids.push(id);
        db.execute(
            "INSERT INTO evidence (id, category, vector_text, vector_vision) VALUES ($id, $c, $t, $v)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("c", Value::Text("B".into())),
                ("t", Value::Vector(b_text_vectors[i].clone())),
                ("v", Value::Vector(b_vision_vectors[i].clone())),
            ]),
        ).expect("insert B");
    }

    // WHERE category='A' filters to A-rows; ORDER BY vector_text routes to vector_text index.
    let r = db
        .execute(
            "SELECT id FROM evidence WHERE category = $c ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![
                ("c", Value::Text("A".into())),
                ("q", Value::Vector(vec![0.0_f32, 0.0, 1.0, 0.0])),
            ]),
        )
        .expect("filtered text search");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    let returned: Vec<Value> = r.rows.iter().map(|row| row[id_idx].clone()).collect();
    assert!(
        returned
            .iter()
            .all(|v| { matches!(v, Value::Uuid(u) if a_ids.contains(u)) }),
        "filtered ANN must return only A-rows; got {:?}",
        returned
    );
    assert!(!returned.is_empty(), "must return at least one A-row");
    assert_eq!(
        returned.len(),
        1,
        "LIMIT 1 filtered vector_text ANN must return exactly one A-row; got {returned:?}"
    );
    assert_eq!(
        returned.first(),
        Some(&Value::Uuid(a_ids[2])),
        "filtered vector_text ANN must rank the nearest A row first, not merely return any A row"
    );

    // EXPLAIN proves the indexed scan feeds VectorSearch with the right (table, column).
    let trace = db.explain(
        "SELECT id FROM evidence WHERE category = 'A' ORDER BY vector_text <=> '[0,0,1,0]' LIMIT 1"
    ).expect("explain");
    assert!(
        trace.contains("IndexScan")
            && trace.contains("VectorSearch")
            && trace.contains("table=evidence")
            && trace.contains("column=vector_text"),
        "EXPLAIN must show IndexScan feeding VectorSearch(table=evidence, column=vector_text); got: {trace}"
    );

    // Same WHERE, different ORDER BY column — the candidate set must be re-keyed per (table, column),
    // not aliased.
    let r2 = db
        .execute(
            "SELECT id FROM evidence WHERE category = $c ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![
                ("c", Value::Text("A".into())),
                (
                    "q",
                    Value::Vector(vec![0.0_f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                ),
            ]),
        )
        .expect("filtered vision search");
    let returned2: Vec<Value> = r2.rows.iter().map(|row| row[id_idx].clone()).collect();
    assert!(
        returned2
            .iter()
            .all(|v| { matches!(v, Value::Uuid(u) if a_ids.contains(u)) }),
        "filtered ANN on vision must still return only A-rows"
    );
    assert_eq!(
        returned2.len(),
        1,
        "LIMIT 1 filtered vector_vision ANN must return exactly one A-row; got {returned2:?}"
    );
    assert_eq!(
        returned2.first(),
        Some(&Value::Uuid(a_ids[1])),
        "filtered vector_vision ANN must rank the nearest A row first, not merely return any A row"
    );
}

#[test]
fn nv17_protocol_version_mismatch_returns_typed_error() {
    use contextdb_server::protocol::{Envelope, MessageType, decode};

    // Forge an envelope at a version higher than what the server supports. Direct rmp_serde encoding
    // bypasses `encode()` (which always stamps PROTOCOL_VERSION) so we can exercise the version guard.
    let bogus_version: u8 = 99;
    let envelope = Envelope {
        version: bogus_version,
        message_type: MessageType::PullRequest,
        payload: rmp_serde::to_vec(&()).expect("encode empty payload"),
    };
    let bytes = rmp_serde::to_vec(&envelope).expect("encode envelope");

    let result = decode(&bytes);
    assert!(
        matches!(
            result,
            Err(contextdb_server::error::SyncError::ProtocolVersionMismatch { received, supported })
                if received == bogus_version && supported >= 2
        ),
        "decode must reject bogus version with the typed variant; got {result:?}"
    );
}

#[test]
fn nv17b_protocol_version_mismatch_rejects_lower_version_envelopes() {
    use contextdb_server::protocol::{Envelope, MessageType, decode};

    // A v2 receiver must also reject a v1 envelope, not just envelopes claiming a higher version.
    // Without symmetric rejection, the asymmetric-upgrade detection contract (RB17) is unwired.
    let envelope = Envelope {
        version: 1,
        message_type: MessageType::PullRequest,
        payload: rmp_serde::to_vec(&()).expect("encode empty payload"),
    };
    let bytes = rmp_serde::to_vec(&envelope).expect("encode envelope");

    let result = decode(&bytes);
    // At Step 3 stub time: PROTOCOL_VERSION = 1 and the guard is still `version > PROTOCOL_VERSION`,
    // so a v1 envelope returns Ok(_) and the matches! arm fails — RED. After Step 5 sub-batch B4,
    // PROTOCOL_VERSION = 2 and the guard becomes `version != PROTOCOL_VERSION`, returning the typed variant.
    assert!(
        matches!(
            result,
            Err(
                contextdb_server::error::SyncError::ProtocolVersionMismatch {
                    received: 1,
                    supported: 2
                }
            )
        ),
        "v2 receiver must reject v1 envelope (asymmetric upgrade detection); got {result:?}"
    );
}

#[test]
fn nv19_unknown_index_apply_returns_typed_error() {
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, SyncDirection, VectorChange,
    };

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
            &empty_params(),
        )
        .expect("create");

    let cs = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("nonexistent", "vector_x"),
            row_id: RowId(1),
            vector: vec![0.0_f32; 4],
            lsn: contextdb_core::Lsn(1),
        }],
        ddl: vec![],
    };
    let res = receiver.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins));
    assert!(matches!(
        res,
        Err(contextdb_core::Error::UnknownVectorIndex { ref index })
            if *index == VectorIndexRef::new("nonexistent", "vector_x")
    ));

    let existing_table_unknown_column = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("evidence", "vector_unknown"),
            row_id: RowId(1),
            vector: vec![0.0_f32; 4],
            lsn: contextdb_core::Lsn(2),
        }],
        ddl: vec![],
    };
    let res = receiver.apply_changes(
        existing_table_unknown_column,
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );
    assert!(
        matches!(
            res,
            Err(contextdb_core::Error::UnknownVectorIndex { ref index })
                if *index == VectorIndexRef::new("evidence", "vector_unknown")
        ),
        "apply must reject an existing table with an unregistered vector column, not just missing tables; got {res:?}"
    );

    let raw_query = receiver.query_vector(
        VectorIndexRef::new("evidence", "vector_unknown"),
        &[0.0_f32; 4],
        1,
        None,
        receiver.snapshot(),
    );
    assert!(
        matches!(
            raw_query,
            Err(contextdb_core::Error::UnknownVectorIndex { ref index })
                if *index == VectorIndexRef::new("evidence", "vector_unknown")
        ),
        "raw Rust API query_vector must reject undeclared indexes, not silently search/create an ad-hoc slot; got {raw_query:?}"
    );

    let tx = receiver.begin();
    let raw_row_id = receiver
        .insert_row(
            tx,
            "evidence",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .expect("raw api seed row");
    let raw_insert = receiver.insert_vector(
        tx,
        VectorIndexRef::new("evidence", "vector_unknown"),
        raw_row_id,
        vec![0.0_f32; 4],
    );
    assert!(
        matches!(
            raw_insert,
            Err(contextdb_core::Error::UnknownVectorIndex { ref index })
                if *index == VectorIndexRef::new("evidence", "vector_unknown")
        ),
        "raw Rust API insert_vector must reject undeclared indexes, not create ad-hoc vector stores; got {raw_insert:?}"
    );
    receiver.rollback(tx).expect("rollback raw api seed");

    let mixed_direction_changes = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_text"),
                row_id: RowId(7),
                vector: vec![1.0_f32, 0.0, 0.0, 0.0],
                lsn: Lsn(3),
            },
            VectorChange {
                index: VectorIndexRef::new("blocked", "vector_text"),
                row_id: RowId(8),
                vector: vec![0.0_f32, 1.0, 0.0, 0.0],
                lsn: Lsn(3),
            },
        ],
        ddl: vec![],
    };
    let mut directions = HashMap::new();
    directions.insert("evidence".to_string(), SyncDirection::Push);
    directions.insert("blocked".to_string(), SyncDirection::None);
    let filtered = mixed_direction_changes
        .filter_by_direction(&directions, &[SyncDirection::Push, SyncDirection::Both]);
    assert_eq!(
        filtered.vectors,
        vec![VectorChange {
            index: VectorIndexRef::new("evidence", "vector_text"),
            row_id: RowId(7),
            vector: vec![1.0_f32, 0.0, 0.0, 0.0],
            lsn: Lsn(3),
        }],
        "sync direction filtering must use VectorChange.index.table, not clone every vector change"
    );
}
