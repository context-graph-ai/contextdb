use contextdb_core::{Lsn, RowId, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, VectorChange};
use contextdb_server::protocol::WireChangeSet;
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn explain_binds_vector_index(trace: &str, table: &str, column: &str) -> bool {
    let Some(vector_pos) = trace.find("VectorSearch") else {
        return false;
    };
    let vector_operator = &trace[vector_pos..];
    let vector_header = vector_operator
        .split("candidates:")
        .next()
        .unwrap_or(vector_operator);
    let debug_table = format!("table: \"{table}\"");
    let debug_column = format!("column: \"{column}\"");
    let legacy_pair = format!("VectorSearch(table={table}, column={column}");
    (vector_header.contains(&debug_table) && vector_header.contains(&debug_column))
        || trace.contains(&legacy_pair)
}

#[test]
fn nv05_planner_emits_full_vector_index_ref_in_explain() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
        &HashMap::new(),
    )
    .expect("create evidence");
    // Sibling table with the same `vector_text` column to prove the planner binds `table=` to the
    // FROM clause, not to a hardcoded literal or to the first declared schema entry.
    db.execute(
        "CREATE TABLE digests (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &HashMap::new(),
    )
    .expect("create digests");

    let trace = db
        .explain("SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 5")
        .expect("explain text query");
    assert!(
        explain_binds_vector_index(&trace, "evidence", "vector_text"),
        "ORDER BY vector_text on evidence must produce VectorSearch(table=evidence, column=vector_text); got: {trace}"
    );
    assert!(
        !trace.contains("column=embedding") && !trace.contains("column: \"embedding\""),
        "planner must not hardcode column=embedding; got: {trace}"
    );

    let trace_vision = db
        .explain("SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 5")
        .expect("explain vision query");
    assert!(
        explain_binds_vector_index(&trace_vision, "evidence", "vector_vision"),
        "ORDER BY vector_vision on evidence must produce VectorSearch(table=evidence, column=vector_vision); got: {trace_vision}"
    );

    let trace_digests = db
        .explain("SELECT id FROM digests ORDER BY vector_text <=> $q LIMIT 5")
        .expect("explain digests query");
    assert!(
        explain_binds_vector_index(&trace_digests, "digests", "vector_text"),
        "ORDER BY vector_text on digests must produce VectorSearch(table=digests, column=vector_text) — \
         a planner that hardcodes table=evidence (or always picks the first declared table) fails here; got: {trace_digests}"
    );
    assert_ne!(trace, trace_vision);
    assert_ne!(trace, trace_digests);
}

#[test]
fn nv07a_sync_vector_change_round_trip_preserves_table_column_identity() {
    let original = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_text"),
                row_id: RowId(7),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                lsn: Lsn(42),
            },
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_vision"),
                row_id: RowId(7),
                vector: vec![0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                lsn: Lsn(42),
            },
        ],
        ddl: vec![],
    };
    let wire: WireChangeSet = original.clone().into();
    let bytes = rmp_serde::to_vec(&wire)
        .expect("rmp_serde encode (matches protocol_newtype_roundtrip pattern)");
    let decoded: WireChangeSet = rmp_serde::from_slice(&bytes).expect("rmp_serde decode");
    let restored: ChangeSet = decoded.into();
    assert_eq!(
        restored.vectors[0].index,
        VectorIndexRef::new("evidence", "vector_text")
    );
    assert_eq!(
        restored.vectors[1].index,
        VectorIndexRef::new("evidence", "vector_vision")
    );
}

#[test]
fn nv07b_sync_apply_routes_to_matching_index() {
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
    };

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (
                id UUID PRIMARY KEY,
                vector_text VECTOR(4),
                vector_vision VECTOR(8)
            )",
            &HashMap::new(),
        )
        .expect("create");

    // Construct a changeset carrying the row identity separately from both vectors; nv07b must prove
    // the apply path consumes VectorChange envelopes rather than rebuilding vectors from row values.
    let id = Uuid::new_v4();
    let row_id = RowId(1);
    let lsn = Lsn(10);
    let mut row_values: HashMap<String, Value> = HashMap::new();
    row_values.insert("id".into(), Value::Uuid(id));
    let cs = ChangeSet {
        rows: vec![RowChange {
            table: "evidence".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: Value::Uuid(id),
            },
            values: row_values,
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
        ddl: vec![],
    };
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .expect("apply changeset");

    // After apply, ANN through each index returns the row — proving the vector landed in the right (table, column) on the receiver.
    let text_hits = receiver
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]),
        )
        .expect("text search");
    let id_idx = text_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(text_hits.rows.len(), 1);
    assert_eq!(text_hits.rows[0][id_idx], Value::Uuid(id));

    let vision_hits = receiver
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![(
                "q",
                Value::Vector(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            )]),
        )
        .expect("vision search");
    assert_eq!(vision_hits.rows.len(), 1);
    assert_eq!(vision_hits.rows[0][id_idx], Value::Uuid(id));

    // Wrong-dim probe against vision must fire VectorIndexDimensionMismatch with the right index identity —
    // proves apply registered the vector against the vision index, not against vector_text or a global slot.
    let mismatched = receiver.execute(
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
fn nv07c_receiver_promoted_to_leader_emits_correct_index_keyed_envelopes() {
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
    };

    // Phase 1: receiver applies a multi-(table, column) ChangeSet from a remote writer.
    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
            &HashMap::new(),
        )
        .expect("create");
    let id = Uuid::new_v4();
    let lsn = Lsn(10);
    let mut row_values: HashMap<String, Value> = HashMap::new();
    row_values.insert("id".into(), Value::Uuid(id));
    row_values.insert(
        "vector_text".into(),
        Value::Vector(vec![1.0, 0.0, 0.0, 0.0]),
    );
    row_values.insert(
        "vector_vision".into(),
        Value::Vector(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    );
    let cs = ChangeSet {
        rows: vec![RowChange {
            table: "evidence".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: Value::Uuid(id),
            },
            values: row_values,
            deleted: false,
            lsn,
        }],
        edges: vec![],
        vectors: vec![
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_text"),
                row_id: RowId(1),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                lsn,
            },
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_vision"),
                row_id: RowId(1),
                vector: vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                lsn,
            },
        ],
        ddl: vec![],
    };
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .expect("apply");

    // Phase 2: receiver is now promoted to leader. It originates a NEW multi-(table, column) write.
    let leader_lsn_before = receiver.current_lsn();
    let new_id = Uuid::new_v4();
    let new_text_vec = vec![0.0_f32, 0.0, 1.0, 0.0];
    let new_vision_vec = vec![0.0_f32, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    receiver
        .execute(
            "INSERT INTO evidence (id, vector_text, vector_vision) VALUES ($id, $t, $v)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(new_id)),
                ("t".to_string(), Value::Vector(new_text_vec.clone())),
                ("v".to_string(), Value::Vector(new_vision_vec.clone())),
            ]),
        )
        .expect("native insert on promoted leader");

    // The promoted leader's outgoing changeset must carry both (table, column) pairs — not a default,
    // not inferred from "first match per row_id", not coalesced into a single envelope.
    let outgoing = receiver.changes_since(leader_lsn_before);
    let by_index: std::collections::HashSet<&VectorIndexRef> =
        outgoing.vectors.iter().map(|c| &c.index).collect();
    assert!(
        by_index.contains(&VectorIndexRef::new("evidence", "vector_text")),
        "promoted-leader changeset must carry the vector_text envelope explicitly"
    );
    assert!(
        by_index.contains(&VectorIndexRef::new("evidence", "vector_vision")),
        "promoted-leader changeset must carry the vector_vision envelope explicitly"
    );
    // None of the envelopes carry a default (empty) VectorIndexRef.
    assert!(
        outgoing
            .vectors
            .iter()
            .all(|c| !c.index.table.is_empty() && !c.index.column.is_empty()),
        "no envelope may carry a default VectorIndexRef in production state"
    );
    let outgoing_by_index: HashMap<VectorIndexRef, &VectorChange> = outgoing
        .vectors
        .iter()
        .map(|change| (change.index.clone(), change))
        .collect();
    assert_eq!(
        &outgoing_by_index
            .get(&VectorIndexRef::new("evidence", "vector_text"))
            .expect("outgoing vector_text change")
            .vector,
        &new_text_vec,
        "promoted-leader vector_text envelope must carry the live vector_text payload"
    );
    assert_eq!(
        &outgoing_by_index
            .get(&VectorIndexRef::new("evidence", "vector_vision"))
            .expect("outgoing vector_vision change")
            .vector,
        &new_vision_vec,
        "promoted-leader vector_vision envelope must carry the live vector_vision payload"
    );

    let mut envelope_only = outgoing.clone();
    for row in &mut envelope_only.rows {
        row.values.remove("vector_text");
        row.values.remove("vector_vision");
    }
    let downstream = Database::open_memory();
    downstream
        .execute(
            "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            vector_text VECTOR(4),
            vector_vision VECTOR(8)
        )",
            &HashMap::new(),
        )
        .expect("downstream create");
    downstream
        .apply_changes(
            envelope_only,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("downstream applies promoted-leader vector-envelope-only delta");
    let text_hits = downstream
        .execute(
            "SELECT id FROM evidence ORDER BY vector_text <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(new_text_vec))]),
        )
        .expect("downstream text search");
    let id_idx = text_hits.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(text_hits.rows[0][id_idx], Value::Uuid(new_id));
    let vision_hits = downstream
        .execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(new_vision_vec))]),
        )
        .expect("downstream vision search");
    assert_eq!(
        vision_hits.rows[0][id_idx],
        Value::Uuid(new_id),
        "downstream receiver must materialize promoted-leader vector_vision payload from the generated envelope"
    );
}
