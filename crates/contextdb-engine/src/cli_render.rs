//! Public rendering helpers used by both the CLI binary and the test suite.

use crate::Database;
use crate::database::QueryTrace;
use crate::sync_types::Conflict;
use contextdb_core::Value;
use contextdb_core::table_meta::{ColumnType, TableMeta};
use std::fmt::Write;

/// Render a column type as a DDL token.
pub fn render_column_type(col_type: &ColumnType) -> String {
    match col_type {
        ColumnType::Integer => "INTEGER".to_string(),
        ColumnType::Real => "REAL".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Boolean => "BOOLEAN".to_string(),
        ColumnType::Json => "JSON".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Vector(dim) => format!("VECTOR({dim})"),
        ColumnType::Timestamp => "TIMESTAMP".to_string(),
        ColumnType::TxId => "TXID".to_string(),
    }
}

/// Render a table's `.schema` DDL. Auto-indexes (`kind == IndexKind::Auto`)
/// are suppressed from output to keep `.schema` focused on user-authored
/// DDL. Pass `render_table_meta_verbose` to include them.
pub fn render_table_meta(table: &str, meta: &TableMeta) -> String {
    render_table_meta_inner(table, meta, false)
}

/// Render a table's `.schema` DDL INCLUDING auto-indexes. Used by
/// `.schema --verbose` / `EXPLAIN SCHEMA t` for agents that need to see the
/// full picture.
pub fn render_table_meta_verbose(table: &str, meta: &TableMeta) -> String {
    render_table_meta_inner(table, meta, true)
}

fn render_table_meta_inner(table: &str, meta: &TableMeta, verbose: bool) -> String {
    let mut buf = String::new();
    writeln!(&mut buf, "CREATE TABLE {table} (").unwrap();
    let mut first = true;
    for col in &meta.columns {
        if !first {
            buf.push_str(",\n");
        }
        first = false;
        // Reuse the canonical column renderer so `.schema` renders the full
        // declared column contract (foreign-key REFERENCES and its ON STATE
        // PROPAGATE SET form, UNIQUE, EXPIRES, quantization, immutability, rank
        // policy) identically to the sync DDL emitter — author once.
        let ty = crate::database::sql_type_for_meta_column(col, &meta.propagation_rules);
        write!(&mut buf, "  {} {}", col.name, ty).unwrap();
    }
    // A multi-column primary key is a table-level element, rendered after the
    // columns so `.schema` gives back the `PRIMARY KEY (a, b, ...)` the operator
    // wrote and re-parses to the same identity. A single-column primary key
    // rides its column's type token above and never reaches here.
    if !meta.primary_key_columns.is_empty() {
        write!(
            &mut buf,
            ",\n  PRIMARY KEY ({})",
            meta.primary_key_columns.join(", ")
        )
        .unwrap();
    }
    for unique_constraint in &meta.unique_constraints {
        write!(&mut buf, ",\n  UNIQUE ({})", unique_constraint.join(", ")).unwrap();
    }
    buf.push_str("\n)");
    if meta.immutable {
        buf.push_str(" IMMUTABLE");
    }
    if let Some(sm) = &meta.state_machine {
        let mut entries: Vec<_> = sm.transitions.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let transitions: Vec<String> = entries
            .into_iter()
            .map(|(from, tos)| format!("{from} -> [{}]", tos.join(", ")))
            .collect();
        write!(
            &mut buf,
            " STATE MACHINE ({}: {})",
            sm.column,
            transitions.join(", ")
        )
        .unwrap();
    }
    if !meta.dag_edge_types.is_empty() {
        let edge_types = meta
            .dag_edge_types
            .iter()
            .map(|edge_type| format!("'{edge_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(&mut buf, " DAG({edge_types})").unwrap();
    }
    for clause in crate::database::retain_and_propagate_clauses_from_meta(meta) {
        write!(&mut buf, " {clause}").unwrap();
    }
    buf.push_str(";\n");
    for decl in &meta.indexes {
        if !verbose && decl.kind == contextdb_core::IndexKind::Auto {
            continue;
        }
        let cols: Vec<String> = decl
            .columns
            .iter()
            .map(|(c, dir)| {
                let dir_str = match dir {
                    contextdb_core::SortDirection::Asc => "ASC",
                    contextdb_core::SortDirection::Desc => "DESC",
                };
                format!("{c} {dir_str}")
            })
            .collect();
        writeln!(
            &mut buf,
            "CREATE INDEX {} ON {} ({});",
            decl.name,
            table,
            cols.join(", ")
        )
        .unwrap();
    }
    buf
}

/// What `.explain` shows about a statement's route: the plan it took, the
/// index it went through, what it pushed into that index, what it turned down
/// and why, and whether the ordering came for free.
///
/// One place owns this shape. `.explain` on a statement the CLI could run and
/// `.explain` on one it could only plan are the same question about the same
/// route, so an operator must not have to learn two answers -- and a caller
/// that already HAS the result must not have to run the statement a second
/// time to be told about it.
pub fn render_explain_trace(trace: &QueryTrace) -> String {
    let mut out = String::new();
    out.push_str(trace.physical_plan);
    if let Some(idx) = &trace.index_used {
        out.push_str(&format!(" {{ index: {idx} }}"));
    }
    out.push('\n');
    if !trace.predicates_pushed.is_empty() {
        out.push_str("  predicates_pushed: [");
        for (i, p) in trace.predicates_pushed.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(p.as_ref());
        }
        out.push_str("]\n");
    }
    if !trace.indexes_considered.is_empty() {
        out.push_str("  indexes_considered: [");
        for (i, c) in trace.indexes_considered.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&format!("{}: {}", c.name, c.rejected_reason));
        }
        out.push_str("]\n");
    }
    if trace.sort_elided {
        out.push_str("  sort_elided: true\n");
    }
    out
}

/// Render the `.explain <sql>` REPL output. Runs the SQL to populate the
/// trace, then formats the physical plan + index-usage summary.
pub fn render_explain(
    db: &Database,
    sql: &str,
    params: &std::collections::HashMap<String, Value>,
) -> contextdb_core::Result<String> {
    let result = db.execute(sql, params)?;
    Ok(render_explain_trace(&result.trace))
}

pub fn render_query_trace(trace: &QueryTrace, rows_examined: u64) -> String {
    let mut out = format!("trace: {}", trace.physical_plan);
    if let Some(index) = &trace.index_used {
        out.push_str(&format!(" index={index}"));
    }
    if !trace.predicates_pushed.is_empty() {
        out.push_str(" pushed=[");
        for (idx, predicate) in trace.predicates_pushed.iter().enumerate() {
            if idx > 0 {
                out.push_str(", ");
            }
            out.push_str(predicate.as_ref());
        }
        out.push(']');
    }
    out.push_str(&format!(" rows_examined={rows_examined}"));
    out
}

/// Render a single `Value` as the CLI displays it in SELECT output.
pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Uuid(u) => u.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Vector(vs) => format!("{vs:?}"),
        Value::TxId(tx) => tx.0.to_string(),
    }
}

/// Render the `.sync status` output buffer. Includes the live committed-TxId.
pub fn render_sync_status(db: &Database) -> String {
    format!("Committed TxId: {}\n", db.committed_watermark().0)
}

/// Stable public document for one synchronous conflict receipt. Optional
/// facts are absent rather than fabricated, which matters for authority
/// refusals that deliberately have no winning value.
pub fn sync_conflict_document(conflict: &Conflict) -> serde_json::Value {
    let mut fields = serde_json::Map::from_iter([(
        "natural_key".to_string(),
        serde_json::to_value(&conflict.natural_key).expect("natural keys are serializable"),
    )]);
    for (name, value) in [
        ("table", conflict.table.clone()),
        ("mutation_kind", conflict.mutation_kind.clone()),
        ("reason", conflict.reason.clone()),
        (
            "winning_author_node_id",
            conflict.winning_author_node_id.clone(),
        ),
    ] {
        if let Some(value) = value {
            fields.insert(name.to_string(), serde_json::Value::String(value));
        }
    }
    if let Some(position) = conflict.hub_acceptance_position {
        fields.insert(
            "hub_acceptance_position".to_string(),
            serde_json::Value::Number(position.0.into()),
        );
    }
    // A member with nothing of its own to report still says why it was
    // turned away, so the reader is never left with a bare key and no cause.
    if let Some(cause) = conflict.refusal_cause.as_ref() {
        fields.insert(
            "refusal_cause".to_string(),
            serde_json::json!({
                "table": cause.table.clone(),
                "natural_key": serde_json::to_value(&cause.natural_key)
                    .expect("natural keys are serializable"),
            }),
        );
    }
    serde_json::Value::Object(fields)
}

/// Human rendering uses the same complete document as JSON and bundled
/// auto-sync, so no path can silently reduce a receipt to its reason string.
pub fn render_sync_conflict(conflict: &Conflict) -> String {
    sync_conflict_document(conflict).to_string()
}

#[cfg(test)]
mod sync_conflict_tests {
    use super::*;
    use crate::sync_types::ConflictPolicy;
    use crate::sync_types::NaturalKey;
    use contextdb_core::{Lsn, Value};

    #[test]
    fn conflict_document_keeps_every_provenance_field() {
        let conflict = Conflict {
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            resolution: ConflictPolicy::InsertIfNotExists,
            reason: Some("keep_first".to_string()),
            table: Some("camera_events".to_string()),
            mutation_kind: Some("delete".to_string()),
            winning_author_node_id: Some("ab".repeat(32)),
            hub_acceptance_position: Some(Lsn(41)),
            refusal_cause: None,
        };

        assert_eq!(
            sync_conflict_document(&conflict),
            serde_json::json!({
                "natural_key": { "column": "id", "value": { "Int64": 7 }, "rest": [] },
                "table": "camera_events",
                "mutation_kind": "delete",
                "reason": "keep_first",
                "winning_author_node_id": "ab".repeat(32),
                "hub_acceptance_position": 41,
            })
        );
    }

    #[test]
    fn winnerless_document_does_not_invent_winner_fields() {
        let conflict = Conflict {
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            resolution: ConflictPolicy::InsertIfNotExists,
            reason: Some("purge_requires_authoritative_hub".to_string()),
            table: Some("camera_events".to_string()),
            mutation_kind: Some("purge".to_string()),
            winning_author_node_id: None,
            hub_acceptance_position: None,
            refusal_cause: None,
        };

        let document = sync_conflict_document(&conflict);
        assert!(document.get("winning_author_node_id").is_none());
        assert!(document.get("hub_acceptance_position").is_none());
    }

    #[test]
    fn winnerless_document_names_the_row_that_caused_the_refusal() {
        let conflict = Conflict {
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(9)),
            resolution: ConflictPolicy::InsertIfNotExists,
            reason: Some("dependency_complete_refused".to_string()),
            table: Some("camera_event_groups".to_string()),
            mutation_kind: Some("edit".to_string()),
            winning_author_node_id: None,
            hub_acceptance_position: None,
            refusal_cause: Some(crate::sync_types::RefusalCause {
                table: "camera_events".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            }),
        };

        let document = sync_conflict_document(&conflict);
        assert!(document.get("winning_author_node_id").is_none());
        assert!(document.get("hub_acceptance_position").is_none());
        assert_eq!(
            document.get("refusal_cause"),
            Some(&serde_json::json!({
                "table": "camera_events",
                "natural_key": { "column": "id", "value": { "Int64": 7 }, "rest": [] },
            }))
        );
    }
}
