//! Public rendering helpers used by both the CLI binary and the test suite.

use crate::Database;
use crate::database::QueryTrace;
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

/// Render the `.explain <sql>` REPL output. Runs the SQL to populate the
/// trace, then formats the physical plan + index-usage summary.
pub fn render_explain(
    db: &Database,
    sql: &str,
    params: &std::collections::HashMap<String, Value>,
) -> contextdb_core::Result<String> {
    let result = db.execute(sql, params)?;
    let mut out = String::new();
    out.push_str(result.trace.physical_plan);
    if let Some(idx) = &result.trace.index_used {
        out.push_str(&format!(" {{ index: {idx} }}"));
    }
    out.push('\n');
    if !result.trace.predicates_pushed.is_empty() {
        out.push_str("  predicates_pushed: [");
        for (i, p) in result.trace.predicates_pushed.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(p.as_ref());
        }
        out.push_str("]\n");
    }
    if !result.trace.indexes_considered.is_empty() {
        out.push_str("  indexes_considered: [");
        for (i, c) in result.trace.indexes_considered.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&format!("{}: {}", c.name, c.rejected_reason));
        }
        out.push_str("]\n");
    }
    if result.trace.sort_elided {
        out.push_str("  sort_elided: true\n");
    }
    Ok(out)
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
