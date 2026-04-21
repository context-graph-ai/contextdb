//! Public rendering helpers used by both the CLI binary and the test suite.

use crate::Database;
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

/// Render a table's `.schema` DDL.
pub fn render_table_meta(table: &str, meta: &TableMeta) -> String {
    let mut buf = String::new();
    writeln!(&mut buf, "CREATE TABLE {table} (").unwrap();
    let mut first = true;
    for col in &meta.columns {
        if !first {
            buf.push_str(",\n");
        }
        first = false;
        let mut ty = render_column_type(&col.column_type);
        if !col.nullable && !col.primary_key {
            ty.push_str(" NOT NULL");
        }
        if col.primary_key {
            ty.push_str(" PRIMARY KEY");
        }
        if col.immutable {
            ty.push_str(" IMMUTABLE");
        }
        write!(&mut buf, "  {} {}", col.name, ty).unwrap();
    }
    buf.push_str("\n)\n");
    if meta.immutable {
        buf.push_str("IMMUTABLE\n");
    }
    if let Some(sm) = &meta.state_machine {
        let mut entries: Vec<_> = sm.transitions.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let transitions: Vec<String> = entries
            .into_iter()
            .map(|(from, tos)| format!("{from} -> [{}]", tos.join(", ")))
            .collect();
        writeln!(
            &mut buf,
            "STATE MACHINE ({}: {})",
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
        writeln!(&mut buf, "DAG({edge_types})").unwrap();
    }
    // Stub: emit NO `CREATE INDEX ...` lines for declared indexes. Impl must
    // walk `meta.indexes` and emit one line per index with per-column ASC/DESC.
    let _ = meta.indexes.len();
    buf
}

/// Render the `.explain <sql>` REPL output. Stub returns a placeholder that
/// does NOT contain `"IndexScan"`, so EX03 fails until the impl fills this
/// with a real physical-plan + trace summary.
pub fn render_explain(
    db: &Database,
    sql: &str,
    params: &std::collections::HashMap<String, Value>,
) -> contextdb_core::Result<String> {
    let _ = (db, sql, params);
    Ok("Scan(stub)\n".to_string())
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
