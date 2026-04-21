//! Public rendering helpers used by both the CLI binary and the test suite.
//! Stub values are intentionally wrong so tests fail on runtime assertion; impl
//! plan Step 5 replaces them with the real logic.

use crate::Database;
use contextdb_core::Value;
use contextdb_core::table_meta::{ColumnType, TableMeta};
use std::fmt::Write;

/// Render a column type as a DDL token. Stub: returns `"BROKEN"` for `ColumnType::TxId`
/// and the canonical DDL token for every other variant. Impl must change the TxId arm
/// to `"TXID"`.
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
        ColumnType::TxId => "BROKEN".to_string(),
    }
}

/// Render a table's `.schema` DDL. Stub: emits a CREATE TABLE that uses the stub
/// `render_column_type` output (so TxId columns emit `BROKEN`, which fails to
/// re-parse — the intended RED signal for T10).
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
        write!(&mut buf, "  {} {}", col.name, ty).unwrap();
    }
    buf.push_str("\n)\n");
    buf
}

/// Render a single `Value` as the CLI displays it in SELECT output. Stub: returns
/// `"BROKEN"` for `Value::TxId`. Impl must change the TxId arm to `tx.0.to_string()`.
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
        Value::TxId(_) => "BROKEN".to_string(),
    }
}

/// Render the `.sync status` output buffer. Stub: returns a string containing the
/// fixed wrong `Committed TxId: 999999999` line. Impl must change to read the live
/// `db.committed_watermark().0`.
pub fn render_sync_status(_db: &Database) -> String {
    "Committed TxId: 999999999\n".to_string()
}
