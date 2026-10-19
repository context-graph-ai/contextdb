use contextdb_core::Value;
use contextdb_engine::QueryResult;

pub fn format_query_result(result: &QueryResult) -> String {
    format_query_result_with_empty_headers(result, false)
}

pub fn format_query_result_with_empty_headers(
    result: &QueryResult,
    show_empty_headers: bool,
) -> String {
    render_table(&result.columns, &result.rows, show_empty_headers)
}

/// Rows as objects keyed by column name, one object per row.
///
/// One builder for every machine surface that publishes rows — an ordinary
/// result and a cursor page alike — so the two can never render the same row
/// differently.
pub fn rows_as_objects<'a>(columns: &'a [String], rows: &'a [Vec<Value>]) -> RowsAsObjects<'a> {
    RowsAsObjects { columns, rows }
}

/// A serialization view over admitted answers; cells are never deep copied.
pub struct RowsAsObjects<'a> {
    columns: &'a [String],
    rows: &'a [Vec<Value>],
}
impl serde::Serialize for RowsAsObjects<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in self.rows {
            seq.serialize_element(&ObjectRow {
                columns: self.columns,
                row,
            })?;
        }
        seq.end()
    }
}
struct ObjectRow<'a> {
    columns: &'a [String],
    row: &'a [Value],
}
impl serde::Serialize for ObjectRow<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (index, column) in self.columns.iter().enumerate() {
            map.serialize_entry(column, &JsonCell(self.row.get(index)))?;
        }
        map.end()
    }
}
struct JsonCell<'a>(Option<&'a Value>);
impl serde::Serialize for JsonCell<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            None | Some(Value::Null) => serializer.serialize_none(),
            Some(Value::Bool(value)) => value.serialize(serializer),
            Some(Value::Int64(value) | Value::Timestamp(value)) => value.serialize(serializer),
            Some(Value::Float64(value)) => value.serialize(serializer),
            Some(Value::Text(value)) => value.serialize(serializer),
            Some(Value::Uuid(value)) => serializer.collect_str(value),
            Some(Value::Json(value)) => value.serialize(serializer),
            Some(Value::Vector(value)) => value.serialize(serializer),
            Some(Value::TxId(value)) => value.0.serialize(serializer),
        }
    }
}

/// Serialize the existing admitted rows directly to their public JSON shape.
pub fn format_query_result_json(result: &QueryResult) -> String {
    serde_json::to_string(&rows_as_objects(&result.columns, &result.rows))
        .unwrap_or_else(|_| "[]".to_string())
}

fn render_table(columns: &[String], data: &[Vec<Value>], show_empty_headers: bool) -> String {
    if columns.is_empty() || (data.is_empty() && !show_empty_headers) {
        return String::new();
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    let rows: Vec<Vec<String>> = data
        .iter()
        .map(|row| row.iter().map(render_value).collect())
        .collect();

    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let sep = widths
        .iter()
        .map(|w| format!("+{}", "-".repeat(*w + 2)))
        .collect::<String>()
        + "+";

    let mut out = String::new();
    out.push_str(&sep);
    out.push('\n');

    out.push('|');
    for (i, col) in columns.iter().enumerate() {
        out.push(' ');
        out.push_str(&format!("{:<width$}", col, width = widths[i]));
        out.push(' ');
        out.push('|');
    }
    out.push('\n');

    out.push_str(&sep);
    out.push('\n');

    for row in rows {
        out.push('|');
        for (i, cell) in row.iter().enumerate() {
            out.push(' ');
            out.push_str(&format!("{:<width$}", cell, width = widths[i]));
            out.push(' ');
            out.push('|');
        }
        out.push('\n');
    }

    out.push_str(&sep);
    out
}

/// One cell as a person reads it. Shared by the human table and any other
/// human rendering of a row, so two surfaces never print the same value
/// differently.
pub fn render_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Uuid(u) => u.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Vector(vec) => {
            let inner = vec
                .iter()
                .map(|component| format!("{component:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{inner}]")
        }
        Value::TxId(tx) => tx.0.to_string(),
    }
}
