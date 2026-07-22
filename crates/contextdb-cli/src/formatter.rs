use contextdb_core::Value;
use contextdb_engine::QueryResult;

/// Default cap on the number of data rows printed in the human table. A
/// `SELECT *` on a large table would otherwise dump every row; the cap keeps
/// default output bounded and prints a footer naming how to see more.
pub const DEFAULT_ROW_CAP: usize = 100;

pub fn format_query_result(result: &QueryResult) -> String {
    format_query_result_with_empty_headers(result, false)
}

pub fn format_query_result_with_empty_headers(
    result: &QueryResult,
    show_empty_headers: bool,
) -> String {
    render_table(&result.columns, &result.rows, show_empty_headers)
}

/// Render the human table, capping data rows at `cap` when set. When the cap
/// truncates, a footer line names the total and the `--all` override. `None`
/// prints every row (the `--all` path).
pub fn format_query_result_capped(
    result: &QueryResult,
    cap: Option<usize>,
    show_empty_headers: bool,
) -> String {
    let total = result.rows.len();
    let limit = cap.map(|c| c.min(total)).unwrap_or(total);
    let mut out = render_table(&result.columns, &result.rows[..limit], show_empty_headers);
    if limit < total {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!(
            "-- showing {limit} of {total} rows; narrow with WHERE/LIMIT, or pass --all to print every row"
        ));
    }
    out
}

/// Serialize a query result as a JSON array of objects (column name → value),
/// one object per row. Uncapped — a machine consumer pages itself.
pub fn format_query_result_json(result: &QueryResult) -> String {
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = result
                .columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let cell = row
                        .get(i)
                        .map(value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    (col.clone(), cell)
                })
                .collect();
            serde_json::Value::Object(obj)
        })
        .collect();
    serde_json::to_string(&serde_json::Value::Array(rows)).unwrap_or_else(|_| "[]".to_string())
}

/// Map a cell to a clean, stable `serde_json::Value`. Deliberately independent
/// of `Value`'s own (enum-tagged, JSON-as-string) Serialize impl so the output
/// stays flat and predictable.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(i) => serde_json::Value::from(*i),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Timestamp(ts) => serde_json::Value::from(*ts),
        Value::Json(j) => j.clone(),
        Value::Vector(vec) => serde_json::Value::Array(
            vec.iter()
                .map(|f| {
                    serde_json::Number::from_f64(*f as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::TxId(tx) => serde_json::Value::from(tx.0),
    }
}

fn render_table(columns: &[String], data: &[Vec<Value>], show_empty_headers: bool) -> String {
    if columns.is_empty() || (data.is_empty() && !show_empty_headers) {
        return String::new();
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    let rows: Vec<Vec<String>> = data
        .iter()
        .map(|row| row.iter().map(value_to_string).collect())
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

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Uuid(u) => u.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Vector(vec) => format!("{:?}", vec),
        Value::TxId(tx) => tx.0.to_string(),
    }
}
