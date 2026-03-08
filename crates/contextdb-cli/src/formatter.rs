use contextdb_core::Value;
use contextdb_engine::QueryResult;

pub fn format_query_result(result: &QueryResult) -> String {
    if result.columns.is_empty() {
        return String::new();
    }

    let mut widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();

    let rows: Vec<Vec<String>> = result
        .rows
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
    for (i, col) in result.columns.iter().enumerate() {
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
    }
}
