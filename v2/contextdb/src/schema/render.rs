//! Printing a declaration back as SQL.

use super::{Consolidation, Quantization, RESERVED_PREFIXES, Table};
use crate::types::{ColumnDef, ColumnDefault, ColumnType, SortDirection, Value, VectorSearchMode};

/// A table's declaration as SQL: the CREATE TABLE, then one CREATE INDEX for each index the
/// author made, every statement ended by `;` and a new line. A clause left at its default is
/// not printed, and neither are the indexes the engine keeps behind a key or a UNIQUE rule.
pub(super) fn declaration(table: &Table) -> String {
    let meta = &table.meta;
    let mut lines: Vec<String> = meta
        .columns
        .iter()
        .map(|declared| column(table, declared))
        .collect();
    for rule in &meta.unique_constraints {
        let columns: Vec<String> = rule.iter().map(|column| name(column)).collect();
        lines.push(format!("UNIQUE ({})", columns.join(", ")));
    }
    let table = name(&table.name);
    let mut text = format!("CREATE TABLE {table} (\n  {}\n);\n", lines.join(",\n  "));
    for index in &meta.indexes {
        if RESERVED_PREFIXES
            .iter()
            .any(|prefix| index.name.starts_with(*prefix))
        {
            continue;
        }
        let columns: Vec<String> = index
            .columns
            .iter()
            .map(|(column, direction)| match direction {
                SortDirection::Asc => name(column),
                SortDirection::Desc => format!("{} DESC", name(column)),
            })
            .collect();
        text.push_str(&format!(
            "CREATE INDEX {} ON {table} ({});\n",
            name(&index.name),
            columns.join(", ")
        ));
    }
    text
}

/// One column's line: its name, its type, and the clauses it was given.
fn column(table: &Table, column: &ColumnDef) -> String {
    let mut text = name(&column.name);
    text.push(' ');
    match column.column_type {
        ColumnType::Integer => text.push_str("INTEGER"),
        ColumnType::Real => text.push_str("REAL"),
        ColumnType::Text => text.push_str("TEXT"),
        ColumnType::Boolean => text.push_str("BOOLEAN"),
        ColumnType::Json => text.push_str("JSON"),
        ColumnType::Uuid => text.push_str("UUID"),
        ColumnType::Timestamp => text.push_str("TIMESTAMP"),
        ColumnType::Vector(size) => text.push_str(&format!("VECTOR({size})")),
    }
    if let Some(vector) = table.vector(&column.name) {
        if vector.quantization == Quantization::Sq8 {
            text.push_str(" WITH (quantization = 'SQ8')");
        }
        // The limit on partitions refuses writes, so it is printed for every column with a
        // PARTITION_KEY, whether or not it was written. The clauses after it are printed only
        // when they were written.
        if let Some(limit) = vector.limit() {
            let key: Vec<String> = vector.partition_key.iter().map(|key| name(key)).collect();
            text.push_str(&format!(
                " PARTITION_KEY ({}) MAX_PARTITIONS {limit}",
                key.join(", ")
            ));
        }
        text.push_str(match vector.search_mode {
            None => "",
            Some(VectorSearchMode::Auto) => " SEARCH_MODE AUTO",
            Some(VectorSearchMode::Exact) => " SEARCH_MODE EXACT",
            Some(VectorSearchMode::Indexed) => " SEARCH_MODE INDEXED",
        });
        if let Some(at) = vector.auto_index_at {
            text.push_str(&format!(" AUTO_INDEX_AT {at}"));
        }
        match vector.consolidation {
            None => {}
            Some(Consolidation::None) => text.push_str(" CONSOLIDATION NONE"),
            Some(Consolidation::Percent { change, tombstone }) => {
                let members = [("CHANGE_PERCENT", change), ("TOMBSTONE_PERCENT", tombstone)];
                let written = members.iter().filter_map(|(member, percent)| {
                    percent.map(|percent| format!("{member} = {percent}"))
                });
                let written: Vec<String> = written.collect();
                text.push_str(&format!(" CONSOLIDATION ({})", written.join(", ")));
            }
        }
    }
    if column.primary_key {
        text.push_str(" PRIMARY KEY");
    }
    if !column.nullable {
        text.push_str(" NOT NULL");
    }
    match &column.default {
        Some(ColumnDefault::Literal(value)) => {
            text.push_str(" DEFAULT ");
            text.push_str(&literal(value));
        }
        Some(ColumnDefault::Now) => text.push_str(" DEFAULT NOW()"),
        None => {}
    }
    if column.unique {
        text.push_str(" UNIQUE");
    }
    text
}

/// A default's value as the author wrote it.
fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(true) => "TRUE".to_owned(),
        Value::Bool(false) => "FALSE".to_owned(),
        Value::Int64(whole) => whole.to_string(),
        // A whole REAL keeps its decimal point, so it reads back as a REAL.
        Value::Float64(real) if real.fract() == 0.0 => format!("{real:.1}"),
        Value::Float64(real) => real.to_string(),
        Value::Text(words) => format!("'{}'", words.replace('\'', "''")),
        other => unreachable!("a default is declared as a literal, never as {other:?}"),
    }
}

/// A name as SQL reads it back: bare when it is one plain word, in double quotes otherwise.
fn name(name: &str) -> String {
    let plain = name.starts_with(|first: char| first.is_ascii_alphabetic() || first == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
