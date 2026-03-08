use crate::formatter::format_query_result;
use contextdb_core::{ColumnType, TableMeta};
use contextdb_engine::Database;
use rustyline::DefaultEditor;
use std::collections::HashMap;

pub fn run(db: Database) {
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
    println!("ContextDB v{}", env!("CARGO_PKG_VERSION"));
    println!("Enter .help for usage hints.");

    loop {
        let readline = rl.readline("contextdb> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);

                if line.starts_with('.') || line.starts_with('\\') {
                    if !handle_meta_command(&db, line) {
                        break;
                    }
                } else {
                    execute_sql(&db, line);
                }
            }
            Err(_) => break,
        }
    }
}

pub(crate) fn handle_meta_command(db: &Database, line: &str) -> bool {
    let mut parts = line.splitn(2, ' ');
    let cmd = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();

    match cmd {
        ".quit" | ".exit" | "\\q" => return false,
        ".help" | "\\?" => {
            println!(".help / \\?          Show this message");
            println!(".quit/.exit / \\q    Exit REPL");
            println!(".tables / \\dt       List tables");
            println!(".schema / \\d <tbl>  Show table schema and constraints");
            println!(".explain <sql>      Show execution plan");
        }
        ".tables" | "\\dt" => {
            for t in db.table_names() {
                println!("{t}");
            }
        }
        ".schema" | "\\d" => {
            if rest.is_empty() {
                println!("Usage: .schema <table> or \\d <table>");
            } else {
                if let Some(meta) = db.table_meta(rest) {
                    print_table_meta(rest, &meta);
                } else {
                    println!("Table not found: {rest}");
                }
            }
        }
        ".explain" => {
            if rest.is_empty() {
                println!("Usage: .explain <sql>");
            } else {
                match db.explain(rest) {
                    Ok(plan) => println!("{}", plan),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
        }
        _ => println!("Unknown command: {}. Type \\? for help.", cmd),
    }

    true
}

fn print_table_meta(table: &str, meta: &TableMeta) {
    println!("CREATE TABLE {table} (");
    for (idx, col) in meta.columns.iter().enumerate() {
        let comma = if idx + 1 == meta.columns.len() { "" } else { "," };
        let nullable = if col.nullable { "" } else { " NOT NULL" };
        let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
        println!(
            "  {} {}{}{}{}",
            col.name,
            render_column_type(&col.column_type),
            nullable,
            pk,
            comma
        );
    }
    println!(")");
    if meta.immutable {
        println!("IMMUTABLE");
    }
    if let Some(sm) = &meta.state_machine {
        let mut entries: Vec<_> = sm.transitions.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let transitions: Vec<String> = entries
            .into_iter()
            .map(|(from, tos)| format!("{from} -> [{}]", tos.join(", ")))
            .collect();
        println!("STATE MACHINE ({}: {})", sm.column, transitions.join(", "));
    }
}

fn render_column_type(col_type: &ColumnType) -> String {
    match col_type {
        ColumnType::Integer => "INTEGER".to_string(),
        ColumnType::Real => "REAL".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Boolean => "BOOLEAN".to_string(),
        ColumnType::Json => "JSON".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Vector(dim) => format!("VECTOR({dim})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backslash_dt() {
        let db = Database::open_memory();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        assert!(handle_meta_command(&db, "\\dt"));
    }
}

fn execute_sql(db: &Database, sql: &str) {
    match db.execute(sql, &HashMap::new()) {
        Ok(result) => {
            if result.columns.is_empty() {
                println!("ok (rows_affected={})", result.rows_affected);
            } else {
                println!("{}", format_query_result(&result));
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}
