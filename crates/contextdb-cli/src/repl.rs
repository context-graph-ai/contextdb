use crate::formatter::format_query_result;
use contextdb_core::schema;
use contextdb_engine::Database;
use rustyline::DefaultEditor;
use std::collections::HashMap;

pub fn run(db: Database) {
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
    println!("ContextDB v0.1.0");
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

                if line.starts_with('.') {
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

fn handle_meta_command(db: &Database, line: &str) -> bool {
    let mut parts = line.splitn(2, ' ');
    let cmd = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();

    match cmd {
        ".quit" | ".exit" => return false,
        ".help" => {
            println!(".help               Show this message");
            println!(".quit/.exit         Exit REPL");
            println!(".tables             List ontology tables");
            println!(".schema <table>     Show basic schema hint for a table");
            println!(".explain <sql>      Show execution plan");
        }
        ".tables" => {
            for t in schema::ONTOLOGY_TABLES {
                println!("{}", t);
            }
        }
        ".schema" => {
            if rest.is_empty() {
                println!("Usage: .schema <table>");
            } else {
                println!("CREATE TABLE {} (...)", rest);
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
        _ => println!("Unknown meta-command: {}", cmd),
    }

    true
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
