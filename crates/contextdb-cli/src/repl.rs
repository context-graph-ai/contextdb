use crate::formatter::format_query_result;
use contextdb_core::{ColumnType, TableMeta};
use contextdb_engine::Database;
use contextdb_server::SyncClient;
use rustyline::DefaultEditor;
use std::collections::HashMap;
use std::sync::Arc;

pub fn run(
    db: Arc<Database>,
    _sync_client: Option<&SyncClient>,
    _rt: Option<&tokio::runtime::Runtime>,
) {
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
                    if !handle_meta_command(&db, _sync_client, _rt, line) {
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

pub(crate) fn handle_meta_command(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
) -> bool {
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
            } else if let Some(meta) = db.table_meta(rest) {
                print_table_meta(rest, &meta);
            } else {
                println!("Table not found: {rest}");
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
        ".sync" | "\\sync" => {
            println!("{}", handle_sync_command(sync_client, rt, rest));
        }
        _ => println!("Unknown command: {}. Type \\? for help.", cmd),
    }

    true
}

fn handle_sync_command(
    _sync_client: Option<&SyncClient>,
    _rt: Option<&tokio::runtime::Runtime>,
    _args: &str,
) -> String {
    "not implemented".to_string()
}

fn print_table_meta(table: &str, meta: &TableMeta) {
    println!("CREATE TABLE {table} (");
    for (idx, col) in meta.columns.iter().enumerate() {
        let comma = if idx + 1 == meta.columns.len() {
            ""
        } else {
            ","
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};

    #[test]
    fn test_backslash_dt() {
        let db = Database::open_memory();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        assert!(handle_meta_command(&db, None, None, "\\dt"));
    }

    // B1: Existing \dt works with new handle_meta_command signature
    #[test]
    fn b1_existing_dt_works_with_new_signature() {
        let db = Database::open_memory();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        // Pass None for sync_client and rt — existing commands must work without sync
        assert!(handle_meta_command(&db, None, None, "\\dt"));
        // Also verify .tables works
        assert!(handle_meta_command(&db, None, None, ".tables"));
        // .quit returns false
        assert!(!handle_meta_command(&db, None, None, ".quit"));
    }

    // B2: .sync subcommands handle missing sync configuration
    #[test]
    fn b2_sync_not_configured_message() {
        for subcmd in [
            "status",
            "push",
            "pull",
            "reconnect",
            "direction t Push",
            "policy t ServerWins",
        ] {
            let result = handle_sync_command(None, None, subcmd);
            assert!(
                result.contains("Sync not configured"),
                "subcmd '{}' should return 'Sync not configured', got: {}",
                subcmd,
                result
            );
        }
    }

    // B3: .sync direction parses all four direction values
    #[test]
    fn b3_sync_direction_parsing() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client =
            rt.block_on(async { SyncClient::new(db, "nats://localhost:19999", "b3-test") });

        // Suppress unused import warning — SyncDirection is used to verify the API exists
        let _directions = [
            SyncDirection::Push,
            SyncDirection::Pull,
            SyncDirection::Both,
            SyncDirection::None,
        ];

        for (table, dir) in [
            ("observations", "Push"),
            ("patterns", "pull"),
            ("decisions", "Both"),
            ("scratch", "None"),
        ] {
            let args = format!("direction {} {}", table, dir);
            let result = handle_sync_command(Some(&client), Some(&rt), &args);
            assert!(
                result.contains(table),
                "direction command for '{}' should contain table name, got: {}",
                table,
                result
            );
        }
    }

    // B4: .sync policy parses all four policies + default
    #[test]
    fn b4_sync_policy_parsing() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client =
            rt.block_on(async { SyncClient::new(db, "nats://localhost:19999", "b4-test") });

        // Suppress unused import warning — ConflictPolicy is used to verify the API exists
        let _policies = [
            ConflictPolicy::InsertIfNotExists,
            ConflictPolicy::ServerWins,
            ConflictPolicy::EdgeWins,
            ConflictPolicy::LatestWins,
        ];

        let result = handle_sync_command(Some(&client), Some(&rt), "policy obs InsertIfNotExists");
        assert!(
            result.contains("InsertIfNotExists"),
            "policy command should contain 'InsertIfNotExists', got: {}",
            result
        );

        let result = handle_sync_command(Some(&client), Some(&rt), "policy default ServerWins");
        assert!(
            result.contains("Default") || result.contains("default"),
            "default policy command should reference 'default', got: {}",
            result
        );
    }

    // B5: .sync invalid arguments handled gracefully
    #[test]
    fn b5_sync_invalid_args() {
        let db = Arc::new(Database::open_memory());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client =
            rt.block_on(async { SyncClient::new(db, "nats://localhost:19999", "b5-test") });

        for bad_input in [
            "bogus",
            "direction",
            "direction table_only",
            "direction t InvalidDir",
            "policy",
            "policy table_only",
            "policy t InvalidPolicy",
        ] {
            let result = handle_sync_command(Some(&client), Some(&rt), bad_input);
            assert!(
                !result.contains("not implemented"),
                "bad input '{}' should not return 'not implemented', got: {}",
                bad_input,
                result
            );
        }
    }
}
